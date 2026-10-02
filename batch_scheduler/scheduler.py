"""核心调度器：资源记账、配额、公平与调度决策。"""

from __future__ import annotations

import threading
from collections import deque
from typing import Callable

from .errors import (
    JobNotFoundError,
    JobRejectedError,
    JobStateConflictError,
)
from .models import (
    Job,
    JobState,
    RejectReason,
    Resources,
    SchedulingSnapshot,
    SubmitRequest,
)

# 等待时间换算成优先级加分的尺度：等待这么久约等于 1 个优先级档位。
# 优先级数值越大越优先；等待越久加分越多，避免低优先级提交方被无限饿死。
_AGE_NS_PER_PRIORITY_POINT = 1_000_000_000  # 1 秒


def _default_clock_ns() -> int:
    import time

    return time.monotonic_ns()


def _would_create_cycle(
    existing_deps: dict[str, list[str]], new_id: str, new_deps: list[str]
) -> bool:
    """若加入 `new_id -> new_deps` 的边后会形成（直接或间接）环则返回 True。

    纯函数，便于独立单测。从 new_id 沿依赖边深度优先搜索，
    若能再次回到 new_id 则成环。自依赖（new_id in new_deps）是环的特例。
    """
    adjacency: dict[str, list[str]] = dict(existing_deps)
    adjacency[new_id] = list(new_deps)

    visited: set[str] = set()
    stack: list[str] = [new_id]
    while stack:
        current = stack.pop()
        for nxt in adjacency.get(current, []):
            if nxt == new_id:
                return True
            if nxt not in visited:
                visited.add(nxt)
                stack.append(nxt)
    return False


class Scheduler:
    """单节点批处理调度器（线程安全，进程内状态）。"""

    def __init__(
        self,
        node_capacity: Resources,
        submitter_quotas: dict[str, Resources] | None = None,
        clock_ns: Callable[[], int] | None = None,
    ) -> None:
        self._capacity = node_capacity
        self._quotas: dict[str, Resources] = dict(submitter_quotas or {})
        self._clock_ns = clock_ns or _default_clock_ns

        self._lock = threading.RLock()
        self._jobs: dict[str, Job] = {}
        self._key_index: dict[str, str] = {}
        self._pending: deque[str] = deque()
        self._used = Resources()
        self._submitter_used: dict[str, Resources] = {}
        self._next_id = 1  # 只有成功落库的作业才消耗序号，被拒绝的提交不留空洞
        self._last_snapshot: SchedulingSnapshot | None = None

    # ---- 只读视图 -------------------------------------------------------

    @property
    def node_capacity(self) -> Resources:
        return self._capacity

    def used_resources(self) -> Resources:
        with self._lock:
            return self._used

    def submitter_used(self, submitter: str) -> Resources:
        with self._lock:
            return self._submitter_used.get(submitter, Resources())

    def submit(self, request: SubmitRequest) -> Job:
        """提交作业。

        - 非法请求 -> JobRejectedError(INVALID_REQUEST)
        - 超过节点容量 -> JobRejectedError(EXCEEDS_NODE_CAPACITY)
        - 幂等键重复 -> JobRejectedError(DUPLICATE_IDEMPOTENCY_KEY)
          （错误对象上携带已存在的作业，便于提交方按“先到为准”处理）
        - 依赖不存在的作业 -> JobRejectedError(UNKNOWN_DEPENDENCY)
        - 依赖自己 -> JobRejectedError(SELF_DEPENDENCY)
        - 依赖成环 -> JobRejectedError(DEPENDENCY_CYCLE)

        声明了依赖的作业按上游现状决定初始状态：
        - 上游全部 COMPLETED -> PENDING（直接进入调度队列）；
        - 存在已 FAILED/CANCELLED/DEPENDENCY_FAILED 的上游 -> 立即 DEPENDENCY_FAILED
          （终态，永不调度，note 中给出可解释原因）；
        - 否则 -> BLOCKED（等待依赖：不占资源、不占配额、不进调度队列）。
        无依赖或依赖已就绪的提交成功后立即触发一次调度尝试，再返回最新作业状态。
        """
        self._validate_request(request)
        # 依赖列表规范化：去重且保持声明顺序，保证相同输入结论一致。
        depends_on = list(dict.fromkeys(request.depends_on))

        with self._lock:
            existing_id = self._key_index.get(request.idempotency_key)
            if existing_id is not None:
                existing = self._jobs[existing_id]
                err = JobRejectedError(
                    f"idempotency key {request.idempotency_key!r} 已存在: {existing.id}",
                    RejectReason.DUPLICATE_IDEMPOTENCY_KEY,
                )
                err.existing_job = existing  # type: ignore[attr-defined]
                raise err

            job = Job(
                id=f"job-{self._next_id:06d}",
                idempotency_key=request.idempotency_key,
                submitter=request.submitter,
                priority=request.priority,
                requested=request.resources,
                created_at_ns=self._clock_ns(),
                duration_ms=request.duration_ms,
                depends_on=depends_on,
            )
            self._validate_dependencies(job)

            job.history.append(
                f"{job.created_at_ns}: SUBMITTED submitter={job.submitter} "
                f"priority={job.priority} request={request.resources.as_dict()} "
                f"depends_on={depends_on}"
            )
            self._jobs[job.id] = job
            self._key_index[job.idempotency_key] = job.id
            self._next_id += 1
            self._enter_initial_state(job)

        # 在锁外执行调度（tick 内部自行加锁，RLock 可重入也无妨）。
        self.tick()
        with self._lock:
            return self._snapshot_job(job)

    def _validate_dependencies(self, job: Job) -> None:
        """提交当下的依赖校验：自依赖 -> 未知依赖 -> 成环，按此顺序给出可区分原因。"""
        if job.id in job.depends_on:
            raise JobRejectedError(
                f"作业 {job.id} 不能依赖自己",
                RejectReason.SELF_DEPENDENCY,
            )
        for dep_id in job.depends_on:
            if dep_id not in self._jobs:
                raise JobRejectedError(
                    f"依赖的作业 {dep_id!r} 不存在",
                    RejectReason.UNKNOWN_DEPENDENCY,
                )
        existing_deps = {jid: j.depends_on for jid, j in self._jobs.items()}
        if _would_create_cycle(existing_deps, job.id, job.depends_on):
            raise JobRejectedError(
                f"依赖 {job.depends_on} 会与现有作业构成依赖环",
                RejectReason.DEPENDENCY_CYCLE,
            )

    def _enter_initial_state(self, job: Job) -> None:
        """按上游现状决定新作业的初始状态（持锁调用）。"""
        if not job.depends_on:
            self._pending.append(job.id)
            return
        failed = [
            d for d in job.depends_on if self._is_failed_upstream(self._jobs[d])
        ]
        if failed:
            self._mark_dependency_failed(
                job,
                self._clock_ns(),
                cause=f"提交时上游 {failed} 已失败或被取消",
            )
            return
        if all(self._jobs[d].state is JobState.COMPLETED for d in job.depends_on):
            job.scheduling_note = "提交时依赖已全部就绪，直接进入调度队列"
            job.history.append(
                f"{self._clock_ns()}: DEPENDENCIES_READY at submit; enqueued"
            )
            self._pending.append(job.id)
            return
        job.state = JobState.BLOCKED
        waiting = [
            d
            for d in job.depends_on
            if self._jobs[d].state is not JobState.COMPLETED
        ]
        job.scheduling_note = f"等待依赖完成: {waiting}（不占资源与配额）"
        job.history.append(
            f"{self._clock_ns()}: BLOCKED waiting_for={waiting}"
        )

    @staticmethod
    def _is_failed_upstream(job: Job) -> bool:
        return job.state in (
            JobState.FAILED,
            JobState.CANCELLED,
            JobState.DEPENDENCY_FAILED,
        )

    def _validate_request(self, request: SubmitRequest) -> None:
        if not isinstance(request, SubmitRequest):
            raise JobRejectedError(
                "请求类型错误", RejectReason.INVALID_REQUEST
            )
        if not request.idempotency_key or not request.idempotency_key.strip():
            raise JobRejectedError(
                "idempotency_key 不能为空", RejectReason.INVALID_REQUEST
            )
        if not request.submitter or not request.submitter.strip():
            raise JobRejectedError(
                "submitter 不能为空", RejectReason.INVALID_REQUEST
            )
        res = request.resources
        if not isinstance(res, Resources) or not res.is_positive_request():
            raise JobRejectedError(
                "资源请求非法：各维度必须为非负整数且至少申请一项资源",
                RejectReason.INVALID_REQUEST,
            )
        if request.priority < 0:
            raise JobRejectedError(
                "priority 不能为负数", RejectReason.INVALID_REQUEST
            )
        if request.duration_ms <= 0:
            raise JobRejectedError(
                "duration_ms 必须为正整数", RejectReason.INVALID_REQUEST
            )
        if not isinstance(request.depends_on, list) or any(
            not isinstance(d, str) or not d.strip() for d in request.depends_on
        ):
            raise JobRejectedError(
                "depends_on 必须是非空作业 ID 字符串列表",
                RejectReason.INVALID_REQUEST,
            )
        if self._quotas and request.submitter not in self._quotas:
            raise JobRejectedError(
                f"未知提交方 {request.submitter!r}，未配置配额",
                RejectReason.UNKNOWN_SUBMITTER,
            )
        if not res.fits_in(self._capacity):
            raise JobRejectedError(
                f"资源请求 {res.as_dict()} 超过节点容量 {self._capacity.as_dict()}",
                RejectReason.EXCEEDS_NODE_CAPACITY,
            )
        quota = self._quotas.get(request.submitter)
        if quota is not None and not res.fits_in(quota):
            # 超过自身配额上限的作业永远无法运行，按非法请求拒绝，
            # 与“暂时超额可排队”的情形区分开。
            raise JobRejectedError(
                f"资源请求 {res.as_dict()} 超过提交方 {request.submitter!r} "
                f"配额上限 {quota.as_dict()}",
                RejectReason.INVALID_REQUEST,
            )

    def get_job(self, job_id: str) -> Job:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFoundError(f"作业 {job_id!r} 不存在")
            return self._snapshot_job(job)

    def get_job_by_key(self, idempotency_key: str) -> Job | None:
        with self._lock:
            job_id = self._key_index.get(idempotency_key)
            if job_id is None:
                return None
            return self._snapshot_job(self._jobs[job_id])

    def list_jobs(self) -> list[Job]:
        with self._lock:
            return [self._snapshot_job(j) for j in self._jobs.values()]

    def cancel(self, job_id: str) -> Job:
        """取消作业。

        - PENDING：直接转为 CANCELLED，移出队列，不涉及资源。
        - BLOCKED：转为 CANCELLED，本就不占资源与配额，仅做状态流转。
        - RUNNING：转为 CANCELLED 并释放资源，随后触发调度。
        - 终态：抛 JobStateConflictError，取消不生效、资源不重复释放。
        无论从哪个状态取消，都会把“上游被取消”传播给仍在等待依赖的下游。
        """
        with self._lock:
            job = self._require_job(job_id)
            if job.state is JobState.CANCELLED:
                raise JobStateConflictError(
                    f"作业 {job_id} 已取消，不能重复取消"
                )
            if job.state in (
                JobState.COMPLETED,
                JobState.FAILED,
                JobState.DEPENDENCY_FAILED,
            ):
                raise JobStateConflictError(
                    f"作业 {job_id} 已处于终态 {job.state.value}，不能取消"
                )

            now_ns = self._clock_ns()
            if job.state is JobState.PENDING:
                self._pending.remove(job.id)
                job.state = JobState.CANCELLED
                job.finished_at_ns = now_ns
                job.scheduling_note = "排队中被取消，未占用资源"
                job.history.append(f"{now_ns}: CANCELLED while PENDING (no resources held)")
            elif job.state is JobState.BLOCKED:
                job.state = JobState.CANCELLED
                job.finished_at_ns = now_ns
                job.scheduling_note = "等待依赖期间被取消，未占用资源与配额"
                job.history.append(f"{now_ns}: CANCELLED while BLOCKED (no resources held)")
            else:
                # RUNNING
                self._release(job, reason="CANCELLED")
                job.state = JobState.CANCELLED
                job.finished_at_ns = now_ns
                job.history.append(f"{now_ns}: CANCELLED while RUNNING; resources released")

            self._propagate_dependency_failure(job, now_ns)

        self.tick()
        with self._lock:
            return self._snapshot_job(job)

    def complete(self, job_id: str, failed: bool = False) -> Job:
        """标记作业完成（或失败），释放资源并触发调度。

        非 RUNNING 状态调用一律抛 JobStateConflictError，
        保证资源不会被重复释放（release_count 至多为 1）。

        成功：唤醒所有“等待该作业”的下游中依赖已全部就绪者（BLOCKED -> PENDING）。
        失败：把失败沿依赖边传播给仍在等待的下游（-> DEPENDENCY_FAILED 终态）。
        """
        with self._lock:
            job = self._require_job(job_id)
            if job.state is not JobState.RUNNING:
                raise JobStateConflictError(
                    f"作业 {job_id} 当前状态为 {job.state.value}，"
                    "只有 RUNNING 作业可以完成/失败"
                )
            now_ns = self._clock_ns()
            outcome = "FAILED" if failed else "COMPLETED"
            self._release(job, reason=outcome)
            job.state = JobState.FAILED if failed else JobState.COMPLETED
            job.finished_at_ns = now_ns
            job.history.append(f"{now_ns}: {outcome}; resources released")

            if failed:
                self._propagate_dependency_failure(job, now_ns)
            else:
                self._promote_ready_dependents(job, now_ns)

        self.tick()
        with self._lock:
            return self._snapshot_job(job)

    # ---- 依赖传播 ---------------------------------------------------------

    def _dependents_of(self, job_id: str) -> list[Job]:
        """直接依赖 job_id 的全部作业（按提交顺序，确定可复现）。持锁调用。"""
        return [j for j in self._jobs.values() if job_id in j.depends_on]

    def _promote_ready_dependents(self, job: Job, now_ns: int) -> None:
        """上游成功后，把依赖已全部就绪的 BLOCKED 下游放回调度队列。持锁调用。"""
        for dependent in self._dependents_of(job.id):
            if dependent.state is not JobState.BLOCKED:
                continue
            waiting = [
                d
                for d in dependent.depends_on
                if self._jobs[d].state is not JobState.COMPLETED
            ]
            if not waiting:
                dependent.state = JobState.PENDING
                dependent.scheduling_note = "依赖已全部成功完成，进入调度队列"
                dependent.history.append(
                    f"{now_ns}: DEPENDENCIES_READY (upstream {job.id} COMPLETED); enqueued"
                )
                self._pending.append(dependent.id)
            else:
                # 刷新剩余等待列表，查询时能看到实时的未就绪上游。
                dependent.scheduling_note = (
                    f"等待依赖完成: {waiting}（不占资源与配额）"
                )

    def _propagate_dependency_failure(self, job: Job, now_ns: int) -> None:
        """把失败/取消沿依赖边传播给所有仍在等待的下游（含传递下游）。持锁调用。

        不变量：只有 BLOCKED 作业可能存在未终态的上游——一旦依赖全部 COMPLETED
        就会被提升为 PENDING，而 COMPLETED 是终态不会再变，因此传播只会命中
        BLOCKED 作业，绝不会触碰 PENDING/RUNNING 作业。
        """
        queue = [job]
        while queue:
            upstream = queue.pop(0)
            for dependent in self._dependents_of(upstream.id):
                if dependent.state is JobState.BLOCKED:
                    self._mark_dependency_failed(
                        dependent,
                        now_ns,
                        cause=(
                            f"上游 {upstream.id} 终态为 {upstream.state.value}，"
                            "依赖不再可能就绪"
                        ),
                    )
                    queue.append(dependent)
                elif dependent.state is JobState.DEPENDENCY_FAILED:
                    # 已因其它上游被标记，继续向下游传播。
                    queue.append(dependent)

    def _mark_dependency_failed(self, job: Job, now_ns: int, *, cause: str) -> None:
        """把作业置为 DEPENDENCY_FAILED 终态（从不占资源，无需释放）。持锁调用。"""
        job.state = JobState.DEPENDENCY_FAILED
        job.finished_at_ns = now_ns
        job.scheduling_note = f"依赖失败：{cause}；本作业不会被调度"
        job.history.append(f"{now_ns}: DEPENDENCY_FAILED cause={cause}")

    def _require_job(self, job_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise JobNotFoundError(f"作业 {job_id!r} 不存在")
        return job

    def _release(self, job: Job, *, reason: str) -> None:
        """释放作业占用的资源。只能在持锁状态下、对 RUNNING 作业调用一次。"""
        assert job.state is JobState.RUNNING, f"_release 前置状态错误: {job.state}"
        if job.release_count != 0:
            raise JobStateConflictError(
                f"作业 {job.id} 资源已释放过 {job.release_count} 次，禁止重复释放"
            )
        self._used = self._used - job.requested
        before = self._submitter_used.get(job.submitter, Resources())
        self._submitter_used[job.submitter] = before - job.requested
        job.release_count += 1
        if not self._used.is_non_negative():
            raise AssertionError(f"资源记账变为负数: {self._used.as_dict()}")

    def tick(self) -> SchedulingSnapshot:
        """执行一轮调度，返回本轮调度快照（含每个作业的判定依据）。

        调度规则（确定性，无随机、无 map 迭代顺序依赖）：
        1. 候选集为全部 PENDING 作业；
        2. 有效分数 = priority + 已等待秒数（年龄加分），分数高者优先，
           分数相同按 created_at_ns 升序（先到先得），再相同按 job.id；
        3. 逐一遍历排序后的队列：满足节点剩余容量与提交方剩余配额则立即授予，
           否则记录阻塞原因并跳过；一轮内允许多次尝试，被前面的作业“挤掉”的
           作业会在同一轮继续评估，直到一轮内没有任何新授予为止，
           以充分利用释放出的碎片容量。
        """
        with self._lock:
            now_ns = self._clock_ns()
            decisions: list[dict[str, object]] = []
            granted_in_round = 0

            # 最多多轮扫描：每轮至少授予一个作业，否则停止；避免饥饿同时保证终止。
            while True:
                pending_ids = list(self._pending)
                if not pending_ids:
                    break

                ordered = sorted(
                    pending_ids,
                    key=lambda jid: self._schedule_key(self._jobs[jid], now_ns),
                )

                granted_this_pass = False
                blocked: list[str] = []
                for jid in ordered:
                    job = self._jobs[jid]
                    if job.state is not JobState.PENDING:
                        # 极端情况下已被并发取消，清理掉脏 id。
                        blocked.append(jid)
                        continue

                    verdict, reason, detail = self._admission_check(job)
                    if verdict == "grant":
                        self._grant(job, now_ns, detail)
                        decisions.append(
                            {
                                "job_id": job.id,
                                "submitter": job.submitter,
                                "decision": "GRANTED",
                                "effective_priority": round(
                                    self._effective_priority(job, now_ns), 3
                                ),
                                "reason": reason,
                                "detail": detail,
                            }
                        )
                        granted_this_pass = True
                        granted_in_round += 1
                        # 授予后立即从待处理列表移除，剩余作业继续按序尝试。
                    else:
                        job.scheduling_note = reason
                        decisions.append(
                            {
                                "job_id": job.id,
                                "submitter": job.submitter,
                                "decision": "BLOCKED",
                                "effective_priority": round(
                                    self._effective_priority(job, now_ns), 3
                                ),
                                "reason": reason,
                                "detail": detail,
                            }
                        )

                # 重建 deque：保留仍处于 PENDING 的作业，维持创建顺序。
                self._pending = deque(
                    jid for jid in self._pending if self._jobs[jid].state is JobState.PENDING
                )

                if not granted_this_pass:
                    break
                # 若本轮把所有待处理作业都授予了，下一轮自然为空。

            snap = self._build_snapshot(now_ns, decisions, granted_in_round)
            self._last_snapshot = snap
            return snap

    def _effective_priority(self, job: Job, now_ns: int) -> float:
        age_seconds = (now_ns - job.created_at_ns) / _AGE_NS_PER_PRIORITY_POINT
        return job.priority + age_seconds

    def _schedule_key(self, job: Job, now_ns: int) -> tuple[float, int, str]:
        # 分数降序 -> 取负值；等待时长已并入有效分数，故同分时创建更早者在前。
        return (-self._effective_priority(job, now_ns), job.created_at_ns, job.id)

    def _admission_check(
        self, job: Job
    ) -> tuple[str, str, dict[str, object]]:
        """返回 (判定, 人类可读原因, 明细)。判定为 grant 或 blocked。"""
        node_remaining = self._capacity - self._used
        submitter_used = self._submitter_used.get(job.submitter, Resources())
        quota = self._quotas.get(job.submitter)
        quota_remaining = (
            quota - submitter_used if quota is not None else None
        )

        detail: dict[str, object] = {
            "request": job.requested.as_dict(),
            "node_remaining": node_remaining.as_dict(),
        }
        if quota_remaining is not None:
            detail["submitter_quota_remaining"] = quota_remaining.as_dict()

        if quota_remaining is not None and not job.requested.fits_in(quota_remaining):
            return (
                "blocked",
                "等待提交方配额释放（当前已用配额不足以容纳本作业）",
                detail,
            )
        if not job.requested.fits_in(node_remaining):
            return (
                "blocked",
                "等待节点容量释放（当前剩余容量不足）",
                detail,
            )
        return (
            "grant",
            "节点剩余容量与提交方剩余配额均满足，授予资源",
            detail,
        )

    def _grant(self, job: Job, now_ns: int, detail: dict[str, object]) -> None:
        """授予资源。只能在持锁状态下、对 PENDING 作业调用，且至多一次。"""
        if job.schedule_count != 0:
            raise JobStateConflictError(
                f"作业 {job.id} 已被调度 {job.schedule_count} 次，禁止重复调度"
            )
        self._used = self._used + job.requested
        self._submitter_used[job.submitter] = (
            self._submitter_used.get(job.submitter, Resources()) + job.requested
        )
        if not self._used.fits_in(self._capacity):
            raise AssertionError(
                f"授予后超配: used={self._used.as_dict()} cap={self._capacity.as_dict()}"
            )
        quota = self._quotas.get(job.submitter)
        if quota is not None and not self._submitter_used[job.submitter].fits_in(quota):
            raise AssertionError(f"授予后超过提交方 {job.submitter} 配额")

        job.state = JobState.RUNNING
        job.scheduled_at_ns = now_ns
        job.schedule_count += 1
        job.scheduling_note = "已授予资源并开始运行"
        job.history.append(
            f"{now_ns}: GRANTED request={job.requested.as_dict()} "
            f"node_used_after={self._used.as_dict()}"
        )

    def snapshot(self) -> SchedulingSnapshot:
        with self._lock:
            return self._build_snapshot(self._clock_ns(), [], 0)

    def _build_snapshot(
        self,
        now_ns: int,
        decisions: list[dict[str, object]],
        granted_in_round: int,
    ) -> SchedulingSnapshot:
        running = [
            jid for jid, job in self._jobs.items() if job.state is JobState.RUNNING
        ]
        pending_view: list[dict[str, object]] = []
        for jid in self._pending:
            job = self._jobs[jid]
            pending_view.append(
                {
                    "job_id": job.id,
                    "submitter": job.submitter,
                    "priority": job.priority,
                    "effective_priority": round(
                        self._effective_priority(job, now_ns), 3
                    ),
                    "wait_ms": (now_ns - job.created_at_ns) // 1_000_000,
                    "request": job.requested.as_dict(),
                    "note": job.scheduling_note,
                }
            )
        pending_view.sort(
            key=lambda p: (-float(p["effective_priority"]), p["job_id"])
        )
        blocked_view: list[dict[str, object]] = []
        for job in self._jobs.values():
            if job.state is JobState.BLOCKED:
                waiting = [
                    d
                    for d in job.depends_on
                    if self._jobs[d].state is not JobState.COMPLETED
                ]
                blocked_view.append(
                    {
                        "job_id": job.id,
                        "submitter": job.submitter,
                        "priority": job.priority,
                        "depends_on": list(job.depends_on),
                        "waiting_for": waiting,
                        "wait_ms": (now_ns - job.created_at_ns) // 1_000_000,
                        "request": job.requested.as_dict(),
                        "note": job.scheduling_note,
                    }
                )
        blocked_view.sort(key=lambda b: str(b["job_id"]))
        return SchedulingSnapshot(
            timestamp_ns=now_ns,
            node_capacity=self._capacity,
            used=self._used,
            running_jobs=sorted(running),
            pending_jobs=pending_view,
            blocked_jobs=blocked_view,
            submitter_usage={
                name: usage.as_dict()
                for name, usage in sorted(self._submitter_used.items())
            },
            decisions=decisions,
        )

    def _snapshot_job(self, job: Job) -> Job:
        """返回作业的浅拷贝视图，避免外部修改内部状态。"""
        return Job(
            id=job.id,
            idempotency_key=job.idempotency_key,
            submitter=job.submitter,
            priority=job.priority,
            requested=job.requested,
            state=job.state,
            created_at_ns=job.created_at_ns,
            scheduled_at_ns=job.scheduled_at_ns,
            finished_at_ns=job.finished_at_ns,
            duration_ms=job.duration_ms,
            depends_on=list(job.depends_on),
            schedule_count=job.schedule_count,
            release_count=job.release_count,
            scheduling_note=job.scheduling_note,
            history=list(job.history),
        )
