"""核心调度器：资源记账、配额、公平与调度决策。"""

from __future__ import annotations

import itertools
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
        # 反向依赖索引：上游作业 id -> 等待它的下游作业 id 列表（按提交顺序）。
        self._dependents: dict[str, list[str]] = {}
        self._used = Resources()
        self._submitter_used: dict[str, Resources] = {}
        self._id_seq = itertools.count(1)
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
        - 依赖校验（均在持锁状态下原子完成，与并发提交互斥）：
          依赖不存在的作业 -> JobRejectedError(UNKNOWN_DEPENDENCY)
          依赖自己 -> JobRejectedError(SELF_DEPENDENCY)
          依赖构成环（直接或间接） -> JobRejectedError(DEPENDENCY_CYCLE)
        依赖未全部成功时作业进入 WAITING：不进排队队列、不占资源与配额；
        依赖已全部成功则直接进入 PENDING 参与调度。
        提交成功后立即触发一次调度尝试，再返回最新作业状态。
        """
        self._validate_request(request)

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

            job_id = f"job-{next(self._id_seq):06d}"
            depends_on = self._validate_dependencies(job_id, request.depends_on)

            job = Job(
                id=job_id,
                idempotency_key=request.idempotency_key,
                submitter=request.submitter,
                priority=request.priority,
                requested=request.resources,
                created_at_ns=self._clock_ns(),
                duration_ms=request.duration_ms,
                depends_on=depends_on,
            )
            job.history.append(
                f"{job.created_at_ns}: SUBMITTED submitter={job.submitter} "
                f"priority={job.priority} request={request.resources.as_dict()} "
                f"depends_on={depends_on}"
            )
            self._jobs[job.id] = job
            self._key_index[job.idempotency_key] = job.id
            for dep_id in depends_on:
                self._dependents.setdefault(dep_id, []).append(job.id)
            self._admit_or_wait(job)

        # 在锁外执行调度（tick 内部自行加锁，RLock 可重入也无妨）。
        self.tick()
        with self._lock:
            return self._snapshot_job(job)

    def _validate_dependencies(
        self, job_id: str, depends_on: list[str]
    ) -> list[str]:
        """校验依赖列表，返回去重后（保持声明顺序）的依赖作业 id。

        三类非法依赖分别给出可区分的拒绝原因，且一律拒绝受理，
        不允许“先收下再搅乱队列”。
        """
        if not depends_on:
            return []
        deps: list[str] = []
        seen: set[str] = set()
        for dep in depends_on:
            if not isinstance(dep, str) or not dep.strip():
                raise JobRejectedError(
                    f"依赖作业 id 非法: {dep!r}", RejectReason.INVALID_REQUEST
                )
            if dep not in seen:
                seen.add(dep)
                deps.append(dep)

        for dep in deps:
            if dep == job_id:
                raise JobRejectedError(
                    f"作业 {job_id} 不能依赖自己",
                    RejectReason.SELF_DEPENDENCY,
                )
            if dep not in self._jobs:
                raise JobRejectedError(
                    f"依赖的作业 {dep!r} 不存在",
                    RejectReason.UNKNOWN_DEPENDENCY,
                )

        # 环检测：从每个直接依赖沿 depends_on 边遍历，若能回到新作业则成环。
        # 由于作业 id 由服务端单调生成，正常流程下新作业不可能出现在既有
        # 作业的依赖链中；此处仍做完整 DFS 防御，保证任何内部不变量被破坏
        # 时也能在提交当下挡下，而不是收下后让队列死锁。
        for dep in deps:
            if self._dep_reaches(dep, job_id):
                raise JobRejectedError(
                    f"依赖 {dep!r} 会与作业 {job_id} 构成依赖环",
                    RejectReason.DEPENDENCY_CYCLE,
                )
        return deps

    def _dep_reaches(self, start: str, target: str) -> bool:
        """沿 depends_on 边从 start 出发能否到达 target（含 start==target）。"""
        stack = [start]
        visited: set[str] = set()
        while stack:
            current = stack.pop()
            if current == target:
                return True
            if current in visited:
                continue
            visited.add(current)
            job = self._jobs.get(current)
            if job is not None:
                stack.extend(job.depends_on)
        return False

    def _admit_or_wait(self, job: Job) -> None:
        """根据依赖的当前状态决定新作业的初始状态（持锁调用）。

        - 无依赖或依赖全部 COMPLETED：进入 PENDING 排队；
        - 任一依赖已 FAILED/CANCELLED/DEPENDENCY_FAILED：直接终态
          DEPENDENCY_FAILED（这些上游已不可能再成功）；
        - 否则进入 WAITING，不占资源、不占配额、不进排队队列。
        """
        if not job.depends_on:
            job.state = JobState.PENDING
            self._pending.append(job.id)
            return

        failed = [
            d
            for d in job.depends_on
            if self._jobs[d].state
            in (JobState.FAILED, JobState.CANCELLED, JobState.DEPENDENCY_FAILED)
        ]
        if failed:
            job.state = JobState.DEPENDENCY_FAILED
            job.finished_at_ns = self._clock_ns()
            job.scheduling_note = (
                f"提交时上游已终结且未成功: {failed}，作业不会运行"
            )
            job.history.append(
                f"{job.finished_at_ns}: DEPENDENCY_FAILED at submit; "
                f"failed upstreams={failed}"
            )
            return

        unfinished = [
            d for d in job.depends_on if self._jobs[d].state is not JobState.COMPLETED
        ]
        if unfinished:
            job.state = JobState.WAITING
            job.scheduling_note = f"等待依赖完成: {unfinished}"
            job.history.append(
                f"{self._clock_ns()}: WAITING on dependencies {unfinished}"
            )
            return

        job.state = JobState.PENDING
        job.scheduling_note = "提交时依赖已全部成功，直接排队"
        self._pending.append(job.id)
        job.history.append(
            f"{self._clock_ns()}: PENDING (all dependencies already COMPLETED)"
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

        - WAITING/PENDING：直接转为 CANCELLED，移出队列（若在队列中），
          不涉及资源；其下游按“上游被取消”级联为 DEPENDENCY_FAILED。
        - RUNNING：转为 CANCELLED 并释放资源，随后触发调度。
        - 终态：抛 JobStateConflictError，取消不生效、资源不重复释放。
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
            if job.state in (JobState.WAITING, JobState.PENDING):
                prev = job.state
                if prev is JobState.PENDING:
                    self._pending.remove(job.id)
                job.state = JobState.CANCELLED
                job.finished_at_ns = now_ns
                job.scheduling_note = f"{prev.value} 状态下被取消，未占用资源"
                job.history.append(
                    f"{now_ns}: CANCELLED while {prev.value} (no resources held)"
                )
                self._propagate_upstream_finished(job)
                return self._snapshot_job(job)

            # RUNNING
            self._release(job, reason="CANCELLED")
            job.state = JobState.CANCELLED
            job.finished_at_ns = now_ns
            job.history.append(f"{now_ns}: CANCELLED while RUNNING; resources released")
            self._propagate_upstream_finished(job)

        self.tick()
        with self._lock:
            return self._snapshot_job(job)

    def complete(self, job_id: str, failed: bool = False) -> Job:
        """标记作业完成（或失败），释放资源并触发调度。

        非 RUNNING 状态调用一律抛 JobStateConflictError，
        保证资源不会被重复释放（release_count 至多为 1）。
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
            self._propagate_upstream_finished(job)

        self.tick()
        with self._lock:
            return self._snapshot_job(job)

    def _propagate_upstream_finished(self, upstream: Job) -> None:
        """上游进入终态后的依赖传播（持锁调用，确定性顺序）。

        - 上游 COMPLETED：检查其 WAITING 下游，依赖全部成功的转入 PENDING
          排队（等待时长自原始提交时间起算，年龄加分不受影响）；
        - 上游 FAILED/CANCELLED：其 WAITING 下游级联转为终态
          DEPENDENCY_FAILED，并递归向下传播；下游永不自动运行。
        只处理 WAITING 下游：能离开 WAITING 的下游其全部依赖已 COMPLETED，
        而 COMPLETED 是终态，不会再失败，故 PENDING/RUNNING 下游不受影响。
        """
        now_ns = self._clock_ns()
        for dep_id in list(self._dependents.get(upstream.id, [])):
            dependent = self._jobs[dep_id]
            if dependent.state is not JobState.WAITING:
                continue
            if upstream.state is JobState.COMPLETED:
                unfinished = [
                    d
                    for d in dependent.depends_on
                    if self._jobs[d].state is not JobState.COMPLETED
                ]
                if not unfinished:
                    dependent.state = JobState.PENDING
                    dependent.scheduling_note = "依赖已全部成功，进入排队"
                    dependent.history.append(
                        f"{now_ns}: WAITING -> PENDING "
                        f"(all dependencies COMPLETED, triggered by {upstream.id})"
                    )
                    self._pending.append(dependent.id)
                else:
                    dependent.scheduling_note = f"等待依赖完成: {unfinished}"
                    dependent.history.append(
                        f"{now_ns}: still WAITING; unfinished={unfinished} "
                        f"(upstream {upstream.id} COMPLETED)"
                    )
            else:
                dependent.state = JobState.DEPENDENCY_FAILED
                dependent.finished_at_ns = now_ns
                dependent.scheduling_note = (
                    f"上游 {upstream.id} 终态为 {upstream.state.value}，"
                    "作业不会运行"
                )
                dependent.history.append(
                    f"{now_ns}: WAITING -> DEPENDENCY_FAILED "
                    f"(upstream {upstream.id} -> {upstream.state.value})"
                )
                # 级联：该下游自身也是更下游作业的上游。
                self._propagate_upstream_finished(dependent)

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
        waiting_view: list[dict[str, object]] = []
        for job in self._jobs.values():
            if job.state is not JobState.WAITING:
                continue
            waiting_view.append(
                {
                    "job_id": job.id,
                    "submitter": job.submitter,
                    "priority": job.priority,
                    "depends_on": list(job.depends_on),
                    "unfinished_dependencies": [
                        d
                        for d in job.depends_on
                        if self._jobs[d].state is not JobState.COMPLETED
                    ],
                    "wait_ms": (now_ns - job.created_at_ns) // 1_000_000,
                    "request": job.requested.as_dict(),
                    "note": job.scheduling_note,
                }
            )
        waiting_view.sort(key=lambda w: str(w["job_id"]))
        return SchedulingSnapshot(
            timestamp_ns=now_ns,
            node_capacity=self._capacity,
            used=self._used,
            running_jobs=sorted(running),
            pending_jobs=pending_view,
            waiting_jobs=waiting_view,
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
