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

            job = Job(
                id=f"job-{next(self._id_seq):06d}",
                idempotency_key=request.idempotency_key,
                submitter=request.submitter,
                priority=request.priority,
                requested=request.resources,
                created_at_ns=self._clock_ns(),
                duration_ms=request.duration_ms,
            )
            job.history.append(
                f"{job.created_at_ns}: SUBMITTED submitter={job.submitter} "
                f"priority={job.priority} request={request.resources.as_dict()}"
            )
            self._jobs[job.id] = job
            self._key_index[job.idempotency_key] = job.id
            self._pending.append(job.id)

        # 在锁外执行调度（tick 内部自行加锁，RLock 可重入也无妨）。
        self.tick()
        with self._lock:
            return self._snapshot_job(job)

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

        - PENDING：直接转为 CANCELLED，移出队列，不涉及资源。
        - RUNNING：转为 CANCELLED 并释放资源，随后触发调度。
        - 终态：抛 JobStateConflictError，取消不生效、资源不重复释放。
        """
        with self._lock:
            job = self._require_job(job_id)
            if job.state is JobState.CANCELLED:
                raise JobStateConflictError(
                    f"作业 {job_id} 已取消，不能重复取消"
                )
            if job.state in (JobState.COMPLETED, JobState.FAILED):
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
                return self._snapshot_job(job)

            # RUNNING
            self._release(job, reason="CANCELLED")
            job.state = JobState.CANCELLED
            job.finished_at_ns = now_ns
            job.history.append(f"{now_ns}: CANCELLED while RUNNING; resources released")

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

        self.tick()
        with self._lock:
            return self._snapshot_job(job)

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
        return SchedulingSnapshot(
            timestamp_ns=now_ns,
            node_capacity=self._capacity,
            used=self._used,
            running_jobs=sorted(running),
            pending_jobs=pending_view,
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
            schedule_count=job.schedule_count,
            release_count=job.release_count,
            scheduling_note=job.scheduling_note,
            history=list(job.history),
        )
