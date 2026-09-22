"""调度核心：线程安全的单节点进程内调度器。

调度策略（详见 README）：
- 有效优先级 = 基础优先级 + aging_weight * 等待 tick 数，随等待时间增长防止饿死；
- 每轮调度按 (有效优先级降序, 提交序号升序 FIFO) 扫描队列，
  同时满足节点容量与提交方配额的作业立即授予资源，否则跳过并记录原因；
- 跳过不阻塞后续作业（小作业可补位）；完成 / 取消 / 失败释放资源后再跑一轮，
  迟到的高优先级作业因此不会无限期饿死（容量最终会轮到它）。
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from collections.abc import Callable

from scheduler.errors import (
    InvalidPriorityError,
    InvalidRequestError,
    InvalidResourceError,
    JobNotFoundError,
    ResourceExceedsCapacityError,
)
from scheduler.models import (
    TERMINAL_STATES,
    Job,
    JobState,
    QueuedDecision,
    Resource,
    SchedulerSnapshot,
    SubmitResult,
)


class ManualClock:
    """测试用手动推进的逻辑时钟。"""

    def __init__(self, start: int = 0) -> None:
        self._t = start

    def __call__(self) -> int:
        return self._t

    def advance(self, n: int = 1) -> int:
        self._t += n
        return self._t


class Scheduler:
    def __init__(
        self,
        capacity: Resource,
        *,
        default_quota: Resource | None = None,
        priority_min: int = 0,
        priority_max: int = 100,
        aging_weight: int = 1,
        clock: Callable[[], int] | None = None,
    ) -> None:
        if not isinstance(capacity, Resource) or not capacity.all_positive():
            raise InvalidResourceError("节点容量每一维必须为 >= 1 的整数")
        self._capacity = capacity
        self._default_quota = (
            default_quota if default_quota is not None else capacity
        )
        if not self._default_quota.all_non_negative():
            raise InvalidResourceError("默认配额不能含负数")
        if self._default_quota.dimensions != capacity.dimensions:
            raise InvalidResourceError("默认配额维度必须与节点容量一致")
        self._priority_min = priority_min
        self._priority_max = priority_max
        if aging_weight < 0:
            raise InvalidRequestError("aging_weight 不能为负数")
        self._aging_weight = aging_weight

        self._lock = threading.RLock()
        self._jobs: dict[str, Job] = {}
        self._idem: dict[str, str] = {}
        self._queue: deque[str] = deque()
        self._running: set[str] = set()
        self._used = Resource.zero(capacity.dimensions)
        self._submitter_used: dict[str, Resource] = {}
        self._quotas: dict[str, Resource] = {}
        self._seq = 0
        self._clock: ManualClock | Callable[[], int] = clock or ManualClock()
        self._auto_tick_on_submit = clock is None

    # ------------------------------------------------------------------ #
    # 内部小工具
    # ------------------------------------------------------------------ #
    def _now(self) -> int:
        return int(self._clock())

    def _zero(self) -> Resource:
        return Resource.zero(self._capacity.dimensions)

    def _used_of(self, submitter: str) -> Resource:
        return self._submitter_used.get(submitter, self._zero())

    def _quota_of(self, submitter: str) -> Resource:
        return self._quotas.get(submitter, self._default_quota)

    def _validate_request(self, request: object) -> Resource:
        if not isinstance(request, Resource):
            raise InvalidResourceError("request 必须是 Resource 类型")
        if request.dimensions != self._capacity.dimensions:
            raise InvalidResourceError(
                f"资源维度 {request.dimensions} 与节点容量维度 "
                f"{self._capacity.dimensions} 不一致"
            )
        if any(v < 0 for v in request.values):
            raise InvalidResourceError("资源请求不能含负数")
        if not request.all_positive():
            raise InvalidResourceError("资源请求每一维必须 >= 1")
        if not request.fits_in(self._capacity):
            raise ResourceExceedsCapacityError(
                f"资源请求 {request} 超过节点容量 {self._capacity}"
            )
        return request

    def _release(self, job: Job) -> None:
        """释放运行中作业占用的资源。仅允许对 RUNNING 作业调用一次。"""
        if job.state is not JobState.RUNNING or job.id not in self._running:
            raise AssertionError(
                f"内部错误：试图对非 RUNNING 作业 {job.id} 释放资源"
            )
        self._used = self._used - job.request
        cur = self._submitter_used.get(job.submitter, self._zero())
        self._submitter_used[job.submitter] = cur - job.request
        self._running.discard(job.id)

    # ------------------------------------------------------------------ #
    # 提交与查询
    # ------------------------------------------------------------------ #
    def submit(
        self,
        *,
        submitter: str,
        request: Resource,
        priority: int = 0,
        idempotency_key: str | None = None,
    ) -> SubmitResult:
        if not isinstance(submitter, str) or not submitter.strip():
            raise InvalidRequestError("submitter 不能为空")
        if not isinstance(priority, int) or isinstance(priority, bool):
            raise InvalidPriorityError("priority 必须是整数")
        if not (self._priority_min <= priority <= self._priority_max):
            raise InvalidPriorityError(
                f"priority 必须在 [{self._priority_min}, "
                f"{self._priority_max}] 之间，收到 {priority}"
            )
        req = self._validate_request(request)

        with self._lock:
            if idempotency_key is not None:
                existing_id = self._idem.get(idempotency_key)
                if existing_id is not None:
                    existing = self._jobs[existing_id]
                    same = (
                        existing.submitter == submitter
                        and existing.request.values == req.values
                        and existing.priority == priority
                    )
                    if not same:
                        from scheduler.errors import IdempotencyConflictError

                        raise IdempotencyConflictError(
                            f"幂等键 {idempotency_key!r} 已存在但提交参数不一致: "
                            f"已有 submitter={existing.submitter} "
                            f"request={existing.request} priority={existing.priority}"
                        )
                    return SubmitResult(job=existing, duplicated=True)

            self._seq += 1
            if self._auto_tick_on_submit and isinstance(self._clock, ManualClock):
                self._clock.advance()
            now = self._now()
            job_id = f"job-{self._seq:08d}"
            job = Job(
                id=job_id,
                submitter=submitter,
                request=req,
                priority=priority,
                state=JobState.QUEUED,
                submitted_at=now,
                idempotency_key=idempotency_key,
            )
            self._jobs[job_id] = job
            if idempotency_key is not None:
                self._idem[idempotency_key] = job_id
            self._queue.append(job_id)
            self._schedule_locked()
            return SubmitResult(job=job, duplicated=False)

    def get_job(self, job_id: str) -> Job:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFoundError(f"作业 {job_id} 不存在")
            return job

    def list_jobs(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def cancel(self, job_id: str) -> Job:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFoundError(f"作业 {job_id} 不存在")
            job.cancel_requested = True
            if job.state is JobState.CANCELLED:
                # 重复取消是幂等重试，不重复释放资源
                return job
            if job.state in TERMINAL_STATES:
                from scheduler.errors import InvalidJobStateError

                raise InvalidJobStateError(
                    f"作业 {job_id} 已处于终态 {job.state.value}，不能取消"
                )
            if job.state is JobState.QUEUED:
                job.state = JobState.CANCELLED
                job.finished_at = self._now()
                try:
                    self._queue.remove(job_id)
                except ValueError:
                    pass
                return job
            # RUNNING
            self._release(job)
            job.state = JobState.CANCELLED
            job.finished_at = self._now()
            self._schedule_locked()
            return job

    def complete(self, job_id: str) -> Job:
        return self._finish(job_id, JobState.COMPLETED)

    def fail(self, job_id: str) -> Job:
        return self._finish(job_id, JobState.FAILED)

    def _finish(self, job_id: str, target: JobState) -> Job:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFoundError(f"作业 {job_id} 不存在")
            if job.state is target:
                # 重复完成/失败通知是幂等重试，不重复释放资源
                return job
            from scheduler.errors import InvalidJobStateError

            if job.state in TERMINAL_STATES:
                raise InvalidJobStateError(
                    f"作业 {job_id} 已处于终态 {job.state.value}，不能标记为 "
                    f"{target.value}"
                )
            if job.state is JobState.QUEUED:
                raise InvalidJobStateError(
                    f"作业 {job_id} 尚未调度（QUEUED），不能标记为 {target.value}"
                )
            self._release(job)
            job.state = target
            job.finished_at = self._now()
            self._schedule_locked()
            return job

    # ------------------------------------------------------------------ #
    # 配额
    # ------------------------------------------------------------------ #
    def set_quota(self, submitter: str, limit: Resource | None) -> None:
        if not isinstance(submitter, str) or not submitter.strip():
            raise InvalidRequestError("submitter 不能为空")
        with self._lock:
            if limit is None:
                self._quotas.pop(submitter, None)
                return
            if limit.dimensions != self._capacity.dimensions:
                raise InvalidResourceError("配额维度必须与节点容量一致")
            if any(v < 0 for v in limit.values):
                raise InvalidResourceError("配额不能含负数")
            # 不抢占已运行作业：新配额只影响后续调度决策
            self._quotas[submitter] = limit

    # ------------------------------------------------------------------ #
    # 调度与可观测
    # ------------------------------------------------------------------ #
    def schedule(self) -> SchedulerSnapshot:
        with self._lock:
            return self._schedule_locked()

    def _schedule_locked(self) -> SchedulerSnapshot:
        now = self._now()
        queued_ids = [jid for jid in self._queue if self._jobs[jid].state is JobState.QUEUED]

        scored: list[tuple[int, int, str, Job]] = []
        for pos, jid in enumerate(queued_ids):
            job = self._jobs[jid]
            waited = max(0, now - job.submitted_at)
            effective = job.priority + self._aging_weight * waited
            scored.append((-effective, pos, jid, job))
        scored.sort(key=lambda x: (x[0], x[1]))

        decisions: list[QueuedDecision] = []
        admitted: set[str] = set()
        for neg_eff, _pos, jid, job in scored:
            waited = max(0, now - job.submitted_at)
            effective = -neg_eff
            node_available = self._capacity - self._used
            sub_used = self._used_of(job.submitter)
            quota = self._quota_of(job.submitter)
            quota_remaining = quota - sub_used

            node_ok = job.request.fits_in(node_available)
            quota_ok = job.request.fits_in(quota_remaining)

            if node_ok and quota_ok:
                job.state = JobState.RUNNING
                job.scheduled_at = now
                self._used = self._used + job.request
                self._submitter_used[job.submitter] = sub_used + job.request
                self._running.add(jid)
                admitted.add(jid)
                decisions.append(
                    QueuedDecision(
                        job_id=jid,
                        submitter=job.submitter,
                        priority=job.priority,
                        waited=waited,
                        effective_priority=effective,
                        admitted=True,
                        reason=(
                            f"授予资源 request={job.request}；"
                            f"节点剩余 {node_available}，"
                            f"提交方配额剩余 {quota_remaining}"
                        ),
                        request=job.request.values,
                    )
                )
            else:
                reasons = []
                if not node_ok:
                    reasons.append(
                        f"节点容量不足: 需要 {job.request}，"
                        f"当前剩余 {node_available}"
                    )
                if not quota_ok:
                    reasons.append(
                        f"提交方 {job.submitter} 配额不足: 需要 {job.request}，"
                        f"已用 {sub_used}，配额上限 {quota}，剩余 {quota_remaining}"
                    )
                decisions.append(
                    QueuedDecision(
                        job_id=jid,
                        submitter=job.submitter,
                        priority=job.priority,
                        waited=waited,
                        effective_priority=effective,
                        admitted=False,
                        reason="；".join(reasons),
                        request=job.request.values,
                    )
                )

        if admitted:
            self._queue = deque(jid for jid in self._queue if jid not in admitted)
        return self._build_snapshot_locked(decisions)

    def snapshot(self) -> SchedulerSnapshot:
        with self._lock:
            return self._build_snapshot_locked([])

    def _build_snapshot_locked(
        self, decisions: list[QueuedDecision]
    ) -> SchedulerSnapshot:
        available = self._capacity - self._used
        return SchedulerSnapshot(
            tick=self._now(),
            capacity=self._capacity.values,
            used=self._used.values,
            available=available.values,
            running_jobs=sorted(self._running),
            queued_jobs=list(self._queue),
            submitter_used={
                name: used.values for name, used in self._submitter_used.items()
            },
            decisions=decisions,
        )

    def tick(self, n: int = 1) -> int:
        """推进逻辑时钟（仅默认的 ManualClock 支持；返回推进后的时刻）。"""
        with self._lock:
            if not isinstance(self._clock, ManualClock):
                raise InvalidRequestError("注入了外部时钟，请直接操作该时钟对象")
            return self._clock.advance(n)

    # ------------------------------------------------------------------ #
    # 不变量校验（测试用）
    # ------------------------------------------------------------------ #
    def assert_invariants(self) -> None:
        with self._lock:
            dim = self._capacity.dimensions
            # 1) used 非负且不超过容量
            assert self._used.all_non_negative(), f"used 出现负数: {self._used}"
            assert self._used.fits_in(self._capacity), (
                f"used {self._used} 超过容量 {self._capacity}"
            )

            running_jobs = [j for j in self._jobs.values() if j.state is JobState.RUNNING]
            # 2) RUNNING 集合与作业状态一致
            assert {j.id for j in running_jobs} == self._running, "RUNNING 集合不一致"
            # 3) 每个作业至多出现在一个集合里
            for job in self._jobs.values():
                in_queue = job.id in self._queue
                in_running = job.id in self._running
                if job.state is JobState.QUEUED:
                    assert in_queue and not in_running, f"{job.id} 队列状态不一致"
                elif job.state is JobState.RUNNING:
                    assert in_running and not in_queue, f"{job.id} 运行状态不一致"
                else:
                    assert not in_queue and not in_running, (
                        f"{job.id} 终态作业仍占用队列/资源槽"
                    )

            # 4) used == 所有 RUNNING 作业请求之和（资源守恒）
            total = Resource.zero(dim)
            per: dict[str, Resource] = defaultdict(lambda: Resource.zero(dim))
            for job in running_jobs:
                total = total + job.request
                per[job.submitter] = per[job.submitter] + job.request
            assert total.values == self._used.values, (
                f"资源不守恒: 累加 {total} != 记账 {self._used}"
            )
            # 5) submitter_used 记账一致。
            # 注意：配额只在「调度授予」时点保证；配额事后被下调时，
            # 已运行作业不被抢占，用量可能临时高于新配额（非抢占语义）。
            for name, used in self._submitter_used.items():
                assert used.all_non_negative(), f"{name} 用量出现负数"
                assert per.get(name, self._zero()).values == used.values, (
                    f"提交方 {name} 记账不一致: {used} vs {per.get(name)}"
                )
