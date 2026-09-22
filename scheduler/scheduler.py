"""调度核心：线程安全的单节点进程内调度器。

调度策略（详见 docs / README）：
- 有效优先级 = 基础优先级 + aging_weight * 等待 tick 数，随等待时间增长防止饿死；
- 每轮调度按 (有效优先级降序, 提交序号升序) 扫描队列，
  同时满足节点容量与提交方配额的作业立即授予资源，否则跳过并记录原因；
- 完成 / 取消 / 失败释放资源，再触发一轮调度（迟到作业自动获得补位机会）。
"""

from __future__ import annotations

import threading
from collections.abc import Callable

from scheduler.models import Job, JobState, Resource, SchedulerSnapshot, SubmitterQuota


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
        raise NotImplementedError

    # ---- 提交与查询 ----
    def submit(
        self,
        *,
        submitter: str,
        request: Resource,
        priority: int = 0,
        idempotency_key: str | None = None,
    ) -> object:
        """提交作业；返回 SubmitResult(job, duplicated)。"""
        raise NotImplementedError

    def get_job(self, job_id: str) -> Job:
        raise NotImplementedError

    def list_jobs(self) -> list[Job]:
        raise NotImplementedError

    # ---- 生命周期 ----
    def cancel(self, job_id: str) -> Job:
        raise NotImplementedError

    def complete(self, job_id: str) -> Job:
        raise NotImplementedError

    def fail(self, job_id: str) -> Job:
        raise NotImplementedError

    # ---- 配额 ----
    def set_quota(self, submitter: str, limit: Resource | None) -> None:
        raise NotImplementedError

    # ---- 调度与可观测 ----
    def schedule(self) -> SchedulerSnapshot:
        """执行一轮调度并返回该轮快照（含每个排队作业的判定依据）。"""
        raise NotImplementedError

    def snapshot(self) -> SchedulerSnapshot:
        raise NotImplementedError

    def tick(self) -> int:
        """推进一个逻辑时间单位（用于等待时长 / 老化测试）。"""
        raise NotImplementedError

    # ---- 不变量校验（测试用） ----
    def assert_invariants(self) -> None:
        """校验资源守恒、作业不重复调度/不重复释放等不变量。"""
        raise NotImplementedError
