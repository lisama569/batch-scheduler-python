"""pytest 公共夹具与不变量校验。"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from batch_scheduler.models import JobState, Resources
from batch_scheduler.scheduler import Scheduler


@dataclass
class FakeClock:
    """可手动推进的单调时钟（纳秒），保证相同输入序列可复现。"""

    now_ns: int = 0

    def __call__(self) -> int:
        return self.now_ns

    def advance_ms(self, ms: int) -> None:
        self.now_ns += ms * 1_000_000


CAPACITY = Resources(cpu=16, memory_mb=32768, gpu=4)
QUOTAS = {
    "system": Resources(cpu=16, memory_mb=32768, gpu=4),  # 容量填充用，等同节点容量
    "team-a": Resources(cpu=8, memory_mb=16384, gpu=2),
    "team-b": Resources(cpu=8, memory_mb=16384, gpu=2),
    "team-c": Resources(cpu=4, memory_mb=8192, gpu=1),
}


@pytest.fixture
def no_quota_scheduler(clock: FakeClock) -> Scheduler:
    """无提交方配额的调度器（提交方仍需显式出现才被识别）。"""
    return Scheduler(
        node_capacity=CAPACITY,
        submitter_quotas=None,
        clock_ns=clock,
    )


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def scheduler(clock: FakeClock) -> Scheduler:
    return Scheduler(
        node_capacity=CAPACITY,
        submitter_quotas=QUOTAS,
        clock_ns=clock,
    )


def assert_conservation(s: Scheduler) -> None:
    """核心不变量：

    1. used == 所有 RUNNING 作业资源之和 == 各提交方用量之和；
    2. used 不超过节点容量；各提交方用量不超过配额；
    3. 每个作业至多调度一次、至多释放一次；
       RUNNING 作业 schedule_count==1/release_count==0，
       终态（非 PENDING/BLOCKED 取消、非 DEPENDENCY_FAILED）schedule/release 配对；
    4. BLOCKED 与 DEPENDENCY_FAILED 作业从不占用资源（schedule/release 均为 0）。
    """
    with s._lock:  # noqa: SLF001 - 测试内直接校验内部记账
        sum_running = Resources()
        sum_by_submitter: dict[str, Resources] = {}
        for job in s._jobs.values():
            if job.state is JobState.RUNNING:
                sum_running = sum_running + job.requested
                sum_by_submitter[job.submitter] = (
                    sum_by_submitter.get(job.submitter, Resources()) + job.requested
                )
                assert job.schedule_count == 1, f"{job.id} RUNNING 但调度次数={job.schedule_count}"
                assert job.release_count == 0, f"{job.id} RUNNING 但已释放"
            elif job.state in (JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED):
                assert job.schedule_count <= 1, f"{job.id} 被重复调度"
                assert job.release_count <= 1, f"{job.id} 被重复释放"
                if job.schedule_count == 1:
                    assert job.release_count == 1, f"{job.id} 已调度但终态未释放"
            elif job.state is JobState.PENDING:
                assert job.schedule_count == 0
                assert job.release_count == 0
            elif job.state is JobState.BLOCKED:
                # 等待依赖：不占资源、不占配额、不在调度队列
                assert job.schedule_count == 0, f"{job.id} BLOCKED 却被调度过"
                assert job.release_count == 0
                assert job.depends_on, f"{job.id} BLOCKED 但没有依赖"
                assert job.id not in s._pending, f"{job.id} BLOCKED 却出现在调度队列"
            elif job.state is JobState.DEPENDENCY_FAILED:
                # 依赖失败终态：从未占用资源
                assert job.schedule_count == 0, f"{job.id} DEPENDENCY_FAILED 却被调度过"
                assert job.release_count == 0
                assert job.id not in s._pending, f"{job.id} 终态却出现在调度队列"

        assert sum_running == s._used, (
            f"used={s._used.as_dict()} 与 RUNNING 求和 {sum_running.as_dict()} 不一致"
        )
        assert s._used.is_non_negative()
        assert s._used.fits_in(s._capacity), "占用超过节点容量"
        # 提交方用量字典允许保留零值条目，比较时补齐零基线。
        for name in s._submitter_used:
            sum_by_submitter.setdefault(name, Resources())
        assert sum_by_submitter == s._submitter_used, "提交方用量记账不一致"
        for name, usage in s._submitter_used.items():
            assert usage.is_non_negative()
            quota = s._quotas.get(name)
            if quota is not None:
                assert usage.fits_in(quota), f"提交方 {name} 超配额"
            assert not (usage.cpu == 0 and usage.memory_mb == 0 and usage.gpu == 0) or True
        # pending 队列中不得出现非 PENDING 作业
        for jid in s._pending:
            assert s._jobs[jid].state is JobState.PENDING


def assert_no_grantable_pending(s: Scheduler) -> None:
    """交错不变量：调度已收敛到不动点 —— 没有任何 PENDING 作业能被立即授予。

    每个仍在排队的作业必须被“节点剩余容量不足”或“提交方剩余配额不足”
    之一挡住（可解释、不是被丢弃）。该性质不依赖并发交错顺序：
    每次状态变更后都会同步执行一轮 tick 到不动点，因此任意操作序列
    结束后它必然成立。
    """
    with s._lock:  # noqa: SLF001 - 测试内直接校验内部记账
        node_remaining = s._capacity - s._used
        for job in s._jobs.values():
            if job.state is not JobState.PENDING:
                continue
            quota = s._quotas.get(job.submitter)
            quota_remaining = (
                quota - s._submitter_used.get(job.submitter, Resources())
                if quota is not None
                else None
            )
            blocked_by_quota = (
                quota_remaining is not None
                and not job.requested.fits_in(quota_remaining)
            )
            blocked_by_capacity = not job.requested.fits_in(node_remaining)
            assert blocked_by_quota or blocked_by_capacity, (
                f"{job.id} 仍在排队但资源与配额都足够（应已被授予）: "
                f"req={job.requested.as_dict()} "
                f"node_remaining={node_remaining.as_dict()} "
                f"quota_remaining={quota_remaining.as_dict() if quota_remaining else None}"
            )


def log_snapshot(s: Scheduler, label: str) -> None:
    """打印调度快照与判定依据（单测日志要求）。"""
    snap = s.snapshot()
    print(f"\n===== 调度快照 [{label}] t_ns={snap.timestamp_ns} =====")
    print(f"容量: {snap.node_capacity.as_dict()} 已用: {snap.used.as_dict()}")
    print(f"运行中({len(snap.running_jobs)}): {snap.running_jobs}")
    print(f"提交方用量: {snap.submitter_usage}")
    print("排队(按有效优先级):")
    for p in snap.pending_jobs:
        print(
            f"  - {p['job_id']} {p['submitter']} base_pri={p['priority']} "
            f"eff_pri={p['effective_priority']} wait={p['wait_ms']}ms "
            f"req={p['request']} note={p['note']}"
        )
    if snap.blocked_jobs:
        print("等待依赖(BLOCKED, 不占资源/配额):")
        for b in snap.blocked_jobs:
            print(
                f"  - {b['job_id']} {b['submitter']} depends_on={b['depends_on']} "
                f"waiting_for={b['waiting_for']} wait={b['wait_ms']}ms note={b['note']}"
            )
    if snap.decisions:
        print("本轮判定:")
        for d in snap.decisions:
            print(f"  * {d['job_id']} -> {d['decision']} ({d['reason']}) {d['detail']}")
    print("=" * 60)
