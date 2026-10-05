"""并发交错场景测试：提交、取消、完成同时发生。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from batch_scheduler.errors import (
    JobRejectedError,
    JobStateConflictError,
)
from batch_scheduler.models import JobState, Resources, SubmitRequest

from .conftest import assert_conservation, log_snapshot


def _req(i, submitter, cpu=1, priority=0):
    return SubmitRequest(
        idempotency_key=f"k-{submitter}-{i}",
        submitter=submitter,
        priority=priority,
        resources=Resources(cpu=cpu),
        duration_ms=1000,
    )


def test_concurrent_submissions_never_oversell(scheduler):
    # 容量 cpu=16；50 个作业每个 1cpu，并发提交 -> 恰好 16 个运行
    results: dict[int, object] = {}

    def submit(i: int) -> None:
        submitter = ("team-a", "team-b", "team-c")[i % 3]
        results[i] = scheduler.submit(_req(i, submitter, cpu=1))

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(submit, range(50)))

    jobs = scheduler.list_jobs()
    running = [j for j in jobs if j.state is JobState.RUNNING]
    pending = [j for j in jobs if j.state is JobState.PENDING]
    assert len(jobs) == 50
    assert len(running) == 16
    assert len(pending) == 34
    # 容量/额度不够的作业全部在排队等待：没有任何作业被丢弃或提前判成终态
    assert len(running) + len(pending) == len(jobs)
    assert scheduler.used_resources().cpu == 16
    # 提交方维度也不能超配额（8+8+... 节点容量先成为瓶颈）
    log_snapshot(scheduler, "50 作业并发提交后")
    assert_conservation(scheduler)


def test_concurrent_duplicate_keys_single_job(scheduler):
    # 同一幂等键并发提交 20 次：恰好一个作业，其余全部 DUPLICATE
    barrier = threading.Barrier(20)
    outcomes = {"created": 0, "duplicate": 0}
    lock = threading.Lock()

    def submit() -> None:
        barrier.wait()
        try:
            scheduler.submit(_req(1, "team-a", cpu=1))
            with lock:
                outcomes["created"] += 1
        except JobRejectedError as e:
            assert e.reason.value == "DUPLICATE_IDEMPOTENCY_KEY"
            with lock:
                outcomes["duplicate"] += 1

    with ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(lambda _: submit(), range(20)))

    assert outcomes == {"created": 1, "duplicate": 19}
    assert len(scheduler.list_jobs()) == 1
    assert_conservation(scheduler)


def test_concurrent_complete_and_cancel_one_winner(scheduler):
    job = scheduler.submit(_req(1, "team-a", cpu=2))
    barrier = threading.Barrier(2)
    outcomes: list[str] = []
    lock = threading.Lock()

    def complete() -> None:
        barrier.wait()
        try:
            scheduler.complete(job.id)
            with lock:
                outcomes.append("completed")
        except JobStateConflictError:
            with lock:
                outcomes.append("conflict")

    def cancel() -> None:
        barrier.wait()
        try:
            scheduler.cancel(job.id)
            with lock:
                outcomes.append("cancelled")
        except JobStateConflictError:
            with lock:
                outcomes.append("conflict")

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(complete)
        f2 = pool.submit(cancel)
        f1.result(); f2.result()

    final = scheduler.get_job(job.id)
    assert final.state in (JobState.COMPLETED, JobState.CANCELLED)
    assert final.release_count == 1  # 资源只释放一次
    assert len([o for o in outcomes if o != "conflict"]) == 1
    assert scheduler.used_resources() == Resources()
    assert_conservation(scheduler)


# 交错混沌场景的确定性补提方案：
# 预置 24 个 1cpu 作业后，运行中必为 team-a=6 / team-b=6 / team-c=4（提交顺序固定、
# 事件驱动调度无随机），排队池为 a=2 / b=2 / c=4。补提 16 个作业按“排序后的序号
# 取模”固定分配提交方（a:6, b:5, c:5），最终各方可调度池为 a=8 / b=7 / c=9。
# 不变量论证（与交错顺序无关）：每次操作都以 tick 收尾，tick 会扫描到“整轮无新授予”
# 才停，因此任意时刻的稳定态里，凡还有排队作业的提交方要么配额已用满、要么节点已满。
# 若节点未满（used<16），则 a 必须跑满 8、b 的 7 个全部在跑、c 必须跑满 4，
# 合计 19 > 16，矛盾。故最终必然恰好 16 个 RUNNING、8 个 PENDING——
# 该结论不依赖线程交错运气，也不依赖进程级哈希种子。
_CHURN_SUBMITTERS = ("team-a", "team-b", "team-c")


def _run_interleaved_churn(clock) -> dict[str, object]:
    """同一脚本在全新调度器上跑一遍“完成+补提”交错混沌，返回终态摘要。

    摘要是所有合法交错下都必然成立的性质（见上方论证），因此可跨轮次、
    跨进程比较且必须逐位相同。
    """
    from .conftest import CAPACITY, QUOTAS
    from batch_scheduler.scheduler import Scheduler

    s = Scheduler(node_capacity=CAPACITY, submitter_quotas=QUOTAS, clock_ns=clock)
    for i in range(24):
        s.submit(_req(i, _CHURN_SUBMITTERS[i % 3], cpu=1))
    assert s.used_resources().cpu == 16

    running_ids = sorted(
        j.id for j in s.list_jobs() if j.state is JobState.RUNNING
    )
    assert len(running_ids) == 16

    errors: list[BaseException] = []

    def churn(idx_id) -> None:
        # 每个线程：完成一个运行中作业，再补提一个新作业。
        # 提交方按序号确定性分配（严禁 hash()：字符串哈希随进程随机化，
        # 会把“配额池是否够填满节点”变成运气问题）。
        idx, job_id = idx_id
        try:
            s.complete(job_id)
            submitter = _CHURN_SUBMITTERS[idx % 3]
            s.submit(SubmitRequest(f"new-{job_id}", submitter, 2, Resources(cpu=1), 1000))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(churn, list(enumerate(running_ids))))

    assert not errors, errors
    jobs = s.list_jobs()
    by_state: dict[str, int] = {}
    for j in jobs:
        by_state[j.state.value] = by_state.get(j.state.value, 0) + 1
    return {
        "total": len(jobs),
        "by_state": by_state,
        "used_cpu": s.used_resources().cpu,
        "scheduler": s,
    }


def test_interleaved_submit_complete_conservation(scheduler):
    # 预先塞满：24 个 1cpu 作业 -> 16 运行 / 8 排队
    for i in range(24):
        scheduler.submit(_req(i, _CHURN_SUBMITTERS[i % 3], cpu=1))
    assert scheduler.used_resources().cpu == 16
    assert_conservation(scheduler)

    running_ids = sorted(
        j.id for j in scheduler.list_jobs() if j.state is JobState.RUNNING
    )

    errors: list[BaseException] = []

    def churn(idx_id) -> None:
        idx, job_id = idx_id
        try:
            scheduler.complete(job_id)
            submitter = _CHURN_SUBMITTERS[idx % 3]
            scheduler.submit(
                SubmitRequest(f"new-{job_id}", submitter, 2, Resources(cpu=1), 1000)
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(churn, list(enumerate(running_ids))))

    assert not errors, errors
    # 终态必然为：16 完成 + 16 运行 + 8 排队（论证见 _CHURN_SUBMITTERS 注释），
    # 不存在“还在等的作业被判终态”或“作业被丢弃”。
    jobs = scheduler.list_jobs()
    by_state: dict[JobState, list] = {}
    for j in jobs:
        by_state.setdefault(j.state, []).append(j)
    assert len(jobs) == 40
    assert len(by_state[JobState.COMPLETED]) == 16
    assert len(by_state[JobState.RUNNING]) == 16
    assert len(by_state[JobState.PENDING]) == 8
    assert scheduler.used_resources().cpu == 16
    # 排队作业全部带着“等容量/等配额”的可解释原因，而非被丢弃
    for j in by_state[JobState.PENDING]:
        assert j.scheduling_note, f"{j.id} 排队但无可解释原因"
    log_snapshot(scheduler, "交错完成+补提后")
    assert_conservation(scheduler)


def test_interleaved_churn_reproducible_across_rounds():
    # 同一交错混沌脚本在 5 个全新调度器实例上各跑一遍（线程交错每轮都不同），
    # 终态摘要必须逐位相同：结论可复现，不依赖某一轮的调度运气。
    from .conftest import FakeClock

    summaries = []
    for round_no in range(5):
        summary = _run_interleaved_churn(FakeClock())
        s = summary.pop("scheduler")
        summaries.append(summary)
        print(f"\n第 {round_no + 1} 轮终态摘要: {summary}")
        assert_conservation(s)
    assert all(summ == summaries[0] for summ in summaries), summaries
    # 摘要内容本身即“容量/额度边界下的正确行为”：填满但不超卖、余下排队不丢弃
    assert summaries[0] == {
        "total": 40,
        "by_state": {"COMPLETED": 16, "RUNNING": 16, "PENDING": 8},
        "used_cpu": 16,
    }


def test_concurrent_cancel_pending_and_grant(scheduler):
    # 塞满后并发取消排队作业；最终状态必须自洽，不允许“取消后仍被调度”
    for i in range(20):
        scheduler.submit(_req(i, "team-a", cpu=1))
    pending_ids = [j.id for j in scheduler.list_jobs() if j.state is JobState.PENDING]
    assert len(pending_ids) == 12

    def cancel_or_complete(idx_id):
        idx, jid = idx_id
        try:
            if idx % 2 == 0:
                scheduler.cancel(jid)
            else:
                # 对部分排队作业尝试完成 -> 必然状态冲突
                scheduler.complete(jid)
        except JobStateConflictError:
            pass

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(cancel_or_complete, list(enumerate(pending_ids))))

    for jid in pending_ids[::2]:
        j = scheduler.get_job(jid)
        assert j.state is JobState.CANCELLED
        assert j.schedule_count == 0 and j.release_count == 0
    assert_conservation(scheduler)


def test_concurrent_quota_shortage_queues_then_fills(scheduler):
    # team-c 配额 cpu=4：并发提交 12 个 1cpu 作业。
    # 无论提交如何交错，稳定态下 team-c 必用满配额 4（池 12 > 4 且节点有余量），
    # 故必然 4 运行 / 8 排队，且排队者带“等配额”原因、不被丢弃。
    barrier = threading.Barrier(12)

    def submit(i: int) -> None:
        barrier.wait(timeout=10)
        scheduler.submit(_req(i, "team-c", cpu=1))

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(submit, range(12)))

    jobs = scheduler.list_jobs()
    running = [j for j in jobs if j.state is JobState.RUNNING]
    pending = [j for j in jobs if j.state is JobState.PENDING]
    assert len(jobs) == 12
    assert len(running) == 4 and len(pending) == 8
    for j in pending:
        assert "配额" in j.scheduling_note
    print(f"\n配额不足边界: 4 运行 / 8 排队, 排队原因示例: {pending[0].scheduling_note}")
    log_snapshot(scheduler, "team-c 配额 4，12 作业并发提交")
    assert_conservation(scheduler)

    # 并发完成 4 个运行中作业：释放的配额必须被排队作业接住，
    # 终态必然 4 完成 / 4 运行 / 4 排队，无丢失、无提前终态、无重复释放。
    running_ids = [j.id for j in running]
    barrier2 = threading.Barrier(4)

    def complete(jid: str) -> None:
        barrier2.wait(timeout=10)
        scheduler.complete(jid)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(complete, running_ids))

    jobs = scheduler.list_jobs()
    by_state: dict[JobState, int] = {}
    for j in jobs:
        by_state[j.state] = by_state.get(j.state, 0) + 1
    print(f"配额释放后状态分布: { {s.value: n for s, n in by_state.items()} }")
    assert by_state == {JobState.COMPLETED: 4, JobState.RUNNING: 4, JobState.PENDING: 4}
    assert scheduler.submitter_used("team-c").cpu == 4  # 配额记账守恒，不超卖
    log_snapshot(scheduler, "配额释放后被排队作业复用")
    assert_conservation(scheduler)


def test_exact_fit_boundary_grant_vs_queue(scheduler):
    # 边界固定行为：恰好放得下 -> 授予；差一个单位 -> 排队（不拒绝、不丢弃）。
    # 配额恰好到顶：team-a 配额 cpu=8，7 + 1 恰好放满，再来 1 个只能排队。
    a7 = scheduler.submit(_req(7, "team-a", cpu=7))
    a1 = scheduler.submit(_req(1, "team-a", cpu=1))
    over_quota = scheduler.submit(_req(2, "team-a", cpu=1))
    assert a7.state is JobState.RUNNING and a1.state is JobState.RUNNING
    assert over_quota.state is JobState.PENDING
    assert "配额" in over_quota.scheduling_note

    # 节点恰好占满：system 补 8cpu 使节点用量恰好 16，team-b 再要 1cpu 只能排队。
    fill = scheduler.submit(_req(3, "system", cpu=8))
    over_capacity = scheduler.submit(_req(4, "team-b", cpu=1))
    assert fill.state is JobState.RUNNING
    assert scheduler.used_resources().cpu == 16
    assert over_capacity.state is JobState.PENDING
    assert "容量" in over_capacity.scheduling_note
    print(
        f"\n边界行为: 配额到顶排队原因={over_quota.scheduling_note!r}; "
        f"容量到顶排队原因={over_capacity.scheduling_note!r}"
    )
    log_snapshot(scheduler, "恰好放满即授予、差一单位即排队")
    assert_conservation(scheduler)


def test_reject_reasons_are_distinguishable(scheduler):
    from batch_scheduler.models import RejectReason

    cases = []
    with pytest.raises(JobRejectedError) as e1:
        scheduler.submit(_req(0, "team-a", cpu=0))
    cases.append(e1.value.reason)

    with pytest.raises(JobRejectedError) as e2:
        scheduler.submit(_req(1, "team-a", cpu=999))
    cases.append(e2.value.reason)

    scheduler.submit(_req(2, "team-a", cpu=1))
    with pytest.raises(JobRejectedError) as e3:
        scheduler.submit(_req(2, "team-a", cpu=1))
    cases.append(e3.value.reason)

    with pytest.raises(JobRejectedError) as e4:
        scheduler.submit(_req(3, "nobody", cpu=1))
    cases.append(e4.value.reason)

    assert cases == [
        RejectReason.INVALID_REQUEST,
        RejectReason.EXCEEDS_NODE_CAPACITY,
        RejectReason.DUPLICATE_IDEMPOTENCY_KEY,
        RejectReason.UNKNOWN_SUBMITTER,
    ]
    print("\n拒绝原因序列:", [c.value for c in cases])
    assert_conservation(scheduler)
