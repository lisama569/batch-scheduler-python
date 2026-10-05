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

from .conftest import (
    assert_conservation,
    assert_no_grantable_pending,
    log_snapshot,
)


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
    assert scheduler.used_resources().cpu == 16
    # 提交方维度也不能超配额（8+8+... 节点容量先成为瓶颈）；
    # 34 个排队作业必须每一个都被容量或配额明确挡住（等待而非丢失）
    assert_no_grantable_pending(scheduler)
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


def test_interleaved_submit_complete_conservation(scheduler):
    # 预先塞满：24 个 1cpu 作业 -> 16 运行 / 8 排队
    for i in range(24):
        submitter = ("team-a", "team-b", "team-c")[i % 3]
        scheduler.submit(_req(i, submitter, cpu=1))
    assert scheduler.used_resources().cpu == 16
    assert_conservation(scheduler)

    running_ids = [j.id for j in scheduler.list_jobs() if j.state is JobState.RUNNING]

    errors: list[Exception] = []

    def churn(idx_id) -> None:
        # 每个线程：完成一个运行中作业，再补提一个新作业。
        # 提交方按枚举序号确定性分配（不得使用 hash()：字符串哈希随进程
        # 随机化，会让新作业的提交方分布随 PYTHONHASHSEED 变化，使
        # “恰好 16 个运行”在某些进程里因配额分布而不成立 —— 曾经的偶发
        # 失败正源于此）。
        idx, job_id = idx_id
        try:
            scheduler.complete(job_id)
            submitter = ("team-a", "team-b", "team-c")[idx % 3]
            tag = f"new-{job_id}"
            scheduler.submit(
                SubmitRequest(tag, submitter, 2, Resources(cpu=1), 1000)
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(churn, list(enumerate(running_ids))))

    assert not errors, errors
    jobs = scheduler.list_jobs()
    assert len(jobs) == 40  # 24 旧 + 16 新，一个都不丢
    assert len([j for j in jobs if j.state is JobState.COMPLETED]) == 16

    # 以下结论在所有合法交错下必然成立（不依赖线程调度运气）：
    # 新作业分布确定为 team-a:6 / team-b:5 / team-c:5，加上旧排队
    # team-a:2 / team-b:2 / team-c:4，候选为 8/7/9；配额 8/8/4。
    # 若 used<16 则必有某提交方既有排队作业又未用满配额（矛盾），
    # 故不动点处恰好 16 个运行、8 个排队。
    running = [j for j in jobs if j.state is JobState.RUNNING]
    pending = [j for j in jobs if j.state is JobState.PENDING]
    assert len(running) == 16
    assert len(pending) == 8
    assert scheduler.used_resources().cpu == 16
    # 排队作业必须都被容量或配额明确挡住（等待而非被丢弃）
    assert_no_grantable_pending(scheduler)
    log_snapshot(scheduler, "交错完成+补提后")
    assert_conservation(scheduler)


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
    # 剩余排队作业仍被配额明确挡住，没有被误判终态、没有被丢弃
    assert_no_grantable_pending(scheduler)
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


# ---------- 可复现性：同一并发交错场景，反复执行结论必须逐位一致 ----------


def _interleaved_scenario_signature() -> dict[str, object]:
    """执行一轮“并发提交 + 完成 + 取消”交错场景，返回终局签名。

    签名只包含在所有合法交错下都必然成立的量（各状态作业数、资源与
    提交方占用），不包含任何依赖线程调度运气的量（例如具体哪些作业
    在运行）。场景内不使用 hash()/随机数，因此签名与进程、与
    PYTHONHASHSEED 无关。
    """
    from batch_scheduler.scheduler import Scheduler
    from .conftest import CAPACITY, QUOTAS, FakeClock

    s = Scheduler(node_capacity=CAPACITY, submitter_quotas=QUOTAS, clock_ns=FakeClock())

    # 1) 顺序塞满：24 个 1cpu 作业 -> 16 运行 / 8 排队（容量与配额双重约束）
    for i in range(24):
        submitter = ("team-a", "team-b", "team-c")[i % 3]
        s.submit(_req(i, submitter, cpu=1))
    # 容量/额度暂时不够的作业：排队等待、不是终态、不被丢弃
    waiting = [j for j in s.list_jobs() if j.state is JobState.PENDING]
    assert len(waiting) == 8
    assert all(not j.state.is_terminal for j in waiting)
    assert_no_grantable_pending(s)

    running_ids = sorted(j.id for j in s.list_jobs() if j.state is JobState.RUNNING)
    cancel_ids = sorted(j.id for j in waiting)[:4]

    errors: list[BaseException] = []

    def do_complete(job_id: str) -> None:
        try:
            s.complete(job_id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def do_cancel(job_id: str) -> None:
        try:
            s.cancel(job_id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def do_submit(idx: int) -> None:
        try:
            submitter = ("team-a", "team-b", "team-c")[idx % 3]
            s.submit(_req(100 + idx, submitter, cpu=1, priority=2))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    # 2) 全部交错：16 个完成 + 4 个取消 + 8 个新提交同时发生
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = []
        futures += [pool.submit(do_complete, jid) for jid in running_ids]
        futures += [pool.submit(do_cancel, jid) for jid in cancel_ids]
        futures += [pool.submit(do_submit, i) for i in range(8)]
        for f in futures:
            f.result()

    assert not errors, errors
    assert_conservation(s)
    assert_no_grantable_pending(s)

    jobs = s.list_jobs()
    state_counts: dict[str, int] = {}
    for j in jobs:
        state_counts[j.state.value] = state_counts.get(j.state.value, 0) + 1
    return {
        "total": len(jobs),
        "state_counts": state_counts,
        "used": s.used_resources().as_dict(),
        "submitter_used": {
            name: s.submitter_used(name).as_dict()
            for name in ("team-a", "team-b", "team-c")
        },
    }


# 该场景的终局签名（推导见下）：与交错顺序无关，每轮必须完全相同。
# - 完成 16 个运行中作业、取消 4 个排队作业：COMPLETED=16, CANCELLED=4；
# - 剩余候选 = 4 个旧排队（a:1,b:1,c:2）+ 8 个新提交（a:3,b:3,c:2）
#   = a:4, b:4, c:4，全部装得下（配额 8/8/4，容量 16）-> 12 个 RUNNING，
#   无 PENDING；team-c 恰好用满配额 4，是“额度暂时不够就排队”的边界。
_EXPECTED_SIGNATURE = {
    "total": 32,
    "state_counts": {"COMPLETED": 16, "CANCELLED": 4, "RUNNING": 12},
    "used": {"cpu": 12, "memory_mb": 0, "gpu": 0},
    "submitter_used": {
        "team-a": {"cpu": 4, "memory_mb": 0, "gpu": 0},
        "team-b": {"cpu": 4, "memory_mb": 0, "gpu": 0},
        "team-c": {"cpu": 4, "memory_mb": 0, "gpu": 0},
    },
}


def test_repeated_interleaved_scenario_converges_to_same_signature():
    rounds = 20
    signatures = [_interleaved_scenario_signature() for _ in range(rounds)]
    for i, sig in enumerate(signatures):
        print(f"\n第 {i + 1} 轮终局签名: {sig}")
    assert all(sig == signatures[0] for sig in signatures), (
        "同一场景多轮执行的终局签名不一致："
        + "\n".join(f"round {i}: {s}" for i, s in enumerate(signatures))
    )
    assert signatures[0] == _EXPECTED_SIGNATURE
