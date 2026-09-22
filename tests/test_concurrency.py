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


def test_interleaved_submit_complete_conservation(scheduler):
    # 预先塞满：24 个 1cpu 作业 -> 16 运行 / 8 排队
    for i in range(24):
        submitter = ("team-a", "team-b", "team-c")[i % 3]
        scheduler.submit(_req(i, submitter, cpu=1))
    assert scheduler.used_resources().cpu == 16
    assert_conservation(scheduler)

    running_ids = [j.id for j in scheduler.list_jobs() if j.state is JobState.RUNNING]

    errors: list[Exception] = []

    def churn(job_id: str) -> None:
        # 每个线程：完成一个运行中作业，再补提一个新作业
        try:
            scheduler.complete(job_id)
            submitter = ("team-a", "team-b", "team-c")[hash(job_id) % 3]
            tag = f"new-{job_id}"
            scheduler.submit(
                SubmitRequest(tag, submitter, 2, Resources(cpu=1), 1000)
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(churn, running_ids))

    assert not errors, errors
    # 最终：仍然 16 个运行（容量守恒），总作业数 = 24 + 16
    jobs = scheduler.list_jobs()
    running = [j for j in jobs if j.state is JobState.RUNNING]
    assert len(running) == 16
    assert scheduler.used_resources().cpu == 16
    assert len(jobs) == 40
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
