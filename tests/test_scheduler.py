"""调度器核心行为单测。"""

from __future__ import annotations

import pytest

from batch_scheduler.errors import (
    JobNotFoundError,
    JobRejectedError,
    JobStateConflictError,
)
from batch_scheduler.models import JobState, RejectReason, Resources, SubmitRequest

from .conftest import assert_conservation, log_snapshot


def req(key, submitter="team-a", priority=0, cpu=0, mem=0, gpu=0, duration=1000):
    return SubmitRequest(
        idempotency_key=key,
        submitter=submitter,
        priority=priority,
        resources=Resources(cpu=cpu, memory_mb=mem, gpu=gpu),
        duration_ms=duration,
    )


def expect_reject(scheduler, request, reason):
    with pytest.raises(JobRejectedError) as ei:
        scheduler.submit(request)
    assert ei.value.reason is reason
    return ei.value


# ---------- 提交校验与可区分拒绝原因 ----------

def test_valid_job_runs_immediately_when_capacity_available(scheduler, clock):
    job = scheduler.submit(req("k1", cpu=4, mem=1024))
    assert job.state is JobState.RUNNING
    assert job.schedule_count == 1
    assert scheduler.used_resources() == Resources(cpu=4, memory_mb=1024)
    log_snapshot(scheduler, "单作业立即调度")
    assert_conservation(scheduler)


def test_duplicate_idempotency_key_returns_distinguishable_reason(scheduler):
    first = scheduler.submit(req("dup", cpu=1))
    err = expect_reject(scheduler, req("dup", cpu=2), RejectReason.DUPLICATE_IDEMPOTENCY_KEY)
    assert err.existing_job.id == first.id  # type: ignore[attr-defined]
    assert len(scheduler.list_jobs()) == 1  # 不产生重复作业
    assert_conservation(scheduler)


def test_zero_resource_request_rejected(scheduler):
    expect_reject(scheduler, req("zero"), RejectReason.INVALID_REQUEST)


def test_negative_values_rejected(scheduler):
    expect_reject(
        scheduler,
        SubmitRequest("neg", "team-a", 0, Resources(cpu=-1, memory_mb=10)),
        RejectReason.INVALID_REQUEST,
    )


def test_blank_fields_rejected(scheduler):
    expect_reject(
        scheduler, req("", cpu=1), RejectReason.INVALID_REQUEST
    )
    expect_reject(
        scheduler, req("k", submitter="  "), RejectReason.INVALID_REQUEST
    )


def test_unknown_submitter_rejected(scheduler):
    expect_reject(
        scheduler, req("k", submitter="ghost", cpu=1),
        RejectReason.UNKNOWN_SUBMITTER,
    )


def test_request_exceeding_node_capacity_rejected(scheduler, clock):
    err = expect_reject(
        scheduler, req("huge", cpu=999, mem=999_999, gpu=99),
        RejectReason.EXCEEDS_NODE_CAPACITY,
    )
    assert "节点容量" in str(err)
    assert len(scheduler.list_jobs()) == 0
    log_snapshot(scheduler, "超额请求被拒")


def test_request_exceeding_own_quota_rejected_as_invalid(scheduler):
    # team-c 配额 cpu=4：单作业要 5 cpu 永远无法满足 -> INVALID_REQUEST
    expect_reject(
        scheduler, req("over-quota", "team-c", cpu=5),
        RejectReason.INVALID_REQUEST,
    )


def test_temporary_quota_excess_queues_not_drops(scheduler, clock):
    # team-a 配额 cpu=8：先占 6，再申请 4（不超过配额上限，但当前放不下）-> 排队
    scheduler.submit(req("a1", "team-a", cpu=6))
    queued = scheduler.submit(req("a2", "team-a", cpu=4))
    assert queued.state is JobState.PENDING
    assert "配额" in queued.scheduling_note
    log_snapshot(scheduler, "暂时超额 -> 排队")
    assert_conservation(scheduler)


# ---------- 调度顺序：优先级 + 等待时间 ----------

def test_priority_order_when_capacity_frees(scheduler, clock):
    # 占满节点 cpu=16；base 归 team-b，排队作业归 team-a（同提交方，只比优先级）
    scheduler.submit(req("base", "system", cpu=16))
    low = scheduler.submit(req("low", "team-a", priority=1, cpu=8))
    clock.advance_ms(10)
    high = scheduler.submit(req("high", "team-a", priority=9, cpu=8))
    assert low.state is JobState.PENDING and high.state is JobState.PENDING

    snap = scheduler.snapshot()
    log_snapshot(scheduler, "高低优先级共同排队")
    order = [p["job_id"] for p in snap.pending_jobs]
    assert order[0] == high.id

    scheduler.complete("job-000001")
    assert scheduler.get_job(high.id).state is JobState.RUNNING
    assert scheduler.get_job(low.id).state is JobState.PENDING
    log_snapshot(scheduler, "容量释放后高优先级先上")
    assert_conservation(scheduler)


def test_aging_prevents_starvation(scheduler, clock):
    # 低优先级先等很久，高优先级后到；等待加分应使低优先级最终反超。
    scheduler.submit(req("base", "system", cpu=16))
    old_low = scheduler.submit(req("old-low", "team-a", priority=1, cpu=8))
    clock.advance_ms(10_000)  # 低优先级已等 10 秒 = +10 有效优先级
    new_high = scheduler.submit(req("new-high", "team-a", priority=5, cpu=8))

    snap = scheduler.snapshot()
    log_snapshot(scheduler, "等待时间加分反超静态优先级")
    order = [p["job_id"] for p in snap.pending_jobs]
    assert order[0] == old_low.id  # 1+10 > 5+0

    scheduler.complete("job-000001")
    assert scheduler.get_job(old_low.id).state is JobState.RUNNING
    assert scheduler.get_job(new_high.id).state is JobState.PENDING
    assert_conservation(scheduler)


def test_deterministic_schedule_for_same_sequence(scheduler, clock):
    scheduler.submit(req("base", "system", cpu=16))
    scheduler.submit(req("j1", "team-a", priority=3, cpu=1))
    clock.advance_ms(5)
    scheduler.submit(req("j2", "team-a", priority=3, cpu=1))

    s1 = scheduler.snapshot()
    clock.advance_ms(2)
    s2 = scheduler.snapshot()
    ids1 = [p["job_id"] for p in s1.pending_jobs]
    ids2 = [p["job_id"] for p in s2.pending_jobs]
    assert ids1 == ids2  # 同分按到达顺序，可复现


# ---------- 配额边界 ----------

def test_submitter_quota_boundary_enforced(scheduler, clock):
    scheduler.submit(req("a-6", "team-a", cpu=6))
    scheduler.submit(req("a-2", "team-a", cpu=2))  # 正好到配额 8
    other = scheduler.submit(req("b-8", "team-b", cpu=2))
    over = scheduler.submit(req("a-more", "team-a", cpu=1))
    # 节点还剩 cpu=6，但 team-a 已到配额；team-b 的作业应照常被调度
    assert scheduler.get_job(other.id).state is JobState.RUNNING
    assert scheduler.get_job(over.id).state is JobState.PENDING
    log_snapshot(scheduler, "配额到顶不影响其他提交方")
    assert_conservation(scheduler)


def test_quota_released_and_reused_after_completion(scheduler, clock):
    scheduler.submit(req("a1", "team-a", cpu=8))
    waiter = scheduler.submit(req("a2", "team-a", cpu=4))
    assert waiter.state is JobState.PENDING
    scheduler.complete("job-000001")
    assert scheduler.get_job(waiter.id).state is JobState.RUNNING
    assert scheduler.used_resources() == Resources(cpu=4)
    assert_conservation(scheduler)


# ---------- 取消 / 完成竞争与重复操作 ----------

def test_cancel_pending_job_releases_no_resource(scheduler):
    scheduler.submit(req("base", "system", cpu=16))
    waiter = scheduler.submit(req("waiter", "team-a", cpu=2))
    cancelled = scheduler.cancel(waiter.id)
    assert cancelled.state is JobState.CANCELLED
    assert cancelled.release_count == 0
    with pytest.raises(JobStateConflictError):
        scheduler.cancel(waiter.id)
    with pytest.raises(JobStateConflictError):
        scheduler.complete(waiter.id)
    assert_conservation(scheduler)


def test_cancel_running_job_releases_and_schedules_next(scheduler, clock):
    scheduler.submit(req("base", "system", cpu=10))
    waiter = scheduler.submit(req("waiter", "team-a", cpu=8))
    assert waiter.state is JobState.PENDING

    cancelled = scheduler.cancel("job-000001")
    assert cancelled.state is JobState.CANCELLED
    assert cancelled.release_count == 1
    assert scheduler.get_job(waiter.id).state is JobState.RUNNING
    log_snapshot(scheduler, "取消运行中作业触发资源复用")
    assert_conservation(scheduler)


def test_complete_is_idempotent_guard(scheduler):
    scheduler.submit(req("j", cpu=2))
    scheduler.complete("job-000001")
    with pytest.raises(JobStateConflictError):
        scheduler.complete("job-000001")
    with pytest.raises(JobStateConflictError):
        scheduler.complete("job-000001", failed=True)
    assert scheduler.used_resources() == Resources()
    assert_conservation(scheduler)


def test_fail_releases_resources(scheduler):
    scheduler.submit(req("ok", cpu=3))
    scheduler.submit(req("boom", cpu=3))
    failed = scheduler.complete("job-000002", failed=True)
    assert failed.state is JobState.FAILED
    assert failed.release_count == 1
    assert scheduler.used_resources() == Resources(cpu=3)
    assert_conservation(scheduler)


def test_cancel_then_complete_race_resolves_once(scheduler):
    scheduler.submit(req("j", cpu=2))
    scheduler.cancel("job-000001")
    with pytest.raises(JobStateConflictError):
        scheduler.complete("job-000001")
    assert_conservation(scheduler)


def test_unknown_job_operations_raise_not_found(scheduler):
    with pytest.raises(JobNotFoundError):
        scheduler.get_job("nope")
    with pytest.raises(JobNotFoundError):
        scheduler.cancel("nope")
    with pytest.raises(JobNotFoundError):
        scheduler.complete("nope")


def test_fragment_packing_within_single_tick(scheduler, clock):
    # anchor(2) 常驻，hold(14) 占满剩余；释放 hold 后节点空闲 14，
    # 两个 8cpu 作业（team-a/team-b 各一个）只能放下一个：2+8+8=18>16。
    scheduler.submit(req("anchor", "system", cpu=2))
    scheduler.submit(req("hold", "system", cpu=14))
    j1 = scheduler.submit(req("p1", "team-a", priority=1, cpu=8))
    clock.advance_ms(1)
    j2 = scheduler.submit(req("p2", "team-b", priority=1, cpu=8))
    assert j1.state is JobState.PENDING and j2.state is JobState.PENDING
    scheduler.complete("job-000002")  # 释放 hold
    assert scheduler.get_job(j1.id).state is JobState.RUNNING
    assert scheduler.get_job(j2.id).state is JobState.PENDING
    assert scheduler.used_resources().cpu == 10  # anchor 2 + p1 8
    log_snapshot(scheduler, "碎片容量只够一个大作业")
    assert_conservation(scheduler)


def test_aging_bounds_waiting_time(scheduler, clock):
    """真正的反饿死：anchor 占满 team-a 配额(8cpu)，释放后配额成为唯一瓶颈，
    4cpu 槽位每轮只够一个作业，老化保证老作业最终排到最前。"""
    scheduler.submit(req("anchor", "team-a", priority=0, cpu=8))
    victim = scheduler.submit(req("a-victim", "team-a", priority=0, cpu=4))
    clock.advance_ms(100)
    for i in range(3):
        scheduler.submit(req(f"a-burst-{i}", "team-a", priority=10, cpu=4))
        clock.advance_ms(100)

    # anchor 占满配额，所有排队作业都在等
    assert scheduler.get_job(victim.id).state is JobState.PENDING

    # 释放 anchor：配额槽位只有 4cpu，一轮只放一个。
    # victim 已等 0.4 秒，eff=0.4，burst eff≈10 -> burst 先上，victim 继续等。
    scheduler.complete("job-000001")
    assert scheduler.get_job(victim.id).state is JobState.PENDING
    first = next(
        j for j in scheduler.list_jobs()
        if j.state is JobState.RUNNING and j.submitter == "team-a"
    )
    assert first.idempotency_key == "a-burst-0"
    log_snapshot(scheduler, "配额槽位被高优先级 burst 取得")

    # 此刻还有一个 burst 与 victim 排队。时间推进超过 10 秒，
    # victim eff≈10.4 反超 burst 的静态 10。
    clock.advance_ms(11_000)
    # 排空正在运行的两个 burst，每完成一个触发一次调度；
    # 槽位全部释放的那一轮，victim 有效优先级最高 -> victim 上。
    running_ids = [
        j.id for j in scheduler.list_jobs() if j.state is JobState.RUNNING
    ]
    for jid in running_ids:
        scheduler.complete(jid)
    assert scheduler.get_job(victim.id).state is JobState.RUNNING
    log_snapshot(scheduler, "老作业靠老化取得配额槽位")
    assert_conservation(scheduler)
