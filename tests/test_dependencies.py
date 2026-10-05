"""作业依赖能力单测：阻塞/放行/失败传播/非法依赖拒绝/并发交错。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from batch_scheduler.errors import JobRejectedError, JobStateConflictError
from batch_scheduler.models import JobState, RejectReason, Resources, SubmitRequest
from batch_scheduler.scheduler import Scheduler, _would_create_cycle

from .conftest import CAPACITY, QUOTAS, assert_conservation, log_snapshot


def req(key, submitter="team-a", priority=0, cpu=0, mem=0, gpu=0,
        duration=1000, depends_on=None):
    return SubmitRequest(
        idempotency_key=key,
        submitter=submitter,
        priority=priority,
        resources=Resources(cpu=cpu, memory_mb=mem, gpu=gpu),
        duration_ms=duration,
        depends_on=list(depends_on or []),
    )


def expect_reject(scheduler, request, reason):
    with pytest.raises(JobRejectedError) as ei:
        scheduler.submit(request)
    assert ei.value.reason is reason
    return ei.value


# ---------- 依赖就绪前：BLOCKED 不占资源、不占配额、不影响他人 ----------

def test_blocked_until_all_dependencies_completed(scheduler, clock):
    up1 = scheduler.submit(req("up1", cpu=2))
    up2 = scheduler.submit(req("up2", cpu=2))
    assert up1.state is JobState.RUNNING and up2.state is JobState.RUNNING

    down = scheduler.submit(req("down", cpu=1, depends_on=[up1.id, up2.id]))
    print(f"\n下游提交后状态: {down.state.value}, note={down.scheduling_note}")
    assert down.state is JobState.BLOCKED
    # 不占节点容量：已用只有两个上游
    assert scheduler.used_resources() == Resources(cpu=4)
    # 不占提交方配额
    assert scheduler.submitter_used("team-a") == Resources(cpu=4)
    # 不进调度队列，但能在快照的 blocked 视图中看到可解释信息
    snap = scheduler.snapshot()
    assert [b["job_id"] for b in snap.blocked_jobs] == [down.id]
    assert snap.blocked_jobs[0]["waiting_for"] == [up1.id, up2.id]
    assert all(p["job_id"] != down.id for p in snap.pending_jobs)
    log_snapshot(scheduler, "两个上游运行中，下游 BLOCKED")
    assert_conservation(scheduler)

    # 只完成一个上游：依赖未全部就绪，仍然 BLOCKED
    clock.advance_ms(10)
    scheduler.complete(up1.id)
    mid = scheduler.get_job(down.id)
    print(f"上游1完成后下游状态: {mid.state.value}, note={mid.scheduling_note}")
    assert mid.state is JobState.BLOCKED
    log_snapshot(scheduler, "上游1完成，上游2未完成，下游仍 BLOCKED")

    # 全部上游成功：放行并立即获得资源
    scheduler.complete(up2.id)
    done = scheduler.get_job(down.id)
    print(f"全部上游完成后下游状态: {done.state.value}, note={done.scheduling_note}")
    assert done.state is JobState.RUNNING
    assert done.schedule_count == 1
    assert scheduler.used_resources() == Resources(cpu=1)
    log_snapshot(scheduler, "全部上游完成，下游放行运行")
    assert_conservation(scheduler)


def test_blocked_job_does_not_affect_other_jobs_scheduling(scheduler):
    # 上游占 team-a 全部配额；高优先级下游被依赖挡住；
    # 另一个低优先级、无依赖的 team-b 作业应照常调度，不受 BLOCKED 作业影响。
    up = scheduler.submit(req("up", submitter="team-a", cpu=8))
    assert up.state is JobState.RUNNING
    down = scheduler.submit(
        req("down", submitter="team-a", priority=99, cpu=4, depends_on=[up.id])
    )
    assert down.state is JobState.BLOCKED

    other = scheduler.submit(req("other", submitter="team-b", priority=0, cpu=8))
    print(f"\n无关作业状态: {other.state.value}（BLOCKED 高优先级作业不得挤占它）")
    assert other.state is JobState.RUNNING
    assert scheduler.used_resources() == Resources(cpu=16)
    log_snapshot(scheduler, "BLOCKED 不影响其它作业排队")
    assert_conservation(scheduler)


def test_single_upstream_multiple_dependents(scheduler):
    up = scheduler.submit(req("up", cpu=1))
    d1 = scheduler.submit(req("d1", cpu=1, depends_on=[up.id]))
    d2 = scheduler.submit(req("d2", cpu=1, depends_on=[up.id]))
    assert d1.state is JobState.BLOCKED and d2.state is JobState.BLOCKED

    scheduler.complete(up.id)
    assert scheduler.get_job(d1.id).state is JobState.RUNNING
    assert scheduler.get_job(d2.id).state is JobState.RUNNING
    log_snapshot(scheduler, "一个上游放行多个下游")
    assert_conservation(scheduler)


def test_submit_after_dependencies_already_completed(scheduler):
    up = scheduler.submit(req("up", cpu=1))
    scheduler.complete(up.id)
    down = scheduler.submit(req("down", cpu=1, depends_on=[up.id]))
    print(f"\n依赖提交前已就绪，下游直接调度: {down.state.value}")
    assert down.state is JobState.RUNNING
    assert_conservation(scheduler)


# ---------- 上游失败 / 取消的传播 ----------

def test_upstream_failure_propagates_transitively(scheduler):
    a = scheduler.submit(req("a", cpu=1))
    b = scheduler.submit(req("b", cpu=1, depends_on=[a.id]))
    c = scheduler.submit(req("c", cpu=1, depends_on=[b.id]))
    assert b.state is JobState.BLOCKED and c.state is JobState.BLOCKED

    scheduler.complete(a.id, failed=True)
    b_now = scheduler.get_job(b.id)
    c_now = scheduler.get_job(c.id)
    print(f"\n上游失败后: b={b_now.state.value} note={b_now.scheduling_note}")
    print(f"传递传播:     c={c_now.state.value} note={c_now.scheduling_note}")
    assert b_now.state is JobState.DEPENDENCY_FAILED
    assert c_now.state is JobState.DEPENDENCY_FAILED
    assert b_now.state.is_terminal and c_now.state.is_terminal
    assert a.id in b_now.scheduling_note  # 查询结果能解释原因
    assert scheduler.used_resources() == Resources()
    log_snapshot(scheduler, "失败沿依赖链传递传播")
    assert_conservation(scheduler)


def test_upstream_cancel_propagates_from_running_and_pending(scheduler):
    # 情形1：RUNNING 上游被取消
    a = scheduler.submit(req("a", cpu=1))
    b = scheduler.submit(req("b", cpu=1, depends_on=[a.id]))
    scheduler.cancel(a.id)
    assert scheduler.get_job(b.id).state is JobState.DEPENDENCY_FAILED

    # 情形2：PENDING 上游（容量不足排队中）被取消
    filler = scheduler.submit(req("filler", submitter="system", cpu=16))
    assert filler.state is JobState.RUNNING
    p = scheduler.submit(req("p", submitter="team-b", cpu=4))
    assert p.state is JobState.PENDING  # 容量不足，排队
    q = scheduler.submit(req("q", submitter="team-b", cpu=1, depends_on=[p.id]))
    assert q.state is JobState.BLOCKED
    scheduler.cancel(p.id)
    q_now = scheduler.get_job(q.id)
    print(f"\nPENDING 上游被取消后下游: {q_now.state.value} note={q_now.scheduling_note}")
    assert q_now.state is JobState.DEPENDENCY_FAILED
    log_snapshot(scheduler, "取消传播（RUNNING/PENDING 两种上游）")
    assert_conservation(scheduler)


def test_submit_with_already_failed_upstream_terminates_immediately(scheduler):
    a = scheduler.submit(req("a", cpu=1))
    scheduler.complete(a.id, failed=True)
    down = scheduler.submit(req("down", cpu=1, depends_on=[a.id]))
    print(f"\n提交时上游已失败: {down.state.value} note={down.scheduling_note}")
    assert down.state is JobState.DEPENDENCY_FAILED
    assert down.state.is_terminal
    assert down.schedule_count == 0 and down.release_count == 0
    assert scheduler.used_resources() == Resources()
    assert_conservation(scheduler)


def test_blocked_dependent_cancelled_by_user(scheduler):
    up = scheduler.submit(req("up", cpu=1))
    b = scheduler.submit(req("b", cpu=1, depends_on=[up.id]))
    c = scheduler.submit(req("c", cpu=1, depends_on=[b.id]))

    cancelled = scheduler.cancel(b.id)
    print(f"\nBLOCKED 作业被取消: {cancelled.state.value} note={cancelled.scheduling_note}")
    assert cancelled.state is JobState.CANCELLED
    assert cancelled.release_count == 0  # 从未占资源
    # 被取消的上游同样触发传播
    assert scheduler.get_job(c.id).state is JobState.DEPENDENCY_FAILED
    assert_conservation(scheduler)


def test_dependency_terminal_states_reject_further_transitions(scheduler):
    a = scheduler.submit(req("a", cpu=1))
    b = scheduler.submit(req("b", cpu=1, depends_on=[a.id]))
    scheduler.complete(a.id, failed=True)
    assert scheduler.get_job(b.id).state is JobState.DEPENDENCY_FAILED

    with pytest.raises(JobStateConflictError):
        scheduler.cancel(b.id)  # 终态不可取消
    with pytest.raises(JobStateConflictError):
        scheduler.complete(b.id)  # 非 RUNNING 不可完成
    # BLOCKED 作业也不能被“完成”
    up2 = scheduler.submit(req("up2", cpu=1))
    c = scheduler.submit(req("c", cpu=1, depends_on=[up2.id]))
    with pytest.raises(JobStateConflictError):
        scheduler.complete(c.id)
    assert_conservation(scheduler)


# ---------- 非法依赖：提交当下可区分拒绝 ----------

def test_unknown_dependency_rejected(scheduler):
    err = expect_reject(
        scheduler, req("bad", cpu=1, depends_on=["job-999999"]),
        RejectReason.UNKNOWN_DEPENDENCY,
    )
    print(f"\n未知依赖拒绝: {err}")
    assert scheduler.list_jobs() == []  # 被拒作业不落库、不进队列
    assert_conservation(scheduler)


def test_self_dependency_rejected(scheduler):
    # 作业 ID 按序生成，首个作业必为 job-000001：依赖它即依赖自己
    err = expect_reject(
        scheduler, req("self", cpu=1, depends_on=["job-000001"]),
        RejectReason.SELF_DEPENDENCY,
    )
    print(f"\n自依赖拒绝: {err}")
    assert scheduler.list_jobs() == []
    assert_conservation(scheduler)


def test_invalid_depends_on_entries_rejected(scheduler):
    expect_reject(
        scheduler, req("blank", cpu=1, depends_on=["  "]),
        RejectReason.INVALID_REQUEST,
    )
    expect_reject(
        scheduler,
        SubmitRequest("nonstr", "team-a", 0, Resources(cpu=1), 1000, [1]),
        RejectReason.INVALID_REQUEST,
    )
    assert scheduler.list_jobs() == []


def test_cycle_detection_pure_function():
    # 依赖只能指向已存在作业，API 层面天然无环；环检测作为防御逻辑独立单测。
    # 无环：链式与菱形
    assert not _would_create_cycle({"a": [], "b": ["a"]}, "c", ["b"])
    assert not _would_create_cycle({"a": [], "b": ["a"], "c": ["a"]}, "d", ["b", "c"])
    # 直接自环
    assert _would_create_cycle({}, "a", ["a"])
    # 间接环：c 依赖 a，而 a->b->c 已存在
    assert _would_create_cycle({"a": ["b"], "b": ["c"]}, "c", ["a"])
    # 更长的间接环
    assert _would_create_cycle({"a": ["b"], "b": ["c"], "c": ["d"]}, "d", ["a"])


def test_rejected_dependency_does_not_consume_idempotency_key_or_quota(scheduler):
    expect_reject(
        scheduler, req("k", cpu=1, depends_on=["job-999999"]),
        RejectReason.UNKNOWN_DEPENDENCY,
    )
    # 同一幂等键修正依赖后可重新提交（拒绝不产生任何登记）
    up = scheduler.submit(req("up", cpu=1))
    ok = scheduler.submit(req("k", cpu=1, depends_on=[up.id]))
    assert ok.state is JobState.BLOCKED
    assert_conservation(scheduler)


# ---------- 依赖就绪后仍按配额/容量排队（不丢弃） ----------

def test_ready_dependent_still_queues_when_quota_short(scheduler, clock):
    up = scheduler.submit(req("up", submitter="team-a", cpu=4))
    other = scheduler.submit(req("other", submitter="team-a", cpu=4))
    assert up.state is JobState.RUNNING and other.state is JobState.RUNNING
    # team-a 配额 cpu=8 已用满；下游就绪后也需等配额
    down = scheduler.submit(req("down", submitter="team-a", cpu=8, depends_on=[up.id]))
    assert down.state is JobState.BLOCKED

    scheduler.complete(up.id)  # 依赖就绪，但 team-a 剩余配额 4 < 8
    waiting = scheduler.get_job(down.id)
    print(f"\n依赖就绪但配额不足: {waiting.state.value} note={waiting.scheduling_note}")
    assert waiting.state is JobState.PENDING  # 排队而不是被丢弃
    assert "配额" in waiting.scheduling_note
    log_snapshot(scheduler, "依赖就绪但配额不足，正常排队")

    scheduler.complete(other.id)  # 配额释放后应被调度
    assert scheduler.get_job(down.id).state is JobState.RUNNING
    assert_conservation(scheduler)


# ---------- 确定性：同样输入顺序复现同样结论 ----------

def _run_dependency_script(clock) -> list[tuple[str, str]]:
    s = Scheduler(node_capacity=CAPACITY, submitter_quotas=QUOTAS, clock_ns=clock)
    a = s.submit(req("a", cpu=4))
    b = s.submit(req("b", cpu=4, depends_on=[a.id]))
    c = s.submit(req("c", submitter="team-b", cpu=4, depends_on=[a.id]))
    d = s.submit(req("d", submitter="team-b", cpu=4, depends_on=[b.id, c.id]))
    clock.advance_ms(5)
    s.complete(a.id)
    clock.advance_ms(5)
    s.complete(b.id, failed=True)
    s.cancel(c.id)
    return [(j.idempotency_key, j.state.value) for j in s.list_jobs()]


def test_same_input_sequence_reproduces_same_outcome():
    from .conftest import FakeClock

    first = _run_dependency_script(FakeClock())
    second = _run_dependency_script(FakeClock())
    print(f"\n第一次: {first}\n第二次: {second}")
    assert first == second
    # 结论本身也符合预期：b/c 一败一取消，d 传递失败
    assert dict(first)["d"] == JobState.DEPENDENCY_FAILED.value


# ---------- 并发交错：依赖提交与取消/完成同时发生 ----------

def test_concurrent_dependents_with_upstream_failure(scheduler):
    up = scheduler.submit(req("up", cpu=1))
    barrier = threading.Barrier(9)
    errors: list[BaseException] = []

    def submit_dependent(i: int) -> None:
        try:
            barrier.wait(timeout=10)
            scheduler.submit(req(f"dep-{i}", cpu=1, depends_on=[up.id]))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(submit_dependent, i) for i in range(8)]
        barrier.wait(timeout=10)
        scheduler.complete(up.id, failed=True)  # 与并发提交交错
        for f in futures:
            f.result()

    assert not errors
    jobs = {j.idempotency_key: j for j in scheduler.list_jobs()}
    states = {k: j.state for k, j in jobs.items() if k.startswith("dep-")}
    print(f"\n交错后 8 个下游状态: {{...}} 全部={set(states.values())}")
    # 无论提交与失败回报如何交错：提交时上游已败 -> 立即 DEPENDENCY_FAILED；
    # 提交时仍 BLOCKED -> 被传播为 DEPENDENCY_FAILED。殊途同归，全部终态。
    assert len(states) == 8
    assert set(states.values()) == {JobState.DEPENDENCY_FAILED}
    assert scheduler.used_resources() == Resources()
    log_snapshot(scheduler, "并发下游提交 x 上游失败")
    assert_conservation(scheduler)


def test_concurrent_dependents_with_upstream_success(scheduler):
    up = scheduler.submit(req("up", cpu=1))
    barrier = threading.Barrier(9)
    errors: list[BaseException] = []

    def submit_dependent(i: int) -> None:
        try:
            barrier.wait(timeout=10)
            scheduler.submit(req(f"dep-{i}", cpu=1, depends_on=[up.id]))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(submit_dependent, i) for i in range(8)]
        barrier.wait(timeout=10)
        scheduler.complete(up.id)  # 成功：与并发提交交错
        for f in futures:
            f.result()

    assert not errors
    states = [
        j.state for j in scheduler.list_jobs() if j.idempotency_key.startswith("dep-")
    ]
    print(f"\n交错后 8 个下游状态集合: {set(states)}")
    # 容量充足（各 1cpu）：无论交错顺序如何，最终全部放行运行
    assert set(states) == {JobState.RUNNING}
    log_snapshot(scheduler, "并发下游提交 x 上游成功")
    assert_conservation(scheduler)


def test_concurrent_submit_cancel_complete_interleaved(scheduler):
    # 上游成功/取消与下游提交、下游自身取消全部交错：只断言所有交错下都成立的结论。
    up_ok = scheduler.submit(req("up-ok", cpu=1))
    up_cancel = scheduler.submit(req("up-cancel", submitter="team-b", cpu=1))
    errors: list[BaseException] = []

    def submit_dependent(i: int) -> None:
        try:
            scheduler.submit(req(f"dep-{i}", cpu=1, depends_on=[up_ok.id]))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def cancel_upstream() -> None:
        try:
            scheduler.cancel(up_cancel.id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(submit_dependent, i) for i in range(10)]
        futures.append(pool.submit(cancel_upstream))
        futures.append(pool.submit(scheduler.complete, up_ok.id))
        for f in futures:
            f.result()

    assert not errors
    deps = [j for j in scheduler.list_jobs() if j.idempotency_key.startswith("dep-")]
    print(f"\n交错后 10 个下游状态集合: {set(j.state for j in deps)}")
    # 上游 up_ok 已成功：下游要么直接就绪、要么被唤醒，全部放行；
    # team-a 配额 cpu=8，故恰 8 个 RUNNING、2 个因配额排队（PENDING，不丢弃）。
    running = [j for j in deps if j.state is JobState.RUNNING]
    pending = [j for j in deps if j.state is JobState.PENDING]
    assert len(running) == 8 and len(pending) == 2
    assert scheduler.get_job(up_cancel.id).state is JobState.CANCELLED
    # 2 个排队下游必须被配额明确挡住（等待而非被丢弃/误判终态）
    from .conftest import assert_no_grantable_pending
    assert_no_grantable_pending(scheduler)
    log_snapshot(scheduler, "提交/取消/完成全部交错")
    assert_conservation(scheduler)
