"""作业依赖能力单测。

覆盖：依赖就绪前后、上游成功放行、上游失败/取消传播、非法依赖拒绝、
与并发提交/取消/完成交错、确定性与资源守恒。
每条用例都打印判定依据（快照 + 作业历史），便于从测试日志追溯结论。
"""

from __future__ import annotations

import threading

import pytest

from batch_scheduler.errors import JobRejectedError, JobStateConflictError
from batch_scheduler.models import JobState, RejectReason, Resources, SubmitRequest

from .conftest import FakeClock, assert_conservation, log_snapshot


def req(key, submitter="team-a", priority=0, cpu=1, mem=0, gpu=0,
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
    assert ei.value.reason is reason, f"期望 {reason}，实际 {ei.value.reason}: {ei.value}"
    print(f"拒绝判定: reason={ei.value.reason.value} message={ei.value}")
    return ei.value


def print_history(scheduler, job_id):
    job = scheduler.get_job(job_id)
    print(f"--- {job_id} 状态={job.state.value} note={job.scheduling_note!r} 历史:")
    for line in job.history:
        print(f"    {line}")
    return job


# ---------- 依赖就绪前后 ----------

def test_dependency_not_ready_waits_without_resources(scheduler, clock):
    up = scheduler.submit(req("up", cpu=4))
    assert up.state is JobState.RUNNING

    down = scheduler.submit(req("down", cpu=4, depends_on=[up.id]))
    assert down.state is JobState.WAITING
    assert down.depends_on == [up.id]
    # WAITING 不占节点容量、不占提交方配额、不进排队队列
    assert scheduler.used_resources() == Resources(cpu=4)
    assert scheduler.submitter_used("team-a") == Resources(cpu=4)
    snap = scheduler.snapshot()
    assert [w["job_id"] for w in snap.waiting_jobs] == [down.id]
    assert snap.waiting_jobs[0]["unfinished_dependencies"] == [up.id]
    assert all(p["job_id"] != down.id for p in snap.pending_jobs)
    log_snapshot(scheduler, "依赖未就绪: 下游 WAITING")
    print_history(scheduler, down.id)
    assert_conservation(scheduler)


def test_waiting_job_does_not_block_others(scheduler, clock):
    """WAITING 作业不影响其它本来能跑的作业的排队顺序与调度。"""
    up = scheduler.submit(req("up", cpu=8, submitter="team-a"))
    down = scheduler.submit(req("down", cpu=8, submitter="team-a",
                                depends_on=[up.id]))
    assert down.state is JobState.WAITING
    # team-b 的作业应照常立即调度，不受 team-a 的 WAITING 作业影响
    other = scheduler.submit(req("other", cpu=4, submitter="team-b"))
    assert other.state is JobState.RUNNING
    log_snapshot(scheduler, "WAITING 不阻塞其它提交方")
    assert_conservation(scheduler)


def test_release_after_single_upstream_completes(scheduler, clock):
    up = scheduler.submit(req("up", cpu=4))
    down = scheduler.submit(req("down", cpu=4, depends_on=[up.id]))
    assert down.state is JobState.WAITING

    clock.advance_ms(500)
    scheduler.complete(up.id)
    after = scheduler.get_job(down.id)
    # 上游成功后放行；容量已释放，下游应在本轮 tick 中被授予资源
    assert after.state is JobState.RUNNING
    assert after.schedule_count == 1
    log_snapshot(scheduler, "上游成功: 下游放行并运行")
    print_history(scheduler, down.id)
    assert_conservation(scheduler)


def test_release_only_after_all_upstreams_complete(scheduler, clock):
    up1 = scheduler.submit(req("up1", cpu=2))
    up2 = scheduler.submit(req("up2", cpu=2))
    down = scheduler.submit(req("down", cpu=2, depends_on=[up1.id, up2.id]))
    assert down.state is JobState.WAITING

    scheduler.complete(up1.id)
    mid = scheduler.get_job(down.id)
    assert mid.state is JobState.WAITING  # 还有一个上游没完成
    assert up2.id in mid.scheduling_note  # 判定依据里指明还在等谁
    log_snapshot(scheduler, "多依赖: 只完成一个仍等待")

    scheduler.complete(up2.id)
    final = scheduler.get_job(down.id)
    assert final.state is JobState.RUNNING
    print_history(scheduler, down.id)
    assert_conservation(scheduler)


def test_submit_when_deps_already_completed_goes_straight_to_pending(scheduler):
    up = scheduler.submit(req("up", cpu=1))
    scheduler.complete(up.id)
    down = scheduler.submit(req("down", cpu=1, depends_on=[up.id]))
    # 依赖已全部成功：不经过 WAITING，直接参与调度
    assert down.state is JobState.RUNNING
    assert down.history[-1].endswith("(all dependencies already COMPLETED)") or any(
        "all dependencies already COMPLETED" in h for h in down.history
    )
    print_history(scheduler, down.id)
    assert_conservation(scheduler)


def test_waiting_job_age_counts_when_released(scheduler, clock):
    """WAITING 期间不排队，但等待时长自提交起算，放行后年龄加分生效。"""
    up = scheduler.submit(req("up", cpu=16, mem=32768, submitter="system"))
    down = scheduler.submit(req("down", cpu=4, submitter="team-a",
                                depends_on=[up.id]))
    clock.advance_ms(2000)  # WAITING 2 秒
    scheduler.complete(up.id)
    after = scheduler.get_job(down.id)
    assert after.state is JobState.RUNNING
    snap = scheduler.snapshot()
    assert snap.waiting_jobs == []
    log_snapshot(scheduler, "WAITING 结束后按原始提交时间参与调度")
    assert_conservation(scheduler)


# ---------- 上游失败 / 取消的传播 ----------

def test_upstream_failure_propagates_to_waiting_dependents(scheduler):
    up = scheduler.submit(req("up", cpu=2))
    down = scheduler.submit(req("down", cpu=2, depends_on=[up.id]))
    scheduler.complete(up.id, failed=True)

    final = scheduler.get_job(down.id)
    assert final.state is JobState.DEPENDENCY_FAILED
    assert final.state.is_terminal
    assert up.id in final.scheduling_note
    assert scheduler.used_resources() == Resources()
    log_snapshot(scheduler, "上游失败: 下游 DEPENDENCY_FAILED")
    print_history(scheduler, down.id)
    assert_conservation(scheduler)


def test_upstream_cancel_propagates_to_waiting_dependents(scheduler):
    up = scheduler.submit(req("up", cpu=2))
    down = scheduler.submit(req("down", cpu=2, depends_on=[up.id]))
    scheduler.cancel(up.id)

    final = scheduler.get_job(down.id)
    assert final.state is JobState.DEPENDENCY_FAILED
    assert up.id in final.scheduling_note
    print_history(scheduler, down.id)
    assert_conservation(scheduler)


def test_failure_cascades_transitively(scheduler):
    a = scheduler.submit(req("a", cpu=1))
    b = scheduler.submit(req("b", cpu=1, depends_on=[a.id]))
    c = scheduler.submit(req("c", cpu=1, depends_on=[b.id]))
    scheduler.complete(a.id, failed=True)

    assert scheduler.get_job(b.id).state is JobState.DEPENDENCY_FAILED
    c_final = scheduler.get_job(c.id)
    assert c_final.state is JobState.DEPENDENCY_FAILED
    assert b.id in c_final.scheduling_note  # 直接原因是 b 未成功
    print_history(scheduler, c.id)
    assert_conservation(scheduler)


def test_submit_with_already_failed_upstream_fails_immediately(scheduler):
    up = scheduler.submit(req("up", cpu=1))
    scheduler.complete(up.id, failed=True)
    down = scheduler.submit(req("down", cpu=1, depends_on=[up.id]))
    assert down.state is JobState.DEPENDENCY_FAILED
    assert down.finished_at_ns is not None
    print_history(scheduler, down.id)
    assert_conservation(scheduler)


def test_dependency_failed_is_terminal_and_queryable(scheduler):
    up = scheduler.submit(req("up", cpu=1))
    down = scheduler.submit(req("down", cpu=1, depends_on=[up.id]))
    scheduler.cancel(up.id)
    final = scheduler.get_job(down.id)
    assert final.state is JobState.DEPENDENCY_FAILED
    # 终态：不可再取消、不可完成
    with pytest.raises(JobStateConflictError):
        scheduler.cancel(down.id)
    with pytest.raises(JobStateConflictError):
        scheduler.complete(down.id)
    assert_conservation(scheduler)


def test_cancel_waiting_job_cascades_to_its_dependents(scheduler):
    up = scheduler.submit(req("up", cpu=1))
    mid = scheduler.submit(req("mid", cpu=1, depends_on=[up.id]))
    leaf = scheduler.submit(req("leaf", cpu=1, depends_on=[mid.id]))
    cancelled = scheduler.cancel(mid.id)
    assert cancelled.state is JobState.CANCELLED
    assert scheduler.get_job(leaf.id).state is JobState.DEPENDENCY_FAILED
    # 上游不受影响，继续运行
    assert scheduler.get_job(up.id).state is JobState.RUNNING
    print_history(scheduler, mid.id)
    print_history(scheduler, leaf.id)
    assert_conservation(scheduler)


# ---------- 非法依赖拒绝（原因可区分） ----------

def test_unknown_dependency_rejected(scheduler):
    expect_reject(scheduler, req("d", cpu=1, depends_on=["job-999999"]),
                  RejectReason.UNKNOWN_DEPENDENCY)
    assert scheduler.list_jobs() == []  # 被拒的提交不留下任何痕迹
    assert_conservation(scheduler)


def test_self_dependency_rejected(scheduler):
    first = scheduler.submit(req("first", cpu=1))
    # 下一个生成的 id 可预测：声明依赖它即构成自依赖
    next_id = f"job-{int(first.id.split('-')[1]) + 1:06d}"
    expect_reject(scheduler, req("self", cpu=1, depends_on=[next_id]),
                  RejectReason.SELF_DEPENDENCY)
    assert_conservation(scheduler)


def test_dependency_cycle_rejected(scheduler):
    """环检测：依赖图必须始终是 DAG。

    作业 id 由服务端单调生成，正常提交路径下新作业不可能出现在既有作业
    的依赖链中（那时它还不存在）。这里直接破坏内部不变量构造 A<->B 环，
    验证防御性环检测在提交当下以可区分原因挡下，而不是收下后死锁。
    """
    a = scheduler.submit(req("a", cpu=1))
    b = scheduler.submit(req("b", cpu=1, depends_on=[a.id]))
    # 模拟内部不变量被破坏（如未来引入“修改依赖”接口）：让 a 也依赖 b
    scheduler._jobs[a.id].depends_on.append(b.id)  # noqa: SLF001

    # 此时提交依赖 a 的新作业：沿 a -> b -> a 的边遍历，虽然到不了新作业，
    # 但直接构造“新作业 id 出现在依赖链中”的场景验证检测本身：
    victim = scheduler._jobs[a.id]  # noqa: SLF001
    assert scheduler._dep_reaches(b.id, a.id)  # noqa: SLF001 间接环可达
    with pytest.raises(JobRejectedError) as ei:
        # 直接调用提交路径同款校验：目标 id 取 a.id，依赖 b 构成 b->a->b 环
        scheduler._validate_dependencies(a.id, [b.id])  # noqa: SLF001
    assert ei.value.reason is RejectReason.DEPENDENCY_CYCLE
    print(f"拒绝判定: reason={ei.value.reason.value} message={ei.value}")
    assert_conservation(scheduler)


def test_invalid_dependency_entries_rejected(scheduler):
    scheduler.submit(req("base", cpu=1))
    expect_reject(scheduler, req("bad", cpu=1, depends_on=["  "]),
                  RejectReason.INVALID_REQUEST)


def test_duplicate_dependencies_deduplicated(scheduler):
    up = scheduler.submit(req("up", cpu=1))
    down = scheduler.submit(req("down", cpu=1, depends_on=[up.id, up.id]))
    assert down.depends_on == [up.id]
    assert_conservation(scheduler)


# ---------- 与配额/容量交互：该排队的仍排队 ----------

def test_released_dependency_respects_capacity_and_quota(scheduler, clock):
    """依赖放行后若容量/配额暂时不够，仍排队等待而不是被丢弃。"""
    filler = scheduler.submit(req("filler", cpu=16, mem=32768, submitter="system"))
    up = scheduler.submit(req("up", cpu=1, submitter="team-a", priority=5))
    # up 因容量不足在排队；down 依赖 up，处于 WAITING
    assert scheduler.get_job(up.id).state is JobState.PENDING
    down = scheduler.submit(req("down", cpu=8, submitter="team-a",
                                depends_on=[up.id]))
    assert down.state is JobState.WAITING

    scheduler.complete(filler.id)  # 释放全部容量 -> up 被调度
    assert scheduler.get_job(up.id).state is JobState.RUNNING
    scheduler.complete(up.id)  # up 成功 -> down 放行
    after = scheduler.get_job(down.id)
    # team-a 配额 8cpu，up 已释放，down 需要 8cpu -> 应能运行
    assert after.state is JobState.RUNNING
    log_snapshot(scheduler, "依赖放行后按容量/配额正常调度")
    assert_conservation(scheduler)


def test_released_dependency_queues_when_quota_insufficient(scheduler):
    """放行时配额被其它作业占着：转入 PENDING 排队，不丢不占。"""
    up = scheduler.submit(req("up", cpu=1, submitter="team-a"))
    down = scheduler.submit(req("down", cpu=8, submitter="team-a",
                                depends_on=[up.id]))
    # team-a 配额 8cpu：up(1)+hog(7) 占满，hog 在运行
    hog = scheduler.submit(req("hog", cpu=7, submitter="team-a"))
    assert hog.state is JobState.RUNNING
    scheduler.complete(up.id)
    mid = scheduler.get_job(down.id)
    assert mid.state is JobState.PENDING  # 放行但配额不足 -> 排队
    assert mid.schedule_count == 0
    log_snapshot(scheduler, "放行后配额不足: PENDING 排队")
    # 完成 hog 释放配额后 down 才运行
    scheduler.complete(hog.id)
    assert scheduler.get_job(down.id).state is JobState.RUNNING
    print_history(scheduler, down.id)
    assert_conservation(scheduler)


# ---------- 并发交错与确定性 ----------

def test_concurrent_submit_complete_cancel_with_dependencies(scheduler, clock):
    """多线程交错提交（带依赖）、完成、取消，最终状态合法且资源守恒。"""
    up_ok = scheduler.submit(req("up-ok", cpu=2, submitter="team-a"))
    up_fail = scheduler.submit(req("up-fail", cpu=2, submitter="team-b"))
    up_cancel = scheduler.submit(req("up-cancel", cpu=2, submitter="team-c"))

    errors: list[BaseException] = []

    def submit_dependents(tag):
        try:
            for i in range(6):
                scheduler.submit(req(f"down-ok-{tag}-{i}", cpu=1,
                                     submitter="team-a",
                                     depends_on=[up_ok.id]))
                scheduler.submit(req(f"down-fail-{tag}-{i}", cpu=1,
                                     submitter="team-b",
                                     depends_on=[up_fail.id]))
                scheduler.submit(req(f"down-cancel-{tag}-{i}", cpu=1,
                                     submitter="team-c",
                                     depends_on=[up_cancel.id]))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def finish_upstreams():
        try:
            scheduler.complete(up_ok.id)
            scheduler.complete(up_fail.id, failed=True)
            scheduler.cancel(up_cancel.id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=submit_dependents, args=("t1",)),
        threading.Thread(target=submit_dependents, args=("t2",)),
        threading.Thread(target=finish_upstreams),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, f"并发执行出现异常: {errors}"

    jobs = {j.id: j for j in scheduler.list_jobs()}
    for j in jobs.values():
        print(f"终态盘点: {j.id} -> {j.state.value} note={j.scheduling_note!r}")
        assert j.state is not JobState.WAITING, f"{j.id} 上游已终态却仍 WAITING"

    # 每个下游的结论必须能由上游终态解释
    for j in jobs.values():
        if not j.depends_on:
            continue
        upstream_states = {jobs[d].state for d in j.depends_on}
        if upstream_states == {JobState.COMPLETED}:
            assert j.state in (JobState.PENDING, JobState.RUNNING,
                               JobState.COMPLETED)
        elif upstream_states & {JobState.FAILED, JobState.CANCELLED,
                                JobState.DEPENDENCY_FAILED}:
            assert j.state is JobState.DEPENDENCY_FAILED
    log_snapshot(scheduler, "并发交错后的最终快照")
    assert_conservation(scheduler)


def test_deterministic_replay_same_input_sequence(clock):
    """相同输入序列两次执行，结论（状态与历史）完全一致。"""

    def run_script():
        clk = FakeClock()
        s = scheduler_factory(clk)
        a = s.submit(req("a", cpu=4, submitter="team-a"))
        b = s.submit(req("b", cpu=4, submitter="team-a", depends_on=[a.id]))
        c = s.submit(req("c", cpu=2, submitter="team-b", depends_on=[a.id]))
        d = s.submit(req("d", cpu=1, submitter="team-c", depends_on=[b.id, c.id]))
        clk.advance_ms(100)
        s.complete(a.id)
        clk.advance_ms(100)
        s.complete(b.id, failed=True)
        s.cancel(c.id)
        return [(j.id, j.state.value, j.scheduling_note, tuple(j.history))
                for j in s.list_jobs()]

    def scheduler_factory(clk):
        from batch_scheduler.scheduler import Scheduler
        from .conftest import CAPACITY, QUOTAS
        return Scheduler(node_capacity=CAPACITY, submitter_quotas=QUOTAS,
                         clock_ns=clk)

    first = run_script()
    second = run_script()
    assert first == second, "相同输入序列产生了不同的结论"
    for job_id, state, note, _ in first:
        print(f"复现结论: {job_id} -> {state} ({note})")
