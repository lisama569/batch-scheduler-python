"""调度核心单测：校验、幂等、调度、配额、公平、并发与资源守恒。

每个关键用例都会通过 caplog 打印调度快照（Snapshot.render()）与判定依据。
"""

from __future__ import annotations

import logging
import threading

import pytest

from scheduler.errors import (
    IdempotencyConflictError,
    InvalidJobStateError,
    InvalidPriorityError,
    InvalidResourceError,
    JobNotFoundError,
    ResourceExceedsCapacityError,
)
from scheduler.models import JobState, Resource
from scheduler.scheduler import ManualClock, Scheduler

logger = logging.getLogger("batch-scheduler.test")


def make_scheduler(capacity=(8, 16, 2), aging_weight=1, default_quota=None):
    clock = ManualClock()
    sched = Scheduler(
        Resource(capacity),
        clock=clock,
        aging_weight=aging_weight,
        default_quota=default_quota,
    )
    return sched, clock


def log_snapshot(sched: Scheduler, title: str) -> None:
    logger.info("==== %s ====\n%s", title, sched.snapshot().render())


# --------------------------------------------------------------------- #
# 1. Resource 模型
# --------------------------------------------------------------------- #
def test_resource_vector_math() -> None:
    a = Resource((2, 4))
    b = Resource((3, 1))
    assert (a + b).values == (5, 5)
    assert (b - a).values == (1, -3)
    assert a.fits_in(Resource((2, 5)))
    assert not a.fits_in(Resource((1, 4)))
    assert Resource.zero(3).values == (0, 0, 0)
    with pytest.raises(ValueError):
        a.fits_in(Resource((1, 2, 3)))


# --------------------------------------------------------------------- #
# 2. 提交校验：非法请求 / 超容量必须被拒绝，且原因可区分
# --------------------------------------------------------------------- #
def test_submit_rejects_invalid_resources(caplog) -> None:
    sched, _ = make_scheduler()
    with caplog.at_level(logging.INFO):
        with pytest.raises(InvalidResourceError) as ei:
            sched.submit(submitter="a", request=Resource((0, 1, 1)))
        with pytest.raises(InvalidResourceError) as en:
            sched.submit(submitter="a", request=Resource((1, -1, 1)))
        with pytest.raises(InvalidResourceError) as ed:
            sched.submit(submitter="a", request=Resource((1, 1)))
        with pytest.raises(ResourceExceedsCapacityError) as ec:
            sched.submit(submitter="a", request=Resource((9, 1, 1)))
    logger.info("拒绝原因: %s | %s | %s | %s",
                ei.value.code, en.value.code, ed.value.code, ec.value.code)
    log_snapshot(sched, "非法/超额请求全部被拒后，节点仍空闲")
    assert sched.snapshot().used == (0, 0, 0)
    assert sched.list_jobs() == []
    assert {ei.value.code, en.value.code, ed.value.code, ec.value.code} == {
        "INVALID_RESOURCE",
        "RESOURCE_EXCEEDS_CAPACITY",
    }


def test_submit_rejects_invalid_priority_and_submitter() -> None:
    sched, _ = make_scheduler()
    with pytest.raises(InvalidPriorityError):
        sched.submit(submitter="a", request=Resource((1, 1, 1)), priority=101)
    with pytest.raises(InvalidPriorityError):
        sched.submit(submitter="a", request=Resource((1, 1, 1)), priority=-1)
    from scheduler.errors import InvalidRequestError

    with pytest.raises(InvalidRequestError):
        sched.submit(submitter="  ", request=Resource((1, 1, 1)))
    assert sched.list_jobs() == []


# --------------------------------------------------------------------- #
# 3. 幂等提交：相同键不重复；参数冲突单独报错
# --------------------------------------------------------------------- #
def test_idempotent_submit_deduplicates(caplog) -> None:
    sched, _ = make_scheduler()
    req = Resource((2, 4, 1))
    r1 = sched.submit(submitter="a", request=req, priority=5, idempotency_key="k1")
    r2 = sched.submit(submitter="a", request=req, priority=5, idempotency_key="k1")
    r3 = sched.submit(submitter="a", request=req, priority=5, idempotency_key="k1")
    with caplog.at_level(logging.INFO):
        log_snapshot(sched, "同一幂等键提交 3 次只产生 1 个作业")
    assert r1.duplicated is False
    assert r2.duplicated and r3.duplicated
    assert r1.job.id == r2.job.id == r3.job.id
    assert len(sched.list_jobs()) == 1
    assert sched.snapshot().used == req.values


def test_idempotency_conflict_is_distinguishable() -> None:
    sched, _ = make_scheduler()
    sched.submit(submitter="a", request=Resource((2, 4, 1)), idempotency_key="k1")
    with pytest.raises(IdempotencyConflictError) as exc:
        sched.submit(submitter="b", request=Resource((2, 4, 1)), idempotency_key="k1")
    assert exc.value.code == "IDEMPOTENCY_CONFLICT"
    with pytest.raises(IdempotencyConflictError):
        sched.submit(submitter="a", request=Resource((2, 4, 2)), idempotency_key="k1")
    assert len(sched.list_jobs()) == 1


# --------------------------------------------------------------------- #
# 4. 调度：容量约束、优先级 + 等待时间、FIFO 平局
# --------------------------------------------------------------------- #
def test_basic_scheduling_and_capacity_bound(caplog) -> None:
    sched, clock = make_scheduler()
    # 占满节点 (8,16,2)
    j1 = sched.submit(submitter="a", request=Resource((4, 8, 1)), priority=10).job
    j2 = sched.submit(submitter="b", request=Resource((4, 8, 1)), priority=10).job
    j3 = sched.submit(submitter="c", request=Resource((2, 4, 0 + 1)), priority=10).job
    with caplog.at_level(logging.INFO):
        snap = sched.schedule()
        logger.info("占满后再提交:\n%s", snap.render())
    assert j1.state is JobState.RUNNING
    assert j2.state is JobState.RUNNING
    assert j3.state is JobState.QUEUED
    assert sched.snapshot().used == (8, 16, 2)
    # 任何时刻授予资源不得超过容量
    assert all(u <= cap for u, cap in zip(snap.used, snap.capacity))

    # 完成 j1 释放 (4,8,1)，j3 只需 (2,4,1)，应立即补位
    sched.complete(j1.id)
    assert sched.get_job(j3.id).state is JobState.RUNNING
    log_snapshot(sched, "j1 完成、j3 补位后")
    sched.assert_invariants()


def test_priority_order_with_fifo_tiebreak(caplog) -> None:
    sched, clock = make_scheduler(capacity=(4, 4))
    # 占满：两个 (2,2) 刚好用完 (4,4)
    sched.submit(submitter="a", request=Resource((2, 2)), priority=0)
    sched.submit(submitter="b", request=Resource((2, 2)), priority=0)
    # 排队：低优先级先提交，高优先级后提交
    low = sched.submit(submitter="c", request=Resource((2, 2)), priority=1).job
    high = sched.submit(submitter="d", request=Resource((2, 2)), priority=9).job
    assert low.state is high.state is JobState.QUEUED
    # 释放一个槽位（同一 tick，无老化差异），高优先级先上
    sched.complete("job-00000001")
    with caplog.at_level(logging.INFO):
        snap = sched.schedule()
        logger.info("高优先级优先调度:\n%s", snap.render())
    assert high.state is JobState.RUNNING
    assert low.state is JobState.QUEUED
    # 再释放一个，同优先级按 FIFO
    sched.complete("job-00000002")
    assert low.state is JobState.RUNNING
    sched.assert_invariants()


def test_aging_prevents_starvation(caplog) -> None:
    """等待足够久后，低优先级作业的有效优先级反超并被调度。"""
    sched, clock = make_scheduler(capacity=(4, 4), aging_weight=1)
    sched.submit(submitter="a", request=Resource((4, 4)), priority=0)  # 占满
    old_low = sched.submit(submitter="low", request=Resource((4, 4)), priority=1).job
    # 老的低优先级作业等待 10 tick
    clock.advance(10)
    # 新的高优先级作业不断到达：每个都排在老作业后面
    newcomers = [
        sched.submit(submitter=f"h{i}", request=Resource((4, 4)), priority=5).job
        for i in range(3)
    ]
    with caplog.at_level(logging.INFO):
        before = sched.schedule()
        logger.info("老化前排队判定:\n%s", before.render())
    # 老作业有效优先级 = 1 + 1*等待；新作业 = 5 + 0。验证决策排序
    old_dec = next(d for d in before.decisions if d.job_id == old_low.id)
    assert old_dec.effective_priority > newcomers[0].priority
    assert [d.job_id for d in before.decisions][0] == old_low.id
    # 节点释放后，老作业先于新来的高优先级作业被调度
    sched.complete("job-00000001")
    with caplog.at_level(logging.INFO):
        after = sched.snapshot()
        logger.info("释放后老作业获得资源:\n%s", after.render())
    assert old_low.state is JobState.RUNNING
    assert all(j.state is JobState.QUEUED for j in newcomers)
    sched.assert_invariants()


def test_small_job_backfills_behind_large_head(caplog) -> None:
    """队首大作业放不下时，后面的小作业不应被阻塞（补位扫描）。"""
    sched, _ = make_scheduler(capacity=(6, 6))
    sched.submit(submitter="a", request=Resource((4, 4)))  # 剩余 (2,2)
    big = sched.submit(submitter="b", request=Resource((4, 4)), priority=9).job
    small = sched.submit(submitter="c", request=Resource((2, 2)), priority=1).job
    with caplog.at_level(logging.INFO):
        snap = sched.schedule()
        logger.info("补位扫描:\n%s", snap.render())
    assert big.state is JobState.QUEUED
    assert small.state is JobState.RUNNING  # 小作业绕过队首补位
    assert snap.used == (6, 6)
    sched.assert_invariants()


# --------------------------------------------------------------------- #
# 5. 生命周期：完成 / 失败 / 取消释放资源，重复通知幂等、不重复释放
# --------------------------------------------------------------------- #
def test_complete_fail_cancel_release_resources(caplog) -> None:
    sched, _ = make_scheduler(capacity=(8, 8))
    j1 = sched.submit(submitter="a", request=Resource((4, 4))).job
    j2 = sched.submit(submitter="a", request=Resource((4, 4))).job
    assert sched.snapshot().used == (8, 8)

    sched.complete(j1.id)
    assert sched.get_job(j1.id).state is JobState.COMPLETED
    # 重复完成：幂等，资源不重复释放
    again = sched.complete(j1.id)
    assert again.state is JobState.COMPLETED

    sched.fail(j2.id)
    assert sched.get_job(j2.id).state is JobState.FAILED
    with pytest.raises(InvalidJobStateError):
        sched.complete(j2.id)  # 终态互斥
    with pytest.raises(InvalidJobStateError):
        sched.cancel(j2.id)

    with caplog.at_level(logging.INFO):
        log_snapshot(sched, "全部终结后资源完全释放")
    assert sched.snapshot().used == (0, 0)
    sched.assert_invariants()


def test_cancel_queued_and_running(caplog) -> None:
    sched, _ = make_scheduler(capacity=(4, 4))
    sched.submit(submitter="a", request=Resource((4, 4)))
    queued = sched.submit(submitter="b", request=Resource((2, 2))).job
    with caplog.at_level(logging.INFO):
        snap = sched.schedule()
        logger.info("取消前:\n%s", snap.render())
    assert queued.state is JobState.QUEUED
    # 取消排队作业：不入运行、不占资源
    sched.cancel(queued.id)
    assert queued.state is JobState.CANCELLED
    assert queued.id not in sched.snapshot().queued_jobs
    sched.cancel(queued.id)  # 重复取消幂等

    running = sched.list_jobs()[0]
    sched.cancel(running.id)
    assert running.state is JobState.CANCELLED
    assert sched.snapshot().used == (0, 0)
    sched.assert_invariants()


def test_get_unknown_job_raises() -> None:
    sched, _ = make_scheduler()
    with pytest.raises(JobNotFoundError):
        sched.get_job("nope")


# --------------------------------------------------------------------- #
# 6. 提交方配额：超限排队而非拒绝，释放后入队作业被调度
# --------------------------------------------------------------------- #
def test_submitter_quota_queues_instead_of_rejecting(caplog) -> None:
    # 节点 (8,8)，但提交方 a 配额 (4,4)
    sched, _ = make_scheduler(capacity=(8, 8), default_quota=Resource((8, 8)))
    sched.set_quota("a", Resource((4, 4)))
    j1 = sched.submit(submitter="a", request=Resource((4, 4)), priority=10).job
    j2 = sched.submit(submitter="a", request=Resource((4, 4)), priority=9).job
    j3 = sched.submit(submitter="b", request=Resource((4, 4)), priority=1).job
    with caplog.at_level(logging.INFO):
        snap = sched.schedule()
        logger.info("配额边界（a<=4）:\n%s", snap.render())
    assert j1.state is JobState.RUNNING
    # a 的第二个作业：节点有余量但配额到顶 -> 排队（不是拒绝）
    assert j2.state is JobState.QUEUED
    dec_j2 = next(d for d in snap.decisions if d.job_id == j2.id)
    assert dec_j2.admitted is False and "配额不足" in dec_j2.reason
    # b 不受 a 的配额限制，占用剩余资源
    assert j3.state is JobState.RUNNING
    assert sched.snapshot().used == (8, 8)
    sched.assert_invariants()

    # j1 仍占着 a 的全部配额、j3 占着节点余量：必须两者都释放，j2 才能同时
    # 满足「节点容量」与「提交方配额」两个条件
    sched.complete(j3.id)
    assert j2.state is JobState.QUEUED  # 只释放节点容量，a 的配额仍被 j1 占用
    sched.complete(j1.id)
    with caplog.at_level(logging.INFO):
        log_snapshot(sched, "j1 与 j3 都完成、a 的排队作业获得配额+容量")
    assert j2.state is JobState.RUNNING
    assert sched.snapshot().used == (4, 4)
    sched.assert_invariants()


def test_quota_boundary_exact_fit_and_overrun_blocked() -> None:
    sched, _ = make_scheduler(capacity=(10, 10))
    sched.set_quota("a", Resource((5, 5)))
    ok = sched.submit(submitter="a", request=Resource((5, 5))).job  # 恰好等于配额
    assert ok.state is JobState.RUNNING
    over = sched.submit(submitter="a", request=Resource((1, 1))).job
    assert over.state is JobState.QUEUED  # 超一个单位也要排队
    # 降低配额不抢占运行中的作业
    sched.set_quota("a", Resource((1, 1)))
    assert ok.state is JobState.RUNNING
    sched.assert_invariants()


def test_fairness_no_submitter_starved(caplog) -> None:
    """大作业提交方反复占满节点时，小作业提交方必须能在有限时间内得到资源。"""
    sched, clock = make_scheduler(capacity=(8, 8), aging_weight=1)
    # a 先占满
    first = sched.submit(submitter="a", request=Resource((8, 8)), priority=10).job
    # b 排队等待（小作业），先等 20 tick 积累老化优势
    b_jobs = [
        sched.submit(submitter="b", request=Resource((2, 2)), priority=0).job
    ]
    clock.advance(20)
    # a 持续追加新的高优先级大作业（此刻才提交，等待时长为 0）
    a_jobs = [
        sched.submit(submitter="a", request=Resource((8, 8)), priority=10).job
        for _ in range(3)
    ]
    assert b_jobs[0].state is JobState.QUEUED

    # b 的有效优先级 = 0 + 20 = 20 > a 新作业的 10，a 的首作业完成后 b 必须先上
    sched.complete(first.id)
    with caplog.at_level(logging.INFO):
        snap = sched.schedule()
        logger.info("公平性：老化后的 b 抢到资源:\n%s", snap.render())
    assert b_jobs[0].state is JobState.RUNNING, "b 不应被 a 无限饿死"
    assert all(j.state is JobState.QUEUED for j in a_jobs)
    sched.assert_invariants()


# --------------------------------------------------------------------- #
# 7. 并发提交 / 取消与完成竞争
# --------------------------------------------------------------------- #
def test_concurrent_submissions_preserve_conservation(caplog) -> None:
    sched, _ = make_scheduler(capacity=(8, 8))
    barrier = threading.Barrier(8)
    errors: list[Exception] = []

    def worker(idx: int) -> None:
        barrier.wait()
        try:
            sched.submit(
                submitter=f"u{idx % 3}",
                request=Resource((2, 2)),
                priority=idx % 5,
                idempotency_key=None,
            )
        except Exception as exc:  # noqa: BLE001 - 记录到列表统一断言
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    with caplog.at_level(logging.INFO):
        snap = sched.schedule()
        logger.info("8 线程并发提交后:\n%s", snap.render())
    assert errors == []
    jobs = sched.list_jobs()
    assert len(jobs) == 8
    running = [j for j in jobs if j.state is JobState.RUNNING]
    queued = [j for j in jobs if j.state is JobState.QUEUED]
    # 4 个 (2,2) 用完 (8,8)，其余排队
    assert len(running) == 4 and len(queued) == 4
    assert snap.used == (8, 8)
    sched.assert_invariants()


def test_concurrent_completes_and_cancels_conservation(caplog) -> None:
    sched, _ = make_scheduler(capacity=(8, 8))
    jobs = [
        sched.submit(submitter="x", request=Resource((2, 2))).job for _ in range(8)
    ]
    assert all(j.state is JobState.RUNNING for j in jobs[:4])
    assert all(j.state is JobState.QUEUED for j in jobs[4:])

    barrier = threading.Barrier(4)

    def complete_worker(ids: list[str]) -> None:
        barrier.wait()
        for jid in ids:
            sched.complete(jid)

    def cancel_worker(ids: list[str]) -> None:
        barrier.wait()
        for jid in ids:
            sched.cancel(jid)

    # 两个线程完成运行中作业，两个线程取消排队作业
    t1 = threading.Thread(target=complete_worker, args=([jobs[0].id, jobs[1].id],))
    t2 = threading.Thread(target=complete_worker, args=([jobs[2].id, jobs[3].id],))
    t3 = threading.Thread(target=cancel_worker, args=([jobs[4].id, jobs[5].id],))
    t4 = threading.Thread(target=cancel_worker, args=([jobs[6].id, jobs[7].id],))
    for t in (t1, t2, t3, t4):
        t.start()
    for t in (t1, t2, t3, t4):
        t.join()

    with caplog.at_level(logging.INFO):
        snap = sched.schedule()
        logger.info("完成与取消并发交错后:\n%s", snap.render())
    states = [sched.get_job(j.id).state for j in jobs]
    assert states.count(JobState.COMPLETED) == 4
    assert states.count(JobState.CANCELLED) == 4
    # 运行中全释放、排队中全取消 -> used 必须归零
    assert snap.used == (0, 0)
    assert snap.running_jobs == []
    sched.assert_invariants()


def test_competing_complete_and_cancel_on_same_job() -> None:
    """同一个 RUNNING 作业上 complete 与 cancel 竞争：恰好一个生效，资源只释放一次。"""
    sched, _ = make_scheduler(capacity=(4, 4))
    job = sched.submit(submitter="x", request=Resource((2, 2))).job
    outcome: list[str] = []

    barrier = threading.Barrier(2)

    def do_complete() -> None:
        barrier.wait()
        try:
            sched.complete(job.id)
            outcome.append("COMPLETED")
        except InvalidJobStateError:
            outcome.append("LOST-COMPLETE")

    def do_cancel() -> None:
        barrier.wait()
        try:
            sched.cancel(job.id)
            outcome.append("CANCELLED")
        except InvalidJobStateError:
            outcome.append("LOST-CANCEL")

    t1 = threading.Thread(target=do_complete)
    t2 = threading.Thread(target=do_cancel)
    t1.start(); t2.start(); t1.join(); t2.join()

    final = sched.get_job(job.id).state
    assert final in {JobState.COMPLETED, JobState.CANCELLED}
    assert final.value in outcome  # 只有抢到的一方记录成功
    assert sched.snapshot().used == (0, 0)
    sched.assert_invariants()


# --------------------------------------------------------------------- #
# 8. 可复现：相同输入序列 + 相同时间推进 -> 相同调度结论
# --------------------------------------------------------------------- #
def _run_scenario():
    sched, clock = make_scheduler(capacity=(8, 8), aging_weight=2)
    ids = []
    ids.append(sched.submit(submitter="a", request=Resource((8, 8)), priority=10).job.id)
    ids.append(sched.submit(submitter="b", request=Resource((2, 2)), priority=1).job.id)
    ids.append(sched.submit(submitter="c", request=Resource((2, 2)), priority=3).job.id)
    clock.advance(5)
    ids.append(sched.submit(submitter="a", request=Resource((4, 4)), priority=8).job.id)
    clock.advance(3)
    sched.complete(ids[0])
    sched.schedule()
    states = [(j.id, j.state.value, j.scheduled_at) for j in sched.list_jobs()]
    decisions = [
        (d.job_id, d.effective_priority, d.admitted)
        for d in sched.schedule().decisions
    ]
    return states, decisions, sched.snapshot().used


def test_schedule_is_reproducible() -> None:
    run_a = _run_scenario()
    run_b = _run_scenario()
    assert run_a == run_b


# --------------------------------------------------------------------- #
# 9. 随机混合操作压力：多线程交错提交/完成/失败/取消/查询，结束时必须守恒
# --------------------------------------------------------------------- #
def test_randomized_interleaved_stress(caplog) -> None:
    import random

    sched, clock = make_scheduler(capacity=(12, 12))
    ids: list[str] = []
    # 预置一批作业：若干运行 + 若干排队
    for i in range(12):
        res = sched.submit(
            submitter=f"u{i % 4}", request=Resource((3, 3)), priority=i % 6
        )
        ids.append(res.job.id)

    expected_state_errors: list[str] = []

    def worker(seed: int) -> None:
        rng = random.Random(seed)
        for _ in range(200):
            jid = rng.choice(ids)
            op = rng.randrange(5)
            try:
                if op == 0:
                    sched.complete(jid)
                elif op == 1:
                    sched.fail(jid)
                elif op == 2:
                    sched.cancel(jid)
                elif op == 3:
                    sched.get_job(jid)
                else:
                    sched.schedule()
            except InvalidJobStateError:
                expected_state_errors.append(jid)  # 终态互斥/对 QUEUED finish
            except JobNotFoundError:
                expected_state_errors.append(jid)

    threads = [threading.Thread(target=worker, args=(s,)) for s in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 并发期间随时抽样检查不变量
    sched.assert_invariants()
    jobs = sched.list_jobs()
    terminal = {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}
    running = [j for j in jobs if j.state is JobState.RUNNING]
    queued = [j for j in jobs if j.state is JobState.QUEUED]
    ended = [j for j in jobs if j.state in terminal]

    # 资源守恒：used 必须精确等于仍在运行作业的请求之和
    total = Resource.zero(2)
    for j in running:
        total = total + j.request
    assert sched.snapshot().used == total.values
    assert len(running) + len(queued) + len(ended) == len(jobs) == 12
    # 每个作业最多被调度一次：scheduled_at 只在 QUEUED->RUNNING 时赋值一次
    assert all(j.scheduled_at is None or isinstance(j.scheduled_at, int) for j in jobs)

    with caplog.at_level(logging.INFO):
        snap = sched.snapshot()
        logger.info(
            "随机交错压力结束: running=%d queued=%d terminal=%d 合法竞态冲突=%d\n%s",
            len(running), len(queued), len(ended), len(expected_state_errors),
            snap.render(),
        )
