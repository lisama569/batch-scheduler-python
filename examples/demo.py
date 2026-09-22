"""端到端演示：提交 → 容量打满排队 → 配额限制 → 推进时间 → 完成释放 → 老化补位。

运行：uv run python examples/demo.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scheduler.models import Resource
from scheduler.scheduler import ManualClock, Scheduler


def show(sched: Scheduler, title: str) -> None:
    print(f"\n========== {title} ==========")
    print(sched.snapshot().render())


def main() -> None:
    clock = ManualClock()
    sched = Scheduler(Resource((8, 8)), clock=clock, aging_weight=1)

    # 1) 两个作业刚好占满 (8,8)
    sched.submit(submitter="a", request=Resource((4, 4)), priority=10,
                 idempotency_key="a-1")
    sched.submit(submitter="b", request=Resource((4, 4)), priority=5)

    # 2) 幂等：重复键不产生新作业
    dup = sched.submit(submitter="a", request=Resource((4, 4)), priority=10,
                       idempotency_key="a-1")
    print(f"幂等重复提交 -> duplicated={dup.duplicated}, job={dup.job.id}")

    # 3) 给提交方 c 设置配额 (2,2)，其两个 (2,2) 作业只能同时跑一个
    sched.set_quota("c", Resource((2, 2)))
    c1 = sched.submit(submitter="c", request=Resource((2, 2)), priority=1).job
    c2 = sched.submit(submitter="c", request=Resource((2, 2)), priority=1).job

    # 4) 一个低优先级大作业排队
    low = sched.submit(submitter="d", request=Resource((8, 8)), priority=0).job
    show(sched, "容量打满：a/b/c1 运行，c2 与 d 排队（含配额/容量判定）")
    for d in sched.schedule().decisions:
        print(f"  [{'ADMIT' if d.admitted else 'SKIP '}] {d.job_id}: {d.reason}")

    # 5) 推进逻辑时钟 30 tick，让低优先级大作业老化
    clock.advance(30)
    print(f"\n推进 30 tick 后，作业 {low.id} 的有效优先级 = "
          f"{low.priority + 1 * (clock() - low.submitted_at)}")

    # 6) 完成 a、b、c1：c2 受配额保护随后可上；d 老化分数最高先抢整节点
    sched.complete("job-00000001")
    sched.complete("job-00000002")
    show(sched, "a/b 完成：老化的 d 抢到整节点（c2 仍受 c 配额限制排队）")

    sched.complete("job-00000003")
    show(sched, "c1 也完成：c2 获得配额开始运行（d 需要整节点仍排队）")

    sched.complete("job-00000004")
    show(sched, "c2 完成：节点全空，老化的 d 终于获得整节点")
    assert sched.get_job(low.id).state.value == "RUNNING"

    sched.assert_invariants()
    print("\n不变量校验通过：资源守恒、无重复调度/重复释放。")


if __name__ == "__main__":
    main()
