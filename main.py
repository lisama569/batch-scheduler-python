"""服务入口：构造节点容量、提交方配额并启动 FastAPI。"""

from __future__ import annotations

from batch_scheduler.models import Resources
from batch_scheduler.scheduler import Scheduler
from batch_scheduler.service import create_app

NODE_CAPACITY = Resources(cpu=16, memory_mb=32768, gpu=2)

SUBMITTER_QUOTAS = {
    "team-a": Resources(cpu=8, memory_mb=16384, gpu=2),
    "team-b": Resources(cpu=8, memory_mb=16384, gpu=1),
}

scheduler = Scheduler(
    node_capacity=NODE_CAPACITY,
    submitter_quotas=SUBMITTER_QUOTAS,
)

app = create_app(scheduler)
