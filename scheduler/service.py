"""FastAPI 服务层：把调度核心暴露为 HTTP 接口。"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from scheduler.errors import (
    IdempotencyConflictError,
    InvalidJobStateError,
    JobNotFoundError,
    ResourceExceedsCapacityError,
    SchedulerError,
)
from scheduler.models import Job, Resource
from scheduler.scheduler import ManualClock, Scheduler

logger = logging.getLogger("batch-scheduler")

# 拒绝类错误 -> HTTP 状态码；错误码字段（body.code）用于进一步区分原因
_STATUS_MAP = {
    "INVALID_REQUEST": 422,
    "INVALID_RESOURCE": 422,
    "INVALID_PRIORITY": 422,
    "RESOURCE_EXCEEDS_CAPACITY": 422,
    "IDEMPOTENCY_CONFLICT": 409,
    "JOB_NOT_FOUND": 404,
    "INVALID_JOB_STATE": 409,
}


class SubmitBody(BaseModel):
    submitter: str = Field(min_length=1, description="提交方标识")
    resources: list[int] = Field(description="资源向量，如 [cpu, memory_gb, gpu]")
    priority: int = 0
    idempotency_key: str | None = None


class QuotaBody(BaseModel):
    limit: list[int] | None = Field(
        description="配额向量；null 表示清除该提交方的自定义配额"
    )


class TickBody(BaseModel):
    n: int = 1


def job_to_dict(job: Job) -> dict:
    return {
        "id": job.id,
        "submitter": job.submitter,
        "request": list(job.request.values),
        "priority": job.priority,
        "state": job.state.value,
        "submitted_at": job.submitted_at,
        "scheduled_at": job.scheduled_at,
        "finished_at": job.finished_at,
        "cancel_requested": job.cancel_requested,
        "idempotency_key": job.idempotency_key,
    }


def snapshot_to_dict(snap) -> dict:
    return {
        "tick": snap.tick,
        "capacity": list(snap.capacity),
        "used": list(snap.used),
        "available": list(snap.available),
        "running_jobs": snap.running_jobs,
        "queued_jobs": snap.queued_jobs,
        "submitter_used": {k: list(v) for k, v in snap.submitter_used.items()},
        "decisions": [
            {
                "job_id": d.job_id,
                "submitter": d.submitter,
                "priority": d.priority,
                "waited": d.waited,
                "effective_priority": d.effective_priority,
                "admitted": d.admitted,
                "reason": d.reason,
                "request": list(d.request) if d.request is not None else None,
            }
            for d in snap.decisions
        ],
    }


def default_scheduler() -> Scheduler:
    """默认节点：CPU=16, 内存=32GB, GPU=4；可用 ManualClock 手动推进等待时间。"""
    clock = ManualClock()
    return Scheduler(Resource((16, 32, 4)), aging_weight=1, clock=clock)


def create_app(scheduler: Scheduler | None = None) -> FastAPI:
    sched = scheduler or default_scheduler()
    app = FastAPI(title="batch-scheduler", version="0.1.0")
    app.state.scheduler = sched

    @app.exception_handler(SchedulerError)
    async def handle_scheduler_error(_: Request, exc: SchedulerError) -> JSONResponse:
        status = _STATUS_MAP.get(exc.code, 400)
        return JSONResponse(
            status_code=status,
            content={"error": {"code": exc.code, "message": str(exc)}},
        )

    @app.get("/")
    def root() -> dict:
        snap = sched.snapshot()
        return {
            "service": "batch-scheduler",
            "capacity": list(snap.capacity),
            "used": list(snap.used),
            "available": list(snap.available),
            "running": len(snap.running_jobs),
            "queued": len(snap.queued_jobs),
        }

    @app.post("/jobs", status_code=201)
    def submit_job(body: SubmitBody) -> dict:
        result = sched.submit(
            submitter=body.submitter,
            request=Resource(tuple(body.resources)),
            priority=body.priority,
            idempotency_key=body.idempotency_key,
        )
        return {"job": job_to_dict(result.job), "duplicated": result.duplicated}

    @app.get("/jobs")
    def list_jobs() -> dict:
        return {"jobs": [job_to_dict(j) for j in sched.list_jobs()]}

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict:
        return job_to_dict(sched.get_job(job_id))

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: str) -> dict:
        return job_to_dict(sched.cancel(job_id))

    @app.post("/jobs/{job_id}/complete")
    def complete_job(job_id: str) -> dict:
        return job_to_dict(sched.complete(job_id))

    @app.post("/jobs/{job_id}/fail")
    def fail_job(job_id: str) -> dict:
        return job_to_dict(sched.fail(job_id))

    @app.post("/quotas/{submitter}")
    def set_quota(submitter: str, body: QuotaBody) -> dict:
        limit = Resource(tuple(body.limit)) if body.limit is not None else None
        sched.set_quota(submitter, limit)
        return {"submitter": submitter, "limit": body.limit}

    @app.get("/snapshot")
    def get_snapshot() -> dict:
        return snapshot_to_dict(sched.snapshot())

    @app.post("/schedule")
    def run_schedule() -> dict:
        snap = sched.schedule()
        logger.info("schedule pass @tick=%s:\n%s", snap.tick, snap.render())
        return snapshot_to_dict(snap)

    @app.post("/tick")
    def advance_clock(body: TickBody) -> dict:
        return {"tick": sched.tick(body.n)}

    return app
