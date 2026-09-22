"""FastAPI HTTP 服务层。"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import (
    JobNotFoundError,
    JobRejectedError,
    JobStateConflictError,
)
from .models import Job, Resources, SubmitRequest
from .scheduler import Scheduler

# 拒绝原因 -> HTTP 状态码（原因本身仍在响应体中给出，保证可区分）。
_REJECT_STATUS = {
    "INVALID_REQUEST": 422,
    "EXCEEDS_NODE_CAPACITY": 422,
    "DUPLICATE_IDEMPOTENCY_KEY": 409,
    "UNKNOWN_SUBMITTER": 422,
}


class ResourcesIn(BaseModel):
    cpu: int = Field(default=0, ge=0)
    memory_mb: int = Field(default=0, ge=0)
    gpu: int = Field(default=0, ge=0)


class JobSubmitIn(BaseModel):
    idempotency_key: str = Field(min_length=1)
    submitter: str = Field(min_length=1)
    priority: int = Field(default=0, ge=0)
    resources: ResourcesIn
    duration_ms: int = Field(default=1000, gt=0)


def _job_to_dict(job: Job) -> dict[str, object]:
    return {
        "id": job.id,
        "idempotency_key": job.idempotency_key,
        "submitter": job.submitter,
        "priority": job.priority,
        "state": job.state.value,
        "requested": job.requested.as_dict(),
        "created_at_ns": job.created_at_ns,
        "scheduled_at_ns": job.scheduled_at_ns,
        "finished_at_ns": job.finished_at_ns,
        "duration_ms": job.duration_ms,
        "schedule_count": job.schedule_count,
        "release_count": job.release_count,
        "scheduling_note": job.scheduling_note,
        "history": job.history,
    }


def _snapshot_to_dict(snap) -> dict[str, object]:
    return {
        "timestamp_ns": snap.timestamp_ns,
        "node_capacity": snap.node_capacity.as_dict(),
        "used": snap.used.as_dict(),
        "running_jobs": snap.running_jobs,
        "pending_jobs": snap.pending_jobs,
        "submitter_usage": snap.submitter_usage,
        "decisions": snap.decisions,
    }


def create_app(scheduler: Scheduler) -> FastAPI:
    app = FastAPI(
        title="单节点批处理作业调度服务",
        version="0.1.0",
    )

    @app.exception_handler(JobRejectedError)
    async def _on_rejected(_request, exc: JobRejectedError) -> JSONResponse:
        existing = getattr(exc, "existing_job", None)
        return JSONResponse(
            status_code=_REJECT_STATUS.get(exc.reason.value, 422),
            content={
                "error": "JOB_REJECTED",
                "reason": exc.reason.value,
                "message": str(exc),
                "existing_job": _job_to_dict(existing) if existing else None,
            },
        )

    @app.exception_handler(JobNotFoundError)
    async def _on_not_found(_request, exc: JobNotFoundError) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={"error": "JOB_NOT_FOUND", "message": str(exc)},
        )

    @app.exception_handler(JobStateConflictError)
    async def _on_conflict(_request, exc: JobStateConflictError) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"error": "JOB_STATE_CONFLICT", "message": str(exc)},
        )

    @app.get("/health")
    def health() -> dict[str, object]:
        return {"status": "ok"}

    @app.post("/jobs", status_code=201)
    def submit_job(payload: JobSubmitIn) -> dict[str, object]:
        req = SubmitRequest(
            idempotency_key=payload.idempotency_key,
            submitter=payload.submitter,
            priority=payload.priority,
            resources=Resources(
                cpu=payload.resources.cpu,
                memory_mb=payload.resources.memory_mb,
                gpu=payload.resources.gpu,
            ),
            duration_ms=payload.duration_ms,
        )
        job = scheduler.submit(req)
        return _job_to_dict(job)

    @app.get("/jobs")
    def list_jobs() -> dict[str, object]:
        return {"jobs": [_job_to_dict(j) for j in scheduler.list_jobs()]}

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, object]:
        return _job_to_dict(scheduler.get_job(job_id))

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: str) -> dict[str, object]:
        return _job_to_dict(scheduler.cancel(job_id))

    @app.post("/jobs/{job_id}/complete")
    def complete_job(job_id: str, failed: bool = False) -> dict[str, object]:
        return _job_to_dict(scheduler.complete(job_id, failed=failed))

    @app.post("/scheduler/tick")
    def run_tick() -> dict[str, object]:
        return _snapshot_to_dict(scheduler.tick())

    @app.get("/scheduler/snapshot")
    def get_snapshot() -> dict[str, object]:
        return _snapshot_to_dict(scheduler.snapshot())

    return app
