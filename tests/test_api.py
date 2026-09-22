"""FastAPI HTTP 层端到端测试。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from batch_scheduler.models import Resources
from batch_scheduler.scheduler import Scheduler
from batch_scheduler.service import create_app


@pytest.fixture
def client(clock) -> TestClient:
    sched = Scheduler(
        node_capacity=Resources(cpu=8, memory_mb=8192, gpu=1),
        submitter_quotas={
            "team-a": Resources(cpu=4, memory_mb=4096, gpu=1),
            "team-b": Resources(cpu=8, memory_mb=8192, gpu=1),
        },
        clock_ns=clock,
    )
    return TestClient(create_app(sched))


def _body(key="k1", submitter="team-a", priority=0, **res):
    resources = {"cpu": 1, "memory_mb": 0, "gpu": 0}
    resources.update(res)
    return {
        "idempotency_key": key,
        "submitter": submitter,
        "priority": priority,
        "resources": resources,
        "duration_ms": 1000,
    }


def test_health_and_submit_flow(client: TestClient):
    assert client.get("/health").json() == {"status": "ok"}
    r = client.post("/jobs", json=_body(cpu=2, memory_mb=512))
    assert r.status_code == 201, r.text
    job = r.json()
    assert job["state"] == "RUNNING"
    assert job["schedule_count"] == 1

    r2 = client.get(f"/jobs/{job['id']}")
    assert r2.status_code == 200
    assert r2.json()["id"] == job["id"]


def test_rejected_reasons_over_http(client: TestClient):
    # 非法请求（零资源）
    r = client.post("/jobs", json=_body("bad", cpu=0, memory_mb=0, gpu=0))
    assert r.status_code == 422
    assert r.json()["reason"] == "INVALID_REQUEST"

    # 超容量
    r = client.post("/jobs", json=_body("huge", cpu=999))
    assert r.status_code == 422
    assert r.json()["reason"] == "EXCEEDS_NODE_CAPACITY"

    # 未知提交方
    r = client.post("/jobs", json=_body("ghost", submitter="ghost"))
    assert r.status_code == 422
    assert r.json()["reason"] == "UNKNOWN_SUBMITTER"

    # 幂等重复
    client.post("/jobs", json=_body("dup", cpu=1))
    r = client.post("/jobs", json=_body("dup", cpu=1))
    assert r.status_code == 409
    body = r.json()
    assert body["reason"] == "DUPLICATE_IDEMPOTENCY_KEY"
    assert body["existing_job"] is not None


def test_validation_schema_rejects_negative(client: TestClient):
    body = _body("neg", cpu=-1)
    r = client.post("/jobs", json=body)
    assert r.status_code == 422  # pydantic 层拦截


def test_queue_cancel_complete_lifecycle_over_http(client: TestClient):
    # 占满 team-a 配额与节点
    client.post("/jobs", json=_body("a-big", "team-a", cpu=4))
    queued = client.post("/jobs", json=_body("a-wait", "team-a", cpu=2)).json()
    assert queued["state"] == "PENDING"

    snap = client.get("/scheduler/snapshot").json()
    assert snap["used"] == {"cpu": 4, "memory_mb": 0, "gpu": 0}
    assert [p["job_id"] for p in snap["pending_jobs"]] == [queued["id"]]
    print("\n[HTTP] 排队快照:", snap)

    # 取消排队作业
    r = client.post(f"/jobs/{queued['id']}/cancel")
    assert r.status_code == 200 and r.json()["state"] == "CANCELLED"
    r = client.post(f"/jobs/{queued['id']}/cancel")
    assert r.status_code == 409  # 重复取消冲突

    # 完成运行中作业触发 tick
    r = client.post("/jobs/a-big/complete")  # 路径用幂等键查不到 -> 404
    assert r.status_code == 404

    running = client.get("/jobs").json()["jobs"][0]["id"]
    r = client.post(f"/jobs/{running}/complete")
    assert r.status_code == 200 and r.json()["state"] == "COMPLETED"

    r = client.post(f"/jobs/{running}/complete")
    assert r.status_code == 409  # 重复完成冲突

    snap = client.get("/scheduler/snapshot").json()
    assert snap["used"] == {"cpu": 0, "memory_mb": 0, "gpu": 0}
    assert snap["running_jobs"] == []
    print("[HTTP] 终态快照:", snap)


def test_manual_tick_endpoint_returns_decisions(client: TestClient):
    # team-b 占满节点 8cpu（同时也到配额），其排队作业 -> 配额阻塞判定
    client.post("/jobs", json=_body("full", "team-b", cpu=8))
    client.post("/jobs", json=_body("wait", "team-b", cpu=2))
    r = client.post("/scheduler/tick")
    assert r.status_code == 200
    decisions = r.json()["decisions"]
    blocked = [d for d in decisions if d["decision"] == "BLOCKED"]
    assert blocked, decisions
    assert "配额" in blocked[0]["reason"]
    assert blocked[0]["detail"]["submitter_quota_remaining"]["cpu"] == 0
    print("\n[HTTP] tick 判定:", decisions)

    # team-a 自身配额空闲，但节点已被占满 -> 容量阻塞判定
    client.post("/jobs", json=_body("a-wait", "team-a", cpu=1))
    r = client.post("/scheduler/tick")
    reasons = {d["job_id"]: d["reason"] for d in r.json()["decisions"]}
    assert any("容量" in reason for reason in reasons.values())
    print("[HTTP] 二次 tick 判定:", r.json()["decisions"])
