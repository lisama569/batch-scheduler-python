"""HTTP 接口单测：错误码可区分、幂等语义、生命周期与快照接口。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from scheduler.models import Resource
from scheduler.scheduler import ManualClock, Scheduler
from scheduler.service import create_app

logger = logging.getLogger("batch-scheduler.test")


@pytest.fixture()
def client(caplog):
    clock = ManualClock()
    sched = Scheduler(Resource((8, 16)), clock=clock, aging_weight=1)
    app = create_app(sched)
    app.state.clock = clock
    app.state.scheduler = sched
    with caplog.at_level(logging.INFO, logger="batch-scheduler"):
        yield TestClient(app)
    logger.info("用例结束快照:\n%s", sched.snapshot().render())


def test_root_and_submit_flow(client) -> None:
    r = client.post(
        "/jobs", json={"submitter": "a", "resources": [4, 8], "priority": 5}
    )
    assert r.status_code == 201
    body = r.json()
    assert body["duplicated"] is False
    assert body["job"]["state"] == "RUNNING"
    assert client.get("/").json()["used"] == [4, 8]


def test_rejection_reasons_are_distinguishable(client) -> None:
    # 前 5 个请求体通过 Pydantic 结构校验，错误由调度服务给出可区分错误码
    cases = [
        ({"submitter": "a", "resources": [9, 1]}, "RESOURCE_EXCEEDS_CAPACITY"),
        ({"submitter": "a", "resources": [0, 8]}, "INVALID_RESOURCE"),
        ({"submitter": "a", "resources": [-1, 8]}, "INVALID_RESOURCE"),
        ({"submitter": "a", "resources": [4]}, "INVALID_RESOURCE"),
        ({"submitter": "a", "resources": [4, 8], "priority": 999},
         "INVALID_PRIORITY"),
    ]
    seen = {}
    for payload, code in cases:
        r = client.post("/jobs", json=payload)
        assert r.status_code == 422, payload
        assert r.json()["error"]["code"] == code, payload
        seen[code] = seen.get(code, 0) + 1
    # 空提交方由 Pydantic 结构校验直接拒绝（同样是 422）
    r = client.post("/jobs", json={"submitter": "", "resources": [4, 8]})
    assert r.status_code == 422
    seen["INVALID_REQUEST"] = seen.get("INVALID_REQUEST", 0) + 1
    logger.info("被拒绝原因分布（可区分）: %s", seen)
    assert set(seen) >= {
        "RESOURCE_EXCEEDS_CAPACITY",
        "INVALID_RESOURCE",
        "INVALID_PRIORITY",
        "INVALID_REQUEST",
    }


def test_idempotency_over_http(client) -> None:
    payload = {"submitter": "a", "resources": [2, 4], "idempotency_key": "dup"}
    r1 = client.post("/jobs", json=payload)
    r2 = client.post("/jobs", json=payload)
    assert r1.status_code == r2.status_code == 201
    assert r1.json()["job"]["id"] == r2.json()["job"]["id"]
    assert r2.json()["duplicated"] is True
    assert len(client.get("/jobs").json()["jobs"]) == 1

    conflict = client.post(
        "/jobs",
        json={"submitter": "a", "resources": [2, 5], "idempotency_key": "dup"},
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


def test_lifecycle_and_404(client) -> None:
    jid = client.post(
        "/jobs", json={"submitter": "a", "resources": [8, 16]}
    ).json()["job"]["id"]
    assert client.get(f"/jobs/{jid}").json()["state"] == "RUNNING"

    assert client.post(f"/jobs/{jid}/complete").status_code == 200
    # 重复完成幂等
    assert client.post(f"/jobs/{jid}/complete").json()["state"] == "COMPLETED"
    # 终态互斥
    assert client.post(f"/jobs/{jid}/fail").status_code == 409
    # 不存在
    assert client.get("/jobs/missing").status_code == 404
    assert client.post("/jobs/missing/cancel").json()["error"]["code"] == (
        "JOB_NOT_FOUND"
    )


def test_queue_schedule_snapshot_and_quota(client) -> None:
    # 占满
    client.post("/jobs", json={"submitter": "a", "resources": [8, 16]})
    queued = client.post(
        "/jobs", json={"submitter": "b", "resources": [4, 8], "priority": 2}
    ).json()
    assert queued["job"]["state"] == "QUEUED"

    snap = client.get("/snapshot").json()
    assert snap["available"] == [0, 0]
    assert snap["queued_jobs"] == ["job-00000002"]

    # 配额接口：把 b 限到 (2,2)，其排队作业即使将来有容量也会先受配额限制
    rq = client.post("/quotas/b", json={"limit": [2, 2]})
    assert rq.status_code == 200
    assert rq.json()["limit"] == [2, 2]

    # 完成后触发调度；(4,8) 超出 b 配额 (2,2)，继续排队
    client.post("/jobs/job-00000001/complete")
    got = client.get("/jobs/job-00000002").json()
    assert got["state"] == "QUEUED"
    # 显式调度返回判定依据
    dec = client.post("/schedule").json()["decisions"][0]
    logger.info("HTTP 调度判定: %s", dec)
    assert dec["admitted"] is False and "配额不足" in dec["reason"]

    # 放宽配额后立刻可调度
    client.post("/quotas/b", json={"limit": [8, 16]})
    r = client.post("/schedule")
    assert r.json()["used"] == [4, 8]
    assert client.get("/jobs/job-00000002").json()["state"] == "RUNNING"


def test_tick_advances_clock(client) -> None:
    assert client.post("/tick", json={"n": 3}).json()["tick"] == 3
    assert client.post("/tick", json={"n": 2}).json()["tick"] == 5
