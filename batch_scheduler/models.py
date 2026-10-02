"""领域模型与类型定义。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class JobState(str, Enum):
    """作业生命周期状态。

    WAITING：已受理但依赖未就绪，不进排队队列、不占资源与配额。
    DEPENDENCY_FAILED：上游失败/被取消导致的终态，永不自动运行。
    """

    WAITING = "WAITING"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    DEPENDENCY_FAILED = "DEPENDENCY_FAILED"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_STATES


_TERMINAL_STATES = frozenset(
    {
        JobState.COMPLETED,
        JobState.FAILED,
        JobState.CANCELLED,
        JobState.DEPENDENCY_FAILED,
    }
)


class RejectReason(str, Enum):
    """提交被拒绝的可区分原因。"""

    INVALID_REQUEST = "INVALID_REQUEST"
    EXCEEDS_NODE_CAPACITY = "EXCEEDS_NODE_CAPACITY"
    DUPLICATE_IDEMPOTENCY_KEY = "DUPLICATE_IDEMPOTENCY_KEY"
    UNKNOWN_SUBMITTER = "UNKNOWN_SUBMITTER"
    UNKNOWN_DEPENDENCY = "UNKNOWN_DEPENDENCY"
    SELF_DEPENDENCY = "SELF_DEPENDENCY"
    DEPENDENCY_CYCLE = "DEPENDENCY_CYCLE"


@dataclass(frozen=True)
class Resources:
    """资源向量（不可变值对象）。"""

    cpu: int = 0
    memory_mb: int = 0
    gpu: int = 0

    def fits_in(self, other: "Resources") -> bool:
        """各维度均不超过 other。"""
        return (
            self.cpu <= other.cpu
            and self.memory_mb <= other.memory_mb
            and self.gpu <= other.gpu
        )

    def __add__(self, other: "Resources") -> "Resources":
        return Resources(
            cpu=self.cpu + other.cpu,
            memory_mb=self.memory_mb + other.memory_mb,
            gpu=self.gpu + other.gpu,
        )

    def __sub__(self, other: "Resources") -> "Resources":
        return Resources(
            cpu=self.cpu - other.cpu,
            memory_mb=self.memory_mb - other.memory_mb,
            gpu=self.gpu - other.gpu,
        )

    def as_dict(self) -> dict[str, int]:
        return {"cpu": self.cpu, "memory_mb": self.memory_mb, "gpu": self.gpu}

    def is_non_negative(self) -> bool:
        return self.cpu >= 0 and self.memory_mb >= 0 and self.gpu >= 0

    def is_positive_request(self) -> bool:
        """合法资源请求：至少申请一个维度的资源，且各维度非负。"""
        return self.is_non_negative() and (
            self.cpu > 0 or self.memory_mb > 0 or self.gpu > 0
        )


@dataclass
class SubmitRequest:
    """作业提交请求。"""

    idempotency_key: str
    submitter: str
    priority: int
    resources: Resources
    duration_ms: int = 1000
    depends_on: list[str] = field(default_factory=list)


@dataclass
class Job:
    """作业运行时记录。"""

    id: str
    idempotency_key: str
    submitter: str
    priority: int
    requested: Resources
    state: JobState = JobState.PENDING
    created_at_ns: int = 0
    scheduled_at_ns: int | None = None
    finished_at_ns: int | None = None
    duration_ms: int = 1000
    depends_on: list[str] = field(default_factory=list)
    schedule_count: int = 0
    release_count: int = 0
    scheduling_note: str = ""
    history: list[str] = field(default_factory=list)


@dataclass
class SchedulingSnapshot:
    """调度快照，用于可解释性与测试断言。"""

    timestamp_ns: int = 0
    node_capacity: Resources | None = None
    used: Resources | None = None
    running_jobs: list[str] = field(default_factory=list)
    pending_jobs: list[dict[str, Any]] = field(default_factory=list)
    waiting_jobs: list[dict[str, Any]] = field(default_factory=list)
    submitter_usage: dict[str, dict[str, int]] = field(default_factory=dict)
    decisions: list[dict[str, Any]] = field(default_factory=list)
