"""调度器领域模型：资源、作业、快照。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class JobState(str, enum.Enum):
    """作业状态机：

    QUEUED -> RUNNING -> COMPLETED
                    \\-> FAILED
    QUEUED \\---------> CANCELLED
    RUNNING \\--------> CANCELLED
    """

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# 终态集合与活跃态集合（模块加载后由 fill 阶段补充使用）
TERMINAL_STATES: frozenset[JobState] = frozenset(
    {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}
)


@dataclass(frozen=True)
class Resource:
    """多维资源向量，例如 (cpu, memory_gb, gpu)。

    每个维度必须为非负整数；合法作业请求的每个维度必须 >= 1。
    """

    values: tuple[int, ...]

    @property
    def dimensions(self) -> int:
        raise NotImplementedError

    def fits_in(self, other: "Resource") -> bool:
        """self 的每一维都不超过 other。"""
        raise NotImplementedError

    def __add__(self, other: "Resource") -> "Resource":
        raise NotImplementedError

    def __sub__(self, other: "Resource") -> "Resource":
        raise NotImplementedError


@dataclass
class Job:
    """一个批处理作业。"""

    id: str
    submitter: str
    request: Resource
    priority: int  # 数值越大优先级越高
    state: JobState
    submitted_at: int  # 单调提交序号，同时作为等待时长基准
    scheduled_at: int | None = None
    finished_at: int | None = None
    cancel_requested: bool = False
    idempotency_key: str | None = None


@dataclass
class SubmitterQuota:
    """提交方可占用资源上限（每一维）。None 表示不单独限制（用默认配额）。"""

    submitter: str
    limit: Resource | None


@dataclass
class QueuedDecision:
    """单次调度中对一个排队作业的判定依据（用于可解释快照）。"""

    job_id: str
    submitter: str
    priority: int
    waited: int
    effective_priority: int
    admitted: bool
    reason: str
    request: tuple[int, ...] | None = None


@dataclass
class SchedulerSnapshot:
    """调度快照：某一时刻的资源视图与排队判定依据。"""

    tick: int
    capacity: tuple[int, ...]
    used: tuple[int, ...]
    available: tuple[int, ...]
    running_jobs: list[str] = field(default_factory=list)
    queued_jobs: list[str] = field(default_factory=list)
    submitter_used: dict[str, tuple[int, ...]] = field(default_factory=dict)
    decisions: list[QueuedDecision] = field(default_factory=list)

    def render(self) -> str:
        """渲染为人类可读的多行文本（测试日志打印用）。"""
        raise NotImplementedError
