"""调度器领域模型：资源、作业、快照。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class JobState(str, enum.Enum):
    """作业状态机：

    QUEUED -> RUNNING -> COMPLETED
                    \\-> FAILED
    QUEUED \\---------> CANCELLED
    RUNNING \\-------> CANCELLED
    """

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# 终态集合：终态作业永远不再参与调度，也不会再次释放资源
TERMINAL_STATES: frozenset[JobState] = frozenset(
    {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}
)


@dataclass(frozen=True)
class Resource:
    """多维资源向量，例如 (cpu, memory_gb, gpu)。

    每个维度必须为非负整数；合法作业请求的每个维度必须 >= 1
    （由调度器提交入口校验）。
    """

    values: tuple[int, ...]

    @property
    def dimensions(self) -> int:
        return len(self.values)

    @classmethod
    def zero(cls, dimensions: int) -> "Resource":
        return cls(tuple(0 for _ in range(dimensions)))  # type: ignore[arg-type]

    def _check_dim(self, other: "Resource") -> None:
        if self.dimensions != other.dimensions:
            raise ValueError(
                f"资源维度不一致: {self.dimensions} != {other.dimensions}"
            )

    def fits_in(self, other: "Resource") -> bool:
        """self 的每一维都不超过 other。"""
        self._check_dim(other)
        return all(a <= b for a, b in zip(self.values, other.values))

    def all_positive(self) -> bool:
        """每个维度都 >= 1（作业请求的合法性要求）。"""
        return len(self.values) > 0 and all(v >= 1 for v in self.values)

    def all_non_negative(self) -> bool:
        return len(self.values) > 0 and all(v >= 0 for v in self.values)

    def __add__(self, other: "Resource") -> "Resource":
        self._check_dim(other)
        return Resource(tuple(a + b for a, b in zip(self.values, other.values)))

    def __sub__(self, other: "Resource") -> "Resource":
        self._check_dim(other)
        return Resource(tuple(a - b for a, b in zip(self.values, other.values)))

    def __str__(self) -> str:
        return "(" + ",".join(str(v) for v in self.values) + ")"


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


@dataclass(frozen=True)
class SubmitResult:
    """提交结果：duplicated=True 表示命中幂等键，返回的是既有作业。"""

    job: Job
    duplicated: bool


@dataclass
class SubmitterQuota:
    """提交方可占用资源上限（每一维）。limit=None 表示退回默认配额。"""

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
        lines = [
            f"[scheduler snapshot tick={self.tick}]",
            f"capacity : {self.capacity}",
            f"used     : {self.used}",
            f"available: {self.available}",
        ]
        if self.submitter_used:
            lines.append("submitter usage:")
            for name in sorted(self.submitter_used):
                lines.append(f"  - {name}: used={self.submitter_used[name]}")
        lines.append(f"running ({len(self.running_jobs)}): {self.running_jobs}")
        lines.append(f"queued  ({len(self.queued_jobs)}): {self.queued_jobs}")
        if self.decisions:
            lines.append("decisions:")
            for d in self.decisions:
                verdict = "ADMIT" if d.admitted else "SKIP"
                lines.append(
                    f"  - [{verdict}] {d.job_id} submitter={d.submitter} "
                    f"base_priority={d.priority} waited={d.waited} "
                    f"effective_priority={d.effective_priority} "
                    f"request={d.request} reason={d.reason}"
                )
        return "\n".join(lines)
