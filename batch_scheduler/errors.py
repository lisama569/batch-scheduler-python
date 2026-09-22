"""领域错误类型。"""

from __future__ import annotations

from .models import RejectReason


class SchedulerError(Exception):
    """调度器错误基类。"""

    def __init__(self, message: str, reason: RejectReason) -> None:
        super().__init__(message)
        self.reason = reason


class JobRejectedError(SchedulerError):
    """作业提交被拒绝。"""


class JobNotFoundError(Exception):
    """作业不存在。"""


class JobStateConflictError(Exception):
    """作业状态冲突（重复完成 / 重复取消 / 重复释放等）。"""
