"""调度器错误类型。

错误码（code）用于让调用方区分不同的拒绝/失败原因：

- INVALID_REQUEST          : 请求体非法（缺字段/类型错误等）
- INVALID_RESOURCE         : 资源向量非法（负数/含零维/维度不匹配）
- INVALID_PRIORITY         : 优先级越界
- RESOURCE_EXCEEDS_CAPACITY: 单作业请求超过节点总容量
- IDEMPOTENCY_CONFLICT     : 同一幂等键重复提交但参数不一致
- JOB_NOT_FOUND            : 作业不存在
- INVALID_JOB_STATE        : 作业当前状态不允许该操作
"""

from __future__ import annotations


class SchedulerError(Exception):
    """所有调度器错误的基类，携带可区分的错误码。"""

    code: str = "SCHEDULER_ERROR"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class InvalidRequestError(SchedulerError):
    code = "INVALID_REQUEST"


class InvalidResourceError(SchedulerError):
    code = "INVALID_RESOURCE"


class InvalidPriorityError(SchedulerError):
    code = "INVALID_PRIORITY"


class ResourceExceedsCapacityError(SchedulerError):
    code = "RESOURCE_EXCEEDS_CAPACITY"


class IdempotencyConflictError(SchedulerError):
    code = "IDEMPOTENCY_CONFLICT"


class JobNotFoundError(SchedulerError):
    code = "JOB_NOT_FOUND"


class InvalidJobStateError(SchedulerError):
    code = "INVALID_JOB_STATE"
