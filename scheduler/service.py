"""FastAPI 服务层：把调度核心暴露为 HTTP 接口。"""

from __future__ import annotations

from fastapi import FastAPI

from scheduler.scheduler import Scheduler


def create_app(scheduler: Scheduler | None = None) -> FastAPI:
    """构造 FastAPI 应用；scheduler 为 None 时由 fill 阶段提供默认节点。"""
    app = FastAPI(title="batch-scheduler")
    # fill 阶段：绑定调度器、注册路由、错误处理
    app.state.scheduler = scheduler
    return app
