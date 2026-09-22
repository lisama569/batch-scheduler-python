"""服务入口：uvicorn main:app --reload"""

from __future__ import annotations

from scheduler.service import create_app

app = create_app()
