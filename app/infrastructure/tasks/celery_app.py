"""Celery 应用配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass

from celery import Celery


@dataclass(frozen=True, slots=True)
class CelerySettings:
    """Celery 连接和执行参数。"""

    broker_url: str
    result_backend: str
    task_always_eager: bool = False

    @classmethod
    def from_env(cls) -> "CelerySettings":
        """从环境变量读取 Celery 配置。"""

        return cls(
            broker_url=os.getenv("GRAPH_CELERY_BROKER_URL", "redis://127.0.0.1:6379/0"),
            result_backend=os.getenv("GRAPH_CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/1"),
            task_always_eager=os.getenv("GRAPH_CELERY_TASK_ALWAYS_EAGER", "false").casefold()
            == "true",
        )


def create_celery_app(settings: CelerySettings) -> Celery:
    """使用指定配置创建 Celery 应用。"""

    app = Celery(
        "graph",
        broker=settings.broker_url,
        backend=settings.result_backend,
        include=["app.wiki.tasks.wiki_tasks"],
    )
    app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        enable_utc=True,
        timezone="UTC",
        task_always_eager=settings.task_always_eager,
        task_eager_propagates=True,
    )
    return app


celery_app = create_celery_app(CelerySettings.from_env())
