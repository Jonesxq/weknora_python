"""Celery 应用配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass

from celery import Celery

DEFAULT_CELERY_BROKER_URL = "redis://127.0.0.1:6379/0"
DEFAULT_CELERY_RESULT_BACKEND = "redis://127.0.0.1:6379/1"
_TRUE_VALUES = {"true", "1", "yes", "on"}
_FALSE_VALUES = {"false", "0", "no", "off"}


def _read_non_empty(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise ValueError(f"{name} 不能为空")
    return value


def _read_bool(name: str, default: bool) -> bool:
    value = os.getenv(name, str(default)).strip().casefold()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    raise ValueError(f"{name} 必须是 true/1/yes/on 或 false/0/no/off")


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
            broker_url=_read_non_empty("GRAPH_CELERY_BROKER_URL", DEFAULT_CELERY_BROKER_URL),
            result_backend=_read_non_empty(
                "GRAPH_CELERY_RESULT_BACKEND", DEFAULT_CELERY_RESULT_BACKEND
            ),
            task_always_eager=_read_bool("GRAPH_CELERY_TASK_ALWAYS_EAGER", False),
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
