"""Wiki Celery 批次任务入口。"""

from __future__ import annotations

import asyncio
from typing import Any, NoReturn
from uuid import UUID

from app.infrastructure.tasks.celery_app import celery_app
from app.wiki.ingest.errors import WikiBatchBusy
from app.wiki.ingest.schemas import BatchResult
from app.wiki.scope import WikiScope


def _build_scope(tenant_id: int, knowledge_base_id: str) -> WikiScope:
    if type(tenant_id) is not int or tenant_id <= 0:
        raise ValueError("tenant_id 必须是正整数")
    if not isinstance(knowledge_base_id, str):
        raise TypeError("knowledge_base_id 必须是规范 UUID 字符串")
    try:
        parsed_knowledge_base_id = UUID(knowledge_base_id)
    except ValueError as exc:
        raise ValueError("knowledge_base_id 必须是规范 UUID 字符串") from exc
    if str(parsed_knowledge_base_id) != knowledge_base_id:
        raise ValueError("knowledge_base_id 必须是规范 UUID 字符串")
    return WikiScope(
        tenant_id=tenant_id,
        knowledge_base_id=parsed_knowledge_base_id,
        actor_id="wiki-worker",
        can_write=True,
    )


def build_runtime() -> NoReturn:
    """任务 10 才会组装完整 Worker runtime。"""

    raise RuntimeError("Wiki Worker runtime 尚未组装，将在阶段二任务 10 实现")


def run_batch_sync(scope: WikiScope) -> BatchResult:
    """Celery 同步任务到异步 Worker 的可替换桥接点。"""

    runtime: Any = build_runtime()
    return asyncio.run(runtime.worker.run_batch(scope))


@celery_app.task(
    bind=True,
    name="wiki.batch.run",
    acks_late=True,
    reject_on_worker_lost=True,
)
def wiki_batch_task(
    self: Any, tenant_id: int, knowledge_base_id: str
) -> dict[str, object]:
    scope = _build_scope(tenant_id, knowledge_base_id)
    try:
        result = run_batch_sync(scope)
    except WikiBatchBusy as exc:
        raise self.retry(exc=exc, countdown=15, max_retries=10)
    if not isinstance(result, BatchResult):
        raise TypeError("Wiki Worker 必须返回 BatchResult")
    return result.model_dump(mode="json")
