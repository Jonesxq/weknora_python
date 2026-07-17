"""可靠领取并投递 Wiki Outbox 事件。"""

from __future__ import annotations

import asyncio
import logging
import math
import os
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine

from app.infrastructure.database.config import DatabaseSettings
from app.infrastructure.database.session import (
    create_database_engine,
    create_session_factory,
)
from app.wiki.ingest.store import (
    InvariantError,
    OutboxEventRecord,
    SqlAlchemyIngestStore,
    SqlFinalizationPort,
)


logger = logging.getLogger(__name__)
WIKI_BATCH_EVENT_TYPE = "wiki.batch.trigger"


class OutboxStore(Protocol):
    async def claim_outbox(
        self, limit: int, claim_timeout: timedelta | int
    ) -> list[OutboxEventRecord]: ...

    async def mark_outbox_sent(
        self, ids: Sequence[UUID], claim_token: UUID | None
    ) -> None: ...

    async def release_outbox(
        self, ids: Sequence[UUID], claim_token: UUID | None
    ) -> None: ...


class OutboxPublisher(Protocol):
    async def publish(self, event: OutboxEventRecord) -> None: ...


class CeleryTask(Protocol):
    def apply_async(self, *args: object, **kwargs: object) -> object: ...


class OutboxDispatcher:
    """按 claim 批次串行发布，每个事件成功后立即确认。"""

    def __init__(
        self,
        store: OutboxStore,
        publisher: OutboxPublisher,
        batch_size: int = 100,
        claim_timeout: timedelta | int = 60,
    ) -> None:
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size 必须是正整数")
        if isinstance(claim_timeout, timedelta):
            valid_timeout = claim_timeout.total_seconds() > 0
        else:
            valid_timeout = not isinstance(claim_timeout, bool) and claim_timeout > 0
        if not valid_timeout:
            raise ValueError("claim_timeout 必须大于 0")
        self.store = store
        self.publisher = publisher
        self.batch_size = batch_size
        self.claim_timeout = claim_timeout

    async def dispatch_once(self) -> int:
        events = await self.store.claim_outbox(self.batch_size, self.claim_timeout)
        if not events:
            return 0

        token = events[0].claim_token
        if not isinstance(token, UUID) or any(
            event.claim_token != token for event in events
        ):
            raise InvariantError("Outbox 批次必须共享同一个非空 claim token")
        event_ids = [event.id for event in events]
        if len(event_ids) != len(set(event_ids)):
            raise InvariantError("Outbox 批次事件 ID 不能重复")

        first_error: tuple[Exception, TracebackType | None] | None = None
        confirmed = 0
        for index, event in enumerate(events):
            try:
                await self.publisher.publish(event)
            except Exception as exc:
                first_error = first_error or (exc, exc.__traceback__)
                logger.error(
                    "Outbox 事件发布失败",
                    extra={
                        "outbox_event_ids": [str(event.id)],
                        "outbox_error_type": type(exc).__name__,
                    },
                )
                await self._release_without_masking([event.id], token)
                continue
            except BaseException:
                await self._release_without_masking(
                    [remaining.id for remaining in events[index:]], token
                )
                raise

            try:
                await self.store.mark_outbox_sent([event.id], token)
            except Exception as exc:
                # broker 已可能接收，不能释放；由 stale claim 触发至少一次重投。
                first_error = first_error or (exc, exc.__traceback__)
                logger.error(
                    "Outbox 事件确认失败",
                    extra={
                        "outbox_event_ids": [str(event.id)],
                        "outbox_error_type": type(exc).__name__,
                    },
                )
                continue
            except BaseException:
                await self._release_without_masking(
                    [remaining.id for remaining in events[index + 1 :]], token
                )
                raise
            confirmed += 1

        if first_error is not None:
            error, traceback = first_error
            raise error.with_traceback(traceback)
        return confirmed

    async def _release_without_masking(
        self, event_ids: Sequence[UUID], claim_token: UUID
    ) -> None:
        if not event_ids:
            return
        try:
            await self.store.release_outbox(event_ids, claim_token)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Outbox claim 释放失败",
                extra={
                    "outbox_event_ids": [str(event_id) for event_id in event_ids],
                    "outbox_error_type": type(exc).__name__,
                },
            )


class CeleryBatchPublisher:
    """把受信任的 Wiki 触发事件发布为固定 Celery 任务。"""

    def __init__(self, task: CeleryTask | None = None) -> None:
        if task is None:
            from app.wiki.tasks.wiki_tasks import wiki_batch_task

            task = wiki_batch_task
        self.task = task

    async def publish(self, event: OutboxEventRecord) -> None:
        kwargs = _task_kwargs(event)
        await asyncio.to_thread(
            self.task.apply_async,
            kwargs=kwargs,
            task_id=str(event.id),
        )


def _task_kwargs(event: OutboxEventRecord) -> dict[str, object]:
    if event.event_type != WIKI_BATCH_EVENT_TYPE:
        raise ValueError(f"不支持的 Outbox 事件类型: {event.event_type}")
    payload = event.payload
    if not isinstance(payload, dict) or set(payload) != {
        "tenant_id",
        "knowledge_base_id",
    }:
        raise ValueError("Wiki 批次 payload 必须精确包含租户和知识库")

    tenant_id = payload["tenant_id"]
    if type(tenant_id) is not int or tenant_id <= 0:
        raise ValueError("tenant_id 必须是正整数")
    raw_knowledge_base_id = payload["knowledge_base_id"]
    if not isinstance(raw_knowledge_base_id, str):
        raise ValueError("knowledge_base_id 必须是规范 UUID 字符串")
    try:
        knowledge_base_id = UUID(raw_knowledge_base_id)
    except ValueError as exc:
        raise ValueError("knowledge_base_id 必须是规范 UUID 字符串") from exc
    if str(knowledge_base_id) != raw_knowledge_base_id:
        raise ValueError("knowledge_base_id 必须是规范 UUID 字符串")
    if tenant_id != event.tenant_id or knowledge_base_id != event.knowledge_base_id:
        raise ValueError("Outbox payload 与事件租户或知识库不一致")
    return {
        "tenant_id": tenant_id,
        "knowledge_base_id": raw_knowledge_base_id,
    }


def _read_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} 必须是整数") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} 必须在 {minimum} 到 {maximum} 之间")
    return value


def _read_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} 必须是数字") from exc
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError(f"{name} 必须在 {minimum} 到 {maximum} 之间")
    return value


@dataclass(frozen=True, slots=True)
class OutboxDispatcherSettings:
    """Outbox dispatcher 的有界运行参数。"""

    batch_size: int = 100
    poll_seconds: float = 1.0
    claim_timeout_seconds: int = 60

    def __post_init__(self) -> None:
        if type(self.batch_size) is not int or not 1 <= self.batch_size <= 1000:
            raise ValueError("batch_size 必须在 1 到 1000 之间")
        if (
            type(self.poll_seconds) not in {int, float}
            or not math.isfinite(self.poll_seconds)
            or not 0.01 <= self.poll_seconds <= 60
        ):
            raise ValueError("poll_seconds 必须在 0.01 到 60 之间")
        if (
            type(self.claim_timeout_seconds) is not int
            or not 1 <= self.claim_timeout_seconds <= 86400
        ):
            raise ValueError("claim_timeout_seconds 必须在 1 到 86400 之间")

    @classmethod
    def from_env(cls) -> "OutboxDispatcherSettings":
        return cls(
            batch_size=_read_int(
                "GRAPH_WIKI_OUTBOX_BATCH_SIZE", 100, minimum=1, maximum=1000
            ),
            poll_seconds=_read_float(
                "GRAPH_WIKI_OUTBOX_POLL_SECONDS",
                1.0,
                minimum=0.01,
                maximum=60,
            ),
            claim_timeout_seconds=_read_int(
                "GRAPH_WIKI_OUTBOX_CLAIM_TIMEOUT_SECONDS",
                60,
                minimum=1,
                maximum=86400,
            ),
        )


@dataclass(frozen=True, slots=True)
class DispatcherRuntime:
    engine: AsyncEngine
    dispatcher: OutboxDispatcher
    poll_seconds: float


def build_dispatcher_runtime(
    *,
    settings: OutboxDispatcherSettings | None = None,
    database_settings: DatabaseSettings | None = None,
    task: CeleryTask | None = None,
) -> DispatcherRuntime:
    """组装真实 PostgreSQL store；创建 engine 不会立即建立连接。"""

    settings = settings or OutboxDispatcherSettings.from_env()
    database_settings = database_settings or DatabaseSettings.from_env()
    if database_settings.url.drivername != "postgresql+asyncpg":
        raise ValueError("Outbox dispatcher 只支持 PostgreSQL asyncpg")
    engine = create_database_engine(database_settings)
    session_factory = create_session_factory(engine)
    store = SqlAlchemyIngestStore(session_factory, SqlFinalizationPort())
    dispatcher = OutboxDispatcher(
        store,
        CeleryBatchPublisher(task=task),
        batch_size=settings.batch_size,
        claim_timeout=settings.claim_timeout_seconds,
    )
    return DispatcherRuntime(engine, dispatcher, settings.poll_seconds)


async def run_dispatcher_loop(
    dispatcher: OutboxDispatcher,
    *,
    poll_seconds: float,
    max_backoff_seconds: float = 60,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """持续投递；普通轮次故障被记录并在下一轮重试。"""

    if (
        type(poll_seconds) not in {int, float}
        or not math.isfinite(poll_seconds)
        or poll_seconds <= 0
    ):
        raise ValueError("poll_seconds 必须是有限正数")
    if (
        type(max_backoff_seconds) not in {int, float}
        or not math.isfinite(max_backoff_seconds)
        or max_backoff_seconds < poll_seconds
    ):
        raise ValueError("max_backoff_seconds 必须是不小于 poll_seconds 的有限正数")
    consecutive_failures = 0
    while True:
        try:
            dispatched = await dispatcher.dispatch_once()
            consecutive_failures = 0
            logger.info(
                "Outbox dispatcher 完成一轮投递",
                extra={"outbox_dispatched_count": dispatched},
            )
        except asyncio.CancelledError:
            logger.info("Outbox dispatcher 收到取消信号，正在退出")
            raise
        except Exception as exc:
            consecutive_failures += 1
            logger.error(
                "Outbox dispatcher 本轮投递失败",
                extra={"outbox_error_type": type(exc).__name__},
            )
        delay = min(
            poll_seconds * (2 ** min(max(consecutive_failures - 1, 0), 63)),
            max_backoff_seconds,
        )
        await sleep(delay)


async def _run_main(
    runtime_builder: Callable[[], DispatcherRuntime] = build_dispatcher_runtime,
) -> None:
    runtime = runtime_builder()
    try:
        await run_dispatcher_loop(
            runtime.dispatcher, poll_seconds=runtime.poll_seconds
        )
    finally:
        await runtime.engine.dispose()


def main() -> None:
    asyncio.run(_run_main())


if __name__ == "__main__":
    main()
