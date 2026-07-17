"""Wiki 摄取队列与结果写入的 PostgreSQL 仓储。"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, Protocol, runtime_checkable
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import Select, delete, func, or_, select, update
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.exc import IntegrityError

from app.wiki.domain import extract_wiki_links
from app.wiki.ingest.ports import FinalizationPort
from app.wiki.ingest.schemas import FinalizationRequest, ReducedPage, SourceKnowledge
from app.wiki.models import (
    TaskOutbox,
    WikiFinalizationMarker,
    WikiLogEntry,
    WikiPage,
    WikiPendingOp,
)
from app.wiki.scope import WikiScope
from app.wiki.sql_page_store import (
    SqlAlchemyPageStore,
    build_link_backfill_statement,
)


@dataclass(frozen=True, slots=True)
class EnqueueRecord:
    """一次原子入队后可在 session 外安全读取的快照。"""

    id: UUID | None
    tenant_id: int
    knowledge_base_id: UUID
    knowledge_id: str
    op_version: str
    payload: dict[str, object]
    outbox_event_id: UUID | None
    deduplicated: bool

    @property
    def pending_op_id(self) -> UUID | None:
        return self.id


@dataclass(frozen=True, slots=True)
class PendingOpRecord:
    """Worker 使用的 pending-op 脱离 ORM 快照。"""

    id: UUID
    tenant_id: int
    knowledge_base_id: UUID
    knowledge_id: str
    op: str
    op_version: str
    payload: dict[str, object]
    fail_count: int
    enqueued_at: datetime
    claimed_at: datetime | None
    claim_token: UUID | None


@dataclass(frozen=True, slots=True)
class OutboxEventRecord:
    """Dispatcher 使用的 outbox 脱离 ORM 快照。"""

    id: UUID
    tenant_id: int
    knowledge_base_id: UUID
    event_type: str
    dedup_key: str
    payload: dict[str, object]
    available_at: datetime
    claimed_at: datetime | None
    claim_token: UUID | None
    attempts: int
    sent_at: datetime | None


@dataclass(frozen=True, slots=True)
class ExistingPageRecord:
    """Reduce 前读取的页面身份、版本和内容快照。"""

    page_id: UUID
    version: int
    page: ReducedPage


class IngestStoreError(RuntimeError):
    """摄取仓储边界错误。"""


class ClaimLost(IngestStoreError):
    """claim token 已失效或部分目标不再属于调用方。"""


class PageConflict(IngestStoreError):
    """模型计算期间页面身份或版本发生变化。"""


class InvariantError(IngestStoreError):
    """调用参数违反摄取事务不变量。"""


@runtime_checkable
class IngestStore(Protocol):
    async def enqueue(
        self,
        scope: WikiScope,
        knowledge: SourceKnowledge,
        payload: dict[str, object],
        *,
        delay_seconds: int = 30,
    ) -> EnqueueRecord: ...

    async def claim_pending(
        self, scope: WikiScope, limit: int, claim_timeout: timedelta | int
    ) -> list[PendingOpRecord]: ...

    async def release_failed(
        self, scope: WikiScope, ids: Sequence[UUID], claim_token: UUID | None
    ) -> None: ...

    async def find_existing_pages(
        self, scope: WikiScope, slugs: Iterable[str]
    ) -> dict[str, ExistingPageRecord]: ...

    async def apply_results(
        self,
        scope: WikiScope,
        claim_token: UUID | None,
        pages: Sequence[ReducedPage],
        completed_op_ids: Sequence[UUID],
        operation_id: UUID,
        *,
        failed_op_ids: Sequence[UUID] = (),
        expected_pages: Mapping[str, ExistingPageRecord | None] | None = None,
    ) -> bool: ...

    async def pending_count(self, scope: WikiScope) -> int: ...

    async def claim_outbox(
        self, limit: int, claim_timeout: timedelta | int
    ) -> list[OutboxEventRecord]: ...

    async def mark_outbox_sent(
        self, ids: Sequence[UUID], claim_token: UUID | None
    ) -> None: ...

    async def release_outbox(
        self, ids: Sequence[UUID], claim_token: UUID | None
    ) -> None: ...


def _positive_limit(limit: int) -> int:
    if isinstance(limit, bool) or limit <= 0:
        raise ValueError("limit 必须是正整数")
    return limit


def _timeout_delta(value: timedelta | int) -> timedelta:
    if isinstance(value, timedelta):
        if value.total_seconds() <= 0:
            raise ValueError("claim_timeout 必须大于 0")
        return value
    if isinstance(value, bool) or value <= 0:
        raise ValueError("claim_timeout 必须大于 0")
    return timedelta(seconds=value)


def _require_claim_token(claim_token: UUID | None) -> UUID:
    if not isinstance(claim_token, UUID):
        raise ClaimLost("claim token 必须是非空 UUID")
    return claim_token


def build_claim_pending_ops_statement(
    scope: WikiScope, *, limit: int, stale_before: datetime
) -> Select[tuple[WikiPendingOp]]:
    """构造 tenant/KB 范围内可并发安全领取 pending-op 的语句。"""

    return (
        select(WikiPendingOp)
        .where(
            WikiPendingOp.tenant_id == scope.tenant_id,
            WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
            or_(
                WikiPendingOp.claimed_at.is_(None),
                WikiPendingOp.claimed_at < stale_before,
            ),
        )
        .order_by(WikiPendingOp.enqueued_at, WikiPendingOp.id)
        .limit(_positive_limit(limit))
        .with_for_update(skip_locked=True)
    )


def build_claim_outbox_statement(
    *, limit: int, now: datetime, stale_before: datetime
) -> Select[tuple[TaskOutbox]]:
    """构造只领取已到可投递时间且未发送 outbox 的语句。"""

    return (
        select(TaskOutbox)
        .where(
            TaskOutbox.sent_at.is_(None),
            TaskOutbox.available_at <= now,
            or_(TaskOutbox.claimed_at.is_(None), TaskOutbox.claimed_at < stale_before),
        )
        .order_by(TaskOutbox.available_at, TaskOutbox.created_at, TaskOutbox.id)
        .limit(_positive_limit(limit))
        .with_for_update(skip_locked=True)
    )


def build_outbox_dedup_key(
    tenant_id: int,
    knowledge_base_id: UUID,
    event_type: str,
    knowledge_id: str,
    op_version: str,
) -> str:
    """由完整事件身份的 canonical JSON 生成稳定 SHA-256。"""

    canonical = json.dumps(
        {
            "event_type": event_type,
            "knowledge_base_id": str(knowledge_base_id),
            "knowledge_id": knowledge_id,
            "op_version": op_version,
            "tenant_id": tenant_id,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def build_finalization_register_statement(request: FinalizationRequest):
    return (
        postgresql.insert(WikiFinalizationMarker)
        .values(**request.model_dump(mode="python"))
        .on_conflict_do_nothing(constraint="uq_wiki_finalization_markers_attempt")
        .returning(WikiFinalizationMarker.id)
    )


def build_finalization_release_statement(
    request: FinalizationRequest, *, released_at: datetime
):
    return (
        update(WikiFinalizationMarker)
        .where(
            WikiFinalizationMarker.tenant_id == request.tenant_id,
            WikiFinalizationMarker.knowledge_base_id == request.knowledge_base_id,
            WikiFinalizationMarker.knowledge_id == request.knowledge_id,
            WikiFinalizationMarker.attempt == request.attempt,
            WikiFinalizationMarker.subtask_name == request.subtask_name,
            WikiFinalizationMarker.released_at.is_(None),
        )
        .values(released_at=released_at)
        .returning(WikiFinalizationMarker.id)
    )


class SqlFinalizationPort:
    async def register(self, session: AsyncSession, request: FinalizationRequest) -> bool:
        result = await session.execute(build_finalization_register_statement(request))
        return result.scalar_one_or_none() is not None

    async def release(self, session: AsyncSession, request: FinalizationRequest) -> bool:
        result = await session.execute(
            build_finalization_release_statement(
                request, released_at=datetime.now(UTC)
            )
        )
        return result.scalar_one_or_none() is not None


def _pending_record(row: WikiPendingOp) -> PendingOpRecord:
    return PendingOpRecord(
        id=row.id,
        tenant_id=row.tenant_id,
        knowledge_base_id=row.knowledge_base_id,
        knowledge_id=row.knowledge_id,
        op=row.op,
        op_version=row.op_version,
        payload=deepcopy(row.payload),
        fail_count=row.fail_count,
        enqueued_at=row.enqueued_at,
        claimed_at=row.claimed_at,
        claim_token=row.claim_token,
    )


def _outbox_record(row: TaskOutbox) -> OutboxEventRecord:
    return OutboxEventRecord(
        id=row.id,
        tenant_id=row.tenant_id,
        knowledge_base_id=row.knowledge_base_id,
        event_type=row.event_type,
        dedup_key=row.dedup_key,
        payload=deepcopy(row.payload),
        available_at=row.available_at,
        claimed_at=row.claimed_at,
        claim_token=row.claim_token,
        attempts=row.attempts,
        sent_at=row.sent_at,
    )


def _stable_clean(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = value.strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            result.append(cleaned)
    return result


def _snapshot_page(page: ReducedPage) -> ReducedPage:
    if not isinstance(page, ReducedPage):
        raise InvariantError("结果页面必须是 ReducedPage")
    try:
        snapshot = ReducedPage.model_validate(page.model_dump(mode="python"))
    except (ValidationError, TypeError, ValueError) as exc:
        raise InvariantError("结果页面未通过完整校验") from exc
    return snapshot.model_copy(
        update={
            "aliases": _stable_clean(snapshot.aliases),
            "source_refs": _stable_clean(snapshot.source_refs),
            "chunk_refs": _stable_clean(snapshot.chunk_refs),
            "contributor_op_ids": list(dict.fromkeys(snapshot.contributor_op_ids)),
        }
    )


async def _enqueue_follow_up(
    session: AsyncSession,
    scope: WikiScope,
    operation_id: UUID,
) -> None:
    event_type = "wiki.batch.trigger"
    dedup_key = build_outbox_dedup_key(
        scope.tenant_id,
        scope.knowledge_base_id,
        event_type,
        f"operation:{operation_id}",
        "follow-up",
    )
    await session.execute(
        postgresql.insert(TaskOutbox)
        .values(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            event_type=event_type,
            dedup_key=dedup_key,
            payload={
                "tenant_id": scope.tenant_id,
                "knowledge_base_id": str(scope.knowledge_base_id),
            },
            available_at=datetime.now(UTC) + timedelta(seconds=5),
        )
        .on_conflict_do_nothing(
            constraint="uq_task_outbox_scope_event_dedup"
        )
    )


class SqlAlchemyIngestStore:
    """每个公开操作使用独立短 session 的 PostgreSQL 摄取仓储。"""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        finalization: FinalizationPort,
    ) -> None:
        self._session_factory = session_factory
        self._finalization = finalization

    async def enqueue(
        self,
        scope: WikiScope,
        knowledge: SourceKnowledge,
        payload: dict[str, object],
        *,
        delay_seconds: int = 30,
    ) -> EnqueueRecord:
        if delay_seconds < 0:
            raise ValueError("delay_seconds 不能小于 0")
        if (
            knowledge.tenant_id != scope.tenant_id
            or knowledge.knowledge_base_id != scope.knowledge_base_id
            or payload.get("knowledge_id") != knowledge.id
        ):
            raise ValueError("知识条目、payload 与 WikiScope 不一致")
        event_type = "wiki.batch.trigger"
        dedup_key = build_outbox_dedup_key(
            scope.tenant_id,
            scope.knowledge_base_id,
            event_type,
            knowledge.id,
            knowledge.op_version,
        )
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            async with session.begin():
                registered = await self._finalization.register(
                    session, FinalizationRequest.from_knowledge(scope, knowledge)
                )
                inserted_id: UUID | None = None
                if registered:
                    inserted = await session.execute(
                        postgresql.insert(WikiPendingOp)
                        .values(
                            tenant_id=scope.tenant_id,
                            knowledge_base_id=scope.knowledge_base_id,
                            knowledge_id=knowledge.id,
                            op="ingest",
                            op_version=knowledge.op_version,
                            payload=dict(payload),
                        )
                        .on_conflict_do_nothing(
                            constraint="uq_wiki_pending_ops_version"
                        )
                        .returning(WikiPendingOp.id)
                    )
                    inserted_id = inserted.scalar_one_or_none()
                    await session.execute(
                        postgresql.insert(TaskOutbox)
                        .values(
                            tenant_id=scope.tenant_id,
                            knowledge_base_id=scope.knowledge_base_id,
                            event_type=event_type,
                            dedup_key=dedup_key,
                            payload={
                                "tenant_id": scope.tenant_id,
                                "knowledge_base_id": str(scope.knowledge_base_id),
                            },
                            available_at=now + timedelta(seconds=delay_seconds),
                        )
                        .on_conflict_do_nothing(
                            constraint="uq_task_outbox_scope_event_dedup"
                        )
                    )
                pending = (
                    await session.execute(
                        select(WikiPendingOp).where(
                            WikiPendingOp.tenant_id == scope.tenant_id,
                            WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
                            WikiPendingOp.knowledge_id == knowledge.id,
                            WikiPendingOp.op == "ingest",
                            WikiPendingOp.op_version == knowledge.op_version,
                        )
                    )
                ).scalar_one_or_none()
                outbox = (
                    await session.execute(
                        select(TaskOutbox).where(
                            TaskOutbox.tenant_id == scope.tenant_id,
                            TaskOutbox.knowledge_base_id == scope.knowledge_base_id,
                            TaskOutbox.event_type == event_type,
                            TaskOutbox.dedup_key == dedup_key,
                        )
                    )
                ).scalar_one_or_none()
                return EnqueueRecord(
                    id=pending.id if pending is not None else None,
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    knowledge_id=knowledge.id,
                    op_version=knowledge.op_version,
                    payload=(
                        deepcopy(pending.payload)
                        if pending is not None
                        else deepcopy(payload)
                    ),
                    outbox_event_id=outbox.id if outbox is not None else None,
                    deduplicated=not registered or inserted_id is None,
                )

    async def claim_pending(
        self, scope: WikiScope, limit: int, claim_timeout: timedelta | int
    ) -> list[PendingOpRecord]:
        now = datetime.now(UTC)
        stale_before = now - _timeout_delta(claim_timeout)
        token = uuid4()
        async with self._session_factory() as session:
            async with session.begin():
                rows = list(
                    (
                        await session.execute(
                            build_claim_pending_ops_statement(
                                scope, limit=limit, stale_before=stale_before
                            )
                        )
                    ).scalars()
                )
                for row in rows:
                    row.claimed_at = now
                    row.claim_token = token
                await session.flush()
                return [_pending_record(row) for row in rows]

    async def release_failed(
        self, scope: WikiScope, ids: Sequence[UUID], claim_token: UUID | None
    ) -> None:
        failed_ids = list(ids)
        if not failed_ids:
            return
        if len(failed_ids) != len(set(failed_ids)):
            raise InvariantError("failed op ids 不能重复")
        token = _require_claim_token(claim_token)
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(WikiPendingOp)
                    .where(
                        WikiPendingOp.tenant_id == scope.tenant_id,
                        WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
                        WikiPendingOp.id.in_(failed_ids),
                        WikiPendingOp.claim_token == token,
                    )
                    .values(
                        fail_count=WikiPendingOp.fail_count + 1,
                        claimed_at=None,
                        claim_token=None,
                    )
                )
                if result.rowcount != len(failed_ids):
                    raise ClaimLost("pending-op claim token 或 scope 不匹配")
                await _enqueue_follow_up(session, scope, token)

    async def find_existing_pages(
        self, scope: WikiScope, slugs: Iterable[str]
    ) -> dict[str, ExistingPageRecord]:
        unique_slugs = list(dict.fromkeys(slugs))
        if not unique_slugs:
            return {}
        async with self._session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(WikiPage).where(
                            WikiPage.tenant_id == scope.tenant_id,
                            WikiPage.knowledge_base_id == scope.knowledge_base_id,
                            WikiPage.slug.in_(unique_slugs),
                            WikiPage.deleted_at.is_(None),
                        )
                    )
                ).scalars()
            )
            return {
                row.slug: ExistingPageRecord(
                    page_id=row.id,
                    version=row.version,
                    page=ReducedPage(
                        slug=row.slug,
                        title=row.title,
                        page_type=row.page_type,
                        content=row.content,
                        summary=row.summary,
                        aliases=deepcopy(row.aliases),
                        source_refs=deepcopy(row.source_refs),
                        chunk_refs=deepcopy(row.chunk_refs),
                    ),
                )
                for row in rows
            }

    async def apply_results(
        self,
        scope: WikiScope,
        claim_token: UUID | None,
        pages: Sequence[ReducedPage],
        completed_op_ids: Sequence[UUID],
        operation_id: UUID,
        *,
        failed_op_ids: Sequence[UUID] = (),
        expected_pages: Mapping[str, ExistingPageRecord | None] | None = None,
    ) -> bool:
        if completed_op_ids or failed_op_ids:
            _require_claim_token(claim_token)
        async with self._session_factory() as session:
            async with session.begin():
                existing_log = (
                    await session.execute(
                        select(WikiLogEntry).where(
                            WikiLogEntry.operation_id == operation_id
                        )
                    )
                ).scalar_one_or_none()
                if existing_log is not None:
                    if (
                        existing_log.tenant_id != scope.tenant_id
                        or existing_log.knowledge_base_id != scope.knowledge_base_id
                    ):
                        raise InvariantError("operation_id 已被其他 scope 使用")
                    return False

                snapshots = [_snapshot_page(page) for page in pages]
                if len({page.slug for page in snapshots}) != len(snapshots):
                    raise InvariantError("同一结果批次不能包含重复 slug")
                completed_ids = list(completed_op_ids)
                failed_ids = list(failed_op_ids)
                if len(completed_ids) != len(set(completed_ids)):
                    raise InvariantError("completed_op_ids 不能重复")
                if len(failed_ids) != len(set(failed_ids)):
                    raise InvariantError("failed_op_ids 不能重复")
                if set(completed_ids) & set(failed_ids):
                    raise InvariantError("completed 与 failed op ids 不能重叠")
                page_contributors = {
                    contributor
                    for page in snapshots
                    for contributor in page.contributor_op_ids
                }
                if not page_contributors.issubset(set(completed_ids)):
                    raise InvariantError("页面 contributor 必须属于 completed_op_ids")
                all_ids = [*completed_ids, *failed_ids]
                if not all_ids:
                    if snapshots:
                        raise InvariantError("没有完成操作时不能提交结果页面")
                    if expected_pages:
                        raise InvariantError("空结果不能携带 expected_pages")
                    return False
                token = _require_claim_token(claim_token)

                pending_rows = list(
                    (
                        await session.execute(
                            select(WikiPendingOp)
                            .where(
                                WikiPendingOp.tenant_id == scope.tenant_id,
                                WikiPendingOp.knowledge_base_id
                                == scope.knowledge_base_id,
                                WikiPendingOp.id.in_(all_ids),
                                WikiPendingOp.claim_token == token,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                if len(pending_rows) != len(all_ids):
                    raise ClaimLost("pending-op 不属于当前 scope 或 claim")
                pending_by_id = {row.id: row for row in pending_rows}

                slugs = [page.slug for page in snapshots]
                expected = dict(expected_pages or {})
                if set(expected) != set(slugs):
                    raise InvariantError("expected_pages 必须完整覆盖结果 slug")
                existing_rows = list(
                    (
                        await session.execute(
                            select(WikiPage)
                            .where(
                                WikiPage.tenant_id == scope.tenant_id,
                                WikiPage.knowledge_base_id
                                == scope.knowledge_base_id,
                                WikiPage.slug.in_(slugs),
                                WikiPage.deleted_at.is_(None),
                            )
                            .with_for_update()
                        )
                    ).scalars()
                ) if slugs else []
                existing_by_slug = {row.slug: row for row in existing_rows}
                persisted: list[tuple[WikiPage, ReducedPage]] = []
                for reduced in snapshots:
                    row = existing_by_slug.get(reduced.slug)
                    expected_record = expected[reduced.slug]
                    if expected_record is None:
                        if row is not None:
                            raise PageConflict("期望新建的页面已经存在")
                    else:
                        if not isinstance(expected_record, ExistingPageRecord):
                            raise InvariantError("expected_pages 快照类型无效")
                        if (
                            expected_record.page.slug != reduced.slug
                            or row is None
                            or row.id != expected_record.page_id
                            or row.version != expected_record.version
                        ):
                            raise PageConflict("页面身份或版本已在模型计算期间变化")
                    if row is not None and row.page_type != reduced.page_type:
                        raise PageConflict("已有页面类型与结果 slug 类型不一致")
                    values: dict[str, Any] = {
                        "title": reduced.title,
                        "page_type": reduced.page_type,
                        "content": reduced.content,
                        "summary": reduced.summary,
                        "aliases": list(reduced.aliases),
                        "source_refs": list(reduced.source_refs),
                        "chunk_refs": list(reduced.chunk_refs),
                        "status": "published",
                    }
                    if row is None:
                        row = WikiPage(
                            tenant_id=scope.tenant_id,
                            knowledge_base_id=scope.knowledge_base_id,
                            slug=reduced.slug,
                            status="draft",
                            version=1,
                            title=reduced.title,
                            page_type=reduced.page_type,
                            content=reduced.content,
                            summary=reduced.summary,
                            aliases=list(reduced.aliases),
                            source_refs=list(reduced.source_refs),
                            chunk_refs=list(reduced.chunk_refs),
                            wiki_path=f"/{reduced.slug}",
                        )
                        session.add(row)
                    else:
                        changed = any(getattr(row, key) != value for key, value in values.items())
                        if changed:
                            row.version += 1
                        for key, value in values.items():
                            setattr(row, key, value)
                    persisted.append((row, reduced))
                try:
                    await session.flush()
                except IntegrityError as exc:
                    raise PageConflict("并发创建了相同的活跃页面") from exc

                page_store = SqlAlchemyPageStore(session)
                for row, reduced in persisted:
                    await page_store.replace_page_links(
                        scope, row, extract_wiki_links(reduced.content)
                    )
                for row, _ in persisted:
                    await session.execute(
                        build_link_backfill_statement(scope, row.slug, row.id)
                    )
                for row, _ in persisted:
                    row.status = "published"

                session.add(
                    WikiLogEntry(
                        tenant_id=scope.tenant_id,
                        knowledge_base_id=scope.knowledge_base_id,
                        operation_id=operation_id,
                        action="wiki_ingest_batch",
                        message=(
                            f"完成 {len(completed_ids)} 个 Wiki 摄取操作，"
                            f"释放 {len(failed_ids)} 个失败操作"
                        ),
                        pages_affected=[
                            {"slug": page.slug, "title": page.title}
                            for page in snapshots
                        ],
                        actor_id=scope.actor_id,
                    )
                )
                for op_id in completed_ids:
                    pending = pending_by_id[op_id]
                    released = await self._finalization.release(
                        session,
                        FinalizationRequest(
                            tenant_id=scope.tenant_id,
                            knowledge_base_id=scope.knowledge_base_id,
                            knowledge_id=pending.knowledge_id,
                            attempt=pending.op_version,
                        ),
                    )
                    if not released:
                        raise InvariantError("finalization marker 不存在或已被释放")
                deleted = await session.execute(
                    delete(WikiPendingOp).where(
                        WikiPendingOp.tenant_id == scope.tenant_id,
                        WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
                        WikiPendingOp.id.in_(completed_ids),
                        WikiPendingOp.claim_token == token,
                    )
                )
                if deleted.rowcount != len(completed_ids):
                    raise ClaimLost("完成操作的 claim 已失效")
                for op_id in failed_ids:
                    pending = pending_by_id[op_id]
                    pending.fail_count += 1
                    pending.claimed_at = None
                    pending.claim_token = None
                remaining = int(
                    (
                        await session.execute(
                            select(func.count(WikiPendingOp.id)).where(
                                WikiPendingOp.tenant_id == scope.tenant_id,
                                WikiPendingOp.knowledge_base_id
                                == scope.knowledge_base_id,
                            )
                        )
                    ).scalar_one()
                )
                if remaining:
                    await _enqueue_follow_up(session, scope, operation_id)
                await session.flush()
                return True

    async def pending_count(self, scope: WikiScope) -> int:
        async with self._session_factory() as session:
            return int(
                (
                    await session.execute(
                        select(func.count(WikiPendingOp.id)).where(
                            WikiPendingOp.tenant_id == scope.tenant_id,
                            WikiPendingOp.knowledge_base_id
                            == scope.knowledge_base_id,
                        )
                    )
                ).scalar_one()
            )

    async def claim_outbox(
        self, limit: int, claim_timeout: timedelta | int
    ) -> list[OutboxEventRecord]:
        now = datetime.now(UTC)
        stale_before = now - _timeout_delta(claim_timeout)
        token = uuid4()
        async with self._session_factory() as session:
            async with session.begin():
                rows = list(
                    (
                        await session.execute(
                            build_claim_outbox_statement(
                                limit=limit, now=now, stale_before=stale_before
                            )
                        )
                    ).scalars()
                )
                for row in rows:
                    row.claimed_at = now
                    row.claim_token = token
                    row.attempts += 1
                await session.flush()
                return [_outbox_record(row) for row in rows]

    async def mark_outbox_sent(
        self, ids: Sequence[UUID], claim_token: UUID | None
    ) -> None:
        await self._finish_outbox(ids, claim_token, sent=True)

    async def release_outbox(
        self, ids: Sequence[UUID], claim_token: UUID | None
    ) -> None:
        await self._finish_outbox(ids, claim_token, sent=False)

    async def _finish_outbox(
        self, ids: Sequence[UUID], claim_token: UUID | None, *, sent: bool
    ) -> None:
        event_ids = list(ids)
        if not event_ids:
            return
        if len(event_ids) != len(set(event_ids)):
            raise InvariantError("outbox ids 不能重复")
        token = _require_claim_token(claim_token)
        values: dict[str, object] = {"claimed_at": None, "claim_token": None}
        if sent:
            values["sent_at"] = datetime.now(UTC)
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(TaskOutbox)
                    .where(
                        TaskOutbox.id.in_(event_ids),
                        TaskOutbox.claim_token == token,
                        TaskOutbox.sent_at.is_(None),
                    )
                    .values(**values)
                )
                if result.rowcount != len(event_ids):
                    raise ClaimLost("outbox claim token 或 ids 不匹配")
