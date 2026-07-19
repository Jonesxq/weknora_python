"""Wiki 摄取队列与结果写入的 PostgreSQL 仓储。"""

from __future__ import annotations

import hashlib
import json
import math
import re
from numbers import Real
from copy import deepcopy
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any, Mapping, Protocol, runtime_checkable
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import (
    Select,
    Text,
    cast,
    delete,
    func,
    literal_column,
    or_,
    select,
    update,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.exc import IntegrityError

from app.wiki.domain import extract_wiki_links
from app.wiki.ingest.ports import FinalizationPort
from app.wiki.ingest.retract import project_active_refs
from app.wiki.ingest.schemas import (
    BatchApplyOutcome,
    BatchApplyRequest,
    ContributionDelta,
    ContributionState,
    DedupPageCandidate,
    FinalizationRequest,
    FolderCatalogEntry,
    OperationFailure,
    PageExpectation,
    ReducedPage,
    SourceKnowledge,
    StoredContributionRecord,
    TaxonomyContext,
    TopicCandidate,
)
from app.wiki.models import (
    TaskOutbox,
    WikiFinalizationMarker,
    WikiDeadLetter,
    WikiFolder,
    WikiLogEntry,
    WikiLink,
    WikiPage,
    WikiPageContribution,
    WikiPendingOp,
)
from app.wiki.scope import WikiScope
from app.wiki.sql_page_store import (
    SqlAlchemyPageStore,
    build_link_backfill_statement,
)

_MAX_DEDUP_QUERY_NAMES = 64


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
class DeadLetterRecord:
    """达到重试上限后可在 session 外安全读取的不可变快照。"""

    id: UUID
    pending_op_id: UUID
    tenant_id: int
    knowledge_base_id: UUID
    knowledge_id: str
    op: str
    op_version: str
    payload: Mapping[str, object]
    fail_count: int
    last_error_code: str
    last_error_summary: str
    dead_at: datetime


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

    async def enqueue_ingest(
        self,
        scope: WikiScope,
        knowledge: SourceKnowledge,
        payload: dict[str, object],
        *,
        delay_seconds: int = 30,
    ) -> EnqueueRecord: ...

    async def enqueue_retract(
        self,
        scope: WikiScope,
        knowledge_id: str,
        op_version: str,
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

    async def release_claim(
        self, scope: WikiScope, ids: Sequence[UUID], claim_token: UUID
    ) -> None: ...

    async def find_existing_pages(
        self, scope: WikiScope, slugs: Iterable[str]
    ) -> dict[str, ExistingPageRecord]: ...

    async def load_taxonomy_context(
        self, scope: WikiScope, slugs: Iterable[str]
    ) -> TaxonomyContext: ...

    async def list_source_contributions(
        self,
        scope: WikiScope,
        knowledge_id: str,
        *,
        state: ContributionState,
    ) -> list[StoredContributionRecord]: ...

    async def find_dedup_candidates(
        self, scope: WikiScope, candidate: TopicCandidate, limit: int = 20
    ) -> list[DedupPageCandidate]: ...

    async def apply_results(
        self,
        scope: WikiScope,
        request: BatchApplyRequest,
    ) -> bool: ...

    async def apply_results_with_outcome(
        self,
        scope: WikiScope,
        request: BatchApplyRequest,
    ) -> BatchApplyOutcome: ...

    async def list_dead_letters(
        self, scope: WikiScope, *, limit: int = 100
    ) -> list[DeadLetterRecord]: ...

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


def _dedup_limit(limit: int) -> int:
    if type(limit) is not int or not 1 <= limit <= 20:
        raise ValueError("dedup limit 必须在 1 到 20 之间")
    return limit


def _dead_letter_limit(limit: int) -> int:
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("dead-letter limit 必须在 1 到 100 之间")
    return limit


def _dedup_names_expression():
    """与 ix_wiki_pages_dedup_names_trgm 完全相同的索引表达式。"""
    return (
        func.lower(WikiPage.title)
        .op("||")(literal_column("' '"))
        .op("||")(
            func.lower(
                func.coalesce(cast(WikiPage.aliases, Text), literal_column("''"))
            )
        )
    )


def build_dedup_candidate_statement(
    scope: WikiScope,
    candidate: TopicCandidate,
    limit: int = 20,
    *,
    query_name: str | None = None,
) -> Select[tuple[WikiPage, float]]:
    """构造限定范围的 pg_trgm 候选页查询。"""
    checked_limit = _dedup_limit(limit)
    try:
        candidate = TopicCandidate.model_validate(candidate.model_dump(mode="python"))
    except (ValidationError, TypeError, AttributeError, ValueError) as exc:
        raise ValueError("dedup candidate 无效") from exc
    query = query_name if query_name is not None else candidate.name
    if not isinstance(query, str) or not query.strip():
        raise ValueError("dedup query_name 不能为空")
    expression = _dedup_names_expression()
    distance = expression.op("<->")(func.lower(query))
    ranked = (
        select(
            WikiPage.id.label("page_id"),
            distance.label("dedup_distance"),
        )
        .where(
            WikiPage.tenant_id == scope.tenant_id,
            WikiPage.knowledge_base_id == scope.knowledge_base_id,
            WikiPage.deleted_at.is_(None),
            WikiPage.status == literal_column("'published'"),
            WikiPage.page_type == literal_column(f"'{candidate.page_type}'"),
        )
        .order_by(distance)
        .fetch(checked_limit, with_ties=True)
        .subquery("dedup_ranked")
    )
    return (
        select(WikiPage, ranked.c.dedup_distance)
        .join(ranked, ranked.c.page_id == WikiPage.id)
        .order_by(ranked.c.dedup_distance, WikiPage.slug)
        .limit(checked_limit)
    )


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
                WikiPendingOp.claimed_at <= stale_before,
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


def build_operation_outbox_identity(op: str, knowledge_id: str) -> str:
    """编码不受前缀碰撞影响的 Wiki 操作身份。"""

    if type(op) is not str or op not in {"ingest", "retract"}:
        raise ValueError("outbox op 必须是 ingest 或 retract")
    return json.dumps(
        [op, knowledge_id],
        separators=(",", ":"),
        ensure_ascii=True,
    )


def build_operation_lock_key(operation_id: UUID) -> int:
    """为 operation_id 生成跨 scope 稳定的 PostgreSQL advisory lock key。"""

    if not isinstance(operation_id, UUID):
        raise TypeError("operation_id 必须是 UUID")
    digest = hashlib.sha256(operation_id.bytes).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


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
    async def register(
        self, session: AsyncSession, request: FinalizationRequest
    ) -> bool:
        result = await session.execute(build_finalization_register_statement(request))
        return result.scalar_one_or_none() is not None

    async def release(
        self, session: AsyncSession, request: FinalizationRequest
    ) -> bool:
        result = await session.execute(
            build_finalization_release_statement(request, released_at=datetime.now(UTC))
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


def _dedup_candidate_record(row: WikiPage) -> DedupPageCandidate:
    if type(row.aliases) is not list:
        raise InvariantError("dedup 页面 aliases 必须是 JSON 数组")
    try:
        return DedupPageCandidate(
            slug=row.slug,
            title=row.title,
            page_type=row.page_type,
            aliases=tuple(deepcopy(row.aliases)),
        )
    except (ValidationError, TypeError, ValueError) as exc:
        raise InvariantError("dedup 查询返回的页面快照无效") from exc


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


def _safe_dead_letter_payload(
    payload: dict[str, Any], *, knowledge_id: str
) -> dict[str, Any]:
    if type(payload) is not dict or payload.get("knowledge_id") != knowledge_id:
        raise InvariantError("pending payload 与知识来源不一致")
    return {"knowledge_id": knowledge_id}


_SENSITIVE_ERROR_PATTERN = re.compile(
    r"traceback|claim(?:[\s_.-]*token)|chunk[ _-]+text|raw[ _-]+chunk|"
    r"model[ _-]+output|raw[ _-]+output|chunk原文|模型原始输出",
    re.IGNORECASE,
)


def _safe_error_summary(summary: str) -> str:
    match = _SENSITIVE_ERROR_PATTERN.search(summary)
    safe_prefix = summary[: match.start()] if match is not None else summary
    cleaned = " ".join(safe_prefix.replace("\r", " ").replace("\n", " ").split())
    return (cleaned or "敏感错误详情已省略")[:2000]


def _freeze_json_snapshot(value: Any) -> object:
    if isinstance(value, dict):
        return MappingProxyType(
            {str(key): _freeze_json_snapshot(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json_snapshot(item) for item in value)
    return deepcopy(value)


def _dead_letter_record(row: WikiDeadLetter) -> DeadLetterRecord:
    if (
        not isinstance(row.knowledge_id, str)
        or not row.knowledge_id.strip()
        or not isinstance(row.op_version, str)
        or not row.op_version.strip()
    ):
        raise InvariantError("dead-letter 来源身份无效")
    if type(row.payload) is not dict or row.payload != {
        "knowledge_id": row.knowledge_id
    }:
        raise InvariantError("dead-letter payload 不符合安全白名单")
    if row.op not in {"ingest", "retract"}:
        raise InvariantError("dead-letter op 无效")
    if type(row.fail_count) is not int or row.fail_count < 5:
        raise InvariantError("dead-letter fail_count 无效")
    if (
        not isinstance(row.last_error_code, str)
        or not row.last_error_code.strip()
        or len(row.last_error_code) > 128
    ):
        raise InvariantError("dead-letter error code 无效")
    if (
        not isinstance(row.last_error_summary, str)
        or not row.last_error_summary.strip()
        or len(row.last_error_summary) > 2000
        or "\r" in row.last_error_summary
        or "\n" in row.last_error_summary
        or _SENSITIVE_ERROR_PATTERN.search(row.last_error_summary) is not None
    ):
        raise InvariantError("dead-letter error summary 无效")
    payload = _freeze_json_snapshot(row.payload)
    if not isinstance(payload, Mapping):
        raise InvariantError("dead-letter payload 必须是 JSON object")
    return DeadLetterRecord(
        id=row.id,
        pending_op_id=row.pending_op_id,
        tenant_id=row.tenant_id,
        knowledge_base_id=row.knowledge_base_id,
        knowledge_id=row.knowledge_id,
        op=row.op,
        op_version=row.op_version,
        payload=payload,
        fail_count=row.fail_count,
        last_error_code=row.last_error_code,
        last_error_summary=row.last_error_summary,
        dead_at=row.dead_at,
    )


def _validate_batch_request(
    request: BatchApplyRequest, *, _legacy_coverage: bool = False
) -> BatchApplyRequest:
    if not isinstance(request, BatchApplyRequest):
        raise InvariantError("结果批次必须是 BatchApplyRequest")
    try:
        snapshot = BatchApplyRequest.model_validate(request.model_dump(mode="python"))
    except (ValidationError, TypeError, ValueError) as exc:
        raise InvariantError("结果批次未通过完整校验") from exc
    page_slugs = {page.slug for page in snapshot.pages}
    expected_slugs = {expectation.slug for expectation in snapshot.expected_pages}
    if page_slugs != expected_slugs:
        raise InvariantError("expected_pages 必须完整覆盖 pages slug")

    completed_ids = set(snapshot.completed_op_ids)
    claimed_ids = {
        *snapshot.completed_op_ids,
        *snapshot.superseded_op_ids,
        *(failure.pending_op_id for failure in snapshot.failures),
    }
    delta_contributors: dict[str, set[UUID]] = {}
    delta_slugs = {delta.slug for delta in snapshot.contribution_deltas}
    active_sources: set[tuple[str, str]] = set()
    for delta in snapshot.contribution_deltas:
        delta_contributors.setdefault(delta.slug, set()).add(delta.pending_op_id)
        if delta.pending_op_id not in claimed_ids:
            raise InvariantError("贡献 delta pending_op 不属于本批次 claim 集合")
        if delta.pending_op_id not in completed_ids:
            raise InvariantError("贡献 delta 必须属于 completed operation")
        if delta.current is not None:
            active_key = (delta.slug, delta.knowledge_id)
            if active_key in active_sources:
                raise InvariantError("同一 slug 和 source 不能产生两个 current active")
            active_sources.add(active_key)
    for page in snapshot.pages:
        if not page.contributor_op_ids:
            raise InvariantError("每个结果页面必须至少包含一个 contributor")
        for contributor_id in page.contributor_op_ids:
            if contributor_id not in completed_ids:
                raise InvariantError("页面 contributor 必须属于 completed operation")
        if not _legacy_coverage and set(page.contributor_op_ids) != delta_contributors.get(
            page.slug, set()
        ):
            raise InvariantError("页面 contributor 必须完整匹配同 slug contribution delta")
    if not _legacy_coverage and delta_slugs - page_slugs:
        raise InvariantError("completed contribution delta 必须有对应 page")
    return snapshot


def _batch_apply_outcome(
    request: BatchApplyRequest,
    *,
    applied: bool,
    completed_op_ids: Sequence[UUID] | None = None,
    superseded_op_ids: Sequence[UUID] | None = None,
) -> BatchApplyOutcome:
    return BatchApplyOutcome(
        applied=applied,
        completed_op_ids=(
            request.completed_op_ids
            if completed_op_ids is None
            else tuple(completed_op_ids)
        ),
        superseded_op_ids=(
            request.superseded_op_ids
            if superseded_op_ids is None
            else tuple(superseded_op_ids)
        ),
        failed_op_ids=tuple(failure.pending_op_id for failure in request.failures),
    )


_BATCH_OUTCOME_KEYS = {
    "completed_op_ids",
    "superseded_op_ids",
    "failed_op_ids",
}


def _serialize_batch_outcome(outcome: BatchApplyOutcome) -> dict[str, object]:
    return outcome.model_dump(mode="json", exclude={"applied"})


def _restore_batch_outcome(
    value: object, request: BatchApplyRequest
) -> BatchApplyOutcome:
    if not isinstance(value, dict) or set(value) != _BATCH_OUTCOME_KEYS:
        raise InvariantError("操作日志批次终态结构损坏")
    if any(
        not isinstance(value[key], list)
        or any(not isinstance(item, str) for item in value[key])
        for key in _BATCH_OUTCOME_KEYS
    ):
        raise InvariantError("操作日志批次终态结构损坏")
    try:
        outcome = BatchApplyOutcome.model_validate({"applied": False, **value})
    except (ValidationError, TypeError, ValueError) as exc:
        raise InvariantError("操作日志批次终态内容损坏") from exc
    if _serialize_batch_outcome(outcome) != value:
        raise InvariantError("操作日志批次终态 UUID 不规范")
    persisted_ids = {
        *outcome.completed_op_ids,
        *outcome.superseded_op_ids,
        *outcome.failed_op_ids,
    }
    requested_ids = {
        *request.completed_op_ids,
        *request.superseded_op_ids,
        *(failure.pending_op_id for failure in request.failures),
    }
    if persisted_ids != requested_ids:
        raise InvariantError("操作日志批次终态未覆盖请求操作")
    return outcome


def _validate_delta_for_pending(
    pending: WikiPendingOp, delta: ContributionDelta
) -> None:
    if delta.pending_op_id != pending.id:
        raise InvariantError("贡献 delta pending_op 身份不一致")
    if delta.knowledge_id != pending.knowledge_id:
        raise InvariantError("贡献 delta 与 pending 来源不一致")

    records = [
        record for record in (delta.previous, delta.current) if record is not None
    ]
    if any(record.knowledge_id != pending.knowledge_id for record in records):
        raise InvariantError("贡献 delta record 与 pending 来源不一致")
    if any(
        record.tenant_id != pending.tenant_id
        or record.knowledge_base_id != pending.knowledge_base_id
        for record in records
    ):
        raise InvariantError("贡献 delta 越出 pending scope")

    expected_shapes = {
        "add": (False, True, None, "active"),
        "replace": (True, True, "active", "active"),
        "retract_stale": (True, False, "active", None),
        "retract": (True, False, "retract_pending", None),
    }
    require_previous, require_current, previous_state, current_state = expected_shapes[
        delta.action
    ]
    if (
        (delta.previous is not None) != require_previous
        or (delta.current is not None) != require_current
        or previous_state is not None
        and delta.previous is not None
        and delta.previous.state != previous_state
        or current_state is not None
        and delta.current is not None
        and delta.current.state != current_state
    ):
        raise InvariantError("贡献 delta action 记录状态或形态无效")

    allowed_actions = (
        {"add", "replace", "retract_stale"}
        if pending.op == "ingest"
        else {"retract"}
        if pending.op == "retract"
        else set()
    )
    if delta.action not in allowed_actions:
        raise InvariantError("贡献 delta action 与 pending operation 不匹配")
    if (
        pending.op == "ingest"
        and delta.current is not None
        and delta.current.op_version != pending.op_version
    ):
        raise InvariantError("current contribution 版本与 ingest pending 不一致")


def _validate_batch_inputs(
    pages: Sequence[ReducedPage],
    completed_op_ids: Sequence[UUID],
    failed_op_ids: Sequence[UUID],
    expected_pages: Mapping[str, ExistingPageRecord | None] | None,
) -> tuple[
    list[ReducedPage],
    list[UUID],
    list[UUID],
    dict[str, ExistingPageRecord | None],
]:
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

    completed_set = set(completed_ids)
    for page in snapshots:
        if not page.contributor_op_ids:
            raise InvariantError("每个结果页面必须至少包含一个 completed contributor")
        if not set(page.contributor_op_ids).issubset(completed_set):
            raise InvariantError("页面 contributor 必须属于 completed_op_ids")

    expected = dict(expected_pages or {})
    slugs = {page.slug for page in snapshots}
    if set(expected) != slugs:
        raise InvariantError("expected_pages 必须完整覆盖结果 slug")
    if not completed_ids and snapshots:
        raise InvariantError("没有完成操作时不能提交结果页面")
    if not completed_ids and not failed_ids and expected:
        raise InvariantError("空结果不能携带 expected_pages")
    return snapshots, completed_ids, failed_ids, expected


def _legacy_batch_request(
    claim_token: UUID | None,
    pages: Sequence[ReducedPage],
    completed_op_ids: Sequence[UUID],
    operation_id: UUID | None,
    failed_op_ids: Sequence[UUID],
    expected_pages: Mapping[str, ExistingPageRecord | None] | None,
) -> BatchApplyRequest:
    snapshots, completed_ids, failed_ids, expected = _validate_batch_inputs(
        pages, completed_op_ids, failed_op_ids, expected_pages
    )
    empty_batch = not snapshots and not completed_ids and not failed_ids
    token = (
        _require_claim_token(claim_token)
        if completed_ids or failed_ids
        else claim_token
        if isinstance(claim_token, UUID)
        else UUID(int=0)
    )
    checked_operation_id = (
        operation_id
        if isinstance(operation_id, UUID)
        else UUID(int=0)
        if empty_batch
        else operation_id
    )
    return _validate_batch_request(
        BatchApplyRequest(
            claim_token=token,
            pages=tuple(snapshots),
            contribution_deltas=(),
            completed_op_ids=tuple(completed_ids),
            superseded_op_ids=(),
            failures=tuple(
                OperationFailure(
                    pending_op_id=op_id,
                    error_code="LEGACY_WORKER_FAILURE",
                    error_summary="阶段二 Worker 未提供结构化失败详情",
                )
                for op_id in failed_ids
            ),
            expected_pages=tuple(
                PageExpectation(
                    slug=slug,
                    page_id=record.page_id if record is not None else None,
                    version=record.version if record is not None else None,
                )
                for slug, record in expected.items()
            ),
            operation_id=checked_operation_id,
        ),
        _legacy_coverage=True,
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
        .on_conflict_do_nothing(constraint="uq_task_outbox_scope_event_dedup")
    )


def build_claim_recovery_dedup_key(scope: WikiScope, claim_token: UUID) -> str:
    return build_outbox_dedup_key(
        scope.tenant_id,
        scope.knowledge_base_id,
        "wiki.batch.trigger",
        f"claim:{claim_token}",
        "recovery",
    )


async def _enqueue_claim_recovery(
    session: AsyncSession,
    scope: WikiScope,
    claim_token: UUID,
    *,
    available_at: datetime,
) -> None:
    await session.execute(
        postgresql.insert(TaskOutbox)
        .values(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            event_type="wiki.batch.trigger",
            dedup_key=build_claim_recovery_dedup_key(scope, claim_token),
            payload={
                "tenant_id": scope.tenant_id,
                "knowledge_base_id": str(scope.knowledge_base_id),
            },
            available_at=available_at,
        )
        .on_conflict_do_nothing(constraint="uq_task_outbox_scope_event_dedup")
    )


async def _cancel_claim_recovery(
    session: AsyncSession,
    scope: WikiScope,
    claim_token: UUID,
) -> None:
    await session.execute(
        update(TaskOutbox)
        .where(
            TaskOutbox.tenant_id == scope.tenant_id,
            TaskOutbox.knowledge_base_id == scope.knowledge_base_id,
            TaskOutbox.event_type == "wiki.batch.trigger",
            TaskOutbox.dedup_key == build_claim_recovery_dedup_key(scope, claim_token),
            TaskOutbox.sent_at.is_(None),
        )
        .values(
            sent_at=datetime.now(UTC),
            claimed_at=None,
            claim_token=None,
        )
    )


def _validate_enqueue_boundary(
    scope: WikiScope,
    knowledge_id: str,
    op_version: str,
    payload: dict[str, object],
    delay_seconds: int,
) -> tuple[str, str, dict[str, object]]:
    if not isinstance(scope, WikiScope):
        raise TypeError("scope 必须是 WikiScope")
    if not isinstance(knowledge_id, str) or not knowledge_id.strip():
        raise ValueError("knowledge_id 不能为空")
    if not isinstance(op_version, str) or not op_version.strip():
        raise ValueError("op_version 不能为空")
    if type(payload) is not dict:
        raise TypeError("payload 必须是 dict")
    checked_id = knowledge_id.strip()
    checked_version = op_version.strip()
    if payload.get("knowledge_id") != checked_id:
        raise ValueError("payload 与知识条目标识不一致")
    if type(delay_seconds) is not int:
        raise TypeError("delay_seconds 必须是整数")
    if delay_seconds < 0:
        raise ValueError("delay_seconds 不能小于 0")
    return checked_id, checked_version, deepcopy(payload)


def _finalization_request(
    scope: WikiScope, knowledge_id: str, op_version: str, op: str
) -> FinalizationRequest:
    if op not in {"ingest", "retract"}:
        raise ValueError("finalization op 无效")
    return FinalizationRequest(
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        knowledge_id=knowledge_id,
        attempt=op_version,
        subtask_name="wiki" if op == "ingest" else "wiki-retract",
    )


def _enqueue_scope_lock_key(scope: WikiScope) -> int:
    identity = f"wiki-enqueue:{scope.tenant_id}:{scope.knowledge_base_id}".encode(
        "ascii"
    )
    return int.from_bytes(hashlib.sha256(identity).digest()[:8], "big", signed=True)


def _validate_pending_row(
    row: WikiPendingOp, scope: WikiScope, knowledge_id: str
) -> None:
    if (
        not isinstance(row, WikiPendingOp)
        or row.tenant_id != scope.tenant_id
        or row.knowledge_base_id != scope.knowledge_base_id
        or row.knowledge_id != knowledge_id
        or row.op != "ingest"
        or row.claimed_at is not None
        or not isinstance(row.op_version, str)
        or not row.op_version.strip()
    ):
        raise InvariantError("旧 pending-op 的 scope 或身份无效")


def _contribution_record(row: WikiPageContribution) -> StoredContributionRecord:
    if not isinstance(row, WikiPageContribution):
        raise InvariantError("贡献查询返回了无效记录")
    try:
        return StoredContributionRecord(
            id=row.id,
            tenant_id=row.tenant_id,
            knowledge_base_id=row.knowledge_base_id,
            slug=row.slug,
            knowledge_id=row.knowledge_id,
            op_version=row.op_version,
            page_type=row.page_type,
            state=row.state,
            title=row.title,
            content=row.content,
            summary=row.summary,
            aliases=tuple(row.aliases) if type(row.aliases) is list else row.aliases,
            chunk_refs=(
                tuple(row.chunk_refs)
                if type(row.chunk_refs) is list
                else row.chunk_refs
            ),
        )
    except (ValidationError, TypeError, ValueError) as exc:
        raise InvariantError("贡献查询返回了脏数据") from exc


def _taxonomy_requested_slugs(slugs: Iterable[str]) -> tuple[str, ...]:
    requested: set[str] = set()
    for value in slugs:
        if not isinstance(value, str):
            continue
        slug = value.strip().casefold()
        if not slug.startswith(("entity/", "concept/")):
            continue
        normalized = TaxonomyContext(
            classifiable_slugs=(slug,)
        ).classifiable_slugs[0]
        requested.add(normalized)
    return tuple(sorted(requested))


def _taxonomy_context(
    folder_rows: Iterable[Sequence[object]],
    existing_page_slugs: Iterable[object],
    requested_slugs: tuple[str, ...],
) -> TaxonomyContext:
    try:
        folders = tuple(
            FolderCatalogEntry(
                id=folder_id,
                parent_id=parent_id,
                name=name,
                path=path,
                depth=depth,
            )
            for folder_id, parent_id, name, path, depth in folder_rows
        )
        existing: set[str] = set()
        requested = set(requested_slugs)
        for value in existing_page_slugs:
            if not isinstance(value, str):
                raise TypeError("page slug 必须是字符串")
            normalized = TaxonomyContext(
                classifiable_slugs=(value,)
            ).classifiable_slugs[0]
            if normalized != value or normalized not in requested:
                raise ValueError("page slug 不属于 taxonomy context 请求")
            existing.add(normalized)
        return TaxonomyContext(
            folders=folders,
            classifiable_slugs=tuple(
                slug for slug in requested_slugs if slug not in existing
            ),
        )
    except (ValidationError, TypeError, ValueError) as exc:
        raise InvariantError("taxonomy context 查询返回脏数据") from exc


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
        """兼容阶段二调用方的 ingest 入队别名。"""

        return await self.enqueue_ingest(
            scope, knowledge, payload, delay_seconds=delay_seconds
        )

    async def enqueue_ingest(
        self,
        scope: WikiScope,
        knowledge: SourceKnowledge,
        payload: dict[str, object],
        *,
        delay_seconds: int = 30,
    ) -> EnqueueRecord:
        if not isinstance(knowledge, SourceKnowledge):
            raise TypeError("knowledge 必须是 SourceKnowledge")
        knowledge_id, op_version, checked_payload = _validate_enqueue_boundary(
            scope,
            knowledge.id,
            knowledge.op_version,
            payload,
            delay_seconds,
        )
        if (
            knowledge.tenant_id != scope.tenant_id
            or knowledge.knowledge_base_id != scope.knowledge_base_id
        ):
            raise ValueError("知识条目、payload 与 WikiScope 不一致")
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    select(func.pg_advisory_xact_lock(_enqueue_scope_lock_key(scope)))
                )
                await self._cancel_unclaimed_ingest(
                    session,
                    scope,
                    knowledge_id,
                    excluding_op_version=op_version,
                )
                return await self._enqueue_operation(
                    session,
                    scope,
                    knowledge_id,
                    "ingest",
                    op_version,
                    checked_payload,
                    delay_seconds,
                )

    async def enqueue_retract(
        self,
        scope: WikiScope,
        knowledge_id: str,
        op_version: str,
        payload: dict[str, object],
        *,
        delay_seconds: int = 30,
    ) -> EnqueueRecord:
        knowledge_id, op_version, checked_payload = _validate_enqueue_boundary(
            scope, knowledge_id, op_version, payload, delay_seconds
        )
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    select(func.pg_advisory_xact_lock(_enqueue_scope_lock_key(scope)))
                )
                await self._cancel_unclaimed_ingest(session, scope, knowledge_id)
                contributions = list(
                    (
                        await session.execute(
                            select(WikiPageContribution)
                            .where(
                                WikiPageContribution.tenant_id == scope.tenant_id,
                                WikiPageContribution.knowledge_base_id
                                == scope.knowledge_base_id,
                                WikiPageContribution.knowledge_id == knowledge_id,
                                WikiPageContribution.state == "active",
                            )
                            .order_by(
                                WikiPageContribution.slug,
                                WikiPageContribution.id,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                by_slug: dict[str, list[StoredContributionRecord]] = {}
                now = datetime.now(UTC)
                for row in contributions:
                    record = _contribution_record(row)
                    if (
                        record.tenant_id != scope.tenant_id
                        or record.knowledge_base_id != scope.knowledge_base_id
                        or record.knowledge_id != knowledge_id
                        or record.state != "active"
                    ):
                        raise InvariantError("活动贡献的 scope 或来源身份无效")
                    by_slug.setdefault(record.slug, []).append(record)
                    row.state = "retract_pending"
                    row.updated_at = now

                await session.flush()
                for slug in sorted(by_slug):
                    page = (
                        await session.execute(
                            select(WikiPage)
                            .where(
                                WikiPage.tenant_id == scope.tenant_id,
                                WikiPage.knowledge_base_id == scope.knowledge_base_id,
                                WikiPage.slug == slug,
                                WikiPage.deleted_at.is_(None),
                            )
                            .with_for_update()
                        )
                    ).scalar_one_or_none()
                    remaining_rows = list(
                        (
                            await session.execute(
                                select(WikiPageContribution)
                                .where(
                                    WikiPageContribution.tenant_id == scope.tenant_id,
                                    WikiPageContribution.knowledge_base_id
                                    == scope.knowledge_base_id,
                                    WikiPageContribution.slug == slug,
                                    WikiPageContribution.state == "active",
                                )
                                .order_by(
                                    WikiPageContribution.knowledge_id,
                                    WikiPageContribution.op_version,
                                    WikiPageContribution.id,
                                )
                                .with_for_update()
                            )
                        ).scalars()
                    )
                    remaining = [_contribution_record(row) for row in remaining_rows]
                    self._validate_retract_page(
                        scope, slug, page, [*by_slug[slug], *remaining]
                    )
                    source_refs, chunk_refs = project_active_refs(remaining)
                    if remaining:
                        if (
                            page.source_refs != source_refs
                            or page.chunk_refs != chunk_refs
                        ):
                            page.source_refs = source_refs
                            page.chunk_refs = chunk_refs
                            page.version += 1
                            page.updated_at = now
                    else:
                        page.source_refs = []
                        page.chunk_refs = []
                        page.deleted_at = now
                        page.updated_at = now
                        page.version += 1
                        await session.execute(
                            delete(WikiLink).where(
                                WikiLink.tenant_id == scope.tenant_id,
                                WikiLink.knowledge_base_id == scope.knowledge_base_id,
                                WikiLink.source_page_id == page.id,
                            )
                        )
                        await session.execute(
                            update(WikiLink)
                            .where(
                                WikiLink.tenant_id == scope.tenant_id,
                                WikiLink.knowledge_base_id == scope.knowledge_base_id,
                                WikiLink.target_page_id == page.id,
                            )
                            .values(target_page_id=None)
                        )
                await session.flush()
                return await self._enqueue_operation(
                    session,
                    scope,
                    knowledge_id,
                    "retract",
                    op_version,
                    checked_payload,
                    delay_seconds,
                )

    async def _cancel_unclaimed_ingest(
        self,
        session: AsyncSession,
        scope: WikiScope,
        knowledge_id: str,
        *,
        excluding_op_version: str | None = None,
    ) -> None:
        statement = select(WikiPendingOp).where(
            WikiPendingOp.tenant_id == scope.tenant_id,
            WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
            WikiPendingOp.knowledge_id == knowledge_id,
            WikiPendingOp.op == "ingest",
            WikiPendingOp.claimed_at.is_(None),
        )
        if excluding_op_version is not None:
            statement = statement.where(
                WikiPendingOp.op_version != excluding_op_version
            )
        rows = list(
            (
                await session.execute(
                    statement.order_by(
                        WikiPendingOp.op_version, WikiPendingOp.id
                    ).with_for_update()
                )
            ).scalars()
        )
        for row in rows:
            _validate_pending_row(row, scope, knowledge_id)
            released = await self._finalization.release(
                session,
                _finalization_request(scope, knowledge_id, row.op_version, "ingest"),
            )
            if not released:
                raise InvariantError("finalization marker 不存在或已被释放")
        if rows:
            result = await session.execute(
                delete(WikiPendingOp).where(
                    WikiPendingOp.tenant_id == scope.tenant_id,
                    WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
                    WikiPendingOp.knowledge_id == knowledge_id,
                    WikiPendingOp.op == "ingest",
                    WikiPendingOp.claimed_at.is_(None),
                    WikiPendingOp.id.in_([row.id for row in rows]),
                )
            )
            rowcount = getattr(result, "rowcount", None)
            if rowcount is not None and rowcount != len(rows):
                raise InvariantError("取消旧 pending-op 时行数不一致")

    @staticmethod
    def _validate_retract_page(
        scope: WikiScope,
        slug: str,
        page: WikiPage | None,
        active_before: Sequence[StoredContributionRecord],
    ) -> None:
        if page is None:
            raise InvariantError("活动贡献缺少对应的活动页面")
        page_types = {record.page_type for record in active_before}
        if (
            page.tenant_id != scope.tenant_id
            or page.knowledge_base_id != scope.knowledge_base_id
            or page.slug != slug
            or page.deleted_at is not None
            or page_types != {page.page_type}
            or type(page.source_refs) is not list
            or type(page.chunk_refs) is not list
        ):
            raise InvariantError("活动页面的 scope、类型或 refs 无效")
        expected_sources, expected_chunks = project_active_refs(active_before)
        if page.source_refs != expected_sources or page.chunk_refs != expected_chunks:
            raise InvariantError("活动页面 refs 与贡献投影不一致")

    async def _enqueue_operation(
        self,
        session: AsyncSession,
        scope: WikiScope,
        knowledge_id: str,
        op: str,
        op_version: str,
        payload: dict[str, object],
        delay_seconds: int,
    ) -> EnqueueRecord:
        event_type = "wiki.batch.trigger"
        dedup_identity = build_operation_outbox_identity(op, knowledge_id)
        dedup_key = build_outbox_dedup_key(
            scope.tenant_id,
            scope.knowledge_base_id,
            event_type,
            dedup_identity,
            op_version,
        )
        registered = await self._finalization.register(
            session, _finalization_request(scope, knowledge_id, op_version, op)
        )
        inserted_id: UUID | None = None
        if registered:
            inserted = await session.execute(
                postgresql.insert(WikiPendingOp)
                .values(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    knowledge_id=knowledge_id,
                    op=op,
                    op_version=op_version,
                    payload=deepcopy(payload),
                )
                .on_conflict_do_nothing(constraint="uq_wiki_pending_ops_version")
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
                    available_at=datetime.now(UTC) + timedelta(seconds=delay_seconds),
                )
                .on_conflict_do_nothing(constraint="uq_task_outbox_scope_event_dedup")
            )
        pending = (
            await session.execute(
                select(WikiPendingOp).where(
                    WikiPendingOp.tenant_id == scope.tenant_id,
                    WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
                    WikiPendingOp.knowledge_id == knowledge_id,
                    WikiPendingOp.op == op,
                    WikiPendingOp.op_version == op_version,
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
        if pending is not None and (
            pending.tenant_id != scope.tenant_id
            or pending.knowledge_base_id != scope.knowledge_base_id
            or pending.knowledge_id != knowledge_id
            or pending.op != op
            or pending.op_version != op_version
            or type(pending.payload) is not dict
        ):
            raise InvariantError("pending-op 快照的 scope 或身份无效")
        return EnqueueRecord(
            id=pending.id if pending is not None else None,
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            knowledge_id=knowledge_id,
            op_version=op_version,
            payload=(
                deepcopy(pending.payload) if pending is not None else deepcopy(payload)
            ),
            outbox_event_id=outbox.id if outbox is not None else None,
            deduplicated=not registered or inserted_id is None,
        )

    async def claim_pending(
        self, scope: WikiScope, limit: int, claim_timeout: timedelta | int
    ) -> list[PendingOpRecord]:
        now = datetime.now(UTC)
        timeout = _timeout_delta(claim_timeout)
        stale_before = now - timeout
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
                stale_tokens = {
                    row.claim_token for row in rows if row.claim_token is not None
                }
                for stale_token in stale_tokens:
                    await _cancel_claim_recovery(session, scope, stale_token)
                for row in rows:
                    row.claimed_at = now
                    row.claim_token = token
                if rows:
                    await _enqueue_claim_recovery(
                        session,
                        scope,
                        token,
                        available_at=now + timeout,
                    )
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
                remaining_claimed = int(
                    (
                        await session.execute(
                            select(func.count(WikiPendingOp.id)).where(
                                WikiPendingOp.tenant_id == scope.tenant_id,
                                WikiPendingOp.knowledge_base_id
                                == scope.knowledge_base_id,
                                WikiPendingOp.claim_token == token,
                            )
                        )
                    ).scalar_one()
                )
                if not remaining_claimed:
                    await _cancel_claim_recovery(session, scope, token)
                await _enqueue_follow_up(session, scope, token)

    async def release_claim(
        self, scope: WikiScope, ids: Sequence[UUID], claim_token: UUID
    ) -> None:
        pending_ids = list(ids)
        if not pending_ids:
            return
        if len(pending_ids) != len(set(pending_ids)):
            raise InvariantError("release claim ids 不能重复")
        token = _require_claim_token(claim_token)
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(WikiPendingOp)
                    .where(
                        WikiPendingOp.tenant_id == scope.tenant_id,
                        WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
                        WikiPendingOp.id.in_(pending_ids),
                        WikiPendingOp.claim_token == token,
                    )
                    .values(claimed_at=None, claim_token=None)
                )
                if result.rowcount != len(pending_ids):
                    raise ClaimLost("pending-op claim token、scope 或 ids 不匹配")
                await session.flush()

    async def list_dead_letters(
        self, scope: WikiScope, *, limit: int = 100
    ) -> list[DeadLetterRecord]:
        checked_limit = _dead_letter_limit(limit)
        async with self._session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(WikiDeadLetter)
                        .where(
                            WikiDeadLetter.tenant_id == scope.tenant_id,
                            WikiDeadLetter.knowledge_base_id == scope.knowledge_base_id,
                        )
                        .order_by(WikiDeadLetter.dead_at, WikiDeadLetter.id)
                        .limit(checked_limit)
                    )
                ).scalars()
            )
        records = [_dead_letter_record(row) for row in rows]
        if any(
            record.tenant_id != scope.tenant_id
            or record.knowledge_base_id != scope.knowledge_base_id
            for record in records
        ):
            raise InvariantError("dead-letter 查询返回了越界记录")
        return records

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

    async def load_taxonomy_context(
        self, scope: WikiScope, slugs: Iterable[str]
    ) -> TaxonomyContext:
        requested_slugs = _taxonomy_requested_slugs(slugs)
        async with self._session_factory() as session:
            folder_rows = list(
                (
                    await session.execute(
                        select(
                            WikiFolder.id,
                            WikiFolder.parent_id,
                            WikiFolder.name,
                            WikiFolder.path,
                            WikiFolder.depth,
                        )
                        .where(
                            WikiFolder.tenant_id == scope.tenant_id,
                            WikiFolder.knowledge_base_id
                            == scope.knowledge_base_id,
                            WikiFolder.deleted_at.is_(None),
                        )
                        .order_by(WikiFolder.depth, WikiFolder.path, WikiFolder.id)
                    )
                ).all()
            )
            existing_page_slugs: list[object] = []
            if requested_slugs:
                existing_page_slugs = list(
                    (
                        await session.execute(
                            select(WikiPage.slug)
                            .where(
                                WikiPage.tenant_id == scope.tenant_id,
                                WikiPage.knowledge_base_id
                                == scope.knowledge_base_id,
                                WikiPage.slug.in_(requested_slugs),
                            )
                            .order_by(WikiPage.slug)
                        )
                    ).scalars()
                )
        return _taxonomy_context(
            folder_rows, existing_page_slugs, requested_slugs
        )

    async def list_source_contributions(
        self,
        scope: WikiScope,
        knowledge_id: str,
        *,
        state: ContributionState,
    ) -> list[StoredContributionRecord]:
        if not isinstance(scope, WikiScope):
            raise TypeError("scope 必须是 WikiScope")
        if not isinstance(knowledge_id, str) or not knowledge_id.strip():
            raise ValueError("knowledge_id 不能为空")
        if state not in {"active", "retract_pending"}:
            raise ValueError("contribution state 无效")
        checked_knowledge_id = knowledge_id.strip()
        async with self._session_factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(WikiPageContribution)
                        .where(
                            WikiPageContribution.tenant_id == scope.tenant_id,
                            WikiPageContribution.knowledge_base_id
                            == scope.knowledge_base_id,
                            WikiPageContribution.knowledge_id == checked_knowledge_id,
                            WikiPageContribution.state == state,
                        )
                        .order_by(
                            WikiPageContribution.slug,
                            WikiPageContribution.op_version,
                            WikiPageContribution.id,
                        )
                    )
                ).scalars()
            )
            records = [_contribution_record(row) for row in rows]
        if any(
            record.tenant_id != scope.tenant_id
            or record.knowledge_base_id != scope.knowledge_base_id
            or record.knowledge_id != checked_knowledge_id
            or record.state != state
            for record in records
        ):
            raise InvariantError("贡献查询返回了越界记录")
        return records

    async def find_dedup_candidates(
        self, scope: WikiScope, candidate: TopicCandidate, limit: int = 20
    ) -> list[DedupPageCandidate]:
        names: list[str] = []
        for name in (candidate.name, *candidate.aliases):
            cleaned = " ".join(name.split())
            if cleaned and cleaned not in names:
                names.append(cleaned)
        if len(names) > _MAX_DEDUP_QUERY_NAMES:
            raise ValueError("dedup 查询名称不能超过 64 个")
        async with self._session_factory() as session:
            rows: list[tuple[WikiPage, object]] = []
            for name in names:
                result = await session.execute(
                    build_dedup_candidate_statement(
                        scope, candidate, limit, query_name=name
                    )
                )
                found = list(result.all())
                if len(found) > limit:
                    raise InvariantError("dedup 查询返回超过 limit 的候选")
                rows.extend(found)
        merged: dict[str, tuple[WikiPage, float]] = {}
        for result_row in rows:
            if (
                isinstance(result_row, (str, bytes))
                or not isinstance(result_row, Sequence)
                or len(result_row) != 2
            ):
                raise InvariantError("dedup 查询返回行形状无效")
            row, distance = result_row
            if not isinstance(row, WikiPage):
                raise InvariantError("dedup 查询返回非 WikiPage")
            if (
                row.tenant_id != scope.tenant_id
                or row.knowledge_base_id != scope.knowledge_base_id
                or row.deleted_at is not None
                or row.status != "published"
                or row.page_type != candidate.page_type
            ):
                raise InvariantError("dedup 查询返回越界页面")
            if (
                not isinstance(distance, Real)
                or isinstance(distance, bool)
                or not math.isfinite(float(distance))
                or float(distance) < 0
            ):
                raise InvariantError("dedup 查询返回无效距离")
            old = merged.get(row.slug)
            if old is not None and (
                old[0].id != row.id
                or old[0].title != row.title
                or old[0].page_type != row.page_type
                or old[0].aliases != row.aliases
            ):
                raise InvariantError("dedup 查询返回冲突页面")
            if old is None or float(distance) < old[1]:
                merged[row.slug] = (row, float(distance))
        return [
            _dedup_candidate_record(row)
            for _, (row, _) in sorted(
                merged.items(), key=lambda item: (item[1][1], item[0])
            )[:limit]
        ]

    async def apply_results(
        self,
        scope: WikiScope,
        request: BatchApplyRequest | UUID | None,
        pages: Sequence[ReducedPage] | None = None,
        completed_op_ids: Sequence[UUID] | None = None,
        operation_id: UUID | None = None,
        *,
        failed_op_ids: Sequence[UUID] = (),
        expected_pages: Mapping[str, ExistingPageRecord | None] | None = None,
    ) -> bool:
        if isinstance(request, BatchApplyRequest):
            if (
                pages is not None
                or completed_op_ids is not None
                or operation_id is not None
                or failed_op_ids
                or expected_pages is not None
            ):
                raise TypeError("现代 apply_results 不能混用 legacy 参数")
            checked = _validate_batch_request(request)
        else:
            if pages is None or completed_op_ids is None:
                raise TypeError("legacy apply_results 参数不完整")
            if operation_id is None and (completed_op_ids or failed_op_ids or pages):
                raise TypeError("legacy apply_results 参数不完整")
            checked = _legacy_batch_request(
                request,
                pages,
                completed_op_ids,
                operation_id,
                failed_op_ids,
                expected_pages,
            )
        return (await self._apply_checked_results(scope, checked)).applied

    async def apply_results_with_outcome(
        self,
        scope: WikiScope,
        request: BatchApplyRequest,
    ) -> BatchApplyOutcome:
        return await self._apply_checked_results(
            scope, _validate_batch_request(request)
        )

    async def _apply_checked_results(
        self,
        scope: WikiScope,
        checked: BatchApplyRequest,
    ) -> BatchApplyOutcome:
        if not (
            checked.pages
            or checked.contribution_deltas
            or checked.completed_op_ids
            or checked.superseded_op_ids
            or checked.failures
            or checked.expected_pages
        ):
            return _batch_apply_outcome(checked, applied=False)
        return await self._apply_batch_results(scope, checked)

    async def _apply_batch_results(
        self, scope: WikiScope, request: BatchApplyRequest
    ) -> BatchApplyOutcome:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    select(
                        func.pg_advisory_xact_lock(
                            build_operation_lock_key(request.operation_id)
                        )
                    )
                )
                existing_log = (
                    await session.execute(
                        select(WikiLogEntry).where(
                            WikiLogEntry.operation_id == request.operation_id
                        )
                    )
                ).scalar_one_or_none()
                if existing_log is not None:
                    if (
                        existing_log.tenant_id != scope.tenant_id
                        or existing_log.knowledge_base_id != scope.knowledge_base_id
                    ):
                        raise InvariantError("operation_id 已被其他 scope 使用")
                    if existing_log.result_outcome is not None:
                        return _restore_batch_outcome(
                            existing_log.result_outcome, request
                        )
                    return _batch_apply_outcome(request, applied=False)
                requested_ids = {
                    *request.completed_op_ids,
                    *request.superseded_op_ids,
                    *(failure.pending_op_id for failure in request.failures),
                }
                pending_rows = list(
                    (
                        await session.execute(
                            select(WikiPendingOp)
                            .where(
                                WikiPendingOp.tenant_id == scope.tenant_id,
                                WikiPendingOp.knowledge_base_id
                                == scope.knowledge_base_id,
                                WikiPendingOp.claim_token == request.claim_token,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                if {row.id for row in pending_rows} != requested_ids:
                    raise ClaimLost("批次必须完整覆盖当前 scope 和 claim 的 pending-op")
                pending_by_id = {row.id: row for row in pending_rows}
                for delta in request.contribution_deltas:
                    _validate_delta_for_pending(
                        pending_by_id[delta.pending_op_id], delta
                    )

                completed_ingest = [
                    pending_by_id[op_id]
                    for op_id in request.completed_op_ids
                    if pending_by_id[op_id].op == "ingest"
                ]
                later_retracts = (
                    list(
                        (
                            await session.execute(
                                select(WikiPendingOp)
                                .where(
                                    WikiPendingOp.tenant_id == scope.tenant_id,
                                    WikiPendingOp.knowledge_base_id
                                    == scope.knowledge_base_id,
                                    WikiPendingOp.op == "retract",
                                    WikiPendingOp.knowledge_id.in_(
                                        [row.knowledge_id for row in completed_ingest]
                                    ),
                                )
                                .with_for_update()
                            )
                        ).scalars()
                    )
                    if completed_ingest
                    else []
                )
                auto_superseded = {
                    ingest.id
                    for ingest in completed_ingest
                    if any(
                        retract.knowledge_id == ingest.knowledge_id
                        and retract.enqueued_at > ingest.enqueued_at
                        for retract in later_retracts
                    )
                }
                superseded_ids = list(
                    dict.fromkeys(
                        [
                            *request.superseded_op_ids,
                            *(
                                op_id
                                for op_id in request.completed_op_ids
                                if op_id in auto_superseded
                            ),
                        ]
                    )
                )
                superseded_id_set = set(superseded_ids)
                completed_ids = [
                    op_id
                    for op_id in request.completed_op_ids
                    if op_id not in superseded_id_set
                ]
                completed_id_set = set(completed_ids)
                terminal_ids = [*completed_ids, *superseded_ids]

                auto_superseded_slugs = {
                    delta.slug
                    for delta in request.contribution_deltas
                    if delta.pending_op_id in auto_superseded
                }
                effective_completed_slugs = {
                    delta.slug
                    for delta in request.contribution_deltas
                    if delta.pending_op_id in completed_ids
                }
                for page in request.pages:
                    contributor_ids = set(page.contributor_op_ids)
                    if (
                        page.slug in auto_superseded_slugs & effective_completed_slugs
                        or contributor_ids & auto_superseded
                        and contributor_ids & completed_id_set
                    ):
                        raise PageConflict(
                            "auto-superseded 来源与有效 completed 来源共享页面 slug"
                        )

                superseded_slugs = {
                    delta.slug
                    for delta in request.contribution_deltas
                    if delta.pending_op_id in superseded_ids
                }
                pages = [
                    page
                    for page in request.pages
                    if page.slug not in superseded_slugs
                    and set(page.contributor_op_ids).issubset(completed_id_set)
                ]
                expected_by_slug = {
                    expectation.slug: expectation
                    for expectation in request.expected_pages
                }
                slugs = [page.slug for page in pages]
                page_rows = (
                    list(
                        (
                            await session.execute(
                                select(WikiPage)
                                .where(
                                    WikiPage.tenant_id == scope.tenant_id,
                                    WikiPage.knowledge_base_id
                                    == scope.knowledge_base_id,
                                    WikiPage.slug.in_(slugs),
                                )
                                .order_by(
                                    WikiPage.slug,
                                    WikiPage.deleted_at.desc().nulls_last(),
                                    WikiPage.id,
                                )
                                .with_for_update()
                            )
                        ).scalars()
                    )
                    if slugs
                    else []
                )
                rows_by_slug: dict[str, list[WikiPage]] = {}
                for row in page_rows:
                    if (
                        row.tenant_id != scope.tenant_id
                        or row.knowledge_base_id != scope.knowledge_base_id
                    ):
                        raise InvariantError("页面锁查询返回了越界记录")
                    rows_by_slug.setdefault(row.slug, []).append(row)

                selected_pages: list[tuple[WikiPage | None, object]] = []
                for reduced in pages:
                    candidates = rows_by_slug.get(reduced.slug, [])
                    active = [row for row in candidates if row.deleted_at is None]
                    if len(active) > 1:
                        raise InvariantError("同一 scope 和 slug 存在多个活跃页面")
                    expectation = expected_by_slug[reduced.slug]
                    if expectation.page_id is None:
                        if active:
                            raise PageConflict("期望不存在的页面已经出现")
                        row = (
                            candidates[0]
                            if candidates and not reduced.deleted
                            else None
                        )
                    else:
                        row = active[0] if active else None
                        if (
                            row is None
                            or row.id != expectation.page_id
                            or row.version != expectation.version
                        ):
                            raise PageConflict("页面身份或版本已在模型计算期间变化")
                    if row is not None and row.page_type != reduced.page_type:
                        raise PageConflict("已有页面类型与结果 slug 类型不一致")
                    selected_pages.append((row, reduced))

                active_deltas = [
                    delta
                    for delta in request.contribution_deltas
                    if delta.pending_op_id in completed_ids
                ]
                previous_deletions: dict[
                    tuple[int, UUID, str, str, str],
                    tuple[StoredContributionRecord, str],
                ] = {}
                for delta in active_deltas:
                    previous = delta.previous
                    if previous is None:
                        continue
                    identity = (
                        previous.tenant_id,
                        previous.knowledge_base_id,
                        previous.slug,
                        previous.knowledge_id,
                        previous.op_version,
                    )
                    existing = previous_deletions.get(identity)
                    if existing is not None and existing != (previous, delta.action):
                        raise InvariantError(
                            "同一 previous contribution 存在冲突的删除差量"
                        )
                    previous_deletions.setdefault(identity, (previous, delta.action))

                deleted_contributions = False
                for previous, _action in previous_deletions.values():
                    statement = delete(WikiPageContribution).where(
                        WikiPageContribution.tenant_id == scope.tenant_id,
                        WikiPageContribution.knowledge_base_id
                        == scope.knowledge_base_id,
                        WikiPageContribution.slug == previous.slug,
                        WikiPageContribution.knowledge_id == previous.knowledge_id,
                        WikiPageContribution.op_version == previous.op_version,
                        WikiPageContribution.state == previous.state,
                    )
                    if previous.id is not None:
                        statement = statement.where(
                            WikiPageContribution.id == previous.id
                        )
                    result = await session.execute(statement)
                    if result.rowcount != 1:
                        raise InvariantError("previous contribution 已变化或不存在")
                    deleted_contributions = True
                if deleted_contributions:
                    await session.flush()

                for delta in active_deltas:
                    current = delta.current
                    if current is None:
                        continue
                    session.add(
                        WikiPageContribution(
                            tenant_id=scope.tenant_id,
                            knowledge_base_id=scope.knowledge_base_id,
                            slug=current.slug,
                            knowledge_id=current.knowledge_id,
                            op_version=current.op_version,
                            page_type=current.page_type,
                            state="active",
                            title=current.title,
                            content=current.content,
                            summary=current.summary,
                            aliases=list(current.aliases),
                            chunk_refs=list(current.chunk_refs),
                        )
                    )
                if any(delta.current is not None for delta in active_deltas):
                    try:
                        await session.flush()
                    except IntegrityError as exc:
                        raise InvariantError(
                            "current active contribution 唯一性冲突"
                        ) from exc

                now = datetime.now(UTC)
                persisted: list[tuple[WikiPage, object]] = []
                for row, reduced in selected_pages:
                    if row is None:
                        if reduced.deleted:
                            continue
                        row = WikiPage(
                            tenant_id=scope.tenant_id,
                            knowledge_base_id=scope.knowledge_base_id,
                            slug=reduced.slug,
                            title=reduced.title,
                            page_type=reduced.page_type,
                            status="published",
                            content=reduced.content,
                            summary=reduced.summary,
                            aliases=list(reduced.aliases),
                            source_refs=list(reduced.source_refs),
                            chunk_refs=list(reduced.chunk_refs),
                            wiki_path=f"/{reduced.slug}",
                            version=1,
                        )
                        session.add(row)
                    else:
                        target_deleted_at = (
                            row.deleted_at or now if reduced.deleted else None
                        )
                        values = {
                            "title": reduced.title,
                            "content": reduced.content,
                            "summary": reduced.summary,
                            "aliases": list(reduced.aliases),
                            "source_refs": list(reduced.source_refs),
                            "chunk_refs": list(reduced.chunk_refs),
                            "status": "published",
                            "deleted_at": target_deleted_at,
                        }
                        if any(
                            getattr(row, key) != value for key, value in values.items()
                        ):
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
                    targets = (
                        [] if reduced.deleted else extract_wiki_links(reduced.content)
                    )
                    await page_store.replace_page_links(scope, row, targets)
                    if reduced.deleted:
                        await session.execute(
                            update(WikiLink)
                            .where(
                                WikiLink.tenant_id == scope.tenant_id,
                                WikiLink.knowledge_base_id == scope.knowledge_base_id,
                                WikiLink.target_page_id == row.id,
                            )
                            .values(target_page_id=None)
                        )
                    else:
                        await session.execute(
                            build_link_backfill_statement(scope, row.slug, row.id)
                        )

                actual_outcome = _batch_apply_outcome(
                    request,
                    applied=True,
                    completed_op_ids=completed_ids,
                    superseded_op_ids=superseded_ids,
                )
                op_kinds = {row.op for row in pending_rows}
                action = (
                    "wiki_ingest_batch"
                    if op_kinds == {"ingest"}
                    else "wiki_retract_batch"
                    if op_kinds == {"retract"}
                    else "wiki_incremental_batch"
                )
                session.add(
                    WikiLogEntry(
                        tenant_id=scope.tenant_id,
                        knowledge_base_id=scope.knowledge_base_id,
                        operation_id=request.operation_id,
                        action=action,
                        message=(
                            f"完成 {len(completed_ids)} 个 Wiki 操作，"
                            f"跳过 {len(superseded_ids)} 个过期操作"
                        ),
                        pages_affected=[
                            {"slug": page.slug, "title": page.title} for page in pages
                        ],
                        result_outcome=_serialize_batch_outcome(actual_outcome),
                        actor_id=scope.actor_id,
                    )
                )

                for op_id in terminal_ids:
                    pending = pending_by_id[op_id]
                    released = await self._finalization.release(
                        session,
                        _finalization_request(
                            scope,
                            pending.knowledge_id,
                            pending.op_version,
                            pending.op,
                        ),
                    )
                    if not released:
                        raise InvariantError("finalization marker 不存在或已被释放")
                if terminal_ids:
                    deleted = await session.execute(
                        delete(WikiPendingOp).where(
                            WikiPendingOp.tenant_id == scope.tenant_id,
                            WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
                            WikiPendingOp.id.in_(terminal_ids),
                            WikiPendingOp.claim_token == request.claim_token,
                        )
                    )
                    if deleted.rowcount != len(terminal_ids):
                        raise ClaimLost("完成操作的 claim 已失效")

                for failure in request.failures:
                    pending = pending_by_id[failure.pending_op_id]
                    next_fail_count = pending.fail_count + 1
                    if next_fail_count >= 5:
                        await session.execute(
                            postgresql.insert(WikiDeadLetter)
                            .values(
                                id=uuid4(),
                                pending_op_id=pending.id,
                                tenant_id=scope.tenant_id,
                                knowledge_base_id=scope.knowledge_base_id,
                                knowledge_id=pending.knowledge_id,
                                op=pending.op,
                                op_version=pending.op_version,
                                payload=_safe_dead_letter_payload(
                                    pending.payload,
                                    knowledge_id=pending.knowledge_id,
                                ),
                                fail_count=next_fail_count,
                                last_error_code=failure.error_code.strip()[:128],
                                last_error_summary=_safe_error_summary(
                                    failure.error_summary
                                ),
                            )
                            .on_conflict_do_nothing(
                                constraint="uq_wiki_dead_letters_pending_op"
                            )
                        )
                        released = await self._finalization.release(
                            session,
                            _finalization_request(
                                scope,
                                pending.knowledge_id,
                                pending.op_version,
                                pending.op,
                            ),
                        )
                        if not released:
                            raise InvariantError(
                                "dead-letter finalization marker 不存在或已被释放"
                            )
                        deleted = await session.execute(
                            delete(WikiPendingOp).where(
                                WikiPendingOp.tenant_id == scope.tenant_id,
                                WikiPendingOp.knowledge_base_id
                                == scope.knowledge_base_id,
                                WikiPendingOp.id == pending.id,
                                WikiPendingOp.claim_token == request.claim_token,
                            )
                        )
                        if deleted.rowcount != 1:
                            raise ClaimLost("dead-letter 操作的 claim 已失效")
                        continue
                    pending.fail_count = next_fail_count
                    pending.claimed_at = None
                    pending.claim_token = None

                await _cancel_claim_recovery(session, scope, request.claim_token)
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
                    await _enqueue_follow_up(session, scope, request.operation_id)
                await session.flush()
                return actual_outcome

    async def pending_count(self, scope: WikiScope) -> int:
        async with self._session_factory() as session:
            return int(
                (
                    await session.execute(
                        select(func.count(WikiPendingOp.id)).where(
                            WikiPendingOp.tenant_id == scope.tenant_id,
                            WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
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
