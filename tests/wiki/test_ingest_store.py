from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy.dialects import postgresql

from app.wiki.ingest.schemas import FinalizationRequest, ReducedPage, SourceKnowledge
from app.wiki.ingest.store import (
    ClaimLost,
    EnqueueRecord,
    ExistingPageRecord,
    InvariantError,
    SqlAlchemyIngestStore,
    SqlFinalizationPort,
    build_claim_recovery_dedup_key,
    build_claim_outbox_statement,
    build_claim_pending_ops_statement,
    build_finalization_register_statement,
    build_finalization_release_statement,
    build_outbox_dedup_key,
    _cancel_claim_recovery,
    _enqueue_follow_up,
    _pending_record,
)
from app.wiki.models import WikiPendingOp
from app.wiki.scope import WikiScope


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
SCOPE = WikiScope(tenant_id=7, knowledge_base_id=KB_ID, actor_id="worker")
NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)


def _sql(statement) -> str:
    return " ".join(
        str(statement.compile(dialect=postgresql.dialect())).split()
    )


def test_claim_pending_sql_is_scoped_ordered_and_skip_locked() -> None:
    sql = _sql(
        build_claim_pending_ops_statement(
            SCOPE, limit=5, stale_before=NOW - timedelta(minutes=10)
        )
    )

    assert "wiki_pending_ops.tenant_id" in sql
    assert "wiki_pending_ops.knowledge_base_id" in sql
    assert "wiki_pending_ops.claimed_at IS NULL" in sql
    assert "wiki_pending_ops.claimed_at <" in sql
    assert "ORDER BY wiki_pending_ops.enqueued_at, wiki_pending_ops.id" in sql
    assert "LIMIT" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql


def test_claim_outbox_sql_excludes_sent_and_future_events() -> None:
    sql = _sql(
        build_claim_outbox_statement(
            limit=10,
            now=NOW,
            stale_before=NOW - timedelta(minutes=10),
        )
    )

    assert "task_outbox.sent_at IS NULL" in sql
    assert "task_outbox.available_at <=" in sql
    assert "task_outbox.claimed_at IS NULL" in sql
    assert "task_outbox.claimed_at <" in sql
    assert "ORDER BY task_outbox.available_at, task_outbox.created_at, task_outbox.id" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql


@pytest.mark.parametrize("limit", [0, -1])
def test_claim_builders_reject_non_positive_limit(limit: int) -> None:
    with pytest.raises(ValueError, match="正整数"):
        build_claim_pending_ops_statement(SCOPE, limit=limit, stale_before=NOW)
    with pytest.raises(ValueError, match="正整数"):
        build_claim_outbox_statement(limit=limit, now=NOW, stale_before=NOW)


def test_outbox_dedup_is_stable_scoped_sha256() -> None:
    key = build_outbox_dedup_key(
        7, KB_ID, "wiki.batch.trigger", "knowledge-1", "version-1"
    )
    same = build_outbox_dedup_key(
        7, KB_ID, "wiki.batch.trigger", "knowledge-1", "version-1"
    )

    assert key == same
    assert len(key) == 64
    int(key, 16)
    variants = {
        build_outbox_dedup_key(8, KB_ID, "wiki.batch.trigger", "knowledge-1", "version-1"),
        build_outbox_dedup_key(7, uuid4(), "wiki.batch.trigger", "knowledge-1", "version-1"),
        build_outbox_dedup_key(7, KB_ID, "other", "knowledge-1", "version-1"),
        build_outbox_dedup_key(7, KB_ID, "wiki.batch.trigger", "knowledge-2", "version-1"),
        build_outbox_dedup_key(7, KB_ID, "wiki.batch.trigger", "knowledge-1", "version-2"),
    }
    assert key not in variants
    assert len(variants) == 5


def test_finalization_sql_uses_named_conflict_and_strict_release_identity() -> None:
    request = FinalizationRequest(
        tenant_id=7,
        knowledge_base_id=KB_ID,
        knowledge_id="knowledge-1",
        attempt="version-1",
        subtask_name="wiki",
    )
    register_sql = _sql(build_finalization_register_statement(request))
    release_sql = _sql(
        build_finalization_release_statement(request, released_at=NOW)
    )

    assert "ON CONFLICT ON CONSTRAINT uq_wiki_finalization_markers_attempt DO NOTHING" in register_sql
    assert "RETURNING wiki_finalization_markers.id" in register_sql
    for column in (
        "tenant_id",
        "knowledge_base_id",
        "knowledge_id",
        "attempt",
        "subtask_name",
    ):
        assert f"wiki_finalization_markers.{column}" in release_sql
    assert "wiki_finalization_markers.released_at IS NULL" in release_sql
    assert "RETURNING wiki_finalization_markers.id" in release_sql


def test_enqueue_record_is_frozen_and_exposes_pending_id() -> None:
    pending_id = uuid4()
    record = EnqueueRecord(
        id=pending_id,
        tenant_id=7,
        knowledge_base_id=KB_ID,
        knowledge_id="knowledge-1",
        op_version="version-1",
        payload={"knowledge_id": "knowledge-1"},
        outbox_event_id=uuid4(),
        deduplicated=False,
    )

    assert record.pending_op_id == pending_id
    with pytest.raises(FrozenInstanceError):
        record.tenant_id = 8  # type: ignore[misc]


def test_existing_page_record_is_frozen() -> None:
    record = ExistingPageRecord(
        page_id=uuid4(),
        version=3,
        page=ReducedPage(
            slug="entity/acme",
            title="Acme",
            page_type="entity",
            content="正文",
            summary="摘要",
        ),
    )
    with pytest.raises(FrozenInstanceError):
        record.version = 4  # type: ignore[misc]


def test_pending_record_deep_copies_nested_payload() -> None:
    row = WikiPendingOp(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        knowledge_id="knowledge-1",
        op="ingest",
        op_version="version-1",
        payload={"nested": {"values": ["original"]}},
        fail_count=0,
        enqueued_at=NOW,
    )
    record = _pending_record(row)
    record.payload["nested"]["values"].append("record-only")  # type: ignore[index,union-attr]
    assert row.payload == {"nested": {"values": ["original"]}}


@pytest.mark.asyncio
async def test_claim_pending_atomically_schedules_timeout_recovery() -> None:
    pending = WikiPendingOp(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        knowledge_id="knowledge-recovery",
        op="ingest",
        op_version="version-1",
        payload={"knowledge_id": "knowledge-recovery"},
        fail_count=0,
        enqueued_at=NOW,
    )

    class Rows:
        def scalars(self):
            return [pending]

    class Session:
        def __init__(self) -> None:
            self.statements = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def begin(self):
            return self

        async def execute(self, statement):
            self.statements.append(statement)
            return Rows()

        async def flush(self) -> None:
            return None

    session = Session()

    class Factory:
        def __call__(self):
            return session

    store = SqlAlchemyIngestStore(Factory(), SqlFinalizationPort())  # type: ignore[arg-type]
    claimed = await store.claim_pending(SCOPE, 1, 600)

    assert len(claimed) == 1
    assert len(session.statements) == 2
    recovery = session.statements[1]
    sql = _sql(recovery)
    assert "INSERT INTO task_outbox" in sql
    params = recovery.compile(dialect=postgresql.dialect()).params
    assert params["event_type"] == "wiki.batch.trigger"
    assert params["dedup_key"] == build_claim_recovery_dedup_key(
        SCOPE, claimed[0].claim_token
    )


@pytest.mark.asyncio
async def test_follow_up_and_recovery_cancel_emit_one_statement_each() -> None:
    class Session:
        def __init__(self) -> None:
            self.statements = []

        async def execute(self, statement):
            self.statements.append(statement)

    follow_up_session = Session()
    await _enqueue_follow_up(follow_up_session, SCOPE, uuid4())  # type: ignore[arg-type]
    assert len(follow_up_session.statements) == 1
    assert "INSERT INTO task_outbox" in _sql(follow_up_session.statements[0])

    cancel_session = Session()
    await _cancel_claim_recovery(cancel_session, SCOPE, uuid4())  # type: ignore[arg-type]
    assert len(cancel_session.statements) == 1
    assert _sql(cancel_session.statements[0]).startswith("UPDATE task_outbox")


@pytest.mark.asyncio
async def test_non_empty_runtime_operations_reject_missing_claim_token() -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("不应打开数据库 session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]
    item_id = uuid4()

    with pytest.raises(ClaimLost, match="非空 UUID"):
        await store.release_failed(SCOPE, [item_id], None)
    with pytest.raises(ClaimLost, match="非空 UUID"):
        await store.mark_outbox_sent([item_id], None)
    with pytest.raises(ClaimLost, match="非空 UUID"):
        await store.release_outbox([item_id], None)
    with pytest.raises(ClaimLost, match="非空 UUID"):
        await store.apply_results(
            SCOPE,
            None,
            [],
            [item_id],
            uuid4(),
            expected_pages={},
        )

    await store.release_failed(SCOPE, [], None)
    await store.mark_outbox_sent([], None)
    await store.release_outbox([], None)


@pytest.mark.asyncio
async def test_sql_store_rejects_forged_scope_before_opening_session() -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("不应打开数据库 session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]
    knowledge = SourceKnowledge(
        id="knowledge-1",
        tenant_id=8,
        knowledge_base_id=KB_ID,
        title="伪造来源",
        op_version="version-1",
    )

    with pytest.raises(ValueError, match="不一致"):
        await store.enqueue(
            SCOPE, knowledge, {"knowledge_id": knowledge.id}, delay_seconds=0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("completed_count", [0, 1])
async def test_result_pages_require_completed_contributors(
    completed_count: int,
) -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("非法批次不应打开数据库 session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]
    completed_ids = [uuid4()] if completed_count else []
    failed_ids = [uuid4()]
    page = ReducedPage(
        slug="entity/no-contributor",
        title="无贡献页面",
        page_type="entity",
        content="正文",
        summary="摘要",
        contributor_op_ids=[],
    )

    with pytest.raises(InvariantError, match="contributor"):
        await store.apply_results(
            SCOPE,
            uuid4(),
            [page],
            completed_ids,
            uuid4(),
            failed_op_ids=failed_ids,
            expected_pages={page.slug: None},
        )
