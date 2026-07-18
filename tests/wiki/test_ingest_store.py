from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from collections.abc import Sequence
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql

from app.wiki.ingest import store as ingest_store
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
    _finalization_request,
    _pending_record,
    build_dedup_candidate_statement,
)
from app.wiki.ingest.schemas import TopicCandidate
from app.wiki.models import TaskOutbox, WikiPage, WikiPageContribution, WikiPendingOp
from app.wiki.scope import WikiScope


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
SCOPE = WikiScope(tenant_id=7, knowledge_base_id=KB_ID, actor_id="worker")
NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)


def _sql(statement) -> str:
    return " ".join(str(statement.compile(dialect=postgresql.dialect())).split())


def test_claim_pending_sql_is_scoped_ordered_and_skip_locked() -> None:
    sql = _sql(
        build_claim_pending_ops_statement(
            SCOPE, limit=5, stale_before=NOW - timedelta(minutes=10)
        )
    )

    assert "wiki_pending_ops.tenant_id" in sql
    assert "wiki_pending_ops.knowledge_base_id" in sql
    assert "wiki_pending_ops.claimed_at IS NULL" in sql
    assert "wiki_pending_ops.claimed_at <=" in sql
    assert "ORDER BY wiki_pending_ops.enqueued_at, wiki_pending_ops.id" in sql
    assert "LIMIT" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql


def test_dedup_candidate_sql_is_scoped_index_equivalent_and_limited() -> None:
    statement = build_dedup_candidate_statement(
        SCOPE,
        TopicCandidate(
            name="Acme", slug="entity/acme", page_type="entity", aliases=["ACME Corp"]
        ),
        limit=20,
    )
    sql = _sql(statement)

    for fragment in (
        "wiki_pages.tenant_id",
        "wiki_pages.knowledge_base_id",
        "wiki_pages.deleted_at IS NULL",
        "wiki_pages.status =",
        "wiki_pages.page_type =",
        "lower(wiki_pages.title)",
        "coalesce(CAST(wiki_pages.aliases AS TEXT),",
        " <-> ",
        "ORDER BY",
        "wiki_pages.slug",
        "LIMIT",
    ):
        assert fragment in sql
    assert "|| ' '" in sql
    assert "coalesce(CAST(wiki_pages.aliases AS TEXT), '')" in sql
    assert "least(" not in sql.lower()


def test_dedup_single_name_sql_has_no_least_and_aliases_do_not_add_empty_query() -> (
    None
):
    sql = _sql(
        build_dedup_candidate_statement(
            SCOPE,
            TopicCandidate(
                name="Acme", slug="entity/acme", page_type="entity", aliases=[]
            ),
        )
    )
    assert "LEAST" not in sql
    assert sql.count(" <-> ") == 2  # select distance + KNN order


def _dedup_row(**updates) -> WikiPage:
    values = {
        "id": uuid4(),
        "tenant_id": SCOPE.tenant_id,
        "knowledge_base_id": KB_ID,
        "slug": "entity/db",
        "title": "Database",
        "page_type": "entity",
        "status": "published",
        "deleted_at": None,
        "aliases": ["DB"],
    }
    values.update(updates)
    return WikiPage(**values)


class _DedupSession:
    def __init__(self, rows):
        self.rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def execute(self, _statement):
        class Result:
            def __init__(self, rows):
                self.rows = rows

            def all(self):
                return [(row, float(index)) for index, row in enumerate(self.rows)]

        return Result(self.rows)


class _QuerySession:
    def __init__(self, batches):
        self.batches = list(batches)
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def execute(self, _statement):
        batch = self.batches[self.calls]
        self.calls += 1

        class Result:
            def __init__(self, rows):
                self.rows = rows

            def all(self):
                return self.rows

        return Result(batch)


class _RowLike(Sequence[object]):
    def __init__(self, page: WikiPage, distance: object):
        self.values = (page, distance)

    def __getitem__(self, index):
        return self.values[index]

    def __len__(self):
        return 2


@pytest.mark.asyncio
async def test_dedup_query_aggregates_min_distance_and_global_top_limit() -> None:
    main = [
        (_dedup_row(slug=f"entity/main-{index}"), float(index + 1))
        for index in range(20)
    ]
    alias_best = _dedup_row(slug="entity/alias-best")
    duplicate = main[5][0]
    row_like = _RowLike(alias_best, 0.01)
    assert not isinstance(row_like, tuple)
    session = _QuerySession([main, [row_like, _RowLike(duplicate, 0.1)]])
    store = SqlAlchemyIngestStore(lambda: session, SqlFinalizationPort())  # type: ignore[arg-type]
    result = await store.find_dedup_candidates(
        SCOPE,
        TopicCandidate(
            name="Main", slug="entity/new", page_type="entity", aliases=["Alias"]
        ),
    )
    assert result[0].slug == "entity/alias-best"
    assert "entity/main-19" not in [item.slug for item in result]
    assert session.calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("distance", [None, "bad", float("nan"), float("inf"), -0.1])
async def test_dedup_query_rejects_invalid_distance(distance) -> None:
    store = SqlAlchemyIngestStore(
        lambda: _QuerySession([[(_dedup_row(), distance)]]), SqlFinalizationPort()
    )  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await store.find_dedup_candidates(
            SCOPE, TopicCandidate(name="A", slug="entity/new", page_type="entity")
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad", [[_dedup_row()], [(_dedup_row(), 1.0, "extra")], [(object(), 1.0)]]
)
async def test_dedup_query_rejects_bad_result_shape(bad) -> None:
    store = SqlAlchemyIngestStore(lambda: _QuerySession([bad]), SqlFinalizationPort())  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await store.find_dedup_candidates(
            SCOPE, TopicCandidate(name="A", slug="entity/new", page_type="entity")
        )


@pytest.mark.asyncio
async def test_dedup_query_validates_late_alias_batch_after_limit_is_full() -> None:
    first = [(_dedup_row(slug=f"entity/{index}"), float(index)) for index in range(20)]
    bad = _dedup_row(slug="entity/bad", tenant_id=99)
    store = SqlAlchemyIngestStore(
        lambda: _QuerySession([first, [(bad, 1.0)]]), SqlFinalizationPort()
    )  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await store.find_dedup_candidates(
            SCOPE,
            TopicCandidate(
                name="A", slug="entity/new", page_type="entity", aliases=["B"]
            ),
        )


@pytest.mark.asyncio
async def test_find_dedup_candidates_returns_detached_frozen_snapshots() -> None:
    first, second = (
        _dedup_row(slug="entity/a", aliases=["A"]),
        _dedup_row(slug="entity/b", aliases=["B"]),
    )
    store = SqlAlchemyIngestStore(
        lambda: _DedupSession([first, second]), SqlFinalizationPort()
    )  # type: ignore[arg-type]
    result = await store.find_dedup_candidates(
        SCOPE, TopicCandidate(name="A", slug="entity/new", page_type="entity")
    )
    first.aliases.append("mutated")
    assert [(item.slug, item.aliases) for item in result] == [
        ("entity/a", ("A",)),
        ("entity/b", ("B",)),
    ]
    with pytest.raises(ValidationError) as exc:
        result[0].aliases += ("nope",)
    assert exc.value.errors()[0]["type"] == "frozen_instance"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "updates",
    [
        {"tenant_id": 8},
        {"knowledge_base_id": uuid4()},
        {"deleted_at": NOW},
        {"status": "draft"},
        {"page_type": "concept", "slug": "concept/db"},
    ],
)
async def test_find_dedup_candidates_rejects_polluted_rows(updates) -> None:
    store = SqlAlchemyIngestStore(
        lambda: _DedupSession([_dedup_row(**updates)]), SqlFinalizationPort()
    )  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await store.find_dedup_candidates(
            SCOPE, TopicCandidate(name="A", slug="entity/new", page_type="entity")
        )


@pytest.mark.asyncio
async def test_find_dedup_candidates_rejects_overflow_and_dirty_aliases() -> None:
    overflow = SqlAlchemyIngestStore(
        lambda: _DedupSession(
            [_dedup_row(slug=f"entity/{index}") for index in range(2)]
        ),
        SqlFinalizationPort(),
    )  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await overflow.find_dedup_candidates(
            SCOPE,
            TopicCandidate(name="A", slug="entity/new", page_type="entity"),
            limit=1,
        )
    dirty = SqlAlchemyIngestStore(
        lambda: _DedupSession([_dedup_row(aliases=["", "A", "A"])]),
        SqlFinalizationPort(),
    )  # type: ignore[arg-type]
    with pytest.raises(InvariantError):
        await dirty.find_dedup_candidates(
            SCOPE, TopicCandidate(name="A", slug="entity/new", page_type="entity")
        )


@pytest.mark.parametrize("limit", [True, 0, 21])
def test_dedup_candidate_statement_rejects_invalid_limit(limit: int) -> None:
    with pytest.raises(ValueError):
        build_dedup_candidate_statement(
            SCOPE,
            TopicCandidate(name="Acme", slug="entity/acme", page_type="entity"),
            limit=limit,
        )


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
    assert (
        "ORDER BY task_outbox.available_at, task_outbox.created_at, task_outbox.id"
        in sql
    )
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
        build_outbox_dedup_key(
            8, KB_ID, "wiki.batch.trigger", "knowledge-1", "version-1"
        ),
        build_outbox_dedup_key(
            7, uuid4(), "wiki.batch.trigger", "knowledge-1", "version-1"
        ),
        build_outbox_dedup_key(7, KB_ID, "other", "knowledge-1", "version-1"),
        build_outbox_dedup_key(
            7, KB_ID, "wiki.batch.trigger", "knowledge-2", "version-1"
        ),
        build_outbox_dedup_key(
            7, KB_ID, "wiki.batch.trigger", "knowledge-1", "version-2"
        ),
    }
    assert key not in variants
    assert len(variants) == 5


def test_operation_outbox_identity_is_canonical_stable_and_strict() -> None:
    ingest = ingest_store.build_operation_outbox_identity("ingest", "retract:doc")
    retract = ingest_store.build_operation_outbox_identity("retract", "doc")
    special = ingest_store.build_operation_outbox_identity("ingest", "文档:一")

    assert ingest == '["ingest","retract:doc"]'
    assert retract == '["retract","doc"]'
    assert special == '["ingest","\\u6587\\u6863:\\u4e00"]'
    assert special == ingest_store.build_operation_outbox_identity("ingest", "文档:一")
    for invalid in ("", "INGEST", "delete", True, None):
        with pytest.raises(ValueError, match="op"):
            ingest_store.build_operation_outbox_identity(  # type: ignore[attr-defined,arg-type]
                invalid, "doc"
            )


def test_operation_outbox_identity_prevents_prefix_collision() -> None:
    ingest_key = build_outbox_dedup_key(
        7,
        KB_ID,
        "wiki.batch.trigger",
        ingest_store.build_operation_outbox_identity("ingest", "retract:doc"),
        "version-1",
    )
    retract_key = build_outbox_dedup_key(
        7,
        KB_ID,
        "wiki.batch.trigger",
        ingest_store.build_operation_outbox_identity("retract", "doc"),
        "version-1",
    )

    assert ingest_key != retract_key
    assert ingest_key == build_outbox_dedup_key(
        7,
        KB_ID,
        "wiki.batch.trigger",
        ingest_store.build_operation_outbox_identity("ingest", "retract:doc"),
        "version-1",
    )


def test_finalization_sql_uses_named_conflict_and_strict_release_identity() -> None:
    request = FinalizationRequest(
        tenant_id=7,
        knowledge_base_id=KB_ID,
        knowledge_id="knowledge-1",
        attempt="version-1",
        subtask_name="wiki",
    )
    register_sql = _sql(build_finalization_register_statement(request))
    release_sql = _sql(build_finalization_release_statement(request, released_at=NOW))

    assert (
        "ON CONFLICT ON CONSTRAINT uq_wiki_finalization_markers_attempt DO NOTHING"
        in register_sql
    )
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


class _ScriptedResult:
    def __init__(self, *, rows=(), scalar=None) -> None:
        self._rows = list(rows)
        self._scalar = scalar

    def scalars(self):
        return list(self._rows)

    def scalar_one_or_none(self):
        return self._scalar


class _ScriptedSession:
    def __init__(self, results, events: list[str] | None = None) -> None:
        self.results = list(results)
        self.statements = []
        self.events = events if events is not None else []
        self.rolled_back = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, *_args):
        if exc_type is not None:
            self.rolled_back = True
        return None

    def begin(self):
        return self

    async def execute(self, statement):
        self.statements.append(statement)
        sql = _sql(statement)
        self.events.append(sql)
        if "pg_advisory_xact_lock" in sql:
            return _ScriptedResult()
        if not self.results:
            raise AssertionError(f"unexpected SQL: {sql}")
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    async def flush(self) -> None:
        self.events.append("FLUSH")
        return None


class _OneSessionFactory:
    def __init__(self, session: _ScriptedSession) -> None:
        self.session = session

    def __call__(self):
        return self.session


class _RecordingFinalization:
    def __init__(
        self,
        events: list[str],
        *,
        registered: bool = True,
        release_ok: bool = True,
    ) -> None:
        self.events = events
        self.registered = registered
        self.release_ok = release_ok
        self.requests: list[tuple[str, FinalizationRequest]] = []

    async def register(self, _session, request: FinalizationRequest) -> bool:
        self.events.append(f"finalization.register:{request.attempt}")
        self.requests.append(("register", request))
        return self.registered

    async def release(self, _session, request: FinalizationRequest) -> bool:
        self.events.append(f"finalization.release:{request.attempt}")
        self.requests.append(("release", request))
        return self.release_ok


def _pending(
    *,
    knowledge_id: str = "knowledge-1",
    op: str = "ingest",
    version: str = "version-1",
    claimed: bool = False,
) -> WikiPendingOp:
    return WikiPendingOp(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        knowledge_id=knowledge_id,
        op=op,
        op_version=version,
        payload={"knowledge_id": knowledge_id},
        fail_count=0,
        enqueued_at=NOW,
        claimed_at=NOW if claimed else None,
        claim_token=uuid4() if claimed else None,
    )


def _outbox() -> TaskOutbox:
    return TaskOutbox(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        event_type="wiki.batch.trigger",
        dedup_key="a" * 64,
        payload={"tenant_id": SCOPE.tenant_id},
        available_at=NOW,
    )


def _contribution(
    *,
    knowledge_id: str,
    slug: str = "entity/acme",
    version: str = "version-1",
    page_type: str = "entity",
    refs: list[str] | None = None,
) -> WikiPageContribution:
    return WikiPageContribution(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        slug=slug,
        knowledge_id=knowledge_id,
        op_version=version,
        page_type=page_type,
        state="active",
        title="Acme",
        content=f"content:{knowledge_id}",
        summary=f"summary:{knowledge_id}",
        aliases=[knowledge_id],
        chunk_refs=refs or [f"chunk:{knowledge_id}"],
        created_at=NOW,
        updated_at=NOW,
    )


def _page(
    *,
    slug: str = "entity/acme",
    page_type: str = "entity",
    sources: list[str] | None = None,
    chunks: list[str] | None = None,
) -> WikiPage:
    return WikiPage(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        slug=slug,
        title="Stable title",
        page_type=page_type,
        status="published",
        content="Stable [[entity/linked]] content",
        summary="Stable summary",
        aliases=["Stable alias"],
        source_refs=sources or [],
        chunk_refs=chunks or [],
        version=4,
        created_at=NOW,
        updated_at=NOW,
        deleted_at=None,
    )


def _knowledge(version: str = "version-2") -> SourceKnowledge:
    return SourceKnowledge(
        id="knowledge-1",
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        title="Document",
        op_version=version,
    )


@pytest.mark.asyncio
async def test_enqueue_ingest_releases_and_deletes_only_old_unclaimed_versions() -> (
    None
):
    old = _pending(version="version-1")
    new = _pending(version="version-2")
    outbox = _outbox()
    events: list[str] = []
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[old]),
            _ScriptedResult(),
            _ScriptedResult(scalar=new.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=new),
            _ScriptedResult(scalar=outbox),
        ],
        events,
    )
    finalization = _RecordingFinalization(events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    result = await store.enqueue_ingest(
        SCOPE, _knowledge(), {"knowledge_id": "knowledge-1"}, delay_seconds=0
    )

    assert result.id == new.id and result.deduplicated is False
    first_sql = _sql(session.statements[0])
    assert "wiki_pending_ops.claimed_at IS NULL" in first_sql
    assert "wiki_pending_ops.op_version !=" in first_sql
    assert "FOR UPDATE" in first_sql
    delete_sql = _sql(session.statements[1])
    assert delete_sql.startswith("DELETE FROM wiki_pending_ops")
    assert "wiki_pending_ops.claimed_at IS NULL" in delete_sql
    assert [(kind, request.attempt) for kind, request in finalization.requests] == [
        ("release", "version-1"),
        ("register", "version-2"),
    ]
    assert events.index("finalization.release:version-1") < events.index(
        "finalization.register:version-2"
    )


@pytest.mark.asyncio
async def test_enqueue_ingest_duplicate_does_not_insert_release_or_emit_outbox() -> (
    None
):
    pending = _pending(version="version-2")
    outbox = _outbox()
    events: list[str] = []
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=outbox),
        ],
        events,
    )
    finalization = _RecordingFinalization(events, registered=False)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    result = await store.enqueue(
        SCOPE, _knowledge(), {"knowledge_id": "knowledge-1"}, delay_seconds=0
    )

    assert result.id == pending.id and result.deduplicated is True
    assert not any(sql.startswith("INSERT INTO wiki_pending_ops") for sql in events)
    assert not any(sql.startswith("INSERT INTO task_outbox") for sql in events)
    assert [kind for kind, _request in finalization.requests] == ["register"]


@pytest.mark.asyncio
async def test_enqueue_ingest_release_failure_rolls_back_before_new_insert() -> None:
    events: list[str] = []
    session = _ScriptedSession([_ScriptedResult(rows=[_pending()])], events)
    finalization = _RecordingFinalization(events, release_ok=False)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    with pytest.raises(InvariantError, match="finalization"):
        await store.enqueue_ingest(SCOPE, _knowledge(), {"knowledge_id": "knowledge-1"})

    assert session.rolled_back is True
    assert len(session.statements) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,delay",
    [({"knowledge_id": "knowledge-1"}, True), ([], 0), ({}, 0)],
)
async def test_enqueue_ingest_rejects_invalid_payload_and_delay_before_session(
    payload, delay
) -> None:
    class ExplodingFactory:
        def __call__(self):
            raise AssertionError("invalid boundary must not open a session")

    store = SqlAlchemyIngestStore(ExplodingFactory(), SqlFinalizationPort())  # type: ignore[arg-type]
    with pytest.raises((TypeError, ValueError)):
        await store.enqueue_ingest(SCOPE, _knowledge(), payload, delay_seconds=delay)


@pytest.mark.asyncio
async def test_enqueue_retract_removes_only_target_refs_and_keeps_visible_page() -> (
    None
):
    target = _contribution(knowledge_id="knowledge-1", refs=["chunk:a"])
    remaining = _contribution(knowledge_id="knowledge-2", refs=["chunk:b"])
    page = _page(sources=["knowledge-1", "knowledge-2"], chunks=["chunk:a", "chunk:b"])
    pending = _pending(op="retract", version="delete-1")
    outbox = _outbox()
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[target]),
            _ScriptedResult(scalar=page),
            _ScriptedResult(rows=[remaining]),
            _ScriptedResult(scalar=pending.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=outbox),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization(session.events)
    )  # type: ignore[arg-type]
    before = (page.title, page.content, page.summary, list(page.aliases), page.status)

    result = await store.enqueue_retract(
        SCOPE,
        "knowledge-1",
        "delete-1",
        {"knowledge_id": "knowledge-1"},
        delay_seconds=0,
    )

    assert result.id == pending.id and result.deduplicated is False
    assert target.state == "retract_pending"
    assert page.source_refs == ["knowledge-2"]
    assert page.chunk_refs == ["chunk:b"]
    assert page.version == 5 and page.deleted_at is None
    assert (page.title, page.content, page.summary, page.aliases, page.status) == before
    assert not any("wiki_links" in _sql(statement) for statement in session.statements)
    first_page_select = next(
        index
        for index, event in enumerate(session.events)
        if event.startswith("SELECT wiki_pages.")
    )
    assert session.events.index("FLUSH") < first_page_select


@pytest.mark.asyncio
async def test_enqueue_retract_unique_source_soft_deletes_and_clears_links() -> None:
    target = _contribution(knowledge_id="knowledge-1", refs=["chunk:a"])
    page = _page(sources=["knowledge-1"], chunks=["chunk:a"])
    pending = _pending(op="retract", version="delete-1")
    outbox = _outbox()
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[target]),
            _ScriptedResult(scalar=page),
            _ScriptedResult(rows=[]),
            _ScriptedResult(),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=outbox),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization(session.events)
    )  # type: ignore[arg-type]

    await store.enqueue_retract(
        SCOPE,
        "knowledge-1",
        "delete-1",
        {"knowledge_id": "knowledge-1"},
        delay_seconds=0,
    )

    assert target.state == "retract_pending"
    assert page.deleted_at is not None
    assert page.source_refs == [] and page.chunk_refs == []
    assert page.version == 5
    link_sql = [
        _sql(statement)
        for statement in session.statements
        if "wiki_links" in _sql(statement)
    ]
    assert link_sql[0].startswith("DELETE FROM wiki_links")
    assert link_sql[1].startswith("UPDATE wiki_links")
    assert all("wiki_links.tenant_id" in sql for sql in link_sql)


@pytest.mark.asyncio
async def test_enqueue_retract_repeat_and_no_contributions_only_return_pending() -> (
    None
):
    pending = _pending(op="retract", version="delete-1")
    outbox = _outbox()
    events: list[str] = []
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[]),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=outbox),
        ],
        events,
    )
    finalization = _RecordingFinalization(events, registered=False)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    result = await store.enqueue_retract(
        SCOPE, "knowledge-1", "delete-1", {"knowledge_id": "knowledge-1"}
    )

    assert result.id == pending.id and result.deduplicated is True
    assert "pg_advisory_xact_lock" in _sql(session.statements[0])
    assert len(session.statements) == 5
    assert not any(sql.startswith("INSERT") for sql in events)


def test_finalization_identity_distinguishes_retract_from_ingest() -> None:
    ingest = _finalization_request(SCOPE, "knowledge-1", "version-1", "ingest")
    retract = _finalization_request(SCOPE, "knowledge-1", "version-1", "retract")

    assert ingest.subtask_name == "wiki"
    assert retract.subtask_name == "wiki-retract"
    assert ingest != retract


@pytest.mark.asyncio
async def test_enqueue_retract_keeps_claimed_ingest_and_releases_unclaimed() -> None:
    unclaimed = _pending(version="version-1")
    pending = _pending(op="retract", version="delete-1")
    outbox = _outbox()
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[unclaimed]),
            _ScriptedResult(),
            _ScriptedResult(rows=[]),
            _ScriptedResult(scalar=pending.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=outbox),
        ]
    )
    finalization = _RecordingFinalization(session.events)
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), finalization)  # type: ignore[arg-type]

    await store.enqueue_retract(
        SCOPE, "knowledge-1", "delete-1", {"knowledge_id": "knowledge-1"}
    )

    select_sql = next(
        _sql(statement)
        for statement in session.statements
        if _sql(statement).startswith("SELECT wiki_pending_ops.")
    )
    assert "wiki_pending_ops.claimed_at IS NULL" in select_sql
    assert "FOR UPDATE" in select_sql
    assert ("release", "version-1") in [
        (kind, request.attempt) for kind, request in finalization.requests
    ]
    assert any(
        _sql(statement).startswith("DELETE FROM wiki_pending_ops")
        for statement in session.statements
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["missing", "scope", "type", "refs"])
async def test_enqueue_retract_dirty_or_missing_page_rolls_back(problem: str) -> None:
    target = _contribution(knowledge_id="knowledge-1")
    page = _page(sources=["knowledge-1"], chunks=["chunk:knowledge-1"])
    if problem == "scope":
        page.tenant_id = SCOPE.tenant_id + 1
    elif problem == "type":
        page.page_type = "concept"
    elif problem == "refs":
        page.source_refs = ["polluted"]
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[target]),
            _ScriptedResult(scalar=None if problem == "missing" else page),
            _ScriptedResult(rows=[]),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization(session.events)
    )  # type: ignore[arg-type]

    with pytest.raises(InvariantError):
        await store.enqueue_retract(
            SCOPE, "knowledge-1", "delete-1", {"knowledge_id": "knowledge-1"}
        )

    assert session.rolled_back is True


@pytest.mark.asyncio
async def test_enqueue_retract_locks_slugs_in_stable_order() -> None:
    second = _contribution(knowledge_id="knowledge-1", slug="entity/z")
    first = _contribution(knowledge_id="knowledge-1", slug="entity/a")
    page_a = _page(
        slug="entity/a", sources=["knowledge-1"], chunks=["chunk:knowledge-1"]
    )
    page_z = _page(
        slug="entity/z", sources=["knowledge-1"], chunks=["chunk:knowledge-1"]
    )
    pending = _pending(op="retract", version="delete-1")
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[]),
            _ScriptedResult(rows=[second, first]),
            _ScriptedResult(scalar=page_a),
            _ScriptedResult(rows=[]),
            _ScriptedResult(),
            _ScriptedResult(),
            _ScriptedResult(scalar=page_z),
            _ScriptedResult(rows=[]),
            _ScriptedResult(),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending.id),
            _ScriptedResult(),
            _ScriptedResult(scalar=pending),
            _ScriptedResult(scalar=_outbox()),
        ]
    )
    store = SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization(session.events)
    )  # type: ignore[arg-type]

    await store.enqueue_retract(
        SCOPE, "knowledge-1", "delete-1", {"knowledge_id": "knowledge-1"}
    )

    page_selects = [
        statement
        for statement in session.statements
        if _sql(statement).startswith("SELECT wiki_pages.")
    ]
    assert [
        statement.compile(dialect=postgresql.dialect()).params["slug_1"]
        for statement in page_selects
    ] == [
        "entity/a",
        "entity/z",
    ]
