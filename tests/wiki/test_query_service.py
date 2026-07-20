from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

from sqlalchemy.dialects import postgresql
import pytest

from app.wiki.errors import WikiValidationError
from app.wiki.enums import WikiPageType
from app.wiki.query_service import (
    WikiQueryService,
    _decode_issue_cursor,
    _graph_degree_cte,
    build_broken_link_statement,
    build_ego_neighbor_statement,
    build_empty_page_statement,
    build_index_intro_statement,
    build_orphan_page_statement,
    build_search_statement,
)
from app.wiki.scope import WikiScope


class _ScalarResult:
    def __init__(self, value: int) -> None:
        self._value = value

    def scalar_one(self) -> int:
        return self._value


class _Scalars:
    def __init__(self, values: list[object]) -> None:
        self._values = values

    def __iter__(self):
        return iter(self._values)


class _RowsResult:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def all(self) -> list[object]:
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalars(self) -> _Scalars:
        return _Scalars(self._rows)


class _RecordingSession:
    def __init__(self, values: list[int]) -> None:
        self._values = iter(values)
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return _ScalarResult(next(self._values))


class _ScriptedSession:
    def __init__(self, results: list[_RowsResult]) -> None:
        self._results = iter(results)
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return next(self._results)


class _RecordingActivityProbe:
    def __init__(self, active: bool) -> None:
        self._active = active
        self.knowledge_base_ids: list[UUID] = []

    async def is_active(self, knowledge_base_id: UUID) -> bool:
        self.knowledge_base_ids.append(knowledge_base_id)
        return self._active


def test_search_sql_is_scoped_ranked_and_does_not_use_regex() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")
    statement = build_search_statement(scope, "Acme [invalid regex", limit=20)
    sql = " ".join(
        str(
            statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        ).split()
    )

    assert "wiki_pages.tenant_id = 7" in sql
    assert f"wiki_pages.knowledge_base_id = '{scope.knowledge_base_id}'" in sql
    assert "similarity" in sql
    assert "to_tsvector" in sql
    assert "ILIKE" in sql
    assert "~" not in sql
    assert "LIMIT 20" in sql


def test_search_uses_static_regconfig_instead_of_asyncpg_varchar_parameter() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")

    compiled = build_search_statement(scope, "Acme", limit=20).compile(
        dialect=postgresql.asyncpg.dialect()
    )
    sql = str(compiled)

    assert "to_tsvector('simple'::regconfig," in sql
    assert "plainto_tsquery('simple'::regconfig," in sql
    assert "simple" not in compiled.params.values()


def test_invalid_issue_cursor_returns_domain_validation_error() -> None:
    with pytest.raises(WikiValidationError, match="cursor"):
        _decode_issue_cursor("not-valid-base64")


def test_index_intro_sql_reads_only_canonical_identity_candidates() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")

    sql = " ".join(
        str(
            build_index_intro_statement(scope).compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        ).split()
    )

    assert "wiki_pages.slug = 'index'" in sql
    assert "wiki_pages.page_type = 'index'" in sql
    assert "wiki_pages.deleted_at IS NULL" in sql
    assert "wiki_pages.status = 'published'" in sql
    assert "updated_at" not in sql
    assert "LIMIT 2" in sql


def test_graph_degree_sql_counts_only_visible_resolved_edges() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")

    sql = " ".join(
        str(
            _graph_degree_cte(scope)
            .select()
            .compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        ).split()
    )

    assert "JOIN wiki_pages AS source_page" in sql
    assert "JOIN wiki_pages AS target_page" in sql
    assert "wiki_links.target_page_id IS NOT NULL" in sql
    assert "source_page.deleted_at IS NULL" in sql
    assert "source_page.status = 'published'" in sql
    assert "source_page.tenant_id = 7" in sql
    assert f"source_page.knowledge_base_id = '{scope.knowledge_base_id}'" in sql
    assert "target_page.deleted_at IS NULL" in sql
    assert "target_page.status = 'published'" in sql
    assert "target_page.tenant_id = 7" in sql
    assert f"target_page.knowledge_base_id = '{scope.knowledge_base_id}'" in sql
    assert "wiki_links.tenant_id = 7" in sql
    assert f"wiki_links.knowledge_base_id = '{scope.knowledge_base_id}'" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("intro_rows", "expected_intro", "expected_version"),
    [
        ([], "", 0),
        (
            [SimpleNamespace(slug="index", page_type="index", content="intro", version=3)],
            "intro",
            3,
        ),
    ],
)
async def test_get_index_returns_only_unique_canonical_intro(
    intro_rows, expected_intro: str, expected_version: int
) -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")
    session = _ScriptedSession([_RowsResult(intro_rows), _RowsResult([])])

    response = await WikiQueryService(session).get_index(
        scope,
        page_type=WikiPageType.ENTITY,
        limit=1,
    )

    assert response.intro == expected_intro
    assert response.version == expected_version
    assert len(response.groups) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "intro_rows",
    [
        [
            SimpleNamespace(slug="index", page_type="index", content="first", version=1),
            SimpleNamespace(slug="index", page_type="index", content="second", version=2),
        ],
        [SimpleNamespace(slug="index", page_type="entity", content="wrong", version=1)],
        [SimpleNamespace(slug="summary/index", page_type="index", content="wrong", version=1)],
    ],
)
async def test_get_index_rejects_canonical_identity_conflicts(intro_rows) -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")
    session = _ScriptedSession([_RowsResult(intro_rows), _RowsResult([])])

    with pytest.raises(WikiValidationError) as raised:
        await WikiQueryService(session).get_index(
            scope,
            page_type=WikiPageType.ENTITY,
            limit=1,
        )

    assert raised.value.code == "INDEX_IDENTITY_CONFLICT"
    assert raised.value.message == "canonical Index 身份冲突"


def test_ego_neighbor_sql_deduplicates_ranks_and_applies_remaining_budget() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")
    statement = build_ego_neighbor_statement(
        scope,
        frontier={uuid4()},
        visited={uuid4()},
        limit=25,
        types={"entity", "concept"},
    )
    sql = " ".join(
        str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})).split()
    )

    assert "SELECT DISTINCT" in sql
    assert "link_count DESC" in sql
    assert "neighbor.slug ASC" in sql
    assert "LIMIT 25" in sql
    assert "FROM visible_edges JOIN wiki_pages AS neighbor" in sql


@pytest.mark.asyncio
async def test_edges_between_sql_uses_visible_edges() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")
    page_ids = {uuid4(), uuid4()}
    session = _ScriptedSession([_RowsResult([])])

    await WikiQueryService(session)._edges_between(
        scope,
        page_ids,
        {page_id: str(page_id) for page_id in page_ids},
    )

    sql = " ".join(
        str(
            session.statements[0].compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        ).split()
    )
    assert "FROM visible_edges" in sql
    assert "visible_edges.source_page_id" in sql
    assert "visible_edges.target_page_id" in sql


def test_lint_detail_queries_are_limited_to_one_batch() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")
    statements = (
        build_broken_link_statement(scope, limit=200),
        build_empty_page_statement(scope, limit=200),
        build_orphan_page_statement(scope, limit=200),
    )

    for statement in statements:
        sql = " ".join(
            str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})).split()
        )
        assert "LIMIT 200" in sql


@pytest.mark.asyncio
async def test_stats_counts_pending_ops_in_scope_and_probes_activity() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")
    session = _RecordingSession([3, 4, 5, 6, 2])
    activity_probe = _RecordingActivityProbe(True)

    stats = await WikiQueryService(session, activity_probe).get_stats(scope)

    assert stats.model_dump() == {
        "page_count": 3,
        "folder_count": 4,
        "link_count": 5,
        "issue_count": 6,
        "pending_tasks": 2,
        "is_active": True,
    }
    assert activity_probe.knowledge_base_ids == [scope.knowledge_base_id]
    pending_sql = " ".join(
        str(
            session.statements[-1].compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        ).split()
    )
    assert "FROM wiki_pending_ops" in pending_sql
    assert "wiki_pending_ops.tenant_id = 7" in pending_sql
    assert (
        f"wiki_pending_ops.knowledge_base_id = '{scope.knowledge_base_id}'"
        in pending_sql
    )


@pytest.mark.asyncio
async def test_stats_defaults_to_inactive_probe() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="viewer")

    stats = await WikiQueryService(_RecordingSession([0, 0, 0, 0, 0])).get_stats(scope)

    assert stats.is_active is False
