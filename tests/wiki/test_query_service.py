from __future__ import annotations

from uuid import uuid4

from sqlalchemy.dialects import postgresql
import pytest

from app.wiki.errors import WikiValidationError
from app.wiki.query_service import (
    _decode_issue_cursor,
    build_broken_link_statement,
    build_ego_neighbor_statement,
    build_empty_page_statement,
    build_orphan_page_statement,
    build_search_statement,
)
from app.wiki.scope import WikiScope


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
