from __future__ import annotations

from uuid import uuid4

from sqlalchemy.dialects import postgresql

from app.schemas.wiki.pages import WikiPageListQuery
from app.wiki.scope import WikiScope
from app.wiki.sql_page_store import (
    build_page_list_statement,
    build_page_lookup_statement,
    build_page_update_statement,
)


def _sql(statement) -> str:
    return " ".join(
        str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})).split()
    )


def test_page_lookup_sql_is_scoped_and_excludes_deleted_rows() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="user")

    sql = _sql(build_page_lookup_statement(scope, "entity/acme", include_inactive=False))

    assert "wiki_pages.tenant_id = 7" in sql
    assert f"wiki_pages.knowledge_base_id = '{scope.knowledge_base_id}'" in sql
    assert "wiki_pages.slug = 'entity/acme'" in sql
    assert "wiki_pages.deleted_at IS NULL" in sql
    assert "wiki_pages.status = 'published'" in sql


def test_page_update_sql_uses_optimistic_version_and_scope() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="user")

    sql = _sql(
        build_page_update_statement(
            scope,
            "entity/acme",
            expected_version=4,
            changes={"summary": "新摘要"},
            increment_version=True,
        )
    )

    assert "wiki_pages.tenant_id = 7" in sql
    assert "wiki_pages.version = 4" in sql
    assert "version=(wiki_pages.version + 1)" in sql
    assert "RETURNING" in sql


def test_page_list_sql_is_a_narrow_projection() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="user")

    sql = _sql(build_page_list_statement(scope, WikiPageListQuery(page_size=20)))
    selected = sql.split(" FROM ", maxsplit=1)[0]

    assert "wiki_pages.summary" in selected
    assert "wiki_pages.content" not in selected
    assert "wiki_pages.source_refs" not in selected
    assert "wiki_pages.chunk_refs" not in selected
