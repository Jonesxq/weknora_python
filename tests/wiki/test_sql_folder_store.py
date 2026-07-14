from __future__ import annotations

from uuid import uuid4

from sqlalchemy.dialects import postgresql

from app.wiki.scope import WikiScope
from app.wiki.sql_folder_store import (
    build_folder_lookup_statement,
    build_folder_subtree_statement,
)


def _sql(statement) -> str:
    return " ".join(
        str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})).split()
    )


def test_folder_lookup_is_scoped_and_excludes_deleted_rows() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="user")
    folder_id = uuid4()

    sql = _sql(build_folder_lookup_statement(scope, folder_id, for_update=True))

    assert "wiki_folders.tenant_id = 7" in sql
    assert f"wiki_folders.knowledge_base_id = '{scope.knowledge_base_id}'" in sql
    assert "wiki_folders.deleted_at IS NULL" in sql
    assert "FOR UPDATE" in sql


def test_folder_subtree_query_is_scoped_and_uses_escaped_prefix() -> None:
    scope = WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="user")

    sql = _sql(build_folder_subtree_statement(scope, "/技术/AI_100%"))

    assert "wiki_folders.tenant_id = 7" in sql
    assert "wiki_folders.path =" in sql
    assert "LIKE" in sql
    assert "ESCAPE '/'" in sql
