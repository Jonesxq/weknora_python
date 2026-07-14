from __future__ import annotations

from uuid import uuid4

from sqlalchemy.dialects import postgresql
import pytest

from app.wiki.errors import WikiValidationError
from app.wiki.query_service import _decode_issue_cursor, build_search_statement
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


def test_invalid_issue_cursor_returns_domain_validation_error() -> None:
    with pytest.raises(WikiValidationError, match="cursor"):
        _decode_issue_cursor("not-valid-base64")
