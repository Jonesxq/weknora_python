from __future__ import annotations

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from app.infrastructure.database.base import Base
from app.wiki.models import WikiFolder, WikiLink, WikiLogEntry, WikiPage, WikiPageIssue


def test_wiki_metadata_contains_phase_one_tables() -> None:
    assert {
        "wiki_pages",
        "wiki_folders",
        "wiki_links",
        "wiki_page_issues",
        "wiki_log_entries",
    }.issubset(Base.metadata.tables)


def test_page_slug_unique_index_only_applies_to_active_rows() -> None:
    index = next(index for index in WikiPage.__table__.indexes if index.name == "uq_wiki_pages_active_slug")

    sql = str(CreateIndex(index).compile(dialect=postgresql.dialect()))

    assert "UNIQUE INDEX" in sql
    assert "knowledge_base_id, slug" in sql
    assert "WHERE deleted_at IS NULL" in sql


def test_phase_one_models_are_scoped_by_tenant_and_knowledge_base() -> None:
    for model in (WikiPage, WikiFolder, WikiPageIssue, WikiLogEntry):
        columns = model.__table__.columns
        assert "tenant_id" in columns
        assert "knowledge_base_id" in columns

    assert "tenant_id" in WikiLink.__table__.columns
    assert "knowledge_base_id" in WikiLink.__table__.columns


def test_orm_metadata_tracks_search_indexes_created_by_migration() -> None:
    index_names = {index.name for index in WikiPage.__table__.indexes}

    assert "ix_wiki_pages_title_trgm" in index_names
    assert "ix_wiki_pages_search_fts" in index_names

    for index in WikiPage.__table__.indexes:
        str(CreateIndex(index).compile(dialect=postgresql.dialect()))
