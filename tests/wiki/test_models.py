from __future__ import annotations

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from app.infrastructure.database.base import Base
from app.wiki.models import (
    TaskOutbox,
    WikiFinalizationMarker,
    WikiFolder,
    WikiLink,
    WikiLogEntry,
    WikiPage,
    WikiPageIssue,
    WikiPendingOp,
)


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


def test_phase_two_models_are_registered() -> None:
    assert WikiPendingOp.__table__.name == "wiki_pending_ops"
    assert TaskOutbox.__table__.name == "task_outbox"
    assert WikiFinalizationMarker.__table__.name == "wiki_finalization_markers"


def test_pending_op_has_scope_and_idempotency_constraint() -> None:
    columns = {column.name for column in WikiPendingOp.__table__.columns}
    assert {
        "tenant_id",
        "knowledge_base_id",
        "knowledge_id",
        "op",
        "op_version",
        "payload",
        "fail_count",
        "enqueued_at",
        "claimed_at",
        "claim_token",
    }.issubset(columns)
    assert any(
        constraint.name == "uq_wiki_pending_ops_version"
        for constraint in WikiPendingOp.__table__.constraints
    )
    assert "ix_wiki_pending_ops_scope_claim" in {
        index.name for index in WikiPendingOp.__table__.indexes
    }


def test_task_outbox_has_delivery_contract() -> None:
    columns = {column.name for column in TaskOutbox.__table__.columns}
    assert {
        "tenant_id",
        "knowledge_base_id",
        "event_type",
        "dedup_key",
        "payload",
        "available_at",
        "claimed_at",
        "claim_token",
        "attempts",
        "sent_at",
        "created_at",
    }.issubset(columns)
    assert any(
        constraint.name == "uq_task_outbox_dedup_key"
        for constraint in TaskOutbox.__table__.constraints
    )
    assert {
        "ix_task_outbox_delivery",
        "ix_task_outbox_scope",
    }.issubset(index.name for index in TaskOutbox.__table__.indexes)


def test_finalization_marker_has_attempt_idempotency_contract() -> None:
    columns = {column.name for column in WikiFinalizationMarker.__table__.columns}
    assert {
        "tenant_id",
        "knowledge_base_id",
        "knowledge_id",
        "attempt",
        "subtask_name",
        "registered_at",
        "released_at",
    }.issubset(columns)
    assert any(
        constraint.name == "uq_wiki_finalization_markers_attempt"
        for constraint in WikiFinalizationMarker.__table__.constraints
    )
    assert "ix_wiki_finalization_markers_scope" in {
        index.name for index in WikiFinalizationMarker.__table__.indexes
    }
