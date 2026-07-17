from __future__ import annotations

from sqlalchemy import BigInteger, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from app.infrastructure.database.base import Base
from app.wiki.models import (
    TaskOutbox,
    WikiFinalizationMarker,
    WikiDeadLetter,
    WikiFolder,
    WikiLink,
    WikiLogEntry,
    WikiPage,
    WikiPageIssue,
    WikiPendingOp,
    WikiPageContribution,
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
    assert "ix_wiki_pages_dedup_names_trgm" in index_names

    for index in WikiPage.__table__.indexes:
        str(CreateIndex(index).compile(dialect=postgresql.dialect()))


def test_page_dedup_names_index_matches_phase_three_migration() -> None:
    index = next(
        index for index in WikiPage.__table__.indexes if index.name == "ix_wiki_pages_dedup_names_trgm"
    )

    sql = str(CreateIndex(index).compile(dialect=postgresql.dialect()))

    assert "USING gist ((lower(title) || ' ' || lower(coalesce(aliases::text, ''))) gist_trgm_ops)" in sql
    assert "WHERE deleted_at IS NULL AND status = 'published' AND page_type IN ('entity', 'concept')" in sql


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
    assert _unique_constraints(WikiPendingOp) == {
        "uq_wiki_pending_ops_version": (
            "tenant_id",
            "knowledge_base_id",
            "knowledge_id",
            "op",
            "op_version",
        )
    }
    assert _indexes(WikiPendingOp) == {
        "ix_wiki_pending_ops_scope_claim": (
            "tenant_id",
            "knowledge_base_id",
            "claimed_at",
            "enqueued_at",
        )
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
    assert _unique_constraints(TaskOutbox) == {
        "uq_task_outbox_scope_event_dedup": (
            "tenant_id",
            "knowledge_base_id",
            "event_type",
            "dedup_key",
        )
    }
    assert _indexes(TaskOutbox) == {
        "ix_task_outbox_delivery": ("sent_at", "available_at", "claimed_at"),
        "ix_task_outbox_scope": ("tenant_id", "knowledge_base_id", "sent_at"),
    }


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
    assert _unique_constraints(WikiFinalizationMarker) == {
        "uq_wiki_finalization_markers_attempt": (
            "tenant_id",
            "knowledge_base_id",
            "knowledge_id",
            "attempt",
            "subtask_name",
        )
    }
    assert _indexes(WikiFinalizationMarker) == {
        "ix_wiki_finalization_markers_scope": (
            "tenant_id",
            "knowledge_base_id",
            "released_at",
        )
    }


def test_phase_two_column_types_and_lengths_are_exact() -> None:
    assert _string_lengths(WikiPendingOp) == {
        "knowledge_id": 255,
        "op": 32,
        "op_version": 255,
    }
    assert _string_lengths(TaskOutbox) == {
        "event_type": 64,
        "dedup_key": 64,
    }
    assert _string_lengths(WikiFinalizationMarker) == {
        "knowledge_id": 255,
        "attempt": 255,
        "subtask_name": 64,
    }

    for model in (WikiPendingOp, TaskOutbox, WikiFinalizationMarker):
        columns = model.__table__.columns
        assert isinstance(columns.id.type, postgresql.UUID)
        assert isinstance(columns.tenant_id.type, BigInteger)
        assert isinstance(columns.knowledge_base_id.type, postgresql.UUID)

    assert isinstance(WikiPendingOp.__table__.c.payload.type, postgresql.JSONB)
    assert isinstance(WikiPendingOp.__table__.c.fail_count.type, Integer)
    assert isinstance(TaskOutbox.__table__.c.payload.type, postgresql.JSONB)
    assert isinstance(TaskOutbox.__table__.c.attempts.type, Integer)
    assert isinstance(WikiPendingOp.__table__.c.claim_token.type, postgresql.UUID)
    assert isinstance(TaskOutbox.__table__.c.claim_token.type, postgresql.UUID)

    for model, names in (
        (WikiPendingOp, ("enqueued_at", "claimed_at")),
        (TaskOutbox, ("available_at", "claimed_at", "sent_at", "created_at")),
        (WikiFinalizationMarker, ("registered_at", "released_at")),
    ):
        for name in names:
            column_type = model.__table__.c[name].type
            assert isinstance(column_type, DateTime)
            assert column_type.timezone is True


def test_phase_two_nullable_contracts_are_exact() -> None:
    assert _nullable_columns(WikiPendingOp) == {"claimed_at", "claim_token"}
    assert _nullable_columns(TaskOutbox) == {"claimed_at", "claim_token", "sent_at"}
    assert _nullable_columns(WikiFinalizationMarker) == {"released_at"}


def test_phase_two_python_and_server_defaults_are_exact() -> None:
    pending = WikiPendingOp.__table__.c
    assert pending.op.default.arg == "ingest"
    assert pending.fail_count.default.arg == 0
    assert pending.payload.default.arg.__name__ == "dict"
    assert pending.id.default.arg.__name__ == "_uuid"
    assert str(pending.enqueued_at.server_default.arg) == "now()"

    outbox = TaskOutbox.__table__.c
    assert outbox.attempts.default.arg == 0
    assert outbox.payload.default.arg.__name__ == "dict"
    assert outbox.id.default.arg.__name__ == "_uuid"
    assert str(outbox.available_at.server_default.arg) == "now()"
    assert str(outbox.created_at.server_default.arg) == "now()"

    finalization = WikiFinalizationMarker.__table__.c
    assert finalization.subtask_name.default.arg == "wiki"
    assert finalization.id.default.arg.__name__ == "_uuid"
    assert str(finalization.registered_at.server_default.arg) == "now()"


def test_phase_three_models_are_registered() -> None:
    assert WikiPageContribution.__table__.name == "wiki_page_contributions"
    assert WikiDeadLetter.__table__.name == "wiki_dead_letters"


def test_page_contribution_has_idempotency_and_active_source_contract() -> None:
    columns = WikiPageContribution.__table__.c
    assert _string_lengths(WikiPageContribution) == {
        "slug": 255,
        "knowledge_id": 255,
        "op_version": 255,
        "page_type": 32,
        "state": 32,
        "title": 512,
        "content": None,
        "summary": None,
    }
    assert _nullable_columns(WikiPageContribution) == set()
    assert _unique_constraints(WikiPageContribution) == {
        "uq_wiki_page_contributions_version": (
            "tenant_id",
            "knowledge_base_id",
            "slug",
            "knowledge_id",
            "op_version",
        )
    }
    assert _indexes(WikiPageContribution) == {
        "uq_wiki_page_contributions_active_source": (
            "tenant_id",
            "knowledge_base_id",
            "slug",
            "knowledge_id",
        ),
        "ix_wiki_page_contributions_slug_state": (
            "tenant_id",
            "knowledge_base_id",
            "slug",
            "state",
        ),
        "ix_wiki_page_contributions_source_state": (
            "tenant_id",
            "knowledge_base_id",
            "knowledge_id",
            "state",
        ),
    }
    assert isinstance(columns.id.type, postgresql.UUID)
    assert isinstance(columns.tenant_id.type, BigInteger)
    assert isinstance(columns.knowledge_base_id.type, postgresql.UUID)
    assert isinstance(columns.aliases.type, postgresql.JSONB)
    assert isinstance(columns.chunk_refs.type, postgresql.JSONB)
    assert isinstance(columns.content.type, Text)
    assert isinstance(columns.summary.type, Text)
    assert columns.id.default.arg.__name__ == "_uuid"
    assert columns.state.default.arg == "active"
    assert columns.content.default.arg == ""
    assert columns.summary.default.arg == ""
    assert str(columns.content.server_default.arg) == "''"
    assert str(columns.summary.server_default.arg) == "''"
    assert columns.aliases.default.arg.__name__ == "list"
    assert columns.chunk_refs.default.arg.__name__ == "list"
    assert str(columns.created_at.server_default.arg) == "now()"
    assert str(columns.updated_at.server_default.arg) == "now()"

    active_index = next(
        index
        for index in WikiPageContribution.__table__.indexes
        if index.name == "uq_wiki_page_contributions_active_source"
    )
    sql = str(CreateIndex(active_index).compile(dialect=postgresql.dialect()))
    assert "CREATE UNIQUE INDEX uq_wiki_page_contributions_active_source" in sql
    assert "WHERE state = 'active'" in sql


def test_dead_letter_has_failure_record_contract() -> None:
    columns = WikiDeadLetter.__table__.c
    assert _string_lengths(WikiDeadLetter) == {
        "knowledge_id": 255,
        "op": 32,
        "op_version": 255,
        "last_error_code": 128,
        "last_error_summary": 2000,
    }
    assert _nullable_columns(WikiDeadLetter) == set()
    assert _unique_constraints(WikiDeadLetter) == {
        "uq_wiki_dead_letters_pending_op": ("pending_op_id",)
    }
    assert _indexes(WikiDeadLetter) == {
        "ix_wiki_dead_letters_scope_dead_at": (
            "tenant_id",
            "knowledge_base_id",
            "dead_at",
        )
    }
    assert isinstance(columns.id.type, postgresql.UUID)
    assert isinstance(columns.pending_op_id.type, postgresql.UUID)
    assert isinstance(columns.tenant_id.type, BigInteger)
    assert isinstance(columns.knowledge_base_id.type, postgresql.UUID)
    assert isinstance(columns.payload.type, postgresql.JSONB)
    assert isinstance(columns.fail_count.type, Integer)
    assert isinstance(columns.dead_at.type, DateTime)
    assert columns.dead_at.type.timezone is True
    assert columns.id.default.arg.__name__ == "_uuid"
    assert columns.payload.default.arg.__name__ == "dict"
    assert columns.fail_count.default is None
    assert str(columns.dead_at.server_default.arg) == "now()"


def test_phase_three_timestamp_columns_are_timezone_aware() -> None:
    for model, names in (
        (WikiPageContribution, ("created_at", "updated_at")),
        (WikiDeadLetter, ("dead_at",)),
    ):
        for name in names:
            column_type = model.__table__.c[name].type
            assert isinstance(column_type, DateTime)
            assert column_type.timezone is True


def _unique_constraints(model: type) -> dict[str, tuple[str, ...]]:
    return {
        constraint.name: tuple(column.name for column in constraint.columns)
        for constraint in model.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }


def _indexes(model: type) -> dict[str, tuple[str, ...]]:
    return {
        index.name: tuple(column.name for column in index.columns)
        for index in model.__table__.indexes
    }


def _string_lengths(model: type) -> dict[str, int | None]:
    return {
        column.name: column.type.length
        for column in model.__table__.columns
        if isinstance(column.type, String)
    }


def _nullable_columns(model: type) -> set[str]:
    return {column.name for column in model.__table__.columns if column.nullable}
