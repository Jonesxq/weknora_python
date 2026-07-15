"""创建 Wiki 阶段二摄取队列表结构

Revision ID: 20260714_02
Revises: 20260714_01
Create Date: 2026-07-14
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260714_02"
down_revision: str | None = "20260714_01"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSON_OBJECT = sa.text("'{}'::jsonb")


def upgrade() -> None:
    op.create_table(
        "wiki_pending_ops",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        sa.Column("knowledge_base_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("knowledge_id", sa.String(length=255), nullable=False),
        sa.Column("op", sa.String(length=32), server_default=sa.text("'ingest'"), nullable=False),
        sa.Column("op_version", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_OBJECT, nullable=False),
        sa.Column("fail_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("enqueued_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "knowledge_id",
            "op",
            "op_version",
            name="uq_wiki_pending_ops_version",
        ),
    )
    op.create_index(
        "ix_wiki_pending_ops_scope_claim",
        "wiki_pending_ops",
        ["tenant_id", "knowledge_base_id", "claimed_at", "enqueued_at"],
    )

    op.create_table(
        "wiki_finalization_markers",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        sa.Column("knowledge_base_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("knowledge_id", sa.String(length=255), nullable=False),
        sa.Column("attempt", sa.String(length=255), nullable=False),
        sa.Column("subtask_name", sa.String(length=64), server_default=sa.text("'wiki'"), nullable=False),
        sa.Column("registered_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "knowledge_id",
            "attempt",
            "subtask_name",
            name="uq_wiki_finalization_markers_attempt",
        ),
    )
    op.create_index(
        "ix_wiki_finalization_markers_scope",
        "wiki_finalization_markers",
        ["tenant_id", "knowledge_base_id", "released_at"],
    )

    op.create_table(
        "task_outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        sa.Column("knowledge_base_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("dedup_key", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_OBJECT, nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "event_type",
            "dedup_key",
            name="uq_task_outbox_scope_event_dedup",
        ),
    )
    op.create_index(
        "ix_task_outbox_delivery",
        "task_outbox",
        ["sent_at", "available_at", "claimed_at"],
    )
    op.create_index(
        "ix_task_outbox_scope",
        "task_outbox",
        ["tenant_id", "knowledge_base_id", "sent_at"],
    )


def downgrade() -> None:
    op.drop_table("task_outbox")
    op.drop_table("wiki_finalization_markers")
    op.drop_table("wiki_pending_ops")
