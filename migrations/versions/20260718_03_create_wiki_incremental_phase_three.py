"""创建 Wiki 阶段三贡献账本与死信队列表结构

Revision ID: 20260718_03
Revises: 20260714_02
Create Date: 2026-07-18
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260718_03"
down_revision: str | None = "20260714_02"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSON_LIST = sa.text("'[]'::jsonb")
JSON_OBJECT = sa.text("'{}'::jsonb")


def upgrade() -> None:
    op.create_table(
        "wiki_page_contributions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        sa.Column("knowledge_base_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column("knowledge_id", sa.String(length=255), nullable=False),
        sa.Column("op_version", sa.String(length=255), nullable=False),
        sa.Column("page_type", sa.String(length=32), nullable=False),
        sa.Column("state", sa.String(length=32), server_default=sa.text("'active'"), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("aliases", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_LIST, nullable=False),
        sa.Column("chunk_refs", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_LIST, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "slug",
            "knowledge_id",
            "op_version",
            name="uq_wiki_page_contributions_version",
        ),
    )
    op.create_index(
        "uq_wiki_page_contributions_active_source",
        "wiki_page_contributions",
        ["tenant_id", "knowledge_base_id", "slug", "knowledge_id"],
        unique=True,
        postgresql_where=sa.text("state = 'active'"),
    )
    op.create_index(
        "ix_wiki_page_contributions_slug_state",
        "wiki_page_contributions",
        ["tenant_id", "knowledge_base_id", "slug", "state"],
    )
    op.create_index(
        "ix_wiki_page_contributions_source_state",
        "wiki_page_contributions",
        ["tenant_id", "knowledge_base_id", "knowledge_id", "state"],
    )

    op.create_table(
        "wiki_dead_letters",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("pending_op_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        sa.Column("knowledge_base_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("knowledge_id", sa.String(length=255), nullable=False),
        sa.Column("op", sa.String(length=32), nullable=False),
        sa.Column("op_version", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_OBJECT, nullable=False),
        sa.Column("fail_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_error_code", sa.String(length=128), nullable=False),
        sa.Column("last_error_summary", sa.String(length=2000), nullable=False),
        sa.Column("dead_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("pending_op_id", name="uq_wiki_dead_letters_pending_op"),
    )
    op.create_index(
        "ix_wiki_dead_letters_scope_dead_at",
        "wiki_dead_letters",
        ["tenant_id", "knowledge_base_id", "dead_at"],
    )

    op.create_index(
        "ix_wiki_pages_dedup_names_trgm",
        "wiki_pages",
        [sa.text("(lower(title) || ' ' || lower(coalesce(aliases::text, ''))) gist_trgm_ops")],
        postgresql_using="gist",
        postgresql_where=sa.text(
            "deleted_at IS NULL AND status = 'published' AND page_type IN ('entity', 'concept')"
        ),
    )


def downgrade() -> None:
    op.drop_index("ix_wiki_pages_dedup_names_trgm", table_name="wiki_pages")
    op.drop_table("wiki_dead_letters")
    op.drop_table("wiki_page_contributions")
