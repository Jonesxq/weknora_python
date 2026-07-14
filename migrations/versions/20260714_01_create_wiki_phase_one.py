"""创建 Wiki 阶段一表结构

Revision ID: 20260714_01
Revises:
Create Date: 2026-07-14
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260714_01"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

JSON_LIST = sa.text("'[]'::jsonb")
JSON_OBJECT = sa.text("'{}'::jsonb")


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.create_table(
        "wiki_folders",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        sa.Column("knowledge_base_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("parent_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("name", sa.String(length=512), nullable=False),
        sa.Column("path", sa.String(length=2048), nullable=False),
        sa.Column("depth", sa.Integer(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["parent_id"], ["wiki_folders.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_wiki_folders_active_sibling",
        "wiki_folders",
        ["knowledge_base_id", "parent_id", "name"],
        unique=True,
        postgresql_where=sa.text("deleted_at IS NULL"),
        postgresql_nulls_not_distinct=True,
    )
    op.create_index(
        "ix_wiki_folders_scope_path",
        "wiki_folders",
        ["tenant_id", "knowledge_base_id", "path"],
    )

    op.create_table(
        "wiki_pages",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        sa.Column("knowledge_base_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("page_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("aliases", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_LIST, nullable=False),
        sa.Column("parent_slug", sa.String(length=255), nullable=True),
        sa.Column("folder_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("category_path", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_LIST, nullable=False),
        sa.Column("wiki_path", sa.String(length=1024), nullable=False),
        sa.Column("depth", sa.Integer(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False),
        sa.Column("source_refs", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_LIST, nullable=False),
        sa.Column("chunk_refs", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_LIST, nullable=False),
        sa.Column("page_metadata", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_OBJECT, nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["folder_id"], ["wiki_folders.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_wiki_pages_active_slug",
        "wiki_pages",
        ["knowledge_base_id", "slug"],
        unique=True,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_wiki_pages_scope_type_status",
        "wiki_pages",
        ["tenant_id", "knowledge_base_id", "page_type", "status"],
    )
    op.create_index(
        "ix_wiki_pages_folder_order",
        "wiki_pages",
        ["knowledge_base_id", "folder_id", "wiki_path", "sort_order", "title"],
    )
    op.create_index(
        "ix_wiki_pages_source_refs_gin",
        "wiki_pages",
        ["source_refs"],
        postgresql_using="gin",
        postgresql_ops={"source_refs": "jsonb_path_ops"},
    )
    op.execute(
        "CREATE INDEX ix_wiki_pages_title_trgm ON wiki_pages "
        "USING gin (lower(title) gin_trgm_ops) WHERE deleted_at IS NULL"
    )
    op.execute(
        "CREATE INDEX ix_wiki_pages_search_fts ON wiki_pages USING gin "
        "(to_tsvector('simple', title || ' ' || summary || ' ' || content)) "
        "WHERE deleted_at IS NULL"
    )

    op.create_table(
        "wiki_links",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("knowledge_base_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_page_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_slug", sa.String(length=255), nullable=False),
        sa.Column("target_page_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["source_page_id"], ["wiki_pages.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_page_id"], ["wiki_pages.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_page_id", "target_slug", name="uq_wiki_links_source_target"),
    )
    op.create_index("ix_wiki_links_scope_target", "wiki_links", ["knowledge_base_id", "target_slug"])
    op.create_index("ix_wiki_links_target_page", "wiki_links", ["target_page_id"])

    op.create_table(
        "wiki_page_issues",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        sa.Column("knowledge_base_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("page_slug", sa.String(length=255), nullable=False),
        sa.Column("issue_type", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("suspected_knowledge_ids", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_LIST, nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("reported_by", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_wiki_page_issues_scope_status",
        "wiki_page_issues",
        ["tenant_id", "knowledge_base_id", "status", "created_at"],
    )

    op.create_table(
        "wiki_log_entries",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        sa.Column("knowledge_base_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("pages_affected", postgresql.JSONB(astext_type=sa.Text()), server_default=JSON_LIST, nullable=False),
        sa.Column("actor_id", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("operation_id", name="uq_wiki_log_entries_operation"),
    )
    op.create_index(
        "ix_wiki_log_entries_scope_cursor",
        "wiki_log_entries",
        ["tenant_id", "knowledge_base_id", "id"],
    )


def downgrade() -> None:
    op.drop_table("wiki_log_entries")
    op.drop_table("wiki_page_issues")
    op.drop_table("wiki_links")
    op.drop_index("ix_wiki_pages_search_fts", table_name="wiki_pages")
    op.drop_index("ix_wiki_pages_title_trgm", table_name="wiki_pages")
    op.drop_table("wiki_pages")
    op.drop_table("wiki_folders")
