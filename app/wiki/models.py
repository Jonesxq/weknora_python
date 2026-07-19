"""Wiki PostgreSQL 持久化模型。"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.base import Base


def _uuid() -> uuid.UUID:
    return uuid.uuid4()


class WikiFolder(Base):
    """知识库中的邻接表目录节点。"""

    __tablename__ = "wiki_folders"
    __table_args__ = (
        Index(
            "uq_wiki_folders_active_sibling",
            "knowledge_base_id",
            "parent_id",
            "name",
            unique=True,
            postgresql_where=text("deleted_at IS NULL"),
            postgresql_nulls_not_distinct=True,
        ),
        Index("ix_wiki_folders_scope_path", "tenant_id", "knowledge_base_id", "path"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    tenant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    knowledge_base_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("wiki_folders.id", ondelete="RESTRICT"), nullable=True
    )
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    path: Mapped[str] = mapped_column(String(2048), nullable=False)
    depth: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class WikiPage(Base):
    """Wiki 页面，正文是链接关系的唯一文本真源。"""

    __tablename__ = "wiki_pages"
    __table_args__ = (
        Index(
            "uq_wiki_pages_active_slug",
            "knowledge_base_id",
            "slug",
            unique=True,
            postgresql_where=text("deleted_at IS NULL"),
        ),
        Index(
            "ix_wiki_pages_scope_type_status",
            "tenant_id",
            "knowledge_base_id",
            "page_type",
            "status",
        ),
        Index(
            "ix_wiki_pages_folder_order",
            "knowledge_base_id",
            "folder_id",
            "wiki_path",
            "sort_order",
            "title",
        ),
        Index(
            "ix_wiki_pages_source_refs_gin",
            "source_refs",
            postgresql_using="gin",
            postgresql_ops={"source_refs": "jsonb_path_ops"},
        ),
        Index(
            "ix_wiki_pages_dedup_names_trgm",
            text("(lower(title) || ' ' || lower(coalesce(aliases::text, ''))) gist_trgm_ops"),
            postgresql_using="gist",
            postgresql_where=text(
                "deleted_at IS NULL AND status = 'published' AND page_type IN ('entity', 'concept')"
            ),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    tenant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    knowledge_base_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    page_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="draft")
    content: Mapped[str] = mapped_column(Text, nullable=False, default="")
    summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    aliases: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    parent_slug: Mapped[str | None] = mapped_column(String(255), nullable=True)
    folder_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("wiki_folders.id", ondelete="RESTRICT"), nullable=True
    )
    category_path: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    wiki_path: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    depth: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source_refs: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    chunk_refs: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    page_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


Index(
    "ix_wiki_pages_title_trgm",
    func.lower(WikiPage.title).label("title_lower"),
    postgresql_using="gin",
    postgresql_ops={"title_lower": "gin_trgm_ops"},
    postgresql_where=WikiPage.deleted_at.is_(None),
)
Index(
    "ix_wiki_pages_search_fts",
    func.to_tsvector(
        text("'simple'"),
        WikiPage.title + " " + WikiPage.summary + " " + WikiPage.content,
    ),
    postgresql_using="gin",
    postgresql_where=WikiPage.deleted_at.is_(None),
)


class WikiLink(Base):
    """从页面正文解析得到的结构化链接投影。"""

    __tablename__ = "wiki_links"
    __table_args__ = (
        UniqueConstraint("source_page_id", "target_slug", name="uq_wiki_links_source_target"),
        Index("ix_wiki_links_scope_target", "tenant_id", "knowledge_base_id", "target_slug"),
        Index("ix_wiki_links_target_page", "target_page_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    tenant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    knowledge_base_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_page_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("wiki_pages.id", ondelete="CASCADE"), nullable=False
    )
    target_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    target_page_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("wiki_pages.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class WikiPageIssue(Base):
    """Wiki 页面质量问题单。"""

    __tablename__ = "wiki_page_issues"
    __table_args__ = (
        Index(
            "ix_wiki_page_issues_scope_status",
            "tenant_id",
            "knowledge_base_id",
            "status",
            "created_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    tenant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    knowledge_base_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    page_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    issue_type: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    suspected_knowledge_ids: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    reported_by: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class WikiLogEntry(Base):
    """按稳定自增游标读取的追加式操作日志。"""

    __tablename__ = "wiki_log_entries"
    __table_args__ = (
        UniqueConstraint("operation_id", name="uq_wiki_log_entries_operation"),
        Index("ix_wiki_log_entries_scope_cursor", "tenant_id", "knowledge_base_id", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    tenant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    knowledge_base_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    operation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, default=_uuid)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    pages_affected: Mapped[list[dict[str, str]]] = mapped_column(JSONB, nullable=False, default=list)
    result_outcome: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    actor_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class WikiPendingOp(Base):
    """等待进入异步摄取队列的 Wiki 操作。"""

    __tablename__ = "wiki_pending_ops"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "knowledge_id",
            "op",
            "op_version",
            name="uq_wiki_pending_ops_version",
        ),
        Index(
            "ix_wiki_pending_ops_scope_claim",
            "tenant_id",
            "knowledge_base_id",
            "claimed_at",
            "enqueued_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    tenant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    knowledge_base_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    knowledge_id: Mapped[str] = mapped_column(String(255), nullable=False)
    op: Mapped[str] = mapped_column(String(32), nullable=False, default="ingest")
    op_version: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    fail_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    enqueued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    claim_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class WikiPageContribution(Base):
    """按知识来源记录页面内容贡献及其当前状态。"""

    __tablename__ = "wiki_page_contributions"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "slug",
            "knowledge_id",
            "op_version",
            name="uq_wiki_page_contributions_version",
        ),
        Index(
            "uq_wiki_page_contributions_active_source",
            "tenant_id",
            "knowledge_base_id",
            "slug",
            "knowledge_id",
            unique=True,
            postgresql_where=text("state = 'active'"),
        ),
        Index(
            "ix_wiki_page_contributions_slug_state",
            "tenant_id",
            "knowledge_base_id",
            "slug",
            "state",
        ),
        Index(
            "ix_wiki_page_contributions_source_state",
            "tenant_id",
            "knowledge_base_id",
            "knowledge_id",
            "state",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    tenant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    knowledge_base_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False)
    knowledge_id: Mapped[str] = mapped_column(String(255), nullable=False)
    op_version: Mapped[str] = mapped_column(String(255), nullable=False)
    page_type: Mapped[str] = mapped_column(String(32), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default=text("''"))
    summary: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default=text("''"))
    aliases: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    chunk_refs: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class WikiDeadLetter(Base):
    """达到重试上限后保留的 Wiki 异步操作。"""

    __tablename__ = "wiki_dead_letters"
    __table_args__ = (
        UniqueConstraint("pending_op_id", name="uq_wiki_dead_letters_pending_op"),
        Index(
            "ix_wiki_dead_letters_scope_dead_at",
            "tenant_id",
            "knowledge_base_id",
            "dead_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    pending_op_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    tenant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    knowledge_base_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    knowledge_id: Mapped[str] = mapped_column(String(255), nullable=False)
    op: Mapped[str] = mapped_column(String(32), nullable=False)
    op_version: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    fail_count: Mapped[int] = mapped_column(Integer, nullable=False)
    last_error_code: Mapped[str] = mapped_column(String(128), nullable=False)
    last_error_summary: Mapped[str] = mapped_column(String(2000), nullable=False)
    dead_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class WikiFinalizationMarker(Base):
    """Wiki 摄取尝试的收尾子任务登记标记。"""

    __tablename__ = "wiki_finalization_markers"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "knowledge_id",
            "attempt",
            "subtask_name",
            name="uq_wiki_finalization_markers_attempt",
        ),
        Index(
            "ix_wiki_finalization_markers_scope",
            "tenant_id",
            "knowledge_base_id",
            "released_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    tenant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    knowledge_base_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    knowledge_id: Mapped[str] = mapped_column(String(255), nullable=False)
    attempt: Mapped[str] = mapped_column(String(255), nullable=False)
    subtask_name: Mapped[str] = mapped_column(String(64), nullable=False, default="wiki")
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TaskOutbox(Base):
    """等待可靠投递的异步任务事件。"""

    __tablename__ = "task_outbox"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "event_type",
            "dedup_key",
            name="uq_task_outbox_scope_event_dedup",
        ),
        Index("ix_task_outbox_delivery", "sent_at", "available_at", "claimed_at"),
        Index("ix_task_outbox_scope", "tenant_id", "knowledge_base_id", "sent_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_uuid)
    tenant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    knowledge_base_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    dedup_key: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    claim_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
