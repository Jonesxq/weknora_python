"""Wiki 索引、图谱、日志、搜索和质量响应 DTO。"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.wiki.enums import WikiIssueStatus, WikiPageType


class WikiIndexItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    slug: str
    title: str
    summary: str
    page_type: WikiPageType
    updated_at: datetime | None = None


class WikiIndexGroup(BaseModel):
    page_type: WikiPageType
    items: list[WikiIndexItem] = Field(default_factory=list)
    next_cursor: str | None = None


class WikiIndexResponse(BaseModel):
    intro: str = ""
    version: int = 0
    groups: list[WikiIndexGroup] = Field(default_factory=list)


class WikiLogEntryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    action: str
    message: str
    pages_affected: list[dict[str, str]] = Field(default_factory=list)
    actor_id: str | None = None
    created_at: datetime


class WikiLogResponse(BaseModel):
    entries: list[WikiLogEntryResponse] = Field(default_factory=list)
    next_cursor: int | None = None


class WikiGraphNode(BaseModel):
    slug: str
    title: str
    page_type: str
    link_count: int = 0


class WikiGraphEdgeResponse(BaseModel):
    source: str
    target: str


class WikiGraphResponse(BaseModel):
    mode: Literal["overview", "ego"]
    center: str | None = None
    nodes: list[WikiGraphNode] = Field(default_factory=list)
    edges: list[WikiGraphEdgeResponse] = Field(default_factory=list)


class WikiStatsResponse(BaseModel):
    page_count: int = 0
    folder_count: int = 0
    link_count: int = 0
    issue_count: int = 0
    pending_tasks: int = 0
    is_active: bool = False


class WikiSearchResult(BaseModel):
    id: UUID
    slug: str
    title: str
    summary: str
    page_type: WikiPageType
    score: float


class WikiSearchResponse(BaseModel):
    results: list[WikiSearchResult] = Field(default_factory=list)
    total: int = 0


class WikiLintItem(BaseModel):
    issue_type: Literal["orphan_page", "broken_link", "empty_content"]
    severity: Literal["info", "warning", "error"]
    page_slug: str
    description: str
    target_slug: str | None = None


class WikiLintResponse(BaseModel):
    health_score: int
    issues: list[WikiLintItem] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


class WikiIssueResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    page_slug: str
    issue_type: str
    description: str
    suspected_knowledge_ids: list[str] = Field(default_factory=list)
    status: WikiIssueStatus
    reported_by: str
    created_at: datetime
    updated_at: datetime


class WikiIssueListResponse(BaseModel):
    issues: list[WikiIssueResponse] = Field(default_factory=list)
    next_cursor: str | None = None


class WikiIssueStatusRequest(BaseModel):
    status: WikiIssueStatus


class WikiRebuildLinksResponse(BaseModel):
    pages_scanned: int
    links_created: int
