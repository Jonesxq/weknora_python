"""Wiki 页面请求与响应 DTO。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, computed_field, field_validator

from app.wiki.enums import WikiPageStatus, WikiPageType


class WikiPageCreateRequest(BaseModel):
    """人工创建 Wiki 页面的输入，服务端字段会被忽略。"""

    model_config = ConfigDict(extra="ignore")

    slug: str = Field(min_length=1, max_length=255)
    title: str = Field(min_length=1, max_length=512)
    page_type: WikiPageType
    status: WikiPageStatus = WikiPageStatus.PUBLISHED
    content: str = ""
    summary: str = ""
    aliases: list[str] = Field(default_factory=list)
    parent_slug: str | None = Field(default=None, max_length=255)
    folder_id: UUID | None = None
    sort_order: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class WikiPageUpdateRequest(BaseModel):
    """页面部分更新；通过 model_fields_set 区分缺失字段和显式空值。"""

    model_config = ConfigDict(extra="ignore")

    version: int | None = Field(default=None, ge=1)
    title: str | None = Field(default=None, max_length=512)
    page_type: WikiPageType | None = None
    status: WikiPageStatus | None = None
    content: str | None = None
    summary: str | None = None
    aliases: list[str] | None = None
    parent_slug: str | None = Field(default=None, max_length=255)
    sort_order: int | None = None
    metadata: dict[str, Any] | None = None


class WikiPageResponse(BaseModel):
    """兼容前端的页面响应投影。"""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: UUID
    tenant_id: int | None = None
    knowledge_base_id: UUID | None = None
    slug: str
    title: str
    page_type: WikiPageType
    status: WikiPageStatus
    content: str
    summary: str
    aliases: list[str] = Field(default_factory=list)
    parent_slug: str | None = None
    folder_id: UUID | None = None
    category_path: list[str] = Field(default_factory=list)
    wiki_path: str = ""
    depth: int = 0
    sort_order: int = 0
    source_refs: list[str] = Field(default_factory=list)
    chunk_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("page_metadata", "metadata"),
    )
    version: int
    in_links: list[str] = Field(default_factory=list)
    out_links: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class WikiPageListQuery(BaseModel):
    """页面列表筛选与 offset 分页参数。"""

    page_type: str | None = None
    status: WikiPageStatus | None = None
    query: str | None = Field(default=None, max_length=512)
    folder_id: UUID | Literal[""] | None = None
    category_path: str | None = Field(default=None, max_length=2048)
    category_depth: int | None = Field(default=None, ge=0, le=3)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=200)
    sort_by: Literal["title", "created_at", "updated_at", "wiki_path", "sort_order"] = "wiki_path"
    sort_order: Literal["asc", "desc"] = "asc"

    @field_validator("page_type")
    @classmethod
    def validate_page_types(cls, value: str | None) -> str | None:
        if value is None:
            return value
        values = [item.strip() for item in value.split(",") if item.strip()]
        allowed = {item.value for item in WikiPageType}
        invalid = set(values) - allowed
        if invalid:
            raise ValueError(f"不支持的页面类型: {', '.join(sorted(invalid))}")
        return ",".join(values)

    @computed_field
    @property
    def page_types(self) -> list[str]:
        return self.page_type.split(",") if self.page_type else []

    @computed_field
    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size


class WikiPageListItem(BaseModel):
    """页面列表使用的窄列投影，不携带正文和大 JSON 字段。"""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    slug: str
    title: str
    page_type: WikiPageType
    status: WikiPageStatus
    summary: str
    aliases: list[str] = Field(default_factory=list)
    folder_id: UUID | None = None
    category_path: list[str] = Field(default_factory=list)
    wiki_path: str = ""
    depth: int = 0
    sort_order: int = 0
    version: int
    updated_at: datetime | None = None


class WikiPageListResponse(BaseModel):
    pages: list[WikiPageListItem] = Field(default_factory=list)
    total: int
    page: int
    page_size: int
    total_pages: int


class WikiPageMoveRequest(BaseModel):
    slug: str = Field(min_length=1, max_length=255)
    folder_id: UUID | None = None
