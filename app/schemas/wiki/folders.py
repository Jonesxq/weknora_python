"""Wiki 目录请求与响应 DTO。"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class WikiFolderCreateRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str = Field(min_length=1, max_length=512)
    parent_id: UUID | None = None
    sort_order: int = 0


class WikiFolderUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str | None = Field(default=None, max_length=512)
    parent_id: UUID | None = None
    sort_order: int | None = None


class WikiFolderResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    parent_id: UUID | None = None
    path: str
    depth: int
    sort_order: int
    created_at: datetime | None = None
    updated_at: datetime | None = None


class WikiFolderListResponse(BaseModel):
    folders: list[WikiFolderResponse] = Field(default_factory=list)
