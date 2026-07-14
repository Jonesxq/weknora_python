"""Wiki 页面应用服务。"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from app.schemas.wiki.pages import (
    WikiPageCreateRequest,
    WikiPageListItem,
    WikiPageListQuery,
    WikiPageListResponse,
    WikiPageMoveRequest,
    WikiPageResponse,
    WikiPageUpdateRequest,
)
from app.wiki.domain import extract_wiki_links, normalize_category_path, normalize_slug
from app.wiki.errors import WikiConflictError, WikiNotFoundError, WikiPermissionError, WikiValidationError
from app.wiki.models import WikiPage
from app.wiki.scope import WikiScope


@dataclass(slots=True)
class PageListResult:
    pages: list[WikiPage]
    total: int


class PageStore(Protocol):
    async def find_page(
        self, scope: WikiScope, slug: str, *, include_inactive: bool = False
    ) -> WikiPage | None: ...

    async def get_folder_path(self, scope: WikiScope, folder_id: UUID | None) -> list[str]: ...

    async def insert_page(
        self, scope: WikiScope, page: WikiPage, targets: list[str]
    ) -> WikiPage: ...

    async def update_page(
        self,
        scope: WikiScope,
        slug: str,
        expected_version: int,
        changes: dict[str, object],
        targets: list[str] | None,
        increment_version: bool,
    ) -> WikiPage: ...

    async def soft_delete_page(self, scope: WikiScope, slug: str) -> WikiPage: ...

    async def get_links(
        self, scope: WikiScope, page: WikiPage
    ) -> tuple[list[str], list[str]]: ...

    async def list_pages(self, scope: WikiScope, query: WikiPageListQuery) -> PageListResult: ...


class WikiPageService:
    """实现页面不变量，并把原子持久化委托给 PageStore。"""

    _VISIBLE_FIELDS = {"title", "content", "summary", "page_type", "status"}

    def __init__(self, store: PageStore) -> None:
        self._store = store

    @staticmethod
    def _require_write(scope: WikiScope) -> None:
        if not scope.can_write:
            raise WikiPermissionError()

    async def create_page(
        self, scope: WikiScope, request: WikiPageCreateRequest
    ) -> WikiPageResponse:
        self._require_write(scope)
        slug = normalize_slug(request.slug)
        if await self._store.find_page(scope, slug, include_inactive=True) is not None:
            raise WikiConflictError("SLUG_CONFLICT", f"页面 slug {slug!r} 已存在")

        category_path = normalize_category_path(
            await self._store.get_folder_path(scope, request.folder_id)
        )
        now = datetime.now(UTC)
        page = WikiPage(
            id=uuid4(),
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug=slug,
            title=request.title.strip(),
            page_type=request.page_type.value,
            status=request.status.value,
            content=request.content,
            summary=request.summary,
            aliases=list(dict.fromkeys(alias.strip() for alias in request.aliases if alias.strip())),
            parent_slug=normalize_slug(request.parent_slug) if request.parent_slug else None,
            folder_id=request.folder_id,
            category_path=category_path,
            wiki_path=self._wiki_path(category_path, slug),
            depth=len(category_path),
            sort_order=request.sort_order,
            source_refs=[],
            chunk_refs=[],
            page_metadata=dict(request.metadata),
            version=1,
            created_at=now,
            updated_at=now,
            deleted_at=None,
        )
        page = await self._store.insert_page(scope, page, extract_wiki_links(page.content))
        return await self._response(scope, page)

    async def get_page(self, scope: WikiScope, slug: str) -> WikiPageResponse:
        page = await self._store.find_page(scope, normalize_slug(slug))
        if page is None:
            raise WikiNotFoundError("PAGE_NOT_FOUND", "Wiki 页面不存在")
        return await self._response(scope, page)

    async def update_page(
        self,
        scope: WikiScope,
        slug: str,
        request: WikiPageUpdateRequest,
    ) -> WikiPageResponse:
        self._require_write(scope)
        normalized_slug = normalize_slug(slug)
        current = await self._store.find_page(scope, normalized_slug, include_inactive=True)
        if current is None:
            raise WikiNotFoundError("PAGE_NOT_FOUND", "Wiki 页面不存在")

        changes = self._update_changes(current, request)
        increment_version = any(
            name in self._VISIBLE_FIELDS and getattr(current, name) != value
            for name, value in changes.items()
        )
        targets = extract_wiki_links(str(changes["content"])) if "content" in changes else None
        updated = await self._store.update_page(
            scope,
            normalized_slug,
            request.version or current.version,
            changes,
            targets,
            increment_version,
        )
        return await self._response(scope, updated)

    async def delete_page(self, scope: WikiScope, slug: str) -> None:
        self._require_write(scope)
        await self._store.soft_delete_page(scope, normalize_slug(slug))

    async def move_page(
        self, scope: WikiScope, request: WikiPageMoveRequest
    ) -> WikiPageResponse:
        self._require_write(scope)
        slug = normalize_slug(request.slug)
        current = await self._store.find_page(scope, slug, include_inactive=True)
        if current is None:
            raise WikiNotFoundError("PAGE_NOT_FOUND", "Wiki 页面不存在")
        category_path = normalize_category_path(
            await self._store.get_folder_path(scope, request.folder_id)
        )
        moved = await self._store.update_page(
            scope,
            slug,
            current.version,
            {
                "folder_id": request.folder_id,
                "category_path": category_path,
                "wiki_path": self._wiki_path(category_path, slug),
                "depth": len(category_path),
            },
            None,
            False,
        )
        return await self._response(scope, moved)

    async def list_pages(
        self, scope: WikiScope, query: WikiPageListQuery
    ) -> WikiPageListResponse:
        result = await self._store.list_pages(scope, query)
        pages = [WikiPageListItem.model_validate(page) for page in result.pages]
        return WikiPageListResponse(
            pages=pages,
            total=result.total,
            page=query.page,
            page_size=query.page_size,
            total_pages=math.ceil(result.total / query.page_size) if result.total else 0,
        )

    @staticmethod
    def _wiki_path(category_path: list[str], slug: str) -> str:
        return "/" + "/".join([*category_path, slug])

    @staticmethod
    def _update_changes(
        current: WikiPage, request: WikiPageUpdateRequest
    ) -> dict[str, object]:
        values: dict[str, object] = {}
        for field in request.model_fields_set - {"version"}:
            value = getattr(request, field)
            if field in {"title", "page_type", "status"} and value is None:
                raise WikiValidationError("INVALID_PAGE_UPDATE", f"字段 {field} 不能清空")
            if field in {"content", "summary"} and value is None:
                value = ""
            elif field == "aliases":
                value = [] if value is None else list(dict.fromkeys(item.strip() for item in value if item.strip()))
            elif field == "metadata":
                field = "page_metadata"
                value = {} if value is None else dict(value)
            elif field == "sort_order" and value is None:
                value = 0
            elif field in {"page_type", "status"}:
                value = value.value
            elif field == "parent_slug" and value:
                value = normalize_slug(value)
            values[field] = value
        return values

    async def _response(self, scope: WikiScope, page: WikiPage) -> WikiPageResponse:
        incoming, outgoing = await self._store.get_links(scope, page)
        data = {
            column.name: getattr(page, column.name)
            for column in WikiPage.__table__.columns
            if column.name != "deleted_at"
        }
        data.update(in_links=incoming, out_links=outgoing)
        return WikiPageResponse.model_validate(data)
