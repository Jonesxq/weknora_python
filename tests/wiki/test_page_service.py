from __future__ import annotations

from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from app.schemas.wiki.pages import (
    WikiPageCreateRequest,
    WikiPageListQuery,
    WikiPageMoveRequest,
    WikiPageUpdateRequest,
)
from app.wiki.errors import WikiConflictError, WikiNotFoundError, WikiPermissionError, WikiVersionConflictError
from app.wiki.errors import WikiValidationError
from app.wiki.models import WikiPage
from app.wiki.page_service import PageListResult, WikiPageService
from app.wiki.scope import WikiScope


class MemoryPageStore:
    def __init__(self) -> None:
        self.pages: dict[tuple[int, UUID, str], WikiPage] = {}
        self.links: dict[UUID, list[str]] = {}
        self.folder_paths: dict[UUID, list[str]] = {}
        self.logs: list[tuple[str, str]] = []

    @staticmethod
    def _key(scope: WikiScope, slug: str) -> tuple[int, UUID, str]:
        return scope.tenant_id, scope.knowledge_base_id, slug

    async def find_page(self, scope: WikiScope, slug: str, *, include_inactive: bool = False) -> WikiPage | None:
        page = self.pages.get(self._key(scope, slug))
        if page is None or page.deleted_at is not None:
            return None
        if not include_inactive and page.status in {"draft", "archived"}:
            return None
        return page

    async def get_folder_path(self, scope: WikiScope, folder_id: UUID | None) -> list[str]:
        if folder_id is None:
            return []
        if folder_id not in self.folder_paths:
            raise WikiNotFoundError("FOLDER_NOT_FOUND", "目录不存在")
        return self.folder_paths[folder_id]

    async def insert_page(self, scope: WikiScope, page: WikiPage, targets: list[str]) -> WikiPage:
        key = self._key(scope, page.slug)
        if key in self.pages:
            raise WikiConflictError("SLUG_CONFLICT", "slug 已存在")
        self.pages[key] = page
        self.links[page.id] = targets
        self.logs.append(("page_created", page.slug))
        return page

    async def update_page(
        self,
        scope: WikiScope,
        slug: str,
        expected_version: int,
        changes: dict[str, object],
        targets: list[str] | None,
        increment_version: bool,
    ) -> WikiPage:
        page = self.pages[self._key(scope, slug)]
        if page.version != expected_version:
            raise WikiVersionConflictError()
        for name, value in changes.items():
            setattr(page, name, value)
        if increment_version:
            page.version += 1
        if targets is not None:
            self.links[page.id] = targets
        self.logs.append(("page_updated", page.slug))
        return page

    async def soft_delete_page(self, scope: WikiScope, slug: str) -> WikiPage:
        page = self.pages.get(self._key(scope, slug))
        if page is None:
            raise WikiNotFoundError("PAGE_NOT_FOUND", "页面不存在")
        self.pages.pop(self._key(scope, slug))
        self.links.pop(page.id, None)
        self.logs.append(("page_deleted", page.slug))
        return page

    async def get_links(self, scope: WikiScope, page: WikiPage) -> tuple[list[str], list[str]]:
        outgoing = self.links.get(page.id, [])
        incoming = [
            source.slug
            for source in self.pages.values()
            if page.slug in self.links.get(source.id, [])
            and source.tenant_id == scope.tenant_id
            and source.knowledge_base_id == scope.knowledge_base_id
        ]
        return incoming, outgoing

    async def list_pages(self, scope: WikiScope, query: WikiPageListQuery) -> PageListResult:
        pages = [
            page
            for (tenant_id, kb_id, _), page in self.pages.items()
            if tenant_id == scope.tenant_id
            and kb_id == scope.knowledge_base_id
            and page.deleted_at is None
            and page.status == "published"
        ]
        pages.sort(key=lambda page: page.slug)
        return PageListResult(pages=pages[query.offset : query.offset + query.page_size], total=len(pages))


@pytest.fixture
def scope() -> WikiScope:
    return WikiScope(tenant_id=7, knowledge_base_id=uuid4(), actor_id="user-1", can_write=True)


@pytest.mark.asyncio
async def test_create_page_derives_directory_cache_and_link_projection(scope: WikiScope) -> None:
    store = MemoryPageStore()
    folder_id = uuid4()
    store.folder_paths[folder_id] = ["技术", "架构"]
    service = WikiPageService(store)

    response = await service.create_page(
        scope,
        WikiPageCreateRequest(
            slug=" Entity/Acme Corp ",
            title="Acme",
            page_type="entity",
            content="关联 [[concept/Knowledge Graph]]",
            folder_id=folder_id,
        ),
    )

    assert response.slug == "entity/acme-corp"
    assert response.category_path == ["技术", "架构"]
    assert response.wiki_path == "/技术/架构/entity/acme-corp"
    assert response.out_links == ["concept/knowledge-graph"]
    assert response.version == 1
    assert store.logs == [("page_created", "entity/acme-corp")]


@pytest.mark.asyncio
async def test_create_page_rejects_read_only_scope(scope: WikiScope) -> None:
    service = WikiPageService(MemoryPageStore())

    with pytest.raises(WikiPermissionError):
        await service.create_page(
            replace(scope, can_write=False),
            WikiPageCreateRequest(slug="entity/acme", title="Acme", page_type="entity"),
        )


@pytest.mark.asyncio
async def test_update_page_uses_optimistic_version_and_explicit_clear(scope: WikiScope) -> None:
    store = MemoryPageStore()
    service = WikiPageService(store)
    created = await service.create_page(
        scope,
        WikiPageCreateRequest(
            slug="entity/acme",
            title="Acme",
            page_type="entity",
            content="旧内容",
            summary="旧摘要",
        ),
    )

    updated = await service.update_page(
        scope,
        created.slug,
        WikiPageUpdateRequest.model_validate({"version": 1, "summary": None, "content": "[[concept/new]]"}),
    )

    assert updated.summary == ""
    assert updated.content == "[[concept/new]]"
    assert updated.out_links == ["concept/new"]
    assert updated.version == 2

    with pytest.raises(WikiVersionConflictError):
        await service.update_page(
            scope,
            created.slug,
            WikiPageUpdateRequest(version=1, title="并发旧写入"),
        )


@pytest.mark.asyncio
async def test_update_sort_order_does_not_increment_visible_version(scope: WikiScope) -> None:
    service = WikiPageService(store := MemoryPageStore())
    created = await service.create_page(
        scope,
        WikiPageCreateRequest(slug="entity/acme", title="Acme", page_type="entity"),
    )

    updated = await service.update_page(
        scope,
        created.slug,
        WikiPageUpdateRequest(version=1, sort_order=20),
    )

    assert updated.sort_order == 20
    assert updated.version == 1
    assert store.logs[-1] == ("page_updated", "entity/acme")


@pytest.mark.asyncio
async def test_get_page_is_scoped_by_tenant_and_kb(scope: WikiScope) -> None:
    store = MemoryPageStore()
    service = WikiPageService(store)
    await service.create_page(
        scope,
        WikiPageCreateRequest(slug="entity/acme", title="Acme", page_type="entity"),
    )

    with pytest.raises(WikiNotFoundError):
        await service.get_page(replace(scope, tenant_id=8), "entity/acme")


@pytest.mark.asyncio
async def test_soft_delete_allows_slug_to_be_reused(scope: WikiScope) -> None:
    store = MemoryPageStore()
    service = WikiPageService(store)
    request = WikiPageCreateRequest(slug="entity/acme", title="Acme", page_type="entity")
    await service.create_page(scope, request)

    await service.delete_page(scope, "entity/acme")
    recreated = await service.create_page(scope, request)

    assert recreated.slug == "entity/acme"
    assert store.logs[-2:] == [("page_deleted", "entity/acme"), ("page_created", "entity/acme")]


@pytest.mark.asyncio
async def test_move_page_derives_cache_without_incrementing_version(scope: WikiScope) -> None:
    store = MemoryPageStore()
    folder_id = uuid4()
    store.folder_paths[folder_id] = ["产品", "后端"]
    service = WikiPageService(store)
    created = await service.create_page(
        scope,
        WikiPageCreateRequest(slug="entity/acme", title="Acme", page_type="entity"),
    )

    moved = await service.move_page(
        scope,
        WikiPageMoveRequest(slug=created.slug, folder_id=folder_id),
    )

    assert moved.folder_id == folder_id
    assert moved.category_path == ["产品", "后端"]
    assert moved.wiki_path == "/产品/后端/entity/acme"
    assert moved.version == 1


@pytest.mark.asyncio
async def test_invalid_slug_is_mapped_to_domain_validation_error(scope: WikiScope) -> None:
    service = WikiPageService(MemoryPageStore())

    with pytest.raises(WikiValidationError, match="slug"):
        await service.create_page(
            scope,
            WikiPageCreateRequest(slug="../bad", title="Bad", page_type="entity"),
        )
    with pytest.raises(WikiValidationError, match="slug"):
        await service.get_page(scope, "../bad")


@pytest.mark.asyncio
async def test_create_and_update_reject_blank_title(scope: WikiScope) -> None:
    service = WikiPageService(MemoryPageStore())

    with pytest.raises(WikiValidationError, match="标题"):
        await service.create_page(
            scope,
            WikiPageCreateRequest(slug="entity/acme", title="   ", page_type="entity"),
        )

    created = await service.create_page(
        scope,
        WikiPageCreateRequest(slug="entity/good", title="Good", page_type="entity"),
    )
    with pytest.raises(WikiValidationError, match="标题"):
        await service.update_page(
            scope,
            created.slug,
            WikiPageUpdateRequest(version=1, title="   "),
        )


@pytest.mark.asyncio
async def test_missing_update_version_records_compatibility_warning(
    scope: WikiScope, caplog: pytest.LogCaptureFixture
) -> None:
    service = WikiPageService(MemoryPageStore())
    created = await service.create_page(
        scope,
        WikiPageCreateRequest(slug="entity/acme", title="Acme", page_type="entity"),
    )

    await service.update_page(
        scope,
        created.slug,
        WikiPageUpdateRequest(summary="兼容旧客户端"),
    )

    assert "未提交 version" in caplog.text
