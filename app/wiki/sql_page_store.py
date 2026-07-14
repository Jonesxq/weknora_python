"""Wiki 页面 PostgreSQL 仓储。"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import Select, delete, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import load_only

from app.schemas.wiki.pages import WikiPageListQuery
from app.wiki.errors import (
    WikiConflictError,
    WikiNotFoundError,
    WikiVersionConflictError,
)
from app.wiki.models import WikiFolder, WikiLink, WikiLogEntry, WikiPage
from app.wiki.page_service import PageListResult
from app.wiki.scope import WikiScope

_LIST_COLUMNS = (
    WikiPage.id,
    WikiPage.slug,
    WikiPage.title,
    WikiPage.page_type,
    WikiPage.status,
    WikiPage.summary,
    WikiPage.aliases,
    WikiPage.folder_id,
    WikiPage.category_path,
    WikiPage.wiki_path,
    WikiPage.depth,
    WikiPage.sort_order,
    WikiPage.version,
    WikiPage.updated_at,
)


def _page_scope(scope: WikiScope):
    return (
        WikiPage.tenant_id == scope.tenant_id,
        WikiPage.knowledge_base_id == scope.knowledge_base_id,
        WikiPage.deleted_at.is_(None),
    )


def build_page_lookup_statement(
    scope: WikiScope, slug: str, *, include_inactive: bool
) -> Select[tuple[WikiPage]]:
    conditions = [*_page_scope(scope), WikiPage.slug == slug]
    if not include_inactive:
        conditions.append(WikiPage.status == "published")
    return select(WikiPage).where(*conditions)


def build_page_update_statement(
    scope: WikiScope,
    slug: str,
    expected_version: int,
    changes: dict[str, object],
    increment_version: bool,
):
    values = {**changes, "updated_at": func.now()}
    if increment_version:
        values["version"] = WikiPage.version + 1
    return (
        update(WikiPage)
        .where(*_page_scope(scope), WikiPage.slug == slug, WikiPage.version == expected_version)
        .values(**values)
        .returning(WikiPage)
    )


def build_page_list_statement(
    scope: WikiScope, query: WikiPageListQuery
) -> Select[tuple[WikiPage]]:
    statement = select(WikiPage).options(load_only(*_LIST_COLUMNS)).where(*_page_scope(scope))
    statement = statement.where(
        WikiPage.status == (query.status.value if query.status else "published")
    )
    if query.page_types:
        statement = statement.where(WikiPage.page_type.in_(query.page_types))
    if query.query:
        pattern = f"%{query.query.strip()}%"
        statement = statement.where(
            or_(
                WikiPage.title.ilike(pattern),
                WikiPage.slug.ilike(pattern),
                WikiPage.summary.ilike(pattern),
            )
        )
    if query.folder_id == "":
        statement = statement.where(WikiPage.folder_id.is_(None))
    elif query.folder_id is not None:
        statement = statement.where(WikiPage.folder_id == query.folder_id)
    if query.category_path:
        path = [part.strip() for part in query.category_path.split("/") if part.strip()]
        statement = statement.where(WikiPage.category_path.contains(path))
    if query.category_depth is not None:
        statement = statement.where(WikiPage.depth == query.category_depth)

    order_column = getattr(WikiPage, query.sort_by)
    order = order_column.desc() if query.sort_order == "desc" else order_column.asc()
    return statement.order_by(order, WikiPage.slug.asc()).offset(query.offset).limit(query.page_size)


class SqlAlchemyPageStore:
    """使用同一 AsyncSession 原子维护页面、链接和日志。"""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def find_page(
        self, scope: WikiScope, slug: str, *, include_inactive: bool = False
    ) -> WikiPage | None:
        result = await self._session.execute(
            build_page_lookup_statement(scope, slug, include_inactive=include_inactive)
        )
        return result.scalar_one_or_none()

    async def get_folder_path(self, scope: WikiScope, folder_id: UUID | None) -> list[str]:
        if folder_id is None:
            return []
        result = await self._session.execute(
            select(WikiFolder.path).where(
                WikiFolder.id == folder_id,
                WikiFolder.tenant_id == scope.tenant_id,
                WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                WikiFolder.deleted_at.is_(None),
            )
        )
        path = result.scalar_one_or_none()
        if path is None:
            raise WikiNotFoundError("FOLDER_NOT_FOUND", "Wiki 目录不存在")
        return [part for part in path.split("/") if part]

    async def insert_page(
        self, scope: WikiScope, page: WikiPage, targets: list[str]
    ) -> WikiPage:
        self._session.add(page)
        try:
            await self._session.flush()
        except IntegrityError as exc:
            await self._session.rollback()
            raise WikiConflictError("SLUG_CONFLICT", f"页面 slug {page.slug!r} 已存在") from exc
        await self._replace_links(scope, page, targets)
        await self._backfill_target(scope, page)
        self._append_log(scope, "page_created", page, f"创建页面 {page.title}")
        await self._session.flush()
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
        result = await self._session.execute(
            build_page_update_statement(
                scope, slug, expected_version, changes, increment_version
            )
        )
        page = result.scalar_one_or_none()
        if page is None:
            raise WikiVersionConflictError()
        if targets is not None:
            await self._replace_links(scope, page, targets)
        self._append_log(scope, "page_updated", page, f"更新页面 {page.title}")
        await self._session.flush()
        return page

    async def soft_delete_page(self, scope: WikiScope, slug: str) -> WikiPage:
        result = await self._session.execute(
            update(WikiPage)
            .where(*_page_scope(scope), WikiPage.slug == slug)
            .values(deleted_at=datetime.now(UTC), updated_at=func.now())
            .returning(WikiPage)
        )
        page = result.scalar_one_or_none()
        if page is None:
            raise WikiNotFoundError("PAGE_NOT_FOUND", "Wiki 页面不存在")
        await self._session.execute(delete(WikiLink).where(WikiLink.source_page_id == page.id))
        await self._session.execute(
            update(WikiLink)
            .where(
                WikiLink.knowledge_base_id == scope.knowledge_base_id,
                WikiLink.target_page_id == page.id,
            )
            .values(target_page_id=None)
        )
        self._append_log(scope, "page_deleted", page, f"删除页面 {page.title}")
        await self._session.flush()
        return page

    async def get_links(
        self, scope: WikiScope, page: WikiPage
    ) -> tuple[list[str], list[str]]:
        outgoing_result = await self._session.execute(
            select(WikiLink.target_slug)
            .where(
                WikiLink.knowledge_base_id == scope.knowledge_base_id,
                WikiLink.source_page_id == page.id,
            )
            .order_by(WikiLink.id)
        )
        incoming_result = await self._session.execute(
            select(WikiPage.slug)
            .join(WikiLink, WikiLink.source_page_id == WikiPage.id)
            .where(
                *_page_scope(scope),
                WikiPage.status == "published",
                WikiLink.target_slug == page.slug,
            )
            .order_by(WikiPage.slug)
        )
        return list(incoming_result.scalars()), list(outgoing_result.scalars())

    async def list_pages(self, scope: WikiScope, query: WikiPageListQuery) -> PageListResult:
        statement = build_page_list_statement(scope, query)
        count_statement = select(func.count()).select_from(statement.order_by(None).limit(None).offset(None).subquery())
        total = int((await self._session.execute(count_statement)).scalar_one())
        pages = list((await self._session.execute(statement)).scalars())
        return PageListResult(pages=pages, total=total)

    async def replace_page_links(
        self, scope: WikiScope, page: WikiPage, targets: list[str]
    ) -> None:
        """重建单页链接投影，不改变页面内容版本。"""

        await self._replace_links(scope, page, targets)
        await self._session.flush()

    async def _replace_links(
        self, scope: WikiScope, page: WikiPage, targets: list[str]
    ) -> None:
        await self._session.execute(delete(WikiLink).where(WikiLink.source_page_id == page.id))
        if not targets:
            return
        target_rows = await self._session.execute(
            select(WikiPage.slug, WikiPage.id).where(
                *_page_scope(scope), WikiPage.slug.in_(targets)
            )
        )
        target_ids = dict(target_rows.all())
        self._session.add_all(
            WikiLink(
                knowledge_base_id=scope.knowledge_base_id,
                source_page_id=page.id,
                target_slug=target,
                target_page_id=target_ids.get(target),
            )
            for target in targets
        )

    async def _backfill_target(self, scope: WikiScope, page: WikiPage) -> None:
        await self._session.execute(
            update(WikiLink)
            .where(
                WikiLink.knowledge_base_id == scope.knowledge_base_id,
                WikiLink.target_slug == page.slug,
                WikiLink.target_page_id.is_(None),
            )
            .values(target_page_id=page.id)
        )

    def _append_log(
        self, scope: WikiScope, action: str, page: WikiPage, message: str
    ) -> None:
        self._session.add(
            WikiLogEntry(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                operation_id=uuid4(),
                action=action,
                message=message,
                pages_affected=[{"slug": page.slug, "title": page.title}],
                actor_id=scope.actor_id,
            )
        )
