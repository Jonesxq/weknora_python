"""Wiki Index、搜索、图谱、日志和质量查询。"""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from sqlalchemy import (
    case,
    exists,
    func,
    literal,
    literal_column,
    or_,
    select,
    union_all,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, load_only

from app.schemas.wiki.queries import (
    WikiGraphEdgeResponse,
    WikiGraphNode,
    WikiGraphResponse,
    WikiIndexGroup,
    WikiIndexItem,
    WikiIndexResponse,
    WikiIssueListResponse,
    WikiIssueResponse,
    WikiLintItem,
    WikiLintResponse,
    WikiLogEntryResponse,
    WikiLogResponse,
    WikiRebuildLinksResponse,
    WikiSearchResponse,
    WikiSearchResult,
    WikiStatsResponse,
)
from app.wiki.domain import calculate_health_score, extract_wiki_links, normalize_slug
from app.wiki.enums import WikiPageType
from app.wiki.errors import WikiNotFoundError, WikiPermissionError, WikiValidationError
from app.wiki.models import (
    WikiFolder,
    WikiLink,
    WikiLogEntry,
    WikiPage,
    WikiPageIssue,
    WikiPendingOp,
)
from app.wiki.scope import WikiScope
from app.wiki.sql_page_store import SqlAlchemyPageStore


class ActivityProbe(Protocol):
    async def is_active(self, knowledge_base_id: UUID) -> bool: ...


class _InactiveActivityProbe:
    async def is_active(self, knowledge_base_id: UUID) -> bool:
        return False


_INACTIVE_ACTIVITY_PROBE = _InactiveActivityProbe()


def build_search_statement(scope: WikiScope, query: str, *, limit: int):
    """构建不解释用户正则的 PostgreSQL 排名搜索语句。"""

    normalized = query.strip().casefold()
    contains_pattern = f"%{normalized}%"
    prefix_pattern = f"{normalized}%"
    regconfig = literal_column("'simple'::regconfig")
    vector = func.to_tsvector(
        regconfig,
        WikiPage.title + literal(" ") + WikiPage.summary + literal(" ") + WikiPage.content,
    )
    ts_query = func.plainto_tsquery(regconfig, query.strip())
    title_similarity = func.similarity(func.lower(WikiPage.title), normalized)
    rank = (
        title_similarity * 4
        + case((func.lower(WikiPage.slug).ilike(prefix_pattern), 3.0), else_=0.0)
        + case((func.lower(WikiPage.slug).ilike(contains_pattern), 1.5), else_=0.0)
        + func.ts_rank_cd(vector, ts_query)
    ).label("rank")
    total_count = func.count().over().label("total_count")
    return (
        select(
            WikiPage.id,
            WikiPage.slug,
            WikiPage.title,
            WikiPage.summary,
            WikiPage.page_type,
            rank,
            total_count,
        )
        .where(
            WikiPage.tenant_id == scope.tenant_id,
            WikiPage.knowledge_base_id == scope.knowledge_base_id,
            WikiPage.deleted_at.is_(None),
            WikiPage.status == "published",
            or_(
                title_similarity > 0.05,
                func.lower(WikiPage.slug).ilike(contains_pattern),
                vector.op("@@")(ts_query),
            ),
        )
        .order_by(rank.desc(), WikiPage.slug.asc())
        .limit(limit)
    )


def build_index_intro_statement(scope: WikiScope):
    return (
        select(
            WikiPage.slug,
            WikiPage.page_type,
            WikiPage.content,
            WikiPage.version,
        )
        .where(
            *_active_page_scope(scope),
            or_(
                WikiPage.slug == "index",
                WikiPage.page_type == WikiPageType.INDEX.value,
            ),
        )
        .limit(2)
    )


def _visible_edges_cte(scope: WikiScope):
    source_page = aliased(WikiPage, name="source_page")
    target_page = aliased(WikiPage, name="target_page")
    return (
        select(WikiLink.source_page_id, WikiLink.target_page_id)
        .select_from(WikiLink)
        .join(source_page, source_page.id == WikiLink.source_page_id)
        .join(target_page, target_page.id == WikiLink.target_page_id)
        .where(
            WikiLink.tenant_id == scope.tenant_id,
            WikiLink.knowledge_base_id == scope.knowledge_base_id,
            WikiLink.target_page_id.is_not(None),
            source_page.tenant_id == scope.tenant_id,
            source_page.knowledge_base_id == scope.knowledge_base_id,
            source_page.deleted_at.is_(None),
            source_page.status == "published",
            target_page.tenant_id == scope.tenant_id,
            target_page.knowledge_base_id == scope.knowledge_base_id,
            target_page.deleted_at.is_(None),
            target_page.status == "published",
        )
        .cte("visible_edges")
    )


def _graph_degree_cte(scope: WikiScope, visible_edges=None):
    if visible_edges is None:
        visible_edges = _visible_edges_cte(scope)
    edge_nodes = union_all(
        select(visible_edges.c.source_page_id.label("page_id")),
        select(visible_edges.c.target_page_id.label("page_id")),
    ).cte("edge_nodes")
    return (
        select(edge_nodes.c.page_id, func.count().label("link_count"))
        .group_by(edge_nodes.c.page_id)
        .cte("degree")
    )


def build_ego_neighbor_statement(
    scope: WikiScope,
    *,
    frontier: set[UUID],
    visited: set[UUID],
    limit: int,
    types: set[str] | None,
):
    """为一层 BFS 返回去重、稳定排序且受预算限制的邻居。"""

    visible_edges = _visible_edges_cte(scope)
    neighbor = aliased(WikiPage, name="neighbor")
    degree = _graph_degree_cte(scope, visible_edges)
    neighbor_id = case(
        (
            visible_edges.c.source_page_id.in_(frontier),
            visible_edges.c.target_page_id,
        ),
        else_=visible_edges.c.source_page_id,
    )
    link_count = func.coalesce(degree.c.link_count, 0).label("link_count")
    statement = (
        select(
            neighbor.id,
            neighbor.slug,
            neighbor.title,
            neighbor.page_type,
            link_count,
        )
        .select_from(visible_edges)
        .join(neighbor, neighbor.id == neighbor_id)
        .outerjoin(degree, degree.c.page_id == neighbor.id)
        .where(
            or_(
                visible_edges.c.source_page_id.in_(frontier),
                visible_edges.c.target_page_id.in_(frontier),
            ),
            neighbor.tenant_id == scope.tenant_id,
            neighbor.knowledge_base_id == scope.knowledge_base_id,
            neighbor.deleted_at.is_(None),
            neighbor.status == "published",
            neighbor.id.not_in(visited),
        )
        .distinct()
        .order_by(link_count.desc(), neighbor.slug.asc())
        .limit(limit)
    )
    if types:
        statement = statement.where(neighbor.page_type.in_(types))
    return statement


def build_broken_link_statement(scope: WikiScope, *, limit: int):
    return (
        select(WikiPage.slug, WikiLink.target_slug)
        .join(WikiLink, WikiLink.source_page_id == WikiPage.id)
        .where(
            *_active_page_scope(scope),
            WikiLink.tenant_id == scope.tenant_id,
            WikiLink.target_page_id.is_(None),
        )
        .order_by(WikiPage.slug, WikiLink.target_slug)
        .limit(limit)
    )


def build_empty_page_statement(scope: WikiScope, *, limit: int):
    return (
        select(WikiPage.slug)
        .where(*_active_page_scope(scope), func.length(func.trim(WikiPage.content)) == 0)
        .order_by(WikiPage.slug)
        .limit(limit)
    )


def build_orphan_page_statement(scope: WikiScope, *, limit: int):
    outgoing = exists(
        select(WikiLink.id).where(
            WikiLink.tenant_id == scope.tenant_id,
            WikiLink.source_page_id == WikiPage.id,
        )
    )
    incoming = exists(
        select(WikiLink.id).where(
            WikiLink.tenant_id == scope.tenant_id,
            WikiLink.target_page_id == WikiPage.id,
        )
    )
    return (
        select(WikiPage.slug)
        .where(*_active_page_scope(scope), ~outgoing, ~incoming)
        .order_by(WikiPage.slug)
        .limit(limit)
    )


class WikiQueryService:
    """面向现有前端的只读查询和确定性维护操作。"""

    GRAPH_HARD_LIMIT = 2000
    GRAPH_EDGE_HARD_LIMIT = 10000
    _INDEX_TYPES = (
        WikiPageType.SUMMARY,
        WikiPageType.ENTITY,
        WikiPageType.CONCEPT,
        WikiPageType.SYNTHESIS,
        WikiPageType.COMPARISON,
    )

    def __init__(
        self,
        session: AsyncSession,
        activity_probe: ActivityProbe | None = None,
    ) -> None:
        self._session = session
        self._activity_probe = activity_probe or _INACTIVE_ACTIVITY_PROBE

    async def get_index(
        self,
        scope: WikiScope,
        *,
        page_type: WikiPageType | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> WikiIndexResponse:
        limit = max(1, min(limit, 200))
        intro_rows = list(
            (await self._session.execute(build_index_intro_statement(scope))).all()
        )
        if len(intro_rows) > 1 or (
            intro_rows
            and (
                intro_rows[0].slug != "index"
                or intro_rows[0].page_type != WikiPageType.INDEX.value
            )
        ):
            raise WikiValidationError(
                "INDEX_IDENTITY_CONFLICT", "canonical Index 身份冲突"
            )
        intro_row = intro_rows[0] if intro_rows else None

        groups: list[WikiIndexGroup] = []
        for selected_type in ((page_type,) if page_type else self._INDEX_TYPES):
            statement = (
                select(WikiPage)
                .options(
                    load_only(
                        WikiPage.id,
                        WikiPage.slug,
                        WikiPage.title,
                        WikiPage.summary,
                        WikiPage.page_type,
                        WikiPage.updated_at,
                    )
                )
                .where(
                    *_active_page_scope(scope),
                    WikiPage.page_type == selected_type.value,
                )
                .order_by(WikiPage.slug)
                .limit(limit + 1)
            )
            if cursor:
                statement = statement.where(WikiPage.slug > cursor)
            rows = list((await self._session.execute(statement)).scalars())
            has_more = len(rows) > limit
            visible = rows[:limit]
            groups.append(
                WikiIndexGroup(
                    page_type=selected_type,
                    items=[WikiIndexItem.model_validate(item) for item in visible],
                    next_cursor=visible[-1].slug if has_more and visible else None,
                )
            )
        return WikiIndexResponse(
            intro=intro_row.content if intro_row else "",
            version=intro_row.version if intro_row else 0,
            groups=groups,
        )

    async def get_log(
        self, scope: WikiScope, *, cursor: int | None = None, limit: int = 50
    ) -> WikiLogResponse:
        limit = max(1, min(limit, 200))
        statement = (
            select(WikiLogEntry)
            .where(
                WikiLogEntry.tenant_id == scope.tenant_id,
                WikiLogEntry.knowledge_base_id == scope.knowledge_base_id,
            )
            .order_by(WikiLogEntry.id.desc())
            .limit(limit + 1)
        )
        if cursor is not None:
            statement = statement.where(WikiLogEntry.id < cursor)
        rows = list((await self._session.execute(statement)).scalars())
        has_more = len(rows) > limit
        visible = rows[:limit]
        return WikiLogResponse(
            entries=[WikiLogEntryResponse.model_validate(item) for item in visible],
            next_cursor=visible[-1].id if has_more and visible else None,
        )

    async def search(
        self, scope: WikiScope, query: str, *, limit: int = 50
    ) -> WikiSearchResponse:
        value = query.strip()
        if not value:
            return WikiSearchResponse()
        limit = max(1, min(limit, 200))
        rows = (await self._session.execute(build_search_statement(scope, value, limit=limit))).all()
        results = [
            WikiSearchResult(
                id=row.id,
                slug=row.slug,
                title=row.title,
                summary=row.summary,
                page_type=row.page_type,
                score=float(row.rank),
            )
            for row in rows
        ]
        return WikiSearchResponse(
            results=results,
            total=int(rows[0].total_count) if rows else 0,
        )

    async def get_stats(self, scope: WikiScope) -> WikiStatsResponse:
        page_count = await self._count(
            select(func.count(WikiPage.id)).where(*_active_page_scope(scope))
        )
        folder_count = await self._count(
            select(func.count(WikiFolder.id)).where(
                WikiFolder.tenant_id == scope.tenant_id,
                WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                WikiFolder.deleted_at.is_(None),
            )
        )
        link_count = await self._count(
            select(func.count(WikiLink.id))
            .join(WikiPage, WikiPage.id == WikiLink.source_page_id)
            .where(*_active_page_scope(scope), WikiLink.tenant_id == scope.tenant_id)
        )
        issue_count = await self._count(
            select(func.count(WikiPageIssue.id)).where(
                WikiPageIssue.tenant_id == scope.tenant_id,
                WikiPageIssue.knowledge_base_id == scope.knowledge_base_id,
                WikiPageIssue.status == "pending",
            )
        )
        pending_tasks = await self._count(
            select(func.count(WikiPendingOp.id)).where(
                WikiPendingOp.tenant_id == scope.tenant_id,
                WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
            )
        )
        return WikiStatsResponse(
            page_count=page_count,
            folder_count=folder_count,
            link_count=link_count,
            issue_count=issue_count,
            pending_tasks=pending_tasks,
            is_active=await self._activity_probe.is_active(scope.knowledge_base_id),
        )

    async def get_graph(
        self,
        scope: WikiScope,
        *,
        mode: str,
        center: str | None,
        hops: int,
        limit: int,
        types: set[str] | None,
    ) -> WikiGraphResponse:
        limit = max(1, min(limit, self.GRAPH_HARD_LIMIT))
        if mode == "ego":
            if not center:
                raise WikiValidationError("GRAPH_CENTER_REQUIRED", "ego 图需要 center")
            return await self._ego_graph(scope, normalize_slug(center), hops, limit, types)
        return await self._overview_graph(scope, limit, types)

    async def _overview_graph(
        self, scope: WikiScope, limit: int, types: set[str] | None
    ) -> WikiGraphResponse:
        degree = _graph_degree_cte(scope)
        statement = (
            select(
                WikiPage.id,
                WikiPage.slug,
                WikiPage.title,
                WikiPage.page_type,
                func.coalesce(degree.c.link_count, 0).label("link_count"),
            )
            .outerjoin(degree, degree.c.page_id == WikiPage.id)
            .where(*_active_page_scope(scope))
            .order_by(func.coalesce(degree.c.link_count, 0).desc(), WikiPage.slug)
            .limit(limit)
        )
        if types:
            statement = statement.where(WikiPage.page_type.in_(types))
        rows = (await self._session.execute(statement)).all()
        id_to_slug = {row.id: row.slug for row in rows}
        edges = await self._edges_between(scope, set(id_to_slug), id_to_slug)
        return WikiGraphResponse(
            mode="overview",
            nodes=[
                WikiGraphNode(
                    slug=row.slug,
                    title=row.title,
                    page_type=row.page_type,
                    link_count=int(row.link_count),
                )
                for row in rows
            ],
            edges=edges,
        )

    async def _ego_graph(
        self,
        scope: WikiScope,
        center: str,
        hops: int,
        limit: int,
        types: set[str] | None,
    ) -> WikiGraphResponse:
        degree = _graph_degree_cte(scope)
        page_statement = (
            select(
                WikiPage.id,
                WikiPage.slug,
                WikiPage.title,
                WikiPage.page_type,
                func.coalesce(degree.c.link_count, 0).label("link_count"),
            )
            .outerjoin(degree, degree.c.page_id == WikiPage.id)
            .where(*_active_page_scope(scope), WikiPage.slug == center)
        )
        if types:
            page_statement = page_statement.where(WikiPage.page_type.in_(types))
        center_row = (await self._session.execute(page_statement)).first()
        if center_row is None:
            raise WikiNotFoundError("PAGE_NOT_FOUND", "图谱中心页面不存在")

        pages = {center_row.id: center_row}
        frontier = {center_row.id}
        for _ in range(max(1, min(hops, 3))):
            remaining = limit - len(pages)
            if remaining <= 0:
                break
            neighbor_rows = (
                await self._session.execute(
                    build_ego_neighbor_statement(
                        scope,
                        frontier=frontier,
                        visited=set(pages),
                        limit=remaining,
                        types=types,
                    )
                )
            ).all()
            if not neighbor_rows:
                break
            pages.update({row.id: row for row in neighbor_rows})
            frontier = {row.id for row in neighbor_rows}
            if len(pages) >= limit:
                break

        id_to_slug = {page_id: row.slug for page_id, row in pages.items()}
        edges = await self._edges_between(scope, set(pages), id_to_slug)
        ordered_pages = sorted(
            pages.values(), key=lambda row: (-int(row.link_count), row.slug)
        )
        return WikiGraphResponse(
            mode="ego",
            center=center,
            nodes=[
                WikiGraphNode(
                    slug=row.slug,
                    title=row.title,
                    page_type=row.page_type,
                    link_count=int(row.link_count),
                )
                for row in ordered_pages
            ],
            edges=edges,
        )

    async def lint(self, scope: WikiScope, *, issue_limit: int = 200) -> WikiLintResponse:
        issue_limit = max(1, min(issue_limit, 200))
        total_pages = await self._count(
            select(func.count(WikiPage.id)).where(*_active_page_scope(scope))
        )
        total_links = await self._count(
            select(func.count(WikiLink.id))
            .join(WikiPage, WikiPage.id == WikiLink.source_page_id)
            .where(*_active_page_scope(scope), WikiLink.tenant_id == scope.tenant_id)
        )
        broken_count = await self._count(
            select(func.count(WikiLink.id))
            .join(WikiPage, WikiLink.source_page_id == WikiPage.id)
            .where(
                *_active_page_scope(scope),
                WikiLink.tenant_id == scope.tenant_id,
                WikiLink.target_page_id.is_(None),
            )
        )
        empty_count = await self._count(
            select(func.count(WikiPage.id)).where(
                *_active_page_scope(scope), func.length(func.trim(WikiPage.content)) == 0
            )
        )
        outgoing = exists(
            select(WikiLink.id).where(
                WikiLink.tenant_id == scope.tenant_id,
                WikiLink.source_page_id == WikiPage.id,
            )
        )
        incoming = exists(
            select(WikiLink.id).where(
                WikiLink.tenant_id == scope.tenant_id,
                WikiLink.target_page_id == WikiPage.id,
            )
        )
        orphan_count = await self._count(
            select(func.count(WikiPage.id)).where(
                *_active_page_scope(scope), ~outgoing, ~incoming
            )
        )

        broken_rows = (
            await self._session.execute(
                build_broken_link_statement(scope, limit=issue_limit)
            )
        ).all()
        remaining = issue_limit - len(broken_rows)
        empty_slugs = (
            list(
                (
                    await self._session.execute(
                        build_empty_page_statement(scope, limit=remaining)
                    )
                ).scalars()
            )
            if remaining > 0
            else []
        )
        remaining -= len(empty_slugs)
        orphan_slugs = (
            list(
                (
                    await self._session.execute(
                        build_orphan_page_statement(scope, limit=remaining)
                    )
                ).scalars()
            )
            if remaining > 0
            else []
        )

        issues = [
            WikiLintItem(
                issue_type="broken_link",
                severity="error",
                page_slug=row.slug,
                target_slug=row.target_slug,
                description=f"链接目标 {row.target_slug} 不存在",
            )
            for row in broken_rows
        ]
        issues.extend(
            WikiLintItem(
                issue_type="empty_content",
                severity="warning",
                page_slug=slug,
                description="页面正文为空",
            )
            for slug in empty_slugs
        )
        issues.extend(
            WikiLintItem(
                issue_type="orphan_page",
                severity="warning",
                page_slug=slug,
                description="页面没有入链或出链",
            )
            for slug in orphan_slugs
        )
        counts = {
            "orphan_page": orphan_count,
            "broken_link": broken_count,
            "empty_content": empty_count,
        }
        return WikiLintResponse(
            health_score=calculate_health_score(
                total_pages,
                counts["orphan_page"],
                counts["broken_link"],
                counts["empty_content"],
                total_links,
            ),
            issues=issues,
            counts=counts,
        )

    async def list_issues(
        self,
        scope: WikiScope,
        *,
        status: str | None,
        cursor: str | None,
        limit: int,
    ) -> WikiIssueListResponse:
        limit = max(1, min(limit, 200))
        statement = (
            select(WikiPageIssue)
            .where(
                WikiPageIssue.tenant_id == scope.tenant_id,
                WikiPageIssue.knowledge_base_id == scope.knowledge_base_id,
            )
            .order_by(WikiPageIssue.created_at.desc(), WikiPageIssue.id.desc())
            .limit(limit + 1)
        )
        if status:
            statement = statement.where(WikiPageIssue.status == status)
        if cursor:
            cursor_time, cursor_id = _decode_issue_cursor(cursor)
            statement = statement.where(
                or_(
                    WikiPageIssue.created_at < cursor_time,
                    (
                        (WikiPageIssue.created_at == cursor_time)
                        & (WikiPageIssue.id < cursor_id)
                    ),
                )
            )
        rows = list((await self._session.execute(statement)).scalars())
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = (
            _encode_issue_cursor(visible[-1].created_at, visible[-1].id)
            if has_more and visible
            else None
        )
        return WikiIssueListResponse(
            issues=[WikiIssueResponse.model_validate(item) for item in visible],
            next_cursor=next_cursor,
        )

    async def update_issue_status(
        self, scope: WikiScope, issue_id: UUID, status: str
    ) -> WikiIssueResponse:
        if not scope.can_write:
            raise WikiPermissionError()
        result = await self._session.execute(
            update(WikiPageIssue)
            .where(
                WikiPageIssue.id == issue_id,
                WikiPageIssue.tenant_id == scope.tenant_id,
                WikiPageIssue.knowledge_base_id == scope.knowledge_base_id,
            )
            .values(status=status, updated_at=datetime.now(UTC))
            .returning(WikiPageIssue)
        )
        issue = result.scalar_one_or_none()
        if issue is None:
            raise WikiNotFoundError("ISSUE_NOT_FOUND", "Wiki 问题单不存在")
        return WikiIssueResponse.model_validate(issue)

    async def rebuild_links(self, scope: WikiScope) -> WikiRebuildLinksResponse:
        if not scope.can_write:
            raise WikiPermissionError()
        page_store = SqlAlchemyPageStore(self._session)
        last_id: UUID | None = None
        pages_scanned = 0
        links_created = 0
        while True:
            statement = (
                select(WikiPage)
                .options(load_only(WikiPage.id, WikiPage.slug, WikiPage.content))
                .where(*_active_page_scope(scope))
                .order_by(WikiPage.id)
                .limit(200)
            )
            if last_id is not None:
                statement = statement.where(WikiPage.id > last_id)
            pages = list((await self._session.execute(statement)).scalars())
            if not pages:
                break
            for page in pages:
                targets = extract_wiki_links(page.content)
                await page_store.replace_page_links(scope, page, targets)
                links_created += len(targets)
            pages_scanned += len(pages)
            last_id = pages[-1].id
        self._session.add(
            WikiLogEntry(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                operation_id=uuid4(),
                action="links_rebuilt",
                message=f"重建 {pages_scanned} 个页面的链接投影",
                pages_affected=[],
                actor_id=scope.actor_id,
            )
        )
        await self._session.flush()
        return WikiRebuildLinksResponse(
            pages_scanned=pages_scanned, links_created=links_created
        )

    async def _edges_between(
        self,
        scope: WikiScope,
        page_ids: set[UUID],
        id_to_slug: dict[UUID, str],
    ) -> list[WikiGraphEdgeResponse]:
        if not page_ids:
            return []
        visible_edges = _visible_edges_cte(scope)
        rows = (
            await self._session.execute(
                select(
                    visible_edges.c.source_page_id,
                    visible_edges.c.target_page_id,
                )
                .where(
                    visible_edges.c.source_page_id.in_(page_ids),
                    visible_edges.c.target_page_id.in_(page_ids),
                )
                .order_by(
                    visible_edges.c.source_page_id,
                    visible_edges.c.target_page_id,
                )
                .limit(self.GRAPH_EDGE_HARD_LIMIT)
            )
        ).all()
        edges = [
            WikiGraphEdgeResponse(
                source=id_to_slug[row.source_page_id],
                target=id_to_slug[row.target_page_id],
            )
            for row in rows
        ]
        return sorted(edges, key=lambda edge: (edge.source, edge.target))

    async def _count(self, statement) -> int:
        return int((await self._session.execute(statement)).scalar_one())


def _active_page_scope(scope: WikiScope):
    return (
        WikiPage.tenant_id == scope.tenant_id,
        WikiPage.knowledge_base_id == scope.knowledge_base_id,
        WikiPage.deleted_at.is_(None),
        WikiPage.status == "published",
    )


def _encode_issue_cursor(created_at: datetime, issue_id: UUID) -> str:
    raw = f"{created_at.isoformat()}|{issue_id}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_issue_cursor(value: str) -> tuple[datetime, UUID]:
    try:
        decoded = base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8")
        timestamp, issue_id = decoded.rsplit("|", 1)
        return datetime.fromisoformat(timestamp), UUID(issue_id)
    except (ValueError, UnicodeError) as exc:
        raise WikiValidationError("INVALID_CURSOR", "问题单 cursor 无效") from exc
