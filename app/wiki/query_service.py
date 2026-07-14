"""Wiki Index、搜索、图谱、日志和质量查询。"""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import case, exists, func, literal, or_, select, union_all, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import load_only

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
from app.wiki.graph import GraphPage, WikiGraphEdge, build_ego_graph
from app.wiki.models import WikiFolder, WikiLink, WikiLogEntry, WikiPage, WikiPageIssue
from app.wiki.scope import WikiScope
from app.wiki.sql_page_store import SqlAlchemyPageStore


def build_search_statement(scope: WikiScope, query: str, *, limit: int):
    """构建不解释用户正则的 PostgreSQL 排名搜索语句。"""

    normalized = query.strip().casefold()
    contains_pattern = f"%{normalized}%"
    prefix_pattern = f"{normalized}%"
    vector = func.to_tsvector(
        literal("simple"),
        WikiPage.title + literal(" ") + WikiPage.summary + literal(" ") + WikiPage.content,
    )
    ts_query = func.plainto_tsquery(literal("simple"), query.strip())
    title_similarity = func.similarity(func.lower(WikiPage.title), normalized)
    rank = (
        title_similarity * 4
        + case((func.lower(WikiPage.slug).ilike(prefix_pattern), 3.0), else_=0.0)
        + case((func.lower(WikiPage.slug).ilike(contains_pattern), 1.5), else_=0.0)
        + func.ts_rank_cd(vector, ts_query)
    ).label("rank")
    return (
        select(
            WikiPage.id,
            WikiPage.slug,
            WikiPage.title,
            WikiPage.summary,
            WikiPage.page_type,
            rank,
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


class WikiQueryService:
    """面向现有前端的只读查询和确定性维护操作。"""

    GRAPH_HARD_LIMIT = 2000
    _INDEX_TYPES = (
        WikiPageType.SUMMARY,
        WikiPageType.ENTITY,
        WikiPageType.CONCEPT,
        WikiPageType.SYNTHESIS,
        WikiPageType.COMPARISON,
    )

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_index(
        self,
        scope: WikiScope,
        *,
        page_type: WikiPageType | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> WikiIndexResponse:
        limit = max(1, min(limit, 200))
        intro_result = await self._session.execute(
            select(WikiPage.content, WikiPage.version)
            .where(
                *_active_page_scope(scope),
                WikiPage.page_type == WikiPageType.INDEX.value,
            )
            .order_by(WikiPage.updated_at.desc())
            .limit(1)
        )
        intro_row = intro_result.first()

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
        return WikiSearchResponse(results=results, total=len(results))

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
            .where(*_active_page_scope(scope))
        )
        issue_count = await self._count(
            select(func.count(WikiPageIssue.id)).where(
                WikiPageIssue.tenant_id == scope.tenant_id,
                WikiPageIssue.knowledge_base_id == scope.knowledge_base_id,
                WikiPageIssue.status == "pending",
            )
        )
        return WikiStatsResponse(
            page_count=page_count,
            folder_count=folder_count,
            link_count=link_count,
            issue_count=issue_count,
            pending_tasks=0,
            is_active=False,
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
        edge_nodes = union_all(
            select(WikiLink.source_page_id.label("page_id")).where(
                WikiLink.knowledge_base_id == scope.knowledge_base_id
            ),
            select(WikiLink.target_page_id.label("page_id")).where(
                WikiLink.knowledge_base_id == scope.knowledge_base_id,
                WikiLink.target_page_id.is_not(None),
            ),
        ).cte("edge_nodes")
        degree = (
            select(edge_nodes.c.page_id, func.count().label("link_count"))
            .group_by(edge_nodes.c.page_id)
            .cte("degree")
        )
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
        page_statement = select(
            WikiPage.id, WikiPage.slug, WikiPage.title, WikiPage.page_type
        ).where(*_active_page_scope(scope), WikiPage.slug == center)
        if types:
            page_statement = page_statement.where(WikiPage.page_type.in_(types))
        center_row = (await self._session.execute(page_statement)).first()
        if center_row is None:
            raise WikiNotFoundError("PAGE_NOT_FOUND", "图谱中心页面不存在")

        pages = {center_row.id: center_row}
        frontier = {center_row.id}
        collected_edges: set[tuple[UUID, UUID]] = set()
        for _ in range(max(1, min(hops, 3))):
            edge_rows = (
                await self._session.execute(
                    select(WikiLink.source_page_id, WikiLink.target_page_id).where(
                        WikiLink.knowledge_base_id == scope.knowledge_base_id,
                        WikiLink.target_page_id.is_not(None),
                        or_(
                            WikiLink.source_page_id.in_(frontier),
                            WikiLink.target_page_id.in_(frontier),
                        ),
                    )
                )
            ).all()
            candidate_ids = {
                page_id
                for row in edge_rows
                for page_id in (row.source_page_id, row.target_page_id)
                if page_id is not None
            }
            if not candidate_ids:
                break
            visited_ids = set(pages)
            candidate_statement = select(
                WikiPage.id, WikiPage.slug, WikiPage.title, WikiPage.page_type
            ).where(*_active_page_scope(scope), WikiPage.id.in_(candidate_ids))
            if types:
                candidate_statement = candidate_statement.where(WikiPage.page_type.in_(types))
            candidate_rows = (await self._session.execute(candidate_statement)).all()
            allowed_ids = {row.id for row in candidate_rows}
            pages.update({row.id: row for row in candidate_rows})
            valid_edges = {
                (row.source_page_id, row.target_page_id)
                for row in edge_rows
                if row.source_page_id in allowed_ids and row.target_page_id in allowed_ids
            }
            collected_edges.update(valid_edges)
            next_frontier = allowed_ids - visited_ids
            if not next_frontier:
                break
            frontier = next_frontier
            if len(pages) >= self.GRAPH_HARD_LIMIT:
                break

        id_to_slug = {page_id: row.slug for page_id, row in pages.items()}
        graph = build_ego_graph(
            [
                GraphPage(slug=row.slug, title=row.title, page_type=row.page_type)
                for row in pages.values()
            ],
            [
                WikiGraphEdge(source=id_to_slug[source], target=id_to_slug[target])
                for source, target in collected_edges
                if source in id_to_slug and target in id_to_slug
            ],
            center=center,
            hops=hops,
            limit=limit,
            allowed_types=types,
        )
        return WikiGraphResponse(
            mode="ego",
            center=center,
            nodes=[
                WikiGraphNode(
                    slug=node.slug,
                    title=node.title,
                    page_type=node.page_type,
                    link_count=node.link_count,
                )
                for node in graph.nodes
            ],
            edges=[WikiGraphEdgeResponse(source=edge.source, target=edge.target) for edge in graph.edges],
        )

    async def lint(self, scope: WikiScope, *, issue_limit: int = 1000) -> WikiLintResponse:
        total_pages = await self._count(
            select(func.count(WikiPage.id)).where(*_active_page_scope(scope))
        )
        total_links = await self._count(
            select(func.count(WikiLink.id))
            .join(WikiPage, WikiPage.id == WikiLink.source_page_id)
            .where(*_active_page_scope(scope))
        )
        broken_statement = (
            select(WikiPage.slug, WikiLink.target_slug)
            .join(WikiLink, WikiLink.source_page_id == WikiPage.id)
            .where(*_active_page_scope(scope), WikiLink.target_page_id.is_(None))
            .order_by(WikiPage.slug, WikiLink.target_slug)
        )
        broken_rows = (await self._session.execute(broken_statement)).all()
        empty_statement = select(WikiPage.slug).where(
            *_active_page_scope(scope), func.length(func.trim(WikiPage.content)) == 0
        )
        empty_slugs = list((await self._session.execute(empty_statement)).scalars())
        outgoing = exists(select(WikiLink.id).where(WikiLink.source_page_id == WikiPage.id))
        incoming = exists(select(WikiLink.id).where(WikiLink.target_page_id == WikiPage.id))
        orphan_statement = select(WikiPage.slug).where(
            *_active_page_scope(scope), ~outgoing, ~incoming
        )
        orphan_slugs = list((await self._session.execute(orphan_statement)).scalars())

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
            "orphan_page": len(orphan_slugs),
            "broken_link": len(broken_rows),
            "empty_content": len(empty_slugs),
        }
        return WikiLintResponse(
            health_score=calculate_health_score(
                total_pages,
                counts["orphan_page"],
                counts["broken_link"],
                counts["empty_content"],
                total_links,
            ),
            issues=issues[:issue_limit],
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
        rows = (
            await self._session.execute(
                select(WikiLink.source_page_id, WikiLink.target_page_id)
                .where(
                    WikiLink.knowledge_base_id == scope.knowledge_base_id,
                    WikiLink.source_page_id.in_(page_ids),
                    WikiLink.target_page_id.in_(page_ids),
                )
                .order_by(WikiLink.source_page_id, WikiLink.target_page_id)
            )
        ).all()
        return [
            WikiGraphEdgeResponse(
                source=id_to_slug[row.source_page_id],
                target=id_to_slug[row.target_page_id],
            )
            for row in rows
        ]

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
