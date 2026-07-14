"""Wiki 阶段一兼容 REST API。"""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status

from app.api.dependencies import (
    WikiServices,
    get_wiki_scope,
    get_wiki_services,
    get_wiki_write_scope,
)
from app.schemas.wiki.folders import (
    WikiFolderCreateRequest,
    WikiFolderListResponse,
    WikiFolderResponse,
    WikiFolderUpdateRequest,
)
from app.schemas.wiki.pages import (
    WikiPageCreateRequest,
    WikiPageListQuery,
    WikiPageListResponse,
    WikiPageMoveRequest,
    WikiPageResponse,
    WikiPageUpdateRequest,
)
from app.schemas.wiki.queries import (
    WikiGraphResponse,
    WikiIndexResponse,
    WikiIssueListResponse,
    WikiIssueResponse,
    WikiIssueStatusRequest,
    WikiLintResponse,
    WikiLogResponse,
    WikiRebuildLinksResponse,
    WikiSearchResponse,
    WikiStatsResponse,
)
from app.wiki.enums import WikiIssueStatus, WikiPageType
from app.wiki.errors import WikiValidationError
from app.wiki.scope import WikiScope

router = APIRouter(prefix="/knowledgebase/{kb_id}/wiki", tags=["Wiki"])

ReadScope = Annotated[WikiScope, Depends(get_wiki_scope)]
WriteScope = Annotated[WikiScope, Depends(get_wiki_write_scope)]
Services = Annotated[WikiServices, Depends(get_wiki_services)]


@router.get("/pages", response_model=WikiPageListResponse, summary="分页列出 Wiki 页面")
async def list_pages(
    scope: ReadScope,
    services: Services,
    query: Annotated[WikiPageListQuery, Query()],
) -> WikiPageListResponse:
    return await services.page.list_pages(scope, query)


@router.post(
    "/pages",
    response_model=WikiPageResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建 Wiki 页面",
)
async def create_page(
    request: WikiPageCreateRequest, scope: WriteScope, services: Services
) -> WikiPageResponse:
    return await services.page.create_page(scope, request)


@router.get("/pages/{slug:path}", response_model=WikiPageResponse, summary="读取 Wiki 页面")
async def get_page(slug: str, scope: ReadScope, services: Services) -> WikiPageResponse:
    return await services.page.get_page(scope, slug)


@router.put("/pages/{slug:path}", response_model=WikiPageResponse, summary="更新 Wiki 页面")
async def update_page(
    slug: str,
    request: WikiPageUpdateRequest,
    scope: WriteScope,
    services: Services,
) -> WikiPageResponse:
    return await services.page.update_page(scope, slug, request)


@router.delete(
    "/pages/{slug:path}", status_code=status.HTTP_204_NO_CONTENT, summary="删除 Wiki 页面"
)
async def delete_page(slug: str, scope: WriteScope, services: Services) -> Response:
    await services.page.delete_page(scope, slug)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put("/move-page", response_model=WikiPageResponse, summary="移动 Wiki 页面")
async def move_page(
    request: WikiPageMoveRequest, scope: WriteScope, services: Services
) -> WikiPageResponse:
    return await services.page.move_page(scope, request)


@router.get("/folders", response_model=WikiFolderListResponse, summary="列出直接子目录")
async def list_folders(
    scope: ReadScope,
    services: Services,
    parent_id: UUID | None = None,
) -> WikiFolderListResponse:
    return WikiFolderListResponse(
        folders=await services.folder.list_folders(scope, parent_id)
    )


@router.post(
    "/folders",
    response_model=WikiFolderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建 Wiki 目录",
)
async def create_folder(
    request: WikiFolderCreateRequest, scope: WriteScope, services: Services
) -> WikiFolderResponse:
    return await services.folder.create_folder(scope, request)


@router.put("/folders/{folder_id}", response_model=WikiFolderResponse, summary="更新 Wiki 目录")
async def update_folder(
    folder_id: UUID,
    request: WikiFolderUpdateRequest,
    scope: WriteScope,
    services: Services,
) -> WikiFolderResponse:
    return await services.folder.update_folder(scope, folder_id, request)


@router.delete(
    "/folders/{folder_id}", status_code=status.HTTP_204_NO_CONTENT, summary="删除空目录"
)
async def delete_folder(
    folder_id: UUID, scope: WriteScope, services: Services
) -> Response:
    await services.folder.delete_folder(scope, folder_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/index", response_model=WikiIndexResponse, summary="读取结构化 Wiki 索引")
async def get_index(
    scope: ReadScope,
    services: Services,
    page_type: WikiPageType | None = None,
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> WikiIndexResponse:
    return await services.query.get_index(
        scope, page_type=page_type, cursor=cursor, limit=limit
    )


@router.get("/log", response_model=WikiLogResponse, summary="读取 Wiki 操作日志")
async def get_log(
    scope: ReadScope,
    services: Services,
    cursor: int | None = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> WikiLogResponse:
    return await services.query.get_log(scope, cursor=cursor, limit=limit)


@router.get("/graph", response_model=WikiGraphResponse, summary="读取 Wiki 关系图")
async def get_graph(
    scope: ReadScope,
    services: Services,
    mode: Literal["overview", "ego"] = "overview",
    center: str | None = None,
    hops: int = Query(default=1, ge=1, le=3),
    limit: int = Query(default=500, ge=1, le=2000),
    types: str | None = None,
) -> WikiGraphResponse:
    allowed_types = _parse_page_types(types)
    return await services.query.get_graph(
        scope,
        mode=mode,
        center=center,
        hops=hops,
        limit=limit,
        types=allowed_types,
    )


@router.get("/stats", response_model=WikiStatsResponse, summary="读取 Wiki 统计")
async def get_stats(scope: ReadScope, services: Services) -> WikiStatsResponse:
    return await services.query.get_stats(scope)


@router.get("/search", response_model=WikiSearchResponse, summary="搜索 Wiki 页面")
async def search(
    scope: ReadScope,
    services: Services,
    query: str = Query(alias="q", min_length=1, max_length=512),
    limit: int = Query(default=50, ge=1, le=200),
) -> WikiSearchResponse:
    return await services.query.search(scope, query, limit=limit)


@router.post(
    "/rebuild-links", response_model=WikiRebuildLinksResponse, summary="重建 Wiki 链接投影"
)
async def rebuild_links(scope: WriteScope, services: Services) -> WikiRebuildLinksResponse:
    return await services.query.rebuild_links(scope)


@router.get("/lint", response_model=WikiLintResponse, summary="检查 Wiki 健康状况")
async def lint(scope: ReadScope, services: Services) -> WikiLintResponse:
    return await services.query.lint(scope)


@router.get("/issues", response_model=WikiIssueListResponse, summary="查询 Wiki 问题单")
async def list_issues(
    scope: ReadScope,
    services: Services,
    issue_status: WikiIssueStatus | None = Query(default=None, alias="status"),
    cursor: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> WikiIssueListResponse:
    return await services.query.list_issues(
        scope,
        status=issue_status.value if issue_status else None,
        cursor=cursor,
        limit=limit,
    )


@router.put(
    "/issues/{issue_id}/status",
    response_model=WikiIssueResponse,
    summary="更新 Wiki 问题状态",
)
async def update_issue_status(
    issue_id: UUID,
    request: WikiIssueStatusRequest,
    scope: WriteScope,
    services: Services,
) -> WikiIssueResponse:
    return await services.query.update_issue_status(scope, issue_id, request.status.value)


def _parse_page_types(value: str | None) -> set[str] | None:
    if not value:
        return None
    requested = {item.strip() for item in value.split(",") if item.strip()}
    allowed = {item.value for item in WikiPageType}
    invalid = requested - allowed
    if invalid:
        raise WikiValidationError(
            "INVALID_PAGE_TYPE", f"不支持的页面类型: {', '.join(sorted(invalid))}"
        )
    return requested
