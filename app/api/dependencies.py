"""应用依赖提供器。"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from functools import lru_cache
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.docreader import DocumentReaderService
from app.infrastructure.database.config import DatabaseSettings
from app.infrastructure.database.session import create_database_engine, create_session_factory
from app.wiki.errors import WikiPermissionError, WikiValidationError
from app.wiki.folder_service import WikiFolderService
from app.wiki.page_service import WikiPageService
from app.wiki.query_service import WikiQueryService
from app.wiki.scope import WikiScope
from app.wiki.sql_folder_store import SqlAlchemyFolderStore
from app.wiki.sql_page_store import SqlAlchemyPageStore


@lru_cache(maxsize=1)
def get_document_reader() -> DocumentReaderService:
    """返回进程内共享的文档解析服务实例。"""

    return DocumentReaderService()


@lru_cache(maxsize=1)
def get_database_engine() -> AsyncEngine:
    return create_database_engine(DatabaseSettings.from_env())


@lru_cache(maxsize=1)
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return create_session_factory(get_database_engine())


async def get_database_session() -> AsyncIterator[AsyncSession]:
    """请求成功时提交，异常时回滚同一 Wiki 事务。"""

    async with get_session_factory()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@dataclass(frozen=True, slots=True)
class WikiServices:
    page: WikiPageService
    folder: WikiFolderService
    query: WikiQueryService


def get_wiki_services(
    session: Annotated[AsyncSession, Depends(get_database_session)],
) -> WikiServices:
    return WikiServices(
        page=WikiPageService(SqlAlchemyPageStore(session)),
        folder=WikiFolderService(SqlAlchemyFolderStore(session)),
        query=WikiQueryService(session),
    )


def get_wiki_scope(
    kb_id: UUID,
    tenant_id: Annotated[int, Header(alias="X-Tenant-ID")],
    user_id: Annotated[str, Header(alias="X-User-ID", min_length=1)],
    role: Annotated[str, Header(alias="X-Role")],
) -> WikiScope:
    """临时访问适配层；接入真实鉴权后只需替换此依赖。"""

    normalized_role = role.strip().casefold()
    allowed_roles = {"viewer", "contributor", "owner", "admin"}
    if normalized_role not in allowed_roles:
        raise WikiValidationError("INVALID_WIKI_ROLE", "X-Role 不是受支持的 Wiki 角色")
    return WikiScope(
        tenant_id=tenant_id,
        knowledge_base_id=kb_id,
        actor_id=user_id,
        can_write=normalized_role in {"contributor", "owner", "admin"},
    )


def get_wiki_write_scope(
    scope: Annotated[WikiScope, Depends(get_wiki_scope)],
) -> WikiScope:
    if not scope.can_write:
        raise WikiPermissionError()
    return scope
