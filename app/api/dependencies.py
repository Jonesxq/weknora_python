"""应用依赖提供器。"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import hmac
import os
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.docreader import DocumentReaderService
from app.infrastructure.database.config import DatabaseSettings
from app.infrastructure.database.session import create_database_engine, create_session_factory
from app.wiki.errors import WikiError, WikiPermissionError, WikiValidationError
from app.wiki.folder_service import WikiFolderService
from app.wiki.page_service import WikiPageService
from app.wiki.query_service import ActivityProbe, WikiQueryService
from app.wiki.scope import WikiScope
from app.wiki.sql_folder_store import SqlAlchemyFolderStore
from app.wiki.sql_page_store import SqlAlchemyPageStore
from app.wiki.tasks.locks import build_lock_manager_from_env


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


@lru_cache(maxsize=1)
def get_wiki_activity_probe() -> ActivityProbe:
    return build_lock_manager_from_env()


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
    activity_probe: Annotated[ActivityProbe, Depends(get_wiki_activity_probe)],
) -> WikiServices:
    return WikiServices(
        page=WikiPageService(SqlAlchemyPageStore(session)),
        folder=WikiFolderService(SqlAlchemyFolderStore(session)),
        query=WikiQueryService(session, activity_probe),
    )


def get_wiki_scope(
    kb_id: UUID,
    tenant_id: Annotated[int, Header(alias="X-Tenant-ID")],
    user_id: Annotated[str, Header(alias="X-User-ID", min_length=1)],
    role: Annotated[str, Header(alias="X-Role")],
    signature: Annotated[
        str,
        Header(alias="X-Wiki-Context-Signature", min_length=64, max_length=64),
    ],
) -> WikiScope:
    """临时访问适配层；接入真实鉴权后只需替换此依赖。"""

    normalized_role = role.strip().casefold()
    secret = os.getenv("GRAPH_WIKI_CONTEXT_SECRET", "")
    if len(secret) < 32:
        raise WikiError(
            "WIKI_CONTEXT_NOT_CONFIGURED",
            "服务端未配置安全的 Wiki 访问上下文密钥",
            503,
        )
    expected_signature = sign_wiki_context(
        secret,
        tenant_id=tenant_id,
        user_id=user_id,
        role=normalized_role,
        knowledge_base_id=kb_id,
    )
    if not hmac.compare_digest(signature.casefold(), expected_signature):
        raise WikiError(
            "INVALID_WIKI_CONTEXT_SIGNATURE",
            "Wiki 访问上下文签名无效",
            401,
        )
    allowed_roles = {"viewer", "contributor", "owner", "admin"}
    if normalized_role not in allowed_roles:
        raise WikiValidationError("INVALID_WIKI_ROLE", "X-Role 不是受支持的 Wiki 角色")
    return WikiScope(
        tenant_id=tenant_id,
        knowledge_base_id=kb_id,
        actor_id=user_id,
        can_write=normalized_role in {"owner", "admin"},
    )


def sign_wiki_context(
    secret: str,
    *,
    tenant_id: int,
    user_id: str,
    role: str,
    knowledge_base_id: UUID,
) -> str:
    """为可信网关注入的 Wiki 上下文生成 HMAC-SHA256 签名。"""

    payload = "\n".join(
        (
            str(tenant_id),
            user_id,
            role.strip().casefold(),
            str(knowledge_base_id),
        )
    ).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def get_wiki_write_scope(
    scope: Annotated[WikiScope, Depends(get_wiki_scope)],
) -> WikiScope:
    if not scope.can_write:
        raise WikiPermissionError()
    return scope
