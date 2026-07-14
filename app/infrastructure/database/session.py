"""Async SQLAlchemy Engine 与 Session 工厂。"""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.infrastructure.database.config import DatabaseSettings


def create_database_engine(settings: DatabaseSettings) -> AsyncEngine:
    """按配置创建 PostgreSQL 异步引擎，但不主动建立连接。"""

    return create_async_engine(settings.url, echo=settings.echo, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """创建禁用过期加载的异步会话工厂。"""

    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
