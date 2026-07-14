from __future__ import annotations

import os
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.infrastructure.database.base import Base
from app.schemas.wiki.pages import WikiPageCreateRequest, WikiPageUpdateRequest
from app.wiki.errors import WikiNotFoundError, WikiVersionConflictError
from app.wiki.page_service import WikiPageService
from app.wiki.scope import WikiScope
from app.wiki.sql_page_store import SqlAlchemyPageStore

TEST_DATABASE_URL = os.getenv("GRAPH_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="未配置 GRAPH_TEST_DATABASE_URL，不使用 SQLite 替代 PostgreSQL",
)


@pytest_asyncio.fixture
async def postgres_session() -> AsyncSession:
    assert TEST_DATABASE_URL is not None
    schema = f"wiki_test_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": f"{schema},public"}},
    )
    async with engine.begin() as connection:
        await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        await connection.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            yield session
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_page_store_enforces_scope_links_and_optimistic_version(
    postgres_session: AsyncSession,
) -> None:
    scope = WikiScope(
        tenant_id=7,
        knowledge_base_id=uuid4(),
        actor_id="owner-1",
        can_write=True,
    )
    service = WikiPageService(SqlAlchemyPageStore(postgres_session))
    target = await service.create_page(
        scope,
        WikiPageCreateRequest(
            slug="concept/knowledge-graph",
            title="Knowledge Graph",
            page_type="concept",
        ),
    )
    source = await service.create_page(
        scope,
        WikiPageCreateRequest(
            slug="entity/acme",
            title="Acme",
            page_type="entity",
            content="使用 [[concept/knowledge-graph]]",
        ),
    )
    await postgres_session.commit()

    assert source.out_links == [target.slug]
    assert (await service.get_page(scope, target.slug)).in_links == [source.slug]

    updated = await service.update_page(
        scope,
        source.slug,
        WikiPageUpdateRequest(version=1, summary="新摘要"),
    )
    assert updated.version == 2
    with pytest.raises(WikiVersionConflictError):
        await service.update_page(
            scope,
            source.slug,
            WikiPageUpdateRequest(version=1, summary="并发旧写入"),
        )

    other_tenant = WikiScope(
        tenant_id=8,
        knowledge_base_id=scope.knowledge_base_id,
        actor_id="viewer-2",
    )
    with pytest.raises(WikiNotFoundError):
        await service.get_page(other_tenant, source.slug)
