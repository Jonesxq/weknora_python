from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.infrastructure.database.base import Base
from app.schemas.wiki.pages import WikiPageCreateRequest, WikiPageUpdateRequest
from app.wiki.errors import WikiNotFoundError, WikiVersionConflictError
from app.wiki.ingest.schemas import ReducedPage, SourceKnowledge
from app.wiki.ingest.store import SqlAlchemyIngestStore, SqlFinalizationPort
from app.wiki.models import (
    TaskOutbox,
    WikiFinalizationMarker,
    WikiLink,
    WikiLogEntry,
    WikiPage,
    WikiPendingOp,
)
from app.wiki.page_service import WikiPageService
from app.wiki.query_service import WikiQueryService
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


@pytest_asyncio.fixture
async def postgres_factory() -> async_sessionmaker[AsyncSession]:
    """为自行管理短 session 的摄取仓储提供真实 PostgreSQL 工厂。"""

    assert TEST_DATABASE_URL is not None
    schema = f"wiki_ingest_test_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": f"{schema},public"}},
    )
    async with engine.begin() as connection:
        await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    try:
        yield factory
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

    neighbor_slugs: list[str] = []
    for index in range(30):
        neighbor = await service.create_page(
            scope,
            WikiPageCreateRequest(
                slug=f"concept/neighbor-{index:02d}",
                title=f"Neighbor {index:02d}",
                page_type="concept",
                summary="Knowledge Graph neighbor",
            ),
        )
        neighbor_slugs.append(neighbor.slug)
    updated = await service.update_page(
        scope,
        source.slug,
        WikiPageUpdateRequest(
            version=2,
            content=" ".join(f"[[{slug}]]" for slug in neighbor_slugs),
        ),
    )
    assert updated.version == 3
    await postgres_session.commit()

    queries = WikiQueryService(postgres_session)
    search = await queries.search(scope, "Knowledge Graph", limit=10)
    assert search.total >= 30
    assert search.results

    graph = await queries.get_graph(
        scope,
        mode="ego",
        center=source.slug,
        hops=1,
        limit=10,
        types={"entity", "concept"},
    )
    assert len(graph.nodes) == 10
    assert graph.nodes[0].slug == source.slug
    assert [node.slug for node in graph.nodes[1:]] == sorted(
        node.slug for node in graph.nodes[1:]
    )

    other_tenant = WikiScope(
        tenant_id=8,
        knowledge_base_id=scope.knowledge_base_id,
        actor_id="viewer-2",
    )
    with pytest.raises(WikiNotFoundError):
        await service.get_page(other_tenant, source.slug)


@pytest.mark.asyncio
async def test_ingest_store_is_atomic_idempotent_and_writes_follow_up(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(
        tenant_id=7,
        knowledge_base_id=uuid4(),
        actor_id="wiki-worker",
        can_write=True,
    )
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    first_knowledge = SourceKnowledge(
        id="knowledge-1",
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        title="文档一",
        op_version="version-1",
    )
    second_knowledge = first_knowledge.model_copy(
        update={"id": "knowledge-2", "title": "文档二"}
    )
    first = await store.enqueue(
        scope, first_knowledge, {"knowledge_id": first_knowledge.id}, delay_seconds=0
    )
    duplicate = await store.enqueue(
        scope, first_knowledge, {"knowledge_id": first_knowledge.id}, delay_seconds=0
    )
    await store.enqueue(
        scope, second_knowledge, {"knowledge_id": second_knowledge.id}, delay_seconds=0
    )
    assert duplicate.id == first.id
    assert duplicate.deduplicated is True

    outbox = await store.claim_outbox(10, 600)
    assert len(outbox) == 2
    outbox_token = outbox[0].claim_token
    assert outbox_token is not None
    assert {event.claim_token for event in outbox} == {outbox_token}
    assert {event.attempts for event in outbox} == {1}
    with pytest.raises(ValueError, match="token"):
        await store.mark_outbox_sent([event.id for event in outbox], uuid4())
    await store.release_outbox([event.id for event in outbox], outbox_token)
    retried_outbox = await store.claim_outbox(10, 600)
    assert {event.id for event in retried_outbox} == {event.id for event in outbox}
    assert {event.attempts for event in retried_outbox} == {2}
    retry_token = retried_outbox[0].claim_token
    assert retry_token is not None
    await store.mark_outbox_sent(
        [event.id for event in retried_outbox], retry_token
    )

    claimed = await store.claim_pending(scope, 1, 600)
    assert len(claimed) == 1
    op = claimed[0]
    assert op.claim_token is not None
    async with postgres_factory() as session, session.begin():
        deleted_target = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="concept/target",
            title="已删除旧目标",
            page_type="concept",
            status="published",
            content="旧正文",
            summary="旧摘要",
            deleted_at=datetime.now(UTC),
        )
        old_source = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/old-source",
            title="旧来源",
            page_type="entity",
            status="published",
            content="[[concept/target]]",
            summary="旧来源",
        )
        session.add_all([deleted_target, old_source])
        await session.flush()
        session.add(
            WikiLink(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                source_page_id=old_source.id,
                target_slug="concept/target",
                target_page_id=None,
            )
        )
    operation_id = uuid4()
    applied = await store.apply_results(
        scope,
        op.claim_token,
        [
            ReducedPage(
                slug="entity/source",
                title="Source",
                page_type="entity",
                content="链接到 [[concept/target]]",
                summary="来源",
                aliases=[" Source ", "Source"],
                source_refs=[op.knowledge_id, op.knowledge_id],
                chunk_refs=["chunk-1", "chunk-1"],
                contributor_op_ids=[op.id],
            ),
            ReducedPage(
                slug="concept/target",
                title="Target",
                page_type="concept",
                content="目标正文",
                summary="目标",
                contributor_op_ids=[op.id],
            ),
        ],
        [op.id],
        operation_id,
    )
    assert applied is True
    assert await store.apply_results(
        scope, op.claim_token, [], [op.id], operation_id
    ) is False
    completed_knowledge = (
        first_knowledge if op.knowledge_id == first_knowledge.id else second_knowledge
    )
    completed_duplicate = await store.enqueue(
        scope,
        completed_knowledge,
        {"knowledge_id": completed_knowledge.id},
        delay_seconds=0,
    )
    assert completed_duplicate.pending_op_id is None
    assert completed_duplicate.deduplicated is True

    async with postgres_factory() as session:
        pages = list(
            (
                await session.execute(
                    select(WikiPage).where(
                        WikiPage.tenant_id == scope.tenant_id,
                        WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    )
                )
            ).scalars()
        )
        assert {page.status for page in pages} == {"published"}
        source_page = next(page for page in pages if page.slug == "entity/source")
        assert source_page.version == 1
        assert source_page.aliases == ["Source"]
        assert source_page.source_refs == [op.knowledge_id]
        assert source_page.chunk_refs == ["chunk-1"]
        link = (
            await session.execute(
                select(WikiLink).where(WikiLink.source_page_id == source_page.id)
            )
        ).scalar_one()
        assert link.target_slug == "concept/target"
        assert link.target_page_id is not None
        old_incoming = (
            await session.execute(
                select(WikiLink)
                .join(WikiPage, WikiPage.id == WikiLink.source_page_id)
                .where(WikiPage.slug == "entity/old-source")
            )
        ).scalar_one()
        assert old_incoming.target_page_id == link.target_page_id
        assert (
            await session.execute(
                select(func.count(WikiLogEntry.id)).where(
                    WikiLogEntry.operation_id == operation_id
                )
            )
        ).scalar_one() == 1
        marker = (
            await session.execute(
                select(WikiFinalizationMarker).where(
                    WikiFinalizationMarker.knowledge_id == op.knowledge_id
                )
            )
        ).scalar_one()
        assert marker.released_at is not None
        assert (
            await session.execute(
                select(func.count(WikiPendingOp.id)).where(
                    WikiPendingOp.tenant_id == scope.tenant_id,
                    WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
                )
            )
        ).scalar_one() == 1
        assert (
            await session.execute(
                select(func.count(TaskOutbox.id)).where(
                    TaskOutbox.tenant_id == scope.tenant_id,
                    TaskOutbox.knowledge_base_id == scope.knowledge_base_id,
                )
            )
        ).scalar_one() == 3
        assert (
            await session.execute(
                select(func.count(TaskOutbox.id)).where(
                    TaskOutbox.tenant_id == scope.tenant_id,
                    TaskOutbox.knowledge_base_id == scope.knowledge_base_id,
                    TaskOutbox.sent_at.is_not(None),
                )
            )
        ).scalar_one() == 2

    class FailingFinalization(SqlFinalizationPort):
        async def release(self, session, request):
            raise RuntimeError("release failed")

    remaining = (await store.claim_pending(scope, 1, 600))[0]
    assert remaining.claim_token is not None
    rollback_operation_id = uuid4()
    rollback_store = SqlAlchemyIngestStore(postgres_factory, FailingFinalization())
    with pytest.raises(RuntimeError, match="release failed"):
        await rollback_store.apply_results(
            scope,
            remaining.claim_token,
            [
                ReducedPage(
                    slug=f"summary/{remaining.knowledge_id}",
                    title="应回滚",
                    page_type="summary",
                    content="不应保存",
                    summary="不应保存",
                    source_refs=[remaining.knowledge_id],
                    contributor_op_ids=[remaining.id],
                )
            ],
            [remaining.id],
            rollback_operation_id,
        )
    async with postgres_factory() as session:
        assert (
            await session.execute(
                select(WikiPage).where(
                    WikiPage.slug == f"summary/{remaining.knowledge_id}"
                )
            )
        ).scalar_one_or_none() is None
        assert (
            await session.execute(
                select(WikiLogEntry).where(
                    WikiLogEntry.operation_id == rollback_operation_id
                )
            )
        ).scalar_one_or_none() is None
        assert await session.get(WikiPendingOp, remaining.id) is not None
