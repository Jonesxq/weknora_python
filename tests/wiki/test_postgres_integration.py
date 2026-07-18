from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.infrastructure.database.base import Base
from app.schemas.wiki.pages import WikiPageCreateRequest, WikiPageUpdateRequest
from app.wiki.errors import WikiNotFoundError, WikiVersionConflictError
from app.wiki.ingest.schemas import (
    BatchApplyRequest,
    ContributionDelta,
    OperationFailure,
    PageExpectation,
    ReducedPage,
    SourceKnowledge,
    StoredContributionRecord,
)
from app.wiki.ingest.store import (
    ClaimLost,
    InvariantError,
    PageConflict,
    SqlAlchemyIngestStore,
    SqlFinalizationPort,
    build_claim_recovery_dedup_key,
)
from app.wiki.models import (
    TaskOutbox,
    WikiFinalizationMarker,
    WikiDeadLetter,
    WikiLink,
    WikiLogEntry,
    WikiPage,
    WikiPageContribution,
    WikiPendingOp,
)
from app.wiki.page_service import WikiPageService
from app.wiki.query_service import WikiQueryService
from app.wiki.scope import WikiScope
from app.wiki.sql_page_store import SqlAlchemyPageStore

TEST_DATABASE_URL = os.getenv("GRAPH_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="未配置 GRAPH_TEST_POSTGRES_URL，不连接默认数据库或使用 SQLite 替代 PostgreSQL",
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

    empty_operation_id = uuid4()
    assert await store.apply_results(scope, None, [], [], empty_operation_id) is False
    assert await store.apply_results(scope, None, [], [], empty_operation_id) is False
    with pytest.raises(InvariantError, match="不能提交结果页面"):
        await store.apply_results(
            scope,
            uuid4(),
            [
                ReducedPage(
                    slug="concept/invalid-empty-batch",
                    title="非法空完成批次",
                    page_type="concept",
                    content="不应写入",
                    summary="不应写入",
                )
            ],
            [],
            uuid4(),
        )

    outbox = await store.claim_outbox(10, 600)
    assert len(outbox) == 2
    outbox_token = outbox[0].claim_token
    assert outbox_token is not None
    assert {event.claim_token for event in outbox} == {outbox_token}
    assert {event.attempts for event in outbox} == {1}
    with pytest.raises(ClaimLost, match="token"):
        await store.mark_outbox_sent([event.id for event in outbox], uuid4())
    await store.release_outbox([event.id for event in outbox], outbox_token)
    retried_outbox = await store.claim_outbox(10, 600)
    assert {event.id for event in retried_outbox} == {event.id for event in outbox}
    assert {event.attempts for event in retried_outbox} == {2}
    retry_token = retried_outbox[0].claim_token
    assert retry_token is not None
    await store.mark_outbox_sent([event.id for event in retried_outbox], retry_token)

    failed_claim = (await store.claim_pending(scope, 1, 600))[0]
    assert failed_claim.claim_token is not None
    assert (
        await store.apply_results(
            scope,
            failed_claim.claim_token,
            [],
            [],
            uuid4(),
            failed_op_ids=[failed_claim.id],
            expected_pages={},
        )
        is True
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
        expected_pages={"entity/source": None, "concept/target": None},
    )
    assert applied is True
    assert (
        await store.apply_results(scope, op.claim_token, [], [op.id], operation_id)
        is False
    )
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
        recovery = (
            await session.execute(
                select(TaskOutbox).where(
                    TaskOutbox.dedup_key
                    == build_claim_recovery_dedup_key(scope, op.claim_token)
                )
            )
        ).scalar_one()
        assert recovery.sent_at is not None
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
        assert source_page.wiki_path == "/entity/source"
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
        ).scalar_one() == 6
        assert (
            await session.execute(
                select(func.count(TaskOutbox.id)).where(
                    TaskOutbox.tenant_id == scope.tenant_id,
                    TaskOutbox.knowledge_base_id == scope.knowledge_base_id,
                    TaskOutbox.sent_at.is_not(None),
                )
            )
        ).scalar_one() == 4

    class MissingFinalization(SqlFinalizationPort):
        async def release(self, session, request):
            return False

    remaining = (await store.claim_pending(scope, 1, 600))[0]
    assert remaining.claim_token is not None
    rollback_operation_id = uuid4()
    rollback_store = SqlAlchemyIngestStore(postgres_factory, MissingFinalization())
    with pytest.raises(InvariantError, match="finalization"):
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
            expected_pages={f"summary/{remaining.knowledge_id}": None},
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


@pytest.mark.asyncio
async def test_ingest_concurrent_enqueue_claim_stale_recovery_and_atomic_release(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=17, knowledge_base_id=uuid4(), actor_id="worker")
    store_a = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    store_b = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    first_knowledge = SourceKnowledge(
        id="concurrent-1",
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        title="并发文档一",
        op_version="version-1",
    )
    results = await asyncio.gather(
        *(
            store_a.enqueue(
                scope,
                first_knowledge,
                {"knowledge_id": first_knowledge.id},
                delay_seconds=0,
            )
            for _ in range(5)
        )
    )
    assert len({result.pending_op_id for result in results}) == 1
    assert sum(not result.deduplicated for result in results) == 1
    async with postgres_factory() as session:
        assert (
            await session.execute(select(func.count(WikiPendingOp.id)))
        ).scalar_one() == 1
        assert (
            await session.execute(select(func.count(WikiFinalizationMarker.id)))
        ).scalar_one() == 1
        assert (
            await session.execute(select(func.count(TaskOutbox.id)))
        ).scalar_one() == 1

    second_knowledge = first_knowledge.model_copy(
        update={"id": "concurrent-2", "title": "并发文档二"}
    )
    await store_a.enqueue(
        scope,
        second_knowledge,
        {"knowledge_id": second_knowledge.id},
        delay_seconds=0,
    )
    claimed_a, claimed_b = await asyncio.gather(
        store_a.claim_pending(scope, 1, 600),
        store_b.claim_pending(scope, 1, 600),
    )
    assert len(claimed_a) == len(claimed_b) == 1
    assert claimed_a[0].id != claimed_b[0].id
    assert claimed_a[0].claim_token is not None
    assert claimed_b[0].claim_token is not None
    old_recovery_key = build_claim_recovery_dedup_key(scope, claimed_a[0].claim_token)
    async with postgres_factory() as session:
        recovery = (
            await session.execute(
                select(TaskOutbox).where(TaskOutbox.dedup_key == old_recovery_key)
            )
        ).scalar_one()
        assert recovery.sent_at is None
        assert recovery.available_at >= claimed_a[0].claimed_at + timedelta(seconds=600)

    stale = claimed_a[0]
    async with postgres_factory() as session, session.begin():
        await session.execute(
            update(WikiPendingOp)
            .where(WikiPendingOp.id == stale.id)
            .values(claimed_at=datetime.now(UTC) - timedelta(hours=1))
        )
    reclaimed = (await store_b.claim_pending(scope, 1, 60))[0]
    assert reclaimed.id == stale.id
    assert reclaimed.claim_token not in {None, stale.claim_token}
    assert reclaimed.claim_token is not None
    async with postgres_factory() as session:
        old_recovery = (
            await session.execute(
                select(TaskOutbox).where(TaskOutbox.dedup_key == old_recovery_key)
            )
        ).scalar_one()
        new_recovery = (
            await session.execute(
                select(TaskOutbox).where(
                    TaskOutbox.dedup_key
                    == build_claim_recovery_dedup_key(scope, reclaimed.claim_token)
                )
            )
        ).scalar_one()
        assert old_recovery.sent_at is not None
        assert new_recovery.sent_at is None

    invalid_id = uuid4()
    with pytest.raises(ClaimLost):
        await store_b.release_failed(
            scope, [reclaimed.id, invalid_id], reclaimed.claim_token
        )
    async with postgres_factory() as session:
        row = await session.get(WikiPendingOp, reclaimed.id)
        assert row is not None
        assert row.fail_count == 0
        assert row.claim_token == reclaimed.claim_token


@pytest.mark.asyncio
async def test_ingest_page_snapshot_cas_rejects_concurrent_edit_and_create(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=18, knowledge_base_id=uuid4(), actor_id="worker")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    knowledge = SourceKnowledge(
        id="cas-source",
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        title="CAS 文档",
        op_version="version-1",
    )
    await store.enqueue(
        scope, knowledge, {"knowledge_id": knowledge.id}, delay_seconds=0
    )
    pending = (await store.claim_pending(scope, 1, 600))[0]
    assert pending.claim_token is not None
    async with postgres_factory() as session, session.begin():
        page = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/cas",
            title="旧标题",
            page_type="entity",
            status="published",
            content="旧正文 [[concept/old]]",
            summary="旧摘要",
            wiki_path="/entity/cas",
        )
        session.add(page)
        await session.flush()
        session.add(
            WikiLink(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                source_page_id=page.id,
                target_slug="concept/old",
            )
        )
    expected = await store.find_existing_pages(scope, ["entity/cas"])
    async with postgres_factory() as session, session.begin():
        await session.execute(
            update(WikiPage)
            .where(WikiPage.slug == "entity/cas")
            .values(content="人工并发编辑", version=WikiPage.version + 1)
        )
    with pytest.raises(PageConflict):
        await store.apply_results(
            scope,
            pending.claim_token,
            [
                ReducedPage(
                    slug="entity/cas",
                    title="模型标题",
                    page_type="entity",
                    content="模型正文 [[concept/new]]",
                    summary="模型摘要",
                    contributor_op_ids=[pending.id],
                )
            ],
            [pending.id],
            uuid4(),
            expected_pages={"entity/cas": expected["entity/cas"]},
        )
    async with postgres_factory() as session:
        page = (
            await session.execute(select(WikiPage).where(WikiPage.slug == "entity/cas"))
        ).scalar_one()
        assert page.content == "人工并发编辑"
        assert page.version == 2
        assert (
            await session.execute(
                select(WikiLink.target_slug).where(WikiLink.source_page_id == page.id)
            )
        ).scalar_one() == "concept/old"
        pending_row = await session.get(WikiPendingOp, pending.id)
        assert pending_row is not None
        assert pending_row.claim_token == pending.claim_token

    async with postgres_factory() as session, session.begin():
        session.add(
            WikiPage(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                slug="concept/concurrent-new",
                title="并发新建",
                page_type="concept",
                status="published",
                wiki_path="/concept/concurrent-new",
            )
        )
    with pytest.raises(PageConflict):
        await store.apply_results(
            scope,
            pending.claim_token,
            [
                ReducedPage(
                    slug="concept/concurrent-new",
                    title="模型新建",
                    page_type="concept",
                    content="模型正文",
                    summary="模型摘要",
                    contributor_op_ids=[pending.id],
                )
            ],
            [pending.id],
            uuid4(),
            expected_pages={"concept/concurrent-new": None},
        )


@pytest.mark.asyncio
async def test_modern_results_replace_contribution_and_dead_letter_on_fifth_failure(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=19, knowledge_base_id=uuid4(), actor_id="worker")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    knowledge = SourceKnowledge(
        id="replace-source",
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        title="替换来源",
        op_version="version-2",
    )
    await store.enqueue_ingest(
        scope, knowledge, {"knowledge_id": knowledge.id}, delay_seconds=0
    )
    pending = (await store.claim_pending(scope, 1, 600))[0]
    assert pending.claim_token is not None

    async with postgres_factory() as session, session.begin():
        page = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/replace",
            title="旧页面",
            page_type="entity",
            status="published",
            content="旧正文",
            summary="旧摘要",
            aliases=["旧别名"],
            source_refs=[knowledge.id],
            chunk_refs=["chunk:old"],
            wiki_path="/entity/replace",
        )
        previous = WikiPageContribution(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug=page.slug,
            knowledge_id=knowledge.id,
            op_version="version-1",
            page_type="entity",
            state="active",
            title="旧贡献",
            content="旧贡献正文",
            summary="旧贡献摘要",
            aliases=[],
            chunk_refs=["chunk:old"],
        )
        session.add_all([page, previous])
        await session.flush()

    previous_record = StoredContributionRecord(
        id=previous.id,
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        slug=previous.slug,
        knowledge_id=previous.knowledge_id,
        op_version=previous.op_version,
        page_type="entity",
        state="active",
        title=previous.title,
        content=previous.content,
        summary=previous.summary,
        aliases=(),
        chunk_refs=("chunk:old",),
    )
    current_record = previous_record.model_copy(
        update={
            "id": None,
            "op_version": pending.op_version,
            "title": "新贡献",
            "content": "新贡献正文",
            "summary": "新贡献摘要",
            "chunk_refs": ("chunk:new",),
        }
    )
    reduced = ReducedPage(
        slug=page.slug,
        title="新页面",
        page_type="entity",
        content="新正文",
        summary="新摘要",
        aliases=["新别名"],
        source_refs=[knowledge.id],
        chunk_refs=["chunk:new"],
        contributor_op_ids=[pending.id],
    )
    assert (
        await store.apply_results(
            scope,
            BatchApplyRequest(
                claim_token=pending.claim_token,
                pages=(reduced,),
                contribution_deltas=(
                    ContributionDelta(
                        pending_op_id=pending.id,
                        action="replace",
                        slug=page.slug,
                        knowledge_id=knowledge.id,
                        previous=previous_record,
                        current=current_record,
                    ),
                ),
                completed_op_ids=(pending.id,),
                superseded_op_ids=(),
                failures=(),
                expected_pages=(
                    PageExpectation(
                        slug=page.slug, page_id=page.id, version=page.version
                    ),
                ),
                operation_id=uuid4(),
            ),
        )
        is True
    )

    async with postgres_factory() as session:
        contributions = list(
            (
                await session.execute(
                    select(WikiPageContribution).where(
                        WikiPageContribution.tenant_id == scope.tenant_id,
                        WikiPageContribution.knowledge_base_id
                        == scope.knowledge_base_id,
                        WikiPageContribution.slug == page.slug,
                    )
                )
            ).scalars()
        )
        assert len(contributions) == 1
        assert contributions[0].op_version == pending.op_version
        persisted_page = await session.get(WikiPage, page.id)
        assert persisted_page is not None
        assert persisted_page.title == "新页面"
        assert persisted_page.version == 2

    failing = knowledge.model_copy(
        update={"id": "failing-source", "title": "失败来源", "op_version": "version-1"}
    )
    failed_enqueue = await store.enqueue_ingest(
        scope, failing, {"knowledge_id": failing.id}, delay_seconds=0
    )
    assert failed_enqueue.id is not None
    async with postgres_factory() as session, session.begin():
        await session.execute(
            update(WikiPendingOp)
            .where(WikiPendingOp.id == failed_enqueue.id)
            .values(fail_count=3)
        )
    fourth = (await store.claim_pending(scope, 1, 600))[0]
    assert fourth.claim_token is not None
    assert (
        await store.apply_results(
            scope,
            BatchApplyRequest(
                claim_token=fourth.claim_token,
                pages=(),
                contribution_deltas=(),
                completed_op_ids=(),
                superseded_op_ids=(),
                failures=(
                    OperationFailure(
                        pending_op_id=fourth.id,
                        error_code="MODEL_TEMPORARY",
                        error_summary="第 4 次失败",
                    ),
                ),
                expected_pages=(),
                operation_id=uuid4(),
            ),
        )
        is True
    )
    async with postgres_factory() as session:
        fourth_row = await session.get(WikiPendingOp, fourth.id)
        assert fourth_row is not None
        assert fourth_row.fail_count == 4
        assert fourth_row.claim_token is None

    fifth = (await store.claim_pending(scope, 1, 600))[0]
    assert fifth.id == fourth.id and fifth.claim_token is not None
    assert (
        await store.apply_results(
            scope,
            BatchApplyRequest(
                claim_token=fifth.claim_token,
                pages=(),
                contribution_deltas=(),
                completed_op_ids=(),
                superseded_op_ids=(),
                failures=(
                    OperationFailure(
                        pending_op_id=fifth.id,
                        error_code="MODEL_PERMANENT",
                        error_summary="第 5 次失败",
                    ),
                ),
                expected_pages=(),
                operation_id=uuid4(),
            ),
        )
        is True
    )
    dead_letters = await store.list_dead_letters(scope)
    assert len(dead_letters) == 1
    assert dead_letters[0].pending_op_id == fifth.id
    assert dead_letters[0].fail_count == 5
    assert dict(dead_letters[0].payload) == {"knowledge_id": failing.id}
    async with postgres_factory() as session:
        assert await session.get(WikiPendingOp, fifth.id) is None
        assert (
            await session.execute(
                select(func.count(WikiDeadLetter.id)).where(
                    WikiDeadLetter.pending_op_id == fifth.id
                )
            )
        ).scalar_one() == 1
