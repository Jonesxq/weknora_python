from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.infrastructure.database.base import Base
from app.schemas.wiki.pages import WikiPageCreateRequest, WikiPageUpdateRequest
from app.wiki.errors import WikiNotFoundError, WikiVersionConflictError
from app.wiki.ingest.schemas import (
    BatchApplyRequest,
    ContributionDelta,
    OperationFailure,
    PageExpectation,
    PageMergeOutput,
    ReducedPage,
    SourceKnowledge,
    StoredContributionRecord,
    TopicCandidate,
    WikiWorkerOptions,
)
from app.wiki.ingest.store import (
    ClaimLost,
    InvariantError,
    PageConflict,
    SqlAlchemyIngestStore,
    SqlFinalizationPort,
    build_claim_recovery_dedup_key,
    build_dedup_candidate_statement,
    build_outbox_dedup_key,
)
from app.wiki.ingest.reduce_slug import reduce_slug
from app.wiki.ingest.worker import WikiIngestWorker
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
from app.wiki.tasks.locks import MemoryWikiLockManager

TEST_DATABASE_URL = os.getenv("GRAPH_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="未配置 GRAPH_TEST_POSTGRES_URL，不连接默认数据库或使用 SQLite 替代 PostgreSQL",
)


def _plan_nodes(value: object) -> list[dict[str, Any]]:
    payload = json.loads(value) if isinstance(value, str) else value
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
        raise AssertionError("EXPLAIN JSON 顶层结构无效")
    root = payload[0].get("Plan")
    if not isinstance(root, dict):
        raise AssertionError("EXPLAIN JSON 缺少 Plan")
    nodes: list[dict[str, Any]] = []

    def visit(node: dict[str, Any]) -> None:
        nodes.append(node)
        children = node.get("Plans", [])
        if not isinstance(children, list):
            raise AssertionError("EXPLAIN Plans 结构无效")
        for child in children:
            if not isinstance(child, dict):
                raise AssertionError("EXPLAIN child plan 结构无效")
            visit(child)

    visit(root)
    return nodes


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
    with pytest.raises(InvariantError, match="completed contributor"):
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
    fourth_operation_id = uuid4()
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
                operation_id=fourth_operation_id,
            ),
        )
        is True
    )
    async with postgres_factory() as session:
        fourth_row = await session.get(WikiPendingOp, fourth.id)
        assert fourth_row is not None
        assert fourth_row.fail_count == 4
        assert fourth_row.claim_token is None
        marker = (
            await session.execute(
                select(WikiFinalizationMarker).where(
                    WikiFinalizationMarker.tenant_id == scope.tenant_id,
                    WikiFinalizationMarker.knowledge_base_id == scope.knowledge_base_id,
                    WikiFinalizationMarker.knowledge_id == failing.id,
                    WikiFinalizationMarker.subtask_name == "wiki",
                )
            )
        ).scalar_one()
        assert marker.released_at is None
        recovery = (
            await session.execute(
                select(TaskOutbox).where(
                    TaskOutbox.dedup_key
                    == build_claim_recovery_dedup_key(scope, fourth.claim_token)
                )
            )
        ).scalar_one()
        assert recovery.sent_at is not None
        follow_up_key = build_outbox_dedup_key(
            scope.tenant_id,
            scope.knowledge_base_id,
            "wiki.batch.trigger",
            f"operation:{fourth_operation_id}",
            "follow-up",
        )
        follow_up = (
            await session.execute(
                select(TaskOutbox).where(TaskOutbox.dedup_key == follow_up_key)
            )
        ).scalar_one()
        assert follow_up.sent_at is None
        fourth_log = (
            await session.execute(
                select(WikiLogEntry).where(
                    WikiLogEntry.operation_id == fourth_operation_id
                )
            )
        ).scalar_one()
        assert fourth_log.result_outcome == {
            "completed_op_ids": [],
            "superseded_op_ids": [],
            "failed_op_ids": [str(fourth.id)],
        }

    fifth = (await store.claim_pending(scope, 1, 600))[0]
    assert fifth.id == fourth.id and fifth.claim_token is not None
    fifth_operation_id = uuid4()
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
                operation_id=fifth_operation_id,
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
        marker = (
            await session.execute(
                select(WikiFinalizationMarker).where(
                    WikiFinalizationMarker.tenant_id == scope.tenant_id,
                    WikiFinalizationMarker.knowledge_base_id == scope.knowledge_base_id,
                    WikiFinalizationMarker.knowledge_id == failing.id,
                    WikiFinalizationMarker.subtask_name == "wiki",
                )
            )
        ).scalar_one()
        assert marker.released_at is not None
        recovery = (
            await session.execute(
                select(TaskOutbox).where(
                    TaskOutbox.dedup_key
                    == build_claim_recovery_dedup_key(scope, fifth.claim_token)
                )
            )
        ).scalar_one()
        assert recovery.sent_at is not None
        fifth_follow_up_key = build_outbox_dedup_key(
            scope.tenant_id,
            scope.knowledge_base_id,
            "wiki.batch.trigger",
            f"operation:{fifth_operation_id}",
            "follow-up",
        )
        assert (
            await session.execute(
                select(TaskOutbox).where(TaskOutbox.dedup_key == fifth_follow_up_key)
            )
        ).scalar_one_or_none() is None
        fifth_log = (
            await session.execute(
                select(WikiLogEntry).where(
                    WikiLogEntry.operation_id == fifth_operation_id
                )
            )
        ).scalar_one()
        assert fifth_log.result_outcome == {
            "completed_op_ids": [],
            "superseded_op_ids": [],
            "failed_op_ids": [str(fifth.id)],
        }


@pytest.mark.asyncio
async def test_contribution_version_and_active_partial_uniqueness_are_enforced(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=31, knowledge_base_id=uuid4(), actor_id="task13")
    base = {
        "tenant_id": scope.tenant_id,
        "knowledge_base_id": scope.knowledge_base_id,
        "slug": "entity/constraint",
        "knowledge_id": "constraint-source",
        "page_type": "entity",
        "title": "Constraint",
        "content": "Body",
        "summary": "Summary",
        "aliases": [],
        "chunk_refs": [],
    }
    async with postgres_factory() as session, session.begin():
        session.add(
            WikiPageContribution(
                **base,
                op_version="v1",
                state="active",
            )
        )

    with pytest.raises(IntegrityError):
        async with postgres_factory() as session, session.begin():
            session.add(
                WikiPageContribution(
                    **base,
                    op_version="v1",
                    state="retract_pending",
                )
            )

    with pytest.raises(IntegrityError):
        async with postgres_factory() as session, session.begin():
            session.add(
                WikiPageContribution(
                    **base,
                    op_version="v2",
                    state="active",
                )
            )

    async with postgres_factory() as session, session.begin():
        session.add(
            WikiPageContribution(
                **base,
                op_version="v2",
                state="retract_pending",
            )
        )

    async with postgres_factory() as session:
        rows = list(
            (
                await session.execute(
                    select(WikiPageContribution)
                    .where(
                        WikiPageContribution.tenant_id == scope.tenant_id,
                        WikiPageContribution.knowledge_base_id
                        == scope.knowledge_base_id,
                    )
                    .order_by(WikiPageContribution.op_version)
                )
            ).scalars()
        )
        assert [(row.op_version, row.state) for row in rows] == [
            ("v1", "active"),
            ("v2", "retract_pending"),
        ]


async def _seed_dedup_pages(
    postgres_factory: async_sessionmaker[AsyncSession],
    scope: WikiScope,
    *,
    count: int,
) -> None:
    rows = [
        WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug=f"entity/acme-target-{index:03d}",
            title=f"Acme Target {index:03d}",
            page_type="entity",
            status="published",
            aliases=[f"Acme Alias {index:03d}"],
            wiki_path=f"/entity/acme-target-{index:03d}",
        )
        for index in range(count)
    ]
    rows.extend(
        [
            WikiPage(
                tenant_id=scope.tenant_id + 1,
                knowledge_base_id=scope.knowledge_base_id,
                slug="entity/wrong-tenant",
                title="Acme Target",
                page_type="entity",
                status="published",
                wiki_path="/entity/wrong-tenant",
            ),
            WikiPage(
                tenant_id=scope.tenant_id,
                knowledge_base_id=uuid4(),
                slug="entity/wrong-kb",
                title="Acme Target",
                page_type="entity",
                status="published",
                wiki_path="/entity/wrong-kb",
            ),
            WikiPage(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                slug="concept/wrong-type",
                title="Acme Target",
                page_type="concept",
                status="published",
                wiki_path="/concept/wrong-type",
            ),
            WikiPage(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                slug="entity/not-published",
                title="Acme Target",
                page_type="entity",
                status="draft",
                wiki_path="/entity/not-published",
            ),
            WikiPage(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                slug="entity/deleted",
                title="Acme Target",
                page_type="entity",
                status="published",
                wiki_path="/entity/deleted",
                deleted_at=datetime.now(UTC),
            ),
        ]
    )
    async with postgres_factory() as session, session.begin():
        session.add_all(rows)


@pytest.mark.asyncio
async def test_real_dedup_function_filters_scope_type_state_and_limits_top_twenty(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=32, knowledge_base_id=uuid4(), actor_id="task13")
    await _seed_dedup_pages(postgres_factory, scope, count=35)
    candidate = TopicCandidate(
        name="Acme Target",
        slug="entity/generated-acme",
        page_type="entity",
    )

    found = await SqlAlchemyIngestStore(
        postgres_factory, SqlFinalizationPort()
    ).find_dedup_candidates(scope, candidate, limit=20)

    assert len(found) == 20
    assert all(item.page_type == "entity" for item in found)
    assert all(item.slug.startswith("entity/acme-target-") for item in found)
    assert len({item.slug for item in found}) == 20


@pytest.mark.asyncio
async def test_real_dedup_explain_uses_partial_trigram_gist_index(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=33, knowledge_base_id=uuid4(), actor_id="task13")
    await _seed_dedup_pages(postgres_factory, scope, count=300)
    candidate = TopicCandidate(
        name="Acme Target",
        slug="entity/generated-explain",
        page_type="entity",
    )
    statement = build_dedup_candidate_statement(
        scope,
        candidate,
        limit=20,
        query_name=candidate.name,
    )
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    async with postgres_factory() as session, session.begin():
        await session.execute(text("ANALYZE wiki_pages"))
        await session.execute(text("SET LOCAL enable_seqscan = off"))
        explained = (
            await session.execute(text(f"EXPLAIN (FORMAT JSON) {sql}"))
        ).scalar_one()

    index_nodes = [
        node
        for node in _plan_nodes(explained)
        if node.get("Index Name") == "ix_wiki_pages_dedup_names_trgm"
    ]
    assert index_nodes, [
        {
            key: node.get(key)
            for key in ("Node Type", "Index Name", "Index Cond", "Filter", "Sort Key")
        }
        for node in _plan_nodes(explained)
    ]
    assert any("Index Scan" in str(node.get("Node Type")) for node in index_nodes)


@pytest.mark.asyncio
async def test_two_source_canonical_apply_and_outcome_replay_are_idempotent(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=34, knowledge_base_id=uuid4(), actor_id="task13")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    knowledge = [
        SourceKnowledge(
            id=source_id,
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            title=source_id,
            op_version="v1",
        )
        for source_id in ("source-a", "source-b")
    ]
    for item in knowledge:
        first = await store.enqueue_ingest(
            scope, item, {"knowledge_id": item.id}, delay_seconds=0
        )
        duplicate = await store.enqueue_ingest(
            scope, item, {"knowledge_id": item.id}, delay_seconds=0
        )
        assert duplicate.id == first.id
        assert duplicate.deduplicated

    claimed = await store.claim_pending(scope, 10, 600)
    assert len(claimed) == 2
    claim_token = claimed[0].claim_token
    assert claim_token is not None
    assert {item.claim_token for item in claimed} == {claim_token}
    deltas = tuple(
        ContributionDelta(
            pending_op_id=item.id,
            action="add",
            slug="entity/canonical",
            knowledge_id=item.knowledge_id,
            previous=None,
            current=StoredContributionRecord(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                slug="entity/canonical",
                knowledge_id=item.knowledge_id,
                op_version=item.op_version,
                page_type="entity",
                state="active",
                title=f"Contribution {item.knowledge_id}",
                content=f"Body {item.knowledge_id}",
                summary=f"Summary {item.knowledge_id}",
                aliases=(f"Alias {item.knowledge_id}",),
                chunk_refs=(f"chunk:{item.knowledge_id}",),
            ),
        )
        for item in claimed
    )

    class MergeModel:
        async def merge_page(self, request):
            assert [item.knowledge_id for item in request.contributions] == [
                "source-a",
                "source-b",
            ]
            return PageMergeOutput(
                headline="Canonical",
                markdown="Canonical body",
            )

    reduced = await reduce_slug(
        "entity/canonical",
        deltas,
        None,
        (),
        MergeModel(),  # type: ignore[arg-type]
    )
    operation_id = uuid4()
    request = BatchApplyRequest(
        claim_token=claim_token,
        pages=(reduced,),
        contribution_deltas=deltas,
        completed_op_ids=tuple(item.id for item in claimed),
        superseded_op_ids=(),
        failures=(),
        expected_pages=(PageExpectation(slug="entity/canonical"),),
        operation_id=operation_id,
    )

    outcome = await store.apply_results_with_outcome(scope, request)
    replay = await store.apply_results_with_outcome(scope, request)

    assert outcome.applied is True
    assert replay.applied is False
    assert replay.completed_op_ids == outcome.completed_op_ids
    assert replay.superseded_op_ids == replay.failed_op_ids == ()
    assert reduced.source_refs == ["source-a", "source-b"]
    assert reduced.chunk_refs == ["chunk:source-a", "chunk:source-b"]

    async with postgres_factory() as session:
        page = (
            await session.execute(
                select(WikiPage).where(
                    WikiPage.tenant_id == scope.tenant_id,
                    WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    WikiPage.slug == "entity/canonical",
                )
            )
        ).scalar_one()
        assert page.version == 1
        assert page.source_refs == ["source-a", "source-b"]
        assert (
            await session.execute(
                select(func.count(WikiPageContribution.id)).where(
                    WikiPageContribution.tenant_id == scope.tenant_id,
                    WikiPageContribution.knowledge_base_id == scope.knowledge_base_id,
                )
            )
        ).scalar_one() == 2
        log = (
            await session.execute(
                select(WikiLogEntry).where(WikiLogEntry.operation_id == operation_id)
            )
        ).scalar_one()
        assert log.result_outcome == {
            "completed_op_ids": [str(item.id) for item in claimed],
            "superseded_op_ids": [],
            "failed_op_ids": [],
        }
        assert (
            await session.execute(
                select(func.count(WikiLogEntry.id)).where(
                    WikiLogEntry.operation_id == operation_id
                )
            )
        ).scalar_one() == 1
        assert (
            await session.execute(
                select(func.count(WikiDeadLetter.id)).where(
                    WikiDeadLetter.tenant_id == scope.tenant_id,
                    WikiDeadLetter.knowledge_base_id == scope.knowledge_base_id,
                )
            )
        ).scalar_one() == 0
        assert (
            await session.execute(
                select(func.count(TaskOutbox.id)).where(
                    TaskOutbox.tenant_id == scope.tenant_id,
                    TaskOutbox.knowledge_base_id == scope.knowledge_base_id,
                )
            )
        ).scalar_one() == 3


@pytest.mark.asyncio
async def test_replace_and_retract_stale_roll_back_together_when_one_page_cas_fails(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=35, knowledge_base_id=uuid4(), actor_id="task13")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    knowledge = SourceKnowledge(
        id="versioned-source",
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        title="Versioned",
        op_version="v2",
    )
    await store.enqueue_ingest(
        scope, knowledge, {"knowledge_id": knowledge.id}, delay_seconds=0
    )
    pending = (await store.claim_pending(scope, 1, 600))[0]
    assert pending.claim_token is not None

    async with postgres_factory() as session, session.begin():
        retained_page = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/retained",
            title="Retained v1",
            page_type="entity",
            status="published",
            content="Retained old",
            summary="Retained old",
            source_refs=[knowledge.id],
            chunk_refs=["chunk:retained:v1"],
            wiki_path="/entity/retained",
        )
        stale_page = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="concept/stale",
            title="Stale v1",
            page_type="concept",
            status="published",
            content="Stale old",
            summary="Stale old",
            source_refs=[knowledge.id],
            chunk_refs=["chunk:stale:v1"],
            wiki_path="/concept/stale",
        )
        retained_contribution = WikiPageContribution(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug=retained_page.slug,
            knowledge_id=knowledge.id,
            op_version="v1",
            page_type="entity",
            state="active",
            title="Retained contribution v1",
            content="Retained contribution old",
            summary="Retained contribution old",
            aliases=[],
            chunk_refs=["chunk:retained:v1"],
        )
        stale_contribution = WikiPageContribution(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug=stale_page.slug,
            knowledge_id=knowledge.id,
            op_version="v1",
            page_type="concept",
            state="active",
            title="Stale contribution v1",
            content="Stale contribution old",
            summary="Stale contribution old",
            aliases=[],
            chunk_refs=["chunk:stale:v1"],
        )
        session.add_all(
            [
                retained_page,
                stale_page,
                retained_contribution,
                stale_contribution,
            ]
        )
        await session.flush()

    def record(row: WikiPageContribution) -> StoredContributionRecord:
        return StoredContributionRecord(
            id=row.id,
            tenant_id=row.tenant_id,
            knowledge_base_id=row.knowledge_base_id,
            slug=row.slug,
            knowledge_id=row.knowledge_id,
            op_version=row.op_version,
            page_type=row.page_type,
            state="active",
            title=row.title,
            content=row.content,
            summary=row.summary,
            chunk_refs=tuple(row.chunk_refs),
        )

    retained_previous = record(retained_contribution)
    stale_previous = record(stale_contribution)
    retained_current = retained_previous.model_copy(
        update={
            "id": None,
            "op_version": "v2",
            "title": "Retained contribution v2",
            "content": "Retained contribution new",
            "summary": "Retained contribution new",
            "chunk_refs": ("chunk:retained:v2",),
        }
    )
    operation_id = uuid4()
    request = BatchApplyRequest(
        claim_token=pending.claim_token,
        pages=(
            ReducedPage(
                slug=retained_page.slug,
                title="Retained v2",
                page_type="entity",
                content="Retained new",
                summary="Retained new",
                source_refs=[knowledge.id],
                chunk_refs=["chunk:retained:v2"],
                contributor_op_ids=[pending.id],
            ),
            ReducedPage(
                slug=stale_page.slug,
                title=stale_page.title,
                page_type="concept",
                content=stale_page.content,
                summary=stale_page.summary,
                contributor_op_ids=[pending.id],
                deleted=True,
            ),
        ),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=pending.id,
                action="replace",
                slug=retained_page.slug,
                knowledge_id=knowledge.id,
                previous=retained_previous,
                current=retained_current,
            ),
            ContributionDelta(
                pending_op_id=pending.id,
                action="retract_stale",
                slug=stale_page.slug,
                knowledge_id=knowledge.id,
                previous=stale_previous,
                current=None,
            ),
        ),
        completed_op_ids=(pending.id,),
        superseded_op_ids=(),
        failures=(),
        expected_pages=(
            PageExpectation(
                slug=retained_page.slug,
                page_id=retained_page.id,
                version=retained_page.version,
            ),
            PageExpectation(
                slug=stale_page.slug,
                page_id=stale_page.id,
                version=stale_page.version + 1,
            ),
        ),
        operation_id=operation_id,
    )

    with pytest.raises(PageConflict):
        await store.apply_results_with_outcome(scope, request)

    async with postgres_factory() as session:
        contributions = list(
            (
                await session.execute(
                    select(WikiPageContribution)
                    .where(
                        WikiPageContribution.tenant_id == scope.tenant_id,
                        WikiPageContribution.knowledge_base_id
                        == scope.knowledge_base_id,
                    )
                    .order_by(WikiPageContribution.slug)
                )
            ).scalars()
        )
        assert [(item.slug, item.op_version, item.state) for item in contributions] == [
            ("concept/stale", "v1", "active"),
            ("entity/retained", "v1", "active"),
        ]
        retained = await session.get(WikiPage, retained_page.id)
        stale = await session.get(WikiPage, stale_page.id)
        assert retained is not None and retained.content == "Retained old"
        assert stale is not None and stale.deleted_at is None
        assert retained.version == stale.version == 1
        pending_row = await session.get(WikiPendingOp, pending.id)
        assert (
            pending_row is not None and pending_row.claim_token == pending.claim_token
        )
        assert (
            await session.execute(
                select(func.count(WikiLogEntry.id)).where(
                    WikiLogEntry.operation_id == operation_id
                )
            )
        ).scalar_one() == 0


@pytest.mark.asyncio
async def test_retract_worker_minimally_cleans_unique_and_shared_source_pages(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=36, knowledge_base_id=uuid4(), actor_id="task13")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    async with postgres_factory() as session, session.begin():
        unique_page = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/unique-source",
            title="Unique",
            page_type="entity",
            status="published",
            content="Unique body",
            summary="Unique summary",
            source_refs=["source-a"],
            chunk_refs=["chunk:a:unique"],
            wiki_path="/entity/unique-source",
        )
        shared_page = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/shared-source",
            title="Shared",
            page_type="entity",
            status="published",
            content="Shared body",
            summary="Shared summary",
            source_refs=["source-a", "source-b"],
            chunk_refs=["chunk:a:shared", "chunk:b:shared"],
            wiki_path="/entity/shared-source",
        )
        session.add_all(
            [
                unique_page,
                shared_page,
                WikiPageContribution(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    slug=unique_page.slug,
                    knowledge_id="source-a",
                    op_version="v1",
                    page_type="entity",
                    state="active",
                    title="Unique A",
                    content="Unique A",
                    summary="Unique A",
                    aliases=[],
                    chunk_refs=["chunk:a:unique"],
                ),
                WikiPageContribution(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    slug=shared_page.slug,
                    knowledge_id="source-a",
                    op_version="v1",
                    page_type="entity",
                    state="active",
                    title="Shared A",
                    content="Shared A",
                    summary="Shared A",
                    aliases=[],
                    chunk_refs=["chunk:a:shared"],
                ),
                WikiPageContribution(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    slug=shared_page.slug,
                    knowledge_id="source-b",
                    op_version="v1",
                    page_type="entity",
                    state="active",
                    title="Shared B",
                    content="Shared B",
                    summary="Shared B",
                    aliases=[],
                    chunk_refs=["chunk:b:shared"],
                ),
            ]
        )
        await session.flush()

    enqueued = await store.enqueue_retract(
        scope,
        "source-a",
        "delete-v1",
        {"knowledge_id": "source-a"},
        delay_seconds=0,
    )
    assert enqueued.id is not None
    async with postgres_factory() as session:
        unique_after_enqueue = await session.get(WikiPage, unique_page.id)
        shared_after_enqueue = await session.get(WikiPage, shared_page.id)
        assert unique_after_enqueue is not None
        assert unique_after_enqueue.deleted_at is not None
        assert unique_after_enqueue.source_refs == []
        assert shared_after_enqueue is not None
        assert shared_after_enqueue.deleted_at is None
        assert shared_after_enqueue.source_refs == ["source-b"]
        assert shared_after_enqueue.chunk_refs == ["chunk:b:shared"]

    class NoReadSource:
        def __getattr__(self, name: str):
            raise AssertionError(f"retract worker 不应读取 source: {name}")

    class MergeModel:
        async def merge_page(self, request):
            assert request.slug == "entity/shared-source"
            assert [item.knowledge_id for item in request.contributions] == ["source-b"]
            return PageMergeOutput(
                headline="Shared B only",
                markdown="Shared B only body",
            )

    worker = WikiIngestWorker(
        store=store,
        locks=MemoryWikiLockManager(),
        source=NoReadSource(),  # type: ignore[arg-type]
        model=MergeModel(),  # type: ignore[arg-type]
        options=WikiWorkerOptions(),
        retry_wait=lambda _seconds: asyncio.sleep(0),
    )
    result = await worker.run_batch(scope)

    assert result.completed_op_ids == (enqueued.id,)
    assert result.failed_op_ids == result.superseded_op_ids == ()
    async with postgres_factory() as session:
        contributions = list(
            (
                await session.execute(
                    select(WikiPageContribution).where(
                        WikiPageContribution.tenant_id == scope.tenant_id,
                        WikiPageContribution.knowledge_base_id
                        == scope.knowledge_base_id,
                    )
                )
            ).scalars()
        )
        assert [
            (item.knowledge_id, item.slug, item.state) for item in contributions
        ] == [("source-b", "entity/shared-source", "active")]
        unique_final = await session.get(WikiPage, unique_page.id)
        shared_final = await session.get(WikiPage, shared_page.id)
        assert unique_final is not None and unique_final.deleted_at is not None
        assert shared_final is not None and shared_final.deleted_at is None
        assert shared_final.source_refs == ["source-b"]
        assert shared_final.content == "Shared B only body"
        assert await session.get(WikiPendingOp, enqueued.id) is None
        marker = (
            await session.execute(
                select(WikiFinalizationMarker).where(
                    WikiFinalizationMarker.tenant_id == scope.tenant_id,
                    WikiFinalizationMarker.knowledge_base_id == scope.knowledge_base_id,
                    WikiFinalizationMarker.knowledge_id == "source-a",
                    WikiFinalizationMarker.subtask_name == "wiki-retract",
                )
            )
        ).scalar_one()
        assert marker.released_at is not None
