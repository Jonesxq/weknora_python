from __future__ import annotations

import ast
import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from random import Random
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.infrastructure.database.base import Base
from app.schemas.wiki.pages import WikiPageCreateRequest, WikiPageUpdateRequest
from app.wiki.errors import WikiNotFoundError, WikiVersionConflictError
from app.wiki.ingest.enqueue import WikiEnqueueService
from app.wiki.ingest.fakes import (
    FakeChatModel,
    FakeEmbeddingModel,
    load_fake_runtime_adapters,
)
from app.wiki.ingest.schemas import (
    BatchApplyOutcome,
    BatchApplyRequest,
    ContributionDelta,
    FolderAssignment,
    OperationFailure,
    PageExpectation,
    PageMergeOutput,
    ReducedPage,
    SourceKnowledge,
    StoredContributionRecord,
    TopicCandidate,
    WikiWorkerOptions,
)
from app.wiki.ingest.ports import PermanentModelError
from app.wiki.ingest.store import (
    ClaimLost,
    InvariantError,
    PageConflict,
    PendingOpRecord,
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
    WikiFolder,
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
POSTGRES_SKIP_REASON = (
    "未配置 GRAPH_TEST_POSTGRES_URL，不连接默认数据库或使用 SQLite 替代 PostgreSQL"
)


class NeverEmbedding:
    async def embed(self, _request):
        raise AssertionError("retract worker 不应调用 embedding")


def test_postgres_worker_constructions_supply_embedding_model() -> None:
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    worker_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "WikiIngestWorker"
    ]

    assert len(worker_calls) == 4
    assert [
        node.lineno
        for node in worker_calls
        if not any(keyword.arg == "embedding_model" for keyword in node.keywords)
    ] == []


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


async def _drop_test_schema(engine: AsyncEngine, schema: str) -> None:
    async with engine.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


async def _cleanup_postgres_engine(
    engine: AsyncEngine, schema: str, *, schema_created: bool
) -> None:
    try:
        if schema_created:
            await _drop_test_schema(engine, schema)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_postgres_cleanup_disposes_when_schema_setup_never_completed() -> None:
    class Engine:
        def __init__(self) -> None:
            self.begin_calls = 0
            self.dispose_calls = 0

        def begin(self):
            self.begin_calls += 1
            raise AssertionError("schema 未创建时不应执行 DROP")

        async def dispose(self) -> None:
            self.dispose_calls += 1

    engine = Engine()

    await _cleanup_postgres_engine(engine, "not-created", schema_created=False)

    assert engine.begin_calls == 0
    assert engine.dispose_calls == 1


@pytest.mark.asyncio
async def test_postgres_cleanup_disposes_when_drop_schema_fails() -> None:
    class Connection:
        async def execute(self, _statement) -> None:
            raise RuntimeError("drop failed")

    class Begin:
        async def __aenter__(self) -> Connection:
            return Connection()

        async def __aexit__(self, *_args) -> None:
            return None

    class Engine:
        def __init__(self) -> None:
            self.dispose_calls = 0

        def begin(self) -> Begin:
            return Begin()

        async def dispose(self) -> None:
            self.dispose_calls += 1

    engine = Engine()

    with pytest.raises(RuntimeError, match="drop failed"):
        await _cleanup_postgres_engine(engine, "created", schema_created=True)

    assert engine.dispose_calls == 1


@pytest_asyncio.fixture
async def postgres_session() -> AsyncSession:
    if TEST_DATABASE_URL is None:
        pytest.skip(POSTGRES_SKIP_REASON)
    schema = f"wiki_test_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": f"{schema},public"}},
    )
    schema_created = False
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            schema_created = True
            await connection.run_sync(Base.metadata.create_all)

        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            yield session
    finally:
        await _cleanup_postgres_engine(
            engine, schema, schema_created=schema_created
        )


@pytest_asyncio.fixture
async def postgres_factory() -> async_sessionmaker[AsyncSession]:
    """为自行管理短 session 的摄取仓储提供真实 PostgreSQL 工厂。"""

    if TEST_DATABASE_URL is None:
        pytest.skip(POSTGRES_SKIP_REASON)
    schema = f"wiki_ingest_test_{uuid4().hex}"
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"server_settings": {"search_path": f"{schema},public"}},
    )
    schema_created = False
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            schema_created = True
            await connection.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
        yield factory
    finally:
        await _cleanup_postgres_engine(
            engine, schema, schema_created=schema_created
        )


class RecordingSqlAlchemyIngestStore(SqlAlchemyIngestStore):
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        finalization: SqlFinalizationPort,
    ) -> None:
        super().__init__(factory, finalization)
        self.last_request: BatchApplyRequest | None = None

    async def apply_results_with_outcome(
        self,
        scope: WikiScope,
        request: BatchApplyRequest,
    ) -> BatchApplyOutcome:
        snapshot = BatchApplyRequest.model_validate(request.model_dump(mode="python"))
        self.last_request = snapshot
        return await super().apply_results_with_outcome(scope, snapshot)


async def _real_taxonomy_worker(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> tuple[
    WikiScope,
    RecordingSqlAlchemyIngestStore,
    WikiIngestWorker,
    FakeChatModel,
    FakeEmbeddingModel,
    UUID,
]:
    source, model, embedding = load_fake_runtime_adapters(
        Path("examples/wiki_fake_data.json")
    )
    scope = WikiScope(
        tenant_id=1,
        knowledge_base_id=UUID("11111111-1111-1111-1111-111111111111"),
        actor_id="task11",
    )
    store = RecordingSqlAlchemyIngestStore(
        postgres_factory, SqlFinalizationPort()
    )
    enqueued = await WikiEnqueueService(source, store).enqueue(scope, "knowledge-1")
    assert enqueued.pending_op_id is not None

    async def retry_wait(_seconds: int) -> None:
        return None

    worker = WikiIngestWorker(
        store=store,
        locks=MemoryWikiLockManager(),
        source=source,
        model=model,
        embedding_model=embedding,
        options=WikiWorkerOptions(),
        retry_wait=retry_wait,
    )
    return scope, store, worker, model, embedding, enqueued.pending_op_id


@pytest.mark.asyncio
async def test_real_worker_applies_taxonomy_once_and_replay_is_idempotent(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope, store, worker, model, embedding, op_id = await _real_taxonomy_worker(
        postgres_factory
    )

    result = await worker.run_batch(scope)

    assert result.completed_op_ids == (op_id,)
    assert result.failed_op_ids == result.superseded_op_ids == ()
    assert len(model.taxonomy_requests) == 1
    assert embedding.calls == []
    request = store.last_request
    assert request is not None

    async with postgres_factory() as session:
        pages = {
            page.slug: page
            for page in (
                await session.execute(
                    select(WikiPage).where(
                        WikiPage.tenant_id == scope.tenant_id,
                        WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    )
                )
            ).scalars()
        }
        folders = {
            folder.path: folder
            for folder in (
                await session.execute(
                    select(WikiFolder).where(
                        WikiFolder.tenant_id == scope.tenant_id,
                        WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                    )
                )
            ).scalars()
        }
        versions = {slug: page.version for slug, page in pages.items()}
        folder_count = len(folders)

    organizations = folders["/Organizations"]
    products = folders["/Organizations/Products"]
    assert products.parent_id == organizations.id
    assert pages["entity/acme"].folder_id == products.id
    assert pages["entity/acme"].category_path == ["Organizations", "Products"]
    assert pages["entity/acme"].wiki_path == "/Organizations/Products/entity/acme"
    assert pages["entity/acme"].depth == 2
    assert pages["concept/retrieval"].folder_id is None
    assert pages["concept/retrieval"].category_path == []
    assert pages["concept/retrieval"].wiki_path == "/concept/retrieval"
    assert pages["concept/retrieval"].depth == 0
    assert folder_count == 2

    replay = await store.apply_results_with_outcome(scope, request)

    assert replay.applied is False
    assert replay.completed_op_ids == (op_id,)
    assert replay.failed_op_ids == ()
    assert replay.superseded_op_ids == ()
    async with postgres_factory() as session:
        replayed_versions = dict(
            (
                await session.execute(
                    select(WikiPage.slug, WikiPage.version).where(
                        WikiPage.tenant_id == scope.tenant_id,
                        WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    )
                )
            ).all()
        )
        replayed_folder_count = await session.scalar(
            select(func.count(WikiFolder.id)).where(
                WikiFolder.tenant_id == scope.tenant_id,
                WikiFolder.knowledge_base_id == scope.knowledge_base_id,
            )
        )
    assert replayed_versions == versions
    assert replayed_folder_count == folder_count


async def _claim_taxonomy_ingest(
    store: SqlAlchemyIngestStore,
    scope: WikiScope,
    *,
    knowledge_id: str,
) -> PendingOpRecord:
    knowledge = SourceKnowledge(
        id=knowledge_id,
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        title=knowledge_id,
        op_version="version-1",
    )
    await store.enqueue_ingest(
        scope, knowledge, {"knowledge_id": knowledge.id}, delay_seconds=0
    )
    pending = (await store.claim_pending(scope, 1, 600))[0]
    assert pending.claim_token is not None
    return pending


def _taxonomy_apply_request(
    pending: PendingOpRecord,
    page: ReducedPage,
    *,
    expected: PageExpectation | None = None,
    folder_assignments: tuple[FolderAssignment, ...] = (),
    operation_id=None,
) -> BatchApplyRequest:
    current = StoredContributionRecord(
        tenant_id=pending.tenant_id,
        knowledge_base_id=pending.knowledge_base_id,
        slug=page.slug,
        knowledge_id=pending.knowledge_id,
        op_version=pending.op_version,
        page_type=page.page_type,
        state="active",
        title=page.title,
        content=page.content,
        summary=page.summary,
        aliases=tuple(page.aliases),
        chunk_refs=tuple(page.chunk_refs),
    )
    return BatchApplyRequest(
        claim_token=pending.claim_token,
        pages=(page,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=pending.id,
                action="add",
                slug=page.slug,
                knowledge_id=pending.knowledge_id,
                previous=None,
                current=current,
            ),
        ),
        completed_op_ids=(pending.id,),
        superseded_op_ids=(),
        failures=(),
        expected_pages=(expected or PageExpectation(slug=page.slug),),
        operation_id=operation_id or uuid4(),
        folder_assignments=folder_assignments,
    )


def _taxonomy_multi_apply_request(
    pending: PendingOpRecord,
    pages: tuple[ReducedPage, ...],
    assignments: tuple[FolderAssignment, ...],
) -> BatchApplyRequest:
    deltas = tuple(
        ContributionDelta(
            pending_op_id=pending.id,
            action="add",
            slug=page.slug,
            knowledge_id=pending.knowledge_id,
            previous=None,
            current=StoredContributionRecord(
                tenant_id=pending.tenant_id,
                knowledge_base_id=pending.knowledge_base_id,
                slug=page.slug,
                knowledge_id=pending.knowledge_id,
                op_version=pending.op_version,
                page_type=page.page_type,
                state="active",
                title=page.title,
                content=page.content,
                summary=page.summary,
                aliases=tuple(page.aliases),
                chunk_refs=tuple(page.chunk_refs),
            ),
        )
        for page in pages
    )
    return BatchApplyRequest(
        claim_token=pending.claim_token,
        pages=pages,
        contribution_deltas=deltas,
        completed_op_ids=(pending.id,),
        superseded_op_ids=(),
        failures=(),
        expected_pages=tuple(PageExpectation(slug=page.slug) for page in pages),
        operation_id=uuid4(),
        folder_assignments=assignments,
    )


@pytest.mark.asyncio
async def test_concurrent_crossed_folder_assignments_are_serialized_per_scope(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=50, knowledge_base_id=uuid4(), actor_id="task8-lock")
    async with postgres_factory() as session, session.begin():
        folder_a = WikiFolder(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            parent_id=None,
            name="A",
            path="/A",
            depth=1,
        )
        folder_c = WikiFolder(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            parent_id=None,
            name="C",
            path="/C",
            depth=1,
        )
        session.add_all([folder_a, folder_c])
        await session.flush()
        folder_a_id, folder_c_id = folder_a.id, folder_c.id

    class DelayingStore(SqlAlchemyIngestStore):
        def __init__(self) -> None:
            super().__init__(postgres_factory, SqlFinalizationPort())
            self.resolve_count = 0

        async def _resolve_folder_assignment(
            self,
            session: AsyncSession,
            actual_scope: WikiScope,
            assignment: FolderAssignment,
        ):
            placement = await super()._resolve_folder_assignment(
                session, actual_scope, assignment
            )
            self.resolve_count += 1
            if self.resolve_count == 1:
                await asyncio.sleep(0.2)
            return placement

    first_store = DelayingStore()
    second_store = DelayingStore()
    first_pending = await _claim_taxonomy_ingest(
        first_store, scope, knowledge_id="cross-lock-first"
    )
    second_pending = await _claim_taxonomy_ingest(
        second_store, scope, knowledge_id="cross-lock-second"
    )
    first_pages = tuple(
        ReducedPage(
            slug=slug,
            title=slug,
            page_type="entity",
            content=slug,
            summary=slug,
            contributor_op_ids=[first_pending.id],
        )
        for slug in ("entity/a-first", "entity/z-first")
    )
    second_pages = tuple(
        ReducedPage(
            slug=slug,
            title=slug,
            page_type="entity",
            content=slug,
            summary=slug,
            contributor_op_ids=[second_pending.id],
        )
        for slug in ("entity/a-second", "entity/z-second")
    )
    first_request = _taxonomy_multi_apply_request(
        first_pending,
        first_pages,
        (
            FolderAssignment(
                slug=first_pages[0].slug,
                contributor_op_ids=(first_pending.id,),
                base_folder_id=folder_a_id,
                base_path="/A",
                base_depth=1,
            ),
            FolderAssignment(
                slug=first_pages[1].slug,
                contributor_op_ids=(first_pending.id,),
                base_folder_id=folder_c_id,
                base_path="/C",
                base_depth=1,
            ),
        ),
    )
    second_request = _taxonomy_multi_apply_request(
        second_pending,
        second_pages,
        (
            FolderAssignment(
                slug=second_pages[0].slug,
                contributor_op_ids=(second_pending.id,),
                base_folder_id=folder_c_id,
                base_path="/C",
                base_depth=1,
            ),
            FolderAssignment(
                slug=second_pages[1].slug,
                contributor_op_ids=(second_pending.id,),
                base_folder_id=folder_a_id,
                base_path="/A",
                base_depth=1,
            ),
        ),
    )

    outcomes = await asyncio.wait_for(
        asyncio.gather(
            first_store.apply_results_with_outcome(scope, first_request),
            second_store.apply_results_with_outcome(scope, second_request),
        ),
        timeout=10,
    )
    assert all(outcome.applied for outcome in outcomes)

    async with postgres_factory() as session:
        pages = {
            page.slug: page
            for page in (
                await session.execute(
                    select(WikiPage).where(
                        WikiPage.tenant_id == scope.tenant_id,
                        WikiPage.knowledge_base_id == scope.knowledge_base_id,
                        WikiPage.slug.in_(
                            [page.slug for page in (*first_pages, *second_pages)]
                        ),
                    )
                )
            ).scalars()
        }
        expected = {
            first_pages[0].slug: (folder_a_id, ["A"], f"/A/{first_pages[0].slug}"),
            first_pages[1].slug: (folder_c_id, ["C"], f"/C/{first_pages[1].slug}"),
            second_pages[0].slug: (
                folder_c_id,
                ["C"],
                f"/C/{second_pages[0].slug}",
            ),
            second_pages[1].slug: (
                folder_a_id,
                ["A"],
                f"/A/{second_pages[1].slug}",
            ),
        }
        for slug, (folder_id, category_path, wiki_path) in expected.items():
            assert pages[slug].folder_id == folder_id
            assert pages[slug].category_path == category_path
            assert pages[slug].wiki_path == wiki_path
            assert pages[slug].depth == 1


@pytest.mark.asyncio
async def test_concurrent_new_topics_share_one_new_taxonomy_chain(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(
        tenant_id=51, knowledge_base_id=uuid4(), actor_id="task11-concurrent"
    )
    first_inside = asyncio.Event()
    release_first = asyncio.Event()

    class PausingSqlAlchemyIngestStore(SqlAlchemyIngestStore):
        async def _resolve_folder_assignment(
            self,
            session: AsyncSession,
            actual_scope: WikiScope,
            assignment: FolderAssignment,
        ):
            first_inside.set()
            await release_first.wait()
            return await super()._resolve_folder_assignment(
                session, actual_scope, assignment
            )

    first_store = PausingSqlAlchemyIngestStore(
        postgres_factory, SqlFinalizationPort()
    )
    second_store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    first_pending = await _claim_taxonomy_ingest(
        first_store, scope, knowledge_id="same-chain-first"
    )
    second_pending = await _claim_taxonomy_ingest(
        second_store, scope, knowledge_id="same-chain-second"
    )
    first_page = ReducedPage(
        slug="entity/same-chain-first",
        title="Same chain first",
        page_type="entity",
        content="First",
        summary="First",
        contributor_op_ids=[first_pending.id],
    )
    second_page = ReducedPage(
        slug="entity/same-chain-second",
        title="Same chain second",
        page_type="entity",
        content="Second",
        summary="Second",
        contributor_op_ids=[second_pending.id],
    )
    first_request = _taxonomy_apply_request(
        first_pending,
        first_page,
        folder_assignments=(
            FolderAssignment(
                slug=first_page.slug,
                contributor_op_ids=(first_pending.id,),
                new_segments=("Organizations", "Products"),
            ),
        ),
    )
    second_request = _taxonomy_apply_request(
        second_pending,
        second_page,
        folder_assignments=(
            FolderAssignment(
                slug=second_page.slug,
                contributor_op_ids=(second_pending.id,),
                new_segments=("Organizations", "Products"),
            ),
        ),
    )

    first_task = asyncio.create_task(
        first_store.apply_results_with_outcome(scope, first_request)
    )
    second_task: asyncio.Task[BatchApplyOutcome] | None = None
    try:
        await asyncio.wait_for(first_inside.wait(), timeout=5)
        assert not first_task.done()
        second_task = asyncio.create_task(
            second_store.apply_results_with_outcome(scope, second_request)
        )
        await asyncio.sleep(0)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(second_task), timeout=0.1)
        assert not second_task.done()

        release_first.set()
        outcomes = await asyncio.wait_for(
            asyncio.gather(first_task, second_task), timeout=10
        )
    finally:
        release_first.set()
        tasks = (first_task,) if second_task is None else (first_task, second_task)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True), timeout=10
        )

    assert [outcome.applied for outcome in outcomes] == [True, True]
    async with postgres_factory() as session:
        folders = list(
            (
                await session.execute(
                    select(WikiFolder)
                    .where(
                        WikiFolder.tenant_id == scope.tenant_id,
                        WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                    )
                    .order_by(WikiFolder.path)
                )
            ).scalars()
        )
        pages = {
            page.slug: page
            for page in (
                await session.execute(
                    select(WikiPage).where(
                        WikiPage.tenant_id == scope.tenant_id,
                        WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    )
                )
            ).scalars()
        }

    assert [folder.path for folder in folders] == [
        "/Organizations",
        "/Organizations/Products",
    ]
    assert folders[1].parent_id == folders[0].id
    assert pages[first_page.slug].category_path == ["Organizations", "Products"]
    assert pages[second_page.slug].category_path == ["Organizations", "Products"]


@pytest.mark.asyncio
async def test_atomic_taxonomy_creates_reuses_and_resolves_base_folders(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=41, knowledge_base_id=uuid4(), actor_id="task8")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())

    first = await _claim_taxonomy_ingest(store, scope, knowledge_id="taxonomy-first")
    first_page = ReducedPage(
        slug="concept/postgresql",
        title="PostgreSQL",
        page_type="concept",
        content="PostgreSQL",
        summary="Database",
        contributor_op_ids=[first.id],
    )
    first_assignment = FolderAssignment(
        slug=first_page.slug,
        contributor_op_ids=(first.id,),
        new_segments=("Engineering", "Databases"),
    )
    first_request = _taxonomy_apply_request(
        first,
        first_page,
        folder_assignments=(first_assignment,),
    )
    applied = await store.apply_results_with_outcome(scope, first_request)
    replay = await store.apply_results_with_outcome(scope, first_request)
    assert applied.applied is True
    assert replay.applied is False

    second = await _claim_taxonomy_ingest(store, scope, knowledge_id="taxonomy-second")
    second_page = ReducedPage(
        slug="entity/redis",
        title="Redis",
        page_type="entity",
        content="Redis",
        summary="Cache",
        contributor_op_ids=[second.id],
    )
    await store.apply_results_with_outcome(
        scope,
        _taxonomy_apply_request(
            second,
            second_page,
            folder_assignments=(
                FolderAssignment(
                    slug=second_page.slug,
                    contributor_op_ids=(second.id,),
                    new_segments=("Engineering", "Storage"),
                ),
            ),
        ),
    )

    async with postgres_factory() as session:
        engineering = (
            await session.execute(
                select(WikiFolder).where(
                    WikiFolder.tenant_id == scope.tenant_id,
                    WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                    WikiFolder.path == "/Engineering",
                )
            )
        ).scalar_one()

    third = await _claim_taxonomy_ingest(store, scope, knowledge_id="taxonomy-third")
    third_page = ReducedPage(
        slug="entity/nginx",
        title="Nginx",
        page_type="entity",
        content="Nginx",
        summary="Proxy",
        contributor_op_ids=[third.id],
    )
    await store.apply_results_with_outcome(
        scope,
        _taxonomy_apply_request(
            third,
            third_page,
            folder_assignments=(
                FolderAssignment(
                    slug=third_page.slug,
                    contributor_op_ids=(third.id,),
                    base_folder_id=engineering.id,
                    base_path=engineering.path,
                    base_depth=engineering.depth,
                    new_segments=("Networks",),
                ),
            ),
        ),
    )

    root_pending = await _claim_taxonomy_ingest(
        store, scope, knowledge_id="taxonomy-root"
    )
    root_page = ReducedPage(
        slug="entity/root-topic",
        title="Root topic",
        page_type="entity",
        content="Root",
        summary="Root",
        contributor_op_ids=[root_pending.id],
    )
    await store.apply_results_with_outcome(
        scope,
        _taxonomy_apply_request(
            root_pending,
            root_page,
            folder_assignments=(
                FolderAssignment(
                    slug=root_page.slug,
                    contributor_op_ids=(root_pending.id,),
                ),
            ),
        ),
    )

    async with postgres_factory() as session:
        folders = list(
            (
                await session.execute(
                    select(WikiFolder)
                    .where(
                        WikiFolder.tenant_id == scope.tenant_id,
                        WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                        WikiFolder.deleted_at.is_(None),
                    )
                    .order_by(WikiFolder.path)
                )
            ).scalars()
        )
        assert [folder.path for folder in folders] == [
            "/Engineering",
            "/Engineering/Databases",
            "/Engineering/Networks",
            "/Engineering/Storage",
        ]
        pages = {
            page.slug: page
            for page in (
                await session.execute(
                    select(WikiPage).where(
                        WikiPage.tenant_id == scope.tenant_id,
                        WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    )
                )
            ).scalars()
        }
        assert pages[first_page.slug].category_path == ["Engineering", "Databases"]
        assert pages[first_page.slug].wiki_path == (
            "/Engineering/Databases/concept/postgresql"
        )
        assert pages[first_page.slug].depth == 2
        assert pages[first_page.slug].version == 1
        assert pages[second_page.slug].category_path == ["Engineering", "Storage"]
        assert pages[third_page.slug].category_path == ["Engineering", "Networks"]
        assert pages[third_page.slug].folder_id == next(
            folder.id for folder in folders if folder.path == "/Engineering/Networks"
        )
        assert pages[root_page.slug].folder_id is None
        assert pages[root_page.slug].category_path == []
        assert pages[root_page.slug].wiki_path == f"/{root_page.slug}"
        assert pages[root_page.slug].depth == 0


@pytest.mark.asyncio
async def test_taxonomy_restores_soft_deleted_page_without_reassigning_folder(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=42, knowledge_base_id=uuid4(), actor_id="task8")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    async with postgres_factory() as session, session.begin():
        manual = WikiFolder(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            parent_id=None,
            name="Manual",
            path="/Manual",
            depth=1,
        )
        session.add(manual)
        await session.flush()
        historical = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/restored",
            title="Restored",
            page_type="entity",
            status="published",
            content="Same body",
            summary="Same summary",
            folder_id=manual.id,
            category_path=["Manual"],
            wiki_path="/Manual/entity/restored",
            depth=1,
            version=1,
            deleted_at=datetime.now(UTC),
        )
        session.add(historical)
        await session.flush()
        historical_id = historical.id
        manual_id = manual.id

    pending = await _claim_taxonomy_ingest(store, scope, knowledge_id="restore-source")
    restored = ReducedPage(
        slug="entity/restored",
        title="Restored",
        page_type="entity",
        content="Same body",
        summary="Same summary",
        contributor_op_ids=[pending.id],
    )
    await store.apply_results_with_outcome(
        scope,
        _taxonomy_apply_request(pending, restored, folder_assignments=()),
    )

    async with postgres_factory() as session:
        page = await session.get(WikiPage, historical_id)
        assert page is not None and page.deleted_at is None
        assert page.folder_id == manual_id
        assert page.category_path == ["Manual"]
        assert page.wiki_path == "/Manual/entity/restored"
        assert page.depth == 1
        assert page.version == 2
        assert (
            await session.scalar(
                select(func.count(WikiFolder.id)).where(
                    WikiFolder.tenant_id == scope.tenant_id,
                    WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                )
            )
            == 1
        )


@pytest.mark.asyncio
async def test_page_create_conflict_rolls_back_new_taxonomy_and_keeps_claim(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=43, knowledge_base_id=uuid4(), actor_id="task8")
    resolved = asyncio.Event()
    resume = asyncio.Event()

    class PausingStore(SqlAlchemyIngestStore):
        async def _resolve_folder_assignment(
            self,
            session: AsyncSession,
            actual_scope: WikiScope,
            assignment: FolderAssignment,
        ):
            placement = await super()._resolve_folder_assignment(
                session, actual_scope, assignment
            )
            resolved.set()
            await resume.wait()
            return placement

    store = PausingStore(postgres_factory, SqlFinalizationPort())
    pending = await _claim_taxonomy_ingest(store, scope, knowledge_id="race-source")
    page = ReducedPage(
        slug="entity/raced",
        title="Store page",
        page_type="entity",
        content="Store body",
        summary="Store summary",
        contributor_op_ids=[pending.id],
    )
    request = _taxonomy_apply_request(
        pending,
        page,
        folder_assignments=(
            FolderAssignment(
                slug=page.slug,
                contributor_op_ids=(pending.id,),
                new_segments=("Rollback", "Nested"),
            ),
        ),
    )
    applying = asyncio.create_task(store.apply_results_with_outcome(scope, request))
    await resolved.wait()
    try:
        async with postgres_factory() as session, session.begin():
            session.add(
                WikiPage(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    slug=page.slug,
                    title="Concurrent winner",
                    page_type="entity",
                    status="published",
                    content="Concurrent",
                    summary="Concurrent",
                    wiki_path=f"/{page.slug}",
                )
            )
    finally:
        resume.set()

    with pytest.raises(PageConflict, match="并发创建"):
        await applying

    async with postgres_factory() as session:
        assert (
            await session.scalar(
                select(func.count(WikiFolder.id)).where(
                    WikiFolder.tenant_id == scope.tenant_id,
                    WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                )
            )
            == 0
        )
        assert (
            await session.scalar(
                select(func.count(WikiPageContribution.id)).where(
                    WikiPageContribution.tenant_id == scope.tenant_id,
                    WikiPageContribution.knowledge_base_id == scope.knowledge_base_id,
                )
            )
            == 0
        )
        pending_row = await session.get(WikiPendingOp, pending.id)
        assert pending_row is not None
        assert pending_row.fail_count == 0
        assert pending_row.claim_token == pending.claim_token


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ["tenant", "knowledge_base", "moved", "deleted"])
async def test_invalid_taxonomy_base_rolls_back_without_residual_folder(
    postgres_factory: async_sessionmaker[AsyncSession],
    conflict: str,
) -> None:
    scope = WikiScope(tenant_id=44, knowledge_base_id=uuid4(), actor_id="task8")
    folder_tenant = scope.tenant_id + 1 if conflict == "tenant" else scope.tenant_id
    folder_kb = uuid4() if conflict == "knowledge_base" else scope.knowledge_base_id
    async with postgres_factory() as session, session.begin():
        base = WikiFolder(
            tenant_id=folder_tenant,
            knowledge_base_id=folder_kb,
            parent_id=None,
            name="Base",
            path="/Base",
            depth=1,
            deleted_at=datetime.now(UTC) if conflict == "deleted" else None,
        )
        session.add(base)
        await session.flush()
        base_id = base.id

    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    pending = await _claim_taxonomy_ingest(
        store, scope, knowledge_id=f"base-{conflict}"
    )
    page = ReducedPage(
        slug=f"entity/base-{conflict}",
        title="Base conflict",
        page_type="entity",
        content="Base conflict",
        summary="Base conflict",
        contributor_op_ids=[pending.id],
    )
    assignment = FolderAssignment(
        slug=page.slug,
        contributor_op_ids=(pending.id,),
        base_folder_id=base_id,
        base_path="/Base",
        base_depth=1,
        new_segments=("Products",),
    )
    request = _taxonomy_apply_request(
        pending,
        page,
        folder_assignments=(assignment,),
    )
    if conflict == "moved":
        async with postgres_factory() as session, session.begin():
            moved_base = await session.get(WikiFolder, base_id)
            assert moved_base is not None
            moved_base.name = "Moved"
            moved_base.path = "/Moved"

    with pytest.raises(PageConflict, match="taxonomy base 目录已移动或失效"):
        await store.apply_results_with_outcome(scope, request)

    async with postgres_factory() as session:
        assert (
            await session.scalar(
                select(func.count(WikiFolder.id)).where(
                    WikiFolder.tenant_id == scope.tenant_id,
                    WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                    WikiFolder.name == "Products",
                )
            )
            == 0
        )
        pending_row = await session.get(WikiPendingOp, pending.id)
        assert pending_row is not None
        assert pending_row.fail_count == 0
        assert pending_row.claim_token == pending.claim_token


@pytest.mark.asyncio
async def test_existing_sibling_with_wrong_path_depth_is_not_silently_reused(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=45, knowledge_base_id=uuid4(), actor_id="task8")
    async with postgres_factory() as session, session.begin():
        session.add(
            WikiFolder(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                parent_id=None,
                name="Engineering",
                path="/Wrong",
                depth=2,
            )
        )

    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    pending = await _claim_taxonomy_ingest(store, scope, knowledge_id="corrupt-folder")
    page = ReducedPage(
        slug="entity/corrupt-folder",
        title="Corrupt folder",
        page_type="entity",
        content="Corrupt folder",
        summary="Corrupt folder",
        contributor_op_ids=[pending.id],
    )
    with pytest.raises(InvariantError, match="path 或 depth"):
        await store.apply_results_with_outcome(
            scope,
            _taxonomy_apply_request(
                pending,
                page,
                folder_assignments=(
                    FolderAssignment(
                        slug=page.slug,
                        contributor_op_ids=(pending.id,),
                        new_segments=("Engineering",),
                    ),
                ),
            ),
        )

    async with postgres_factory() as session:
        pending_row = await session.get(WikiPendingOp, pending.id)
        assert pending_row is not None and pending_row.fail_count == 0


@pytest.mark.asyncio
async def test_existing_page_folder_cache_does_not_increment_version(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=46, knowledge_base_id=uuid4(), actor_id="task8")
    async with postgres_factory() as session, session.begin():
        manual = WikiFolder(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            parent_id=None,
            name="Manual",
            path="/Manual",
            depth=1,
        )
        session.add(manual)
        await session.flush()
        existing = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/stable-folder",
            title="Stable",
            page_type="entity",
            status="published",
            content="Stable body",
            summary="Stable summary",
            folder_id=manual.id,
            category_path=["Manual"],
            wiki_path="/Manual/entity/stable-folder",
            depth=1,
            version=1,
        )
        session.add(existing)
        await session.flush()
        existing_id = existing.id

    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    pending = await _claim_taxonomy_ingest(store, scope, knowledge_id="stable-source")
    reduced = ReducedPage(
        slug="entity/stable-folder",
        title="Stable",
        page_type="entity",
        content="Stable body",
        summary="Stable summary",
        contributor_op_ids=[pending.id],
    )
    await store.apply_results_with_outcome(
        scope,
        _taxonomy_apply_request(
            pending,
            reduced,
            expected=PageExpectation(
                slug=reduced.slug,
                page_id=existing_id,
                version=1,
            ),
            folder_assignments=(),
        ),
    )

    async with postgres_factory() as session:
        page = await session.get(WikiPage, existing_id)
        assert page is not None and page.version == 1
        assert page.category_path == ["Manual"]
        assert page.wiki_path == "/Manual/entity/stable-folder"
        assert page.depth == 1


@pytest.mark.asyncio
async def test_taxonomy_requires_exact_new_topic_assignment_coverage(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=47, knowledge_base_id=uuid4(), actor_id="task8")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    pending = await _claim_taxonomy_ingest(store, scope, knowledge_id="coverage-source")
    new_page = ReducedPage(
        slug="entity/missing-assignment",
        title="Missing",
        page_type="entity",
        content="Missing",
        summary="Missing",
        contributor_op_ids=[pending.id],
    )
    with pytest.raises(
        InvariantError,
        match="folder assignments 必须精确覆盖真正新建 topic 页面",
    ):
        await store.apply_results_with_outcome(
            scope,
            _taxonomy_apply_request(pending, new_page, folder_assignments=()),
        )

    async with postgres_factory() as session, session.begin():
        history = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/history-extra",
            title="History",
            page_type="entity",
            status="published",
            content="History",
            summary="History",
            wiki_path="/entity/history-extra",
            deleted_at=datetime.now(UTC),
        )
        session.add(history)

    history_page = ReducedPage(
        slug="entity/history-extra",
        title="History",
        page_type="entity",
        content="History",
        summary="History",
        contributor_op_ids=[pending.id],
    )
    with pytest.raises(
        InvariantError,
        match="folder assignments 必须精确覆盖真正新建 topic 页面",
    ):
        await store.apply_results_with_outcome(
            scope,
            _taxonomy_apply_request(
                pending,
                history_page,
                folder_assignments=(
                    FolderAssignment(
                        slug=history_page.slug,
                        contributor_op_ids=(pending.id,),
                        new_segments=("MustNotExist",),
                    ),
                ),
            ),
        )

    async with postgres_factory() as session:
        assert (
            await session.scalar(
                select(func.count(WikiFolder.id)).where(
                    WikiFolder.tenant_id == scope.tenant_id,
                    WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                )
            )
            == 0
        )
        pending_row = await session.get(WikiPendingOp, pending.id)
        assert pending_row is not None and pending_row.fail_count == 0


@pytest.mark.asyncio
async def test_auto_superseded_assignment_is_filtered_before_folder_creation(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=48, knowledge_base_id=uuid4(), actor_id="task8")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    pending = await _claim_taxonomy_ingest(
        store, scope, knowledge_id="superseded-source"
    )
    retract = await store.enqueue_retract(
        scope,
        pending.knowledge_id,
        "delete-1",
        {"knowledge_id": pending.knowledge_id},
        delay_seconds=0,
    )
    assert retract.id is not None
    async with postgres_factory() as session, session.begin():
        await session.execute(
            update(WikiPendingOp)
            .where(WikiPendingOp.id == retract.id)
            .values(enqueued_at=pending.enqueued_at + timedelta(seconds=1))
        )

    page = ReducedPage(
        slug="entity/superseded-topic",
        title="Superseded",
        page_type="entity",
        content="Superseded",
        summary="Superseded",
        contributor_op_ids=[pending.id],
    )
    outcome = await store.apply_results_with_outcome(
        scope,
        _taxonomy_apply_request(
            pending,
            page,
            folder_assignments=(
                FolderAssignment(
                    slug=page.slug,
                    contributor_op_ids=(pending.id,),
                    new_segments=("MustNotExist",),
                ),
            ),
        ),
    )
    assert outcome.completed_op_ids == ()
    assert outcome.superseded_op_ids == (pending.id,)

    async with postgres_factory() as session:
        assert (
            await session.scalar(
                select(func.count(WikiFolder.id)).where(
                    WikiFolder.tenant_id == scope.tenant_id,
                    WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                )
            )
            == 0
        )
        assert (
            await session.scalar(
                select(func.count(WikiPage.id)).where(
                    WikiPage.tenant_id == scope.tenant_id,
                    WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    WikiPage.slug == page.slug,
                )
            )
            == 0
        )
        assert await session.get(WikiPendingOp, pending.id) is None
        assert await session.get(WikiPendingOp, retract.id) is not None


@pytest.mark.asyncio
async def test_legacy_new_page_keeps_root_placement(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=49, knowledge_base_id=uuid4(), actor_id="task8")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    pending = await _claim_taxonomy_ingest(store, scope, knowledge_id="legacy-source")
    page = ReducedPage(
        slug="entity/legacy-root",
        title="Legacy",
        page_type="entity",
        content="Legacy",
        summary="Legacy",
        contributor_op_ids=[pending.id],
    )

    assert await store.apply_results(
        scope,
        pending.claim_token,
        [page],
        [pending.id],
        uuid4(),
        expected_pages={page.slug: None},
    )

    async with postgres_factory() as session:
        persisted = (
            await session.execute(
                select(WikiPage).where(
                    WikiPage.tenant_id == scope.tenant_id,
                    WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    WikiPage.slug == page.slug,
                )
            )
        ).scalar_one()
        assert persisted.folder_id is None
        assert persisted.category_path == []
        assert persisted.wiki_path == f"/{page.slug}"
        assert persisted.depth == 0
        assert persisted.version == 1


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


async def _create_graph_page(
    service: WikiPageService,
    scope: WikiScope,
    *,
    slug: str,
    page_type: str = "entity",
    content: str = "",
):
    return await service.create_page(
        scope,
        WikiPageCreateRequest(
            slug=slug,
            title=slug,
            page_type=page_type,
            content=content,
        ),
    )


@pytest.mark.asyncio
async def test_graph_excludes_inactive_unresolved_edges_and_sorts_visible_edges(
    postgres_session: AsyncSession,
) -> None:
    scope = WikiScope(
        tenant_id=7,
        knowledge_base_id=uuid4(),
        actor_id="owner-graph-visible",
        can_write=True,
    )
    pages = WikiPageService(SqlAlchemyPageStore(postgres_session))
    active_a = await _create_graph_page(
        pages, scope, slug="entity/active-a"
    )
    active_z = await _create_graph_page(
        pages,
        scope,
        slug="entity/active-z",
        content=f"[[{active_a.slug}]]",
    )
    archived_target = await _create_graph_page(
        pages, scope, slug="entity/archived-target"
    )
    deleted_target = await _create_graph_page(
        pages, scope, slug="entity/deleted-target"
    )
    source = await _create_graph_page(
        pages,
        scope,
        slug="entity/source",
        content=" ".join(
            f"[[{slug}]]"
            for slug in (
                active_z.slug,
                active_a.slug,
                archived_target.slug,
                deleted_target.slug,
                "entity/missing-target",
            )
        ),
    )
    archived_source = await _create_graph_page(
        pages,
        scope,
        slug="entity/archived-source",
        content=f"[[{active_a.slug}]]",
    )
    deleted_source = await _create_graph_page(
        pages,
        scope,
        slug="entity/deleted-source",
        content=f"[[{active_a.slug}]]",
    )
    await postgres_session.execute(
        update(WikiPage)
        .where(WikiPage.id.in_([archived_target.id, archived_source.id]))
        .values(status="archived")
    )
    await postgres_session.execute(
        update(WikiPage)
        .where(WikiPage.id.in_([deleted_target.id, deleted_source.id]))
        .values(deleted_at=datetime.now(UTC))
    )
    await postgres_session.commit()

    source_links = {
        link.target_slug: link.target_page_id
        for link in (
            await postgres_session.execute(
                select(WikiLink).where(
                    WikiLink.tenant_id == scope.tenant_id,
                    WikiLink.knowledge_base_id == scope.knowledge_base_id,
                    WikiLink.source_page_id == source.id,
                )
            )
        ).scalars()
    }
    assert source_links[active_a.slug] == active_a.id
    assert source_links[active_z.slug] == active_z.id
    assert source_links[archived_target.slug] == archived_target.id
    assert source_links[deleted_target.slug] == deleted_target.id
    assert source_links["entity/missing-target"] is None
    stale_source_ids = set(
        (
            await postgres_session.execute(
                select(WikiLink.source_page_id).where(
                    WikiLink.source_page_id.in_(
                        [archived_source.id, deleted_source.id]
                    )
                )
            )
        ).scalars()
    )
    assert stale_source_ids == {archived_source.id, deleted_source.id}

    graph = await WikiQueryService(postgres_session).get_graph(
        scope,
        mode="overview",
        center=None,
        hops=1,
        limit=3,
        types={"entity"},
    )

    assert [(node.slug, node.link_count) for node in graph.nodes] == [
        (active_a.slug, 2),
        (active_z.slug, 2),
        (source.slug, 2),
    ]
    assert [(edge.source, edge.target) for edge in graph.edges] == [
        (active_z.slug, active_a.slug),
        (source.slug, active_a.slug),
        (source.slug, active_z.slug),
    ]


@pytest.mark.asyncio
async def test_graph_overview_returns_only_edges_between_top_nodes(
    postgres_session: AsyncSession,
) -> None:
    scope = WikiScope(
        tenant_id=7,
        knowledge_base_id=uuid4(),
        actor_id="owner-graph-overview",
        can_write=True,
    )
    pages = WikiPageService(SqlAlchemyPageStore(postgres_session))
    page_c = await _create_graph_page(pages, scope, slug="entity/c")
    page_b = await _create_graph_page(
        pages,
        scope,
        slug="entity/b",
        content=f"[[{page_c.slug}]]",
    )
    page_a = await _create_graph_page(
        pages,
        scope,
        slug="entity/a",
        content=f"[[{page_b.slug}]]",
    )
    await postgres_session.commit()

    graph = await WikiQueryService(postgres_session).get_graph(
        scope,
        mode="overview",
        center=None,
        hops=1,
        limit=2,
        types={"entity"},
    )

    assert [(node.slug, node.link_count) for node in graph.nodes] == [
        (page_b.slug, 2),
        (page_a.slug, 1),
    ]
    assert [(edge.source, edge.target) for edge in graph.edges] == [
        (page_a.slug, page_b.slug)
    ]


@pytest.mark.asyncio
async def test_graph_types_do_not_expand_through_hidden_frontier(
    postgres_session: AsyncSession,
) -> None:
    scope = WikiScope(
        tenant_id=7,
        knowledge_base_id=uuid4(),
        actor_id="owner-graph-types",
        can_write=True,
    )
    pages = WikiPageService(SqlAlchemyPageStore(postgres_session))
    second_hop = await _create_graph_page(
        pages, scope, slug="entity/second-hop"
    )
    hidden_middle = await _create_graph_page(
        pages,
        scope,
        slug="concept/hidden-middle",
        page_type="concept",
        content=f"[[{second_hop.slug}]]",
    )
    center = await _create_graph_page(
        pages,
        scope,
        slug="entity/center",
        content=f"[[{hidden_middle.slug}]]",
    )
    await postgres_session.commit()

    graph = await WikiQueryService(postgres_session).get_graph(
        scope,
        mode="ego",
        center=center.slug,
        hops=2,
        limit=10,
        types={"entity"},
    )

    assert [node.slug for node in graph.nodes] == [center.slug]
    assert graph.edges == []


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
async def test_real_taxonomy_context_is_scoped_and_excludes_all_page_history(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=39, knowledge_base_id=uuid4(), actor_id="task6")
    other_scope = WikiScope(
        tenant_id=scope.tenant_id + 1,
        knowledge_base_id=uuid4(),
        actor_id="other-task6",
    )
    root_id, child_id, leaf_id = uuid4(), uuid4(), uuid4()
    async with postgres_factory() as session, session.begin():
        session.add_all(
            [
                WikiFolder(
                    id=root_id,
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    name="Engineering",
                    path="/Engineering",
                    depth=1,
                ),
                WikiFolder(
                    id=child_id,
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    parent_id=root_id,
                    name="Databases",
                    path="/Engineering/Databases",
                    depth=2,
                ),
                WikiFolder(
                    id=leaf_id,
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    parent_id=child_id,
                    name="PostgreSQL",
                    path="/Engineering/Databases/PostgreSQL",
                    depth=3,
                ),
                WikiFolder(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    name="Deleted",
                    path="/Deleted",
                    depth=1,
                    deleted_at=datetime.now(UTC),
                ),
                WikiFolder(
                    tenant_id=other_scope.tenant_id,
                    knowledge_base_id=other_scope.knowledge_base_id,
                    name="Hidden",
                    path="/Hidden",
                    depth=1,
                ),
                WikiPage(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    slug="concept/current",
                    title="Current",
                    page_type="concept",
                    status="published",
                ),
                WikiPage(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    slug="concept/deleted-history",
                    title="Deleted history",
                    page_type="concept",
                    status="published",
                    deleted_at=datetime.now(UTC),
                ),
                WikiPage(
                    tenant_id=other_scope.tenant_id,
                    knowledge_base_id=other_scope.knowledge_base_id,
                    slug="entity/other-only",
                    title="Other only",
                    page_type="entity",
                    status="published",
                ),
            ]
        )

    context = await SqlAlchemyIngestStore(
        postgres_factory, SqlFinalizationPort()
    ).load_taxonomy_context(
        scope,
        [
            "entity/other-only",
            "concept/current",
            "entity/new",
            "concept/deleted-history",
            "summary/overview",
        ],
    )

    assert [(folder.id, folder.path) for folder in context.folders] == [
        (root_id, "/Engineering"),
        (child_id, "/Engineering/Databases"),
        (leaf_id, "/Engineering/Databases/PostgreSQL"),
    ]
    assert context.classifiable_slugs == ("entity/new", "entity/other-only")


@pytest.mark.asyncio
async def test_real_dedup_cutoff_ties_return_stable_smallest_twenty_slugs(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=35, knowledge_base_id=uuid4(), actor_id="task13")
    slugs = [f"entity/exact-tie-{index:02d}" for index in range(25)]
    insertion_order = slugs.copy()
    Random(1307).shuffle(insertion_order)
    async with postgres_factory() as session, session.begin():
        session.add_all(
            [
                WikiPage(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    slug=slug,
                    title="Exact Tie",
                    page_type="entity",
                    status="published",
                    aliases=["Exact Tie Alias"],
                    wiki_path=f"/{slug}",
                )
                for slug in insertion_order
            ]
        )
    candidate = TopicCandidate(
        name="Exact Tie",
        slug="entity/generated-exact-tie",
        page_type="entity",
    )
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    expected = sorted(slugs)[:20]

    for _ in range(5):
        found = await store.find_dedup_candidates(scope, candidate, limit=20)
        assert [item.slug for item in found] == expected


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
        folder_assignments=(
            FolderAssignment(
                slug="entity/canonical",
                contributor_op_ids=tuple(item.id for item in claimed),
            ),
        ),
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
        embedding_model=NeverEmbedding(),
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


@pytest.mark.asyncio
async def test_multiple_retract_versions_for_one_source_complete_in_one_batch(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=38, knowledge_base_id=uuid4(), actor_id="task13")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    async with postgres_factory() as session, session.begin():
        page = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/retract-versions",
            title="Retract versions",
            page_type="entity",
            status="published",
            content="Original body",
            summary="Original summary",
            source_refs=["source-a"],
            chunk_refs=["chunk:a"],
            wiki_path="/entity/retract-versions",
        )
        contribution = WikiPageContribution(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug=page.slug,
            knowledge_id="source-a",
            op_version="v1",
            page_type="entity",
            state="active",
            title="Contribution source-a",
            content="Body source-a",
            summary="Summary source-a",
            aliases=[],
            chunk_refs=["chunk:a"],
        )
        session.add_all([page, contribution])

    first = await store.enqueue_retract(
        scope,
        "source-a",
        "delete-v1",
        {"knowledge_id": "source-a"},
        delay_seconds=0,
    )
    second = await store.enqueue_retract(
        scope,
        "source-a",
        "delete-v2",
        {"knowledge_id": "source-a"},
        delay_seconds=0,
    )
    assert first.id is not None and second.id is not None

    class NoReadSource:
        def __getattr__(self, name: str):
            raise AssertionError(f"retract worker 不应读取 source: {name}")

    class NoMergeModel:
        async def merge_page(self, _request):
            raise AssertionError("纯 retract 不应调用模型")

    worker = WikiIngestWorker(
        store=store,
        locks=MemoryWikiLockManager(),
        source=NoReadSource(),  # type: ignore[arg-type]
        model=NoMergeModel(),  # type: ignore[arg-type]
        embedding_model=NeverEmbedding(),
        options=WikiWorkerOptions(),
    )

    result = await worker.run_batch(scope)

    assert set(result.completed_op_ids) == {first.id, second.id}
    assert result.failed_op_ids == result.superseded_op_ids == ()
    async with postgres_factory() as session:
        assert (
            await session.scalar(
                select(func.count(WikiPageContribution.id)).where(
                    WikiPageContribution.tenant_id == scope.tenant_id,
                    WikiPageContribution.knowledge_base_id == scope.knowledge_base_id,
                )
            )
            == 0
        )
        assert (
            await session.scalar(
                select(func.count(WikiPendingOp.id)).where(
                    WikiPendingOp.tenant_id == scope.tenant_id,
                    WikiPendingOp.knowledge_base_id == scope.knowledge_base_id,
                )
            )
            == 0
        )
        markers = list(
            (
                await session.execute(
                    select(WikiFinalizationMarker)
                    .where(
                        WikiFinalizationMarker.tenant_id == scope.tenant_id,
                        WikiFinalizationMarker.knowledge_base_id
                        == scope.knowledge_base_id,
                        WikiFinalizationMarker.knowledge_id == "source-a",
                    )
                    .order_by(WikiFinalizationMarker.attempt)
                )
            ).scalars()
        )
        assert [(marker.attempt, marker.released_at is not None) for marker in markers] == [
            ("delete-v1", True),
            ("delete-v2", True),
        ]


@pytest.mark.asyncio
async def test_retract_worker_dead_letters_after_five_real_failed_batches(
    postgres_factory: async_sessionmaker[AsyncSession],
) -> None:
    scope = WikiScope(tenant_id=37, knowledge_base_id=uuid4(), actor_id="task13")
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    async with postgres_factory() as session, session.begin():
        page = WikiPage(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            slug="entity/retract-retry",
            title="Shared before retract",
            page_type="entity",
            status="published",
            content="Original shared body",
            summary="Original shared summary",
            source_refs=["source-a", "source-b"],
            chunk_refs=["chunk:a", "chunk:b"],
            wiki_path="/entity/retract-retry",
        )
        session.add(page)
        session.add_all(
            [
                WikiPageContribution(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    slug=page.slug,
                    knowledge_id=knowledge_id,
                    op_version="v1",
                    page_type="entity",
                    state="active",
                    title=f"Contribution {knowledge_id}",
                    content=f"Body {knowledge_id}",
                    summary=f"Summary {knowledge_id}",
                    aliases=[],
                    chunk_refs=[chunk_ref],
                )
                for knowledge_id, chunk_ref in (
                    ("source-a", "chunk:a"),
                    ("source-b", "chunk:b"),
                )
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

    class NoReadSource:
        def __getattr__(self, name: str):
            raise AssertionError(f"retract worker 不应读取 source: {name}")

    class PermanentlyFailingModel:
        def __init__(self) -> None:
            self.calls = 0

        async def merge_page(self, _request):
            self.calls += 1
            raise PermanentModelError("retract reduce failed")

    model = PermanentlyFailingModel()
    worker = WikiIngestWorker(
        store=store,
        locks=MemoryWikiLockManager(),
        source=NoReadSource(),  # type: ignore[arg-type]
        model=model,  # type: ignore[arg-type]
        embedding_model=NeverEmbedding(),
        options=WikiWorkerOptions(),
        retry_wait=lambda _seconds: asyncio.sleep(0),
    )

    for expected_fail_count in range(1, 6):
        result = await worker.run_batch(scope)
        assert result.failed_op_ids == (enqueued.id,)
        assert result.completed_op_ids == result.superseded_op_ids == ()
        assert model.calls == expected_fail_count

        async with postgres_factory() as session:
            pending = await session.get(WikiPendingOp, enqueued.id)
            if expected_fail_count < 5:
                assert pending is not None
                assert pending.fail_count == expected_fail_count
                assert pending.claimed_at is None
                assert pending.claim_token is None
            else:
                assert pending is None

            persisted_page = await session.get(WikiPage, page.id)
            assert persisted_page is not None
            assert persisted_page.deleted_at is None
            assert persisted_page.source_refs == ["source-b"]
            assert persisted_page.chunk_refs == ["chunk:b"]
            assert persisted_page.content == "Original shared body"

            contributions = list(
                (
                    await session.execute(
                        select(WikiPageContribution)
                        .where(
                            WikiPageContribution.tenant_id == scope.tenant_id,
                            WikiPageContribution.knowledge_base_id
                            == scope.knowledge_base_id,
                            WikiPageContribution.slug == page.slug,
                        )
                        .order_by(WikiPageContribution.knowledge_id)
                    )
                ).scalars()
            )
            assert [
                (item.knowledge_id, item.state) for item in contributions
            ] == [("source-a", "retract_pending"), ("source-b", "active")]

            trigger_rows = list(
                (
                    await session.execute(
                        select(TaskOutbox).where(
                            TaskOutbox.tenant_id == scope.tenant_id,
                            TaskOutbox.knowledge_base_id == scope.knowledge_base_id,
                            TaskOutbox.event_type == "wiki.batch.trigger",
                        )
                    )
                ).scalars()
            )
            assert sum(row.sent_at is not None for row in trigger_rows) == (
                expected_fail_count
            )
            assert sum(row.sent_at is None for row in trigger_rows) == 1 + min(
                expected_fail_count, 4
            )

            marker = (
                await session.execute(
                    select(WikiFinalizationMarker).where(
                        WikiFinalizationMarker.tenant_id == scope.tenant_id,
                        WikiFinalizationMarker.knowledge_base_id
                        == scope.knowledge_base_id,
                        WikiFinalizationMarker.knowledge_id == "source-a",
                        WikiFinalizationMarker.subtask_name == "wiki-retract",
                    )
                )
            ).scalar_one()
            assert (marker.released_at is not None) is (expected_fail_count == 5)

            dead_count = int(
                (
                    await session.execute(
                        select(func.count(WikiDeadLetter.id)).where(
                            WikiDeadLetter.pending_op_id == enqueued.id
                        )
                    )
                ).scalar_one()
            )
            assert dead_count == (1 if expected_fail_count == 5 else 0)

    dead_letters = await store.list_dead_letters(scope)
    assert len(dead_letters) == 1
    assert dead_letters[0].pending_op_id == enqueued.id
    assert dead_letters[0].op == "retract"
    assert dead_letters[0].fail_count == 5
