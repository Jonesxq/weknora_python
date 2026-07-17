from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from app.wiki.ingest.errors import WikiBatchBusy
from app.wiki.ingest.fakes import FakeChatModel, FakeDataset, FakeKnowledgeSource
from app.wiki.ingest.schemas import ReducedPage, WikiWorkerOptions
from app.wiki.ingest.store import ExistingPageRecord, PageConflict, PendingOpRecord
from app.wiki.ingest.worker import WikiIngestWorker, WikiLockLost
from app.wiki.scope import WikiScope
from app.wiki.tasks.locks import LockOwnershipLost


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
CLAIM_TOKEN = UUID("99999999-9999-9999-9999-999999999999")
OP_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OP_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
OP_C = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
SCOPE = WikiScope(tenant_id=1, knowledge_base_id=KB_ID, actor_id="worker")
NOW = datetime(2026, 7, 17, tzinfo=UTC)


def pending_op(
    op_id: UUID,
    knowledge_id: str,
    *,
    claim_token: UUID | None = CLAIM_TOKEN,
) -> PendingOpRecord:
    return PendingOpRecord(
        id=op_id,
        tenant_id=1,
        knowledge_base_id=KB_ID,
        knowledge_id=knowledge_id,
        op="upsert",
        op_version="v1",
        payload={},
        fail_count=0,
        enqueued_at=NOW,
        claimed_at=NOW,
        claim_token=claim_token,
    )


def fake_dataset(
    knowledge_ids: tuple[str, ...] = ("doc-a", "doc-b"),
    *,
    concepts_by_knowledge: dict[str, tuple[str, ...]] | None = None,
    include_shared: bool = True,
    omitted_summaries: set[str] | None = None,
    omitted_merges: set[str] | None = None,
    transient_failures: dict[str, int] | None = None,
    short_knowledge: set[str] | None = None,
) -> FakeDataset:
    concepts_by_knowledge = concepts_by_knowledge or {}
    omitted_summaries = omitted_summaries or set()
    omitted_merges = omitted_merges or set()
    short_knowledge = short_knowledge or set()
    all_concepts = {
        slug
        for slugs in concepts_by_knowledge.values()
        for slug in slugs
    }
    merge_slugs = ({"entity/shared"} if include_shared else set()) | all_concepts
    merge_slugs -= omitted_merges
    if not merge_slugs:
        # The shared fake schema intentionally requires at least one merge response.
        merge_slugs = {"entity/unused"}
    return FakeDataset.model_validate(
        {
            "knowledge_bases": [
                {
                    "tenant_id": 1,
                    "knowledge_base_id": str(KB_ID),
                    "config": {"wiki_enabled": True},
                }
            ],
            "knowledge": [
                {
                    "id": knowledge_id,
                    "tenant_id": 1,
                    "knowledge_base_id": str(KB_ID),
                    "title": f"Document {knowledge_id}",
                    "op_version": "v1",
                    "status": "ready",
                    "chunks": [
                        {
                            "id": f"chunk-{knowledge_id}",
                            "text": (
                                "short"
                                if knowledge_id in short_knowledge
                                else f"Meaningful source content for {knowledge_id}."
                            ),
                        }
                    ],
                }
                for knowledge_id in knowledge_ids
            ],
            "model_responses": {
                "extract_candidates": {
                    knowledge_id: {
                        "entities": (
                            [
                                {
                                    "name": "Shared",
                                    "slug": "entity/shared",
                                    "page_type": "entity",
                                    "description": f"Shared from {knowledge_id}",
                                    "details": f"Details from {knowledge_id}",
                                }
                            ]
                            if include_shared
                            else []
                        ),
                        "concepts": [
                            {
                                "name": slug.rpartition("/")[2].title(),
                                "slug": slug,
                                "page_type": "concept",
                                "description": f"Concept from {knowledge_id}",
                                "details": f"Concept details from {knowledge_id}",
                            }
                            for slug in concepts_by_knowledge.get(knowledge_id, ())
                        ],
                    }
                    for knowledge_id in knowledge_ids
                },
                "summaries": {
                    knowledge_id: {
                        "headline": f"Headline {knowledge_id}",
                        "markdown": f"Summary body {knowledge_id}",
                    }
                    for knowledge_id in knowledge_ids
                    if knowledge_id not in omitted_summaries
                },
                "merges": {
                    slug: {
                        "headline": f"Merged {slug}",
                        "markdown": f"Merged body {slug}",
                    }
                    for slug in sorted(merge_slugs)
                },
            },
            "transient_failures": transient_failures or {},
        }
    )


class FakeLease:
    def __init__(self, *, lose_on_assert: bool = False) -> None:
        self.lose_on_assert = lose_on_assert
        self.assert_calls = 0
        self.enter_calls = 0
        self.exit_calls = 0

    async def __aenter__(self) -> FakeLease:
        self.enter_calls += 1
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.exit_calls += 1

    async def assert_owned(self) -> None:
        self.assert_calls += 1
        if self.lose_on_assert:
            raise LockOwnershipLost("lost")


class FakeLocks:
    def __init__(self, lease: FakeLease | None) -> None:
        self.lease = lease
        self.acquire_calls: list[UUID] = []

    async def acquire(self, knowledge_base_id: UUID) -> FakeLease | None:
        self.acquire_calls.append(knowledge_base_id)
        return self.lease


class WorkerStore:
    def __init__(
        self,
        records: list[PendingOpRecord],
        *,
        existing: dict[str, ExistingPageRecord] | None = None,
        pending_override: int | None = None,
        conflict: bool = False,
    ) -> None:
        self.records = list(records)
        self.existing = existing or {}
        self.pending_override = pending_override
        self.conflict = conflict
        self.claim_calls: list[tuple[WikiScope, int, int]] = []
        self.find_calls: list[tuple[str, ...]] = []
        self.apply_calls: list[dict[str, Any]] = []
        self.release_calls: list[tuple[WikiScope, list[UUID], UUID | None]] = []
        self.page_writes: list[list[ReducedPage]] = []

    async def pending_count(self, scope: WikiScope) -> int:
        assert scope == SCOPE
        if self.pending_override is not None:
            return self.pending_override
        return len(self.records)

    async def claim_pending(
        self, scope: WikiScope, limit: int, claim_timeout: int
    ) -> list[PendingOpRecord]:
        self.claim_calls.append((scope, limit, claim_timeout))
        return list(self.records)

    async def find_existing_pages(
        self, scope: WikiScope, slugs: list[str]
    ) -> dict[str, ExistingPageRecord]:
        assert scope == SCOPE
        ordered = tuple(slugs)
        self.find_calls.append(ordered)
        return {slug: self.existing[slug] for slug in ordered if slug in self.existing}

    async def apply_results(
        self,
        scope: WikiScope,
        claim_token: UUID | None,
        pages: list[ReducedPage],
        completed_op_ids: list[UUID],
        operation_id: UUID,
        *,
        failed_op_ids: list[UUID],
        expected_pages: dict[str, ExistingPageRecord | None],
    ) -> bool:
        call = {
            "scope": scope,
            "claim_token": claim_token,
            "pages": [page.model_copy(deep=True) for page in pages],
            "completed_op_ids": list(completed_op_ids),
            "failed_op_ids": list(failed_op_ids),
            "operation_id": operation_id,
            "expected_pages": dict(expected_pages),
        }
        self.apply_calls.append(call)
        if self.conflict:
            raise PageConflict("conflict")
        self.page_writes.append(call["pages"])
        self.records = []
        self.pending_override = None
        return True

    async def release_failed(
        self, scope: WikiScope, ids: list[UUID], claim_token: UUID | None
    ) -> None:
        self.release_calls.append((scope, list(ids), claim_token))


def worker(
    store: WorkerStore,
    source: FakeKnowledgeSource,
    model: FakeChatModel,
    *,
    lease: FakeLease | None = None,
    options: WikiWorkerOptions | None = None,
    waits: list[int] | None = None,
) -> WikiIngestWorker:
    async def retry_wait(seconds: int) -> None:
        assert waits is not None
        waits.append(seconds)

    return WikiIngestWorker(
        store=store,
        locks=FakeLocks(FakeLease() if lease is None else lease),
        source=source,
        model=model,
        options=options,
        retry_wait=retry_wait if waits is not None else None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(("pending", "busy"), [(0, False), (1, True)])
async def test_unavailable_lock_distinguishes_empty_and_busy_batches(
    pending: int, busy: bool
) -> None:
    store = WorkerStore([], pending_override=pending)
    source = FakeKnowledgeSource(fake_dataset(("doc-a",)))
    model = FakeChatModel(fake_dataset(("doc-a",)))
    ingest_worker = WikiIngestWorker(
        store=store,
        locks=FakeLocks(None),
        source=source,
        model=model,
    )

    if busy:
        with pytest.raises(WikiBatchBusy):
            await ingest_worker.run_batch(SCOPE)
    else:
        assert (await ingest_worker.run_batch(SCOPE)).completed_op_ids == []
    assert store.claim_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(("pending", "busy"), [(0, False), (1, True)])
async def test_empty_claim_distinguishes_empty_and_busy_batches(
    pending: int, busy: bool
) -> None:
    store = WorkerStore([], pending_override=pending)
    lease = FakeLease()
    dataset = fake_dataset(("doc-a",))
    ingest_worker = worker(
        store, FakeKnowledgeSource(dataset), FakeChatModel(dataset), lease=lease
    )

    if busy:
        with pytest.raises(WikiBatchBusy):
            await ingest_worker.run_batch(SCOPE)
    else:
        assert (await ingest_worker.run_batch(SCOPE)).failed_op_ids == []
    assert lease.exit_calls == 1
    assert store.apply_calls == []


@pytest.mark.asyncio
async def test_full_fake_batch_commits_snapshots_and_is_idempotent() -> None:
    dataset = fake_dataset(("doc-a",))
    source = FakeKnowledgeSource(dataset)
    model = FakeChatModel(dataset)
    old_page = ReducedPage(
        slug="entity/shared",
        title="Old shared",
        page_type="entity",
        content="Old body",
        summary="Old summary",
    )
    snapshot = ExistingPageRecord(page_id=uuid4(), version=7, page=old_page)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")], existing={"entity/shared": snapshot}
    )
    options = WikiWorkerOptions(batch_size=8, claim_timeout_seconds=321)
    ingest_worker = worker(store, source, model, options=options)

    first = await ingest_worker.run_batch(SCOPE)
    second = await ingest_worker.run_batch(SCOPE)

    assert first.completed_op_ids == [OP_A]
    assert first.failed_op_ids == []
    assert second.completed_op_ids == second.failed_op_ids == []
    assert store.claim_calls == [(SCOPE, 8, 321), (SCOPE, 8, 321)]
    assert len(store.find_calls) == 1
    assert set(store.find_calls[0]) == {"summary/doc-a", "entity/shared"}
    assert len(store.apply_calls) == len(store.page_writes) == 1
    call = store.apply_calls[0]
    assert call["claim_token"] == CLAIM_TOKEN
    assert call["operation_id"] == uuid5(
        NAMESPACE_URL, f"wiki:{KB_ID}:{CLAIM_TOKEN}"
    )
    assert call["completed_op_ids"] == [OP_A]
    assert call["failed_op_ids"] == []
    assert call["expected_pages"] == {
        "summary/doc-a": None,
        "entity/shared": snapshot,
    }
    assert call["expected_pages"]["entity/shared"] is snapshot
    assert model.merge_requests[0].existing_content == "Old body"
    assert {page.slug for page in call["pages"]} == {
        "summary/doc-a",
        "entity/shared",
    }


@pytest.mark.asyncio
async def test_rejects_missing_or_inconsistent_claim_tokens() -> None:
    dataset = fake_dataset()
    for records in (
        [pending_op(OP_A, "doc-a", claim_token=None)],
        [
            pending_op(OP_A, "doc-a"),
            pending_op(OP_B, "doc-b", claim_token=uuid4()),
        ],
    ):
        store = WorkerStore(records)
        with pytest.raises(RuntimeError, match="claim token"):
            await worker(
                store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)
            ).run_batch(SCOPE)
        assert store.apply_calls == []


@pytest.mark.asyncio
async def test_map_permanent_failure_isolates_only_that_operation() -> None:
    dataset = fake_dataset(omitted_summaries={"doc-a"})
    store = WorkerStore([pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")])
    waits: list[int] = []
    model = FakeChatModel(dataset)

    result = await worker(
        store, FakeKnowledgeSource(dataset), model, waits=waits
    ).run_batch(SCOPE)

    assert result.completed_op_ids == [OP_B]
    assert result.failed_op_ids == [OP_A]
    assert waits == []
    assert model.calls.count("summarize:doc-a") == 1
    assert {page.slug for page in store.apply_calls[0]["pages"]} == {
        "summary/doc-b",
        "entity/shared",
    }


@pytest.mark.asyncio
async def test_reduce_failure_removes_contributor_and_rereduces_mixed_slug() -> None:
    dataset = fake_dataset(
        concepts_by_knowledge={"doc-a": ("concept/alpha",)},
        omitted_merges={"concept/alpha"},
    )
    store = WorkerStore([pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")])
    waits: list[int] = []
    model = FakeChatModel(dataset)

    result = await worker(
        store, FakeKnowledgeSource(dataset), model, waits=waits
    ).run_batch(SCOPE)

    assert result.completed_op_ids == [OP_B]
    assert result.failed_op_ids == [OP_A]
    assert waits == []
    assert model.calls.count("merge:concept/alpha") == 1
    shared_requests = [
        request for request in model.merge_requests if request.slug == "entity/shared"
    ]
    assert len(shared_requests) == 2
    assert [
        contribution.knowledge_id
        for contribution in shared_requests[-1].contributions
    ] == ["doc-b"]
    assert {page.slug for page in store.apply_calls[0]["pages"]} == {
        "summary/doc-b",
        "entity/shared",
    }


@pytest.mark.asyncio
async def test_source_invalidated_after_map_is_removed_before_commit() -> None:
    dataset = fake_dataset()

    class ExpiringSource(FakeKnowledgeSource):
        def __init__(self) -> None:
            super().__init__(dataset)
            self.active_calls: defaultdict[str, int] = defaultdict(int)

        async def is_active(
            self, scope: WikiScope, knowledge_id: str, op_version: str
        ) -> bool:
            self.active_calls[knowledge_id] += 1
            active = await super().is_active(scope, knowledge_id, op_version)
            return active and not (
                knowledge_id == "doc-a" and self.active_calls[knowledge_id] >= 2
            )

    source = ExpiringSource()
    model = FakeChatModel(dataset)
    store = WorkerStore([pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")])

    result = await worker(store, source, model).run_batch(SCOPE)

    assert result.completed_op_ids == [OP_B]
    assert result.failed_op_ids == [OP_A]
    assert source.active_calls["doc-a"] == 2
    shared_requests = [
        request for request in model.merge_requests if request.slug == "entity/shared"
    ]
    assert len(shared_requests) == 2
    assert [item.knowledge_id for item in shared_requests[-1].contributions] == [
        "doc-b"
    ]


@pytest.mark.asyncio
async def test_skipped_short_document_completes_without_pages() -> None:
    dataset = fake_dataset(("doc-a",), short_knowledge={"doc-a"})
    model = FakeChatModel(dataset)
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert result.completed_op_ids == [OP_A]
    assert result.failed_op_ids == []
    assert store.find_calls == [()]
    assert store.apply_calls[0]["pages"] == []
    assert model.calls == []


@pytest.mark.asyncio
async def test_map_transient_failure_retries_three_attempts_with_backoff() -> None:
    dataset = fake_dataset(
        ("doc-a",), transient_failures={"extract_candidates:doc-a": 2}
    )
    model = FakeChatModel(dataset)
    waits: list[int] = []
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    result = await worker(
        store, FakeKnowledgeSource(dataset), model, waits=waits
    ).run_batch(SCOPE)

    assert result.completed_op_ids == [OP_A]
    assert model.calls.count("extract_candidates:doc-a") == 3
    assert waits == [2, 4]


@pytest.mark.asyncio
async def test_reduce_transient_failure_stops_after_three_attempts() -> None:
    dataset = fake_dataset(
        ("doc-a",), transient_failures={"merge:entity/shared": 3}
    )
    model = FakeChatModel(dataset)
    waits: list[int] = []
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    result = await worker(
        store, FakeKnowledgeSource(dataset), model, waits=waits
    ).run_batch(SCOPE)

    assert result.completed_op_ids == []
    assert result.failed_op_ids == [OP_A]
    assert model.calls.count("merge:entity/shared") == 3
    assert waits == [2, 4]
    assert store.apply_calls[0]["pages"] == []


@pytest.mark.asyncio
async def test_map_parallelism_is_bounded() -> None:
    dataset = fake_dataset(("doc-a", "doc-b", "doc-c"))

    class BlockingSource(FakeKnowledgeSource):
        def __init__(self) -> None:
            super().__init__(dataset)
            self.active = 0
            self.maximum = 0
            self.two_started = asyncio.Event()
            self.release = asyncio.Event()

        async def get_knowledge(self, scope: WikiScope, knowledge_id: str):
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            if self.active == 2:
                self.two_started.set()
            try:
                await self.release.wait()
                return await super().get_knowledge(scope, knowledge_id)
            finally:
                self.active -= 1

    source = BlockingSource()
    store = WorkerStore(
        [
            pending_op(OP_A, "doc-a"),
            pending_op(OP_B, "doc-b"),
            pending_op(OP_C, "doc-c"),
        ]
    )
    task = asyncio.create_task(
        worker(
            store,
            source,
            FakeChatModel(dataset),
            options=WikiWorkerOptions(map_parallel=2),
        ).run_batch(SCOPE)
    )
    try:
        await asyncio.wait_for(source.two_started.wait(), timeout=1)
        await asyncio.sleep(0.02)
        assert source.maximum == 2
        source.release.set()
        result = await asyncio.wait_for(task, timeout=1)
    finally:
        source.release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    assert result.completed_ops == 3


@pytest.mark.asyncio
async def test_map_child_cancellation_cleans_up_sibling_before_propagating() -> None:
    dataset = fake_dataset()

    class CancellingSource(FakeKnowledgeSource):
        def __init__(self) -> None:
            super().__init__(dataset)
            self.started: set[str] = set()
            self.both_started = asyncio.Event()
            self.release_sibling = asyncio.Event()
            self.sibling_cleaned = asyncio.Event()

        async def get_knowledge(self, scope: WikiScope, knowledge_id: str):
            self.started.add(knowledge_id)
            if len(self.started) == 2:
                self.both_started.set()
            await self.both_started.wait()
            if knowledge_id == "doc-a":
                await asyncio.sleep(0)
                raise asyncio.CancelledError
            try:
                await self.release_sibling.wait()
                return await super().get_knowledge(scope, knowledge_id)
            finally:
                self.sibling_cleaned.set()

    source = CancellingSource()
    store = WorkerStore([pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")])
    try:
        with pytest.raises(asyncio.CancelledError):
            await worker(store, source, FakeChatModel(dataset)).run_batch(SCOPE)

        assert source.sibling_cleaned.is_set()
        assert store.apply_calls == []
        assert store.release_calls == []
    finally:
        source.release_sibling.set()
        await asyncio.wait_for(source.sibling_cleaned.wait(), timeout=1)


@pytest.mark.asyncio
async def test_reduce_parallelism_is_bounded() -> None:
    dataset = fake_dataset(
        ("doc-a",), concepts_by_knowledge={"doc-a": ("concept/alpha",)}
    )

    class BlockingModel(FakeChatModel):
        def __init__(self) -> None:
            super().__init__(dataset)
            self.active = 0
            self.maximum = 0
            self.two_started = asyncio.Event()
            self.release = asyncio.Event()

        async def merge_page(self, request):
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            if self.active == 2:
                self.two_started.set()
            try:
                await self.release.wait()
                return await super().merge_page(request)
            finally:
                self.active -= 1

    model = BlockingModel()
    store = WorkerStore([pending_op(OP_A, "doc-a")])
    task = asyncio.create_task(
        worker(
            store,
            FakeKnowledgeSource(dataset),
            model,
            options=WikiWorkerOptions(reduce_parallel=2),
        ).run_batch(SCOPE)
    )
    try:
        await asyncio.wait_for(model.two_started.wait(), timeout=1)
        await asyncio.sleep(0.02)
        assert model.maximum == 2
        model.release.set()
        result = await asyncio.wait_for(task, timeout=1)
    finally:
        model.release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    assert result.completed_op_ids == [OP_A]


@pytest.mark.asyncio
async def test_reduce_child_cancellation_cleans_up_sibling_before_propagating() -> None:
    dataset = fake_dataset(
        ("doc-a",), concepts_by_knowledge={"doc-a": ("concept/alpha",)}
    )

    class CancellingModel(FakeChatModel):
        def __init__(self) -> None:
            super().__init__(dataset)
            self.started: set[str] = set()
            self.both_started = asyncio.Event()
            self.release_sibling = asyncio.Event()
            self.sibling_cleaned = asyncio.Event()

        async def merge_page(self, request):
            self.started.add(request.slug)
            if len(self.started) == 2:
                self.both_started.set()
            await self.both_started.wait()
            if request.slug == "entity/shared":
                await asyncio.sleep(0)
                raise asyncio.CancelledError
            try:
                await self.release_sibling.wait()
                return await super().merge_page(request)
            finally:
                self.sibling_cleaned.set()

    model = CancellingModel()
    store = WorkerStore([pending_op(OP_A, "doc-a")])
    try:
        with pytest.raises(asyncio.CancelledError):
            await worker(
                store,
                FakeKnowledgeSource(dataset),
                model,
                options=WikiWorkerOptions(reduce_parallel=2),
            ).run_batch(SCOPE)

        assert model.sibling_cleaned.is_set()
        assert store.apply_calls == []
        assert store.release_calls == []
    finally:
        model.release_sibling.set()
        await asyncio.wait_for(model.sibling_cleaned.wait(), timeout=1)


@pytest.mark.asyncio
async def test_lock_loss_never_commits_or_releases_claim() -> None:
    dataset = fake_dataset(("doc-a",))
    store = WorkerStore([pending_op(OP_A, "doc-a")])
    lease = FakeLease(lose_on_assert=True)

    with pytest.raises(WikiLockLost) as error:
        await worker(
            store,
            FakeKnowledgeSource(dataset),
            FakeChatModel(dataset),
            lease=lease,
        ).run_batch(SCOPE)

    assert isinstance(error.value.__cause__, LockOwnershipLost)
    assert lease.assert_calls == 1
    assert store.apply_calls == []
    assert store.release_calls == []


@pytest.mark.asyncio
async def test_page_conflict_releases_entire_claim_as_failed() -> None:
    dataset = fake_dataset()
    store = WorkerStore(
        [pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")], conflict=True
    )

    result = await worker(
        store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)
    ).run_batch(SCOPE)

    assert result.completed_op_ids == []
    assert result.failed_op_ids == [OP_A, OP_B]
    assert store.release_calls == [(SCOPE, [OP_A, OP_B], CLAIM_TOKEN)]
