from __future__ import annotations

import asyncio
from collections import defaultdict
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from typing import get_type_hints
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from pydantic import ValidationError

from app.wiki.errors import WikiValidationError
from app.wiki.ingest import worker as worker_module
from app.wiki.ingest.errors import WikiBatchBusy
from app.wiki.ingest.enqueue import WikiEnqueueService
from app.wiki.ingest.fakes import FakeChatModel, FakeDataset, FakeKnowledgeSource
from app.wiki.ingest.ports import (
    PermanentModelError,
    TransientModelError,
    WikiIngestModelPort,
)
from app.wiki.ingest.schemas import (
    BatchApplyOutcome,
    BatchApplyRequest,
    BatchResult,
    EmbeddingOutput,
    EmbeddingRequest,
    FolderCatalogEntry,
    IndexIntroContext,
    IndexIntroOutput,
    IndexIntroRequest,
    IndexPageSnapshot,
    ReducedPage,
    StoredContributionRecord,
    TaxonomyContext,
    TaxonomyDecision,
    TaxonomyOutput,
    TaxonomyRequest,
    WikiWorkerOptions,
)
from app.wiki.ingest.store import (
    ClaimLost,
    EnqueueRecord,
    ExistingPageRecord,
    PageConflict,
    PendingOpRecord,
)
from app.wiki.ingest.worker import WikiIngestWorker, WikiLockLost
from app.wiki.scope import WikiScope
from app.wiki.tasks.locks import LockOwnershipLost


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
CLAIM_TOKEN = UUID("99999999-9999-9999-9999-999999999999")
OP_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OP_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
OP_C = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
FOLDER_ROOT = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
FOLDER_CHILD = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
SCOPE = WikiScope(tenant_id=1, knowledge_base_id=KB_ID, actor_id="worker")
NOW = datetime(2026, 7, 17, tzinfo=UTC)


def test_batch_result_uses_disjoint_frozen_operation_sets() -> None:
    result = BatchResult(
        completed_op_ids=(OP_A,),
        failed_op_ids=(OP_B,),
        superseded_op_ids=(OP_C,),
    )

    assert result.completed_op_ids == (OP_A,)
    assert result.failed_op_ids == (OP_B,)
    assert result.superseded_op_ids == (OP_C,)
    with pytest.raises(ValidationError):
        result.completed_op_ids = ()

    for kwargs in (
        {"completed_op_ids": (OP_A, OP_A)},
        {"failed_op_ids": (OP_A, OP_A)},
        {"superseded_op_ids": (OP_A, OP_A)},
        {"completed_op_ids": (OP_A,), "failed_op_ids": (OP_A,)},
        {"completed_op_ids": (OP_A,), "superseded_op_ids": (OP_A,)},
        {"failed_op_ids": (OP_A,), "superseded_op_ids": (OP_A,)},
    ):
        with pytest.raises(ValidationError):
            BatchResult(**kwargs)


def test_worker_model_dependency_uses_composite_ingest_protocol() -> None:
    assert get_type_hints(WikiIngestWorker.__init__)["model"] is WikiIngestModelPort


@pytest.mark.parametrize(
    ("error", "expected_code", "safe_summary"),
    [
        (
            PermanentModelError('raw model {"secret": "value"}\nTraceback'),
            "MODEL_PERMANENT",
            "模型调用发生永久错误",
        ),
        (
            TransientModelError('raw model {"secret": "value"}\nTraceback'),
            "MODEL_RETRY_EXHAUSTED",
            "模型调用重试已耗尽",
        ),
        (
            WikiValidationError(
                "WIKI_BAD_INPUT", 'raw model {"secret": "value"}\nTraceback'
            ),
            "WIKI_BAD_INPUT",
            "Wiki 数据校验失败",
        ),
        (
            RuntimeError('raw model {"secret": "value"}\nTraceback'),
            "WIKI_INGEST_FAILED",
            "Wiki 处理失败（RuntimeError）",
        ),
    ],
)
def test_operation_failure_classifies_and_sanitizes_errors(
    error: Exception, expected_code: str, safe_summary: str
) -> None:
    failure = worker_module.operation_failure(OP_A, error)

    assert failure.pending_op_id == OP_A
    assert failure.error_code == expected_code[:128]
    assert len(failure.error_code) <= 128
    assert failure.error_summary == safe_summary
    assert "secret" not in failure.error_summary
    assert "Traceback" not in failure.error_summary
    assert "\n" not in failure.error_summary


def pending_op(
    op_id: UUID,
    knowledge_id: str,
    *,
    op: str = "ingest",
    claim_token: UUID | None = CLAIM_TOKEN,
) -> PendingOpRecord:
    return PendingOpRecord(
        id=op_id,
        tenant_id=1,
        knowledge_base_id=KB_ID,
        knowledge_id=knowledge_id,
        op=op,
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
    all_concepts = {slug for slugs in concepts_by_knowledge.values() for slug in slugs}
    taxonomy_slugs = ({"entity/shared"} if include_shared else set()) | all_concepts
    merge_slugs = set(taxonomy_slugs)
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
                "taxonomies": {
                    ",".join(batch): {
                        "decisions": [
                            {
                                "slug": slug,
                                "base_folder_id": None,
                                "new_segments": [],
                            }
                            for slug in batch
                        ]
                    }
                    for batch_size in range(1, min(60, len(taxonomy_slugs)) + 1)
                    for start in range(0, len(taxonomy_slugs), batch_size)
                    if (
                        batch := tuple(sorted(taxonomy_slugs))[
                            start : start + batch_size
                        ]
                    )
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
        contributions: list[StoredContributionRecord] | None = None,
        pending_override: int | None = None,
        conflict: bool = False,
        claim_lost: bool = False,
        apply_outcome: BatchApplyOutcome | None = None,
        folders: tuple[FolderCatalogEntry, ...] = (),
        classifiable_slugs: tuple[str, ...] = (),
        index_intro_context: IndexIntroContext | None = None,
        index_context_error: BaseException | None = None,
        events: list[str] | None = None,
    ) -> None:
        self.records = list(records)
        self.existing = existing or {}
        self.contributions = list(contributions or [])
        self.pending_override = pending_override
        self.conflict = conflict
        self.claim_lost = claim_lost
        self.apply_outcome = apply_outcome
        self.folders = folders
        self.classifiable_slugs = classifiable_slugs
        self.index_intro_context = index_intro_context or IndexIntroContext()
        self.index_context_error = index_context_error
        self.events = events
        self.claim_calls: list[tuple[WikiScope, int, int]] = []
        self.find_calls: list[tuple[str, ...]] = []
        self.taxonomy_context_calls: list[tuple[str, ...]] = []
        self.index_context_calls: list[WikiScope] = []
        self.contribution_calls: list[tuple[str, str]] = []
        self.apply_calls: list[BatchApplyRequest] = []
        self.bool_apply_calls = 0
        self.release_calls: list[tuple[WikiScope, list[UUID], UUID]] = []
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
        if self.events is not None:
            self.events.append("existing")
        return {slug: self.existing[slug] for slug in ordered if slug in self.existing}

    async def load_taxonomy_context(
        self, scope: WikiScope, slugs: list[str]
    ) -> TaxonomyContext:
        assert scope == SCOPE
        ordered = tuple(slugs)
        self.taxonomy_context_calls.append(ordered)
        if self.events is not None:
            self.events.append("taxonomy-context")
        requested = set(ordered)
        return TaxonomyContext(
            folders=self.folders,
            classifiable_slugs=tuple(
                slug
                for slug in self.classifiable_slugs
                if slug in requested and slug not in self.existing
            ),
        )

    async def list_source_contributions(
        self, scope: WikiScope, knowledge_id: str, *, state: str
    ) -> list[StoredContributionRecord]:
        assert scope == SCOPE
        self.contribution_calls.append((knowledge_id, state))
        if self.events is not None:
            self.events.append(f"contributions:{state}")
        return [
            item
            for item in self.contributions
            if item.knowledge_id == knowledge_id and item.state == state
        ]

    async def load_index_intro_context(self, scope: WikiScope) -> IndexIntroContext:
        assert scope == SCOPE
        self.index_context_calls.append(scope)
        if self.events is not None:
            self.events.append("index-context")
        if self.index_context_error is not None:
            raise self.index_context_error
        return IndexIntroContext.model_validate(
            self.index_intro_context.model_dump(mode="python", warnings="error")
        )

    async def find_dedup_candidates(
        self, scope: WikiScope, candidate: object, limit: int = 20
    ) -> list[object]:
        assert scope == SCOPE
        return []

    async def apply_results_with_outcome(
        self,
        scope: WikiScope,
        request: BatchApplyRequest,
    ) -> BatchApplyOutcome:
        assert scope == SCOPE
        if self.events is not None:
            self.events.append("apply")
        call = BatchApplyRequest.model_validate(request.model_dump(mode="python"))
        self.apply_calls.append(call)
        if self.conflict:
            raise PageConflict("conflict")
        if self.claim_lost:
            raise ClaimLost("claim lost")
        self.page_writes.append(
            [
                ReducedPage.model_validate(page.model_dump(mode="python"))
                for page in call.pages
            ]
        )
        self.records = []
        self.pending_override = None
        return self.apply_outcome or BatchApplyOutcome(
            applied=True,
            completed_op_ids=request.completed_op_ids,
            superseded_op_ids=request.superseded_op_ids,
            failed_op_ids=tuple(failure.pending_op_id for failure in request.failures),
        )

    async def apply_results(
        self,
        scope: WikiScope,
        request: BatchApplyRequest,
    ) -> bool:
        self.bool_apply_calls += 1
        return (await self.apply_results_with_outcome(scope, request)).applied

    async def release_claim(
        self, scope: WikiScope, ids: list[UUID], claim_token: UUID
    ) -> None:
        self.release_calls.append((scope, list(ids), claim_token))


class FakeTombstones:
    def __init__(self, deleted_on_call: dict[str, int] | None = None) -> None:
        self.deleted_on_call = deleted_on_call or {}
        self.calls: defaultdict[str, int] = defaultdict(int)

    async def mark_deleted(self, scope: WikiScope, knowledge_id: str) -> None:
        raise AssertionError("Worker 不应写 tombstone")

    async def is_deleted(self, scope: WikiScope, knowledge_id: str) -> bool:
        assert scope == SCOPE
        self.calls[knowledge_id] += 1
        threshold = self.deleted_on_call.get(knowledge_id)
        return threshold is not None and self.calls[knowledge_id] >= threshold


class NeverEmbedding:
    async def embed(self, request: EmbeddingRequest) -> EmbeddingOutput:
        raise AssertionError(f"embedding 不应被调用: {request}")


class FailingEmbedding:
    def __init__(self, transient_failures: int = 0) -> None:
        self.transient_failures = transient_failures
        self.calls = 0

    async def embed(self, request: EmbeddingRequest) -> EmbeddingOutput:
        self.calls += 1
        if self.calls <= self.transient_failures:
            raise TransientModelError("embedding transient")
        return EmbeddingOutput(
            vectors={
                item.key: (1.0, 0.0) if item.key.startswith("topic:") else (0.9, 0.1)
                for item in request.items
            }
        )


class OrderedTaxonomyModel(FakeChatModel):
    def __init__(
        self,
        dataset: FakeDataset,
        *,
        events: list[str] | None = None,
        permanent_batches: set[str] | None = None,
        transient_batches: dict[str, int] | None = None,
        block_taxonomy: bool = False,
        started_target: int = 2,
    ) -> None:
        super().__init__(dataset)
        self.events = events
        self.permanent_batches = permanent_batches or set()
        self.transient_batches = transient_batches or {}
        self.block_taxonomy = block_taxonomy
        self.started_target = started_target
        self.taxonomy_active = 0
        self.taxonomy_maximum = 0
        self.taxonomy_started = asyncio.Event()
        self.release_taxonomy = asyncio.Event()

    async def extract_candidates(self, *args, **kwargs):
        if self.events is not None:
            self.events.append("map")
        return await super().extract_candidates(*args, **kwargs)

    async def merge_page(self, request):
        if self.events is not None:
            self.events.append("reduce")
        return await super().merge_page(request)

    async def plan_folders(self, request: TaxonomyRequest) -> TaxonomyOutput:
        snapshot = TaxonomyRequest.model_validate(request.model_dump(mode="python"))
        self.taxonomy_requests.append(snapshot)
        batch_key = ",".join(topic.slug for topic in snapshot.topics)
        self.calls.append(f"taxonomy:{batch_key}")
        if self.events is not None:
            self.events.append("taxonomy")

        remaining = self.transient_batches.get(batch_key, 0)
        if remaining:
            self.transient_batches[batch_key] = remaining - 1
            raise TransientModelError("taxonomy transient")
        if batch_key in self.permanent_batches:
            raise PermanentModelError("taxonomy permanent")

        self.taxonomy_active += 1
        self.taxonomy_maximum = max(self.taxonomy_maximum, self.taxonomy_active)
        if self.taxonomy_active >= self.started_target:
            self.taxonomy_started.set()
        try:
            if self.block_taxonomy:
                await self.release_taxonomy.wait()
            base_folder_id = (
                snapshot.allowed_bases[0].id if snapshot.allowed_bases else None
            )
            return TaxonomyOutput(
                decisions=tuple(
                    TaxonomyDecision(
                        slug=topic.slug,
                        base_folder_id=base_folder_id,
                    )
                    for topic in snapshot.topics
                )
            )
        finally:
            self.taxonomy_active -= 1


class OrderedIndexModel(OrderedTaxonomyModel):
    def __init__(
        self,
        dataset: FakeDataset,
        *,
        intro: object = None,
        transient_failures: int = 0,
        error: BaseException | None = None,
        events: list[str] | None = None,
    ) -> None:
        super().__init__(dataset, events=events)
        self.index_result = (
            IndexIntroOutput(intro="Generated index intro") if intro is None else intro
        )
        self.index_transient_failures = transient_failures
        self.index_error = error

    async def generate_index_intro(
        self, request: IndexIntroRequest
    ) -> IndexIntroOutput:
        snapshot = IndexIntroRequest.model_validate(
            request.model_dump(mode="python", warnings="error")
        )
        self.index_intro_requests.append(snapshot)
        self.calls.append("index-intro")
        if self.events is not None:
            self.events.append("index-model")
        if self.index_transient_failures > 0:
            self.index_transient_failures -= 1
            raise TransientModelError("index intro transient")
        if self.index_error is not None:
            raise self.index_error
        return deepcopy(self.index_result)  # type: ignore[return-value]


def contribution(
    knowledge_id: str,
    slug: str,
    *,
    state: str = "active",
    op_version: str = "v1",
) -> StoredContributionRecord:
    page_type = slug.partition("/")[0]
    return StoredContributionRecord(
        tenant_id=1,
        knowledge_base_id=KB_ID,
        slug=slug,
        knowledge_id=knowledge_id,
        op_version=op_version,
        page_type=page_type,
        state=state,
        title=f"Title {slug}",
        content=f"Content {knowledge_id} {slug}",
        summary=f"Summary {knowledge_id} {slug}",
    )


def worker(
    store: WorkerStore,
    source: FakeKnowledgeSource,
    model: FakeChatModel,
    *,
    lease: FakeLease | None = None,
    options: WikiWorkerOptions | None = None,
    waits: list[int] | None = None,
    tombstones: FakeTombstones | None = None,
    embedding_model: object | None = None,
) -> WikiIngestWorker:
    async def retry_wait(seconds: int) -> None:
        assert waits is not None
        waits.append(seconds)

    return WikiIngestWorker(
        store=store,
        locks=FakeLocks(FakeLease() if lease is None else lease),
        source=source,
        model=model,
        embedding_model=embedding_model or NeverEmbedding(),
        tombstones=tombstones or FakeTombstones(),
        options=options,
        retry_wait=retry_wait if waits is not None else None,
    )


def update_index_context(intro: str = "Old index intro") -> IndexIntroContext:
    return IndexIntroContext(
        index=IndexPageSnapshot(
            id=UUID("12345678-1234-5678-1234-567812345678"),
            version=7,
            content=intro,
            summary="Index summary",
        )
    )


@pytest.mark.asyncio
async def test_index_intro_create_runs_once_after_fixed_point_and_before_apply() -> (
    None
):
    events: list[str] = []
    dataset = fake_dataset(("doc-a",))
    model = OrderedIndexModel(
        dataset,
        intro=IndexIntroOutput(intro="Generated intro\n## Directory\n- ignored"),
        events=events,
    )
    store = WorkerStore([pending_op(OP_A, "doc-a")], events=events)

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert len(store.index_context_calls) == 1
    assert len(model.index_intro_requests) == 1
    assert len(store.apply_calls) == 1
    plan = store.apply_calls[0].index_intro_plan
    assert plan is not None
    assert plan.mode == "create"
    assert plan.intro == "Generated intro"
    assert plan.model_status == "generated"
    assert model.index_intro_requests[0].mode == "create"
    assert [item.slug for item in model.index_intro_requests[0].summaries] == [
        "summary/doc-a"
    ]
    assert max(
        index for index, event in enumerate(events) if event == "reduce"
    ) < events.index("index-context")
    assert events.index("index-context") < events.index("index-model")
    assert events.index("index-model") < events.index("apply")
    with pytest.raises(ValidationError):
        plan.intro = "mutated"  # type: ignore[misc]


@pytest.mark.asyncio
async def test_index_intro_update_contains_only_old_intro_and_completed_changes() -> (
    None
):
    dataset = fake_dataset(("doc-a",))
    old = contribution("doc-z", "entity/old", state="retract_pending")
    existing = ExistingPageRecord(
        page_id=uuid4(),
        version=3,
        page=ReducedPage(
            slug="entity/old",
            title="Old",
            page_type="entity",
            content="Old body",
            summary="Old summary",
            source_refs=["doc-z"],
        ),
    )
    model = OrderedIndexModel(dataset)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-z", op="retract")],
        existing={"entity/old": existing},
        contributions=[old],
        index_intro_context=update_index_context(),
    )

    await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert len(model.index_intro_requests) == 1
    request = model.index_intro_requests[0]
    assert request.mode == "update"
    assert request.existing_intro == "Old index intro"
    assert request.summaries == ()
    assert [(change.action, change.knowledge_id) for change in request.changes] == [
        ("ingest", "doc-a"),
        ("retract", "doc-z"),
    ]
    plan = store.apply_calls[0].index_intro_plan
    assert plan is not None
    assert plan.expected_page_id == update_index_context().index.id  # type: ignore[union-attr]
    assert plan.expected_version == 7


@pytest.mark.asyncio
async def test_index_intro_skips_context_and_model_when_completed_operation_has_no_delta() -> (
    None
):
    dataset = fake_dataset(("doc-a",), short_knowledge={"doc-a"})
    model = OrderedIndexModel(dataset)
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert store.index_context_calls == []
    assert model.index_intro_requests == []
    assert store.apply_calls[0].index_intro_plan is None


@pytest.mark.asyncio
async def test_index_intro_filters_failed_and_superseded_deltas_after_fixed_point() -> (
    None
):
    events: list[str] = []
    dataset = fake_dataset(
        ("doc-a", "doc-b", "doc-c"),
        concepts_by_knowledge={"doc-c": ("concept/fail",)},
        include_shared=False,
        omitted_merges={"concept/fail"},
    )

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

    model = OrderedIndexModel(dataset, events=events)
    store = WorkerStore(
        [
            pending_op(OP_A, "doc-a"),
            pending_op(OP_B, "doc-b"),
            pending_op(OP_C, "doc-c"),
        ],
        index_intro_context=update_index_context(),
        events=events,
    )

    result = await worker(store, ExpiringSource(), model).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_B,)
    assert result.superseded_op_ids == (OP_A,)
    assert result.failed_op_ids == (OP_C,)
    assert len(store.index_context_calls) == 1
    assert len(model.index_intro_requests) == 1
    assert [
        (change.action, change.knowledge_id)
        for change in model.index_intro_requests[0].changes
    ] == [("ingest", "doc-b")]
    assert {
        delta.pending_op_id for delta in store.apply_calls[0].contribution_deltas
    } == {OP_B}
    assert max(
        index for index, event in enumerate(events) if event == "reduce"
    ) < events.index("index-context")


@pytest.mark.asyncio
async def test_index_intro_transient_exhaustion_defaults_without_failing_operation() -> (
    None
):
    dataset = fake_dataset(("doc-a",), include_shared=False)
    waits: list[int] = []
    model = OrderedIndexModel(dataset, transient_failures=3)
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    result = await worker(
        store, FakeKnowledgeSource(dataset), model, waits=waits
    ).run_batch(SCOPE)

    assert waits == [2, 4]
    assert len(model.index_intro_requests) == 3
    assert result.completed_op_ids == (OP_A,)
    assert result.failed_op_ids == ()
    request = store.apply_calls[0]
    assert request.completed_op_ids == (OP_A,)
    assert request.failures == ()
    assert request.index_intro_plan is not None
    assert request.index_intro_plan.model_status == "defaulted"
    assert request.index_intro_plan.error_code == "INDEX_INTRO_TRANSIENT_ERROR"


@pytest.mark.asyncio
async def test_index_intro_permanent_error_keeps_existing_intro_after_one_attempt() -> (
    None
):
    dataset = fake_dataset(("doc-a",), include_shared=False)
    model = OrderedIndexModel(dataset, error=PermanentModelError("no index intro"))
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")], index_intro_context=update_index_context()
    )

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert len(model.index_intro_requests) == 1
    assert result.completed_op_ids == (OP_A,)
    plan = store.apply_calls[0].index_intro_plan
    assert plan is not None
    assert plan.intro == "Old index intro"
    assert plan.model_status == "kept_after_error"
    assert plan.error_code == "INDEX_INTRO_PERMANENT_ERROR"
    assert store.apply_calls[0].failures == ()


@pytest.mark.parametrize(
    "invalid_output",
    [
        {"intro": ""},
        IndexIntroOutput(intro="## Directory only"),
    ],
)
@pytest.mark.asyncio
async def test_index_intro_invalid_or_clean_empty_output_uses_invalid_fallback(
    invalid_output: object,
) -> None:
    dataset = fake_dataset(("doc-a",), include_shared=False)
    model = OrderedIndexModel(dataset, intro=invalid_output)
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert len(model.index_intro_requests) == 1
    plan = store.apply_calls[0].index_intro_plan
    assert plan is not None
    assert plan.model_status == "defaulted"
    assert plan.error_code == "INDEX_INTRO_INVALID_OUTPUT"
    assert store.apply_calls[0].failures == ()


@pytest.mark.asyncio
async def test_index_intro_context_error_propagates_and_releases_claim() -> None:
    dataset = fake_dataset(("doc-a",), include_shared=False)
    model = OrderedIndexModel(dataset)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")], index_context_error=RuntimeError("db down")
    )

    with pytest.raises(RuntimeError, match="db down"):
        await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert len(store.index_context_calls) == 1
    assert model.index_intro_requests == []
    assert store.apply_calls == []
    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]


@pytest.mark.asyncio
async def test_index_intro_cancelled_error_propagates_without_apply() -> None:
    dataset = fake_dataset(("doc-a",), include_shared=False)
    model = OrderedIndexModel(dataset, error=asyncio.CancelledError())
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    with pytest.raises(asyncio.CancelledError):
        await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert len(model.index_intro_requests) == 1
    assert store.apply_calls == []
    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]


@pytest.mark.asyncio
async def test_index_intro_control_error_propagates_through_lock_lost_path() -> None:
    dataset = fake_dataset(("doc-a",), include_shared=False)
    model = OrderedIndexModel(dataset, error=LockOwnershipLost("model lost lock"))
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    with pytest.raises(WikiLockLost) as error:
        await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert isinstance(error.value.__cause__, LockOwnershipLost)
    assert len(model.index_intro_requests) == 1
    assert store.apply_calls == []
    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]


@pytest.mark.asyncio
async def test_index_intro_plans_before_lease_check_and_lock_loss_blocks_apply() -> (
    None
):
    dataset = fake_dataset(("doc-a",), include_shared=False)
    model = OrderedIndexModel(dataset)
    store = WorkerStore([pending_op(OP_A, "doc-a")])
    lease = FakeLease(lose_on_assert=True)

    with pytest.raises(WikiLockLost):
        await worker(
            store,
            FakeKnowledgeSource(dataset),
            model,
            lease=lease,
        ).run_batch(SCOPE)

    assert len(store.index_context_calls) == 1
    assert len(model.index_intro_requests) == 1
    assert lease.assert_calls == 1
    assert store.apply_calls == []
    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]


@pytest.mark.asyncio
async def test_dispatches_ingest_and_retract_and_applies_once() -> None:
    dataset = fake_dataset(("doc-a",))
    old = contribution("doc-z", "entity/old", state="retract_pending")
    existing = ExistingPageRecord(
        page_id=uuid4(),
        version=3,
        page=ReducedPage(
            slug="entity/old",
            title="Old",
            page_type="entity",
            content="Old body",
            summary="Old summary",
            source_refs=["doc-z"],
        ),
    )
    store = WorkerStore(
        [pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-z", op="retract")],
        existing={"entity/old": existing},
        contributions=[old],
    )

    result = await worker(
        store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)
    ).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A, OP_B)
    assert result.failed_op_ids == result.superseded_op_ids == ()
    assert len(store.apply_calls) == 1
    request = store.apply_calls[0]
    assert request.claim_token == CLAIM_TOKEN
    assert request.completed_op_ids == (OP_A, OP_B)
    assert {
        (delta.pending_op_id, delta.action) for delta in request.contribution_deltas
    } == {
        (OP_A, "add"),
        (OP_B, "retract"),
    }
    assert {page.slug for page in request.pages} == {
        "summary/doc-a",
        "entity/shared",
        "entity/old",
    }
    assert next(page for page in request.pages if page.slug == "entity/old").deleted


@pytest.mark.asyncio
async def test_unknown_operation_is_permanent_failure_without_source_or_model_reads() -> (
    None
):
    dataset = fake_dataset(("doc-a",))

    class NoReadSource(FakeKnowledgeSource):
        def __init__(self) -> None:
            super().__init__(dataset)
            self.read_calls: list[str] = []

        async def get_config(self, scope: WikiScope):
            self.read_calls.append("config")
            raise AssertionError("unknown op 不应读取 config")

        async def get_knowledge(self, scope: WikiScope, knowledge_id: str):
            self.read_calls.append("knowledge")
            raise AssertionError("unknown op 不应读取正文")

        async def list_chunks(self, scope: WikiScope, knowledge_id: str):
            self.read_calls.append("chunks")
            raise AssertionError("unknown op 不应读取 chunks")

        async def is_active(
            self, scope: WikiScope, knowledge_id: str, op_version: str
        ) -> bool:
            self.read_calls.append("active")
            raise AssertionError("unknown op 不应读取 active")

    source = NoReadSource()
    model = FakeChatModel(dataset)
    store = WorkerStore([pending_op(OP_A, "doc-a", op="unsupported")])

    result = await worker(store, source, model).run_batch(SCOPE)

    assert result.completed_op_ids == result.superseded_op_ids == ()
    assert result.failed_op_ids == (OP_A,)
    assert source.read_calls == []
    assert model.calls == []
    assert store.contribution_calls == []
    assert len(store.apply_calls) == 1
    assert store.apply_calls[0].failures[0].error_code == "WIKI_UNKNOWN_OP"


@pytest.mark.asyncio
async def test_retract_reads_only_retract_pending_contributions() -> None:
    dataset = fake_dataset(("doc-a",))

    class NoReadSource(FakeKnowledgeSource):
        async def get_config(self, scope: WikiScope):
            raise AssertionError("retract 不应读取 config")

        async def get_knowledge(self, scope: WikiScope, knowledge_id: str):
            raise AssertionError("retract 不应读取 source 正文")

        async def list_chunks(self, scope: WikiScope, knowledge_id: str):
            raise AssertionError("retract 不应读取 chunks")

        async def is_active(
            self, scope: WikiScope, knowledge_id: str, op_version: str
        ) -> bool:
            raise AssertionError("retract 不应读取 source 状态")

    old = contribution("doc-z", "entity/old", state="retract_pending")
    store = WorkerStore(
        [pending_op(OP_A, "doc-z", op="retract")],
        existing={
            "entity/old": ExistingPageRecord(
                page_id=uuid4(),
                version=1,
                page=ReducedPage(
                    slug="entity/old",
                    title="Old",
                    page_type="entity",
                    content="Body",
                    summary="Summary",
                    source_refs=["doc-z"],
                ),
            )
        },
        contributions=[old],
    )

    result = await worker(
        store, NoReadSource(dataset), FakeChatModel(dataset)
    ).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert store.contribution_calls[0] == ("doc-z", "retract_pending")
    assert store.apply_calls[0].contribution_deltas[0].action == "retract"


@pytest.mark.asyncio
async def test_map_tombstone_result_is_superseded_without_failure_or_delta() -> None:
    dataset = fake_dataset(("doc-a",))
    model = FakeChatModel(dataset)
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    result = await worker(
        store,
        FakeKnowledgeSource(dataset),
        model,
        tombstones=FakeTombstones({"doc-a": 1}),
    ).run_batch(SCOPE)

    assert result.superseded_op_ids == (OP_A,)
    assert result.completed_op_ids == result.failed_op_ids == ()
    assert model.calls == []
    assert len(store.apply_calls) == 1
    request = store.apply_calls[0]
    assert request.superseded_op_ids == (OP_A,)
    assert request.contribution_deltas == request.pages == request.failures == ()


@pytest.mark.asyncio
async def test_precommit_tombstone_supersedes_and_rereduces_shared_slug() -> None:
    dataset = fake_dataset()
    model = OrderedTaxonomyModel(dataset)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")],
        classifiable_slugs=("entity/shared",),
    )

    result = await worker(
        store,
        FakeKnowledgeSource(dataset),
        model,
        tombstones=FakeTombstones({"doc-a": 2}),
    ).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_B,)
    assert result.superseded_op_ids == (OP_A,)
    assert result.failed_op_ids == ()
    shared_requests = [
        request for request in model.merge_requests if request.slug == "entity/shared"
    ]
    assert len(shared_requests) == 2
    assert [item.knowledge_id for item in shared_requests[-1].contributions] == [
        "doc-b"
    ]
    request = store.apply_calls[0]
    assert {delta.pending_op_id for delta in request.contribution_deltas} == {OP_B}
    assert {page.slug for page in request.pages} == {
        "summary/doc-b",
        "entity/shared",
    }
    shared_taxonomy = [
        taxonomy_request.topics[0]
        for taxonomy_request in model.taxonomy_requests
        if [topic.slug for topic in taxonomy_request.topics] == ["entity/shared"]
    ]
    assert len(shared_taxonomy) == 2
    assert request.folder_assignments[0].contributor_op_ids == (OP_B,)


@pytest.mark.asyncio
async def test_precommit_rechecks_remaining_ingests_after_each_rereduce() -> None:
    dataset = fake_dataset()
    store = WorkerStore([pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")])

    result = await worker(
        store,
        FakeKnowledgeSource(dataset),
        FakeChatModel(dataset),
        tombstones=FakeTombstones({"doc-a": 2, "doc-b": 3}),
    ).run_batch(SCOPE)

    assert result.completed_op_ids == result.failed_op_ids == ()
    assert result.superseded_op_ids == (OP_A, OP_B)
    request = store.apply_calls[0]
    assert request.superseded_op_ids == (OP_A, OP_B)
    assert request.pages == request.contribution_deltas == request.failures == ()


@pytest.mark.asyncio
async def test_returns_store_transaction_terminal_outcome() -> None:
    dataset = fake_dataset(("doc-a",))
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")],
        apply_outcome=BatchApplyOutcome(
            applied=True,
            completed_op_ids=(),
            superseded_op_ids=(OP_A,),
            failed_op_ids=(),
        ),
    )

    result = await worker(
        store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)
    ).run_batch(SCOPE)

    assert result.completed_op_ids == result.failed_op_ids == ()
    assert result.superseded_op_ids == (OP_A,)
    assert len(store.apply_calls) == 1
    assert store.bool_apply_calls == 0


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
        embedding_model=NeverEmbedding(),
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
    assert set(store.find_calls[-1]) == {"summary/doc-a", "entity/shared"}
    assert len(store.apply_calls) == len(store.page_writes) == 1
    call = store.apply_calls[0]
    assert call.claim_token == CLAIM_TOKEN
    assert call.operation_id == uuid5(NAMESPACE_URL, f"wiki:{KB_ID}:{CLAIM_TOKEN}")
    assert call.completed_op_ids == (OP_A,)
    assert call.failures == ()
    assert {
        item.slug: (item.page_id, item.version) for item in call.expected_pages
    } == {
        "summary/doc-a": (None, None),
        "entity/shared": (snapshot.page_id, snapshot.version),
    }
    assert model.merge_requests[0].existing_content == "Old body"
    assert {page.slug for page in call.pages} == {
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
    assert store.apply_calls[0].failures[0].error_code == "MODEL_PERMANENT"
    assert {page.slug for page in store.apply_calls[0].pages} == {
        "summary/doc-b",
        "entity/shared",
    }


@pytest.mark.asyncio
async def test_reduce_failure_removes_contributor_and_rereduces_mixed_slug() -> None:
    dataset = fake_dataset(
        concepts_by_knowledge={"doc-a": ("concept/alpha",)},
        omitted_merges={"concept/alpha"},
    )
    store = WorkerStore(
        [pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")],
        classifiable_slugs=("concept/alpha", "entity/shared"),
    )
    waits: list[int] = []
    model = OrderedTaxonomyModel(dataset)

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
    assert len(shared_requests) >= 2
    assert [
        contribution.knowledge_id for contribution in shared_requests[-1].contributions
    ] == ["doc-b"]
    assert {
        delta.pending_op_id for delta in store.apply_calls[0].contribution_deltas
    } == {OP_B}
    assert {page.slug for page in store.apply_calls[0].pages} == {
        "summary/doc-b",
        "entity/shared",
    }
    taxonomy_topics = [
        tuple(topic.slug for topic in request.topics)
        for request in model.taxonomy_requests
    ]
    assert taxonomy_topics == [
        ("concept/alpha", "entity/shared"),
        ("entity/shared",),
    ]
    assert store.apply_calls[0].folder_assignments[0].contributor_op_ids == (OP_B,)


@pytest.mark.asyncio
async def test_taxonomy_runs_after_map_context_and_before_reduce_and_commits_assignment() -> (
    None
):
    events: list[str] = []
    dataset = fake_dataset(
        ("doc-a",),
        concepts_by_knowledge={"doc-a": ("concept/alpha",)},
        include_shared=False,
    )
    model = OrderedTaxonomyModel(dataset, events=events)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")],
        classifiable_slugs=("concept/alpha",),
        events=events,
    )

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert len(store.taxonomy_context_calls) == 1
    assert set(store.taxonomy_context_calls[0]) == {
        "summary/doc-a",
        "concept/alpha",
    }
    assert events.index("map") < events.index("existing")
    assert events.index("contributions:active") < events.index("taxonomy-context")
    assert events.index("taxonomy-context") < events.index("taxonomy")
    assert events.index("taxonomy") < events.index("reduce")
    assignment = store.apply_calls[0].folder_assignments[0]
    assert assignment.slug == "concept/alpha"
    assert assignment.contributor_op_ids == (OP_A,)


@pytest.mark.asyncio
async def test_existing_topic_is_not_classified() -> None:
    dataset = fake_dataset(
        ("doc-a",),
        concepts_by_knowledge={"doc-a": ("concept/alpha",)},
        include_shared=False,
    )
    existing = ExistingPageRecord(
        page_id=uuid4(),
        version=1,
        page=ReducedPage(
            slug="concept/alpha",
            title="Alpha",
            page_type="concept",
            content="Body",
            summary="Summary",
            source_refs=("doc-z",),
        ),
    )
    model = OrderedTaxonomyModel(dataset)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")],
        existing={"concept/alpha": existing},
        contributions=[contribution("doc-z", "concept/alpha")],
        classifiable_slugs=("concept/alpha",),
    )

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert model.taxonomy_requests == []
    assert store.apply_calls[0].folder_assignments == ()


@pytest.mark.asyncio
async def test_empty_catalog_skips_embedding_but_still_classifies_new_topic() -> None:
    dataset = fake_dataset(
        ("doc-a",),
        concepts_by_knowledge={"doc-a": ("concept/alpha",)},
        include_shared=False,
    )
    model = OrderedTaxonomyModel(dataset)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")],
        folders=(),
        classifiable_slugs=("concept/alpha",),
    )

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert len(model.taxonomy_requests) == 1
    assert model.taxonomy_requests[0].allowed_bases == ()
    assignment = store.apply_calls[0].folder_assignments[0]
    assert assignment.slug == "concept/alpha"
    assert assignment.contributor_op_ids == (OP_A,)


@pytest.mark.asyncio
async def test_historical_only_topic_is_excluded_by_taxonomy_context() -> None:
    dataset = fake_dataset(
        ("doc-a",),
        concepts_by_knowledge={"doc-a": ("concept/alpha",)},
        include_shared=False,
    )
    model = OrderedTaxonomyModel(dataset)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")],
        existing={},
        classifiable_slugs=(),
    )

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert "concept/alpha" in store.taxonomy_context_calls[0]
    assert model.taxonomy_requests == []
    assert store.apply_calls[0].folder_assignments == ()


@pytest.mark.asyncio
async def test_taxonomy_batch_failure_isolates_only_contributing_operation() -> None:
    dataset = fake_dataset(
        concepts_by_knowledge={
            "doc-a": ("concept/alpha",),
            "doc-b": ("concept/beta",),
        },
        include_shared=False,
    )
    model = OrderedTaxonomyModel(
        dataset,
        permanent_batches={"concept/alpha"},
    )
    store = WorkerStore(
        [pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")],
        classifiable_slugs=("concept/alpha", "concept/beta"),
    )

    result = await worker(
        store,
        FakeKnowledgeSource(dataset),
        model,
        options=WikiWorkerOptions(taxonomy_topic_batch_size=1),
    ).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_B,)
    assert result.failed_op_ids == (OP_A,)
    assert [item.slug for item in store.apply_calls[0].folder_assignments] == [
        "concept/beta"
    ]
    assert store.apply_calls[0].failures[0].error_code == "MODEL_PERMANENT"


@pytest.mark.asyncio
async def test_embedding_and_taxonomy_transient_failures_retry_with_existing_backoff() -> (
    None
):
    dataset = fake_dataset(
        ("doc-a",),
        concepts_by_knowledge={"doc-a": ("concept/alpha",)},
        include_shared=False,
    )
    folders = (
        FolderCatalogEntry(
            id=FOLDER_ROOT,
            name="Root",
            path="/Root",
            depth=1,
        ),
        FolderCatalogEntry(
            id=FOLDER_CHILD,
            parent_id=FOLDER_ROOT,
            name="Child",
            path="/Root/Child",
            depth=2,
        ),
    )
    embedding = FailingEmbedding(transient_failures=2)
    model = OrderedTaxonomyModel(
        dataset,
        transient_batches={"concept/alpha": 2},
    )
    waits: list[int] = []
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")],
        folders=folders,
        classifiable_slugs=("concept/alpha",),
    )

    result = await worker(
        store,
        FakeKnowledgeSource(dataset),
        model,
        embedding_model=embedding,
        waits=waits,
        options=WikiWorkerOptions(taxonomy_full_catalog_limit=1),
    ).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert embedding.calls == 3
    assert model.calls.count("taxonomy:concept/alpha") == 3
    assert waits == [2, 4, 2, 4]


@pytest.mark.asyncio
async def test_sixty_one_topics_use_configured_batches_and_bounded_parallelism() -> (
    None
):
    slugs = tuple(f"concept/topic-{index:02d}" for index in range(61))
    dataset = fake_dataset(
        ("doc-a",),
        concepts_by_knowledge={"doc-a": slugs},
        include_shared=False,
    )
    model = OrderedTaxonomyModel(dataset, block_taxonomy=True, started_target=2)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")],
        classifiable_slugs=slugs,
    )
    task = asyncio.create_task(
        worker(
            store,
            FakeKnowledgeSource(dataset),
            model,
            options=WikiWorkerOptions(
                taxonomy_topic_batch_size=20,
                taxonomy_parallel=2,
            ),
        ).run_batch(SCOPE)
    )
    await asyncio.wait_for(model.taxonomy_started.wait(), timeout=1)

    assert model.taxonomy_maximum == 2
    model.release_taxonomy.set()
    result = await asyncio.wait_for(task, timeout=5)

    assert result.completed_op_ids == (OP_A,)
    assert [len(request.topics) for request in model.taxonomy_requests] == [
        20,
        20,
        20,
        1,
    ]
    assert len(store.apply_calls[0].folder_assignments) == 61


@pytest.mark.asyncio
async def test_taxonomy_child_cancellation_cleans_sibling_and_releases_claim() -> None:
    dataset = fake_dataset(
        concepts_by_knowledge={
            "doc-a": ("concept/alpha",),
            "doc-b": ("concept/beta",),
        },
        include_shared=False,
    )

    class CancellingTaxonomyModel(OrderedTaxonomyModel):
        def __init__(self) -> None:
            super().__init__(dataset)
            self.sibling_started = asyncio.Event()
            self.sibling_cleaned = asyncio.Event()

        async def plan_folders(self, request: TaxonomyRequest) -> TaxonomyOutput:
            slug = request.topics[0].slug
            if slug == "concept/alpha":
                await self.sibling_started.wait()
                raise asyncio.CancelledError
            self.sibling_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.sibling_cleaned.set()
            raise AssertionError("unreachable")

    model = CancellingTaxonomyModel()
    store = WorkerStore(
        [pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")],
        classifiable_slugs=("concept/alpha", "concept/beta"),
    )

    with pytest.raises(asyncio.CancelledError):
        await worker(
            store,
            FakeKnowledgeSource(dataset),
            model,
            options=WikiWorkerOptions(
                taxonomy_topic_batch_size=1,
                taxonomy_parallel=2,
            ),
        ).run_batch(SCOPE)

    assert model.sibling_cleaned.is_set()
    assert store.apply_calls == []
    assert store.release_calls == [(SCOPE, [OP_A, OP_B], CLAIM_TOKEN)]


@pytest.mark.asyncio
async def test_source_invalidated_after_map_is_superseded_before_commit() -> None:
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
    assert result.failed_op_ids == []
    assert result.superseded_op_ids == [OP_A]
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
    assert store.apply_calls[0].pages == ()
    assert model.calls == []


@pytest.mark.asyncio
async def test_summary_only_batch_skips_taxonomy_context() -> None:
    dataset = fake_dataset(("doc-a",), include_shared=False)
    model = FakeChatModel(dataset)
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert [page.slug for page in store.apply_calls[0].pages] == ["summary/doc-a"]
    assert store.taxonomy_context_calls == []


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
    dataset = fake_dataset(("doc-a",), transient_failures={"merge:entity/shared": 3})
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
    assert store.apply_calls[0].pages == ()


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
        assert store.release_calls == [(SCOPE, [OP_A, OP_B], CLAIM_TOKEN)]
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
        assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]
    finally:
        model.release_sibling.set()
        await asyncio.wait_for(model.sibling_cleaned.wait(), timeout=1)


@pytest.mark.asyncio
async def test_lock_loss_never_commits_and_releases_claim() -> None:
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
    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]


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
    assert store.apply_calls[0].failures == ()


@pytest.mark.asyncio
async def test_claim_lost_releases_entire_claim_without_operation_failure() -> None:
    dataset = fake_dataset(("doc-a",))
    store = WorkerStore([pending_op(OP_A, "doc-a")], claim_lost=True)

    with pytest.raises(ClaimLost):
        await worker(
            store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)
        ).run_batch(SCOPE)

    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]
    assert store.apply_calls[0].failures == ()


@pytest.mark.asyncio
async def test_parent_cancellation_during_conflict_release_is_propagated_after_drain() -> (
    None
):
    dataset = fake_dataset(("doc-a",))

    class BlockingReleaseStore(WorkerStore):
        def __init__(self) -> None:
            super().__init__([pending_op(OP_A, "doc-a")], conflict=True)
            self.release_started = asyncio.Event()
            self.release_allowed = asyncio.Event()
            self.release_finished = False

        async def release_claim(
            self, scope: WikiScope, ids: list[UUID], claim_token: UUID
        ) -> None:
            self.release_started.set()
            try:
                await self.release_allowed.wait()
                await super().release_claim(scope, ids, claim_token)
            finally:
                self.release_finished = True

    store = BlockingReleaseStore()
    task = asyncio.create_task(
        worker(store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)).run_batch(
            SCOPE
        )
    )
    await asyncio.wait_for(store.release_started.wait(), timeout=1)
    task.cancel()
    await asyncio.sleep(0)
    store.release_allowed.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert store.release_finished
    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]


@pytest.mark.asyncio
async def test_parent_cancellation_during_claim_lost_release_takes_precedence() -> None:
    dataset = fake_dataset(("doc-a",))

    class BlockingReleaseStore(WorkerStore):
        def __init__(self) -> None:
            super().__init__([pending_op(OP_A, "doc-a")], claim_lost=True)
            self.release_started = asyncio.Event()
            self.release_allowed = asyncio.Event()
            self.release_finished = False

        async def release_claim(
            self, scope: WikiScope, ids: list[UUID], claim_token: UUID
        ) -> None:
            self.release_started.set()
            try:
                await self.release_allowed.wait()
                await super().release_claim(scope, ids, claim_token)
            finally:
                self.release_finished = True

    store = BlockingReleaseStore()
    task = asyncio.create_task(
        worker(store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)).run_batch(
            SCOPE
        )
    )
    await asyncio.wait_for(store.release_started.wait(), timeout=1)
    task.cancel()
    await asyncio.sleep(0)
    store.release_allowed.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert store.release_finished
    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]


@pytest.mark.asyncio
async def test_same_turn_conflict_release_and_parent_cancellation_prefers_cancel() -> (
    None
):
    dataset = fake_dataset(("doc-a",))

    class BarrierReleaseStore(WorkerStore):
        def __init__(self) -> None:
            super().__init__([pending_op(OP_A, "doc-a")], conflict=True)
            self.release_started = asyncio.Event()
            self.release_allowed = asyncio.Event()
            self.release_finished = False

        async def release_claim(
            self, scope: WikiScope, ids: list[UUID], claim_token: UUID
        ) -> None:
            self.release_started.set()
            try:
                await self.release_allowed.wait()
                await super().release_claim(scope, ids, claim_token)
            finally:
                self.release_finished = True

    store = BarrierReleaseStore()
    task = asyncio.create_task(
        worker(store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)).run_batch(
            SCOPE
        )
    )
    await asyncio.wait_for(store.release_started.wait(), timeout=1)

    store.release_allowed.set()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert store.release_finished
    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]


@pytest.mark.asyncio
async def test_same_turn_claim_lost_release_and_parent_cancellation_prefers_cancel() -> (
    None
):
    dataset = fake_dataset(("doc-a",))

    class BarrierReleaseStore(WorkerStore):
        def __init__(self) -> None:
            super().__init__([pending_op(OP_A, "doc-a")], claim_lost=True)
            self.release_started = asyncio.Event()
            self.release_allowed = asyncio.Event()
            self.release_finished = False

        async def release_claim(
            self, scope: WikiScope, ids: list[UUID], claim_token: UUID
        ) -> None:
            self.release_started.set()
            try:
                await self.release_allowed.wait()
                await super().release_claim(scope, ids, claim_token)
            finally:
                self.release_finished = True

    store = BarrierReleaseStore()
    task = asyncio.create_task(
        worker(store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)).run_batch(
            SCOPE
        )
    )
    await asyncio.wait_for(store.release_started.wait(), timeout=1)

    store.release_allowed.set()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert store.release_finished
    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]


@pytest.mark.asyncio
async def test_same_turn_cancellation_retrieves_release_failure_as_cause() -> None:
    dataset = fake_dataset(("doc-a",))

    class ReleaseFailure(RuntimeError):
        pass

    class FailingReleaseStore(WorkerStore):
        def __init__(self) -> None:
            super().__init__([pending_op(OP_A, "doc-a")], conflict=True)
            self.release_started = asyncio.Event()
            self.release_allowed = asyncio.Event()

        async def release_claim(
            self, scope: WikiScope, ids: list[UUID], claim_token: UUID
        ) -> None:
            self.release_started.set()
            await self.release_allowed.wait()
            await super().release_claim(scope, ids, claim_token)
            raise ReleaseFailure("release failed")

    store = FailingReleaseStore()
    task = asyncio.create_task(
        worker(store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)).run_batch(
            SCOPE
        )
    )
    await asyncio.wait_for(store.release_started.wait(), timeout=1)

    store.release_allowed.set()
    task.cancel()

    with pytest.raises(asyncio.CancelledError) as error:
        await asyncio.wait_for(task, timeout=1)
    assert isinstance(error.value.__cause__, ReleaseFailure)
    assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]


@pytest.mark.asyncio
async def test_ingest_mapped_before_retract_tombstone_finishes_superseded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset = fake_dataset(("doc-a",))
    source = FakeKnowledgeSource(dataset)
    mapped = asyncio.Event()
    release_map = asyncio.Event()

    class MutableTombstones:
        def __init__(self) -> None:
            self.deleted: set[str] = set()

        async def mark_deleted(self, scope: WikiScope, knowledge_id: str) -> None:
            assert scope == SCOPE
            self.deleted.add(knowledge_id)

        async def is_deleted(self, scope: WikiScope, knowledge_id: str) -> bool:
            assert scope == SCOPE
            return knowledge_id in self.deleted

    class BarrierStore(WorkerStore):
        def __init__(self) -> None:
            super().__init__([pending_op(OP_A, "doc-a")])
            self.retract_enqueues: list[tuple[str, str]] = []

        async def enqueue_retract(
            self,
            scope: WikiScope,
            knowledge_id: str,
            op_version: str,
            payload: dict[str, object],
            *,
            delay_seconds: int = 30,
        ) -> EnqueueRecord:
            assert scope == SCOPE
            assert payload == {"knowledge_id": knowledge_id}
            assert delay_seconds == 30
            self.retract_enqueues.append((knowledge_id, op_version))
            return EnqueueRecord(
                id=OP_B,
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                knowledge_id=knowledge_id,
                op_version=op_version,
                payload=payload,
                outbox_event_id=uuid4(),
                deduplicated=False,
            )

    original_map_document = worker_module.map_document

    async def paused_map_document(*args, **kwargs):
        result = await original_map_document(*args, **kwargs)
        mapped.set()
        await release_map.wait()
        return result

    monkeypatch.setattr(worker_module, "map_document", paused_map_document)
    tombstones = MutableTombstones()
    store = BarrierStore()
    ingest_worker = worker(
        store,
        source,
        FakeChatModel(dataset),
        tombstones=tombstones,  # type: ignore[arg-type]
    )
    task = asyncio.create_task(ingest_worker.run_batch(SCOPE))
    await asyncio.wait_for(mapped.wait(), timeout=1)
    enqueue_result = await WikiEnqueueService(
        source,
        store,
        tombstones,  # type: ignore[arg-type]
    ).enqueue_retract(SCOPE, "doc-a", "delete-v2")
    release_map.set()
    result = await asyncio.wait_for(task, timeout=1)

    assert enqueue_result.pending_op_id == OP_B
    assert store.retract_enqueues == [("doc-a", "delete-v2")]
    assert result.superseded_op_ids == (OP_A,)
    assert result.completed_op_ids == result.failed_op_ids == ()
    assert store.contributions == []
    assert store.apply_calls[0].contribution_deltas == ()
    assert store.apply_calls[0].failures == ()


@pytest.mark.asyncio
async def test_single_citation_batch_permanent_failure_still_completes_ingest() -> None:
    dataset = fake_dataset(("doc-a",))
    model = FakeChatModel(dataset)
    store = WorkerStore([pending_op(OP_A, "doc-a")])

    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert result.failed_op_ids == result.superseded_op_ids == ()
    assert model.calls.count("citation:doc-a:0") == 1
    shared = next(
        page for page in store.apply_calls[0].pages if page.slug == "entity/shared"
    )
    assert shared.chunk_refs == ()


@pytest.mark.asyncio
async def test_summary_transient_failure_uses_exactly_three_map_attempts() -> None:
    dataset = fake_dataset(("doc-a",), transient_failures={"summarize:doc-a": 2})
    model = FakeChatModel(dataset)
    waits: list[int] = []

    result = await worker(
        WorkerStore([pending_op(OP_A, "doc-a")]),
        FakeKnowledgeSource(dataset),
        model,
        waits=waits,
    ).run_batch(SCOPE)

    assert result.completed_op_ids == (OP_A,)
    assert model.calls.count("summarize:doc-a") == 3
    assert waits == [2, 4]


@pytest.mark.asyncio
async def test_retract_reduce_failure_reaches_dead_letter_after_five_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset = fake_dataset(("doc-a",), include_shared=False)
    retract_pending = contribution("doc-a", "entity/retract", state="retract_pending")
    existing = ExistingPageRecord(
        page_id=uuid4(),
        version=1,
        page=ReducedPage(
            slug="entity/retract",
            title="Retract",
            page_type="entity",
            content="Body",
            summary="Summary",
            source_refs=["doc-a"],
        ),
    )

    class RetryingRetractStore(WorkerStore):
        def __init__(self) -> None:
            super().__init__(
                [pending_op(OP_A, "doc-a", op="retract")],
                existing={"entity/retract": existing},
                contributions=[retract_pending],
            )
            self.dead: list[UUID] = []

        async def apply_results_with_outcome(
            self, scope: WikiScope, request: BatchApplyRequest
        ) -> BatchApplyOutcome:
            assert scope == SCOPE
            call = BatchApplyRequest.model_validate(request.model_dump(mode="python"))
            self.apply_calls.append(call)
            assert len(call.failures) == 1
            current = self.records[0]
            next_count = current.fail_count + 1
            if next_count == 5:
                self.dead.append(current.id)
                self.records = []
            else:
                self.records = [
                    replace(
                        current,
                        fail_count=next_count,
                        claimed_at=NOW,
                        claim_token=CLAIM_TOKEN,
                    )
                ]
            return BatchApplyOutcome(
                applied=True,
                failed_op_ids=(current.id,),
            )

    async def failing_reduce(*args, **kwargs):
        raise PermanentModelError("retract reduce failed")

    monkeypatch.setattr(worker_module, "reduce_slug", failing_reduce)
    store = RetryingRetractStore()
    results = []
    for expected_fail_count in range(1, 6):
        result = await worker(
            store, FakeKnowledgeSource(dataset), FakeChatModel(dataset)
        ).run_batch(SCOPE)
        results.append(result)
        if expected_fail_count < 5:
            assert store.records[0].fail_count == expected_fail_count

    assert [result.failed_op_ids for result in results] == [(OP_A,)] * 5
    assert store.dead == [OP_A]
    assert store.records == []
    assert all(call.failures for call in store.apply_calls)
