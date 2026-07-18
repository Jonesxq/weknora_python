from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql

from app.wiki.ingest.map_document import (
    has_meaningful_text,
    map_document,
    rebuild_source_content,
)
from app.wiki.ingest.schemas import (
    CandidateExtraction,
    CitationBatchOutput,
    DedupDecision,
    DedupOutput,
    DedupPageCandidate,
    DocumentSummary,
    MapDocumentResult,
    SourceChunk,
    SourceKnowledge,
    StoredContributionRecord,
    TopicCandidate,
    WikiIngestConfig,
    WikiWorkerOptions,
)
from app.wiki.ingest.store import SqlAlchemyIngestStore
from app.wiki.models import WikiPageContribution
from app.wiki.scope import WikiScope


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
PENDING_OP_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
SCOPE = WikiScope(tenant_id=1, knowledge_base_id=KB_ID, actor_id="worker")
_DEFAULT = object()


class StubSource:
    def __init__(
        self,
        *,
        knowledge: SourceKnowledge | None | object = _DEFAULT,
        config: WikiIngestConfig | None = None,
        chunks: list[SourceChunk] | object = _DEFAULT,
        active: bool = True,
    ) -> None:
        self.knowledge = (
            SourceKnowledge(
                id="knowledge-1",
                tenant_id=1,
                knowledge_base_id=KB_ID,
                title="Document One",
                op_version="version-1",
            )
            if knowledge is _DEFAULT
            else knowledge
        )
        self.config = config or WikiIngestConfig(
            extraction_granularity="exhaustive",
            max_pages_per_ingest=0,
        )
        self.chunks = (
            [SourceChunk(id="chunk-1", text="This document has meaningful content.")]
            if chunks is _DEFAULT
            else chunks
        )
        self.active = active
        self.active_calls: list[tuple[str, str]] = []
        self.chunk_calls: list[str] = []

    async def get_config(self, scope: WikiScope) -> WikiIngestConfig:
        return self.config

    async def get_knowledge(
        self, scope: WikiScope, knowledge_id: str
    ) -> SourceKnowledge | None:
        assert self.knowledge is None or isinstance(self.knowledge, SourceKnowledge)
        return self.knowledge

    async def list_chunks(
        self, scope: WikiScope, knowledge_id: str
    ) -> list[SourceChunk]:
        assert isinstance(self.chunks, list)
        self.chunk_calls.append(knowledge_id)
        return self.chunks

    async def is_active(
        self, scope: WikiScope, knowledge_id: str, op_version: str
    ) -> bool:
        self.active_calls.append((knowledge_id, op_version))
        return self.active


class RecordingModel:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, object]] = []
        self.extract_input: tuple[str, str, WikiIngestConfig] | None = None
        self.summary_input: tuple[str, str, str] | None = None
        self.extraction = CandidateExtraction(
            entities=[
                TopicCandidate(
                    name="Acme",
                    slug="entity/acme",
                    page_type="entity",
                    aliases=["Acme Inc."],
                    description="An example company.",
                    details="Acme builds retrieval tools.",
                ),
                TopicCandidate(
                    name="Beta",
                    slug="entity/beta",
                    page_type="entity",
                    details="A second entity.",
                ),
            ],
            concepts=[
                TopicCandidate(
                    name="Retrieval",
                    slug="concept/retrieval",
                    page_type="concept",
                    aliases=["Search"],
                    description="Finding relevant material.",
                    details="Retrieval grounds answers.",
                )
            ],
        )
        self.document_summary = DocumentSummary(
            headline="Document headline", markdown="Document summary context."
        )

    async def extract_candidates(
        self, knowledge_id: str, text: str, config: WikiIngestConfig
    ) -> CandidateExtraction:
        self.extract_input = (knowledge_id, text, config)
        self.calls.append(("extract_candidates", knowledge_id, config))
        return self.extraction

    async def summarize(
        self, knowledge_id: str, title: str, text: str
    ) -> DocumentSummary:
        self.summary_input = (knowledge_id, title, text)
        self.calls.append(("summarize", knowledge_id, title))
        return self.document_summary

    async def merge_page(self, request: object) -> object:
        raise AssertionError("Map 阶段不应调用 merge_page")


class IncrementalStore:
    def __init__(
        self,
        previous: list[StoredContributionRecord] | None = None,
        *,
        dedup_targets: dict[str, list[DedupPageCandidate]] | None = None,
    ) -> None:
        self.previous = list(previous or [])
        self.dedup_targets = dedup_targets or {}
        self.events: list[tuple[object, ...]] = []
        self.deleted_contributions: list[object] = []

    async def list_source_contributions(
        self,
        scope: WikiScope,
        knowledge_id: str,
        *,
        state: str,
    ) -> list[StoredContributionRecord]:
        self.events.append(("list_source_contributions", scope, knowledge_id, state))
        return list(self.previous)

    async def find_existing_pages(
        self, scope: WikiScope, slugs: list[str]
    ) -> dict[str, object]:
        self.events.append(("find_existing_pages", scope, tuple(slugs)))
        return {}

    async def find_dedup_candidates(
        self,
        scope: WikiScope,
        candidate: TopicCandidate,
        limit: int = 20,
    ) -> list[DedupPageCandidate]:
        self.events.append(("find_dedup_candidates", scope, candidate.slug, limit))
        return self.dedup_targets.get(candidate.slug, [])[:limit]


class MemoryTombstones:
    def __init__(self, deleted: bool = False) -> None:
        self.deleted = deleted
        self.calls: list[tuple[WikiScope, str]] = []

    async def is_deleted(self, scope: WikiScope, knowledge_id: str) -> bool:
        self.calls.append((scope, knowledge_id))
        return self.deleted

    async def mark_deleted(self, scope: WikiScope, knowledge_id: str) -> None:
        raise AssertionError("Map 阶段不应写 tombstone")


class IncrementalModel(RecordingModel):
    def __init__(self) -> None:
        super().__init__()
        self.citation_output = CitationBatchOutput()
        self.dedup_mapping: dict[str, str | None] = {}

    async def classify_chunks(self, request: object) -> CitationBatchOutput:
        self.calls.append(("classify_chunks", request, None))
        return self.citation_output

    async def resolve_duplicates(self, request: object) -> DedupOutput:
        candidates = request.candidates  # type: ignore[attr-defined]
        self.calls.append(("resolve_duplicates", request, None))
        return DedupOutput(
            decisions=tuple(
                DedupDecision(
                    candidate_slug=item.candidate.slug,
                    canonical_slug=self.dedup_mapping.get(item.candidate.slug),
                )
                for item in candidates
            )
        )


def contribution(
    slug: str,
    *,
    version: str = "version-1",
    page_type: str | None = None,
    title: str = "Old title",
    content: str = "Old content",
    summary: str = "Old summary",
    aliases: tuple[str, ...] = (),
    refs: tuple[str, ...] = (),
) -> StoredContributionRecord:
    return StoredContributionRecord(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        slug=slug,
        knowledge_id="knowledge-1",
        op_version=version,
        page_type=page_type or slug.split("/", 1)[0],
        state="active",
        title=title,
        content=content,
        summary=summary,
        aliases=aliases,
        chunk_refs=refs,
    )


def test_rebuild_content_sorts_parts_and_truncates_unicode_characters() -> None:
    chunks = [
        SourceChunk(id="b", chunk_index=2, start_at=0, text=" 后文 "),
        SourceChunk(id="z", chunk_index=1, start_at=5, text=" 前文 ", ocr_text=" OCR "),
        SourceChunk(id="a", chunk_index=1, start_at=5, image_caption=" 图片 "),
    ]

    assert rebuild_source_content(chunks) == "图片\n前文\nOCR\n后文"
    assert rebuild_source_content(chunks, max_chars=5) == "图片\n前文"
    assert rebuild_source_content(chunks, max_chars=0) == ""
    with pytest.raises(ValueError, match="max_chars"):
        rebuild_source_content(chunks, max_chars=-1)


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (" ![diagram](/image.png) \n short ", False),
        ("12345 6789", False),
        ("12345 67890", True),
        ("![x](image.png)十个有效字符正好通过测试", True),
        ("![diagram][image-ref]\n[image-ref]: /image.png", False),
    ],
)
def test_has_meaningful_text_ignores_markdown_images(
    content: str, expected: bool
) -> None:
    assert has_meaningful_text(content) is expected


@pytest.mark.asyncio
async def test_map_document_skips_missing_inactive_and_short_sources() -> None:
    model = RecordingModel()
    missing = StubSource(knowledge=None)

    missing_result = await map_document(SCOPE, "knowledge-1", missing, model)
    inactive_result = await map_document(
        SCOPE, "knowledge-1", StubSource(active=False), model
    )
    short_result = await map_document(
        SCOPE,
        "knowledge-1",
        StubSource(chunks=[SourceChunk(id="1", text=" ![](/image.png) short ")]),
        model,
    )
    empty_result = await map_document(
        SCOPE, "knowledge-1", StubSource(chunks=[]), model
    )

    assert missing_result.skipped_reason == "knowledge_not_found"
    assert inactive_result.skipped_reason == "source_inactive"
    assert short_result.skipped_reason == "insufficient_text"
    assert empty_result.skipped_reason == "insufficient_text"
    assert model.calls == []
    assert inactive_result.updates == short_result.updates == []


@pytest.mark.asyncio
async def test_map_document_rejects_source_identity_mismatch_before_other_calls() -> (
    None
):
    returned = SourceKnowledge(
        id="knowledge-2",
        tenant_id=1,
        knowledge_base_id=KB_ID,
        title="Wrong Document",
        op_version="version-2",
    )
    source = StubSource(knowledge=returned)
    model = RecordingModel()

    result = await map_document(SCOPE, "knowledge-1", source, model)

    assert result.skipped_reason == "source_identity_mismatch"
    assert source.active_calls == []
    assert source.chunk_calls == []
    assert model.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledge_id", ["knowledge:1", "Doc-A", "x" * 248])
async def test_map_document_rejects_noncanonical_summary_identity_before_models(
    knowledge_id: str,
) -> None:
    knowledge = SourceKnowledge(
        id=knowledge_id,
        tenant_id=1,
        knowledge_base_id=KB_ID,
        title="Invalid Identity",
        op_version="version-1",
    )
    source = StubSource(knowledge=knowledge)
    model = RecordingModel()

    result = await map_document(SCOPE, knowledge_id, source, model)

    assert result.skipped_reason == "invalid_knowledge_id"
    assert source.active_calls == []
    assert source.chunk_calls == []
    assert model.calls == []


@pytest.mark.asyncio
async def test_map_document_builds_ordered_summary_entity_and_concept_updates() -> None:
    source = StubSource()
    model = RecordingModel()

    result = await map_document(
        SCOPE,
        "knowledge-1",
        source,
        model,
        pending_op_id=PENDING_OP_ID,
    )

    assert result.pending_op_id == PENDING_OP_ID
    assert result.skipped_reason is None
    assert [update.slug for update in result.updates] == [
        "summary/knowledge-1",
        "entity/acme",
        "entity/beta",
        "concept/retrieval",
    ]
    assert result.updates[0].title == "Document One 摘要"
    assert result.updates[0].content == "Document summary context."
    assert result.updates[0].summary == "Document headline"
    entity = result.updates[1]
    assert (entity.title, entity.page_type, entity.aliases) == (
        "Acme",
        "entity",
        ["Acme Inc."],
    )
    assert entity.content == "Acme builds retrieval tools."
    assert entity.summary == "An example company.\n\nDocument summary context."
    assert all(update.source_refs == ["knowledge-1"] for update in result.updates)
    assert all(update.chunk_refs == [] for update in result.updates)
    assert source.active_calls == [("knowledge-1", "version-1")]
    assert [(call[0], call[1]) for call in model.calls] == [
        ("extract_candidates", "knowledge-1"),
        ("summarize", "knowledge-1"),
    ]
    expected_content = "This document has meaningful content."
    assert model.extract_input is not None
    assert model.extract_input[:2] == ("knowledge-1", expected_content)
    assert model.summary_input == (
        "knowledge-1",
        "Document One",
        expected_content,
    )


@pytest.mark.asyncio
async def test_map_document_truncates_same_input_for_both_model_calls() -> None:
    source = StubSource(chunks=[SourceChunk(id="chunk", text="abcdefghijklmno")])
    model = RecordingModel()

    await map_document(SCOPE, "knowledge-1", source, model, max_chars=10)

    assert model.extract_input is not None
    assert model.summary_input is not None
    assert model.extract_input[1] == model.summary_input[2] == "abcdefghij"


@pytest.mark.asyncio
async def test_map_document_starts_model_calls_concurrently() -> None:
    both_started = asyncio.Event()
    release = asyncio.Event()
    started: set[str] = set()

    class BarrierModel(RecordingModel):
        async def _wait_for_peer(self, name: str) -> None:
            started.add(name)
            if len(started) == 2:
                both_started.set()
            await release.wait()

        async def extract_candidates(
            self, knowledge_id: str, text: str, config: WikiIngestConfig
        ) -> CandidateExtraction:
            await self._wait_for_peer("extract")
            return await super().extract_candidates(knowledge_id, text, config)

        async def summarize(
            self, knowledge_id: str, title: str, text: str
        ) -> DocumentSummary:
            await self._wait_for_peer("summarize")
            return await super().summarize(knowledge_id, title, text)

    task = asyncio.create_task(
        map_document(SCOPE, "knowledge-1", StubSource(), BarrierModel())
    )
    try:
        await asyncio.wait_for(both_started.wait(), timeout=1)
        release.set()
        await task
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    assert started == {"extract", "summarize"}


@pytest.mark.asyncio
async def test_map_document_cancels_and_awaits_sibling_after_model_failure() -> None:
    sibling_started = asyncio.Event()
    sibling_cancelled = asyncio.Event()
    failure = RuntimeError("extract failed")

    class FailingModel(RecordingModel):
        async def extract_candidates(
            self, knowledge_id: str, text: str, config: WikiIngestConfig
        ) -> CandidateExtraction:
            await sibling_started.wait()
            raise failure

        async def summarize(
            self, knowledge_id: str, title: str, text: str
        ) -> DocumentSummary:
            sibling_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                sibling_cancelled.set()

    with pytest.raises(RuntimeError) as error:
        await map_document(SCOPE, "knowledge-1", StubSource(), FailingModel())

    assert error.value is failure
    assert sibling_cancelled.is_set()
    assert not any(
        task.get_name().startswith("wiki-map-") and not task.done()
        for task in asyncio.all_tasks()
    )


@pytest.mark.asyncio
async def test_parent_cancellation_cleans_up_both_model_tasks() -> None:
    started = asyncio.Event()
    cancelled: set[str] = set()
    entered: set[str] = set()

    class BlockingModel(RecordingModel):
        async def _block(self, name: str) -> None:
            entered.add(name)
            if len(entered) == 2:
                started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.add(name)

        async def extract_candidates(
            self, knowledge_id: str, text: str, config: WikiIngestConfig
        ) -> CandidateExtraction:
            await self._block("extract")
            raise AssertionError("unreachable")

        async def summarize(
            self, knowledge_id: str, title: str, text: str
        ) -> DocumentSummary:
            await self._block("summarize")
            raise AssertionError("unreachable")

    parent = asyncio.create_task(
        map_document(SCOPE, "knowledge-1", StubSource(), BlockingModel())
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        parent.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parent
    finally:
        if not parent.done():
            parent.cancel()
            await asyncio.gather(parent, return_exceptions=True)

    assert cancelled == {"extract", "summarize"}
    assert not any(
        task.get_name().startswith("wiki-map-") and not task.done()
        for task in asyncio.all_tasks()
    )


@pytest.mark.asyncio
async def test_worker_options_override_a_copy_and_limit_only_topic_updates() -> None:
    config = WikiIngestConfig(extraction_granularity="focused", max_pages_per_ingest=3)
    source = StubSource(config=config)
    model = RecordingModel()

    result = await map_document(
        SCOPE,
        "knowledge-1",
        source,
        model,
        options=WikiWorkerOptions(
            extraction_granularity="exhaustive", max_pages_per_ingest=2
        ),
    )

    assert [update.slug for update in result.updates] == [
        "summary/knowledge-1",
        "entity/acme",
        "entity/beta",
    ]
    model_config = model.calls[0][2]
    assert isinstance(model_config, WikiIngestConfig)
    assert model_config.extraction_granularity == "exhaustive"
    assert model_config.max_pages_per_ingest == 2
    assert model_config is not config
    assert config.extraction_granularity == "focused"
    assert config.max_pages_per_ingest == 3


@pytest.mark.asyncio
async def test_zero_worker_limit_falls_back_to_fixture_limit() -> None:
    source = StubSource(
        config=WikiIngestConfig(max_pages_per_ingest=1),
    )
    model = RecordingModel()

    result = await map_document(
        SCOPE,
        "knowledge-1",
        source,
        model,
        options=WikiWorkerOptions(max_pages_per_ingest=0),
    )

    assert [update.slug for update in result.updates] == [
        "summary/knowledge-1",
        "entity/acme",
    ]
    assert model.calls[0][2].extraction_granularity == "standard"


@pytest.mark.asyncio
async def test_incremental_map_builds_real_chunk_refs_and_add_deltas() -> None:
    source = StubSource(
        chunks=[
            SourceChunk(id="chunk-2", chunk_index=2, text="Second useful section."),
            SourceChunk(id="empty", chunk_index=0, text=" ", ocr_text=""),
            SourceChunk(id="chunk-1", chunk_index=1, text="First useful section."),
        ]
    )
    model = IncrementalModel()
    model.citation_output = CitationBatchOutput(
        refs_by_slug={
            "entity/acme": ("c001", "c000"),
            "concept/retrieval": ("c000",),
        }
    )
    store = IncrementalStore()
    tombstones = MemoryTombstones()

    result = await map_document(
        SCOPE,
        "knowledge-1",
        source,
        model,
        store,
        tombstones,
        pending_op_id=PENDING_OP_ID,
        op_version="trusted-version-1",
        options=WikiWorkerOptions(),
    )

    assert result.pending_op_id == PENDING_OP_ID
    assert result.knowledge_id == "knowledge-1"
    assert result.skipped_reason is None
    assert result.superseded is False
    assert isinstance(result.contribution_deltas, tuple)
    assert [(item.action, item.slug) for item in result.contribution_deltas] == [
        ("add", "summary/knowledge-1"),
        ("add", "entity/acme"),
        ("add", "entity/beta"),
        ("add", "concept/retrieval"),
    ]
    by_slug = {item.slug: item.current for item in result.contribution_deltas}
    assert by_slug["summary/knowledge-1"] is not None
    assert by_slug["summary/knowledge-1"].chunk_refs == ("chunk-1", "chunk-2")
    assert by_slug["entity/acme"] is not None
    assert by_slug["entity/acme"].chunk_refs == ("chunk-1", "chunk-2")
    assert by_slug["entity/beta"] is not None
    assert by_slug["entity/beta"].chunk_refs == ()
    assert by_slug["concept/retrieval"] is not None
    assert by_slug["concept/retrieval"].chunk_refs == ("chunk-1",)
    assert all(
        ref != "c000"
        for current in by_slug.values()
        if current is not None
        for ref in current.chunk_refs
    )
    assert all(
        current is not None
        and current.tenant_id == SCOPE.tenant_id
        and current.knowledge_base_id == SCOPE.knowledge_base_id
        and current.knowledge_id == "knowledge-1"
        and current.op_version == "trusted-version-1"
        for current in by_slug.values()
    )
    assert source.active_calls == [("knowledge-1", "trusted-version-1")]
    assert store.events[0] == (
        "list_source_contributions",
        SCOPE,
        "knowledge-1",
        "active",
    )
    with pytest.raises((ValidationError, FrozenInstanceError)):
        result.superseded = True
    legacy_update = result.updates[1]
    with pytest.raises((ValidationError, FrozenInstanceError)):
        legacy_update.title = "mutated"
    with pytest.raises(AttributeError):
        legacy_update.aliases.append("mutated")


@pytest.mark.asyncio
async def test_reparse_plans_replace_then_stale_without_mutating_old_records() -> None:
    previous = [
        contribution("summary/knowledge-1", page_type="summary"),
        contribution("entity/acme"),
        contribution("entity/retired"),
    ]
    before = [record.model_dump() for record in previous]
    store = IncrementalStore(previous)
    source = StubSource()
    model = IncrementalModel()
    model.extraction = CandidateExtraction(
        entities=[model.extraction.entities[0]],
        concepts=[],
    )
    model.citation_output = CitationBatchOutput(refs_by_slug={"entity/acme": ("c000",)})

    result = await map_document(
        SCOPE,
        "knowledge-1",
        source,
        model,
        store,
        MemoryTombstones(),
        pending_op_id=PENDING_OP_ID,
        op_version="version-2",
        options=WikiWorkerOptions(),
    )

    assert [(item.action, item.slug) for item in result.contribution_deltas] == [
        ("replace", "summary/knowledge-1"),
        ("replace", "entity/acme"),
        ("retract_stale", "entity/retired"),
    ]
    assert [record.model_dump() for record in previous] == before
    assert store.deleted_contributions == []
    assert [event[0] for event in store.events].count("list_source_contributions") == 1


@pytest.mark.asyncio
async def test_tombstoned_ingest_is_superseded_before_store_and_models() -> None:
    source = StubSource()
    model = IncrementalModel()
    store = IncrementalStore()
    tombstones = MemoryTombstones(deleted=True)

    result = await map_document(
        SCOPE,
        "knowledge-1",
        source,
        model,
        store,
        tombstones,
        pending_op_id=PENDING_OP_ID,
        op_version="version-1",
        options=WikiWorkerOptions(),
    )

    assert result.superseded is True
    assert result.contribution_deltas == ()
    assert result.skipped_reason is None
    assert tombstones.calls == [(SCOPE, "knowledge-1")]
    assert store.events == []
    assert model.calls == []


@pytest.mark.asyncio
async def test_supplemental_and_generated_citations_merge_into_canonical_slug() -> None:
    source = StubSource(
        chunks=[
            SourceChunk(id="chunk-1", chunk_index=0, text="First evidence section."),
            SourceChunk(id="chunk-2", chunk_index=1, text="Second evidence section."),
        ]
    )
    model = IncrementalModel()
    generated = TopicCandidate(
        name="Acme New",
        slug="entity/acme-new",
        page_type="entity",
        description="Generated description",
    )
    supplemental = TopicCandidate(
        name="Acme Alternative",
        slug="entity/acme-alt",
        page_type="entity",
        details="Supplemental details",
    )
    model.extraction = CandidateExtraction(entities=[generated])
    model.citation_output = CitationBatchOutput(
        refs_by_slug={
            "entity/acme-new": ("c001",),
            "entity/acme-alt": ("c000",),
        },
        supplemental_candidates=(supplemental,),
    )
    model.dedup_mapping = {
        "entity/acme-new": "entity/acme",
        "entity/acme-alt": "entity/acme",
    }
    canonical = DedupPageCandidate(
        slug="entity/acme",
        title="ACME",
        page_type="entity",
        aliases=("Company",),
    )
    store = IncrementalStore(
        dedup_targets={
            "entity/acme-new": [canonical],
            "entity/acme-alt": [canonical],
        }
    )

    result = await map_document(
        SCOPE,
        "knowledge-1",
        source,
        model,
        store,
        MemoryTombstones(),
        pending_op_id=PENDING_OP_ID,
        op_version="version-1",
        options=WikiWorkerOptions(),
    )

    canonical_delta = next(
        item for item in result.contribution_deltas if item.slug == "entity/acme"
    )
    assert canonical_delta.current is not None
    assert canonical_delta.current.chunk_refs == ("chunk-1", "chunk-2")
    assert canonical_delta.current.aliases == (
        "Company",
        "Acme New",
        "Acme Alternative",
    )
    assert all(
        item.slug not in {"entity/acme-new", "entity/acme-alt"}
        for item in result.contribution_deltas
    )


@pytest.mark.asyncio
async def test_map_propagates_dedup_failure() -> None:
    target = DedupPageCandidate(
        slug="entity/canonical", title="Canonical", page_type="entity"
    )
    store = IncrementalStore(
        dedup_targets={
            "entity/acme": [target],
            "entity/beta": [target],
        }
    )
    model = IncrementalModel()
    failure = RuntimeError("dedup failed")

    async def fail_dedup(_request: object) -> DedupOutput:
        raise failure

    model.resolve_duplicates = fail_dedup  # type: ignore[method-assign]

    with pytest.raises(RuntimeError) as error:
        await map_document(
            SCOPE,
            "knowledge-1",
            StubSource(),
            model,
            store,
            MemoryTombstones(),
            pending_op_id=PENDING_OP_ID,
            op_version="version-1",
            options=WikiWorkerOptions(),
        )

    assert error.value is failure


@pytest.mark.asyncio
async def test_topic_limit_excludes_candidates_before_dedup_failure_boundary() -> None:
    target = DedupPageCandidate(
        slug="entity/canonical", title="Canonical", page_type="entity"
    )
    store = IncrementalStore(dedup_targets={"entity/beta": [target]})
    model = IncrementalModel()
    failure = RuntimeError("excluded candidate must not reach dedup")

    async def fail_dedup(_request: object) -> DedupOutput:
        raise failure

    model.resolve_duplicates = fail_dedup  # type: ignore[method-assign]

    result = await map_document(
        SCOPE,
        "knowledge-1",
        StubSource(),
        model,
        store,
        MemoryTombstones(),
        pending_op_id=PENDING_OP_ID,
        op_version="version-1",
        options=WikiWorkerOptions(max_pages_per_ingest=1),
    )

    assert [item.slug for item in result.contribution_deltas] == [
        "summary/knowledge-1",
        "entity/acme",
    ]
    assert not any(
        event[0] == "find_dedup_candidates" and event[2] == "entity/beta"
        for event in store.events
    )


def test_map_result_rejects_conflicting_terminal_states() -> None:
    with pytest.raises(ValidationError):
        MapDocumentResult(
            pending_op_id=PENDING_OP_ID,
            knowledge_id="knowledge-1",
            skipped_reason="source_inactive",
            superseded=True,
        )


class _ContributionResult:
    def __init__(self, rows: list[WikiPageContribution]) -> None:
        self.rows = rows

    def scalars(self) -> _ContributionResult:
        return self

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self.rows)


class _ContributionSession:
    def __init__(self, rows: list[WikiPageContribution]) -> None:
        self.rows = rows
        self.statements: list[object] = []
        self.flush_calls = 0

    async def __aenter__(self) -> _ContributionSession:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, statement: object) -> _ContributionResult:
        self.statements.append(statement)
        return _ContributionResult(self.rows)

    async def flush(self) -> None:
        self.flush_calls += 1


class _ContributionSessionFactory:
    def __init__(self, session: _ContributionSession) -> None:
        self.session = session

    def __call__(self) -> _ContributionSession:
        return self.session


@pytest.mark.asyncio
async def test_store_lists_detached_scoped_contributions_in_stable_sql_order() -> None:
    row = WikiPageContribution(
        id=uuid4(),
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        slug="entity/acme",
        knowledge_id="knowledge-1",
        op_version="version-1",
        page_type="entity",
        state="active",
        title="Acme",
        content="Body",
        summary="Summary",
        aliases=["Company"],
        chunk_refs=["chunk-1"],
    )
    session = _ContributionSession([row])
    store = SqlAlchemyIngestStore(
        _ContributionSessionFactory(session),
        object(),  # type: ignore[arg-type]
    )

    records = await store.list_source_contributions(
        SCOPE, "knowledge-1", state="active"
    )

    sql = " ".join(
        str(session.statements[0].compile(dialect=postgresql.dialect())).split()
    )
    assert "wiki_page_contributions.tenant_id" in sql
    assert "wiki_page_contributions.knowledge_base_id" in sql
    assert "wiki_page_contributions.knowledge_id" in sql
    assert "wiki_page_contributions.state" in sql
    assert (
        "ORDER BY wiki_page_contributions.slug, "
        "wiki_page_contributions.op_version, wiki_page_contributions.id"
    ) in sql
    assert session.flush_calls == 0
    assert records[0].aliases == ("Company",)
    assert records[0].chunk_refs == ("chunk-1",)
    row.aliases.append("mutated")
    row.chunk_refs.append("mutated")
    assert records[0].aliases == ("Company",)
    assert records[0].chunk_refs == ("chunk-1",)
