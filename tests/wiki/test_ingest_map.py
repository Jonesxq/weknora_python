from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

from app.wiki.ingest.map_document import (
    has_meaningful_text,
    map_document,
    rebuild_source_content,
)
from app.wiki.ingest.schemas import (
    CandidateExtraction,
    DocumentSummary,
    SourceChunk,
    SourceKnowledge,
    TopicCandidate,
    WikiIngestConfig,
    WikiWorkerOptions,
)
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


def test_rebuild_content_sorts_parts_and_truncates_unicode_characters() -> None:
    chunks = [
        SourceChunk(id="b", chunk_index=2, start_at=0, text=" 后文 "),
        SourceChunk(
            id="z", chunk_index=1, start_at=5, text=" 前文 ", ocr_text=" OCR "
        ),
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
async def test_map_document_rejects_source_identity_mismatch_before_other_calls() -> None:
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
    config = WikiIngestConfig(
        extraction_granularity="focused", max_pages_per_ingest=3
    )
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
