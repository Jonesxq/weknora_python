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


class StubSource:
    def __init__(
        self,
        *,
        knowledge: SourceKnowledge | None = None,
        config: WikiIngestConfig | None = None,
        chunks: list[SourceChunk] | None = None,
        active: bool = True,
    ) -> None:
        self.knowledge = knowledge or SourceKnowledge(
            id="knowledge-1",
            tenant_id=1,
            knowledge_base_id=KB_ID,
            title="Document One",
            op_version="version-1",
        )
        self.config = config or WikiIngestConfig(
            extraction_granularity="exhaustive",
            max_pages_per_ingest=0,
        )
        self.chunks = chunks or [
            SourceChunk(id="chunk-1", text="This document has meaningful content.")
        ]
        self.active = active
        self.active_calls: list[tuple[str, str]] = []

    async def get_config(self, scope: WikiScope) -> WikiIngestConfig:
        return self.config

    async def get_knowledge(
        self, scope: WikiScope, knowledge_id: str
    ) -> SourceKnowledge | None:
        return self.knowledge

    async def list_chunks(
        self, scope: WikiScope, knowledge_id: str
    ) -> list[SourceChunk]:
        return self.chunks

    async def is_active(
        self, scope: WikiScope, knowledge_id: str, op_version: str
    ) -> bool:
        self.active_calls.append((knowledge_id, op_version))
        return self.active


class RecordingModel:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, object]] = []
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
        self.calls.append(("extract_candidates", knowledge_id, config))
        return self.extraction

    async def summarize(
        self, knowledge_id: str, title: str, text: str
    ) -> DocumentSummary:
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
    ],
)
def test_has_meaningful_text_ignores_markdown_images(
    content: str, expected: bool
) -> None:
    assert has_meaningful_text(content) is expected


@pytest.mark.asyncio
async def test_map_document_skips_missing_inactive_and_short_sources() -> None:
    model = RecordingModel()
    missing = StubSource()
    missing.knowledge = None

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

    assert missing_result.skipped_reason == "knowledge_not_found"
    assert inactive_result.skipped_reason == "source_inactive"
    assert short_result.skipped_reason == "insufficient_text"
    assert model.calls == []
    assert inactive_result.updates == short_result.updates == []


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
    await asyncio.wait_for(both_started.wait(), timeout=1)
    release.set()
    await task

    assert started == {"extract", "summarize"}


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
