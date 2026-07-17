from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from app.wiki.ingest.enqueue import WikiEnqueueService
from app.wiki.ingest.schemas import SourceChunk, SourceKnowledge, WikiIngestConfig
from app.wiki.ingest.store import EnqueueRecord
from app.wiki.scope import WikiScope


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
SCOPE = WikiScope(tenant_id=7, knowledge_base_id=KB_ID, actor_id="api")


class MemorySource:
    def __init__(self) -> None:
        self.config = WikiIngestConfig(
            wiki_enabled=True,
            synthesis_model_id="fake-synthesis",
            summary_model_id="fake-summary",
        )
        self.knowledge = SourceKnowledge(
            id="knowledge-1",
            tenant_id=SCOPE.tenant_id,
            knowledge_base_id=SCOPE.knowledge_base_id,
            title="文档一",
            op_version="version-1",
        )
        self.chunks = [SourceChunk(id="chunk-1", text="有意义的正文")]
        self.active = True

    async def get_config(self, scope: WikiScope) -> WikiIngestConfig:
        return self.config.model_copy(deep=True)

    async def get_knowledge(
        self, scope: WikiScope, knowledge_id: str
    ) -> SourceKnowledge | None:
        return self.knowledge.model_copy(deep=True)

    async def list_chunks(
        self, scope: WikiScope, knowledge_id: str
    ) -> list[SourceChunk]:
        return [chunk.model_copy(deep=True) for chunk in self.chunks]

    async def is_active(
        self, scope: WikiScope, knowledge_id: str, op_version: str
    ) -> bool:
        return self.active and op_version == self.knowledge.op_version


class MemoryStore:
    def __init__(self) -> None:
        self.pending_id = uuid4()
        self.outbox_id = uuid4()
        self.calls: list[tuple[WikiScope, SourceKnowledge, dict[str, object]]] = []

    async def enqueue(
        self,
        scope: WikiScope,
        knowledge: SourceKnowledge,
        payload: dict[str, object],
        *,
        delay_seconds: int = 30,
    ) -> EnqueueRecord:
        self.calls.append((scope, knowledge.model_copy(deep=True), dict(payload)))
        return EnqueueRecord(
            id=self.pending_id,
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            knowledge_id=knowledge.id,
            op_version=knowledge.op_version,
            payload=dict(payload),
            outbox_event_id=self.outbox_id,
            deduplicated=len(self.calls) > 1,
        )


@pytest.mark.asyncio
async def test_enqueue_success_uses_minimal_payload_and_reports_deduplication() -> None:
    source = MemorySource()
    store = MemoryStore()
    service = WikiEnqueueService(source, store)  # type: ignore[arg-type]

    first = await service.enqueue(SCOPE, "knowledge-1")
    second = await service.enqueue(SCOPE, "knowledge-1")

    assert first.pending_op_id == second.pending_op_id == store.pending_id
    assert first.deduplicated is False
    assert second.deduplicated is True
    assert [call[2] for call in store.calls] == [
        {"knowledge_id": "knowledge-1"},
        {"knowledge_id": "knowledge-1"},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda source: setattr(source.config, "wiki_enabled", False), "wiki_disabled"),
        (lambda source: setattr(source.knowledge, "status", "deleted"), "source_inactive"),
        (lambda source: setattr(source, "active", False), "source_inactive"),
        (lambda source: setattr(source, "chunks", [SourceChunk(id="empty")]), "no_text_chunks"),
        (
            lambda source: (
                setattr(source.config, "synthesis_model_id", None),
                setattr(source.config, "summary_model_id", None),
            ),
            "model_not_configured",
        ),
    ],
)
async def test_enqueue_skip_reasons_never_write(mutate, reason: str) -> None:
    source = MemorySource()
    mutate(source)
    store = MemoryStore()

    result = await WikiEnqueueService(source, store).enqueue(SCOPE, "knowledge-1")  # type: ignore[arg-type]

    assert result.skipped_reason == reason
    assert result.pending_op_id is None
    assert store.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["id", "tenant_id", "knowledge_base_id"])
async def test_enqueue_rejects_source_identity_or_scope_mismatch_without_write(
    field: str,
) -> None:
    source = MemorySource()
    if field == "id":
        source.knowledge.id = "knowledge-forged"
    elif field == "tenant_id":
        source.knowledge.tenant_id = SCOPE.tenant_id + 1
    else:
        source.knowledge.knowledge_base_id = uuid4()
    store = MemoryStore()

    result = await WikiEnqueueService(source, store).enqueue(SCOPE, "knowledge-1")  # type: ignore[arg-type]

    assert result.skipped_reason == "source_inactive"
    assert store.calls == []


@pytest.mark.asyncio
async def test_ocr_text_counts_but_image_caption_does_not() -> None:
    source = MemorySource()
    source.chunks = [SourceChunk(id="ocr", ocr_text="OCR 正文")]
    store = MemoryStore()
    result = await WikiEnqueueService(source, store).enqueue(SCOPE, "knowledge-1")  # type: ignore[arg-type]
    assert result.pending_op_id == store.pending_id

    source.chunks = [SourceChunk(id="image", image_caption="只有图片说明")]
    skipped = await WikiEnqueueService(source, MemoryStore()).enqueue(  # type: ignore[arg-type]
        SCOPE, "knowledge-1"
    )
    assert skipped.skipped_reason == "no_text_chunks"
