from __future__ import annotations

import asyncio
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
        self.calls: list[str] = []

    async def get_config(self, scope: WikiScope) -> WikiIngestConfig:
        self.calls.append("config")
        return self.config.model_copy(deep=True)

    async def get_knowledge(
        self, scope: WikiScope, knowledge_id: str
    ) -> SourceKnowledge | None:
        self.calls.append("knowledge")
        return self.knowledge.model_copy(deep=True)

    async def list_chunks(
        self, scope: WikiScope, knowledge_id: str
    ) -> list[SourceChunk]:
        self.calls.append("chunks")
        return [chunk.model_copy(deep=True) for chunk in self.chunks]

    async def is_active(
        self, scope: WikiScope, knowledge_id: str, op_version: str
    ) -> bool:
        self.calls.append("active")
        return self.active and op_version == self.knowledge.op_version


class MemoryStore:
    def __init__(self) -> None:
        self.pending_id = uuid4()
        self.outbox_id = uuid4()
        self.calls: list[tuple[WikiScope, SourceKnowledge, dict[str, object]]] = []
        self.retract_calls: list[tuple[WikiScope, str, str, dict[str, object]]] = []
        self.events: list[str] = []
        self.error: BaseException | None = None

    async def enqueue_ingest(
        self,
        scope: WikiScope,
        knowledge: SourceKnowledge,
        payload: dict[str, object],
        *,
        delay_seconds: int = 30,
    ) -> EnqueueRecord:
        self.events.append("store_ingest")
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

    async def enqueue_retract(
        self,
        scope: WikiScope,
        knowledge_id: str,
        op_version: str,
        payload: dict[str, object],
        *,
        delay_seconds: int = 30,
    ) -> EnqueueRecord:
        self.events.append("store_retract")
        if self.error is not None:
            raise self.error
        self.retract_calls.append((scope, knowledge_id, op_version, dict(payload)))
        return EnqueueRecord(
            id=self.pending_id,
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            knowledge_id=knowledge_id,
            op_version=op_version,
            payload=dict(payload),
            outbox_event_id=self.outbox_id,
            deduplicated=len(self.retract_calls) > 1,
        )


class MemoryTombstones:
    def __init__(self, *, deleted: bool = False) -> None:
        self.deleted = deleted
        self.events: list[str] = []
        self.error: BaseException | None = None

    async def mark_deleted(self, scope: WikiScope, knowledge_id: str) -> None:
        self.events.append("tombstone_mark")
        if self.error is not None:
            raise self.error
        self.deleted = True

    async def is_deleted(self, scope: WikiScope, knowledge_id: str) -> bool:
        self.events.append("tombstone_check")
        if self.error is not None:
            raise self.error
        return self.deleted


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
async def test_enqueue_alias_and_enqueue_ingest_share_stage_two_validation() -> None:
    source = MemorySource()
    store = MemoryStore()
    service = WikiEnqueueService(source, store)  # type: ignore[arg-type]

    alias = await service.enqueue(SCOPE, " knowledge-1 ")
    direct = await service.enqueue_ingest(SCOPE, "knowledge-1")

    assert alias.pending_op_id == direct.pending_op_id == store.pending_id
    assert [call[1].id for call in store.calls] == ["knowledge-1", "knowledge-1"]


@pytest.mark.asyncio
async def test_ingest_tombstone_skips_before_reading_source_body_or_store() -> None:
    source = MemorySource()
    store = MemoryStore()
    tombstones = MemoryTombstones(deleted=True)

    result = await WikiEnqueueService(source, store, tombstones).enqueue_ingest(
        SCOPE, "knowledge-1"
    )  # type: ignore[arg-type]

    assert result.skipped_reason == "tombstoned"
    assert source.calls == ["config"]
    assert tombstones.events == ["tombstone_check"]
    assert store.calls == []


@pytest.mark.asyncio
async def test_retract_marks_tombstone_before_store_without_reading_source() -> None:
    source = MemorySource()
    store = MemoryStore()
    tombstones = MemoryTombstones()
    service = WikiEnqueueService(source, store, tombstones)  # type: ignore[arg-type]

    result = await service.enqueue_retract(SCOPE, " knowledge-1 ", " version-2 ")

    assert result.pending_op_id == store.pending_id
    assert tombstones.deleted is True
    assert tombstones.events == ["tombstone_mark"]
    assert store.retract_calls == [
        (SCOPE, "knowledge-1", "version-2", {"knowledge_id": "knowledge-1"})
    ]
    assert source.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [RuntimeError("redis down"), asyncio.CancelledError()]
)
async def test_retract_tombstone_failure_or_cancellation_never_calls_store(
    error: BaseException,
) -> None:
    store = MemoryStore()
    tombstones = MemoryTombstones()
    tombstones.error = error

    with pytest.raises(type(error)):
        await WikiEnqueueService(MemorySource(), store, tombstones).enqueue_retract(
            SCOPE, "knowledge-1", "version-2"
        )  # type: ignore[arg-type]

    assert store.retract_calls == []


@pytest.mark.asyncio
async def test_retract_store_failure_keeps_tombstone_for_retry() -> None:
    store = MemoryStore()
    store.error = RuntimeError("db down")
    tombstones = MemoryTombstones()

    with pytest.raises(RuntimeError, match="db down"):
        await WikiEnqueueService(MemorySource(), store, tombstones).enqueue_retract(
            SCOPE, "knowledge-1", "version-2"
        )  # type: ignore[arg-type]

    assert tombstones.deleted is True
    assert tombstones.events == ["tombstone_mark"]


@pytest.mark.asyncio
async def test_retract_without_tombstone_configuration_fails_before_store() -> None:
    store = MemoryStore()
    service = WikiEnqueueService(MemorySource(), store)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="tombstone"):
        await service.enqueue_retract(SCOPE, "knowledge-1", "version-2")

    assert store.retract_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledge_id,op_version", [("", "v1"), ("id", " ")])
async def test_retract_rejects_blank_identity_before_tombstone(
    knowledge_id: str, op_version: str
) -> None:
    tombstones = MemoryTombstones()
    with pytest.raises(ValueError, match="不能为空"):
        await WikiEnqueueService(
            MemorySource(), MemoryStore(), tombstones
        ).enqueue_retract(SCOPE, knowledge_id, op_version)  # type: ignore[arg-type]
    assert tombstones.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda source: setattr(source.config, "wiki_enabled", False), "wiki_disabled"),
        (
            lambda source: setattr(source.knowledge, "status", "deleted"),
            "source_inactive",
        ),
        (lambda source: setattr(source, "active", False), "source_inactive"),
        (
            lambda source: setattr(source, "chunks", [SourceChunk(id="empty")]),
            "no_text_chunks",
        ),
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
