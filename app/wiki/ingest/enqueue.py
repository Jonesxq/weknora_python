"""Wiki 摄取的轻量校验与原子入队服务。"""

from __future__ import annotations

from app.wiki.ingest.ports import KnowledgeSourcePort, TombstonePort
from app.wiki.ingest.schemas import EnqueueResult
from app.wiki.ingest.store import IngestStore
from app.wiki.scope import WikiScope


class WikiEnqueueService:
    def __init__(
        self,
        source: KnowledgeSourcePort,
        store: IngestStore,
        tombstones: TombstonePort | None = None,
    ) -> None:
        self.source = source
        self.store = store
        self.tombstones = tombstones

    async def enqueue(self, scope: WikiScope, knowledge_id: str) -> EnqueueResult:
        """兼容阶段二调用方的摄取入队别名。"""

        return await self.enqueue_ingest(scope, knowledge_id)

    async def enqueue_ingest(
        self, scope: WikiScope, knowledge_id: str
    ) -> EnqueueResult:
        knowledge_id = knowledge_id.strip()
        if not knowledge_id:
            return EnqueueResult(skipped_reason="source_inactive")
        config = await self.source.get_config(scope)
        if not config.wiki_enabled:
            return EnqueueResult(skipped_reason="wiki_disabled")
        if self.tombstones is not None and await self.tombstones.is_deleted(
            scope, knowledge_id
        ):
            return EnqueueResult(skipped_reason="tombstoned")
        knowledge = await self.source.get_knowledge(scope, knowledge_id)
        if (
            knowledge is None
            or knowledge.id != knowledge_id
            or knowledge.tenant_id != scope.tenant_id
            or knowledge.knowledge_base_id != scope.knowledge_base_id
            or not knowledge.is_active
            or not await self.source.is_active(
                scope, knowledge_id, knowledge.op_version
            )
        ):
            return EnqueueResult(skipped_reason="source_inactive")
        chunks = await self.source.list_chunks(scope, knowledge_id)
        if not any(chunk.text.strip() or chunk.ocr_text.strip() for chunk in chunks):
            return EnqueueResult(skipped_reason="no_text_chunks")
        if not (config.synthesis_model_id or config.summary_model_id):
            return EnqueueResult(skipped_reason="model_not_configured")
        record = await self.store.enqueue_ingest(
            scope, knowledge, {"knowledge_id": knowledge_id}
        )
        return EnqueueResult(
            pending_op_id=record.pending_op_id,
            deduplicated=record.deduplicated,
        )

    async def enqueue_retract(
        self, scope: WikiScope, knowledge_id: str, op_version: str
    ) -> EnqueueResult:
        if self.tombstones is None:
            raise RuntimeError("retract 必须配置 tombstone store")
        if not isinstance(knowledge_id, str) or not knowledge_id.strip():
            raise ValueError("knowledge_id 不能为空")
        if not isinstance(op_version, str) or not op_version.strip():
            raise ValueError("op_version 不能为空")
        knowledge_id = knowledge_id.strip()
        op_version = op_version.strip()
        await self.tombstones.mark_deleted(scope, knowledge_id)
        record = await self.store.enqueue_retract(
            scope,
            knowledge_id,
            op_version,
            {"knowledge_id": knowledge_id},
        )
        return EnqueueResult(
            pending_op_id=record.pending_op_id,
            deduplicated=record.deduplicated,
        )
