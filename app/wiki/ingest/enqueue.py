"""Wiki 摄取的轻量校验与原子入队服务。"""

from __future__ import annotations

from app.wiki.ingest.ports import KnowledgeSourcePort
from app.wiki.ingest.schemas import EnqueueResult
from app.wiki.ingest.store import IngestStore
from app.wiki.scope import WikiScope


class WikiEnqueueService:
    def __init__(self, source: KnowledgeSourcePort, store: IngestStore) -> None:
        self.source = source
        self.store = store

    async def enqueue(self, scope: WikiScope, knowledge_id: str) -> EnqueueResult:
        knowledge_id = knowledge_id.strip()
        if not knowledge_id:
            return EnqueueResult(skipped_reason="source_inactive")
        config = await self.source.get_config(scope)
        if not config.wiki_enabled:
            return EnqueueResult(skipped_reason="wiki_disabled")
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
        record = await self.store.enqueue(
            scope, knowledge, {"knowledge_id": knowledge_id}
        )
        return EnqueueResult(
            pending_op_id=record.pending_op_id,
            deduplicated=record.deduplicated,
        )
