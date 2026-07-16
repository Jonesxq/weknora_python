"""单个知识条目的确定性内容重建与 Map。"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from app.wiki.ingest.ports import ChatModelPort, KnowledgeSourcePort
from app.wiki.ingest.schemas import (
    DocumentSummary,
    MapDocumentResult,
    SlugUpdate,
    SourceChunk,
    SourceKnowledge,
    TopicCandidate,
    WikiWorkerOptions,
)
from app.wiki.scope import WikiScope


_MARKDOWN_IMAGE_PATTERN = re.compile(
    r"!\[[^\]]*\](?:\([^)]*\)|\[[^\]]*\])"
)
_MARKDOWN_REFERENCE_DEFINITION_PATTERN = re.compile(
    r"^\s*\[[^\]\r\n]+\]:[^\r\n]*$", re.MULTILINE
)
_KNOWLEDGE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def rebuild_source_content(
    chunks: Sequence[SourceChunk], *, max_chars: int = 32768
) -> str:
    """按来源位置稳定拼接 chunk，并以 Unicode 字符数限制模型输入。"""

    if max_chars < 0:
        raise ValueError("max_chars 不能小于 0")
    ordered = sorted(
        chunks, key=lambda item: (item.chunk_index, item.start_at, item.id)
    )
    parts: list[str] = []
    for chunk in ordered:
        for value in (chunk.text, chunk.ocr_text, chunk.image_caption):
            cleaned = value.strip()
            if cleaned:
                parts.append(cleaned)
    return "\n".join(parts)[:max_chars]


def has_meaningful_text(content: str) -> bool:
    """图片标记和空白之外至少有十个字符时才值得调用模型。"""

    without_images = _MARKDOWN_IMAGE_PATTERN.sub("", content)
    without_images = _MARKDOWN_REFERENCE_DEFINITION_PATTERN.sub("", without_images)
    return len("".join(without_images.split())) >= 10


def _has_canonical_summary_identity(knowledge_id: str) -> bool:
    return bool(
        _KNOWLEDGE_ID_PATTERN.fullmatch(knowledge_id)
        and len(f"summary/{knowledge_id}") <= 255
    )


def _fallback_pending_op_id(
    scope: WikiScope, knowledge_id: str, op_version: str = ""
) -> UUID:
    identity = (
        f"wiki-map:{scope.tenant_id}:{scope.knowledge_base_id}:"
        f"{knowledge_id}:{op_version}"
    )
    return uuid5(NAMESPACE_URL, identity)


def _skipped_result(
    scope: WikiScope,
    knowledge_id: str,
    reason: str,
    *,
    pending_op_id: UUID | None,
    op_version: str = "",
) -> MapDocumentResult:
    return MapDocumentResult(
        pending_op_id=pending_op_id
        or _fallback_pending_op_id(scope, knowledge_id, op_version),
        knowledge_id=knowledge_id,
        skipped_reason=reason,
    )


def _summary_update(
    knowledge: SourceKnowledge,
    summary: DocumentSummary,
    pending_op_id: UUID,
) -> SlugUpdate:
    return SlugUpdate(
        pending_op_id=pending_op_id,
        knowledge_id=knowledge.id,
        slug=f"summary/{knowledge.id}",
        title=f"{knowledge.title} 摘要",
        page_type="summary",
        content=summary.markdown,
        summary=summary.headline,
        source_refs=[knowledge.id],
        chunk_refs=[],
    )


def _topic_update(
    knowledge: SourceKnowledge,
    candidate: TopicCandidate,
    document_summary: DocumentSummary,
    pending_op_id: UUID,
) -> SlugUpdate:
    summary_parts = [candidate.description.strip(), document_summary.markdown]
    return SlugUpdate(
        pending_op_id=pending_op_id,
        knowledge_id=knowledge.id,
        slug=candidate.slug,
        title=candidate.name,
        page_type=candidate.page_type,
        content=candidate.details,
        summary="\n\n".join(part for part in summary_parts if part),
        aliases=list(candidate.aliases),
        source_refs=[knowledge.id],
        chunk_refs=[],
    )


async def map_document(
    scope: WikiScope,
    knowledge_id: str,
    source: KnowledgeSourcePort,
    model: ChatModelPort,
    *,
    pending_op_id: UUID | None = None,
    options: WikiWorkerOptions | None = None,
    max_chars: int = 32768,
) -> MapDocumentResult:
    """将单个有效来源映射为固定顺序的 summary 和 topic 更新。

    Worker 应传入真实 ``pending_op_id``。未传时生成稳定 ID，便于在不依赖
    pending-op 仓储的开发与单元测试中直接运行 Map。
    """

    knowledge = await source.get_knowledge(scope, knowledge_id)
    if knowledge is None:
        return _skipped_result(
            scope,
            knowledge_id,
            "knowledge_not_found",
            pending_op_id=pending_op_id,
        )
    if knowledge.id != knowledge_id:
        return _skipped_result(
            scope,
            knowledge_id,
            "source_identity_mismatch",
            pending_op_id=pending_op_id,
            op_version=knowledge.op_version,
        )
    if not _has_canonical_summary_identity(knowledge_id):
        return _skipped_result(
            scope,
            knowledge_id,
            "invalid_knowledge_id",
            pending_op_id=pending_op_id,
            op_version=knowledge.op_version,
        )
    if (
        knowledge.tenant_id != scope.tenant_id
        or knowledge.knowledge_base_id != scope.knowledge_base_id
    ):
        return _skipped_result(
            scope,
            knowledge_id,
            "source_scope_mismatch",
            pending_op_id=pending_op_id,
            op_version=knowledge.op_version,
        )

    config = (await source.get_config(scope)).model_copy(deep=True)
    if not config.wiki_enabled:
        return _skipped_result(
            scope,
            knowledge_id,
            "wiki_disabled",
            pending_op_id=pending_op_id,
            op_version=knowledge.op_version,
        )
    if not knowledge.is_active or not await source.is_active(
        scope, knowledge_id, knowledge.op_version
    ):
        return _skipped_result(
            scope,
            knowledge_id,
            "source_inactive",
            pending_op_id=pending_op_id,
            op_version=knowledge.op_version,
        )

    chunks = await source.list_chunks(scope, knowledge_id)
    content = rebuild_source_content(chunks, max_chars=max_chars)
    if not has_meaningful_text(content):
        return _skipped_result(
            scope,
            knowledge_id,
            "insufficient_text",
            pending_op_id=pending_op_id,
            op_version=knowledge.op_version,
        )

    if options is not None:
        config.extraction_granularity = options.extraction_granularity
        if options.max_pages_per_ingest > 0:
            config.max_pages_per_ingest = options.max_pages_per_ingest

    extraction_task = asyncio.create_task(
        model.extract_candidates(knowledge_id, content, config),
        name=f"wiki-map-extract:{knowledge_id}",
    )
    summary_task = asyncio.create_task(
        model.summarize(knowledge_id, knowledge.title, content),
        name=f"wiki-map-summarize:{knowledge_id}",
    )
    model_tasks = (extraction_task, summary_task)
    try:
        extraction, document_summary = await asyncio.gather(*model_tasks)
    except BaseException:
        for task in model_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*model_tasks, return_exceptions=True)
        raise

    effective_pending_op_id = pending_op_id or _fallback_pending_op_id(
        scope, knowledge_id, knowledge.op_version
    )
    topics = [*extraction.entities, *extraction.concepts]
    if config.max_pages_per_ingest > 0:
        topics = topics[: config.max_pages_per_ingest]

    updates = [
        _summary_update(knowledge, document_summary, effective_pending_op_id),
        *(
            _topic_update(
                knowledge,
                candidate,
                document_summary,
                effective_pending_op_id,
            )
            for candidate in topics
        ),
    ]
    return MapDocumentResult(
        pending_op_id=effective_pending_op_id,
        knowledge_id=knowledge.id,
        updates=updates,
    )
