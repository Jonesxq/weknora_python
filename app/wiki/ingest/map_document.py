"""单个知识条目的确定性内容重建与 Map。"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Sequence
from typing import cast
from uuid import NAMESPACE_URL, UUID, uuid5

from app.wiki.ingest.citations import classify_citations
from app.wiki.ingest.dedup import deduplicate_candidates
from app.wiki.ingest.ports import (
    ChatModelPort,
    CitationModelPort,
    DedupModelPort,
    KnowledgeSourcePort,
    TombstonePort,
)
from app.wiki.ingest.retract import plan_ingest_deltas
from app.wiki.ingest.schemas import (
    DocumentSummary,
    MapDocumentResult,
    SourceChunk,
    SourceKnowledge,
    StoredContributionRecord,
    TopicCandidate,
    WikiWorkerOptions,
)
from app.wiki.ingest.store import IngestStore
from app.wiki.scope import WikiScope


_MARKDOWN_IMAGE_PATTERN = re.compile(r"!\[[^\]]*\](?:\([^)]*\)|\[[^\]]*\])")
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


def _ordered_chunks(chunks: Sequence[SourceChunk]) -> list[SourceChunk]:
    return sorted(chunks, key=lambda item: (item.chunk_index, item.start_at, item.id))


def _has_chunk_content(chunk: SourceChunk) -> bool:
    return any(
        value.strip() for value in (chunk.text, chunk.ocr_text, chunk.image_caption)
    )


def _summary_chunk_refs(chunks: Sequence[SourceChunk]) -> tuple[str, ...]:
    refs: list[str] = []
    seen: set[str] = set()
    for chunk in _ordered_chunks(chunks):
        if _has_chunk_content(chunk) and chunk.id not in seen:
            seen.add(chunk.id)
            refs.append(chunk.id)
    return tuple(refs)


def _canonical_chunk_refs(
    refs_by_generated_slug: dict[str, list[str]],
    canonical_by_generated_slug: dict[str, str],
    chunks: Sequence[SourceChunk],
) -> dict[str, tuple[str, ...]]:
    ranks: dict[str, tuple[int, int, str]] = {}
    for chunk in chunks:
        rank = (chunk.chunk_index, chunk.start_at, chunk.id)
        ranks[chunk.id] = min(ranks.get(chunk.id, rank), rank)
    merged: dict[str, set[str]] = {}
    for generated_slug, chunk_ids in refs_by_generated_slug.items():
        canonical_slug = canonical_by_generated_slug.get(generated_slug, generated_slug)
        merged.setdefault(canonical_slug, set()).update(chunk_ids)
    return {
        slug: tuple(sorted(chunk_ids, key=lambda chunk_id: ranks[chunk_id]))
        for slug, chunk_ids in merged.items()
        if chunk_ids
    }


def _summary_record(
    scope: WikiScope,
    knowledge: SourceKnowledge,
    summary: DocumentSummary,
    op_version: str,
    chunk_refs: tuple[str, ...],
) -> StoredContributionRecord:
    return StoredContributionRecord(
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        slug=f"summary/{knowledge.id}",
        knowledge_id=knowledge.id,
        op_version=op_version,
        page_type="summary",
        state="active",
        title=f"{knowledge.title} 摘要",
        content=summary.markdown,
        summary=summary.headline,
        chunk_refs=chunk_refs,
    )


def _topic_record(
    scope: WikiScope,
    knowledge: SourceKnowledge,
    candidate: TopicCandidate,
    document_summary: DocumentSummary,
    op_version: str,
    chunk_refs: tuple[str, ...],
) -> StoredContributionRecord:
    summary_parts = [candidate.description.strip(), document_summary.markdown]
    return StoredContributionRecord(
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        slug=candidate.slug,
        knowledge_id=knowledge.id,
        op_version=op_version,
        page_type=candidate.page_type,
        state="active",
        title=candidate.name,
        content=candidate.details,
        summary="\n\n".join(part for part in summary_parts if part),
        aliases=tuple(candidate.aliases),
        chunk_refs=chunk_refs,
    )


async def map_document(
    scope: WikiScope,
    knowledge_id: str,
    source: KnowledgeSourcePort,
    model: ChatModelPort,
    store: IngestStore | None = None,
    tombstones: TombstonePort | None = None,
    *,
    pending_op_id: UUID | None = None,
    op_version: str | None = None,
    options: WikiWorkerOptions | None = None,
    max_chars: int = 32768,
) -> MapDocumentResult:
    """将单个有效来源映射为贡献差量。

    旧 Worker 未传入阶段三依赖时走内部兼容路径；任务 11 切换后移除。
    """

    legacy_mode = store is None and tombstones is None and op_version is None
    if not legacy_mode:
        if store is None or tombstones is None:
            raise TypeError("阶段三 Map 必须同时传入 store 和 tombstones")
        if not isinstance(pending_op_id, UUID):
            raise TypeError("阶段三 Map 的 pending_op_id 必须是 UUID")
        if not isinstance(op_version, str) or not op_version.strip():
            raise ValueError("阶段三 Map 的 op_version 不能为空")
        op_version = op_version.strip()

    knowledge = await source.get_knowledge(scope, knowledge_id)
    if knowledge is None:
        return _skipped_result(
            scope,
            knowledge_id,
            "knowledge_not_found",
            pending_op_id=pending_op_id,
            op_version=op_version or "",
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
    effective_op_version = op_version or knowledge.op_version
    if not knowledge.is_active or not await source.is_active(
        scope, knowledge_id, effective_op_version
    ):
        return _skipped_result(
            scope,
            knowledge_id,
            "source_inactive",
            pending_op_id=pending_op_id,
            op_version=effective_op_version,
        )

    chunks = await source.list_chunks(scope, knowledge_id)
    content = rebuild_source_content(chunks, max_chars=max_chars)
    if not has_meaningful_text(content):
        return _skipped_result(
            scope,
            knowledge_id,
            "insufficient_text",
            pending_op_id=pending_op_id,
            op_version=effective_op_version,
        )

    effective_pending_op_id = pending_op_id or _fallback_pending_op_id(
        scope, knowledge_id, effective_op_version
    )
    previous: list[StoredContributionRecord] = []
    if not legacy_mode:
        assert tombstones is not None and store is not None
        if await tombstones.is_deleted(scope, knowledge_id):
            return MapDocumentResult(
                pending_op_id=effective_pending_op_id,
                knowledge_id=knowledge.id,
                superseded=True,
            )
        previous = await store.list_source_contributions(
            scope, knowledge_id, state="active"
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

    topics = [*extraction.entities, *extraction.concepts]
    topic_limit = config.max_pages_per_ingest
    if topic_limit > 0:
        topics = topics[:topic_limit]
    citation_refs: dict[str, list[str]] = {}
    if legacy_mode:
        deduped_topics = topics
        canonical_by_generated = {
            candidate.slug: candidate.slug for candidate in topics
        }
    else:
        assert store is not None
        effective_options = options or WikiWorkerOptions()
        citation_refs, supplemental = await classify_citations(
            knowledge_id=knowledge_id,
            chunks=_ordered_chunks(chunks),
            candidates=topics,
            model=cast(CitationModelPort, model),
            max_chars=effective_options.citation_batch_chars,
            max_parallel=effective_options.citation_parallel,
        )
        if topic_limit > 0:
            supplemental = supplemental[: max(topic_limit - len(topics), 0)]
        topics.extend(supplemental)
        deduped_topics, canonical_by_generated = await deduplicate_candidates(
            scope,
            topics,
            store,
            cast(DedupModelPort, model),
            limit=effective_options.dedup_candidate_limit,
        )

    refs_by_canonical = _canonical_chunk_refs(
        citation_refs, canonical_by_generated, chunks
    )
    current = [
        _summary_record(
            scope,
            knowledge,
            document_summary,
            effective_op_version,
            () if legacy_mode else _summary_chunk_refs(chunks),
        ),
        *(
            _topic_record(
                scope,
                knowledge,
                candidate,
                document_summary,
                effective_op_version,
                refs_by_canonical.get(candidate.slug, ()),
            )
            for candidate in deduped_topics
        ),
    ]
    return MapDocumentResult(
        pending_op_id=effective_pending_op_id,
        knowledge_id=knowledge.id,
        contribution_deltas=tuple(
            plan_ingest_deltas(effective_pending_op_id, previous, current)
        ),
    )
