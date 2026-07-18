"""Chunk citation batching and model result normalization."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import MappingProxyType
from typing import Awaitable, Callable, Mapping, Sequence

from pydantic import ValidationError

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.ports import CitationModelPort, PermanentModelError, TransientModelError
from app.wiki.ingest.schemas import (
    CitationBatchChunk,
    CitationBatchOutput,
    CitationBatchRequest,
    SourceChunk,
    TopicCandidate,
)


@dataclass(frozen=True, slots=True)
class PreparedCitationBatch:
    batch_index: int
    chunks: tuple[CitationBatchChunk, ...]
    alias_to_chunk_id: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "chunks", tuple(self.chunks))
        object.__setattr__(self, "alias_to_chunk_id", MappingProxyType(dict(self.alias_to_chunk_id)))


def _source_text(chunk: SourceChunk) -> str:
    return "\n".join(value.strip() for value in (chunk.text, chunk.ocr_text, chunk.image_caption) if value.strip())


def prepare_citation_batches(
    chunks: Sequence[SourceChunk], *, max_chars: int = 12000
) -> list[PreparedCitationBatch]:
    if max_chars <= 0:
        raise ValueError("max_chars 必须大于 0")

    batches: list[PreparedCitationBatch] = []
    pending: list[CitationBatchChunk] = []
    aliases: dict[str, str] = {}
    used_chars = 0

    def flush() -> None:
        nonlocal pending, aliases, used_chars
        if pending:
            batches.append(PreparedCitationBatch(len(batches), tuple(pending), aliases))
        pending, aliases, used_chars = [], {}, 0

    for chunk in sorted(chunks, key=lambda item: (item.chunk_index, item.start_at, item.id)):
        text = _source_text(chunk)
        if not text:
            continue
        for offset in range(0, len(text), max_chars):
            piece = text[offset : offset + max_chars]
            if pending and (used_chars + len(piece) > max_chars or len(pending) == 1000):
                flush()
            alias = f"c{len(pending):03d}"
            pending.append(CitationBatchChunk(alias=alias, text=piece))
            aliases[alias] = chunk.id
            used_chars += len(piece)
    flush()
    return batches


async def classify_citations(
    *,
    knowledge_id: str,
    chunks: Sequence[SourceChunk],
    candidates: Sequence[TopicCandidate],
    model: CitationModelPort,
    max_chars: int = 12000,
    max_parallel: int = 4,
    retry_wait: Callable[[int], Awaitable[None]] = asyncio.sleep,
) -> tuple[dict[str, list[str]], list[TopicCandidate]]:
    if max_parallel <= 0:
        raise ValueError("max_parallel 必须大于 0")
    batches = prepare_citation_batches(chunks, max_chars=max_chars)
    if not batches:
        return {}, []

    initial = tuple(candidates)
    initial_slugs = {item.slug for item in initial}
    semaphore = asyncio.Semaphore(max_parallel)

    async def run_batch(batch: PreparedCitationBatch) -> tuple[PreparedCitationBatch, CitationBatchOutput] | None:
        request = CitationBatchRequest(
            knowledge_id=knowledge_id,
            batch_index=batch.batch_index,
            candidates=initial,
            chunks=batch.chunks,
        )
        for attempt in range(3):
            try:
                async with semaphore:
                    output = await model.classify_chunks(request)
                _validate_batch_output(output, batch, initial_slugs)
                return batch, output
            except TransientModelError:
                if attempt == 2:
                    return None
                await retry_wait(2 if attempt == 0 else 4)
            except (PermanentModelError, WikiValidationError, ValidationError, ValueError, TypeError):
                return None
        return None

    tasks = [asyncio.create_task(run_batch(batch)) for batch in batches]
    try:
        results = await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    rank_by_id: dict[str, tuple[int, int, str]] = {}
    for source in chunks:
        rank_by_id.setdefault(source.id, (source.chunk_index, source.start_at, source.id))
        rank_by_id[source.id] = min(rank_by_id[source.id], (source.chunk_index, source.start_at, source.id))

    refs: dict[str, set[str]] = {}
    supplements: list[TopicCandidate] = []
    known_supplements: dict[str, TopicCandidate] = {}
    for result in results:
        if result is None:
            continue
        batch, output = result
        candidates_for_batch = list(output.supplemental_candidates)
        conflict = any(
            item.slug in known_supplements and known_supplements[item.slug].model_dump() != item.model_dump()
            for item in candidates_for_batch
        )
        if conflict:
            continue
        for item in candidates_for_batch:
            if item.slug not in known_supplements:
                mutable = TopicCandidate.model_validate(item.model_dump())
                known_supplements[item.slug] = mutable
                supplements.append(mutable)
        for slug, aliases in output.refs_by_slug.items():
            resolved = refs.setdefault(slug, set())
            resolved.update(batch.alias_to_chunk_id[alias] for alias in aliases)

    ordered_refs = {
        slug: sorted(chunk_ids, key=lambda chunk_id: rank_by_id[chunk_id])
        for slug, chunk_ids in refs.items()
        if chunk_ids
    }
    return ordered_refs, supplements


def _validate_batch_output(
    output: CitationBatchOutput,
    batch: PreparedCitationBatch,
    initial_slugs: set[str],
) -> None:
    supplemental_by_slug: dict[str, object] = {}
    for item in output.supplemental_candidates:
        previous = supplemental_by_slug.get(item.slug)
        snapshot = item.model_dump()
        if previous is not None and previous != snapshot:
            raise ValueError("citation supplemental candidate slug 冲突")
        supplemental_by_slug[item.slug] = snapshot
    supplemental_slugs = set(supplemental_by_slug)
    allowed_slugs = initial_slugs | supplemental_slugs
    for slug, aliases in output.refs_by_slug.items():
        if slug not in allowed_slugs or any(alias not in batch.alias_to_chunk_id for alias in aliases):
            raise ValueError("citation 模型输出包含未授权 slug 或 alias")
