"""Wiki 主题候选与既有页面的受限去重。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.schemas import (
    DedupCandidateRequest,
    DedupOutput,
    DedupPageCandidate,
    DedupRequest,
    TopicCandidate,
)
from app.wiki.scope import WikiScope


class DedupStorePort(Protocol):
    async def find_existing_pages(self, scope: WikiScope, slugs: Sequence[str]): ...

    async def find_dedup_candidates(
        self, scope: WikiScope, candidate: TopicCandidate, limit: int = 20
    ) -> list[DedupPageCandidate]: ...


def _name(value: str) -> str:
    return " ".join(value.split()).casefold()


def _unique(values: Sequence[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        value = value.strip()
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _merge_candidate(first: TopicCandidate, later: TopicCandidate) -> TopicCandidate:
    if first.page_type != later.page_type:
        raise WikiValidationError("DEDUP_TYPE_CONFLICT", "同名候选的类型不一致")
    return TopicCandidate(
        name=first.name, slug=first.slug, page_type=first.page_type,
        aliases=_unique([*first.aliases, later.name, *later.aliases]),
        description="\n\n".join(_unique([first.description, later.description])),
        details="\n\n".join(_unique([first.details, later.details])),
    )


def validate_dedup_output(request: DedupRequest, output: DedupOutput) -> dict[str, str | None]:
    requested = {item.candidate.slug: item for item in request.candidates}
    decisions = {decision.candidate_slug: decision for decision in output.decisions}
    if len(decisions) != len(output.decisions) or set(decisions) != set(requested):
        raise WikiValidationError("DEDUP_OUTPUT_INVALID", "dedup 输出必须完整且恰好覆盖请求候选")
    resolved: dict[str, str | None] = {}
    for slug, decision in decisions.items():
        canonical = decision.canonical_slug
        if canonical is not None:
            allowed = {target.slug for target in requested[slug].allowed_targets}
            if canonical not in allowed:
                raise WikiValidationError("DEDUP_OUTPUT_INVALID", "canonical_slug 不在 allowed targets 中")
        resolved[slug] = canonical
    return resolved


async def deduplicate_candidates(
    scope: WikiScope,
    candidates: Sequence[TopicCandidate],
    store: DedupStorePort,
    model,
    *,
    limit: int = 20,
) -> tuple[list[TopicCandidate], dict[str, str]]:
    if isinstance(limit, bool) or not 1 <= limit <= 20:
        raise ValueError("dedup limit 必须在 1 到 20 之间")
    originals = [item.model_copy(deep=True) for item in candidates]
    merged: list[TopicCandidate] = []
    mapping: dict[str, str] = {}
    by_slug: dict[str, int] = {}
    by_name: dict[tuple[str, str], int] = {}
    for item in originals:
        if item.slug in by_slug:
            index = by_slug[item.slug]
            merged[index] = _merge_candidate(merged[index], item)
            mapping[item.slug] = merged[index].slug
            continue
        key = (item.page_type, _name(item.name))
        if key in by_name:
            index = by_name[key]
            merged[index] = _merge_candidate(merged[index], item)
            mapping[item.slug] = merged[index].slug
            by_slug[item.slug] = index
            continue
        by_slug[item.slug] = len(merged)
        by_name[key] = len(merged)
        merged.append(item)
        mapping[item.slug] = item.slug

    existing = await store.find_existing_pages(scope, [item.slug for item in merged])
    existing_targets: dict[str, DedupPageCandidate] = {}
    remaining: list[TopicCandidate] = []
    for item in merged:
        record = existing.get(item.slug)
        if record is None:
            remaining.append(item)
            continue
        page = record.page
        target = DedupPageCandidate(slug=page.slug, title=page.title, page_type=page.page_type, aliases=tuple(page.aliases))
        existing_targets[target.slug] = target
        mapping[item.slug] = target.slug

    generated = {item.slug for item in remaining}
    requests: list[DedupCandidateRequest] = []
    for item in remaining:
        allowed: list[DedupPageCandidate] = []
        seen: set[str] = set()
        for target in await store.find_dedup_candidates(scope, item, limit):
            if target.slug in generated or target.slug in seen or target.page_type != item.page_type:
                continue
            seen.add(target.slug)
            allowed.append(target)
            existing_targets[target.slug] = target
        requests.append(DedupCandidateRequest(candidate=item, allowed_targets=tuple(allowed)))

    if requests:
        request = DedupRequest(candidates=tuple(requests))
        decisions = validate_dedup_output(request, await model.resolve_duplicates(request))
        for slug, canonical in decisions.items():
            if canonical is not None:
                mapping[slug] = canonical

    output: list[TopicCandidate] = []
    output_by_slug: dict[str, int] = {}
    for item in merged:
        final_slug = mapping[item.slug]
        target = existing_targets.get(final_slug)
        candidate = (
            TopicCandidate(name=target.title, slug=target.slug, page_type=target.page_type,
                           aliases=_unique([*target.aliases, item.name, *item.aliases]),
                           description=item.description, details=item.details)
            if target is not None else item.model_copy(deep=True)
        )
        if final_slug in output_by_slug:
            index = output_by_slug[final_slug]
            previous = output[index]
            output[index] = TopicCandidate(
                name=previous.name,
                slug=previous.slug,
                page_type=previous.page_type,
                aliases=_unique([*previous.aliases, *candidate.aliases]),
                description="\n\n".join(_unique([previous.description, candidate.description])),
                details="\n\n".join(_unique([previous.details, candidate.details])),
            )
        else:
            output_by_slug[final_slug] = len(output)
            output.append(candidate)
    for original in originals:
        mapping[original.slug] = mapping[mapping[original.slug]] if mapping[original.slug] in mapping else mapping[original.slug]
    return output, mapping
