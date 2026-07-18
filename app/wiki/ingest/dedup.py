"""Wiki 主题候选与既有页面的受限去重。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol
from uuid import UUID

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.schemas import DedupCandidateRequest, DedupOutput, DedupPageCandidate, DedupRequest, TopicCandidate
from app.wiki.ingest.store import ExistingPageRecord
from app.wiki.scope import WikiScope


class DedupStorePort(Protocol):
    async def find_existing_pages(self, scope: WikiScope, slugs: Sequence[str]) -> dict[str, ExistingPageRecord]: ...
    async def find_dedup_candidates(self, scope: WikiScope, candidate: TopicCandidate, limit: int = 20) -> list[DedupPageCandidate]: ...


def _name(value: str) -> str:
    return " ".join(value.split()).casefold()


def _unique(values: Sequence[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = value.strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            result.append(cleaned)
    return result


def _combine(items: Sequence[TopicCandidate]) -> TopicCandidate:
    first = items[0]
    if any(item.page_type != first.page_type for item in items):
        raise WikiValidationError("DEDUP_TYPE_CONFLICT", "同一去重簇的类型不一致")
    return TopicCandidate(
        name=first.name, slug=first.slug, page_type=first.page_type,
        aliases=_unique([*first.aliases, *[value for item in items[1:] for value in (item.name, *item.aliases)]]),
        description="\n\n".join(_unique([item.description for item in items])),
        details="\n\n".join(_unique([item.details for item in items])),
    )


def validate_dedup_output(request: DedupRequest, output: DedupOutput) -> dict[str, str | None]:
    requested = {item.candidate.slug: item for item in request.candidates}
    generated = set(requested)
    decisions = {decision.candidate_slug: decision for decision in output.decisions}
    if len(decisions) != len(output.decisions) or set(decisions) != set(requested):
        raise WikiValidationError("DEDUP_OUTPUT_INVALID", "dedup 输出必须完整且恰好覆盖请求候选")
    result: dict[str, str | None] = {}
    for slug, decision in decisions.items():
        canonical = decision.canonical_slug
        if canonical is not None:
            if canonical in generated:
                raise WikiValidationError("DEDUP_OUTPUT_INVALID", "canonical_slug 不能指向 generated 候选")
            if canonical not in {target.slug for target in requested[slug].allowed_targets}:
                raise WikiValidationError("DEDUP_OUTPUT_INVALID", "canonical_slug 不在 allowed targets 中")
        result[slug] = canonical
    return result


def _clusters(items: Sequence[TopicCandidate]) -> list[list[TopicCandidate]]:
    parent = list(range(len(items)))
    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index
    def union(left: int, right: int) -> None:
        left, right = find(left), find(right)
        if left != right:
            parent[max(left, right)] = min(left, right)
    slugs: dict[str, int] = {}
    names: dict[tuple[str, str], int] = {}
    for index, item in enumerate(items):
        if item.slug in slugs:
            union(index, slugs[item.slug])
        else:
            slugs[item.slug] = index
        key = (item.page_type, _name(item.name))
        if key in names:
            union(index, names[key])
        else:
            names[key] = index
    grouped: dict[int, list[TopicCandidate]] = {}
    for index, item in enumerate(items):
        grouped.setdefault(find(index), []).append(item)
    return list(grouped.values())


def _exact_target(item: TopicCandidate, record: ExistingPageRecord) -> DedupPageCandidate:
    page = record.page
    if not isinstance(record.page_id, UUID) or not isinstance(record.version, int) or record.version < 1:
        raise WikiValidationError("DEDUP_EXISTING_INVALID", "已有页面记录身份无效")
    if page.slug != item.slug or page.page_type != item.page_type:
        raise WikiValidationError("DEDUP_EXISTING_INVALID", "已有页面与候选 slug 或类型不一致")
    return DedupPageCandidate(slug=page.slug, title=page.title, page_type=page.page_type, aliases=tuple(page.aliases))


async def deduplicate_candidates(scope: WikiScope, candidates: Sequence[TopicCandidate], store: DedupStorePort, model, *, limit: int = 20) -> tuple[list[TopicCandidate], dict[str, str]]:
    if isinstance(limit, bool) or not 1 <= limit <= 20:
        raise ValueError("dedup limit 必须在 1 到 20 之间")
    originals = [item.model_copy(deep=True) for item in candidates]
    merged = [_combine(cluster) for cluster in _clusters(originals)]
    mapping = {item.slug: merged[index].slug for index, cluster in enumerate(_clusters(originals)) for item in cluster}
    exact = await store.find_existing_pages(scope, [item.slug for item in merged])
    targets: dict[str, DedupPageCandidate] = {}
    undecided: list[TopicCandidate] = []
    for item in merged:
        record = exact.get(item.slug)
        if record is None:
            undecided.append(item)
        else:
            target = _exact_target(item, record)
            targets[target.slug] = target
            mapping[item.slug] = target.slug
    generated = {item.slug for item in undecided}
    requests: list[DedupCandidateRequest] = []
    for item in undecided:
        allowed: list[DedupPageCandidate] = []
        seen: set[str] = set()
        for target in await store.find_dedup_candidates(scope, item, limit):
            if target.slug in seen:
                continue
            seen.add(target.slug)
            if target.slug in generated or target.page_type != item.page_type or target.slug.startswith("summary/"):
                raise WikiValidationError("DEDUP_TARGET_INVALID", "dedup 候选目标不合法")
            allowed.append(target)
            targets[target.slug] = target
            if len(allowed) == limit:
                break
        if allowed:
            requests.append(DedupCandidateRequest(candidate=item, allowed_targets=tuple(allowed)))
    if requests:
        request = DedupRequest(candidates=tuple(requests))
        for slug, canonical in validate_dedup_output(request, await model.resolve_duplicates(request)).items():
            if canonical is not None:
                mapping[slug] = canonical
    output: list[TopicCandidate] = []
    positions: dict[str, int] = {}
    for item in merged:
        final = mapping[item.slug]
        target = targets.get(final)
        current = TopicCandidate(name=target.title, slug=target.slug, page_type=target.page_type, aliases=_unique([*target.aliases, item.name, *item.aliases]), description=item.description, details=item.details) if target else item.model_copy(deep=True)
        if final in positions:
            old = output[positions[final]]
            output[positions[final]] = TopicCandidate(name=old.name, slug=old.slug, page_type=old.page_type, aliases=_unique([*old.aliases, *current.aliases]), description="\n\n".join(_unique([old.description, current.description])), details="\n\n".join(_unique([old.details, current.details])))
        else:
            positions[final] = len(output)
            output.append(current)
    return output, {item.slug: mapping[mapping[item.slug]] if mapping[item.slug] in mapping else mapping[item.slug] for item in originals}
