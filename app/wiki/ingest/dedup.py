"""Wiki 主题候选与既有页面的受限去重。"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Protocol
from uuid import UUID

from pydantic import ValidationError
from app.wiki.errors import WikiValidationError
from app.wiki.ingest.ports import DedupModelPort
from app.wiki.ingest.schemas import (
    DedupCandidateRequest,
    DedupOutput,
    DedupPageCandidate,
    DedupRequest,
    ReducedPage,
    TopicCandidate,
)
from app.wiki.ingest.store import ExistingPageRecord
from app.wiki.scope import WikiScope

_MAX_DEDUP_QUERY_NAMES = 64


class DedupStorePort(Protocol):
    async def find_existing_pages(
        self, scope: WikiScope, slugs: Sequence[str]
    ) -> dict[str, ExistingPageRecord]: ...
    async def find_dedup_candidates(
        self, scope: WikiScope, candidate: TopicCandidate, limit: int = 20
    ) -> list[DedupPageCandidate]: ...


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
        name=first.name,
        slug=first.slug,
        page_type=first.page_type,
        aliases=_unique(
            [
                *first.aliases,
                *[value for item in items[1:] for value in (item.name, *item.aliases)],
            ]
        ),
        description="\n\n".join(_unique([item.description for item in items])),
        details="\n\n".join(_unique([item.details for item in items])),
    )


def validate_dedup_output(
    request: DedupRequest, output: DedupOutput
) -> dict[str, str | None]:
    try:
        request = DedupRequest.model_validate(request.model_dump(mode="python"))
        output = DedupOutput.model_validate(output.model_dump(mode="python"))
    except (ValidationError, TypeError, AttributeError, ValueError) as exc:
        raise WikiValidationError("DEDUP_OUTPUT_INVALID", "dedup 输出结构无效") from exc
    requested = {item.candidate.slug: item for item in request.candidates}
    generated = set(requested)
    decisions = {decision.candidate_slug: decision for decision in output.decisions}
    if len(decisions) != len(output.decisions) or set(decisions) != set(requested):
        raise WikiValidationError(
            "DEDUP_OUTPUT_INVALID", "dedup 输出必须完整且恰好覆盖请求候选"
        )
    result: dict[str, str | None] = {}
    for slug, decision in decisions.items():
        canonical = decision.canonical_slug
        if canonical is not None:
            if canonical in generated:
                raise WikiValidationError(
                    "DEDUP_OUTPUT_INVALID", "canonical_slug 不能指向 generated 候选"
                )
            allowed = {
                target.slug: target for target in requested[slug].allowed_targets
            }
            target = allowed.get(canonical)
            if target is None:
                raise WikiValidationError(
                    "DEDUP_OUTPUT_INVALID", "canonical_slug 不在 allowed targets 中"
                )
            if target.page_type != requested[
                slug
            ].candidate.page_type or target.slug.startswith("summary/"):
                raise WikiValidationError(
                    "DEDUP_OUTPUT_INVALID", "canonical_slug 类型不合法"
                )
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


def _exact_target(
    item: TopicCandidate, record: ExistingPageRecord
) -> DedupPageCandidate:
    if (
        not isinstance(record, ExistingPageRecord)
        or not isinstance(record.page_id, UUID)
        or type(record.version) is not int
        or record.version < 1
    ):
        raise WikiValidationError("DEDUP_EXISTING_INVALID", "已有页面记录身份无效")
    try:
        page = ReducedPage.model_validate(
            record.page.model_dump(mode="python", warnings=False)
        )
    except (ValidationError, TypeError, AttributeError, ValueError) as exc:
        raise WikiValidationError(
            "DEDUP_EXISTING_INVALID", "已有页面记录内容无效"
        ) from exc
    if page.slug != item.slug or page.page_type != item.page_type:
        raise WikiValidationError(
            "DEDUP_EXISTING_INVALID", "已有页面与候选 slug 或类型不一致"
        )
    try:
        return DedupPageCandidate.model_validate(
            {
                "slug": page.slug,
                "title": page.title,
                "page_type": page.page_type,
                "aliases": page.aliases,
            }
        )
    except (ValidationError, TypeError, AttributeError, ValueError) as exc:
        raise WikiValidationError(
            "DEDUP_EXISTING_INVALID", "已有页面记录内容无效"
        ) from exc


async def deduplicate_candidates(
    scope: WikiScope,
    candidates: Sequence[TopicCandidate],
    store: DedupStorePort,
    model: DedupModelPort,
    *,
    limit: int = 20,
) -> tuple[list[TopicCandidate], dict[str, str]]:
    if type(limit) is not int or not 1 <= limit <= 20:
        raise ValueError("dedup limit 必须在 1 到 20 之间")
    originals = [item.model_copy(deep=True) for item in candidates]
    for item in originals:
        if (
            len({_name(value) for value in (item.name, *item.aliases) if _name(value)})
            > _MAX_DEDUP_QUERY_NAMES
        ):
            raise ValueError("dedup 查询名称不能超过 64 个")
    clusters = _clusters(originals)
    exact = await store.find_existing_pages(scope, [item.slug for item in originals])
    source_by_slug = {item.slug: item for item in originals}
    exact_targets: dict[str, DedupPageCandidate] = {}
    for slug, record in exact.items():
        source = source_by_slug.get(slug)
        if source is None:
            raise WikiValidationError(
                "DEDUP_EXISTING_INVALID", "已有页面不属于请求候选"
            )
        exact_targets[slug] = _exact_target(source, record)
    mapping: dict[str, str] = {}
    targets: dict[str, DedupPageCandidate] = {}
    undecided: list[TopicCandidate] = []
    for cluster in clusters:
        anchors = {
            exact_targets[item.slug].slug: exact_targets[item.slug]
            for item in cluster
            if item.slug in exact_targets
        }
        if len(anchors) > 1:
            raise WikiValidationError(
                "DEDUP_AMBIGUOUS_EXACT", "同一候选簇命中多个已有页面"
            )
        item = _combine(cluster)
        if anchors:
            target = next(iter(anchors.values()))
            targets[target.slug] = target
            item = TopicCandidate(
                name=target.title,
                slug=target.slug,
                page_type=target.page_type,
                aliases=_unique(
                    [
                        *target.aliases,
                        *[
                            v
                            for candidate in cluster
                            for v in (candidate.name, *candidate.aliases)
                        ],
                    ]
                ),
                description=item.description,
                details=item.details,
            )
            for candidate in cluster:
                mapping[candidate.slug] = target.slug
        else:
            for candidate in cluster:
                mapping[candidate.slug] = item.slug
            undecided.append(item)
    generated = {item.slug for item in originals} - set(exact_targets)
    requests: list[DedupCandidateRequest] = []
    results: list[list[DedupPageCandidate] | None] = [None] * len(undecided)
    queue: asyncio.Queue[tuple[int, TopicCandidate] | None] = asyncio.Queue()
    for index, item in enumerate(undecided):
        queue.put_nowait((index, item))
    worker_count = min(8, len(undecided))
    for _ in range(worker_count):
        queue.put_nowait(None)

    async def worker() -> None:
        while True:
            work = await queue.get()
            if work is None:
                return
            index, item = work
            results[index] = await store.find_dedup_candidates(scope, item, limit)

    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    try:
        await asyncio.gather(*workers)
    except BaseException:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise
    for item, found_targets in zip(undecided, results, strict=True):
        if found_targets is None:
            raise RuntimeError("dedup worker 未返回结果")
        allowed: list[DedupPageCandidate] = []
        seen: set[str] = set()
        for raw_target in found_targets:
            try:
                target = DedupPageCandidate.model_validate(
                    raw_target.model_dump(mode="python", warnings=False)
                )
            except (ValidationError, TypeError, AttributeError, ValueError) as exc:
                raise WikiValidationError(
                    "DEDUP_TARGET_INVALID", "dedup 候选目标无效"
                ) from exc
            if target.slug in seen:
                continue
            seen.add(target.slug)
            if (
                target.slug in generated
                or target.page_type != item.page_type
                or target.slug.startswith("summary/")
            ):
                raise WikiValidationError(
                    "DEDUP_TARGET_INVALID", "dedup 候选目标不合法"
                )
            allowed.append(target)
            targets[target.slug] = target
            if len(allowed) == limit:
                break
        if allowed:
            requests.append(
                DedupCandidateRequest(candidate=item, allowed_targets=tuple(allowed))
            )
    if requests:
        request = DedupRequest(candidates=tuple(requests))
        for slug, canonical in validate_dedup_output(
            request, await model.resolve_duplicates(request)
        ).items():
            if canonical is not None:
                mapping[slug] = canonical
    output: list[TopicCandidate] = []
    positions: dict[str, int] = {}
    for cluster in clusters:
        representative = _combine(cluster)
        final = mapping[cluster[0].slug]
        target = targets.get(final)
        current = (
            TopicCandidate(
                name=target.title,
                slug=target.slug,
                page_type=target.page_type,
                aliases=_unique(
                    [
                        *target.aliases,
                        *[v for c in cluster for v in (c.name, *c.aliases)],
                    ]
                ),
                description=representative.description,
                details=representative.details,
            )
            if target
            else representative.model_copy(deep=True)
        )
        if final in positions:
            old = output[positions[final]]
            output[positions[final]] = TopicCandidate(
                name=old.name,
                slug=old.slug,
                page_type=old.page_type,
                aliases=_unique([*old.aliases, *current.aliases]),
                description="\n\n".join(
                    _unique([old.description, current.description])
                ),
                details="\n\n".join(_unique([old.details, current.details])),
            )
        else:
            positions[final] = len(output)
            output.append(current)
    return output, {
        item.slug: mapping[mapping[item.slug]]
        if mapping[item.slug] in mapping
        else mapping[item.slug]
        for item in originals
    }
