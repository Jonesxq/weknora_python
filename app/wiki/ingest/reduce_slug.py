"""将同一 slug 的 Map 结果确定性合并为一个待写入页面。"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from typing import NoReturn, cast, overload
from uuid import UUID

from pydantic import ValidationError

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.ports import ChatModelPort
from app.wiki.ingest.schemas import (
    ContributionDelta,
    PageContribution,
    PageMergeRequest,
    ReducedPage,
    SlugUpdate,
    StoredContributionRecord,
    TopicPageType,
)
from app.wiki.ingest.retract import project_active_refs, project_aliases


_SLUG_PATTERN = re.compile(
    r"^(summary|entity|concept)/[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*$"
)


def _reject(code: str, message: str) -> NoReturn:
    raise WikiValidationError(code, message)


def _stable_clean(values: Iterable[str]) -> list[str]:
    """去除空值并稳定去重，不改变调用方传入的列表。"""

    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = value.strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            result.append(cleaned)
    return result


def _stable_unique_ids(values: Iterable[UUID]) -> list[UUID]:
    return list(dict.fromkeys(values))


def _snapshot_updates(updates: object) -> list[SlugUpdate]:
    if not isinstance(updates, Sequence) or isinstance(updates, (str, bytes)):
        _reject("WIKI_REDUCE_INVALID_UPDATES", "updates 必须是 SlugUpdate 序列")
    if not updates:
        _reject("WIKI_REDUCE_EMPTY", "同一 slug 的 updates 不能为空")

    snapshots: list[SlugUpdate] = []
    for index, update in enumerate(updates):
        if not isinstance(update, SlugUpdate):
            _reject(
                "WIKI_REDUCE_INVALID_UPDATE",
                f"update[{index}] 必须是有效的 SlugUpdate",
            )
        try:
            snapshots.append(
                SlugUpdate.model_validate(
                    update.model_dump(mode="python", warnings=False)
                )
            )
        except (ValidationError, TypeError, ValueError) as exc:
            _reject(
                "WIKI_REDUCE_INVALID_UPDATE",
                f"update[{index}] 类型或字段未通过完整校验: {exc}",
            )
    return snapshots


def _snapshot_existing(existing_page: object) -> ReducedPage | None:
    if existing_page is None:
        return None
    if not isinstance(existing_page, ReducedPage):
        _reject(
            "WIKI_REDUCE_INVALID_EXISTING",
            "已有页面必须是有效的 ReducedPage",
        )
    try:
        return ReducedPage.model_validate(
            existing_page.model_dump(mode="python", warnings=False)
        )
    except (ValidationError, TypeError, ValueError) as exc:
        _reject(
            "WIKI_REDUCE_INVALID_EXISTING",
            f"已有页面未通过完整校验: {exc}",
        )


def _trusted_source_refs(update: SlugUpdate) -> list[str]:
    supplied_refs = _stable_clean(update.source_refs)
    if any(source_ref != update.knowledge_id for source_ref in supplied_refs):
        _reject(
            "WIKI_REDUCE_SOURCE_MISMATCH",
            "topic update.source_refs 只能为空或包含自身 knowledge_id",
        )
    return [update.knowledge_id]


def _record_identity(
    record: StoredContributionRecord,
) -> tuple[int, UUID, str, str, str]:
    return (
        record.tenant_id,
        record.knowledge_base_id,
        record.slug,
        record.knowledge_id,
        record.op_version,
    )


def _record_contribution(
    record: StoredContributionRecord, pending_op_id: UUID
) -> PageContribution:
    return PageContribution(
        pending_op_id=pending_op_id,
        knowledge_id=record.knowledge_id,
        title=record.title,
        content=record.content,
        summary=record.summary,
        aliases=list(record.aliases),
        source_refs=[record.knowledge_id],
        chunk_refs=list(record.chunk_refs),
    )


def _record_sort_key(
    record: StoredContributionRecord,
) -> tuple[str, str, str, int, int]:
    return (
        record.knowledge_id,
        record.op_version,
        record.slug,
        0 if record.id is None else 1,
        0 if record.id is None else record.id.int,
    )


def _snapshot_deltas(deltas: object) -> list[ContributionDelta]:
    if not isinstance(deltas, Sequence) or isinstance(deltas, (str, bytes)):
        _reject("WIKI_REDUCE_INVALID_DELTAS", "deltas 必须是 ContributionDelta 序列")
    if not deltas:
        _reject("WIKI_REDUCE_EMPTY", "同一 slug 的 deltas 不能为空")
    snapshots: list[ContributionDelta] = []
    for index, delta in enumerate(deltas):
        if not isinstance(delta, ContributionDelta):
            _reject(
                "WIKI_REDUCE_INVALID_DELTA",
                f"delta[{index}] 必须是有效的 ContributionDelta",
            )
        try:
            snapshots.append(
                ContributionDelta.model_validate(
                    delta.model_dump(mode="python", warnings=False)
                )
            )
        except (ValidationError, TypeError, ValueError) as exc:
            _reject(
                "WIKI_REDUCE_INVALID_DELTA",
                f"delta[{index}] 类型或字段未通过完整校验: {exc}",
            )
    return snapshots


def _snapshot_active_contributions(
    records: object,
) -> list[StoredContributionRecord]:
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        _reject(
            "WIKI_REDUCE_INVALID_ACTIVE",
            "active_contributions 必须是贡献记录序列",
        )
    snapshots: list[StoredContributionRecord] = []
    for index, record in enumerate(records):
        if not isinstance(record, StoredContributionRecord):
            _reject(
                "WIKI_REDUCE_INVALID_ACTIVE_RECORD",
                f"active_contributions[{index}] 必须是有效贡献记录",
            )
        try:
            snapshot = StoredContributionRecord.model_validate(
                record.model_dump(mode="python", warnings=False)
            )
        except (ValidationError, TypeError, ValueError) as exc:
            _reject(
                "WIKI_REDUCE_INVALID_ACTIVE_RECORD",
                f"active_contributions[{index}] 未通过完整校验: {exc}",
            )
        if snapshot.state != "active":
            _reject(
                "WIKI_REDUCE_INVALID_ACTIVE_STATE",
                "active_contributions 只能包含 active 贡献",
            )
        snapshots.append(snapshot)
    return snapshots


def _validate_contribution_inputs(
    slug: object,
    deltas: Sequence[ContributionDelta],
    existing_page: ReducedPage | None,
    active_contributions: Sequence[StoredContributionRecord],
) -> str:
    if (
        not isinstance(slug, str)
        or len(slug) > 255
        or not _SLUG_PATTERN.fullmatch(slug)
    ):
        _reject(
            "WIKI_REDUCE_INVALID_SLUG",
            "slug 必须是 canonical 小写分层路径，且长度不能超过 255",
        )
    page_type = slug.partition("/")[0]
    records = [
        record
        for delta in deltas
        for record in (delta.previous, delta.current)
        if record is not None
    ]
    records.extend(active_contributions)
    if any(delta.slug != slug for delta in deltas):
        _reject("WIKI_REDUCE_SLUG_MISMATCH", "delta slug 必须与目标 slug 一致")
    if any(record.slug != slug for record in records):
        _reject(
            "WIKI_REDUCE_CONTRIBUTION_SLUG_MISMATCH",
            "所有 contribution slug 必须与目标 slug 一致",
        )
    if any(record.page_type != page_type for record in records):
        _reject(
            "WIKI_REDUCE_CONTRIBUTION_TYPE_MISMATCH",
            "所有 contribution 页面类型必须与目标 slug 前缀一致",
        )
    scopes = {(record.tenant_id, record.knowledge_base_id) for record in records}
    if len(scopes) > 1:
        _reject(
            "WIKI_REDUCE_CONTRIBUTION_SCOPE_MISMATCH",
            "所有 contribution 必须属于同一租户和知识库",
        )
    if page_type == "summary" and any(
        record.slug != f"summary/{record.knowledge_id}" for record in records
    ):
        _reject(
            "WIKI_REDUCE_SUMMARY_IDENTITY_MISMATCH",
            "summary slug 必须与 contribution knowledge_id 完全一致",
        )
    if existing_page is not None and (
        existing_page.slug != slug or existing_page.page_type != page_type
    ):
        _reject(
            "WIKI_REDUCE_EXISTING_MISMATCH",
            "已有页面的 slug 和页面类型必须与目标页面一致",
        )
    return page_type


async def _reduce_contributions(
    slug: str,
    deltas: Sequence[ContributionDelta],
    existing_page: ReducedPage | None,
    active_contributions: Sequence[StoredContributionRecord],
    model: ChatModelPort,
    *,
    legacy_metadata: bool = False,
) -> ReducedPage:
    """阶段三 Reduce 的唯一实现。"""

    contribution_deltas = _snapshot_deltas(deltas)
    snapshots = _snapshot_active_contributions(active_contributions)
    existing_page = _snapshot_existing(existing_page)
    _validate_contribution_inputs(slug, contribution_deltas, existing_page, snapshots)
    for delta in contribution_deltas:
        if delta.previous is not None:
            previous_identity = _record_identity(delta.previous)
            snapshots = [
                record
                for record in snapshots
                if _record_identity(record) != previous_identity
            ]
        if delta.current is not None:
            snapshots.append(delta.current)

    if not legacy_metadata:
        snapshots.sort(key=_record_sort_key)

    if snapshots and slug.startswith("summary/") and len(snapshots) != 1:
        _reject(
            "WIKI_REDUCE_SUMMARY_CONTRIBUTION_COUNT",
            "summary 页面必须恰好一份 active contribution",
        )

    if legacy_metadata and existing_page is not None and not existing_page.deleted:
        aliases = _stable_clean(
            [
                *(alias for record in snapshots for alias in record.aliases),
                *existing_page.aliases,
            ]
        )
        source_refs = _stable_clean(
            [*(record.knowledge_id for record in snapshots), *existing_page.source_refs]
        )
        chunk_refs = _stable_clean(
            [
                *(chunk for record in snapshots for chunk in record.chunk_refs),
                *existing_page.chunk_refs,
            ]
        )
    else:
        aliases = project_aliases(snapshots)
        source_refs, chunk_refs = project_active_refs(snapshots)
    contributor_op_ids = _stable_unique_ids(
        delta.pending_op_id for delta in contribution_deltas
    )
    if not snapshots:
        anchor = existing_page or next(
            record
            for delta in contribution_deltas
            for record in (delta.previous, delta.current)
            if record is not None
        )
        return ReducedPage(
            slug=slug,
            title=anchor.title,
            page_type=anchor.page_type,
            content=anchor.content,
            summary=anchor.summary,
            aliases=[],
            source_refs=[],
            chunk_refs=[],
            contributor_op_ids=contributor_op_ids,
            deleted=True,
        )

    if snapshots[0].page_type == "summary":
        record = snapshots[0]
        return ReducedPage(
            slug=slug,
            title=record.title,
            page_type="summary",
            content=record.content,
            summary=record.summary,
            aliases=aliases,
            source_refs=source_refs,
            chunk_refs=chunk_refs,
            contributor_op_ids=contributor_op_ids,
        )

    pending_by_record = {
        _record_identity(delta.current): delta.pending_op_id
        for delta in contribution_deltas
        if delta.current is not None
    }
    fallback_pending_op_id = contribution_deltas[0].pending_op_id
    topic_page_type = cast(TopicPageType, snapshots[0].page_type)
    request = PageMergeRequest(
        slug=slug,
        title=snapshots[0].title,
        page_type=topic_page_type,
        aliases=aliases,
        existing_content=(
            existing_page.content
            if existing_page is not None and not existing_page.deleted
            else ""
        ),
        existing_summary=(
            existing_page.summary
            if existing_page is not None and not existing_page.deleted
            else ""
        ),
        contributions=[
            _record_contribution(
                record,
                pending_by_record.get(_record_identity(record), fallback_pending_op_id),
            )
            for record in snapshots
        ],
    )
    output = await model.merge_page(request)
    summary_parts = [record.summary for record in snapshots]
    if legacy_metadata and existing_page is not None and not existing_page.deleted:
        summary_parts.append(existing_page.summary)
    return ReducedPage(
        slug=slug,
        title=output.headline,
        page_type=topic_page_type,
        content=output.markdown,
        summary="\n\n".join(_stable_clean(summary_parts)),
        aliases=aliases,
        source_refs=source_refs,
        chunk_refs=chunk_refs,
        contributor_op_ids=contributor_op_ids,
    )


def _legacy_record(update: SlugUpdate, index: int) -> StoredContributionRecord:
    summary = update.page_type == "summary"
    if not summary:
        _trusted_source_refs(update)
    return StoredContributionRecord(
        tenant_id=1,
        knowledge_base_id=UUID(int=0),
        slug=update.slug,
        knowledge_id=update.knowledge_id,
        op_version=f"legacy-{index}-{update.pending_op_id}",
        page_type=update.page_type,
        state="active",
        title=update.title,
        content=update.content,
        summary=update.summary,
        aliases=() if summary else tuple(_stable_clean(update.aliases)),
        chunk_refs=() if summary else tuple(_stable_clean(update.chunk_refs)),
    )


async def _reduce_legacy_adapter(
    slug: str,
    updates: Sequence[SlugUpdate],
    existing_page: ReducedPage | None,
    model: ChatModelPort,
) -> ReducedPage:
    """任务 11 前将阶段二 Worker 输入转换为阶段三贡献语义。"""

    snapshots = _snapshot_updates(updates)
    if snapshots[0].page_type == "summary" and len(snapshots) != 1:
        _reject("WIKI_REDUCE_SUMMARY_COUNT", "summary 页面每次必须恰好一个 update")
    records = [_legacy_record(update, index) for index, update in enumerate(snapshots)]
    deltas = [
        ContributionDelta(
            pending_op_id=update.pending_op_id,
            action="add",
            slug=update.slug,
            knowledge_id=update.knowledge_id,
            previous=None,
            current=record,
        )
        for update, record in zip(snapshots, records, strict=True)
    ]
    return await _reduce_contributions(
        slug,
        deltas,
        existing_page,
        [],
        model,
        legacy_metadata=snapshots[0].page_type != "summary",
    )


@overload
async def reduce_slug(
    slug: str,
    deltas: Sequence[ContributionDelta],
    existing_page: ReducedPage | None,
    active_contributions: Sequence[StoredContributionRecord],
    model: ChatModelPort,
) -> ReducedPage: ...


@overload
async def reduce_slug(
    slug: str,
    updates: Sequence[SlugUpdate],
    existing_page: ReducedPage | None,
    active_contributions: ChatModelPort,
) -> ReducedPage: ...


async def reduce_slug(
    slug: str,
    deltas: Sequence[ContributionDelta] | Sequence[SlugUpdate],
    existing_page: ReducedPage | None,
    active_contributions: Sequence[StoredContributionRecord] | ChatModelPort,
    model: ChatModelPort | None = None,
) -> ReducedPage:
    """投影贡献差量；四参数 overload 仅用于阶段二 Worker 兼容。"""

    if model is None:
        if not callable(getattr(active_contributions, "merge_page", None)):
            _reject("WIKI_REDUCE_INVALID_MODEL", "legacy Reduce 的第四参数必须是模型")
        return await _reduce_legacy_adapter(
            slug,
            cast(Sequence[SlugUpdate], deltas),
            existing_page,
            cast(ChatModelPort, active_contributions),
        )
    return await _reduce_contributions(
        slug,
        cast(Sequence[ContributionDelta], deltas),
        existing_page,
        cast(Sequence[StoredContributionRecord], active_contributions),
        model,
    )
