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
    record: StoredContributionRecord, pending_op_id: UUID | None
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


def _source_identity(record: StoredContributionRecord) -> tuple[int, UUID, str, str]:
    return (
        record.tenant_id,
        record.knowledge_base_id,
        record.slug,
        record.knowledge_id,
    )


def _record_payload(record: StoredContributionRecord) -> tuple[object, ...]:
    """比较贡献内容，忽略数据库 id 和 retract_pending 状态。"""

    return (
        record.page_type,
        record.title,
        record.content,
        record.summary,
        record.aliases,
        record.chunk_refs,
    )


def _stable_unique_deltas(
    deltas: Sequence[ContributionDelta],
) -> list[ContributionDelta]:
    result: list[ContributionDelta] = []
    for delta in deltas:
        if delta not in result:
            result.append(delta)
    return result


def _apply_contribution_transitions(
    deltas: Sequence[ContributionDelta],
    active_contributions: Sequence[StoredContributionRecord],
) -> list[StoredContributionRecord]:
    """先规划整个批次，再以集合语义统一应用 remove/add。"""

    unique_deltas = _stable_unique_deltas(deltas)
    removals: dict[tuple[int, UUID, str, str, str], list[int]] = {}
    additions: dict[tuple[int, UUID, str, str, str], StoredContributionRecord] = {}
    addition_origins: dict[tuple[int, UUID, str, str, str], list[int]] = {}
    current_by_source: dict[
        tuple[int, UUID, str, str], tuple[int, UUID, str, str, str]
    ] = {}

    for index, delta in enumerate(unique_deltas):
        if delta.previous is not None:
            removals.setdefault(_record_identity(delta.previous), []).append(index)
        if delta.current is None:
            continue
        identity = _record_identity(delta.current)
        source = _source_identity(delta.current)
        existing_current = additions.get(identity)
        if existing_current is not None and _record_payload(
            existing_current
        ) != _record_payload(delta.current):
            _reject(
                "WIKI_REDUCE_CURRENT_CONFLICT",
                "同一 exact identity 存在冲突的 current contribution",
            )
        existing_identity = current_by_source.get(source)
        if existing_identity is not None and existing_identity != identity:
            _reject(
                "WIKI_REDUCE_CURRENT_CONFLICT",
                "同一 source/slug 不能包含多个不同 current contribution",
            )
        additions.setdefault(identity, delta.current)
        addition_origins.setdefault(identity, []).append(index)
        current_by_source[source] = identity

    for identity in removals.keys() & additions.keys():
        remove_origins = removals[identity]
        add_origins = addition_origins[identity]
        same_replace = (
            len(remove_origins) == 1
            and remove_origins == add_origins
            and unique_deltas[remove_origins[0]].action == "replace"
        )
        if not same_replace:
            _reject(
                "WIKI_REDUCE_TRANSITION_CONFLICT",
                "同一 exact identity 的 add 与 retract transition 冲突",
            )

    active_by_identity: dict[
        tuple[int, UUID, str, str, str], StoredContributionRecord
    ] = {}
    active_by_source: dict[
        tuple[int, UUID, str, str], tuple[int, UUID, str, str, str]
    ] = {}
    for record in active_contributions:
        identity = _record_identity(record)
        source = _source_identity(record)
        existing = active_by_identity.get(identity)
        if existing is not None:
            if _record_payload(existing) != _record_payload(record):
                _reject(
                    "WIKI_REDUCE_ACTIVE_CONFLICT",
                    "active snapshot 的同一 identity 存在不同 payload",
                )
            continue
        existing_identity = active_by_source.get(source)
        if existing_identity is not None and existing_identity != identity:
            _reject(
                "WIKI_REDUCE_ACTIVE_CONFLICT",
                "active snapshot 的同一 source/slug 存在多个版本",
            )
        active_by_identity[identity] = record
        active_by_source[source] = identity

    for identity in removals:
        removed = active_by_identity.pop(identity, None)
        if removed is not None:
            active_by_source.pop(_source_identity(removed), None)

    for identity, record in additions.items():
        existing = active_by_identity.get(identity)
        if existing is not None:
            if _record_payload(existing) != _record_payload(record):
                _reject(
                    "WIKI_REDUCE_CURRENT_CONFLICT",
                    "current contribution 与 active payload 冲突",
                )
            continue
        source = _source_identity(record)
        existing_identity = active_by_source.get(source)
        if existing_identity is not None and existing_identity != identity:
            _reject(
                "WIKI_REDUCE_CURRENT_CONFLICT",
                "current contribution 与 active source/version 冲突",
            )
        active_by_identity[identity] = record
        active_by_source[source] = identity

    return list(active_by_identity.values())


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
    page_type = _validate_contribution_inputs(
        slug, contribution_deltas, existing_page, snapshots
    )
    if page_type == "summary":
        summary_identities = {_record_identity(record) for record in snapshots}
        summary_identities.difference_update(
            _record_identity(delta.previous)
            for delta in contribution_deltas
            if delta.previous is not None
        )
        summary_identities.update(
            _record_identity(delta.current)
            for delta in contribution_deltas
            if delta.current is not None
        )
        if len(summary_identities) > 1:
            _reject(
                "WIKI_REDUCE_SUMMARY_CONTRIBUTION_COUNT",
                "summary 页面必须恰好一份 active contribution",
            )
    snapshots = _apply_contribution_transitions(contribution_deltas, snapshots)

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

    pending_ids_by_record: dict[tuple[int, UUID, str, str, str], set[UUID]] = {}
    for delta in contribution_deltas:
        if delta.current is not None:
            pending_ids_by_record.setdefault(
                _record_identity(delta.current), set()
            ).add(delta.pending_op_id)
    pending_by_record = {
        identity: next(iter(pending_ids)) if len(pending_ids) == 1 else None
        for identity, pending_ids in pending_ids_by_record.items()
    }
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
                pending_by_record.get(_record_identity(record)),
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
    model: ChatModelPort,
) -> ReducedPage: ...


async def reduce_slug(
    slug: str,
    deltas: Sequence[ContributionDelta] | Sequence[SlugUpdate] | None = None,
    existing_page: ReducedPage | None = None,
    active_contributions: Sequence[StoredContributionRecord]
    | ChatModelPort
    | None = None,
    model: ChatModelPort | None = None,
    *,
    updates: Sequence[SlugUpdate] | None = None,
) -> ReducedPage:
    """投影贡献差量；四参数 overload 仅用于阶段二 Worker 兼容。"""

    if updates is not None:
        if deltas is not None or active_contributions is not None:
            _reject(
                "WIKI_REDUCE_INVALID_LEGACY_ARGUMENTS",
                "legacy updates 不能与 deltas/active_contributions 同时提供",
            )
        if model is None:
            _reject("WIKI_REDUCE_INVALID_MODEL", "legacy Reduce 必须提供 model")
        return await _reduce_legacy_adapter(
            slug,
            updates,
            existing_page,
            model,
        )

    if deltas is None:
        _reject("WIKI_REDUCE_INVALID_DELTAS", "必须提供 deltas 或 legacy updates")
    positional_legacy_model = (
        cast(ChatModelPort, active_contributions)
        if callable(getattr(active_contributions, "merge_page", None)) and model is None
        else None
    )
    keyword_legacy_model = (
        model
        if active_contributions is None
        and isinstance(deltas, Sequence)
        and not isinstance(deltas, (str, bytes))
        and bool(deltas)
        and all(isinstance(update, SlugUpdate) for update in deltas)
        else None
    )
    legacy_model = positional_legacy_model or keyword_legacy_model
    if legacy_model is not None:
        return await _reduce_legacy_adapter(
            slug,
            cast(Sequence[SlugUpdate], deltas),
            existing_page,
            legacy_model,
        )
    if active_contributions is None or callable(
        getattr(active_contributions, "merge_page", None)
    ):
        _reject(
            "WIKI_REDUCE_INVALID_ACTIVE",
            "modern Reduce 必须提供 active_contributions 序列",
        )
    if model is None:
        _reject("WIKI_REDUCE_INVALID_MODEL", "modern Reduce 必须提供 model")
    return await _reduce_contributions(
        slug,
        cast(Sequence[ContributionDelta], deltas),
        existing_page,
        cast(Sequence[StoredContributionRecord], active_contributions),
        model,
    )
