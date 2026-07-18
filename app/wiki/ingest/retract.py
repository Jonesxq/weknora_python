"""贡献记录的差量规划和活动贡献投影。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import NoReturn
from uuid import UUID

from pydantic import ValidationError

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.schemas import ContributionDelta, StoredContributionRecord


def _reject(code: str, message: str) -> NoReturn:
    raise WikiValidationError(code, message)


def _snapshot_records(records: object, label: str) -> list[StoredContributionRecord]:
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        _reject("WIKI_CONTRIBUTION_INVALID_RECORDS", f"{label} 必须是贡献记录序列")
    snapshots: list[StoredContributionRecord] = []
    for index, record in enumerate(records):
        if not isinstance(record, StoredContributionRecord):
            _reject(
                "WIKI_CONTRIBUTION_INVALID_RECORD",
                f"{label}[{index}] 必须是有效贡献记录",
            )
        try:
            snapshots.append(
                StoredContributionRecord.model_validate(
                    record.model_dump(mode="python", warnings=False)
                )
            )
        except (ValidationError, TypeError, ValueError):
            _reject(
                "WIKI_CONTRIBUTION_INVALID_RECORD", f"{label}[{index}] 未通过完整校验"
            )
    return snapshots


def _pending_op_id(value: object) -> UUID:
    if not isinstance(value, UUID):
        _reject("WIKI_CONTRIBUTION_INVALID_PENDING_OP", "pending_op_id 必须是 UUID")
    return value


def _validate_operation(
    records: list[StoredContributionRecord], label: str, state: str
) -> tuple[int, UUID, str] | None:
    seen: set[str] = set()
    scope: tuple[int, UUID, str] | None = None
    version: str | None = None
    for record in records:
        if record.slug in seen:
            _reject("WIKI_CONTRIBUTION_DUPLICATE_SLUG", f"{label} 中 slug 不能重复")
        seen.add(record.slug)
        record_scope = (record.tenant_id, record.knowledge_base_id, record.knowledge_id)
        if scope is None:
            scope = record_scope
        elif scope != record_scope:
            _reject(
                "WIKI_CONTRIBUTION_MIXED_SCOPE",
                f"{label} 必须属于同一租户、知识库和知识来源",
            )
        if record.state != state:
            _reject("WIKI_CONTRIBUTION_INVALID_STATE", f"{label} 的记录状态不正确")
        if version is None:
            version = record.op_version
        elif version != record.op_version:
            _reject("WIKI_CONTRIBUTION_MIXED_VERSION", f"{label} 必须属于同一操作版本")
    return scope


def _changed(
    previous: StoredContributionRecord, current: StoredContributionRecord
) -> bool:
    return (
        previous.op_version,
        previous.page_type,
        previous.title,
        previous.content,
        previous.summary,
        previous.aliases,
        previous.chunk_refs,
    ) != (
        current.op_version,
        current.page_type,
        current.title,
        current.content,
        current.summary,
        current.aliases,
        current.chunk_refs,
    )


def plan_ingest_deltas(
    pending_op_id: UUID,
    previous: Sequence[StoredContributionRecord],
    current: Sequence[StoredContributionRecord],
) -> list[ContributionDelta]:
    """为一次新摄取规划新增、替换及过期撤回差量。"""

    pending = _pending_op_id(pending_op_id)
    old_records = _snapshot_records(previous, "previous")
    new_records = _snapshot_records(current, "current")
    old_scope = _validate_operation(old_records, "previous", "active")
    new_scope = _validate_operation(new_records, "current", "active")
    if old_scope is not None and new_scope is not None and old_scope != new_scope:
        _reject(
            "WIKI_CONTRIBUTION_SCOPE_MISMATCH",
            "新旧贡献必须属于同一租户、知识库和知识来源",
        )
    old_by_slug = {record.slug: record for record in old_records}
    result: list[ContributionDelta] = []
    for new in new_records:
        old = old_by_slug.get(new.slug)
        if old is None:
            result.append(
                ContributionDelta(
                    pending_op_id=pending,
                    action="add",
                    slug=new.slug,
                    knowledge_id=new.knowledge_id,
                    previous=None,
                    current=new,
                )
            )
            continue
        if (old.tenant_id, old.knowledge_base_id, old.knowledge_id, old.page_type) != (
            new.tenant_id,
            new.knowledge_base_id,
            new.knowledge_id,
            new.page_type,
        ):
            _reject(
                "WIKI_CONTRIBUTION_SCOPE_MISMATCH",
                "同 slug 的新旧贡献 scope、知识来源或页面类型不一致",
            )
        if _changed(old, new):
            result.append(
                ContributionDelta(
                    pending_op_id=pending,
                    action="replace",
                    slug=new.slug,
                    knowledge_id=new.knowledge_id,
                    previous=old,
                    current=new,
                )
            )
    current_slugs = {record.slug for record in new_records}
    for old in old_records:
        if old.slug not in current_slugs:
            result.append(
                ContributionDelta(
                    pending_op_id=pending,
                    action="retract_stale",
                    slug=old.slug,
                    knowledge_id=old.knowledge_id,
                    previous=old,
                    current=None,
                )
            )
    return result


def plan_retract_deltas(
    pending_op_id: UUID, records: Sequence[StoredContributionRecord]
) -> list[ContributionDelta]:
    """为已经标记为撤回中的贡献规划撤回差量。"""

    pending = _pending_op_id(pending_op_id)
    snapshots = _snapshot_records(records, "records")
    _validate_operation(snapshots, "records", "retract_pending")
    return [
        ContributionDelta(
            pending_op_id=pending,
            action="retract",
            slug=record.slug,
            knowledge_id=record.knowledge_id,
            previous=record,
            current=None,
        )
        for record in snapshots
    ]


def _active_projection(
    records: Sequence[StoredContributionRecord],
) -> list[StoredContributionRecord]:
    snapshots = _snapshot_records(records, "records")
    active = [record for record in snapshots if record.state == "active"]
    if not active:
        return []
    target = (
        active[0].tenant_id,
        active[0].knowledge_base_id,
        active[0].slug,
        active[0].page_type,
    )
    if any(
        (record.tenant_id, record.knowledge_base_id, record.slug, record.page_type)
        != target
        for record in active
    ):
        _reject("WIKI_CONTRIBUTION_MIXED_TARGET", "活动贡献必须投影到同一目标页面")
    return sorted(
        active,
        key=lambda record: (
            record.knowledge_id,
            record.op_version,
            record.slug,
            str(record.id or UUID(int=0)),
        ),
    )


def project_active_refs(
    records: Sequence[StoredContributionRecord],
) -> tuple[list[str], list[str]]:
    """投影活动贡献的知识来源与分块引用。"""

    sources: list[str] = []
    chunks: list[str] = []
    source_seen: set[str] = set()
    chunk_seen: set[str] = set()
    for record in _active_projection(records):
        if record.knowledge_id not in source_seen:
            source_seen.add(record.knowledge_id)
            sources.append(record.knowledge_id)
        for chunk in record.chunk_refs:
            if chunk not in chunk_seen:
                chunk_seen.add(chunk)
                chunks.append(chunk)
    return sources, chunks


def project_aliases(records: Sequence[StoredContributionRecord]) -> list[str]:
    """投影活动贡献的别名。"""

    aliases: list[str] = []
    seen: set[str] = set()
    for record in _active_projection(records):
        for alias in record.aliases:
            if alias not in seen:
                seen.add(alias)
                aliases.append(alias)
    return aliases
