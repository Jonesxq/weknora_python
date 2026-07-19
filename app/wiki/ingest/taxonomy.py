"""Taxonomy topic 聚合与模型输出恢复。"""

from collections.abc import Iterable
from dataclasses import dataclass
from uuid import UUID

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.schemas import (
    AllowedFolderBase,
    ContributionDelta,
    TaxonomyDecision,
    TaxonomyOutput,
    TaxonomyRequest,
    TaxonomyTopic,
)


@dataclass(frozen=True, slots=True)
class TaxonomyWorkItem:
    topic: TaxonomyTopic
    contributor_op_ids: tuple[UUID, ...]


def build_taxonomy_work_items(
    deltas: Iterable[ContributionDelta], *, classifiable_slugs: Iterable[str]
) -> tuple[TaxonomyWorkItem, ...]:
    """把可分类的贡献增量归并为稳定的 taxonomy 工作项。"""
    allowed_slugs = set(classifiable_slugs)
    grouped: dict[str, list[ContributionDelta]] = {}
    for delta in deltas:
        current = delta.current
        if (
            current is None
            or current.slug not in allowed_slugs
            or current.page_type not in ("entity", "concept")
        ):
            continue
        grouped.setdefault(current.slug, []).append(delta)

    work_items: list[TaxonomyWorkItem] = []
    for slug in sorted(grouped):
        records = sorted(
            grouped[slug],
            key=lambda delta: (
                delta.current.knowledge_id if delta.current is not None else "",
                delta.current.op_version if delta.current is not None else "",
                str(delta.pending_op_id),
            ),
        )
        first_current = records[0].current
        assert first_current is not None

        summaries: list[str] = []
        seen_summaries: set[str] = set()
        for delta in records:
            current = delta.current
            assert current is not None
            summary = current.summary.strip()
            if summary and summary not in seen_summaries:
                summaries.append(summary)
                seen_summaries.add(summary)

        topic = TaxonomyTopic(
            slug=slug,
            title=first_current.title,
            page_type=first_current.page_type,
            summary="\n\n".join(summaries)[:4000],
        )
        contributor_op_ids = tuple(
            sorted({delta.pending_op_id for delta in records}, key=str)
        )
        work_items.append(
            TaxonomyWorkItem(topic=topic, contributor_op_ids=contributor_op_ids)
        )
    return tuple(work_items)


def recover_taxonomy_output(
    request: TaxonomyRequest, output: TaxonomyOutput
) -> dict[str, TaxonomyDecision]:
    """验证模型 taxonomy 输出，并返回与调用者隔离的严格快照。"""
    request_snapshot = TaxonomyRequest.model_validate(request.model_dump(mode="python"))
    output_snapshot = TaxonomyOutput.model_validate(output.model_dump(mode="python"))

    requested_slugs = {topic.slug for topic in request_snapshot.topics}
    decisions = output_snapshot.decisions
    decision_slugs = [decision.slug for decision in decisions]
    if (
        len(decision_slugs) != len(requested_slugs)
        or len(decision_slugs) != len(set(decision_slugs))
        or set(decision_slugs) != requested_slugs
    ):
        _invalid("taxonomy 输出必须完整且恰好覆盖请求 topic")

    allowed_bases = {base.id: base for base in request_snapshot.allowed_bases}
    recovered: dict[str, TaxonomyDecision] = {}
    for decision in decisions:
        base = _resolve_allowed_base(decision, allowed_bases)
        base_depth = base.depth if base is not None else 0
        if base_depth + len(decision.new_segments) > 3:
            _invalid("taxonomy 目录总深度不能超过 3")

        folder_path = _folder_path(base, decision.new_segments)
        wiki_path = (
            f"{folder_path}/{decision.slug}"
            if folder_path
            else f"/{decision.slug}"
        )
        if len(folder_path) > 2048:
            _invalid("taxonomy 目录 path 长度不能超过 2048")
        if len(wiki_path) > 1024:
            _invalid("taxonomy 最终 wiki_path 长度不能超过 1024")
        recovered[decision.slug] = decision
    return recovered


def _resolve_allowed_base(
    decision: TaxonomyDecision, allowed_bases: dict[UUID, AllowedFolderBase]
) -> AllowedFolderBase | None:
    if decision.base_folder_id is None:
        return None
    base = allowed_bases.get(decision.base_folder_id)
    if base is None:
        _invalid("taxonomy base_folder_id 必须位于请求白名单")
    return base


def _folder_path(
    base: AllowedFolderBase | None, new_segments: tuple[str, ...]
) -> str:
    base_path = base.path if base is not None else ""
    if not new_segments:
        return base_path
    suffix = "/".join(new_segments)
    return f"{base_path}/{suffix}" if base_path else f"/{suffix}"


def _invalid(message: str) -> None:
    raise WikiValidationError("TAXONOMY_OUTPUT_INVALID", message)
