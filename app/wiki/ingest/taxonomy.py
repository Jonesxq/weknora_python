"""Taxonomy topic 聚合、目录候选筛选与模型输出恢复。"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
import math
from uuid import UUID

from pydantic import ValidationError
from pydantic_core import PydanticSerializationError

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.ports import EmbeddingModelPort
from app.wiki.ingest.schemas import (
    AllowedFolderBase,
    ContributionDelta,
    EmbeddingItem,
    EmbeddingOutput,
    EmbeddingRequest,
    FolderCatalogEntry,
    TaxonomyDecision,
    TaxonomyContext,
    TaxonomyOutput,
    TaxonomyRequest,
    TaxonomyTopic,
)


@dataclass(frozen=True, slots=True)
class TaxonomyWorkItem:
    topic: TaxonomyTopic
    contributor_op_ids: tuple[UUID, ...]


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    """返回两个有限、等维向量的余弦相似度。"""
    if not left or not right or len(left) != len(right):
        _embedding_invalid("embedding 向量维度必须一致且不能为空")
    try:
        if not all(math.isfinite(value) for value in left) or not all(
            math.isfinite(value) for value in right
        ):
            _embedding_invalid("embedding 向量必须全部为有限数")
    except TypeError as exc:
        raise WikiValidationError(
            "EMBEDDING_OUTPUT_INVALID", "embedding 向量必须全部为有限数"
        ) from exc

    left_scale = max(abs(value) for value in left)
    right_scale = max(abs(value) for value in right)
    if left_scale == 0.0 or right_scale == 0.0:
        return 0.0

    left_scaled = tuple(value / left_scale for value in left)
    right_scaled = tuple(value / right_scale for value in right)
    left_norm = math.sqrt(math.fsum(value**2 for value in left_scaled))
    right_norm = math.sqrt(math.fsum(value**2 for value in right_scaled))
    left_unit = tuple(value / left_norm for value in left_scaled)
    right_unit = tuple(value / right_norm for value in right_scaled)
    similarity = math.fsum(
        left_value * right_value
        for left_value, right_value in zip(left_unit, right_unit)
    )
    return max(-1.0, min(1.0, similarity))


async def select_allowed_bases(
    topics: Iterable[TaxonomyTopic],
    folders: Iterable[FolderCatalogEntry],
    embedding: EmbeddingModelPort,
    *,
    full_catalog_limit: int,
    related_limit: int,
) -> tuple[AllowedFolderBase, ...]:
    """为 taxonomy 模型选择稳定且可追溯的目录候选集。"""
    if full_catalog_limit <= 0 or related_limit <= 0:
        raise ValueError("full_catalog_limit 和 related_limit 必须为正数")

    topic_snapshots, folder_snapshots = _snapshot_selection_inputs(topics, folders)
    ordered_folders = tuple(sorted(folder_snapshots, key=_folder_sort_key))
    if len(ordered_folders) <= full_catalog_limit:
        return _allowed_bases(ordered_folders)

    roots = tuple(folder for folder in ordered_folders if folder.depth == 1)
    deep_folders = tuple(folder for folder in ordered_folders if folder.depth > 1)
    if not deep_folders:
        return _allowed_bases(roots)
    if not topic_snapshots:
        _embedding_invalid("embedding 选择深层目录时 topic 不能为空")

    items = tuple(
        [
            EmbeddingItem(
                key=f"topic:{topic.slug}",
                text=f"{topic.title}\n{topic.summary}".strip(),
            )
            for topic in topic_snapshots
        ]
        + [
            EmbeddingItem(key=f"folder:{folder.id}", text=folder.path)
            for folder in deep_folders
        ]
    )
    request = EmbeddingRequest(items=items)
    output = await embedding.embed(request)
    vectors = _recover_embedding_output(request, output)

    topic_vectors = tuple(vectors[f"topic:{topic.slug}"] for topic in topic_snapshots)
    ranked = sorted(
        (
            (
                max(
                    cosine_similarity(vectors[f"folder:{folder.id}"], topic_vector)
                    for topic_vector in topic_vectors
                ),
                folder,
            )
            for folder in deep_folders
        ),
        key=lambda item: (-item[0], *_folder_sort_key(item[1])),
    )
    selected_ids = {folder.id for _, folder in ranked[:related_limit]}
    selected_ids.update(folder.id for folder in roots)
    by_id = {folder.id: folder for folder in ordered_folders}
    for folder_id in tuple(selected_ids):
        current = by_id[folder_id]
        while current.parent_id is not None:
            current = by_id[current.parent_id]
            selected_ids.add(current.id)
    return _allowed_bases(folder for folder in ordered_folders if folder.id in selected_ids)


def _snapshot_selection_inputs(
    topics: Iterable[TaxonomyTopic], folders: Iterable[FolderCatalogEntry]
) -> tuple[tuple[TaxonomyTopic, ...], tuple[FolderCatalogEntry, ...]]:
    try:
        raw_topics = tuple(topics)
        raw_folders = tuple(folders)
    except TypeError as exc:
        raise WikiValidationError(
            "EMBEDDING_OUTPUT_INVALID", "embedding 输入目录或 topic 结构无效"
        ) from exc
    if not all(isinstance(topic, TaxonomyTopic) for topic in raw_topics) or not all(
        isinstance(folder, FolderCatalogEntry) for folder in raw_folders
    ):
        _embedding_invalid("embedding 输入目录或 topic 结构无效")
    try:
        topic_snapshots = tuple(
            TaxonomyTopic.model_validate(
                topic.model_dump(mode="python", warnings="error")
            )
            for topic in raw_topics
        )
        context = TaxonomyContext.model_validate(
            {
                "folders": [
                    folder.model_dump(mode="python", warnings="error")
                    for folder in raw_folders
                ],
                "classifiable_slugs": (),
            }
        )
    except (ValidationError, PydanticSerializationError) as exc:
        raise WikiValidationError(
            "EMBEDDING_OUTPUT_INVALID", "embedding 输入目录或 topic 结构无效"
        ) from exc
    if len({topic.slug for topic in topic_snapshots}) != len(topic_snapshots):
        _embedding_invalid("embedding topic slug 不能重复")
    return tuple(sorted(topic_snapshots, key=lambda topic: topic.slug)), context.folders


def _recover_embedding_output(
    request: EmbeddingRequest, output: EmbeddingOutput
) -> dict[str, tuple[float, ...]]:
    if not isinstance(output, EmbeddingOutput):
        _embedding_invalid("embedding 输出结构无效")
    try:
        output_snapshot = EmbeddingOutput.model_validate(
            output.model_dump(mode="python", warnings="error")
        )
    except (ValidationError, PydanticSerializationError) as exc:
        raise WikiValidationError(
            "EMBEDDING_OUTPUT_INVALID", "embedding 输出结构无效"
        ) from exc
    requested_keys = {item.key for item in request.items}
    if set(output_snapshot.vectors) != requested_keys:
        _embedding_invalid("embedding 输出必须完整且恰好覆盖请求 key")
    return dict(output_snapshot.vectors)


def _folder_sort_key(folder: FolderCatalogEntry) -> tuple[int, str, str]:
    return folder.depth, folder.path, str(folder.id)


def _allowed_bases(
    folders: Iterable[FolderCatalogEntry],
) -> tuple[AllowedFolderBase, ...]:
    return tuple(
        AllowedFolderBase(id=folder.id, path=folder.path, depth=folder.depth)
        for folder in folders
    )


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
                delta.current.page_type if delta.current is not None else "",
                delta.current.title if delta.current is not None else "",
                delta.current.summary.strip() if delta.current is not None else "",
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
    if not isinstance(request, TaxonomyRequest) or not isinstance(
        output, TaxonomyOutput
    ):
        _invalid("taxonomy 请求或输出结构无效")
    try:
        request_snapshot = TaxonomyRequest.model_validate(
            request.model_dump(mode="python", warnings="error")
        )
        output_snapshot = TaxonomyOutput.model_validate(
            output.model_dump(mode="python", warnings="error")
        )
    except (ValidationError, PydanticSerializationError) as exc:
        raise WikiValidationError(
            "TAXONOMY_OUTPUT_INVALID", "taxonomy 请求或输出结构无效"
        ) from exc

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
        if (
            base is not None
            and decision.new_segments
            and base.path.rsplit("/", maxsplit=1)[-1].casefold()
            == decision.new_segments[0].casefold()
        ):
            _invalid("taxonomy 相邻目录段不能仅大小写不同")
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


def _embedding_invalid(message: str) -> None:
    raise WikiValidationError("EMBEDDING_OUTPUT_INVALID", message)
