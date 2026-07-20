"""Wiki 索引介绍的模型请求规划与确定性回退。"""

from __future__ import annotations

from collections.abc import Iterable
from uuid import UUID

from app.wiki.ingest.schemas import (
    ContributionDelta,
    IndexIntroChange,
    IndexIntroContext,
    IndexIntroOutput,
    IndexIntroPlan,
    IndexIntroRequest,
    IndexSummaryItem,
    ReducedPage,
    StoredContributionRecord,
)


DEFAULT_INDEX_INTRO = "本知识库汇总了当前已发布的文档摘要、实体与概念。"
LEGACY_INDEX_PLACEHOLDERS = frozenset(("", "Wiki Index", "知识库索引"))
INDEX_INTRO_MAX_CHARS = 4000


def clean_index_intro(value: str) -> str:
    """保留模型的 Markdown 正文，移除其附加的目录段。"""
    if not isinstance(value, str):
        raise ValueError("index intro 必须是字符串")
    intro = value.strip()
    directory_start = intro.find("\n## ")
    if intro.startswith("## "):
        intro = ""
    elif directory_start >= 0:
        intro = intro[:directory_start].strip()
    if not intro or len(intro) > INDEX_INTRO_MAX_CHARS:
        raise ValueError("index intro 清理后必须为 1 到 4000 个字符")
    return intro


def build_index_intro_request(
    context: IndexIntroContext,
    *,
    completed_op_ids: Iterable[UUID],
    pages: Iterable[ReducedPage],
    contribution_deltas: Iterable[ContributionDelta],
    operation_actions: Iterable[tuple[str, str]],
) -> IndexIntroRequest | None:
    """根据已完成操作为 Index 生成一次模型请求。"""
    completed = tuple(completed_op_ids)
    if not completed:
        return None

    actions = _normalize_actions(operation_actions)
    if not actions:
        return None
    if not isinstance(context, IndexIntroContext):
        raise ValueError("index intro context 无效")

    page_snapshots = tuple(_snapshot_page(page) for page in pages)
    delta_snapshots = tuple(_snapshot_delta(delta) for delta in contribution_deltas)
    mode = "create" if _is_create_context(context) else "update"
    if mode == "create":
        return IndexIntroRequest(
            mode="create",
            summaries=_create_summaries(page_snapshots, context.recent_summaries),
        )

    index = context.index
    assert index is not None
    final_pages = _final_summary_pages(page_snapshots)
    changes = tuple(
        IndexIntroChange(
            action=action,
            knowledge_id=knowledge_id,
            pages=_change_pages(
                action,
                knowledge_id,
                final_pages,
                delta_snapshots,
                set(completed),
            ),
        )
        for action, knowledge_id in actions
    )
    return IndexIntroRequest(
        mode="update", existing_intro=index.content, changes=changes
    )


def build_success_index_intro_plan(
    context: IndexIntroContext,
    request: IndexIntroRequest,
    output: IndexIntroOutput,
) -> IndexIntroPlan:
    """把清理后的模型输出转换为带 CAS 快照的写入计划。"""
    _validate_request_context(context, request)
    if not isinstance(output, IndexIntroOutput):
        raise ValueError("index intro output 无效")
    expected_page_id, expected_version = _expected_index(context)
    return IndexIntroPlan(
        mode=request.mode,
        expected_page_id=expected_page_id,
        expected_version=expected_version,
        intro=clean_index_intro(output.intro),
        model_status="generated",
    )


def fallback_index_intro_plan(
    context: IndexIntroContext,
    request: IndexIntroRequest,
    *,
    error_code: str,
) -> IndexIntroPlan:
    """模型调用失败时，生成可安全应用的默认或保留计划。"""
    _validate_request_context(context, request)
    expected_page_id, expected_version = _expected_index(context)
    if request.mode == "create":
        return IndexIntroPlan(
            mode="create",
            expected_page_id=expected_page_id,
            expected_version=expected_version,
            intro=DEFAULT_INDEX_INTRO,
            model_status="defaulted",
            error_code=error_code,
        )

    index = context.index
    assert index is not None
    return IndexIntroPlan(
        mode="update",
        expected_page_id=expected_page_id,
        expected_version=expected_version,
        intro=index.content,
        model_status="kept_after_error",
        error_code=error_code,
    )


def _normalize_actions(
    operation_actions: Iterable[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    normalized: set[tuple[str, str]] = set()
    for item in operation_actions:
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError("operation action 必须是 (action, knowledge_id)")
        action, knowledge_id = item
        if not isinstance(action, str) or not isinstance(knowledge_id, str):
            raise ValueError("operation action 必须是字符串")
        action = action.strip()
        knowledge_id = knowledge_id.strip()
        if action not in {"ingest", "retract"}:
            raise ValueError("operation action 只能是 ingest 或 retract")
        if not knowledge_id:
            raise ValueError("operation action knowledge_id 不能为空")
        normalized.add((action, knowledge_id))
    return tuple(sorted(normalized))


def _snapshot_page(page: ReducedPage) -> ReducedPage:
    if not isinstance(page, ReducedPage):
        raise ValueError("index intro page 无效")
    return ReducedPage.model_validate(page.model_dump(mode="python", warnings="error"))


def _snapshot_delta(delta: ContributionDelta) -> ContributionDelta:
    if not isinstance(delta, ContributionDelta):
        raise ValueError("index intro contribution delta 无效")
    return ContributionDelta.model_validate(
        delta.model_dump(mode="python", warnings="error")
    )


def _is_create_context(context: IndexIntroContext) -> bool:
    return (
        context.index is None
        or context.index.content.strip() in LEGACY_INDEX_PLACEHOLDERS
    )


def _create_summaries(
    pages: tuple[ReducedPage, ...], recent_summaries: tuple[IndexSummaryItem, ...]
) -> tuple[IndexSummaryItem, ...]:
    items: list[IndexSummaryItem] = []
    items.extend(
        _summary_from_page(page)
        for page in pages
        if page.page_type == "summary" and not page.deleted
    )
    items.extend(recent_summaries)
    return _deduplicate_summaries(items)[:200]


def _final_summary_pages(
    pages: tuple[ReducedPage, ...],
) -> dict[str, IndexSummaryItem]:
    candidates = sorted(
        (
            _summary_from_page(page)
            for page in pages
            if page.page_type == "summary" and not page.deleted
        ),
        key=lambda item: (item.slug, item.title, item.summary),
    )
    final: dict[str, IndexSummaryItem] = {}
    for item in candidates:
        final.setdefault(item.slug, item)
    return final


def _change_pages(
    action: str,
    knowledge_id: str,
    final_pages: dict[str, IndexSummaryItem],
    deltas: tuple[ContributionDelta, ...],
    completed_op_ids: set[UUID],
) -> tuple[IndexSummaryItem, ...]:
    candidates: list[IndexSummaryItem] = []
    for delta in deltas:
        if delta.pending_op_id not in completed_op_ids or delta.knowledge_id != knowledge_id:
            continue
        record = delta.current if action == "ingest" else delta.previous
        if record is None or record.page_type != "summary":
            continue
        candidates.append(final_pages.get(record.slug, _summary_from_record(record)))
    return tuple(
        sorted(_deduplicate_summaries(candidates), key=lambda item: item.slug)
    )


def _summary_from_page(page: ReducedPage) -> IndexSummaryItem:
    return IndexSummaryItem(slug=page.slug, title=page.title, summary=page.summary)


def _summary_from_record(record: StoredContributionRecord) -> IndexSummaryItem:
    return IndexSummaryItem(slug=record.slug, title=record.title, summary=record.summary)


def _deduplicate_summaries(
    items: Iterable[IndexSummaryItem],
) -> tuple[IndexSummaryItem, ...]:
    unique: list[IndexSummaryItem] = []
    seen: set[str] = set()
    for item in items:
        if item.slug not in seen:
            seen.add(item.slug)
            unique.append(item)
    return tuple(unique)


def _expected_index(context: IndexIntroContext) -> tuple[UUID | None, int | None]:
    if context.index is None:
        return None, None
    return context.index.id, context.index.version


def _validate_request_context(
    context: IndexIntroContext, request: IndexIntroRequest,
) -> None:
    if not isinstance(context, IndexIntroContext) or not isinstance(
        request, IndexIntroRequest
    ):
        raise ValueError("index intro context 或 request 无效")
    expected_mode = "create" if _is_create_context(context) else "update"
    if request.mode != expected_mode:
        raise ValueError("index intro request 与 context mode 不一致")
    if request.mode == "update":
        index = context.index
        assert index is not None
        if request.existing_intro != index.content:
            raise ValueError("index intro request 与 context 内容不一致")
