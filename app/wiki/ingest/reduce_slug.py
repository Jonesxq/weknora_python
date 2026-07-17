"""将同一 slug 的 Map 结果确定性合并为一个待写入页面。"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from typing import NoReturn
from uuid import UUID

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.ports import ChatModelPort
from app.wiki.ingest.schemas import (
    PageContribution,
    PageMergeRequest,
    ReducedPage,
    SlugUpdate,
)


_TOPIC_TYPES = {"entity", "concept"}
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


def _validate_inputs(
    slug: str,
    updates: Sequence[SlugUpdate],
    existing_page: ReducedPage | None,
) -> str:
    if not updates:
        _reject("WIKI_REDUCE_EMPTY", "同一 slug 的 updates 不能为空")

    if len(slug) > 255 or not _SLUG_PATTERN.fullmatch(slug):
        _reject(
            "WIKI_REDUCE_INVALID_SLUG",
            "slug 必须是 canonical 小写分层路径，且长度不能超过 255",
        )
    prefix = slug.partition("/")[0]

    for update in updates:
        if update.slug != slug:
            _reject(
                "WIKI_REDUCE_SLUG_MISMATCH",
                f"update slug {update.slug!r} 与目标 slug {slug!r} 不一致",
            )
        if update.page_type != prefix:
            _reject(
                "WIKI_REDUCE_TYPE_MISMATCH",
                "update 页面类型必须与 slug 前缀一致",
            )

    page_types = {update.page_type for update in updates}
    if len(page_types) != 1:
        _reject(
            "WIKI_REDUCE_MIXED_TYPES",
            "同一 slug 的 updates 页面类型必须一致",
        )

    if existing_page is not None and (
        existing_page.slug != slug or existing_page.page_type != prefix
    ):
        _reject(
            "WIKI_REDUCE_EXISTING_MISMATCH",
            "已有页面的 slug 和页面类型必须与目标页面一致",
        )
    return prefix


def _reduce_summary(update: SlugUpdate) -> ReducedPage:
    """摘要页代表单份来源文档，因此始终执行整页替换。"""

    return ReducedPage(
        slug=update.slug,
        title=update.title,
        page_type="summary",
        content=update.content,
        summary=update.summary,
        aliases=[],
        source_refs=[update.knowledge_id],
        chunk_refs=[],
        contributor_op_ids=[update.pending_op_id],
    )


def _contribution(update: SlugUpdate) -> PageContribution:
    return PageContribution(
        pending_op_id=update.pending_op_id,
        knowledge_id=update.knowledge_id,
        title=update.title,
        content=update.content,
        summary=update.summary,
        aliases=_stable_clean(update.aliases),
        source_refs=_stable_clean(update.source_refs),
        chunk_refs=_stable_clean(update.chunk_refs),
    )


async def reduce_slug(
    slug: str,
    updates: Sequence[SlugUpdate],
    existing_page: ReducedPage | None,
    model: ChatModelPort,
) -> ReducedPage:
    """按调用方给定的稳定顺序合并一个 slug 的全部更新。

    Summary 路径不调用模型并整页替换；entity/concept 路径只调用一次
    ``merge_page``。本批元数据始终排在已有页面元数据之前。
    """

    page_type = _validate_inputs(slug, updates, existing_page)
    if page_type == "summary":
        if len(updates) != 1:
            _reject(
                "WIKI_REDUCE_SUMMARY_COUNT",
                "summary 页面每次必须恰好一个 update",
            )
        if slug != f"summary/{updates[0].knowledge_id}":
            _reject(
                "WIKI_REDUCE_SUMMARY_IDENTITY_MISMATCH",
                "summary slug 必须与 update knowledge_id 完全一致",
            )
        return _reduce_summary(updates[0])

    if page_type not in _TOPIC_TYPES:
        _reject("WIKI_REDUCE_INVALID_TYPE", "仅支持 summary、entity 和 concept 类型")

    existing_aliases = existing_page.aliases if existing_page is not None else []
    existing_source_refs = (
        existing_page.source_refs if existing_page is not None else []
    )
    existing_chunk_refs = existing_page.chunk_refs if existing_page is not None else []

    aliases = _stable_clean(
        value
        for update in updates
        for value in update.aliases
    )
    aliases = _stable_clean([*aliases, *existing_aliases])
    source_refs = _stable_clean(
        [
            *(value for update in updates for value in update.source_refs),
            *existing_source_refs,
        ]
    )
    chunk_refs = _stable_clean(
        [
            *(value for update in updates for value in update.chunk_refs),
            *existing_chunk_refs,
        ]
    )

    request = PageMergeRequest(
        slug=slug,
        title=updates[0].title,
        page_type=page_type,
        aliases=aliases,
        existing_content=existing_page.content if existing_page is not None else "",
        existing_summary=existing_page.summary if existing_page is not None else "",
        contributions=[_contribution(update) for update in updates],
    )
    output = await model.merge_page(request)

    summary_parts = [update.summary for update in updates]
    if existing_page is not None:
        summary_parts.append(existing_page.summary)

    return ReducedPage(
        slug=slug,
        title=output.headline,
        page_type=page_type,
        content=output.markdown,
        summary="\n\n".join(_stable_clean(summary_parts)),
        aliases=aliases,
        source_refs=source_refs,
        chunk_refs=chunk_refs,
        contributor_op_ids=_stable_unique_ids(
            update.pending_op_id for update in updates
        ),
    )
