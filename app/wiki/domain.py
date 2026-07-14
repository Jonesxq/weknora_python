"""Wiki 的确定性领域规则。"""

from __future__ import annotations

import re

_WIKI_LINK_RE = re.compile(r"\[\[([^\]|]+)(?:\|[^\]]*)?\]\]")
_WHITESPACE_RE = re.compile(r"\s+")
_HYPHEN_RE = re.compile(r"-+")


class WikiSlugError(ValueError):
    """slug 为空或包含不安全字符。"""


def normalize_slug(value: str) -> str:
    """规范化 Wiki slug，同时保留由斜杠分隔的类型前缀。"""

    raw = value.strip().casefold()
    if not raw or len(raw) > 255:
        raise WikiSlugError("Wiki slug 不能为空且长度不能超过 255")

    normalized_parts: list[str] = []
    for raw_part in raw.split("/"):
        part = _HYPHEN_RE.sub("-", _WHITESPACE_RE.sub("-", raw_part)).strip("-")
        if not part or not all(character.isalnum() or character in {"-", "_"} for character in part):
            raise WikiSlugError(f"Wiki slug 包含不安全片段: {raw_part!r}")
        normalized_parts.append(part)
    return "/".join(normalized_parts)


def extract_wiki_links(content: str) -> list[str]:
    """按正文出现顺序解析并去重合法 Wiki 链接目标。"""

    links: list[str] = []
    seen: set[str] = set()
    for match in _WIKI_LINK_RE.finditer(content):
        try:
            slug = normalize_slug(match.group(1))
        except WikiSlugError:
            continue
        if slug not in seen:
            seen.add(slug)
            links.append(slug)
    return links


def normalize_category_path(values: list[str], *, max_depth: int = 3) -> list[str]:
    """清洗目录缓存路径，保序去重并限制存储层最大深度。"""

    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        name = value.strip()
        if not name or name in seen:
            continue
        seen.add(name)
        result.append(name)
        if len(result) == max_depth:
            break
    return result


def calculate_health_score(
    total_pages: int,
    orphan_pages: int,
    broken_links: int,
    empty_pages: int,
    total_links: int,
) -> int:
    """按兼容规则计算 0 至 100 的 Wiki 健康分。"""

    score = 100
    if total_pages > 0:
        score -= round(min(orphan_pages, total_pages) / total_pages * 20)
    score -= max(broken_links, 0) * 5
    score -= max(empty_pages, 0) * 3
    if total_pages > 2 and total_links == 0:
        score -= 15
    return max(0, min(100, score))
