from __future__ import annotations

import pytest

from app.wiki.domain import (
    WikiSlugError,
    calculate_health_score,
    extract_wiki_links,
    normalize_category_path,
    normalize_slug,
)


def test_normalize_slug_preserves_type_prefix_and_normalizes_spaces() -> None:
    assert normalize_slug(" Entity/Acme Corp ") == "entity/acme-corp"
    assert normalize_slug("concept/知识 图谱") == "concept/知识-图谱"


@pytest.mark.parametrize("slug", ["", "/entity", "entity/", "entity/acme?x=1", "../acme"])
def test_normalize_slug_rejects_unsafe_values(slug: str) -> None:
    with pytest.raises(WikiSlugError):
        normalize_slug(slug)


def test_extract_wiki_links_supports_labels_and_stable_deduplication() -> None:
    content = (
        "关联 [[Entity/Acme Corp|Acme 公司]] 和 [[concept/知识 图谱]]，"
        "再次出现 [[entity/acme-corp]]。"
    )

    assert extract_wiki_links(content) == ["entity/acme-corp", "concept/知识-图谱"]


def test_extract_wiki_links_ignores_malformed_targets() -> None:
    assert extract_wiki_links("[[entity/ok]] [[../bad]] [[entity/good|显示]]") == [
        "entity/ok",
        "entity/good",
    ]


def test_extract_wiki_links_uses_safe_body_markup_and_returns_a_list() -> None:
    content = (
        "`[[concept/code]]` [[concept/real|真实]]\n"
        "```md\n"
        "[[entity/fenced]]\n"
        "```\n"
        "[[concept/real]] [[entity/acme]]"
    )

    links = extract_wiki_links(content)

    assert isinstance(links, list)
    assert links == ["concept/real", "entity/acme"]


def test_extract_wiki_links_skips_invalid_targets_without_losing_later_safe_links() -> None:
    assert extract_wiki_links("[[not valid]] [[concept/python]] [[../escape]] [[entity/acme]]") == [
        "concept/python",
        "entity/acme",
    ]


def test_normalize_category_path_removes_empty_duplicate_and_extra_levels() -> None:
    assert normalize_category_path([" 技术 ", "", "技术", " AI ", "模型", "多余"]) == [
        "技术",
        "AI",
        "模型",
    ]


def test_health_score_applies_documented_penalties_and_bounds() -> None:
    assert calculate_health_score(
        total_pages=10,
        orphan_pages=5,
        broken_links=2,
        empty_pages=1,
        total_links=3,
    ) == 77
    assert calculate_health_score(
        total_pages=3,
        orphan_pages=3,
        broken_links=20,
        empty_pages=3,
        total_links=0,
    ) == 0
    assert calculate_health_score(0, 0, 0, 0, 0) == 100
