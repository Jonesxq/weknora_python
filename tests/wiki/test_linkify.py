from __future__ import annotations

import importlib

import pytest

from app.wiki.domain import WikiSlugError


def _api():
    try:
        return importlib.import_module("app.wiki.linkify")
    except ModuleNotFoundError:
        pytest.fail("缺少 app.wiki.linkify 模块")


def _candidate(slug: str, display: str):
    return _api().LinkCandidate(slug=slug, display=display)


def _linkify(content: str, *, current_slug: str, candidates):
    return _api().linkify_markdown(content, current_slug=current_slug, candidates=candidates)


def test_linkify_prefers_longer_display_and_adds_each_slug_once() -> None:
    result = _linkify(
        "机器学习依赖学习。机器学习再次出现。",
        current_slug="concept/overview",
        candidates=(
            _candidate("concept/learning", "学习"),
            _candidate("concept/machine-learning", "机器学习"),
        ),
    )

    assert result.content == "[[concept/machine-learning|机器学习]]依赖[[concept/learning|学习]]。机器学习再次出现。"
    assert result.changed is True
    assert result.added_slugs == ("concept/machine-learning", "concept/learning")


def test_ascii_boundary_and_case_rules_are_deterministic() -> None:
    result = _linkify(
        "AI TRAINING_AI AI2 ai，人工智能，人工智能。",
        current_slug="concept/overview",
        candidates=(
            _candidate("concept/ai", "AI"),
            _candidate("concept/artificial-intelligence", "人工智能"),
        ),
    )

    assert result.content == "[[concept/ai|AI]] TRAINING_AI AI2 ai，[[concept/artificial-intelligence|人工智能]]，人工智能。"
    assert result.added_slugs == ("concept/ai", "concept/artificial-intelligence")


def test_protects_code_and_links_but_links_later_safe_text() -> None:
    content = (
        "`AI`\n"
        "```python\n"
        "AI\n"
        "```\n"
        "[AI](https://example.test) ![AI](image.png) [AI][ref] <https://AI.test>\n"
        "[ref]: https://example.test/AI\n"
        "AI"
    )

    result = _linkify(
        content,
        current_slug="concept/overview",
        candidates=(_candidate("concept/ai", "AI"),),
    )

    assert result.content == content[:-2] + "[[concept/ai|AI]]"
    assert result.added_slugs == ("concept/ai",)


def test_protects_nested_markdown_link_label_and_parentheses() -> None:
    content = "[AI [note]](https://example.test/a_(b)) AI"

    result = _linkify(
        content,
        current_slug="concept/overview",
        candidates=(_candidate("concept/ai", "AI"),),
    )

    assert result.content == content[:-2] + "[[concept/ai|AI]]"
    assert result.added_slugs == ("concept/ai",)


def test_existing_wiki_link_suppresses_all_candidates_for_its_slug() -> None:
    result = _linkify(
        "[[concept/ai|AI]] and AI",
        current_slug="concept/overview",
        candidates=(_candidate("concept/ai", "AI"),),
    )

    assert result.content == "[[concept/ai|AI]] and AI"
    assert result.changed is False
    assert result.added_slugs == ()


def test_wiki_link_inside_code_does_not_suppress_safe_text() -> None:
    content = "`[[concept/ai|AI]]`\n```\nignored\n```\nAI"

    result = _linkify(
        content,
        current_slug="concept/overview",
        candidates=(_candidate("concept/ai", "AI"),),
    )

    assert result.content == content[:-2] + "[[concept/ai|AI]]"
    assert result.added_slugs == ("concept/ai",)


def test_ignores_ambiguous_duplicate_self_empty_and_invalid_candidates() -> None:
    result = _linkify(
        "Acme Python Python Current Empty Bad",
        current_slug="concept/current",
        candidates=(
            _candidate("entity/acme", "Acme"),
            _candidate("company/acme", "Acme"),
            _candidate("lang/python", "Python"),
            _candidate("lang/python", "Python"),
            _candidate("concept/current", "Current"),
            _candidate("concept/empty", ""),
            _candidate("../bad", "Bad"),
        ),
    )

    assert result.content == "Acme [[lang/python|Python]] Python Current Empty Bad"
    assert result.added_slugs == ("lang/python",)


def test_escaped_match_is_skipped_and_later_match_is_linked() -> None:
    result = _linkify(
        r"\AI AI",
        current_slug="concept/overview",
        candidates=(_candidate("concept/ai", "AI"),),
    )

    assert result.content == r"\AI [[concept/ai|AI]]"
    assert result.added_slugs == ("concept/ai",)


def test_protected_spans_are_merged_and_cover_unclosed_fences() -> None:
    module = _api()
    content = "before\n  ~~~python\nAI\n"

    assert module.protected_spans(content) == ((7, len(content)),)


def test_unclosed_inline_code_preserves_crlf_and_is_idempotent() -> None:
    content = "AI\r\n`AI\r\nAI"
    candidates = (_candidate("concept/ai", "AI"),)

    first = _linkify(content, current_slug="concept/overview", candidates=candidates)
    second = _linkify(first.content, current_slug="concept/overview", candidates=candidates)

    assert first.content == "[[concept/ai|AI]]\r\n`AI\r\nAI"
    assert first.added_slugs == ("concept/ai",)
    assert second == _api().LinkifyResult(first.content, False, ())


def test_invalid_current_slug_is_not_silenced() -> None:
    with pytest.raises(WikiSlugError):
        _linkify("AI", current_slug="../bad", candidates=())
