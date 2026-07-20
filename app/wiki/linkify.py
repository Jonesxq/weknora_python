"""Deterministic Wiki-link insertion for Markdown source text."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import re

from app.wiki.domain import WikiSlugError, normalize_slug


_AUTOLINK_RE = re.compile(r"<[A-Za-z][A-Za-z0-9+.-]{1,31}:[^\x00-\x20\x7f<>]*>")
_REFERENCE_DEFINITION_RE = re.compile(r"(?m)^[ \t]{0,3}\[[^\]\r\n]+\]:[^\r\n]*(?:\r\n|\r|\n|$)")
_WIKI_TARGET_RE = r"[^\]|\r\n]+"
_WIKI_DISPLAY_RE = r"[^\]\r\n]*"
_WIKI_LINK_RE = re.compile(
    rf"\[\[({_WIKI_TARGET_RE})(?:\|({_WIKI_DISPLAY_RE}))?\]\]"
)
_WIKI_PROTECTED_MARKUP_RE = re.compile(r"\[\[[^\]\r\n]*\]\]")
_MULTILINE_WIKI_MARKUP_RE = re.compile(r"\[\[[^\]]*[\r\n][^\]]*\]\]")
_ASCII_WORD_RE = re.compile(r"[A-Za-z0-9_]")


@dataclass(frozen=True, slots=True)
class LinkCandidate:
    slug: str
    display: str


@dataclass(frozen=True, slots=True)
class LinkifyResult:
    content: str
    changed: bool
    added_slugs: tuple[str, ...]


class _CandidateTrieNode:
    __slots__ = ("children", "candidate")

    def __init__(self) -> None:
        self.children: dict[str, _CandidateTrieNode] = {}
        self.candidate: LinkCandidate | None = None


def protected_spans(content: str) -> tuple[tuple[int, int], ...]:
    """Return merged source ranges that must remain unchanged."""

    code_spans = _fenced_code_spans(content)
    code_spans.extend(_inline_code_spans(content, code_spans))
    spans = code_spans + _markdown_spans(content)
    return tuple(_merge_spans(spans))


def extract_safe_wiki_links(content: str) -> tuple[str, ...]:
    """Return distinct normalized Wiki targets found in safe body text."""

    return _extract_safe_wiki_links(content, _unsafe_body_spans(content))


def wiki_link_text_projection(content: str) -> str:
    """Project safe, valid Wiki links back to their visible source text."""

    unsafe_body_spans = _unsafe_body_spans(content)
    pieces: list[str] = []
    cursor = 0
    for match in _WIKI_LINK_RE.finditer(content):
        if _overlaps_protected(unsafe_body_spans, match.start(), match.end()):
            continue
        try:
            normalize_slug(match.group(1))
        except WikiSlugError:
            continue
        pieces.append(content[cursor : match.start()])
        pieces.append(match.group(1) if match.group(2) is None else match.group(2))
        cursor = match.end()
    if not pieces:
        return content
    pieces.append(content[cursor:])
    return "".join(pieces)


def _extract_safe_wiki_links(
    content: str,
    unsafe_body_spans: list[tuple[int, int]],
) -> tuple[str, ...]:
    links: list[str] = []
    seen: set[str] = set()
    for match in _WIKI_LINK_RE.finditer(content):
        if _overlaps_protected(unsafe_body_spans, match.start(), match.end()):
            continue
        try:
            slug = normalize_slug(match.group(1))
        except WikiSlugError:
            continue
        if slug in seen:
            continue
        seen.add(slug)
        links.append(slug)
    return tuple(links)


def linkify_markdown(
    content: str,
    *,
    current_slug: str,
    candidates: Iterable[LinkCandidate],
) -> LinkifyResult:
    """Insert one deterministic Wiki link per eligible candidate slug."""

    normalized_current_slug = normalize_slug(current_slug)
    code_spans = _fenced_code_spans(content)
    code_spans.extend(_inline_code_spans(content, code_spans))
    code_spans = _merge_spans(code_spans)
    non_wiki_markdown_spans = _non_wiki_markdown_spans(content)
    wiki_markup_spans = _wiki_markup_spans(content)
    spans = _merge_spans(code_spans + wiki_markup_spans + non_wiki_markdown_spans)
    unsafe_body_spans = _merge_spans(
        code_spans + non_wiki_markdown_spans + _multiline_wiki_markup_spans(content)
    )
    existing_slugs = set(_extract_safe_wiki_links(content, unsafe_body_spans))
    prepared = _prepare_candidates(candidates, normalized_current_slug, existing_slugs)
    candidate_trie = _build_candidate_trie(prepared)
    added_slugs: list[str] = []
    added_slug_set: set[str] = set()
    pieces: list[str] = []
    copy_cursor = 0
    index = 0
    protected_cursor = 0

    while index < len(content):
        while protected_cursor < len(spans) and spans[protected_cursor][1] <= index:
            protected_cursor += 1
        if protected_cursor < len(spans) and spans[protected_cursor][0] <= index:
            index = spans[protected_cursor][1]
            protected_cursor += 1
            continue

        match = _candidate_at(content, index, candidate_trie, added_slug_set, spans)
        if match is None:
            index += 1
            continue

        candidate, end = match
        replacement = f"[[{candidate.slug}|{candidate.display}]]"
        pieces.append(content[copy_cursor:index])
        pieces.append(replacement)
        copy_cursor = end
        added_slugs.append(candidate.slug)
        added_slug_set.add(candidate.slug)
        index = end

    if not added_slugs:
        return LinkifyResult(content, False, ())
    pieces.append(content[copy_cursor:])
    return LinkifyResult("".join(pieces), True, tuple(added_slugs))


def _prepare_candidates(
    candidates: Iterable[LinkCandidate],
    current_slug: str,
    existing_slugs: set[str],
) -> tuple[LinkCandidate, ...]:
    by_display: dict[str, set[str]] = {}
    pairs: set[tuple[str, str]] = set()

    for candidate in candidates:
        try:
            slug = normalize_slug(candidate.slug)
            display = candidate.display.strip()
        except (TypeError, AttributeError, ValueError):
            continue
        if not display or any(character in display for character in "]\r\n"):
            continue
        pairs.add((slug, display))
        by_display.setdefault(display, set()).add(slug)

    filtered = [
        LinkCandidate(slug=slug, display=display)
        for slug, display in pairs
        if (
            len(by_display[display]) == 1
            and slug != current_slug
            and slug not in existing_slugs
        )
    ]
    return tuple(sorted(filtered, key=lambda item: (-len(item.display), item.display, item.slug)))


def _build_candidate_trie(candidates: tuple[LinkCandidate, ...]) -> _CandidateTrieNode:
    root = _CandidateTrieNode()
    for candidate in candidates:
        node = root
        for character in candidate.display:
            child = node.children.get(character)
            if child is None:
                child = _CandidateTrieNode()
                node.children[character] = child
            node = child
        node.candidate = candidate
    return root


def _candidate_at(
    content: str,
    index: int,
    trie: _CandidateTrieNode,
    added_slugs: set[str],
    protected: list[tuple[int, int]],
) -> tuple[LinkCandidate, int] | None:
    node = trie
    matches: list[tuple[LinkCandidate, int]] = []
    cursor = index
    while cursor < len(content):
        child = node.children.get(content[cursor])
        if child is None:
            break
        node = child
        cursor += 1
        if node.candidate is not None:
            matches.append((node.candidate, cursor))

    if not matches or _is_escaped(content, index):
        return None
    for candidate, end in reversed(matches):
        if candidate.slug in added_slugs:
            continue
        if (
            _overlaps_protected(protected, index, end)
            or not _has_valid_boundary(content, index, end, candidate.display)
        ):
            continue
        return candidate, end
    return None


def _has_valid_boundary(content: str, start: int, end: int, display: str) -> bool:
    if not display.isascii():
        return True
    return (
        (start == 0 or _ASCII_WORD_RE.fullmatch(content[start - 1]) is None)
        and (end == len(content) or _ASCII_WORD_RE.fullmatch(content[end]) is None)
    )


def _is_escaped(content: str, index: int) -> bool:
    slash_count = 0
    cursor = index - 1
    while cursor >= 0 and content[cursor] == "\\":
        slash_count += 1
        cursor -= 1
    return slash_count % 2 == 1


def _unsafe_body_spans(content: str) -> list[tuple[int, int]]:
    code_spans = _fenced_code_spans(content)
    code_spans.extend(_inline_code_spans(content, code_spans))
    return _merge_spans(
        code_spans + _non_wiki_markdown_spans(content) + _multiline_wiki_markup_spans(content)
    )


def _fenced_code_spans(content: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    line_start = 0
    while line_start < len(content):
        line_end = _line_end(content, line_start)
        opener = _fence_opener(content[line_start:line_end])
        if opener is None:
            line_start = line_end
            continue

        marker, run_length = opener
        cursor = line_end
        while cursor < len(content):
            closing_end = _fence_closing_end(content, cursor, marker, run_length)
            if closing_end is not None:
                spans.append((line_start, closing_end))
                line_start = closing_end
                break
            cursor = _line_end(content, cursor)
        else:
            spans.append((line_start, len(content)))
            return spans
    return spans


def _fence_opener(line: str) -> tuple[str, int] | None:
    stripped = line.rstrip("\r\n")
    prefix_length = len(stripped) - len(stripped.lstrip(" "))
    if prefix_length > 3 or prefix_length == len(stripped):
        return None
    marker = stripped[prefix_length]
    if marker not in {"`", "~"}:
        return None
    run_length = 0
    while prefix_length + run_length < len(stripped) and stripped[prefix_length + run_length] == marker:
        run_length += 1
    if run_length < 3:
        return None
    return marker, run_length


def _fence_closing_end(content: str, line_start: int, marker: str, minimum_length: int) -> int | None:
    line_end = _line_end(content, line_start)
    line = content[line_start:line_end].rstrip("\r\n")
    prefix_length = len(line) - len(line.lstrip(" "))
    if prefix_length > 3:
        return None
    run_length = 0
    while prefix_length + run_length < len(line) and line[prefix_length + run_length] == marker:
        run_length += 1
    if run_length < minimum_length or line[prefix_length + run_length :].strip(" \t"):
        return None
    return line_end


def _inline_code_spans(content: str, fenced_spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    index = 0
    fenced_cursor = 0
    while index < len(content):
        while fenced_cursor < len(fenced_spans) and fenced_spans[fenced_cursor][1] <= index:
            fenced_cursor += 1
        if fenced_cursor < len(fenced_spans) and fenced_spans[fenced_cursor][0] <= index:
            index = fenced_spans[fenced_cursor][1]
            fenced_cursor += 1
            continue
        if content[index] != "`":
            index += 1
            continue

        opener_end = _run_end(content, index, "`")
        run_length = opener_end - index
        cursor = opener_end
        closer_fenced_cursor = fenced_cursor
        while cursor < len(content):
            while (
                closer_fenced_cursor < len(fenced_spans)
                and fenced_spans[closer_fenced_cursor][1] <= cursor
            ):
                closer_fenced_cursor += 1
            if (
                closer_fenced_cursor < len(fenced_spans)
                and fenced_spans[closer_fenced_cursor][0] <= cursor
            ):
                cursor = fenced_spans[closer_fenced_cursor][1]
                closer_fenced_cursor += 1
                continue
            if content[cursor] != "`":
                cursor += 1
                continue
            closer_end = _run_end(content, cursor, "`")
            if closer_end - cursor == run_length:
                spans.append((index, closer_end))
                index = closer_end
                fenced_cursor = closer_fenced_cursor
                break
            cursor = closer_end
        else:
            spans.append((index, len(content)))
            return spans
    return spans


def _markdown_spans(content: str) -> list[tuple[int, int]]:
    spans = _wiki_markup_spans(content)
    spans.extend(_non_wiki_markdown_spans(content))
    return spans


def _wiki_markup_spans(content: str) -> list[tuple[int, int]]:
    spans = [(match.start(), match.end()) for match in _WIKI_PROTECTED_MARKUP_RE.finditer(content)]
    spans.extend(_multiline_wiki_markup_spans(content))
    return spans


def _multiline_wiki_markup_spans(content: str) -> list[tuple[int, int]]:
    return [(match.start(), match.end()) for match in _MULTILINE_WIKI_MARKUP_RE.finditer(content)]


def _non_wiki_markdown_spans(content: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    spans.extend((match.start(), match.end()) for match in _REFERENCE_DEFINITION_RE.finditer(content))
    spans.extend((match.start(), match.end()) for match in _AUTOLINK_RE.finditer(content))
    spans.extend(_markdown_link_spans(content))
    return spans


def _markdown_link_spans(content: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    bracket_pairs = _delimiter_pairs(content, "[", "]")
    parenthesis_pairs = _delimiter_pairs(content, "(", ")")
    index = 0
    while index < len(content):
        opening = content.find("[", index)
        if opening == -1:
            break
        closing = bracket_pairs.get(opening)
        if closing is None:
            index = opening + 1
            continue
        start = opening - 1 if opening > 0 and content[opening - 1] == "!" else opening
        next_index = closing + 1
        if next_index < len(content) and content[next_index] == "(":
            end = parenthesis_pairs.get(next_index)
            if end is not None:
                spans.append((start, end + 1))
                index = end + 1
                continue
        if next_index < len(content) and content[next_index] == "[":
            end = bracket_pairs.get(next_index)
            if end is not None:
                spans.append((start, end + 1))
                index = end + 1
                continue
        index = closing + 1
    return spans


def _delimiter_pairs(content: str, opening: str, closing: str) -> dict[int, int]:
    openings: list[int] = []
    pairs: dict[int, int] = {}
    for index, character in enumerate(content):
        if character == opening and not _is_escaped(content, index):
            openings.append(index)
        elif character == closing and openings and not _is_escaped(content, index):
            pairs[openings.pop()] = index
    return pairs


def _line_end(content: str, start: int) -> int:
    index = start
    while index < len(content) and content[index] not in "\r\n":
        index += 1
    if index < len(content) and content[index] == "\r" and index + 1 < len(content) and content[index + 1] == "\n":
        return index + 2
    return index + 1 if index < len(content) else index


def _run_end(content: str, start: int, character: str) -> int:
    index = start
    while index < len(content) and content[index] == character:
        index += 1
    return index


def _protected_end(spans: list[tuple[int, int]], index: int) -> int | None:
    for start, end in spans:
        if index < start:
            return None
        if start <= index < end:
            return end
    return None


def _overlaps_protected(spans: list[tuple[int, int]], start: int, end: int) -> bool:
    cursor = _first_span_ending_after(spans, start)
    return cursor < len(spans) and spans[cursor][0] < end


def _first_span_ending_after(spans: list[tuple[int, int]], index: int) -> int:
    low = 0
    high = len(spans)
    while low < high:
        middle = (low + high) // 2
        if spans[middle][1] <= index:
            low = middle + 1
        else:
            high = middle
    return low


def _merge_spans(spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted((start, end) for start, end in spans if start < end):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged
