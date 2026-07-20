import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
_TERMINAL_LANGUAGES = frozenset({"powershell", "pwsh", "bash", "sh", "shell"})


def _read_project_text(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")


def _terminal_fence_blocks(text: str) -> list[str]:
    lines = text.splitlines(keepends=True)
    blocks: list[str] = []
    index = 0
    while index < len(lines):
        opener = _fence_opener(lines[index])
        if opener is None:
            index += 1
            continue

        marker, marker_length, language = opener
        body_start = index + 1
        index = body_start
        while index < len(lines) and not _is_fence_closer(
            lines[index], marker, marker_length
        ):
            index += 1
        if language in _TERMINAL_LANGUAGES:
            blocks.append("".join(lines[body_start:index]))
        if index < len(lines):
            index += 1
    return blocks


def _fence_opener(line: str) -> tuple[str, int, str] | None:
    content = line.rstrip("\r\n")
    indent = len(content) - len(content.lstrip(" "))
    if indent > 3 or indent == len(content):
        return None
    marker = content[indent]
    if marker not in {"`", "~"}:
        return None
    marker_end = indent
    while marker_end < len(content) and content[marker_end] == marker:
        marker_end += 1
    marker_length = marker_end - indent
    if marker_length < 3:
        return None
    info = content[marker_end:].strip()
    language = info.split(maxsplit=1)[0].casefold() if info else ""
    return marker, marker_length, language


def _is_fence_closer(line: str, marker: str, minimum_length: int) -> bool:
    content = line.rstrip("\r\n")
    indent = len(content) - len(content.lstrip(" "))
    if indent > 3:
        return False
    marker_end = indent
    while marker_end < len(content) and content[marker_end] == marker:
        marker_end += 1
    return (
        marker_end - indent >= minimum_length
        and not content[marker_end:].strip(" \t")
    )


def _continues_terminal_command(line: str) -> bool:
    stripped = line.rstrip()
    return stripped.endswith(("`", "\\", "|", "&&", "||"))


def _assert_terminal_commands_have_chinese_comments(text: str) -> None:
    blocks = _terminal_fence_blocks(text)
    assert blocks
    for block in blocks:
        previous = ""
        continuing = False
        for raw_line in block.splitlines():
            line = raw_line.strip()
            if not line:
                previous = ""
                continuing = False
                continue
            if line.startswith("#"):
                previous = line
                continue
            if continuing:
                continuing = _continues_terminal_command(line)
                continue
            has_chinese_comment = previous.startswith("#") and re.search(
                r"[\u4e00-\u9fff]", previous
            )
            assert has_chinese_comment, f"命令缺少中文用途注释: {line}"
            previous = ""
            continuing = _continues_terminal_command(line)


@pytest.mark.parametrize(
    ("indent", "marker", "language"),
    [
        ("   ", "~~~", "pwsh"),
        ("  ", "```", "BASH"),
        (" ", "~~~", "sh"),
        ("   ", "```", "shell"),
        ("", "~~~", "PowerShell"),
    ],
)
def test_terminal_fence_scanner_recognizes_markers_indentation_and_aliases(
    indent: str, marker: str, language: str
):
    body = f"{indent}# 运行示例命令\n{indent}echo ok\n"
    markdown = f"{indent}{marker}{language}\n{body}{indent}{marker}\n"

    assert _terminal_fence_blocks(markdown) == [body]


def test_terminal_comment_contract_checks_every_recognized_block():
    markdown = (
        "```powershell\n"
        "# 运行合法命令\n"
        "Write-Output ok\n"
        "```\n"
        "   ~~~sh\n"
        "# run undocumented command\n"
        "echo missing-comment\n"
        "   ~~~\n"
    )

    with pytest.raises(AssertionError, match="命令缺少中文用途注释"):
        _assert_terminal_commands_have_chinese_comments(markdown)


def test_terminal_fence_scanner_ignores_non_terminal_output_blocks():
    markdown = (
        "13 passed in 0.09s\n"
        "```text\n"
        "command output without comments\n"
        "```\n"
        "~~~json\n"
        '{"status": "ok"}\n'
        "~~~\n"
    )

    assert _terminal_fence_blocks(markdown) == []


def test_terminal_comment_contract_allows_shell_and_powershell_continuations():
    markdown = (
        "```pwsh\n"
        "# 运行多行 PowerShell 命令\n"
        "uv run pytest `\n"
        "  tests/wiki/test_linkify.py `\n"
        "  -q\n"
        "```\n"
        "~~~bash\n"
        "# 运行多行 shell 命令\n"
        "printf '%s' \\\n"
        "  value\n"
        "~~~\n"
    )

    _assert_terminal_commands_have_chinese_comments(markdown)


def test_phase_four_b_doc_describes_implemented_contracts():
    text = _read_project_text("docs/Wiki阶段四B.md")
    for phrase in (
        "受保护 Markdown 区域",
        "本批受影响页面",
        "每个 slug 只链接一次",
        "strict fake",
        "最多 200 条",
        "stale_skipped",
        "自动链接不额外增加页面版本",
        "活动、published",
        "同一事务",
    ):
        assert phrase in text


def test_phase_four_b_doc_limits_reference_link_protection():
    text = _read_project_text("docs/Wiki阶段四B.md")
    assert "full `[text][label]`" in text
    assert "collapsed `[text][]`" in text
    assert "shortcut `[text]` 当前不在保护范围" in text
    assert "图片和 reference-style 链接" not in text


def test_phase_four_b_doc_does_not_claim_later_scope():
    text = _read_project_text("docs/Wiki阶段四B.md")
    for forbidden in (
        "全库自动补链已实现",
        "broken-link 自动清理已实现",
        "auto-fix 已实现",
        "Agent 已实现",
        "WikiPageIndexer 已实现",
        "真实 Index 模型已实现",
    ):
        assert forbidden not in text


def test_phase_four_b_adds_no_migration_or_rest_contract_change():
    versions = sorted((PROJECT_ROOT / "migrations/versions").glob("*.py"))
    assert versions[-1].name == "20260719_04_add_wiki_log_result_outcome.py"
    design = _read_project_text(
        "docs/superpowers/specs/2026-07-20-wiki-phase-4b-design.md"
    )
    assert "REST DTO 不增加字段" in design


def test_phase_four_b_doc_commands_have_chinese_purpose_comments():
    text = _read_project_text("docs/Wiki阶段四B.md")
    _assert_terminal_commands_have_chinese_comments(text)

    assert (
        "uv run pytest tests/wiki/test_linkify.py "
        "tests/wiki/test_ingest_index_intro.py tests/wiki/test_ingest_worker.py -q"
        in text
    )
    assert "uv run python -m app.wiki.tasks.enqueue_fake --op ingest" in text


def test_readme_links_phase_four_b_and_summarizes_current_scope():
    text = _read_project_text("README.md")
    assert "[Wiki 阶段四 B](docs/Wiki阶段四B.md)" in text
    assert "阶段四 B 增加确定性自动交叉链接" in text
    assert "canonical Index" in text
    assert "活动、published、已解析边" in text
    assert (
        "当前服务包含项目内文档解析 API，以及 Wiki 阶段一 REST API 和阶段二 fake 摄取运行链路。"
        not in text
    )
