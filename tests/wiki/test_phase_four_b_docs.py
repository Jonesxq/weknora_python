import re
from pathlib import Path


def test_phase_four_b_doc_describes_implemented_contracts():
    text = Path("docs/Wiki阶段四B.md").read_text(encoding="utf-8")
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


def test_phase_four_b_doc_does_not_claim_later_scope():
    text = Path("docs/Wiki阶段四B.md").read_text(encoding="utf-8")
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
    versions = sorted(Path("migrations/versions").glob("*.py"))
    assert versions[-1].name == "20260719_04_add_wiki_log_result_outcome.py"
    design = Path(
        "docs/superpowers/specs/2026-07-20-wiki-phase-4b-design.md"
    ).read_text(encoding="utf-8")
    assert "REST DTO 不增加字段" in design


def test_phase_four_b_doc_commands_have_chinese_purpose_comments():
    text = Path("docs/Wiki阶段四B.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```powershell\n(.*?)```", text, flags=re.DOTALL)
    assert blocks
    for block in blocks:
        previous = ""
        for raw_line in block.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("#"):
                previous = line
                continue
            assert previous.startswith("#"), f"命令缺少中文用途注释: {line}"
            assert re.search(r"[\u4e00-\u9fff]", previous), previous
            previous = ""

    assert (
        "uv run pytest tests/wiki/test_linkify.py "
        "tests/wiki/test_ingest_index_intro.py tests/wiki/test_ingest_worker.py -q"
        in text
    )
    assert "uv run python -m app.wiki.tasks.enqueue_fake --op ingest" in text


def test_readme_links_phase_four_b_and_summarizes_current_scope():
    text = Path("README.md").read_text(encoding="utf-8")
    assert "[Wiki 阶段四 B](docs/Wiki阶段四B.md)" in text
    assert "阶段四 B 增加确定性自动交叉链接" in text
    assert "canonical Index" in text
    assert "活动、published、已解析边" in text
    assert (
        "当前服务包含项目内文档解析 API，以及 Wiki 阶段一 REST API 和阶段二 fake 摄取运行链路。"
        not in text
    )
