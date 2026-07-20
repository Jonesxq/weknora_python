"""Wiki 阶段四 A 运行手册与配置合同。"""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MANUAL_PATH = "docs/Wiki阶段四A.md"
CHINESE_RE = re.compile(r"[\u4e00-\u9fff]")


def _read(path: str) -> str:
    target = ROOT / path
    return target.read_text(encoding="utf-8") if target.exists() else ""


def _powershell_commands(markdown: str) -> list[tuple[str, str]]:
    blocks = re.findall(r"(?ms)^```powershell\s*\n(.*?)^```\s*$", markdown)
    commands: list[tuple[str, str]] = []
    for block in blocks:
        previous = ""
        for raw_line in block.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("#"):
                previous = line
                continue
            commands.append((previous, line))
            previous = line
    return commands


def test_phase_four_a_manual_exists_and_is_chinese() -> None:
    manual = _read(MANUAL_PATH)

    assert manual, "缺少 Wiki 阶段四 A 中文运行手册"
    assert CHINESE_RE.search(manual)


def test_phase_four_a_env_defaults_and_migration_head_are_explicit() -> None:
    env_lines = set(_read(".env.example").splitlines())
    expected_defaults = {
        "GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE=60",
        "GRAPH_WIKI_TAXONOMY_PARALLEL=4",
        "GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT=120",
        "GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT=40",
    }
    assert expected_defaults <= env_lines

    versions = sorted((ROOT / "migrations" / "versions").glob("*.py"))
    assert versions
    assert versions[-1].name == "20260719_04_add_wiki_log_result_outcome.py"

    manual = _read(MANUAL_PATH)
    assert "20260719_04_add_wiki_log_result_outcome.py" in manual
    assert "阶段四 A 不新增迁移" in manual


def test_phase_four_a_manual_describes_current_taxonomy_contract() -> None:
    manual = _read(MANUAL_PATH)
    required_terms = (
        "批次 taxonomy",
        "fake embedding",
        "真正新页面",
        "人工目录",
        "失败隔离",
        "同一事务",
        "model_responses.embeddings",
        "model_responses.taxonomies",
        "稳定 batch key",
        "full catalog limit",
        "embedding top-K",
        "祖先补齐",
        "60",
        "默认并发数是 4",
        "TransientModelError",
        "最多调用 3 次",
        "等待 2 秒",
        "等待 4 秒",
        "contributors",
        "fail_count",
        "dead-letter",
        "summary",
        "entity",
        "concept",
        "软删除历史恢复",
        "base snapshot",
        "释放 claim",
        "legacy root",
        "folder_id",
        "category_path",
        "wiki_path",
        "depth",
        "version",
        "scope advisory lock",
        "同级唯一",
        "幂等",
        "pending",
        "outbox",
    )
    missing = [term for term in required_terms if term not in manual]
    assert not missing, f"阶段四 A 手册缺少当前实现合同: {missing}"

    assert "小目录" in manual and "全量活动目录" in manual
    assert "大目录" in manual and "一级目录" in manual
    assert all(term in manual for term in ("页面", "贡献", "链接", "log"))


def test_phase_four_a_manual_does_not_claim_unimplemented_capabilities() -> None:
    manual = _read(MANUAL_PATH)
    limitation_terms = (
        "只有 fake 上游",
        "不提供自动交叉链接",
        "不提供完整 Lint auto-fix",
        "不包含 Agent",
        "不包含 WikiPageIndexer",
        "不接入真实 embedding",
    )
    missing = [term for term in limitation_terms if term not in manual]
    assert not missing, f"阶段四 A 手册缺少限制说明: {missing}"

    forbidden_claims = (
        "已实现自动交叉链接",
        "已实现自动链接",
        "已实现完整 Lint auto-fix",
        "已实现 Agent",
        "已实现 WikiPageIndexer",
        "已接入真实 embedding",
        "使用真实 embedding 模型",
        "已实现真实模型",
    )
    present = [claim for claim in forbidden_claims if claim in manual]
    assert not present, f"阶段四 A 手册夸大了未实现能力: {present}"


def test_phase_four_a_manual_commands_are_real_and_commented_in_chinese() -> None:
    manual = _read(MANUAL_PATH)
    commands = _powershell_commands(manual)

    assert commands, "阶段四 A 手册缺少 PowerShell 命令示例"
    assert all(
        comment.startswith("#") and CHINESE_RE.search(comment)
        for comment, _command in commands
    )
    command_lines = [command for _comment, command in commands]
    assert "uv run pytest tests/wiki/test_ingest_taxonomy.py -q" in command_lines
    assert (
        "uv run python -m app.wiki.tasks.enqueue_fake --op ingest "
        "--kb-id 11111111-1111-1111-1111-111111111111 "
        "--knowledge-id knowledge-1"
    ) in command_lines


def test_readme_links_phase_four_a_without_copying_the_manual() -> None:
    readme = _read("README.md")

    assert "[Wiki 阶段四 A](docs/Wiki阶段四A.md)" in readme
    assert all(term in readme for term in ("taxonomy", "fake embedding", "真正新页面"))
    assert "model_responses.taxonomies" not in readme
