"""Wiki 阶段三运行配置与中文文档合同。"""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CHINESE_RE = re.compile(r"[\u4e00-\u9fff]")


def _read(path: str) -> str:
    target = ROOT / path
    return target.read_text(encoding="utf-8") if target.exists() else ""


def _env_values() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in _read(".env.example").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def _service_block(compose: str, service: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(service)}:\s*\n(?P<body>.*?)(?=^  [\w-]+:\s*\n|^volumes:\s*$|\Z)",
        compose,
    )
    return match.group("body") if match else ""


def _commands_have_chinese_comments(markdown: str) -> bool:
    blocks = re.findall(r"(?ms)^```(?:powershell|bash|sh)\s*\n(.*?)^```\s*$", markdown)
    if not blocks:
        return False
    for block in blocks:
        previous = ""
        for raw_line in block.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("#"):
                previous = line
                continue
            if not (previous.startswith("#") and CHINESE_RE.search(previous)):
                return False
            previous = line
    return True


def test_phase_three_env_and_compose_defaults_match_runtime() -> None:
    expected = {
        "GRAPH_REDIS_URL": "redis://127.0.0.1:6379/2",
        "GRAPH_WIKI_CITATION_BATCH_CHARS": "12000",
        "GRAPH_WIKI_CITATION_PARALLEL": "4",
        "GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT": "20",
        "GRAPH_WIKI_TOMBSTONE_TTL_SECONDS": "3600",
        "GRAPH_WIKI_TOMBSTONE_MODE": "redis",
    }
    env = _env_values()
    assert {key: env.get(key) for key in expected} == expected
    assert "GRAPH_WIKI_DEAD_LETTER_THRESHOLD" not in env

    compose = _read("docker-compose.yml")
    defaults = (
        "GRAPH_WIKI_CITATION_BATCH_CHARS=${GRAPH_WIKI_CITATION_BATCH_CHARS:-12000}",
        "GRAPH_WIKI_CITATION_PARALLEL=${GRAPH_WIKI_CITATION_PARALLEL:-4}",
        "GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT=${GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT:-20}",
        "GRAPH_WIKI_TOMBSTONE_TTL_SECONDS=${GRAPH_WIKI_TOMBSTONE_TTL_SECONDS:-3600}",
        "GRAPH_WIKI_TOMBSTONE_MODE=${GRAPH_WIKI_TOMBSTONE_MODE:-redis}",
        "GRAPH_REDIS_URL=redis://redis:6379/2",
    )
    for service in ("wiki-worker", "outbox-dispatcher"):
        block = _service_block(compose, service)
        assert block, f"缺少 compose 服务: {service}"
        assert all(value in block for value in defaults), service
    assert "GRAPH_WIKI_DEAD_LETTER_THRESHOLD" not in compose


def test_phase_three_readme_and_manual_cover_only_implemented_behavior() -> None:
    readme = _read("README.md")
    assert "[Wiki 阶段三说明](docs/Wiki阶段三.md)" in readme
    assert all(term in readme for term in ("增量", "citation", "撤回", "dead-letter"))

    manual = _read("docs/Wiki阶段三.md")
    assert manual and CHINESE_RE.search(manual)
    required = (
        "20260719_04",
        "wiki_page_contributions",
        "wiki_dead_letters",
        "result_outcome",
        "uv run alembic upgrade head",
        "examples/wiki_fake_data.json",
        "c001",
        "supplemental_candidates",
        "canonical_slug",
        "白名单",
        "app.wiki.tasks.enqueue_fake",
        "--op retract",
        "op_version",
        "chunks",
        "model_responses",
        "Redis-first",
        "立即最小清理",
        "语义撤回",
        "3600",
        "第 5 次",
        "dead-letter",
        "GRAPH_TEST_POSTGRES_URL",
        "GRAPH_TEST_REDIS_URL",
        "随机 schema",
        "随机 key",
        "EXPLAIN",
        "GiST",
        "fake 上游",
        "taxonomy",
        "自动链接",
        "Lint",
        "Agent",
        "WikiPageIndexer",
        "前端 citation 语法",
        "REST 字段不变",
    )
    missing = [term for term in required if term not in manual]
    assert not missing, f"阶段三手册缺少: {missing}"

    failure_section = manual.partition("普通失败")[2]
    assert all(
        term in failure_section
        for term in ("busy", "CAS", "claim", "锁", "取消", "superseded", "不计")
    )
    assert "无查询 API" in manual and "无重放 API" in manual
    assert "真实 LLM" not in manual or "不包含" in manual


def test_phase_three_manual_commands_are_real_and_have_chinese_comments() -> None:
    manual = _read("docs/Wiki阶段三.md")
    commands = (
        "uv run alembic upgrade head",
        "uv run python -m app.wiki.tasks.enqueue_fake",
        "--op retract",
        "docker compose up -d wiki-worker",
        "docker compose up -d outbox-dispatcher",
        "uv run pytest",
    )
    assert all(command in manual for command in commands)
    assert _commands_have_chinese_comments(manual)
