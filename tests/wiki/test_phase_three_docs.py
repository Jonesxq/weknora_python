"""Wiki 阶段三运行配置与中文文档合同。"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from app.wiki.ingest.schemas import WikiWorkerOptions


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


def _compose_environment(service: str) -> dict[str, str]:
    compose = yaml.safe_load(_read("docker-compose.yml"))
    services = compose.get("services", {}) if isinstance(compose, dict) else {}
    service_config = services.get(service, {})
    environment = service_config.get("environment", {})
    if isinstance(environment, dict):
        return {str(key): str(value) for key, value in environment.items()}

    values: dict[str, str] = {}
    for item in environment:
        key, value = str(item).split("=", 1)
        values[key] = value
    return values


def _command_pairs(markdown: str, languages: str) -> list[tuple[str, str]]:
    blocks = re.findall(rf"(?ms)^```(?:{languages})\s*\n(.*?)^```\s*$", markdown)
    pairs: list[tuple[str, str]] = []
    for block in blocks:
        previous = ""
        for raw_line in block.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("#"):
                previous = line
                continue
            pairs.append((previous, line))
            previous = line
    return pairs


def _commands_have_chinese_comments(markdown: str) -> bool:
    pairs = _command_pairs(markdown, "powershell|bash|sh")
    if not pairs:
        return False
    return all(
        comment.startswith("#") and CHINESE_RE.search(comment)
        for comment, _command in pairs
    )


def test_phase_three_env_defaults_match_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_env = {
        "GRAPH_REDIS_URL": "redis://127.0.0.1:6379/2",
        "GRAPH_WIKI_CITATION_BATCH_CHARS": "12000",
        "GRAPH_WIKI_CITATION_PARALLEL": "4",
        "GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT": "20",
        "GRAPH_WIKI_TOMBSTONE_TTL_SECONDS": "3600",
        "GRAPH_WIKI_TOMBSTONE_MODE": "redis",
    }
    env = _env_values()
    assert {key: env.get(key) for key in expected_env} == expected_env
    assert "GRAPH_WIKI_DEAD_LETTER_THRESHOLD" not in env

    option_keys = {
        "GRAPH_WIKI_CITATION_BATCH_CHARS": "citation_batch_chars",
        "GRAPH_WIKI_CITATION_PARALLEL": "citation_parallel",
        "GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT": "dedup_candidate_limit",
        "GRAPH_WIKI_TOMBSTONE_TTL_SECONDS": "tombstone_ttl_seconds",
    }
    for key in option_keys:
        monkeypatch.delenv(key, raising=False)
    options = WikiWorkerOptions.from_env()
    assert {
        key: str(getattr(options, attribute))
        for key, attribute in option_keys.items()
    } == {key: expected_env[key] for key in option_keys}


def test_phase_three_compose_scopes_runtime_environment_to_worker() -> None:
    worker = _compose_environment("wiki-worker")
    dispatcher = _compose_environment("outbox-dispatcher")
    worker_runtime = {
        "GRAPH_WIKI_CITATION_BATCH_CHARS": "${GRAPH_WIKI_CITATION_BATCH_CHARS:-12000}",
        "GRAPH_WIKI_CITATION_PARALLEL": "${GRAPH_WIKI_CITATION_PARALLEL:-4}",
        "GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT": "${GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT:-20}",
        "GRAPH_WIKI_TOMBSTONE_TTL_SECONDS": "${GRAPH_WIKI_TOMBSTONE_TTL_SECONDS:-3600}",
        "GRAPH_WIKI_TOMBSTONE_MODE": "${GRAPH_WIKI_TOMBSTONE_MODE:-redis}",
    }
    assert {key: worker.get(key) for key in worker_runtime} == worker_runtime
    assert worker.get("GRAPH_REDIS_URL") == "redis://redis:6379/2"

    dispatcher_only = {
        "GRAPH_DATABASE_URL",
        "GRAPH_CELERY_BROKER_URL",
        "GRAPH_CELERY_RESULT_BACKEND",
    }
    worker_only = {"GRAPH_REDIS_URL", *worker_runtime}
    assert dispatcher_only <= dispatcher.keys()
    assert worker_only.isdisjoint(dispatcher)
    assert "GRAPH_WIKI_DEAD_LETTER_THRESHOLD" not in worker
    assert "GRAPH_WIKI_DEAD_LETTER_THRESHOLD" not in dispatcher


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

    powershell_commands = [
        command for _comment, command in _command_pairs(manual, "powershell")
    ]
    acceptance_order = (
        '$env:GRAPH_TEST_POSTGRES_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph"',
        '$env:GRAPH_TEST_REDIS_URL="redis://127.0.0.1:6379/15"',
        "uv run pytest tests/wiki/test_postgres_integration.py tests/wiki/test_tombstones.py tests/wiki/test_ingest_worker.py -q",
        "Remove-Item Env:GRAPH_TEST_POSTGRES_URL -ErrorAction SilentlyContinue",
        "Remove-Item Env:GRAPH_TEST_REDIS_URL -ErrorAction SilentlyContinue",
        "uv run pytest -q",
    )
    positions = [powershell_commands.index(command) for command in acceptance_order]
    assert positions == sorted(positions)
