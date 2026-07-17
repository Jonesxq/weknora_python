"""Wiki 阶段二运行配置与文档契约。"""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CHINESE_RE = re.compile(r"[\u4e00-\u9fff]")


def _read(path: str) -> str:
    target = ROOT / path
    return target.read_text(encoding="utf-8") if target.exists() else ""


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


def test_wiki_phase_two_runtime_and_documentation_contract() -> None:
    violations: list[str] = []

    dockerfile = _read("Dockerfile")
    docker_requirements = {
        "Dockerfile 非空": bool(dockerfile.strip()),
        "Python 3.12 slim 基础镜像": "FROM python:3.12-slim" in dockerfile,
        "安装 uv": "pip install --no-cache-dir uv" in dockerfile,
        "锁定生产依赖": "uv sync --frozen --no-dev" in dockerfile,
        "复制 app": "COPY app ./app" in dockerfile,
        "复制 migrations": "COPY migrations ./migrations" in dockerfile,
        "复制 examples": "COPY examples ./examples" in dockerfile,
        "复制 alembic.ini": "COPY alembic.ini ./" in dockerfile,
        "venv PATH": 'ENV PATH="/app/.venv/bin:$PATH"' in dockerfile,
        "FastAPI 默认命令": (
            'CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]'
            in dockerfile
        ),
    }
    violations.extend(name for name, ok in docker_requirements.items() if not ok)

    compose = _read("docker-compose.yml")
    postgres = _service_block(compose, "postgres")
    redis = _service_block(compose, "redis")
    worker = _service_block(compose, "wiki-worker")
    dispatcher = _service_block(compose, "outbox-dispatcher")
    compose_requirements = {
        "保留 PostgreSQL": "postgres:16-alpine" in postgres,
        "保留 PostgreSQL volume": "graph_postgres_data:/var/lib/postgresql/data" in postgres,
        "Redis 7 alpine": "image: redis:7-alpine" in redis,
        "Redis 端口": '"6379:6379"' in redis,
        "Redis healthcheck": all(
            value in redis
            for value in ("redis-cli", "ping", "interval: 5s", "timeout: 3s", "retries: 20")
        ),
        "Worker 共用镜像": "build: ." in worker,
        "Worker Celery 命令": (
            "celery -A app.infrastructure.tasks.celery_app:celery_app worker -l INFO" in worker
        ),
        "Worker 依赖健康服务": all(
            value in worker
            for value in ("postgres:", "redis:", "condition: service_healthy")
        ),
        "Worker 阶段二环境": all(
            value in worker
            for value in (
                "GRAPH_DATABASE_URL=postgresql+asyncpg://postgres:postgres@postgres:5432/graph",
                "GRAPH_REDIS_URL=redis://redis:6379/2",
                "GRAPH_CELERY_BROKER_URL=redis://redis:6379/0",
                "GRAPH_CELERY_RESULT_BACKEND=redis://redis:6379/1",
                "GRAPH_WIKI_LOCK_MODE=redis",
                "GRAPH_WIKI_FAKE_DATA_FILE=/app/examples/wiki_fake_data.json",
            )
        ),
        "Dispatcher 共用镜像": "build: ." in dispatcher,
        "Dispatcher 命令": "python -m app.wiki.tasks.outbox_dispatcher" in dispatcher,
        "Dispatcher 依赖健康服务": all(
            value in dispatcher
            for value in ("postgres:", "redis:", "condition: service_healthy")
        ),
        "Dispatcher 阶段二环境": all(
            value in dispatcher
            for value in (
                "GRAPH_DATABASE_URL=postgresql+asyncpg://postgres:postgres@postgres:5432/graph",
                "GRAPH_CELERY_BROKER_URL=redis://redis:6379/0",
                "GRAPH_CELERY_RESULT_BACKEND=redis://redis:6379/1",
            )
        ),
        "Compose 不包含真实密钥": all(
            marker not in compose.lower() for marker in ("sk-", "api_key", "access_token")
        ),
    }
    violations.extend(name for name, ok in compose_requirements.items() if not ok)

    env_values = {}
    for raw_line in _read(".env.example").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            env_values[key] = value

    expected_env = {
        "GRAPH_DATABASE_URL": "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph",
        "GRAPH_DATABASE_ECHO": "false",
        "GRAPH_CELERY_BROKER_URL": "redis://127.0.0.1:6379/0",
        "GRAPH_CELERY_RESULT_BACKEND": "redis://127.0.0.1:6379/1",
        "GRAPH_REDIS_URL": "redis://127.0.0.1:6379/2",
        "GRAPH_WIKI_LOCK_MODE": "redis",
        "GRAPH_WIKI_FAKE_DATA_FILE": "examples/wiki_fake_data.json",
        "GRAPH_WIKI_OUTBOX_BATCH_SIZE": "100",
        "GRAPH_WIKI_OUTBOX_POLL_SECONDS": "1",
        "GRAPH_WIKI_OUTBOX_CLAIM_TIMEOUT_SECONDS": "60",
        "GRAPH_WIKI_INGEST_BATCH_SIZE": "5",
        "GRAPH_WIKI_INGEST_MAP_PARALLEL": "10",
        "GRAPH_WIKI_INGEST_REDUCE_PARALLEL": "10",
        "GRAPH_WIKI_CLAIM_TIMEOUT_SECONDS": "600",
        "GRAPH_WIKI_MAX_PAGES_PER_INGEST": "0",
        "GRAPH_WIKI_EXTRACTION_GRANULARITY": "standard",
    }
    for key, value in expected_env.items():
        if env_values.get(key) != value:
            violations.append(f".env.example: {key}")
    context_secret = env_values.get("GRAPH_WIKI_CONTEXT_SECRET", "")
    if len(context_secret) < 32 or "replace" not in context_secret.lower():
        violations.append(".env.example: 安全的上下文密钥占位符")

    phase_two = _read("docs/Wiki阶段二.md")
    required_doc_text = (
        "## 已实现能力",
        "pending",
        "Outbox",
        "finalization",
        "Redis token lock",
        "Celery",
        "Map/Reduce",
        "原子页面提交",
        "真实统计",
        "## 目录对应关系",
        "app/wiki/ingest/",
        "app/wiki/tasks/",
        "app/infrastructure/tasks/celery_app.py",
        "## Alembic 迁移",
        "examples/wiki_fake_data.json",
        "knowledge_id",
        "全局唯一",
        "## 启动阶段二运行组件",
        "docker compose up -d postgres redis",
        "wiki-worker",
        "outbox-dispatcher",
        "app.wiki.tasks.enqueue_fake",
        "/stats",
        "X-Wiki-Context-Signature",
        "## 失败恢复",
        "至少一次",
        "claim timeout",
        "follow-up",
        "PageConflict",
        "锁丢失",
        "## 阶段二限制",
    )
    if not phase_two:
        violations.append("docs/Wiki阶段二.md 存在")
    elif not CHINESE_RE.search(phase_two):
        violations.append("docs/Wiki阶段二.md 使用中文")
    for text in required_doc_text:
        if text not in phase_two:
            violations.append(f"docs/Wiki阶段二.md: {text}")
    limitations = phase_two.partition("## 阶段二限制")[2]
    for term in (
        "chunk citation",
        "dead-letter",
        "retract/tombstone",
        "taxonomy",
        "索引同步",
        "真实 LLM",
        "真实知识服务",
    ):
        if term not in limitations or "未实现" not in limitations:
            violations.append(f"阶段二限制声明: {term}")
    if phase_two and not _commands_have_chinese_comments(phase_two):
        violations.append("阶段二文档命令具有中文注释")

    readme = _read("README.md")
    readme_requirements = {
        "README 阶段一链接": "[Wiki 阶段一说明](docs/Wiki阶段一.md)" in readme,
        "README 阶段二链接": "[Wiki 阶段二说明](docs/Wiki阶段二.md)" in readme,
        "README 启动 PostgreSQL/Redis": "docker compose up -d postgres redis" in readme,
        "README 启动 Worker": "docker compose up -d wiki-worker outbox-dispatcher" in readme,
        "README 数据库迁移": "uv run alembic upgrade head" in readme,
        "README 启动 API": "uv run uvicorn app.main:app --reload --env-file .env" in readme,
        "README 运行测试": "uv run pytest -q" in readme,
        "README 命令具有中文注释": _commands_have_chinese_comments(readme),
    }
    violations.extend(name for name, ok in readme_requirements.items() if not ok)

    assert not violations, "阶段二运行与文档契约缺失:\n- " + "\n- ".join(violations)
