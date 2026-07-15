from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_alembic_has_single_phase_two_head() -> None:
    config = Config("alembic.ini")
    scripts = ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ["20260714_02"]
    revision = scripts.get_revision("20260714_02")
    assert revision is not None
    assert revision.down_revision == "20260714_01"
    assert revision.doc == "创建 Wiki 阶段二摄取队列表结构"
    assert callable(revision.module.upgrade)
    assert callable(revision.module.downgrade)


def test_phase_two_upgrade_generates_expected_offline_sql() -> None:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )

    assert result.returncode == 0, result.stderr
    sql = " ".join(result.stdout.split())

    assert "CREATE TABLE wiki_pending_ops" in sql
    assert "CREATE TABLE wiki_finalization_markers" in sql
    assert "CREATE TABLE task_outbox" in sql
    assert "dedup_key VARCHAR(64) NOT NULL" in sql
    assert (
        "CONSTRAINT uq_task_outbox_scope_event_dedup "
        "UNIQUE (tenant_id, knowledge_base_id, event_type, dedup_key)"
    ) in sql
    assert "CONSTRAINT uq_wiki_pending_ops_version UNIQUE" in sql
    assert "CONSTRAINT uq_wiki_finalization_markers_attempt UNIQUE" in sql
    assert "payload JSONB DEFAULT '{}'::jsonb NOT NULL" in sql
    assert "fail_count INTEGER DEFAULT 0 NOT NULL" in sql
    assert "attempts INTEGER DEFAULT 0 NOT NULL" in sql
    assert "DEFAULT now() NOT NULL" in sql
    assert "CREATE INDEX ix_wiki_pending_ops_scope_claim" in sql
    assert "CREATE INDEX ix_wiki_finalization_markers_scope" in sql
    assert "CREATE INDEX ix_task_outbox_delivery" in sql
    assert "CREATE INDEX ix_task_outbox_scope" in sql
