from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_alembic_has_single_phase_three_head() -> None:
    config = Config("alembic.ini")
    scripts = ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ["20260718_03"]
    revision = scripts.get_revision("20260718_03")
    assert revision is not None
    assert revision.down_revision == "20260714_02"
    assert revision.doc == "创建 Wiki 阶段三贡献账本与死信队列表结构"
    assert callable(revision.module.upgrade)
    assert callable(revision.module.downgrade)

    phase_two = scripts.get_revision("20260714_02")
    assert phase_two is not None
    assert phase_two.down_revision == "20260714_01"
    assert phase_two.doc == "创建 Wiki 阶段二摄取队列表结构"
    assert callable(phase_two.module.upgrade)
    assert callable(phase_two.module.downgrade)


def test_phase_three_upgrade_generates_expected_offline_sql() -> None:
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

    assert "CREATE TABLE wiki_page_contributions" in sql
    assert "CREATE TABLE wiki_dead_letters" in sql
    assert "CONSTRAINT uq_wiki_page_contributions_version UNIQUE" in sql
    assert "CONSTRAINT uq_wiki_dead_letters_pending_op UNIQUE (pending_op_id)" in sql
    contributions_sql = _create_table_sql(sql, "wiki_page_contributions")
    dead_letters_sql = _create_table_sql(sql, "wiki_dead_letters")

    assert "state VARCHAR(32) DEFAULT 'active' NOT NULL" in contributions_sql
    assert "content TEXT DEFAULT '' NOT NULL" in contributions_sql
    assert "summary TEXT DEFAULT '' NOT NULL" in contributions_sql
    assert "aliases JSONB DEFAULT '[]'::jsonb NOT NULL" in contributions_sql
    assert "chunk_refs JSONB DEFAULT '[]'::jsonb NOT NULL" in contributions_sql
    assert (
        "CONSTRAINT uq_wiki_page_contributions_version UNIQUE "
        "(tenant_id, knowledge_base_id, slug, knowledge_id, op_version)"
    ) in contributions_sql
    assert "payload JSONB DEFAULT '{}'::jsonb NOT NULL" in dead_letters_sql
    assert "fail_count INTEGER NOT NULL" in dead_letters_sql
    assert "fail_count INTEGER DEFAULT 0 NOT NULL" not in dead_letters_sql
    assert "last_error_code VARCHAR(128) NOT NULL" in dead_letters_sql
    assert "last_error_summary VARCHAR(2000) NOT NULL" in dead_letters_sql
    assert "CREATE UNIQUE INDEX uq_wiki_page_contributions_active_source" in sql
    assert "(tenant_id, knowledge_base_id, slug, knowledge_id) WHERE state = 'active'" in sql
    assert "WHERE state = 'active'" in sql
    assert "CREATE INDEX ix_wiki_page_contributions_slug_state ON wiki_page_contributions (tenant_id, knowledge_base_id, slug, state)" in sql
    assert "CREATE INDEX ix_wiki_page_contributions_source_state ON wiki_page_contributions (tenant_id, knowledge_base_id, knowledge_id, state)" in sql
    assert "CREATE INDEX ix_wiki_dead_letters_scope_dead_at ON wiki_dead_letters (tenant_id, knowledge_base_id, dead_at)" in sql
    assert (
        "CREATE INDEX ix_wiki_pages_dedup_names_trgm ON wiki_pages USING gist "
        "((lower(title) || ' ' || lower(coalesce(aliases::text, ''))) gist_trgm_ops)"
    ) in sql
    assert "deleted_at IS NULL AND status = 'published' AND page_type IN ('entity', 'concept')" in sql


def test_phase_three_downgrade_generates_expected_offline_sql() -> None:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "downgrade", "20260718_03:20260714_02", "--sql"],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )

    assert result.returncode == 0, result.stderr
    sql = " ".join(result.stdout.split())

    dedup_index = "DROP INDEX ix_wiki_pages_dedup_names_trgm"
    dead_letters = "DROP TABLE wiki_dead_letters"
    contributions = "DROP TABLE wiki_page_contributions"
    assert dedup_index in sql
    assert dead_letters in sql
    assert contributions in sql
    assert sql.index(dedup_index) < sql.index(dead_letters) < sql.index(contributions)


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


def _create_table_sql(sql: str, table_name: str) -> str:
    start = sql.index(f"CREATE TABLE {table_name}")
    end = sql.index(";", start)
    return sql[start:end]
