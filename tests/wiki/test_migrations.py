from __future__ import annotations

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_alembic_has_single_phase_one_head() -> None:
    config = Config("alembic.ini")
    scripts = ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ["20260714_01"]
    revision = scripts.get_revision("20260714_01")
    assert revision is not None
    assert revision.doc == "创建 Wiki 阶段一表结构"
