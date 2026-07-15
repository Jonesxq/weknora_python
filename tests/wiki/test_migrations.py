from __future__ import annotations

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
