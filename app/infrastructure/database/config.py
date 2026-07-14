"""PostgreSQL 数据库配置。"""

from __future__ import annotations

import os
from dataclasses import dataclass

from sqlalchemy.engine import URL, make_url

DEFAULT_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph"
_TRUE_VALUES = {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """应用数据库连接参数。"""

    url: URL
    echo: bool = False

    @classmethod
    def from_env(cls) -> "DatabaseSettings":
        """从环境变量读取并验证 PostgreSQL async URL。"""

        url = make_url(os.getenv("GRAPH_DATABASE_URL", DEFAULT_DATABASE_URL))
        if url.drivername != "postgresql+asyncpg":
            raise ValueError("GRAPH_DATABASE_URL 必须使用 PostgreSQL asyncpg 驱动")
        echo = os.getenv("GRAPH_DATABASE_ECHO", "false").strip().lower() in _TRUE_VALUES
        return cls(url=url, echo=echo)
