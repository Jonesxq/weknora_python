# Graph Python 服务

当前服务包含项目内文档解析 API 和 Wiki 阶段一 API。Wiki 使用 PostgreSQL 16、Async SQLAlchemy 2 和 Alembic。

```powershell
# 同步 Python 依赖
uv sync

# 启动本地 PostgreSQL
docker compose up -d postgres

# 创建或升级数据库表结构
uv run alembic upgrade head

# 启动 FastAPI 开发服务
uv run uvicorn app.main:app --reload

# 运行全部自动化测试
uv run pytest -q
```

Wiki 的配置、访问头和接口范围见 [Wiki 阶段一说明](docs/Wiki阶段一.md)。
