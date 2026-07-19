# Graph Python 服务

当前服务包含项目内文档解析 API，以及 Wiki 阶段一 REST API 和阶段二 fake 摄取运行链路。Wiki 使用 PostgreSQL 16、Redis 7、Celery、Async SQLAlchemy 2 和 Alembic。

```powershell
# 同步 Python 依赖
uv sync

# 创建本地环境变量文件并替换其中的 Wiki 上下文密钥
Copy-Item .env.example .env

# 启动本地 PostgreSQL 和 Redis
docker compose up -d postgres redis

# 创建或升级数据库表结构
uv run alembic upgrade head

# 构建并启动 Wiki Worker 与 Outbox dispatcher
docker compose up -d wiki-worker outbox-dispatcher

# 启动 FastAPI 开发服务
uv run uvicorn app.main:app --reload --env-file .env

# 运行全部自动化测试
uv run pytest -q
```

Wiki 的 REST API、签名访问头和阶段一范围见 [Wiki 阶段一说明](docs/Wiki阶段一.md)；fake 摄取、Worker、Outbox 和失败恢复见 [Wiki 阶段二说明](docs/Wiki阶段二.md)；增量贡献、citation、重解析、撤回和 dead-letter 的当前运行方式见 [Wiki 阶段三说明](docs/Wiki阶段三.md)。

Wiki 阶段三继续使用可校验的 fake 上游，在阶段二链路上增加 citation、canonical 去重、贡献差量、Redis-first 撤回与第 5 次普通失败 dead-letter；现有 REST 字段保持不变。
