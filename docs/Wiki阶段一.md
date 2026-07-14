# Wiki 阶段一说明

## 已实现范围

Wiki API 复用当前 FastAPI 应用入口，统一前缀为：

```text
/api/v1/knowledgebase/{kb_id}/wiki
```

当前实现包含以下能力：

- Wiki 页面分页、创建、读取、更新、软删除和目录移动。
- Wiki 目录的直接子目录查询、创建、重命名、移动和空目录删除。
- 页面正文中的 `[[slug]]` 与 `[[slug|显示名称]]` 链接投影。
- Index、追加式 Log、Graph overview/ego、Stats、Search、Lint 和 Issues 查询。
- 链接投影重建和范围化问题状态更新。
- 页面乐观锁、目录移动防环、最多三层目录和租户/知识库范围限制。

阶段一未实现文档摄取、LLM Map/Reduce、Celery、Redis、Outbox、`auto-fix`、Agent 工具和普通检索索引同步。

## 目录集成

实现沿用当前项目结构，没有创建独立 Python 子项目：

- `app/api/v1/endpoints/wiki.py`：Wiki REST 路由。
- `app/schemas/wiki/`：请求和响应 DTO。
- `app/wiki/`：领域规则、页面/目录服务和 PostgreSQL 仓储。
- `app/infrastructure/database/`：数据库配置与异步会话。
- `migrations/`：Alembic 环境和版本迁移。
- `tests/wiki/`：领域、服务、SQL 方言和 API 合同测试。

## 数据库

生产和集成测试目标是 PostgreSQL，不提供 SQLite 兼容模式。默认连接地址为：

```text
postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph
```

可通过以下环境变量覆盖：

- `GRAPH_DATABASE_URL`：必须使用 `postgresql+asyncpg` 驱动。
- `GRAPH_DATABASE_ECHO`：设置为 `true` 时输出 SQL 日志。
- `GRAPH_WIKI_CONTEXT_SECRET`：至少 32 个字符，用于验证可信网关注入的访问上下文。

```powershell
# 启动仓库内定义的 PostgreSQL 16 服务
docker compose up -d postgres

# 执行 Wiki 阶段一数据库迁移
uv run alembic upgrade head

# 仅生成迁移 SQL，不连接数据库
uv run alembic upgrade head --sql
```

## 临时访问上下文

当前仓库还没有统一鉴权模块，因此 Wiki API 通过可替换的 FastAPI 依赖读取可信网关注入的请求头。网关必须移除外部请求中的同名头，再使用 `GRAPH_WIKI_CONTEXT_SECRET` 生成签名：

| 请求头 | 说明 |
| --- | --- |
| `X-Tenant-ID` | 当前租户整数 ID |
| `X-User-ID` | 当前用户或调用方 ID |
| `X-Role` | `viewer`、`contributor`、`owner` 或 `admin` |
| `X-Wiki-Context-Signature` | 对 tenant、user、role 和 kb_id 计算的 HMAC-SHA256 十六进制签名 |

`viewer` 和尚未携带 KB 编辑授权证明的 `contributor` 只能读取；`owner` 和 `admin` 可以写入。接入正式鉴权后，应替换 `app.api.dependencies.get_wiki_scope`，并由认证/ACL 层确认 KB 归属或共享权限；Wiki 服务和仓储不需要改动。

签名载荷使用换行连接以下字段：`tenant_id`、`user_id`、规范化小写 `role`、`kb_id`。服务端未配置密钥时返回 503，签名不匹配时返回 401。

## 接口清单

阶段一共提供 19 个 HTTP 操作：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET/POST | `/pages` | 分页列表、创建页面 |
| GET/PUT/DELETE | `/pages/{slug}` | 读取、更新、软删除页面 |
| PUT | `/move-page` | 使用 `folder_id` 移动页面 |
| GET/POST | `/folders` | 列出直接子目录、创建目录 |
| PUT/DELETE | `/folders/{folder_id}` | 更新目录、删除空目录 |
| GET | `/index` | 按页面类型返回窄列索引 |
| GET | `/log` | 按自增 ID 游标读取日志 |
| GET | `/graph` | 读取 overview 或最多三跳 ego 图 |
| GET | `/stats` | 返回页面、目录、链接和问题统计 |
| GET | `/search` | 使用 trigram、slug 和全文检索排名 |
| POST | `/rebuild-links` | 从正文重建结构化链接 |
| GET | `/lint` | 检查孤立页、死链和空页面 |
| GET | `/issues` | 游标分页查询问题单 |
| PUT | `/issues/{issue_id}/status` | 范围化更新问题状态 |

页面更新建议提交 `version`。版本冲突返回 HTTP 409 和 `VERSION_CONFLICT`；暂时缺失版本时使用读取到的当前版本，供旧客户端过渡。

## PostgreSQL 集成测试

仓储集成测试只连接专用 PostgreSQL 测试库。设置测试连接后运行：

```powershell
# 指向允许创建临时 schema 的专用 PostgreSQL 测试库
$env:GRAPH_TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph_test"

# 运行真实 PostgreSQL 仓储集成测试
uv run pytest tests/wiki/test_postgres_integration.py -q
```

未设置 `GRAPH_TEST_DATABASE_URL` 时该测试会跳过，不会回退到 SQLite。
