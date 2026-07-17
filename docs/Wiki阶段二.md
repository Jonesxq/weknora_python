# Wiki 阶段二说明

## 已实现能力

阶段二在阶段一 REST API 和 PostgreSQL 页面仓储之上，增加了可运行的 fake 摄取链路：

- 入队时在同一 PostgreSQL 事务中登记 `finalization` marker、`pending` 操作和延迟 `Outbox` 事件，并按知识版本去重。
- Outbox dispatcher 领取事件后发布固定的 Celery 任务；事件只有在 broker 接收后才标记为已发送，投递语义为至少一次。
- Celery Worker 按知识库执行批次，使用 Redis token lock 保证同一知识库同时只有一个批次提交结果。
- Map/Reduce 先按知识条目并发生成 summary、entity 和 concept 更新，再按 slug 合并本批贡献与现有页面。
- 当前知识来源和模型响应由 `examples/wiki_fake_data.json` 提供；瞬时模型失败最多尝试三次。
- 原子页面提交会把页面、链接投影、摄取日志、finalization 释放、pending 删除或失败释放以及后续事件放在一个数据库事务内完成。
- `/stats` 返回页面、目录、链接、待处理问题和 pending 操作的真实统计，并通过锁探针返回当前知识库是否正在处理。

## 目录对应关系

阶段二沿用当前应用目录，没有独立子项目：

| 路径 | 当前职责 |
| --- | --- |
| `app/wiki/models.py` | `WikiPendingOp`、`WikiFinalizationMarker` 和 `TaskOutbox` ORM 模型。 |
| `app/wiki/ingest/schemas.py` | fake 数据、Map/Reduce 结果、Worker 参数和 finalization 请求的数据契约。 |
| `app/wiki/ingest/ports.py` | 知识来源、模型和 finalization 的端口协议。 |
| `app/wiki/ingest/fakes.py` | 加载共享 JSON fixture，并提供 fake 知识来源与 fake 模型响应。 |
| `app/wiki/ingest/enqueue.py` | 校验知识库配置、知识状态和文本块后执行原子入队。 |
| `app/wiki/ingest/map_document.py` | 重建来源文本并生成 summary、entity 和 concept 更新。 |
| `app/wiki/ingest/reduce_slug.py` | 按 slug 归并贡献，并与已有页面内容合并。 |
| `app/wiki/ingest/store.py` | pending/Outbox 领取、页面原子提交、finalization 和恢复事件。 |
| `app/wiki/ingest/worker.py` | 锁内批次编排、Map/Reduce 并发、重试、来源复核和冲突处理。 |
| `app/wiki/tasks/locks.py` | Redis token lock、续期、所有权确认和开发用进程内锁。 |
| `app/wiki/tasks/wiki_tasks.py` | `wiki.batch.run` Celery 任务和同步到异步 Worker 的运行桥接。 |
| `app/wiki/tasks/outbox_dispatcher.py` | Outbox 领取、Celery 发布、确认、释放和轮询退避。 |
| `app/wiki/tasks/enqueue_fake.py` | 从共享 fixture 手动入队的命令行入口。 |
| `app/infrastructure/tasks/celery_app.py` | Celery broker、result backend 和 JSON 序列化配置。 |
| `app/wiki/query_service.py` | `/stats` 的范围化数据库统计和锁活动探测。 |

## Alembic 迁移

阶段二迁移为 `migrations/versions/20260714_02_create_wiki_ingest_phase_two.py`，依赖阶段一迁移 `20260714_01`。它创建 `wiki_pending_ops`、`wiki_finalization_markers` 和 `task_outbox` 三张表及其唯一约束和领取索引。迁移目标是 PostgreSQL，不提供 SQLite 兼容模式。

```powershell
# 仅生成阶段一和阶段二迁移 SQL，不连接数据库
uv run alembic upgrade head --sql

# 将数据库升级到当前 Alembic head
uv run alembic upgrade head
```

## Fake fixture

`examples/wiki_fake_data.json` 是 Worker 当前使用的知识来源和模型响应。顶层包含：

- `knowledge_bases`：租户、知识库 UUID 和摄取配置。
- `knowledge`：知识条目、版本、状态以及文本/OCR/图片说明块。
- `model_responses`：按 `knowledge_id` 提供抽取与摘要响应，按规范化 slug 提供合并响应。
- `transient_failures`：可选的瞬时失败次数，用于验证重试行为。

由于 fake 模型响应以 `knowledge_id` 为键，fixture 中的 `knowledge_id` 必须全局唯一，不能只在租户或知识库内唯一。所有知识条目还必须属于 `knowledge_bases` 中已声明的租户和知识库。

JSON 示例：

```json
{
  "knowledge_bases": [
    {
      "tenant_id": 1,
      "knowledge_base_id": "11111111-1111-1111-1111-111111111111",
      "config": {
        "wiki_enabled": true,
        "synthesis_model_id": "fake-synthesis",
        "summary_model_id": "fake-summary",
        "extraction_granularity": "standard",
        "max_pages_per_ingest": 0
      }
    }
  ],
  "knowledge": [
    {
      "id": "knowledge-1",
      "tenant_id": 1,
      "knowledge_base_id": "11111111-1111-1111-1111-111111111111",
      "title": "Acme Retrieval Guide",
      "op_version": "version-1",
      "status": "ready",
      "chunks": [
        {
          "id": "chunk-1",
          "chunk_index": 0,
          "start_at": 0,
          "text": "Acme uses retrieval.",
          "ocr_text": "",
          "image_caption": ""
        }
      ]
    }
  ],
  "model_responses": {
    "extract_candidates": {
      "knowledge-1": {
        "entities": [],
        "concepts": [
          {
            "name": "Retrieval",
            "slug": "concept/retrieval",
            "page_type": "concept",
            "aliases": [],
            "description": "Finding source material.",
            "details": "Retrieval supplies context."
          }
        ]
      }
    },
    "summaries": {
      "knowledge-1": {
        "headline": "Acme Retrieval Guide",
        "markdown": "Acme uses retrieval."
      }
    },
    "merges": {
      "concept/retrieval": {
        "headline": "Retrieval",
        "markdown": "Retrieval finds source material."
      }
    }
  },
  "transient_failures": {}
}
```

## 环境变量

`.env.example` 给出本机安全默认值。Compose 中 Worker 和 dispatcher 使用服务名连接 PostgreSQL 与 Redis。

| 变量 | 当前用途 |
| --- | --- |
| `GRAPH_DATABASE_URL`、`GRAPH_DATABASE_ECHO` | PostgreSQL asyncpg 连接与 SQL 日志开关。 |
| `GRAPH_WIKI_CONTEXT_SECRET` | 阶段一签名访问上下文的占位密钥，本地使用前必须替换。 |
| `GRAPH_CELERY_BROKER_URL`、`GRAPH_CELERY_RESULT_BACKEND` | Celery broker 和 result backend，分别使用 Redis DB 0 和 DB 1。 |
| `GRAPH_REDIS_URL`、`GRAPH_WIKI_LOCK_MODE` | Redis token lock 使用 Redis DB 2；生产式运行使用 `redis` 模式。 |
| `GRAPH_WIKI_FAKE_DATA_FILE` | 当前 fake 知识来源和模型响应的 JSON 文件。 |
| `GRAPH_WIKI_OUTBOX_BATCH_SIZE`、`GRAPH_WIKI_OUTBOX_POLL_SECONDS`、`GRAPH_WIKI_OUTBOX_CLAIM_TIMEOUT_SECONDS` | Dispatcher 每轮领取数、轮询间隔和领取超时。 |
| `GRAPH_WIKI_INGEST_BATCH_SIZE`、`GRAPH_WIKI_INGEST_MAP_PARALLEL`、`GRAPH_WIKI_INGEST_REDUCE_PARALLEL` | Worker 批次大小和 Map/Reduce 并发上限。 |
| `GRAPH_WIKI_CLAIM_TIMEOUT_SECONDS` | pending 操作领取超时和恢复事件延迟。 |
| `GRAPH_WIKI_MAX_PAGES_PER_INGEST`、`GRAPH_WIKI_EXTRACTION_GRANULARITY` | 单次页面上限和抽取粒度。 |

## 启动阶段二运行组件

仓库没有新增 API Compose 服务。FastAPI 仍在宿主机启动；PostgreSQL、Redis、Worker 和 dispatcher 由 Compose 启动。

```powershell
# 同步锁文件中的 Python 依赖
uv sync

# 创建本地环境变量文件，随后替换其中的上下文签名密钥占位符
Copy-Item .env.example .env

# 启动 PostgreSQL 和 Redis，并等待健康检查通过
docker compose up -d postgres redis

# 应用阶段一和阶段二数据库迁移
uv run alembic upgrade head

# 构建共用镜像并启动 Celery Worker 与 Outbox dispatcher
docker compose up -d wiki-worker outbox-dispatcher

# 使用本地环境文件启动 FastAPI
uv run uvicorn app.main:app --reload --env-file .env
```

Compose 中 Worker 的真实命令是 `celery -A app.infrastructure.tasks.celery_app:celery_app worker -l INFO`，dispatcher 的真实命令是 `python -m app.wiki.tasks.outbox_dispatcher`。

## Fake 入队 CLI

先完成迁移并启动 PostgreSQL、Redis、Worker 和 dispatcher。以下命令在 Worker 服务的相同镜像和环境中入队 fixture 自带的知识条目；输出包含 `pending_op_id`、`skipped_reason` 和 `deduplicated`。

```powershell
# 从共享 fake fixture 入队 knowledge-1
docker compose run --rm wiki-worker python -m app.wiki.tasks.enqueue_fake --kb-id 11111111-1111-1111-1111-111111111111 --knowledge-id knowledge-1
```

相同 `tenant_id`、知识库、`knowledge_id` 和 `op_version` 重复入队会复用已有记录，并返回 `deduplicated: true`。

## 查看 Stats

Stats 路径仍属于阶段一 REST API：

```text
GET /api/v1/knowledgebase/{kb_id}/wiki/stats
```

该接口没有省略鉴权的开发捷径。请复用[阶段一说明](Wiki阶段一.md#临时访问上下文)中的签名访问头，通过可信网关或现有签名客户端提供 `X-Tenant-ID`、`X-User-ID`、`X-Role` 和 `X-Wiki-Context-Signature`。响应中的 `pending_tasks` 是当前租户和知识库下 `wiki_pending_ops` 的实际数量，`is_active` 来自同一知识库的锁活动探针。

## 失败恢复

- **Outbox 至少一次**：dispatcher 在 broker 发布成功后逐条确认。发布失败会释放该事件供下轮重试；broker 已接收但数据库确认失败时不释放 claim，超过 Outbox `claim timeout` 后会重新领取并可能重复发布，因此 Celery 任务使用稳定的 Outbox 事件 ID 作为 task ID。
- **pending claim timeout**：Worker 领取 pending 批次时同时创建一条在领取超时后可用的恢复 Outbox。正常提交会取消该恢复事件；Worker 中断后，超时 claim 可以被后续批次重新领取。
- **follow-up**：事务提交后若知识库仍有 pending 操作，或失败操作被释放，会写入短延迟 follow-up Outbox，继续触发该知识库的下一批。
- **PageConflict**：若模型计算期间页面身份、版本或类型发生变化，当前批次不会部分覆盖页面。Worker 释放本批全部 pending，增加 `fail_count`，并由 follow-up 重新处理。
- **锁丢失**：Redis token lock 使用随机 token、定时续期和比较 token 后释放。Worker 在原子提交前再次确认所有权；确认失败时抛出锁丢失错误，不提交页面，pending 留待 claim timeout 和恢复事件处理。

## 阶段二限制

阶段二仅实现 fake 数据驱动的摄取运行链路，阶段三至五尚未实现。以下能力均未实现：chunk citation、dead-letter、retract/tombstone、taxonomy、普通检索索引同步、真实 LLM、真实知识服务。当前也没有生产知识服务回调、真实模型适配器或自动清理长期失败任务的机制。
