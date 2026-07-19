# Wiki 阶段三

阶段三在阶段二的 PostgreSQL、Redis、Celery、Outbox 和 fake Map/Reduce 链路上增加增量贡献、citation、canonical 去重、重解析、撤回与 dead-letter。上游仍由 `examples/wiki_fake_data.json` 驱动，本文只描述当前仓库已经实现的运行方式。

## 数据库迁移

迁移 `20260718_03` 创建 `wiki_page_contributions` 和 `wiki_dead_letters`，迁移 `20260719_04` 为 `wiki_log_entries` 增加 `result_outcome`。当前 Alembic head 是 `20260719_04`。

```powershell
# 将数据库升级到当前唯一的阶段三迁移 head
uv run alembic upgrade head

# 查看当前迁移 head，输出应包含 20260719_04
uv run alembic heads
```

## Fake fixture 合同

fixture 中每条知识记录的 `knowledge_id` 全局唯一，`op_version` 标识本次内容版本，`chunks` 保存本次来源片段，`model_responses` 保存严格模型响应。citation 使用每个 batch 内局部的 `cNNN` alias；`c001` 只在当前 batch 内解析，模型响应不得写真实 chunk ID。

`supplemental_candidates` 可以补充本批发现的候选。`deduplications` 按候选 slug 提供决定；非空 `canonical_slug` 必须来自当前去重请求的 `allowed_targets` 白名单，不能指向同批生成候选。以下是当前 schema 接受的精简形状：

```json
{
  "citations": {
    "knowledge-1": [
      {
        "refs_by_slug": {"entity/acme": ["c001"]},
        "supplemental_candidates": [
          {
            "name": "Grounding",
            "slug": "concept/grounding",
            "page_type": "concept"
          }
        ]
      }
    ]
  },
  "deduplications": {
    "concept/grounding": {
      "candidate_slug": "concept/grounding",
      "canonical_slug": "concept/retrieval"
    }
  }
}
```

## 入队与运行

CLI 默认执行 ingest。它从 fixture 严格读取 tenant、`op_version` 和 status；ingest 只接受 `ready`，retract 接受当前 schema 的 `ready`、`deleting`、`cancelled` 和 `deleted`。

```powershell
# 指定 CLI 和 Worker 共用的 fake fixture
$env:GRAPH_WIKI_FAKE_DATA_FILE="examples/wiki_fake_data.json"

# 指定默认 Redis tombstone 和锁使用的本地 Redis
$env:GRAPH_REDIS_URL="redis://127.0.0.1:6379/2"

# 使用默认 ingest 操作入队 fixture 中的当前版本
uv run python -m app.wiki.tasks.enqueue_fake --kb-id 11111111-1111-1111-1111-111111111111 --knowledge-id knowledge-1

# 使用 fixture 中的 op_version 入队 retract 操作
uv run python -m app.wiki.tasks.enqueue_fake --op retract --kb-id 11111111-1111-1111-1111-111111111111 --knowledge-id knowledge-1

# 启动处理 Wiki 批次的 Celery Worker
docker compose up -d wiki-worker

# 启动把事务 Outbox 事件投递给 Celery 的 dispatcher
docker compose up -d outbox-dispatcher
```

## 重解析

重解析同一知识时，先在 fixture 中更新该知识的 `op_version`、`chunks`，以及对应的 `model_responses`（包括 extraction、summary、citation、dedup 和 merge 响应），再使用默认 ingest CLI 入队。Worker 会把旧 active contribution 规划为 replace 或 retract_stale，并按 slug 重算页面；提交仍通过页面 expectation/CAS 原子完成。

## 删除与撤回

删除采用 Redis-first 顺序：`enqueue_retract` 先写 tombstone，再登记 retract pending operation。登记事务会立即最小清理可见页面和引用：唯一来源页先软删除，多来源页立即移除被删来源的 source/chunk refs，并把该来源贡献转为 `retract_pending`。

Worker 随后执行语义撤回：它不读取来源正文，只读取 `retract_pending` contribution，按剩余 active contribution 重算共享页面。并发中的旧 ingest 在 Map 或提交前看到 tombstone 后进入 superseded，不计普通失败。tombstone 默认 TTL 是 `3600` 秒，即 1 小时；生产默认 `GRAPH_WIKI_TOMBSTONE_MODE=redis`。

## 普通失败与 dead-letter

模型永久错误、瞬时错误重试耗尽、数据校验错误和未知 operation 属于普通失败。前 4 次会释放 claim 并保留 pending；第 5 次普通失败在同一事务写入 `wiki_dead_letters`、释放 finalization marker 并删除 pending。阈值固定为 5，没有对应环境变量。

busy、页面 CAS/PageConflict、claim 丢失、锁所有权丢失、父任务取消和 superseded 都不计普通失败，也不会增加 `fail_count`。错误摘要使用安全分类文本，不保存 traceback 或原始模型输出。

dead-letter 在阶段三无查询 API、无重放 API；当前只有 Store 内部读取能力和数据库记录，不应声明对外管理接口。

## 真实服务验收

真实集成测试只读取 `GRAPH_TEST_POSTGRES_URL` 和 `GRAPH_TEST_REDIS_URL`。未配置时测试以中文原因 skip，不连接默认开发服务。PostgreSQL fixture 使用随机 schema，Redis 用例使用随机 key，并在 teardown/finally 中清理各自资源。

```powershell
# 指定真实 PostgreSQL 集成测试数据库
$env:GRAPH_TEST_POSTGRES_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph"

# 指定真实 Redis 集成测试数据库
$env:GRAPH_TEST_REDIS_URL="redis://127.0.0.1:6379/15"

# 运行 PostgreSQL、Redis 和 Worker 阶段三验收
uv run pytest tests/wiki/test_postgres_integration.py tests/wiki/test_tombstones.py tests/wiki/test_ingest_worker.py -q

# 清除真实 PostgreSQL 测试地址，避免后续全量测试继续连接真实服务
Remove-Item Env:GRAPH_TEST_POSTGRES_URL -ErrorAction SilentlyContinue

# 清除真实 Redis 测试地址，恢复未配置真实服务时的 skip 行为
Remove-Item Env:GRAPH_TEST_REDIS_URL -ErrorAction SilentlyContinue

# 运行不连接默认真实服务的完整测试集
uv run pytest -q
```

去重索引验收会运行 `EXPLAIN`，确认局部 trigram `GiST` 索引 `ix_wiki_pages_dedup_names_trgm` 可用于 KNN 查询，并验证 tenant、KB、page type、published、未删除过滤及 top-20 cutoff tie 的稳定顺序。

## 阶段三限制

- 上游仍是 fake 上游，不包含真实 LLM 或真实知识服务适配器。
- 不包含 taxonomy、自动链接、Lint、Agent 和 `WikiPageIndexer`。
- 不包含前端 citation 语法或 citation 展示组件。
- 不新增 dead-letter 查询或重放 REST API。
- 现有 REST 字段不变；阶段三没有修改阶段一页面接口的请求和响应字段。
