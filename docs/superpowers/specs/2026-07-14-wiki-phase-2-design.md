# Wiki 阶段二设计规格

> 状态：已确认
> 日期：2026-07-14
> 基础版本：`main@9bdfc50`

## 1. 目标

在现有 `app/` 项目结构内实现 Wiki 阶段二：可靠异步入队、每知识库串行批处理、单文档 Map、按 slug Reduce、页面批量发布，以及知识条目 finalizing 的幂等登记与释放。

阶段二先使用 fake 上游适配器。Celery、Redis 锁、PostgreSQL pending-op 和 Outbox 使用真实实现，使后续接入真实知识/chunk 服务和模型网关时只替换适配器，不重写 Worker。

## 2. 范围

### 2.1 本阶段实现

- `wiki_pending_ops`、`task_outbox`、`wiki_finalization_markers` 数据模型和 Alembic 迁移。
- pending-op、Outbox 和 finalization marker 的同事务幂等入队。
- Outbox dispatcher 和 Celery Wiki 批次任务。
- Redis 随机 token 锁、Lua 比较续期和释放。
- 无 Redis 时只支持单 Worker 的进程内锁。
- fake 知识条目、chunk、模型和 finalizing 适配器。
- 文档内容重建、摘要生成、entity/concept 候选抽取。
- 单文档 Map 和按 slug Reduce。
- `summary`、`entity`、`concept` 页面写入及 `draft` 到 `published` 的批量发布。
- `/stats` 返回真实的 `pending_tasks` 和 `is_active`。
- Celery eager、fake 上游、可选真实 PostgreSQL/Redis 的自动化测试。

### 2.2 明确不实现

- chunk citation 和引用白名单恢复。
- `pg_trgm` 与 LLM 去重。
- 重解析替换、retract、删除 tombstone 和 dead-letter。
- taxonomy、自动交叉链接和 Index 简介更新。
- Agent 工具、Wiki 页面检索同步和 rerank boost。
- 真实知识服务、真实模型网关和 OpenAI-compatible HTTP 适配器。

以上能力分别属于后续阶段，不在阶段二中提前留下未完成入口。

## 3. 当前项目结构下的模块划分

不采用总设计文档中的独立 `python_wiki/src/wiki_app` 结构，继续使用当前目录：

```text
app/
  infrastructure/
    tasks/
      celery_app.py
  wiki/
    ingest/
      __init__.py
      ports.py
      fakes.py
      schemas.py
      store.py
      enqueue.py
      map_document.py
      reduce_slug.py
      worker.py
    tasks/
      __init__.py
      locks.py
      outbox_dispatcher.py
      wiki_tasks.py
      enqueue_fake.py
    models.py
    query_service.py
tests/
  wiki/
    test_ingest_schemas.py
    test_ingest_store.py
    test_ingest_enqueue.py
    test_ingest_map.py
    test_ingest_reduce.py
    test_ingest_worker.py
    test_task_locks.py
    test_outbox_dispatcher.py
    test_wiki_tasks.py
```

职责边界如下：

- `ports.py` 只定义外部能力协议，不包含业务流程。
- `fakes.py` 提供可编程内存 fake，并支持从 JSON fixture 加载数据。
- `schemas.py` 保存 fake 输入、LLM 结构化输出和 Map/Reduce 中间类型。
- `store.py` 负责 pending-op、Outbox、claim 和 finalization marker 的数据库操作。
- `enqueue.py` 只负责触发条件和原子入队。
- `map_document.py` 只负责单文档确定性预处理和模型调用。
- `reduce_slug.py` 只负责一个 slug 的页面合并决策。
- `worker.py` 负责编排锁、批次、并发、结果事务和后续调度。
- `tasks/` 只负责 Celery/Redis 边界，不承载领域算法。

## 4. 持久化模型

### 4.1 `wiki_pending_ops`

字段：

- `id`：UUID 主键。
- `tenant_id`、`knowledge_base_id`：租户和知识库范围。
- `knowledge_id`：fake 或未来真实知识条目 ID。
- `op`：阶段二固定为 `ingest`。
- `op_version`：上游解析版本；同一版本重复触发必须幂等。
- `payload`：JSONB，只保存任务所需的轻量元数据。
- `fail_count`：处理失败次数。
- `enqueued_at`、`claimed_at`、`claim_token`：批次领取状态。

唯一键为 `(tenant_id, knowledge_base_id, knowledge_id, op, op_version)`。

### 4.2 `task_outbox`

字段：

- `id`：UUID 主键。
- `tenant_id`、`knowledge_base_id`。
- `event_type`：阶段二使用 `wiki.batch.trigger`。
- `dedup_key`：由事件的 canonical tuple 计算得到的 64 字符 SHA-256 十六进制摘要。
- `payload`：Celery 任务参数。
- `available_at`：默认延迟 30 秒投递。
- `claimed_at`、`claim_token`：dispatcher 领取状态。
- `attempts`、`sent_at`、`created_at`。

唯一键为 `(tenant_id, knowledge_base_id, event_type, dedup_key)`。数据库直接隔离租户、知识库和事件类型，不依赖调用方把 scope 拼入字符串。

dispatcher 使用 `FOR UPDATE SKIP LOCKED` 分页领取；只有 Celery 发送成功后才写 `sent_at`。

### 4.3 `wiki_finalization_markers`

字段：

- `id`：UUID 主键。
- `tenant_id`、`knowledge_base_id`、`knowledge_id`。
- `attempt`：对应 `op_version`。
- `subtask_name`：固定为 `wiki`。
- `registered_at`、`released_at`。

唯一键为 `(tenant_id, knowledge_base_id, knowledge_id, attempt, subtask_name)`。登记与 pending-op 入队在同一事务完成；释放与成功结果提交或确定性跳过在同一事务完成，因此重复 Celery 投递不能重复释放。

## 5. 外部端口与 fake 适配器

### 5.1 `KnowledgeSourcePort`

提供以下异步能力：

- 查询 KB 是否启用 Wiki 及摄取配置。
- 查询知识条目的标题、解析版本和存活状态。
- 按顺序读取文本、OCR 和图片说明 chunk。
- 在 Map 前和最终写入前重新验证知识条目是否仍有效。

### 5.2 `ChatModelPort`

接收 Prompt 名称、渲染后的文本和 Pydantic 输出类型，返回已经验证的结构化结果。阶段二 fake 按调用类型返回预设的候选、摘要和页面合并结果，并可配置抛出瞬时或永久错误。

### 5.3 `FinalizationPort`

接收当前 SQLAlchemy `AsyncSession`，在调用方事务中登记或释放 finalization。阶段二的 SQL fake 通过 `wiki_finalization_markers` 实现；未来真实同库适配器可直接参与事务，异库适配器必须通过幂等事件桥接。

### 5.4 fake 数据加载

`GRAPH_WIKI_FAKE_DATA_FILE` 指向 JSON fixture。API、dispatcher 和 Worker 进程读取同一个只读 fixture，避免依赖进程内共享状态。fixture 包含 KB 配置、知识条目、chunks 及模型预设输出。

开发用入队入口是 `app.wiki.tasks.enqueue_fake`，不增加设计文档之外的公共 REST API。

## 6. 入队与 Outbox

`enqueue_ingest` 流程：

1. 从 `KnowledgeSourcePort` 读取 KB 和知识条目。
2. Wiki 未启用、来源无效、无有效文本或没有模型配置时，返回明确的跳过原因，不写任务。
3. 在一个数据库事务中幂等写 pending-op、finalization marker 和 Outbox。
4. 首次事件的 `available_at` 为当前时间加 30 秒。
5. 重复调用返回已有 pending-op，不新增 finalization marker 或 Outbox。

Outbox dispatcher 每次处理有限行数。发送失败时释放 claim 并保留事件；发送成功才记录 `sent_at`。多个 dispatcher 通过数据库行锁安全并行。超过 60 秒仍未发送的 claim 可以被其他 dispatcher 回收。

## 7. 锁和批次领取

Redis 锁键为 `wiki:active:{kb_id}`，值为随机 token：

- 获取：`SET key token NX EX 60`。
- 每 20 秒使用 Lua 比较 token 后续期。
- 释放时使用 Lua 比较 token 后删除。
- 旧 Worker 不能续期或删除新 Worker 的锁。

无 Redis 时使用按 KB 分片的进程内 `asyncio.Lock`，启动时记录只支持单 Worker 的告警。

Worker 获取锁后使用 `FOR UPDATE SKIP LOCKED` 领取最多 `ingest_batch_size` 个 pending-op，并为本批写同一个随机 `claim_token`。拿锁失败且仍有 pending-op 时抛出 `WikiBatchBusy`，Celery 在 15 秒后重试；已无 pending-op 时直接成功。

批次结果事务提交前再次统计当前 KB 的 pending-op。仍有待处理项时，在同一事务中幂等写入一个 `available_at = 当前时间 + 5 秒` 的后续 `wiki.batch.trigger` Outbox 事件，避免进程在提交后、调度前崩溃导致队列停滞。

## 8. Map 阶段

### 8.1 内容重建

- chunk 按 `chunk_index`、`start_at`、`id` 稳定排序。
- 依次合并文本、OCR 和图片说明。
- 候选抽取和摘要输入最多 32768 个 Unicode 字符。
- 去掉图片标记后少于 10 个有效文本字符时确定性跳过模型调用并释放 finalization。

### 8.2 结构化输出

fake 模型输出必须通过 Pydantic v2 校验：

- 候选名称不能为空。
- entity slug 必须以 `entity/` 开头。
- concept slug 必须以 `concept/` 开头。
- slug 必须规范化为小写，并限制为安全字符。
- aliases 清洗空值并保持稳定去重。
- `max_pages_per_ingest` 大于 0 时进行确定性截断。

Map 输出包括：

- 一个 `summary/{knowledge_id}` 整页替换更新。
- 零个或多个 entity/concept addition。
- 阶段二不生成精确 `chunk_refs`，entity/concept 内容使用候选 `details` 和文档摘要上下文，引用能力留到阶段三。

## 9. Reduce 和结果事务

Map 成功结果按 slug 分组。不同 slug 可并发调用 fake 合并模型，同一 slug 的多个文档贡献必须在一次 Reduce 中处理。

### 9.1 Summary

- slug 固定为 `summary/{knowledge_id}`。
- 标题为来源标题加“摘要”。
- 正文和 summary 整页替换。
- `source_refs` 只包含当前知识 ID，`chunk_refs` 为空。

### 9.2 Entity/Concept

- 新页面使用候选标题、类型和 aliases。
- 已有页面把旧正文、旧 summary 和本批新增上下文交给 fake 合并模型。
- 页面类型与 slug 前缀不一致时拒绝写入。
- fake 输出中的 Wiki 链接仍由阶段一链接解析器确定性投影到 `wiki_links`。

### 9.3 原子提交

模型调用结束后开启一个短结果事务：

1. 校验 pending-op 的 `claim_token`。
2. 再次验证所有待提交来源仍有效。
3. 创建或更新页面并设置为 `draft`。
4. 原子替换正文对应的链接边。
5. 将本批页面统一设置为 `published`。
6. 每个批次追加一条带稳定 `operation_id` 的日志。
7. 删除成功 pending-op。
8. 释放对应 finalization marker。

任何一步失败都回滚整个结果事务，不会暴露半成品页面。由于 pending-op 删除、日志、页面和 finalization 释放在同一事务中，重复投递不会重复增加页面版本或日志。

## 10. 失败处理

- 单个 Map 失败时，该 pending-op 不参与本次结果事务，`fail_count` 增加并回到 pending；其他成功文档继续处理。
- Reduce 失败时，所有贡献到该 slug 的 pending-op 本批不提交任何页面变化，增加 `fail_count` 后重试；其余不相关文档可以提交。
- 如果一个文档贡献多个 slug，其中任一 Reduce 失败，该文档全部更新均不提交，避免部分完成。Worker 会从剩余成功文档重新计算受影响的混合 slug。
- 瞬时模型错误最多重试 3 次，退避 2 秒、4 秒；fake 可精确模拟这些错误。
- 阶段二记录 `fail_count` 但不转 dead-letter，dead-letter 和最终失败释放在阶段三实现。
- Celery 任务异常不会直接确认 pending-op；claim 超时后可重新领取。
- Redis 锁续期失败时停止提交结果，防止失去所有权的 Worker 写入。

## 11. `/stats` 集成

阶段一的统计响应字段保持不变：

- `pending_tasks`：按租户和 KB 统计未完成 pending-op。
- `is_active`：Redis 锁存在时为真；内存模式查询当前进程锁状态。

查询必须同时带 `tenant_id` 和 `knowledge_base_id`，不能仅按 URL 中的 KB ID 统计。

## 12. 配置和运行方式

新增环境变量：

- `GRAPH_CELERY_BROKER_URL`、`GRAPH_CELERY_RESULT_BACKEND`。
- `GRAPH_REDIS_URL`。
- `GRAPH_WIKI_LOCK_MODE=redis|memory`。
- `GRAPH_WIKI_FAKE_DATA_FILE`。
- `GRAPH_WIKI_INGEST_BATCH_SIZE=5`。
- `GRAPH_WIKI_INGEST_MAP_PARALLEL=10`。
- `GRAPH_WIKI_INGEST_REDUCE_PARALLEL=10`。
- `GRAPH_WIKI_CLAIM_TIMEOUT_SECONDS=600`。
- `GRAPH_WIKI_MAX_PAGES_PER_INGEST=0`。
- `GRAPH_WIKI_EXTRACTION_GRANULARITY=standard`。

`docker-compose.yml` 增加 Redis、Wiki Worker 和 Outbox dispatcher；PostgreSQL 继续复用阶段一服务。

开发环境通过 fake fixture 入队：

```powershell
# 使用 fake fixture 为指定知识条目创建 Wiki ingest pending-op 和 Outbox 事件
python -m app.wiki.tasks.enqueue_fake --kb-id <知识库UUID> --knowledge-id <知识条目ID>
```

## 13. 测试策略

所有生产行为按 TDD 实现，测试先失败再写最小代码。

### 13.1 单元测试

- fake fixture 解析和端口契约。
- chunk 排序、内容合并、字符上限和短文本跳过。
- 候选 slug、类型和 aliases 校验。
- Summary/Entity/Concept Reduce。
- Redis 锁 token 所有权和内存锁行为。
- 模型瞬时错误分类和重试次数。

### 13.2 仓储与任务测试

- pending-op、Outbox、finalization marker 重复入队幂等。
- claim token 和 `SKIP LOCKED` 查询结构。
- dispatcher 只在发送成功后标记事件。
- Celery eager 模式执行完整批次。
- 重复投递不重复增加版本、日志或 finalization 释放次数。
- Map/Reduce 失败只重试相关 pending-op。
- 结果事务失败时 draft 页面不可见。
- `/stats` 的租户范围和真实队列状态。

### 13.3 可选真实基础设施测试

- `GRAPH_TEST_DATABASE_URL` 存在时运行 PostgreSQL 事务和并发领取测试。
- `GRAPH_TEST_REDIS_URL` 存在时运行 Lua 获取、续期、释放和锁冲突测试。
- 未配置外部服务时明确 skip，不允许静默假装通过。

## 14. 文档同步

阶段二实现完成后更新：

- `.env.example`：新增配置及中文说明。
- `README.md`：增加 Celery、Redis 和 fake fixture 的运行方法。
- `docs/Wiki阶段二.md`：记录真实实现的模块、配置、任务流程、限制和验证命令。

文档只描述已经实现并通过测试的能力，命令必须附中文用途说明。

## 15. 验收标准

- 同一 KB 同时最多运行一个批次。
- 重复 enqueue 只产生一个 pending-op、一个 finalization marker 和一个有效 Outbox 事件。
- Outbox 发送失败不会丢失任务。
- fake 文档能生成 published summary/entity/concept 页面。
- 普通列表看不到未完成 draft 页面。
- 重复 Celery 投递不重复增加页面版本或日志。
- 成功、确定性跳过的 finalization 各释放一次。
- Map/Reduce 或结果事务失败不会留下半套页面和链接。
- `/stats` 返回当前 KB 的真实 pending 和 active 状态。
- Viewer 权限和租户隔离不因 Worker 路径被绕过。
- 阶段一测试保持通过，阶段二新增测试全部通过。
