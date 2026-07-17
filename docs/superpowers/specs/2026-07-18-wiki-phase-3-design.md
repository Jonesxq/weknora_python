# Wiki 阶段三设计

## 1. 目标

在已完成的 Wiki 阶段二基础上实现引用、去重和增量维护，同时保持当前 `app/` 项目结构与 REST API 合同不变。

阶段三继续使用 JSON fixture 驱动的 fake 知识来源和 fake LLM。PostgreSQL、Redis、Celery、Outbox、pending-op 和 finalization 使用真实实现。真实知识服务、真实模型网关和前端 citation 组件不在本阶段接入。

## 2. 范围

### 2.1 本阶段实现

- chunk citation 分批、并发、白名单恢复和稳定排序。
- PostgreSQL `pg_trgm` 候选查询与 fake LLM 去重判断。
- durable 页面贡献账本。
- 重解析时旧版本贡献的原子替换和 `retract_stale`。
- 删除时 Redis tombstone、同步最小清理和异步语义 retract。
- pending-op 第 5 次批次失败时原子进入 dead-letter。
- fake CLI 的 `ingest`、`retract` 增量入口。
- 阶段三迁移、单元测试、PostgreSQL/Redis 可选集成测试和中文运行文档。

### 2.2 明确不实现

- taxonomy 和目录规划。
- 自动交叉链接、Index intro 和新的 Graph 能力。
- Lint、auto-fix 和问题单生成。
- Agent 工具和 `WikiPageIndexer`。
- Wiki chunk 的 1.3 倍 rerank boost。
- dead-letter 公开查询、重放、自动过期或管理 API。
- 新的公开 REST API。
- 正文中的自定义 citation 语法或 Vue citation 组件。

## 3. 已确认的设计决策

1. 阶段三继续使用 fake 上游。
2. 删除与重解析通过内部 service 和 CLI 触发，不新增 REST API。
3. 新增规范化的 `wiki_page_contributions`，不把来源贡献塞进页面 metadata。
4. 第 5 次批次失败即进入 dead-letter，并在同一事务释放 finalization。
5. 模型内部使用 `cNNN` chunk alias；最终页面只保存结构化 `chunk_refs`，正文暂不增加新语法。
6. 页面是贡献集合的派生结果；贡献账本是增量维护的事实来源。

## 4. 当前结构中的模块边界

阶段三沿用现有目录，不复制总设计文档中的新项目结构。

### 4.1 新增模块

- `app/wiki/ingest/citations.py`
  - chunk 分段与分批。
  - 当前批次 `cNNN` alias 映射。
  - citation 输出白名单校验和真实 chunk ID 恢复。
- `app/wiki/ingest/dedup.py`
  - trigram 候选 DTO 和查询结果整理。
  - fake LLM 去重决策的白名单、类型和循环校验。
- `app/wiki/ingest/retract.py`
  - 旧贡献与新贡献的集合差量。
  - `add`、`replace`、`retract_stale` 和 `retract` 计划。
  - retract 后页面来源和 chunk 引用的确定性投影。

### 4.2 修改模块

- `app/wiki/models.py`：新增贡献和 dead-letter ORM。
- `app/wiki/ingest/schemas.py`：新增 citation、dedup、贡献差量、失败和结果事务 DTO。
- `app/wiki/ingest/ports.py`：扩展模型能力并新增 tombstone 协议。
- `app/wiki/ingest/fakes.py`：解析 citation 和 dedup fixture 响应。
- `app/wiki/ingest/enqueue.py`：提供 ingest/retract 增量服务。
- `app/wiki/ingest/map_document.py`：引用、去重和旧 slug 连续性。
- `app/wiki/ingest/reduce_slug.py`：接收贡献差量和剩余贡献上下文。
- `app/wiki/ingest/store.py`：贡献、最小清理、dead-letter 和扩展结果事务。
- `app/wiki/ingest/worker.py`：按 op 分派 Map、失败分类和 dead-letter。
- `app/wiki/tasks/tombstones.py`：Redis/内存 tombstone 实现；不得复用跨事件循环 Redis client。
- `app/wiki/tasks/wiki_tasks.py`：每批 runtime 注入增量 service 和 tombstone port，不缓存 loop-bound 对象。
- `app/wiki/tasks/enqueue_fake.py`：增加 `--op ingest|retract`。

## 5. PostgreSQL 数据模型

阶段三新增迁移 `20260718_03`，依赖阶段二 revision `20260714_02`。

### 5.1 `wiki_page_contributions`

每行保存一个来源版本对一个页面 slug 的原始贡献。summary、entity 和 concept 都写贡献行；summary 页面因此也能参与重解析替换和删除撤回。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | UUID | 主键 |
| `tenant_id` | BIGINT | 租户范围 |
| `knowledge_base_id` | UUID | 知识库范围 |
| `slug` | VARCHAR(255) | summary/entity/concept slug |
| `knowledge_id` | VARCHAR(255) | 来源知识条目 |
| `op_version` | VARCHAR(255) | 来源解析版本 |
| `page_type` | VARCHAR(32) | summary/entity/concept |
| `state` | VARCHAR(32) | `active` 或 `retract_pending` |
| `title` | VARCHAR(512) | 此贡献建议的页面标题 |
| `content` | TEXT | 原始贡献正文或候选 details |
| `summary` | TEXT | 文档摘要上下文 |
| `aliases` | JSONB | 此贡献携带的 aliases |
| `chunk_refs` | JSONB | 白名单恢复后的真实 chunk ID |
| `created_at` | TIMESTAMPTZ | 首次写入时间 |
| `updated_at` | TIMESTAMPTZ | 状态或内容更新时间 |

约束和索引：

- 唯一约束：`tenant_id + knowledge_base_id + slug + knowledge_id + op_version`。
- 活跃贡献部分唯一索引：同一 `tenant + KB + slug + knowledge_id` 同时最多一个 `active` 版本。
- `(tenant_id, knowledge_base_id, slug, state)` 索引用于 Reduce。
- `(tenant_id, knowledge_base_id, knowledge_id, state)` 索引用于重解析和删除。
- JSONB 数组仍使用稳定去重后的字符串列表，不接受空 ID。

重解析结果提交时，在同一事务删除当前来源的旧版本贡献，再插入新版本。删除结果提交时物理删除 `retract_pending` 贡献。事务提交前旧页面和旧贡献保持可见。

### 5.2 `wiki_dead_letters`

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | UUID | 主键 |
| `pending_op_id` | UUID | 原 pending-op，唯一 |
| `tenant_id` | BIGINT | 租户范围 |
| `knowledge_base_id` | UUID | 知识库范围 |
| `knowledge_id` | VARCHAR(255) | 来源知识条目 |
| `op` | VARCHAR(32) | `ingest` 或 `retract` |
| `op_version` | VARCHAR(255) | 原操作版本 |
| `payload` | JSONB | 原始可信 payload |
| `fail_count` | INTEGER | 固定为达到阈值时的计数，至少为 5 |
| `last_error_code` | VARCHAR(128) | 稳定、非敏感错误码 |
| `last_error_summary` | VARCHAR(2000) | 清洗后的有限摘要 |
| `dead_at` | TIMESTAMPTZ | 进入死信时间 |

约束和规则：

- `pending_op_id` 唯一，重复 Worker 投递不会重复写死信。
- dead-letter 不设 TTL。
- 不记录 claim token、模型原始响应或异常 traceback。
- 阶段三只提供 store 内部读取能力和测试，不增加公共 API。

### 5.3 去重索引

现有 `pg_trgm` 扩展继续复用。迁移新增 active entity/concept 页面的名称检索表达式 GiST trigram 索引，表达式覆盖规范化标题和 aliases 文本；候选查询使用同一表达式的 trigram 距离排序后取 top-20。

候选查询必须同时限定：

- `tenant_id`。
- `knowledge_base_id`。
- `deleted_at IS NULL`。
- `status = published`。
- 相同 `page_type`。

每个候选名称或 alias 最多返回 top-20；调用方做稳定去重后再交给 fake LLM。

## 6. DTO 和端口

### 6.1 模型端口

保留现有摘要、候选抽取和页面合并方法，增加：

- `CitationModelPort.classify_chunks(request) -> CitationBatchOutput`
- `DedupModelPort.resolve_duplicates(request) -> DedupOutput`
- 组合协议 `WikiIngestModelPort`，由 `FakeChatModel` 实现全部能力。

`CitationBatchRequest` 包含 knowledge 身份、批次序号、当前候选和带 alias 的 chunk 文本。`CitationBatchOutput` 包含 slug 到 chunk aliases 的映射，以及可选的补充候选。

`DedupRequest` 对每个新候选提供同类型、当前 scope 内的白名单页面。`DedupOutput` 只能选择白名单中的 canonical slug 或返回不合并。

### 6.2 tombstone 端口

```python
class TombstonePort(Protocol):
    async def mark_deleted(self, scope: WikiScope, knowledge_id: str) -> None: ...
    async def is_deleted(self, scope: WikiScope, knowledge_id: str) -> bool: ...
```

Redis key 固定为 `wiki:deleted:{knowledge_base_id}:{knowledge_id}`，值固定为非敏感标记 `1`，TTL 默认 3600 秒。knowledge base UUID 作为全局身份；所有入口仍必须同时按 tenant 和 knowledge base 验证数据库范围，不能只凭 Redis key 授权操作。

Redis 连接使用有限超时，每次调用或每个短租约创建独立 client，不跨事件循环缓存连接。内存实现仅供单进程测试和显式开发模式。

### 6.3 贡献和失败 DTO

- `ContributionDelta`：`add`、`replace`、`retract_stale`、`retract`。
- `StoredContributionRecord`：数据库贡献快照。
- `OperationFailure`：pending-op ID、稳定错误码、有限摘要。
- `BatchApplyRequest`：最终页面、贡献差量、completed IDs、失败记录、页面 CAS 快照和稳定 operation ID。

使用 `BatchApplyRequest` 收束扩展后的结果事务参数，避免继续增加 `apply_results()` 的位置参数。

## 7. chunk citation

### 7.1 分批

- 使用原始有序 chunks，不使用摘要输入的 32768 字符截断结果。
- 单批最多约 12000 Unicode 字符。
- 超长单 chunk 按字符稳定切片；多个片段仍映射到同一个真实 chunk ID。
- 每批独立生成从 `c000` 开始的局部 alias，alias 不能跨批复用。
- 同一文档最多 4 个 citation 批次并发，更多批次通过 semaphore 排队，不丢弃。

### 7.2 白名单恢复

- 模型返回的 alias 必须属于当前批次。
- slug 必须来自已验证候选或当前输出中通过 Schema 校验的补充候选。
- alias 恢复为当前 `SourceChunk.id` 字符串；当前项目不强制 chunk ID 为 UUID。
- 同一真实 chunk 被多个片段选中时只保留一次。
- 最终 chunk refs 按原始 `chunk_index/start_at/id` 顺序稳定排列。

### 7.3 失败语义

- citation 单批瞬时错误沿用 3 次模型重试。
- 重试耗尽或永久错误只降级当前批次。
- 其他成功批次继续贡献 refs。
- 没有精确 refs 时仍可使用候选 `details`；不得伪造 chunk ID。
- 文档摘要失败仍使整个 ingest pending-op 失败。

## 8. 去重

### 8.1 候选生成

- 完成候选抽取和 citation 补充候选后统一执行。
- exact slug 已存在时直接保持 canonical 身份，不调用去重模型。
- 对其余 entity/concept 候选按名称和 aliases 查询同类型 top-20 页面。
- 当前文档内部先按规范化 slug 和名称稳定去重。

### 8.2 fake LLM 决策

模型可以选择白名单中的现有 slug，或明确不合并。

确定性代码拒绝：

- 白名单外目标。
- entity 到 concept 或 concept 到 entity。
- 目标为 summary 或其他页面类型。
- generated slug 自映射伪装成去重决策。
- 一个决策链再次指向另一个 generated slug。
- 循环映射。

通过只允许目标为数据库已存在的 canonical 页面，可从结构上消除 generated-to-generated 循环。多个新候选选择同一 canonical slug 时，先稳定合并 aliases、details 和 chunk refs，再生成一个贡献差量。

不确定时保留新 slug。

## 9. Map 数据流

1. 读取 knowledge、配置和 chunks。
2. 检查来源状态与 tombstone。
3. 读取该 knowledge 当前 active 贡献的旧 slug。
4. 启动文档摘要；同时执行候选抽取。
5. 候选确定后执行 citation 批次，最多 4 个并发。
6. 合并 citation 补充候选。
7. 查询 trigram 候选并执行 fake LLM 去重。
8. 比较旧 active 贡献与新贡献：
   - 新增 slug：`add`。
   - 重合 slug 且 op_version 或内容变化：`replace`。
   - 旧有但新结果缺失：`retract_stale`。
9. summary 生成 `add` 或 `replace` 贡献及整页投影；entity/concept 生成 add/replace/retract_stale 贡献差量。

重解析时旧贡献只作为上下文读取。Map/Reduce 全部成功前不删除旧贡献。

## 10. 增量入口

### 10.1 内部 service

新增或扩展 `WikiIncrementalService`：

- `enqueue_ingest(scope, knowledge_id)`
- `enqueue_retract(scope, knowledge_id, op_version)`

ingest 继续验证来源 active、文本和模型配置。retract 不要求来源仍可读取，调用方提供可信 knowledge ID 和版本。

### 10.2 fake CLI

现有 CLI 增加：

```text
--op ingest|retract
```

默认 `ingest` 以兼容阶段二用法。CLI 仍从结构化 fixture 确定 tenant，不增加任意 tenant 参数。retract 允许 fixture 中知识状态为 `deleting`、`cancelled` 或 `deleted`。

重解析的开发流程是更新 fixture 中该知识条目的 `op_version`、chunks 和 fake 响应，然后再次执行 ingest。

## 11. 删除语义

### 11.1 入队顺序

1. `TombstonePort.mark_deleted()` 成功写入 1 小时 tombstone。
2. PostgreSQL 事务取消该来源所有未 claim 的 ingest pending-op。
3. 把该来源 active 贡献标记为 `retract_pending`。
4. 执行确定性最小清理。
5. 幂等写 retract pending-op、finalization marker 和触发 Outbox。

Redis 与 PostgreSQL 无法组成同一事务。tombstone 先成功、数据库事务后失败时，后续重试仍可幂等完成；短期 tombstone 假阳性比并发 ingest 写回已删除来源更安全。Redis 写失败时不能假装 retract 入队成功。

### 11.2 确定性最小清理

- 页面只有当前删除来源时立即软删除，并清理其可见链接边。
- 页面仍有其他 active 来源时，立即重算 `source_refs/chunk_refs`，排除 `retract_pending` 贡献。
- 多来源页面正文暂时保留，等待 Worker 语义撤回。
- 最小清理造成的用户可见状态或 refs 变化必须增加页面 version。
- 原贡献仍保留为 `retract_pending`，用于语义撤回和 dead-letter 后人工诊断。

### 11.3 Worker retract

retract Map 不读取已删除来源正文，只加载 `retract_pending` 贡献和同 slug 的剩余 active 贡献。

- 没有剩余贡献：页面保持软删除，成功后删除待撤回贡献。
- 仍有贡献：Reduce 接收旧页面、被撤回贡献和剩余贡献摘要，移除仅由删除来源支持的内容。
- retract Reduce 失败时不进一步修改页面或贡献；确定性最小清理不回滚。
- 成功时删除待撤回贡献，更新页面正文、refs、链接、日志和版本。

## 12. 重解析语义

- 不写 tombstone。
- 新 ingest 入队时取消同来源所有未 claim 的旧 ingest pending-op。
- 已 claim 的旧 ingest 由 KB 锁串行执行；新版本随后替换，避免并发结果事务。
- 旧 active 贡献在新版本成功前继续服务。
- 新结果事务对受影响 slug 执行旧贡献删除和新贡献插入。
- `retract_stale` 与新贡献更新在同一事务完成。
- Summary 页面按 `summary/{knowledge_id}` 整页替换。

如果删除与旧 ingest 并发，tombstone 和提交前第二次来源检查使旧 ingest 以“已被 supersede”的成功清理结束，不把它计入 dead-letter，也不提交新增页面。

## 13. Reduce 与结果事务

### 13.1 affected slug

Worker 对 contribution delta 涉及的 slug 分组。每个 slug 同时读取：

- ExistingPageRecord CAS 快照。
- 当前 active 贡献。
- 本批 add/replace。
- 本批 retract/retract_stale。

只重算 affected slugs，不扫描 KB 全部页面。

### 13.2 页面投影

- Summary：单来源整页替换；retract 后软删除。
- Entity/concept 没有剩余 active 贡献：软删除。
- Entity/concept 仍有贡献：模型基于旧页、差量和剩余贡献上下文合并。
- `source_refs` 是 active 贡献 knowledge IDs 的稳定去重结果。
- `chunk_refs` 是按 knowledge ID、op_version 和贡献内原始 chunk 顺序合并的稳定去重结果。
- 页面正文暂不写入自定义 citation 标记。

### 13.3 原子提交

扩展后的结果事务一次完成：

1. 验证 claim token、completed/failed/superseded 操作集合。
2. 再次验证 ingest 来源 active 且无 tombstone。
3. 锁定页面并检查 `id + version` CAS；期望不存在也必须仍不存在。
4. 应用 contribution 删除、状态变化和插入。
5. 创建、更新、恢复或软删除页面。
6. 原子替换正文派生链接边。
7. 写摄取或 retract 日志。
8. 删除 completed/superseded pending-op并释放 finalization。
9. 对普通失败增加失败计数，或在达到阈值时移入 dead-letter。
10. 取消 claim recovery，并在仍有 pending 时写 follow-up Outbox。

任一步失败回滚整个结果事务。LLM、Redis 和 source 调用不得发生在该事务内。

## 14. dead-letter

### 14.1 阈值

- 初始 `fail_count = 0`。
- 每次批次级 Map/Reduce 永久失败或瞬时重试耗尽后加 1。
- 加 1 后达到 5 时立即进入 dead-letter，不再回 pending。
- `WikiBatchBusy`、PageConflict、claim 丢失、进程取消、锁丢失和 superseded ingest 不消耗此计数。

### 14.2 原子转移

同一事务：

1. 插入 `wiki_dead_letters`。
2. 删除 pending-op。
3. 取消 claim recovery Outbox。
4. 释放 finalization marker。
5. 如 KB 仍有其他 pending，写 follow-up Outbox。

retract 进入 dead-letter 时保留 `retract_pending` 贡献和确定性最小清理结果。阶段三不自动恢复它。

## 15. Redis tombstone

- 默认 TTL：3600 秒。
- 相同删除重复调用只续写/刷新 tombstone，不创建重复 pending-op。
- ingest 在 Map 开始和结果提交前都检查 tombstone。
- retract 不因 tombstone 被跳过。
- tombstone 自然过期；阶段三不提供手工清除命令。
- Redis 操作使用有限超时，异常视为可重试的入口或 Worker 失败，不静默降级。

## 16. fake fixture

在现有 JSON 结构上增加：

- citation 响应：按 knowledge ID 和批次序号提供 slug 到 `cNNN` aliases 的映射。
- citation 补充候选。
- dedup 响应：按新候选 slug 选择 canonical slug 或 null。
- citation/dedup/merge 的瞬时失败计数。

fixture 继续拒绝未知字段。加载时能验证的静态错误立即拒绝；依赖运行时白名单的错误由调用阶段拒绝。不得在 fixture 中直接信任真实 chunk ID 或 canonical slug。

## 17. 并发、取消和错误

- citation 批次并发由独立 semaphore 限制为 4。
- 文档 Map 和 slug Reduce 继续受现有全局并发参数限制。
- 子任务自身取消或父任务取消时，必须取消并完整收集兄弟任务，再原样传播 `CancelledError`。
- 只有 `TransientModelError` 使用 3 次、2/4 秒重试。
- 模型 Schema 错误、白名单错误和跨类型去重为永久错误。
- citation 单批永久错误降级；摘要、去重主决策和页面 Reduce 永久错误使当前 pending-op 失败。
- 错误日志只记录 scope、操作 ID、受限错误码和类型，不记录 claim token、完整 payload、chunk 原文或模型原始输出。

## 18. 配置

新增安全默认值：

- `GRAPH_WIKI_CITATION_BATCH_CHARS=12000`
- `GRAPH_WIKI_CITATION_PARALLEL=4`
- `GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT=20`
- `GRAPH_WIKI_TOMBSTONE_TTL_SECONDS=3600`

dead-letter 阈值固定为 5，不作为环境变量，避免不同 Worker 配置不一致。

## 19. 测试

### 19.1 单元测试

- chunk 稳定分段、12000 字符边界和超长 chunk。
- 每批 alias 从 `c000` 开始且不能跨批使用。
- alias 白名单、真实 ID 恢复、稳定去重和原文顺序。
- citation 最大 4 并发、单批失败降级和取消清理。
- dedup top-20 输入整理、白名单、同类型、自映射和循环拒绝。
- old/new contribution 的 add/replace/retract_stale 差量。
- refs 只由 active 贡献投影。
- 第 5 次失败阈值。

### 19.2 PostgreSQL 测试

- ORM 与迁移字段、约束和索引一致。
- contribution 版本唯一和 active 部分唯一。
- tenant/KB 范围化 trigram 候选和相同 page_type。
- 重解析旧贡献替换与页面 CAS 原子回滚。
- 删除最小清理、retract_pending 和成功物理删除。
- 第 4 次失败回 pending，第 5 次原子进入 dead-letter。
- dead-letter 与 finalization 释放、recovery 取消和 follow-up 同事务。
- 重复投递不重复贡献、页面版本、日志或死信。

### 19.3 Worker 测试

- 首次 ingest 产生结构化 chunk refs。
- 两个文档贡献同一 canonical slug。
- 去重选择已有页面和非法选择隔离。
- 重解析 replace 和 retract_stale。
- 删除唯一来源软删除页面。
- 删除部分来源保留页面并撤回内容。
- tombstone 阻止并发旧 ingest 提交。
- citation 批次失败降级，摘要失败重试。
- retract Reduce 失败保持最小清理并最终 dead-letter。
- Worker/Outbox 重复投递幂等。

### 19.4 Redis 和合同测试

- tombstone TTL、重复刷新、跨 client 可见性和超时。
- memory tombstone 明确只支持单进程测试。
- CLI `--op` 参数、默认 ingest、retract scope 和 JSON 输出。
- 现有 REST API 响应字段不变。
- 所有阶段一、二测试继续通过。

真实 PostgreSQL/Redis 测试仍由环境变量显式启用；未配置时明确 skip。

## 20. 文档与运行

阶段三完成时更新：

- `.env.example` 的 citation/dedup/tombstone 配置。
- Compose Worker 的必要环境变量或代码默认值说明。
- `docs/Wiki阶段三.md`，包含迁移、fixture、ingest/retract CLI、重解析、删除、dead-letter 和限制。
- README 的阶段三链接。

文档中的终端命令必须有中文注释，且不能声称阶段四、五能力已实现。

## 21. 核心不变量

1. LLM 返回的 chunk alias 和 dedup 目标必须经过当前请求白名单校验。
2. 页面 `source_refs/chunk_refs` 只能由 active 贡献投影。
3. 重解析成功前旧 active 贡献不能提前删除。
4. 删除 tombstone 生效后，旧 ingest 不能提交新增页面或贡献。
5. retract pending 失败不能恢复已删除来源的可见 refs。
6. 页面、贡献、链接、日志、pending/dead-letter 和 finalization 的结果变化必须原子提交。
7. 第 5 次普通批次失败必须进入 dead-letter，并且 finalization 只释放一次。
8. superseded ingest、锁冲突、PageConflict、取消和 claim 恢复不消耗 dead-letter 失败次数。
9. 同一 KB 仍然最多一个 Worker 批次持有提交权。
10. 所有读取和写入同时限定 tenant 和 knowledge base。
11. 只重算 affected slugs，不扫描整个 KB 的页面正文。
12. 页面 version 只在用户可见内容、状态或受保护来源引用发生变化时递增。

## 22. 验收标准

- fake 首次 ingest 能生成 summary/entity/concept，并保存真实 chunk refs。
- fake 去重只能合并到同 scope、同类型、白名单内的已存在页面。
- 重解析成功后旧版本贡献不存在，旧 slug 缺失时完成 retract_stale。
- 删除唯一来源后页面不可见；删除部分来源后页面保留且 refs 不含已删除来源。
- 删除与旧 ingest 竞态时，旧 ingest 不提交新贡献。
- 普通失败第 4 次仍可重试，第 5 次进入唯一 dead-letter 并释放 finalization。
- 重复 Celery/Outbox 投递不重复页面版本、日志、贡献或 dead-letter。
- 现有阶段一、二 API 和测试保持通过。
- 迁移离线 SQL、Python compileall、Compose 解析和全量测试通过。
