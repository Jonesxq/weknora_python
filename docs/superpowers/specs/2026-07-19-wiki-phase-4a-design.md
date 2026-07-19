# Wiki 阶段四 A：批次目录规划设计

> 状态：已确认设计，尚未实现。本文描述阶段四 A 的目标和约束，不代表当前 `main` 已具备这些能力。

## 1. 目标

在当前 `app/wiki` 和阶段三增量摄取结构上增加批次 taxonomy 与目录规划：

- 所有本批新建的 `entity`、`concept` 页面在 Reduce 前统一规划目录。
- 小目录树向 fake taxonomy 提供全部目录；大目录树使用 fake embedding 选择相关深层目录，同时保留全部一级目录。
- taxonomy 失败只影响关联的 pending-op，不阻塞无关文档。
- 最终目录创建、页面 `folder_id`、贡献、链接、日志和 pending-op 在同一 PostgreSQL 事务中提交。
- 保留现有 REST 请求和响应合同，不覆盖已有页面的人工目录位置。

## 2. 范围拆分

阶段四按三个连续子阶段实施：

- **4A：批次 taxonomy 与目录规划**，即本文范围。
- **4B：自动交叉链接，以及 Index、日志和 Graph 增强**。
- **4C：完整 Lint、auto-fix 和问题单维护**。

本文不提前实现 4B、4C，也不实现阶段五的 Agent、`WikiPageIndexer`、向量检索同步或真实模型适配。

## 3. 当前结构映射

阶段四 A 必须延续当前目录和职责，不照搬总设计文档中的示例项目结构：

| 当前文件 | 4A 职责 |
| --- | --- |
| `app/wiki/ingest/taxonomy.py` | 新增纯规划模块，负责候选选择、embedding 排序、taxonomy 分批和严格恢复 |
| `app/wiki/ingest/schemas.py` | 增加不可变 embedding、taxonomy、目录目录表和最终 assignment DTO |
| `app/wiki/ingest/ports.py` | 增加 `EmbeddingModelPort`、`TaxonomyModelPort`，扩展组合模型协议 |
| `app/wiki/ingest/fakes.py` | 增加严格 fake embedding 与 taxonomy 响应读取 |
| `app/wiki/ingest/worker.py` | 在现有 Map、Reduce 和预提交检查循环中加入 taxonomy 稳定计算 |
| `app/wiki/ingest/store.py` | 读取窄列目录目录表、识别真正新页面，并在结果事务内解析/创建目录 |
| `app/wiki/tasks/wiki_tasks.py` | 每个 runtime 注入独立 fake embedding/taxonomy 依赖，不缓存 event-loop 对象 |
| `examples/wiki_fake_data.json` | 保存确定性 embedding 向量和 taxonomy 输出 |

现有 `WikiFolder`、`WikiPage.folder_id`、活动同级目录唯一约束和目录 CRUD 已满足持久化基础，4A 不新增迁移。

## 4. 核心决策

### 4.1 路径规划与目录创建分离

Worker 在 Reduce 前只规划受约束的目录路径，不提前写数据库。`BatchApplyRequest` 携带最终 `FolderAssignment`，Store 在现有 `_apply_batch_results()` 事务中解析和创建目录。

这与总设计中“Reduce 前先获得稳定 `folder_id`”的字面顺序略有不同，但更符合当前统一结果事务：

- Reduce 模型本身不依赖数据库 UUID。
- 页面 CAS、claim 或贡献提交失败时不会留下空目录。
- `folder_id` 仍是唯一持久化真源，规划路径只存在于内部请求 DTO。

### 4.2 只分类真正新页面

只有同时满足以下条件的 slug 才参与 taxonomy：

- 页面类型为 `entity` 或 `concept`。
- 本批存在尚未排除的 contribution delta。
- 当前 scope 中不存在同 slug 的活动页面。
- 当前 scope 中也不存在待恢复的历史软删除页面。

Store 提供窄列分类上下文查询，同时返回目录目录表和真正可分类的 slug。Worker 的模型计算结果只是快照；Store 提交时必须再次检查页面状态。

已有页面无论 `folder_id` 是否为空都不重新分类。这样可以保护用户主动移动到根目录的页面，因为数据库无法仅通过空 `folder_id` 区分“人工根目录”和“尚未分类”。

### 4.3 fake 模型边界

4A 完整定义 embedding 与 taxonomy 端口，但 runtime 和测试只使用确定性 fake：

- 不发起网络请求。
- 不读取真实知识服务。
- 不把正文或 chunk 原文发送给 taxonomy。
- fake 输出缺失、维度错误、非有限数值或不满足白名单时明确失败，禁止隐式猜测。

## 5. 数据合同

所有新增 DTO 继承当前不可变严格模型模式，构造后深层数据不可变。

### 5.1 目录目录表

`FolderCatalogEntry` 包含：

- `id: UUID`
- `parent_id: UUID | None`
- `name: str`
- `path: str`
- `depth: int`，范围为 1 到 3

目录目录表必须按 `depth, path, id` 稳定排序，只读取非删除行和窄列，不读取页面正文。

`TaxonomyContext` 包含：

- `folders: tuple[FolderCatalogEntry, ...]`
- `classifiable_slugs: tuple[str, ...]`

Store 查询必须限定 `tenant_id + knowledge_base_id`，并检查同一路径、ID 和父子身份没有冲突。

### 5.2 Embedding 合同

`EmbeddingItem` 包含稳定 `key` 和有限长度 `text`。`EmbeddingRequest` 中 key 必须唯一。

`EmbeddingOutput` 使用 `key -> tuple[float, ...]` 映射，并满足：

- 完整覆盖请求 key，不能多也不能少。
- 全部向量维度一致且大于 0。
- 每个值都是有限浮点数，拒绝 `NaN` 和无穷值。
- 零向量允许存在，但余弦相似度固定视为 0。

`EmbeddingModelPort` 只暴露：

```python
async def embed(self, request: EmbeddingRequest) -> EmbeddingOutput: ...
```

### 5.3 Taxonomy 合同

`TaxonomyTopic` 只包含：

- `slug`
- `title`
- `page_type`
- `summary`

`AllowedFolderBase` 包含当前请求允许选择的既有目录 `id/path/depth`。

`TaxonomyRequest` 每批最多 60 个 topic，topic slug 和 allowed base ID 必须唯一。

`TaxonomyDecision` 包含：

- `slug`：必须来自当前请求。
- `base_folder_id: UUID | None`：非空时必须来自当前 allowed base 白名单。
- `new_segments: tuple[str, ...]`：最多两个待创建目录名。

`TaxonomyOutput` 必须对请求中的每个 slug 恰好返回一个 decision。显式 `base_folder_id=None, new_segments=()` 表示根目录；缺失 decision 不表示根目录。

### 5.4 最终目录分配

`FolderAssignment` 包含：

- `slug`
- `contributor_op_ids`
- `base_folder_id`
- `base_path` 和 `base_depth`：模型计算时看到的 base 快照；根目录分别为 `None` 和 `0`
- `new_segments`

`BatchApplyRequest` 新增 `folder_assignments`，默认空元组以保持阶段三内部调用兼容。

Store 必须验证：

- assignment slug 与最终新建、未删除的 entity/concept 结果页精确对应。
- `contributor_op_ids` 与同 slug `ReducedPage.contributor_op_ids` 精确一致。
- summary、已有页面、恢复的软删除页面、删除结果页不能应用 assignment。
- assignment 不得引用 failed 或 superseded operation。

## 6. 候选目录选择

### 6.1 小目录树

当活动目录数不超过 `taxonomy_full_catalog_limit` 时，把全部目录作为 allowed base，保持 `depth, path, id` 顺序。

### 6.2 大目录树

目录数超过阈值时：

1. 无条件保留全部一级目录。
2. 为每个 topic 构造 `title + summary` embedding 文本。
3. 为每个二级、三级目录构造完整 path embedding 文本。
4. 计算目录向量与任一 topic 向量的最大余弦相似度。
5. 按 `score DESC, depth ASC, path ASC, id ASC` 选择前 `taxonomy_related_folder_limit` 个深层目录。
6. 补齐所选目录的全部祖先。
7. 合并一级目录、相关目录和祖先后去重并稳定排序。

fake embedding 输出只用于候选筛选，不能直接决定最终目录。

默认配置：

- `taxonomy_topic_batch_size=60`，硬上限 60。
- `taxonomy_parallel=4`，最大 16。
- `taxonomy_full_catalog_limit=120`。
- `taxonomy_related_folder_limit=40`。

环境变量、`.env.example`、Compose 和 `WikiWorkerOptions` 的默认值必须一致。

## 7. 目录名和深度约束

模型新增目录名必须：

- 去除首尾空白后非空。
- 不包含 `/`、反斜杠或控制字符。
- 单段长度不超过数据库 `name` 字段限制。
- 不能是 `.` 或 `..`。
- 同一个 decision 中不能产生大小写折叠后重复的相邻段。

`base.depth + len(new_segments)` 不能超过 3；根目录 base 深度按 0 计算。模型最多新增两层，因此不会一次从根目录创建三级路径。

## 8. Worker 数据流

阶段三的 claim、KB 锁、Map 并发、Reduce 并发和最终 Store 调用保持不变。4A 在 `_process_claimed_batch()` 的稳定循环中加入以下步骤：

1. 根据 failures、superseded 过滤 contribution delta。
2. 加载 affected page 快照，以及目录 taxonomy context。
3. 从真正新建的 entity/concept slug 生成 taxonomy topic 和 contributor 集合。
4. 选择 allowed base；大目录树先调用 embedding。
5. 按最多 60 个 topic 分批调用 taxonomy。
6. 任一 taxonomy 分组失败时，把该分组全部 topic 的 contributor operation 标为失败。
7. failures 或 superseded 集合变化后，从步骤 1 重新计算，禁止复用 contributor 已变化的 assignment。
8. 对稳定后的 delta 执行现有 Reduce。
9. Reduce 产生新失败或预提交 ingest 检查产生 superseded 时，重新进入步骤 1。
10. exclusions、pages 和 folder assignments 同时稳定后构造一次 `BatchApplyRequest`。

这个外层固定点保证：

- 同一 operation 的任一 slug 失败后，其其他 slug 不会携带旧 assignment 提交。
- 共享 slug 的 contributor 变化后，assignment coverage 会重新生成。
- taxonomy 和 Reduce 的错误隔离都遵守现有 pending-op 失败计数。

## 9. 重试、取消和失败隔离

- embedding 与 taxonomy 的 `TransientModelError` 使用现有 2 秒、4 秒等待并最多尝试 3 次。
- `PermanentModelError`、非法模型结构和白名单违规不重试。
- 一个 taxonomy 分组失败时，只失败该分组主题涉及的 pending-op；无关分组继续。
- 一个 pending-op 贡献多个 topic 时，任一 topic 失败会排除该 operation 的全部 delta。
- 取消、KB 锁丢失、claim 丢失和页面 CAS 冲突仍属于控制流错误，不增加 fail_count。
- sibling embedding/taxonomy task 在异常或取消时必须 cancel，并完整 `gather(return_exceptions=True)`。
- 普通模型失败继续使用现有第五次 dead-letter 和 finalization 释放语义。

## 10. Store 原子事务

Store 在锁定 claim 和页面、确定实际可写结果页后处理目录：

1. 再次查询 assignment 对应 slug 的全部页面行，区分真正新建与历史恢复。
2. 历史软删除页面恢复时保留原 `folder_id`，不应用 taxonomy。
3. 锁定 assignment 引用的 base 目录，并验证 scope、active、path、depth 和父级身份与 `base_path/base_depth` 快照一致；目录被人工移动后必须报页面冲突并让 claim 重试。
4. 从浅到深解析 `new_segments`。
5. 每一级按 scope + `parent_id + name` 复用已有活动兄弟目录；不存在时插入目录。
6. 并发唯一冲突时按同一兄弟目录身份重新读取，并验证父级、名称、path 和深度完全一致；不一致则报冲突。
7. 把最终目录 UUID 写入新页面 `folder_id`；同时从目录链派生 `category_path`、`wiki_path` 和 `depth` 缓存。显式根目录写 `folder_id=None`、`category_path=[]`、`wiki_path=/{slug}`、`depth=0`。
8. 目录位置和缓存变化不额外增加页面版本；新页面仍使用现有初始版本规则。
9. 然后继续现有贡献、页面正文、链接、日志、finalization、pending-op、dead-letter 和 follow-up Outbox 提交。

任一错误必须回滚本批新增目录和全部现有结果变化。重复 operation replay 从 `WikiLogEntry.result_outcome` 返回，不重复创建目录或增加页面版本。

## 11. 并发与安全不变量

- 所有目录查询和写入必须包含 tenant 与 knowledge base 条件。
- 模型永远不能提供 tenant、KB、目录 UUID 以外的数据库身份。
- assignment 的 base UUID 必须来自当前请求白名单，Store 仍需重新校验数据库行。
- 目录创建依赖现有 `(knowledge_base_id, parent_id, name)` 活动兄弟部分唯一约束，不使用 path 索引或进程内缓存作为一致性依据。
- Worker 的 KB 锁不替代数据库不变量，因为人工目录 CRUD 可以并发发生。
- 不允许 taxonomy 改动已有页面 folder、页面正文、source refs、chunk refs 或页面版本。
- 新页面写入 `folder_id/category_path/wiki_path/depth` 本身不额外增加版本；仍按现有新建页面版本规则保存。

## 12. Fake fixture 与 runtime

`examples/wiki_fake_data.json` 增加：

- embedding key 到向量的严格映射。
- taxonomy 请求分组对应的 decision 列表。
- 至少一个复用既有目录、一个创建新目录、一个显式根目录的示例。

`FakeEmbeddingModel` 和 `FakeChatModel` 每次调用返回深拷贝快照，并支持按调用键配置瞬时失败和永久失败。

`WikiTaskRuntime` 每次构造独立 fake embedding 实例，并把它和组合 fake 模型注入 Worker。不得把 runtime、模型或 event-loop-bound 对象缓存在模块全局。

阶段四 A 继续只支持 fake 上游。真实 OpenAI-compatible embedding/taxonomy 适配不在本文范围。

## 13. 测试策略

### 13.1 Schema 与纯函数

- DTO 深层不可变、严格字段和序列化往返。
- embedding key 覆盖、维度、有限数、零向量。
- 小目录全量候选。
- 大目录 top-K、全部一级目录、祖先补齐和稳定 tie-break。
- 60 topic 切批。
- 未知 slug、重复或缺失 decision、非白名单 base。
- 非法名称、超过两段、超过三级和显式根目录。

### 13.2 Worker

- taxonomy 位于 Map 后、Reduce 前。
- summary、已有页面和历史恢复页不分类。
- taxonomy 单分组失败只排除关联 operation。
- 同一 operation 多 slug、共享 slug 和 Reduce 后新增失败会重新稳定 assignment。
- embedding/taxonomy 瞬时错误只重试三次。
- 取消和 sibling cleanup 不泄漏任务。
- Store 请求中的 assignment contributor 与页面精确一致。

### 13.3 Store 与真实 PostgreSQL

- 既有路径复用和缺失父子目录顺序创建。
- 显式根目录和最大深度。
- 新页面写入正确 `folder_id`，并同步派生 `category_path/wiki_path/depth`。
- 已有页面和恢复页面保留人工目录。
- 非法 base、跨 scope UUID、路径身份冲突和 contributor 不匹配全部回滚。
- 页面 CAS、claim 丢失、贡献冲突时目录不残留。
- 重复 operation 不重复目录、页面版本或日志。
- 并发创建同一路径得到唯一一致目录。

### 13.4 回归与静态验收

- 阶段一至三 REST、Map/Reduce、retract、dead-letter 合同保持通过。
- Alembic head 仍为 `20260719_04`，4A 不产生迁移。
- Python compileall、changed-file Ruff、Compose 解析通过。
- 无真实服务全量测试明确跳过 opt-in 集成项。
- 真实 PostgreSQL/Redis 全量测试全部通过。

## 14. 文档和配置

新增 `docs/Wiki阶段四A.md`，必须使用中文并只描述已经随实现落地的能力。文档包含：

- fake embedding 和 taxonomy fixture 结构。
- 小目录与大目录选择规则。
- 失败隔离和 dead-letter 关系。
- 新页面分类、已有页面保护和根目录语义。
- 当前限制：无真实模型、无自动链接、无完整质量修复、无 Agent/Indexer。

文档中的终端命令前必须有中文用途注释，例如：

```powershell
# 运行阶段四 A 的目录规划定向测试
uv run pytest tests/wiki/test_ingest_taxonomy.py -q
```

## 15. 验收标准

阶段四 A 完成时必须同时满足：

- 本批新 entity/concept 在一次批次 taxonomy 中得到稳定目录 assignment。
- 小目录使用全部路径，大目录保留全部一级目录并通过 fake embedding 选择相关深层路径。
- 模型输出只能引用请求白名单并最多新增两层，持久化深度不超过 3。
- taxonomy 分组失败只影响关联 pending-op，其他文档可完成。
- failures、superseded 或 Reduce 结果变化后不会提交旧 contributor assignment。
- 目录和页面结果在同一事务提交，失败不留下空目录。
- 已有页面、人工根目录页面和历史恢复页面不会被重新分类。
- 重复投递不重复创建目录、不增加页面版本或日志。
- 不修改现有 REST 合同，不引入数据库迁移。
- 全部新增和既有测试、静态检查以及真实 PostgreSQL/Redis 验收通过。
