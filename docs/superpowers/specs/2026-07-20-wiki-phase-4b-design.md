# Wiki 阶段四 B：自动交叉链接、Index 简介与 Graph/日志增强设计

> 状态：设计已确认，尚未实现。本文描述阶段四 B 的目标和约束，不代表当前 `main` 已具备这些能力。

## 1. 目标

在阶段四 A 已完成的批次 taxonomy、原子目录写入和增量摄取固定点之上，实现完整阶段四 B：

- 对本批最终成功写入的页面执行确定性自动交叉链接，不调用模型。
- 使用严格 fake 模型首次生成或增量更新 Index 简介，不接入真实网络模型。
- 让正文中的安全 Wiki 链接、结构化 `WikiLink` 和 Graph 查询保持一致。
- 保留现有批次幂等日志，在同一日志中记录自动链接和 Index 处理结果。
- 保持现有 REST 请求、响应、数据库表结构和阶段一至四 A 行为不变。

阶段四 B 不扫描并重写整个知识库。历史页面的长尾缺失交叉引用、broken-link 自动清理、完整 Lint/auto-fix 和问题单生成属于阶段四 C。

## 2. 当前结构与缺口

当前代码已经具备以下基础：

- `app/wiki/models.py` 已定义 `WikiLink`、`WikiLogEntry` 和 `WikiPageIssue`。
- `SqlAlchemyPageStore.replace_page_links()` 和 ingest Store 已能从正文重建链接投影。
- `WikiQueryService` 已提供 Index、Log、Graph、Lint 和链接重建接口。
- Graph Overview 已返回 top-N 节点之间的边，Ego 已使用入边和出边进行无向 BFS。
- ingest Store 已在同一结果事务内提交页面、链接、日志、pending-op、finalization 和 Outbox。
- `WikiIngestWorker` 已实现 taxonomy、Reduce 和 precommit 的稳定固定点。

阶段四 B 的实际缺口是：

- 当前正文只保留模型或用户显式写入的 Wiki 链接，没有自动交叉链接。
- `extract_wiki_links()` 使用简单正则，无法排除代码块等受保护 Markdown 区域。
- Index API 能读取 Index 页面简介，但 ingest 流程不会创建或更新该简介。
- Graph degree 计算没有完整限定活动、published 的来源页和目标页。
- 批次日志没有记录自动链接和 Index intro 的处理结果。

## 3. 架构决策

### 3.1 在现有结果事务内完成增强

阶段四 B 不增加提交后维护任务，也不新增 Outbox 事件类型。Worker 负责模型规划，Store 负责最终一致性写入：

1. Worker 先完成现有 taxonomy、Reduce 和 precommit 固定点。
2. 固定点稳定后，Worker 加载一次 Index intro 上下文。
3. Worker 使用 strict fake 生成 `IndexIntroPlan`；模型失败在 Worker 内降级，不形成 operation failure。
4. `BatchApplyRequest` 携带可选 Index 计划进入 Store。
5. Store 确定实际完成、失败和 superseded operation 后，只处理最终成功页面。
6. Store 先按未装饰正文计算页面内容版本，再执行自动交叉链接。
7. Store 用装饰后正文重建 `WikiLink`，应用或跳过 Index 计划，并写增强后的批次日志。
8. 页面、链接、Index、贡献、日志、pending-op、finalization 和 Outbox 在同一事务提交。

这样可以避免正文已经出现自动链接而 Graph 仍读取旧边，也不会在页面 CAS、claim 或目录写入失败时留下半完成的增强结果。

### 3.2 不改变 REST 和数据库结构

阶段四 B 不新增迁移。现有 `wiki_pages`、`wiki_links` 和 `wiki_log_entries` 字段足以承载结果：

- Index intro 仍存储在 `page_type=index` 的系统页面中。
- 自动链接仍存储在页面 Markdown 正文中。
- Graph 继续读取 `wiki_links`。
- Log 继续使用现有稳定自增游标和 `result_outcome`。

Index、Graph 和 Log REST DTO 不增加字段。增强状态只进入批次日志的 `message` 和 `pages_affected`。

## 4. 自动交叉链接

### 4.1 纯函数模块

新增 `app/wiki/linkify.py`，负责：

- 扫描 Markdown 受保护区间。
- 解析安全位置中的现有 Wiki 链接。
- 按候选引用装饰正文。
- 返回装饰后的正文、是否变化以及本次新增的目标 slug。

该模块不访问数据库，不调用模型，不修改传入候选集合。`app/wiki/domain.py` 的链接解析入口改为复用该 scanner，避免自动链接和结构化边使用两套规则。

### 4.2 候选范围

只改写本批最终成功、未删除且非 `index/log` 的受影响页面。每页候选来自：

1. 本批最终成功写入的非系统页面的 `title + aliases`。
2. 该页面在本批写入前已有出链目标的 `title + aliases`。

候选目录查询只读取 slug、title、aliases、page type 和状态等窄列，并限定 `tenant_id + knowledge_base_id`。候选目标必须活动且 published；软删除、archived、Index 和 Log 页面不能成为自动链接目标。

本批失败或 superseded operation 独占的页面不能进入 fresh candidate 集。共享 slug 只要最终仍由成功 contributor 产出，就可以作为候选。

### 4.3 匹配和歧义规则

- 候选显示文本去除首尾空白后必须非空。
- 同一 slug 的重复 title/alias 去重。
- 同一显示文本若指向多个 slug，则该文本视为歧义并整体排除，禁止任意选择目标。
- 候选按 Unicode 字符长度降序排列，再按显示文本和 slug 稳定排序。
- 每个 slug 最多包装第一次安全出现。
- 当前页面自身的 slug 永不链接。
- 正文安全区域中已存在该 slug 的 Wiki 链接时，所有同 slug title/alias 候选都跳过。
- 匹配保持大小写敏感，不执行模糊搜索或词形推断。
- ASCII 名称要求两侧满足单词边界，避免把 `AI` 插入 `TRAINING`。
- CJK 或其他非 ASCII 显示文本按字符位置匹配，不强制 ASCII 单词边界。
- 插入格式统一为 `[[slug|原显示文本]]`。

### 4.4 受保护 Markdown 区域

scanner 必须保留原始 Markdown 字节内容，不通过 AST 重新渲染。以下区域禁止插入自动链接：

- 反引号或波浪线 fenced code block。
- 使用相同反引号 run 闭合的 inline code。
- 已存在的 `[[slug]]` 和 `[[slug|display]]`。
- Markdown inline link 和 image。
- reference-style link。
- reference link definition 整行。
- `<scheme://...>` 等 autolink。

插入一个链接后必须同步平移受保护区间，后续候选不能再次匹配刚插入的 markup。重复运行相同候选必须返回完全相同正文并报告无变化。

### 4.5 链接投影一致性

`extract_wiki_links()` 改为复用 protected-span scanner：

- 只解析正文安全区域中的 Wiki 链接。
- 代码块或 inline code 中的 `[[...]]` 不形成 `WikiLink`。
- 结果仍按正文出现顺序稳定去重并规范化 slug。
- 非法或不安全 slug 继续忽略。

页面服务、ingest Store 和 `rebuild_links()` 全部通过同一入口重建投影。

## 5. 页面版本语义

Store 对既有页面使用两个内容值：

- `semantic_content`：Reduce 得到、尚未自动装饰的正文。
- `persisted_content`：执行自动交叉链接后的最终正文。

版本判断发生在自动装饰之前：

- title、semantic content、summary、status 等现有用户可见字段变化时，沿用当前规则增加版本。
- 自动链接在同一次写入中增加或不增加 markup，不再额外增加一次版本。
- 若本批页面语义字段均未变化、只有自动链接新增，则保持原版本。
- 新页面仍以 `version=1` 创建。

结构化链接投影和 Graph 变化永不单独增加页面版本。

## 6. Index intro 合同

### 6.1 上下文 DTO

新增不可变严格 DTO：

- `IndexSummaryItem`：summary 页的 slug、title、summary。
- `IndexPageSnapshot`：canonical Index 页的 id、version、content、summary；不存在时为空。
- `IndexIntroContext`：可选 Index 快照和最近 summary 列表。
- `IndexIntroChange`：`ingest/retract`、knowledge ID、相关页面标题和摘要快照。
- `IndexIntroRequest`：`create/update` 模式、现有简介、summary 样本和本批变更。
- `IndexIntroOutput`：一段非空、有长度上限的 intro。
- `IndexIntroPlan`：期望 Index id/version、待写 intro、模型结果状态和可选错误码。

canonical Index 的身份固定为 `slug=index, page_type=index`。Store、Index 查询和写入都使用该身份，不以“最近更新的任意 index 类型页面”代替 canonical 行；同 scope 出现非 canonical 或身份冲突的 index 行时，不允许静默覆盖 canonical 行。

Store 按 scope 读取 canonical Index 快照和最近更新的最多 200 个活动、published summary，只读取窄列。排序使用 `updated_at DESC, id DESC`，保证相同时间值下稳定。

### 6.2 首次生成

不存在 Index 页，或现有内容为空、仅为兼容占位文案时，进入 `create`：

1. 将本批最终成功的 summary 放在样本前部。
2. 合并数据库最近 summary，按 slug 去重。
3. 最多保留 200 条。
4. 使用 strict fake 生成一段简介。

本批失败、superseded 或仅 retract 的无效 current summary 不进入样本。

### 6.3 增量更新

现有 Index 已有有效简介且本批存在成功 ingest/retract 变化时，进入 `update`：

- 请求只包含现有简介和本批结构化 change items。
- 不把全部历史 summary 再次放入请求。
- change items 按 action、knowledge ID 和页面 slug 稳定排序。
- 只有失败或 superseded operation 时不生成 Index 计划。

### 6.4 strict fake key

`FakeChatModel` 增加 Index intro 调用，fixture 使用稳定 key：

- create：`index_intro:create:<逗号分隔的 summary slug>`。
- update：`index_intro:update:<逗号分隔的 action:knowledge_id>`。

例如：

- `index_intro:create:summary/knowledge-1`
- `index_intro:update:ingest:knowledge-1,retract:knowledge-2`

请求快照保留完整结构；key 只用于查找确定性 fixture 响应。缺失响应和非法输出为 `PermanentModelError` 或严格输出校验错误。

### 6.5 输出清理和长度

- intro 先去除首尾空白，再裁掉第一个旧式 `\n## ` 目录段。
- 清理后的 intro 必须非空并满足明确长度上限，避免把目录或大段正文写入系统页面。
- Index 页的 content 和 summary 始终写入同一 intro。

## 7. Index 失败与并发语义

Index intro 是 best-effort 增强，不影响 pending-op 成败：

- `TransientModelError` 使用现有模型重试器，最多尝试 3 次，等待 2 秒和 4 秒。
- 永久错误、缺失 fake 输出或输出校验错误不重试。
- 首次生成最终失败时，计划写入固定默认简介。
- 增量更新最终失败时，计划保留旧简介。
- `CancelledError`、锁丢失和其他控制流异常继续传播，不能被 fallback 吞掉。
- Index 模型失败不增加 operation `fail_count`，也不生成 dead-letter。

Store 在事务中重新锁定 Index 页：

- 新建使用系统 slug `index`、`page_type=index`、根目录和 `version=1`。
- create 时若并发事务已创建 Index，当前计划不覆盖新行。
- update 仅在 id/version 与快照一致时写入。
- 快照过期时记录 `stale_skipped`，正文摄取继续提交。
- intro 与当前值相同时不增加 Index 版本。
- 实际变化时 content 和 summary 同步更新，版本增加一次。

Index 页不参与 taxonomy 或自动交叉链接。

## 8. Worker 数据流

现有 taxonomy、Reduce 和 precommit 固定点不改变。Index 规划只在固定点稳定后执行：

1. 根据最终 failures 和 superseded 集合确定 completed operation。
2. 若没有成功 ingest/retract 变化，不加载 Index context。
3. 加载 Index 快照和最近 summary。
4. 从最终 pages 和 contribution delta 构造 summary 样本及 change items。
5. 选择 create、update 或 no-op。
6. 调用 strict fake，并按第 7 节降级为默认或保留计划。
7. 构造一次带可选 `index_intro_plan` 的 `BatchApplyRequest`。

Index 规划不加入 taxonomy/Reduce 固定点，因为它不改变 contributor、page slug 或 operation 排除集合。

## 9. Store 原子写入

### 9.1 自动链接候选加载

Store 在替换本批链接投影之前，以窄列查询当前出链：

- 一次读取全部受影响 source page 的旧 `WikiLink.target_slug`。
- 一次读取这些目标以及本批成功 fresh page 的活动 title/aliases/page type。
- 在内存中按 source page 构造候选，避免逐页查询。

fresh candidate 必须来自 `selected_pages` 中实际会写入的页面，不能直接信任 Worker 请求中的原始 pages。

### 9.2 写入顺序

在现有 `_apply_batch_results()` 事务中：

1. 完成 claim、expectation、contributor 和 assignment 校验。
2. 解析目录并确定 selected pages。
3. 加载旧出链候选。
4. 写入页面语义字段并按语义字段计算版本。
5. flush 新页面以获得 ID。
6. 对非删除、非系统受影响页执行 linkify，并写最终 content。
7. 从最终 content 重建每页 `WikiLink`。
8. 回填新创建 target 的 `target_page_id`。
9. 应用、保留或跳过 Index plan。
10. 写贡献、批次日志、finalization、pending-op 和 Outbox。

任何数据库异常、页面 CAS、claim 丢失或内部不变量错误都回滚自动链接、链接投影和 Index 变化。

### 9.3 幂等重放

现有 `WikiLogEntry.result_outcome` 继续作为批次幂等结果：

- 已提交 operation 重放时直接恢复 outcome。
- 不重新执行 linkify。
- 不重新应用 Index plan。
- 不重复写日志或增加页面版本。

## 10. Graph 增强

现有 Graph REST 和遍历算法保持不变，只修正结构化边的可见性与度数：

- degree 只统计活动、published source page 发出的边。
- target 必须解析到活动、published 页面。
- archived、软删除和未解析 target 不能增加可见节点的 link count。
- Overview 先按 `link_count DESC, slug ASC` 选择 top-N，再只返回这些节点之间的边。
- Ego 继续对入边和出边做无向 BFS。
- 类型过滤既过滤结果节点，也阻止通过隐藏类型继续扩展。
- 边按 source slug、target slug 稳定排序。

链接解析改用 protected-span scanner 后，代码示例中的伪 Wiki 链接不会进入 Graph。

## 11. 日志增强

阶段四 B 保留当前每批一条幂等日志，不重构为每个 pending-op 一行，也不新增迁移。

批次日志继续包含：

- 原 action：`wiki_ingest_batch`、`wiki_retract_batch` 或 `wiki_incremental_batch`。
- `result_outcome`：completed、failed 和 superseded operation IDs。
- `pages_affected`：本批实际语义页面快照。

message 增加稳定增强摘要：

- 自动链接实际变化的页面数。
- 自动插入的链接数。
- Index 结果：`created`、`updated`、`unchanged`、`defaulted`、`kept_after_error`、`stale_skipped` 或 `not_requested`。

Index 实际创建或更新时，其 slug/title 快照加入 `pages_affected`；保留、no-op 或跳过时不能伪报页面变化。日志消息不得包含完整模型输出、正文、异常堆栈或密钥。

## 12. 错误边界

- 自动链接纯函数对任意合法字符串必须返回确定结果；脏数据库候选或内部身份冲突属于 `InvariantError`，回滚批次。
- Index 模型普通失败按第 7 节降级，不形成 operation failure。
- Index 数据库写入失败仍是事务失败，不能伪装成模型 fallback。
- Index 快照竞争是可预期 best-effort skip，不回滚正文。
- 控制流错误始终传播并触发现有 claim release，不写增强结果。
- 批次中部分 operation 失败时，自动链接和 Index change 只能基于最终成功集合。

## 13. 性能边界

- 自动链接不扫描全库正文。
- 候选页和旧出链使用批量窄列查询，不逐页 N+1。
- linkify 只处理本批受影响页面。
- Index 首次 summary 样本硬上限 200。
- Index 增量请求大小只随本批变化增长。
- Graph 继续使用节点和边硬上限。
- 不在 `wiki_pages.content` 持久化完整 Index 目录；目录项继续由 GET `/index` 动态分页查询。

## 14. 测试策略

### 14.1 纯函数和领域规则

- CJK、ASCII 单词边界和大小写敏感匹配。
- 长名称优先、同名歧义排除和稳定 tie-break。
- self-link、已使用 slug 和每 slug 只插入一次。
- fenced/inline code、Wiki 链接、Markdown link/image、reference link/definition、autolink。
- 转义、未闭合结构、CRLF 和重复运行幂等。
- 链接提取忽略受保护区域，与 linkify 结果一致。

### 14.2 Schema、fake 和 Worker

- Index DTO 深层不可变和交叉字段校验。
- create/update stable fake key 和请求快照。
- 缺失响应、非法输出、瞬时失败与控制流异常。
- 首次 200 summary 上限和本批 summary 优先。
- 增量只含旧 intro 和本批 change。
- ingest/retract 混合排序。
- 默认简介、保留旧简介和不增加 fail_count。
- 固定点失败或 superseded 页面不进入增强计划。

### 14.3 Store 和真实 PostgreSQL

- fresh pages 与旧出链候选合并、歧义过滤和 scope 隔离。
- 自动链接与 `WikiLink` 同事务可见。
- 仅自动 markup 变化不增加页面版本。
- 页面 CAS、claim 丢失和内部冲突回滚正文、边和 Index。
- Index create/update/no-op/fallback/stale snapshot。
- Index content/summary 同步和版本规则。
- 增强日志计数、pages affected 和幂等重放。
- 并发人工 Index 编辑不被覆盖。

### 14.4 Query 和端到端

- Graph degree 排除 archived、deleted 和 unresolved target。
- Overview top-N 边闭包、Ego 无向 BFS、类型过滤和稳定排序。
- fake runtime 首次摄取生成自动链接和 Index intro。
- 第二次 ingest 增量更新 Index。
- 无服务全量只跳过显式 opt-in PostgreSQL/Redis 用例。
- 真实 PostgreSQL/Redis 全量全部通过。

## 15. 文档与配置

新增 `docs/Wiki阶段四B.md`，使用中文并只描述实际实现：

- protected-span 自动交叉链接规则。
- 候选只限本批受影响页面。
- Index create/update/fallback 和 200 summary 上限。
- Graph/日志增强和版本语义。
- fake fixture 结构及运行命令。
- 当前限制与阶段四 C 边界。

文档中的每条终端命令前必须有中文用途注释。

阶段四 B 默认不增加环境变量。若实现过程中发现必须配置的硬限制，应优先保持设计常量；只有确有部署差异时才增加配置，并同步 `.env.example`、Compose 和中文文档。

## 16. 明确不在范围内

- 扫描并重写整个知识库的历史正文。
- broken-link 文本清理。
- `missing_cross_ref` 全库检测。
- 完整 Lint、auto-fix 和问题单生成。
- Agent 工具和 `WikiPageIndexer`。
- 真实 Index intro、embedding 或 taxonomy 模型适配。
- REST DTO 变更。
- 数据库迁移。

## 17. 验收标准

阶段四 B 完成时必须同时满足：

- 本批受影响页面按确定性规则自动插入安全交叉链接。
- protected Markdown 区域不被修改，重复运行幂等。
- 自动链接本身不额外增加页面版本。
- 正文安全 Wiki 链接与 `WikiLink`、Graph 边一致。
- Index 首次生成和增量更新均由 strict fake 驱动，输入大小有界。
- Index 模型失败不阻断正文摄取，也不增加 pending fail count。
- Index 快照过期不覆盖并发人工编辑。
- Graph 只按活动、published 的已解析页面计算可见连接度。
- 批次日志记录自动链接和 Index 实际结果，幂等重放无重复副作用。
- 现有 REST、数据库迁移 head 和阶段一至四 A 测试保持不变。
- 中文文档、静态检查、无服务全量和真实 PostgreSQL/Redis 全量全部通过。
