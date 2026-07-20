# Wiki 阶段四 B

阶段四 B 在现有 fake 摄取 Worker 上增加确定性自动交叉链接、strict fake 驱动的 canonical Index 简介，以及只基于可见结构化边的 Graph 查询。页面写入、Index、日志与异步续跑仍沿用现有 PostgreSQL 事务和 Outbox 链路；REST DTO 不增加字段，也不新增 Alembic migration。本文只描述当前仓库已经实现的能力。

## 受保护 Markdown 区域

自动链接与 `WikiLink` 抽取共用 `app/wiki/linkify.py` 中的扫描规则。下列受保护 Markdown 区域保持原文，不会插入自动 Wiki markup，其中的伪 Wiki 链接也不会形成结构化边：

- 反引号或波浪号 fenced code，包括未闭合 fence 到文件末尾的内容；
- inline code，包括未闭合 inline code 到文件末尾的内容；
- 已有 `[[slug]]`、`[[slug|display]]` Wiki markup；
- inline Markdown 链接、图片和 reference-style 链接；
- reference definition 行；
- `<https://example.test/path>` 这类 autolink；
- 被反斜杠转义的候选文本。

扫描保留 CRLF 和原始 Markdown。重复执行 linkify 时，已经存在的 Wiki 目标不会再次插入，因此结果幂等。

## 本批受影响页面与候选范围

自动链接只处理本批固定点结算后最终成功、未删除且不是 `index`/`log` 的页面。失败页面、superseded 页面及只由这些操作贡献的独占页面不会进入候选，也不会持久化为本批结果。

每个本批受影响页面的候选只来自两处：

- 本批最终成功页的标题与 aliases；
- 该页面落库前已有 `WikiLink` 出链所指向的活动、published 页面标题与 aliases。

Store 先按本批 source page id 批量读取旧出链，再按这些 target slug 批量读取候选页面；它不扫描全库正文，也不会把与本批无关的全库页面加入候选。

候选应用遵循下列确定性规则：

- 显示名越长越优先，避免短名称抢占长名称中的文本；
- 同一显示名若对应多个 slug，则把该显示名的所有候选整体排除；
- ASCII 显示名要求两侧不是 ASCII 字母、数字或下划线，避免命中更长标识符；
- CJK 等非 ASCII 显示名直接按连续文本匹配；
- 当前页面的自链接候选会被排除；
- 已存在目标或本次已插入目标后，每个 slug 只链接一次；
- 候选按稳定顺序处理，第二次执行不会改变第一次的结果。

## canonical Index 与 strict fake

canonical Index 必须同时满足 `slug=index` 和 `page_type=index`。读取或写入发现多个活动身份记录，或发现只满足其中一个条件的冲突记录时，会拒绝把它当作 canonical Index。新建 Index 使用 `status=published`、`wiki_path=/index` 和 `version=1`；正文与摘要保存同一份简介。

Index 模型请求分为 create 与 update：

- create 的 strict fake key 是 `index_intro:create:<summary slugs>`。summary slug 按请求顺序用逗号连接；请求先放本批最终成功的 summary，再补最近历史 summary，按 slug 去重后最多 200 条。
- update 的 strict fake key 是 `index_intro:update:<action:knowledge_id>`。变化项去重并稳定排序，只传旧 intro 与本批 `ingest`/`retract` 的结构化变化；不会把全库正文发送给模型。

fake fixture 必须显式声明完全匹配的 key。缺少 key 是永久模型错误，fake 不会猜测输出。Index intro 输出会去掉附加的 `##` 目录段，并限制为清理后 1 到 4000 个字符。

只有 `TransientModelError` 会重试，单次 Index 模型操作最多调用 3 次，等待间隔为 2 秒和 4 秒；永久错误、非法输出和取消不进入该重试。Index 是 best-effort：首次创建失败时使用固定默认简介，增量更新失败时保留旧 intro，不增加 pending 的 `fail_count`，也不把本批成功操作改为失败。

Index 计划绑定读取时的页面 id 与 version。落库前若人工修改或其他并发写入改变了 canonical Index，CAS 不匹配会记录 `stale_skipped`，本批页面仍可提交，但不会覆盖人工内容。固定点结算剔除额外 superseded 操作时，同样不会应用基于旧成功集合生成的 Index 计划。

## 正文与页面版本

Store 区分两种正文：

- semantic content 是 Reduce 得到、尚未插入自动 Wiki markup 的业务正文；
- persisted content 是对 semantic content 完成确定性 linkify 后真正写入数据库的正文。

已有正文中的有效 Wiki markup 会先投影回可见文本，再与 semantic content 比较。只有语义正文或标题、摘要、来源、发布/删除状态等页面值实际变化时，已有页面才递增 `version`；仅因相同语义正文重新生成或补上确定性 markup 时，自动链接不额外增加页面版本。新页面仍从 `version=1` 创建。

持久化后，Store 从最终正文重新抽取安全 Wiki 目标并替换该页面的 `WikiLink`；目标页面存在时回填 `target_page_id`，删除页面时清空指向它的已解析目标。

## Graph 与批次日志

Graph degree、overview、ego BFS 和返回边共用同一组 visible edges。可见边要求 source 与 target 两端都属于当前 tenant 和 knowledge base、未软删除、状态为活动、published，并且 `WikiLink.target_page_id` 已解析；未解析 broken link、archived 页面或软删除页面不会进入 Graph。

每个批次仍写一条幂等日志。日志 message 记录完成数、superseded 数、自动链接页面数、新增链接数和 Index 结果。`pages_affected` 只包含本批实际持久化页面，以及确实创建或修改的 Index；`unchanged`、`kept_after_error` 与 `stale_skipped` 不会虚增 Index 页面。自动链接计数也只统计本次真实插入的页面和 slug。

## 同一事务

现代 Worker 的一次批次结算把页面、自动链接后的正文、`WikiLink`、Index、贡献、日志、pending、finalization、失败或 dead-letter 结算以及 follow-up Outbox 放在同一事务中。页面 CAS、目录解析、贡献不变量、finalization 释放或 claim 校验任一失败时，这些写入一起回滚；提交后 dispatcher 再投递已提交的 Outbox 事件。

## 定向验证与 fake 摄取

下面的命令使用当前项目的 uv runtime，并把缓存限制在工作树内。每条命令都需要在项目根目录执行。

```powershell
# 把 uv 缓存限制在当前工作树
$env:UV_CACHE_DIR = "$PWD\.uv-cache"

# 运行阶段四 B 的纯函数、Index 与 Worker 定向测试
uv run pytest tests/wiki/test_linkify.py tests/wiki/test_ingest_index_intro.py tests/wiki/test_ingest_worker.py -q

# 指定包含 strict fake Index 响应的共享 fixture
$env:GRAPH_WIKI_FAKE_DATA_FILE = "examples/wiki_fake_data.json"

# 指定本地 PostgreSQL 数据库连接
$env:GRAPH_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph"

# 指定 Worker 使用的本地 Redis 连接
$env:GRAPH_REDIS_URL = "redis://127.0.0.1:6379/2"

# 启动 fake 摄取依赖的 PostgreSQL 与 Redis
docker compose up -d postgres redis

# 升级到当前唯一 Alembic head
uv run alembic upgrade head

# 使用 strict fake fixture 入队首次摄取
uv run python -m app.wiki.tasks.enqueue_fake --op ingest --kb-id 11111111-1111-1111-1111-111111111111 --knowledge-id knowledge-1

# 启动 Wiki Worker 与 Outbox dispatcher 处理已入队操作
docker compose up -d wiki-worker outbox-dispatcher
```

## 当前限制与后续边界

- 当前只为本批最终成功页面补充确定性链接，不遍历全库旧正文；全库 `missing_cross_ref` 扫描属于阶段四 C。
- 当前 broken-link 能被解析、统计和列出，但不改写正文清理失效文本；该清理属于阶段四 C。
- 当前 Lint 保持查询与问题展示范围，不提供完整自动修复；完整 auto-fix 和问题单自动生成属于阶段四 C。
- 当前 Index intro、知识源、embedding 与 taxonomy 仍由 fake adapter 和 JSON fixture 驱动，没有接入真实 Index 模型或真实上游适配器。
- Agent 工具和 `WikiPageIndexer` 属于阶段五，当前代码未提供这些能力。

阶段四 B 不修改 Index、Graph、Log 的 REST DTO。当前唯一 Alembic head 仍是 `20260719_04_add_wiki_log_result_outcome.py`，修订号为 `20260719_04`。
