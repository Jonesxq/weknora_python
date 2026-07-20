# Wiki 阶段四 A

阶段四 A 在现有 fake 摄取 Worker 上增加批次 taxonomy：它为真正新页面规划最多三级的目录归属，并在 PostgreSQL 中原子创建或复用目录链。上游和模型适配器仍全部由 `examples/wiki_fake_data.json` 驱动；本文只描述当前仓库已经实现的能力。

## 迁移与配置

阶段四 A 不新增迁移。当前唯一 Alembic head 仍来自 `20260719_04_add_wiki_log_result_outcome.py`，修订号是 `20260719_04`。

`.env.example` 提供四个 taxonomy 默认值：

- `GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE=60`：每个模型批次最多包含 60 个 topic。
- `GRAPH_WIKI_TAXONOMY_PARALLEL=4`：taxonomy 模型批次的默认并发数是 4。
- `GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT=120`：目录不超过 full catalog limit 时传入全部活动目录。
- `GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT=40`：大目录中 embedding top-K 深层目录的 K 值。

```powershell
# 运行 taxonomy 数据结构、候选目录与模型输出恢复的定向测试
uv run pytest tests/wiki/test_ingest_taxonomy.py -q
```

## fake fixture 合同

fixture 的 `model_responses.embeddings` 是“请求 key 到有限数值向量”的映射。topic 使用 `topic:<slug>` key，深层目录使用 `folder:<UUID>` key；fake embedding 只返回本次请求列出的向量，缺少任一 key 会成为永久模型错误。一次调用记录为 `embedding:<key1>,<key2>`，其中 key 顺序来自已稳定排序的请求，因此可作为稳定 batch key。需要模拟瞬时失败时，在 `transient_failures` 中为同一调用 key 配置剩余失败次数。

`model_responses.taxonomies` 是“taxonomy batch key 到输出”的映射。batch key 由规范化、去重并按 slug 排序的 topic 以逗号连接，例如 `concept/retrieval,entity/acme`；对应 `decisions` 必须按相同顺序完整覆盖该批次。fake taxonomy 的调用 key 是 `taxonomy:<batch key>`，找不到显式响应时不会猜测目录，而是返回永久模型错误。

最小结构如下，`embeddings` 可以在小目录场景保持为空：

```json
{
  "model_responses": {
    "embeddings": {
      "topic:entity/acme": [1.0, 0.0],
      "folder:00000000-0000-0000-0000-000000000001": [0.8, 0.2]
    },
    "taxonomies": {
      "entity/acme": {
        "decisions": [
          {
            "slug": "entity/acme",
            "new_segments": ["Organizations", "Products"]
          }
        ]
      }
    }
  }
}
```

## 目录候选

Store 按 tenant 和 knowledge base scope 读取活动目录。小目录，即活动目录数量 `<= full catalog limit`，直接向 taxonomy 提供全量活动目录，不调用 embedding。大目录始终保留所有一级目录，再以 topic 的标题和摘要向量对深层目录 path 向量计算余弦相似度；每个深层目录取它与所有 topic 的最高分，选择 embedding top-K 深层目录，并做祖先补齐。最终白名单因此可能多于 K，但不会漏掉所选目录的父链。

taxonomy 输出只能选择请求白名单中的 `base_folder_id`，可以从根开始，也可以在该 base 下追加最多两个新目录段；base 深度加新段总深度不能超过 3。模型输出会被重新校验，不完整 topic 覆盖、未知 base、非法 path 段或超深结果都不会写入数据库。

## 真正新页面

只有当前 scope 中从未出现过的 `entity` 或 `concept` 才进入分类。Worker 虽然按本批贡献生成 topic，但 Store 会同时查询活动页面和软删除历史；因此下列内容不分类：

- `summary` 页面；
- 已存在的活动 `entity` 或 `concept`；
- 有软删除历史、这次将被恢复的页面。

软删除历史恢复不分类，并保留原来的人工目录及 `folder_id`、`category_path`、`wiki_path`、`depth` 缓存。它不会因为恢复而被 taxonomy 移到新目录；恢复改变 `deleted_at` 等页面状态时，仍按正常页面更新规则递增 `version`。

旧调用方通过 legacy `apply_results` 写入真正新页面时不强制 taxonomy，继续采用 legacy root：`folder_id=None`、空 `category_path`、`wiki_path=/<slug>`、`depth=0`。

## 批次、重试与失败隔离

同一 slug 的贡献先合并为一个稳定 topic，并记录其 `contributors` pending-op 集合；topic 按 slug 排序后每 60 个切成一个批次 taxonomy 请求，实际批大小可由环境变量调低。请求以 semaphore 限制并发，默认并发数是 4。

embedding 选择和每个 taxonomy 批次都沿用 Worker 的模型重试器。只有 `TransientModelError` 会重试，单次模型操作最多调用 3 次：第一次失败后等待 2 秒，第二次失败后等待 4 秒，第三次失败直接耗尽；永久错误、输出校验错误和取消不走这组内部重试。测试可注入等待函数，生产默认使用异步 sleep。

失败隔离以 taxonomy 批次的 contributors 为边界：一个 taxonomy 批次失败，只把该批 topic 的 contributors 标为普通失败，其他成功批次仍可形成目录归属；若同一个 pending operation 同时贡献了失败 topic，它作为操作整体被排除。embedding 候选选择发生在切批之前，因此该步骤失败会影响本轮所有 taxonomy work item 的 contributors。

“最多调用 3 次”是一次 Worker 批次内部的模型尝试，不等于把 `fail_count` 增加三次。内部尝试耗尽后，相关 pending 才在本次数据库结算中增加一次 `fail_count`：前 4 次普通失败释放 claim 并保留 pending，供后续批次重试；第 5 次普通失败在同一事务写入 dead-letter、释放 finalization marker 并删除 pending。不要把这个机制理解为 Worker 在一个批次内自动执行五轮 pending 重放。

## 人工目录冲突

taxonomy 请求中的既有 base 是人工目录的 `base snapshot`，包含 `id`、`path` 和 `depth`。落库时 Store 会再次确认它仍属于同一 scope、未软删除，且 path/depth 未移动；人工移动或删除 base snapshot 会触发 `PageConflict`。整个写事务随之回滚，Worker 释放 claim 供后续重试，不会静默改用新路径，也不会留下半条新目录链。

创建目录链时，Store 持有 tenant+knowledge base 的 `scope advisory lock`，逐级复用或创建目录；数据库还以“knowledge base、parent、name、活动状态”约束同级唯一，根级 sibling 也纳入同一规则。已有 sibling 的 path/depth 不符合推导值时会报不变量错误，不会静默复用。批次 `operation_id` 的 advisory lock 与 `wiki_log_entries.result_outcome` 提供幂等重放：已提交的同一操作返回原结果，不重复创建目录或递增页面版本。

## 事务与页面缓存

现代 Worker 把目录与页面/贡献/链接/log/pending/outbox 放在同一事务中提交。目录解析或页面 CAS 冲突会使这些写入一起回滚；成功时再统一删除已完成 pending、释放失败 pending 或登记 dead-letter，并按剩余工作写 follow-up outbox。

页面上的目录字段是落库时的非规范化缓存：`folder_id` 指向最终目录，根级页面为 `None`；`category_path` 保存目录名数组；`wiki_path` 保存“目录 path + 页面 slug”；`depth` 保存最终目录深度。真正新页面以这些缓存和 `version=1` 创建。已有活动页面或软删除历史恢复都保留原目录缓存，taxonomy 不负责自动搬家；单纯读取或保留缓存不会递增 `version`，只有正文、摘要、来源、发布/删除状态等实际页面值改变时才按现有更新规则递增版本。

## fake CLI 摄取

下面的命令使用示例 fixture 入队 `knowledge-1`。CLI 只负责入队；实际处理仍由 Wiki Worker 执行。

```powershell
# 指定包含 fake embedding 与 taxonomy 响应的共享 fixture
$env:GRAPH_WIKI_FAKE_DATA_FILE="examples/wiki_fake_data.json"

# 指定本地 PostgreSQL 数据库连接
$env:GRAPH_DATABASE_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph"

# 指定 Worker 使用的本地 Redis 连接
$env:GRAPH_REDIS_URL="redis://127.0.0.1:6379/2"

# 启动本地 PostgreSQL 与 Redis 服务
docker compose up -d postgres redis

# 升级到当前唯一 Alembic head
uv run alembic upgrade head

# 从示例 fixture 入队一条 ingest 操作
uv run python -m app.wiki.tasks.enqueue_fake --op ingest --kb-id 11111111-1111-1111-1111-111111111111 --knowledge-id knowledge-1

# 启动 Wiki Worker 与 Outbox dispatcher 处理已入队操作
docker compose up -d wiki-worker outbox-dispatcher
```

## 真实服务验收

真实集成测试只读取显式测试 URL；未配置时相关用例 skip，不会回退连接开发数据库。可用本地 PostgreSQL 和 Redis 执行：

```powershell
# 启动验收所需的 PostgreSQL 容器
docker compose up -d postgres

# 指定真实 PostgreSQL 集成测试数据库
$env:GRAPH_TEST_POSTGRES_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph"

# 指定真实 Redis 集成测试数据库
$env:GRAPH_TEST_REDIS_URL="redis://127.0.0.1:6379/15"

# 运行 Wiki 全量测试并启用真实服务用例
uv run pytest tests/wiki -q
```

## 当前限制

- 当前只有 fake 上游；Worker 的知识源、chat taxonomy 与 fake embedding 都来自 JSON fixture。
- 不提供自动交叉链接生成；当前只解析并维护正文中已经存在的 Wiki 链接关系。
- 不提供完整 Lint auto-fix。
- 不包含 Agent，也不包含 WikiPageIndexer。
- 不接入真实 embedding、真实 taxonomy 模型或真实知识服务适配器。
