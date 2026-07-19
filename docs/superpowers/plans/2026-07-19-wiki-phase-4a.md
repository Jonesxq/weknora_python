# Wiki 阶段四 A 实施计划

> **面向执行代理：** 必须使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans` 逐任务执行；所有步骤使用复选框跟踪，生产代码必须先有失败测试。

**目标：** 在当前 `app/wiki` 增量摄取结构中实现 fake embedding 驱动的批次 taxonomy，并把新页面目录、目录缓存、贡献、链接、日志和 pending-op 原子提交。

**架构：** Worker 在现有 Map、Reduce 和预提交检查固定点中规划受白名单约束的目录 assignment；小目录使用全部目录，大目录通过 fake embedding 选择相关深层目录。Store 负责识别真正新页面，并在现有结果事务中按活动兄弟目录唯一键复用或创建目录，最终写入 `folder_id/category_path/wiki_path/depth`。

**技术栈：** Python 3.12、Pydantic v2、SQLAlchemy 2 Async、PostgreSQL 16、Celery、Redis、Tenacity、pytest、pytest-asyncio、Ruff。

---

## 设计依据

- 总设计：`E:\code\WeKnora\docs\superpowers\specs\2026-07-14-python-wiki-reimplementation-design.md`
- 当前结构设计：`docs/superpowers/specs/2026-07-19-wiki-phase-4a-design.md`
- 阶段三运行文档：`docs/Wiki阶段三.md`

4A 只实现批次 taxonomy 与目录规划。自动交叉链接、Index/日志/Graph 增强属于 4B；完整 Lint/auto-fix/问题单属于 4C；Agent 和 `WikiPageIndexer` 属于阶段五。

## 文件职责

新增文件：

- `app/wiki/ingest/taxonomy.py`：topic 整理、embedding 候选选择、taxonomy 切批和严格输出恢复。
- `tests/wiki/test_ingest_taxonomy.py`：纯函数、embedding、白名单、切批和错误边界。
- `docs/Wiki阶段四A.md`：与最终实现一致的中文运行说明。
- `tests/wiki/test_phase_four_a_docs.py`：环境、文档、迁移 head 和限制合同。

修改文件：

- `app/wiki/ingest/schemas.py`：新增不可变 taxonomy DTO、目录 assignment 和 Worker 配置。
- `app/wiki/ingest/ports.py`：新增 embedding/taxonomy 端口。
- `app/wiki/ingest/fakes.py`：新增严格 fake embedding/taxonomy。
- `app/wiki/ingest/store.py`：taxonomy context、assignment 不变量和原子目录写入。
- `app/wiki/ingest/worker.py`：taxonomy/Reduce/预提交固定点。
- `app/wiki/tasks/wiki_tasks.py`：runtime 注入 fake embedding。
- `examples/wiki_fake_data.json`：确定性 embedding 和 taxonomy 响应。
- `.env.example`、`docker-compose.yml`：4A 配置默认值。
- `README.md`：阶段四 A 文档入口。
- `tests/wiki/test_ingest_schemas.py`、`tests/wiki/test_ingest_fakes.py`、`tests/wiki/test_ingest_store.py`、`tests/wiki/test_ingest_worker.py`、`tests/wiki/test_wiki_tasks.py`、`tests/wiki/test_postgres_integration.py`：对应合同和集成回归。

## 执行环境

```powershell
# 进入阶段四 A 隔离 worktree
Set-Location E:\code\graph\.worktrees\wiki-phase-4a

# 把 uv 缓存限制在 worktree 内
$env:UV_CACHE_DIR = "$PWD\.uv-cache"

# 同步锁文件中的 Python 3.12 环境
uv sync --python 3.12

# 验证阶段四 A 起点；未配置真实服务时预期 832 passed、16 skipped
$env:PYTHONDONTWRITEBYTECODE = '1'
uv run pytest -p no:cacheprovider --basetemp "$env:TEMP\graph-wiki-phase4a-baseline" -q
```

---

### 任务 1：不可变 taxonomy DTO 与配置

**文件：**

- 修改：`app/wiki/ingest/schemas.py:40-166, 387-418, 873-950`
- 修改：`tests/wiki/test_ingest_schemas.py`

- [ ] **步骤 1：先写 DTO、深层不可变和环境配置失败测试**

在 `tests/wiki/test_ingest_schemas.py` 增加：

```python
import math
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.wiki.ingest.schemas import (
    AllowedFolderBase,
    EmbeddingItem,
    EmbeddingOutput,
    EmbeddingRequest,
    FolderAssignment,
    FolderCatalogEntry,
    TaxonomyDecision,
    TaxonomyOutput,
    TaxonomyRequest,
    TaxonomyTopic,
    WikiWorkerOptions,
)


def test_phase_four_a_options_defaults_and_env(monkeypatch):
    monkeypatch.setenv("GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE", "40")
    monkeypatch.setenv("GRAPH_WIKI_TAXONOMY_PARALLEL", "3")
    monkeypatch.setenv("GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT", "80")
    monkeypatch.setenv("GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT", "24")
    options = WikiWorkerOptions.from_env()
    assert options.taxonomy_topic_batch_size == 40
    assert options.taxonomy_parallel == 3
    assert options.taxonomy_full_catalog_limit == 80
    assert options.taxonomy_related_folder_limit == 24


def test_embedding_output_is_complete_finite_and_deeply_immutable():
    request = EmbeddingRequest(
        items=(EmbeddingItem(key="topic:entity/acme", text="Acme"),)
    )
    output = EmbeddingOutput(vectors={"topic:entity/acme": (1.0, 0.0)})
    assert tuple(output.vectors) == ("topic:entity/acme",)
    with pytest.raises(TypeError):
        output.vectors["topic:entity/acme"] = (0.0, 1.0)  # type: ignore[index]
    with pytest.raises(ValidationError):
        EmbeddingOutput(vectors={"topic:entity/acme": (math.nan, 0.0)})
    assert request.items[0].key == "topic:entity/acme"


def test_taxonomy_request_and_assignment_require_canonical_identities():
    folder_id = uuid4()
    op_id = uuid4()
    base = AllowedFolderBase(
        id=folder_id,
        path="/Organizations",
        depth=1,
    )
    request = TaxonomyRequest(
        topics=(
            TaxonomyTopic(
                slug="entity/acme",
                title="Acme",
                page_type="entity",
                summary="Organization",
            ),
        ),
        allowed_bases=(base,),
    )
    output = TaxonomyOutput(
        decisions=(
            TaxonomyDecision(
                slug="entity/acme",
                base_folder_id=folder_id,
                new_segments=("Products",),
            ),
        )
    )
    assignment = FolderAssignment(
        slug="entity/acme",
        contributor_op_ids=(op_id,),
        base_folder_id=folder_id,
        base_path="/Organizations",
        base_depth=1,
        new_segments=("Products",),
    )
    assert request.topics[0].slug == output.decisions[0].slug == assignment.slug
    with pytest.raises(ValidationError):
        FolderAssignment(
            slug="entity/acme",
            contributor_op_ids=(op_id,),
            base_folder_id=None,
            base_path="/forged",
            base_depth=0,
        )


def test_folder_catalog_rejects_invalid_path_depth_and_name():
    with pytest.raises(ValidationError):
        FolderCatalogEntry(
            id=uuid4(),
            parent_id=None,
            name="bad/name",
            path="/bad/name",
            depth=1,
        )
    with pytest.raises(ValidationError, match="相邻"):
        TaxonomyDecision(
            slug="entity/acme",
            new_segments=("Products", "products"),
        )
```

- [ ] **步骤 2：运行测试确认 RED**

```powershell
# 新 DTO 尚不存在，预期 ImportError
uv run pytest tests/wiki/test_ingest_schemas.py -q
```

- [ ] **步骤 3：实现 Worker 配置和严格 DTO**

在 `WikiWorkerOptions` 增加并接入 `from_env()`：

```python
taxonomy_topic_batch_size: int = Field(default=60, ge=1, le=60)
taxonomy_parallel: int = Field(default=4, ge=1, le=16)
taxonomy_full_catalog_limit: int = Field(default=120, ge=1, le=5000)
taxonomy_related_folder_limit: int = Field(default=40, ge=1, le=500)
```

对应环境字段：

```python
"taxonomy_topic_batch_size": os.getenv(
    "GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE", "60"
),
"taxonomy_parallel": os.getenv("GRAPH_WIKI_TAXONOMY_PARALLEL", "4"),
"taxonomy_full_catalog_limit": os.getenv(
    "GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT", "120"
),
"taxonomy_related_folder_limit": os.getenv(
    "GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT", "40"
),
```

增加目录名验证和向量只读映射：

```python
def _folder_name(value: str) -> str:
    value = value.strip()
    if (
        not value
        or len(value) > 512
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError("目录名无效")
    return value


class _FrozenVectorMapping(Mapping[str, tuple[float, ...]]):
    def __init__(self, values: Mapping[str, Sequence[float]]) -> None:
        self._items = tuple((key, tuple(vector)) for key, vector in values.items())
        self._values = dict(self._items)

    def __getitem__(self, key: str) -> tuple[float, ...]:
        return self._values[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def _iter_pairs(self):
        return iter(self._items)

    def __deepcopy__(self, memo):
        return type(self)(dict(self._items))
```

增加以下严格模型；字段验证必须使用 `_normalize_slug()`、`_folder_name()`、唯一集合和跨字段检查：

```python
class FolderCatalogEntry(_FrozenValueModel):
    id: UUID
    parent_id: UUID | None = None
    name: str
    path: str
    depth: int = Field(ge=1, le=3)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _folder_name(value)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        value = value.strip()
        if not value.startswith("/") or value.endswith("/") or len(value) > 2048:
            raise ValueError("目录 path 无效")
        return value

    @model_validator(mode="after")
    def validate_shape(self) -> Self:
        parts = self.path.removeprefix("/").split("/")
        if (
            len(parts) != self.depth
            or parts[-1] != self.name
            or any(_folder_name(part) != part for part in parts)
            or self.parent_id == self.id
        ):
            raise ValueError("目录 path、depth、name 或 parent_id 不一致")
        return self


class TaxonomyContext(_FrozenValueModel):
    folders: tuple[FolderCatalogEntry, ...] = ()
    classifiable_slugs: tuple[str, ...] = ()

    @field_validator("classifiable_slugs")
    @classmethod
    def validate_slugs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(_normalize_slug(item, ("entity", "concept")) for item in value)
        if len(normalized) != len(set(normalized)):
            raise ValueError("classifiable slug 不能重复")
        return normalized

    @model_validator(mode="after")
    def validate_folder_tree(self) -> Self:
        by_id = {item.id: item for item in self.folders}
        if len(by_id) != len(self.folders) or len({item.path for item in self.folders}) != len(self.folders):
            raise ValueError("folder catalog 的 id 和 path 必须唯一")
        for folder in self.folders:
            parent = by_id.get(folder.parent_id) if folder.parent_id else None
            if folder.depth == 1:
                if folder.parent_id is not None:
                    raise ValueError("一级目录不能有 parent")
            elif (
                parent is None
                or parent.depth + 1 != folder.depth
                or folder.path != f"{parent.path}/{folder.name}"
            ):
                raise ValueError("folder catalog 必须包含一致的完整祖先链")
        return self


class EmbeddingItem(_FrozenValueModel):
    key: str = Field(min_length=1, max_length=512)
    text: str = Field(min_length=1, max_length=8000)

    @field_validator("key")
    @classmethod
    def validate_key(cls, value: str) -> str:
        value = value.strip()
        if not value or "," in value:
            raise ValueError("embedding key 必须非空且不能包含逗号")
        return value


class EmbeddingRequest(_FrozenValueModel):
    items: tuple[EmbeddingItem, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_keys(self) -> Self:
        keys = [item.key for item in self.items]
        if len(keys) != len(set(keys)) or any(not key.strip() for key in keys):
            raise ValueError("embedding key 必须非空且唯一")
        return self


class EmbeddingOutput(_FrozenValueModel):
    vectors: Mapping[str, tuple[float, ...]]

    @field_validator("vectors")
    @classmethod
    def validate_vectors(cls, value):
        if not isinstance(value, Mapping) or not value:
            raise ValueError("embedding vectors 不能为空")
        normalized = {str(key): tuple(float(item) for item in vector) for key, vector in value.items()}
        dimensions = {len(vector) for vector in normalized.values()}
        if dimensions == {0} or len(dimensions) != 1:
            raise ValueError("embedding 向量维度必须一致且非零")
        if any(not math.isfinite(item) for vector in normalized.values() for item in vector):
            raise ValueError("embedding 向量必须为有限数值")
        return _FrozenVectorMapping(normalized)

    @field_serializer("vectors")
    def serialize_vectors(self, value):
        return dict(value._iter_pairs()) if isinstance(value, _FrozenVectorMapping) else dict(value)
```

继续实现完整 taxonomy 与 assignment 模型：

```python
class TaxonomyTopic(_FrozenValueModel):
    slug: str
    title: str
    page_type: Literal["entity", "concept"]
    summary: str = Field(max_length=4000)

    @field_validator("slug")
    @classmethod
    def validate_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("entity", "concept"))

    @field_validator("title")
    @classmethod
    def validate_title(cls, value: str) -> str:
        value = " ".join(value.split())
        if not 1 <= len(value) <= 512:
            raise ValueError("taxonomy title 长度无效")
        return value

    @model_validator(mode="after")
    def validate_page_type(self) -> Self:
        if self.slug.partition("/")[0] != self.page_type:
            raise ValueError("taxonomy slug 与 page_type 不一致")
        return self


class AllowedFolderBase(_FrozenValueModel):
    id: UUID
    path: str
    depth: int = Field(ge=1, le=3)

    @model_validator(mode="after")
    def validate_path_depth(self) -> Self:
        path = self.path.strip()
        parts = path.removeprefix("/").split("/") if path.startswith("/") else []
        if (
            path != self.path
            or path.endswith("/")
            or len(path) > 2048
            or len(parts) != self.depth
            or any(_folder_name(part) != part for part in parts)
        ):
            raise ValueError("allowed base path 与 depth 不一致")
        return self


class TaxonomyRequest(_FrozenValueModel):
    topics: tuple[TaxonomyTopic, ...] = Field(min_length=1, max_length=60)
    allowed_bases: tuple[AllowedFolderBase, ...] = ()

    @model_validator(mode="after")
    def validate_identities(self) -> Self:
        topic_slugs = [item.slug for item in self.topics]
        base_ids = [item.id for item in self.allowed_bases]
        base_paths = [item.path for item in self.allowed_bases]
        if len(topic_slugs) != len(set(topic_slugs)):
            raise ValueError("taxonomy topic slug 不能重复")
        if len(base_ids) != len(set(base_ids)) or len(base_paths) != len(set(base_paths)):
            raise ValueError("taxonomy allowed base 不能重复")
        return self


class TaxonomyDecision(_FrozenValueModel):
    slug: str
    base_folder_id: UUID | None = None
    new_segments: tuple[str, ...] = Field(default=(), max_length=2)

    @field_validator("slug")
    @classmethod
    def validate_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("entity", "concept"))

    @field_validator("new_segments")
    @classmethod
    def validate_segments(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(_folder_name(item) for item in value)
        if any(
            left.casefold() == right.casefold()
            for left, right in zip(normalized, normalized[1:])
        ):
            raise ValueError("相邻 taxonomy 目录名不能仅大小写不同")
        return normalized


class TaxonomyOutput(_FrozenValueModel):
    decisions: tuple[TaxonomyDecision, ...] = ()


class FolderAssignment(_FrozenValueModel):
    slug: str
    contributor_op_ids: tuple[UUID, ...] = Field(min_length=1)
    base_folder_id: UUID | None = None
    base_path: str | None = None
    base_depth: int = Field(default=0, ge=0, le=3)
    new_segments: tuple[str, ...] = Field(default=(), max_length=2)

    @field_validator("slug")
    @classmethod
    def validate_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("entity", "concept"))

    @field_validator("new_segments")
    @classmethod
    def validate_segments(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(_folder_name(item) for item in value)
        if any(
            left.casefold() == right.casefold()
            for left, right in zip(normalized, normalized[1:])
        ):
            raise ValueError("相邻 assignment 目录名不能仅大小写不同")
        return normalized

    @model_validator(mode="after")
    def validate_base_snapshot(self) -> Self:
        root = self.base_folder_id is None
        if root != (self.base_path is None) or root != (self.base_depth == 0):
            raise ValueError("base id、path 和 depth 必须共同表示根目录或既有目录")
        if self.base_path is not None:
            path = self.base_path.strip()
            parts = path.removeprefix("/").split("/") if path.startswith("/") else []
            if (
                path != self.base_path
                or path.endswith("/")
                or len(path) > 2048
                or len(parts) != self.base_depth
                or any(_folder_name(part) != part for part in parts)
            ):
                raise ValueError("base path 与 depth 不一致")
        if self.base_depth + len(self.new_segments) > 3:
            raise ValueError("目录分配不能超过三级")
        folder_path = self.base_path or ""
        for segment in self.new_segments:
            folder_path = f"{folder_path}/{segment}"
        wiki_path = f"{folder_path}/{self.slug}" if folder_path else f"/{self.slug}"
        if len(folder_path) > 2048 or len(wiki_path) > 1024:
            raise ValueError("目录或页面 wiki_path 超过数据库字段上限")
        if len(self.contributor_op_ids) != len(set(self.contributor_op_ids)):
            raise ValueError("目录 contributor 不能重复")
        return self
```

为 `BatchApplyRequest` 增加：

```python
folder_assignments: tuple[FolderAssignment, ...] = ()
```

- [ ] **步骤 4：运行 Schema 测试确认 GREEN**

```powershell
# 验证新增 DTO 与阶段一至三 DTO 回归
uv run pytest tests/wiki/test_ingest_schemas.py -q
```

- [ ] **步骤 5：提交 DTO 和配置**

```powershell
# 暂存严格合同及测试
git add app/wiki/ingest/schemas.py tests/wiki/test_ingest_schemas.py

# 创建独立 Schema 提交
git commit -m "feat: define wiki taxonomy contracts"
```

---

### 任务 2：Embedding/Taxonomy 端口与严格 fake

**文件：**

- 修改：`app/wiki/ingest/ports.py`
- 修改：`app/wiki/ingest/fakes.py`
- 修改：`tests/wiki/test_ingest_fakes.py`

- [ ] **步骤 1：先写 fake 输出、缺项、瞬时失败和深拷贝测试**

```python
from copy import deepcopy


@pytest.mark.asyncio
async def test_fake_embedding_returns_exact_requested_vectors():
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["embeddings"] = {
        "topic:entity/acme": (1.0, 0.0),
        "folder:00000000-0000-0000-0000-000000000001": (0.5, 0.5),
    }
    dataset = FakeDataset.model_validate(payload)
    model = FakeEmbeddingModel(dataset)
    request = EmbeddingRequest(
        items=(EmbeddingItem(key="topic:entity/acme", text="Acme"),)
    )
    output = await model.embed(request)
    assert output.vectors == {"topic:entity/acme": (1.0, 0.0)}
    assert model.calls == ["embedding:topic:entity/acme"]


@pytest.mark.asyncio
async def test_fake_taxonomy_requires_explicit_batch_response():
    model = FakeChatModel(FakeDataset.model_validate(deepcopy(FIXTURE)))
    request = TaxonomyRequest(
        topics=(TaxonomyTopic(slug="entity/acme", title="Acme", page_type="entity", summary="A"),),
        allowed_bases=(),
    )
    with pytest.raises(PermanentModelError, match="taxonomy:entity/acme"):
        await model.plan_folders(request)


@pytest.mark.asyncio
async def test_fake_embedding_transient_failure_uses_existing_failure_counter():
    payload = deepcopy(FIXTURE)
    payload["model_responses"]["embeddings"] = {
        "topic:entity/acme": (1.0, 0.0)
    }
    payload["transient_failures"]["embedding:topic:entity/acme"] = 1
    dataset = FakeDataset.model_validate(payload)
    model = FakeEmbeddingModel(dataset)
    request = EmbeddingRequest(items=(EmbeddingItem(key="topic:entity/acme", text="Acme"),))
    with pytest.raises(TransientModelError):
        await model.embed(request)
    assert (await model.embed(request)).vectors["topic:entity/acme"] == (1.0, 0.0)
```

- [ ] **步骤 2：运行测试确认 RED**

```powershell
# fake embedding/taxonomy 尚不存在，预期 ImportError 或 AttributeError
uv run pytest tests/wiki/test_ingest_fakes.py -q
```

- [ ] **步骤 3：扩展端口和 fake dataset**

在 `ports.py` 增加：

```python
@runtime_checkable
class EmbeddingModelPort(Protocol):
    async def embed(self, request: EmbeddingRequest) -> EmbeddingOutput: ...


@runtime_checkable
class TaxonomyModelPort(Protocol):
    async def plan_folders(self, request: TaxonomyRequest) -> TaxonomyOutput: ...


@runtime_checkable
class WikiIngestModelPort(
    ChatModelPort,
    CitationModelPort,
    DedupModelPort,
    TaxonomyModelPort,
    Protocol,
):
    pass
```

在 `_ModelResponses` 增加：

```python
embeddings: dict[str, tuple[float, ...]] = Field(default_factory=dict)
taxonomies: dict[str, TaxonomyOutput] = Field(default_factory=dict)
```

同时在 `_ModelResponses` 增加向量验证：

```python
@field_validator("embeddings")
@classmethod
def validate_embeddings(
    cls, value: dict[str, tuple[float, ...]]
) -> dict[str, tuple[float, ...]]:
    if any(not key.strip() or "," in key for key in value):
        raise ValueError("embedding fixture key 必须非空且不能包含逗号")
    dimensions = {len(vector) for vector in value.values()}
    if value and (0 in dimensions or len(dimensions) != 1):
        raise ValueError("embedding fixture 向量必须同维且非空")
    if any(
        not math.isfinite(item)
        for vector in value.values()
        for item in vector
    ):
        raise ValueError("embedding fixture 向量必须为有限数值")
    return value
```

在 `FakeDataset.validate_identity_and_scope()` 中验证 taxonomy fixture 和新瞬时失败键：

```python
for batch_key, output in self.model_responses.taxonomies.items():
    slugs = batch_key.split(",")
    if (
        not batch_key
        or slugs != sorted(set(slugs))
        or any(_normalize_slug(slug, ("entity", "concept")) != slug for slug in slugs)
        or tuple(item.slug for item in output.decisions) != tuple(slugs)
    ):
        raise ValueError("taxonomy fixture 必须按规范 slug 排序并精确覆盖 batch key")

for key in self.transient_failures:
    prefix, _, suffix = key.partition(":")
    if prefix == "embedding":
        vector_keys = suffix.split(",")
        if (
            not suffix
            or vector_keys != list(dict.fromkeys(vector_keys))
            or any(item not in self.model_responses.embeddings for item in vector_keys)
        ):
            raise ValueError("embedding 瞬时失败键必须引用已声明向量")
    if prefix == "taxonomy" and suffix not in self.model_responses.taxonomies:
        raise ValueError("taxonomy 瞬时失败键必须引用已声明 batch")
```

`transient_failures` 的 prefix 白名单同步加入 `embedding` 和 `taxonomy`。

实现 fake：

```python
def _batch_key(values: Sequence[str]) -> str:
    return ",".join(values)


class FakeEmbeddingModel:
    def __init__(self, dataset: FakeDataset) -> None:
        self._vectors = deepcopy(dataset.model_responses.embeddings)
        self._remaining_failures = dict(dataset.transient_failures)
        self.calls: list[str] = []
        self.requests: list[EmbeddingRequest] = []

    async def embed(self, request: EmbeddingRequest) -> EmbeddingOutput:
        snapshot = EmbeddingRequest.model_validate(request.model_dump(mode="python"))
        self.requests.append(snapshot)
        call_key = f"embedding:{_batch_key([item.key for item in snapshot.items])}"
        self.calls.append(call_key)
        remaining = self._remaining_failures.get(call_key, 0)
        if remaining > 0:
            self._remaining_failures[call_key] = remaining - 1
            raise TransientModelError(f"模型调用瞬时失败: {call_key}")
        missing = [item.key for item in snapshot.items if item.key not in self._vectors]
        if missing:
            raise PermanentModelError(f"缺少模型响应: {call_key}")
        return EmbeddingOutput(vectors={item.key: self._vectors[item.key] for item in snapshot.items})
```

在 `FakeChatModel` 增加 `taxonomy_requests` 和：

```python
async def plan_folders(self, request: TaxonomyRequest) -> TaxonomyOutput:
    snapshot = TaxonomyRequest.model_validate(request.model_dump(mode="python"))
    self.taxonomy_requests.append(snapshot)
    batch_key = _batch_key([topic.slug for topic in snapshot.topics])
    call_key = f"taxonomy:{batch_key}"
    self._record_call(call_key)
    response = self._responses.taxonomies.get(batch_key)
    if response is None:
        raise PermanentModelError(f"缺少模型响应: {call_key}")
    return response.model_copy(deep=True)
```

保留现有 `load_fake_adapters()` 二元返回值，新增：

```python
def load_fake_runtime_adapters(
    path: str | Path,
) -> tuple[FakeKnowledgeSource, FakeChatModel, FakeEmbeddingModel]:
    dataset = FakeDataset.model_validate_json(Path(path).read_text(encoding="utf-8"))
    return FakeKnowledgeSource(dataset), FakeChatModel(dataset), FakeEmbeddingModel(dataset)
```

- [ ] **步骤 4：运行 fake 和组合协议测试确认 GREEN**

```powershell
# 验证旧 fake 调用及新端口全部通过
uv run pytest tests/wiki/test_ingest_fakes.py -q
```

- [ ] **步骤 5：提交 fake 端口**

```powershell
# 暂存 fake 模型、端口及测试
git add app/wiki/ingest/ports.py app/wiki/ingest/fakes.py tests/wiki/test_ingest_fakes.py

# 创建独立 fake 提交
git commit -m "feat: add fake wiki taxonomy models"
```

---

### 任务 3：Topic 整理与 taxonomy 输出白名单

**文件：**

- 新建：`app/wiki/ingest/taxonomy.py`
- 新建：`tests/wiki/test_ingest_taxonomy.py`

- [ ] **步骤 1：先写 topic 稳定整理和输出恢复失败测试**

```python
from uuid import UUID, uuid4

import pytest

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.schemas import (
    AllowedFolderBase,
    ContributionDelta,
    StoredContributionRecord,
    TaxonomyDecision,
    TaxonomyOutput,
    TaxonomyRequest,
    TaxonomyTopic,
)
from app.wiki.ingest.taxonomy import build_taxonomy_work_items, recover_taxonomy_output


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
OP_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OP_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _add_delta(
    op_id: UUID,
    *,
    slug: str = "entity/acme",
    knowledge_id: str = "source-a",
    summary: str = "Summary",
) -> ContributionDelta:
    current = StoredContributionRecord(
        tenant_id=1,
        knowledge_base_id=KB_ID,
        slug=slug,
        knowledge_id=knowledge_id,
        op_version="v1",
        page_type=slug.partition("/")[0],
        state="active",
        title=slug.rpartition("/")[2].title(),
        content=f"Content {knowledge_id}",
        summary=summary,
    )
    return ContributionDelta(
        pending_op_id=op_id,
        action="add",
        slug=slug,
        knowledge_id=knowledge_id,
        current=current,
    )


def test_build_taxonomy_work_items_is_stable_and_keeps_contributors():
    first = _add_delta(OP_A)
    second = _add_delta(
        OP_B, knowledge_id="source-b", summary="Second summary"
    )
    items = build_taxonomy_work_items(
        [second, first],
        classifiable_slugs=(first.slug,),
    )
    assert [item.topic.slug for item in items] == [first.slug]
    item = items[0]
    assert item.contributor_op_ids == tuple(
        sorted((first.pending_op_id, second.pending_op_id), key=str)
    )
    assert item.topic.summary == "Summary\n\nSecond summary"


def test_recover_taxonomy_output_rejects_missing_unknown_and_non_whitelisted_base():
    topic = TaxonomyTopic(
        slug="entity/acme", title="Acme", page_type="entity", summary="A"
    )
    allowed = AllowedFolderBase(id=uuid4(), path="/Organizations", depth=1)
    request = TaxonomyRequest(topics=(topic,), allowed_bases=(allowed,))
    with pytest.raises(WikiValidationError, match="完整且恰好覆盖"):
        recover_taxonomy_output(request, TaxonomyOutput())
    with pytest.raises(WikiValidationError, match="白名单"):
        recover_taxonomy_output(
            request,
            TaxonomyOutput(
                decisions=(
                    TaxonomyDecision(
                        slug=topic.slug,
                        base_folder_id=uuid4(),
                    ),
                )
            ),
        )
```

- [ ] **步骤 2：运行测试确认 RED**

```powershell
# taxonomy 模块尚不存在，预期 ModuleNotFoundError
uv run pytest tests/wiki/test_ingest_taxonomy.py -q
```

- [ ] **步骤 3：实现 work item 与严格输出恢复**

```python
@dataclass(frozen=True, slots=True)
class TaxonomyWorkItem:
    topic: TaxonomyTopic
    contributor_op_ids: tuple[UUID, ...]


def build_taxonomy_work_items(
    deltas: Sequence[ContributionDelta],
    *,
    classifiable_slugs: Sequence[str],
) -> tuple[TaxonomyWorkItem, ...]:
    allowed = set(classifiable_slugs)
    grouped: dict[str, list[tuple[ContributionDelta, StoredContributionRecord]]] = {}
    for delta in deltas:
        current = delta.current
        if current is None or current.slug not in allowed or current.page_type == "summary":
            continue
        grouped.setdefault(current.slug, []).append((delta, current))
    output: list[TaxonomyWorkItem] = []
    for slug in sorted(grouped):
        pairs = sorted(
            grouped[slug],
            key=lambda pair: (
                pair[1].knowledge_id,
                pair[1].op_version,
                str(pair[0].pending_op_id),
            ),
        )
        records = [record for _, record in pairs]
        summaries = list(dict.fromkeys(record.summary.strip() for record in records if record.summary.strip()))
        output.append(
            TaxonomyWorkItem(
                topic=TaxonomyTopic(
                    slug=slug,
                    title=records[0].title,
                    page_type=records[0].page_type,
                    summary="\n\n".join(summaries)[:4000],
                ),
                contributor_op_ids=tuple(
                    sorted({delta.pending_op_id for delta, _ in pairs}, key=str)
                ),
            )
        )
    return tuple(output)


def recover_taxonomy_output(
    request: TaxonomyRequest,
    output: TaxonomyOutput,
) -> dict[str, TaxonomyDecision]:
    request = TaxonomyRequest.model_validate(request.model_dump(mode="python"))
    output = TaxonomyOutput.model_validate(output.model_dump(mode="python"))
    topics = {topic.slug for topic in request.topics}
    decisions = {decision.slug: decision for decision in output.decisions}
    if len(decisions) != len(output.decisions) or set(decisions) != topics:
        raise WikiValidationError(
            "TAXONOMY_OUTPUT_INVALID", "taxonomy 输出必须完整且恰好覆盖请求 topic"
        )
    allowed = {folder.id: folder for folder in request.allowed_bases}
    for decision in decisions.values():
        base = allowed.get(decision.base_folder_id) if decision.base_folder_id else None
        if decision.base_folder_id is not None and base is None:
            raise WikiValidationError(
                "TAXONOMY_OUTPUT_INVALID", "taxonomy base 不在当前请求白名单"
            )
        depth = base.depth if base else 0
        if depth + len(decision.new_segments) > 3:
            raise WikiValidationError(
                "TAXONOMY_OUTPUT_INVALID", "taxonomy 目录路径超过三级"
            )
        folder_path = base.path if base else ""
        for segment in decision.new_segments:
            folder_path = f"{folder_path}/{segment}"
        wiki_path = (
            f"{folder_path}/{decision.slug}"
            if folder_path
            else f"/{decision.slug}"
        )
        if len(folder_path) > 2048 or len(wiki_path) > 1024:
            raise WikiValidationError(
                "TAXONOMY_OUTPUT_INVALID", "taxonomy 目录或页面路径过长"
            )
    return decisions
```

- [ ] **步骤 4：运行 taxonomy 纯函数测试确认 GREEN**

```powershell
# 验证稳定 topic、白名单和深度
uv run pytest tests/wiki/test_ingest_taxonomy.py -q
```

- [ ] **步骤 5：提交纯 taxonomy 恢复**

```powershell
# 暂存 taxonomy 纯模块和测试
git add app/wiki/ingest/taxonomy.py tests/wiki/test_ingest_taxonomy.py

# 创建独立纯函数提交
git commit -m "feat: validate wiki taxonomy batches"
```

---

### 任务 4：Embedding 候选目录选择

**文件：**

- 修改：`app/wiki/ingest/taxonomy.py`
- 修改：`tests/wiki/test_ingest_taxonomy.py`

- [ ] **步骤 1：先写小目录、大目录、祖先、tie 和零向量测试**

```python
from app.wiki.ingest.schemas import (
    EmbeddingOutput,
    EmbeddingRequest,
    FolderCatalogEntry,
)
from app.wiki.ingest.taxonomy import cosine_similarity, select_allowed_bases


class RecordingEmbedding:
    def __init__(
        self,
        vectors: dict[str, tuple[float, ...]],
        events: list[str] | None = None,
    ) -> None:
        self.vectors = dict(vectors)
        self.events = events
        self.requests: list[EmbeddingRequest] = []

    async def embed(self, request: EmbeddingRequest) -> EmbeddingOutput:
        snapshot = EmbeddingRequest.model_validate(request.model_dump(mode="python"))
        self.requests.append(snapshot)
        if self.events is not None:
            self.events.append("embedding")
        return EmbeddingOutput(
            vectors={item.key: self.vectors[item.key] for item in snapshot.items}
        )


def _topic() -> TaxonomyTopic:
    return TaxonomyTopic(
        slug="entity/acme", title="Acme", page_type="entity", summary="Company"
    )


def _folder_tree() -> tuple[FolderCatalogEntry, ...]:
    organizations = FolderCatalogEntry(
        id=UUID(int=1), parent_id=None, name="Organizations",
        path="/Organizations", depth=1,
    )
    products = FolderCatalogEntry(
        id=UUID(int=2), parent_id=organizations.id, name="Products",
        path="/Organizations/Products", depth=2,
    )
    platform = FolderCatalogEntry(
        id=UUID(int=3), parent_id=products.id, name="Platform",
        path="/Organizations/Products/Platform", depth=3,
    )
    research = FolderCatalogEntry(
        id=UUID(int=4), parent_id=organizations.id, name="Research",
        path="/Organizations/Research", depth=2,
    )
    topics = FolderCatalogEntry(
        id=UUID(int=5), parent_id=None, name="Topics", path="/Topics", depth=1,
    )
    return organizations, products, platform, research, topics


@pytest.mark.asyncio
async def test_small_catalog_returns_every_folder_without_embedding():
    topic = _topic()
    folders = _folder_tree()[:2]
    embedding = RecordingEmbedding({})
    selected = await select_allowed_bases(
        (topic,), folders, embedding, full_catalog_limit=10, related_limit=2
    )
    assert [item.id for item in selected] == [folder.id for folder in folders]
    assert embedding.requests == []


@pytest.mark.asyncio
async def test_large_catalog_keeps_roots_top_related_and_ancestors():
    topic = _topic()
    folder_tree = _folder_tree()
    embedding = RecordingEmbedding(
        {
            f"topic:{topic.slug}": (1.0, 0.0),
            f"folder:{folder_tree[1].id}": (0.2, 0.8),
            f"folder:{folder_tree[2].id}": (1.0, 0.0),
            f"folder:{folder_tree[3].id}": (0.9, 0.1),
        }
    )
    selected = await select_allowed_bases(
        (topic,), folder_tree, embedding, full_catalog_limit=1, related_limit=1
    )
    assert folder_tree[0].id in {item.id for item in selected}
    assert folder_tree[2].id in {item.id for item in selected}
    assert folder_tree[1].id in {item.id for item in selected}


@pytest.mark.asyncio
async def test_large_catalog_breaks_equal_scores_by_stable_folder_order():
    topic = _topic()
    folder_tree = _folder_tree()
    vectors = {f"topic:{topic.slug}": (1.0, 0.0)}
    vectors.update(
        {
            f"folder:{folder.id}": (1.0, 0.0)
            for folder in folder_tree
            if folder.depth > 1
        }
    )
    selected = await select_allowed_bases(
        (topic,), folder_tree, RecordingEmbedding(vectors),
        full_catalog_limit=1, related_limit=1,
    )
    assert folder_tree[1].id in {item.id for item in selected}
    assert folder_tree[2].id not in {item.id for item in selected}


def test_cosine_similarity_treats_zero_vector_as_zero():
    assert cosine_similarity((0.0, 0.0), (1.0, 0.0)) == 0.0
```

- [ ] **步骤 2：运行测试确认 RED**

```powershell
# 候选选择函数尚不存在，预期 ImportError
uv run pytest tests/wiki/test_ingest_taxonomy.py -q
```

- [ ] **步骤 3：实现余弦相似度和候选选择**

```python
def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or not left:
        raise WikiValidationError("EMBEDDING_OUTPUT_INVALID", "embedding 维度不一致")
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return sum(a * b for a, b in zip(left, right, strict=True)) / (
        left_norm * right_norm
    )


async def select_allowed_bases(
    topics: Sequence[TaxonomyTopic],
    folders: Sequence[FolderCatalogEntry],
    embedding: EmbeddingModelPort,
    *,
    full_catalog_limit: int,
    related_limit: int,
) -> tuple[AllowedFolderBase, ...]:
    ordered = sorted(folders, key=lambda item: (item.depth, item.path, str(item.id)))
    if len(ordered) <= full_catalog_limit:
        return tuple(
            AllowedFolderBase(id=item.id, path=item.path, depth=item.depth)
            for item in ordered
        )
    roots = [item for item in ordered if item.depth == 1]
    deep = [item for item in ordered if item.depth > 1]
    request = EmbeddingRequest(
        items=tuple(
            [
                EmbeddingItem(
                    key=f"topic:{topic.slug}",
                    text=f"{topic.title}\n{topic.summary}".strip(),
                )
                for topic in topics
            ]
            + [
                EmbeddingItem(key=f"folder:{folder.id}", text=folder.path)
                for folder in deep
            ]
        )
    )
    raw_output = await embedding.embed(request)
    if not isinstance(raw_output, EmbeddingOutput):
        raise WikiValidationError(
            "EMBEDDING_OUTPUT_INVALID", "embedding 返回类型无效"
        )
    output = EmbeddingOutput.model_validate(raw_output.model_dump(mode="python"))
    requested_keys = {item.key for item in request.items}
    if set(output.vectors) != requested_keys:
        raise WikiValidationError(
            "EMBEDDING_OUTPUT_INVALID", "embedding 输出必须完整覆盖请求 key"
        )
    topic_vectors = [output.vectors[f"topic:{topic.slug}"] for topic in topics]
    scored = [
        (
            max(
                cosine_similarity(output.vectors[f"folder:{folder.id}"], vector)
                for vector in topic_vectors
            ),
            folder,
        )
        for folder in deep
    ]
    chosen = [
        folder
        for _, folder in sorted(
            scored,
            key=lambda item: (-item[0], item[1].depth, item[1].path, str(item[1].id)),
        )[:related_limit]
    ]
    by_id = {folder.id: folder for folder in ordered}
    selected = {folder.id: folder for folder in roots}
    for folder in chosen:
        current: FolderCatalogEntry | None = folder
        while current is not None:
            selected[current.id] = current
            current = by_id.get(current.parent_id) if current.parent_id else None
    return tuple(
        AllowedFolderBase(id=item.id, path=item.path, depth=item.depth)
        for item in sorted(selected.values(), key=lambda value: (value.depth, value.path, str(value.id)))
    )
```

- [ ] **步骤 4：运行定向测试和取消回归**

```powershell
# 验证候选选择、白名单和取消传播
uv run pytest tests/wiki/test_ingest_taxonomy.py -q
```

- [ ] **步骤 5：提交 embedding 选择**

```powershell
# 暂存候选选择及测试
git add app/wiki/ingest/taxonomy.py tests/wiki/test_ingest_taxonomy.py

# 创建独立候选选择提交
git commit -m "feat: select wiki taxonomy folders"
```

---

### 任务 5：Taxonomy 切批和最终 assignment

**文件：**

- 修改：`app/wiki/ingest/taxonomy.py`
- 修改：`tests/wiki/test_ingest_taxonomy.py`

- [ ] **步骤 1：先写 60 topic 切批、根目录和 base 快照测试**

```python
from app.wiki.ingest.taxonomy import (
    build_folder_assignment,
    build_taxonomy_requests,
)


def _work_items(count: int):
    deltas = tuple(
        _add_delta(
            UUID(int=100 + index),
            slug=f"concept/topic-{index:03d}",
            knowledge_id=f"source-{index:03d}",
        )
        for index in range(count)
    )
    return build_taxonomy_work_items(
        deltas, classifiable_slugs=tuple(delta.slug for delta in deltas)
    )


def test_build_taxonomy_requests_splits_sixty_one_topics():
    work_items = _work_items(61)
    requests = build_taxonomy_requests(work_items, (), batch_size=60)
    assert [len(request.topics) for request in requests] == [60, 1]
    assert [topic.slug for request in requests for topic in request.topics] == [
        item.topic.slug for item in work_items
    ]


def test_build_folder_assignment_copies_base_snapshot():
    work_item = _work_items(1)[0]
    base = AllowedFolderBase(id=uuid4(), path="/Organizations", depth=1)
    decision = TaxonomyDecision(
        slug=work_item.topic.slug,
        base_folder_id=base.id,
        new_segments=("Products",),
    )
    assignment = build_folder_assignment(work_item, decision, {base.id: base})
    assert assignment.base_folder_id == base.id
    assert assignment.base_path == base.path
    assert assignment.base_depth == 1
    assert assignment.contributor_op_ids == work_item.contributor_op_ids


def test_build_folder_assignment_rejects_unknown_base():
    work_item = _work_items(1)[0]
    decision = TaxonomyDecision(
        slug=work_item.topic.slug,
        base_folder_id=uuid4(),
    )
    with pytest.raises(WikiValidationError, match="不在白名单"):
        build_folder_assignment(work_item, decision, {})
```

- [ ] **步骤 2：运行测试确认 RED**

```powershell
# 切批和 assignment helper 尚不存在，预期 ImportError
uv run pytest tests/wiki/test_ingest_taxonomy.py -q
```

- [ ] **步骤 3：实现稳定切批和 assignment 构造**

```python
def build_taxonomy_requests(
    work_items: Sequence[TaxonomyWorkItem],
    allowed_bases: Sequence[AllowedFolderBase],
    *,
    batch_size: int,
) -> tuple[TaxonomyRequest, ...]:
    if not 1 <= batch_size <= 60:
        raise ValueError("taxonomy batch_size 必须在 1 到 60 之间")
    ordered = sorted(work_items, key=lambda item: item.topic.slug)
    bases = tuple(allowed_bases)
    return tuple(
        TaxonomyRequest(
            topics=tuple(item.topic for item in ordered[index : index + batch_size]),
            allowed_bases=bases,
        )
        for index in range(0, len(ordered), batch_size)
    )


def build_folder_assignment(
    work_item: TaxonomyWorkItem,
    decision: TaxonomyDecision,
    allowed_by_id: Mapping[UUID, AllowedFolderBase],
) -> FolderAssignment:
    if decision.slug != work_item.topic.slug:
        raise WikiValidationError(
            "TAXONOMY_OUTPUT_INVALID", "taxonomy decision 与 work item slug 不一致"
        )
    base = allowed_by_id.get(decision.base_folder_id) if decision.base_folder_id else None
    if decision.base_folder_id is not None and base is None:
        raise WikiValidationError(
            "TAXONOMY_OUTPUT_INVALID", "taxonomy decision base 不在白名单"
        )
    return FolderAssignment(
        slug=work_item.topic.slug,
        contributor_op_ids=work_item.contributor_op_ids,
        base_folder_id=base.id if base else None,
        base_path=base.path if base else None,
        base_depth=base.depth if base else 0,
        new_segments=decision.new_segments,
    )
```

- [ ] **步骤 4：运行全部 taxonomy 测试确认 GREEN**

```powershell
# 验证 4A 纯规划模块全部合同
uv run pytest tests/wiki/test_ingest_taxonomy.py -q
```

- [ ] **步骤 5：提交 taxonomy 请求构造**

```powershell
# 暂存切批和 assignment
git add app/wiki/ingest/taxonomy.py tests/wiki/test_ingest_taxonomy.py

# 创建独立批次合同提交
git commit -m "feat: batch wiki taxonomy topics"
```

---

### 任务 6：Store taxonomy context 与真正新页面识别

**文件：**

- 修改：`app/wiki/ingest/store.py:166-252, 1575-1655`
- 修改：`tests/wiki/test_ingest_store.py`
- 修改：`tests/wiki/test_postgres_integration.py`

- [ ] **步骤 1：先写 scope、窄列、历史删除页和稳定顺序测试**

```python
@pytest.mark.asyncio
async def test_taxonomy_context_excludes_active_and_historical_pages():
    active = _page(slug="entity/active")
    deleted = _page(slug="entity/deleted")
    deleted.deleted_at = NOW
    folder = WikiFolder(
        tenant_id=SCOPE.tenant_id,
        knowledge_base_id=SCOPE.knowledge_base_id,
        parent_id=None,
        name="Organizations",
        path="/Organizations",
        depth=1,
    )
    session = _ScriptedSession(
        [
            _ScriptedResult(rows=[folder]),
            _ScriptedResult(rows=[active.slug, deleted.slug]),
        ]
    )
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), SqlFinalizationPort())
    context = await store.load_taxonomy_context(
        SCOPE,
        ["entity/new", active.slug, deleted.slug, "summary/source-a"],
    )
    assert context.classifiable_slugs == ("entity/new",)
    assert context.folders[0].id == folder.id
    assert "wiki_folders.content" not in _sql(session.statements[0])
    assert "wiki_pages.content" not in _sql(session.statements[1])


@pytest.mark.asyncio
async def test_real_taxonomy_context_is_scoped_and_includes_folder_ancestors(
    postgres_factory,
):
    scope = WikiScope(
        tenant_id=30, knowledge_base_id=uuid4(), actor_id="worker", can_write=True
    )
    organizations = WikiFolder(
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        parent_id=None,
        name="Organizations",
        path="/Organizations",
        depth=1,
    )
    async with postgres_factory() as session, session.begin():
        session.add(organizations)
        await session.flush()
        session.add_all(
            [
                WikiFolder(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    parent_id=organizations.id,
                    name="Products",
                    path="/Organizations/Products",
                    depth=2,
                ),
                WikiPage(
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    slug="entity/existing",
                    title="Existing",
                    page_type="entity",
                    status="published",
                    wiki_path="/entity/existing",
                ),
            ]
        )
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    context = await store.load_taxonomy_context(
        scope, ["entity/new", "entity/existing"]
    )
    assert context.classifiable_slugs == ("entity/new",)
    assert [(item.depth, item.path) for item in context.folders] == [
        (1, "/Organizations"),
        (2, "/Organizations/Products"),
]
```

在 `tests/wiki/test_ingest_store.py` 与 `tests/wiki/test_postgres_integration.py` 的 ORM import 中补入 `WikiFolder`；集成测试复用现有 `postgres_factory`，不依赖额外 fixture。

- [ ] **步骤 2：运行测试确认 RED**

```powershell
# Store 协议尚无 taxonomy context，预期 AttributeError
uv run pytest tests/wiki/test_ingest_store.py -q
```

- [ ] **步骤 3：实现协议和窄列查询**

在 `IngestStore` 增加：

```python
async def load_taxonomy_context(
    self,
    scope: WikiScope,
    slugs: Iterable[str],
) -> TaxonomyContext: ...
```

在 `SqlAlchemyIngestStore` 实现：

```python
async def load_taxonomy_context(
    self,
    scope: WikiScope,
    slugs: Iterable[str],
) -> TaxonomyContext:
    requested = sorted(
        {
            _normalize_topic_slug(slug)
            for slug in slugs
            if isinstance(slug, str)
            and (slug.strip().casefold().startswith("entity/") or slug.strip().casefold().startswith("concept/"))
        }
    )
    async with self._session_factory() as session:
        folder_rows = list(
            (
                await session.execute(
                    select(
                        WikiFolder.id,
                        WikiFolder.parent_id,
                        WikiFolder.name,
                        WikiFolder.path,
                        WikiFolder.depth,
                    )
                    .where(
                        WikiFolder.tenant_id == scope.tenant_id,
                        WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                        WikiFolder.deleted_at.is_(None),
                    )
                    .order_by(WikiFolder.depth, WikiFolder.path, WikiFolder.id)
                )
            ).all()
        )
        occupied = (
            set(
                (
                    await session.execute(
                        select(WikiPage.slug).where(
                            WikiPage.tenant_id == scope.tenant_id,
                            WikiPage.knowledge_base_id == scope.knowledge_base_id,
                            WikiPage.slug.in_(requested),
                        )
                    )
                ).scalars()
            )
            if requested
            else set()
        )
    try:
        return TaxonomyContext(
            folders=tuple(
                FolderCatalogEntry(
                    id=row.id,
                    parent_id=row.parent_id,
                    name=row.name,
                    path=row.path,
                    depth=row.depth,
                )
                for row in folder_rows
            ),
            classifiable_slugs=tuple(
                slug for slug in requested if slug not in occupied
            ),
        )
    except (ValidationError, TypeError, ValueError) as exc:
        raise InvariantError("taxonomy context 查询返回脏数据") from exc
```

在 Store 中增加同一条规范化边界，复用现有 `TopicCandidate` 校验，不接受 summary：

```python
def _normalize_topic_slug(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("taxonomy slug 必须是字符串")
    normalized = value.strip().casefold()
    page_type, separator, name = normalized.partition("/")
    if not separator or page_type not in {"entity", "concept"}:
        raise ValueError("taxonomy slug 必须是 entity 或 concept")
    return TopicCandidate(
        name=name,
        slug=normalized,
        page_type=page_type,
    ).slug
```

- [ ] **步骤 4：运行 Store 和真实 PostgreSQL 定向测试**

```powershell
# 验证 context 单元合同；真实 URL 未配置时集成项明确跳过
uv run pytest tests/wiki/test_ingest_store.py tests/wiki/test_postgres_integration.py -q
```

- [ ] **步骤 5：提交 taxonomy context**

```powershell
# 暂存 context 查询及测试
git add app/wiki/ingest/store.py tests/wiki/test_ingest_store.py tests/wiki/test_postgres_integration.py

# 创建独立查询提交
git commit -m "feat: load wiki taxonomy context"
```

---

### 任务 7：Batch assignment 边界和 legacy 根目录兼容

**文件：**

- 修改：`app/wiki/ingest/store.py:626-893`
- 修改：`tests/wiki/test_ingest_store.py`
- 修改：`tests/wiki/test_ingest_schemas.py`
- 修改：`tests/wiki/test_postgres_integration.py`

- [ ] **步骤 1：先写已有页、summary、forged contributor 和 legacy 模式测试**

```python
def test_batch_request_rejects_folder_assignment_for_existing_page():
    op_id = uuid4()
    page = _result_page().model_copy(update={"contributor_op_ids": [op_id]})
    current = _stored_contribution()
    request = _batch_request(
        pages=(page,),
        completed_op_ids=(op_id,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=op_id,
                action="add",
                slug=page.slug,
                knowledge_id=current.knowledge_id,
                current=current,
            ),
        ),
        expected_pages=(PageExpectation(slug=page.slug, page_id=uuid4(), version=1),),
        folder_assignments=(
            FolderAssignment(
                slug=page.slug,
                contributor_op_ids=tuple(page.contributor_op_ids),
                base_folder_id=None,
                base_path=None,
                base_depth=0,
            ),
        ),
    )
    with pytest.raises(InvariantError, match="新页面"):
        _validate_batch_request(request)


def test_batch_request_rejects_folder_assignment_for_summary_page():
    op_id = uuid4()
    page = _result_page().model_copy(
        update={
            "slug": "summary/source-a",
            "page_type": "summary",
            "contributor_op_ids": [op_id],
        }
    )
    current = _stored_contribution().model_copy(
        update={"slug": page.slug, "page_type": "summary"}
    )
    request = _batch_request(
        pages=(page,),
        completed_op_ids=(op_id,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=op_id,
                action="add",
                slug=page.slug,
                knowledge_id=current.knowledge_id,
                current=current,
            ),
        ),
        expected_pages=(PageExpectation(slug=page.slug),),
        folder_assignments=(
            FolderAssignment(
                slug=page.slug,
                contributor_op_ids=tuple(page.contributor_op_ids),
                base_folder_id=None,
                base_path=None,
                base_depth=0,
            ),
        ),
    )
    with pytest.raises(InvariantError, match="topic 页面"):
        _validate_batch_request(request)


def test_batch_request_rejects_assignment_contributor_mismatch():
    op_a, op_b = uuid4(), uuid4()
    page = _result_page().model_copy(update={"contributor_op_ids": [op_a]})
    current = _stored_contribution()
    request = _batch_request(
        pages=(page,),
        completed_op_ids=(op_a, op_b),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=op_a,
                action="add",
                slug=page.slug,
                knowledge_id=current.knowledge_id,
                current=current,
            ),
        ),
        expected_pages=(PageExpectation(slug=page.slug),),
        folder_assignments=(
            FolderAssignment(
                slug=page.slug,
                contributor_op_ids=(op_b,),
                base_folder_id=None,
                base_path=None,
                base_depth=0,
            ),
        ),
    )
    with pytest.raises(InvariantError, match="contributor"):
        _validate_batch_request(request)


def test_legacy_apply_keeps_root_placement_without_taxonomy():
    claim_token, op_id = uuid4(), uuid4()
    page = _result_page().model_copy(update={"contributor_op_ids": [op_id]})
    checked = _legacy_batch_request(
        claim_token,
        [page],
        [op_id],
        uuid4(),
        [],
        {page.slug: None},
    )
    assert checked.folder_assignments == ()
```

在 Schema import 中补入 `FolderAssignment`，在 Store import 中补入 `_legacy_batch_request` 和 `_validate_batch_request`。

- [ ] **步骤 2：运行测试确认 RED**

```powershell
# Store 尚未校验目录 assignment，预期断言失败
uv run pytest tests/wiki/test_ingest_store.py tests/wiki/test_ingest_schemas.py -q
```

- [ ] **步骤 3：扩展 `_validate_batch_request()` 并显式区分现代/legacy 应用模式**

在 `_validate_batch_request()` 增加：

```python
assignments = {item.slug: item for item in snapshot.folder_assignments}
if len(assignments) != len(snapshot.folder_assignments):
    raise InvariantError("folder assignment slug 不能重复")
pages_by_slug = {page.slug: page for page in snapshot.pages}
expectations = {item.slug: item for item in snapshot.expected_pages}
for slug, assignment in assignments.items():
    page = pages_by_slug.get(slug)
    expectation = expectations.get(slug)
    if page is None or expectation is None:
        raise InvariantError("folder assignment 必须属于结果页面")
    if page.page_type not in {"entity", "concept"} or page.deleted:
        raise InvariantError("folder assignment 只能用于未删除 topic 页面")
    if expectation.page_id is not None:
        raise InvariantError("folder assignment 不能覆盖已有页面")
    if set(assignment.contributor_op_ids) != set(page.contributor_op_ids):
        raise InvariantError("folder assignment contributor 必须匹配结果页面")
    if not set(assignment.contributor_op_ids).issubset(completed_ids):
        raise InvariantError("folder assignment contributor 必须属于 completed operation")
```

不要让 `_legacy_batch_request()` 生成 assignment。legacy 入口只有 `expected page_id=None`，无法区分数据库中完全不存在的页面和待恢复的软删除历史页面；提前生成 assignment 会把历史恢复误判为新页面。

把现代 `BatchApplyRequest` 和 legacy positional 调用显式传给 `_apply_checked_results()`：

```python
async def apply_results(
    self,
    scope: WikiScope,
    request: BatchApplyRequest | UUID | None,
    pages: Sequence[ReducedPage] | None = None,
    completed_op_ids: Sequence[UUID] | None = None,
    operation_id: UUID | None = None,
    *,
    failed_op_ids: Sequence[UUID] = (),
    expected_pages: Mapping[str, ExistingPageRecord | None] | None = None,
) -> bool:
    if isinstance(request, BatchApplyRequest):
        if (
            pages is not None
            or completed_op_ids is not None
            or operation_id is not None
            or failed_op_ids
            or expected_pages is not None
        ):
            raise TypeError("现代 apply_results 不能混用 legacy 参数")
        checked = _validate_batch_request(request)
        require_taxonomy = True
    else:
        if pages is None or completed_op_ids is None:
            raise TypeError("legacy apply_results 参数不完整")
        if operation_id is None and (completed_op_ids or failed_op_ids or pages):
            raise TypeError("legacy apply_results 参数不完整")
        checked = _legacy_batch_request(
            request,
            pages,
            completed_op_ids,
            operation_id,
            failed_op_ids,
            expected_pages,
        )
        require_taxonomy = False
    return (
        await self._apply_checked_results(
            scope, checked, require_taxonomy=require_taxonomy
        )
    ).applied


async def apply_results_with_outcome(
    self,
    scope: WikiScope,
    request: BatchApplyRequest,
) -> BatchApplyOutcome:
    return await self._apply_checked_results(
        scope,
        _validate_batch_request(request),
        require_taxonomy=True,
    )


async def _apply_checked_results(
    self,
    scope: WikiScope,
    checked: BatchApplyRequest,
    *,
    require_taxonomy: bool,
) -> BatchApplyOutcome:
    if not (
        checked.pages
        or checked.contribution_deltas
        or checked.completed_op_ids
        or checked.superseded_op_ids
        or checked.failures
        or checked.expected_pages
    ):
        return _batch_apply_outcome(checked, applied=False)
    return await self._apply_batch_results(
        scope, checked, require_taxonomy=require_taxonomy
    )
```

`_apply_batch_results()` 同样增加 keyword-only `require_taxonomy: bool`。把 Store 单元测试 helper 改成只为形态有效、期望不存在的现代 topic 自动生成根目录 assignment；显式传 `folder_assignments=()` 仍可测试历史恢复或缺失 assignment：

```python
_UNSET_ASSIGNMENTS = object()


def _batch_request(*, folder_assignments=_UNSET_ASSIGNMENTS, **updates) -> BatchApplyRequest:
    values = {
        "claim_token": uuid4(),
        "pages": (),
        "contribution_deltas": (),
        "completed_op_ids": (uuid4(),),
        "superseded_op_ids": (),
        "failures": (),
        "expected_pages": (),
        "operation_id": uuid4(),
    }
    values.update(updates)
    if folder_assignments is _UNSET_ASSIGNMENTS:
        expected = {item.slug: item for item in values["expected_pages"]}
        folder_assignments = tuple(
            FolderAssignment(
                slug=page.slug,
                contributor_op_ids=tuple(page.contributor_op_ids),
                base_folder_id=None,
                base_path=None,
                base_depth=0,
            )
            for page in values["pages"]
            if page.page_type in {"entity", "concept"}
            and not page.deleted
            and page.contributor_op_ids
            and page.slug in expected
            and expected[page.slug].page_id is None
        )
    values["folder_assignments"] = folder_assignments
    return BatchApplyRequest(**values)
```

在 `test_two_source_canonical_apply_and_outcome_replay_are_idempotent` 的现代请求中补入：

```python
folder_assignments=(
    FolderAssignment(
        slug=reduced.slug,
        contributor_op_ids=tuple(item.id for item in claimed),
        base_folder_id=None,
        base_path=None,
        base_depth=0,
    ),
),
```

- [ ] **步骤 4：运行 Schema/Store 回归确认 GREEN**

```powershell
# 验证 assignment 边界和 legacy 兼容
uv run pytest tests/wiki/test_ingest_schemas.py tests/wiki/test_ingest_store.py -q
```

- [ ] **步骤 5：提交结果请求不变量**

```powershell
# 暂存 Store 边界与测试
git add app/wiki/ingest/store.py tests/wiki/test_ingest_store.py tests/wiki/test_ingest_schemas.py tests/wiki/test_postgres_integration.py

# 创建独立不变量提交
git commit -m "feat: validate wiki folder assignments"
```

---

### 任务 8：Store 原子目录解析和页面缓存

**文件：**

- 修改：`app/wiki/ingest/store.py:1783-2130`
- 修改：`tests/wiki/test_ingest_store.py`
- 修改：`tests/wiki/test_postgres_integration.py`

- [ ] **步骤 1：先写根目录、父子创建、历史恢复和回滚测试**

```python
async def _claimed_topic_request(
    postgres_factory,
    *,
    scope: WikiScope | None = None,
    slug: str = "entity/acme",
    segments: tuple[str, ...] | None = ("Organizations", "Products"),
    base: WikiFolder | None = None,
) -> tuple[WikiScope, SqlAlchemyIngestStore, BatchApplyRequest, PendingOpRecord]:
    scope = scope or WikiScope(
        tenant_id=31,
        knowledge_base_id=uuid4(),
        actor_id="taxonomy-worker",
        can_write=True,
    )
    store = SqlAlchemyIngestStore(postgres_factory, SqlFinalizationPort())
    knowledge = SourceKnowledge(
        id=f"source-{slug.rpartition('/')[2]}",
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        title="Taxonomy source",
        op_version="v1",
    )
    await store.enqueue_ingest(
        scope, knowledge, {"knowledge_id": knowledge.id}, delay_seconds=0
    )
    pending = (await store.claim_pending(scope, 1, 600))[0]
    assert pending.claim_token is not None
    page_type = slug.partition("/")[0]
    current = StoredContributionRecord(
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        slug=slug,
        knowledge_id=knowledge.id,
        op_version=knowledge.op_version,
        page_type=page_type,
        state="active",
        title="Acme",
        content="Acme body",
        summary="Acme summary",
    )
    page = ReducedPage(
        slug=slug,
        title="Acme",
        page_type=page_type,
        content="Acme body",
        summary="Acme summary",
        source_refs=[knowledge.id],
        contributor_op_ids=[pending.id],
    )
    assignments = (
        (
            FolderAssignment(
                slug=slug,
                contributor_op_ids=(pending.id,),
                base_folder_id=base.id if base is not None else None,
                base_path=base.path if base is not None else None,
                base_depth=base.depth if base is not None else 0,
                new_segments=segments,
            ),
        )
        if segments is not None
        else ()
    )
    request = BatchApplyRequest(
        claim_token=pending.claim_token,
        pages=(page,),
        contribution_deltas=(
            ContributionDelta(
                pending_op_id=pending.id,
                action="add",
                slug=slug,
                knowledge_id=knowledge.id,
                current=current,
            ),
        ),
        completed_op_ids=(pending.id,),
        superseded_op_ids=(),
        failures=(),
        expected_pages=(PageExpectation(slug=slug),),
        folder_assignments=assignments,
        operation_id=uuid4(),
    )
    return scope, store, request, pending


@pytest.mark.asyncio
async def test_store_creates_folder_chain_and_sets_page_location_cache(
    postgres_factory,
):
    scope, store, request, pending = await _claimed_topic_request(postgres_factory)
    result = await store.apply_results_with_outcome(scope, request)
    assert result.completed_op_ids == (pending.id,)
    async with postgres_factory() as session:
        folders = list(
            (
                await session.execute(
                    select(WikiFolder)
                    .where(
                        WikiFolder.tenant_id == scope.tenant_id,
                        WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                    )
                    .order_by(WikiFolder.depth)
                )
            ).scalars()
        )
        page = (
            await session.execute(
                select(WikiPage).where(
                    WikiPage.tenant_id == scope.tenant_id,
                    WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    WikiPage.slug == "entity/acme",
                )
            )
        ).scalar_one()
        assert [(item.name, item.depth) for item in folders] == [
            ("Organizations", 1),
            ("Products", 2),
        ]
        assert page.folder_id == folders[-1].id
        assert page.category_path == ["Organizations", "Products"]
        assert page.wiki_path == "/Organizations/Products/entity/acme"
        assert page.depth == 2 and page.version == 1


@pytest.mark.asyncio
async def test_store_reuses_existing_parent_and_creates_only_missing_child(
    postgres_factory,
):
    scope = WikiScope(
        tenant_id=35, knowledge_base_id=uuid4(), actor_id="worker", can_write=True
    )
    organizations = WikiFolder(
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        parent_id=None,
        name="Organizations",
        path="/Organizations",
        depth=1,
    )
    async with postgres_factory() as session, session.begin():
        session.add(organizations)
        await session.flush()
    _, store, request, _ = await _claimed_topic_request(
        postgres_factory, scope=scope
    )
    await store.apply_results_with_outcome(scope, request)
    async with postgres_factory() as session:
        folders = list(
            (
                await session.execute(
                    select(WikiFolder)
                    .where(WikiFolder.knowledge_base_id == scope.knowledge_base_id)
                    .order_by(WikiFolder.depth)
                )
            ).scalars()
        )
        assert len(folders) == 2
        assert folders[0].id == organizations.id
        assert folders[1].parent_id == organizations.id
        assert folders[1].path == "/Organizations/Products"


@pytest.mark.asyncio
async def test_store_preserves_historical_page_folder_and_creates_no_planned_folder(
    postgres_factory,
):
    scope = WikiScope(
        tenant_id=32, knowledge_base_id=uuid4(), actor_id="worker", can_write=True
    )
    original_folder = WikiFolder(
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        parent_id=None,
        name="Manual",
        path="/Manual",
        depth=1,
    )
    async with postgres_factory() as session, session.begin():
        session.add(original_folder)
        await session.flush()
        session.add(
            WikiPage(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                slug="entity/acme",
                title="Historical",
                page_type="entity",
                status="published",
                folder_id=original_folder.id,
                category_path=["Manual"],
                wiki_path="/Manual/entity/acme",
                depth=1,
                deleted_at=datetime.now(UTC),
            )
        )
    _, store, request, _ = await _claimed_topic_request(
        postgres_factory, scope=scope, segments=None
    )
    await store.apply_results_with_outcome(scope, request)
    async with postgres_factory() as session:
        restored = (
            await session.execute(
                select(WikiPage).where(
                    WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    WikiPage.slug == "entity/acme",
                    WikiPage.deleted_at.is_(None),
                )
            )
        ).scalar_one()
        assert restored.folder_id == original_folder.id
        assert restored.category_path == ["Manual"]
        assert restored.wiki_path == "/Manual/entity/acme"
        assert await session.scalar(
            select(func.count(WikiFolder.id)).where(
                WikiFolder.knowledge_base_id == scope.knowledge_base_id
            )
        ) == 1


@pytest.mark.asyncio
async def test_page_conflict_rolls_back_new_taxonomy_folders(postgres_factory):
    scope, store, request, pending = await _claimed_topic_request(postgres_factory)
    async with postgres_factory() as session, session.begin():
        session.add(
            WikiPage(
                tenant_id=scope.tenant_id,
                knowledge_base_id=scope.knowledge_base_id,
                slug=request.pages[0].slug,
                title="Concurrent",
                page_type="entity",
                status="published",
                wiki_path=f"/{request.pages[0].slug}",
            )
        )
    with pytest.raises(PageConflict):
        await store.apply_results_with_outcome(scope, request)
    async with postgres_factory() as session:
        assert await session.scalar(
            select(func.count(WikiFolder.id)).where(
                WikiFolder.knowledge_base_id == scope.knowledge_base_id
            )
        ) == 0
        row = await session.get(WikiPendingOp, pending.id)
        assert row is not None and row.fail_count == 0


@pytest.mark.asyncio
async def test_store_rejects_cross_scope_taxonomy_base(postgres_factory):
    scope = WikiScope(
        tenant_id=36, knowledge_base_id=uuid4(), actor_id="worker", can_write=True
    )
    foreign = WikiFolder(
        tenant_id=99,
        knowledge_base_id=uuid4(),
        parent_id=None,
        name="Foreign",
        path="/Foreign",
        depth=1,
    )
    async with postgres_factory() as session, session.begin():
        session.add(foreign)
        await session.flush()
    _, store, request, pending = await _claimed_topic_request(
        postgres_factory,
        scope=scope,
        segments=("Child",),
        base=foreign,
    )
    with pytest.raises(PageConflict, match="移动或失效"):
        await store.apply_results_with_outcome(scope, request)
    async with postgres_factory() as session:
        assert await session.scalar(
            select(func.count(WikiFolder.id)).where(
                WikiFolder.knowledge_base_id == scope.knowledge_base_id
            )
        ) == 0
        row = await session.get(WikiPendingOp, pending.id)
        assert row is not None and row.claim_token == request.claim_token
```

在该测试文件的 import 中补入 `FolderAssignment`、`PendingOpRecord` 和 `WikiFolder`；以上 helper 与测试均放在 `tests/wiki/test_postgres_integration.py`，不创建跨文件 fixture。

- [ ] **步骤 2：运行测试确认 RED**

```powershell
# Store 尚未解析 assignment，预期 folder_id/cache 断言失败
uv run pytest tests/wiki/test_ingest_store.py tests/wiki/test_postgres_integration.py -q
```

- [ ] **步骤 3：实现目录锁定、复用和创建 helper**

不要调用 `SqlAlchemyFolderStore.insert_folder()`，因为它在 `IntegrityError` 时执行 `session.rollback()`，会破坏当前外层结果事务。直接在 ingest Store 内实现：

```python
async def _resolve_folder_assignment(
    session: AsyncSession,
    scope: WikiScope,
    assignment: FolderAssignment,
) -> tuple[UUID | None, list[str], str, int]:
    parent: WikiFolder | None = None
    if assignment.base_folder_id is not None:
        parent = (
            await session.execute(
                select(WikiFolder)
                .where(
                    WikiFolder.tenant_id == scope.tenant_id,
                    WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                    WikiFolder.id == assignment.base_folder_id,
                    WikiFolder.deleted_at.is_(None),
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if (
            parent is None
            or parent.path != assignment.base_path
            or parent.depth != assignment.base_depth
        ):
            raise PageConflict("taxonomy base 目录已移动或失效")
    for segment in assignment.new_segments:
        parent_id = parent.id if parent is not None else None
        parent_path = parent.path if parent is not None else ""
        depth = (parent.depth if parent is not None else 0) + 1
        path = f"{parent_path}/{segment}"
        inserted_id = (
            await session.execute(
                postgresql.insert(WikiFolder)
                .values(
                    id=uuid4(),
                    tenant_id=scope.tenant_id,
                    knowledge_base_id=scope.knowledge_base_id,
                    parent_id=parent_id,
                    name=segment,
                    path=path,
                    depth=depth,
                    sort_order=0,
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        WikiFolder.knowledge_base_id,
                        WikiFolder.parent_id,
                        WikiFolder.name,
                    ],
                    index_where=WikiFolder.deleted_at.is_(None),
                )
                .returning(WikiFolder.id)
            )
        ).scalar_one_or_none()
        parent_filter = (
            WikiFolder.parent_id.is_(None)
            if parent_id is None
            else WikiFolder.parent_id == parent_id
        )
        parent = (
            await session.execute(
                select(WikiFolder)
                .where(
                    WikiFolder.tenant_id == scope.tenant_id,
                    WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                    parent_filter,
                    WikiFolder.name == segment,
                    WikiFolder.deleted_at.is_(None),
                )
                .with_for_update()
            )
        ).scalar_one()
        if parent.path != path or parent.depth != depth:
            raise InvariantError("同级目录唯一行的 path 或 depth 冲突")
        if inserted_id is not None and parent.id != inserted_id:
            raise InvariantError("新目录 returning 身份与锁定行不一致")
    if parent is None:
        return None, [], f"/{assignment.slug}", 0
    category_path = parent.path.removeprefix("/").split("/")
    return parent.id, category_path, f"{parent.path}/{assignment.slug}", parent.depth
```

- [ ] **步骤 4：接入 `_apply_batch_results()` 的页面选择和创建**

在 `selected_pages` 完成后：

```python
assignments_by_slug = {
    item.slug: item
    for item in request.folder_assignments
    if set(item.contributor_op_ids).issubset(completed_id_set)
}
new_topics = {
    reduced.slug
    for row, reduced in selected_pages
    if row is None and not reduced.deleted and reduced.page_type in {"entity", "concept"}
}
if require_taxonomy and set(assignments_by_slug) != new_topics:
    raise InvariantError("folder assignments 必须精确覆盖真正新建 topic 页面")
placements = {
    slug: await _resolve_folder_assignment(session, scope, assignments_by_slug[slug])
    for slug in sorted(new_topics & assignments_by_slug.keys())
}
```

创建 `WikiPage` 时加入：

```python
folder_id, category_path, wiki_path, depth = placements.get(
    reduced.slug,
    (None, [], f"/{reduced.slug}", 0),
)
row = WikiPage(
    tenant_id=scope.tenant_id,
    knowledge_base_id=scope.knowledge_base_id,
    slug=reduced.slug,
    title=reduced.title,
    page_type=reduced.page_type,
    status="published",
    content=reduced.content,
    summary=reduced.summary,
    aliases=list(reduced.aliases),
    folder_id=folder_id,
    category_path=category_path,
    wiki_path=wiki_path,
    depth=depth,
    source_refs=list(reduced.source_refs),
    chunk_refs=list(reduced.chunk_refs),
    version=1,
)
```

现代请求以 `require_taxonomy=True` 精确覆盖真正新建 topic；Store 自动 supersede 掉的 assignment 先按最终 `completed_id_set` 过滤。legacy 请求以 `False` 进入，同样的新页面在 `placements` 缺省分支保持根目录。历史软删除行进入 `row is not None` 分支，不读取或应用 assignment，因此保留原目录字段；目录字段不加入已有页面的 `values` 版本比较。

- [ ] **步骤 5：运行 Store、页面、目录和真实 PostgreSQL 回归**

```powershell
# 验证原子目录、页面缓存和已有目录 CRUD 不回归
uv run pytest tests/wiki/test_ingest_store.py tests/wiki/test_postgres_integration.py tests/wiki/test_folder_service.py tests/wiki/test_page_service.py -q
```

- [ ] **步骤 6：提交原子目录写入**

```powershell
# 暂存 Store 原子写入及测试
git add app/wiki/ingest/store.py tests/wiki/test_ingest_store.py tests/wiki/test_postgres_integration.py

# 创建独立事务提交
git commit -m "feat: apply wiki taxonomy folders atomically"
```

---

### 任务 9：Worker taxonomy/Reduce 固定点

**文件：**

- 修改：`app/wiki/ingest/worker.py:110-455`
- 修改：`tests/wiki/test_ingest_worker.py`

- [ ] **步骤 1：先写调用顺序、失败隔离、共享 slug 和重试测试**

```python
class OrderedTaxonomyModel(FakeChatModel):
    def __init__(self, dataset: FakeDataset, events: list[str]) -> None:
        super().__init__(dataset)
        self.events = events

    async def extract_candidates(self, knowledge_id, text, config):
        self.events.append("map")
        return await super().extract_candidates(knowledge_id, text, config)

    async def plan_folders(self, request):
        self.events.append("taxonomy")
        return await super().plan_folders(request)

    async def merge_page(self, request):
        self.events.append("reduce")
        return await super().merge_page(request)


class FailingEmbedding:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    async def embed(self, request: EmbeddingRequest) -> EmbeddingOutput:
        self.calls += 1
        if self.calls <= self.failures:
            raise TransientModelError("embedding unavailable")
        return EmbeddingOutput(
            vectors={item.key: (1.0, 0.0) for item in request.items}
        )


class NeverEmbedding:
    async def embed(self, _request: EmbeddingRequest) -> EmbeddingOutput:
        raise AssertionError("空目录 catalog 不应调用 embedding")


@pytest.mark.asyncio
async def test_worker_plans_taxonomy_after_map_before_reduce():
    events: list[str] = []
    dataset = fake_dataset(("doc-a",))
    store = WorkerStore([pending_op(OP_A, "doc-a")], events=events)
    model = OrderedTaxonomyModel(dataset, events)
    result = await worker(
        store, FakeKnowledgeSource(dataset), model
    ).run_batch(SCOPE)
    assert result.completed_op_ids == (OP_A,)
    assert events.index("map") < events.index("taxonomy") < events.index("reduce")
    request = store.apply_calls[0]
    assert request.folder_assignments[0].slug == "entity/shared"


@pytest.mark.asyncio
async def test_taxonomy_batch_failure_only_fails_contributing_operations():
    dataset = fake_dataset(
        concepts_by_knowledge={
            "doc-a": ("concept/a-only",),
            "doc-b": ("concept/b-only",),
        },
        include_shared=False,
    )

    class SelectiveTaxonomyModel(FakeChatModel):
        async def plan_folders(self, request):
            if request.topics[0].slug == "concept/a-only":
                raise PermanentModelError("taxonomy failed")
            return await super().plan_folders(request)

    store = WorkerStore(
        [pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")]
    )
    result = await worker(
        store,
        FakeKnowledgeSource(dataset),
        SelectiveTaxonomyModel(dataset),
        options=WikiWorkerOptions(taxonomy_topic_batch_size=1),
    ).run_batch(SCOPE)
    assert result.failed_op_ids == (OP_A,)
    assert result.completed_op_ids == (OP_B,)
    request = store.apply_calls[0]
    assert all(OP_A not in item.contributor_op_ids for item in request.folder_assignments)


@pytest.mark.asyncio
@pytest.mark.parametrize("context_kind", ["active", "historical"])
async def test_worker_does_not_classify_existing_or_historical_topic(context_kind):
    dataset = fake_dataset(("doc-a",))
    existing = (
        {
            "entity/shared": ExistingPageRecord(
                page_id=uuid4(),
                version=1,
                page=ReducedPage(
                    slug="entity/shared",
                    title="Manual",
                    page_type="entity",
                    content="Manual body",
                    summary="Manual summary",
                ),
            )
        }
        if context_kind == "active"
        else {}
    )
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")],
        existing=existing,
        classifiable_slugs=() if context_kind == "historical" else None,
    )
    model = FakeChatModel(dataset)
    result = await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)
    assert result.completed_op_ids == (OP_A,)
    assert model.taxonomy_requests == []
    assert store.apply_calls[0].folder_assignments == ()


@pytest.mark.asyncio
async def test_reduce_failure_restarts_taxonomy_with_current_contributors():
    dataset = fake_dataset(
        concepts_by_knowledge={"doc-a": ("concept/alpha",)},
        omitted_merges={"concept/alpha"},
    )
    store = WorkerStore([pending_op(OP_A, "doc-a"), pending_op(OP_B, "doc-b")])
    model = FakeChatModel(dataset)
    await worker(store, FakeKnowledgeSource(dataset), model).run_batch(SCOPE)
    assert len(model.taxonomy_requests) == 2
    assignment = store.apply_calls[0].folder_assignments[0]
    assert assignment.contributor_op_ids == (OP_B,)


@pytest.mark.asyncio
async def test_embedding_transient_failure_retries_three_attempts():
    folders = (
        FolderCatalogEntry(
            id=UUID(int=1), parent_id=None, name="Organizations",
            path="/Organizations", depth=1,
        ),
        FolderCatalogEntry(
            id=UUID(int=2), parent_id=UUID(int=1), name="Products",
            path="/Organizations/Products", depth=2,
        ),
    )
    dataset = fake_dataset(("doc-a",))
    store = WorkerStore([pending_op(OP_A, "doc-a")], folders=folders)
    embedding = FailingEmbedding(failures=2)
    waits: list[int] = []
    result = await worker(
        store,
        FakeKnowledgeSource(dataset),
        FakeChatModel(dataset),
        embedding=embedding,
        waits=waits,
        options=WikiWorkerOptions(taxonomy_full_catalog_limit=1),
    ).run_batch(SCOPE)
    assert result.completed_op_ids == (OP_A,)
    assert embedding.calls == 3
    assert waits == [2, 4]


@pytest.mark.asyncio
async def test_taxonomy_child_cancellation_cleans_up_sibling_before_propagating():
    dataset = fake_dataset(
        ("doc-a",), concepts_by_knowledge={"doc-a": ("concept/alpha",)}
    )

    class CancellingTaxonomyModel(FakeChatModel):
        def __init__(self) -> None:
            super().__init__(dataset)
            self.started: set[str] = set()
            self.both_started = asyncio.Event()
            self.release_sibling = asyncio.Event()
            self.sibling_cleaned = asyncio.Event()

        async def plan_folders(self, request):
            slug = request.topics[0].slug
            self.started.add(slug)
            if len(self.started) == 2:
                self.both_started.set()
            await self.both_started.wait()
            if slug == "concept/alpha":
                await asyncio.sleep(0)
                raise asyncio.CancelledError
            try:
                await self.release_sibling.wait()
                return await super().plan_folders(request)
            finally:
                self.sibling_cleaned.set()

    model = CancellingTaxonomyModel()
    store = WorkerStore([pending_op(OP_A, "doc-a")])
    try:
        with pytest.raises(asyncio.CancelledError):
            await worker(
                store,
                FakeKnowledgeSource(dataset),
                model,
                options=WikiWorkerOptions(
                    taxonomy_topic_batch_size=1,
                    taxonomy_parallel=2,
                ),
            ).run_batch(SCOPE)
        assert model.sibling_cleaned.is_set()
        assert store.apply_calls == []
        assert store.release_calls == [(SCOPE, [OP_A], CLAIM_TOKEN)]
    finally:
        model.release_sibling.set()
        await asyncio.wait_for(model.sibling_cleaned.wait(), timeout=1)
```

同一步先把现有测试 helper 扩成可直接支撑这些测试：

```python
# fake_dataset() 中先保留候选全集；omitted_merges 只删除 Reduce 响应，
# 不删除 taxonomy 响应，否则无法测试“taxonomy 成功后 Reduce 失败”的固定点。
taxonomy_slugs = ({"entity/shared"} if include_shared else set()) | all_concepts
merge_slugs = taxonomy_slugs - omitted_merges
if not merge_slugs:
    merge_slugs = {"entity/unused"}
ordered_taxonomy_slugs = sorted(taxonomy_slugs)
taxonomies = {
    ",".join(batch): {
        "decisions": [
            {"slug": slug, "base_folder_id": None, "new_segments": []}
            for slug in batch
        ]
    }
    for size in range(1, len(ordered_taxonomy_slugs) + 1)
    for batch in combinations(ordered_taxonomy_slugs, size)
}
# 放入现有 model_responses dict："taxonomies": taxonomies


class WorkerStore:
    def __init__(
        self,
        records: list[PendingOpRecord],
        *,
        existing: dict[str, ExistingPageRecord] | None = None,
        contributions: list[StoredContributionRecord] | None = None,
        pending_override: int | None = None,
        conflict: bool = False,
        claim_lost: bool = False,
        apply_outcome: BatchApplyOutcome | None = None,
        folders: tuple[FolderCatalogEntry, ...] = (),
        classifiable_slugs: tuple[str, ...] | None = None,
        events: list[str] | None = None,
    ) -> None:
        self.records = list(records)
        self.existing = existing or {}
        self.contributions = list(contributions or [])
        self.pending_override = pending_override
        self.conflict = conflict
        self.claim_lost = claim_lost
        self.apply_outcome = apply_outcome
        self.claim_calls: list[tuple[WikiScope, int, int]] = []
        self.find_calls: list[tuple[str, ...]] = []
        self.contribution_calls: list[tuple[str, str]] = []
        self.apply_calls: list[BatchApplyRequest] = []
        self.bool_apply_calls = 0
        self.release_calls: list[tuple[WikiScope, list[UUID], UUID]] = []
        self.page_writes: list[list[ReducedPage]] = []
        self.folders = folders
        self.classifiable_slugs = classifiable_slugs
        self.events = events

    async def load_taxonomy_context(self, scope, slugs) -> TaxonomyContext:
        assert scope == SCOPE
        requested = tuple(
            slug
            for slug in slugs
            if slug.startswith(("entity/", "concept/"))
            and slug not in self.existing
        )
        return TaxonomyContext(
            folders=self.folders,
            classifiable_slugs=(
                requested
                if self.classifiable_slugs is None
                else self.classifiable_slugs
            ),
        )


def worker(
    store: WorkerStore,
    source: FakeKnowledgeSource,
    model: FakeChatModel,
    *,
    lease: FakeLease | None = None,
    options: WikiWorkerOptions | None = None,
    waits: list[int] | None = None,
    tombstones: FakeTombstones | None = None,
    embedding: EmbeddingModelPort | None = None,
) -> WikiIngestWorker:
    async def retry_wait(seconds: int) -> None:
        assert waits is not None
        waits.append(seconds)

    return WikiIngestWorker(
        store=store,
        locks=FakeLocks(FakeLease() if lease is None else lease),
        source=source,
        model=model,
        embedding_model=embedding or NeverEmbedding(),
        tombstones=tombstones or FakeTombstones(),
        options=options,
        retry_wait=retry_wait if waits is not None else None,
    )
```

在 import 中补入 `combinations`、`EmbeddingModelPort`、`EmbeddingOutput`、`EmbeddingRequest`、`FolderCatalogEntry` 和 `TaxonomyContext`。把现有 `test_reduce_failure_removes_contributor_and_rereduces_mixed_slug` 合并为上面的重算测试，避免维护两个相同场景。

- [ ] **步骤 2：运行 Worker 测试确认 RED**

```powershell
# Worker 尚未接收 embedding 或 taxonomy context，预期签名/断言失败
uv run pytest tests/wiki/test_ingest_worker.py -q
```

- [ ] **步骤 3：扩展构造函数和 taxonomy 分组执行**

```python
def __init__(
    self,
    *,
    store: IngestStore,
    locks: WikiLockManager,
    source: KnowledgeSourcePort,
    model: WikiIngestModelPort,
    embedding_model: EmbeddingModelPort,
    tombstones: TombstonePort | None = None,
    options: WikiWorkerOptions | None = None,
    retry_wait: _RetryWait | None = None,
) -> None:
    self._embedding_model = embedding_model
```

新增 `_plan_folder_assignments()`：

```python
async def _plan_folder_assignments(
    self,
    deltas: Sequence[ContributionDelta],
    context: TaxonomyContext,
    failures: dict[UUID, OperationFailure],
    superseded: set[UUID],
) -> tuple[FolderAssignment, ...]:
    eligible = [
        delta
        for delta in deltas
        if delta.pending_op_id not in failures and delta.pending_op_id not in superseded
    ]
    work_items = build_taxonomy_work_items(
        eligible,
        classifiable_slugs=context.classifiable_slugs,
    )
    if not work_items:
        return ()
    try:
        allowed = await self._retry_model(
            lambda: select_allowed_bases(
                tuple(item.topic for item in work_items),
                context.folders,
                self._embedding_model,
                full_catalog_limit=self._options.taxonomy_full_catalog_limit,
                related_limit=self._options.taxonomy_related_folder_limit,
            )
        )
    except Exception as error:
        if self._is_control_error(error):
            raise
        for op_id in sorted(
            {op_id for item in work_items for op_id in item.contributor_op_ids},
            key=str,
        ):
            failures.setdefault(op_id, operation_failure(op_id, error))
        return ()
    requests = build_taxonomy_requests(
        work_items,
        allowed,
        batch_size=self._options.taxonomy_topic_batch_size,
    )
    semaphore = asyncio.Semaphore(self._options.taxonomy_parallel)

    async def run(request: TaxonomyRequest):
        async with semaphore:
            try:
                output = await self._retry_model(lambda: self._model.plan_folders(request))
                return recover_taxonomy_output(request, output), None
            except Exception as error:
                if self._is_control_error(error):
                    raise
                return None, error

    outcomes = await _gather_with_cleanup(*(run(request) for request in requests))
    work_by_slug = {item.topic.slug: item for item in work_items}
    allowed_by_id = {item.id: item for item in allowed}
    assignments: list[FolderAssignment] = []
    for request, (decisions, error) in zip(requests, outcomes, strict=True):
        if error is not None:
            impacted = {
                op_id
                for topic in request.topics
                for op_id in work_by_slug[topic.slug].contributor_op_ids
            }
            for op_id in sorted(impacted, key=str):
                failures.setdefault(op_id, operation_failure(op_id, error))
            continue
        assert decisions is not None
        assignments.extend(
            build_folder_assignment(work_by_slug[topic.slug], decisions[topic.slug], allowed_by_id)
            for topic in request.topics
        )
    return tuple(assignments)
```

- [ ] **步骤 4：把 taxonomy、Reduce 和预提交检查改为共同固定点**

在初始页面和贡献查询后加载一次 context：

```python
taxonomy_context = await self._store.load_taxonomy_context(scope, slugs)
```

替换当前外层循环：

```python
while True:
    excluded_before = {*failures, *superseded}
    folder_assignments = await self._plan_folder_assignments(
        initial_deltas,
        taxonomy_context,
        failures,
        superseded,
    )
    pages = await self._stabilize_pages(
        initial_deltas,
        existing_pages,
        active_by_slug,
        failures,
        superseded,
    )
    await self._precommit_ingest_checks(scope, records, failures, superseded)
    if {*failures, *superseded} == excluded_before:
        break
```

构造请求时加入：

```python
folder_assignments=folder_assignments,
```

如果 Reduce 在同一轮增加 failure，下一轮必须重新调用 taxonomy，并使用当前 contributor 集；不得缓存旧 assignment。

更新 `WorkerStore.load_taxonomy_context()`，默认把请求中的所有 entity/concept slug 标为 classifiable，并允许测试注入目录目录表。更新 `worker()` helper 注入 `FakeEmbeddingModel` 或测试 embedding。

- [ ] **步骤 5：运行 Worker、Map/Reduce 和 Store 组合回归**

```powershell
# 验证固定点、失败隔离、重试和旧编排不回归
uv run pytest tests/wiki/test_ingest_worker.py tests/wiki/test_ingest_map.py tests/wiki/test_ingest_reduce.py tests/wiki/test_ingest_store.py -q
```

- [ ] **步骤 6：提交 Worker 固定点**

```powershell
# 暂存 Worker 和测试
git add app/wiki/ingest/worker.py tests/wiki/test_ingest_worker.py

# 创建独立编排提交
git commit -m "feat: orchestrate wiki taxonomy batches"
```

---

### 任务 10：Runtime、fixture 和环境配置

**文件：**

- 修改：`app/wiki/tasks/wiki_tasks.py:20-220`
- 修改：`examples/wiki_fake_data.json`
- 修改：`.env.example`
- 修改：`docker-compose.yml`
- 修改：`tests/wiki/test_wiki_tasks.py`
- 修改：`tests/wiki/test_enqueue_fake_cli.py`

- [ ] **步骤 1：先写 runtime 注入和配置默认值失败测试**

```python
# 插入现有
# test_build_runtime_creates_independent_batch_resources_and_disposes_own_engine
# 中 first/second 资源身份断言之后。
assert first.embedding_model is first.worker._embedding_model
assert second.embedding_model is second.worker._embedding_model
assert first.embedding_model is not first.worker._model
assert second.embedding_model is not second.worker._model
assert first.embedding_model is not second.embedding_model


def test_phase_four_a_worker_option_defaults(monkeypatch):
    for key in (
        "GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE",
        "GRAPH_WIKI_TAXONOMY_PARALLEL",
        "GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT",
        "GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT",
    ):
        monkeypatch.delenv(key, raising=False)
    options = WikiWorkerOptions.from_env()
    assert (
        options.taxonomy_topic_batch_size,
        options.taxonomy_parallel,
        options.taxonomy_full_catalog_limit,
        options.taxonomy_related_folder_limit,
    ) == (60, 4, 120, 40)
```

第一项是对 `tests/wiki/test_wiki_tasks.py` 现有同名 runtime 测试的精确断言增量，不新增重复测试。

- [ ] **步骤 2：运行 runtime 测试确认 RED**

```powershell
# runtime 尚无 embedding_model，预期属性/构造失败
uv run pytest tests/wiki/test_wiki_tasks.py tests/wiki/test_enqueue_fake_cli.py -q
```

- [ ] **步骤 3：注入 fake runtime 并更新 fixture**

`WikiTaskRuntime` 增加：

```python
embedding_model: EmbeddingModelPort
```

`build_runtime()` 改为：

```python
source, model, embedding_model = load_fake_runtime_adapters(Path(fixture_value))
worker = WikiIngestWorker(
    store=store,
    locks=locks,
    source=source,
    model=model,
    embedding_model=embedding_model,
    tombstones=tombstones,
    options=worker_options,
)
return WikiTaskRuntime(
    engine=engine,
    worker=worker,
    enqueue=enqueue,
    tombstones=tombstones,
    embedding_model=embedding_model,
)
```

`examples/wiki_fake_data.json` 增加确定性响应；实际 topic 顺序使用 `concept/retrieval,entity/acme`：

```json
"embeddings": {},
"taxonomies": {
  "concept/retrieval": {
    "decisions": [
      {
        "slug": "concept/retrieval",
        "base_folder_id": null,
        "new_segments": []
      }
    ]
  },
  "entity/acme": {
    "decisions": [
      {
        "slug": "entity/acme",
        "base_folder_id": null,
        "new_segments": ["Organizations", "Products"]
      }
    ]
  },
  "concept/retrieval,entity/acme": {
    "decisions": [
      {
        "slug": "concept/retrieval",
        "base_folder_id": null,
        "new_segments": []
      },
      {
        "slug": "entity/acme",
        "base_folder_id": null,
        "new_segments": ["Organizations", "Products"]
      }
    ]
  }
}
```

默认示例目录为空，不触发 embedding，`entity/acme` 会创建 `Organizations/Products`，`concept/retrieval` 显式留在根目录；任务 8 的预置 `Organizations` 集成测试使用同一 decision 验证“复用父目录并创建子目录”。大目录 embedding 响应由 taxonomy 定向测试覆盖。

- [ ] **步骤 4：更新环境和 Compose 默认值**

`.env.example` 增加：

```dotenv
GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE=60
GRAPH_WIKI_TAXONOMY_PARALLEL=4
GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT=120
GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT=40
```

`docker-compose.yml` 当前只有 `wiki-worker` 构造 Wiki runtime，因此只在该服务现有 `GRAPH_WIKI_*` 环境列表中加入：

```yaml
- GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE=${GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE:-60}
- GRAPH_WIKI_TAXONOMY_PARALLEL=${GRAPH_WIKI_TAXONOMY_PARALLEL:-4}
- GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT=${GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT:-120}
- GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT=${GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT:-40}
```

`postgres`、`redis` 和 `outbox-dispatcher` 不构造该 runtime，不增加这些变量；不得新增真实模型 URL 或密钥配置。

- [ ] **步骤 5：运行 runtime、CLI 和 fixture 回归**

```powershell
# 验证 fork-safe runtime、CLI 和严格 fixture
uv run pytest tests/wiki/test_wiki_tasks.py tests/wiki/test_enqueue_fake_cli.py tests/wiki/test_ingest_fakes.py -q
```

- [ ] **步骤 6：提交 runtime 和配置**

```powershell
# 暂存 runtime、fixture、配置及测试
git add app/wiki/tasks/wiki_tasks.py examples/wiki_fake_data.json .env.example docker-compose.yml tests/wiki/test_wiki_tasks.py tests/wiki/test_enqueue_fake_cli.py

# 创建独立 runtime 提交
git commit -m "feat: configure wiki taxonomy runtime"
```

---

### 任务 11：真实 PostgreSQL 事务和端到端验收

**文件：**

- 修改：`tests/wiki/test_postgres_integration.py`
- 修改：`tests/wiki/test_ingest_worker.py`

- [ ] **步骤 1：先写真实 Worker 首次分类、重放和人工目录保护测试**

```python
class RecordingSqlAlchemyIngestStore(SqlAlchemyIngestStore):
    def __init__(self, postgres_factory) -> None:
        super().__init__(postgres_factory, SqlFinalizationPort())
        self.last_request: BatchApplyRequest | None = None

    async def apply_results_with_outcome(self, scope, request):
        self.last_request = BatchApplyRequest.model_validate(
            request.model_dump(mode="python")
        )
        return await super().apply_results_with_outcome(scope, request)


async def _real_taxonomy_worker(postgres_factory):
    source, model, embedding = load_fake_runtime_adapters(
        Path("examples/wiki_fake_data.json")
    )
    scope = WikiScope(
        tenant_id=1,
        knowledge_base_id=UUID("11111111-1111-1111-1111-111111111111"),
        actor_id="taxonomy-worker",
        can_write=True,
    )
    store = RecordingSqlAlchemyIngestStore(postgres_factory)
    enqueued = await WikiEnqueueService(source, store).enqueue_ingest(
        scope, "knowledge-1"
    )
    assert enqueued.pending_op_id is not None
    worker = WikiIngestWorker(
        store=store,
        locks=MemoryWikiLockManager(),
        source=source,
        model=model,
        embedding_model=embedding,
        options=WikiWorkerOptions(),
        retry_wait=lambda _seconds: asyncio.sleep(0),
    )
    return scope, store, worker, model, embedding, enqueued.pending_op_id


@pytest.mark.asyncio
async def test_real_worker_classifies_only_new_topics_and_replay_is_idempotent(
    postgres_factory,
):
    scope, store, worker, model, embedding, op_id = await _real_taxonomy_worker(
        postgres_factory
    )
    first = await worker.run_batch(scope)
    assert first.completed_op_ids == (op_id,)
    assert len(model.taxonomy_requests) == 1
    assert embedding.calls == []
    assert store.last_request is not None
    async with postgres_factory() as session:
        pages = {
            page.slug: page
            for page in (
                await session.execute(
                    select(WikiPage).where(
                        WikiPage.tenant_id == scope.tenant_id,
                        WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    )
                )
            ).scalars()
        }
        assert pages["entity/acme"].category_path == ["Organizations", "Products"]
        assert pages["entity/acme"].wiki_path == "/Organizations/Products/entity/acme"
        assert pages["concept/retrieval"].folder_id is None
        assert pages["concept/retrieval"].wiki_path == "/concept/retrieval"
        first_version = pages["entity/acme"].version
        first_folder_count = await session.scalar(select(func.count(WikiFolder.id)))
    replay = await store.apply_results_with_outcome(scope, store.last_request)
    assert replay.applied is False
    async with postgres_factory() as session:
        page = (
            await session.execute(
                select(WikiPage).where(
                    WikiPage.knowledge_base_id == scope.knowledge_base_id,
                    WikiPage.slug == "entity/acme",
                )
            )
        ).scalar_one()
        assert page.version == first_version
        assert await session.scalar(select(func.count(WikiFolder.id))) == first_folder_count
```

在 import 中补入 `Path`、`UUID`、`WikiEnqueueService`、`load_fake_runtime_adapters` 和 `WikiFolder`。人工目录页与历史恢复页已经分别由任务 6 的真实 context 测试和任务 8 的真实恢复测试覆盖，此处只验证 Worker 到事务提交的整条链路。

- [ ] **步骤 2：写并发同级目录复用和 base 快照移动冲突测试**

```python
@pytest.mark.asyncio
async def test_concurrent_taxonomy_requests_reuse_one_folder_chain(postgres_factory):
    scope = WikiScope(
        tenant_id=33, knowledge_base_id=uuid4(), actor_id="worker", can_write=True
    )
    _, store_a, request_a, _ = await _claimed_topic_request(
        postgres_factory, scope=scope, slug="entity/acme"
    )
    _, store_b, request_b, _ = await _claimed_topic_request(
        postgres_factory, scope=scope, slug="concept/retrieval"
    )
    outcomes = await asyncio.gather(
        store_a.apply_results_with_outcome(scope, request_a),
        store_b.apply_results_with_outcome(scope, request_b),
    )
    assert all(outcome.applied for outcome in outcomes)
    async with postgres_factory() as session:
        folders = list(
            (
                await session.execute(
                    select(WikiFolder)
                    .where(WikiFolder.knowledge_base_id == scope.knowledge_base_id)
                    .order_by(WikiFolder.depth)
                )
            ).scalars()
        )
        assert [(item.name, item.depth) for item in folders] == [
            ("Organizations", 1),
            ("Products", 2),
        ]


@pytest.mark.asyncio
async def test_moved_taxonomy_base_releases_claim_without_creating_child(
    postgres_factory,
):
    scope = WikiScope(
        tenant_id=34, knowledge_base_id=uuid4(), actor_id="worker", can_write=True
    )
    base = WikiFolder(
        tenant_id=scope.tenant_id,
        knowledge_base_id=scope.knowledge_base_id,
        parent_id=None,
        name="Organizations",
        path="/Organizations",
        depth=1,
    )
    async with postgres_factory() as session, session.begin():
        session.add(base)
        await session.flush()
    _, store, request, pending = await _claimed_topic_request(
        postgres_factory,
        scope=scope,
        segments=("Products",),
        base=base,
    )
    async with postgres_factory() as session, session.begin():
        await session.execute(
            update(WikiFolder)
            .where(WikiFolder.id == base.id)
            .values(name="Moved", path="/Moved")
        )
    with pytest.raises(PageConflict, match="移动或失效"):
        await store.apply_results_with_outcome(scope, request)
    async with postgres_factory() as session:
        assert await session.scalar(
            select(func.count(WikiFolder.id)).where(
                WikiFolder.knowledge_base_id == scope.knowledge_base_id,
                WikiFolder.name == "Products",
            )
        ) == 0
        row = await session.get(WikiPendingOp, pending.id)
        assert row is not None and row.fail_count == 0
```

- [ ] **步骤 3：运行真实服务集成验收**

```powershell
# 启动只供测试使用的 PostgreSQL
docker compose up -d postgres

# 配置真实 PostgreSQL；Redis 使用本机测试 DB 15
$env:GRAPH_TEST_POSTGRES_URL = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph'
$env:GRAPH_TEST_REDIS_URL = 'redis://127.0.0.1:6379/15'

# 运行 4A 真实事务和 Worker 竞态
uv run pytest tests/wiki/test_postgres_integration.py tests/wiki/test_ingest_worker.py -q
```

预期以上用例全部通过；若定向用例失败，返回对应实现任务修正后先重跑该用例，再重跑以上两个文件，不在本任务扩展 4A 范围。

- [ ] **步骤 4：运行阶段一至四 A 组合回归**

```powershell
# 验证目录服务、页面服务、查询和摄取组合合同
uv run pytest tests/wiki/test_folder_service.py tests/wiki/test_page_service.py tests/wiki/test_query_service.py tests/wiki/test_ingest_store.py tests/wiki/test_ingest_worker.py tests/wiki/test_postgres_integration.py -q
```

- [ ] **步骤 5：提交真实集成验收**

```powershell
# 暂存真实服务和 Worker 竞态测试
git add tests/wiki/test_postgres_integration.py tests/wiki/test_ingest_worker.py

# 创建独立验收提交
git commit -m "test: cover wiki taxonomy integration"
```

---

### 任务 12：中文文档、合同测试和最终验证

**文件：**

- 新建：`docs/Wiki阶段四A.md`
- 新建：`tests/wiki/test_phase_four_a_docs.py`
- 修改：`README.md`

- [ ] **步骤 1：先写中文文档、配置和无迁移合同失败测试**

```python
from pathlib import Path


def test_phase_four_a_environment_defaults_are_documented():
    env = Path(".env.example").read_text(encoding="utf-8")
    for line in (
        "GRAPH_WIKI_TAXONOMY_TOPIC_BATCH_SIZE=60",
        "GRAPH_WIKI_TAXONOMY_PARALLEL=4",
        "GRAPH_WIKI_TAXONOMY_FULL_CATALOG_LIMIT=120",
        "GRAPH_WIKI_TAXONOMY_RELATED_FOLDER_LIMIT=40",
    ):
        assert line in env


def test_phase_four_a_doc_describes_only_implemented_scope():
    text = Path("docs/Wiki阶段四A.md").read_text(encoding="utf-8")
    for phrase in (
        "批次 taxonomy",
        "fake embedding",
        "真正新页面",
        "人工目录",
        "失败隔离",
        "同一事务",
    ):
        assert phrase in text
    for forbidden in (
        "自动交叉链接已实现",
        "auto-fix 已实现",
        "Agent 已实现",
        "WikiPageIndexer 已实现",
        "真实 embedding 已实现",
    ):
        assert forbidden not in text


def test_phase_four_a_does_not_add_migration():
    versions = sorted(Path("migrations/versions").glob("*.py"))
    assert versions[-1].name == "20260719_04_add_wiki_log_result_outcome.py"
```

- [ ] **步骤 2：运行文档测试确认 RED**

```powershell
# 阶段四 A 文档尚不存在，预期 FileNotFoundError
uv run pytest tests/wiki/test_phase_four_a_docs.py -q
```

- [ ] **步骤 3：编写与实现一致的中文文档和 README 入口**

`docs/Wiki阶段四A.md` 必须包含：

- fake fixture 的 embedding/taxonomy 结构。
- 小目录全量候选和大目录 top-K/祖先补齐规则。
- 60 topic 切批、4 并发和三次重试。
- taxonomy 失败与 pending-op/dead-letter 的关系。
- 真正新页面、历史恢复页和人工根目录保护。
- 目录与页面同事务、`folder_id` 与路径缓存语义。
- 当前限制：fake 上游，无自动链接、完整 Lint auto-fix、Agent、Indexer 和真实模型。

所有命令前加中文用途注释：

```powershell
# 运行阶段四 A 的目录规划定向测试
uv run pytest tests/wiki/test_ingest_taxonomy.py -q

# 使用 fake fixture 入队一次首次摄取
uv run python -m app.wiki.tasks.enqueue_fake --op ingest --kb-id 11111111-1111-1111-1111-111111111111 --knowledge-id knowledge-1
```

README 只增加“Wiki 阶段四 A”文档链接和一句当前能力摘要，不复制运行手册。

- [ ] **步骤 4：执行最终静态、迁移、Compose 和无 URL 全量验收**

```powershell
# 验证所有 Python 文件可编译
uv run python -m compileall app tests migrations

# 验证 Alembic 仍只有阶段三唯一 head
uv run alembic heads

# 验证完整离线升级 SQL 仍可生成
uv run alembic upgrade head --sql

# 验证 Compose 配置可解析
docker compose config --quiet

# 验证 changed-file Ruff
uv run ruff check app/wiki/ingest/taxonomy.py app/wiki/ingest/schemas.py app/wiki/ingest/ports.py app/wiki/ingest/fakes.py app/wiki/ingest/store.py app/wiki/ingest/worker.py app/wiki/tasks/wiki_tasks.py tests/wiki/test_ingest_taxonomy.py tests/wiki/test_ingest_schemas.py tests/wiki/test_ingest_fakes.py tests/wiki/test_ingest_store.py tests/wiki/test_ingest_worker.py tests/wiki/test_wiki_tasks.py tests/wiki/test_postgres_integration.py tests/wiki/test_phase_four_a_docs.py

# 运行不连接默认服务的完整测试；真实服务用例必须只按环境变量明确跳过
uv run pytest -p no:cacheprovider --basetemp "$env:TEMP\graph-wiki-phase4a-final-no-services" -q

# 检查补丁空白、冲突标记和工作树状态
git diff --check
git status --short
```

- [ ] **步骤 5：执行真实 PostgreSQL/Redis 全量验收**

```powershell
# 启动测试 PostgreSQL
docker compose up -d postgres

# 指定 opt-in 真实服务 URL
$env:GRAPH_TEST_POSTGRES_URL = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph'
$env:GRAPH_TEST_REDIS_URL = 'redis://127.0.0.1:6379/15'

# 运行真实服务完整测试集
uv run pytest -p no:cacheprovider --basetemp "$env:TEMP\graph-wiki-phase4a-final-real-services" -q

# 停止本计划启动的 PostgreSQL，不停止本机既有 Redis
docker compose stop postgres
```

- [ ] **步骤 6：提交文档和最终合同**

```powershell
# 暂存中文文档、README 和文档合同测试
git add docs/Wiki阶段四A.md README.md tests/wiki/test_phase_four_a_docs.py

# 创建独立文档提交
git commit -m "docs: document wiki phase four taxonomy"
```

---

## 最终验收清单

- [ ] 只有真正新建的 entity/concept 进入 taxonomy；summary、已有页和历史恢复页不分类。
- [ ] 小目录使用全部活动目录，大目录保留全部一级目录并通过 fake embedding 选择相关深层目录及祖先。
- [ ] embedding 输出完整、有限、同维；零向量稳定得到 0 相似度。
- [ ] taxonomy 每批最多 60 topic、最多 4 并发、瞬时失败最多重试 3 次。
- [ ] taxonomy 输出完整覆盖请求，base 只能来自白名单，最多新增两层且总深度不超过 3。
- [ ] 单个 taxonomy 分组失败只失败相关 pending-op；共享 slug 和 Reduce 后失败会重新稳定 contributor assignment。
- [ ] base 目录被人工移动或删除后，Store 检测快照冲突并重试，不静默改用新路径。
- [ ] 新目录、页面目录字段、贡献、链接、日志、pending-op 和 Outbox 在同一事务变化。
- [ ] `folder_id/category_path/wiki_path/depth` 一致，目录位置变化不额外增加页面版本。
- [ ] 同级目录并发创建得到唯一行；冲突或 CAS/claim 失败不残留空目录。
- [ ] 重复 operation 不重复目录、页面版本、日志或 finalization。
- [ ] 现有 REST 请求和响应字段、阶段一至三测试保持不变。
- [ ] Alembic head 仍为 `20260719_04`，4A 不增加迁移。
- [ ] 中文文档、compileall、Ruff、Compose、无 URL 全量和真实 PostgreSQL/Redis 全量全部通过。
