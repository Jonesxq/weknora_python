# Wiki 阶段四 B 实施计划

> **面向执行代理：** 必须使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans` 逐任务执行；所有步骤使用复选框跟踪，生产代码必须先有失败测试。

**目标：** 在当前 `app/wiki` 增量摄取结构中实现确定性自动交叉链接、strict fake 驱动的 Index 简介，以及 Graph 与批次日志增强，并保持 REST DTO、数据库结构和阶段一至四 A 行为不变。

**架构：** Worker 在现有 taxonomy、Reduce、precommit 固定点稳定后规划一次 best-effort Index 更新；Store 在现有结果事务中锁定真实成功页面，按语义正文计算版本，随后自动装饰正文、重建 `WikiLink`、CAS 写入 canonical Index 并记录一条增强批次日志。Markdown 受保护区间扫描和链接抽取集中在 `app/wiki/linkify.py`，查询服务只读取活动、published 且已解析的结构化边。

**技术栈：** Python 3.12、Pydantic v2、SQLAlchemy 2 Async、PostgreSQL 16、Celery、Redis、Tenacity、pytest、pytest-asyncio、Ruff。

---

## 设计依据

- 总设计：`docs/2026-07-14-python-wiki-reimplementation-design.md`
- 4B 已确认设计：`docs/superpowers/specs/2026-07-20-wiki-phase-4b-design.md`
- 当前运行基线：`docs/Wiki阶段四A.md`

本计划只实现阶段四 B。broken-link 文本清理、全库 `missing_cross_ref` 扫描、完整 Lint/auto-fix、问题单自动生成属于阶段四 C；Agent 和 `WikiPageIndexer` 属于阶段五。

## 文件职责

新增文件：

- `app/wiki/linkify.py`：Markdown 受保护区间扫描、安全 Wiki 链接提取和确定性正文装饰。
- `app/wiki/ingest/index_intro.py`：Index create/update 请求构造、输出清理、fallback 与稳定排序。
- `tests/wiki/test_linkify.py`：受保护 Markdown、边界、歧义、长名称优先和幂等单元测试。
- `tests/wiki/test_ingest_index_intro.py`：Index 请求规划、样本上限、fallback 和输出清理测试。
- `docs/Wiki阶段四B.md`：与最终实现一致的中文运行说明。
- `tests/wiki/test_phase_four_b_docs.py`：中文文档、fake fixture、无迁移和阶段边界合同。

修改文件：

- `app/wiki/domain.py`：`extract_wiki_links()` 复用统一 scanner。
- `app/wiki/ingest/schemas.py`：新增不可变 Index DTO，并为 `BatchApplyRequest` 增加可选计划。
- `app/wiki/ingest/ports.py`：模型端口增加 Index intro 调用。
- `app/wiki/ingest/fakes.py`：strict fake 响应、稳定 key、请求快照和瞬时失败。
- `app/wiki/ingest/store.py`：Index context、自动链接候选、Index CAS、增强日志和原子写入。
- `app/wiki/ingest/worker.py`：固定点稳定后的 Index 规划与 best-effort fallback。
- `app/wiki/query_service.py`：canonical Index 读取和活动已解析边的 Graph degree/BFS。
- `examples/wiki_fake_data.json`：首次和增量 Index 的确定性 fake 响应。
- `README.md`：阶段四 B 文档入口。
- `tests/wiki/test_domain.py`、`tests/wiki/test_ingest_schemas.py`、`tests/wiki/test_ingest_fakes.py`、`tests/wiki/test_ingest_store.py`、`tests/wiki/test_ingest_worker.py`、`tests/wiki/test_query_service.py`、`tests/wiki/test_postgres_integration.py`、`tests/wiki/test_enqueue_fake_cli.py`：对应合同和回归。

不修改 Alembic migration、REST schema 和 Vue 前端。

## 执行环境

```powershell
# 从 main 创建隔离工作树，避免覆盖当前用户改动
git worktree add E:\code\graph\.worktrees\wiki-phase-4b -b codex/wiki-phase-4b main

# 进入阶段四 B 工作树
Set-Location E:\code\graph\.worktrees\wiki-phase-4b

# 把 uv 缓存限制在工作树内
$env:UV_CACHE_DIR = "$PWD\.uv-cache"

# 同步锁文件中的 Python 3.12 环境
uv sync --python 3.12

# 验证阶段四 A 起点；真实服务用例未显式配置时应跳过
$env:PYTHONDONTWRITEBYTECODE = '1'
uv run pytest -p no:cacheprovider --basetemp "$env:TEMP\graph-wiki-phase4b-baseline" -q
```

---

### 任务 1：Markdown 受保护区间与确定性 linkify

**文件：**

- 新建：`app/wiki/linkify.py`
- 新建：`tests/wiki/test_linkify.py`

- [ ] **步骤 1：先写安全区域、边界、歧义和幂等失败测试**

在 `tests/wiki/test_linkify.py` 新增：

```python
import pytest

from app.wiki.linkify import LinkCandidate, linkify_markdown


def candidates(*values: tuple[str, str]) -> tuple[LinkCandidate, ...]:
    return tuple(LinkCandidate(slug=slug, display=display) for slug, display in values)


def test_linkify_prefers_long_name_and_wraps_each_slug_once():
    result = linkify_markdown(
        "机器学习依赖学习。机器学习再次出现。",
        current_slug="concept/current",
        candidates=candidates(
            ("concept/learning", "学习"),
            ("concept/machine-learning", "机器学习"),
        ),
    )
    assert result.content == (
        "[[concept/machine-learning|机器学习]]依赖"
        "[[concept/learning|学习]]。机器学习再次出现。"
    )
    assert result.changed is True
    assert result.added_slugs == (
        "concept/machine-learning",
        "concept/learning",
    )


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("AI and TRAINING_AI", "[[concept/ai|AI]] and TRAINING_AI"),
        ("`AI` and AI", "`AI` and [[concept/ai|AI]]"),
        ("```python\nAI\n```\nAI", "```python\nAI\n```\n[[concept/ai|AI]]"),
        ("~~~\nAI\n~~~\nAI", "~~~\nAI\n~~~\n[[concept/ai|AI]]"),
        ("[AI](https://example.test) AI", "[AI](https://example.test) [[concept/ai|AI]]"),
        ("![AI](image.png) AI", "![AI](image.png) [[concept/ai|AI]]"),
        ("[AI][ref] AI\n[ref]: /target", "[AI][ref] [[concept/ai|AI]]\n[ref]: /target"),
        ("<https://example.test/AI> AI", "<https://example.test/AI> [[concept/ai|AI]]"),
        ("[[concept/ai|AI]] AI", "[[concept/ai|AI]] AI"),
        (r"\AI AI", r"\AI [[concept/ai|AI]]"),
    ],
)
def test_linkify_preserves_markdown_protected_regions(content: str, expected: str):
    result = linkify_markdown(
        content,
        current_slug="concept/current",
        candidates=candidates(("concept/ai", "AI")),
    )
    assert result.content == expected


def test_linkify_excludes_ambiguous_self_empty_and_duplicate_candidates():
    result = linkify_markdown(
        "Acme Python Current",
        current_slug="entity/current",
        candidates=candidates(
            ("entity/acme-a", "Acme"),
            ("entity/acme-b", "Acme"),
            ("concept/python", "Python"),
            ("concept/python", "Python"),
            ("entity/current", "Current"),
            ("concept/blank", "   "),
        ),
    )
    assert result.content == "Acme [[concept/python|Python]] Current"
    assert result.added_slugs == ("concept/python",)


def test_linkify_is_idempotent_with_crlf_and_unclosed_markup():
    first = linkify_markdown(
        "前言\r\n`未闭合 AI\r\n正文 AI",
        current_slug="concept/current",
        candidates=candidates(("concept/ai", "AI")),
    )
    second = linkify_markdown(
        first.content,
        current_slug="concept/current",
        candidates=candidates(("concept/ai", "AI")),
    )
    assert second.content == first.content
    assert second.changed is False
    assert second.added_slugs == ()
```

- [ ] **步骤 2：运行测试确认 RED**

```powershell
# linkify 模块尚不存在，预期 ModuleNotFoundError
uv run pytest tests/wiki/test_linkify.py -q
```

- [ ] **步骤 3：实现不可变候选、结果和受保护区间扫描**

`app/wiki/linkify.py` 的公开合同固定为：

```python
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from app.wiki.domain import normalize_slug

_WIKI_LINK = re.compile(r"\[\[([^\]|]+)(?:\|([^\]]+))?\]\]")
_REFERENCE_DEFINITION = re.compile(r"(?m)^[ \t]{0,3}\[[^\]\r\n]+\]:[^\r\n]*(?:\r?\n|$)")
_AUTOLINK = re.compile(r"<[A-Za-z][A-Za-z0-9+.-]*://[^<>\s]+>")


@dataclass(frozen=True, slots=True)
class LinkCandidate:
    slug: str
    display: str


@dataclass(frozen=True, slots=True)
class LinkifyResult:
    content: str
    changed: bool
    added_slugs: tuple[str, ...]


def protected_spans(content: str) -> tuple[tuple[int, int], ...]:
    spans: list[tuple[int, int]] = []
    spans.extend(_fenced_code_spans(content))
    spans.extend(_inline_code_spans(content, spans))
    spans.extend(match.span() for match in _WIKI_LINK.finditer(content))
    spans.extend(_markdown_link_spans(content))
    spans.extend(match.span() for match in _REFERENCE_DEFINITION.finditer(content))
    spans.extend(match.span() for match in _AUTOLINK.finditer(content))
    return _merge_spans(spans)


def linkify_markdown(
    content: str,
    *,
    current_slug: str,
    candidates: Iterable[LinkCandidate],
) -> LinkifyResult:
    current = normalize_slug(current_slug)
    usable = _prepare_candidates(current, candidates)
    occupied = list(protected_spans(content))
    used = set(extract_safe_wiki_links(content))
    added: list[str] = []
    value = content
    for candidate in usable:
        if candidate.slug in used:
            continue
        match = _first_safe_match(value, candidate.display, occupied)
        if match is None:
            continue
        start, end = match
        replacement = f"[[{candidate.slug}|{value[start:end]}]]"
        value = value[:start] + replacement + value[end:]
        delta = len(replacement) - (end - start)
        occupied = _shift_and_add(occupied, start, end, delta)
        used.add(candidate.slug)
        added.append(candidate.slug)
    return LinkifyResult(value, value != content, tuple(added))
```

内部 helper 必须按以下确定性规则实现：fence 只在行首最多三个空格后识别，闭合 fence 的字符与 run 长度必须兼容；inline code 使用同长度反引号 run 闭合；Markdown link/image 使用成对中括号和成对圆括号扫描，reference link 保护两个 bracket group；所有 span 合并为不重叠半开区间。`_prepare_candidates()` 先 trim、规范化 slug、按显示文本找出多 slug 歧义项并删除，再按 `(-len(display), display, slug)` 排序；`_first_safe_match()` 对纯 ASCII 显示名检查前后字符均不是 `[A-Za-z0-9_]`，含非 ASCII 时直接按字符匹配，并跳过前方连续反斜杠数量为奇数的转义匹配。

- [ ] **步骤 4：运行纯函数测试确认 GREEN**

```powershell
# 验证 Markdown 保护、匹配规则和重复运行幂等
uv run pytest tests/wiki/test_linkify.py -q
```

- [ ] **步骤 5：提交纯函数模块**

```powershell
# 暂存 linkify 实现和定向测试
git add app/wiki/linkify.py tests/wiki/test_linkify.py

# 创建独立纯函数提交
git commit -m "feat: add deterministic wiki linkifier"
```

---

### 任务 2：统一安全 Wiki 链接提取

**文件：**

- 修改：`app/wiki/linkify.py`
- 修改：`app/wiki/domain.py:32-45`
- 修改：`tests/wiki/test_linkify.py`
- 修改：`tests/wiki/test_domain.py`

- [ ] **步骤 1：先写代码区伪链接和稳定去重失败测试**

```python
from app.wiki.domain import extract_wiki_links


def test_extract_wiki_links_ignores_protected_regions_and_stably_deduplicates():
    content = (
        "`[[concept/code]]` [[concept/real|真实]]\n"
        "```md\n[[entity/fenced]]\n```\n"
        "[[concept/real]] [[entity/acme]]"
    )
    assert extract_wiki_links(content) == ["concept/real", "entity/acme"]


def test_extract_wiki_links_ignores_invalid_slugs_without_losing_later_links():
    assert extract_wiki_links(
        "[[not valid]] [[concept/python]] [[../escape]] [[entity/acme]]"
    ) == ["concept/python", "entity/acme"]
```

- [ ] **步骤 2：运行定向测试确认 RED**

```powershell
# 旧正则仍会读取代码块中的伪链接，预期断言失败
uv run pytest tests/wiki/test_domain.py tests/wiki/test_linkify.py -q
```

- [ ] **步骤 3：在 scanner 中实现安全抽取并切换领域入口**

在 `app/wiki/linkify.py` 增加：

```python
def extract_safe_wiki_links(content: str) -> tuple[str, ...]:
    blocked = tuple(
        span
        for span in protected_spans(content)
        if not _span_is_wiki_link(content, span)
    )
    result: list[str] = []
    seen: set[str] = set()
    for match in _WIKI_LINK.finditer(content):
        if _overlaps(match.start(), match.end(), blocked):
            continue
        try:
            slug = normalize_slug(match.group(1))
        except ValueError:
            continue
        if slug not in seen:
            seen.add(slug)
            result.append(slug)
    return tuple(result)
```

避免 `linkify.py -> domain.py -> linkify.py` 循环依赖：把 `normalize_slug()` 移到 `app/wiki/linkify.py` 不合适；因此 `domain.py` 保留 slug 实现，并在函数内部延迟导入：

```python
def extract_wiki_links(content: str) -> list[str]:
    from app.wiki.linkify import extract_safe_wiki_links

    return list(extract_safe_wiki_links(content))
```

`protected_spans()` 内部必须分别保留 Wiki span 和其他 protected span，使 `extract_safe_wiki_links()` 能解析安全位置的 Wiki markup，而 linkify 仍把全部 Wiki span 视为禁止改写区。

- [ ] **步骤 4：运行领域、页面服务和查询重建回归**

```powershell
# 验证统一抽取入口不会破坏页面写入和 rebuild_links
uv run pytest tests/wiki/test_domain.py tests/wiki/test_linkify.py tests/wiki/test_page_service.py tests/wiki/test_query_service.py -q
```

- [ ] **步骤 5：提交统一链接解析**

```powershell
# 暂存领域入口、scanner 和测试
git add app/wiki/domain.py app/wiki/linkify.py tests/wiki/test_domain.py tests/wiki/test_linkify.py

# 创建独立链接投影提交
git commit -m "fix: ignore protected wiki link markup"
```

---

### 任务 3：Index intro 严格不可变 DTO

**文件：**

- 修改：`app/wiki/ingest/schemas.py`
- 修改：`tests/wiki/test_ingest_schemas.py`

- [ ] **步骤 1：先写深层不可变、create/update 和计划交叉校验失败测试**

```python
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.wiki.ingest.schemas import (
    BatchApplyRequest,
    IndexIntroChange,
    IndexIntroContext,
    IndexIntroOutput,
    IndexIntroPlan,
    IndexIntroRequest,
    IndexPageSnapshot,
    IndexSummaryItem,
)


def test_index_intro_contracts_are_deeply_immutable():
    item = IndexSummaryItem(
        slug="summary/knowledge-1", title="文档一", summary="摘要一"
    )
    context = IndexIntroContext(recent_summaries=(item,))
    request = IndexIntroRequest(mode="create", summaries=context.recent_summaries)
    with pytest.raises(ValidationError):
        request.summaries[0].title = "污染"  # type: ignore[misc]
    assert request.summaries == (item,)


def test_index_intro_create_and_update_have_disjoint_payloads():
    item = IndexSummaryItem(
        slug="summary/knowledge-1", title="文档一", summary="摘要一"
    )
    change = IndexIntroChange(
        action="ingest", knowledge_id="knowledge-1", pages=(item,)
    )
    with pytest.raises(ValidationError, match="create"):
        IndexIntroRequest(mode="create", existing_intro="旧简介", summaries=(item,))
    with pytest.raises(ValidationError, match="update"):
        IndexIntroRequest(mode="update", changes=(change,))
    assert IndexIntroRequest(
        mode="update", existing_intro="旧简介", changes=(change,)
    ).summaries == ()


def test_index_plan_requires_matching_snapshot_identity():
    page_id = uuid4()
    snapshot = IndexPageSnapshot(
        id=page_id, version=3, content="旧简介", summary="旧简介"
    )
    plan = IndexIntroPlan(
        mode="update",
        expected_page_id=snapshot.id,
        expected_version=snapshot.version,
        intro="新简介",
        model_status="generated",
    )
    assert plan.expected_page_id == page_id
    with pytest.raises(ValidationError, match="同时为空"):
        IndexIntroPlan(
            mode="create",
            expected_page_id=page_id,
            expected_version=None,
            intro="简介",
            model_status="generated",
        )


def test_index_output_enforces_non_empty_bounded_intro():
    assert IndexIntroOutput(intro=" 简介 ").intro == "简介"
    with pytest.raises(ValidationError):
        IndexIntroOutput(intro="x" * 4001)


def test_batch_request_rejects_index_plan_without_completed_operation():
    with pytest.raises(ValidationError, match="Index intro"):
        BatchApplyRequest(
            claim_token=uuid4(),
            pages=(),
            contribution_deltas=(),
            completed_op_ids=(),
            superseded_op_ids=(),
            failures=(),
            expected_pages=(),
            operation_id=uuid4(),
            index_intro_plan=IndexIntroPlan(
                mode="create",
                intro="简介",
                model_status="generated",
            ),
        )
```

- [ ] **步骤 2：运行 Schema 测试确认 RED**

```powershell
# Index DTO 尚不存在，预期 ImportError
uv run pytest tests/wiki/test_ingest_schemas.py -q
```

- [ ] **步骤 3：实现严格 DTO 并扩展 BatchApplyRequest**

在 `app/wiki/ingest/schemas.py` 增加以下合同；全部继承 `_FrozenValueModel`，字符串 validator 必须 trim，集合字段必须检查唯一性：

```python
IndexIntroMode = Literal["create", "update"]
IndexModelStatus = Literal["generated", "defaulted", "kept_after_error"]


class IndexSummaryItem(_FrozenValueModel):
    slug: str
    title: str = Field(min_length=1, max_length=512)
    summary: str = Field(max_length=4000)

    @field_validator("slug")
    @classmethod
    def normalize_summary_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("summary",))

    @field_validator("title")
    @classmethod
    def clean_title(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Index summary 标题不能为空")
        return value

    @field_validator("summary")
    @classmethod
    def clean_summary(cls, value: str) -> str:
        value = value.strip()
        return value


class IndexPageSnapshot(_FrozenValueModel):
    id: UUID
    version: int = Field(ge=1)
    content: str = Field(max_length=4000)
    summary: str = Field(max_length=4000)


class IndexIntroContext(_FrozenValueModel):
    index: IndexPageSnapshot | None = None
    recent_summaries: tuple[IndexSummaryItem, ...] = Field(default=(), max_length=200)


class IndexIntroChange(_FrozenValueModel):
    action: Literal["ingest", "retract"]
    knowledge_id: str = Field(min_length=1, max_length=512)
    pages: tuple[IndexSummaryItem, ...] = ()


class IndexIntroRequest(_FrozenValueModel):
    mode: IndexIntroMode
    existing_intro: str = Field(default="", max_length=4000)
    summaries: tuple[IndexSummaryItem, ...] = Field(default=(), max_length=200)
    changes: tuple[IndexIntroChange, ...] = ()

    @model_validator(mode="after")
    def validate_mode_payload(self) -> Self:
        if self.mode == "create" and (self.existing_intro or self.changes):
            raise ValueError("create 请求只能携带 summary 样本")
        if self.mode == "update" and (
            not self.existing_intro or self.summaries or not self.changes
        ):
            raise ValueError("update 请求必须只携带旧简介和本批变化")
        return self


class IndexIntroOutput(_FrozenValueModel):
    intro: str = Field(min_length=1, max_length=4000)

    @field_validator("intro")
    @classmethod
    def clean_intro(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Index intro 不能为空")
        return value


class IndexIntroPlan(_FrozenValueModel):
    mode: IndexIntroMode
    expected_page_id: UUID | None = None
    expected_version: int | None = Field(default=None, ge=1)
    intro: str = Field(min_length=1, max_length=4000)
    model_status: IndexModelStatus
    error_code: str | None = Field(default=None, max_length=128)

    @model_validator(mode="after")
    def validate_snapshot_and_status(self) -> Self:
        if (self.expected_page_id is None) != (self.expected_version is None):
            raise ValueError("Index 期望 id/version 必须同时为空或同时存在")
        if self.mode == "create" and self.expected_page_id is not None:
            raise ValueError("create 计划不能携带既有 Index 身份")
        if self.mode == "update" and self.expected_page_id is None:
            raise ValueError("update 计划必须携带既有 Index 身份")
        if (self.model_status == "generated") == (self.error_code is not None):
            raise ValueError("成功计划不能有错误码，fallback 计划必须有错误码")
        return self
```

为 `BatchApplyRequest` 增加并让现有 frozen snapshot 自动覆盖它：

```python
index_intro_plan: IndexIntroPlan | None = None
```

在 `validate_batch_identities()` 增加：

```python
if self.index_intro_plan is not None and not self.completed_op_ids:
    raise ValueError("Index intro 计划必须至少对应一个 completed operation")
```

`IndexSummaryItem.slug` 只允许 `summary/`；title 必须非空，summary 可以为空但必须 trim。knowledge ID、summary/change 重复项使用稳定唯一校验。`IndexPageSnapshot` 分别保留 content 和 summary；Worker 以 content 为现有简介，Store 成功写入后再保证两个字段同步。

- [ ] **步骤 4：运行 Schema 和 Store 请求合同测试**

```powershell
# 验证 DTO 深层不可变和 BatchApplyRequest 兼容
uv run pytest tests/wiki/test_ingest_schemas.py tests/wiki/test_ingest_store.py -q
```

- [ ] **步骤 5：提交 Index DTO**

```powershell
# 暂存 Index 合同和测试
git add app/wiki/ingest/schemas.py tests/wiki/test_ingest_schemas.py

# 创建独立 Schema 提交
git commit -m "feat: define wiki index intro contracts"
```

---

### 任务 4：Index 模型端口与 strict fake

**文件：**

- 修改：`app/wiki/ingest/ports.py`
- 修改：`app/wiki/ingest/fakes.py`
- 修改：`tests/wiki/test_ingest_fakes.py`

- [ ] **步骤 1：先写稳定 key、快照、缺失响应和瞬时失败测试**

```python
from copy import deepcopy

import pytest

from app.wiki.ingest.ports import PermanentModelError, TransientModelError
from app.wiki.ingest.schemas import (
    IndexIntroChange,
    IndexIntroRequest,
    IndexSummaryItem,
)


@pytest.mark.asyncio
async def test_fake_index_intro_uses_stable_create_and_update_keys(tmp_path):
    data = deepcopy(FIXTURE)
    data["model_responses"]["index_intros"] = {
        "index_intro:create:summary/knowledge-1": {"intro": "首次简介"},
        "index_intro:update:ingest:knowledge-1,retract:knowledge-2": {
            "intro": "增量简介"
        },
    }
    fixture = tmp_path / "wiki.json"
    write_fixture(fixture, data)
    _, model = load_fake_adapters(fixture)
    item = IndexSummaryItem(
        slug="summary/knowledge-1", title="文档一", summary="摘要一"
    )
    assert (
        await model.generate_index_intro(
            IndexIntroRequest(mode="create", summaries=(item,))
        )
    ).intro == "首次简介"
    changes = (
        IndexIntroChange(action="ingest", knowledge_id="knowledge-1", pages=(item,)),
        IndexIntroChange(action="retract", knowledge_id="knowledge-2"),
    )
    assert (
        await model.generate_index_intro(
            IndexIntroRequest(
                mode="update", existing_intro="旧简介", changes=changes
            )
        )
    ).intro == "增量简介"
    assert model.index_intro_requests[-1].changes == changes


@pytest.mark.asyncio
async def test_fake_index_intro_missing_and_transient_responses_are_strict(tmp_path):
    data = deepcopy(FIXTURE)
    data["model_responses"]["index_intros"] = {}
    fixture = tmp_path / "wiki.json"
    write_fixture(fixture, data)
    _, model = load_fake_adapters(fixture)
    request = IndexIntroRequest(
        mode="create",
        summaries=(
            IndexSummaryItem(
                slug="summary/knowledge-1", title="文档一", summary="摘要一"
            ),
        ),
    )
    with pytest.raises(PermanentModelError, match="缺少模型响应"):
        await model.generate_index_intro(request)

    data["transient_failures"] = {
        "index_intro:create:summary/knowledge-1": 1
    }
    data["model_responses"]["index_intros"] = {
        "index_intro:create:summary/knowledge-1": {"intro": "首次简介"}
    }
    write_fixture(fixture, data)
    _, retrying_model = load_fake_adapters(fixture)
    with pytest.raises(TransientModelError):
        await retrying_model.generate_index_intro(request)
    assert (await retrying_model.generate_index_intro(request)).intro == "首次简介"
```

- [ ] **步骤 2：运行 fake 测试确认 RED**

```powershell
# fake 尚无 index_intros 字段和调用，预期校验或属性失败
uv run pytest tests/wiki/test_ingest_fakes.py -q
```

- [ ] **步骤 3：扩展端口、fixture 校验和 FakeChatModel**

`ChatModelPort` 增加：

```python
async def generate_index_intro(
    self, request: IndexIntroRequest
) -> IndexIntroOutput: ...
```

`_ModelResponses` 增加：

```python
index_intros: dict[str, IndexIntroOutput] = {}
```

`FakeDataset` 验证所有 key 必须精确匹配 `index_intro:(create|update):非空后缀`，并允许 `transient_failures` 引用已声明的 Index key。现有 `validate_failure_keys()` 的 prefix 集合增加 `index_intro`，`validate_identity_and_scope()` 增加精确引用校验：

```python
for key in self.model_responses.index_intros:
    if re.fullmatch(
        r"index_intro:create:summary/[^,]+(?:,summary/[^,]+)*"
        r"|index_intro:update:(?:ingest|retract):[^,]+"
        r"(?:,(?:ingest|retract):[^,]+)*",
        key,
    ) is None:
        raise ValueError("index_intros 响应键必须使用稳定的非空 batch key")

if prefix == "index_intro" and (
    f"index_intro:{suffix}" not in self.model_responses.index_intros
):
    raise ValueError("index_intro 瞬时失败键必须引用已声明响应")
```

`FakeChatModel` 增加：

```python
self.index_intro_requests: list[IndexIntroRequest] = []

async def generate_index_intro(
    self, request: IndexIntroRequest
) -> IndexIntroOutput:
    snapshot = IndexIntroRequest.model_validate(request.model_dump(mode="python"))
    self.index_intro_requests.append(snapshot)
    if snapshot.mode == "create":
        suffix = _batch_key(item.slug for item in snapshot.summaries)
    else:
        suffix = _batch_key(
            f"{item.action}:{item.knowledge_id}" for item in snapshot.changes
        )
    key = f"index_intro:{snapshot.mode}:{suffix}"
    self._record_call(key)
    response = self._responses.index_intros.get(key)
    if response is None:
        raise PermanentModelError(f"缺少模型响应: {key}")
    return response.model_copy(deep=True)
```

必须把 `index_intros` 加入公开 `responses` 快照，但每次 adapter 加载仍深拷贝独立状态。

- [ ] **步骤 4：运行 fake 和协议回归**

```powershell
# 验证 strict fake、旧 fixture 形状和组合模型协议
uv run pytest tests/wiki/test_ingest_fakes.py tests/wiki/test_ingest_map.py tests/wiki/test_ingest_worker.py -q
```

- [ ] **步骤 5：提交 fake Index 模型**

```powershell
# 暂存模型端口、fake 和测试
git add app/wiki/ingest/ports.py app/wiki/ingest/fakes.py tests/wiki/test_ingest_fakes.py

# 创建独立 fake 提交
git commit -m "feat: add fake wiki index intro model"
```

---

### 任务 5：Store canonical Index 上下文

**文件：**

- 修改：`app/wiki/ingest/store.py`
- 修改：`tests/wiki/test_ingest_store.py`

- [ ] **步骤 1：先写 canonical 身份、200 上限、稳定排序和 scope 测试**

```python
@pytest.mark.asyncio
async def test_load_index_intro_context_is_canonical_scoped_and_narrow():
    session = _ScriptedSession(
        [
            _ScriptedResult(
                rows=[(uuid4(), 3, "旧简介", "旧简介")]
            ),
            _ScriptedResult(
                rows=[
                    ("summary/knowledge-2", "文档二", "摘要二"),
                    ("summary/knowledge-1", "文档一", "摘要一"),
                ]
            ),
        ]
    )
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), _RecordingFinalization())
    context = await store.load_index_intro_context(SCOPE)
    assert context.index is not None and context.index.version == 3
    assert [item.slug for item in context.recent_summaries] == [
        "summary/knowledge-2",
        "summary/knowledge-1",
    ]
    index_sql, summary_sql = (_sql(statement) for statement in session.statements)
    assert "wiki_pages.slug = 'index'" in index_sql
    assert "wiki_pages.page_type = 'index'" in index_sql
    assert " OR " in index_sql
    assert "wiki_pages.tenant_id" in index_sql
    assert "wiki_pages.knowledge_base_id" in index_sql
    assert "ORDER BY wiki_pages.updated_at DESC, wiki_pages.id DESC" in summary_sql
    assert "LIMIT 200" in summary_sql
    assert "wiki_pages.content" not in summary_sql
```

另加污染行测试：canonical 查询返回两行、Index `content != summary`、summary slug/type 不一致或超过 200 行时均抛 `InvariantError`，不能静默选一行或截断脏结果。

- [ ] **步骤 2：运行 Store 定向测试确认 RED**

```powershell
# Store 协议和实现尚无 Index context，预期 AttributeError
uv run pytest tests/wiki/test_ingest_store.py -q
```

- [ ] **步骤 3：扩展 StorePort、FakeStore 和 SQL Store**

`WikiIngestStore` 增加：

```python
async def load_index_intro_context(
    self, scope: WikiScope
) -> IndexIntroContext: ...
```

`SqlAlchemyIngestStore.load_index_intro_context()` 必须在一个短 session 中执行两条窄查询：

```python
index_statement = select(
    WikiPage.id,
    WikiPage.slug,
    WikiPage.page_type,
    WikiPage.version,
    WikiPage.content,
    WikiPage.summary,
).where(
    WikiPage.tenant_id == scope.tenant_id,
    WikiPage.knowledge_base_id == scope.knowledge_base_id,
    or_(
        WikiPage.slug == "index",
        WikiPage.page_type == WikiPageType.INDEX.value,
    ),
    WikiPage.status == "published",
    WikiPage.deleted_at.is_(None),
).limit(2)

summary_statement = (
    select(WikiPage.slug, WikiPage.title, WikiPage.summary)
    .where(
        WikiPage.tenant_id == scope.tenant_id,
        WikiPage.knowledge_base_id == scope.knowledge_base_id,
        WikiPage.page_type == WikiPageType.SUMMARY.value,
        WikiPage.status == "published",
        WikiPage.deleted_at.is_(None),
    )
    .order_by(WikiPage.updated_at.desc(), WikiPage.id.desc())
    .limit(200)
)
```

Index 查询若返回超过一行，或唯一行不是同时满足 `slug='index'` 与 `page_type='index'`，抛 `InvariantError("canonical Index 身份冲突")`。然后将合法结果构造为 `IndexPageSnapshot`，summary 查询逐行构造 `IndexSummaryItem`；Pydantic 行校验错误统一包装为 `InvariantError("Index intro 上下文包含无效数据库记录")`。`FakeStore` 增加可注入 `index_intro_context`、`index_context_calls` 和返回深拷贝，后续 Worker 测试不得直接模拟 SQL。

- [ ] **步骤 4：运行 Store 和 Worker fake 回归**

```powershell
# 验证 SQL 合同、FakeStore 快照隔离和旧 Worker 行为
uv run pytest tests/wiki/test_ingest_store.py tests/wiki/test_ingest_worker.py -q
```

- [ ] **步骤 5：提交 Index 上下文读取**

```powershell
# 暂存 Store 协议、实现和测试
git add app/wiki/ingest/store.py tests/wiki/test_ingest_store.py tests/wiki/test_ingest_worker.py

# 创建独立上下文提交
git commit -m "feat: load canonical wiki index context"
```

---

### 任务 6：Index 请求规划、清理与 fallback

**文件：**

- 新建：`app/wiki/ingest/index_intro.py`
- 新建：`tests/wiki/test_ingest_index_intro.py`

- [ ] **步骤 1：先写 create/update、200 样本和清理失败测试**

```python
from uuid import uuid4

from app.wiki.ingest.index_intro import (
    DEFAULT_INDEX_INTRO,
    build_index_intro_request,
    clean_index_intro,
    fallback_index_intro_plan,
)
from app.wiki.ingest.schemas import (
    ContributionDelta,
    IndexIntroContext,
    IndexPageSnapshot,
    IndexSummaryItem,
)


def test_create_prioritizes_current_summary_and_caps_two_hundred():
    current = IndexSummaryItem(
        slug="summary/current", title="当前", summary="当前摘要"
    )
    history = tuple(
        IndexSummaryItem(
            slug=f"summary/history-{index}",
            title=f"历史 {index}",
            summary=f"摘要 {index}",
        )
        for index in range(205)
    )
    request = build_index_intro_request(
        IndexIntroContext(recent_summaries=history[:200]),
        completed_op_ids=(uuid4(),),
        pages=(current,),
        contribution_deltas=(),
        operation_actions=(("ingest", "knowledge-current"),),
    )
    assert request is not None and request.mode == "create"
    assert len(request.summaries) == 200
    assert request.summaries[0].slug == "summary/current"
    assert len({item.slug for item in request.summaries}) == 200


def test_update_contains_only_old_intro_and_stably_sorted_changes():
    context = IndexIntroContext(
        index=IndexPageSnapshot(
            id=uuid4(), version=2, content="旧简介", summary="旧简介"
        ),
        recent_summaries=(
            IndexSummaryItem(slug="summary/old", title="旧", summary="旧摘要"),
        ),
    )
    request = build_index_intro_request(
        context,
        completed_op_ids=(uuid4(), uuid4()),
        pages=(),
        contribution_deltas=(),
        operation_actions=(("retract", "knowledge-2"), ("ingest", "knowledge-1")),
    )
    assert request is not None and request.mode == "update"
    assert request.summaries == ()
    assert request.existing_intro == "旧简介"
    assert [(item.action, item.knowledge_id) for item in request.changes] == [
        ("ingest", "knowledge-1"),
        ("retract", "knowledge-2"),
    ]


def test_clean_and_fallback_follow_create_update_semantics():
    assert clean_index_intro(" 简介\n## Summary\n目录 ") == "简介"
    create_plan = fallback_index_intro_plan(
        IndexIntroContext(), mode="create", error_code="MODEL_MISSING"
    )
    assert create_plan.intro == DEFAULT_INDEX_INTRO
    assert create_plan.model_status == "defaulted"
    context = IndexIntroContext(
        index=IndexPageSnapshot(
            id=uuid4(), version=5, content="人工简介", summary="人工简介"
        )
    )
    update_plan = fallback_index_intro_plan(
        context, mode="update", error_code="MODEL_INVALID"
    )
    assert update_plan.intro == "人工简介"
    assert update_plan.model_status == "kept_after_error"
```

- [ ] **步骤 2：运行规划测试确认 RED**

```powershell
# Index 规划模块尚不存在，预期 ModuleNotFoundError
uv run pytest tests/wiki/test_ingest_index_intro.py -q
```

- [ ] **步骤 3：实现纯规划函数**

`app/wiki/ingest/index_intro.py` 固定以下常量和入口：

```python
DEFAULT_INDEX_INTRO = "本知识库汇总了当前已发布的文档摘要、实体与概念。"
LEGACY_INDEX_PLACEHOLDERS = frozenset(("", "Wiki Index", "知识库索引"))
INDEX_INTRO_MAX_CHARS = 4000


def clean_index_intro(value: str) -> str:
    cleaned = value.strip()
    marker = cleaned.find("\n## ")
    if marker >= 0:
        cleaned = cleaned[:marker].rstrip()
    if not cleaned or len(cleaned) > INDEX_INTRO_MAX_CHARS:
        raise ValueError("Index intro 必须非空且不超过 4000 字符")
    return cleaned


def build_index_intro_request(
    context: IndexIntroContext,
    *,
    completed_op_ids: tuple[UUID, ...],
    pages: tuple[ReducedPage | IndexSummaryItem, ...],
    contribution_deltas: tuple[ContributionDelta, ...],
    operation_actions: tuple[tuple[Literal["ingest", "retract"], str], ...],
) -> IndexIntroRequest | None:
    if not completed_op_ids or not operation_actions:
        return None
    current_summaries = _current_summary_items(pages)
    old_intro = context.index.content.strip() if context.index else ""
    if old_intro in LEGACY_INDEX_PLACEHOLDERS:
        samples = _stable_unique_summaries(
            (*current_summaries, *context.recent_summaries)
        )[:200]
        return IndexIntroRequest(mode="create", summaries=samples)
    changes = _build_changes(operation_actions, pages, contribution_deltas)
    if not changes:
        return None
    return IndexIntroRequest(
        mode="update",
        existing_intro=old_intro,
        changes=changes,
    )
```

`_current_summary_items()` 只接受未删除 `page_type=summary` 的最终页面；`_stable_unique_summaries()` 按输入顺序以 slug 去重；`_build_changes()` 只使用 completed operation 的 action/knowledge ID，并把相关最终/previous summary 快照放入 `pages`，最后按 `(action, knowledge_id, page.slug)` 排序。`build_success_index_intro_plan()` 使用 `clean_index_intro()` 后，把 context 的 id/version 带入 update 计划；`fallback_index_intro_plan()` 首次固定使用默认简介，增量固定保留 snapshot content。

- [ ] **步骤 4：运行纯规划和 Schema 测试确认 GREEN**

```powershell
# 验证请求有界、排序稳定、输出清理和 fallback
uv run pytest tests/wiki/test_ingest_index_intro.py tests/wiki/test_ingest_schemas.py -q
```

- [ ] **步骤 5：提交 Index 规划模块**

```powershell
# 暂存 Index 纯函数和测试
git add app/wiki/ingest/index_intro.py tests/wiki/test_ingest_index_intro.py

# 创建独立规划提交
git commit -m "feat: plan wiki index intro updates"
```

---

### 任务 7：Worker 固定点后的 Index best-effort 编排

**文件：**

- 修改：`app/wiki/ingest/worker.py:185-279`
- 修改：`tests/wiki/test_ingest_worker.py`

- [ ] **步骤 1：先写 create、update、跳过和 fallback 失败测试**

在 `tests/wiki/test_ingest_worker.py` 增加：

```python
class OrderedIndexModel(OrderedTaxonomyModel):
    def __init__(
        self,
        dataset: FakeDataset,
        *,
        events: list[str],
        intro: str = "首次简介",
        transient_failures: int = 0,
        permanent_error: bool = False,
    ) -> None:
        super().__init__(dataset, events=events)
        self.index_intro = intro
        self.index_transient_failures = transient_failures
        self.index_permanent_error = permanent_error

    async def generate_index_intro(
        self, request: IndexIntroRequest
    ) -> IndexIntroOutput:
        snapshot = IndexIntroRequest.model_validate(request.model_dump(mode="python"))
        self.index_intro_requests.append(snapshot)
        assert self.events is not None
        self.events.append("index-model")
        if self.index_transient_failures:
            self.index_transient_failures -= 1
            raise TransientModelError("index transient")
        if self.index_permanent_error:
            raise PermanentModelError("index permanent")
        return IndexIntroOutput(intro=self.index_intro)


@pytest.mark.asyncio
async def test_index_intro_runs_after_fixed_point_and_reaches_apply_request():
    events: list[str] = []
    dataset = fake_dataset(("doc-a",))
    source = FakeKnowledgeSource(dataset)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")],
        index_intro_context=IndexIntroContext(),
        events=events,
    )
    model = OrderedIndexModel(dataset, events=events, intro="首次简介")
    result = await worker(store, source, model).run_batch(SCOPE)
    assert result.completed_ops == 1
    assert events.index("reduce") < events.index("index-context")
    assert events.index("index-model") < events.index("apply")
    assert len(store.apply_calls) == 1
    plan = store.apply_calls[0].index_intro_plan
    assert plan is not None
    assert (plan.mode, plan.intro, plan.model_status) == (
        "create",
        "首次简介",
        "generated",
    )


@pytest.mark.asyncio
async def test_index_intro_update_uses_only_completed_operation_changes():
    dataset = fake_dataset(("doc-a", "doc-b"))
    source = FakeKnowledgeSource(dataset)
    completed = pending_op(OP_A, "doc-a", op="ingest")
    superseded = pending_op(OP_B, "doc-b", op="retract")
    store = WorkerStore(
        [completed, superseded],
        index_intro_context=IndexIntroContext(
            index=IndexPageSnapshot(
                id=uuid4(), version=4, content="旧简介", summary="旧简介"
            )
        ),
    )
    tombstones = FakeTombstones(deleted_on_call={"doc-b": 1})
    model = OrderedIndexModel(dataset, events=[], intro="增量简介")
    await worker(
        store, source, model, tombstones=tombstones
    ).run_batch(SCOPE)
    assert len(model.index_intro_requests) == 1
    request = model.index_intro_requests[0]
    assert request.mode == "update"
    assert [(item.action, item.knowledge_id) for item in request.changes] == [
        ("ingest", "doc-a")
    ]


@pytest.mark.asyncio
async def test_no_completed_change_skips_index_context_and_model():
    dataset = fake_dataset(("doc-a",))
    source = FakeKnowledgeSource(dataset)
    store = WorkerStore([pending_op(OP_A, "doc-a", op="unknown")])
    model = OrderedIndexModel(dataset, events=[], intro="不能调用")
    await worker(store, source, model).run_batch(SCOPE)
    assert store.index_context_calls == []
    assert model.index_intro_requests == []
    assert store.apply_calls[0].index_intro_plan is None


@pytest.mark.asyncio
async def test_index_model_failures_fallback_without_failing_pending_operation():
    waits: list[int] = []
    dataset = fake_dataset(("doc-a",))
    source = FakeKnowledgeSource(dataset)
    store = WorkerStore(
        [pending_op(OP_A, "doc-a")],
        index_intro_context=IndexIntroContext(),
    )
    model = OrderedIndexModel(dataset, events=[], transient_failures=3)
    result = await worker(store, source, model, waits=waits).run_batch(SCOPE)
    assert result.completed_ops == 1
    assert result.failed_ops == 0
    assert waits == [2, 4]
    plan = store.apply_calls[0].index_intro_plan
    assert plan is not None and plan.model_status == "defaulted"
```

再加两个控制流测试：`generate_index_intro()` 抛 `asyncio.CancelledError` 或 `WikiLockLost` 时异常向上传播、`apply_calls == []`，并沿用现有 claim release 断言；永久错误只调用一次并产生 fallback。

同时扩展现有 `WorkerStore` 测试替身，确保所有新 Worker 调用都有可观察合同：

```python
self.index_intro_context = index_intro_context or IndexIntroContext()
self.index_context_calls: list[WikiScope] = []

async def load_index_intro_context(self, scope: WikiScope) -> IndexIntroContext:
    self.index_context_calls.append(scope)
    if self.events is not None:
        self.events.append("index-context")
    return IndexIntroContext.model_validate(
        self.index_intro_context.model_dump(mode="python")
    )
```

在 `apply_results_with_outcome()` 写入 `apply_calls` 前追加 `self.events.append("apply")`；`__init__()` 增加关键字参数 `index_intro_context: IndexIntroContext | None = None`。

- [ ] **步骤 2：运行 Worker 定向测试确认 RED**

```powershell
# Worker 尚未读取 Index context 或携带计划，预期断言失败
uv run pytest tests/wiki/test_ingest_worker.py -q
```

- [ ] **步骤 3：在稳定固定点后构造并执行 Index 计划**

在 `_process_claimed_batch()` 完成 `completed_ids`、`contribution_deltas` 和 `expectations` 后调用新 helper：

```python
index_intro_plan = await self._plan_index_intro(
    scope,
    records=records,
    completed_ids=tuple(completed_ids),
    pages=tuple(pages),
    contribution_deltas=contribution_deltas,
)
```

helper 固定为：

```python
async def _plan_index_intro(
    self,
    scope: WikiScope,
    *,
    records: Sequence[PendingOpRecord],
    completed_ids: tuple[UUID, ...],
    pages: tuple[ReducedPage, ...],
    contribution_deltas: tuple[ContributionDelta, ...],
) -> IndexIntroPlan | None:
    completed = set(completed_ids)
    actions = tuple(
        (record.op, record.knowledge_id)
        for record in records
        if record.id in completed and record.op in {"ingest", "retract"}
    )
    if not actions:
        return None
    context = await self._store.load_index_intro_context(scope)
    request = build_index_intro_request(
        context,
        completed_op_ids=completed_ids,
        pages=pages,
        contribution_deltas=contribution_deltas,
        operation_actions=actions,
    )
    if request is None:
        return None
    try:
        output = await self._retry_model(
            lambda: self._model.generate_index_intro(request)
        )
        return build_success_index_intro_plan(context, request.mode, output)
    except (PermanentModelError, ValidationError, ValueError) as error:
        return fallback_index_intro_plan(
            context,
            mode=request.mode,
            error_code=type(error).__name__.upper()[:128],
        )
    except TransientModelError as error:
        return fallback_index_intro_plan(
            context,
            mode=request.mode,
            error_code=type(error).__name__.upper()[:128],
        )
```

`CancelledError`、`WikiLockLost`、`KeyboardInterrupt` 和 Store 异常不在 catch 集合中。`BatchApplyRequest(...)` 增加 `index_intro_plan=index_intro_plan`；Index 规划不放入 `while True` 固定点，也不改变 failures、superseded、fail_count 或 dead-letter。

- [ ] **步骤 4：运行 Worker 固定点、重试和取消回归**

```powershell
# 验证 Index 在固定点后执行且不改变 operation 终态
uv run pytest tests/wiki/test_ingest_worker.py tests/wiki/test_ingest_index_intro.py -q
```

- [ ] **步骤 5：提交 Worker Index 编排**

```powershell
# 暂存 Worker 编排和测试
git add app/wiki/ingest/worker.py tests/wiki/test_ingest_worker.py

# 创建独立 Worker 提交
git commit -m "feat: orchestrate wiki index intro planning"
```

---

### 任务 8：Store 自动链接候选与页面版本语义

**文件：**

- 修改：`app/wiki/ingest/store.py:2081-2466`
- 修改：`tests/wiki/test_ingest_store.py`

- [ ] **步骤 1：先写候选范围、旧出链、版本和事务一致性失败测试**

在 scripted Store 测试中覆盖以下一批行为：

```python
@pytest.mark.asyncio
async def test_apply_linkifies_only_selected_success_pages_and_rebuilds_final_links():
    request = _batch_request(
        pages=(
            _result_page(
                slug="concept/python",
                title="Python",
                content="Python uses PostgreSQL. Redis is shown in `Redis`.",
            ),
            _result_page(
                slug="entity/postgresql",
                title="PostgreSQL",
                aliases=("Postgres",),
                content="Database",
            ),
        )
    )
    session = scripted_apply_session(
        request,
        old_outgoing={"concept/python": ("entity/redis",)},
        candidate_pages=(
            candidate_row("entity/redis", "Redis", ()),
        ),
    )
    store = SqlAlchemyIngestStore(_OneSessionFactory(session), _RecordingFinalization())
    await store.apply_results_with_outcome(SCOPE, request)
    page = session.added_page("concept/python")
    assert page.content == (
        "Python uses [[entity/postgresql|PostgreSQL]]. "
        "[[entity/redis|Redis]] is shown in `Redis`."
    )
    assert session.replaced_links[page.id] == (
        "entity/postgresql",
        "entity/redis",
    )


@pytest.mark.asyncio
async def test_auto_link_markup_alone_does_not_increment_existing_page_version():
    existing = _page(
        slug="concept/python",
        title="Python",
        content="Python uses PostgreSQL.",
        version=7,
    )
    request = _batch_request(
        pages=(
            _result_page(
                slug="concept/python",
                title="Python",
                content="Python uses PostgreSQL.",
            ),
            _result_page(slug="entity/postgresql", title="PostgreSQL"),
        ),
        expected_pages=(
            PageExpectation(
                slug=existing.slug, page_id=existing.id, version=existing.version
            ),
            PageExpectation(slug="entity/postgresql"),
        ),
    )
    session = scripted_apply_session(request, existing_pages=(existing,))
    await SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization()
    ).apply_results_with_outcome(SCOPE, request)
    assert existing.version == 7
    assert existing.content == "Python uses [[entity/postgresql|PostgreSQL]]."
```

同文件再加断言：失败或 superseded operation 独占页面不进入 fresh candidate；当前页 self slug 不链接；archived/deleted/index/log/跨 tenant/跨 KB 候选被拒绝；同显示名指向 fresh 与旧出链两个 slug 时整体不链接；页面 CAS、claim 丢失或污染候选触发异常时 content、`WikiLink` 和 Index 均未提交。

- [ ] **步骤 2：运行 Store 自动链接测试确认 RED**

```powershell
# Store 仍直接保存 Reduce 正文，预期正文和版本断言失败
uv run pytest tests/wiki/test_ingest_store.py -q
```

- [ ] **步骤 3：增加批量窄查询和候选快照 helper**

在 `store.py` 增加只供事务内部使用的 frozen 记录：

```python
@dataclass(frozen=True, slots=True)
class _LinkCandidatePage:
    slug: str
    title: str
    aliases: tuple[str, ...]
    page_type: str


async def _load_linkify_candidates(
    session: AsyncSession,
    scope: WikiScope,
    selected_pages: Sequence[tuple[WikiPage | None, ReducedPage]],
) -> dict[str, tuple[LinkCandidate, ...]]:
    source_ids = tuple(
        row.id
        for row, reduced in selected_pages
        if row is not None and not reduced.deleted
    )
    old_edges = await _load_old_outgoing_targets(session, scope, source_ids)
    fresh = _fresh_candidate_pages(selected_pages)
    target_slugs = set(fresh)
    for values in old_edges.values():
        target_slugs.update(values)
    historical = await _load_active_candidate_pages(
        session, scope, tuple(sorted(target_slugs - set(fresh)))
    )
    return _candidates_by_source(selected_pages, old_edges, fresh, historical)
```

三条查询边界固定为：旧边按 `tenant_id + knowledge_base_id + source_page_id IN (...)` 一次读取；候选页按同 scope、slug 集合、`deleted_at IS NULL`、`status='published'` 一次读取；只选 `slug/title/aliases/page_type`。fresh 候选必须由经过 completed contributor 和 Store expectation 校验后的 `selected_pages` 构造，不读取 Worker 原始失败页面。

- [ ] **步骤 4：先按 semantic content 计算版本，再写 persisted content**

进入页面循环前调用 `_load_linkify_candidates()`。既有页面的版本比较保持当前字段集合，但 `values["content"]` 必须使用 `reduced.content`；完成比较和 `row.version += 1` 后再装饰：

```python
semantic_content = reduced.content
if reduced.deleted or reduced.page_type in {"index", "log"}:
    persisted_content = semantic_content
    added_slugs: tuple[str, ...] = ()
else:
    linkified = linkify_markdown(
        semantic_content,
        current_slug=reduced.slug,
        candidates=candidates_by_source.get(reduced.slug, ()),
    )
    persisted_content = linkified.content
    added_slugs = linkified.added_slugs
row.content = persisted_content
linkify_results[row.slug] = added_slugs
```

flush 新页获得 id 后，对 `row.content` 调用统一 `extract_wiki_links()`，不得再使用 `reduced.content`。保留现有 `replace_page_links()` 和 target backfill 顺序，因此正文与结构化边在同一事务可见。

- [ ] **步骤 5：运行 Store、linkify 和页面服务组合回归**

```powershell
# 验证候选范围、版本语义、链接投影和原子回滚
uv run pytest tests/wiki/test_linkify.py tests/wiki/test_ingest_store.py tests/wiki/test_sql_page_store.py tests/wiki/test_page_service.py -q
```

- [ ] **步骤 6：提交 Store 自动链接**

```powershell
# 暂存事务内自动链接和测试
git add app/wiki/ingest/store.py tests/wiki/test_ingest_store.py

# 创建独立 Store 提交
git commit -m "feat: atomically linkify wiki ingest pages"
```

---

### 任务 9：Store canonical Index CAS 与增强批次日志

**文件：**

- 修改：`app/wiki/ingest/store.py:2468-2497`
- 修改：`tests/wiki/test_ingest_store.py`

- [ ] **步骤 1：先写 Index create/update/no-op/stale 与日志失败测试**

```python
@pytest.mark.asyncio
async def test_apply_creates_canonical_index_and_reports_actual_enhancements():
    request = _batch_request(
        index_intro_plan=IndexIntroPlan(
            mode="create",
            intro="首次简介",
            model_status="generated",
        )
    )
    session = scripted_apply_session(request, canonical_index_rows=())
    await SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization()
    ).apply_results_with_outcome(SCOPE, request)
    index = session.added_page("index")
    assert (index.page_type, index.status, index.version) == (
        "index",
        "published",
        1,
    )
    assert index.content == index.summary == "首次简介"
    log = session.added_log()
    assert "自动链接页面" in log.message
    assert "Index=created" in log.message
    assert {item["slug"] for item in log.pages_affected} >= {"index"}


@pytest.mark.asyncio
async def test_apply_updates_index_only_when_id_and_version_match():
    index = _page(
        slug="index",
        page_type="index",
        content="旧简介",
        summary="旧简介",
        version=4,
    )
    matching = _batch_request(
        index_intro_plan=IndexIntroPlan(
            mode="update",
            expected_page_id=index.id,
            expected_version=4,
            intro="新简介",
            model_status="generated",
        )
    )
    session = scripted_apply_session(matching, canonical_index_rows=(index,))
    await SqlAlchemyIngestStore(
        _OneSessionFactory(session), _RecordingFinalization()
    ).apply_results_with_outcome(SCOPE, matching)
    assert index.content == index.summary == "新简介"
    assert index.version == 5

    stale = matching.model_copy(
        update={
            "operation_id": uuid4(),
            "index_intro_plan": matching.index_intro_plan.model_copy(
                update={"expected_version": 4, "intro": "过期写入"}
            ),
        }
    )
    index.version = 6
    index.content = index.summary = "人工编辑"
    stale_session = scripted_apply_session(stale, canonical_index_rows=(index,))
    await SqlAlchemyIngestStore(
        _OneSessionFactory(stale_session), _RecordingFinalization()
    ).apply_results_with_outcome(SCOPE, stale)
    assert index.content == index.summary == "人工编辑"
    assert "Index=stale_skipped" in stale_session.added_log().message
```

再覆盖：并发 create 已出现 canonical Index 时为 `stale_skipped`；相同 intro 为 `unchanged` 且不增版本；default create 日志为 `defaulted`；update fallback 保留旧值且为 `kept_after_error`；无计划为 `not_requested`；非 canonical `page_type=index` 行不被覆盖；同 scope canonical 身份冲突抛 `InvariantError` 并回滚整批。

- [ ] **步骤 2：运行 Store Index/log 测试确认 RED**

```powershell
# Store 尚未应用 index_intro_plan 或记录增强摘要，预期断言失败
uv run pytest tests/wiki/test_ingest_store.py -q
```

- [ ] **步骤 3：实现锁定 canonical Index 和实际结果枚举**

在结果事务内、页面和链接写入后执行：

```python
IndexApplyResult = Literal[
    "created",
    "updated",
    "unchanged",
    "defaulted",
    "kept_after_error",
    "stale_skipped",
    "not_requested",
]

index_result, changed_index = await self._apply_index_intro_plan(
    session,
    scope,
    request.index_intro_plan,
)
```

`_apply_index_intro_plan()` 用 `tenant_id + knowledge_base_id + (slug='index' OR page_type='index')` 加 `FOR UPDATE`，最多读取两行；超过一行或唯一行不是 canonical 身份时抛 `InvariantError`。create 只有零行时创建根目录系统页；必须使用 PostgreSQL `insert(WikiPage).on_conflict_do_nothing(index_elements=(WikiPage.knowledge_base_id, WikiPage.slug), index_where=WikiPage.deleted_at.is_(None)).returning(WikiPage.id)`，冲突未返回 id 时重新检查身份并返回 `stale_skipped`，不能捕获 `IntegrityError` 后继续使用已失败事务。update 只有 id/version 精确匹配时写 content/summary 并增一次版本；相同 intro 不增版本。snapshot 不匹配或 canonical 被删除/归档返回 `stale_skipped`，其他数据库错误继续传播。

Store 在应用计划前还必须比较实际 `completed_ids` 与 `request.completed_op_ids`。只要 Store 因同批 operation 顺序重新判定了 superseded，整个 Index 计划返回 `stale_skipped`，不能把已经不属于最终成功集合的模型结果写入；正文、自动链接和剩余成功 operation 仍正常提交。

锁定 pending rows 后还要验证：存在计划时，`completed_ids` 中至少一行 `op` 为 `ingest/retract`；否则抛 `InvariantError("Index intro 计划没有对应的成功增量操作")`。该校验发生在页面、链接和 Index 写入前，防止调用方伪造只对应未知 operation 的计划。

- [ ] **步骤 4：用实际 linkify/Index 结果构造一条幂等日志**

日志内容固定为稳定、无正文的摘要：

```python
linkified_page_count = sum(bool(slugs) for slugs in linkify_results.values())
inserted_link_count = sum(len(slugs) for slugs in linkify_results.values())
message = (
    f"完成 {len(completed_ids)} 个 Wiki 操作，"
    f"跳过 {len(superseded_ids)} 个过期操作；"
    f"自动链接页面={linkified_page_count}，"
    f"新增链接={inserted_link_count}，Index={index_result}"
)
```

`pages_affected` 先取实际 persisted 语义页面的 slug/title 稳定快照；仅当 `changed_index is not None` 且结果为 `created/updated/defaulted` 时追加 canonical Index。`unchanged`、`kept_after_error`、`stale_skipped`、`not_requested` 不得伪报 Index 页面变化。现有 `result_outcome` 仍先于所有副作用处理重放：已存在日志直接恢复 outcome，不执行 linkify、Index 或第二条日志。

- [ ] **步骤 5：运行 Store 幂等、失败和日志回归**

```powershell
# 验证 Index CAS、日志计数、失败回滚和 operation 重放
uv run pytest tests/wiki/test_ingest_store.py tests/wiki/test_models.py -q
```

- [ ] **步骤 6：提交 Index 事务和日志增强**

```powershell
# 暂存 Index CAS、日志和测试
git add app/wiki/ingest/store.py tests/wiki/test_ingest_store.py

# 创建独立事务提交
git commit -m "feat: persist wiki index intro atomically"
```

---

### 任务 10：canonical Index 查询与 Graph 可见边修正

**文件：**

- 修改：`app/wiki/query_service.py:115-182, 250-307, 411-515, 740-768`
- 修改：`tests/wiki/test_query_service.py`
- 修改：`tests/wiki/test_postgres_integration.py`

- [ ] **步骤 1：先写 canonical Index SQL 和活动已解析 degree 失败测试**

```python
def test_index_intro_query_uses_canonical_slug_and_type():
    statement = build_index_intro_statement(SCOPE)
    sql = _sql(statement)
    assert "wiki_pages.slug = 'index'" in sql
    assert "wiki_pages.page_type = 'index'" in sql
    assert "ORDER BY wiki_pages.updated_at" not in sql


def test_graph_degree_filters_active_published_resolved_endpoints():
    sql = _sql(_graph_degree_cte(SCOPE).select())
    assert sql.count("wiki_pages") >= 2
    assert "source_page.deleted_at IS NULL" in sql
    assert "source_page.status = 'published'" in sql
    assert "target_page.deleted_at IS NULL" in sql
    assert "target_page.status = 'published'" in sql
    assert "wiki_links.target_page_id IS NOT NULL" in sql
    assert "wiki_links.tenant_id" in sql
    assert "wiki_links.knowledge_base_id" in sql
```

真实 PostgreSQL 测试创建 active、archived、soft-deleted 和 unresolved target，断言 overview/ego 只返回 active-published 两端的边；overview top-N 只返回选中节点之间的边；types 过滤不能通过隐藏类型扩展下一跳；边按 `(source, target)` 排序。

- [ ] **步骤 2：运行 Query 测试确认 RED**

```powershell
# 当前 degree 直接统计 wiki_links，canonical Index 也按最近更新时间选择，预期断言失败
uv run pytest tests/wiki/test_query_service.py -q
```

- [ ] **步骤 3：固定 canonical Index 读取**

新增并在 `get_index()` 中使用：

```python
def build_index_intro_statement(scope: WikiScope):
    return select(
        WikiPage.slug,
        WikiPage.page_type,
        WikiPage.content,
        WikiPage.version,
    ).where(
        *_active_page_scope(scope),
        or_(
            WikiPage.slug == "index",
            WikiPage.page_type == WikiPageType.INDEX.value,
        ),
    ).limit(2)
```

若查询返回超过一行，或唯一行不是同时满足 `slug='index'` 和 `page_type='index'`，抛 `WikiValidationError("INDEX_IDENTITY_CONFLICT", "canonical Index 身份冲突")`；零行继续返回 `intro="", version=0`。REST 响应字段保持原样。

- [ ] **步骤 4：用活动 source/target 构造统一可见边 CTE**

将 `_graph_degree_cte()` 改为先构造：

```python
source_page = aliased(WikiPage, name="source_page")
target_page = aliased(WikiPage, name="target_page")
visible_edges = (
    select(
        WikiLink.source_page_id.label("source_page_id"),
        WikiLink.target_page_id.label("target_page_id"),
    )
    .join(source_page, source_page.id == WikiLink.source_page_id)
    .join(target_page, target_page.id == WikiLink.target_page_id)
    .where(
        WikiLink.tenant_id == scope.tenant_id,
        WikiLink.knowledge_base_id == scope.knowledge_base_id,
        WikiLink.target_page_id.is_not(None),
        source_page.tenant_id == scope.tenant_id,
        source_page.knowledge_base_id == scope.knowledge_base_id,
        source_page.deleted_at.is_(None),
        source_page.status == "published",
        target_page.tenant_id == scope.tenant_id,
        target_page.knowledge_base_id == scope.knowledge_base_id,
        target_page.deleted_at.is_(None),
        target_page.status == "published",
    )
    .cte("visible_edges")
)
```

degree、ego neighbor 和 `_edges_between()` 全部从同一 `visible_edges` 构造，避免某一查询重新放宽条件。Overview 仍先按 `link_count DESC, slug ASC` 取 top-N，再查询 id 集合内部边；Ego 仍做入边/出边无向 BFS，types 同时限制中心、邻居和下一轮 frontier。

- [ ] **步骤 5：运行 Query 和真实 PostgreSQL 图谱回归**

```powershell
# 验证 SQL 结构和无服务查询单元测试
uv run pytest tests/wiki/test_query_service.py -q

# 显式配置 PostgreSQL 后验证真实图谱可见性
$env:GRAPH_TEST_POSTGRES_URL = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph'
uv run pytest tests/wiki/test_postgres_integration.py -k "graph or index" -q
```

- [ ] **步骤 6：提交查询增强**

```powershell
# 暂存 Query、真实数据库用例和测试
git add app/wiki/query_service.py tests/wiki/test_query_service.py tests/wiki/test_postgres_integration.py

# 创建独立查询提交
git commit -m "fix: scope wiki graph to visible resolved pages"
```

---

### 任务 11：fake runtime、fixture 与真实事务端到端

**文件：**

- 修改：`examples/wiki_fake_data.json`
- 修改：`tests/wiki/test_enqueue_fake_cli.py`
- 修改：`tests/wiki/test_postgres_integration.py`

- [ ] **步骤 1：先写示例 fixture strict key 合同失败测试**

```python
def test_example_fixture_has_phase_four_b_index_intro_responses():
    fixture_path = Path(__file__).parents[2] / "examples" / "wiki_fake_data.json"
    raw = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert raw["model_responses"]["index_intros"] == {
        "index_intro:create:summary/knowledge-1": {"intro": "知识库首次简介。"},
        "index_intro:update:ingest:knowledge-1,retract:knowledge-2": {
            "intro": "知识库增量简介。"
        },
    }
    _, model, _ = load_fake_runtime_adapters(fixture_path)
    assert set(model.responses["index_intros"]) == set(
        raw["model_responses"]["index_intros"]
    )
```

- [ ] **步骤 2：写真实首次摄取、增量更新和人工 CAS 保护测试**

```python
@pytest.mark.asyncio
async def test_real_worker_linkifies_pages_creates_index_and_logs_once(
    postgres_factory,
):
    scope, store, worker, _, _, op_id = await _real_taxonomy_worker(
        postgres_factory
    )
    first = await worker.run_batch(scope)
    assert first.completed_op_ids == (op_id,)
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
        assert pages["index"].content == pages["index"].summary
        assert pages["index"].version == 1
        assert "[[" in pages["concept/retrieval"].content
        links = list(
            (
                await session.execute(
                    select(WikiLink).where(
                        WikiLink.knowledge_base_id == scope.knowledge_base_id
                    )
                )
            ).scalars()
        )
        assert links
        logs = list(
            (
                await session.execute(
                    select(WikiLogEntry).where(
                        WikiLogEntry.knowledge_base_id == scope.knowledge_base_id
                    )
                )
            ).scalars()
        )
        assert len(logs) == 1 and "Index=created" in logs[0].message
```

第二个真实测试先完成首次 ingest，再入队 fixture 中的 ingest/retract 增量组合，断言 Index 只增一个版本、request key 为 `index_intro:update:ingest:knowledge-1,retract:knowledge-2`。第三个测试在 Worker snapshot 后人工修改 canonical Index 并增版本，再提交 batch，断言语义页面和自动链接正常提交、人工 Index 保留、日志为 `stale_skipped`。第四个测试让 fake Index 永久失败，断言首次默认、增量保留、pending `fail_count` 不增加。

- [ ] **步骤 3：更新示例 fixture 并运行 fake runtime 测试**

`examples/wiki_fake_data.json` 的 `model_responses` 增加：

```json
"index_intros": {
  "index_intro:create:summary/knowledge-1": {
    "intro": "知识库首次简介。"
  },
  "index_intro:update:ingest:knowledge-1,retract:knowledge-2": {
    "intro": "知识库增量简介。"
  }
}
```

```powershell
# 验证示例 fixture 可加载且 CLI 仍使用相同环境入口
uv run pytest tests/wiki/test_ingest_fakes.py tests/wiki/test_enqueue_fake_cli.py tests/wiki/test_wiki_tasks.py -q
```

- [ ] **步骤 4：运行真实 PostgreSQL/Redis 4B 端到端**

```powershell
# 启动本计划需要的测试服务
docker compose up -d postgres redis

# 显式指定 opt-in 测试连接
$env:GRAPH_TEST_POSTGRES_URL = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph'
$env:GRAPH_TEST_REDIS_URL = 'redis://127.0.0.1:6379/15'

# 运行摄取、Index、自动链接、Graph 和日志真实事务测试
uv run pytest tests/wiki/test_postgres_integration.py tests/wiki/test_ingest_worker.py -q
```

- [ ] **步骤 5：提交 fixture 和端到端测试**

```powershell
# 暂存示例 fixture 与真实事务测试
git add examples/wiki_fake_data.json tests/wiki/test_enqueue_fake_cli.py tests/wiki/test_postgres_integration.py

# 创建独立端到端提交
git commit -m "test: cover wiki phase four b pipeline"
```

---

### 任务 12：中文文档、合同测试和最终验收

**文件：**

- 新建：`docs/Wiki阶段四B.md`
- 新建：`tests/wiki/test_phase_four_b_docs.py`
- 修改：`README.md`

- [ ] **步骤 1：先写中文文档、无迁移和范围失败测试**

```python
from pathlib import Path


def test_phase_four_b_doc_describes_implemented_contracts():
    text = Path("docs/Wiki阶段四B.md").read_text(encoding="utf-8")
    for phrase in (
        "受保护 Markdown 区域",
        "本批受影响页面",
        "每个 slug 只链接一次",
        "strict fake",
        "最多 200 条",
        "stale_skipped",
        "自动链接不额外增加页面版本",
        "活动、published",
        "同一事务",
    ):
        assert phrase in text


def test_phase_four_b_doc_does_not_claim_later_scope():
    text = Path("docs/Wiki阶段四B.md").read_text(encoding="utf-8")
    for forbidden in (
        "全库自动补链已实现",
        "broken-link 自动清理已实现",
        "auto-fix 已实现",
        "Agent 已实现",
        "WikiPageIndexer 已实现",
        "真实 Index 模型已实现",
    ):
        assert forbidden not in text


def test_phase_four_b_adds_no_migration_or_rest_contract_change():
    versions = sorted(Path("migrations/versions").glob("*.py"))
    assert versions[-1].name == "20260719_04_add_wiki_log_result_outcome.py"
    design = Path(
        "docs/superpowers/specs/2026-07-20-wiki-phase-4b-design.md"
    ).read_text(encoding="utf-8")
    assert "REST DTO 不增加字段" in design
```

- [ ] **步骤 2：运行文档测试确认 RED**

```powershell
# 阶段四 B 文档尚不存在，预期 FileNotFoundError
uv run pytest tests/wiki/test_phase_four_b_docs.py -q
```

- [ ] **步骤 3：编写与实现一致的中文运行文档和 README 入口**

`docs/Wiki阶段四B.md` 必须说明：

- protected-span 的 fenced/inline code、Wiki/Markdown/reference/autolink 保护规则。
- 候选只来自本批最终成功页和受影响页原有出链目标，不扫描全库正文。
- 长名称、歧义、ASCII 边界、CJK、自链接、每 slug 一次和幂等规则。
- canonical `slug=index/page_type=index`、create/update strict fake key、200 summary 上限。
- 三次瞬时重试、首次默认、增量保留、人工 CAS 冲突 `stale_skipped`。
- semantic/persisted content 与页面版本语义。
- Graph 活动已解析边、批次日志计数和 `pages_affected` 规则。
- 当前限制和阶段四 C/阶段五边界。

所有命令前必须有中文用途注释，例如：

```powershell
# 运行阶段四 B 的纯函数和 Worker 定向测试
uv run pytest tests/wiki/test_linkify.py tests/wiki/test_ingest_index_intro.py tests/wiki/test_ingest_worker.py -q

# 使用 strict fake fixture 入队首次摄取
uv run python -m app.wiki.tasks.enqueue_fake --op ingest --kb-id 11111111-1111-1111-1111-111111111111 --knowledge-id knowledge-1
```

README 只增加“Wiki 阶段四 B”链接和一句当前能力摘要，不复制运行手册。

- [ ] **步骤 4：执行静态、迁移、Compose 和无服务全量验收**

```powershell
# 验证所有 Python 文件可编译
uv run python -m compileall app tests migrations

# 验证 Alembic 仍只有阶段三唯一 head
uv run alembic heads

# 验证完整离线升级 SQL 仍可生成
uv run alembic upgrade head --sql

# 验证 Compose 配置可解析
docker compose config --quiet

# 验证阶段四 B 修改文件的 Ruff 规则
uv run ruff check app/wiki/linkify.py app/wiki/domain.py app/wiki/query_service.py app/wiki/ingest/index_intro.py app/wiki/ingest/schemas.py app/wiki/ingest/ports.py app/wiki/ingest/fakes.py app/wiki/ingest/store.py app/wiki/ingest/worker.py tests/wiki/test_linkify.py tests/wiki/test_ingest_index_intro.py tests/wiki/test_ingest_schemas.py tests/wiki/test_ingest_fakes.py tests/wiki/test_ingest_store.py tests/wiki/test_ingest_worker.py tests/wiki/test_query_service.py tests/wiki/test_postgres_integration.py tests/wiki/test_phase_four_b_docs.py

# 运行不连接默认服务的完整测试；真实服务用例只能按环境变量明确跳过
Remove-Item Env:GRAPH_TEST_POSTGRES_URL -ErrorAction SilentlyContinue
Remove-Item Env:GRAPH_TEST_REDIS_URL -ErrorAction SilentlyContinue
uv run pytest -p no:cacheprovider --basetemp "$env:TEMP\graph-wiki-phase4b-final-no-services" -q

# 检查补丁空白、冲突标记和工作树状态
git diff --check
git status --short
```

- [ ] **步骤 5：执行真实 PostgreSQL/Redis 全量验收**

```powershell
# 启动测试 PostgreSQL 和 Redis
docker compose up -d postgres redis

# 指定 opt-in 真实服务 URL
$env:GRAPH_TEST_POSTGRES_URL = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/graph'
$env:GRAPH_TEST_REDIS_URL = 'redis://127.0.0.1:6379/15'

# 运行真实服务完整测试集
uv run pytest -p no:cacheprovider --basetemp "$env:TEMP\graph-wiki-phase4b-final-real-services" -q

# 停止本计划启动的测试服务
docker compose stop postgres redis
```

- [ ] **步骤 6：提交中文文档和最终合同**

```powershell
# 暂存中文文档、README 和文档合同测试
git add docs/Wiki阶段四B.md README.md tests/wiki/test_phase_four_b_docs.py

# 创建独立文档提交
git commit -m "docs: document wiki phase four b"
```

---

## 最终验收清单

- [ ] 本批最终成功页面按确定性规则自动链接；失败和 superseded 独占页面不进入候选。
- [ ] fenced/inline code、Wiki/Markdown/reference 链接、定义和 autolink 保持原文，重复 linkify 完全幂等。
- [ ] 长名称优先、同显示名多 slug 整体排除、ASCII 单词边界、CJK 直接匹配、自链接和每 slug 一次规则成立。
- [ ] `extract_wiki_links()` 与 linkify 共用 scanner，代码示例伪链接不形成 `WikiLink` 或 Graph 边。
- [ ] Store 按 semantic content 判断版本，自动 markup 不额外增加页面版本。
- [ ] fresh 候选与旧出链目标按 scope 批量窄查询，无全库正文扫描和逐页 N+1。
- [ ] Index create 最多 200 条 summary 且本批优先；update 只含旧 intro 和本批结构化变化。
- [ ] strict fake key、请求快照、输出清理、三次瞬时重试和永久错误边界稳定。
- [ ] Index 首次失败写固定默认简介，增量失败保留旧简介，不增加 pending fail_count 或 dead-letter。
- [ ] canonical Index 使用 `slug=index/page_type=index`，CAS 过期不覆盖人工编辑。
- [ ] 页面、自动链接、`WikiLink`、Index、贡献、日志、pending-op、finalization 和 Outbox 同事务提交或回滚。
- [ ] 每批仍只有一条幂等日志，准确记录自动链接页数、新增链接数、Index 结果和实际 `pages_affected`。
- [ ] Graph degree、overview、ego 和边只使用活动、published、已解析的 source/target，类型过滤不穿透隐藏节点。
- [ ] Index、Graph、Log REST DTO 和迁移 head 保持不变，阶段一至四 A 测试全部通过。
- [ ] 中文文档、compileall、Ruff、Compose、无服务全量和真实 PostgreSQL/Redis 全量全部通过。
