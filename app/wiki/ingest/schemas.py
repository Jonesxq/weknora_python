"""Wiki 摄取阶段使用的结构化 DTO。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import copy
import os
import re
from types import MappingProxyType
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator

from app.wiki.scope import WikiScope


ExtractionGranularity = Literal["focused", "standard", "exhaustive"]
TopicPageType = Literal["entity", "concept"]
IngestPageType = Literal["summary", "entity", "concept"]

_SLUG_PATTERN = re.compile(
    r"^(summary|entity|concept)/[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*$"
)


class _StrictModel(BaseModel):
    """所有摄取 DTO 的共同边界：拒绝未知字段并允许测试时安全修改副本。"""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class _FrozenValueModel(BaseModel):
    """阶段三跨字段 DTO 的不可变边界。"""

    model_config = ConfigDict(extra="forbid", frozen=True)


class _FrozenMapping(Mapping[str, tuple[str, ...]]):
    """可深拷贝且不继承 dict 的只读映射。"""

    __slots__ = ("_items", "_lookup")

    def __init__(self, values: Mapping[str, tuple[str, ...]] | Iterable[tuple[str, tuple[str, ...]]]) -> None:
        source = values.items() if isinstance(values, Mapping) else values
        items = tuple((key, tuple(value)) for key, value in source)
        object.__setattr__(self, "_items", items)
        object.__setattr__(self, "_lookup", MappingProxyType(dict(items)))

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("citation refs 映射不可修改")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("citation refs 映射不可修改")

    def __getitem__(self, key: str) -> tuple[str, ...]:
        return self._lookup[key]

    def __iter__(self):  # type: ignore[no-untyped-def]
        return (key for key, _ in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def _iter_pairs(self) -> Iterable[tuple[str, tuple[str, ...]]]:
        return iter(self._items)

    def __copy__(self) -> Self:
        return self

    def __reduce__(self) -> tuple[type[Self], tuple[tuple[tuple[str, tuple[str, ...]], ...]]]:
        return type(self), (self._items,)

    def __deepcopy__(self, memo: dict[int, object]) -> Self:
        return type(self)(copy.deepcopy(self._items, memo))


def _normalize_slug(value: str, allowed_prefixes: tuple[str, ...]) -> str:
    slug = value.strip().casefold()
    if len(slug) > 255 or not _SLUG_PATTERN.fullmatch(slug):
        raise ValueError("slug 必须是合法的分层路径，且长度不能超过 255")
    if not any(slug.startswith(f"{prefix}/") for prefix in allowed_prefixes):
        raise ValueError(f"slug 必须使用以下前缀之一: {', '.join(allowed_prefixes)}")
    return slug


def _stable_clean_strings(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = value.strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            result.append(cleaned)
    return result


def _strict_clean_strings(values: list[str], field_name: str) -> list[str]:
    cleaned = [value.strip() for value in values]
    if any(not value for value in cleaned):
        raise ValueError(f"{field_name} 不能包含空值")
    if len(cleaned) != len(set(cleaned)):
        raise ValueError(f"{field_name} 不能包含重复值")
    return cleaned


class WikiIngestConfig(_StrictModel):
    wiki_enabled: bool = True
    synthesis_model_id: str | None = None
    summary_model_id: str | None = None
    extraction_granularity: ExtractionGranularity = "standard"
    max_pages_per_ingest: int = Field(default=0, ge=0)


class WikiWorkerOptions(_StrictModel):
    batch_size: int = Field(default=5, ge=1, le=100)
    map_parallel: int = Field(default=10, ge=1, le=100)
    reduce_parallel: int = Field(default=10, ge=1, le=100)
    claim_timeout_seconds: int = Field(default=600, ge=60, le=86400)
    max_pages_per_ingest: int = Field(default=0, ge=0)
    extraction_granularity: ExtractionGranularity = "standard"
    citation_batch_chars: int = Field(default=12000, ge=1000, le=100000)
    citation_parallel: int = Field(default=4, ge=1, le=32)
    dedup_candidate_limit: int = Field(default=20, ge=1, le=20)
    tombstone_ttl_seconds: int = Field(default=3600, ge=60, le=86400)

    @classmethod
    def from_env(cls) -> Self:
        return cls.model_validate(
            {
                "batch_size": os.getenv("GRAPH_WIKI_INGEST_BATCH_SIZE", "5"),
                "map_parallel": os.getenv("GRAPH_WIKI_INGEST_MAP_PARALLEL", "10"),
                "reduce_parallel": os.getenv("GRAPH_WIKI_INGEST_REDUCE_PARALLEL", "10"),
                "claim_timeout_seconds": os.getenv(
                    "GRAPH_WIKI_CLAIM_TIMEOUT_SECONDS", "600"
                ),
                "max_pages_per_ingest": os.getenv(
                    "GRAPH_WIKI_MAX_PAGES_PER_INGEST", "0"
                ),
                "extraction_granularity": os.getenv(
                    "GRAPH_WIKI_EXTRACTION_GRANULARITY", "standard"
                ),
                "citation_batch_chars": os.getenv(
                    "GRAPH_WIKI_CITATION_BATCH_CHARS", "12000"
                ),
                "citation_parallel": os.getenv("GRAPH_WIKI_CITATION_PARALLEL", "4"),
                "dedup_candidate_limit": os.getenv(
                    "GRAPH_WIKI_DEDUP_CANDIDATE_LIMIT", "20"
                ),
                "tombstone_ttl_seconds": os.getenv(
                    "GRAPH_WIKI_TOMBSTONE_TTL_SECONDS", "3600"
                ),
            }
        )


class SourceChunk(_StrictModel):
    id: str
    chunk_index: int = Field(default=0, ge=0)
    start_at: int = Field(default=0, ge=0)
    text: str = ""
    ocr_text: str = ""
    image_caption: str = ""

    @field_validator("id")
    @classmethod
    def normalize_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("chunk id 不能为空")
        return value


class SourceKnowledge(_StrictModel):
    id: str
    tenant_id: int
    knowledge_base_id: UUID
    title: str
    op_version: str
    status: Literal["ready", "deleting", "cancelled", "deleted"] = "ready"

    @property
    def is_active(self) -> bool:
        return self.status == "ready"

    @field_validator("id", "title", "op_version")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("知识条目标识、标题和版本不能为空")
        return value


class TopicCandidate(_StrictModel):
    name: str
    slug: str
    page_type: TopicPageType
    aliases: list[str] = Field(default_factory=list)
    description: str = ""
    details: str = ""

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name 不能为空")
        return value

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("entity", "concept"))

    @field_validator("aliases")
    @classmethod
    def normalize_aliases(cls, value: list[str]) -> list[str]:
        return _stable_clean_strings(value)

    @model_validator(mode="after")
    def validate_page_type_prefix(self) -> Self:
        if not self.slug.startswith(f"{self.page_type}/"):
            raise ValueError("slug 前缀必须与 page_type 一致")
        return self


class CandidateExtraction(_StrictModel):
    entities: list[TopicCandidate] = Field(default_factory=list)
    concepts: list[TopicCandidate] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_candidate_groups(self) -> Self:
        if any(candidate.page_type != "entity" for candidate in self.entities):
            raise ValueError("entities 只能包含 entity 候选")
        if any(candidate.page_type != "concept" for candidate in self.concepts):
            raise ValueError("concepts 只能包含 concept 候选")
        return self


class _ModelTextOutput(_StrictModel):
    headline: str
    markdown: str

    @field_validator("headline", "markdown")
    @classmethod
    def strip_non_empty_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("模型文本输出不能为空")
        return value


class DocumentSummary(_ModelTextOutput):
    pass


class PageContribution(_StrictModel):
    pending_op_id: UUID
    knowledge_id: str
    title: str
    content: str
    summary: str
    aliases: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    chunk_refs: list[str] = Field(default_factory=list)

    @field_validator("knowledge_id", "title")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("贡献来源标识和标题不能为空")
        return value


class PageMergeRequest(_StrictModel):
    slug: str
    title: str
    page_type: TopicPageType
    aliases: list[str] = Field(default_factory=list)
    existing_content: str = ""
    existing_summary: str = ""
    contributions: list[PageContribution] = Field(min_length=1)

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("entity", "concept"))

    @field_validator("title")
    @classmethod
    def normalize_title(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("页面标题不能为空")
        return value

    @model_validator(mode="after")
    def validate_page_type_prefix(self) -> Self:
        if not self.slug.startswith(f"{self.page_type}/"):
            raise ValueError("slug 前缀必须与 page_type 一致")
        return self


class PageMergeOutput(_ModelTextOutput):
    pass


class SlugUpdate(_StrictModel):
    pending_op_id: UUID
    knowledge_id: str
    slug: str
    title: str
    page_type: IngestPageType
    content: str = ""
    summary: str = ""
    aliases: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    chunk_refs: list[str] = Field(default_factory=list)

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("summary", "entity", "concept"))

    @model_validator(mode="after")
    def validate_page_type_prefix(self) -> Self:
        if not self.slug.startswith(f"{self.page_type}/"):
            raise ValueError("slug 前缀必须与 page_type 一致")
        return self

    @field_validator("knowledge_id", "title")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("知识标识和标题不能为空")
        return value


class MapDocumentResult(_StrictModel):
    pending_op_id: UUID
    knowledge_id: str
    updates: list[SlugUpdate] = Field(default_factory=list)
    skipped_reason: str | None = None

    @field_validator("knowledge_id")
    @classmethod
    def normalize_knowledge_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("知识标识不能为空")
        return value


class ReducedPage(_StrictModel):
    slug: str
    title: str
    page_type: IngestPageType
    content: str
    summary: str
    aliases: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    chunk_refs: list[str] = Field(default_factory=list)
    contributor_op_ids: list[UUID] = Field(default_factory=list)

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("summary", "entity", "concept"))

    @field_validator("title")
    @classmethod
    def normalize_title(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("页面标题不能为空")
        return value

    @model_validator(mode="after")
    def validate_page_type_prefix(self) -> Self:
        if not self.slug.startswith(f"{self.page_type}/"):
            raise ValueError("slug 前缀必须与 page_type 一致")
        return self


class _FrozenTopicCandidate(_FrozenValueModel):
    name: str
    slug: str
    page_type: TopicPageType
    aliases: tuple[str, ...] = ()
    description: str = ""
    details: str = ""

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name 不能为空")
        return value

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("entity", "concept"))

    @field_validator("aliases")
    @classmethod
    def normalize_aliases(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_stable_clean_strings(list(value)))

    @model_validator(mode="after")
    def validate_page_type_prefix(self) -> Self:
        if not self.slug.startswith(f"{self.page_type}/"):
            raise ValueError("slug 前缀必须与 page_type 一致")
        return self


class _FrozenReducedPage(_FrozenValueModel):
    slug: str
    title: str
    page_type: IngestPageType
    content: str
    summary: str
    aliases: tuple[str, ...] = ()
    source_refs: tuple[str, ...] = ()
    chunk_refs: tuple[str, ...] = ()
    contributor_op_ids: tuple[UUID, ...] = ()

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("summary", "entity", "concept"))

    @field_validator("title")
    @classmethod
    def normalize_title(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("页面标题不能为空")
        return value

    @model_validator(mode="after")
    def validate_page_type_prefix(self) -> Self:
        if not self.slug.startswith(f"{self.page_type}/"):
            raise ValueError("slug 前缀必须与 page_type 一致")
        return self


def _snapshot_topic(value: TopicCandidate | _FrozenTopicCandidate | dict[str, Any]) -> object:
    return value.model_dump() if isinstance(value, TopicCandidate) else value


def _snapshot_page(value: ReducedPage | _FrozenReducedPage | dict[str, Any]) -> object:
    return value.model_dump() if isinstance(value, ReducedPage) else value


class CitationBatchChunk(_FrozenValueModel):
    alias: str
    text: str

    @field_validator("alias")
    @classmethod
    def validate_alias(cls, value: str) -> str:
        value = value.strip()
        if not re.fullmatch(r"c\d{3}", value):
            raise ValueError("citation alias 必须是 c 加三位数字")
        return value

    @field_validator("text")
    @classmethod
    def validate_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("citation chunk 文本不能为空")
        return value


class CitationBatchRequest(_FrozenValueModel):
    knowledge_id: str
    batch_index: int = Field(ge=0)
    candidates: tuple[_FrozenTopicCandidate, ...]
    chunks: tuple[CitationBatchChunk, ...] = Field(min_length=1)

    @field_validator("candidates", mode="before")
    @classmethod
    def snapshot_candidates(cls, value: object) -> object:
        return [_snapshot_topic(item) for item in value] if isinstance(value, (list, tuple)) else value

    @field_validator("knowledge_id")
    @classmethod
    def validate_knowledge_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("知识标识不能为空")
        return value

    @model_validator(mode="after")
    def validate_unique_aliases(self) -> Self:
        aliases = [chunk.alias for chunk in self.chunks]
        if len(aliases) != len(set(aliases)):
            raise ValueError("citation batch 中 alias 不能重复")
        return self


class CitationBatchOutput(_FrozenValueModel):
    refs_by_slug: Mapping[str, tuple[str, ...]] = Field(default_factory=dict)
    supplemental_candidates: tuple[_FrozenTopicCandidate, ...] = ()

    @field_validator("supplemental_candidates", mode="before")
    @classmethod
    def snapshot_supplemental_candidates(cls, value: object) -> object:
        return [_snapshot_topic(item) for item in value] if isinstance(value, (list, tuple)) else value

    @field_serializer("refs_by_slug")
    def serialize_refs_by_slug(self, value: Mapping[str, tuple[str, ...]]) -> dict[str, tuple[str, ...]]:
        if isinstance(value, _FrozenMapping):
            return dict(value._iter_pairs())
        return dict(value)

    @field_validator("refs_by_slug")
    @classmethod
    def normalize_refs(cls, value: Mapping[str, tuple[str, ...]]) -> Mapping[str, tuple[str, ...]]:
        result: dict[str, tuple[str, ...]] = {}
        for slug, aliases in value.items():
            normalized_slug = _normalize_slug(slug, ("entity", "concept"))
            if normalized_slug in result:
                raise ValueError("citation refs 的 slug 不能重复")
            cleaned = _strict_clean_strings(list(aliases), "citation ref alias")
            if not cleaned:
                raise ValueError("citation refs 不能包含空 alias 列表")
            for alias in cleaned:
                if not re.fullmatch(r"c\d{3}", alias):
                    raise ValueError("citation ref alias 必须是 c 加三位数字")
            result[normalized_slug] = tuple(cleaned)
        return _FrozenMapping(result)


class DedupPageCandidate(_FrozenValueModel):
    slug: str
    title: str
    page_type: TopicPageType
    aliases: tuple[str, ...] = ()

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("entity", "concept"))

    @field_validator("title")
    @classmethod
    def normalize_title(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("页面标题不能为空")
        return value

    @field_validator("aliases")
    @classmethod
    def normalize_aliases(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_strict_clean_strings(list(value), "dedup aliases"))

    @model_validator(mode="after")
    def validate_page_type_prefix(self) -> Self:
        if not self.slug.startswith(f"{self.page_type}/"):
            raise ValueError("slug 前缀必须与 page_type 一致")
        return self


class DedupCandidateRequest(_FrozenValueModel):
    candidate: _FrozenTopicCandidate
    allowed_targets: tuple[DedupPageCandidate, ...] = Field(max_length=20)

    @field_validator("candidate", mode="before")
    @classmethod
    def snapshot_candidate(cls, value: object) -> object:
        return _snapshot_topic(value) if isinstance(value, TopicCandidate) else value

    @model_validator(mode="after")
    def validate_targets(self) -> Self:
        slugs = [target.slug for target in self.allowed_targets]
        if len(slugs) != len(set(slugs)):
            raise ValueError("dedup target slug 不能重复")
        if any(target.page_type != self.candidate.page_type for target in self.allowed_targets):
            raise ValueError("dedup target page_type 必须与候选一致")
        return self


class DedupRequest(_FrozenValueModel):
    candidates: tuple[DedupCandidateRequest, ...] = ()

    @model_validator(mode="after")
    def validate_candidate_slugs(self) -> Self:
        slugs = [item.candidate.slug for item in self.candidates]
        if len(slugs) != len(set(slugs)):
            raise ValueError("dedup generated candidate slug 不能重复")
        return self


class DedupDecision(_FrozenValueModel):
    candidate_slug: str
    canonical_slug: str | None = None

    @field_validator("candidate_slug", "canonical_slug")
    @classmethod
    def normalize_decision_slug(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _normalize_slug(value, ("entity", "concept"))


class DedupOutput(_FrozenValueModel):
    decisions: tuple[DedupDecision, ...] = ()

    @model_validator(mode="after")
    def validate_decision_slugs(self) -> Self:
        slugs = [decision.candidate_slug for decision in self.decisions]
        if len(slugs) != len(set(slugs)):
            raise ValueError("dedup decision candidate_slug 不能重复")
        return self


ContributionAction = Literal["add", "replace", "retract_stale", "retract"]
ContributionState = Literal["active", "retract_pending"]


class StoredContributionRecord(_FrozenValueModel):
    id: UUID | None = None
    tenant_id: int = Field(gt=0)
    knowledge_base_id: UUID
    slug: str
    knowledge_id: str
    op_version: str
    page_type: IngestPageType
    state: ContributionState
    title: str
    content: str
    summary: str
    aliases: tuple[str, ...] = ()
    chunk_refs: tuple[str, ...] = ()

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("summary", "entity", "concept"))

    @field_validator("knowledge_id", "op_version", "title")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("贡献身份字段不能为空")
        return value

    @field_validator("aliases", "chunk_refs")
    @classmethod
    def normalize_arrays(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_strict_clean_strings(list(value), "贡献数组"))

    @model_validator(mode="after")
    def validate_page_type_prefix(self) -> Self:
        if not self.slug.startswith(f"{self.page_type}/"):
            raise ValueError("slug 前缀必须与 page_type 一致")
        return self


class ContributionDelta(_FrozenValueModel):
    pending_op_id: UUID
    action: ContributionAction
    slug: str
    knowledge_id: str
    previous: StoredContributionRecord | None
    current: StoredContributionRecord | None

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("summary", "entity", "concept"))

    @field_validator("knowledge_id")
    @classmethod
    def validate_knowledge_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("知识标识不能为空")
        return value

    @model_validator(mode="after")
    def validate_action_contract(self) -> Self:
        expected = {
            "add": (False, True, None),
            "replace": (True, True, "active"),
            "retract_stale": (True, False, "active"),
            "retract": (True, False, "retract_pending"),
        }[self.action]
        require_previous, require_current, previous_state = expected
        if (self.previous is not None) != require_previous or (self.current is not None) != require_current:
            raise ValueError("contribution action 的 previous/current 合同不成立")
        if self.current is not None and self.current.state != "active":
            raise ValueError("current contribution 必须为 active")
        if self.previous is not None and previous_state is not None and self.previous.state != previous_state:
            raise ValueError("previous contribution state 与 action 不一致")
        records = [record for record in (self.previous, self.current) if record is not None]
        if any(record.slug != self.slug or record.knowledge_id != self.knowledge_id for record in records):
            raise ValueError("contribution 的 slug 和 knowledge_id 必须与 delta 一致")
        if len(records) == 2 and (records[0].tenant_id, records[0].knowledge_base_id, records[0].page_type) != (records[1].tenant_id, records[1].knowledge_base_id, records[1].page_type):
            raise ValueError("previous/current 的 scope 和 page_type 必须一致")
        return self


class OperationFailure(_FrozenValueModel):
    pending_op_id: UUID
    error_code: str
    error_summary: str

    @field_validator("error_code")
    @classmethod
    def validate_error_code(cls, value: str) -> str:
        value = value.strip()
        if not 1 <= len(value) <= 128:
            raise ValueError("error_code 长度必须在 1 到 128 之间")
        return value

    @field_validator("error_summary")
    @classmethod
    def normalize_error_summary(cls, value: str) -> str:
        value = " ".join(value.split())
        if not 1 <= len(value) <= 2000:
            raise ValueError("error_summary 长度必须在 1 到 2000 之间")
        return value


class PageExpectation(_FrozenValueModel):
    slug: str
    page_id: UUID | None = None
    version: int | None = Field(default=None, ge=1)

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("summary", "entity", "concept"))

    @model_validator(mode="after")
    def validate_page_pair(self) -> Self:
        if (self.page_id is None) != (self.version is None):
            raise ValueError("page_id 与 version 必须同时存在或同时为空")
        return self


class BatchApplyRequest(_FrozenValueModel):
    claim_token: UUID
    pages: tuple[_FrozenReducedPage, ...]
    contribution_deltas: tuple[ContributionDelta, ...]
    completed_op_ids: tuple[UUID, ...]
    superseded_op_ids: tuple[UUID, ...]
    failures: tuple[OperationFailure, ...]
    expected_pages: tuple[PageExpectation, ...]
    operation_id: UUID

    @field_validator("pages", mode="before")
    @classmethod
    def snapshot_pages(cls, value: object) -> object:
        return [_snapshot_page(item) for item in value] if isinstance(value, (list, tuple)) else value

    @model_validator(mode="after")
    def validate_batch_identities(self) -> Self:
        groups = [self.completed_op_ids, self.superseded_op_ids, [failure.pending_op_id for failure in self.failures]]
        if any(len(group) != len(set(group)) for group in groups):
            raise ValueError("batch operation id 不能重复")
        if len(set().union(*[set(group) for group in groups])) != sum(len(group) for group in groups):
            raise ValueError("completed、superseded 与 failure operation id 不能重叠")
        page_slugs = [page.slug for page in self.pages]
        expected_slugs = [page.slug for page in self.expected_pages]
        if len(page_slugs) != len(set(page_slugs)):
            raise ValueError("pages slug 不能重复")
        if len(expected_slugs) != len(set(expected_slugs)):
            raise ValueError("expected_pages slug 不能重复")
        return self


class BatchResult(_StrictModel):
    completed_op_ids: list[UUID] = Field(default_factory=list)
    failed_op_ids: list[UUID] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_ids(self) -> Self:
        if len(self.completed_op_ids) != len(set(self.completed_op_ids)):
            raise ValueError("completed_op_ids 不能重复")
        if len(self.failed_op_ids) != len(set(self.failed_op_ids)):
            raise ValueError("failed_op_ids 不能重复")
        if set(self.completed_op_ids) & set(self.failed_op_ids):
            raise ValueError("completed_op_ids 与 failed_op_ids 不能重叠")
        return self

    @classmethod
    def from_ids(
        cls,
        pending_op_ids: Iterable[UUID],
        failed_op_ids: Iterable[UUID],
    ) -> Self:
        pending = list(dict.fromkeys(pending_op_ids))
        failed = list(dict.fromkeys(failed_op_ids))
        pending_set = set(pending)
        unknown_failed = [op_id for op_id in failed if op_id not in pending_set]
        if unknown_failed:
            raise ValueError("failed_op_ids 必须是 pending_op_ids 的子集")
        failed_set = set(failed)
        return cls(
            completed_op_ids=[op_id for op_id in pending if op_id not in failed_set],
            failed_op_ids=failed,
        )

    @property
    def completed_ops(self) -> int:
        return len(self.completed_op_ids)

    @property
    def failed_ops(self) -> int:
        return len(self.failed_op_ids)


class FinalizationRequest(_StrictModel):
    tenant_id: int
    knowledge_base_id: UUID
    knowledge_id: str
    attempt: str
    subtask_name: str = "wiki"

    @field_validator("knowledge_id", "attempt", "subtask_name")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("finalization 标识不能为空")
        return value

    @classmethod
    def from_knowledge(cls, scope: WikiScope, knowledge: SourceKnowledge) -> Self:
        if knowledge.tenant_id != scope.tenant_id:
            raise ValueError("知识条目租户与 WikiScope 不一致")
        if knowledge.knowledge_base_id != scope.knowledge_base_id:
            raise ValueError("知识条目知识库与 WikiScope 不一致")
        return cls(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            knowledge_id=knowledge.id,
            attempt=knowledge.op_version,
        )


class EnqueueResult(_StrictModel):
    pending_op_id: UUID | None = None
    skipped_reason: str | None = None
    deduplicated: bool = False
