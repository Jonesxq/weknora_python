"""Wiki 摄取阶段使用的结构化 DTO。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import copy
import math
import os
import re
from types import MappingProxyType
from typing import Any, Literal, Self
import unicodedata
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from app.wiki.scope import WikiScope


ExtractionGranularity = Literal["focused", "standard", "exhaustive"]
TopicPageType = Literal["entity", "concept"]
IngestPageType = Literal["summary", "entity", "concept"]
IndexIntroMode = Literal["create", "update"]
IndexModelStatus = Literal["generated", "defaulted", "kept_after_error"]

_SLUG_PATTERN = re.compile(
    r"^(summary|entity|concept)/[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*$"
)


class _StrictModel(BaseModel):
    """所有摄取 DTO 的共同边界：拒绝未知字段并允许测试时安全修改副本。"""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class _FrozenValueModel(BaseModel):
    """阶段三跨字段 DTO 的不可变边界。"""

    model_config = ConfigDict(extra="forbid", frozen=True)


class _TaxonomyValueModel(_FrozenValueModel):
    """对 taxonomy DTO 的 model_copy update 重新执行完整验证。"""

    def model_copy(
        self, *, update: Mapping[str, Any] | None = None, deep: bool = False
    ) -> Self:
        if not update:
            return super().model_copy(deep=deep)

        source = super().model_copy(deep=deep)
        payload = {
            field_name: getattr(source, field_name)
            for field_name in type(self).model_fields
        }
        payload.update(update)
        validated = type(self).model_validate(payload)
        object.__setattr__(
            validated,
            "__pydantic_fields_set__",
            source.model_fields_set | set(update),
        )
        private_state = getattr(source, "__pydantic_private__", None)
        if private_state is not None:
            object.__setattr__(validated, "__pydantic_private__", private_state)
        return validated


class _FrozenMapping(Mapping[str, tuple[str, ...]]):
    """可深拷贝且不继承 dict 的只读映射。"""

    __slots__ = ("_items", "_lookup")

    def __init__(
        self,
        values: Mapping[str, tuple[str, ...]] | Iterable[tuple[str, tuple[str, ...]]],
    ) -> None:
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

    def __reduce__(
        self,
    ) -> tuple[type[Self], tuple[tuple[tuple[str, tuple[str, ...]], ...]]]:
        return type(self), (self._items,)

    def __deepcopy__(self, memo: dict[int, object]) -> Self:
        return type(self)(copy.deepcopy(self._items, memo))


class _FrozenVectorMapping(Mapping[str, tuple[float, ...]]):
    """可深拷贝且可序列化的只读嵌入向量映射。"""

    __slots__ = ("_items", "_lookup")

    def __init__(
        self,
        values: (
            Mapping[str, tuple[float, ...]] | Iterable[tuple[str, tuple[float, ...]]]
        ),
    ) -> None:
        source = values.items() if isinstance(values, Mapping) else values
        items = tuple((key, tuple(value)) for key, value in source)
        object.__setattr__(self, "_items", items)
        object.__setattr__(self, "_lookup", MappingProxyType(dict(items)))

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("embedding 向量映射不可修改")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("embedding 向量映射不可修改")

    def __getitem__(self, key: str) -> tuple[float, ...]:
        return self._lookup[key]

    def __iter__(self):  # type: ignore[no-untyped-def]
        return (key for key, _ in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def _iter_pairs(self) -> Iterable[tuple[str, tuple[float, ...]]]:
        return iter(self._items)

    def __copy__(self) -> Self:
        return self

    def __reduce__(
        self,
    ) -> tuple[type[Self], tuple[tuple[tuple[str, tuple[float, ...]], ...]]]:
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


def _folder_name(value: str) -> str:
    if any(unicodedata.category(character).startswith("C") for character in value):
        raise ValueError("目录名不能包含控制字符")
    name = value.strip()
    if not name or len(name) > 512:
        raise ValueError("目录名长度必须在 1 到 512 之间")
    if name in {".", ".."} or "/" in name or "\\" in name:
        raise ValueError("目录名不能包含路径分隔符或保留路径名")
    return name


def _normalize_folder_path(value: str) -> str:
    path = value
    if not 1 <= len(path) <= 2048:
        raise ValueError("目录 path 长度必须在 1 到 2048 之间")
    if not path.startswith("/") or path.endswith("/"):
        raise ValueError("目录 path 必须以 / 开头且不能以 / 结尾")
    segments = path[1:].split("/")
    if not segments or any(not segment for segment in segments):
        raise ValueError("目录 path 不能包含空路径段")
    for segment in segments:
        if _folder_name(segment) != segment:
            raise ValueError("目录 path 段必须是原样规范的目录名")
    return "/" + "/".join(segments)


def _folder_path_segments(path: str) -> tuple[str, ...]:
    return tuple(path[1:].split("/"))


def _normalize_embedding_key(value: str) -> str:
    key = value.strip()
    if not 1 <= len(key) <= 512 or "," in key:
        raise ValueError("embedding key 长度必须在 1 到 512 之间且不能包含逗号")
    return key


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
    taxonomy_topic_batch_size: int = Field(default=60, ge=1, le=60)
    taxonomy_parallel: int = Field(default=4, ge=1, le=16)
    taxonomy_full_catalog_limit: int = Field(default=120, ge=1, le=5000)
    taxonomy_related_folder_limit: int = Field(default=40, ge=1, le=500)

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
    pending_op_id: UUID | None
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


class _ReadOnlyStrings(tuple):
    """保留阶段二 list 比较语义的只读字符串序列。"""

    def __new__(cls, values: Iterable[str] = ()) -> Self:
        return super().__new__(cls, values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (list, tuple)):
            return tuple.__eq__(self, tuple(other))
        return False

    __hash__ = tuple.__hash__


class _FrozenSlugUpdate(SlugUpdate):
    """兼容 Reduce 类型检查的深度只读 SlugUpdate 快照。"""

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    aliases: _ReadOnlyStrings = Field(default_factory=_ReadOnlyStrings)
    source_refs: _ReadOnlyStrings = Field(default_factory=_ReadOnlyStrings)
    chunk_refs: _ReadOnlyStrings = Field(default_factory=_ReadOnlyStrings)

    @field_validator("aliases", "source_refs", "chunk_refs", mode="before")
    @classmethod
    def snapshot_strings(cls, value: object) -> _ReadOnlyStrings:
        if not isinstance(value, (list, tuple)):
            raise ValueError("兼容 update 的引用字段必须是字符串序列")
        if any(not isinstance(item, str) for item in value):
            raise ValueError("兼容 update 的引用字段只能包含字符串")
        return _ReadOnlyStrings(value)


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
    deleted: bool = False

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
        if self.deleted and (self.source_refs or self.chunk_refs):
            raise ValueError("删除页面的 source_refs 和 chunk_refs 必须为空")
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
    deleted: bool = False

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
        if self.deleted and (self.source_refs or self.chunk_refs):
            raise ValueError("删除页面的 source_refs 和 chunk_refs 必须为空")
        return self


def _snapshot_topic(
    value: TopicCandidate | _FrozenTopicCandidate | dict[str, Any],
) -> object:
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
        return (
            [_snapshot_topic(item) for item in value]
            if isinstance(value, (list, tuple))
            else value
        )

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
        return (
            [_snapshot_topic(item) for item in value]
            if isinstance(value, (list, tuple))
            else value
        )

    @field_serializer("refs_by_slug")
    def serialize_refs_by_slug(
        self, value: Mapping[str, tuple[str, ...]]
    ) -> dict[str, tuple[str, ...]]:
        if isinstance(value, _FrozenMapping):
            return dict(value._iter_pairs())
        return dict(value)

    @field_validator("refs_by_slug")
    @classmethod
    def normalize_refs(
        cls, value: Mapping[str, tuple[str, ...]]
    ) -> Mapping[str, tuple[str, ...]]:
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
        if any(
            target.page_type != self.candidate.page_type
            for target in self.allowed_targets
        ):
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


class FolderCatalogEntry(_TaxonomyValueModel):
    id: UUID
    parent_id: UUID | None = None
    name: str
    path: str
    depth: int = Field(ge=1, le=3)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        return _folder_name(value)

    @field_validator("path")
    @classmethod
    def normalize_path(cls, value: str) -> str:
        return _normalize_folder_path(value)

    @model_validator(mode="after")
    def validate_catalog_identity(self) -> Self:
        segments = _folder_path_segments(self.path)
        if len(segments) != self.depth:
            raise ValueError("目录 path 段数必须与 depth 一致")
        if segments[-1] != self.name:
            raise ValueError("目录 path 末段必须与 name 一致")
        if self.parent_id == self.id:
            raise ValueError("目录 parent_id 不能指向自身")
        return self


class TaxonomyContext(_TaxonomyValueModel):
    folders: tuple[FolderCatalogEntry, ...] = ()
    classifiable_slugs: tuple[str, ...] = ()

    @field_validator("classifiable_slugs")
    @classmethod
    def normalize_classifiable_slugs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_normalize_slug(slug, ("entity", "concept")) for slug in value)

    @model_validator(mode="after")
    def validate_catalog_tree(self) -> Self:
        folder_ids = [folder.id for folder in self.folders]
        paths = [folder.path for folder in self.folders]
        if len(folder_ids) != len(set(folder_ids)):
            raise ValueError("taxonomy folder id 不能重复")
        if len(paths) != len(set(paths)):
            raise ValueError("taxonomy folder path 不能重复")
        if len(self.classifiable_slugs) != len(set(self.classifiable_slugs)):
            raise ValueError("classifiable slug 不能重复")

        by_id = {folder.id: folder for folder in self.folders}
        by_path = {folder.path: folder for folder in self.folders}
        for folder in self.folders:
            if folder.depth == 1:
                if folder.parent_id is not None:
                    raise ValueError("一级目录 parent_id 必须为空")
                continue
            parent_path = "/" + "/".join(_folder_path_segments(folder.path)[:-1])
            parent = by_path.get(parent_path)
            if parent is None or parent.depth != folder.depth - 1:
                raise ValueError("二三级目录必须包含完整且一致的祖先链")
            if folder.parent_id != parent.id or by_id.get(folder.parent_id) != parent:
                raise ValueError("目录 parent_id 必须与 path 祖先一致")
        return self


class EmbeddingItem(_TaxonomyValueModel):
    key: str
    text: str

    @field_validator("key")
    @classmethod
    def normalize_key(cls, value: str) -> str:
        return _normalize_embedding_key(value)

    @field_validator("text")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        text = value.strip()
        if not 1 <= len(text) <= 8000:
            raise ValueError("embedding text 长度必须在 1 到 8000 之间")
        return text


class EmbeddingRequest(_TaxonomyValueModel):
    items: tuple[EmbeddingItem, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_keys(self) -> Self:
        keys = [item.key for item in self.items]
        if len(keys) != len(set(keys)):
            raise ValueError("embedding item key 不能重复")
        return self


class EmbeddingOutput(_TaxonomyValueModel):
    vectors: Mapping[str, tuple[float, ...]]

    @field_serializer("vectors")
    def serialize_vectors(
        self, value: Mapping[str, tuple[float, ...]]
    ) -> dict[str, tuple[float, ...]]:
        if isinstance(value, _FrozenVectorMapping):
            return dict(value._iter_pairs())
        return dict(value)

    @field_validator("vectors")
    @classmethod
    def normalize_vectors(
        cls, value: Mapping[object, object]
    ) -> Mapping[str, tuple[float, ...]]:
        if not value:
            raise ValueError("embedding vectors 不能为空")
        vectors: dict[str, tuple[float, ...]] = {}
        dimension: int | None = None
        for raw_key, raw_vector in value.items():
            key = _normalize_embedding_key(str(raw_key))
            if key in vectors:
                raise ValueError("embedding vector key 不能重复")
            if isinstance(raw_vector, (str, bytes, Mapping)):
                raise ValueError("embedding vector 必须是数值序列")
            try:
                vector = tuple(float(component) for component in raw_vector)  # type: ignore[union-attr]
            except (TypeError, ValueError) as exc:
                raise ValueError("embedding vector 必须是数值序列") from exc
            if not vector or not all(math.isfinite(component) for component in vector):
                raise ValueError("embedding vector 必须非空且全部为有限数")
            if dimension is None:
                dimension = len(vector)
            elif len(vector) != dimension:
                raise ValueError("embedding vector 维度必须一致")
            vectors[key] = vector
        return _FrozenVectorMapping(vectors)


class TaxonomyTopic(_TaxonomyValueModel):
    slug: str
    title: str
    page_type: TopicPageType
    summary: str = ""

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("entity", "concept"))

    @field_validator("title")
    @classmethod
    def normalize_title(cls, value: str) -> str:
        title = " ".join(value.split())
        if not 1 <= len(title) <= 512:
            raise ValueError("taxonomy title 长度必须在 1 到 512 之间")
        return title

    @field_validator("summary")
    @classmethod
    def normalize_summary(cls, value: str) -> str:
        summary = value.strip()
        if len(summary) > 4000:
            raise ValueError("taxonomy summary 长度不能超过 4000")
        return summary

    @model_validator(mode="after")
    def validate_page_type_prefix(self) -> Self:
        if not self.slug.startswith(f"{self.page_type}/"):
            raise ValueError("slug 前缀必须与 page_type 一致")
        return self


class AllowedFolderBase(_TaxonomyValueModel):
    id: UUID
    path: str
    depth: int = Field(ge=1, le=3)

    @field_validator("path")
    @classmethod
    def normalize_path(cls, value: str) -> str:
        return _normalize_folder_path(value)

    @model_validator(mode="after")
    def validate_path_depth(self) -> Self:
        if len(_folder_path_segments(self.path)) != self.depth:
            raise ValueError("目录 path 段数必须与 depth 一致")
        return self


class TaxonomyRequest(_TaxonomyValueModel):
    topics: tuple[TaxonomyTopic, ...] = Field(min_length=1, max_length=60)
    allowed_bases: tuple[AllowedFolderBase, ...] = ()

    @model_validator(mode="after")
    def validate_request_identities(self) -> Self:
        topic_slugs = [topic.slug for topic in self.topics]
        ids = [base.id for base in self.allowed_bases]
        paths = [base.path for base in self.allowed_bases]
        if len(topic_slugs) != len(set(topic_slugs)):
            raise ValueError("taxonomy topic slug 不能重复")
        if len(ids) != len(set(ids)) or len(paths) != len(set(paths)):
            raise ValueError("allowed base id 和 path 不能重复")
        return self


class TaxonomyDecision(_TaxonomyValueModel):
    slug: str
    base_folder_id: UUID | None = None
    new_segments: tuple[str, ...] = Field(default=(), max_length=2)

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("entity", "concept"))

    @field_validator("new_segments")
    @classmethod
    def normalize_segments(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_folder_name(segment) for segment in value)

    @model_validator(mode="after")
    def validate_segment_adjacency(self) -> Self:
        if any(
            current.casefold() == following.casefold()
            for current, following in zip(self.new_segments, self.new_segments[1:])
        ):
            raise ValueError("相邻目录段不能仅大小写不同")
        return self


class TaxonomyOutput(_TaxonomyValueModel):
    decisions: tuple[TaxonomyDecision, ...] = ()

    @model_validator(mode="after")
    def validate_decision_slugs(self) -> Self:
        slugs = [decision.slug for decision in self.decisions]
        if len(slugs) != len(set(slugs)):
            raise ValueError("taxonomy decision slug 不能重复")
        return self


class FolderAssignment(_TaxonomyValueModel):
    slug: str
    contributor_op_ids: tuple[UUID, ...] = Field(min_length=1)
    base_folder_id: UUID | None = None
    base_path: str | None = None
    base_depth: int = Field(default=0, ge=0, le=3)
    new_segments: tuple[str, ...] = Field(default=(), max_length=2)

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("entity", "concept"))

    @field_validator("base_path")
    @classmethod
    def normalize_base_path(cls, value: str | None) -> str | None:
        return None if value is None else _normalize_folder_path(value) if value else ""

    @field_validator("new_segments")
    @classmethod
    def normalize_segments(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_folder_name(segment) for segment in value)

    @model_validator(mode="after")
    def validate_assignment(self) -> Self:
        if len(self.contributor_op_ids) != len(set(self.contributor_op_ids)):
            raise ValueError("folder assignment contributor op id 不能重复")
        root = self.base_folder_id is None
        if root and (self.base_path is not None or self.base_depth != 0):
            raise ValueError("根目录必须使用 None base_path 和 base_depth=0")
        if not root:
            if not self.base_path or self.base_depth == 0:
                raise ValueError("既有目录必须同时提供 base id、path 和 depth")
            if len(_folder_path_segments(self.base_path)) != self.base_depth:
                raise ValueError("base path 段数必须与 base depth 一致")
        if self.base_depth + len(self.new_segments) > 3:
            raise ValueError("目录总深度不能超过 3")
        derived_segments = (
            [*_folder_path_segments(self.base_path)[-1:], *self.new_segments]
            if self.base_path
            else list(self.new_segments)
        )
        if any(
            current.casefold() == following.casefold()
            for current, following in zip(derived_segments, derived_segments[1:])
        ):
            raise ValueError("相邻目录段不能仅大小写不同")
        if len(self.folder_path) > 2048:
            raise ValueError("目录 path 长度不能超过 2048")
        if len(self.wiki_path) > 1024:
            raise ValueError("最终 wiki_path 长度不能超过 1024")
        return self

    @property
    def folder_path(self) -> str:
        segments = [
            *(_folder_path_segments(self.base_path) if self.base_path else ()),
            *self.new_segments,
        ]
        return "/" + "/".join(segments) if segments else ""

    @property
    def wiki_path(self) -> str:
        return (
            f"{self.folder_path}/{self.slug}" if self.folder_path else f"/{self.slug}"
        )


class _FolderAssignmentsPayload(_TaxonomyValueModel):
    items: tuple[FolderAssignment, ...]

    @model_validator(mode="after")
    def validate_unique_slugs(self) -> Self:
        slugs = [assignment.slug for assignment in self.items]
        if len(slugs) != len(set(slugs)):
            raise ValueError("folder assignment slug 不能重复")
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
        if (self.previous is not None) != require_previous or (
            self.current is not None
        ) != require_current:
            raise ValueError("contribution action 的 previous/current 合同不成立")
        if self.current is not None and self.current.state != "active":
            raise ValueError("current contribution 必须为 active")
        if (
            self.previous is not None
            and previous_state is not None
            and self.previous.state != previous_state
        ):
            raise ValueError("previous contribution state 与 action 不一致")
        records = [
            record for record in (self.previous, self.current) if record is not None
        ]
        if any(
            record.slug != self.slug or record.knowledge_id != self.knowledge_id
            for record in records
        ):
            raise ValueError("contribution 的 slug 和 knowledge_id 必须与 delta 一致")
        if len(records) == 2 and (
            records[0].tenant_id,
            records[0].knowledge_base_id,
            records[0].page_type,
        ) != (records[1].tenant_id, records[1].knowledge_base_id, records[1].page_type):
            raise ValueError("previous/current 的 scope 和 page_type 必须一致")
        return self


class _LegacyUpdates(tuple):
    """阶段二 Worker 的只读更新视图。"""

    def __new__(cls, values: Iterable[SlugUpdate] = ()) -> Self:
        return super().__new__(cls, values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (list, tuple)):
            return tuple.__eq__(self, tuple(other))
        return False

    __hash__ = tuple.__hash__


class MapDocumentResult(_FrozenValueModel):
    pending_op_id: UUID
    knowledge_id: str
    contribution_deltas: tuple[ContributionDelta, ...] = ()
    skipped_reason: str | None = None
    superseded: bool = False

    @property
    def updates(self) -> _LegacyUpdates:
        """任务 11 切换 Worker 后移除的只读派生视图。"""

        updates: list[_FrozenSlugUpdate] = []
        for raw_delta in self.contribution_deltas:
            delta = (
                raw_delta
                if isinstance(raw_delta, ContributionDelta)
                else ContributionDelta.model_validate(raw_delta)
            )
            current = delta.current
            if current is None:
                continue
            updates.append(
                _FrozenSlugUpdate(
                    pending_op_id=delta.pending_op_id,
                    knowledge_id=current.knowledge_id,
                    slug=current.slug,
                    title=current.title,
                    page_type=current.page_type,
                    content=current.content,
                    summary=current.summary,
                    aliases=current.aliases,
                    source_refs=(current.knowledge_id,),
                    chunk_refs=current.chunk_refs,
                )
            )
        return _LegacyUpdates(updates)

    @field_validator("knowledge_id")
    @classmethod
    def normalize_knowledge_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("知识标识不能为空")
        return value

    @model_validator(mode="after")
    def validate_terminal_state(self) -> Self:
        if self.skipped_reason is not None and self.superseded:
            raise ValueError("skipped_reason 与 superseded 不能同时设置")
        if (
            self.skipped_reason is not None or self.superseded
        ) and self.contribution_deltas:
            raise ValueError("跳过或 superseded 结果不能包含贡献差量")
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


class IndexSummaryItem(_TaxonomyValueModel):
    slug: str
    title: str
    summary: str

    @field_validator("slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        return _normalize_slug(value, ("summary",))

    @field_validator("title")
    @classmethod
    def normalize_title(cls, value: str) -> str:
        value = value.strip()
        if not 1 <= len(value) <= 512:
            raise ValueError("索引摘要标题长度必须在 1 到 512 之间")
        return value

    @field_validator("summary")
    @classmethod
    def normalize_summary(cls, value: str) -> str:
        value = value.strip()
        if len(value) > 4000:
            raise ValueError("索引摘要长度不能超过 4000")
        return value


class IndexPageSnapshot(_TaxonomyValueModel):
    id: UUID
    version: int = Field(ge=1)
    content: str
    summary: str

    @field_validator("content", "summary")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        value = value.strip()
        if len(value) > 4000:
            raise ValueError("索引页面文本长度不能超过 4000")
        return value


class IndexIntroContext(_TaxonomyValueModel):
    index: IndexPageSnapshot | None = None
    recent_summaries: tuple[IndexSummaryItem, ...] = Field(default=(), max_length=200)

    @model_validator(mode="after")
    def validate_recent_summary_slugs(self) -> Self:
        slugs = [item.slug for item in self.recent_summaries]
        if len(slugs) != len(set(slugs)):
            raise ValueError("recent_summaries slug 不能重复")
        return self


class IndexIntroChange(_TaxonomyValueModel):
    action: Literal["ingest", "retract"]
    knowledge_id: str
    pages: tuple[IndexSummaryItem, ...] = ()

    @field_validator("knowledge_id")
    @classmethod
    def normalize_knowledge_id(cls, value: str) -> str:
        value = value.strip()
        if not 1 <= len(value) <= 512:
            raise ValueError("知识标识长度必须在 1 到 512 之间")
        return value

    @model_validator(mode="after")
    def validate_page_slugs(self) -> Self:
        slugs = [page.slug for page in self.pages]
        if len(slugs) != len(set(slugs)):
            raise ValueError("index intro change pages slug 不能重复")
        return self


class IndexIntroRequest(_TaxonomyValueModel):
    mode: IndexIntroMode
    existing_intro: str = ""
    summaries: tuple[IndexSummaryItem, ...] = Field(default=(), max_length=200)
    changes: tuple[IndexIntroChange, ...] = ()

    @field_validator("existing_intro")
    @classmethod
    def normalize_existing_intro(cls, value: str) -> str:
        value = value.strip()
        if len(value) > 4000:
            raise ValueError("existing_intro 长度不能超过 4000")
        return value

    @model_validator(mode="after")
    def validate_payload_contract(self) -> Self:
        summary_slugs = [summary.slug for summary in self.summaries]
        changes = [(change.action, change.knowledge_id) for change in self.changes]
        if len(summary_slugs) != len(set(summary_slugs)):
            raise ValueError("summaries slug 不能重复")
        if len(changes) != len(set(changes)):
            raise ValueError("changes action 和 knowledge_id 不能重复")
        if self.mode == "create":
            if self.existing_intro or self.changes:
                raise ValueError("create index intro 只能包含 summaries")
        elif not self.existing_intro or self.summaries or not self.changes:
            raise ValueError("update index intro 必须包含 existing_intro 和 changes")
        return self


class IndexIntroOutput(_TaxonomyValueModel):
    intro: str

    @field_validator("intro")
    @classmethod
    def normalize_intro(cls, value: str) -> str:
        value = value.strip()
        if not 1 <= len(value) <= 4000:
            raise ValueError("index intro 长度必须在 1 到 4000 之间")
        return value


class IndexIntroPlan(_TaxonomyValueModel):
    mode: IndexIntroMode
    expected_page_id: UUID | None = None
    expected_version: int | None = Field(default=None, ge=1)
    intro: str
    model_status: IndexModelStatus
    error_code: str | None = None

    @field_validator("intro")
    @classmethod
    def normalize_intro(cls, value: str) -> str:
        value = value.strip()
        if not 1 <= len(value) <= 4000:
            raise ValueError("index intro 长度必须在 1 到 4000 之间")
        return value

    @field_validator("error_code")
    @classmethod
    def normalize_error_code(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not 1 <= len(value) <= 128:
            raise ValueError("error_code 长度必须在 1 到 128 之间")
        return value

    @model_validator(mode="after")
    def validate_plan_contract(self) -> Self:
        has_page = self.expected_page_id is not None
        if has_page != (self.expected_version is not None):
            raise ValueError(
                "expected_page_id 与 expected_version 必须同时存在或同时为空"
            )
        if self.mode == "create" and has_page:
            raise ValueError("create index intro 不能提供 expected page")
        if self.mode == "update" and not has_page:
            raise ValueError("update index intro 必须提供 expected page")
        if self.model_status == "generated" and self.error_code is not None:
            raise ValueError("generated index intro 不能提供 error_code")
        if (
            self.model_status in {"defaulted", "kept_after_error"}
            and self.error_code is None
        ):
            raise ValueError("回退 index intro 必须提供 error_code")
        if self.model_status == "defaulted" and self.mode != "create":
            raise ValueError("defaulted index intro 只允许 create")
        if self.model_status == "kept_after_error" and self.mode != "update":
            raise ValueError("kept_after_error index intro 只允许 update")
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
    folder_assignments: tuple[FolderAssignment, ...] = ()
    index_intro_plan: IndexIntroPlan | None = None

    def model_copy(
        self, *, update: Mapping[str, Any] | None = None, deep: bool = False
    ) -> Self:
        if not update or not {"folder_assignments", "index_intro_plan"}.intersection(
            update
        ):
            return super().model_copy(update=update, deep=deep)
        source = super().model_copy(deep=deep)
        payload = {
            field_name: getattr(source, field_name)
            for field_name in type(self).model_fields
        }
        payload.update(update)
        validated = type(self).model_validate(payload)
        object.__setattr__(
            validated,
            "__pydantic_fields_set__",
            source.model_fields_set | set(update),
        )
        private_state = getattr(source, "__pydantic_private__", None)
        if private_state is not None:
            object.__setattr__(validated, "__pydantic_private__", private_state)
        return validated

    @field_validator("pages", mode="before")
    @classmethod
    def snapshot_pages(cls, value: object) -> object:
        return (
            [_snapshot_page(item) for item in value]
            if isinstance(value, (list, tuple))
            else value
        )

    @model_validator(mode="after")
    def validate_batch_identities(self) -> Self:
        groups = [
            self.completed_op_ids,
            self.superseded_op_ids,
            [failure.pending_op_id for failure in self.failures],
        ]
        if any(len(group) != len(set(group)) for group in groups):
            raise ValueError("batch operation id 不能重复")
        if len(set().union(*[set(group) for group in groups])) != sum(
            len(group) for group in groups
        ):
            raise ValueError("completed、superseded 与 failure operation id 不能重叠")
        page_slugs = [page.slug for page in self.pages]
        expected_slugs = [page.slug for page in self.expected_pages]
        if len(page_slugs) != len(set(page_slugs)):
            raise ValueError("pages slug 不能重复")
        if len(expected_slugs) != len(set(expected_slugs)):
            raise ValueError("expected_pages slug 不能重复")
        _FolderAssignmentsPayload(items=self.folder_assignments)
        if self.index_intro_plan is not None and not self.completed_op_ids:
            raise ValueError("index intro plan 至少需要一个 completed operation")
        return self


class BatchApplyOutcome(_FrozenValueModel):
    applied: bool
    completed_op_ids: tuple[UUID, ...] = ()
    superseded_op_ids: tuple[UUID, ...] = ()
    failed_op_ids: tuple[UUID, ...] = ()

    @model_validator(mode="after")
    def validate_operation_ids(self) -> Self:
        groups = (
            self.completed_op_ids,
            self.superseded_op_ids,
            self.failed_op_ids,
        )
        if any(len(group) != len(set(group)) for group in groups):
            raise ValueError("apply outcome operation id 不能重复")
        if len(set().union(*(set(group) for group in groups))) != sum(
            len(group) for group in groups
        ):
            raise ValueError("completed、superseded 与 failed operation id 不能重叠")
        return self


class _OperationIds(tuple):
    """不可变 ID 序列，同时兼容阶段二测试中的 list 比较。"""

    def __new__(cls, values: Iterable[UUID] = ()) -> Self:
        return super().__new__(cls, values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (list, tuple)):
            return tuple.__eq__(self, tuple(other))
        return False

    __hash__ = tuple.__hash__


class BatchResult(_FrozenValueModel):
    completed_op_ids: tuple[UUID, ...] = ()
    failed_op_ids: tuple[UUID, ...] = ()
    superseded_op_ids: tuple[UUID, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )

    def __setattr__(self, name: str, value: object) -> None:
        if name in {"completed_ops", "failed_ops", "superseded_ops"}:
            raise AttributeError(f"{name} 是只读派生属性")
        super().__setattr__(name, value)

    @model_validator(mode="after")
    def validate_ids(self) -> Self:
        groups = (
            self.completed_op_ids,
            self.failed_op_ids,
            self.superseded_op_ids,
        )
        if any(len(group) != len(set(group)) for group in groups):
            raise ValueError("BatchResult operation id 不能重复")
        if len(set().union(*(set(group) for group in groups))) != sum(
            len(group) for group in groups
        ):
            raise ValueError("completed、failed 与 superseded operation id 不能重叠")
        object.__setattr__(
            self, "completed_op_ids", _OperationIds(self.completed_op_ids)
        )
        object.__setattr__(self, "failed_op_ids", _OperationIds(self.failed_op_ids))
        object.__setattr__(
            self, "superseded_op_ids", _OperationIds(self.superseded_op_ids)
        )
        return self

    @classmethod
    def from_ids(
        cls,
        pending_op_ids: Iterable[UUID],
        failed_op_ids: Iterable[UUID],
        superseded_op_ids: Iterable[UUID] = (),
    ) -> Self:
        pending = list(dict.fromkeys(pending_op_ids))
        failed = list(dict.fromkeys(failed_op_ids))
        superseded = list(dict.fromkeys(superseded_op_ids))
        pending_set = set(pending)
        unknown = [
            op_id for op_id in (*failed, *superseded) if op_id not in pending_set
        ]
        if unknown:
            raise ValueError("failed/superseded op ids 必须是 pending_op_ids 的子集")
        failed_set = set(failed)
        superseded_set = set(superseded)
        return cls(
            completed_op_ids=tuple(
                op_id
                for op_id in pending
                if op_id not in failed_set and op_id not in superseded_set
            ),
            failed_op_ids=tuple(failed),
            superseded_op_ids=tuple(superseded),
        )

    @property
    def completed_ops(self) -> int:
        return len(self.completed_op_ids)

    @property
    def failed_ops(self) -> int:
        return len(self.failed_op_ids)

    @property
    def superseded_ops(self) -> int:
        return len(self.superseded_op_ids)


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
