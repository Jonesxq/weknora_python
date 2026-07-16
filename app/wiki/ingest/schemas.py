"""Wiki 摄取阶段使用的结构化 DTO。"""

from __future__ import annotations

from collections.abc import Iterable
import os
import re
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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
