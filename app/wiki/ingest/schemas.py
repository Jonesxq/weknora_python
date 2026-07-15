"""Wiki 摄取阶段使用的结构化 DTO。"""

from __future__ import annotations

import os
import re
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

from app.wiki.scope import WikiScope


ExtractionGranularity = Literal["focused", "standard", "exhaustive"]
TopicPageType = Literal["entity", "concept"]
IngestPageType = Literal["summary", "entity", "concept"]

_SLUG_PATTERN = re.compile(r"[a-z0-9][a-z0-9/_-]*\Z")


def _normalize_slug(value: str, allowed_prefixes: tuple[str, ...]) -> str:
    slug = value.strip().casefold()
    if not _SLUG_PATTERN.fullmatch(slug):
        raise ValueError("slug 只能包含小写 ASCII 字母、数字、斜杠、下划线和连字符")
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


class WikiIngestConfig(BaseModel):
    wiki_enabled: bool = True
    synthesis_model_id: str | None = None
    summary_model_id: str | None = None
    extraction_granularity: ExtractionGranularity = "standard"
    max_pages_per_ingest: int = Field(default=0, ge=0)


class WikiWorkerOptions(BaseModel):
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


class SourceChunk(BaseModel):
    id: str = Field(min_length=1)
    chunk_index: int = 0
    start_at: int = 0
    text: str = ""
    ocr_text: str = ""
    image_caption: str = ""


class SourceKnowledge(BaseModel):
    id: str = Field(min_length=1)
    tenant_id: int
    knowledge_base_id: UUID
    title: str = Field(min_length=1)
    op_version: str = Field(min_length=1)
    status: Literal["ready", "deleting", "cancelled", "deleted"] = "ready"

    @property
    def is_active(self) -> bool:
        return self.status == "ready"


class TopicCandidate(BaseModel):
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


class CandidateExtraction(BaseModel):
    entities: list[TopicCandidate] = Field(default_factory=list)
    concepts: list[TopicCandidate] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_candidate_groups(self) -> Self:
        if any(candidate.page_type != "entity" for candidate in self.entities):
            raise ValueError("entities 只能包含 entity 候选")
        if any(candidate.page_type != "concept" for candidate in self.concepts):
            raise ValueError("concepts 只能包含 concept 候选")
        return self


class _ModelTextOutput(BaseModel):
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


class PageContribution(BaseModel):
    pending_op_id: UUID
    knowledge_id: str
    title: str
    content: str
    summary: str
    aliases: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    chunk_refs: list[str] = Field(default_factory=list)


class PageMergeRequest(BaseModel):
    slug: str
    title: str
    page_type: TopicPageType
    aliases: list[str] = Field(default_factory=list)
    existing_content: str = ""
    existing_summary: str = ""
    contributions: list[PageContribution] = Field(min_length=1)


class PageMergeOutput(_ModelTextOutput):
    pass


class SlugUpdate(BaseModel):
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


class MapDocumentResult(BaseModel):
    pending_op_id: UUID
    knowledge_id: str
    updates: list[SlugUpdate] = Field(default_factory=list)
    skipped_reason: str | None = None


class ReducedPage(BaseModel):
    slug: str
    title: str
    page_type: IngestPageType
    content: str
    summary: str
    aliases: list[str] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    chunk_refs: list[str] = Field(default_factory=list)
    contributor_op_ids: list[UUID] = Field(default_factory=list)


class BatchResult(BaseModel):
    completed_op_ids: list[UUID] = Field(default_factory=list)
    failed_op_ids: list[UUID] = Field(default_factory=list)

    @property
    def completed_ops(self) -> int:
        return len(self.completed_op_ids)

    @property
    def failed_ops(self) -> int:
        return len(self.failed_op_ids)


class FinalizationRequest(BaseModel):
    tenant_id: int
    knowledge_base_id: UUID
    knowledge_id: str
    attempt: str
    subtask_name: str = "wiki"

    @classmethod
    def from_knowledge(cls, scope: WikiScope, knowledge: SourceKnowledge) -> Self:
        return cls(
            tenant_id=scope.tenant_id,
            knowledge_base_id=scope.knowledge_base_id,
            knowledge_id=knowledge.id,
            attempt=knowledge.op_version,
        )


class EnqueueResult(BaseModel):
    pending_op_id: UUID | None = None
    skipped_reason: str | None = None
    deduplicated: bool = False
