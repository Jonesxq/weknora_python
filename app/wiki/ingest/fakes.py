"""由共享 JSON fixture 驱动的 Wiki 摄取 fake 适配器。"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from app.wiki.ingest.ports import PermanentModelError, TransientModelError
from app.wiki.ingest.schemas import (
    CandidateExtraction,
    DocumentSummary,
    PageMergeOutput,
    PageMergeRequest,
    SourceChunk,
    SourceKnowledge,
    WikiIngestConfig,
)
from app.wiki.scope import WikiScope


class _KnowledgeBaseFixture(BaseModel):
    tenant_id: int
    knowledge_base_id: UUID
    config: WikiIngestConfig


class _KnowledgeFixture(SourceKnowledge):
    chunks: list[SourceChunk] = Field(default_factory=list)


class _ModelResponses(BaseModel):
    extract_candidates: CandidateExtraction
    summaries: dict[str, DocumentSummary] = Field(min_length=1)
    merges: dict[str, PageMergeOutput] = Field(min_length=1)


FailureCount = Annotated[int, Field(ge=0)]


class FakeDataset(BaseModel):
    knowledge_bases: list[_KnowledgeBaseFixture] = Field(min_length=1)
    knowledge: list[_KnowledgeFixture] = Field(min_length=1)
    model_responses: _ModelResponses
    transient_failures: dict[str, FailureCount] = Field(default_factory=dict)

    @field_validator("transient_failures")
    @classmethod
    def validate_failure_keys(cls, value: dict[str, int]) -> dict[str, int]:
        for key in value:
            if key == "extract_candidates":
                continue
            prefix, separator, suffix = key.partition(":")
            if separator and suffix and prefix in {"summarize", "merge"}:
                continue
            raise ValueError(f"不支持的 transient failure key: {key}")
        return value


class FakeKnowledgeSource:
    def __init__(self, dataset: FakeDataset) -> None:
        self.knowledge_bases = {
            (item.tenant_id, item.knowledge_base_id): item.config.model_copy(deep=True)
            for item in dataset.knowledge_bases
        }
        self.knowledge = {
            (item.tenant_id, item.knowledge_base_id, item.id): SourceKnowledge.model_validate(
                item.model_dump(exclude={"chunks"})
            )
            for item in dataset.knowledge
        }
        self.chunks = {
            (item.tenant_id, item.knowledge_base_id, item.id): [
                chunk.model_copy(deep=True) for chunk in item.chunks
            ]
            for item in dataset.knowledge
        }

    async def get_config(self, scope: WikiScope) -> WikiIngestConfig:
        config = self.knowledge_bases.get((scope.tenant_id, scope.knowledge_base_id))
        if config is None:
            return WikiIngestConfig(wiki_enabled=False)
        return config.model_copy(deep=True)

    async def get_knowledge(
        self, scope: WikiScope, knowledge_id: str
    ) -> SourceKnowledge | None:
        knowledge = self.knowledge.get(
            (scope.tenant_id, scope.knowledge_base_id, knowledge_id)
        )
        return knowledge.model_copy(deep=True) if knowledge is not None else None

    async def list_chunks(self, scope: WikiScope, knowledge_id: str) -> list[SourceChunk]:
        chunks = self.chunks.get(
            (scope.tenant_id, scope.knowledge_base_id, knowledge_id), []
        )
        return [chunk.model_copy(deep=True) for chunk in chunks]

    async def is_active(
        self, scope: WikiScope, knowledge_id: str, op_version: str
    ) -> bool:
        knowledge = self.knowledge.get(
            (scope.tenant_id, scope.knowledge_base_id, knowledge_id)
        )
        return bool(
            knowledge is not None
            and knowledge.op_version == op_version
            and knowledge.is_active
        )


class FakeChatModel:
    def __init__(self, dataset: FakeDataset) -> None:
        self._responses = dataset.model_responses.model_copy(deep=True)
        self._remaining_failures = dict(dataset.transient_failures)
        self.calls: list[str] = []
        self.merge_requests: list[PageMergeRequest] = []

    def _record_call(self, key: str) -> None:
        self.calls.append(key)
        remaining = self._remaining_failures.get(key, 0)
        if remaining > 0:
            self._remaining_failures[key] = remaining - 1
            raise TransientModelError(f"模型调用瞬时失败: {key}")

    async def extract_candidates(
        self, text: str, config: WikiIngestConfig
    ) -> CandidateExtraction:
        self._record_call("extract_candidates")
        return self._responses.extract_candidates.model_copy(deep=True)

    async def summarize(self, title: str, text: str) -> DocumentSummary:
        key = f"summarize:{title}"
        self._record_call(key)
        response = self._responses.summaries.get(title)
        if response is None:
            raise PermanentModelError(f"缺少模型响应: {key}")
        return response.model_copy(deep=True)

    async def merge_page(self, request: PageMergeRequest) -> PageMergeOutput:
        key = f"merge:{request.slug}"
        self.merge_requests.append(request.model_copy(deep=True))
        self._record_call(key)
        response = self._responses.merges.get(request.slug)
        if response is None:
            raise PermanentModelError(f"缺少模型响应: {key}")
        return response.model_copy(deep=True)


def load_fake_adapters(path: str | Path) -> tuple[FakeKnowledgeSource, FakeChatModel]:
    dataset = FakeDataset.model_validate_json(Path(path).read_text(encoding="utf-8"))
    return FakeKnowledgeSource(dataset), FakeChatModel(dataset)
