"""由共享 JSON fixture 驱动的 Wiki 摄取 fake 适配器。"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import re
from typing import Annotated, Iterable, Mapping
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator, model_validator

from app.wiki.ingest.ports import PermanentModelError, TransientModelError
from app.wiki.ingest.schemas import (
    CandidateExtraction,
    CitationBatchOutput,
    CitationBatchRequest,
    DedupDecision,
    DedupOutput,
    DedupRequest,
    DocumentSummary,
    EmbeddingOutput,
    EmbeddingRequest,
    IndexIntroChange,
    IndexIntroOutput,
    IndexIntroRequest,
    PageMergeOutput,
    PageMergeRequest,
    SourceChunk,
    SourceKnowledge,
    TaxonomyOutput,
    TaxonomyRequest,
    WikiIngestConfig,
    _StrictModel,
    _normalize_embedding_key,
    _normalize_slug,
)
from app.wiki.scope import WikiScope


class _KnowledgeBaseFixture(_StrictModel):
    tenant_id: int
    knowledge_base_id: UUID
    config: WikiIngestConfig


class _KnowledgeFixture(SourceKnowledge):
    chunks: list[SourceChunk] = Field(default_factory=list)


class _ModelResponses(_StrictModel):
    extract_candidates: dict[str, CandidateExtraction] = Field(min_length=1)
    summaries: dict[str, DocumentSummary] = Field(min_length=1)
    merges: dict[str, PageMergeOutput] = Field(min_length=1)
    citations: dict[str, list[CitationBatchOutput]] = Field(default_factory=dict)
    deduplications: dict[str, DedupDecision] = Field(default_factory=dict)
    embeddings: dict[str, tuple[float, ...]] = Field(default_factory=dict)
    taxonomies: dict[str, TaxonomyOutput] = Field(default_factory=dict)
    index_intros: dict[str, IndexIntroOutput] = Field(default_factory=dict)

    @field_validator("embeddings", mode="before")
    @classmethod
    def validate_embedding_responses(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            raise ValueError("embeddings 响应必须是映射")
        if not value:
            return {}
        for raw_key in value:
            if not isinstance(raw_key, str) or _normalize_embedding_key(raw_key) != raw_key:
                raise ValueError("embeddings 响应键必须是非空规范 key")
        return dict(EmbeddingOutput(vectors=value).vectors)

    @field_validator("index_intros")
    @classmethod
    def validate_index_intro_responses(
        cls, value: dict[str, IndexIntroOutput]
    ) -> dict[str, IndexIntroOutput]:
        for key in value:
            if key.startswith("index_intro:create:"):
                slugs = key.removeprefix("index_intro:create:").split(",")
                try:
                    if (
                        any(_normalize_slug(slug, ("summary",)) != slug for slug in slugs)
                        or len(slugs) != len(set(slugs))
                    ):
                        raise ValueError
                except ValueError as exc:
                    raise ValueError("index intro create 响应键必须是唯一的规范 summary slug") from exc
                continue

            if key.startswith("index_intro:update:"):
                raw_changes = key.removeprefix("index_intro:update:").split(",")
                changes: list[tuple[str, str]] = []
                try:
                    for raw_change in raw_changes:
                        action, knowledge_id = raw_change.split(":")
                        change = IndexIntroChange(action=action, knowledge_id=knowledge_id)
                        if change.knowledge_id != knowledge_id:
                            raise ValueError
                        changes.append((change.action, change.knowledge_id))
                    if len(changes) != len(set(changes)) or changes != sorted(changes):
                        raise ValueError
                except ValueError as exc:
                    raise ValueError(
                        "index intro update 响应键必须是有序且唯一的 action:knowledge_id"
                    ) from exc
                continue

            raise ValueError("index intro 响应键必须使用 create 或 update 模式")
        return value


FailureCount = Annotated[int, Field(ge=0)]


class FakeDataset(_StrictModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    knowledge_bases: list[_KnowledgeBaseFixture] = Field(min_length=1)
    knowledge: list[_KnowledgeFixture] = Field(min_length=1)
    model_responses: _ModelResponses
    transient_failures: dict[str, FailureCount] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_identity_and_scope(self) -> "FakeDataset":
        kb_keys = [(item.tenant_id, item.knowledge_base_id) for item in self.knowledge_bases]
        if len(kb_keys) != len(set(kb_keys)):
            raise ValueError("knowledge_bases 不能包含重复的租户和知识库")
        knowledge_keys = [
            (item.tenant_id, item.knowledge_base_id, item.id) for item in self.knowledge
        ]
        if len(knowledge_keys) != len(set(knowledge_keys)):
            raise ValueError("knowledge 不能包含重复的租户、知识库和知识条目标识")
        knowledge_ids_in_order = [item.id for item in self.knowledge]
        if len(knowledge_ids_in_order) != len(set(knowledge_ids_in_order)):
            raise ValueError("fake 模型响应要求 knowledge_id 在 fixture 中全局唯一")
        known_kbs = set(kb_keys)
        for item in self.knowledge:
            if (item.tenant_id, item.knowledge_base_id) not in known_kbs:
                raise ValueError("knowledge 必须属于已声明的 knowledge_base")
        knowledge_ids = {item.id for item in self.knowledge}
        candidate_responses = self.model_responses.extract_candidates
        unknown_candidate_ids = set(candidate_responses) - knowledge_ids
        if unknown_candidate_ids:
            raise ValueError("extract_candidates 响应包含未知 knowledge_id")
        unknown_summary_ids = set(self.model_responses.summaries) - knowledge_ids
        if unknown_summary_ids:
            raise ValueError("summaries 响应包含未知 knowledge_id")
        unknown_citation_ids = set(self.model_responses.citations) - knowledge_ids
        if unknown_citation_ids:
            raise ValueError("citations 响应包含未知 knowledge_id")
        for slug in self.model_responses.merges:
            normalized_slug = _normalize_slug(slug, ("entity", "concept"))
            if normalized_slug != slug:
                raise ValueError("merges 响应键必须是已规范化的小写页面 slug")
        for slug, decision in self.model_responses.deduplications.items():
            normalized_slug = _normalize_slug(slug, ("entity", "concept"))
            if normalized_slug != slug or decision.candidate_slug != slug:
                raise ValueError("deduplications 响应键必须是规范 slug 且等于 candidate_slug")
        for batch_key, output in self.model_responses.taxonomies.items():
            slugs = batch_key.split(",")
            if not batch_key or any(not slug for slug in slugs):
                raise ValueError("taxonomies 响应 batch key 不能为空")
            normalized_slugs = [_normalize_slug(slug, ("entity", "concept")) for slug in slugs]
            if normalized_slugs != slugs or slugs != sorted(slugs) or len(slugs) != len(set(slugs)):
                raise ValueError("taxonomies 响应 batch key 必须是有序且唯一的规范 slug")
            if [decision.slug for decision in output.decisions] != slugs:
                raise ValueError("taxonomies 响应必须按 batch key 顺序完整覆盖 decisions")
        for key in self.transient_failures:
            prefix, _, suffix = key.partition(":")
            if prefix == "extract_candidates" and suffix not in knowledge_ids:
                raise ValueError("extract_candidates 瞬时失败键包含未知 knowledge_id")
            if prefix == "summarize" and suffix not in knowledge_ids:
                raise ValueError("summarize 瞬时失败键包含未知 knowledge_id")
            if prefix == "merge" and suffix not in self.model_responses.merges:
                raise ValueError("merge 瞬时失败键包含未知 slug")
            if prefix == "citation":
                knowledge_id, separator, batch_index = suffix.rpartition(":")
                if (
                    not separator
                    or knowledge_id not in knowledge_ids
                    or re.fullmatch(r"0|[1-9][0-9]*", batch_index) is None
                ):
                    raise ValueError("citation 瞬时失败键必须引用已知 knowledge_id 和非负 batch")
            if prefix == "dedup" and suffix not in self.model_responses.deduplications:
                raise ValueError("dedup 瞬时失败键包含未知 slug")
            if prefix == "embedding":
                vector_keys = suffix.split(",")
                if (
                    any(not vector_key for vector_key in vector_keys)
                    or len(vector_keys) != len(set(vector_keys))
                    or any(vector_key not in self.model_responses.embeddings for vector_key in vector_keys)
                ):
                    raise ValueError("embedding 瞬时失败键必须引用有序且唯一的已声明 vector key")
            if prefix == "taxonomy" and suffix not in self.model_responses.taxonomies:
                raise ValueError("taxonomy 瞬时失败键必须引用已声明 batch")
            if prefix == "index_intro" and f"index_intro:{suffix}" not in self.model_responses.index_intros:
                raise ValueError("index intro 瞬时失败键包含未知响应 key")
        return self

    @field_validator("transient_failures")
    @classmethod
    def validate_failure_keys(cls, value: dict[str, int]) -> dict[str, int]:
        for key in value:
            prefix, separator, suffix = key.partition(":")
            if not separator or not suffix or prefix not in {
                "extract_candidates",
                "summarize",
                "merge",
                "citation",
                "dedup",
                "embedding",
                "taxonomy",
                "index_intro",
            }:
                raise ValueError(f"不支持的 transient failure key: {key}")
            if prefix == "merge" and not (
                suffix.startswith("entity/") or suffix.startswith("concept/")
            ):
                raise ValueError(f"merge transient failure key 必须使用页面 slug: {key}")
            if prefix == "citation" and ":" not in suffix:
                raise ValueError(f"citation transient failure key 格式无效: {key}")
            if prefix == "dedup":
                try:
                    if _normalize_slug(suffix, ("entity", "concept")) != suffix:
                        raise ValueError
                except ValueError as exc:
                    raise ValueError(f"dedup transient failure key 必须使用页面 slug: {key}") from exc
            if prefix == "embedding":
                vector_keys = suffix.split(",")
                try:
                    if (
                        any(_normalize_embedding_key(vector_key) != vector_key for vector_key in vector_keys)
                        or len(vector_keys) != len(set(vector_keys))
                    ):
                        raise ValueError
                except ValueError as exc:
                    raise ValueError(
                        f"embedding transient failure key 必须使用有序且唯一的 vector key: {key}"
                    ) from exc
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
        response_snapshot = self._responses.model_copy(deep=True)
        self.responses = {
            "extract_candidates": response_snapshot.extract_candidates,
            "summaries": response_snapshot.summaries,
            "merges": response_snapshot.merges,
            "citations": response_snapshot.citations,
            "deduplications": response_snapshot.deduplications,
            "taxonomies": response_snapshot.taxonomies,
            "index_intros": response_snapshot.index_intros,
        }
        self._remaining_failures = dict(dataset.transient_failures)
        self.calls: list[str] = []
        self.merge_requests: list[PageMergeRequest] = []
        self.citation_requests: list[CitationBatchRequest] = []
        self.dedup_requests: list[DedupRequest] = []
        self.taxonomy_requests: list[TaxonomyRequest] = []
        self.index_intro_requests: list[IndexIntroRequest] = []

    def _record_call(self, key: str) -> None:
        self.calls.append(key)
        remaining = self._remaining_failures.get(key, 0)
        if remaining > 0:
            self._remaining_failures[key] = remaining - 1
            raise TransientModelError(f"模型调用瞬时失败: {key}")

    async def extract_candidates(
        self,
        knowledge_id: str,
        text: str,
        config: WikiIngestConfig,
    ) -> CandidateExtraction:
        key = f"extract_candidates:{knowledge_id}"
        self._record_call(key)
        response = self._responses.extract_candidates.get(knowledge_id)
        if response is None:
            raise PermanentModelError(f"缺少模型响应: {key}")
        return response.model_copy(deep=True)

    async def summarize(
        self,
        knowledge_id: str,
        title: str,
        text: str,
    ) -> DocumentSummary:
        key = f"summarize:{knowledge_id}"
        self._record_call(key)
        response = self._responses.summaries.get(knowledge_id)
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

    async def generate_index_intro(self, request: IndexIntroRequest) -> IndexIntroOutput:
        snapshot = IndexIntroRequest.model_validate(request.model_dump())
        self.index_intro_requests.append(snapshot)
        if snapshot.mode == "create":
            suffix = _batch_key(summary.slug for summary in snapshot.summaries)
        else:
            suffix = _batch_key(
                f"{change.action}:{change.knowledge_id}" for change in snapshot.changes
            )
        key = f"index_intro:{snapshot.mode}:{suffix}"
        self._record_call(key)
        response = self._responses.index_intros.get(key)
        if response is None:
            raise PermanentModelError(f"缺少模型响应: {key}")
        return response.model_copy(deep=True)

    async def classify_chunks(self, request: CitationBatchRequest) -> CitationBatchOutput:
        key = f"citation:{request.knowledge_id}:{request.batch_index}"
        self.citation_requests.append(request.model_copy(deep=True))
        self._record_call(key)
        responses = self._responses.citations.get(request.knowledge_id)
        if responses is None or request.batch_index >= len(responses):
            raise PermanentModelError(f"缺少模型响应: {key}")
        return responses[request.batch_index].model_copy(deep=True)

    async def resolve_duplicates(self, request: DedupRequest) -> DedupOutput:
        self.dedup_requests.append(request.model_copy(deep=True))
        decisions: list[DedupDecision] = []
        for item in request.candidates:
            slug = item.candidate.slug
            self._record_call(f"dedup:{slug}")
            decision = self._responses.deduplications.get(slug)
            if decision is None:
                decision = DedupDecision(candidate_slug=slug)
            decisions.append(decision.model_copy(deep=True))
        return DedupOutput(decisions=decisions)

    async def plan_folders(self, request: TaxonomyRequest) -> TaxonomyOutput:
        snapshot = TaxonomyRequest.model_validate(request.model_dump())
        self.taxonomy_requests.append(snapshot)
        batch_key = _batch_key(sorted(topic.slug for topic in snapshot.topics))
        key = f"taxonomy:{batch_key}"
        self._record_call(key)
        response = self._responses.taxonomies.get(batch_key)
        if response is None:
            raise PermanentModelError(f"缺少模型响应: {key}")
        return response.model_copy(deep=True)


def _batch_key(values: Iterable[str]) -> str:
    return ",".join(values)


class FakeEmbeddingModel:
    def __init__(self, dataset: FakeDataset) -> None:
        self._vectors = deepcopy(dataset.model_responses.embeddings)
        self._remaining_failures = deepcopy(dataset.transient_failures)
        self.calls: list[str] = []
        self.requests: list[EmbeddingRequest] = []

    def _record_call(self, key: str) -> None:
        self.calls.append(key)
        remaining = self._remaining_failures.get(key, 0)
        if remaining > 0:
            self._remaining_failures[key] = remaining - 1
            raise TransientModelError(f"模型调用瞬时失败: {key}")

    async def embed(self, request: EmbeddingRequest) -> EmbeddingOutput:
        snapshot = EmbeddingRequest.model_validate(request.model_dump())
        self.requests.append(snapshot)
        keys = tuple(item.key for item in snapshot.items)
        key = f"embedding:{_batch_key(keys)}"
        self._record_call(key)
        missing = [item_key for item_key in keys if item_key not in self._vectors]
        if missing:
            raise PermanentModelError(f"缺少模型响应: {key}")
        return EmbeddingOutput(vectors={item_key: deepcopy(self._vectors[item_key]) for item_key in keys})


def load_fake_adapters(path: str | Path) -> tuple[FakeKnowledgeSource, FakeChatModel]:
    dataset = FakeDataset.model_validate_json(Path(path).read_text(encoding="utf-8"))
    return FakeKnowledgeSource(dataset), FakeChatModel(dataset)


def load_fake_runtime_adapters(
    path: str | Path,
) -> tuple[FakeKnowledgeSource, FakeChatModel, FakeEmbeddingModel]:
    dataset = FakeDataset.model_validate_json(Path(path).read_text(encoding="utf-8"))
    return FakeKnowledgeSource(dataset), FakeChatModel(dataset), FakeEmbeddingModel(dataset)
