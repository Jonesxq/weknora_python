"""Wiki 摄取依赖的外部端口。"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from sqlalchemy.ext.asyncio import AsyncSession

from app.wiki.ingest.schemas import (
    CandidateExtraction,
    CitationBatchOutput,
    CitationBatchRequest,
    DedupOutput,
    DedupRequest,
    DocumentSummary,
    FinalizationRequest,
    PageMergeOutput,
    PageMergeRequest,
    SourceChunk,
    SourceKnowledge,
    WikiIngestConfig,
)
from app.wiki.scope import WikiScope


class TransientModelError(RuntimeError):
    """允许调用方重试的模型错误。"""


class PermanentModelError(RuntimeError):
    """重试无法恢复的模型错误。"""


@runtime_checkable
class KnowledgeSourcePort(Protocol):
    async def get_config(self, scope: WikiScope) -> WikiIngestConfig: ...

    async def get_knowledge(
        self, scope: WikiScope, knowledge_id: str
    ) -> SourceKnowledge | None: ...

    async def list_chunks(self, scope: WikiScope, knowledge_id: str) -> list[SourceChunk]: ...

    async def is_active(
        self, scope: WikiScope, knowledge_id: str, op_version: str
    ) -> bool: ...


@runtime_checkable
class ChatModelPort(Protocol):
    async def extract_candidates(
        self, knowledge_id: str, text: str, config: WikiIngestConfig
    ) -> CandidateExtraction: ...

    async def summarize(
        self, knowledge_id: str, title: str, text: str
    ) -> DocumentSummary: ...

    async def merge_page(self, request: PageMergeRequest) -> PageMergeOutput: ...


@runtime_checkable
class CitationModelPort(Protocol):
    async def classify_chunks(self, request: CitationBatchRequest) -> CitationBatchOutput: ...


@runtime_checkable
class DedupModelPort(Protocol):
    async def resolve_duplicates(self, request: DedupRequest) -> DedupOutput: ...


@runtime_checkable
class WikiIngestModelPort(ChatModelPort, CitationModelPort, DedupModelPort, Protocol):
    pass


@runtime_checkable
class TombstonePort(Protocol):
    async def mark_deleted(self, scope: WikiScope, knowledge_id: str) -> None: ...

    async def is_deleted(self, scope: WikiScope, knowledge_id: str) -> bool: ...


@runtime_checkable
class FinalizationPort(Protocol):
    async def register(self, session: AsyncSession, request: FinalizationRequest) -> bool: ...

    async def release(self, session: AsyncSession, request: FinalizationRequest) -> bool: ...
