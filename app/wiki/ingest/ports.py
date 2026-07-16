"""Wiki 摄取依赖的外部端口。"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from sqlalchemy.ext.asyncio import AsyncSession

from app.wiki.ingest.schemas import (
    CandidateExtraction,
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
class FinalizationPort(Protocol):
    async def register(self, session: AsyncSession, request: FinalizationRequest) -> bool: ...

    async def release(self, session: AsyncSession, request: FinalizationRequest) -> bool: ...
