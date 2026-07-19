"""Wiki pending-op 批次的 Map/Reduce 编排。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import TypeVar
from uuid import NAMESPACE_URL, UUID, uuid5

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.wiki.errors import WikiValidationError
from app.wiki.ingest.errors import WikiBatchBusy
from app.wiki.ingest.map_document import map_document
from app.wiki.ingest.ports import (
    ChatModelPort,
    KnowledgeSourcePort,
    PermanentModelError,
    TombstonePort,
    TransientModelError,
)
from app.wiki.ingest.reduce_slug import reduce_slug
from app.wiki.ingest.retract import plan_retract_deltas
from app.wiki.ingest.schemas import (
    BatchApplyRequest,
    BatchResult,
    ContributionDelta,
    MapDocumentResult,
    OperationFailure,
    PageExpectation,
    ReducedPage,
    StoredContributionRecord,
    WikiWorkerOptions,
)
from app.wiki.ingest.store import (
    ExistingPageRecord,
    IngestStore,
    IngestStoreError,
    PageConflict,
    PendingOpRecord,
)
from app.wiki.scope import WikiScope
from app.wiki.tasks.locks import LockLease, LockOwnershipLost, WikiLockManager


_T = TypeVar("_T")
_RetryWait = Callable[[int], Awaitable[None]]


def operation_failure(op_id: UUID, error: Exception) -> OperationFailure:
    if isinstance(error, PermanentModelError):
        code = "MODEL_PERMANENT"
        summary = "模型调用发生永久错误"
    elif isinstance(error, TransientModelError):
        code = "MODEL_RETRY_EXHAUSTED"
        summary = "模型调用重试已耗尽"
    elif isinstance(error, WikiValidationError):
        code = error.code
        summary = "Wiki 数据校验失败"
    else:
        code = "WIKI_INGEST_FAILED"
        summary = f"Wiki 处理失败（{type(error).__name__}）"
    summary = " ".join(summary.split())[:2000] or type(error).__name__
    return OperationFailure(
        pending_op_id=op_id,
        error_code=code.strip()[:128] or "WIKI_INGEST_FAILED",
        error_summary=summary,
    )


async def _gather_with_cleanup(*operations: Awaitable[_T]) -> list[_T]:
    tasks = [asyncio.create_task(operation) for operation in operations]
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


class WikiLockLost(RuntimeError):
    """提交前无法确认 Wiki 批次锁仍由当前 Worker 持有。"""


class _NeverDeletedTombstones:
    """仅供任务 12 前旧 Worker 构造调用方使用。"""

    async def mark_deleted(self, scope: WikiScope, knowledge_id: str) -> None:
        raise RuntimeError("Worker 兼容 tombstone 不支持写入")

    async def is_deleted(self, scope: WikiScope, knowledge_id: str) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class _PreparedOperation:
    deltas: tuple[ContributionDelta, ...] = ()
    superseded: bool = False
    failure: OperationFailure | None = None


class WikiIngestWorker:
    """领取一个知识库批次并隔离单文档、单 slug 的失败。"""

    def __init__(
        self,
        *,
        store: IngestStore,
        locks: WikiLockManager,
        source: KnowledgeSourcePort,
        model: ChatModelPort,
        tombstones: TombstonePort | None = None,
        options: WikiWorkerOptions | None = None,
        retry_wait: _RetryWait | None = None,
    ) -> None:
        self._store = store
        self._locks = locks
        self._source = source
        self._model = model
        self._tombstones = (
            tombstones if tombstones is not None else _NeverDeletedTombstones()
        )
        self._options = options or WikiWorkerOptions.from_env()
        self._retry_wait = retry_wait or asyncio.sleep

    async def run_batch(self, scope: WikiScope) -> BatchResult:
        lease = await self._locks.acquire(scope.knowledge_base_id)
        if lease is None:
            if await self._store.pending_count(scope) > 0:
                raise WikiBatchBusy("同一知识库已有 Wiki 批次运行")
            return BatchResult()

        async with lease:
            return await self._run_locked(scope, lease)

    async def _run_locked(self, scope: WikiScope, lease: LockLease) -> BatchResult:
        records = await self._store.claim_pending(
            scope,
            self._options.batch_size,
            self._options.claim_timeout_seconds,
        )
        if not records:
            if await self._store.pending_count(scope) > 0:
                raise WikiBatchBusy("Wiki pending-op 暂时由其他批次持有")
            return BatchResult()

        claim_token = self._claim_token(records)
        all_ids = [record.id for record in records]
        try:
            return await self._process_claimed_batch(scope, lease, records, claim_token)
        except PageConflict:
            await self._release_claim(scope, all_ids, claim_token)
            return BatchResult.from_ids(all_ids, all_ids)
        except LockOwnershipLost as error:
            await self._release_claim_preserving(
                scope, all_ids, claim_token, primary=error
            )
            raise WikiLockLost("Wiki 批次锁所有权已丢失") from error
        except BaseException as error:
            await self._release_claim_preserving(
                scope, all_ids, claim_token, primary=error
            )
            raise

    async def _process_claimed_batch(
        self,
        scope: WikiScope,
        lease: LockLease,
        records: Sequence[PendingOpRecord],
        claim_token: UUID,
    ) -> BatchResult:
        prepared = await self._prepare_operations(scope, records)
        failures = {
            record.id: outcome.failure
            for record, outcome in zip(records, prepared, strict=True)
            if outcome.failure is not None
        }
        superseded = {
            record.id
            for record, outcome in zip(records, prepared, strict=True)
            if outcome.superseded
        }
        all_deltas = [delta for outcome in prepared for delta in outcome.deltas]
        initially_excluded = {*failures, *superseded}
        initial_deltas = [
            delta
            for delta in all_deltas
            if delta.pending_op_id not in initially_excluded
        ]
        slugs = list(dict.fromkeys(delta.slug for delta in initial_deltas))
        existing_pages = await self._store.find_existing_pages(scope, slugs)
        active_by_slug = await self._load_active_contributions(
            scope, initial_deltas, existing_pages
        )
        while True:
            pages = await self._stabilize_pages(
                initial_deltas,
                existing_pages,
                active_by_slug,
                failures,
                superseded,
            )
            newly_excluded = await self._precommit_ingest_checks(
                scope, records, failures, superseded
            )
            if not newly_excluded:
                break

        failed_ids = [record.id for record in records if record.id in failures]
        superseded_ids = [record.id for record in records if record.id in superseded]
        completed_ids = [
            record.id
            for record in records
            if record.id not in failures and record.id not in superseded
        ]
        completed_set = set(completed_ids)
        contribution_deltas = tuple(
            delta for delta in initial_deltas if delta.pending_op_id in completed_set
        )
        expectations = tuple(
            self._page_expectation(page.slug, existing_pages.get(page.slug))
            for page in pages
        )
        request = BatchApplyRequest(
            claim_token=claim_token,
            pages=tuple(pages),
            contribution_deltas=contribution_deltas,
            completed_op_ids=tuple(completed_ids),
            superseded_op_ids=tuple(superseded_ids),
            failures=tuple(failures[op_id] for op_id in failed_ids),
            expected_pages=expectations,
            operation_id=uuid5(
                NAMESPACE_URL,
                f"wiki:{scope.knowledge_base_id}:{claim_token}",
            ),
        )
        await lease.assert_owned()
        outcome = await self._store.apply_results_with_outcome(scope, request)
        return BatchResult(
            completed_op_ids=outcome.completed_op_ids,
            failed_op_ids=outcome.failed_op_ids,
            superseded_op_ids=outcome.superseded_op_ids,
        )

    @staticmethod
    def _claim_token(records: Sequence[PendingOpRecord]) -> UUID:
        claim_token = records[0].claim_token
        if not isinstance(claim_token, UUID) or any(
            record.claim_token != claim_token for record in records
        ):
            raise RuntimeError("批次 claim token 必须一致且非空")
        return claim_token

    async def _prepare_operations(
        self,
        scope: WikiScope,
        records: Sequence[PendingOpRecord],
    ) -> list[_PreparedOperation]:
        semaphore = asyncio.Semaphore(self._options.map_parallel)

        async def run(record: PendingOpRecord) -> _PreparedOperation:
            async with semaphore:
                try:
                    if record.op == "ingest":
                        result = await self._retry_model(
                            lambda: map_document(
                                scope,
                                record.knowledge_id,
                                self._source,
                                self._model,
                                self._store,
                                self._tombstones,
                                pending_op_id=record.id,
                                op_version=record.op_version,
                                options=self._options,
                            )
                        )
                        return self._prepared_map_result(record, result)
                    if record.op == "retract":
                        contributions = await self._store.list_source_contributions(
                            scope, record.knowledge_id, state="retract_pending"
                        )
                        return _PreparedOperation(
                            deltas=tuple(plan_retract_deltas(record.id, contributions))
                        )
                    return _PreparedOperation(
                        failure=OperationFailure(
                            pending_op_id=record.id,
                            error_code="WIKI_UNKNOWN_OP",
                            error_summary="不支持的 Wiki pending operation",
                        )
                    )
                except Exception as error:
                    if self._is_control_error(error):
                        raise
                    return _PreparedOperation(
                        failure=operation_failure(record.id, error)
                    )

        return await _gather_with_cleanup(*(run(record) for record in records))

    @staticmethod
    def _prepared_map_result(
        record: PendingOpRecord, result: MapDocumentResult
    ) -> _PreparedOperation:
        if (
            result.pending_op_id != record.id
            or result.knowledge_id != record.knowledge_id
        ):
            raise WikiValidationError(
                "WIKI_MAP_IDENTITY_MISMATCH", "Map 结果与 pending operation 身份不一致"
            )
        return _PreparedOperation(
            deltas=result.contribution_deltas,
            superseded=result.superseded,
        )

    async def _load_active_contributions(
        self,
        scope: WikiScope,
        deltas: Sequence[ContributionDelta],
        existing_pages: dict[str, ExistingPageRecord],
    ) -> dict[str, list[StoredContributionRecord]]:
        affected = set(delta.slug for delta in deltas)
        knowledge_ids = list(
            dict.fromkeys(
                [
                    *(
                        knowledge_id
                        for page in existing_pages.values()
                        for knowledge_id in page.page.source_refs
                    ),
                    *(delta.knowledge_id for delta in deltas),
                ]
            )
        )

        async def load(knowledge_id: str) -> list[StoredContributionRecord]:
            return await self._store.list_source_contributions(
                scope, knowledge_id, state="active"
            )

        records_by_source = await _gather_with_cleanup(
            *(load(knowledge_id) for knowledge_id in knowledge_ids)
        )
        active_by_slug: dict[str, list[StoredContributionRecord]] = {
            slug: [] for slug in affected
        }
        for records in records_by_source:
            for record in records:
                if record.slug in affected:
                    active_by_slug[record.slug].append(record)
        return active_by_slug

    async def _stabilize_pages(
        self,
        deltas: Sequence[ContributionDelta],
        existing_pages: dict[str, ExistingPageRecord],
        active_by_slug: dict[str, list[StoredContributionRecord]],
        failures: dict[UUID, OperationFailure],
        superseded: set[UUID],
    ) -> list[ReducedPage]:
        semaphore = asyncio.Semaphore(self._options.reduce_parallel)
        while True:
            excluded = {*failures, *superseded}
            grouped: dict[str, list[ContributionDelta]] = {}
            for delta in deltas:
                if delta.pending_op_id not in excluded:
                    grouped.setdefault(delta.slug, []).append(delta)

            async def run(
                slug: str, slug_deltas: list[ContributionDelta]
            ) -> tuple[ReducedPage | None, Exception | None]:
                async with semaphore:
                    try:
                        page = await self._retry_model(
                            lambda: reduce_slug(
                                slug,
                                slug_deltas,
                                (
                                    existing_pages[slug].page
                                    if slug in existing_pages
                                    else None
                                ),
                                active_by_slug.get(slug, ()),
                                self._model,
                            )
                        )
                    except Exception as error:
                        if self._is_control_error(error):
                            raise
                        return None, error
                    return page, None

            outcomes = await _gather_with_cleanup(
                *(run(slug, slug_deltas) for slug, slug_deltas in grouped.items())
            )
            newly_failed: set[UUID] = set()
            pages: list[ReducedPage] = []
            for (slug, slug_deltas), (page, error) in zip(
                grouped.items(), outcomes, strict=True
            ):
                if error is None:
                    assert page is not None
                    pages.append(page)
                    continue
                for op_id in dict.fromkeys(
                    delta.pending_op_id for delta in slug_deltas
                ):
                    if op_id not in failures:
                        failures[op_id] = operation_failure(op_id, error)
                        newly_failed.add(op_id)
            if not newly_failed:
                return pages

    async def _precommit_ingest_checks(
        self,
        scope: WikiScope,
        records: Sequence[PendingOpRecord],
        failures: dict[UUID, OperationFailure],
        superseded: set[UUID],
    ) -> bool:
        changed = False
        for record in records:
            if (
                record.op != "ingest"
                or record.id in failures
                or record.id in superseded
            ):
                continue
            try:
                active = await self._source.is_active(
                    scope,
                    record.knowledge_id,
                    record.op_version,
                )
                deleted = await self._tombstones.is_deleted(scope, record.knowledge_id)
            except Exception as error:
                if self._is_control_error(error):
                    raise
                failures[record.id] = operation_failure(record.id, error)
                changed = True
                continue
            if not active or deleted:
                superseded.add(record.id)
                changed = True
        return changed

    @staticmethod
    def _page_expectation(
        slug: str, existing: ExistingPageRecord | None
    ) -> PageExpectation:
        if existing is None:
            return PageExpectation(slug=slug)
        return PageExpectation(
            slug=slug, page_id=existing.page_id, version=existing.version
        )

    @staticmethod
    def _is_control_error(error: Exception) -> bool:
        return isinstance(
            error,
            (WikiBatchBusy, IngestStoreError, LockOwnershipLost, WikiLockLost),
        )

    async def _release_claim(
        self, scope: WikiScope, ids: Sequence[UUID], claim_token: UUID
    ) -> None:
        task = asyncio.create_task(
            self._store.release_claim(scope, ids, claim_token),
            name="wiki-release-claim",
        )
        cancellation: asyncio.CancelledError | None = None
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as error:
                if task.done():
                    break
                cancellation = error
        await task
        if cancellation is not None:
            raise cancellation

    async def _release_claim_preserving(
        self,
        scope: WikiScope,
        ids: Sequence[UUID],
        claim_token: UUID,
        *,
        primary: BaseException,
    ) -> None:
        try:
            await self._release_claim(scope, ids, claim_token)
        except asyncio.CancelledError:
            raise
        except BaseException as cleanup_error:
            if primary.__cause__ is None:
                primary.__cause__ = cleanup_error

    async def _retry_model(self, operation: Callable[[], Awaitable[_T]]) -> _T:
        async def retry_sleep(seconds: int | float) -> None:
            await self._retry_wait(int(seconds))

        async for attempt in AsyncRetrying(
            sleep=retry_sleep,
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, max=4),
            retry=retry_if_exception_type(TransientModelError),
            reraise=True,
        ):
            with attempt:
                return await operation()
        raise AssertionError("unreachable")
