"""Wiki pending-op 批次的 Map/Reduce 编排。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar
from uuid import NAMESPACE_URL, UUID, uuid5

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.wiki.ingest.errors import WikiBatchBusy
from app.wiki.ingest.map_document import map_document
from app.wiki.ingest.ports import (
    ChatModelPort,
    KnowledgeSourcePort,
    TransientModelError,
)
from app.wiki.ingest.reduce_slug import reduce_slug
from app.wiki.ingest.schemas import (
    BatchResult,
    MapDocumentResult,
    ReducedPage,
    SlugUpdate,
    WikiWorkerOptions,
)
from app.wiki.ingest.store import (
    ExistingPageRecord,
    IngestStore,
    PageConflict,
    PendingOpRecord,
)
from app.wiki.scope import WikiScope
from app.wiki.tasks.locks import LockLease, LockOwnershipLost, WikiLockManager


_T = TypeVar("_T")
_RetryWait = Callable[[int], Awaitable[None]]


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


class WikiIngestWorker:
    """领取一个知识库批次并隔离单文档、单 slug 的失败。"""

    def __init__(
        self,
        *,
        store: IngestStore,
        locks: WikiLockManager,
        source: KnowledgeSourcePort,
        model: ChatModelPort,
        options: WikiWorkerOptions | None = None,
        retry_wait: _RetryWait | None = None,
    ) -> None:
        self._store = store
        self._locks = locks
        self._source = source
        self._model = model
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
        map_results, failed_ids = await self._map_records(scope, records)
        updates = [
            update
            for result in map_results
            for update in result.updates
        ]
        slugs = list(dict.fromkeys(update.slug for update in updates))
        existing_pages = await self._store.find_existing_pages(scope, slugs)

        pages = await self._stabilize_pages(
            updates,
            existing_pages,
            failed_ids,
        )
        while True:
            invalid_ids = await self._find_invalid_sources(scope, records, failed_ids)
            if not invalid_ids:
                break
            failed_ids.update(invalid_ids)
            pages = await self._stabilize_pages(
                updates,
                existing_pages,
                failed_ids,
            )

        failed_in_order = [op_id for op_id in all_ids if op_id in failed_ids]
        result = BatchResult.from_ids(all_ids, failed_in_order)
        expected_pages = {
            page.slug: existing_pages.get(page.slug)
            for page in pages
        }
        operation_id = uuid5(
            NAMESPACE_URL,
            f"wiki:{scope.knowledge_base_id}:{claim_token}",
        )

        try:
            await lease.assert_owned()
        except LockOwnershipLost as error:
            raise WikiLockLost("Wiki 批次锁所有权已丢失") from error

        try:
            await self._store.apply_results(
                scope,
                claim_token,
                pages,
                result.completed_op_ids,
                operation_id,
                failed_op_ids=result.failed_op_ids,
                expected_pages=expected_pages,
            )
        except PageConflict:
            await self._store.release_failed(scope, all_ids, claim_token)
            return BatchResult.from_ids(all_ids, all_ids)
        return result

    @staticmethod
    def _claim_token(records: Sequence[PendingOpRecord]) -> UUID:
        claim_token = records[0].claim_token
        if not isinstance(claim_token, UUID) or any(
            record.claim_token != claim_token for record in records
        ):
            raise RuntimeError("批次 claim token 必须一致且非空")
        return claim_token

    async def _map_records(
        self,
        scope: WikiScope,
        records: Sequence[PendingOpRecord],
    ) -> tuple[list[MapDocumentResult], set[UUID]]:
        semaphore = asyncio.Semaphore(self._options.map_parallel)

        async def run(record: PendingOpRecord) -> MapDocumentResult | None:
            async with semaphore:
                try:
                    return await self._retry_model(
                        lambda: map_document(
                            scope,
                            record.knowledge_id,
                            self._source,
                            self._model,
                            pending_op_id=record.id,
                            options=self._options,
                        )
                    )
                except Exception:
                    return None

        outcomes = await _gather_with_cleanup(*(run(record) for record in records))
        results: list[MapDocumentResult] = []
        failed_ids: set[UUID] = set()
        for record, outcome in zip(records, outcomes, strict=True):
            if outcome is None:
                failed_ids.add(record.id)
            else:
                results.append(outcome)
        return results, failed_ids

    async def _stabilize_pages(
        self,
        updates: Sequence[SlugUpdate],
        existing_pages: dict[str, ExistingPageRecord],
        failed_ids: set[UUID],
    ) -> list[ReducedPage]:
        semaphore = asyncio.Semaphore(self._options.reduce_parallel)
        while True:
            grouped: dict[str, list[SlugUpdate]] = {}
            for update in updates:
                if update.pending_op_id not in failed_ids:
                    grouped.setdefault(update.slug, []).append(update)

            async def run(
                slug: str, slug_updates: list[SlugUpdate]
            ) -> tuple[ReducedPage | None, set[UUID]]:
                async with semaphore:
                    try:
                        page = await self._retry_model(
                            lambda: reduce_slug(
                                slug,
                                slug_updates,
                                (
                                    existing_pages[slug].page
                                    if slug in existing_pages
                                    else None
                                ),
                                self._model,
                            )
                        )
                    except Exception:
                        return None, {
                            update.pending_op_id for update in slug_updates
                        }
                    return page, set()

            outcomes = await _gather_with_cleanup(
                *(run(slug, slug_updates) for slug, slug_updates in grouped.items())
            )
            newly_failed: set[UUID] = set()
            pages: list[ReducedPage] = []
            for page, contributor_failures in outcomes:
                newly_failed.update(contributor_failures)
                if page is not None:
                    pages.append(page)
            newly_failed.difference_update(failed_ids)
            if not newly_failed:
                return pages
            failed_ids.update(newly_failed)

    async def _find_invalid_sources(
        self,
        scope: WikiScope,
        records: Sequence[PendingOpRecord],
        failed_ids: set[UUID],
    ) -> set[UUID]:
        invalid_ids: set[UUID] = set()
        for record in records:
            if record.id in failed_ids:
                continue
            try:
                active = await self._source.is_active(
                    scope,
                    record.knowledge_id,
                    record.op_version,
                )
            except Exception:
                active = False
            if not active:
                invalid_ids.add(record.id)
        return invalid_ids

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
