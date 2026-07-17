"""Wiki Celery 批次任务入口。"""

from __future__ import annotations

import atexit
import asyncio
import inspect
import logging
import os
import threading
from collections.abc import Callable, Coroutine
from concurrent.futures import CancelledError as FutureCancelledError
from typing import Any, NoReturn, TypeVar
from uuid import UUID

from app.infrastructure.tasks.celery_app import celery_app
from app.wiki.ingest.errors import WikiBatchBusy
from app.wiki.ingest.schemas import BatchResult
from app.wiki.scope import WikiScope


T = TypeVar("T")
logger = logging.getLogger(__name__)


class ProcessEventLoopRunner:
    """为同步 Celery 任务提供 lazy、fork-safe 的进程级事件循环。"""

    def __init__(
        self,
        *,
        pid_provider: Callable[[], int] = os.getpid,
        start_timeout: float = 5,
        stop_timeout: float = 5,
    ) -> None:
        self._pid_provider = pid_provider
        self._start_timeout = start_timeout
        self._stop_timeout = stop_timeout
        self._lock = threading.Lock()
        self._pid: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    @property
    def thread(self) -> threading.Thread | None:
        return self._thread

    def run(self, coroutine_factory: Callable[[], Coroutine[Any, Any, T]]) -> T:
        loop = self._ensure_started()
        coroutine = coroutine_factory()
        try:
            future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        except BaseException:
            coroutine.close()
            raise
        try:
            return future.result()
        except FutureCancelledError as exc:
            future.cancel()
            raise asyncio.CancelledError from exc
        except BaseException:
            future.cancel()
            raise

    def close(self) -> None:
        with self._lock:
            self._stop_current_locked()

    def _ensure_started(self) -> asyncio.AbstractEventLoop:
        current_pid = self._pid_provider()
        with self._lock:
            if self._pid != current_pid:
                self._stop_current_locked()
            if (
                self._thread is not None
                and self._thread.is_alive()
                and self._loop is not None
                and self._loop.is_running()
            ):
                return self._loop

            started = threading.Event()
            thread = threading.Thread(
                target=self._thread_main,
                args=(started,),
                name="wiki-batch-event-loop",
                daemon=True,
            )
            self._pid = current_pid
            self._thread = thread
            thread.start()
            if not started.wait(self._start_timeout):
                raise RuntimeError("Wiki 批次事件循环启动超时")
            if self._loop is None or not self._loop.is_running():
                raise RuntimeError("Wiki 批次事件循环启动失败")
            return self._loop

    def _thread_main(self, started: threading.Event) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        loop.call_soon(started.set)
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()

    def _stop_current_locked(self) -> None:
        thread = self._thread
        loop = self._loop
        if thread is not None and thread.is_alive():
            if thread is threading.current_thread():
                raise RuntimeError("不能从 Wiki 批次事件循环线程关闭自身")
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
            thread.join(self._stop_timeout)
            if thread.is_alive():
                raise RuntimeError("Wiki 批次事件循环关闭超时")
        self._pid = None
        self._loop = None
        self._thread = None


def _build_scope(tenant_id: int, knowledge_base_id: str) -> WikiScope:
    if type(tenant_id) is not int or tenant_id <= 0:
        raise ValueError("tenant_id 必须是正整数")
    if not isinstance(knowledge_base_id, str):
        raise TypeError("knowledge_base_id 必须是规范 UUID 字符串")
    try:
        parsed_knowledge_base_id = UUID(knowledge_base_id)
    except ValueError as exc:
        raise ValueError("knowledge_base_id 必须是规范 UUID 字符串") from exc
    if str(parsed_knowledge_base_id) != knowledge_base_id:
        raise ValueError("knowledge_base_id 必须是规范 UUID 字符串")
    return WikiScope(
        tenant_id=tenant_id,
        knowledge_base_id=parsed_knowledge_base_id,
        actor_id="wiki-worker",
        can_write=True,
    )


def build_runtime() -> NoReturn:
    """任务 10 才会组装完整 Worker runtime。"""

    raise RuntimeError("Wiki Worker runtime 尚未组装，将在阶段二任务 10 实现")


async def _run_batch_on_worker_loop(scope: WikiScope) -> BatchResult:
    runtime: Any = build_runtime()
    close_runtime = getattr(runtime, "aclose", None)
    if not inspect.iscoroutinefunction(close_runtime):
        raise TypeError("Wiki runtime 必须实现 async aclose()")
    try:
        result = await runtime.worker.run_batch(scope)
        if not isinstance(result, BatchResult):
            raise TypeError("Wiki Worker 必须返回 BatchResult")
    except BaseException:
        try:
            await close_runtime()
        except asyncio.CancelledError:
            raise
        except Exception as close_error:
            logger.error(
                "Wiki runtime 关闭失败",
                extra={"wiki_runtime_error_type": type(close_error).__name__},
            )
        raise
    await close_runtime()
    return result


_batch_runner = ProcessEventLoopRunner()


def close_batch_runner() -> None:
    """显式关闭当前进程的 Wiki 批次事件循环。"""

    _batch_runner.close()


def _close_batch_runner_at_exit() -> None:
    try:
        close_batch_runner()
    except Exception:
        # 解释器关闭阶段不再抛出异常；显式 close 仍会向调用方报告失败。
        pass


atexit.register(_close_batch_runner_at_exit)


def run_batch_sync(scope: WikiScope) -> BatchResult:
    """Celery 同步任务到异步 Worker 的可替换桥接点。"""

    return _batch_runner.run(lambda: _run_batch_on_worker_loop(scope))


@celery_app.task(
    bind=True,
    name="wiki.batch.run",
    acks_late=True,
    reject_on_worker_lost=True,
)
def wiki_batch_task(
    self: Any, tenant_id: int, knowledge_base_id: str
) -> dict[str, object]:
    scope = _build_scope(tenant_id, knowledge_base_id)
    try:
        result = run_batch_sync(scope)
    except WikiBatchBusy as exc:
        raise self.retry(exc=exc, countdown=15, max_retries=10)
    if not isinstance(result, BatchResult):
        raise TypeError("Wiki Worker 必须返回 BatchResult")
    return result.model_dump(mode="json")
