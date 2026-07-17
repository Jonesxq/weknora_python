from __future__ import annotations

import asyncio
import json
from pathlib import Path
import threading
from uuid import UUID

import pytest
from celery.exceptions import Retry

from app.wiki.ingest.errors import WikiBatchBusy
from app.wiki.ingest.schemas import BatchResult
from app.wiki.tasks import wiki_tasks


KB_ID = UUID("11111111-1111-1111-1111-111111111111")


def _write_fake_fixture(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "knowledge_bases": [
                    {
                        "tenant_id": 7,
                        "knowledge_base_id": str(KB_ID),
                        "config": {
                            "wiki_enabled": True,
                            "synthesis_model_id": "fake-synthesis",
                        },
                    }
                ],
                "knowledge": [
                    {
                        "id": "knowledge-1",
                        "tenant_id": 7,
                        "knowledge_base_id": str(KB_ID),
                        "title": "Document One",
                        "op_version": "v1",
                        "chunks": [{"id": "chunk-1", "text": "Source text"}],
                    }
                ],
                "model_responses": {
                    "extract_candidates": {"knowledge-1": {}},
                    "summaries": {
                        "knowledge-1": {"headline": "Document One", "markdown": "Summary"}
                    },
                    "merges": {
                        "entity/example": {"headline": "Example", "markdown": "Body"}
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def test_wiki_batch_task_returns_json_batch_result_in_eager_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed_id = UUID("22222222-2222-2222-2222-222222222222")
    monkeypatch.setattr(
        wiki_tasks,
        "run_batch_sync",
        lambda scope: BatchResult(completed_op_ids=[completed_id]),
    )

    result = wiki_tasks.wiki_batch_task.apply(
        kwargs={"tenant_id": 7, "knowledge_base_id": str(KB_ID)}
    ).get()

    assert result == {
        "completed_op_ids": [str(completed_id)],
        "failed_op_ids": [],
    }


def test_wiki_batch_task_builds_worker_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    scopes = []

    def run(scope):
        scopes.append(scope)
        return BatchResult()

    monkeypatch.setattr(wiki_tasks, "run_batch_sync", run)

    wiki_tasks.wiki_batch_task.run(7, str(KB_ID))

    assert len(scopes) == 1
    assert scopes[0].tenant_id == 7
    assert scopes[0].knowledge_base_id == KB_ID
    assert scopes[0].actor_id == "wiki-worker"


def test_wiki_batch_task_retries_only_busy_batch_with_fixed_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retry_calls: list[dict[str, object]] = []

    def busy(_scope):
        raise WikiBatchBusy("KB 正在处理")

    def retry(**kwargs):
        retry_calls.append(kwargs)
        raise Retry()

    monkeypatch.setattr(wiki_tasks, "run_batch_sync", busy)
    monkeypatch.setattr(wiki_tasks.wiki_batch_task, "retry", retry)

    with pytest.raises(Retry):
        wiki_tasks.wiki_batch_task.run(7, str(KB_ID))

    assert len(retry_calls) == 1
    assert isinstance(retry_calls[0]["exc"], WikiBatchBusy)
    assert retry_calls[0]["countdown"] == 15
    assert retry_calls[0]["max_retries"] == 10


def test_wiki_batch_task_does_not_retry_other_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(_scope):
        raise RuntimeError("model failed")

    def unexpected_retry(**_kwargs):
        raise AssertionError("普通异常不应重试")

    monkeypatch.setattr(wiki_tasks, "run_batch_sync", fail)
    monkeypatch.setattr(wiki_tasks.wiki_batch_task, "retry", unexpected_retry)

    with pytest.raises(RuntimeError, match="model failed"):
        wiki_tasks.wiki_batch_task.run(7, str(KB_ID))


@pytest.mark.parametrize(
    "knowledge_base_id",
    [
        "not-a-uuid",
        "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
        f" {KB_ID}",
        123,
    ],
)
def test_wiki_batch_task_strictly_rejects_invalid_uuid_text(
    monkeypatch: pytest.MonkeyPatch,
    knowledge_base_id: object,
) -> None:
    monkeypatch.setattr(
        wiki_tasks,
        "run_batch_sync",
        lambda _scope: pytest.fail("非法 UUID 不应启动 runtime"),
    )

    with pytest.raises((TypeError, ValueError)):
        wiki_tasks.wiki_batch_task.run(7, knowledge_base_id)


@pytest.mark.parametrize("tenant_id", [0, -1, True, "7"])
def test_wiki_batch_task_rejects_invalid_tenant_id(
    monkeypatch: pytest.MonkeyPatch, tenant_id: object
) -> None:
    monkeypatch.setattr(
        wiki_tasks,
        "run_batch_sync",
        lambda _scope: pytest.fail("非法租户不应启动 runtime"),
    )

    with pytest.raises((TypeError, ValueError)):
        wiki_tasks.wiki_batch_task.run(tenant_id, str(KB_ID))


def test_wiki_batch_task_configuration_is_reliable() -> None:
    assert wiki_tasks.wiki_batch_task.name == "wiki.batch.run"
    assert wiki_tasks.wiki_batch_task.acks_late is True
    assert wiki_tasks.wiki_batch_task.reject_on_worker_lost is True


@pytest.mark.asyncio
async def test_build_runtime_creates_independent_batch_resources_and_disposes_own_engine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "wiki-fake.json"
    _write_fake_fixture(fixture)
    monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", str(fixture))
    monkeypatch.setenv("GRAPH_WIKI_LOCK_MODE", "memory")
    locks = object()
    engines = []
    session_factories = []

    class Engine:
        def __init__(self) -> None:
            self.dispose_count = 0

        async def dispose(self) -> None:
            self.dispose_count += 1

    def create_engine(_settings):
        engine = Engine()
        engines.append(engine)
        return engine

    def create_sessions(_engine):
        factory = object()
        session_factories.append(factory)
        return factory

    monkeypatch.setattr(wiki_tasks, "create_database_engine", create_engine, raising=False)
    monkeypatch.setattr(wiki_tasks, "create_session_factory", create_sessions, raising=False)
    monkeypatch.setattr(
        wiki_tasks, "build_lock_manager_from_env", lambda: locks, raising=False
    )

    first = wiki_tasks.build_runtime()
    second = wiki_tasks.build_runtime()

    assert isinstance(first, wiki_tasks.WikiTaskRuntime)
    assert isinstance(second, wiki_tasks.WikiTaskRuntime)
    assert first is not second
    assert first.engine is not second.engine
    assert first.worker is not second.worker
    assert first.enqueue is not second.enqueue
    assert first.worker._store is first.enqueue.store
    assert second.worker._store is second.enqueue.store
    assert first.worker._store is not second.worker._store
    assert first.worker._locks is second.worker._locks is locks
    assert len(session_factories) == 2

    await first.aclose()
    assert engines[0].dispose_count == 1
    assert engines[1].dispose_count == 0
    await second.aclose()
    assert engines[1].dispose_count == 1


@pytest.mark.parametrize("fixture_value", [None, "", "   "])
def test_build_runtime_requires_non_empty_fake_fixture_before_engine_creation(
    monkeypatch: pytest.MonkeyPatch,
    fixture_value: str | None,
) -> None:
    if fixture_value is None:
        monkeypatch.delenv("GRAPH_WIKI_FAKE_DATA_FILE", raising=False)
    else:
        monkeypatch.setenv("GRAPH_WIKI_FAKE_DATA_FILE", fixture_value)
    monkeypatch.setattr(
        wiki_tasks,
        "create_database_engine",
        lambda _settings: pytest.fail("fixture 缺失时不应创建数据库引擎"),
        raising=False,
    )

    with pytest.raises(RuntimeError, match="GRAPH_WIKI_FAKE_DATA_FILE"):
        wiki_tasks.build_runtime()


def test_run_batch_sync_reuses_loop_but_creates_and_closes_runtime_per_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wiki_tasks.close_batch_runner()
    build_loops = []
    worker_loops = []
    close_loops = []
    runtimes = []

    class Runtime:
        def __init__(self) -> None:
            self.worker = self
            self.close_count = 0

        async def run_batch(self, _scope):
            worker_loops.append(asyncio.get_running_loop())
            return BatchResult()

        async def aclose(self) -> None:
            self.close_count += 1
            close_loops.append(asyncio.get_running_loop())

    def build_runtime():
        build_loops.append(asyncio.get_running_loop())
        runtime = Runtime()
        runtimes.append(runtime)
        return runtime

    monkeypatch.setattr(wiki_tasks, "build_runtime", build_runtime)
    scope = wiki_tasks._build_scope(7, str(KB_ID))

    try:
        wiki_tasks.run_batch_sync(scope)
        thread = wiki_tasks._batch_runner.thread
        wiki_tasks.run_batch_sync(scope)

        assert thread is not None and thread.is_alive()
        assert build_loops == [worker_loops[0], worker_loops[0]]
        assert worker_loops == [worker_loops[0], worker_loops[0]]
        assert close_loops == [worker_loops[0], worker_loops[0]]
        assert len(runtimes) == 2
        assert runtimes[0] is not runtimes[1]
        assert [runtime.close_count for runtime in runtimes] == [1, 1]
    finally:
        wiki_tasks.close_batch_runner()

    assert thread is not None and not thread.is_alive()


def test_worker_error_closes_runtime_without_being_masked(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    wiki_tasks.close_batch_runner()
    worker_error = RuntimeError("worker failed")

    class Runtime:
        worker = None

        def __init__(self) -> None:
            self.worker = self
            self.close_count = 0

        async def run_batch(self, _scope):
            raise worker_error

        async def aclose(self) -> None:
            self.close_count += 1
            raise OSError("close failed")

    runtime = Runtime()
    monkeypatch.setattr(wiki_tasks, "build_runtime", lambda: runtime)

    try:
        with pytest.raises(RuntimeError, match="worker failed") as raised:
            wiki_tasks.run_batch_sync(wiki_tasks._build_scope(7, str(KB_ID)))
    finally:
        wiki_tasks.close_batch_runner()

    assert raised.value is worker_error
    assert runtime.close_count == 1
    assert "close failed" not in caplog.text
    assert caplog.records[-1].wiki_runtime_error_type == "OSError"


def test_successful_worker_propagates_runtime_close_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wiki_tasks.close_batch_runner()
    close_error = OSError("close failed")

    class Runtime:
        worker = None

        def __init__(self) -> None:
            self.worker = self

        async def run_batch(self, _scope):
            return BatchResult()

        async def aclose(self) -> None:
            raise close_error

    monkeypatch.setattr(wiki_tasks, "build_runtime", Runtime)

    try:
        with pytest.raises(OSError, match="close failed") as raised:
            wiki_tasks.run_batch_sync(wiki_tasks._build_scope(7, str(KB_ID)))
    finally:
        wiki_tasks.close_batch_runner()

    assert raised.value is close_error


def test_runtime_without_async_close_fails_before_worker_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wiki_tasks.close_batch_runner()
    worker_called = False

    class Runtime:
        worker = None

        def __init__(self) -> None:
            self.worker = self

        async def run_batch(self, _scope):
            nonlocal worker_called
            worker_called = True
            return BatchResult()

    monkeypatch.setattr(wiki_tasks, "build_runtime", Runtime)

    try:
        with pytest.raises(TypeError, match="async aclose"):
            wiki_tasks.run_batch_sync(wiki_tasks._build_scope(7, str(KB_ID)))
    finally:
        wiki_tasks.close_batch_runner()

    assert worker_called is False


def test_close_and_reopen_uses_new_loop_and_new_closed_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wiki_tasks.close_batch_runner()
    worker_loops = []
    runtimes = []

    class Runtime:
        def __init__(self) -> None:
            self.worker = self
            self.closed = False

        async def run_batch(self, _scope):
            worker_loops.append(asyncio.get_running_loop())
            return BatchResult()

        async def aclose(self) -> None:
            self.closed = True

    def build_runtime():
        runtime = Runtime()
        runtimes.append(runtime)
        return runtime

    monkeypatch.setattr(wiki_tasks, "build_runtime", build_runtime)
    scope = wiki_tasks._build_scope(7, str(KB_ID))

    try:
        wiki_tasks.run_batch_sync(scope)
        wiki_tasks.close_batch_runner()
        wiki_tasks.run_batch_sync(scope)
    finally:
        wiki_tasks.close_batch_runner()

    assert worker_loops[0] is not worker_loops[1]
    assert len(runtimes) == 2
    assert runtimes[0] is not runtimes[1]
    assert all(runtime.closed for runtime in runtimes)


def test_process_runner_rebuilds_loop_when_pid_changes() -> None:
    current_pid = [100]
    runner = wiki_tasks.ProcessEventLoopRunner(pid_provider=lambda: current_pid[0])

    async def current_loop():
        return asyncio.get_running_loop()

    try:
        first_loop = runner.run(current_loop)
        first_thread = runner.thread
        current_pid[0] = 101
        second_loop = runner.run(current_loop)

        assert second_loop is not first_loop
        assert first_thread is not None and not first_thread.is_alive()
    finally:
        runner.close()


def test_pid_change_runs_new_closed_runtime_on_new_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_pid = [100]
    runner = wiki_tasks.ProcessEventLoopRunner(pid_provider=lambda: current_pid[0])
    loops = []
    runtimes = []

    class Runtime:
        def __init__(self) -> None:
            self.worker = self
            self.closed = False

        async def run_batch(self, _scope):
            loops.append(asyncio.get_running_loop())
            return BatchResult()

        async def aclose(self) -> None:
            self.closed = True

    def build_runtime():
        runtime = Runtime()
        runtimes.append(runtime)
        return runtime

    monkeypatch.setattr(wiki_tasks, "build_runtime", build_runtime)
    scope = wiki_tasks._build_scope(7, str(KB_ID))

    try:
        runner.run(lambda: wiki_tasks._run_batch_on_worker_loop(scope))
        current_pid[0] = 101
        runner.run(lambda: wiki_tasks._run_batch_on_worker_loop(scope))
    finally:
        runner.close()

    assert loops[0] is not loops[1]
    assert len(runtimes) == 2
    assert all(runtime.closed for runtime in runtimes)


def test_runner_close_cancels_active_batch_and_closes_runtime(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = wiki_tasks.ProcessEventLoopRunner()
    worker_started = threading.Event()
    runtime_closed = threading.Event()

    class Runtime:
        def __init__(self) -> None:
            self.worker = self

        async def run_batch(self, _scope):
            worker_started.set()
            await asyncio.Event().wait()

        async def aclose(self) -> None:
            runtime_closed.set()
            raise OSError("cancel close failed")

    monkeypatch.setattr(wiki_tasks, "build_runtime", Runtime)
    scope = wiki_tasks._build_scope(7, str(KB_ID))
    errors: list[BaseException] = []

    def run() -> None:
        try:
            runner.run(lambda: wiki_tasks._run_batch_on_worker_loop(scope))
        except BaseException as exc:
            errors.append(exc)

    caller = threading.Thread(target=run)
    caller.start()
    assert worker_started.wait(timeout=2)
    runner.close()
    caller.join(timeout=2)

    assert not caller.is_alive()
    assert runtime_closed.is_set()
    assert len(errors) == 1
    assert isinstance(errors[0], asyncio.CancelledError)
    assert caplog.records[-1].wiki_runtime_error_type == "OSError"
