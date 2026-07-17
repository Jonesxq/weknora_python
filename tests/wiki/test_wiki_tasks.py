from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import UUID

import pytest
from celery.exceptions import Retry

from app.wiki.ingest.errors import WikiBatchBusy
from app.wiki.ingest.schemas import BatchResult
from app.wiki.tasks import wiki_tasks


KB_ID = UUID("11111111-1111-1111-1111-111111111111")


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


def test_runtime_placeholder_fails_fast_until_task_ten_assembly() -> None:
    try:
        with pytest.raises(RuntimeError, match="任务 10"):
            wiki_tasks.run_batch_sync(
                wiki_tasks._build_scope(7, str(KB_ID))
            )
    finally:
        wiki_tasks.close_batch_runner()


def test_run_batch_sync_reuses_one_loop_and_closes_background_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wiki_tasks.close_batch_runner()
    build_loops = []
    worker_loops = []

    class Worker:
        async def run_batch(self, _scope):
            worker_loops.append(asyncio.get_running_loop())
            return BatchResult()

    def build_runtime():
        build_loops.append(asyncio.get_running_loop())
        return SimpleNamespace(worker=Worker())

    monkeypatch.setattr(wiki_tasks, "build_runtime", build_runtime)
    scope = wiki_tasks._build_scope(7, str(KB_ID))

    try:
        wiki_tasks.run_batch_sync(scope)
        thread = wiki_tasks._batch_runner.thread
        wiki_tasks.run_batch_sync(scope)

        assert thread is not None and thread.is_alive()
        assert build_loops == [worker_loops[0], worker_loops[0]]
        assert worker_loops == [worker_loops[0], worker_loops[0]]
    finally:
        wiki_tasks.close_batch_runner()

    assert thread is not None and not thread.is_alive()


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
