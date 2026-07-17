from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from app.infrastructure.database.config import DatabaseSettings
from app.wiki.ingest.store import InvariantError, OutboxEventRecord
from app.wiki.tasks import outbox_dispatcher
from app.wiki.tasks.outbox_dispatcher import (
    CeleryBatchPublisher,
    DispatcherRuntime,
    OutboxDispatcher,
    OutboxDispatcherSettings,
    _run_main,
    build_dispatcher_runtime,
    run_dispatcher_loop,
)


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
NOW = datetime(2026, 7, 17, 12, tzinfo=UTC)


def _event(
    *,
    event_id: UUID | None = None,
    event_type: str = "wiki.batch.trigger",
    claim_token: UUID | None = None,
    payload: dict[str, object] | None = None,
) -> OutboxEventRecord:
    return OutboxEventRecord(
        id=event_id or uuid4(),
        tenant_id=7,
        knowledge_base_id=KB_ID,
        event_type=event_type,
        dedup_key="d" * 64,
        payload=payload
        or {"tenant_id": 7, "knowledge_base_id": str(KB_ID)},
        available_at=NOW,
        claimed_at=NOW,
        claim_token=claim_token,
        attempts=1,
        sent_at=None,
    )


class MemoryOutboxStore:
    def __init__(
        self,
        events: list[OutboxEventRecord],
        *,
        release_error: BaseException | None = None,
    ) -> None:
        self.events = events
        self.release_error = release_error
        self.calls: list[tuple[object, ...]] = []

    async def claim_outbox(self, limit: int, claim_timeout: timedelta | int):
        self.calls.append(("claim", limit, claim_timeout))
        return list(self.events)

    async def mark_outbox_sent(self, ids, claim_token):
        self.calls.append(("mark", list(ids), claim_token))

    async def release_outbox(self, ids, claim_token):
        self.calls.append(("release", list(ids), claim_token))
        if self.release_error is not None:
            raise self.release_error


class RecordingPublisher:
    def __init__(self, fail_at: int | None = None) -> None:
        self.events: list[OutboxEventRecord] = []
        self.fail_at = fail_at

    async def publish(self, event: OutboxEventRecord) -> None:
        if self.fail_at == len(self.events):
            raise RuntimeError("broker unavailable")
        self.events.append(event)


@pytest.mark.asyncio
async def test_dispatcher_marks_whole_batch_sent_only_after_ordered_publish() -> None:
    token = uuid4()
    events = [_event(claim_token=token), _event(claim_token=token)]
    store = MemoryOutboxStore(events)
    publisher = RecordingPublisher()

    sent = await OutboxDispatcher(
        store, publisher, batch_size=2, claim_timeout=60
    ).dispatch_once()

    assert sent == 2
    assert publisher.events == events
    assert store.calls == [
        ("claim", 2, 60),
        ("mark", [events[0].id], token),
        ("mark", [events[1].id], token),
    ]


@pytest.mark.asyncio
async def test_dispatcher_empty_batch_returns_zero_without_confirmation() -> None:
    store = MemoryOutboxStore([])

    sent = await OutboxDispatcher(store, RecordingPublisher()).dispatch_once()

    assert sent == 0
    assert store.calls == [("claim", 100, 60)]


@pytest.mark.asyncio
async def test_dispatcher_isolates_bad_event_and_retries_only_that_event() -> None:
    token = uuid4()
    events = [
        _event(claim_token=token),
        _event(claim_token=token),
        _event(claim_token=token),
    ]
    store = MemoryOutboxStore(events)
    failure = RuntimeError("broker unavailable")

    class FailSecondOncePublisher:
        def __init__(self) -> None:
            self.calls: list[UUID] = []
            self.failed = False

        async def publish(self, event: OutboxEventRecord) -> None:
            self.calls.append(event.id)
            if event.id == events[1].id and not self.failed:
                self.failed = True
                raise failure

    publisher = FailSecondOncePublisher()

    with pytest.raises(RuntimeError, match="broker unavailable") as raised:
        await OutboxDispatcher(store, publisher, batch_size=3).dispatch_once()

    assert raised.value is failure
    assert publisher.calls == [event.id for event in events]
    assert store.calls == [
        ("claim", 3, 60),
        ("mark", [events[0].id], token),
        ("release", [events[1].id], token),
        ("mark", [events[2].id], token),
    ]

    store.events = [events[1]]
    assert await OutboxDispatcher(store, publisher).dispatch_once() == 1
    assert publisher.calls.count(events[0].id) == 1
    assert publisher.calls.count(events[1].id) == 2
    assert publisher.calls.count(events[2].id) == 1
    assert store.calls[-1] == ("mark", [events[1].id], token)


@pytest.mark.asyncio
async def test_dispatcher_preserves_publish_error_when_release_also_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = uuid4()
    event = _event(claim_token=token, payload={"tenant_id": 7, "secret": "do-not-log"})
    store = MemoryOutboxStore([event], release_error=OSError("database unavailable"))

    with pytest.raises(RuntimeError, match="broker unavailable"):
        await OutboxDispatcher(store, RecordingPublisher(0)).dispatch_once()

    assert "database unavailable" not in caplog.text
    assert "do-not-log" not in caplog.text
    assert "outbox_claim_token" not in caplog.text
    assert str(token) not in caplog.text
    assert all("outbox_claim_token" not in record.__dict__ for record in caplog.records)
    assert caplog.records[-1].outbox_error_type == "OSError"


@pytest.mark.asyncio
async def test_dispatcher_releases_batch_and_propagates_publish_cancellation() -> None:
    token = uuid4()
    events = [
        _event(claim_token=token),
        _event(claim_token=token),
        _event(claim_token=token),
    ]
    store = MemoryOutboxStore(events)

    class CancelledPublisher:
        def __init__(self) -> None:
            self.calls: list[UUID] = []

        async def publish(self, event: OutboxEventRecord) -> None:
            self.calls.append(event.id)
            if event.id == events[1].id:
                raise asyncio.CancelledError

    publisher = CancelledPublisher()

    with pytest.raises(asyncio.CancelledError):
        await OutboxDispatcher(store, publisher).dispatch_once()

    assert publisher.calls == [events[0].id, events[1].id]
    assert store.calls == [
        ("claim", 100, 60),
        ("mark", [events[0].id], token),
        ("release", [events[1].id, events[2].id], token),
    ]


@pytest.mark.asyncio
async def test_dispatcher_does_not_swallow_new_cancellation_during_release() -> None:
    token = uuid4()
    event = _event(claim_token=token)
    store = MemoryOutboxStore([event], release_error=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await OutboxDispatcher(store, RecordingPublisher(0)).dispatch_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("tokens", [[None], [uuid4(), uuid4()]])
async def test_dispatcher_rejects_missing_or_mixed_claim_tokens_before_publish(
    tokens: list[UUID | None],
) -> None:
    events = [_event(claim_token=token) for token in tokens]
    publisher = RecordingPublisher()
    store = MemoryOutboxStore(events)

    with pytest.raises(InvariantError, match="claim token"):
        await OutboxDispatcher(store, publisher).dispatch_once()

    assert publisher.events == []
    assert all(call[0] not in {"mark", "release"} for call in store.calls)


@pytest.mark.asyncio
async def test_dispatcher_does_not_release_when_mark_sent_fails() -> None:
    token = uuid4()
    event = _event(claim_token=token)

    class MarkFailingStore(MemoryOutboxStore):
        async def mark_outbox_sent(self, ids, claim_token):
            await super().mark_outbox_sent(ids, claim_token)
            raise RuntimeError("commit uncertain")

    store = MarkFailingStore([event])

    with pytest.raises(RuntimeError, match="commit uncertain"):
        await OutboxDispatcher(store, RecordingPublisher()).dispatch_once()

    assert all(call[0] != "release" for call in store.calls)


@pytest.mark.asyncio
async def test_dispatcher_continues_after_mark_failure_without_releasing_published_event() -> None:
    token = uuid4()
    events = [
        _event(claim_token=token),
        _event(claim_token=token),
        _event(claim_token=token),
    ]
    failure = RuntimeError("commit uncertain")

    class OneMarkFailingStore(MemoryOutboxStore):
        async def mark_outbox_sent(self, ids, claim_token):
            await super().mark_outbox_sent(ids, claim_token)
            if list(ids) == [events[1].id]:
                raise failure

    store = OneMarkFailingStore(events)
    publisher = RecordingPublisher()

    with pytest.raises(RuntimeError, match="commit uncertain") as raised:
        await OutboxDispatcher(store, publisher).dispatch_once()

    assert raised.value is failure
    assert publisher.events == events
    assert all(call[0] != "release" for call in store.calls)
    assert store.calls[-1] == ("mark", [events[2].id], token)


class RecordingTask:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def apply_async(self, **kwargs):
        self.calls.append(kwargs)
        return object()


@pytest.mark.asyncio
async def test_celery_publisher_uses_to_thread_task_id_and_scope_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = RecordingTask()
    thread_calls: list[tuple[object, tuple[object, ...], dict[str, object]]] = []

    async def fake_to_thread(function, /, *args, **kwargs):
        thread_calls.append((function, args, kwargs))
        return function(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", fake_to_thread)
    event = _event(claim_token=uuid4())

    await CeleryBatchPublisher(task=task).publish(event)

    assert thread_calls == [
        (
            task.apply_async,
            (),
            {
                "kwargs": {
                    "tenant_id": 7,
                    "knowledge_base_id": str(KB_ID),
                },
                "task_id": str(event.id),
            },
        )
    ]


@pytest.mark.asyncio
async def test_cancelled_thread_publish_may_finish_after_dispatcher_releases_claim() -> None:
    token = uuid4()
    events = [_event(claim_token=token), _event(claim_token=token)]
    store = MemoryOutboxStore(events)
    started = threading.Event()
    allow_finish = threading.Event()
    finished = threading.Event()
    task_calls: list[dict[str, object]] = []

    class BlockingTask:
        def apply_async(self, **kwargs):
            task_calls.append(kwargs)
            started.set()
            allow_finish.wait(timeout=5)
            finished.set()

    dispatch = asyncio.create_task(
        OutboxDispatcher(store, CeleryBatchPublisher(task=BlockingTask())).dispatch_once()
    )
    assert await asyncio.to_thread(started.wait, 2)
    dispatch.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await dispatch
        assert store.calls[-1] == (
            "release",
            [events[0].id, events[1].id],
            token,
        )
    finally:
        allow_finish.set()

    assert await asyncio.to_thread(finished.wait, 2)
    assert task_calls == [
        {
            "kwargs": {
                "tenant_id": 7,
                "knowledge_base_id": str(KB_ID),
            },
            "task_id": str(events[0].id),
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event",
    [
        _event(event_type="other", claim_token=uuid4()),
        _event(claim_token=uuid4(), payload={"tenant_id": 7}),
        _event(
            claim_token=uuid4(),
            payload={
                "tenant_id": 7,
                "knowledge_base_id": str(KB_ID),
                "extra": True,
            },
        ),
        _event(
            claim_token=uuid4(),
            payload={"tenant_id": 0, "knowledge_base_id": str(KB_ID)},
        ),
        _event(
            claim_token=uuid4(),
            payload={"tenant_id": True, "knowledge_base_id": str(KB_ID)},
        ),
        _event(
            claim_token=uuid4(),
            payload={"tenant_id": 7, "knowledge_base_id": "not-a-uuid"},
        ),
        _event(
            claim_token=uuid4(),
            payload={"tenant_id": 8, "knowledge_base_id": str(KB_ID)},
        ),
        _event(
            claim_token=uuid4(),
            payload={"tenant_id": 7, "knowledge_base_id": str(uuid4())},
        ),
    ],
)
async def test_celery_publisher_rejects_invalid_event_or_payload(
    event: OutboxEventRecord,
) -> None:
    task = RecordingTask()

    with pytest.raises(ValueError):
        await CeleryBatchPublisher(task=task).publish(event)

    assert task.calls == []


@pytest.mark.asyncio
async def test_dispatch_loop_continues_after_error_and_sleeps_every_round() -> None:
    class Dispatcher:
        def __init__(self) -> None:
            self.calls = 0

        async def dispatch_once(self) -> int:
            self.calls += 1
            if self.calls <= 2:
                raise RuntimeError("temporary")
            if self.calls == 4:
                raise asyncio.CancelledError
            return 0

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    dispatcher = Dispatcher()
    with pytest.raises(asyncio.CancelledError):
        await run_dispatcher_loop(
            dispatcher,
            poll_seconds=0.25,
            max_backoff_seconds=1,
            sleep=fake_sleep,
        )

    assert dispatcher.calls == 4
    assert sleeps == [0.25, 0.5, 0.25]


@pytest.mark.asyncio
@pytest.mark.parametrize("poll_seconds", [0, -1, float("nan"), float("inf"), True])
async def test_dispatch_loop_rejects_invalid_direct_poll_interval(
    poll_seconds: object,
) -> None:
    class Dispatcher:
        async def dispatch_once(self) -> int:
            pytest.fail("非法间隔不应启动循环")

    with pytest.raises(ValueError, match="poll_seconds"):
        await run_dispatcher_loop(  # type: ignore[arg-type]
            Dispatcher(), poll_seconds=poll_seconds
        )


@pytest.mark.parametrize("batch_size", [True, 1.5, "2"])
def test_dispatcher_rejects_non_integer_batch_size(batch_size: object) -> None:
    with pytest.raises(ValueError, match="batch_size"):
        OutboxDispatcher(  # type: ignore[arg-type]
            MemoryOutboxStore([]), RecordingPublisher(), batch_size=batch_size
        )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("GRAPH_WIKI_OUTBOX_BATCH_SIZE", "0"),
        ("GRAPH_WIKI_OUTBOX_BATCH_SIZE", "1001"),
        ("GRAPH_WIKI_OUTBOX_POLL_SECONDS", "0"),
        ("GRAPH_WIKI_OUTBOX_POLL_SECONDS", "not-a-number"),
        ("GRAPH_WIKI_OUTBOX_CLAIM_TIMEOUT_SECONDS", "0"),
        ("GRAPH_WIKI_OUTBOX_CLAIM_TIMEOUT_SECONDS", "86401"),
    ],
)
def test_dispatcher_settings_fail_fast_on_invalid_environment(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        OutboxDispatcherSettings.from_env()


def test_runtime_builder_wires_postgres_store_without_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = SimpleNamespace()
    session_factory = SimpleNamespace()
    monkeypatch.setattr(outbox_dispatcher, "create_database_engine", lambda _settings: engine)
    monkeypatch.setattr(
        outbox_dispatcher, "create_session_factory", lambda actual: session_factory
    )
    settings = OutboxDispatcherSettings(
        batch_size=25,
        poll_seconds=0.5,
        claim_timeout_seconds=120,
    )
    database_settings = DatabaseSettings.from_env()
    task = RecordingTask()

    runtime = build_dispatcher_runtime(
        settings=settings, database_settings=database_settings, task=task
    )

    assert runtime.engine is engine
    assert runtime.poll_seconds == 0.5
    assert runtime.dispatcher.batch_size == 25
    assert runtime.dispatcher.claim_timeout == 120
    assert runtime.dispatcher.store._session_factory is session_factory
    assert runtime.dispatcher.publisher.task is task


@pytest.mark.asyncio
async def test_dispatcher_main_disposes_engine_when_loop_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Engine:
        def __init__(self) -> None:
            self.disposed = False

        async def dispose(self) -> None:
            self.disposed = True

    engine = Engine()
    runtime = DispatcherRuntime(
        engine=engine,  # type: ignore[arg-type]
        dispatcher=SimpleNamespace(),  # type: ignore[arg-type]
        poll_seconds=1,
    )

    async def cancelled_loop(*_args, **_kwargs) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(outbox_dispatcher, "run_dispatcher_loop", cancelled_loop)

    with pytest.raises(asyncio.CancelledError):
        await _run_main(lambda: runtime)

    assert engine.disposed is True
