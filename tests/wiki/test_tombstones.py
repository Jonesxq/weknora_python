"""删除墓碑适配器的行为测试。"""

from __future__ import annotations

import asyncio
import math
import os
from collections.abc import Callable
from uuid import UUID, uuid4

import pytest
from redis.asyncio import Redis

from app.wiki.ingest.ports import TombstonePort
from app.wiki.scope import WikiScope
from app.wiki.tasks.tombstones import MemoryTombstones, RedisTombstones, tombstone_key


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
OTHER_KB_ID = UUID("22222222-2222-2222-2222-222222222222")
TEST_REDIS_URL = os.getenv("GRAPH_TEST_REDIS_URL")


def scope(*, tenant_id: int = 7, knowledge_base_id: UUID = KB_ID) -> WikiScope:
    return WikiScope(
        tenant_id=tenant_id, knowledge_base_id=knowledge_base_id, actor_id="worker"
    )


class Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class FakeRedisClient:
    def __init__(self, *, set_result: object = True, get_result: object = None) -> None:
        self.set_result = set_result
        self.get_result = get_result
        self.set_error: BaseException | None = None
        self.get_error: BaseException | None = None
        self.close_error: BaseException | None = None
        self.calls: list[tuple[object, ...]] = []
        self.closed = 0

    async def set(self, key: str, value: str, *, ex: int) -> object:
        self.calls.append(("set", key, value, ex))
        if self.set_error is not None:
            raise self.set_error
        return self.set_result

    async def get(self, key: str) -> object:
        self.calls.append(("get", key))
        if self.get_error is not None:
            raise self.get_error
        return self.get_result

    async def aclose(self) -> None:
        self.closed += 1
        if self.close_error is not None:
            raise self.close_error


class FakeFactory:
    def __init__(self, make_client: Callable[[], FakeRedisClient]) -> None:
        self.make_client = make_client
        self.kwargs: list[dict[str, object]] = []
        self.clients: list[FakeRedisClient] = []

    def __call__(self, url: str, **kwargs: object) -> FakeRedisClient:
        assert url == "redis://example"
        self.kwargs.append(kwargs)
        client = self.make_client()
        self.clients.append(client)
        return client


class SharedRedisState:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.values: dict[str, tuple[str, float]] = {}


class StatefulFakeRedisClient(FakeRedisClient):
    def __init__(self, state: SharedRedisState) -> None:
        super().__init__()
        self.state = state

    async def set(self, key: str, value: str, *, ex: int) -> object:
        self.calls.append(("set", key, value, ex))
        self.state.values[key] = (value, self.state.clock() + ex)
        return True

    async def get(self, key: str) -> object:
        self.calls.append(("get", key))
        value = self.state.values.get(key)
        if value is None or self.state.clock() >= value[1]:
            self.state.values.pop(key, None)
            return None
        return value[0]


class AwaitableRedisClient(FakeRedisClient):
    """模拟 redis.asyncio.Redis：实例可 await，同时直接提供命令方法。"""

    def __await__(self):
        async def resolve() -> AwaitableRedisClient:
            return self

        return resolve().__await__()


class BlockingCloseClient(FakeRedisClient):
    def __init__(self, *, get_result: object = None) -> None:
        super().__init__(get_result=get_result)
        self.close_started = asyncio.Event()
        self.close_release = asyncio.Event()
        self.close_finished = False

    async def aclose(self) -> None:
        self.closed += 1
        self.close_started.set()
        await self.close_release.wait()
        self.close_finished = True


async def _wait_for_close_start(client: BlockingCloseClient) -> None:
    await asyncio.wait_for(client.close_started.wait(), timeout=1)


def _pending_close_tasks() -> list[asyncio.Task[object]]:
    return [
        task
        for task in asyncio.all_tasks()
        if task.get_name() == "wiki-tombstone-close" and not task.done()
    ]


def test_key_uses_kb_identity_without_changing_knowledge_id() -> None:
    assert tombstone_key(scope(), "knowledge-1") == f"wiki:deleted:{KB_ID}:knowledge-1"
    assert tombstone_key(scope(tenant_id=8), "knowledge-1") == tombstone_key(
        scope(), "knowledge-1"
    )
    assert tombstone_key(
        scope(knowledge_base_id=OTHER_KB_ID), "knowledge-1"
    ) != tombstone_key(scope(), "knowledge-1")


@pytest.mark.parametrize(
    "knowledge_id",
    [
        "",
        "   ",
        " knowledge-1",
        "knowledge-1 ",
        "a\nb",
        "a\tb",
        "a\x7fb",
        "a\x80b",
        "a\x81b",
        "a\x9fb",
    ],
)
def test_key_rejects_ambiguous_knowledge_id(knowledge_id: str) -> None:
    with pytest.raises(ValueError):
        tombstone_key(scope(), knowledge_id)


def test_key_allows_normal_unicode_knowledge_id() -> None:
    assert tombstone_key(scope(), "知识-Δ-文档") == f"wiki:deleted:{KB_ID}:知识-Δ-文档"


@pytest.mark.asyncio
async def test_memory_marks_refreshes_and_expires_at_exact_boundary() -> None:
    clock = Clock()
    tombstones = MemoryTombstones(ttl_seconds=10, clock=clock)
    assert isinstance(tombstones, TombstonePort)
    await tombstones.mark_deleted(scope(), "knowledge-1")
    assert await tombstones.is_deleted(scope(), "knowledge-1")
    clock.now = 109.0
    await tombstones.mark_deleted(scope(), "knowledge-1")
    clock.now = 110.0
    assert await tombstones.is_deleted(scope(), "knowledge-1")
    clock.now = 118.0
    assert await tombstones.is_deleted(scope(), "knowledge-1")
    clock.now = 119.0
    assert not await tombstones.is_deleted(scope(), "knowledge-1")


@pytest.mark.asyncio
async def test_memory_separates_kbs_but_shares_same_kb_across_tenants() -> None:
    tombstones = MemoryTombstones(clock=Clock())
    await tombstones.mark_deleted(scope(), "knowledge-1")
    assert await tombstones.is_deleted(scope(tenant_id=9), "knowledge-1")
    assert not await tombstones.is_deleted(
        scope(knowledge_base_id=OTHER_KB_ID), "knowledge-1"
    )
    assert not await tombstones.is_deleted(scope(), "knowledge-2")


@pytest.mark.asyncio
async def test_memory_uses_one_hour_default_ttl() -> None:
    clock = Clock()
    tombstones = MemoryTombstones(clock=clock)
    await tombstones.mark_deleted(scope(), "knowledge-1")
    clock.now = 3699.0
    assert await tombstones.is_deleted(scope(), "knowledge-1")
    clock.now = 3700.0
    assert not await tombstones.is_deleted(scope(), "knowledge-1")


@pytest.mark.asyncio
async def test_memory_purges_expired_entries_during_unrelated_write() -> None:
    clock = Clock()
    tombstones = MemoryTombstones(ttl_seconds=1, clock=clock)
    for index in range(10_000):
        await tombstones.mark_deleted(scope(), f"old-{index}")
    clock.now = 101.0
    await tombstones.mark_deleted(scope(), "current")
    assert tombstones._expires_at == {tombstone_key(scope(), "current"): 102.0}


@pytest.mark.asyncio
async def test_memory_refresh_stale_heap_entry_does_not_delete_current_tombstone() -> (
    None
):
    clock = Clock()
    tombstones = MemoryTombstones(ttl_seconds=10, clock=clock)
    await tombstones.mark_deleted(scope(), "knowledge-1")
    clock.now = 105.0
    await tombstones.mark_deleted(scope(), "knowledge-1")
    clock.now = 110.0
    await tombstones.mark_deleted(scope(), "other")
    assert await tombstones.is_deleted(scope(), "knowledge-1")


@pytest.mark.parametrize("ttl", [True, False, 0, -1, 1.0, "1"])
def test_memory_rejects_invalid_ttl(ttl: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        MemoryTombstones(ttl_seconds=ttl)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_redis_creates_and_closes_a_client_for_every_call() -> None:
    factory = FakeFactory(lambda: FakeRedisClient(get_result="1"))
    tombstones = RedisTombstones(
        "redis://example", ttl_seconds=30, socket_timeout=2.5, client_factory=factory
    )
    assert isinstance(tombstones, TombstonePort)
    await tombstones.mark_deleted(scope(), "knowledge-1")
    assert await tombstones.is_deleted(scope(), "knowledge-1")
    assert len(factory.clients) == 2
    assert all(client.closed == 1 for client in factory.clients)
    assert factory.clients[0].calls == [
        ("set", f"wiki:deleted:{KB_ID}:knowledge-1", "1", 30)
    ]
    assert factory.clients[1].calls == [("get", f"wiki:deleted:{KB_ID}:knowledge-1")]
    assert factory.kwargs == [
        {
            "decode_responses": True,
            "socket_connect_timeout": 2.5,
            "socket_timeout": 2.5,
        },
        {
            "decode_responses": True,
            "socket_connect_timeout": 2.5,
            "socket_timeout": 2.5,
        },
    ]


def test_redis_refreshes_ttl_and_works_across_independent_event_loops() -> None:
    factory = FakeFactory(FakeRedisClient)
    tombstones = RedisTombstones("redis://example", client_factory=factory)

    async def run_once() -> None:
        await asyncio.wait_for(
            tombstones.mark_deleted(scope(), "knowledge-1"), timeout=1
        )

    asyncio.run(run_once())
    asyncio.run(run_once())
    assert len(factory.clients) == 2
    assert all(
        client.calls[-1][-1] == 3600 and client.closed == 1
        for client in factory.clients
    )
    assert factory.kwargs == [
        {
            "decode_responses": True,
            "socket_connect_timeout": 2.0,
            "socket_timeout": 2.0,
        },
        {
            "decode_responses": True,
            "socket_connect_timeout": 2.0,
            "socket_timeout": 2.0,
        },
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("set_result", [False, None])
async def test_redis_rejects_unsuccessful_writes_and_closes_client(
    set_result: object,
) -> None:
    factory = FakeFactory(lambda: FakeRedisClient(set_result=set_result))
    with pytest.raises(RuntimeError, match="写入删除墓碑失败"):
        await RedisTombstones("redis://example", client_factory=factory).mark_deleted(
            scope(), "knowledge-1"
        )
    assert factory.clients[0].closed == 1


@pytest.mark.asyncio
async def test_redis_rejects_corrupt_value_and_propagates_operation_errors() -> None:
    corrupt = FakeFactory(lambda: FakeRedisClient(get_result="unexpected"))
    with pytest.raises(RuntimeError, match="删除墓碑数据损坏"):
        await RedisTombstones("redis://example", client_factory=corrupt).is_deleted(
            scope(), "knowledge-1"
        )
    assert corrupt.clients[0].closed == 1
    failed = FakeFactory(FakeRedisClient)
    failed_client = failed.make_client()
    failed.make_client = lambda: failed_client
    failed_client.set_error = ConnectionError("redis down")
    with pytest.raises(ConnectionError, match="redis down"):
        await RedisTombstones("redis://example", client_factory=failed).mark_deleted(
            scope(), "knowledge-1"
        )
    assert failed_client.closed == 1
    failed_get = FakeFactory(FakeRedisClient)
    failed_get_client = failed_get.make_client()
    failed_get.make_client = lambda: failed_get_client
    failed_get_client.get_error = ConnectionError("redis read down")
    with pytest.raises(ConnectionError, match="redis read down"):
        await RedisTombstones("redis://example", client_factory=failed_get).is_deleted(
            scope(), "knowledge-1"
        )
    assert failed_get_client.closed == 1


@pytest.mark.asyncio
async def test_redis_returns_false_for_missing_key_and_factory_failure_has_no_client() -> (
    None
):
    missing = FakeFactory(FakeRedisClient)
    assert not await RedisTombstones(
        "redis://example", client_factory=missing
    ).is_deleted(scope(), "knowledge-1")
    assert missing.clients[0].closed == 1

    def failing_factory(*args: object, **kwargs: object) -> FakeRedisClient:
        raise ConnectionError("factory down")

    with pytest.raises(ConnectionError, match="factory down"):
        await RedisTombstones(
            "redis://example", client_factory=failing_factory
        ).is_deleted(scope(), "knowledge-1")


@pytest.mark.asyncio
async def test_redis_closes_on_cancellation_and_preserves_primary_error_over_close_error() -> (
    None
):
    cancelled = FakeFactory(FakeRedisClient)
    cancelled_client = cancelled.make_client()
    cancelled.make_client = lambda: cancelled_client
    cancelled_client.get_error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await RedisTombstones("redis://example", client_factory=cancelled).is_deleted(
            scope(), "knowledge-1"
        )
    assert cancelled_client.closed == 1
    failed = FakeFactory(FakeRedisClient)
    failed_client = failed.make_client()
    failed.make_client = lambda: failed_client
    failed_client.set_error = ConnectionError("primary")
    failed_client.close_error = OSError("close")
    with pytest.raises(ConnectionError, match="primary"):
        await RedisTombstones("redis://example", client_factory=failed).mark_deleted(
            scope(), "knowledge-1"
        )
    assert failed_client.closed == 1


@pytest.mark.asyncio
async def test_redis_propagates_close_failure_when_operation_succeeds() -> None:
    factory = FakeFactory(FakeRedisClient)
    client = factory.make_client()
    factory.make_client = lambda: client
    client.close_error = OSError("close")
    with pytest.raises(OSError, match="close"):
        await RedisTombstones("redis://example", client_factory=factory).mark_deleted(
            scope(), "knowledge-1"
        )
    assert client.closed == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["mark", "read"])
async def test_redis_sync_client_preserves_primary_await_type_error(
    operation: str,
) -> None:
    class SyncClient:
        def __init__(self) -> None:
            self.closed = 0

        def set(self, *_args: object, **_kwargs: object) -> bool:
            return True

        def get(self, *_args: object, **_kwargs: object) -> None:
            return None

        def aclose(self) -> None:
            self.closed += 1

    client = SyncClient()
    tombstones = RedisTombstones(
        "redis://example", client_factory=lambda *_args, **_kwargs: client
    )

    with pytest.raises(TypeError) as error:
        if operation == "mark":
            await tombstones.mark_deleted(scope(), "knowledge-1")
        else:
            await tombstones.is_deleted(scope(), "knowledge-1")

    assert "can't be used in 'await' expression" in str(error.value)
    assert client.closed == 1


@pytest.mark.asyncio
async def test_redis_primary_error_wins_when_aclose_awaitable_creation_fails() -> None:
    class SyncFailingCloseClient(FakeRedisClient):
        def aclose(self) -> None:
            self.closed += 1
            raise OSError("close setup failed")

    client = SyncFailingCloseClient()
    client.set_error = ConnectionError("primary")

    with pytest.raises(ConnectionError, match="primary"):
        await RedisTombstones(
            "redis://example", client_factory=FakeFactory(lambda: client)
        ).mark_deleted(scope(), "knowledge-1")

    assert client.closed == 1


@pytest.mark.asyncio
async def test_redis_cancellation_wins_when_aclose_awaitable_creation_fails() -> None:
    class SyncFailingCloseClient(FakeRedisClient):
        def aclose(self) -> None:
            self.closed += 1
            raise OSError("close setup failed")

    client = SyncFailingCloseClient()
    client.get_error = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await RedisTombstones(
            "redis://example", client_factory=FakeFactory(lambda: client)
        ).is_deleted(scope(), "knowledge-1")

    assert client.closed == 1


@pytest.mark.asyncio
async def test_redis_requires_exact_true_set_result() -> None:
    for written in (False, None, 0, 1, "", object()):
        factory = FakeFactory(lambda: FakeRedisClient(set_result=written))
        with pytest.raises(RuntimeError, match="写入删除墓碑失败"):
            await RedisTombstones(
                "redis://example", client_factory=factory
            ).mark_deleted(scope(), "knowledge-1")
        assert factory.clients[0].closed == 1

    accepted = FakeFactory(lambda: FakeRedisClient(set_result=True))
    await RedisTombstones("redis://example", client_factory=accepted).mark_deleted(
        scope(), "knowledge-1"
    )


@pytest.mark.asyncio
async def test_redis_cancellation_waits_for_close_and_leaves_no_close_task() -> None:
    client = BlockingCloseClient()
    factory = FakeFactory(lambda: client)
    task = asyncio.create_task(
        RedisTombstones("redis://example", client_factory=factory).mark_deleted(
            scope(), "knowledge-1"
        )
    )
    await _wait_for_close_start(client)
    try:
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
    finally:
        client.close_release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert client.close_finished
    assert not _pending_close_tasks()


@pytest.mark.asyncio
async def test_redis_cancellation_preserves_primary_connection_error_as_cause() -> None:
    client = BlockingCloseClient()
    client.set_error = ConnectionError("primary")
    task = asyncio.create_task(
        RedisTombstones(
            "redis://example", client_factory=FakeFactory(lambda: client)
        ).mark_deleted(scope(), "knowledge-1")
    )
    await _wait_for_close_start(client)
    task.cancel()
    client.close_release.set()
    with pytest.raises(asyncio.CancelledError) as error:
        await asyncio.wait_for(task, timeout=1)
    assert isinstance(error.value.__cause__, ConnectionError)
    assert client.close_finished
    assert not _pending_close_tasks()


@pytest.mark.asyncio
async def test_redis_operation_cancellation_survives_second_cancellation_during_close() -> (
    None
):
    client = BlockingCloseClient()
    client.get_error = asyncio.CancelledError()
    task = asyncio.create_task(
        RedisTombstones(
            "redis://example", client_factory=FakeFactory(lambda: client)
        ).is_deleted(scope(), "knowledge-1")
    )
    await _wait_for_close_start(client)
    task.cancel()
    client.close_release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert client.close_finished
    assert not _pending_close_tasks()


def test_redis_clients_share_backend_state_and_refresh_ttl_across_loops() -> None:
    clock = Clock()
    state = SharedRedisState(clock)
    factory = FakeFactory(lambda: StatefulFakeRedisClient(state))
    tombstones = RedisTombstones("redis://example", client_factory=factory)

    async def mark() -> None:
        await asyncio.wait_for(
            tombstones.mark_deleted(scope(), "knowledge-1"), timeout=1
        )

    async def deleted() -> bool:
        return await asyncio.wait_for(
            tombstones.is_deleted(scope(), "knowledge-1"), timeout=1
        )

    asyncio.run(mark())
    clock.now = 3699.0
    asyncio.run(mark())
    clock.now = 3700.0
    assert asyncio.run(deleted())
    clock.now = 7298.999
    assert asyncio.run(deleted())
    clock.now = 7299.0
    assert not asyncio.run(deleted())
    assert len({id(client) for client in factory.clients}) == len(factory.clients)


@pytest.mark.asyncio
async def test_redis_rejects_async_factory_without_runtime_warning(
    recwarn: pytest.WarningsRecorder,
) -> None:
    async def async_factory(*args: object, **kwargs: object) -> FakeRedisClient:
        return FakeRedisClient()

    with pytest.raises(TypeError, match="client_factory 不能返回 awaitable"):
        await RedisTombstones(
            "redis://example", client_factory=async_factory
        ).is_deleted(scope(), "knowledge-1")
    assert not [warning for warning in recwarn if warning.category is RuntimeWarning]


@pytest.mark.asyncio
async def test_redis_accepts_awaitable_official_client_shape() -> None:
    client = AwaitableRedisClient(get_result="1")
    tombstones = RedisTombstones(
        "redis://example",
        client_factory=lambda *args, **kwargs: client,
    )

    await tombstones.mark_deleted(scope(), "knowledge-1")
    assert await tombstones.is_deleted(scope(), "knowledge-1")
    assert client.closed == 2


@pytest.mark.asyncio
async def test_redis_rejects_and_cancels_future_factory_result() -> None:
    future = asyncio.get_running_loop().create_future()

    with pytest.raises(TypeError, match="client_factory 不能返回 awaitable"):
        RedisTombstones(
            "redis://example",
            client_factory=lambda *args, **kwargs: future,  # type: ignore[arg-type]
        )._new_client()

    assert future.cancelled()


@pytest.mark.parametrize("client", [object(), object()])
def test_redis_rejects_invalid_client_shape(client: object) -> None:
    with pytest.raises(TypeError, match="Redis 客户端缺少"):
        RedisTombstones(
            "redis://example", client_factory=lambda *args, **kwargs: client
        )._new_client()


@pytest.mark.parametrize("url", ["", "   "])
def test_redis_rejects_empty_url(url: str) -> None:
    with pytest.raises(ValueError):
        RedisTombstones(url)


@pytest.mark.parametrize("client_factory", [False, 0, "", object()])
def test_redis_rejects_non_callable_client_factory(client_factory: object) -> None:
    with pytest.raises(TypeError):
        RedisTombstones("redis://example", client_factory=client_factory)  # type: ignore[arg-type]


def test_redis_accepts_function_client_factory() -> None:
    def client_factory(*args: object, **kwargs: object) -> FakeRedisClient:
        return FakeRedisClient()

    assert RedisTombstones("redis://example", client_factory=client_factory)


@pytest.mark.parametrize("ttl", [True, False, 0, -1, 1.0, "1"])
def test_redis_rejects_invalid_ttl_with_valid_timeout(ttl: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        RedisTombstones("redis://example", ttl_seconds=ttl, socket_timeout=2.0)  # type: ignore[arg-type]


@pytest.mark.parametrize("timeout", [True, False, 0, -1.0, math.nan, math.inf, "1"])
def test_redis_rejects_invalid_timeout_with_valid_ttl(timeout: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        RedisTombstones("redis://example", ttl_seconds=3600, socket_timeout=timeout)  # type: ignore[arg-type]


async def _delete_and_close(client: Redis, keys: tuple[str, ...]) -> None:
    try:
        await client.delete(*keys)
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_real_redis_cleanup_closes_when_delete_fails() -> None:
    class CleanupClient:
        def __init__(self) -> None:
            self.close_calls = 0

        async def delete(self, *_keys: str) -> None:
            raise RuntimeError("delete failed")

        async def aclose(self) -> None:
            self.close_calls += 1

    cleanup = CleanupClient()

    with pytest.raises(RuntimeError, match="delete failed"):
        await _delete_and_close(cleanup, ("key",))

    assert cleanup.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.skipif(
    not TEST_REDIS_URL,
    reason="未配置 GRAPH_TEST_REDIS_URL，不连接默认开发 Redis",
)
async def test_real_redis_refreshes_short_ttl_and_isolates_keys_without_persistent_client() -> (
    None
):
    assert TEST_REDIS_URL is not None
    first_scope = scope(knowledge_base_id=uuid4())
    other_scope = scope(knowledge_base_id=uuid4())
    first_source = f"task13-{uuid4().hex}"
    other_source = f"task13-{uuid4().hex}"
    keys = (
        tombstone_key(first_scope, first_source),
        tombstone_key(first_scope, other_source),
        tombstone_key(other_scope, first_source),
    )
    tombstones = RedisTombstones(TEST_REDIS_URL, ttl_seconds=10)
    cleanup = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    try:
        await cleanup.delete(*keys)
        await tombstones.mark_deleted(first_scope, first_source)
        assert await tombstones.is_deleted(first_scope, first_source)
        assert not await tombstones.is_deleted(first_scope, other_source)
        assert not await tombstones.is_deleted(other_scope, first_source)

        await asyncio.sleep(0.05)
        first_ttl = await cleanup.pttl(keys[0])
        await tombstones.mark_deleted(first_scope, first_source)
        second_ttl = await cleanup.pttl(keys[0])

        assert second_ttl >= first_ttl > 0
        assert not hasattr(tombstones, "_client")
    finally:
        await _delete_and_close(cleanup, keys)
