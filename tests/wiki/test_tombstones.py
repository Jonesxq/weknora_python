"""删除墓碑适配器的行为测试。"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable
from uuid import UUID

import pytest

from app.wiki.ingest.ports import TombstonePort
from app.wiki.scope import WikiScope
from app.wiki.tasks.tombstones import MemoryTombstones, RedisTombstones, tombstone_key


KB_ID = UUID("11111111-1111-1111-1111-111111111111")
OTHER_KB_ID = UUID("22222222-2222-2222-2222-222222222222")


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


def test_key_uses_kb_identity_without_changing_knowledge_id() -> None:
    assert tombstone_key(scope(), "knowledge-1") == f"wiki:deleted:{KB_ID}:knowledge-1"
    assert tombstone_key(scope(tenant_id=8), "knowledge-1") == tombstone_key(
        scope(), "knowledge-1"
    )
    assert tombstone_key(
        scope(knowledge_base_id=OTHER_KB_ID), "knowledge-1"
    ) != tombstone_key(scope(), "knowledge-1")


@pytest.mark.parametrize(
    "knowledge_id", ["", "   ", " knowledge-1", "knowledge-1 ", "a\nb", "a\tb"]
)
def test_key_rejects_ambiguous_knowledge_id(knowledge_id: str) -> None:
    with pytest.raises(ValueError):
        tombstone_key(scope(), knowledge_id)


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
        client.calls[-1][-1] == 60 and client.closed == 1 for client in factory.clients
    )


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


@pytest.mark.parametrize("url", ["", "   "])
def test_redis_rejects_empty_url(url: str) -> None:
    with pytest.raises(ValueError):
        RedisTombstones(url)


@pytest.mark.parametrize("ttl", [True, False, 0, -1, 1.0, "1"])
@pytest.mark.parametrize("timeout", [True, False, 0, -1.0, math.nan, math.inf, "1"])
def test_redis_rejects_invalid_constructor_bounds(ttl: object, timeout: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        RedisTombstones("redis://example", ttl_seconds=ttl, socket_timeout=timeout)  # type: ignore[arg-type]
