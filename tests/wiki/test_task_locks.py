from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from typing import Any
from uuid import UUID, uuid4

import pytest
from redis.asyncio import Redis

from app.wiki.tasks.locks import (
    RELEASE_SCRIPT,
    RENEW_SCRIPT,
    LockLease,
    LockOwnershipLost,
    MemoryWikiLockManager,
    RedisWikiLockManager,
    build_lock_manager_from_env,
)


KB_ID = UUID("11111111-1111-1111-1111-111111111111")


class FakeRedis:
    """只实现锁测试需要的 Redis 命令。"""

    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = values if values is not None else {}
        self.set_calls: list[tuple[str, str, bool, int]] = []
        self.eval_calls: list[tuple[str, int, tuple[object, ...]]] = []
        self.exists_calls: list[str] = []
        self.delete_calls: list[str] = []
        self.allow_set = True
        self.set_error: Exception | None = None
        self.eval_errors: dict[str, Exception] = {}
        self.eval_after_effect_errors: dict[str, Exception] = {}
        self.eval_waiter: asyncio.Event | None = None
        self.exists_error: Exception | None = None
        self.aclose_error: Exception | None = None
        self.aclose_count = 0

    async def set(
        self,
        name: str,
        value: str,
        *,
        nx: bool,
        ex: int,
    ) -> bool:
        self.set_calls.append((name, value, nx, ex))
        if self.set_error is not None:
            raise self.set_error
        if not self.allow_set or (nx and name in self.values):
            return False
        self.values[name] = value
        return True

    async def eval(self, script: str, numkeys: int, *args: object) -> int:
        self.eval_calls.append((script, numkeys, args))
        if script in self.eval_errors:
            raise self.eval_errors[script]
        if self.eval_waiter is not None:
            await self.eval_waiter.wait()
        key, token = str(args[0]), str(args[1])
        if self.values.get(key) != token:
            return 0
        if script == RENEW_SCRIPT:
            return 1
        if script == RELEASE_SCRIPT:
            del self.values[key]
            if script in self.eval_after_effect_errors:
                raise self.eval_after_effect_errors.pop(script)
            return 1
        raise AssertionError("收到未知 Lua 脚本")

    async def exists(self, key: str) -> int:
        self.exists_calls.append(key)
        if self.exists_error is not None:
            raise self.exists_error
        return int(key in self.values)

    async def delete(self, key: str) -> int:
        self.delete_calls.append(key)
        return int(self.values.pop(key, None) is not None)

    async def aclose(self) -> None:
        self.aclose_count += 1
        if self.aclose_error is not None:
            raise self.aclose_error


class LoopBoundFakeRedis(FakeRedis):
    """模拟 redis.asyncio 客户端不能跨事件循环复用。"""

    def __init__(self, values: dict[str, str] | None = None) -> None:
        super().__init__(values)
        self.loop = asyncio.get_running_loop()

    def _assert_loop(self) -> None:
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("Redis client used from a different event loop")

    async def set(
        self,
        name: str,
        value: str,
        *,
        nx: bool,
        ex: int,
    ) -> bool:
        self._assert_loop()
        return await super().set(name, value, nx=nx, ex=ex)

    async def eval(self, script: str, numkeys: int, *args: object) -> int:
        self._assert_loop()
        return await super().eval(script, numkeys, *args)

    async def exists(self, key: str) -> int:
        self._assert_loop()
        return await super().exists(key)

    async def aclose(self) -> None:
        self._assert_loop()
        await super().aclose()


class ManualClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


async def wait_until(
    predicate: Callable[[], bool], *, timeout_seconds: float = 0.5
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("等待异步条件超时")
        await asyncio.sleep(0.001)


@pytest.fixture(autouse=True)
def clear_lock_manager_cache() -> None:
    build_lock_manager_from_env.cache_clear()
    yield
    build_lock_manager_from_env.cache_clear()


@pytest.mark.asyncio
async def test_redis_lock_uses_random_token_and_exact_commands() -> None:
    redis = FakeRedis()
    manager = RedisWikiLockManager(redis, ttl_seconds=60, renew_interval_seconds=20)

    first = await manager.acquire(KB_ID)

    assert isinstance(first, LockLease)
    assert first.key == f"wiki:active:{KB_ID}"
    assert len(first.token) == 32
    assert redis.set_calls == [(first.key, first.token, True, 60)]
    await first.assert_owned()
    assert first.lost is False
    assert await first.renew() is True
    assert redis.eval_calls[-1] == (
        RENEW_SCRIPT,
        1,
        (first.key, first.token, 60),
    )
    assert await first.release() is True
    assert redis.aclose_count == 0
    assert redis.eval_calls[-1] == (
        RELEASE_SCRIPT,
        1,
        (first.key, first.token),
    )

    second = await manager.acquire(KB_ID)

    assert second is not None
    assert second.token != first.token


@pytest.mark.asyncio
async def test_redis_set_conflict_returns_none_and_connection_error_propagates() -> None:
    redis = FakeRedis()
    manager = RedisWikiLockManager(redis)
    redis.allow_set = False

    assert await manager.acquire(KB_ID) is None

    redis.set_error = ConnectionError("redis unavailable")
    with pytest.raises(ConnectionError, match="redis unavailable"):
        await manager.acquire(KB_ID)


@pytest.mark.asyncio
async def test_old_redis_owner_cannot_renew_or_release_new_owner() -> None:
    redis = FakeRedis()
    lease = await RedisWikiLockManager(redis).acquire(KB_ID)
    assert lease is not None
    redis.values[lease.key] = "new-owner-token"

    assert await lease.renew() is False
    assert lease.lost is True
    assert await lease.release() is False
    assert redis.values[lease.key] == "new-owner-token"


@pytest.mark.asyncio
async def test_assert_owned_wraps_redis_error_and_marks_lease_lost() -> None:
    redis = FakeRedis()
    lease = await RedisWikiLockManager(redis).acquire(KB_ID)
    assert lease is not None
    redis.eval_errors[RENEW_SCRIPT] = ConnectionError("renew failed")

    with pytest.raises(LockOwnershipLost) as error:
        await lease.assert_owned()

    assert isinstance(error.value.__cause__, ConnectionError)
    assert lease.lost is True
    assert lease.token not in str(error.value)


@pytest.mark.asyncio
async def test_assert_owned_has_finite_operation_timeout() -> None:
    redis = FakeRedis()
    manager = RedisWikiLockManager(
        redis,
        ttl_seconds=1,
        renew_interval_seconds=0.1,
        operation_timeout_seconds=0.01,
    )
    lease = await manager.acquire(KB_ID)
    assert lease is not None
    redis.eval_waiter = asyncio.Event()

    with pytest.raises(LockOwnershipLost) as error:
        await lease.assert_owned()

    assert isinstance(error.value.__cause__, TimeoutError)
    assert lease.lost is True
    redis.eval_waiter = None
    assert await lease.release() is True


@pytest.mark.asyncio
async def test_redis_is_active_uses_exists() -> None:
    redis = FakeRedis()
    manager = RedisWikiLockManager(redis)

    assert await manager.is_active(KB_ID) is False
    lease = await manager.acquire(KB_ID)
    assert lease is not None
    assert await manager.is_active(KB_ID) is True
    assert redis.exists_calls == [lease.key, lease.key]


@pytest.mark.asyncio
async def test_keep_alive_marks_lease_lost_when_renew_returns_false() -> None:
    redis = FakeRedis()
    manager = RedisWikiLockManager(
        redis,
        ttl_seconds=1,
        renew_interval_seconds=0.01,
        operation_timeout_seconds=0.1,
    )
    lease = await manager.acquire(KB_ID)
    assert lease is not None

    async with lease:
        redis.values[lease.key] = "replacement-token"
        await wait_until(lambda: lease.lost)

    renew_count = sum(call[0] == RENEW_SCRIPT for call in redis.eval_calls)
    await asyncio.sleep(0.03)
    assert sum(call[0] == RENEW_SCRIPT for call in redis.eval_calls) == renew_count
    assert redis.values[lease.key] == "replacement-token"


@pytest.mark.asyncio
async def test_keep_alive_marks_lease_lost_and_stops_on_redis_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    redis = FakeRedis()
    manager = RedisWikiLockManager(
        redis,
        ttl_seconds=1,
        renew_interval_seconds=0.01,
        operation_timeout_seconds=0.1,
    )
    lease = await manager.acquire(KB_ID)
    assert lease is not None
    redis.eval_errors[RENEW_SCRIPT] = ConnectionError("renew failed")

    with caplog.at_level(logging.WARNING):
        async with lease:
            await wait_until(lambda: lease.lost)

    renew_count = sum(call[0] == RENEW_SCRIPT for call in redis.eval_calls)
    await asyncio.sleep(0.03)
    assert sum(call[0] == RENEW_SCRIPT for call in redis.eval_calls) == renew_count
    assert sum(call[0] == RELEASE_SCRIPT for call in redis.eval_calls) == 1
    assert "所有权确认失败" in caplog.text
    assert lease.token not in caplog.text


@pytest.mark.asyncio
async def test_release_can_retry_after_command_error() -> None:
    redis = FakeRedis()
    lease = await RedisWikiLockManager(redis).acquire(KB_ID)
    assert lease is not None
    redis.eval_errors[RELEASE_SCRIPT] = ConnectionError("release failed")

    with pytest.raises(ConnectionError, match="release failed"):
        await lease.release()

    assert lease.key in redis.values
    del redis.eval_errors[RELEASE_SCRIPT]
    assert await lease.release() is True
    assert await lease.release() is False


@pytest.mark.asyncio
async def test_release_response_loss_can_retry_to_definitive_false() -> None:
    values: dict[str, str] = {}
    clients: list[FakeRedis] = []

    def client_factory() -> FakeRedis:
        client = FakeRedis(values)
        if len(clients) == 1:
            client.eval_after_effect_errors[RELEASE_SCRIPT] = ConnectionError(
                "response lost"
            )
        clients.append(client)
        return client

    manager = RedisWikiLockManager(client_factory=client_factory)
    lease = await manager.acquire(KB_ID)
    assert lease is not None

    with pytest.raises(ConnectionError, match="response lost"):
        await lease.release()

    assert lease.key not in values
    assert len(clients) == 2
    assert [client.aclose_count for client in clients] == [1, 1]
    assert await lease.release() is False
    assert len(clients) == 3
    assert [client.aclose_count for client in clients] == [1, 1, 1]
    assert await lease.release() is False
    assert sum(
        call[0] == RELEASE_SCRIPT
        for client in clients
        for call in client.eval_calls
    ) == 2


@pytest.mark.asyncio
async def test_cancelled_release_retains_retry_ability() -> None:
    started = asyncio.Event()
    blocker = asyncio.Event()
    attempts = 0

    async def renew() -> bool:
        return True

    async def release() -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            started.set()
            await blocker.wait()
        return True

    lease = LockLease(
        key=f"wiki:active:{KB_ID}",
        token="not-logged",
        renew_interval_seconds=20,
        operation_timeout_seconds=5,
        renew_operation=renew,
        release_operation=release,
    )
    task = asyncio.create_task(lease.release())
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert await lease.release() is True
    assert attempts == 2


@pytest.mark.asyncio
async def test_context_preserves_business_error_when_release_cleanup_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    redis = FakeRedis()
    lease = await RedisWikiLockManager(redis).acquire(KB_ID)
    assert lease is not None
    redis.eval_errors[RELEASE_SCRIPT] = ConnectionError("cleanup failed")

    with caplog.at_level(logging.WARNING):
        with pytest.raises(ValueError, match="business failed"):
            async with lease:
                raise ValueError("business failed")

    assert "释放清理失败" in caplog.text
    assert lease.token not in caplog.text
    del redis.eval_errors[RELEASE_SCRIPT]
    assert await lease.release() is True


@pytest.mark.asyncio
async def test_context_propagates_release_error_on_normal_exit() -> None:
    redis = FakeRedis()
    lease = await RedisWikiLockManager(redis).acquire(KB_ID)
    assert lease is not None
    redis.eval_errors[RELEASE_SCRIPT] = ConnectionError("cleanup failed")

    with pytest.raises(ConnectionError, match="cleanup failed"):
        async with lease:
            pass

    del redis.eval_errors[RELEASE_SCRIPT]
    assert await lease.release() is True


@pytest.mark.asyncio
async def test_context_preserves_cancellation_when_release_cleanup_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    redis = FakeRedis()
    manager = RedisWikiLockManager(
        redis,
        ttl_seconds=1,
        renew_interval_seconds=0.01,
        operation_timeout_seconds=0.1,
    )
    lease = await manager.acquire(KB_ID)
    assert lease is not None
    entered = asyncio.Event()

    async def hold_lock() -> None:
        async with lease:
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(hold_lock())
    await entered.wait()
    redis.eval_errors[RELEASE_SCRIPT] = ConnectionError("cleanup failed")
    with caplog.at_level(logging.WARNING):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    renew_count = sum(call[0] == RENEW_SCRIPT for call in redis.eval_calls)
    await asyncio.sleep(0.03)
    assert sum(call[0] == RENEW_SCRIPT for call in redis.eval_calls) == renew_count
    assert "释放清理失败" in caplog.text
    assert lease.token not in caplog.text
    del redis.eval_errors[RELEASE_SCRIPT]
    assert await lease.release() is True


@pytest.mark.asyncio
async def test_new_cancellation_during_release_overrides_body_error() -> None:
    release_started = asyncio.Event()
    release_blocker = asyncio.Event()
    renew_calls = 0
    close_calls = 0

    async def renew() -> bool:
        nonlocal renew_calls
        renew_calls += 1
        return True

    async def release() -> bool:
        release_started.set()
        await release_blocker.wait()
        return True

    async def close() -> None:
        nonlocal close_calls
        close_calls += 1

    lease = LockLease(
        key=f"wiki:active:{KB_ID}",
        token="not-logged",
        renew_interval_seconds=0.01,
        operation_timeout_seconds=0.1,
        renew_operation=renew,
        release_operation=release,
        close_operation=close,
    )

    async def fail_in_body() -> None:
        async with lease:
            raise ValueError("business failed")

    task = asyncio.create_task(fail_in_body())
    await release_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    frozen_renew_calls = renew_calls
    await asyncio.sleep(0.03)
    assert renew_calls == frozen_renew_calls
    assert close_calls == 1


@pytest.mark.asyncio
async def test_owned_clients_close_when_business_and_release_both_fail(
    caplog: pytest.LogCaptureFixture,
) -> None:
    values: dict[str, str] = {}
    clients: list[FakeRedis] = []

    def client_factory() -> FakeRedis:
        client = FakeRedis(values)
        if len(clients) == 1:
            client.eval_errors[RELEASE_SCRIPT] = ConnectionError("release failed")
        clients.append(client)
        return client

    lease = await RedisWikiLockManager(client_factory=client_factory).acquire(KB_ID)
    assert lease is not None

    with caplog.at_level(logging.WARNING):
        with pytest.raises(ValueError, match="business failed"):
            async with lease:
                raise ValueError("business failed")

    assert len(clients) == 2
    assert [client.aclose_count for client in clients] == [1, 1]
    assert lease.key in values
    assert "释放清理失败" in caplog.text
    assert lease.token not in caplog.text

    assert await lease.release() is True
    assert len(clients) == 3
    assert [client.aclose_count for client in clients] == [1, 1, 1]
    assert lease.key not in values


@pytest.mark.asyncio
async def test_owned_lease_close_error_warns_without_losing_release_result(
    caplog: pytest.LogCaptureFixture,
) -> None:
    values: dict[str, str] = {}
    clients: list[FakeRedis] = []

    def client_factory() -> FakeRedis:
        client = FakeRedis(values)
        if not clients:
            client.aclose_error = ConnectionError("close failed")
        clients.append(client)
        return client

    lease = await RedisWikiLockManager(client_factory=client_factory).acquire(KB_ID)
    assert lease is not None

    with caplog.at_level(logging.WARNING):
        assert await lease.release() is True

    assert [client.aclose_count for client in clients] == [1, 1]
    assert "客户端关闭失败" in caplog.text
    assert lease.token not in caplog.text
    assert await lease.release() is False


@pytest.mark.asyncio
async def test_close_operation_cancellation_is_not_swallowed() -> None:
    async def renew() -> bool:
        return True

    async def release() -> bool:
        return True

    async def close() -> None:
        raise asyncio.CancelledError

    lease = LockLease(
        key=f"wiki:active:{KB_ID}",
        token="not-logged",
        renew_interval_seconds=20,
        operation_timeout_seconds=5,
        renew_operation=renew,
        release_operation=release,
        close_operation=close,
    )

    with pytest.raises(asyncio.CancelledError):
        await lease.release()

    assert await lease.release() is False


@pytest.mark.asyncio
async def test_async_context_cancellation_stops_guard_and_releases_once() -> None:
    redis = FakeRedis()
    manager = RedisWikiLockManager(
        redis,
        ttl_seconds=1,
        renew_interval_seconds=0.01,
        operation_timeout_seconds=0.1,
    )
    lease = await manager.acquire(KB_ID)
    assert lease is not None
    entered = asyncio.Event()

    async def hold_lock() -> None:
        async with lease:
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(hold_lock())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    renew_count = sum(call[0] == RENEW_SCRIPT for call in redis.eval_calls)
    await asyncio.sleep(0.03)
    assert sum(call[0] == RENEW_SCRIPT for call in redis.eval_calls) == renew_count
    assert sum(call[0] == RELEASE_SCRIPT for call in redis.eval_calls) == 1
    assert await lease.release() is False
    assert sum(call[0] == RELEASE_SCRIPT for call in redis.eval_calls) == 1
    assert lease.key not in redis.values


@pytest.mark.asyncio
async def test_memory_lock_allows_one_holder_per_kb_and_parallel_kbs() -> None:
    manager = MemoryWikiLockManager()
    other_kb_id = uuid4()

    first, other = await asyncio.gather(
        manager.acquire(KB_ID), manager.acquire(other_kb_id)
    )

    assert first is not None
    assert other is not None
    assert await manager.acquire(KB_ID) is None
    assert await manager.is_active(KB_ID) is True
    assert await first.release() is True
    assert await manager.acquire(KB_ID) is not None


@pytest.mark.asyncio
async def test_memory_lock_expires_and_old_lease_cannot_touch_new_owner() -> None:
    clock = ManualClock()
    manager = MemoryWikiLockManager(
        ttl_seconds=60, renew_interval_seconds=20, monotonic=clock
    )
    old = await manager.acquire(KB_ID)
    assert old is not None

    clock.advance(61)
    replacement = await manager.acquire(KB_ID)

    assert replacement is not None
    assert replacement.token != old.token
    with pytest.raises(LockOwnershipLost):
        await old.assert_owned()
    assert old.lost is True
    assert await old.release() is False
    assert await manager.is_active(KB_ID) is True
    assert await replacement.renew() is True
    assert await replacement.release() is True
    assert await manager.is_active(KB_ID) is False


@pytest.mark.parametrize(
    ("manager_type", "kwargs"),
    [
        (RedisWikiLockManager, {"redis_client": FakeRedis()}),
        (MemoryWikiLockManager, {}),
    ],
)
@pytest.mark.parametrize(
    ("ttl_seconds", "renew_interval_seconds", "operation_timeout_seconds"),
    [
        (0, 1, 1),
        (-1, 1, 1),
        (60, 0, 1),
        (60, -1, 1),
        (20, 20, 1),
        (20, 21, 1),
        (60, 20, 0),
        (60, 20, -1),
        (20, 5, 20),
        (20, 5, 21),
    ],
)
def test_lock_managers_reject_invalid_timing(
    manager_type: type[Any],
    kwargs: dict[str, object],
    ttl_seconds: int,
    renew_interval_seconds: int,
    operation_timeout_seconds: int,
) -> None:
    with pytest.raises(ValueError):
        manager_type(
            ttl_seconds=ttl_seconds,
            renew_interval_seconds=renew_interval_seconds,
            operation_timeout_seconds=operation_timeout_seconds,
            **kwargs,
        )


def test_environment_factory_defaults_to_cached_redis_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: list[FakeRedis] = []
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_from_url(url: str, **kwargs: object) -> FakeRedis:
        calls.append((url, kwargs))
        client = FakeRedis()
        clients.append(client)
        return client

    monkeypatch.delenv("GRAPH_WIKI_LOCK_MODE", raising=False)
    monkeypatch.delenv("GRAPH_REDIS_URL", raising=False)
    monkeypatch.setattr(Redis, "from_url", fake_from_url)

    first = build_lock_manager_from_env()
    second = build_lock_manager_from_env()

    assert isinstance(first, RedisWikiLockManager)
    assert second is first
    assert calls == []
    assert asyncio.run(first.is_active(KB_ID)) is False
    assert calls == [
        (
            "redis://127.0.0.1:6379/2",
            {"socket_connect_timeout": 5.0, "socket_timeout": 5.0},
        )
    ]
    assert clients[0].aclose_count == 1


def test_cached_environment_manager_uses_fresh_client_per_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values: dict[str, str] = {}
    clients: list[LoopBoundFakeRedis] = []

    def fake_from_url(url: str, **kwargs: object) -> LoopBoundFakeRedis:
        client = LoopBoundFakeRedis(values)
        clients.append(client)
        return client

    monkeypatch.setenv("GRAPH_WIKI_LOCK_MODE", "redis")
    monkeypatch.setattr(Redis, "from_url", fake_from_url)
    manager = build_lock_manager_from_env()

    async def acquire_and_release() -> None:
        lease = await manager.acquire(KB_ID)
        assert lease is not None
        await lease.assert_owned()
        assert await lease.release() is True

    asyncio.run(acquire_and_release())
    asyncio.run(acquire_and_release())

    assert len(clients) == 4
    assert len({id(client) for client in clients}) == 4
    assert clients[0].loop is clients[1].loop
    assert clients[0].loop is not clients[2].loop
    assert clients[2].loop is clients[3].loop
    assert [client.aclose_count for client in clients] == [1, 1, 1, 1]


@pytest.mark.asyncio
async def test_owned_clients_close_after_conflict_error_and_is_active() -> None:
    conflict = FakeRedis()
    conflict.allow_set = False
    manager = RedisWikiLockManager(client_factory=lambda: conflict)
    assert await manager.acquire(KB_ID) is None
    assert conflict.aclose_count == 1

    failed_set = FakeRedis()
    failed_set.set_error = ConnectionError("set failed")
    manager = RedisWikiLockManager(client_factory=lambda: failed_set)
    with pytest.raises(ConnectionError, match="set failed"):
        await manager.acquire(KB_ID)
    assert failed_set.aclose_count == 1

    failed_exists = FakeRedis()
    failed_exists.exists_error = ConnectionError("exists failed")
    manager = RedisWikiLockManager(client_factory=lambda: failed_exists)
    with pytest.raises(ConnectionError, match="exists failed"):
        await manager.is_active(KB_ID)
    assert failed_exists.aclose_count == 1


def test_environment_factory_uses_explicit_memory_mode_and_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("GRAPH_WIKI_LOCK_MODE", "  MeMoRy  ")

    with caplog.at_level(logging.WARNING):
        manager = build_lock_manager_from_env()

    assert isinstance(manager, MemoryWikiLockManager)
    assert "仅支持单 Worker" in caplog.text


@pytest.mark.parametrize("mode", ["", "local", "rediss"])
def test_environment_factory_rejects_invalid_mode(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    monkeypatch.setenv("GRAPH_WIKI_LOCK_MODE", mode)

    with pytest.raises(ValueError, match="GRAPH_WIKI_LOCK_MODE"):
        build_lock_manager_from_env()


def test_environment_factory_rejects_empty_redis_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GRAPH_WIKI_LOCK_MODE", "redis")
    monkeypatch.setenv("GRAPH_REDIS_URL", "  ")

    with pytest.raises(ValueError, match="GRAPH_REDIS_URL"):
        build_lock_manager_from_env()


def test_environment_factory_does_not_fall_back_when_redis_creation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GRAPH_WIKI_LOCK_MODE", "redis")

    def fail_from_url(url: str, **kwargs: object) -> None:
        raise ConnectionError(f"cannot connect to {url}")

    monkeypatch.setattr(Redis, "from_url", fail_from_url)

    manager = build_lock_manager_from_env()
    with pytest.raises(ConnectionError, match="cannot connect"):
        asyncio.run(manager.is_active(KB_ID))


@pytest.mark.asyncio
async def test_real_redis_lock_when_configured() -> None:
    """仅在显式配置测试 Redis 时验证真实 Lua 锁语义。"""

    url = os.getenv("GRAPH_TEST_REDIS_URL")
    if not url:
        pytest.skip("未配置 GRAPH_TEST_REDIS_URL，跳过真实 Redis 锁测试")

    redis = Redis.from_url(url, decode_responses=True)
    manager = RedisWikiLockManager(redis)
    kb_id = uuid4()
    key = f"wiki:active:{kb_id}"
    try:
        first = await manager.acquire(kb_id)
        assert first is not None
        assert await manager.acquire(kb_id) is None
        assert await first.renew() is True
        assert await first.release() is True
        assert await manager.is_active(kb_id) is False
    finally:
        try:
            await redis.delete(key)
        finally:
            await redis.aclose()
