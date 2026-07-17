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
    MemoryWikiLockManager,
    RedisWikiLockManager,
    build_lock_manager_from_env,
)


KB_ID = UUID("11111111-1111-1111-1111-111111111111")


class FakeRedis:
    """只实现锁测试需要的 Redis 命令。"""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.set_calls: list[tuple[str, str, bool, int]] = []
        self.eval_calls: list[tuple[str, int, tuple[object, ...]]] = []
        self.exists_calls: list[str] = []
        self.delete_calls: list[str] = []
        self.allow_set = True
        self.set_error: Exception | None = None
        self.eval_errors: dict[str, Exception] = {}

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
        key, token = str(args[0]), str(args[1])
        if self.values.get(key) != token:
            return 0
        if script == RENEW_SCRIPT:
            return 1
        if script == RELEASE_SCRIPT:
            del self.values[key]
            return 1
        raise AssertionError("收到未知 Lua 脚本")

    async def exists(self, key: str) -> int:
        self.exists_calls.append(key)
        return int(key in self.values)

    async def delete(self, key: str) -> int:
        self.delete_calls.append(key)
        return int(self.values.pop(key, None) is not None)


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
    assert await first.renew() is True
    assert redis.eval_calls[-1] == (
        RENEW_SCRIPT,
        1,
        (first.key, first.token, 60),
    )
    assert await first.release() is True
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
        redis, ttl_seconds=1, renew_interval_seconds=0.01
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
async def test_keep_alive_marks_lease_lost_and_stops_on_redis_error() -> None:
    redis = FakeRedis()
    manager = RedisWikiLockManager(
        redis, ttl_seconds=1, renew_interval_seconds=0.01
    )
    lease = await manager.acquire(KB_ID)
    assert lease is not None
    redis.eval_errors[RENEW_SCRIPT] = ConnectionError("renew failed")

    async with lease:
        await wait_until(lambda: lease.lost)

    renew_count = sum(call[0] == RENEW_SCRIPT for call in redis.eval_calls)
    await asyncio.sleep(0.03)
    assert sum(call[0] == RENEW_SCRIPT for call in redis.eval_calls) == renew_count
    assert sum(call[0] == RELEASE_SCRIPT for call in redis.eval_calls) == 1


@pytest.mark.asyncio
async def test_async_context_cancellation_stops_guard_and_releases_once() -> None:
    redis = FakeRedis()
    manager = RedisWikiLockManager(
        redis, ttl_seconds=1, renew_interval_seconds=0.01
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
    assert await old.renew() is False
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
    ("ttl_seconds", "renew_interval_seconds"),
    [(0, 1), (-1, 1), (60, 0), (60, -1), (20, 20), (20, 21)],
)
def test_lock_managers_reject_invalid_timing(
    manager_type: type[Any],
    kwargs: dict[str, object],
    ttl_seconds: int,
    renew_interval_seconds: int,
) -> None:
    with pytest.raises(ValueError):
        manager_type(
            ttl_seconds=ttl_seconds,
            renew_interval_seconds=renew_interval_seconds,
            **kwargs,
        )


def test_environment_factory_defaults_to_cached_redis_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_redis = FakeRedis()
    urls: list[str] = []

    def fake_from_url(url: str) -> FakeRedis:
        urls.append(url)
        return fake_redis

    monkeypatch.delenv("GRAPH_WIKI_LOCK_MODE", raising=False)
    monkeypatch.delenv("GRAPH_REDIS_URL", raising=False)
    monkeypatch.setattr(Redis, "from_url", fake_from_url)

    first = build_lock_manager_from_env()
    second = build_lock_manager_from_env()

    assert isinstance(first, RedisWikiLockManager)
    assert second is first
    assert urls == ["redis://127.0.0.1:6379/2"]


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

    def fail_from_url(url: str) -> None:
        raise ConnectionError(f"cannot connect to {url}")

    monkeypatch.setattr(Redis, "from_url", fail_from_url)

    with pytest.raises(ConnectionError, match="cannot connect"):
        build_lock_manager_from_env()


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
        await redis.delete(key)
        await redis.aclose()
