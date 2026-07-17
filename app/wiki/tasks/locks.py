"""Wiki 批次的 Redis token 锁和显式进程内锁。"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from functools import lru_cache
from typing import Protocol, runtime_checkable
from uuid import UUID, uuid4

from redis.asyncio import Redis


logger = logging.getLogger(__name__)

DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/2"
DEFAULT_TTL_SECONDS = 60
DEFAULT_RENEW_INTERVAL_SECONDS = 20.0
DEFAULT_OPERATION_TIMEOUT_SECONDS = 5.0

RENEW_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('expire', KEYS[1], ARGV[2])
end
return 0
"""

RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""

_LeaseOperation = Callable[[], Awaitable[bool]]
_LeaseCloseOperation = Callable[[], Awaitable[None]]
_RedisClientFactory = Callable[[], "_RedisLockClient"]


class LockOwnershipLost(RuntimeError):
    """锁 token 无法在有限时间内确认时抛出。"""


class _RedisLockClient(Protocol):
    async def set(
        self, name: str, value: str, *, nx: bool, ex: int
    ) -> object: ...

    async def eval(self, script: str, numkeys: int, *args: object) -> object: ...

    async def exists(self, key: str) -> object: ...

    async def aclose(self) -> None: ...


def _validate_timing(
    ttl_seconds: int,
    renew_interval_seconds: float,
    operation_timeout_seconds: float,
) -> None:
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds 必须大于 0")
    if renew_interval_seconds <= 0:
        raise ValueError("renew_interval_seconds 必须大于 0")
    if renew_interval_seconds >= ttl_seconds:
        raise ValueError("renew_interval_seconds 必须小于 ttl_seconds")
    if operation_timeout_seconds <= 0:
        raise ValueError("operation_timeout_seconds 必须大于 0")
    if operation_timeout_seconds >= ttl_seconds:
        raise ValueError("operation_timeout_seconds 必须小于 ttl_seconds")


class LockLease:
    """一次带 token 的锁租约，负责续期守护和一次性释放。"""

    def __init__(
        self,
        *,
        key: str,
        token: str,
        renew_interval_seconds: float,
        operation_timeout_seconds: float,
        renew_operation: _LeaseOperation,
        release_operation: _LeaseOperation,
        close_operation: _LeaseCloseOperation | None = None,
    ) -> None:
        self.key = key
        self.token = token
        self.lost = False
        self._renew_interval_seconds = renew_interval_seconds
        self._operation_timeout_seconds = operation_timeout_seconds
        self._renew_operation = renew_operation
        self._release_operation = release_operation
        self._close_operation = close_operation
        self._keep_alive_task: asyncio.Task[None] | None = None
        self._released = False
        self._close_started = False
        self._renew_lock = asyncio.Lock()
        self._release_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()

    async def renew(self) -> bool:
        """比较 token 后续期；任何失败都会把租约标记为已丢失。"""

        return await self._renew_once()

    async def assert_owned(self) -> None:
        """提交结果前同步确认 token 仍归当前 Worker 所有。"""

        if self._released or self.lost:
            self.lost = True
            raise LockOwnershipLost("Wiki 锁所有权已丢失")
        try:
            renewed = await self._renew_once()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise LockOwnershipLost("Wiki 锁所有权确认失败") from error
        if not renewed:
            raise LockOwnershipLost("Wiki 锁所有权已丢失")

    async def _renew_once(self) -> bool:
        async with self._renew_lock:
            if self._released or self.lost:
                self.lost = True
                return False
            try:
                async with asyncio.timeout(self._operation_timeout_seconds):
                    renewed = bool(await self._renew_operation())
            except asyncio.CancelledError:
                raise
            except Exception:
                self.lost = True
                raise
            if not renewed:
                self.lost = True
            return renewed

    async def release(self) -> bool:
        """比较 token 后至多释放一次锁。"""

        async with self._release_lock:
            if self._released:
                return False
            try:
                released = bool(await self._release_operation())
                self._released = True
                return released
            finally:
                await self._close_once()

    async def _close_once(self) -> None:
        if self._close_operation is None:
            return
        async with self._close_lock:
            if self._close_started:
                return
            self._close_started = True
            try:
                await self._close_operation()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning(
                    "Wiki Redis 租约客户端关闭失败: key=%s error_type=%s",
                    self.key,
                    type(error).__name__,
                )

    async def keep_alive(self) -> None:
        """按固定间隔续期，失去所有权或 Redis 异常后立即结束。"""

        while True:
            await asyncio.sleep(self._renew_interval_seconds)
            try:
                await self.assert_owned()
            except asyncio.CancelledError:
                raise
            except LockOwnershipLost as error:
                cause = type(error.__cause__).__name__ if error.__cause__ else "lost"
                logger.warning(
                    "Wiki 锁所有权确认失败，停止续期: key=%s error_type=%s",
                    self.key,
                    cause,
                )
                return

    async def __aenter__(self) -> LockLease:
        if self._released:
            raise RuntimeError("已释放的锁租约不能重复进入")
        if self._keep_alive_task is not None:
            raise RuntimeError("锁租约已处于上下文中")
        self._keep_alive_task = asyncio.create_task(
            self.keep_alive(), name=f"wiki-lock-keep-alive:{self.key}"
        )
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self._stop_keep_alive()
        try:
            await self.release()
        except Exception as cleanup_error:
            if exc_type is None:
                raise
            logger.warning(
                "Wiki 锁释放清理失败，保留原始异常: key=%s error_type=%s",
                self.key,
                type(cleanup_error).__name__,
            )

    async def _stop_keep_alive(self) -> None:
        task = self._keep_alive_task
        self._keep_alive_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


@runtime_checkable
class WikiLockManager(Protocol):
    """Wiki 批次锁管理器契约。"""

    async def acquire(self, knowledge_base_id: UUID) -> LockLease | None: ...

    async def is_active(self, knowledge_base_id: UUID) -> bool: ...


class RedisWikiLockManager:
    """使用 Redis SET NX 和 token 比较脚本实现的分布式锁。"""

    def __init__(
        self,
        redis_client: _RedisLockClient | None = None,
        *,
        client_factory: _RedisClientFactory | None = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        renew_interval_seconds: float = DEFAULT_RENEW_INTERVAL_SECONDS,
        operation_timeout_seconds: float = DEFAULT_OPERATION_TIMEOUT_SECONDS,
    ) -> None:
        _validate_timing(
            ttl_seconds, renew_interval_seconds, operation_timeout_seconds
        )
        if (redis_client is None) == (client_factory is None):
            raise ValueError("redis_client 和 client_factory 必须且只能配置一个")
        self._redis_client = redis_client
        self._client_factory = client_factory
        self._ttl_seconds = ttl_seconds
        self._renew_interval_seconds = renew_interval_seconds
        self._operation_timeout_seconds = operation_timeout_seconds

    @staticmethod
    def _key(knowledge_base_id: UUID) -> str:
        return f"wiki:active:{knowledge_base_id}"

    async def acquire(self, knowledge_base_id: UUID) -> LockLease | None:
        key = self._key(knowledge_base_id)
        token = uuid4().hex
        client, owned_client = self._get_client()
        try:
            acquired = await client.set(key, token, nx=True, ex=self._ttl_seconds)
        except BaseException:
            if owned_client:
                await self._close_owned_client(client)
            raise
        if not acquired:
            if owned_client:
                await self._close_owned_client(client)
            return None

        async def renew() -> bool:
            return bool(
                await client.eval(RENEW_SCRIPT, 1, key, token, self._ttl_seconds)
            )

        if owned_client:

            async def release() -> bool:
                release_client, _ = self._get_client()
                try:
                    return bool(
                        await release_client.eval(RELEASE_SCRIPT, 1, key, token)
                    )
                finally:
                    await self._close_owned_client(release_client)

            close_operation: _LeaseCloseOperation | None = client.aclose
        else:

            async def release() -> bool:
                return bool(await client.eval(RELEASE_SCRIPT, 1, key, token))

            close_operation = None

        return LockLease(
            key=key,
            token=token,
            renew_interval_seconds=self._renew_interval_seconds,
            operation_timeout_seconds=self._operation_timeout_seconds,
            renew_operation=renew,
            release_operation=release,
            close_operation=close_operation,
        )

    async def is_active(self, knowledge_base_id: UUID) -> bool:
        client, owned_client = self._get_client()
        try:
            return bool(await client.exists(self._key(knowledge_base_id)))
        finally:
            if owned_client:
                await self._close_owned_client(client)

    def _get_client(self) -> tuple[_RedisLockClient, bool]:
        if self._client_factory is not None:
            return self._client_factory(), True
        assert self._redis_client is not None
        return self._redis_client, False

    @staticmethod
    async def _close_owned_client(client: _RedisLockClient) -> None:
        try:
            await client.aclose()
        except Exception as error:
            logger.warning(
                "Wiki Redis 客户端关闭失败: error_type=%s",
                type(error).__name__,
            )


@dataclass(frozen=True, slots=True)
class _MemoryHolder:
    token: str
    expires_at: float


class MemoryWikiLockManager:
    """仅供单 Worker 开发模式使用的进程内 token 锁。"""

    def __init__(
        self,
        *,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        renew_interval_seconds: float = DEFAULT_RENEW_INTERVAL_SECONDS,
        operation_timeout_seconds: float = DEFAULT_OPERATION_TIMEOUT_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        _validate_timing(
            ttl_seconds, renew_interval_seconds, operation_timeout_seconds
        )
        self._ttl_seconds = ttl_seconds
        self._renew_interval_seconds = renew_interval_seconds
        self._operation_timeout_seconds = operation_timeout_seconds
        self._monotonic = monotonic
        self._holders: dict[UUID, _MemoryHolder] = {}
        self._meta_lock = asyncio.Lock()

    async def acquire(self, knowledge_base_id: UUID) -> LockLease | None:
        token = uuid4().hex
        async with self._meta_lock:
            now = self._monotonic()
            holder = self._holders.get(knowledge_base_id)
            if holder is not None and holder.expires_at > now:
                return None
            self._holders[knowledge_base_id] = _MemoryHolder(
                token=token, expires_at=now + self._ttl_seconds
            )

        async def renew() -> bool:
            return await self._renew(knowledge_base_id, token)

        async def release() -> bool:
            return await self._release(knowledge_base_id, token)

        return LockLease(
            key=f"wiki:active:{knowledge_base_id}",
            token=token,
            renew_interval_seconds=self._renew_interval_seconds,
            operation_timeout_seconds=self._operation_timeout_seconds,
            renew_operation=renew,
            release_operation=release,
        )

    async def is_active(self, knowledge_base_id: UUID) -> bool:
        async with self._meta_lock:
            holder = self._holders.get(knowledge_base_id)
            if holder is None:
                return False
            if holder.expires_at <= self._monotonic():
                del self._holders[knowledge_base_id]
                return False
            return True

    async def _renew(self, knowledge_base_id: UUID, token: str) -> bool:
        async with self._meta_lock:
            now = self._monotonic()
            holder = self._holders.get(knowledge_base_id)
            if holder is None:
                return False
            if holder.expires_at <= now:
                del self._holders[knowledge_base_id]
                return False
            if holder.token != token:
                return False
            self._holders[knowledge_base_id] = _MemoryHolder(
                token=token, expires_at=now + self._ttl_seconds
            )
            return True

    async def _release(self, knowledge_base_id: UUID, token: str) -> bool:
        async with self._meta_lock:
            holder = self._holders.get(knowledge_base_id)
            if holder is None:
                return False
            if holder.expires_at <= self._monotonic():
                del self._holders[knowledge_base_id]
                return False
            if holder.token != token:
                return False
            del self._holders[knowledge_base_id]
            return True


@lru_cache(maxsize=1)
def build_lock_manager_from_env() -> WikiLockManager:
    """按显式环境配置构造进程复用的锁管理器。"""

    mode = os.getenv("GRAPH_WIKI_LOCK_MODE", "redis").strip().casefold()
    if mode == "memory":
        logger.warning("Wiki 使用进程内锁，仅支持单 Worker")
        return MemoryWikiLockManager()
    if mode != "redis":
        raise ValueError("GRAPH_WIKI_LOCK_MODE 只能是 redis 或 memory")

    redis_url = os.getenv("GRAPH_REDIS_URL", DEFAULT_REDIS_URL).strip()
    if not redis_url:
        raise ValueError("GRAPH_REDIS_URL 不能为空")

    def client_factory() -> _RedisLockClient:
        return Redis.from_url(
            redis_url,
            socket_connect_timeout=DEFAULT_OPERATION_TIMEOUT_SECONDS,
            socket_timeout=DEFAULT_OPERATION_TIMEOUT_SECONDS,
        )

    return RedisWikiLockManager(client_factory=client_factory)
