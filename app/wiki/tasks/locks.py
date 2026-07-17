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


def _validate_timing(ttl_seconds: int, renew_interval_seconds: float) -> None:
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds 必须大于 0")
    if renew_interval_seconds <= 0:
        raise ValueError("renew_interval_seconds 必须大于 0")
    if renew_interval_seconds >= ttl_seconds:
        raise ValueError("renew_interval_seconds 必须小于 ttl_seconds")


class LockLease:
    """一次带 token 的锁租约，负责续期守护和一次性释放。"""

    def __init__(
        self,
        *,
        key: str,
        token: str,
        renew_interval_seconds: float,
        renew_operation: _LeaseOperation,
        release_operation: _LeaseOperation,
    ) -> None:
        self.key = key
        self.token = token
        self.lost = False
        self._renew_interval_seconds = renew_interval_seconds
        self._renew_operation = renew_operation
        self._release_operation = release_operation
        self._keep_alive_task: asyncio.Task[None] | None = None
        self._released = False
        self._renew_lock = asyncio.Lock()
        self._release_lock = asyncio.Lock()

    async def renew(self) -> bool:
        """比较 token 后续期；任何失败都会把租约标记为已丢失。"""

        async with self._renew_lock:
            if self._released or self.lost:
                return False
            try:
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
            self._released = True
            return bool(await self._release_operation())

    async def keep_alive(self) -> None:
        """按固定间隔续期，失去所有权或 Redis 异常后立即结束。"""

        while True:
            await asyncio.sleep(self._renew_interval_seconds)
            try:
                if not await self.renew():
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                # renew 已设置 lost；守护任务消费异常，避免产生未处理任务。
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
        task = self._keep_alive_task
        self._keep_alive_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await self.release()


@runtime_checkable
class WikiLockManager(Protocol):
    """Wiki 批次锁管理器契约。"""

    async def acquire(self, knowledge_base_id: UUID) -> LockLease | None: ...

    async def is_active(self, knowledge_base_id: UUID) -> bool: ...


class RedisWikiLockManager:
    """使用 Redis SET NX 和 token 比较脚本实现的分布式锁。"""

    def __init__(
        self,
        redis_client,
        *,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        renew_interval_seconds: float = DEFAULT_RENEW_INTERVAL_SECONDS,
    ) -> None:
        _validate_timing(ttl_seconds, renew_interval_seconds)
        self._redis = redis_client
        self._ttl_seconds = ttl_seconds
        self._renew_interval_seconds = renew_interval_seconds

    @staticmethod
    def _key(knowledge_base_id: UUID) -> str:
        return f"wiki:active:{knowledge_base_id}"

    async def acquire(self, knowledge_base_id: UUID) -> LockLease | None:
        key = self._key(knowledge_base_id)
        token = uuid4().hex
        acquired = await self._redis.set(
            key, token, nx=True, ex=self._ttl_seconds
        )
        if not acquired:
            return None

        async def renew() -> bool:
            return bool(
                await self._redis.eval(
                    RENEW_SCRIPT, 1, key, token, self._ttl_seconds
                )
            )

        async def release() -> bool:
            return bool(await self._redis.eval(RELEASE_SCRIPT, 1, key, token))

        return LockLease(
            key=key,
            token=token,
            renew_interval_seconds=self._renew_interval_seconds,
            renew_operation=renew,
            release_operation=release,
        )

    async def is_active(self, knowledge_base_id: UUID) -> bool:
        return bool(await self._redis.exists(self._key(knowledge_base_id)))


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
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        _validate_timing(ttl_seconds, renew_interval_seconds)
        self._ttl_seconds = ttl_seconds
        self._renew_interval_seconds = renew_interval_seconds
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
    return RedisWikiLockManager(Redis.from_url(redis_url))
