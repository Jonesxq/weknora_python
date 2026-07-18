"""Wiki 删除墓碑的进程内和 Redis 适配器。"""

from __future__ import annotations

import math
import time
import unicodedata
from collections.abc import Awaitable, Callable
from typing import Protocol

from redis.asyncio import Redis

from app.wiki.scope import WikiScope


DEFAULT_TTL_SECONDS = 3600
DEFAULT_SOCKET_TIMEOUT_SECONDS = 2.0


class _RedisTombstoneClient(Protocol):
    async def set(self, key: str, value: str, *, ex: int) -> object: ...

    async def get(self, key: str) -> object: ...

    async def aclose(self) -> None: ...


_RedisClientFactory = Callable[..., _RedisTombstoneClient]


def _validate_ttl(ttl_seconds: int) -> None:
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int):
        raise TypeError("ttl_seconds 必须是整数")
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds 必须大于 0")


def _validate_socket_timeout(socket_timeout: float) -> None:
    if isinstance(socket_timeout, bool) or not isinstance(socket_timeout, (int, float)):
        raise TypeError("socket_timeout 必须是有限正数")
    if not math.isfinite(socket_timeout) or socket_timeout <= 0:
        raise ValueError("socket_timeout 必须是有限正数")


def tombstone_key(scope: WikiScope, knowledge_id: str) -> str:
    """构造只依赖知识库全局身份的删除墓碑键。"""

    if not isinstance(scope, WikiScope):
        raise TypeError("scope 必须是 WikiScope")
    if not isinstance(knowledge_id, str):
        raise TypeError("knowledge_id 必须是字符串")
    normalized = knowledge_id.strip()
    if not normalized:
        raise ValueError("knowledge_id 不能为空")
    if knowledge_id != normalized:
        raise ValueError("knowledge_id 不能包含首尾空白")
    if any(
        character.isspace() and character not in {" "} for character in knowledge_id
    ):
        raise ValueError("knowledge_id 不能包含控制字符")
    if any(unicodedata.category(character) == "Cc" for character in knowledge_id):
        raise ValueError("knowledge_id 不能包含控制字符")
    return f"wiki:deleted:{scope.knowledge_base_id}:{knowledge_id}"


class MemoryTombstones:
    """仅供单进程开发和测试使用的内存墓碑。"""

    def __init__(
        self,
        *,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        _validate_ttl(ttl_seconds)
        if not callable(clock):
            raise TypeError("clock 必须可调用")
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._expires_at: dict[str, float] = {}

    async def mark_deleted(self, scope: WikiScope, knowledge_id: str) -> None:
        self._expires_at[tombstone_key(scope, knowledge_id)] = (
            self._clock() + self._ttl_seconds
        )

    async def is_deleted(self, scope: WikiScope, knowledge_id: str) -> bool:
        key = tombstone_key(scope, knowledge_id)
        expires_at = self._expires_at.get(key)
        if expires_at is None:
            return False
        if self._clock() >= expires_at:
            del self._expires_at[key]
            return False
        return True


class RedisTombstones:
    """按调用创建 Redis 客户端的删除墓碑适配器。"""

    def __init__(
        self,
        url: str,
        *,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        socket_timeout: float = DEFAULT_SOCKET_TIMEOUT_SECONDS,
        client_factory: _RedisClientFactory | None = None,
    ) -> None:
        if not isinstance(url, str) or not url.strip():
            raise ValueError("Redis URL 不能为空")
        _validate_ttl(ttl_seconds)
        _validate_socket_timeout(socket_timeout)
        self._url = url.strip()
        self._ttl_seconds = ttl_seconds
        self._socket_timeout = socket_timeout
        if client_factory is not None and not callable(client_factory):
            raise TypeError("client_factory 必须可调用")
        self._client_factory = (
            Redis.from_url if client_factory is None else client_factory
        )

    def _new_client(self) -> _RedisTombstoneClient:
        return self._client_factory(
            self._url,
            decode_responses=True,
            socket_connect_timeout=self._socket_timeout,
            socket_timeout=self._socket_timeout,
        )

    async def mark_deleted(self, scope: WikiScope, knowledge_id: str) -> None:
        key = tombstone_key(scope, knowledge_id)

        async def operation(client: _RedisTombstoneClient) -> None:
            written = await client.set(key, "1", ex=self._ttl_seconds)
            if written is False or written is None:
                raise RuntimeError("写入删除墓碑失败")

        await self._with_client(operation)

    async def is_deleted(self, scope: WikiScope, knowledge_id: str) -> bool:
        key = tombstone_key(scope, knowledge_id)

        async def operation(client: _RedisTombstoneClient) -> bool:
            value = await client.get(key)
            if value is None:
                return False
            if value == "1":
                return True
            raise RuntimeError("删除墓碑数据损坏")

        return await self._with_client(operation)

    async def _with_client(
        self, operation: Callable[[_RedisTombstoneClient], Awaitable[object]]
    ) -> object:
        client = self._new_client()
        try:
            result = await operation(client)
        except BaseException:
            try:
                await client.aclose()
            except BaseException:
                pass
            raise
        await client.aclose()
        return result
