from __future__ import annotations

import json
from typing import Any

from loguru import logger

try:
    import redis.asyncio as aioredis
    _REDIS = True
except ImportError:
    _REDIS = False

from core.config import Config


class Cache:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._client: Any = None
        redis_cfg = config.get_value("storage", "redis") or {}
        self._host = redis_cfg.get("host", "localhost")
        self._port = int(redis_cfg.get("port", 6379))
        self._db = int(redis_cfg.get("db", 0))
        self._password = redis_cfg.get("password") or None
        self._default_ttl = int(redis_cfg.get("default_ttl_seconds", 300))

    async def connect(self) -> None:
        if not _REDIS:
            logger.warning("redis not installed — cache disabled")
            return
        try:
            self._client = aioredis.Redis(
                host=self._host,
                port=self._port,
                db=self._db,
                password=self._password,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            await self._client.ping()
            logger.info("Redis cache connected at {}:{}", self._host, self._port)
        except Exception as exc:
            logger.warning("Redis connection failed: {} — running without cache", exc)
            self._client = None

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        if self._client is None:
            return
        try:
            serialized = json.dumps(value, default=str)
            await self._client.set(key, serialized, ex=ttl or self._default_ttl)
        except Exception as exc:
            logger.debug("Cache set error for '{}': {}", key, exc)

    async def get(self, key: str) -> Any:
        if self._client is None:
            return None
        try:
            raw = await self._client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as exc:
            logger.debug("Cache get error for '{}': {}", key, exc)
            return None

    async def delete(self, key: str) -> None:
        if self._client is None:
            return
        try:
            await self._client.delete(key)
        except Exception as exc:
            logger.debug("Cache delete failed for key '{}': {}", key, exc)

    async def publish(self, channel: str, message: Any) -> None:
        if self._client is None:
            return
        try:
            serialized = json.dumps(message, default=str)
            await self._client.publish(channel, serialized)
        except Exception as exc:
            logger.debug("Cache publish error on '{}': {}", channel, exc)

    async def subscribe(self, channel: str) -> Any:
        if self._client is None:
            return None
        try:
            pubsub = self._client.pubsub()
            await pubsub.subscribe(channel)
            return pubsub
        except Exception as exc:
            logger.debug("Cache subscribe error on '{}': {}", channel, exc)
            return None

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            logger.info("Redis cache closed")

    @property
    def available(self) -> bool:
        return self._client is not None
