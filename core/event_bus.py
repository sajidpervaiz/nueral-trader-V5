from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Callable, Coroutine

from loguru import logger


Handler = Callable[..., Coroutine[Any, Any, None]]


class EventBus:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Handler]] = defaultdict(list)
        self._queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(maxsize=10_000)
        self._running = False

    def subscribe(self, event_type: str, handler: Handler) -> None:
        self._handlers[event_type].append(handler)
        logger.debug("EventBus: {} subscribed to '{}'", handler.__qualname__, event_type)

    def unsubscribe(self, event_type: str, handler: Handler) -> None:
        handlers = self._handlers.get(event_type, [])
        try:
            handlers.remove(handler)
        except ValueError:
            pass

    async def publish(self, event_type: str, payload: Any = None) -> None:
        await self._queue.put((event_type, payload))

    def publish_nowait(self, event_type: str, payload: Any = None) -> None:
        try:
            self._queue.put_nowait((event_type, payload))
        except asyncio.QueueFull:
            logger.warning("EventBus queue full — dropping event '{}'", event_type)

    async def run(self) -> None:
        self._running = True
        logger.info("EventBus started")
        while self._running:
            try:
                event_type, payload = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            handlers = self._handlers.get(event_type, [])
            for handler in handlers:
                try:
                    await handler(payload)
                except Exception as exc:
                    logger.exception("EventBus handler error in '{}': {}", handler.__qualname__, exc)

    async def stop(self) -> None:
        self._running = False
        logger.info("EventBus stopped")
