from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any


class RateLimiter:
    def __init__(self, max_calls: int, period_seconds: float = 60.0) -> None:
        self._max_calls = max_calls
        self._period = period_seconds
        self._calls: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self._period
            while self._calls and self._calls[0] < cutoff:
                self._calls.popleft()
            if len(self._calls) >= self._max_calls:
                sleep_time = self._period - (now - self._calls[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
            self._calls.append(time.monotonic())

    def __call__(self, fn: Any) -> Any:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            await self.acquire()
            return await fn(*args, **kwargs)
        return wrapper
