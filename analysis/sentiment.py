from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import aiohttp
from loguru import logger

from core.config import Config
from core.event_bus import EventBus


@dataclass
class SentimentScore:
    symbol: str
    source: str
    score: float
    magnitude: float
    timestamp: int

    @property
    def label(self) -> str:
        if self.score > 0.2:
            return "bullish"
        if self.score < -0.2:
            return "bearish"
        return "neutral"


class FearAndGreedFeed:
    URL = "https://api.alternative.me/fng/?limit=1&format=json"

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._latest: SentimentScore | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self._session

    async def fetch(self) -> SentimentScore | None:
        session = await self._get_session()
        try:
            async with session.get(self.URL) as resp:
                data = await resp.json(content_type=None)
                item = data.get("data", [{}])[0]
                value = int(item.get("value", 50))
                normalized = (value - 50) / 50.0
                self._latest = SentimentScore(
                    symbol="CRYPTO",
                    source="fear_and_greed",
                    score=normalized,
                    magnitude=abs(normalized),
                    timestamp=int(time.time()),
                )
                return self._latest
        except Exception as exc:
            logger.debug("Fear & Greed fetch error: {}", exc)
            return None

    def get_latest(self) -> SentimentScore | None:
        return self._latest

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


class SentimentManager:
    def __init__(self, config: Config, event_bus: EventBus) -> None:
        self.config = config
        self.event_bus = event_bus
        self._fear_greed = FearAndGreedFeed()
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("Sentiment manager started")
        while self._running:
            score = await self._fear_greed.fetch()
            if score:
                await self.event_bus.publish("SENTIMENT", score)
                logger.debug("Sentiment: {} ({:.2f})", score.label, score.score)
            await asyncio.sleep(3600)

    async def stop(self) -> None:
        self._running = False
        await self._fear_greed.close()
