from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

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


# ---------------------------------------------------------------------------
# Individual sentiment feeds
# ---------------------------------------------------------------------------

class FearAndGreedFeed:
    """Crypto Fear & Greed Index from alternative.me."""
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
                if resp.status != 200:
                    logger.debug("Fear & Greed returned status {}", resp.status)
                    return None
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


class CoinGlassLongShortFeed:
    """Long/short ratio from CoinGlass public API."""
    URL = "https://open-api.coinglass.com/public/v2/long_short"

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None
        self._latest: SentimentScore | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self._session

    async def fetch(self, symbol: str = "BTC") -> SentimentScore | None:
        session = await self._get_session()
        try:
            params = {"symbol": symbol, "time_type": "4"}  # 4h
            async with session.get(self.URL, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                items = data.get("data", [])
                if not items:
                    return None
                # Aggregate across exchanges
                long_rates = [float(it.get("longRate", 50)) for it in items if "longRate" in it]
                if not long_rates:
                    return None
                avg_long_pct = sum(long_rates) / len(long_rates)
                # Normalize: 50% long = neutral, >50 = bullish, <50 = bearish
                normalized = (avg_long_pct - 50) / 50.0
                self._latest = SentimentScore(
                    symbol=symbol,
                    source="coinglass_long_short",
                    score=max(-1.0, min(1.0, normalized)),
                    magnitude=abs(normalized),
                    timestamp=int(time.time()),
                )
                return self._latest
        except Exception as exc:
            logger.debug("CoinGlass long/short fetch error: {}", exc)
            return None

    def get_latest(self) -> SentimentScore | None:
        return self._latest

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


class CoinGeckoTrendingFeed:
    """CoinGecko trending coins as a market-heat proxy."""
    URL = "https://api.coingecko.com/api/v3/search/trending"

    # Map top crypto IDs to our trading symbols
    _KNOWN_SYMBOLS = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL"}

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
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                coins = data.get("coins", [])
                if not coins:
                    return None
                # Count how many of the top-7 trending coins are major caps
                top_ids = [c.get("item", {}).get("id", "") for c in coins[:7]]
                major_count = sum(1 for cid in top_ids if cid in self._KNOWN_SYMBOLS)
                # If major coins are trending, market is active/bullish
                # If none are trending, retail is chasing alts (often a top signal)
                if major_count >= 2:
                    score = 0.3  # mild bullish — mainstream interest
                elif major_count == 0:
                    score = -0.2  # mild bearish — alt-season froth
                else:
                    score = 0.0
                self._latest = SentimentScore(
                    symbol="CRYPTO",
                    source="coingecko_trending",
                    score=score,
                    magnitude=abs(score),
                    timestamp=int(time.time()),
                )
                return self._latest
        except Exception as exc:
            logger.debug("CoinGecko trending fetch error: {}", exc)
            return None

    def get_latest(self) -> SentimentScore | None:
        return self._latest

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ---------------------------------------------------------------------------
# Aggregated manager
# ---------------------------------------------------------------------------

class SentimentManager:
    """Aggregates multiple sentiment feeds into a single composite score."""

    # Source weights for composite score
    _WEIGHTS: dict[str, float] = {
        "fear_and_greed": 0.40,
        "coinglass_long_short": 0.35,
        "coingecko_trending": 0.25,
    }

    def __init__(self, config: Config, event_bus: EventBus) -> None:
        self.config = config
        self.event_bus = event_bus
        self._fear_greed = FearAndGreedFeed()
        self._long_short = CoinGlassLongShortFeed()
        self._trending = CoinGeckoTrendingFeed()
        self._running = False
        self._poll_interval = config.get_value("sentiment", "poll_interval_seconds", default=1800)
        self._latest_composite: SentimentScore | None = None

    async def run(self) -> None:
        self._running = True
        logger.info("Sentiment manager started (poll={}s, sources=3)", self._poll_interval)
        while self._running:
            scores = await self._fetch_all()
            composite = self._compute_composite(scores)
            if composite:
                self._latest_composite = composite
                await self.event_bus.publish("SENTIMENT", composite)
                logger.debug("Sentiment composite: {} ({:.2f}) from {} sources",
                             composite.label, composite.score, len(scores))
            # Also publish individual scores for downstream consumers
            for s in scores:
                await self.event_bus.publish("SENTIMENT_SOURCE", s)
            await asyncio.sleep(self._poll_interval)

    async def _fetch_all(self) -> list[SentimentScore]:
        """Fetch from all sources concurrently, return whatever succeeds."""
        results: list[SentimentScore] = []
        tasks = [
            self._fear_greed.fetch(),
            self._long_short.fetch(),
            self._trending.fetch(),
        ]
        fetched = await asyncio.gather(*tasks, return_exceptions=True)
        for result in fetched:
            if isinstance(result, SentimentScore):
                results.append(result)
            elif isinstance(result, Exception):
                logger.debug("Sentiment source failed: {}", result)
        return results

    def _compute_composite(self, scores: list[SentimentScore]) -> SentimentScore | None:
        if not scores:
            return None
        weighted_sum = 0.0
        total_weight = 0.0
        for s in scores:
            w = self._WEIGHTS.get(s.source, 0.1)
            weighted_sum += s.score * w
            total_weight += w
        if total_weight == 0:
            return None
        composite_score = weighted_sum / total_weight
        return SentimentScore(
            symbol="CRYPTO",
            source="composite",
            score=round(composite_score, 4),
            magnitude=round(abs(composite_score), 4),
            timestamp=int(time.time()),
        )

    def get_latest(self) -> SentimentScore | None:
        return self._latest_composite

    def get_all_sources(self) -> dict[str, SentimentScore | None]:
        return {
            "fear_and_greed": self._fear_greed.get_latest(),
            "coinglass_long_short": self._long_short.get_latest(),
            "coingecko_trending": self._trending.get_latest(),
        }

    async def stop(self) -> None:
        self._running = False
        await asyncio.gather(
            self._fear_greed.close(),
            self._long_short.close(),
            self._trending.close(),
            return_exceptions=True,
        )
