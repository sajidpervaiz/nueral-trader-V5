"""Crypto news feed — fetches headlines and publishes NEWS_SENTIMENT events.

Primary: CryptoCompare news API (free, no key required for basic access).
Fallback: CoinGecko status updates.
Each headline is scored with a keyword-based sentiment classifier.
"""
from __future__ import annotations

import asyncio
import re
import time
from typing import Any

import aiohttp
from loguru import logger

from core.config import Config
from core.event_bus import EventBus

# ── Keyword-based sentiment classifier ────────────────────────────────────────
_BULLISH_KEYWORDS = re.compile(
    r"\b(bull|surge|rally|gain|soar|jump|record high|all.time high|ath|"
    r"breakout|adoption|approval|etf approv|upgrade|partnership|buy)\b",
    re.IGNORECASE,
)
_BEARISH_KEYWORDS = re.compile(
    r"\b(bear|crash|dump|plunge|drop|sell.off|selloff|hack|exploit|"
    r"ban|regulation|sec sue|lawsuit|fraud|liquidat|fear|panic)\b",
    re.IGNORECASE,
)


def classify_sentiment(text: str) -> float:
    """Return a sentiment score in [-1.0, 1.0] from headline text."""
    bull = len(_BULLISH_KEYWORDS.findall(text))
    bear = len(_BEARISH_KEYWORDS.findall(text))
    total = bull + bear
    if total == 0:
        return 0.0
    return max(-1.0, min(1.0, (bull - bear) / total))


class NewsFeed:
    """Periodically fetches crypto news and publishes sentiment events."""

    CRYPTOCOMPARE_URL = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&sortOrder=latest"
    COINGECKO_URL = "https://api.coingecko.com/api/v3/status_updates?per_page=20"

    def __init__(
        self,
        config: Config,
        event_bus: EventBus,
        fetch_interval: float = 300.0,
    ) -> None:
        self.config = config
        self.event_bus = event_bus
        self._interval = fetch_interval
        self._running = False
        self._session: aiohttp.ClientSession | None = None
        self._seen_ids: set[str] = set()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    async def _fetch_cryptocompare(self) -> list[dict[str, Any]]:
        session = await self._get_session()
        try:
            async with session.get(self.CRYPTOCOMPARE_URL) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                return data.get("Data", [])
        except Exception as exc:
            logger.debug("CryptoCompare news fetch error: {}", exc)
            return []

    async def _fetch_coingecko(self) -> list[dict[str, Any]]:
        session = await self._get_session()
        try:
            async with session.get(self.COINGECKO_URL) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                raw = data.get("status_updates", [])
                return [
                    {
                        "id": str(item.get("created_at", "")),
                        "title": item.get("description", "")[:200],
                        "published_on": int(time.time()),
                    }
                    for item in raw
                ]
        except Exception as exc:
            logger.debug("CoinGecko news fetch error: {}", exc)
            return []

    async def _poll_once(self) -> None:
        articles = await self._fetch_cryptocompare()
        if not articles:
            articles = await self._fetch_coingecko()
        if not articles:
            return

        for article in articles[:20]:
            aid = str(article.get("id", article.get("url", "")))
            if aid in self._seen_ids:
                continue
            self._seen_ids.add(aid)

            title = article.get("title", article.get("description", ""))
            body = article.get("body", "")
            text = f"{title} {body[:300]}"
            sentiment = classify_sentiment(text)
            ts = int(article.get("published_on", time.time()))

            await self.event_bus.publish("NEWS_SENTIMENT", {
                "sentiment": sentiment,
                "timestamp": ts,
                "title": title[:200],
                "source": "cryptocompare",
            })

        # Limit memory — keep last 500 IDs
        if len(self._seen_ids) > 500:
            self._seen_ids = set(list(self._seen_ids)[-300:])

    async def run(self) -> None:
        self._running = True
        logger.info("NewsFeed started (interval={}s)", self._interval)
        while self._running:
            try:
                await self._poll_once()
            except Exception as exc:
                logger.warning("NewsFeed poll error: {}", exc)
            await asyncio.sleep(self._interval)

    async def stop(self) -> None:
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()
