"""Orderbook depth feed — polls exchange REST API and publishes ORDERBOOK_UPDATE events.

Fetches top-of-book snapshots at a configurable interval and publishes them
to the event bus so the signal generator can compute bid/ask imbalance.
"""
from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
from loguru import logger

from core.config import Config
from core.event_bus import EventBus


_BINANCE_DEPTH_URL = "https://fapi.binance.com/fapi/v1/depth"
_BINANCE_TESTNET_DEPTH_URL = "https://testnet.binancefuture.com/fapi/v1/depth"


def _symbol_to_binance(symbol: str) -> str:
    """Convert 'BTC/USDT:USDT' → 'BTCUSDT'."""
    return symbol.replace("/", "").replace(":USDT", "").upper()


class OrderbookFeed:
    """Periodically fetches orderbook snapshots and publishes depth events."""

    def __init__(
        self,
        config: Config,
        event_bus: EventBus,
        poll_interval: float = 30.0,
        depth: int = 20,
    ) -> None:
        self.config = config
        self.event_bus = event_bus
        self._interval = poll_interval
        self._depth = depth
        self._running = False
        self._session: aiohttp.ClientSession | None = None

    def _get_symbols(self) -> list[str]:
        binance_cfg = self.config.get_value("exchanges", "binance") or {}
        return binance_cfg.get("symbols", [])

    def _get_base_url(self) -> str:
        binance_cfg = self.config.get_value("exchanges", "binance") or {}
        if binance_cfg.get("testnet", True):
            return _BINANCE_TESTNET_DEPTH_URL
        return _BINANCE_DEPTH_URL

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def _fetch_depth(self, symbol: str) -> dict[str, Any] | None:
        session = await self._get_session()
        binance_sym = _symbol_to_binance(symbol)
        url = self._get_base_url()
        try:
            async with session.get(
                url,
                params={"symbol": binance_sym, "limit": self._depth},
            ) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
        except Exception as exc:
            logger.debug("Orderbook fetch error for {}: {}", symbol, exc)
            return None

    async def _poll_once(self) -> None:
        symbols = self._get_symbols()
        if not symbols:
            return

        for symbol in symbols:
            data = await self._fetch_depth(symbol)
            if data is None:
                continue

            bids = [
                (float(price), float(qty))
                for price, qty in data.get("bids", [])
            ]
            asks = [
                (float(price), float(qty))
                for price, qty in data.get("asks", [])
            ]

            if not bids and not asks:
                continue

            await self.event_bus.publish("ORDERBOOK_UPDATE", {
                "exchange": "binance",
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
            })

    async def run(self) -> None:
        self._running = True
        symbols = self._get_symbols()
        logger.info(
            "OrderbookFeed started (interval={}s, depth={}, symbols={})",
            self._interval, self._depth, len(symbols),
        )
        while self._running:
            try:
                await self._poll_once()
            except Exception as exc:
                logger.warning("OrderbookFeed poll error: {}", exc)
            await asyncio.sleep(self._interval)

    async def stop(self) -> None:
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()
