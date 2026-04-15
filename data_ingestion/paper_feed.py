"""Paper-mode market data feed.

Polls Binance public klines API (no auth) and emits CANDLE events
into the EventBus so the full signal → trade pipeline works without
live websocket connections or API keys.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
from loguru import logger

from core.event_bus import EventBus
from data_ingestion.normalizer import Candle

# Binance futures public klines endpoint (no auth needed)
KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

TF_MAP = {
    "1m": ("1m", 60),
    "5m": ("5m", 300),
    "15m": ("15m", 900),
    "1h": ("1h", 3600),
    "4h": ("4h", 14400),
    "1d": ("1d", 86400),
}


class PaperFeed:
    """Fetches candles from Binance public API and publishes CANDLE events."""

    def __init__(
        self,
        event_bus: EventBus,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        poll_interval: float = 30.0,
        data_manager: Any = None,
    ) -> None:
        self.event_bus = event_bus
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self.timeframes = timeframes or ["1m", "15m", "1h", "4h"]
        self.poll_interval = poll_interval
        self._data_manager = data_manager
        self._running = False
        self._seeding_complete = False
        self._client: httpx.AsyncClient | None = None
        self._last_candle_time: dict[str, int] = {}

    def _binance_symbol(self, sym: str) -> str:
        """Normalize symbol to Binance format: BTC/USDT:USDT -> BTCUSDT"""
        return sym.replace("/", "").replace(":USDT", "").upper()

    def _internal_symbol(self, binance_sym: str) -> str:
        """Convert BTCUSDT -> BTC/USDT:USDT for internal use."""
        for suffix in ("USDT", "BUSD"):
            if binance_sym.endswith(suffix):
                base = binance_sym[: -len(suffix)]
                return f"{base}/{suffix}:{suffix}"
        return binance_sym

    async def _fetch_klines(
        self, symbol: str, timeframe: str, limit: int = 100,
    ) -> list[Candle]:
        """Fetch klines from Binance public API."""
        if self._client is None:
            return []
        binance_tf = TF_MAP.get(timeframe, (timeframe, 60))[0]
        try:
            resp = await self._client.get(
                KLINES_URL,
                params={
                    "symbol": self._binance_symbol(symbol),
                    "interval": binance_tf,
                    "limit": limit,
                },
                timeout=10.0,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.debug("PaperFeed klines error {}/{}: {}", symbol, timeframe, exc)
            return []

        candles = []
        internal_sym = self._internal_symbol(self._binance_symbol(symbol))
        for k in data:
            candles.append(Candle(
                exchange="binance",
                symbol=internal_sym,
                timeframe=timeframe,
                timestamp=int(k[0]) // 1000,
                open=float(k[1]),
                high=float(k[2]),
                low=float(k[3]),
                close=float(k[4]),
                volume=float(k[5]),
                num_trades=int(k[8]) if len(k) > 8 else 0,
            ))
        return candles

    async def _poll_once(self) -> int:
        """Poll all symbols/timeframes and emit new candles. Returns count emitted."""
        emitted = 0
        for sym in self.symbols:
            for tf in self.timeframes:
                candles = await self._fetch_klines(sym, tf, limit=100)
                key = f"{sym}:{tf}"
                last_ts = self._last_candle_time.get(key, 0)

                for c in candles:
                    if c.timestamp > last_ts:
                        await self.event_bus.publish("CANDLE", c)
                        emitted += 1

                if candles:
                    self._last_candle_time[key] = candles[-1].timestamp
        return emitted

    async def seed_history(self) -> None:
        """Seed DataManager with historical candles on startup.
        
        If data_manager is available, seeds directly (fast, no event bus overhead).
        Otherwise falls back to publishing through event bus.
        """
        logger.info("PaperFeed: seeding historical candles...")
        self.event_bus._seeding = True  # Signal pipeline skips during seeding
        total = 0
        for sym in self.symbols:
            for tf in self.timeframes:
                candles = await self._fetch_klines(sym, tf, limit=200)
                if self._data_manager is not None:
                    # Direct inject — bypasses EventBus queue, skip indicator compute per-candle
                    for c in candles:
                        self._data_manager._store_candle(c.exchange, c.symbol, c.timeframe, c, compute=False)
                        total += 1
                else:
                    for c in candles:
                        await self.event_bus.publish("CANDLE", c)
                        total += 1
                        if total % 50 == 0:
                            await asyncio.sleep(0)
                if candles:
                    key = f"{sym}:{tf}"
                    self._last_candle_time[key] = candles[-1].timestamp
                # Yield to event loop between symbol/tf combos so HTTP stays responsive
                await asyncio.sleep(0)

        # Bulk recompute indicators once after all candles are loaded
        if self._data_manager is not None:
            self._data_manager.recompute_all()

        self.event_bus._seeding = False
        logger.info("PaperFeed: seeded {} candles across {} symbols × {} timeframes",
                     total, len(self.symbols), len(self.timeframes))
        self._seeding_complete = True

    async def run(self, seed_only: bool = False) -> None:
        """Main loop: seed history, then poll for new candles.
        
        Args:
            seed_only: If True, seed history and return (used in live mode where WS provides data).
        """
        self._running = True
        self._client = httpx.AsyncClient()
        try:
            await self.seed_history()
            if seed_only:
                logger.info("PaperFeed: seed-only mode — polling disabled (live WS provides data)")
                return
            logger.info("PaperFeed started — polling every {}s for {}", 
                        self.poll_interval, self.symbols)
            while self._running:
                await asyncio.sleep(self.poll_interval)
                count = await self._poll_once()
                if count > 0:
                    logger.debug("PaperFeed: emitted {} new candles", count)
        except asyncio.CancelledError:
            pass
        finally:
            if self._client:
                await self._client.aclose()
                self._client = None
            self._running = False
            logger.info("PaperFeed stopped")

    async def stop(self) -> None:
        self._running = False
