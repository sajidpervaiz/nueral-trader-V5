from __future__ import annotations

import asyncio
import math
from collections import deque
from dataclasses import dataclass
from typing import Any

from loguru import logger

from core.config import Config
from core.event_bus import EventBus


@dataclass
class VolatilityIndex:
    symbol: str
    method: str
    value: float
    annualized: float
    lookback_days: int
    timestamp: int

    @property
    def regime(self) -> str:
        if self.annualized < 0.30:
            return "low"
        if self.annualized < 0.60:
            return "medium"
        if self.annualized < 1.20:
            return "high"
        return "extreme"


class VIXProxy:
    """Computes a VIX-like volatility index from price returns."""

    PERIODS_PER_YEAR = 365 * 24 * 12

    def __init__(self, config: Config, event_bus: EventBus) -> None:
        self.config = config
        self.event_bus = event_bus
        self._price_history: dict[str, deque[float]] = {}
        self._running = False
        self._last_published: dict[str, float] = {}
        self._max_lookback = 30 * 24 * 12

    def _register_price(self, symbol: str, price: float) -> None:
        if symbol not in self._price_history:
            self._price_history[symbol] = deque(maxlen=self._max_lookback)
        self._price_history[symbol].append(price)

    def _calc_realized_vol(self, prices: deque[float], lookback: int) -> float:
        arr = list(prices)[-lookback:]
        if len(arr) < 2:
            return 0.0
        returns = [
            math.log(arr[i] / arr[i - 1])
            for i in range(1, len(arr))
            if arr[i - 1] > 0
        ]
        if not returns:
            return 0.0
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / len(returns)
        return math.sqrt(variance)

    def compute(self, symbol: str) -> list[VolatilityIndex]:
        import time
        macro_cfg = self.config.get_value("macro", "vix_proxy") or {}
        lookback_days: list[int] = macro_cfg.get("lookback_periods", [7, 14, 30])

        history = self._price_history.get(symbol)
        if history is None:
            return list()

        results = []
        now = int(time.time())
        for days in lookback_days:
            periods = days * 24 * 12
            vol_per_period = self._calc_realized_vol(history, periods)
            annualized = vol_per_period * math.sqrt(self.PERIODS_PER_YEAR)
            results.append(VolatilityIndex(
                symbol=symbol,
                method="realized_vol",
                value=vol_per_period,
                annualized=annualized,
                lookback_days=days,
                timestamp=now,
            ))
        return results

    async def _handle_tick(self, payload: Any) -> None:
        tick = payload
        if not hasattr(tick, "price") or not hasattr(tick, "symbol"):
            return
        self._register_price(tick.symbol, tick.price)

        macro_cfg = self.config.get_value("macro", "vix_proxy") or {}
        if not macro_cfg.get("enabled", False):
            return

        last = self._last_published.get(tick.symbol, 0.0)
        import time
        if time.time() - last < 300:
            return

        vix_readings = self.compute(tick.symbol)
        if vix_readings:
            await self.event_bus.publish("VOLATILITY_INDEX", vix_readings)
            self._last_published[tick.symbol] = time.time()
            logger.debug(
                "VIX proxy for {}: {:.2%} (30d annualized)",
                tick.symbol,
                next((v.annualized for v in vix_readings if v.lookback_days == 30), 0.0),
            )

    async def run(self) -> None:
        self._running = True
        self.event_bus.subscribe("TICK", self._handle_tick)
        logger.info("VIX proxy started")
        while self._running:
            await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False
        self.event_bus.unsubscribe("TICK", self._handle_tick)
