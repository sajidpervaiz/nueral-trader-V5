from __future__ import annotations

import time
from typing import Any

from loguru import logger

from data_ingestion.normalizer import Tick, Candle


MAX_AGE_SECONDS = 60
MAX_PRICE_DEVIATION_PCT = 0.10
MIN_VOLUME = 0.0


class TickValidator:
    def __init__(self, max_age_s: float = MAX_AGE_SECONDS) -> None:
        self._max_age_us = max_age_s * 1_000_000
        self._last_prices: dict[str, float] = {}

    def validate(self, tick: Tick) -> bool:
        now_us = time.time_ns() // 1000

        if tick.price <= 0:
            logger.debug("Invalid tick: price <= 0 for {}/{}", tick.exchange, tick.symbol)
            return False

        if tick.volume < MIN_VOLUME:
            return False

        age_us = now_us - tick.timestamp_us
        if age_us > self._max_age_us:
            logger.debug(
                "Stale tick: age {}s for {}/{}",
                age_us / 1_000_000,
                tick.exchange,
                tick.symbol,
            )
            return False

        key = f"{tick.exchange}:{tick.symbol}"
        last = self._last_prices.get(key)
        if last is not None and last > 0:
            deviation = abs(tick.price - last) / last
            if deviation > MAX_PRICE_DEVIATION_PCT:
                logger.warning(
                    "Suspicious tick: {:.1%} price jump for {}/{} ({} -> {})",
                    deviation,
                    tick.exchange,
                    tick.symbol,
                    last,
                    tick.price,
                )
                return False

        self._last_prices[key] = tick.price
        return True


class CandleValidator:
    def validate(self, candle: Candle) -> bool:
        if candle.high < candle.low:
            return False
        if candle.open <= 0 or candle.close <= 0:
            return False
        if candle.high < max(candle.open, candle.close):
            return False
        if candle.low > min(candle.open, candle.close):
            return False
        return True


class OrderBookValidator:
    def validate(self, bids: list[Any], asks: list[Any]) -> bool:
        if not bids or not asks:
            return False
        best_bid = float(bids[0][0]) if bids else 0.0
        best_ask = float(asks[0][0]) if asks else 0.0
        if best_bid <= 0 or best_ask <= 0:
            return False
        if best_bid >= best_ask:
            return False
        spread_pct = (best_ask - best_bid) / best_ask
        if spread_pct > 0.05:
            logger.debug("Wide spread: {:.2%}", spread_pct)
        return True
