"""Unit tests for the tick and candle validators."""
from __future__ import annotations

import time

import pytest

from data_ingestion.normalizer import Tick, Candle
from data_ingestion.validators import TickValidator, CandleValidator


@pytest.fixture
def tick_validator() -> TickValidator:
    return TickValidator(max_age_s=60)


@pytest.fixture
def candle_validator() -> CandleValidator:
    return CandleValidator()


class TestTickValidator:
    def _fresh_tick(self, price: float = 50000.0, volume: float = 1.0, side: str = "buy") -> Tick:
        return Tick(
            exchange="binance",
            symbol="BTC/USDT",
            timestamp_us=time.time_ns() // 1000,
            price=price,
            volume=volume,
            side=side,
        )

    def test_valid_tick_passes(self, tick_validator: TickValidator) -> None:
        assert tick_validator.validate(self._fresh_tick()) is True

    def test_zero_price_rejected(self, tick_validator: TickValidator) -> None:
        assert tick_validator.validate(self._fresh_tick(price=0.0)) is False

    def test_negative_price_rejected(self, tick_validator: TickValidator) -> None:
        assert tick_validator.validate(self._fresh_tick(price=-1.0)) is False

    def test_stale_tick_rejected(self, tick_validator: TickValidator) -> None:
        stale = Tick(
            exchange="binance",
            symbol="BTC/USDT",
            timestamp_us=int((time.time() - 300) * 1_000_000),
            price=50000.0,
            volume=1.0,
        )
        assert tick_validator.validate(stale) is False

    def test_suspicious_price_jump_rejected(self, tick_validator: TickValidator) -> None:
        tick_validator.validate(self._fresh_tick(price=50000.0))
        tick2 = self._fresh_tick(price=65000.0)  # 30% jump exceeds 25% threshold
        assert tick_validator.validate(tick2) is False

    def test_small_price_move_passes(self, tick_validator: TickValidator) -> None:
        tick_validator.validate(self._fresh_tick(price=50000.0))
        tick2 = self._fresh_tick(price=50050.0)
        assert tick_validator.validate(tick2) is True


class TestCandleValidator:
    def _valid_candle(self) -> Candle:
        return Candle(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="15m",
            timestamp=int(time.time()),
            open=50000,
            high=50500,
            low=49800,
            close=50200,
            volume=100,
        )

    def test_valid_candle_passes(self, candle_validator: CandleValidator) -> None:
        assert candle_validator.validate(self._valid_candle()) is True

    def test_high_below_low_rejected(self, candle_validator: CandleValidator) -> None:
        c = self._valid_candle()
        c.high, c.low = 49000, 51000
        assert candle_validator.validate(c) is False

    def test_close_above_high_rejected(self, candle_validator: CandleValidator) -> None:
        c = self._valid_candle()
        c.close = 60000
        assert candle_validator.validate(c) is False

    def test_zero_open_rejected(self, candle_validator: CandleValidator) -> None:
        c = self._valid_candle()
        c.open = 0
        assert candle_validator.validate(c) is False
