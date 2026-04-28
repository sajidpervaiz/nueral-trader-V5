"""Unit tests for the tick processor / candle aggregator."""
from __future__ import annotations

import time

import pytest

from data_ingestion.normalizer import Tick
from data_ingestion.tick_processor import CandleAggregator, TickBatchParser


@pytest.fixture
def aggregator() -> CandleAggregator:
    return CandleAggregator("binance", "BTC/USDT", 60)


def _tick(ts_s: float, price: float, volume: float = 1.0) -> Tick:
    return Tick(
        exchange="binance",
        symbol="BTC/USDT",
        timestamp_us=int(ts_s * 1_000_000),
        price=price,
        volume=volume,
    )


class TestCandleAggregator:
    def test_first_tick_no_candle(self, aggregator: CandleAggregator) -> None:
        candle = aggregator.add_tick(_tick(1_000.0, 50000))
        assert candle is None

    def test_second_tick_same_bucket_no_candle(self, aggregator: CandleAggregator) -> None:
        aggregator.add_tick(_tick(960.0, 50000))
        candle = aggregator.add_tick(_tick(1_010.0, 50100))
        assert candle is None

    def test_new_bucket_completes_candle(self, aggregator: CandleAggregator) -> None:
        aggregator.add_tick(_tick(960.0, 50000))
        aggregator.add_tick(_tick(1_010.0, 50100))
        candle = aggregator.add_tick(_tick(1_020.0, 50200))
        assert candle is not None
        assert candle.open == 50000.0
        assert candle.high == 50100.0
        assert candle.low == 50000.0
        assert candle.close == 50100.0
        assert candle.volume == pytest.approx(2.0)
        assert candle.num_trades == 2

    def test_single_tick_candle(self, aggregator: CandleAggregator) -> None:
        aggregator.add_tick(_tick(960.0, 50000, volume=3.0))
        candle = aggregator.add_tick(_tick(1_020.0, 50200))
        assert candle is not None
        assert candle.open == candle.high == candle.low == candle.close == 50000.0
        assert candle.volume == pytest.approx(3.0)
        assert candle.num_trades == 1


class TestTickBatchParser:
    def test_parse_valid_batch(self) -> None:
        parser = TickBatchParser()
        raw = [
            {"symbol": "BTC/USDT", "price": 50000.0, "volume": 1.0, "timestamp_us": 1_000_000_000},
            {"symbol": "ETH/USDT", "price": 3000.0, "volume": 2.0, "timestamp_us": 1_000_000_001},
        ]
        ticks = parser.parse_batch("binance", raw)
        assert len(ticks) == 2
        assert ticks[0].price == 50000.0
        assert ticks[1].symbol == "ETH/USDT"

    def test_parse_skips_invalid(self) -> None:
        parser = TickBatchParser()
        raw = [
            {"symbol": "BTC/USDT", "price": 50000.0},
            {"no_symbol": True},
            {"symbol": "ETH/USDT", "price": "not_a_number"},
        ]
        ticks = parser.parse_batch("binance", raw)
        assert len(ticks) == 1
        assert ticks[0].price == 50000.0

    def test_parse_empty_batch(self) -> None:
        parser = TickBatchParser()
        assert parser.parse_batch("binance", []) == []
