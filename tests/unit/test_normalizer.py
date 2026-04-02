"""Unit tests for the tick normalizer."""
from __future__ import annotations

import pytest

from data_ingestion.normalizer import Normalizer, Tick


@pytest.fixture
def normalizer() -> Normalizer:
    return Normalizer()


class TestNormalizerBinance:
    def test_binance_agg_trade(self, normalizer: Normalizer) -> None:
        raw = {"s": "BTCUSDT", "T": 1_700_000_000_000, "p": "50000.00", "q": "0.1", "m": False, "t": "123"}
        tick = normalizer.normalize_tick("binance", raw)
        assert tick is not None
        assert tick.exchange == "binance"
        assert tick.price == 50000.0
        assert tick.volume == 0.1
        assert tick.side == "buy"
        assert tick.trade_id == "123"

    def test_binance_sell_side(self, normalizer: Normalizer) -> None:
        raw = {"s": "ETHUSDT", "T": 1_700_000_000_000, "p": "3000.00", "q": "0.5", "m": True, "t": "456"}
        tick = normalizer.normalize_tick("binance", raw)
        assert tick is not None
        assert tick.side == "sell"

    def test_binance_invalid_price(self, normalizer: Normalizer) -> None:
        raw = {"s": "BTCUSDT", "T": 1_700_000_000_000, "p": "not_a_number", "q": "0.1", "m": False}
        tick = normalizer.normalize_tick("binance", raw)
        assert tick is None

    def test_bybit_tick(self, normalizer: Normalizer) -> None:
        raw = {
            "data": [{"s": "BTCUSDT", "T": 1_700_000_000_000, "p": "50100", "v": "0.2", "S": "Buy", "i": "789"}]
        }
        tick = normalizer.normalize_tick("bybit", raw)
        assert tick is not None
        assert tick.exchange == "bybit"
        assert tick.price == 50100.0

    def test_okx_tick(self, normalizer: Normalizer) -> None:
        raw = {
            "data": [{"instId": "BTC-USDT-SWAP", "ts": "1700000000000", "px": "49900", "sz": "10", "side": "sell", "tradeId": "abc"}]
        }
        tick = normalizer.normalize_tick("okx", raw)
        assert tick is not None
        assert tick.exchange == "okx"
        assert tick.price == 49900.0
        assert tick.side == "sell"

    def test_kraken_tick(self, normalizer: Normalizer) -> None:
        raw = [
            42,
            [["50250.1", "0.25", "1700000000.123", "b", "l", "trade-kraken-1"]],
            "trade",
            "XBT/USD",
        ]
        tick = normalizer.normalize_tick("kraken", raw)
        assert tick is not None
        assert tick.exchange == "kraken"
        assert tick.symbol == "XBT/USD"
        assert tick.price == 50250.1
        assert tick.volume == 0.25
        assert tick.side == "buy"
        assert tick.trade_id == "trade-kraken-1"

    def test_kraken_malformed_payload_returns_none(self, normalizer: Normalizer) -> None:
        tick = normalizer.normalize_tick("kraken", [123, "bad-payload"])
        assert tick is None

    def test_unknown_exchange_returns_none(self, normalizer: Normalizer) -> None:
        tick = normalizer.normalize_tick("unknown_exchange", {"price": "50000"})
        assert tick is None
