from __future__ import annotations

from unittest.mock import MagicMock

from core.event_bus import EventBus
from data_ingestion.funding_feed import FundingRate, FundingRateFeed


def test_detect_arbitrage_opportunities_finds_spread() -> None:
    config = MagicMock()
    bus = EventBus()
    feed = FundingRateFeed(config=config, event_bus=bus)

    rates = [
        FundingRate(exchange="binance", symbol="BTCUSDT", rate=-0.0002, predicted_rate=None, next_funding_time=0, timestamp=1),
        FundingRate(exchange="bybit", symbol="BTCUSDT", rate=0.0006, predicted_rate=None, next_funding_time=0, timestamp=1),
        FundingRate(exchange="okx", symbol="ETHUSDT", rate=0.0001, predicted_rate=None, next_funding_time=0, timestamp=1),
    ]

    opps = feed.detect_arbitrage_opportunities(rates, min_spread_bps=2.0)

    assert len(opps) == 1
    opp = opps[0]
    assert opp.symbol == "BTCUSDT"
    assert opp.long_exchange == "binance"
    assert opp.short_exchange == "bybit"
    assert opp.spread_bps > 2.0


def test_detect_arbitrage_opportunities_normalizes_symbol_keys() -> None:
    config = MagicMock()
    bus = EventBus()
    feed = FundingRateFeed(config=config, event_bus=bus)

    rates = [
        FundingRate(exchange="okx", symbol="BTC-USDT-SWAP", rate=-0.0001, predicted_rate=None, next_funding_time=0, timestamp=1),
        FundingRate(exchange="binance", symbol="BTCUSDT", rate=0.0004, predicted_rate=None, next_funding_time=0, timestamp=1),
    ]

    opps = feed.detect_arbitrage_opportunities(rates, min_spread_bps=2.0)
    assert len(opps) == 1
