"""Unit tests for CEX websocket subscription formatting."""
from __future__ import annotations

from data_ingestion.cex_websocket import CEXWebSocketManager


class _DummyConfig:
    pass


class _DummyEventBus:
    pass


def _manager() -> CEXWebSocketManager:
    return CEXWebSocketManager(config=_DummyConfig(), event_bus=_DummyEventBus())


def test_kraken_subscription_pairs_are_mapped_per_symbol() -> None:
    manager = _manager()

    msgs = manager._build_subscribe_msg(
        "kraken",
        ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USD"],
    )

    assert len(msgs) == 1
    assert msgs[0]["event"] == "subscribe"
    assert msgs[0]["subscription"]["name"] == "trade"
    assert msgs[0]["pair"] == ["XBT/USD", "ETH/USD", "SOL/USD"]


def test_kraken_compact_symbol_is_converted() -> None:
    manager = _manager()

    msgs = manager._build_subscribe_msg("kraken", ["BTCUSDT"])

    assert msgs[0]["pair"] == ["XBT/USD"]
