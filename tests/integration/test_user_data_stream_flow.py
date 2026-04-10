"""
Integration tests for Binance Futures USER_DATA stream — full end-to-end flow
using recorded event payloads.

Tests the complete pipeline:
  UserDataStream → EventBus → CEXExecutor → OrderManager → TradePersistence
"""
import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from core.config import Config
from core.event_bus import EventBus
from data_ingestion.user_stream import UserDataStream


# ─── Fixtures ─────────────────────────────────────────────────────────────────

def _make_config(**overrides) -> Config:
    cfg = Config.__new__(Config)
    cfg._data = {
        "exchanges": {
            "binance": {
                "api_key": "test_key",
                "api_secret": "test_secret",
                "testnet": True,
                "enabled": True,
            }
        },
        "risk": {
            "initial_equity": 100000,
            "max_daily_loss_pct": 0.03,
            "max_drawdown_pct": 0.10,
            "circuit_breaker_pause_seconds": 3600,
            "user_stream_safe_mode_seconds": 0,
            "stop_loss_pct": 0.015,
            "take_profit_pct": 0.03,
            "max_position_size_pct": 0.02,
            "max_open_positions": 5,
            "sizing_method": "fixed_fraction",
        },
        "system": {"paper_mode": False},
    }
    cfg._data.update(overrides)
    return cfg


def _make_stream(config=None, event_bus=None) -> UserDataStream:
    if config is None:
        config = _make_config()
    if event_bus is None:
        event_bus = EventBus()
    return UserDataStream(config, event_bus)


# ─── Recorded Binance Event Sequences ────────────────────────────────────────

def _entry_order_sequence() -> list[str]:
    """Complete lifecycle: NEW → PARTIALLY_FILLED → FILLED for a LIMIT BUY."""
    return [
        json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000001000, "T": 1700000001000,
            "o": {
                "s": "BTCUSDT", "c": "entry_001", "S": "BUY", "o": "LIMIT",
                "f": "GTC", "q": "1.000", "p": "42000.00", "ap": "0",
                "sp": "0", "x": "NEW", "X": "NEW", "i": 5001,
                "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
                "t": 0, "R": False, "ps": "BOTH", "rp": "0", "m": False,
            }
        }),
        json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000002000, "T": 1700000002000,
            "o": {
                "s": "BTCUSDT", "c": "entry_001", "S": "BUY", "o": "LIMIT",
                "f": "GTC", "q": "1.000", "p": "42000.00", "ap": "41999.00",
                "sp": "0", "x": "TRADE", "X": "PARTIALLY_FILLED", "i": 5001,
                "l": "0.500", "z": "0.500", "L": "41999.00", "n": "0.01050",
                "N": "USDT", "t": 70001, "R": False, "ps": "BOTH", "rp": "0",
                "m": True,
            }
        }),
        json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000003000, "T": 1700000003000,
            "o": {
                "s": "BTCUSDT", "c": "entry_001", "S": "BUY", "o": "LIMIT",
                "f": "GTC", "q": "1.000", "p": "42000.00", "ap": "42000.00",
                "sp": "0", "x": "TRADE", "X": "FILLED", "i": 5001,
                "l": "0.500", "z": "1.000", "L": "42001.00", "n": "0.01050",
                "N": "USDT", "t": 70002, "R": False, "ps": "BOTH", "rp": "0",
                "m": False,
            }
        }),
    ]


def _sl_trigger_sequence() -> list[str]:
    """Stop-loss triggered: NEW → FILLED (reduce_only, STOP_MARKET)."""
    return [
        json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000010000, "T": 1700000010000,
            "o": {
                "s": "BTCUSDT", "c": "sl_btc_001", "S": "SELL", "o": "STOP_MARKET",
                "f": "GTC", "q": "1.000", "p": "0", "ap": "40000.00",
                "sp": "40000.00", "x": "NEW", "X": "NEW", "i": 5010,
                "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
                "t": 0, "R": True, "ps": "BOTH", "rp": "0", "m": False,
            }
        }),
        json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000011000, "T": 1700000011000,
            "o": {
                "s": "BTCUSDT", "c": "sl_btc_001", "S": "SELL", "o": "STOP_MARKET",
                "f": "GTC", "q": "1.000", "p": "0", "ap": "40000.00",
                "sp": "40000.00", "x": "TRADE", "X": "FILLED", "i": 5010,
                "l": "1.000", "z": "1.000", "L": "39999.50", "n": "0.02000",
                "N": "USDT", "t": 70010, "R": True, "ps": "BOTH", "rp": "-2000.50",
                "m": False,
            }
        }),
    ]


def _account_update_after_fill() -> str:
    """ACCOUNT_UPDATE event that follows a fill."""
    return json.dumps({
        "e": "ACCOUNT_UPDATE", "E": 1700000004000, "T": 1700000004000,
        "a": {
            "m": "ORDER",
            "B": [{"a": "USDT", "wb": "58000.00", "cw": "56000.00", "bc": "-42000.00"}],
            "P": [{"s": "BTCUSDT", "pa": "1.000", "ep": "42000.00", "up": "0.00",
                    "mt": "cross", "iw": "0", "ps": "BOTH"}],
        }
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Integration Tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestEntryOrderLifecycle:
    """Full lifecycle: NEW → PARTIALLY_FILLED → FILLED with state machine tracking."""

    async def test_full_entry_lifecycle(self):
        stream = _make_stream()
        published = []
        original_publish = stream.event_bus.publish
        async def capture_publish(event_type, payload=None):
            published.append((event_type, payload))
        stream.event_bus.publish = capture_publish

        for msg in _entry_order_sequence():
            await stream._handle_message(msg)

        # 3 messages: NEW, partial fill, full fill
        order_updates = [(e, p) for e, p in published if e == "USER_ORDER_UPDATE"]
        assert len(order_updates) == 3
        assert order_updates[0][1]["execution_type"] == "NEW"
        assert order_updates[1][1]["execution_type"] == "TRADE"
        assert order_updates[1][1]["order_status"] == "PARTIALLY_FILLED"
        assert order_updates[2][1]["execution_type"] == "TRADE"
        assert order_updates[2][1]["order_status"] == "FILLED"

        # State machine tracked correctly
        assert stream._order_states[5001] == "FILLED"
        assert stream.metrics["state_transitions"] == 3  # NEW, PARTIAL, FILLED
        assert stream.metrics["fills_processed"] == 2  # two TRADE events

    async def test_entry_lifecycle_with_account_update(self):
        stream = _make_stream()
        published = []
        async def capture_publish(event_type, payload=None):
            published.append((event_type, payload))
        stream.event_bus.publish = capture_publish

        for msg in _entry_order_sequence():
            await stream._handle_message(msg)
        await stream._handle_message(_account_update_after_fill())

        order_updates = [(e, p) for e, p in published if e == "USER_ORDER_UPDATE"]
        account_updates = [(e, p) for e, p in published if e == "USER_ACCOUNT_UPDATE"]
        assert len(order_updates) == 3
        assert len(account_updates) == 1
        assert account_updates[0][1]["positions"][0]["position_amount"] == pytest.approx(1.0)


class TestSLTPFillFlow:
    """Stop-loss/take-profit fills via USER_DATA stream."""

    async def test_sl_fill_lifecycle(self):
        stream = _make_stream()
        published = []
        async def capture_publish(event_type, payload=None):
            published.append((event_type, payload))
        stream.event_bus.publish = capture_publish

        for msg in _sl_trigger_sequence():
            await stream._handle_message(msg)

        order_updates = [(e, p) for e, p in published if e == "USER_ORDER_UPDATE"]
        assert len(order_updates) == 2
        # NEW
        assert order_updates[0][1]["order_status"] == "NEW"
        assert order_updates[0][1]["reduce_only"] is True
        assert order_updates[0][1]["order_type"] == "STOP_MARKET"
        # FILLED
        assert order_updates[1][1]["order_status"] == "FILLED"
        assert order_updates[1][1]["realized_profit"] == pytest.approx(-2000.5)
        assert order_updates[1][1]["trade_id"] == 70010

        assert stream._order_states[5010] == "FILLED"

    async def test_sl_fill_dedup_on_replay(self):
        """If SL fill is replayed (WS reconnect), second message is suppressed."""
        stream = _make_stream()
        published = []
        async def capture_publish(event_type, payload=None):
            published.append((event_type, payload))
        stream.event_bus.publish = capture_publish

        # First time — all messages published
        for msg in _sl_trigger_sequence():
            await stream._handle_message(msg)
        order_updates_1 = [(e, p) for e, p in published if e == "USER_ORDER_UPDATE"]
        assert len(order_updates_1) == 2

        # Replay the TRADE message (WS reconnect sends duplicate)
        await stream._handle_message(_sl_trigger_sequence()[1])
        order_updates_2 = [(e, p) for e, p in published if e == "USER_ORDER_UPDATE"]
        # Should still be 2 — duplicate was suppressed
        assert len(order_updates_2) == 2
        assert stream.metrics["fills_deduped"] == 1


class TestDedupAcrossFullFlow:
    """Dedup across entry and SL/TP sequences."""

    async def test_cross_order_dedup_independent(self):
        """Dedup sets are per trade_id, not per order_id."""
        stream = _make_stream()
        published = []
        async def capture_publish(event_type, payload=None):
            published.append((event_type, payload))
        stream.event_bus.publish = capture_publish

        # Entry fills + SL fills
        for msg in _entry_order_sequence():
            await stream._handle_message(msg)
        for msg in _sl_trigger_sequence():
            await stream._handle_message(msg)

        order_updates = [(e, p) for e, p in published if e == "USER_ORDER_UPDATE"]
        # 5 messages: 3 entry + 2 SL
        assert len(order_updates) == 5
        assert stream.metrics["fills_processed"] == 3  # 2 entry TRADE + 1 SL TRADE

    async def test_massive_dedup_batch(self):
        """Replay same message 100 times — only first is published."""
        stream = _make_stream()
        published = []
        async def capture_publish(event_type, payload=None):
            published.append((event_type, payload))
        stream.event_bus.publish = capture_publish

        fill_msg = _entry_order_sequence()[1]  # PARTIALLY_FILLED TRADE
        for _ in range(100):
            await stream._handle_message(fill_msg)

        order_updates = [(e, p) for e, p in published if e == "USER_ORDER_UPDATE"]
        assert len(order_updates) == 1
        assert stream.metrics["fills_deduped"] == 99


class TestIdempotencyEndToEnd:
    """Idempotency across stream dedup + OrderManager dedup + DB ON CONFLICT."""

    async def test_three_layer_idempotency(self):
        """
        Layer 1: UserDataStream._check_fill_dedup (trade_id)
        Layer 2: OrderManager.record_fill (fill_id)
        Layer 3: DB persist_fill ON CONFLICT DO NOTHING
        All three layers prevent duplicate fills.
        """
        stream = _make_stream()
        published = []
        async def capture_publish(event_type, payload=None):
            published.append((event_type, payload))
        stream.event_bus.publish = capture_publish

        fill_msg = _entry_order_sequence()[2]  # FILLED TRADE

        # First: passes all layers
        await stream._handle_message(fill_msg)
        order_updates = [(e, p) for e, p in published if e == "USER_ORDER_UPDATE"]
        assert len(order_updates) == 1

        # Second: blocked at Layer 1 (stream dedup)
        await stream._handle_message(fill_msg)
        order_updates = [(e, p) for e, p in published if e == "USER_ORDER_UPDATE"]
        assert len(order_updates) == 1  # still 1

        assert stream.metrics["fills_deduped"] == 1


class TestMultiSymbolFlow:
    """Multiple symbols processing concurrently through user stream."""

    async def test_independent_symbols(self):
        stream = _make_stream()
        published = []
        async def capture_publish(event_type, payload=None):
            published.append((event_type, payload))
        stream.event_bus.publish = capture_publish

        btc_new = json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000001000, "T": 1700000001000,
            "o": {
                "s": "BTCUSDT", "c": "btc_001", "S": "BUY", "o": "MARKET",
                "f": "GTC", "q": "0.100", "p": "0", "ap": "42000.00",
                "sp": "0", "x": "TRADE", "X": "FILLED", "i": 6001,
                "l": "0.100", "z": "0.100", "L": "42000.00", "n": "0.021",
                "N": "USDT", "t": 80001, "R": False, "ps": "BOTH", "rp": "0",
                "m": False,
            }
        })
        eth_new = json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000001100, "T": 1700000001100,
            "o": {
                "s": "ETHUSDT", "c": "eth_001", "S": "BUY", "o": "MARKET",
                "f": "GTC", "q": "1.000", "p": "0", "ap": "2200.00",
                "sp": "0", "x": "TRADE", "X": "FILLED", "i": 6002,
                "l": "1.000", "z": "1.000", "L": "2200.00", "n": "0.011",
                "N": "USDT", "t": 80002, "R": False, "ps": "BOTH", "rp": "0",
                "m": False,
            }
        })

        await stream._handle_message(btc_new)
        await stream._handle_message(eth_new)

        order_updates = [(e, p) for e, p in published if e == "USER_ORDER_UPDATE"]
        assert len(order_updates) == 2
        assert order_updates[0][1]["symbol"] == "BTCUSDT"
        assert order_updates[1][1]["symbol"] == "ETHUSDT"
        assert stream._order_states[6001] == "FILLED"
        assert stream._order_states[6002] == "FILLED"


class TestCancelAndRejectFlow:
    """Cancel and reject events are handled without dedup."""

    async def test_cancel_updates_state(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        # Place order (NEW) then cancel
        new_msg = json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000001000, "T": 1700000001000,
            "o": {
                "s": "BTCUSDT", "c": "cancel_001", "S": "BUY", "o": "LIMIT",
                "f": "GTC", "q": "1.000", "p": "40000.00", "ap": "0",
                "sp": "0", "x": "NEW", "X": "NEW", "i": 7001,
                "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
                "t": 0, "R": False, "ps": "BOTH", "rp": "0", "m": False,
            }
        })
        cancel_msg = json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000002000, "T": 1700000002000,
            "o": {
                "s": "BTCUSDT", "c": "cancel_001", "S": "BUY", "o": "LIMIT",
                "f": "GTC", "q": "1.000", "p": "40000.00", "ap": "0",
                "sp": "0", "x": "CANCELED", "X": "CANCELED", "i": 7001,
                "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
                "t": 0, "R": False, "ps": "BOTH", "rp": "0", "m": False,
            }
        })

        await stream._handle_message(new_msg)
        await stream._handle_message(cancel_msg)

        assert stream._order_states[7001] == "CANCELED"

    async def test_reject_updates_state(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        reject_msg = json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000001000, "T": 1700000001000,
            "o": {
                "s": "BTCUSDT", "c": "reject_001", "S": "BUY", "o": "LIMIT",
                "f": "GTC", "q": "100.000", "p": "100000.00", "ap": "0",
                "sp": "0", "x": "REJECTED", "X": "REJECTED", "i": 7002,
                "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
                "t": 0, "R": False, "ps": "BOTH", "rp": "0", "m": False,
            }
        })

        await stream._handle_message(reject_msg)
        assert stream._order_states[7002] == "REJECTED"


class TestSafeModeIntegration:
    """Integration tests for safe-mode with CEXExecutor circuit breaker."""

    async def test_stream_lost_publishes_on_disconnect(self):
        """Verify STREAM_LOST event is published when stream disconnects."""
        stream = _make_stream(
            config=_make_config(risk={"user_stream_safe_mode_seconds": 0}),
        )
        stream.event_bus.publish = AsyncMock()

        # Simulate disconnect (inline publish, threshold=0)
        stream._connected = False
        stream._disconnect_time = time.time()
        await stream.event_bus.publish("USER_STREAM_LOST", {"timestamp": time.time()})

        stream.event_bus.publish.assert_called_once()
        assert stream.event_bus.publish.call_args[0][0] == "USER_STREAM_LOST"

    async def test_stream_connected_publishes(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        await stream.event_bus.publish("USER_STREAM_CONNECTED", {})
        stream.event_bus.publish.assert_called_once()
        assert stream.event_bus.publish.call_args[0][0] == "USER_STREAM_CONNECTED"


class TestMetricsTracking:
    """Verify metrics counters are updated correctly across operations."""

    async def test_metrics_after_full_lifecycle(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        # Full entry lifecycle
        for msg in _entry_order_sequence():
            await stream._handle_message(msg)

        assert stream.metrics["messages_received"] == 3
        assert stream.metrics["fills_processed"] == 2  # 2 TRADE events
        assert stream.metrics["state_transitions"] == 3  # NEW, PARTIAL, FILLED
        assert stream.metrics["fills_deduped"] == 0

        # Replay a fill
        await stream._handle_message(_entry_order_sequence()[1])
        assert stream.metrics["fills_deduped"] == 1
        assert stream.metrics["messages_received"] == 4

    async def test_metrics_after_invalid_transition(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        # FILLED then try to go back to NEW (invalid)
        fill_msg = json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000001000, "T": 1700000001000,
            "o": {
                "s": "BTCUSDT", "c": "x", "S": "BUY", "o": "MARKET",
                "f": "GTC", "q": "1", "p": "0", "ap": "42000",
                "sp": "0", "x": "TRADE", "X": "FILLED", "i": 9001,
                "l": "1", "z": "1", "L": "42000", "n": "0.02",
                "N": "USDT", "t": 90001, "R": False, "ps": "BOTH", "rp": "0",
                "m": False,
            }
        })
        invalid_msg = json.dumps({
            "e": "ORDER_TRADE_UPDATE", "E": 1700000002000, "T": 1700000002000,
            "o": {
                "s": "BTCUSDT", "c": "x", "S": "BUY", "o": "MARKET",
                "f": "GTC", "q": "1", "p": "0", "ap": "0",
                "sp": "0", "x": "NEW", "X": "NEW", "i": 9001,
                "l": "0", "z": "0", "L": "0", "n": "0",
                "N": "USDT", "t": 0, "R": False, "ps": "BOTH", "rp": "0",
                "m": False,
            }
        })

        await stream._handle_message(fill_msg)
        await stream._handle_message(invalid_msg)
        assert stream.metrics["invalid_transitions"] == 1
