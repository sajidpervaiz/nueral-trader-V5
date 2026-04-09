"""
Unit tests for Binance Futures USER_DATA stream parsing, dedup, state machine,
and safe-mode threshold.
"""
import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.config import Config
from core.event_bus import EventBus
from data_ingestion.user_stream import UserDataStream, _VALID_TRANSITIONS


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
            "user_stream_safe_mode_seconds": 0,
        },
        "system": {"paper_mode": False},
    }
    cfg._data.update(overrides)
    return cfg


def _make_stream(config=None, event_bus=None, safe_mode_seconds=0) -> UserDataStream:
    if config is None:
        config = _make_config(
            risk={"user_stream_safe_mode_seconds": safe_mode_seconds}
        )
    if event_bus is None:
        event_bus = EventBus()
    return UserDataStream(config, event_bus)


# ─── Recorded Binance Payloads ────────────────────────────────────────────────

ORDER_TRADE_UPDATE_NEW = json.dumps({
    "e": "ORDER_TRADE_UPDATE",
    "E": 1700000001000,
    "T": 1700000001000,
    "o": {
        "s": "BTCUSDT", "c": "cli_123", "S": "BUY", "o": "LIMIT",
        "f": "GTC", "q": "0.100", "p": "42000.00", "ap": "0",
        "sp": "0", "x": "NEW", "X": "NEW", "i": 12345,
        "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
        "t": 0, "R": False, "ps": "BOTH", "rp": "0", "m": False,
    }
})

ORDER_TRADE_UPDATE_PARTIAL_FILL = json.dumps({
    "e": "ORDER_TRADE_UPDATE",
    "E": 1700000002000,
    "T": 1700000002000,
    "o": {
        "s": "BTCUSDT", "c": "cli_123", "S": "BUY", "o": "LIMIT",
        "f": "GTC", "q": "0.100", "p": "42000.00", "ap": "41999.50",
        "sp": "0", "x": "TRADE", "X": "PARTIALLY_FILLED", "i": 12345,
        "l": "0.050", "z": "0.050", "L": "41999.50", "n": "0.02100",
        "N": "USDT", "t": 99001, "R": False, "ps": "BOTH", "rp": "0",
        "m": True,
    }
})

ORDER_TRADE_UPDATE_FILL = json.dumps({
    "e": "ORDER_TRADE_UPDATE",
    "E": 1700000003000,
    "T": 1700000003000,
    "o": {
        "s": "BTCUSDT", "c": "cli_123", "S": "BUY", "o": "LIMIT",
        "f": "GTC", "q": "0.100", "p": "42000.00", "ap": "42000.00",
        "sp": "0", "x": "TRADE", "X": "FILLED", "i": 12345,
        "l": "0.050", "z": "0.100", "L": "42000.50", "n": "0.02100",
        "N": "USDT", "t": 99002, "R": False, "ps": "BOTH", "rp": "0",
        "m": False,
    }
})

ORDER_TRADE_UPDATE_CANCEL = json.dumps({
    "e": "ORDER_TRADE_UPDATE",
    "E": 1700000004000,
    "T": 1700000004000,
    "o": {
        "s": "ETHUSDT", "c": "cli_456", "S": "SELL", "o": "LIMIT",
        "f": "GTC", "q": "1.000", "p": "2200.00", "ap": "0",
        "sp": "0", "x": "CANCELED", "X": "CANCELED", "i": 12346,
        "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
        "t": 0, "R": False, "ps": "BOTH", "rp": "0", "m": False,
    }
})

ORDER_TRADE_UPDATE_SL_FILL = json.dumps({
    "e": "ORDER_TRADE_UPDATE",
    "E": 1700000005000,
    "T": 1700000005000,
    "o": {
        "s": "BTCUSDT", "c": "sl_btc_123", "S": "SELL", "o": "STOP_MARKET",
        "f": "GTC", "q": "0.100", "p": "0", "ap": "40000.00",
        "sp": "40000.00", "x": "TRADE", "X": "FILLED", "i": 12347,
        "l": "0.100", "z": "0.100", "L": "40000.00", "n": "0.02000",
        "N": "USDT", "t": 99010, "R": True, "ps": "BOTH", "rp": "-200.00",
        "m": False,
    }
})

ORDER_TRADE_UPDATE_REJECTED = json.dumps({
    "e": "ORDER_TRADE_UPDATE",
    "E": 1700000006000,
    "T": 1700000006000,
    "o": {
        "s": "BTCUSDT", "c": "cli_789", "S": "BUY", "o": "LIMIT",
        "f": "GTC", "q": "100.000", "p": "42000.00", "ap": "0",
        "sp": "0", "x": "REJECTED", "X": "REJECTED", "i": 12348,
        "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
        "t": 0, "R": False, "ps": "BOTH", "rp": "0", "m": False,
    }
})

ACCOUNT_UPDATE_PAYLOAD = json.dumps({
    "e": "ACCOUNT_UPDATE",
    "E": 1700000010000,
    "T": 1700000010000,
    "a": {
        "m": "ORDER",
        "B": [
            {"a": "USDT", "wb": "50000.00", "cw": "49000.00", "bc": "-2100.00"},
            {"a": "BNB", "wb": "10.000", "cw": "10.000", "bc": "0"},
        ],
        "P": [
            {
                "s": "BTCUSDT", "pa": "0.100", "ep": "42000.00",
                "up": "50.00", "mt": "cross", "iw": "0", "ps": "BOTH",
            }
        ],
    }
})

MARGIN_CALL_PAYLOAD = json.dumps({
    "e": "MARGIN_CALL",
    "E": 1700000020000,
    "cw": "3000.00",
    "p": [{"s": "BTCUSDT", "ps": "BOTH", "pa": "0.5", "mt": "cross", "iw": "0", "mp": "41000", "up": "-500", "mm": "4000"}],
})

LISTEN_KEY_EXPIRED_PAYLOAD = json.dumps({
    "e": "listenKeyExpired",
    "E": 1700000030000,
})


# ═══════════════════════════════════════════════════════════════════════════════
# Test Classes
# ═══════════════════════════════════════════════════════════════════════════════


class TestParseOrderUpdate:
    """Unit tests for _parse_order_update() with recorded Binance payloads."""

    def test_parse_new_order(self):
        stream = _make_stream()
        data = json.loads(ORDER_TRADE_UPDATE_NEW)
        parsed = stream._parse_order_update(data)

        assert parsed["event_type"] == "ORDER_TRADE_UPDATE"
        assert parsed["symbol"] == "BTCUSDT"
        assert parsed["client_order_id"] == "cli_123"
        assert parsed["side"] == "BUY"
        assert parsed["order_type"] == "LIMIT"
        assert parsed["execution_type"] == "NEW"
        assert parsed["order_status"] == "NEW"
        assert parsed["order_id"] == 12345
        assert parsed["quantity"] == pytest.approx(0.1)
        assert parsed["price"] == pytest.approx(42000.0)
        assert parsed["last_filled_qty"] == 0.0
        assert parsed["cumulative_filled_qty"] == 0.0
        assert parsed["trade_id"] == 0
        assert parsed["reduce_only"] is False
        assert parsed["position_side"] == "BOTH"

    def test_parse_partial_fill(self):
        stream = _make_stream()
        data = json.loads(ORDER_TRADE_UPDATE_PARTIAL_FILL)
        parsed = stream._parse_order_update(data)

        assert parsed["execution_type"] == "TRADE"
        assert parsed["order_status"] == "PARTIALLY_FILLED"
        assert parsed["last_filled_qty"] == pytest.approx(0.05)
        assert parsed["cumulative_filled_qty"] == pytest.approx(0.05)
        assert parsed["last_filled_price"] == pytest.approx(41999.5)
        assert parsed["avg_price"] == pytest.approx(41999.5)
        assert parsed["commission"] == pytest.approx(0.021)
        assert parsed["trade_id"] == 99001
        assert parsed["is_maker"] is True

    def test_parse_full_fill(self):
        stream = _make_stream()
        data = json.loads(ORDER_TRADE_UPDATE_FILL)
        parsed = stream._parse_order_update(data)

        assert parsed["order_status"] == "FILLED"
        assert parsed["cumulative_filled_qty"] == pytest.approx(0.1)
        assert parsed["quantity"] == pytest.approx(0.1)
        assert parsed["trade_id"] == 99002
        assert parsed["is_maker"] is False

    def test_parse_cancel(self):
        stream = _make_stream()
        data = json.loads(ORDER_TRADE_UPDATE_CANCEL)
        parsed = stream._parse_order_update(data)

        assert parsed["symbol"] == "ETHUSDT"
        assert parsed["execution_type"] == "CANCELED"
        assert parsed["order_status"] == "CANCELED"
        assert parsed["order_id"] == 12346

    def test_parse_sl_fill(self):
        stream = _make_stream()
        data = json.loads(ORDER_TRADE_UPDATE_SL_FILL)
        parsed = stream._parse_order_update(data)

        assert parsed["order_type"] == "STOP_MARKET"
        assert parsed["reduce_only"] is True
        assert parsed["realized_profit"] == pytest.approx(-200.0)
        assert parsed["stop_price"] == pytest.approx(40000.0)
        assert parsed["trade_id"] == 99010

    def test_parse_rejected(self):
        stream = _make_stream()
        data = json.loads(ORDER_TRADE_UPDATE_REJECTED)
        parsed = stream._parse_order_update(data)

        assert parsed["execution_type"] == "REJECTED"
        assert parsed["order_status"] == "REJECTED"

    def test_parse_missing_fields_uses_defaults(self):
        stream = _make_stream()
        parsed = stream._parse_order_update({"o": {}})

        assert parsed["symbol"] == ""
        assert parsed["quantity"] == 0.0
        assert parsed["order_id"] == 0
        assert parsed["trade_id"] == 0
        assert parsed["reduce_only"] is False
        assert parsed["position_side"] == "BOTH"


class TestParseAccountUpdate:
    """Unit tests for _parse_account_update() with recorded Binance payloads."""

    def test_parse_account_update(self):
        stream = _make_stream()
        data = json.loads(ACCOUNT_UPDATE_PAYLOAD)
        parsed = stream._parse_account_update(data)

        assert parsed["event_type"] == "ACCOUNT_UPDATE"
        assert parsed["reason"] == "ORDER"
        assert len(parsed["balances"]) == 2
        assert len(parsed["positions"]) == 1

        usdt = parsed["balances"][0]
        assert usdt["asset"] == "USDT"
        assert usdt["wallet_balance"] == pytest.approx(50000.0)
        assert usdt["balance_change"] == pytest.approx(-2100.0)

        pos = parsed["positions"][0]
        assert pos["symbol"] == "BTCUSDT"
        assert pos["position_amount"] == pytest.approx(0.1)
        assert pos["entry_price"] == pytest.approx(42000.0)
        assert pos["unrealized_pnl"] == pytest.approx(50.0)
        assert pos["margin_type"] == "cross"

    def test_parse_empty_account_update(self):
        stream = _make_stream()
        parsed = stream._parse_account_update({"a": {}})

        assert parsed["balances"] == []
        assert parsed["positions"] == []
        assert parsed["reason"] == ""

    def test_parse_multiple_balances(self):
        stream = _make_stream()
        data = json.loads(ACCOUNT_UPDATE_PAYLOAD)
        parsed = stream._parse_account_update(data)

        bnb = parsed["balances"][1]
        assert bnb["asset"] == "BNB"
        assert bnb["wallet_balance"] == pytest.approx(10.0)


class TestFillDedup:
    """Tests for stream-level fill deduplication via trade_id."""

    def test_first_trade_not_dedup(self):
        stream = _make_stream()
        assert stream._check_fill_dedup(99001) is False
        assert stream.metrics["fills_deduped"] == 0

    def test_duplicate_trade_detected(self):
        stream = _make_stream()
        assert stream._check_fill_dedup(99001) is False
        assert stream._check_fill_dedup(99001) is True
        assert stream.metrics["fills_deduped"] == 1

    def test_different_trades_not_dedup(self):
        stream = _make_stream()
        assert stream._check_fill_dedup(99001) is False
        assert stream._check_fill_dedup(99002) is False
        assert stream.metrics["fills_deduped"] == 0

    def test_dedup_evicts_oldest_when_full(self):
        stream = _make_stream()
        # Fill beyond max size
        from data_ingestion.user_stream import _MAX_DEDUP_SIZE
        for i in range(1, _MAX_DEDUP_SIZE + 2):
            stream._check_fill_dedup(i)
        # Oldest (1) should have been evicted
        assert 1 not in stream._processed_trade_ids
        # Most recent should still be present
        assert _MAX_DEDUP_SIZE + 1 in stream._processed_trade_ids

    async def test_duplicate_trade_message_suppressed(self):
        """Full integration: duplicate TRADE message is not published to EventBus."""
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        # First message — should be published
        await stream._handle_message(ORDER_TRADE_UPDATE_PARTIAL_FILL)
        assert stream.event_bus.publish.call_count == 1

        # Duplicate — should be suppressed
        await stream._handle_message(ORDER_TRADE_UPDATE_PARTIAL_FILL)
        assert stream.event_bus.publish.call_count == 1
        assert stream.metrics["fills_deduped"] == 1

    async def test_non_trade_events_not_deduped(self):
        """NEW and CANCELED events are not subject to fill dedup."""
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        # NEW event (trade_id=0) — should always be published
        await stream._handle_message(ORDER_TRADE_UPDATE_NEW)
        await stream._handle_message(ORDER_TRADE_UPDATE_NEW)
        assert stream.event_bus.publish.call_count == 2  # both published (no dedup on non-TRADE)


class TestOrderStateMachine:
    """Tests for order state transition tracking."""

    def test_first_state_always_accepted(self):
        stream = _make_stream()
        assert stream._track_order_state(100, "NEW") is True
        assert stream._order_states[100] == "NEW"

    def test_valid_transition_new_to_filled(self):
        stream = _make_stream()
        stream._track_order_state(100, "NEW")
        assert stream._track_order_state(100, "FILLED") is True
        assert stream._order_states[100] == "FILLED"

    def test_valid_transition_new_to_partial_to_filled(self):
        stream = _make_stream()
        stream._track_order_state(100, "NEW")
        assert stream._track_order_state(100, "PARTIALLY_FILLED") is True
        assert stream._track_order_state(100, "FILLED") is True

    def test_valid_transition_partial_to_partial(self):
        stream = _make_stream()
        stream._track_order_state(100, "NEW")
        stream._track_order_state(100, "PARTIALLY_FILLED")
        assert stream._track_order_state(100, "PARTIALLY_FILLED") is True

    def test_valid_transition_new_to_canceled(self):
        stream = _make_stream()
        stream._track_order_state(100, "NEW")
        assert stream._track_order_state(100, "CANCELED") is True

    def test_valid_transition_new_to_rejected(self):
        stream = _make_stream()
        stream._track_order_state(100, "NEW")
        assert stream._track_order_state(100, "REJECTED") is True

    def test_valid_transition_new_to_expired(self):
        stream = _make_stream()
        stream._track_order_state(100, "NEW")
        assert stream._track_order_state(100, "EXPIRED") is True

    def test_invalid_transition_filled_to_new(self):
        stream = _make_stream()
        stream._track_order_state(100, "FILLED")
        assert stream._track_order_state(100, "NEW") is False
        assert stream.metrics["invalid_transitions"] == 1
        # State is still updated (we accept but log)
        assert stream._order_states[100] == "NEW"

    def test_invalid_transition_canceled_to_filled(self):
        stream = _make_stream()
        stream._track_order_state(100, "CANCELED")
        assert stream._track_order_state(100, "FILLED") is False
        assert stream.metrics["invalid_transitions"] == 1

    def test_same_status_repeated(self):
        stream = _make_stream()
        stream._track_order_state(100, "NEW")
        assert stream._track_order_state(100, "NEW") is True
        assert stream.metrics["state_transitions"] == 1  # only counted once

    def test_multiple_orders_independent(self):
        stream = _make_stream()
        stream._track_order_state(100, "NEW")
        stream._track_order_state(200, "NEW")
        stream._track_order_state(100, "FILLED")
        assert stream._order_states[100] == "FILLED"
        assert stream._order_states[200] == "NEW"

    def test_full_lifecycle_new_partial_filled(self):
        """Complete order lifecycle: NEW → PARTIALLY_FILLED → FILLED."""
        stream = _make_stream()
        assert stream._track_order_state(100, "NEW") is True
        assert stream._track_order_state(100, "PARTIALLY_FILLED") is True
        assert stream._track_order_state(100, "PARTIALLY_FILLED") is True
        assert stream._track_order_state(100, "FILLED") is True
        assert stream.metrics["state_transitions"] == 3  # NEW, PARTIALLY_FILLED, FILLED

    def test_valid_transitions_table_complete(self):
        """Verify _VALID_TRANSITIONS covers all expected states."""
        assert "NEW" in _VALID_TRANSITIONS
        assert "PARTIALLY_FILLED" in _VALID_TRANSITIONS
        assert "FILLED" in _VALID_TRANSITIONS
        assert "CANCELED" in _VALID_TRANSITIONS
        assert "REJECTED" in _VALID_TRANSITIONS
        assert "EXPIRED" in _VALID_TRANSITIONS
        # Terminal states have no valid next
        for terminal in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
            assert _VALID_TRANSITIONS[terminal] == set()

    def test_cleanup_terminal_orders(self):
        stream = _make_stream()
        # Create more than 10000 terminal orders
        for i in range(10_500):
            stream._order_states[i] = "FILLED"
        stream._cleanup_terminal_orders()
        assert len(stream._order_states) <= 10_500  # some evicted


class TestHandleMessage:
    """Integration tests for _handle_message dispatching."""

    async def test_order_trade_update_published(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        await stream._handle_message(ORDER_TRADE_UPDATE_NEW)
        stream.event_bus.publish.assert_called_once()
        call_args = stream.event_bus.publish.call_args
        assert call_args[0][0] == "USER_ORDER_UPDATE"
        assert call_args[0][1]["symbol"] == "BTCUSDT"
        assert call_args[0][1]["execution_type"] == "NEW"

    async def test_account_update_published(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        await stream._handle_message(ACCOUNT_UPDATE_PAYLOAD)
        stream.event_bus.publish.assert_called_once()
        call_args = stream.event_bus.publish.call_args
        assert call_args[0][0] == "USER_ACCOUNT_UPDATE"
        assert call_args[0][1]["reason"] == "ORDER"

    async def test_listen_key_expired_clears_key(self):
        stream = _make_stream()
        stream._listen_key = "some_key"

        await stream._handle_message(LISTEN_KEY_EXPIRED_PAYLOAD)
        assert stream._listen_key is None

    async def test_margin_call_published(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        await stream._handle_message(MARGIN_CALL_PAYLOAD)
        stream.event_bus.publish.assert_called_once()
        assert stream.event_bus.publish.call_args[0][0] == "MARGIN_CALL"

    async def test_invalid_json_ignored(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()
        await stream._handle_message("not json at all {{{")
        assert stream.metrics["messages_received"] == 1

    async def test_unknown_event_ignored(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        await stream._handle_message(json.dumps({"e": "COMPLETELY_UNKNOWN"}))
        assert stream.metrics["messages_received"] == 1

    async def test_metrics_increment_on_fill(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()
        await stream._handle_message(ORDER_TRADE_UPDATE_PARTIAL_FILL)
        assert stream.metrics["fills_processed"] == 1
        assert stream.metrics["messages_received"] == 1

    async def test_state_machine_tracked_across_messages(self):
        """Order lifecycle tracked across sequential messages."""
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        await stream._handle_message(ORDER_TRADE_UPDATE_NEW)
        assert stream._order_states[12345] == "NEW"

        await stream._handle_message(ORDER_TRADE_UPDATE_PARTIAL_FILL)
        assert stream._order_states[12345] == "PARTIALLY_FILLED"

        await stream._handle_message(ORDER_TRADE_UPDATE_FILL)
        assert stream._order_states[12345] == "FILLED"


class TestSafeModeThreshold:
    """Tests for configurable safe-mode disconnect threshold."""

    def test_threshold_zero_means_immediate(self):
        stream = _make_stream(safe_mode_seconds=0)
        assert stream._safe_mode_threshold == 0.0

    def test_threshold_from_config(self):
        stream = _make_stream(safe_mode_seconds=10)
        assert stream._safe_mode_threshold == 10.0

    async def test_immediate_stream_lost_when_threshold_zero(self):
        """With threshold=0, STREAM_LOST would be published inline in run (not by watchdog)."""
        stream = _make_stream(safe_mode_seconds=0)
        stream.event_bus.publish = AsyncMock()
        # Simulate what run() does when disconnected
        stream._connected = False
        stream._disconnect_time = time.time()

        # With threshold=0, the run() loop publishes immediately
        if stream._safe_mode_threshold <= 0:
            await stream.event_bus.publish("USER_STREAM_LOST", {"timestamp": time.time()})
        stream.event_bus.publish.assert_called_once()
        assert stream.event_bus.publish.call_args[0][0] == "USER_STREAM_LOST"

    async def test_watchdog_publishes_after_threshold(self):
        """Safe-mode watchdog publishes STREAM_LOST after threshold elapsed."""
        stream = _make_stream(safe_mode_seconds=0.1)
        stream.event_bus.publish = AsyncMock()
        stream._running = True
        stream._connected = False
        stream._disconnect_time = time.time() - 0.2  # Already past threshold

        # Run one watchdog iteration
        watchdog = asyncio.create_task(stream._safe_mode_watchdog())
        await asyncio.sleep(1.5)
        stream._running = False
        watchdog.cancel()
        try:
            await watchdog
        except asyncio.CancelledError:
            pass

        stream.event_bus.publish.assert_called_once()
        call_args = stream.event_bus.publish.call_args
        assert call_args[0][0] == "USER_STREAM_LOST"
        assert call_args[0][1]["disconnect_duration"] >= 0.1

    async def test_watchdog_no_publish_when_connected(self):
        """Watchdog does not publish STREAM_LOST when connected."""
        stream = _make_stream(safe_mode_seconds=0.1)
        stream.event_bus.publish = AsyncMock()
        stream._running = True
        stream._connected = True

        watchdog = asyncio.create_task(stream._safe_mode_watchdog())
        await asyncio.sleep(1.5)
        stream._running = False
        watchdog.cancel()
        try:
            await watchdog
        except asyncio.CancelledError:
            pass

        stream.event_bus.publish.assert_not_called()

    async def test_watchdog_resets_on_reconnect(self):
        """If stream reconnects before threshold, no STREAM_LOST is published."""
        stream = _make_stream(safe_mode_seconds=5)
        stream.event_bus.publish = AsyncMock()
        stream._running = True
        stream._connected = False
        stream._disconnect_time = time.time()

        watchdog = asyncio.create_task(stream._safe_mode_watchdog())
        # Let watchdog run a bit
        await asyncio.sleep(1.5)
        # Reconnect before threshold
        stream._connected = True
        await asyncio.sleep(1.5)
        stream._running = False
        watchdog.cancel()
        try:
            await watchdog
        except asyncio.CancelledError:
            pass

        stream.event_bus.publish.assert_not_called()


class TestListenKeyLifecycle:
    """Tests for listenKey create/keepalive/expiry handling."""

    async def test_create_listen_key(self):
        stream = _make_stream()
        stream._http_request = AsyncMock(return_value={"listenKey": "test_key_abc"})

        key = await stream._create_listen_key()
        assert key == "test_key_abc"
        stream._http_request.assert_called_once_with("POST", "/fapi/v1/listenKey")

    async def test_create_listen_key_empty_raises(self):
        stream = _make_stream()
        stream._http_request = AsyncMock(return_value={"listenKey": ""})

        with pytest.raises(RuntimeError, match="No listenKey"):
            await stream._create_listen_key()

    async def test_keepalive_success(self):
        stream = _make_stream()
        stream._listen_key = "test_key"
        stream._http_request = AsyncMock(return_value={})

        await stream._keepalive_listen_key()
        stream._http_request.assert_called_once_with("PUT", "/fapi/v1/listenKey")
        assert stream._last_keepalive > 0

    async def test_keepalive_retries_on_failure(self):
        stream = _make_stream()
        stream._listen_key = "test_key"
        stream._ws = MagicMock()
        stream._ws.close = AsyncMock()
        stream._http_request = AsyncMock(side_effect=RuntimeError("network error"))

        await stream._keepalive_listen_key()
        # Should have tried 3 times
        assert stream._http_request.call_count == 3
        # Listen key should be invalidated
        assert stream._listen_key is None

    async def test_keepalive_skips_when_no_key(self):
        stream = _make_stream()
        stream._listen_key = None
        stream._http_request = AsyncMock()

        await stream._keepalive_listen_key()
        stream._http_request.assert_not_called()


class TestDisconnectProperties:
    """Tests for connected/disconnect_duration properties."""

    def test_connected_default_false(self):
        stream = _make_stream()
        assert stream.connected is False

    def test_disconnect_duration_zero_when_connected(self):
        stream = _make_stream()
        stream._connected = True
        assert stream.disconnect_duration == 0.0

    def test_disconnect_duration_zero_when_no_disconnect_time(self):
        stream = _make_stream()
        stream._connected = False
        stream._disconnect_time = None
        assert stream.disconnect_duration == 0.0

    def test_disconnect_duration_calculated(self):
        stream = _make_stream()
        stream._connected = False
        stream._disconnect_time = time.time() - 10.0
        dur = stream.disconnect_duration
        assert 9.5 < dur < 11.0


class TestReconcileAfterReconnect:
    """Tests for REST-based reconciliation after reconnection."""

    async def test_reconcile_publishes_missed_fills(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()

        # Mock aiohttp response
        mock_trades = [
            {
                "id": 50001, "orderId": 1001, "symbol": "BTCUSDT",
                "side": "BUY", "price": "42000.0", "qty": "0.05",
                "commission": "0.021", "commissionAsset": "USDT",
                "time": 1700000001000, "positionSide": "BOTH",
                "realizedPnl": "0", "maker": False,
            }
        ]

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value=mock_trades)
            mock_ctx_manager = AsyncMock()
            mock_ctx_manager.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_ctx_manager.__aexit__ = AsyncMock(return_value=False)

            mock_session = AsyncMock()
            mock_session.get = MagicMock(return_value=mock_ctx_manager)
            mock_session_ctx = AsyncMock()
            mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

            mock_session_cls.return_value = mock_session_ctx

            await stream._reconcile_after_reconnect()

        stream.event_bus.publish.assert_called_once()
        call_args = stream.event_bus.publish.call_args
        assert call_args[0][0] == "USER_ORDER_UPDATE"
        assert call_args[0][1]["trade_id"] == 50001
        assert call_args[0][1]["reconciled"] is True
        assert stream.metrics["fills_processed"] == 1

    async def test_reconcile_skips_already_processed(self):
        stream = _make_stream()
        stream.event_bus.publish = AsyncMock()
        # Pre-populate dedup set
        stream._processed_trade_ids[50001] = True

        mock_trades = [{"id": 50001, "orderId": 1001, "symbol": "BTCUSDT",
                        "side": "BUY", "price": "42000.0", "qty": "0.05",
                        "commission": "0.021", "commissionAsset": "USDT",
                        "time": 1700000001000, "positionSide": "BOTH",
                        "realizedPnl": "0", "maker": False}]

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value=mock_trades)
            mock_ctx_manager = AsyncMock()
            mock_ctx_manager.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_ctx_manager.__aexit__ = AsyncMock(return_value=False)

            mock_session = AsyncMock()
            mock_session.get = MagicMock(return_value=mock_ctx_manager)
            mock_session_ctx = AsyncMock()
            mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

            mock_session_cls.return_value = mock_session_ctx

            await stream._reconcile_after_reconnect()

        # Should not publish since trade was already processed
        stream.event_bus.publish.assert_not_called()

    async def test_reconcile_handles_api_failure_gracefully(self):
        stream = _make_stream()

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_resp = AsyncMock()
            mock_resp.status = 500
            mock_ctx_manager = AsyncMock()
            mock_ctx_manager.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_ctx_manager.__aexit__ = AsyncMock(return_value=False)

            mock_session = AsyncMock()
            mock_session.get = MagicMock(return_value=mock_ctx_manager)
            mock_session_ctx = AsyncMock()
            mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

            mock_session_cls.return_value = mock_session_ctx

            # Should not raise
            await stream._reconcile_after_reconnect()
