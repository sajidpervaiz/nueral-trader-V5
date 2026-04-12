"""Chaos / Disaster Testing — Prompt 9.

Tests simulate real-world infrastructure failures and verify:
1. SafeMode activates — no new trades opened while blind.
2. Existing positions remain protected (exchange-side SL/TP).
3. Every failure mode has a clear recovery path.
4. The system resumes after fault resolution.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.circuit_breaker import CircuitBreaker, CircuitState
from core.event_bus import EventBus
from core.safe_mode import SafeModeManager, SafeModeReason, SafeModeEvent
from engine.signal_generator import TradingSignal
from execution.risk_manager import RiskManager, Position
from execution.risk_manager import CircuitBreaker as RiskCircuitBreaker
from tests.chaos import FaultInjector


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

class DummyConfig:
    paper_mode = True

    def get_value(self, *args, **kwargs):
        return {}


class DummyEventBus:
    """Minimal event bus that records publishes."""

    def __init__(self):
        self._published: list[tuple[str, Any]] = []
        self._handlers: dict[str, list] = {}

    def subscribe(self, event_type, handler):
        self._handlers.setdefault(event_type, []).append(handler)

    def unsubscribe(self, event_type, handler):
        handlers = self._handlers.get(event_type, [])
        if handler in handlers:
            handlers.remove(handler)

    async def publish(self, event_type, payload=None):
        self._published.append((event_type, payload))
        for h in self._handlers.get(event_type, []):
            await h(payload)

    def publish_nowait(self, event_type, payload=None):
        self._published.append((event_type, payload))


def _make_signal(
    exchange="binance", symbol="BTCUSDT", direction="long",
    score=0.8, price=50_000.0, atr=500.0, spread_bps=1.0,
) -> TradingSignal:
    sl = price - atr * 1.5 if direction == "long" else price + atr * 1.5
    tp = price + atr * 3.0 if direction == "long" else price - atr * 3.0
    return TradingSignal(
        exchange=exchange,
        symbol=symbol,
        direction=direction,
        score=score,
        technical_score=0.5,
        ml_score=0.3,
        sentiment_score=0.2,
        macro_score=0.1,
        news_score=0.1,
        orderbook_score=0.1,
        regime="trending",
        regime_confidence=0.9,
        price=price,
        atr=atr,
        stop_loss=sl,
        take_profit=tp,
        timestamp=int(time.time()),
        metadata={"spread_bps": spread_bps},
    )


def _make_risk_manager(safe_mode=None) -> tuple[RiskManager, DummyEventBus]:
    cfg = DummyConfig()
    bus = DummyEventBus()
    sm = safe_mode or SafeModeManager()
    rm = RiskManager(config=cfg, event_bus=bus, safe_mode=sm)
    return rm, bus


def _open_test_position(rm: RiskManager, exchange="binance", symbol="BTCUSDT") -> Position:
    """Force-open a position directly in risk manager for testing."""
    pos = Position(
        exchange=exchange,
        symbol=symbol,
        direction="long",
        size=0.1,
        entry_price=50_000.0,
        current_price=50_000.0,
        stop_loss=49_250.0,
        take_profit=51_500.0,
        open_time=int(time.time()),
        highest_since_entry=50_000.0,
        lowest_since_entry=50_000.0,
    )
    key = f"{exchange}:{symbol}"
    rm._positions[key] = pos
    return pos


# ═══════════════════════════════════════════════════════════════════════════
# SafeMode unit tests
# ═══════════════════════════════════════════════════════════════════════════

class TestSafeModeManager:
    def test_initial_state_is_inactive(self):
        sm = SafeModeManager()
        assert not sm.is_active
        assert sm.active_reasons == []

    def test_activate_single_reason(self):
        sm = SafeModeManager()
        sm.activate(SafeModeReason.WEBSOCKET_DISCONNECT, "test")
        assert sm.is_active
        assert SafeModeReason.WEBSOCKET_DISCONNECT in sm.active_reasons

    def test_deactivate_clears_reason(self):
        sm = SafeModeManager()
        sm.activate(SafeModeReason.DB_OUTAGE)
        sm.deactivate(SafeModeReason.DB_OUTAGE)
        assert not sm.is_active

    def test_multiple_reasons_require_all_cleared(self):
        sm = SafeModeManager()
        sm.activate(SafeModeReason.WEBSOCKET_DISCONNECT)
        sm.activate(SafeModeReason.DB_OUTAGE)
        sm.deactivate(SafeModeReason.WEBSOCKET_DISCONNECT)
        assert sm.is_active  # DB_OUTAGE still active
        sm.deactivate(SafeModeReason.DB_OUTAGE)
        assert not sm.is_active

    def test_clear_all_force_clears(self):
        sm = SafeModeManager()
        sm.activate(SafeModeReason.WEBSOCKET_DISCONNECT)
        sm.activate(SafeModeReason.DB_OUTAGE)
        sm.activate(SafeModeReason.EXTREME_VOLATILITY)
        sm.clear_all()
        assert not sm.is_active

    def test_idempotent_activation(self):
        sm = SafeModeManager()
        sm.activate(SafeModeReason.WEBSOCKET_DISCONNECT, "first")
        sm.activate(SafeModeReason.WEBSOCKET_DISCONNECT, "updated")
        assert len(sm.active_reasons) == 1
        assert sm.active_events[0].detail == "updated"

    def test_deactivate_nonexistent_is_safe(self):
        sm = SafeModeManager()
        result = sm.deactivate(SafeModeReason.DB_OUTAGE)
        assert result is True  # no reasons left = fully clear

    def test_get_status(self):
        sm = SafeModeManager()
        sm.activate(SafeModeReason.MANUAL, "test")
        status = sm.get_status()
        assert status["safe_mode_active"] is True
        assert len(status["active_reasons"]) == 1
        assert status["active_reasons"][0]["reason"] == "manual"

    def test_history_records_all_actions(self):
        sm = SafeModeManager()
        sm.activate(SafeModeReason.DB_OUTAGE)
        sm.deactivate(SafeModeReason.DB_OUTAGE)
        history = sm.get_history()
        assert len(history) == 2
        assert history[0]["action"] == "activate"
        assert history[1]["action"] == "deactivate"


# ═══════════════════════════════════════════════════════════════════════════
# SafeMode blocks new trades
# ═══════════════════════════════════════════════════════════════════════════

class TestSafeModeBlocksTrades:
    def test_safe_mode_rejects_signal(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        sm.activate(SafeModeReason.WEBSOCKET_DISCONNECT)
        signal = _make_signal()
        approved, reason, size = rm.approve_signal(signal)
        assert not approved
        assert "safe_mode_active" in reason
        assert "websocket_disconnect" in reason

    def test_safe_mode_cleared_allows_signal(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        sm.activate(SafeModeReason.WEBSOCKET_DISCONNECT)
        sm.deactivate(SafeModeReason.WEBSOCKET_DISCONNECT)
        signal = _make_signal()
        approved, reason, size = rm.approve_signal(signal)
        # May be rejected for other reasons (score, etc.) but NOT safe_mode
        assert "safe_mode_active" not in reason

    @pytest.mark.asyncio
    async def test_approve_and_open_blocked_in_safe_mode(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        sm.activate(SafeModeReason.DB_OUTAGE)
        signal = _make_signal()
        approved, reason, size, pos = await rm.approve_and_open(signal)
        assert not approved
        assert "safe_mode_active" in reason
        assert pos is None

    def test_existing_positions_survive_safe_mode(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        pos = _open_test_position(rm)
        sm.activate(SafeModeReason.WEBSOCKET_DISCONNECT)
        # Position should still exist
        assert "binance:BTCUSDT" in rm._positions
        assert rm._positions["binance:BTCUSDT"].entry_price == 50_000.0


# ═══════════════════════════════════════════════════════════════════════════
# FaultInjector tests
# ═══════════════════════════════════════════════════════════════════════════

class TestFaultInjector:
    @pytest.fixture
    def setup(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(
            event_bus=bus, risk_manager=rm, safe_mode=sm,
        )
        return rm, bus, sm, injector

    @pytest.mark.asyncio
    async def test_websocket_disconnect_activates_safe_mode(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_websocket_disconnect()
        assert sm.is_active
        assert SafeModeReason.WEBSOCKET_DISCONNECT in sm.active_reasons

    @pytest.mark.asyncio
    async def test_websocket_disconnect_blocks_new_trades(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_websocket_disconnect()
        signal = _make_signal()
        approved, reason, _ = rm.approve_signal(signal)
        assert not approved
        assert "safe_mode_active" in reason

    @pytest.mark.asyncio
    async def test_websocket_reconnect_clears_safe_mode(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_websocket_disconnect()
        await injector.simulate_websocket_reconnect()
        assert not sm.is_active

    @pytest.mark.asyncio
    async def test_user_stream_failure_triggers_circuit_breaker(self, setup):
        rm, bus, sm, injector = setup
        # Subscribe executor handler for stream lost/connected
        from execution.cex_executor import CEXExecutor
        executor = CEXExecutor(
            config=DummyConfig(), event_bus=bus,
            risk_manager=rm, exchange_id="binance",
        )
        bus.subscribe("USER_STREAM_LOST", executor._handle_user_stream_lost)
        bus.subscribe("USER_STREAM_CONNECTED", executor._handle_user_stream_connected)

        await injector.simulate_user_stream_failure()
        # Allow event dispatch
        assert rm._circuit_breaker.tripped
        assert "user_stream_disconnected" in rm._circuit_breaker.trip_reason
        assert sm.is_active

    @pytest.mark.asyncio
    async def test_user_stream_reconnect_clears_circuit_breaker(self, setup):
        rm, bus, sm, injector = setup
        from execution.cex_executor import CEXExecutor
        executor = CEXExecutor(
            config=DummyConfig(), event_bus=bus,
            risk_manager=rm, exchange_id="binance",
        )
        bus.subscribe("USER_STREAM_LOST", executor._handle_user_stream_lost)
        bus.subscribe("USER_STREAM_CONNECTED", executor._handle_user_stream_connected)

        await injector.simulate_user_stream_failure()
        await injector.simulate_user_stream_reconnect()
        assert not rm._circuit_breaker.tripped
        assert not sm.is_active

    @pytest.mark.asyncio
    async def test_exchange_api_timeout_activates_safe_mode(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_exchange_api_timeout()
        assert sm.is_active
        assert SafeModeReason.EXCHANGE_API_TIMEOUT in sm.active_reasons

    @pytest.mark.asyncio
    async def test_exchange_api_recovery(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_exchange_api_timeout()
        await injector.simulate_exchange_api_recovery()
        assert not sm.is_active

    @pytest.mark.asyncio
    async def test_db_outage_activates_safe_mode(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_db_outage()
        assert sm.is_active
        assert SafeModeReason.DB_OUTAGE in sm.active_reasons

    @pytest.mark.asyncio
    async def test_db_outage_blocks_new_trades(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_db_outage()
        signal = _make_signal()
        approved, reason, _ = rm.approve_signal(signal)
        assert not approved

    @pytest.mark.asyncio
    async def test_db_recovery(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_db_outage()
        await injector.simulate_db_recovery()
        assert not sm.is_active

    @pytest.mark.asyncio
    async def test_order_rejection_burst_activates_safe_mode(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_order_rejection("BTCUSDT", count=5)
        assert sm.is_active
        assert SafeModeReason.ORDER_REJECTION_BURST in sm.active_reasons

    @pytest.mark.asyncio
    async def test_volatility_spike_activates_safe_mode(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_volatility_spike("binance", "BTCUSDT", 0.10)
        assert sm.is_active
        assert SafeModeReason.EXTREME_VOLATILITY in sm.active_reasons

    @pytest.mark.asyncio
    async def test_spread_widening_activates_safe_mode(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_spread_widening()
        assert sm.is_active
        assert SafeModeReason.EXTREME_SPREAD in sm.active_reasons

    @pytest.mark.asyncio
    async def test_spread_recovery(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_spread_widening()
        await injector.simulate_spread_recovery()
        assert not sm.is_active

    @pytest.mark.asyncio
    async def test_fault_history_recorded(self, setup):
        rm, bus, sm, injector = setup
        await injector.simulate_websocket_disconnect()
        await injector.simulate_db_outage()
        history = injector.history
        assert len(history) == 2
        assert history[0]["fault"] == "websocket_disconnect"
        assert history[1]["fault"] == "db_outage"


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: WebSocket disconnect mid-position
# ═══════════════════════════════════════════════════════════════════════════

class TestWebSocketDisconnectMidPosition:
    @pytest.mark.asyncio
    async def test_positions_survive_ws_disconnect(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        _open_test_position(rm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_websocket_disconnect()
        # Position still exists
        assert len(rm._positions) == 1
        # But new trades are blocked
        signal = _make_signal(symbol="ETHUSDT")
        approved, reason, _ = rm.approve_signal(signal)
        assert not approved

    @pytest.mark.asyncio
    async def test_ws_reconnect_allows_new_trades_after_clearing(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_websocket_disconnect()
        await injector.simulate_websocket_reconnect()
        signal = _make_signal()
        approved, reason, _ = rm.approve_signal(signal)
        # Should not contain safe_mode
        assert "safe_mode_active" not in reason


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Bot crash mid-position (exchange-side SL protection)
# ═══════════════════════════════════════════════════════════════════════════

class TestBotCrashMidPosition:
    @pytest.mark.asyncio
    async def test_crash_preserves_positions(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        pos = _open_test_position(rm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        snapshot = await injector.simulate_bot_crash_mid_position()
        assert "binance:BTCUSDT" in snapshot
        # Positions still have valid SL/TP for exchange-side protection
        assert snapshot["binance:BTCUSDT"].stop_loss == 49_250.0
        assert snapshot["binance:BTCUSDT"].take_profit == 51_500.0

    @pytest.mark.asyncio
    async def test_crash_with_no_positions(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        snapshot = await injector.simulate_bot_crash_mid_position()
        assert snapshot == {}


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Partial fills
# ═══════════════════════════════════════════════════════════════════════════

class TestPartialFills:
    @pytest.mark.asyncio
    async def test_partial_fill_publishes_event(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_partial_fill("BTCUSDT", filled_pct=0.3)
        # Check event was published
        user_updates = [e for e in bus._published if e[0] == "USER_ORDER_UPDATE"]
        assert len(user_updates) == 1
        assert user_updates[0][1]["order_status"] == "PARTIALLY_FILLED"


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Volatility spike triggers SL on existing position
# ═══════════════════════════════════════════════════════════════════════════

class TestVolatilitySpike:
    @pytest.mark.asyncio
    async def test_volatility_spike_triggers_stop_loss(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        pos = _open_test_position(rm)
        # Subscribe the tick handler
        bus.subscribe("TICK", rm._handle_tick)

        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_volatility_spike("binance", "BTCUSDT", price_drop_pct=0.10)

        # SL should have been triggered (price crashed to ~45000, SL is 49250)
        sl_events = [e for e in bus._published if e[0] == "STOP_LOSS"]
        assert len(sl_events) > 0

    @pytest.mark.asyncio
    async def test_volatility_spike_blocks_new_trades(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_volatility_spike("binance", "BTCUSDT", 0.15)
        signal = _make_signal()
        approved, reason, _ = rm.approve_signal(signal)
        assert not approved
        assert "safe_mode_active" in reason


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Multiple simultaneous failures
# ═══════════════════════════════════════════════════════════════════════════

class TestMultipleSimultaneousFailures:
    @pytest.mark.asyncio
    async def test_all_faults_activate_safe_mode(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.run_all_faults()
        assert sm.is_active
        assert len(sm.active_reasons) >= 5  # WS, API, DB, volatility, spread, rejections

    @pytest.mark.asyncio
    async def test_clearing_one_fault_keeps_safe_mode(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_websocket_disconnect()
        await injector.simulate_db_outage()
        sm.deactivate(SafeModeReason.WEBSOCKET_DISCONNECT)
        assert sm.is_active  # DB_OUTAGE still on
        sm.deactivate(SafeModeReason.DB_OUTAGE)
        assert not sm.is_active

    @pytest.mark.asyncio
    async def test_admin_clear_all_overrides(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.run_all_faults()
        sm.clear_all()
        assert not sm.is_active


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Circuit breaker integration
# ═══════════════════════════════════════════════════════════════════════════

class TestCircuitBreakerIntegration:
    def test_circuit_breaker_trip_blocks_trades(self):
        rm, bus = _make_risk_manager()
        rm._circuit_breaker.trip("test_fault")
        signal = _make_signal()
        approved, reason, _ = rm.approve_signal(signal)
        assert not approved
        assert "circuit_breaker" in reason

    def test_circuit_breaker_clear_allows_trades(self):
        rm, bus = _make_risk_manager()
        rm._circuit_breaker.trip("test_fault")
        rm._circuit_breaker.clear_if_reason("test_fault")
        signal = _make_signal()
        approved, reason, _ = rm.approve_signal(signal)
        # Should not be blocked by circuit breaker
        assert "circuit_breaker" not in reason

    def test_risk_circuit_breaker_daily_loss_trip(self):
        rm, bus = _make_risk_manager()
        # Simulate 4% daily loss (exceeds default 3%)
        rm._circuit_breaker.record_pnl(-0.04, rm._equity)
        assert rm._circuit_breaker.tripped

    def test_risk_circuit_breaker_drawdown_trip(self):
        rm, bus = _make_risk_manager()
        rm._circuit_breaker._peak_equity = 100_000.0
        # Simulate 13% drawdown (exceeds default 12% — V1.0 Spec)
        rm._circuit_breaker.record_pnl(-0.001, 87_000.0)
        assert rm._circuit_breaker.tripped


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Kill switch
# ═══════════════════════════════════════════════════════════════════════════

class TestKillSwitch:
    @pytest.mark.asyncio
    async def test_kill_switch_closes_all_positions(self):
        rm, bus = _make_risk_manager()
        _open_test_position(rm, symbol="BTCUSDT")
        _open_test_position(rm, symbol="ETHUSDT")
        assert len(rm._positions) == 2
        closed = await rm.activate_kill_switch()
        assert len(closed) == 2
        assert len(rm._positions) == 0
        assert rm.killed

    @pytest.mark.asyncio
    async def test_kill_switch_blocks_new_trades(self):
        rm, bus = _make_risk_manager()
        await rm.activate_kill_switch()
        signal = _make_signal()
        approved, reason, _ = rm.approve_signal(signal)
        assert not approved
        assert "kill_switch_active" in reason

    @pytest.mark.asyncio
    async def test_kill_switch_deactivation_allows_trades(self):
        rm, bus = _make_risk_manager()
        await rm.activate_kill_switch()
        rm.deactivate_kill_switch()
        signal = _make_signal()
        approved, reason, _ = rm.approve_signal(signal)
        assert "kill_switch_active" not in reason


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Order rejection (exchange rejects order)
# ═══════════════════════════════════════════════════════════════════════════

class TestOrderRejection:
    @pytest.mark.asyncio
    async def test_rejection_burst_activates_safe_mode(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_order_rejection("BTCUSDT", count=5)
        assert sm.is_active

    @pytest.mark.asyncio
    async def test_few_rejections_do_not_activate_safe_mode(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_order_rejection("BTCUSDT", count=2)
        assert not sm.is_active  # < 3 threshold

    @pytest.mark.asyncio
    async def test_rejection_events_published(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_order_rejection("BTCUSDT", count=3)
        reject_events = [e for e in bus._published if e[0] == "USER_ORDER_UPDATE"]
        assert len(reject_events) == 3


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: DB outage — bot continues with positions protected
# ═══════════════════════════════════════════════════════════════════════════

class TestDBOutage:
    @pytest.mark.asyncio
    async def test_db_outage_positions_survive(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        pos = _open_test_position(rm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_db_outage()
        # Positions survive — they're in memory
        assert "binance:BTCUSDT" in rm._positions
        assert rm._positions["binance:BTCUSDT"].stop_loss == pos.stop_loss

    @pytest.mark.asyncio
    async def test_db_recovery_clears_safe_mode(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_db_outage()
        await injector.simulate_db_recovery()
        assert not sm.is_active


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: SL placement failure — circuit breaker trip
# ═══════════════════════════════════════════════════════════════════════════

class TestSLPlacementFailure:
    def test_sl_failure_trips_circuit_breaker(self):
        rm, bus = _make_risk_manager()
        rm._circuit_breaker.trip("sl_placement_failed:BTCUSDT")
        assert rm._circuit_breaker.tripped
        signal = _make_signal()
        approved, reason, _ = rm.approve_signal(signal)
        assert not approved
        assert "circuit_breaker" in reason

    def test_sl_failure_reason_clearable(self):
        rm, bus = _make_risk_manager()
        rm._circuit_breaker.trip("sl_placement_failed:BTCUSDT")
        rm._circuit_breaker.clear_if_reason("sl_placement_failed:BTCUSDT")
        assert not rm._circuit_breaker.tripped


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Exchange-side SL/TP verified after crash
# ═══════════════════════════════════════════════════════════════════════════

class TestExchangeSideSLTP:
    @pytest.mark.asyncio
    async def test_position_has_valid_sl_tp(self):
        """Verify that any open position always has valid SL/TP values
        that can be used for exchange-side protective orders."""
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        signal = _make_signal()
        approved, reason, size, pos = await rm.approve_and_open(signal)
        if approved:
            assert pos is not None
            assert pos.stop_loss > 0
            assert pos.take_profit > 0
            if pos.direction == "long":
                assert pos.stop_loss < pos.entry_price
                assert pos.take_profit > pos.entry_price
            else:
                assert pos.stop_loss > pos.entry_price
                assert pos.take_profit < pos.entry_price


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Equity zero / negative triggers kill switch
# ═══════════════════════════════════════════════════════════════════════════

class TestEquityProtection:
    @pytest.mark.asyncio
    async def test_equity_zero_triggers_kill_switch(self):
        rm, bus = _make_risk_manager()
        rm._equity = 100.0  # Very low equity
        pos = _open_test_position(rm)
        pos.size = 100.0  # Huge position
        # Close at a disastrous price
        closed = await rm.close_position("binance", "BTCUSDT", 1.0)
        assert rm.killed or rm._equity == 0.0


# ═══════════════════════════════════════════════════════════════════════════
# End-to-end: Full chaos run then recovery
# ═══════════════════════════════════════════════════════════════════════════

class TestFullChaosRun:
    @pytest.mark.asyncio
    async def test_full_chaos_run_and_recovery(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        pos = _open_test_position(rm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)

        # Run all faults
        log = await injector.run_all_faults()
        assert len(log) >= 7
        assert sm.is_active

        # Verify position survived
        assert "binance:BTCUSDT" in rm._positions

        # Verify new trades blocked
        signal = _make_signal(symbol="ETHUSDT")
        approved, reason, _ = rm.approve_signal(signal)
        assert not approved

        # Recovery: clear all faults
        sm.clear_all()
        assert not sm.is_active

        # New trades allowed again (safe_mode no longer blocking)
        approved2, reason2, _ = rm.approve_signal(signal)
        assert "safe_mode_active" not in reason2

    @pytest.mark.asyncio
    async def test_safe_mode_status_snapshot(self):
        sm = SafeModeManager()
        rm, bus = _make_risk_manager(safe_mode=sm)
        injector = FaultInjector(event_bus=bus, risk_manager=rm, safe_mode=sm)
        await injector.simulate_websocket_disconnect()
        await injector.simulate_db_outage()
        status = sm.get_status()
        assert status["safe_mode_active"] is True
        assert len(status["active_reasons"]) == 2
