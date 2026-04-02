"""Unit tests for the risk manager."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from core.config import Config
from core.event_bus import EventBus
from engine.signal_generator import TradingSignal
from execution.risk_manager import RiskManager, CircuitBreaker


@pytest.fixture
def config() -> Config:
    c = MagicMock(spec=Config)
    c.paper_mode = True
    c.get_value.return_value = {
        "max_position_size_pct": 0.02,
        "max_open_positions": 5,
        "default_leverage": 1.0,
        "max_daily_loss_pct": 0.03,
        "max_drawdown_pct": 0.10,
        "max_portfolio_var_pct": 0.08,
        "returns_window": 250,
        "var_min_history": 30,
        "stop_loss_pct": 0.015,
        "take_profit_pct": 0.03,
    }
    return c


@pytest.fixture
def risk_manager(config: Config) -> RiskManager:
    bus = EventBus()
    rm = RiskManager(config, bus)
    rm._equity = 100_000.0
    return rm


def _make_signal(score: float = 0.75, price: float = 50000.0) -> TradingSignal:
    sl = price * 0.985
    tp = price * 1.03
    rr = (tp - price) / (price - sl)
    return TradingSignal(
        exchange="binance",
        symbol="BTC/USDT:USDT",
        direction="long",
        score=score,
        technical_score=score,
        ml_score=score,
        sentiment_score=0.0,
        macro_score=0.0,
        regime="trending_up",
        regime_confidence=0.8,
        price=price,
        atr=price * 0.01,
        stop_loss=sl,
        take_profit=tp,
        timestamp=1_700_000_000,
    )


class TestRiskManager:
    def test_approve_valid_signal(self, risk_manager: RiskManager) -> None:
        signal = _make_signal()
        approved, reason, size = risk_manager.approve_signal(signal)
        assert approved is True
        assert size > 0

    def test_reject_low_score(self, risk_manager: RiskManager) -> None:
        signal = _make_signal(score=0.3)
        approved, reason, size = risk_manager.approve_signal(signal)
        assert approved is False
        assert "score" in reason.lower()

    def test_reject_poor_risk_reward(self, risk_manager: RiskManager) -> None:
        sl = 49999.0
        tp = 50001.0
        signal = TradingSignal(
            exchange="binance", symbol="BTC/USDT:USDT", direction="long",
            score=0.9, technical_score=0.9, ml_score=0.9,
            sentiment_score=0.0, macro_score=0.0,
            regime="trending_up", regime_confidence=0.8,
            price=50000.0, atr=500.0,
            stop_loss=sl, take_profit=tp, timestamp=1_700_000_000,
        )
        approved, reason, _ = risk_manager.approve_signal(signal)
        assert approved is False

    def test_open_and_close_position(self, risk_manager: RiskManager) -> None:
        signal = _make_signal()
        pos = risk_manager.open_position(signal, size=1000.0)
        assert pos is not None
        assert len(risk_manager.positions) == 1

        closed = risk_manager.close_position("binance", "BTC/USDT:USDT", 51500.0)
        assert closed is not None
        assert len(risk_manager.positions) == 0
        assert closed.pnl_pct > 0

    def test_max_positions_limit(self, risk_manager: RiskManager) -> None:
        for i in range(5):
            sig = _make_signal()
            sig.symbol = f"TOKEN{i}/USDT"
            approved, _, size = risk_manager.approve_signal(sig)
            if approved:
                risk_manager.open_position(sig, size)
        extra = _make_signal()
        extra.symbol = "EXTRA/USDT"
        approved, reason, _ = risk_manager.approve_signal(extra)
        assert approved is False
        assert "max_positions" in reason.lower()

    def test_calculate_var_and_cvar(self, risk_manager: RiskManager) -> None:
        returns = [-0.02, -0.01, 0.01, -0.03, 0.015, -0.005, -0.04, 0.02]
        for r in returns:
            risk_manager.record_return(r)

        var95 = risk_manager.calculate_historical_var(0.95)
        cvar95 = risk_manager.calculate_cvar(0.95)

        assert var95 > 0
        assert cvar95 > 0
        assert cvar95 >= var95

    def test_stress_test_output(self, risk_manager: RiskManager) -> None:
        signal = _make_signal(price=50000.0)
        pos = risk_manager.open_position(signal, size=1000.0)
        assert pos is not None

        report = risk_manager.run_stress_test(shocks=[-0.10], correlation_breakdown_factor=2.0)
        assert report["portfolio_notional_usd"] == pytest.approx(1000.0)
        assert "shock_10pct" in report["scenarios"]
        assert report["scenarios"]["shock_10pct"]["estimated_loss_usd"] == pytest.approx(100.0)
        assert report["scenarios"]["correlation_breakdown"]["estimated_loss_usd"] == pytest.approx(200.0)

    def test_var_gate_rejects_signal_when_limit_exceeded(self, risk_manager: RiskManager) -> None:
        for _ in range(30):
            risk_manager.record_return(-0.10)

        signal = _make_signal()
        approved, reason, _ = risk_manager.approve_signal(signal)
        assert approved is False
        assert "var_limit_breach" in reason


class TestCircuitBreaker:
    def test_daily_loss_trips_breaker(self) -> None:
        cb = CircuitBreaker(max_daily_loss_pct=0.03, max_drawdown_pct=0.10)
        cb.record_pnl(-0.01, 99000)
        assert not cb.tripped
        cb.record_pnl(-0.025, 97500)
        assert cb.tripped
        assert "daily_loss" in cb.trip_reason

    def test_reset_clears_breaker(self) -> None:
        cb = CircuitBreaker(max_daily_loss_pct=0.03, max_drawdown_pct=0.10)
        cb.record_pnl(-0.05, 95000)
        assert cb.tripped
        cb.reset()
        assert not cb.tripped
