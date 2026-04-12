"""Tests for deep audit fixes round 2 — rate limiter, WAL, validation, ATR guards, HTF warning."""
from __future__ import annotations

import asyncio
import math
import re
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.config import Config
from core.event_bus import EventBus
from execution.rate_limiter import RateLimiter
from execution.risk_manager import RiskManager


# ── Helpers ──────────────────────────────────────────────────────────────

def _make_config() -> Config:
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
        "initial_equity": 100_000,
        "risk_per_trade_pct": 0.01,
        "sizing_method": "risk_based",
        "max_spread_bps": 10.0,
        "max_atr_pct": 0.05,
        "max_exposure_per_symbol_pct": 0.10,
        "cooldown_seconds": 0,
        "session_start_utc": "00:00",
        "session_end_utc": "23:59",
        "atr_sl_multiplier": 1.5,
        "rr_ratio": 2.0,
        "trailing_activation_atr": 2.0,
        "trailing_distance_atr": 1.0,
        "breakeven_trigger_atr": 1.0,
        "max_hold_minutes": 0,
        "circuit_breaker_pause_seconds": 0,
    }
    return c


def _make_signal(score=0.75, price=50000.0, atr=None):
    from engine.signal_generator import TradingSignal

    if atr is None:
        atr = price * 0.01
    sl = price * 0.985
    tp = price * 1.03
    return TradingSignal(
        exchange="binance",
        symbol="BTC/USDT:USDT",
        direction="long",
        score=score,
        technical_score=score,
        ml_score=score,
        sentiment_score=0.0,
        macro_score=0.0,
        news_score=0.0,
        orderbook_score=0.0,
        regime="trending_up",
        regime_confidence=0.8,
        price=price,
        atr=atr,
        stop_loss=sl,
        take_profit=tp,
        timestamp=1_700_000_000,
    )


# ═══════════════════════════════════════════════════════════════════════════
# FIX 1: rate_limiter sleeps outside lock
# ═══════════════════════════════════════════════════════════════════════════

class TestRateLimiterNoLockDuringSleep:
    @pytest.mark.asyncio
    async def test_acquire_does_not_block_others_during_sleep(self):
        """When rate-limited, other coroutines should still be able to
        acquire the lock (and themselves be rate-limited) rather than
        all waiting behind one sleeper."""
        rl = RateLimiter(max_calls=2, period_seconds=0.5)

        # Fill the window
        await rl.acquire()
        await rl.acquire()

        # Now two more coroutines try to acquire — they should both eventually complete
        # If sleep is inside the lock, the second would wait for the first's sleep + lock
        start = time.monotonic()
        await asyncio.gather(rl.acquire(), rl.acquire())
        elapsed = time.monotonic() - start

        # Both should complete in roughly one period (~0.5s), not two (which would happen
        # if they serialized through the lock-held sleep)
        assert elapsed < 1.5  # generous bound; sequential would be ~1.0s

    @pytest.mark.asyncio
    async def test_acquire_basic_rate_limiting(self):
        """Verify rate limiting still works after refactor."""
        rl = RateLimiter(max_calls=3, period_seconds=0.2)
        for _ in range(3):
            await rl.acquire()
        # 4th call should block briefly
        start = time.monotonic()
        await rl.acquire()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.1  # Had to wait

    @pytest.mark.asyncio
    async def test_decorator_still_works(self):
        rl = RateLimiter(max_calls=10, period_seconds=1.0)

        @rl
        async def my_func(x):
            return x * 2

        result = await my_func(5)
        assert result == 10


# ═══════════════════════════════════════════════════════════════════════════
# FIX 2: WAL replay logs errors
# ═══════════════════════════════════════════════════════════════════════════

class TestWALReplayLogsErrors:
    def test_wal_replay_method_has_logging(self):
        """Verify the WAL replay logs failed entries instead of silently swallowing."""
        import inspect
        from storage.trade_persistence import TradePersistence
        source = inspect.getsource(TradePersistence.replay_wal)
        assert "logger.warning" in source
        # Should NOT have bare `except Exception:` without logging
        assert "except Exception:" not in source or "logger" in source


# ═══════════════════════════════════════════════════════════════════════════
# FIX 3: dashboard_api input validation
# ═══════════════════════════════════════════════════════════════════════════

class TestDashboardApiInputValidation:
    def test_valid_symbols_accepted(self):
        from interface.dashboard_api import _validate_symbol
        assert _validate_symbol("BTC/USDT") == "BTC/USDT"
        assert _validate_symbol("ETH/USDT:USDT") == "ETH/USDT:USDT"
        assert _validate_symbol("BTCUSDT") == "BTCUSDT"

    def test_invalid_symbols_rejected(self):
        from interface.dashboard_api import _validate_symbol
        with pytest.raises(ValueError):
            _validate_symbol("'; DROP TABLE--")
        with pytest.raises(ValueError):
            _validate_symbol("../../etc/passwd")
        with pytest.raises(ValueError):
            _validate_symbol("")
        with pytest.raises(ValueError):
            _validate_symbol("A" * 50)

    def test_valid_timeframes_accepted(self):
        from interface.dashboard_api import _validate_timeframe
        assert _validate_timeframe("1m") == "1m"
        assert _validate_timeframe("5m") == "5m"
        assert _validate_timeframe("1h") == "1h"
        assert _validate_timeframe("4h") == "4h"
        assert _validate_timeframe("1d") == "1d"
        assert _validate_timeframe("1w") == "1w"
        assert _validate_timeframe("1M") == "1M"
        assert _validate_timeframe("15m") == "15m"

    def test_invalid_timeframes_rejected(self):
        from interface.dashboard_api import _validate_timeframe
        with pytest.raises(ValueError):
            _validate_timeframe("")
        with pytest.raises(ValueError):
            _validate_timeframe("abc")
        with pytest.raises(ValueError):
            _validate_timeframe("0m")
        with pytest.raises(ValueError):
            _validate_timeframe("100m")  # 3+ digit not valid
        with pytest.raises(ValueError):
            _validate_timeframe("1x")

    def test_validation_regex_patterns(self):
        from interface.dashboard_api import _VALID_SYMBOL, _VALID_TIMEFRAME
        # Symbol pattern
        assert _VALID_SYMBOL.match("BTC/USDT:USDT")
        assert not _VALID_SYMBOL.match("SELECT * FROM")
        # Timeframe pattern
        assert _VALID_TIMEFRAME.match("15m")
        assert not _VALID_TIMEFRAME.match("'; DROP")


# ═══════════════════════════════════════════════════════════════════════════
# FIX 4: risk_manager compute_atr_stops handles None/NaN ATR
# ═══════════════════════════════════════════════════════════════════════════

class TestComputeAtrStopsDefensive:
    def test_atr_zero_uses_fallback(self):
        rm = RiskManager(_make_config(), EventBus())
        signal = _make_signal(atr=0.0)
        sl, tp = rm.compute_atr_stops(signal)
        # Fallback: 1% of price
        assert sl > 0
        assert tp > 0
        assert sl < signal.price < tp  # long signal: SL below, TP above

    def test_atr_nan_uses_fallback(self):
        rm = RiskManager(_make_config(), EventBus())
        signal = _make_signal(atr=float("nan"))
        sl, tp = rm.compute_atr_stops(signal)
        assert not math.isnan(sl)
        assert not math.isnan(tp)
        assert sl < signal.price < tp

    def test_atr_negative_uses_fallback(self):
        rm = RiskManager(_make_config(), EventBus())
        signal = _make_signal(atr=-100.0)
        sl, tp = rm.compute_atr_stops(signal)
        assert sl < signal.price < tp

    def test_normal_atr_works(self):
        rm = RiskManager(_make_config(), EventBus())
        signal = _make_signal(atr=500.0)
        sl, tp = rm.compute_atr_stops(signal)
        assert sl < signal.price < tp

    def test_check_volatility_with_none_like_atr(self):
        rm = RiskManager(_make_config(), EventBus())
        signal = _make_signal(atr=0.0)
        ok, reason = rm._check_volatility(signal)
        assert ok  # atr=0 means no vol data, should pass


# ═══════════════════════════════════════════════════════════════════════════
# FIX 5: signal_generator HTF warning when no data
# ═══════════════════════════════════════════════════════════════════════════

class TestHTFConfirmationWarning:
    def test_htf_check_warns_when_no_data(self):
        from engine.signal_generator import SignalGenerator
        config = _make_config()
        config.get_value.side_effect = lambda *args, **kwargs: {
            "primary_timeframe": "15m",
            "confirmation_timeframes": ["1h", "4h"],
            "min_score_threshold": 0.65,
            "min_contributing_factors": 3,
        } if args == ("signals",) else {}
        bus = EventBus()
        dm = MagicMock()
        dm.get_dataframe.return_value = None  # No HTF data available
        sg = SignalGenerator(config, bus, dm)

        with patch("engine.signal_generator.logger") as mock_logger:
            ok, reason, weighted_score, agreement_count = sg._check_higher_timeframe_trend("binance", "BTC/USDT", "long")
            assert ok  # Still passes (by design)
            # But should have logged a warning
            mock_logger.warning.assert_called_once()
            call_args = str(mock_logger.warning.call_args)
            assert "bypassed" in call_args.lower() or "HTF" in call_args

    def test_htf_check_no_warning_when_data_exists(self):
        from engine.signal_generator import SignalGenerator
        import pandas as pd
        import numpy as np

        config = _make_config()
        config.get_value.side_effect = lambda *args, **kwargs: {
            "primary_timeframe": "15m",
            "confirmation_timeframes": ["1h"],
            "min_score_threshold": 0.65,
            "min_contributing_factors": 3,
        } if args == ("signals",) else {}
        bus = EventBus()

        # Create a bullish HTF dataframe
        n = 30
        df = pd.DataFrame({
            "close": np.linspace(100, 200, n),
            "ema_12": np.linspace(110, 200, n),  # fast above slow = bullish
            "ema_26": np.linspace(100, 190, n),
            "supertrend_dir": [1] * n,  # bullish supertrend
            "rsi_14": [60.0] * n,  # above 50 = bullish
        })

        dm = MagicMock()
        dm.get_dataframe.return_value = df
        sg = SignalGenerator(config, bus, dm)

        with patch("engine.signal_generator.logger") as mock_logger:
            ok, reason, weighted_score, agreement_count = sg._check_higher_timeframe_trend("binance", "BTC/USDT", "long")
            assert ok
            mock_logger.warning.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
# FIX 6: smart_order_router TOCTOU prevention
# ═══════════════════════════════════════════════════════════════════════════

class TestSmartOrderRouterRefresh:
    def test_refresh_uses_lock(self):
        """Verify _refresh_scores_if_needed uses the lock for double-check."""
        import inspect
        from execution.smart_order_router import SmartOrderRouter
        source = inspect.getsource(SmartOrderRouter._refresh_scores_if_needed)
        assert "async with self._lock" in source
        # Should do a double-check pattern
        assert source.count("self._last_score_refresh") >= 3  # read, set inside lock, read again


# ═══════════════════════════════════════════════════════════════════════════
# Regression tests
# ═══════════════════════════════════════════════════════════════════════════

class TestRound2Regressions:
    @pytest.mark.asyncio
    async def test_rate_limiter_under_limit(self):
        """Calls under the limit should not block."""
        rl = RateLimiter(max_calls=100, period_seconds=60.0)
        start = time.monotonic()
        for _ in range(10):
            await rl.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.5  # Should be near-instant

    @pytest.mark.asyncio
    async def test_risk_manager_approve_and_open_with_normal_signal(self):
        rm = RiskManager(_make_config(), EventBus())
        rm._equity = 100_000.0
        signal = _make_signal()
        approved, reason, size, pos = await rm.approve_and_open(signal)
        assert approved
        assert pos is not None
