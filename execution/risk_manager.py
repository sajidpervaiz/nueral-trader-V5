from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
from loguru import logger

from core.config import Config
from core.event_bus import EventBus
from engine.signal_generator import TradingSignal


@dataclass
class Position:
    exchange: str
    symbol: str
    direction: str
    size: float
    entry_price: float
    current_price: float
    stop_loss: float
    take_profit: float
    open_time: int
    pnl: float = 0.0
    pnl_pct: float = 0.0
    trailing_active: bool = False
    trailing_stop: float = 0.0
    breakeven_moved: bool = False
    highest_since_entry: float = 0.0
    lowest_since_entry: float = float("inf")

    @property
    def is_long(self) -> bool:
        return self.direction == "long"

    def update_price(self, price: float) -> None:
        self.current_price = price
        if self.is_long:
            self.pnl_pct = (price - self.entry_price) / self.entry_price
            if price > self.highest_since_entry:
                self.highest_since_entry = price
        else:
            self.pnl_pct = (self.entry_price - price) / self.entry_price
            if price < self.lowest_since_entry:
                self.lowest_since_entry = price
        self.pnl = self.pnl_pct * self.size * self.entry_price

    @property
    def hold_seconds(self) -> int:
        return int(time.time()) - self.open_time


class CircuitBreaker:
    def __init__(
        self,
        max_daily_loss_pct: float,
        max_drawdown_pct: float,
        pause_seconds: float = 3600.0,
    ) -> None:
        self._max_daily_loss = max_daily_loss_pct
        self._max_drawdown = max_drawdown_pct
        self._pause_seconds = pause_seconds
        self._daily_loss = 0.0
        self._peak_equity = 0.0
        self._tripped = False
        self._trip_reason = ""
        self._trip_time: float = 0.0

    def reset_daily(self) -> None:
        self._daily_loss = 0.0

    def record_pnl(self, pnl_pct: float, equity: float) -> bool:
        self._daily_loss += min(0, pnl_pct)
        if equity > self._peak_equity:
            self._peak_equity = equity
        drawdown = (self._peak_equity - equity) / self._peak_equity if self._peak_equity > 0 else 0.0
        if abs(self._daily_loss) >= self._max_daily_loss:
            self._tripped = True
            self._trip_reason = f"daily_loss={self._daily_loss:.2%}"
            self._trip_time = time.time()
        if drawdown >= self._max_drawdown:
            self._tripped = True
            self._trip_reason = f"drawdown={drawdown:.2%}"
            self._trip_time = time.time()
        return self._tripped

    @property
    def tripped(self) -> bool:
        if self._tripped and self._pause_seconds > 0:
            elapsed = time.time() - self._trip_time
            if elapsed >= self._pause_seconds:
                logger.info(
                    "Circuit breaker auto-reset after {:.0f}s pause (was: {})",
                    elapsed, self._trip_reason,
                )
                self._tripped = False
                self._trip_reason = ""
        return self._tripped

    @property
    def trip_reason(self) -> str:
        return self._trip_reason

    def reset(self) -> None:
        self._tripped = False
        self._trip_reason = ""
        self._daily_loss = 0.0


class RiskManager:
    def __init__(self, config: Config, event_bus: EventBus) -> None:
        self.config = config
        self.event_bus = event_bus
        risk_cfg = config.get_value("risk") or {}

        # ── Position sizing ───────────────────────────────────────────────
        self._max_position_pct = float(risk_cfg.get("max_position_size_pct", 0.02))
        self._risk_per_trade = float(risk_cfg.get("risk_per_trade_pct", 0.01))
        self._sizing_method = str(risk_cfg.get("sizing_method", "risk_based"))  # risk_based | fixed_fraction
        self._kelly_fraction = float(risk_cfg.get("kelly_fraction", 0.25))
        self._max_open = int(risk_cfg.get("max_open_positions", 5))
        self._leverage = float(risk_cfg.get("default_leverage", 1.0))

        # ── Pre-trade filters ─────────────────────────────────────────────
        self._max_spread_bps = float(risk_cfg.get("max_spread_bps", 10.0))
        self._max_atr_pct = float(risk_cfg.get("max_atr_pct", 0.05))
        self._max_exposure_per_symbol = float(risk_cfg.get("max_exposure_per_symbol_pct", 0.10))
        self._cooldown_seconds = float(risk_cfg.get("cooldown_seconds", 300.0))
        self._session_start_utc = str(risk_cfg.get("session_start_utc", "00:00"))
        self._session_end_utc = str(risk_cfg.get("session_end_utc", "23:59"))

        # ── Position management ───────────────────────────────────────────
        self._atr_sl_multiplier = float(risk_cfg.get("atr_sl_multiplier", 1.5))
        self._rr_ratio = float(risk_cfg.get("rr_ratio", 2.0))
        self._trailing_activation_atr = float(risk_cfg.get("trailing_activation_atr", 2.0))
        self._trailing_distance_atr = float(risk_cfg.get("trailing_distance_atr", 1.0))
        self._breakeven_trigger_atr = float(risk_cfg.get("breakeven_trigger_atr", 1.0))
        self._max_hold_minutes = int(risk_cfg.get("max_hold_minutes", 0))  # 0 = disabled

        # ── VaR ───────────────────────────────────────────────────────────
        self._max_portfolio_var_pct = float(risk_cfg.get("max_portfolio_var_pct", 0.08))
        self._returns_window = int(risk_cfg.get("returns_window", 250))
        self._var_min_history = int(risk_cfg.get("var_min_history", 30))

        # ── New risk filters ──────────────────────────────────────────────
        self._max_funding_rate_bps = float(risk_cfg.get("max_funding_rate_bps", 50.0))
        self._min_orderbook_depth_usd = float(risk_cfg.get("min_orderbook_depth_usd", 50_000.0))
        self._max_order_size_usd = float(risk_cfg.get("max_order_size_usd", 500_000.0))
        self._max_leverage_per_symbol: dict[str, float] = risk_cfg.get("max_leverage_per_symbol", {})

        # ── State ─────────────────────────────────────────────────────────
        self._positions: dict[str, Position] = {}
        self._equity = float(risk_cfg.get("initial_equity", 100_000.0))
        self._return_history: list[float] = []
        self._last_trade_time: dict[str, float] = {}
        self._killed = False
        self._current_funding_rates: dict[str, float] = {}  # symbol → rate (decimal)
        self._current_orderbook_depth: dict[str, float] = {}  # symbol → top-of-book USD
        self._kill_switch_file = Path("data/.kill_switch")
        self._circuit_breaker = CircuitBreaker(
            max_daily_loss_pct=float(risk_cfg.get("max_daily_loss_pct", 0.03)),
            max_drawdown_pct=float(risk_cfg.get("max_drawdown_pct", 0.10)),
            pause_seconds=float(risk_cfg.get("circuit_breaker_pause_seconds", 3600)),
        )
        self._running = False

    # ── Risk-based position sizing ────────────────────────────────────────
    def calculate_position_size(self, signal: TradingSignal) -> float:
        """size = (equity × risk_per_trade) / |entry − stop|"""
        risk_distance = abs(signal.price - signal.stop_loss)
        if risk_distance == 0:
            return 0.0

        if self._sizing_method == "risk_based":
            notional = (self._equity * self._risk_per_trade) / (risk_distance / signal.price)
        else:
            # Fixed-fraction fallback
            notional = self._equity * self._max_position_pct

        notional *= self._leverage

        # Cap at max position pct of equity
        max_notional = self._equity * self._max_position_pct * self._leverage
        notional = min(notional, max_notional)
        return max(0.0, notional)

    # ── Pre-trade gate checks ─────────────────────────────────────────────
    def _check_session_window(self) -> tuple[bool, str]:
        """Check if current UTC time is within trading session."""
        if self._session_start_utc == "00:00" and self._session_end_utc == "23:59":
            return True, ""
        now = time.gmtime()
        current_minutes = now.tm_hour * 60 + now.tm_min
        start_parts = self._session_start_utc.split(":")
        end_parts = self._session_end_utc.split(":")
        start_min = int(start_parts[0]) * 60 + int(start_parts[1])
        end_min = int(end_parts[0]) * 60 + int(end_parts[1])
        if start_min <= end_min:
            in_session = start_min <= current_minutes <= end_min
        else:
            in_session = current_minutes >= start_min or current_minutes <= end_min
        if not in_session:
            return False, f"outside_session ({self._session_start_utc}-{self._session_end_utc})"
        return True, ""

    def _check_cooldown(self, symbol: str) -> tuple[bool, str]:
        """Per-symbol post-trade cooldown."""
        last = self._last_trade_time.get(symbol, 0.0)
        elapsed = time.time() - last
        if elapsed < self._cooldown_seconds:
            remaining = self._cooldown_seconds - elapsed
            return False, f"cooldown ({remaining:.0f}s remaining for {symbol})"
        return True, ""

    def _check_spread(self, signal: TradingSignal) -> tuple[bool, str]:
        """Spread filter: reject if spread > max_spread_bps."""
        spread_bps = float(signal.metadata.get("spread_bps", 0.0))
        if spread_bps > 0 and spread_bps > self._max_spread_bps:
            return False, f"spread_too_wide ({spread_bps:.1f} > {self._max_spread_bps:.1f} bps)"
        return True, ""

    def _check_volatility(self, signal: TradingSignal) -> tuple[bool, str]:
        """Volatility filter: ATR(14)/price < max_atr_pct."""
        if signal.price <= 0:
            return True, ""
        atr_pct = signal.atr / signal.price
        if atr_pct > self._max_atr_pct:
            return False, f"volatility_too_high (atr_pct={atr_pct:.4f} > {self._max_atr_pct:.4f})"
        return True, ""

    def _check_symbol_exposure(self, signal: TradingSignal, proposed_size: float) -> tuple[bool, str]:
        """Max exposure per symbol."""
        max_exposure = self._equity * self._max_exposure_per_symbol
        if proposed_size > max_exposure:
            return False, f"symbol_exposure_exceeded ({proposed_size:.0f} > {max_exposure:.0f})"
        return True, ""

    def _check_funding_rate(self, signal: TradingSignal) -> tuple[bool, str]:
        """Funding rate filter: skip trading when funding is extreme."""
        rate = self._current_funding_rates.get(signal.symbol, 0.0)
        rate_bps = abs(rate) * 10_000
        if rate_bps > self._max_funding_rate_bps:
            return False, f"funding_rate_extreme ({rate_bps:.1f} > {self._max_funding_rate_bps:.1f} bps)"
        return True, ""

    def _check_orderbook_depth(self, signal: TradingSignal) -> tuple[bool, str]:
        """Order book depth filter: reject if top-of-book liquidity insufficient."""
        depth_usd = self._current_orderbook_depth.get(signal.symbol, float("inf"))
        if depth_usd < self._min_orderbook_depth_usd:
            return False, f"orderbook_depth_insufficient ({depth_usd:.0f} < {self._min_orderbook_depth_usd:.0f} USD)"
        return True, ""

    def _check_max_order_size(self, proposed_size: float) -> tuple[bool, str]:
        """Hard max order size cap per trade."""
        if proposed_size > self._max_order_size_usd:
            return False, f"max_order_size_exceeded ({proposed_size:.0f} > {self._max_order_size_usd:.0f} USD)"
        return True, ""

    def _check_liquidation_risk(self, signal: TradingSignal) -> tuple[bool, str]:
        """Refuse trades that place liquidation price too close to entry."""
        if self._leverage <= 1.0:
            return True, ""  # No leverage = no liquidation risk
        # Liquidation price ≈ entry × (1 - 1/leverage) for longs
        # Reject if stop_loss is beyond estimated liquidation price
        liq_margin = 1.0 / self._leverage
        if signal.is_long:
            est_liq = signal.price * (1 - liq_margin * 0.9)  # 90% of margin = danger zone
            if signal.stop_loss <= est_liq:
                return False, f"liquidation_risk (SL={signal.stop_loss:.2f} <= est_liq={est_liq:.2f})"
        else:
            est_liq = signal.price * (1 + liq_margin * 0.9)
            if signal.stop_loss >= est_liq:
                return False, f"liquidation_risk (SL={signal.stop_loss:.2f} >= est_liq={est_liq:.2f})"
        return True, ""

    def update_funding_rate(self, symbol: str, rate: float) -> None:
        """Update current funding rate for a symbol (in decimal, e.g., 0.0001)."""
        self._current_funding_rates[symbol] = rate

    def update_orderbook_depth(self, symbol: str, depth_usd: float) -> None:
        """Update current top-of-book depth in USD for a symbol."""
        self._current_orderbook_depth[symbol] = depth_usd

    def approve_signal(self, signal: TradingSignal) -> tuple[bool, str, float]:
        if self._killed:
            return False, "kill_switch_active", 0.0

        # File-based kill switch — manual override
        if self._kill_switch_file.exists():
            logger.critical("Kill switch FILE detected: {}", self._kill_switch_file)
            self._killed = True
            return False, "kill_switch_file_detected", 0.0

        if self._circuit_breaker.tripped:
            return False, f"circuit_breaker: {self._circuit_breaker.trip_reason}", 0.0

        key = f"{signal.exchange}:{signal.symbol}"
        if key in self._positions:
            return False, "already_in_position", 0.0

        if len(self._positions) >= self._max_open:
            return False, f"max_positions_reached ({self._max_open})", 0.0

        if signal.score < 0.5:
            return False, f"score_too_low ({signal.score:.2f})", 0.0

        if signal.risk_reward < 1.5:
            return False, f"poor_risk_reward ({signal.risk_reward:.2f})", 0.0

        # ── New pre-trade filters ─────────────────────────────────────────
        ok, reason = self._check_session_window()
        if not ok:
            return False, reason, 0.0

        ok, reason = self._check_cooldown(signal.symbol)
        if not ok:
            return False, reason, 0.0

        ok, reason = self._check_spread(signal)
        if not ok:
            return False, reason, 0.0

        ok, reason = self._check_volatility(signal)
        if not ok:
            return False, reason, 0.0

        ok, reason = self._check_funding_rate(signal)
        if not ok:
            return False, reason, 0.0

        ok, reason = self._check_orderbook_depth(signal)
        if not ok:
            return False, reason, 0.0

        ok, reason = self._check_liquidation_risk(signal)
        if not ok:
            return False, reason, 0.0

        # ── Position sizing ───────────────────────────────────────────────
        size = self.calculate_position_size(signal)
        if size <= 0:
            return False, "position_size_zero", 0.0

        ok, reason = self._check_symbol_exposure(signal, size)
        if not ok:
            return False, reason, 0.0

        ok, reason = self._check_max_order_size(size)
        if not ok:
            return False, reason, 0.0

        # ── VaR check ────────────────────────────────────────────────────
        if len(self._return_history) >= self._var_min_history and self._equity > 0:
            var95 = self.calculate_historical_var(0.95)
            projected_position_impact = size / self._equity
            projected_var = var95 + projected_position_impact
            if projected_var > self._max_portfolio_var_pct:
                return False, (
                    f"var_limit_breach (projected={projected_var:.2%}, "
                    f"limit={self._max_portfolio_var_pct:.2%})"
                ), 0.0

        return True, "approved", size

    def compute_atr_stops(
        self, signal: TradingSignal
    ) -> tuple[float, float]:
        """Compute ATR-based SL and RR-based TP."""
        atr = signal.atr if signal.atr > 0 else signal.price * 0.01
        sl_distance = atr * self._atr_sl_multiplier

        if signal.is_long:
            sl = signal.price - sl_distance
            tp = signal.price + sl_distance * self._rr_ratio
        else:
            sl = signal.price + sl_distance
            tp = signal.price - sl_distance * self._rr_ratio

        # Fallback: ensure SL is at least min percent away
        fallback_pct = float((self.config.get_value("risk") or {}).get("stop_loss_pct", 0.015))
        if signal.is_long:
            min_sl = signal.price * (1 - fallback_pct)
            sl = min(sl, min_sl)
        else:
            max_sl = signal.price * (1 + fallback_pct)
            sl = max(sl, max_sl)

        return sl, tp

    def open_position(self, signal: TradingSignal, size: float) -> Position:
        sl, tp = self.compute_atr_stops(signal)

        pos = Position(
            exchange=signal.exchange,
            symbol=signal.symbol,
            direction=signal.direction,
            size=size / signal.price if signal.price > 0 else 0.0,
            entry_price=signal.price,
            current_price=signal.price,
            stop_loss=sl,
            take_profit=tp,
            open_time=int(time.time()),
            highest_since_entry=signal.price,
            lowest_since_entry=signal.price,
        )
        key = f"{signal.exchange}:{signal.symbol}"
        self._positions[key] = pos
        self._last_trade_time[signal.symbol] = time.time()
        logger.info(
            "Position opened: {}/{} {} size={:.4f} @ {:.2f} SL={:.2f} TP={:.2f} (ATR-based)",
            signal.exchange, signal.symbol, signal.direction,
            pos.size, pos.entry_price, pos.stop_loss, pos.take_profit,
        )
        return pos

    def close_position(self, exchange: str, symbol: str, exit_price: float) -> Position | None:
        key = f"{exchange}:{symbol}"
        pos = self._positions.pop(key, None)
        if pos is None:
            return None
        pos.update_price(exit_price)
        pnl_dollar = pos.pnl
        self._equity += pnl_dollar
        self.record_return(pos.pnl_pct)
        self._circuit_breaker.record_pnl(pos.pnl_pct, self._equity)
        self._last_trade_time[symbol] = time.time()
        logger.info(
            "Position closed: {}/{} pnl={:.2f} ({:.2%}) equity={:.2f} held={:.0f}s",
            exchange, symbol, pnl_dollar, pos.pnl_pct, self._equity, pos.hold_seconds,
        )
        return pos

    # ── Trailing stop / breakeven / time exit ─────────────────────────────
    def manage_position(self, pos: Position) -> str | None:
        """Returns exit reason string if position should be closed, else None."""
        atr = pos.entry_price * 0.01  # fallback; real ATR should be on position

        # Time-based exit
        if self._max_hold_minutes > 0:
            if pos.hold_seconds >= self._max_hold_minutes * 60:
                return "time_exit"

        profit_in_atr = 0.0
        if pos.is_long:
            profit_in_atr = (pos.current_price - pos.entry_price) / atr if atr > 0 else 0
        else:
            profit_in_atr = (pos.entry_price - pos.current_price) / atr if atr > 0 else 0

        # Breakeven move: after +1×ATR profit, move SL to entry
        if not pos.breakeven_moved and profit_in_atr >= self._breakeven_trigger_atr:
            pos.stop_loss = pos.entry_price
            pos.breakeven_moved = True
            logger.info("Breakeven moved for {}/{}", pos.exchange, pos.symbol)

        # Trailing stop activation: after trailing_activation_atr × ATR profit
        if profit_in_atr >= self._trailing_activation_atr:
            pos.trailing_active = True
            if pos.is_long:
                new_trail = pos.highest_since_entry - atr * self._trailing_distance_atr
                if new_trail > pos.stop_loss:
                    pos.stop_loss = new_trail
                    pos.trailing_stop = new_trail
            else:
                new_trail = pos.lowest_since_entry + atr * self._trailing_distance_atr
                if new_trail < pos.stop_loss:
                    pos.stop_loss = new_trail
                    pos.trailing_stop = new_trail

        return None

    # ── Kill switch ───────────────────────────────────────────────────────
    def activate_kill_switch(self) -> list[Position]:
        """Emergency: close all positions and block new signals."""
        self._killed = True
        closed: list[Position] = []
        for key in list(self._positions.keys()):
            pos = self._positions[key]
            closed_pos = self.close_position(pos.exchange, pos.symbol, pos.current_price)
            if closed_pos:
                closed.append(closed_pos)
        logger.critical("KILL SWITCH activated — closed {} positions", len(closed))
        return closed

    def deactivate_kill_switch(self) -> None:
        self._killed = False
        logger.info("Kill switch deactivated")

    @property
    def killed(self) -> bool:
        return self._killed

    def record_return(self, pnl_pct: float) -> None:
        self._return_history.append(float(pnl_pct))
        if len(self._return_history) > self._returns_window:
            self._return_history = self._return_history[-self._returns_window:]

    def calculate_historical_var(self, confidence: float = 0.95) -> float:
        if not self._return_history:
            return 0.0
        arr = np.array(self._return_history, dtype=float)
        q = np.percentile(arr, (1.0 - confidence) * 100.0)
        return float(max(0.0, -q))

    def calculate_cvar(self, confidence: float = 0.95) -> float:
        if not self._return_history:
            return 0.0
        arr = np.array(self._return_history, dtype=float)
        q = np.percentile(arr, (1.0 - confidence) * 100.0)
        tail = arr[arr <= q]
        if tail.size == 0:
            return 0.0
        return float(max(0.0, -tail.mean()))

    def portfolio_notional(self) -> float:
        return float(sum(abs(p.size * p.current_price) for p in self._positions.values()))

    def current_drawdown_pct(self) -> float:
        peak = self._circuit_breaker._peak_equity
        if peak <= 0:
            return 0.0
        return float((peak - self._equity) / peak)

    def run_stress_test(
        self,
        shocks: list[float] | None = None,
        correlation_breakdown_factor: float = 1.5,
    ) -> dict[str, Any]:
        if shocks is None:
            shocks = [-0.05, -0.10, -0.20]

        notional = self.portfolio_notional()
        scenarios: dict[str, Any] = {}
        worst_loss = 0.0

        for shock in shocks:
            loss = abs(notional * shock)
            worst_loss = max(worst_loss, loss)
            scenarios[f"shock_{int(abs(shock) * 100)}pct"] = {
                "shock_pct": float(shock),
                "estimated_loss_usd": float(loss),
                "loss_pct_equity": float(loss / self._equity) if self._equity > 0 else 0.0,
            }

        corr_loss = worst_loss * correlation_breakdown_factor
        scenarios["correlation_breakdown"] = {
            "factor": float(correlation_breakdown_factor),
            "estimated_loss_usd": float(corr_loss),
            "loss_pct_equity": float(corr_loss / self._equity) if self._equity > 0 else 0.0,
        }

        return {
            "portfolio_notional_usd": notional,
            "var_95": self.calculate_historical_var(0.95),
            "var_99": self.calculate_historical_var(0.99),
            "cvar_95": self.calculate_cvar(0.95),
            "cvar_99": self.calculate_cvar(0.99),
            "scenarios": scenarios,
            "max_portfolio_var_pct": self._max_portfolio_var_pct,
        }

    def get_risk_snapshot(self) -> dict[str, Any]:
        return {
            "equity": float(self._equity),
            "open_positions": len(self._positions),
            "portfolio_notional_usd": self.portfolio_notional(),
            "var_95": self.calculate_historical_var(0.95),
            "var_99": self.calculate_historical_var(0.99),
            "cvar_95": self.calculate_cvar(0.95),
            "cvar_99": self.calculate_cvar(0.99),
            "return_history_size": len(self._return_history),
            "var_min_history": self._var_min_history,
            "max_portfolio_var_pct": self._max_portfolio_var_pct,
            "circuit_breaker_tripped": self._circuit_breaker.tripped,
            "circuit_breaker_reason": self._circuit_breaker.trip_reason,
            "kill_switch_active": self._killed,
            "current_drawdown_pct": self.current_drawdown_pct(),
        }

    def update_prices(self, exchange: str, symbol: str, price: float) -> None:
        key = f"{exchange}:{symbol}"
        pos = self._positions.get(key)
        if pos is None:
            return
        pos.update_price(price)

        # Run position management (trailing stop, breakeven, time exit)
        exit_reason = self.manage_position(pos)
        if exit_reason:
            logger.info("{} exit triggered for {}/{}", exit_reason, exchange, symbol)
            self.event_bus.publish_nowait("STOP_LOSS", {
                "exchange": exchange, "symbol": symbol, "price": price, "reason": exit_reason,
            })
            return

        if (pos.is_long and price <= pos.stop_loss) or (not pos.is_long and price >= pos.stop_loss):
            reason = "trailing_stop" if pos.trailing_active else "stop_loss"
            logger.warning("{} triggered for {}/{} at {:.2f}", reason, exchange, symbol, price)
            self.event_bus.publish_nowait("STOP_LOSS", {
                "exchange": exchange, "symbol": symbol, "price": price, "reason": reason,
            })
        elif (pos.is_long and price >= pos.take_profit) or (not pos.is_long and price <= pos.take_profit):
            logger.info("Take profit triggered for {}/{} at {:.2f}", exchange, symbol, price)
            self.event_bus.publish_nowait("TAKE_PROFIT", {
                "exchange": exchange, "symbol": symbol, "price": price, "reason": "take_profit",
            })

    @property
    def positions(self) -> dict[str, Position]:
        return dict(self._positions)

    @property
    def equity(self) -> float:
        return self._equity

    async def _handle_tick(self, payload: Any) -> None:
        tick = payload
        self.update_prices(tick.exchange, tick.symbol, tick.price)

    async def run(self) -> None:
        self._running = True
        self.event_bus.subscribe("TICK", self._handle_tick)
        logger.info("RiskManager started — equity={:.2f}, sizing={}", self._equity, self._sizing_method)
        while self._running:
            await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False
        self.event_bus.unsubscribe("TICK", self._handle_tick)
