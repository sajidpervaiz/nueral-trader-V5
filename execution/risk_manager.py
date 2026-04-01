from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

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

    @property
    def is_long(self) -> bool:
        return self.direction == "long"

    def update_price(self, price: float) -> None:
        self.current_price = price
        if self.is_long:
            self.pnl_pct = (price - self.entry_price) / self.entry_price
        else:
            self.pnl_pct = (self.entry_price - price) / self.entry_price
        self.pnl = self.pnl_pct * self.size * self.entry_price


class CircuitBreaker:
    def __init__(self, max_daily_loss_pct: float, max_drawdown_pct: float) -> None:
        self._max_daily_loss = max_daily_loss_pct
        self._max_drawdown = max_drawdown_pct
        self._daily_loss = 0.0
        self._peak_equity = 0.0
        self._tripped = False
        self._trip_reason = ""

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
        if drawdown >= self._max_drawdown:
            self._tripped = True
            self._trip_reason = f"drawdown={drawdown:.2%}"
        return self._tripped

    @property
    def tripped(self) -> bool:
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
        self._max_position_pct = float(risk_cfg.get("max_position_size_pct", 0.02))
        self._max_open = int(risk_cfg.get("max_open_positions", 5))
        self._leverage = float(risk_cfg.get("default_leverage", 1.0))
        self._positions: dict[str, Position] = {}
        self._equity = 100_000.0
        self._circuit_breaker = CircuitBreaker(
            max_daily_loss_pct=float(risk_cfg.get("max_daily_loss_pct", 0.03)),
            max_drawdown_pct=float(risk_cfg.get("max_drawdown_pct", 0.10)),
        )
        self._running = False

    def approve_signal(self, signal: TradingSignal) -> tuple[bool, str, float]:
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

        size = self._equity * self._max_position_pct * self._leverage
        return True, "approved", size

    def open_position(self, signal: TradingSignal, size: float) -> Position:
        pos = Position(
            exchange=signal.exchange,
            symbol=signal.symbol,
            direction=signal.direction,
            size=size / signal.price if signal.price > 0 else 0.0,
            entry_price=signal.price,
            current_price=signal.price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            open_time=signal.timestamp,
        )
        key = f"{signal.exchange}:{signal.symbol}"
        self._positions[key] = pos
        logger.info(
            "Position opened: {}/{} {} size={:.4f} @ {:.2f}",
            signal.exchange, signal.symbol, signal.direction, pos.size, pos.entry_price
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
        self._circuit_breaker.record_pnl(pos.pnl_pct, self._equity)
        logger.info(
            "Position closed: {}/{} pnl={:.2f} ({:.2%}) equity={:.2f}",
            exchange, symbol, pnl_dollar, pos.pnl_pct, self._equity
        )
        return pos

    def update_prices(self, exchange: str, symbol: str, price: float) -> None:
        key = f"{exchange}:{symbol}"
        pos = self._positions.get(key)
        if pos is None:
            return
        pos.update_price(price)
        if (pos.is_long and price <= pos.stop_loss) or (not pos.is_long and price >= pos.stop_loss):
            logger.warning("Stop loss triggered for {}/{} at {:.2f}", exchange, symbol, price)
            asyncio.get_event_loop().call_soon_threadsafe(
                self.event_bus.publish_nowait, "STOP_LOSS", {"exchange": exchange, "symbol": symbol, "price": price}
            )
        elif (pos.is_long and price >= pos.take_profit) or (not pos.is_long and price <= pos.take_profit):
            logger.info("Take profit triggered for {}/{} at {:.2f}", exchange, symbol, price)
            asyncio.get_event_loop().call_soon_threadsafe(
                self.event_bus.publish_nowait, "TAKE_PROFIT", {"exchange": exchange, "symbol": symbol, "price": price}
            )

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
        logger.info("RiskManager started — equity={:.2f}", self._equity)
        while self._running:
            await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False
        self.event_bus.unsubscribe("TICK", self._handle_tick)
