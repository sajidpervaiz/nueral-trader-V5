from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

try:
    from prometheus_client import (
        Counter,
        Gauge,
        Histogram,
        Summary,
        start_http_server,
        REGISTRY,
    )
    _PROMETHEUS = True
except ImportError:
    _PROMETHEUS = False

from core.config import Config
from core.event_bus import EventBus


def _noop(*a: Any, **kw: Any) -> Any:
    class _Fake:
        def labels(self, **kw: Any) -> "_Fake":
            return self
        def inc(self, *a: Any) -> None: pass
        def dec(self, *a: Any) -> None: pass
        def set(self, *a: Any) -> None: pass
        def observe(self, *a: Any) -> None: pass
    return _Fake()


class Metrics:
    def __init__(self, config: Config, event_bus: EventBus) -> None:
        self.config = config
        self.event_bus = event_bus
        self._running = False

        if _PROMETHEUS:
            self.ticks_total = Counter(
                "neuraltrader_ticks_total",
                "Total number of ticks received",
                ["exchange", "symbol"],
            )
            self.signals_total = Counter(
                "neuraltrader_signals_total",
                "Total number of trading signals generated",
                ["exchange", "symbol", "direction"],
            )
            self.orders_total = Counter(
                "neuraltrader_orders_total",
                "Total number of orders submitted",
                ["exchange", "symbol", "is_paper"],
            )
            self.open_positions = Gauge(
                "neuraltrader_open_positions",
                "Number of currently open positions",
                ["exchange"],
            )
            self.equity = Gauge(
                "neuraltrader_equity_usd",
                "Current account equity in USD",
            )
            self.daily_pnl = Gauge(
                "neuraltrader_daily_pnl_usd",
                "Daily profit/loss in USD",
            )
            self.funding_rate = Gauge(
                "neuraltrader_funding_rate",
                "Latest funding rate",
                ["exchange", "symbol"],
            )
            self.open_interest_usd = Gauge(
                "neuraltrader_open_interest_usd",
                "Open interest in USD",
                ["exchange", "symbol"],
            )
            self.vix_proxy = Gauge(
                "neuraltrader_vix_proxy",
                "VIX proxy annualized volatility",
                ["symbol", "lookback_days"],
            )
            self.rust_tick_latency = Histogram(
                "neuraltrader_rust_tick_latency_seconds",
                "Rust tick parsing latency",
                buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1],
            )
            self.dex_quote_latency = Histogram(
                "neuraltrader_dex_quote_latency_seconds",
                "DEX quote generation latency",
                buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
            )
            self.service_health = Gauge(
                "neuraltrader_service_health",
                "Health of external services (1=up, 0=down)",
                ["service"],
            )
            self.alerts_total = Counter(
                "neuraltrader_alerts_total",
                "Total alerts fired by type and severity",
                ["alert_type", "severity"],
            )
            self.alerts_suppressed_total = Counter(
                "neuraltrader_alerts_suppressed_total",
                "Total alerts suppressed by throttling",
            )
            self.circuit_breaker_trips_total = Counter(
                "neuraltrader_circuit_breaker_trips_total",
                "Total circuit breaker trips",
                ["reason"],
            )
            self.safe_mode_active = Gauge(
                "neuraltrader_safe_mode_active",
                "Whether safe mode is currently active (1=yes, 0=no)",
            )
            self.kill_switch_active = Gauge(
                "neuraltrader_kill_switch_active",
                "Whether kill switch is active (1=yes, 0=no)",
            )
            self.stop_loss_hits_total = Counter(
                "neuraltrader_stop_loss_hits_total",
                "Total stop loss events",
                ["exchange", "symbol"],
            )
            self.take_profit_hits_total = Counter(
                "neuraltrader_take_profit_hits_total",
                "Total take profit events",
                ["exchange", "symbol"],
            )
        else:
            for name in [
                "ticks_total", "signals_total", "orders_total",
                "open_positions", "equity", "daily_pnl",
                "funding_rate", "open_interest_usd", "vix_proxy",
                "rust_tick_latency", "dex_quote_latency", "service_health",
                "alerts_total", "alerts_suppressed_total",
                "circuit_breaker_trips_total", "safe_mode_active",
                "kill_switch_active", "stop_loss_hits_total",
                "take_profit_hits_total",
            ]:
                setattr(self, name, _noop())

    async def _handle_tick(self, payload: Any) -> None:
        tick = payload
        if hasattr(tick, "exchange") and hasattr(tick, "symbol"):
            self.ticks_total.labels(exchange=tick.exchange, symbol=tick.symbol).inc()

    async def _handle_signal(self, payload: Any) -> None:
        signal = payload
        if hasattr(signal, "exchange"):
            self.signals_total.labels(
                exchange=signal.exchange,
                symbol=signal.symbol,
                direction=signal.direction,
            ).inc()

    async def _handle_order(self, payload: Any) -> None:
        order = payload
        if hasattr(order, "exchange"):
            self.orders_total.labels(
                exchange=order.exchange,
                symbol=order.symbol,
                is_paper=str(order.is_paper),
            ).inc()

    async def _handle_funding(self, payload: Any) -> None:
        if isinstance(payload, list):
            for rate in payload:
                self.funding_rate.labels(exchange=rate.exchange, symbol=rate.symbol).set(rate.rate)

    async def _handle_oi(self, payload: Any) -> None:
        if isinstance(payload, list):
            for oi in payload:
                self.open_interest_usd.labels(exchange=oi.exchange, symbol=oi.symbol).set(oi.oi_usd)

    async def _handle_vix(self, payload: Any) -> None:
        if isinstance(payload, list):
            for v in payload:
                self.vix_proxy.labels(symbol=v.symbol, lookback_days=v.lookback_days).set(v.annualized)

    async def _handle_stop_loss(self, payload: Any) -> None:
        data = payload if isinstance(payload, dict) else {}
        exchange = data.get("exchange", "unknown")
        symbol = data.get("symbol", "unknown")
        self.stop_loss_hits_total.labels(exchange=exchange, symbol=symbol).inc()

    async def _handle_take_profit(self, payload: Any) -> None:
        data = payload if isinstance(payload, dict) else {}
        exchange = data.get("exchange", "unknown")
        symbol = data.get("symbol", "unknown")
        self.take_profit_hits_total.labels(exchange=exchange, symbol=symbol).inc()

    async def _handle_kill_switch(self, payload: Any) -> None:
        self.kill_switch_active.set(1)

    async def run(self) -> None:
        self._running = True
        mon_cfg = self.config.get_value("monitoring", "prometheus") or {}
        if mon_cfg.get("enabled", True) and _PROMETHEUS:
            port = int(mon_cfg.get("port", 9090))
            try:
                start_http_server(port)
                logger.info("Prometheus metrics server started on :{}", port)
            except Exception as exc:
                logger.warning("Prometheus server failed: {}", exc)

        self.event_bus.subscribe("TICK", self._handle_tick)
        self.event_bus.subscribe("SIGNAL", self._handle_signal)
        self.event_bus.subscribe("ORDER_FILLED", self._handle_order)
        self.event_bus.subscribe("FUNDING_RATE", self._handle_funding)
        self.event_bus.subscribe("OPEN_INTEREST", self._handle_oi)
        self.event_bus.subscribe("VOLATILITY_INDEX", self._handle_vix)
        self.event_bus.subscribe("STOP_LOSS", self._handle_stop_loss)
        self.event_bus.subscribe("TAKE_PROFIT", self._handle_take_profit)
        self.event_bus.subscribe("KILL_SWITCH", self._handle_kill_switch)

        while self._running:
            await asyncio.sleep(15)

    async def stop(self) -> None:
        self._running = False
        self.event_bus.unsubscribe("TICK", self._handle_tick)
        self.event_bus.unsubscribe("SIGNAL", self._handle_signal)
        self.event_bus.unsubscribe("ORDER_FILLED", self._handle_order)
        self.event_bus.unsubscribe("FUNDING_RATE", self._handle_funding)
        self.event_bus.unsubscribe("OPEN_INTEREST", self._handle_oi)
        self.event_bus.unsubscribe("VOLATILITY_INDEX", self._handle_vix)
        self.event_bus.unsubscribe("STOP_LOSS", self._handle_stop_loss)
        self.event_bus.unsubscribe("TAKE_PROFIT", self._handle_take_profit)
        self.event_bus.unsubscribe("KILL_SWITCH", self._handle_kill_switch)
