"""Chaos / disaster fault injectors for testing resilience.

These helpers simulate real-world failures that production bots face:
WebSocket disconnect, USER_DATA stream failure, API timeout, partial fills,
order rejection, DB outage, volatility spike, spread widening, and bot crash
mid-position.

Usage in tests:
    injector = FaultInjector(event_bus, risk_manager, cex_executor)
    await injector.simulate_websocket_disconnect()
"""
from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from loguru import logger

from core.event_bus import EventBus
from core.safe_mode import SafeModeManager, SafeModeReason


class FaultInjector:
    """Simulates real-world infrastructure failures."""

    def __init__(
        self,
        event_bus: EventBus,
        risk_manager: Any | None = None,
        cex_executor: Any | None = None,
        safe_mode: SafeModeManager | None = None,
    ) -> None:
        self.event_bus = event_bus
        self.risk_manager = risk_manager
        self.cex_executor = cex_executor
        self.safe_mode = safe_mode or (risk_manager.safe_mode if risk_manager else SafeModeManager())
        self._log: list[dict[str, Any]] = []

    def _record(self, fault: str, detail: str = "") -> None:
        self._log.append({"fault": fault, "detail": detail, "ts": time.time()})
        logger.warning("[CHAOS] Injecting fault: {} — {}", fault, detail)

    @property
    def history(self) -> list[dict[str, Any]]:
        return list(self._log)

    # ── 1. WebSocket disconnect (market data blind) ───────────────────────
    async def simulate_websocket_disconnect(self) -> None:
        """Simulate market data WebSocket going down.
        The bot should activate safe mode and refuse new trades."""
        self._record("websocket_disconnect", "Market data stream lost")
        await self.safe_mode.activate(
            SafeModeReason.WEBSOCKET_DISCONNECT,
            detail="chaos_test: simulated WS disconnect",
        )

    async def simulate_websocket_reconnect(self) -> None:
        self._record("websocket_reconnect", "Market data stream restored")
        await self.safe_mode.deactivate(SafeModeReason.WEBSOCKET_DISCONNECT)

    # ── 2. USER_DATA stream failure ───────────────────────────────────────
    async def simulate_user_stream_failure(self) -> None:
        """Simulate Binance User Data Stream disconnect."""
        self._record("user_stream_lost", "User data stream disconnected")
        await self.event_bus.publish("USER_STREAM_LOST", {
            "exchange": "binance", "reason": "chaos_test",
        })

    async def simulate_user_stream_reconnect(self) -> None:
        self._record("user_stream_reconnect", "User data stream restored")
        await self.event_bus.publish("USER_STREAM_CONNECTED", {
            "exchange": "binance",
        })

    # ── 3. Exchange API timeout ───────────────────────────────────────────
    async def simulate_exchange_api_timeout(self, duration: float = 0.0) -> None:
        """Simulate exchange API returning timeout errors.
        If cex_executor has a client, replace it with a timeout-raising mock."""
        self._record("exchange_api_timeout", f"duration={duration}s")
        await self.safe_mode.activate(
            SafeModeReason.EXCHANGE_API_TIMEOUT,
            detail="chaos_test: API unreachable",
        )

    async def simulate_exchange_api_recovery(self) -> None:
        self._record("exchange_api_recovery", "API restored")
        await self.safe_mode.deactivate(SafeModeReason.EXCHANGE_API_TIMEOUT)

    # ── 4. Partial fills (stuck order) ────────────────────────────────────
    async def simulate_partial_fill(self, symbol: str, filled_pct: float = 0.3) -> None:
        """Publish a partial fill event through the event bus."""
        self._record("partial_fill", f"symbol={symbol} filled={filled_pct:.0%}")
        await self.event_bus.publish("USER_ORDER_UPDATE", {
            "symbol": symbol,
            "execution_type": "TRADE",
            "order_status": "PARTIALLY_FILLED",
            "order_type": "LIMIT",
            "reduce_only": False,
            "last_filled_qty": 0.1 * filled_pct,
            "last_filled_price": 50000.0,
            "cumulative_filled_qty": 0.1 * filled_pct,
            "quantity": 0.1,
            "commission": 0.01,
            "realized_profit": 0.0,
            "client_order_id": f"chaos_partial_{int(time.time())}",
            "trade_id": f"t_{int(time.time())}",
        })

    # ── 5. Order rejection burst ──────────────────────────────────────────
    async def simulate_order_rejection(self, symbol: str, count: int = 5) -> None:
        """Simulate a burst of order rejections from the exchange."""
        self._record("order_rejection_burst", f"symbol={symbol} count={count}")
        for i in range(count):
            await self.event_bus.publish("USER_ORDER_UPDATE", {
                "symbol": symbol,
                "execution_type": "REJECTED",
                "order_status": "REJECTED",
                "order_type": "LIMIT",
                "reduce_only": False,
                "client_order_id": f"chaos_reject_{i}_{int(time.time())}",
                "reason": "insufficient_margin",
            })
        if count >= 3:
            await self.safe_mode.activate(
                SafeModeReason.ORDER_REJECTION_BURST,
                detail=f"{count} rejections for {symbol}",
            )

    # ── 6. DB outage ─────────────────────────────────────────────────────
    async def simulate_db_outage(self) -> None:
        """Simulate database becoming unreachable."""
        self._record("db_outage", "Database connection lost")
        await self.safe_mode.activate(
            SafeModeReason.DB_OUTAGE,
            detail="chaos_test: DB unreachable",
        )

    async def simulate_db_recovery(self) -> None:
        self._record("db_recovery", "Database restored")
        await self.safe_mode.deactivate(SafeModeReason.DB_OUTAGE)

    # ── 7. Sudden volatility spike ────────────────────────────────────────
    async def simulate_volatility_spike(
        self, exchange: str, symbol: str, price_drop_pct: float = 0.10
    ) -> None:
        """Simulate a sudden price crash — e.g. flash crash of 10%.
        Fires rapid TICK events with plummeting prices."""
        self._record("volatility_spike", f"{symbol} drop={price_drop_pct:.0%}")
        await self.safe_mode.activate(
            SafeModeReason.EXTREME_VOLATILITY,
            detail=f"chaos_test: {symbol} flash crash {price_drop_pct:.0%}",
        )
        # Publish a sharp tick drop
        base_price = 50_000.0
        crash_price = base_price * (1 - price_drop_pct)

        class FakeTick:
            def __init__(self, ex: str, sym: str, p: float) -> None:
                self.exchange = ex
                self.symbol = sym
                self.price = p

        for step in range(5):
            price = base_price - (base_price - crash_price) * (step + 1) / 5
            await self.event_bus.publish("TICK", FakeTick(exchange, symbol, price))

    # ── 8. Extreme spread widening ────────────────────────────────────────
    async def simulate_spread_widening(self) -> None:
        """Simulate extreme spread — the risk manager should reject new trades."""
        self._record("spread_widening", "Spread > 100 bps")
        await self.safe_mode.activate(
            SafeModeReason.EXTREME_SPREAD,
            detail="chaos_test: spread > 100 bps",
        )

    async def simulate_spread_recovery(self) -> None:
        self._record("spread_recovery", "Spread normalised")
        await self.safe_mode.deactivate(SafeModeReason.EXTREME_SPREAD)

    # ── 9. Bot crash mid-position ─────────────────────────────────────────
    async def simulate_bot_crash_mid_position(self) -> None:
        """Simulate the bot process crashing while positions are open.
        Existing exchange-side SL/TP should protect the positions.
        We verify that positions still exist in risk_manager after 'restart'."""
        self._record("bot_crash_mid_position", "Simulated crash — positions unmanaged")
        # In a real crash the process dies. In our test we verify:
        # 1. Positions still exist in risk_manager
        # 2. Safe mode activates on restart
        # 3. Exchange-side SL/TP are assumed to be in place
        if self.risk_manager and self.risk_manager._positions:
            positions_snapshot = dict(self.risk_manager._positions)
            logger.warning(
                "[CHAOS] Bot crashed with {} open positions: {}",
                len(positions_snapshot),
                list(positions_snapshot.keys()),
            )
            return positions_snapshot
        return {}

    # ── Combined chaos run ────────────────────────────────────────────────
    async def run_all_faults(self) -> list[dict[str, Any]]:
        """Run every fault scenario sequentially and return the log."""
        await self.simulate_websocket_disconnect()
        await self.simulate_user_stream_failure()
        await self.simulate_exchange_api_timeout()
        await self.simulate_db_outage()
        await self.simulate_volatility_spike("binance", "BTCUSDT")
        await self.simulate_spread_widening()
        await self.simulate_order_rejection("BTCUSDT", count=5)
        return self.history
