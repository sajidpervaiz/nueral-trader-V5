"""
Startup reconciliation — ensures bot state matches exchange reality before trading.

On boot:
1. Fetch open positions from Binance Futures
2. Fetch open orders per symbol
3. Fetch account balance / margin
4. Compare with local DB state
5. Rebuild in-memory state from exchange truth
6. If mismatch → SAFE MODE (no new entries, manage existing positions)
7. Ensure every open position has SL/TP; if missing, place them immediately
"""
from __future__ import annotations

import time
from typing import Any

from loguru import logger

from core.config import Config
from core.event_bus import EventBus
from execution.risk_manager import RiskManager, Position
from execution.exchange_order_placer import ExchangeOrderPlacer


class ReconciliationResult:
    """Result of startup reconciliation."""

    def __init__(self) -> None:
        self.success = False
        self.safe_mode = False
        self.exchange_positions: list[dict] = []
        self.exchange_open_orders: list[dict] = []
        self.balance: float = 0.0
        self.available_balance: float = 0.0
        self.mismatches: list[str] = []
        self.positions_without_sl: list[str] = []
        self.actions_taken: list[str] = []

    def __repr__(self) -> str:
        return (
            f"ReconciliationResult(success={self.success}, safe_mode={self.safe_mode}, "
            f"positions={len(self.exchange_positions)}, mismatches={len(self.mismatches)})"
        )


class StartupReconciler:
    """
    Reconciles bot state with exchange reality on startup.

    If mismatches are detected, the bot enters SAFE MODE:
    - No new entries allowed
    - Existing positions continue to be managed (SL/TP active)
    """

    def __init__(
        self,
        config: Config,
        event_bus: EventBus,
        risk_manager: RiskManager,
        client: Any,
        order_placer: ExchangeOrderPlacer | None = None,
    ) -> None:
        self.config = config
        self.event_bus = event_bus
        self.risk_manager = risk_manager
        self._client = client
        self._order_placer = order_placer
        self._safe_mode = False

    @property
    def safe_mode(self) -> bool:
        return self._safe_mode

    async def reconcile(self) -> ReconciliationResult:
        """
        Run full startup reconciliation.

        Returns ReconciliationResult with details of what was found and done.
        """
        result = ReconciliationResult()

        if self._client is None:
            logger.warning("No exchange client — skipping reconciliation")
            result.success = True
            return result

        logger.info("=" * 50)
        logger.info("STARTUP RECONCILIATION BEGIN")
        logger.info("=" * 50)

        try:
            # 1. Fetch account balance
            await self._fetch_balance(result)

            # 2. Fetch open positions from exchange
            await self._fetch_positions(result)

            # 3. Fetch open orders
            await self._fetch_open_orders(result)

            # 4. Rebuild internal state from exchange truth
            await self._rebuild_state(result)

            # 5. Ensure every position has protective orders
            await self._ensure_protective_orders(result)

            # 6. Determine if safe mode needed
            if result.mismatches:
                self._safe_mode = True
                result.safe_mode = True
                # Trip circuit breaker to prevent new entries
                self.risk_manager._circuit_breaker.trip(
                    f"reconciliation_mismatch ({len(result.mismatches)} issues)",
                    pause_seconds=86400.0,
                )
                logger.critical(
                    "SAFE MODE ACTIVATED — {} mismatches detected. No new entries allowed.",
                    len(result.mismatches),
                )
                for m in result.mismatches:
                    logger.critical("  Mismatch: {}", m)
            else:
                logger.info("Reconciliation clean — no mismatches")

            result.success = True
            logger.info("STARTUP RECONCILIATION COMPLETE (safe_mode={})", result.safe_mode)
            logger.info("=" * 50)

        except Exception as exc:
            logger.critical("Reconciliation FAILED: {} — entering safe mode", exc)
            result.success = False
            result.safe_mode = True
            self._safe_mode = True
            # Trip circuit breaker
            self.risk_manager._circuit_breaker.trip(
                f"reconciliation_failed: {exc}",
                pause_seconds=86400.0,
            )

        return result

    async def _fetch_balance(self, result: ReconciliationResult) -> None:
        """Fetch account balance from exchange."""
        try:
            balance = await self._client.fetch_balance()
            total = float(balance.get("total", {}).get("USDT", 0))
            free = float(balance.get("free", {}).get("USDT", 0))
            result.balance = total
            result.available_balance = free
            logger.info(
                "Exchange balance: total={:.2f} USDT, available={:.2f} USDT",
                total, free,
            )
            # Update risk manager equity to real exchange value
            if total > 0:
                old_equity = self.risk_manager._equity
                self.risk_manager._equity = total
                if abs(old_equity - total) / max(old_equity, 1) > 0.1:
                    result.mismatches.append(
                        f"equity_mismatch: config={old_equity:.2f}, exchange={total:.2f}"
                    )
        except Exception as exc:
            logger.error("Failed to fetch balance: {}", exc)
            result.mismatches.append(f"balance_fetch_failed: {exc}")

    async def _fetch_positions(self, result: ReconciliationResult) -> None:
        """Fetch open positions from Binance Futures."""
        try:
            # ccxt fetch_positions returns all (including zero-size)
            positions = await self._client.fetch_positions()
            for pos in positions:
                size = abs(float(pos.get("contracts", 0) or 0))
                if size > 0:
                    result.exchange_positions.append({
                        "symbol": pos.get("symbol", ""),
                        "side": pos.get("side", ""),
                        "size": size,
                        "notional": abs(float(pos.get("notional", 0) or 0)),
                        "entry_price": float(pos.get("entryPrice", 0) or 0),
                        "unrealized_pnl": float(pos.get("unrealizedPnl", 0) or 0),
                        "leverage": float(pos.get("leverage", 1) or 1),
                        "margin_type": pos.get("marginType", ""),
                        "liquidation_price": float(pos.get("liquidationPrice", 0) or 0),
                    })

            logger.info(
                "Exchange has {} open position(s)",
                len(result.exchange_positions),
            )
            for ep in result.exchange_positions:
                logger.info(
                    "  {} {} size={:.6f} entry={:.2f} pnl={:.4f}",
                    ep["symbol"], ep["side"], ep["size"], ep["entry_price"], ep["unrealized_pnl"],
                )
        except Exception as exc:
            logger.error("Failed to fetch positions: {}", exc)
            result.mismatches.append(f"positions_fetch_failed: {exc}")

    async def _fetch_open_orders(self, result: ReconciliationResult) -> None:
        """Fetch all open orders from exchange."""
        try:
            orders = await self._client.fetch_open_orders()
            result.exchange_open_orders = [
                {
                    "id": o.get("id", ""),
                    "symbol": o.get("symbol", ""),
                    "type": o.get("type", ""),
                    "side": o.get("side", ""),
                    "amount": float(o.get("amount", 0)),
                    "price": float(o.get("price", 0) or 0),
                    "stop_price": float(o.get("stopPrice", 0) or 0),
                    "status": o.get("status", ""),
                    "reduce_only": o.get("reduceOnly", False),
                }
                for o in orders
            ]
            logger.info("Exchange has {} open order(s)", len(result.exchange_open_orders))
        except Exception as exc:
            logger.error("Failed to fetch open orders: {}", exc)
            result.mismatches.append(f"orders_fetch_failed: {exc}")

    async def _rebuild_state(self, result: ReconciliationResult) -> None:
        """Rebuild internal position state from exchange truth."""
        # Clear stale in-memory positions
        old_positions = dict(self.risk_manager._positions)
        self.risk_manager._positions.clear()

        for ep in result.exchange_positions:
            symbol = ep["symbol"]
            side = ep["side"]  # "long" or "short"
            direction = side if side in ("long", "short") else ("long" if side == "buy" else "short")
            key = f"binance:{symbol}"

            pos = Position(
                exchange="binance",
                symbol=symbol,
                direction=direction,
                size=ep["size"],
                entry_price=ep["entry_price"],
                current_price=ep["entry_price"],  # Will update on next tick
                stop_loss=0.0,  # Will be set from existing orders or computed
                take_profit=0.0,
                open_time=int(time.time()),
                highest_since_entry=ep["entry_price"],
                lowest_since_entry=ep["entry_price"],
            )
            self.risk_manager._positions[key] = pos

            # Check if this was in old state
            if key in old_positions:
                old = old_positions.pop(key)
                # Preserve SL/TP from old state
                pos.stop_loss = old.stop_loss
                pos.take_profit = old.take_profit
                pos.open_time = old.open_time
                pos.trailing_active = old.trailing_active
                pos.breakeven_moved = old.breakeven_moved
                result.actions_taken.append(f"restored {symbol} from previous state")
            else:
                result.mismatches.append(
                    f"unknown_position: {symbol} {direction} size={ep['size']:.6f} "
                    f"(exists on exchange but not in local state)"
                )

        # Check for positions in old state that don't exist on exchange
        for key, old_pos in old_positions.items():
            result.mismatches.append(
                f"stale_position: {key} {old_pos.direction} size={old_pos.size:.6f} "
                f"(in local state but not on exchange)"
            )

        logger.info(
            "State rebuilt: {} positions from exchange, {} stale removed",
            len(result.exchange_positions), len(old_positions),
        )

    async def _ensure_protective_orders(self, result: ReconciliationResult) -> None:
        """Ensure every open position has SL/TP orders on exchange."""
        for ep in result.exchange_positions:
            symbol = ep["symbol"]
            has_sl = False
            has_tp = False

            # Check existing open orders for this symbol and read prices back
            key = f"binance:{symbol}"
            pos = self.risk_manager._positions.get(key)
            sl_order_id: str | None = None
            tp_order_id: str | None = None
            sl_price_found: float = 0.0
            tp_price_found: float = 0.0
            for order in result.exchange_open_orders:
                if order["symbol"] != symbol or not order.get("reduce_only", False):
                    continue
                if order["type"] in ("stop_market", "stop", "STOP_MARKET", "STOP"):
                    has_sl = True
                    sl_order_id = str(order.get("id", ""))
                    # Read SL price from exchange into in-memory position
                    sl_price_found = float(order.get("stop_price", 0) or 0)
                    if pos and sl_price_found > 0:
                        pos.stop_loss = sl_price_found
                        logger.info("Read SL price from exchange for {}: {:.2f}", symbol, sl_price_found)
                if order["type"] in ("take_profit_market", "take_profit", "TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                    has_tp = True
                    tp_order_id = str(order.get("id", ""))
                    # Read TP price from exchange into in-memory position
                    tp_price_found = float(order.get("stop_price", 0) or 0)
                    if pos and tp_price_found > 0:
                        pos.take_profit = tp_price_found
                        logger.info("Read TP price from exchange for {}: {:.2f}", symbol, tp_price_found)

            # Register existing exchange SL/TP in order_placer so OCO cancel logic works
            if (has_sl or has_tp) and pos and self._order_placer:
                from execution.exchange_order_placer import ProtectiveOrders
                prot = ProtectiveOrders(
                    symbol=symbol,
                    direction=pos.direction,
                    quantity=ep["size"],
                    entry_price=ep["entry_price"],
                    sl_price=sl_price_found,
                    tp_price=tp_price_found,
                    sl_order_id=sl_order_id,
                    tp_order_id=tp_order_id,
                    sl_placed=has_sl,
                    tp_placed=has_tp,
                )
                self._order_placer._protective[symbol] = prot
                logger.info(
                    "Registered existing protective orders for {} (SL={}, TP={})",
                    symbol, sl_order_id, tp_order_id,
                )

            if not has_sl:
                result.positions_without_sl.append(symbol)
                logger.critical("Position {} has NO exchange-side SL — placing now", symbol)

                # Compute SL/TP from risk config
                key = f"binance:{symbol}"
                pos = self.risk_manager._positions.get(key)
                if pos and pos.stop_loss == 0.0:
                    # Fallback: use 1.5% from entry
                    sl_pct = float((self.risk_manager.config.get_value("risk") or {}).get("stop_loss_pct", 0.015))
                    if pos.is_long:
                        pos.stop_loss = ep["entry_price"] * (1 - sl_pct)
                        pos.take_profit = ep["entry_price"] * (1 + sl_pct * self.risk_manager._rr_ratio)
                    else:
                        pos.stop_loss = ep["entry_price"] * (1 + sl_pct)
                        pos.take_profit = ep["entry_price"] * (1 - sl_pct * self.risk_manager._rr_ratio)

                if pos and self._order_placer:
                    try:
                        await self._order_placer.place_protective_orders(
                            symbol=symbol,
                            direction=pos.direction,
                            quantity=ep["size"],
                            entry_price=ep["entry_price"],
                            sl_price=pos.stop_loss,
                            tp_price=pos.take_profit,
                        )
                        result.actions_taken.append(f"placed SL/TP for {symbol}")
                    except Exception as exc:
                        logger.critical("FAILED to place SL for {}: {}", symbol, exc)
                        result.mismatches.append(f"sl_placement_failed: {symbol}: {exc}")
                elif pos and not self._order_placer:
                    logger.warning("No order_placer — cannot place SL/TP for {}", symbol)
            elif not has_tp:
                logger.warning("Position {} has SL but no TP — acceptable", symbol)
