"""
Exchange-side SL/TP order management for Binance Futures.

Places STOP_MARKET and TAKE_PROFIT_MARKET orders on the exchange immediately
after an entry fill, so positions are ALWAYS protected even if the bot crashes.

Implements manual OCO: when SL fills → cancel TP, when TP fills → cancel SL.
Handles partial fills by adjusting protective order quantities.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from loguru import logger


@dataclass
class ProtectiveOrders:
    """Tracks exchange-side SL and TP for a position."""
    symbol: str
    direction: str  # "long" or "short"
    quantity: float
    entry_price: float
    sl_price: float
    tp_price: float
    sl_order_id: str | None = None
    tp_order_id: str | None = None
    sl_placed: bool = False
    tp_placed: bool = False
    sl_filled: bool = False
    tp_filled: bool = False
    created_at: float = field(default_factory=time.time)

    @property
    def is_long(self) -> bool:
        return self.direction == "long"

    @property
    def close_side(self) -> str:
        return "SELL" if self.is_long else "BUY"


class ExchangeOrderPlacer:
    """
    Places and manages exchange-side protective orders (SL/TP) on Binance Futures.

    Key guarantees:
    - SL is ALWAYS placed even if TP fails
    - Partial fills adjust SL/TP quantity to match filled qty
    - OCO-like: SL fill cancels TP, TP fill cancels SL
    - workingType is configurable (MARK_PRICE or CONTRACT_PRICE)
    """

    def __init__(
        self,
        client: Any,
        working_type: str = "CONTRACT_PRICE",
    ) -> None:
        self._client = client
        self._working_type = working_type
        # symbol → ProtectiveOrders
        self._protective: dict[str, ProtectiveOrders] = {}
        self._lock = asyncio.Lock()

    @property
    def protective_orders(self) -> dict[str, ProtectiveOrders]:
        return dict(self._protective)

    async def place_protective_orders(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        entry_price: float,
        sl_price: float,
        tp_price: float,
    ) -> ProtectiveOrders:
        """
        Place exchange-side STOP_MARKET and TAKE_PROFIT_MARKET immediately
        after entry fill. SL is always placed first; TP failure does not
        prevent SL from existing.
        """
        async with self._lock:
            prot = ProtectiveOrders(
                symbol=symbol,
                direction=direction,
                quantity=quantity,
                entry_price=entry_price,
                sl_price=sl_price,
                tp_price=tp_price,
            )

            # ── Place stop-loss FIRST (most critical) ─────────────────────
            try:
                sl_order = await self._place_stop_market(
                    symbol=symbol,
                    side=prot.close_side,
                    quantity=quantity,
                    stop_price=sl_price,
                )
                prot.sl_order_id = sl_order.get("id", "")
                prot.sl_placed = True
                logger.info(
                    "SL placed: {} {} qty={:.6f} stop={:.2f} orderId={}",
                    symbol, prot.close_side, quantity, sl_price, prot.sl_order_id,
                )
            except Exception as exc:
                logger.critical(
                    "FAILED to place SL for {} — POSITION UNPROTECTED: {}",
                    symbol, exc,
                )
                # Store anyway so reconciliation can retry
                self._protective[symbol] = prot
                raise

            # ── Place take-profit (less critical, but important) ──────────
            try:
                tp_order = await self._place_take_profit_market(
                    symbol=symbol,
                    side=prot.close_side,
                    quantity=quantity,
                    stop_price=tp_price,
                )
                prot.tp_order_id = tp_order.get("id", "")
                prot.tp_placed = True
                logger.info(
                    "TP placed: {} {} qty={:.6f} stop={:.2f} orderId={}",
                    symbol, prot.close_side, quantity, tp_price, prot.tp_order_id,
                )
            except Exception as exc:
                logger.error(
                    "FAILED to place TP for {} (SL is active): {}", symbol, exc,
                )

            self._protective[symbol] = prot
            return prot

    async def adjust_quantity(self, symbol: str, new_quantity: float) -> None:
        """Adjust SL/TP quantities for partial fills."""
        async with self._lock:
            prot = self._protective.get(symbol)
            if prot is None:
                return

            old_qty = prot.quantity
            prot.quantity = new_quantity
            logger.info(
                "Adjusting protective orders for {}: qty {:.6f} → {:.6f}",
                symbol, old_qty, new_quantity,
            )

            # Cancel and replace SL with new qty
            if prot.sl_order_id and prot.sl_placed and not prot.sl_filled:
                try:
                    await self._client.cancel_order(prot.sl_order_id, symbol)
                    sl_order = await self._place_stop_market(
                        symbol=symbol,
                        side=prot.close_side,
                        quantity=new_quantity,
                        stop_price=prot.sl_price,
                    )
                    prot.sl_order_id = sl_order.get("id", "")
                    logger.info("SL adjusted for {} new_qty={:.6f}", symbol, new_quantity)
                except Exception as exc:
                    logger.error("Failed to adjust SL for {}: {}", symbol, exc)

            # Cancel and replace TP with new qty
            if prot.tp_order_id and prot.tp_placed and not prot.tp_filled:
                try:
                    await self._client.cancel_order(prot.tp_order_id, symbol)
                    tp_order = await self._place_take_profit_market(
                        symbol=symbol,
                        side=prot.close_side,
                        quantity=new_quantity,
                        stop_price=prot.tp_price,
                    )
                    prot.tp_order_id = tp_order.get("id", "")
                    logger.info("TP adjusted for {} new_qty={:.6f}", symbol, new_quantity)
                except Exception as exc:
                    logger.error("Failed to adjust TP for {}: {}", symbol, exc)

    async def handle_sl_filled(self, symbol: str) -> None:
        """OCO: SL filled → cancel TP."""
        async with self._lock:
            prot = self._protective.get(symbol)
            if prot is None:
                return
            prot.sl_filled = True
            if prot.tp_order_id and prot.tp_placed and not prot.tp_filled:
                try:
                    await self._client.cancel_order(prot.tp_order_id, symbol)
                    logger.info("OCO: cancelled TP {} after SL fill for {}", prot.tp_order_id, symbol)
                except Exception as exc:
                    logger.warning("Failed to cancel TP after SL fill for {}: {}", symbol, exc)

    async def handle_tp_filled(self, symbol: str) -> None:
        """OCO: TP filled → cancel SL."""
        async with self._lock:
            prot = self._protective.get(symbol)
            if prot is None:
                return
            prot.tp_filled = True
            if prot.sl_order_id and prot.sl_placed and not prot.sl_filled:
                try:
                    await self._client.cancel_order(prot.sl_order_id, symbol)
                    logger.info("OCO: cancelled SL {} after TP fill for {}", prot.sl_order_id, symbol)
                except Exception as exc:
                    logger.warning("Failed to cancel SL after TP fill for {}: {}", symbol, exc)

    async def cancel_all_for_symbol(self, symbol: str) -> None:
        """Cancel all protective orders for a symbol."""
        async with self._lock:
            prot = self._protective.pop(symbol, None)
            if prot is None:
                return
            for order_id, label in [
                (prot.sl_order_id, "SL"),
                (prot.tp_order_id, "TP"),
            ]:
                if order_id:
                    try:
                        await self._client.cancel_order(order_id, symbol)
                        logger.info("Cancelled {} {} for {}", label, order_id, symbol)
                    except Exception as exc:
                        logger.debug("Cancel {} for {} failed (may already be filled): {}", label, symbol, exc)

    def remove_tracking(self, symbol: str) -> None:
        """Remove protective order tracking after position fully closed."""
        self._protective.pop(symbol, None)

    def get_protective(self, symbol: str) -> ProtectiveOrders | None:
        return self._protective.get(symbol)

    # ── Private: raw Binance order placement ──────────────────────────────

    async def _place_stop_market(
        self, symbol: str, side: str, quantity: float, stop_price: float,
    ) -> dict:
        """Place STOP_MARKET order on Binance Futures."""
        params = {
            "stopPrice": stop_price,
            "reduceOnly": True,
            "workingType": self._working_type,
            "type": "STOP_MARKET",
        }
        order = await self._client.create_order(
            symbol=symbol,
            type="STOP_MARKET",
            side=side.lower(),
            amount=quantity,
            params=params,
        )
        return order

    async def _place_take_profit_market(
        self, symbol: str, side: str, quantity: float, stop_price: float,
    ) -> dict:
        """Place TAKE_PROFIT_MARKET order on Binance Futures."""
        params = {
            "stopPrice": stop_price,
            "reduceOnly": True,
            "workingType": self._working_type,
            "type": "TAKE_PROFIT_MARKET",
        }
        order = await self._client.create_order(
            symbol=symbol,
            type="TAKE_PROFIT_MARKET",
            side=side.lower(),
            amount=quantity,
            params=params,
        )
        return order
