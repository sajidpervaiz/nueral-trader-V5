"""
Exchange-side SL/TP order management for Binance Futures.

Places STOP_MARKET and TAKE_PROFIT_MARKET orders on the exchange immediately
after an entry fill, so positions are ALWAYS protected even if the bot crashes.

Implements manual OCO: when SL fills → cancel TP, when TP fills → cancel SL.
Handles partial fills by adjusting protective order quantities.
Supports both one-way mode (reduceOnly) and hedge mode (positionSide).
Enforces tick-size and step-size rounding per symbol.
"""
from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import Any

from loguru import logger


class ProtectiveOrderFallbackRequired(RuntimeError):
    """Exchange-side protective orders are unavailable; use bot-managed exits."""


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
    - Supports hedge mode (positionSide=LONG/SHORT) and one-way mode (reduceOnly)
    - Enforces tick-size / step-size rounding from exchange market info
    """

    def __init__(
        self,
        client: Any,
        working_type: str = "CONTRACT_PRICE",
        rate_limiter: Any = None,
        hedge_mode: bool = False,
    ) -> None:
        self._client = client
        self._working_type = working_type
        self._rate_limiter = rate_limiter
        self._hedge_mode = hedge_mode
        # symbol → ProtectiveOrders
        self._protective: dict[str, ProtectiveOrders] = {}
        self._lock = asyncio.Lock()

    # ── Precision helpers ─────────────────────────────────────────────────

    def _get_market(self, symbol: str) -> dict | None:
        """Retrieve the CCXT market entry for a symbol (cached by load_markets)."""
        if self._client is None:
            return None
        markets = getattr(self._client, "markets", None)
        if not isinstance(markets, dict):
            return None
        return markets.get(symbol)

    def round_price(self, symbol: str, price: float) -> float:
        """Round a price to the symbol's tick size or decimal precision."""
        market = self._get_market(symbol)
        if market:
            precision = market.get("precision", {})
            limits = market.get("limits", {})
            tick = _normalize_step_size(
                precision.get("price"),
                ((limits.get("price") or {}).get("min")),
            )
            if tick is not None and tick > 0:
                return _round_to_precision(price, tick)
        return price

    def round_quantity(self, symbol: str, qty: float) -> float:
        """Round a quantity to the symbol's step size or decimal precision."""
        market = self._get_market(symbol)
        if market:
            precision = market.get("precision", {})
            limits = market.get("limits", {})
            step = _normalize_step_size(
                precision.get("amount"),
                ((limits.get("amount") or {}).get("min")),
            )
            if step is not None and step > 0:
                return _round_to_precision(qty, step)
        return qty

    def _position_side_param(self, direction: str) -> dict[str, str]:
        """Return positionSide params for hedge mode, empty dict for one-way."""
        if not self._hedge_mode:
            return {}
        # In hedge mode, the *position* side is set (not the order side).
        # For a long position, protective (close) orders carry positionSide=LONG.
        # For a short position, protective (close) orders carry positionSide=SHORT.
        return {"positionSide": "LONG" if direction == "long" else "SHORT"}

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

        Prices and quantities are rounded to exchange tick/step size.
        """
        async with self._lock:
            # Round to exchange precision
            sl_price = self.round_price(symbol, sl_price)
            tp_price = self.round_price(symbol, tp_price)
            quantity = self.round_quantity(symbol, quantity)

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
                    direction=direction,
                )
                prot.sl_order_id = sl_order.get("id", "")
                prot.sl_placed = True
                logger.info(
                    "SL placed: {} {} qty={:.6f} stop={:.2f} orderId={}",
                    symbol, prot.close_side, quantity, sl_price, prot.sl_order_id,
                )
            except Exception as exc:
                if _is_unsupported_protective_order_error(exc):
                    logger.warning(
                        "Exchange-side SL unsupported for {} on this runtime — falling back to bot-managed exits: {}",
                        symbol,
                        exc,
                    )
                    self._protective[symbol] = prot
                    raise ProtectiveOrderFallbackRequired(str(exc)) from exc
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
                    direction=direction,
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

            new_quantity = self.round_quantity(symbol, new_quantity)
            old_qty = prot.quantity
            prot.quantity = new_quantity
            logger.info(
                "Adjusting protective orders for {}: qty {:.6f} → {:.6f}",
                symbol, old_qty, new_quantity,
            )

            # Cancel and replace SL with new qty
            if prot.sl_order_id and prot.sl_placed and not prot.sl_filled:
                try:
                    if self._rate_limiter:
                        await self._rate_limiter.acquire()
                    await self._client.cancel_order(prot.sl_order_id, symbol)
                    sl_order = await self._place_stop_market(
                        symbol=symbol,
                        side=prot.close_side,
                        quantity=new_quantity,
                        stop_price=prot.sl_price,
                        direction=prot.direction,
                    )
                    prot.sl_order_id = sl_order.get("id", "")
                    logger.info("SL adjusted for {} new_qty={:.6f}", symbol, new_quantity)
                except Exception as exc:
                    logger.error("Failed to adjust SL for {}: {}", symbol, exc)

            # Cancel and replace TP with new qty
            if prot.tp_order_id and prot.tp_placed and not prot.tp_filled:
                try:
                    if self._rate_limiter:
                        await self._rate_limiter.acquire()
                    await self._client.cancel_order(prot.tp_order_id, symbol)
                    tp_order = await self._place_take_profit_market(
                        symbol=symbol,
                        side=prot.close_side,
                        quantity=new_quantity,
                        stop_price=prot.tp_price,
                        direction=prot.direction,
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
                    if self._rate_limiter:
                        await self._rate_limiter.acquire()
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
                    if self._rate_limiter:
                        await self._rate_limiter.acquire()
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
                        if self._rate_limiter:
                            await self._rate_limiter.acquire()
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
        direction: str = "long",
    ) -> dict:
        """Place STOP_MARKET order on Binance Futures.

        In hedge mode, includes positionSide=LONG/SHORT instead of reduceOnly.
        """
        params: dict[str, Any] = {
            "stopPrice": stop_price,
            "workingType": self._working_type,
        }
        if self._hedge_mode:
            params.update(self._position_side_param(direction))
        else:
            params["reduceOnly"] = True
        if self._rate_limiter:
            await self._rate_limiter.acquire()
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
        direction: str = "long",
    ) -> dict:
        """Place TAKE_PROFIT_MARKET order on Binance Futures.

        In hedge mode, includes positionSide=LONG/SHORT instead of reduceOnly.
        """
        params: dict[str, Any] = {
            "stopPrice": stop_price,
            "workingType": self._working_type,
        }
        if self._hedge_mode:
            params.update(self._position_side_param(direction))
        else:
            params["reduceOnly"] = True
        if self._rate_limiter:
            await self._rate_limiter.acquire()
        order = await self._client.create_order(
            symbol=symbol,
            type="TAKE_PROFIT_MARKET",
            side=side.lower(),
            amount=quantity,
            params=params,
        )
        return order


# ── Module-level helper ──────────────────────────────────────────────────────

def _is_unsupported_protective_order_error(exc: Exception) -> bool:
    """Detect venue/runtime combinations that do not support exchange-native stop orders."""
    msg = str(exc).lower()
    return (
        "code\":-4120" in msg
        or "order type not supported for this endpoint" in msg
        or "algo order api endpoints" in msg
    )


def _normalize_step_size(precision: float | int | None, min_value: float | int | None = None) -> float | None:
    """Convert CCXT precision metadata into a concrete step size.

    CCXT markets may expose precision either as:
    - a literal step size like 0.001, or
    - a number of decimal places like 3.
    """
    if precision is None:
        try:
            min_float = float(min_value) if min_value is not None else None
        except (TypeError, ValueError):
            min_float = None
        return min_float if min_float and min_float > 0 else None

    try:
        prec = float(precision)
    except (TypeError, ValueError):
        return None

    if prec <= 0:
        return None
    if prec < 1:
        return prec

    min_float: float | None = None
    try:
        min_float = float(min_value) if min_value is not None else None
    except (TypeError, ValueError):
        min_float = None

    if min_float is not None and min_float > 0:
        if min_float < 1 or float(min_float).is_integer():
            return min_float

    if float(prec).is_integer() and prec <= 18:
        return 10 ** (-int(prec))

    return prec


def _round_to_precision(value: float, precision: float) -> float:
    """Round *value* down to the nearest multiple of *precision*.

    Uses floor rounding so we never exceed available balance / position size.
    Example: _round_to_precision(0.12345, 0.001) → 0.123
    """
    if precision <= 0:
        return value
    decimals = max(0, -math.floor(math.log10(precision))) if precision < 1 else 0
    floored = math.floor(value / precision) * precision
    return round(floored, decimals)
