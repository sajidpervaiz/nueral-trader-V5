"""
ARMS-V2.1 Shadow Stop-Loss — 4-layer redundancy for stop placement (§13).

Layer 1: Primary exchange stop-loss order (immediate)
Layer 2: Retry — resubmit if primary fails within timeout
Layer 3: Shadow — local monitoring thread that triggers market sell if SL breached
Layer 4: Fallback — if all else fails, emergency market close after grace period
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine

from loguru import logger


@dataclass
class StopOrder:
    """Represents a stop-loss order across redundancy layers."""
    symbol: str
    exchange: str
    direction: str  # original position direction
    stop_price: float
    size: float
    # Layer tracking
    primary_placed: bool = False
    primary_order_id: str | None = None
    retry_count: int = 0
    shadow_active: bool = False
    fallback_triggered: bool = False
    created_at: float = field(default_factory=time.time)
    last_check: float = 0.0


class ShadowStopManager:
    """4-layer stop-loss redundancy.

    Parameters:
        primary_timeout_seconds: Time to wait for primary confirmation (default: 5s)
        max_retries: Max retries for Layer 2 (default: 3)
        shadow_check_interval: Shadow monitor interval (default: 1s)
        fallback_grace_seconds: Grace period before Layer 4 emergency close (default: 10s)
    """

    def __init__(
        self,
        primary_timeout_seconds: float = 5.0,
        max_retries: int = 3,
        shadow_check_interval: float = 1.0,
        fallback_grace_seconds: float = 10.0,
    ) -> None:
        self._primary_timeout = primary_timeout_seconds
        self._max_retries = max_retries
        self._shadow_interval = shadow_check_interval
        self._fallback_grace = fallback_grace_seconds
        self._stops: dict[str, StopOrder] = {}  # key → StopOrder
        self._running = False

    def register_stop(
        self,
        key: str,
        symbol: str,
        exchange: str,
        direction: str,
        stop_price: float,
        size: float,
    ) -> StopOrder:
        """Register a new stop-loss for 4-layer monitoring."""
        stop = StopOrder(
            symbol=symbol,
            exchange=exchange,
            direction=direction,
            stop_price=stop_price,
            size=size,
        )
        self._stops[key] = stop
        logger.info("Shadow SL registered: {} {} @ {:.2f}", key, direction, stop_price)
        return stop

    def update_stop_price(self, key: str, new_price: float) -> None:
        """Update stop price (e.g., trailing stop movement)."""
        stop = self._stops.get(key)
        if stop:
            old = stop.stop_price
            stop.stop_price = new_price
            stop.primary_placed = False  # Need to re-place on exchange
            logger.debug("Shadow SL updated: {} {:.2f} → {:.2f}", key, old, new_price)

    def remove_stop(self, key: str) -> None:
        """Remove stop tracking (position closed normally)."""
        self._stops.pop(key, None)

    async def place_primary_stop(
        self,
        key: str,
        place_fn: Callable[[str, str, str, float, float], Coroutine[Any, Any, str | None]],
    ) -> bool:
        """Layer 1 + 2: Place primary stop with retry.

        place_fn(symbol, exchange, direction, stop_price, size) → order_id or None
        """
        stop = self._stops.get(key)
        if stop is None:
            return False

        # Layer 1: Primary placement
        for attempt in range(1 + self._max_retries):
            try:
                order_id = await asyncio.wait_for(
                    place_fn(stop.symbol, stop.exchange, stop.direction, stop.stop_price, stop.size),
                    timeout=self._primary_timeout,
                )
                if order_id:
                    stop.primary_placed = True
                    stop.primary_order_id = order_id
                    stop.retry_count = attempt
                    logger.info(
                        "Shadow SL Layer {}: {} placed (order={})",
                        1 if attempt == 0 else 2, key, order_id,
                    )
                    return True
            except asyncio.TimeoutError:
                logger.warning(
                    "Shadow SL Layer {} timeout: {} (attempt {}/{})",
                    1 if attempt == 0 else 2, key, attempt + 1, 1 + self._max_retries,
                )
            except Exception as e:
                logger.error(
                    "Shadow SL Layer {} error: {} — {} (attempt {}/{})",
                    1 if attempt == 0 else 2, key, e, attempt + 1, 1 + self._max_retries,
                )

        # All retries exhausted — activate shadow monitoring
        stop.shadow_active = True
        logger.warning("Shadow SL Layers 1-2 failed for {} — activating Layer 3 shadow monitor", key)
        return False

    def check_shadow_stops(
        self, current_prices: dict[str, float],
    ) -> list[dict[str, Any]]:
        """Layer 3: Shadow monitoring — check if any stops are breached.

        Called from a periodic task. Returns list of positions needing emergency close.
        """
        actions: list[dict[str, Any]] = []
        now = time.time()

        for key, stop in self._stops.items():
            if not stop.shadow_active:
                continue
            if now - stop.last_check < self._shadow_interval:
                continue
            stop.last_check = now

            price = current_prices.get(stop.symbol, 0.0)
            if price <= 0:
                continue

            breached = False
            if stop.direction == "long" and price <= stop.stop_price:
                breached = True
            elif stop.direction == "short" and price >= stop.stop_price:
                breached = True

            if breached:
                actions.append({
                    "key": key,
                    "symbol": stop.symbol,
                    "exchange": stop.exchange,
                    "price": price,
                    "stop_price": stop.stop_price,
                    "size": stop.size,
                    "layer": 3,
                    "reason": "shadow_stop_triggered",
                })
                logger.warning(
                    "Shadow SL Layer 3 TRIGGERED: {} price={:.2f} SL={:.2f}",
                    key, price, stop.stop_price,
                )

        return actions

    def check_fallback(self) -> list[dict[str, Any]]:
        """Layer 4: Fallback — emergency close if shadow stop has been active
        beyond grace period without resolution.
        """
        actions: list[dict[str, Any]] = []
        now = time.time()

        for key, stop in self._stops.items():
            if not stop.shadow_active:
                continue
            if not stop.fallback_triggered and now - stop.created_at > self._fallback_grace:
                stop.fallback_triggered = True
                actions.append({
                    "key": key,
                    "symbol": stop.symbol,
                    "exchange": stop.exchange,
                    "size": stop.size,
                    "layer": 4,
                    "reason": "fallback_emergency_close",
                })
                logger.critical(
                    "Shadow SL Layer 4 FALLBACK: {} — emergency market close",
                    key,
                )

        return actions

    @property
    def active_stops(self) -> dict[str, StopOrder]:
        return dict(self._stops)

    @property
    def shadow_active_count(self) -> int:
        return sum(1 for s in self._stops.values() if s.shadow_active)

    def get_snapshot(self) -> dict[str, Any]:
        return {
            "total_stops": len(self._stops),
            "shadow_active": self.shadow_active_count,
            "stops": {
                k: {
                    "symbol": s.symbol,
                    "stop_price": s.stop_price,
                    "primary_placed": s.primary_placed,
                    "shadow_active": s.shadow_active,
                    "fallback_triggered": s.fallback_triggered,
                }
                for k, s in self._stops.items()
            },
        }
