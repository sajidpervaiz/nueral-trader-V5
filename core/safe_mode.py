"""
Centralized Safe Mode manager for disaster resilience.

When SafeMode is active:
- No new trades are opened.
- Existing positions remain protected by exchange-side SL/TP.
- The system logs the reason and timestamp of activation.
- SafeMode can only be cleared explicitly after the fault resolves.

Triggers:
- WebSocket disconnect (market data blind)
- USER_DATA stream failure (fill/cancel blind)
- Exchange API timeout
- DB outage
- Circuit breaker trip
- Manual activation
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger


class SafeModeReason(str, Enum):
    WEBSOCKET_DISCONNECT = "websocket_disconnect"
    USER_STREAM_LOST = "user_stream_lost"
    EXCHANGE_API_TIMEOUT = "exchange_api_timeout"
    DB_OUTAGE = "db_outage"
    CIRCUIT_BREAKER = "circuit_breaker"
    EXTREME_VOLATILITY = "extreme_volatility"
    EXTREME_SPREAD = "extreme_spread"
    ORDER_REJECTION_BURST = "order_rejection_burst"
    PARTIAL_FILL_STUCK = "partial_fill_stuck"
    MANUAL = "manual"


@dataclass
class SafeModeEvent:
    reason: SafeModeReason
    timestamp: float
    detail: str = ""


class SafeModeManager:
    """Thread-safe safe mode controller.  Multiple reasons can be active
    simultaneously — safe mode is ON as long as *any* reason remains."""

    def __init__(self) -> None:
        self._active_reasons: dict[SafeModeReason, SafeModeEvent] = {}
        self._history: list[dict[str, Any]] = []
        self._lock = asyncio.Lock()

    # ── Queries ───────────────────────────────────────────────────────────
    @property
    def is_active(self) -> bool:
        return len(self._active_reasons) > 0

    @property
    def active_reasons(self) -> list[SafeModeReason]:
        return list(self._active_reasons.keys())

    @property
    def active_events(self) -> list[SafeModeEvent]:
        return list(self._active_reasons.values())

    # ── Mutations ─────────────────────────────────────────────────────────
    async def activate(self, reason: SafeModeReason, detail: str = "") -> None:
        """Activate safe mode for *reason*.  Idempotent — re-activation updates detail."""
        async with self._lock:
            was_active = self.is_active
            event = SafeModeEvent(reason=reason, timestamp=time.time(), detail=detail)
            self._active_reasons[reason] = event
            self._history.append({
                "action": "activate",
                "reason": reason.value,
                "detail": detail,
                "timestamp": event.timestamp,
            })
        if not was_active:
            logger.critical("SAFE MODE ACTIVATED — reason: {} ({})", reason.value, detail)
        else:
            logger.warning("Safe mode reason added: {} ({})", reason.value, detail)

    def activate_sync(self, reason: SafeModeReason, detail: str = "") -> None:
        """Synchronous activate for non-async contexts (e.g. callbacks)."""
        was_active = self.is_active
        event = SafeModeEvent(reason=reason, timestamp=time.time(), detail=detail)
        self._active_reasons[reason] = event
        self._history.append({
            "action": "activate",
            "reason": reason.value,
            "detail": detail,
            "timestamp": event.timestamp,
        })
        if not was_active:
            logger.critical("SAFE MODE ACTIVATED — reason: {} ({})", reason.value, detail)
        else:
            logger.warning("Safe mode reason added: {} ({})", reason.value, detail)

    async def deactivate(self, reason: SafeModeReason) -> bool:
        """Clear a single reason.  Returns True if safe mode is now fully clear."""
        async with self._lock:
            result = self._deactivate_internal(reason)
        if result:
            logger.info("SAFE MODE FULLY CLEARED — normal operation resumed")
        return result

    def deactivate_sync(self, reason: SafeModeReason) -> bool:
        """Synchronous deactivate for non-async contexts."""
        result = self._deactivate_internal(reason)
        if result:
            logger.info("SAFE MODE FULLY CLEARED — normal operation resumed")
        return result

    def _deactivate_internal(self, reason: SafeModeReason) -> bool:
        removed = self._active_reasons.pop(reason, None)
        if removed:
            self._history.append({
                "action": "deactivate",
                "reason": reason.value,
                "timestamp": time.time(),
            })
            logger.info("Safe mode reason cleared: {}", reason.value)
        return not self.is_active

    async def clear_all(self) -> None:
        """Force-clear all reasons (admin override)."""
        async with self._lock:
            self._clear_all_internal()
        logger.info("SAFE MODE force-cleared (all reasons)")

    def clear_all_sync(self) -> None:
        """Synchronous clear_all for non-async contexts."""
        self._clear_all_internal()
        logger.info("SAFE MODE force-cleared (all reasons)")

    def _clear_all_internal(self) -> None:
        self._active_reasons.clear()
        self._history.append({
            "action": "clear_all",
            "timestamp": time.time(),
        })

    def get_status(self) -> dict[str, Any]:
        return {
            "safe_mode_active": self.is_active,
            "active_reasons": [
                {"reason": r.value, "detail": e.detail, "since": e.timestamp}
                for r, e in self._active_reasons.items()
            ],
            "history_count": len(self._history),
        }

    def get_history(self) -> list[dict[str, Any]]:
        return list(self._history)
