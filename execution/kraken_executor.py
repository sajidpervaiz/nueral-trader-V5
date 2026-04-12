from __future__ import annotations

import time

from loguru import logger

from core.config import Config
from core.event_bus import EventBus
from engine.signal_generator import TradingSignal
from execution.cex_executor import CEXExecutor, OrderResult
from execution.risk_manager import RiskManager


class KrakenExecutor(CEXExecutor):
    """Kraken-specific CEX executor (spot + futures)."""

    def __init__(self, config: Config, event_bus: EventBus, risk_manager: RiskManager) -> None:
        super().__init__(config, event_bus, risk_manager, exchange_id="kraken")

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        """
        Normalize internal symbol formats to Kraken-compatible market symbols.
        """
        if symbol == "BTC/USDT:USDT":
            return "BTC/USD"
        if symbol == "ETH/USDT:USDT":
            return "ETH/USD"
        if ":" in symbol:
            symbol = symbol.split(":", 1)[0]
        return symbol

    async def _live_execute(self, signal: TradingSignal, size: float) -> OrderResult | None:
        if self._client is None:
            logger.error("No live client for kraken — cannot execute")
            return None
        try:
            await self._rate_limiter.acquire()
            side = "buy" if signal.is_long else "sell"
            kraken_symbol = self._normalize_symbol(signal.symbol)
            amount = size / signal.price if signal.price > 0 else 0.0
            if amount <= 0:
                logger.error("Invalid amount calculated for {}", signal.symbol)
                return None

            order = await self._client.create_market_order(
                symbol=kraken_symbol,
                side=side,
                amount=amount,
                params={},
            )

            fill_price = float(order.get("average", signal.price))
            filled_qty = float(order.get("filled", amount))
            filled_notional = filled_qty * fill_price

            result = OrderResult(
                order_id=order.get("id", ""),
                exchange=signal.exchange,
                symbol=signal.symbol,
                direction=signal.direction,
                price=fill_price,
                quantity=filled_qty,
                status=order.get("status", "unknown"),
                is_paper=False,
                timestamp=int(time.time()),
                raw=order,
            )
            # Use actual filled notional for position tracking
            await self.risk_manager.open_position(signal, filled_notional)

            # Place protective SL/TP via parent class order placer
            if self._order_placer and hasattr(signal, "sl") and signal.sl:
                try:
                    sl_side = "sell" if signal.is_long else "buy"
                    await self._client.create_order(
                        symbol=kraken_symbol,
                        type="stop-loss",
                        side=sl_side,
                        amount=filled_qty,
                        price=signal.sl,
                    )
                except Exception as sl_exc:
                    logger.warning("Kraken SL placement failed: {}", sl_exc)

            await self.event_bus.publish("ORDER_FILLED", result)
            return result
        except Exception as exc:
            logger.exception("Kraken live order failed: {}", exc)
            return None
