from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import ccxt.async_support as ccxt
from loguru import logger

from core.config import Config
from core.event_bus import EventBus
from engine.signal_generator import TradingSignal
from execution.risk_manager import RiskManager, Position


@dataclass
class OrderResult:
    order_id: str
    exchange: str
    symbol: str
    direction: str
    price: float
    quantity: float
    status: str
    is_paper: bool
    timestamp: int
    raw: dict[str, Any] | None = None


class CEXExecutor:
    def __init__(
        self,
        config: Config,
        event_bus: EventBus,
        risk_manager: RiskManager,
        exchange_id: str,
    ) -> None:
        self.config = config
        self.event_bus = event_bus
        self.risk_manager = risk_manager
        self.exchange_id = exchange_id
        self._client: Any = None
        self._running = False

    async def _init_client(self) -> None:
        cfg = self.config.get_value("exchanges", self.exchange_id) or {}
        if not cfg.get("enabled", False):
            return
        cls = getattr(ccxt, self.exchange_id, None)
        if cls is None:
            logger.warning("Unknown exchange: {}", self.exchange_id)
            return
        params: dict[str, Any] = {
            "apiKey": cfg.get("api_key", ""),
            "secret": cfg.get("api_secret", ""),
            "enableRateLimit": True,
        }
        passphrase = cfg.get("passphrase")
        if passphrase:
            params["password"] = passphrase
        if cfg.get("testnet"):
            params["options"] = {"defaultType": cfg.get("type", "future")}
            params["urls"] = {"api": params.get("urls", {}).get("test", {})}
        try:
            self._client = cls(params)
            if cfg.get("testnet"):
                self._client.set_sandbox_mode(True)
            await self._client.load_markets()
            logger.info("{} CEX client initialized", self.exchange_id)
        except Exception as exc:
            logger.warning("{} client init failed: {}", self.exchange_id, exc)
            self._client = None

    async def execute_signal(self, signal: TradingSignal, size: float) -> OrderResult | None:
        if self.config.paper_mode:
            return await self._paper_execute(signal, size)
        return await self._live_execute(signal, size)

    async def _paper_execute(self, signal: TradingSignal, size: float) -> OrderResult:
        slippage = self.config.get_value("backtest", "slippage_pct") or 0.0002
        fill_price = signal.price * (1 + slippage if signal.is_long else 1 - slippage)
        result = OrderResult(
            order_id=f"paper_{int(time.time()*1000)}",
            exchange=signal.exchange,
            symbol=signal.symbol,
            direction=signal.direction,
            price=fill_price,
            quantity=size / fill_price if fill_price > 0 else 0,
            status="filled",
            is_paper=True,
            timestamp=int(time.time()),
        )
        pos = self.risk_manager.open_position(signal, size)
        await self.event_bus.publish("ORDER_FILLED", result)
        logger.info("Paper order filled: {} {}/{} @ {:.2f}", signal.direction.upper(), signal.exchange, signal.symbol, fill_price)
        return result

    async def _live_execute(self, signal: TradingSignal, size: float) -> OrderResult | None:
        """Execute using LIMIT order for entries; MARKET only for emergency exits."""
        if self._client is None:
            logger.error("No live client for {} — cannot execute", self.exchange_id)
            return None
        try:
            side = "buy" if signal.is_long else "sell"
            amount = size / signal.price
            is_emergency = signal.metadata.get("emergency_exit", False)

            if is_emergency:
                order = await self._client.create_market_order(
                    symbol=signal.symbol, side=side, amount=amount, params={},
                )
            else:
                # Use limit order at signal price for controlled entry
                order = await self._client.create_limit_order(
                    symbol=signal.symbol, side=side, amount=amount,
                    price=signal.price, params={},
                )
                # Wait for fill with cancel/replace retry
                order = await self._wait_for_fill(signal, order, amount)

            result = OrderResult(
                order_id=order.get("id", ""),
                exchange=signal.exchange,
                symbol=signal.symbol,
                direction=signal.direction,
                price=float(order.get("average", order.get("price", signal.price))),
                quantity=float(order.get("filled", amount)),
                status=order.get("status", "unknown"),
                is_paper=False,
                timestamp=int(time.time()),
                raw=order,
            )
            if result.status in ("filled", "closed"):
                self.risk_manager.open_position(signal, size)
                await self.event_bus.publish("ORDER_FILLED", result)
            else:
                logger.warning("{} order not filled: status={}", self.exchange_id, result.status)
                await self.event_bus.publish("ORDER_FAILED", result)
            return result
        except Exception as exc:
            logger.exception("{} live order failed: {}", self.exchange_id, exc)
            return None

    async def _wait_for_fill(
        self, signal: TradingSignal, order: dict, amount: float,
        max_retries: int = 3, wait_sec: float = 5.0,
    ) -> dict:
        """Poll for fill; cancel & replace at market after max_retries."""
        order_id = order.get("id", "")
        for attempt in range(max_retries):
            await asyncio.sleep(wait_sec)
            try:
                fetched = await self._client.fetch_order(order_id, signal.symbol)
            except Exception:
                continue
            status = fetched.get("status", "")
            if status in ("closed", "filled"):
                return fetched
            filled_qty = float(fetched.get("filled", 0))
            if filled_qty > 0:
                return fetched  # partial fill — accept it
            logger.debug("{} order {} not filled after {}s, attempt {}/{}",
                         self.exchange_id, order_id, (attempt + 1) * wait_sec, attempt + 1, max_retries)

        # Cancel the limit order and fall back to market
        try:
            await self._client.cancel_order(order_id, signal.symbol)
            logger.info("{} cancelled unfilled limit order {}, placing market order",
                        self.exchange_id, order_id)
        except Exception as exc:
            logger.warning("{} cancel failed: {}", self.exchange_id, exc)

        side = "buy" if signal.is_long else "sell"
        market_order = await self._client.create_market_order(
            symbol=signal.symbol, side=side, amount=amount, params={},
        )
        return market_order

    async def _handle_signal(self, payload: Any) -> None:
        signal: TradingSignal = payload
        if signal.exchange != self.exchange_id:
            return
        approved, reason, size = self.risk_manager.approve_signal(signal)
        if not approved:
            logger.debug("Signal rejected for {}/{}: {}", signal.exchange, signal.symbol, reason)
            return
        await self.execute_signal(signal, size)

    async def _handle_stop_loss(self, payload: Any) -> None:
        exchange = payload.get("exchange", "")
        symbol = payload.get("symbol", "")
        price = float(payload.get("price", 0))
        if exchange != self.exchange_id:
            return
        pos = self.risk_manager.close_position(exchange, symbol, price)
        if pos:
            await self.event_bus.publish("POSITION_CLOSED", pos)

    async def _handle_take_profit(self, payload: Any) -> None:
        await self._handle_stop_loss(payload)

    async def _handle_kill_switch(self, payload: Any) -> None:
        """Emergency: cancel all open orders, close all positions at market."""
        logger.critical("KILL SWITCH received on {} executor", self.exchange_id)
        closed = self.risk_manager.activate_kill_switch()
        # Cancel all open orders on exchange
        if self._client:
            try:
                open_orders = await self._client.fetch_open_orders()
                for o in open_orders:
                    try:
                        await self._client.cancel_order(o["id"], o.get("symbol"))
                    except Exception:
                        pass
                logger.info("{} cancelled {} open orders", self.exchange_id, len(open_orders))
            except Exception as exc:
                logger.warning("{} failed to cancel orders: {}", self.exchange_id, exc)
        # Close remaining positions at market
        for pos in closed:
            await self.event_bus.publish("POSITION_CLOSED", pos)

    async def run(self) -> None:
        self._running = True
        await self._init_client()
        self.event_bus.subscribe("SIGNAL", self._handle_signal)
        self.event_bus.subscribe("STOP_LOSS", self._handle_stop_loss)
        self.event_bus.subscribe("TAKE_PROFIT", self._handle_take_profit)
        self.event_bus.subscribe("KILL_SWITCH", self._handle_kill_switch)
        logger.info("{} CEX executor started (paper_mode={})", self.exchange_id, self.config.paper_mode)
        while self._running:
            await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False
        self.event_bus.unsubscribe("SIGNAL", self._handle_signal)
        self.event_bus.unsubscribe("STOP_LOSS", self._handle_stop_loss)
        self.event_bus.unsubscribe("TAKE_PROFIT", self._handle_take_profit)
        self.event_bus.unsubscribe("KILL_SWITCH", self._handle_kill_switch)
        if self._client:
            try:
                await self._client.close()
            except Exception as exc:
                logger.warning("{} client close failed: {}", self.exchange_id, exc)
            finally:
                self._client = None

    async def close(self) -> None:
        """Compatibility alias used by main shutdown sequence."""
        await self.stop()
