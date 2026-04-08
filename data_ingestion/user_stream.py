"""
Binance Futures User Data Stream — real-time fill tracking and account updates.

Manages the listenKey lifecycle (create, keepalive every 30 min, reconnect on expiry)
and parses ORDER_TRADE_UPDATE, ACCOUNT_UPDATE, and listenKeyExpired events.

Every state change is published to EventBus for Order Manager and Risk Engine.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import websockets
from loguru import logger

from core.config import Config
from core.event_bus import EventBus


# Binance Futures endpoints
_BASE_URL_LIVE = "https://fapi.binance.com"
_BASE_URL_TESTNET = "https://testnet.binancefuture.com"
_WS_URL_LIVE = "wss://fstream.binance.com/ws/"
_WS_URL_TESTNET = "wss://stream.binancefuture.com/ws/"

# Keepalive interval (Binance requires every 60 min; we do every 30 min for safety)
_KEEPALIVE_INTERVAL = 30 * 60


class UserDataStream:
    """
    Connects to Binance Futures User Data Stream via WebSocket.

    Events published to EventBus:
    - USER_ORDER_UPDATE: order fill, partial fill, cancel, reject, liquidation
    - USER_ACCOUNT_UPDATE: balance/position change
    - USER_STREAM_LOST: stream disconnected (triggers safety mode)
    """

    def __init__(
        self,
        config: Config,
        event_bus: EventBus,
    ) -> None:
        self.config = config
        self.event_bus = event_bus
        self._running = False
        self._listen_key: str | None = None
        self._ws: Any = None
        self._last_keepalive: float = 0.0
        self._connected = False
        self._disconnect_time: float | None = None

        # Parse exchange config
        cfg = config.get_value("exchanges", "binance") or {}
        self._api_key = cfg.get("api_key", "")
        self._api_secret = cfg.get("api_secret", "")
        self._testnet = cfg.get("testnet", True)
        self._enabled = cfg.get("enabled", False)

        self._base_url = _BASE_URL_TESTNET if self._testnet else _BASE_URL_LIVE
        self._ws_url = _WS_URL_TESTNET if self._testnet else _WS_URL_LIVE

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def disconnect_duration(self) -> float:
        """Seconds since last disconnect, or 0 if connected."""
        if self._connected or self._disconnect_time is None:
            return 0.0
        return time.time() - self._disconnect_time

    async def _http_request(self, method: str, path: str, **kwargs: Any) -> dict:
        """Make authenticated HTTP request to Binance Futures API."""
        import aiohttp
        url = f"{self._base_url}{path}"
        headers = {"X-MBX-APIKEY": self._api_key}
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RuntimeError(f"Binance API {method} {path} failed ({resp.status}): {body}")
                return await resp.json()

    async def _create_listen_key(self) -> str:
        """POST /fapi/v1/listenKey — create user data stream."""
        data = await self._http_request("POST", "/fapi/v1/listenKey")
        key = data.get("listenKey", "")
        if not key:
            raise RuntimeError(f"No listenKey in response: {data}")
        logger.info("User data stream listenKey created")
        return key

    async def _keepalive_listen_key(self) -> None:
        """PUT /fapi/v1/listenKey — extend listen key validity with retry."""
        if not self._listen_key:
            return
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                await self._http_request("PUT", "/fapi/v1/listenKey")
                self._last_keepalive = time.time()
                logger.debug("User data stream listenKey keepalive sent")
                return
            except Exception as exc:
                logger.warning(
                    "listenKey keepalive attempt {}/{} failed: {}",
                    attempt, max_retries, exc,
                )
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)  # 2s, 4s backoff
        # All retries exhausted — invalidate key to force reconnect
        logger.error(
            "listenKey keepalive failed after {} retries — forcing reconnect",
            max_retries,
        )
        self._listen_key = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass

    async def _keepalive_loop(self) -> None:
        """Background task to send keepalive every 30 minutes."""
        while self._running:
            await asyncio.sleep(_KEEPALIVE_INTERVAL)
            if self._running and self._listen_key:
                await self._keepalive_listen_key()

    def _parse_order_update(self, data: dict) -> dict:
        """Parse ORDER_TRADE_UPDATE event from Binance."""
        o = data.get("o", {})
        return {
            "event_type": "ORDER_TRADE_UPDATE",
            "event_time": data.get("E", 0),
            "transaction_time": data.get("T", 0),
            "symbol": o.get("s", ""),
            "client_order_id": o.get("c", ""),
            "side": o.get("S", ""),
            "order_type": o.get("o", ""),
            "time_in_force": o.get("f", ""),
            "quantity": float(o.get("q", 0)),
            "price": float(o.get("p", 0)),
            "avg_price": float(o.get("ap", 0)),
            "stop_price": float(o.get("sp", 0)),
            "execution_type": o.get("x", ""),  # NEW, TRADE, CANCELED, EXPIRED, etc.
            "order_status": o.get("X", ""),  # NEW, PARTIALLY_FILLED, FILLED, CANCELED, etc.
            "order_id": int(o.get("i", 0)),
            "last_filled_qty": float(o.get("l", 0)),
            "cumulative_filled_qty": float(o.get("z", 0)),
            "last_filled_price": float(o.get("L", 0)),
            "commission": float(o.get("n", 0)),
            "commission_asset": o.get("N", ""),
            "trade_id": int(o.get("t", 0)),
            "reduce_only": o.get("R", False),
            "position_side": o.get("ps", "BOTH"),
            "realized_profit": float(o.get("rp", 0)),
            "is_maker": o.get("m", False),
        }

    def _parse_account_update(self, data: dict) -> dict:
        """Parse ACCOUNT_UPDATE event from Binance."""
        a = data.get("a", {})
        balances = [
            {
                "asset": b.get("a", ""),
                "wallet_balance": float(b.get("wb", 0)),
                "cross_wallet_balance": float(b.get("cw", 0)),
                "balance_change": float(b.get("bc", 0)),
            }
            for b in a.get("B", [])
        ]
        positions = [
            {
                "symbol": p.get("s", ""),
                "position_amount": float(p.get("pa", 0)),
                "entry_price": float(p.get("ep", 0)),
                "unrealized_pnl": float(p.get("up", 0)),
                "margin_type": p.get("mt", ""),
                "isolated_wallet": float(p.get("iw", 0)),
                "position_side": p.get("ps", "BOTH"),
            }
            for p in a.get("P", [])
        ]
        return {
            "event_type": "ACCOUNT_UPDATE",
            "event_time": data.get("E", 0),
            "transaction_time": data.get("T", 0),
            "reason": a.get("m", ""),
            "balances": balances,
            "positions": positions,
        }

    async def _handle_message(self, raw: str) -> None:
        """Parse and dispatch user data stream messages."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON from user stream: {}", raw[:200])
            return

        event = data.get("e", "")

        if event == "ORDER_TRADE_UPDATE":
            parsed = self._parse_order_update(data)
            logger.info(
                "Order update: {} {} {} exec={} status={} filled={}/{}",
                parsed["symbol"], parsed["side"], parsed["order_type"],
                parsed["execution_type"], parsed["order_status"],
                parsed["cumulative_filled_qty"], parsed["quantity"],
            )
            await self.event_bus.publish("USER_ORDER_UPDATE", parsed)

        elif event == "ACCOUNT_UPDATE":
            parsed = self._parse_account_update(data)
            logger.info(
                "Account update: reason={} balances={} positions={}",
                parsed["reason"], len(parsed["balances"]), len(parsed["positions"]),
            )
            await self.event_bus.publish("USER_ACCOUNT_UPDATE", parsed)

        elif event == "listenKeyExpired":
            logger.warning("User data stream listenKey expired — reconnecting")
            self._listen_key = None

        elif event == "MARGIN_CALL":
            logger.critical("MARGIN CALL received: {}", data)
            await self.event_bus.publish("MARGIN_CALL", data)

        else:
            logger.debug("Unknown user stream event: {}", event)

    async def run(self) -> None:
        """Main loop: connect, listen, reconnect on failure."""
        if not self._enabled:
            logger.info("Binance not enabled — user data stream disabled")
            return
        if not self._api_key:
            logger.warning("No Binance API key — user data stream disabled")
            return
        if self.config.paper_mode:
            logger.info("Paper mode — user data stream disabled (no real fills)")
            return

        self._running = True
        reconnect_delay = 1.0

        # Start keepalive background task
        keepalive_task = asyncio.create_task(self._keepalive_loop(), name="user_stream_keepalive")

        try:
            while self._running:
                try:
                    # Get or refresh listen key
                    if not self._listen_key:
                        self._listen_key = await self._create_listen_key()
                        self._last_keepalive = time.time()

                    ws_url = f"{self._ws_url}{self._listen_key}"
                    logger.info("Connecting to Binance user data stream...")

                    async with websockets.connect(
                        ws_url,
                        ping_interval=20,
                        ping_timeout=30,
                        close_timeout=10,
                    ) as ws:
                        self._ws = ws
                        self._connected = True
                        self._disconnect_time = None
                        reconnect_delay = 1.0
                        logger.info("Binance user data stream connected")
                        await self.event_bus.publish("USER_STREAM_CONNECTED", {})

                        async for raw in ws:
                            if not self._running:
                                break
                            await self._handle_message(raw)

                except (websockets.ConnectionClosed, ConnectionError, OSError) as exc:
                    logger.warning("User data stream disconnected: {} — retry in {}s", exc, reconnect_delay)
                except Exception as exc:
                    logger.exception("User data stream error: {}", exc)

                # Mark disconnected
                self._connected = False
                self._disconnect_time = time.time()
                await self.event_bus.publish("USER_STREAM_LOST", {
                    "timestamp": time.time(),
                })

                if self._running:
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 30.0)
                    # Force new listen key on reconnect
                    self._listen_key = None
        finally:
            keepalive_task.cancel()
            try:
                await keepalive_task
            except asyncio.CancelledError:
                pass

    async def stop(self) -> None:
        self._running = False
        self._connected = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        logger.info("User data stream stopped")
