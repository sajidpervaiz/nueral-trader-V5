from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Coroutine

import websockets
from loguru import logger

from core.config import Config
from core.event_bus import EventBus
from data_ingestion.normalizer import Normalizer, Tick
from data_ingestion.validators import TickValidator


WS_URLS: dict[str, str] = {
    "binance": "wss://fstream.binance.com/ws",
    "bybit": "wss://stream.bybit.com/v5/public/linear",
    "okx": "wss://ws.okx.com:8443/ws/v5/public",
    "kraken": "wss://ws.kraken.com",
}

WS_TESTNET_URLS: dict[str, str] = {
    "binance": "wss://stream.binancefuture.com/ws",
    "bybit": "wss://stream-testnet.bybit.com/v5/public/linear",
    "okx": "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999",
    "kraken": "wss://demo-futures.kraken.com/ws/v1",
}


class CEXWebSocketManager:
    def __init__(
        self,
        config: Config,
        event_bus: EventBus,
        normalizer: Normalizer | None = None,
        validator: TickValidator | None = None,
    ) -> None:
        self.config = config
        self.event_bus = event_bus
        self.normalizer = normalizer or Normalizer()
        self.validator = validator or TickValidator()
        self._connections: dict[str, Any] = {}
        self._running = False

    def _to_kraken_pair(self, symbol: str) -> str:
        """Convert internal symbol formats to Kraken pair notation."""
        normalized = symbol.upper()

        if "/" in normalized:
            base, quote = normalized.split("/", 1)
            quote = quote.split(":", 1)[0]
        else:
            if normalized.endswith("USDT"):
                base, quote = normalized[:-4], "USDT"
            elif normalized.endswith("USD"):
                base, quote = normalized[:-3], "USD"
            else:
                return normalized

        base_map = {"BTC": "XBT"}
        quote_map = {"USDT": "USD"}
        base = base_map.get(base, base)
        quote = quote_map.get(quote, quote)
        return f"{base}/{quote}"

    def _build_subscribe_msg(self, exchange: str, symbols: list[str]) -> list[dict | str]:
        if exchange == "binance":
            streams = [f"{s.replace('/', '').replace(':USDT', '').lower()}@aggTrade" for s in symbols]
            return [{"method": "SUBSCRIBE", "params": streams, "id": 1}]
        if exchange == "bybit":
            topics = [f"publicTrade.{s.replace('/', '').replace(':USDT', '')}" for s in symbols]
            return [{"op": "subscribe", "args": topics}]
        if exchange == "okx":
            args = [{"channel": "trades", "instId": s.replace("/USDT:USDT", "-USDT-SWAP")} for s in symbols]
            return [{"op": "subscribe", "args": args}]
        if exchange == "kraken":
            pairs = [self._to_kraken_pair(s) for s in symbols]
            return [{"event": "subscribe", "pair": pairs, "subscription": {"name": "trade"}}]
        logger.debug("No subscribe payload builder for exchange '{}'", exchange)
        return list()

    async def _handle_message(self, exchange: str, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        tick = self.normalizer.normalize_tick(exchange, data)
        if tick is None:
            return
        if not self.validator.validate(tick):
            return

        await self.event_bus.publish("TICK", tick)

    async def _connect_exchange(self, exchange: str, cfg: dict[str, Any]) -> None:
        testnet = cfg.get("testnet", True)
        urls = WS_TESTNET_URLS if testnet else WS_URLS
        url = urls.get(exchange)
        if not url:
            logger.warning("No WebSocket URL for exchange '{}'", exchange)
            return

        symbols: list[str] = cfg.get("symbols", [])
        subscribe_msgs = self._build_subscribe_msg(exchange, symbols)
        reconnect_delay = 1.0

        while self._running:
            try:
                logger.info("Connecting to {} WebSocket: {}", exchange, url)
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=30,
                    close_timeout=10,
                ) as ws:
                    self._connections[exchange] = ws
                    reconnect_delay = 1.0
                    for msg in subscribe_msgs:
                        await ws.send(json.dumps(msg))
                    logger.info("{} WebSocket connected and subscribed", exchange)

                    async for raw in ws:
                        if not self._running:
                            break
                        await self._handle_message(exchange, raw)

            except (websockets.ConnectionClosed, ConnectionError, OSError) as exc:
                logger.warning("{} WebSocket disconnected: {} — retry in {}s", exchange, exc, reconnect_delay)
            except Exception as exc:
                logger.exception("{} WebSocket unexpected error: {}", exchange, exc)

            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60.0)

    async def run(self) -> None:
        self._running = True
        exchanges_cfg = self.config.get_value("exchanges") or {}
        tasks = []
        for exchange, cfg in exchanges_cfg.items():
            if not cfg.get("enabled", False):
                continue
            tasks.append(asyncio.create_task(
                self._connect_exchange(exchange, cfg),
                name=f"ws_{exchange}",
            ))

        if not tasks:
            logger.warning("No exchanges enabled — CEX WebSocket manager idle")
            while self._running:
                await asyncio.sleep(5)
            return

        await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self) -> None:
        self._running = False
        for exchange, ws in self._connections.items():
            try:
                await ws.close()
            except Exception as exc:
                logger.debug("Error closing websocket for {}: {}", exchange, exc)
        logger.info("CEX WebSocket manager stopped")
