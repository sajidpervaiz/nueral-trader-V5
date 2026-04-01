from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from loguru import logger

from core.config import Config
from core.event_bus import EventBus


@dataclass
class DEXQuote:
    dex: str
    token_in: str
    token_out: str
    amount_in: float
    amount_out: float
    price_impact_pct: float
    gas_estimate: int
    route: list[str]
    timestamp: int
    valid_until: int


@dataclass
class DEXSwapResult:
    tx_hash: str
    dex: str
    token_in: str
    token_out: str
    amount_in: float
    amount_out: float
    gas_used: int
    status: str
    is_paper: bool
    timestamp: int


try:
    import grpc
    from google.protobuf import empty_pb2
    _GRPC_AVAILABLE = True
except ImportError:
    _GRPC_AVAILABLE = False


class DEXClient:
    """Python client for the TypeScript DEX layer gRPC service."""

    def __init__(self, config: Config, event_bus: EventBus) -> None:
        self.config = config
        self.event_bus = event_bus
        self._channel: Any = None
        self._stub: Any = None
        self._available = False
        self._quote_cache: dict[str, tuple[DEXQuote, float]] = {}

    async def connect(self) -> bool:
        ts_cfg = self.config.get_value("ts_dex_layer") or {}
        if not ts_cfg.get("enabled", False):
            logger.info("TypeScript DEX layer disabled — DEXClient offline")
            return False

        if not _GRPC_AVAILABLE:
            logger.warning("grpcio not installed — DEXClient unavailable")
            return False

        host = ts_cfg.get("grpc_host", "localhost")
        port = int(ts_cfg.get("grpc_port", 50051))
        try:
            self._channel = grpc.aio.insecure_channel(f"{host}:{port}")
            await self._channel.channel_ready()
            self._available = True
            logger.info("Connected to TypeScript DEX layer at {}:{}", host, port)
            return True
        except Exception as exc:
            logger.warning("DEX layer connection failed: {} — using Python fallback", exc)
            self._available = False
            return False

    async def get_quote(
        self,
        dex: str,
        token_in: str,
        token_out: str,
        amount_in: float,
    ) -> DEXQuote | None:
        cache_key = f"{dex}:{token_in}:{token_out}:{amount_in}"
        ts_cfg = self.config.get_value("ts_dex_layer") or {}
        cache_ttl = float(ts_cfg.get("quote_cache_ttl_seconds", 10))

        cached = self._quote_cache.get(cache_key)
        if cached and time.time() - cached[1] < cache_ttl:
            return cached[0]

        if not self._available:
            return self._simulate_quote(dex, token_in, token_out, amount_in)

        try:
            pass
        except Exception as exc:
            logger.debug("DEX gRPC quote error: {}", exc)
            return self._simulate_quote(dex, token_in, token_out, amount_in)

        return None

    def _simulate_quote(
        self,
        dex: str,
        token_in: str,
        token_out: str,
        amount_in: float,
    ) -> DEXQuote:
        now = int(time.time())
        return DEXQuote(
            dex=dex,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_in * 0.997,
            price_impact_pct=0.3,
            gas_estimate=150_000,
            route=[token_in, token_out],
            timestamp=now,
            valid_until=now + 30,
        )

    async def execute_swap(self, quote: DEXQuote) -> DEXSwapResult | None:
        if self.config.paper_mode:
            return DEXSwapResult(
                tx_hash=f"0xpaper_{int(time.time()*1000):x}",
                dex=quote.dex,
                token_in=quote.token_in,
                token_out=quote.token_out,
                amount_in=quote.amount_in,
                amount_out=quote.amount_out,
                gas_used=quote.gas_estimate,
                status="success",
                is_paper=True,
                timestamp=int(time.time()),
            )

        if not self._available:
            logger.error("DEX layer unavailable — cannot execute live swap")
            return None

        logger.error("Live DEX swap not yet implemented — enable ts_dex_layer in config")
        return None

    async def close(self) -> None:
        if self._channel:
            await self._channel.close()


class DEXAggregator:
    """Routes DEX quotes across multiple protocols and picks the best."""

    DEX_PROTOCOLS = ["uniswap", "sushiswap", "pancakeswap"]

    def __init__(self, config: Config, event_bus: EventBus) -> None:
        self._client = DEXClient(config, event_bus)
        self.config = config

    async def connect(self) -> None:
        await self._client.connect()

    async def best_quote(
        self, token_in: str, token_out: str, amount_in: float
    ) -> DEXQuote | None:
        quotes = []
        for dex in self.DEX_PROTOCOLS:
            q = await self._client.get_quote(dex, token_in, token_out, amount_in)
            if q:
                quotes.append(q)

        if not quotes:
            return None

        return max(quotes, key=lambda q: q.amount_out)

    async def close(self) -> None:
        await self._client.close()
