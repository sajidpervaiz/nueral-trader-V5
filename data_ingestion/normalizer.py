from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import time


@dataclass
class Tick:
    exchange: str
    symbol: str
    timestamp_us: int
    price: float
    volume: float
    side: str = ""
    trade_id: str = ""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Tick":
        return cls(
            exchange=d["exchange"],
            symbol=d["symbol"],
            timestamp_us=int(d.get("timestamp_us", time.time_ns() // 1000)),
            price=float(d["price"]),
            volume=float(d.get("volume", 0.0)),
            side=str(d.get("side", "")),
            trade_id=str(d.get("trade_id", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timestamp_us": self.timestamp_us,
            "price": self.price,
            "volume": self.volume,
            "side": self.side,
            "trade_id": self.trade_id,
        }


@dataclass
class Candle:
    exchange: str
    symbol: str
    timeframe: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    num_trades: int = 0


@dataclass
class OrderBookSnapshot:
    exchange: str
    symbol: str
    timestamp_us: int
    bids: list[tuple[float, float]] = field(default_factory=list)
    asks: list[tuple[float, float]] = field(default_factory=list)

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    @property
    def mid_price(self) -> float:
        if self.bids and self.asks:
            return (self.best_bid + self.best_ask) / 2.0
        return 0.0

    @property
    def spread_bps(self) -> float:
        mid = self.mid_price
        if mid == 0:
            return 0.0
        return ((self.best_ask - self.best_bid) / mid) * 10_000


class Normalizer:
    """Normalises raw exchange messages into unified Tick / Candle objects."""

    EXCHANGE_SYMBOL_MAP: dict[str, dict[str, str]] = {
        "binance": {},
        "bybit": {},
        "okx": {},
        "kraken": {},
    }

    def normalize_tick(self, exchange: str, raw: dict[str, Any]) -> Tick | None:
        try:
            if exchange == "binance":
                return Tick(
                    exchange=exchange,
                    symbol=self._unify_symbol(exchange, raw.get("s", "")),
                    timestamp_us=int(raw.get("T", time.time_ns() // 1_000_000)) * 1000,
                    price=float(raw.get("p", 0)),
                    volume=float(raw.get("q", 0)),
                    side="buy" if raw.get("m") is False else "sell",
                    trade_id=str(raw.get("t", "")),
                )
            if exchange == "bybit":
                data = raw.get("data", [{}])
                if isinstance(data, list):
                    data = data[0] if data else {}
                return Tick(
                    exchange=exchange,
                    symbol=self._unify_symbol(exchange, data.get("s", "")),
                    timestamp_us=int(data.get("T", time.time_ns() // 1_000_000)) * 1000,
                    price=float(data.get("p", 0)),
                    volume=float(data.get("v", 0)),
                    side=data.get("S", "").lower(),
                    trade_id=str(data.get("i", "")),
                )
            if exchange == "okx":
                data = raw.get("data", [{}])
                if isinstance(data, list):
                    data = data[0] if data else {}
                return Tick(
                    exchange=exchange,
                    symbol=self._unify_symbol(exchange, data.get("instId", "")),
                    timestamp_us=int(data.get("ts", time.time_ns() // 1_000_000)) * 1000,
                    price=float(data.get("px", 0)),
                    volume=float(data.get("sz", 0)),
                    side=data.get("side", "").lower(),
                    trade_id=str(data.get("tradeId", "")),
                )
            if exchange == "kraken":
                trades = raw if isinstance(raw, list) else []
                if trades:
                    t = trades[0]
                    return Tick(
                        exchange=exchange,
                        symbol=self._unify_symbol(exchange, str(t[0])),
                        timestamp_us=int(float(t[2]) * 1_000_000),
                        price=float(t[0]) if len(t) > 0 else 0.0,
                        volume=float(t[1]) if len(t) > 1 else 0.0,
                        side="buy" if len(t) > 3 and t[3] == "b" else "sell",
                        trade_id=str(t[5]) if len(t) > 5 else "",
                    )
        except (KeyError, ValueError, IndexError):
            pass
        return None

    def _unify_symbol(self, exchange: str, symbol: str) -> str:
        mapping = self.EXCHANGE_SYMBOL_MAP.get(exchange, {})
        return mapping.get(symbol, symbol.replace("-USDT-SWAP", "/USDT:USDT")
                                          .replace("USDT", "/USDT:USDT")
                                          .rstrip(":USDT"))
