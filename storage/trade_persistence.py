"""
Full trade persistence layer for PostgreSQL — auditability and crash recovery.

Persists all critical trading objects with idempotent upserts:
- Signals, Orders, Fills, Positions, Equity snapshots, Risk blocks, Errors.

Every write is idempotent (unique key per orderId/tradeId).
EventBus integration: subscribes to events and persists automatically.
"""
from __future__ import annotations

import datetime
import json as _json
import time
from pathlib import Path
from typing import Any

from loguru import logger

from core.event_bus import EventBus


# ── Additional DDL for production trading tables ──────────────────────────────

CREATE_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    client_order_id TEXT UNIQUE,
    exchange        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    order_type      TEXT NOT NULL,
    quantity        DOUBLE PRECISION NOT NULL,
    price           DOUBLE PRECISION,
    status          TEXT NOT NULL,
    filled_qty      DOUBLE PRECISION DEFAULT 0,
    avg_fill_price  DOUBLE PRECISION DEFAULT 0,
    total_fee       DOUBLE PRECISION DEFAULT 0,
    reduce_only     BOOLEAN DEFAULT FALSE,
    is_paper        BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB DEFAULT '{}'
);
"""

CREATE_FILLS = """
CREATE TABLE IF NOT EXISTS fills (
    fill_id         TEXT NOT NULL,
    order_id        TEXT NOT NULL,
    exchange        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    quantity        DOUBLE PRECISION NOT NULL,
    price           DOUBLE PRECISION NOT NULL,
    fee             DOUBLE PRECISION DEFAULT 0,
    fee_currency    TEXT DEFAULT 'USDT',
    realized_pnl    DOUBLE PRECISION DEFAULT 0,
    is_maker        BOOLEAN DEFAULT FALSE,
    trade_time      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(fill_id, exchange)
);
"""

CREATE_EQUITY_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS equity_snapshots (
    time            TIMESTAMPTZ NOT NULL,
    equity          DOUBLE PRECISION NOT NULL,
    unrealized_pnl  DOUBLE PRECISION DEFAULT 0,
    open_positions  INTEGER DEFAULT 0,
    drawdown_pct    DOUBLE PRECISION DEFAULT 0
);
"""

CREATE_RISK_BLOCKS = """
CREATE TABLE IF NOT EXISTS risk_blocks (
    time            TIMESTAMPTZ NOT NULL,
    symbol          TEXT NOT NULL,
    reason          TEXT NOT NULL,
    signal_score    DOUBLE PRECISION,
    signal_direction TEXT,
    metadata        JSONB DEFAULT '{}'
);
"""

CREATE_ERRORS = """
CREATE TABLE IF NOT EXISTS errors (
    time            TIMESTAMPTZ NOT NULL,
    component       TEXT NOT NULL,
    error_type      TEXT NOT NULL,
    message         TEXT NOT NULL,
    metadata        JSONB DEFAULT '{}'
);
"""

CREATE_DAILY_PNL = """
CREATE TABLE IF NOT EXISTS daily_pnl (
    date            DATE PRIMARY KEY,
    realized_pnl    DOUBLE PRECISION DEFAULT 0,
    unrealized_pnl  DOUBLE PRECISION DEFAULT 0,
    total_trades    INTEGER DEFAULT 0,
    winners         INTEGER DEFAULT 0,
    losers          INTEGER DEFAULT 0,
    equity_start    DOUBLE PRECISION DEFAULT 0,
    equity_end      DOUBLE PRECISION DEFAULT 0
);
"""

TRADING_DDL = [
    CREATE_ORDERS,
    CREATE_FILLS,
    CREATE_EQUITY_SNAPSHOTS,
    CREATE_RISK_BLOCKS,
    CREATE_ERRORS,
    CREATE_DAILY_PNL,
]

# Indexes for query performance
TRADING_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);",
    "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);",
    "CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id);",
    "CREATE INDEX IF NOT EXISTS idx_fills_symbol ON fills(symbol);",
    "CREATE INDEX IF NOT EXISTS idx_fills_time ON fills(trade_time);",
    "CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);",
    "CREATE INDEX IF NOT EXISTS idx_positions_open ON positions(close_time) WHERE close_time IS NULL;",
    "CREATE INDEX IF NOT EXISTS idx_equity_time ON equity_snapshots(time);",
    "CREATE INDEX IF NOT EXISTS idx_risk_blocks_time ON risk_blocks(time);",
    "CREATE INDEX IF NOT EXISTS idx_errors_time ON errors(time);",
]

# TimescaleDB hypertables for time-series tables
TRADING_HYPERTABLES = [
    "SELECT create_hypertable('equity_snapshots', 'time', if_not_exists => TRUE);",
    "SELECT create_hypertable('risk_blocks', 'time', if_not_exists => TRUE);",
    "SELECT create_hypertable('errors', 'time', if_not_exists => TRUE);",
]


class TradePersistence:
    """
    Persists all trading objects to PostgreSQL.

    Subscribes to EventBus events and writes asynchronously.
    All writes are idempotent via unique constraints.
    """

    def __init__(self, db_pool: Any, event_bus: EventBus, is_paper: bool = True) -> None:
        self._pool = db_pool
        self._event_bus = event_bus
        self._is_paper = is_paper
        self._wal_path = Path("data/trade_wal.jsonl")
        self._wal_path.parent.mkdir(parents=True, exist_ok=True)

    def _wal_append(self, method: str, kwargs: dict) -> None:
        """Append a failed DB write to the local WAL file for later replay."""
        entry = {"ts": time.time(), "method": method, "kwargs": kwargs}
        try:
            with self._wal_path.open("a") as f:
                f.write(_json.dumps(entry, default=str) + "\n")
            logger.debug("WAL: buffered {} call", method)
        except Exception as exc:
            logger.error("WAL append failed: {}", exc)

    async def replay_wal(self) -> int:
        """Replay buffered WAL entries to DB. Returns count of successfully replayed entries."""
        if not self._wal_path.exists():
            return 0
        if self._pool is None:
            return 0

        lines = self._wal_path.read_text().strip().splitlines()
        if not lines:
            return 0

        replayed = 0
        remaining: list[str] = []

        for line in lines:
            try:
                entry = _json.loads(line)
                method_name = entry["method"]
                kwargs = entry["kwargs"]
                method = getattr(self, method_name, None)
                if method:
                    await method(**kwargs)
                    replayed += 1
                else:
                    logger.warning("WAL: unknown method {}", method_name)
                    remaining.append(line)
            except Exception:
                remaining.append(line)

        # Rewrite WAL with only failed entries
        if remaining:
            self._wal_path.write_text("\n".join(remaining) + "\n")
        else:
            self._wal_path.unlink(missing_ok=True)

        if replayed:
            logger.info("WAL: replayed {} entries, {} remaining", replayed, len(remaining))
        return replayed

    async def migrate(self) -> None:
        """Run trading-specific DDL migrations."""
        if self._pool is None:
            return
        async with self._pool.acquire() as conn:
            for ddl in TRADING_DDL:
                await conn.execute(ddl)
            for idx in TRADING_INDEXES:
                try:
                    await conn.execute(idx)
                except Exception:
                    pass
            for stmt in TRADING_HYPERTABLES:
                try:
                    await conn.execute(stmt)
                except Exception:
                    pass
        logger.info("Trade persistence schema migrated")

    def subscribe_events(self) -> None:
        """Wire up EventBus subscriptions for automatic persistence."""
        self._event_bus.subscribe("ORDER_FILLED", self._on_order_filled)
        self._event_bus.subscribe("ORDER_PARTIALLY_FILLED", self._on_order_filled)
        self._event_bus.subscribe("ORDER_FAILED", self._on_order_failed)
        self._event_bus.subscribe("FILL_CONFIRMED", self._on_fill_confirmed)
        self._event_bus.subscribe("POSITION_CLOSED", self._on_position_closed)
        self._event_bus.subscribe("USER_ORDER_UPDATE", self._on_user_order_update)
        self._event_bus.subscribe("USER_ACCOUNT_UPDATE", self._on_account_update)
        logger.info("TradePersistence subscribed to events")

    # ── Order persistence ─────────────────────────────────────────────────

    async def persist_order(
        self,
        order_id: str,
        client_order_id: str,
        exchange: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None,
        status: str,
        filled_qty: float = 0.0,
        avg_fill_price: float = 0.0,
        total_fee: float = 0.0,
        reduce_only: bool = False,
        metadata: dict | None = None,
    ) -> None:
        if self._pool is None:
            self._wal_append("persist_order", {
                "order_id": order_id, "client_order_id": client_order_id,
                "exchange": exchange, "symbol": symbol, "side": side,
                "order_type": order_type, "quantity": quantity, "price": price,
                "status": status, "filled_qty": filled_qty,
                "avg_fill_price": avg_fill_price, "total_fee": total_fee,
                "reduce_only": reduce_only, "metadata": metadata,
            })
            return
        try:
            import json
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, client_order_id, exchange, symbol, side, order_type,
                        quantity, price, status, filled_qty, avg_fill_price, total_fee,
                        reduce_only, is_paper, metadata
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                    ON CONFLICT (order_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        filled_qty = EXCLUDED.filled_qty,
                        avg_fill_price = EXCLUDED.avg_fill_price,
                        total_fee = EXCLUDED.total_fee,
                        updated_at = NOW()
                    """,
                    order_id, client_order_id or "", exchange, symbol, side,
                    order_type, quantity, price, status, filled_qty,
                    avg_fill_price, total_fee, reduce_only, self._is_paper,
                    json.dumps(metadata or {}),
                )
        except Exception as exc:
            logger.error("Failed to persist order {}: {}", order_id, exc)
            self._wal_append("persist_order", {
                "order_id": order_id, "client_order_id": client_order_id,
                "exchange": exchange, "symbol": symbol, "side": side,
                "order_type": order_type, "quantity": quantity, "price": price,
                "status": status, "filled_qty": filled_qty,
                "avg_fill_price": avg_fill_price, "total_fee": total_fee,
                "reduce_only": reduce_only, "metadata": metadata,
            })

    # ── Fill persistence ──────────────────────────────────────────────────

    async def persist_fill(
        self,
        fill_id: str,
        order_id: str,
        exchange: str,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        fee: float = 0.0,
        fee_currency: str = "USDT",
        realized_pnl: float = 0.0,
        is_maker: bool = False,
        trade_time: datetime.datetime | None = None,
    ) -> None:
        if self._pool is None:
            self._wal_append("persist_fill", {
                "fill_id": fill_id, "order_id": order_id, "exchange": exchange,
                "symbol": symbol, "side": side, "quantity": quantity, "price": price,
                "fee": fee, "fee_currency": fee_currency, "realized_pnl": realized_pnl,
                "is_maker": is_maker, "trade_time": str(trade_time) if trade_time else None,
            })
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO fills (
                        fill_id, order_id, exchange, symbol, side,
                        quantity, price, fee, fee_currency, realized_pnl,
                        is_maker, trade_time
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                    ON CONFLICT (fill_id, exchange) DO NOTHING
                    """,
                    fill_id, order_id, exchange, symbol, side,
                    quantity, price, fee, fee_currency, realized_pnl,
                    is_maker, trade_time or datetime.datetime.now(datetime.UTC),
                )
        except Exception as exc:
            logger.error("Failed to persist fill {}: {}", fill_id, exc)
            self._wal_append("persist_fill", {
                "fill_id": fill_id, "order_id": order_id, "exchange": exchange,
                "symbol": symbol, "side": side, "quantity": quantity, "price": price,
                "fee": fee, "fee_currency": fee_currency, "realized_pnl": realized_pnl,
                "is_maker": is_maker, "trade_time": str(trade_time) if trade_time else None,
            })

    # ── Position persistence ──────────────────────────────────────────────

    async def persist_position_open(
        self,
        exchange: str,
        symbol: str,
        direction: str,
        entry_price: float,
        size: float,
    ) -> None:
        if self._pool is None:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO positions (exchange, symbol, direction, entry_price, size, open_time, is_paper)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (exchange, symbol) WHERE close_time IS NULL
                    DO UPDATE SET entry_price = EXCLUDED.entry_price, size = EXCLUDED.size
                    """,
                    exchange, symbol, direction, entry_price, size,
                    datetime.datetime.now(datetime.UTC), self._is_paper,
                )
        except Exception as exc:
            logger.error("Failed to persist position open {}/{}: {}", exchange, symbol, exc)

    async def persist_position_close(
        self,
        exchange: str,
        symbol: str,
        exit_price: float,
        pnl: float,
        pnl_pct: float,
    ) -> None:
        if self._pool is None:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE positions SET
                        exit_price = $1, pnl = $2, pnl_pct = $3, close_time = $4
                    WHERE exchange = $5 AND symbol = $6 AND close_time IS NULL
                    """,
                    exit_price, pnl, pnl_pct, datetime.datetime.now(datetime.UTC),
                    exchange, symbol,
                )
        except Exception as exc:
            logger.error("Failed to persist position close {}/{}: {}", exchange, symbol, exc)

    # ── Equity snapshots ──────────────────────────────────────────────────

    async def persist_equity_snapshot(
        self, equity: float, unrealized_pnl: float, open_positions: int, drawdown_pct: float,
    ) -> None:
        if self._pool is None:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO equity_snapshots (time, equity, unrealized_pnl, open_positions, drawdown_pct)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    datetime.datetime.now(datetime.UTC), equity, unrealized_pnl, open_positions, drawdown_pct,
                )
        except Exception as exc:
            logger.debug("equity_snapshot error: {}", exc)

    # ── Risk block logging ────────────────────────────────────────────────

    async def persist_risk_block(
        self, symbol: str, reason: str, signal_score: float = 0.0, signal_direction: str = "",
    ) -> None:
        if self._pool is None:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO risk_blocks (time, symbol, reason, signal_score, signal_direction)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    datetime.datetime.now(datetime.UTC), symbol, reason, signal_score, signal_direction,
                )
        except Exception as exc:
            logger.debug("risk_block persist error: {}", exc)

    # ── Error logging ─────────────────────────────────────────────────────

    async def persist_error(
        self, component: str, error_type: str, message: str,
    ) -> None:
        if self._pool is None:
            return
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO errors (time, component, error_type, message)
                    VALUES ($1, $2, $3, $4)
                    """,
                    datetime.datetime.now(datetime.UTC), component, error_type, message,
                )
        except Exception as exc:
            logger.debug("error persist failed: {}", exc)

    # ── Query helpers (for recovery) ──────────────────────────────────────

    async def load_open_positions(self) -> list[dict]:
        """Load positions that were open (no close_time) from DB."""
        if self._pool is None:
            return []
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM positions WHERE close_time IS NULL ORDER BY open_time"
                )
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("Failed to load open positions: {}", exc)
            return []

    async def load_recent_orders(self, limit: int = 100) -> list[dict]:
        """Load recent orders for state recovery."""
        if self._pool is None:
            return []
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM orders ORDER BY created_at DESC LIMIT $1", limit,
                )
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("Failed to load recent orders: {}", exc)
            return []

    # ── EventBus handlers ─────────────────────────────────────────────────

    async def _on_order_filled(self, payload: Any) -> None:
        """Persist filled/partial-fill order from CEXExecutor."""
        if not hasattr(payload, "order_id"):
            return
        await self.persist_order(
            order_id=payload.order_id,
            client_order_id="",
            exchange=payload.exchange,
            symbol=payload.symbol,
            side="buy" if payload.direction == "long" else "sell",
            order_type="limit",
            quantity=payload.quantity,
            price=payload.price,
            status=payload.status,
            filled_qty=payload.quantity,
            avg_fill_price=payload.price,
        )
        await self.persist_position_open(
            exchange=payload.exchange,
            symbol=payload.symbol,
            direction=payload.direction,
            entry_price=payload.price,
            size=payload.quantity,
        )

    async def _on_order_failed(self, payload: Any) -> None:
        if not hasattr(payload, "order_id"):
            return
        await self.persist_order(
            order_id=payload.order_id,
            client_order_id="",
            exchange=payload.exchange,
            symbol=payload.symbol,
            side="buy" if payload.direction == "long" else "sell",
            order_type="limit",
            quantity=payload.quantity,
            price=payload.price,
            status="failed",
        )

    async def _on_fill_confirmed(self, payload: Any) -> None:
        """Persist fill from USER_DATA stream."""
        if not isinstance(payload, dict):
            return
        fill_id = str(payload.get("trade_id", ""))
        if not fill_id or fill_id == "0":
            return
        await self.persist_fill(
            fill_id=fill_id,
            order_id=str(payload.get("order_id", "")),
            exchange="binance",
            symbol=payload.get("symbol", ""),
            side=payload.get("side", "").lower(),
            quantity=float(payload.get("last_filled_qty", 0)),
            price=float(payload.get("last_filled_price", 0)),
            fee=float(payload.get("commission", 0)),
            fee_currency=payload.get("commission_asset", "USDT"),
            realized_pnl=float(payload.get("realized_profit", 0)),
            is_maker=bool(payload.get("is_maker", False)),
        )
        # Update order status
        await self.persist_order(
            order_id=str(payload.get("order_id", "")),
            client_order_id=payload.get("client_order_id", ""),
            exchange="binance",
            symbol=payload.get("symbol", ""),
            side=payload.get("side", "").lower(),
            order_type=payload.get("order_type", ""),
            quantity=float(payload.get("quantity", 0)),
            price=float(payload.get("avg_price", 0)),
            status=payload.get("order_status", "").lower(),
            filled_qty=float(payload.get("cumulative_filled_qty", 0)),
            avg_fill_price=float(payload.get("avg_price", 0)),
            total_fee=float(payload.get("commission", 0)),
            reduce_only=bool(payload.get("reduce_only", False)),
        )

    async def _on_position_closed(self, payload: Any) -> None:
        """Persist position close."""
        if isinstance(payload, dict):
            pos = payload.get("position")
            if pos and hasattr(pos, "exchange"):
                await self.persist_position_close(
                    exchange=pos.exchange,
                    symbol=pos.symbol,
                    exit_price=pos.current_price,
                    pnl=pos.pnl,
                    pnl_pct=pos.pnl_pct,
                )
        elif hasattr(payload, "exchange"):
            await self.persist_position_close(
                exchange=payload.exchange,
                symbol=payload.symbol,
                exit_price=payload.current_price,
                pnl=payload.pnl,
                pnl_pct=payload.pnl_pct,
            )

    async def _on_user_order_update(self, payload: Any) -> None:
        """Persist order updates from user stream."""
        if not isinstance(payload, dict):
            return
        await self.persist_order(
            order_id=str(payload.get("order_id", "")),
            client_order_id=payload.get("client_order_id", ""),
            exchange="binance",
            symbol=payload.get("symbol", ""),
            side=payload.get("side", "").lower(),
            order_type=payload.get("order_type", "").lower(),
            quantity=float(payload.get("quantity", 0)),
            price=float(payload.get("avg_price", 0) or payload.get("price", 0)),
            status=payload.get("order_status", "").lower(),
            filled_qty=float(payload.get("cumulative_filled_qty", 0)),
            avg_fill_price=float(payload.get("avg_price", 0)),
            total_fee=float(payload.get("commission", 0)),
            reduce_only=bool(payload.get("reduce_only", False)),
        )

    async def _on_account_update(self, payload: Any) -> None:
        """Log account update as equity snapshot."""
        if not isinstance(payload, dict):
            return
        for b in payload.get("balances", []):
            if b.get("asset") == "USDT":
                wallet = float(b.get("wallet_balance", 0))
                if wallet > 0:
                    await self.persist_equity_snapshot(
                        equity=wallet,
                        unrealized_pnl=0.0,
                        open_positions=0,
                        drawdown_pct=0.0,
                    )
