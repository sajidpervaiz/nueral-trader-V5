"""SQLAlchemy table metadata for neural-trader-5.

This module is the **single source of truth** for the PostgreSQL schema.
Alembic auto-generate reads ``metadata`` from here; raw ``asyncpg`` code
references column names that must match these definitions.

Design notes
------------
* All tables use ``TIMESTAMPTZ`` for temporal columns.
* ``correlation_id`` links every audit row back to the originating request.
* Unique constraints enforce idempotent inserts (order_id, fill_id, etc.).
* JSONB ``metadata`` columns carry extensible context without schema changes.
"""
from __future__ import annotations

import sqlalchemy as sa

metadata = sa.MetaData()

# ── Market-data tables ────────────────────────────────────────────────────

ticks = sa.Table(
    "ticks",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("price", sa.Float, nullable=False),
    sa.Column("volume", sa.Float, nullable=False),
    sa.Column("side", sa.Text),
)

candles = sa.Table(
    "candles",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("timeframe", sa.Text, nullable=False),
    sa.Column("open", sa.Float, nullable=False),
    sa.Column("high", sa.Float, nullable=False),
    sa.Column("low", sa.Float, nullable=False),
    sa.Column("close", sa.Float, nullable=False),
    sa.Column("volume", sa.Float, nullable=False),
    sa.Column("num_trades", sa.Integer, server_default="0"),
)

funding_rates = sa.Table(
    "funding_rates",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("funding_rate", sa.Float, nullable=False),
    sa.Column("predicted_rate", sa.Float),
)

open_interest = sa.Table(
    "open_interest",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("oi_usd", sa.Float, nullable=False),
    sa.Column("oi_contracts", sa.Float, nullable=False),
    sa.Column("oi_change_24h", sa.Float, server_default="0"),
)

# ── Trading tables ────────────────────────────────────────────────────────

positions = sa.Table(
    "positions",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("direction", sa.Text, nullable=False),
    sa.Column("entry_price", sa.Float, nullable=False),
    sa.Column("exit_price", sa.Float),
    sa.Column("size", sa.Float, nullable=False),
    sa.Column("pnl", sa.Float),
    sa.Column("pnl_pct", sa.Float),
    sa.Column("open_time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("close_time", sa.DateTime(timezone=True)),
    sa.Column("is_paper", sa.Boolean, server_default="true"),
    sa.Column("correlation_id", sa.Text),
    sa.UniqueConstraint("exchange", "symbol", name="idx_positions_open_unique"),
)

orders = sa.Table(
    "orders",
    metadata,
    sa.Column("order_id", sa.Text, primary_key=True),
    sa.Column("client_order_id", sa.Text, unique=True),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("side", sa.Text, nullable=False),
    sa.Column("order_type", sa.Text, nullable=False),
    sa.Column("quantity", sa.Float, nullable=False),
    sa.Column("price", sa.Float),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("filled_qty", sa.Float, server_default="0"),
    sa.Column("avg_fill_price", sa.Float, server_default="0"),
    sa.Column("total_fee", sa.Float, server_default="0"),
    sa.Column("reduce_only", sa.Boolean, server_default="false"),
    sa.Column("is_paper", sa.Boolean, server_default="true"),
    sa.Column("correlation_id", sa.Text),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    sa.Column("metadata", sa.JSON, server_default="{}"),
)

fills = sa.Table(
    "fills",
    metadata,
    sa.Column("fill_id", sa.Text, nullable=False),
    sa.Column("order_id", sa.Text, nullable=False),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("side", sa.Text, nullable=False),
    sa.Column("quantity", sa.Float, nullable=False),
    sa.Column("price", sa.Float, nullable=False),
    sa.Column("fee", sa.Float, server_default="0"),
    sa.Column("fee_currency", sa.Text, server_default="'USDT'"),
    sa.Column("realized_pnl", sa.Float, server_default="0"),
    sa.Column("is_maker", sa.Boolean, server_default="false"),
    sa.Column("correlation_id", sa.Text),
    sa.Column("trade_time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    sa.UniqueConstraint("fill_id", "exchange", name="fills_fill_id_exchange_key"),
)

# ── Signal events ─────────────────────────────────────────────────────────

signal_events = sa.Table(
    "signal_events",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("time", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    sa.Column("correlation_id", sa.Text),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("direction", sa.Text, nullable=False),
    sa.Column("score", sa.Float, nullable=False),
    sa.Column("technical_score", sa.Float, server_default="0"),
    sa.Column("ml_score", sa.Float, server_default="0"),
    sa.Column("sentiment_score", sa.Float, server_default="0"),
    sa.Column("macro_score", sa.Float, server_default="0"),
    sa.Column("news_score", sa.Float, server_default="0"),
    sa.Column("orderbook_score", sa.Float, server_default="0"),
    sa.Column("regime", sa.Text),
    sa.Column("regime_confidence", sa.Float, server_default="0"),
    sa.Column("price", sa.Float, nullable=False),
    sa.Column("atr", sa.Float),
    sa.Column("stop_loss", sa.Float),
    sa.Column("take_profit", sa.Float),
    sa.Column("factors_active", sa.Integer, server_default="0"),
    sa.Column("acted_on", sa.Boolean, server_default="false"),
    sa.Column("metadata", sa.JSON, server_default="{}"),
)

# ── Risk decisions ────────────────────────────────────────────────────────

risk_decisions = sa.Table(
    "risk_decisions",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("time", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    sa.Column("correlation_id", sa.Text),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("decision", sa.Text, nullable=False),
    sa.Column("reason", sa.Text, nullable=False),
    sa.Column("signal_score", sa.Float, server_default="0"),
    sa.Column("signal_direction", sa.Text),
    sa.Column("portfolio_heat", sa.Float, server_default="0"),
    sa.Column("drawdown_pct", sa.Float, server_default="0"),
    sa.Column("metadata", sa.JSON, server_default="{}"),
)

# ── Risk blocks (existing, but now with correlation_id) ───────────────────

risk_blocks = sa.Table(
    "risk_blocks",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("reason", sa.Text, nullable=False),
    sa.Column("signal_score", sa.Float),
    sa.Column("signal_direction", sa.Text),
    sa.Column("correlation_id", sa.Text),
    sa.Column("metadata", sa.JSON, server_default="{}"),
)

# ── Equity snapshots ──────────────────────────────────────────────────────

equity_snapshots = sa.Table(
    "equity_snapshots",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("equity", sa.Float, nullable=False),
    sa.Column("unrealized_pnl", sa.Float, server_default="0"),
    sa.Column("open_positions", sa.Integer, server_default="0"),
    sa.Column("drawdown_pct", sa.Float, server_default="0"),
    sa.Column("correlation_id", sa.Text),
)

# ── PnL snapshots ─────────────────────────────────────────────────────────

pnl_snapshots = sa.Table(
    "pnl_snapshots",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("time", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text),
    sa.Column("realized_pnl", sa.Float, server_default="0"),
    sa.Column("unrealized_pnl", sa.Float, server_default="0"),
    sa.Column("cumulative_pnl", sa.Float, server_default="0"),
    sa.Column("equity", sa.Float, server_default="0"),
    sa.Column("drawdown_pct", sa.Float, server_default="0"),
    sa.Column("metadata", sa.JSON, server_default="{}"),
)

# ── Daily P&L ─────────────────────────────────────────────────────────────

daily_pnl = sa.Table(
    "daily_pnl",
    metadata,
    sa.Column("date", sa.Date, primary_key=True),
    sa.Column("realized_pnl", sa.Float, server_default="0"),
    sa.Column("unrealized_pnl", sa.Float, server_default="0"),
    sa.Column("total_trades", sa.Integer, server_default="0"),
    sa.Column("winners", sa.Integer, server_default="0"),
    sa.Column("losers", sa.Integer, server_default="0"),
    sa.Column("equity_start", sa.Float, server_default="0"),
    sa.Column("equity_end", sa.Float, server_default="0"),
)

# ── User stream events ────────────────────────────────────────────────────

user_stream_events = sa.Table(
    "user_stream_events",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("time", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    sa.Column("event_type", sa.Text, nullable=False),
    sa.Column("exchange", sa.Text, nullable=False, server_default="'binance'"),
    sa.Column("correlation_id", sa.Text),
    sa.Column("raw_payload", sa.JSON, nullable=False),
    sa.Column("processed", sa.Boolean, server_default="false"),
)

# ── Reconciliation events ─────────────────────────────────────────────────

reconciliation_events = sa.Table(
    "reconciliation_events",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("reconciliation_id", sa.Text, nullable=False, unique=True),
    sa.Column("time", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    sa.Column("success", sa.Boolean, nullable=False),
    sa.Column("safe_mode", sa.Boolean, server_default="false"),
    sa.Column("exchange_positions", sa.Integer, server_default="0"),
    sa.Column("db_positions", sa.Integer, server_default="0"),
    sa.Column("open_orders", sa.Integer, server_default="0"),
    sa.Column("mismatches", sa.JSON, server_default="[]"),
    sa.Column("positions_without_sl", sa.JSON, server_default="[]"),
    sa.Column("actions_taken", sa.JSON, server_default="[]"),
    sa.Column("balance", sa.Float, server_default="0"),
    sa.Column("leverage_settings", sa.JSON, server_default="{}"),
)

# ── Error log ─────────────────────────────────────────────────────────────

errors = sa.Table(
    "errors",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("time", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    sa.Column("component", sa.Text, nullable=False),
    sa.Column("error_type", sa.Text, nullable=False),
    sa.Column("message", sa.Text, nullable=False),
    sa.Column("correlation_id", sa.Text),
    sa.Column("stack_trace", sa.Text),
    sa.Column("metadata", sa.JSON, server_default="{}"),
)

# ── Signals table (existing market-data level, kept for backward compat) ──

signals = sa.Table(
    "signals",
    metadata,
    sa.Column("time", sa.DateTime(timezone=True), nullable=False),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("direction", sa.Text, nullable=False),
    sa.Column("score", sa.Float, nullable=False),
    sa.Column("price", sa.Float, nullable=False),
    sa.Column("stop_loss", sa.Float),
    sa.Column("take_profit", sa.Float),
    sa.Column("regime", sa.Text),
    sa.Column("is_paper", sa.Boolean, server_default="true"),
)
