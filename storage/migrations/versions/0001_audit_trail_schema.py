"""Initial audit-trail schema — signal_events, risk_decisions, user_stream_events,
reconciliation_events, pnl_snapshots, plus correlation_id on existing tables.

Revision ID: 0001
Revises: None
Create Date: 2026-04-08
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── New tables ────────────────────────────────────────────────────────

    op.create_table(
        "signal_events",
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

    op.create_table(
        "risk_decisions",
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

    op.create_table(
        "user_stream_events",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("time", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("event_type", sa.Text, nullable=False),
        sa.Column("exchange", sa.Text, nullable=False, server_default="'binance'"),
        sa.Column("correlation_id", sa.Text),
        sa.Column("raw_payload", sa.JSON, nullable=False),
        sa.Column("processed", sa.Boolean, server_default="false"),
    )

    op.create_table(
        "reconciliation_events",
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

    op.create_table(
        "pnl_snapshots",
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

    # ── Add correlation_id to existing tables (safe ALTER IF NOT EXISTS) ──
    # We use raw SQL because Alembic doesn't natively support IF NOT EXISTS
    # for ADD COLUMN, and these tables may already exist from raw DDL.

    _add_column_safe("orders", "correlation_id", "TEXT")
    _add_column_safe("fills", "correlation_id", "TEXT")
    _add_column_safe("positions", "correlation_id", "TEXT")
    _add_column_safe("risk_blocks", "correlation_id", "TEXT")
    _add_column_safe("equity_snapshots", "correlation_id", "TEXT")
    _add_column_safe("errors", "correlation_id", "TEXT")
    _add_column_safe("errors", "stack_trace", "TEXT")

    # ── Performance indexes ───────────────────────────────────────────────

    op.create_index("idx_signal_events_time", "signal_events", ["time"])
    op.create_index("idx_signal_events_symbol", "signal_events", ["exchange", "symbol"])
    op.create_index("idx_signal_events_corr", "signal_events", ["correlation_id"])

    op.create_index("idx_risk_decisions_time", "risk_decisions", ["time"])
    op.create_index("idx_risk_decisions_symbol", "risk_decisions", ["exchange", "symbol"])
    op.create_index("idx_risk_decisions_corr", "risk_decisions", ["correlation_id"])

    op.create_index("idx_user_stream_time", "user_stream_events", ["time"])
    op.create_index("idx_user_stream_type", "user_stream_events", ["event_type"])
    op.create_index("idx_user_stream_corr", "user_stream_events", ["correlation_id"])

    op.create_index("idx_recon_time", "reconciliation_events", ["time"])

    op.create_index("idx_pnl_snapshots_time", "pnl_snapshots", ["time"])
    op.create_index("idx_pnl_snapshots_symbol", "pnl_snapshots", ["exchange", "symbol"])

    op.create_index("idx_orders_corr", "orders", ["correlation_id"])
    op.create_index("idx_fills_corr", "fills", ["correlation_id"])
    op.create_index("idx_errors_corr", "errors", ["correlation_id"])


def downgrade() -> None:
    op.drop_table("pnl_snapshots")
    op.drop_table("reconciliation_events")
    op.drop_table("user_stream_events")
    op.drop_table("risk_decisions")
    op.drop_table("signal_events")

    # Remove correlation_id columns
    for tbl in ("orders", "fills", "positions", "risk_blocks", "equity_snapshots"):
        op.drop_column(tbl, "correlation_id")
    op.drop_column("errors", "correlation_id")
    op.drop_column("errors", "stack_trace")


def _add_column_safe(table: str, column: str, col_type: str) -> None:
    """Add column if it doesn't already exist (PostgreSQL-specific)."""
    op.execute(
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}"
    )
