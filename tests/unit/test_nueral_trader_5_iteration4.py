"""
Iteration 4 proof tests — non-negotiable live-production requirements.

Tests prove:
1. Crash test: exchange-side SL/TP orders exist after entry fill
2. Restart test: reconciliation rebuilds state from exchange
3. DB recovery test: WAL replay restores persisted state
4. Fill tracking test: USER_DATA stream updates are processed correctly
5. Kill switch: directly cancels exchange orders (no event-bus race)
6. Idempotency: duplicate fills are rejected
"""
from __future__ import annotations

import asyncio
import json
import tempfile
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.config import Config
from core.event_bus import EventBus
from execution.exchange_order_placer import ExchangeOrderPlacer, ProtectiveOrders
from execution.reconciliation import StartupReconciler, ReconciliationResult
from execution.risk_manager import RiskManager, Position
from storage.trade_persistence import TradePersistence


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_config(**overrides: Any) -> Config:
    cfg = Config.__new__(Config)
    cfg._data = {"risk": {"initial_equity": 100_000}, "exchanges": {}, "system": {"paper_mode": True}}
    for k, v in overrides.items():
        if k == "paper_mode":
            cfg._data["system"]["paper_mode"] = v
        else:
            setattr(cfg, k, v)
    return cfg


def _make_risk_manager(config: Config | None = None, event_bus: EventBus | None = None) -> RiskManager:
    return RiskManager(config or _make_config(), event_bus or EventBus())


# ═══════════════════════════════════════════════════════════════════════════════
# 1. CRASH TEST — Exchange-side SL/TP exist after entry fill
# ═══════════════════════════════════════════════════════════════════════════════

class TestExchangeSideSLTPAfterFill:
    """Prove: after an entry fill, STOP_MARKET (SL) and TAKE_PROFIT_MARKET (TP)
    orders are placed on the exchange.  Even if TP fails, SL must exist."""

    @pytest.mark.asyncio
    async def test_sl_and_tp_placed_after_fill(self) -> None:
        """Both SL and TP placed with correct parameters."""
        client = AsyncMock()
        client.create_order = AsyncMock(side_effect=[
            {"id": "sl-001", "status": "NEW"},   # SL
            {"id": "tp-001", "status": "NEW"},   # TP
        ])

        placer = ExchangeOrderPlacer(client=client, working_type="CONTRACT_PRICE")
        prot = await placer.place_protective_orders(
            symbol="BTC/USDT:USDT",
            direction="long",
            quantity=0.1,
            entry_price=50000.0,
            sl_price=49000.0,
            tp_price=53000.0,
        )

        assert prot.sl_placed is True
        assert prot.tp_placed is True
        assert prot.sl_order_id == "sl-001"
        assert prot.tp_order_id == "tp-001"
        assert client.create_order.call_count == 2

        # Verify SL was placed FIRST (call order matters for crash safety)
        first_call = client.create_order.call_args_list[0]
        assert first_call.kwargs.get("type") == "STOP_MARKET" or "stopPrice" in str(first_call)

    @pytest.mark.asyncio
    async def test_sl_exists_even_if_tp_fails(self) -> None:
        """SL MUST be placed even if TP placement raises."""
        client = AsyncMock()
        client.create_order = AsyncMock(side_effect=[
            {"id": "sl-safe", "status": "NEW"},  # SL succeeds
            Exception("TP endpoint down"),         # TP fails
        ])

        placer = ExchangeOrderPlacer(client=client)
        prot = await placer.place_protective_orders(
            symbol="ETH/USDT:USDT",
            direction="short",
            quantity=1.0,
            entry_price=3000.0,
            sl_price=3100.0,
            tp_price=2800.0,
        )

        # SL must be placed regardless of TP failure
        assert prot.sl_placed is True
        assert prot.sl_order_id == "sl-safe"
        # TP failed but that's not fatal
        assert prot.tp_placed is False

    @pytest.mark.asyncio
    async def test_sl_failure_raises_for_circuit_breaker(self) -> None:
        """If even SL placement fails, the exception propagates (caller trips breaker)."""
        client = AsyncMock()
        client.create_order = AsyncMock(side_effect=Exception("exchange down"))

        placer = ExchangeOrderPlacer(client=client)
        with pytest.raises(Exception, match="exchange down"):
            await placer.place_protective_orders(
                symbol="BTC/USDT:USDT",
                direction="long",
                quantity=0.1,
                entry_price=50000.0,
                sl_price=49000.0,
                tp_price=53000.0,
            )

    @pytest.mark.asyncio
    async def test_oco_sl_fill_cancels_tp(self) -> None:
        """Manual OCO: SL fill cancels the TP order."""
        client = AsyncMock()
        client.create_order = AsyncMock(side_effect=[
            {"id": "sl-100", "status": "NEW"},
            {"id": "tp-100", "status": "NEW"},
        ])
        client.cancel_order = AsyncMock(return_value={"id": "tp-100"})

        placer = ExchangeOrderPlacer(client=client)
        await placer.place_protective_orders(
            symbol="BTC/USDT:USDT", direction="long",
            quantity=0.1, entry_price=50000, sl_price=49000, tp_price=53000,
        )

        await placer.handle_sl_filled("BTC/USDT:USDT")

        # TP should be cancelled after SL fill
        client.cancel_order.assert_called_once_with("tp-100", "BTC/USDT:USDT")

    @pytest.mark.asyncio
    async def test_oco_tp_fill_cancels_sl(self) -> None:
        """Manual OCO: TP fill cancels the SL order."""
        client = AsyncMock()
        client.create_order = AsyncMock(side_effect=[
            {"id": "sl-200", "status": "NEW"},
            {"id": "tp-200", "status": "NEW"},
        ])
        client.cancel_order = AsyncMock(return_value={"id": "sl-200"})

        placer = ExchangeOrderPlacer(client=client)
        await placer.place_protective_orders(
            symbol="ETH/USDT:USDT", direction="short",
            quantity=1.0, entry_price=3000, sl_price=3100, tp_price=2800,
        )

        await placer.handle_tp_filled("ETH/USDT:USDT")
        client.cancel_order.assert_called_once_with("sl-200", "ETH/USDT:USDT")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. RESTART TEST — Reconciliation rebuilds state from exchange
# ═══════════════════════════════════════════════════════════════════════════════

class TestStartupReconciliation:
    """Prove: on restart, reconciliation fetches exchange state and rebuilds
    local risk manager positions / equity."""

    @pytest.mark.asyncio
    async def test_reconciliation_rebuilds_positions_from_exchange(self) -> None:
        """Exchange has positions → risk manager rebuilt from exchange truth."""
        config = _make_config()
        bus = EventBus()
        rm = _make_risk_manager(config, bus)

        client = AsyncMock()
        client.fetch_balance = AsyncMock(return_value={
            "total": {"USDT": 50_000.0},
            "free": {"USDT": 45_000.0},
        })
        client.fetch_positions = AsyncMock(return_value=[
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": 0.5,
                "notional": 25000,
                "entryPrice": 50000,
                "unrealizedPnl": 500,
                "leverage": 10,
                "marginType": "cross",
                "liquidationPrice": 45000,
            },
        ])
        client.fetch_open_orders = AsyncMock(return_value=[
            {"id": "sl-1", "symbol": "BTC/USDT:USDT", "type": "stop_market", "side": "sell"},
            {"id": "tp-1", "symbol": "BTC/USDT:USDT", "type": "take_profit_market", "side": "sell"},
        ])

        reconciler = StartupReconciler(config, bus, rm, client)
        result = await reconciler.reconcile()

        assert result.success is True
        assert len(result.exchange_positions) == 1
        assert result.balance == 50_000.0
        # Equity updated to exchange truth
        assert rm._equity == 50_000.0

    @pytest.mark.asyncio
    async def test_reconciliation_enters_safe_mode_on_mismatch(self) -> None:
        """If local and exchange state disagree, safe mode is triggered."""
        config = _make_config()
        bus = EventBus()
        rm = _make_risk_manager(config, bus)
        # Pretend local equity is 200k but exchange says 50k → mismatch
        rm._equity = 200_000.0

        client = AsyncMock()
        client.fetch_balance = AsyncMock(return_value={
            "total": {"USDT": 50_000.0},
            "free": {"USDT": 45_000.0},
        })
        client.fetch_positions = AsyncMock(return_value=[])
        client.fetch_open_orders = AsyncMock(return_value=[])

        reconciler = StartupReconciler(config, bus, rm, client)
        result = await reconciler.reconcile()

        assert result.success is True
        # Large equity deviation should trigger mismatch
        assert len(result.mismatches) >= 1
        assert result.safe_mode is True
        assert reconciler.safe_mode is True
        # Circuit breaker tripped
        assert rm._circuit_breaker.tripped is True

    @pytest.mark.asyncio
    async def test_reconciliation_skipped_without_client(self) -> None:
        """No exchange client → reconciliation returns success (paper mode)."""
        config = _make_config()
        bus = EventBus()
        rm = _make_risk_manager(config, bus)

        reconciler = StartupReconciler(config, bus, rm, client=None)
        result = await reconciler.reconcile()

        assert result.success is True
        assert result.safe_mode is False


# ═══════════════════════════════════════════════════════════════════════════════
# 3. DB RECOVERY TEST — WAL replay restores persisted state
# ═══════════════════════════════════════════════════════════════════════════════

class TestWALRecovery:
    """Prove: when DB is down, writes go to WAL; on reconnect, replay succeeds."""

    @pytest.mark.asyncio
    async def test_wal_append_on_db_failure(self) -> None:
        """When pool is None, persist_order() writes to WAL file."""
        bus = EventBus()
        with tempfile.TemporaryDirectory() as tmpdir:
            tp = TradePersistence(db_pool=None, event_bus=bus, is_paper=True)
            wal_path = Path(tmpdir) / "trade_wal.jsonl"
            tp._wal_path = wal_path

            await tp.persist_order(
                order_id="ord-001",
                client_order_id="coid-001",
                exchange="binance",
                symbol="BTC/USDT",
                side="buy",
                order_type="market",
                quantity=0.1,
                price=50000.0,
                status="filled",
            )

            assert wal_path.exists()
            lines = wal_path.read_text().strip().splitlines()
            assert len(lines) == 1
            entry = json.loads(lines[0])
            assert entry["method"] == "persist_order"
            assert entry["kwargs"]["order_id"] == "ord-001"

    @pytest.mark.asyncio
    async def test_wal_replay_calls_correct_methods(self) -> None:
        """WAL replay invokes the correct persistence methods with stored args."""
        bus = EventBus()
        with tempfile.TemporaryDirectory() as tmpdir:
            tp = TradePersistence(db_pool=None, event_bus=bus, is_paper=True)
            wal_path = Path(tmpdir) / "trade_wal.jsonl"
            tp._wal_path = wal_path

            # Write 2 WAL entries
            await tp.persist_order(
                order_id="ord-A", client_order_id="cA", exchange="binance",
                symbol="BTC/USDT", side="buy", order_type="market",
                quantity=0.1, price=50000.0, status="filled",
            )
            await tp.persist_order(
                order_id="ord-B", client_order_id="cB", exchange="binance",
                symbol="ETH/USDT", side="sell", order_type="limit",
                quantity=1.0, price=3000.0, status="open",
            )

            assert len(wal_path.read_text().strip().splitlines()) == 2

            # Now "reconnect" the DB — mock the pool and persist_order
            mock_pool = AsyncMock()
            mock_conn = AsyncMock()
            mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
            tp._pool = mock_pool

            replayed = await tp.replay_wal()

            assert replayed == 2
            # WAL should be deleted after successful replay
            assert not wal_path.exists()

    @pytest.mark.asyncio
    async def test_wal_keeps_failed_entries_on_bad_format(self) -> None:
        """WAL entries with invalid JSON / unknown methods stay in the WAL file."""
        bus = EventBus()
        with tempfile.TemporaryDirectory() as tmpdir:
            tp = TradePersistence(db_pool=None, event_bus=bus, is_paper=True)
            wal_path = Path(tmpdir) / "trade_wal.jsonl"
            tp._wal_path = wal_path

            # Write a corrupt WAL entry (invalid JSON) + unknown method entry
            wal_path.write_text(
                '{bad json\n'
                '{"ts": 1, "method": "nonexistent_method", "kwargs": {}}\n'
            )

            # Even with a pool, these cannot be replayed
            mock_pool = AsyncMock()
            tp._pool = mock_pool

            replayed = await tp.replay_wal()

            assert replayed == 0
            assert wal_path.exists()
            remaining = wal_path.read_text().strip().splitlines()
            assert len(remaining) == 2


# ═══════════════════════════════════════════════════════════════════════════════
# 4. FILL TRACKING TEST — USER_DATA stream fills are processed
# ═══════════════════════════════════════════════════════════════════════════════

class TestFillTrackingViaUserDataStream:
    """Prove: USER_ORDER_UPDATE events from the websocket stream are routed
    to the executor, which calls order_manager.record_fill()."""

    @pytest.mark.asyncio
    async def test_user_order_update_triggers_record_fill(self) -> None:
        """Simulate a TRADE execution type that calls record_fill."""
        from execution.cex_executor import CEXExecutor

        config = _make_config()
        bus = EventBus()
        rm = _make_risk_manager(config, bus)

        mock_om = MagicMock()
        mock_om.record_fill = AsyncMock()

        executor = CEXExecutor.__new__(CEXExecutor)
        executor.exchange_id = "binance"
        executor.config = config
        executor.event_bus = bus
        executor.risk_manager = rm
        executor._order_manager = mock_om
        executor._order_placer = None
        executor._client = None
        executor._rate_limiter = None

        payload = {
            "symbol": "BTC/USDT:USDT",
            "execution_type": "TRADE",
            "order_status": "FILLED",
            "order_type": "MARKET",
            "reduce_only": False,
            "last_filled_qty": "0.1",
            "last_filled_price": "50500.0",
            "cumulative_filled_qty": "0.1",
            "quantity": "0.1",
            "commission": "0.025",
            "realized_profit": "0",
            "client_order_id": "coid-test-123",
            "trade_id": "fill-xyz-001",
        }

        await executor._handle_user_order_update(payload)

        mock_om.record_fill.assert_called_once_with(
            client_order_id="coid-test-123",
            fill_id="fill-xyz-001",
            quantity=0.1,
            price=50500.0,
            fee=0.025,
        )

    @pytest.mark.asyncio
    async def test_non_binance_executor_ignores_user_updates(self) -> None:
        """Non-Binance executors must skip USER_ORDER_UPDATE processing."""
        from execution.cex_executor import CEXExecutor

        config = _make_config()
        bus = EventBus()
        rm = _make_risk_manager(config, bus)
        mock_om = MagicMock()
        mock_om.record_fill = AsyncMock()

        executor = CEXExecutor.__new__(CEXExecutor)
        executor.exchange_id = "bybit"
        executor.config = config
        executor.event_bus = bus
        executor.risk_manager = rm
        executor._order_manager = mock_om
        executor._order_placer = None
        executor._client = None
        executor._rate_limiter = None

        await executor._handle_user_order_update({"execution_type": "TRADE", "symbol": "BTC"})

        mock_om.record_fill.assert_not_called()

    @pytest.mark.asyncio
    async def test_sl_fill_via_user_stream_triggers_oco_cancel(self) -> None:
        """When a STOP_MARKET reduce_only fill comes through user stream,
        the executor cancels the corresponding TP (OCO behaviour)."""
        from execution.cex_executor import CEXExecutor

        config = _make_config()
        bus = EventBus()
        rm = _make_risk_manager(config, bus)

        mock_placer = AsyncMock()
        mock_placer.handle_sl_filled = AsyncMock()
        mock_placer.remove_tracking = MagicMock()

        mock_om = MagicMock()
        mock_om.record_fill = AsyncMock()

        executor = CEXExecutor.__new__(CEXExecutor)
        executor.exchange_id = "binance"
        executor.config = config
        executor.event_bus = bus
        executor.risk_manager = rm
        executor._order_manager = mock_om
        executor._order_placer = mock_placer
        executor._client = None
        executor._rate_limiter = None

        # Add a fake position so _on_exchange_sl_filled has something to close
        pos = Position(
            exchange="binance", symbol="BTC/USDT:USDT", direction="long",
            size=0.1, entry_price=50000, current_price=49000,
            stop_loss=49000, take_profit=53000, open_time=int(time.time()),
        )
        rm._positions["binance:BTC/USDT:USDT"] = pos

        payload = {
            "symbol": "BTC/USDT:USDT",
            "execution_type": "TRADE",
            "order_status": "FILLED",
            "order_type": "STOP_MARKET",
            "reduce_only": True,
            "last_filled_qty": "0.1",
            "last_filled_price": "49000.0",
            "cumulative_filled_qty": "0.1",
            "quantity": "0.1",
            "commission": "0.02",
            "realized_profit": "-100",
            "client_order_id": "sl-coid",
            "trade_id": "fill-sl-001",
        }

        await executor._handle_user_order_update(payload)

        # SL fill should trigger OCO: cancel TP
        mock_placer.handle_sl_filled.assert_called_once_with("BTC/USDT:USDT")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. KILL SWITCH — Directly cancels exchange orders (no event-bus race)
# ═══════════════════════════════════════════════════════════════════════════════

class TestKillSwitchDirectCancellation:
    """Prove: kill switch endpoint directly calls exchange cancel methods,
    not just publishing an event for the executor to handle."""

    @pytest.mark.asyncio
    async def test_kill_switch_blocks_new_signals(self) -> None:
        """After kill switch, approve_signal returns False."""
        from engine.signal_generator import TradingSignal

        rm = _make_risk_manager()
        # Add a position so activate actually closes something
        pos = Position(
            exchange="binance", symbol="BTC/USDT:USDT", direction="long",
            size=0.1, entry_price=50000, current_price=50000,
            stop_loss=49000, take_profit=53000, open_time=int(time.time()),
        )
        rm._positions["BTC/USDT:USDT"] = pos

        closed = await rm.activate_kill_switch()
        assert len(closed) == 1
        assert rm._killed is True

        sig = TradingSignal(
            exchange="binance", symbol="ETH/USDT:USDT",
            direction="long", score=0.9,
            technical_score=0.8, ml_score=0.7, sentiment_score=0.5,
            macro_score=0.5, news_score=0.5, orderbook_score=0.5,
            regime="trending", regime_confidence=0.9,
            price=3000.0, atr=50.0,
            stop_loss=2900.0, take_profit=3200.0,
            timestamp=int(time.time()),
        )
        approved, reason, _ = rm.approve_signal(sig)
        assert approved is False
        assert "kill_switch" in reason

    @pytest.mark.asyncio
    async def test_kill_switch_api_cancels_orders_directly(self) -> None:
        """The /v1/kill endpoint must cancel orders directly via executors,
        not just rely on event bus."""
        from interface.dashboard_api import build_app

        config = _make_config(paper_mode=False)
        # Override monitoring config to disable auth for test
        config._data["monitoring"] = {
            "dashboard_api": {"auth": {"require_api_key": False, "allow_unauthenticated_non_paper": True}}
        }
        bus = EventBus()
        rm = _make_risk_manager(config, bus)

        # Mock executor with client
        mock_client = AsyncMock()
        mock_client.fetch_open_orders = AsyncMock(return_value=[
            {"id": "ord-1", "symbol": "BTC/USDT:USDT"},
            {"id": "ord-2", "symbol": "ETH/USDT:USDT"},
        ])
        mock_client.cancel_order = AsyncMock()

        mock_rl = AsyncMock()
        mock_rl.acquire = AsyncMock()

        mock_executor = MagicMock()
        mock_executor._client = mock_client
        mock_executor._rate_limiter = mock_rl
        mock_executor.exchange_id = "binance"
        mock_executor._order_placer = None

        app = build_app(
            config, bus, rm,
            executors=[mock_executor],
        )

        if app is None:
            pytest.skip("FastAPI not installed")

        from starlette.testclient import TestClient
        with TestClient(app) as client:
            resp = client.post("/v1/kill")
            assert resp.status_code == 200
            data = resp.json()
            assert data["success"] is True
            assert data["kill_switch_active"] is True
            # Orders were directly cancelled
            assert data["orders_cancelled"] == 2
            assert mock_client.cancel_order.call_count == 2


# ═══════════════════════════════════════════════════════════════════════════════
# 6. IDEMPOTENCY — Duplicate fills rejected
# ═══════════════════════════════════════════════════════════════════════════════

class TestFillIdempotency:
    """Prove: duplicate fill_id values are rejected by OrderManager.record_fill."""

    @pytest.mark.asyncio
    async def test_duplicate_fill_id_rejected(self) -> None:
        """Same fill_id must not be recorded twice."""
        from execution.order_manager import OrderManager, Order, OrderStatus, OrderSide, OrderType
        from core.circuit_breaker import CircuitBreaker as AppCircuitBreaker

        config = _make_config()
        bus = EventBus()
        cb = AppCircuitBreaker()
        om = OrderManager(config, bus, cb)

        # Create and track an order
        order = Order(
            order_id="test-order-001",
            client_order_id="coid-001",
            symbol="BTC/USDT:USDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=0.1,
            price=50000.0,
            status=OrderStatus.OPEN,
        )
        om.orders[order.order_id] = order
        om.client_order_map["coid-001"] = order

        # First fill should succeed
        await om.record_fill(
            client_order_id="coid-001",
            fill_id="fill-001",
            quantity=0.05,
            price=50000.0,
            fee=0.01,
        )
        assert len(order.fills) == 1

        # Duplicate fill_id should be rejected (idempotent)
        await om.record_fill(
            client_order_id="coid-001",
            fill_id="fill-001",
            quantity=0.05,
            price=50000.0,
            fee=0.01,
        )
        assert len(order.fills) == 1, "Duplicate fill_id must be rejected"

        # Different fill_id should be accepted
        await om.record_fill(
            client_order_id="coid-001",
            fill_id="fill-002",
            quantity=0.05,
            price=50100.0,
            fee=0.01,
        )
        assert len(order.fills) == 2


# ═══════════════════════════════════════════════════════════════════════════════
# 7. SAFE MODE — Feed disconnect blocks new signals
# ═══════════════════════════════════════════════════════════════════════════════

class TestSafeModeOnFeedLoss:
    """Prove: circuit breaker trips when user data stream is lost,
    blocking new entries until reconnection."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_blocks_after_stream_loss(self) -> None:
        rm = _make_risk_manager()
        # Simulate what happens when USER_STREAM_LOST fires
        rm._circuit_breaker.trip("user_stream_lost")

        from engine.signal_generator import TradingSignal
        sig = TradingSignal(
            exchange="binance", symbol="BTC/USDT:USDT",
            direction="long", score=0.9,
            technical_score=0.8, ml_score=0.7, sentiment_score=0.5,
            macro_score=0.5, news_score=0.5, orderbook_score=0.5,
            regime="trending", regime_confidence=0.9,
            price=50000.0, atr=500.0,
            stop_loss=49000.0, take_profit=53000.0,
            timestamp=int(time.time()),
        )
        approved, reason, _ = rm.approve_signal(sig)
        assert approved is False
        assert "circuit_breaker" in reason

    @pytest.mark.asyncio
    async def test_circuit_breaker_resets_after_stream_reconnect(self) -> None:
        rm = _make_risk_manager()
        rm._circuit_breaker.trip("user_stream_lost")
        assert rm._circuit_breaker.tripped is True

        # On reconnect, circuit breaker is cleared for this specific reason
        cleared = rm._circuit_breaker.clear_if_reason("user_stream_lost")
        assert cleared is True
        assert rm._circuit_breaker.tripped is False
