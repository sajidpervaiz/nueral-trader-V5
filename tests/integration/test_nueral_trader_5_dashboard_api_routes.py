"""Integration tests for dashboard orders/risk API routes."""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from core.circuit_breaker import CircuitBreaker
from core.event_bus import EventBus
from engine.signal_generator import TradingSignal
from execution.order_manager import OrderManager
from execution.risk_manager import RiskManager
from interface.dashboard_api import build_app


@pytest.fixture
def config_mock() -> MagicMock:
    cfg = MagicMock()
    cfg.paper_mode = True

    def _get_value(*keys, default=None):
        if keys == ("risk",):
            return {
                "max_position_size_pct": 0.02,
                "max_open_positions": 5,
                "default_leverage": 1.0,
                "max_daily_loss_pct": 0.03,
                "max_drawdown_pct": 0.10,
                "max_portfolio_var_pct": 0.08,
                "returns_window": 250,
                "var_min_history": 30,
                "stop_loss_pct": 0.015,
                "take_profit_pct": 0.03,
            }
        if keys == ("risk", "stop_loss_pct"):
            return 0.015
        if keys == ("risk", "take_profit_pct"):
            return 0.03
        if keys == ("monitoring", "dashboard_api"):
            return {"host": "127.0.0.1", "port": 8000}
        if keys == ("exchanges",):
            return {"binance": {"enabled": True}}
        if keys == ("dex",):
            return {"enabled": False}
        if keys == ("dex", "enabled"):
            return False
        if keys == ("rust_services", "enabled"):
            return False
        if keys == ("ts_dex_layer", "enabled"):
            return False
        return default

    cfg.get_value.side_effect = _get_value
    return cfg


@pytest.fixture
def api_context(config_mock: MagicMock) -> dict:
    bus = EventBus()
    breaker = CircuitBreaker()
    risk_mgr = RiskManager(config_mock, bus)
    risk_mgr._equity = 100_000.0
    order_mgr = OrderManager(config_mock, bus, breaker)

    app = build_app(config_mock, bus, risk_manager=risk_mgr, order_manager=order_mgr)
    return {
        "client": TestClient(app),
        "risk_mgr": risk_mgr,
    }


@pytest.fixture
def api_client(api_context: dict) -> TestClient:
    return api_context["client"]


def test_orders_create_list_and_cancel(api_client: TestClient) -> None:
    create_resp = api_client.post(
        "/orders/",
        json={
            "symbol": "BTC/USDT:USDT",
            "side": "buy",
            "order_type": "limit",
            "quantity": 0.1,
            "price": 50000.0,
            "time_in_force": "GTC",
            "venue": "binance",
            "reduce_only": False,
            "client_order_id": "test-order-1",
        },
    )
    assert create_resp.status_code == 200
    payload = create_resp.json()
    order_id = payload["order_id"]
    assert payload["status"] == "PENDING"
    assert payload["symbol"] == "BTC/USDT:USDT"

    list_resp = api_client.get("/orders/")
    assert list_resp.status_code == 200
    listed = list_resp.json()
    assert any(o["order_id"] == order_id for o in listed)

    open_resp = api_client.get("/orders/open")
    assert open_resp.status_code == 200
    open_orders = open_resp.json()
    assert any(o["order_id"] == order_id for o in open_orders)

    cancel_resp = api_client.delete(f"/orders/{order_id}", params={"venue": "binance"})
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] == "cancelled"


def test_risk_limits_check_and_stress(api_client: TestClient) -> None:
    limits_resp = api_client.get("/risk/limits")
    assert limits_resp.status_code == 200
    limits = limits_resp.json()
    assert "max_position_value" in limits
    assert "leverage_limit" in limits

    check_resp = api_client.post(
        "/risk/check",
        params={
            "symbol": "ETH/USDT:USDT",
            "side": "buy",
            "quantity": 1.5,
            "price": 2500.0,
            "user_id": "u1",
        },
    )
    assert check_resp.status_code == 200
    check_payload = check_resp.json()
    assert "passed" in check_payload
    assert "reason" in check_payload

    stress_resp = api_client.get("/risk/stress-test", params={"scenario": "flash_crash"})
    assert stress_resp.status_code == 200
    stress_payload = stress_resp.json()
    assert stress_payload["status"] == "completed"
    assert "report" in stress_payload


def test_risk_circuit_breaker_status_and_reset(api_client: TestClient) -> None:
    status_resp = api_client.get("/risk/circuit-breaker")
    assert status_resp.status_code == 200
    status_payload = status_resp.json()
    assert "overall_status" in status_payload
    assert "venues" in status_payload

    reset_resp = api_client.post("/risk/circuit-breaker/reset", params={"venue": "global_risk"})
    assert reset_resp.status_code == 200
    assert reset_resp.json()["status"] == "reset"


def test_orders_place_compat_alias(api_client: TestClient) -> None:
    resp = api_client.post(
        "/orders/place",
        json={
            "symbol": "BTC/USDT:USDT",
            "side": "buy",
            "order_type": "limit",
            "quantity": 0.2,
            "price": 51000.0,
            "time_in_force": "GTC",
            "venue": "binance",
            "reduce_only": False,
            "client_order_id": "compat-alias-1",
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["status"] == "PENDING"
    assert payload["client_order_id"] == "compat-alias-1"


def test_positions_routes_are_manager_backed(api_context: dict) -> None:
    client: TestClient = api_context["client"]
    risk_mgr: RiskManager = api_context["risk_mgr"]

    signal = TradingSignal(
        exchange="binance",
        symbol="ETHUSDT",
        direction="long",
        score=0.8,
        technical_score=0.8,
        ml_score=0.8,
        sentiment_score=0.0,
        macro_score=0.0,
        news_score=0.0,
        orderbook_score=0.0,
        regime="integration",
        regime_confidence=1.0,
        price=3000.0,
        atr=30.0,
        stop_loss=2950.0,
        take_profit=3060.0,
        timestamp=1700000000,
    )
    asyncio.get_event_loop().run_until_complete(risk_mgr.open_position(signal, size=1500.0))

    list_resp = client.get("/positions/")
    assert list_resp.status_code == 200
    items = list_resp.json()
    assert len(items) == 1
    assert items[0]["symbol"] == "ETHUSDT"

    summary_resp = client.get("/positions/summary")
    assert summary_resp.status_code == 200
    summary = summary_resp.json()
    assert summary["total_positions"] == 1

    get_resp = client.get("/positions/ETHUSDT", params={"venue": "binance"})
    assert get_resp.status_code == 200

    close_resp = client.post("/positions/ETHUSDT/close", params={"venue": "binance"})
    assert close_resp.status_code == 200
    assert close_resp.json()["status"] == "closed"
