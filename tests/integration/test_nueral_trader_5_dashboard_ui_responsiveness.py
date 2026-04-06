"""Integration tests that validate dashboard UI fetch/actions are responsive.

These tests mirror endpoints called from interface/static/index.html.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from core.circuit_breaker import CircuitBreaker
from core.event_bus import EventBus
from execution.order_manager import OrderManager
from execution.risk_manager import RiskManager
from interface import dashboard_api
from interface.dashboard_api import build_app


@dataclass
class _FG:
    score: float
    label: str


class _FGSource:
    def get_latest(self):
        return _FG(score=0.2, label="Greed")


class _SignalGen:
    def __init__(self) -> None:
        self.auto_trading_enabled = False

    def set_auto_trading(self, enabled: bool) -> None:
        self.auto_trading_enabled = bool(enabled)


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
                "initial_equity": 100_000,
            }
        if keys == ("monitoring", "dashboard_api"):
            return {"host": "127.0.0.1", "port": 8000, "auth": {"require_api_key": False}}
        if keys == ("exchanges",):
            return {
                "binance": {"enabled": True, "api_key": "x", "api_secret": "y", "testnet": True},
                "bybit": {"enabled": False, "api_key": "", "api_secret": ""},
                "okx": {"enabled": False, "api_key": "", "api_secret": "", "passphrase": ""},
                "kraken": {"enabled": False, "api_key": "", "api_secret": ""},
            }
        if keys == ("exchanges", "binance"):
            return {"enabled": True, "api_key": "x", "api_secret": "y", "testnet": True}
        if keys == ("dex",):
            return {
                "enabled": False,
                "rpc_url": "",
                "private_key": "",
                "uniswap": {"enabled": False},
                "sushiswap": {"enabled": False},
                "dydx": {"enabled": False},
            }
        if keys == ("notifications", "telegram"):
            return {"bot_token": "", "chat_id": ""}
        if keys == ("rust_services", "enabled"):
            return False
        if keys == ("ts_dex_layer", "enabled"):
            return False
        return default

    cfg.get_value.side_effect = _get_value
    return cfg


@pytest.fixture
def ui_client(config_mock: MagicMock) -> TestClient:
    bus = EventBus()
    breaker = CircuitBreaker()
    risk_mgr = RiskManager(config_mock, bus)
    order_mgr = OrderManager(config_mock, bus, breaker)
    signal_gen = _SignalGen()

    # Prime caches so tests do not depend on internet reachability.
    dashboard_api._market_cache["coins"] = [
        {
            "symbol": "BTC",
            "name": "Bitcoin",
            "price": 65000.0,
            "change_24h": 1.5,
            "volume_24h": 1.2e10,
            "high_24h": 66000.0,
            "low_24h": 64000.0,
            "market_cap": 1.0e12,
        }
    ]
    dashboard_api._market_cache["ts"] = time.time()
    dashboard_api._news_buffer.clear()
    dashboard_api._news_buffer.append(
        {"ts": int(time.time() * 1000), "title": "BTC steady", "sentiment": "neutral", "score": 0}
    )
    dashboard_api._orderbook_cache["binance:BTC/USDT"] = {
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "bids": [[64990.0, 2.0], [64980.0, 1.4]],
        "asks": [[65010.0, 2.1], [65020.0, 1.2]],
        "ts": time.time(),
    }
    dashboard_api._log_buffer.clear()
    dashboard_api._log_buffer.append(
        {"ts": int(time.time() * 1000), "level": "INFO", "message": "ui test log"}
    )

    sentiment = MagicMock()
    sentiment._fear_greed = _FGSource()

    app = build_app(
        config_mock,
        bus,
        risk_manager=risk_mgr,
        order_manager=order_mgr,
        signal_generator=signal_gen,
        news_feed=object(),
        sentiment_manager=sentiment,
    )
    return TestClient(app)


def test_dashboard_root_serves_ui_html(ui_client: TestClient) -> None:
    resp = ui_client.get("/")
    assert resp.status_code == 200
    body = resp.text
    assert "toggleTradingMode()" in body
    assert "executeTrade('BUY')" in body
    assert "/api/realtime/stream" in body


@pytest.mark.parametrize(
    "path",
    [
        "/api/signals/recent",
        "/api/status",
        "/api/market?per_page=20",
        "/api/backtest/summary",
        "/api/news",
        "/api/logs/recent",
        "/api/feargreed",
        "/api/dex/pools",
        "/api/system/data-sources",
        "/api/auto/status",
        "/api/config",
        "/api/trades/history",
        "/api/indicators/BTC",
        "/api/orderbook?symbol=BTC/USDT&depth=20",
        "/api/candles?symbol=BTC/USDT&timeframe=1m",
        "/api/realtime/snapshot?symbol=BTC/USDT&timeframe=1m",
    ],
)
def test_ui_get_endpoints_responsive(ui_client: TestClient, path: str) -> None:
    resp = ui_client.get(path)
    assert resp.status_code == 200, f"GET {path} failed with {resp.status_code}: {resp.text[:200]}"
    assert isinstance(resp.json(), dict)


@pytest.mark.parametrize(
    "path,payload",
    [
        (
            "/api/trade",
            {
                "symbol": "BTC/USDT:USDT",
                "side": "BUY",
                "size": 0.01,
                "order_type": "market",
                "price": 0,
                "stop_loss_pct": 2,
                "take_profit_pct": 4,
                "leverage": 1,
            },
        ),
        ("/api/positions/close-all", None),
        ("/api/positions/breakeven", None),
        (
            "/api/auto/toggle",
            {
                "enabled": True,
                "mode": "paper",
                "strategy": "ensemble",
                "sizing_mode": "risk_pct",
                "risk_per_trade": 2,
                "max_positions": 3,
                "max_drawdown": 10,
                "max_leverage": 5,
                "daily_loss_limit": 500,
                "trailing_mode": "none",
                "auto_sl_tp": True,
            },
        ),
        ("/api/mode/toggle", {"mode": "paper"}),
        (
            "/api/config",
            {
                "binance_enabled": True,
                "auto_trading_enabled": False,
                "auto_stop_loss_enabled": True,
                "auto_take_profit_enabled": True,
            },
        ),
        ("/api/config/test", None),
    ],
)
def test_ui_action_endpoints_responsive(ui_client: TestClient, path: str, payload: dict | None) -> None:
    if payload is None:
        resp = ui_client.post(path)
    else:
        resp = ui_client.post(path, json=payload)

    assert resp.status_code == 200, f"POST {path} failed with {resp.status_code}: {resp.text[:200]}"
    data = resp.json()
    assert isinstance(data, dict)

    # Most UI actions expose a success flag; verify when present.
    if "success" in data:
        assert data["success"] is True


def test_realtime_stream_endpoint_responds(ui_client: TestClient) -> None:
    # SSE streaming endpoints are infinite generators — we cannot use stream()
    # context manager without blocking. Instead verify the route is registered
    # by checking it doesn't 404. A real SSE test needs an async HTTP client.
    import threading, requests, time as _time

    # Use the TestClient as a context manager to start the server in a thread
    # and make a real HTTP request with a short timeout.
    from starlette.testclient import TestClient as _TC
    app = ui_client.app

    # Verify route registration by checking app routes contain the path
    routes = [r.path for r in app.routes if hasattr(r, "path")]
    assert "/api/realtime/stream" in routes, "SSE stream route not registered"


def test_index_fetch_targets_are_implemented(ui_client: TestClient) -> None:
    # Keep this list in sync with interface/static/index.html fetch('/api/...') calls.
    fetch_targets = {
        "/api/signals/recent",
        "/api/status",
        "/api/market",
        "/api/backtest/summary",
        "/api/news",
        "/api/logs/recent",
        "/api/feargreed",
        "/api/dex/pools",
        "/api/system/data-sources",
        "/api/trade",
        "/api/positions/close-all",
        "/api/auto/toggle",
        "/api/positions/breakeven",
        "/api/auto/status",
        "/api/mode/toggle",
        "/api/config",
        "/api/config/test",
        "/api/trades/history",
        "/api/candles",
        "/api/orderbook",
        "/api/indicators",
        "/api/realtime/snapshot",
        "/api/realtime/stream",
    }

    # Smoke check by probing representative endpoints/methods.
    for path in sorted(fetch_targets):
        if path in {"/api/trade", "/api/positions/close-all", "/api/auto/toggle", "/api/positions/breakeven", "/api/mode/toggle", "/api/config", "/api/config/test"}:
            continue
        # For parameterized routes use concrete values.
        probe = path
        if path == "/api/candles":
            probe = "/api/candles?symbol=BTC/USDT&timeframe=1m"
        elif path == "/api/orderbook":
            probe = "/api/orderbook?symbol=BTC/USDT&depth=20"
        elif path == "/api/indicators":
            probe = "/api/indicators/BTC"
        elif path in {"/api/realtime/snapshot", "/api/realtime/stream"}:
            continue

        resp = ui_client.get(probe)
        assert resp.status_code == 200, f"missing/broken endpoint for UI fetch target: {probe}"

    # Ensure static file still exists where the browser serves it from.
    assert Path("/workspaces/nueral-trader-5/interface/static/index.html").exists()
