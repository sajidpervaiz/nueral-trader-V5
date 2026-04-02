from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from core.circuit_breaker import CircuitBreaker
from core.config import Config
from core.event_bus import EventBus
from execution.order_manager import OrderManager, OrderSide


CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "settings.yaml"


class _FakeRouter:
    async def route_order(self, symbol: str, side, quantity: float, max_venues: int, min_score_threshold: float):
        _ = (symbol, side, quantity, max_venues, min_score_threshold)
        route = SimpleNamespace(
            venue=SimpleNamespace(value="okx"),
            quantity=quantity,
            expected_fill_price=100.0,
            score=0.82,
        )
        return SimpleNamespace(
            recommended_venue=SimpleNamespace(value="okx"),
            confidence=0.82,
            expected_avg_price=100.0,
            routes=[route],
        )


@pytest.mark.asyncio
async def test_place_order_uses_smart_router_for_auto_exchange() -> None:
    config = Config(config_path=CONFIG_PATH)
    bus = EventBus()
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
    manager = OrderManager(config, bus, breaker)
    manager.attach_router(_FakeRouter())

    success, order, reason = await manager.place_order(
        exchange="auto",
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        quantity=0.1,
        price=42000.0,
    )

    assert success is True
    assert reason == "created"
    assert order is not None
    assert order.venue == "okx"
    assert order.metadata.get("routing_mode") == "smart_router"
    assert order.metadata.get("routing_decision", {}).get("recommended_venue") == "okx"


@pytest.mark.asyncio
async def test_place_order_auto_exchange_falls_back_when_router_missing() -> None:
    config = Config(config_path=CONFIG_PATH)
    bus = EventBus()
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
    manager = OrderManager(config, bus, breaker)

    success, order, reason = await manager.place_order(
        exchange="auto",
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        quantity=0.1,
        price=42000.0,
    )

    assert success is True
    assert reason == "created"
    assert order is not None
    assert order.venue != "auto"
    assert order.metadata.get("routing_mode") == "fallback_no_router"
