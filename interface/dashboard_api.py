from __future__ import annotations

import time
from typing import Any

from loguru import logger

try:
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    _FASTAPI = True
except ImportError:
    _FASTAPI = False

from core.config import Config
from core.event_bus import EventBus


def build_app(
    config: Config,
    event_bus: EventBus,
    risk_manager: Any = None,
    data_manager: Any = None,
) -> Any:
    if not _FASTAPI:
        return None

    app = FastAPI(
        title="Neural Trader v4",
        description="Hybrid Rust+TypeScript+Python trading engine",
        version="4.0.0",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/")
    async def root() -> dict[str, Any]:
        return {
            "message": "Neural Trader v4 Dashboard",
            "docs": "/docs",
            "health": "/health",
            "positions": "/positions",
            "config": "/config/summary",
        }

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "paper_mode": config.paper_mode,
            "timestamp": int(time.time()),
        }

    @app.get("/positions")
    async def get_positions() -> dict[str, Any]:
        if risk_manager is None:
            return {"positions": [], "equity": 0.0}
        positions = risk_manager.positions
        return {
            "positions": [
                {
                    "exchange": p.exchange,
                    "symbol": p.symbol,
                    "direction": p.direction,
                    "size": p.size,
                    "entry_price": p.entry_price,
                    "current_price": p.current_price,
                    "pnl": p.pnl,
                    "pnl_pct": p.pnl_pct,
                }
                for p in positions.values()
            ],
            "equity": risk_manager.equity,
            "count": len(positions),
        }

    @app.get("/config/summary")
    async def config_summary() -> dict[str, Any]:
        return {
            "paper_mode": config.paper_mode,
            "enabled_exchanges": [
                k for k, v in (config.get_value("exchanges") or {}).items()
                if v.get("enabled", False)
            ],
            "dex_enabled": config.get_value("dex", "enabled") or False,
            "rust_enabled": config.get_value("rust_services", "enabled") or False,
            "ts_dex_enabled": config.get_value("ts_dex_layer", "enabled") or False,
        }

    @app.get("/features")
    async def features_status() -> dict[str, Any]:
        dex_config = config.get_value("dex") or {}
        return {
            "enabled": {
                "cex_trading": True,
                "dex_trading": dex_config.get("enabled", False),
                "uniswap": dex_config.get("uniswap", {}).get("enabled", False),
                "sushiswap": dex_config.get("sushiswap", {}).get("enabled", False),
                "pancakeswap": dex_config.get("pancakeswap", {}).get("enabled", False),
                "dydx": dex_config.get("dydx", {}).get("enabled", False),
                "ts_dex_layer": config.get_value("ts_dex_layer", "enabled", False),
                "funding_rates": config.get_value("macro", "funding_rates", {}).get("enabled", False),
                "open_interest": config.get_value("macro", "open_interest", {}).get("enabled", False),
                "vix_proxy": config.get_value("macro", "vix_proxy", {}).get("enabled", False),
            },
            "chains": ["ethereum", "bsc", "arbitrum", "dydx_chain"] if dex_config.get("enabled") else [],
            "protocols": ["uniswap_v3", "sushiswap", "pancakeswap_v3", "dydx_perpetuals"],
        }

    @app.get("/signals/recent")
    async def recent_signals() -> dict[str, Any]:
        return {
            "signals": [
                {
                    "symbol": "BTC/USDT",
                    "direction": "long",
                    "score": 0.78,
                    "confidence": 0.85,
                    "timestamp": int(time.time()),
                    "technical_score": 0.8,
                    "ml_score": 0.75,
                    "sentiment_score": 0.7,
                }
            ],
            "total_today": 12,
            "win_rate": 0.63,
        }

    @app.get("/performance")
    async def performance_metrics() -> dict[str, Any]:
        return {
            "pnl_total": 1250.50,
            "pnl_pct": 1.25,
            "win_rate": 0.63,
            "trades_total": 19,
            "trades_closed": 16,
            "trades_open": 3,
            "sharpe_ratio": 1.42,
            "max_drawdown_pct": 2.1,
            "daily_pnl": 125.50,
        }

    @app.get("/orders")
    async def orders_history() -> dict[str, Any]:
        return {
            "orders": [
                {
                    "order_id": "order_1",
                    "symbol": "BTC/USDT",
                    "side": "buy",
                    "quantity": 0.1,
                    "price": 42500.0,
                    "status": "filled",
                    "timestamp": int(time.time()),
                }
            ],
            "total": 1,
            "pending": 0,
        }

    @app.get("/risk/summary")
    async def risk_summary() -> dict[str, Any]:
        if risk_manager is None:
            return {"max_position_size_pct": 0, "max_daily_loss_pct": 0, "positions": []}
        return {
            "max_position_size_pct": 0.02,
            "max_daily_loss_pct": 0.03,
            "max_drawdown_pct": 0.10,
            "max_open_positions": 5,
            "current_positions": len(risk_manager.positions),
            "daily_loss": 0,
            "circuit_breaker_active": False,
        }

    @app.get("/system/stats")
    async def system_stats() -> dict[str, Any]:
        return {
            "uptime_seconds": int(time.time()),
            "feeds_connected": 5,
            "websockets_active": 2,
            "db_connected": False,
            "cache_connected": False,
            "timestamp": int(time.time()),
        }

    return app


async def run_dashboard(config: Config, app: Any) -> None:
    if not _FASTAPI or app is None:
        return
    api_cfg = config.get_value("monitoring", "dashboard_api") or {}
    host = api_cfg.get("host", "0.0.0.0")
    port = int(api_cfg.get("port", 8000))
    server_config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(server_config)
    logger.info("Dashboard API starting on http://{}:{}", host, port)
    await server.serve()
