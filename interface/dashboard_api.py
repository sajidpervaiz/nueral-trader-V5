from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from loguru import logger

try:
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    _FASTAPI = True
except ImportError:
    _FASTAPI = False

from core.config import Config
from core.event_bus import EventBus
from interface.routes.config import router as config_router
from interface.routes.orders import router as orders_router, configure_order_routes
from interface.routes.positions import router as positions_router, configure_positions_routes
from interface.routes.risk import router as risk_router, configure_risk_routes


def build_app(
    config: Config,
    event_bus: EventBus,
    risk_manager: Any = None,
    data_manager: Any = None,
    order_manager: Any = None,
) -> Any:
    if not _FASTAPI:
        return None

    app = FastAPI(
        title="Neural Trader v4",
        description="Hybrid Rust+TypeScript+Python trading engine",
        version="4.0.0",
    )

    dashboard_cfg = config.get_value("monitoring", "dashboard_api", default={}) or {}
    cors_origins = dashboard_cfg.get("allow_origins") or ["http://localhost", "http://127.0.0.1"]
    auth_cfg = dashboard_cfg.get("auth", {}) if hasattr(dashboard_cfg, "get") else {}
    if not isinstance(auth_cfg, dict):
        auth_cfg = {}

    require_api_key = bool(auth_cfg.get("require_api_key", False))
    api_key = str(auth_cfg.get("api_key", "") or "").strip()
    rate_limit_per_min = int(auth_cfg.get("rate_limit_per_min", 120))
    exempt_paths = {
        "/",
        "/health",
        "/docs",
        "/openapi.json",
        "/redoc",
    }

    # In-memory limiter is sufficient for single-instance Tier 0 deployments.
    ip_rate_state: dict[str, dict[str, int]] = defaultdict(lambda: {"window_start": 0, "count": 0})

    @app.middleware("http")
    async def api_guard(request: Request, call_next):
        path = request.url.path
        if path not in exempt_paths:
            if rate_limit_per_min > 0:
                ip = request.client.host if request.client else "unknown"
                now = int(time.time())
                state = ip_rate_state[ip]
                if now - state["window_start"] >= 60:
                    state["window_start"] = now
                    state["count"] = 0
                state["count"] += 1
                if state["count"] > rate_limit_per_min:
                    return JSONResponse(status_code=429, content={"detail": "rate_limit_exceeded"})

            if require_api_key and api_key:
                provided = request.headers.get("x-api-key")
                if not provided:
                    auth_header = request.headers.get("authorization", "")
                    if auth_header.lower().startswith("bearer "):
                        provided = auth_header[7:].strip()
                if provided != api_key:
                    return JSONResponse(status_code=401, content={"detail": "unauthorized"})

        return await call_next(request)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    configure_order_routes(order_manager)
    configure_risk_routes(risk_manager)
    configure_positions_routes(risk_manager)
    app.include_router(config_router)
    app.include_router(orders_router)
    app.include_router(positions_router)
    app.include_router(risk_router)

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
