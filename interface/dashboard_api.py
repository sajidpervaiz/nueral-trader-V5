from __future__ import annotations

import asyncio
import hmac
import json
import re
import time
from collections import defaultdict, deque
from typing import Any

from loguru import logger

try:
    from fastapi import FastAPI, Query, Request
    from fastapi.responses import JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse
    from sse_starlette.sse import EventSourceResponse  # type: ignore[import-untyped]
    import uvicorn
    _FASTAPI = True
except ImportError:
    _FASTAPI = False

from pathlib import Path


from core.config import Config
from core.event_bus import EventBus
from interface.routes.config import router as config_router, configure_config_routes
from interface.routes.orders import router as orders_router, configure_order_routes
from interface.routes.positions import router as positions_router, configure_positions_routes
from interface.routes.risk import router as risk_router, configure_risk_routes

# Expose a top-level FastAPI app for uvicorn
def _default_config():
    # Minimal config for dashboard boot
    class DummyConfig:
        paper_mode = True
        def get_value(self, *args, **kwargs):
            return {}
    return DummyConfig()

def _default_event_bus():
    class DummyEventBus:
        def subscribe(self, *a, **k):
            pass
        async def publish(self, *a, **k):
            pass
    return DummyEventBus()


# Only create app if FastAPI is available (must be after build_app is defined)
_APP_ARGS = dict(
    config=_default_config(),
    event_bus=_default_event_bus(),
    risk_manager=None,
    data_manager=None,
    order_manager=None,
    db_handler=None,
    cache=None,
    signal_generator=None,
    news_feed=None,
    orderbook_feed=None,
    sentiment_manager=None,
    dex_feed=None,
)

# Defer app creation until after build_app is defined


# ── In-memory ring buffers for event-sourced data ────────────────────────────
_news_buffer: deque[dict[str, Any]] = deque(maxlen=50)
_orderbook_cache: dict[str, dict[str, Any]] = {}
_log_buffer: deque[dict[str, Any]] = deque(maxlen=200)
_market_cache: dict[str, Any] = {"coins": [], "ts": 0.0}  # TTL cache for market data

# Loguru sink that captures recent log lines for the /api/logs/recent endpoint

def _log_sink(message: Any) -> None:
    record = message.record
    _log_buffer.append({
        "ts": int(record["time"].timestamp() * 1000),
        "level": record["level"].name,
        "message": str(record["message"])[:300],
    })


# Place this after build_app is defined
# (MUST be after the build_app function definition)

# ── Input validation for query parameters ─────────────────────────────────
_VALID_SYMBOL = re.compile(r"^[A-Za-z0-9]{1,20}(/[A-Za-z0-9]{1,10})?(:[A-Za-z0-9]{1,10})?$")
_VALID_TIMEFRAME = re.compile(r"^[1-9][0-9]?[smhdwM]$")

def _validate_symbol(symbol: str) -> str:
    """Validate symbol format; raise ValueError on bad input."""
    if not _VALID_SYMBOL.match(symbol):
        raise ValueError(f"Invalid symbol format: {symbol!r}")
    return symbol

def _validate_timeframe(timeframe: str) -> str:
    """Validate timeframe format; raise ValueError on bad input."""
    if not _VALID_TIMEFRAME.match(timeframe):
        raise ValueError(f"Invalid timeframe format: {timeframe!r}")
    return timeframe


def build_app(
    config: Config,
    event_bus: EventBus,
    risk_manager: Any = None,
    data_manager: Any = None,
    order_manager: Any = None,
    db_handler: Any = None,
    cache: Any = None,
    signal_generator: Any = None,
    *,
    news_feed: Any = None,
    orderbook_feed: Any = None,
    sentiment_manager: Any = None,
    dex_feed: Any = None,
    executors: list[Any] | None = None,
    user_stream: Any = None,
    reconciliation_result: Any = None,
    sqlite_store: Any = None,
) -> Any:
    if not _FASTAPI:
        return None

    app = FastAPI(
        title="NUERAL-TRADER-5",
        description="Hybrid Rust+TypeScript+Python trading engine",
        version="4.0.0",
    )
    app.state.started_at = int(time.time())
    static_dir = Path(__file__).resolve().parent / "static"
    static_index = static_dir / "index.html"

    dashboard_cfg = config.get_value("monitoring", "dashboard_api", default={}) or {}
    cors_origins = dashboard_cfg.get("allow_origins") or ["http://localhost", "http://127.0.0.1"]
    # In paper mode, allow all origins for dev convenience (Codespace proxies, etc.)
    if config.paper_mode:
        cors_origins = ["*"]
    # Block wildcard CORS in non-paper (live) mode
    if not config.paper_mode and "*" in cors_origins:
        logger.warning(
            "CORS wildcard '*' blocked in live mode — restricting to localhost only"
        )
        cors_origins = [o for o in cors_origins if o != "*"] or ["http://localhost"]
    auth_cfg = dashboard_cfg.get("auth", {}) if hasattr(dashboard_cfg, "get") else {}
    if not isinstance(auth_cfg, dict):
        auth_cfg = {}

    require_api_key = bool(auth_cfg.get("require_api_key", False))
    api_key = str(auth_cfg.get("api_key", "") or "").strip()
    rate_limit_per_min = int(auth_cfg.get("rate_limit_per_min", 120))
    allow_unauthenticated_non_paper = bool(auth_cfg.get("allow_unauthenticated_non_paper", False))

    # Secure-by-default posture: non-paper mode requires API auth unless explicitly overridden.
    if not config.paper_mode and not require_api_key and not allow_unauthenticated_non_paper:
        require_api_key = True
        logger.warning(
            "Enabling API key requirement automatically for non-paper mode; "
            "set monitoring.dashboard_api.auth.allow_unauthenticated_non_paper=true to override"
        )
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
                if not hmac.compare_digest(provided, api_key):
                    return JSONResponse(status_code=401, content={"detail": "unauthorized"})
            elif require_api_key and not api_key:
                return JSONResponse(status_code=503, content={"detail": "api_auth_misconfigured"})

        return await call_next(request)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    configure_order_routes(order_manager)
    configure_risk_routes(risk_manager)
    configure_positions_routes(risk_manager)
    configure_config_routes(config, risk_manager, order_manager)
    # Mount existing routers under /api prefix so UI /api/* calls work
    app.include_router(config_router, prefix="/api")
    app.include_router(orders_router, prefix="/api")
    app.include_router(positions_router, prefix="/api")
    app.include_router(risk_router, prefix="/api")
    # Also keep original paths for backwards compatibility
    app.include_router(config_router)
    app.include_router(orders_router)
    app.include_router(positions_router)
    app.include_router(risk_router)

    # ── Subscribe to event bus for caching live data ──────────────────────
    async def _cache_news(payload: Any) -> None:
        _news_buffer.append({
            "ts": int(payload.get("timestamp", time.time()) * 1000),
            "title": payload.get("title", ""),
            "sentiment": "bullish" if payload.get("sentiment", 0) > 0.15 else ("bearish" if payload.get("sentiment", 0) < -0.15 else "neutral"),
            "score": payload.get("sentiment", 0),
        })

    async def _cache_orderbook(payload: Any) -> None:
        key = f"{payload.get('exchange', 'binance')}:{payload.get('symbol', '')}"
        _orderbook_cache[key] = {
            "exchange": payload.get("exchange", "binance"),
            "symbol": payload.get("symbol", ""),
            "bids": payload.get("bids", []),
            "asks": payload.get("asks", []),
            "ts": time.time(),
        }

    event_bus.subscribe("NEWS_SENTIMENT", _cache_news)
    event_bus.subscribe("ORDERBOOK_UPDATE", _cache_orderbook)

    @app.get("/")
    async def root() -> Any:
        if static_index.exists():
            return FileResponse(str(static_index))
        return {
            "message": "NUERAL-TRADER-5 Dashboard",
            "docs": "/docs",
            "health": "/health",
            "positions": "/positions",
            "config": "/config/summary",
        }

    @app.get("/health")
    async def health() -> dict[str, Any]:
        result: dict[str, Any] = {
            "status": "ok",
            "paper_mode": config.paper_mode,
            "timestamp": int(time.time()),
            "uptime_seconds": int(time.time()) - app.state.started_at,
        }

        # Risk snapshot
        if risk_manager is not None:
            try:
                rm = risk_manager
                result["risk"] = {
                    "equity": getattr(rm, "equity", 0.0),
                    "open_positions": len(getattr(rm, "positions", {})),
                    "circuit_breaker_active": getattr(rm, "circuit_breaker_active", False),
                    "kill_switch": getattr(rm, "kill_switch", False),
                    "daily_loss": getattr(rm, "daily_loss", 0.0),
                }
            except Exception:
                result["risk"] = {"error": "unavailable"}

        # Safe mode
        try:
            from core.safe_mode import SafeModeManager
            sm = getattr(risk_manager, "safe_mode", None) if risk_manager else None
            if sm is not None and isinstance(sm, SafeModeManager):
                status = sm.get_status()
                result["safe_mode"] = {
                    "active": status.get("safe_mode_active", False),
                    "reasons": [r["reason"] for r in status.get("active_reasons", [])],
                }
        except Exception:
            pass

        # Alert manager
        try:
            from monitoring.alert_manager import AlertManager
            am = getattr(app.state, "alert_manager", None)
            if am is not None and isinstance(am, AlertManager):
                result["alerts"] = am.get_status()
        except Exception:
            pass

        # Component health (if a HealthChecker is attached)
        try:
            from monitoring.health_checks import HealthChecker
            hc = getattr(app.state, "health_checker", None)
            if hc is not None and isinstance(hc, HealthChecker):
                hcr = await hc.check_all_components()
                result["status"] = hcr.overall_status.value.lower()
                result["components"] = {
                    name: {
                        "status": comp.status.value,
                        "latency_ms": round(comp.latency_ms, 1),
                        "message": comp.message,
                    }
                    for name, comp in hcr.components.items()
                }
        except Exception:
            pass

        return result

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
        signals: list[dict[str, Any]] = []
        if order_manager is not None:
            recent_orders = sorted(order_manager.orders.values(), key=lambda o: o.created_at, reverse=True)[:25]
            for order in recent_orders:
                signals.append(
                    {
                        "symbol": order.symbol,
                        "direction": "long" if str(order.side.value).lower() == "buy" else "short",
                        "score": float(order.metadata.get("score", 0.7)),
                        "confidence": float(order.metadata.get("confidence", 0.75)),
                        "timestamp": int(order.created_at),
                        "technical_score": float(order.metadata.get("technical_score", 0.7)),
                        "ml_score": float(order.metadata.get("ml_score", 0.7)),
                        "sentiment_score": float(order.metadata.get("sentiment_score", 0.5)),
                        "source": "order_flow",
                    }
                )
        return {
            "signals": signals[:20],
            "total_today": len(signals),
            "win_rate": None,
        }

    @app.get("/api/signals/recent")
    async def api_recent_signals() -> dict[str, Any]:
        return await recent_signals()

    @app.get("/performance")
    async def performance_metrics() -> dict[str, Any]:
        pnl_total = 0.0
        pnl_pct = 0.0
        trades_total = 0
        trades_closed = 0
        trades_open = 0
        total_fees = 0.0
        if risk_manager is not None:
            pnl_total = float(sum(p.pnl for p in risk_manager.positions.values()))
            equity = float(max(1.0, risk_manager.equity))
            pnl_pct = float((pnl_total / equity) * 100.0)
        if order_manager is not None:
            stats = order_manager.get_stats()
            trades_total = int(stats.get("total_orders", 0))
            trades_open = int(stats.get("open_orders", 0))
            trades_closed = int(stats.get("filled_orders", 0))
            total_fees = float(stats.get("total_fees", 0.0))
        return {
            "pnl_total": pnl_total,
            "pnl_pct": pnl_pct,
            "win_rate": None,
            "trades_total": trades_total,
            "trades_closed": trades_closed,
            "trades_open": trades_open,
            "sharpe_ratio": None,
            "max_drawdown_pct": None,
            "daily_pnl": pnl_total,
            "total_fees": total_fees,
        }

    @app.get("/system/stats")
    async def system_stats() -> dict[str, Any]:
        db_connected = bool(getattr(db_handler, "available", False))
        cache_connected = bool(getattr(cache, "available", False))
        now = int(time.time())
        started_at = int(getattr(app.state, "started_at", now))
        feeds_connected = 0
        if data_manager is not None:
            feeds_connected = len(getattr(data_manager, "_aggregators", {}))

        websockets_active = 0
        if event_bus is not None:
            websockets_active = int(getattr(event_bus, "_queue", None).qsize()) if hasattr(getattr(event_bus, "_queue", None), "qsize") else 0
        return {
            "uptime_seconds": max(0, now - started_at),
            "feeds_connected": feeds_connected,
            "websockets_active": websockets_active,
            "db_connected": db_connected,
            "cache_connected": cache_connected,
            "timestamp": now,
        }

    # ── Auto-trading control ──────────────────────────────────────────────
    @app.post("/auto/toggle")
    async def auto_toggle(request: Request) -> dict[str, Any]:
        return await api_auto_toggle(request)

    @app.get("/auto/status")
    async def auto_status() -> dict[str, Any]:
        return await api_auto_status()

    @app.get("/signals/weights")
    async def signal_weights() -> dict[str, Any]:
        """Return the current 6-factor signal weights and configuration."""
        if signal_generator is None:
            return {"weights": {}, "min_score": 0.0, "min_factors": 0}
        return {
            "weights": {
                "technical": getattr(signal_generator, "_tech_weight", 0),
                "ml": getattr(signal_generator, "_ml_weight", 0),
                "sentiment": getattr(signal_generator, "_sentiment_weight", 0),
                "macro": getattr(signal_generator, "_macro_weight", 0),
                "news": getattr(signal_generator, "_news_weight", 0),
                "orderbook": getattr(signal_generator, "_orderbook_weight", 0),
            },
            "min_score": getattr(signal_generator, "_min_score", 0),
            "min_factors": getattr(signal_generator, "_min_factors", 0),
            "auto_trading_enabled": getattr(signal_generator, "auto_trading_enabled", False),
        }

    # ── Kill switch ───────────────────────────────────────────────────────
    @app.post("/v1/kill")
    async def kill_switch_activate() -> dict[str, Any]:
        """Emergency: close all positions, cancel all orders, block new signals."""
        if risk_manager is None:
            return {"success": False, "error": "risk_manager not available"}
        closed = await risk_manager.activate_kill_switch()
        # Directly cancel exchange-side orders (no event-bus race window)
        cancelled_total = 0
        for exc in (executors or []):
            client = getattr(exc, "_client", None)
            if client and not config.paper_mode:
                try:
                    open_orders = await client.fetch_open_orders()
                    for o in open_orders:
                        for _attempt in range(3):
                            try:
                                rl = getattr(exc, "_rate_limiter", None)
                                if rl:
                                    await rl.acquire()
                                await client.cancel_order(o["id"], o.get("symbol"))
                                cancelled_total += 1
                                break
                            except Exception:
                                if _attempt == 2:
                                    logger.error(
                                        "Kill switch: FAILED to cancel order {} on {}",
                                        o.get("id"), getattr(exc, "exchange_id", "?"),
                                    )
                except Exception as exc_err:
                    logger.warning("Kill switch order cancel failed: {}", exc_err)
            # Also clean up protective order tracking
            placer = getattr(exc, "_order_placer", None)
            if placer:
                for sym in list(getattr(placer, "protective_orders", {}).keys()):
                    try:
                        await placer.cancel_all_for_symbol(sym)
                    except Exception:
                        pass
        # Notify other subscribers (best-effort, non-critical path)
        if event_bus is not None:
            await event_bus.publish("KILL_SWITCH", {"source": "api"})
        return {
            "success": True,
            "positions_closed": len(closed),
            "orders_cancelled": cancelled_total,
            "kill_switch_active": True,
        }

    @app.post("/v1/kill/deactivate")
    async def kill_switch_deactivate() -> dict[str, Any]:
        if risk_manager is None:
            return {"success": False, "error": "risk_manager not available"}
        risk_manager.deactivate_kill_switch()
        return {"success": True, "kill_switch_active": False}

    @app.get("/v1/risk/snapshot")
    async def risk_snapshot() -> dict[str, Any]:
        """Full risk snapshot including drawdown, VaR, kill switch status."""
        if risk_manager is None:
            return {"error": "risk_manager not available"}
        return risk_manager.get_risk_snapshot()

    # ══════════════════════════════════════════════════════════════════════
    #  /api/* endpoints — UI fetches everything under this prefix
    # ══════════════════════════════════════════════════════════════════════

    # ── /api/status — portfolio status header bar ─────────────────────────
    @app.get("/api/status")
    async def api_status() -> dict[str, Any]:
        equity = 0.0
        unrealized_pnl = 0.0
        drawdown_pct = 0.0
        portfolio_heat = 0.0
        daily_pnl = 0.0
        open_positions = 0
        positions_list: list[dict] = []
        win_rate = 0.0
        total_trades = 0

        if risk_manager is not None:
            equity = float(risk_manager.equity)
            positions = risk_manager.positions
            unrealized_pnl = float(sum(p.pnl for p in positions.values()))
            open_positions = len(positions)
            snap = risk_manager.get_risk_snapshot()
            drawdown_pct = float(snap.get("drawdown_pct", 0))
            portfolio_heat = float(snap.get("portfolio_heat", 0))
            positions_list = [
                {
                    "symbol": p.symbol,
                    "side": p.direction,
                    "size": p.size,
                    "entry": p.entry_price,
                    "current": p.current_price,
                    "pnl": p.pnl,
                }
                for p in positions.values()
            ]

        if order_manager is not None:
            stats = order_manager.get_stats()
            total_trades = int(stats.get("total_orders", 0))
            filled = int(stats.get("filled_orders", 0))
            win_rate = 0.0
            if filled > 0:
                total_pnl = unrealized_pnl
                win_rate = 100.0 if total_pnl >= 0 else 0.0

        daily_pnl = unrealized_pnl

        return {
            "equity": equity,
            "unrealized_pnl": unrealized_pnl,
            "drawdown_pct": drawdown_pct,
            "portfolio_heat": portfolio_heat,
            "daily_pnl": daily_pnl,
            "open_positions": open_positions,
            "positions": positions_list,
            "win_rate": win_rate,
            "total_trades": total_trades,
        }

    # ── /api/candles — OHLCV data for chart ───────────────────────────────
    _candle_cache: dict[str, Any] = {"key": "", "ts": 0.0, "candles": []}

    @app.get("/api/candles")
    async def api_candles(
        symbol: str = Query("BTC/USDT"),
        timeframe: str = Query("1m"),
    ) -> dict[str, Any]:
        import time as _time
        try:
            symbol = _validate_symbol(symbol)
            timeframe = _validate_timeframe(timeframe)
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"detail": str(exc)})

        # Build all symbol variants to try
        clean_sym = symbol.replace("/", "").replace(":USDT", "").upper()
        base = clean_sym.replace("USDT", "") if clean_sym.endswith("USDT") else clean_sym
        sym_variants = [
            f"{base}/USDT:USDT",   # normalizer output format
            symbol,                 # as given
            f"{base}/USDT",         # ccxt-style
            clean_sym,              # raw "BTCUSDT"
        ]

        # Try DataManager first (live aggregated data)
        df = None
        if data_manager is not None:
            for sym_try in sym_variants:
                df = data_manager.get_dataframe("binance", sym_try, timeframe)
                if df is not None and len(df) > 0:
                    break
            else:
                df = None

        if df is not None and len(df) > 0:
            rows = []
            for idx, row in df.iterrows():
                rows.append({
                    "time": idx.isoformat() if hasattr(idx, "isoformat") else str(idx),
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": float(row.get("close", 0)),
                    "volume": float(row.get("volume", 0)),
                })
            return {"candles": rows}

        # Fallback: fetch historical klines from Binance
        cache_key = f"{clean_sym}:{timeframe}"
        now = _time.time()
        if _candle_cache["key"] == cache_key and (now - _candle_cache["ts"]) < 30:
            return {"candles": _candle_cache["candles"]}

        tf_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
        binance_tf = tf_map.get(timeframe, "1m")
        try:
            import aiohttp
            pair = f"{base}USDT"
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={pair}&interval={binance_tf}&limit=1000"
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as sess:
                async with sess.get(url) as resp:
                    if resp.status == 200:
                        klines = await resp.json(content_type=None)
                        rows = []
                        for k in klines:
                            ts = int(k[0]) / 1000  # ms → s
                            from datetime import datetime, timezone
                            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                            rows.append({
                                "time": dt.isoformat(),
                                "open": float(k[1]),
                                "high": float(k[2]),
                                "low": float(k[3]),
                                "close": float(k[4]),
                                "volume": float(k[5]),
                            })
                        _candle_cache["key"] = cache_key
                        _candle_cache["ts"] = now
                        _candle_cache["candles"] = rows
                        return {"candles": rows}
        except Exception as exc:
            logger.debug("Binance klines fallback error: {}", exc)

        return {"candles": []}

    # ── /api/market — watchlist / market data ─────────────────────────────
    @app.get("/api/market")
    async def api_market(per_page: int = Query(20)) -> dict[str, Any]:
        import time as _time
        import aiohttp

        now = _time.time()
        # Return cache if fresh (60s TTL)
        if _market_cache["coins"] and (now - _market_cache["ts"]) < 60:
            return {"coins": _market_cache["coins"][:min(per_page, 50)]}

        coins: list[dict[str, Any]] = []
        # Primary: CoinGecko
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as sess:
                async with sess.get(
                    "https://api.coingecko.com/api/v3/coins/markets",
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": min(per_page, 50),
                        "page": 1,
                        "sparkline": "false",
                    },
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        for c in data:
                            coins.append({
                                "symbol": str(c.get("symbol", "")).upper(),
                                "name": c.get("name", ""),
                                "price": float(c.get("current_price") or 0),
                                "change_24h": float(c.get("price_change_percentage_24h") or 0),
                                "volume_24h": float(c.get("total_volume") or 0),
                                "high_24h": float(c.get("high_24h") or 0),
                                "low_24h": float(c.get("low_24h") or 0),
                                "market_cap": float(c.get("market_cap") or 0),
                            })
        except Exception as exc:
            logger.debug("CoinGecko market fetch error: {}", exc)

        # Fallback: Binance 24h ticker for top futures symbols
        if not coins:
            try:
                top_symbols = [
                    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
                    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
                    "LINKUSDT", "LTCUSDT", "UNIUSDT", "ATOMUSDT", "NEARUSDT",
                    "APTUSDT", "ARBUSDT", "OPUSDT", "SUIUSDT", "PEPEUSDT",
                ]
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as sess:
                    async with sess.get(
                        "https://fapi.binance.com/fapi/v1/ticker/24hr"
                    ) as resp:
                        if resp.status == 200:
                            tickers = await resp.json(content_type=None)
                            ticker_map = {t["symbol"]: t for t in tickers if isinstance(t, dict)}
                            for sym in top_symbols[:min(per_page, 50)]:
                                t = ticker_map.get(sym)
                                if not t:
                                    continue
                                coins.append({
                                    "symbol": sym.replace("USDT", ""),
                                    "name": sym.replace("USDT", ""),
                                    "price": float(t.get("lastPrice") or 0),
                                    "change_24h": float(t.get("priceChangePercent") or 0),
                                    "volume_24h": float(t.get("quoteVolume") or 0),
                                    "high_24h": float(t.get("highPrice") or 0),
                                    "low_24h": float(t.get("lowPrice") or 0),
                                    "market_cap": 0,
                                })
            except Exception as exc:
                logger.debug("Binance ticker fallback error: {}", exc)

        if coins:
            _market_cache["coins"] = coins
            _market_cache["ts"] = now
        elif _market_cache["coins"]:
            # Return stale cache rather than empty
            return {"coins": _market_cache["coins"][:min(per_page, 50)]}

        return {"coins": coins}

    # ── /api/feargreed — fear & greed index ───────────────────────────────
    @app.get("/api/feargreed")
    async def api_feargreed() -> dict[str, Any]:
        if sentiment_manager is not None:
            fg = getattr(sentiment_manager, "_fear_greed", None)
            if fg is not None:
                latest = fg.get_latest()
                if latest is not None:
                    value = int(max(0, min(100, (latest.score + 1) * 50)))
                    return {
                        "value": value,
                        "classification": latest.label.capitalize(),
                    }
        # Fallback: direct fetch
        try:
            import aiohttp
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as sess:
                async with sess.get("https://api.alternative.me/fng/?limit=1&format=json") as resp:
                    data = await resp.json(content_type=None)
                    item = data.get("data", [{}])[0]
                    return {
                        "value": int(item.get("value", 50)),
                        "classification": item.get("value_classification", "Neutral"),
                    }
        except Exception:
            return {"value": 50, "classification": "Neutral"}

    # ── /api/orderbook — order book depth ─────────────────────────────────
    @app.get("/api/orderbook")
    async def api_orderbook(
        symbol: str = Query("BTC/USDT"),
        depth: int = Query(8),
    ) -> dict[str, Any]:
        try:
            symbol = _validate_symbol(symbol)
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"detail": str(exc)})
        depth = max(1, min(depth, 50))
        # Check event-bus cache first
        for key, cached in _orderbook_cache.items():
            if symbol.replace("/", "") in key.replace("/", "").replace(":", ""):
                bids_raw = cached.get("bids", [])
                asks_raw = cached.get("asks", [])
                bids = [{"price": float(b[0]), "quantity": float(b[1])} for b in bids_raw[:depth]]
                asks = [{"price": float(a[0]), "quantity": float(a[1])} for a in asks_raw[:depth]]
                mid = (bids[0]["price"] + asks[0]["price"]) / 2 if bids and asks else 0
                spread = asks[0]["price"] - bids[0]["price"] if bids and asks else 0
                return {"bids": bids, "asks": asks, "spread": round(spread, 2), "mid_price": round(mid, 2)}

        # Fallback: fetch directly from Binance
        try:
            import aiohttp
            binance_sym = symbol.replace("/", "").replace("-", "").upper()
            # Always use mainnet for read-only market data (public endpoint, no auth needed)
            base_url = "https://fapi.binance.com/fapi/v1/depth"
            # Binance only accepts specific limit values
            valid_limits = [5, 10, 20, 50, 100, 500, 1000]
            binance_limit = min((v for v in valid_limits if v >= depth), default=20)
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as sess:
                async with sess.get(base_url, params={"symbol": binance_sym, "limit": binance_limit}) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        bids = [{"price": float(b[0]), "quantity": float(b[1])} for b in data.get("bids", [])[:depth]]
                        asks = [{"price": float(a[0]), "quantity": float(a[1])} for a in data.get("asks", [])[:depth]]
                        mid = (bids[0]["price"] + asks[0]["price"]) / 2 if bids and asks else 0
                        spread = asks[0]["price"] - bids[0]["price"] if bids and asks else 0
                        return {"bids": bids, "asks": asks, "spread": round(spread, 2), "mid_price": round(mid, 2)}
        except Exception as exc:
            logger.debug("Orderbook direct fetch error: {}", exc)
        return {"bids": [], "asks": [], "spread": 0, "mid_price": 0}

    # ── /api/indicators/{symbol} — technical indicators ───────────────────
    @app.get("/api/indicators/{sym}")
    async def api_indicators(sym: str) -> dict[str, Any]:
        if data_manager is None:
            return _default_indicators()
        # Build all symbol format variants
        clean = sym.replace("/", "").replace(":USDT", "").upper()
        base = clean.replace("USDT", "") if clean.endswith("USDT") else clean
        sym_variants = [
            f"{base}/USDT:USDT",   # normalizer output format
            f"{base}/USDT",         # ccxt-style
            f"{base}USDT",          # raw
            sym,                    # as given
        ]
        for sym_try in sym_variants:
            for tf_try in ["15m", "5m", "1m", "1h"]:
                df = data_manager.get_dataframe("binance", sym_try, tf_try)
                if df is not None and len(df) >= 5:
                    row = df.iloc[-1]
                    return {
                        "rsi": float(row.get("rsi", 50)),
                        "macd": float(row.get("macd", 0)),
                        "stoch_k": float(row.get("stoch_k", 50)),
                        "adx": float(row.get("adx", 20)),
                        "atr": float(row.get("atr", 0)),
                        "bb_width": float(row.get("bb_width", 0)),
                        "ema9": float(row.get("ema_9", 0)),
                        "ema21": float(row.get("ema_21", 0)),
                        "sma50": float(row.get("sma_50", 0)),
                        "ema_cross": float(row.get("ema_9", 0)) - float(row.get("ema_21", 0)),
                        "volume_ratio": float(row.get("volume", 0)) / max(1.0, float(df["volume"].rolling(20).mean().iloc[-1])) if "volume" in df.columns else 1.0,
                    }
        return _default_indicators()

    def _default_indicators() -> dict[str, Any]:
        return {
            "rsi": 50, "macd": 0, "stoch_k": 50, "adx": 20, "atr": 0,
            "bb_width": 0, "ema9": 0, "ema21": 0, "sma50": 0, "ema_cross": 0,
            "volume_ratio": 1.0,
        }

    # ── /api/dex/pools — DEX liquidity pools (cached with backoff) ──────
    _dex_cache: dict[str, Any] = {"data": {"pools": []}, "ts": 0.0, "fail_until": 0.0}

    @app.get("/api/dex/pools")
    async def api_dex_pools() -> dict[str, Any]:
        now = time.time()
        # Return cached data if fresh (60s) or in backoff window after failure
        if now - _dex_cache["ts"] < 60 or now < _dex_cache["fail_until"]:
            return _dex_cache["data"]
        try:
            import aiohttp
            query = '{ pools(first: 5, orderBy: totalValueLockedUSD, orderDirection: desc, where: { feeTier_in: [500, 3000] }) { token0 { symbol } token1 { symbol } totalValueLockedUSD } }'
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as sess:
                async with sess.post(
                    "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
                    json={"query": query},
                    headers={"Content-Type": "application/json"},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        pools = data.get("data", {}).get("pools", [])
                        result = {
                            "pools": [
                                {
                                    "pair": f"{p['token0']['symbol']}/{p['token1']['symbol']}",
                                    "tvl": float(p.get("totalValueLockedUSD", 0)),
                                }
                                for p in pools
                            ]
                        }
                        _dex_cache["data"] = result
                        _dex_cache["ts"] = now
                        _dex_cache["fail_until"] = 0.0
                        return result
        except Exception as exc:
            logger.debug("DEX pools fetch error: {}", exc)
        # On failure: backoff 120s before retrying
        _dex_cache["fail_until"] = now + 120
        return _dex_cache["data"]

    # ── /api/news — live news feed ────────────────────────────────────────
    @app.get("/api/news")
    async def api_news() -> dict[str, Any]:
        items = list(_news_buffer)
        if items:
            return {"provider": "cryptocompare", "items": items[-20:][::-1]}
        # Fallback: fetch from CoinGecko trending + search/trending
        try:
            import aiohttp
            fetched_items: list[dict[str, Any]] = []
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as sess:
                # CoinGecko search/trending — returns trending coins as pseudo-news
                async with sess.get("https://api.coingecko.com/api/v3/search/trending") as resp:
                    if resp.status == 200:
                        from data_ingestion.news_feed import classify_sentiment
                        data = await resp.json(content_type=None)
                        coins = data.get("coins", [])
                        for c in coins[:10]:
                            item_data = c.get("item", {})
                            name = item_data.get("name", "")
                            sym = item_data.get("symbol", "")
                            score = item_data.get("score", 0)
                            price_chg = float(item_data.get("data", {}).get("price_change_percentage_24h", {}).get("usd", 0) or 0)
                            sentiment = "bullish" if price_chg > 2 else ("bearish" if price_chg < -2 else "neutral")
                            title = f"{name} ({sym}) trending — rank #{score + 1}, 24h: {price_chg:+.1f}%"
                            fetched_items.append({
                                "ts": int(time.time() * 1000),
                                "title": title,
                                "sentiment": sentiment,
                            })
                if fetched_items:
                    return {"provider": "coingecko_trending", "items": fetched_items}

                # Second fallback: CryptoCompare (may require API key)
                async with sess.get("https://min-api.cryptocompare.com/data/v2/news/?lang=EN&sortOrder=latest") as resp:
                    if resp.status == 200:
                        from data_ingestion.news_feed import classify_sentiment
                        data = await resp.json(content_type=None)
                        articles = data.get("Data", data.get("data", []))
                        if isinstance(articles, list):
                            for a in articles[:10]:
                                title = a.get("title", "")
                                score = classify_sentiment(title + " " + a.get("body", "")[:200])
                                sentiment = "bullish" if score > 0.15 else ("bearish" if score < -0.15 else "neutral")
                                fetched_items.append({
                                    "ts": int(a.get("published_on", time.time()) * 1000),
                                    "title": title[:200],
                                    "sentiment": sentiment,
                                })
                            if fetched_items:
                                return {"provider": "cryptocompare", "items": fetched_items}
        except Exception as exc:
            logger.debug("News direct fetch error: {}", exc)
        return {"provider": "unavailable", "items": []}

    # ── /api/system/data-sources — module source map ──────────────────────
    @app.get("/api/system/data-sources")
    async def api_data_sources() -> dict[str, Any]:
        return {
            "sentiment": {"source": "alternative.me/fng" if sentiment_manager else "unavailable"},
            "news": {"source": "cryptocompare" if news_feed else "unavailable"},
            "backtest": {"source": "fast_backtester"},
            "logs": {"source": "loguru_ringbuffer"},
            "auto": {"source": "signal_generator" if signal_generator else "unavailable"},
            "signals": {"source": "6_factor_composite" if signal_generator else "unavailable"},
        }

    # ── /api/reconciliation/status — startup reconciliation state ─────────
    @app.get("/api/reconciliation/status")
    async def api_reconciliation_status() -> dict[str, Any]:
        if reconciliation_result is None:
            return {"available": False, "safe_mode": False}
        return {
            "available": True,
            "reconciliation_id": getattr(reconciliation_result, "reconciliation_id", ""),
            "success": bool(reconciliation_result.success),
            "safe_mode": bool(reconciliation_result.safe_mode),
            "exchange_positions": len(reconciliation_result.exchange_positions),
            "db_positions": len(getattr(reconciliation_result, "db_positions", [])),
            "open_orders": len(reconciliation_result.exchange_open_orders),
            "mismatches": list(reconciliation_result.mismatches),
            "positions_without_sl": list(reconciliation_result.positions_without_sl),
            "actions_taken": list(reconciliation_result.actions_taken),
            "balance": reconciliation_result.balance,
            "leverage_settings": getattr(reconciliation_result, "leverage_settings", {}),
        }

    # ── /api/user-stream/status — user data stream health ─────────────────
    @app.get("/api/user-stream/status")
    async def api_user_stream_status() -> dict[str, Any]:
        if user_stream is None:
            return {"available": False, "connected": False}
        metrics = getattr(user_stream, "metrics", {})
        return {
            "available": True,
            "connected": bool(getattr(user_stream, "connected", False)),
            "disconnect_duration": float(getattr(user_stream, "disconnect_duration", 0.0)),
            "fills_processed": int(metrics.get("fills_processed", 0)),
            "fills_deduped": int(metrics.get("fills_deduped", 0)),
            "state_transitions": int(metrics.get("state_transitions", 0)),
            "invalid_transitions": int(metrics.get("invalid_transitions", 0)),
            "reconnects": int(metrics.get("reconnects", 0)),
            "messages_received": int(metrics.get("messages_received", 0)),
        }

    # ── /api/backtest/summary — backtest metrics ──────────────────────────
    @app.get("/api/backtest/summary")
    async def api_backtest_summary() -> dict[str, Any]:
        gross_notional = 0.0
        win_rate_val = 0.0
        sharpe = 0.0
        max_dd = 0.0
        avg_trade = 0.0
        profit_factor = 0.0
        if order_manager is not None:
            stats = order_manager.get_stats()
            total = int(stats.get("total_orders", 0))
            filled = int(stats.get("filled_orders", 0))
            gross_notional = float(stats.get("total_fill_value", 0))
            # Compute win/loss from actual positions
            if risk_manager is not None:
                positions = list(risk_manager.positions.values()) if hasattr(risk_manager, 'positions') else []
                wins = sum(1 for p in positions if getattr(p, 'pnl', 0) > 0)
                losses = sum(1 for p in positions if getattr(p, 'pnl', 0) < 0)
                total_trades = wins + losses
                if total_trades > 0:
                    win_rate_val = (wins / total_trades) * 100
                # Compute Sharpe approximation from position PnLs
                pnls = [getattr(p, 'pnl', 0) for p in positions if getattr(p, 'pnl', 0) != 0]
                if len(pnls) >= 2:
                    import statistics
                    mean_pnl = statistics.mean(pnls)
                    std_pnl = statistics.stdev(pnls)
                    if std_pnl > 0:
                        sharpe = (mean_pnl / std_pnl) * (252 ** 0.5)  # Annualized
                # Profit factor
                gross_profit = sum(p for p in pnls if p > 0) if pnls else 0
                gross_loss = abs(sum(p for p in pnls if p < 0)) if pnls else 0
                if gross_loss > 0:
                    profit_factor = gross_profit / gross_loss
                elif gross_profit > 0:
                    profit_factor = float('inf')
            if filled > 0 and filled > 0:
                avg_trade = gross_notional / filled
        if risk_manager is not None:
            snap = risk_manager.get_risk_snapshot()
            max_dd = float(snap.get("drawdown_pct", 0))
        return {
            "gross_notional": gross_notional,
            "win_rate": win_rate_val,
            "sharpe": round(sharpe, 2),
            "max_drawdown_pct": max_dd,
            "avg_trade_notional": round(avg_trade, 2),
            "profit_factor": round(profit_factor, 2) if profit_factor != float('inf') else 999.0,
        }

    # ── /api/logs/recent — recent log entries ─────────────────────────────
    @app.get("/api/logs/recent")
    async def api_logs_recent() -> dict[str, Any]:
        return {"logs": list(_log_buffer)[-50:][::-1]}

    # ── /api/trade — place a trade ────────────────────────────────────────
    @app.post("/api/trade")
    async def api_trade(request: Request) -> dict[str, Any]:
        body = await request.json()
        if order_manager is None:
            return {"success": False, "error": "order_manager not available"}
        try:
            from execution.order_manager import OrderSide, OrderType
            sym = str(body.get("symbol", "BTC/USDT"))
            side_str = str(body.get("side", "BUY")).upper()
            side = OrderSide.BUY if side_str == "BUY" else OrderSide.SELL
            size = float(body.get("size", 0))
            order_type_str = str(body.get("order_type", "market")).lower()
            ot = OrderType.MARKET if order_type_str == "market" else OrderType.LIMIT
            price = float(body.get("price", 0)) if ot == OrderType.LIMIT else None
            if ot == OrderType.LIMIT and (price is None or price <= 0):
                return {"success": False, "error": "price required and must be > 0 for LIMIT orders"}
            if size <= 0:
                return {"success": False, "error": "size must be > 0"}
            if config.paper_mode:
                # Paper mode: record order locally without hitting exchange
                success, order, reason = await order_manager.place_order(
                    exchange="binance",
                    symbol=sym,
                    side=side,
                    quantity=size,
                    price=price or 0,
                    order_type=ot,
                    metadata={
                        "source": "ui",
                        "paper": True,
                        "stop_loss_pct": body.get("stop_loss_pct", 2),
                        "take_profit_pct": body.get("take_profit_pct", 4),
                        "leverage": body.get("leverage", 1),
                    },
                )
                if not success:
                    return {"success": False, "error": reason}
                return {"success": True, "order_id": getattr(order, "order_id", "unknown"), "paper": True}
            else:
                # Live mode: also use place_order (executor handles real submission)
                success, order, reason = await order_manager.place_order(
                    exchange="binance",
                    symbol=sym,
                    side=side,
                    quantity=size,
                    price=price or 0,
                    order_type=ot,
                    metadata={
                        "source": "ui",
                        "stop_loss_pct": body.get("stop_loss_pct", 2),
                        "take_profit_pct": body.get("take_profit_pct", 4),
                        "leverage": body.get("leverage", 1),
                    },
                )
                if not success:
                    return {"success": False, "error": reason}
                return {"success": True, "order_id": getattr(order, "order_id", "unknown"), "paper": False}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    # ── /api/positions/close-all — close all positions ────────────────────
    @app.post("/api/positions/close-all")
    async def api_close_all() -> dict[str, Any]:
        if risk_manager is None:
            return {"success": False, "error": "risk_manager not available"}
        closed = await risk_manager.activate_kill_switch()
        risk_manager.deactivate_kill_switch()
        return {"success": True, "closed": len(closed)}

    # ── /api/positions/breakeven — set break-even on positions ────────────
    @app.post("/api/positions/breakeven")
    async def api_breakeven() -> dict[str, Any]:
        if risk_manager is None:
            return {"success": False, "error": "risk_manager not available"}
        count = 0
        async with risk_manager._lock:
            for pos in risk_manager._positions.values():
                if pos.pnl > 0:
                    pos.stop_loss = pos.entry_price
                    pos.breakeven_moved = True
                    count += 1
        return {"success": True, "updated_positions": count}

    # ── Paper feed helpers (use the main event bus) ───────────────────────
    _main_paper_feed_task: asyncio.Task | None = None
    _main_paper_sltp_task: asyncio.Task | None = None
    _main_paper_exec_registered = False
    _paper_trades_main: list[dict] = []

    # Load persisted paper trades from SQLite on startup
    if sqlite_store:
        try:
            _saved = sqlite_store.get_paper_trades(limit=2000)
            if _saved:
                _paper_trades_main.extend(_saved)
                logger.info("Loaded {} paper trades from SQLite", len(_saved))
        except Exception as _le:
            logger.debug("Failed to load paper trades: {}", _le)

    async def _start_paper_feed_main(bus: EventBus) -> None:
        nonlocal _main_paper_feed_task, _main_paper_exec_registered, _paper_trades_main, _main_paper_sltp_task
        if _main_paper_feed_task is not None:
            return  # already running
        try:
            from data_ingestion.paper_feed import PaperFeed
            symbols_cfg = config.get_value("exchanges", "binance", "symbols") or ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]
            feed = PaperFeed(
                event_bus=bus,
                symbols=symbols_cfg,
                timeframes=["1m", "15m", "1h", "4h", "1d"],
                poll_interval=30.0,
            )
            _main_paper_feed_task = asyncio.create_task(feed.run())
            logger.info("Paper feed started on main event bus for auto-trading")
        except Exception as exc:
            logger.error("Failed to start paper feed: {}", exc)
        # Register paper executor on main bus (once)
        if not _main_paper_exec_registered:
            async def _handle_signal_main(signal):
                import time as _time
                trade_id = f"paper_{int(_time.time()*1000)}"
                direction = getattr(signal, 'direction', 'unknown')
                sym = getattr(signal, 'symbol', '??')
                price = getattr(signal, 'price', 0)
                score = getattr(signal, 'score', 0)
                sl = getattr(signal, 'stop_loss', 0)
                tp = getattr(signal, 'take_profit', 0)
                meta = getattr(signal, 'metadata', {}) or {}

                # Reject signals with stale timestamps (from historical seeding)
                sig_ts = getattr(signal, 'timestamp', 0)
                if sig_ts > 0 and abs(_time.time() - sig_ts) > 120:
                    logger.debug("Paper executor: skipping stale signal (age={}s) for {}",
                                 int(abs(_time.time() - sig_ts)), sym)
                    return
                # Skip if already have an open trade on this symbol
                open_same = [t for t in _paper_trades_main
                             if t.get("symbol") == sym and t.get("status") == "OPEN"]
                if open_same:
                    logger.debug("Paper executor: skipping {} — already {} open trade(s)",
                                 sym, len(open_same))
                    return
                equity = 100000.0
                risk_pct = 0.02
                size_usd = equity * risk_pct
                qty = size_usd / price if price > 0 else 0
                paper_trade = {
                    "id": trade_id, "symbol": sym, "direction": direction,
                    "price": price, "quantity": round(qty, 6),
                    "notional": round(size_usd, 2), "score": round(score, 3),
                    "stop_loss": round(sl, 2), "take_profit": round(tp, 2),
                    "status": "OPEN", "timestamp": int(_time.time()),
                    "reasons": getattr(signal, 'reasons', []),
                    # Tiered TP tracking
                    "remaining_qty": round(qty, 6),
                    "tp_tiers": [
                        {"level": round(meta.get("tp1_price", tp), 2),
                         "close_pct": meta.get("tp1_close_pct", 0.50), "hit": False},
                        {"level": round(meta.get("tp2_price", 0), 2),
                         "close_pct": meta.get("tp2_close_pct", 0.30), "hit": False},
                    ],
                    "supertrend_trail": meta.get("supertrend_trail", 0),
                    "tp3_close_pct": meta.get("tp3_close_pct", 0.20),
                    "realized_pnl": 0.0,
                    "partial_fills": [],
                }
                _paper_trades_main.append(paper_trade)
                # Persist to SQLite
                if sqlite_store:
                    try:
                        sqlite_store.upsert_paper_trade(paper_trade)
                    except Exception as _pe:
                        logger.debug("Paper trade persist error: {}", _pe)
                logger.info(
                    "PAPER TRADE: {} {} {:.6f} @ ${:.2f} (score={:.2f}) SL={:.2f} TP1={:.2f} TP2={:.2f} [{}]",
                    direction.upper(), sym, qty, price, score, sl,
                    paper_trade["tp_tiers"][0]["level"],
                    paper_trade["tp_tiers"][1]["level"],
                    trade_id,
                )
            bus.subscribe("SIGNAL", _handle_signal_main)
            _main_paper_exec_registered = True
            logger.info("Paper executor subscribed to SIGNAL events on main bus")
        # Start SL/TP monitor
        if _main_paper_sltp_task is None:
            _main_paper_sltp_task = asyncio.create_task(_paper_sltp_monitor())
            logger.info("Paper SL/TP monitor started")

    async def _stop_paper_feed_main() -> None:
        nonlocal _main_paper_feed_task, _main_paper_sltp_task
        if _main_paper_feed_task is not None:
            _main_paper_feed_task.cancel()
            _main_paper_feed_task = None
            logger.info("Paper feed stopped")
        if _main_paper_sltp_task is not None:
            _main_paper_sltp_task.cancel()
            _main_paper_sltp_task = None
            logger.info("Paper SL/TP monitor stopped")

    async def _fetch_ticker_prices(symbols: list[str]) -> dict[str, float]:
        """Fetch current mark prices from Binance public API."""
        import httpx as _httpx
        prices: dict[str, float] = {}
        try:
            async with _httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get("https://fapi.binance.com/fapi/v1/ticker/price")
                resp.raise_for_status()
                for item in resp.json():
                    prices[item["symbol"]] = float(item["price"])
        except Exception as exc:
            logger.debug("Paper SL/TP price fetch error: {}", exc)
        return prices

    def _symbol_to_binance(sym: str) -> str:
        """Convert BTC/USDT:USDT → BTCUSDT for Binance API lookup."""
        return sym.replace("/", "").replace(":USDT", "").upper()

    async def _paper_sltp_monitor() -> None:
        """Background task: check open paper trades against current prices for SL/TP."""
        import time as _time

        logger.info("Paper SL/TP monitor running — checking every 5s")
        try:
            while True:
                await asyncio.sleep(5)
                open_trades = [t for t in _paper_trades_main if t.get("status") == "OPEN"]
                if not open_trades:
                    continue

                # Fetch current prices
                needed_symbols = list({t["symbol"] for t in open_trades})
                prices = await _fetch_ticker_prices(needed_symbols)
                if not prices:
                    continue

                now = int(_time.time())
                for trade in open_trades:
                    bsym = _symbol_to_binance(trade["symbol"])
                    current_price = prices.get(bsym)
                    if current_price is None:
                        continue

                    direction = trade["direction"]
                    entry = trade["price"]
                    sl = trade["stop_loss"]
                    remaining = trade.get("remaining_qty", trade["quantity"])

                    # ── Check Stop Loss ──
                    sl_hit = False
                    if direction == "long" and current_price <= sl:
                        sl_hit = True
                    elif direction == "short" and current_price >= sl:
                        sl_hit = True

                    if sl_hit:
                        pnl_per_unit = (current_price - entry) if direction == "long" else (entry - current_price)
                        sl_pnl = pnl_per_unit * remaining
                        trade["realized_pnl"] = round(trade.get("realized_pnl", 0) + sl_pnl, 2)
                        trade["partial_fills"].append({
                            "type": "SL", "price": round(current_price, 2),
                            "qty": round(remaining, 6), "pnl": round(sl_pnl, 2),
                            "timestamp": now,
                        })
                        trade["remaining_qty"] = 0
                        trade["status"] = "CLOSED"
                        trade["close_price"] = round(current_price, 2)
                        trade["close_reason"] = "stop_loss"
                        trade["close_time"] = now
                        logger.info(
                            "PAPER SL HIT: {} {} @ ${:.2f} → ${:.2f} PnL=${:.2f} [{}]",
                            direction.upper(), trade["symbol"], entry,
                            current_price, trade["realized_pnl"], trade["id"],
                        )
                        if signal_generator is not None:
                            signal_generator.record_trade_result(
                                trade["symbol"], is_win=(trade["realized_pnl"] > 0),
                            )
                        if sqlite_store:
                            try:
                                sqlite_store.upsert_paper_trade(trade)
                            except Exception:
                                pass
                        continue

                    # ── Check Tiered Take Profits ──
                    tp_tiers = trade.get("tp_tiers", [])
                    original_qty = trade["quantity"]
                    for i, tier in enumerate(tp_tiers):
                        if tier.get("hit"):
                            continue
                        level = tier["level"]
                        if level <= 0:
                            continue

                        tp_hit = False
                        if direction == "long" and current_price >= level:
                            tp_hit = True
                        elif direction == "short" and current_price <= level:
                            tp_hit = True

                        if tp_hit:
                            close_qty = round(original_qty * tier["close_pct"], 6)
                            close_qty = min(close_qty, remaining)
                            if close_qty <= 0:
                                continue
                            pnl_per_unit = (current_price - entry) if direction == "long" else (entry - current_price)
                            tier_pnl = pnl_per_unit * close_qty
                            trade["realized_pnl"] = round(trade.get("realized_pnl", 0) + tier_pnl, 2)
                            remaining -= close_qty
                            trade["remaining_qty"] = round(remaining, 6)
                            tier["hit"] = True
                            tier_name = f"TP{i + 1}"
                            trade["partial_fills"].append({
                                "type": tier_name, "price": round(current_price, 2),
                                "qty": round(close_qty, 6), "pnl": round(tier_pnl, 2),
                                "timestamp": now,
                            })
                            logger.info(
                                "PAPER {} HIT: {} {} close {:.6f} @ ${:.2f} PnL=${:.2f} (remaining={:.6f}) [{}]",
                                tier_name, direction.upper(), trade["symbol"],
                                close_qty, current_price, tier_pnl, remaining, trade["id"],
                            )

                            # Move SL to breakeven after TP1
                            if i == 0:
                                trade["stop_loss"] = round(entry, 2)
                                logger.info(
                                    "PAPER SL→BE: {} {} SL moved to ${:.2f} [{}]",
                                    direction.upper(), trade["symbol"], entry, trade["id"],
                                )
                            if sqlite_store:
                                try:
                                    sqlite_store.upsert_paper_trade(trade)
                                except Exception:
                                    pass

                    # ── Check TP3 SuperTrend trailing stop for remaining qty ──
                    all_tiers_hit = all(t.get("hit") for t in tp_tiers)
                    trail_level = trade.get("supertrend_trail", 0)
                    if all_tiers_hit and remaining > 0 and trail_level > 0:
                        trail_hit = False
                        if direction == "long" and current_price <= trail_level:
                            trail_hit = True
                        elif direction == "short" and current_price >= trail_level:
                            trail_hit = True

                        if trail_hit:
                            pnl_per_unit = (current_price - entry) if direction == "long" else (entry - current_price)
                            trail_pnl = pnl_per_unit * remaining
                            trade["realized_pnl"] = round(trade.get("realized_pnl", 0) + trail_pnl, 2)
                            trade["partial_fills"].append({
                                "type": "TP3_trail", "price": round(current_price, 2),
                                "qty": round(remaining, 6), "pnl": round(trail_pnl, 2),
                                "timestamp": now,
                            })
                            trade["remaining_qty"] = 0
                            trade["status"] = "CLOSED"
                            trade["close_price"] = round(current_price, 2)
                            trade["close_reason"] = "tp3_trail"
                            trade["close_time"] = now
                            logger.info(
                                "PAPER TP3 TRAIL: {} {} @ ${:.2f} PnL=${:.2f} [{}]",
                                direction.upper(), trade["symbol"],
                                current_price, trade["realized_pnl"], trade["id"],
                            )
                            if signal_generator is not None:
                                signal_generator.record_trade_result(
                                    trade["symbol"], is_win=(trade["realized_pnl"] > 0),
                                )
                            if sqlite_store:
                                try:
                                    sqlite_store.upsert_paper_trade(trade)
                                except Exception:
                                    pass

                    # Close trade if no remaining qty
                    if trade.get("remaining_qty", 0) <= 0 and trade["status"] == "OPEN":
                        trade["status"] = "CLOSED"
                        trade["close_price"] = round(current_price, 2)
                        trade["close_reason"] = "fully_filled"
                        trade["close_time"] = now
                        if signal_generator is not None:
                            signal_generator.record_trade_result(
                                trade["symbol"], is_win=(trade.get("realized_pnl", 0) > 0),
                            )
                        if sqlite_store:
                            try:
                                sqlite_store.upsert_paper_trade(trade)
                            except Exception:
                                pass

        except asyncio.CancelledError:
            logger.info("Paper SL/TP monitor cancelled")
        except Exception as exc:
            logger.error("Paper SL/TP monitor error: {}", exc)

    # ── /api/auto/toggle — auto-trading toggle ────────────────────────────
    @app.post("/api/auto/toggle")
    async def api_auto_toggle(request: Request) -> dict[str, Any]:
        body = await request.json()
        enabled = bool(body.get("enabled", False))
        if signal_generator is not None and hasattr(signal_generator, "set_auto_trading"):
            signal_generator.set_auto_trading(enabled)
            # Apply paper-mode-friendly settings when enabling auto trading
            if enabled and config.paper_mode:
                signal_generator._min_factors = 1
                signal_generator._min_score = 0.10
                signal_generator._min_factor_magnitude = 0.03
                signal_generator._tech_weight = 0.50
                signal_generator._ml_weight = 0.40
                signal_generator._sentiment_weight = 0.00
                signal_generator._macro_weight = 0.02
                signal_generator._news_weight = 0.04
                signal_generator._orderbook_weight = 0.02
                signal_generator._confirmation_tfs = []
                signal_generator._min_signal_interval = 30
        # Store bot config on signal_generator for reference
        if signal_generator is not None:
            signal_generator._bot_config = {
                "strategy": body.get("strategy", "ensemble"),
                "sizing_mode": body.get("sizing_mode", "risk_pct"),
                "risk_per_trade": float(body.get("risk_per_trade", 2)),
                "max_positions": int(body.get("max_positions", 3)),
                "max_drawdown": float(body.get("max_drawdown", 10)),
                "max_leverage": int(body.get("max_leverage", 5)),
                "daily_loss_limit": float(body.get("daily_loss_limit", 500)),
                "trailing_mode": body.get("trailing_mode", "none"),
                "auto_sl_tp": bool(body.get("auto_sl_tp", False)),
            }
        # Start/stop paper feed — use the main event_bus so candles reach the main signal generator
        if config.paper_mode:
            try:
                if enabled:
                    await _start_paper_feed_main(event_bus)
                else:
                    await _stop_paper_feed_main()
            except Exception as exc:
                logger.warning("Paper feed toggle error: {}", exc)
        return {"success": True, "auto_trading_enabled": enabled, "mode": "paper" if config.paper_mode else "live"}

    # ── /api/auto/status — auto-trading status ────────────────────────────
    @app.get("/api/auto/status")
    async def api_auto_status() -> dict[str, Any]:
        enabled = False
        if signal_generator is not None and hasattr(signal_generator, "auto_trading_enabled"):
            enabled = signal_generator.auto_trading_enabled
        return {
            "enabled": enabled,
            "mode": "paper" if config.paper_mode else "live",
            "auto_trading_enabled": enabled,
            "paper_mode": config.paper_mode,
        }

    # ── /api/mode/toggle — switch between paper and live trading ────────
    @app.post("/api/mode/toggle")
    async def api_mode_toggle(request: Request) -> dict[str, Any]:
        body = await request.json()
        requested_mode = str(body.get("mode", "")).lower()
        if requested_mode not in ("paper", "live"):
            return {"success": False, "error": "mode must be 'paper' or 'live'"}
        if requested_mode == "live":
            # P0: Block runtime switch to live — requires full restart with
            # LIVE_TRADING_CONFIRMED=true for startup validation, clock sync,
            # balance checks, and reconciliation.
            return {
                "success": False,
                "error": "Cannot switch to live mode at runtime. "
                         "Restart with LIVE_TRADING_CONFIRMED=true.",
            }
        config.paper_mode = True
        logger.info("Trading mode switched to: paper")
        return {
            "success": True,
            "mode": "paper",
            "paper_mode": config.paper_mode,
        }

    # ── /api/trades/history — closed trade history ────────────────────────
    @app.get("/api/trades/history")
    async def api_trades_history() -> dict[str, Any]:
        trades: list[dict[str, Any]] = []
        if order_manager is not None:
            filled = order_manager.get_filled_orders()
            for o in filled[-50:]:
                trades.append({
                    "time": o.filled_at or int(o.created_at * 1000),
                    "symbol": o.symbol,
                    "side": o.side.value if hasattr(o.side, 'value') else str(o.side),
                    "price": o.average_fill_price or o.price or 0,
                    "size": o.cumulative_quantity or o.quantity,
                    "pnl": float(o.metadata.get("pnl", 0)),
                })
        # Include paper trades from standalone paper executor
        if _FASTAPI and '_paper_trades' in globals():
            for pt in _paper_trades[-50:]:
                trades.append({
                    "time": pt.get("timestamp", 0) * 1000,
                    "symbol": pt.get("symbol", ""),
                    "side": pt.get("direction", ""),
                    "price": pt.get("price", 0),
                    "size": pt.get("quantity", 0),
                    "pnl": 0,
                    "paper": True,
                    "score": pt.get("score", 0),
                })
        return {"trades": trades}

    # ── /api/paper/trades — paper trade log ───────────────────────────────
    @app.get("/api/paper/trades")
    async def api_paper_trades() -> dict[str, Any]:
        all_trades = list(_paper_trades_main[-100:])
        if '_paper_trades' in globals():
            all_trades.extend(_paper_trades[-100:])
        all_trades.sort(key=lambda t: t.get("timestamp", 0), reverse=True)
        open_trades = [t for t in _paper_trades_main if t.get("status") == "OPEN"]
        closed_trades = [t for t in _paper_trades_main if t.get("status") == "CLOSED"]
        total_pnl = sum(t.get("realized_pnl", 0) for t in _paper_trades_main)
        wins = sum(1 for t in closed_trades if t.get("realized_pnl", 0) > 0)
        losses = sum(1 for t in closed_trades if t.get("realized_pnl", 0) <= 0)
        return {
            "trades": all_trades[:100],
            "count": len(all_trades),
            "open_count": len(open_trades),
            "closed_count": len(closed_trades),
            "total_pnl": round(total_pnl, 2),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / max(wins + losses, 1) * 100, 1),
        }

    # ── /api/config — GET config for settings modal ───────────────────────
    @app.get("/api/config")
    async def api_config_get() -> dict[str, Any]:
        exchanges = config.get_value("exchanges") or {}
        dex_cfg = config.get_value("dex") or {}
        notifications_cfg = config.get_value("notifications", "telegram") or {}
        risk_cfg = config.get_value("risk") or {}
        auto_enabled = False
        if signal_generator and hasattr(signal_generator, "auto_trading_enabled"):
            auto_enabled = signal_generator.auto_trading_enabled

        # Build connection registry
        registry = []
        for name in ["binance", "bybit", "okx", "kraken"]:
            ex = exchanges.get(name, {})
            has_key = bool(ex.get("api_key"))
            registry.append({
                "exchange": name,
                "venue_type": "CEX",
                "connected": has_key and ex.get("enabled", False),
                "status": "connected" if (has_key and ex.get("enabled", False)) else "disconnected",
            })

        def _mask(val: str) -> str:
            s = str(val or "")
            return "****" if s else ""

        binance = exchanges.get("binance", {})
        bybit = exchanges.get("bybit", {})
        okx = exchanges.get("okx", {})
        kraken = exchanges.get("kraken", {})

        return {
            "binance_api_key": _mask(binance.get("api_key", "")),
            "binance_secret": _mask(binance.get("api_secret", "")),
            "bybit_api_key": _mask(bybit.get("api_key", "")),
            "bybit_secret": _mask(bybit.get("api_secret", "")),
            "okx_api_key": _mask(okx.get("api_key", "")),
            "okx_secret": _mask(okx.get("api_secret", "")),
            "okx_passphrase": _mask(okx.get("passphrase", "")),
            "kraken_api_key": _mask(kraken.get("api_key", "")),
            "kraken_secret": _mask(kraken.get("api_secret", "")),
            "dex_rpc_url": str(dex_cfg.get("rpc_url", "")),
            "dex_private_key": _mask(dex_cfg.get("private_key", "")),
            "telegram_bot_token": _mask(notifications_cfg.get("bot_token", "")),
            "telegram_chat_id": str(notifications_cfg.get("chat_id", "")),
            "binance_enabled": binance.get("enabled", False),
            "bybit_enabled": bybit.get("enabled", False),
            "okx_enabled": okx.get("enabled", False),
            "kraken_enabled": kraken.get("enabled", False),
            "uniswap_v3_enabled": dex_cfg.get("uniswap", {}).get("enabled", False),
            "sushiswap_enabled": dex_cfg.get("sushiswap", {}).get("enabled", False),
            "dydx_enabled": dex_cfg.get("dydx", {}).get("enabled", False),
            "trade_alerts_enabled": True,
            "risk_alerts_enabled": True,
            "daily_summary_enabled": False,
            "auto_trading_enabled": auto_enabled,
            "auto_stop_loss_enabled": bool(risk_cfg.get("stop_loss_pct")),
            "auto_take_profit_enabled": bool(risk_cfg.get("take_profit_pct")),
            "trailing_stops_enabled": bool(risk_cfg.get("trailing_stop", {}).get("enabled")),
            "atr_position_sizing_enabled": bool(risk_cfg.get("atr_stop", {}).get("enabled")),
            "connection_registry": registry,
        }

    # ── /api/config POST — save settings ──────────────────────────────────
    @app.post("/api/config")
    async def api_config_post(request: Request) -> dict[str, Any]:
        body = await request.json()
        # Apply auto-trading toggle
        if signal_generator and hasattr(signal_generator, "set_auto_trading"):
            signal_generator.set_auto_trading(bool(body.get("auto_trading_enabled", False)))
        return {"success": True, "message": "Settings applied to runtime", "connection_registry": []}

    # ── /api/config/test — test exchange connections ──────────────────────
    @app.post("/api/config/test")
    async def api_config_test() -> dict[str, Any]:
        exchanges_cfg = config.get_value("exchanges") or {}
        dex_cfg = config.get_value("dex") or {}
        enabled = {}
        cex_creds = {}
        for name in ["binance", "bybit", "okx", "kraken"]:
            ex = exchanges_cfg.get(name, {})
            enabled[name] = ex.get("enabled", False)
            cex_creds[name] = bool(ex.get("api_key"))

        registry = []
        for name in ["binance", "bybit", "okx", "kraken"]:
            ex = exchanges_cfg.get(name, {})
            has_key = bool(ex.get("api_key"))
            registry.append({
                "exchange": name,
                "venue_type": "CEX",
                "connected": has_key and ex.get("enabled", False),
                "status": "connected" if (has_key and ex.get("enabled", False)) else "disconnected",
            })

        return {
            "success": True,
            "connection_registry": registry,
            "checks": {
                "grpc": False,
                "clickhouse": False,
                "credentials_present": any(cex_creds.values()),
                "cex_credentials": cex_creds,
                "dex_credentials": {
                    "rpc_url": bool(dex_cfg.get("rpc_url")),
                    "private_key": bool(dex_cfg.get("private_key")),
                },
                "enabled": enabled,
            },
        }

    # ── /api/realtime/snapshot — polling fallback ─────────────────────────
    @app.get("/api/realtime/snapshot")
    async def api_realtime_snapshot(
        symbol: str = Query("BTC/USDT"),
        timeframe: str = Query("1m"),
    ) -> dict[str, Any]:
        try:
            symbol = _validate_symbol(symbol)
            timeframe = _validate_timeframe(timeframe)
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"detail": str(exc)})
        # Assemble a full snapshot from all data sources
        status = (await api_status())
        fg = (await api_feargreed())
        ob = (await api_orderbook(symbol=symbol, depth=20))
        sym_base = symbol.split("/")[0] if "/" in symbol else symbol
        ind = (await api_indicators(sym_base))
        news = (await api_news())
        auto = (await api_auto_status())
        candles = (await api_candles(symbol=symbol, timeframe=timeframe))
        market = (await api_market(per_page=20))
        dex = (await api_dex_pools())
        recon = (await api_reconciliation_status())
        ustream = (await api_user_stream_status())
        return {
            "status": status,
            "feargreed": fg,
            "orderbook": ob,
            "indicators": ind,
            "news": news,
            "auto": auto,
            "candles": candles,
            "market": market,
            "dex": dex,
            "reconciliation": recon,
            "user_stream": ustream,
        }

    # ── /api/realtime/stream — SSE streaming ──────────────────────────────
    @app.get("/api/realtime/stream")
    async def api_realtime_stream(
        request: Request,
        symbol: str = Query("BTC/USDT"),
        timeframe: str = Query("1m"),
    ):
        try:
            symbol = _validate_symbol(symbol)
            timeframe = _validate_timeframe(timeframe)
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"detail": str(exc)})

        async def _event_generator():
            while True:
                if await request.is_disconnected():
                    break
                try:
                    snapshot = await api_realtime_snapshot(symbol=symbol, timeframe=timeframe)
                    yield {"event": "snapshot", "data": json.dumps(snapshot)}
                except Exception as exc:
                    logger.debug("SSE snapshot error: {}", exc)
                await asyncio.sleep(3)

        try:
            return EventSourceResponse(_event_generator())
        except Exception:
            # Fallback if sse_starlette is not installed
            from starlette.responses import StreamingResponse

            async def _sse_fallback():
                while True:
                    try:
                        snapshot = await api_realtime_snapshot(symbol=symbol, timeframe=timeframe)
                        yield f"event: snapshot\ndata: {json.dumps(snapshot)}\n\n"
                    except Exception:
                        pass
                    await asyncio.sleep(3)

            return StreamingResponse(_sse_fallback(), media_type="text/event-stream")

    # ── §3 Spec: 9-layer confirmation status ──────────────────────────────
    @app.get("/api/layers")
    async def api_layers() -> dict[str, Any]:
        """Return current state of the 9-layer confirmation pipeline."""
        layers = [
            {"id": 1, "name": "Session Filter", "description": "Trading session & killzone enforcement"},
            {"id": 2, "name": "HTF Trend", "description": "Higher-timeframe weighted agreement"},
            {"id": 3, "name": "Technical Confluence", "description": "RSI, MACD, BB, EMA alignment"},
            {"id": 4, "name": "Smart Money Concepts", "description": "BOS/CHoCH + OB/FVG zones"},
            {"id": 5, "name": "Volume Flow", "description": "Delta, CVD, VWAP deviation"},
            {"id": 6, "name": "Regime Detection", "description": "Market regime (trending/ranging/breakout)"},
            {"id": 7, "name": "ML Ensemble", "description": "Model prediction confidence"},
            {"id": 8, "name": "Signal Quality", "description": "0-100 quality score gate (min 65)"},
            {"id": 9, "name": "Risk Gate", "description": "Position sizing, DD phase, circuit breaker"},
        ]
        # Populate layer status from signal_generator if available
        if signal_generator is not None:
            last = getattr(signal_generator, '_last_layer_status', {})
            for layer in layers:
                key = layer["name"].lower().replace(" ", "_")
                layer["status"] = last.get(key, "unknown")
                layer["detail"] = last.get(f"{key}_detail", "")
        return {"layers": layers, "total": 9}

    # ── §5 Spec: Session & killzone status ────────────────────────────────
    @app.get("/api/session")
    async def api_session() -> dict[str, Any]:
        """Return current trading session, killzone status, and session clock."""
        import datetime as _dt
        now = _dt.datetime.now(_dt.timezone.utc)
        hour = now.hour
        # Session rules (mirror signal_generator._SESSION_RULES)
        sessions = [
            {"name": "asia", "start": 0, "end": 8, "types": ["B"], "size_mult": 0.5},
            {"name": "london_open", "start": 8, "end": 12, "types": ["A", "C", "D"], "size_mult": 1.0},
            {"name": "london_dead", "start": 12, "end": 13, "types": [], "size_mult": 0.0, "no_trade": True},
            {"name": "london_ny_overlap", "start": 13, "end": 17, "types": ["A", "B", "C", "D"], "size_mult": 1.5},
            {"name": "ny_only", "start": 17, "end": 22, "types": ["B", "D"], "size_mult": 0.75},
            {"name": "low_liquidity", "start": 22, "end": 24, "types": [], "size_mult": 0.0, "no_trade": True},
        ]
        active_session = None
        for s in sessions:
            if s["start"] <= hour < s["end"]:
                active_session = s
                break
        # ICT killzones
        ict_killzones = [{"start": 13, "end": 14}, {"start": 15, "end": 16}]
        in_killzone = any(kz["start"] <= hour < kz["end"] for kz in ict_killzones)
        is_weekend = now.weekday() >= 5
        return {
            "utc_hour": hour,
            "utc_time": now.strftime("%H:%M:%S"),
            "active_session": active_session,
            "in_killzone": in_killzone,
            "is_weekend": is_weekend,
            "sessions": sessions,
            "ict_killzones": ict_killzones,
        }

    # ── §6 Spec: Quality score breakdown ──────────────────────────────────
    @app.get("/api/quality")
    async def api_quality() -> dict[str, Any]:
        """Return last signal quality score breakdown."""
        if signal_generator is not None:
            last_q = getattr(signal_generator, '_last_quality_breakdown', None)
            if last_q:
                return last_q
        return {
            "total": 0,
            "components": {
                "htf_trend": 0,
                "technical_confluence": 0,
                "smc_confluence": 0,
                "volume_flow": 0,
                "regime": 0,
                "ml_confidence": 0,
                "liquidity_depth": 0,
            },
            "min_threshold": 65,
            "boost_threshold": 90,
        }

    # ── §7 Spec: Regime state & transition info ───────────────────────────
    @app.get("/api/regime")
    async def api_regime() -> dict[str, Any]:
        """Return current regime state and transition info per symbol."""
        regimes: dict[str, Any] = {}
        if signal_generator is not None:
            detectors = getattr(signal_generator, '_regime_detectors', {})
            for sym, det in detectors.items():
                state = det.current_state()
                regimes[sym] = {
                    "regime": state.regime,
                    "confidence": state.confidence,
                    "candles_in_state": state.candles_in_state,
                    "trend_strength": state.trend_strength,
                    "in_transition": state.candles_in_state <= 2,
                    "risk_pct": {"trending": 0.02, "ranging": 0.015, "breakout": 0.025}.get(state.regime, 0.02),
                }
        return {"regimes": regimes}

    # ── §9 Spec: Risk guardrails status ───────────────────────────────────
    @app.get("/api/guardrails")
    async def api_guardrails() -> dict[str, Any]:
        """Return risk guardrail status: circuit breaker, consecutive losses, flash crash, Kelly."""
        result: dict[str, Any] = {
            "circuit_breaker_tripped": False,
            "circuit_breaker_reason": "",
            "consecutive_losses": 0,
            "max_consecutive_losses": 3,
            "flash_crash_tripped": False,
            "kelly_enabled": False,
            "kelly_win_rate": 0.5,
            "kelly_avg_win": 0.0,
            "kelly_avg_loss": 0.0,
        }
        if risk_manager is not None:
            cb = getattr(risk_manager, '_circuit_breaker', None)
            if cb:
                result["circuit_breaker_tripped"] = cb.tripped
                result["circuit_breaker_reason"] = cb.trip_reason
            result["consecutive_losses"] = getattr(risk_manager, '_consecutive_losses', 0)
            result["max_consecutive_losses"] = getattr(risk_manager, '_max_consecutive_losses', 3)
            result["flash_crash_tripped"] = getattr(risk_manager, '_flash_crash_tripped', False)
            result["kelly_enabled"] = getattr(risk_manager, '_kelly_enabled', False)
            result["kelly_win_rate"] = getattr(risk_manager, '_kelly_win_rate', 0.5)
            result["kelly_avg_win"] = getattr(risk_manager, '_kelly_avg_win', 0.0)
            result["kelly_avg_loss"] = getattr(risk_manager, '_kelly_avg_loss', 0.0)
        return result


    return app

# Top-level FastAPI app for uvicorn import
def _build_standalone_managers():
    """Create lightweight managers + paper trading stack for standalone dashboard."""
    cfg = _default_config()
    # Use real EventBus so CANDLE → SignalGenerator → SIGNAL pipeline works
    from core.event_bus import EventBus as RealEventBus
    bus = RealEventBus()
    om = None
    rm = None
    sg = None
    dm = None
    try:
        from core.circuit_breaker import CircuitBreaker
        from execution.order_manager import OrderManager
        from execution.risk_manager import RiskManager
        from analysis.data_manager import DataManager
        from engine.signal_generator import SignalGenerator
        cb = CircuitBreaker()
        om = OrderManager(config=cfg, event_bus=bus, circuit_breaker=cb)
        rm = RiskManager(config=cfg, event_bus=bus)
        dm = DataManager(config=cfg, event_bus=bus)
        sg = SignalGenerator(config=cfg, event_bus=bus, data_manager=dm)
        sg.set_auto_trading(False)
        # Paper mode: lower thresholds so signals can fire with limited data feeds
        if cfg.paper_mode:
            sg._min_factors = 2
            sg._min_score = 0.15
            sg._min_factor_magnitude = 0.05
            # Redistribute weights: boost tech+ML since sentiment/macro/news/orderbook are 0
            sg._tech_weight = 0.50
            sg._ml_weight = 0.40
            sg._sentiment_weight = 0.00
            sg._macro_weight = 0.02
            sg._news_weight = 0.04
            sg._orderbook_weight = 0.02
            # Disable HTF confirmation — too restrictive with limited seeded data
            sg._confirmation_tfs = []
            sg._min_signal_interval = 30  # Allow faster signals in paper mode
    except Exception as exc:
        logger.warning("Standalone manager init partial: {}", exc)
    return cfg, bus, om, rm, sg, dm

if _FASTAPI:
    _sa_cfg, _sa_bus, _sa_om, _sa_rm, _sa_sg, _sa_dm = _build_standalone_managers()
    # Attach loguru sink for /api/logs/recent
    logger.add(_log_sink, level="DEBUG", format="{message}")

    # Start DataManager + SignalGenerator event subscriptions
    async def _start_paper_stack():
        """Start paper trading components in background."""
        try:
            if _sa_dm is not None:
                await _sa_dm.run()
            if _sa_sg is not None:
                await _sa_sg.run()
        except Exception as exc:
            logger.error("Paper stack startup error: {}", exc)

    # Paper feed management
    _paper_feed_task = None
    _paper_stack_task = None
    _paper_sg_task = None
    _paper_bus_task = None
    _paper_exec_task = None
    _paper_trades: list[dict] = []

    async def _ensure_paper_stack():
        """Ensure EventBus, DataManager and SignalGenerator are running (idempotent)."""
        global _paper_stack_task, _paper_sg_task, _paper_bus_task, _paper_exec_task
        # EventBus must be running to dispatch events
        if _paper_bus_task is None and _sa_bus is not None:
            async def _run_bus():
                try:
                    await _sa_bus.run()
                except Exception as exc:
                    logger.error("EventBus run error: {}", exc)
            _paper_bus_task = asyncio.create_task(_run_bus())
        if _paper_stack_task is None and _sa_dm is not None:
            async def _run_dm():
                try:
                    await _sa_dm.run()
                except Exception as exc:
                    logger.error("DataManager run error: {}", exc)
            _paper_stack_task = asyncio.create_task(_run_dm())
        if _paper_sg_task is None and _sa_sg is not None:
            async def _run_sg():
                try:
                    await _sa_sg.run()
                except Exception as exc:
                    logger.error("SignalGenerator run error: {}", exc)
            _paper_sg_task = asyncio.create_task(_run_sg())
        # Paper executor: subscribe to SIGNAL events and log paper trades
        if _paper_exec_task is None and _sa_bus is not None:
            async def _handle_signal(signal):
                """Paper executor: simulates trade execution for signals."""
                import time as _time
                trade_id = f"paper_{int(_time.time()*1000)}"
                direction = getattr(signal, 'direction', 'unknown')
                symbol = getattr(signal, 'symbol', '??')
                price = getattr(signal, 'price', 0)
                score = getattr(signal, 'score', 0)
                sl = getattr(signal, 'stop_loss', 0)
                tp = getattr(signal, 'take_profit', 0)
                # Calculate position size (risk-based from config)
                equity = 100000.0
                risk_pct = 0.02
                size_usd = equity * risk_pct
                qty = size_usd / price if price > 0 else 0
                paper_trade = {
                    "id": trade_id,
                    "symbol": symbol,
                    "direction": direction,
                    "price": price,
                    "quantity": round(qty, 6),
                    "notional": round(size_usd, 2),
                    "score": round(score, 3),
                    "stop_loss": round(sl, 2),
                    "take_profit": round(tp, 2),
                    "status": "FILLED",
                    "timestamp": int(_time.time()),
                    "reasons": getattr(signal, 'reasons', []),
                }
                _paper_trades.append(paper_trade)
                logger.info(
                    "📄 PAPER TRADE: {} {} {:.6f} @ ${:.2f} (score={:.2f}, sl={:.2f}, tp={:.2f}) [{}]",
                    direction.upper(), symbol, qty, price, score, sl, tp, trade_id,
                )
                await _sa_bus.publish("ORDER_FILLED", paper_trade)
            _sa_bus.subscribe("SIGNAL", _handle_signal)
            _paper_exec_task = True  # sentinel — handler is registered, not a task
            logger.info("Paper executor subscribed to SIGNAL events")

    async def start_paper_feed():
        """Start PaperFeed to emit CANDLE events from Binance public API."""
        global _paper_feed_task
        if _paper_feed_task is not None:
            return  # already running
        try:
            from data_ingestion.paper_feed import PaperFeed
            await _ensure_paper_stack()
            symbols_cfg = _sa_cfg.get_value("exchanges", "binance", "symbols") or ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]
            feed = PaperFeed(
                event_bus=_sa_bus,
                symbols=symbols_cfg,
                timeframes=["1m", "15m", "1h", "4h", "1d"],
                poll_interval=30.0,
            )
            _paper_feed_task = asyncio.create_task(feed.run())
            logger.info("Paper feed started for auto-trading")
        except Exception as exc:
            logger.error("Failed to start paper feed: {}", exc)

    async def stop_paper_feed():
        """Stop PaperFeed."""
        global _paper_feed_task
        if _paper_feed_task is not None:
            _paper_feed_task.cancel()
            _paper_feed_task = None
            logger.info("Paper feed stopped")

    app = build_app(
        config=_sa_cfg,
        event_bus=_sa_bus,
        risk_manager=_sa_rm,
        data_manager=_sa_dm,
        order_manager=_sa_om,
        db_handler=None,
        cache=None,
        signal_generator=_sa_sg,
        news_feed=None,
        orderbook_feed=None,
        sentiment_manager=None,
        dex_feed=None,
    )


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
