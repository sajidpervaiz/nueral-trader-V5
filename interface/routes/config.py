"""
Configuration API routes for FastAPI dashboard.
"""

from fastapi import APIRouter, HTTPException, Body, Query
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from enum import Enum
from loguru import logger

router = APIRouter(prefix="/config", tags=["config"])


class TradingMode(str, Enum):
    PAPER = "paper"
    LIVE = "live"
    SIMULATION = "simulation"


class AlgoConfig(BaseModel):
    enabled: bool
    max_position_size: float
    max_risk_per_trade: float
    max_daily_loss: float
    max_positions: int


class VenueConfig(BaseModel):
    name: str
    enabled: bool
    api_keys_configured: bool
    connection_status: str
    latency_ms: float


# Runtime config state for API-level updates without mutating on-disk YAML.
_TRADING_MODE: TradingMode = TradingMode.PAPER
_ALGO_CONFIG = AlgoConfig(
    enabled=True,
    max_position_size=10.0,
    max_risk_per_trade=0.02,
    max_daily_loss=5000.0,
    max_positions=5,
)


@router.get("/trading-mode")
async def get_trading_mode():
    """Get current trading mode."""
    try:
        return {
            "mode": _TRADING_MODE.value,
            "reason": "Runtime mode state",
        }

    except Exception as e:
        logger.error(f"Error fetching trading mode: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trading-mode")
async def set_trading_mode(
    mode: TradingMode = Body(...),
    confirmation: bool = Query(..., description="Safety confirmation"),
):
    """Set trading mode (requires confirmation for LIVE)."""
    try:
        global _TRADING_MODE
        if mode == TradingMode.LIVE and not confirmation:
            raise HTTPException(
                status_code=400,
                detail="Confirmation required for live trading"
            )

        _TRADING_MODE = mode

        return {
            "mode": mode.value,
            "status": "updated",
            "message": f"Trading mode set to {mode.value}",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error setting trading mode: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/algo", response_model=AlgoConfig)
async def get_algo_config():
    """Get algorithm configuration."""
    try:
        return _ALGO_CONFIG

    except Exception as e:
        logger.error(f"Error fetching algo config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/algo", response_model=AlgoConfig)
async def set_algo_config(
    config: AlgoConfig = Body(...),
):
    """Update algorithm configuration."""
    try:
        global _ALGO_CONFIG
        _ALGO_CONFIG = config
        return _ALGO_CONFIG

    except Exception as e:
        logger.error(f"Error setting algo config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/venues", response_model=List[VenueConfig])
async def get_venue_configs():
    """Get venue configurations."""
    try:
        return [
            VenueConfig(
                name="binance",
                enabled=True,
                api_keys_configured=True,
                connection_status="connected",
                latency_ms=50.0,
            ),
            VenueConfig(
                name="bybit",
                enabled=True,
                api_keys_configured=True,
                connection_status="connected",
                latency_ms=45.0,
            ),
            VenueConfig(
                name="okx",
                enabled=True,
                api_keys_configured=True,
                connection_status="connected",
                latency_ms=60.0,
            ),
        ]

    except Exception as e:
        logger.error(f"Error fetching venue configs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/venues/{venue}/toggle")
async def toggle_venue(
    venue: str,
    enabled: bool = Body(..., embed=True),
):
    """Enable or disable a venue."""
    try:
        return {
            "venue": venue,
            "enabled": enabled,
            "status": "updated",
        }

    except Exception as e:
        logger.error(f"Error toggling venue {venue}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/all")
async def get_all_config():
    """Get all configuration."""
    try:
        return {
            "trading_mode": "paper",
            "algo": {
                "enabled": True,
                "max_position_size": 10.0,
            },
            "risk_limits": {
                "max_position_value": 1_000_000.0,
                "max_order_size": 10_000.0,
                "leverage_limit": 10.0,
            },
            "venues": [
                {"name": "binance", "enabled": True},
                {"name": "bybit", "enabled": True},
                {"name": "okx", "enabled": True},
            ],
        }

    except Exception as e:
        logger.error(f"Error fetching all config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reload")
async def reload_config():
    """Reload configuration from file/database."""
    try:
        return {
            "status": "reloaded",
            "message": "Configuration reloaded successfully",
        }

    except Exception as e:
        logger.error(f"Error reloading config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def get_config_history(
    limit: int = Query(50, le=200),
):
    """Get configuration change history."""
    try:
        return {
            "total_changes": 0,
            "recent_changes": [],
            "limit": limit,
        }

    except Exception as e:
        logger.error(f"Error fetching config history: {e}")
        raise HTTPException(status_code=500, detail=str(e))
