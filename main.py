#!/usr/bin/env python3
"""neural-trader-v4 — Hybrid Rust + TypeScript + Python trading engine."""
from __future__ import annotations

import asyncio
import signal
import sys
from pathlib import Path

from loguru import logger

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    logger.info("uvloop event loop policy activated")
except ImportError:
    logger.debug("uvloop not available — using default asyncio event loop")

from core.config import Config
from core.event_bus import EventBus
from core.dispatcher import Dispatcher

from data_ingestion.cex_websocket import CEXWebSocketManager
from data_ingestion.dex_rpc import DEXRPCFeed
from data_ingestion.funding_feed import FundingRateFeed
from data_ingestion.oi_feed import OpenInterestFeed
from data_ingestion.vix_proxy import VIXProxy

from analysis.data_manager import DataManager
from analysis.sentiment import SentimentManager

from engine.signal_generator import SignalGenerator

from execution.risk_manager import RiskManager
from execution.order_manager import OrderManager
from execution.exchange_factory import create_all_executors

from storage.db_handler import DBHandler
from storage.cache import Cache

from monitoring.metrics import Metrics

from interface.dashboard_api import build_app, run_dashboard
from interface.telegram_bot import TelegramNotifier


def _setup_logging(config: Config) -> None:
    log_dir = Path(config.get_value("system", "log_dir") or "logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(
        sys.stdout,
        level=config.log_level,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> — {message}",
    )
    logger.add(
        log_dir / "neural_trader_{time:YYYY-MM-DD}.log",
        level="DEBUG",
        rotation="00:00",
        retention="30 days",
        compression="gz",
    )


async def main() -> None:
    config = Config.get()
    _setup_logging(config)

    logger.info("=" * 60)
    logger.info("  neural-trader-v4  |  paper_mode={}", config.paper_mode)
    logger.info("=" * 60)

    event_bus = EventBus()
    db = DBHandler(config)
    cache = Cache(config)
    metrics = Metrics(config, event_bus)

    ws_manager = CEXWebSocketManager(config, event_bus)
    dex_feed = DEXRPCFeed(config, event_bus)
    funding_feed = FundingRateFeed(config, event_bus)
    oi_feed = OpenInterestFeed(config, event_bus)
    vix_proxy = VIXProxy(config, event_bus)
    sentiment = SentimentManager(config, event_bus)

    data_manager = DataManager(config, event_bus)
    signal_gen = SignalGenerator(config, event_bus, data_manager)
    risk_mgr = RiskManager(config, event_bus)
    order_mgr = OrderManager(config, event_bus, risk_mgr._circuit_breaker)

    executors = create_all_executors(config, event_bus, risk_mgr)

    telegram = TelegramNotifier(config, event_bus)

    app = build_app(config, event_bus, risk_mgr, data_manager, order_mgr, db, cache)

    dispatcher = Dispatcher(
        config=config,
        event_bus=event_bus,
        data_manager=data_manager,
        signal_generator=signal_gen,
        risk_manager=risk_mgr,
        db_handler=db,
        cache=cache,
        metrics=metrics,
    )

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def _signal_handler(*_: object) -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    tasks = [
        asyncio.create_task(dispatcher.start(), name="dispatcher"),
        asyncio.create_task(ws_manager.run(), name="cex_ws"),
        asyncio.create_task(dex_feed.run(), name="dex_rpc"),
        asyncio.create_task(funding_feed.run(), name="funding"),
        asyncio.create_task(oi_feed.run(), name="oi"),
        asyncio.create_task(vix_proxy.run(), name="vix"),
        asyncio.create_task(sentiment.run(), name="sentiment"),
        asyncio.create_task(telegram.run(), name="telegram"),
        asyncio.create_task(order_mgr.run(), name="order_manager"),
    ]

    for executor in executors:
        tasks.append(asyncio.create_task(executor.run(), name=f"exec_{executor.exchange_id}"))

    if app is not None:
        tasks.append(asyncio.create_task(run_dashboard(config, app), name="dashboard"))

    await stop_event.wait()
    logger.info("Initiating graceful shutdown…")

    # Close executors first
    for executor in executors:
        await executor.close()

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    await dispatcher.stop()
    await ws_manager.stop()
    await funding_feed.stop()
    await oi_feed.stop()
    await sentiment.stop()
    await telegram.stop()
    await order_mgr.stop()

    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
