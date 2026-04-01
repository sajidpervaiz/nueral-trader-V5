from __future__ import annotations

from core.config import Config
from core.event_bus import EventBus
from execution.cex_executor import CEXExecutor
from execution.risk_manager import RiskManager


class KrakenExecutor(CEXExecutor):
    """Kraken-specific CEX executor (spot + futures)."""

    def __init__(self, config: Config, event_bus: EventBus, risk_manager: RiskManager) -> None:
        super().__init__(config, event_bus, risk_manager, exchange_id="kraken")
