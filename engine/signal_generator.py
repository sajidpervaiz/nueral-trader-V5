from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from core.config import Config
from core.event_bus import EventBus
from analysis.data_manager import DataManager
from analysis.regime import MarketRegime, RegimeState


@dataclass
class TradingSignal:
    exchange: str
    symbol: str
    direction: str
    score: float
    technical_score: float
    ml_score: float
    sentiment_score: float
    macro_score: float
    regime: str
    regime_confidence: float
    price: float
    atr: float
    stop_loss: float
    take_profit: float
    timestamp: int
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_long(self) -> bool:
        return self.direction == "long"

    @property
    def is_short(self) -> bool:
        return self.direction == "short"

    @property
    def risk_reward(self) -> float:
        if self.is_long:
            risk = self.price - self.stop_loss
            reward = self.take_profit - self.price
        else:
            risk = self.stop_loss - self.price
            reward = self.price - self.take_profit
        return reward / risk if risk > 0 else 0.0


class TechnicalScorer:
    def score(self, df: pd.DataFrame) -> float:
        if df is None or len(df) < 5:
            return 0.0
        last = df.iloc[-1]
        scores: list[float] = []

        rsi = last.get("rsi_14", 50.0)
        if rsi < 30:
            scores.append(1.0)
        elif rsi < 40:
            scores.append(0.5)
        elif rsi > 70:
            scores.append(-1.0)
        elif rsi > 60:
            scores.append(-0.5)
        else:
            scores.append(0.0)

        macd = last.get("macd", 0)
        macd_signal = last.get("macd_signal", 0)
        if macd > macd_signal:
            scores.append(0.5)
        else:
            scores.append(-0.5)

        trend = last.get("trend_ema", 0.0)
        scores.append(trend * 0.5)

        bb_pct = last.get("bb_pct", 0.5)
        if bb_pct < 0.2:
            scores.append(0.5)
        elif bb_pct > 0.8:
            scores.append(-0.5)
        else:
            scores.append(0.0)

        if not scores:
            return 0.0
        return float(np.clip(np.mean(scores), -1.0, 1.0))


class MLScorer:
    def __init__(self) -> None:
        self._model = None

    def score(self, df: pd.DataFrame) -> float:
        if df is None or len(df) < 50 or self._model is None:
            return self._heuristic_score(df)
        return self._heuristic_score(df)

    def _heuristic_score(self, df: pd.DataFrame) -> float:
        if df is None or len(df) < 20:
            return 0.0
        last = df.iloc[-1]
        returns_5 = last.get("returns_5", 0.0)
        vol = last.get("vol_20", 0.3)
        if vol == 0:
            return 0.0
        z_score = returns_5 / (vol / (252 ** 0.5))
        return float(np.clip(z_score * 0.1, -1.0, 1.0))


class SignalGenerator:
    def __init__(
        self,
        config: Config,
        event_bus: EventBus,
        data_manager: DataManager,
    ) -> None:
        self.config = config
        self.event_bus = event_bus
        self.data_manager = data_manager
        self._technical_scorer = TechnicalScorer()
        self._ml_scorer = MLScorer()
        self._running = False
        self._sentiment_score: float = 0.0
        self._macro_score: float = 0.0
        self._last_signal_time: dict[str, float] = {}
        self._min_signal_interval = 60.0

        signals_cfg = config.get_value("signals") or {}
        self._ml_weight = float(signals_cfg.get("ml_weight", 0.4))
        self._tech_weight = float(signals_cfg.get("technical_weight", 0.4))
        self._sentiment_weight = float(signals_cfg.get("sentiment_weight", 0.1))
        self._macro_weight = float(signals_cfg.get("macro_weight", 0.1))
        self._min_score = float(signals_cfg.get("min_score_threshold", 0.65))
        self._primary_tf = signals_cfg.get("primary_timeframe", "15m")

    async def _handle_candle(self, payload: Any) -> None:
        candle = payload
        if candle.timeframe != self._primary_tf:
            return

        key = f"{candle.exchange}:{candle.symbol}"
        now = time.time()
        if now - self._last_signal_time.get(key, 0) < self._min_signal_interval:
            return

        df = self.data_manager.get_dataframe(candle.exchange, candle.symbol, self._primary_tf)
        if df is None or len(df) < 30:
            return

        regime_state = self.data_manager.get_regime(candle.exchange, candle.symbol)

        tech_score = self._technical_scorer.score(df)
        ml_score = self._ml_scorer.score(df)

        composite = (
            self._tech_weight * tech_score
            + self._ml_weight * ml_score
            + self._sentiment_weight * self._sentiment_score
            + self._macro_weight * self._macro_score
        )

        abs_score = abs(composite)
        if abs_score < self._min_score:
            return

        last = df.iloc[-1]
        price = float(last.get("close", 0))
        atr = float(last.get("atr_14", price * 0.01))

        risk_cfg = self.config.get_value("risk") or {}
        sl_pct = float(risk_cfg.get("stop_loss_pct", 0.015))
        tp_pct = float(risk_cfg.get("take_profit_pct", 0.03))

        direction = "long" if composite > 0 else "short"
        if direction == "long":
            sl = price * (1 - sl_pct)
            tp = price * (1 + tp_pct)
        else:
            sl = price * (1 + sl_pct)
            tp = price * (1 - tp_pct)

        signal = TradingSignal(
            exchange=candle.exchange,
            symbol=candle.symbol,
            direction=direction,
            score=abs_score,
            technical_score=tech_score,
            ml_score=ml_score,
            sentiment_score=self._sentiment_score,
            macro_score=self._macro_score,
            regime=regime_state.regime.value if regime_state else MarketRegime.UNKNOWN.value,
            regime_confidence=regime_state.confidence if regime_state else 0.0,
            price=price,
            atr=atr,
            stop_loss=sl,
            take_profit=tp,
            timestamp=int(now),
            metadata={"timeframe": self._primary_tf},
        )

        self._last_signal_time[key] = now
        await self.event_bus.publish("SIGNAL", signal)
        logger.info(
            "Signal: {}/{} {} score={:.2f} price={:.2f} sl={:.2f} tp={:.2f}",
            signal.exchange, signal.symbol, signal.direction.upper(),
            signal.score, signal.price, signal.stop_loss, signal.take_profit,
        )

    async def _handle_sentiment(self, payload: Any) -> None:
        self._sentiment_score = float(payload.score) if hasattr(payload, "score") else 0.0

    async def _handle_funding(self, payload: Any) -> None:
        if not isinstance(payload, list) or not payload:
            return
        rates = [abs(r.rate) for r in payload]
        avg = sum(rates) / len(rates) if rates else 0.0
        self._macro_score = float(min(avg * 100, 1.0))

    async def run(self) -> None:
        self._running = True
        self.event_bus.subscribe("CANDLE", self._handle_candle)
        self.event_bus.subscribe("SENTIMENT", self._handle_sentiment)
        self.event_bus.subscribe("FUNDING_RATE", self._handle_funding)
        logger.info("SignalGenerator started (primary_tf={})", self._primary_tf)
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        self._running = False
        self.event_bus.unsubscribe("CANDLE", self._handle_candle)
        self.event_bus.unsubscribe("SENTIMENT", self._handle_sentiment)
        self.event_bus.unsubscribe("FUNDING_RATE", self._handle_funding)
