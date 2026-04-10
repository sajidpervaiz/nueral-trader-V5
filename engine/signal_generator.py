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
from engine.strategy_modules import StrategySelector, StrategySignal


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
    news_score: float
    orderbook_score: float
    regime: str
    regime_confidence: float
    price: float
    atr: float
    stop_loss: float
    take_profit: float
    timestamp: int
    metadata: dict[str, Any] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)

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

    @property
    def factor_count(self) -> int:
        """Number of signal factors that have a non-zero contribution."""
        count = 0
        if self.technical_score != 0.0:
            count += 1
        if self.ml_score != 0.0:
            count += 1
        if self.sentiment_score != 0.0:
            count += 1
        if self.macro_score != 0.0:
            count += 1
        if self.news_score != 0.0:
            count += 1
        if self.orderbook_score != 0.0:
            count += 1
        return count


class TechnicalScorer:
    def score(self, df: pd.DataFrame) -> tuple[float, list[str]]:
        """Returns (score, reasons) where reasons lists which indicators fired."""
        if df is None or len(df) < 5:
            return 0.0, []
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else last
        scores: list[float] = []
        reasons: list[str] = []

        # RSI
        rsi = last.get("rsi_14", 50.0)
        if rsi < 30:
            scores.append(1.0)
            reasons.append("RSI_oversold")
        elif rsi < 40:
            scores.append(0.5)
            reasons.append("RSI_low")
        elif rsi > 70:
            scores.append(-1.0)
            reasons.append("RSI_overbought")
        elif rsi > 60:
            scores.append(-0.5)
            reasons.append("RSI_high")
        else:
            scores.append(0.0)

        # MACD crossover (event-based: compare current bar vs previous)
        macd = last.get("macd", 0)
        macd_signal = last.get("macd_signal", 0)
        prev_macd = prev.get("macd", 0)
        prev_macd_signal = prev.get("macd_signal", 0)
        if prev_macd <= prev_macd_signal and macd > macd_signal:
            scores.append(1.0)
            reasons.append("MACD_bullish_cross")
        elif prev_macd >= prev_macd_signal and macd < macd_signal:
            scores.append(-1.0)
            reasons.append("MACD_bearish_cross")
        elif macd > macd_signal:
            scores.append(0.3)
        else:
            scores.append(-0.3)

        # MA crossover: EMA-fast vs EMA-slow (event detection)
        ema_fast = last.get("ema_12", last.get("close", 0))
        ema_slow = last.get("ema_26", last.get("close", 0))
        prev_ema_fast = prev.get("ema_12", prev.get("close", 0))
        prev_ema_slow = prev.get("ema_26", prev.get("close", 0))
        if prev_ema_fast <= prev_ema_slow and ema_fast > ema_slow:
            scores.append(1.0)
            reasons.append("EMA_golden_cross")
        elif prev_ema_fast >= prev_ema_slow and ema_fast < ema_slow:
            scores.append(-1.0)
            reasons.append("EMA_death_cross")
        elif ema_fast > ema_slow:
            scores.append(0.2)
        else:
            scores.append(-0.2)

        # Trend EMA
        trend = last.get("trend_ema", 0.0)
        scores.append(trend * 0.5)
        if abs(trend) > 0.3:
            reasons.append(f"trend_{'up' if trend > 0 else 'down'}")

        # Bollinger Band %B
        bb_pct = last.get("bb_pct", 0.5)
        if bb_pct < 0.2:
            scores.append(0.5)
            reasons.append("BB_oversold")
        elif bb_pct > 0.8:
            scores.append(-0.5)
            reasons.append("BB_overbought")
        else:
            scores.append(0.0)

        if not scores:
            return 0.0, reasons
        return float(np.clip(np.mean(scores), -1.0, 1.0)), reasons


class MLScorer:

    def __init__(self, model_path: str = "ml_model.pkl") -> None:
        self._model = None
        self._feature_names = None
        self._model_loaded = False
        self._model_path = model_path
        self._load_model()

    def _load_model(self):
        import os
        import hashlib
        import hmac
        try:
            if os.path.exists(self._model_path):
                # P0: Verify model file integrity before loading to prevent
                # arbitrary code execution via pickle deserialization.
                expected_digest = os.getenv("ML_MODEL_SHA256", "").strip()
                if expected_digest:
                    with open(self._model_path, "rb") as f:
                        file_hash = hashlib.sha256(f.read()).hexdigest()
                    if not hmac.compare_digest(file_hash, expected_digest):
                        logger.critical(
                            "Model file integrity check FAILED — refusing to load {}",
                            self._model_path,
                        )
                        return
                    logger.info("Model file SHA-256 verified")
                else:
                    logger.warning(
                        "ML_MODEL_SHA256 env var not set — model integrity not verified. "
                        "Set ML_MODEL_SHA256=<hex> in production.",
                    )
                import joblib
                model_data = joblib.load(self._model_path)
                self._model = model_data.get("model")
                self._feature_names = model_data.get("feature_names")
                self._model_loaded = True
                logger.info(f"MLScorer loaded model from {self._model_path}")
            else:
                logger.warning(f"MLScorer: model file {self._model_path} not found, using heuristics")
        except Exception as e:
            logger.error(f"MLScorer: failed to load model: {e}")
            self._model = None
            self._feature_names = None
            self._model_loaded = False

    def score(self, df: pd.DataFrame) -> float:
        if df is None or len(df) < 20:
            return 0.0
        if self._model is not None and self._feature_names is not None:
            try:
                last = df.iloc[-1]
                X = last[self._feature_names].values.reshape(1, -1)
                if hasattr(self._model, "predict_proba"):
                    proba = self._model.predict_proba(X)
                    # Use probability of class 1 (up)
                    score = float(proba[0, 1]) * 2 - 1  # scale to [-1, 1]
                    return float(np.clip(score, -1.0, 1.0))
                elif hasattr(self._model, "predict"):
                    pred = self._model.predict(X)
                    return float(np.clip(pred[0], -1.0, 1.0))
            except Exception as e:
                logger.error(f"MLScorer: model prediction failed: {e}")
        # Fallback to heuristic
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


class NewsScorer:
    """Converts news sentiment events into a directional score in [-1, 1].

    Maintains a decay-weighted rolling average so recent headlines
    dominate while stale news fades.
    """

    def __init__(self, decay_seconds: float = 3600.0) -> None:
        self._decay_seconds = decay_seconds
        self._events: list[tuple[float, float]] = []  # (timestamp, sentiment)

    def ingest(self, sentiment: float, ts: float | None = None) -> None:
        now = ts or time.time()
        self._events.append((now, float(np.clip(sentiment, -1.0, 1.0))))
        # prune events older than 4× decay window
        cutoff = now - self._decay_seconds * 4
        self._events = [(t, s) for t, s in self._events if t >= cutoff]

    def score(self) -> float:
        if not self._events:
            return 0.0
        now = time.time()
        weighted_sum = 0.0
        weight_sum = 0.0
        for ts, sent in self._events:
            age = max(0.0, now - ts)
            w = np.exp(-age / self._decay_seconds)
            weighted_sum += w * sent
            weight_sum += w
        if weight_sum == 0:
            return 0.0
        return float(np.clip(weighted_sum / weight_sum, -1.0, 1.0))


class OrderbookScorer:
    """Computes a directional score from orderbook bid/ask imbalance.

    Positive = buy-side dominance (bullish), negative = sell pressure.
    """

    def __init__(self) -> None:
        self._last_imbalance: float = 0.0

    def update(self, bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> float:
        """Accept orderbook levels as [(price, qty), …] for bids and asks."""
        bid_volume = sum(qty for _, qty in bids) if bids else 0.0
        ask_volume = sum(qty for _, qty in asks) if asks else 0.0
        total = bid_volume + ask_volume
        if total == 0:
            self._last_imbalance = 0.0
        else:
            # Imbalance ratio: +1 = 100% bids, -1 = 100% asks, 0 = balanced
            self._last_imbalance = float(np.clip((bid_volume - ask_volume) / total, -1.0, 1.0))
        return self._last_imbalance

    def score(self) -> float:
        return self._last_imbalance


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
        self._news_scorer = NewsScorer()
        self._orderbook_scorer = OrderbookScorer()
        self._strategy_selector = StrategySelector()
        self._running = False
        self._sentiment_score: float = 0.0
        self._macro_score: float = 0.0
        self._last_signal_time: dict[str, float] = {}
        self._min_signal_interval = 60.0
        self._auto_trading_enabled = False
        self._bot_config: dict = {
            "strategy": "ensemble",
            "sizing_mode": "risk_pct",
            "risk_per_trade": 2.0,
            "max_positions": 3,
            "max_drawdown": 10.0,
            "max_leverage": 5,
            "daily_loss_limit": 500.0,
            "trailing_mode": "none",
            "auto_sl_tp": False,
        }

        signals_cfg = config.get_value("signals") or {}
        self._ml_weight = float(signals_cfg.get("ml_weight", 0.25))
        self._tech_weight = float(signals_cfg.get("technical_weight", 0.30))
        self._sentiment_weight = float(signals_cfg.get("sentiment_weight", 0.10))
        self._macro_weight = float(signals_cfg.get("macro_weight", 0.10))
        self._news_weight = float(signals_cfg.get("news_weight", 0.15))
        self._orderbook_weight = float(signals_cfg.get("orderbook_weight", 0.10))
        self._min_score = float(signals_cfg.get("min_score_threshold", 0.65))
        self._min_factors = int(signals_cfg.get("min_contributing_factors", 3))
        self._primary_tf = signals_cfg.get("primary_timeframe", "15m")
        self._confirmation_tfs: list[str] = list(signals_cfg.get("confirmation_timeframes", ["1h", "4h"]))

    def set_auto_trading(self, enabled: bool) -> None:
        prev = self._auto_trading_enabled
        self._auto_trading_enabled = enabled
        if prev != enabled:
            logger.info("Auto-trading toggled: {} → {}", prev, enabled)

    @property
    def auto_trading_enabled(self) -> bool:
        return self._auto_trading_enabled

    def _check_higher_timeframe_trend(
        self, exchange: str, symbol: str, direction: str,
    ) -> tuple[bool, str]:
        """Multi-timeframe confirmation: higher TF trend must agree with direction.

        Returns (ok, reason).  If no HTF data is available, passes by default.
        """
        if not self._confirmation_tfs:
            return True, ""

        checked_any = False
        for tf in self._confirmation_tfs:
            htf_df = self.data_manager.get_dataframe(exchange, symbol, tf)
            if htf_df is None or len(htf_df) < 20:
                continue  # no data — skip this TF rather than block

            checked_any = True

            last = htf_df.iloc[-1]
            ema_fast = last.get("ema_12", last.get("close", 0))
            ema_slow = last.get("ema_26", last.get("close", 0))

            if ema_fast == 0 or ema_slow == 0:
                continue

            htf_bullish = ema_fast > ema_slow
            htf_bearish = ema_fast < ema_slow

            if direction == "long" and htf_bearish:
                return False, f"{tf}_trend_bearish (EMA12={ema_fast:.2f} < EMA26={ema_slow:.2f})"
            if direction == "short" and htf_bullish:
                return False, f"{tf}_trend_bullish (EMA12={ema_fast:.2f} > EMA26={ema_slow:.2f})"

        if not checked_any:
            logger.warning(
                "HTF confirmation bypassed for {}:{} — no data for any of {}",
                exchange, symbol, self._confirmation_tfs,
            )

        return True, ""

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

        # ── ARMS-V2.1: Regime-gated strategy module evaluation ────────────
        strategy_signal: StrategySignal | None = None
        if regime_state and regime_state.regime != MarketRegime.UNKNOWN:
            strategy_signal = self._strategy_selector.select_and_evaluate(df, regime_state)

        # ── 6-factor composite scoring ────────────────────────────────────
        tech_score, tech_reasons = self._technical_scorer.score(df)
        ml_score = self._ml_scorer.score(df)
        news_score = self._news_scorer.score()
        ob_score = self._orderbook_scorer.score()

        composite = (
            self._tech_weight * tech_score
            + self._ml_weight * ml_score
            + self._sentiment_weight * self._sentiment_score
            + self._macro_weight * self._macro_score
            + self._news_weight * news_score
            + self._orderbook_weight * ob_score
        )

        # If strategy module fired, blend its direction and boost score
        if strategy_signal is not None:
            strat_direction = 1.0 if strategy_signal.is_long else -1.0
            composite_direction = 1.0 if composite > 0 else -1.0
            # Strategy agreement boosts composite; disagreement attenuates
            if strat_direction == composite_direction:
                composite = composite * 1.0 + strat_direction * strategy_signal.score * 0.3
            else:
                # Strategy module overrides if its score is high enough
                if strategy_signal.score > 0.6:
                    composite = strat_direction * strategy_signal.score * 0.8
                else:
                    composite *= 0.5  # attenuate conflicting signal

        abs_score = abs(composite)
        if abs_score < self._min_score:
            return

        # ── Multi-timeframe confirmation ──────────────────────────────────
        # Higher TF trend must agree with signal direction
        proposed_direction = "long" if composite > 0 else "short"
        htf_ok, htf_reason = self._check_higher_timeframe_trend(
            candle.exchange, candle.symbol, proposed_direction,
        )
        if not htf_ok:
            logger.debug(
                "Signal rejected for {} on HTF filter: {}",
                key, htf_reason,
            )
            return

        # ── Require minimum factor agreement ──────────────────────────────
        # Build preliminary signal to count contributing factors
        tmp_scores = {
            "technical": tech_score,
            "ml": ml_score,
            "sentiment": self._sentiment_score,
            "macro": self._macro_score,
            "news": news_score,
            "orderbook": ob_score,
        }
        active_factors = sum(1 for v in tmp_scores.values() if v != 0.0)
        if active_factors < self._min_factors:
            logger.debug(
                "Signal rejected for {}: only {}/{} factors active (scores={})",
                key, active_factors, self._min_factors,
                {k: f"{v:.3f}" for k, v in tmp_scores.items()},
            )
            return

        # ── Auto-trading gate ─────────────────────────────────────────────
        if not self._auto_trading_enabled:
            logger.debug(
                "Signal suppressed for {} (auto_trading=off) — composite={:.3f} factors={}",
                key, composite, active_factors,
            )
            return

        # ── Bot config gate: max positions check ─────────────────────────
        max_pos = int(self._bot_config.get("max_positions", 3))
        # Count approximate open signals by checking recent signal times
        open_signal_count = sum(
            1 for t in self._last_signal_time.values()
            if now - t < 300  # signals within last 5 minutes considered "active"
        )
        if open_signal_count >= max_pos:
            logger.debug(
                "Signal suppressed for {}: max_positions={} reached (active={})",
                key, max_pos, open_signal_count,
            )
            return

        last = df.iloc[-1]
        price = float(last.get("close", 0))
        atr = float(last.get("atr_14", price * 0.01))

        risk_cfg = self.config.get_value("risk") or {}
        sl_pct = float(risk_cfg.get("stop_loss_pct", 0.015))
        tp_pct = float(risk_cfg.get("take_profit_pct", 0.03))
        # Override with bot config if auto_sl_tp is enabled
        if self._bot_config.get("auto_sl_tp"):
            bot_risk = float(self._bot_config.get("risk_per_trade", 2)) / 100.0
            sl_pct = min(sl_pct, bot_risk) if bot_risk > 0 else sl_pct

        direction = "long" if composite > 0 else "short"
        if direction == "long":
            sl = price * (1 - sl_pct)
            tp = price * (1 + tp_pct)
        else:
            sl = price * (1 + sl_pct)
            tp = price * (1 - tp_pct)

        # ── Build reasons list ─────────────────────────────────────────────
        reasons: list[str] = list(tech_reasons)
        if strategy_signal is not None:
            reasons.extend(strategy_signal.reasons)
            reasons.append(f"strategy:{strategy_signal.strategy}")
        if abs(ml_score) > 0.3:
            reasons.append(f"ML_{'bullish' if ml_score > 0 else 'bearish'}")
        if abs(self._sentiment_score) > 0.3:
            reasons.append(f"sentiment_{'bullish' if self._sentiment_score > 0 else 'bearish'}")
        if abs(self._macro_score) > 0.3:
            reasons.append(f"macro_{'bullish' if self._macro_score > 0 else 'bearish'}")
        if abs(news_score) > 0.3:
            reasons.append(f"news_{'bullish' if news_score > 0 else 'bearish'}")
        if abs(ob_score) > 0.3:
            reasons.append(f"orderbook_{'bullish' if ob_score > 0 else 'bearish'}")

        signal = TradingSignal(
            exchange=candle.exchange,
            symbol=candle.symbol,
            direction=direction,
            score=abs_score,
            technical_score=tech_score,
            ml_score=ml_score,
            sentiment_score=self._sentiment_score,
            macro_score=self._macro_score,
            news_score=news_score,
            orderbook_score=ob_score,
            regime=regime_state.regime.value if regime_state else MarketRegime.UNKNOWN.value,
            regime_confidence=regime_state.confidence if regime_state else 0.0,
            price=price,
            atr=atr,
            stop_loss=sl,
            take_profit=tp,
            timestamp=int(now),
            reasons=reasons,
            metadata={
                "timeframe": self._primary_tf,
                "factors_active": active_factors,
                "strategy": strategy_signal.strategy if strategy_signal else "composite",
                "strategy_score": strategy_signal.score if strategy_signal else 0.0,
                "weights": {
                    "technical": self._tech_weight,
                    "ml": self._ml_weight,
                    "sentiment": self._sentiment_weight,
                    "macro": self._macro_weight,
                    "news": self._news_weight,
                    "orderbook": self._orderbook_weight,
                },
            },
        )

        self._last_signal_time[key] = now
        await self.event_bus.publish("SIGNAL", signal)
        logger.info(
            "Signal: {}/{} {} score={:.2f} factors={} price={:.2f} sl={:.2f} tp={:.2f} "
            "[tech={:.2f} ml={:.2f} sent={:.2f} macro={:.2f} news={:.2f} ob={:.2f}]",
            signal.exchange, signal.symbol, signal.direction.upper(),
            signal.score, active_factors, signal.price, signal.stop_loss, signal.take_profit,
            tech_score, ml_score, self._sentiment_score, self._macro_score,
            news_score, ob_score,
        )

    async def _handle_sentiment(self, payload: Any) -> None:
        self._sentiment_score = float(payload.score) if hasattr(payload, "score") else 0.0

    async def _handle_funding(self, payload: Any) -> None:
        if not isinstance(payload, list) or not payload:
            return
        rates = [abs(r.rate) for r in payload]
        avg = sum(rates) / len(rates) if rates else 0.0
        self._macro_score = float(min(avg * 100, 1.0))

    async def _handle_news_sentiment(self, payload: Any) -> None:
        """Consume NEWS_SENTIMENT events — {sentiment: float, timestamp: int}."""
        if isinstance(payload, dict):
            sent = float(payload.get("sentiment", 0.0))
            ts = float(payload.get("timestamp", time.time()))
        elif hasattr(payload, "sentiment"):
            sent = float(payload.sentiment)
            ts = float(getattr(payload, "timestamp", time.time()))
        else:
            return
        self._news_scorer.ingest(sent, ts)

    async def _handle_orderbook_update(self, payload: Any) -> None:
        """Consume ORDERBOOK_UPDATE events — {bids: [(p,q),...], asks: [(p,q),...]}."""
        if isinstance(payload, dict):
            bids = [(float(b[0]), float(b[1])) for b in payload.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in payload.get("asks", [])]
        elif hasattr(payload, "bids") and hasattr(payload, "asks"):
            bids = [(float(b[0]), float(b[1])) for b in payload.bids]
            asks = [(float(a[0]), float(a[1])) for a in payload.asks]
        else:
            return
        self._orderbook_scorer.update(bids, asks)

    async def run(self) -> None:
        self._running = True
        self.event_bus.subscribe("CANDLE", self._handle_candle)
        self.event_bus.subscribe("SENTIMENT", self._handle_sentiment)
        self.event_bus.subscribe("FUNDING_RATE", self._handle_funding)
        self.event_bus.subscribe("NEWS_SENTIMENT", self._handle_news_sentiment)
        self.event_bus.subscribe("ORDERBOOK_UPDATE", self._handle_orderbook_update)
        logger.info(
            "SignalGenerator started (primary_tf={}, auto_trading={}, "
            "weights: tech={:.0%} ml={:.0%} sent={:.0%} macro={:.0%} news={:.0%} ob={:.0%}, "
            "min_factors={})",
            self._primary_tf, self._auto_trading_enabled,
            self._tech_weight, self._ml_weight, self._sentiment_weight,
            self._macro_weight, self._news_weight, self._orderbook_weight,
            self._min_factors,
        )
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        self._running = False
        self.event_bus.unsubscribe("CANDLE", self._handle_candle)
        self.event_bus.unsubscribe("SENTIMENT", self._handle_sentiment)
        self.event_bus.unsubscribe("FUNDING_RATE", self._handle_funding)
        self.event_bus.unsubscribe("NEWS_SENTIMENT", self._handle_news_sentiment)
        self.event_bus.unsubscribe("ORDERBOOK_UPDATE", self._handle_orderbook_update)
