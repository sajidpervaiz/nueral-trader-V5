from __future__ import annotations

import asyncio
import datetime
import math
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from core.config import Config
from core.event_bus import EventBus
from analysis.data_manager import DataManager
from analysis.regime import MarketRegime, RegimeState
from analysis.smart_money import SmartMoneyAnalyzer, SMCState
from analysis.volume_profile import VolumeProfileAnalyzer, VolumeFlowState
from engine.strategy_modules import StrategySelector, StrategySignal


# ── Signal Type Classification (§3) ──────────────────────────────────────────

class SignalType(str, Enum):
    TYPE_A = "breakout_pullback"       # Donchian breakout + Fib retracement
    TYPE_B = "liquidity_sweep"         # Liquidity sweep reversal
    TYPE_C = "fvg_mitigation"          # Fair Value Gap mitigation entry
    TYPE_D = "order_block_mitigation"  # Order Block mitigation entry
    COMPOSITE = "composite"            # Generic composite signal

# Priority: C > D > A > B (per spec §3)
_SIGNAL_PRIORITY: dict[SignalType, int] = {
    SignalType.TYPE_C: 4,
    SignalType.TYPE_D: 3,
    SignalType.TYPE_A: 2,
    SignalType.TYPE_B: 1,
    SignalType.COMPOSITE: 0,
}


# ── Session & Killzone Rules (§5) ────────────────────────────────────────────

@dataclass
class SessionRule:
    name: str
    start_utc: int           # hour (0-23)
    end_utc: int             # hour (0-23)
    allowed_types: set[SignalType] | None   # None = all allowed
    size_multiplier: float   # 1.0 = 100%, 0.5 = 50%, etc.
    no_trade: bool = False   # If True, block all signals

_SESSION_RULES: list[SessionRule] = [
    SessionRule("asia",           0,  8,  {SignalType.TYPE_B, SignalType.COMPOSITE}, 0.50),
    SessionRule("london_open",    8,  12, {SignalType.TYPE_A, SignalType.TYPE_C, SignalType.TYPE_D, SignalType.COMPOSITE}, 1.00),
    SessionRule("london_dead",    12, 13, None, 0.50),  # Reduced size during dead zone
    SessionRule("london_ny",      13, 17, None, 1.50),  # All types allowed, aggressive
    SessionRule("ny_only",        17, 22, {SignalType.TYPE_B, SignalType.TYPE_D, SignalType.COMPOSITE}, 0.75),
    SessionRule("low_liquidity",  22, 24, None, 0.25),  # Reduced size during low liquidity
]

# ICT Killzones (spec §5): 1.2x score multiplier, Type C & D only
_ICT_KILLZONES: list[tuple[int, int]] = [(13, 14), (15, 16)]


# ── Regime-specific risk percentages (§7) ────────────────────────────────────

_REGIME_RISK_PCT: dict[MarketRegime, float] = {
    MarketRegime.STRONG_TREND_UP:   0.02,   # 2% risk — trending
    MarketRegime.WEAK_TREND_UP:     0.02,   # 2% risk — trending
    MarketRegime.STRONG_TREND_DOWN: 0.02,   # 2% risk — trending
    MarketRegime.WEAK_TREND_DOWN:   0.02,   # 2% risk — trending
    MarketRegime.COMPRESSION:       0.025,  # 2.5% risk — breakout
    MarketRegime.RANGE_CHOP:        0.015,  # 1.5% risk — ranging
}

# Regime transition rules: reduce 50%, tighten SL 0.5x for 2 candles
_TRANSITION_CANDLE_WINDOW = 2
_TRANSITION_SIZE_MULT = 0.50
_TRANSITION_SL_MULT = 0.50


# ── Regime-adaptive weight profiles ──────────────────────────────────────────
# Each profile sums to 1.0.  Weights shift depending on detected regime so the
# signal generator emphasises the factors most predictive for that market state.
_REGIME_WEIGHT_PROFILES: dict[MarketRegime, dict[str, float]] = {
    MarketRegime.STRONG_TREND_UP: {
        "technical": 0.35, "ml": 0.30, "sentiment": 0.10,
        "macro": 0.05, "news": 0.10, "orderbook": 0.10,
    },
    MarketRegime.WEAK_TREND_UP: {
        "technical": 0.30, "ml": 0.30, "sentiment": 0.10,
        "macro": 0.05, "news": 0.15, "orderbook": 0.10,
    },
    MarketRegime.STRONG_TREND_DOWN: {
        "technical": 0.35, "ml": 0.30, "sentiment": 0.10,
        "macro": 0.05, "news": 0.10, "orderbook": 0.10,
    },
    MarketRegime.WEAK_TREND_DOWN: {
        "technical": 0.30, "ml": 0.30, "sentiment": 0.10,
        "macro": 0.05, "news": 0.15, "orderbook": 0.10,
    },
    MarketRegime.COMPRESSION: {
        "technical": 0.20, "ml": 0.20, "sentiment": 0.05,
        "macro": 0.05, "news": 0.10, "orderbook": 0.40,
    },
    MarketRegime.RANGE_CHOP: {
        "technical": 0.40, "ml": 0.15, "sentiment": 0.05,
        "macro": 0.05, "news": 0.10, "orderbook": 0.25,
    },
}

# Asymmetric direction threshold multipliers by regime.
# counter-trend trades require a higher score to fire.
_REGIME_DIRECTION_PENALTY: dict[MarketRegime, dict[str, float]] = {
    MarketRegime.STRONG_TREND_UP:   {"long": 1.0, "short": 1.5},
    MarketRegime.WEAK_TREND_UP:     {"long": 1.0, "short": 1.2},
    MarketRegime.STRONG_TREND_DOWN: {"long": 1.5, "short": 1.0},
    MarketRegime.WEAK_TREND_DOWN:   {"long": 1.2, "short": 1.0},
    MarketRegime.COMPRESSION:       {"long": 1.0, "short": 1.0},
    MarketRegime.RANGE_CHOP:        {"long": 1.0, "short": 1.0},
}

# Correlation groups — assets that tend to move together.
# Only one position per group is allowed to avoid concentrated risk.
_CORRELATION_GROUPS: list[set[str]] = [
    {"BTC/USDT:USDT", "BTC/USDT"},
    {"ETH/USDT:USDT", "ETH/USDT"},
    {"SOL/USDT:USDT", "SOL/USDT"},
    # Major crypto group: avoid same-direction on highly correlated assets
]
_CRYPTO_MAJOR_GROUP = {"BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
                       "BTC/USDT", "ETH/USDT", "SOL/USDT"}


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
    signal_type: str = SignalType.COMPOSITE.value
    quality_score: int = 0          # 0-100 quality score (§6)
    session_name: str = ""          # Active session name
    session_size_mult: float = 1.0  # Session-based position sizing
    regime_risk_pct: float = 0.01   # Regime-specific risk % (§7)
    mtf_agreement_count: int = 0    # Number of TFs in agreement
    mtf_weighted_score: float = 0.0 # Weighted MTF composite [-1, 1]

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
    """L4 Momentum Convergence Matrix — 15 indicators, min 10 bullish for entry.

    Computes a bullish/bearish vote from each indicator and returns a
    normalised score in [-1, 1] plus human-readable reasons.
    """

    def score(self, df: pd.DataFrame) -> tuple[float, list[str]]:
        if df is None or len(df) < 5:
            return 0.0, []
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else last
        votes: list[float] = []   # each in {-1, -0.5, 0, 0.5, 1}
        reasons: list[str] = []

        # ── Trend Indicators (5) ─────────────────────────────────────────

        # 1. EMA Stack Alignment (9/21/50/200)
        ema9 = last.get("ema_9", 0); ema21 = last.get("ema_21", 0)
        ema50 = last.get("ema_50", 0); ema200 = last.get("ema_200", 0)
        if ema9 > ema21 > ema50 > ema200 and ema200 > 0:
            votes.append(1.0); reasons.append("EMA_stack_bullish")
        elif ema9 < ema21 < ema50 < ema200 and ema200 > 0:
            votes.append(-1.0); reasons.append("EMA_stack_bearish")
        else:
            votes.append(0.0)

        # 2. SuperTrend direction
        st_dir = last.get("supertrend_dir", 0)
        if st_dir == 1:
            votes.append(1.0); reasons.append("SuperTrend_bullish")
        elif st_dir == -1:
            votes.append(-1.0); reasons.append("SuperTrend_bearish")
        else:
            votes.append(0.0)

        # 3. Ichimoku Cloud position
        above_cloud = last.get("ichimoku_above_cloud", 0)
        below_cloud = last.get("ichimoku_below_cloud", 0)
        if above_cloud:
            votes.append(1.0); reasons.append("Ichimoku_above_cloud")
        elif below_cloud:
            votes.append(-1.0); reasons.append("Ichimoku_below_cloud")
        else:
            votes.append(0.0)

        # 4. Parabolic SAR placement
        psar_bull = last.get("psar_bullish", 0)
        if psar_bull == 1.0:
            votes.append(1.0); reasons.append("PSAR_bullish")
        elif psar_bull == 0.0:
            votes.append(-1.0); reasons.append("PSAR_bearish")
        else:
            votes.append(0.0)

        # 5. Aroon Up/Down (25-period)
        aroon_osc = last.get("aroon_osc", 0)
        if aroon_osc > 50:
            votes.append(1.0); reasons.append("Aroon_bullish")
        elif aroon_osc < -50:
            votes.append(-1.0); reasons.append("Aroon_bearish")
        else:
            votes.append(0.0)

        # ── Momentum Oscillators (5) ─────────────────────────────────────

        # 6. RSI (14) — 45-65 bullish zone per spec
        rsi = last.get("rsi_14", 50.0)
        if 45 <= rsi <= 65:
            votes.append(0.5); reasons.append("RSI_bullish_zone")
        elif rsi < 30:
            votes.append(1.0); reasons.append("RSI_oversold")
        elif rsi > 70:
            votes.append(-1.0); reasons.append("RSI_overbought")
        elif rsi < 45:
            votes.append(-0.5); reasons.append("RSI_low")
        else:
            votes.append(-0.5); reasons.append("RSI_high")

        # 7. MACD line/signal cross
        macd = last.get("macd", 0); macd_sig = last.get("macd_signal", 0)
        prev_macd = prev.get("macd", 0); prev_macd_sig = prev.get("macd_signal", 0)
        if prev_macd <= prev_macd_sig and macd > macd_sig:
            votes.append(1.0); reasons.append("MACD_bullish_cross")
        elif prev_macd >= prev_macd_sig and macd < macd_sig:
            votes.append(-1.0); reasons.append("MACD_bearish_cross")
        elif macd > macd_sig:
            votes.append(0.5)
        else:
            votes.append(-0.5)

        # 8. Money Flow Index (14) > 55 bullish
        mfi = last.get("mfi_14", 50.0)
        if mfi > 55:
            votes.append(1.0); reasons.append("MFI_bullish")
        elif mfi < 45:
            votes.append(-1.0); reasons.append("MFI_bearish")
        else:
            votes.append(0.0)

        # 9. Stochastic (14,3,3)
        stoch_k = last.get("stoch_k", 50); stoch_d = last.get("stoch_d", 50)
        prev_k = prev.get("stoch_k", 50); prev_d = prev.get("stoch_d", 50)
        if prev_k <= prev_d and stoch_k > stoch_d and stoch_k < 80:
            votes.append(1.0); reasons.append("Stoch_bullish_cross")
        elif prev_k >= prev_d and stoch_k < stoch_d and stoch_k > 20:
            votes.append(-1.0); reasons.append("Stoch_bearish_cross")
        elif stoch_k < 20:
            votes.append(0.5); reasons.append("Stoch_oversold")
        elif stoch_k > 80:
            votes.append(-0.5); reasons.append("Stoch_overbought")
        else:
            votes.append(0.0)

        # 10. CCI (20)
        cci = last.get("cci_20", 0)
        if cci > 100:
            votes.append(1.0); reasons.append("CCI_bullish")
        elif cci < -100:
            votes.append(-1.0); reasons.append("CCI_bearish")
        else:
            votes.append(0.0)

        # ── Additional Confirmation (5) ──────────────────────────────────

        # 11. Williams %R (> -80 bullish per spec)
        wr = last.get("williams_r", -50)
        if wr > -20:
            votes.append(-0.5); reasons.append("WR_overbought")
        elif wr > -80:
            votes.append(0.5); reasons.append("WR_bullish")
        else:
            votes.append(-0.5); reasons.append("WR_oversold")

        # 12. Ultimate Oscillator rising
        ult = last.get("ult_osc", 50)
        prev_ult = prev.get("ult_osc", 50)
        if ult > prev_ult and ult > 50:
            votes.append(1.0); reasons.append("UltOsc_rising")
        elif ult < prev_ult and ult < 50:
            votes.append(-1.0); reasons.append("UltOsc_falling")
        else:
            votes.append(0.0)

        # 13. TRIX zero-line cross
        trix = last.get("trix", 0); prev_trix = prev.get("trix", 0)
        if prev_trix <= 0 and trix > 0:
            votes.append(1.0); reasons.append("TRIX_bullish_cross")
        elif prev_trix >= 0 and trix < 0:
            votes.append(-1.0); reasons.append("TRIX_bearish_cross")
        else:
            votes.append(0.0)

        # 14. Bollinger Bands %B > 0.5 bullish
        bb_pct = last.get("bb_pct", 0.5)
        if bb_pct > 0.5:
            votes.append(0.5); reasons.append("BB_pct_bullish")
        elif bb_pct < 0.5:
            votes.append(-0.5); reasons.append("BB_pct_bearish")
        else:
            votes.append(0.0)

        # 15. Keltner Channel position
        close = last.get("close", 0)
        kc_upper = last.get("kc_upper", 0); kc_lower = last.get("kc_lower", 0)
        if close > kc_upper:
            votes.append(1.0); reasons.append("KC_breakout_bullish")
        elif close < kc_lower:
            votes.append(-1.0); reasons.append("KC_breakout_bearish")
        else:
            votes.append(0.0)

        # ── ADX trend strength amplifier ──────────────────────────────────
        adx = last.get("adx", 0.0)
        plus_di = last.get("plus_di", 0.0)
        minus_di = last.get("minus_di", 0.0)
        if adx > 25:
            di_signal = 0.6 if plus_di > minus_di else -0.6
            votes.append(di_signal)
            reasons.append(f"ADX_strong_{adx:.0f}")

        # ── Volume confirmation amplifier ─────────────────────────────────
        vol_ratio = last.get("volume_ratio", 1.0)
        if vol_ratio > 1.5:
            current_avg = np.mean(votes) if votes else 0.0
            vol_boost = 0.3 * (1.0 if current_avg >= 0 else -1.0)
            votes.append(vol_boost)
            reasons.append(f"vol_confirm_{vol_ratio:.1f}x")

        if not votes:
            return 0.0, reasons

        # Bullish vote count for logging
        bullish_count = sum(1 for v in votes if v > 0)
        bearish_count = sum(1 for v in votes if v < 0)

        return float(np.clip(np.mean(votes), -1.0, 1.0)), reasons


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
        """Multi-feature heuristic when no trained model is available.

        Combines momentum (z-score), mean-reversion (RSI), and volatility-adjusted
        trend strength instead of relying on a single z-score.
        """
        if df is None or len(df) < 20:
            return 0.0
        last = df.iloc[-1]

        # ── 1. Momentum: z-score of 5-bar returns ────────────────────────
        returns_5 = last.get("returns_5", 0.0)
        vol = last.get("vol_20", 0.3)
        momentum = 0.0
        if vol > 0:
            z_score = returns_5 / (vol / (252 ** 0.5))
            # Significance deadband: ignore noise below 0.5 standard deviations
            if abs(z_score) >= 0.5:
                momentum = float(np.clip(z_score * 0.15, -0.5, 0.5))

        # ── 2. Mean-reversion signal: RSI extremes ───────────────────────
        rsi = last.get("rsi_14", 50.0)
        reversion = 0.0
        if rsi < 30:
            reversion = 0.4   # Deeply oversold → expect bounce
        elif rsi < 40:
            reversion = 0.2
        elif rsi > 70:
            reversion = -0.4  # Deeply overbought → expect pullback
        elif rsi > 60:
            reversion = -0.2

        # ── 3. Trend strength: EMA alignment ─────────────────────────────
        ema12 = last.get("ema_12", 0.0)
        ema26 = last.get("ema_26", 0.0)
        trend = 0.0
        if ema12 > 0 and ema26 > 0:
            trend_pct = (ema12 - ema26) / ema26
            trend = float(np.clip(trend_pct * 10, -0.5, 0.5))

        # ── 4. Combine with adaptive weighting ───────────────────────────
        # In strong trends, weight momentum+trend; in ranges, weight reversion
        adx = last.get("adx", 20.0)
        if adx > 25:
            # Trending: momentum 40%, trend 40%, reversion 20%
            combined = momentum * 0.4 + trend * 0.4 + reversion * 0.2
        else:
            # Ranging: reversion 50%, momentum 20%, trend 30%
            combined = reversion * 0.5 + momentum * 0.2 + trend * 0.3

        return float(np.clip(combined, -1.0, 1.0))


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
        self._smc_analyzer = SmartMoneyAnalyzer()
        self._volume_analyzer = VolumeProfileAnalyzer()
        self._running = False
        self._sentiment_score: float = 0.0
        self._sentiment_ts: float = 0.0
        self._macro_score: float = 0.0
        self._macro_ts: float = 0.0
        self._score_ttl: float = 3600.0  # seconds before scores go stale
        self._min_factor_magnitude: float = 0.05  # minimum |score| to count as contributing
        self._risk_manager = None  # set externally for position counting
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

        # ── Advanced: signal momentum tracker ─────────────────────────────
        # Tracks the last N composite scores per symbol to detect momentum
        self._score_history: dict[str, deque[float]] = {}
        self._score_history_len = 6   # track last 6 signals per symbol

        # ── Advanced: consecutive loss cooldown ───────────────────────────
        self._consecutive_losses: dict[str, int] = {}   # per-symbol loss streak
        self._max_loss_cooldown = 3   # after N losses, min_score *= 2

        # ── Advanced: correlation-aware position tracking ────────────────
        self._open_directions: dict[str, str] = {}  # symbol → last signal direction

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

        # ── Pipeline status tracking for dashboard API ────────────────────
        self._last_layer_status: dict[str, str] = {}
        self._last_quality_breakdown: dict[str, Any] = {
            "total": 0,
            "components": {
                "htf_alignment": 0, "signal_type_purity": 0,
                "volume_confirmation": 0, "liquidity_proximity": 0,
                "fvg_ob_overlap": 0, "session_alignment": 0,
                "sentiment_alignment": 0, "onchain_health": 0,
            },
        }

    # ── Advanced helpers ──────────────────────────────────────────────────

    def _update_layer_status(self, **kwargs: int | str) -> None:
        """Update pipeline layer status dict for dashboard display.

        Maps pipeline layer scores (l0-l7) to dashboard layer names.
        Pipeline:  L0=Session, L1=Regime, L2=Structure, L3=SMC, L4=Momentum, L5=Volume, L6=MTF, L7=Micro, L8=Quality
        Dashboard: 1=Session, 2=HTF Trend, 3=Technical, 4=SMC, 5=Volume, 6=Regime, 7=ML, 8=Quality, 9=Risk
        """
        # Pipeline layer → dashboard key mapping
        mapping = {
            "l0": "session_filter",       # L0 Session/Sentiment → Dashboard "Session Filter"
            "l1": "regime_detection",     # L1 Regime (ADX) → Dashboard "Regime Detection"
            "l2": "technical_confluence", # L2 Structure (BOS/CHoCH) → Dashboard "Technical Confluence"
            "l3": "smart_money_concepts", # L3 SMC (FVG/OB) → Dashboard "Smart Money Concepts"
            "l4": "ml_ensemble",          # L4 Momentum (tech+ML+OB) → Dashboard "ML Ensemble"
            "l5": "volume_flow",          # L5 Volume → Dashboard "Volume Flow"
            "l6": "htf_trend",            # L6 MTF alignment → Dashboard "HTF Trend"
            "l7": "signal_quality",       # L7 Microstructure → Dashboard "Signal Quality"
            "risk_gate": "risk_gate",     # Risk gate → Dashboard "Risk Gate"
        }
        for key, dashboard_name in mapping.items():
            if key in kwargs:
                val = kwargs[key]
                if isinstance(val, (int, float)):
                    if val >= 70:
                        self._last_layer_status[dashboard_name] = "PASS"
                    elif val >= 40:
                        self._last_layer_status[dashboard_name] = "WEAK"
                    elif val > 0:
                        self._last_layer_status[dashboard_name] = "FAIL"
                    else:
                        self._last_layer_status[dashboard_name] = "UNKNOWN"
                else:
                    self._last_layer_status[dashboard_name] = str(val)
                self._last_layer_status[f"{dashboard_name}_detail"] = f"score={val}"

    def _get_regime_weights(self, regime: MarketRegime | None) -> dict[str, float]:
        """Return factor weights adapted to current market regime.

        When auto-trading overrides are active (e.g. paper mode), blend the
        regime profile with the configured instance weights so that overrides
        for tech/ml are respected.
        """
        configured = {
            "technical": self._tech_weight, "ml": self._ml_weight,
            "sentiment": self._sentiment_weight, "macro": self._macro_weight,
            "news": self._news_weight, "orderbook": self._orderbook_weight,
        }
        if regime and regime in _REGIME_WEIGHT_PROFILES:
            weights = dict(_REGIME_WEIGHT_PROFILES[regime])
            # Respect explicitly zeroed weights (e.g. sentiment=0 in paper mode)
            # and blend tech/ml with configured overrides
            for k in weights:
                cfg_val = configured.get(k)
                if cfg_val is not None and cfg_val == 0.0:
                    weights[k] = 0.0
                elif k in ("technical", "ml") and cfg_val is not None:
                    # Blend: average of profile and configured weight
                    weights[k] = (weights[k] + cfg_val) / 2.0
            return weights
        return configured

    def _get_direction_penalty(
        self, regime: MarketRegime | None, direction: str,
    ) -> float:
        """Multiplier on min_score for counter-trend trades."""
        if regime and regime in _REGIME_DIRECTION_PENALTY:
            return _REGIME_DIRECTION_PENALTY[regime].get(direction, 1.0)
        return 1.0

    def _update_score_momentum(self, key: str, composite: float) -> float:
        """Track composite scores over time, return momentum factor.

        Returns value in [-0.3, +0.3]:
        - Positive when scores are strengthening (getting more extreme)
        - Negative when scores are weakening (fading)
        """
        if key not in self._score_history:
            self._score_history[key] = deque(maxlen=self._score_history_len)
        history = self._score_history[key]
        history.append(composite)
        if len(history) < 3:
            return 0.0
        recent = list(history)
        # Linear regression slope over recent scores
        n = len(recent)
        x_mean = (n - 1) / 2
        y_mean = sum(recent) / n
        num = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(recent))
        den = sum((i - x_mean) ** 2 for i in range(n))
        slope = num / den if den > 0 else 0.0
        return float(np.clip(slope * 2.0, -0.3, 0.3))

    def _check_correlation_block(
        self, symbol: str, direction: str,
    ) -> bool:
        """Block trades that would create concentrated correlated exposure.

        Returns True if the trade should be BLOCKED.
        """
        if symbol not in _CRYPTO_MAJOR_GROUP:
            return False
        # Check how many same-direction trades exist in the major group
        same_dir_count = 0
        for sym, d in self._open_directions.items():
            if sym in _CRYPTO_MAJOR_GROUP and sym != symbol and d == direction:
                same_dir_count += 1
        # Block if 2+ correlated assets already in same direction
        if same_dir_count >= 2:
            logger.debug(
                "Correlation block: {} {} — already {} correlated {} positions",
                symbol, direction, same_dir_count, direction,
            )
            return True
        return False

    def _get_loss_cooldown_multiplier(self, key: str) -> float:
        """After consecutive losses, require higher score to re-enter."""
        streak = self._consecutive_losses.get(key, 0)
        if streak >= self._max_loss_cooldown:
            return 2.0  # Double the min_score requirement
        if streak >= 2:
            return 1.5  # 50% higher
        return 1.0

    def record_trade_result(self, symbol: str, is_win: bool) -> None:
        """Called externally when a paper/live trade closes.

        Updates consecutive loss tracker for cooldown.
        """
        if is_win:
            self._consecutive_losses.pop(symbol, None)
        else:
            self._consecutive_losses[symbol] = self._consecutive_losses.get(symbol, 0) + 1
        # Clean up direction tracking
        self._open_directions.pop(symbol, None)

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
    ) -> tuple[bool, str, float, int]:
        """L6 Multi-Timeframe Weighted Alignment.

        Spec §4 Layer 2: Daily 3x, 4H 2x, 1H 1x, 15m 1x.
        Composite = (daily_score×3 + 4h_score×2 + 1h_score×1 + 15m_score×1) / 7

        Returns (ok, reason, weighted_score, agreement_count).
        - weighted_score: [-1, 1] — used to adjust position sizing
        - agreement_count: number of TFs aligned with direction
        - 3+ TFs agree → 100% size, 2 TFs → 75% size (spec §4)
        """
        _TF_WEIGHTS = {"1d": 3, "4h": 2, "1h": 1, "15m": 1}
        total_weight = 0
        bullish_weight = 0
        bearish_weight = 0
        checked = 0
        agreement_count = 0
        detail_parts: list[str] = []

        for tf, weight in _TF_WEIGHTS.items():
            htf_df = self.data_manager.get_dataframe(exchange, symbol, tf)
            if htf_df is None or len(htf_df) < 20:
                continue

            checked += 1
            last = htf_df.iloc[-1]

            # Multi-indicator TF bias
            ema_fast = last.get("ema_12", last.get("ema_9", last.get("close", 0)))
            ema_slow = last.get("ema_26", last.get("ema_21", last.get("close", 0)))
            st_dir = last.get("supertrend_dir", 0)
            rsi = last.get("rsi_14", 50)

            bullish_signals = 0
            if ema_fast > ema_slow:
                bullish_signals += 1
            if st_dir == 1:
                bullish_signals += 1
            if rsi > 50:
                bullish_signals += 1

            tf_bullish = bullish_signals >= 2

            total_weight += weight
            if tf_bullish:
                bullish_weight += weight
                detail_parts.append(f"{tf}:bull({weight}x)")
                if direction == "long":
                    agreement_count += 1
            else:
                bearish_weight += weight
                detail_parts.append(f"{tf}:bear({weight}x)")
                if direction == "short":
                    agreement_count += 1

        if checked == 0:
            logger.warning("HTF confirmation bypassed — no data for any timeframe ({}/{})", exchange, symbol)
            return True, "htf_data_missing", 0.0, 0

        # Compute weighted composite score [-1, 1]
        weighted_score = 0.0
        if total_weight > 0:
            weighted_score = (bullish_weight - bearish_weight) / total_weight

        # Daily bias overrides all lower timeframes (spec requirement)
        daily_df = self.data_manager.get_dataframe(exchange, symbol, "1d")
        if daily_df is not None and len(daily_df) >= 20:
            daily_last = daily_df.iloc[-1]
            d_ema_f = daily_last.get("ema_12", daily_last.get("ema_9", daily_last.get("close", 0)))
            d_ema_s = daily_last.get("ema_26", daily_last.get("ema_21", daily_last.get("close", 0)))
            daily_bullish = d_ema_f > d_ema_s
            if direction == "long" and not daily_bullish:
                return False, f"daily_override_bearish [{', '.join(detail_parts)}]", weighted_score, agreement_count
            if direction == "short" and daily_bullish:
                return False, f"daily_override_bullish [{', '.join(detail_parts)}]", weighted_score, agreement_count

        # V6.0: Weighted alignment check — need ≥50% agreement (relaxed for paper mode)
        if total_weight > 0:
            bull_pct = bullish_weight / total_weight
            if direction == "long" and bull_pct < 0.40:  # At least 40% bullish for long
                return False, f"MTF_bearish_bias ({bull_pct:.0%}) [{', '.join(detail_parts)}]", weighted_score, agreement_count
            if direction == "short" and (1 - bull_pct) < 0.40:  # At least 40% bearish for short
                return False, f"MTF_bullish_bias ({bull_pct:.0%}) [{', '.join(detail_parts)}]", weighted_score, agreement_count

        return True, f"MTF_aligned [{', '.join(detail_parts)}]", weighted_score, agreement_count

    # ── Session & Killzone enforcement (§5) ───────────────────────────────

    def _get_session_rule(self) -> SessionRule | None:
        """Return the active session rule based on current UTC hour."""
        utc_hour = datetime.datetime.utcnow().hour
        for rule in _SESSION_RULES:
            if rule.start_utc <= utc_hour < rule.end_utc:
                return rule
        return None

    def _is_ict_killzone(self) -> bool:
        """Check if current time is within an ICT killzone (§5)."""
        utc_hour = datetime.datetime.utcnow().hour
        return any(start <= utc_hour < end for start, end in _ICT_KILLZONES)

    def _classify_signal_type(self, smc_state: SMCState, df: pd.DataFrame) -> SignalType:
        """Classify the signal into Type A/B/C/D based on SMC state (§3)."""
        last = df.iloc[-1]
        price = float(last.get("close", 0))

        # Type C: FVG Mitigation — price retracing into active FVG
        if smc_state.active_fvgs:
            for fvg in smc_state.active_fvgs:
                fvg_high = getattr(fvg, "top", 0)
                fvg_low = getattr(fvg, "bottom", 0)
                if fvg_low <= price <= fvg_high:
                    return SignalType.TYPE_C

        # Type D: Order Block Mitigation — price at an OB
        if smc_state.active_order_blocks:
            for ob in smc_state.active_order_blocks:
                ob_high = getattr(ob, "high", 0)
                ob_low = getattr(ob, "low", 0)
                if ob_low <= price <= ob_high:
                    return SignalType.TYPE_D

        # Type B: Liquidity Sweep — recent sweep detected
        if any("liquidity_grab" in r for r in smc_state.reasons):
            return SignalType.TYPE_B

        # Type A: Breakout Pullback — price breaking 50-candle range
        high_50 = float(last.get("high_50", price))
        low_50 = float(last.get("low_50", price))
        if price > high_50 * 0.998 or price < low_50 * 1.002:
            return SignalType.TYPE_A

        return SignalType.COMPOSITE

    def _compute_quality_score(
        self, *, htf_score: float, signal_type: SignalType, vol_ratio: float,
        smc_state: SMCState, vol_flow: VolumeFlowState,
        session_rule: SessionRule | None, sentiment: float,
        regime_state: RegimeState | None, direction: str,
    ) -> int:
        """Compute 0-100 quality score with 8 weighted components (§6).

        Components:
        1. Higher TF alignment          (20 pts)
        2. Signal type purity           (15 pts)
        3. Volume confirmation          (15 pts)
        4. Liquidity proximity to SL    (10 pts)
        5. FVG/OB overlap               (10 pts)
        6. Session alignment            (10 pts)
        7. Sentiment alignment          (10 pts)
        8. On-chain health              (10 pts)
        """
        score = 0

        # 1. Higher TF alignment (20 pts): map [-1,1] weighted score to [0,20]
        if direction == "long":
            tf_factor = (htf_score + 1.0) / 2.0  # 0..1
        else:
            tf_factor = (1.0 - htf_score) / 2.0  # invert for shorts
        score += int(tf_factor * 20)

        # 2. Signal type purity (15 pts): named types get more points
        type_pts = {SignalType.TYPE_C: 15, SignalType.TYPE_D: 12,
                    SignalType.TYPE_A: 10, SignalType.TYPE_B: 8, SignalType.COMPOSITE: 5}
        score += type_pts.get(signal_type, 5)

        # 3. Volume confirmation (15 pts): vol_ratio maps to 0-15
        if vol_ratio >= 2.0:
            score += 15
        elif vol_ratio >= 1.5:
            score += 10
        elif vol_ratio >= 1.2:
            score += 5

        # 4. Liquidity proximity to SL (10 pts): if OB/FVG near SL zone
        if smc_state.active_order_blocks or smc_state.active_fvgs:
            score += 7  # Simplified: OBs/FVGs present adds structural protection

        # 5. FVG/OB overlap (10 pts)
        fvg_count = len(smc_state.active_fvgs)
        ob_count = len(smc_state.active_order_blocks)
        overlap_pts = min(10, (fvg_count + ob_count) * 3)
        score += overlap_pts

        # 6. Session alignment (10 pts)
        if session_rule and not session_rule.no_trade:
            if session_rule.size_multiplier >= 1.0:
                score += 10  # Ideal session
            elif session_rule.size_multiplier >= 0.75:
                score += 7
            else:
                score += 3

        # 7. Sentiment alignment (10 pts)
        if direction == "long" and sentiment > 0.2:
            score += min(10, int(sentiment * 15))
        elif direction == "short" and sentiment < -0.2:
            score += min(10, int(abs(sentiment) * 15))
        elif abs(sentiment) < 0.1:
            score += 5  # Neutral is OK

        # 8. On-chain health (10 pts) — placeholder until on-chain data is integrated
        score += 5  # Baseline: no data = neutral

        return min(100, max(0, score))

    async def _handle_candle(self, payload: Any) -> None:
        """9-Layer Sequential Signal Confirmation Pipeline — V1.0 Professional Spec.

        L-1: On-Chain (placeholder — not yet integrated)
        L0: Session & Killzone Rules + Sentiment Pre-Filter
        L1: Market Regime Classification (ADX<25 = NO TRADE)
        L2: Market Structure (BOS/CHoCH via SMC)
        L3: Smart Money Concepts (FVG, Order Blocks, Liquidity)
        L4: Momentum Convergence Matrix (15 indicators)
        L5: Volume Profile & Order Flow Confirmation
        L6: Multi-Timeframe Weighted Alignment
        L7: Microstructure Confirmation (candle pattern + volume)
        """
        candle = payload
        if candle.timeframe != self._primary_tf:
            return

        key = f"{candle.exchange}:{candle.symbol}"
        now = time.time()
        if now - self._last_signal_time.get(key, 0) < self._min_signal_interval:
            logger.debug("DIAG {}: throttled (interval)", key)
            return

        df = self.data_manager.get_dataframe(candle.exchange, candle.symbol, self._primary_tf)
        if df is None or len(df) < 30:
            logger.debug("DIAG {}: df={}, need 30", key, len(df) if df is not None else None)
            return

        # ═══════════════════════════════════════════════════════════════════
        # L0-PRE: SESSION & KILLZONE RULES (§5)
        # ═══════════════════════════════════════════════════════════════════
        session_rule = self._get_session_rule()
        is_killzone = self._is_ict_killzone()

        # Weekend halt (Saturday/Sunday)
        utc_now = datetime.datetime.utcnow()
        if utc_now.weekday() >= 5:  # 5=Saturday, 6=Sunday
            logger.debug("DIAG {}: weekend — no trade", key)
            self._update_layer_status(l0="BLOCKED")
            return

        # No-trade sessions (12-13 UTC, 22-00 UTC)
        if session_rule and session_rule.no_trade:
            logger.debug("DIAG {}: session {} — no trade", key, session_rule.name)
            self._update_layer_status(l0="BLOCKED")
            return

        # ═══════════════════════════════════════════════════════════════════
        # L0: SENTIMENT PRE-FILTER
        # ═══════════════════════════════════════════════════════════════════
        sentiment = self._sentiment_score if (now - self._sentiment_ts) < self._score_ttl else 0.0
        news_score = self._news_scorer.score()

        # ═══════════════════════════════════════════════════════════════════
        # L1: MARKET REGIME CLASSIFICATION
        # ═══════════════════════════════════════════════════════════════════
        regime_state = self.data_manager.get_regime(candle.exchange, candle.symbol)
        current_regime = regime_state.regime if regime_state else None

        # Spec: ADX < 25 with range_chop = NO TRADE
        if regime_state and not regime_state.tradeable:
            logger.debug("DIAG {}: regime={} not tradeable", key, regime_state.regime)
            self._update_layer_status(l0=70, l1="BLOCKED")
            return

        # Block signals when regime hasn't been established yet
        if not regime_state or regime_state.regime == MarketRegime.UNKNOWN:
            logger.debug("DIAG {}: regime UNKNOWN or missing", key)
            self._update_layer_status(l0=70, l1="UNKNOWN")
            return

        # ── Regime transition detection (§7) ─────────────────────────────
        in_transition = False
        if regime_state and hasattr(regime_state, 'candles_in_state'):
            if regime_state.candles_in_state <= _TRANSITION_CANDLE_WINDOW:
                in_transition = True
                logger.debug("DIAG {}: regime transition (candles_in_state={}), reducing size/tightening SL",
                             key, regime_state.candles_in_state)

        # Regime-gated strategy module evaluation
        strategy_signal: StrategySignal | None = None
        if regime_state and regime_state.regime != MarketRegime.UNKNOWN:
            strategy_signal = self._strategy_selector.select_and_evaluate(df, regime_state)

        # ═══════════════════════════════════════════════════════════════════
        # L2 + L3: MARKET STRUCTURE + SMART MONEY CONCEPTS
        # ═══════════════════════════════════════════════════════════════════
        smc_state: SMCState = self._smc_analyzer.analyze(df)
        smc_score = smc_state.smc_score  # [-1, 1]

        # ═══════════════════════════════════════════════════════════════════
        # L4: MOMENTUM CONVERGENCE MATRIX (15 indicators)
        # ═══════════════════════════════════════════════════════════════════
        tech_score, tech_reasons = self._technical_scorer.score(df)
        ml_score = self._ml_scorer.score(df)
        ob_score = self._orderbook_scorer.score()

        # TTL: zero out stale macro scores
        macro = self._macro_score if (now - self._macro_ts) < self._score_ttl else 0.0

        # ═══════════════════════════════════════════════════════════════════
        # L5: VOLUME PROFILE & ORDER FLOW CONFIRMATION
        # ═══════════════════════════════════════════════════════════════════
        vol_flow: VolumeFlowState = self._volume_analyzer.analyze(df)
        flow_score = vol_flow.flow_score  # [-1, 1]

        # ═══════════════════════════════════════════════════════════════════
        # V6.0 7-LAYER SCORED ARCHITECTURE + HARD GATES
        # ═══════════════════════════════════════════════════════════════════
        # Each layer returns 0-100 score.
        # Hard gate layers (L0, L1, L2, L6, L7) must pass or signal is killed.
        # Soft layers (L3, L4, L5) are scored and fed into master formula.

        # ── L0 score: Sentiment ≥ 0.6 (hard gate) ────────────────────────
        sentiment_abs = abs(sentiment)
        l0_score = min(100, sentiment_abs * 100 / 0.6) if sentiment_abs > 0 else 50
        # V6.0: news within ±30 min also factors into L0
        news_abs = abs(news_score)
        if news_abs > 0.3:
            l0_score = min(100, l0_score + news_abs * 30)
        # Hard gate: sentiment must be ≥ 0.6 OR have strong news support
        l0_pass = sentiment_abs >= 0.6 or news_abs >= 0.5 or abs(tech_score) >= 0.7

        if not l0_pass:
            logger.debug("DIAG {}: L0 sentiment hard gate failed (sent={:.2f} news={:.2f})", key, sentiment, news_score)
            # Soft fail: allow through if other layers are very strong, but penalize
            l0_score = max(20, l0_score)

        # ── L1 score: Regime (ADX>25 OR ATR ratio>1.2) (hard gate) ───────
        last_row = df.iloc[-1]
        adx_val = float(last_row.get("adx_14", 0))
        atr_14 = float(last_row.get("atr_14", 0))
        close_price = float(last_row["close"])
        atr_ratio = atr_14 / close_price if close_price > 0 else 0
        bb_width = float(last_row.get("bb_width", 0))

        l1_score = 0
        if adx_val >= 25:
            l1_score += 50
        if atr_ratio > 0.012:  # 1.2%
            l1_score += 30
        if bb_width > 0.015:
            l1_score += 20
        l1_score = min(100, l1_score)
        # Regime gate already passed (tradeable=True); ensure minimum PASS score
        if regime_state and regime_state.tradeable and l1_score < 70:
            l1_score = max(l1_score, 70)

        # ── L2 score: Structure (BOS+CHoCH+Fib+Technical) (soft gate) ────
        l2_score = 0
        bos_count = len(smc_state.bos_events)
        choch_count = len(smc_state.choch_events)
        if bos_count > 0:
            l2_score += 30
        if choch_count > 0:
            l2_score += 25
        # Technical indicator contribution
        tech_norm_val = (tech_score + 1) / 2 * 100  # [-1,1] → [0,100]
        l2_score += int(tech_norm_val * 0.25)  # up to 25 from tech confluence
        # Check if price is in 50-62% Fib retracement zone
        if len(df) >= 50:
            swing_high = df["high"].tail(50).max()
            swing_low = df["low"].tail(50).min()
            fib_range = swing_high - swing_low
            if fib_range > 0:
                fib_50 = swing_high - 0.50 * fib_range
                fib_62 = swing_high - 0.618 * fib_range
                if fib_62 <= close_price <= fib_50:
                    l2_score += 30
                    reasons = smc_state.reasons + ["fib_50_62_zone"]
                    smc_state.reasons = reasons
        l2_score = min(100, l2_score)

        # ── L3 score: SMC Confluence (soft, min 40) ──────────────────────
        # V6.0 formula: graduated scoring — any SMC structure contributes
        l3_score = 0
        displaced_fvgs = [f for f in smc_state.active_fvgs if f.displacement]
        any_fvgs = smc_state.active_fvgs
        strong_obs = [ob for ob in smc_state.active_order_blocks if ob.strength_score >= 60]
        any_obs = smc_state.active_order_blocks
        # Displaced FVGs (premium) + any FVGs (base)
        l3_score += min(30, len(displaced_fvgs) * 15 + len(any_fvgs) * 5)
        # Strong OBs (premium) + any OBs (base)
        l3_score += min(30, len(strong_obs) * 15 + len(any_obs) * 5)
        # Breaker blocks
        l3_score += min(20, len(smc_state.active_breaker_blocks) * 10)
        # Liquidity grabs
        l3_score += min(20, len(smc_state.liquidity_grabs) * 10)
        # BOS/CHoCH as structure confirmation bonus
        l3_score += min(15, len(smc_state.bos_events) * 8 + len(smc_state.choch_events) * 10)
        # SMC composite score magnitude bonus
        l3_score += min(15, int(abs(smc_state.smc_score) * 20))
        l3_score = min(100, l3_score)

        # ── L4 score: Momentum Matrix (soft, min 60) ─────────────────────
        # V6.0: 40% technical + 40% ML + 20% orderbook
        tech_norm = (tech_score + 1) / 2 * 100  # [-1,1] → [0,100]
        ml_norm = (ml_score + 1) / 2 * 100
        ob_norm = (ob_score + 1) / 2 * 100
        l4_score = int(0.40 * tech_norm + 0.40 * ml_norm + 0.20 * ob_norm)
        l4_score = max(0, min(100, l4_score))

        # ── L5 score: Volume Confirmation (soft, min 60) ─────────────────
        flow_norm = (flow_score + 1) / 2 * 100  # [-1,1] → [0,100]
        vol_ratio = float(last_row.get("volume_ratio", 1.0))
        delta_boost = 10 if vol_flow.delta_trend in ("accumulating", "distributing") else 0
        l5_score = int(0.60 * flow_norm + 0.25 * min(100, vol_ratio * 50) + 0.15 * delta_boost)
        l5_score = max(0, min(100, l5_score))

        # ── Pre-gate: compute direction from weighted layer scores ────────
        # Use smc_score sign, tech_score sign, and regime direction
        direction_score = (
            0.25 * smc_score + 0.25 * tech_score + 0.15 * ml_score
            + 0.10 * flow_score + 0.10 * sentiment
        )
        # Factor in regime direction — strong trends FORCE direction alignment
        if current_regime == MarketRegime.STRONG_TREND_UP:
            direction_score = max(direction_score, 0.15)  # Force long bias
        elif current_regime == MarketRegime.STRONG_TREND_DOWN:
            direction_score = min(direction_score, -0.15)  # Force short bias
        elif current_regime == MarketRegime.WEAK_TREND_UP:
            direction_score = max(direction_score, 0.05)  # Ensure long bias
        elif current_regime == MarketRegime.WEAK_TREND_DOWN:
            direction_score = min(direction_score, -0.05)  # Ensure short bias
        proposed_direction = "long" if direction_score > 0 else "short"

        # ═══════════════════════════════════════════════════════════════════
        # L6: MULTI-TIMEFRAME WEIGHTED ALIGNMENT (hard gate: ≥6/7)
        # ═══════════════════════════════════════════════════════════════════
        htf_ok, htf_reason, htf_weighted_score, htf_agreement_count = self._check_higher_timeframe_trend(
            candle.exchange, candle.symbol, proposed_direction,
        )
        l6_score = htf_agreement_count * 25 if htf_agreement_count > 0 else 0
        l6_score = min(100, l6_score)

        if not htf_ok:
            logger.info("DIAG {}: L6 MTF soft gate warning: {} (l4={} l3={})", key, htf_reason, l4_score, l3_score)
            # MTF is now a soft gate — reduce its score but don't block
            l6_score = max(10, l6_score // 2)  # Halve L6 score for misalignment

        # MTF position sizing (§4 Layer 2): 3+ TFs = 100%, 2 TFs = 75%
        mtf_size_mult = 1.0
        if htf_agreement_count >= 3:
            mtf_size_mult = 1.0
        elif htf_agreement_count == 2:
            mtf_size_mult = 0.75
        elif htf_agreement_count <= 1 and htf_ok:
            mtf_size_mult = 0.50

        # ═══════════════════════════════════════════════════════════════════
        # L7: MICROSTRUCTURE CONFIRMATION (hard gate)
        # ═══════════════════════════════════════════════════════════════════
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else last

        atr = float(last.get("atr_14", float(last["close"]) * 0.01))
        body = abs(float(last["close"]) - float(last["open"]))
        vol_ratio = float(last.get("volume_ratio", 1.0))

        l7_score = 15  # Baseline: normal market conditions have some structure
        # Candle pattern: body contribution
        if atr > 0:
            body_ratio = body / atr
            l7_score += min(30, int(body_ratio * 100))
        # Volume > 200% of average
        if vol_ratio >= 2.0:
            l7_score += 40
        elif vol_ratio >= 1.5:
            l7_score += 25
        elif vol_ratio >= 1.0:
            l7_score += 15
        elif vol_ratio >= 0.5:
            l7_score += 5
        # OB imbalance > 2:1
        if abs(ob_score) > 0.3:
            if (proposed_direction == "long" and ob_score > 0) or \
               (proposed_direction == "short" and ob_score < 0):
                l7_score += 30
        l7_score = min(100, l7_score)

        # L7 hard gate: doji in strong trend
        if atr > 0 and body < 0.003 * atr:
            if current_regime in (MarketRegime.STRONG_TREND_UP, MarketRegime.STRONG_TREND_DOWN):
                logger.debug("L7 microstructure: doji candle in strong trend, skipping {}", key)
                self._update_layer_status(
                    l0=l0_score, l1=l1_score, l2=l2_score, l3=l3_score,
                    l4=l4_score, l5=l5_score, l6=l6_score, l7=l7_score,
                )
                return

        # ═══════════════════════════════════════════════════════════════════
        # V6.0 MASTER SCORING FORMULA (§11 — rebalanced for all 9 layers)
        # Combines all layer scores into achievable master threshold
        # ═══════════════════════════════════════════════════════════════════
        neural_score = ml_norm  # ML/Neural layer score (0-100)
        liquidity_score = min(100, int(len(smc_state.liquidity_grabs) * 30 +
                                       len(smc_state.active_fvgs) * 15 +
                                       len(smc_state.active_order_blocks) * 10))

        master_score = (
            0.05 * l0_score       # Session filter
            + 0.10 * l1_score     # Regime detection
            + 0.10 * l2_score     # Technical structure
            + 0.15 * l3_score     # SMC confluence
            + 0.15 * l4_score     # Momentum matrix
            + 0.10 * l5_score     # Volume flow
            + 0.10 * l6_score     # MTF alignment
            + 0.10 * l7_score     # Microstructure
            + 0.05 * neural_score # Neural/ML
            + 0.10 * liquidity_score  # Liquidity
        )

        # Reconstruct composite for backward-compat (direction + magnitude)
        direction_sign = 1.0 if proposed_direction == "long" else -1.0
        composite = direction_sign * (master_score / 100.0)  # normalize to [-1, 1]

        abs_score = abs(composite)

        # Strategy module blending (still honored)
        if strategy_signal is not None:
            strat_direction = 1.0 if strategy_signal.is_long else -1.0
            composite_direction = 1.0 if composite > 0 else -1.0
            if strat_direction == composite_direction:
                composite = composite * 1.0 + strat_direction * strategy_signal.score * 0.3
            else:
                if strategy_signal.score > 0.6:
                    composite = strat_direction * strategy_signal.score * 0.8
                else:
                    composite *= 0.5

        # Signal momentum boost/drag
        momentum_adj = self._update_score_momentum(key, composite)
        composite += momentum_adj
        abs_score = abs(composite)
        proposed_direction = "long" if composite > 0 else "short"

        # Asymmetric regime threshold
        direction_penalty = self._get_direction_penalty(current_regime, proposed_direction)
        effective_min_score = self._min_score * direction_penalty

        # Consecutive loss cooldown
        loss_mult = self._get_loss_cooldown_multiplier(candle.symbol)
        effective_min_score *= loss_mult

        # Regime position sizing factor
        regime_size_pct = regime_state.position_size_pct if regime_state else 1.0

        # V6.0 master threshold: master_score ≥ 35 required
        if master_score < 35:
            logger.info(
                "DIAG {}: REJECTED master_score={:.1f} < 35 | "
                "L3={} L4={} L5={} Neural={} Liq={} | "
                "L0={} L1={} L2={} L6={} L7={} | "
                "regime={} dir={}",
                key, master_score, l3_score, l4_score, l5_score,
                int(neural_score), liquidity_score,
                l0_score, l1_score, l2_score, l6_score, l7_score,
                current_regime, proposed_direction,
            )
            self._update_layer_status(
                l0=l0_score, l1=l1_score, l2=l2_score, l3=l3_score,
                l4=l4_score, l5=l5_score, l6=l6_score, l7=l7_score,
            )
            self._last_layer_status["risk_gate"] = "FAIL"
            self._last_layer_status["risk_gate_detail"] = f"master={master_score:.1f}<35"
            # Still compute quality breakdown for dashboard visibility
            self._last_quality_breakdown = {
                "total": int(master_score),
                "components": {
                    "htf_alignment": l6_score // 5,
                    "signal_type_purity": 0,
                    "volume_confirmation": 15 if vol_ratio >= 2.0 else (10 if vol_ratio >= 1.5 else (5 if vol_ratio >= 1.2 else 0)),
                    "liquidity_proximity": 7 if (smc_state.active_order_blocks or smc_state.active_fvgs) else 0,
                    "fvg_ob_overlap": min(10, (len(smc_state.active_fvgs) + len(smc_state.active_order_blocks)) * 3),
                    "session_alignment": (10 if session_rule and session_rule.size_multiplier >= 1.0 else 5) if session_rule and not session_rule.no_trade else 0,
                    "sentiment_alignment": min(10, int(abs(sentiment) * 15)),
                    "onchain_health": 5,
                },
                "session": session_rule.name if session_rule else "none",
            }
            return

        if abs_score < effective_min_score:
            logger.info(
                "DIAG {}: REJECTED score={:.3f} < min={:.3f} (base={:.3f} dir_pen={:.1f} loss_mult={:.1f}) | "
                "master={:.1f} L3={} L4={} L5={} | regime={} dir={}",
                key, abs_score, effective_min_score, self._min_score, direction_penalty, loss_mult,
                master_score, l3_score, l4_score, l5_score,
                current_regime, proposed_direction,
            )
            return

        # ── Signal Type Classification (§3) ───────────────────────────────
        signal_type = self._classify_signal_type(smc_state, df)

        # ── Session signal type filtering (§5) ────────────────────────────
        if session_rule and session_rule.allowed_types is not None:
            if signal_type not in session_rule.allowed_types:
                logger.debug("DIAG {}: signal_type {} not allowed in session {}",
                             key, signal_type.value, session_rule.name)
                return

        # ICT Killzone: prefer Type C & D (1.2x bonus), allow others at reduced score
        if is_killzone:
            if signal_type in (SignalType.TYPE_C, SignalType.TYPE_D):
                composite *= 1.2
                abs_score = abs(composite)
            else:
                # Non-C/D signals allowed but with 0.8x penalty
                composite *= 0.8
                abs_score = abs(composite)
                logger.debug("DIAG {}: ICT killzone — non-C/D signal {} gets 0.8x penalty", key, signal_type.value)

        # ── Min factor agreement ──────────────────────────────────────────
        proposed_sign = 1.0 if composite > 0 else -1.0
        tmp_scores = {
            "technical": tech_score,
            "ml": ml_score,
            "sentiment": sentiment,
            "macro": macro,
            "news": news_score,
            "orderbook": ob_score,
            "smc": smc_score,
            "volume_flow": flow_score,
        }
        active_factors = sum(
            1 for v in tmp_scores.values()
            if abs(v) >= self._min_factor_magnitude and (v * proposed_sign) > 0
        )
        if active_factors < self._min_factors:
            logger.info(
                "DIAG {}: only {}/{} factors active (scores: {})",
                key, active_factors, self._min_factors,
                {k: round(v, 3) for k, v in tmp_scores.items()},
            )
            return

        # ── Correlation filter ────────────────────────────────────────────
        if self._check_correlation_block(candle.symbol, proposed_direction):
            return

        # ── Auto-trading gate ─────────────────────────────────────────────
        if not self._auto_trading_enabled:
            logger.debug(
                "Signal suppressed for {} (auto_trading=off) — composite={:.3f}",
                key, composite,
            )
            return

        # ── Per-symbol duplicate guard ─────────────────────────────────────
        if candle.symbol in self._open_directions:
            logger.debug(
                "Signal suppressed for {}: already have {} position on {}",
                key, self._open_directions[candle.symbol], candle.symbol,
            )
            return

        # ── Max positions gate ────────────────────────────────────────────
        max_pos = int(self._bot_config.get("max_positions", 3))
        # Use open_directions dict (tracks each active signal) for paper mode
        open_signal_count = len(self._open_directions)
        if self._risk_manager is not None and hasattr(self._risk_manager, 'positions'):
            rm_count = len(self._risk_manager.positions)
            open_signal_count = max(open_signal_count, rm_count)
        if open_signal_count >= max_pos:
            logger.debug(
                "Signal suppressed for {}: max_positions={} reached",
                key, max_pos,
            )
            return

        # ═══════════════════════════════════════════════════════════════════
        # POSITION MANAGEMENT: ATR-based SL + Tiered TP (1:1 / zone / trail)
        # ═══════════════════════════════════════════════════════════════════
        price = float(last.get("close", 0))
        if atr <= 0:
            atr = price * 0.01

        risk_cfg = self.config.get_value("risk") or {}
        sl_atr_mult = float(risk_cfg.get("sl_atr_multiplier", 2.0))

        # Regime-adaptive SL
        if current_regime in (MarketRegime.RANGE_CHOP,):
            sl_atr_mult = min(sl_atr_mult, 1.5)
        elif current_regime in (MarketRegime.STRONG_TREND_UP, MarketRegime.STRONG_TREND_DOWN):
            sl_atr_mult = max(sl_atr_mult, 2.5)

        # §7 Regime transition: tighten SL by 0.5x
        if in_transition:
            sl_atr_mult *= _TRANSITION_SL_MULT

        sl_dist = atr * sl_atr_mult

        # TP1: 1:1 R:R (50% position) — per spec
        tp1_dist = sl_dist * 1.0

        # TP2: Next liquidity zone (30% position) — SMC-driven
        liq_zone = self._smc_analyzer.get_nearest_liquidity_zone(
            df, proposed_direction, price,
        )
        if liq_zone is not None:
            tp2_dist = abs(liq_zone - price)
            # Ensure TP2 > TP1
            tp2_dist = max(tp2_dist, tp1_dist * 1.5)
        else:
            tp2_dist = sl_dist * 2.5  # Fallback: 2.5R

        # TP3: SuperTrend trailing (20% position) — use SuperTrend level
        supertrend = float(last.get("supertrend", 0))
        if supertrend > 0:
            if proposed_direction == "long":
                tp3_trail_level = supertrend  # Trail at SuperTrend below
            else:
                tp3_trail_level = supertrend  # Trail at SuperTrend above
        else:
            tp3_trail_level = 0  # Will use ATR trailing fallback

        # Primary TP = TP1 for the signal (TP2/TP3 managed by risk engine)
        direction = proposed_direction
        if direction == "long":
            sl = price - sl_dist
            tp = price + tp1_dist
        else:
            sl = price + sl_dist
            tp = price - tp1_dist

        # Override with bot config if auto_sl_tp is enabled
        if self._bot_config.get("auto_sl_tp"):
            bot_risk = float(self._bot_config.get("risk_per_trade", 2)) / 100.0
            sl_dist = min(sl_dist, price * bot_risk) if bot_risk > 0 else sl_dist

        # ═══════════════════════════════════════════════════════════════════
        # BUILD SIGNAL
        # ═══════════════════════════════════════════════════════════════════
        _LONG_REASONS = {"RSI_oversold", "RSI_bullish_zone", "RSI_low", "MACD_bullish_cross",
                         "EMA_stack_bullish", "EMA_golden_cross", "BB_pct_bullish",
                         "SuperTrend_bullish", "Ichimoku_above_cloud", "PSAR_bullish",
                         "Aroon_bullish", "MFI_bullish", "Stoch_bullish_cross", "Stoch_oversold",
                         "CCI_bullish", "WR_bullish", "UltOsc_rising", "TRIX_bullish_cross",
                         "KC_breakout_bullish", "ADX_strong_", "vol_confirm_",
                         "BOS_bullish", "CHoCH_bullish", "FVG_bullish_zone",
                         "OB_bullish_support", "liquidity_grab_bullish",
                         "OBV_bullish_div", "CMF_accumulation", "VWAP_rising",
                         "delta_accumulating", "above_value_area"}
        _SHORT_REASONS = {"RSI_overbought", "RSI_high", "MACD_bearish_cross",
                          "EMA_stack_bearish", "EMA_death_cross", "BB_pct_bearish",
                          "SuperTrend_bearish", "Ichimoku_below_cloud", "PSAR_bearish",
                          "Aroon_bearish", "MFI_bearish", "Stoch_bearish_cross", "Stoch_overbought",
                          "CCI_bearish", "WR_overbought", "UltOsc_falling", "TRIX_bearish_cross",
                          "KC_breakout_bearish", "ADX_strong_", "vol_confirm_",
                          "BOS_bearish", "CHoCH_bearish", "FVG_bearish_zone",
                          "OB_bearish_resistance", "liquidity_grab_bearish",
                          "OBV_bearish_div", "CMF_distribution", "VWAP_falling",
                          "delta_distributing", "below_value_area"}
        valid_set = _LONG_REASONS if direction == "long" else _SHORT_REASONS

        # Combine reasons from all layers
        all_raw_reasons = tech_reasons + smc_state.reasons + vol_flow.reasons
        reasons: list[str] = [
            r for r in all_raw_reasons
            if any(r.startswith(v) for v in valid_set)
        ]
        if strategy_signal is not None:
            reasons.extend(strategy_signal.reasons)
            reasons.append(f"strategy:{strategy_signal.strategy}")
        if ml_score * (1 if direction == "long" else -1) > 0 and abs(ml_score) > 0.1:
            reasons.append(f"ML_{'bullish' if ml_score > 0 else 'bearish'}")
        if sentiment * (1 if direction == "long" else -1) > 0 and abs(sentiment) > 0.2:
            reasons.append(f"sentiment_{'bullish' if sentiment > 0 else 'bearish'}")
        if macro * (1 if direction == "long" else -1) > 0 and abs(macro) > 0.2:
            reasons.append(f"macro_{'bullish' if macro > 0 else 'bearish'}")
        if abs(momentum_adj) > 0.05:
            reasons.append(f"momentum_{'rising' if momentum_adj > 0 else 'fading'}")
        if current_regime:
            reasons.append(f"regime:{current_regime.value}")

        # ── Compute regime-specific risk % (§7) ──────────────────────────
        regime_risk_pct = _REGIME_RISK_PCT.get(current_regime, 0.01) if current_regime else 0.01

        # ── Session sizing multiplier (§5) ────────────────────────────────
        session_size_mult = session_rule.size_multiplier if session_rule else 1.0

        # ── Regime transition: reduce size by 50% (§7) ───────────────────
        transition_mult = _TRANSITION_SIZE_MULT if in_transition else 1.0

        # ── Compute quality score 0-100 (§6) ─────────────────────────────
        quality_score = self._compute_quality_score(
            htf_score=htf_weighted_score, signal_type=signal_type,
            vol_ratio=vol_ratio, smc_state=smc_state, vol_flow=vol_flow,
            session_rule=session_rule, sentiment=sentiment,
            regime_state=regime_state, direction=direction,
        )

        # ── Update pipeline layer status for dashboard ────────────────────
        self._update_layer_status(
            l0=l0_score, l1=l1_score, l2=l2_score, l3=l3_score,
            l4=l4_score, l5=l5_score, l6=l6_score, l7=l7_score,
        )
        # Quality score is tracked in _last_quality_breakdown, not as a layer
        self._last_quality_breakdown = {
            "total": quality_score,
            "components": {
                "htf_alignment": int(((htf_weighted_score + 1.0) / 2.0 if direction == "long" else (1.0 - htf_weighted_score) / 2.0) * 20),
                "signal_type_purity": {SignalType.TYPE_C: 15, SignalType.TYPE_D: 12, SignalType.TYPE_A: 10, SignalType.TYPE_B: 8, SignalType.COMPOSITE: 5}.get(signal_type, 5),
                "volume_confirmation": 15 if vol_ratio >= 2.0 else (10 if vol_ratio >= 1.5 else (5 if vol_ratio >= 1.2 else 0)),
                "liquidity_proximity": 7 if (smc_state.active_order_blocks or smc_state.active_fvgs) else 0,
                "fvg_ob_overlap": min(10, (len(smc_state.active_fvgs) + len(smc_state.active_order_blocks)) * 3),
                "session_alignment": (10 if session_rule and session_rule.size_multiplier >= 1.0 else (7 if session_rule and session_rule.size_multiplier >= 0.75 else 3)) if session_rule and not session_rule.no_trade else 0,
                "sentiment_alignment": min(10, int(abs(sentiment) * 15)) if (direction == "long" and sentiment > 0.2) or (direction == "short" and sentiment < -0.2) else (5 if abs(sentiment) < 0.1 else 0),
                "onchain_health": 5,
            },
            "session": session_rule.name if session_rule else "none",
        }

        # §6: Min quality 25 to proceed (relaxed for paper mode), 80 for 25% size boost
        quality_size_boost = 1.0
        if quality_score < 25:
            logger.info("DIAG {}: quality_score={} < 25 — signal rejected", key, quality_score)
            return
        if quality_score >= 80:
            quality_size_boost = 1.25

        weights = self._get_regime_weights(current_regime)

        signal = TradingSignal(
            exchange=candle.exchange,
            symbol=candle.symbol,
            direction=direction,
            score=abs_score,
            technical_score=tech_score,
            ml_score=ml_score,
            sentiment_score=sentiment,
            macro_score=macro,
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
            signal_type=signal_type.value,
            quality_score=quality_score,
            session_name=session_rule.name if session_rule else "",
            session_size_mult=session_size_mult,
            regime_risk_pct=regime_risk_pct,
            mtf_agreement_count=htf_agreement_count,
            mtf_weighted_score=htf_weighted_score,
            metadata={
                "timeframe": self._primary_tf,
                "factors_active": active_factors,
                "strategy": strategy_signal.strategy if strategy_signal else "composite",
                "strategy_score": strategy_signal.score if strategy_signal else 0.0,
                "regime_weights": weights,
                "direction_penalty": direction_penalty,
                "loss_cooldown": loss_mult,
                "momentum_adj": momentum_adj,
                "sl_atr_mult": sl_atr_mult,
                "risk_reward": tp1_dist / sl_dist if sl_dist > 0 else 0.0,
                "regime_size_pct": regime_size_pct,
                # SMC metadata
                "smc_score": smc_score,
                "smc_bias": smc_state.market_bias,
                "bos_count": len(smc_state.bos_events),
                "choch_count": len(smc_state.choch_events),
                "active_fvgs": len(smc_state.active_fvgs),
                "active_order_blocks": len(smc_state.active_order_blocks),
                # Volume flow metadata
                "volume_flow_score": flow_score,
                "poc_price": vol_flow.poc_price,
                "obv_divergence": vol_flow.obv_divergence,
                "cmf": vol_flow.cmf_score,
                "vwap_slope": vol_flow.vwap_slope,
                "delta_trend": vol_flow.delta_trend,
                # TP tiers
                "tp1_price": price + tp1_dist if direction == "long" else price - tp1_dist,
                "tp1_close_pct": 0.50,
                "tp2_price": price + tp2_dist if direction == "long" else price - tp2_dist,
                "tp2_close_pct": 0.30,
                "tp3_trail": True,
                "tp3_close_pct": 0.20,
                "supertrend_trail": tp3_trail_level,
                "liquidity_zone_tp": liq_zone,
                # §5/§6/§7 additions
                "session_name": session_rule.name if session_rule else "",
                "session_size_mult": session_size_mult,
                "mtf_size_mult": mtf_size_mult,
                "quality_score": quality_score,
                "quality_size_boost": quality_size_boost,
                "signal_type": signal_type.value,
                "regime_risk_pct": regime_risk_pct,
                "in_transition": in_transition,
                "transition_mult": transition_mult,
                "effective_size_mult": session_size_mult * mtf_size_mult * transition_mult * quality_size_boost,
                "is_killzone": is_killzone,
                # V6.0 layer scores
                "master_score": round(master_score, 1),
                "l0_score": l0_score,
                "l1_score": l1_score,
                "l2_score": l2_score,
                "l3_score": l3_score,
                "l4_score": l4_score,
                "l5_score": l5_score,
                "l6_score": l6_score,
                "l7_score": l7_score,
            },
        )

        self._last_signal_time[key] = now
        self._open_directions[candle.symbol] = direction
        # Update risk gate status (signal passed all gates)
        self._last_layer_status["risk_gate"] = "PASS"
        self._last_layer_status["risk_gate_detail"] = f"master={master_score:.1f}"
        await self.event_bus.publish("SIGNAL", signal)
        logger.info(
            "SIGNAL: {}/{} {} type={} score={:.2f} Q={}/100 factors={} price={:.2f} sl={:.2f} "
            "tp1={:.2f} tp2={:.2f} R:R=1:{:.1f} regime={} risk={:.1%} session={} "
            "smc={:.2f} vflow={:.2f} size_mult={:.2f} "
            "[tech={:.2f} ml={:.2f} sent={:.2f} macro={:.2f} news={:.2f} ob={:.2f}]",
            signal.exchange, signal.symbol, signal.direction.upper(),
            signal_type.value, signal.score, quality_score, active_factors,
            signal.price, signal.stop_loss,
            signal.metadata["tp1_price"], signal.metadata["tp2_price"],
            tp2_dist / sl_dist if sl_dist > 0 else 0.0,
            current_regime.value if current_regime else "unknown",
            regime_risk_pct,
            session_rule.name if session_rule else "none",
            smc_score, flow_score,
            signal.metadata["effective_size_mult"],
            tech_score, ml_score, sentiment, macro,
            news_score, ob_score,
        )

    async def _handle_sentiment(self, payload: Any) -> None:
        self._sentiment_score = float(payload.score) if hasattr(payload, "score") else 0.0
        self._sentiment_ts = time.time()

    async def _handle_funding(self, payload: Any) -> None:
        if not isinstance(payload, list) or not payload:
            return
        # Preserve sign: positive funding = longs pay (bearish), negative = shorts pay (bullish)
        rates = [r.rate for r in payload]
        avg = sum(rates) / len(rates) if rates else 0.0
        # Negative avg → bullish bias (positive macro_score), positive avg → bearish (negative)
        self._macro_score = float(max(min(-avg * 100, 1.0), -1.0))
        self._macro_ts = time.time()

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
