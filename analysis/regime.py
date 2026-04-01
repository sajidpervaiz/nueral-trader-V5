from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger


class MarketRegime(str, Enum):
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGING = "ranging"
    VOLATILE = "volatile"
    UNKNOWN = "unknown"


@dataclass
class RegimeState:
    regime: MarketRegime
    confidence: float
    adx: float
    trend_slope: float
    realized_vol: float
    hurst_exponent: float
    timestamp: int


class RegimeDetector:
    def __init__(
        self,
        adx_threshold: float = 25.0,
        vol_threshold: float = 0.6,
        hurst_threshold: float = 0.55,
    ) -> None:
        self._adx_threshold = adx_threshold
        self._vol_threshold = vol_threshold
        self._hurst_threshold = hurst_threshold

    def _calc_adx(self, high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
        prev_high = high.shift(1)
        prev_low = low.shift(1)
        prev_close = close.shift(1)

        dm_plus = (high - prev_high).clip(lower=0)
        dm_minus = (prev_low - low).clip(lower=0)
        mask = dm_plus <= dm_minus
        dm_plus[mask] = 0
        mask2 = dm_minus <= dm_plus
        dm_minus[mask2] = 0

        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)

        atr_s = tr.ewm(com=period - 1, adjust=False).mean()
        di_plus = 100 * dm_plus.ewm(com=period - 1, adjust=False).mean() / atr_s.replace(0, np.nan)
        di_minus = 100 * dm_minus.ewm(com=period - 1, adjust=False).mean() / atr_s.replace(0, np.nan)

        dx = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan)
        adx = dx.ewm(com=period - 1, adjust=False).mean()
        return float(adx.iloc[-1]) if not adx.empty else 0.0

    def _calc_hurst(self, series: pd.Series, max_lag: int = 20) -> float:
        prices = series.dropna().values
        if len(prices) < 30:
            return 0.5
        lags = range(2, min(max_lag, len(prices) // 4))
        tau = []
        for lag in lags:
            diffs = prices[lag:] - prices[:-lag]
            tau.append(np.sqrt(np.var(diffs)))
        if len(tau) < 2:
            return 0.5
        try:
            poly = np.polyfit(np.log(list(lags)), np.log(tau), 1)
            return float(poly[0])
        except Exception:
            return 0.5

    def _calc_realized_vol(self, close: pd.Series, periods: int = 20) -> float:
        returns = close.pct_change().dropna()
        if len(returns) < periods:
            return 0.0
        return float(returns.tail(periods).std() * np.sqrt(252))

    def detect(self, df: pd.DataFrame) -> RegimeState:
        import time
        if len(df) < 30:
            return RegimeState(
                regime=MarketRegime.UNKNOWN,
                confidence=0.0,
                adx=0.0,
                trend_slope=0.0,
                realized_vol=0.0,
                hurst_exponent=0.5,
                timestamp=int(time.time()),
            )

        high = df["high"]
        low = df["low"]
        close = df["close"]

        adx = self._calc_adx(high, low, close)
        realized_vol = self._calc_realized_vol(close)
        hurst = self._calc_hurst(close)

        recent = close.tail(20).values
        trend_slope = float(np.polyfit(range(len(recent)), recent, 1)[0]) / float(recent.mean()) if recent.mean() > 0 else 0.0

        if realized_vol > self._vol_threshold:
            regime = MarketRegime.VOLATILE
            confidence = min(1.0, realized_vol / (self._vol_threshold * 2))
        elif adx > self._adx_threshold and hurst > self._hurst_threshold:
            regime = MarketRegime.TRENDING_UP if trend_slope > 0 else MarketRegime.TRENDING_DOWN
            confidence = min(1.0, adx / 50.0)
        else:
            regime = MarketRegime.RANGING
            confidence = max(0.0, 1.0 - adx / self._adx_threshold)

        return RegimeState(
            regime=regime,
            confidence=confidence,
            adx=adx,
            trend_slope=trend_slope,
            realized_vol=realized_vol,
            hurst_exponent=hurst,
            timestamp=int(time.time()),
        )
