"""Volume Profile & Order Flow Analysis — V1.0 Spec.

Implements:
- Volume Profile Fixed Range (VPFR)
- Point of Control (POC) & Value Area
- On-Balance Volume (OBV) divergence
- Cumulative Volume Delta
- VWAP slope confirmation
- Chaikin Money Flow (CMF) scoring
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class VolumeProfileLevel:
    price: float
    volume: float
    is_poc: bool = False
    in_value_area: bool = False


@dataclass
class VolumeFlowState:
    """Composite volume/order-flow analysis."""
    poc_price: float = 0.0
    value_area_high: float = 0.0
    value_area_low: float = 0.0
    obv_divergence: str = "none"       # "bullish", "bearish", "none"
    cmf_score: float = 0.0
    vwap_slope: str = "flat"           # "rising", "falling", "flat"
    volume_delta: float = 0.0          # cumulative session delta
    delta_trend: str = "neutral"       # "accumulating", "distributing", "neutral"
    flow_score: float = 0.0            # [-1, 1] composite
    reasons: list[str] = field(default_factory=list)


class VolumeProfileAnalyzer:
    """Computes volume profile, order flow metrics, and delta from OHLCV data."""

    def __init__(self, profile_bins: int = 50, value_area_pct: float = 0.70) -> None:
        self._bins = profile_bins
        self._va_pct = value_area_pct

    # ── Volume Profile Fixed Range ────────────────────────────────────────
    def _compute_vpfr(
        self, df: pd.DataFrame, lookback: int = 100,
    ) -> tuple[float, float, float, list[VolumeProfileLevel]]:
        """Compute POC, VAH, VAL from last `lookback` bars.

        Returns (poc, vah, val, levels).
        """
        segment = df.tail(lookback)
        if len(segment) < 10:
            mid = float(df["close"].iloc[-1])
            return mid, mid, mid, []

        price_low = float(segment["low"].min())
        price_high = float(segment["high"].max())
        if price_high == price_low:
            return price_low, price_high, price_low, []

        bin_edges = np.linspace(price_low, price_high, self._bins + 1)
        bin_vols = np.zeros(self._bins)

        for _, row in segment.iterrows():
            vol = row.get("volume", 1.0)
            # Distribute volume across the bar's range
            bar_low = row["low"]
            bar_high = row["high"]
            for j in range(self._bins):
                if bin_edges[j + 1] >= bar_low and bin_edges[j] <= bar_high:
                    # Fraction of bar range that overlaps this bin
                    overlap_low = max(bin_edges[j], bar_low)
                    overlap_high = min(bin_edges[j + 1], bar_high)
                    bar_range = bar_high - bar_low if bar_high > bar_low else 1.0
                    fraction = (overlap_high - overlap_low) / bar_range
                    bin_vols[j] += vol * fraction

        # POC: bin with highest volume
        poc_idx = int(np.argmax(bin_vols))
        poc_price = float((bin_edges[poc_idx] + bin_edges[poc_idx + 1]) / 2)

        # Value area: expand from POC until 70% of total volume captured
        total_vol = bin_vols.sum()
        if total_vol == 0:
            return poc_price, price_high, price_low, []

        va_vol = bin_vols[poc_idx]
        lo_idx, hi_idx = poc_idx, poc_idx
        while va_vol / total_vol < self._va_pct:
            expand_up = bin_vols[hi_idx + 1] if hi_idx + 1 < self._bins else 0
            expand_down = bin_vols[lo_idx - 1] if lo_idx - 1 >= 0 else 0
            if expand_up >= expand_down and hi_idx + 1 < self._bins:
                hi_idx += 1
                va_vol += bin_vols[hi_idx]
            elif lo_idx - 1 >= 0:
                lo_idx -= 1
                va_vol += bin_vols[lo_idx]
            else:
                break

        vah = float(bin_edges[hi_idx + 1])
        val = float(bin_edges[lo_idx])

        levels = []
        for j in range(self._bins):
            mid_price = float((bin_edges[j] + bin_edges[j + 1]) / 2)
            levels.append(VolumeProfileLevel(
                price=mid_price,
                volume=float(bin_vols[j]),
                is_poc=(j == poc_idx),
                in_value_area=(lo_idx <= j <= hi_idx),
            ))

        return poc_price, vah, val, levels

    # ── OBV Divergence ────────────────────────────────────────────────────
    @staticmethod
    def _detect_obv_divergence(df: pd.DataFrame, lookback: int = 20) -> str:
        """Detect OBV vs price divergence over last bars."""
        if "obv" not in df.columns or len(df) < lookback:
            return "none"
        seg = df.tail(lookback)
        price_change = seg["close"].iloc[-1] - seg["close"].iloc[0]
        obv_change = seg["obv"].iloc[-1] - seg["obv"].iloc[0]

        # Bullish divergence: price down but OBV up (accumulation)
        if price_change < 0 and obv_change > 0:
            return "bullish"
        # Bearish divergence: price up but OBV down (distribution)
        if price_change > 0 and obv_change < 0:
            return "bearish"
        return "none"

    # ── VWAP slope ────────────────────────────────────────────────────────
    @staticmethod
    def _vwap_slope(df: pd.DataFrame, lookback: int = 10) -> str:
        if "vwap" not in df.columns or len(df) < lookback:
            return "flat"
        vwap_seg = df["vwap"].tail(lookback).dropna()
        if len(vwap_seg) < 3:
            return "flat"
        slope = (vwap_seg.iloc[-1] - vwap_seg.iloc[0]) / vwap_seg.iloc[0]
        if slope > 0.001:
            return "rising"
        elif slope < -0.001:
            return "falling"
        return "flat"

    # ── Volume Delta (approximate from OHLCV) ────────────────────────────
    @staticmethod
    def _compute_delta(df: pd.DataFrame, lookback: int = 50) -> tuple[float, str]:
        """Approximate volume delta from candle direction."""
        seg = df.tail(lookback)
        delta = 0.0
        for _, row in seg.iterrows():
            vol = row.get("volume", 0.0)
            close = row["close"]
            open_ = row["open"]
            high = row["high"]
            low = row["low"]
            # Estimate buy/sell ratio from wick analysis
            bar_range = high - low if high > low else 1.0
            close_pos = (close - low) / bar_range  # 0..1, 1 = closed at high
            buy_pct = close_pos
            delta += vol * (2 * buy_pct - 1)

        if delta > 0:
            trend = "accumulating"
        elif delta < 0:
            trend = "distributing"
        else:
            trend = "neutral"
        return delta, trend

    # ── Composite analysis ────────────────────────────────────────────────
    def analyze(self, df: pd.DataFrame) -> VolumeFlowState:
        """Full volume profile + flow analysis."""
        if df is None or len(df) < 20:
            return VolumeFlowState()

        state = VolumeFlowState()
        reasons: list[str] = []
        score = 0.0

        # VPFR
        poc, vah, val, levels = self._compute_vpfr(df)
        state.poc_price = poc
        state.value_area_high = vah
        state.value_area_low = val

        close = float(df["close"].iloc[-1])

        # Price position relative to value area
        if close > vah:
            score += 0.2
            reasons.append("above_value_area")
        elif close < val:
            score -= 0.2
            reasons.append("below_value_area")
        else:
            reasons.append("inside_value_area")

        # POC proximity
        poc_dist = abs(close - poc) / poc if poc > 0 else 0
        if poc_dist < 0.005:
            reasons.append("near_POC")

        # OBV divergence
        obv_div = self._detect_obv_divergence(df)
        state.obv_divergence = obv_div
        if obv_div == "bullish":
            score += 0.3
            reasons.append("OBV_bullish_div")
        elif obv_div == "bearish":
            score -= 0.3
            reasons.append("OBV_bearish_div")

        # CMF
        cmf = float(df["cmf"].iloc[-1]) if "cmf" in df.columns and not np.isnan(df["cmf"].iloc[-1]) else 0.0
        state.cmf_score = cmf
        if cmf > 0.05:
            score += 0.2
            reasons.append("CMF_accumulation")
        elif cmf < -0.05:
            score -= 0.2
            reasons.append("CMF_distribution")

        # VWAP slope
        vslope = self._vwap_slope(df)
        state.vwap_slope = vslope
        if vslope == "rising":
            score += 0.15
            reasons.append("VWAP_rising")
        elif vslope == "falling":
            score -= 0.15
            reasons.append("VWAP_falling")

        # Volume delta
        delta, delta_trend = self._compute_delta(df)
        state.volume_delta = delta
        state.delta_trend = delta_trend
        if delta_trend == "accumulating":
            score += 0.15
            reasons.append("delta_accumulating")
        elif delta_trend == "distributing":
            score -= 0.15
            reasons.append("delta_distributing")

        state.flow_score = float(np.clip(score, -1.0, 1.0))
        state.reasons = reasons
        return state
