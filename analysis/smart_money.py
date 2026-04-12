"""Smart Money Concepts (SMC/ICT) Framework — V1.0 Spec.

Implements:
- Break of Structure (BOS)
- Change of Character (CHoCH)
- Fair Value Gaps (FVG)
- Order Blocks
- Breaker Blocks
- Liquidity Grabs
- Inducement detection
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class FairValueGap:
    index: int
    direction: str  # "bullish" or "bearish"
    top: float
    bottom: float
    size_pct: float
    filled: bool = False


@dataclass
class OrderBlock:
    index: int
    direction: str  # "bullish" or "bearish"
    high: float
    low: float
    mitigated: bool = False


@dataclass
class StructureBreak:
    index: int
    break_type: str  # "BOS" or "CHoCH"
    direction: str  # "bullish" or "bearish"
    level: float


@dataclass
class LiquidityGrab:
    index: int
    direction: str  # "bullish" (swept lows, reversed up) or "bearish"
    swept_level: float
    reversal_close: float


@dataclass
class SMCState:
    """Complete SMC analysis for the latest bar."""
    bos_events: list[StructureBreak] = field(default_factory=list)
    choch_events: list[StructureBreak] = field(default_factory=list)
    active_fvgs: list[FairValueGap] = field(default_factory=list)
    active_order_blocks: list[OrderBlock] = field(default_factory=list)
    active_breaker_blocks: list[OrderBlock] = field(default_factory=list)
    liquidity_grabs: list[LiquidityGrab] = field(default_factory=list)
    market_bias: str = "neutral"  # "bullish", "bearish", "neutral"
    smc_score: float = 0.0  # [-1, 1] composite
    reasons: list[str] = field(default_factory=list)


class SmartMoneyAnalyzer:
    """Analyzes price-action for SMC/ICT concepts on a given DataFrame.

    The DataFrame must have columns: open, high, low, close, volume,
    plus swing_high / swing_low (from TechnicalIndicators).
    """

    def __init__(
        self,
        fvg_min_pct: float = 0.005,
        ob_lookback: int = 50,
        liquidity_lookback: int = 20,
        pivot_bars: int = 5,
    ) -> None:
        self._fvg_min_pct = fvg_min_pct
        self._ob_lookback = ob_lookback
        self._liq_lookback = liquidity_lookback
        self._pivot_bars = pivot_bars
        self._prev_swing_highs: list[float] = []
        self._prev_swing_lows: list[float] = []
        self._last_structure_dir: str = "neutral"

    # ── Swing Structure ───────────────────────────────────────────────────
    def _extract_swings(self, df: pd.DataFrame) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
        """Return lists of (index, price) for swing highs and lows."""
        swing_highs: list[tuple[int, float]] = []
        swing_lows: list[tuple[int, float]] = []
        n = len(df)
        pb = self._pivot_bars
        for i in range(pb, n - pb):
            h = df["high"].iloc[i]
            l = df["low"].iloc[i]
            if all(h >= df["high"].iloc[i - pb:i].max() and h >= df["high"].iloc[i + 1:i + pb + 1].max()
                   for _ in [0]) if n > i + pb else False:
                swing_highs.append((i, h))
            if all(l <= df["low"].iloc[i - pb:i].min() and l <= df["low"].iloc[i + 1:i + pb + 1].min()
                   for _ in [0]) if n > i + pb else False:
                swing_lows.append((i, l))
        # Also get from pre-computed columns if available
        if "swing_high" in df.columns:
            for i, v in df["swing_high"].dropna().items():
                idx = df.index.get_loc(i) if not isinstance(i, int) else i
                if (idx, v) not in swing_highs:
                    swing_highs.append((idx, v))
        if "swing_low" in df.columns:
            for i, v in df["swing_low"].dropna().items():
                idx = df.index.get_loc(i) if not isinstance(i, int) else i
                if (idx, v) not in swing_lows:
                    swing_lows.append((idx, v))
        swing_highs.sort(key=lambda x: x[0])
        swing_lows.sort(key=lambda x: x[0])
        return swing_highs, swing_lows

    # ── Break of Structure / Change of Character ──────────────────────────
    def _detect_structure_breaks(
        self,
        df: pd.DataFrame,
        swing_highs: list[tuple[int, float]],
        swing_lows: list[tuple[int, float]],
    ) -> tuple[list[StructureBreak], list[StructureBreak]]:
        """Detect BOS and CHoCH events from recent swing structure."""
        bos_list: list[StructureBreak] = []
        choch_list: list[StructureBreak] = []
        n = len(df)
        if n < 3 or len(swing_highs) < 2 or len(swing_lows) < 2:
            return bos_list, choch_list

        last_idx = n - 1
        close = df["close"].iloc[-1]
        high = df["high"].iloc[-1]
        low = df["low"].iloc[-1]

        # Last two swing highs and lows
        sh1_idx, sh1 = swing_highs[-1]
        sh2_idx, sh2 = swing_highs[-2] if len(swing_highs) >= 2 else swing_highs[-1]
        sl1_idx, sl1 = swing_lows[-1]
        sl2_idx, sl2 = swing_lows[-2] if len(swing_lows) >= 2 else swing_lows[-1]

        # BOS bullish: price breaks above most recent swing high
        if high > sh1 and sh1_idx < last_idx:
            # Is this a continuation (BOS) or reversal (CHoCH)?
            if self._last_structure_dir in ("bullish", "neutral"):
                bos_list.append(StructureBreak(last_idx, "BOS", "bullish", sh1))
            else:
                choch_list.append(StructureBreak(last_idx, "CHoCH", "bullish", sh1))
            self._last_structure_dir = "bullish"

        # BOS bearish: price breaks below most recent swing low
        if low < sl1 and sl1_idx < last_idx:
            if self._last_structure_dir in ("bearish", "neutral"):
                bos_list.append(StructureBreak(last_idx, "BOS", "bearish", sl1))
            else:
                choch_list.append(StructureBreak(last_idx, "CHoCH", "bearish", sl1))
            self._last_structure_dir = "bearish"

        return bos_list, choch_list

    # ── Fair Value Gaps ───────────────────────────────────────────────────
    def _detect_fvgs(self, df: pd.DataFrame) -> list[FairValueGap]:
        """Detect Fair Value Gaps in last N candles."""
        fvgs: list[FairValueGap] = []
        n = len(df)
        lookback = min(self._ob_lookback, n - 2)
        for i in range(max(2, n - lookback), n):
            # Bullish FVG: candle[i-2].high < candle[i].low (gap up)
            prev2_high = df["high"].iloc[i - 2]
            curr_low = df["low"].iloc[i]
            if curr_low > prev2_high:
                gap_size = (curr_low - prev2_high) / df["close"].iloc[i]
                if gap_size >= self._fvg_min_pct:
                    fvg = FairValueGap(i, "bullish", curr_low, prev2_high, gap_size)
                    # Check if filled by subsequent price action
                    if i < n - 1:
                        subsequent_low = df["low"].iloc[i + 1:].min() if i + 1 < n else curr_low
                        if subsequent_low <= prev2_high:
                            fvg.filled = True
                    fvgs.append(fvg)

            # Bearish FVG: candle[i-2].low > candle[i].high (gap down)
            prev2_low = df["low"].iloc[i - 2]
            curr_high = df["high"].iloc[i]
            if prev2_low > curr_high:
                gap_size = (prev2_low - curr_high) / df["close"].iloc[i]
                if gap_size >= self._fvg_min_pct:
                    fvg = FairValueGap(i, "bearish", prev2_low, curr_high, gap_size)
                    if i < n - 1:
                        subsequent_high = df["high"].iloc[i + 1:].max() if i + 1 < n else curr_high
                        if subsequent_high >= prev2_low:
                            fvg.filled = True
                    fvgs.append(fvg)

        return [f for f in fvgs if not f.filled]

    # ── Order Blocks ──────────────────────────────────────────────────────
    def _detect_order_blocks(self, df: pd.DataFrame) -> list[OrderBlock]:
        """Detect Order Blocks: last opposing candle before an impulse move."""
        obs: list[OrderBlock] = []
        n = len(df)
        lookback = min(self._ob_lookback, n - 3)
        for i in range(max(1, n - lookback), n - 1):
            curr_close = df["close"].iloc[i]
            curr_open = df["open"].iloc[i]
            next_close = df["close"].iloc[i + 1]
            next_open = df["open"].iloc[i + 1]

            body = abs(curr_close - curr_open)
            next_body = abs(next_close - next_open)
            if body == 0 or next_body == 0:
                continue

            # Bullish OB: bearish candle followed by bullish impulse (>2x body)
            if curr_close < curr_open and next_close > next_open and next_body > 2 * body:
                ob = OrderBlock(i, "bullish", df["high"].iloc[i], df["low"].iloc[i])
                # Check mitigation: if price later came into the OB zone
                mid = (ob.high + ob.low) / 2
                if i + 2 < n:
                    subsequent_low = df["low"].iloc[i + 2:].min()
                    if subsequent_low <= mid:
                        ob.mitigated = True
                obs.append(ob)

            # Bearish OB: bullish candle followed by bearish impulse
            if curr_close > curr_open and next_close < next_open and next_body > 2 * body:
                ob = OrderBlock(i, "bearish", df["high"].iloc[i], df["low"].iloc[i])
                mid = (ob.high + ob.low) / 2
                if i + 2 < n:
                    subsequent_high = df["high"].iloc[i + 2:].max()
                    if subsequent_high >= mid:
                        ob.mitigated = True
                obs.append(ob)

        return obs

    # ── Breaker Blocks ────────────────────────────────────────────────────
    def _detect_breaker_blocks(self, order_blocks: list[OrderBlock]) -> list[OrderBlock]:
        """Breaker = mitigated Order Block that becomes S/R."""
        return [ob for ob in order_blocks if ob.mitigated]

    # ── Liquidity Grabs ───────────────────────────────────────────────────
    def _detect_liquidity_grabs(
        self, df: pd.DataFrame,
        swing_highs: list[tuple[int, float]],
        swing_lows: list[tuple[int, float]],
    ) -> list[LiquidityGrab]:
        """Detect liquidity sweeps: price pierces swing level then reverses."""
        grabs: list[LiquidityGrab] = []
        n = len(df)
        if n < 3:
            return grabs

        last = df.iloc[-1]
        prev = df.iloc[-2]

        # Check if current bar swept recent swing highs then closed below
        for _, sh_price in swing_highs[-self._liq_lookback:]:
            if last["high"] > sh_price and last["close"] < sh_price:
                grabs.append(LiquidityGrab(n - 1, "bearish", sh_price, last["close"]))

        # Check if current bar swept recent swing lows then closed above
        for _, sl_price in swing_lows[-self._liq_lookback:]:
            if last["low"] < sl_price and last["close"] > sl_price:
                grabs.append(LiquidityGrab(n - 1, "bullish", sl_price, last["close"]))

        return grabs

    # ── Composite Analysis ────────────────────────────────────────────────
    def analyze(self, df: pd.DataFrame) -> SMCState:
        """Run full SMC analysis and return composite state."""
        if df is None or len(df) < 20:
            return SMCState()

        state = SMCState()
        swing_highs, swing_lows = self._extract_swings(df)

        # Structure breaks
        bos, choch = self._detect_structure_breaks(df, swing_highs, swing_lows)
        state.bos_events = bos
        state.choch_events = choch

        # FVGs
        state.active_fvgs = self._detect_fvgs(df)

        # Order Blocks
        all_obs = self._detect_order_blocks(df)
        state.active_order_blocks = [ob for ob in all_obs if not ob.mitigated]
        state.active_breaker_blocks = self._detect_breaker_blocks(all_obs)

        # Liquidity Grabs
        state.liquidity_grabs = self._detect_liquidity_grabs(df, swing_highs, swing_lows)

        # ── Score computation ─────────────────────────────────────────────
        score = 0.0
        reasons: list[str] = []
        close = df["close"].iloc[-1]

        # BOS / CHoCH
        for b in bos:
            if b.direction == "bullish":
                score += 0.3
                reasons.append("BOS_bullish")
            else:
                score -= 0.3
                reasons.append("BOS_bearish")

        for c in choch:
            if c.direction == "bullish":
                score += 0.4
                reasons.append("CHoCH_bullish")
            else:
                score -= 0.4
                reasons.append("CHoCH_bearish")

        # FVG proximity: price near an unfilled FVG
        for fvg in state.active_fvgs[-3:]:  # most recent 3
            if fvg.direction == "bullish":
                # Price approaching bullish FVG (from above) is a buy zone
                if close <= fvg.top * 1.005:
                    score += 0.2
                    reasons.append("FVG_bullish_zone")
            else:
                if close >= fvg.bottom * 0.995:
                    score -= 0.2
                    reasons.append("FVG_bearish_zone")

        # Active Order Blocks — price near unmitigated OB
        for ob in state.active_order_blocks[-3:]:
            if ob.direction == "bullish" and ob.low <= close <= ob.high * 1.01:
                score += 0.25
                reasons.append("OB_bullish_support")
            elif ob.direction == "bearish" and ob.low * 0.99 <= close <= ob.high:
                score -= 0.25
                reasons.append("OB_bearish_resistance")

        # Liquidity grabs
        for lg in state.liquidity_grabs:
            if lg.direction == "bullish":
                score += 0.35
                reasons.append("liquidity_grab_bullish")
            else:
                score -= 0.35
                reasons.append("liquidity_grab_bearish")

        state.smc_score = float(np.clip(score, -1.0, 1.0))
        state.reasons = reasons

        if score > 0.1:
            state.market_bias = "bullish"
        elif score < -0.1:
            state.market_bias = "bearish"
        else:
            state.market_bias = "neutral"

        return state

    def get_nearest_liquidity_zone(
        self, df: pd.DataFrame, direction: str, entry_price: float,
    ) -> float | None:
        """Find the nearest unfilled FVG or Order Block as a TP target.

        Returns a price level or None if no zone found.
        """
        if df is None or len(df) < 20:
            return None

        targets: list[float] = []
        # Unfilled FVGs
        fvgs = self._detect_fvgs(df)
        for fvg in fvgs:
            if direction == "long" and fvg.direction == "bearish" and fvg.bottom > entry_price:
                targets.append(fvg.bottom)
            elif direction == "short" and fvg.direction == "bullish" and fvg.top < entry_price:
                targets.append(fvg.top)

        # Unmitigated Order Blocks
        obs = self._detect_order_blocks(df)
        for ob in obs:
            if not ob.mitigated:
                if direction == "long" and ob.direction == "bearish" and ob.low > entry_price:
                    targets.append(ob.low)
                elif direction == "short" and ob.direction == "bullish" and ob.high < entry_price:
                    targets.append(ob.high)

        if not targets:
            return None
        # Return the nearest target
        return min(targets, key=lambda t: abs(t - entry_price))
