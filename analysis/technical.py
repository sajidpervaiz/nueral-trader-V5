from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

try:
    import talib as ta
    _TALIB = True
except ImportError:
    _TALIB = False
    try:
        import pandas_ta as pta
        _PANDAS_TA = True
    except ImportError:
        _PANDAS_TA = False


def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period).mean()


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    if _TALIB:
        return pd.Series(ta.RSI(series.values, timeperiod=period), index=series.index)
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _macd(
    series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
) -> tuple[pd.Series, pd.Series, pd.Series]:
    if _TALIB:
        m, s, h = ta.MACD(series.values, fastperiod=fast, slowperiod=slow, signalperiod=signal)
        idx = series.index
        return pd.Series(m, index=idx), pd.Series(s, index=idx), pd.Series(h, index=idx)
    ema_fast = _ema(series, fast)
    ema_slow = _ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def _bollinger_bands(
    series: pd.Series, period: int = 20, num_std: float = 2.0
) -> tuple[pd.Series, pd.Series, pd.Series]:
    if _TALIB:
        upper, middle, lower = ta.BBANDS(series.values, timeperiod=period, nbdevup=num_std, nbdevdn=num_std)
        idx = series.index
        return pd.Series(upper, index=idx), pd.Series(middle, index=idx), pd.Series(lower, index=idx)
    mid = _sma(series, period)
    std = series.rolling(window=period).std()
    return mid + num_std * std, mid, mid - num_std * std


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    if _TALIB:
        return pd.Series(ta.ATR(high.values, low.values, close.values, timeperiod=period), index=close.index)
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(com=period - 1, adjust=False).mean()


def _stochastic(
    high: pd.Series, low: pd.Series, close: pd.Series,
    k_period: int = 14, d_period: int = 3,
) -> tuple[pd.Series, pd.Series]:
    if _TALIB:
        k, d = ta.STOCH(high.values, low.values, close.values, fastk_period=k_period, slowd_period=d_period)
        return pd.Series(k, index=close.index), pd.Series(d, index=close.index)
    lowest_low = low.rolling(window=k_period).min()
    highest_high = high.rolling(window=k_period).max()
    denom = highest_high - lowest_low
    k = 100 * (close - lowest_low) / denom.replace(0, np.nan)
    d = _sma(k, d_period)
    return k, d


def _vwap(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
    typical = (high + low + close) / 3
    cum_vol = volume.cumsum()
    cum_tp_vol = (typical * volume).cumsum()
    return cum_tp_vol / cum_vol.replace(0, np.nan)


class TechnicalIndicators:
    def __init__(self) -> None:
        self._cache: dict[str, Any] = {}

    def compute_all(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df.get("volume", pd.Series(1.0, index=df.index))

        df["rsi_14"] = _rsi(close, 14)
        df["rsi_21"] = _rsi(close, 21)

        df["ema_9"] = _ema(close, 9)
        df["ema_21"] = _ema(close, 21)
        df["ema_50"] = _ema(close, 50)
        df["ema_200"] = _ema(close, 200)
        df["sma_20"] = _sma(close, 20)
        df["sma_50"] = _sma(close, 50)

        macd_line, macd_signal, macd_hist = _macd(close)
        df["macd"] = macd_line
        df["macd_signal"] = macd_signal
        df["macd_hist"] = macd_hist

        bb_upper, bb_mid, bb_lower = _bollinger_bands(close)
        df["bb_upper"] = bb_upper
        df["bb_mid"] = bb_mid
        df["bb_lower"] = bb_lower
        df["bb_width"] = (bb_upper - bb_lower) / bb_mid.replace(0, np.nan)
        df["bb_pct"] = (close - bb_lower) / (bb_upper - bb_lower).replace(0, np.nan)

        df["atr_14"] = _atr(high, low, close, 14)
        df["atr_pct"] = df["atr_14"] / close.replace(0, np.nan)

        stoch_k, stoch_d = _stochastic(high, low, close)
        df["stoch_k"] = stoch_k
        df["stoch_d"] = stoch_d

        df["vwap"] = _vwap(high, low, close, volume)

        df["returns_1"] = close.pct_change(1)
        df["returns_5"] = close.pct_change(5)
        df["returns_20"] = close.pct_change(20)

        df["vol_20"] = df["returns_1"].rolling(20).std() * (252 ** 0.5)

        df["trend_ema"] = np.where(df["ema_21"] > df["ema_50"], 1.0, -1.0)
        df["momentum_rsi"] = np.where(df["rsi_14"] > 50, 1.0, -1.0)

        return df.dropna(subset=["rsi_14", "ema_21", "macd"])
