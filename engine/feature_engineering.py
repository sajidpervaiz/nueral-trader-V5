"""
Feature Engineering with technical, microstructure, cross-asset, and macro features.
"""

import numpy as np
import pandas as pd
import importlib
from typing import Dict, List, Optional
from dataclasses import dataclass
from loguru import logger

try:
    ta = importlib.import_module("ta")
except ImportError:  # pragma: no cover
    ta = None


@dataclass
class FeatureSet:
    timestamp: int
    technical_features: Dict[str, float]
    microstructure_features: Dict[str, float]
    macro_features: Dict[str, float]
    cross_asset_features: Dict[str, float]
    metadata: Dict


class FeatureEngineering:
    def __init__(self):
        self.feature_names: List[str] = []

    def generate_technical_features(
        self,
        df: pd.DataFrame,
        price_col: str = "close",
        volume_col: str = "volume",
    ) -> pd.DataFrame:
        df = df.copy()

        if ta is not None:
            df["sma_7"] = ta.trend.sma_indicator(df[price_col], window=7)
            df["sma_20"] = ta.trend.sma_indicator(df[price_col], window=20)
            df["sma_50"] = ta.trend.sma_indicator(df[price_col], window=50)

            df["ema_12"] = ta.trend.ema_indicator(df[price_col], window=12)
            df["ema_26"] = ta.trend.ema_indicator(df[price_col], window=26)

            df["rsi"] = ta.momentum.rsi(df[price_col], window=14)

            macd = ta.trend.MACD(df[price_col])
            df["macd"] = macd.macd()
            df["macd_signal"] = macd.macd_signal()
            df["macd_diff"] = macd.macd_diff()

            bb = ta.volatility.BollingerBands(df[price_col])
            df["bb_high"] = bb.bollinger_hband()
            df["bb_mid"] = bb.bollinger_mavg()
            df["bb_low"] = bb.bollinger_lband()

            df["atr"] = ta.volatility.average_true_range(
                df["high"], df["low"], df["close"], window=14
            )

            df["stoch_k"] = ta.momentum.stoch(df["high"], df["low"], df["close"])
            df["stoch_d"] = ta.momentum.stoch_signal(df["high"], df["low"], df["close"])

            df["obv"] = ta.volume.on_balance_volume(df[price_col], df[volume_col])
        else:
            logger.warning("ta package unavailable, using fallback indicator implementations")

            df["sma_7"] = df[price_col].rolling(7).mean()
            df["sma_20"] = df[price_col].rolling(20).mean()
            df["sma_50"] = df[price_col].rolling(50).mean()

            df["ema_12"] = df[price_col].ewm(span=12, adjust=False).mean()
            df["ema_26"] = df[price_col].ewm(span=26, adjust=False).mean()

            delta = df[price_col].diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs = gain / loss.replace(0, np.nan)
            df["rsi"] = (100 - (100 / (1 + rs))).fillna(50.0)

            df["macd"] = df["ema_12"] - df["ema_26"]
            df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
            df["macd_diff"] = df["macd"] - df["macd_signal"]

            df["bb_mid"] = df[price_col].rolling(20).mean()
            bb_std = df[price_col].rolling(20).std()
            df["bb_high"] = df["bb_mid"] + 2 * bb_std
            df["bb_low"] = df["bb_mid"] - 2 * bb_std

            tr_components = pd.concat(
                [
                    (df["high"] - df["low"]).abs(),
                    (df["high"] - df["close"].shift(1)).abs(),
                    (df["low"] - df["close"].shift(1)).abs(),
                ],
                axis=1,
            )
            true_range = tr_components.max(axis=1)
            df["atr"] = true_range.rolling(14).mean()

            lowest_low = df["low"].rolling(14).min()
            highest_high = df["high"].rolling(14).max()
            denom = (highest_high - lowest_low).replace(0, np.nan)
            df["stoch_k"] = ((df["close"] - lowest_low) / denom * 100).fillna(50.0)
            df["stoch_d"] = df["stoch_k"].rolling(3).mean().fillna(50.0)

            direction = np.sign(df[price_col].diff()).fillna(0.0)
            df["obv"] = (direction * df[volume_col]).cumsum()

        bb_range = (df["bb_high"] - df["bb_low"]).replace(0, np.nan)
        bb_mid = df["bb_mid"].replace(0, np.nan)
        df["bb_width"] = ((df["bb_high"] - df["bb_low"]) / bb_mid).fillna(0.0)
        df["bb_pct"] = ((df[price_col] - df["bb_low"]) / bb_range).fillna(0.0)

        df["returns_1"] = df[price_col].pct_change(1)
        df["returns_5"] = df[price_col].pct_change(5)
        df["returns_20"] = df[price_col].pct_change(20)
        df["volatility_10"] = df["returns_1"].rolling(10).std()
        df["volatility_30"] = df["returns_1"].rolling(30).std()

        return df

    def generate_microstructure_features(self, orderbook_data: List[Dict], trade_data: List[Dict]) -> Dict[str, float]:
        features: Dict[str, float] = {}

        if orderbook_data:
            latest = orderbook_data[-1]
            bids = latest.get("bids", [])
            asks = latest.get("asks", [])
            if bids and asks:
                mid_price = (bids[0][0] + asks[0][0]) / 2
                spread = asks[0][0] - bids[0][0]
                features["spread"] = spread
                features["spread_pct"] = (spread / mid_price) * 100 if mid_price else 0.0
                features["mid_price"] = mid_price

                bid_volume = sum(b[1] for b in bids[:5])
                ask_volume = sum(a[1] for a in asks[:5])
                denom = bid_volume + ask_volume
                features["order_flow_imbalance"] = (bid_volume - ask_volume) / denom if denom > 0 else 0.0
                features["depth_imbalance"] = len(bids) / (len(bids) + len(asks)) if (len(bids) + len(asks)) > 0 else 0.5

        if trade_data:
            recent_trades = trade_data[-100:] if len(trade_data) >= 100 else trade_data
            trade_sizes = [t.get("quantity", 0.0) for t in recent_trades]
            trade_values = [t.get("price", 0.0) * t.get("quantity", 0.0) for t in recent_trades]
            if trade_values:
                features["avg_trade_size"] = float(np.mean(trade_sizes))
                features["trade_volume_100"] = float(sum(trade_values))
                buy_volume = sum(t.get("quantity", 0.0) for t in recent_trades if t.get("side") == "buy")
                sell_volume = sum(t.get("quantity", 0.0) for t in recent_trades if t.get("side") == "sell")
                denom = buy_volume + sell_volume
                features["buy_ratio"] = buy_volume / denom if denom > 0 else 0.5

        return features

    def generate_macro_features(self, macro_signals: List[Dict]) -> Dict[str, float]:
        features: Dict[str, float] = {}
        if not macro_signals:
            return features

        recent = macro_signals[-50:] if len(macro_signals) >= 50 else macro_signals
        risk_scores = {"LOW": 0.25, "MEDIUM": 0.5, "HIGH": 0.75, "EXTREME": 1.0}

        high_risk_count = sum(1 for s in recent if risk_scores.get(s.get("risk_level", "LOW"), 0.25) >= 0.75)
        total = len(recent)

        features["macro_risk_score"] = high_risk_count / total if total > 0 else 0.0
        bullish = sum(1 for s in recent if s.get("bias") == "bullish")
        bearish = sum(1 for s in recent if s.get("bias") == "bearish")
        features["bullish_bias"] = bullish / total if total > 0 else 0.0
        features["bearish_bias"] = bearish / total if total > 0 else 0.0
        features["avg_importance"] = sum(float(s.get("importance", 0.0)) for s in recent) / total if total > 0 else 0.0

        return features

    def generate_cross_asset_features(
        self,
        prices: Dict[str, pd.DataFrame],
        symbols: List[str],
        target_symbol: str,
    ) -> Dict[str, float]:
        features: Dict[str, float] = {}
        if target_symbol not in prices or len(symbols) < 2:
            return features

        target_prices = prices[target_symbol]["close"]
        for symbol in symbols:
            if symbol == target_symbol or symbol not in prices:
                continue

            symbol_prices = prices[symbol]["close"]
            if len(target_prices) >= 30 and len(symbol_prices) >= 30:
                aligned_target = target_prices.tail(30)
                aligned_symbol = symbol_prices.tail(30)

                correlation = aligned_target.corr(aligned_symbol)
                features[f"correlation_{symbol}"] = float(0.0 if np.isnan(correlation) else correlation)

                returns_target = aligned_target.pct_change().dropna()
                returns_symbol = aligned_symbol.pct_change().dropna()
                if len(returns_target) == len(returns_symbol) and len(returns_target) > 0:
                    covariance = np.cov(returns_target, returns_symbol)[0, 1]
                    var_target = np.var(returns_target)
                    if var_target > 0:
                        beta = covariance / var_target
                        features[f"beta_{symbol}"] = float(1.0 if np.isnan(beta) else beta)

                if aligned_symbol.iloc[0] > 0 and aligned_target.iloc[0] > 0:
                    rs = (aligned_target.iloc[-1] / aligned_target.iloc[0]) / (aligned_symbol.iloc[-1] / aligned_symbol.iloc[0])
                    features[f"relative_strength_{symbol}"] = float(rs)

        return features

    def create_feature_set(
        self,
        df: pd.DataFrame,
        orderbook_data: Optional[List[Dict]] = None,
        trade_data: Optional[List[Dict]] = None,
        macro_signals: Optional[List[Dict]] = None,
        cross_asset_prices: Optional[Dict[str, pd.DataFrame]] = None,
        symbols: Optional[List[str]] = None,
        target_symbol: str = "BTC/USDT",
    ) -> FeatureSet:
        latest_row = df.iloc[-1]

        technical_features = {}
        for col in df.columns:
            if col in ["open", "high", "low", "close", "volume"]:
                continue
            val = latest_row[col]
            technical_features[col] = float(val) if pd.notna(val) else 0.0

        micro = self.generate_microstructure_features(orderbook_data or [], trade_data or [])
        macro = self.generate_macro_features(macro_signals or [])
        cross = self.generate_cross_asset_features(cross_asset_prices or {}, symbols or [], target_symbol)

        all_features = {**technical_features, **micro, **macro, **cross}
        self.feature_names = list(all_features.keys())

        return FeatureSet(
            timestamp=int(latest_row.get("timestamp", pd.Timestamp.now().timestamp())),
            technical_features=technical_features,
            microstructure_features=micro,
            macro_features=macro,
            cross_asset_features=cross,
            metadata={
                "feature_count": len(all_features),
                "has_technical": len(technical_features) > 0,
                "has_microstructure": len(micro) > 0,
                "has_macro": len(macro) > 0,
                "has_cross_asset": len(cross) > 0,
            },
        )

    def normalize_features(self, features: Dict[str, float], scaler: Optional[Dict[str, Dict]] = None) -> Dict[str, float]:
        if not scaler:
            return features

        normalized = {}
        for key, value in features.items():
            if key in scaler:
                scaler_info = scaler[key]
                if scaler_info["max"] != scaler_info["min"]:
                    normalized_value = (value - scaler_info["min"]) / (scaler_info["max"] - scaler_info["min"])
                else:
                    normalized_value = 0.0
            else:
                normalized_value = value
            normalized[key] = normalized_value

        return normalized

    def get_feature_names(self) -> List[str]:
        return self.feature_names
