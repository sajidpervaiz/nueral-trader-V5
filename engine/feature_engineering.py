"""
Feature Engineering with technical, microstructure, cross-asset, and macro features.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional
from dataclasses import dataclass
import ta
from loguru import logger


@dataclass
class FeatureSet:
    timestamp: int
    technical_features: Dict[str, float]
    microstructure_features: Dict[str, float]
    macro_features: Dict[str, float]
    cross_asset_features: Dict[str, float]
    metadata: Dict


class FeatureEngineering:
    """
    Production-grade feature engineering with:
    - Technical indicators
    - Microstructure features
    - Cross-asset features
    - Macro features
    - Feature normalization
    """

    def __init__(self):
        self.feature_names: List[str] = []

    def generate_technical_features(
        self,
        df: pd.DataFrame,
        price_col: str = 'close',
        volume_col: str = 'volume',
    ) -> pd.DataFrame:
        """
        Generate technical indicators.

        Features: SMA, EMA, RSI, MACD, BB, ATR, etc.
        """
        df = df.copy()

        df['sma_7'] = ta.trend.sma_indicator(df[price_col], window=7)
        df['sma_20'] = ta.trend.sma_indicator(df[price_col], window=20)
        df['sma_50'] = ta.trend.sma_indicator(df[price_col], window=50)

        df['ema_12'] = ta.trend.ema_indicator(df[price_col], window=12)
        df['ema_26'] = ta.trend.ema_indicator(df[price_col], window=26)

        df['rsi'] = ta.momentum.rsi(df[price_col], window=14)

        macd = ta.trend.MACD(df[price_col])
        df['macd'] = macd.macd()
        df['macd_signal'] = macd.macd_signal()
        df['macd_diff'] = macd.macd_diff()

        bb = ta.volatility.BollingerBands(df[price_col])
        df['bb_high'] = bb.bollinger_hband()
        df['bb_mid'] = bb.bollinger_mavg()
        df['bb_low'] = bb.bollinger_lband()
        df['bb_width'] = (df['bb_high'] - df['bb_low']) / df['bb_mid']
        df['bb_pct'] = (df[price_col] - df['bb_low']) / (df['bb_high'] - df['bb_low'])

        df['atr'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], window=14)

        df['stoch_k'] = ta.momentum.stoch(df['high'], df['low'], df['close'])['STOCHk']
        df['stoch_d'] = ta.momentum.stoch(df['high'], df['low'], df['close'])['STOCHd']

        df['obv'] = ta.volume.on_balance_volume(df[price_col], df[volume_col])

        df['returns_1'] = df[price_col].pct_change(1)
        df['returns_5'] = df[price_col].pct_change(5)
        df['returns_20'] = df[price_col].pct_change(20)

        df['volatility_10'] = df['returns_1'].rolling(10).std()
        df['volatility_30'] = df['returns_1'].rolling(30).std()

        return df

    def generate_microstructure_features(
        self,
        orderbook_data: List[Dict],
        trade_data: List[Dict],
    ) -> Dict[str, float]:
        """
        Generate microstructure features.

        Features: Order flow imbalance, spread, depth, volume profile
        """
        features = {}

        if orderbook_data:
            latest = orderbook_data[-1]

            bids = latest.get('bids', [])
            asks = latest.get('asks', [])

            if bids and asks:
                mid_price = (bids[0][0] + asks[0][0]) / 2
                spread = asks[0][0] - bids[0][0]
                spread_pct = (spread / mid_price) * 100

                features['spread'] = spread
                features['spread_pct'] = spread_pct
                features['mid_price'] = mid_price

                bid_volume = sum(b[1] for b in bids[:5])
                ask_volume = sum(a[1] for a in asks[:5])

                ofi = (bid_volume - ask_volume) / (bid_volume + ask_volume) if (bid_volume + ask_volume) > 0 else 0
                features['order_flow_imbalance'] = ofi

                depth_imbalance = len(bids) / (len(bids) + len(asks)) if (len(bids) + len(asks)) > 0 else 0.5
                features['depth_imbalance'] = depth_imbalance

        if trade_data:
            recent_trades = trade_data[-100:] if len(trade_data) >= 100 else trade_data

            trade_sizes = [t.get('quantity', 0) for t in recent_trades]
            trade_values = [t.get('price', 0) * t.get('quantity', 0) for t in recent_trades]

            if trade_values:
                features['avg_trade_size'] = np.mean(trade_sizes)
                features['trade_volume_100'] = sum(trade_values)

                buy_volume = sum(t.get('quantity', 0) for t in recent_trades if t.get('side') == 'buy')
                sell_volume = sum(t.get('quantity', 0) for t in recent_trades if t.get('side') == 'sell')

                buy_ratio = buy_volume / (buy_volume + sell_volume) if (buy_volume + sell_volume) > 0 else 0.5
                features['buy_ratio'] = buy_ratio

        return features

    def generate_macro_features(
        self,
        macro_signals: List[Dict],
    ) -> Dict[str, float]:
        """
        Generate macro features.

        Features: Risk level, regime confidence, event proximity
        """
        features = {}

        if not macro_signals:
            return features

        recent_signals = macro_signals[-50:] if len(macro_signals) >= 50 else macro_signals

        risk_scores = {
            "LOW": 0.25,
            "MEDIUM": 0.5,
            "HIGH": 0.75,
            "EXTREME": 1.0,
        }

        high_risk_count = sum(
            1 for s in recent_signals
            if risk_scores.get(s.get('risk_level', 'LOW'), 0.25) >= 0.75
        )

        features['macro_risk_score'] = high_risk_count / len(recent_signals) if recent_signals else 0

        bullish_count = sum(1 for s in recent_signals if s.get('bias') == 'bullish')
        bearish_count = sum(1 for s in recent_signals if s.get('bias') == 'bearish')

        total = len(recent_signals)
        features['bullish_bias'] = bullish_count / total if total > 0 else 0
        features['bearish_bias'] = bearish_count / total if total > 0 else 0

        avg_importance = sum(s.get('importance', 0) for s in recent_signals) / total if total > 0 else 0
        features['avg_importance'] = avg_importance

        return features

    def generate_cross_asset_features(
        self,
        prices: Dict[str, pd.DataFrame],
        symbols: List[str],
        target_symbol: str,
    ) -> Dict[str, float]:
        """
        Generate cross-asset features.

        Features: Correlation, beta, relative strength
        """
        features = {}

        if target_symbol not in prices or len(symbols) < 2:
            return features

        target_prices = prices[target_symbol]['close']

        for symbol in symbols:
            if symbol == target_symbol or symbol not in prices:
                continue

            symbol_prices = prices[symbol]['close']

            if len(target_prices) >= 30 and len(symbol_prices) >= 30:
                aligned_target = target_prices.tail(30)
                aligned_symbol = symbol_prices.tail(30)

                correlation = aligned_target.corr(aligned_symbol)
                features[f'correlation_{symbol}'] = correlation if not np.isnan(correlation) else 0

                returns_target = aligned_target.pct_change().dropna()
                returns_symbol = aligned_symbol.pct_change().dropna()

                if len(returns_target) == len(returns_symbol) and len(returns_target) > 0:
                    covariance = np.cov(returns_target, returns_symbol)[0, 1]
                    var_target = np.var(returns_target)

                    if var_target > 0:
                        beta = covariance / var_target
                        features[f'beta_{symbol}'] = beta if not np.isnan(beta) else 1.0

                relative_strength = (aligned_target.iloc[-1] / aligned_target.iloc[0]) / \
                                (aligned_symbol.iloc[-1] / aligned_symbol.iloc[0]) if aligned_symbol.iloc[0] > 0 else 1.0
                features[f'relative_strength_{symbol}'] = relative_strength

        return features

    def create_feature_set(
        self,
        df: pd.DataFrame,
        orderbook_data: Optional[List[Dict]] = None,
        trade_data: Optional[List[Dict]] = None,
        macro_signals: Optional[List[Dict]] = None,
        cross_asset_prices: Optional[Dict[str, pd.DataFrame]] = None,
        symbols: Optional[List[str]] = None,
        target_symbol: str = 'BTC/USDT',
    ) -> FeatureSet:
        """
        Create complete feature set for prediction.
        """
        latest_row = df.iloc[-1]

        technical_features = {}
        for col in df.columns:
            if col in ['open', 'high', 'low', 'close', 'volume']:
                continue
            technical_features[col] = latest_row[col]

        microstructure_features = {}
        if orderbook_data and trade_data:
            microstructure_features = self.generate_microstructure_features(
                orderbook_data,
                trade_data,
            )

        macro_features = {}
        if macro_signals:
            macro_features = self.generate_macro_features(macro_signals)

        cross_asset_features = {}
        if cross_asset_prices and symbols:
            cross_asset_features = self.generate_cross_asset_features(
                cross_asset_prices,
                symbols,
                target_symbol,
            )

        all_features = {**technical_features, **microstructure_features, **macro_features, **cross_asset_features}

        self.feature_names = list(all_features.keys())

        return FeatureSet(
            timestamp=int(latest_row.get('timestamp', pd.Timestamp.now().timestamp())),
            technical_features=technical_features,
            microstructure_features=microstructure_features,
            macro_features=macro_features,
            cross_asset_features=cross_asset_features,
            metadata={
                'feature_count': len(all_features),
                'has_technical': len(technical_features) > 0,
                'has_microstructure': len(microstructure_features) > 0,
                'has_macro': len(macro_features) > 0,
                'has_cross_asset': len(cross_asset_features) > 0,
            },
        )

    def normalize_features(
        self,
        features: Dict[str, float],
        scaler: Optional[Dict[str, Dict]] = None,
    ) -> Dict[str, float]:
        """Normalize features using min-max scaling."""
        if not scaler:
            return features

        normalized = {}
        for key, value in features.items():
            if key in scaler:
                scaler_info = scaler[key]
                if scaler_info['max'] != scaler_info['min']:
                    normalized_value = (value - scaler_info['min']) / (scaler_info['max'] - scaler_info['min'])
                else:
                    normalized_value = 0.0
            else:
                normalized_value = value

            normalized[key] = normalized_value

        return normalized

    def get_feature_names(self) -> List[str]:
        """Get list of feature names."""
        return self.feature_names
