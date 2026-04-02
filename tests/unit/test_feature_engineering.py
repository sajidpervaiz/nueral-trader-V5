from __future__ import annotations

import numpy as np
import pandas as pd

from engine.feature_engineering import FeatureEngineering


def _build_ohlcv(rows: int = 120) -> pd.DataFrame:
    idx = np.arange(rows)
    close = 50000 + np.sin(idx / 8.0) * 500 + idx * 2
    high = close + 50
    low = close - 50
    open_ = close - 10
    volume = 100 + (idx % 10) * 5
    return pd.DataFrame({
        "timestamp": idx,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    })


def test_generate_technical_features_includes_stoch_and_bb_columns() -> None:
    fe = FeatureEngineering()
    df = _build_ohlcv(150)

    out = fe.generate_technical_features(df)

    for col in ["stoch_k", "stoch_d", "bb_width", "bb_pct", "atr", "rsi"]:
        assert col in out.columns

    assert out["stoch_k"].dropna().shape[0] > 0
    assert out["stoch_d"].dropna().shape[0] > 0
    assert np.isfinite(out["bb_width"].fillna(0.0)).all()
    assert np.isfinite(out["bb_pct"].fillna(0.0)).all()


def test_create_feature_set_populates_sections() -> None:
    fe = FeatureEngineering()
    df = fe.generate_technical_features(_build_ohlcv(120))

    orderbook_data = [{
        "bids": [[50000.0, 5.0], [49990.0, 5.0]],
        "asks": [[50010.0, 4.0], [50020.0, 4.0]],
    }]
    trade_data = [{"price": 50005.0, "quantity": 0.4, "side": "buy"} for _ in range(20)]
    macro_signals = [{"risk_level": "MEDIUM", "bias": "bullish", "importance": 0.7} for _ in range(5)]

    eth_df = _build_ohlcv(120)
    cross_prices = {"BTC/USDT": df, "ETH/USDT": eth_df}

    fs = fe.create_feature_set(
        df=df,
        orderbook_data=orderbook_data,
        trade_data=trade_data,
        macro_signals=macro_signals,
        cross_asset_prices=cross_prices,
        symbols=["BTC/USDT", "ETH/USDT"],
        target_symbol="BTC/USDT",
    )

    assert fs.metadata["has_technical"] is True
    assert fs.metadata["has_microstructure"] is True
    assert fs.metadata["has_macro"] is True
    assert fs.metadata["has_cross_asset"] is True
    assert fs.metadata["feature_count"] > 0
