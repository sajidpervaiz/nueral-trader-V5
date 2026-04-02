from __future__ import annotations

import numpy as np
import pandas as pd

from engine.model_trainer import ModelTrainer


def _dataset(rows: int = 260) -> pd.DataFrame:
    idx = np.arange(rows)
    close = 100 + np.sin(idx / 9.0) + idx * 0.03
    volume = 1000 + (idx % 13) * 10

    return pd.DataFrame({
        "timestamp": idx,
        "open": close - 0.2,
        "high": close + 0.3,
        "low": close - 0.4,
        "close": close,
        "volume": volume,
        "feat_momentum": np.sin(idx / 5.0),
        "feat_vol": np.cos(idx / 7.0),
    })


def test_prepare_training_data_returns_consistent_shapes() -> None:
    trainer = ModelTrainer(model_type="lightgbm")
    df = _dataset(280)

    X, y, feature_names = trainer.prepare_training_data(
        df,
        target_col="returns",
        feature_cols=["feat_momentum", "feat_vol"],
    )

    assert X.shape[0] == y.shape[0]
    assert X.shape[1] == 2
    assert feature_names == ["feat_momentum", "feat_vol"]


def test_train_model_lightgbm_smoke() -> None:
    trainer = ModelTrainer(model_type="lightgbm")
    df = _dataset(320)

    trained = trainer.train_model(
        df,
        target_col="returns",
        feature_cols=["feat_momentum", "feat_vol"],
        validation_split=0.2,
        hyperparameters={"n_estimators": 20, "learning_rate": 0.1},
        perform_cv=False,
        model_id="tier1_lgbm_smoke",
    )

    assert trained.model is not None
    assert trained.metrics.accuracy >= 0.0
    assert "tier1_lgbm_smoke" in trainer.trained_models
