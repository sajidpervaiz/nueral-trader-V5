#!/usr/bin/env python3
"""
Model training script with walk-forward validation, trading metrics,
feature drift detection, and model versioning.

Usage:
    python scripts/train_model.py                          # Train from SQLite data
    python scripts/train_model.py --data data/candles.csv  # Train from CSV
    python scripts/train_model.py --wfo-splits 8           # Walk-forward with 8 splits
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.model_trainer import ModelTrainer, ModelMetrics
from research.utils.feature_engineering import engineer_features

try:
    import joblib
except ImportError:
    joblib = None


# ---------------------------------------------------------------------------
# Trading-specific metrics
# ---------------------------------------------------------------------------

def compute_trading_metrics(y_true: np.ndarray, y_pred_proba: np.ndarray, threshold: float = 0.5) -> dict:
    """Compute Sharpe, Calmar, win rate, and profit factor from predictions."""
    y_pred = (y_pred_proba >= threshold).astype(int)
    # Simulate: go long when predicted up (1), flat otherwise
    # Use sign of (true - 0.5) as proxy returns
    returns = np.where(y_pred == 1, np.where(y_true == 1, 0.01, -0.01), 0.0)
    nonzero = returns[returns != 0]

    if len(nonzero) == 0:
        return {"sharpe": 0.0, "calmar": 0.0, "win_rate": 0.0, "profit_factor": 0.0, "total_trades": 0}

    wins = nonzero[nonzero > 0]
    losses = nonzero[nonzero < 0]
    win_rate = len(wins) / len(nonzero) if len(nonzero) > 0 else 0.0
    gross_profit = wins.sum() if len(wins) > 0 else 0.0
    gross_loss = abs(losses.sum()) if len(losses) > 0 else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Sharpe (annualized, assuming ~252 trading days)
    mean_ret = nonzero.mean()
    std_ret = nonzero.std()
    sharpe = (mean_ret / std_ret) * np.sqrt(252) if std_ret > 0 else 0.0

    # Calmar (annualized return / max drawdown)
    cumulative = np.cumsum(returns)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = running_max - cumulative
    max_dd = drawdowns.max() if len(drawdowns) > 0 else 0.0
    annualized_return = mean_ret * 252
    calmar = annualized_return / max_dd if max_dd > 0 else 0.0

    return {
        "sharpe": round(float(sharpe), 4),
        "calmar": round(float(calmar), 4),
        "win_rate": round(float(win_rate), 4),
        "profit_factor": round(float(profit_factor), 4),
        "total_trades": int(len(nonzero)),
        "max_drawdown": round(float(max_dd), 6),
    }


# ---------------------------------------------------------------------------
# Feature drift detection (PSI)
# ---------------------------------------------------------------------------

def population_stability_index(reference: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
    """Calculate PSI between two distributions. PSI > 0.2 indicates significant drift."""
    eps = 1e-6
    breakpoints = np.linspace(np.min(reference), np.max(reference), bins + 1)
    ref_pcts = np.histogram(reference, bins=breakpoints)[0] / len(reference) + eps
    cur_pcts = np.histogram(current, bins=breakpoints)[0] / len(current) + eps
    psi = np.sum((cur_pcts - ref_pcts) * np.log(cur_pcts / ref_pcts))
    return float(psi)


def check_feature_drift(X_train: np.ndarray, X_test: np.ndarray, feature_names: list[str], threshold: float = 0.2) -> list[dict]:
    """Check for feature drift between train and test sets."""
    drifted = []
    for i, name in enumerate(feature_names):
        if X_train.shape[0] < 20 or X_test.shape[0] < 20:
            continue
        psi = population_stability_index(X_train[:, i], X_test[:, i])
        if psi > threshold:
            drifted.append({"feature": name, "psi": round(psi, 4)})
    return drifted


# ---------------------------------------------------------------------------
# Walk-forward optimization
# ---------------------------------------------------------------------------

def walk_forward_train(
    df: pd.DataFrame,
    trainer: ModelTrainer,
    n_splits: int = 5,
    min_train_rows: int = 500,
    target_col: str = "returns",
    feature_cols: list[str] | None = None,
) -> dict:
    """Expanding-window walk-forward optimization with trading metrics per fold."""
    X, y, fnames = trainer.prepare_training_data(df, target_col, feature_cols)
    n = len(X)
    test_size = max(50, n // (n_splits + 1))

    fold_results = []
    for fold in range(n_splits):
        train_end = n - (n_splits - fold) * test_size
        test_start = train_end
        test_end = test_start + test_size

        if train_end < min_train_rows:
            continue

        X_train, y_train = X[:train_end], y[:train_end]
        X_test, y_test = X[test_start:test_end], y[test_start:test_end]

        # Train
        if trainer.model_type == "lightgbm":
            model = trainer.train_lightgbm(X_train, y_train)
        else:
            model = trainer.train_xgboost(X_train, y_train)

        # Predict
        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else y_pred.astype(float)

        # Metrics
        from engine.model_trainer import _accuracy, _precision, _recall, _f1
        classification = {
            "accuracy": _accuracy(y_test, y_pred),
            "precision": _precision(y_test, y_pred),
            "recall": _recall(y_test, y_pred),
            "f1": _f1(y_test, y_pred),
        }
        trading = compute_trading_metrics(y_test, y_proba)
        drift = check_feature_drift(X_train, X_test, fnames)

        fold_results.append({
            "fold": fold,
            "train_size": train_end,
            "test_size": test_end - test_start,
            "classification": classification,
            "trading": trading,
            "drifted_features": drift,
        })

        logger.info(
            "Fold {}: F1={:.3f} Sharpe={:.2f} WR={:.1%} PF={:.2f} drift={}",
            fold,
            classification["f1"],
            trading["sharpe"],
            trading["win_rate"],
            trading["profit_factor"],
            len(drift),
        )

    # Summary
    if fold_results:
        avg_sharpe = np.mean([f["trading"]["sharpe"] for f in fold_results])
        avg_f1 = np.mean([f["classification"]["f1"] for f in fold_results])
        avg_wr = np.mean([f["trading"]["win_rate"] for f in fold_results])
    else:
        avg_sharpe = avg_f1 = avg_wr = 0.0

    return {
        "folds": fold_results,
        "summary": {
            "avg_sharpe": round(float(avg_sharpe), 4),
            "avg_f1": round(float(avg_f1), 4),
            "avg_win_rate": round(float(avg_wr), 4),
            "n_folds": len(fold_results),
        },
    }


# ---------------------------------------------------------------------------
# Model versioning
# ---------------------------------------------------------------------------

def save_versioned_model(
    trainer: ModelTrainer,
    model_id: str,
    output_dir: Path,
    wfo_results: dict | None = None,
) -> dict:
    """Save model with SHA-256, metadata, and version tracking."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    model_filename = f"ml_signal_{timestamp}.joblib"
    model_path = output_dir / model_filename

    # Save model
    if not trainer.save_model(model_id, str(model_path)):
        return {"error": "save_model failed"}

    # Compute SHA-256
    sha256 = hashlib.sha256(model_path.read_bytes()).hexdigest()

    # Build metadata
    trained = trainer.trained_models[model_id]
    metadata = {
        "version": timestamp,
        "model_type": trained.model_type,
        "feature_count": len(trained.feature_names),
        "feature_names": trained.feature_names,
        "train_time_sec": round(trained.train_time, 2),
        "metrics": {
            "accuracy": trained.metrics.accuracy,
            "precision": trained.metrics.precision,
            "recall": trained.metrics.recall,
            "f1": trained.metrics.f1,
        },
        "sha256": sha256,
        "hyperparameters": trained.hyperparameters,
    }
    if wfo_results:
        metadata["walk_forward"] = wfo_results.get("summary", {})

    # Save metadata
    meta_path = output_dir / f"ml_signal_{timestamp}.meta.json"
    meta_path.write_text(json.dumps(metadata, indent=2))

    # Update symlink to latest
    latest_link = output_dir / "ml_signal.lgb"
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(model_filename)

    logger.info("Model saved: {} (SHA-256: {})", model_path.name, sha256[:16])
    return metadata


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_training_data(args: argparse.Namespace) -> pd.DataFrame:
    """Load data from CSV or SQLite."""
    if args.data and Path(args.data).exists():
        logger.info("Loading data from {}", args.data)
        df = pd.read_csv(args.data)
        if "timestamp" in df.columns:
            df = df.sort_values("timestamp").reset_index(drop=True)
        return df

    # Try SQLite store
    db_path = Path("data/neural_trader.db")
    if db_path.exists():
        logger.info("Loading data from SQLite: {}", db_path)
        from storage.sqlite_store import SQLiteStore
        store = SQLiteStore(str(db_path))
        candles = store.get_candles(
            exchange=args.exchange,
            symbol=args.symbol,
            timeframe=args.timeframe,
            limit=args.max_rows,
        )
        store.close()
        if candles:
            df = pd.DataFrame(candles)
            df = df.rename(columns={"time_ns": "timestamp"})
            df = df.sort_values("timestamp").reset_index(drop=True)
            logger.info("Loaded {} candles from SQLite", len(df))
            return df

    logger.warning("No data source available — generating synthetic data for testing")
    np.random.seed(42)
    n = args.max_rows
    close = 50000.0 + np.cumsum(np.random.randn(n) * 100)
    df = pd.DataFrame({
        "open": close + np.random.randn(n) * 50,
        "high": close + abs(np.random.randn(n) * 100),
        "low": close - abs(np.random.randn(n) * 100),
        "close": close,
        "volume": abs(np.random.randn(n) * 1000) + 100,
    })
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Train ML model for Neural Trader")
    parser.add_argument("--data", type=str, help="Path to CSV data file")
    parser.add_argument("--exchange", default="binance", help="Exchange for SQLite data")
    parser.add_argument("--symbol", default="BTC/USDT:USDT", help="Symbol for SQLite data")
    parser.add_argument("--timeframe", default="15m", help="Timeframe for SQLite data")
    parser.add_argument("--max-rows", type=int, default=10000, help="Max candles to load")
    parser.add_argument("--model-type", default="lightgbm", choices=["lightgbm", "xgboost"])
    parser.add_argument("--wfo-splits", type=int, default=5, help="Walk-forward splits")
    parser.add_argument("--tune", action="store_true", help="Run hyperparameter tuning")
    parser.add_argument("--output-dir", default="models", help="Model output directory")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  Neural Trader — Model Training Pipeline")
    logger.info("=" * 60)

    # 1. Load data
    df = load_training_data(args)
    logger.info("Raw data: {} rows, {} columns", len(df), len(df.columns))

    if len(df) < 200:
        logger.error("Insufficient data ({} rows, need 200+). Aborting.", len(df))
        sys.exit(1)

    # 2. Feature engineering
    logger.info("Engineering features...")
    df = engineer_features(df)
    logger.info("After feature engineering: {} rows, {} features", len(df), len(df.columns))

    # 3. Initialize trainer
    trainer = ModelTrainer(model_type=args.model_type)

    # 4. Optional hyperparameter tuning
    hyperparams = None
    if args.tune:
        logger.info("Running hyperparameter tuning...")
        X, y, fnames = trainer.prepare_training_data(df, "returns")
        hyperparams = trainer.hyperparameter_tuning(X, y, fnames)
        logger.info("Best hyperparameters: {}", hyperparams)

    # 5. Walk-forward validation
    logger.info("Running walk-forward optimization ({} splits)...", args.wfo_splits)
    wfo_results = walk_forward_train(
        df, trainer, n_splits=args.wfo_splits, target_col="returns",
    )
    summary = wfo_results["summary"]
    logger.info(
        "WFO Summary: Sharpe={} F1={} WinRate={} Folds={}",
        summary["avg_sharpe"], summary["avg_f1"], summary["avg_win_rate"], summary["n_folds"],
    )

    # 6. Quality gate
    if summary["avg_f1"] < 0.45:
        logger.warning("Model F1 ({}) below quality gate (0.45) — saving anyway but flagging", summary["avg_f1"])

    # 7. Final training on all data
    logger.info("Training final model on full dataset...")
    trained = trainer.train_model(
        df, target_col="returns", hyperparameters=hyperparams,
        perform_cv=False, model_id="production",
    )
    logger.info(
        "Final model: F1={:.3f} Precision={:.3f} Recall={:.3f} Time={:.1f}s",
        trained.metrics.f1, trained.metrics.precision, trained.metrics.recall, trained.train_time,
    )

    # 8. Save versioned model
    output_dir = Path(args.output_dir)
    meta = save_versioned_model(trainer, "production", output_dir, wfo_results)
    if "error" not in meta:
        logger.info("Set ML_MODEL_SHA256={} for integrity verification", meta["sha256"])

    logger.info("Training pipeline complete.")


if __name__ == "__main__":
    main()
