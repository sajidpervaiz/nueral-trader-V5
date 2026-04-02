"""
Model Trainer with optional LightGBM/XGBoost and portable fallbacks.
"""

import time
import pickle
import itertools
import importlib
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from loguru import logger

try:
    joblib = importlib.import_module("joblib")
except ImportError:  # pragma: no cover
    joblib = None

try:
    lgb = importlib.import_module("lightgbm")
except ImportError:  # pragma: no cover
    lgb = None

try:
    xgb = importlib.import_module("xgboost")
except ImportError:  # pragma: no cover
    xgb = None


@dataclass
class ModelMetrics:
    accuracy: float
    precision: float
    recall: float
    f1: float
    auc: float
    feature_importance: Dict[str, float]


@dataclass
class TrainedModel:
    model_type: str
    model: object
    feature_names: List[str]
    metrics: ModelMetrics
    train_time: float
    hyperparameters: Dict


class _LinearProbClassifier:
    """Small, dependency-free probabilistic classifier."""

    def __init__(self) -> None:
        self.w: Optional[np.ndarray] = None
        self.b: float = 0.0

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        X = np.asarray(X, dtype=float)
        y = np.asarray(y, dtype=float).reshape(-1)
        if X.size == 0:
            self.w = np.zeros(1)
            self.b = 0.0
            return

        X_aug = np.hstack([X, np.ones((X.shape[0], 1))])
        coeffs, *_ = np.linalg.lstsq(X_aug, y, rcond=None)
        self.w = coeffs[:-1]
        self.b = float(coeffs[-1])

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        X = np.asarray(X, dtype=float)
        if self.w is None:
            p = np.full((X.shape[0],), 0.5)
        else:
            logits = X @ self.w + self.b
            p = 1.0 / (1.0 + np.exp(-logits))
            p = np.clip(p, 1e-6, 1 - 1e-6)
        return np.column_stack([1.0 - p, p])

    def predict(self, X: np.ndarray) -> np.ndarray:
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)

    @property
    def feature_importances_(self) -> np.ndarray:
        if self.w is None:
            return np.array([])
        return np.abs(self.w)


def _accuracy(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    if y_true.size == 0:
        return 0.0
    return float(np.mean(y_true == y_pred))


def _precision(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    tp = np.sum((y_true == 1) & (y_pred == 1))
    fp = np.sum((y_true == 0) & (y_pred == 1))
    return float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0


def _recall(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    tp = np.sum((y_true == 1) & (y_pred == 1))
    fn = np.sum((y_true == 1) & (y_pred == 0))
    return float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0


def _f1(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    p = _precision(y_true, y_pred)
    r = _recall(y_true, y_pred)
    return float(2 * p * r / (p + r)) if (p + r) > 0 else 0.0


class ModelTrainer:
    def __init__(self, model_type: str = "lightgbm", random_state: int = 42):
        self.model_type = model_type
        self.random_state = random_state
        self.trained_models: Dict[str, TrainedModel] = {}

    def prepare_training_data(
        self,
        df: pd.DataFrame,
        target_col: str,
        feature_cols: Optional[List[str]] = None,
        lookahead: int = 1,
    ) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        df = df.copy()

        if target_col == "returns":
            df["target_returns"] = df["close"].pct_change(lookahead).shift(-lookahead)
            df["target_direction"] = (df["target_returns"] > 0).astype(int)
            target = df["target_direction"]
        else:
            target = df[target_col].shift(-lookahead)

        df = df.dropna()

        if feature_cols is None:
            feature_cols = [
                c for c in df.columns
                if c not in ["open", "high", "low", "close", "volume", "target_returns", "target_direction", "timestamp"]
            ]

        X = df[feature_cols].values
        y = target.loc[df.index].values

        logger.info(f"Prepared training data: X shape {X.shape}, y shape {y.shape}")
        return X, y, feature_cols

    def train_lightgbm(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
        hyperparameters: Optional[Dict] = None,
    ) -> object:
        if lgb is not None:
            params = {
                "objective": "binary",
                "metric": "binary_logloss",
                "boosting_type": "gbdt",
                "num_leaves": 31,
                "learning_rate": 0.05,
                "feature_fraction": 0.9,
                "bagging_fraction": 0.8,
                "bagging_freq": 5,
                "verbose": -1,
                "random_state": self.random_state,
                "n_estimators": 100,
            }
            if hyperparameters:
                params.update(hyperparameters)
            model = lgb.LGBMClassifier(**params)
            model.fit(X_train, y_train)
            return model

        logger.warning("lightgbm unavailable, using linear-probability fallback")
        model = _LinearProbClassifier()
        model.fit(X_train, y_train)
        return model

    def train_xgboost(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
        hyperparameters: Optional[Dict] = None,
    ) -> object:
        if xgb is not None:
            params = {
                "objective": "binary:logistic",
                "eval_metric": "logloss",
                "learning_rate": 0.05,
                "max_depth": 6,
                "min_child_weight": 1,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "random_state": self.random_state,
                "n_estimators": 100,
            }
            if hyperparameters:
                params.update(hyperparameters)
            model = xgb.XGBClassifier(**params)
            model.fit(X_train, y_train)
            return model

        logger.warning("xgboost unavailable, using linear-probability fallback")
        model = _LinearProbClassifier()
        model.fit(X_train, y_train)
        return model

    def time_series_cv(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: List[str],
        n_splits: int = 5,
        test_size: int = 100,
    ) -> Dict[str, List[float]]:
        _ = feature_names
        n = len(X)
        if n < (n_splits + 1) * test_size:
            return {"accuracy": [], "precision": [], "recall": [], "f1": []}

        metrics = {"accuracy": [], "precision": [], "recall": [], "f1": []}
        for fold in range(n_splits):
            train_end = n - (n_splits - fold) * test_size
            val_start = train_end
            val_end = val_start + test_size

            X_train, y_train = X[:train_end], y[:train_end]
            X_val, y_val = X[val_start:val_end], y[val_start:val_end]

            model = self.train_lightgbm(X_train, y_train) if self.model_type == "lightgbm" else self.train_xgboost(X_train, y_train)
            y_pred = model.predict(X_val)

            metrics["accuracy"].append(_accuracy(y_val, y_pred))
            metrics["precision"].append(_precision(y_val, y_pred))
            metrics["recall"].append(_recall(y_val, y_pred))
            metrics["f1"].append(_f1(y_val, y_pred))

        return metrics

    def hyperparameter_tuning(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: List[str],
        param_grid: Optional[Dict] = None,
        cv_folds: int = 3,
    ) -> Dict[str, any]:
        _ = feature_names

        default_grid = {
            "n_estimators": [50, 100, 150],
            "learning_rate": [0.01, 0.05, 0.1],
        }
        if self.model_type == "xgboost":
            default_grid.update({"max_depth": [4, 6]})
        elif self.model_type == "lightgbm":
            default_grid.update({"num_leaves": [15, 31]})

        grid = param_grid or default_grid
        grid_keys = list(grid.keys())
        grid_values = [v if isinstance(v, list) else [v] for v in grid.values()]

        best_params: Dict[str, any] = {}
        best_score = float("-inf")

        for combo in itertools.product(*grid_values):
            candidate = {k: combo[i] for i, k in enumerate(grid_keys)}
            scores = self.time_series_cv(
                X,
                y,
                feature_names=[],
                n_splits=max(2, cv_folds),
                test_size=max(20, min(100, len(X) // (cv_folds + 2))) if len(X) > 0 else 20,
            )
            fold_f1 = scores.get("f1", [])
            mean_f1 = float(np.mean(fold_f1)) if fold_f1 else float("-inf")

            # Re-train quickly with candidate to keep behavior aligned with requested grid.
            # If model-specific params are invalid they will be ignored by the selected backend or raise.
            try:
                split_idx = int(len(X) * 0.8)
                X_train, X_val = X[:split_idx], X[split_idx:]
                y_train, y_val = y[:split_idx], y[split_idx:]
                if self.model_type == "lightgbm":
                    m = self.train_lightgbm(X_train, y_train, X_val, y_val, hyperparameters=candidate)
                else:
                    m = self.train_xgboost(X_train, y_train, X_val, y_val, hyperparameters=candidate)
                y_pred = m.predict(X_val) if len(X_val) > 0 else np.array([])
                val_f1 = _f1(y_val, y_pred) if len(X_val) > 0 else 0.0
                score = (mean_f1 + val_f1) / 2 if np.isfinite(mean_f1) else val_f1
            except Exception as exc:
                logger.debug("Skipping invalid hyperparameter candidate {} due to {}", candidate, exc)
                continue

            if score > best_score:
                best_score = score
                best_params = candidate

        if not best_params:
            # Fallback to first candidate if every trial fails under constrained env.
            best_params = {k: vals[0] for k, vals in zip(grid_keys, grid_values)}

        logger.info("Selected hyperparameters: {} (score={:.4f})", best_params, best_score if np.isfinite(best_score) else -1.0)
        return best_params

    def calculate_feature_importance(self, model: object, feature_names: List[str]) -> Dict[str, float]:
        if hasattr(model, "feature_importances_"):
            importances = getattr(model, "feature_importances_")
            if len(importances) == len(feature_names):
                return dict(sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True))
        return {}

    def train_model(
        self,
        df: pd.DataFrame,
        target_col: str = "returns",
        feature_cols: Optional[List[str]] = None,
        validation_split: float = 0.2,
        hyperparameters: Optional[Dict] = None,
        perform_cv: bool = True,
        model_id: str = "model_1",
    ) -> TrainedModel:
        start = time.time()
        X, y, feature_names = self.prepare_training_data(df, target_col, feature_cols)

        split_idx = int(len(X) * (1 - validation_split))
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]

        model = self.train_lightgbm(X_train, y_train, X_val, y_val, hyperparameters) if self.model_type == "lightgbm" else self.train_xgboost(X_train, y_train, X_val, y_val, hyperparameters)

        y_pred = model.predict(X_val)
        metrics = ModelMetrics(
            accuracy=_accuracy(y_val, y_pred),
            precision=_precision(y_val, y_pred),
            recall=_recall(y_val, y_pred),
            f1=_f1(y_val, y_pred),
            auc=0.0,
            feature_importance=self.calculate_feature_importance(model, feature_names),
        )

        trained = TrainedModel(
            model_type=self.model_type,
            model=model,
            feature_names=feature_names,
            metrics=metrics,
            train_time=time.time() - start,
            hyperparameters=hyperparameters or {},
        )
        self.trained_models[model_id] = trained

        if perform_cv:
            self.time_series_cv(X, y, feature_names)

        return trained

    def save_model(self, model_id: str, filepath: str) -> bool:
        if model_id not in self.trained_models:
            logger.error(f"Model {model_id} not found")
            return False
        try:
            trained = self.trained_models[model_id]
            model_data = {
                "model": trained.model,
                "model_type": trained.model_type,
                "feature_names": trained.feature_names,
                "metrics": {
                    "accuracy": trained.metrics.accuracy,
                    "precision": trained.metrics.precision,
                    "recall": trained.metrics.recall,
                    "f1": trained.metrics.f1,
                    "auc": trained.metrics.auc,
                    "feature_importance": trained.metrics.feature_importance,
                },
                "hyperparameters": trained.hyperparameters,
            }
            if joblib is not None:
                joblib.dump(model_data, filepath)
            else:
                with open(filepath, "wb") as f:
                    pickle.dump(model_data, f)
            return True
        except Exception as e:
            logger.error(f"Error saving model: {e}")
            return False

    def load_model(self, filepath: str, model_id: str) -> bool:
        try:
            if joblib is not None:
                data = joblib.load(filepath)
            else:
                with open(filepath, "rb") as f:
                    data = pickle.load(f)
            trained = TrainedModel(
                model_type=data["model_type"],
                model=data["model"],
                feature_names=data["feature_names"],
                metrics=ModelMetrics(**data["metrics"]),
                train_time=0.0,
                hyperparameters=data["hyperparameters"],
            )
            self.trained_models[model_id] = trained
            return True
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            return False

    def predict(self, model_id: str, X: np.ndarray) -> np.ndarray:
        if model_id not in self.trained_models:
            return np.array([])
        return self.trained_models[model_id].model.predict(X)

    def predict_proba(self, model_id: str, X: np.ndarray) -> np.ndarray:
        if model_id not in self.trained_models:
            return np.array([])
        model = self.trained_models[model_id].model
        if hasattr(model, "predict_proba"):
            return model.predict_proba(X)
        preds = model.predict(X)
        return np.column_stack([1.0 - preds, preds])
