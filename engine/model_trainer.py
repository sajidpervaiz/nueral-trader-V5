"""
Model Trainer with LightGBM/XGBoost, time-series CV, feature importance.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import lightgbm as lgb
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import joblib
import json
from loguru import logger


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


class ModelTrainer:
    """
    Production-grade model trainer with:
    - LightGBM and XGBoost support
    - Time-series cross-validation
    - Hyperparameter tuning
    - Feature importance analysis
    - Model serialization
    """

    def __init__(
        self,
        model_type: str = 'lightgbm',
        random_state: int = 42,
    ):
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
        """
        Prepare training data with lookahead target.

        Args:
            df: Feature dataframe
            target_col: Column name for price/returns
            feature_cols: List of feature columns
            lookahead: Number of periods to look ahead for target

        Returns:
            X, y, feature_names
        """
        df = df.copy()

        if target_col == 'returns':
            df['target_returns'] = df['close'].pct_change(lookahead).shift(-lookahead)
            df['target_direction'] = (df['target_returns'] > 0).astype(int)
            target = df['target_direction']
        else:
            target = df[target_col].shift(-lookahead)

        df = df.dropna()

        if feature_cols is None:
            feature_cols = [
                col for col in df.columns
                if col not in ['open', 'high', 'low', 'close', 'volume',
                               'target_returns', 'target_direction', 'timestamp']
            ]

        X = df[feature_cols].values
        y = target.values

        logger.info(f"Prepared training data: X shape {X.shape}, y shape {y.shape}")

        return X, y, feature_cols

    def train_lightgbm(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
        hyperparameters: Optional[Dict] = None,
    ) -> lgb.LGBMClassifier:
        """Train LightGBM model."""
        default_params = {
            'objective': 'binary',
            'metric': 'binary_logloss',
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'learning_rate': 0.05,
            'feature_fraction': 0.9,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
            'random_state': self.random_state,
            'n_estimators': 100,
        }

        if hyperparameters:
            default_params.update(hyperparameters)

        model = lgb.LGBMClassifier(**default_params)

        if X_val is not None and y_val is not None:
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                callbacks=[lgb.early_stopping(stopping_rounds=10, verbose=False)],
            )
        else:
            model.fit(X_train, y_train)

        return model

    def train_xgboost(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
        hyperparameters: Optional[Dict] = None,
    ) -> xgb.XGBClassifier:
        """Train XGBoost model."""
        default_params = {
            'objective': 'binary:logistic',
            'eval_metric': 'logloss',
            'learning_rate': 0.05,
            'max_depth': 6,
            'min_child_weight': 1,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'random_state': self.random_state,
            'n_estimators': 100,
        }

        if hyperparameters:
            default_params.update(hyperparameters)

        model = xgb.XGBClassifier(**default_params)

        if X_val is not None and y_val is not None:
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                early_stopping_rounds=10,
                verbose=False,
            )
        else:
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
        """
        Perform time-series cross-validation.

        Returns cross-validation metrics.
        """
        tscv = TimeSeriesSplit(n_splits=n_splits, test_size=test_size)

        cv_metrics = {
            'accuracy': [],
            'precision': [],
            'recall': [],
            'f1': [],
        }

        for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
            logger.info(f"Training fold {fold + 1}/{n_splits}")

            X_train, X_val = X[train_idx], X[val_idx]
            y_train, y_val = y[train_idx], y[val_idx]

            if self.model_type == 'lightgbm':
                model = self.train_lightgbm(X_train, y_train, X_val, y_val)
            else:
                model = self.train_xgboost(X_train, y_train, X_val, y_val)

            y_pred = model.predict(X_val)

            cv_metrics['accuracy'].append(accuracy_score(y_val, y_pred))
            cv_metrics['precision'].append(precision_score(y_val, y_pred, zero_division=0))
            cv_metrics['recall'].append(recall_score(y_val, y_pred, zero_division=0))
            cv_metrics['f1'].append(f1_score(y_val, y_pred, zero_division=0))

        for metric, values in cv_metrics.items():
            logger.info(f"CV {metric}: {np.mean(values):.4f} (+/- {np.std(values):.4f})")

        return cv_metrics

    def hyperparameter_tuning(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: List[str],
        param_grid: Optional[Dict] = None,
        cv_folds: int = 3,
    ) -> Dict[str, any]:
        """Perform hyperparameter tuning with grid search."""
        if self.model_type == 'lightgbm':
            model = lgb.LGBMClassifier(random_state=self.random_state, verbose=-1)
            default_params = {
                'num_leaves': [15, 31, 63],
                'learning_rate': [0.01, 0.05, 0.1],
                'n_estimators': [50, 100, 200],
            }
        else:
            model = xgb.XGBClassifier(random_state=self.random_state, use_label_encoder=False, eval_metric='logloss')
            default_params = {
                'max_depth': [4, 6, 8],
                'learning_rate': [0.01, 0.05, 0.1],
                'n_estimators': [50, 100, 200],
            }

        param_grid = param_grid or default_params

        tscv = TimeSeriesSplit(n_splits=cv_folds, test_size=100)

        grid_search = GridSearchCV(
            model,
            param_grid,
            cv=tscv,
            scoring='f1',
            n_jobs=-1,
            verbose=1,
        )

        grid_search.fit(X, y)

        logger.info(f"Best parameters: {grid_search.best_params_}")
        logger.info(f"Best score: {grid_search.best_score_:.4f}")

        return grid_search.best_params_

    def calculate_feature_importance(
        self,
        model: object,
        feature_names: List[str],
    ) -> Dict[str, float]:
        """Calculate feature importance."""
        if hasattr(model, 'feature_importances_'):
            importances = model.feature_importances_
        elif hasattr(model, 'feature_importance'):
            importances = model.feature_importance(importance_type='gain')
        else:
            logger.warning("Model does not have feature importance attribute")
            return {}

        feature_importance = dict(zip(feature_names, importances))

        sorted_features = sorted(
            feature_importance.items(),
            key=lambda x: x[1],
            reverse=True,
        )

        logger.info("Top 10 features:")
        for feature, importance in sorted_features[:10]:
            logger.info(f"  {feature}: {importance:.4f}")

        return dict(sorted_features)

    def train_model(
        self,
        df: pd.DataFrame,
        target_col: str = 'returns',
        feature_cols: Optional[List[str]] = None,
        validation_split: float = 0.2,
        hyperparameters: Optional[Dict] = None,
        perform_cv: bool = True,
        model_id: str = 'model_1',
    ) -> TrainedModel:
        """
        Train a complete model with validation.

        Returns trained model with metrics.
        """
        import time
        start_time = time.time()

        X, y, feature_names = self.prepare_training_data(
            df, target_col, feature_cols
        )

        split_idx = int(len(X) * (1 - validation_split))
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]

        if self.model_type == 'lightgbm':
            model = self.train_lightgbm(X_train, y_train, X_val, y_val, hyperparameters)
        else:
            model = self.train_xgboost(X_train, y_train, X_val, y_val, hyperparameters)

        y_pred = model.predict(X_val)

        metrics = ModelMetrics(
            accuracy=accuracy_score(y_val, y_pred),
            precision=precision_score(y_val, y_pred, zero_division=0),
            recall=recall_score(y_val, y_pred, zero_division=0),
            f1=f1_score(y_val, y_pred, zero_division=0),
            auc=0.0,
            feature_importance=self.calculate_feature_importance(model, feature_names),
        )

        train_time = time.time() - start_time

        trained_model = TrainedModel(
            model_type=self.model_type,
            model=model,
            feature_names=feature_names,
            metrics=metrics,
            train_time=train_time,
            hyperparameters=hyperparameters or {},
        )

        self.trained_models[model_id] = trained_model

        logger.info(f"Model training completed in {train_time:.2f}s")
        logger.info(f"Validation metrics: Accuracy={metrics.accuracy:.4f}, F1={metrics.f1:.4f}")

        if perform_cv:
            logger.info("Performing time-series cross-validation...")
            self.time_series_cv(X, y, feature_names)

        return trained_model

    def save_model(
        self,
        model_id: str,
        filepath: str,
    ) -> bool:
        """Save trained model to disk."""
        if model_id not in self.trained_models:
            logger.error(f"Model {model_id} not found")
            return False

        trained_model = self.trained_models[model_id]

        try:
            model_data = {
                'model': trained_model.model,
                'model_type': trained_model.model_type,
                'feature_names': trained_model.feature_names,
                'metrics': {
                    'accuracy': trained_model.metrics.accuracy,
                    'precision': trained_model.metrics.precision,
                    'recall': trained_model.metrics.recall,
                    'f1': trained_model.metrics.f1,
                },
                'hyperparameters': trained_model.hyperparameters,
            }

            joblib.dump(model_data, filepath)
            logger.info(f"Model saved to {filepath}")
            return True

        except Exception as e:
            logger.error(f"Error saving model: {e}")
            return False

    def load_model(
        self,
        filepath: str,
        model_id: str,
    ) -> bool:
        """Load trained model from disk."""
        try:
            model_data = joblib.load(filepath)

            trained_model = TrainedModel(
                model_type=model_data['model_type'],
                model=model_data['model'],
                feature_names=model_data['feature_names'],
                metrics=ModelMetrics(**model_data['metrics']),
                train_time=0.0,
                hyperparameters=model_data['hyperparameters'],
            )

            self.trained_models[model_id] = trained_model
            logger.info(f"Model loaded from {filepath}")
            return True

        except Exception as e:
            logger.error(f"Error loading model: {e}")
            return False

    def predict(
        self,
        model_id: str,
        X: np.ndarray,
    ) -> np.ndarray:
        """Make predictions with trained model."""
        if model_id not in self.trained_models:
            logger.error(f"Model {model_id} not found")
            return np.array([])

        model = self.trained_models[model_id].model
        return model.predict(X)

    def predict_proba(
        self,
        model_id: str,
        X: np.ndarray,
    ) -> np.ndarray:
        """Get prediction probabilities."""
        if model_id not in self.trained_models:
            logger.error(f"Model {model_id} not found")
            return np.array([])

        model = self.trained_models[model_id].model
        return model.predict_proba(X)
