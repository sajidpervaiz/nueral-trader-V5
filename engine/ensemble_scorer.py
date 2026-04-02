"""
Ensemble Scorer with regime-aware voting, online learning, calibration.
"""

import numpy as np
import pandas as pd
import importlib
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from collections import deque
from loguru import logger

try:
    calibr8 = importlib.import_module("calibr8")
except ImportError:  # pragma: no cover - runtime fallback
    calibr8 = None


@dataclass
class EnsemblePrediction:
    timestamp: int
    raw_prediction: float
    calibrated_prediction: float
    confidence: float
    individual_predictions: Dict[str, float]
    weights: Dict[str, float]
    regime: str
    metadata: Dict


class _NumpyLogisticCalibrator:
    """Minimal logistic calibrator fallback when calibr8 is unavailable."""

    def __init__(self) -> None:
        self._mean_pred = 0.5
        self._mean_actual = 0.5

    def fit(self, predictions: np.ndarray, actuals: np.ndarray) -> None:
        p = np.asarray(predictions, dtype=float).reshape(-1)
        a = np.asarray(actuals, dtype=float).reshape(-1)
        if p.size == 0 or a.size == 0:
            return
        self._mean_pred = float(np.clip(np.mean(p), 1e-6, 1 - 1e-6))
        self._mean_actual = float(np.clip(np.mean(a), 1e-6, 1 - 1e-6))

    def predict(self, values: List[List[float]]) -> np.ndarray:
        v = np.asarray(values, dtype=float).reshape(-1)
        shift = self._mean_actual - self._mean_pred
        calibrated = np.clip(v + shift, 0.01, 0.99)
        return calibrated


class EnsembleScorer:
    """
    Production-grade ensemble scorer with:
    - Regime-aware voting
    - Online learning
    - Probability calibration
    - Dynamic weighting
    """

    def __init__(
        self,
        models: Dict[str, Any],
        default_weights: Optional[Dict[str, float]] = None,
        calibration_window: int = 1000,
    ):
        if not models:
            raise ValueError("EnsembleScorer requires at least one model")

        self.models = models
        base_weights = default_weights or {model_id: 1.0 for model_id in models}
        self.weights = self._normalized_weights(base_weights)

        self.calibration_window = calibration_window
        self.prediction_history: deque = deque(maxlen=calibration_window)
        self.actual_history: deque = deque(maxlen=calibration_window)

        self.regime_weights: Dict[str, Dict[str, float]] = {
            'BULL': {k: 1.0 for k in models},
            'BEAR': {k: 1.0 for k in models},
            'SIDEWAYS': {k: 1.0 for k in models},
            'VOLATILE': {k: 1.0 for k in models},
            'TRANSITION': {k: 1.0 for k in models},
        }

        self.calibrators: Dict[str, Any] = {}
        self.regime_prediction_history: Dict[str, deque] = {
            r: deque(maxlen=calibration_window) for r in self.regime_weights
        }
        self.regime_actual_history: Dict[str, deque] = {
            r: deque(maxlen=calibration_window) for r in self.regime_weights
        }

    def set_regime_weights(self, regime: str, weights: Dict[str, float]) -> None:
        """Set or override regime-specific weight multipliers for known models."""
        if not regime:
            raise ValueError("regime must be a non-empty string")
        self.regime_weights[regime] = {
            model_id: max(0.0, float(weights.get(model_id, 1.0)))
            for model_id in self.models
        }

    def get_regime_weights(self, regime: str) -> Dict[str, float]:
        """Get normalized effective weights for a regime based on current dynamic weights."""
        regime_prior = self.regime_weights.get(regime, {mid: 1.0 for mid in self.models})
        blended = {
            model_id: self.weights.get(model_id, 0.0) * float(regime_prior.get(model_id, 1.0))
            for model_id in self.models
        }
        return self._normalized_weights(blended)

    def _normalized_weights(self, weights: Dict[str, float]) -> Dict[str, float]:
        filtered = {mid: max(0.0, float(weights.get(mid, 0.0))) for mid in self.models}
        total = float(sum(filtered.values()))
        if total <= 0:
            uniform = 1.0 / len(self.models)
            return {mid: uniform for mid in self.models}
        return {mid: w / total for mid, w in filtered.items()}

    def predict(
        self,
        features: np.ndarray,
        regime: str = 'SIDEWAYS',
        use_regime_weights: bool = True,
    ) -> EnsemblePrediction:
        """Generate ensemble prediction."""
        individual_predictions = {}

        for model_id, model in self.models.items():
            try:
                if hasattr(model, 'predict_proba'):
                    pred = model.predict_proba(features)[0, 1]
                elif hasattr(model, 'predict'):
                    pred = float(model.predict(features)[0])
                else:
                    pred = 0.5
                individual_predictions[model_id] = pred
            except Exception as e:
                logger.error(f"Error predicting with {model_id}: {e}")
                individual_predictions[model_id] = 0.5

        if use_regime_weights:
            weights = self.get_regime_weights(regime)
        else:
            weights = self.weights

        weighted_pred = sum(
            individual_predictions[model_id] * weights.get(model_id, 0)
            for model_id in individual_predictions
        )
        total_weight = sum(weights.values())
        weighted_pred = weighted_pred / total_weight if total_weight > 0 else 0.5
        weighted_pred = np.clip(weighted_pred, 0.0, 1.0)

        calibrated_pred = self._calibrate_prediction(weighted_pred, regime)
        confidence = self._calculate_confidence(individual_predictions, weights)

        prediction = EnsemblePrediction(
            timestamp=pd.Timestamp.now().value // 10**6,
            raw_prediction=weighted_pred,
            calibrated_prediction=calibrated_pred,
            confidence=confidence,
            individual_predictions=individual_predictions,
            weights=weights,
            regime=regime,
            metadata={
                'num_models': len(individual_predictions),
                'has_regime_weights': use_regime_weights,
            },
        )
        return prediction

    def _calibrate_prediction(self, prediction: float, regime: str) -> float:
        if len(self.prediction_history) < 50:
            return prediction

        calibrator_key = f"{regime}_calibrator"
        if calibrator_key not in self.calibrators:
            return prediction

        try:
            calibrated = self.calibrators[calibrator_key].predict([[prediction]])[0]
            return float(np.clip(calibrated, 0.01, 0.99))
        except Exception:
            return prediction

    def _calculate_confidence(
        self,
        individual_predictions: Dict[str, float],
        weights: Dict[str, float],
    ) -> float:
        preds = list(individual_predictions.values())
        if not preds:
            return 0.0

        std_dev = np.std(preds)
        weight_sum = sum(weights.get(model_id, 0.0) for model_id in individual_predictions)
        if weight_sum <= 0:
            mean_weighted = float(np.mean(preds))
        else:
            mean_weighted = sum(
                pred * weights.get(model_id, 0.0)
                for model_id, pred in individual_predictions.items()
            ) / weight_sum

        distance_from_neutral = abs(mean_weighted - 0.5)
        confidence = distance_from_neutral * 2 * (1 - std_dev)
        return float(np.clip(confidence, 0.0, 1.0))

    def update_with_actual(self, prediction: EnsemblePrediction, actual: float) -> None:
        self.prediction_history.append(prediction.calibrated_prediction)
        self.actual_history.append(actual)

        if prediction.regime not in self.regime_prediction_history:
            self.regime_prediction_history[prediction.regime] = deque(maxlen=self.calibration_window)
            self.regime_actual_history[prediction.regime] = deque(maxlen=self.calibration_window)
        self.regime_prediction_history[prediction.regime].append(prediction.calibrated_prediction)
        self.regime_actual_history[prediction.regime].append(actual)

        self._update_weights(prediction, actual)

        if len(self.regime_prediction_history[prediction.regime]) >= 100:
            self._update_calibrators(prediction.regime)

    def _update_weights(
        self,
        prediction: EnsemblePrediction,
        actual: float,
        learning_rate: float = 0.01,
    ) -> None:
        error = actual - prediction.calibrated_prediction

        for model_id, model_pred in prediction.individual_predictions.items():
            model_error = actual - model_pred
            if abs(model_error) < abs(error):
                weight_adjustment = learning_rate * abs(error)
            else:
                weight_adjustment = -learning_rate * abs(error) * 0.5
            self.weights[model_id] = max(0.01, self.weights[model_id] + weight_adjustment)

        self.weights = self._normalized_weights(self.weights)

    def _update_calibrators(self, regime: str) -> None:
        pred_hist = self.regime_prediction_history.get(regime)
        act_hist = self.regime_actual_history.get(regime)
        if pred_hist is None or act_hist is None or len(pred_hist) < 100:
            return

        predictions = list(pred_hist)
        actuals = list(act_hist)

        try:
            if calibr8 is not None and hasattr(calibr8, "LogisticCalibrator"):
                calibrator = calibr8.LogisticCalibrator()
                calibrator.fit(np.array(predictions).reshape(-1, 1), np.array(actuals))
            else:
                calibrator = _NumpyLogisticCalibrator()
                calibrator.fit(np.array(predictions), np.array(actuals))

            calibrator_key = f"{regime}_calibrator"
            self.calibrators[calibrator_key] = calibrator
            logger.debug(f"Updated calibrator for regime {regime}")
        except Exception as e:
            logger.error(f"Error updating calibrator: {e}")

    def calculate_ensemble_weights(
        self,
        recent_predictions: List[Dict[str, float]],
        actuals: List[float],
    ) -> Dict[str, float]:
        """Calculate ensemble weights based on recent performance."""
        model_performances = {model_id: [] for model_id in self.models}

        for preds, actual in zip(recent_predictions, actuals):
            for model_id, pred in preds.items():
                error = abs(actual - pred)
                model_performances[model_id].append(error)

        new_weights = {}
        for model_id, errors in model_performances.items():
            if errors:
                avg_error = np.mean(errors)
                performance = 1.0 / (1.0 + avg_error)
            else:
                performance = 1.0
            new_weights[model_id] = performance

        total_performance = sum(new_weights.values())
        if total_performance > 0:
            new_weights = {k: v / total_performance for k, v in new_weights.items()}
        else:
            new_weights = {k: 1.0 / len(self.models) for k in self.models}

        logger.info(f"Updated ensemble weights: {new_weights}")
        return new_weights

    def detect_prediction_decay(
        self,
        window: int = 100,
        threshold: float = 0.1,
    ) -> bool:
        """Detect if prediction quality is decaying."""
        if len(self.prediction_history) < window * 2:
            return False

        recent_predictions = list(self.prediction_history)[-window:]
        older_predictions = list(self.prediction_history)[-2 * window:-window]

        if len(self.actual_history) < window * 2:
            return False

        recent_actuals = list(self.actual_history)[-window:]
        older_actuals = list(self.actual_history)[-2 * window:-window]

        recent_mse = np.mean([(r - a) ** 2 for r, a in zip(recent_predictions, recent_actuals)])
        older_mse = np.mean([(r - a) ** 2 for r, a in zip(older_predictions, older_actuals)])

        mse_increase = (recent_mse - older_mse) / older_mse if older_mse > 0 else 0
        decay_detected = mse_increase > threshold

        if decay_detected:
            logger.warning(f"Prediction decay detected: MSE increased by {mse_increase:.2%}")

        return decay_detected

    def get_statistics(self) -> Dict:
        """Get ensemble scorer statistics."""
        if len(self.prediction_history) == 0 or len(self.actual_history) == 0:
            return {}

        predictions = np.array(list(self.prediction_history))
        actuals = np.array(list(self.actual_history))

        mse = np.mean((predictions - actuals) ** 2)
        mae = np.mean(np.abs(predictions - actuals))

        direction_correct = np.mean(
            ((predictions > 0.5) & (actuals == 1))
            | ((predictions <= 0.5) & (actuals == 0))
        )

        return {
            'num_predictions': len(predictions),
            'mean_squared_error': mse,
            'mean_absolute_error': mae,
            'direction_accuracy': direction_correct,
            'mean_prediction': np.mean(predictions),
            'std_prediction': np.std(predictions),
            'current_weights': self.weights,
        }

    def get_health_status(self) -> Dict[str, Any]:
        """Operational health snapshot for monitoring and diagnostics."""
        return {
            'num_models': len(self.models),
            'history_size': len(self.prediction_history),
            'actual_size': len(self.actual_history),
            'calibration_ready': len(self.prediction_history) >= 100,
            'calibrator_backend': 'calibr8' if calibr8 is not None else 'numpy_fallback',
            'calibrators_loaded': list(self.calibrators.keys()),
            'regimes_configured': sorted(self.regime_weights.keys()),
        }
