"""
Ensemble Scorer with regime-aware voting, online learning, calibration.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional
from dataclasses import dataclass
from collections import deque
import calibr8
from loguru import logger


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
        models: Dict[str, any],
        default_weights: Optional[Dict[str, float]] = None,
        calibration_window: int = 1000,
    ):
        self.models = models
        self.weights = default_weights or {model_id: 1.0 / len(models) for model_id in models}

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

        self.calibrators: Dict[str, any] = {}

    def predict(
        self,
        features: np.ndarray,
        regime: str = 'SIDEWAYS',
        use_regime_weights: bool = True,
    ) -> EnsemblePrediction:
        """
        Generate ensemble prediction.

        Args:
            features: Feature array for prediction
            regime: Current market regime
            use_regime_weights: Whether to use regime-specific weights

        Returns:
            Ensemble prediction with confidence
        """
        individual_predictions = {}

        for model_id, model in self.models.items():
            try:
                if hasattr(model, 'predict_proba'):
                    pred = model.predict_proba(features)[0, 1]
                elif hasattr(model, 'predict'):
                    pred = model.predict(features)[0]
                    pred = float(pred)
                else:
                    pred = 0.5

                individual_predictions[model_id] = pred
            except Exception as e:
                logger.error(f"Error predicting with {model_id}: {e}")
                individual_predictions[model_id] = 0.5

        if use_regime_weights:
            weights = self.regime_weights.get(regime, self.weights)
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

        self.prediction_history.append(calibrated_pred)

        return prediction

    def _calibrate_prediction(self, prediction: float, regime: str) -> float:
        """Calibrate prediction using Platt scaling or isotonic regression."""
        if len(self.prediction_history) < 50:
            return prediction

        calibrator_key = f"{regime}_calibrator"

        if calibrator_key not in self.calibrators:
            return prediction

        try:
            calibrated = self.calibrators[calibrator_key].predict([[prediction]])[0]
            return np.clip(calibrated, 0.01, 0.99)
        except Exception:
            return prediction

    def _calculate_confidence(
        self,
        individual_predictions: Dict[str, float],
        weights: Dict[str, float],
    ) -> float:
        """Calculate prediction confidence based on model agreement."""
        preds = list(individual_predictions.values())

        if not preds:
            return 0.0

        std_dev = np.std(preds)

        mean_weighted = sum(
            preds[i] * list(weights.values())[i]
            for i in range(len(preds))
        ) / sum(weights.values())

        distance_from_neutral = abs(mean_weighted - 0.5)

        confidence = distance_from_neutral * 2 * (1 - std_dev)

        return np.clip(confidence, 0.0, 1.0)

    def update_with_actual(
        self,
        prediction: EnsemblePrediction,
        actual: float,
    ) -> None:
        """
        Update model with actual outcome for online learning.

        Args:
            prediction: The ensemble prediction
            actual: Actual outcome (0 or 1)
        """
        self.prediction_history.append(prediction.raw_prediction)
        self.actual_history.append(actual)

        self._update_weights(prediction, actual)

        if len(self.prediction_history) >= 100:
            self._update_calibrators(prediction.regime)

    def _update_weights(
        self,
        prediction: EnsemblePrediction,
        actual: float,
        learning_rate: float = 0.01,
    ) -> None:
        """Update model weights based on prediction accuracy."""
        error = actual - prediction.calibrated_prediction

        for model_id, model_pred in prediction.individual_predictions.items():
            model_error = actual - model_pred

            if abs(model_error) < abs(error):
                weight_adjustment = learning_rate * abs(error)
            else:
                weight_adjustment = -learning_rate * abs(error) * 0.5

            self.weights[model_id] = max(0.01, self.weights[model_id] + weight_adjustment)

        total_weight = sum(self.weights.values())
        self.weights = {k: v / total_weight for k, v in self.weights.items()}

    def _update_calibrators(self, regime: str) -> None:
        """Update probability calibrators."""
        if len(self.prediction_history) < 100:
            return

        predictions = list(self.prediction_history)
        actuals = list(self.actual_history)

        try:
            calibrator = calibr8.LogisticCalibrator()
            calibrator.fit(np.array(predictions).reshape(-1, 1), np.array(actuals))

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
        """
        Calculate ensemble weights based on recent performance.

        Args:
            recent_predictions: List of individual model predictions
            actuals: List of actual outcomes

        Returns:
            Updated weights for each model
        """
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
        """
        Detect if prediction quality is decaying.

        Returns True if decay is detected.
        """
        if len(self.prediction_history) < window * 2:
            return False

        recent_predictions = list(self.prediction_history)[-window:]
        older_predictions = list(self.prediction_history)[-2*window:-window]

        if len(self.actual_history) < window * 2:
            return False

        recent_actuals = list(self.actual_history)[-window:]
        older_actuals = list(self.actual_history)[-2*window:-window]

        recent_mse = np.mean(
            [(r - a) ** 2 for r, a in zip(recent_predictions, recent_actuals)]
        )
        older_mse = np.mean(
            [(r - a) ** 2 for r, a in zip(older_predictions, older_actuals)]
        )

        mse_increase = (recent_mse - older_mse) / older_mse if older_mse > 0 else 0

        decay_detected = mse_increase > threshold

        if decay_detected:
            logger.warning(
                f"Prediction decay detected: MSE increased by {mse_increase:.2%}"
            )

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
            ((predictions > 0.5) & (actuals == 1)) |
            ((predictions <= 0.5) & (actuals == 0))
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
