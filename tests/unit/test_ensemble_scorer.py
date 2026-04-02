from __future__ import annotations

import numpy as np

from engine.ensemble_scorer import EnsembleScorer


class _ProbModel:
    def __init__(self, p: float) -> None:
        self.p = p

    def predict_proba(self, features):
        _ = features
        return np.array([[1.0 - self.p, self.p]])


class _BinaryModel:
    def __init__(self, y: int) -> None:
        self.y = y

    def predict(self, features):
        _ = features
        return np.array([self.y])


def test_predict_returns_weighted_and_confident_output() -> None:
    models = {
        "m1": _ProbModel(0.70),
        "m2": _ProbModel(0.60),
        "m3": _BinaryModel(1),
    }
    scorer = EnsembleScorer(models=models)

    pred = scorer.predict(np.array([[0.1, 0.2]]), regime="BULL")

    assert 0.0 <= pred.raw_prediction <= 1.0
    assert 0.0 <= pred.calibrated_prediction <= 1.0
    assert 0.0 <= pred.confidence <= 1.0
    assert len(pred.individual_predictions) == 3


def test_weight_update_and_stats_smoke() -> None:
    models = {
        "m1": _ProbModel(0.8),
        "m2": _ProbModel(0.4),
    }
    scorer = EnsembleScorer(models=models)

    for i in range(120):
        pred = scorer.predict(np.array([[0.3, 0.7]]), regime="SIDEWAYS")
        actual = 1.0 if i % 2 == 0 else 0.0
        scorer.update_with_actual(pred, actual)

    stats = scorer.get_statistics()
    assert stats["num_predictions"] > 0
    assert abs(sum(scorer.weights.values()) - 1.0) < 1e-9


def test_regime_weight_management_and_health_status() -> None:
    models = {
        "m1": _ProbModel(0.55),
        "m2": _ProbModel(0.45),
    }
    scorer = EnsembleScorer(models=models)

    scorer.set_regime_weights("CUSTOM", {"m1": 2.0, "m2": 1.0})
    custom_weights = scorer.get_regime_weights("CUSTOM")

    assert set(custom_weights.keys()) == {"m1", "m2"}
    assert abs(sum(custom_weights.values()) - 1.0) < 1e-9
    assert custom_weights["m1"] > custom_weights["m2"]

    health = scorer.get_health_status()
    assert health["num_models"] == 2
    assert "CUSTOM" in health["regimes_configured"]
    assert health["calibrator_backend"] in {"calibr8", "numpy_fallback"}


def test_regime_scoped_calibrator_history() -> None:
    models = {
        "m1": _ProbModel(0.7),
        "m2": _ProbModel(0.3),
    }
    scorer = EnsembleScorer(models=models)

    for i in range(120):
        pred = scorer.predict(np.array([[0.2, 0.8]]), regime="BULL")
        scorer.update_with_actual(pred, 1.0 if i % 2 == 0 else 0.0)

    assert "BULL_calibrator" in scorer.calibrators
    assert "BEAR_calibrator" not in scorer.calibrators
