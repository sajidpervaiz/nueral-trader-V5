# Engine Module - ML Pipeline

from importlib import import_module

from .signal_validator import SignalValidator, Signal, PnLTracking, SignalQuality


_LAZY_IMPORTS = {
    "FeatureEngineering": (".feature_engineering", "FeatureEngineering"),
    "FeatureSet": (".feature_engineering", "FeatureSet"),
    "ModelTrainer": (".model_trainer", "ModelTrainer"),
    "TrainedModel": (".model_trainer", "TrainedModel"),
    "ModelMetrics": (".model_trainer", "ModelMetrics"),
    "EnsembleScorer": (".ensemble_scorer", "EnsembleScorer"),
    "EnsemblePrediction": (".ensemble_scorer", "EnsemblePrediction"),
}


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        module_name, attr_name = _LAZY_IMPORTS[name]
        try:
            module = import_module(module_name, __name__)
            value = getattr(module, attr_name)
            globals()[name] = value
            return value
        except Exception as exc:  # pragma: no cover - defensive import guard
            raise ImportError(
                f"Failed to import engine symbol '{name}'. "
                "Ensure optional ML dependencies are installed (for example: ta, lightgbm, xgboost)."
            ) from exc
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = [
    'FeatureEngineering',
    'FeatureSet',
    'ModelTrainer',
    'TrainedModel',
    'ModelMetrics',
    'EnsembleScorer',
    'EnsemblePrediction',
    'SignalValidator',
    'Signal',
    'PnLTracking',
    'SignalQuality',
]
