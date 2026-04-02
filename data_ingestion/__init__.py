# Data Ingestion Module - Macro Data Feeds

from importlib import import_module


_LAZY_IMPORTS = {
    "FedCalendar": (".fed_calendar", "FedCalendar"),
    "FedEvent": (".fed_calendar", "FedEvent"),
    "FedEventType": (".fed_calendar", "FedEventType"),
    "EconomicReleases": (".economic_releases", "EconomicReleases"),
    "EconomicRelease": (".economic_releases", "EconomicRelease"),
    "IndicatorType": (".economic_releases", "IndicatorType"),
    "MacroAggregator": (".macro_aggregator", "MacroAggregator"),
    "MacroSignal": (".macro_aggregator", "MacroSignal"),
    "MarketRegime": (".macro_aggregator", "MarketRegime"),
    "RegimeClassification": (".macro_aggregator", "RegimeClassification"),
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
                f"Failed to import data_ingestion symbol '{name}'. "
                "Ensure optional feed dependencies are installed (for example: aiohttp)."
            ) from exc
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = [
    'FedCalendar',
    'FedEvent',
    'FedEventType',
    'EconomicReleases',
    'EconomicRelease',
    'IndicatorType',
    'MacroAggregator',
    'MacroSignal',
    'MarketRegime',
    'RegimeClassification',
]
