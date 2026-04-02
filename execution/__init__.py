# Execution Module - CEX Executors and Order Management

from importlib import import_module

from .order_manager import OrderManager, Order, OrderStatus


_LAZY_IMPORTS = {
    "BinanceExecutor": (".binance_executor", "BinanceExecutor"),
    "BybitExecutor": (".bybit_executor", "BybitExecutor"),
    "OKXExecutor": (".okx_executor", "OKXExecutor"),
    "SmartOrderRouter": (".smart_order_router", "SmartOrderRouter"),
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
                f"Failed to import execution symbol '{name}'. "
                "Ensure optional exchange dependencies are installed (for example: ccxt)."
            ) from exc
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = [
    'BinanceExecutor',
    'BybitExecutor',
    'OKXExecutor',
    'OrderManager',
    'Order',
    'OrderStatus',
    'SmartOrderRouter',
]
