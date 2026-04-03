#!/usr/bin/env python3
"""Preflight validation before enabling live auto-trading."""

from __future__ import annotations

import os
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "settings.live.yaml"
EXCHANGE_ENV_REQUIREMENTS = {
    "binance": ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
    "bybit": ("BYBIT_API_KEY", "BYBIT_API_SECRET"),
    "okx": ("OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"),
    "kraken": ("KRAKEN_API_KEY", "KRAKEN_API_SECRET"),
}


def _load(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _fail(message: str) -> int:
    print(f"FAIL: {message}")
    return 1


def _required_env_for_enabled_exchanges(exchanges: dict) -> tuple[list[str], list[str]]:
    enabled_names: list[str] = []
    required: set[str] = set()

    for name, cfg in exchanges.items():
        if not isinstance(cfg, dict):
            continue
        if bool(cfg.get("enabled", False)):
            enabled_names.append(name)
            required.update(EXCHANGE_ENV_REQUIREMENTS.get(name, ()))

    return enabled_names, sorted(required)


def main() -> int:
    cfg_path = Path(os.getenv("NT_CONFIG_PATH", str(DEFAULT_CONFIG))).resolve()
    if not cfg_path.exists():
        return _fail(f"config not found: {cfg_path}")

    data = _load(cfg_path)
    system = data.get("system", {})
    exchanges = data.get("exchanges", {})

    if bool(system.get("paper_mode", True)):
        return _fail("paper_mode is true (live trading disabled)")

    enabled = [
        (name, cfg) for name, cfg in exchanges.items() if isinstance(cfg, dict) and bool(cfg.get("enabled", False))
    ]
    if not enabled:
        return _fail("no enabled exchanges configured")

    bad_testnet = [name for name, cfg in enabled if bool(cfg.get("testnet", True))]
    if bad_testnet:
        return _fail(f"enabled exchanges still using testnet: {', '.join(bad_testnet)}")

    enabled_names, required_env = _required_env_for_enabled_exchanges(exchanges)
    missing_env = [key for key in required_env if not os.getenv(key)]
    if missing_env:
        return _fail(f"missing required env vars: {', '.join(missing_env)}")

    print(f"PASS: live preflight checks succeeded for {cfg_path}")
    print(f"PASS: enabled exchanges validated: {', '.join(enabled_names)}")
    print("PASS: auto-trading mode can be started with NT_CONFIG_PATH set")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
