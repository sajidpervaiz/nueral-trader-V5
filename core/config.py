from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from loguru import logger


_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")


def _interpolate(value: Any) -> Any:
    if isinstance(value, str):
        def replace(m: re.Match) -> str:
            var_name = m.group(1)
            default = m.group(2) if m.group(2) is not None else ""
            return os.environ.get(var_name, default)
        return _ENV_PATTERN.sub(replace, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


class Config:
    _instance: Config | None = None

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            path = Path(__file__).parent.parent / "config" / "settings.yaml"
        with open(path, "r") as fh:
            raw = yaml.safe_load(fh)
        self._data: dict[str, Any] = _interpolate(raw)
        logger.debug("Configuration loaded from {}", path)

    @classmethod
    def get(cls, path: str | Path | None = None) -> "Config":
        if cls._instance is None:
            cls._instance = cls(path)
        return cls._instance

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def section(self, *keys: str) -> Any:
        node = self._data
        for k in keys:
            if not isinstance(node, dict):
                raise KeyError(f"Key '{k}' not found in config path {keys!r}")
            node = node[k]
        return node

    def get_value(self, *keys: str, default: Any = None) -> Any:
        try:
            return self.section(*keys)
        except (KeyError, TypeError):
            return default

    @property
    def paper_mode(self) -> bool:
        return bool(self._data.get("system", {}).get("paper_mode", True))

    @property
    def log_level(self) -> str:
        return str(self._data.get("system", {}).get("log_level", "INFO"))
