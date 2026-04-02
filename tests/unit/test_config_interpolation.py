"""Unit tests for config environment interpolation semantics."""
from __future__ import annotations

from core.config import _interpolate


def test_interpolate_default_with_colon_dash_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    out = _interpolate("${POSTGRES_HOST:-localhost}")
    assert out == "localhost"


def test_interpolate_default_with_colon_dash_when_empty(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "")
    out = _interpolate("${POSTGRES_HOST:-localhost}")
    assert out == "localhost"


def test_interpolate_default_with_dash_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("REDIS_HOST", raising=False)
    out = _interpolate("${REDIS_HOST-localhost}")
    assert out == "localhost"


def test_interpolate_preserves_env_value(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_USER", "trader")
    out = _interpolate("${POSTGRES_USER:-fallback}")
    assert out == "trader"


def test_interpolate_without_default(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    out = _interpolate("${POSTGRES_PASSWORD}")
    assert out == "secret"
