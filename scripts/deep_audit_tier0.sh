#!/bin/bash

################################################################################
# TIER 0 DEEP AUDIT (ONE-COMMAND)
# Fast executive validation for code + API wiring + test integrity.
################################################################################

set -euo pipefail

PROJECT_ROOT="/workspaces/CTO-TEST-AI-trading-Bot"
cd "$PROJECT_ROOT"

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

echo "============================================="
echo "TIER 0 DEEP AUDIT"
echo "Repo: CTO-TEST-AI-trading-Bot"
echo "Date: $(date -u +'%Y-%m-%d %H:%M:%S UTC')"
echo "============================================="

echo "[1/3] Running unit + integration tests..."
pytest -q tests/unit tests/integration

echo "[2/3] Running API build and route sanity checks..."
python3 << 'PYEOF'
from pathlib import Path

from core.config import Config
from core.event_bus import EventBus
from execution.order_manager import OrderManager
from execution.risk_manager import RiskManager
from core.circuit_breaker import CircuitBreaker
from interface.dashboard_api import build_app

repo = Path('/workspaces/CTO-TEST-AI-trading-Bot')
cfg = Config(config_path=repo / 'config/settings.yaml')
bus = EventBus()
risk = RiskManager(cfg, bus)
breaker = CircuitBreaker()
order_mgr = OrderManager(cfg, bus, breaker)
app = build_app(cfg, bus, risk_manager=risk, order_manager=order_mgr)
assert app is not None, 'FastAPI app failed to build'

routes = {r.path for r in app.routes}
required = {
    '/health',
    '/orders/',
    '/orders/place',
    '/positions/',
    '/positions/summary',
    '/risk/limits',
    '/risk/circuit-breaker',
}
missing = sorted(required - routes)
if missing:
    raise AssertionError(f'Missing required Tier 0 routes: {missing}')

print(f'APP_OK routes={len(routes)} required_routes_verified={len(required)}')
PYEOF

echo "[3/3] Checking recall audit artifact..."
if [[ -f "TIER0_DEEP_AUDIT_RECALL.md" ]]; then
  echo "RECALL_AUDIT_OK TIER0_DEEP_AUDIT_RECALL.md"
else
  echo "RECALL_AUDIT_MISSING"
  exit 1
fi

echo "============================================="
echo "TIER 0 DEEP AUDIT: PASS"
echo "============================================="
