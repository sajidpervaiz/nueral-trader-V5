#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "============================================="
echo "FULL REPOSITORY DEEP AUDIT"
echo "Repo: CTO-TEST-AI-trading-Bot"
echo "Date: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
echo "============================================="

echo "[1/6] Running Tier 2 gate (includes Tier 1 and Tier 0 gates)..."
bash scripts/deep_audit_tier2.sh

echo "[2/6] Running full regression suite..."
pytest -q

echo "[3/6] Verifying critical feature files exist..."
required_files=(
  "core/idempotency.py"
  "core/circuit_breaker.py"
  "core/retry.py"
  "execution/order_manager.py"
  "execution/smart_order_router.py"
  "execution/risk_manager.py"
  "engine/feature_engineering.py"
  "engine/model_trainer.py"
  "engine/ensemble_scorer.py"
  "data_ingestion/funding_feed.py"
  "interface/dashboard_api.py"
  "scripts/deep_audit_tier0.sh"
  "scripts/deep_audit_tier1.sh"
  "scripts/deep_audit_tier2.sh"
)

for f in "${required_files[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "MISSING: $f"
    exit 1
  fi
  echo "OK: $f"
done

echo "[4/6] Verifying API route retention..."
python - <<'PY'
from core.config import Config
from core.event_bus import EventBus
from interface.dashboard_api import build_app

app = build_app(Config("config/settings.yaml"), EventBus())
routes = {getattr(r, "path", None) for r in app.routes}
required = {
    "/health",
    "/orders/place",
    "/orders/open",
    "/orders/{order_id}",
  "/risk/limits",
  "/risk/check",
  "/risk/circuit-breaker",
}
missing = sorted(p for p in required if p not in routes)
if missing:
    raise SystemExit(f"MISSING_ROUTES: {missing}")
print(f"ROUTE_RETENTION_OK routes={len(routes)} verified={len(required)}")
PY

echo "[5/6] Guardrail: ensure no tracked feature file deletions in working tree..."
if git diff --name-status --diff-filter=D | grep -q .; then
  echo "DELETION_DETECTED"
  git diff --name-status --diff-filter=D
  exit 1
fi
echo "NO_TRACKED_DELETIONS"

echo "[6/6] Checking Tier 2 recall artifact..."
if [[ -f "TIER2_DEEP_AUDIT_RECALL.md" ]]; then
  echo "RECALL_AUDIT_OK TIER2_DEEP_AUDIT_RECALL.md"
else
  echo "RECALL_AUDIT_MISSING TIER2_DEEP_AUDIT_RECALL.md"
  exit 1
fi

echo "============================================="
echo "FULL REPOSITORY DEEP AUDIT: PASS"
echo "============================================="
