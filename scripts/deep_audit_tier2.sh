#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "============================================="
echo "TIER 2 DEEP AUDIT"
echo "Repo: CTO-TEST-AI-trading-Bot"
echo "Date: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
echo "============================================="

echo "[1/6] Verifying Tier 1 baseline gate..."
bash scripts/deep_audit_tier1.sh

echo "[2/6] Static file existence sanity checks..."
required_files=(
  "rust/Cargo.toml"
  "rust/risk-engine/src/lib.rs"
  "rust/order-matcher/src/lib.rs"
  "rust/tick-parser/src/lib.rs"
  "rust/gateway/src/lib.rs"
  "docker-compose.prod.yml"
  "monitoring/prometheus.yml"
  "monitoring/alertmanager.yml"
  "monitoring/grafana-datasources.yml"
  "monitoring/grafana-dashboards/tier2-overview.json"
)

for f in "${required_files[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "MISSING: $f"
    exit 1
  fi
  echo "OK: $f"
done

echo "[3/6] Validating production infrastructure service wiring..."
python - <<'PY'
from pathlib import Path

compose_text = Path("docker-compose.prod.yml").read_text(encoding="utf-8")
required_services = [
    "redis:",
    "nats:",
    "postgres:",
    "prometheus:",
    "grafana:",
    "alertmanager:",
    "rust-gateway:",
    "trading-engine:",
]
missing = [s for s in required_services if s not in compose_text]
if missing:
    raise SystemExit(f"MISSING_SERVICES: {missing}")

prom_text = Path("monitoring/prometheus.yml").read_text(encoding="utf-8")
for key in ["job_name: 'gateway'", "job_name: 'trading-engine'", "job_name: 'dex-layer'"]:
    if key not in prom_text:
        raise SystemExit(f"PROM_MISSING_JOB: {key}")

alert_text = Path("monitoring/alertmanager.yml").read_text(encoding="utf-8")
for key in ["critical-alerts", "warning-alerts", "trading-alerts"]:
    if key not in alert_text:
        raise SystemExit(f"ALERT_MISSING_RECEIVER: {key}")

print("INFRA_WIRING_OK compose_services=8 prometheus_jobs=3 alert_receivers=3")
PY

echo "[4/6] Checking Rust workspace membership..."
python - <<'PY'
from pathlib import Path

cargo_text = Path("rust/Cargo.toml").read_text(encoding="utf-8")
required_members = [
    '"tick-parser"',
    '"order-matcher"',
    '"risk-engine"',
    '"gateway"',
    '"py-bindings"',
]
missing = [m for m in required_members if m not in cargo_text]
if missing:
    raise SystemExit(f"RUST_WORKSPACE_MEMBER_MISSING: {missing}")
print("RUST_WORKSPACE_OK members=5")
PY

echo "[5/6] Running strict Rust compile check via Docker rust-builder target..."
docker build --target rust-builder -t cto-tier2-rustcheck .

echo "[6/6] Checking Tier 1 and Tier 2 recall artifacts..."
for f in TIER1_DEEP_AUDIT_RECALL.md TIER2_DEEP_AUDIT_RECALL.md; do
  if [[ ! -f "$f" ]]; then
    echo "RECALL_AUDIT_MISSING $f"
    exit 1
  fi
  echo "RECALL_AUDIT_OK $f"
done

echo "============================================="
echo "TIER 2 DEEP AUDIT: PASS"
echo "============================================="
