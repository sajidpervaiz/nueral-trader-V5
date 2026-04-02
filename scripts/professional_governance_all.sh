#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

APPLY_BRANCH_PROTECTION=0
TARGET_BRANCH="main"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply-branch-protection)
      APPLY_BRANCH_PROTECTION=1
      shift
      ;;
    --branch)
      TARGET_BRANCH="${2:-main}"
      shift 2
      ;;
    --help|-h)
      cat <<'EOF'
Usage: bash scripts/professional_governance_all.sh [--apply-branch-protection] [--branch main]

Runs full production governance checks:
1) Full repository deep audit
2) CI workflow presence checks
3) Branch protection dry-run or apply
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

echo "============================================="
echo "PROFESSIONAL GOVERNANCE: ALL CHECKS"
echo "Repo: CTO-TEST-AI-trading-Bot"
echo "Date: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
echo "============================================="

echo "[1/3] Running full deep audit gate..."
bash scripts/deep_audit_full.sh

echo "[2/3] Verifying CI governance workflows..."
required_workflows=(
  ".github/workflows/deep-audit.yml"
  ".github/workflows/audit-failure-comment.yml"
)
for wf in "${required_workflows[@]}"; do
  if [[ ! -f "$wf" ]]; then
    echo "MISSING_WORKFLOW: $wf"
    exit 1
  fi
  echo "OK: $wf"
done

echo "[3/3] Branch protection governance..."
if [[ ${APPLY_BRANCH_PROTECTION} -eq 1 ]]; then
  bash scripts/enforce_branch_protection.sh --apply --branch "$TARGET_BRANCH"
else
  bash scripts/enforce_branch_protection.sh --branch "$TARGET_BRANCH"
fi

echo "============================================="
echo "PROFESSIONAL GOVERNANCE: COMPLETE"
echo "============================================="
bash scripts/deep_audit_full.sh

echo "[2/3] Verifying CI governance workflows..."
required_workflows=(
  ".github/workflows/deep-audit.yml"
  ".github/workflows/audit-failure-comment.yml"
)
for wf in "${required_workflows[@]}"; do
  if [[ ! -f "$wf" ]]; then
    echo "MISSING_WORKFLOW: $wf"
    exit 1
  fi
  echo "OK: $wf"
done

echo "[3/3] Branch protection governance..."
if [[ ${APPLY_BRANCH_PROTECTION} -eq 1 ]]; then
  bash scripts/enforce_branch_protection.sh --apply --branch "$TARGET_BRANCH"
else
  bash scripts/enforce_branch_protection.sh --branch "$TARGET_BRANCH"
fi

echo "============================================="
echo "PROFESSIONAL GOVERNANCE: COMPLETE"
echo "============================================="
