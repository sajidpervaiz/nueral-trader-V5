#!/usr/bin/env bash
set -euo pipefail

BRANCH="main"
REQUIRED_CONTEXT="Deep Audit Gate / Tier 0/1 + Full Audit"
APPLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch)
      BRANCH="${2:-}"
      shift 2
      ;;
    --context)
      REQUIRED_CONTEXT="${2:-}"
      shift 2
      ;;
    --apply)
      APPLY=1
      shift
      ;;
    --help|-h)
      cat <<'EOF'
Usage: bash scripts/enforce_branch_protection.sh [--apply] [--branch main] [--context "Deep Audit Gate / Tier 0/1 + Full Audit"]

Default mode is dry-run. Use --apply to write protection settings via GitHub API.
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: gh CLI is required"
  exit 1
fi

repo_full_name="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
owner="${repo_full_name%/*}"
repo="${repo_full_name#*/}"

payload="$(cat <<JSON
{
  "required_status_checks": {
    "strict": true,
    "contexts": ["${REQUIRED_CONTEXT}"]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": false,
    "required_approving_review_count": 1,
    "require_last_push_approval": true
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "block_creations": false,
  "required_conversation_resolution": true,
  "lock_branch": false
}
JSON
)"

echo "Repository: ${repo_full_name}"
echo "Target branch: ${BRANCH}"
echo "Required status context: ${REQUIRED_CONTEXT}"

if [[ ${APPLY} -eq 0 ]]; then
  echo "DRY-RUN: branch protection payload preview"
  echo "${payload}"
  exit 0
fi

echo "Applying branch protection..."
set +e
apply_output="$({
  gh api \
    --method PUT \
    -H "Accept: application/vnd.github+json" \
    "/repos/${owner}/${repo}/branches/${BRANCH}/protection" \
    --input - <<<"${payload}"
} 2>&1)"
apply_code=$?
set -e

if [[ ${apply_code} -ne 0 ]]; then
  echo "ERROR: Failed to apply branch protection"
  echo "${apply_output}"
  if echo "${apply_output}" | grep -q "Resource not accessible by integration"; then
    cat <<'EOF'

The current token cannot administer branch protection.
Use a PAT with repository administration rights, then run:

  gh auth login --hostname github.com
  bash scripts/enforce_branch_protection.sh --apply

Recommended PAT scopes for classic tokens:
- repo
- admin:repo_hook

For fine-grained tokens, grant:
- Repository administration: Read and write
- Contents: Read and write
EOF
  fi
  exit ${apply_code}
fi

echo "Branch protection applied."
echo "Verifying required contexts..."
gh api \
  -H "Accept: application/vnd.github+json" \
  "/repos/${owner}/${repo}/branches/${BRANCH}/protection/required_status_checks" \
  -q '.contexts'
