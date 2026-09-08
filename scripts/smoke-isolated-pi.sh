#!/usr/bin/env bash
set -euo pipefail

# Smoke test for isolated Pi execution of pi-safe-agent-team and companions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if ! command -v pi >/dev/null 2>&1; then
  echo "pi binary not found on PATH; skipping isolated Pi smoke."
  exit 0
fi

TEST_PI_DIR="$(mktemp -d)"
trap 'rm -rf "${TEST_PI_DIR}"' EXIT

echo "Running isolated Pi smoke test under disposable PI_CODING_AGENT_DIR=${TEST_PI_DIR}"

# 1. safe-agent-team alone with explicit -e and no extensions
echo "Scenario 1: safe-agent-team alone"
PI_CODING_AGENT_DIR="${TEST_PI_DIR}" pi \
  --no-extensions \
  -e "${REPO_DIR}/index.ts" \
  --list-models >/dev/null

# 2. Companion integration if local-context-manager is available
LCM_DIR="${LCM_DIR:-/Users/saehwan/repos/local-context-manager}"
if [[ -f "${LCM_DIR}/src/index.ts" ]]; then
  echo "Scenario 2: safe-agent-team + local-context-manager"
  PI_CODING_AGENT_DIR="${TEST_PI_DIR}" pi \
    --no-extensions \
    -e "${REPO_DIR}/index.ts" \
    -e "${LCM_DIR}/src/index.ts" \
    --list-models >/dev/null
fi

echo "Isolated Pi smoke tests completed successfully."
