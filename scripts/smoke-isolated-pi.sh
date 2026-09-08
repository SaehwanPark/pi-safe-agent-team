#!/usr/bin/env bash
set -euo pipefail

# Smoke test for isolated Pi execution of pi-safe-agent-team and companions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MODE="${1:-load}"

if ! command -v pi >/dev/null 2>&1; then
  echo "pi binary not found on PATH; skipping isolated Pi smoke."
  exit 0
fi

make_temp_agent_dir() {
  local base_dir="/tmp"
  if [[ "${OSTYPE:-}" != "darwin"* ]]; then
    base_dir="${TMPDIR:-/tmp}"
  fi
  mktemp -d "${base_dir%/}/pi-smoke-XXXXXX"
}

run_scenario_1() {
  local test_dir
  test_dir="$(make_temp_agent_dir)"
  trap 'rm -rf "${test_dir}"' EXIT
  echo "Scenario 1: safe-agent-team load smoke"
  echo "  PI_CODING_AGENT_DIR=${test_dir}"
  PI_CODING_AGENT_DIR="${test_dir}" pi \
    --no-extensions \
    -e "${REPO_DIR}/index.ts" \
    --list-models >/dev/null
  rm -rf "${test_dir}"
  trap - EXIT
}

run_scenario_lcm() {
  local test_dir
  test_dir="$(make_temp_agent_dir)"
  trap 'rm -rf "${test_dir}"' EXIT

  local lcm_dir="${LCM_DIR:-${REPO_DIR}/../local-context-manager}"
  local lcm_entry=""
  if [[ -f "${lcm_dir}/src/index.ts" ]]; then
    lcm_entry="${lcm_dir}/src/index.ts"
  elif [[ -f "${lcm_dir}/index.ts" ]]; then
    lcm_entry="${lcm_dir}/index.ts"
  else
    echo "Error: local-context-manager not found at '${lcm_dir}'." >&2
    echo "Set LCM_DIR=<path-to-local-context-manager> or checkout sibling repository." >&2
    exit 1
  fi

  echo "Scenario 2: safe-agent-team + local-context-manager load smoke"
  echo "  PI_CODING_AGENT_DIR=${test_dir}"
  echo "  LCM_DIR=${lcm_dir}"
  PI_CODING_AGENT_DIR="${test_dir}" pi \
    --no-extensions \
    -e "${REPO_DIR}/index.ts" \
    -e "${lcm_entry}" \
    --list-models >/dev/null
  rm -rf "${test_dir}"
  trap - EXIT
}

run_scenario_integration() {
  if [[ -z "${PI_SMOKE_MODEL:-}" ]]; then
    echo "PI_SMOKE_MODEL not set; skipping model-backed integration smoke."
    echo "To run integration smoke: PI_SMOKE_MODEL=<provider/model> npm run smoke:pi:integration"
    return 0
  fi

  local test_dir
  test_dir="$(make_temp_agent_dir)"
  trap 'rm -rf "${test_dir}"' EXIT

  echo "Scenario 3: Model-backed integration smoke with model=${PI_SMOKE_MODEL}"
  echo "  PI_CODING_AGENT_DIR=${test_dir}"
  PI_CODING_AGENT_DIR="${test_dir}" pi \
    --no-extensions \
    -e "${REPO_DIR}/index.ts" \
    --model "${PI_SMOKE_MODEL}" \
    -p "echo 'pi-safe-agent-team smoke test'" >/dev/null
  rm -rf "${test_dir}"
  trap - EXIT
}

case "${MODE}" in
  load)
    run_scenario_1
    ;;
  with-lcm|lcm)
    run_scenario_1
    run_scenario_lcm
    ;;
  integration)
    run_scenario_1
    run_scenario_integration
    ;;
  all)
    run_scenario_1
    run_scenario_lcm
    run_scenario_integration
    ;;
  *)
    echo "Unknown mode: ${MODE}. Supported modes: load, with-lcm, integration, all" >&2
    exit 1
    ;;
esac

echo "Isolated Pi smoke tests completed successfully."
