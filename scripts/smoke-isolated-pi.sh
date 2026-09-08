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

resolve_lcm_entrypoint() {
  local lcm_dir="${LCM_DIR:-${REPO_DIR}/../local-context-manager}"
  if [[ -f "${lcm_dir}/src/index.ts" ]]; then
    printf '%s\n' "${lcm_dir}/src/index.ts"
    return 0
  fi
  if [[ -f "${lcm_dir}/index.ts" ]]; then
    printf '%s\n' "${lcm_dir}/index.ts"
    return 0
  fi
  echo "Error: local-context-manager not found at '${lcm_dir}'." >&2
  echo "Set LCM_DIR=<path-to-local-context-manager> or checkout sibling repository." >&2
  return 1
}

run_scenario_lcm() {
  local test_dir
  test_dir="$(make_temp_agent_dir)"
  trap 'rm -rf "${test_dir}"' EXIT

  local lcm_entry
  lcm_entry="$(resolve_lcm_entrypoint)"

  echo "Scenario 2: safe-agent-team + local-context-manager load smoke"
  echo "  PI_CODING_AGENT_DIR=${test_dir}"
  echo "  LCM_DIR=${LCM_DIR:-${REPO_DIR}/../local-context-manager}"
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

  local lcm_entry
  lcm_entry="$(resolve_lcm_entrypoint)"

  # Pi's model registry and auth stores are deliberately copied into the
  # disposable directory. The smoke must not read or mutate the normal agent
  # directory while it creates a real managed child.
  local source_agent_dir="${PI_SMOKE_SOURCE_AGENT_DIR:-${PI_CODING_AGENT_DIR:-${HOME}/.pi/agent}}"
  local auth_file="${PI_SMOKE_AUTH_FILE:-${source_agent_dir}/auth.json}"
  local models_file="${PI_SMOKE_MODELS_FILE:-${source_agent_dir}/models.json}"
  if [[ ! -f "${auth_file}" || ! -f "${models_file}" ]]; then
    echo "Model-backed integration smoke needs auth.json and models.json fixtures." >&2
    echo "Set PI_SMOKE_SOURCE_AGENT_DIR, or PI_SMOKE_AUTH_FILE and PI_SMOKE_MODELS_FILE." >&2
    exit 1
  fi
  cp "${auth_file}" "${test_dir}/auth.json"
  cp "${models_file}" "${test_dir}/models.json"
  chmod 600 "${test_dir}/auth.json" "${test_dir}/models.json"

  echo "Scenario 3: Model-backed integration smoke with model=${PI_SMOKE_MODEL}"
  echo "  PI_CODING_AGENT_DIR=${test_dir}"
  local output_file="${test_dir}/pi-output.txt"
  local prompt="${PI_SMOKE_PROMPT:-Use agent_spawn exactly once with taskDescription 'Integration smoke worker: call agent_task with action=complete and result summary smoke-child-complete, then report completion to the parent.' Wait for that child task to reach a terminal completed state by using agent_status; do not do the worker task yourself. After the child is completed and no child work remains, report the exact marker SMOKE_ROOT_QUIESCENT.}"
  if ! PI_CODING_AGENT_DIR="${test_dir}" pi \
    --no-extensions \
    -e "${REPO_DIR}/index.ts" \
    -e "${lcm_entry}" \
    --model "${PI_SMOKE_MODEL}" \
    -p "${prompt}" >"${output_file}" 2>&1; then
    cat "${output_file}" >&2
    exit 1
  fi

  if ! rg -q "SMOKE_ROOT_QUIESCENT" "${output_file}"; then
    echo "Integration smoke did not report the quiescent completion marker." >&2
    cat "${output_file}" >&2
    exit 1
  fi

  local journal
  journal="$(find "${test_dir}/safe-agents" -name events.jsonl -type f -print -quit 2>/dev/null || true)"
  if [[ -z "${journal}" ]]; then
    echo "Integration smoke did not leave a safe-agent journal under the disposable agent directory." >&2
    exit 1
  fi
  if ! rg -q 'lcm-embedded' "${journal}"; then
    echo "Integration smoke did not observe a managed child in lcm-embedded mode." >&2
    exit 1
  fi
  if ! rg -q 'smoke-child-complete|"status":"completed"' "${journal}"; then
    echo "Integration smoke did not observe a completed managed-child task." >&2
    exit 1
  fi
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
