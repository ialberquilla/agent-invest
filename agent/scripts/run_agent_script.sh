#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  printf '%s\n' "Usage: bash agent/scripts/run_agent_script.sh <script> [args...]" >&2
  exit 2
fi

script_name="$1"
shift

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
timeout_ms="${AGENT_SCRIPT_TIMEOUT_MS:-30000}"

if ! [[ "$timeout_ms" =~ ^[1-9][0-9]*$ ]]; then
  printf '%s\n' "Invalid AGENT_SCRIPT_TIMEOUT_MS value: $timeout_ms" >&2
  exit 2
fi

timeout_seconds=$(((timeout_ms + 999) / 1000))

set +e
timeout --signal=KILL "${timeout_seconds}s" \
  uv run --project "$script_dir" python -m "agent_invest_scripts.${script_name}" "$@"
exit_code=$?
set -e

if [ "$exit_code" -eq 124 ] || [ "$exit_code" -eq 137 ]; then
  printf '%s\n' "AGENT_SCRIPT_TIMEOUT: ${script_name} exceeded ${timeout_ms}ms" >&2
  exit 124
fi

exit "$exit_code"
