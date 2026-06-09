#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
  set -- node agent/dist/server.js
fi

max_attempts="${MIGRATION_MAX_ATTEMPTS:-30}"
retry_delay="${MIGRATION_RETRY_DELAY_SECONDS:-2}"
attempt=1

while true; do
  migration_log="$(mktemp)"
  if pnpm --filter @pond3r-portfolio/agent db:migrate >"${migration_log}" 2>&1; then
    cat "${migration_log}"
    rm -f "${migration_log}"
    break
  fi

  cat "${migration_log}" >&2
  rm -f "${migration_log}"

  if [ "${attempt}" -ge "${max_attempts}" ]; then
    echo "Migrations failed after ${attempt} attempt(s)." >&2
    exit 1
  fi

  echo "Migration attempt ${attempt} failed. Retrying in ${retry_delay}s..." >&2
  attempt=$((attempt + 1))
  sleep "${retry_delay}"
done

exec "$@"
