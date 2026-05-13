#!/usr/bin/env sh
set -eu

cutoff="$(${PYTHON:-python3} - <<'PY'
from datetime import datetime, timedelta, timezone

print((datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ'))
PY
)"

exec uv "$@" --exclude-newer "${cutoff}"
