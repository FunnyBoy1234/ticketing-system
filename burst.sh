#!/usr/bin/env bash
# One-command on-sale stampede against a running deployment to test the system.
#
#   ./burst.sh https://your-app.onrender.com                # ~20k requests, prints outcome table + checks
#   ADMIN_KEY=... ./burst.sh <BASE_URL> --requests 5000     # any scripts/burst.py flag works
#
# Exit code 0 only if every correctness check passed.
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "usage: ./burst.sh <BASE_URL> [burst.py flags...]   (ADMIN_KEY env var, default dev-admin-key)" >&2
  exit 2
fi
BASE_URL="$1"; shift
cd "$(dirname "$0")"

if command -v uv >/dev/null 2>&1; then
  exec uv run --quiet scripts/burst.py "$BASE_URL" "$@"
fi

if [ ! -x .venv-burst/bin/python ]; then
  python3 -m venv .venv-burst
  .venv-burst/bin/pip install --quiet "aiohttp>=3.9"
fi
exec .venv-burst/bin/python scripts/burst.py "$BASE_URL" "$@"
