#!/usr/bin/env bash
set -euo pipefail
if git diff --cached --name-only | grep -E '(^|/)\.streamlit/secrets\.toml$' >/dev/null; then
  echo "ERROR: Do not commit .streamlit/secrets.toml" >&2
  exit 1
fi
exit 0
