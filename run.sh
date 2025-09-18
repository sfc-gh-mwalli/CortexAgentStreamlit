#!/usr/bin/env bash
set -euo pipefail

# Location of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Install deps (idempotent)
python -m pip install -r requirements.txt

# Run Streamlit (configure Account URL and Token in the sidebar or via st.secrets)
exec streamlit run app.py


