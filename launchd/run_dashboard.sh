#!/bin/bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python"
PORT="${STREAMLIT_SERVER_PORT:-8502}"

if [[ ! -x "$PYTHON" ]]; then
  echo "Missing project interpreter: $PYTHON. Run launchd/install.sh first." >&2
  exit 78
fi
if [[ ! -x /usr/bin/caffeinate ]]; then
  echo "macOS caffeinate utility not found at /usr/bin/caffeinate." >&2
  exit 69
fi
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
  echo "Invalid STREAMLIT_SERVER_PORT: $PORT" >&2
  exit 64
fi

cd "$PROJECT_DIR"
echo "Starting read-only Delta BTC terminal on 127.0.0.1:$PORT"
# -i inhibits idle system sleep; -s prevents system sleep while on AC power.
# Closing a MacBook lid can still put the machine to sleep.
exec /usr/bin/caffeinate -is "$PYTHON" -m streamlit run "$PROJECT_DIR/app.py" \
  --server.port "$PORT" \
  --server.address 127.0.0.1 \
  --server.headless true \
  --browser.gatherUsageStats false
