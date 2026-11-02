#!/bin/bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python 3 was not found. Install Python 3 and try again." >&2
  exit 1
fi

PORT="${STREAMLIT_SERVER_PORT:-8502}"
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
  echo "STREAMLIT_SERVER_PORT must be an integer from 1 to 65535." >&2
  exit 1
fi

if [ ! -x "$PROJECT_DIR/.venv/bin/python" ]; then
  "$PYTHON_BIN" -m venv "$PROJECT_DIR/.venv"
fi

VENV_PY="$PROJECT_DIR/.venv/bin/python"
"$VENV_PY" -m pip install --disable-pip-version-check --upgrade pip
"$VENV_PY" -m pip install --disable-pip-version-check -r "$PROJECT_DIR/requirements.txt"

echo "Starting the read-only Delta BTC perpetual terminal on http://localhost:$PORT"
exec "$VENV_PY" -m streamlit run "$PROJECT_DIR/app.py" --server.port "$PORT"
