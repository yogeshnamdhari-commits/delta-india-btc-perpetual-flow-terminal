#!/bin/bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This installer is for macOS only." >&2
  exit 1
fi

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHD_DIR="$PROJECT_DIR/launchd"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PORT="${STREAMLIT_SERVER_PORT:-8502}"
MAIN_LABEL="com.yogesh.delta-btc-terminal"
WATCHDOG_LABEL="com.yogesh.delta-btc-terminal-watchdog"
AGENT_DIR="$HOME/Library/LaunchAgents"
LOG_DIR="$HOME/Library/Logs/DeltaBTCTerminal"
SUPPORT_DIR="$HOME/Library/Application Support/DeltaBTCTerminal"
MAIN_PLIST="$AGENT_DIR/$MAIN_LABEL.plist"
WATCHDOG_PLIST="$AGENT_DIR/$WATCHDOG_LABEL.plist"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python 3 not found. Install Python 3 first." >&2
  exit 1
fi
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
  echo "STREAMLIT_SERVER_PORT must be an integer from 1 to 65535." >&2
  exit 1
fi

mkdir -p "$PROJECT_DIR/.venv" "$AGENT_DIR" "$LOG_DIR" "$SUPPORT_DIR" "$HOME/Downloads"
if [[ ! -x "$PROJECT_DIR/.venv/bin/python" ]]; then
  "$PYTHON_BIN" -m venv "$PROJECT_DIR/.venv"
fi
VENV_PY="$PROJECT_DIR/.venv/bin/python"
if ! "$VENV_PY" -c 'import streamlit, pandas, plotly, websocket, requests' >/dev/null 2>&1; then
  echo "Installing missing application dependencies once..."
  "$VENV_PY" -m pip install --disable-pip-version-check -r "$PROJECT_DIR/requirements.txt"
fi

uid="$(id -u)"
gui_domain="gui/$uid"

# Refuse to replace a same-label agent that belongs to a different checkout.
for plist in "$MAIN_PLIST" "$WATCHDOG_PLIST"; do
  if [[ -f "$plist" ]]; then
    existing_dir="$(/usr/libexec/PlistBuddy -c 'Print :WorkingDirectory' "$plist" 2>/dev/null || true)"
    if [[ -n "$existing_dir" && "$existing_dir" != "$PROJECT_DIR" ]]; then
      echo "Refusing to replace $plist: it belongs to $existing_dir" >&2
      exit 1
    fi
  fi
done

# If our own service is currently loaded, stop it before checking the port.
if launchctl print "$gui_domain/$MAIN_LABEL" >/dev/null 2>&1; then
  launchctl bootout "$gui_domain/$MAIN_LABEL"
  for _ in {1..20}; do
    if ! lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then break; fi
    sleep 1
  done
fi

if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port $PORT is already occupied. Stop the existing Streamlit process with Ctrl+C, then run this installer again. No process was killed." >&2
  exit 1
fi

stamp="$(date +%Y%m%d%H%M%S)"
for plist in "$MAIN_PLIST" "$WATCHDOG_PLIST"; do
  if [[ -f "$plist" ]]; then
    cp "$plist" "$plist.backup.$stamp"
    rm "$plist"
  fi
done

export PROJECT_DIR VENV_PY PORT MAIN_LABEL WATCHDOG_LABEL MAIN_PLIST WATCHDOG_PLIST LOG_DIR SUPPORT_DIR HOME
"$VENV_PY" - <<'PY'
import os
import plistlib
from pathlib import Path

project = Path(os.environ["PROJECT_DIR"])
venv_python = os.environ["VENV_PY"]
port = os.environ["PORT"]
home = Path(os.environ["HOME"])
log_dir = Path(os.environ["LOG_DIR"])
support_dir = Path(os.environ["SUPPORT_DIR"])
main_label = os.environ["MAIN_LABEL"]
watchdog_label = os.environ["WATCHDOG_LABEL"]
main_plist = Path(os.environ["MAIN_PLIST"])
watchdog_plist = Path(os.environ["WATCHDOG_PLIST"])

env = {
    "HOME": str(home),
    "PATH": f"{project / '.venv' / 'bin'}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
    "PYTHONUNBUFFERED": "1",
    "STREAMLIT_SERVER_PORT": port,
    "DELTA_EVENT_LOG_FILE": os.environ.get("DELTA_EVENT_LOG_FILE", str(home / "Downloads/delta_feed_events.jsonl")),
    "DELTA_EVENT_LOG_FILE_FULL": os.environ.get("DELTA_EVENT_LOG_FILE_FULL", str(home / "Downloads/delta_feed_events_full.jsonl")),
    "DELTA_DECISION_LOG_FILE": os.environ.get("DELTA_DECISION_LOG_FILE", str(home / "Downloads/signal_decisions.jsonl")),
    "DELTA_SHADOW_LOG_FILE": os.environ.get("DELTA_SHADOW_LOG_FILE", str(home / "Downloads/delta-live-recovery/shadow_trades.jsonl")),
    "DELTA_SHADOW_DECISION_LOG": os.environ.get("DELTA_SHADOW_DECISION_LOG", str(home / "Downloads/delta-live-recovery/shadow_decisions.jsonl")),
    "DELTA_FEED_HEALTH_FILE": str(support_dir / "feed_health.json"),
    "DELTA_WATCHDOG_STATE_FILE": str(support_dir / "watchdog_state.json"),
    "DELTA_DASHBOARD_HEALTH_URL": f"http://127.0.0.1:{port}/_stcore/health",
    "DELTA_MAIN_AGENT_LABEL": main_label,
    "DELTA_WATCHDOG_FAILURE_THRESHOLD": "3",
    "DELTA_WATCHDOG_RESTART_COOLDOWN_SECONDS": "600",
}

main = {
    "Label": main_label,
    "ProgramArguments": ["/bin/bash", str(project / "launchd/run_dashboard.sh")],
    "WorkingDirectory": str(project),
    "EnvironmentVariables": env,
    "RunAtLoad": True,
    "KeepAlive": True,
    "ThrottleInterval": 15,
    "StandardOutPath": str(log_dir / "streamlit.stdout.log"),
    "StandardErrorPath": str(log_dir / "streamlit.stderr.log"),
}

watchdog = {
    "Label": watchdog_label,
    "ProgramArguments": [venv_python, str(project / "launchd/watchdog.py")],
    "WorkingDirectory": str(project),
    "EnvironmentVariables": env,
    "RunAtLoad": True,
    "StartInterval": 60,
    "ThrottleInterval": 10,
    "StandardOutPath": str(log_dir / "watchdog.stdout.log"),
    "StandardErrorPath": str(log_dir / "watchdog.stderr.log"),
}

for path, payload in ((main_plist, main), (watchdog_plist, watchdog)):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        plistlib.dump(payload, handle, fmt=plistlib.FMT_XML, sort_keys=True)
PY

chmod 644 "$MAIN_PLIST" "$WATCHDOG_PLIST"
plutil -lint "$MAIN_PLIST"
plutil -lint "$WATCHDOG_PLIST"

launchctl bootstrap "$gui_domain" "$MAIN_PLIST"
launchctl bootstrap "$gui_domain" "$WATCHDOG_PLIST"

echo "LaunchAgents installed. Waiting for the local Streamlit health endpoint..."
ready=0
for _ in {1..45}; do
  if curl --fail --silent "http://127.0.0.1:$PORT/_stcore/health" >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "$ready" -ne 1 ]]; then
  echo "Dashboard did not become healthy within 45 seconds. Agents remain loaded for diagnosis." >&2
  echo "--- Streamlit stderr ---" >&2
  tail -n 60 "$LOG_DIR/streamlit.stderr.log" 2>/dev/null || true
  echo "--- Watchdog stderr ---" >&2
  tail -n 60 "$LOG_DIR/watchdog.stderr.log" 2>/dev/null || true
  exit 1
fi

echo "SUCCESS: dashboard health endpoint returned HTTP 200 on port $PORT."
echo "Main service: launchctl print $gui_domain/$MAIN_LABEL"
echo "Watchdog:     launchctl print $gui_domain/$WATCHDOG_LABEL"
echo "Logs:         $LOG_DIR"
