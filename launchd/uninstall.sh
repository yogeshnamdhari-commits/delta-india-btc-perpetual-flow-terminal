#!/bin/bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This uninstaller is for macOS only." >&2
  exit 1
fi

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AGENT_DIR="$HOME/Library/LaunchAgents"
MAIN_LABEL="com.yogesh.delta-btc-terminal"
WATCHDOG_LABEL="com.yogesh.delta-btc-terminal-watchdog"
MAIN_PLIST="$AGENT_DIR/$MAIN_LABEL.plist"
WATCHDOG_PLIST="$AGENT_DIR/$WATCHDOG_LABEL.plist"
DOMAIN="gui/$(id -u)"

# Stop only these two named services. Preserve captured data, logs and .venv.
for label in "$WATCHDOG_LABEL" "$MAIN_LABEL"; do
  if launchctl print "$DOMAIN/$label" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$label"
  fi
done

for plist in "$MAIN_PLIST" "$WATCHDOG_PLIST"; do
  if [[ -f "$plist" ]]; then
    existing_dir="$(/usr/libexec/PlistBuddy -c 'Print :WorkingDirectory' "$plist" 2>/dev/null || true)"
    if [[ -n "$existing_dir" && "$existing_dir" == "$PROJECT_DIR" ]]; then
      rm "$plist"
    else
      echo "Kept $plist because it does not identify this project checkout."
    fi
  fi
done

echo "LaunchAgent services stopped. Captured data, logs, backups and .venv were not deleted."
