# macOS LaunchAgent deployment

This is a **per-user macOS LaunchAgent**, not a system LaunchDaemon. It starts when the user logs in and runs within that user session.

## Install

1. Stop any manually started Streamlit process using port 8502 (Ctrl+C in the terminal that started it). The installer will not kill an unrelated process.
2. From the repository root run:

   ```bash
   ./launchd/install.sh
   ```

The installer creates/reuses `.venv`, installs app requirements only if imports are missing, writes two plists under `~/Library/LaunchAgents`, validates them with `plutil`, loads both services and waits up to 45 seconds for the local Streamlit health endpoint to return HTTP 200.

## Services

- `com.yogesh.delta-btc-terminal`: runs the read-only Streamlit app and in-process Delta public WebSocket collector. `RunAtLoad` and `KeepAlive` start it at agent load/login and relaunch it after an exit. The wrapper uses absolute paths and `caffeinate -is` while the app is running.
- `com.yogesh.delta-btc-terminal-watchdog`: runs every 60 seconds. It checks the Streamlit HTTP health endpoint and an atomic heartbeat written by the DeltaFeed health thread. Three consecutive watchdog failures trigger `launchctl kickstart -k`, subject to a 10-minute restart cooldown.

The watchdog intentionally does **not** restart a healthy collector just because Delta or the network is disconnected. The in-process WebSocket reconnect loop owns upstream recovery; signal freshness/integrity gates must continue to block decisions while data is stale or unsynchronized.

## Status and logs

```bash
launchctl print gui/$(id -u)/com.yogesh.delta-btc-terminal
launchctl print gui/$(id -u)/com.yogesh.delta-btc-terminal-watchdog
tail -f ~/Library/Logs/DeltaBTCTerminal/streamlit.stdout.log
tail -f ~/Library/Logs/DeltaBTCTerminal/streamlit.stderr.log
tail -f ~/Library/Logs/DeltaBTCTerminal/watchdog.stdout.log
```

Health heartbeat: `~/Library/Application Support/DeltaBTCTerminal/feed_health.json`.

The event and decision logs default to `~/Downloads`; their paths can be overridden in the shell environment when installing. Do not put these logs inside the Git repository or commit them.

## Uninstall

```bash
./launchd/uninstall.sh
```

This stops and removes only this project's two LaunchAgent plists. It does not remove `.venv`, captured data, logs or backups.

## Operational limits

- A LaunchAgent starts at user login, not before login. It does not run in a logged-out GUI session.
- Keep the Mac powered, online, awake and logged in. `caffeinate -i` prevents idle sleep; `-s` asserts against system sleep while on AC power. A MacBook lid close, power loss, OS failure, disk exhaustion or network/exchange outage can still interrupt collection.
- `KeepAlive` can recover from process exit; the watchdog can recover from a dead feed worker or unresponsive HTTP app. Neither proves market-data continuity or prevents all possible failures.
- Runtime event logs are append-only JSONL and the dashboard's CVD/order-book sampling caches remain in memory. A process restart preserves raw captured log records but resets in-memory session-derived analytics. Recovery/replay of all historical trades into the live signal state is a separate data-persistence milestone.
- Passing tests or the watchdog checks does not establish signal profitability. The system remains read-only, and live trading remains not approved until the separate historical and prospective validation gates pass.
