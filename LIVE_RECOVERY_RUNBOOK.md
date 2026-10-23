# Live disconnect/reconnect recovery acceptance

This is a **read-only, real-feed acceptance procedure** for the running Streamlit application. It causes a brief disconnect of the public market-data WebSocket; it does not place orders. Do not run it while depending on the feed for any live decisions.

## What this validates

The procedure captures one explicit `Trigger reconnect test` in the running app, then audits the actual JSONL evidence for:
- a diagnostic trigger, connection-close event, and subsequent reconnect;
- a `BLOCKED` decision during the failure window;
- no non-`BLOCKED` decisions before a checksum-bearing, CRC-verified order-book update;
- a post-reconnect decision with explicit fresh ticker, order-book, and trades channels, verified book state, no unresolved sequence gap, and CRC state;
- recovery to a healthy decision within the specified time limit.

A PASS does **not** prove strategy profitability and does not authorize order execution. The audit checks the recorded decision and feed-health state; visually confirm in the browser that the displayed decision equals the most recent persisted record. Keep screen/log evidence private and redact local paths before sharing.

## 1. Start a clean, isolated diagnostic capture on the Mac

Use the **actual application working copy**, not the separate Git-sync mirror. For the configured setup, that is normally `~/Downloads/delta-india-btc-perpetual-flow-terminal`; set `APP_DIR` below if yours differs:

```bash
APP_DIR="${APP_DIR:-$HOME/Downloads/delta-india-btc-perpetual-flow-terminal}"
cd "$APP_DIR"
test -f app.py && test -x .venv/bin/python || { echo "App working copy or .venv not found: $APP_DIR"; exit 1; }
test -f audit_live_recovery.py || { echo "Recovery auditor is not synced into this app working copy yet"; exit 1; }
mkdir -p "$HOME/Downloads/delta-live-recovery"

export DELTA_ENABLE_DIAGNOSTIC_RECONNECT=1
export DELTA_EVENT_LOG_FILE="$HOME/Downloads/delta-live-recovery/events.jsonl"
export DELTA_EVENT_LOG_FILE_FULL="$HOME/Downloads/delta-live-recovery/events_full.jsonl"
export DELTA_DECISION_LOG_FILE="$HOME/Downloads/delta-live-recovery/decisions.jsonl"

.venv/bin/python -m streamlit run app.py --server.port 8503
```

This capture is outside the watched repository, so raw trade logs and decisions will not be auto-synced to GitHub. Keep these files local; event logs contain captured market data.

Wait until the dashboard shows recent ticker/trade activity and a synchronized book. A checksum must be reported as verified, not unavailable, before attempting to validate verified recovery. Confirm order execution is disabled.

## 2. Trigger exactly one reconnect

In the terminal's sidebar, use **Trigger reconnect test** once. Do not press it repeatedly. Wait for the feed to reconnect, a new order-book snapshot and updates to arrive, required channels to refresh, and the signal panel to render at least one new decision. Allow up to two minutes; if it does not recover, capture the failure and leave the release blocked.

## 3. Run the read-only audit

Open a second terminal window and run:

```bash
APP_DIR="${APP_DIR:-$HOME/Downloads/delta-india-btc-perpetual-flow-terminal}"
cd "$APP_DIR"

.venv/bin/python audit_live_recovery.py \
  --events "$HOME/Downloads/delta-live-recovery/events.jsonl" \
  --decisions "$HOME/Downloads/delta-live-recovery/decisions.jsonl" \
  --output "$HOME/Downloads/delta-live-recovery/recovery-audit.json" \
  --max-recovery-seconds 120
```

Exit code `0` means the logged checks passed; exit code `2` means fail/insufficient evidence. The script is read-only with respect to the input logs and records SHA-256 hashes in its report. It selects the **latest** diagnostic marker in the event log, so this run should use fresh, empty capture files.

## 4. Acceptance criteria

Require all of the following:
- report status is `PASS` with zero JSONL parse/I/O errors;
- disconnect and reconnect events are ordered after the diagnostic marker;
- at least one decision is `BLOCKED` during the disconnection/recovery interval;
- no normal (`WAIT`, `BUY` or `SELL`) decision occurs before the first checksum-bearing, CRC-verified book update after reconnect;
- after recovery, a non-`BLOCKED` decision records `feed_fresh=true`, `book_synced=true`, `no_unresolved_sequence_gap=true`, `crc_valid=true`, `orderbook_integrity_ok=true`, fresh required channels, and no failed data-quality gates;
- recovery completes in at most 120 seconds;
- visually confirm the dashboard decision equals the most recent decision-log `decision` field and that the UI does not claim CRC verification while the checksum is unavailable.

A failure is not to be waived: preserve the private logs, review the failed check and keep the release **NOT APPROVED** until corrected and rerun.

## 5. Record the result without exposing market data

Share or attach only `recovery-audit.json` and a concise note of the visual check. Do not commit the full JSONL event/trade logs, decision logs, screenshots with account information, credentials, or API keys to GitHub.

This runtime acceptance is independent of the OOS gate. Authentic historical bars and real aggressor-side trade prints with adequate coverage, realistic fees/spread/slippage/funding treatment, and an independently reviewed holdout remain required for profitability certification.
