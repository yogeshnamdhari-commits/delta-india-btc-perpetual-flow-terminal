# Release Status Report — 2026-10-23

## Disposition

**NOT APPROVED for live order execution.**

The current repository baseline has a passing automated-test milestone. Automated CI evidence is distinguished from live-runtime acceptance and economic certification below.

## Verified automated tests

The end-to-end test branch run is available at:
https://github.com/yogeshnamdhari-commits/delta-india-btc-perpetual-flow-terminal/actions/runs/40001060408

| Area | Verified result | Scope limitation |
|---|---:|---|
| GitHub Actions — Python 3.11 | 131 passed, 1 skipped, 2 subtests passed | Deterministic unit/integration tests |
| GitHub Actions — Python 3.12 | 131 passed, 1 skipped, 2 subtests passed | Deterministic unit/integration tests |
| Decision-log audit | 921 records passed (operator-reported historical result) | Internal consistency only; cannot prove feed truth or live application behavior |
| End-to-end source-path regression | Passed in both CI jobs | Actual `DeltaFeed` and `_render_signal_panel` source plus bridge/engine are exercised with controlled socket and Streamlit stubs; no live WebSocket or full running Streamlit process |

The new `test_e2e_signal_panel_failure_injection.py` verifies that disconnect, checksum-unavailable, sequence-gap, and CRC-mismatch states fail closed, recovery requires a checksum-verified snapshot, the same `SignalEngine` instance is retained, and the displayed decision matches the last persisted decision record. These are synthetic, deterministic software tests, not proof of live-feed reliability.


The read-only `audit_live_recovery.py` utility and `LIVE_RECOVERY_RUNBOOK.md` were added to evaluate a local diagnostic reconnect capture. They are intended to require the observed trigger/disconnect/reconnect sequence, a BLOCKED decision during failure, a checksum-bearing CRC-verified book update, fresh required channels, and a healthy post-recovery decision. **This audit has not yet been run against the user's actual Mac logs**, so live-runtime recovery remains pending.

## Required release gates still open

### 1. Live application failure injection and recovery

The required production-like acceptance chain is:

`DeltaFeed → app.py / Streamlit → signal_integration_bridge.py → signal_engine.py → displayed decision → persisted decision log`

Automated source-path fault cases now cover:
- disconnected feed
- missing/unverified checksum
- invalid sequence / unresolved sequence gap
- recovery after a fresh checksum-verified snapshot
- parity between the displayed decision and persisted decision record

Still required on the actual running Streamlit process:
- controlled disconnect/reconnect against the live public feed
- observe stale/missing channel handling and book resynchronization
- confirm the application stays `BLOCKED` until required data channels are fresh, the book is synchronized, sequence continuity is restored, and checksum state is verified when provided
- save sanitized screen/log evidence showing that displayed and persisted decisions agree across failure and recovery

Do not treat the automated source-extraction test as completion of live-runtime acceptance. The running-application test remains open.

### 2. Out-of-sample profitability

The replay harness must be run against real, timestamped historical market data with frozen strategy parameters and chronological train/validation/OOS separation. Include fees, bid/ask spread, slippage, funding where applicable, and realistic execution assumptions.

Report at minimum:
- number of trades
- gross and net P&L
- net expectancy in R
- profit factor
- maximum drawdown
- assumptions and uncertainty / sensitivity analysis

The presence of `oos_replay.py` and passing replay-mechanics tests does not establish a profitable strategy. No OOS profitability result is certified by this report.

## Repository scope

This is a curated source/test snapshot. Runtime logs, credentials and raw market-data captures must remain excluded. Any live-session evidence or historical market data should be stored privately and sanitized before being attached to an acceptance report.

## Release rule

Keep live order execution disabled until:
1. live disconnect/reconnect and failure recovery pass against the running application;
2. displayed and persisted final decisions agree throughout those scenarios;
3. chronological OOS evaluation with authentic data and realistic costs is completed and meets pre-defined economic acceptance thresholds.

Passing safety tests is not proof of profitability; passing a profitable replay is not proof of feed safety. These are independent gates.
