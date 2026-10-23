# Release Status Report — 2026-10-24

## Disposition

**NOT APPROVED for live order execution.** The reported live reconnect audit is passing, but out-of-sample (OOS) economic certification is still open. Keep execution disabled.

## Gate 1 — Automated source-path failure tests

**PASS — automated CI.** The latest recorded main baseline `6b603b1` passed 131 tests, 1 skipped test, and 2 subtests on both Python 3.11 and Python 3.12. These deterministic tests exercise application source paths with controlled socket/Streamlit stubs; they are not a substitute for live-runtime or profitability evidence.

## Gate 2 — Live disconnect/reconnect recovery

**PASS — operator-reported audit result.** The operator reports that `audit_live_recovery.py` exited with code 0 and all 12 audit checks passed. Summary supplied by the operator:

| Observation | Reported result |
|---|---|
| Diagnostic trigger / disconnect | 19:13:03 |
| Reconnection observed | 19:13:06 |
| Checksum-bearing, CRC-verified book recovered | 19:13:06.69 |
| Healthy post-recovery decision | `WAIT` at 19:13:11 |
| Decision safety during failure | 1 `BLOCKED` decision; 0 non-`BLOCKED` decisions before verified recovery |
| Recovery duration | 8.14 seconds, within the 120-second limit |
| Audit exit code | 0 |
| Diagnostic Streamlit process | Reported stopped (PID 74544) |

**Evidence qualification:** these are operator-reported results; the raw `recovery-audit.json` was not included in the status update and has not been independently re-parsed here. The runbook also requires a visual confirmation that the dashboard decision equals the latest persisted decision record. The supplied summary does not explicitly record that visual parity check, so retain it as an outstanding evidence item unless it was completed and documented locally.

Runbook: [LIVE_RECOVERY_RUNBOOK.md](LIVE_RECOVERY_RUNBOOK.md).

## Gate 3 — Authentic OOS profitability evaluation

**PENDING. No economic certification exists yet.** The required local-only workflow is documented in [OOS_REPLAY_README.md](OOS_REPLAY_README.md) and uses `download_delta_1m_candles.py`, `export_delta_trades.py`, and `run_oos_evaluation.py`.

Minimum evidence screens enforced by the evaluator:

- At least 60 days of timestamped 1-minute bars.
- At least 1,000 authentic aggressor-side trade prints overlapping the bar window.
- Trade prints present on at least 80% of minutes represented by the bars.
- No chronology warnings or bar gaps.
- At least 100 completed holdout trades for consideration; even then, the report remains research-only and not certified.

The historical-candle downloader does not provide historical aggressor-side trade prints. A short live/reconnect capture is not a substitute for a matching historical tape. The frozen replay currently excludes funding, so intended-account funding, exchange fee tier, spread, slippage, execution assumptions, drawdown, uncertainty, and parameter sensitivity require independent review. A positive replay or passing data screens alone cannot approve live execution.

## Release rule

Live order execution remains **NOT APPROVED** until the recorded recovery evidence and display/persisted-decision parity are reviewed, and a chronological OOS evaluation using authentic data and realistic economic costs is independently accepted against pre-defined thresholds.
