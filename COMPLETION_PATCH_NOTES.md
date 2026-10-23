# Completion patch notes

## Purpose

This patch closes identified software-engineering gaps in the read-only BTC perpetual research terminal. It does **not** enable order submission and does not certify strategy profitability.

## Source fixes

- Feed-to-engine health is derived from explicit connection status, recent timestamps for ticker/order-book/trade channels, order-book sync, unresolved sequence-gap state, and CRC availability. Missing/unknown health state fails closed.
- Historical 1m seed bars are only accepted when the event has an explicit `candle_ts_utc` or `candle_ts`; `ts_utc` by itself is treated as log receive time, not candle time.
- The dashboard uses one canonical `evaluate_feed_snapshot` decision record for both display and persistence, and reuses a `SignalEngine` in Streamlit session state.
- JSONL persistence refuses NaN/non-JSON records and reports I/O failures. Full market-tick logs are flushed without forcing an `fsync` for every tick; decision writes are synced to disk.
- Disconnects, socket errors, sequence gaps, invalid snapshots, and CRC failures invalidate current book-integrity state. An explicitly valid snapshot/update can recover the sequence state; an absent checksum remains unverified and is blocked by the strict engine gate.
- Initial candle retrieval and retention are increased to support 5m/15m warm-up after live updates begin.
- OOS replay excludes the bar that is still open at decision time and excludes trade prints stamped exactly at the decision time. Entry simulation uses the supplied next-bar timestamp, incorporates half-spread plus slippage at each fill, includes both-side fees, and censors incomplete/gapped outcome paths.
- A replay refuses a simulated fill if the next-bar open is already outside the planned stop/target corridor.

## Reproducible evidence added

- `test_live_signal_pipeline.py`: synthetic feed-to-engine contract and timestamp/seed-data tests. It is not live-feed proof.
- `test_delta_feed_failure_injection.py`: executes the actual `DeltaFeed` class source from `app.py` with controlled stubs to inject close/error/gap/CRC/invalid-snapshot paths without opening a socket.
- `test_completion_regressions.py`: replay look-ahead and execution-cost regressions.
- `.github/workflows/tests.yml`: runs `pytest` on Python 3.11 and 3.12 after the source patch has been pushed.
- Data download/export and OOS report tools record file hashes and report coverage gates. Missing trade-tape evidence blocks the evaluation.

## Remaining release gates

The local fault-injection tests are controlled software tests, not a test of the running Streamlit process against an actual live Delta feed. Run the manual checklist in `LIVE_ACCEPTANCE_CHECKLIST.md`. Authentic out-of-sample evaluation also requires real trade-print coverage matching the historical bar window, actual account fee/spread/slippage assumptions, and a separate funding model. The report remains research-only; it cannot approve live execution by itself.
