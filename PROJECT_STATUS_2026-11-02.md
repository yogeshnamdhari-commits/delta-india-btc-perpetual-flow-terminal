# Project status — 2026-11-02

## Disposition

**Research-only, read-only. Live order execution is NOT APPROVED.**

This status note consolidates the repository state and the operator-reported local historical-data diagnostic. It is not an independent certification of local runtime or strategy profitability.

## Software implementation

The repository contains the Delta India BTCUSD public-feed Streamlit dashboard, its signal and order-flow research components, AI narration integrations, Bybit/Binance/OKX reference adapters, historical-data tools, release-safety checks, and OOS replay tooling.

The project-readiness patch was merged to `main` on 2026-11-02 as commit `7b7ce9323a55a62889837ac0f74e060efac32437` (PR #16). It fixes the persisted Delta ticker quote mapping, adds an actual-`DeltaFeed` regression test, adds a macOS launcher, separates development dependencies, and updates the run/acceptance instructions. No strategy or evaluator logic was changed.

The merge-triggered main-branch CI run 168 completed successfully on that commit. All six jobs passed:
- Python 3.11 and 3.12 test jobs;
- macOS Streamlit startup smoke test on port 8502;
- Bybit, Binance and OKX public-data smoke tests.

The Python test jobs also passed the live Kilo Gateway response smoke test. Hosted CI success is evidence of automated software checks only. It does not establish that the user's Mac currently receives a healthy live feed.

## Historical data and diagnostic replay

The following figures are **operator-reported from local files** kept outside this repository:

| Evidence | Reported result |
|---|---:|
| 1-minute candle bars | 129,588 |
| Candle date range | 2026-07-30 through 2026-10-28 |
| Candle chronology | One 780-second gap on 2026-10-17 |
| Consolidated real trade prints | 22,189,460 |
| Matched trade-minute coverage | 125,155 / 129,588 = 96.58% |
| Gap-aware segment 1 | 114,001 bars |
| Gap-aware segment 2 | 15,587 bars |
| Reconciled segment total | 129,588 bars |
| Simulated trades | 3 |
| Trade outcomes | 3 timeouts |
| Net diagnostic result | -4.15 R |
| Diagnostic expectancy | -1.38 R |
| Diagnostic win rate | 0.0% |

The segment split retains the final candle before the gap and starts the second segment after it. Each segment is evaluated independently; no candles are interpolated and no engine state is carried across the gap.

The result is negative and based on only three simulated trades. It is not sufficient evidence of strategy profitability.

## Frozen evaluator and release gates

- `oos_replay.py` and `run_oos_evaluation.py` remain unchanged by the diagnostic workflow.
- The original frozen evaluator remains **BLOCKED** because the original candle chronology contains a gap.
- The 96.58% minute-overlap figure does not override the evaluator's no-chronology-warning requirement.
- The gap-aware diagnostic is a separate research output; it does not convert the original OOS gate into a pass.
- The report files and market data remain local under `~/Downloads/delta_oos_data/` and are intentionally excluded from Git.

## Outstanding manual acceptance

1. Run `start_mac.command` on the target Mac and verify the Streamlit dashboard opens on port 8502.
2. Confirm current Delta ticker, trades and order-book freshness; confirm the book is sequence-synchronized and CRC32-verified before treating order-book-derived fields as valid.
3. Perform the controlled reconnect acceptance using `LIVE_RECOVERY_RUNBOOK.md`. Retain the local audit JSON and visually verify that the dashboard decision equals the latest persisted decision record. A previously reported recovery result is not independently verified by this repository note.
4. Do not run the original frozen OOS replay expecting a pass while the chronology gap remains.
5. Before any future profitability claim, obtain a substantially larger independent holdout sample and include intended-account fees, spread, slippage, funding, execution assumptions, drawdown and uncertainty analysis.

## Parameter and execution boundary

No strategy thresholds, signal precedence, evaluator gates or strategy parameters are changed by this status update. The dashboard remains read-only. No order submission, amendment or cancellation is enabled or authorized.

**Final status: Software research prototype maintained; local runtime acceptance and economic certification remain open; live trading NOT APPROVED.**
