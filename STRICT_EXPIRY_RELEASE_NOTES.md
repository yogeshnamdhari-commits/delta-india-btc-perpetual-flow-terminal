# Strict Expiry Cutoff — Release Notes

**Date:** 2026-11-18  
**Scope:** Documentation and regression-test delta for the reviewed strict-expiry implementation.

## Trigger-window behavior

The 1-minute trigger search uses a fixed window consisting of the first six completed 1-minute candles after the structure-break setup. The sixth completed candle is included; a qualifying trigger that appears only on the seventh or a later candle is rejected with `WAIT_NO_1M_TRIGGER`.

This is a behavior clarification relative to the previous rolling-tail search, which could allow a later candle to enter the most-recent-six window. The fixed window prevents an already-expired structural setup from being re-triggered by a later candle.

## Frozen values and expiry semantics

- `trigger_max_bars`: **6** (unchanged)
- `expiry_1m_candles`: **3** (unchanged)
- Expiry remains exclusive at `trigger_ts_utc + 3 minutes`.
- The approved `stop_first` precedence and historical evaluation rules are unchanged.

## Regression coverage added

- BUY trigger occurring only on completed bar 7 must return `WAIT_NO_1M_TRIGGER`.
- SELL trigger occurring only on completed bar 7 must return `WAIT_NO_1M_TRIGGER`.
- Existing sixth-bar inclusive-boundary coverage remains in place.

These tests are guardrails for the fixed six-bar rule; they do not alter strategy parameters or establish strategy profitability.

## Release and safety status

This note records the trigger-window compatibility behavior only. It does not approve historical OOS replay, order execution, deployment, or live trading. Order execution remains disabled and live trading remains not approved pending the project's separate release gates.
