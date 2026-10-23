# Delta BTC signal engine — release safety test bundle

## Included

- `test_release_safety_integration.py`: self-contained synthetic negative-path and recovery tests. It defines its own NOW/build_snapshot fixture and does not import or modify `test_signal_engine.py`.
- `audit_release_decisions.py`: read-only checker for the local `signal_decisions.jsonl` log. Supports `data_quality_gate_results` and `data_quality_gates` schemas.

## Run the synthetic negative-path/recovery tests on the Mac

Copy `test_release_safety_integration.py` into `~/Downloads` beside the project's `signal_engine.py`, then run:

```bash
cd ~/Downloads
python3 -m pytest -v test_release_safety_integration.py
```

The suite injects disconnect, unsynchronized book, invalid sequence, unresolved gap, failed/unavailable checksum, stale/missing channel, stale trade and missing book levels. It asserts that the engine returns WAIT (not a confirmed entry), records the expected gate reason, and exposes the failed gate. Recovery tests then use the SAME engine instance with a fresh valid synthetic snapshot and verify that a recovered feed can become eligible again.

The BUY/SELL controls are fully synthetic fixtures specifically built to meet the engine's entry rules. They are mechanics tests only; they are not live market signals, profitability evidence, or permission to place orders.

## Audit live decision-log consistency

```bash
python3 audit_release_decisions.py ~/Downloads/signal_decisions.jsonl
```

The audit supports `--since` and `--runtime-context` filters, tolerates expected diagnostic records without engine gate data, and compares only fields with directly aligned semantics. It deliberately does not compare `sequence_valid` to `no_unresolved_sequence_gap` unless the exact `sequence_valid` gate exists, because those fields can have different meanings across schema versions. A PASS means only that the inspected log records are internally consistent. It cannot independently confirm that Delta feed fields are truthful or that the dashboard invoked the tested code.

## Important boundary

This bundle tests the supplied `signal_engine.py` snapshot and logs. It does not patch `app.py`, create a WebSocket, or directly test the current Streamlit `_render_signal_panel()`/`_evaluate_signal_decision()` call graph. The exact local application should still be run through negative-path end-to-end tests that inject faults into the actual `DeltaFeed` and assert both the displayed decision and persisted JSONL record. Do not claim full end-to-end integration has passed until that test runs against the current local source.

## Separate OOS profitability gate

No profitability claim is made here. Economic certification requires a frozen strategy and parameters, chronological train/validation/OOS separation, realistic fees/spread/slippage/funding/execution assumptions, and reporting net expectancy, profit factor, drawdown, trade count and uncertainty. Safety tests and synthetic fixtures must not be used to measure or imply profitability.
