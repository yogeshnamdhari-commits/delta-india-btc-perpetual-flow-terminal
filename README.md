# Delta India BTC Perpetual Flow Terminal

Read-only BTC perpetual market-data dashboard and signal-engine research project.

## Main files

- `app.py` — Streamlit terminal and application integration.
- `signal_integration_bridge.py` — Converts the existing feed snapshot into signal-engine input and aggregates completed one-minute candles into higher timeframes.
- `signal_engine.py` — Read-only signal evaluation, quality gates, setup criteria, risk checks, and result reason codes.
- `test_signal_engine.py`, `test_signal_timeframes.py`, `test_synchronized_logging.py` — unit and regression tests.
- `oos_replay.py` and related tests are included if present in the source directory.

## Run tests

Install the dependencies required by the project, then run:

```bash
python3 -m pytest -v
```

## Safety and release status

**Not approved for live order execution.** This repository is for read-only research and validation. Unit tests, synthetic failure tests, and log consistency audits do not alone certify the live `DeltaFeed -> app -> bridge -> engine -> persisted decision` path or establish profitability.

Before any deployment, run end-to-end failure-injection and recovery tests against the actual application, then conduct chronological out-of-sample evaluation with frozen strategy parameters and realistic fees, spread, slippage, and funding costs. Report net expectancy, profit factor, maximum drawdown, and trade count.

Runtime feed logs, decision JSONL files, credentials, and raw market-data captures are intentionally excluded from Git by `.gitignore`. Add only sanitized, intentionally selected datasets to a separate location if needed for research.
