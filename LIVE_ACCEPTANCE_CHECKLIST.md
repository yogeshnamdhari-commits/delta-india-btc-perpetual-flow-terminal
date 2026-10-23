# Live application acceptance checklist (read-only)

Run these steps on the Mac **after** installing the patch and passing the local test suite. These actions test market-data recovery only; do not enable order execution.

## 1. Local automated tests

From the project directory with the project virtual environment active:

```bash
.venv/bin/python -m pytest -q
```

Every test must pass. Review failures rather than bypassing tests. The `DeltaFeed` fault-injection tests call the actual class body with stubs but do not open a real websocket.

## 2. Restart application with diagnostic control enabled

Stop the existing Streamlit process cleanly. From the project directory, start the read-only terminal with the existing app's port:

```bash
DELTA_ENABLE_DIAGNOSTIC_RECONNECT=1 .venv/bin/python -m streamlit run app.py --server.port 8503
```

Use the UI's `Trigger reconnect test` control only after confirming that execution remains disabled. This setting enables a controlled websocket reconnect diagnostic; it does not submit orders.

## 3. Observe disconnect and recovery

Keep the app running while recording the following from the screen and JSONL logs:

- Before reconnect: `connection=connected`, channel timestamps are recent, `book_synced=true`, and a CRC is verified if the exchange supplies one.
- Immediately after the controlled disconnect/error: book sync is false, sequence-gap state is unresolved, checksum is unknown/invalid, and decisions fail closed as `BLOCKED` until sufficient new data arrives.
- After reconnect: a fresh book snapshot is received with a valid sequence; subsequent updates are contiguous; a checksum is verified if supplied; ticker, order-book, and trades channels become fresh. Only then may the engine produce an ordinary `WAIT`/signal decision.
- A missing checksum must never be shown as CRC-verified. A successful socket reconnect by itself is not book recovery.
- The displayed decision equals the `decision` field in the most recent `signal_decisions.jsonl` record. Log write failures must be surfaced by the UI/stderr.

Inspect files configured by `DELTA_EVENT_LOG_FILE`, `DELTA_EVENT_LOG_FILE_FULL`, and `DELTA_DECISION_LOG_FILE` (defaults are under `~/Downloads`). Redact local identifiers and never upload raw logs to a public repository.

## 4. Verify CI publication

Once local tests pass, re-enable/run the GitHub auto-sync and inspect `sync.log` for a successful push. GitHub Actions should then run `.github/workflows/tests.yml` on the pushed `main` commit. Confirm the latest commit includes the patch files and the test workflow reports success. If the workflow fails, keep the release blocked.

## 5. Real-data OOS evidence

Follow `OOS_REPLAY_README.md`. Generated bars, trade tape, and reports belong outside the watched project tree (default: `~/Downloads/delta_oos_data`) to avoid the auto-sync worker's large-file safeguard. The trade tape must be genuine, timestamped aggressor-side data with at least 80% minute-bar coverage; candle-only history is not a substitute.

## Acceptance outcome

A local pytest pass plus CI pass establishes software checks only. The end-to-end recovery gate requires the real controlled reconnect observation above. The profitability gate requires an independently reviewed holdout report on correctly covered historical bars and real trade prints, with execution costs and funding addressed. Until both gates pass, status is **NOT APPROVED FOR LIVE TRADING**.
