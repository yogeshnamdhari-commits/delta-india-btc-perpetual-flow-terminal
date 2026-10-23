# Real-data out-of-sample (OOS) evaluation

This workflow is read-only: it never sends orders. Unit tests prove software mechanics, not profitability. Store market-data files and reports **outside** the watched source project so the five-minute GitHub sync does not mirror large data files. Defaults use `~/Downloads/delta_oos_data/`.

## 1. Download BTCUSD 1-minute candles

```bash
python3 download_delta_1m_candles.py --days 90 --output ~/Downloads/delta_oos_data/bars_1m.csv
```

The downloader calls Delta India's public history-candles API (`BTCUSD`, `1m`) in chunks of up to 2,000 bars, sorts/deduplicates records, and writes `ts_utc,open,high,low,close,volume`. Check its reported range, missing-minute intervals and SHA-256.

## 2. Export real local aggressor-side trade prints

```bash
python3 export_delta_trades.py --input ~/Downloads/delta_feed_events.jsonl --output ~/Downloads/delta_oos_data/trades.csv
```

Only JSONL records with `event=trade`, a real timestamp, price, size and recognized buyer-role (`t` -> `BUY_TAKER`; `m` -> `SELL_TAKER`) are exported. Nothing is fabricated. A zero-event capture still writes an empty-header CSV and warns; continue only to produce a blocked evidence report, not to claim OOS results. Historical OHLC candles cannot reconstruct historical aggressor flow. Delta’s documented public `GET /v2/trades/{symbol}` endpoint returns recent public trades, not an archival 60–90 day tick tape; do not treat it as a substitute for the logged window. If your real local capture does not span the historical bar window, the evaluation intentionally remains blocked until enough authentic prints have been captured or obtained from a separately verifiable archive.

## 3. Run the frozen, coverage-gated evaluation

```bash
python3 run_oos_evaluation.py \
  --bars ~/Downloads/delta_oos_data/bars_1m.csv \
  --trades ~/Downloads/delta_oos_data/trades.csv \
  --output-dir ~/Downloads/delta_oos_data/oos_report
```

The runner records input hashes, time ranges, frozen parameters, simulated outcomes and chronological train/validation/holdout metrics. It checks evidence **before** performing the expensive replay and skips replay when any of these data screens fail: less than 60 days of bar history; fewer than 1,000 real trade prints; real trade prints on fewer than 80% of the minutes represented by bars; or chronology warnings. Those limits are conservative evidence-sufficiency screens, not exchange rules or profitability criteria. Even when they pass, fewer than 100 completed holdout trades is marked as insufficient sample, and any other result remains research-only/not certified.

## Important limitations

- The 60/20/20 split is chronological, never random. A holdout seen during strategy development is retrospective, not prospective validation.
- Funding is not modelled by the default frozen harness; results explicitly exclude it. Validate fees for the actual account, spread, slippage, funding, execution realism, drawdown, sample size, uncertainty and parameter sensitivity independently.
- The local live trade tape only covers the time the terminal actually captured prints. A short tape does not support a 90-day flow-driven backtest. A blocked report is the correct result when that evidence is missing.
- A positive replay, passing software tests or green CI is not release approval. Live execution remains **NOT APPROVED** until end-to-end recovery and economic certification are independently reviewed.

Never commit credentials, private captures, raw logs, or market-data files to GitHub.
