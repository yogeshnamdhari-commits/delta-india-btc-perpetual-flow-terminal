# Binance USDⓈ-M cross-venue reference feed

The dashboard adds an independent public-data-only Binance BTCUSDT perpetual reference feed. It is context for analysis only and cannot change Delta-specific signals or fill missing Delta history.

## Supported public market-data streams

- Aggregate trades (aggTrade) with taker direction inferred from the buyer-is-maker flag; CVD is a process-local signed BTC-volume proxy.
- Best bid/ask (bookTicker), mark/index price and funding (markPrice), 24-hour ticker, and native 1m/5m/15m/1h klines.
- Public force-order liquidation snapshots, labelled as sampled observed events rather than a complete liquidation tape.
- Open interest from the public USDⓈ-M REST endpoint.

## Order-book synchronization

The feed buffers depth@100ms events, obtains /fapi/v1/depth?symbol=BTCUSDT&limit=1000, bridges the snapshot update ID with buffered diff-depth updates, and then requires pu continuity for subsequent updates. A sequence break invalidates and clears the local book and requests a fresh REST snapshot. Only a synchronized book is displayed as valid.

Binance's public USDⓈ-M depth stream does not provide the Delta-style CRC32 checksum. The UI explicitly says checksum unavailable. The initial REST snapshot covers up to 1000 levels per side; levels outside that range remain unknown until changed by a stream update. This is a local synchronized book, not a claim to possess the exchange's complete order book.

## Data boundaries and limitations

- Binance BTCUSDT quote prices/volume and BTC quantities remain separate from Delta India BTCUSD INR-settled contract counts and from Bybit/OKX data.
- Binance book, trade tape, CVD, open interest, mark/funding, candles and force-order observations are never appended to Delta historical tape or included in Delta Gate 3/OOS evidence.
- Binance forceOrder is a sampled per-symbol stream (at most the latest liquidation order snapshot per one-second interval), not a complete liquidation history or predictive liquidation map.
- CVD starts when this process receives trades and resets on restart. Candle history is limited to the bounded in-memory window unless a separate history endpoint is explicitly implemented.
- No API keys, user-data stream, account endpoints, or order-entry methods are used.

## Offline parser and sequence tests

    python -m pytest -q test_binance_market.py

CI also runs a public-feed smoke test that separately reports WebSocket/order-book readiness, REST snapshot access and errors. A hosted runner result does not guarantee access from every network or jurisdiction. Verify freshness and disconnect/recovery locally before relying on the panel.
