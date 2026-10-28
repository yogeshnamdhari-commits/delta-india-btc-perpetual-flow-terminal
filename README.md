# Delta India BTC Perpetual Flow Terminal

A read-only local Streamlit dashboard that consumes Delta Exchange India's public WebSocket feed directly and includes a **separate, read-only Binance USDⓈ-M BTCUSDT reference panel**. It does **not** place, amend, or cancel orders and does not need API keys.

## Live channels used

- `ob_updates`: incremental L2 order book, sequence continuity and CRC32 checks
- `trades`: public executed trades and a session-only CVD proxy
- `ticker`: periodic market fields including mark price, best bid/ask and open interest
- `mark_price`: liquidation mark price
- `spot_price`: Delta BTCUSD underlying index (`.DEXBTUSD`)
- `funding_rate`: perpetual funding data
- `candlestick_1m`: live candle updates
- `system_status`: public status events

WebSocket: `wss://public-socket.india.delta.exchange`

The app fetches a small set of recent historical candles over the public REST API to populate the chart on startup. Delta prices/depth/trades then update from its WebSocket messages.

## Secondary feed — Binance USDⓈ-M (read-only)

A collapsed **Cross-exchange reference** panel subscribes to Binance public BTCUSDT aggregate trades, best bid/ask, partial top-20 depth, mark price/funding and sampled liquidation events, and polls open interest from a public REST endpoint. It shows a session-only aggressor-side CVD proxy and its chart. No API key or private account endpoint is used.

**Keep the venues separate:** Delta BTCUSD is USD-quoted and INR-settled; Binance BTCUSDT is USDT-quoted. Price differences are contextual only, not an executable arbitrage spread. Binance trade flow must not be merged into Delta's historical tape or used to satisfy Delta's OOS data-coverage gate.

The Binance feed's top-20 depth is a partial snapshot, not Delta's sequence/CRC-verified order book. Binance's force-order stream is sampled, not a complete liquidation tape. Session CVD resets when the app restarts. Live feed acceptance still requires visual verification, a disconnect/recovery check, and confirmed freshness in the running app.

## Run on macOS

### Option A — Terminal commands

```bash
cd delta_india_btc_terminal
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m streamlit run app.py --server.port 8502
```

Open `http://localhost:8502` in your browser if it does not open automatically.

### Option B — Double-click launcher

Double-click `start_mac.command`. macOS may ask you to confirm that you want to run a downloaded command file. The launcher creates a local virtual environment, installs the listed Python packages, and launches the app.

## What you should see

- BTCUSD last-trade price, mark price and index price
- Bid/ask, spread and spread in basis points
- Live candlesticks (1m, 3m, 5m or 15m views; larger views are aggregated from 1m candles)
- Incremental order book with sequence and CRC32 status
- Recent trade tape
- Session-only signed-volume/CVD proxy
- Delta open interest and funding rate, when delivered by the feed
- Separate Binance BTCUSDT price, spread, mark/funding, open interest, partial depth, recent trades, sampled liquidation event and session CVD reference panel
- Feed age, sequence gaps, checksum failures and reconnect status

## Safety / interpretation notes

- Public market data should not need Delta API credentials. **Do not paste API keys or secrets into this app.**
- The app is read-only. It contains no order-entry controls.
- The local CVD proxy begins at app startup, is not an exchange-calculated historical CVD, and resets when the app is restarted.
- The order book can change or be cancelled rapidly; visible depth is not a promise of execution.
- A fresh WebSocket feed does not guarantee zero latency. On-screen receive age is not a proof of network latency because this build does not calibrate against an independently synchronized clock.
- The book is marked unsynchronized after a sequence gap or CRC32 mismatch, and the client requests a new snapshot.
- This terminal is for monitoring and research, not a guarantee of profitable trades or absence of market manipulation.

## Troubleshooting

1. Confirm the internet connection and that the machine clock is automatically synchronized.
2. Wait several seconds after startup for the first order-book snapshot and market updates.
3. If the feed remains disconnected, verify Delta India's current API/WebSocket status and the official docs: https://docs.delta.exchange/ .
4. If the historical chart is blank but live price/tape is updating, the public historical REST request may have failed; the candle chart should gradually populate from new live candles.
5. Stop the app with `Ctrl+C` in the terminal.
