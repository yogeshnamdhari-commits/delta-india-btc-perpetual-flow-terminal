# Delta India BTC Perpetual Flow Terminal

A read-only local Streamlit dashboard that consumes Delta Exchange India's public WebSocket feed directly. It does **not** place, amend, or cancel orders and does not need API keys.

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

The app fetches a small set of recent historical candles over the public REST API to populate the chart on startup. Market prices/depth/trades then update from WebSocket messages.

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
- Open interest and funding rate, when delivered by the feed
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

## AI Trading Analyst (Q&A)

The dashboard includes a read-only chat panel that answers questions using a compact snapshot of the current BTCUSD public feed: last/mark/index prices, bid/ask and depth, recent trades, recent candles, session aggressor flow/CVD, funding, open interest, channel freshness and book-integrity state. It does not retrieve missing historical trade archives, certify Gate 3, or place orders.

### Option A — Local Ollama (optional; no API billing)

1. Install and start [Ollama](https://ollama.com/).
2. Pull a small local model:

   ```bash
   ollama pull llama3.2:3b
   ```

3. Run the dashboard as usual. In **AI Trading Analyst**, select **Local Ollama (no API billing)** and use `http://localhost:11434` and `llama3.2:3b`, unless your local setup differs.

Optional environment variables: `OLLAMA_BASE_URL` and `OLLAMA_MODEL`.

### Option B — Kilo Gateway free models (default; no API key required)

The panel defaults to **Kilo Gateway free model (no API key)**. Its current default model ID is `nvidia/nemotron-3.5-super-vl:free`, selected after a successful anonymous live-response check in GitHub Actions on 26 October 2026. You can change it in the panel or select another provider. Change it to another currently available Kilo model whose ID ends in `:free` if needed. This provider rejects model IDs without the `:free` suffix to prevent accidental requests to a paid Kilo model. An optional `KILO_API_KEY` environment variable or Streamlit secret may be supplied, but it is not required for Kilo's anonymous free-model access.

Kilo documents a free-model rate limit of 200 requests per hour per IP, and model availability can change. Some free model routes may send prompts and outputs to upstream providers that log them or use them to improve services. Use this option only if you are comfortable sharing the compact market snapshot and recent chat turns with the selected provider. Do not send personal, account, or confidential data.

See [Kilo's free-model guidance](https://kilo.ai/docs/getting-started/using-kilo-for-free) and [Gateway authentication/rate limits](https://kilo.ai/docs/gateway/authentication).

### Option C — OpenAI API

Select **OpenAI API (separate API billing)** in the panel. Configure `OPENAI_API_KEY` locally as an environment variable or in Streamlit secrets. Optionally set `OPENAI_MODEL`.

**A ChatGPT subscription does not include OpenAI API usage.** API requests are billed separately by OpenAI. Do not commit keys to Git or paste exchange API secrets into the dashboard.

### Data handling and limitations

- Only the compact snapshot supplied to the panel and recent chat turns are sent to the selected model provider. OpenAI mode sends that context to OpenAI; Ollama mode sends it to the configured Ollama server.
- Chat history is kept in Streamlit session state, not in the market event JSONL logs.
- The analyst has no execution tools and no access to exchange API credentials.
- Answers are generated commentary, not validated trading signals or a guarantee of profitability. Live data alone cannot certify historical completeness or out-of-sample results.
- Unit tests for the context packer and provider-response parsing can be run with `python -m unittest test_ai_analyst.py`.

## Market structure and liquidity intelligence

The read-only **Order flow & liquidity** area distinguishes measured market data from price-action inference:

- **Market-wide liquidation levels:** deliberately shown as unavailable. Delta's documented public WebSocket channel list does not include a market-wide liquidation feed; liquidation reasons appear in private/account order notifications. This dashboard does not fabricate liquidation levels or intensity from assumed leverage.
- **Observed resting liquidity:** requires a sequence-synchronized, checksum-valid order book with no unresolved sequence gap. It displays contracts, BTC equivalent, approximate USD notional at the observed price, time observed, and size relative to the adaptive depth threshold. Resting orders can be cancelled or moved.
- **Supply and demand:** derived bullish/bearish order-block zones from completed candles. The “Displacement (x ATR)” field measures the price displacement used to confirm the zone; it is not a measured order quantity or probability.
- **Stop-hunt candidates:** a prior swing/equal/session level is exceeded and price reclaims the level within the detector's configured candle window. The panel exposes breach extreme, overshoot in price points and reclaim delay. This confirms a price sweep, not that stop orders were actually present or triggered.
- **Buy/sell exhaustion:** labels require a completed one-minute candle to break a prior local extreme, reject it with a qualifying wick, and meet the existing same-candle aggressor-flow rule. The panel shows buy/sell contracts, dominant-side ratio, wick ratio, close location and classified print count.

All levels and labels are monitoring context, not trade guarantees. These UI additions do not modify the signal engine, its thresholds, signal precedence, evaluator or release criteria.

Delta API references: [WebSocket public channels](https://docs.delta.exchange/) and [private order updates](https://docs.delta.exchange/).
