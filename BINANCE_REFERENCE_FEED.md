# Binance read-only reference feed

This feature adds a **public, read-only** Binance USDⓈ-M BTCUSDT reference feed alongside the existing Delta India BTCUSD feed.

## Scope

- Streams BTCUSDT aggregate trades, best bid/ask, the venue's partial top-20 depth snapshots, mark price, and funding rate.
- Polls public BTCUSDT open interest over REST every 15 seconds.
- Tracks a session-only aggressor-side CVD proxy from the aggregate-trade buyer-is-maker flag.
- Shows the most recent reported liquidation event only as a sampled observation.
- Uses no API key, account endpoint, private user stream, order-entry endpoint, or withdrawal permission.

## Important limitations

- This does **not** replace Delta India data. Binance BTCUSDT is USDT-quoted, while Delta India BTCUSD is USD-quoted and INR-settled; observed price differences are contextual, not an executable arbitrage spread.
- Cross-venue data is kept separate from Delta's signal pipeline and must not be appended to the venue-matched Delta historical trade tape or counted toward the Delta OOS coverage gate.
- The Binance depth stream provides partial top-20 snapshots. It is not the same as Delta's locally synchronized, sequence/CRC-checked L2 book. The UI must not present it as a full, checksum-verified book.
- Binance's forceOrder stream is a sampled latest-event feed (not a complete liquidation tape). The displayed event is not total liquidation volume.
- Connection state and local receive age are monitoring diagnostics, not proof of zero network latency or exchange availability.
- This code is read-only. It must not place or alter orders, unlock the Delta execution gate, or produce a confirmed trading decision.

## Local validation

Run the offline parser tests:

    python -m pytest -q test_binance_reference.py

For acceptance, inspect the collapsed Cross-exchange reference panel in the Streamlit app. Confirm prices and top-of-book are updating, funding/open-interest fields show timestamps or an explicit unavailable/stale state, and no Binance fields appear in Delta's signal_decisions.jsonl as source data. Check behavior during a deliberate network disconnect before relying on the UI.
