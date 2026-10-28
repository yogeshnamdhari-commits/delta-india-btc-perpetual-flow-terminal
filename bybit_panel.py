"""Streamlit UI for the separate Bybit V5 cross-venue market monitor."""
from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ai_analyst import _technical_indicators
from bybit_market import (
    BybitV5Feed,
    DEFAULT_SYMBOL,
    INTERVAL_SECONDS,
    aggregate_observed_bankruptcy_buckets,
)

IST = ZoneInfo("Asia/Kolkata")


def _technical_by_timeframe(snapshot: dict[str, Any], now_epoch: float) -> dict[str, dict[str, Any]]:
    candle_sets = snapshot.get("candles_by_interval") or {"1m": snapshot.get("candles", [])}
    current_price = snapshot.get("last_price")
    result = {}
    for timeframe, seconds in INTERVAL_SECONDS.items():
        rows = candle_sets.get(timeframe, []) if isinstance(candle_sets, dict) else []
        result[timeframe] = _technical_indicators(
            {"candles": rows, "candle_interval_seconds": seconds, "timeframe": timeframe},
            now_epoch=now_epoch,
            current_price=current_price,
        )
    return result


@st.cache_resource(show_spinner=False)
def get_bybit_feed(symbol: str = DEFAULT_SYMBOL) -> BybitV5Feed:
    feed = BybitV5Feed(symbol=symbol)
    feed.start()
    return feed


def _price(value: Any, digits: int = 2) -> str:
    try:
        number = float(value)
        return f"USDT {number:,.{digits}f}" if number > 0 else "—"
    except (TypeError, ValueError, OverflowError):
        return "—"


def _quantity(value: Any, digits: int = 4) -> str:
    try:
        number = float(value)
        return f"{number:,.{digits}f} BTC" if number >= 0 else "—"
    except (TypeError, ValueError, OverflowError):
        return "—"


def _time_ist(value: Any) -> str:
    try:
        return datetime.fromtimestamp(float(value), tz=IST).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OverflowError, OSError):
        return "—"


def _top_book_rows(snapshot: dict[str, Any], side: str, limit: int = 10) -> list[dict[str, Any]]:
    levels = snapshot.get(side)
    if not isinstance(levels, dict):
        return []
    rows = []
    for raw_price, raw_qty in levels.items():
        try:
            price, qty = float(raw_price), float(raw_qty)
        except (TypeError, ValueError):
            continue
        if price <= 0 or qty <= 0:
            continue
        rows.append({
            "Price (USDT)": price,
            "Size (BTC)": qty,
            "Approx. notional (USDT)": price * qty,
        })
    rows.sort(key=lambda row: row["Price (USDT)"], reverse=(side == "bids"))
    return rows[:limit]


def _render_bybit_panel(feed: BybitV5Feed, delta_feed: Any) -> None:
    snap = feed.snapshot()
    delta = delta_feed.snapshot() if delta_feed is not None else {}
    now = time.time()
    last_market = snap.get("last_market_message_at")
    market_age = now - float(last_market) if last_market else None

    st.markdown("---")
    st.subheader("Alternative exchange · Bybit V5 Linear BTCUSDT")
    st.caption(
        "Independent read-only public futures feed. Quantities use BTC units, not Delta BTCUSD contract counts. "
        "Bybit values are not injected into the Delta-specific signal engine or Gate 3 evaluator."
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Bybit connection", str(snap.get("connection", "starting")))
    c2.metric("Bybit book synchronized", "YES" if snap.get("book_synced") and not snap.get("sequence_gap_unresolved") else "NO")
    c3.metric("Last market message", f"{market_age:.1f}s ago" if market_age is not None else "—")
    c4.metric("Bybit last trade", _price(snap.get("last_price")))

    if not snap.get("book_synced") or snap.get("sequence_gap_unresolved"):
        st.warning(
            "Bybit order book is not currently synchronized from a valid snapshot/delta sequence. "
            "Visible depth is withheld until a fresh snapshot is received. "
            + str(snap.get("book_integrity_message") or "")
        )

    prices = st.columns(5)
    prices[0].metric("Mark price", _price(snap.get("mark_price")))
    prices[1].metric("Index price", _price(snap.get("index_price")))
    prices[2].metric("Best bid", _price(snap.get("bid")))
    prices[3].metric("Best ask", _price(snap.get("ask")))
    bid, ask = snap.get("bid"), snap.get("ask")
    try:
        bid_f, ask_f = float(bid), float(ask)
        mid = (bid_f + ask_f) / 2
        spread = ask_f - bid_f
        spread_bps = spread / mid * 10000 if mid > 0 and spread >= 0 else None
    except (TypeError, ValueError, ZeroDivisionError):
        spread, spread_bps = None, None
    prices[4].metric("Spread", f"USDT {spread:,.2f} / {spread_bps:.2f} bps" if spread is not None and spread_bps is not None else "—")

    delta_last = delta.get("last_price") if isinstance(delta, dict) else None
    bybit_last = snap.get("last_price")
    try:
        delta_f, bybit_f = float(delta_last), float(bybit_last)
        delta_pct = (bybit_f - delta_f) / delta_f * 100 if delta_f > 0 else None
        delta_diff = bybit_f - delta_f
    except (TypeError, ValueError, ZeroDivisionError):
        delta_pct, delta_diff = None, None
    cross = st.columns(3)
    cross[0].metric("Delta last price", _price(delta_last))
    cross[1].metric("Bybit − Delta raw quote", f"{delta_diff:+,.2f} USDT" if delta_diff is not None else "—")
    cross[2].metric("Raw quote difference", f"{delta_pct:+.4f}%" if delta_pct is not None else "—")
    st.caption(
        "This is a raw quote comparison, not an executable arbitrage spread. BTCUSD vs BTCUSDT contracts, "
        "quote/collateral basis, fees, funding, network latency and slippage differ."
    )

    funding = snap.get("funding_rate")
    try:
        funding_display = f"{float(funding) * 100:+.5f}%" if funding is not None else "—"
    except (TypeError, ValueError):
        funding_display = "—"
    ticker = snap.get("ticker_24h") or {}
    funding_cols = st.columns(5)
    funding_cols[0].metric("Funding rate (per exchange period)", funding_display)
    funding_cols[1].metric("Next funding (IST)", _time_ist(snap.get("next_funding_at")))
    funding_cols[2].metric("Open interest", _quantity(snap.get("open_interest_btc")))
    funding_cols[3].metric("OI notional (approx.)", _price(snap.get("open_interest_usd_approx"), 0))
    pct = ticker.get("price24hPcnt")
    try:
        funding_cols[4].metric("24h price change", f"{float(pct) * 100:+.2f}%" if pct is not None else "—")
    except (TypeError, ValueError):
        funding_cols[4].metric("24h price change", "—")
    st.caption(
        "Open interest, funding and mark/index are sourced from Bybit's ticker snapshot/update. "
        "OI notional is a local approximation: OI (BTC) × mark price (USDT/BTC). The funding interval is not hard-coded."
    )
    if snap.get("last_rest_error"):
        st.caption("REST status: " + str(snap["last_rest_error"]))

    if snap.get("book_synced") and not snap.get("sequence_gap_unresolved"):
        top_bids = _top_book_rows(snap, "bids", limit=5)
        top_asks = _top_book_rows(snap, "asks", limit=5)
        bid_depth = sum(row["Size (BTC)"] for row in top_bids)
        ask_depth = sum(row["Size (BTC)"] for row in top_asks)
        total = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / total * 100 if total > 0 else None
        depth_cols = st.columns(3)
        depth_cols[0].metric("Top-5 bid depth", _quantity(bid_depth, 5))
        depth_cols[1].metric("Top-5 ask depth", _quantity(ask_depth, 5))
        depth_cols[2].metric("Top-5 depth imbalance", f"{imbalance:+.2f}%" if imbalance is not None else "—")
        st.caption(
            "Top-5 depth imbalance = (sum of five largest displayed bid BTC sizes − sum of five largest "
            "displayed ask BTC sizes) / their total × 100. Positive means more visible bid size; negative "
            "means more visible ask size. RPI orders are not visible in this public book. This does not prove support or resistance."
        )
        left, right = st.columns(2)
        with left:
            st.markdown("**Observed bids**")
            rows = _top_book_rows(snap, "bids")
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            else:
                st.info("No valid bid levels.")
        with right:
            st.markdown("**Observed asks**")
            rows = _top_book_rows(snap, "asks")
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            else:
                st.info("No valid ask levels.")
        st.caption(
            f"Bybit book update ID: {snap.get('book_update_id')} · cross sequence: {snap.get('book_cross_sequence')} · "
            f"snapshot/resubscribe count: {snap.get('depth_restarts')}. This adapter tracks Bybit snapshot/delta sequence; "
            "it does not claim a venue-provided CRC32 checksum."
        )

    candles_1m = [
        row for row in (snap.get("candles_by_interval", {}).get("1m", []) or [])
        if isinstance(row, dict) and row.get("is_closed") is True
    ]
    if candles_1m:
        rows = candles_1m[-180:]
        figure = go.Figure(data=[go.Candlestick(
            x=[datetime.fromtimestamp(float(row["time"]), tz=IST) for row in rows],
            open=[row["open"] for row in rows],
            high=[row["high"] for row in rows],
            low=[row["low"] for row in rows],
            close=[row["close"] for row in rows],
            name="Bybit BTCUSDT",
        )])
        figure.update_layout(
            title="Bybit BTCUSDT · completed 1-minute candles",
            height=360,
            margin=dict(l=10, r=10, t=45, b=10),
            xaxis_rangeslider_visible=False,
            uirevision="bybit-btcusdt-1m",
        )
        st.plotly_chart(figure, use_container_width=True, key="bybit-v5-1m-chart")
    else:
        st.info("Waiting for Bybit completed 1-minute candle history.")

    technical = _technical_by_timeframe(snap, now)
    technical_rows = []
    for timeframe, metrics in technical.items():
        emas = metrics.get("ema_20_50_144_200", {})
        volume_ratio = metrics.get("latest_volume_vs_preceding_20_mean_ratio")
        atr_percent = metrics.get("atr_14_pct_of_last_closed_close")
        ret = metrics.get("five_closed_bar_close_return_pct")
        technical_rows.append({
            "Timeframe": timeframe,
            "Closed candles": metrics.get("completed_candle_count", 0),
            "EMA alignment": str(metrics.get("ema_alignment", "insufficient_lookback")).replace("_", " "),
            "EMA 20": _price((emas.get("20") or {}).get("value")),
            "EMA 50": _price((emas.get("50") or {}).get("value")),
            "EMA 144": _price((emas.get("144") or {}).get("value")),
            "EMA 200": _price((emas.get("200") or {}).get("value")),
            "ATR 14": _price(metrics.get("atr_14_price")),
            "ATR %": f"{atr_percent:.3f}%" if atr_percent is not None else "—",
            "Latest / prior-20 mean volume": f"{volume_ratio:.2f}×" if volume_ratio is not None else "—",
            "5-closed-candle return": f"{ret:+.3f}%" if ret is not None else "—",
            "20-bar structure": str(metrics.get("last_20_vs_previous_20_bar_structure", "—")).replace("_", " "),
        })
    history_loaded = snap.get("candle_history_loaded") or {}
    missing_history = [tf for tf in INTERVAL_SECONDS if not history_loaded.get(tf)]
    if missing_history:
        st.warning(
            "Historical REST warm-up is incomplete for: " + ", ".join(missing_history) +
            ". The live WebSocket may populate the current candles, but it does not replace the 1,000-bar historical warm-up. "
            "EMA/ATR/structure values can remain unavailable or use insufficient lookback until history loads."
        )
    st.markdown("**Calculated Bybit multi-timeframe technical context**")
    st.dataframe(pd.DataFrame(technical_rows), use_container_width=True, hide_index=True)
    st.caption(
        "Each 1m/5m/15m/1h indicator uses that native Bybit timeframe's completed candles. EMA values use "
        "an initial-period SMA seed; ATR is a simple mean of the last 14 true ranges; volume ratio compares "
        "the latest closed candle with the previous 20; structure compares latest 20 bars to the prior 20. "
        "No higher-timeframe candles are fabricated. Insufficient lookback remains unavailable."
    )

    trades = snap.get("trades", [])[:20]
    if trades:
        st.markdown("**Recent Bybit public trades**")
        st.dataframe(pd.DataFrame([{
            "Time (IST)": _time_ist(row.get("timestamp")),
            "Taker side": row.get("side"),
            "Price": row.get("price"),
            "Size (BTC)": row.get("size_btc"),
            "Approx. notional (USDT)": row.get("notional_usdt_approx"),
            "Block trade": row.get("is_block_trade"),
        } for row in trades]), use_container_width=True, hide_index=True)
    else:
        st.info("Waiting for Bybit public trade messages.")
    flow_cols = st.columns(4)
    flow_cols[0].metric("Session CVD proxy", _quantity(snap.get("cvd_value_btc"), 5))
    flow_cols[1].metric("Taker buy volume", _quantity(snap.get("buy_volume_btc"), 5))
    flow_cols[2].metric("Taker sell volume", _quantity(snap.get("sell_volume_btc"), 5))
    flow_cols[3].metric("Trades observed", str(snap.get("trade_count", 0)))
    st.caption("CVD = taker-buy BTC size − taker-sell BTC size since this process started; resets on restart.")

    with st.expander("Observed Bybit liquidation executions and bankruptcy-price buckets", expanded=True):
        st.caption(
            "Source: Bybit V5 public allLiquidation stream. The feed reports executed size and a bankruptcy price. "
            "These are observed liquidation messages, not a map of untriggered future liquidations. "
            "Buckets aggregate only messages retained in this process's bounded in-memory buffer."
        )
        liquidations = snap.get("liquidations", [])
        if liquidations:
            st.dataframe(pd.DataFrame([{
                "Time (IST)": _time_ist(row.get("timestamp")),
                "Liquidated position side": row.get("position_side_liquidated"),
                "Bankruptcy price": row.get("bankruptcy_price"),
                "Executed size (BTC)": row.get("size_btc"),
                "Approx. notional at bankruptcy (USDT)": row.get("notional_usdt_approx_at_bankruptcy_price"),
            } for row in liquidations[:25]]), use_container_width=True, hide_index=True)
        else:
            st.info("No liquidation messages have been observed in this process yet. This does not prove that no liquidations occurred.")

        buckets = aggregate_observed_bankruptcy_buckets(
            liquidations, now_epoch=now, window_seconds=3600, bucket_width=100
        )
        if buckets:
            st.markdown("**Observed messages by USDT 100 bankruptcy-price bucket · trailing 60 minutes**")
            st.dataframe(pd.DataFrame([{
                "Bankruptcy price bucket": f"USDT {row['bankruptcy_price_bucket_low']:,.0f}–{row['bankruptcy_price_bucket_high']:,.0f}",
                "Liquidated position side": row["position_side_liquidated"],
                "Observed messages": row["observed_liquidation_messages"],
                "Observed executed size (BTC)": row["observed_size_btc"],
                "Approx. notional at bankruptcy (USDT)": row["notional_at_bankruptcy_price_usdt_approx"],
            } for row in buckets[:20]]), use_container_width=True, hide_index=True)
        else:
            st.info("No qualifying observed liquidation messages are retained in the last 60 minutes.")

    with st.expander("Bybit feed diagnostics and candle-history coverage", expanded=False):
        channel_rows = []
        for topic in sorted(set(snap.get("last_message_by_topic", {})) | set(snap.get("message_counts", {}))):
            ts = (snap.get("last_message_by_topic") or {}).get(topic)
            channel_rows.append({
                "Topic": topic,
                "Messages": (snap.get("message_counts") or {}).get(topic, 0),
                "Last message (IST)": _time_ist(ts),
                "Age (seconds)": round(max(0.0, now - float(ts)), 2) if ts else None,
            })
        if channel_rows:
            st.dataframe(pd.DataFrame(channel_rows), use_container_width=True, hide_index=True)
        st.caption(
            "REST host status: " + str(snap.get("rest_endpoint_health", "not checked")) +
            " · active host: " + str(snap.get("rest_endpoint_active") or "none")
        )
        diag = st.columns(4)
        diag[0].metric("Messages total", str(snap.get("message_count", 0)))
        diag[1].metric("Disconnects", str(snap.get("total_disconnects", 0)))
        diag[2].metric("Reconnects", str(snap.get("total_reconnects", 0)))
        diag[3].metric("Depth resubscriptions", str(snap.get("depth_restarts", 0)))
        history = snap.get("candle_history_loaded") or {}
        updated = snap.get("candle_history_updated_at") or {}
        errors = snap.get("candle_history_errors") or {}
        st.dataframe(pd.DataFrame([{
            "Timeframe": tf,
            "History loaded": bool(history.get(tf)),
            "Bars in memory": len((snap.get("candles_by_interval") or {}).get(tf, [])),
            "Last REST history update (IST)": _time_ist(updated.get(tf)),
            "Error": errors.get(tf) or "—",
        } for tf in INTERVAL_SECONDS]), use_container_width=True, hide_index=True)
        if snap.get("last_error"):
            st.error("WebSocket: " + str(snap["last_error"]))
        if snap.get("last_rest_error"):
            st.error("REST: " + str(snap["last_rest_error"]))

    st.caption(
        "Read-only public data: no API keys, account data, or order endpoints. Historical candles and ticker snapshots "
        "come from Bybit V5 REST; prices, book deltas, trades, klines and observed liquidation messages come from its public WebSocket."
    )


@st.fragment(run_every=2.0)
def _bybit_panel_fragment(feed: BybitV5Feed, delta_feed: Any) -> None:
    _render_bybit_panel(feed, delta_feed)


def render_bybit_cross_venue_panel(delta_feed: Any = None) -> dict[str, Any]:
    feed = get_bybit_feed()
    _bybit_panel_fragment(feed, delta_feed)
    context = feed.analyst_context()
    snapshot = feed.snapshot()
    context["technical_indicators_by_timeframe"] = _technical_by_timeframe(snapshot, time.time())
    context["technical_indicators_1m"] = context["technical_indicators_by_timeframe"]["1m"]
    context["candle_history_by_timeframe"] = {
        timeframe: {
            "loaded_from_rest": bool((snapshot.get("candle_history_loaded") or {}).get(timeframe)),
            "candles_in_memory": len((snapshot.get("candles_by_interval") or {}).get(timeframe, [])),
            "history_updated_utc": (
                datetime.fromtimestamp(float((snapshot.get("candle_history_updated_at") or {})[timeframe]), tz=ZoneInfo("UTC")).isoformat()
                if (snapshot.get("candle_history_updated_at") or {}).get(timeframe) else None
            ),
            "history_error": (snapshot.get("candle_history_errors") or {}).get(timeframe) or None,
        }
        for timeframe in INTERVAL_SECONDS
    }
    delta_data = delta_feed.snapshot() if delta_feed is not None else {}
    delta_price = delta_data.get("last_price") if isinstance(delta_data, dict) else None
    alternative_price = context.get("price", {}).get("last_trade")
    try:
        delta_f, alt_f = float(delta_price), float(alternative_price)
        context["raw_price_comparison_vs_delta"] = {
            "delta_btcusd_last_price": delta_f,
            "bybit_btcusdt_last_price": alt_f,
            "bybit_minus_delta_quote_price": alt_f - delta_f,
            "percentage_difference_vs_delta": (alt_f - delta_f) / delta_f * 100 if delta_f > 0 else None,
            "interpretation_limit": "Raw quote comparison, not executable arbitrage; contract design, quote/collateral basis, fees, funding, latency and slippage differ.",
        }
    except (TypeError, ValueError, ZeroDivisionError):
        context["raw_price_comparison_vs_delta"] = {
            "delta_btcusd_last_price": None,
            "bybit_btcusdt_last_price": alternative_price,
            "bybit_minus_delta_quote_price": None,
            "percentage_difference_vs_delta": None,
            "interpretation_limit": "Unavailable until both venues have valid positive last-trade prices.",
        }
    return context
