"""Streamlit UI for the separate Binance USDⓈ-M cross-venue monitor."""
from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ai_analyst import _technical_indicators

from binance_market import (
    BinanceUSDMFeed,
    DEFAULT_SYMBOL,
    aggregate_observed_liquidations,
)

IST = ZoneInfo("Asia/Kolkata")


@st.cache_resource(show_spinner=False)
def get_binance_feed(symbol: str = DEFAULT_SYMBOL) -> BinanceUSDMFeed:
    """Share one background public-feed connection across Streamlit reruns."""
    feed = BinanceUSDMFeed(symbol=symbol)
    feed.start()
    return feed


def _price(value: Any, digits: int = 2) -> str:
    try:
        number = float(value)
        return f"USD {number:,.{digits}f}" if number > 0 else "—"
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


def _render_binance_panel(feed: BinanceUSDMFeed, delta_feed: Any) -> None:
    """Live UI fragment: data is refreshed independently of the Delta chart."""
    snap = feed.snapshot()
    delta = delta_feed.snapshot() if delta_feed is not None else {}
    now = time.time()
    last_message = snap.get("last_market_message_at") or snap.get("last_any_message_at")
    feed_age = now - float(last_message) if last_message else None

    st.markdown("---")
    st.subheader("Alternative exchange · Binance USDⓈ-M BTCUSDT")
    st.caption(
        "Independent public cross-venue feed. Quantities are in BTC (not Delta contracts); "
        "the Binance data is not substituted into the Delta signal engine or Gate 3 evaluator."
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Binance feed", str(snap.get("connection", "starting")))
    c2.metric("Binance book synchronized", "YES" if snap.get("book_synced") else "NO")
    c3.metric("Last market message", f"{feed_age:.1f}s ago" if feed_age is not None else "—")
    c4.metric("Binance last trade", _price(snap.get("last_price")))

    if not snap.get("book_synced"):
        st.warning(
            "Binance order book is not currently synchronized by update IDs. "
            "Do not interpret displayed depth as valid until synchronization recovers. "
            + str(snap.get("book_sync_error") or "")
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
    prices[4].metric("Spread", f"USD {spread:,.2f} / {spread_bps:.2f} bps" if spread is not None and spread_bps is not None else "—")

    delta_last = delta.get("last_price") if isinstance(delta, dict) else None
    binance_last = snap.get("last_price")
    try:
        delta_f, binance_f = float(delta_last), float(binance_last)
        delta_pct = (binance_f - delta_f) / delta_f * 100 if delta_f > 0 else None
        delta_diff = binance_f - delta_f
    except (TypeError, ValueError, ZeroDivisionError):
        delta_pct, delta_diff = None, None
    cross = st.columns(3)
    cross[0].metric("Delta last price", _price(delta_last))
    cross[1].metric("Binance − Delta last price", f"{delta_diff:+,.2f} USD" if delta_diff is not None else "—")
    cross[2].metric("Raw price difference", f"{delta_pct:+.4f}%" if delta_pct is not None else "—")
    st.caption(
        "Cross-venue price difference is a raw quote comparison, not an executable arbitrage spread. "
        "BTCUSD vs BTCUSDT contract design, quote/collateral basis, fees, funding, latency and slippage differ."
    )

    funding = snap.get("funding_rate")
    try:
        funding_display = f"{float(funding) * 100:+.5f}%" if funding is not None else "—"
    except (TypeError, ValueError):
        funding_display = "—"
    oi = snap.get("open_interest_btc")
    oi_usd = snap.get("open_interest_usd_approx")
    ticker = snap.get("ticker_24h") or {}
    flow = st.columns(5)
    flow[0].metric("Funding rate (per published period)", funding_display)
    flow[1].metric("Next funding time (IST)", _time_ist(snap.get("next_funding_at")))
    flow[2].metric("Open interest", _quantity(oi))
    flow[3].metric("OI notional (approx.)", _price(oi_usd, 0))
    flow[4].metric("24h price change", f"{float(ticker['mark_change_pct']):+.2f}%" if ticker.get("mark_change_pct") is not None else "—")
    if snap.get("open_interest_updated_at"):
        st.caption("Open interest is a REST snapshot; last update: " + _time_ist(snap.get("open_interest_updated_at")))
    if snap.get("last_rest_error"):
        st.caption("REST status: " + str(snap["last_rest_error"]))

    if snap.get("book_synced"):
        book_col1, book_col2 = st.columns(2)
        with book_col1:
            st.markdown("**Observed bids — BTC size and approximate notional**")
            rows = _top_book_rows(snap, "bids")
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            else:
                st.info("No valid bid levels in the synchronized snapshot.")
        with book_col2:
            st.markdown("**Observed asks — BTC size and approximate notional**")
            rows = _top_book_rows(snap, "asks")
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            else:
                st.info("No valid ask levels in the synchronized snapshot.")
        st.caption(
            f"Book update ID: {snap.get('book_last_update_id')} · "
            f"sequence gaps observed: {snap.get('sequence_gaps', 0)} · "
            "Binance does not provide the Delta CRC32 checksum in this adapter; update-ID continuity is used."
        )

    candles = [
        row for row in (snap.get("candles", []) or [])
        if isinstance(row, dict) and isinstance(row.get("time"), (int, float))
        and float(row["time"]) + 60 <= now
    ]
    if candles:
        chart_rows = candles[-180:]
        chart = go.Figure(data=[go.Candlestick(
            x=[datetime.fromtimestamp(float(row["time"]), tz=IST) for row in chart_rows],
            open=[row["open"] for row in chart_rows],
            high=[row["high"] for row in chart_rows],
            low=[row["low"] for row in chart_rows],
            close=[row["close"] for row in chart_rows],
            name="Binance BTCUSDT",
        )])
        chart.update_layout(
            title="Binance BTCUSDT · completed 1-minute candles",
            height=380,
            margin=dict(l=10, r=10, t=45, b=10),
            xaxis_rangeslider_visible=False,
            uirevision="binance-btcusdt-1m",
        )
        st.plotly_chart(chart, use_container_width=True, key="binance-usdm-1m-chart")
    else:
        st.info("Waiting for initial Binance candle history. The live chart appears once completed 1-minute candles are available.")

    technical = _technical_indicators(
        {"candles": snap.get("candles", [])},
        now_epoch=now,
        current_price=snap.get("last_price"),
    )
    st.markdown("**Calculated Binance 1-minute technical context**")
    tech_cols = st.columns(5)
    ema_values = technical.get("ema_20_50_144_200", {})
    tech_cols[0].metric("EMA alignment", str(technical.get("ema_alignment", "insufficient_lookback")).replace("_", " "))
    tech_cols[1].metric("EMA 20", _price((ema_values.get("20") or {}).get("value")))
    tech_cols[2].metric("EMA 50", _price((ema_values.get("50") or {}).get("value")))
    tech_cols[3].metric("ATR 14", _price(technical.get("atr_14_price")))
    volume_ratio = technical.get("latest_volume_vs_preceding_20_mean_ratio")
    tech_cols[4].metric("Latest / mean volume", f"{volume_ratio:.2f}×" if volume_ratio is not None else "—")
    extra_cols = st.columns(3)
    extra_cols[0].metric("EMA 144", _price((ema_values.get("144") or {}).get("value")))
    extra_cols[1].metric("EMA 200", _price((ema_values.get("200") or {}).get("value")))
    return_5 = technical.get("five_closed_bar_close_return_pct")
    extra_cols[2].metric("5 closed-bar return", f"{return_5:+.3f}%" if return_5 is not None else "—")
    st.caption(
        f"Technical calculations use {technical.get('completed_candle_count', 0)} completed 1-minute candles. "
        "EMA values use an initial-period SMA seed; ATR is the simple mean of the last 14 true ranges; "
        "volume ratio compares the latest completed candle with the previous 20-candle mean. "
        "These are context, not standalone signals."
    )

    recent_trades = snap.get("trades", [])[:20]
    if recent_trades:
        st.markdown("**Recent Binance aggTrade executions**")
        trade_rows = [{
            "Time (IST)": _time_ist(row.get("timestamp")),
            "Aggressor": row.get("side"),
            "Price": row.get("price"),
            "Size (BTC)": row.get("size_btc"),
            "Approx. notional (USDT)": row.get("notional_usd_approx"),
        } for row in recent_trades]
        st.dataframe(pd.DataFrame(trade_rows), use_container_width=True, hide_index=True)
    else:
        st.info("Waiting for Binance aggregate trades.")

    cvd = snap.get("cvd_value_btc")
    buy_volume, sell_volume = snap.get("buy_volume_btc"), snap.get("sell_volume_btc")
    trade_cols = st.columns(4)
    trade_cols[0].metric("Session CVD proxy", _quantity(cvd, 5))
    trade_cols[1].metric("Aggressive buy volume", _quantity(buy_volume, 5))
    trade_cols[2].metric("Aggressive sell volume", _quantity(sell_volume, 5))
    trade_cols[3].metric("Trades observed this session", str(snap.get("trade_count", 0)))
    st.caption(
        "CVD is calculated locally from aggTrade buyer-maker flags, in BTC, only since this process started. "
        "It resets on restart and is not Binance's historical CVD."
    )

    with st.expander("Observed Binance liquidation executions and price buckets", expanded=True):
        st.caption(
            "Source: Binance USDⓈ-M forceOrder WebSocket. Its documented snapshot stream publishes at most "
            "the latest liquidation order per symbol per 1-second interval. Results below are observed feed "
            "messages with reported filled quantity; they are not a complete liquidation census or predictive levels."
        )
        liquidations = snap.get("liquidations", [])
        recent_liquidations = liquidations[:25] if isinstance(liquidations, list) else []
        if recent_liquidations:
            liq_rows = [{
                "Time (IST)": _time_ist(row.get("timestamp")),
                "Order side": row.get("side"),
                "Price": row.get("price"),
                "Filled size reported (BTC)": row.get("size_btc"),
                "Approx. notional (USDT)": row.get("notional_usd_approx"),
                "Order status": row.get("order_status"),
            } for row in recent_liquidations]
            st.dataframe(pd.DataFrame(liq_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No filled force-order snapshots have been observed in this process yet. This is not proof that no liquidations occurred.")

        buckets = aggregate_observed_liquidations(
            liquidations if isinstance(liquidations, list) else [],
            now_epoch=now,
            window_seconds=3600,
            bucket_width=100.0,
        )
        if buckets:
            st.markdown("**Observed snapshot activity by USD 100 price bucket · trailing 60 minutes**")
            bucket_rows = [{
                "Price bucket": f"USD {row['price_bucket_low']:,.0f}–{row['price_bucket_high']:,.0f}",
                "Exchange order side": row["order_side"],
                "Observed snapshots": row["observed_snapshot_count"],
                "Reported filled size (BTC)": row["filled_qty_btc_reported"],
                "Approx. observed notional (USDT)": row["observed_notional_usd_approx"],
            } for row in buckets[:20]]
            st.dataframe(pd.DataFrame(bucket_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No qualifying filled liquidation snapshots in the trailing 60-minute window.")

    with st.expander("Binance source diagnostics by channel", expanded=False):
        seen = snap.get("channel_last_seen", {}) or {}
        counts = snap.get("channel_message_counts", {}) or {}
        channels = sorted(set(seen) | set(counts))
        channel_rows = [{
            "Channel": channel,
            "Messages received": counts.get(channel, 0),
            "Last message (IST)": _time_ist(seen.get(channel)),
            "Age (seconds)": round(max(0.0, now - float(seen[channel])), 2) if seen.get(channel) else None,
        } for channel in channels]
        if channel_rows:
            st.dataframe(pd.DataFrame(channel_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No Binance public channel messages have arrived yet.")
        diag_cols = st.columns(4)
        diag_cols[0].metric("Messages total", str(snap.get("total_messages_received", 0)))
        diag_cols[1].metric("Disconnects", str(snap.get("total_disconnects", 0)))
        diag_cols[2].metric("Reconnects", str(snap.get("total_reconnects", 0)))
        diag_cols[3].metric("Depth sequence gaps", str(snap.get("sequence_gaps", 0)))
        st.caption("REST last poll (IST): " + _time_ist(snap.get("last_rest_poll_at")))
        st.caption("24-hour ticker last update (IST): " + _time_ist(snap.get("ticker_24h_updated_at")))
        if snap.get("last_error"):
            st.error("WebSocket: " + str(snap["last_error"]))
        if snap.get("last_rest_error"):
            st.error("REST: " + str(snap["last_rest_error"]))

    st.caption(
        "Historical candles: Binance REST klines. Live price/trades/depth/mark/funding/liquidation: public WebSocket. "
        "Open interest and 24-hour statistics: public REST polling. All market-data fields remain read-only."
    )


@st.fragment(run_every=2.0)
def _binance_panel_fragment(feed: BinanceUSDMFeed, delta_feed: Any) -> None:
    _render_binance_panel(feed, delta_feed)


def render_binance_cross_venue_panel(delta_feed: Any = None) -> dict[str, Any]:
    """Render live cross-venue monitoring and return compact analyst context."""
    feed = get_binance_feed()
    _binance_panel_fragment(feed, delta_feed)
    context = feed.analyst_context()
    binance_snapshot = feed.snapshot()
    context["technical_indicators_1m"] = _technical_indicators(
        {"candles": binance_snapshot.get("candles", [])},
        now_epoch=time.time(),
        current_price=binance_snapshot.get("last_price"),
    )
    delta_data = delta_feed.snapshot() if delta_feed is not None else {}
    delta_price = delta_data.get("last_price") if isinstance(delta_data, dict) else None
    binance_price = context.get("price", {}).get("last_trade")
    try:
        d, b = float(delta_price), float(binance_price)
        context["raw_price_comparison_vs_delta"] = {
            "delta_btcusd_last_price": d,
            "binance_btcusdt_last_price": b,
            "binance_minus_delta_quote_price": b - d,
            "percentage_difference_vs_delta": (b - d) / d * 100 if d > 0 else None,
            "interpretation_limit": "Raw quote comparison only; not an executable arbitrage spread due to instrument, quote/collateral basis, fees, funding, latency and slippage.",
        }
    except (TypeError, ValueError, ZeroDivisionError):
        context["raw_price_comparison_vs_delta"] = {
            "delta_btcusd_last_price": None,
            "binance_btcusdt_last_price": binance_price,
            "binance_minus_delta_quote_price": None,
            "percentage_difference_vs_delta": None,
            "interpretation_limit": "Comparison unavailable until both venues have valid positive last-trade prices.",
        }
    return context
