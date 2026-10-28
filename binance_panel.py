"""Streamlit UI for isolated Binance USDⓈ-M public market data."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ai_analyst import _technical_indicators
from binance_market import BinanceUSDMFeed, TIMEFRAMES

IST = ZoneInfo("Asia/Kolkata")


@st.cache_resource(show_spinner=False)
def get_binance_feed(symbol: str = "BTCUSDT") -> BinanceUSDMFeed:
    feed = BinanceUSDMFeed(symbol=symbol)
    feed.start()
    return feed


def _price(value: Any, digits: int = 2) -> str:
    try:
        number = float(value)
        return f"{number:,.{digits}f}" if number > 0 else "—"
    except (TypeError, ValueError, OverflowError):
        return "—"


def _time_ist(value: Any) -> str:
    try:
        return datetime.fromtimestamp(float(value), tz=IST).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OverflowError, OSError):
        return "—"


def _top_book_rows(book: dict[float, float], *, reverse: bool, limit: int = 10) -> pd.DataFrame:
    levels = sorted(book.items(), key=lambda item: float(item[0]), reverse=reverse)[:limit]
    cumulative = 0.0
    rows = []
    for price, qty in levels:
        cumulative += float(qty)
        rows.append({
            "Price (USDT)": float(price),
            "Quantity (BTC)": float(qty),
            "Cumulative (BTC)": cumulative,
        })
    return pd.DataFrame(rows, columns=["Price (USDT)", "Quantity (BTC)", "Cumulative (BTC)"])


def _technical_by_timeframe(snapshot: dict[str, Any], now_epoch: float) -> dict[str, dict[str, Any]]:
    candle_sets = snapshot.get("candles_by_interval") or {}
    current_price = snapshot.get("last_price")
    result: dict[str, dict[str, Any]] = {}
    for timeframe, seconds in TIMEFRAMES.items():
        rows = candle_sets.get(timeframe, [])
        result[timeframe] = _technical_indicators(
            {"candles": rows, "candle_interval_seconds": seconds, "timeframe": timeframe},
            now_epoch=now_epoch,
            current_price=current_price,
        )
    return result


def _render_binance_panel(feed: BinanceUSDMFeed, delta_feed: Any = None) -> None:
    with st.expander("Cross-exchange reference — Binance USDⓈ-M BTCUSDT (read-only)", expanded=False):
        d = feed.snapshot()
        delta = delta_feed.snapshot() if delta_feed is not None else {}
        now = time.time()
        market_ts = d.get("last_market_message_at")
        market_age = None if market_ts is None else max(0.0, now - float(market_ts))
        book_ts = d.get("book_updated_at")
        book_age = None if book_ts is None else max(0.0, now - float(book_ts))
        oi_ts = d.get("open_interest_updated_at")
        oi_age = None if oi_ts is None else max(0.0, now - float(oi_ts))
        funding_ts = d.get("funding_updated_at")
        funding_age = None if funding_ts is None else max(0.0, now - float(funding_ts))

        if d.get("connection") == "connected" and market_age is not None and market_age <= 6:
            st.success(f"Binance public stream connected · last received market message {market_age:.2f}s ago")
        elif d.get("connection") == "connected":
            st.warning("Binance WebSocket is connected, but market data is stale or has not arrived.")
        else:
            st.warning(f"Binance public feed status: {d.get('connection', 'unknown')}")

        st.caption(
            "Separate BTCUSDT context. Binance values are not Delta BTCUSD contract data and do not modify "
            "the Delta signal engine, the frozen evaluator, or Gate 3."
        )

        bid, ask = d.get("bid"), d.get("ask")
        spread = ask - bid if bid is not None and ask is not None and ask >= bid else None
        spread_bps = spread / ((ask + bid) / 2) * 10_000 if spread is not None and (ask + bid) > 0 else None
        funding = d.get("funding_rate")
        oi = d.get("open_interest_btc")
        delta_price = delta.get("last_price") if isinstance(delta, dict) else None
        binance_price = d.get("last_price")
        raw_difference = None
        try:
            if float(delta_price) > 0 and float(binance_price) > 0:
                raw_difference = float(binance_price) - float(delta_price)
        except (TypeError, ValueError, OverflowError):
            pass
        change_pct = (d.get("ticker_24h") or {}).get("price_change_percent")

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Last trade (USDT)", _price(binance_price))
        c2.metric("Mark price", _price(d.get("mark_price")))
        c3.metric("Index price", _price(d.get("index_price")))
        c4.metric("Best bid", _price(bid), help=f"Top bid size: {_price(d.get('bid_qty_btc'), 5)} BTC")
        c5.metric("Best ask", _price(ask), help=f"Top ask size: {_price(d.get('ask_qty_btc'), 5)} BTC")
        c6.metric("Spread", "—" if spread is None else f"{spread:.2f} / {spread_bps:.2f} bps")

        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Funding rate", "—" if funding is None else f"{funding * 100:.5f}%")
        d2.metric("Open interest", _price(oi, 4) + (" BTC" if oi is not None else ""))
        d3.metric("24h price change", "—" if change_pct is None else f"{change_pct:.3f}%")
        d4.metric("Session CVD proxy", f"{float(d.get('cvd_btc') or 0):+,.4f} BTC")
        if funding is not None and (funding_age is None or funding_age > 10):
            st.warning(f"Binance funding value is stale or its timestamp is unavailable (age: {'—' if funding_age is None else f'{funding_age:.1f}s'}). Treat it as unverified current funding.")
        if oi is not None and (oi_age is None or oi_age > 45):
            st.warning(f"Binance open interest is stale or its timestamp is unavailable (age: {'—' if oi_age is None else f'{oi_age:.1f}s'}).")

        if raw_difference is not None:
            st.caption(
                f"Raw numeric quote difference: Binance BTCUSDT price minus Delta BTCUSD price = {raw_difference:+,.2f}. "
                "Quote/settlement conventions and contracts differ, so this is contextual only—not an executable spread."
            )

        status_cols = st.columns(4)
        status_cols[0].metric("Book synchronization", "SYNCED" if d.get("book_synced") else "INVALID / WAITING")
        status_cols[1].metric("Depth update age", "—" if book_age is None else f"{book_age:.2f}s")
        status_cols[2].metric("Sequence gaps", f"{int(d.get('book_sequence_gaps') or 0):,}")
        status_cols[3].metric("Snapshot resyncs", f"{int(d.get('book_resync_count') or 0):,}")
        st.caption(
            f"Book state: {d.get('book_sync_state', 'unknown')} · "
            f"lastUpdateId: {d.get('book_update_id') if d.get('book_update_id') is not None else '—'} · "
            f"snapshot ID: {d.get('book_snapshot_update_id') if d.get('book_snapshot_update_id') is not None else '—'} · "
            f"open-interest age: {'—' if oi_age is None else f'{oi_age:.1f}s'} · "
            f"active REST host: {d.get('rest_host_active') or '—'}"
        )
        st.warning(
            "Binance's USDⓈ-M public diff-depth stream has no exchange-provided CRC32 checksum. "
            "This book is valid only after REST-snapshot/update-ID synchronization; it is not CRC-verified. "
            "The initial REST snapshot contains at most 1,000 levels per side."
        )
        if d.get("last_error"):
            st.error(str(d["last_error"]))
        if d.get("last_rest_error"):
            st.warning("Depth REST: " + str(d["last_rest_error"]))
        if d.get("open_interest_error") and oi is None:
            st.warning("Open interest unavailable: " + str(d["open_interest_error"]))

        book_is_fresh = bool(d.get("book_synced")) and book_age is not None and book_age <= 3
        if book_is_fresh:
            left, right = st.columns(2)
            with left:
                st.markdown("**Live Binance bids · synchronized local book**")
                bid_df = _top_book_rows(d.get("book_bids", {}), reverse=True)
                st.dataframe(
                    bid_df.style.format({"Price (USDT)": "{:,.2f}", "Quantity (BTC)": "{:,.5f}", "Cumulative (BTC)": "{:,.5f}"}),
                    hide_index=True, use_container_width=True, height=270,
                )
            with right:
                st.markdown("**Live Binance asks · synchronized local book**")
                ask_df = _top_book_rows(d.get("book_asks", {}), reverse=False)
                st.dataframe(
                    ask_df.style.format({"Price (USDT)": "{:,.2f}", "Quantity (BTC)": "{:,.5f}", "Cumulative (BTC)": "{:,.5f}"}),
                    hide_index=True, use_container_width=True, height=270,
                )
        else:
            st.warning("Order book is not synchronized and fresh. Do not use its levels or imbalance until a new REST snapshot and contiguous depth updates are confirmed.")

        candles = d.get("candles_by_interval") or {}
        closed_1m = [row for row in candles.get("1m", []) if row.get("is_closed")]
        if closed_1m:
            chart_rows = closed_1m[-180:]
            chart_df = pd.DataFrame(chart_rows)
            chart_df["time_ist"] = pd.to_datetime(chart_df["time"], unit="s", utc=True).dt.tz_convert(IST)
            figure = go.Figure(data=[go.Candlestick(
                x=chart_df["time_ist"],
                open=chart_df["open"], high=chart_df["high"],
                low=chart_df["low"], close=chart_df["close"],
                name="Binance BTCUSDT 1m",
            )])
            figure.update_layout(
                template="plotly_dark", height=360, margin=dict(l=8, r=8, t=15, b=8),
                xaxis_title="Time (IST)", yaxis_title="Price (USDT)",
                xaxis_rangeslider_visible=False,
            )
            st.markdown("**Binance-native 1-minute candles (closed bars only)**")
            st.plotly_chart(figure, use_container_width=True, key="binance_usdm_btcusdt_1m_chart")
        else:
            st.info("Waiting for Binance-native closed 1-minute candles. No candles are substituted from Delta, Bybit or OKX.")

        technical = _technical_by_timeframe(d, now)
        tech_rows = []
        for timeframe in TIMEFRAMES:
            item = technical[timeframe]
            ema = item.get("ema_20_50_144_200") or {}
            tech_rows.append({
                "Timeframe": timeframe,
                "Closed bars": item.get("completed_candle_count", 0),
                "EMA alignment": item.get("ema_alignment", "insufficient_lookback"),
                "EMA 20": (ema.get("20") or {}).get("value"),
                "EMA 50": (ema.get("50") or {}).get("value"),
                "EMA 144": (ema.get("144") or {}).get("value"),
                "EMA 200": (ema.get("200") or {}).get("value"),
                "ATR(14)": item.get("atr_14_price"),
                "Volume / prior 20 mean": item.get("latest_volume_vs_preceding_20_mean_ratio"),
                "5-bar return %": item.get("five_closed_bar_close_return_pct"),
                "20-bar structure": item.get("last_20_vs_previous_20_bar_structure"),
            })
        st.markdown("**Binance-native technical metrics by timeframe**")
        tech_df = pd.DataFrame(tech_rows)
        st.dataframe(
            tech_df.style.format({
                "EMA 20": "{:,.2f}", "EMA 50": "{:,.2f}",
                "EMA 144": "{:,.2f}", "EMA 200": "{:,.2f}",
                "ATR(14)": "{:,.2f}", "Volume / prior 20 mean": "{:.3f}",
                "5-bar return %": "{:+.3f}",
            }, na_rep="—"),
            hide_index=True, use_container_width=True,
        )
        st.caption("EMA/ATR/volume/structure values use Binance's own completed candles only. Missing lookback remains unavailable.")

        trades = d.get("recent_trades") or []
        if trades:
            st.markdown("**Recent Binance aggregate trades**")
            trade_rows = []
            for item in trades[:15]:
                trade_rows.append({
                    "Time (IST)": _time_ist(float(item.get("time_ms") or 0) / 1000),
                    "Taker side": item.get("side"),
                    "Price (USDT)": item.get("price_usdt"),
                    "Size (BTC)": item.get("qty_btc"),
                })
            trade_df = pd.DataFrame(trade_rows)
            st.dataframe(
                trade_df.style.format({"Price (USDT)": "{:,.2f}", "Size (BTC)": "{:,.5f}"}),
                hide_index=True, use_container_width=True, height=300,
            )

        cvd_history = d.get("cvd_history") or []
        if len(cvd_history) > 1:
            cvd_df = pd.DataFrame(cvd_history)
            cvd_df["time_ist"] = pd.to_datetime(cvd_df["time_ms"], unit="ms", utc=True).dt.tz_convert(IST)
            cvd_fig = go.Figure()
            cvd_fig.add_trace(go.Scatter(x=cvd_df["time_ist"], y=cvd_df["cvd_btc"], mode="lines", name="CVD proxy"))
            cvd_fig.update_layout(
                template="plotly_dark", height=240, margin=dict(l=8, r=8, t=12, b=8),
                xaxis_title="Time (IST)", yaxis_title="Cumulative signed BTC",
            )
            st.markdown("**Binance session CVD proxy**")
            st.plotly_chart(cvd_fig, use_container_width=True, key="binance_usdm_cvd_chart")

        liquidation_events = d.get("liquidations") or []
        if liquidation_events:
            st.markdown("**Observed Binance force-order samples**")
            liquidation_rows = []
            for item in liquidation_events[:15]:
                liquidation_rows.append({
                    "Event time (IST)": _time_ist(float(item.get("time_ms") or 0) / 1000),
                    "Side": item.get("side"),
                    "Status": item.get("status"),
                    "Price (USDT)": item.get("price_usdt"),
                    "Quantity (BTC)": item.get("qty_btc"),
                })
            liquidation_df = pd.DataFrame(liquidation_rows)
            st.dataframe(
                liquidation_df.style.format({"Price (USDT)": "{:,.2f}", "Quantity (BTC)": "{:,.5f}"}),
                hide_index=True, use_container_width=True, height=260,
            )
        st.caption(
            "Binance forceOrder is a sampled per-symbol event stream, not a complete liquidation census or predictive liquidation map. "
            "CVD resets when the process restarts. Quote-price differences against Delta are contextual only—not an executable spread."
        )


@st.fragment(run_every=2.0)
def _binance_panel_fragment(feed: BinanceUSDMFeed, delta_feed: Any) -> None:
    _render_binance_panel(feed, delta_feed)


def render_binance_cross_venue_panel(delta_feed: Any = None) -> dict[str, Any]:
    feed = get_binance_feed()
    _binance_panel_fragment(feed, delta_feed)
    context = feed.analyst_context()
    snapshot = feed.snapshot()
    context["technical_indicators_by_timeframe"] = _technical_by_timeframe(snapshot, time.time())
    delta_data = delta_feed.snapshot() if delta_feed is not None else {}
    delta_price = delta_data.get("last_price") if isinstance(delta_data, dict) else None
    binance_price = context.get("price", {}).get("last_trade_usdt")
    try:
        delta_value, binance_value = float(delta_price), float(binance_price)
        if delta_value > 0 and binance_value > 0:
            context["raw_price_comparison_vs_delta"] = {
                "delta_btcusd_last_price": delta_value,
                "binance_btcusdt_last_price": binance_value,
                "raw_quote_difference": binance_value - delta_value,
                "percentage_difference_vs_delta": (binance_value - delta_value) / delta_value * 100,
                "interpretation_limit": "Raw quote comparison only; not executable arbitrage because quote currency, contract design, collateral basis, fees, funding, latency and slippage differ.",
            }
        else:
            raise ValueError("prices must be positive")
    except (TypeError, ValueError, ZeroDivisionError):
        context["raw_price_comparison_vs_delta"] = {
            "delta_btcusd_last_price": delta_price,
            "binance_btcusdt_last_price": binance_price,
            "raw_quote_difference": None,
            "percentage_difference_vs_delta": None,
            "interpretation_limit": "Unavailable until both venues have valid positive last-trade prices.",
        }
    context["source_data"] = {
        "candles_by_timeframe": {
            tf: {
                "bars_in_memory": len((snapshot.get("candles_by_interval") or {}).get(tf, [])),
                "closed_bars": sum(bool(row.get("is_closed")) for row in (snapshot.get("candles_by_interval") or {}).get(tf, [])),
            }
            for tf in TIMEFRAMES
        },
        "not_merged_with_delta": True,
        "liquidations_are_sampled": True,
    }
    return context
