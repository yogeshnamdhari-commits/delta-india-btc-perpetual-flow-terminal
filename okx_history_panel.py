"""Separate OKX public candle-history reference panel; never mixed with Bybit/Delta."""
from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ai_analyst import _technical_indicators
from okx_history import DEFAULT_INST_ID, OKXHistoryFeed, TIMEFRAMES

IST = ZoneInfo("Asia/Kolkata")


@st.cache_resource(show_spinner=False)
def get_okx_history_feed(inst_id: str = DEFAULT_INST_ID) -> OKXHistoryFeed:
    feed = OKXHistoryFeed(inst_id=inst_id)
    feed.start()
    return feed


def _price(value: Any, digits: int = 2) -> str:
    try:
        number = float(value)
        return f"USDT {number:,.{digits}f}" if number > 0 else "—"
    except (TypeError, ValueError, OverflowError):
        return "—"


def _time_ist(value: Any) -> str:
    try:
        return datetime.fromtimestamp(float(value), tz=IST).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OverflowError, OSError):
        return "—"


def _technical_sets(snapshot: dict[str, Any], now_epoch: float) -> dict[str, dict[str, Any]]:
    candle_sets = snapshot.get("candles_by_interval") or {}
    result: dict[str, dict[str, Any]] = {}
    for timeframe in TIMEFRAMES:
        rows = candle_sets.get(timeframe, [])
        latest = next((row for row in reversed(rows) if row.get("is_closed")), None)
        result[timeframe] = _technical_indicators(
            {"candles": rows, "timeframe": timeframe},
            now_epoch=now_epoch,
            current_price=latest.get("close") if latest else None,
        )
    return result


def _render_okx_history(feed: OKXHistoryFeed) -> None:
    snap = feed.snapshot()
    now = time.time()
    last_success = snap.get("last_success_at")
    age = now - float(last_success) if last_success else None
    history = snap.get("history_loaded") or {}
    candles_by_tf = snap.get("candles_by_interval") or {}
    updated = snap.get("history_updated_at") or {}
    errors = snap.get("history_errors") or {}

    st.markdown("---")
    st.subheader("Independent historical reference · OKX BTC-USDT-SWAP")
    st.caption(
        "Separate public candle-history source added as a fallback reference when Bybit historical REST is blocked. "
        "OKX candles, volume and technical calculations are never merged into Bybit or Delta data, signal inputs, Gate 3 or the frozen evaluator."
    )
    cols = st.columns(4)
    cols[0].metric("OKX candle API", str(snap.get("connection", "not_started")).upper())
    cols[1].metric("History endpoint", str(snap.get("rest_endpoint_health", "not_checked")))
    cols[2].metric("Last successful poll", f"{age:.0f}s ago" if age is not None else "—")
    cols[3].metric("Instrument", str(snap.get("inst_id", DEFAULT_INST_ID)))

    loaded_frames = [tf for tf in TIMEFRAMES if history.get(tf)]
    if len(loaded_frames) < len(TIMEFRAMES):
        missing = [tf for tf in TIMEFRAMES if not history.get(tf)]
        st.warning(
            "OKX history unavailable or partial for: " + ", ".join(missing) +
            ". The dashboard will not fill missing timeframes with Bybit/Delta candles."
        )
    if snap.get("last_error"):
        st.error(str(snap["last_error"]))

    candles_1m = [row for row in candles_by_tf.get("1m", []) if row.get("is_closed")]
    if candles_1m:
        rows = candles_1m[-180:]
        fig = go.Figure(data=[go.Candlestick(
            x=[datetime.fromtimestamp(float(row["time"]), tz=IST) for row in rows],
            open=[row["open"] for row in rows],
            high=[row["high"] for row in rows],
            low=[row["low"] for row in rows],
            close=[row["close"] for row in rows],
            name="OKX BTC-USDT-SWAP",
        )])
        fig.update_layout(
            title="OKX BTC-USDT-SWAP · completed 1-minute candles (historical reference)",
            height=340,
            margin=dict(l=10, r=10, t=45, b=10),
            xaxis_rangeslider_visible=False,
            uirevision="okx-btc-usdt-swap-1m",
        )
        st.plotly_chart(fig, use_container_width=True, key="okx-history-btcusdt-swap-1m")
    else:
        st.info("No valid closed OKX one-minute candles are loaded yet.")

    metrics = _technical_sets(snap, now)
    table = []
    for timeframe in TIMEFRAMES:
        item = metrics[timeframe]
        emas = item.get("ema_20_50_144_200", {})
        ret = item.get("five_closed_bar_close_return_pct")
        atr_pct = item.get("atr_14_pct_of_last_closed_close")
        volume_ratio = item.get("latest_volume_vs_preceding_20_mean_ratio")
        table.append({
            "Timeframe": timeframe,
            "Bars loaded": len(candles_by_tf.get(timeframe, [])),
            "Closed bars": item.get("completed_candle_count", 0),
            "EMA alignment": str(item.get("ema_alignment", "insufficient_lookback")).replace("_", " "),
            "EMA 20": _price((emas.get("20") or {}).get("value")),
            "EMA 50": _price((emas.get("50") or {}).get("value")),
            "EMA 144": _price((emas.get("144") or {}).get("value")),
            "EMA 200": _price((emas.get("200") or {}).get("value")),
            "ATR 14": _price(item.get("atr_14_price")),
            "ATR %": f"{atr_pct:.3f}%" if atr_pct is not None else "—",
            "Last volume / prior-20 mean": f"{volume_ratio:.2f}×" if volume_ratio is not None else "—",
            "5 closed-bar return": f"{ret:+.3f}%" if ret is not None else "—",
            "20-bar structure": str(item.get("last_20_vs_previous_20_bar_structure", "—")).replace("_", " "),
        })
    st.markdown("**Calculated OKX multi-timeframe context**")
    st.dataframe(pd.DataFrame(table), use_container_width=True, hide_index=True)
    st.caption(
        "Each timeframe is calculated only from completed OKX candles. EMA 20/50/144/200 uses an initial-period SMA seed; "
        "ATR(14) is the mean of the latest 14 true ranges; volume ratio compares last closed volume with the prior 20-bar mean. "
        "OKX swap candle volume is kept as base-BTC volume separately from reported contract volume and USDT turnover."
    )

    with st.expander("OKX history freshness and source diagnostics", expanded=False):
        st.caption(
            "Active public REST host: " + str(snap.get("rest_endpoint_active") or "none") +
            " · endpoint health: " + str(snap.get("rest_endpoint_health", "not_checked")) +
            " · last poll (IST): " + _time_ist(snap.get("last_poll_at"))
        )
        st.dataframe(pd.DataFrame([{
            "Timeframe": tf,
            "Bar": TIMEFRAMES[tf][0],
            "Loaded": bool(history.get(tf)),
            "Candles in memory": len(candles_by_tf.get(tf, [])),
            "REST host used": (snap.get("history_source_by_interval") or {}).get(tf) or "—",
            "Updated (IST)": _time_ist(updated.get(tf)),
            "Latest closed bar (IST)": _time_ist(max(
                (float(row["time"]) for row in candles_by_tf.get(tf, []) if row.get("is_closed")),
                default=None,
            )),
            "Error": errors.get(tf) or "—",
        } for tf in TIMEFRAMES]), use_container_width=True, hide_index=True)
        st.caption(
            "This is a candle-only REST reference: no OKX live trade tape, L2 order book, funding or liquidation stream is represented in this panel."
        )


@st.fragment(run_every=15.0)
def _okx_history_fragment(feed: OKXHistoryFeed) -> None:
    _render_okx_history(feed)


def render_okx_history_panel() -> dict[str, Any]:
    feed = get_okx_history_feed()
    _okx_history_fragment(feed)
    snapshot = feed.snapshot()
    context = feed.analyst_context()
    context["technical_indicators_by_timeframe"] = _technical_sets(snapshot, time.time())
    context["source_data"] = {
        "candles_only": True,
        "not_merged_with_other_venues": True,
        "bars_available_by_timeframe": {
            tf: len((snapshot.get("candles_by_interval") or {}).get(tf, []))
            for tf in TIMEFRAMES
        },
    }
    return context
