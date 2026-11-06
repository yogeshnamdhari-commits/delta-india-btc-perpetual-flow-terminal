"""Optional read-only CryptoQuant exchange-flow and taker-value panel."""
from __future__ import annotations

import os
from typing import Any

import pandas as pd
import streamlit as st

from exchange_flows import CryptoQuantAPIError, fetch_exchange_flow_data


FLOW_EXCHANGES = {
    "All exchanges": "all_exchange",
    "Spot exchanges": "spot_exchange",
    "Derivative exchanges": "derivative_exchange",
}
TAKER_EXCHANGES = {
    "All exchanges": "all_exchange",
    "Binance": "binance",
    "Bybit": "bybit",
    "BitMEX": "bitmex",
    "Deribit": "deribit",
    "HTX Global": "htx_global",
    "OKX": "okx",
}


def _cryptoquant_api_key() -> str:
    value = os.environ.get("CRYPTOQUANT_API_KEY", "")
    if value:
        return value.strip()
    try:
        secret = st.secrets.get("CRYPTOQUANT_API_KEY", "")
        return str(secret).strip() if secret else ""
    except Exception:
        return ""


@st.cache_data(ttl=60, show_spinner=False)
def _cached_cryptoquant_data(
    api_key: str,
    flow_exchange: str,
    taker_exchange: str,
    flow_window: str,
    taker_window: str,
) -> dict[str, Any]:
    return fetch_exchange_flow_data(
        api_key,
        flow_exchange=flow_exchange,
        taker_exchange=taker_exchange,
        flow_window=flow_window,
        taker_window=taker_window,
        limit=30,
    )


def _btc(value: Any) -> str:
    try:
        number = float(value)
        return f"{number:,.4f} BTC" if pd.notna(number) else "—"
    except (TypeError, ValueError, OverflowError):
        return "—"


def _usd(value: Any) -> str:
    try:
        number = float(value)
        return f"USD {number:,.0f}" if pd.notna(number) else "—"
    except (TypeError, ValueError, OverflowError):
        return "—"


def _pct(value: Any) -> str:
    try:
        number = float(value)
        return f"{number:.2f}%" if pd.notna(number) else "—"
    except (TypeError, ValueError, OverflowError):
        return "—"


def _render_wallet_flows(data: dict[str, Any]) -> None:
    rows = data.get("flow_rows") or []
    if data.get("flow_error"):
        st.warning(str(data["flow_error"]))
    if data.get("price_error"):
        st.caption("USD conversion unavailable: " + str(data["price_error"]))
    if not rows:
        if not data.get("flow_error"):
            st.info("No exchange wallet-flow records were returned for the selected window.")
        return

    latest = rows[-1]
    c1, c2, c3 = st.columns(3)
    c1.metric("Exchange inflow", _btc(latest.get("inflow_btc")), help="BTC transferred into labelled exchange wallets.")
    c2.metric("Exchange outflow", _btc(latest.get("outflow_btc")), help="BTC transferred out of labelled exchange wallets.")
    netflow = latest.get("netflow_btc")
    c3.metric(
        "Net exchange flow",
        _btc(netflow),
        delta="Net inflow" if netflow is not None and netflow > 0 else ("Net outflow" if netflow is not None and netflow < 0 else None),
        help="Inflow minus outflow. Positive means more BTC moved into exchange wallets.",
    )

    v1, v2, v3 = st.columns(3)
    v1.metric("Inflow value (estimated)", _usd(latest.get("inflow_usd_est")))
    v2.metric("Outflow value (estimated)", _usd(latest.get("outflow_usd_est")))
    v3.metric("Netflow value (estimated)", _usd(latest.get("netflow_usd_est")))
    st.caption(
        f"Latest provider interval: {latest.get('time', '—')} UTC · "
        f"Matched BTC index close: {_usd(latest.get('btc_index_close_usd'))}. "
        "USD values are BTC quantity × the matching interval's CryptoQuant all-exchange spot-index close."
    )

    table = pd.DataFrame(rows).rename(columns={
        "time": "UTC interval",
        "inflow_btc": "Inflow (BTC)",
        "inflow_usd_est": "Inflow (USD est.)",
        "outflow_btc": "Outflow (BTC)",
        "outflow_usd_est": "Outflow (USD est.)",
        "netflow_btc": "Netflow (BTC)",
        "netflow_usd_est": "Netflow (USD est.)",
        "btc_index_close_usd": "BTC index close (USD)",
    })
    columns = [
        "UTC interval", "Inflow (BTC)", "Inflow (USD est.)",
        "Outflow (BTC)", "Outflow (USD est.)", "Netflow (BTC)",
        "Netflow (USD est.)", "BTC index close (USD)",
    ]
    st.dataframe(
        table[[name for name in columns if name in table.columns]].tail(14),
        use_container_width=True,
        hide_index=True,
    )


def _render_perpetual_taker_values(data: dict[str, Any]) -> None:
    rows = data.get("taker_rows") or []
    if data.get("taker_error"):
        st.warning(str(data["taker_error"]))
    if not rows:
        if not data.get("taker_error"):
            st.info("No perpetual taker buy/sell records were returned for the selected window.")
        return

    latest = rows[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Perpetual taker BUY value", _usd(latest.get("taker_buy_usd")))
    c2.metric("Perpetual taker SELL value", _usd(latest.get("taker_sell_usd")))
    net = latest.get("net_taker_usd")
    c3.metric(
        "Net taker value",
        _usd(net),
        delta="BUY takers dominant" if net is not None and net > 0 else ("SELL takers dominant" if net is not None and net < 0 else None),
    )
    c4.metric("BUY share", _pct(latest.get("buy_ratio_pct")))

    st.caption(
        f"Latest provider interval: {latest.get('time', '—')} UTC. "
        "CryptoQuant defines these as executed perpetual-swap taker trades and normalizes volumes to USD. "
        "They are not on-chain deposits/withdrawals and do not include spot-market trades."
    )
    table = pd.DataFrame(rows).rename(columns={
        "time": "UTC interval",
        "taker_buy_usd": "Taker BUY (USD)",
        "taker_sell_usd": "Taker SELL (USD)",
        "net_taker_usd": "Net taker (USD)",
        "buy_ratio_pct": "BUY share (%)",
        "sell_ratio_pct": "SELL share (%)",
        "buy_sell_ratio": "BUY / SELL ratio",
    })
    columns = [
        "UTC interval", "Taker BUY (USD)", "Taker SELL (USD)",
        "Net taker (USD)", "BUY share (%)", "SELL share (%)",
        "BUY / SELL ratio",
    ]
    st.dataframe(
        table[[name for name in columns if name in table.columns]].tail(14),
        use_container_width=True,
        hide_index=True,
    )


def render_exchange_flow_panel() -> None:
    with st.expander("Exchange wallet flows & perpetual BUY/SELL values (CryptoQuant)", expanded=False):
        st.caption(
            "Separate on-chain transfer flows from executed perpetual taker trades. "
            "This panel is read-only and is not connected to the frozen Delta signal evaluator."
        )
        api_key = _cryptoquant_api_key()
        if not api_key:
            st.info(
                "CryptoQuant data is not configured. Add CRYPTOQUANT_API_KEY to the local environment "
                "or Streamlit secrets to enable this panel. Do not commit the key to GitHub."
            )
            st.caption(
                "CryptoQuant on-chain inflow/outflow requires Professional or higher. "
                "Professional provides daily on-chain resolution; hourly on-chain data requires Premium."
            )
            return

        c1, c2, c3, c4 = st.columns(4)
        flow_label = c1.selectbox(
            "Wallet-flow group",
            list(FLOW_EXCHANGES),
            index=0,
            key="cq_wallet_flow_group",
        )
        taker_label = c2.selectbox(
            "Perpetual taker exchange",
            list(TAKER_EXCHANGES),
            index=0,
            key="cq_perp_taker_exchange",
        )
        flow_resolution = c3.selectbox(
            "Wallet-flow resolution",
            ["Daily", "Hourly (Premium)"],
            index=0,
            key="cq_wallet_flow_resolution",
        )
        taker_resolution = c4.selectbox(
            "Taker-value resolution",
            ["Daily", "Hourly", "Minute"],
            index=0,
            key="cq_taker_resolution",
        )
        flow_window = "hour" if flow_resolution.startswith("Hourly") else "day"
        taker_window = {"Daily": "day", "Hourly": "hour", "Minute": "min"}[taker_resolution]

        try:
            data = _cached_cryptoquant_data(
                api_key,
                FLOW_EXCHANGES[flow_label],
                TAKER_EXCHANGES[taker_label],
                flow_window,
                taker_window,
            )
        except CryptoQuantAPIError as exc:
            st.error(str(exc))
            return
        except Exception as exc:
            st.error(f"Exchange-flow panel failed safely ({type(exc).__name__}).")
            return

        st.markdown("**On-chain exchange wallet transfers**")
        _render_wallet_flows(data)
        st.markdown("**Perpetual executed taker buy/sell values**")
        _render_perpetual_taker_values(data)
        st.caption(
            "Interpretation: exchange inflow is potential sell-side inventory pressure, not proof of a sale. "
            "Exchange outflow may indicate withdrawal or custody movement, not proof of a buy. "
            "Actual BUY/SELL execution values are shown separately in the perpetual taker section."
        )
