"""CryptoQuant BTC exchange wallet-flow and perpetual taker-flow data.

Wallet transfers and executed taker trades are deliberately separate datasets:
an exchange deposit is not a confirmed sale, and a withdrawal is not a confirmed buy.
CryptoQuant taker volumes are USD-normalized. BTC quantities below are explicitly
estimates derived from the matched all-exchange perpetual BTC index close.
"""
from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any

import requests

CRYPTOQUANT_BASE_URL = "https://api.cryptoquant.com/v1"
REQUEST_TIMEOUT_SECONDS = 12
MAX_LIMIT = 1000


class CryptoQuantAPIError(RuntimeError):
    """Sanitized provider error; never includes request headers or API credentials."""


def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _time_key(row: dict[str, Any]) -> str | None:
    for field in ("datetime", "date", "time", "t"):
        value = row.get(field)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            try:
                stamp = float(value)
                if not math.isfinite(stamp) or stamp <= 0:
                    continue
                if stamp > 10_000_000_000:
                    stamp /= 1000.0
                return datetime.fromtimestamp(stamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            except (OverflowError, OSError, ValueError):
                continue
        text = str(value).strip()
        if text:
            return text.replace("T", " ").replace("Z", "")
    return None


def _indexed_values(rows: list[dict[str, Any]], field: str) -> dict[str, float]:
    indexed: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = _time_key(row)
        value = _finite_number(row.get(field))
        if key is not None and value is not None and value >= 0:
            indexed[key] = value
    return indexed


def parse_exchange_flow_series(
    inflow_rows: list[dict[str, Any]],
    outflow_rows: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Join BTC inflow/outflow to same-window BTC spot-index closes.

    USD values are estimates: BTC transfer quantity multiplied by the
    CryptoQuant all-exchange spot-index close for the matching UTC interval.
    Missing price matches remain None; current price is never substituted.
    """
    inflows = _indexed_values(inflow_rows, "inflow_total")
    outflows = _indexed_values(outflow_rows, "outflow_total")
    prices = _indexed_values(price_rows, "close")
    keys = sorted(set(inflows) | set(outflows))

    result: list[dict[str, Any]] = []
    for key in keys:
        inflow = inflows.get(key)
        outflow = outflows.get(key)
        price = prices.get(key)
        netflow = inflow - outflow if inflow is not None and outflow is not None else None
        result.append({
            "time": key,
            "inflow_btc": inflow,
            "outflow_btc": outflow,
            "netflow_btc": netflow,
            "btc_index_close_usd": price,
            "inflow_usd_est": inflow * price if inflow is not None and price is not None else None,
            "outflow_usd_est": outflow * price if outflow is not None and price is not None else None,
            "netflow_usd_est": netflow * price if netflow is not None and price is not None else None,
        })
    return result


def parse_taker_buy_sell_series(
    rows: list[dict[str, Any]],
    perpetual_price_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Normalize daily/hourly perpetual taker values and estimate BTC equivalents.

    CryptoQuant returns taker BUY/SELL volume in USD across heterogeneous
    perpetual contracts. We divide each USD value by the matching UTC interval's
    all-exchange perpetual BTC index close. These are BTC-equivalent estimates,
    not contract counts or exact BTC settlement quantities.
    """
    prices = _indexed_values(perpetual_price_rows or [], "close")
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = _time_key(row)
        buy = _finite_number(row.get("taker_buy_volume"))
        sell = _finite_number(row.get("taker_sell_volume"))
        if key is None or buy is None or sell is None or buy < 0 or sell < 0:
            continue
        total = buy + sell
        price = prices.get(key)
        buy_btc = buy / price if price is not None and price > 0 else None
        sell_btc = sell / price if price is not None and price > 0 else None
        total_btc = buy_btc + sell_btc if buy_btc is not None and sell_btc is not None else None
        result.append({
            "time": key,
            "taker_buy_usd": buy,
            "taker_sell_usd": sell,
            "taker_total_usd": total,
            "net_taker_usd": buy - sell,
            "buy_ratio_pct": buy / total * 100.0 if total > 0 else None,
            "sell_ratio_pct": sell / total * 100.0 if total > 0 else None,
            "buy_sell_ratio": buy / sell if sell > 0 else None,
            "perpetual_index_close_usd": price,
            "taker_buy_btc_est": buy_btc,
            "taker_sell_btc_est": sell_btc,
            "taker_total_btc_est": total_btc,
            "average_trade_size": None,
            "trade_count": None,
        })
    return sorted(result, key=lambda item: item["time"])


def _get_rows(api_key: str, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(api_key, str) or not api_key.strip():
        raise CryptoQuantAPIError("CryptoQuant API key is not configured.")
    try:
        response = requests.get(
            f"{CRYPTOQUANT_BASE_URL}/{path.lstrip('/')}",
            params=params,
            headers={"Authorization": f"Bearer {api_key.strip()}", "Accept": "application/json"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        raise CryptoQuantAPIError(f"CryptoQuant network request failed ({type(exc).__name__}).") from None

    if response.status_code in (401, 403, 402, 429):
        messages = {
            401: "CryptoQuant rejected the API key (HTTP 401). Check the local key configuration.",
            402: "CryptoQuant API credits are exhausted (HTTP 402). Check the account credit balance.",
            403: "CryptoQuant denied this metric (HTTP 403). On-chain exchange flows require Professional or higher; hourly/per-block on-chain resolution requires Premium.",
            429: "CryptoQuant rate limit reached (HTTP 429). The dashboard will retry after its cache expires.",
        }
        raise CryptoQuantAPIError(messages[response.status_code])
    try:
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        raise CryptoQuantAPIError(f"CryptoQuant returned an invalid response (HTTP {response.status_code}).") from None

    if not isinstance(payload, dict):
        raise CryptoQuantAPIError("CryptoQuant response was not a JSON object.")
    status = payload.get("status") or {}
    provider_code = status.get("code") if isinstance(status, dict) else None
    if provider_code not in (None, 200, "200"):
        code = _finite_number(provider_code)
        if code == 401:
            raise CryptoQuantAPIError("CryptoQuant rejected the API key (provider status 401).")
        if code == 402:
            raise CryptoQuantAPIError("CryptoQuant API credits are exhausted (provider status 402).")
        if code == 403:
            raise CryptoQuantAPIError("CryptoQuant denied this metric (provider status 403). On-chain exchange flows require Professional or higher.")
        raise CryptoQuantAPIError(f"CryptoQuant reported provider status {provider_code}.")

    result = payload.get("result") or {}
    rows = result.get("data") if isinstance(result, dict) else None
    if not isinstance(rows, list):
        raise CryptoQuantAPIError("CryptoQuant response did not contain a data series.")
    return [row for row in rows if isinstance(row, dict)]


def fetch_exchange_flow_data(
    api_key: str,
    *,
    flow_exchange: str = "all_exchange",
    taker_exchange: str = "all_exchange",
    flow_window: str = "day",
    taker_window: str = "day",
    limit: int = 30,
) -> dict[str, Any]:
    """Fetch independent on-chain transfer and perpetual taker-trade series."""
    if flow_exchange not in {"all_exchange", "spot_exchange", "derivative_exchange"}:
        raise ValueError("Unsupported exchange-flow group.")
    if taker_exchange not in {"all_exchange", "binance", "bybit", "bitmex", "deribit", "htx_global", "okx"}:
        raise ValueError("Unsupported taker-flow exchange.")
    if flow_window not in {"day", "hour"}:
        raise ValueError("Unsupported exchange-flow resolution.")
    if taker_window not in {"day", "hour", "min"}:
        raise ValueError("Unsupported taker-flow resolution.")
    if not isinstance(limit, int) or not 1 <= limit <= MAX_LIMIT:
        raise ValueError("Limit must be an integer between 1 and 1000.")

    output: dict[str, Any] = {
        "flow_rows": [],
        "taker_rows": [],
        "flow_error": None,
        "price_error": None,
        "taker_error": None,
        "taker_price_error": None,
    }

    try:
        common = {"exchange": flow_exchange, "window": flow_window, "limit": limit}
        inflow = _get_rows(api_key, "btc/exchange-flows/inflow", common)
        outflow = _get_rows(api_key, "btc/exchange-flows/outflow", common)
        try:
            prices = _get_rows(
                api_key,
                "btc/market-data/price-ohlcv",
                {
                    "market": "spot",
                    "exchange": "all_exchange",
                    "symbol": "btc_usd",
                    "window": flow_window,
                    "limit": limit,
                },
            )
        except CryptoQuantAPIError as exc:
            prices = []
            output["price_error"] = str(exc)
        output["flow_rows"] = parse_exchange_flow_series(inflow, outflow, prices)
    except CryptoQuantAPIError as exc:
        output["flow_error"] = str(exc)

    try:
        taker = _get_rows(
            api_key,
            "btc/market-data/taker-buy-sell-stats",
            {"exchange": taker_exchange, "window": taker_window, "limit": limit},
        )
        try:
            perpetual_prices = _get_rows(
                api_key,
                "btc/market-data/price-ohlcv",
                {
                    "market": "perpetual",
                    "exchange": "all_exchange",
                    "symbol": "btc_usd",
                    "window": taker_window,
                    "limit": limit,
                },
            )
        except CryptoQuantAPIError as exc:
            perpetual_prices = []
            output["taker_price_error"] = str(exc)
        output["taker_rows"] = parse_taker_buy_sell_series(taker, perpetual_prices)
    except CryptoQuantAPIError as exc:
        output["taker_error"] = str(exc)

    return output
