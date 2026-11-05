"""Read-only daily volume and open-interest summaries for Delta India."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _epoch_seconds(value: Any) -> int | None:
    number = _number(value)
    if number is None or number <= 0:
        return None
    if number > 10_000_000_000:
        number /= 1_000_000
    return int(number)


def parse_ticker_payload(payload: Any, symbol: str = "BTCUSD") -> dict[str, Any]:
    """Normalize the public REST ticker response without inventing missing fields."""
    empty = {
        "symbol": symbol,
        "volume_contracts": None,
        "turnover_usd": None,
        "open_interest_contracts": None,
        "open_interest_value_usd": None,
        "timestamp": None,
    }
    if not isinstance(payload, Mapping):
        return empty
    result = payload.get("result")
    row: Any = result
    if isinstance(result, Sequence) and not isinstance(result, (str, bytes, bytearray)):
        row = next(
            (item for item in result if isinstance(item, Mapping) and item.get("symbol") == symbol),
            None,
        )
    if not isinstance(row, Mapping) or row.get("symbol", symbol) != symbol:
        return empty

    volume = _number(row.get("volume"))
    turnover_usd = _number(row.get("turnover_usd"))
    if turnover_usd is None and str(row.get("turnover_symbol", "")).upper() == "USD":
        turnover_usd = _number(row.get("turnover"))
    oi = _number(row.get("oi"))
    oi_value_usd = _number(row.get("oi_value_usd"))
    timestamp = _epoch_seconds(row.get("timestamp"))

    return {
        "symbol": symbol,
        "volume_contracts": volume if volume is not None and volume >= 0 else None,
        "turnover_usd": turnover_usd if turnover_usd is not None and turnover_usd >= 0 else None,
        "open_interest_contracts": oi if oi is not None and oi >= 0 else None,
        "open_interest_value_usd": (
            oi_value_usd if oi_value_usd is not None and oi_value_usd >= 0 else None
        ),
        "timestamp": timestamp,
    }


def parse_daily_volume_candles(payload: Any) -> list[dict[str, int | float]]:
    """Normalize BTCUSD 1d candles; volume follows the exchange candle window."""
    if not isinstance(payload, Mapping):
        return []
    result = payload.get("result")
    if not isinstance(result, Sequence) or isinstance(result, (str, bytes, bytearray)):
        return []
    rows: dict[int, float] = {}
    for row in result:
        if not isinstance(row, Mapping):
            continue
        timestamp = _epoch_seconds(row.get("time"))
        volume = _number(row.get("volume"))
        if timestamp is None or volume is None or volume < 0:
            continue
        rows[timestamp] = volume
    return [
        {"time": timestamp, "volume": volume}
        for timestamp, volume in sorted(rows.items())
    ]


def summarize_aggressor_flow(
    flow_minutes: Mapping[Any, Any] | None,
    *,
    now_ts: float,
    feed_started_at: float | None,
    contract_value_btc: float = 0.001,
    window_seconds: int = 86_400,
) -> dict[str, Any]:
    """Aggregate captured trade aggressors over the rolling window.

    Inputs are minute aggregates from the live public trade feed. They are not
    exchange-backfilled; capture_hours reports process age, not guaranteed
    uninterrupted feed coverage.
    """
    contract_btc = _number(contract_value_btc)
    now = _number(now_ts)
    if contract_btc is None or contract_btc <= 0:
        raise ValueError("contract_value_btc must be finite and positive")
    if now is None or now <= 0:
        raise ValueError("now_ts must be finite and positive")
    if not isinstance(window_seconds, int) or window_seconds <= 0:
        raise ValueError("window_seconds must be a positive integer")

    totals = {
        "buy_count": 0,
        "sell_count": 0,
        "buy_contracts": 0.0,
        "sell_contracts": 0.0,
        "buy_usd": 0.0,
        "sell_usd": 0.0,
        "minutes_with_trades": 0,
    }
    cutoff = now - window_seconds
    first_bucket: int | None = None
    last_bucket: int | None = None

    for raw_minute, raw_flow in (flow_minutes or {}).items():
        minute = _epoch_seconds(raw_minute)
        if minute is None or minute + 60 <= cutoff or minute > now or not isinstance(raw_flow, Mapping):
            continue
        first_bucket = minute if first_bucket is None else min(first_bucket, minute)
        last_bucket = minute if last_bucket is None else max(last_bucket, minute)
        minute_had_trade = False
        for side in ("buy", "sell"):
            count = _number(raw_flow.get(f"{side}_count")) or 0.0
            contracts = _number(raw_flow.get(f"{side}_contracts")) or 0.0
            usd = _number(raw_flow.get(f"{side}_usd")) or 0.0
            if count > 0 or contracts > 0:
                minute_had_trade = True
            totals[f"{side}_count"] += max(0, int(count))
            totals[f"{side}_contracts"] += max(0.0, contracts)
            totals[f"{side}_usd"] += max(0.0, usd)
        if minute_had_trade:
            totals["minutes_with_trades"] += 1

    buy_contracts = totals["buy_contracts"]
    sell_contracts = totals["sell_contracts"]
    total_contracts = buy_contracts + sell_contracts
    buy_share = (buy_contracts / total_contracts * 100.0) if total_contracts > 0 else 0.0
    sell_share = (sell_contracts / total_contracts * 100.0) if total_contracts > 0 else 0.0
    started = _number(feed_started_at)
    capture_hours = (
        min(window_seconds / 3600.0, max(0.0, now - started) / 3600.0)
        if started is not None and started > 0 else None
    )

    return {
        **totals,
        "buy_btc": buy_contracts * contract_btc,
        "sell_btc": sell_contracts * contract_btc,
        "total_contracts": total_contracts,
        "total_btc": total_contracts * contract_btc,
        "total_usd": totals["buy_usd"] + totals["sell_usd"],
        "buy_share_pct": buy_share,
        "sell_share_pct": sell_share,
        "capture_hours": capture_hours,
        "first_bucket": first_bucket,
        "last_bucket": last_bucket,
        "window_seconds": window_seconds,
    }


def summarize_open_interest_24h(
    history_rows: Sequence[Mapping[str, Any]] | None,
    current_oi_contracts: Any,
    *,
    now_ts: float,
    window_seconds: int = 86_400,
    max_baseline_age_seconds: int = 7_200,
) -> dict[str, Any]:
    """Compare live OI with the latest hourly OI close at/before the 24h cutoff."""
    now = _number(now_ts)
    if now is None or now <= 0:
        raise ValueError("now_ts must be finite and positive")

    normalized: list[tuple[int, float]] = []
    for row in history_rows or ():
        if not isinstance(row, Mapping):
            continue
        timestamp = _epoch_seconds(row.get("time"))
        close = _number(row.get("close"))
        if timestamp is not None and close is not None and close >= 0:
            normalized.append((timestamp, close))
    normalized.sort(key=lambda item: item[0])

    current = _number(current_oi_contracts)
    if current is not None and current < 0:
        current = None
    latest_ts = normalized[-1][0] if normalized else None
    if current is None and normalized and now - normalized[-1][0] <= 7_200:
        current = normalized[-1][1]

    cutoff = int(now - window_seconds)
    baselines = [item for item in normalized if item[0] <= cutoff]
    baseline_ts: int | None = None
    baseline_oi: float | None = None
    if baselines:
        baseline_ts, baseline_oi = baselines[-1]
        if cutoff - baseline_ts > max_baseline_age_seconds:
            baseline_ts = None
            baseline_oi = None

    delta = (current - baseline_oi) if current is not None and baseline_oi is not None else None
    pct = (
        delta / baseline_oi * 100.0
        if delta is not None and baseline_oi is not None and baseline_oi > 0 else None
    )
    return {
        "current_contracts": current,
        "baseline_contracts": baseline_oi,
        "change_contracts": delta,
        "change_pct": pct,
        "baseline_timestamp": baseline_ts,
        "latest_history_timestamp": latest_ts,
        "history": [
            {"time": timestamp, "open_interest_contracts": close}
            for timestamp, close in normalized
            if timestamp >= now - window_seconds - 3_600
        ],
    }
