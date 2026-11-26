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


def parse_ticker_payload(
    payload: Any,
    symbol: str = "BTCUSD",
    *,
    contract_value_btc: float = 0.001,
) -> dict[str, Any]:
    """Normalize ticker fields and independently derive BTCUSD OI USD notional.

    Delta documents OI as the outstanding contract count. For BTCUSD,
    contract_value_btc is the BTC amount per contract, so the defensible USD
    notional is contracts × BTC per contract × mark price.

    The exchange-reported oi_value_usd is retained for diagnostics only.
    It is not used as the displayed value because it can disagree with the
    product contract multiplier by orders of magnitude.
    """
    contract_btc = _number(contract_value_btc)
    if contract_btc is None or contract_btc <= 0:
        raise ValueError("contract_value_btc must be finite and positive")

    empty = {
        "symbol": symbol,
        "volume": None,
        "turnover_usd": None,
        "open_interest_contracts": None,
        "open_interest_value_usd": None,
        "reported_open_interest_value_usd": None,
        "open_interest_mark_price": None,
        "open_interest_value_usd_discrepancy_pct": None,
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
    oi_contracts = _number(row.get("oi_contracts"))
    reported_oi_value_usd = _number(row.get("oi_value_usd"))
    mark_price = _number(row.get("mark_price"))
    if mark_price is None or mark_price <= 0:
        mark_price = None
    timestamp = _epoch_seconds(row.get("timestamp"))

    # REST oi field is reported in thousands of contracts; oi_contracts is
    # the raw contract count and matches the WebSocket oi_contracts field.
    # Use oi_contracts as authoritative; fall back to oi x 1000 only when
    # oi_contracts is absent but oi is present.
    valid_oi_contracts = (
        oi_contracts
        if oi_contracts is not None and oi_contracts >= 0
        else (oi * 1000.0 if oi is not None and oi >= 0 else None)
    )
    valid_reported_oi_value = (
        reported_oi_value_usd
        if reported_oi_value_usd is not None and reported_oi_value_usd >= 0
        else None
    )
    calculated_oi_value_usd = (
        valid_oi_contracts * contract_btc * mark_price
        if valid_oi_contracts is not None and mark_price is not None
        else None
    )
    discrepancy_pct = (
        (valid_reported_oi_value / calculated_oi_value_usd - 1.0) * 100.0
        if (
            valid_reported_oi_value is not None
            and calculated_oi_value_usd is not None
            and calculated_oi_value_usd > 0
        )
        else None
    )

    return {
        "symbol": symbol,
        "volume": volume if volume is not None and volume >= 0 else None,
        "turnover_usd": turnover_usd if turnover_usd is not None and turnover_usd >= 0 else None,
        "open_interest_contracts": valid_oi_contracts,
        "oi_field_contracts": oi if oi is not None and oi >= 0 else None,
        "oi_contracts_field": oi_contracts if oi_contracts is not None and oi_contracts >= 0 else None,
        "open_interest_value_usd": calculated_oi_value_usd,
        "reported_open_interest_value_usd": valid_reported_oi_value,
        "open_interest_mark_price": mark_price,
        "open_interest_value_usd_discrepancy_pct": discrepancy_pct,
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




def validate_and_normalize_open_interest_history(
    history_rows: Sequence[Mapping[str, Any]] | None,
    current_oi_contracts: Any,
    *,
    rest_oi: Any,
    rest_oi_contracts: Any,
    websocket_oi_contracts: Any,
    now_ts: float,
    max_history_age_seconds: int = 7_200,
    max_rest_field_diff_pct: float = 1.0,
    max_rest_ws_diff_pct: float = 2.0,
    max_history_alignment_diff_pct: float = 5.0,
) -> dict[str, Any]:
    """Validate REST/WS OI cross-sources and normalize OI candle closes to contracts.

    Delta REST ``oi`` may be expressed in thousands while ``oi_contracts`` and
    the ticker WebSocket ``oi[0]`` are raw contracts. Historical ``OI:SYMBOL``
    candle documentation does not specify that unit, so this function does not
    assume a multiplier: it chooses 1 or 1000 only when the newest candle aligns
    with the independently reconciled live contract count. Otherwise it fails
    closed and returns no normalized history for directional calculations.
    """
    now = _number(now_ts)
    if now is None or now <= 0:
        raise ValueError("now_ts must be finite and positive")
    limits = (
        _number(max_history_age_seconds),
        _number(max_rest_field_diff_pct),
        _number(max_rest_ws_diff_pct),
        _number(max_history_alignment_diff_pct),
    )
    if any(value is None or value < 0 for value in limits) or limits[0] <= 0:
        raise ValueError("OI validation limits must be finite and positive (age) or non-negative (tolerances)")

    rest_oi_value = _number(rest_oi)
    rest_contract_value = _number(rest_oi_contracts)
    websocket_value = _number(websocket_oi_contracts)
    current_value = _number(current_oi_contracts)
    if rest_oi_value is not None and rest_oi_value < 0:
        rest_oi_value = None
    if rest_contract_value is not None and rest_contract_value < 0:
        rest_contract_value = None
    if websocket_value is not None and websocket_value < 0:
        websocket_value = None
    if current_value is not None and current_value < 0:
        current_value = None

    def _diff_pct(left: float | None, right: float | None) -> float | None:
        if left is None or right is None or max(abs(left), abs(right)) <= 0:
            return None
        return abs(left - right) / max(abs(left), abs(right)) * 100.0

    rest_pair_diff_pct = None
    rest_ws_diff_pct = None
    rest_count = rest_contract_value
    source_errors: list[str] = []
    if rest_oi_value is not None and rest_contract_value is not None:
        rest_pair_diff_pct = _diff_pct(rest_oi_value * 1000.0, rest_contract_value)
        if rest_pair_diff_pct is None or rest_pair_diff_pct > float(max_rest_field_diff_pct):
            source_errors.append("REST oi × 1000 does not reconcile with REST oi_contracts")
    if rest_count is None and rest_oi_value is not None:
        rest_count = rest_oi_value * 1000.0
    if rest_count is None:
        source_errors.append("REST OI contract count is unavailable")
    if websocket_value is None:
        source_errors.append("WebSocket oi[0] contract count is unavailable or stale")
    elif rest_count is not None:
        rest_ws_diff_pct = _diff_pct(rest_count, websocket_value)
        if rest_ws_diff_pct is None or rest_ws_diff_pct > float(max_rest_ws_diff_pct):
            source_errors.append("REST OI contract count does not reconcile with WebSocket oi[0]")
    if current_value is not None and rest_count is not None:
        current_rest_diff = _diff_pct(current_value, rest_count)
        if current_rest_diff is None or current_rest_diff > float(max_rest_ws_diff_pct):
            source_errors.append("selected current OI does not reconcile with REST OI contracts")
    if current_value is None:
        current_value = rest_count
    if current_value is None or current_value <= 0:
        source_errors.append("positive current OI contract count is unavailable")

    normalized_rows: list[dict[str, Any]] = []
    clean_rows: list[tuple[int, float]] = []
    for row in history_rows or ():
        if not isinstance(row, Mapping):
            continue
        timestamp = _epoch_seconds(row.get("time"))
        close = _number(row.get("close"))
        if timestamp is not None and close is not None and close > 0:
            clean_rows.append((timestamp, close))
    clean_rows.sort(key=lambda item: item[0])
    latest_timestamp = clean_rows[-1][0] if clean_rows else None
    latest_raw_close = clean_rows[-1][1] if clean_rows else None
    latest_age = now - latest_timestamp if latest_timestamp is not None else None
    history_errors: list[str] = []
    scale_factor = None
    alignment_diff_pct = None
    if not clean_rows:
        history_errors.append("OI historical candles are unavailable")
    elif latest_age is None or latest_age < -60 or latest_age > float(max_history_age_seconds):
        history_errors.append("latest OI historical candle is stale or has a future timestamp")
    elif current_value is not None and current_value > 0 and latest_raw_close is not None:
        candidates = []
        for factor in (1.0, 1000.0):
            diff = _diff_pct(latest_raw_close * factor, current_value)
            if diff is not None:
                candidates.append((diff, factor))
        candidates.sort(key=lambda item: item[0])
        if not candidates or candidates[0][0] > float(max_history_alignment_diff_pct):
            history_errors.append("latest OI candle does not align with reconciled live contract count at ×1 or ×1000")
        else:
            alignment_diff_pct, scale_factor = candidates[0]

    source_valid = not source_errors
    history_valid = not history_errors and scale_factor is not None
    valid = source_valid and history_valid
    if valid:
        normalized_rows = [
            {"time": timestamp, "close": close * scale_factor, "raw_close": close}
            for timestamp, close in clean_rows
        ]
        reason = "REST oi/oi_contracts, WebSocket oi[0], and latest OI candle reconcile"
    else:
        reason = "; ".join(source_errors + history_errors) or "OI scale validation failed"

    return {
        "valid": valid,
        "source_values_valid": source_valid,
        "history_scale_valid": history_valid,
        "reason": reason,
        "history_scale_factor": scale_factor if valid else None,
        "normalized_history": normalized_rows if valid else [],
        "raw_latest_close": latest_raw_close,
        "normalized_latest_close": latest_raw_close * scale_factor if valid and latest_raw_close is not None else None,
        "latest_timestamp": latest_timestamp,
        "latest_age_seconds": latest_age,
        "history_alignment_diff_pct": alignment_diff_pct,
        "rest_field_diff_pct": rest_pair_diff_pct,
        "rest_ws_diff_pct": rest_ws_diff_pct,
        "current_oi_contracts": current_value,
        "rest_oi_raw": rest_oi_value,
        "rest_oi_contracts_raw": rest_contract_value,
        "websocket_oi_contracts": websocket_value,
    }


def summarize_oi_funding_bias(
    price_change_pct: Any,
    oi_change_pct: Any,
    funding_rate: Any,
    funding_interval_seconds: Any = None,
    *,
    neutral_threshold_pct: float = 0.05,
) -> dict[str, Any]:
    """Describe 24h price/OI context and funding payer side without making a signal.

    funding_rate is the raw fractional rate from Delta's funding WebSocket;
    the returned percentage is rate × 100 for display. Positive funding means
    longs pay shorts; negative funding means shorts pay longs.

    Price/OI interpretations are heuristic context only. Aggregate OI cannot
    identify which side opened or closed, and funding is a carry transfer, not
    a directional entry signal.
    """
    price_pct = _number(price_change_pct)
    oi_pct = _number(oi_change_pct)
    funding = _number(funding_rate)
    interval = _number(funding_interval_seconds)
    threshold = _number(neutral_threshold_pct)
    if threshold is None or threshold < 0:
        raise ValueError("neutral_threshold_pct must be finite and non-negative")

    if price_pct is None or oi_pct is None:
        context_bias = "UNAVAILABLE — PRICE/OI DATA MISSING"
        price_oi_regime = "WAIT — insufficient price or OI history"
        context_explanation = "A directional context cannot be calculated without both 24h price change and 24h aggregate OI change."
    elif abs(price_pct) <= threshold or abs(oi_pct) <= threshold:
        context_bias = "MIXED / WAIT"
        price_oi_regime = f"NEUTRAL — price or OI change is inside the ±{threshold:.2f}% noise band"
        context_explanation = "Price and aggregate OI are not both expanding beyond the configured neutral band."
    elif price_pct > 0 and oi_pct > 0:
        context_bias = "BUY BIAS CANDIDATE"
        price_oi_regime = "PRICE UP + OI UP — participation expanding with the advance"
        context_explanation = "A conventional bullish price/OI combination; it does not prove that new long positions dominate."
    elif price_pct < 0 and oi_pct > 0:
        context_bias = "SELL BIAS CANDIDATE"
        price_oi_regime = "PRICE DOWN + OI UP — participation expanding with the decline"
        context_explanation = "A conventional bearish price/OI combination; it does not prove that new short positions dominate."
    elif price_pct > 0 and oi_pct < 0:
        context_bias = "MIXED / WAIT"
        price_oi_regime = "PRICE UP + OI DOWN — possible short covering / deleveraging"
        context_explanation = "The advance occurs while aggregate positions contract; treat it as possible short covering, not confirmed fresh-long buildup."
    else:
        context_bias = "MIXED / WAIT"
        price_oi_regime = "PRICE DOWN + OI DOWN — possible long deleveraging"
        context_explanation = "The decline occurs while aggregate positions contract; treat it as possible long deleveraging, not confirmed fresh-short buildup."

    if funding is None:
        funding_payer = "UNAVAILABLE"
        funding_context = "Funding data is unavailable; no funding-side interpretation is made."
    elif funding > 0:
        funding_payer = "LONGS PAY SHORTS"
        funding_context = "Positive funding means long positions pay shorts; this is a long carry cost, not an automatic SELL signal."
    elif funding < 0:
        funding_payer = "SHORTS PAY LONGS"
        funding_context = "Negative funding means short positions pay longs; this is a short carry cost, not an automatic BUY signal."
    else:
        funding_payer = "NEAR ZERO"
        funding_context = "Funding is zero; there is no payer-side tilt from the sign."

    interval_hours = interval / 3600.0 if interval is not None and interval > 0 else None
    return {
        "context_bias": context_bias,
        "price_oi_regime": price_oi_regime,
        "context_explanation": context_explanation,
        "price_change_pct": price_pct,
        "oi_change_pct": oi_pct,
        "funding_rate_pct": funding * 100.0 if funding is not None else None,
        "funding_payer": funding_payer,
        "funding_context": funding_context,
        "funding_interval_hours": interval_hours,
        "is_directional_candidate": context_bias in {"BUY BIAS CANDIDATE", "SELL BIAS CANDIDATE"},
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
