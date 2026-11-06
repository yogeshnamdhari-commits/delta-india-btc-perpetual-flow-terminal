"""Multi-factor, read-only exhaustion and liquidity-sweep analysis.

Combines Delta-native completed 1m price/volume candles, captured public taker
flow, and Delta OI:BTCUSD 1m candles. This is monitoring evidence, not a
prediction, proof of stop executions, or a strategy entry signal.
"""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

BAR_SECONDS = 60
DEFAULT_LOOKBACK = 10
DEFAULT_VOLUME_LOOKBACK = 20
DEFAULT_VOLUME_MULTIPLE = 1.5
DEFAULT_MIN_PRINTS = 5
DEFAULT_AGGRESSOR_RATIO = 1.25
DEFAULT_MIN_WICK_RATIO = 0.35
DEFAULT_CLOSE_POSITION_LIMIT = 0.65
DEFAULT_MIN_ACTIVE_COVERAGE_PCT = 95.0
DEFAULT_MIN_OI_CHANGE_PCT = 0.01
DEFAULT_OI_BASELINE_LOOKBACK = 20
DEFAULT_MAX_TRADE_AGE_SECONDS = 20.0
DEFAULT_MAX_OI_AGE_SECONDS = 180.0


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _epoch(value: Any) -> int | None:
    number = _number(value)
    if number is None or number <= 0:
        return None
    if number > 1e17:
        number /= 1e9
    elif number > 1e14:
        number /= 1e6
    elif number > 1e11:
        number /= 1e3
    return int(number)


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    clean = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    rank = (len(clean) - 1) * percentile / 100.0
    lower = int(rank)
    upper = min(lower + 1, len(clean) - 1)
    fraction = rank - lower
    return clean[lower] + (clean[upper] - clean[lower]) * fraction


def _normalize_price_candles(
    rows: Sequence[Mapping[str, Any]] | None, now_ts: float
) -> list[dict[str, float | int]]:
    normalized: dict[int, dict[str, float | int]] = {}
    for row in rows or ():
        if not isinstance(row, Mapping):
            continue
        timestamp = _epoch(row.get("time"))
        values = {key: _number(row.get(key)) for key in ("open", "high", "low", "close", "volume")}
        if timestamp is None or timestamp + BAR_SECONDS > now_ts:
            continue
        if any(value is None for value in values.values()):
            continue
        open_price, high, low, close, volume = (float(values[key]) for key in ("open", "high", "low", "close", "volume"))
        if (
            min(open_price, high, low, close) <= 0
            or volume < 0
            or high < max(open_price, close, low)
            or low > min(open_price, close, high)
            or high <= low
        ):
            continue
        normalized[timestamp] = {
            "time": timestamp, "open": open_price, "high": high,
            "low": low, "close": close, "volume": volume,
        }
    return [normalized[key] for key in sorted(normalized)]


def _normalize_oi_candles(
    rows: Sequence[Mapping[str, Any]] | None, now_ts: float
) -> dict[int, float]:
    """OI candles use the close value as outstanding contracts at that bar close."""
    normalized: dict[int, float] = {}
    for row in rows or ():
        if not isinstance(row, Mapping):
            continue
        timestamp = _epoch(row.get("time"))
        close = _number(row.get("close"))
        if timestamp is None or timestamp + BAR_SECONDS > now_ts:
            continue
        if close is None or close <= 0:
            continue
        normalized[timestamp] = close
    return normalized


def _flow_metrics(
    flow_minutes: Mapping[Any, Any] | None,
    *,
    start_ts: int,
    end_ts: int,
    expected_side: str,
    now_ts: float,
    feed_started_at: float | None,
    connection_status: str,
    trade_channel_last_seen: float | None,
    min_prints: int,
    min_aggressor_ratio: float,
    min_active_coverage_pct: float,
    max_trade_age_seconds: float,
) -> dict[str, Any]:
    totals = {
        "buy_prints": 0, "sell_prints": 0,
        "buy_contracts": 0.0, "sell_contracts": 0.0,
    }
    active_minutes: set[int] = set()
    normalized: dict[int, Mapping[str, Any]] = {}
    for raw_minute, row in (flow_minutes or {}).items():
        minute = _epoch(raw_minute)
        if minute is not None and isinstance(row, Mapping):
            normalized[(minute // 60) * 60] = row

    expected_minutes = max(0, (end_ts - start_ts) // 60)
    for minute in range(start_ts, end_ts, 60):
        row = normalized.get(minute)
        if row is None:
            continue
        for side in ("buy", "sell"):
            count = _number(row.get(f"{side}_count")) or 0.0
            contracts = _number(row.get(f"{side}_contracts")) or 0.0
            totals[f"{side}_prints"] += max(0, int(count))
            totals[f"{side}_contracts"] += max(0.0, contracts)
            if count > 0 or contracts > 0:
                active_minutes.add(minute)

    buy_qty = totals["buy_contracts"]
    sell_qty = totals["sell_contracts"]
    total_prints = totals["buy_prints"] + totals["sell_prints"]
    total_qty = buy_qty + sell_qty
    opposite_side = "sell" if expected_side == "BUY" else "buy"
    expected_qty = buy_qty if expected_side == "BUY" else sell_qty
    opposite_qty = sell_qty if expected_side == "BUY" else buy_qty
    aligned_ratio = expected_qty / opposite_qty if opposite_qty > 0 else (
        math.inf if expected_qty > 0 else None
    )
    active_coverage = (
        len(active_minutes) / expected_minutes * 100.0 if expected_minutes else 0.0
    )

    age = (
        now_ts - trade_channel_last_seen
        if trade_channel_last_seen is not None else None
    )
    reasons: list[str] = []
    if feed_started_at is None or feed_started_at <= 0:
        reasons.append("APP START TIME UNKNOWN")
    elif feed_started_at > start_ts:
        reasons.append("APP STARTED AFTER EVENT")
    if str(connection_status).strip().lower() != "connected":
        reasons.append("TRADE FEED NOT CONNECTED")
    if age is None or age < 0 or age > max_trade_age_seconds:
        reasons.append("TRADE CHANNEL STALE")
    if active_coverage < min_active_coverage_pct:
        reasons.append("LOW ACTIVE-MINUTE COVERAGE")
    if total_prints < min_prints:
        reasons.append("INSUFFICIENT CLASSIFIED PRINTS")
    capture_ok = not reasons
    direction_ok = (
        capture_ok and aligned_ratio is not None
        and aligned_ratio >= min_aggressor_ratio and expected_qty > 0
    )
    if capture_ok and not direction_ok:
        reasons.append("EXPECTED-SIDE AGGRESSOR RATIO BELOW THRESHOLD")

    return {
        **totals,
        "classified_prints": total_prints,
        "total_contracts": total_qty,
        "buy_share_pct": buy_qty / total_qty * 100.0 if total_qty > 0 else None,
        "sell_share_pct": sell_qty / total_qty * 100.0 if total_qty > 0 else None,
        "expected_side": expected_side,
        "expected_side_contracts": expected_qty,
        "opposing_side_contracts": opposite_qty,
        "expected_side_aggressor_ratio": aligned_ratio,
        "active_minutes": len(active_minutes),
        "expected_minutes": expected_minutes,
        "active_minute_coverage_pct": active_coverage,
        "latest_trade_age_seconds": age,
        "capture_eligible": capture_ok,
        "direction_confirmed": direction_ok,
        "status": "VERIFIED PROCESS WINDOW · FRESH" if capture_ok else "UNVERIFIED — " + "; ".join(reasons),
        "direction_status": "ALIGNED" if direction_ok else (
            "NOT ALIGNED" if capture_ok else "UNVERIFIED"
        ),
        "limitations": (
            "Captured public trade messages do not expose a per-trade sequence number. "
            "Freshness and active-minute coverage cannot prove every trade was delivered."
        ),
    }


def _oi_metrics(
    oi_by_time: Mapping[int, float],
    *,
    start_ts: int,
    end_ts: int,
    now_ts: float,
    latest_oi_time: int | None,
    max_oi_age_seconds: float,
    baseline_lookback: int,
    min_oi_change_pct: float,
) -> dict[str, Any]:
    age = now_ts - latest_oi_time if latest_oi_time is not None else None
    if age is None or age < 0 or age > max_oi_age_seconds:
        return {
            "available": False, "status": "UNAVAILABLE — OI HISTORY STALE OR MISSING",
            "change_contracts": None, "change_pct": None,
            "threshold_pct": None, "baseline_contracts": None,
            "current_contracts": None, "latest_oi_age_seconds": age,
        }
    event_last = end_ts - BAR_SECONDS
    baseline_time = start_ts - BAR_SECONDS
    if baseline_time not in oi_by_time or event_last not in oi_by_time:
        reason = "OI CANDLE MISSING FOR EVENT OR BASELINE"
        return {
            "available": False, "status": "UNAVAILABLE — " + reason,
            "change_contracts": None, "change_pct": None,
            "threshold_pct": None, "baseline_contracts": oi_by_time.get(baseline_time),
            "current_contracts": oi_by_time.get(event_last),
            "latest_oi_age_seconds": age,
        }

    expected_oi_times = list(range(baseline_time, event_last + 1, BAR_SECONDS))
    if any(timestamp not in oi_by_time for timestamp in expected_oi_times):
        return {
            "available": False, "status": "UNAVAILABLE — OI HISTORY GAP IN EVENT WINDOW",
            "change_contracts": None, "change_pct": None,
            "threshold_pct": None, "baseline_contracts": oi_by_time[baseline_time],
            "current_contracts": oi_by_time[event_last],
            "latest_oi_age_seconds": age,
        }

    baseline_oi = oi_by_time[baseline_time]
    current_oi = oi_by_time[event_last]
    if baseline_oi <= 0:
        return {
            "available": False, "status": "UNAVAILABLE — ZERO OI BASELINE",
            "change_contracts": None, "change_pct": None,
            "threshold_pct": None, "baseline_contracts": baseline_oi,
            "current_contracts": current_oi, "latest_oi_age_seconds": age,
        }
    change = current_oi - baseline_oi
    change_pct = change / baseline_oi * 100.0

    historical_changes: list[float] = []
    for offset in range(baseline_lookback, 0, -1):
        timestamp = start_ts - offset * BAR_SECONDS
        previous_timestamp = timestamp - BAR_SECONDS
        if timestamp not in oi_by_time or previous_timestamp not in oi_by_time:
            historical_changes = []
            break
        previous_oi = oi_by_time[previous_timestamp]
        if previous_oi > 0:
            historical_changes.append(
                abs((oi_by_time[timestamp] - previous_oi) / previous_oi * 100.0)
            )
    if len(historical_changes) != baseline_lookback:
        return {
            "available": False, "status": "UNAVAILABLE — OI BASELINE WINDOW INCOMPLETE",
            "change_contracts": change, "change_pct": change_pct,
            "threshold_pct": None, "baseline_contracts": baseline_oi,
            "current_contracts": current_oi, "latest_oi_age_seconds": age,
        }

    baseline_percentile = _percentile(historical_changes, 75.0)
    threshold_pct = max(min_oi_change_pct, baseline_percentile or 0.0)
    if change_pct >= threshold_pct:
        state = "BUILDING"
        status = "OI BUILDING"
    elif change_pct <= -threshold_pct:
        state = "REDUCING"
        status = "OI REDUCING"
    else:
        state = "FLAT"
        status = "OI CHANGE BELOW ADAPTIVE THRESHOLD"
    return {
        "available": True, "status": status, "state": state,
        "change_contracts": change, "change_pct": change_pct,
        "threshold_pct": threshold_pct,
        "baseline_contracts": baseline_oi, "current_contracts": current_oi,
        "latest_oi_age_seconds": age,
        "baseline_method": (
            f"max({min_oi_change_pct:.4f}%, 75th percentile of prior "
            f"{baseline_lookback} completed 1m absolute OI changes)"
        ),
    }


def _volume_ratio(
    candles: Sequence[Mapping[str, Any]], index: int,
    lookback: int = DEFAULT_VOLUME_LOOKBACK,
) -> tuple[float | None, float | None]:
    if index < lookback:
        return None, None
    previous = candles[index - lookback:index]
    if any(
        int(candles[j]["time"]) - int(candles[j - 1]["time"]) != BAR_SECONDS
        for j in range(index - lookback + 1, index + 1)
    ):
        return None, None
    values = [float(row["volume"]) for row in previous]
    average = sum(values) / len(values) if values else None
    if average is None or average <= 0:
        return None, average
    return float(candles[index]["volume"]) / average, average


def _sweep_volume_ratio(
    candles: Sequence[Mapping[str, Any]], first_index: int, last_index: int,
    baseline_windows: int = DEFAULT_VOLUME_LOOKBACK,
) -> tuple[float | None, float | None, float | None]:
    window_bars = last_index - first_index + 1
    if window_bars < 1:
        return None, None, None
    event_volume = sum(float(candles[i]["volume"]) for i in range(first_index, last_index + 1))
    window_totals: list[float] = []
    for offset in range(1, baseline_windows + 1):
        start = first_index - offset * window_bars
        block = candles[start:start + window_bars]
        if start < 0 or len(block) != window_bars:
            return None, event_volume, None
        if any(
            int(block[i]["time"]) - int(block[i - 1]["time"]) != BAR_SECONDS
            for i in range(1, len(block))
        ):
            return None, event_volume, None
        if any(
            int(block[i]["time"]) - int(block[i - 1]["time"]) != BAR_SECONDS
            for i in range(1, len(block))
        ):
            return None, event_volume, None
        window_totals.append(sum(float(row["volume"]) for row in block))
    average = sum(window_totals) / len(window_totals)
    ratio = event_volume / average if average > 0 else None
    return ratio, event_volume, average


def analyze_exhaustion_and_sweeps(
    candles: Sequence[Mapping[str, Any]] | None,
    flow_minutes: Mapping[Any, Any] | None,
    oi_candles: Sequence[Mapping[str, Any]] | None,
    price_sweeps: Sequence[Mapping[str, Any]] | None,
    *,
    now_ts: float,
    feed_started_at: float | None,
    connection_status: str,
    trade_channel_last_seen: float | None,
    lookback: int = DEFAULT_LOOKBACK,
    volume_lookback: int = DEFAULT_VOLUME_LOOKBACK,
    min_volume_multiple: float = DEFAULT_VOLUME_MULTIPLE,
    min_prints: int = DEFAULT_MIN_PRINTS,
    min_aggressor_ratio: float = DEFAULT_AGGRESSOR_RATIO,
    min_wick_ratio: float = DEFAULT_MIN_WICK_RATIO,
    close_position_limit: float = DEFAULT_CLOSE_POSITION_LIMIT,
    min_active_coverage_pct: float = DEFAULT_MIN_ACTIVE_COVERAGE_PCT,
    min_oi_change_pct: float = DEFAULT_MIN_OI_CHANGE_PCT,
    oi_baseline_lookback: int = DEFAULT_OI_BASELINE_LOOKBACK,
    max_trade_age_seconds: float = DEFAULT_MAX_TRADE_AGE_SECONDS,
    max_oi_age_seconds: float = DEFAULT_MAX_OI_AGE_SECONDS,
) -> dict[str, Any]:
    """Return OI/flow/volume-enriched 1m exhaustion and stop-hunt-like candidates.

    A candidate is multi-factor only when price rejection, same-direction
    executed taker flow, elevated Delta candle volume, and a statistically
    elevated OI change all pass. A sweep is called stop-hunt-like only when
    price reclaims the prior liquidity level, breach-direction taker flow and
    relative volume confirm, and OI materially declines across the event.
    """
    numeric = (
        now_ts, min_volume_multiple, min_aggressor_ratio, min_wick_ratio,
        close_position_limit, min_active_coverage_pct, min_oi_change_pct,
        max_trade_age_seconds, max_oi_age_seconds,
    )
    if not all(math.isfinite(float(value)) for value in numeric):
        raise ValueError("all numeric settings must be finite")
    if now_ts <= 0 or lookback < 2 or volume_lookback < 2 or oi_baseline_lookback < 2:
        raise ValueError("time and lookback settings must be positive")
    if min_volume_multiple <= 0 or min_prints < 1 or min_aggressor_ratio < 1:
        raise ValueError("volume, print-count and aggressor-ratio thresholds are invalid")
    if not 0 < min_wick_ratio <= 1 or not 0 < close_position_limit < 1:
        raise ValueError("wick and close-position thresholds are invalid")
    if not 0 < min_active_coverage_pct <= 100 or min_oi_change_pct <= 0:
        raise ValueError("coverage and OI-change thresholds are invalid")
    if max_trade_age_seconds <= 0 or max_oi_age_seconds <= 0:
        raise ValueError("staleness limits must be positive")

    completed = _normalize_price_candles(candles, float(now_ts))
    oi_by_time = _normalize_oi_candles(oi_candles, float(now_ts))
    latest_oi_time = max(oi_by_time) if oi_by_time else None
    latest_oi_age = float(now_ts) - latest_oi_time if latest_oi_time is not None else None
    oi_fresh = (
        latest_oi_age is not None
        and 0 <= latest_oi_age <= max_oi_age_seconds
    )
    exhaustion_rows: list[dict[str, Any]] = []
    for index in range(max(lookback, volume_lookback), len(completed)):
        current = completed[index]
        previous = completed[index - lookback:index]
        if any(
            int(completed[j]["time"]) - int(completed[j - 1]["time"]) != BAR_SECONDS
            for j in range(index - lookback, index + 1)
        ):
            continue
        high = float(current["high"])
        low = float(current["low"])
        open_price = float(current["open"])
        close = float(current["close"])
        candle_range = high - low
        if candle_range <= 0:
            continue
        upper_wick = (high - max(open_price, close)) / candle_range
        lower_wick = (min(open_price, close) - low) / candle_range
        close_position = (close - low) / candle_range
        prior_high = max(float(row["high"]) for row in previous)
        prior_low = min(float(row["low"]) for row in previous)
        candidates: list[tuple[str, float]] = []
        if high > prior_high and upper_wick >= min_wick_ratio and close_position <= close_position_limit:
            candidates.append(("BUY", upper_wick))
        if low < prior_low and lower_wick >= min_wick_ratio and close_position >= 1.0 - close_position_limit:
            candidates.append(("SELL", lower_wick))
        if not candidates:
            continue

        volume_ratio, average_volume = _volume_ratio(completed, index, volume_lookback)
        for direction, wick_ratio in candidates:
            start_ts = int(current["time"])
            flow = _flow_metrics(
                flow_minutes, start_ts=start_ts, end_ts=start_ts + BAR_SECONDS,
                expected_side=direction, now_ts=float(now_ts),
                feed_started_at=feed_started_at, connection_status=connection_status,
                trade_channel_last_seen=trade_channel_last_seen,
                min_prints=min_prints, min_aggressor_ratio=min_aggressor_ratio,
                min_active_coverage_pct=min_active_coverage_pct,
                max_trade_age_seconds=max_trade_age_seconds,
            )
            oi = _oi_metrics(
                oi_by_time, start_ts=start_ts, end_ts=start_ts + BAR_SECONDS,
                now_ts=float(now_ts), latest_oi_time=latest_oi_time,
                max_oi_age_seconds=max_oi_age_seconds,
                baseline_lookback=oi_baseline_lookback,
                min_oi_change_pct=min_oi_change_pct,
            ) if oi_fresh else {
                "available": False, "status": "UNAVAILABLE — OI HISTORY STALE OR MISSING",
                "change_contracts": None, "change_pct": None,
                "threshold_pct": None, "baseline_contracts": None,
                "current_contracts": None, "latest_oi_age_seconds": latest_oi_age,
            }
            volume_ok = volume_ratio is not None and volume_ratio >= min_volume_multiple
            all_factors = (
                volume_ok and flow["direction_confirmed"] and oi.get("available")
                and oi.get("state") in ("BUILDING", "REDUCING")
            )
            side_name = "BUY" if direction == "BUY" else "SELL"
            if all_factors:
                classification = f"{side_name} EXHAUSTION — MULTI-FACTOR EVIDENCE"
            elif not flow["capture_eligible"] or not flow["direction_confirmed"]:
                classification = f"{side_name} EXHAUSTION CANDIDATE — FLOW NOT CONFIRMED"
            elif not volume_ok:
                classification = f"{side_name} EXHAUSTION CANDIDATE — VOLUME NOT CONFIRMED"
            elif not oi.get("available"):
                classification = f"{side_name} EXHAUSTION CANDIDATE — OI UNVERIFIED"
            else:
                classification = f"{side_name} EXHAUSTION CANDIDATE — OI CHANGE NOT SIGNIFICANT"

            exhaustion_rows.append({
                "direction": direction,
                "kind": "BUY EXHAUSTION" if direction == "BUY" else "SELL EXHAUSTION",
                "time": start_ts, "price": high if direction == "BUY" else low,
                "wick_ratio": wick_ratio, "close_position": close_position,
                "volume": float(current["volume"]),
                "average_prior_volume": average_volume,
                "relative_volume": volume_ratio,
                "volume_confirmed": volume_ok,
                "flow": flow, "oi": oi,
                "multi_factor_evidence": all_factors,
                "classification": classification,
                "score": sum((
                    True, volume_ok, flow["direction_confirmed"],
                    bool(oi.get("available") and oi.get("state") in ("BUILDING", "REDUCING")),
                )),
                "score_max": 4,
                "interpretation_limit": (
                    "OI is aggregate outstanding contracts, not long-side or short-side OI. "
                    "An OI increase means more open contracts; a decrease means fewer open contracts. "
                    "Neither identifies which side opened/closed or proves absorption."
                ),
            })

    candle_index = {int(row["time"]): index for index, row in enumerate(completed)}
    sweep_rows: list[dict[str, Any]] = []
    for sweep in price_sweeps or ():
        if not isinstance(sweep, Mapping):
            continue
        start_ts = _epoch(sweep.get("exceed_time"))
        reclaim_ts = _epoch(sweep.get("sweep_time"))
        kind = str(sweep.get("kind", "")).lower()
        if start_ts is None or reclaim_ts is None or kind not in ("high", "low"):
            continue
        first_index = candle_index.get(start_ts)
        last_index = candle_index.get(reclaim_ts)
        if first_index is None or last_index is None or last_index < first_index:
            continue
        if any(
            int(completed[j]["time"]) - int(completed[j - 1]["time"]) != BAR_SECONDS
            for j in range(first_index + 1, last_index + 1)
        ):
            continue
        direction = "BUY" if kind == "high" else "SELL"
        end_ts = reclaim_ts + BAR_SECONDS
        volume_ratio, event_volume, average_volume = _sweep_volume_ratio(
            completed, first_index, last_index, volume_lookback
        )
        flow = _flow_metrics(
            flow_minutes, start_ts=start_ts, end_ts=end_ts,
            expected_side=direction, now_ts=float(now_ts),
            feed_started_at=feed_started_at, connection_status=connection_status,
            trade_channel_last_seen=trade_channel_last_seen,
            min_prints=min_prints, min_aggressor_ratio=min_aggressor_ratio,
            min_active_coverage_pct=min_active_coverage_pct,
            max_trade_age_seconds=max_trade_age_seconds,
        )
        oi = _oi_metrics(
            oi_by_time, start_ts=start_ts, end_ts=end_ts,
            now_ts=float(now_ts), latest_oi_time=latest_oi_time,
            max_oi_age_seconds=max_oi_age_seconds,
            baseline_lookback=oi_baseline_lookback,
            min_oi_change_pct=min_oi_change_pct,
        ) if oi_fresh else {
            "available": False, "status": "UNAVAILABLE — OI HISTORY STALE OR MISSING",
            "change_contracts": None, "change_pct": None,
            "threshold_pct": None, "baseline_contracts": None,
            "current_contracts": None, "latest_oi_age_seconds": latest_oi_age,
        }
        volume_ok = volume_ratio is not None and volume_ratio >= min_volume_multiple
        stop_hunt_like = (
            volume_ok and flow["direction_confirmed"]
            and oi.get("available") and oi.get("state") == "REDUCING"
        )
        if stop_hunt_like:
            classification = (
                "BUY-SIDE SWEEP — MULTI-FACTOR STOP-HUNT-LIKE EVIDENCE"
                if kind == "high" else
                "SELL-SIDE SWEEP — MULTI-FACTOR STOP-HUNT-LIKE EVIDENCE"
            )
        elif not volume_ok:
            classification = "PRICE SWEEP — VOLUME NOT CONFIRMED"
        elif not flow["capture_eligible"] or not flow["direction_confirmed"]:
            classification = "PRICE SWEEP — FLOW NOT CONFIRMED"
        elif not oi.get("available"):
            classification = "PRICE SWEEP — OI UNVERIFIED"
        elif oi.get("state") == "BUILDING":
            classification = "PRICE SWEEP — OI BUILDING, NOT OI REDUCTION"
        else:
            classification = "PRICE SWEEP — OI REDUCTION NOT SIGNIFICANT"

        sweep_rows.append({
            "kind": kind,
            "direction": direction,
            "level": _number(sweep.get("price")),
            "breach_price": _number(sweep.get("breach_price")),
            "overshoot_points": _number(sweep.get("overshoot_points")),
            "reclaim_delay_bars": int(sweep.get("reclaim_delay_bars", 0)),
            "reclaim_time": reclaim_ts,
            "first_breach_time": start_ts,
            "window_minutes": (end_ts - start_ts) // 60,
            "event_volume": event_volume,
            "average_comparable_volume": average_volume,
            "relative_volume": volume_ratio,
            "volume_confirmed": volume_ok,
            "flow": flow, "oi": oi,
            "multi_factor_evidence": bool(stop_hunt_like),
            "classification": classification,
            "interpretation_limit": (
                "A stop-hunt-like classification is a price breach/reclaim plus elevated "
                "same-direction taker flow, volume and OI reduction. Public data does not "
                "prove resting stop orders existed or that any specific stop was executed."
            ),
        })

    exhaustion_rows.sort(key=lambda row: row["time"], reverse=True)
    sweep_rows.sort(key=lambda row: row["reclaim_time"], reverse=True)
    return {
        "source": "Delta India BTCUSD native 1m candles, OI:BTCUSD 1m candles and this process's captured public trade aggregates",
        "completed_price_candles": len(completed),
        "completed_oi_candles": len(oi_by_time),
        "latest_oi_time": latest_oi_time,
        "latest_oi_age_seconds": latest_oi_age,
        "oi_history_fresh": bool(oi_fresh),
        "exhaustion_candidates": exhaustion_rows,
        "sweep_candidates": sweep_rows,
        "parameters": {
            "price_extreme_lookback_bars": lookback,
            "volume_baseline": f"average of prior {volume_lookback} completed 1m candles/windows",
            "minimum_relative_volume": min_volume_multiple,
            "minimum_classified_prints": min_prints,
            "minimum_expected-side_taker_ratio": min_aggressor_ratio,
            "minimum_active_minute_coverage_pct": min_active_coverage_pct,
            "minimum_wick_ratio": min_wick_ratio,
            "close_position_limit": close_position_limit,
            "oi_baseline": f"75th percentile of prior {oi_baseline_lookback} absolute 1m OI percentage changes",
            "minimum_oi_change_floor_pct": min_oi_change_pct,
            "maximum_trade_age_seconds": max_trade_age_seconds,
            "maximum_oi_age_seconds": max_oi_age_seconds,
        },
        "interpretation_limits": [
            "OI is aggregate outstanding contracts with both a long and short counterparty; it is not long/short directional positioning.",
            "Taker side identifies the aggressor of captured executions, not whether a participant opened or closed a position.",
            "A stop-hunt-like sweep is a statistical description of public price, volume, flow and OI evidence; it does not prove intent, hidden stops, or liquidation executions.",
            "Trade-stream active-minute coverage and freshness do not prove lossless delivery because public trade messages do not expose per-trade sequence identifiers.",
            "All labels are read-only monitoring context and do not change frozen strategy thresholds, evaluator, release gates, or order execution.",
        ],
    }
