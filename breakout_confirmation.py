"""Read-only multi-timeframe breakout confirmation from Delta-native candles and tape.

A "genuine" label is deliberately strict: a completed candle must cross a
pre-existing 20-bar range, show >=1.5x same-timeframe prior-20-bar candle
volume, receive a second completed close beyond the breakout level, and have
same-direction executed taker flow with >=60% contract share, >=10 classified
prints, >=95% active-minute coverage, and an eligible process capture window.

These are standalone monitoring heuristics. They do not call or modify the
frozen strategy engine, its signal thresholds, evaluator, or order execution.
"""

from __future__ import annotations

import math
from bisect import bisect_left
from collections.abc import Mapping, Sequence
from typing import Any


DEFAULT_TIMEFRAMES = (
    ("1m", 60),
    ("5m", 300),
    ("15m", 900),
    ("1h", 3600),
    ("4h", 14400),
    ("1d", 86400),
)
DEFAULT_RANGE_LOOKBACK = 20
DEFAULT_ATR_PERIOD = 14
DEFAULT_RELATIVE_VOLUME_MULTIPLE = 1.5
DEFAULT_BREAKOUT_BUFFER_ATR = 0.10
DEFAULT_ACCEPTANCE_BUFFER_ATR = 0.05
DEFAULT_RETEST_BAND_ATR = 0.25
DEFAULT_MIN_TAKER_PRINTS = 10
DEFAULT_MIN_TAKER_SHARE_PCT = 60.0
DEFAULT_STRONG_TAKER_SHARE_PCT = 70.0
DEFAULT_MIN_ACTIVE_MINUTE_COVERAGE_PCT = 95.0
DEFAULT_MAX_TRADE_AGE_SECONDS = 20.0
DEFAULT_EVENT_LOOKBACK_BARS = 10
DEFAULT_RETEST_LOOKAHEAD_BARS = 10


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _epoch_seconds(value: Any) -> int | None:
    number = _number(value)
    if number is None or number <= 0:
        return None
    if number > 10_000_000_000:
        number /= 1_000_000
    return int(number)


def _normalize_candles(
    rows: Sequence[Mapping[str, Any]] | None,
    *,
    duration_seconds: int,
    now_ts: float,
) -> list[dict[str, float | int]]:
    """Keep valid, unique, completed native candles; preserve exchange volume units."""
    normalized: dict[int, dict[str, float | int]] = {}
    for row in rows or ():
        if not isinstance(row, Mapping):
            continue
        timestamp = _epoch_seconds(row.get("time"))
        open_price = _number(row.get("open"))
        high = _number(row.get("high"))
        low = _number(row.get("low"))
        close = _number(row.get("close"))
        volume = _number(row.get("volume"))
        if (
            timestamp is None
            or timestamp + duration_seconds > now_ts
            or None in (open_price, high, low, close, volume)
            or volume < 0
            or min(open_price, high, low, close) <= 0
            or high < max(open_price, close, low)
            or low > min(open_price, close, high)
        ):
            continue
        normalized[timestamp] = {
            "time": timestamp,
            "open": float(open_price),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume),
        }
    return [normalized[key] for key in sorted(normalized)]


def _build_flow_index(
    flow_minutes: Mapping[Any, Any] | None,
) -> tuple[list[int], dict[str, list[float]]]:
    """Build prefix sums of captured classified taker trades, keyed by UTC minute."""
    fields = ("buy_prints", "sell_prints", "buy_contracts", "sell_contracts", "buy_usd", "sell_usd")
    normalized: dict[int, dict[str, float]] = {}
    for raw_minute, raw_flow in (flow_minutes or {}).items():
        minute = _epoch_seconds(raw_minute)
        if minute is None or not isinstance(raw_flow, Mapping):
            continue
        row = normalized.setdefault(minute, {field: 0.0 for field in fields})
        for side in ("buy", "sell"):
            row[f"{side}_prints"] += max(0, int(_number(raw_flow.get(f"{side}_count")) or 0.0))
            row[f"{side}_contracts"] += max(0.0, _number(raw_flow.get(f"{side}_contracts")) or 0.0)
            row[f"{side}_usd"] += max(0.0, _number(raw_flow.get(f"{side}_usd")) or 0.0)

    minutes = sorted(normalized)
    prefixes = {field: [0.0] for field in fields}
    for minute in minutes:
        for field in fields:
            prefixes[field].append(prefixes[field][-1] + normalized[minute][field])
    return minutes, prefixes


def _flow_for_bar(
    flow_index: tuple[list[int], dict[str, list[float]]],
    *,
    start_ts: int,
    end_ts: int,
    expected_minutes: int,
    feed_started_at: float | None,
    connection_status: str,
    trade_channel_last_seen: float | None,
    now_ts: float,
    min_prints: int,
    min_active_minute_coverage_pct: float,
    max_trade_age_seconds: float,
    contract_value_btc: float,
    min_taker_share_pct: float,
) -> dict[str, Any]:
    minutes, prefixes = flow_index
    left = bisect_left(minutes, start_ts)
    right = bisect_left(minutes, end_ts)
    totals = {
        field: prefixes[field][right] - prefixes[field][left]
        for field in prefixes
    }
    buy_prints = int(round(totals["buy_prints"]))
    sell_prints = int(round(totals["sell_prints"]))
    buy_contracts = totals["buy_contracts"]
    sell_contracts = totals["sell_contracts"]
    total_contracts = buy_contracts + sell_contracts
    total_prints = buy_prints + sell_prints
    buy_share = buy_contracts / total_contracts * 100.0 if total_contracts > 0 else None
    sell_share = sell_contracts / total_contracts * 100.0 if total_contracts > 0 else None
    active_minutes = right - left
    coverage_pct = min(100.0, active_minutes / expected_minutes * 100.0) if expected_minutes > 0 else 0.0
    trade_age = None if trade_channel_last_seen is None else now_ts - trade_channel_last_seen

    capture_reasons: list[str] = []
    if feed_started_at is None or feed_started_at > start_ts:
        capture_reasons.append("APP STARTED AFTER BREAKOUT BAR BEGAN")
    if str(connection_status).lower() != "connected":
        capture_reasons.append("TRADE FEED NOT CONNECTED")
    if trade_age is None or trade_age < 0 or trade_age > max_trade_age_seconds:
        capture_reasons.append("TRADE CHANNEL STALE")
    if coverage_pct < min_active_minute_coverage_pct:
        capture_reasons.append("LOW ACTIVE-MINUTE COVERAGE")
    if total_prints < min_prints:
        capture_reasons.append("INSUFFICIENT CLASSIFIED PRINTS")

    if capture_reasons:
        capture_status = "UNVERIFIED — " + "; ".join(capture_reasons)
        capture_eligible = False
    else:
        capture_status = "PROCESS WINDOW COVERED · FRESH · ACTIVE-MINUTE COVERAGE SUFFICIENT"
        capture_eligible = True

    if not capture_eligible:
        flow_direction = "UNVERIFIED"
    elif buy_share is not None and buy_share >= min_taker_share_pct:
        flow_direction = "BUY"
    elif sell_share is not None and sell_share >= min_taker_share_pct:
        flow_direction = "SELL"
    else:
        flow_direction = "BALANCED"

    return {
        "buy_prints": buy_prints,
        "sell_prints": sell_prints,
        "classified_prints": total_prints,
        "buy_contracts": buy_contracts,
        "sell_contracts": sell_contracts,
        "net_delta_contracts": buy_contracts - sell_contracts,
        "buy_share_pct": buy_share,
        "sell_share_pct": sell_share,
        "buy_btc": buy_contracts * contract_value_btc,
        "sell_btc": sell_contracts * contract_value_btc,
        "buy_usd": totals["buy_usd"],
        "sell_usd": totals["sell_usd"],
        "total_contracts": total_contracts,
        "total_btc": total_contracts * contract_value_btc,
        "active_trade_minutes": active_minutes,
        "expected_minutes": expected_minutes,
        "active_minute_coverage_pct": coverage_pct,
        "latest_trade_age_seconds": trade_age,
        "capture_status": capture_status,
        "capture_eligible": capture_eligible,
        "direction": flow_direction,
    }


def _true_range(current: Mapping[str, Any], previous_close: float) -> float:
    high = float(current["high"])
    low = float(current["low"])
    return max(high - low, abs(high - previous_close), abs(low - previous_close))



def _has_contiguous_candles(
    candles: Sequence[Mapping[str, Any]],
    *,
    start_index: int,
    end_index: int,
    duration_seconds: int,
) -> bool:
    """Reject a signal when any candle needed for its range/ATR/acceptance is missing."""
    if end_index - start_index <= 1:
        return True
    return all(
        int(candles[index]["time"]) - int(candles[index - 1]["time"]) == duration_seconds
        for index in range(start_index + 1, end_index)
    )


def _atr_before(
    candles: Sequence[Mapping[str, Any]],
    index: int,
    period: int,
) -> float | None:
    if index < period:
        return None
    values = [
        _true_range(candles[j], float(candles[j - 1]["close"]))
        for j in range(index - period, index)
    ]
    return sum(values) / period if values else None


def _evaluate_candidate(
    candles: Sequence[Mapping[str, Any]],
    *,
    index: int,
    direction: str,
    level: float,
    atr: float,
    relative_volume: float | None,
    flow_index: tuple[list[int], dict[str, list[float]]],
    duration_seconds: int,
    feed_started_at: float | None,
    connection_status: str,
    trade_channel_last_seen: float | None,
    now_ts: float,
    min_taker_prints: int,
    min_taker_share_pct: float,
    min_active_minute_coverage_pct: float,
    max_trade_age_seconds: float,
    contract_value_btc: float,
    min_relative_volume: float,
    acceptance_buffer_atr: float,
    retest_band_atr: float,
    retest_lookahead_bars: int,
) -> dict[str, Any]:
    breakout = candles[index]
    breakout_start = int(breakout["time"])
    breakout_end = breakout_start + duration_seconds
    flow = _flow_for_bar(
        flow_index,
        start_ts=breakout_start,
        end_ts=breakout_end,
        expected_minutes=duration_seconds // 60,
        feed_started_at=feed_started_at,
        connection_status=connection_status,
        trade_channel_last_seen=trade_channel_last_seen,
        now_ts=now_ts,
        min_prints=min_taker_prints,
        min_active_minute_coverage_pct=min_active_minute_coverage_pct,
        max_trade_age_seconds=max_trade_age_seconds,
        contract_value_btc=contract_value_btc,
        min_taker_share_pct=min_taker_share_pct,
    )

    acceptance_buffer = atr * acceptance_buffer_atr
    confirmation_index = index + 1
    has_confirmation_bar = confirmation_index < len(candles)
    confirmation_close = (
        float(candles[confirmation_index]["close"]) if has_confirmation_bar else None
    )

    if direction == "BUY":
        accepted = has_confirmation_bar and confirmation_close > level + acceptance_buffer
        invalidated = has_confirmation_bar and confirmation_close <= level
    else:
        accepted = has_confirmation_bar and confirmation_close < level - acceptance_buffer
        invalidated = has_confirmation_bar and confirmation_close >= level

    retest_status = "NOT YET TESTED"
    latest_state = "DEVELOPING — WAITING FOR SECOND COMPLETED CLOSE"
    failed = bool(invalidated)
    if has_confirmation_bar and accepted:
        latest_state = "PRICE ACCEPTED — CHECKING VOLUME AND ORDER FLOW"
        end_index = min(len(candles), confirmation_index + retest_lookahead_bars + 1)
        for j in range(confirmation_index + 1, end_index):
            bar = candles[j]
            close = float(bar["close"])
            if direction == "BUY":
                if close <= level:
                    failed = True
                    retest_status = "FAILED — CLOSED BACK BELOW LEVEL"
                    break
                touched = float(bar["low"]) <= level + retest_band_atr * atr
                held = close > level + acceptance_buffer
            else:
                if close >= level:
                    failed = True
                    retest_status = "FAILED — CLOSED BACK ABOVE LEVEL"
                    break
                touched = float(bar["high"]) >= level - retest_band_atr * atr
                held = close < level - acceptance_buffer
            if touched:
                if held:
                    retest_status = "HELD — CLOSE ACCEPTED BEYOND LEVEL"
                else:
                    retest_status = "INCONCLUSIVE — RETEST CLOSE TOO CLOSE TO LEVEL"
        if failed:
            latest_state = "FAILED BREAKOUT — LEVEL RECLAIMED"
        else:
            latest_state = "PRICE ACCEPTED — VOLUME / ORDER FLOW PENDING"
    elif has_confirmation_bar and not accepted:
        latest_state = "FAILED BREAKOUT — SECOND CLOSE DID NOT HOLD"
        failed = True

    volume_confirmed = (
        relative_volume is not None and relative_volume >= min_relative_volume
    )
    if flow["capture_eligible"]:
        flow_confirmed = flow["direction"] == direction
        flow_opposes = flow["direction"] in ("BUY", "SELL") and flow["direction"] != direction
    else:
        flow_confirmed = False
        flow_opposes = False

    if failed:
        classification = "FAILED BREAKOUT"
    elif not has_confirmation_bar:
        classification = "BREAKOUT DEVELOPING"
    elif not accepted:
        classification = "FAILED BREAKOUT"
    elif not volume_confirmed:
        classification = "PRICE ACCEPTED — VOLUME NOT CONFIRMED"
    elif not flow["capture_eligible"]:
        classification = "PRICE + VOLUME — ORDER FLOW UNVERIFIED"
    elif flow_opposes:
        classification = "PRICE + VOLUME — ORDER-FLOW DIVERGENCE"
    elif not flow_confirmed:
        classification = "PRICE + VOLUME — ORDER FLOW BALANCED"
    else:
        classification = f"GENUINE {direction} BREAKOUT"
        if retest_status.startswith("HELD"):
            classification += " — RETEST HELD"

    return {
        "direction": direction,
        "classification": classification,
        "breakout_time": breakout_start,
        "breakout_close": float(breakout["close"]),
        "level": level,
        "atr": atr,
        "breakout_buffer_points": atr * DEFAULT_BREAKOUT_BUFFER_ATR,
        "confirmation_time": (
            int(candles[confirmation_index]["time"]) if has_confirmation_bar else None
        ),
        "confirmation_close": confirmation_close,
        "price_accepted": bool(accepted),
        "failed": failed,
        "retest_status": retest_status,
        "breakout_volume": float(breakout["volume"]),
        "average_prior_20_volume": None,
        "relative_volume": relative_volume,
        "volume_confirmed": volume_confirmed,
        "flow_confirmed": flow_confirmed,
        "flow_opposes": flow_opposes,
        "flow": flow,
        "price_state": latest_state,
    }


def _sweep_result(
    candle: Mapping[str, Any],
    *,
    direction: str,
    level: float,
    atr: float,
    relative_volume: float | None,
    flow: dict[str, Any],
) -> dict[str, Any]:
    label = (
        "BUY-SIDE LIQUIDITY SWEEP — FAILED UPSIDE ACCEPTANCE"
        if direction == "BUY"
        else "SELL-SIDE LIQUIDITY SWEEP — FAILED DOWNSIDE ACCEPTANCE"
    )
    return {
        "direction": direction,
        "classification": label,
        "breakout_time": int(candle["time"]),
        "breakout_close": float(candle["close"]),
        "level": level,
        "atr": atr,
        "breakout_buffer_points": atr * DEFAULT_BREAKOUT_BUFFER_ATR,
        "confirmation_time": None,
        "confirmation_close": None,
        "price_accepted": False,
        "failed": True,
        "retest_status": "NOT APPLICABLE — WICK REJECTED",
        "breakout_volume": float(candle["volume"]),
        "average_prior_20_volume": None,
        "relative_volume": relative_volume,
        "volume_confirmed": relative_volume is not None and relative_volume >= DEFAULT_RELATIVE_VOLUME_MULTIPLE,
        "flow_confirmed": False,
        "flow_opposes": False,
        "flow": flow,
        "price_state": label,
    }


def detect_multi_timeframe_breakouts(
    candle_history: Mapping[str, Sequence[Mapping[str, Any]]] | None,
    flow_minutes: Mapping[Any, Any] | None,
    *,
    now_ts: float,
    feed_started_at: float | None,
    connection_status: str,
    trade_channel_last_seen: float | None,
    timeframes: Sequence[tuple[str, int]] = DEFAULT_TIMEFRAMES,
    range_lookback: int = DEFAULT_RANGE_LOOKBACK,
    atr_period: int = DEFAULT_ATR_PERIOD,
    min_relative_volume: float = DEFAULT_RELATIVE_VOLUME_MULTIPLE,
    breakout_buffer_atr: float = DEFAULT_BREAKOUT_BUFFER_ATR,
    acceptance_buffer_atr: float = DEFAULT_ACCEPTANCE_BUFFER_ATR,
    retest_band_atr: float = DEFAULT_RETEST_BAND_ATR,
    min_taker_prints: int = DEFAULT_MIN_TAKER_PRINTS,
    min_taker_share_pct: float = DEFAULT_MIN_TAKER_SHARE_PCT,
    min_active_minute_coverage_pct: float = DEFAULT_MIN_ACTIVE_MINUTE_COVERAGE_PCT,
    max_trade_age_seconds: float = DEFAULT_MAX_TRADE_AGE_SECONDS,
    contract_value_btc: float = 0.001,
    event_lookback_bars: int = DEFAULT_EVENT_LOOKBACK_BARS,
    retest_lookahead_bars: int = DEFAULT_RETEST_LOOKAHEAD_BARS,
) -> dict[str, Any]:
    """Identify and confirm recent breakout candidates for all requested timeframes.

    Breakout level is the high/low of the preceding range_lookback completed
    candles. Trigger bar must close at least breakout_buffer_atr × ATR beyond
    the level. Confirmation requires the next completed candle to close at
    least acceptance_buffer_atr × ATR beyond the level. Genuine classification
    additionally requires same-side candle volume >= min_relative_volume ×
    average prior-20 candle volume and eligible same-side taker flow on the
    breakout candle. Retest status is reported separately.

    Candle volume remains in Delta's reported unit; only within-timeframe
    ratios are compared. Taker volume comes from classified Delta trade prints.
    """
    now = _number(now_ts)
    if now is None or now <= 0:
        raise ValueError("now_ts must be finite and positive")
    if not isinstance(range_lookback, int) or range_lookback < 2:
        raise ValueError("range_lookback must be an integer >= 2")
    if not isinstance(atr_period, int) or atr_period < 2:
        raise ValueError("atr_period must be an integer >= 2")
    if not isinstance(min_taker_prints, int) or min_taker_prints < 1:
        raise ValueError("min_taker_prints must be a positive integer")
    if not 50.0 < float(min_taker_share_pct) <= 100.0:
        raise ValueError("min_taker_share_pct must be >50 and <=100")
    if not 0 < float(min_active_minute_coverage_pct) <= 100:
        raise ValueError("min_active_minute_coverage_pct must be within (0, 100]")
    if min_relative_volume <= 0 or breakout_buffer_atr <= 0 or acceptance_buffer_atr < 0:
        raise ValueError("volume and breakout buffers must be positive; acceptance buffer cannot be negative")
    if not isinstance(event_lookback_bars, int) or event_lookback_bars < 1:
        raise ValueError("event_lookback_bars must be a positive integer")
    if not isinstance(retest_lookahead_bars, int) or retest_lookahead_bars < 1:
        raise ValueError("retest_lookahead_bars must be a positive integer")

    flow_index = _build_flow_index(flow_minutes)
    history = candle_history if isinstance(candle_history, Mapping) else {}
    results: list[dict[str, Any]] = []

    for name, duration in timeframes:
        candles = _normalize_candles(
            history.get(name, ()),
            duration_seconds=duration,
            now_ts=now,
        )
        base = {
            "timeframe": name,
            "duration_seconds": duration,
            "completed_candles": len(candles),
            "range_lookback": range_lookback,
            "relative_volume_multiple_required": min_relative_volume,
            "taker_dominance_required_pct": min_taker_share_pct,
            "minimum_taker_prints": min_taker_prints,
            "minimum_active_minute_coverage_pct": min_active_minute_coverage_pct,
        }
        minimum_bars = max(range_lookback, atr_period) + 2
        if len(candles) < minimum_bars:
            results.append({
                **base,
                "classification": "INSUFFICIENT PRICE HISTORY",
                "direction": None,
                "reason": f"Need at least {minimum_bars} completed candles; have {len(candles)}.",
            })
            continue

        start_index = max(range_lookback, len(candles) - event_lookback_bars)
        candidates: list[tuple[int, str, float, float, float | None, float]] = []
        latest_sweep: tuple[int, str, float, float, float | None] | None = None

        latest_context_start = max(0, len(candles) - max(range_lookback, atr_period) - 1)
        latest_context_end = len(candles)
        latest_context_contiguous = _has_contiguous_candles(
            candles,
            start_index=latest_context_start,
            end_index=latest_context_end,
            duration_seconds=duration,
        )

        for index in range(start_index, len(candles)):
            context_start = max(0, index - max(range_lookback, atr_period))
            context_end = min(len(candles), index + 2)
            if not _has_contiguous_candles(
                candles,
                start_index=context_start,
                end_index=context_end,
                duration_seconds=duration,
            ):
                continue
            atr = _atr_before(candles, index, atr_period)
            if atr is None or atr <= 0:
                continue
            prior = candles[index - range_lookback:index]
            resistance = max(float(row["high"]) for row in prior)
            support = min(float(row["low"]) for row in prior)
            prior_close = float(candles[index - 1]["close"])
            current = candles[index]
            close = float(current["close"])
            high = float(current["high"])
            low = float(current["low"])
            buffer = breakout_buffer_atr * atr
            previous_volumes = [
                float(row["volume"])
                for row in candles[max(0, index - range_lookback):index]
            ]
            average_volume = sum(previous_volumes) / len(previous_volumes) if previous_volumes else 0.0
            relative_volume = (
                float(current["volume"]) / average_volume
                if average_volume > 0 else None
            )

            up_cross = prior_close <= resistance and close > resistance + buffer
            down_cross = prior_close >= support and close < support - buffer
            if up_cross:
                candidates.append((index, "BUY", resistance, atr, relative_volume, average_volume))
            elif down_cross:
                candidates.append((index, "SELL", support, atr, relative_volume, average_volume))

            if index == len(candles) - 1:
                if high > resistance + buffer and close <= resistance:
                    latest_sweep = (index, "BUY", resistance, atr, relative_volume)
                elif low < support - buffer and close >= support:
                    latest_sweep = (index, "SELL", support, atr, relative_volume)

        if candidates:
            index, direction, level, atr, relative_volume, average_volume = candidates[-1]
            evaluated = _evaluate_candidate(
                candles,
                index=index,
                direction=direction,
                level=level,
                atr=atr,
                relative_volume=relative_volume,
                flow_index=flow_index,
                duration_seconds=duration,
                feed_started_at=_number(feed_started_at),
                connection_status=connection_status,
                trade_channel_last_seen=_number(trade_channel_last_seen),
                now_ts=now,
                min_taker_prints=min_taker_prints,
                min_taker_share_pct=min_taker_share_pct,
                min_active_minute_coverage_pct=min_active_minute_coverage_pct,
                max_trade_age_seconds=max_trade_age_seconds,
                contract_value_btc=contract_value_btc,
                min_relative_volume=min_relative_volume,
                acceptance_buffer_atr=acceptance_buffer_atr,
                retest_band_atr=retest_band_atr,
                retest_lookahead_bars=retest_lookahead_bars,
            )
            evaluated["average_prior_20_volume"] = average_volume
            results.append({**base, **evaluated})
            continue

        if latest_sweep is not None and latest_context_contiguous:
            index, direction, level, atr, relative_volume = latest_sweep
            candle = candles[index]
            flow = _flow_for_bar(
                flow_index,
                start_ts=int(candle["time"]),
                end_ts=int(candle["time"]) + duration,
                expected_minutes=duration // 60,
                feed_started_at=_number(feed_started_at),
                connection_status=connection_status,
                trade_channel_last_seen=_number(trade_channel_last_seen),
                now_ts=now,
                min_prints=min_taker_prints,
                min_active_minute_coverage_pct=min_active_minute_coverage_pct,
                max_trade_age_seconds=max_trade_age_seconds,
                contract_value_btc=contract_value_btc,
                min_taker_share_pct=min_taker_share_pct,
            )
            results.append({
                **base,
                **_sweep_result(
                    candle,
                    direction=direction,
                    level=level,
                    atr=atr,
                    relative_volume=relative_volume,
                    flow=flow,
                ),
            })
            continue

        latest = candles[-1]
        results.append({
            **base,
            "classification": (
                "INSUFFICIENT — CANDLE HISTORY GAP"
                if not latest_context_contiguous
                else "NO RECENT BREAKOUT"
            ),
            "direction": None,
            "latest_close": float(latest["close"]),
            "latest_candle_time": int(latest["time"]),
            "reason": f"No 20-bar range break in the latest {event_lookback_bars} completed bars.",
        })

    return {
        "windows": results,
        "method": {
            "breakout_level": f"Highest high / lowest low of preceding {range_lookback} completed native candles",
            "trigger": f"Completed close beyond level by at least {breakout_buffer_atr:.2f} × prior ATR({atr_period})",
            "price_acceptance": f"Next completed close remains at least {acceptance_buffer_atr:.2f} × ATR beyond level",
            "volume_confirmation": f"Breakout candle Delta volume >= {min_relative_volume:.2f}x average of preceding {range_lookback} candles",
            "orderflow_confirmation": f"Same-side taker share >= {min_taker_share_pct:.1f}%, >= {min_taker_prints} classified prints",
            "flow_activity_coverage": f">={min_active_minute_coverage_pct:.1f}% of one-minute buckets contain classified trade prints",
            "retest": f"Level retest band within {retest_band_atr:.2f} × ATR, evaluated for up to {retest_lookahead_bars} subsequent candles",
        },
        "limitations": (
            "A 'genuine' label means these explicit price, candle-volume and captured taker-flow conditions aligned; it is not a guarantee of continuation or profitability. "
            "Active-minute coverage measures minutes with observed classified prints; it does not prove every trade was delivered. Delta's public trade messages do not expose a per-trade sequence number. "
            "Taker prints are executions, not unique parent orders. Candle volume remains in exchange-reported units and is compared only within the same timeframe. "
            "This module is separate read-only monitoring and does not modify the frozen strategy or place orders."
        ),
    }
