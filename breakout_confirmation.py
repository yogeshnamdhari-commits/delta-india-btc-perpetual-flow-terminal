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
    breakout_buffer_atr: float,
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
        "breakout_buffer_points": atr * breakout_buffer_atr,
        "breakout_buffer_atr": breakout_buffer_atr,
        "acceptance_buffer_atr": acceptance_buffer_atr,
        "acceptance_buffer_points": acceptance_buffer,
        "breakout_high": float(breakout["high"]),
        "breakout_low": float(breakout["low"]),
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
    average_prior_20_volume: float | None = None,
    breakout_buffer_atr: float = DEFAULT_BREAKOUT_BUFFER_ATR,
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
        "breakout_buffer_points": atr * breakout_buffer_atr,
        "confirmation_time": None,
        "confirmation_close": None,
        "price_accepted": False,
        "failed": True,
        "retest_status": "NOT APPLICABLE — WICK REJECTED",
        "breakout_volume": float(candle["volume"]),
        "average_prior_20_volume": average_prior_20_volume,
        "relative_volume": relative_volume,
        "volume_confirmed": relative_volume is not None and relative_volume >= DEFAULT_RELATIVE_VOLUME_MULTIPLE,
        "flow_confirmed": False,
        "flow_opposes": False,
        "flow": flow,
        "price_state": label,
        "breakout_high": float(candle["high"]),
        "breakout_low": float(candle["low"]),
        "breakout_buffer_atr": breakout_buffer_atr,
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
            "strong_taker_dominance_required_pct": DEFAULT_STRONG_TAKER_SHARE_PCT,
            "breakout_buffer_atr": breakout_buffer_atr,
            "acceptance_buffer_atr": acceptance_buffer_atr,
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
        latest_sweep: tuple[int, str, float, float, float | None, float] | None = None

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
                    latest_sweep = (index, "BUY", resistance, atr, relative_volume, average_volume)
                elif low < support - buffer and close >= support:
                    latest_sweep = (index, "SELL", support, atr, relative_volume, average_volume)

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
                breakout_buffer_atr=breakout_buffer_atr,
                acceptance_buffer_atr=acceptance_buffer_atr,
                retest_band_atr=retest_band_atr,
                retest_lookahead_bars=retest_lookahead_bars,
            )
            evaluated["average_prior_20_volume"] = average_volume
            results.append({**base, **evaluated})
            continue

        if latest_sweep is not None and latest_context_contiguous:
            index, direction, level, atr, relative_volume, average_volume = latest_sweep
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
                    average_prior_20_volume=average_volume,
                    breakout_buffer_atr=breakout_buffer_atr,
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



def build_breakout_explanation(item: Mapping[str, Any]) -> dict[str, Any]:
    """Build human-readable, calculation-backed reasons without changing classification.

    This helper is presentation/diagnostics only. It does not emit trading signals,
    mutate the input, or alter any breakout confirmation threshold.
    """
    if not isinstance(item, Mapping):
        item = {}

    classification = str(item.get("classification") or "UNKNOWN")
    direction = str(item.get("direction") or "").upper()
    is_sweep = "LIQUIDITY SWEEP" in classification
    is_genuine = classification.startswith("GENUINE ")
    flow = item.get("flow") if isinstance(item.get("flow"), Mapping) else {}

    def fmt(value: Any, digits: int = 2) -> str:
        number = _number(value)
        return "unavailable" if number is None else f"{number:,.{digits}f}"

    def add_check(
        condition: str,
        status: str,
        actual: str,
        required: str,
        explanation: str,
    ) -> None:
        checks.append({
            "Condition": condition,
            "Result": status,
            "Measured evidence": actual,
            "Required": required,
            "Explanation": explanation,
        })

    checks: list[dict[str, str]] = []
    primary_reason = "CLASSIFICATION_PENDING"
    failure_reason = ""
    success_reason = ""
    failure_details: list[str] = []

    level = _number(item.get("level"))
    atr = _number(item.get("atr"))
    breakout_close = _number(item.get("breakout_close"))
    breakout_buffer_atr = _number(item.get("breakout_buffer_atr"))
    if breakout_buffer_atr is None:
        breakout_buffer_atr = DEFAULT_BREAKOUT_BUFFER_ATR
    acceptance_buffer_atr = _number(item.get("acceptance_buffer_atr"))
    if acceptance_buffer_atr is None:
        acceptance_buffer_atr = DEFAULT_ACCEPTANCE_BUFFER_ATR
    breakout_buffer_points = _number(item.get("breakout_buffer_points"))
    if breakout_buffer_points is None and atr is not None:
        breakout_buffer_points = atr * breakout_buffer_atr
    acceptance_buffer_points = _number(item.get("acceptance_buffer_points"))
    if acceptance_buffer_points is None and atr is not None:
        acceptance_buffer_points = atr * acceptance_buffer_atr

    directional_sign = 1.0 if direction == "BUY" else -1.0 if direction == "SELL" else None
    trigger_clearance = (
        directional_sign * (breakout_close - level)
        if directional_sign is not None and breakout_close is not None and level is not None
        else None
    )

    if direction not in ("BUY", "SELL"):
        no_event_status = "INSUFFICIENT" if "INSUFFICIENT" in classification else "NO EVENT"
        add_check(
            "Breakout event detection",
            no_event_status,
            classification,
            "A completed candle must close beyond the prior 20-candle range plus the ATR buffer",
            str(item.get("reason") or "No directional breakout candidate is available."),
        )
    elif is_sweep:
        wick_value = item.get("breakout_high") if direction == "BUY" else item.get("breakout_low")
        close_text = fmt(breakout_close)
        add_check(
            "Breakout close beyond range",
            "SWEEP",
            f"Wick extreme {fmt(wick_value)}; close {close_text}; level {fmt(level)}",
            f"Close beyond level by more than {fmt(breakout_buffer_points)} points",
            "The wick penetrated the range boundary, but the candle closed back inside. This is classified as a liquidity sweep, not a confirmed breakout.",
        )
    elif trigger_clearance is None or breakout_buffer_points is None:
        add_check(
            "Initial range breakout",
            "UNVERIFIED",
            f"Close {fmt(breakout_close)}; level {fmt(level)}",
            f"> {fmt(breakout_buffer_points)} points beyond level",
            "Required price inputs are missing; the trigger cannot be independently recalculated.",
        )
    else:
        trigger_pass = trigger_clearance > breakout_buffer_points
        trigger_status = "PASS" if trigger_pass else "FAIL"
        add_check(
            "Initial range breakout",
            trigger_status,
            f"Close {fmt(breakout_close)}; level {fmt(level)}; clearance {fmt(trigger_clearance)} points ({fmt(trigger_clearance / atr if atr else None)} ATR)",
            f"> {fmt(breakout_buffer_points)} points ({fmt(breakout_buffer_atr)} ATR)",
            "The trigger candle closed beyond the previous range boundary by the configured ATR buffer."
            if trigger_pass
            else "The trigger candle did not clear the configured ATR buffer.",
        )
        if not trigger_pass:
            failure_details.append("The initial breakout close did not clear the configured ATR buffer.")

    confirmation_close = _number(item.get("confirmation_close"))
    confirmation_clearance = (
        directional_sign * (confirmation_close - level)
        if directional_sign is not None and confirmation_close is not None and level is not None
        else None
    )
    price_accepted = bool(item.get("price_accepted"))
    if is_sweep:
        add_check(
            "Next-candle price acceptance",
            "NOT APPLICABLE",
            "Not evaluated for a wick-only sweep",
            "A completed follow-through candle must close beyond the acceptance buffer",
            "A wick-through/close-back-inside event is handled as a liquidity sweep.",
        )
    elif direction not in ("BUY", "SELL") or level is None or acceptance_buffer_points is None:
        add_check(
            "Next-candle price acceptance",
            "UNVERIFIED",
            f"Confirmation close {fmt(confirmation_close)}",
            "A completed close beyond the ATR acceptance buffer",
            "Price inputs are unavailable for this condition.",
        )
    elif confirmation_close is None:
        add_check(
            "Next-candle price acceptance",
            "PENDING",
            "No next completed candle yet",
            f"Close more than {fmt(acceptance_buffer_points)} points beyond level {fmt(level)}",
            "The confirmation candle has not completed. No pass or failure is assigned yet.",
        )
    elif price_accepted:
        add_check(
            "Next-candle price acceptance",
            "PASS",
            f"Confirmation close {fmt(confirmation_close)}; directional clearance {fmt(confirmation_clearance)} points ({fmt(confirmation_clearance / atr if atr else None)} ATR)",
            f"> {fmt(acceptance_buffer_points)} points ({fmt(acceptance_buffer_atr)} ATR)",
            "The next completed candle closed beyond the required acceptance buffer.",
        )
    else:
        reclaimed = confirmation_clearance is not None and confirmation_clearance <= 0
        reason = (
            "The confirmation candle closed back through the broken range level."
            if reclaimed
            else "The confirmation candle stayed on the breakout side but did not clear the required acceptance buffer."
        )
        add_check(
            "Next-candle price acceptance",
            "FAIL",
            f"Confirmation close {fmt(confirmation_close)}; directional clearance {fmt(confirmation_clearance)} points",
            f"> {fmt(acceptance_buffer_points)} points beyond level {fmt(level)}",
            reason,
        )
        failure_details.append(reason)

    relative_volume = _number(item.get("relative_volume"))
    required_volume = _number(item.get("relative_volume_multiple_required"))
    if required_volume is None:
        required_volume = DEFAULT_RELATIVE_VOLUME_MULTIPLE
    average_volume = _number(item.get("average_prior_20_volume"))
    breakout_volume = _number(item.get("breakout_volume"))
    if relative_volume is None:
        add_check(
            "Breakout-candle relative volume",
            "UNVERIFIED",
            f"Breakout volume {fmt(breakout_volume)}; prior average {fmt(average_volume)}",
            f">= {fmt(required_volume)}x",
            "The volume ratio is unavailable; this condition is not treated as passed.",
        )
    elif relative_volume >= required_volume:
        add_check(
            "Breakout-candle relative volume",
            "PASS",
            f"{fmt(relative_volume)}x; breakout volume {fmt(breakout_volume)}; prior-20 average {fmt(average_volume)}",
            f">= {fmt(required_volume)}x",
            "Breakout-candle volume met or exceeded the same-timeframe prior-20-candle average threshold.",
        )
    else:
        volume_reason = f"Relative volume {fmt(relative_volume)}x was below the required {fmt(required_volume)}x."
        add_check(
            "Breakout-candle relative volume",
            "FAIL",
            f"{fmt(relative_volume)}x; breakout volume {fmt(breakout_volume)}; prior-20 average {fmt(average_volume)}",
            f">= {fmt(required_volume)}x",
            volume_reason,
        )
        failure_details.append(volume_reason)

    min_prints = int(_number(item.get("minimum_taker_prints")) or DEFAULT_MIN_TAKER_PRINTS)
    min_coverage = _number(item.get("minimum_active_minute_coverage_pct"))
    if min_coverage is None:
        min_coverage = DEFAULT_MIN_ACTIVE_MINUTE_COVERAGE_PCT
    min_taker_share = _number(item.get("taker_dominance_required_pct"))
    if min_taker_share is None:
        min_taker_share = DEFAULT_MIN_TAKER_SHARE_PCT
    strong_taker_share = _number(item.get("strong_taker_dominance_required_pct"))
    if strong_taker_share is None:
        strong_taker_share = DEFAULT_STRONG_TAKER_SHARE_PCT

    capture_eligible = bool(flow.get("capture_eligible"))
    capture_status = str(flow.get("capture_status") or "Trade-flow capture evidence unavailable.")
    classified_prints = _number(flow.get("classified_prints"))
    active_minutes = _number(flow.get("active_trade_minutes"))
    expected_minutes = _number(flow.get("expected_minutes"))
    coverage = _number(flow.get("active_minute_coverage_pct"))
    if classified_prints is None:
        add_check(
            "Classified trade prints",
            "UNVERIFIED",
            "Unavailable",
            f">= {min_prints} prints",
            "The print count is missing; missing data is not converted into a zero measurement.",
        )
    else:
        print_status = "PASS" if classified_prints >= min_prints else "INSUFFICIENT"
        add_check(
            "Classified trade prints",
            print_status,
            f"{int(classified_prints):,} prints",
            f">= {min_prints} prints",
            "The minimum classified-print count is present."
            if print_status == "PASS"
            else "The captured print count is below the minimum; order-flow confirmation remains unverified.",
        )
    if coverage is None:
        add_check(
            "Active-minute trade coverage",
            "UNVERIFIED",
            "Unavailable",
            f">= {fmt(min_coverage, 1)}%",
            "Coverage data is missing; this condition cannot be verified.",
        )
    else:
        coverage_status = "PASS" if coverage >= min_coverage else "INSUFFICIENT"
        add_check(
            "Active-minute trade coverage",
            coverage_status,
            f"{fmt(coverage, 1)}% ({fmt(active_minutes, 0)}/{fmt(expected_minutes, 0)} active minutes)",
            f">= {fmt(min_coverage, 1)}%",
            "The active-minute coverage threshold is met."
            if coverage_status == "PASS"
            else "Captured active minutes are below the required coverage; this is a data-quality limitation, not proof that the market condition failed.",
        )

    if capture_eligible:
        add_check(
            "Trade-feed capture quality",
            "PASS",
            capture_status,
            "Feed connected; capture began before the event; last trade fresh; print and coverage gates met",
            "The process capture window passed the configured eligibility checks. This still does not prove lossless delivery because the public stream has no per-trade sequence identifier.",
        )
    else:
        add_check(
            "Trade-feed capture quality",
            "UNVERIFIED",
            capture_status,
            "Feed connected; capture began before the event; last trade fresh; print and coverage gates met",
            "Trade evidence is not eligible for directional confirmation. This must not be interpreted as zero market activity.",
        )

    directional_share = _number(flow.get("buy_share_pct" if direction == "BUY" else "sell_share_pct"))
    opposing_share = _number(flow.get("sell_share_pct" if direction == "BUY" else "buy_share_pct"))
    if direction not in ("BUY", "SELL") or not capture_eligible:
        add_check(
            "Same-direction taker dominance",
            "UNVERIFIED",
            f"{direction or '—'} share {fmt(directional_share, 1)}%; opposing share {fmt(opposing_share, 1)}%",
            f">= {fmt(min_taker_share, 1)}% of captured contracts",
            "Directional taker dominance is not scored because the trade capture window is not eligible.",
        )
    elif directional_share is not None and directional_share >= min_taker_share:
        add_check(
            "Same-direction taker dominance",
            "PASS",
            f"{direction} {fmt(directional_share, 1)}%; opposing side {fmt(opposing_share, 1)}%",
            f">= {fmt(min_taker_share, 1)}% of captured contracts",
            "Same-direction taker contracts met the minimum dominance threshold.",
        )
    else:
        dominance_reason = f"{direction} taker share {fmt(directional_share, 1)}% was below the required {fmt(min_taker_share, 1)}%."
        add_check(
            "Same-direction taker dominance",
            "FAIL",
            f"{direction} {fmt(directional_share, 1)}%; opposing side {fmt(opposing_share, 1)}%",
            f">= {fmt(min_taker_share, 1)}% of captured contracts",
            dominance_reason,
        )
        failure_details.append(dominance_reason)

    if directional_share is not None and capture_eligible:
        add_check(
            "Strong taker dominance (informational)",
            "STRONG" if directional_share >= strong_taker_share else "STANDARD",
            f"{direction} {fmt(directional_share, 1)}%",
            f"Strong >= {fmt(strong_taker_share, 1)}% (not a genuine-breakout gate)",
            "This is an informational strength tier; the genuine-breakout minimum remains the configured 60% threshold.",
        )
    else:
        add_check(
            "Strong taker dominance (informational)",
            "UNVERIFIED",
            f"{direction or '—'} share {fmt(directional_share, 1)}%",
            f"Strong >= {fmt(strong_taker_share, 1)}% (not a genuine-breakout gate)",
            "Strength cannot be assessed without eligible directional trade capture.",
        )

    retest_status = str(item.get("retest_status") or "NOT YET TESTED")
    if is_sweep or retest_status.startswith("NOT APPLICABLE"):
        add_check(
            "Retest (tracked separately)",
            "NOT APPLICABLE",
            retest_status,
            "Retest is not required for initial breakout confirmation",
            "A wick-rejected liquidity sweep has no breakout retest to score.",
        )
    elif retest_status.startswith("HELD"):
        add_check(
            "Retest (tracked separately)",
            "PASS",
            retest_status,
            "Retest touch followed by a close accepted beyond the level",
            "The retest held; this is reported separately from initial breakout confirmation.",
        )
    elif retest_status.startswith("FAILED"):
        add_check(
            "Retest (tracked separately)",
            "FAIL",
            retest_status,
            "Price must not close back through the broken level",
            "The level was reclaimed during the observed retest window.",
        )
    elif retest_status.startswith("INCONCLUSIVE"):
        add_check(
            "Retest (tracked separately)",
            "INCONCLUSIVE",
            retest_status,
            "Retest close must remain beyond the acceptance buffer",
            "A retest was observed but did not provide a decisive hold.",
        )
    else:
        add_check(
            "Retest (tracked separately)",
            "PENDING",
            retest_status,
            "Retest is evaluated independently over the configured lookahead",
            "No qualifying retest has been recorded yet; this does not invalidate an otherwise confirmed breakout.",
        )

    capture_reasons = capture_status.upper()
    if is_sweep:
        primary_reason = "LIQUIDITY_SWEEP"
        failure_reason = (
            f"{direction}-side liquidity sweep: wick extreme {fmt(item.get('breakout_high') if direction == 'BUY' else item.get('breakout_low'))} "
            f"crossed level {fmt(level)}, but candle close {fmt(breakout_close)} returned inside the range."
        )
    elif retest_status.startswith("FAILED"):
        primary_reason = "FAILED_RETEST_RECLAIM"
        failure_reason = f"The initial price acceptance occurred, but the later retest closed back through level {fmt(level)}. {retest_status}."
    elif confirmation_close is None and direction in ("BUY", "SELL"):
        primary_reason = "PENDING_CONFIRMATION"
        failure_reason = "The next completed candle is not available yet; the breakout remains developing and cannot be confirmed."
    elif direction in ("BUY", "SELL") and not price_accepted:
        reclaimed = confirmation_clearance is not None and confirmation_clearance <= 0
        if reclaimed:
            primary_reason = "FAILED_CLOSE_BACK_THROUGH_LEVEL"
            failure_reason = (
                f"The next candle closed back through the {direction} breakout level: "
                f"close {fmt(confirmation_close)} vs level {fmt(level)}. "
                f"Directional clearance was {fmt(confirmation_clearance)} points."
            )
        else:
            primary_reason = "FAILED_PRICE_ACCEPTANCE_BUFFER"
            failure_reason = (
                f"The next candle did not achieve the required {fmt(acceptance_buffer_points)}-point "
                f"({fmt(acceptance_buffer_atr)} ATR) acceptance beyond level {fmt(level)}. "
                f"Observed directional clearance: {fmt(confirmation_clearance)} points."
            )
    elif not is_genuine and relative_volume is not None and relative_volume < required_volume:
        primary_reason = "FAILED_VOLUME"
        failure_reason = f"Breakout volume was {fmt(relative_volume)}x versus the required {fmt(required_volume)}x same-timeframe prior-20 average."
    elif not is_genuine and not capture_eligible:
        if "INSUFFICIENT CLASSIFIED PRINTS" in capture_reasons:
            primary_reason = "INSUFFICIENT_CLASSIFIED_PRINTS"
        elif "LOW ACTIVE-MINUTE COVERAGE" in capture_reasons:
            primary_reason = "INSUFFICIENT_ACTIVE_MINUTE_COVERAGE"
        elif "APP STARTED AFTER BREAKOUT BAR BEGAN" in capture_reasons:
            primary_reason = "CAPTURE_STARTED_AFTER_EVENT"
        elif "TRADE FEED NOT CONNECTED" in capture_reasons:
            primary_reason = "TRADE_FEED_DISCONNECTED"
        elif "TRADE CHANNEL STALE" in capture_reasons:
            primary_reason = "TRADE_CHANNEL_STALE"
        else:
            primary_reason = "ORDER_FLOW_UNVERIFIED"
        failure_reason = (
            f"Price/volume conditions may be satisfied, but same-direction taker flow cannot be validated: "
            f"{capture_status}. No directional-flow pass is assigned."
        )
    elif not is_genuine and bool(item.get("flow_opposes")):
        primary_reason = "ORDER_FLOW_DIVERGENCE"
        failure_reason = (
            f"Executed taker flow was dominated by the opposing side: "
            f"{direction} {fmt(directional_share, 1)}% vs opposing {fmt(opposing_share, 1)}%. "
            f"Required same-direction share is {fmt(min_taker_share, 1)}%."
        )
    elif not is_genuine and not bool(item.get("flow_confirmed")):
        primary_reason = "INSUFFICIENT_TAKER_DOMINANCE"
        failure_reason = (
            f"Same-direction taker share {fmt(directional_share, 1)}% did not meet the "
            f"{fmt(min_taker_share, 1)}% minimum."
        )
    elif is_genuine:
        primary_reason = "CONFIRMED_BREAKOUT"
        breakout_clearance_atr = trigger_clearance / atr if trigger_clearance is not None and atr else None
        acceptance_clearance_atr = confirmation_clearance / atr if confirmation_clearance is not None and atr else None
        success_reason = (
            f"{direction} breakout confirmed: trigger candle closed {fmt(trigger_clearance)} points "
            f"({fmt(breakout_clearance_atr)} ATR) beyond level {fmt(level)}, exceeding the "
            f"{fmt(breakout_buffer_points)}-point trigger buffer. The next completed candle closed "
            f"{fmt(confirmation_clearance)} points ({fmt(acceptance_clearance_atr)} ATR) beyond the level, "
            f"meeting the {fmt(acceptance_buffer_points)}-point acceptance requirement. "
            f"Same-timeframe candle volume was {fmt(relative_volume)}x the prior-20 average "
            f"(minimum {fmt(required_volume)}x). {direction} taker share was "
            f"{fmt(directional_share, 1)}% (minimum {fmt(min_taker_share, 1)}%), with "
            f"{int(classified_prints or 0):,} classified prints and {fmt(coverage, 1)}% active-minute coverage."
        )
        if retest_status.startswith("HELD"):
            success_reason += f" The subsequent retest also held: {retest_status}."
        failure_reason = "None — all required initial breakout, acceptance, volume and order-flow gates passed."
    else:
        primary_reason = "NO_RECENT_BREAKOUT" if "NO RECENT BREAKOUT" in classification else "INSUFFICIENT_OR_UNCONFIRMED_EVIDENCE"
        failure_reason = str(item.get("reason") or classification)

    # Preserve secondary blockers so the user can see every non-passing gate,
    # not only the primary reason used for the headline classification.
    for check in checks:
        if check["Result"] in ("FAIL", "INSUFFICIENT", "UNVERIFIED") and check["Condition"] not in (
            "Breakout event detection",
            "Trade-feed capture quality",
            "Strong taker dominance (informational)",
        ):
            detail = check["Explanation"]
            if detail and detail not in failure_details and check["Condition"] != "Same-direction taker dominance":
                failure_details.append(detail)
    secondary_failures = " | ".join(dict.fromkeys(failure_details))
    short_summary = success_reason if is_genuine else failure_reason
    return {
        "primary_reason": primary_reason,
        "success_reason": success_reason or "Not confirmed — one or more required conditions are pending, failed, or unverified.",
        "failure_reason": failure_reason,
        "secondary_failures": secondary_failures,
        "short_summary": short_summary,
        "checks": checks,
    }
