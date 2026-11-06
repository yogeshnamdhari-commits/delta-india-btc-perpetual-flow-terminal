"""Empirical reaction statistics for price-action-derived liquidity levels.

This module does not estimate market-wide liquidation prices. It evaluates
confirmed swing levels from completed candles and reports historical retest
outcomes, with the sample size and measurement rules kept explicit.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _atr_at(candles: list[Mapping[str, Any]], end_index: int, period: int = 14) -> float | None:
    """Simple mean true range using only candles available at end_index."""
    if end_index < 1:
        return None
    start = max(1, end_index - period + 1)
    true_ranges: list[float] = []
    for index in range(start, end_index + 1):
        high = _finite(candles[index].get("high"))
        low = _finite(candles[index].get("low"))
        previous_close = _finite(candles[index - 1].get("close"))
        if high is None or low is None or previous_close is None or high < low:
            continue
        true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    if len(true_ranges) < min(5, period):
        return None
    value = sum(true_ranges) / len(true_ranges)
    return value if math.isfinite(value) and value > 0 else None


def _is_touch(candle: Mapping[str, Any], kind: str, level: float, tolerance: float) -> bool:
    high = _finite(candle.get("high"))
    low = _finite(candle.get("low"))
    if high is None or low is None:
        return False
    return high >= level - tolerance if kind == "high" else low <= level + tolerance


def _has_sweep(
    candles: list[Mapping[str, Any]],
    touch_index: int,
    kind: str,
    level: float,
    eps: float,
    reclaim_bars: int,
) -> bool:
    """True only when a level is breached and closes back through it promptly."""
    breach_index = None
    scan_end = min(len(candles), touch_index + reclaim_bars)
    for index in range(touch_index, scan_end):
        high = _finite(candles[index].get("high"))
        low = _finite(candles[index].get("low"))
        breached = (
            high is not None and high >= level + eps
            if kind == "high"
            else low is not None and low <= level - eps
        )
        if breached:
            breach_index = index
            break
    if breach_index is None:
        return False
    reclaim_end = min(len(candles), breach_index + reclaim_bars)
    for index in range(breach_index, reclaim_end):
        close = _finite(candles[index].get("close"))
        if close is None:
            continue
        if (kind == "high" and close < level) or (kind == "low" and close > level):
            return True
    return False


def _reaction_outcome(
    candles: list[Mapping[str, Any]],
    touch_index: int,
    kind: str,
    level: float,
    atr_value: float,
    horizon_bars: int,
    reaction_atr: float,
    breakout_atr: float,
) -> tuple[str, int]:
    """Classify the first completed reaction after a touch.

    A rejection wins when price moves 0.5 ATR away from the level before
    acceptance. Acceptance requires two consecutive closes at least 0.25 ATR
    beyond the level. If the same candle spans both conditions, OHLC data
    cannot establish their order, so the outcome is marked ambiguous.
    """
    end_index = touch_index + horizon_bars
    if end_index >= len(candles):
        return "PENDING", len(candles) - 1

    target = reaction_atr * atr_value
    acceptance_buffer = breakout_atr * atr_value
    previous_accepted = False

    for index in range(touch_index + 1, end_index + 1):
        candle = candles[index]
        high = _finite(candle.get("high"))
        low = _finite(candle.get("low"))
        close = _finite(candle.get("close"))
        if high is None or low is None or close is None:
            previous_accepted = False
            continue

        if kind == "high":
            rejection_hit = low <= level - target
            accepted_now = close >= level + acceptance_buffer
        else:
            rejection_hit = high >= level + target
            accepted_now = close <= level - acceptance_buffer

        breakout_hit = previous_accepted and accepted_now
        if rejection_hit and (accepted_now or breakout_hit):
            return "AMBIGUOUS", index
        if breakout_hit:
            return "BREAK_ACCEPTED", index
        if rejection_hit:
            return "REJECTION", index
        previous_accepted = accepted_now

    return "NO_CLEAR_MOVE", end_index


def evaluate_level_retests(
    candles: list[Mapping[str, Any]],
    *,
    level: float,
    kind: str,
    confirm_index: int,
    tolerance_points: float = 10.0,
    horizon_bars: int = 10,
    reaction_atr: float = 0.5,
    breakout_atr: float = 0.25,
    reclaim_bars: int = 3,
    sweep_epsilon: float = 0.5,
) -> dict[str, Any]:
    """Evaluate retests after a level is confirmed; no pre-confirmation bars count."""
    price = _finite(level)
    tolerance = _finite(tolerance_points)
    if kind not in ("high", "low"):
        raise ValueError("kind must be 'high' or 'low'")
    if price is None or price <= 0:
        raise ValueError("level must be finite and positive")
    if tolerance is None or tolerance < 0:
        raise ValueError("tolerance_points must be finite and non-negative")
    if not isinstance(confirm_index, int) or isinstance(confirm_index, bool):
        raise ValueError("confirm_index must be an integer")
    if confirm_index < 1 or confirm_index >= len(candles):
        raise ValueError("confirm_index must identify an existing candle after index zero")
    if horizon_bars < 1 or reclaim_bars < 1:
        raise ValueError("horizon_bars and reclaim_bars must be positive")
    atr_value = _atr_at(candles, confirm_index)
    if atr_value is None:
        return {
            "attempts": 0, "sweeps": 0, "successes": 0, "failures": 0,
            "neutral": 0, "pending": 0, "effectiveness_pct": None,
            "resolved_samples": 0, "last_outcome": "INSUFFICIENT ATR HISTORY",
            "atr_at_confirmation": None,
        }

    attempts = sweeps = successes = failures = neutral = pending = 0
    outcomes: list[str] = []
    start_index = confirm_index + 1
    previous_close = _finite(candles[confirm_index].get("close"))
    armed = (
        previous_close is not None and (
            previous_close < price - tolerance if kind == "high"
            else previous_close > price + tolerance
        )
    )
    index = start_index

    while index < len(candles):
        candle = candles[index]
        close = _finite(candle.get("close"))
        if not armed:
            if close is not None and (
                close < price - tolerance if kind == "high"
                else close > price + tolerance
            ):
                armed = True
        if not armed or not _is_touch(candle, kind, price, tolerance):
            index += 1
            continue

        attempts += 1
        was_swept = _has_sweep(
            candles, index, kind, price, sweep_epsilon, reclaim_bars
        )
        if was_swept:
            sweeps += 1
        outcome, outcome_index = _reaction_outcome(
            candles, index, kind, price, atr_value, horizon_bars,
            reaction_atr, breakout_atr,
        )
        outcomes.append(outcome)
        if outcome == "REJECTION":
            successes += 1
        elif outcome == "BREAK_ACCEPTED":
            failures += 1
        elif outcome == "PENDING":
            pending += 1
            break
        else:
            neutral += 1

        # Do not count overlapping retests within the same forward outcome window.
        index = outcome_index + 1
        end_close = _finite(candles[outcome_index].get("close"))
        armed = end_close is not None and (
            end_close < price - tolerance if kind == "high"
            else end_close > price + tolerance
        )

    resolved = successes + failures
    effectiveness = (successes / resolved * 100.0) if resolved else None
    return {
        "attempts": attempts,
        "sweeps": sweeps,
        "successes": successes,
        "failures": failures,
        "neutral": neutral,
        "pending": pending,
        "effectiveness_pct": effectiveness,
        "resolved_samples": resolved,
        "last_outcome": outcomes[-1] if outcomes else "UNTESTED",
        "atr_at_confirmation": atr_value,
    }


def _confirmed_pivots(
    candles: list[Mapping[str, Any]], left: int, right: int
) -> list[dict[str, Any]]:
    pivots: list[dict[str, Any]] = []
    for index in range(left, len(candles) - right):
        high = _finite(candles[index].get("high"))
        low = _finite(candles[index].get("low"))
        if high is None or low is None:
            continue
        window = candles[index - left:index + right + 1]
        highs = [_finite(item.get("high")) for item in window]
        lows = [_finite(item.get("low")) for item in window]
        valid_highs = [value for value in highs if value is not None]
        valid_lows = [value for value in lows if value is not None]
        if len(valid_highs) != len(window) or len(valid_lows) != len(window):
            continue
        if high == max(valid_highs):
            pivots.append({
                "kind": "high", "price": high, "index": index,
                "confirm_index": index + right, "time": candles[index].get("time"),
            })
        if low == min(valid_lows):
            pivots.append({
                "kind": "low", "price": low, "index": index,
                "confirm_index": index + right, "time": candles[index].get("time"),
            })
    return pivots


def _display_candidates(
    pivots: list[dict[str, Any]], tolerance: float
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for kind in ("high", "low"):
        side_points = sorted(
            (point for point in pivots if point["kind"] == kind),
            key=lambda point: (point["price"], point["index"]),
        )
        grouped_indexes: set[int] = set()
        cursor = 0
        while cursor < len(side_points):
            first_price = side_points[cursor]["price"]
            group = [side_points[cursor]]
            next_index = cursor + 1
            while (
                next_index < len(side_points)
                and side_points[next_index]["price"] - first_price <= tolerance
            ):
                group.append(side_points[next_index])
                next_index += 1
            if len(group) >= 2:
                grouped_indexes.update(point["index"] for point in group)
                candidates.append({
                    "kind": kind,
                    "level_type": "EQUAL_HIGHS" if kind == "high" else "EQUAL_LOWS",
                    "price": sum(point["price"] for point in group) / len(group),
                    "confirm_index": max(point["confirm_index"] for point in group),
                    "pivot_count": len(group),
                    "formation_time": max(
                        (point["time"] for point in group if point["time"] is not None),
                        default=None,
                    ),
                })
                cursor = next_index
            else:
                cursor += 1

        for point in side_points:
            if point["index"] in grouped_indexes:
                continue
            candidates.append({
                "kind": kind,
                "level_type": "SWING_HIGH" if kind == "high" else "SWING_LOW",
                "price": point["price"],
                "confirm_index": point["confirm_index"],
                "pivot_count": 1,
                "formation_time": point["time"],
            })
    return candidates


def build_liquidity_effectiveness(
    candles: list[Mapping[str, Any]] | None,
    current_price: float | None,
    *,
    tolerance_points: float = 10.0,
    left_bars: int = 5,
    right_bars: int = 5,
    horizon_bars: int = 10,
    reaction_atr: float = 0.5,
    breakout_atr: float = 0.25,
    min_effectiveness_samples: int = 5,
    limit_per_side: int = 8,
) -> dict[str, Any]:
    """Build nearby buy-side/sell-side levels and empirical same-side baselines.

    Effectiveness is pooled from confirmed swing-pivot retests on the same side
    (highs or lows) in the supplied completed-candle history. It is not an
    individual-level probability. Only decisive rejection/breakout outcomes
    enter its denominator; ambiguous, neutral and unfinished outcomes remain
    separately visible in the sample accounting.
    """
    rows = list(candles or [])
    price = _finite(current_price)
    tolerance = _finite(tolerance_points)
    if tolerance is None or tolerance < 0:
        raise ValueError("tolerance_points must be finite and non-negative")
    if left_bars < 1 or right_bars < 1 or horizon_bars < 1:
        raise ValueError("pivot and outcome windows must be positive")
    if min_effectiveness_samples < 1 or limit_per_side < 1:
        raise ValueError("sample and display limits must be positive")
    if len(rows) < left_bars + right_bars + 20:
        return {
            "levels": [], "history_bars": len(rows), "effectiveness_min_samples": min_effectiveness_samples,
            "buy_side": {"effectiveness_pct": None, "resolved_samples": 0, "successes": 0, "failures": 0},
            "sell_side": {"effectiveness_pct": None, "resolved_samples": 0, "successes": 0, "failures": 0},
        }

    pivots = _confirmed_pivots(rows, left_bars, right_bars)
    if price is None:
        price = _finite(rows[-1].get("close"))
    side_totals = {
        "high": {"successes": 0, "failures": 0, "neutral": 0, "pending": 0, "sweeps": 0},
        "low": {"successes": 0, "failures": 0, "neutral": 0, "pending": 0, "sweeps": 0},
    }

    # Side-wide baseline uses every historical confirmed pivot of that side,
    # rather than presenting a sparse per-level count as a reliable percentage.
    for pivot in pivots:
        stats = evaluate_level_retests(
            rows, level=pivot["price"], kind=pivot["kind"],
            confirm_index=pivot["confirm_index"], tolerance_points=tolerance,
            horizon_bars=horizon_bars, reaction_atr=reaction_atr,
            breakout_atr=breakout_atr,
        )
        total = side_totals[pivot["kind"]]
        for key in ("successes", "failures", "neutral", "pending", "sweeps"):
            total[key] += stats[key]

    side_baselines: dict[str, dict[str, Any]] = {}
    for kind, total in side_totals.items():
        resolved = total["successes"] + total["failures"]
        side_baselines[kind] = {
            "effectiveness_pct": (
                total["successes"] / resolved * 100.0
                if resolved >= min_effectiveness_samples else None
            ),
            "resolved_samples": resolved,
            "successes": total["successes"],
            "failures": total["failures"],
            "neutral": total["neutral"],
            "pending": total["pending"],
            "sweeps": total["sweeps"],
        }

    candidates = _display_candidates(pivots, tolerance)
    evaluated: list[dict[str, Any]] = []
    for candidate in candidates:
        stats = evaluate_level_retests(
            rows, level=candidate["price"], kind=candidate["kind"],
            confirm_index=candidate["confirm_index"], tolerance_points=tolerance,
            horizon_bars=horizon_bars, reaction_atr=reaction_atr,
            breakout_atr=breakout_atr,
        )
        distance = abs(candidate["price"] - price) if price is not None else None
        age = max(0, len(rows) - 1 - candidate["confirm_index"])
        kind = candidate["kind"]
        total = side_baselines[kind]
        outcome = stats["last_outcome"]
        status = {
            "UNTESTED": "UNTESTED",
            "PENDING": "PENDING",
            "REJECTION": "REJECTED",
            "BREAK_ACCEPTED": "ACCEPTED THROUGH",
            "NO_CLEAR_MOVE": "NO CLEAR REACTION",
            "AMBIGUOUS": "AMBIGUOUS",
            "INSUFFICIENT ATR HISTORY": "INSUFFICIENT HISTORY",
        }.get(outcome, outcome)
        evaluated.append({
            "Side": "BUY-SIDE (above highs)" if kind == "high" else "SELL-SIDE (below lows)",
            "Level type": candidate["level_type"],
            "Price": candidate["price"],
            "Distance (points)": distance,
            "Distance (%)": distance / price * 100.0 if distance is not None and price else None,
            "Pivot touches": candidate["pivot_count"],
            "Retest attempts": stats["attempts"],
            "Sweeps / reclaims": stats["sweeps"],
            "Last reaction": outcome,
            "Status": status,
            "Age (bars)": age,
            "Side effectiveness (%)": total["effectiveness_pct"],
            "Side resolved N": total["resolved_samples"],
            "_kind": kind,
        })

    output: list[dict[str, Any]] = []
    for kind in ("high", "low"):
        side_candidates = [item for item in evaluated if item["_kind"] == kind]
        if price is not None:
            expected_side = (
                [item for item in side_candidates if item["Price"] >= price]
                if kind == "high"
                else [item for item in side_candidates if item["Price"] <= price]
            )
            other_side = [item for item in side_candidates if item not in expected_side]
            expected_side.sort(key=lambda item: (item["Distance (points)"] or 0.0, item["Age (bars)"]))
            other_side.sort(key=lambda item: (item["Distance (points)"] or 0.0, item["Age (bars)"]))
            side_candidates = (expected_side + other_side)[:limit_per_side]
        else:
            side_candidates.sort(key=lambda item: item["Age (bars)"])
            side_candidates = side_candidates[-limit_per_side:]
        for item in side_candidates:
            item.pop("_kind", None)
            output.append(item)

    return {
        "levels": output,
        "history_bars": len(rows),
        "effectiveness_min_samples": min_effectiveness_samples,
        "horizon_bars": horizon_bars,
        "reaction_atr": reaction_atr,
        "breakout_atr": breakout_atr,
        "buy_side": side_baselines["high"],
        "sell_side": side_baselines["low"],
    }
