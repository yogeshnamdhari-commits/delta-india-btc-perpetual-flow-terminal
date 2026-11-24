"""Premium/discount classification from confirmed swing dealing ranges.

This is a price-location tool only. It uses confirmed swing pivots supplied by
the caller and does not claim to identify institutional orders or predict price.
"""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def build_premium_discount_map(
    swings: Sequence[Mapping[str, Any]] | None,
    current_price: Any,
    *,
    max_pair_bars: int = 150,
) -> dict[str, Any]:
    """Build a 50% equilibrium map from the most recent confirmed high/low pair.

    The caller must pass completed-candle swings that are already confirmed.
    The range is selected from opposite-kind swing pivots no more than
    max_pair_bars apart. If no defensible pair exists, the map is unavailable.
    """
    price = _finite(current_price)
    if price is None or price <= 0:
        raise ValueError("current_price must be finite and positive")
    if (
        not isinstance(max_pair_bars, int)
        or isinstance(max_pair_bars, bool)
        or max_pair_bars < 1
    ):
        raise ValueError("max_pair_bars must be a positive integer")

    pivots: list[dict[str, Any]] = []
    for row in swings or ():
        if not isinstance(row, Mapping):
            continue
        index = row.get("index")
        level = _finite(row.get("price"))
        kind = str(row.get("kind", "")).strip().lower()
        if (
            not isinstance(index, int)
            or isinstance(index, bool)
            or index < 0
            or level is None
            or level <= 0
            or kind not in {"high", "low"}
        ):
            continue
        pivots.append({"index": index, "price": level, "kind": kind})

    highs = [row for row in pivots if row["kind"] == "high"]
    lows = [row for row in pivots if row["kind"] == "low"]
    pairs: list[tuple[tuple[int, int, int], dict[str, Any], dict[str, Any]]] = []
    for high in highs:
        for low in lows:
            distance = abs(high["index"] - low["index"])
            if distance > max_pair_bars or high["price"] <= low["price"]:
                continue
            # Prefer the pair with the newest endpoint; then the newest older
            # endpoint; then the tighter temporal pair for deterministic ties.
            score = (
                max(high["index"], low["index"]),
                min(high["index"], low["index"]),
                -distance,
            )
            pairs.append((score, high, low))

    if not pairs:
        return {
            "status": "UNAVAILABLE — NO CONFIRMED SWING RANGE",
            "location": "UNAVAILABLE",
            "current_price": price,
            "range_high": None,
            "range_low": None,
            "equilibrium": None,
            "range_size": None,
            "position_pct": None,
            "premium_low": None,
            "premium_high": None,
            "discount_low": None,
            "discount_high": None,
        }

    _, high, low = max(pairs, key=lambda item: item[0])
    high_price = float(high["price"])
    low_price = float(low["price"])
    range_size = high_price - low_price
    equilibrium = low_price + range_size * 0.5
    position_pct = (price - low_price) / range_size * 100.0

    if price > high_price:
        location = "ABOVE RANGE"
    elif price < low_price:
        location = "BELOW RANGE"
    elif math.isclose(price, equilibrium, rel_tol=0.0, abs_tol=1e-9):
        location = "EQUILIBRIUM"
    elif price > equilibrium:
        location = "PREMIUM"
    else:
        location = "DISCOUNT"

    return {
        "status": "AVAILABLE",
        "location": location,
        "current_price": price,
        "range_high": high_price,
        "range_high_index": high["index"],
        "range_low": low_price,
        "range_low_index": low["index"],
        "equilibrium": equilibrium,
        "range_size": range_size,
        "position_pct": position_pct,
        "premium_low": equilibrium,
        "premium_high": high_price,
        "discount_low": low_price,
        "discount_high": equilibrium,
        "method": "most recent confirmed opposite-kind swing pair; 50% midpoint equilibrium",
    }


def classify_price_in_dealing_range(price: Any, dealing_range: Mapping[str, Any] | None) -> str:
    """Classify a price relative to a previously built premium/discount map."""
    value = _finite(price)
    if value is None or value <= 0 or not isinstance(dealing_range, Mapping):
        return "UNAVAILABLE"
    if dealing_range.get("status") != "AVAILABLE":
        return "UNAVAILABLE"
    high = _finite(dealing_range.get("range_high"))
    low = _finite(dealing_range.get("range_low"))
    equilibrium = _finite(dealing_range.get("equilibrium"))
    if high is None or low is None or equilibrium is None or high <= low:
        return "UNAVAILABLE"
    if value > high:
        return "ABOVE RANGE"
    if value < low:
        return "BELOW RANGE"
    if math.isclose(value, equilibrium, rel_tol=0.0, abs_tol=1e-9):
        return "EQUILIBRIUM"
    return "PREMIUM" if value > equilibrium else "DISCOUNT"
