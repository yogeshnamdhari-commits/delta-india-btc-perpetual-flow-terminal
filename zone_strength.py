"""Measured supply/demand-zone strength from public market data."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Any


def _finite_nonnegative(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return number


def _empty_flow() -> dict[str, float | int]:
    return {"prints": 0, "contracts": 0.0, "btc": 0.0, "usd": 0.0}


def _book_side_in_zone(
    book: Mapping[Any, Any] | None,
    lower: float,
    upper: float,
    contract_value_btc: float,
) -> dict[str, float | int]:
    result = _empty_flow()
    if not isinstance(book, Mapping):
        return result
    for raw_price, raw_size in book.items():
        price = _finite_nonnegative(raw_price)
        size = _finite_nonnegative(raw_size)
        if price is None or size is None or price <= 0 or size <= 0:
            continue
        if lower <= price <= upper:
            result["prints"] += 1  # Here this field represents visible price levels.
            result["contracts"] += size
            result["btc"] += size * contract_value_btc
            result["usd"] += size * contract_value_btc * price
    return result


def measure_zone_strength(
    zone: Mapping[str, Any],
    candles: list[Mapping[str, Any]] | None,
    trade_log: Iterable[Any] | None,
    bids: Mapping[Any, Any] | None,
    asks: Mapping[Any, Any] | None,
    *,
    book_valid: bool,
    contract_value_btc: float = 0.001,
) -> dict[str, Any]:
    """Measure candle volume, executed tape and visible resting depth in one zone.

    Trade records follow (timestamp, price, contracts, aggressor_side). L2 books
    are aggregated by price, so the returned BID/ASK counts are price-level
    counts, never individual resting-order counts. USD notional is approximate:
    contracts × BTC contract value × price.
    """
    contract_btc = _finite_nonnegative(contract_value_btc)
    if contract_btc is None or contract_btc <= 0:
        raise ValueError("contract_value_btc must be finite and positive")

    lower = _finite_nonnegative(zone.get("bottom"))
    upper = _finite_nonnegative(zone.get("top"))
    if lower is None or upper is None or lower > upper:
        raise ValueError("zone must contain finite ordered bottom/top prices")

    formation_volume = None
    formation_index = zone.get("_mi")
    if isinstance(formation_index, int) and not isinstance(formation_index, bool):
        if candles and 0 <= formation_index < len(candles):
            formation_volume = _finite_nonnegative(candles[formation_index].get("volume"))

    buy = _empty_flow()
    sell = _empty_flow()
    for record in trade_log or ():
        if not isinstance(record, (tuple, list)) or len(record) < 4:
            continue
        _, raw_price, raw_contracts, raw_side = record[:4]
        price = _finite_nonnegative(raw_price)
        contracts = _finite_nonnegative(raw_contracts)
        side = str(raw_side).strip().lower()
        if (
            price is None
            or contracts is None
            or price <= 0
            or contracts <= 0
            or not lower <= price <= upper
            or side not in ("buy", "sell")
        ):
            continue
        flow = buy if side == "buy" else sell
        btc = contracts * contract_btc
        flow["prints"] += 1
        flow["contracts"] += contracts
        flow["btc"] += btc
        flow["usd"] += btc * price

    if book_valid:
        bid = _book_side_in_zone(bids, lower, upper, contract_btc)
        ask = _book_side_in_zone(asks, lower, upper, contract_btc)
        bid_levels = int(bid["prints"])
        ask_levels = int(ask["prints"])
    else:
        bid = ask = None
        bid_levels = ask_levels = None

    is_demand = str(zone.get("direction", "")).lower() == "bullish"
    aligned_side = "BID" if is_demand else "ASK"
    aligned = bid if is_demand else ask
    opposing = ask if is_demand else bid
    aligned_levels = bid_levels if is_demand else ask_levels
    opposing_levels = ask_levels if is_demand else bid_levels

    ratio = None
    if aligned is not None and opposing is not None and float(opposing["usd"]) > 0:
        ratio = float(aligned["usd"]) / float(opposing["usd"])

    return {
        "formation_volume": formation_volume,
        "buy": buy,
        "sell": sell,
        "bid": bid,
        "ask": ask,
        "book_available": bool(book_valid),
        "aligned_side": aligned_side,
        "aligned": aligned,
        "opposing": opposing,
        "aligned_levels": aligned_levels,
        "opposing_levels": opposing_levels,
        "aligned_opposing_usd_ratio": ratio,
        "contract_value_btc": contract_btc,
        "zone_lower": lower,
        "zone_upper": upper,
    }
