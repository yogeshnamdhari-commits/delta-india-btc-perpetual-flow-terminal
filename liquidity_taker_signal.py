"""Read-only taker-side liquidity consumption signals from captured trade prints.

A BUY taker executes against resting ASK liquidity. A SELL taker executes
against resting BID liquidity. This module measures classified executed volume;
it does not claim to reconstruct every order-book cancellation or individual
resting order.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from typing import Any


DEFAULT_WINDOWS = (60, 300, 900)
DEFAULT_MIN_PRINTS = 10
DEFAULT_DOMINANCE_SHARE = 60.0
STRONG_DOMINANCE_SHARE = 70.0


def _finite_positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def _normalize_record(record: Any) -> tuple[float, float, float, str] | None:
    """Normalize app trade_log shape: (exchange_ts_seconds, price, contracts, side)."""
    if not isinstance(record, (tuple, list)) or len(record) < 4:
        return None
    timestamp = _finite_positive(record[0])
    price = _finite_positive(record[1])
    contracts = _finite_positive(record[2])
    side = str(record[3]).strip().lower()
    if timestamp is None or price is None or contracts is None or side not in ("buy", "sell"):
        return None
    return timestamp, price, contracts, side


def _window_metrics(
    normalized: Sequence[tuple[float, float, float, str]],
    *,
    now_ts: float,
    window_seconds: int,
    contract_value_btc: float,
    min_prints: int,
    dominance_share_pct: float,
) -> dict[str, Any]:
    cutoff = now_ts - window_seconds
    buy_count = sell_count = 0
    buy_contracts = sell_contracts = 0.0
    buy_usd = sell_usd = 0.0
    latest_trade_ts: float | None = None

    for timestamp, price, contracts, side in normalized:
        if timestamp < cutoff or timestamp > now_ts:
            continue
        latest_trade_ts = timestamp if latest_trade_ts is None else max(latest_trade_ts, timestamp)
        if side == "buy":
            buy_count += 1
            buy_contracts += contracts
            buy_usd += contracts * contract_value_btc * price
        else:
            sell_count += 1
            sell_contracts += contracts
            sell_usd += contracts * contract_value_btc * price

    total_count = buy_count + sell_count
    total_contracts = buy_contracts + sell_contracts
    total_usd = buy_usd + sell_usd
    buy_share = buy_contracts / total_contracts * 100.0 if total_contracts > 0 else None
    sell_share = sell_contracts / total_contracts * 100.0 if total_contracts > 0 else None
    if buy_contracts > sell_contracts:
        dominant_side = "BUY"
        dominant_contracts, opposing_contracts = buy_contracts, sell_contracts
        dominant_share = buy_share
    elif sell_contracts > buy_contracts:
        dominant_side = "SELL"
        dominant_contracts, opposing_contracts = sell_contracts, buy_contracts
        dominant_share = sell_share
    else:
        dominant_side = None
        dominant_contracts = opposing_contracts = 0.0
        dominant_share = None

    dominance_ratio = (
        dominant_contracts / opposing_contracts
        if opposing_contracts > 0 else None
    )

    if total_count == 0:
        signal_code = "NO_RECENT_PRINTS"
        signal = "NO RECENT CLASSIFIED TRADES"
        intensity = "UNAVAILABLE"
    elif total_count < min_prints:
        signal_code = "INSUFFICIENT_SAMPLE"
        signal = "INSUFFICIENT TRADE SAMPLE"
        intensity = "UNAVAILABLE"
    elif dominant_side is None or dominant_share is None or dominant_share < dominance_share_pct:
        signal_code = "BALANCED"
        signal = "BALANCED — NO CLEAR TAKING SIDE"
        intensity = "BALANCED"
    elif dominant_side == "BUY":
        signal_code = "BUYERS_TAKING_ASK"
        signal = "BUYERS TAKING ASK LIQUIDITY"
        intensity = "STRONG" if dominant_share >= STRONG_DOMINANCE_SHARE else "MODERATE"
    else:
        signal_code = "SELLERS_TAKING_BID"
        signal = "SELLERS TAKING BID LIQUIDITY"
        intensity = "STRONG" if dominant_share >= STRONG_DOMINANCE_SHARE else "MODERATE"

    return {
        "window_seconds": window_seconds,
        "window": f"{window_seconds // 60}m",
        "signal_code": signal_code,
        "signal": signal,
        "intensity": intensity,
        "dominant_side": dominant_side,
        "dominant_share_pct": dominant_share,
        "dominance_ratio": dominance_ratio,
        "buy_prints": buy_count,
        "sell_prints": sell_count,
        "classified_prints": total_count,
        "buy_contracts": buy_contracts,
        "sell_contracts": sell_contracts,
        "buy_share_pct": buy_share,
        "sell_share_pct": sell_share,
        "buy_btc": buy_contracts * contract_value_btc,
        "sell_btc": sell_contracts * contract_value_btc,
        "buy_usd": buy_usd,
        "sell_usd": sell_usd,
        "total_contracts": total_contracts,
        "total_usd": total_usd,
        "net_delta_contracts": buy_contracts - sell_contracts,
        "latest_trade_age_seconds": (
            max(0.0, now_ts - latest_trade_ts) if latest_trade_ts is not None else None
        ),
        "minimum_prints": min_prints,
        "dominance_threshold_pct": dominance_share_pct,
    }


def calculate_liquidity_taker_signal(
    trade_log: Iterable[Any] | None,
    *,
    now_ts: float,
    contract_value_btc: float = 0.001,
    windows: Sequence[int] = DEFAULT_WINDOWS,
    min_prints: int = DEFAULT_MIN_PRINTS,
    dominance_share_pct: float = DEFAULT_DOMINANCE_SHARE,
) -> dict[str, Any]:
    """Calculate current buy-/sell-taker direction and volume intensity.

    The main signal uses the 5-minute window. One- and fifteen-minute windows
    provide shorter-term and broader context. A directional signal requires
    at least min_prints classified public trade prints and at least
    dominance_share_pct of captured contract volume on one aggressor side.

    BUY aggressor means buyers are taking ASK liquidity. SELL aggressor means
    sellers are taking BID liquidity. It reports executed taker volume rather
    than asserting that every displayed L2 order-book reduction was a fill.
    """
    try:
        current_time = float(now_ts)
        contract_btc = float(contract_value_btc)
        dominance_threshold = float(dominance_share_pct)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("time, contract value and dominance threshold must be numeric") from exc
    if not math.isfinite(current_time) or current_time <= 0:
        raise ValueError("now_ts must be a finite positive timestamp")
    if not math.isfinite(contract_btc) or contract_btc <= 0:
        raise ValueError("contract_value_btc must be finite and positive")
    if not isinstance(min_prints, int) or isinstance(min_prints, bool) or min_prints < 1:
        raise ValueError("min_prints must be a positive integer")
    if not math.isfinite(dominance_threshold) or not 50.0 < dominance_threshold <= 100.0:
        raise ValueError("dominance_share_pct must be >50 and <=100")

    normalized: list[tuple[float, float, float, str]] = []
    for record in trade_log or ():
        parsed = _normalize_record(record)
        if parsed is not None:
            normalized.append(parsed)

    clean_windows = tuple(dict.fromkeys(windows))
    if not clean_windows or any(
        not isinstance(window, int) or isinstance(window, bool) or window < 1
        for window in clean_windows
    ):
        raise ValueError("windows must contain positive integer seconds")
    window_results = [
        _window_metrics(
            normalized,
            now_ts=current_time,
            window_seconds=window,
            contract_value_btc=contract_btc,
            min_prints=min_prints,
            dominance_share_pct=dominance_threshold,
        )
        for window in sorted(clean_windows)
    ]
    by_seconds = {item["window_seconds"]: item for item in window_results}
    primary = by_seconds.get(300) or window_results[0]
    short = by_seconds.get(60)
    broad = by_seconds.get(900)

    context_signals = [
        item["signal_code"]
        for item in (short, broad)
        if item is not None and item["signal_code"] in (
            "BUYERS_TAKING_ASK", "SELLERS_TAKING_BID"
        )
    ]
    alignment = (
        "ALIGNED"
        if context_signals and all(signal == primary["signal_code"] for signal in context_signals)
        and primary["signal_code"] in ("BUYERS_TAKING_ASK", "SELLERS_TAKING_BID")
        else "MIXED"
        if context_signals and primary["signal_code"] in ("BUYERS_TAKING_ASK", "SELLERS_TAKING_BID")
        else "NO CONFIRMATION"
    )

    return {
        "primary": primary,
        "windows": window_results,
        "timeframe_alignment": alignment,
        "capture_basis": "currently retained in-memory classified public trade prints",
        "classification": {
            "BUY": "buyer was taker; executed against resting ASK liquidity",
            "SELL": "seller was taker; executed against resting BID liquidity",
        },
        "limitations": (
            "Public trade prints count executions, not unique parent orders. "
            "Metrics cover the retained in-memory tape (up to 20,000 prints) and reset on process restart. "
            "Trade aggression does not reconstruct every order-book cancellation or prove that all visible "
            "depth reductions were executions."
        ),
    }
