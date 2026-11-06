"""UTC-aligned completed-bar summaries of captured Delta public taker trades.

This is monitoring analytics only. It does not modify strategy signals, entry
thresholds, execution, or the historical evaluator. Each bar is aligned to a
fixed UTC boundary and excludes the currently forming interval.
"""

from __future__ import annotations

import math
from bisect import bisect_left
from collections.abc import Mapping, Sequence
from typing import Any


DEFAULT_TIMEFRAMES = (
    ("1m", 60),
    ("5m", 5 * 60),
    ("15m", 15 * 60),
    ("1h", 60 * 60),
    ("4h", 4 * 60 * 60),
    ("1d", 24 * 60 * 60),
)
DEFAULT_RELATIVE_VOLUME_LOOKBACK = 20
DEFAULT_MIN_PRINTS = 10
DEFAULT_DOMINANCE_SHARE_PCT = 60.0
STRONG_DOMINANCE_SHARE_PCT = 70.0
DEFAULT_MAX_TRADE_AGE_SECONDS = 20.0
MINUTE_AGGREGATE_RETENTION_SECONDS = 30 * 24 * 60 * 60


def _number(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _build_minute_index(
    flow_minutes: Mapping[Any, Any],
) -> tuple[list[int], dict[str, list[float]]]:
    """Build one sorted minute index and prefix sums for O(log n) range queries."""
    fields = (
        "buy_prints",
        "sell_prints",
        "buy_contracts",
        "sell_contracts",
        "buy_usd",
        "sell_usd",
        "active_trade_minutes",
    )
    normalized: dict[int, dict[str, float]] = {}

    for raw_minute, raw_flow in flow_minutes.items():
        minute_value = _number(raw_minute)
        if minute_value is None or not isinstance(raw_flow, Mapping):
            continue
        minute = int(minute_value)
        row = normalized.setdefault(minute, {field: 0.0 for field in fields})
        minute_active = False
        for side in ("buy", "sell"):
            count = max(0, int(_number(raw_flow.get(f"{side}_count")) or 0.0))
            contracts = max(0.0, _number(raw_flow.get(f"{side}_contracts")) or 0.0)
            usd = max(0.0, _number(raw_flow.get(f"{side}_usd")) or 0.0)
            row[f"{side}_prints"] += count
            row[f"{side}_contracts"] += contracts
            row[f"{side}_usd"] += usd
            if count > 0 or contracts > 0:
                minute_active = True
        if minute_active:
            row["active_trade_minutes"] = 1.0

    minutes = sorted(normalized)
    prefixes = {field: [0.0] for field in fields}
    for minute in minutes:
        row = normalized[minute]
        for field in fields:
            prefixes[field].append(prefixes[field][-1] + row[field])
    return minutes, prefixes


def _aggregate_period(
    minute_index: tuple[list[int], dict[str, list[float]]],
    *,
    start_ts: int,
    end_ts: int,
) -> dict[str, Any]:
    """Sum minute aggregates in a half-open UTC interval using prefix sums."""
    minutes, prefixes = minute_index
    left = bisect_left(minutes, start_ts)
    right = bisect_left(minutes, end_ts)

    totals: dict[str, Any] = {
        field: prefixes[field][right] - prefixes[field][left]
        for field in prefixes
    }
    for key in ("buy_prints", "sell_prints", "active_trade_minutes"):
        totals[key] = int(round(totals[key]))

    buy_contracts = totals["buy_contracts"]
    sell_contracts = totals["sell_contracts"]
    total_contracts = buy_contracts + sell_contracts
    total_prints = totals["buy_prints"] + totals["sell_prints"]
    buy_share = buy_contracts / total_contracts * 100.0 if total_contracts > 0 else None
    sell_share = sell_contracts / total_contracts * 100.0 if total_contracts > 0 else None
    if buy_contracts > sell_contracts:
        dominant_side = "BUY"
        dominant_share = buy_share
        dominant_contracts, opposing_contracts = buy_contracts, sell_contracts
    elif sell_contracts > buy_contracts:
        dominant_side = "SELL"
        dominant_share = sell_share
        dominant_contracts, opposing_contracts = sell_contracts, buy_contracts
    else:
        dominant_side = None
        dominant_share = None
        dominant_contracts = opposing_contracts = 0.0

    dominance_ratio = (
        dominant_contracts / opposing_contracts
        if opposing_contracts > 0 else None
    )

    return {
        **totals,
        "classified_prints": total_prints,
        "total_contracts": total_contracts,
        "total_btc": None,
        "total_usd": totals["buy_usd"] + totals["sell_usd"],
        "net_delta_contracts": buy_contracts - sell_contracts,
        "buy_share_pct": buy_share,
        "sell_share_pct": sell_share,
        "dominant_side": dominant_side,
        "dominant_share_pct": dominant_share,
        "dominance_ratio": dominance_ratio,
    }


def _capture_status(
    *,
    period_start: int,
    feed_started_at: float | None,
    connection_status: str,
    trade_channel_last_seen: float | None,
    now_ts: float,
    max_trade_age_seconds: float,
) -> tuple[str, bool, float | None]:
    if feed_started_at is None or feed_started_at <= 0:
        return "CAPTURE START UNKNOWN", False, None
    if feed_started_at > period_start:
        return "PARTIAL — APP STARTED MID-WINDOW", False, None
    if connection_status.strip().lower() != "connected":
        return "FEED NOT CONNECTED", False, None
    if trade_channel_last_seen is None or trade_channel_last_seen <= 0:
        return "TRADE CHANNEL NOT OBSERVED", False, None

    trade_age = now_ts - trade_channel_last_seen
    if trade_age < 0 or trade_age > max_trade_age_seconds:
        return "TRADE CHANNEL STALE", False, max(0.0, trade_age)

    # Process uptime and latest channel activity are observable. Delta's public
    # trade channel has no per-trade sequence number, so this cannot prove that
    # every message throughout the interval was delivered without a gap.
    return "PROCESS WINDOW COVERED · FEED FRESH", True, trade_age


def summarize_aligned_taker_flow(
    flow_minutes: Mapping[Any, Any] | None,
    *,
    now_ts: float,
    feed_started_at: float | None,
    connection_status: str,
    trade_channel_last_seen: float | None,
    contract_value_btc: float = 0.001,
    timeframes: Sequence[tuple[str, int]] = DEFAULT_TIMEFRAMES,
    min_prints: int = DEFAULT_MIN_PRINTS,
    dominance_share_pct: float = DEFAULT_DOMINANCE_SHARE_PCT,
    relative_volume_lookback: int = DEFAULT_RELATIVE_VOLUME_LOOKBACK,
    max_trade_age_seconds: float = DEFAULT_MAX_TRADE_AGE_SECONDS,
    retention_seconds: int = MINUTE_AGGREGATE_RETENTION_SECONDS,
) -> dict[str, Any]:
    """Summarize the last completed UTC-aligned bar for each requested timeframe.

    The in-progress bar is excluded. Relative volume is returned only when the
    current bar and all requested prior same-duration bars fall within the
    process-captured and retained history. No missing history is backfilled.
    """
    now = _number(now_ts)
    contract_btc = _number(contract_value_btc)
    started = _number(feed_started_at)
    last_seen = _number(trade_channel_last_seen)
    max_age = _number(max_trade_age_seconds)
    dominance = _number(dominance_share_pct)

    if now is None or now <= 0:
        raise ValueError("now_ts must be finite and positive")
    if contract_btc is None or contract_btc <= 0:
        raise ValueError("contract_value_btc must be finite and positive")
    if not isinstance(min_prints, int) or isinstance(min_prints, bool) or min_prints < 1:
        raise ValueError("min_prints must be a positive integer")
    if dominance is None or not 50.0 < dominance <= 100.0:
        raise ValueError("dominance_share_pct must be >50 and <=100")
    if not isinstance(relative_volume_lookback, int) or isinstance(relative_volume_lookback, bool) or relative_volume_lookback < 1:
        raise ValueError("relative_volume_lookback must be a positive integer")
    if max_age is None or max_age <= 0:
        raise ValueError("max_trade_age_seconds must be finite and positive")
    if not isinstance(retention_seconds, int) or retention_seconds < 60:
        raise ValueError("retention_seconds must be an integer of at least 60")

    clean_timeframes = tuple(timeframes)
    if not clean_timeframes:
        raise ValueError("timeframes must not be empty")
    if any(
        not isinstance(item, (tuple, list))
        or len(item) != 2
        or not isinstance(item[0], str)
        or not item[0].strip()
        or not isinstance(item[1], int)
        or isinstance(item[1], bool)
        or item[1] < 60
        or item[1] % 60 != 0
        for item in clean_timeframes
    ):
        raise ValueError("each timeframe must be a non-empty name and positive whole-minute duration")
    if len({item[0] for item in clean_timeframes}) != len(clean_timeframes):
        raise ValueError("timeframe names must be unique")

    minute_map = flow_minutes if isinstance(flow_minutes, Mapping) else {}
    minute_index = _build_minute_index(minute_map)
    rows: list[dict[str, Any]] = []

    for name, duration in clean_timeframes:
        # Fixed UTC epoch boundaries: 4h bars begin at 00:00, 04:00, 08:00,
        # etc.; daily bars begin at 00:00 UTC. The current partial bar is omitted.
        period_end = int(now // duration) * duration
        period_start = period_end - duration
        current = _aggregate_period(
            minute_index, start_ts=period_start, end_ts=period_end
        )
        current["buy_btc"] = current["buy_contracts"] * contract_btc
        current["sell_btc"] = current["sell_contracts"] * contract_btc
        current["total_btc"] = current["total_contracts"] * contract_btc

        capture_text, capture_ok, trade_age = _capture_status(
            period_start=period_start,
            feed_started_at=started,
            connection_status=str(connection_status or ""),
            trade_channel_last_seen=last_seen,
            now_ts=now,
            max_trade_age_seconds=max_age,
        )

        if not capture_ok:
            classification = "UNVERIFIED CAPTURE"
            intensity = "UNAVAILABLE"
        elif current["classified_prints"] == 0:
            classification = "NO CLASSIFIED PRINTS"
            intensity = "UNAVAILABLE"
        elif current["classified_prints"] < min_prints:
            classification = "INSUFFICIENT SAMPLE"
            intensity = "UNAVAILABLE"
        elif current["dominant_side"] is None or current["dominant_share_pct"] is None:
            classification = "BALANCED"
            intensity = "BALANCED"
        elif current["dominant_share_pct"] < dominance:
            classification = "BALANCED"
            intensity = "BALANCED"
        elif current["dominant_side"] == "BUY":
            classification = "BUYER DOMINANCE"
            intensity = (
                "STRONG"
                if current["dominant_share_pct"] >= STRONG_DOMINANCE_SHARE_PCT
                else "MODERATE"
            )
        else:
            classification = "SELLER DOMINANCE"
            intensity = (
                "STRONG"
                if current["dominant_share_pct"] >= STRONG_DOMINANCE_SHARE_PCT
                else "MODERATE"
            )

        # Determine how many *whole* prior same-duration bars are both inside
        # app uptime and the bounded retention interval.
        earliest_available = now - retention_seconds
        if started is not None and started > 0:
            earliest_available = max(earliest_available, started)
            available_prior = min(
                relative_volume_lookback,
                max(0, int((period_start - earliest_available) // duration)),
            )
        else:
            available_prior = 0

        relative_volume = None
        average_prior_contracts = None
        if capture_ok and available_prior >= relative_volume_lookback:
            prior_totals = []
            for index in range(1, relative_volume_lookback + 1):
                prior_end = period_start - (index - 1) * duration
                prior_start = prior_end - duration
                prior = _aggregate_period(
                    minute_index, start_ts=prior_start, end_ts=prior_end
                )
                prior_totals.append(prior["total_contracts"])
            average_prior_contracts = sum(prior_totals) / relative_volume_lookback
            if average_prior_contracts > 0:
                relative_volume = current["total_contracts"] / average_prior_contracts

        if not capture_ok:
            relative_volume_status = "UNAVAILABLE — CAPTURE NOT VERIFIED"
        elif available_prior < relative_volume_lookback:
            relative_volume_status = (
                f"NEEDS {relative_volume_lookback} PRIOR BARS "
                f"({available_prior}/{relative_volume_lookback} CAPTURED)"
            )
        elif average_prior_contracts is None:
            relative_volume_status = "UNAVAILABLE — BASELINE NOT CALCULABLE"
        elif average_prior_contracts <= 0:
            relative_volume_status = "UNAVAILABLE — ZERO BASELINE"
        else:
            relative_volume_status = f"VS PRIOR {relative_volume_lookback} COMPLETED BARS"

        rows.append({
            "timeframe": name,
            "duration_seconds": duration,
            "period_start": period_start,
            "period_end": period_end,
            **current,
            "classification": classification,
            "intensity": intensity,
            "capture_status": capture_text,
            "capture_window_eligible": capture_ok,
            "latest_trade_age_seconds": trade_age,
            "active_trade_minutes": current["active_trade_minutes"],
            "relative_volume": relative_volume,
            "average_prior_contracts": average_prior_contracts,
            "relative_volume_sample_count": available_prior,
            "relative_volume_status": relative_volume_status,
        })

    return {
        "windows": rows,
        "alignment": "fixed completed UTC bars; current in-progress bar excluded",
        "relative_volume_lookback": relative_volume_lookback,
        "min_prints": min_prints,
        "dominance_threshold_pct": dominance,
        "strong_dominance_threshold_pct": STRONG_DOMINANCE_SHARE_PCT,
        "capture_basis": (
            "minute aggregates from this app process; no historical backfill; "
            "public trade messages do not expose a per-trade sequence number"
        ),
        "limitations": (
            "A process window covered and a fresh trade channel do not prove uninterrupted delivery "
            "of every public trade message. Relative volume is calculated only from captured minute "
            "aggregates and is not the exchange's official candle volume. These summaries do not "
            "detect a price breakout or create a strategy entry signal."
        ),
    }
