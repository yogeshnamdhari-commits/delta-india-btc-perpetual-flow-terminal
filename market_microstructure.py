"""Conservative daily-flow and order-book microstructure analytics.

Public aggregated L2 data has no participant/order IDs, so quote-pull and
iceberg-style labels are candidates, never proof of intent or hidden orders.
Daily aggressor totals are process-captured only: the public trade channel
provides no historical backfill or per-trade sequence number.
"""
from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from collections.abc import Mapping, Sequence
import math
from typing import Any


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None




def best_visible_quote_levels(
    bids: Mapping[Any, Any] | None,
    asks: Mapping[Any, Any] | None,
) -> dict[str, dict[str, float] | None]:
    """Return the highest positive-size BID and lowest positive-size ASK.

    Each result contains the displayed price and the contracts resting at that
    exact price level. Invalid, non-finite, non-positive prices or sizes are
    ignored. Callers must still gate display on their book freshness/integrity
    checks; this helper does not certify a live order book.
    """
    def pick(levels: Mapping[Any, Any] | None, *, highest: bool) -> dict[str, float] | None:
        if not isinstance(levels, Mapping):
            return None
        valid: list[tuple[float, float]] = []
        for raw_price, raw_size in levels.items():
            price = _finite(raw_price)
            size = _finite(raw_size)
            if price is None or price <= 0 or size is None or size <= 0:
                continue
            valid.append((price, size))
        if not valid:
            return None
        price, size = (max if highest else min)(valid, key=lambda item: item[0])
        return {"price": price, "size_contracts": size}

    return {
        "best_bid": pick(bids, highest=True),
        "best_ask": pick(asks, highest=False),
    }

def _epoch(value: Any) -> int | None:
    number = _finite(value)
    if number is None or number <= 0:
        return None
    if number > 10_000_000_000:
        number /= 1_000_000
    return int(number)


def summarize_daily_utc_flow(
    flow_minutes: Mapping[Any, Any] | None,
    *,
    now_ts: float,
    feed_started_at: float | None,
    trade_last_seen: float | None,
    connection_status: str,
    contract_value_btc: float = 0.001,
    days: int = 2,
    trade_stale_seconds: float = 20.0,
) -> dict[str, Any]:
    """Return current and previous UTC-day captured taker totals.

    app_window_coverage_pct measures only how much of the elapsed UTC-day
    window falls after this process started. The first minute bucket is rounded
    down to its UTC minute boundary so trades actually captured after startup
    within that partial minute are included. The in-memory flow map is created
    by this process; no pre-start trade history is backfilled. Coverage is not a
    packet-loss or uninterrupted-feed metric. Numeric totals are None when the
    process did not observe any part of a day; unobserved history is never zero-filled.
    """
    now = _finite(now_ts)
    started = _finite(feed_started_at)
    last_seen = _finite(trade_last_seen)
    contract_btc = _finite(contract_value_btc)
    stale_after = _finite(trade_stale_seconds)
    if now is None or now <= 0:
        raise ValueError("now_ts must be finite and positive")
    if contract_btc is None or contract_btc <= 0:
        raise ValueError("contract_value_btc must be finite and positive")
    if not isinstance(days, int) or isinstance(days, bool) or not 1 <= days <= 7:
        raise ValueError("days must be an integer between 1 and 7")
    if stale_after is None or stale_after <= 0:
        raise ValueError("trade_stale_seconds must be finite and positive")

    current_day = int(now // 86_400) * 86_400
    rows: list[dict[str, Any]] = []
    flow_map = flow_minutes if isinstance(flow_minutes, Mapping) else {}

    for offset in range(days - 1, -1, -1):
        day_start = current_day - offset * 86_400
        day_end = min(day_start + 86_400, int(now))
        observed_start = max(float(day_start), started) if started is not None and started > 0 else None
        captured = observed_start is not None and observed_start < day_end
        totals = {
            "buy_prints": 0, "sell_prints": 0,
            "buy_contracts": 0.0, "sell_contracts": 0.0,
            "buy_usd": 0.0, "sell_usd": 0.0,
            "minutes_with_trades": 0,
        }

        if captured:
            for raw_minute, raw_flow in flow_map.items():
                minute = _epoch(raw_minute)
                # Flow records are keyed by UTC minute start. Include the
                # process-start minute bucket because the feed can capture
                # trades later in that minute even though its key precedes
                # the exact process-start timestamp. The in-memory map is
                # process-local, so this does not backfill pre-start trades.
                first_observed_minute = max(
                    day_start, int(started // 60) * 60
                )
                if minute is None or minute < first_observed_minute or minute >= day_end:
                    continue
                if not isinstance(raw_flow, Mapping):
                    continue
                minute_active = False
                for side in ("buy", "sell"):
                    count = _finite(raw_flow.get(f"{side}_count")) or 0.0
                    contracts = _finite(raw_flow.get(f"{side}_contracts")) or 0.0
                    usd = _finite(raw_flow.get(f"{side}_usd")) or 0.0
                    if count > 0 or contracts > 0:
                        minute_active = True
                    totals[f"{side}_prints"] += max(0, int(count))
                    totals[f"{side}_contracts"] += max(0.0, contracts)
                    totals[f"{side}_usd"] += max(0.0, usd)
                if minute_active:
                    totals["minutes_with_trades"] += 1

            elapsed_window = max(1, day_end - day_start)
            process_window_seconds = max(0.0, day_end - observed_start)
            uptime_coverage_pct = min(100.0, process_window_seconds / elapsed_window * 100.0)
            if str(connection_status or "").lower() != "connected":
                capture_status = "PROCESS WINDOW ONLY — TRADE FEED NOT CONNECTED"
            elif last_seen is None or last_seen <= 0 or now - last_seen > stale_after or last_seen > now + 1:
                capture_status = "PROCESS WINDOW ONLY — TRADE CHANNEL NOT FRESH"
            elif uptime_coverage_pct < 99.9:
                capture_status = "PARTIAL UTC DAY — PROCESS STARTED MID-WINDOW"
            elif day_end < day_start + 86_400:
                capture_status = "UTC DAY IN PROGRESS — CAPTURED SINCE PROCESS START"
            else:
                capture_status = "PROCESS WINDOW CAPTURED — DELIVERY COMPLETENESS UNVERIFIED"
        else:
            uptime_coverage_pct = None
            capture_status = "NOT OBSERVED BY THIS PROCESS"

        buy_contracts = totals["buy_contracts"] if captured else None
        sell_contracts = totals["sell_contracts"] if captured else None
        combined = (buy_contracts + sell_contracts) if captured else None
        buy_share = buy_contracts / combined * 100.0 if combined else None
        sell_share = sell_contracts / combined * 100.0 if combined else None
        rows.append({
            "utc_date": day_start,
            "period_start": day_start,
            "period_end": day_end,
            "buy_prints": totals["buy_prints"] if captured else None,
            "sell_prints": totals["sell_prints"] if captured else None,
            "buy_contracts": buy_contracts,
            "sell_contracts": sell_contracts,
            "buy_btc": buy_contracts * contract_btc if captured else None,
            "sell_btc": sell_contracts * contract_btc if captured else None,
            "total_btc": combined * contract_btc if captured else None,
            "buy_usd": totals["buy_usd"] if captured else None,
            "sell_usd": totals["sell_usd"] if captured else None,
            "net_delta_contracts": buy_contracts - sell_contracts if captured else None,
            "buy_share_pct": buy_share,
            "sell_share_pct": sell_share,
            "minutes_with_trades": totals["minutes_with_trades"] if captured else None,
            "app_window_coverage_pct": uptime_coverage_pct,
            "capture_status": capture_status,
            "captured_by_process": captured,
            "is_complete_utc_day": day_end == day_start + 86_400,
        })

    return {
        "days": rows,
        "timezone": "UTC",
        "source": "Delta public trade stream, process-captured minute aggregates",
        "limitations": (
            "Not exchange-backfilled. Process uptime does not prove uninterrupted "
            "message delivery; the public trade stream has no per-trade sequence number."
        ),
    }


def _normalise_trade(raw: Any) -> tuple[float, float, float, str] | None:
    if isinstance(raw, Mapping):
        ts = _finite(raw.get("ts", raw.get("timestamp", raw.get("time"))))
        price = _finite(raw.get("price", raw.get("p")))
        size = _finite(raw.get("size", raw.get("contracts", raw.get("s"))))
        side_raw = raw.get("side", "")
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)) and len(raw) >= 4:
        ts, price, size = (_finite(raw[index]) for index in range(3))
        side_raw = raw[3]
    else:
        return None
    if ts is None or price is None or size is None or ts <= 0 or price <= 0 or size <= 0:
        return None
    side_text = str(side_raw or "").strip().lower()
    if side_text in {"buy", "buy taker", "buyer", "buy_aggressor"}:
        side = "buy"
    elif side_text in {"sell", "sell taker", "seller", "sell_aggressor"}:
        side = "sell"
    else:
        return None
    return ts, price, size, side


def _normalise_snapshot(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        return None
    ts = _finite(raw.get("ts", raw.get("timestamp")))
    if ts is None or ts <= 0:
        return None
    books: dict[str, dict[float, float]] = {}
    for side in ("bids", "asks"):
        raw_book = raw.get(side)
        clean: dict[float, float] = {}
        if isinstance(raw_book, Mapping):
            for raw_price, raw_size in raw_book.items():
                price, size = _finite(raw_price), _finite(raw_size)
                if price is not None and size is not None and price > 0 and size > 0:
                    clean[price] = size
        books[side] = clean
    if not books["bids"] or not books["asks"]:
        return None
    return {"ts": ts, "bids": books["bids"], "asks": books["asks"]}


def analyze_orderbook_events(
    book_snapshots: Sequence[Any] | None,
    trade_log: Sequence[Any] | None,
    *,
    now_ts: float,
    book_valid: bool,
    tick_size: float = 0.5,
    min_large_contracts: float = 50.0,
    min_refill_execution_contracts: float = 25.0,
    max_snapshot_gap_seconds: float = 6.0,
    max_snapshot_age_seconds: float = 6.0,
    lookback_seconds: float = 600.0,
    min_refill_events: int = 2,
) -> dict[str, Any]:
    """Find conservative quote-pull and repeated-refill candidates from samples.

    A pull candidate is a large visible level reduction with little matching
    taker execution between adjacent samples. A refill candidate requires at
    least two intervals where substantial aggressive execution occurs while
    displayed size at the same price remains largely replenished. These
    observations cannot identify intent, parent orders, or hidden size.
    """
    now = _finite(now_ts)
    tick = _finite(tick_size)
    min_large = _finite(min_large_contracts)
    min_execution = _finite(min_refill_execution_contracts)
    max_gap = _finite(max_snapshot_gap_seconds)
    max_age = _finite(max_snapshot_age_seconds)
    lookback = _finite(lookback_seconds)
    if now is None or now <= 0:
        raise ValueError("now_ts must be finite and positive")
    if tick is None or tick <= 0:
        raise ValueError("tick_size must be finite and positive")
    if min_large is None or min_large <= 0 or min_execution is None or min_execution <= 0:
        raise ValueError("contract thresholds must be finite and positive")
    if max_gap is None or max_gap <= 0 or max_age is None or max_age <= 0 or lookback is None or lookback <= 0:
        raise ValueError("snapshot timing thresholds must be finite and positive")
    if not isinstance(min_refill_events, int) or isinstance(min_refill_events, bool) or min_refill_events < 2:
        raise ValueError("min_refill_events must be an integer of at least 2")

    clean_snapshots = [
        row for row in (_normalise_snapshot(item) for item in (book_snapshots or ()))
        if row is not None and row["ts"] >= now - lookback and row["ts"] <= now + 1
    ]
    clean_snapshots.sort(key=lambda row: row["ts"])
    deduped: dict[float, dict[str, Any]] = {row["ts"]: row for row in clean_snapshots}
    snapshots = [deduped[key] for key in sorted(deduped)]
    last_age = now - snapshots[-1]["ts"] if snapshots else None
    ready = (
        bool(book_valid)
        and len(snapshots) >= 3
        and last_age is not None
        and 0 <= last_age <= max_age
    )
    if not ready:
        return {
            "status": "UNAVAILABLE — BOOK NOT FRESH/VERIFIED OR TOO FEW SAMPLES",
            "book_valid": bool(book_valid),
            "snapshot_count": len(snapshots),
            "last_snapshot_age_seconds": last_age,
            "adaptive_large_level_threshold_contracts": None,
            "quote_pull_candidates": [],
            "iceberg_refill_candidates": [],
        }

    sizes = [
        size
        for row in snapshots
        for side in ("bids", "asks")
        for size in row[side].values()
    ]
    ordered_sizes = sorted(sizes)
    p95 = ordered_sizes[int((len(ordered_sizes) - 1) * 0.95)] if ordered_sizes else min_large
    large_threshold = max(min_large, p95)

    trades = [
        item for item in (_normalise_trade(raw) for raw in (trade_log or ()))
        if item is not None and snapshots[0]["ts"] < item[0] <= snapshots[-1]["ts"]
    ]
    trades.sort(key=lambda row: row[0])
    trade_times = [row[0] for row in trades]

    def executed_at_level(start_ts: float, end_ts: float, book_side: str, price: float) -> float:
        expected_aggressor = "buy" if book_side == "asks" else "sell"
        left = bisect_right(trade_times, start_ts)
        right = bisect_right(trade_times, end_ts)
        return sum(
            size for _ts, trade_price, size, aggressor in trades[left:right]
            if aggressor == expected_aggressor and abs(trade_price - price) <= tick * 0.5 + 1e-9
        )

    pulls: list[dict[str, Any]] = []
    refill_groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)

    for previous, current in zip(snapshots, snapshots[1:]):
        gap = current["ts"] - previous["ts"]
        if gap <= 0 or gap > max_gap:
            continue
        for book_side in ("bids", "asks"):
            before_book = previous[book_side]
            after_book = current[book_side]
            visible_prices = list(after_book)
            if not visible_prices:
                continue
            lower_visible, upper_visible = min(visible_prices), max(visible_prices)

            for price, before_size in before_book.items():
                after_size = after_book.get(price, 0.0)
                filled = executed_at_level(previous["ts"], current["ts"], book_side, price)

                if before_size >= large_threshold:
                    reduction = before_size - after_size
                    reduction_pct = reduction / before_size if before_size > 0 else 0.0
                    remains_in_visible_span = (
                        price >= lower_visible - tick if book_side == "bids"
                        else price <= upper_visible + tick
                    )
                    if remains_in_visible_span and reduction_pct >= 0.75 and filled < reduction * 0.25:
                        pulls.append({
                            "time": current["ts"],
                            "side": "BID" if book_side == "bids" else "ASK",
                            "price": price,
                            "displayed_before_contracts": before_size,
                            "displayed_after_contracts": after_size,
                            "displayed_drop_pct": reduction_pct * 100.0,
                            "matched_taker_execution_contracts": filled,
                            "unexplained_reduction_contracts": max(0.0, reduction - filled),
                            "classification": "QUOTE-PULL / SPOOFING-LIKE CANDIDATE",
                        })

                refill_ratio = after_size / before_size if before_size > 0 else 0.0
                if (
                    before_size >= min_large
                    and after_size >= before_size * 0.70
                    and refill_ratio > 0
                    and filled >= max(min_execution, before_size * 0.50)
                ):
                    refill_groups[(book_side, int(round(price / tick)))].append({
                        "time": current["ts"],
                        "side": "BID" if book_side == "bids" else "ASK",
                        "price": price,
                        "visible_before_contracts": before_size,
                        "visible_after_contracts": after_size,
                        "matched_taker_execution_contracts": filled,
                        "displayed_replenishment_ratio": refill_ratio,
                    })

    pulls.sort(key=lambda row: row["time"], reverse=True)
    icebergs: list[dict[str, Any]] = []
    for (_book_side, _price_tick), events in refill_groups.items():
        if len(events) < min_refill_events:
            continue
        total_execution = sum(item["matched_taker_execution_contracts"] for item in events)
        typical_display = sum(item["visible_before_contracts"] for item in events) / len(events)
        if total_execution < typical_display * min_refill_events * 0.5:
            continue
        latest = max(events, key=lambda row: row["time"])
        icebergs.append({
            "side": latest["side"],
            "price": latest["price"],
            "refill_intervals": len(events),
            "matched_taker_execution_contracts": total_execution,
            "typical_visible_size_contracts": typical_display,
            "classification": "REPEATED REFILL / ICEBERG-LIKE CANDIDATE",
            "latest_time": latest["time"],
        })
    icebergs.sort(key=lambda row: (row["refill_intervals"], row["matched_taker_execution_contracts"]), reverse=True)

    return {
        "status": "LIVE SAMPLED BOOK — CANDIDATE ANALYSIS",
        "book_valid": True,
        "snapshot_count": len(snapshots),
        "last_snapshot_age_seconds": last_age,
        "adaptive_large_level_threshold_contracts": large_threshold,
        "quote_pull_candidates": pulls[:12],
        "iceberg_refill_candidates": icebergs[:12],
        "limitations": (
            "A quote pull is an unexplained displayed-size reduction between sampled snapshots, "
            "not proof of spoofing or manipulative intent. Repeated refill is an iceberg-like "
            "candidate, not proof of hidden quantity: ordinary cancels/reposts and new displayed "
            "orders can look similar. Public aggregated L2 data exposes neither participant IDs "
            "nor individual order IDs."
        ),
    }
