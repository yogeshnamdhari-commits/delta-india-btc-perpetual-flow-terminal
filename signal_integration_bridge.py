#!/usr/bin/env python3
"""Integration bridge: DeltaFeed.snapshot() -> signal_engine.MarketSnapshot.

This adapter transforms live feed data into the input format expected by the
confirmed-entry engine, enabling genuine signal generation for production use.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

UTC = timezone.utc

try:
    from signal_engine import (
        AGGRESSOR_BUY,
        AGGRESSOR_SELL,
        ChecksumState,
        Candle,
        MarketSnapshot,
        SignalEngine,
        SignalResult,
        Trade,
    )
    SIGNAL_ENGINE_AVAILABLE = True
except ImportError:
    SIGNAL_ENGINE_AVAILABLE = False

try:
    from signal_adapter import SignalDecisionAdapter
    SIGNAL_ADAPTER_AVAILABLE = True
except ImportError:
    SIGNAL_ADAPTER_AVAILABLE = False


def _to_utc(ts_val: Any) -> datetime:
    """Convert various timestamp formats to timezone-aware UTC datetime."""
    if isinstance(ts_val, datetime):
        if ts_val.tzinfo is None:
            return ts_val.replace(tzinfo=UTC)
        return ts_val.astimezone(UTC)
    if isinstance(ts_val, (int, float)):
        if ts_val > 1e12:
            ts_val = ts_val / 1_000_000  # microseconds
        elif ts_val > 1e9:
            pass  # seconds
        elif ts_val > 1e6:
            ts_val = ts_val * 1000  # milliseconds
        return datetime.fromtimestamp(ts_val, UTC)
    if isinstance(ts_val, str):
        ts_val = ts_val.replace("Z", "+00:00")
        if "+" not in ts_val and "-" not in ts_val[-6:]:
            ts_val = ts_val + "+00:00"
        return datetime.fromisoformat(ts_val)
    raise ValueError(f"Unsupported timestamp type: {type(ts_val)}")


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        f = float(val)
        if f != f:  # NaN check
            return default
        return f
    except (TypeError, ValueError):
        return default


def _seed_candle_history(history_path: str, now: datetime, max_candles: int = 10000) -> list[Candle]:
    """Load historical candles from delta_feed_events.jsonl for warm-up.
    
    Args:
        history_path: Path to delta_feed_events.jsonl (or similar event log)
        now: Current timestamp to align candles to
        max_candles: Maximum number of 1m candles to load
    
    Returns:
        List of Candle objects, newest first, fully completed candles only
    """
    import json
    from pathlib import Path
    
    if not Path(history_path).exists():
        return []
    
    now_epoch = now.timestamp()
    one_min_ago = now_epoch - 60
    candles = []
    seen_minutes = set()
    
    with open(history_path, 'r') as f:
        for line in f:
            try:
                event = json.loads(line.strip())
                if event.get('channel') != 'candlestick_1m':
                    continue
                ts_str = event.get('ts_utc')
                if not ts_str:
                    continue
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                if ts.tzinfo is None:
                    continue
                ts_epoch = ts.timestamp()
                minute_bucket = int(ts_epoch // 60) * 60
                
                if minute_bucket in seen_minutes:
                    continue
                seen_minutes.add(minute_bucket)
                
                if minute_bucket < one_min_ago:
                    o = float(event.get('open', 0))
                    h = float(event.get('high', 0))
                    l = float(event.get('low', 0))
                    cl = float(event.get('close', 0))
                    v = float(event.get('volume', 0))
                    if o > 0 and h > 0 and l > 0 and cl > 0:
                        aligned_ts = datetime.fromtimestamp(minute_bucket, tz=timezone.utc)
                        candles.append(Candle(
                            ts_utc=aligned_ts,
                            open=o, high=h, low=l, close=cl, volume=v
                        ))
            except (json.JSONDecodeError, ValueError, KeyError, TypeError):
                continue
    
    candles.sort(key=lambda c: c.ts_utc)
    return candles


def _aggregate_completed_candles(candles_1m, minutes, *, now=None):
    """Aggregate complete, contiguous 1m candles into closed 5m/15m bars."""
    import time as _time_module
    from datetime import datetime, timezone
    
    if minutes not in (5, 15):
        raise ValueError("minutes must be 5 or 15")
    if now is None:
        now_epoch = _time_module.time()
    elif isinstance(now, datetime):
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        now_epoch = now.timestamp()
    else:
        now_epoch = float(now)
    
    interval_seconds = minutes * 60
    groups = {}
    for candle in candles_1m:
        ts = getattr(candle, "ts_utc", None)
        if not isinstance(ts, datetime):
            continue
        if ts.tzinfo is None or ts.utcoffset() is None:
            continue
        epoch = ts.timestamp()
        minute = int(epoch)
        if abs(epoch - minute) > 1e-6 or minute % 60 != 0:
            continue
        if minute + 60 > now_epoch:
            continue
        bucket = (minute // interval_seconds) * interval_seconds
        if bucket + interval_seconds > now_epoch:
            continue
        groups.setdefault(bucket, {})[minute] = candle
    
    aggregated = []
    for bucket, minute_map in sorted(groups.items()):
        expected = [bucket + offset * 60 for offset in range(minutes)]
        if sorted(minute_map) != expected:
            continue
        bars = [minute_map[timestamp] for timestamp in expected]
        aggregated.append(Candle(
            ts_utc=datetime.fromtimestamp(bucket, tz=timezone.utc),
            open=bars[0].open,
            high=max(bar.high for bar in bars),
            low=min(bar.low for bar in bars),
            close=bars[-1].close,
            volume=sum(bar.volume for bar in bars),
        ))
    return aggregated


def build_market_snapshot(feed_data: dict[str, Any], now: datetime | None = None) -> MarketSnapshot | None:
    """Convert DeltaFeed.snapshot() output to signal_engine.MarketSnapshot.

    Args:
        feed_data: Output from DeltaFeed.snapshot()
        now: Current timestamp (defaults to now)

    Returns:
        MarketSnapshot ready for SignalEngine.evaluate(), or None if required data missing
    """
    if not SIGNAL_ENGINE_AVAILABLE:
        return None

    now = now or datetime.now(UTC)
    
    last_price = _safe_float(feed_data.get("last_price"), 0.0)
    bid = _safe_float(feed_data.get("bid"), 0.0)
    ask = _safe_float(feed_data.get("ask"), 0.0)
    
    if last_price <= 0 or bid <= 0 or ask < bid:
        return None
    
    spread_bps = ((ask - bid) / ((bid + ask) / 2) * 10000) if (bid + ask) > 0 else float('inf')
    
    trades_data = feed_data.get("trades", []) or []
    recent_trades: list[Trade] = []
    for t in trades_data[-1000:]:
        try:
            ts_val = t.get("timestamp") or t.get("ts") or t.get("time_utc") or t.get("time")
            if ts_val is None:
                continue
            trade_ts = _to_utc(ts_val)
            price = _safe_float(t.get("price") or t.get("p"), 0.0)
            size = _safe_float(t.get("size") or t.get("s"), 0.0)
            if price <= 0 or size <= 0:
                continue
            role = str(t.get("buyer_role") or t.get("r", "")).strip().lower()
            if role == "t":
                aggr = AGGRESSOR_BUY
            elif role == "m":
                aggr = AGGRESSOR_SELL
            else:
                continue
            recent_trades.append(Trade(trade_ts, price, size, aggr))
        except Exception:
            continue
    recent_trades.sort(key=lambda t: t.ts_utc)
    
    candles_1m_raw = feed_data.get("candles", []) or []
    candles_1m: list[Candle] = []
    for c in candles_1m_raw:
        if not isinstance(c, dict):
            continue
        try:
            ts_val = c.get("timestamp") or c.get("time") or c.get("ts") or c.get("t")
            if ts_val is None:
                continue
            c_ts = _to_utc(ts_val)
            o = _safe_float(c.get("open") or c.get("o"), 0.0)
            h = _safe_float(c.get("high") or c.get("h"), 0.0)
            l = _safe_float(c.get("low") or c.get("l"), 0.0)
            cl = _safe_float(c.get("close") or c.get("c"), 0.0)
            v = _safe_float(c.get("volume") or c.get("v"), 0.0)
            if o <= 0 or h <= 0 or l <= 0 or cl <= 0:
                continue
            if h < max(o, cl) or l > min(o, cl):
                continue
            candles_1m.append(Candle(ts_utc=c_ts, open=o, high=h, low=l, close=cl, volume=v))
        except Exception:
            continue
    candles_1m.sort(key=lambda c: c.ts_utc)
    
    channel_times = feed_data.get("channel_last_seen", {}) or {}
    last_trade_ts = now
    quote_ts = now
    for ch, ts in channel_times.items():
        try:
            utc_ts = _to_utc(ts)
            if ch == "trades" and utc_ts < last_trade_ts:
                last_trade_ts = utc_ts
            if ch in ("ticker", "mark_price", "ob_updates") and utc_ts < quote_ts:
                quote_ts = utc_ts
        except Exception:
            pass
    
    book_synced = bool(feed_data.get("book_synced", False))
    checksum_val = feed_data.get("checksum_ok")
    if checksum_val is True:
        checksum_state = ChecksumState.VERIFIED
    elif checksum_val is False:
        checksum_state = ChecksumState.FAILED
    else:
        checksum_state = ChecksumState.UNAVAILABLE
    
    aggregation_now = now
    candles_5m = _aggregate_completed_candles(candles_1m, 5, now=aggregation_now)
    candles_15m = _aggregate_completed_candles(candles_1m, 15, now=aggregation_now)
    
    if len(candles_15m) < 55:
        history_path = "/Users/targetmobile/Downloads/delta_feed_events.jsonl"
        seeded_1m = _seed_candle_history(history_path, now, max_candles=10000)
        if seeded_1m:
            combined_1m = list(candles_1m)
            for seed_c in seeded_1m:
                is_dup = False
                for live_c in candles_1m:
                    if live_c.ts_utc == seed_c.ts_utc:
                        is_dup = True
                        break
                if not is_dup:
                    combined_1m.append(seed_c)
            combined_1m.sort(key=lambda c: c.ts_utc)
            candles_1m = combined_1m
            candles_5m = _aggregate_completed_candles(candles_1m, 5, now=aggregation_now)
            candles_15m = _aggregate_completed_candles(candles_1m, 15, now=aggregation_now)
    
    return MarketSnapshot(
        ts_utc=now,
        last_price=last_price,
        bid=bid,
        ask=ask,
        candles_1m=candles_1m,
        candles_5m=candles_5m,
        candles_15m=candles_15m,
        book_synced=book_synced,
        connection_healthy=book_synced,
        sequence_gap_unresolved=not book_synced,
        checksum_state=checksum_state,
        market_fresh=True,
        spread_bps=spread_bps,
        recent_trades=recent_trades,
        completed_candle_timestamps={},
        swings_5m=(),
        order_blocks_5m=(),
        ask_liquidity=(),
        bid_liquidity=(),
        atr_5m=None,
        atr_15m=None,
    )


def _result_to_dict(result) -> dict[str, Any]:
    """Convert SignalResult to dict for logging (current signal_engine doesn't have to_dict)."""
    return {
        "signal": result.signal.value if hasattr(result.signal, 'value') else str(result.signal),
        "state": result.state.name if hasattr(result.state, 'name') else str(result.state),
        "signal_id": getattr(result, 'signal_id', None),
        "generated_at_utc": getattr(result, 'generated_at_utc', None),
        "expires_at_utc": getattr(result, 'expires_at_utc', None),
        "proposed_entry": getattr(result, 'proposed_entry', None),
        "stop_loss": getattr(result, 'stop_loss', None),
        "take_profit": getattr(result, 'take_profit', None),
        "invalidation_price": getattr(result, 'invalidation_price', None),
        "risk_distance": getattr(result, 'risk_distance', None),
        "reward_distance": getattr(result, 'reward_distance', None),
        "risk_reward_ratio": getattr(result, 'risk_reward_ratio', None),
        "reason_codes": list(getattr(result, 'reason_codes', [])),
        "human_readable_explanation": getattr(result, 'human_readable_explanation', ''),
        "confirmation_conditions_passed": list(getattr(result, 'confirmation_conditions_passed', [])),
        "confirmation_conditions_failed": list(getattr(result, 'confirmation_conditions_failed', [])),
        "data_quality_gate_results": dict(getattr(result, 'data_quality_gate_results', {})),
    }


def evaluate_live_signal(feed, log_path: str | None = None, min_interval: float = 1.0) -> dict[str, Any] | None:
    """Evaluate live feed data through the confirmed-entry engine.

    This is the main entry point for production signal generation.
    It converts the live feed snapshot, evaluates through the engine,
    and returns the decision record.

    Args:
        feed: The DeltaFeed instance
        log_path: Optional override for decision log path
        min_interval: Minimum seconds between evaluations (throttling)

    Returns:
        Decision record dict with signal, blocking_reason, and feed state,
        or None if insufficient data for a valid snapshot
    """
    if not SIGNAL_ENGINE_AVAILABLE:
        return None
    
    feed_data = feed.snapshot()
    snapshot = build_market_snapshot(feed_data)
    
    if snapshot is None:
        return None
    
    engine = SignalEngine()
    result = engine.evaluate(snapshot)
    
    feed_state = feed.get_signal_state() if hasattr(feed, 'get_signal_state') else {}
    
    decision_record = {
        "decision": result.signal.value,
        "signal_type": result.signal.value if result.signal.value in ("BUY", "SELL") else None,
        "score": None,
        "confidence": 1.0,
        "blocking_reason": "; ".join(result.reason_codes) if result.reason_codes else None,
        "feed_fresh": all(result.data_quality_gate_results.get(k, False) for k in [
            "channel_fresh:ticker", "channel_fresh:ob_updates", "channel_fresh:trades"
        ]),
        "book_synced": result.data_quality_gate_results.get("book_synced", False),
        "sequence_valid": result.data_quality_gate_results.get("sequence_valid", False),
        "crc_valid": result.data_quality_gate_results.get("checksum_verified", False),
        "feed_age_seconds": None,
        "book_seq": feed_data.get("book_seq"),
        "total_disconnects": feed_state.get("total_disconnects", 0),
        "total_reconnects": feed_state.get("total_reconnects", 0),
        "feed_state": feed_state,
        "engine_result": _result_to_dict(result),
        "metadata": {"runtime_context": "live_signal_integration", "source": "delta_feed"},
    }
    
    return decision_record


if __name__ == "__main__":
    print("This module provides: build_market_snapshot(), evaluate_live_signal()")
    print("Usage: Import from your dashboard and call evaluate_live_signal(feed)")