#!/usr/bin/env python3
"""Integration bridge: DeltaFeed.snapshot() -> signal_engine.MarketSnapshot.

This adapter transforms live feed data into the input format expected by the
confirmed-entry engine, enabling genuine signal generation for production use.
"""
from __future__ import annotations

import math
import os
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
    """Convert ISO, seconds, milliseconds, microseconds, or nanoseconds to UTC."""
    if isinstance(ts_val, datetime):
        if ts_val.tzinfo is None or ts_val.utcoffset() is None:
            return ts_val.replace(tzinfo=UTC)
        return ts_val.astimezone(UTC)
    if isinstance(ts_val, (int, float)):
        value = float(ts_val)
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"Invalid epoch timestamp: {ts_val!r}")
        if value >= 1e17:
            value /= 1e9
        elif value >= 1e14:
            value /= 1e6
        elif value >= 1e11:
            value /= 1e3
        return datetime.fromtimestamp(value, UTC)
    if isinstance(ts_val, str):
        value = ts_val.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    raise ValueError(f"Unsupported timestamp type: {type(ts_val)}")


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        number = float(val)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError, OverflowError):
        return default


_REQUIRED_CHANNEL_MAX_AGE_SECONDS = {"ticker": 8.0, "ob_updates": 3.0, "trades": 20.0}
_SEED_HISTORY_CACHE: dict[str, Any] = {"path": None, "loaded_at": 0.0, "candles": []}
_SEED_HISTORY_CACHE_TTL_SECONDS = 300.0


def _feed_health(feed_data: dict[str, Any], now: datetime) -> dict[str, Any]:
    """Derive health from explicit feed state; absent or unknown state fails closed."""
    connection_healthy = str(feed_data.get("connection", "")).lower() == "connected"
    book_synced = feed_data.get("book_synced") is True
    raw_gap = feed_data.get("sequence_gap_unresolved")
    sequence_gap_unresolved = True if raw_gap is None else bool(raw_gap)
    channel_times = feed_data.get("channel_last_seen", {}) or {}
    channel_age_seconds: dict[str, float | None] = {}
    channel_freshness: dict[str, bool] = {}
    for channel, max_age in _REQUIRED_CHANNEL_MAX_AGE_SECONDS.items():
        try:
            seen = channel_times.get(channel)
            age = (now - _to_utc(seen)).total_seconds() if seen is not None else None
        except (TypeError, ValueError, OverflowError, OSError):
            age = None
        channel_age_seconds[channel] = age
        channel_freshness[channel] = age is not None and 0.0 <= age <= max_age
    market_fresh = connection_healthy and all(channel_freshness.values())
    checksum_verified = feed_data.get("checksum_ok") is True
    try:
        last_market = feed_data.get("last_market_message_at")
        market_age = (now - _to_utc(last_market)).total_seconds() if last_market is not None else None
    except (TypeError, ValueError, OverflowError, OSError):
        market_age = None
    return {
        "connection_healthy": connection_healthy,
        "market_fresh": market_fresh,
        "channel_age_seconds": channel_age_seconds,
        "channel_freshness": channel_freshness,
        "book_synced": book_synced,
        "sequence_gap_unresolved": sequence_gap_unresolved,
        "checksum_verified": checksum_verified,
        "feed_age_seconds": market_age,
    }


def _seed_candle_history(history_path: str, now: datetime, max_candles: int = 10000) -> list[Candle]:
    """Load closed candles only when an exchange candle timestamp is explicit.

    Older logs used ts_utc for receive time, not the candle bucket, so they are
    ignored instead of misaligned into fabricated historical bars.
    """
    import json
    from pathlib import Path
    path = Path(history_path).expanduser()
    if not path.is_file() or max_candles <= 0:
        return []
    # Very large logs must not be fully parsed on every Streamlit rerun. Small
    # fixture/test logs are rescanned so appended test data is immediately visible.
    try:
        large_log = path.stat().st_size >= 5 * 1024 * 1024
    except OSError:
        return []
    cache = _SEED_HISTORY_CACHE
    if (large_log and cache.get("path") == str(path) and
            time.monotonic() - float(cache.get("loaded_at", 0.0)) < _SEED_HISTORY_CACHE_TTL_SECONDS):
        cached = cache.get("candles", [])
        return [c for c in cached if c.ts_utc.timestamp() + 60 <= now.timestamp()][-max_candles:]
    latest_by_minute: dict[int, Candle] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                event = json.loads(line)
                if event.get("channel") != "candlestick_1m":
                    continue
                raw_ts = event.get("candle_ts_utc", event.get("candle_ts"))
                if raw_ts is None:
                    continue
                epoch = _to_utc(raw_ts).timestamp()
                bucket = int(epoch // 60) * 60
                if bucket + 60 > now.timestamp():
                    continue
                o = _safe_float(event.get("open"), 0.0)
                h = _safe_float(event.get("high"), 0.0)
                low = _safe_float(event.get("low"), 0.0)
                close = _safe_float(event.get("close"), 0.0)
                volume = _safe_float(event.get("volume"), 0.0)
                if min(o, h, low, close) <= 0:
                    continue
                latest_by_minute[bucket] = Candle(
                    ts_utc=datetime.fromtimestamp(bucket, tz=UTC),
                    open=o, high=h, low=low, close=close, volume=volume,
                )
            except (json.JSONDecodeError, ValueError, KeyError, TypeError, OverflowError, OSError):
                continue
    result = [latest_by_minute[k] for k in sorted(latest_by_minute)[-max_candles:]]
    if large_log:
        cache.update({"path": str(path), "loaded_at": time.monotonic(), "candles": result})
    return result


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


def build_market_snapshot(
    feed_data: dict[str, Any],
    now: datetime | None = None,
    *,
    history_path: str | None = None,
) -> MarketSnapshot | None:
    """Convert raw DeltaFeed data into an engine snapshot with fail-closed health."""
    if not SIGNAL_ENGINE_AVAILABLE:
        return None
    now = now or datetime.now(UTC)
    health = _feed_health(feed_data, now)
    last_price = _safe_float(feed_data.get("last_price"), 0.0)
    bid = _safe_float(feed_data.get("bid"), 0.0)
    ask = _safe_float(feed_data.get("ask"), 0.0)
    if last_price <= 0 or bid <= 0 or ask < bid:
        return None
    spread_bps = ((ask - bid) / ((bid + ask) / 2) * 10000) if (bid + ask) > 0 else float("inf")

    recent_trades: list[Trade] = []
    for row in (feed_data.get("trades", []) or [])[-1000:]:
        try:
            ts_val = row.get("timestamp") or row.get("ts") or row.get("time_utc") or row.get("time")
            if ts_val is None:
                continue
            trade_ts = _to_utc(ts_val)
            price = _safe_float(row.get("price") or row.get("p"), 0.0)
            size = _safe_float(row.get("size") or row.get("s"), 0.0)
            role = str(row.get("buyer_role") or row.get("r", "")).strip().lower()
            if price <= 0 or size <= 0:
                continue
            if role == "t":
                aggressor = AGGRESSOR_BUY
            elif role == "m":
                aggressor = AGGRESSOR_SELL
            else:
                continue
            recent_trades.append(Trade(trade_ts, price, size, aggressor))
        except (TypeError, ValueError, OverflowError, OSError):
            continue
    recent_trades.sort(key=lambda trade: trade.ts_utc)

    candles_1m: list[Candle] = []
    for row in (feed_data.get("candles", []) or []):
        if not isinstance(row, dict):
            continue
        try:
            ts_val = row.get("timestamp") or row.get("time") or row.get("ts") or row.get("t")
            if ts_val is None:
                continue
            candle_ts = _to_utc(ts_val)
            open_price = _safe_float(row.get("open") or row.get("o"), 0.0)
            high_price = _safe_float(row.get("high") or row.get("h"), 0.0)
            low_price = _safe_float(row.get("low") or row.get("l"), 0.0)
            close_price = _safe_float(row.get("close") or row.get("c"), 0.0)
            raw_volume = row.get("volume") if row.get("volume") is not None else row.get("v")
            volume = _safe_float(raw_volume, 0.0)
            if min(open_price, high_price, low_price, close_price) <= 0:
                continue
            if high_price < max(open_price, close_price) or low_price > min(open_price, close_price):
                continue
            candles_1m.append(Candle(ts_utc=candle_ts, open=open_price, high=high_price,
                                    low=low_price, close=close_price, volume=volume))
        except (TypeError, ValueError, OverflowError, OSError):
            continue
    candles_1m.sort(key=lambda candle: candle.ts_utc)
    candles_5m = _aggregate_completed_candles(candles_1m, 5, now=now)
    candles_15m = _aggregate_completed_candles(candles_1m, 15, now=now)
    if len(candles_15m) < 55:
        seed_path = history_path or os.environ.get(
            "DELTA_EVENT_LOG_FILE", "/Users/targetmobile/Downloads/delta_feed_events.jsonl")
        seeded_1m = _seed_candle_history(seed_path, now, max_candles=10000)
        if seeded_1m:
            existing = {c.ts_utc for c in candles_1m}
            candles_1m = sorted(candles_1m + [c for c in seeded_1m if c.ts_utc not in existing],
                                key=lambda c: c.ts_utc)
            candles_5m = _aggregate_completed_candles(candles_1m, 5, now=now)
            candles_15m = _aggregate_completed_candles(candles_1m, 15, now=now)
    checksum_value = feed_data.get("checksum_ok")
    checksum_state = (ChecksumState.VERIFIED if checksum_value is True else
                      ChecksumState.FAILED if checksum_value is False else ChecksumState.UNAVAILABLE)
    return MarketSnapshot(
        ts_utc=now, last_price=last_price, bid=bid, ask=ask,
        candles_1m=candles_1m, candles_5m=candles_5m, candles_15m=candles_15m,
        book_synced=health["book_synced"], connection_healthy=health["connection_healthy"],
        sequence_gap_unresolved=health["sequence_gap_unresolved"], checksum_state=checksum_state,
        market_fresh=health["market_fresh"], spread_bps=spread_bps, recent_trades=recent_trades,
        completed_candle_timestamps={}, swings_5m=(), order_blocks_5m=(), ask_liquidity=(),
        bid_liquidity=(), atr_5m=None, atr_15m=None,
    )


def _result_to_dict(result) -> dict[str, Any]:
    """Return a JSON-safe representation of a SignalResult."""
    def iso(value: Any) -> Any:
        return value.isoformat() if isinstance(value, datetime) else value
    return {
        "signal": result.signal.value if hasattr(result.signal, "value") else str(result.signal),
        "state": result.state.name if hasattr(result.state, "name") else str(result.state),
        "signal_id": getattr(result, "signal_id", None),
        "generated_at_utc": iso(getattr(result, "generated_at_utc", None)),
        "expires_at_utc": iso(getattr(result, "expires_at_utc", None)),
        "setup_candle_timestamp": iso(getattr(result, "setup_candle_timestamp", None)),
        "proposed_entry": getattr(result, "proposed_entry", None),
        "stop_loss": getattr(result, "stop_loss", None),
        "take_profit": getattr(result, "take_profit", None),
        "invalidation_price": getattr(result, "invalidation_price", None),
        "risk_distance": getattr(result, "risk_distance", None),
        "reward_distance": getattr(result, "reward_distance", None),
        "risk_reward_ratio": getattr(result, "risk_reward_ratio", None),
        "reason_codes": list(getattr(result, "reason_codes", [])),
        "human_readable_explanation": getattr(result, "human_readable_explanation", ""),
        "confirmation_conditions_passed": list(getattr(result, "confirmation_conditions_passed", [])),
        "confirmation_conditions_failed": list(getattr(result, "confirmation_conditions_failed", [])),
        "data_quality_gate_results": dict(getattr(result, "data_quality_gate_results", {})),
        "duplicate_suppressed": bool(getattr(result, "duplicate_suppressed", False)),
        "lifecycle_transitions": list(getattr(result, "lifecycle_transitions", [])),
    }


def _blocked_record(feed_data: dict[str, Any], feed_state: dict[str, Any], now: datetime,
                    reason: str, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
    """Consistent no-entry record when a usable snapshot cannot be evaluated."""
    health = _feed_health(feed_data, now)
    merged = dict(feed_state or {})
    for key in ("connection", "book_synced", "checksum_ok", "book_seq", "sequence_gap_unresolved",
                "channel_last_seen", "channel_message_counts", "total_disconnects", "total_reconnects"):
        if key in feed_data:
            merged[key] = feed_data[key]
    merged["derived_health"] = health
    return {
        "ts_utc": now.isoformat(), "decision": "BLOCKED", "signal_type": None,
        "score": None, "confidence": None, "blocking_reason": reason,
        "feed_fresh": bool(health["market_fresh"]), "book_synced": bool(health["book_synced"]),
        "no_unresolved_sequence_gap": not health["sequence_gap_unresolved"],
        "sequence_valid": not health["sequence_gap_unresolved"],
        "crc_valid": bool(health["checksum_verified"]),
        "orderbook_integrity_ok": bool(health["book_synced"] and not health["sequence_gap_unresolved"] and health["checksum_verified"]),
        "feed_age_seconds": health["feed_age_seconds"], "book_seq": feed_data.get("book_seq"),
        "total_disconnects": feed_data.get("total_disconnects", 0),
        "total_reconnects": feed_data.get("total_reconnects", 0), "feed_state": merged,
        "engine_result": None, "data_quality_gate_results": {},
        "metadata": {"runtime_context": "live_signal", "source": "delta_feed",
                      "engine_evaluated": False, "details": details or {}},
    }


def evaluate_feed_snapshot(feed_data: dict[str, Any], *, feed_state: dict[str, Any] | None = None,
                           engine: SignalEngine | None = None, now: datetime | None = None,
                           history_path: str | None = None, min_5m_candles: int = 55,
                           min_15m_candles: int = 55) -> dict[str, Any]:
    """Canonical feed-to-engine function shared by UI and the integration contract tests."""
    now = now or datetime.now(UTC)
    state = feed_state or {}
    if not SIGNAL_ENGINE_AVAILABLE:
        return _blocked_record(feed_data, state, now, "SIGNAL_ENGINE_UNAVAILABLE")
    try:
        snapshot = build_market_snapshot(feed_data, now=now, history_path=history_path)
    except Exception as exc:
        return _blocked_record(feed_data, state, now, "SNAPSHOT_BUILD_FAILED",
                               details={"error_type": type(exc).__name__})
    if snapshot is None:
        return _blocked_record(feed_data, state, now, "SNAPSHOT_UNAVAILABLE",
                               details={"required_fields": ["last_price", "bid", "ask"]})
    active_engine = engine if engine is not None else SignalEngine()
    try:
        result = active_engine.evaluate(snapshot, now_utc=now)
    except Exception as exc:
        return _blocked_record(feed_data, state, now, "SIGNAL_ENGINE_EVALUATION_FAILED",
                               details={"error_type": type(exc).__name__})
    gates = dict(result.data_quality_gate_results or {})
    failed = sorted(key for key, passed in gates.items() if passed is False)
    count_5m, count_15m = len(snapshot.candles_5m), len(snapshot.candles_15m)
    warmup = count_5m < min_5m_candles or count_15m < min_15m_candles
    if failed:
        decision = "BLOCKED"
    elif warmup:
        decision = "WAIT"
    elif result.signal.value in ("BUY", "SELL") and result.duplicate_suppressed:
        decision = "WAIT"
    else:
        decision = result.signal.value
    if failed:
        reason = "; ".join(result.reason_codes) or "DATA_QUALITY_GATE_FAILED"
    elif warmup:
        reason = "WARMUP_INSUFFICIENT_CANDLES"
    elif result.duplicate_suppressed:
        reason = "DUPLICATE_SIGNAL_SUPPRESSED"
    else:
        reason = "; ".join(result.reason_codes) if result.reason_codes else None
    health = _feed_health(feed_data, now)
    merged = dict(state)
    for key in ("connection", "book_synced", "checksum_ok", "book_seq", "sequence_gap_unresolved",
                "channel_last_seen", "channel_message_counts", "total_disconnects", "total_reconnects"):
        if key in feed_data:
            merged[key] = feed_data[key]
    merged["derived_health"] = health
    no_gap = gates.get("no_unresolved_sequence_gap", False) is True
    checksum_ok = gates.get("checksum_ok", False) is True
    result_dict = _result_to_dict(result)
    if warmup and not failed:
        result_dict["reason_codes"] = ["WARMUP_INSUFFICIENT_CANDLES"]
        result_dict["human_readable_explanation"] = f"Warming up: 5m {count_5m}/{min_5m_candles}; 15m {count_15m}/{min_15m_candles}."
    return {
        "ts_utc": now.isoformat(), "decision": decision,
        "signal_type": decision if decision in ("BUY", "SELL") else None,
        "score": None, "confidence": None, "blocking_reason": reason,
        "feed_fresh": bool(gates.get("connection_healthy", False) and gates.get("market_fresh", False)
                            and gates.get("candles_fresh", False) and gates.get("trades_fresh", False)
                            and gates.get("time_consistent", False)),
        "book_synced": gates.get("book_synced", False), "no_unresolved_sequence_gap": no_gap,
        "sequence_valid": no_gap, "crc_valid": checksum_ok,
        "orderbook_integrity_ok": bool(gates.get("book_synced", False) and no_gap and checksum_ok),
        "feed_age_seconds": health["feed_age_seconds"], "book_seq": feed_data.get("book_seq"),
        "total_disconnects": feed_data.get("total_disconnects", 0),
        "total_reconnects": feed_data.get("total_reconnects", 0), "feed_state": merged,
        "engine_result": result_dict, "data_quality_gate_results": gates,
        "signal_id": getattr(result, "signal_id", None),
        "metadata": {"runtime_context": "live_signal", "source": "delta_feed", "engine_evaluated": True,
                     "failed_data_quality_gates": failed, "warmup_failed": warmup,
                     "candles_5m": count_5m, "candles_15m": count_15m},
    }


def evaluate_live_signal(feed, log_path: str | None = None, min_interval: float = 1.0, *,
                         engine: SignalEngine | None = None, now: datetime | None = None) -> dict[str, Any]:
    """Compatibility wrapper around the canonical feed decision function; it never places orders."""
    try:
        data = feed.snapshot()
        state = feed.get_signal_state() if hasattr(feed, "get_signal_state") else {}
    except Exception as exc:
        timestamp = now or datetime.now(UTC)
        return _blocked_record({}, {}, timestamp, "FEED_SNAPSHOT_FAILED",
                               details={"error_type": type(exc).__name__})
    return evaluate_feed_snapshot(data, feed_state=state, engine=engine, now=now, history_path=log_path)

if __name__ == "__main__":
    print("This module provides: build_market_snapshot(), evaluate_live_signal()")
    print("Usage: Import from your dashboard and call evaluate_live_signal(feed)")