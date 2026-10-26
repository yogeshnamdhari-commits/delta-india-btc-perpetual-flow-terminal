"""orderflow.py - pure, deterministic order-flow detection for the Delta BTC terminal.

No I/O, no network, standard library only. Every function operates on plain
lists/dicts so the rules can be unit-tested against synthetic fixtures.

Candle dict format (only COMPLETED candles are passed by callers):
    {"time": int,      # bucket start, epoch seconds (UTC)
     "open": float, "high": float, "low": float, "close": float,
     "volume": float}  # exchange-reported quantity (contracts)

Trade dict format:
    {"ts": float,      # epoch seconds (exchange-reported trade time preferred)
     "price": float, "size": float,   # size in contracts
     "side": "buy" | "sell"}          # aggressor side from buyer_role

All prices are instrument points (BTCUSD: tick 0.5, contract value 0.001 BTC).
Order-block zones, swing/equal levels and sweeps are PRICE-ACTION-DERIVED
signals, not verified records of institutional orders or resting liquidity.
Book-derived liquidity must be supplied by the caller from the live, sequence-
and CRC32-synchronized order book.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any


# --------------------------------------------------------------------------
# basic statistics
# --------------------------------------------------------------------------

def percentile(values, pct):
    """Linear-interpolation percentile (same method as numpy 'linear').

    Deterministic; returns None for an empty input. pct in [0, 100].
    """
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * (pct / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return float(ordered[lo] + (ordered[hi] - ordered[lo]) * frac)


def adaptive_threshold(values, pct, min_samples):
    """Return the percentile-based threshold, or None when there is not
    enough data to establish one (caller must mark the signal unavailable
    instead of fabricating a threshold)."""
    if values is None or len(values) < min_samples:
        return None
    return percentile(values, pct)


# --------------------------------------------------------------------------
# candles / ATR / swings
# --------------------------------------------------------------------------

def true_range(high, low, prev_close):
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def atr(candles, period=14):
    """Simple (SMA) ATR over `period` true ranges.

    Requires at least period+1 completed candles; returns None otherwise.
    """
    if period < 1 or len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        trs.append(true_range(candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]))
    trs = trs[-period:]
    return sum(trs) / len(trs)


def swing_points(candles, left=5, right=5):
    """Window-extrema swing detection over completed candles.

    A swing high at index i requires high[i] to be the maximum of the closed
    window [i-left, i+right]; a swing low the minimum. The swing is only
    *confirmed* once `right` further candles have closed (callers must not
    treat a swing as confirmed before its confirmation candle). Ties are kept
    (adjacent equal highs both qualify) because equal highs/lows are the
    input for equal-level detection.
    """
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    n = len(candles)
    out = []
    for i in range(n):
        lo = max(0, i - left)
        hi = min(n - 1, i + right)
        window_highs = highs[lo:hi + 1]
        window_lows = lows[lo:hi + 1]
        if highs[i] == max(window_highs):
            out.append({"index": i, "time": candles[i]["time"], "price": highs[i], "kind": "high"})
        if lows[i] == min(window_lows):
            out.append({"index": i, "time": candles[i]["time"], "price": lows[i], "kind": "low"})
    out.sort(key=lambda s: (s["index"], s["kind"]))
    return out


# --------------------------------------------------------------------------
# order blocks (price-action derived)
# --------------------------------------------------------------------------

def detect_order_blocks(candles, *, lookback=150, swing_left=5, swing_right=5,
                        atr_period=14, displacement=1.0, zone="range", max_zones=12):
    """Detect bullish/bearish order-block zones from completed candles.

    Rules (documented, deterministic):
      * Only confirmed swings are used: the swing candle must have at least
        `swing_right` closed candles after it (no future data is read; a zone
        confirmed at candle j depends only on candles up to j).
      * Bullish OB: the last bearish candle (close < open) at index m after a
        confirmed swing high H, where a later candle j (m < j <= H_index +
        lookback) closes above H + displacement * ATR. Zone = candle m's
        high/low ("range") or body [close, open] ("body").
      * Bearish OB: symmetric around a confirmed swing low.
      * A zone's boundaries never change once confirmed (no repainting):
        detection depends only on candles[0..j].

    Returns zones ordered by confirmation time; at most `max_zones` per side.
    Each zone dict carries "_mi"/"_ci" internal candle indices (not for display).
    """
    n = len(candles)
    if n < swing_left + swing_right + 3:
        return []
    atr_value = atr(candles, atr_period)
    if atr_value is None or atr_value <= 0:
        return []
    swings = swing_points(candles, swing_left, swing_right)
    zones = []

    for s in swings:
        i = s["index"]
        if i + swing_right >= n:            # swing not yet confirmed
            continue
        if s["kind"] == "high":
            level = candles[i]["high"]
            j = _first_displacement_up(candles, i, level, displacement, atr_value, lookback)
            if j is None:
                continue
            m = _last_opposite_candle(candles, i, j, bearish=True)
            if m is None:
                continue
            top, bottom = _zone_bounds(candles[m], zone)
            zones.append({
                "direction": "bullish",
                "formation_time": candles[m]["time"],
                "formation_price": (candles[m]["open"] + candles[m]["close"]) / 2.0,
                "confirmed_time": candles[j]["time"],
                "swing_time": candles[i]["time"],
                "swing_price": level,
                "displacement_x_atr": (candles[j]["close"] - level) / atr_value,
                "top": top, "bottom": bottom,
                "status": "active", "status_time": None,
                "_mi": m, "_ci": j,
            })
        else:
            level = candles[i]["low"]
            j = _first_displacement_down(candles, i, level, displacement, atr_value, lookback)
            if j is None:
                continue
            m = _last_opposite_candle(candles, i, j, bearish=False)
            if m is None:
                continue
            top, bottom = _zone_bounds(candles[m], zone)
            zones.append({
                "direction": "bearish",
                "formation_time": candles[m]["time"],
                "formation_price": (candles[m]["open"] + candles[m]["close"]) / 2.0,
                "confirmed_time": candles[j]["time"],
                "swing_time": candles[i]["time"],
                "swing_price": level,
                "displacement_x_atr": (level - candles[j]["close"]) / atr_value,
                "top": top, "bottom": bottom,
                "status": "active", "status_time": None,
                "_mi": m, "_ci": j,
            })

    zones.sort(key=lambda z: z["confirmed_time"])
    deduped, seen = [], set()
    for z in zones:                       # adjacent tied swings can yield the
        key = (z["direction"], z["formation_time"], z["confirmed_time"], z["top"], z["bottom"])
        if key not in seen:               # same zone - keep one
            seen.add(key)
            deduped.append(z)
    zones = deduped
    return zones[-max_zones * 2:] if max_zones else zones


def _first_displacement_up(candles, i, level, mult, atr_value, lookback):
    n = len(candles)
    limit = min(n - 1, i + lookback)
    for j in range(i + 1, limit + 1):
        if candles[j]["close"] > level + mult * atr_value:
            return j
    return None


def _first_displacement_down(candles, i, level, mult, atr_value, lookback):
    n = len(candles)
    limit = min(n - 1, i + lookback)
    for j in range(i + 1, limit + 1):
        if candles[j]["close"] < level - mult * atr_value:
            return j
    return None


def _last_opposite_candle(candles, i, j, bearish):
    """Last bearish (bearish=True) or bullish (bearish=False) candle in [i, j-1]."""
    for m in range(j - 1, i - 1, -1):
        c = candles[m]
        if bearish and c["close"] < c["open"]:
            return m
        if not bearish and c["close"] > c["open"]:
            return m
    return None


def _zone_bounds(candle, mode):
    if mode == "body":
        return max(candle["open"], candle["close"]), min(candle["open"], candle["close"])
    return candle["high"], candle["low"]


def update_zone_statuses(zones, candles):
    """Apply mitigation / invalidation rules to zones using COMPLETED candles
    after each zone's confirmation candle. Mutates and returns `zones`.

    Documented rules (first event in chronological order wins):
      * mitigated: price trades back into the zone (a later candle's
        low <= top and high >= bottom). Revisiting the zone is a revisit,
        not proof that the orders were filled.
      * invalidated: a later candle CLOSES beyond the far boundary
        (bullish: close < bottom; bearish: close > top).
    """
    for z in zones:
        j = z.get("_ci")
        if j is None or j + 1 >= len(candles):
            continue
        z["status"] = "active"
        z["status_time"] = None
        for k in range(j + 1, len(candles)):
            c = candles[k]
            hit_zone = c["low"] <= z["top"] and c["high"] >= z["bottom"]
            if z["direction"] == "bullish":
                if c["close"] < z["bottom"]:
                    z["status"], z["status_time"] = "invalidated", c["time"]
                    break
                if hit_zone:
                    z["status"], z["status_time"] = "mitigated", c["time"]
                    break
            else:
                if c["close"] > z["top"]:
                    z["status"], z["status_time"] = "invalidated", c["time"]
                    break
                if hit_zone:
                    z["status"], z["status_time"] = "mitigated", c["time"]
                    break
    return zones


# --------------------------------------------------------------------------
# price-action liquidity pools (inferred levels - not resting orders)
# --------------------------------------------------------------------------

def equal_levels(points, tolerance, min_count=2):
    """Greedy price clustering of swing points within `tolerance` (instrument
    points). Returns groups of >= min_count with the mean level price."""
    if not points:
        return []
    ordered = sorted(points, key=lambda p: p["price"])
    groups, current = [], [ordered[0]]
    for p in ordered[1:]:
        if p["price"] - current[-1]["price"] <= tolerance:
            current.append(p)
        else:
            groups.append(current)
            current = [p]
    groups.append(current)
    out = []
    for g in groups:
        if len(g) >= min_count:
            out.append({
                "kind": g[0]["kind"],
                "price": sum(p["price"] for p in g) / len(g),
                "count": len(g),
                "times": [p["time"] for p in g],
                "last_time": max(p["time"] for p in g),
            })
    return out


def session_levels(candles, now_epoch=None):
    """Window high/low over the supplied completed candles plus the current
    UTC-session (UTC calendar day) high/low when candles cover it."""
    if not candles:
        return {}
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    hi_c = max(candles, key=lambda c: c["high"])
    lo_c = min(candles, key=lambda c: c["low"])
    out = {
        "window_high": hi_c["high"], "window_high_time": hi_c["time"],
        "window_low": lo_c["low"], "window_low_time": lo_c["time"],
    }
    day = None
    if now_epoch is not None:
        day = datetime.fromtimestamp(now_epoch, tz=timezone.utc).date()
    elif candles:
        day = datetime.fromtimestamp(candles[-1]["time"], tz=timezone.utc).date()
    if day is not None:
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()
        day_candles = [c for c in candles if c["time"] >= start]
        if day_candles:
            dh = max(day_candles, key=lambda c: c["high"])
            dl = min(day_candles, key=lambda c: c["low"])
            out.update({
                "session_high": dh["high"], "session_high_time": dh["time"],
                "session_low": dl["low"], "session_low_time": dl["time"],
            })
    return out


def detect_sweeps(candles, levels, reclaim_bars=3, eps=0.5):
    """Confirmed liquidity sweeps only.

    For a high level: a later candle must trade ABOVE the level (high > level
    and high >= level + eps to avoid ties on the level itself) and then close
    back below the level within `reclaim_bars` candles. Symmetric for lows.
    A mere touch or break without reclaim is NOT a sweep.
    `levels`: list of dicts with kind "high"/"low", price, time.
    """
    swept = []
    for lv in levels:
        kind, level, t0 = lv["kind"], lv["price"], lv["time"]
        start_idx = None
        for idx, c in enumerate(candles):
            if c["time"] > t0:
                start_idx = idx
                break
        if start_idx is None:
            continue
        exceed_idx = None
        for idx in range(start_idx, len(candles)):
            c = candles[idx]
            if kind == "high":
                if c["high"] >= level + eps:
                    exceed_idx = idx
                    break
            else:
                if c["low"] <= level - eps:
                    exceed_idx = idx
                    break
        if exceed_idx is None:
            continue
        for idx in range(exceed_idx, min(exceed_idx + reclaim_bars, len(candles))):
            c = candles[idx]
            high_sweep = kind == "high" and c["close"] < level
            low_sweep = kind == "low" and c["close"] > level
            if high_sweep or low_sweep:
                first_breach_candle = candles[exceed_idx]
                breach_window = candles[exceed_idx:idx + 1]
                breach_candle = (
                    max(breach_window, key=lambda row: row["high"])
                    if kind == "high"
                    else min(breach_window, key=lambda row: row["low"])
                )
                breach_price = breach_candle["high"] if kind == "high" else breach_candle["low"]
                overshoot = (
                    breach_price - level if kind == "high" else level - breach_price
                )
                swept.append({
                    **lv,
                    "sweep_time": c["time"],
                    "exceed_time": first_breach_candle["time"],
                    "breach_extreme_time": breach_candle["time"],
                    "breach_price": float(breach_price),
                    "overshoot_points": max(0.0, float(overshoot)),
                    "reclaim_delay_bars": idx - exceed_idx,
                    "reclaim_close": float(c["close"]),
                })
                break
    return swept


# --------------------------------------------------------------------------
# observed order-book liquidity (caller supplies the synchronized book)
# --------------------------------------------------------------------------

def is_price(value: Any) -> bool:
    """True when value is a finite, positive price (guards chart coordinates).

    Missing values, NaN/inf and non-price metrics (sizes, volumes, thresholds)
    must never be used as a price-axis coordinate.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0.0


def price_axis_range(lows, highs, extras=(), pad_frac: float = 0.06,
                     extend_frac: float = 0.25, min_pad: float = 0.0):
    """Price-chart y-axis range from valid candle lows/highs only.

    Returns (bottom, top), or None when there is no valid price data.
    Missing, non-finite and non-positive values are ignored, so an auxiliary
    zero or any non-price metric can never drag the axis to zero. `extras`
    (e.g. nearby zone/liquidity prices) only widen the range when they lie
    within extend_frac of the data span; distant values are ignored. The
    result stays dynamic as prices move and the timeframe changes; nothing is
    hard-coded.
    """
    valid_lows = [float(x) for x in lows if is_price(x)]
    valid_highs = [float(x) for x in highs if is_price(x)]
    if not valid_lows or not valid_highs:
        return None
    lo, hi = min(valid_lows), max(valid_highs)
    span = hi - lo
    if extras:
        near = [float(x) for x in extras
                if is_price(x) and lo - span * extend_frac <= float(x) <= hi + span * extend_frac]
        if near:
            lo, hi = min(lo, min(near)), max(hi, max(near))
            span = hi - lo
    pad = max(span * pad_frac, min_pad) if span > 0 else max(abs(lo) * 0.001, 0.5)
    return (lo - pad, hi + pad)


def band_levels(levels, tick, band_ticks=2):
    """Aggregate adjacent price levels into bands.

    A band starts at its first (lowest) price and extends at most
    band_ticks * tick above it; the next level beyond that starts a new band.
    Anchoring to the band START - not to the previous level - prevents an
    entire contiguous 0.5-tick grid from collapsing into one giant band whose
    summed size would dominate every adaptive threshold.

    `levels`: iterable of (price, size). Returns band dicts ordered by price.
    """
    if not levels or tick <= 0:
        return []
    band = float(tick) * max(1, int(band_ticks))
    ordered = sorted(levels, key=lambda x: x[0])
    groups, current = [], [ordered[0]]
    band_start = ordered[0][0]
    for price, size in ordered[1:]:
        if price - band_start <= band:
            current.append((price, size))
        else:
            groups.append(current)
            current = [(price, size)]
            band_start = price
    groups.append(current)
    out = []
    for g in groups:
        anchor = max(g, key=lambda x: x[1])
        out.append({
            "price": anchor[0],
            "low": min(p for p, _ in g),
            "high": max(p for p, _ in g),
            "size": sum(s for _, s in g),
            "levels": len(g),
        })
    return out


def relevant_bands(bands, ref_price, max_dev_pct: float = 10.0):
    """Keep bands intersecting a materiality window around ref_price.

    Order books list levels far from the market (deep in/out-of-the-money
    levels). Those are real data but not material to the traded price and must
    not pollute adaptive thresholds, liquidity tables or the price chart.
    """
    if not bands or not is_price(ref_price):
        return []
    lo = float(ref_price) * (1.0 - max_dev_pct / 100.0)
    hi = float(ref_price) * (1.0 + max_dev_pct / 100.0)
    return [b for b in bands if b["high"] >= lo and b["low"] <= hi]


def significant_bands(bands, pct, min_samples=10):
    """Split bands into (significant, threshold). Threshold is the adaptive
    percentile of band sizes; None when not enough bands exist."""
    if not bands:
        return [], None
    thr = adaptive_threshold([b["size"] for b in bands], pct, min_samples)
    if thr is None:
        return [], None
    sig = [b for b in bands if b["size"] >= thr]
    sig.sort(key=lambda b: b["size"], reverse=True)
    return sig, thr


# --------------------------------------------------------------------------
# executed-trade clusters
# --------------------------------------------------------------------------

def detect_clusters(trades, window_s=60.0):
    """Group consecutive same-side large trades whose gaps are <= window_s.

    Input: chronologically ordered trade dicts (already filtered to 'large').
    Returns cluster dicts with side, start/end ts, count, total contracts,
    price range and the first execution price.
    """
    clusters = []
    for t in trades:
        side = t["side"]
        if clusters:
            last = clusters[-1]
            if last["side"] == side and t["ts"] - last["end_ts"] <= window_s:
                last["end_ts"] = t["ts"]
                last["count"] += 1
                last["total_size"] += t["size"]
                last["prices"].append(t["price"])
                continue
        clusters.append({
            "side": side, "start_ts": t["ts"], "end_ts": t["ts"],
            "count": 1, "total_size": t["size"], "prices": [t["price"]],
        })
    for c in clusters:
        c["price_start"] = c["prices"][0]
        c["price_min"] = min(c["prices"])
        c["price_max"] = max(c["prices"])
    return clusters


def rolling_aggressor_metrics(trades, window_s, now_ts=None):
    """Rolling buy/sell aggressor volume, net and CVD over `trades`
    (chronological dicts with ts/side/size) within `window_s` ending at
    now_ts (default: last trade ts)."""
    if not trades:
        return {"buy_contracts": 0.0, "sell_contracts": 0.0,
                "net_contracts": 0.0, "cvd_contracts": 0.0, "count": 0}
    end = now_ts if now_ts is not None else trades[-1]["ts"]
    start = end - window_s
    buy = sell = 0.0
    cvd = 0.0
    count = 0
    for t in trades:
        if t["ts"] < start:
            continue
        count += 1
        if t["side"] == "buy":
            buy += t["size"]
            cvd += t["size"]
        else:
            sell += t["size"]
            cvd -= t["size"]
    return {"buy_contracts": buy, "sell_contracts": sell,
            "net_contracts": buy - sell, "cvd_contracts": cvd, "count": count}

# --------------------------------------------------------------------------
# Exhaustion labels (price rejection + aggressor-flow confirmation)
# --------------------------------------------------------------------------

def _epoch_seconds_for_flow(value: Any) -> float | None:
    """Normalize epoch timestamps and datetime-like values to seconds."""
    try:
        if hasattr(value, "timestamp") and callable(value.timestamp):
            number = float(value.timestamp())
        elif isinstance(value, str):
            text = value.strip()
            try:
                number = float(text)
            except ValueError:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                    return None
                number = float(dt.timestamp())
        else:
            number = float(value)
        if not math.isfinite(number) or number <= 0:
            return None
        if number > 1e17:       # nanoseconds
            number /= 1e9
        elif number > 1e14:     # microseconds
            number /= 1e6
        elif number > 1e11:     # milliseconds
            number /= 1e3
        return number
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def detect_exhaustion_labels(
    candles: list[dict[str, Any]],
    trades: list[Any],
    *,
    timeframe_seconds: int = 60,
    lookback: int = 10,
    min_trades: int = 5,
    imbalance_ratio: float = 1.25,
    min_wick_ratio: float = 0.35,
    close_position_limit: float = 0.65,
    diagnostics: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Identify potential buy/sell exhaustion from completed candles and tape.

    BUY EXHAUST: a candle makes a strict high above the preceding lookback bars,
    rejects that high with a material upper wick and weak close location, while
    buy-aggressor contracts dominate sell-aggressor contracts.

    SELL EXHAUST: the symmetric case at a strict new low, with a material lower
    wick, a stronger close location and sell-aggressor contracts dominating.

    Candles must be completed and in ascending time order. Trades accepts
    dictionaries or tuples of (timestamp, price, size, side), including the
    app's in-memory trade_log format. Dictionary rows may also contain a
    trade_count field to represent aggregate classified prints in a time bucket;
    size then represents that side's summed contract quantity, not one print.
    These are heuristic context labels, not guaranteed reversals or entry signals.
    """
    if timeframe_seconds < 1 or lookback < 1 or min_trades < 1:
        raise ValueError("timeframe_seconds, lookback, and min_trades must be positive")
    if not math.isfinite(imbalance_ratio) or imbalance_ratio < 1.0:
        raise ValueError("imbalance_ratio must be finite and >= 1")
    if not 0.0 < min_wick_ratio <= 1.0:
        raise ValueError("min_wick_ratio must be in (0, 1]")
    if not 0.0 < close_position_limit < 1.0:
        raise ValueError("close_position_limit must be in (0, 1)")

    def candle_epoch(candle: dict[str, Any]) -> int | None:
        ts = _epoch_seconds_for_flow(candle.get("time"))
        return int(ts) if ts is not None else None

    normalized_candles = []
    for candle in candles:
        ts = candle_epoch(candle)
        if ts is None:
            continue
        try:
            o, h, low, close = (float(candle[k]) for k in ("open", "high", "low", "close"))
            if not all(math.isfinite(v) for v in (o, h, low, close)):
                continue
            if low <= 0 or h < max(o, close) or low > min(o, close) or h <= low:
                continue
            normalized_candles.append({
                "time": ts, "open": o, "high": h, "low": low, "close": close,
            })
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    normalized_candles.sort(key=lambda c: c["time"])

    flow_by_bucket: dict[int, dict[str, float]] = {}
    for trade in trades:
        try:
            if isinstance(trade, dict):
                raw_ts = trade.get("ts", trade.get("timestamp", trade.get("ts_utc")))
                size = float(trade.get("size", 0.0))
                side = trade.get("side", trade.get("side_aggressor", ""))
                print_count = float(trade.get("trade_count", 1.0))
            else:
                raw_ts, _price, raw_size, side = trade[:4]
                size = float(raw_size)
                print_count = 1.0
            ts = _epoch_seconds_for_flow(raw_ts)
            if (ts is None or not math.isfinite(size) or size <= 0
                    or not math.isfinite(print_count) or print_count <= 0):
                continue
            normalized_side = str(side).strip().lower().replace("_", " ")
            if normalized_side in ("buy", "buy taker"):
                normalized_side = "buy"
            elif normalized_side in ("sell", "sell taker"):
                normalized_side = "sell"
            else:
                continue
            bucket = int(ts // timeframe_seconds) * timeframe_seconds
            totals = flow_by_bucket.setdefault(bucket, {"buy": 0.0, "sell": 0.0, "count": 0.0})
            totals[normalized_side] += size
            totals["count"] += print_count
        except (KeyError, TypeError, ValueError, OverflowError, IndexError):
            continue

    labels: list[dict[str, Any]] = []
    diag: dict[str, Any] = {
        "input_candles": len(candles),
        "valid_candles": len(normalized_candles),
        "flow_buckets": len(flow_by_bucket),
        "candidate_candles_in_flow_span": 0,
        "gap_windows": 0,
        "missing_flow_buckets": 0,
        "below_min_trade_count": 0,
        "usable_flow_candles": 0,
        "buy_new_high": 0,
        "buy_wick_pass": 0,
        "buy_close_pass": 0,
        "buy_flow_pass": 0,
        "buy_labels": 0,
        "sell_new_low": 0,
        "sell_wick_pass": 0,
        "sell_close_pass": 0,
        "sell_flow_pass": 0,
        "sell_labels": 0,
    }
    flow_times = list(flow_by_bucket)
    flow_start = min(flow_times) if flow_times else None
    flow_end = max(flow_times) if flow_times else None
    diag["flow_start"] = flow_start
    diag["flow_end"] = flow_end

    for i in range(lookback, len(normalized_candles)):
        candle = normalized_candles[i]
        # Older candle history without captured tape cannot be classified from
        # the live feed. Restrict rejection counters to the actual tape window.
        if flow_start is None or candle["time"] < flow_start or candle["time"] > flow_end:
            continue
        diag["candidate_candles_in_flow_span"] += 1
        previous = normalized_candles[i - lookback:i]
        # Gaps invalidate the local comparison window.
        if any(
            (normalized_candles[j]["time"] - normalized_candles[j - 1]["time"]) != timeframe_seconds
            for j in range(i - lookback + 1, i + 1)
        ):
            diag["gap_windows"] += 1
            continue

        flow = flow_by_bucket.get(candle["time"])
        if not flow:
            diag["missing_flow_buckets"] += 1
            continue
        if flow["count"] < min_trades:
            diag["below_min_trade_count"] += 1
            continue
        buy_qty, sell_qty = flow["buy"], flow["sell"]
        if buy_qty <= 0 and sell_qty <= 0:
            diag["missing_flow_buckets"] += 1
            continue
        diag["usable_flow_candles"] += 1

        o, h, low, close = candle["open"], candle["high"], candle["low"], candle["close"]
        candle_range = h - low
        upper_wick_ratio = (h - max(o, close)) / candle_range
        lower_wick_ratio = (min(o, close) - low) / candle_range
        close_position = (close - low) / candle_range
        prior_high = max(bar["high"] for bar in previous)
        prior_low = min(bar["low"] for bar in previous)

        buy_hit = False
        sell_hit = False
        if h > prior_high:
            diag["buy_new_high"] += 1
            if upper_wick_ratio >= min_wick_ratio:
                diag["buy_wick_pass"] += 1
                if close_position <= close_position_limit:
                    diag["buy_close_pass"] += 1
                    if buy_qty > 0 and buy_qty >= imbalance_ratio * sell_qty:
                        diag["buy_flow_pass"] += 1
                        buy_hit = True
        if low < prior_low:
            diag["sell_new_low"] += 1
            if lower_wick_ratio >= min_wick_ratio:
                diag["sell_wick_pass"] += 1
                if close_position >= 1.0 - close_position_limit:
                    diag["sell_close_pass"] += 1
                    if sell_qty > 0 and sell_qty >= imbalance_ratio * buy_qty:
                        diag["sell_flow_pass"] += 1
                        sell_hit = True

        # Preserve the original detector's single-label precedence when an
        # unusually wide candle breaks both prior extremes and both rules match.
        if buy_hit:
            labels.append({
                "kind": "buy_exhaust",
                "time": candle["time"],
                "price": h,
                "buy_contracts": buy_qty,
                "sell_contracts": sell_qty,
                "trade_count": int(flow["count"]),
                "wick_ratio": upper_wick_ratio,
                "close_position": close_position,
                "aggressor_ratio": buy_qty / sell_qty if sell_qty > 0 else None,
            })
        elif sell_hit:
            labels.append({
                "kind": "sell_exhaust",
                "time": candle["time"],
                "price": low,
                "buy_contracts": buy_qty,
                "sell_contracts": sell_qty,
                "trade_count": int(flow["count"]),
                "wick_ratio": lower_wick_ratio,
                "close_position": close_position,
                "aggressor_ratio": sell_qty / buy_qty if buy_qty > 0 else None,
            })

    diag["buy_labels"] = sum(1 for label in labels if label["kind"] == "buy_exhaust")
    diag["sell_labels"] = sum(1 for label in labels if label["kind"] == "sell_exhaust")
    if diagnostics is not None:
        diagnostics.clear()
        diagnostics.update(diag)
    return labels

