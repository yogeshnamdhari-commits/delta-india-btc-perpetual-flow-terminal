#!/usr/bin/env python3
"""Export logged, real trade events to the OOS replay CSV schema."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path


def epoch_seconds(value):
    """Normalize seconds, milliseconds, microseconds or nanoseconds to seconds."""
    try:
        n = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid epoch timestamp: {value!r}") from exc
    if not n.is_finite():
        raise ValueError(f"non-finite epoch timestamp: {value!r}")
    if n > Decimal(10**17):
        n /= Decimal(10**9)
    elif n > Decimal(10**14):
        n /= Decimal(10**6)
    elif n > Decimal(10**11):
        n /= Decimal(10**3)
    return n


def epoch_datetime(value):
    """Convert an epoch value to UTC without losing available fractional seconds."""
    seconds_decimal = epoch_seconds(value)
    seconds = int(seconds_decimal)
    microseconds = int((seconds_decimal - Decimal(seconds)) * Decimal(1_000_000))
    if not 0 <= microseconds < 1_000_000:
        raise ValueError(f"invalid fractional timestamp: {value!r}")
    return datetime.fromtimestamp(seconds, timezone.utc).replace(microsecond=microseconds)


def trade_datetime(event):
    """Resolve timestamp in priority order, preferring the raw exchange trade time."""
    raw_us = event.get("exchange_trade_ts_us")
    if raw_us not in (None, ""):
        try:
            raw_microseconds = int(raw_us)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"invalid exchange_trade_ts_us: {raw_us!r}") from exc
        if raw_microseconds <= 0:
            raise ValueError(f"invalid exchange_trade_ts_us: {raw_us!r}")
        seconds, microseconds = divmod(raw_microseconds, 1_000_000)
        return datetime.fromtimestamp(seconds, timezone.utc).replace(microsecond=microseconds)

    # Backward compatibility for older JSONL records. These timestamps may only
    # have whole-second precision; do not substitute ts_utc, which is capture time.
    for field in ("ts", "exchange_ts"):
        raw = event.get(field)
        if raw not in (None, ""):
            return epoch_datetime(raw)
    return None


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True)
    p.add_argument("--output", default="~/Downloads/delta_oos_data/trades.csv")
    a = p.parse_args()
    source = Path(a.input).expanduser()
    if not source.is_file():
        p.error(f"input file not found: {source}")

    rows = []
    unknown = bad = 0
    with source.open(encoding="utf-8") as fh:
        for line in fh:
            try:
                e = json.loads(line)
                if e.get("event") != "trade":
                    continue
                ts = trade_datetime(e)
                if ts is None:
                    bad += 1
                    continue
                price = float(e["price"])
                size = float(e["size"])
                role = str(e.get("buyer_role", "")).lower()
                side = "BUY_TAKER" if role == "t" else "SELL_TAKER" if role == "m" else None
                if side is None:
                    unknown += 1
                    continue
                if price <= 0 or size <= 0:
                    bad += 1
                    continue
                rows.append((ts, price, size, side))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError, OverflowError, OSError):
                bad += 1

    rows.sort(key=lambda x: x[0])
    target = Path(a.output).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ts_utc", "price", "size", "side_aggressor"])
        for ts, price, size, side in rows:
            w.writerow([ts.isoformat(), price, size, side])

    print(f"trades={len(rows):,}; unknown_side={unknown:,}; malformed={bad:,}; output={target}")
    print(f"range={rows[0][0].isoformat() if rows else 'n/a'} .. {rows[-1][0].isoformat() if rows else 'n/a'}")
    print(f"sha256={hashlib.sha256(target.read_bytes()).hexdigest()}")
    if not rows:
        print("WARNING: No real trade events found. No synthetic trades were written; the OOS report should block on insufficient evidence.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
