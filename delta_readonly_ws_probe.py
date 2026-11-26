#!/usr/bin/env python3
"""One-shot, read-only Delta India public market-data validation probe.

This script does not start/stop the application, mutate files, authenticate to
private channels, or submit orders. It opens one temporary public WebSocket,
checks the current market-data contract, then closes that socket.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WS_URL = "wss://public-socket.india.delta.exchange"
SYMBOL = "BTCUSD"
INDEX_SYMBOL = ".DEXBTUSD"
TICK_SIZE = 0.5


def epoch_seconds(value: Any) -> float | None:
    """Convert Delta seconds/ms/us/ns timestamps to Unix seconds."""
    try:
        number = float(value)
        if not math.isfinite(number) or number <= 0:
            return None
        if number >= 1e17:
            return number / 1e9
        if number >= 1e14:
            return number / 1e6
        if number >= 1e11:
            return number / 1e3
        return number
    except (TypeError, ValueError, OverflowError):
        return None


def apply_levels(book: dict[str, str], rows: Any) -> None:
    if not isinstance(rows, list):
        return
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        price, size = str(row[0]), str(row[1])
        try:
            quantity = float(size)
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(quantity) or quantity < 0:
            continue
        if quantity == 0:
            book.pop(price, None)
        else:
            book[price] = size


def book_checksum(asks: dict[str, str], bids: dict[str, str]) -> int:
    ask_levels = sorted(asks.items(), key=lambda item: float(item[0]))[:10]
    bid_levels = sorted(bids.items(), key=lambda item: float(item[0]), reverse=True)[:10]
    ask_text = ",".join(f"{price}:{size}" for price, size in ask_levels)
    bid_text = ",".join(f"{price}:{size}" for price, size in bid_levels)
    return zlib.crc32(f"{ask_text}|{bid_text}".encode("utf-8")) & 0xFFFFFFFF


def check_streamlit_health(url: str, timeout: float = 3.0) -> dict[str, Any]:
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "delta-readonly-validation/1.0"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(512).decode("utf-8", errors="replace")
            return {"ok": response.status == 200, "http_status": response.status, "body": body[:120]}
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def check_health_file(path: str | None, max_age_seconds: float = 45.0) -> dict[str, Any]:
    if not path:
        return {"configured": False, "ok": None, "note": "DELTA_FEED_HEALTH_FILE is not configured"}
    target = Path(path).expanduser()
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
        age = max(0.0, time.time() - float(payload.get("ts_epoch", 0.0)))
        checks = {
            "heartbeat_fresh": age <= max_age_seconds,
            "connection_connected": payload.get("connection") == "connected",
            "book_synced": payload.get("book_synced") is True,
            "no_unresolved_sequence_gap": payload.get("sequence_gap_unresolved") is False,
            "checksum_verified": payload.get("checksum_ok") is True,
            "websocket_worker_alive": payload.get("websocket_worker_alive") is True,
            "sampler_worker_alive": payload.get("sampler_worker_alive") is True,
        }
        return {
            "configured": True, "ok": all(checks.values()), "age_seconds": round(age, 3),
            "checks": checks, "pid": payload.get("pid"),
            "last_market_message_at": payload.get("last_market_message_at"),
            "channel_last_seen": payload.get("channel_last_seen", {}),
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return {"configured": True, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def run_probe(duration_seconds: float, streamlit_url: str,
              health_file: str | None) -> dict[str, Any]:
    import websocket

    stats: dict[str, Any] = {
        "connection": "not_started",
        "channel_messages": {},
        "book_snapshots": 0,
        "book_updates": 0,
        "checksum_verified": 0,
        "checksum_missing": 0,
        "checksum_failures": 0,
        "sequence_gaps": 0,
        "ob_l1_messages": 0,
        "l1_l2_comparison_samples": 0,
        "l1_l2_divergences": 0,
        "max_l1_l2_price_difference_ticks": 0.0,
        "trade_messages": 0,
        "valid_buyer_roles": 0,
        "unknown_buyer_roles": 0,
        "valid_microsecond_trade_timestamps": 0,
        "funding_messages": 0,
        "next_funding_realization_utc": None,
        "next_funding_seconds_from_now": None,
        "errors": [],
    }
    asks: dict[str, str] = {}
    bids: dict[str, str] = {}
    book_sequence: int | None = None
    book_verified = False
    l1_l2_differences: list[float] = []
    started_at = time.time()
    deadline = time.monotonic() + duration_seconds
    ws = None

    subscription = {
        "type": "subscribe",
        "payload": {
            "channels": [
                {"name": "ticker", "symbols": [SYMBOL]},
                {"name": "ob_l1", "symbols": [SYMBOL]},
                {"name": "ob_updates", "symbols": [SYMBOL]},
                {"name": "trades", "symbols": [SYMBOL]},
                {"name": "mark_price", "symbols": [f"MARK:{SYMBOL}"]},
                {"name": "funding_rate", "symbols": [SYMBOL]},
                {"name": "spot_price", "symbols": [INDEX_SYMBOL]},
                {"name": "candlestick_1m", "symbols": [SYMBOL]},
            ]
        },
    }

    try:
        ws = websocket.create_connection(WS_URL, timeout=3.0, enable_multithread=True)
        ws.settimeout(0.5)
        ws.send(json.dumps({"type": "enable_heartbeat"}))
        ws.send(json.dumps(subscription))
        stats["connection"] = "connected"
        while time.monotonic() < deadline:
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if not raw:
                continue
            try:
                message = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                stats["errors"].append("non_json_message")
                continue
            if not isinstance(message, dict):
                continue
            kind = message.get("type")
            if not isinstance(kind, str):
                continue
            stats["channel_messages"][kind] = stats["channel_messages"].get(kind, 0) + 1
            now = time.time()

            if kind == "ob_updates" and message.get("sy", SYMBOL) == SYMBOL:
                action = str(message.get("action", "")).lower()
                try:
                    incoming_sequence = int(message.get("seq"))
                except (TypeError, ValueError, OverflowError):
                    stats["sequence_gaps"] += 1
                    book_verified = False
                    continue
                if action == "snapshot":
                    asks, bids = {}, {}
                    apply_levels(asks, message.get("a", []))
                    apply_levels(bids, message.get("b", []))
                    stats["book_snapshots"] += 1
                    book_sequence = incoming_sequence
                elif action == "update":
                    if book_sequence is None or incoming_sequence != book_sequence + 1:
                        stats["sequence_gaps"] += 1
                        book_verified = False
                        continue
                    apply_levels(asks, message.get("a", []))
                    apply_levels(bids, message.get("b", []))
                    book_sequence = incoming_sequence
                    stats["book_updates"] += 1
                else:
                    continue
                raw_checksum = message.get("cs")
                try:
                    expected_checksum = int(raw_checksum) if raw_checksum is not None else None
                except (TypeError, ValueError, OverflowError):
                    expected_checksum = None
                if expected_checksum is None:
                    stats["checksum_missing"] += 1
                    book_verified = False
                else:
                    actual_checksum = book_checksum(asks, bids)
                    if actual_checksum == expected_checksum:
                        stats["checksum_verified"] += 1
                        book_verified = True
                    else:
                        stats["checksum_failures"] += 1
                        book_verified = False

            elif kind == "ob_l1" and message.get("sy") == SYMBOL:
                try:
                    l1_ask = float(message.get("ap"))
                    l1_bid = float(message.get("bp"))
                    l1_ask_size = float(message.get("as"))
                    l1_bid_size = float(message.get("bs"))
                    if not (l1_bid > 0 and l1_ask > l1_bid and l1_ask_size > 0 and l1_bid_size > 0):
                        raise ValueError("invalid ob_l1 quote")
                    stats["ob_l1_messages"] += 1
                    if book_verified and asks and bids:
                        l2_ask = min(float(price) for price, size in asks.items() if float(size) > 0)
                        l2_bid = max(float(price) for price, size in bids.items() if float(size) > 0)
                        difference_ticks = max(abs(l1_bid - l2_bid), abs(l1_ask - l2_ask)) / TICK_SIZE
                        l1_l2_differences.append(difference_ticks)
                        stats["l1_l2_comparison_samples"] += 1
                        if difference_ticks > 1.000001:
                            stats["l1_l2_divergences"] += 1
                        stats["max_l1_l2_price_difference_ticks"] = max(
                            stats["max_l1_l2_price_difference_ticks"], difference_ticks
                        )
                except (TypeError, ValueError, OverflowError):
                    stats["errors"].append("invalid_ob_l1_quote")

            elif kind == "trades" and message.get("sy") == SYMBOL:
                stats["trade_messages"] += 1
                role = str(message.get("r", "")).lower()
                if role in ("t", "m"):
                    stats["valid_buyer_roles"] += 1
                else:
                    stats["unknown_buyer_roles"] += 1
                trade_raw, publish_raw = message.get("t"), message.get("ts")
                trade_ts, publish_ts = epoch_seconds(trade_raw), epoch_seconds(publish_raw)
                if (isinstance(trade_raw, (int, float)) and trade_raw >= 1e14
                        and isinstance(publish_raw, (int, float)) and publish_raw >= 1e14
                        and trade_ts is not None and publish_ts is not None
                        and abs(now - trade_ts) <= 120 and abs(now - publish_ts) <= 120):
                    stats["valid_microsecond_trade_timestamps"] += 1

            elif kind == "funding_rate" and message.get("sy") == SYMBOL:
                stats["funding_messages"] += 1
                realization = epoch_seconds(message.get("nfr"))
                if realization is not None and realization > now:
                    stats["next_funding_realization_utc"] = datetime.fromtimestamp(
                        realization, timezone.utc
                    ).isoformat()
                    stats["next_funding_seconds_from_now"] = round(realization - now, 2)

        stats["book_sequence_final"] = book_sequence
        stats["book_verified_at_end"] = book_verified
        stats["trade_window_seconds"] = round(time.time() - started_at, 2)
        if l1_l2_differences:
            stats["l1_l2_mismatch_fraction"] = round(
                sum(value > 1.000001 for value in l1_l2_differences) / len(l1_l2_differences), 4
            )
        else:
            stats["l1_l2_mismatch_fraction"] = None
    except Exception as exc:
        stats["connection"] = "failed"
        stats["errors"].append(f"{type(exc).__name__}: {exc}")
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    stats["streamlit_health"] = check_streamlit_health(streamlit_url)
    stats["feed_health_file"] = check_health_file(health_file)
    mismatch_fraction = stats.get("l1_l2_mismatch_fraction")
    checks = {
        "public_socket_connected": stats["connection"] == "connected",
        "ticker_seen": stats["channel_messages"].get("ticker", 0) > 0,
        "ob_l1_seen": stats["ob_l1_messages"] > 0,
        "ob_updates_snapshot_seen": stats["book_snapshots"] > 0,
        "book_crc_verified": stats["checksum_verified"] > 0
            and stats["checksum_missing"] == 0 and stats["checksum_failures"] == 0,
        "no_orderbook_sequence_gaps": stats["sequence_gaps"] == 0,
        "l1_l2_top_of_book_compared": stats["l1_l2_comparison_samples"] > 0,
        "l1_l2_top_of_book_consistent": mismatch_fraction is not None and mismatch_fraction <= 0.10,
        "classified_trade_seen": stats["valid_buyer_roles"] > 0,
        "microsecond_trade_timestamps_verified": stats["valid_microsecond_trade_timestamps"] > 0,
        "funding_next_realization_available": stats["next_funding_realization_utc"] is not None,
        "existing_streamlit_health_http_200": stats["streamlit_health"].get("ok") is True,
    }
    if stats["feed_health_file"].get("configured"):
        checks["existing_feed_health_heartbeat_ok"] = stats["feed_health_file"].get("ok") is True
    stats["checks"] = checks
    stats["overall_pass"] = all(checks.values())
    stats["safety"] = {
        "private_channels_subscribed": False,
        "orders_submitted": False,
        "project_files_modified": False,
        "existing_processes_restarted": False,
        "probe_socket_closed": ws is None or not getattr(ws, "connected", False),
    }
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=30.0,
                        help="Public market-data observation window in seconds (default: 30)")
    parser.add_argument("--streamlit-url", default="http://127.0.0.1:8502/_stcore/health")
    parser.add_argument("--health-file", default=os.environ.get("DELTA_FEED_HEALTH_FILE"))
    args = parser.parse_args()
    if not math.isfinite(args.duration) or not 10.0 <= args.duration <= 120.0:
        parser.error("--duration must be between 10 and 120 seconds")
    report = run_probe(args.duration, args.streamlit_url, args.health_file)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("overall_pass") else 1


if __name__ == "__main__":
    raise SystemExit(main())
