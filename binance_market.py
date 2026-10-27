"""Read-only Binance USDⓈ-M BTCUSDT public market-data adapter.

This adapter is intentionally separate from the Delta-specific signal pipeline.
It normalizes public trades, candles, depth, mark/funding, OI and observed
liquidation snapshots without requiring credentials or offering order entry.
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

import requests
import websocket

_LOGGER = logging.getLogger(__name__)
REST_BASE_URL = "https://fapi.binance.com"
WS_BASE_URL = "wss://fstream.binance.com"
DEFAULT_SYMBOL = "BTCUSDT"
_MAX_CANDLES = 2000
_MAX_BOOK_LEVELS = 5000
_MAX_DEPTH_BUFFER = 10000


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _positive(value: Any) -> float | None:
    result = _number(value)
    return result if result is not None and result > 0 else None


def _seconds_from_ms(value: Any) -> float | None:
    result = _number(value)
    return result / 1000.0 if result is not None and result > 0 else None


def normalize_agg_trade(event: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize Binance aggTrade; m=True means buyer is maker, so aggressor sold."""
    price = _positive(event.get("p"))
    qty_btc = _positive(event.get("q"))
    timestamp = _seconds_from_ms(event.get("T") or event.get("E"))
    if price is None or qty_btc is None or timestamp is None:
        return None
    buyer_is_maker = event.get("m")
    if not isinstance(buyer_is_maker, bool):
        return None
    return {
        "timestamp": timestamp,
        "price": price,
        "size_btc": qty_btc,
        "size": qty_btc,
        "side": "SELL" if buyer_is_maker else "BUY",
        "buyer_role": "maker" if buyer_is_maker else "taker",
        "notional_usd_approx": price * qty_btc,
        "trade_id": event.get("a"),
        "source": "binance_usdm_aggTrade",
    }


def normalize_rest_kline(row: Any) -> dict[str, Any] | None:
    """Normalize a REST /fapi/v1/klines row."""
    if not isinstance(row, (list, tuple)) or len(row) < 6:
        return None
    timestamp = _seconds_from_ms(row[0])
    o, h, low, close, volume = (_number(row[i]) for i in (1, 2, 3, 4, 5))
    if timestamp is None or any(v is None for v in (o, h, low, close, volume)):
        return None
    if min(o, h, low, close) <= 0 or volume < 0 or h < max(o, low, close) or low > min(o, h, close):
        return None
    return {
        "time": int(timestamp // 60 * 60),
        "open": o, "high": h, "low": low, "close": close,
        "volume": volume,
        "quote_volume": _number(row[7]) if len(row) > 7 else None,
        "is_closed": True,
    }


def normalize_kline_event(event: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize Binance WebSocket kline events. The current candle may be unclosed."""
    kline = event.get("k")
    if not isinstance(kline, dict):
        return None
    timestamp = _seconds_from_ms(kline.get("t"))
    o, h, low, close, volume = (
        _number(kline.get(key)) for key in ("o", "h", "l", "c", "v")
    )
    if timestamp is None or any(v is None for v in (o, h, low, close, volume)):
        return None
    if min(o, h, low, close) <= 0 or volume < 0 or h < max(o, low, close) or low > min(o, h, close):
        return None
    return {
        "time": int(timestamp // 60 * 60),
        "open": o, "high": h, "low": low, "close": close,
        "volume": volume,
        "quote_volume": _number(kline.get("q")),
        "is_closed": bool(kline.get("x", False)),
    }


def normalize_liquidation_event(event: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize an actual Binance force-order snapshot with reported filled size.

    Original order quantity (q) is deliberately not used as executed size.
    The Binance stream is a snapshot feed; these are observed messages, not a
    complete market-wide census or a prediction of future liquidation prices.
    """
    order = event.get("o")
    if not isinstance(order, dict):
        return None
    price = _positive(order.get("ap")) or _positive(order.get("p"))
    # l is the last filled quantity. Do not aggregate z (cumulative fill) across
    # snapshots: doing so could count the same earlier fills more than once.
    # Never use q (original order quantity) as if it were executed.
    qty_btc = _positive(order.get("l"))
    timestamp = _seconds_from_ms(order.get("T") or event.get("E"))
    side = str(order.get("S", "")).upper()
    symbol = str(order.get("s", event.get("s", ""))).upper()
    if price is None or qty_btc is None or timestamp is None or side not in {"BUY", "SELL"}:
        return None
    if not symbol:
        return None
    return {
        "timestamp": timestamp,
        "symbol": symbol,
        "side": side,
        "price": price,
        "size_btc": qty_btc,
        "notional_usd_approx": price * qty_btc,
        "order_status": order.get("X"),
        "trade_time_ms": order.get("T"),
        "event_time_ms": event.get("E"),
        "source": "binance_usdm_forceOrder_snapshot",
    }


def depth_update_action(
    last_update_id: int,
    event: dict[str, Any],
    *,
    require_previous: bool = False,
) -> str:
    """Classify Binance USDⓈ-M diff-depth data using update IDs and pu.

    The first buffered event bridges a REST snapshot. After the bridge, each
    event must carry pu equal to the locally applied previous u.
    """
    try:
        first_id, final_id = int(event["U"]), int(event["u"])
    except (KeyError, TypeError, ValueError):
        return "gap"
    if final_id <= last_update_id:
        return "stale"
    if require_previous:
        try:
            previous_id = int(event["pu"])
        except (KeyError, TypeError, ValueError):
            return "gap"
        if previous_id != last_update_id:
            return "gap"
    elif first_id > last_update_id + 1:
        return "gap"
    return "apply"


def _apply_depth_levels(book: dict[float, float], updates: Any, *, keep_high: bool) -> None:
    if not isinstance(updates, list):
        return
    for item in updates:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        price, qty = _positive(item[0]), _number(item[1])
        if price is None or qty is None or qty < 0:
            continue
        if qty == 0:
            book.pop(price, None)
        else:
            book[price] = qty
    if len(book) > _MAX_BOOK_LEVELS:
        # Retain the best-priced depth: highest bid prices, lowest ask prices.
        ordered = sorted(book, reverse=keep_high)
        for price in ordered[_MAX_BOOK_LEVELS:]:
            book.pop(price, None)


def aggregate_observed_liquidations(
    events: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    now_epoch: float | None = None,
    window_seconds: int = 3600,
    bucket_width: float = 100.0,
) -> list[dict[str, Any]]:
    """Aggregate observed filled force-order snapshots by historical price bucket.

    This is a backward-looking aggregation of feed messages, not a liquidation
    heatmap and not an estimate of untriggered liquidation prices.
    """
    now = time.time() if now_epoch is None else float(now_epoch)
    width = _positive(bucket_width)
    if width is None or window_seconds <= 0:
        return []
    buckets: dict[tuple[float, str], dict[str, Any]] = {}
    for row in events:
        if not isinstance(row, dict):
            continue
        ts = _number(row.get("timestamp"))
        price = _positive(row.get("price"))
        qty = _positive(row.get("size_btc"))
        side = str(row.get("side", "")).upper()
        if ts is None or price is None or qty is None or side not in {"BUY", "SELL"}:
            continue
        if ts < now - window_seconds or ts > now + 5:
            continue
        bucket_low = math.floor(price / width) * width
        key = (bucket_low, side)
        agg = buckets.setdefault(key, {
            "price_bucket_low": bucket_low,
            "price_bucket_high": bucket_low + width,
            "order_side": side,
            "observed_snapshot_count": 0,
            "filled_qty_btc_reported": 0.0,
            "observed_notional_usd_approx": 0.0,
        })
        agg["observed_snapshot_count"] += 1
        agg["filled_qty_btc_reported"] += qty
        agg["observed_notional_usd_approx"] += price * qty
    return sorted(
        buckets.values(),
        key=lambda r: (r["observed_notional_usd_approx"], r["observed_snapshot_count"]),
        reverse=True,
    )


class BinanceUSDMFeed:
    """Read-only normalized public feed for Binance USDⓈ-M BTCUSDT perpetual."""

    def __init__(
        self,
        symbol: str = DEFAULT_SYMBOL,
        *,
        rest_base_url: str = REST_BASE_URL,
        ws_base_url: str = WS_BASE_URL,
        request_timeout: float = 8.0,
    ):
        symbol = str(symbol).strip().upper()
        if not symbol.isalnum() or len(symbol) < 5:
            raise ValueError("symbol must be a valid Binance USDⓈ-M symbol such as BTCUSDT")
        self.symbol = symbol
        self.rest_base_url = rest_base_url.rstrip("/")
        self.ws_base_url = ws_base_url
        self.request_timeout = float(request_timeout)
        now = time.time()
        self.lock = threading.RLock()
        self.data: dict[str, Any] = {
            "provider": "Binance USDⓈ-M Futures",
            "symbol": symbol,
            "connection": "starting",
            "market_connection": "starting",
            "depth_connection": "starting",
            "last_error": "",
            "last_rest_error": "",
            "started_at": now,
            "last_any_message_at": None,
            "last_market_message_at": None,
            "last_price": None,
            "mark_price": None,
            "index_price": None,
            "bid": None,
            "ask": None,
            "bid_size_btc": None,
            "ask_size_btc": None,
            "funding_rate": None,
            "funding_interval_seconds": None,
            "next_funding_at": None,
            "open_interest_btc": None,
            "open_interest_usd_approx": None,
            "ticker_24h": {},
            "bids": {},
            "asks": {},
            "book_last_update_id": None,
            "book_synced": False,
            "sequence_gap_unresolved": True,
            "book_sync_error": "Waiting for depth snapshot",
            # Binance depth has update IDs; do not claim a Delta CRC32 checksum.
            "checksum_ok": None,
            "checksum_msg": "Not provided by this venue; update-ID continuity is used instead",
            "sequence_gaps": 0,
            "total_messages_received": 0,
            "channel_last_seen": {},
            "channel_message_counts": {},
            "total_disconnects": 0,
            "total_reconnects": 0,
            "trades": deque(maxlen=500),
            "candles": {},
            "cvd": deque(maxlen=5000),
            "cvd_value_btc": 0.0,
            "buy_volume_btc": 0.0,
            "sell_volume_btc": 0.0,
            "trade_count": 0,
            "liquidations": deque(maxlen=5000),
            "last_rest_poll_at": None,
            "open_interest_updated_at": None,
            "ticker_24h_updated_at": None,
        }
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._depth_thread: threading.Thread | None = None
        self._rest_thread: threading.Thread | None = None
        self._book_sync_thread: threading.Thread | None = None
        self._book_sync_running = False
        self._depth_buffer: deque[dict[str, Any]] = deque(maxlen=_MAX_DEPTH_BUFFER)
        self._depth_buffer_overflow = False
        self._ws: websocket.WebSocketApp | None = None
        self._ws_depth: websocket.WebSocketApp | None = None
        self._liquidation_seen: set[tuple[Any, ...]] = set()
        self._ever_connected: dict[str, bool] = {"market": False, "depth": False}
        self._liquidation_seen_order: deque[tuple[Any, ...]] = deque(maxlen=10000)

    def start(self) -> None:
        if (
            self._thread and self._thread.is_alive()
            and self._depth_thread and self._depth_thread.is_alive()
        ):
            return
        self._stop.clear()
        if not (self._thread and self._thread.is_alive()):
            self._thread = threading.Thread(
                target=self._run_forever, args=("market",),
                name=f"binance-{self.symbol.lower()}-market-ws", daemon=True
            )
            self._thread.start()
        if not (self._depth_thread and self._depth_thread.is_alive()):
            self._depth_thread = threading.Thread(
                target=self._run_forever, args=("depth",),
                name=f"binance-{self.symbol.lower()}-depth-ws", daemon=True
            )
            self._depth_thread.start()
        if not (self._rest_thread and self._rest_thread.is_alive()):
            self._rest_thread = threading.Thread(
                target=self._poll_rest, name=f"binance-{self.symbol.lower()}-rest", daemon=True
            )
            self._rest_thread.start()

    def stop(self) -> None:
        self._stop.set()
        for ws in (self._ws, self._ws_depth):
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            result = dict(self.data)
            result["bids"] = dict(self.data["bids"])
            result["asks"] = dict(self.data["asks"])
            result["trades"] = [dict(x) for x in reversed(self.data["trades"])]
            result["candles"] = [dict(x) for _, x in sorted(self.data["candles"].items())]
            result["cvd"] = list(self.data["cvd"])
            result["liquidations"] = [dict(x) for x in reversed(self.data["liquidations"])]
            result["ticker_24h"] = dict(self.data["ticker_24h"])
            result["channel_last_seen"] = dict(self.data["channel_last_seen"])
            result["channel_message_counts"] = dict(self.data["channel_message_counts"])
            return result

    def analyst_context(self, *, now_epoch: float | None = None) -> dict[str, Any]:
        """Return a compact, JSON-safe context for the read-only AI analyst."""
        now = time.time() if now_epoch is None else float(now_epoch)
        snap = self.snapshot()
        last_price = _positive(snap.get("last_price"))
        mark_price = _positive(snap.get("mark_price"))
        bid = _positive(snap.get("bid"))
        ask = _positive(snap.get("ask"))
        mid = (bid + ask) / 2 if bid and ask and ask >= bid else None
        spread = ask - bid if bid and ask and ask >= bid else None
        events = snap.get("liquidations", [])
        ages = {}
        for channel, ts in snap.get("channel_last_seen", {}).items():
            ages[channel] = round(max(0.0, now - ts), 3)
        bids = sorted(
            ((float(p), float(q)) for p, q in snap.get("bids", {}).items()),
            reverse=True,
        )[:5]
        asks = sorted(
            ((float(p), float(q)) for p, q in snap.get("asks", {}).items())
        )[:5]
        return {
            "venue": snap.get("provider"),
            "symbol": snap.get("symbol"),
            "observed_at_utc": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "connection": snap.get("connection"),
            "last_message_age_seconds": (
                round(max(0.0, now - snap["last_any_message_at"]), 3)
                if snap.get("last_any_message_at") else None
            ),
            "channel_age_seconds": ages,
            "price": {
                "last_trade": last_price,
                "mark": mark_price,
                "index": _positive(snap.get("index_price")),
                "best_bid": bid,
                "best_ask": ask,
                "mid_calculated": mid,
                "spread_calculated": spread,
                "spread_bps_calculated": spread / mid * 10000 if mid else None,
            },
            "funding_rate": _number(snap.get("funding_rate")),
            "next_funding_time_utc": (
                datetime.fromtimestamp(snap["next_funding_at"], tz=timezone.utc).isoformat()
                if snap.get("next_funding_at") else None
            ),
            "open_interest_btc": _number(snap.get("open_interest_btc")),
            "open_interest_usd_approx": _number(snap.get("open_interest_usd_approx")),
            "order_book": {
                "synced_by_update_ids": bool(snap.get("book_synced")),
                "last_update_id": snap.get("book_last_update_id"),
                "sequence_gap_unresolved": bool(snap.get("sequence_gap_unresolved", True)),
                "checksum": "Not supplied by Binance USDⓈ-M in this feed; do not infer CRC32 validity",
                "bids": [{"price": p, "size_btc": q} for p, q in bids],
                "asks": [{"price": p, "size_btc": q} for p, q in asks],
            },
            "session_trade_flow": {
                "trade_count": int(snap.get("trade_count") or 0),
                "buy_aggressor_volume_btc": _number(snap.get("buy_volume_btc")),
                "sell_aggressor_volume_btc": _number(snap.get("sell_volume_btc")),
                "session_cvd_btc": _number(snap.get("cvd_value_btc")),
                "warning": "Session-only aggTrade aggressor-volume proxy; resets on restart and is not historical CVD.",
            },
            "recent_liquidation_snapshots": [
                {
                    "time_utc": datetime.fromtimestamp(float(row["timestamp"]), tz=timezone.utc).isoformat(),
                    "order_side": row.get("side"),
                    "price": _number(row.get("price")),
                    "filled_qty_btc_reported": _number(row.get("size_btc")),
                    "observed_notional_usd_approx": _number(row.get("notional_usd_approx")),
                }
                for row in events[:8]
                if isinstance(row, dict) and _number(row.get("timestamp")) is not None
            ],
            "liquidation_data_limitations": [
                "Binance forceOrder is a per-symbol liquidation-order snapshot stream that emits at most the latest order per 1-second interval; it is not a complete market-wide census.",
                "Displayed price buckets aggregate observed historical messages only. They are not future liquidation levels, untriggered-position estimates, or predictive heatmaps.",
            ],
        }

    def _refresh_connection_state_locked(self) -> None:
        market = self.data.get("market_connection", "unknown")
        depth = self.data.get("depth_connection", "unknown")
        if market == "connected" and depth == "connected":
            self.data["connection"] = "connected"
        else:
            self.data["connection"] = f"market={market}; depth={depth}"

    def _set_connection(
        self, value: str, error: str | None = None, *, group: str = "market"
    ) -> None:
        key = "depth_connection" if group == "depth" else "market_connection"
        with self.lock:
            self.data[key] = value
            self._refresh_connection_state_locked()
            if error is not None:
                self.data["last_error"] = str(error)[:400]

    def _stream_url(self, group: str = "market") -> str:
        symbol = self.symbol.lower()
        root = self.ws_base_url.rstrip("/")
        if group == "depth":
            streams = f"{symbol}@depth@100ms/{symbol}@bookTicker"
            endpoint = f"{root}/public/stream"
        else:
            streams = (
                f"{symbol}@aggTrade/{symbol}@kline_1m/"
                f"{symbol}@markPrice@1s/{symbol}@forceOrder"
            )
            endpoint = f"{root}/market/stream"
        return f"{endpoint}?streams={streams}"

    def _run_forever(self, group: str = "market") -> None:
        delay = 1.0
        while not self._stop.is_set():
            self._set_connection("connecting", group=group)
            try:
                ws = websocket.WebSocketApp(
                    self._stream_url(group),
                    on_open=lambda socket: self._on_open(socket, group),
                    on_message=self._on_message,
                    on_error=lambda socket, error: self._on_error(socket, error, group),
                    on_close=lambda socket, code, message: self._on_close(socket, code, message, group),
                )
                if group == "depth":
                    self._ws_depth = ws
                else:
                    self._ws = ws
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                self._set_connection("disconnected", f"{type(exc).__name__}: {exc}", group=group)
            if self._stop.is_set():
                break
            if group == "depth":
                with self.lock:
                    self.data["book_synced"] = False
                    self.data["sequence_gap_unresolved"] = True
                    self.data["book_sync_error"] = "Depth WebSocket disconnected; awaiting fresh snapshot"
                    self._depth_buffer.clear()
                    self._depth_buffer_overflow = False
            self._set_connection(f"reconnecting in {delay:.0f}s", group=group)
            self._stop.wait(delay)
            delay = min(delay * 1.7, 15.0)

    def _on_open(self, ws: websocket.WebSocketApp, group: str = "market") -> None:
        self._set_connection("connected", "", group=group)
        with self.lock:
            if self._ever_connected[group]:
                self.data["total_reconnects"] += 1
            self._ever_connected[group] = True
            if group == "depth":
                self.data["book_synced"] = False
                self.data["sequence_gap_unresolved"] = True
                self.data["book_sync_error"] = "Syncing REST depth snapshot with WebSocket diffs"
                self._depth_buffer.clear()
                self._depth_buffer_overflow = False
        if group == "depth":
            self._start_book_sync()

    def _on_error(
        self, ws: websocket.WebSocketApp, error: Any, group: str = "market"
    ) -> None:
        self._set_connection("socket error", str(error), group=group)
        if group == "depth":
            with self.lock:
                self.data["book_synced"] = False
                self.data["sequence_gap_unresolved"] = True

    def _on_close(
        self, ws: websocket.WebSocketApp, code: Any, message: Any, group: str = "market"
    ) -> None:
        with self.lock:
            self.data["total_disconnects"] += 1
            if group == "depth":
                self.data["book_synced"] = False
                self.data["sequence_gap_unresolved"] = True
                self._depth_buffer.clear()
                self._depth_buffer_overflow = False
        self._set_connection("disconnected", f"close={code}; {message or ''}", group=group)


    def _on_message(self, ws: websocket.WebSocketApp, raw: str) -> None:
        received_at = time.time()
        try:
            message = json.loads(raw)
            payload = message.get("data", message) if isinstance(message, dict) else {}
            stream = message.get("stream", "") if isinstance(message, dict) else ""
            if not isinstance(payload, dict):
                return
            event_type = payload.get("e")
            channel = {
                "aggTrade": "aggTrade",
                "depthUpdate": "depth",
                "kline": "kline_1m",
                "bookTicker": "bookTicker",
                "markPriceUpdate": "markPrice",
                "forceOrder": "forceOrder",
            }.get(event_type, stream or "unknown")
            with self.lock:
                self.data["last_any_message_at"] = received_at
                self.data["total_messages_received"] += 1
                self.data["channel_last_seen"][channel] = received_at
                counts = self.data["channel_message_counts"]
                counts[channel] = counts.get(channel, 0) + 1
            if event_type == "depthUpdate":
                self._handle_depth_update(payload)
            elif event_type == "aggTrade":
                self._handle_agg_trade(payload)
            elif event_type == "kline":
                self._handle_kline(payload)
            elif event_type == "bookTicker":
                self._handle_book_ticker(payload)
            elif event_type == "markPriceUpdate":
                self._handle_mark_price(payload)
            elif event_type == "forceOrder":
                self._handle_liquidation(payload)
            with self.lock:
                self.data["last_market_message_at"] = received_at
        except (ValueError, TypeError, KeyError) as exc:
            _LOGGER.debug("Ignoring malformed Binance websocket message: %s", exc)
        except Exception:
            _LOGGER.exception("Unexpected error handling Binance websocket message")

    def _handle_agg_trade(self, event: dict[str, Any]) -> None:
        row = normalize_agg_trade(event)
        if row is None:
            return
        with self.lock:
            self.data["trades"].append(row)
            self.data["last_price"] = row["price"]
            self.data["trade_count"] += 1
            if row["side"] == "BUY":
                self.data["buy_volume_btc"] += row["size_btc"]
                self.data["cvd_value_btc"] += row["size_btc"]
            else:
                self.data["sell_volume_btc"] += row["size_btc"]
                self.data["cvd_value_btc"] -= row["size_btc"]
            self.data["cvd"].append({
                "timestamp": row["timestamp"],
                "value_btc": self.data["cvd_value_btc"],
            })

    def _handle_kline(self, event: dict[str, Any]) -> None:
        candle = normalize_kline_event(event)
        if candle is None:
            return
        with self.lock:
            self.data["candles"][int(candle["time"])] = candle
            if len(self.data["candles"]) > _MAX_CANDLES:
                for key in sorted(self.data["candles"])[:-_MAX_CANDLES]:
                    self.data["candles"].pop(key, None)

    def _handle_book_ticker(self, event: dict[str, Any]) -> None:
        bid, ask = _positive(event.get("b")), _positive(event.get("a"))
        bid_qty, ask_qty = _number(event.get("B")), _number(event.get("A"))
        with self.lock:
            self.data["bid"] = bid
            self.data["ask"] = ask
            self.data["bid_size_btc"] = bid_qty if bid_qty is not None and bid_qty >= 0 else None
            self.data["ask_size_btc"] = ask_qty if ask_qty is not None and ask_qty >= 0 else None

    def _handle_mark_price(self, event: dict[str, Any]) -> None:
        mark, index = _positive(event.get("p")), _positive(event.get("i"))
        rate = _number(event.get("r"))
        next_funding = _seconds_from_ms(event.get("T"))
        with self.lock:
            if mark is not None:
                self.data["mark_price"] = mark
            if index is not None:
                self.data["index_price"] = index
            if rate is not None:
                self.data["funding_rate"] = rate
            if next_funding is not None:
                self.data["next_funding_at"] = next_funding

    def _handle_liquidation(self, event: dict[str, Any]) -> None:
        row = normalize_liquidation_event(event)
        if row is None:
            return
        order_key = (
            row.get("symbol"), row.get("trade_time_ms"), row.get("side"),
            row.get("price"), row.get("size_btc"),
        )
        with self.lock:
            if order_key in self._liquidation_seen:
                return
            if len(self._liquidation_seen_order) == self._liquidation_seen_order.maxlen:
                oldest = self._liquidation_seen_order.popleft()
                self._liquidation_seen.discard(oldest)
            self._liquidation_seen_order.append(order_key)
            self._liquidation_seen.add(order_key)
            self.data["liquidations"].append(row)

    def _handle_depth_update(self, event: dict[str, Any]) -> None:
        sync_again = False
        with self.lock:
            if not self.data["book_synced"]:
                if len(self._depth_buffer) == self._depth_buffer.maxlen:
                    self._depth_buffer_overflow = True
                self._depth_buffer.append(dict(event))
                return
            last_id = self.data.get("book_last_update_id")
            action = depth_update_action(int(last_id or 0), event, require_previous=True)
            if action == "stale":
                return
            if action == "gap":
                self.data["book_synced"] = False
                self.data["sequence_gap_unresolved"] = True
                self.data["sequence_gaps"] += 1
                self.data["book_sync_error"] = (
                    f"Depth update gap: local={last_id}, U={event.get('U')}, u={event.get('u')}"
                )
                if len(self._depth_buffer) == self._depth_buffer.maxlen:
                    self._depth_buffer_overflow = True
                self._depth_buffer.append(dict(event))
                sync_again = True
            else:
                _apply_depth_levels(self.data["bids"], event.get("b", []), keep_high=True)
                _apply_depth_levels(self.data["asks"], event.get("a", []), keep_high=False)
                self.data["book_last_update_id"] = int(event["u"])
        if sync_again:
            self._start_book_sync()

    def _start_book_sync(self) -> None:
        with self.lock:
            if self._book_sync_running or self._stop.is_set():
                return
            self._book_sync_running = True
            thread = threading.Thread(
                target=self._sync_book_loop, name=f"binance-{self.symbol.lower()}-book-sync", daemon=True
            )
            self._book_sync_thread = thread
            thread.start()

    def _sync_book_loop(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    response = requests.get(
                        f"{self.rest_base_url}/fapi/v1/depth",
                        params={"symbol": self.symbol, "limit": 1000},
                        headers={"Accept": "application/json", "User-Agent": "ReadOnlyMarketMonitor/1.0"},
                        timeout=self.request_timeout,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    snapshot_id = int(payload["lastUpdateId"])
                    bids = {float(p): float(q) for p, q in payload.get("bids", []) if _positive(p) and _positive(q)}
                    asks = {float(p): float(q) for p, q in payload.get("asks", []) if _positive(p) and _positive(q)}
                except Exception as exc:
                    with self.lock:
                        self.data["book_synced"] = False
                        self.data["sequence_gap_unresolved"] = True
                        self.data["book_sync_error"] = f"Depth snapshot request failed: {type(exc).__name__}: {exc}"
                    if self._stop.wait(1.0):
                        return
                    continue

                synchronized = True
                with self.lock:
                    if self._depth_buffer_overflow:
                        self._depth_buffer.clear()
                        self._depth_buffer_overflow = False
                        synchronized = False
                        self.data["book_sync_error"] = "Depth buffer overflowed; restarting snapshot synchronization"
                    else:
                        buffered = list(self._depth_buffer)
                        self._depth_buffer.clear()
                        last_id = snapshot_id
                        have_applied_buffered_event = False
                        for index, event in enumerate(buffered):
                            action = depth_update_action(
                                last_id, event, require_previous=have_applied_buffered_event
                            )
                            if action == "stale":
                                continue
                            if action == "gap":
                                self._depth_buffer.extend(buffered[index:])
                                synchronized = False
                                self.data["sequence_gaps"] += 1
                                self.data["book_sync_error"] = (
                                    f"Buffered depth gap: local={last_id}, U={event.get('U')}, "
                                    f"pu={event.get('pu')}, u={event.get('u')}"
                                )
                                break
                            _apply_depth_levels(bids, event.get("b", []), keep_high=True)
                            _apply_depth_levels(asks, event.get("a", []), keep_high=False)
                            last_id = int(event["u"])
                            have_applied_buffered_event = True
                    if synchronized:
                        self.data["bids"] = bids
                        self.data["asks"] = asks
                        self.data["book_last_update_id"] = last_id
                        self.data["book_synced"] = True
                        self.data["sequence_gap_unresolved"] = False
                        self.data["book_sync_error"] = ""
                        return
                if self._stop.wait(0.35):
                    return
        finally:
            with self.lock:
                self._book_sync_running = False
                should_retry = (
                    not self.data.get("book_synced", False) and not self._stop.is_set()
                )
            if should_retry:
                self._start_book_sync()

    def _poll_rest(self) -> None:
        # REST requests are deliberately run off the Streamlit script thread.
        last_oi = 0.0
        last_ticker = 0.0
        last_candle_attempt = 0.0
        candles_loaded = False
        while not self._stop.is_set():
            now = time.monotonic()
            if not candles_loaded and now - last_candle_attempt >= 30:
                last_candle_attempt = now
                try:
                    response = requests.get(
                        f"{self.rest_base_url}/fapi/v1/klines",
                        params={"symbol": self.symbol, "interval": "1m", "limit": 1000},
                        headers={"Accept": "application/json", "User-Agent": "ReadOnlyMarketMonitor/1.0"},
                        timeout=self.request_timeout,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    rows = [row for row in (normalize_rest_kline(x) for x in payload) if row]
                    with self.lock:
                        for candle in rows:
                            self.data["candles"][int(candle["time"])] = candle
                        if len(self.data["candles"]) > _MAX_CANDLES:
                            for key in sorted(self.data["candles"])[:-_MAX_CANDLES]:
                                self.data["candles"].pop(key, None)
                        self.data["last_rest_poll_at"] = time.time()
                        self.data["last_rest_error"] = ""
                    candles_loaded = True
                except Exception as exc:
                    with self.lock:
                        self.data["last_rest_error"] = f"Initial klines failed: {type(exc).__name__}: {exc}"[:400]
            if now - last_oi >= 30:
                try:
                    response = requests.get(
                        f"{self.rest_base_url}/fapi/v1/openInterest",
                        params={"symbol": self.symbol},
                        headers={"Accept": "application/json", "User-Agent": "ReadOnlyMarketMonitor/1.0"},
                        timeout=self.request_timeout,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    oi = _positive(payload.get("openInterest")) if isinstance(payload, dict) else None
                    with self.lock:
                        self.data["open_interest_btc"] = oi
                        last_price = _positive(self.data.get("last_price")) or _positive(self.data.get("mark_price"))
                        self.data["open_interest_usd_approx"] = oi * last_price if oi is not None and last_price is not None else None
                        self.data["open_interest_updated_at"] = time.time()
                        self.data["last_rest_poll_at"] = time.time()
                        self.data["last_rest_error"] = ""
                    last_oi = now
                except Exception as exc:
                    with self.lock:
                        self.data["last_rest_error"] = f"Open-interest poll failed: {type(exc).__name__}: {exc}"[:400]
                    last_oi = now
            if now - last_ticker >= 60:
                try:
                    response = requests.get(
                        f"{self.rest_base_url}/fapi/v1/ticker/24hr",
                        params={"symbol": self.symbol},
                        headers={"Accept": "application/json", "User-Agent": "ReadOnlyMarketMonitor/1.0"},
                        timeout=self.request_timeout,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    ticker = {
                        "open": _number(payload.get("openPrice")),
                        "high": _number(payload.get("highPrice")),
                        "low": _number(payload.get("lowPrice")),
                        "close": _number(payload.get("lastPrice")),
                        "mark_change_pct": _number(payload.get("priceChangePercent")),
                        "base_volume_btc": _number(payload.get("volume")),
                        "quote_volume_usd_approx": _number(payload.get("quoteVolume")),
                    } if isinstance(payload, dict) else {}
                    with self.lock:
                        self.data["ticker_24h"] = ticker
                        self.data["ticker_24h_updated_at"] = time.time()
                        self.data["last_rest_poll_at"] = time.time()
                        self.data["last_rest_error"] = ""
                    last_ticker = now
                except Exception as exc:
                    with self.lock:
                        self.data["last_rest_error"] = f"24-hour ticker poll failed: {type(exc).__name__}: {exc}"[:400]
                    last_ticker = now
            self._stop.wait(1.0)
