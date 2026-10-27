"""Read-only Bybit V5 linear BTCUSDT public-market-data adapter.

Uses Bybit's public linear WebSocket and REST market-data APIs only. There are
no account credentials, private endpoints, or order-entry methods in this module.
"""
from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

import requests
import websocket

_LOGGER = logging.getLogger(__name__)
REST_BASE_URL = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/linear"
DEFAULT_SYMBOL = "BTCUSDT"
TIMEFRAMES = {"1m": "1", "5m": "5", "15m": "15", "1h": "60"}
INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}
MAX_CANDLES = 2000
MAX_BOOK_LEVELS = 5000


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _positive(value: Any) -> float | None:
    value = _number(value)
    return value if value is not None and value > 0 else None


def _ms_to_seconds(value: Any) -> float | None:
    raw = _number(value)
    return raw / 1000.0 if raw is not None and raw > 0 else None


def _level_map(rows: Any, *, is_bid: bool) -> dict[float, float]:
    result: dict[float, float] = {}
    if not isinstance(rows, list):
        return result
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        price, size = _positive(row[0]), _number(row[1])
        if price is None or size is None or size <= 0:
            continue
        result[price] = size
    ordered = sorted(result, reverse=is_bid)
    return {price: result[price] for price in ordered[:MAX_BOOK_LEVELS]}


def normalize_rest_kline(row: Any, interval: str) -> dict[str, Any] | None:
    """Normalize a Bybit V5 REST kline array; last candle may still be open."""
    if interval not in INTERVAL_SECONDS or not isinstance(row, (list, tuple)) or len(row) < 6:
        return None
    ts = _ms_to_seconds(row[0])
    o, h, low, close, volume = (_number(row[i]) for i in (1, 2, 3, 4, 5))
    turnover = _number(row[6]) if len(row) > 6 else None
    if ts is None or any(v is None for v in (o, h, low, close, volume)):
        return None
    if min(o, h, low, close) <= 0 or volume < 0 or h < max(o, low, close) or low > min(o, h, close):
        return None
    seconds = INTERVAL_SECONDS[interval]
    start = int(ts // seconds) * seconds
    return {
        "time": start, "open": o, "high": h, "low": low, "close": close,
        "volume": volume, "turnover": turnover, "interval": interval,
        "is_closed": start + seconds <= time.time(),
    }


def normalize_ws_kline(row: dict[str, Any], interval: str) -> dict[str, Any] | None:
    """Normalize Bybit kline WebSocket object (confirm=true means closed)."""
    if interval not in INTERVAL_SECONDS:
        return None
    normalized = normalize_rest_kline(
        [
            row.get("start"), row.get("open"), row.get("high"), row.get("low"),
            row.get("close"), row.get("volume"), row.get("turnover"),
        ],
        interval,
    )
    if normalized is None:
        return None
    normalized["is_closed"] = bool(row.get("confirm", False))
    return normalized


def normalize_trade(row: dict[str, Any]) -> dict[str, Any] | None:
    price = _positive(row.get("p"))
    size = _positive(row.get("v"))
    timestamp = _ms_to_seconds(row.get("T"))
    side = str(row.get("S", "")).strip().upper()
    if price is None or size is None or timestamp is None or side not in {"BUY", "SELL"}:
        return None
    return {
        "timestamp": timestamp,
        "price": price,
        "size_btc": size,
        "size": size,
        "side": side,
        "trade_id": row.get("i"),
        "sequence": row.get("seq"),
        "is_block_trade": bool(row.get("BT", False)),
        "is_rpi_trade": bool(row.get("RPI", False)),
        "notional_usdt_approx": price * size,
        "source": "bybit_v5_publicTrade",
    }


def normalize_liquidation(row: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize Bybit allLiquidation rows; p is bankruptcy price, not fill price."""
    price = _positive(row.get("p"))
    size = _positive(row.get("v"))
    timestamp = _ms_to_seconds(row.get("T"))
    side = str(row.get("S", "")).strip().upper()
    symbol = str(row.get("s", "")).strip().upper()
    if price is None or size is None or timestamp is None or side not in {"BUY", "SELL"} or not symbol:
        return None
    return {
        "timestamp": timestamp,
        "symbol": symbol,
        "position_side_liquidated": side,
        "bankruptcy_price": price,
        "size_btc": size,
        "notional_usdt_approx_at_bankruptcy_price": price * size,
        "source": "bybit_v5_allLiquidation",
    }


def apply_book_update(
    bids: dict[float, float],
    asks: dict[float, float],
    *,
    message_type: str,
    data: dict[str, Any],
    previous_update_id: int | None,
    previous_sequence: int | None,
) -> tuple[dict[float, float], dict[float, float], int | None, int | None, str]:
    """Apply a documented Bybit snapshot/delta or return an integrity failure.

    Bybit sends full snapshots after subscription and may resend a fresh
    snapshot after its own issue; snapshots always replace the local maps.
    For deltas, stale/duplicate update IDs are ignored; regression in update
    ID or cross-sequence invalidates the book and requires a new snapshot.
    """
    try:
        update_id = int(data.get("u"))
    except (TypeError, ValueError):
        return bids, asks, previous_update_id, previous_sequence, "missing_update_id"
    sequence_raw = data.get("seq")
    try:
        sequence = int(sequence_raw) if sequence_raw is not None else None
    except (TypeError, ValueError):
        sequence = None
    if message_type == "snapshot" or update_id == 1:
        new_bids = _level_map(data.get("b", []), is_bid=True)
        new_asks = _level_map(data.get("a", []), is_bid=False)
        if not new_bids or not new_asks:
            return bids, asks, previous_update_id, previous_sequence, "invalid_snapshot"
        return new_bids, new_asks, update_id, sequence, "snapshot"
    if message_type != "delta" or previous_update_id is None:
        return bids, asks, previous_update_id, previous_sequence, "delta_without_snapshot"
    if update_id <= previous_update_id:
        if update_id == previous_update_id:
            return bids, asks, previous_update_id, previous_sequence, "duplicate"
        return bids, asks, previous_update_id, previous_sequence, "update_id_regression"
    if sequence is not None and previous_sequence is not None and sequence <= previous_sequence:
        return bids, asks, previous_update_id, previous_sequence, "sequence_regression"

    new_bids, new_asks = dict(bids), dict(asks)
    for raw, is_bid in ((data.get("b", []), True), (data.get("a", []), False)):
        if not isinstance(raw, list):
            continue
        target = new_bids if is_bid else new_asks
        for level in raw:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            price, size = _positive(level[0]), _number(level[1])
            if price is None or size is None or size < 0:
                continue
            if size == 0:
                target.pop(price, None)
            else:
                target[price] = size
        if len(target) > MAX_BOOK_LEVELS:
            ordered = sorted(target, reverse=is_bid)
            for price in ordered[MAX_BOOK_LEVELS:]:
                target.pop(price, None)
    return new_bids, new_asks, update_id, sequence, "applied"


def aggregate_observed_bankruptcy_buckets(
    events: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    now_epoch: float | None = None,
    window_seconds: int = 3600,
    bucket_width: float = 100.0,
) -> list[dict[str, Any]]:
    """Aggregate *observed* Bybit liquidation messages by bankruptcy-price bucket."""
    now = time.time() if now_epoch is None else float(now_epoch)
    width = _positive(bucket_width)
    if width is None or window_seconds <= 0:
        return []
    groups: dict[tuple[float, str], dict[str, Any]] = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        ts = _number(event.get("timestamp"))
        price = _positive(event.get("bankruptcy_price"))
        size = _positive(event.get("size_btc"))
        side = str(event.get("position_side_liquidated", "")).upper()
        if ts is None or price is None or size is None or side not in {"BUY", "SELL"}:
            continue
        if ts < now - window_seconds or ts > now + 5:
            continue
        low = math.floor(price / width) * width
        key = (low, side)
        group = groups.setdefault(key, {
            "bankruptcy_price_bucket_low": low,
            "bankruptcy_price_bucket_high": low + width,
            "position_side_liquidated": side,
            "observed_liquidation_messages": 0,
            "observed_size_btc": 0.0,
            "notional_at_bankruptcy_price_usdt_approx": 0.0,
        })
        group["observed_liquidation_messages"] += 1
        group["observed_size_btc"] += size
        group["notional_at_bankruptcy_price_usdt_approx"] += price * size
    return sorted(groups.values(), key=lambda row: row["observed_size_btc"], reverse=True)


class BybitV5Feed:
    """Read-only public Bybit V5 linear BTCUSDT market-data consumer."""

    def __init__(
        self,
        symbol: str = DEFAULT_SYMBOL,
        *,
        rest_base_url: str = REST_BASE_URL,
        ws_url: str = WS_URL,
        request_timeout: float = 8.0,
    ):
        symbol = str(symbol).strip().upper()
        if not symbol.isalnum() or len(symbol) < 5:
            raise ValueError("symbol must be a Bybit market symbol such as BTCUSDT")
        self.symbol = symbol
        self.rest_base_url = rest_base_url.rstrip("/")
        self.ws_url = ws_url
        self.request_timeout = float(request_timeout)
        self.lock = threading.RLock()
        now = time.time()
        self.data: dict[str, Any] = {
            "provider": "Bybit V5 Linear",
            "symbol": symbol,
            "connection": "starting",
            "last_error": "",
            "last_rest_error": "",
            "started_at": now,
            "last_any_message_at": None,
            "last_market_message_at": None,
            "last_message_by_topic": {},
            "message_counts": {},
            "message_count": 0,
            "total_disconnects": 0,
            "total_reconnects": 0,
            "last_price": None,
            "mark_price": None,
            "index_price": None,
            "bid": None,
            "ask": None,
            "bid_size_btc": None,
            "ask_size_btc": None,
            "funding_rate": None,
            "next_funding_at": None,
            "open_interest_btc": None,
            "open_interest_usd_approx": None,
            "ticker_24h": {},
            "bids": {},
            "asks": {},
            "book_update_id": None,
            "book_cross_sequence": None,
            "book_synced": False,
            "sequence_gap_unresolved": True,
            "book_integrity_message": "Waiting for initial Bybit orderbook snapshot",
            "trades": deque(maxlen=1000),
            "candles": {},
            "candles_by_interval": {tf: {} for tf in TIMEFRAMES},
            "candle_history_loaded": {tf: False for tf in TIMEFRAMES},
            "candle_history_updated_at": {},
            "candle_history_errors": {tf: "" for tf in TIMEFRAMES},
            "cvd": deque(maxlen=5000),
            "cvd_value_btc": 0.0,
            "buy_volume_btc": 0.0,
            "sell_volume_btc": 0.0,
            "trade_count": 0,
            "liquidations": deque(maxlen=10000),
            "ticker_updated_at": None,
            "last_rest_poll_at": None,
            "depth_restarts": 0,
        }
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._rest_thread: threading.Thread | None = None
        self._ws: websocket.WebSocketApp | None = None
        self._ever_connected = False

    def start(self) -> None:
        if self._thread and self._thread.is_alive() and self._rest_thread and self._rest_thread.is_alive():
            return
        self._stop.clear()
        if not (self._thread and self._thread.is_alive()):
            self._thread = threading.Thread(target=self._run_forever, name="bybit-v5-public-ws", daemon=True)
            self._thread.start()
        if not (self._rest_thread and self._rest_thread.is_alive()):
            self._rest_thread = threading.Thread(target=self._poll_rest, name="bybit-v5-public-rest", daemon=True)
            self._rest_thread.start()

    def stop(self) -> None:
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            snap = dict(self.data)
            for key in ("bids", "asks", "ticker_24h"):
                snap[key] = dict(self.data[key])
            snap["trades"] = [dict(row) for row in reversed(self.data["trades"])]
            snap["liquidations"] = [dict(row) for row in reversed(self.data["liquidations"])]
            snap["candles"] = [dict(row) for _, row in sorted(self.data["candles"].items())]
            snap["candles_by_interval"] = {
                interval: [dict(row) for _, row in sorted(rows.items())]
                for interval, rows in self.data["candles_by_interval"].items()
            }
            snap["candle_history_loaded"] = dict(self.data["candle_history_loaded"])
            snap["candle_history_updated_at"] = dict(self.data["candle_history_updated_at"])
            snap["candle_history_errors"] = dict(self.data["candle_history_errors"])
            snap["cvd"] = list(self.data["cvd"])
            snap["last_message_by_topic"] = dict(self.data["last_message_by_topic"])
            snap["message_counts"] = dict(self.data["message_counts"])
            return snap

    def analyst_context(self, *, now_epoch: float | None = None) -> dict[str, Any]:
        now = time.time() if now_epoch is None else float(now_epoch)
        snap = self.snapshot()
        bid, ask = _positive(snap.get("bid")), _positive(snap.get("ask"))
        mid = (bid + ask) / 2 if bid and ask and ask >= bid else None
        spread = ask - bid if bid and ask and ask >= bid else None
        topics = {}
        for topic, ts in snap.get("last_message_by_topic", {}).items():
            topics[topic] = round(max(0.0, now - float(ts)), 3)
        bids = sorted(((p, q) for p, q in snap["bids"].items()), reverse=True)[:5]
        asks = sorted(((p, q) for p, q in snap["asks"].items()))[:5]
        liquidations = snap["liquidations"][:12]
        return {
            "venue": snap["provider"],
            "symbol": snap["symbol"],
            "observed_at_utc": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "connection": snap["connection"],
            "last_market_message_age_seconds": round(
                max(0.0, now - snap["last_market_message_at"]), 3
            ) if snap.get("last_market_message_at") else None,
            "topic_age_seconds": topics,
            "price": {
                "last_trade": _positive(snap.get("last_price")),
                "mark": _positive(snap.get("mark_price")),
                "index": _positive(snap.get("index_price")),
                "best_bid": bid, "best_ask": ask,
                "mid_calculated": mid,
                "spread_calculated": spread,
                "spread_bps_calculated": spread / mid * 10000 if mid else None,
            },
            "funding_rate": _number(snap.get("funding_rate")),
            "next_funding_time_utc": datetime.fromtimestamp(
                float(snap["next_funding_at"]), tz=timezone.utc
            ).isoformat() if snap.get("next_funding_at") else None,
            "open_interest_btc": _number(snap.get("open_interest_btc")),
            "open_interest_usd_approx": _number(snap.get("open_interest_usd_approx")),
            "ticker_24h": dict(snap.get("ticker_24h") or {}),
            "order_book": {
                "synced_from_bybit_snapshot_deltas": bool(snap.get("book_synced")),
                "update_id": snap.get("book_update_id"),
                "cross_sequence": snap.get("book_cross_sequence"),
                "bids_top5": [{"price": p, "size_btc": q} for p, q in bids],
                "asks_top5": [{"price": p, "size_btc": q} for p, q in asks],
                "checksum": "No venue checksum claimed by this adapter; snapshot/delta update IDs are tracked.",
            },
            "session_trade_flow": {
                "trade_count": int(snap.get("trade_count") or 0),
                "buy_aggressor_volume_btc": _number(snap.get("buy_volume_btc")),
                "sell_aggressor_volume_btc": _number(snap.get("sell_volume_btc")),
                "session_cvd_btc": _number(snap.get("cvd_value_btc")),
                "warning": "Local session CVD calculated from Bybit taker-side trade messages; resets on restart.",
            },
            "recent_liquidation_messages": [{
                "time_utc": datetime.fromtimestamp(float(row["timestamp"]), tz=timezone.utc).isoformat(),
                "position_side_liquidated": row.get("position_side_liquidated"),
                "bankruptcy_price": row.get("bankruptcy_price"),
                "executed_size_btc": row.get("size_btc"),
                "approx_notional_at_bankruptcy_price_usdt": row.get("notional_usdt_approx_at_bankruptcy_price"),
            } for row in liquidations],
            "liquidation_data_limitations": [
                "Bybit allLiquidation reports observed executions and a bankruptcy price, not a predictive map of untriggered liquidation prices.",
                "The in-memory observation buffer is bounded; missing historical messages are not reconstructed.",
            ],
        }

    def _run_forever(self) -> None:
        delay = 1.0
        topics = [
            f"orderbook.50.{self.symbol}",
            f"publicTrade.{self.symbol}",
            f"tickers.{self.symbol}",
            *(f"kline.{interval}.{self.symbol}" for interval in TIMEFRAMES.values()),
            f"allLiquidation.{self.symbol}",
        ]
        while not self._stop.is_set():
            try:
                self._set_connection("connecting")
                self._ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=lambda ws: self._on_open(ws, topics),
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                self._set_connection("disconnected", f"{type(exc).__name__}: {exc}")
            if self._stop.is_set():
                break
            with self.lock:
                self.data["book_synced"] = False
                self.data["sequence_gap_unresolved"] = True
                self.data["book_integrity_message"] = "WebSocket disconnected; waiting for new full snapshot"
            self._stop.wait(delay)
            delay = min(15.0, delay * 1.7)

    def _set_connection(self, value: str, error: str | None = None) -> None:
        with self.lock:
            self.data["connection"] = value
            if error is not None:
                self.data["last_error"] = str(error)[:500]

    def _on_open(self, ws: websocket.WebSocketApp, topics: list[str]) -> None:
        self._set_connection("connected", "")
        with self.lock:
            if self._ever_connected:
                self.data["total_reconnects"] += 1
            self._ever_connected = True
            self.data["book_synced"] = False
            self.data["sequence_gap_unresolved"] = True
            self.data["book_integrity_message"] = "Subscribed; waiting for Bybit full book snapshot"
            self.data["depth_restarts"] += 1
        ws.send(json.dumps({"op": "subscribe", "args": topics}))
        threading.Thread(
            target=self._heartbeat_loop, args=(ws,),
            name="bybit-v5-json-heartbeat", daemon=True
        ).start()

    def _heartbeat_loop(self, ws: websocket.WebSocketApp) -> None:
        # Bybit V5 documents application-level JSON ping/pong heartbeats.
        while not self._stop.wait(20.0):
            if ws is not self._ws:
                return
            try:
                ws.send(json.dumps({"op": "ping"}))
            except Exception:
                return

    def _on_error(self, ws: websocket.WebSocketApp, error: Any) -> None:
        self._set_connection("socket error", str(error))
        with self.lock:
            self.data["book_synced"] = False
            self.data["sequence_gap_unresolved"] = True

    def _on_close(self, ws: websocket.WebSocketApp, code: Any, message: Any) -> None:
        with self.lock:
            self.data["total_disconnects"] += 1
            self.data["book_synced"] = False
            self.data["sequence_gap_unresolved"] = True
        self._set_connection("disconnected", f"close={code}; {message or ''}")

    def _on_message(self, ws: websocket.WebSocketApp, raw: str) -> None:
        received_at = time.time()
        try:
            message = json.loads(raw)
            if not isinstance(message, dict):
                return
            if message.get("op") == "ping":
                try:
                    ws.send(json.dumps({"op": "pong"}))
                except Exception:
                    pass
                return
            if message.get("success") is False:
                self._set_connection("subscription error", str(message.get("ret_msg") or message))
                return
            topic = str(message.get("topic", ""))
            payload = message.get("data")
            if not topic or payload is None:
                return
            with self.lock:
                self.data["last_any_message_at"] = received_at
                self.data["last_message_by_topic"][topic] = received_at
                self.data["message_counts"][topic] = self.data["message_counts"].get(topic, 0) + 1
                self.data["message_count"] += 1
                if not topic.startswith("orderbook."):
                    self.data["last_market_message_at"] = received_at

            if topic.startswith("orderbook."):
                if isinstance(payload, dict):
                    self._handle_orderbook(str(message.get("type", "")), payload)
            elif topic.startswith("publicTrade."):
                rows = payload if isinstance(payload, list) else [payload]
                for item in rows:
                    if isinstance(item, dict):
                        self._handle_trade(item)
            elif topic.startswith("tickers."):
                if isinstance(payload, dict):
                    self._handle_ticker(payload)
            elif topic.startswith("kline."):
                rows = payload if isinstance(payload, list) else [payload]
                try:
                    interval = topic.split(".")[1]
                except IndexError:
                    interval = ""
                timeframe = {value: key for key, value in TIMEFRAMES.items()}.get(interval)
                if timeframe:
                    for item in rows:
                        if isinstance(item, dict):
                            self._handle_kline(item, timeframe)
            elif topic.startswith("allLiquidation."):
                rows = payload if isinstance(payload, list) else [payload]
                for item in rows:
                    if isinstance(item, dict):
                        self._handle_liquidation(item)
        except (TypeError, ValueError, KeyError) as exc:
            _LOGGER.debug("Ignoring malformed Bybit market message: %s", exc)
        except Exception:
            _LOGGER.exception("Unexpected Bybit message-handler failure")

    def _handle_orderbook(self, message_type: str, payload: dict[str, Any]) -> None:
        with self.lock:
            result = apply_book_update(
                self.data["bids"], self.data["asks"],
                message_type=message_type,
                data=payload,
                previous_update_id=self.data.get("book_update_id"),
                previous_sequence=self.data.get("book_cross_sequence"),
            )
            bids, asks, update_id, seq, action = result
            if action in {"snapshot", "applied", "duplicate"}:
                self.data["bids"] = bids
                self.data["asks"] = asks
                self.data["book_update_id"] = update_id
                self.data["book_cross_sequence"] = seq
                if action != "duplicate":
                    self.data["book_synced"] = True
                    self.data["sequence_gap_unresolved"] = False
                    self.data["book_integrity_message"] = ""
                return
            self.data["book_synced"] = False
            self.data["sequence_gap_unresolved"] = True
            self.data["book_integrity_message"] = f"Book invalidated: {action}; waiting for a new snapshot"
            ws = self._ws
        # Force a fresh handshake/full snapshot after an integrity regression.
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def _handle_trade(self, payload: dict[str, Any]) -> None:
        row = normalize_trade(payload)
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
                "timestamp": row["timestamp"], "value_btc": self.data["cvd_value_btc"]
            })

    def _handle_ticker(self, payload: dict[str, Any]) -> None:
        with self.lock:
            # Ticker messages can be delta updates: omitted fields did not change.
            merged = dict(self.data["ticker_24h"])
            merged.update(payload)
            self.data["ticker_24h"] = merged
            mapping = {
                "lastPrice": "last_price", "markPrice": "mark_price",
                "indexPrice": "index_price", "bid1Price": "bid",
                "ask1Price": "ask", "bid1Size": "bid_size_btc",
                "ask1Size": "ask_size_btc", "fundingRate": "funding_rate",
                "openInterest": "open_interest_btc",
            }
            for source, target in mapping.items():
                if source not in payload:
                    continue
                value = _number(payload[source])
                if target in {"last_price", "mark_price", "index_price", "bid", "ask"}:
                    value = _positive(payload[source])
                if value is not None and (target == "funding_rate" or value >= 0):
                    self.data[target] = value
            if "nextFundingTime" in payload:
                value = _ms_to_seconds(payload["nextFundingTime"])
                if value is not None:
                    self.data["next_funding_at"] = value
            price = _positive(self.data.get("mark_price")) or _positive(self.data.get("last_price"))
            oi = _positive(self.data.get("open_interest_btc"))
            self.data["open_interest_usd_approx"] = price * oi if price and oi else None
            self.data["ticker_updated_at"] = time.time()

    def _handle_kline(self, payload: dict[str, Any], timeframe: str) -> None:
        candle = normalize_ws_kline(payload, timeframe)
        if candle is None:
            return
        with self.lock:
            target = self.data["candles_by_interval"][timeframe]
            target[int(candle["time"])] = candle
            if timeframe == "1m":
                self.data["candles"][int(candle["time"])] = candle
            if len(target) > MAX_CANDLES:
                for key in sorted(target)[:-MAX_CANDLES]:
                    target.pop(key, None)
            if len(self.data["candles"]) > MAX_CANDLES:
                for key in sorted(self.data["candles"])[:-MAX_CANDLES]:
                    self.data["candles"].pop(key, None)

    def _handle_liquidation(self, payload: dict[str, Any]) -> None:
        row = normalize_liquidation(payload)
        if row is None:
            return
        with self.lock:
            self.data["liquidations"].append(row)

    def _poll_rest(self) -> None:
        last_ticker_poll = 0.0
        next_candle_retry = {tf: 0.0 for tf in TIMEFRAMES}
        now_mono = time.monotonic()
        for tf in TIMEFRAMES:
            next_candle_retry[tf] = now_mono
        while not self._stop.is_set():
            now_mono = time.monotonic()
            for timeframe, interval in TIMEFRAMES.items():
                with self.lock:
                    loaded = self.data["candle_history_loaded"][timeframe]
                if loaded or now_mono < next_candle_retry[timeframe]:
                    continue
                next_candle_retry[timeframe] = now_mono + 30.0
                try:
                    response = requests.get(
                        f"{self.rest_base_url}/v5/market/kline",
                        params={
                            "category": "linear", "symbol": self.symbol,
                            "interval": interval, "limit": 1000,
                        },
                        headers={"Accept": "application/json", "User-Agent": "ReadOnlyMarketMonitor/1.0"},
                        timeout=self.request_timeout,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, dict) or payload.get("retCode") != 0:
                        raise ValueError(f"Bybit kline API error: {payload.get('retCode') if isinstance(payload, dict) else 'invalid response'}")
                    raw_rows = payload.get("result", {}).get("list", [])
                    rows = [x for x in (normalize_rest_kline(row, timeframe) for row in raw_rows) if x]
                    if not rows:
                        raise ValueError(f"Bybit returned no valid {timeframe} candles")
                    with self.lock:
                        target = self.data["candles_by_interval"][timeframe]
                        for candle in rows:
                            target[int(candle["time"])] = candle
                        # Keep the legacy 1m map in sync without injecting other intervals.
                        if timeframe == "1m":
                            self.data["candles"] = target
                        if len(target) > MAX_CANDLES:
                            for key in sorted(target)[:-MAX_CANDLES]:
                                target.pop(key, None)
                        self.data["candle_history_loaded"][timeframe] = True
                        self.data["candle_history_updated_at"][timeframe] = time.time()
                        self.data["candle_history_errors"][timeframe] = ""
                        self.data["last_rest_poll_at"] = time.time()
                except Exception as exc:
                    with self.lock:
                        self.data["candle_history_errors"][timeframe] = f"{type(exc).__name__}: {exc}"[:400]
                        self.data["last_rest_error"] = self.data["candle_history_errors"][timeframe]
            if now_mono - last_ticker_poll >= 15.0:
                last_ticker_poll = now_mono
                try:
                    response = requests.get(
                        f"{self.rest_base_url}/v5/market/tickers",
                        params={"category": "linear", "symbol": self.symbol},
                        headers={"Accept": "application/json", "User-Agent": "ReadOnlyMarketMonitor/1.0"},
                        timeout=self.request_timeout,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, dict) or payload.get("retCode") != 0:
                        raise ValueError(f"Bybit ticker API error: {payload.get('retCode') if isinstance(payload, dict) else 'invalid response'}")
                    rows = payload.get("result", {}).get("list", [])
                    if not rows:
                        raise ValueError("Bybit ticker API returned no data")
                    self._handle_ticker(rows[0])
                    with self.lock:
                        self.data["ticker_updated_at"] = time.time()
                        self.data["last_rest_poll_at"] = time.time()
                        self.data["last_rest_error"] = ""
                except Exception as exc:
                    with self.lock:
                        self.data["last_rest_error"] = f"{type(exc).__name__}: {exc}"[:400]
            self._stop.wait(1.0)
