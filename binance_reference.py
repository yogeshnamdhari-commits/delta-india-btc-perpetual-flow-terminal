"""Read-only Binance USDⓈ-M reference feed.

This feed is for cross-venue context only. It is deliberately isolated from
DeltaFeed, the Delta order-book integrity checks, signal decisions and OOS data.
No private endpoints or account credentials are used.
"""
from __future__ import annotations

import json
import math
import threading
import time
from collections import deque
from typing import Any

import requests
import websocket

SYMBOL = "BTCUSDT"
WS_URL = (
    "wss://fstream.binance.com/stream?streams="
    "btcusdt@aggTrade/"
    "btcusdt@bookTicker/"
    "btcusdt@depth20@100ms/"
    "btcusdt@markPrice@1s/"
    "btcusdt@forceOrder"
)
OPEN_INTEREST_URL = "https://fapi.binance.com/fapi/v1/openInterest"
OPEN_INTEREST_POLL_SECONDS = 15.0


def _number(value: Any) -> float | None:
    try:
        if value is None:
            return None
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _milliseconds(value: Any) -> int | None:
    number = _number(value)
    if number is None or number <= 0:
        return None
    return int(number)


def _levels(rows: Any) -> dict[float, float]:
    result: dict[float, float] = {}
    if not isinstance(rows, list):
        return result
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        price, quantity = _number(row[0]), _number(row[1])
        if price is not None and quantity is not None and price > 0 and quantity > 0:
            result[price] = quantity
    return result


class BinanceReferenceFeed:
    """Threaded, public-data-only BTCUSDT market reference adapter."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self._stop_event = threading.Event()
        self._ws_thread: threading.Thread | None = None
        self._oi_thread: threading.Thread | None = None
        self._ws: websocket.WebSocketApp | None = None
        self._opened_once = False
        self.data: dict[str, Any] = {
            "connection": "not started",
            "last_error": "",
            "open_interest_error": "",
            "last_any_message_at": None,
            "last_market_message_at": None,
            "last_price": None,
            "last_trade_time_ms": None,
            "last_trade_side": None,
            "last_trade_qty_btc": None,
            "trade_count": 0,
            "buy_qty_btc": 0.0,
            "sell_qty_btc": 0.0,
            "cvd_btc": 0.0,
            "cvd_history": deque(maxlen=1500),
            "recent_trades": deque(maxlen=20),
            "bid": None,
            "bid_qty_btc": None,
            "ask": None,
            "ask_qty_btc": None,
            "bids": {},
            "asks": {},
            "book_ready": False,
            "book_updated_at": None,
            "book_source": "top-20 partial snapshots",
            "mark_price": None,
            "index_price": None,
            "funding_rate": None,
            "next_funding_ms": None,
            "funding_updated_at": None,
            "open_interest_btc": None,
            "open_interest_updated_at": None,
            "last_liquidation": None,
            "last_liquidation_received_at": None,
            "channel_last_seen": {},
            "channel_message_counts": {},
            "total_messages_received": 0,
            "total_disconnects": 0,
            "total_reconnects": 0,
        }

    def start(self) -> None:
        """Start public WebSocket and REST polling workers once."""
        if self._ws_thread and self._ws_thread.is_alive():
            return
        self._stop_event.clear()
        self._ws_thread = threading.Thread(
            target=self._run_forever, name="binance-reference-ws", daemon=True
        )
        self._oi_thread = threading.Thread(
            target=self._poll_open_interest,
            name="binance-reference-open-interest",
            daemon=True,
        )
        self._ws_thread.start()
        self._oi_thread.start()

    def stop(self) -> None:
        """Best-effort shutdown, primarily for controlled tests/restarts."""
        self._stop_event.set()
        ws = self._ws
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
            result["recent_trades"] = list(self.data["recent_trades"])
            result["cvd_history"] = list(self.data["cvd_history"])
            result["channel_last_seen"] = dict(self.data["channel_last_seen"])
            result["channel_message_counts"] = dict(self.data["channel_message_counts"])
            liquidation = self.data.get("last_liquidation")
            result["last_liquidation"] = dict(liquidation) if liquidation else None
            return result

    def _set_status(self, value: str, error: str | None = None) -> None:
        with self.lock:
            self.data["connection"] = value
            if error is not None:
                self.data["last_error"] = str(error)[:300]

    def _run_forever(self) -> None:
        delay = 2.0
        while not self._stop_event.is_set():
            self._set_status("connecting")
            try:
                self._ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10, skip_utf8_validation=True)
            except Exception as exc:
                self._set_status("socket error", f"{type(exc).__name__}: {exc}")
            if self._stop_event.is_set():
                break
            with self.lock:
                self.data["connection"] = "reconnecting"
                self.data["book_ready"] = False
                self.data["bids"] = {}
                self.data["asks"] = {}
            self._stop_event.wait(delay)
            delay = min(delay * 1.6, 20.0)

    def _poll_open_interest(self) -> None:
        session = requests.Session()
        try:
            while not self._stop_event.is_set():
                try:
                    response = session.get(
                        OPEN_INTEREST_URL,
                        params={"symbol": SYMBOL},
                        headers={"Accept": "application/json", "User-Agent": "BTCReferenceFeed/1.0"},
                        timeout=5,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    value = _number(payload.get("openInterest")) if isinstance(payload, dict) else None
                    if value is None or value < 0:
                        raise ValueError("Open-interest response did not contain a valid openInterest value")
                    with self.lock:
                        self.data["open_interest_btc"] = value
                        self.data["open_interest_updated_at"] = time.time()
                        self.data["open_interest_error"] = ""
                except Exception as exc:
                    with self.lock:
                        self.data["open_interest_error"] = f"{type(exc).__name__}: {exc}"[:250]
                self._stop_event.wait(OPEN_INTEREST_POLL_SECONDS)
        finally:
            session.close()

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        with self.lock:
            self.data["connection"] = "connected"
            self.data["last_error"] = ""
            if self._opened_once:
                self.data["total_reconnects"] += 1
            self._opened_once = True

    def _on_message(self, ws: websocket.WebSocketApp, raw: str) -> None:
        self.ingest_message(raw)

    def _on_error(self, ws: websocket.WebSocketApp, error: Any) -> None:
        self._set_status("socket error", f"{type(error).__name__}: {error}")

    def _on_close(self, ws: websocket.WebSocketApp, code: Any, message: Any) -> None:
        with self.lock:
            if self.data.get("connection") == "connected":
                self.data["total_disconnects"] += 1
            self.data["connection"] = "disconnected"
            self.data["book_ready"] = False
            self.data["bids"] = {}
            self.data["asks"] = {}

    def ingest_message(self, raw: str | bytes | dict[str, Any], received_at: float | None = None) -> bool:
        """Parse one exchange message. Public and deterministic to support offline tests."""
        if isinstance(raw, dict):
            envelope = raw
        else:
            try:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                envelope = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
                return False
        if not isinstance(envelope, dict):
            return False

        payload = envelope.get("data", envelope)
        if not isinstance(payload, dict):
            return False
        event = str(payload.get("e") or "")
        if not event:
            stream = str(envelope.get("stream") or "").lower()
            if "@aggtrade" in stream:
                event = "aggTrade"
            elif "@bookticker" in stream:
                event = "bookTicker"
            elif "@depth" in stream:
                event = "depthUpdate"
            elif "@markprice" in stream:
                event = "markPriceUpdate"
            elif "@forceorder" in stream:
                event = "forceOrder"

        allowed_events = {"aggTrade", "bookTicker", "depthUpdate", "markPriceUpdate", "forceOrder"}
        if event not in allowed_events:
            return False

        if event == "forceOrder":
            order = payload.get("o")
            if not isinstance(order, dict) or order.get("s") != SYMBOL:
                return False
        elif payload.get("s") not in (None, SYMBOL):
            return False

        now = time.time() if received_at is None else float(received_at)
        event_time = _milliseconds(payload.get("T") or payload.get("E"))
        if event == "aggTrade":
            price = _number(payload.get("p"))
            quantity = _number(payload.get("q"))
            if price is None or quantity is None or price <= 0 or quantity <= 0 or "m" not in payload:
                return False
            buyer_is_maker = payload.get("m") is True or str(payload.get("m")).lower() == "true"
            side = "SELL TAKER" if buyer_is_maker else "BUY TAKER"
            signed_quantity = -quantity if buyer_is_maker else quantity
            trade_time = _milliseconds(payload.get("T")) or int(now * 1000)
            with self.lock:
                self.data["last_price"] = price
                self.data["last_trade_time_ms"] = trade_time
                self.data["last_trade_side"] = side
                self.data["last_trade_qty_btc"] = quantity
                self.data["trade_count"] += 1
                if buyer_is_maker:
                    self.data["sell_qty_btc"] += quantity
                else:
                    self.data["buy_qty_btc"] += quantity
                self.data["cvd_btc"] += signed_quantity
                self.data["recent_trades"].appendleft({
                    "time_ms": trade_time,
                    "side": side,
                    "price_usdt": price,
                    "qty_btc": quantity,
                })
                self.data["cvd_history"].append({
                    "time_ms": trade_time,
                    "cvd_btc": self.data["cvd_btc"],
                })
        elif event == "bookTicker":
            bid = _number(payload.get("b"))
            bid_qty = _number(payload.get("B"))
            ask = _number(payload.get("a"))
            ask_qty = _number(payload.get("A"))
            if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
                return False
            with self.lock:
                self.data["bid"] = bid
                self.data["bid_qty_btc"] = bid_qty
                self.data["ask"] = ask
                self.data["ask_qty_btc"] = ask_qty
        elif event == "depthUpdate":
            bids = _levels(payload.get("b"))
            asks = _levels(payload.get("a"))
            if not bids or not asks:
                return False
            with self.lock:
                # This is the venue's top-20 partial-depth snapshot, not a locally
                # reconstructed full depth book with sequence/checksum guarantees.
                self.data["bids"] = bids
                self.data["asks"] = asks
                self.data["book_ready"] = True
                self.data["book_updated_at"] = now
        elif event == "markPriceUpdate":
            mark = _number(payload.get("p"))
            funding = _number(payload.get("r"))
            if mark is None or mark <= 0:
                return False
            with self.lock:
                self.data["mark_price"] = mark
                self.data["index_price"] = _number(payload.get("i"))
                self.data["funding_rate"] = funding
                self.data["next_funding_ms"] = _milliseconds(payload.get("T"))
                self.data["funding_updated_at"] = now
        elif event == "forceOrder":
            order = payload.get("o")
            assert isinstance(order, dict)  # validated above
            price = _number(order.get("ap")) or _number(order.get("p"))
            quantity = _number(order.get("z")) or _number(order.get("q"))
            liquidation_time = _milliseconds(order.get("T")) or event_time or int(now * 1000)
            with self.lock:
                self.data["last_liquidation"] = {
                    "side": str(order.get("S") or "unknown"),
                    "status": str(order.get("X") or "unknown"),
                    "price_usdt": price,
                    "qty_btc": quantity,
                    "time_ms": liquidation_time,
                }
                self.data["last_liquidation_received_at"] = now

        with self.lock:
            self.data["last_any_message_at"] = now
            self.data["last_market_message_at"] = now
            self.data["total_messages_received"] += 1
            seen = self.data["channel_last_seen"]
            seen[event] = now
            counts = self.data["channel_message_counts"]
            counts[event] = counts.get(event, 0) + 1
        return True
