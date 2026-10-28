"""Read-only Binance USDⓈ-M BTCUSDT market-data adapter.

Order book semantics:
- Buffer USDⓈ-M diff-depth events while obtaining the REST snapshot.
- Bridge the snapshot update ID to the buffered stream before marking the book synced.
- Thereafter require pu == previous u; any sequence discontinuity invalidates the book and triggers a fresh snapshot.
- Binance does not provide a CRC checksum on this public depth stream, so no checksum is fabricated.

This adapter is separate from DeltaFeed, Delta decisions, and the frozen OOS dataset.
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
REST_BASE_URLS = (
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
)
PUBLIC_STREAMS = (
    "btcusdt@depth@100ms/"
    "btcusdt@bookTicker"
)
MARKET_STREAMS = (
    "btcusdt@aggTrade/"
    "btcusdt@markPrice@1s/"
    "btcusdt@forceOrder/"
    "btcusdt@ticker/"
    "btcusdt@kline_1m/"
    "btcusdt@kline_5m/"
    "btcusdt@kline_15m/"
    "btcusdt@kline_1h"
)
OPEN_INTEREST_POLL_SECONDS = 15.0
MAX_DEPTH_BUFFER = 10_000
MAX_BOOK_LEVELS = 5_000
MAX_CANDLES_PER_INTERVAL = 500
TIMEFRAMES = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}
INTERVAL_TO_TIMEFRAME = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h"}


class _RegionalAccessDenied(RuntimeError):
    """An explicit HTTP access denial; do not probe alternate hosts after this."""


def _number(value: Any) -> float | None:
    try:
        if value is None:
            return None
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _positive(value: Any) -> float | None:
    result = _number(value)
    return result if result is not None and result > 0 else None


def _integer(value: Any) -> int | None:
    try:
        result = int(value)
        return result if result >= 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _normalise_levels(rows: Any) -> list[tuple[float, float]] | None:
    if not isinstance(rows, list):
        return None
    result: list[tuple[float, float]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            return None
        price, qty = _positive(row[0]), _number(row[1])
        if price is None or qty is None or qty < 0:
            return None
        result.append((price, qty))
    return result


class BinanceUSDMFeed:
    """Concurrent public WebSocket + REST feed; no credentials or order endpoints."""

    def __init__(self, symbol: str = SYMBOL, request_timeout: float = 6.0) -> None:
        symbol = str(symbol).upper().strip()
        if not symbol.isalnum():
            raise ValueError("symbol must contain only alphanumeric characters")
        self.symbol = symbol
        self.request_timeout = float(request_timeout)
        self.rest_base_urls = REST_BASE_URLS
        self.public_ws_url = (
            "wss://fstream.binance.com/public/stream?streams="
            + PUBLIC_STREAMS.replace("btcusdt", symbol.lower())
        )
        self.market_ws_url = (
            "wss://fstream.binance.com/market/stream?streams="
            + MARKET_STREAMS.replace("btcusdt", symbol.lower())
        )
        self.lock = threading.RLock()
        self._stop_event = threading.Event()
        self._started = False
        self._channel_ever_connected = {"public": False, "market": False}
        self._snapshot_worker_active = False
        self._ws_thread: threading.Thread | None = None  # market thread; retained for compatibility
        self._public_ws_thread: threading.Thread | None = None
        self._market_ws_thread: threading.Thread | None = None
        self._oi_thread: threading.Thread | None = None
        self._ws: websocket.WebSocketApp | None = None  # market socket; retained for diagnostics
        self._public_ws: websocket.WebSocketApp | None = None
        self._market_ws: websocket.WebSocketApp | None = None
        self._depth_buffer: deque[dict[str, Any]] = deque(maxlen=MAX_DEPTH_BUFFER)
        self._snapshot_update_id: int | None = None
        self._book_generation = 0

        self.data: dict[str, Any] = {
            "exchange": "Binance USDⓈ-M",
            "symbol": self.symbol,
            "connection": "not started",
            "public_connection": "not started",
            "market_connection": "not started",
            "public_error": "",
            "market_error": "",
            "last_error": "",
            "last_rest_error": "",
            "open_interest_error": "",
            "rest_host_active": None,
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
            "cvd_history": deque(maxlen=3000),
            "recent_trades": deque(maxlen=100),
            "bid": None,
            "bid_qty_btc": None,
            "ask": None,
            "ask_qty_btc": None,
            "book_bids": {},
            "book_asks": {},
            "book_synced": False,
            "book_sync_state": "not started",
            "book_update_id": None,
            "book_snapshot_update_id": None,
            "book_updated_at": None,
            "book_sequence_gaps": 0,
            "book_snapshot_count": 0,
            "book_resync_count": 0,
            "book_buffer_overflows": 0,
            "book_checksum_status": "not provided by Binance USDⓈ-M public depth stream",
            "book_source": "REST snapshot + sequenced diff-depth WebSocket",
            "mark_price": None,
            "index_price": None,
            "funding_rate": None,
            "next_funding_ms": None,
            "funding_updated_at": None,
            "ticker_24h": {},
            "open_interest_btc": None,
            "open_interest_updated_at": None,
            "last_liquidation": None,
            "last_liquidation_received_at": None,
            "liquidations": deque(maxlen=100),
            "candles_by_interval": {tf: {} for tf in TIMEFRAMES},
            "channel_last_seen": {},
            "channel_message_counts": {},
            "total_messages_received": 0,
            "total_disconnects": 0,
            "total_reconnects": 0,
        }

    def start(self) -> None:
        threads = (self._public_ws_thread, self._market_ws_thread, self._oi_thread)
        if any(thread is not None and thread.is_alive() for thread in threads):
            return
        self._stop_event.clear()
        self._started = True
        self._public_ws_thread = threading.Thread(
            target=self._run_forever,
            args=("public", self.public_ws_url),
            name=f"binance-{self.symbol.lower()}-public-ws",
            daemon=True,
        )
        self._market_ws_thread = threading.Thread(
            target=self._run_forever,
            args=("market", self.market_ws_url),
            name=f"binance-{self.symbol.lower()}-market-ws",
            daemon=True,
        )
        self._ws_thread = self._market_ws_thread
        self._oi_thread = threading.Thread(
            target=self._poll_open_interest, name=f"binance-{self.symbol.lower()}-oi", daemon=True
        )
        self._public_ws_thread.start()
        self._market_ws_thread.start()
        self._oi_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        for ws in (self._public_ws, self._market_ws):
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            out = dict(self.data)
            out["book_bids"] = dict(self.data["book_bids"])
            out["book_asks"] = dict(self.data["book_asks"])
            out["recent_trades"] = list(self.data["recent_trades"])
            out["cvd_history"] = list(self.data["cvd_history"])
            out["liquidations"] = [dict(x) for x in self.data["liquidations"]]
            latest = self.data.get("last_liquidation")
            out["last_liquidation"] = dict(latest) if isinstance(latest, dict) else None
            out["channel_last_seen"] = dict(self.data["channel_last_seen"])
            out["channel_message_counts"] = dict(self.data["channel_message_counts"])
            out["candles_by_interval"] = {
                tf: [dict(row) for _, row in sorted(candles.items())]
                for tf, candles in self.data["candles_by_interval"].items()
            }
            return out

    def analyst_context(self) -> dict[str, Any]:
        snap = self.snapshot()
        now = time.time()

        def age(value: Any) -> float | None:
            try:
                return round(max(0.0, now - float(value)), 3) if value is not None else None
            except (TypeError, ValueError, OverflowError):
                return None

        bid, ask = snap.get("bid"), snap.get("ask")
        spread = ask - bid if bid is not None and ask is not None and ask >= bid else None
        top_bids = sorted(snap["book_bids"].items(), key=lambda item: float(item[0]), reverse=True)[:10]
        top_asks = sorted(snap["book_asks"].items(), key=lambda item: float(item[0]))[:10]
        bid_depth = sum(float(q) for _, q in top_bids)
        ask_depth = sum(float(q) for _, q in top_asks)
        total_depth = bid_depth + ask_depth
        return {
            "source": {
                "exchange": "Binance USDⓈ-M",
                "symbol": self.symbol,
                "quote_asset": "USDT",
                "read_only": True,
                "not_merged_with_delta": True,
            },
            "connection": snap.get("connection"),
            "connection_details": {
                "public_websocket": snap.get("public_connection"),
                "market_websocket": snap.get("market_connection"),
                "public_error": snap.get("public_error") or None,
                "market_error": snap.get("market_error") or None,
                "last_error": snap.get("last_error") or None,
                "last_rest_error": snap.get("last_rest_error") or None,
                "rest_host_active": snap.get("rest_host_active"),
            },
            "freshness": {
                "market_message_age_seconds": age(snap.get("last_market_message_at")),
                "book_age_seconds": age(snap.get("book_updated_at")),
                "funding_age_seconds": age(snap.get("funding_updated_at")),
                "open_interest_age_seconds": age(snap.get("open_interest_updated_at")),
            },
            "price": {
                "last_trade_usdt": snap.get("last_price"),
                "mark_usdt": snap.get("mark_price"),
                "index_usdt": snap.get("index_price"),
                "best_bid_usdt": bid,
                "best_ask_usdt": ask,
                "spread_usdt": spread,
                "spread_bps": (spread / ((ask + bid) / 2) * 10000)
                    if spread is not None and (ask + bid) > 0 else None,
            },
            "order_book": {
                "synced": snap.get("book_synced"),
                "sync_state": snap.get("book_sync_state"),
                "last_update_id": snap.get("book_update_id"),
                "snapshot_update_id": snap.get("book_snapshot_update_id"),
                "bid_quantity_btc_in_local_book": round(bid_depth, 8),
                "ask_quantity_btc_in_local_book": round(ask_depth, 8),
                "top_10_book_imbalance": (bid_depth - ask_depth) / total_depth if total_depth > 0 else None,
                "initial_snapshot_depth_levels_per_side": 1000,
                "sequence_gaps": snap.get("book_sequence_gaps"),
                "resync_count": snap.get("book_resync_count"),
                "buffer_overflows": snap.get("book_buffer_overflows"),
                "rest_snapshot_error": snap.get("last_rest_error") or None,
                "checksum": "not provided by this Binance public diff-depth stream",
                "coverage_limit": "REST snapshot initially covers at most 1000 levels per side; levels outside it are unknown until changed by the stream.",
            },
            "trade_flow": {
                "trade_count_since_process_start": snap.get("trade_count"),
                "buy_aggressor_btc_since_process_start": round(float(snap.get("buy_qty_btc") or 0), 8),
                "sell_aggressor_btc_since_process_start": round(float(snap.get("sell_qty_btc") or 0), 8),
                "cvd_btc_since_process_start": round(float(snap.get("cvd_btc") or 0), 8),
                "method": "Binance aggTrade buyer-is-maker flag; session/process-local and resets on restart",
            },
            "funding": {
                "rate": snap.get("funding_rate"),
                "next_funding_time_ms": snap.get("next_funding_ms"),
            },
            "open_interest": {
                "quantity_btc": snap.get("open_interest_btc"),
                "updated_at_epoch": snap.get("open_interest_updated_at"),
                "error": snap.get("open_interest_error") or None,
            },
            "candles_by_timeframe": {
                tf: {
                    "available_bars": len(rows),
                    "last_closed_candle_time": next((x.get("time") for x in reversed(rows) if x.get("is_closed")), None),
                }
                for tf, rows in snap.get("candles_by_interval", {}).items()
            },
            "liquidation_stream": {
                "latest_observed_event": snap.get("last_liquidation"),
                "observed_event_buffer_size": len(snap.get("liquidations") or []),
                "semantics": "Binance forceOrder is a sampled per-symbol stream (at most the latest liquidation order snapshot per one-second interval), not a complete liquidation tape or predictive liquidation map.",
            },
            "limitations": [
                "This is Binance BTCUSDT context only; it is not Delta BTCUSD tape, book, or history.",
                "No order-entry, private account, or user-data endpoint is used.",
                "A synchronized diff-depth book does not have a Binance-provided checksum in this stream.",
                "CVD and liquidation observations are bounded/session-only; missing history is not reconstructed.",
            ],
        }

    def _refresh_overall_connection_locked(self) -> None:
        public = str(self.data.get("public_connection", "not started"))
        market = str(self.data.get("market_connection", "not started"))
        if public == "connected" and market == "connected":
            overall = "connected"
        elif public == "not started" and market == "not started":
            overall = "not started"
        elif public in ("socket error", "disconnected") and market in ("socket error", "disconnected"):
            overall = "socket error" if "socket error" in (public, market) else "disconnected"
        elif public in ("connecting", "reconnecting") and market in ("connecting", "reconnecting"):
            overall = "connecting" if "connecting" in (public, market) else "reconnecting"
        elif public == "connected" or market == "connected":
            overall = f"partial connection (public={public}, market={market})"
        elif public in ("connecting", "reconnecting") or market in ("connecting", "reconnecting"):
            overall = "connecting" if "connecting" in (public, market) else "reconnecting"
        else:
            overall = f"partial connection (public={public}, market={market})"
        self.data["connection"] = overall

    def _run_forever(self, channel: str, ws_url: str) -> None:
        delay = 2.0
        while not self._stop_event.is_set():
            with self.lock:
                self.data[f"{channel}_connection"] = "connecting"
                self._refresh_overall_connection_locked()
            try:
                ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=lambda socket, ch=channel: self._on_channel_open(ch, socket),
                    on_message=self._on_message,
                    on_error=lambda socket, error, ch=channel: self._on_channel_error(ch, error),
                    on_close=lambda socket, code, message, ch=channel: self._on_channel_close(ch, socket, code, message),
                )
                with self.lock:
                    if channel == "public":
                        self._public_ws = ws
                    else:
                        self._market_ws = ws
                        self._ws = ws
                ws.run_forever(ping_interval=120, ping_timeout=30, skip_utf8_validation=True)
            except Exception as exc:
                self._set_channel_status(channel, "socket error", f"{type(exc).__name__}: {exc}")
            if self._stop_event.is_set():
                break
            with self.lock:
                self.data[f"{channel}_connection"] = "reconnecting"
                if channel == "public":
                    self._invalidate_book_locked("Public depth WebSocket disconnected; awaiting new snapshot", count_gap=False)
                self._refresh_overall_connection_locked()
            self._stop_event.wait(delay)
            delay = min(delay * 1.6, 20.0)

    def _set_channel_status(self, channel: str, status: str, error: str | None = None) -> None:
        with self.lock:
            self.data[f"{channel}_connection"] = status
            if error is not None:
                self.data[f"{channel}_error"] = str(error)[:350]
                self.data["last_error"] = f"{channel} WebSocket: {error}"[:350]
            self._refresh_overall_connection_locked()

    def _on_channel_open(self, channel: str, ws: websocket.WebSocketApp) -> None:
        with self.lock:
            self.data[f"{channel}_connection"] = "connected"
            self.data[f"{channel}_error"] = ""
            if self.data.get(f"{channel}_error") == "" and not self.data.get("market_error") and not self.data.get("public_error"):
                self.data["last_error"] = ""
            if self._channel_ever_connected[channel]:
                self.data["total_reconnects"] += 1
            self._channel_ever_connected[channel] = True
            if channel == "public":
                self._invalidate_book_locked("Public depth WebSocket connected; awaiting fresh snapshot", count_gap=False)
                if self.data.get("market_error"):
                    self.data["last_error"] = f"market WebSocket: {self.data['market_error']}"[:350]
                else:
                    self.data["last_error"] = ""
            self._refresh_overall_connection_locked()
        if channel == "public":
            self._schedule_snapshot_worker()

    def _on_message(self, ws: websocket.WebSocketApp, raw: str) -> None:
        self.ingest_message(raw)

    def _on_channel_error(self, channel: str, error: Any) -> None:
        self._set_channel_status(channel, "socket error", f"{type(error).__name__}: {error}")

    def _on_channel_close(self, channel: str, ws: websocket.WebSocketApp, code: Any, message: Any) -> None:
        with self.lock:
            if self.data.get(f"{channel}_connection") == "connected" and not self._stop_event.is_set():
                self.data["total_disconnects"] += 1
            self.data[f"{channel}_connection"] = "disconnected"
            if channel == "public":
                self._invalidate_book_locked("Public depth WebSocket closed; book invalidated", count_gap=False)
            self._refresh_overall_connection_locked()

    def _invalidate_book_locked(self, reason: str, *, count_gap: bool) -> None:
        self._book_generation += 1
        if count_gap:
            self.data["book_sequence_gaps"] += 1
            self.data["book_resync_count"] += 1
        self.data["book_synced"] = False
        self.data["book_sync_state"] = "resync requested" if count_gap else "not synchronized"
        self.data["book_bids"] = {}
        self.data["book_asks"] = {}
        self.data["book_update_id"] = None
        self._snapshot_update_id = None
        self._depth_buffer.clear()
        self.data["last_error"] = str(reason)[:350]

    def _schedule_snapshot_worker(self) -> None:
        with self.lock:
            if self._stop_event.is_set() or self._snapshot_worker_active:
                return
            self._snapshot_worker_active = True
        threading.Thread(
            target=self._snapshot_worker, name=f"binance-{self.symbol.lower()}-depth-snapshot", daemon=True
        ).start()

    def _fetch_depth_snapshot(self) -> tuple[dict[str, Any], str]:
        errors: list[str] = []
        session = requests.Session()
        try:
            for base_url in self.rest_base_urls:
                if self._stop_event.is_set():
                    raise RuntimeError("stop requested")
                try:
                    response = session.get(
                        f"{base_url}/fapi/v1/depth",
                        params={"symbol": self.symbol, "limit": 1000},
                        headers={"Accept": "application/json", "User-Agent": "BTCReferenceFeed/2.0"},
                        timeout=self.request_timeout,
                    )
                    if response.status_code in (403, 451):
                        raise _RegionalAccessDenied(
                            f"HTTP {response.status_code} access denied by Binance/this network; "
                            "stopping REST host fallback rather than probing alternate hosts."
                        )
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, dict) or _integer(payload.get("lastUpdateId")) is None:
                        raise ValueError("depth snapshot missing valid lastUpdateId")
                    if _normalise_levels(payload.get("bids")) is None or _normalise_levels(payload.get("asks")) is None:
                        raise ValueError("depth snapshot has malformed levels")
                    return payload, base_url
                except _RegionalAccessDenied as exc:
                    errors.append(f"{base_url}: {type(exc).__name__}: {exc}")
                    raise _RegionalAccessDenied(
                        "Binance USDⓈ-M depth snapshot unavailable after explicit access denial: "
                        + " | ".join(errors)
                    ) from exc
                except Exception as exc:
                    errors.append(f"{base_url}: {type(exc).__name__}: {exc}")
            raise RuntimeError("Binance USDⓈ-M depth snapshot unavailable: " + " | ".join(errors))
        finally:
            session.close()

    def install_snapshot_payload(
        self,
        payload: dict[str, Any],
        *,
        rest_host: str = "offline-test",
        received_at: float | None = None,
        expected_generation: int | None = None,
    ) -> bool:
        """Install a supplied snapshot and replay buffered updates; also used in socket-free tests."""
        if not isinstance(payload, dict):
            return False
        bids = _normalise_levels(payload.get("bids"))
        asks = _normalise_levels(payload.get("asks"))
        last_id = _integer(payload.get("lastUpdateId"))
        if bids is None or asks is None or last_id is None:
            return False
        now = time.time() if received_at is None else float(received_at)
        with self.lock:
            if expected_generation is not None:
                if expected_generation != self._book_generation or self.data.get("public_connection") != "connected":
                    return False
            self.data["book_bids"] = {price: qty for price, qty in bids if qty > 0}
            self.data["book_asks"] = {price: qty for price, qty in asks if qty > 0}
            self.data["book_synced"] = False
            self.data["book_update_id"] = last_id
            self._snapshot_update_id = last_id
            self.data["book_snapshot_update_id"] = last_id
            self.data["book_updated_at"] = now
            self.data["book_snapshot_count"] += 1
            self.data["rest_host_active"] = rest_host
            self.data["last_rest_error"] = ""
            self.data["book_sync_state"] = "snapshot installed; waiting for sequence bridge"
            buffered = list(self._depth_buffer)
            self._depth_buffer.clear()
            for event in buffered:
                if self._process_depth_event_locked(event):
                    break
            needs_resync = self.data.get("book_sync_state") == "resync requested"
            synced = bool(self.data.get("book_synced"))
        if needs_resync and self._started:
            self._schedule_snapshot_worker()
        return synced

    def _snapshot_worker(self) -> None:
        retry_delay = 2.0
        try:
            while not self._stop_event.is_set():
                with self.lock:
                    if self.data.get("public_connection") != "connected":
                        return
                    generation = self._book_generation
                    self.data["book_sync_state"] = "fetching REST snapshot"
                try:
                    snapshot, host = self._fetch_depth_snapshot()
                except _RegionalAccessDenied as exc:
                    if self._stop_event.is_set():
                        return
                    with self.lock:
                        self.data["last_rest_error"] = f"{type(exc).__name__}: {exc}"[:700]
                        self.data["book_sync_state"] = "REST access denied (HTTP 403/451); depth unavailable"
                        self.data["last_error"] = self.data["book_sync_state"]
                    return
                except Exception as exc:
                    if self._stop_event.is_set():
                        return
                    with self.lock:
                        self.data["last_rest_error"] = f"{type(exc).__name__}: {exc}"[:700]
                        self.data["book_sync_state"] = "snapshot fetch failed; retrying"
                    self._stop_event.wait(retry_delay)
                    retry_delay = min(retry_delay * 1.8, 15.0)
                    continue

                self.install_snapshot_payload(
                    snapshot,
                    rest_host=host,
                    expected_generation=generation,
                )
                with self.lock:
                    if self.data.get("public_connection") != "connected" or self._stop_event.is_set():
                        return
                    if generation != self._book_generation or self.data.get("book_sync_state") == "resync requested":
                        retry = True
                    else:
                        retry = False
                if not retry:
                    return
                retry_delay = 0.5
                self._stop_event.wait(retry_delay)
        finally:
            with self.lock:
                self._snapshot_worker_active = False
                restart = (
                    not self._stop_event.is_set()
                    and self.data.get("public_connection") == "connected"
                    and self.data.get("book_sync_state") == "resync requested"
                )
            if restart:
                self._schedule_snapshot_worker()

    def _parse_payload(self, raw: str | bytes | dict[str, Any]) -> dict[str, Any] | None:
        if isinstance(raw, dict):
            envelope = raw
        else:
            try:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                envelope = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
                return None
        if not isinstance(envelope, dict):
            return None
        payload = envelope.get("data", envelope)
        return payload if isinstance(payload, dict) else None

    def ingest_message(self, raw: str | bytes | dict[str, Any], received_at: float | None = None) -> bool:
        payload = self._parse_payload(raw)
        if not isinstance(payload, dict):
            return False
        event = str(payload.get("e") or "")
        now = time.time() if received_at is None else float(received_at)
        if not event:
            return False
        event_symbol = payload.get("s")
        if event == "forceOrder":
            order = payload.get("o")
            if not isinstance(order, dict) or order.get("s") != self.symbol:
                return False
        elif event_symbol not in (None, self.symbol):
            return False
        if event == "depthUpdate":
            return self._ingest_depth(payload, now)
        known = {"aggTrade", "bookTicker", "markPriceUpdate", "forceOrder", "kline", "24hrTicker"}
        if event not in known:
            return False

        with self.lock:
            if event == "aggTrade":
                price = _positive(payload.get("p"))
                qty = _positive(payload.get("q"))
                if price is None or qty is None or "m" not in payload:
                    return False
                buyer_is_maker = payload.get("m") is True or str(payload.get("m")).lower() == "true"
                side = "SELL TAKER" if buyer_is_maker else "BUY TAKER"
                signed_qty = -qty if buyer_is_maker else qty
                trade_time = _integer(payload.get("T")) or int(now * 1000)
                self.data["last_price"] = price
                self.data["last_trade_time_ms"] = trade_time
                self.data["last_trade_side"] = side
                self.data["last_trade_qty_btc"] = qty
                self.data["trade_count"] += 1
                if buyer_is_maker:
                    self.data["sell_qty_btc"] += qty
                else:
                    self.data["buy_qty_btc"] += qty
                self.data["cvd_btc"] += signed_qty
                self.data["recent_trades"].appendleft({
                    "time_ms": trade_time, "side": side, "price_usdt": price, "qty_btc": qty,
                })
                self.data["cvd_history"].append({
                    "time_ms": trade_time, "cvd_btc": self.data["cvd_btc"],
                })

            elif event == "bookTicker":
                bid, ask = _positive(payload.get("b")), _positive(payload.get("a"))
                bid_qty, ask_qty = _number(payload.get("B")), _number(payload.get("A"))
                if bid is None or ask is None or ask < bid or bid_qty is None or ask_qty is None or min(bid_qty, ask_qty) < 0:
                    return False
                self.data["bid"], self.data["ask"] = bid, ask
                self.data["bid_qty_btc"], self.data["ask_qty_btc"] = bid_qty, ask_qty

            elif event == "markPriceUpdate":
                mark, index = _positive(payload.get("p")), _positive(payload.get("i"))
                funding = _number(payload.get("r"))
                if mark is None or funding is None:
                    return False
                self.data["mark_price"] = mark
                self.data["index_price"] = index
                self.data["funding_rate"] = funding
                self.data["next_funding_ms"] = _integer(payload.get("T"))
                self.data["funding_updated_at"] = now

            elif event == "24hrTicker":
                self.data["ticker_24h"] = {
                    "price_change_percent": _number(payload.get("P")),
                    "high_price": _positive(payload.get("h")),
                    "low_price": _positive(payload.get("l")),
                    "base_volume_btc": _number(payload.get("v")),
                    "quote_volume_usdt": _number(payload.get("q")),
                    "event_time_ms": _integer(payload.get("E")),
                }

            elif event == "forceOrder":
                order = payload.get("o") or {}
                price = _positive(order.get("ap")) or _positive(order.get("p"))
                qty = _number(order.get("z"))
                if qty is None:
                    qty = _number(order.get("l"))
                if qty is None:
                    qty = _number(order.get("q"))
                if qty is not None and qty < 0:
                    qty = None
                event_time = _integer(order.get("T")) or _integer(payload.get("E")) or int(now * 1000)
                observation = {
                    "side": str(order.get("S") or "unknown"),
                    "status": str(order.get("X") or "unknown"),
                    "price_usdt": price,
                    "qty_btc": qty,
                    "time_ms": event_time,
                    "received_at": now,
                    "semantics": "sampled forceOrder event; not a complete liquidation tape",
                }
                self.data["last_liquidation"] = observation
                self.data["last_liquidation_received_at"] = now
                self.data["liquidations"].appendleft(observation)

            elif event == "kline":
                kline = payload.get("k")
                if not isinstance(kline, dict):
                    return False
                interval = str(kline.get("i") or "")
                timeframe = INTERVAL_TO_TIMEFRAME.get(interval)
                if timeframe is None:
                    return False
                start_ms = _integer(kline.get("t"))
                op, hi, lo, close = (_positive(kline.get(key)) for key in ("o", "h", "l", "c"))
                volume = _number(kline.get("v"))
                if start_ms is None or any(x is None for x in (op, hi, lo, close)) or volume is None or volume < 0:
                    return False
                row = {
                    "time": start_ms / 1000.0,
                    "time_ms": start_ms,
                    "close_time_ms": _integer(kline.get("T")),
                    "open": op,
                    "high": hi,
                    "low": lo,
                    "close": close,
                    "volume": volume,
                    "quote_volume_usdt": _number(kline.get("q")),
                    "taker_buy_volume_btc": _number(kline.get("V")),
                    "is_closed": bool(kline.get("x")),
                    "source": "Binance USDⓈ-M WebSocket kline",
                }
                candles = self.data["candles_by_interval"][timeframe]
                candles[start_ms] = row
                if len(candles) > MAX_CANDLES_PER_INTERVAL:
                    for old_key in sorted(candles)[:-MAX_CANDLES_PER_INTERVAL]:
                        candles.pop(old_key, None)

            # Advance freshness only after this supported event passed validation.
            self.data["last_any_message_at"] = now
            self.data["last_market_message_at"] = now
            self.data["total_messages_received"] += 1
            counts = self.data["channel_message_counts"]
            counts[event] = counts.get(event, 0) + 1
            self.data["channel_last_seen"][event] = now
        return True

    def _ingest_depth(self, payload: dict[str, Any], now: float) -> bool:
        if payload.get("s") not in (None, self.symbol):
            return False
        if _integer(payload.get("U")) is None or _integer(payload.get("u")) is None:
            return False
        with self.lock:
            self.data["last_any_message_at"] = now
            self.data["last_market_message_at"] = now
            self.data["total_messages_received"] += 1
            counts = self.data["channel_message_counts"]
            counts["depthUpdate"] = counts.get("depthUpdate", 0) + 1
            self.data["channel_last_seen"]["depthUpdate"] = now
            if self._snapshot_update_id is None:
                if len(self._depth_buffer) == MAX_DEPTH_BUFFER:
                    self.data["book_buffer_overflows"] += 1
                    self._invalidate_book_locked("Depth buffer overflow before snapshot; resynchronization required", count_gap=True)
                self._depth_buffer.append(dict(payload))
                schedule = True
                resync = False
            else:
                resync = self._process_depth_event_locked(payload)
                schedule = resync
        if schedule and self._started:
            self._schedule_snapshot_worker()
        return True

    def _process_depth_event_locked(self, event: dict[str, Any]) -> bool:
        first_id = _integer(event.get("U"))
        final_id = _integer(event.get("u"))
        previous_id = _integer(event.get("pu"))
        snapshot_id = self._snapshot_update_id
        current_id = _integer(self.data.get("book_update_id"))
        if first_id is None or final_id is None or snapshot_id is None or current_id is None:
            return False
        if final_id < first_id:
            self._invalidate_book_locked("Invalid diff-depth update ID range", count_gap=True)
            self._depth_buffer.append(dict(event))
            return True
        if not self.data.get("book_synced"):
            if final_id <= snapshot_id:
                return False
            overlaps_snapshot = first_id <= snapshot_id <= final_id
            links_to_snapshot = previous_id == snapshot_id
            if not (overlaps_snapshot or links_to_snapshot):
                if first_id > snapshot_id:
                    self._invalidate_book_locked(
                        f"Depth sequence did not bridge snapshot {snapshot_id}: U={first_id}, u={final_id}, pu={previous_id}",
                        count_gap=True,
                    )
                    self._depth_buffer.append(dict(event))
                    return True
                return False
        else:
            if final_id <= current_id:
                return False
            if previous_id is not None:
                if previous_id != current_id:
                    self._invalidate_book_locked(
                        f"Depth sequence gap: expected pu={current_id}, received pu={previous_id}, u={final_id}",
                        count_gap=True,
                    )
                    self._depth_buffer.append(dict(event))
                    return True
            elif first_id > current_id + 1:
                self._invalidate_book_locked(
                    f"Depth sequence gap without pu: current={current_id}, U={first_id}, u={final_id}",
                    count_gap=True,
                )
                self._depth_buffer.append(dict(event))
                return True

        bids = _normalise_levels(event.get("b"))
        asks = _normalise_levels(event.get("a"))
        if bids is None or asks is None:
            self._invalidate_book_locked("Malformed depth level in diff update", count_gap=True)
            self._depth_buffer.append(dict(event))
            return True
        for price, qty in bids:
            if qty == 0:
                self.data["book_bids"].pop(price, None)
            else:
                self.data["book_bids"][price] = qty
        for price, qty in asks:
            if qty == 0:
                self.data["book_asks"].pop(price, None)
            else:
                self.data["book_asks"][price] = qty
        if len(self.data["book_bids"]) > MAX_BOOK_LEVELS:
            self.data["book_bids"] = dict(sorted(self.data["book_bids"].items(), reverse=True)[:MAX_BOOK_LEVELS])
        if len(self.data["book_asks"]) > MAX_BOOK_LEVELS:
            self.data["book_asks"] = dict(sorted(self.data["book_asks"].items())[:MAX_BOOK_LEVELS])
        self.data["book_update_id"] = final_id
        self.data["book_synced"] = True
        self.data["book_sync_state"] = "synchronized (REST snapshot + contiguous diff stream)"
        self.data["book_updated_at"] = time.time()
        return False

    def _poll_open_interest(self) -> None:
        session = requests.Session()
        try:
            while not self._stop_event.is_set():
                errors = []
                succeeded = False
                access_denied = False
                for base_url in self.rest_base_urls:
                    if self._stop_event.is_set():
                        break
                    try:
                        response = session.get(
                            f"{base_url}/fapi/v1/openInterest",
                            params={"symbol": self.symbol},
                            headers={"Accept": "application/json", "User-Agent": "BTCReferenceFeed/2.0"},
                            timeout=self.request_timeout,
                        )
                        if response.status_code in (403, 451):
                            raise _RegionalAccessDenied(
                                f"HTTP {response.status_code} access denied by Binance/this network; "
                                "stopping REST host fallback rather than probing alternate hosts."
                            )
                        response.raise_for_status()
                        payload = response.json()
                        value = _positive(payload.get("openInterest")) if isinstance(payload, dict) else None
                        if value is None:
                            raise ValueError("open-interest response missing valid openInterest")
                        with self.lock:
                            self.data["open_interest_btc"] = value
                            self.data["open_interest_updated_at"] = time.time()
                            self.data["open_interest_error"] = ""
                            self.data["rest_host_active"] = base_url
                        succeeded = True
                        break
                    except _RegionalAccessDenied as exc:
                        errors.append(f"{base_url}: {type(exc).__name__}: {exc}")
                        access_denied = True
                        break
                    except Exception as exc:
                        errors.append(f"{base_url}: {type(exc).__name__}: {exc}")
                if not succeeded and not self._stop_event.is_set():
                    with self.lock:
                        self.data["open_interest_error"] = " | ".join(errors)[:700]
                if access_denied:
                    return
                self._stop_event.wait(OPEN_INTEREST_POLL_SECONDS)
        finally:
            session.close()
