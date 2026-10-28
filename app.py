from __future__ import annotations

import json
import logging
import os
import threading
import time
import zlib
from collections import deque
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
import websocket

import orderflow
from signal_integration_bridge import evaluate_feed_snapshot
from ai_analyst import render_ai_analyst
from bybit_panel import render_bybit_cross_venue_panel
from okx_history_panel import render_okx_history_panel
from binance_panel import render_binance_cross_venue_panel

EVENT_LOG_FILE = os.environ.get("DELTA_EVENT_LOG_FILE", "/Users/targetmobile/Downloads/delta_feed_events.jsonl")
EVENT_LOG_FILE_FULL = os.environ.get("DELTA_EVENT_LOG_FILE_FULL", "/Users/targetmobile/Downloads/delta_feed_events_full.jsonl")
DECISION_LOG_FILE = os.environ.get("DELTA_DECISION_LOG_FILE", "/Users/targetmobile/Downloads/signal_decisions.jsonl")
_event_lock = threading.Lock()
_diagnostic_reconnect_triggered = False

def _log_event(event: dict, to_full_log: bool = False, is_decision: bool = False) -> bool:
    """Append JSONL events and return whether every required write succeeded."""
    import sys
    record = dict(event)
    record.setdefault("ts_utc", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    try:
        event_str = json.dumps(record, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        print(f"delta-feed ERROR: refusing non-JSON event ({type(exc).__name__})", file=sys.stderr)
        return False
    paths = [EVENT_LOG_FILE]
    if to_full_log:
        paths.append(EVENT_LOG_FILE_FULL)
    if is_decision:
        paths.append(DECISION_LOG_FILE)
    all_ok = True
    with _event_lock:
        for target in paths:
            try:
                os.makedirs(os.path.dirname(os.path.abspath(target)), exist_ok=True)
                with open(target, "a", encoding="utf-8") as handle:
                    handle.write(event_str + "\n")
                    handle.flush()
                    if is_decision:
                        os.fsync(handle.fileno())
            except OSError as exc:
                all_ok = False
                print(f"delta-feed ERROR: failed to persist {target!r}: {type(exc).__name__}: {exc}", file=sys.stderr)
    return all_ok



def _evaluate_signal_decision(
    feed_state: dict[str, Any],
    signal_type: str,
    score: float,
    confidence: float = 1.0,
    blocking_reason: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate a signal decision against feed-health state and log for audit.

    Args:
        feed_state: Current feed state from DeltaFeed.snapshot()
        signal_type: 'BUY', 'SELL', 'WAIT', or 'BLOCKED'
        score: Signal quality score (0-100)
        confidence: Decision confidence level
        blocking_reason: If blocked, the reason for the block
        metadata: Additional signal metadata

    Returns:
        Decision record dictionary with feed-health state

    Feeds blocked if any integrity check fails:
    - Connection not established or lost
    - Order book not synced within stale threshold
    - Sequence gap detected
    - CRC32 verification failed
    - Channel lag exceeds threshold
    """
    now = time.time()
    snapshot = {
        "connection": feed_state.get("connection", "unknown"),
        "book_synced": feed_state.get("book_synced", False),
        "checksum_ok": feed_state.get("checksum_ok", None),
        "checksum_msg": feed_state.get("checksum_msg", ""),
        "book_seq": feed_state.get("book_seq"),
        "sequence_gaps": feed_state.get("sequence_gaps", 0),
        "checksum_failures": feed_state.get("checksum_failures", 0),
        "last_error": feed_state.get("last_error", ""),
        "total_messages_received": feed_state.get("total_messages_received", 0),
        "channel_message_counts": dict(feed_state.get("channel_message_counts", {})),
        "channel_last_seen": dict(feed_state.get("channel_last_seen", {})),
        "last_market_message_at": feed_state.get("last_market_message_at"),
        "last_any_message_at": feed_state.get("last_any_message_at"),
        "total_disconnects": feed_state.get("total_disconnects", 0),
        "total_reconnects": feed_state.get("total_reconnects", 0),
    }

    last_msg = feed_state.get("last_any_message_at")
    if last_msg is not None and isinstance(last_msg, (int, float)) and last_msg > 0:
        feed_age = now - last_msg
    else:
        feed_age = None

    is_blocked = False
    block_reasons: list[str] = []

    if feed_state.get("connection") != "connected":
        is_blocked = True
        block_reasons.append("not_connected")
    if not feed_state.get("book_synced", False):
        is_blocked = True
        block_reasons.append("book_not_synced")
    if feed_state.get("checksum_ok") is False:
        is_blocked = True
        block_reasons.append("crc_verification_failed")
    if feed_age is not None and feed_age > CHANNEL_STALE_SECONDS.get("ticker", 8):
        is_blocked = True
        block_reasons.append("channel_stale")

    final_decision = "BLOCKED" if is_blocked else signal_type
    effective_blocking_reason = blocking_reason or ("; ".join(block_reasons) if block_reasons else None)

    decision_record = {
        "decision": final_decision,
        "signal_type": signal_type,
        "score": score,
        "confidence": confidence,
        "blocking_reason": effective_blocking_reason,
        "feed_fresh": feed_state.get("connection") == "connected",
        "book_synced": feed_state.get("book_synced", False),
        "sequence_valid": feed_state.get("book_synced", False),  # Implicit: if synced, sequence is valid
        "crc_valid": feed_state.get("checksum_ok", True),  # CURRENT verification state
        "feed_age_seconds": feed_age,
        "book_seq": feed_state.get("book_seq"),
        "total_disconnects": feed_state.get("total_disconnects", 0),
        "total_reconnects": feed_state.get("total_reconnects", 0),
        "sequence_gaps_cumul": feed_state.get("sequence_gaps", 0),  # AUDIT: cumulative gaps (not gating)
        "checksum_failures_cumul": feed_state.get("checksum_failures", 0),  # AUDIT: cumulative failures (not gating)
        "feed_state": snapshot,
    }

    if metadata:
        decision_record["metadata"] = metadata

    _log_event(decision_record, is_decision=True)

    return decision_record

_logger = logging.getLogger("delta_feed")
if not _logger.handlers:
    _fmt = logging.Formatter("%(asctime)s %(levelname)s [delta-feed] %(message)s")
    try:
        _log_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        _log_dir = os.getcwd()
    try:
        _fh = RotatingFileHandler(os.path.join(_log_dir, "delta_feed.log"), maxBytes=2_000_000, backupCount=2)
        _fh.setFormatter(_fmt)
        _logger.addHandler(_fh)
    except Exception as e:
        import sys as _sys
        print(f"delta-feed WARNING: Failed to initialize file handler: {e}. Falling back to stderr.", file=_sys.stderr)
    _sh = logging.StreamHandler()
    _sh.setFormatter(_fmt)
    _logger.addHandler(_sh)
    _logger.setLevel(logging.INFO)
    file_handler_enabled = any(
        isinstance(handler, RotatingFileHandler)
        for handler in _logger.handlers
    )
    _logger.info(
        "Logging initialized. File handler: %s; stream handler: enabled",
        "enabled" if file_handler_enabled else "disabled",
    )
_logger.propagate = False

# Public, read-only Delta Exchange India market-data endpoint.
WS_URL = "wss://public-socket.india.delta.exchange"
REST_URL = "https://api.india.delta.exchange/v2/history/candles"
SYMBOL = "BTCUSD"              # Delta India BTC perpetual, INR-settled
INDEX_SYMBOL = ".DEXBTUSD"    # Delta's BTCUSD underlying index symbol
TZ_IST = ZoneInfo("Asia/Kolkata")
CHANNEL_STALE_SECONDS = {
    "ticker": 8,
    "ob_updates": 3,
    "trades": 20,
    "mark_price": 6,
    "spot_price": 15,
    "candlestick_1m": 90,
    "funding_rate": None,
    "system_status": None,
}
BOOK_TICK = 0.5                # BTCUSD tick size (Delta product metadata, verified 2026-10-12)
CONTRACT_VALUE_BTC = 0.001     # 1 contract = 0.001 BTC (Delta product metadata, verified 2026-10-12)
LIQ_BAND_DISTANCE = 2          # tick multiples around a cluster when flagging nearby resting liquidity

st.set_page_config(
    page_title="Delta India | BTC Perpetual Flow Terminal",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
      .block-container {padding: 0.6rem 1.1rem 1rem; max-width: 2400px;}
      [data-testid="stMetric"] {background: rgba(120,120,120,.07); padding: 12px 14px; border-radius: 9px;}
      .smallnote {font-size: .82rem; opacity: .78;}
      div[data-testid="stDataFrame"] {border-radius: 8px; overflow: hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)


def _f(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _ts_seconds(value: Any) -> int | None:
    """Delta WebSocket timestamps are microseconds; REST candle times are seconds."""
    try:
        number = int(value)
        if number <= 0:
            return None
        return number // 1_000_000 if number > 10_000_000_000 else number
    except (TypeError, ValueError):
        return None


@st.cache_data(ttl=240, show_spinner="Loading recent BTCUSD candles…")
def load_initial_candles() -> list[dict[str, float | int]]:
    """Fetch up to 2000 recent one-minute candles for chart and signal warm-up."""
    end = int(time.time())
    start = end - 2000 * 60
    try:
        response = requests.get(
            REST_URL,
            params={"resolution": "1m", "symbol": SYMBOL, "start": start, "end": end},
            headers={"Accept": "application/json", "User-Agent": "DeltaIndiaBTCFlowTerminal/1.0"},
            timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result", []) if isinstance(payload, dict) else []
        candles: list[dict[str, float | int]] = []
        for row in result:
            if not isinstance(row, dict):
                continue
            timestamp = _ts_seconds(row.get("time"))
            o, h, low, c = (_f(row.get(k)) for k in ("open", "high", "low", "close"))
            if timestamp is None or None in (o, h, low, c):
                continue
            bucket = (timestamp // 60) * 60
            candles.append({
                "time": bucket,
                "open": float(o),
                "high": float(h),
                "low": float(low),
                "close": float(c),
                "volume": float(_f(row.get("volume"), 0.0) or 0.0),
            })
        return candles[-2000:]
    except Exception:
        # The websocket chart can still populate live when historical REST data is unavailable.
        return []


class DeltaFeed:
    """Read-only Delta India WebSocket consumer with sequence and CRC32 book checks."""

    def __init__(self, initial_candles: list[dict[str, float | int]] | None = None):
        self.lock = threading.RLock()
        now = time.time()
        candles = {}
        for row in initial_candles or []:
            try:
                candles[int(row["time"])] = dict(row)
            except (KeyError, TypeError, ValueError):
                continue
        self.data: dict[str, Any] = {
            "connection": "starting",
            "last_error": "",
            "started_at": now,
            "last_any_message_at": None,
            "last_market_message_at": None,
            "last_market_type": "",
            "last_price": None,
            "mark_price": None,
            "index_price": None,
            "bid": None,
            "ask": None,
            "bid_size": None,
            "ask_size": None,
            "funding_rate": None,
            "funding_interval_seconds": None,
            "next_funding_at": None,
            "open_interest_contracts": None,
            "ticker_24h": {},
            "asks": {},
            "bids": {},
            "book_seq": None,
            "book_synced": False,
            "sequence_gap_unresolved": True,
            "checksum_ok": None,
            "checksum_msg": "Awaiting first book snapshot",
            "ob_snapshots": 0,
            "book_updates": 0,
            "ob_updates": 0,
            "sequence_gaps": 0,
            "checksum_failures": 0,
            "total_messages_received": 0,
            "trades": deque(maxlen=120),
            "candles": candles,
            "cvd": deque(maxlen=5000),
            "cvd_value": 0.0,
            "buy_contracts": 0.0,
            "sell_contracts": 0.0,
            "trade_count": 0,
            "system_status": "No status event received",
            "recent_events": deque(maxlen=12),
            "channel_last_seen": {},
            "channel_message_counts": {},
            "total_disconnects": 0,
            "total_reconnects": 0,
            # --- order-flow tracking (added 2026-10-13 upgrade) ---
            "book_version": 0,            # bumped on every applied book message (render cache key)
            "trade_log": deque(maxlen=20000),     # (exchange_ts, price, contracts, buy/sell)
            # Per-minute aggressor aggregates retain candle-level flow after individual
            # prints roll out of trade_log. Kept in memory for 48 hours; never persisted.
            "trade_flow_minutes": {},              # minute epoch -> side counts + summed contracts
            "trade_flow_latest_minute": None,
            "trade_sizes": deque(maxlen=3000),    # rolling sizes for the percentile threshold
            "large_trades": deque(maxlen=300),    # executed trades above the adaptive threshold
            "trade_clusters": deque(maxlen=100),  # finalized aggressive clusters (+ price response)
            "open_clusters": {},                  # side -> cluster still accepting trades
            "depth_samples": deque(maxlen=6000),  # (ts, side, price, size) significant bands
            "level_stats": {},   # (side, price) -> first/last seen, max size (bounded, pruned)
            "book_liq_threshold": None,           # adaptive book-significance threshold (contracts)
            "large_trade_threshold": None,        # adaptive large-trade threshold (contracts)
        }
        self.thread: threading.Thread | None = None
        self.ws: websocket.WebSocketApp | None = None
        self.resubscribe_lock = threading.Lock()
        self.last_resubscribe = 0.0
        self._first_msg_channels: set[str] = set()
        self._checksum_logged = False
        self._hthread: threading.Thread | None = None
        # order-flow sampler settings (leaf values; safe to write from the UI thread)
        self.sample_interval = 2.0
        self.cluster_window_s = 60.0
        self.response_window_s = 30.0
        self.liq_percentile = 95.0
        self.liq_band_ticks = 2
        self.large_percentile = 95.0
        self._sthread: threading.Thread | None = None

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run_forever, name="delta-public-ws", daemon=True)
        self.thread.start()
        self._hthread = threading.Thread(target=self._health_loop, name="delta-health", daemon=True)
        self._hthread.start()
        if not (self._sthread and self._sthread.is_alive()):
            self._sthread = threading.Thread(target=self._sample_loop, name="delta-depth-sampler", daemon=True)
            self._sthread.start()

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            copy = dict(self.data)
            copy["trades"] = list(self.data["trades"])
            copy["candles"] = [dict(x) for _, x in sorted(self.data["candles"].items())]
            copy["cvd"] = list(self.data["cvd"])
            copy["recent_events"] = list(self.data["recent_events"])
            copy["asks"] = dict(self.data["asks"])
            copy["bids"] = dict(self.data["bids"])
            copy["channel_last_seen"] = dict(self.data["channel_last_seen"])
            copy["trade_log"] = list(self.data["trade_log"])
            copy["trade_flow_minutes"] = {
                int(minute): dict(flow)
                for minute, flow in self.data["trade_flow_minutes"].items()
            }
            copy["large_trades"] = [dict(t) for t in self.data["large_trades"]]
            copy["trade_clusters"] = [dict(c) for c in self.data["trade_clusters"]]
            copy["open_clusters"] = [dict(c) for c in self.data["open_clusters"].values()]
            copy["depth_samples"] = list(self.data["depth_samples"])[-1200:]
            copy["level_stats"] = {k: dict(v) for k, v in self.data["level_stats"].items()}
            copy["book_liq_threshold"] = self.data["book_liq_threshold"]
            copy["large_trade_threshold"] = self.data["large_trade_threshold"]
            return copy

    def get_signal_state(self) -> dict[str, Any]:
        """Return lightweight feed state optimized for signal decision logging.

        Returns only the fields needed for signal-safety correlation:
        - connection status
        - book synchronization state
        - sequence/checksum integrity
        - channel freshness metrics
        """
        with self.lock:
            now = time.time()
            last_msg = self.data.get("last_any_message_at")
            return {
                "connection": self.data.get("connection", "unknown"),
                "book_synced": self.data.get("book_synced", False),
                "sequence_gap_unresolved": self.data.get("sequence_gap_unresolved", True),
                "checksum_ok": self.data.get("checksum_ok"),
                "checksum_msg": self.data.get("checksum_msg", ""),
                "book_seq": self.data.get("book_seq"),
                "sequence_gaps": self.data.get("sequence_gaps", 0),
                "checksum_failures": self.data.get("checksum_failures", 0),
                "total_disconnects": self.data.get("total_disconnects", 0),
                "total_reconnects": self.data.get("total_reconnects", 0),
                "total_messages_received": self.data.get("total_messages_received", 0),
                "channel_message_counts": dict(self.data.get("channel_message_counts", {})),
                "last_any_message_at": last_msg,
                "last_market_message_at": self.data.get("last_market_message_at"),
                "channel_last_seen": dict(self.data.get("channel_last_seen", {})),
                "feed_age_seconds": now - last_msg if last_msg else None,
            }

    def _set_status(self, status: str, error: str | None = None) -> None:
        with self.lock:
            self.data["connection"] = status
            if error is not None:
                self.data["last_error"] = error[:400]

    @staticmethod
    def _subscribe_payload() -> dict[str, Any]:
        return {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "ticker", "symbols": [SYMBOL]},
                    {"name": "ob_updates", "symbols": [SYMBOL]},
                    {"name": "trades", "symbols": [SYMBOL]},
                    {"name": "mark_price", "symbols": [f"MARK:{SYMBOL}"]},
                    {"name": "candlestick_1m", "symbols": [SYMBOL]},
                    {"name": "funding_rate", "symbols": [SYMBOL]},
                    {"name": "spot_price", "symbols": [INDEX_SYMBOL]},
                    {"name": "system_status"},
                ]
            },
        }

    def _run_forever(self) -> None:
        delay = 2.0
        attempts = 0
        while True:
            attempts += 1
            self._set_status("connecting")
            _logger.info("Attempting WebSocket connection to %s (attempt %d, delay=%.1fs)", WS_URL, attempts, delay)
            try:
                self.ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10, skip_utf8_validation=True)
                _logger.info("WebSocket connection ended (run_forever returned) after %d attempt(s)", attempts)
            except Exception as exc:
                self._set_status("disconnected", str(exc))
                _logger.error("run_forever exception (attempt %d): %s", attempts, exc)
            with self.lock:
                self.data["book_synced"] = False
                self.data["sequence_gap_unresolved"] = True
                self.data["checksum_ok"] = None
                self.data["book_seq"] = None
                self._checksum_logged = False
            self._set_status(f"reconnecting in {delay:.0f}s")
            _logger.info("Reconnecting in %.1fs", delay)
            time.sleep(delay)
            delay = min(delay * 1.6, 15.0)

    def _health_loop(self) -> None:
        while True:
            time.sleep(30)
            try:
                self._log_health()
            except Exception:
                pass

    def _log_health(self) -> None:
        now = time.time()
        with self.lock:
            seen = dict(self.data.get("channel_last_seen", {}))
            conn = self.data.get("connection")
            gaps = self.data.get("sequence_gaps")
            crc_fail = self.data.get("checksum_failures")
            book_synced = self.data.get("book_synced")
        parts = [f"{ch}={now - ts:.1f}s" for ch, ts in seen.items()]
        _logger.info(
            "Feed health: connection=%s book_synced=%s gaps=%s crc_fail=%s per_channel_age=[%s]",
            conn, book_synced, gaps, crc_fail, ", ".join(parts) if parts else "none",
        )

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        self._set_status("connected", "")
        self._first_msg_channels.clear()
        try:
            ws.send(json.dumps({"type": "enable_heartbeat"}))
            payload = self._subscribe_payload()
            ws.send(json.dumps(payload))
            _logger.info("WebSocket connected to %s at %s", WS_URL, datetime.now(timezone.utc).isoformat())
            _logger.info("Outgoing subscription payload: %s", json.dumps(payload))
            with self.lock:
                self.data["book_synced"] = False
                self.data["sequence_gap_unresolved"] = True
                self.data["book_seq"] = None
                self.data["checksum_ok"] = None
                self.data["checksum_msg"] = "Waiting for order-book snapshot"
                self._checksum_logged = False
                self.data["total_reconnects"] = self.data.get("total_reconnects", 0) + 1
            _log_event({"event": "ws_connected", "url": WS_URL, "reconnect_attempt": self.data.get("total_reconnects")})
        except Exception as exc:
            self._set_status("connected, subscribe error", str(exc))
            _logger.error("Failed to send subscriptions on open: %s", exc)
            _log_event({"event": "subscribe_error", "error": str(exc)})

    def _on_error(self, ws: websocket.WebSocketApp, error: Any) -> None:
        self._set_status("socket error", str(error))
        with self.lock:
            self.data["book_synced"] = False
            self.data["sequence_gap_unresolved"] = True
            self.data["checksum_ok"] = None
        _logger.error("WebSocket socket error: %s", error)
        _log_event({"event": "ws_error", "error": str(error)})


    def _on_close(self, ws: websocket.WebSocketApp, code: Any, message: Any) -> None:
        self._set_status("disconnected", f"close={code}; {message or ''}")
        _logger.warning("WebSocket disconnected at %s (code=%s)", datetime.now(timezone.utc).isoformat(), code)
        with self.lock:
            self.data["total_disconnects"] = self.data.get("total_disconnects", 0) + 1
            self.data["book_synced"] = False
            self.data["sequence_gap_unresolved"] = True
            self.data["checksum_ok"] = None
        _log_event({"event": "ws_disconnected", "code": code, "message": str(message or '')})


    def trigger_diagnostic_reconnect(self) -> bool:
        """Explicitly triggered reconnect for testing. Only works if DELTA_ENABLE_DIAGNOSTIC_RECONNECT=1."""
        global _diagnostic_reconnect_triggered
        if os.environ.get("DELTA_ENABLE_DIAGNOSTIC_RECONNECT") != "1":
            _logger.warning("Diagnostic reconnect disabled (set DELTA_ENABLE_DIAGNOSTIC_RECONNECT=1)")
            return False
        if not self.ws or not self.ws.sock or not self.ws.sock.connected:
            _logger.warning("Cannot trigger reconnect: WebSocket not in connected state")
            return False
        
        _diagnostic_reconnect_triggered = True
        _log_event({"event": "diagnostic_reconnect_requested", "reason": "manual_test"})
        _logger.info("Diagnostic reconnect triggered by test mechanism")
        
        # Close the connection cleanly - let the reconnect loop handle reconnection
        try:
            self.ws.close()
            return True
        except Exception as e:
            _logger.error("Failed to close WebSocket for diagnostic reconnect: %s", e)
            return False

    def _send_resubscribe(self, reason: str) -> None:
        """Resubscribe the order book after a sequence/checksum issue, with a throttle."""
        with self.resubscribe_lock:
            now = time.monotonic()
            if now - self.last_resubscribe < 1.5:
                return
            self.last_resubscribe = now
            ws = self.ws
            with self.lock:
                self.data["book_synced"] = False
                self.data["sequence_gap_unresolved"] = True
                self.data["checksum_ok"] = None
                self.data["checksum_msg"] = reason
                self.data["asks"] = {}
                self.data["bids"] = {}
                self.data["book_seq"] = None
            if not ws or not ws.sock or not ws.sock.connected:
                _logger.warning("Resubscribe skipped (socket not connected): %s", reason)
                return
            _logger.warning("Resubscribing ob_updates: %s", reason)
            try:
                ws.send(json.dumps({"type": "unsubscribe", "payload": {"channels": [{"name": "ob_updates", "symbols": [SYMBOL]}]}}))
                time.sleep(0.25)
                ws.send(json.dumps({"type": "subscribe", "payload": {"channels": [{"name": "ob_updates", "symbols": [SYMBOL]}]}}))
            except Exception as exc:
                self._set_status("resubscribe error", str(exc))
                _logger.error("Resubscribe send failed: %s", exc)

    @staticmethod
    def _book_checksum(asks: dict[str, str], bids: dict[str, str]) -> int:
        ask_levels = sorted(asks.items(), key=lambda item: float(item[0]))[:10]
        bid_levels = sorted(bids.items(), key=lambda item: float(item[0]), reverse=True)[:10]
        ask_str = ",".join(f"{price}:{size}" for price, size in ask_levels)
        bid_str = ",".join(f"{price}:{size}" for price, size in bid_levels)
        return zlib.crc32(f"{ask_str}|{bid_str}".encode("utf-8")) & 0xFFFFFFFF

    @staticmethod
    def _apply_levels(book: dict[str, str], levels: Any) -> None:
        if not isinstance(levels, list):
            return
        for level in levels:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            price = str(level[0])
            size = str(level[1])
            size_float = _f(size, 0.0) or 0.0
            if size_float == 0:
                book.pop(price, None)
            else:
                book[price] = size

    def _handle_book(self, msg: dict[str, Any]) -> None:
        action = str(msg.get("action", "update")).lower()
        do_resub = False
        resync_reason = ""
        if action == "error":
            with self.lock:
                self.data["last_error"] = str(msg.get("msg", "Order book feed error"))
            _logger.error("Order-book feed error from exchange: %s", msg.get("msg") or msg)
            self._send_resubscribe("Feed error; reloading book snapshot")
            _log_event({"event": "ob_error", "action": "error", "msg": msg.get("msg", str(msg))})
            return

        with self.lock:
            if action == "snapshot":
                self.data["asks"] = {}
                self.data["bids"] = {}
                self._apply_levels(self.data["asks"], msg.get("a", []))
                self._apply_levels(self.data["bids"], msg.get("b", []))
                try:
                    self.data["book_seq"] = int(msg.get("seq"))
                except (TypeError, ValueError):
                    self.data["book_seq"] = None
                    self.data["book_synced"] = False
                    self.data["sequence_gap_unresolved"] = True
                    do_resub = True
                    resync_reason = "Order-book snapshot has missing/invalid sequence; reloading book"
                else:
                    self.data["book_synced"] = True
                    self.data["sequence_gap_unresolved"] = False
                self.data["ob_snapshots"] += 1
                self.data["book_version"] = self.data.get("book_version", 0) + 1
                _logger.info("Order-book snapshot received: seq=%s cs=%s asks=%d bids=%d",
                             msg.get("seq"), msg.get("cs"), len(self.data["asks"]), len(self.data["bids"]))
            elif action == "update":
                prior_seq = self.data["book_seq"]
                try:
                    incoming_seq = int(msg.get("seq"))
                except (TypeError, ValueError):
                    incoming_seq = None
                if not self.data["book_synced"] or prior_seq is None:
                    resync_reason = "Update received before snapshot; reloading book"
                    self.data["sequence_gap_unresolved"] = True
                    self.data["sequence_gaps"] += 1
                    _logger.warning("ob_updates received before snapshot (seq=%s); resubscribing", msg.get("seq"))
                    _log_event({"event": "ob_sequence_gap", "seq": msg.get("seq"), "reason": "snapshot_missing"})
                    do_resub = True
                elif incoming_seq != prior_seq + 1:
                    resync_reason = f"Sequence gap: expected {prior_seq + 1}, got {incoming_seq}; reloading book"
                    self.data["sequence_gap_unresolved"] = True
                    self.data["sequence_gaps"] += 1
                    _logger.warning("Order-book sequence gap: expected %s got %s; resubscribing", prior_seq + 1, incoming_seq)
                    _log_event({"event": "ob_sequence_gap", "expected": prior_seq + 1, "received": incoming_seq, "seq": msg.get("seq")})
                    do_resub = True
                else:
                    do_resub = False
                    self._apply_levels(self.data["asks"], msg.get("a", []))
                    self._apply_levels(self.data["bids"], msg.get("b", []))
                    self.data["book_seq"] = incoming_seq
            else:
                return

            if do_resub:
                self._send_resubscribe(resync_reason)
            else:
                expected = msg.get("cs")
                calculated = self._book_checksum(self.data["asks"], self.data["bids"])
                try:
                    expected_int = int(expected) if expected is not None else None
                except (TypeError, ValueError, OverflowError):
                    expected_int = None
                if expected is not None and (expected_int is None or calculated != expected_int):
                    self.data["checksum_ok"] = False
                    self.data["checksum_failures"] += 1
                    self.data["book_synced"] = False
                    self.data["sequence_gap_unresolved"] = True
                    self.data["checksum_msg"] = f"CRC mismatch (expected {expected}, calculated {calculated}); reloading book"
                    resync_reason = self.data["checksum_msg"]
                    do_resub = True
                    _logger.error("Order-book CRC32 mismatch: expected=%s calculated=%s; resubscribing", expected, calculated)
                    _log_event({
                        "event": "ob_crc_mismatch",
                        "expected": expected,
                        "calculated": calculated,
                        "seq": msg.get("seq")
                    })
                    self._send_resubscribe(resync_reason)
                else:
                    self.data["checksum_ok"] = True if expected is not None else None
                    self.data["checksum_msg"] = "CRC32 verified" if expected is not None else "Exchange did not include checksum"
                    self.data["sequence_gap_unresolved"] = False
                    self.data["book_updates"] += 1
                    self.data["ob_updates"] += 1
                    self.data["book_synced"] = True
                    self.data["book_version"] = self.data.get("book_version", 0) + 1
                    if not self._checksum_logged:
                        self._checksum_logged = True
                        _logger.info("Order-book CRC32 verified against exchange checksum (cs=%s)", calculated)
                    _log_event({
                        "event": "ob_update",
                        "action": "update" if action == "update" else "snapshot",
                        "seq": msg.get("seq"),
                        "cs": msg.get("cs"),
                        "crc_ok": expected is None or (expected_int is not None and calculated == expected_int),
                        "checksummed": expected is not None
                    })
                    self.data["last_market_message_at"] = time.time()
                    self.data["last_market_type"] = "ob_updates"

    def _increment_message_count(self, channel: str) -> None:
        """Thread-safe increment of per-channel message counter."""
        with self.lock:
            counts = self.data.get("channel_message_counts", {})
            counts[channel] = counts.get(channel, 0) + 1
            self.data["channel_message_counts"] = counts

    # ------------------------------------------------------------------
    # order-flow sampler: resting liquidity + aggressive cluster tracking
    # ------------------------------------------------------------------

    def _sample_loop(self) -> None:
        """Daemon thread: periodic order-book liquidity sampler.

        Records significant resting-liquidity bands (adaptive percentile of
        banded depth) and finalizes large-trade clusters with a measured price
        response. Never raises; one bad sample must not kill the thread.
        """
        while True:
            try:
                interval = float(self.sample_interval) if self.sample_interval else 2.0
                time.sleep(interval)
                self._take_depth_sample()
            except Exception as exc:
                _logger.warning("Depth sampler error: %s", exc)
                time.sleep(2.0)

    def _take_depth_sample(self) -> None:
        now = time.time()
        with self.lock:
            sizes = list(self.data["trade_sizes"])
            self.data["large_trade_threshold"] = orderflow.adaptive_threshold(
                sizes, self.large_percentile, min_samples=50)
            if not self.data["book_synced"] or not self.data["asks"] or not self.data["bids"]:
                asks = bids = {}
            else:
                asks = {float(k): _f(v, 0.0) or 0.0 for k, v in self.data["asks"].items()}
                bids = {float(k): _f(v, 0.0) or 0.0 for k, v in self.data["bids"].items()}
        mid = None
        if asks and bids:
            mid = (min(asks) + max(bids)) / 2.0
            bid_bands = orderflow.relevant_bands(
                orderflow.band_levels(list(bids.items()), BOOK_TICK, self.liq_band_ticks), mid)
            ask_bands = orderflow.relevant_bands(
                orderflow.band_levels(list(asks.items()), BOOK_TICK, self.liq_band_ticks), mid)
            sig_bid, thr = orderflow.significant_bands(bid_bands, self.liq_percentile, min_samples=10)
            sig_ask, _ = orderflow.significant_bands(ask_bands, self.liq_percentile, min_samples=10)
            with self.lock:
                self.data["book_liq_threshold"] = thr
                stats = self.data["level_stats"]
                for side, bands in (("bid", sig_bid), ("ask", sig_ask)):
                    for b in bands[:8]:
                        key = (side, b["price"])
                        st = stats.get(key)
                        if st is None:
                            stats[key] = {"first_seen": now, "last_seen": now,
                                          "max_size": b["size"], "last_size": b["size"], "side": side}
                        else:
                            st["last_seen"] = now
                            st["max_size"] = max(st["max_size"], b["size"])
                            st["last_size"] = b["size"]
                        self.data["depth_samples"].append((now, side, b["price"], b["size"]))
                if len(stats) > 600:
                    for k, _v in sorted(stats.items(), key=lambda kv: kv[1]["last_seen"])[:len(stats) - 600]:
                        stats.pop(k, None)
        self._finalize_clusters(mid)

    def _extend_cluster(self, ts: float, recv_ts: float, price: float, size: float, side: str) -> None:
        """Merge a large trade into the open same-side aggressive cluster.

        Caller must hold self.lock. A cluster closes once no further same-side
        large trade arrives within cluster_window_s; the sampler then finalizes
        it with the measured price response.
        """
        clusters = self.data["open_clusters"]
        cl = clusters.get(side)
        if cl is not None and ts - cl["end_ts"] <= self.cluster_window_s:
            cl["end_ts"] = ts
            cl["recv_end"] = recv_ts
            cl["count"] += 1
            cl["total_size"] += size
            cl["price_min"] = min(cl["price_min"], price)
            cl["price_max"] = max(cl["price_max"], price)
        else:
            clusters[side] = {
                "side": side, "start_ts": ts, "end_ts": ts, "recv_end": recv_ts,
                "count": 1, "total_size": size, "price_start": price,
                "price_min": price, "price_max": price,
            }

    def _finalize_clusters(self, mid: float | None) -> None:
        """Finalize clusters no later than response_window_s after their last
        trade; price response is book mid vs first execution price (approximate;
        the trade tape's last price is not an execution price)."""
        now = time.time()
        horizon = max(self.cluster_window_s, self.response_window_s)
        finalized = []
        with self.lock:
            for side, cl in list(self.data["open_clusters"].items()):
                if now - cl["recv_end"] < horizon:
                    continue
                cl["price_after"] = mid
                cl["response_pct"] = ((mid - cl["price_start"]) / cl["price_start"] * 100.0) if mid else None
                cl["response_note"] = (
                    f"book mid {self.response_window_s:.0f}s+ after cluster end" if mid
                    else "no book mid available"
                )
                finalized.append(dict(cl))
                del self.data["open_clusters"][side]
            if finalized:
                self.data["trade_clusters"].extend(finalized)
        for cl in finalized:  # log outside the feed lock (file I/O)
            _log_event({
                "event": "trade_cluster",
                "side": cl["side"],
                "count": cl["count"],
                "total_size": round(cl["total_size"], 4),
                "price_start": cl["price_start"],
                "start_ts_utc": datetime.fromtimestamp(cl["start_ts"], timezone.utc).isoformat(),
                "price_after": cl["price_after"],
                "response_pct": round(cl["response_pct"], 5) if cl["response_pct"] is not None else None,
            })

    def _on_message(self, ws: websocket.WebSocketApp, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return
        if not isinstance(msg, dict):
            return
        kind = msg.get("type")
        now = time.time()
        with self.lock:
            self.data["last_any_message_at"] = now

        if kind == "subscriptions":
            _logger.info("Subscription acknowledged by exchange")
            _log_event({"event": "subscription_acked", "payload": msg.get("payload", {})})
            return
        if kind in ("heartbeat", "key-auth", "pong"):
            return
        if isinstance(kind, str):
            with self.lock:
                self.data["channel_last_seen"][kind] = now
                self.data["total_messages_received"] = self.data.get("total_messages_received", 0) + 1
                counts = self.data.get("channel_message_counts", {})
                counts[kind] = counts.get(kind, 0) + 1
                self.data["channel_message_counts"] = counts
            if kind not in self._first_msg_channels:
                self._first_msg_channels.add(kind)
                _logger.info("First message received for channel '%s' at %s", kind, datetime.now(timezone.utc).isoformat())
        if kind == "ob_updates":
            self._handle_book(msg)
            return

        with self.lock:
            # Market message time is local receive time, not a claimed network-latency measurement.
            self.data["last_market_message_at"] = now
            self.data["last_market_type"] = str(kind or "unknown")

            if kind == "trades" and msg.get("sy") == SYMBOL:
                price = _f(msg.get("p"))
                size = _f(msg.get("s"), 0.0) or 0.0
                buyer_role = str(msg.get("r", "")).lower()
                if price is not None:
                    # r is buyer_role: taker buyer is buy-aggressor; maker buyer implies sell-aggressor.
                    if buyer_role == "t":
                        side = "BUY TAKER"
                        signed = size
                        self.data["buy_contracts"] += size
                    elif buyer_role == "m":
                        side = "SELL TAKER"
                        signed = -size
                        self.data["sell_contracts"] += size
                    else:
                        side = "UNKNOWN"
                        signed = 0.0
                    self.data["last_price"] = price
                    self.data["trade_count"] += 1
                    self.data["cvd_value"] += signed
                    trade_ts = _ts_seconds(msg.get("t")) or int(now)
                    self.data["trades"].appendleft({
                        "time": datetime.fromtimestamp(trade_ts, timezone.utc).astimezone(TZ_IST).strftime("%H:%M:%S.%f")[:-3],
                        "side": side,
                        "price": price,
                        "size": size,
                        "buyer_role": buyer_role,
                        "timestamp": trade_ts,
                    })
                    self.data["cvd"].append({"time": datetime.fromtimestamp(trade_ts, timezone.utc), "cvd": self.data["cvd_value"]})
                    _log_event({
                        "event": "trade",
                        "symbol": msg.get("sy"),
                        "price": price,
                        "size": size,
                        "side": side,
                        "buyer_role": buyer_role,
                        # Preserve raw exchange timestamps before normalizing the
                        # legacy `ts` field to whole seconds. The logger adds
                        # `ts_utc` separately as local capture/write time.
                        "exchange_trade_ts_us": msg.get("t"),
                        "server_publish_ts_us": msg.get("ts"),
                        "ts": trade_ts
                    })
                    # --- order-flow tracking (upgrade 2026-10-13) ---
                    side_norm = "buy" if side == "BUY TAKER" else "sell" if side == "SELL TAKER" else "unknown"
                    self.data["trade_log"].append((trade_ts, price, size, side_norm))
                    self.data["trade_sizes"].append(size)
                    # Preserve classified side-volume and actual print counts per minute.
                    # The raw 20k-print deque can otherwise roll past a completed 15m
                    # candle before its flow is evaluated. Keep 48 hours of compact
                    # in-memory aggregates; do not relax the exhaustion criteria.
                    if side_norm in ("buy", "sell") and size > 0:
                        minute_bucket = int(trade_ts // 60) * 60
                        latest_minute = self.data.get("trade_flow_latest_minute")
                        if latest_minute is None or minute_bucket > latest_minute:
                            self.data["trade_flow_latest_minute"] = minute_bucket
                            cutoff = minute_bucket - 48 * 60 * 60
                            flow_minutes = self.data["trade_flow_minutes"]
                            for old_minute in list(flow_minutes):
                                if old_minute < cutoff:
                                    del flow_minutes[old_minute]
                            latest_minute = minute_bucket
                        if minute_bucket >= latest_minute - 48 * 60 * 60:
                            flow = self.data["trade_flow_minutes"].setdefault(
                                minute_bucket,
                                {"buy_count": 0, "sell_count": 0,
                                 "buy_contracts": 0.0, "sell_contracts": 0.0},
                            )
                            flow[f"{side_norm}_count"] += 1
                            flow[f"{side_norm}_contracts"] += size
                    if side_norm != "unknown":
                        threshold = self.data.get("large_trade_threshold")
                        if threshold is not None and size >= threshold:
                            self.data["large_trades"].append({
                                "ts": trade_ts, "recv_ts": now, "price": price,
                                "size": size, "side": side_norm, "threshold": threshold,
                                "buyer_role": buyer_role,
                            })
                            self._extend_cluster(trade_ts, now, price, size, side_norm)

            elif kind == "ticker":
                rows = msg.get("d", [])
                if isinstance(rows, list):
                    for row in rows:
                        if not isinstance(row, dict) or row.get("s") != SYMBOL:
                            continue
                        quotes = row.get("q") or []
                        if len(quotes) >= 4:
                            self.data["ask"] = _f(quotes[0])
                            self.data["ask_size"] = _f(quotes[1])
                            self.data["bid"] = _f(quotes[2])
                            self.data["bid_size"] = _f(quotes[3])
                        self.data["mark_price"] = _f(row.get("m"), self.data["mark_price"])
                        oi = row.get("oi") or []
                        if oi:
                            self.data["open_interest_contracts"] = _f(oi[0], self.data["open_interest_contracts"])
                        ohlc = row.get("ohlc") or []
                        self.data["ticker_24h"] = {
                            "open": _f(ohlc[0]) if len(ohlc) > 0 else None,
                            "high": _f(ohlc[1]) if len(ohlc) > 1 else None,
                            "low": _f(ohlc[2]) if len(ohlc) > 2 else None,
                            "close": _f(ohlc[3]) if len(ohlc) > 3 else None,
                            "mark_change_pct": _f(row.get("m24hc")),
                        }
                        _log_event({
                            "event": "channel_message",
                            "channel": "ticker",
                            "price": _f(quotes[0]),
                            "bid": _f(quotes[2]),
                            "ask": _f(quotes[1]),
                            "bid_size": _f(quotes[3]),
                            "ask_size": _f(quotes[1]),
                            "mark_price": _f(row.get("m")),
                        }, to_full_log=True)

            elif kind == "ob_l1" and msg.get("sy") == SYMBOL:
                self.data["ask"] = _f(msg.get("ap"), self.data["ask"])
                self.data["ask_size"] = _f(msg.get("as"), self.data["ask_size"])
                self.data["bid"] = _f(msg.get("bp"), self.data["bid"])
                self.data["bid_size"] = _f(msg.get("bs"), self.data["bid_size"])

            elif kind == "mark_price" and msg.get("sy") == f"MARK:{SYMBOL}":
                self.data["mark_price"] = _f(msg.get("p"), self.data["mark_price"])
                _log_event({
                    "event": "channel_message",
                    "channel": "mark_price",
                    "price": msg.get("p"),
                }, to_full_log=True)

            elif kind == "spot_price" and msg.get("sy") == INDEX_SYMBOL:
                self.data["index_price"] = _f(msg.get("p"), self.data["index_price"])
                _log_event({
                    "event": "channel_message",
                    "channel": "spot_price",
                    "price": msg.get("p"),
                }, to_full_log=True)

            elif kind == "funding_rate" and msg.get("sy") == SYMBOL:
                self.data["funding_rate"] = _f(msg.get("fr"), self.data["funding_rate"])
                self.data["funding_interval_seconds"] = _f(msg.get("fi"), self.data["funding_interval_seconds"])
                self.data["next_funding_at"] = _ts_seconds(msg.get("nfr"))
                _log_event({
                    "event": "channel_message",
                    "channel": "funding_rate",
                    "funding_rate": _f(msg.get("fr")),
                    "funding_interval_seconds": _f(msg.get("fi")),
                }, to_full_log=True)

            elif isinstance(kind, str) and kind.startswith("candlestick_") and msg.get("sy") == SYMBOL:
                ts = _ts_seconds(msg.get("ts"))
                o, h, low, c = (_f(msg.get(k)) for k in ("o", "h", "l", "c"))
                if ts is not None and None not in (o, h, low, c):
                    resolution = kind.removeprefix("candlestick_")
                    seconds = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
                               "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
                               "12h": 43200, "1d": 86400, "1w": 604800}.get(resolution, 60)
                    bucket = (ts // seconds) * seconds
                    self.data["candles"][bucket] = {
                        "time": bucket,
                        "open": float(o), "high": float(h), "low": float(low), "close": float(c),
                        "volume": float(_f(msg.get("v"), 0.0) or 0.0),
                    }
                    if len(self.data["candles"]) > 3000:
                        for old_key in sorted(self.data["candles"])[:-3000]:
                            self.data["candles"].pop(old_key, None)
                    _log_event({
                        "event": "channel_message",
                        "channel": kind,
                        "resolution": resolution,
                        "candle_ts": bucket,
                        "candle_ts_utc": datetime.fromtimestamp(bucket, timezone.utc).isoformat(),
                        "open": float(o), "high": float(h), "low": float(low), "close": float(c),
                        "volume": float(_f(msg.get("v"), 0.0) or 0.0),
                    }, to_full_log=True)

            elif kind == "system_status":
                event = f"{msg.get('status', 'status')}: {msg.get('event', 'update')}"
                self.data["system_status"] = event
                self.data["recent_events"].appendleft({
                    "time": datetime.now(TZ_IST).strftime("%H:%M:%S"),
                    "channel": "system_status",
                    "detail": event,
                })
                _log_event({
                    "event": "channel_message",
                    "channel": "system_status",
                    "status": msg.get("status"),
                    "event_type": msg.get("event"),
                }, to_full_log=True)

            elif isinstance(kind, str) and kind not in ("heartbeat", "subscriptions"):
                # Keep a diagnostic trail for exchange messages that are not explicitly rendered.
                if msg.get("msg") or msg.get("error"):
                    detail = str(msg.get("msg") or msg.get("error"))
                    self.data["last_error"] = detail[:400]
                    _logger.warning("Exchange message on channel '%s': %s", kind, detail)
                    self.data["recent_events"].appendleft({
                        "time": datetime.now(TZ_IST).strftime("%H:%M:%S"),
                        "channel": str(kind),
                        "detail": detail[:180],
                    })


def _fmt(value: Any, digits: int = 2) -> str:
    number = _f(value)
    return "—" if number is None else f"{number:,.{digits}f}"


def _book_rows(snapshot: dict[str, Any], side: str, limit: int = 15) -> pd.DataFrame:
    book = snapshot[side]
    reverse = side == "bids"
    rows = sorted(book.items(), key=lambda pair: float(pair[0]), reverse=reverse)[:limit]
    cumulative = 0.0
    out = []
    for price, size in rows:
        qty = _f(size, 0.0) or 0.0
        cumulative += qty
        out.append({"Price": float(price), "Contracts": qty, "Cumulative": cumulative})
    return pd.DataFrame(out, columns=["Price", "Contracts", "Cumulative"])


def _candles_df(snapshot: dict[str, Any], timeframe: str) -> pd.DataFrame:
    candles = snapshot["candles"]
    if not candles:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(candles).drop_duplicates(subset=["time"], keep="last").sort_values("time")
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
    df = df.set_index("time")
    rule = {"1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min"}[timeframe]
    return df.resample(rule, label="left", closed="left").agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"),
        close=("close", "last"), volume=("volume", "sum")
    ).dropna(subset=["open", "high", "low", "close"]).reset_index().tail(240)


def _ist_str(ts: float | int | None, with_date: bool = False) -> str:
    """Format an epoch timestamp in IST (all terminal times are IST)."""
    if not ts:
        return "—"
    fmt = "%d %b %H:%M:%S" if with_date else "%H:%M:%S"
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(TZ_IST).strftime(fmt)


def _ist_dt(ts: float) -> Any:
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(TZ_IST)


def _epoch_of(dt: Any) -> float:
    """Epoch seconds for a chart x value (datetime/timestamp/number)."""
    try:
        return datetime.fromisoformat(str(dt)).timestamp() if not isinstance(dt, (int, float)) else float(dt)
    except (TypeError, ValueError):
        try:
            return float(getattr(dt, "timestamp")())
        except Exception:
            return 0.0


def _closed_candles(d: dict[str, Any]) -> list[dict[str, Any]]:
    """Completed candles only: the in-progress minute bucket is excluded so no
    order-block or liquidity signal can use a candle that has not closed."""
    current_bucket = int(time.time() // 60) * 60
    return [dict(c) for c in d["candles"] if c["time"] < current_bucket]


_PA_CACHE: dict[str, Any] = {"key": None, "value": None}


def _price_action_analytics(closed: list[dict[str, Any]], lookback: int, displacement: float,
                            zone_basis: str, eq_tol: float) -> dict[str, Any]:
    """Order blocks, swings, equal levels, session range and confirmed sweeps.

    Cached by the closed-candle signature plus parameters so the dashboard's
    0.5s reruns never recompute detection on unchanged history.
    """
    key = (len(closed), closed[-1]["time"] if closed else 0, lookback, displacement, zone_basis, eq_tol)
    if _PA_CACHE["key"] == key:
        return _PA_CACHE["value"]
    window = closed[-max(60, lookback * 2):] if closed else []
    swings = [s for s in orderflow.swing_points(window, 5, 5) if s["index"] + 5 < len(window)]
    zones = orderflow.detect_order_blocks(
        window, lookback=lookback, swing_left=5, swing_right=5,
        atr_period=14, displacement=displacement,
        zone="body" if zone_basis == "Candle body" else "range", max_zones=10)
    orderflow.update_zone_statuses(zones, window)
    eq = orderflow.equal_levels(swings, tolerance=eq_tol)
    # Session high/low use all currently loaded completed candles, not just the shorter order-block lookback window.
    session = orderflow.session_levels(closed, now_epoch=time.time())
    pools = ([{"kind": s["kind"], "price": s["price"], "time": s["time"]} for s in swings[-12:]]
             + [{"kind": g["kind"], "price": g["price"], "time": g["last_time"]} for g in eq[-8:]])
    if "session_high" in session:
        pools.append({"kind": "high", "price": session["session_high"], "time": session["session_high_time"]})
        pools.append({"kind": "low", "price": session["session_low"], "time": session["session_low_time"]})
    swept = orderflow.detect_sweeps(window, pools[-20:], reclaim_bars=3)
    value = {"swings": swings, "zones": zones, "eq": eq, "session": session, "swept": swept}
    _PA_CACHE["key"], _PA_CACHE["value"] = key, value
    return value


_LIQ_CACHE: dict[str, Any] = {"key": None, "value": None}


def _book_liquidity(d: dict[str, Any], liq_pct: float, band_ticks: int) -> dict[str, Any]:
    """Banded + significance-filtered view of the live synchronized book.

    Only bands material to the traded price are considered (see
    orderflow.relevant_bands): the exchange lists levels far from the market
    whose summed sizes would otherwise dominate the adaptive threshold.
    Cached by (book_seq, book_version, params): the book changes several times
    per second but the dashboard reruns every 0.5s, so identical states are
    served from cache.
    """
    asks = {float(k): _f(v, 0.0) or 0.0 for k, v in d["asks"].items()}
    bids = {float(k): _f(v, 0.0) or 0.0 for k, v in d["bids"].items()}
    ref = None
    if asks and bids:
        ref = (min(asks) + max(bids)) / 2.0
    if ref is None:
        ref = d.get("last_price") or None
    key = (d.get("book_seq"), d.get("book_version"), liq_pct, band_ticks,
           round(ref, 1) if ref else None)
    if _LIQ_CACHE["key"] == key:
        return _LIQ_CACHE["value"]
    bid_bands = orderflow.relevant_bands(
        orderflow.band_levels(list(bids.items()), BOOK_TICK, band_ticks), ref)
    ask_bands = orderflow.relevant_bands(
        orderflow.band_levels(list(asks.items()), BOOK_TICK, band_ticks), ref)
    sig_bid, thr = orderflow.significant_bands(bid_bands, liq_pct, min_samples=10)
    sig_ask, _ = orderflow.significant_bands(ask_bands, liq_pct, min_samples=10)
    value = {"sig_bid": sig_bid, "sig_ask": sig_ask, "threshold": thr,
             "bid_bands": bid_bands, "ask_bands": ask_bands, "ref_price": ref}
    _LIQ_CACHE["key"], _LIQ_CACHE["value"] = key, value
    return value


def _liquidity_level_rows(d: dict[str, Any], liq: dict[str, Any], now: float) -> list[dict[str, Any]]:
    """Report measured live book bands with transparent size and notional metrics.

    "Intensity" is quantity relative to the current adaptive significance
    threshold; it is not a liquidation probability or a forecast of execution.
    Removed rows retain only their last-seen size and are marked non-live.
    """
    rows = []
    threshold = _f(liq.get("threshold") or d.get("book_liq_threshold"), 0.0) or 0.0
    for side, bands in (("bid", liq["sig_bid"]), ("ask", liq["sig_ask"])):
        for b in bands[:10]:
            st = d["level_stats"].get((side, b["price"]))
            dur = max(0.0, now - st["first_seen"]) if st else 0.0
            size = float(b["size"])
            price = float(b["price"])
            rows.append({
                "Side": "BID" if side == "bid" else "ASK",
                "Price": price,
                "Contracts": size,
                "BTC equiv": size * CONTRACT_VALUE_BTC,
                "Approx USD notional": size * CONTRACT_VALUE_BTC * price,
                "Intensity x threshold": (size / threshold) if threshold > 0 else None,
                "Levels": b["levels"],
                "Observed": f"{dur:.0f}s",
                "Status": "active",
            })
    current_keys = {(side, b["price"])
                    for side, bands in (("bid", liq["sig_bid"]), ("ask", liq["sig_ask"])) for b in bands}
    removed = []
    for (side, price), st in d["level_stats"].items():
        if (side, price) in current_keys or now - st["last_seen"] < 6:
            continue
        last_size = float(st["last_size"])
        removed.append({
            "Side": "BID" if side == "bid" else "ASK",
            "Price": float(price),
            "Contracts": last_size,
            "BTC equiv": last_size * CONTRACT_VALUE_BTC,
            "Approx USD notional": last_size * CONTRACT_VALUE_BTC * float(price),
            "Intensity x threshold": None,
            "Levels": None,
            "Observed": f"{max(0.0, st['last_seen'] - st['first_seen']):.0f}s",
            "Status": f"removed (last seen {_ist_str(st['last_seen'])})",
            "_last": st["last_seen"],
        })
    removed.sort(key=lambda r: r["_last"], reverse=True)
    for r in removed[:20]:
        r.pop("_last", None)
        rows.append(r)
    rows.sort(key=lambda r: (r["Side"], -r["Price"] if r["Side"] == "BID" else r["Price"]))
    return rows
def _large_trade_rows(d: dict[str, Any], limit: int = 20) -> list[dict[str, Any]]:
    rows = []
    for t in list(d["large_trades"])[-limit:][::-1]:
        rows.append({
            "Time (IST)": _ist_str(t["ts"]),
            "Side": "BUY" if t["side"] == "buy" else "SELL",
            "Price": t["price"],
            "Contracts": t["size"],
            "BTC equiv": t["size"] * CONTRACT_VALUE_BTC,
            "× threshold": (t["size"] / t["threshold"]) if t.get("threshold") else None,
        })
    return rows


def _cluster_rows(d: dict[str, Any], liq: dict[str, Any] | None, limit: int = 12) -> list[dict[str, Any]]:
    band = BOOK_TICK * LIQ_BAND_DISTANCE
    rows = []
    for c in list(d["trade_clusters"])[-limit:][::-1]:
        near = False
        if liq:
            near = any(c["price_min"] - band <= b["price"] <= c["price_max"] + band
                       for b in liq["sig_bid"] + liq["sig_ask"])
        resp = c.get("response_pct")
        rows.append({
            "Start (IST)": _ist_str(c["start_ts"]),
            "Side": "BUY" if c["side"] == "buy" else "SELL",
            "Trades": c["count"],
            "Contracts": c["total_size"],
            "Price": c["price_start"],
            "Δ response": "—" if resp is None else f"{resp:+.3f}%",
            "Near resting book": "yes" if near else "no",
        })
    return rows


_HEAT_CACHE: dict[str, Any] = {"sig": None, "fig": None}


def _depth_heatmap(d: dict[str, Any]) -> Any:
    """Heatmap from samples this app actually captured (no reconstruction).

    The figure is cached by (sample count, newest sample, second) because
    plotly's validation of the z-matrix is expensive relative to the panel's
    refresh interval.
    """
    samples = list(d["depth_samples"])[-600:]
    if len(samples) < 12:
        return None
    cols = sorted({round(s[0], 1) for s in samples})
    if len(cols) < 3:
        return None
    sig = (len(samples), cols[-1], round(time.time()))
    if _HEAT_CACHE["sig"] == sig and _HEAT_CACHE["fig"] is not None:
        return _HEAT_CACHE["fig"]
    if len(cols) > 150:                       # downsample columns for render cost
        step = len(cols) // 150
        cols = cols[::step]
    prices = [s[2] for s in samples]
    lo, hi = min(prices), max(prices)
    if hi <= lo:
        return None
    n_rows = 24
    col_idx = {t: i for i, t in enumerate(cols)}

    def row_of(p: float) -> int:
        return min(n_rows - 1, max(0, int((p - lo) / (hi - lo) * n_rows)))

    z = [[0.0] * len(cols) for _ in range(n_rows)]
    for t, _side, price, size in samples:
        c = col_idx.get(round(t, 1))
        if c is not None:
            z[row_of(price)][c] = max(z[row_of(price)][c], size)
    y_labels = [f"{lo + (hi - lo) * i / n_rows:.1f}" for i in range(n_rows)][::-1]
    fig = go.Figure(data=go.Heatmap(
        z=list(reversed(z)), x=[_ist_str(t) for t in cols], y=y_labels,
        colorscale=[[0.0, "rgba(0,0,0,0)"], [0.3, "#1b5e20"], [1.0, "#81c784"]],
        showscale=True, colorbar=dict(title="contracts", thickness=10)))
    fig.update_layout(height=240, margin=dict(l=8, r=8, t=15, b=8), template="plotly_dark",
                      xaxis_title="Sample time (IST)", yaxis_title="Price")
    _HEAT_CACHE["sig"], _HEAT_CACHE["fig"] = sig, fig
    return fig


_ZONE_LOG_STATE: dict[tuple, str] = {}


def _log_zone_events(pa: dict[str, Any]) -> None:
    """Append order-block detection/status transitions to the JSONL evidence
    log exactly once per transition (bounded state)."""
    for z in pa["zones"]:
        key = (z["direction"], z["formation_time"], z["top"], z["bottom"])
        prev = _ZONE_LOG_STATE.get(key)
        if prev is None:
            _log_event({"event": "order_block_detected", "direction": z["direction"],
                        "top": z["top"], "bottom": z["bottom"],
                        "formation_ts_utc": datetime.fromtimestamp(z["formation_time"], timezone.utc).isoformat(),
                        "confirmed_ts_utc": datetime.fromtimestamp(z["confirmed_time"], timezone.utc).isoformat(),
                        "displacement_x_atr": round(z["displacement_x_atr"], 3)})
            _ZONE_LOG_STATE[key] = z["status"]
        elif prev != z["status"]:
            _log_event({"event": "order_block_status", "direction": z["direction"],
                        "top": z["top"], "bottom": z["bottom"], "status": z["status"],
                        "status_ts_utc": datetime.fromtimestamp(z["status_time"], timezone.utc).isoformat()
                        if z["status_time"] else None})
            _ZONE_LOG_STATE[key] = z["status"]
    if len(_ZONE_LOG_STATE) > 200:
        for k in list(_ZONE_LOG_STATE)[: len(_ZONE_LOG_STATE) - 200]:
            _ZONE_LOG_STATE.pop(k, None)


def _add_orderflow_overlays(fig: go.Figure, d: dict[str, Any], pa: dict[str, Any],
                            liq: dict[str, Any] | None, toggles: dict[str, bool],
                            now: float, y_range: tuple[float, float] | None = None,
                            x_span: tuple[Any, Any] | None = None) -> None:
    """Draw order-block zones, observed liquidity levels, price-action pools
    and large-trade markers onto the existing candlestick figure.

    Every element must use a real finite price as its Y coordinate; anything
    else is skipped. Labels are sparse (price-proximity deduplicated) and the
    detail lives in hover tooltips so overlapping levels do not obscure the
    price action. When a price-axis range is supplied it is never modified
    here; elements far outside it are not drawn.

    PERFORMANCE: all shapes and annotations are collected and applied in a
    single update_layout call. Calling add_hline/add_shape/add_annotation once
    per element makes plotly re-validate and deep-copy the whole existing
    shape list each time (O(n^2)); with ~50 elements that costs tens of
    seconds per figure and made the dashboard fragment exceed its interval.
    """
    lo_y, hi_y = y_range if y_range else (None, None)
    span_y = (hi_y - lo_y) if (lo_y is not None and hi_y is not None) else 0.0
    min_sep = (span_y * 0.045) if span_y else 0.0   # min label separation (~3% of range)
    annotated: list[float] = []
    shapes: list[dict[str, Any]] = []
    annotations: list[dict[str, Any]] = []

    def _drawable(price: Any) -> bool:
        if not orderflow.is_price(price):
            return False
        if y_range is None:
            return True
        return lo_y - 0.25 * span_y <= float(price) <= hi_y + 0.25 * span_y

    def _label_ok(price: float) -> bool:
        """Allow a text label only when no nearby label was already placed."""
        if min_sep <= 0:
            return True
        for p in annotated:
            if abs(p - price) < min_sep:
                return False
        annotated.append(price)
        return True

    def _line(price: float, color: str, width: float, dash: str, opacity: float) -> dict[str, Any]:
        return {"type": "line", "xref": "paper", "x0": 0, "x1": 1,
                "yref": "y", "y0": price, "y1": price, "layer": "below",
                "line": {"color": color, "width": width, "dash": dash}, "opacity": opacity}

    def _label(x: Any, y: float, text: str, color: str, *, side: str = "left",
               anchor: str = "bottom", size: int = 9, xref: str = "paper") -> None:
        annotations.append({
            "xref": xref, "x": x, "xanchor": side, "yref": "y", "y": y, "yanchor": anchor,
            "text": text, "showarrow": False, "font": {"size": size, "color": color},
            "opacity": 0.9,
        })

    x0, x1 = (x_span if x_span else (None, None))

    # --- price-action order blocks (derived from completed candles) ---
    confirmed = sorted(pa["zones"], key=lambda z: z["confirmed_time"], reverse=True)
    labelled = 0
    for z in confirmed:
        if z["direction"] == "bullish" and not toggles.get("bull_ob"):
            continue
        if z["direction"] == "bearish" and not toggles.get("bear_ob"):
            continue
        if not (_drawable(z["top"]) and _drawable(z["bottom"])):
            continue
        base = "#26a69a" if z["direction"] == "bullish" else "#ef5350"
        if z["status"] == "invalidated":
            base, fill_op, dash = "#9e9e9e", 0.04, "dot"
        elif z["status"] == "mitigated":
            fill_op, dash = 0.07, "dash"
        else:
            fill_op, dash = 0.14, "solid"
        # right edge is the newest candle (data-driven) so an unchanged
        # market produces an identical figure and no browser redraw
        zone_end = max(z["confirmed_time"], _epoch_of(x1)) if x1 is not None else z["confirmed_time"]
        shapes.append({
            "type": "rect", "xref": "x", "x0": _ist_dt(z["formation_time"]),
            "x1": _ist_dt(zone_end), "yref": "y",
            "y0": z["bottom"], "y1": z["top"], "layer": "below",
            "fillcolor": base, "opacity": fill_op,
            "line": {"color": base, "width": 1, "dash": dash}})
        # label only the most recent active/mitigated zones; invalidated stay unlabelled
        if labelled < 3 and z["status"] != "invalidated":
            mid = (z["top"] + z["bottom"]) / 2.0
            if _label_ok(mid):
                labelled += 1
                _label(_ist_dt(z["formation_time"]), mid,
                       f"{'bullish' if z['direction'] == 'bullish' else 'bearish'} OB "
                       f"{z['bottom']:,.1f}-{z['top']:,.1f} ({z['status']})",
                       base, side="left", anchor="middle", xref="x")

    # --- observed resting liquidity (live synchronized book only) ---
    if toggles.get("book_liq") and liq and d["book_synced"]:
        for side, bands in (("bid", liq["sig_bid"]), ("ask", liq["sig_ask"])):
            color = "#26a69a" if side == "bid" else "#ef5350"
            usable = [b for b in bands if _drawable(b["price"])]
            # strong levels stay prominent; weaker nearby levels are faded and
            # capped so the current-price region stays readable
            for strength_group, group, width, opacity in (
                    ("strong", usable[:3], 3.0, 0.85),
                    ("", usable[3:7], 1.2, 0.35)):
                if not group:
                    continue
                xs, ys, texts = [], [], []
                for b in group:
                    st = d["level_stats"].get((side, b["price"]))
                    dur = f"{max(0.0, now - st['first_seen']):.0f}s" if st else "n/a"
                    tip = (f"{'BID' if side == 'bid' else 'ASK'} liquidity {b['price']:,.1f} · "
                           f"{b['size']:,.0f} contracts (~{b['size'] * CONTRACT_VALUE_BTC:,.3f} BTC) · "
                           f"band {b['low']:,.1f}-{b['high']:,.1f} ({b['levels']} levels) · "
                           f"observed {dur} · resting size, can be cancelled/moved")
                    if x0 is not None:
                        xs += [x0, x1, None]
                        ys += [b["price"], b["price"], None]
                        texts += [tip, tip, ""]
                if xs:
                    fig.add_trace(go.Scatter(
                        x=xs, y=ys, mode="lines", line=dict(
                            color=color,
                            width=width,
                            dash="solid" if strength_group == "strong" else "dash"),
                        opacity=opacity,
                        text=texts, hovertemplate="%{text}<extra></extra>",
                        name=f"{'Bid' if side == 'bid' else 'Ask'} liquidity"
                             + (" (strong)" if strength_group else ""),
                        showlegend=True))
            for b in usable[:1]:                    # sparse labels: strongest level only
                if _label_ok(b["price"]):
                    _label(0.995 if side == "bid" else 0.005, b["price"],
                           f"{b['price']:,.1f} · {b['size']:,.0f}c", color,
                           side="right" if side == "bid" else "left", anchor="bottom")
        # removed levels: faint dotted line, detail only in hover
        current_keys = {(side, b["price"])
                        for side, bands in (("bid", liq["sig_bid"]), ("ask", liq["sig_ask"])) for b in bands}
        shown = 0
        for (side, price), st in d["level_stats"].items():
            if shown >= 4:
                break
            if (side, price) in current_keys or now - st["last_seen"] < 6:
                continue
            if not _drawable(price):
                continue
            shown += 1
            color = "#26a69a" if side == "bid" else "#ef5350"
            shapes.append(_line(price, color, 1, "dot", 0.10))
            if x0 is not None:
                tip = (f"{'BID' if side == 'bid' else 'ASK'} level {price:,.1f} REMOVED "
                       f"(last seen {_ist_str(st['last_seen'])} IST, was {st['last_size']:,.0f} contracts)")
                xs = [x0, x1, None]
                ys = [price, price, None]
                texts = [tip, tip, ""]
                fig.add_trace(go.Scatter(
                    x=xs, y=ys, mode="lines", line=dict(color=color, width=1, dash="dot"),
                    opacity=0.18, text=texts, hovertemplate="%{text}<extra></extra>",
                    name=f"{'Bid' if side == 'bid' else 'Ask'} removed", showlegend=False))

    # --- price-action pools (inferred; not resting orders) ---
    if toggles.get("pa_pools"):
        sess = pa["session"]
        for lvl, label, color in (("session_high", "session high", "#42a5f5"),
                                  ("session_low", "session low", "#42a5f5")):
            if lvl in sess and _drawable(sess[lvl]):
                shapes.append(_line(sess[lvl], color, 1.4, "dash", 0.6))
                if _label_ok(sess[lvl]):
                    _label(0.995, sess[lvl], f"{label} {sess[lvl]:,.1f}", color,
                           side="right", anchor="top")
        for g in sorted(pa["eq"], key=lambda g: g["count"], reverse=True)[:3]:
            if not _drawable(g["price"]):
                continue
            color = "#ffa726"
            shapes.append(_line(g["price"], color, 1.2, "dashdot", 0.55))
            if _label_ok(g["price"]):
                _label(0.005, g["price"],
                       f"{'EQH' if g['kind'] == 'high' else 'EQL'} {g['price']:,.1f} ×{g['count']}",
                       color, side="left", anchor="top", size=8)
        for s in pa["swings"][-2:]:
            if not _drawable(s["price"]):
                continue
            color = "#7e57c2"
            shapes.append(_line(s["price"], color, 1, "dot", 0.4))
            if _label_ok(s["price"]):
                _label(0.995, s["price"],
                       f"swing {'H' if s['kind'] == 'high' else 'L'} {s['price']:,.1f}",
                       color, side="right", anchor="top", size=8)
        for s in pa["swept"][-2:]:
            if not _drawable(s["price"]):
                continue
            color = "#ab47bc"
            shapes.append(_line(s["price"], color, 1.4, "solid", 0.65))
            if _label_ok(s["price"]):
                _label(0.005, s["price"],
                       f"SWEPT {'H' if s['kind'] == 'high' else 'L'} {s['price']:,.1f}",
                       color, side="left", anchor="bottom", size=8)
        # a few unlabelled pools stay faintly visible; everything beyond these
        # caps stays in the order-flow panel tables rather than on the chart
        for g in sorted(pa["eq"], key=lambda g: g["count"], reverse=True)[3:6]:
            if _drawable(g["price"]):
                shapes.append(_line(g["price"], "#ffa726", 1, "dashdot", 0.3))
        for s in pa["swings"][:-2][-3:]:
            if _drawable(s["price"]):
                shapes.append(_line(s["price"], "#7e57c2", 1, "dot", 0.25))
        for s in pa["swept"][:-2][-2:]:
            if _drawable(s["price"]):
                shapes.append(_line(s["price"], "#ab47bc", 1.2, "solid", 0.4))

    # --- current price marker (keeps the traded price readable; the value is
    # also shown in the metrics row, so no stacked label is drawn) ---
    last_price = d.get("last_price")
    if last_price is not None and _drawable(last_price):
        shapes.append(_line(float(last_price), "#e0e0e0", 1, "longdash", 0.5))

    # --- large executed trades (actual tape, aggressor from buyer_role) ---
    if toggles.get("large_trades"):
        recent = list(d["large_trades"])[-60:]
        for side, symbol, color, name in (("buy", "triangle-up", "#26a69a", "Large buys"),
                                          ("sell", "triangle-down", "#ef5350", "Large sells")):
            pts = [t for t in recent if t["side"] == side and _drawable(t["price"])]
            if not pts:
                continue
            sizes = []
            for t in pts:
                thr = t.get("threshold") or 1.0
                sizes.append(7.0 + min(13.0, max(0.0, (t["size"] / thr - 1.0)) * 6.0 + 3.0))
            fig.add_trace(go.Scatter(
                x=[_ist_dt(t["ts"]) for t in pts], y=[t["price"] for t in pts],
                mode="markers", name=name,
                marker=dict(symbol=symbol, color=color, size=sizes,
                            line=dict(width=0.8, color="white"), opacity=0.95),
                text=[f"{_ist_str(t['ts'])} IST · {'BUY' if side == 'buy' else 'SELL'} x {t['size']:,.0f} contracts "
                      f"(~{t['size'] * CONTRACT_VALUE_BTC:,.3f} BTC) @ {t['price']:,.1f} · "
                      f"threshold {t['threshold']:,.1f}c · role {t['buyer_role']}"
                      for t in pts],
                hovertemplate="%{text}<extra></extra>", showlegend=True))

    # --- potential buy/sell exhaustion (confirmed candle + tape flow) ---
    if toggles.get("exhaust"):
        exhaustion = pa.get("exhaustion", [])
        for kind, name, color, symbol, textposition, label in (
            ("buy_exhaust", "Buy exhaustion (bearish risk)", "#ef5350", "triangle-down", "top center", "BUY EXHAUST"),
            ("sell_exhaust", "Sell exhaustion (bullish risk)", "#26a69a", "triangle-up", "bottom center", "SELL EXHAUST"),
        ):
            points = [x for x in exhaustion if x.get("kind") == kind and _drawable(x.get("price"))]
            if not points:
                continue
            customdata = [
                [
                    x["buy_contracts"], x["sell_contracts"], x["trade_count"],
                    x["wick_ratio"] * 100.0, x["close_position"] * 100.0,
                    x.get("aggressor_ratio"),
                ]
                for x in points
            ]
            hover = (
                f"{label} · potential exhaustion, not an entry signal<br>"
                "Time: %{x}<br>Price: %{y:,.1f}<br>"
                "Buy-aggressor contracts: %{customdata[0]:,.1f}<br>"
                "Sell-aggressor contracts: %{customdata[1]:,.1f}<br>"
                "Classified trade prints in candle: %{customdata[2]}<br>"
                "Rejection wick: %{customdata[3]:.1f}% of range<br>"
                "Close position: %{customdata[4]:.1f}% of range"
                "<extra></extra>"
            )
            fig.add_trace(go.Scatter(
                x=[_ist_dt(x["time"]) for x in points],
                y=[x["price"] for x in points],
                mode="markers+text",
                name=name,
                text=[label] * len(points),
                textposition=textposition,
                textfont=dict(size=12, color=color),
                marker=dict(
                    symbol=symbol, color=color, size=16,
                    line=dict(width=0.8, color="white"), opacity=0.95,
                ),
                customdata=customdata,
                hovertemplate=hover,
                showlegend=True,
            ))

    # apply every shape/annotation in ONE validated pass (see docstring)
    if shapes:
        fig.update_layout(shapes=shapes)
    if annotations:
        fig.update_layout(annotations=annotations)


_FIG_CACHE: dict[str, Any] = {
    "sig": None, "param_sig": None, "fig": None, "y_range": None,
    "last_build": 0.0, "exhaustion_status": {},
}
_FIG_FLOOR_S = 30.0      # refresh the cached figure at least this often even if data is unchanged
_FIG_REBUILD_S = 3.0     # max chart rebuild rate: a full Plotly re-render more often than
                         # this makes the chart element visibly blink/blank in the browser


def _price_figure_signature(d: dict[str, Any], timeframe: str, height: int,
                            toggles: dict[str, bool], pa: dict[str, Any] | None,
                            liq: dict[str, Any] | None, n_candles: int,
                            last_candle: Any, last_candle_ohlc: tuple) -> tuple[tuple, tuple]:
    """Data-driven signature for the candlestick figure, split into
    (params, data). Parameter changes (timeframe, height, overlay toggles)
    always rebuild immediately; data changes are throttled separately.

    The data signature deliberately excludes wall-clock time: an unchanged
    dataset must yield a byte-identical figure so the browser does not redraw
    (and flash) a chart whose content has not changed. A coarse _FIG_FLOOR_S
    bucket still applies so hover-only details (durations) refresh periodically.
    """
    param_sig = (
        timeframe, height,
        bool(toggles.get("bull_ob")), bool(toggles.get("bear_ob")),
        bool(toggles.get("book_liq")), bool(toggles.get("pa_pools")),
        bool(toggles.get("large_trades")), bool(toggles.get("exhaust")),
    )
    trade_log = d.get("trade_log", [])
    trade_flow_digest = (len(trade_log), tuple(trade_log[-1]) if trade_log else ())
    zones_digest = ((len(pa["zones"]), pa["zones"][-1]["confirmed_time"]) if pa and pa["zones"] else (0, 0))
    pools_digest = ((len(pa["swings"]), len(pa["eq"]), len(pa["swept"])) if pa else (0, 0, 0))
    bands_digest = ((tuple((b["price"], round(b["size"], 1)) for b in liq["sig_bid"][:10]),
                     tuple((b["price"], round(b["size"], 1)) for b in liq["sig_ask"][:10]))
                    if liq else ((), ()))
    removed_digest = (len(d["level_stats"]),
                      max((v["last_seen"] for v in d["level_stats"].values()), default=0.0))
    data_sig = (
        n_candles, str(last_candle), last_candle_ohlc,
        zones_digest, pools_digest, bands_digest, removed_digest,
        len(d["large_trades"]), (d["large_trades"][-1]["ts"] if d["large_trades"] else 0),
        trade_flow_digest,
        d.get("book_synced"),
        int(time.time() // _FIG_FLOOR_S),
    )
    return param_sig, data_sig


def _build_price_figure(d: dict[str, Any], timeframe: str, pa: dict[str, Any] | None,
                        liq: dict[str, Any] | None, toggles: dict[str, bool],
                        height: int, now: float) -> tuple[Any, tuple[float, float] | None, pd.DataFrame]:
    """Build the BTCUSD candlestick figure with order-flow overlays.
    
    Returns (fig, y_range, candles_df); fig is None when there are no candles to draw.
    The figure stretches to its container width (use_container_width by the
    caller) and is `height` px tall. Built figures are cached by a DATA-driven
    signature, so a rerun with unchanged market data returns the identical
    figure (identical serialized spec) instead of forcing a browser redraw.
    """
    candles = _candles_df(d, timeframe)
    if candles.empty:
        return None, None, candles
    last_candle = candles["time"].iloc[-1]
    last_row = candles.iloc[-1]
    last_candle_ohlc = tuple(round(float(last_row[k]), 2)
                             for k in ("open", "high", "low", "close"))
    param_sig, data_sig = _price_figure_signature(d, timeframe, height, toggles, pa, liq,
                                                  len(candles), last_candle, last_candle_ohlc)
    now_ts = time.time()
    if _FIG_CACHE["sig"] == (param_sig, data_sig) and _FIG_CACHE["fig"] is not None:
        return _FIG_CACHE["fig"], _FIG_CACHE["y_range"], candles
    # Throttle rebuilds for DATA changes only: the live candle changes several
    # times a second and a full Plotly re-render at that rate blinks/blank the
    # chart in the browser. Parameter changes (toggle, timeframe, height) are
    # never throttled - the user expects immediate feedback.
    if (_FIG_CACHE["fig"] is not None
            and _FIG_CACHE.get("param_sig") == param_sig
            and now_ts - _FIG_CACHE["last_build"] < _FIG_REBUILD_S):
        return _FIG_CACHE["fig"], _FIG_CACHE["y_range"], candles
    fig = go.Figure(data=[go.Candlestick(
        x=candles["time"], open=candles["open"], high=candles["high"],
        low=candles["low"], close=candles["close"], name="BTCUSD"
    )])
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=20, b=8),
        xaxis_rangeslider_visible=False,
        xaxis_title="Time (IST)", yaxis_title="Contract price",
        template="plotly_dark", showlegend=False,
        dragmode="zoom",
        # Stable per chart context (timeframe/layout) but NOT per data version:
        # Plotly preserves the user's zoom/pan across figure updates while
        # uirevision is unchanged, and resets the view when it changes (e.g.
        # after a timeframe switch). Without this, every rebuild re-applied
        # the candle-derived range and wiped the user's zoom.
        uirevision=f"btcusd-{timeframe}-{height}",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(130,130,130,.15)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(130,130,130,.15)")
    # Price axis from valid candle lows/highs only. An explicit range
    # guarantees that no stray coordinate (auxiliary metric, missing value
    # placeholder, far-from-market book level) can force the axis toward zero
    # and compress the candles.
    y_range = orderflow.price_axis_range(
        candles["low"], candles["high"],
        extras=[p for p in (
            [z["top"] for z in pa["zones"]] + [z["bottom"] for z in pa["zones"]] +
            [b["price"] for b in (liq["sig_bid"] + liq["sig_ask"])] if liq else []
        ) if orderflow.is_price(p)])
    if y_range:
        fig.update_yaxes(range=[y_range[0], y_range[1]], autorange=False)
    x_span = (candles["time"].iloc[0], last_candle)

    # Exhaustion labels are computed from completed chart-timeframe candles only.
    # The current in-progress bucket is never eligible for a label.
    pa_overlay = dict(pa or {"zones": [], "swings": [], "eq": [], "session": {}, "swept": []})
    pa_overlay["exhaustion"] = []
    if toggles.get("exhaust"):
        tf_seconds = {"1m": 60, "3m": 180, "5m": 300, "15m": 900}[timeframe]
        current_closed_epoch = int(now // tf_seconds) * tf_seconds
        completed_chart_candles = []
        for row in candles.to_dict("records"):
            candle_epoch = int(pd.Timestamp(row["time"]).timestamp())
            if candle_epoch + tf_seconds > current_closed_epoch:
                continue
            completed_chart_candles.append({
                "time": candle_epoch,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            })
        # Feed the detector compact side-specific minute totals with true print counts.
        # This keeps 15m/5m labels viable even after raw events roll out of trade_log.
        aggregate_flow_rows = []
        flow_minutes = d.get("trade_flow_minutes", {})
        for minute_start, flow in flow_minutes.items():
            flow_ts = int(minute_start) + 1  # place the aggregate safely inside its minute
            if flow.get("buy_count", 0) > 0 and flow.get("buy_contracts", 0.0) > 0:
                aggregate_flow_rows.append({
                    "ts": flow_ts, "size": flow["buy_contracts"],
                    "side": "buy", "trade_count": flow["buy_count"],
                })
            if flow.get("sell_count", 0) > 0 and flow.get("sell_contracts", 0.0) > 0:
                aggregate_flow_rows.append({
                    "ts": flow_ts, "size": flow["sell_contracts"],
                    "side": "sell", "trade_count": flow["sell_count"],
                })
        exhaustion_diagnostics = {}
        pa_overlay["exhaustion"] = orderflow.detect_exhaustion_labels(
            completed_chart_candles,
            aggregate_flow_rows,
            timeframe_seconds=tf_seconds,
            lookback=10,
            min_trades=5,
            imbalance_ratio=1.25,
            min_wick_ratio=0.35,
            close_position_limit=0.65,
            diagnostics=exhaustion_diagnostics,
        )[-8:]

    flow_minutes = d.get("trade_flow_minutes", {})
    flow_keys = [int(k) for k in flow_minutes]
    flow_span_minutes = ((max(flow_keys) - min(flow_keys)) // 60 + 1) if flow_keys else 0
    _FIG_CACHE["exhaustion_status"] = {
        "count": len(pa_overlay.get("exhaustion", [])) if toggles.get("exhaust") else 0,
        "minute_buckets": len(flow_minutes),
        "flow_span_minutes": flow_span_minutes,
        "timeframe": timeframe,
        "criteria": exhaustion_diagnostics if toggles.get("exhaust") else {},
    }

    _add_orderflow_overlays(fig, d, pa_overlay, liq, toggles, now, y_range, x_span)
    fig.update_layout(
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1,
                    font=dict(size=9), bgcolor="rgba(0,0,0,0)"))
    _FIG_CACHE["sig"], _FIG_CACHE["fig"], _FIG_CACHE["y_range"] = (param_sig, data_sig), fig, y_range
    _FIG_CACHE["param_sig"] = param_sig
    _FIG_CACHE["last_build"] = now_ts
    return fig, y_range, candles


def _render_order_book(d: dict[str, Any], depth_levels: int, compact: bool = False) -> None:
    """Render the live order book: sequence/CRC state, bid/ask levels with
    cumulative depth, spread and depth imbalance. Used by both chart layouts."""
    if not d["book_synced"]:
        st.warning("Order book is not currently sequence-synced. Do not rely on the displayed depth until a fresh snapshot passes checks.")
    crc_text = d["checksum_msg"]
    if d["checksum_ok"] is True:
        st.caption(f"Sequence: {d['book_seq']} · {crc_text} · updates: {d['book_updates']:,}")
    elif d["checksum_ok"] is False:
        st.error(f"Book integrity check failed · {crc_text}")
    else:
        st.caption(f"Sequence: {d['book_seq'] or '—'} · {crc_text}")

    ask_df = _book_rows(d, "asks", depth_levels)
    bid_df = _book_rows(d, "bids", depth_levels)
    table_h = 300 if compact else 330
    book_a, book_b = st.columns(2)
    with book_a:
        st.markdown("**ASKS**")
        if not ask_df.empty:
            st.dataframe(ask_df.style.format({"Price": "{:,.2f}", "Contracts": "{:,.0f}", "Cumulative": "{:,.0f}"}), hide_index=True, use_container_width=True, height=table_h)
        else:
            st.caption("Awaiting snapshot")
    with book_b:
        st.markdown("**BIDS**")
        if not bid_df.empty:
            st.dataframe(bid_df.style.format({"Price": "{:,.2f}", "Contracts": "{:,.0f}", "Cumulative": "{:,.0f}"}), hide_index=True, use_container_width=True, height=table_h)
        else:
            st.caption("Awaiting snapshot")
    if d["book_synced"] and not ask_df.empty and not bid_df.empty:
        ask_depth = float(ask_df["Contracts"].sum())
        bid_depth = float(bid_df["Contracts"].sum())
        total = ask_depth + bid_depth
        imbalance = ((bid_depth - ask_depth) / total * 100) if total else 0.0
        b1, b2, b3 = st.columns(3)
        b1.metric(f"Bid depth ({depth_levels})", _fmt(bid_depth, 0))
        b2.metric(f"Ask depth ({depth_levels})", _fmt(ask_depth, 0))
        b3.metric("Depth imbalance", f"{imbalance:+.1f}%")
    st.caption("Sizes are contracts as supplied by Delta, not BTC quantity. Order-book liquidity can be cancelled and is not a guarantee of execution.")


@st.cache_resource(show_spinner=False)
def get_feed() -> DeltaFeed:
    feed = DeltaFeed(load_initial_candles())
    feed.start()
    return feed


feed = get_feed()

with st.sidebar:
    st.header("Terminal settings")
    timeframe = st.selectbox("Chart timeframe", ["1m", "3m", "5m", "15m"], index=0)
    depth_levels = st.slider("Order-book levels per side", min_value=5, max_value=15, value=10)
    st.caption("Market-data only. No trading or account API keys are used.")
    st.markdown("---")
    st.subheader("Order-flow overlays")
    st.caption("Price-action signals derive from completed candles only. Book signals use the live synchronized order book. Neither implies institutional activity.")
    show_bull_ob = st.checkbox("Bullish order blocks", value=True)
    show_bear_ob = st.checkbox("Bearish order blocks", value=True)
    show_book_liq = st.checkbox("Order-book liquidity levels", value=True)
    show_pa_pools = st.checkbox("Swing / equal-high-low pools", value=True)
    show_large_trades = st.checkbox("Large executed trades", value=True)
    show_exhaustion_labels = st.checkbox(
        "Buy / sell exhaustion labels",
        value=True,
        help="Marks potential exhaustion only when a completed candle rejects a new local extreme and same-candle aggressor flow confirms the pressure. Context labels, not entry signals.",
    )
    show_flow_panel = st.checkbox("Order-flow & liquidity panel", value=True)
    show_depth_heat = st.checkbox("Depth heatmap (captured only)", value=False)
    ob_lookback = st.slider("Order-block lookback (candles)", 30, 400, 150, 10)
    ob_displacement = st.slider("Displacement threshold (× ATR)", 0.3, 3.0, 1.0, 0.1)
    ob_zone_basis = st.radio("Order-block zone basis", ["Candle high/low range", "Candle body"], horizontal=True)
    liq_percentile = st.slider("Book liquidity percentile", 80, 99, 95)
    eq_tolerance = st.slider("Equal H/L tolerance (points)", 1.0, 100.0, 10.0, 1.0)
    trade_percentile = st.slider("Large-trade percentile", 90, 99, 95)
    cvd_window_min = st.slider("Rolling CVD window (min)", 5, 240, 30, 5)
    st.markdown("---")
    st.markdown("### 💥 Chart View Controls")
    st.caption("Your zoom/pan is remembered across live updates. Use Reset to return to candle-derived range.")
    if st.button("💥 Reset Chart View", use_container_width=True, type="secondary",
                 help="Return the BTCUSD chart to the current candle-derived price range "
                      "(clears the remembered zoom/pan and URL params)."):
        for _k in ("zx", "zy"):
            if _k in st.query_params:
                del st.query_params[_k]
    st.markdown("---")
    chart_layout = st.radio(
        "Chart layout",
        ["Large chart (book below)", "Chart + compact book"],
        index=0,
        help="Large chart: full-width price chart with the order book in a collapsible panel below. "
             "Chart + compact book: side-by-side layout with a compact order book.")
    st.markdown("---")
    st.markdown("**Feed**")
    st.code(WS_URL, language="text")
    st.markdown("**Contract**")
    st.code("BTCUSD · Perpetual · INR-settled", language="text")
    
    # Diagnostic reconnect test (only when explicitly enabled via environment variable)
    if os.environ.get("DELTA_ENABLE_DIAGNOSTIC_RECONNECT") == "1":
        st.markdown("---")
        st.markdown("<b style='color:#ff6b6b'>DIAGNOSTIC MODE</b>", unsafe_allow_html=True)
        if st.button("Trigger reconnect test", help="Manually test reconnect recovery"):
            success = feed.trigger_diagnostic_reconnect()
            if success:
                st.toast("Reconnect test triggered", icon="🔄")
            else:
                st.toast("Reconnect test failed or already in progress", icon="⚠️")
        
        # File-based trigger for non-UI operation
        trigger_file = "/tmp/delta_reconnect_trigger"
        if os.path.exists(trigger_file) and not os.path.exists("/tmp/delta_reconnect_triggerED"):
            import os.path
            success = feed.trigger_diagnostic_reconnect()
            # Mark as triggered
            with open("/tmp/delta_reconnect_TRIGGERED", "w") as f:
                json.dump({"triggered_at": datetime.now(timezone.utc).isoformat(), "success": success}, f)
            os.rename(trigger_file, trigger_file + ".executed")

st.title("Delta India · BTC Perpetual Flow Terminal")
st.caption("Direct public WebSocket · BTCUSD · read-only market-data workspace · all times IST")

# propagate overlay settings to the persistent feed (leaf writes; thread-safe)
feed.liq_percentile = float(liq_percentile)
feed.large_percentile = float(trade_percentile)

def _overlay_toggles() -> dict[str, bool]:
    return {"bull_ob": show_bull_ob, "bear_ob": show_bear_ob, "book_liq": show_book_liq,
            "pa_pools": show_pa_pools, "large_trades": show_large_trades,
            "exhaust": show_exhaustion_labels}


@st.fragment(run_every=1.0)
def render_status_panel() -> None:
    """Connection banner and headline price metrics (fast-updating)."""
    d = feed.snapshot()
    now = time.time()
    market_age = None if d["last_market_message_at"] is None else max(0.0, now - d["last_market_message_at"])
    conn = d.get("connection", "unknown")

    if conn == "connected":
        if market_age is not None and market_age <= 6:
            st.success(f"WebSocket connected · market message age {market_age:.2f}s · live data received by this client")
        elif market_age is not None:
            st.warning(f"WebSocket connected, but market feed is not fresh (last market message {market_age:.1f}s ago). Verify exchange/feed activity.")
        else:
            st.info("WebSocket connected · waiting for first market update")
    elif conn in ("connecting", "reconnecting"):
        st.warning(f"WebSocket status: {conn.replace('_', ' ').title()}")
    elif conn.startswith("socket error") or conn.startswith("disconnected"):
        st.error(f"WebSocket status: {conn.split(';')[0].strip()}")
    else:
        st.warning(f"WebSocket status: {conn}")

    last_price = d["last_price"]
    mark = d["mark_price"]
    index = d["index_price"]
    bid = d["bid"]
    ask = d["ask"]
    asks_sorted = sorted(d["asks"].items(), key=lambda item: float(item[0]))
    bids_sorted = sorted(d["bids"].items(), key=lambda item: float(item[0]), reverse=True)
    best_ask = _f(asks_sorted[0][0]) if asks_sorted and d["book_synced"] else ask
    best_bid = _f(bids_sorted[0][0]) if bids_sorted and d["book_synced"] else bid
    best_ask_size = _f(asks_sorted[0][1]) if asks_sorted and d["book_synced"] else d["ask_size"]
    best_bid_size = _f(bids_sorted[0][1]) if bids_sorted and d["book_synced"] else d["bid_size"]
    spread = (best_ask - best_bid) if best_ask is not None and best_bid is not None else None
    spread_bps = (spread / ((best_ask + best_bid) / 2) * 10000) if spread is not None and best_ask is not None and best_bid is not None and (best_ask + best_bid) else None

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Last trade", _fmt(last_price, 2))
    m2.metric("Mark price", _fmt(mark, 2))
    m3.metric("Delta index", _fmt(index, 2))
    m4.metric("Best bid", _fmt(best_bid, 2), help=f"Top bid size: {_fmt(best_bid_size, 0)} contracts")
    m5.metric("Best ask", _fmt(best_ask, 2), help=f"Top ask size: {_fmt(best_ask_size, 0)} contracts")
    m6.metric("Spread", f"{_fmt(spread, 2)} / {_fmt(spread_bps, 2)} bps")


_CHART_STATE: dict[str, Any] = {"emitted_sig": None, "height": 700}


_ZOOM_BRIDGE_HTML = """
<script>
(() => {
  const HOOK = '__zoom_bridge_hooked__';
  const parent = window.parent;
  if (!parent || parent === window) return;
  const doc = parent.document;
  function findChart() {
    return Array.from(doc.querySelectorAll('.js-plotly-plot'))
      .find(g => g.data && g.data[0] && g.data[0].type === 'candlestick' && !g[HOOK]);
  }
  let timer = null;
  function write(g, clear) {
    try {
      const p = new URLSearchParams(parent.location.search);
      if (clear) {
        p.delete('zx'); p.delete('zy');
      } else {
        const xl = g.layout.xaxis, yl = g.layout.yaxis;
        const put = (k, a, b) => {
          if (a != null && b != null) { p.set(k, a + ',' + b); } else { p.delete(k); }
        };
        const xOK = xl.range && !xl.autorange;
        const yOK = yl.range && !yl.autorange;
        put('zx', xOK ? Math.round(new Date(xl.range[0]).getTime() / 1000) : null,
                  xOK ? Math.round(new Date(xl.range[1]).getTime() / 1000) : null);
        put('zy', yOK ? +yl.range[0].toFixed(2) : null,
                  yOK ? +yl.range[1].toFixed(2) : null);
      }
      const q = p.toString();
      parent.history.replaceState(null, '', parent.location.pathname + (q ? '?' + q : ''));
    } catch (e) { /* cross-origin guards: bridge simply stays inactive */ }
  }
  function hook(g) {
    if (!g) return;
    g[HOOK] = true;
    g.on('plotly_relayout', (e) => {
      // Modebar "Reset axes"/"Autoscale" (and double-click reset) emit a
      // relayout containing showspikes instead of plain range changes; treat
      // those as an explicit reset of the remembered view.
      const isReset = (e['xaxis.showspikes'] !== undefined) || (e['yaxis.showspikes'] !== undefined);
      const isAuto = (e['xaxis.autorange'] === true) || (e['yaxis.autorange'] === true)
                  || (e['xaxis.range[0]'] === null) || (e['yaxis.range[0]'] === null);
      if (timer) { clearTimeout(timer); }
      timer = setTimeout(() => {
        write(g, isReset || isAuto);
        if (isReset) {
          // the figure spec carries the remembered zoom, so plotly's own reset
          // would restore it - force autorange to return to the candle range
          try { parent.Plotly.relayout(g, {'xaxis.autorange': true, 'yaxis.autorange': true}); } catch (e2) {}
        }
      }, 150);
    });
  }
  const obs = new MutationObserver(() => hook(findChart()));
  obs.observe(doc.body, {childList: true, subtree: true});
  setInterval(() => hook(findChart()), 700);
  hook(findChart());
})();
</script>
"""


def _remembered_zoom() -> dict[str, tuple[float, float]] | None:
    """The chart view the user zoomed/panned to, mirrored into the URL by the
    zoom-bridge component.

    Why this exists: Streamlit re-creates the Plotly DOM node whenever the
    element is re-emitted (every fragment or full rerun), so Plotly's own
    uirevision state is lost on each live refresh. The bridge records the
    user's axis ranges into the URL query string, and every full run re-applies
    them to the freshly emitted figure - keeping zoom/pan persistent across
    live updates while retaining automatic price scaling when untouched.
    Returns {'x': (t0, t1), 'y': (lo, hi)} with either key absent.

    A timeframe switch also DROPS the saved view: the recorded ranges belong to
    the previous timeframe's candles, so they are removed from the URL. Without
    that, the very rerun after the switch (when session_state already matches)
    would restore them - and a range saved on another resolution can show an
    empty plot, because candle buckets change with the timeframe.

    A first run in a new session does NOT delete anything: it adopts the current
    timeframe and leaves any saved view in the URL to be validated (and, if
    stale, cleared) by _apply_remembered_zoom on the next run - so reloading a
    link with a still-valid saved view restores that view.
    """
    try:
        if "chart_timeframe" not in st.session_state:
            st.session_state["chart_timeframe"] = timeframe   # first run: adopt
            return None
        if st.session_state["chart_timeframe"] != timeframe:
            st.session_state["chart_timeframe"] = timeframe   # timeframe switch => fresh view
            for _k in ("zx", "zy"):
                if _k in st.query_params:
                    del st.query_params[_k]
            return None
        qp = st.query_params
        zoom: dict[str, tuple[float, float]] = {}
        if "zx" in qp:
            t0, t1 = [float(v) for v in str(qp["zx"]).split(",")]
            if 0 < t0 < t1 < 4e9:
                zoom["x"] = (t0, t1)
        if "zy" in qp:
            lo, hi = [float(v) for v in str(qp["zy"]).split(",")]
            if 0 < lo < hi < 1e7:
                zoom["y"] = (lo, hi)
        return zoom or None
    except Exception:
        return None


_X_ZOOM_TOL_S = 30.0     # one-minute candle-bucket granularity


def _candle_covers_zoom(zoom: dict[str, tuple[float, float]],
                        candles_df: pd.DataFrame | None) -> dict[str, bool]:
    """Does the remembered view still contain at least one real candle?

    A saved panned/zoomed view is only useful while some candle lies inside its
    time window AND that candle's high/low range intersects the saved price
    window. Both axes are tested independently (a dead time window is fatal for
    the whole view; a dead price window only invalidates the y axis), so the
    result reports per-axis validity plus their combination.

    Returns dict with keys "x", "y", "both" (values True where that axis the
    stored window still overlaps at least one candle).
    """
    result = {"x": True, "y": True, "both": True}
    if not zoom:
        return result
    if candles_df is None or candles_df.empty:
        # No candle data to validate against: cannot confirm the view is usable.
        return {"x": False, "y": False, "both": False}
    xs = [t.timestamp() for t in candles_df["time"]]
    # orderflow.is_price rejects None/NaN/inf/non-positive values, so a broken
    # coordinate can never be treated as covering the remembered view.
    pairs = [(t, float(lo), float(hi)) for t, lo, hi in zip(xs, candles_df["low"], candles_df["high"])
             if orderflow.is_price(lo) and orderflow.is_price(hi)]
    if not pairs:
        return {"x": False, "y": False, "both": False}
    x_win = zoom.get("x")
    y_win = zoom.get("y")
    if x_win is not None:
        x0, x1 = x_win
        result["x"] = any(x0 - _X_ZOOM_TOL_S <= t <= x1 + _X_ZOOM_TOL_S for t, _lo, _hi in pairs)
    if y_win is not None:
        y0, y1 = y_win
        result["y"] = any(lo <= y1 and hi >= y0 for _t, lo, hi in pairs)
    if x_win is not None and y_win is not None:
        (x0, x1), (y0, y1) = x_win, y_win
        result["both"] = any(
            (x0 - _X_ZOOM_TOL_S <= t <= x1 + _X_ZOOM_TOL_S) and (lo <= y1 and hi >= y0)
            for t, lo, hi in pairs)
    elif x_win is not None:
        result["both"] = result["x"]
    elif y_win is not None:
        result["both"] = result["y"]
    return result


def _apply_remembered_zoom(fig: Any, candles_df: pd.DataFrame | None = None) -> bool | None:
    """Override the candle-derived axis ranges with the user's remembered view.

    Returns:
        True  - the stored view is valid and was applied (zoom preserved)
        False - part of the stored view was stale and was cleared from the URL
        None  - no zoom was stored (timeframe change or fresh page load)

    A saved time window with no candle in it is invalid for the whole view, so
    both zx and zy are dropped. A saved price window that no candle in the
    stored time window reaches only invalidates zy: the remembered time pan is
    kept and the price axis is re-derived from the candles that ARE visible, so
    an intentional horizontal pan never loses the user's time position.
    """
    zoom = _remembered_zoom()
    if zoom is None:
        return None

    cover = _candle_covers_zoom(zoom, candles_df)
    if candles_df is None or candles_df.empty:
        return None

    x_win = zoom.get("x")
    y_win = zoom.get("y")
    cleared = False

    if x_win is not None and not cover["x"]:
        # Dead time window (e.g. saved before the current candle history): the
        # whole remembered view is unusable.
        for _k in ("zx", "zy"):
            if _k in st.query_params:
                del st.query_params[_k]
        return False

    if y_win is not None and not (cover["both"] if x_win is not None else cover["y"]):
        # Price window shows no candle in the visible time window: drop zy and
        # re-derive the price axis from the candles inside the stored x window.
        if "zy" in st.query_params:
            del st.query_params["zy"]
        cleared = True
        y_win = None
        if x_win is not None:
            x0, x1 = x_win
            visible = candles_df[
                (candles_df["time"].map(lambda t: x0 - _X_ZOOM_TOL_S <= t.timestamp() <= x1 + _X_ZOOM_TOL_S))
            ]
            if not visible.empty:
                lo = float(visible["low"].min())
                hi = float(visible["high"].max())
                pad = max((hi - lo) * 0.06, 0.5)
                fig.update_yaxes(range=[lo - pad, hi + pad], autorange=False)

    if x_win is not None and cover["x"]:
        fig.update_xaxes(range=[_ist_dt(x_win[0]), _ist_dt(x_win[1])], autorange=False)
    if y_win is not None:
        fig.update_yaxes(range=list(y_win), autorange=False)
    return False if cleared else True


def _price_chart_inputs(d: dict[str, Any], height: int) -> tuple[Any, dict[str, Any] | None, dict[str, Any] | None, pd.DataFrame]:
    """Shared analytics inputs for the price chart (all cached)."""
    now = time.time()
    pa = None
    closed = _closed_candles(d)
    if closed:
        pa = _price_action_analytics(closed, ob_lookback, ob_displacement,
                                     ob_zone_basis, eq_tolerance)
        _log_zone_events(pa)
    liq = None
    if d["book_synced"] and d["asks"] and d["bids"]:
        liq = _book_liquidity(d, float(liq_percentile), 2)
    fig, y_range, candles_df = _build_price_figure(d, timeframe, pa, liq, _overlay_toggles(), height, now)
    return fig, pa, liq, candles_df


def _emit_price_chart(height: int) -> None:
    """Emit the BTCUSD candlestick chart at MODULE level (static shell).

    Why module level: a Streamlit fragment rerun REPLACES the fragment's DOM
    subtree, which remounts the Plotly element and destroys the user's zoom.
    A module-level element with a stable key keeps the same DOM node across
    full-script reruns, so Plotly.react runs on the same graph div and the
    layout's uirevision preserves the user's zoom/pan across live updates.
    The driver fragment below triggers a full rerun only when the cached
    figure actually changed.
    """
    _CHART_STATE["height"] = height
    d = feed.snapshot()
    st.subheader(f"BTCUSD {timeframe} candles")
    fig, _pa, _liq, candles_df = _price_chart_inputs(d, height)
    if fig is None:
        st.info("Waiting for historical candles or live candle updates. If this persists, check your connection and Delta API status.")
    else:
        zoom_status = _apply_remembered_zoom(fig, candles_df)
        if zoom_status is False:
            # Keep the notice on screen for a while: the dashboard reruns about
            # once a second, so an on-shot message would flash and disappear.
            st.session_state["chart_view_reset_at"] = time.time()
        elif zoom_status is True and "chart_view_reset_at" in st.session_state:
            # a new valid view is being applied: the stale-view notice is history
            del st.session_state["chart_view_reset_at"]
        reset_at = st.session_state.get("chart_view_reset_at")
        if reset_at and (time.time() - float(reset_at)) < 120:
            st.info("💥 Chart view was reset: the saved zoom/pan no longer contained any candle, "
                    "so it was cleared. The current candle range is shown; any new zoom/pan you "
                    "make is remembered from here.")
        st.plotly_chart(fig, use_container_width=True, key="btcusd_price_chart",
                        config={"displayModeBar": True, "displaylogo": False,
                                "scrollZoom": True, "responsive": True})
        if show_exhaustion_labels:
            exhaustion_status = _FIG_CACHE.get("exhaustion_status", {})
            detected = int(exhaustion_status.get("count", 0))
            bucket_count = int(exhaustion_status.get("minute_buckets", 0))
            flow_span = int(exhaustion_status.get("flow_span_minutes", 0))
            if detected:
                st.caption(
                    f"Exhaustion detector: {detected} label(s) found in the current chart/tape window. "
                    "Hover a marker for the candle's aggressor volume and rejection details; labels are context warnings, not entries."
                )
            else:
                diag = exhaustion_status.get("criteria", {})
                st.caption(
                    f"Exhaustion detector: 0 labels currently qualify on the {timeframe} chart. "
                    "A label requires a completed candle that breaks the prior 10-bar extreme, a rejection wick "
                    "of at least 35%, at least 5 classified trade prints, and at least 1.25:1 same-side aggressor imbalance. "
                    f"The in-memory flow summary currently has {bucket_count} minute bucket(s) spanning about {flow_span} minute(s). "
                    f"Capture-window candidates: {int(diag.get('candidate_candles_in_flow_span', 0))}; "
                    f"usable-flow candles: {int(diag.get('usable_flow_candles', 0))}; "
                    f"gap-blocked windows: {int(diag.get('gap_windows', 0))}; "
                    f"no-flow buckets: {int(diag.get('missing_flow_buckets', 0))}; "
                    f"below-minimum prints: {int(diag.get('below_min_trade_count', 0))}. "
                    f"BUY path (higher high → wick → close → aggressor imbalance): "
                    f"{int(diag.get('buy_new_high', 0))} → {int(diag.get('buy_wick_pass', 0))} → "
                    f"{int(diag.get('buy_close_pass', 0))} → {int(diag.get('buy_flow_pass', 0))}. "
                    f"SELL path (lower low → wick → close → aggressor imbalance): "
                    f"{int(diag.get('sell_new_low', 0))} → {int(diag.get('sell_wick_pass', 0))} → "
                    f"{int(diag.get('sell_close_pass', 0))} → {int(diag.get('sell_flow_pass', 0))}. "
                    "No rules are loosened to force a label."
                )
        _CHART_STATE["emitted_sig"] = _FIG_CACHE["sig"]
    basis = (d["last_price"] - d["index_price"]) if d["last_price"] is not None and d["index_price"] is not None else None
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Open interest", _fmt(d["open_interest_contracts"], 0))
    t2.metric("Funding rate (API × 100)", "—" if d["funding_rate"] is None else f"{d['funding_rate'] * 100:.5f}%")
    t3.metric("Last − index", _fmt(basis, 2))
    t4.metric("Session trade count", f"{d['trade_count']:,}")
    st.caption("Index is Delta's underlying BTCUSD index. Basis and funding are monitoring metrics, not standalone trading signals. "
               "Hover any chart element for details; hover a liquidity line for its observed quantity and band. "
               "Box-zoom (drag), scroll-zoom, pan (modebar), autoscale and Reset axes all apply; your zoom/pan is "
               "remembered across live updates - Reset axes (or double-click) returns to the current price range.")


@st.fragment(run_every=1.0)
def chart_driver() -> None:
    """Keep the cached price figure fresh and refresh the static chart shell
    only when its content changed (bounded by the 3s rebuild throttle).

    Calling st.rerun() here re-emits the module-level chart on the SAME DOM
    node, so Plotly.react updates the figure in place and uirevision keeps
    the user's zoom. Emitting from inside a fragment instead would remount
    the chart and drop the zoom on every refresh.
    """
    d = feed.snapshot()
    _price_chart_inputs(d, _CHART_STATE["height"])
    if _FIG_CACHE["sig"] != _CHART_STATE["emitted_sig"]:
        st.rerun()


@st.fragment(run_every=1.0)
def render_book_panel(compact: bool = False) -> None:
    """Live order book: sequence/CRC state, levels, depth, imbalance."""
    d = feed.snapshot()
    _render_order_book(d, depth_levels, compact=compact)


@st.fragment(run_every=2.0)
def render_tape_cvd_panel() -> None:
    """Executed-trade tape and session CVD proxy."""
    d = feed.snapshot()
    tape_col, cvd_col = st.columns([1, 1], gap="large")
    with tape_col:
        st.subheader("Recent executed trades")
        trades = d["trades"]
        if trades:
            tape_df = pd.DataFrame(trades)
            tape_df = tape_df[["time", "side", "price", "size"]]
            st.dataframe(
                tape_df.style.format({"price": "{:,.2f}", "size": "{:,.0f}"}),
                hide_index=True, use_container_width=True, height=360,
            )
        else:
            st.info("Waiting for BTCUSD trades from the public trade feed.")
        st.caption("Tape side is inferred from Delta's buyer_role: taker-buyer = buy-aggressor; maker-buyer = sell-aggressor.")

    with cvd_col:
        st.subheader("Session CVD proxy")
        cvd = d["cvd"]
        if cvd:
            cvd_df = pd.DataFrame(cvd)
            cvd_df["time"] = pd.to_datetime(cvd_df["time"], utc=True).dt.tz_convert("Asia/Kolkata")
            fig_cvd = go.Figure()
            fig_cvd.add_trace(go.Scatter(x=cvd_df["time"], y=cvd_df["cvd"], mode="lines", name="CVD proxy"))
            fig_cvd.update_layout(height=300, margin=dict(l=8, r=8, t=15, b=8), template="plotly_dark", xaxis_title="Time (IST)", yaxis_title="Cumulative signed contracts", showlegend=False)
            fig_cvd.update_xaxes(showgrid=True, gridcolor="rgba(130,130,130,.15)")
            fig_cvd.update_yaxes(showgrid=True, gridcolor="rgba(130,130,130,.15)")
            st.plotly_chart(fig_cvd, use_container_width=True, key="btcusd_cvd_chart",
                            config={"displaylogo": False})
        else:
            st.info("CVD starts collecting when this app receives trades; it is not historical CVD.")
        c1, c2, c3 = st.columns(3)
        c1.metric("Buy-aggressor contracts", _fmt(d["buy_contracts"], 0))
        c2.metric("Sell-aggressor contracts", _fmt(d["sell_contracts"], 0))
        c3.metric("Net session delta", _fmt(d["cvd_value"], 0))
        st.caption("CVD is a client-side proxy collected since this dashboard started. It resets on restart and should not be treated as a verified exchange-calculated metric.")


@st.fragment(run_every=2.0)
def render_flow_panel() -> None:
    """Order-flow and liquidity tables: observed levels, large trades,
    aggressive clusters, derived pools and the captured depth heatmap."""
    d = feed.snapshot()
    now = time.time()
    book_integrity_ok = (
        bool(d.get("book_synced"))
        and not bool(d.get("sequence_gap_unresolved", True))
        and d.get("checksum_ok") is True
    )
    book_ready = book_integrity_ok and bool(d["asks"]) and bool(d["bids"])
    pa = None
    closed = _closed_candles(d)
    if closed:
        pa = _price_action_analytics(closed, ob_lookback, ob_displacement,
                                     ob_zone_basis, eq_tolerance)
    liq = _book_liquidity(d, float(liq_percentile), 2) if book_ready else None

    with st.expander("Authentic market-wide liquidation levels", expanded=True):
        st.warning(
            "UNAVAILABLE — this dashboard subscribes to Delta's public market-data channels, "
            "which do not publish market-wide liquidation executions or liquidation-price clusters. "
            "The documented liquidation reason appears in private/account order notifications; that "
            "does not establish an all-market liquidation feed. No synthetic liquidation levels or "
            "intensity scores are displayed."
        )
        st.caption(
            "A position's own liquidation price is account/margin-specific and is not a market-wide "
            "liquidation map. To add authentic levels later, a verified market-wide data source and "
            "instrument-specific schema are required."
        )

    flow_l, flow_r = st.columns([1, 1], gap="large")
    with flow_l:
        st.markdown("**Observed resting liquidity (live book)**")
        if not book_ready:
            st.warning(
                "Observed order-book levels are withheld unless the book is sequence-synced, "
                "CRC/checksum-valid, and has no unresolved sequence gap. Price-action-derived "
                "zones and sweeps may still be shown separately."
            )
        else:
            lvl_rows = _liquidity_level_rows(d, liq, now)
            if lvl_rows:
                lvl_df = pd.DataFrame(lvl_rows)
                st.dataframe(
                    lvl_df.style.format({
                        "Price": "{:,.1f}",
                        "Contracts": "{:,.0f}",
                        "BTC equiv": "{:,.3f}",
                        "Approx USD notional": lambda value: "—" if pd.isna(value) else "$" + f"{value:,.0f}",
                        "Intensity x threshold": "{:.2f}x",
                    }, na_rep="—"),
                    hide_index=True, use_container_width=True, height=300,
                )
            else:
                st.caption("No level currently exceeds the adaptive significance threshold.")
            thr = d.get("book_liq_threshold")
            st.caption(
                f"Adaptive threshold: {'— (not enough bands yet)' if thr is None else f'{thr:,.0f} contracts (p{int(liq_percentile)} of banded depth)'} · "
                f"bands aggregate within ±{BOOK_TICK * 2:.1f} price points · durations measured since first observed. "
                "Intensity = contracts ÷ adaptive threshold; approximate USD notional = contracts × 0.001 BTC × price. "
                "These are observed resting orders, not proof that the orders will remain or execute."
            )
    with flow_r:
        st.markdown("**Large executed trades (actual tape)**")
        trade_thr = d.get("large_trade_threshold")
        if trade_thr is None:
            st.caption(f"Collecting trade-size samples (≥50 needed) before applying the p{int(trade_percentile)} large-trade threshold.")
        else:
            st.caption(f"Threshold: ≥ {trade_thr:,.1f} contracts (p{int(trade_percentile)} of observed trade sizes, rolling window)")
            trade_rows = _large_trade_rows(d, limit=20)
            if trade_rows:
                st.dataframe(pd.DataFrame(trade_rows).style.format(
                    {"Price": "{:,.1f}", "Contracts": "{:,.0f}", "BTC equiv": "{:,.3f}", "× threshold": "{:.2f}"}),
                    hide_index=True, use_container_width=True, height=230)
            else:
                st.caption("No trade has exceeded the threshold yet.")
        metrics = orderflow.rolling_aggressor_metrics(
            [{"ts": t[0], "side": t[3], "size": t[2]} for t in d["trade_log"]],
            cvd_window_min * 60.0)
        k1, k2, k3 = st.columns(3)
        k1.metric(f"Buy vol ({cvd_window_min}m)", _fmt(metrics["buy_contracts"], 0))
        k2.metric(f"Sell vol ({cvd_window_min}m)", _fmt(metrics["sell_contracts"], 0))
        k3.metric(f"Rolling CVD ({cvd_window_min}m)", _fmt(metrics["cvd_contracts"], 0))
        cl_rows = _cluster_rows(d, liq)
        if cl_rows:
            st.markdown("**Aggressive clusters**")
            st.dataframe(pd.DataFrame(cl_rows).style.format(
                {"Contracts": "{:,.0f}", "Price": "{:,.1f}"}),
                hide_index=True, use_container_width=True, height=170)
            st.caption("Δ response: book mid vs first execution price of the cluster, measured after the response window; a price move after a trade does not prove the trade caused it.")
    with st.expander("Price-action liquidity pools (derived)", expanded=False):
        sess = pa["session"] if pa else {}
        if sess:
            s1, s2 = st.columns(2)
            s1.metric("Session high", f"{sess.get('session_high', 0):,.1f}",
                      help=f"at {_ist_str(sess.get('session_high_time'))} IST (UTC day)")
            s2.metric("Session low", f"{sess.get('session_low', 0):,.1f}",
                      help=f"at {_ist_str(sess.get('session_low_time'))} IST (UTC day)")
        pool1, pool2 = st.columns(2)
        with pool1:
            st.markdown("**Equal highs / lows**")
            eq_rows = [{"Type": "EQH" if g["kind"] == "high" else "EQL", "Price": g["price"],
                        "Touches": g["count"], "Last touch (IST)": _ist_str(g["last_time"])}
                       for g in (pa["eq"][-12:] if pa else [])]
            if eq_rows:
                st.dataframe(pd.DataFrame(eq_rows).style.format({"Price": "{:,.1f}"}),
                             hide_index=True, use_container_width=True, height=200)
            else:
                st.caption("No equal levels confirmed in the current lookback.")
        with pool2:
            st.markdown("**Confirmed sweeps**")
            sweep_rows = [{
                "Candidate": (
                    "Above-high sweep (buy-side liquidity)"
                    if s["kind"] == "high"
                    else "Below-low sweep (sell-side liquidity)"
                ),
                "Level": s["price"],
                "Breach extreme": s.get("breach_price"),
                "Overshoot (pts)": s.get("overshoot_points"),
                "Reclaim delay (bars)": s.get("reclaim_delay_bars"),
                "Reclaim close": s.get("reclaim_close"),
                "Breach at (IST)": _ist_str(s.get("exceed_time")),
                "Extreme at (IST)": _ist_str(s.get("breach_extreme_time")),
                "Reclaim at (IST)": _ist_str(s.get("sweep_time")),
            } for s in (pa["swept"][-12:] if pa else [])]
            if sweep_rows:
                st.dataframe(
                    pd.DataFrame(sweep_rows).style.format({
                        "Level": "{:,.1f}",
                        "Breach extreme": "{:,.1f}",
                        "Overshoot (pts)": "{:,.1f}",
                        "Reclaim close": "{:,.1f}",
                    }, na_rep="—"),
                    hide_index=True, use_container_width=True, height=230,
                )
            else:
                st.caption("No confirmed price sweeps. A touch or break without a close back through the level within the configured reclaim window is not labelled a sweep.")
            st.caption(
                "A confirmed price sweep is a stop-hunt candidate, not proof that stop orders actually existed or were triggered. "
                "Overshoot and reclaim timing are measured from completed candles."
            )
    with st.expander("Supply / demand zones (price-action-derived)", expanded=True):
        zones = (pa or {}).get("zones", [])
        last_price = _f(d.get("last_price"))
        zone_rows = []
        for z in zones:
            top = _f(z.get("top"))
            bottom = _f(z.get("bottom"))
            if top is None or bottom is None or top < bottom:
                continue
            midpoint = (top + bottom) / 2.0
            distance = (midpoint - last_price) if last_price is not None else None
            displacement = _f(z.get("displacement_x_atr"))
            zone_rows.append({
                "Zone": "DEMAND" if z.get("direction") == "bullish" else "SUPPLY",
                "Lower": bottom,
                "Upper": top,
                "Midpoint": midpoint,
                "Width (pts)": top - bottom,
                "Displacement (x ATR)": displacement,
                "Distance to mid (pts)": distance,
                "Status": str(z.get("status", "unknown")).upper(),
                "Confirmed (IST)": _ist_str(z.get("confirmed_time")),
            })
        zone_rows.sort(key=lambda r: (
            r["Status"] == "INVALIDATED",
            abs(r["Distance to mid (pts)"]) if r["Distance to mid (pts)"] is not None else float("inf"),
            -(r["Displacement (x ATR)"] or 0.0),
        ))
        if zone_rows:
            st.dataframe(
                pd.DataFrame(zone_rows[:12]).style.format({
                    "Lower": "{:,.1f}",
                    "Upper": "{:,.1f}",
                    "Midpoint": "{:,.1f}",
                    "Width (pts)": "{:,.1f}",
                    "Displacement (x ATR)": "{:.2f}",
                    "Distance to mid (pts)": "{:+,.1f}",
                }, na_rep="—"),
                hide_index=True, use_container_width=True, height=300,
            )
        else:
            st.caption("No confirmed supply/demand zones in the current completed-candle lookback.")
        st.caption(
            "Demand = bullish order-block candidates; supply = bearish order-block candidates. "
            "Displacement (x ATR) quantifies the confirming price move, not resting order size or a probability. "
            "Zone boundaries are derived from completed candles and do not prove institutional orders exist there."
        )

    with st.expander("Buy / sell exhaustion (1-minute candle + observed tape)", expanded=True):
        exhaustion = orderflow.detect_exhaustion_labels(
            closed,
            d.get("trade_log", []),
            timeframe_seconds=60,
            lookback=10,
            min_trades=5,
            imbalance_ratio=1.25,
            min_wick_ratio=0.35,
            close_position_limit=0.65,
        ) if closed else []
        exhaustion_rows = [{
            "Type": "BUY exhaustion · bearish risk" if e["kind"] == "buy_exhaust" else "SELL exhaustion · bullish risk",
            "Time (IST)": _ist_str(e.get("time")),
            "Price": e.get("price"),
            "Buy aggr. contracts": e.get("buy_contracts"),
            "Sell aggr. contracts": e.get("sell_contracts"),
            "Dominant-side ratio": e.get("aggressor_ratio"),
            "Wick (% range)": 100.0 * e.get("wick_ratio", 0.0),
            "Close position (%)": 100.0 * e.get("close_position", 0.0),
            "Classified prints": e.get("trade_count"),
        } for e in exhaustion[-8:][::-1]]
        if exhaustion_rows:
            st.dataframe(
                pd.DataFrame(exhaustion_rows).style.format({
                    "Price": "{:,.1f}",
                    "Buy aggr. contracts": "{:,.0f}",
                    "Sell aggr. contracts": "{:,.0f}",
                    "Dominant-side ratio": "{:.2f}x",
                    "Wick (% range)": "{:.1f}%",
                    "Close position (%)": "{:.1f}%",
                    "Classified prints": "{:,.0f}",
                }, na_rep="—"),
                hide_index=True, use_container_width=True, height=260,
            )
        else:
            st.caption("No buy/sell exhaustion candidates meet the existing completed-candle and tape-flow rules. Thresholds are not relaxed to force labels.")
        st.caption(
            "BUY exhaustion: a new local high rejects with an upper wick and buy-aggressor imbalance (bearish risk). "
            "SELL exhaustion: a new local low rejects with a lower wick and sell-aggressor imbalance (bullish risk). "
            "This is a heuristic context label, not proof of a reversal or an entry signal."
        )

    if show_depth_heat:
        st.markdown("**Observed depth heatmap (captured samples only)**")
        fig_heat = _depth_heatmap(d)
        if fig_heat is None:
            st.caption("No captured depth history yet. The heatmap shows only samples recorded since this app started; missing history is not reconstructed.")
        else:
            st.plotly_chart(fig_heat, use_container_width=True, key="btcusd_depth_heatmap",
                            config={"displaylogo": False})
    st.caption(
        "Legend — green/red lines: observed resting book liquidity (thick = strongest; dotted/faded = no longer "
        "observed; hover any line for price, contracts, band range and observed duration) · shaded rectangles: "
        "price-action order blocks (solid = active, dashed = mitigated, dotted grey = invalidated; recent zones "
        "are labelled) · amber dash-dot: equal highs/lows · blue dashed: session range · purple dotted: swings · "
        "magenta: confirmed sweep · triangles: large executed trades (size-scaled, aggressor side from Delta "
        "buyer_role; hover for timestamp, contracts and threshold). Labels are intentionally sparse — use hover "
        "rather than reading every overlapping annotation.")


@st.fragment(run_every=5.0)
def render_diagnostics_panel() -> None:
    """Feed diagnostics: channels, freshness, sequence/CRC, session events."""
    d = feed.snapshot()
    now = time.time()
    market_age = None if d["last_market_message_at"] is None else max(0.0, now - d["last_market_message_at"])
    diag1, diag2, diag3, diag4 = st.columns(4)
    diag1.metric("Connection", d["connection"])
    diag2.metric("Market-data age", "—" if market_age is None else f"{market_age:.2f}s")
    diag3.metric("Sequence gaps", f"{d['sequence_gaps']:,}")
    diag4.metric("CRC failures", f"{d['checksum_failures']:,}")
    st.write(f"Last received channel: `{d['last_market_type'] or '—'}`")
    st.write(f"Exchange status event: {d['system_status']}")
    if d["last_error"]:
        st.error(d["last_error"])
    st.write("**Per-channel feed freshness**")

    channel_counts = d.get("channel_message_counts", {})
    disconnect_count = d.get("total_disconnects", 0)
    reconnect_count = d.get("total_reconnects", 0)

    rows = []
    for ch in ["ticker", "ob_updates", "trades", "mark_price", "candlestick_1m", "funding_rate", "spot_price", "system_status"]:
        ts = d["channel_last_seen"].get(ch)
        msg_count = channel_counts.get(ch, 0)
        if ts is None:
            rows.append({"Channel": ch, "Status": "no data yet", "Age (s)": "—", "Messages": msg_count})
        else:
            age = max(0.0, now - ts)
            threshold = CHANNEL_STALE_SECONDS.get(ch)
            if threshold is None:
                status = "infrequent (no freshness threshold)"
            elif age <= threshold:
                status = "live"
            else:
                status = "stale"
            rows.append({"Channel": ch, "Status": status, "Age (s)": _fmt(age, 1), "Messages": msg_count})
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    st.markdown(
        "- This client checks order-book sequence continuity and CRC32 on book messages that include a checksum. Any gap or mismatch triggers a book resubscription.\n"
        "- Market-data timestamps/ages measure when this client receives messages; they do **not** prove zero network or exchange latency.\n"
        "- This app is read-only: it cannot place, edit, or cancel orders. Public market data does not require API credentials.\n"
        "- The app cannot establish that an exchange is manipulation-free. The separate Bybit, Binance and OKX references are cross-venue context only; they do not replace Delta source data or bypass signal-release gates.\n"
        f"- **Session events**: {disconnect_count} disconnect(s), {reconnect_count} reconnect(s)"
    )


# ---------------------------------------------------------------------------
# Static page shell: layout, section headers and expanders live OUTSIDE the
# refresh fragments so only live data re-renders.
# ---------------------------------------------------------------------------
render_status_panel()

# zero-height same-origin iframe: mirrors the chart's axis view into the URL so
# zoom/pan survives Streamlit's Plotly element re-creation on every refresh
st.components.v1.html(_ZOOM_BRIDGE_HTML, height=0)

large_chart = str(chart_layout).startswith("Large")
if large_chart:
    _emit_price_chart(700)
    with st.expander("Live order book — depth, cumulative quantities, spread, imbalance, sequence & CRC", expanded=False):
        render_book_panel()
else:
    chart_col, book_col = st.columns([2.1, 1], gap="medium")
    with chart_col:
        _emit_price_chart(620)
    with book_col:
        st.subheader("Live order book")
        render_book_panel(compact=True)

st.markdown("---")
render_tape_cvd_panel()

if show_flow_panel:
    st.markdown("---")
    st.subheader("Order flow & liquidity")
    st.caption(
        "Observed resting liquidity comes from the live sequence- and CRC32-synchronized order book; "
        "resting orders can be cancelled, moved or replenished at any time and may not execute. "
        "Order blocks, equal highs/lows and sweeps are price-action-derived from completed candles - not verified "
        "institutional activity. A large executed trade indicates size only, not measured market impact or institutional identity.")
    render_flow_panel()


from datetime import datetime, timezone
import time as _time


def _build_ai_market_structure_context(data: dict[str, Any]) -> dict[str, Any]:
    """Package the actual Delta flow-panel calculations for grounded AI narration.

    Uses the same sidebar parameters and detectors as the visible panels; it
    never alters the frozen signal evaluator or its release criteria.
    """
    now = time.time()
    closed = _closed_candles(data)
    pa = (
        _price_action_analytics(
            closed, ob_lookback, ob_displacement, ob_zone_basis, eq_tolerance
        )
        if closed else {"swings": [], "eq": [], "zones": [], "session": {}, "swept": []}
    )

    def utc_time(value: Any) -> str | None:
        numeric = _f(value)
        if numeric is None:
            return None
        try:
            return datetime.fromtimestamp(float(numeric), tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return None

    session = pa.get("session") or {}
    utc_day_start = datetime.fromtimestamp(now, tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp()
    session_candles = [
        candle for candle in closed
        if _f(candle.get("time")) is not None and float(candle["time"]) >= utc_day_start
    ]
    first_session_candle = min(
        (float(c["time"]) for c in session_candles if _f(c.get("time")) is not None),
        default=None,
    )

    zones = []
    for zone in (pa.get("zones") or [])[-12:]:
        top, bottom = _f(zone.get("top")), _f(zone.get("bottom"))
        if top is None or bottom is None or top < bottom:
            continue
        zones.append({
            "classification": "DEMAND" if zone.get("direction") == "bullish" else "SUPPLY",
            "direction": zone.get("direction"),
            "lower": bottom,
            "upper": top,
            "midpoint_calculated": (bottom + top) / 2.0,
            "width_points_calculated": top - bottom,
            "displacement_x_atr_calculated": _f(zone.get("displacement_x_atr")),
            "status": zone.get("status"),
            "formation_time_utc": utc_time(zone.get("formation_time")),
            "confirmation_time_utc": utc_time(zone.get("confirmed_time")),
            "reference_swing_price": _f(zone.get("swing_price")),
        })

    sweeps = []
    for sweep in (pa.get("swept") or [])[-12:]:
        sweeps.append({
            "candidate": "above-high / buy-side liquidity sweep" if sweep.get("kind") == "high" else "below-low / sell-side liquidity sweep",
            "level": _f(sweep.get("price")),
            "breach_extreme": _f(sweep.get("breach_price")),
            "overshoot_points_calculated": _f(sweep.get("overshoot_points")),
            "reclaim_delay_bars": sweep.get("reclaim_delay_bars"),
            "reclaim_close": _f(sweep.get("reclaim_close")),
            "level_time_utc": utc_time(sweep.get("time")),
            "first_breach_time_utc": utc_time(sweep.get("exceed_time")),
            "extreme_time_utc": utc_time(sweep.get("breach_extreme_time")),
            "reclaim_time_utc": utc_time(sweep.get("sweep_time")),
            "interpretation_limit": "Price breach and reclaim are observed; actual stop orders or stop executions are not proven.",
        })

    exhaustion_raw = orderflow.detect_exhaustion_labels(
        closed,
        data.get("trade_log", []),
        timeframe_seconds=60,
        lookback=10,
        min_trades=5,
        imbalance_ratio=1.25,
        min_wick_ratio=0.35,
        close_position_limit=0.65,
    ) if closed else []
    exhaustion = [{
        "type": row.get("kind"),
        "time_utc": utc_time(row.get("time")),
        "price": _f(row.get("price")),
        "buy_aggressor_contracts": _f(row.get("buy_contracts")),
        "sell_aggressor_contracts": _f(row.get("sell_contracts")),
        "dominant_aggressor_ratio": _f(row.get("aggressor_ratio")),
        "wick_ratio_of_range": _f(row.get("wick_ratio")),
        "close_position_0_to_1": _f(row.get("close_position")),
        "classified_print_count": row.get("trade_count"),
    } for row in exhaustion_raw[-8:]]

    book_valid = (
        bool(data.get("book_synced"))
        and not bool(data.get("sequence_gap_unresolved", True))
        and data.get("checksum_ok") is True
        and bool(data.get("bids")) and bool(data.get("asks"))
    )
    observed_liquidity: dict[str, Any] = {
        "available": False,
        "reason": "Withheld unless the live Delta book is synchronized, checksum-valid and gap-free.",
    }
    if book_valid:
        liquidity_metrics = _book_liquidity(data, float(liq_percentile), 2)
        rows = _liquidity_level_rows(data, liquidity_metrics, now)
        active = [
            {
                "side": row.get("Side"),
                "price": _f(row.get("Price")),
                "contracts": _f(row.get("Contracts")),
                "btc_equivalent_calculated": _f(row.get("BTC equiv")),
                "approx_usd_notional_calculated": _f(row.get("Approx USD notional")),
                "intensity_x_threshold_calculated": _f(row.get("Intensity x threshold")),
                "observed_duration": row.get("Observed"),
                "status": row.get("Status"),
            }
            for row in rows if row.get("Status") == "active"
        ]
        removed = [
            {
                "side": row.get("Side"),
                "price": _f(row.get("Price")),
                "last_observed_contracts": _f(row.get("Contracts")),
                "status": row.get("Status"),
            }
            for row in rows if str(row.get("Status", "")).startswith("removed")
        ]
        active.sort(
            key=lambda row: row.get("intensity_x_threshold_calculated") or 0.0,
            reverse=True,
        )
        observed_liquidity = {
            "available": True,
            "basis": "Current synchronized Delta BTCUSD resting L2 book; not guaranteed execution.",
            "adaptive_percentile": float(liq_percentile),
            "threshold_contracts": _f(liquidity_metrics.get("threshold") or data.get("book_liq_threshold")),
            "intensity_formula": "band contracts / current adaptive threshold",
            "approx_usd_notional_formula": "contracts × 0.001 BTC/contract × price",
            "active_levels": active[:10],
            "recently_removed_levels": removed[:6],
        }

    recent_swings = [{
        "kind": "swing_high" if item.get("kind") == "high" else "swing_low",
        "price": _f(item.get("price")),
        "time_utc": utc_time(item.get("time")),
    } for item in (pa.get("swings") or [])[-12:]]
    equal_levels = [{
        "kind": "equal_highs" if item.get("kind") == "high" else "equal_lows",
        "mean_price_calculated": _f(item.get("price")),
        "touch_count": item.get("count"),
        "last_touch_time_utc": utc_time(item.get("last_time")),
        "configured_tolerance_points": float(eq_tolerance),
    } for item in (pa.get("eq") or [])[-10:]]

    trade_rows = [
        {"ts": item[0], "side": item[3], "size": item[2]}
        for item in data.get("trade_log", [])
        if isinstance(item, (tuple, list)) and len(item) >= 4
    ]
    rolling_flow = orderflow.rolling_aggressor_metrics(
        trade_rows, float(cvd_window_min) * 60.0
    )

    return {
        "source": "Delta Exchange India BTCUSD public market data; read-only.",
        "completed_1m_candles_available": len(closed),
        "calculation_parameters": {
            "order_block_lookback_candles": int(ob_lookback),
            "order_block_displacement_threshold_x_atr": float(ob_displacement),
            "order_block_zone_basis": str(ob_zone_basis),
            "equal_level_tolerance_points": float(eq_tolerance),
            "sweep_reclaim_window_bars": 3,
            "exhaustion_lookback_bars": 10,
            "exhaustion_min_classified_prints": 5,
            "exhaustion_min_aggressor_ratio": 1.25,
            "exhaustion_min_wick_ratio": 0.35,
            "exhaustion_close_position_limit": 0.65,
            "rolling_cvd_window_minutes": int(cvd_window_min),
        },
        "current_utc_session": {
            "utc_day_start": utc_time(utc_day_start),
            "session_high": _f(session.get("session_high")),
            "session_high_time_utc": utc_time(session.get("session_high_time")),
            "session_low": _f(session.get("session_low")),
            "session_low_time_utc": utc_time(session.get("session_low_time")),
            "first_loaded_candle_in_current_utc_day": utc_time(first_session_candle),
            "first_loaded_candle_at_or_before_utc_day_start": bool(
                first_session_candle is not None and first_session_candle <= utc_day_start
            ),
            "coverage_note": "Session highs/lows use available completed candles for the current UTC day; incomplete historical coverage must be stated.",
        },
        "swing_high_low_candidates": recent_swings,
        "equal_high_low_clusters": equal_levels,
        "supply_demand_order_blocks": zones,
        "confirmed_price_sweep_candidates": sweeps,
        "buy_sell_exhaustion_candidates": exhaustion,
        "rolling_aggressor_flow": {
            "window_minutes": int(cvd_window_min),
            "buy_aggressor_contracts": _f(rolling_flow.get("buy_contracts")),
            "sell_aggressor_contracts": _f(rolling_flow.get("sell_contracts")),
            "net_cvd_contracts": _f(rolling_flow.get("cvd_contracts")),
            "formula": "net CVD = sum of buy-aggressor contracts - sum of sell-aggressor contracts in the rolling window.",
            "window_is_bounded_by_in_memory_trade_log": True,
        },
        "observed_resting_liquidity": observed_liquidity,
        "liquidation_data": {
            "delta_market_wide_levels_available": False,
            "reason": "Delta's currently subscribed public channels do not supply a verified market-wide liquidation feed. Do not infer market-wide liquidation prices from an account liquidation price.",
        },
    }


def _log_signal_gate_diagnostic(
    decision,
    reason,
    *,
    feed_state=None,
    details=None,
):
    """Log a panel early return without pretending the engine was evaluated."""
    fs = feed_state if isinstance(feed_state, dict) else {}
    details = details or {}
    now_utc = datetime.now(timezone.utc)
    now_mono = _time.monotonic()
    fingerprint = (
        decision,
        reason,
        details.get("candles_5m"),
        details.get("candles_15m"),
        details.get("error_type"),
        fs.get("connection"),
        fs.get("book_synced"),
        fs.get("checksum_ok", fs.get("crc_valid")),
    )
    throttle_key = "_signal_gate_diagnostic_last"
    previous = st.session_state.get(throttle_key, {})
    if (
        previous.get("fingerprint") == fingerprint
        and now_mono - previous.get("monotonic", 0) < 30
    ):
        return
    record = {
        "ts_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "decision": decision,
        "signal_type": "WAIT" if decision == "WAIT" else None,
        "score": None,
        "confidence": None,
        "blocking_reason": reason,
        "feed_fresh": fs.get("feed_fresh"),
        "book_synced": fs.get("book_synced"),
        "sequence_valid": fs.get("sequence_valid"),
        "crc_valid": fs.get("crc_valid", fs.get("checksum_ok")),
        "feed_age_seconds": fs.get("feed_age_seconds"),
        "book_seq": fs.get("book_seq"),
        "total_disconnects": fs.get("total_disconnects"),
        "total_reconnects": fs.get("total_reconnects"),
        "feed_state": fs,
        "metadata": {
            "record_class": "panel_gate_diagnostic",
            "engine_evaluated": False,
            "reason_code": reason,
            "details": details,
        },
    }
    _log_event(record, is_decision=True)
    st.session_state[throttle_key] = {
        "fingerprint": fingerprint,
        "monotonic": now_mono,
    }


def _render_signal_panel() -> None:
    """Render and persist the exact record returned by the canonical live pipeline."""
    try:
        data = feed.snapshot()
        feed_state = feed.get_signal_state()
    except Exception as exc:
        data, feed_state = {}, {}
        st.warning(f"Feed snapshot unavailable ({type(exc).__name__}); entry is blocked.")
    from signal_engine import SignalEngine
    engine = st.session_state.get("_delta_signal_engine")
    if engine is None:
        engine = SignalEngine()
        st.session_state["_delta_signal_engine"] = engine
    decision_record = evaluate_feed_snapshot(data, feed_state=feed_state, engine=engine,
                                              history_path=EVENT_LOG_FILE)
    log_ok = _log_event(decision_record, is_decision=True)
    decision = decision_record.get("decision", "BLOCKED")
    result = decision_record.get("engine_result") or {}
    if decision == "BUY":
        st.success("CONFIRMED BUY · Analysis only — no order has been placed")
    elif decision == "SELL":
        st.success("CONFIRMED SELL · Analysis only — no order has been placed")
    elif decision == "WAIT":
        st.info("WAIT · No confirmed entry signal")
    else:
        st.warning("BLOCKED · Feed integrity, data quality, or snapshot requirements failed")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Decision", decision)
    c2.metric("Book synced", str(decision_record.get("book_synced", False)))
    c3.metric("CRC verified", str(decision_record.get("crc_valid", False)))
    age = decision_record.get("feed_age_seconds")
    c4.metric("Last market message age", "—" if age is None else f"{float(age):.2f}s")
    if decision in ("BUY", "SELL"):
        fields = (("Proposed entry", "proposed_entry"), ("Stop-loss", "stop_loss"),
                  ("Take-profit", "take_profit"), ("Risk/reward", "risk_reward_ratio"))
        cols = st.columns(4)
        for col, (label, key) in zip(cols, fields):
            value = result.get(key)
            col.metric(label, "—" if value is None else (f"{value:.2f}" if key != "risk_reward_ratio" else f"{value:.2f}:1"))
        st.caption("Proposed analysis levels only; no order has been placed.")
    reasons = result.get("reason_codes") or []
    if decision_record.get("blocking_reason") and not reasons:
        reasons = [decision_record["blocking_reason"]]
    if reasons:
        st.caption("Reason codes: " + "; ".join(str(code) for code in reasons))
    explanation = result.get("human_readable_explanation")
    if explanation:
        st.write(explanation)
    if log_ok:
        st.caption(f"Decision ID: {decision_record.get('signal_id') or '—'} · Persisted to decision log · read-only analysis")
    else:
        st.error("Decision evaluated but logging failed. Check DELTA_DECISION_LOG_FILE and DELTA_EVENT_LOG_FILE permissions.")



show_signal_panel = st.checkbox("Live entry signal panel", value=True)
if show_signal_panel:
    st.markdown("---")
    st.subheader("Live confirmed-entry signal")
    _render_signal_panel()

with st.expander("Feed diagnostics and limitations", expanded=False):
    render_diagnostics_panel()

# Independent Bybit V5 public feed adds cross-venue context without altering the
# Delta-specific detector, signal engine, frozen evaluator, or release gate.
bybit_analyst_context = render_bybit_cross_venue_panel(feed)
okx_history_context = render_okx_history_panel()
binance_analyst_context = render_binance_cross_venue_panel(feed)
ai_snapshot = feed.snapshot()
ai_snapshot["cross_venue_market_data"] = bybit_analyst_context
ai_snapshot["cross_venue_okx_history_context"] = okx_history_context
ai_snapshot["cross_venue_binance_market_data"] = binance_analyst_context
ai_snapshot["delta_market_structure"] = _build_ai_market_structure_context(ai_snapshot)
render_ai_analyst(ai_snapshot)

chart_driver()
