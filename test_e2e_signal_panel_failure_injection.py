"""End-to-end fail-closed integration test for the app's signal panel.

This test executes the actual DeltaFeed class, _render_signal_panel, decision
bridge, signal engine, and JSONL logger from app.py with controlled WebSocket
and Streamlit stubs. It does not open a live socket or replace the manual
running-Streamlit reconnect acceptance test.
"""
from __future__ import annotations

import ast
import json
import logging
import os
import tempfile
import threading
import time
import unittest
import zlib
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

from signal_integration_bridge import evaluate_feed_snapshot

PROJECT = Path(__file__).resolve().parent
APP_SOURCE = (PROJECT / "app.py").read_text(encoding="utf-8")
APP_TREE = ast.parse(APP_SOURCE)


def _top_level_node(name: str, kind):
    for node in APP_TREE.body:
        if isinstance(node, kind) and getattr(node, "name", None) == name:
            return node
    raise RuntimeError(f"Could not find {kind.__name__} {name!r} in app.py")


class _QuietLogger:
    def info(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass


class _FakeSocket:
    def __init__(self):
        self.sent = []
        self.sock = SimpleNamespace(connected=True)

    def send(self, message):
        self.sent.append(message)

    def close(self):
        self.sock.connected = False


class _MetricColumn:
    def __init__(self, owner):
        self.owner = owner

    def metric(self, label, value, *args, **kwargs):
        self.owner.metrics.append((str(label), str(value)))


class _FakeStreamlit:
    def __init__(self):
        self.session_state = {}
        self.metrics = []
        self.messages = []

    def columns(self, count):
        return [_MetricColumn(self) for _ in range(count)]

    def success(self, value, *args, **kwargs): self.messages.append(("success", str(value)))
    def info(self, value, *args, **kwargs): self.messages.append(("info", str(value)))
    def warning(self, value, *args, **kwargs): self.messages.append(("warning", str(value)))
    def error(self, value, *args, **kwargs): self.messages.append(("error", str(value)))
    def caption(self, value, *args, **kwargs): self.messages.append(("caption", str(value)))
    def write(self, value, *args, **kwargs): self.messages.append(("write", str(value)))


_NS = {
    "Any": Any,
    "datetime": datetime,
    "timezone": timezone,
    "ZoneInfo": ZoneInfo,
    "threading": threading,
    "time": time,
    "json": json,
    "os": os,
    "zlib": zlib,
    "deque": deque,
    "websocket": SimpleNamespace(WebSocketApp=_FakeSocket),
    "_logger": _QuietLogger(),
    "_event_lock": threading.RLock(),
    "WS_URL": "wss://test.invalid",
    "SYMBOL": "BTCUSD",
    "INDEX_SYMBOL": ".DEXBTUSD",
    "TZ_IST": ZoneInfo("Asia/Kolkata"),
    "EVENT_LOG_FILE": "",
    "EVENT_LOG_FILE_FULL": "",
    "DECISION_LOG_FILE": "",
    "evaluate_feed_snapshot": evaluate_feed_snapshot,
}
for _name, _kind in (
    ("_f", ast.FunctionDef),
    ("_ts_seconds", ast.FunctionDef),
    ("_log_event", ast.FunctionDef),
    ("DeltaFeed", ast.ClassDef),
    ("_render_signal_panel", ast.FunctionDef),
):
    _node = _top_level_node(_name, _kind)
    _module = ast.Module(body=[_node], type_ignores=[])
    exec(compile(ast.fix_missing_locations(_module), str(PROJECT / "app.py"), "exec"), _NS)


def _healthy_feed(now: float):
    minute = int(now // 60) * 60
    candle_count = 1200
    candles = []
    for index in range(candle_count):
        candles.append({
            "time": minute - (candle_count - index) * 60,
            "open": 100000.0,
            "high": 100002.0,
            "low": 99998.0,
            "close": 100000.0,
            "volume": 2.0,
        })
    feed = _NS["DeltaFeed"](candles)
    seen = now - 0.5
    feed.data.update({
        "connection": "connected",
        "last_any_message_at": seen,
        "last_market_message_at": seen,
        "last_market_type": "ticker",
        "last_price": 100000.0,
        "bid": 99999.5,
        "ask": 100000.5,
        "book_seq": 100,
        "book_synced": True,
        "sequence_gap_unresolved": False,
        "checksum_ok": True,
        "checksum_msg": "CRC32 verified (synthetic fixture)",
        "asks": {"100000.5": "2"},
        "bids": {"99999.5": "2"},
        "channel_last_seen": {
            "ticker": seen,
            "ob_updates": seen,
            "trades": seen,
        },
        "channel_message_counts": {"ticker": 1, "ob_updates": 1, "trades": 20},
        "total_messages_received": 22,
        "total_disconnects": 0,
        "total_reconnects": 0,
    })
    feed.data["trades"] = deque([
        {
            "timestamp": now - (20 - index) * 0.5,
            "price": 100000.0,
            "size": 1.0,
            "buyer_role": "t" if index % 2 == 0 else "m",
        }
        for index in range(20)
    ], maxlen=120)
    return feed


class TestEndToEndSignalPanelFailureRecovery(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        _NS["EVENT_LOG_FILE"] = str(root / "events.jsonl")
        _NS["EVENT_LOG_FILE_FULL"] = str(root / "events_full.jsonl")
        _NS["DECISION_LOG_FILE"] = str(root / "decisions.jsonl")
        _NS["_event_lock"] = threading.RLock()
        self.ui = _FakeStreamlit()
        self.feed = _healthy_feed(time.time())
        _NS["st"] = self.ui
        _NS["feed"] = self.feed
        self.next_seq = 300

    def tearDown(self):
        self.tempdir.cleanup()

    def render_and_read_record(self):
        previous_count = len([1 for label, _ in self.ui.metrics if label == "Decision"])
        _NS["_render_signal_panel"]()
        displayed = [value for label, value in self.ui.metrics if label == "Decision"]
        self.assertEqual(len(displayed), previous_count + 1, "signal panel must render a decision metric")
        decision_path = Path(_NS["DECISION_LOG_FILE"])
        self.assertTrue(decision_path.is_file(), "decision log must exist after panel evaluation")
        rows = [line for line in decision_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertTrue(rows, "decision log must contain at least one decision")
        record = json.loads(rows[-1])
        self.assertEqual(displayed[-1], record["decision"],
                         "displayed decision must equal the final persisted safety-gated decision")
        return record

    def open_socket(self):
        socket = _FakeSocket()
        self.feed.ws = socket
        self.feed._on_open(socket)
        return socket

    def send_snapshot(self, socket, *, include_checksum=True, corrupt_checksum=False):
        asks = [[100000.5, 2], [100001.0, 3]]
        bids = [[99999.5, 2], [99999.0, 3]]
        ask_map, bid_map = {}, {}
        _NS["DeltaFeed"]._apply_levels(ask_map, asks)
        _NS["DeltaFeed"]._apply_levels(bid_map, bids)
        checksum = _NS["DeltaFeed"]._book_checksum(ask_map, bid_map)
        if corrupt_checksum:
            checksum ^= 1
        message = {
            "type": "ob_updates",
            "action": "snapshot",
            "seq": self.next_seq,
            "a": asks,
            "b": bids,
        }
        self.next_seq += 1
        if include_checksum:
            message["cs"] = checksum
        self.feed._on_message(socket, json.dumps(message))

    def refresh_required_channels(self, socket):
        self.feed._on_message(socket, json.dumps({
            "type": "ticker",
            "d": [{
                "s": "BTCUSD",
                "q": [100000.5, 2, 99999.5, 2],
                "m": 100000.0,
            }],
        }))
        self.feed._on_message(socket, json.dumps({
            "type": "trades",
            "sy": "BTCUSD",
            "p": 100000.0,
            "s": 1.0,
            "r": "t",
            "t": int(time.time() * 1_000_000),
        }))

    def recover_with_verified_book(self):
        socket = self.open_socket()
        self.send_snapshot(socket, include_checksum=True)
        self.refresh_required_channels(socket)
        return socket

    def assert_blocked_and_persisted(self):
        record = self.render_and_read_record()
        self.assertEqual(record["decision"], "BLOCKED", record)
        return record

    def assert_recovered_and_persisted(self):
        record = self.render_and_read_record()
        self.assertNotEqual(record["decision"], "BLOCKED", record)
        self.assertTrue(record["metadata"]["engine_evaluated"], record)
        return record

    def test_disconnect_gap_checksum_and_recovery_through_actual_panel(self):
        # The valid synthetic control must traverse DeltaFeed -> app panel ->
        # bridge -> SignalEngine and persist exactly what the UI displays.
        initial = self.render_and_read_record()
        self.assertNotEqual(initial["decision"], "BLOCKED", initial)
        engine = self.ui.session_state["_delta_signal_engine"]

        # Inject the real DeltaFeed disconnect callback.
        self.feed._on_close(None, 1006, "injected disconnect")
        disconnected = self.assert_blocked_and_persisted()
        self.assertIn("connection_healthy", disconnected["metadata"]["failed_data_quality_gates"])

        # Socket-open alone is not recovery: the feed must remain BLOCKED until
        # a fresh, checksummed book snapshot arrives.
        socket = self.open_socket()
        self.assert_blocked_and_persisted()

        # A snapshot without checksum must remain blocked rather than being
        # mislabeled as checksum-verified.
        self.send_snapshot(socket, include_checksum=False)
        self.refresh_required_channels(socket)
        missing_checksum = self.assert_blocked_and_persisted()
        self.assertFalse(missing_checksum["crc_valid"])
        self.assertIsNone(missing_checksum["feed_state"].get("checksum_ok"))

        self.send_snapshot(socket, include_checksum=True)
        self.refresh_required_channels(socket)
        recovered = self.assert_recovered_and_persisted()
        self.assertIs(self.ui.session_state["_delta_signal_engine"], engine)

        # Inject a true sequence gap through _on_message -> _handle_book.
        self.feed.ws = None  # prevents the test from making any network call
        self.feed._on_message(None, json.dumps({
            "type": "ob_updates",
            "action": "update",
            "seq": int(self.feed.data["book_seq"]) + 2,
            "a": [],
            "b": [],
        }))
        gap = self.assert_blocked_and_persisted()
        self.assertIn("no_unresolved_sequence_gap", gap["metadata"]["failed_data_quality_gates"])

        self.recover_with_verified_book()
        self.assert_recovered_and_persisted()

        # Inject an actual checksum mismatch; the feed class must invalidate
        # book integrity and the end-to-end decision must fail closed.
        self.feed.ws = None  # keep this deterministic and socket-free
        self.send_snapshot(None, include_checksum=True, corrupt_checksum=True)
        crc_failure = self.assert_blocked_and_persisted()
        self.assertIn("checksum_ok", crc_failure["metadata"]["failed_data_quality_gates"])

        self.recover_with_verified_book()
        final_record = self.assert_recovered_and_persisted()
        self.assertIs(self.ui.session_state["_delta_signal_engine"], engine)
        self.assertTrue(final_record["orderbook_integrity_ok"])


if __name__ == "__main__":
    unittest.main()
