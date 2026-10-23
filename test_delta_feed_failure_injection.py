"""Fault-injection tests run against DeltaFeed's actual class body without booting Streamlit or opening sockets."""
from __future__ import annotations
import ast, json, logging, threading, time, zlib
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
import unittest
from zoneinfo import ZoneInfo

PROJECT = Path(__file__).resolve().parent
APP_SOURCE = (PROJECT / "app.py").read_text(encoding="utf-8")
TREE = ast.parse(APP_SOURCE)
NODE = next((n for n in TREE.body if isinstance(n, ast.ClassDef) and n.name == "DeltaFeed"), None)
if NODE is None:
    raise RuntimeError("Could not locate DeltaFeed class in app.py")
CLASS_SOURCE = ast.get_source_segment(APP_SOURCE, NODE)

class DummyLogger:
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass

class FakeWebSocketApp: pass

NS = {
    "Any": Any, "threading": threading, "time": time, "deque": deque,
    "datetime": datetime, "timezone": timezone, "ZoneInfo": ZoneInfo,
    "json": json, "zlib": zlib,
    "websocket": SimpleNamespace(WebSocketApp=FakeWebSocketApp),
    "_logger": DummyLogger(), "_log_event": lambda *a, **k: True,
    "WS_URL": "wss://test.invalid", "SYMBOL": "BTCUSD", "INDEX_SYMBOL": ".DEXBTUSD",
    "TZ_IST": ZoneInfo("Asia/Kolkata"),
    "_f": lambda value, default=None: default if value is None else float(value),
    "_ts_seconds": lambda value: int(value),
}
exec(compile("from __future__ import annotations\n" + CLASS_SOURCE, str(PROJECT / "app.py"), "exec"), NS)
DeltaFeed = NS["DeltaFeed"]

class FakeSocket:
    def __init__(self): self.sent=[]; self.sock=SimpleNamespace(connected=True)
    def send(self, message): self.sent.append(message)
    def close(self): self.sock.connected=False

class TestRealDeltaFeedClass(unittest.TestCase):
    def setUp(self):
        self.feed=DeltaFeed([])
        # Keep the real resubscribe state-reset path, but the unconnected fake feed
        # makes the method return before any network send.
        self.resubscribe_reasons=[]
        real_resubscribe=self.feed._send_resubscribe
        def safe_resubscribe(reason):
            self.resubscribe_reasons.append(reason)
            return real_resubscribe(reason)
        self.feed._send_resubscribe=safe_resubscribe

    def snapshot(self, seq=100, checksum=None):
        m={"type":"ob_updates","action":"snapshot","seq":seq,
           "a":[[100001, 2]],"b":[[99999, 2]]}
        if checksum is not None: m["cs"]=checksum
        self.feed._handle_book(m)

    def test_disconnect_invalidates_book_sequence_and_checksum(self):
        self.snapshot()
        self.assertTrue(self.feed.data["book_synced"])
        self.feed._on_close(None,1006,"fault injected")
        self.assertFalse(self.feed.data["book_synced"])
        self.assertTrue(self.feed.data["sequence_gap_unresolved"])
        self.assertIsNone(self.feed.data["checksum_ok"])
        self.assertEqual(self.feed.data["connection"],"disconnected")

    def test_websocket_error_invalidates_integrity_state(self):
        self.snapshot()
        self.feed._on_error(None,RuntimeError("fault injected"))
        self.assertFalse(self.feed.data["book_synced"])
        self.assertTrue(self.feed.data["sequence_gap_unresolved"])
        self.assertIsNone(self.feed.data["checksum_ok"])

    def test_sequence_gap_stays_unresolved_until_new_snapshot(self):
        self.snapshot(seq=100)
        self.feed._handle_book({"type":"ob_updates","action":"update","seq":102,"a":[],"b":[]})
        self.assertTrue(self.feed.data["sequence_gap_unresolved"])
        self.assertFalse(self.feed.data["book_synced"])
        self.assertEqual(self.feed.data["sequence_gaps"],1)
        self.snapshot(seq=200)
        self.assertFalse(self.feed.data["sequence_gap_unresolved"])
        self.assertTrue(self.feed.data["book_synced"])

    def test_crc_mismatch_marks_book_unsafe_until_resync(self):
        self.snapshot(seq=100,checksum=1)  # deliberately wrong checksum
        self.assertFalse(self.feed.data["book_synced"])
        self.assertTrue(self.feed.data["sequence_gap_unresolved"])
        self.assertIsNone(self.feed.data["checksum_ok"])  # reset to unknown during resubscribe
        self.assertEqual(self.feed.data["checksum_failures"],1)

    def test_invalid_snapshot_sequence_does_not_mark_book_healthy(self):
        self.feed._handle_book({"type":"ob_updates","action":"snapshot","seq":"bad",
                                "a":[[100001, 2]],"b":[[99999, 2]]})
        self.assertFalse(self.feed.data["book_synced"])
        self.assertTrue(self.feed.data["sequence_gap_unresolved"])
        self.assertTrue(self.resubscribe_reasons)

    def test_malformed_crc_is_handled_as_integrity_failure(self):
        self.snapshot(seq=400,checksum="not-a-number")
        self.assertFalse(self.feed.data["book_synced"])
        self.assertTrue(self.feed.data["sequence_gap_unresolved"])
        self.assertIsNone(self.feed.data["checksum_ok"])  # reset to unknown during resubscribe

    def test_valid_snapshot_without_checksum_is_truthfully_unverified(self):
        self.snapshot(seq=200)
        self.assertTrue(self.feed.data["book_synced"])
        self.assertFalse(self.feed.data["sequence_gap_unresolved"])
        self.assertIsNone(self.feed.data["checksum_ok"])

    def test_open_then_snapshot_restores_connection_and_sequence(self):
        sock=FakeSocket()
        self.feed._on_open(sock)
        self.assertEqual(self.feed.data["connection"],"connected")
        self.assertTrue(self.feed.data["sequence_gap_unresolved"])
        self.snapshot(seq=300)
        state=self.feed.get_signal_state()
        self.assertTrue(state["book_synced"])
        self.assertFalse(state["sequence_gap_unresolved"])
        self.assertEqual(len(sock.sent),2)

if __name__ == "__main__": unittest.main()
