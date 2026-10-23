"""Test synchronized signal-decision logging with feed-health state.

This test verifies that:
1. Decisions cannot be made during invalid feed states
2. Feed-health state is recorded with each decision
3. BLOCKED decisions are properly logged with reasons
4. Timestamp alignment between events and decisions can be verified
"""
import json
import os
import tempfile
import time
import unittest
from datetime import datetime, timezone

import app


class TestSignalDecisionLogging(unittest.TestCase):
    def setUp(self):
        self.original_env = os.environ.copy()
        self.test_dir = tempfile.mkdtemp()
        self.event_log = os.path.join(self.test_dir, "test_events.jsonl")
        self.decision_log = os.path.join(self.test_dir, "test_decisions.jsonl")
        self.full_log = os.path.join(self.test_dir, "test_full.jsonl")

        os.environ["DELTA_EVENT_LOG_FILE"] = self.event_log
        os.environ["DELTA_DECISION_LOG_FILE"] = self.decision_log

        app.EVENT_LOG_FILE = self.event_log
        app.EVENT_LOG_FILE_FULL = self.full_log
        app.DECISION_LOG_FILE = self.decision_log

        app._event_lock = __import__("threading").RLock()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_env)
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_log_event_writes_to_decision_file(self):
        decision = {
            "decision": "BUY",
            "score": 85.5,
            "confidence": 0.9,
        }
        app._log_event(decision, is_decision=True)

        with open(self.decision_log) as f:
            logged = json.loads(f.readline())

        self.assertEqual(logged["decision"], "BUY")
        self.assertIn("ts_utc", logged)

    def test_evaluate_decision_blocks_on_disconnect(self):
        feed_state = {
            "connection": "disconnected",
            "book_synced": False,
            "checksum_ok": None,
            "sequence_gaps": 0,
            "checksum_failures": 0,
            "total_disconnects": 1,
            "total_reconnects": 0,
            "channel_message_counts": {},
            "channel_last_seen": {},
            "last_any_message_at": 0,
            "last_market_message_at": 0,
        }

        decision = app._evaluate_signal_decision(
            feed_state,
            signal_type="BUY",
            score=90.0,
        )

        self.assertEqual(decision["decision"], "BLOCKED")
        self.assertIn("not_connected", decision["blocking_reason"])
        self.assertEqual(decision["feed_fresh"], False)
        self.assertEqual(decision["book_synced"], False)

    def test_evaluate_decision_blocks_on_checksum_failure(self):
        now = time.time()
        feed_state = {
            "connection": "connected",
            "book_synced": True,
            "checksum_ok": False,  # Current CRC verification failed
            "checksum_msg": "CRC mismatch",
            "checksum_failures": 3,  # Historical cumulative count
            "sequence_gaps": 0,
            "total_disconnects": 0,
            "total_reconnects": 5,
            "channel_message_counts": {"ticker": 100},
            "channel_last_seen": {"ticker": now},
            "last_any_message_at": now,
            "last_market_message_at": now,
        }

        decision = app._evaluate_signal_decision(
            feed_state,
            signal_type="BUY",
            score=85.0,
        )

        self.assertEqual(decision["decision"], "BLOCKED")
        self.assertIn("crc_verification_failed", decision["blocking_reason"])
        # Cumulative counter recorded for audit but not gating
        self.assertEqual(decision["checksum_failures_cumul"], 3)

    def test_evaluate_decision_blocks_on_sequence_gap(self):
        now = time.time()
        feed_state = {
            "connection": "connected",
            "book_synced": False,  # Must be False after sequence gap recovery
            "checksum_ok": True,
            "sequence_gaps": 1,    # Historical count - not used for gating
            "checksum_failures": 0,
            "total_disconnects": 0,
            "total_reconnects": 1,
            "channel_message_counts": {"ticker": now},
            "channel_last_seen": {"ticker": now},
            "last_any_message_at": now,
            "last_market_message_at": now,
        }

        decision = app._evaluate_signal_decision(
            feed_state,
            signal_type="SELL",
            score=78.0,
        )

        self.assertEqual(decision["decision"], "BLOCKED")
        # book_not_synced blocks because book is not synchronized
        self.assertIn("book_not_synced", decision["blocking_reason"])
        # Cumulative counter recorded for audit but not gating
        self.assertEqual(decision["sequence_gaps_cumul"], 1)

    def test_evaluate_decision_allows_valid_feed(self):
        now = time.time()
        feed_state = {
            "connection": "connected",
            "book_synced": True,
            "checksum_ok": True,
            "checksum_msg": "CRC32 verified",
            "checksum_failures": 0,
            "sequence_gaps": 0,
            "total_disconnects": 0,
            "total_reconnects": 1,
            "channel_message_counts": {"ticker": 100, "ob_updates": 500000, "trades": 80000},
            "channel_last_seen": {"ticker": now, "ob_updates": now, "trades": now},
            "last_any_message_at": now,
            "last_market_message_at": now,
        }

        decision = app._evaluate_signal_decision(
            feed_state,
            signal_type="BUY",
            score=88.5,
            confidence=0.95,
            metadata={"pattern": "order_block_bullish"},
        )

        self.assertEqual(decision["decision"], "BUY")
        self.assertIsNone(decision["blocking_reason"])
        self.assertEqual(decision["score"], 88.5)
        self.assertEqual(decision["confidence"], 0.95)
        self.assertTrue(decision["feed_fresh"])
        self.assertTrue(decision["book_synced"])
        self.assertTrue(decision["sequence_valid"])

    def test_decision_log_has_feed_health_state(self):
        now = time.time()
        feed_state = {
            "connection": "connected",
            "book_synced": True,
            "checksum_ok": True,
            "sequence_gaps": 0,
            "checksum_failures": 0,
            "total_disconnects": 5,
            "total_reconnects": 10,
            "channel_message_counts": {
                "ticker": 1234,
                "ob_updates": 50000,
                "trades": 40000,
                "candlestick_1m": 10000,
                "mark_price": 5000,
                "spot_price": 600,
                "funding_rate": 50,
                "system_status": 1,
            },
            "channel_last_seen": {
                "ticker": now,
                "ob_updates": now - 0.1,
                "trades": now - 0.2,
            },
            "last_any_message_at": now,
            "last_market_message_at": now - 0.1,
        }

        app._evaluate_signal_decision(feed_state, "BUY", 95.0)

        with open(self.decision_log) as f:
            logged = json.loads(f.readline())

        self.assertIn("feed_state", logged)
        self.assertEqual(logged["feed_state"]["connection"], "connected")
        self.assertEqual(logged["feed_state"]["book_synced"], True)
        self.assertEqual(logged["decision"], "BUY")
        self.assertEqual(logged["feed_state"]["total_disconnects"], 5)

    def test_timestamps_aligned_in_iso_format(self):
        """Verify timestamps use ISO-8601 format for correlation."""
        decision = {
            "decision": "BUY",
            "score": 80.0,
        }
        app._log_event(decision, is_decision=True)

        with open(self.decision_log) as f:
            logged = json.loads(f.readline())

        ts = logged["ts_utc"]
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))

        self.assertIsInstance(dt, datetime)

    def test_blocking_reason_records_correct_cause(self):
        """Test that all blocking causes are properly recorded."""
        now = time.time()
        test_cases = [
            ({
                "connection": "disconnected",
                "book_synced": False,
                "checksum_ok": None,
                "sequence_gaps": 0,
                "checksum_failures": 0,
                "total_disconnects": 0,
                "total_reconnects": 0,
                "channel_message_counts": {},
                "channel_last_seen": {},
                "last_any_message_at": 0,
                "last_market_message_at": 0,
            }, "not_connected"),
            ({
                "connection": "connected",
                "book_synced": False,  # BLOCKS: book not synced
                "checksum_ok": True,
                "sequence_gaps": 0,
                "checksum_failures": 0,
                "total_disconnects": 0,
                "total_reconnects": 1,
                "channel_message_counts": {"ticker": now},
                "channel_last_seen": {"ticker": now},
                "last_any_message_at": now,  # Current timestamp
                "last_market_message_at": now,
            }, "book_not_synced"),
            ({
                "connection": "connected",
                "book_synced": True,
                "checksum_ok": True,
                "sequence_gaps": 1,  # Historical count - NOT gating anymore
                "checksum_failures": 0,
                "total_disconnects": 0,
                "total_reconnects": 1,
                "channel_message_counts": {"ticker": now},
                "channel_last_seen": {"ticker": now},
                "last_any_message_at": now,  # Current timestamp
                "last_market_message_at": now,
            }, None),  # NOT BLOCKED - sequence gap is audit-only, book synced
            ({
                "connection": "connected",
                "book_synced": True,
                "checksum_ok": False,  # BLOCKS: current CRC verification failed
                "checksum_msg": "CRC mismatch",
                "sequence_gaps": 0,
                "checksum_failures": 1,
                "total_disconnects": 0,
                "total_reconnects": 1,
                "channel_message_counts": {"ticker": now},
                "channel_last_seen": {"ticker": now},
                "last_any_message_at": now,  # Current timestamp
                "last_market_message_at": now,
            }, "crc_verification_failed"),
        ]

        for feed_state, expected_cause in test_cases:
            decision = app._evaluate_signal_decision(feed_state, "BUY", 90.0)
            expected_decision = "BLOCKED" if expected_cause else "BUY"
            self.assertEqual(
                decision["decision"], expected_decision,
                f"Expected {expected_decision} for {feed_state.get('connection')}, got {decision['decision']}"
            )
            if expected_cause:
                self.assertIn(expected_cause, decision["blocking_reason"])

    def test_multiple_blocks_combined(self):
        """When multiple integrity checks fail, all causes should be in blocking_reason."""
        now = time.time()
        feed_state = {
            "connection": "disconnected",
            "book_synced": False,
            "checksum_ok": False,
            "sequence_gaps": 1,
            "checksum_failures": 1,
            "total_disconnects": 1,
            "total_reconnects": 0,
            "channel_message_counts": {},
            "channel_last_seen": {},
            "last_any_message_at": 0,
            "last_market_message_at": 0,
        }

        decision = app._evaluate_signal_decision(feed_state, "BUY", 90.0)

        self.assertEqual(decision["decision"], "BLOCKED")
        self.assertIn("not_connected", decision["blocking_reason"])
        self.assertIn("book_not_synced", decision["blocking_reason"])
        # sequence_gap_detected is NOT in blocking_reason because cumulative counter is NOT gating
        self.assertIn("crc_verification_failed", decision["blocking_reason"])
        # But the cumulative counters are still recorded for audit
        self.assertEqual(decision["sequence_gaps_cumul"], 1)
        self.assertEqual(decision["checksum_failures_cumul"], 1)


if __name__ == "__main__":
    unittest.main()