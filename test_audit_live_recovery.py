"""Unit tests for the read-only live reconnect log auditor."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from audit_live_recovery import audit_records, parse_timestamp

UTC = timezone.utc
T0 = datetime(2026, 10, 23, 18, 0, tzinfo=UTC)


def ts(offset_seconds: int) -> str:
    return (T0 + timedelta(seconds=offset_seconds)).isoformat().replace("+00:00", "Z")


def sample_logs():
    events = [
        {"event": "diagnostic_reconnect_requested", "ts_utc": ts(0), "reason": "manual_test"},
        {"event": "ws_disconnected", "ts_utc": ts(1), "code": 1000},
        {"event": "ws_connected", "ts_utc": ts(3)},
        {"event": "ob_update", "ts_utc": ts(4), "action": "snapshot",
         "checksummed": True, "crc_ok": True, "seq": 300},
    ]
    blocked = {
        "ts_utc": ts(2),
        "decision": "BLOCKED",
        "feed_fresh": False,
        "book_synced": False,
        "no_unresolved_sequence_gap": False,
        "crc_valid": False,
        "orderbook_integrity_ok": False,
        "feed_state": {"connection": "disconnected", "book_synced": False, "checksum_ok": None},
        "metadata": {"failed_data_quality_gates": ["connection_healthy"]},
    }
    healthy = {
        "ts_utc": ts(5),
        "decision": "WAIT",
        "feed_fresh": True,
        "book_synced": True,
        "no_unresolved_sequence_gap": True,
        "crc_valid": True,
        "orderbook_integrity_ok": True,
        "feed_state": {
            "connection": "connected",
            "book_synced": True,
            "checksum_ok": True,
            "derived_health": {"channel_freshness": {
                "ticker": True, "ob_updates": True, "trades": True
            }},
        },
        "metadata": {"engine_evaluated": True, "failed_data_quality_gates": []},
    }
    return events, [blocked, healthy]


class TestParseTimestamp(unittest.TestCase):
    def test_iso_z_is_utc(self):
        self.assertEqual(parse_timestamp("2026-10-23T18:00:00Z"), T0)

    def test_numeric_milliseconds(self):
        self.assertEqual(parse_timestamp(T0.timestamp() * 1000), T0)

    def test_invalid_timestamp_returns_none(self):
        self.assertIsNone(parse_timestamp("not-a-timestamp"))


class TestLiveRecoveryAudit(unittest.TestCase):
    def test_complete_reconnect_path_passes(self):
        events, decisions = sample_logs()
        report = audit_records(events, decisions)
        self.assertEqual(report["status"], "PASS", report)
        self.assertEqual(report["diagnostic"]["recovered_decision"], "WAIT")
        self.assertEqual(report["diagnostic"]["recovery_seconds"], 4.0)

    def test_missing_diagnostic_marker_fails(self):
        events, decisions = sample_logs()
        events = [e for e in events if e["event"] != "diagnostic_reconnect_requested"]
        report = audit_records(events, decisions)
        self.assertEqual(report["status"], "FAIL")
        self.assertFalse(next(c for c in report["checks"] if c["name"] == "diagnostic_trigger_recorded")["passed"])

    def test_no_blocked_decision_during_disconnect_fails(self):
        events, decisions = sample_logs()
        report = audit_records(events, decisions[1:])
        self.assertEqual(report["status"], "FAIL")
        self.assertFalse(next(c for c in report["checks"] if c["name"] == "blocked_during_failure")["passed"])

    def test_nonblocked_decision_before_book_recovery_fails(self):
        events, decisions = sample_logs()
        unsafe = dict(decisions[0], ts_utc=ts(2), decision="WAIT")
        report = audit_records(events, [unsafe, decisions[1]])
        self.assertEqual(report["status"], "FAIL")
        self.assertFalse(next(c for c in report["checks"] if c["name"] == "no_entry_decisions_before_book_recovery")["passed"])

    def test_checksum_missing_from_recovered_book_fails(self):
        events, decisions = sample_logs()
        events[-1] = dict(events[-1], checksummed=False, crc_ok=True)
        report = audit_records(events, decisions)
        self.assertEqual(report["status"], "FAIL")
        self.assertFalse(next(c for c in report["checks"] if c["name"] == "verified_book_recovered")["passed"])

    def test_stale_channel_prevents_recovery_pass(self):
        events, decisions = sample_logs()
        unhealthy = dict(decisions[1])
        unhealthy["feed_state"] = dict(unhealthy["feed_state"])
        unhealthy["feed_state"]["derived_health"] = {"channel_freshness": {
            "ticker": True, "ob_updates": True, "trades": False
        }}
        report = audit_records(events, [decisions[0], unhealthy])
        self.assertEqual(report["status"], "FAIL")
        self.assertFalse(next(c for c in report["checks"] if c["name"] == "healthy_decision_after_recovery")["passed"])

    def test_parse_errors_fail_even_if_other_evidence_is_valid(self):
        events, decisions = sample_logs()
        report = audit_records(events, decisions, event_parse_errors=["line 8: bad JSON"])
        self.assertEqual(report["status"], "FAIL")
        self.assertFalse(next(c for c in report["checks"] if c["name"] == "event_log_readable")["passed"])

    def test_recovery_over_deadline_fails(self):
        events, decisions = sample_logs()
        late = dict(decisions[1], ts_utc=ts(200))
        report = audit_records(events, [decisions[0], late], max_recovery_seconds=120)
        self.assertEqual(report["status"], "FAIL")
        self.assertFalse(next(c for c in report["checks"] if c["name"] == "recovery_within_time_limit")["passed"])


if __name__ == "__main__":
    unittest.main()
