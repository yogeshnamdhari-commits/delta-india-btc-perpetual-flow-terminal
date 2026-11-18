"""Report serialization tests for the frozen OOS evaluator."""
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import oos_replay as oos
import run_oos_evaluation as evaluator

T0 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)


def build_inputs(print_count=1000, covered_minutes=800):
    # 901 bars spanning exactly 60 days; chronology is patched in these unit
    # tests so the serializer can be tested independently of data validation.
    bars = [oos.Bar1m(
        ts_utc=T0 + timedelta(minutes=96 * i),
        open=100.0, high=101.0, low=99.0, close=100.5, volume=10.0,
    ) for i in range(901)]
    trades = []
    for i in range(print_count):
        minute_index = i % covered_minutes
        trades.append(oos.Print1m(
            ts_utc=bars[minute_index].ts_utc + timedelta(seconds=i % 50),
            price=100.0, size=1.0, side_aggressor="BUY_TAKER",
        ))
    return bars, trades


class TestOosReportPersistence(unittest.TestCase):
    def _fake_replay(self, bars):
        decision_ts = bars[850].ts_utc.isoformat()
        simulated_trade = {
            "signal_id": "sim-1", "decision_ts_utc": decision_ts,
            "direction": "BUY", "outcome": "timeout", "net_r": -0.25,
        }
        return {
            "params_version": "oos-1.0",
            "metrics": {"trade_count": 1, "net_r_total": -0.25},
            "stats": {
                "signals_confirmed": 2, "signals_simulated": 1,
                "signals_rejected": 1,
                "simulation_rejection_counts": {"REJECTED_EXPIRED_AT_ENTRY": 1},
            },
            "simulated_trades": [simulated_trade],
            "signals": [
                {
                    "signal_id": "sim-1", "decision_ts_utc": decision_ts,
                    "audit_event_key": f"sim-1|{decision_ts}",
                    "simulation_status": "SIMULATED",
                    "simulation_rejection_reason": None, "rejection_ts_utc": None,
                },
                {
                    "signal_id": "rej-1", "decision_ts_utc": bars[851].ts_utc.isoformat(),
                    "audit_event_key": f"rej-1|{bars[851].ts_utc.isoformat()}",
                    "simulation_status": "REJECTED",
                    "simulation_rejection_reason": "REJECTED_EXPIRED_AT_ENTRY",
                    "rejection_ts_utc": bars[851].ts_utc.isoformat(),
                },
            ],
            "expired_candidates": [
                {
                    "signal_id": "expired-1", "audit_event_key": "expired-1|decision",
                    "decision_ts_utc": bars[852].ts_utc.isoformat(),
                    "trigger_ts_utc": bars[849].ts_utc.isoformat(),
                    "last_completed_candle_ts_utc": bars[851].ts_utc.isoformat(),
                    "setup_candle_timestamp": bars[840].ts_utc.isoformat(),
                    "expires_at_utc": bars[852].ts_utc.isoformat(),
                    "reason_code": "WAIT_SETUP_EXPIRED",
                }
            ],
        }

    def _run_main(self, root, bars, trades, replay=None):
        bars_file = Path(root) / "bars.csv"
        trades_file = Path(root) / "trades.csv"
        bars_file.write_text("synthetic bars source hash\n", encoding="utf-8")
        trades_file.write_text("synthetic trades source hash\n", encoding="utf-8")
        output_dir = Path(root) / "report"
        fake_replay = replay if replay is not None else self._fake_replay(bars)
        argv = ["run_oos_evaluation.py", "--bars", str(bars_file),
                "--trades", str(trades_file), "--output-dir", str(output_dir)]
        patches = [
            patch.object(oos, "load_bars_csv", return_value=bars),
            patch.object(oos, "load_trades_csv", return_value=trades),
            patch.object(oos, "validate_chronology", return_value=[]),
            patch.object(oos, "run_replay", return_value=fake_replay),
            patch.object(sys, "argv", argv),
        ]
        with patches[0], patches[1], patches[2], patches[3] as replay_mock, patches[4]:
            evaluator.main()
        report_path = output_dir / "oos_report.json"
        return json.loads(report_path.read_text(encoding="utf-8")), replay_mock

    def test_report_persists_simulated_rejected_and_expired_audit_records(self):
        bars, trades = build_inputs()
        with tempfile.TemporaryDirectory() as root:
            report, _ = self._run_main(root, bars, trades)

        self.assertEqual(report["signals"][0]["simulation_status"], "SIMULATED")
        self.assertEqual(report["signals"][1]["simulation_status"], "REJECTED")
        self.assertEqual(report["signals"][1]["simulation_rejection_reason"],
                         "REJECTED_EXPIRED_AT_ENTRY")
        self.assertEqual(report["signals"][1]["rejection_ts_utc"],
                         bars[851].ts_utc.isoformat())
        self.assertEqual(report["expired_candidates"][0]["trigger_ts_utc"],
                         bars[849].ts_utc.isoformat())
        self.assertEqual(report["expired_candidates"][0]["last_completed_candle_ts_utc"],
                         bars[851].ts_utc.isoformat())
        self.assertEqual(report["expired_candidates"][0]["setup_candle_timestamp"],
                         bars[840].ts_utc.isoformat())
        self.assertEqual(report["expired_candidates"][0]["expires_at_utc"],
                         bars[852].ts_utc.isoformat())
        self.assertEqual(report["stats"]["signals_rejected"], 1)

    def test_gate_blocked_report_contains_empty_audit_arrays(self):
        bars, trades = build_inputs(print_count=10, covered_minutes=10)
        with tempfile.TemporaryDirectory() as root:
            report, replay_mock = self._run_main(root, bars, trades)

        self.assertEqual(report["status"], "BLOCKED_INSUFFICIENT_DATA")
        self.assertEqual(report["signals"], [])
        self.assertEqual(report["expired_candidates"], [])
        replay_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
