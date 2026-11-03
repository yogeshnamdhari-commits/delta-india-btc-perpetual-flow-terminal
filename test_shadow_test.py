#!/usr/bin/env python3
"""Unit tests for shadow_test.py — prospective shadow-testing engine.

Covers signal normalization (BUY/SELL/WAIT/BLOCKED), shadow-trade creation
and completion, JSONL persistence, restart behavior, and holdout counter
accuracy.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from dataclasses import asdict

from shadow_test import ShadowTestEngine, ShadowTrade, get_shadow_engine, reset_shadow_engine


def _make_result(sig_value, entry=100.0, stop=99.0, target=102.0, inv=98.5,
                 signal_id="test_123", state_name="CONFIRMED"):
    """Build a mock SignalResult-like object."""
    return SimpleNamespace(
        signal=sig_value,
        signal_id=signal_id,
        state=SimpleNamespace(name=state_name),
        generated_at_utc=datetime(2026, 11, 3, 1, 0, 0, tzinfo=timezone.utc),
        expires_at_utc=None,
        proposed_entry=entry,
        stop_loss=stop,
        take_profit=target,
        invalidation_price=inv,
        risk_distance=1.0,
        reward_distance=2.0,
        risk_reward_ratio=2.0,
        setup_timeframe="5m",
        setup_candle_timestamp=datetime(2026, 11, 3, 0, 55, 0, tzinfo=timezone.utc),
        reason_codes=[],
        confirmation_conditions_passed=[],
        confirmation_conditions_failed=[],
        human_readable_explanation="test",
        input_candle_timestamps={},
        data_quality_gate_results={},
        duplicate_suppressed=False,
        superseded_signal_id=None,
        lifecycle_transitions=[],
    )


class TestShadowSignalNormalization(unittest.TestCase):
    """BUY / SELL / WAIT / BLOCKED signal classification."""

    def setUp(self):
        reset_shadow_engine()
        self._tmpdir = tempfile.TemporaryDirectory()
        self._shadow_path = Path(self._tmpdir.name) / "shadow_trades.jsonl"
        self._decision_path = Path(self._tmpdir.name) / "shadow_decisions.jsonl"
        os.environ["DELTA_SHADOW_LOG_FILE"] = str(self._shadow_path)
        os.environ["DELTA_SHADOW_DECISION_LOG"] = str(self._decision_path)
        self.engine = ShadowTestEngine()

    def test_buy_signal_is_confirmed(self):
        self.engine.ingest_result(_make_result("BUY"), {})
        self.assertEqual(self.engine.total_confirmed, 1)
        self.assertEqual(self.engine.total_signals_evaluated, 1)
        self.assertEqual(self.engine.open_trade_count, 1)

    def test_sell_signal_is_confirmed(self):
        self.engine.ingest_result(_make_result("SELL"), {})
        self.assertEqual(self.engine.total_confirmed, 1)
        self.assertEqual(self.engine.open_trade_count, 1)

    def test_wait_signal_is_not_confirmed(self):
        self.engine.ingest_result(_make_result("WAIT"), {})
        self.assertEqual(self.engine.total_confirmed, 0)
        self.assertEqual(self.engine.total_wait, 1)
        self.assertEqual(self.engine.open_trade_count, 0)

    def test_blocked_signal_is_not_confirmed(self):
        self.engine.ingest_result(_make_result("BLOCKED"), {})
        self.assertEqual(self.engine.total_confirmed, 0)
        self.assertEqual(self.engine.total_blocked, 1)
        self.assertEqual(self.engine.open_trade_count, 0)

    def test_mixed_signals_counted_correctly(self):
        self.engine.ingest_result(_make_result("BUY"), {})
        self.engine.ingest_result(_make_result("SELL"), {})
        self.engine.ingest_result(_make_result("WAIT"), {})
        self.engine.ingest_result(_make_result("BLOCKED"), {})
        self.assertEqual(self.engine.total_confirmed, 2)
        self.assertEqual(self.engine.total_wait, 1)
        self.assertEqual(self.engine.total_blocked, 1)
        self.assertEqual(self.engine.total_signals_evaluated, 4)

    def test_enum_signal_members_recognized(self):
        from signal_engine import Signal
        self.engine.ingest_result(_make_result(Signal.BUY), {})
        self.engine.ingest_result(_make_result(Signal.SELL), {})
        self.engine.ingest_result(_make_result(Signal.WAIT), {})
        self.assertEqual(self.engine.total_confirmed, 2)
        self.assertEqual(self.engine.total_wait, 1)

    def test_blocked_decision_preserved_separately(self):
        """BLOCKED decisions must not be collapsed into WAIT."""
        self.engine.ingest_result(_make_result("BLOCKED"), {})
        self.assertEqual(self.engine.total_blocked, 1)
        self.assertEqual(self.engine.total_wait, 0)
        self.assertEqual(self.engine.total_confirmed, 0)
        self.assertEqual(self.engine.open_trade_count, 0)

    def test_blocked_not_counted_as_confirmed(self):
        self.engine.ingest_result(_make_result("BUY"), {})
        self.engine.ingest_result(_make_result("BLOCKED"), {})
        self.engine.ingest_result(_make_result("WAIT"), {})
        self.assertEqual(self.engine.total_confirmed, 1)
        self.assertEqual(self.engine.total_blocked, 1)
        self.assertEqual(self.engine.total_wait, 1)

    def tearDown(self):
        self._tmpdir.cleanup()
        os.environ.pop("DELTA_SHADOW_LOG_FILE", None)
        os.environ.pop("DELTA_SHADOW_DECISION_LOG", None)
        reset_shadow_engine()


class TestShadowTradeCreation(unittest.TestCase):
    """Shadow trade creation from confirmed signal results."""

    def setUp(self):
        reset_shadow_engine()
        self._tmpdir = tempfile.TemporaryDirectory()
        self._shadow_path = Path(self._tmpdir.name) / "shadow_trades.jsonl"
        self._decision_path = Path(self._tmpdir.name) / "shadow_decisions.jsonl"
        os.environ["DELTA_SHADOW_LOG_FILE"] = str(self._shadow_path)
        os.environ["DELTA_SHADOW_DECISION_LOG"] = str(self._decision_path)
        self.engine = ShadowTestEngine()

    def test_buy_trade_created_with_correct_levels(self):
        self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0), {})
        trades = self.engine.get_open_trades()
        self.assertEqual(len(trades), 1)
        t = trades[0]
        self.assertEqual(t.direction, "BUY")
        self.assertEqual(t.entry, 100.0)
        self.assertEqual(t.stop_loss, 99.0)
        self.assertEqual(t.take_profit, 102.0)
        self.assertEqual(t.invalidation_price, 98.5)
        self.assertEqual(t.state, "OPEN")
        self.assertEqual(t.pnl_r, None)
        self.assertEqual(t.exit_price, None)

    def test_sell_trade_created_with_correct_levels(self):
        self.engine.ingest_result(_make_result("SELL", entry=100.0, stop=101.0, target=98.0), {})
        trades = self.engine.get_open_trades()
        self.assertEqual(len(trades), 1)
        t = trades[0]
        self.assertEqual(t.direction, "SELL")
        self.assertEqual(t.entry, 100.0)
        self.assertEqual(t.stop_loss, 101.0)
        self.assertEqual(t.take_profit, 98.0)

    def test_wait_signal_does_not_create_trade(self):
        self.engine.ingest_result(_make_result("WAIT"), {})
        self.assertEqual(self.engine.open_trade_count, 0)
        self.assertEqual(self.engine.completed_holdout_count, 0)

    def test_duplicate_signal_ids_are_separate_trades(self):
        self.engine.ingest_result(_make_result("BUY", signal_id="sig_1"), {})
        self.engine.ingest_result(_make_result("BUY", signal_id="sig_2"), {})
        self.assertEqual(self.engine.open_trade_count, 2)

    def tearDown(self):
        self._tmpdir.cleanup()
        os.environ.pop("DELTA_SHADOW_LOG_FILE", None)
        os.environ.pop("DELTA_SHADOW_DECISION_LOG", None)
        reset_shadow_engine()


class TestShadowTradeCompletion(unittest.TestCase):
    """Shadow trade terminal states and P&L computation."""

    def setUp(self):
        reset_shadow_engine()
        self._tmpdir = tempfile.TemporaryDirectory()
        self._shadow_path = Path(self._tmpdir.name) / "shadow_trades.jsonl"
        self._decision_path = Path(self._tmpdir.name) / "shadow_decisions.jsonl"
        os.environ["DELTA_SHADOW_LOG_FILE"] = str(self._shadow_path)
        os.environ["DELTA_SHADOW_DECISION_LOG"] = str(self._decision_path)
        self.engine = ShadowTestEngine()
        self.now = datetime(2026, 11, 3, 1, 10, 0, tzinfo=timezone.utc)

    def test_buy_target_hit(self):
        self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0), {})
        completed = self.engine.track_price(102.0, self.now)
        self.assertEqual(len(completed), 1)
        t = completed[0]
        self.assertEqual(t.state, "TARGET_HIT")
        self.assertEqual(t.exit_price, 102.0)
        self.assertEqual(t.exit_reason, "target_hit")
        self.assertEqual(t.pnl_r, 2.0)  # (102-100)/1.0 = 2.0 R

    def test_buy_stop_hit(self):
        self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0), {})
        completed = self.engine.track_price(99.0, self.now)
        self.assertEqual(len(completed), 1)
        t = completed[0]
        self.assertEqual(t.state, "STOP_HIT")
        self.assertEqual(t.exit_price, 99.0)
        self.assertEqual(t.exit_reason, "stop_hit")
        self.assertEqual(t.pnl_r, -1.0)  # (99-100)/1.0 = -1.0 R

    def test_buy_invalidated(self):
        self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0, inv=98.5), {})
        completed = self.engine.track_price(98.5, self.now)
        self.assertEqual(len(completed), 1)
        t = completed[0]
        self.assertEqual(t.state, "INVALIDATED")
        self.assertEqual(t.exit_price, 98.5)
        self.assertEqual(t.exit_reason, "invalidated")

    def test_sell_target_hit(self):
        self.engine.ingest_result(_make_result("SELL", entry=100.0, stop=101.0, target=98.0), {})
        completed = self.engine.track_price(98.0, self.now)
        self.assertEqual(len(completed), 1)
        t = completed[0]
        self.assertEqual(t.state, "TARGET_HIT")
        self.assertEqual(t.exit_price, 98.0)
        self.assertEqual(t.pnl_r, 2.0)  # (100-98)/1.0 = 2.0 R

    def test_sell_stop_hit(self):
        self.engine.ingest_result(_make_result("SELL", entry=100.0, stop=101.0, target=98.0, inv=105.0), {})
        completed = self.engine.track_price(101.0, self.now)
        self.assertEqual(len(completed), 1)
        t = completed[0]
        self.assertEqual(t.state, "STOP_HIT")
        self.assertEqual(t.exit_price, 101.0)
        self.assertEqual(t.exit_reason, "stop_hit")
        self.assertEqual(t.pnl_r, -1.0)

    def test_price_below_target_does_not_complete(self):
        self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0), {})
        completed = self.engine.track_price(101.5, self.now)
        self.assertEqual(len(completed), 0)
        self.assertEqual(self.engine.open_trade_count, 1)

    def test_price_above_stop_does_not_complete_buy(self):
        self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0), {})
        completed = self.engine.track_price(100.5, self.now)
        self.assertEqual(len(completed), 0)
        self.assertEqual(self.engine.open_trade_count, 1)

    def test_no_risk_distance_returns_none_pnl_r(self):
        res = _make_result("BUY", entry=100.0, stop=99.0, target=102.0)
        res.risk_distance = None
        self.engine.ingest_result(res, {})
        completed = self.engine.track_price(102.0, self.now)
        self.assertEqual(len(completed), 1)
        self.assertIsNone(completed[0].pnl_r)

    def test_expired_trade_not_reclassified_as_target_hit(self):
        """An expired trade must not be re-classified as TARGET_HIT by a later price."""
        res = _make_result("BUY", entry=100.0, stop=99.0, target=102.0)
        res.expires_at_utc = self.now  # expires at now
        self.engine.ingest_result(res, {})
        # First price update after expiry: trade should be EXPIRED, not TARGET_HIT,
        # even though price 102.0 would otherwise hit target.
        completed = self.engine.track_price(102.0, self.now)
        self.assertEqual(len(completed), 1)
        t = completed[0]
        self.assertEqual(t.state, "EXPIRED")
        self.assertEqual(t.exit_reason, "expired")
        self.assertEqual(t.exit_price, 102.0)
        self.assertEqual(self.engine.open_trade_count, 0)
        # Trade should not be re-classified on subsequent price updates
        completed_again = self.engine.track_price(105.0, self.now)
        self.assertEqual(len(completed_again), 0)

    def tearDown(self):
        self._tmpdir.cleanup()
        os.environ.pop("DELTA_SHADOW_LOG_FILE", None)
        os.environ.pop("DELTA_SHADOW_DECISION_LOG", None)
        reset_shadow_engine()


class TestShadowTradeExpiry(unittest.TestCase):
    """Shadow trade expiry when expires_ts is reached."""

    def setUp(self):
        reset_shadow_engine()
        self._tmpdir = tempfile.TemporaryDirectory()
        self._shadow_path = Path(self._tmpdir.name) / "shadow_trades.jsonl"
        self._decision_path = Path(self._tmpdir.name) / "shadow_decisions.jsonl"
        os.environ["DELTA_SHADOW_LOG_FILE"] = str(self._shadow_path)
        os.environ["DELTA_SHADOW_DECISION_LOG"] = str(self._decision_path)
        self.engine = ShadowTestEngine()
        self.expires = datetime(2026, 11, 3, 1, 5, 0, tzinfo=timezone.utc)
        self.now_before = datetime(2026, 11, 3, 1, 4, 59, tzinfo=timezone.utc)
        self.now_after = datetime(2026, 11, 3, 1, 5, 1, tzinfo=timezone.utc)

    def test_trade_not_expired_before_expiry(self):
        res = _make_result("BUY", entry=100.0, stop=99.0, target=102.0)
        res.expires_at_utc = self.expires
        self.engine.ingest_result(res, {})
        completed = self.engine.track_expiry(self.now_before)
        self.assertEqual(len(completed), 0)
        self.assertEqual(self.engine.open_trade_count, 1)

    def test_trade_expired_after_expiry(self):
        res = _make_result("BUY", entry=100.0, stop=99.0, target=102.0)
        res.expires_at_utc = self.expires
        self.engine.ingest_result(res, {})
        completed = self.engine.track_expiry(self.now_after)
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0].state, "EXPIRED")
        self.assertEqual(completed[0].exit_reason, "expired")
        # No price provided → no P&L valuation (pnl_r is None, not zero)
        self.assertIsNone(completed[0].exit_price)
        self.assertIsNone(completed[0].pnl_r)
        self.assertIsNone(completed[0].pnl_contracts)
        self.assertEqual(self.engine.open_trade_count, 0)

    def test_trade_without_expiry_never_expires(self):
        res = _make_result("BUY", entry=100.0, stop=99.0, target=102.0)
        res.expires_at_utc = None
        self.engine.ingest_result(res, {})
        completed = self.engine.track_expiry(datetime(2026, 12, 31, tzinfo=timezone.utc))
        self.assertEqual(len(completed), 0)
        self.assertEqual(self.engine.open_trade_count, 1)

    def tearDown(self):
        self._tmpdir.cleanup()
        os.environ.pop("DELTA_SHADOW_LOG_FILE", None)
        os.environ.pop("DELTA_SHADOW_DECISION_LOG", None)
        reset_shadow_engine()


class TestHoldoutCounter(unittest.TestCase):
    """Holdout counter counts only eligible completed trades."""

    def setUp(self):
        reset_shadow_engine()
        self._tmpdir = tempfile.TemporaryDirectory()
        self._shadow_path = Path(self._tmpdir.name) / "shadow_trades.jsonl"
        self._decision_path = Path(self._tmpdir.name) / "shadow_decisions.jsonl"
        os.environ["DELTA_SHADOW_LOG_FILE"] = str(self._shadow_path)
        os.environ["DELTA_SHADOW_DECISION_LOG"] = str(self._decision_path)
        self.engine = ShadowTestEngine()
        self.now = datetime(2026, 11, 3, 1, 10, 0, tzinfo=timezone.utc)

    def test_holdout_increments_on_completion(self):
        self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0), {})
        self.assertEqual(self.engine.completed_holdout_count, 0)
        self.engine.track_price(102.0, self.now)
        self.assertEqual(self.engine.completed_holdout_count, 1)

    def test_holdout_does_not_increment_for_open_trades(self):
        self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0), {})
        self.assertEqual(self.engine.completed_holdout_count, 0)

    def test_holdout_does_not_increment_for_wait_signals(self):
        self.engine.ingest_result(_make_result("WAIT"), {})
        self.assertEqual(self.engine.completed_holdout_count, 0)

    def test_holdout_threshold_not_met_at_99(self):
        for i in range(99):
            self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0, signal_id=f"s{i}"), {})
            self.engine.track_price(102.0, self.now)
        self.assertEqual(self.engine.completed_holdout_count, 99)
        self.assertFalse(self.engine.get_summary()["threshold_met"])

    def test_holdout_threshold_met_at_100(self):
        for i in range(100):
            self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0, signal_id=f"s{i}"), {})
            self.engine.track_price(102.0, self.now)
        self.assertEqual(self.engine.completed_holdout_count, 100)
        self.assertTrue(self.engine.get_summary()["threshold_met"])

    def test_holdout_count_excludes_open_trades(self):
        self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0, signal_id="open"), {})
        self.engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=105.0, signal_id="closed"), {})
        self.engine.track_price(102.0, self.now)  # completes only "open" trade at its target
        self.assertEqual(self.engine.open_trade_count, 1)  # "closed" still open
        self.assertEqual(self.engine.completed_holdout_count, 1)

    def tearDown(self):
        self._tmpdir.cleanup()
        os.environ.pop("DELTA_SHADOW_LOG_FILE", None)
        os.environ.pop("DELTA_SHADOW_DECISION_LOG", None)
        reset_shadow_engine()


class TestShadowJsonlPersistence(unittest.TestCase):
    """Shadow trade JSONL persistence and restart behavior."""

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.shadow_path = Path(self.tempdir.name) / "shadow_trades.jsonl"
        self.decision_path = Path(self.tempdir.name) / "shadow_decisions.jsonl"
        # Save original environment and set temp paths for test isolation
        self._orig_shadow_log = os.environ.get("DELTA_SHADOW_LOG_FILE")
        self._orig_decision_log = os.environ.get("DELTA_SHADOW_DECISION_LOG")
        os.environ["DELTA_SHADOW_LOG_FILE"] = str(self.shadow_path)
        os.environ["DELTA_SHADOW_DECISION_LOG"] = str(self.decision_path)

    def tearDown(self):
        self.tempdir.cleanup()
        reset_shadow_engine()
        # Restore original environment values
        if self._orig_shadow_log is not None:
            os.environ["DELTA_SHADOW_LOG_FILE"] = self._orig_shadow_log
        else:
            os.environ.pop("DELTA_SHADOW_LOG_FILE", None)
        if self._orig_decision_log is not None:
            os.environ["DELTA_SHADOW_DECISION_LOG"] = self._orig_decision_log
        else:
            os.environ.pop("DELTA_SHADOW_DECISION_LOG", None)

    def test_completed_trade_persisted_to_jsonl(self):
        reset_shadow_engine()
        engine = ShadowTestEngine()
        now = datetime(2026, 11, 3, 1, 10, 0, tzinfo=timezone.utc)
        engine.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0), {})
        completed = engine.track_price(102.0, now)
        for c in completed:
            engine._persist_shadow_trade(c)

        self.assertTrue(self.shadow_path.is_file())
        lines = [l for l in self.shadow_path.read_text(encoding="utf-8").splitlines() if l.strip()]
        # Two records: OPEN (created immediately) + COMPLETED (persisted on completion)
        self.assertEqual(len(lines), 2)
        # Last record should be the completed trade
        record = json.loads(lines[-1])
        self.assertEqual(record["state"], "TARGET_HIT")
        self.assertEqual(record["exit_price"], 102.0)
        self.assertEqual(record["pnl_r"], 2.0)

    def test_decision_log_persisted(self):
        os.environ["DELTA_SHADOW_DECISION_LOG"] = str(self.decision_path)
        reset_shadow_engine()
        engine = ShadowTestEngine()
        engine.ingest_result(_make_result("BUY"), {})
        engine.ingest_result(_make_result("WAIT"), {})
        self.assertTrue(self.decision_path.is_file())
        lines = [l for l in self.decision_path.read_text(encoding="utf-8").splitlines() if l.strip()]
        self.assertEqual(len(lines), 2)

    def test_restart_preserves_completed_count(self):
        os.environ["DELTA_SHADOW_LOG_FILE"] = str(self.shadow_path)
        reset_shadow_engine()
        engine1 = ShadowTestEngine()
        now = datetime(2026, 11, 3, 1, 10, 0, tzinfo=timezone.utc)
        engine1.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0, signal_id="s1"), {})
        completed = engine1.track_price(102.0, now)
        for c in completed:
            engine1._persist_shadow_trade(c)

        # Simulate restart: new engine reads same log file
        reset_shadow_engine()
        engine2 = ShadowTestEngine()
        # Completed trades are restored from persisted JSONL records
        self.assertEqual(engine2.completed_holdout_count, 1)
        # But the persisted file still has the record
        self.assertTrue(self.shadow_path.is_file())

    def test_get_summary_contains_threshold_fields(self):
        engine = ShadowTestEngine()
        summary = engine.get_summary()
        self.assertIn("completed_holdout_trades", summary)
        self.assertIn("threshold_required", summary)
        self.assertIn("threshold_met", summary)
        self.assertEqual(summary["threshold_required"], 100)
        self.assertFalse(summary["threshold_met"])


class TestShadowEngineSingleton(unittest.TestCase):
    """Module-level shadow engine singleton behavior."""

    def setUp(self):
        reset_shadow_engine()
        self._tmpdir = tempfile.TemporaryDirectory()
        self._shadow_path = Path(self._tmpdir.name) / "shadow_trades.jsonl"
        self._decision_path = Path(self._tmpdir.name) / "shadow_decisions.jsonl"
        os.environ["DELTA_SHADOW_LOG_FILE"] = str(self._shadow_path)
        os.environ["DELTA_SHADOW_DECISION_LOG"] = str(self._decision_path)

    def tearDown(self):
        reset_shadow_engine()
        os.environ.pop("DELTA_SHADOW_LOG_FILE", None)
        os.environ.pop("DELTA_SHADOW_DECISION_LOG", None)
        self._tmpdir.cleanup()

    def test_get_shadow_engine_returns_singleton(self):
        e1 = get_shadow_engine()
        e2 = get_shadow_engine()
        self.assertIs(e1, e2)

    def test_reset_clears_singleton(self):
        e1 = get_shadow_engine()
        reset_shadow_engine()
        e2 = get_shadow_engine()
        self.assertIsNot(e1, e2)


class TestExpiredTradeValuation(unittest.TestCase):
    """Expired trades must use last known price for consistent P&L."""

    def setUp(self):
        reset_shadow_engine()
        self._tmpdir = tempfile.TemporaryDirectory()
        self._shadow_path = Path(self._tmpdir.name) / "shadow_trades.jsonl"
        self._decision_path = Path(self._tmpdir.name) / "shadow_decisions.jsonl"
        os.environ["DELTA_SHADOW_LOG_FILE"] = str(self._shadow_path)
        os.environ["DELTA_SHADOW_DECISION_LOG"] = str(self._decision_path)
        self.engine = ShadowTestEngine()
        self.now = datetime(2026, 11, 3, 1, 10, 0, tzinfo=timezone.utc)
        self.expires = datetime(2026, 11, 3, 1, 5, 0, tzinfo=timezone.utc)
        self.now_after = datetime(2026, 11, 3, 1, 5, 1, tzinfo=timezone.utc)

    def tearDown(self):
        self._tmpdir.cleanup()
        os.environ.pop("DELTA_SHADOW_LOG_FILE", None)
        os.environ.pop("DELTA_SHADOW_DECISION_LOG", None)
        reset_shadow_engine()

    def test_expired_trade_with_price_computes_pnl(self):
        res = _make_result("BUY", entry=100.0, stop=99.0, target=102.0)
        res.expires_at_utc = self.expires
        self.engine.ingest_result(res, {})
        completed = self.engine.track_expiry(self.now_after, last_price=101.5)
        self.assertEqual(len(completed), 1)
        t = completed[0]
        self.assertEqual(t.state, "EXPIRED")
        self.assertEqual(t.exit_price, 101.5)
        self.assertEqual(t.exit_reason, "expired")
        # P&L = (101.5 - 100.0) / 1.0 = 1.5 R
        self.assertEqual(t.pnl_r, 1.5)
        self.assertEqual(t.pnl_contracts, 1.5)

    def test_expired_trade_sell_with_price_computes_pnl(self):
        res = _make_result("SELL", entry=100.0, stop=101.0, target=98.0)
        res.expires_at_utc = self.expires
        self.engine.ingest_result(res, {})
        completed = self.engine.track_expiry(self.now_after, last_price=99.0)
        self.assertEqual(len(completed), 1)
        t = completed[0]
        self.assertEqual(t.state, "EXPIRED")
        self.assertEqual(t.exit_price, 99.0)
        # P&L = (100.0 - 99.0) / 1.0 = 1.0 R
        self.assertEqual(t.pnl_r, 1.0)

    def test_expired_trade_without_price_has_no_pnl(self):
        res = _make_result("BUY", entry=100.0, stop=99.0, target=102.0)
        res.expires_at_utc = self.expires
        self.engine.ingest_result(res, {})
        completed = self.engine.track_expiry(self.now_after)
        self.assertEqual(len(completed), 1)
        t = completed[0]
        self.assertIsNone(t.exit_price)
        self.assertIsNone(t.pnl_r)
        self.assertIsNone(t.pnl_contracts)


class TestOpenTradeRestoration(unittest.TestCase):
    """Open trades must be restored from JSONL on restart."""

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.shadow_path = Path(self.tempdir.name) / "shadow_trades.jsonl"
        self.decision_path = Path(self.tempdir.name) / "shadow_decisions.jsonl"
        # Save original environment and set temp paths for test isolation
        self._orig_shadow_log = os.environ.get("DELTA_SHADOW_LOG_FILE")
        self._orig_decision_log = os.environ.get("DELTA_SHADOW_DECISION_LOG")
        os.environ["DELTA_SHADOW_LOG_FILE"] = str(self.shadow_path)
        os.environ["DELTA_SHADOW_DECISION_LOG"] = str(self.decision_path)

    def tearDown(self):
        self.tempdir.cleanup()
        reset_shadow_engine()
        # Restore original environment values
        if self._orig_shadow_log is not None:
            os.environ["DELTA_SHADOW_LOG_FILE"] = self._orig_shadow_log
        else:
            os.environ.pop("DELTA_SHADOW_LOG_FILE", None)
        if self._orig_decision_log is not None:
            os.environ["DELTA_SHADOW_DECISION_LOG"] = self._orig_decision_log
        else:
            os.environ.pop("DELTA_SHADOW_DECISION_LOG", None)

    def test_open_trade_restored_on_restart(self):
        reset_shadow_engine()
        engine1 = ShadowTestEngine()
        engine1.ingest_result(_make_result("BUY", entry=100.0, stop=99.0, target=102.0, signal_id="open_sig"), {})
        # Trade is still OPEN - persist it manually to simulate crash mid-life
        open_trade = engine1.get_open_trades()[0]
        record = asdict(open_trade)
        record["logged_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        from shadow_test import ShadowTestEngine as STE
        STE._append_jsonl(str(self.shadow_path), record)

        # Simulate restart
        reset_shadow_engine()
        engine2 = ShadowTestEngine()
        self.assertEqual(engine2.open_trade_count, 1)
        restored = engine2.get_open_trades()[0]
        self.assertEqual(restored.signal_id, "open_sig")
        self.assertEqual(restored.state, "OPEN")
        self.assertEqual(restored.entry, 100.0)
        self.assertEqual(restored.stop_loss, 99.0)
        self.assertEqual(restored.take_profit, 102.0)


if __name__ == "__main__":
    unittest.main()
