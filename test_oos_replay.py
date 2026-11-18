"""test_oos_replay.py - deterministic tests for the out-of-sample replay harness.

All fixtures are synthetic. No live data is fetched, no order is placed, and no
result here is a statement about real-market profitability.
"""
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import oos_replay as oos
import signal_engine as se

T0 = datetime(2026, 10, 15, 0, 0, tzinfo=timezone.utc)


def bar(ts, o, h, l, cl, v=10.0):
    return oos.Bar1m(ts_utc=ts, open=float(o), high=float(h), low=float(l),
                     close=float(cl), volume=float(v))


def ts_of(minutes):
    return T0 + timedelta(minutes=minutes)


def synth_1m(n=1400, start=1090.0, drift=0.05):
    """Long clean 1m series: a slow uptrend with a small oscillation."""
    out = []
    for i in range(n):
        base = start + drift * i
        wobble = 2.0 * ((-1) ** (i % 2))
        out.append(bar(ts_of(i), base, base + 1.5 + abs(wobble) * 0.2,
                       base - 1.5 - abs(wobble) * 0.2, base + wobble * 0.3))
    return out


class TestChronologyAndValidation(unittest.TestCase):
    def test_naive_timestamp_rejected(self):
        with self.assertRaises(oos.ReplayDataError):
            oos._parse_ts("2026-10-15T00:00:00")

    def test_offset_timestamp_converted_to_utc(self):
        got = oos._parse_ts("2026-10-15T05:30:00+05:30")
        self.assertEqual(got, T0)

    def test_garbage_timestamp_rejected(self):
        with self.assertRaises(oos.ReplayDataError):
            oos._parse_ts("not-a-time")

    def test_out_of_order_bars_rejected(self):
        bars = [bar(ts_of(0), 1, 2, 0.5, 1.5), bar(ts_of(0), 1, 2, 0.5, 1.5)]
        with self.assertRaises(oos.ReplayDataError):
            oos.validate_chronology(bars)

    def test_gap_detection(self):
        bars = [bar(ts_of(0), 1, 2, 0.5, 1.5),
                bar(ts_of(1), 1, 2, 0.5, 1.5),
                bar(ts_of(5), 1, 2, 0.5, 1.5)]
        notes = oos.validate_chronology(bars)
        self.assertEqual(len(notes), 1)
        self.assertIn("240s", notes[0])

    def test_invalid_ohlc_rejected(self):
        with self.assertRaises(oos.ReplayDataError):
            bar(ts_of(0), 1.0, 1.2, 2.0, 1.1)
        with self.assertRaises(oos.ReplayDataError):
            bar(ts_of(0), -1.0, 2.0, 0.5, 1.5)


class TestCandleAggregation(unittest.TestCase):
    def test_bucket_boundaries_and_values(self):
        bars = [bar(ts_of(i), 100 + i, 101 + i, 99 + i, 100.5 + i) for i in range(10)]
        agg = oos.aggregate_candles(bars, 5)
        self.assertEqual(len(agg), 2)
        self.assertEqual(agg[0].bucket_ts, T0)
        self.assertEqual(agg[0].close_ts, T0 + timedelta(minutes=5))
        self.assertEqual(agg[0].open, 100.0)
        self.assertEqual(agg[0].high, 105.0)
        self.assertEqual(agg[0].low, 99.0)
        self.assertEqual(agg[0].close, 104.5)
        self.assertEqual(agg[0].volume, 50.0)
        self.assertTrue(agg[0].complete)

    def test_incomplete_bucket_marked_and_dropped(self):
        bars = [bar(ts_of(i), 100 + i, 101 + i, 99 + i, 100.5 + i) for i in range(7)]
        agg = oos.aggregate_candles(bars, 5)
        self.assertFalse(agg[-1].complete)
        candles = oos.to_candles(agg)
        self.assertEqual(len(candles), 1)  # incomplete bucket is dropped

    def test_as_of_does_not_leak_future_bars(self):
        bars = [bar(ts_of(i), 100 + i, 101 + i, 99 + i, 100.5 + i) for i in range(20)]
        now = T0 + timedelta(minutes=9)
        agg = oos.aggregate_candles(bars, 5, as_of=now)
        self.assertTrue(all(b.close_ts <= now for b in agg))
        self.assertTrue(all(not any(x.ts_utc > now for x in
                                   [bar(b.bucket_ts, b.open, b.high, b.low, b.close)])
                            for b in agg))


class TestLookAheadPrevention(unittest.TestCase):
    def test_snapshot_excludes_future_data(self):
        bars = synth_1m(1000)
        runner = oos.ReplayRunner(bars, [], config={"min_history_1m": 900})
        now = T0 + timedelta(minutes=1000)
        snap = runner.snapshot_at(now)
        self.assertLessEqual(snap.candles_1m[-1].ts_utc, now)
        self.assertLessEqual(snap.candles_5m[-1].ts_utc, now)
        self.assertLessEqual(snap.candles_15m[-1].ts_utc, now)

    def test_later_snapshot_superset_never_backdated(self):
        bars = synth_1m(1000)
        runner = oos.ReplayRunner(bars, [], config={"min_history_1m": 900})
        early = runner.snapshot_at(T0 + timedelta(minutes=905))
        late = runner.snapshot_at(T0 + timedelta(minutes=950))
        self.assertLess(early.candles_1m[-1].ts_utc, late.candles_1m[-1].ts_utc)
        self.assertLess(early.candles_15m[-1].ts_utc, late.candles_15m[-1].ts_utc)
        self.assertTrue(all(x in [c.ts_utc for c in late.candles_1m]
                            for x in [c.ts_utc for c in early.candles_1m]))

    def test_no_decision_before_minimum_history(self):
        bars = synth_1m(1000)
        runner = oos.ReplayRunner(bars, [], config={"min_history_1m": 900})
        firsts = [now for now, _ in runner.iter_snapshots()]
        self.assertTrue(firsts)
        self.assertGreaterEqual(firsts[0], T0 + timedelta(minutes=900))

    def test_gap_window_skips_decision(self):
        bars = synth_1m(1000)
        # remove ten consecutive 1m bars in the middle of the history window
        for i in range(400, 410):
            del bars[400]
        runner = oos.ReplayRunner(bars, [], config={"min_history_1m": 900})
        snaps = list(runner.iter_snapshots())
        self.assertGreater(runner.stats.skipped_gap_windows, 0)
        self.assertLess(len(snaps), len(bars))

    def test_aggregated_buckets_are_a_subset_of_visible_1m(self):
        bars = synth_1m(1000)
        runner = oos.ReplayRunner(bars, [], config={"min_history_1m": 900})
        for now, snap in list(runner.iter_snapshots())[:3]:
            highs_1m = {b.high for b in bars if b.ts_utc <= now}
            for c in snap.candles_5m:
                self.assertLessEqual(c.high, max(highs_1m))


class TestSignalTimestampsAndLifecycle(unittest.TestCase):
    def test_decision_ts_is_the_bar_ts(self):
        bars = synth_1m(1200)
        trades = [oos.Print1m(ts_utc=T0 + timedelta(minutes=1000 + i), price=1200.0,
                              size=50.0, side_aggressor=se.AGGRESSOR_BUY)
                  for i in range(20)]
        runner = oos.ReplayRunner(bars, trades, config={"min_history_1m": 900})
        seen = {}
        for now, snap in runner.iter_snapshots():
            engine = se.SignalEngine(runner.engine_cfg)
            res = engine.evaluate(snap, now_utc=now)
            seen[now] = res
        # every evaluated snapshot timestamp is a recorded bar timestamp
        self.assertTrue(all(now in set(b.ts_utc for b in bars) for now in seen))
        # no result may reference a candle later than the decision bar
        for now, res in seen.items():
            for key in ("1m", "5m", "15m"):
                for c in res.input_candle_timestamps[key]:
                    self.assertLessEqual(c, now)

    def test_determinism_of_replay(self):
        bars = synth_1m(1200)
        a = oos.run_replay(bars, [], config={"min_history_1m": 900})
        b = oos.run_replay(bars, [], config={"min_history_1m": 900})
        self.assertEqual(a["signals"], b["signals"])
        self.assertEqual(a["metrics"], b["metrics"])
        self.assertEqual(a["params_version"], "oos-1.0")


class TestConfigFreezeAndPolicy(unittest.TestCase):
    def test_unknown_config_key_rejected(self):
        bars = synth_1m(1000)
        with self.assertRaises(oos.ReplayDataError):
            oos.ReplayRunner(bars, [], config={"spread_bps_typo": 1.0})

    def test_checksum_unavailable_is_truthful_and_opted_out(self):
        bars = synth_1m(1000)
        runner = oos.ReplayRunner(bars, [], config={"min_history_1m": 900})
        snap = runner.snapshot_at(T0 + timedelta(minutes=1000))
        self.assertIs(snap.checksum_state, se.ChecksumState.UNAVAILABLE)
        self.assertFalse(runner.engine_cfg["require_checksum_verified"])
        self.assertIn("require_checksum_verified", oos.FROZEN_ENGINE_OVERRIDES)

    def test_frozen_config_file_matches_module(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "oos_config_frozen.json")
        self.assertTrue(os.path.isfile(path), "tracked frozen OOS config fixture is required")
        import json
        with open(path, encoding="utf-8") as fh:
            frozen = json.load(fh)

        expected_keys = set(oos.FROZEN_OOS_CONFIG) | {"engine_overrides"}
        self.assertEqual(set(frozen), expected_keys,
                         "frozen config fixture must contain every expected key and no extras")
        frozen_oos_config = {key: value for key, value in frozen.items()
                             if key != "engine_overrides"}
        self.assertEqual(frozen_oos_config, oos.FROZEN_OOS_CONFIG,
                         "frozen OOS config drift or missing configuration key")
        self.assertEqual(frozen["engine_overrides"], oos.FROZEN_ENGINE_OVERRIDES)

    def test_report_declares_policy_relaxation(self):
        bars = synth_1m(1000)
        out = oos.run_replay(bars, [], config={"min_history_1m": 900})
        self.assertIn("policy_relaxation", out)
        self.assertIn("require_checksum_verified", out["policy_relaxation"])


class TestSimulationAndCosts(unittest.TestCase):
    def _signal(self):
        snap_bars = synth_1m(1000)
        runner = oos.ReplayRunner(snap_bars, [], config={"min_history_1m": 900})
        now = T0 + timedelta(minutes=1000)
        snap = runner.snapshot_at(now)
        return runner, snap, now

    def test_costs_reduce_net_r(self):
        bars = synth_1m(1200)
        entry_bar = bars[1000]
        entry = entry_bar.open
        result = se.SignalResult(
            signal=se.Signal.BUY, state=se.SignalState.BUY_CONFIRMED,
            signal_id="x", proposed_entry=entry, stop_loss=entry - 10.0, take_profit=entry + 30.0,
            invalidation_price=entry - 5.0, risk_distance=10.0, reward_distance=30.0,
            risk_reward_ratio=3.0, setup_timeframe="5m",
            setup_candle_timestamp=T0, expires_at_utc=T0 + timedelta(minutes=1005))
        runner = oos.ReplayRunner(bars, [], config={"min_history_1m": 900})
        sim = runner.simulate(result, entry_bar)
        self.assertIsNotNone(sim)
        self.assertGreater(sim["gross_r"], sim["net_r"])
        self.assertGreater(sim["costs_r"], 0)

    def test_metrics_empty_input(self):
        self.assertEqual(oos.compute_metrics([])["trade_count"], 0)

    def test_metrics_computation(self):
        trades = [
            {"net_r": 2.0, "outcome": "target", "direction": "BUY"},
            {"net_r": -1.0, "outcome": "stop", "direction": "BUY"},
            {"net_r": 0.0, "outcome": "timeout", "direction": "SELL"},
        ]
        m = oos.compute_metrics(trades)
        self.assertEqual(m["trade_count"], 3)
        self.assertEqual(m["wins"], 1)
        self.assertAlmostEqual(m["win_rate"], 1 / 3)
        self.assertAlmostEqual(m["expectancy_r"], 1.0 / 3)
        self.assertAlmostEqual(m["profit_factor"], 2.0)
        self.assertEqual(m["by_outcome"]["timeout"], 1)
        self.assertEqual(m["by_direction"]["SELL"], 1)

    def test_stop_first_convention_is_conservative(self):
        bars = [bar(ts_of(1000 + i), 100.0, 130.0, 90.0, 100.0) for i in range(4)]
        result = se.SignalResult(
            signal=se.Signal.BUY, state=se.SignalState.BUY_CONFIRMED,
            signal_id="y", proposed_entry=100.0, stop_loss=90.0, take_profit=130.0,
            invalidation_price=95.0, risk_distance=10.0, reward_distance=30.0,
            risk_reward_ratio=3.0, setup_timeframe="5m", setup_candle_timestamp=T0,
            expires_at_utc=T0 + timedelta(minutes=1005))
        runner = oos.ReplayRunner(bars, [], config={"min_history_1m": 900})
        sim = runner.simulate(result, bars[0])
        self.assertEqual(sim["outcome"], "stop")  # conservative default

    def test_invalid_cost_config_rejected(self):
        with self.assertRaises(oos.ReplayDataError):
            oos.ReplayRunner(synth_1m(1000), [], config={"slippage_bps": -1.0})


class TestSimulationDispositions(unittest.TestCase):
    def _result(self, **overrides):
        values = dict(
            signal=se.Signal.BUY, state=se.SignalState.BUY_CONFIRMED,
            signal_id="expiry-test", generated_at_utc=ts_of(999),
            proposed_entry=100.0, stop_loss=90.0, take_profit=130.0,
            invalidation_price=95.0, risk_distance=10.0, reward_distance=30.0,
            risk_reward_ratio=3.0, setup_timeframe="5m",
            setup_candle_timestamp=ts_of(990), expires_at_utc=ts_of(1005),
        )
        values.update(overrides)
        return se.SignalResult(**values)

    def _bars(self, start=1000, end=1004):
        return [bar(ts_of(i), 100.0, 102.0, 98.0, 100.0) for i in range(start, end + 1)]

    def _runner(self, bars):
        return oos.ReplayRunner(bars, [], config={"min_history_1m": 1})

    def test_simulation_entry_before_expiry_is_eligible(self):
        bars = self._bars()
        runner = self._runner(bars)
        disposition = runner.simulate_with_disposition(
            self._result(), ts_of(1000), bars[0])

        self.assertEqual(disposition.status, "SIMULATED")
        self.assertIsNone(disposition.reason_code)
        self.assertIsNotNone(disposition.trade)
        self.assertEqual(disposition.trade["outcome"], "timeout")

    def test_simulation_entry_exactly_at_expiry_is_rejected(self):
        bars = self._bars(start=1005, end=1005)
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(), ts_of(1005), bars[0])

        self.assertEqual(disposition.status, "REJECTED")
        self.assertEqual(disposition.reason_code, "REJECTED_EXPIRED_AT_ENTRY")
        self.assertEqual(disposition.rejection_ts_utc, ts_of(1005).isoformat())
        self.assertIsNone(disposition.trade)

    def test_simulation_entry_after_expiry_is_rejected(self):
        bars = self._bars(start=1006, end=1006)
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(), ts_of(1006), bars[0])

        self.assertEqual(disposition.status, "REJECTED")
        self.assertEqual(disposition.reason_code, "REJECTED_EXPIRED_AT_ENTRY")
        self.assertEqual(disposition.rejection_ts_utc, ts_of(1006).isoformat())

    def test_expired_entry_takes_precedence_over_missing_entry_bar(self):
        disposition = self._runner([]).simulate_with_disposition(
            self._result(), ts_of(1005), None)

        self.assertEqual(disposition.reason_code, "REJECTED_EXPIRED_AT_ENTRY")
        self.assertEqual(disposition.rejection_ts_utc, ts_of(1005).isoformat())

    def test_non_minute_aligned_expiry_is_rejected(self):
        expiry = ts_of(1005) + timedelta(seconds=30)
        bars = self._bars()
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(expires_at_utc=expiry), ts_of(1000), bars[0])

        self.assertEqual(disposition.reason_code, "REJECTED_EXPIRY_NOT_BAR_ALIGNED")
        self.assertEqual(disposition.rejection_ts_utc, expiry.isoformat())

    def test_missing_entry_bar_within_valid_window_is_rejected(self):
        disposition = self._runner([]).simulate_with_disposition(
            self._result(), ts_of(1000), None)

        self.assertEqual(disposition.reason_code, "REJECTED_MISSING_ENTRY_BAR")
        self.assertEqual(disposition.rejection_ts_utc, ts_of(1000).isoformat())

    def test_missing_or_wrong_side_levels_are_invalid_levels(self):
        bars = self._bars()
        runner = self._runner(bars)
        invalid_results = (
            self._result(stop_loss=None),
            self._result(take_profit=None),
            self._result(stop_loss=101.0),
            self._result(take_profit=99.0),
            self._result(proposed_entry=float("nan")),
        )
        for result in invalid_results:
            with self.subTest(result=result):
                disposition = runner.simulate_with_disposition(
                    result, ts_of(1000), bars[0])
                self.assertEqual(disposition.reason_code, "REJECTED_INVALID_LEVELS")
                self.assertEqual(disposition.rejection_ts_utc, ts_of(1000).isoformat())

    def test_directionally_invalid_supplied_invalidation_level_is_rejected(self):
        bars = self._bars()
        runner = self._runner(bars)
        invalid_results = (
            self._result(invalidation_price=105.0),
            self._result(signal=se.Signal.SELL, proposed_entry=100.0,
                         stop_loss=110.0, take_profit=80.0,
                         invalidation_price=95.0),
        )
        for result in invalid_results:
            with self.subTest(signal=result.signal, invalidation=result.invalidation_price):
                disposition = runner.simulate_with_disposition(
                    result, ts_of(1000), bars[0])
                self.assertEqual(disposition.reason_code, "REJECTED_INVALID_LEVELS")
                self.assertEqual(disposition.rejection_ts_utc, ts_of(1000).isoformat())

    def test_entry_gap_through_stop_or_target_has_distinct_reason(self):
        bars = [bar(ts_of(1000), 120.0, 122.0, 118.0, 120.0)]
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(stop_loss=90.0, take_profit=110.0), ts_of(1000), bars[0])

        self.assertEqual(disposition.reason_code, "REJECTED_ENTRY_GAP_THROUGH_LEVEL")
        self.assertEqual(disposition.rejection_ts_utc, ts_of(1000).isoformat())

    def test_bar_gap_inside_path_is_path_gap_not_incomplete_history(self):
        bars = [bar(ts_of(i), 100.0, 102.0, 98.0, 100.0)
                for i in (1000, 1001, 1003, 1004)]
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(), ts_of(1000), bars[0])

        self.assertEqual(disposition.reason_code, "REJECTED_PATH_GAP")
        self.assertEqual(disposition.rejection_ts_utc, ts_of(1002).isoformat())

    def test_gap_before_expiry_is_path_gap_when_next_observed_bar_is_expiry_bar(self):
        expiry = ts_of(1005)
        bars = [bar(ts_of(1000), 100.0, 102.0, 98.0, 100.0),
                bar(expiry, 100.0, 102.0, 98.0, 100.0)]
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(expires_at_utc=expiry), ts_of(1000), bars[0])

        self.assertEqual(disposition.reason_code, "REJECTED_PATH_GAP")
        self.assertEqual(disposition.rejection_ts_utc, ts_of(1001).isoformat())

    def test_contiguous_history_ending_before_expiry_is_incomplete_history(self):
        bars = self._bars(start=1000, end=1002)
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(), ts_of(1000), bars[0])

        self.assertEqual(disposition.reason_code, "REJECTED_INCOMPLETE_HISTORY_BEFORE_EXPIRY")
        self.assertEqual(disposition.rejection_ts_utc, ts_of(1003).isoformat())

    def test_timeout_requires_complete_last_candle_through_expiry(self):
        bars = self._bars(start=1000, end=1003)
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(), ts_of(1000), bars[0])

        self.assertEqual(disposition.reason_code, "REJECTED_INCOMPLETE_HISTORY_BEFORE_EXPIRY")

    def test_timeout_final_eligible_candle_closes_exactly_at_expiry(self):
        expiry = ts_of(1005)
        bars = self._bars(start=1000, end=1004)
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(expires_at_utc=expiry), ts_of(1000), bars[0])

        self.assertEqual(disposition.status, "SIMULATED")
        self.assertEqual(disposition.trade["outcome"], "timeout")
        self.assertEqual(disposition.trade["exit_bar_open_ts_utc"], ts_of(1004).isoformat())
        self.assertEqual(disposition.trade["exit_ts_utc"], expiry.isoformat())

    def test_candle_opening_at_expiry_is_excluded(self):
        expiry = ts_of(1005)
        bars = self._bars(start=1000, end=1004)
        bars.append(bar(expiry, 100.0, 140.0, 80.0, 100.0))
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(expires_at_utc=expiry), ts_of(1000), bars[0])

        self.assertEqual(disposition.status, "SIMULATED")
        self.assertEqual(disposition.trade["outcome"], "timeout")

    def test_timeout_exit_timestamp_is_expiry_boundary(self):
        expiry = ts_of(1005)
        bars = self._bars()
        disposition = self._runner(bars).simulate_with_disposition(
            self._result(expires_at_utc=expiry), ts_of(1000), bars[0])

        self.assertEqual(disposition.trade["exit_ts_utc"], expiry.isoformat())
        self.assertEqual(disposition.trade["exit_bar_open_ts_utc"], ts_of(1004).isoformat())


class TestDataLoading(unittest.TestCase):
    def test_csv_roundtrip_and_validation(self):
        bars = synth_1m(20)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "bars.csv")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("ts_utc,open,high,low,close,volume\n")
                for b in bars:
                    fh.write(f"{b.ts_utc.isoformat()},{b.open},{b.high},"
                             f"{b.low},{b.close},{b.volume}\n")
            loaded = oos.load_bars_csv(path)
            self.assertEqual(loaded, bars)

            bad = os.path.join(tmp, "bad.csv")
            with open(bad, "w", encoding="utf-8") as fh:
                fh.write("ts_utc,open,high,low,close\n")
                fh.write("2026-10-15T00:00:00Z,1,2,0.5,1.5\n")
            with self.assertRaises(oos.ReplayDataError):
                oos.load_bars_csv(bad)

    def test_json_loading(self):
        bars = synth_1m(10)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "bars.json")
            payload = {"candles": [{
                "ts_utc": b.ts_utc.isoformat(), "open": b.open, "high": b.high,
                "low": b.low, "close": b.close, "volume": b.volume} for b in bars]}
            with open(path, "w", encoding="utf-8") as fh:
                import json
                json.dump(payload, fh)
            self.assertEqual(oos.load_bars_json(path), bars)


class TestReplayAuditRecords(unittest.TestCase):
    def _synthetic_replay(self):
        bars, trades, first, _ = _synthetic_series("bullish")
        ask_liq = [se.LiquidityLevel(price=1200.0, contracts=250.0, side="ask",
                                     observed_ts_utc=first - timedelta(minutes=10))]
        return oos.run_replay(bars, trades, config={"min_history_1m": 900},
                              ask_liquidity=ask_liq)

    def test_every_confirmed_signal_has_exactly_one_simulation_disposition(self):
        replay = self._synthetic_replay()

        self.assertEqual(len(replay["signals"]), replay["stats"]["signals_confirmed"])
        for record in replay["signals"]:
            self.assertIn(record["simulation_status"], ("SIMULATED", "REJECTED"))
            self.assertTrue(record["audit_event_key"])

    def test_simulation_rejection_is_retained_on_signal_record(self):
        from unittest.mock import patch
        rejected = oos.SimulationDisposition(
            "REJECTED", "REJECTED_EXPIRED_AT_ENTRY", ts_of(1000).isoformat())
        with patch.object(oos.ReplayRunner, "simulate_with_disposition", return_value=rejected):
            replay = self._synthetic_replay()

        self.assertTrue(replay["signals"])
        self.assertEqual(replay["signals"][0]["simulation_status"], "REJECTED")
        self.assertEqual(replay["signals"][0]["simulation_rejection_reason"],
                         "REJECTED_EXPIRED_AT_ENTRY")
        self.assertEqual(replay["signals"][0]["rejection_ts_utc"], ts_of(1000).isoformat())
        self.assertEqual(replay["stats"]["signals_rejected"], replay["stats"]["signals_confirmed"])

    def test_replay_stats_reconcile_confirmed_simulated_and_rejected_counts(self):
        replay = self._synthetic_replay()
        stats = replay["stats"]

        self.assertEqual(len(replay["signals"]), stats["signals_confirmed"])
        self.assertEqual(stats["signals_simulated"] + stats["signals_rejected"],
                         stats["signals_confirmed"])
        self.assertEqual(len(replay["simulated_trades"]), stats["signals_simulated"])

    def test_same_signal_id_at_different_decision_times_has_unique_audit_keys(self):
        first = oos.signal_audit_key("same-setup", ts_of(1000))
        second = oos.signal_audit_key("same-setup", ts_of(1001))

        self.assertNotEqual(first, second)
        self.assertEqual(first, f"same-setup|{ts_of(1000).isoformat()}")

    def _replay_with_one_expired_preconfirmation_candidate(self):
        from unittest.mock import patch
        bars, trades, first, _ = _synthetic_series("bullish")
        original_evaluate = se.SignalEngine.evaluate
        injected = {"done": False}

        def evaluate_with_one_expired(self, snap, now_utc=None):
            now = now_utc or snap.ts_utc
            if not injected["done"]:
                injected["done"] = True
                trigger = now - timedelta(minutes=4)
                return se.SignalResult(
                    signal=se.Signal.WAIT, state=se.SignalState.WAIT,
                    signal_id="expired-preconfirmation-001", generated_at_utc=now,
                    trigger_ts_utc=trigger,
                    last_completed_candle_ts_utc=snap.candles_1m[-1].ts_utc,
                    setup_candle_timestamp=now - timedelta(minutes=30),
                    expires_at_utc=trigger + timedelta(minutes=3),
                    reason_codes=[se.WAIT_SETUP_EXPIRED],
                )
            return original_evaluate(self, snap, now_utc=now)

        with patch.object(se.SignalEngine, "evaluate", autospec=True,
                          side_effect=evaluate_with_one_expired):
            return oos.run_replay(bars, trades, config={"min_history_1m": 900})

    def test_expired_preconfirmation_candidates_are_visible_separately(self):
        replay = self._replay_with_one_expired_preconfirmation_candidate()

        self.assertEqual(len(replay["expired_candidates"]), 1)
        candidate = replay["expired_candidates"][0]
        self.assertEqual(candidate["signal_id"], "expired-preconfirmation-001")
        self.assertEqual(candidate["reason_code"], se.WAIT_SETUP_EXPIRED)

    def test_expired_candidates_preserve_all_four_lifecycle_timestamps(self):
        replay = self._replay_with_one_expired_preconfirmation_candidate()
        candidate = replay["expired_candidates"][0]

        for field in ("trigger_ts_utc", "last_completed_candle_ts_utc",
                      "setup_candle_timestamp", "expires_at_utc"):
            self.assertTrue(candidate[field], field)

    def test_expired_candidates_retain_deterministic_signal_id_and_decision_timestamp(self):
        replay = self._replay_with_one_expired_preconfirmation_candidate()
        candidate = replay["expired_candidates"][0]

        self.assertEqual(candidate["signal_id"], "expired-preconfirmation-001")
        self.assertTrue(candidate["decision_ts_utc"])
        self.assertEqual(candidate["audit_event_key"],
                         oos.signal_audit_key(candidate["signal_id"],
                                              oos._parse_ts(candidate["decision_ts_utc"])))

    def test_expired_candidate_missing_audit_metadata_fails_closed(self):
        from unittest.mock import patch
        bars, trades, _, _ = _synthetic_series("bullish")

        def missing_metadata(self, snap, now_utc=None):
            now = now_utc or snap.ts_utc
            return se.SignalResult(
                signal=se.Signal.WAIT, state=se.SignalState.WAIT,
                generated_at_utc=now, reason_codes=[se.WAIT_SETUP_EXPIRED],
            )

        with patch.object(se.SignalEngine, "evaluate", autospec=True,
                          side_effect=missing_metadata):
            with self.assertRaisesRegex(oos.ReplayDataError, "expired candidate missing audit metadata"):
                oos.run_replay(bars, trades, config={"min_history_1m": 900})

    def test_expired_candidates_are_not_counted_as_confirmed_signals(self):
        replay = self._replay_with_one_expired_preconfirmation_candidate()

        self.assertEqual(replay["stats"]["signals_confirmed"], 0)
        self.assertEqual(replay["signals"], [])
        self.assertEqual(len(replay["expired_candidates"]), 1)


class TestSignalLifecycleAuditRegression(unittest.TestCase):
    def _snapshots(self, liquidity_observed_at):
        import test_signal_engine as S

        first = S.STRUCT_TS + timedelta(minutes=5)
        closes = [1098.0] + [1090.0] * 6 + [1100.0]
        snapshots = []
        for decision_offset in range(1, len(closes) + 1):
            now = first + timedelta(minutes=decision_offset)
            visible_closes = closes[:decision_offset]
            bars = []
            previous_close = 1096.0
            for index, close in enumerate(visible_closes):
                open_price = previous_close
                bars.append(S.c(
                    first + timedelta(minutes=index), open_price,
                    max(open_price, close) + 1.0,
                    min(open_price, close) - 1.0, close,
                ))
                previous_close = close

            ask_liquidity = []
            if liquidity_observed_at <= now:
                ask_liquidity = [se.LiquidityLevel(
                    price=1200.0, contracts=250.0, side="ask",
                    observed_ts_utc=liquidity_observed_at,
                )]
            snap = S.base_snapshot(
                "bullish", ask_liq=ask_liquidity,
                trades_end=now - timedelta(seconds=1),
            )
            object.__setattr__(snap, "candles_1m", bars)
            object.__setattr__(snap, "ts_utc", now)
            object.__setattr__(snap, "last_price", bars[-1].close)
            object.__setattr__(snap, "bid", bars[-1].close - 0.5)
            object.__setattr__(snap, "ask", bars[-1].close + 0.5)
            object.__setattr__(snap, "recent_trades", S.build_trades(
                now - timedelta(seconds=1), "bullish"))
            snapshots.append((now, snap))
        return snapshots

    def _run_engine_replay(self, liquidity_observed_at):
        from unittest.mock import patch

        snapshots = self._snapshots(liquidity_observed_at)
        with patch.object(
            oos.ReplayRunner, "iter_snapshots", autospec=True,
            side_effect=lambda runner: iter(snapshots),
        ):
            return oos.run_replay([], [], config={"min_history_1m": 1})

    def test_expired_candidate_then_later_cross_does_not_confirm_same_structure(self):
        first = __import__("test_signal_engine").STRUCT_TS + timedelta(minutes=5)
        replay = self._run_engine_replay(first + timedelta(minutes=8))

        self.assertTrue(replay["expired_candidates"])
        self.assertEqual(replay["signals"], [])

    def test_confirmed_signal_then_expiry_does_not_reconfirm_same_structure(self):
        first = __import__("test_signal_engine").STRUCT_TS + timedelta(minutes=5)
        replay = self._run_engine_replay(first)

        self.assertEqual(len(replay["signals"]), 1)
        confirmed_id = replay["signals"][0]["signal_id"]
        self.assertFalse(any(
            item["signal_id"] == confirmed_id
            for item in replay["expired_candidates"]
        ))


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# End-to-end synthetic replay (synthetic data only - no market claim)
# ---------------------------------------------------------------------------

def _mirror_bar(b):
    return bar(b.ts_utc, 2200.0 - b.open, 2200.0 - b.low, 2200.0 - b.high, 2200.0 - b.close)


def _mirror_trade(t):
    return oos.Print1m(ts_utc=t.ts_utc, price=2200.0 - t.price, size=t.size,
                       side_aggressor=(se.AGGRESSOR_SELL if t.side_aggressor == se.AGGRESSOR_BUY
                                       else se.AGGRESSOR_BUY))


def _synthetic_series(direction="bullish"):
    """Build the whole synthetic series, then mirror it for the bearish variant.

    Trend history + the module's synthetic 5m setup + 3 trigger minutes. Mirrored
    bar-for-bar so the bearish case is the exact structural mirror.
    """
    import test_signal_engine as S
    bars = [bar(ts_of(i), 950 + 0.155 * i, 950 + 0.155 * i + 1.0,
                950 + 0.155 * i - 1.0, 950 + 0.155 * i) for i in range(900)]
    idx = 900
    for cd in S.build_5m_bullish():
        for _k in range(5):
            bars.append(bar(ts_of(idx), cd.open, cd.high, cd.low, cd.close))
            idx += 1
    first = ts_of(idx)
    for k, (o, h, l, cl) in enumerate([(1096.0, 1098.5, 1094.0, 1098.0),
                                       (1098.0, 1100.0, 1096.5, 1099.5),
                                       (1099.5, 1101.0, 1097.0, 1100.0)]):
        bars.append(bar(first + timedelta(minutes=k), o, h, l, cl))
    trades = [oos.Print1m(ts_utc=first - timedelta(minutes=4) + timedelta(seconds=18 * i),
                          price=1080.0 + 0.4 * i, size=20.0 if i % 4 == 0 else 60.0,
                          side_aggressor=se.AGGRESSOR_BUY)
              for i in range(15)]
    trades.append(oos.Print1m(ts_utc=bars[-1].ts_utc, price=1094.0, size=30.0,
                              side_aggressor=se.AGGRESSOR_SELL))
    if direction == "bearish":
        bars = [_mirror_bar(b) for b in bars]
        trades = [_mirror_trade(t) for t in trades]
        closing = 2200.0 - 1094.0
    else:
        closing = 1094.0
    return bars, trades, first, closing



class TestEndToEndSyntheticReplay(unittest.TestCase):
    """Synthetic end-to-end proof that the harness can drive the frozen engine.

    These are SYNTHETIC fixtures. They demonstrate mechanics only and say nothing
    about real-market performance.
    """

    def _run(self, direction):
        bars, trades, first, _ = _synthetic_series(direction)
        if direction == "bullish":
            ask_liq = [se.LiquidityLevel(price=1200.0, contracts=250.0, side="ask",
                                         observed_ts_utc=first - timedelta(minutes=10))]
            bid_liq = []
        else:
            ask_liq = []
            bid_liq = [se.LiquidityLevel(price=1000.0, contracts=250.0, side="bid",
                                         observed_ts_utc=first - timedelta(minutes=10))]
        return oos.run_replay(bars, trades, config={"min_history_1m": 900},
                              ask_liquidity=ask_liq, bid_liquidity=bid_liq)

    def test_bullish_signals_are_generated_and_simulated(self):
        rep = self._run("bullish")
        self.assertGreaterEqual(rep["stats"]["signals_confirmed"], 1)
        self.assertEqual(rep["stats"]["signals_simulated"], 1)
        self.assertEqual(rep["metrics"]["trade_count"], 1)
        self.assertTrue(all(s["signal"] == "BUY" for s in rep["signals"]))

    def test_bearish_signals_are_generated_and_simulated(self):
        rep = self._run("bearish")
        self.assertGreaterEqual(rep["stats"]["signals_confirmed"], 1)
        self.assertTrue(all(s["signal"] == "SELL" for s in rep["signals"]))

    def test_duplicate_decisions_collapse_to_one_signal(self):
        rep = self._run("bullish")
        self.assertEqual(len({s["signal_id"] for s in rep["signals"]}),
                         rep["stats"]["signals_confirmed"])
        self.assertEqual(rep["stats"]["signals_simulated"], 1)

    def test_simulated_trade_pays_costs(self):
        rep = self._run("bullish")
        trade = rep["simulated_trades"][0]
        self.assertGreater(trade["costs_r"], 0)
        self.assertLess(trade["net_r"], trade["gross_r"])
        self.assertGreaterEqual(trade["planned_rr"], 1.5)

    def test_confirmed_signal_is_not_reclassified_as_preconfirmation_expired(self):
        bars, trades, first, _ = _synthetic_series("bullish")
        ask_liq = [se.LiquidityLevel(
            price=1200.0, contracts=250.0, side="ask",
            observed_ts_utc=first - timedelta(minutes=10))]
        config = {"min_history_1m": 900}

        initial = oos.run_replay(bars, trades, config=config, ask_liquidity=ask_liq)
        self.assertEqual(len(initial["signals"]), 1)
        confirmed_signal_id = initial["signals"][0]["signal_id"]
        expiry_ts = oos._parse_ts(initial["signals"][0]["expires_ts_utc"])

        # Continue the same contiguous synthetic path through the exclusive
        # expiry boundary so the engine emits WAIT_SETUP_EXPIRED for this setup.
        last = bars[-1]
        while last.ts_utc < expiry_ts:
            next_ts = last.ts_utc + timedelta(minutes=1)
            close = last.close
            last = bar(next_ts, close, close + 0.2, close - 0.2, close)
            bars.append(last)

        replay = oos.run_replay(bars, trades, config=config, ask_liquidity=ask_liq)
        confirmed_ids = {record["signal_id"] for record in replay["signals"]}
        expired_ids = {record["signal_id"] for record in replay["expired_candidates"]}

        self.assertIn(confirmed_signal_id, confirmed_ids)
        self.assertNotIn(
            confirmed_signal_id,
            expired_ids,
            "a confirmed setup must not be reclassified as a pre-confirmation expiry",
        )
        self.assertTrue(confirmed_ids.isdisjoint(expired_ids))

    def test_report_carries_policy_and_version(self):
        rep = self._run("bullish")
        self.assertEqual(rep["params_version"], "oos-1.0")
        self.assertIn("require_checksum_verified", rep["policy_relaxation"])
        self.assertIn("engine_overrides", rep)

    def test_signal_timestamps_match_recorded_bars(self):
        bars, trades, first, _ = _synthetic_series("bullish")
        ask_liq = [se.LiquidityLevel(price=1200.0, contracts=250.0, side="ask",
                                     observed_ts_utc=first - timedelta(minutes=10))]
        rep = oos.run_replay(bars, trades, config={"min_history_1m": 900},
                             ask_liquidity=ask_liq)
        bar_ts = {b.ts_utc for b in bars}
        for s in rep["signals"]:
            self.assertIn(oos._parse_ts(s["decision_ts_utc"]), bar_ts)
            self.assertLessEqual(oos._parse_ts(s["setup_candle_ts_utc"]),
                                 oos._parse_ts(s["decision_ts_utc"]))
            decision = oos._parse_ts(s["decision_ts_utc"])
            expiry = oos._parse_ts(s["expires_ts_utc"])
            # expiry is always a bounded number of completed 1m candles ahead
            self.assertLessEqual(decision, expiry)
            self.assertLessEqual((expiry - decision).total_seconds(),
                                 se.DEFAULT_CONFIG["expiry_1m_candles"] * 60 + 120)
