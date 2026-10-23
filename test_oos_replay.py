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
        if not os.path.exists(path):
            self.skipTest("oos_config_frozen.json not written yet")
        import json
        with open(path, encoding="utf-8") as fh:
            frozen = json.load(fh)
        self.assertEqual(frozen["params_version"], oos.PARAMS_VERSION)
        for key, value in oos.FROZEN_OOS_CONFIG.items():
            if key in frozen and key != "params_version":
                self.assertEqual(frozen[key], value, f"frozen config drift on {key}")
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
        result = se.SignalResult(
            signal=se.Signal.BUY, state=se.SignalState.BUY_CONFIRMED,
            signal_id="x", proposed_entry=100.0, stop_loss=90.0, take_profit=130.0,
            invalidation_price=95.0, risk_distance=10.0, reward_distance=30.0,
            risk_reward_ratio=3.0, setup_timeframe="5m",
            setup_candle_timestamp=T0, expires_at_utc=T0 + timedelta(minutes=1005))
        runner = oos.ReplayRunner(bars, [], config={"min_history_1m": 900})
        entry_bar = bar(ts_of(1000), 100.0, 101.0, 99.0, 100.5)
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
