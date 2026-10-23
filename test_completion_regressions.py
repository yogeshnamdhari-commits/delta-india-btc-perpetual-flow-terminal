"""Focused regression tests for replay timestamps and execution costs."""
from datetime import timedelta
import unittest
import oos_replay as oos
from test_oos_replay import T0, synth_1m

class TestReplayNoLookahead(unittest.TestCase):
    def test_snapshot_excludes_open_bar_at_decision_timestamp(self):
        bars=synth_1m(1000)
        runner=oos.ReplayRunner(bars,[],config={"min_history_1m":900})
        now=T0+timedelta(minutes=900)
        snap=runner.snapshot_at(now)
        self.assertIsNotNone(snap)
        self.assertLess(snap.candles_1m[-1].ts_utc,now)
        self.assertEqual(snap.candles_1m[-1].ts_utc,now-timedelta(minutes=1))
    def test_print_at_decision_timestamp_is_not_visible(self):
        bars=synth_1m(1000); now=T0+timedelta(minutes=900)
        prints=[oos.Print1m(now,100.0,1.0,oos.se.AGGRESSOR_BUY)]
        runner=oos.ReplayRunner(bars,prints,config={"min_history_1m":900})
        snap=runner.snapshot_at(now)
        self.assertIsNotNone(snap)
        self.assertFalse(any(t.ts_utc==now for t in snap.recent_trades))

class TestReplayExecutionCosts(unittest.TestCase):
    def test_spread_included_in_adverse_fill(self):
        zero=oos.CostModel(taker_fee_bps=0,slippage_bps=0,spread_bps=0)
        spread=oos.CostModel(taker_fee_bps=0,slippage_bps=0,spread_bps=4)
        self.assertGreater(spread.entry_price(100,"BUY"),zero.entry_price(100,"BUY"))
        self.assertLess(spread.exit_price(100,"BUY"),zero.exit_price(100,"BUY"))
    def test_negative_cost_rejected(self):
        with self.assertRaises(oos.ReplayDataError):
            oos.CostModel(taker_fee_bps=5,slippage_bps=-1,spread_bps=2)
    def test_fill_uses_first_bar_at_requested_timestamp(self):
        bars=synth_1m(1200)
        runner=oos.ReplayRunner(bars,[],config={"min_history_1m":900})
        entry=runner.bars[1000]
        result=oos.se.SignalResult(signal=oos.se.Signal.BUY,
            state=oos.se.SignalState.BUY_CONFIRMED,signal_id="regression-fill-ts",
            proposed_entry=entry.open,stop_loss=entry.open-10.0,take_profit=entry.open+30.0,
            risk_distance=10.0,risk_reward_ratio=3.0,
            expires_at_utc=T0+timedelta(minutes=1005),
            generated_at_utc=T0+timedelta(minutes=1000))
        expected=oos.CostModel(taker_fee_bps=5,slippage_bps=2,spread_bps=2).entry_price(entry.open,"BUY")
        trade=runner.simulate(result,entry)
        self.assertIsNotNone(trade)
        self.assertAlmostEqual(trade["simulated_entry"],expected,places=8)
        self.assertGreater(trade["costs_r"],0)


class TestDashboardDecisionContract(unittest.TestCase):
    def test_dashboard_displays_and_persists_the_same_canonical_record(self):
        from pathlib import Path
        source = (Path(__file__).resolve().parent / "app.py").read_text(encoding="utf-8")
        self.assertIn("decision_record = evaluate_feed_snapshot(", source)
        self.assertIn("log_ok = _log_event(decision_record, is_decision=True)", source)
        self.assertIn('engine = st.session_state.get("_delta_signal_engine")', source)
        self.assertIn('decision = decision_record.get("decision", "BLOCKED")', source)
        self.assertNotIn("decision = evaluate_live_signal(feed)", source)

if __name__=="__main__": unittest.main()
