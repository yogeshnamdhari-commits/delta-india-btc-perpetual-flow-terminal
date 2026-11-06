import unittest

from exhaustion_confirmation import analyze_exhaustion_and_sweeps


BASE = 1_800_000_000


def price_candles(count=60, *, base_close=101.0):
    rows = []
    for index in range(count):
        rows.append({
            "time": BASE + index * 60,
            "open": 100.0,
            "high": 102.0,
            "low": 98.0,
            "close": base_close,
            "volume": 100.0,
        })
    return rows


def oi_candles(count=60, *, base_oi=10_000.0):
    return [
        {
            "time": BASE + index * 60,
            "open": base_oi,
            "high": base_oi,
            "low": base_oi,
            "close": base_oi,
        }
        for index in range(count)
    ]


def flow_row(*, buy_count=10, sell_count=0, buy=200.0, sell=0.0):
    return {
        "buy_count": buy_count,
        "sell_count": sell_count,
        "buy_contracts": buy,
        "sell_contracts": sell,
    }


def run_analysis(candles, flow, oi, sweeps=(), *, now=None):
    now = now or candles[-1]["time"] + 70
    return analyze_exhaustion_and_sweeps(
        candles, flow, oi, sweeps,
        now_ts=now,
        feed_started_at=BASE - 60,
        connection_status="connected",
        trade_channel_last_seen=now - 2,
    )


class TestMultiFactorExhaustion(unittest.TestCase):
    def test_buy_exhaustion_needs_price_flow_volume_and_material_oi_change(self):
        candles = price_candles()
        event_index = 35
        event_time = candles[event_index]["time"]
        candles[event_index].update({
            "open": 103.0, "high": 110.0, "low": 102.0,
            "close": 104.0, "volume": 200.0,
        })
        oi = oi_candles()
        oi[event_index]["close"] = 10_020.0
        flow = {event_time: flow_row(buy_count=8, sell_count=2, buy=160, sell=10)}

        result = run_analysis(candles, flow, oi)
        row = next(item for item in result["exhaustion_candidates"] if item["time"] == event_time)

        self.assertEqual(row["kind"], "BUY EXHAUSTION")
        self.assertTrue(row["volume_confirmed"])
        self.assertTrue(row["flow"]["direction_confirmed"])
        self.assertEqual(row["oi"]["state"], "BUILDING")
        self.assertTrue(row["multi_factor_evidence"])
        self.assertEqual(row["score"], 4)

    def test_sell_exhaustion_uses_sell_aggressor_and_oi_measurement(self):
        candles = price_candles(base_close=99.0)
        event_index = 35
        event_time = candles[event_index]["time"]
        candles[event_index].update({
            "open": 99.0, "high": 100.0, "low": 90.0,
            "close": 98.0, "volume": 200.0,
        })
        oi = oi_candles()
        oi[event_index]["close"] = 9_980.0
        flow = {event_time: flow_row(buy_count=2, sell_count=8, buy=10, sell=160)}

        result = run_analysis(candles, flow, oi)
        row = next(item for item in result["exhaustion_candidates"] if item["time"] == event_time)

        self.assertEqual(row["kind"], "SELL EXHAUSTION")
        self.assertTrue(row["multi_factor_evidence"])
        self.assertEqual(row["oi"]["state"], "REDUCING")

    def test_low_relative_volume_keeps_candidate_unconfirmed(self):
        candles = price_candles()
        index = 35
        event_time = candles[index]["time"]
        candles[index].update({
            "open": 103.0, "high": 110.0, "low": 102.0,
            "close": 104.0, "volume": 149.0,
        })
        oi = oi_candles()
        oi[index]["close"] = 10_020.0
        flow = {event_time: flow_row(buy_count=8, sell_count=2, buy=160, sell=10)}

        result = run_analysis(candles, flow, oi)
        row = next(item for item in result["exhaustion_candidates"] if item["time"] == event_time)

        self.assertFalse(row["volume_confirmed"])
        self.assertFalse(row["multi_factor_evidence"])
        self.assertIn("VOLUME NOT CONFIRMED", row["classification"])

    def test_missing_oi_never_receives_multi_factor_confirmation(self):
        candles = price_candles()
        index = 35
        event_time = candles[index]["time"]
        candles[index].update({
            "open": 103.0, "high": 110.0, "low": 102.0,
            "close": 104.0, "volume": 200.0,
        })
        flow = {event_time: flow_row(buy_count=8, sell_count=2, buy=160, sell=10)}

        result = run_analysis(candles, flow, [])
        row = next(item for item in result["exhaustion_candidates"] if item["time"] == event_time)

        self.assertFalse(row["oi"]["available"])
        self.assertFalse(row["multi_factor_evidence"])
        self.assertIn("OI UNVERIFIED", row["classification"])

    def test_stale_trade_channel_blocks_flow_confirmation(self):
        candles = price_candles()
        index = 35
        event_time = candles[index]["time"]
        candles[index].update({
            "open": 103.0, "high": 110.0, "low": 102.0,
            "close": 104.0, "volume": 200.0,
        })
        oi = oi_candles()
        oi[index]["close"] = 10_020.0
        now = candles[-1]["time"] + 70
        result = analyze_exhaustion_and_sweeps(
            candles, {event_time: flow_row(buy_count=8, sell_count=2, buy=160, sell=10)},
            oi, [], now_ts=now, feed_started_at=BASE - 60,
            connection_status="connected", trade_channel_last_seen=now - 60,
        )
        row = next(item for item in result["exhaustion_candidates"] if item["time"] == event_time)

        self.assertFalse(row["flow"]["capture_eligible"])
        self.assertFalse(row["multi_factor_evidence"])

    def test_app_start_after_event_blocks_historical_tape_confirmation(self):
        candles = price_candles()
        index = 35
        event_time = candles[index]["time"]
        candles[index].update({
            "open": 103.0, "high": 110.0, "low": 102.0,
            "close": 104.0, "volume": 200.0,
        })
        oi = oi_candles()
        oi[index]["close"] = 10_020.0
        now = candles[-1]["time"] + 70
        result = analyze_exhaustion_and_sweeps(
            candles, {event_time: flow_row(buy_count=8, sell_count=2, buy=160, sell=10)},
            oi, [], now_ts=now, feed_started_at=event_time + 1,
            connection_status="connected", trade_channel_last_seen=now - 2,
        )
        row = next(item for item in result["exhaustion_candidates"] if item["time"] == event_time)

        self.assertFalse(row["flow"]["capture_eligible"])
        self.assertFalse(row["flow"]["direction_confirmed"])

    def test_gap_in_price_comparison_window_suppresses_candidate(self):
        candles = price_candles()
        index = 35
        event_time = candles[index]["time"]
        candles[index].update({
            "open": 103.0, "high": 110.0, "low": 102.0,
            "close": 104.0, "volume": 200.0,
        })
        candles.pop(25)
        oi = oi_candles()
        flow = {event_time: flow_row(buy_count=8, sell_count=2, buy=160, sell=10)}

        result = run_analysis(candles, flow, oi)
        self.assertFalse(any(item["time"] == event_time for item in result["exhaustion_candidates"]))

    def test_in_progress_price_and_oi_candles_are_excluded(self):
        candles = price_candles()
        index = 59
        candles[index].update({
            "open": 103.0, "high": 110.0, "low": 102.0,
            "close": 104.0, "volume": 200.0,
        })
        oi = oi_candles()
        oi[index]["close"] = 10_020.0
        now = candles[index]["time"] + 10
        result = run_analysis(candles, {}, oi, now=now)
        self.assertEqual(result["completed_price_candles"], 59)
        self.assertNotIn(candles[index]["time"], [item["time"] for item in result["exhaustion_candidates"]])


class TestMultiFactorStopHuntLikeSweeps(unittest.TestCase):
    def _high_sweep(self, *, oi_direction="down"):
        candles = price_candles(count=80)
        first = 50
        reclaim = 51
        candles[first].update({
            "open": 101.0, "high": 104.0, "low": 100.0,
            "close": 103.0, "volume": 200.0,
        })
        candles[reclaim].update({
            "open": 103.0, "high": 103.5, "low": 100.0,
            "close": 101.0, "volume": 200.0,
        })
        oi = oi_candles(count=80)
        if oi_direction == "down":
            oi[first]["close"] = 9_990.0
            oi[reclaim]["close"] = 9_980.0
        else:
            oi[first]["close"] = 10_010.0
            oi[reclaim]["close"] = 10_020.0
        t1, t2 = candles[first]["time"], candles[reclaim]["time"]
        flow = {
            t1: flow_row(buy_count=5, buy=100),
            t2: flow_row(buy_count=5, buy=100),
        }
        sweep = {
            "kind": "high", "price": 102.0, "time": candles[10]["time"],
            "exceed_time": t1, "sweep_time": t2,
            "breach_price": 104.0, "overshoot_points": 2.0,
            "reclaim_delay_bars": 1,
        }
        return candles, flow, oi, sweep

    def test_high_sweep_requires_volume_flow_and_oi_reduction(self):
        candles, flow, oi, sweep = self._high_sweep()
        result = run_analysis(candles, flow, oi, [sweep])
        row = result["sweep_candidates"][0]

        self.assertTrue(row["volume_confirmed"])
        self.assertTrue(row["flow"]["direction_confirmed"])
        self.assertEqual(row["oi"]["state"], "REDUCING")
        self.assertTrue(row["multi_factor_evidence"])
        self.assertIn("BUY-SIDE SWEEP", row["classification"])

    def test_high_sweep_with_oi_build_is_not_called_stop_hunt_like(self):
        candles, flow, oi, sweep = self._high_sweep(oi_direction="up")
        result = run_analysis(candles, flow, oi, [sweep])
        row = result["sweep_candidates"][0]

        self.assertEqual(row["oi"]["state"], "BUILDING")
        self.assertFalse(row["multi_factor_evidence"])
        self.assertIn("OI BUILDING", row["classification"])

    def test_low_sweep_requires_sell_taker_flow_and_oi_reduction(self):
        candles = price_candles(count=80, base_close=99.0)
        first, reclaim = 50, 51
        candles[first].update({
            "open": 99.0, "high": 100.0, "low": 96.0,
            "close": 97.0, "volume": 200.0,
        })
        candles[reclaim].update({
            "open": 97.0, "high": 100.0, "low": 97.0,
            "close": 99.0, "volume": 200.0,
        })
        oi = oi_candles(count=80)
        oi[first]["close"] = 9_990.0
        oi[reclaim]["close"] = 9_980.0
        t1, t2 = candles[first]["time"], candles[reclaim]["time"]
        flow = {
            t1: flow_row(buy_count=0, sell_count=5, buy=0, sell=100),
            t2: flow_row(buy_count=0, sell_count=5, buy=0, sell=100),
        }
        sweep = {
            "kind": "low", "price": 98.0, "time": candles[10]["time"],
            "exceed_time": t1, "sweep_time": t2,
            "breach_price": 96.0, "overshoot_points": 2.0,
            "reclaim_delay_bars": 1,
        }

        result = run_analysis(candles, flow, oi, [sweep])
        row = result["sweep_candidates"][0]

        self.assertTrue(row["multi_factor_evidence"])
        self.assertIn("SELL-SIDE SWEEP", row["classification"])

    def test_sweep_without_reclaim_is_not_analyzed_as_sweep(self):
        candles, flow, oi, sweep = self._high_sweep()
        sweep.pop("sweep_time")
        result = run_analysis(candles, flow, oi, [sweep])
        self.assertEqual(result["sweep_candidates"], [])

    def test_sweep_with_missing_oi_is_not_multi_factor_confirmed(self):
        candles, flow, _, sweep = self._high_sweep()
        result = run_analysis(candles, flow, [], [sweep])
        row = result["sweep_candidates"][0]
        self.assertFalse(row["multi_factor_evidence"])
        self.assertIn("OI UNVERIFIED", row["classification"])

    def test_sweep_flow_must_match_the_break_direction(self):
        candles, _, oi, sweep = self._high_sweep()
        t1, t2 = sweep["exceed_time"], sweep["sweep_time"]
        flow = {
            t1: flow_row(buy_count=0, sell_count=5, buy=0, sell=100),
            t2: flow_row(buy_count=0, sell_count=5, buy=0, sell=100),
        }
        result = run_analysis(candles, flow, oi, [sweep])
        row = result["sweep_candidates"][0]
        self.assertFalse(row["flow"]["direction_confirmed"])
        self.assertFalse(row["multi_factor_evidence"])

    def test_bad_inputs_are_rejected(self):
        with self.assertRaises(ValueError):
            analyze_exhaustion_and_sweeps(
                [], {}, [], [], now_ts=float("nan"), feed_started_at=None,
                connection_status="connected", trade_channel_last_seen=None,
            )
        with self.assertRaises(ValueError):
            analyze_exhaustion_and_sweeps(
                [], {}, [], [], now_ts=BASE + 100,
                feed_started_at=BASE, connection_status="connected",
                trade_channel_last_seen=BASE + 99, min_volume_multiple=0,
            )


if __name__ == "__main__":
    unittest.main()
