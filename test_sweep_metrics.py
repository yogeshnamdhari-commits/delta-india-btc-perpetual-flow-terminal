"""Tests for transparent metadata on confirmed price sweeps / stop-hunt candidates."""

import unittest

import orderflow


def candle(ts, *, high, low, close, open_=100.0):
    return {
        "time": ts,
        "open": float(open_),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": 100.0,
    }


class TestSweepMetrics(unittest.TestCase):
    def test_high_sweep_reports_overshoot_and_same_candle_reclaim(self):
        candles = [candle(60, high=101.5, low=99.0, close=99.5)]
        levels = [{"kind": "high", "price": 100.0, "time": 0}]

        sweeps = orderflow.detect_sweeps(candles, levels, reclaim_bars=3, eps=0.5)

        self.assertEqual(len(sweeps), 1)
        self.assertEqual(sweeps[0]["kind"], "high")
        self.assertEqual(sweeps[0]["breach_price"], 101.5)
        self.assertEqual(sweeps[0]["overshoot_points"], 1.5)
        self.assertEqual(sweeps[0]["reclaim_delay_bars"], 0)
        self.assertEqual(sweeps[0]["reclaim_close"], 99.5)

    def test_low_sweep_reports_overshoot_and_delayed_reclaim(self):
        candles = [
            candle(60, high=101.0, low=98.0, close=98.2),
            candle(120, high=101.0, low=97.0, close=100.5),
        ]
        levels = [{"kind": "low", "price": 98.5, "time": 0}]

        sweeps = orderflow.detect_sweeps(candles, levels, reclaim_bars=3, eps=0.5)

        self.assertEqual(len(sweeps), 1)
        self.assertEqual(sweeps[0]["kind"], "low")
        self.assertEqual(sweeps[0]["breach_price"], 97.0)
        self.assertEqual(sweeps[0]["overshoot_points"], 1.5)
        self.assertEqual(sweeps[0]["reclaim_delay_bars"], 1)
        self.assertEqual(sweeps[0]["reclaim_close"], 100.5)

    def test_break_without_reclaim_is_not_a_sweep(self):
        candles = [
            candle(60, high=101.5, low=99.0, close=100.5),
            candle(120, high=102.0, low=100.0, close=101.0),
            candle(180, high=102.5, low=100.5, close=101.5),
        ]
        levels = [{"kind": "high", "price": 100.0, "time": 0}]

        sweeps = orderflow.detect_sweeps(candles, levels, reclaim_bars=3, eps=0.5)

        self.assertEqual(sweeps, [])


if __name__ == "__main__":
    unittest.main()
