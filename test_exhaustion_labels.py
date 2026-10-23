"""Tests for conservative, tape-confirmed exhaustion labels."""
import unittest

import orderflow


BASE = (1791780000 // 60) * 60


def candle(ts, *, open_, high, low, close):
    return {
        "time": ts,
        "open": float(open_),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": 100.0,
    }


def prior_bars(direction="up"):
    if direction == "up":
        # Same neutral body/range before a higher-high rejection.
        return [
            candle(BASE + i * 60, open_=100, high=102, low=98, close=101)
            for i in range(10)
        ]
    return [
        candle(BASE + i * 60, open_=100, high=102, low=98, close=99)
        for i in range(10)
    ]


def tape(ts, side, *, buy_count=4, sell_count=1):
    rows = []
    for i in range(buy_count):
        rows.append((ts + 5 + i, 104.0, 2.0, "buy"))
    for i in range(sell_count):
        rows.append((ts + 20 + i, 104.0, 1.0, "sell"))
    return rows


class TestExhaustionLabels(unittest.TestCase):
    def test_buy_exhaustion_requires_higher_high_rejection_and_buy_pressure(self):
        ts = BASE + 10 * 60
        candles = prior_bars("up") + [
            candle(ts, open_=103, high=110, low=102, close=104)
        ]
        trades = tape(ts, buy_count=4, sell_count=1)

        labels = orderflow.detect_exhaustion_labels(candles, trades)

        self.assertEqual(len(labels), 1)
        label = labels[0]
        self.assertEqual(label["kind"], "buy_exhaust")
        self.assertEqual(label["time"], ts)
        self.assertEqual(label["price"], 110.0)
        self.assertEqual(label["trade_count"], 5)
        self.assertEqual(label["buy_contracts"], 8.0)
        self.assertEqual(label["sell_contracts"], 1.0)
        self.assertGreaterEqual(label["wick_ratio"], 0.35)

    def test_sell_exhaustion_requires_lower_low_rejection_and_sell_pressure(self):
        ts = BASE + 10 * 60
        candles = prior_bars("down") + [
            candle(ts, open_=99, high=100, low=90, close=98)
        ]
        trades = tape(ts, buy_count=1, sell_count=4)
        # Use a low-price tape sample for the sell exhaustion bar.
        trades = [
            (t[0], 95.0, t[2], t[3]) for t in trades
        ]

        labels = orderflow.detect_exhaustion_labels(candles, trades)

        self.assertEqual(len(labels), 1)
        label = labels[0]
        self.assertEqual(label["kind"], "sell_exhaust")
        self.assertEqual(label["time"], ts)
        self.assertEqual(label["price"], 90.0)
        self.assertEqual(label["trade_count"], 5)
        self.assertEqual(label["buy_contracts"], 2.0)
        self.assertEqual(label["sell_contracts"], 4.0)
        self.assertGreaterEqual(label["wick_ratio"], 0.35)

    def test_no_label_when_trade_sample_is_too_small(self):
        ts = BASE + 10 * 60
        candles = prior_bars("up") + [
            candle(ts, open_=103, high=110, low=102, close=104)
        ]
        labels = orderflow.detect_exhaustion_labels(candles, tape(ts, buy_count=3, sell_count=1))
        self.assertEqual(labels, [])

    def test_no_label_without_strict_break_of_local_high(self):
        ts = BASE + 10 * 60
        candles = prior_bars("up") + [
            candle(ts, open_=101, high=102, low=98, close=99)
        ]
        labels = orderflow.detect_exhaustion_labels(candles, tape(ts))
        self.assertEqual(labels, [])

    def test_gap_in_comparison_window_suppresses_label(self):
        bars = prior_bars("up")
        bars.pop(4)
        ts = BASE + 10 * 60
        bars.append(candle(ts, open_=103, high=110, low=102, close=104))
        labels = orderflow.detect_exhaustion_labels(bars, tape(ts))
        self.assertEqual(labels, [])

    def test_unrecognized_side_and_nonpositive_size_are_ignored(self):
        ts = BASE + 10 * 60
        candles = prior_bars("up") + [
            candle(ts, open_=103, high=110, low=102, close=104)
        ]
        trades = [
            (ts + 5, 104.0, 20.0, "unknown"),
            (ts + 6, 104.0, 0.0, "buy"),
            (ts + 7, 104.0, -1.0, "sell"),
        ]
        self.assertEqual(orderflow.detect_exhaustion_labels(candles, trades), [])

    def test_invalid_parameters_raise(self):
        with self.assertRaises(ValueError):
            orderflow.detect_exhaustion_labels([], [], timeframe_seconds=0)
        with self.assertRaises(ValueError):
            orderflow.detect_exhaustion_labels([], [], imbalance_ratio=0.5)


if __name__ == "__main__":
    unittest.main()
