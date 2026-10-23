import unittest
from datetime import datetime, timedelta, timezone
from signal_engine import Candle
from signal_integration_bridge import _aggregate_completed_candles

UTC = timezone.utc
BASE = datetime(2026, 10, 20, 0, 0, tzinfo=UTC)

def make_candles(count, missing=()):
    result = []
    for i in range(count):
        if i in missing:
            continue
        price = 100.0 + i
        result.append(Candle(
            ts_utc=BASE + timedelta(minutes=i),
            open=price,
            high=price + 2,
            low=price - 1,
            close=price + 1,
            volume=float(i + 1),
        ))
    return result

class TestTimeframeAggregation(unittest.TestCase):
    def test_five_minute_ohlcv(self):
        source = make_candles(5)
        result = _aggregate_completed_candles(
            source, 5, now=BASE + timedelta(minutes=5)
        )
        self.assertEqual(len(result), 1)
        bar = result[0]
        self.assertEqual(bar.ts_utc, BASE)
        self.assertEqual(bar.open, source[0].open)
        self.assertEqual(bar.high, max(c.high for c in source))
        self.assertEqual(bar.low, min(c.low for c in source))
        self.assertEqual(bar.close, source[-1].close)
        self.assertEqual(bar.volume, sum(c.volume for c in source))
    
    def test_fifteen_minute_aggregation(self):
        result = _aggregate_completed_candles(
            make_candles(15), 15,
            now=BASE + timedelta(minutes=15),
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].ts_utc, BASE)
    
    def test_missing_minute_drops_interval(self):
        result = _aggregate_completed_candles(
            make_candles(5, missing={2}), 5,
            now=BASE + timedelta(minutes=5),
        )
        self.assertEqual(result, [])
    
    def test_unclosed_interval_is_not_emitted(self):
        result = _aggregate_completed_candles(
            make_candles(5), 5,
            now=BASE + timedelta(minutes=5, seconds=-1),
        )
        self.assertEqual(result, [])
    
    def test_warmup_history(self):
        now = BASE + timedelta(minutes=825)
        source = make_candles(825)
        five = _aggregate_completed_candles(source, 5, now=now)
        fifteen = _aggregate_completed_candles(source, 15, now=now)
        self.assertEqual(len(five), 165)
        self.assertEqual(len(fifteen), 55)

if __name__ == "__main__":
    unittest.main(verbosity=2)