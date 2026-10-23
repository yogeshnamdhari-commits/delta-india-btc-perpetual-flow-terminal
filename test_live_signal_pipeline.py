"""Synthetic integration-contract tests; not live-feed or profitability evidence."""
from __future__ import annotations
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from signal_engine import SignalEngine
from signal_integration_bridge import _seed_candle_history, _to_utc, build_market_snapshot, evaluate_feed_snapshot, evaluate_live_signal

UTC = timezone.utc

def healthy_feed(now: datetime, *, candles: int = 1100) -> dict:
    end_minute = int(now.timestamp() // 60) * 60
    rows = []
    for i in range(candles):
        bucket = end_minute - 60 * (candles - i)
        rows.append({"time": bucket, "open": 100000.0, "high": 100002.0,
                     "low": 99998.0, "close": 100000.0, "volume": 2.0})
    trades = [{"timestamp": now.timestamp() - (19-i)*0.5, "price": 100000.0,
               "size": 1.0, "buyer_role": "t" if i % 2 == 0 else "m"} for i in range(20)]
    seen = now.timestamp() - 1.0
    return {"connection": "connected", "last_price": 100000.0, "bid": 99999.5,
            "ask": 100000.5, "book_synced": True, "sequence_gap_unresolved": False,
            "checksum_ok": True, "book_seq": 9001,
            "channel_last_seen": {"ticker": seen, "ob_updates": seen, "trades": seen},
            "last_market_message_at": seen, "trades": trades, "candles": rows,
            "total_disconnects": 0, "total_reconnects": 1}

class FakeFeed:
    def __init__(self, data): self.data = data
    def snapshot(self): return self.data
    def get_signal_state(self):
        return {k:self.data.get(k) for k in ("connection", "book_synced", "checksum_ok", "sequence_gap_unresolved", "channel_last_seen", "total_disconnects", "total_reconnects")}

class TestTimestampContract(unittest.TestCase):
    def test_seconds_milliseconds_microseconds_nanoseconds(self):
        epoch = 1_797_177_600
        expected = datetime.fromtimestamp(epoch, UTC)
        for value in (epoch, epoch*1000, epoch*1_000_000, epoch*1_000_000_000):
            self.assertEqual(_to_utc(value), expected)
    def test_iso_timestamps(self):
        expected = datetime(2026,10,23,12,tzinfo=UTC)
        self.assertEqual(_to_utc("2026-10-23T12:00:00Z"), expected)
        self.assertEqual(_to_utc("2026-10-23T12:00:00"), expected)


class TestHistoricalCandleTimestampSafety(unittest.TestCase):
    def test_receive_timestamp_is_not_mistaken_for_candle_timestamp(self):
        now = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
        receive_only = {"event": "channel_message", "channel": "candlestick_1m",
                        "ts_utc": (now - timedelta(minutes=1)).isoformat(),
                        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 2.0}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "events.jsonl"
            path.write_text(json.dumps(receive_only) + "\n", encoding="utf-8")
            self.assertEqual(_seed_candle_history(str(path), now), [])
            good = dict(receive_only, candle_ts_utc=(now - timedelta(minutes=1)).isoformat())
            path.write_text(json.dumps(good) + "\n", encoding="utf-8")
            seeded = _seed_candle_history(str(path), now)
            self.assertEqual(len(seeded), 1)
            self.assertEqual(seeded[0].ts_utc, now - timedelta(minutes=1))

class TestFeedToDecisionPipeline(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026,10,23,12,0,0,tzinfo=UTC)
        self.data = healthy_feed(self.now)
    def record(self,data=None,engine=None):
        return evaluate_feed_snapshot(self.data if data is None else data,
            feed_state={"fixture":"synthetic"}, engine=engine, now=self.now,
            history_path="/path/that/does/not/exist")
    def test_healthy_feed_reaches_engine(self):
        snap = build_market_snapshot(self.data, now=self.now, history_path="/missing")
        self.assertIsNotNone(snap)
        self.assertTrue(snap.connection_healthy and snap.market_fresh and snap.book_synced)
        self.assertFalse(snap.sequence_gap_unresolved)
        record = self.record()
        self.assertTrue(record["metadata"]["engine_evaluated"])
        self.assertNotEqual(record["decision"], "BLOCKED")
        self.assertGreaterEqual(record["metadata"]["candles_15m"],55)
    def test_disconnect_blocks_and_same_engine_recovers(self):
        engine=SignalEngine()
        blocked=self.record(dict(self.data,connection="disconnected"),engine)
        self.assertEqual(blocked["decision"],"BLOCKED")
        self.assertIn("connection_healthy",blocked["metadata"]["failed_data_quality_gates"])
        recovered=self.record(self.data,engine)
        self.assertNotEqual(recovered["decision"],"BLOCKED")
    def test_missing_channel_timestamps_fail_closed(self):
        r=self.record(dict(self.data,channel_last_seen={}))
        self.assertEqual(r["decision"],"BLOCKED")
        self.assertIn("market_fresh",r["metadata"]["failed_data_quality_gates"])
    def test_stale_trade_channel_blocks(self):
        times=dict(self.data["channel_last_seen"]); times["trades"]=self.now.timestamp()-90
        self.assertEqual(self.record(dict(self.data,channel_last_seen=times))["decision"],"BLOCKED")
    def test_unsynced_book_blocks(self):
        r=self.record(dict(self.data,book_synced=False))
        self.assertEqual(r["decision"],"BLOCKED")
        self.assertIn("book_synced",r["metadata"]["failed_data_quality_gates"])
    def test_unresolved_sequence_gap_blocks(self):
        r=self.record(dict(self.data,sequence_gap_unresolved=True))
        self.assertEqual(r["decision"],"BLOCKED")
        self.assertIn("no_unresolved_sequence_gap",r["metadata"]["failed_data_quality_gates"])
    def test_failed_or_unavailable_checksum_blocks(self):
        for checksum in (False,None):
            with self.subTest(checksum=checksum):
                self.assertEqual(self.record(dict(self.data,checksum_ok=checksum))["decision"],"BLOCKED")
    def test_bad_quote_returns_blocked_record(self):
        r=self.record(dict(self.data,bid=0.0))
        self.assertEqual(r["decision"],"BLOCKED")
        self.assertEqual(r["blocking_reason"],"SNAPSHOT_UNAVAILABLE")
    def test_log_record_serializes_and_preserves_decision(self):
        r=self.record(); out=json.loads(json.dumps(r,allow_nan=False))
        self.assertEqual(out["decision"],r["decision"])
        self.assertEqual(out["signal_type"],r["signal_type"])
    def test_compatibility_wrapper_uses_same_pipeline(self):
        r=evaluate_live_signal(FakeFeed(self.data),engine=SignalEngine(),now=self.now)
        self.assertIn(r["decision"],("WAIT","BUY","SELL","BLOCKED"))
        self.assertTrue(r["metadata"]["engine_evaluated"] or r["decision"]=="BLOCKED")

if __name__=="__main__": unittest.main()
