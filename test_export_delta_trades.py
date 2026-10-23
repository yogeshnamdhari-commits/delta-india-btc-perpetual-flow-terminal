"""Timestamp-precision regression tests for the Delta trade exporter."""
import unittest
from datetime import datetime, timezone

from export_delta_trades import epoch_datetime, trade_datetime


class TestTradeTimestampResolution(unittest.TestCase):
    def test_prefers_raw_exchange_microseconds_and_preserves_precision(self):
        event = {
            "exchange_trade_ts_us": "1791780701123456",
            "ts": 1791780700,
            "server_publish_ts_us": "1791780701987654",
            "ts_utc": "2026-10-12T04:51:41.128325Z",
        }
        expected = datetime.fromtimestamp(1791780701, timezone.utc).replace(microsecond=123456)
        self.assertEqual(trade_datetime(event), expected)

    def test_legacy_seconds_timestamp_remains_supported(self):
        event = {"ts": 1791780700, "ts_utc": "2026-10-12T04:51:41.128325Z"}
        self.assertEqual(
            trade_datetime(event),
            datetime.fromtimestamp(1791780700, timezone.utc),
        )

    def test_falls_back_to_exchange_ts_if_legacy_ts_missing(self):
        expected = datetime.fromtimestamp(1791780701, timezone.utc).replace(microsecond=123456)
        self.assertEqual(trade_datetime({"exchange_ts": "1791780701123456"}), expected)

    def test_missing_timestamps_return_none(self):
        self.assertIsNone(trade_datetime({"ts_utc": "2026-10-12T04:51:41.128325Z"}))

    def test_invalid_preferred_timestamp_is_not_silently_hidden_by_fallback(self):
        with self.assertRaises(ValueError):
            trade_datetime({"exchange_trade_ts_us": "invalid", "ts": 1791780700})

    def test_epoch_decimal_conversion_preserves_fractional_seconds(self):
        expected = datetime.fromtimestamp(1791780701, timezone.utc).replace(microsecond=123456)
        self.assertEqual(epoch_datetime("1791780701123456"), expected)


if __name__ == "__main__":
    unittest.main()
