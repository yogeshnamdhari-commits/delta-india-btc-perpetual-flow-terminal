import unittest
from unittest.mock import Mock, patch

from binance_market import (
    BinanceUSDMFeed,
    aggregate_observed_liquidations,
    depth_update_action,
    normalize_agg_trade,
    normalize_kline_event,
    normalize_liquidation_event,
    normalize_rest_kline,
)


class BinanceNormalizationTests(unittest.TestCase):
    def test_agg_trade_maps_aggressor_side_and_usd_notional(self):
        buy = normalize_agg_trade({"p": "100000", "q": "0.02", "T": 1_700_000_000_000, "m": False, "a": 7})
        sell = normalize_agg_trade({"p": "100000", "q": "0.03", "T": 1_700_000_001_000, "m": True, "a": 8})
        self.assertIsNotNone(buy)
        self.assertIsNotNone(sell)
        self.assertEqual(buy["side"], "BUY")
        self.assertEqual(sell["side"], "SELL")
        self.assertAlmostEqual(buy["notional_usd_approx"], 2000.0)
        self.assertAlmostEqual(sell["size_btc"], 0.03)

    def test_bad_or_incomplete_trade_is_rejected(self):
        self.assertIsNone(normalize_agg_trade({"p": "NaN", "q": "1", "T": 1_700_000_000_000, "m": False}))
        self.assertIsNone(normalize_agg_trade({"p": "100", "q": "1", "T": 1_700_000_000_000}))

    def test_rest_kline_normalization_and_invalid_ohlc_rejection(self):
        row = normalize_rest_kline([
            1_700_000_000_000, "100", "110", "95", "105", "12",
            1_700_000_059_999, "1250"
        ])
        self.assertIsNotNone(row)
        self.assertEqual(row["time"], 1_699_999_980)
        self.assertEqual(row["close"], 105.0)
        self.assertAlmostEqual(row["quote_volume"], 1250.0)
        invalid = normalize_rest_kline([1_700_000_000_000, "100", "90", "95", "105", "12"])
        self.assertIsNone(invalid)

    def test_websocket_kline_uses_exchange_closed_flag(self):
        event = {"k": {
            "t": 1_700_000_000_000, "o": "100", "h": "110", "l": "95",
            "c": "105", "v": "12", "q": "1250", "x": False
        }}
        row = normalize_kline_event(event)
        self.assertIsNotNone(row)
        self.assertFalse(row["is_closed"])
        self.assertEqual(row["volume"], 12.0)

    def test_force_order_uses_reported_filled_quantity_not_original_quantity(self):
        row = normalize_liquidation_event({
            "E": 1_700_000_000_000,
            "o": {
                "s": "BTCUSDT", "S": "SELL", "p": "100000", "ap": "99950",
                "q": "3.0", "l": "0.01", "z": "0.02", "T": 1_700_000_000_000,
                "X": "FILLED"
            }
        })
        self.assertIsNotNone(row)
        self.assertEqual(row["side"], "SELL")
        self.assertAlmostEqual(row["size_btc"], 0.01)
        self.assertAlmostEqual(row["notional_usd_approx"], 999.5)
        # No execution quantity: do not turn original order size q into a liquidation print.
        self.assertIsNone(normalize_liquidation_event({
            "E": 1_700_000_000_000,
            "o": {"s": "BTCUSDT", "S": "BUY", "p": "100000", "q": "2",
                  "l": "0", "z": "2", "T": 1_700_000_000_000}
        }))

    def test_depth_update_sequence_classification(self):
        self.assertEqual(depth_update_action(100, {"U": 99, "u": 100}), "stale")
        self.assertEqual(depth_update_action(100, {"U": 101, "u": 105}), "apply")
        self.assertEqual(depth_update_action(100, {"U": 102, "u": 105}), "gap")
        self.assertEqual(depth_update_action(100, {"u": 105}), "gap")

    def test_liquidation_buckets_are_observed_historical_events(self):
        events = [
            {"timestamp": 1900, "price": 100020, "size_btc": 0.1, "side": "SELL"},
            {"timestamp": 1950, "price": 100050, "size_btc": 0.2, "side": "SELL"},
            {"timestamp": 1920, "price": 100110, "size_btc": 0.1, "side": "BUY"},
            {"timestamp": 1000, "price": 100025, "size_btc": 1.0, "side": "SELL"},
        ]
        rows = aggregate_observed_liquidations(events, now_epoch=2000, window_seconds=300, bucket_width=100)
        self.assertEqual(len(rows), 2)
        sell = next(row for row in rows if row["order_side"] == "SELL")
        self.assertEqual(sell["price_bucket_low"], 100000)
        self.assertEqual(sell["observed_snapshot_count"], 2)
        self.assertAlmostEqual(sell["filled_qty_btc_reported"], 0.3)
        self.assertIn("observed_notional_usd_approx", sell)


    @patch("binance_market.requests.get")
    def test_depth_snapshot_and_buffered_diff_synchronize_the_book(self, get):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "lastUpdateId": 100,
            "bids": [["99000", "1.0"]],
            "asks": [["101000", "2.0"]],
        }
        get.return_value = response
        feed = BinanceUSDMFeed()
        feed._depth_buffer.append({
            "U": 99, "u": 101,
            "b": [["99500", "3.0"]],
            "a": [["101000", "0"]],
        })
        feed._sync_book_loop()
        snapshot = feed.snapshot()
        self.assertTrue(snapshot["book_synced"])
        self.assertFalse(snapshot["sequence_gap_unresolved"])
        self.assertEqual(snapshot["book_last_update_id"], 101)
        self.assertAlmostEqual(snapshot["bids"][99500.0], 3.0)
        self.assertNotIn(101000.0, snapshot["asks"])
        self.assertAlmostEqual(snapshot["asks"][101000.0] if 101000.0 in snapshot["asks"] else 0.0, 0.0)
        get.assert_called_once()

    def test_session_cvd_uses_aggtrade_aggressor_side(self):
        feed = BinanceUSDMFeed()
        feed._handle_agg_trade({"p": "100000", "q": "0.02", "T": 1_700_000_000_000, "m": False, "a": 1})
        feed._handle_agg_trade({"p": "100000", "q": "0.03", "T": 1_700_000_001_000, "m": True, "a": 2})
        snapshot = feed.snapshot()
        self.assertAlmostEqual(snapshot["buy_volume_btc"], 0.02)
        self.assertAlmostEqual(snapshot["sell_volume_btc"], 0.03)
        self.assertAlmostEqual(snapshot["cvd_value_btc"], -0.01)
        self.assertEqual(snapshot["trade_count"], 2)

    def test_feed_initializes_read_only_and_analyst_context_is_explicit_about_limitations(self):
        feed = BinanceUSDMFeed()
        snapshot = feed.snapshot()
        context = feed.analyst_context(now_epoch=1_700_000_000)
        self.assertEqual(snapshot["symbol"], "BTCUSDT")
        self.assertFalse(snapshot["book_synced"])
        self.assertIsNone(snapshot["checksum_ok"])
        self.assertIn("per-symbol", " ".join(context["liquidation_data_limitations"]))
        self.assertIn("not a complete market-wide census", " ".join(context["liquidation_data_limitations"]))
        self.assertEqual(context["order_book"]["checksum"].find("Not supplied by Binance") >= 0, True)


if __name__ == "__main__":
    unittest.main()
