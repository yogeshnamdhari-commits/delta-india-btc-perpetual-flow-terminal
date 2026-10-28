import json
import unittest
from unittest.mock import Mock, patch

from bybit_market import (
    BybitV5Feed,
    aggregate_observed_bankruptcy_buckets,
    apply_book_update,
    normalize_liquidation,
    normalize_rest_kline,
    normalize_trade,
    normalize_ws_kline,
)


class BybitV5NormalizationTests(unittest.TestCase):
    def test_taker_side_trade_mapping_and_notional(self):
        buy = normalize_trade({"p": "100000", "v": "0.02", "T": 1700000000000, "S": "Buy", "i": "a"})
        sell = normalize_trade({"p": "100000", "v": "0.03", "T": 1700000001000, "S": "Sell", "i": "b"})
        self.assertEqual(buy["side"], "BUY")
        self.assertEqual(sell["side"], "SELL")
        self.assertAlmostEqual(buy["notional_usdt_approx"], 2000)
        self.assertAlmostEqual(sell["size_btc"], 0.03)
        self.assertIsNone(normalize_trade({"p": "NaN", "v": "0.1", "T": 1700000000000, "S": "Buy"}))

    def test_public_rest_falls_back_to_official_alternate_host(self):
        feed = BybitV5Feed(request_timeout=1)
        primary = Mock()
        primary.raise_for_status.side_effect = __import__("requests").HTTPError("403 Forbidden")
        fallback = Mock()
        fallback.raise_for_status.return_value = None
        fallback.json.return_value = {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"list": [["1700000000000", "100", "110", "95", "105", "12", "1250"]]},
        }
        with patch("bybit_market.requests.get", side_effect=[primary, fallback]) as get:
            payload = feed._get_public_rest_json(
                "/v5/market/kline",
                {"category": "linear", "symbol": "BTCUSDT", "interval": "1", "limit": 1},
            )
        self.assertEqual(payload["retCode"], 0)
        self.assertEqual(get.call_count, 2)
        self.assertEqual(get.call_args_list[0].args[0], "https://api.bybit.com/v5/market/kline")
        self.assertEqual(get.call_args_list[1].args[0], "https://api.bytick.com/v5/market/kline")
        self.assertEqual(feed.data["rest_endpoint_active"], "https://api.bytick.com")
        self.assertEqual(feed.data["rest_endpoint_health"], "official_fallback")
        self.assertEqual(feed.data["last_rest_error"], "")

    def test_rest_kline_and_websocket_closed_flag(self):
        now = 1700000400
        with patch("bybit_market.time.time", return_value=now):
            row = normalize_rest_kline(
                ["1699999800000", "100", "110", "95", "105", "12", "1250"], "1m"
            )
        self.assertEqual(row["time"], 1699999800)
        self.assertEqual(row["close"], 105)
        self.assertTrue(row["is_closed"])
        live = normalize_ws_kline({
            "start": "1700000400000", "open": "100", "high": "105",
            "low": "95", "close": "103", "volume": "7", "turnover": "720",
            "confirm": False,
        }, "1m")
        self.assertFalse(live["is_closed"])
        self.assertIsNone(normalize_rest_kline(
            ["1699999800000", "100", "90", "95", "105", "12"], "1m"
        ))

    def test_orderbook_snapshot_replaces_and_delta_updates_or_deletes(self):
        bids, asks, update_id, seq, status = apply_book_update(
            {}, {}, message_type="snapshot",
            data={"u": 100, "seq": 1000, "b": [["100", "1"], ["99", "2"]], "a": [["101", "3"]]},
            previous_update_id=None, previous_sequence=None,
        )
        self.assertEqual(status, "snapshot")
        self.assertEqual(update_id, 100)
        bids, asks, update_id, seq, status = apply_book_update(
            bids, asks, message_type="delta",
            data={"u": 101, "seq": 1003, "b": [["100", "0"], ["98", "4"]], "a": [["101", "2"]]},
            previous_update_id=update_id, previous_sequence=seq,
        )
        self.assertEqual(status, "applied")
        self.assertNotIn(100.0, bids)
        self.assertAlmostEqual(bids[98.0], 4)
        self.assertAlmostEqual(asks[101.0], 2)
        replacement = apply_book_update(
            bids, asks, message_type="snapshot",
            data={"u": 200, "seq": 2000, "b": [["97", "1"]], "a": [["102", "2"]]},
            previous_update_id=update_id, previous_sequence=seq,
        )
        self.assertEqual(replacement[4], "snapshot")
        self.assertEqual(replacement[0], {97.0: 1.0})
        self.assertEqual(replacement[1], {102.0: 2.0})

    def test_orderbook_update_regression_is_reported(self):
        result = apply_book_update(
            {100.0: 1.0}, {101.0: 1.0}, message_type="delta",
            data={"u": 99, "seq": 999, "b": [], "a": []},
            previous_update_id=100, previous_sequence=1000,
        )
        self.assertEqual(result[4], "update_id_regression")
        seq_result = apply_book_update(
            {100.0: 1.0}, {101.0: 1.0}, message_type="delta",
            data={"u": 101, "seq": 1000, "b": [], "a": []},
            previous_update_id=100, previous_sequence=1000,
        )
        self.assertEqual(seq_result[4], "sequence_regression")
        gap_result = apply_book_update(
            {100.0: 1.0}, {101.0: 1.0}, message_type="delta",
            data={"u": 102, "seq": 1002, "b": [], "a": []},
            previous_update_id=100, previous_sequence=1000,
        )
        self.assertEqual(gap_result[4], "update_id_gap")

    def test_liquidation_reports_bankruptcy_price_not_execution_price(self):
        row = normalize_liquidation({
            "s": "BTCUSDT", "S": "Buy", "p": "99000", "v": "0.15", "T": 1700000000000
        })
        self.assertEqual(row["position_side_liquidated"], "BUY")
        self.assertAlmostEqual(row["bankruptcy_price"], 99000)
        self.assertAlmostEqual(row["size_btc"], 0.15)
        self.assertAlmostEqual(row["notional_usdt_approx_at_bankruptcy_price"], 14850)
        self.assertIsNone(normalize_liquidation({"s": "BTCUSDT", "S": "Buy", "p": "0", "v": "1", "T": 1700000000000}))

    def test_liquidation_buckets_aggregate_observed_bankruptcy_messages(self):
        rows = aggregate_observed_bankruptcy_buckets([
            {"timestamp": 1900, "bankruptcy_price": 100020, "size_btc": 0.1, "position_side_liquidated": "SELL"},
            {"timestamp": 1950, "bankruptcy_price": 100050, "size_btc": 0.2, "position_side_liquidated": "SELL"},
            {"timestamp": 1920, "bankruptcy_price": 100110, "size_btc": 0.1, "position_side_liquidated": "BUY"},
            {"timestamp": 1000, "bankruptcy_price": 100025, "size_btc": 1.0, "position_side_liquidated": "SELL"},
        ], now_epoch=2000, window_seconds=300, bucket_width=100)
        self.assertEqual(len(rows), 2)
        sell = next(row for row in rows if row["position_side_liquidated"] == "SELL")
        self.assertEqual(sell["bankruptcy_price_bucket_low"], 100000)
        self.assertEqual(sell["observed_liquidation_messages"], 2)
        self.assertAlmostEqual(sell["observed_size_btc"], 0.3)


    def test_ping_acknowledgement_is_not_answered_with_invalid_pong_operation(self):
        feed = BybitV5Feed()
        ws = Mock()
        feed._on_message(ws, json.dumps({
            "success": True,
            "ret_msg": "pong",
            "conn_id": "test",
            "op": "ping",
        }))
        ws.send.assert_not_called()

    def test_ws_topics_use_bybit_public_linear_stream(self):
        feed = BybitV5Feed()
        self.assertEqual(feed.ws_url, "wss://stream.bybit.com/v5/public/linear")
        snap = feed.snapshot()
        self.assertEqual(snap["symbol"], "BTCUSDT")
        self.assertFalse(snap["book_synced"])
        self.assertIsNone(snap["open_interest_btc"])

    def test_combined_topics_update_trade_ticker_and_native_kline_timeframes(self):
        feed = BybitV5Feed()
        feed._handle_trade({"p": "100000", "v": "0.02", "T": 1700000000000, "S": "Buy"})
        feed._handle_trade({"p": "100000", "v": "0.03", "T": 1700000001000, "S": "Sell"})
        feed._handle_ticker({
            "lastPrice": "100000", "markPrice": "99990", "indexPrice": "100010",
            "bid1Price": "99999", "ask1Price": "100001", "bid1Size": "2", "ask1Size": "3",
            "fundingRate": "-0.0001", "nextFundingTime": "1700003600000",
            "openInterest": "50000", "price24hPcnt": "0.01",
        })
        feed._handle_kline({
            "start": "1700000000000", "open": "100000", "high": "100100",
            "low": "99900", "close": "100050", "volume": "10",
            "turnover": "1000000", "confirm": True,
        }, "5m")
        snapshot = feed.snapshot()
        self.assertAlmostEqual(snapshot["cvd_value_btc"], -0.01)
        self.assertAlmostEqual(snapshot["funding_rate"], -0.0001)
        self.assertEqual(snapshot["bid"], 99999)
        self.assertEqual(snapshot["ask"], 100001)
        self.assertAlmostEqual(snapshot["open_interest_usd_approx"], 99990 * 50000)
        self.assertEqual(len(snapshot["candles_by_interval"]["5m"]), 1)
        self.assertTrue(snapshot["candles_by_interval"]["5m"][0]["is_closed"])


if __name__ == "__main__":
    unittest.main()
