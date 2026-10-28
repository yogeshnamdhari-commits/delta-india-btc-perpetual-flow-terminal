import unittest
from unittest.mock import Mock, patch

import requests

from okx_history import OKXHistoryFeed, normalize_okx_candle


class OKXHistoryTests(unittest.TestCase):
    def test_normalizer_keeps_base_volume_contract_volume_and_turnover_separate(self):
        now = 1_800_000_000.0
        start = int(now // 60) * 60 - 120
        row = [
            str(start * 1000), "100000", "100500", "99900", "100100",
            "120", "0.12", "12012", "1",
        ]
        candle = normalize_okx_candle(row, "1m", now_epoch=now)
        self.assertIsNotNone(candle)
        self.assertEqual(candle["time"], start)
        self.assertTrue(candle["is_closed"])
        self.assertAlmostEqual(candle["volume_btc"], 0.12)
        self.assertAlmostEqual(candle["volume_contracts"], 120.0)
        self.assertAlmostEqual(candle["turnover_usdt"], 12012.0)
        self.assertEqual(candle["source"], "OKX V5 public candles")

    def test_incomplete_current_bar_is_never_marked_closed(self):
        now = 1_800_000_000.0
        start = int(now // 300) * 300
        row = [
            str(start * 1000), "100000", "100500", "99900", "100100",
            "120", "0.12", "12012", "0",
        ]
        candle = normalize_okx_candle(row, "5m", now_epoch=now)
        self.assertIsNotNone(candle)
        self.assertFalse(candle["is_closed"])

    def test_invalid_ohlc_or_unknown_confirm_is_rejected(self):
        now = 1_800_000_000.0
        valid = [str(int(now * 1000)), "100", "110", "90", "105", "1", "0.01", "1.05", "1"]
        invalid_range = list(valid)
        invalid_range[2] = "80"
        invalid_confirm = list(valid)
        invalid_confirm[8] = "?"
        self.assertIsNone(normalize_okx_candle(invalid_range, "1m", now_epoch=now))
        self.assertIsNone(normalize_okx_candle(invalid_confirm, "1m", now_epoch=now))
        self.assertIsNone(normalize_okx_candle(valid[:8], "1m", now_epoch=now))

    @patch("okx_history.requests.get")
    def test_rest_falls_back_only_to_the_second_configured_official_host(self, get):
        blocked = Mock()
        blocked.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")
        okay = Mock()
        okay.raise_for_status.return_value = None
        start = 1_799_999_880
        okay.json.return_value = {
            "code": "0",
            "msg": "",
            "data": [[str(start * 1000), "100", "110", "90", "105", "1", "0.01", "1.05", "1"]],
        }
        get.side_effect = [blocked, okay]
        feed = OKXHistoryFeed(rest_base_urls=("https://openapi.okx.com", "https://www.okx.com"))
        candles, base = feed._get_candles("1m", "1m")
        self.assertEqual(base, "https://www.okx.com")
        self.assertEqual(len(candles), 1)
        self.assertEqual(get.call_count, 2)
        self.assertEqual(get.call_args_list[0].args[0], "https://openapi.okx.com/api/v5/market/candles")
        self.assertEqual(get.call_args_list[1].args[0], "https://www.okx.com/api/v5/market/candles")

    def test_failed_refresh_does_not_claim_old_candles_are_fresh(self):
        feed = OKXHistoryFeed()
        row = {
            "time": 1_799_999_880,
            "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0,
            "volume": 0.01, "volume_btc": 0.01, "volume_contracts": 1.0,
            "turnover_usdt": 1050.0, "interval": "1m", "is_closed": True,
            "source": "OKX V5 public candles",
        }
        with patch.object(feed, "_get_candles", return_value=([row], "https://openapi.okx.com")):
            self.assertTrue(all(feed.poll_once().values()))
        with feed.lock:
            feed.data["last_success_at"] = 123.0
        with patch.object(feed, "_get_candles", side_effect=RuntimeError("403 Forbidden")):
            result = feed.poll_once()
        self.assertFalse(any(result.values()))
        snap = feed.snapshot()
        self.assertEqual(snap["connection"], "stale")
        self.assertEqual(snap["rest_endpoint_health"], "stale")
        self.assertEqual(snap["last_success_at"], 123.0)
        self.assertEqual(len(snap["candles_by_interval"]["1m"]), 1)
        self.assertIn("403 Forbidden", snap["last_error"])

    def test_context_marks_okx_as_separate_candle_reference(self):
        feed = OKXHistoryFeed()
        context = feed.analyst_context(now_epoch=1_800_000_000)
        self.assertEqual(context["venue"], "OKX")
        self.assertEqual(context["instrument"], "BTC-USDT-SWAP")
        self.assertIn("not merged into Bybit or Delta", context["data_role"])
        self.assertIn("1m", context["history"])
        self.assertTrue(any("not merged" in item for item in context["limitations"]))


if __name__ == "__main__":
    unittest.main()
