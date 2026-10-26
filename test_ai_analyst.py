import json
import unittest
from datetime import datetime, timezone

from ai_analyst import (
    _extract_responses_api_text,
    _question_with_context,
    build_market_context,
)


class AiAnalystContextTests(unittest.TestCase):
    def setUp(self):
        self.now = 1_700_000_000.0
        minute = int(self.now // 60) * 60
        self.feed = {
            "connection": "connected",
            "book_synced": True,
            "sequence_gap_unresolved": False,
            "checksum_ok": True,
            "sequence_gaps": 0,
            "checksum_failures": 0,
            "total_disconnects": 1,
            "total_reconnects": 1,
            "channel_message_counts": {"ticker": 25, "trades": 500},
            "channel_last_seen": {"ticker": self.now - 2, "trades": self.now - 0.5},
            "last_price": 101.5,
            "mark_price": 101.4,
            "index_price": 101.0,
            "bid": 101.0,
            "ask": 102.0,
            "bid_size": 12,
            "ask_size": 8,
            "funding_rate": 0.0001,
            "funding_interval_seconds": 28800,
            "next_funding_at": self.now + 3600,
            "open_interest_contracts": 12345,
            "ticker_24h": {
                "open": 99, "high": 104, "low": 98, "close": 101,
                "mark_change_pct": 1.2,
            },
            "trade_count": 501,
            "buy_contracts": 700,
            "sell_contracts": 450,
            "cvd_value": 250,
            "asks": {"103.0": 4, "102.0": 8},
            "bids": {"99.0": 3, "101.0": 12},
            "trades": [
                {"timestamp": self.now, "side": "BUY TAKER", "buyer_role": "t", "price": 101.5, "size": 4},
                {"timestamp": self.now - 1, "side": "SELL TAKER", "buyer_role": "m", "price": 101, "size": 2},
            ],
            "candles": [
                {"time": int(self.now) - 60, "open": 100, "high": 102, "low": 99, "close": 101, "volume": 40}
            ],
            "trade_flow_minutes": {
                minute: {"buy_count": 3, "sell_count": 2, "buy_contracts": 10, "sell_contracts": 4},
                minute - 60: {"buy_count": 1, "sell_count": 4, "buy_contracts": 2, "sell_contracts": 9},
            },
            "large_trades": [
                {"ts": self.now - 5, "side": "buy", "price": 101, "size": 20, "threshold": 15}
            ],
            "trade_clusters": [
                {"side": "buy", "price": 101, "contracts": 40, "trade_count": 6, "ignored": "not included"}
            ],
        }

    def test_context_contains_live_metrics_and_derived_spread(self):
        context = build_market_context(self.feed, now_epoch=self.now)
        self.assertEqual(context["instrument"]["symbol"], "BTCUSD")
        self.assertEqual(context["instrument"]["product_id"], 27)
        self.assertEqual(context["price_and_quotes"]["bid"], 101)
        self.assertEqual(context["price_and_quotes"]["ask"], 102)
        self.assertEqual(context["price_and_quotes"]["spread_price_calculated"], 1)
        self.assertAlmostEqual(context["price_and_quotes"]["spread_bps_calculated"], 10000 / 101.5, places=3)
        self.assertEqual(context["funding"]["interval_seconds"], 28800)
        self.assertEqual(context["open_interest_contracts"], 12345)

    def test_book_is_sorted_best_first_and_recent_tape_is_bounded(self):
        context = build_market_context(self.feed, now_epoch=self.now)
        asks = context["order_book"]["asks_best_to_worse"]
        bids = context["order_book"]["bids_best_to_worse"]
        self.assertEqual([r["price"] for r in asks], [102, 103])
        self.assertEqual([r["price"] for r in bids], [101, 99])
        self.assertEqual(len(context["recent_trades"]), 2)
        self.assertEqual(context["recent_trades"][0]["buyer_role"], "t")

    def test_flow_context_marks_historical_validation_as_unavailable(self):
        context = build_market_context(self.feed, now_epoch=self.now)
        self.assertTrue(context["recent_15m_aggressor_flow"])
        self.assertEqual(context["recent_15m_aggressor_flow"][-1]["net_aggressor_contracts"], -1)
        self.assertFalse(context["historical_validation"]["historical_market_wide_trade_archive_in_this_context"])
        self.assertFalse(context["historical_validation"]["oos_profitability_certified_by_this_live_snapshot"])

    def test_context_is_json_serializable_and_rejects_non_finite_numbers(self):
        data = dict(self.feed)
        data["last_price"] = float("nan")
        context = build_market_context(data, now_epoch=self.now)
        self.assertIsNone(context["price_and_quotes"]["last_trade_price"])
        serialised = json.dumps(context, allow_nan=False)
        self.assertTrue(serialised)

    def test_question_includes_compact_context_and_question(self):
        context = build_market_context(self.feed, now_epoch=self.now)
        prompt = _question_with_context("Is the spread wide?", context)
        self.assertIn('"symbol":"BTCUSD"', prompt)
        self.assertIn("Is the spread wide?", prompt)
        self.assertIn("CURRENT DELTA EXCHANGE MARKET SNAPSHOT", prompt)

    def test_openai_responses_text_extraction_handles_output_content(self):
        payload = {
            "output": [
                {"type": "reasoning", "content": []},
                {"type": "message", "content": [
                    {"type": "output_text", "text": "Evidence first."},
                    {"type": "output_text", "text": "Then interpretation."},
                ]},
            ]
        }
        self.assertEqual(
            _extract_responses_api_text(payload),
            "Evidence first.\n\nThen interpretation.",
        )
        self.assertEqual(_extract_responses_api_text({"output_text": "Direct answer"}), "Direct answer")


if __name__ == "__main__":
    unittest.main()
