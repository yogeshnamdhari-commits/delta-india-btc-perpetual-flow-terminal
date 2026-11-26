import pytest

from daily_metrics import (
    parse_daily_volume_candles,
    parse_ticker_payload,
    summarize_aggressor_flow,
    summarize_funding_cap,
    summarize_oi_funding_bias,
    summarize_open_interest_24h,
    validate_and_normalize_open_interest_history,
)


def test_parse_ticker_payload_derives_contract_notional_and_flags_bad_exchange_value():
    result = parse_ticker_payload({
        "success": True,
        "result": {
            "symbol": "BTCUSD",
            "volume": 123456,
            "turnover": 987654321,
            "turnover_symbol": "USD",
            "oi": "602",
            "mark_price": "59541",
            "oi_value_usd": "35843701",
            "timestamp": 1793923200000000,
        },
    })

    # REST oi field is in thousands of contracts; fallback multiplies by 1000.
    calculated = 602 * 1000 * 0.001 * 59541
    assert result["symbol"] == "BTCUSD"
    assert result["volume"] == 123456.0
    assert result["turnover_usd"] == 987654321.0
    assert result["open_interest_contracts"] == 602000.0
    assert result["oi_field_contracts"] == 602.0
    assert result["oi_contracts_field"] is None
    assert result["open_interest_mark_price"] == 59541.0
    assert result["open_interest_value_usd"] == pytest.approx(calculated)
    assert result["reported_open_interest_value_usd"] == 35843701.0
    assert result["open_interest_value_usd_discrepancy_pct"] == pytest.approx(
        (35843701 / calculated - 1) * 100
    )
    assert result["timestamp"] == 1793923200


def test_parse_ticker_payload_uses_oi_contracts_field_when_present():
    result = parse_ticker_payload({
        "result": {
            "symbol": "BTCUSD",
            "volume": 123456,
            "turnover": 987654321,
            "turnover_symbol": "USD",
            "oi": "638.4310",
            "oi_contracts": "638431",
            "mark_price": "58753.56577027",
            "oi_value_usd": "37520526.0269",
            "timestamp": 1795624049978671,
        },
    })

    calculated = 638431 * 0.001 * 58753.56577027
    assert result["open_interest_contracts"] == 638431.0
    assert result["oi_field_contracts"] == 638.4310
    assert result["oi_contracts_field"] == 638431.0
    assert result["open_interest_value_usd"] == pytest.approx(calculated)
    # Discrepancy should be tiny once oi_contracts is used correctly.
    assert abs(result["open_interest_value_usd_discrepancy_pct"]) < 0.1


def test_parse_ticker_payload_does_not_use_unverified_reported_oi_value_without_mark_price():
    result = parse_ticker_payload({
        "result": {
            "symbol": "BTCUSD",
            "oi": "30",
            "oi_value_usd": "999999",
        },
    })
    assert result["open_interest_contracts"] == 30000
    assert result["open_interest_value_usd"] is None
    assert result["reported_open_interest_value_usd"] == 999999
    assert result["open_interest_mark_price"] is None
    assert result["open_interest_value_usd_discrepancy_pct"] is None


def test_parse_ticker_payload_selects_requested_symbol_from_list():
    payload = {
        "result": [
            {"symbol": "ETHUSD", "volume": 9},
            {"symbol": "BTCUSD", "volume": 10, "turnover_usd": 20, "oi": 30},
        ]
    }
    result = parse_ticker_payload(payload)
    assert result["volume"] == 10
    assert result["turnover_usd"] == 20
    assert result["open_interest_contracts"] == 30000
    assert result["oi_field_contracts"] == 30
    assert result["open_interest_value_usd"] is None


def test_parse_ticker_payload_does_not_invent_missing_or_invalid_values():
    result = parse_ticker_payload({"result": {"symbol": "BTCUSD", "volume": "nan", "oi": -1}})
    assert result["volume"] is None
    assert result["open_interest_contracts"] is None
    assert result["open_interest_value_usd"] is None
    assert result["turnover_usd"] is None


def test_parse_ticker_payload_rejects_invalid_contract_multiplier():
    with pytest.raises(ValueError):
        parse_ticker_payload({}, contract_value_btc=0)

def test_summarize_aggressor_flow_uses_only_rolling_24h_buckets():
    now = 200_000.0
    flow = {
        113_520: {"buy_count": 9, "buy_contracts": 900, "buy_usd": 9000},
        114_000: {"buy_count": 2, "sell_count": 1, "buy_contracts": 20,
                  "sell_contracts": 10, "buy_usd": 200, "sell_usd": 100},
        199_980: {"buy_count": 1, "sell_count": 2, "buy_contracts": 3,
                  "sell_contracts": 7, "buy_usd": 30, "sell_usd": 70},
    }

    result = summarize_aggressor_flow(
        flow, now_ts=now, feed_started_at=100_000, contract_value_btc=0.001
    )

    assert result["buy_count"] == 3
    assert result["sell_count"] == 3
    assert result["buy_contracts"] == pytest.approx(23)
    assert result["sell_contracts"] == pytest.approx(17)
    assert result["buy_btc"] == pytest.approx(0.023)
    assert result["sell_btc"] == pytest.approx(0.017)
    assert result["buy_usd"] == pytest.approx(230)
    assert result["sell_usd"] == pytest.approx(170)
    assert result["buy_share_pct"] == pytest.approx(57.5)
    assert result["sell_share_pct"] == pytest.approx(42.5)
    assert result["capture_hours"] == pytest.approx(24)
    assert result["minutes_with_trades"] == 2


def test_summarize_aggressor_flow_handles_empty_tape_and_unknown_window():
    result = summarize_aggressor_flow({}, now_ts=1000, feed_started_at=None)
    assert result["total_contracts"] == 0
    assert result["buy_share_pct"] == 0
    assert result["sell_share_pct"] == 0
    assert result["capture_hours"] is None


def test_summarize_aggressor_flow_rejects_invalid_contract_value():
    with pytest.raises(ValueError):
        summarize_aggressor_flow({}, now_ts=1000, feed_started_at=1, contract_value_btc=0)


def test_summarize_open_interest_24h_uses_nearest_hourly_close_before_cutoff():
    now = 200_000
    history = [
        {"time": 100_000, "close": "900"},
        {"time": 110_000, "close": "950"},
        {"time": 113_000, "close": "1000"},
        {"time": 114_000, "close": "1050"},  # after 24h cutoff; not the baseline
    ]
    result = summarize_open_interest_24h(history, 1200, now_ts=now)
    assert result["current_contracts"] == 1200
    assert result["baseline_contracts"] == 1000
    assert result["change_contracts"] == 200
    assert result["change_pct"] == pytest.approx(20)
    assert result["baseline_timestamp"] == 113000


def test_summarize_open_interest_24h_marks_stale_baseline_unavailable():
    result = summarize_open_interest_24h(
        [{"time": 1000, "close": 100}], 120, now_ts=100_000,
        max_baseline_age_seconds=3600,
    )
    assert result["baseline_contracts"] is None
    assert result["change_contracts"] is None
    assert result["change_pct"] is None


def test_summarize_open_interest_24h_avoids_percentage_division_by_zero():
    result = summarize_open_interest_24h(
        [{"time": 113_000, "close": 0}], 10, now_ts=200_000
    )
    assert result["change_contracts"] == 10
    assert result["change_pct"] is None


def test_parse_daily_volume_candles_sorts_and_discards_invalid_rows():
    rows = parse_daily_volume_candles({
        "result": [
            {"time": 300, "volume": "30"},
            {"time": 100, "volume": "10"},
            {"time": 200, "volume": "20"},
            {"time": 400, "volume": "nan"},
            {"time": -1, "volume": 10},
        ]
    })
    assert rows == [
        {"time": 100, "volume": 10.0},
        {"time": 200, "volume": 20.0},
        {"time": 300, "volume": 30.0},
    ]


def test_parse_daily_volume_candles_handles_empty_or_unexpected_payload():
    assert parse_daily_volume_candles({}) == []
    assert parse_daily_volume_candles({"result": "bad"}) == []


def test_summarize_oi_funding_bias_marks_rising_price_and_oi_as_buy_candidate():
    result = summarize_oi_funding_bias(
        price_change_pct=1.2,
        oi_change_pct=2.4,
        funding_rate=0.0001,
        funding_interval_seconds=28_800,
        oi_validation_passed=True,
    )
    assert result["context_bias"] == "BUY BIAS CANDIDATE"
    assert "PRICE UP + OI UP" in result["price_oi_regime"]
    assert result["funding_payer"] == "LONGS PAY SHORTS"
    assert result["funding_rate_pct"] == pytest.approx(0.01)
    assert result["funding_interval_hours"] == pytest.approx(8)
    assert result["is_directional_candidate"] is True
    assert "not an automatic SELL signal" in result["funding_context"]


def test_summarize_oi_funding_bias_marks_falling_price_and_rising_oi_as_sell_candidate():
    result = summarize_oi_funding_bias(
        price_change_pct=-1.1,
        oi_change_pct=1.8,
        funding_rate=-0.0002,
        funding_interval_seconds=14_400,
        oi_validation_passed=True,
    )
    assert result["context_bias"] == "SELL BIAS CANDIDATE"
    assert "PRICE DOWN + OI UP" in result["price_oi_regime"]
    assert result["funding_payer"] == "SHORTS PAY LONGS"
    assert result["funding_rate_pct"] == pytest.approx(-0.02)
    assert result["funding_interval_hours"] == pytest.approx(4)


def test_summarize_oi_funding_bias_does_not_promote_deleveraging_to_buy_or_sell():
    short_covering = summarize_oi_funding_bias(1.0, -0.8, 0.0, oi_validation_passed=True)
    long_deleveraging = summarize_oi_funding_bias(-1.0, -0.8, 0.0, oi_validation_passed=True)
    assert short_covering["context_bias"] == "MIXED / WAIT"
    assert "short covering" in short_covering["price_oi_regime"].lower()
    assert long_deleveraging["context_bias"] == "MIXED / WAIT"
    assert "long deleveraging" in long_deleveraging["price_oi_regime"].lower()


def test_summarize_oi_funding_bias_withholds_direction_when_inputs_missing_or_flat():
    missing = summarize_oi_funding_bias(None, 1.0, None, oi_validation_passed=True)
    flat = summarize_oi_funding_bias(0.02, 1.0, 0.0, oi_validation_passed=True)
    assert missing["context_bias"] == "UNAVAILABLE — PRICE/OI DATA MISSING"
    assert missing["funding_payer"] == "UNAVAILABLE"
    assert flat["context_bias"] == "MIXED / WAIT"
    assert flat["funding_payer"] == "NEAR ZERO"
    assert flat["is_directional_candidate"] is False


def test_summarize_oi_funding_bias_rejects_invalid_neutral_threshold():
    with pytest.raises(ValueError):
        summarize_oi_funding_bias(1.0, 1.0, 0.0, neutral_threshold_pct=-0.1)



def test_normalize_oi_history_detects_thousand_contract_scale_from_live_cross_checks():
    now = 1_800_000_000.0
    rows = [
        {"time": int(now - 86_400 - 600), "close": 678.2},
        {"time": int(now - 1_800), "close": 656.107},
    ]
    result = validate_and_normalize_open_interest_history(
        rows,
        current_oi_contracts=656_107,
        rest_oi=656.107,
        rest_oi_contracts=656_107,
        websocket_oi_contracts=656_090,
        now_ts=now,
    )

    assert result["valid"] is True
    assert result["source_values_valid"] is True
    assert result["history_scale_valid"] is True
    assert result["history_scale_factor"] == 1000.0
    assert result["rest_field_diff_pct"] == pytest.approx(0)
    assert result["rest_ws_diff_pct"] < 0.01
    assert result["normalized_history"][-1]["close"] == pytest.approx(656_107)
    assert result["normalized_history"][0]["close"] == pytest.approx(678_200)

    summary = summarize_open_interest_24h(
        result["normalized_history"], 656_107, now_ts=now
    )
    assert summary["baseline_contracts"] == pytest.approx(678_200)
    assert summary["change_contracts"] == pytest.approx(-22_093)
    assert summary["change_pct"] < 0
    assert summary["change_pct"] == pytest.approx((656_107 / 678_200 - 1) * 100)


def test_normalize_oi_history_accepts_already_raw_contract_candles():
    now = 1_800_000_000.0
    result = validate_and_normalize_open_interest_history(
        [
            {"time": int(now - 86_400 - 300), "close": 678_200},
            {"time": int(now - 1_200), "close": 656_107},
        ],
        current_oi_contracts=656_107,
        rest_oi=656.107,
        rest_oi_contracts=656_107,
        websocket_oi_contracts=656_100,
        now_ts=now,
    )
    assert result["valid"] is True
    assert result["history_scale_factor"] == 1.0
    assert result["normalized_history"][-1]["close"] == pytest.approx(656_107)


def test_normalize_oi_history_fails_closed_on_rest_contract_mismatch():
    now = 1_800_000_000.0
    result = validate_and_normalize_open_interest_history(
        [{"time": int(now - 600), "close": 656.1}],
        current_oi_contracts=656_107,
        rest_oi=656.107,
        rest_oi_contracts=600_000,
        websocket_oi_contracts=656_107,
        now_ts=now,
    )
    assert result["valid"] is False
    assert result["normalized_history"] == []
    assert "REST oi × 1000" in result["reason"]


def test_normalize_oi_history_fails_closed_on_rest_websocket_mismatch():
    now = 1_800_000_000.0
    result = validate_and_normalize_open_interest_history(
        [{"time": int(now - 600), "close": 656.1}],
        current_oi_contracts=656_107,
        rest_oi=656.107,
        rest_oi_contracts=656_107,
        websocket_oi_contracts=700_000,
        now_ts=now,
    )
    assert result["valid"] is False
    assert result["normalized_history"] == []
    assert "WebSocket oi[0]" in result["reason"]


def test_normalize_oi_history_fails_closed_when_latest_candle_is_stale():
    now = 1_800_000_000.0
    result = validate_and_normalize_open_interest_history(
        [{"time": int(now - 7_201), "close": 656.1}],
        current_oi_contracts=656_107,
        rest_oi=656.107,
        rest_oi_contracts=656_107,
        websocket_oi_contracts=656_107,
        now_ts=now,
    )
    assert result["valid"] is False
    assert result["normalized_history"] == []
    assert "stale" in result["reason"]


def test_normalize_oi_history_fails_closed_when_no_candidate_scale_aligns():
    now = 1_800_000_000.0
    result = validate_and_normalize_open_interest_history(
        [{"time": int(now - 600), "close": 400}],
        current_oi_contracts=656_107,
        rest_oi=656.107,
        rest_oi_contracts=656_107,
        websocket_oi_contracts=656_107,
        now_ts=now,
    )
    assert result["valid"] is False
    assert result["normalized_history"] == []
    assert "does not align" in result["reason"]



def test_oi_funding_bias_defaults_to_unavailable_without_explicit_validation():
    result = summarize_oi_funding_bias(
        price_change_pct=2.0,
        oi_change_pct=3.0,
        funding_rate=0.001,
    )
    assert result["context_bias"] == "UNAVAILABLE — OI SCALE / BASELINE NOT VALIDATED"
    assert result["is_directional_candidate"] is False

def test_oi_funding_bias_is_suppressed_when_scale_or_baseline_is_unverified():
    result = summarize_oi_funding_bias(
        price_change_pct=2.87,
        oi_change_pct=96_642.98,
        funding_rate=0.0090042,
        funding_interval_seconds=28_800,
        oi_validation_passed=False,
    )
    assert result["context_bias"] == "UNAVAILABLE — OI SCALE / BASELINE NOT VALIDATED"
    assert result["is_directional_candidate"] is False
    assert result["oi_validation_passed"] is False


def test_funding_cap_warning_flags_ninety_percent_utilization():
    result = summarize_funding_cap(
        funding_rate=0.0090042,
        funding_cap_pct=1.0,
        funding_interval_seconds=28_800,
    )
    assert result["status"] == "NEAR CAP"
    assert result["funding_rate_pct"] == pytest.approx(0.90042)
    assert result["cap_utilization_pct"] == pytest.approx(90.042)
    assert result["funding_interval_hours"] == pytest.approx(8)


def test_funding_cap_warning_handles_approaching_normal_at_cap_and_negative_rates():
    approaching = summarize_funding_cap(0.008, 1.0, 28_800)
    normal = summarize_funding_cap(-0.001, 1.0, 28_800)
    at_cap = summarize_funding_cap(-0.01, 1.0, 28_800)
    unavailable = summarize_funding_cap(None, 1.0, 28_800)

    assert approaching["status"] == "APPROACHING CAP"
    assert normal["status"] == "NORMAL"
    assert at_cap["status"] == "AT OR ABOVE CAP"
    assert at_cap["cap_utilization_pct"] == pytest.approx(100.0)
    assert unavailable["status"] == "UNAVAILABLE"


def test_funding_cap_warning_rejects_invalid_product_cap_or_thresholds():
    with pytest.raises(ValueError):
        summarize_funding_cap(0.001, 0.0)
    with pytest.raises(ValueError):
        summarize_funding_cap(0.001, 1.0, warning_utilization_pct=95, high_utilization_pct=90)
