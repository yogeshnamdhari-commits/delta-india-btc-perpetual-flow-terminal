import pytest

from daily_metrics import (
    parse_daily_volume_candles,
    parse_ticker_payload,
    summarize_aggressor_flow,
    summarize_open_interest_24h,
)


def test_parse_ticker_payload_normalizes_official_24h_fields():
    result = parse_ticker_payload({
        "success": True,
        "result": {
            "symbol": "BTCUSD",
            "volume": 123456,
            "turnover": 987654321,
            "turnover_symbol": "USD",
            "oi": "250000",
            "oi_value_usd": "1250000000",
            "timestamp": 1793923200000000,
        },
    })

    assert result == {
        "symbol": "BTCUSD",
        "volume_contracts": 123456.0,
        "turnover_usd": 987654321.0,
        "open_interest_contracts": 250000.0,
        "open_interest_value_usd": 1250000000.0,
        "timestamp": 1793923200,
    }


def test_parse_ticker_payload_selects_requested_symbol_from_list():
    payload = {
        "result": [
            {"symbol": "ETHUSD", "volume": 9},
            {"symbol": "BTCUSD", "volume": 10, "turnover_usd": 20, "oi": 30},
        ]
    }
    result = parse_ticker_payload(payload)
    assert result["volume_contracts"] == 10
    assert result["turnover_usd"] == 20
    assert result["open_interest_contracts"] == 30


def test_parse_ticker_payload_does_not_invent_missing_or_invalid_values():
    result = parse_ticker_payload({"result": {"symbol": "BTCUSD", "volume": "nan", "oi": -1}})
    assert result["volume_contracts"] is None
    assert result["open_interest_contracts"] is None
    assert result["turnover_usd"] is None


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
