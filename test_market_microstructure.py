import pytest

from market_microstructure import analyze_orderbook_events, summarize_daily_utc_flow


def test_daily_utc_flow_reports_buy_sell_contracts_and_btc_equivalent():
    now = 1_800_000_000.0
    day = int(now // 86_400) * 86_400
    flow = {
        day: {
            "buy_count": 4, "sell_count": 2,
            "buy_contracts": 120, "sell_contracts": 80,
            "buy_usd": 12_000, "sell_usd": 8_000,
        }
    }
    result = summarize_daily_utc_flow(
        flow, now_ts=now, feed_started_at=day - 60,
        trade_last_seen=now - 1, connection_status="connected", days=1,
    )
    row = result["days"][0]
    assert row["buy_prints"] == 4
    assert row["sell_prints"] == 2
    assert row["buy_contracts"] == pytest.approx(120)
    assert row["sell_contracts"] == pytest.approx(80)
    assert row["buy_btc"] == pytest.approx(0.12)
    assert row["sell_btc"] == pytest.approx(0.08)
    assert row["net_delta_contracts"] == pytest.approx(40)
    assert row["captured_by_process"] is True


def test_daily_flow_marks_previous_day_unobserved_instead_of_fabricating_zero():
    now = 1_800_000_000.0
    day = int(now // 86_400) * 86_400
    result = summarize_daily_utc_flow(
        {day: {"buy_count": 1, "buy_contracts": 10}},
        now_ts=now, feed_started_at=day + 1, trade_last_seen=now - 1,
        connection_status="connected", days=2,
    )
    previous, current = result["days"]
    assert previous["captured_by_process"] is False
    assert previous["buy_contracts"] is None
    assert previous["capture_status"] == "NOT OBSERVED BY THIS PROCESS"
    assert current["captured_by_process"] is True


def test_daily_flow_exposes_partial_day_coverage_and_stale_channel():
    now = 1_800_000_000.0
    day = int(now // 86_400) * 86_400
    result = summarize_daily_utc_flow(
        {}, now_ts=now, feed_started_at=day + 43_200,
        trade_last_seen=now - 60, connection_status="connected", days=1,
    )
    row = result["days"][0]
    assert row["app_window_coverage_pct"] == pytest.approx(50.0)
    assert "PARTIAL UTC DAY" in row["capture_status"]
    assert row["buy_contracts"] == pytest.approx(0.0)


def _snapshot(ts, *, bids=None, asks=None):
    return {
        "ts": float(ts),
        "bids": bids if bids is not None else {100.0: 100.0, 99.5: 100.0},
        "asks": asks if asks is not None else {101.0: 100.0, 101.5: 100.0},
    }


def test_unexplained_large_bid_reduction_emits_candidate_not_proof():
    rows = [
        _snapshot(100, bids={100.0: 2_000.0, 99.5: 100.0}),
        _snapshot(102, bids={99.5: 100.0}),
        _snapshot(104, bids={99.5: 100.0}),
    ]
    result = analyze_orderbook_events(
        rows, [], now_ts=104, book_valid=True,
        min_large_contracts=500, max_snapshot_age_seconds=3,
    )
    assert result["status"] == "LIVE SAMPLED BOOK — CANDIDATE ANALYSIS"
    assert result["quote_pull_candidates"]
    candidate = result["quote_pull_candidates"][0]
    assert candidate["side"] == "BID"
    assert candidate["price"] == pytest.approx(100.0)
    assert candidate["displayed_drop_pct"] == pytest.approx(100.0)
    assert "CANDIDATE" in candidate["classification"]


def test_matched_sell_taker_execution_suppresses_bid_quote_pull_candidate():
    rows = [
        _snapshot(100, bids={100.0: 2_000.0, 99.5: 100.0}),
        _snapshot(102, bids={99.5: 100.0}),
        _snapshot(104, bids={99.5: 100.0}),
    ]
    trades = [(101.0, 100.0, 1_900.0, "sell")]
    result = analyze_orderbook_events(
        rows, trades, now_ts=104, book_valid=True,
        min_large_contracts=500, max_snapshot_age_seconds=3,
    )
    assert result["quote_pull_candidates"] == []


def test_repeated_execution_with_stable_visible_ask_size_emits_iceberg_like_candidate():
    rows = [_snapshot(100), _snapshot(102), _snapshot(104)]
    trades = [
        (101.0, 101.0, 80.0, "buy"),
        (103.0, 101.0, 80.0, "buy"),
    ]
    result = analyze_orderbook_events(
        rows, trades, now_ts=104, book_valid=True,
        min_large_contracts=50, min_refill_execution_contracts=25,
        max_snapshot_age_seconds=3,
    )
    assert result["iceberg_refill_candidates"]
    candidate = result["iceberg_refill_candidates"][0]
    assert candidate["side"] == "ASK"
    assert candidate["price"] == pytest.approx(101.0)
    assert candidate["refill_intervals"] >= 2
    assert "CANDIDATE" in candidate["classification"]


def test_unverified_or_stale_book_never_emits_spoof_or_iceberg_candidates():
    rows = [_snapshot(100), _snapshot(102), _snapshot(104)]
    for kwargs in (
        {"book_valid": False, "now_ts": 104},
        {"book_valid": True, "now_ts": 120},
    ):
        result = analyze_orderbook_events(rows, [], **kwargs)
        assert result["quote_pull_candidates"] == []
        assert result["iceberg_refill_candidates"] == []
        assert result["status"].startswith("UNAVAILABLE")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"now_ts": float("nan")},
        {"tick_size": 0},
        {"min_large_contracts": 0},
        {"min_refill_execution_contracts": 0},
        {"max_snapshot_gap_seconds": 0},
        {"max_snapshot_age_seconds": 0},
        {"lookback_seconds": 0},
        {"min_refill_events": 1},
    ],
)
def test_invalid_microstructure_configuration_is_rejected(kwargs):
    args = {"now_ts": 104.0, "book_valid": True}
    args.update(kwargs)
    with pytest.raises(ValueError):
        analyze_orderbook_events([], [], **args)
