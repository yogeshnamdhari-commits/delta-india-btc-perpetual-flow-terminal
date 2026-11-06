import pytest

from multi_timeframe_orderflow import (
    DEFAULT_TIMEFRAMES,
    summarize_aligned_taker_flow,
)


def _flow_for_minute(minute, *, buy=0, sell=0, buy_size=0.0, sell_size=0.0, price=100_000.0):
    return {
        "buy_count": buy,
        "sell_count": sell,
        "buy_contracts": buy_size,
        "sell_contracts": sell_size,
        "buy_usd": buy_size * 0.001 * price,
        "sell_usd": sell_size * 0.001 * price,
    }


def _healthy_args(now, started):
    return {
        "now_ts": now,
        "feed_started_at": started,
        "connection_status": "connected",
        "trade_channel_last_seen": now - 1,
    }


def test_default_timeframes_are_six_requested_intervals():
    assert DEFAULT_TIMEFRAMES == (
        ("1m", 60),
        ("5m", 300),
        ("15m", 900),
        ("1h", 3600),
        ("4h", 14400),
        ("1d", 86400),
    )


def test_completed_periods_are_utc_aligned_and_exclude_current_partial_bar():
    now = 1_800_000_000 + 7 * 60 + 25
    period_end = (int(now) // 300) * 300
    period_start = period_end - 300
    flow = {
        period_start: _flow_for_minute(period_start, buy=6, sell=4, buy_size=60, sell_size=40),
        period_end: _flow_for_minute(period_end, buy=9, sell=0, buy_size=900),
    }

    result = summarize_aligned_taker_flow(
        flow,
        **_healthy_args(now, period_start - 60),
        timeframes=(("5m", 300),),
    )
    row = result["windows"][0]

    assert row["period_start"] == period_start
    assert row["period_end"] == period_end
    assert row["classified_prints"] == 10
    assert row["buy_contracts"] == pytest.approx(60)
    assert row["sell_contracts"] == pytest.approx(40)
    assert row["net_delta_contracts"] == pytest.approx(20)
    assert row["buy_share_pct"] == pytest.approx(60)
    assert row["classification"] == "BUYER DOMINANCE"
    assert row["total_btc"] == pytest.approx(0.1)
    assert row["total_usd"] == pytest.approx(10_000)


def test_partial_capture_does_not_emit_directional_classification():
    now = 1_800_000_000 + 20
    period_end = (int(now) // 60) * 60
    period_start = period_end - 60
    flow = {
        period_start: _flow_for_minute(period_start, buy=20, buy_size=200),
    }

    result = summarize_aligned_taker_flow(
        flow,
        **_healthy_args(now, period_start + 5),
        timeframes=(("1m", 60),),
    )
    row = result["windows"][0]

    assert row["buy_contracts"] == pytest.approx(200)
    assert row["classification"] == "UNVERIFIED CAPTURE"
    assert row["capture_status"] == "PARTIAL — APP STARTED MID-WINDOW"
    assert row["relative_volume"] is None


def test_stale_trade_channel_does_not_emit_directional_classification():
    now = 1_800_000_000 + 20
    period_end = (int(now) // 60) * 60
    period_start = period_end - 60
    flow = {
        period_start: _flow_for_minute(period_start, buy=20, buy_size=200),
    }
    args = _healthy_args(now, period_start - 60)
    args["trade_channel_last_seen"] = now - 30

    result = summarize_aligned_taker_flow(
        flow, **args, timeframes=(("1m", 60),)
    )
    row = result["windows"][0]

    assert row["classification"] == "UNVERIFIED CAPTURE"
    assert row["capture_status"] == "TRADE CHANNEL STALE"


def test_sell_dominance_and_volume_metrics_are_calculated_from_trade_aggregates():
    now = 1_800_000_000 + 20
    period_end = (int(now) // 60) * 60
    period_start = period_end - 60
    flow = {
        period_start: _flow_for_minute(
            period_start, buy=3, sell=12, buy_size=30, sell_size=120
        ),
    }

    result = summarize_aligned_taker_flow(
        flow,
        **_healthy_args(now, period_start - 60),
        timeframes=(("1m", 60),),
    )
    row = result["windows"][0]

    assert row["classification"] == "SELLER DOMINANCE"
    assert row["intensity"] == "STRONG"
    assert row["sell_share_pct"] == pytest.approx(80)
    assert row["dominance_ratio"] == pytest.approx(4)
    assert row["net_delta_contracts"] == pytest.approx(-90)
    assert row["sell_btc"] == pytest.approx(0.12)


def test_relative_volume_requires_twenty_prior_full_same_duration_bars():
    now = 1_800_000_000 + 20
    period_end = (int(now) // 60) * 60
    period_start = period_end - 60
    prior_start = period_start - 20 * 60
    flow = {
        minute: _flow_for_minute(minute, buy=10, buy_size=10)
        for minute in range(prior_start, period_start, 60)
    }
    flow[period_start] = _flow_for_minute(period_start, buy=10, buy_size=200)

    result = summarize_aligned_taker_flow(
        flow,
        **_healthy_args(now, prior_start),
        timeframes=(("1m", 60),),
    )
    row = result["windows"][0]

    assert row["relative_volume_sample_count"] == 20
    assert row["average_prior_contracts"] == pytest.approx(10)
    assert row["relative_volume"] == pytest.approx(20)
    assert row["relative_volume_status"] == "VS PRIOR 20 COMPLETED BARS"


def test_relative_volume_is_unavailable_until_twenty_prior_bars_are_captured():
    now = 1_800_000_000 + 20
    period_end = (int(now) // 60) * 60
    period_start = period_end - 60
    started = period_start - 5 * 60
    flow = {
        minute: _flow_for_minute(minute, buy=10, buy_size=10)
        for minute in range(started, period_start, 60)
    }
    flow[period_start] = _flow_for_minute(period_start, buy=10, buy_size=20)

    result = summarize_aligned_taker_flow(
        flow,
        **_healthy_args(now, started),
        timeframes=(("1m", 60),),
    )
    row = result["windows"][0]

    assert row["relative_volume"] is None
    assert row["relative_volume_sample_count"] == 5
    assert row["relative_volume_status"] == "NEEDS 20 PRIOR BARS (5/20 CAPTURED)"


def test_empty_window_is_not_mislabeled_as_zero_volume_signal():
    now = 1_800_000_000 + 20
    period_end = (int(now) // 60) * 60
    period_start = period_end - 60

    result = summarize_aligned_taker_flow(
        {},
        **_healthy_args(now, period_start - 60),
        timeframes=(("1m", 60),),
    )
    row = result["windows"][0]

    assert row["classified_prints"] == 0
    assert row["classification"] == "NO CLASSIFIED PRINTS"
    assert row["buy_share_pct"] is None
    assert row["sell_share_pct"] is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"now_ts": float("nan")},
        {"contract_value_btc": 0},
        {"min_prints": 0},
        {"dominance_share_pct": 50},
        {"relative_volume_lookback": 0},
        {"max_trade_age_seconds": 0},
        {"retention_seconds": 0},
        {"timeframes": ()},
        {"timeframes": (("bad", 61),)},
        {"timeframes": (("1m", 60), ("1m", 120))},
    ],
)
def test_invalid_configuration_is_rejected(kwargs):
    args = {
        "now_ts": 1_800_000_000,
        "feed_started_at": 1_799_999_000,
        "connection_status": "connected",
        "trade_channel_last_seen": 1_800_000_000,
    }
    args.update(kwargs)
    with pytest.raises(ValueError):
        summarize_aligned_taker_flow({}, **args)


def test_minimum_sample_and_dominance_threshold_are_not_lowered():
    now = 1_800_000_000 + 20
    period_end = (int(now) // 60) * 60
    period_start = period_end - 60
    flow = {
        period_start: _flow_for_minute(
            period_start, buy=8, sell=2, buy_size=58, sell_size=42
        ),
    }

    result = summarize_aligned_taker_flow(
        flow,
        **_healthy_args(now, period_start - 60),
        timeframes=(("1m", 60),),
    )
    row = result["windows"][0]

    assert row["classified_prints"] == 10
    assert row["buy_share_pct"] == pytest.approx(58)
    assert row["classification"] == "BALANCED"
    assert row["intensity"] == "BALANCED"
