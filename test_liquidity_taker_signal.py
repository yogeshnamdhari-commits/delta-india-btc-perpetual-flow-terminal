import pytest

from liquidity_taker_signal import calculate_liquidity_taker_signal


NOW = 1_800_000_000


def trades(*, buy=0, sell=0, buy_size=10.0, sell_size=10.0, age=10):
    rows = []
    for index in range(buy):
        rows.append((NOW - age + index * 0.01, 100_000.0, buy_size, "buy"))
    for index in range(sell):
        rows.append((NOW - age + index * 0.01, 100_000.0, sell_size, "sell"))
    return rows


def test_buy_taker_signal_means_buyers_take_ask_liquidity():
    result = calculate_liquidity_taker_signal(trades(buy=12, sell=4), now_ts=NOW)
    primary = result["primary"]

    assert primary["signal_code"] == "BUYERS_TAKING_ASK"
    assert primary["signal"] == "BUYERS TAKING ASK LIQUIDITY"
    assert primary["intensity"] == "STRONG"
    assert primary["buy_contracts"] == pytest.approx(120.0)
    assert primary["sell_contracts"] == pytest.approx(40.0)
    assert primary["buy_share_pct"] == pytest.approx(75.0)
    assert primary["dominance_ratio"] == pytest.approx(3.0)


def test_sell_taker_signal_means_sellers_take_bid_liquidity():
    result = calculate_liquidity_taker_signal(trades(buy=3, sell=12), now_ts=NOW)
    primary = result["primary"]

    assert primary["signal_code"] == "SELLERS_TAKING_BID"
    assert primary["signal"] == "SELLERS TAKING BID LIQUIDITY"
    assert primary["intensity"] == "STRONG"
    assert primary["sell_contracts"] == pytest.approx(120.0)
    assert primary["buy_contracts"] == pytest.approx(30.0)
    assert primary["sell_share_pct"] == pytest.approx(80.0)


def test_moderate_directional_pressure_requires_sixty_percent_share():
    result = calculate_liquidity_taker_signal(
        trades(buy=9, sell=6), now_ts=NOW, min_prints=10
    )
    primary = result["primary"]

    assert primary["signal_code"] == "BUYERS_TAKING_ASK"
    assert primary["intensity"] == "MODERATE"
    assert primary["buy_share_pct"] == pytest.approx(60.0)


def test_balanced_trade_volume_does_not_force_a_direction():
    result = calculate_liquidity_taker_signal(trades(buy=8, sell=8), now_ts=NOW)
    primary = result["primary"]

    assert primary["signal_code"] == "BALANCED"
    assert primary["intensity"] == "BALANCED"
    assert primary["dominant_side"] is None


def test_insufficient_print_sample_does_not_emit_directional_signal():
    result = calculate_liquidity_taker_signal(trades(buy=4, sell=2), now_ts=NOW)
    primary = result["primary"]

    assert primary["signal_code"] == "INSUFFICIENT_SAMPLE"
    assert primary["intensity"] == "UNAVAILABLE"


def test_zero_recent_prints_are_reported_as_no_data():
    result = calculate_liquidity_taker_signal([], now_ts=NOW)
    primary = result["primary"]

    assert primary["signal_code"] == "NO_RECENT_PRINTS"
    assert primary["classified_prints"] == 0
    assert primary["dominant_share_pct"] is None


def test_window_filter_excludes_old_and_future_prints():
    rows = [
        (NOW - 301, 100_000.0, 100.0, "buy"),
        (NOW + 10, 100_000.0, 100.0, "buy"),
        *trades(buy=2, sell=8, sell_size=20.0),
    ]
    result = calculate_liquidity_taker_signal(rows, now_ts=NOW)
    primary = result["primary"]

    assert primary["buy_prints"] == 2
    assert primary["sell_prints"] == 8
    assert primary["buy_contracts"] == pytest.approx(20.0)
    assert primary["sell_contracts"] == pytest.approx(160.0)


def test_approximate_btc_and_usd_notional_use_contract_multiplier():
    result = calculate_liquidity_taker_signal(
        [(NOW - 1, 100_000.0, 10.0, "buy")] * 10,
        now_ts=NOW,
        contract_value_btc=0.001,
    )
    primary = result["primary"]

    assert primary["buy_btc"] == pytest.approx(0.1)
    assert primary["buy_usd"] == pytest.approx(10_000.0)


def test_one_sided_flow_reports_share_without_infinite_ratio():
    result = calculate_liquidity_taker_signal(trades(buy=10), now_ts=NOW)
    primary = result["primary"]

    assert primary["signal_code"] == "BUYERS_TAKING_ASK"
    assert primary["dominant_share_pct"] == pytest.approx(100.0)
    assert primary["dominance_ratio"] is None
    assert primary["intensity"] == "STRONG"


def test_one_five_and_fifteen_minute_contexts_are_returned():
    rows = [
        (NOW - 700, 100_000.0, 5.0, "sell"),
        *trades(buy=12, sell=3, age=30),
    ]
    result = calculate_liquidity_taker_signal(rows, now_ts=NOW)

    assert [item["window"] for item in result["windows"]] == ["1m", "5m", "15m"]
    assert result["primary"]["window"] == "5m"
    assert result["timeframe_alignment"] in ("ALIGNED", "PARTIAL", "MIXED", "NO CONFIRMATION")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"now_ts": float("nan")},
        {"contract_value_btc": 0},
        {"min_prints": 0},
        {"dominance_share_pct": 50},
        {"dominance_share_pct": 101},
        {"windows": ()},
        {"windows": (60, 0)},
    ],
)
def test_invalid_configuration_is_rejected(kwargs):
    params = {"now_ts": NOW}
    params.update(kwargs)
    with pytest.raises(ValueError):
        calculate_liquidity_taker_signal([], **params)


def test_malformed_and_unclassified_records_are_ignored():
    rows = [
        (NOW - 1, 100_000.0, 10, "unknown"),
        (NOW - 1, float("nan"), 10, "buy"),
        (NOW - 1, 100_000.0, -1, "sell"),
        ("bad", 100_000.0, 2, "buy"),
        (NOW - 1, 100_000.0, 1, "buy"),
    ]
    result = calculate_liquidity_taker_signal(rows, now_ts=NOW)

    assert result["primary"]["classified_prints"] == 1
    assert result["primary"]["signal_code"] == "INSUFFICIENT_SAMPLE"
