import pytest

from exchange_flows import parse_exchange_flow_series, parse_taker_buy_sell_series


def test_exchange_wallet_flow_values_join_to_matching_interval_price():
    rows = parse_exchange_flow_series(
        [
            {"date": "2026-11-04", "inflow_total": "12.5"},
            {"date": "2026-11-05", "inflow_total": "8"},
        ],
        [
            {"date": "2026-11-04", "outflow_total": "7.5"},
            {"date": "2026-11-05", "outflow_total": "10"},
        ],
        [
            {"date": "2026-11-04", "close": "100000"},
            {"date": "2026-11-05", "close": "102000"},
        ],
    )

    assert rows[0]["inflow_btc"] == pytest.approx(12.5)
    assert rows[0]["outflow_btc"] == pytest.approx(7.5)
    assert rows[0]["netflow_btc"] == pytest.approx(5.0)
    assert rows[0]["inflow_usd_est"] == pytest.approx(1_250_000)
    assert rows[0]["outflow_usd_est"] == pytest.approx(750_000)
    assert rows[0]["netflow_usd_est"] == pytest.approx(500_000)
    assert rows[1]["netflow_btc"] == pytest.approx(-2.0)
    assert rows[1]["netflow_usd_est"] == pytest.approx(-204_000)


def test_exchange_flow_missing_matching_price_does_not_use_latest_price():
    rows = parse_exchange_flow_series(
        [{"date": "2026-11-05", "inflow_total": "2"}],
        [{"date": "2026-11-05", "outflow_total": "1"}],
        [{"date": "2026-11-04", "close": "100000"}],
    )

    assert rows[0]["netflow_btc"] == pytest.approx(1)
    assert rows[0]["inflow_usd_est"] is None
    assert rows[0]["outflow_usd_est"] is None
    assert rows[0]["netflow_usd_est"] is None


def test_exchange_flow_preserves_partial_inflow_outflow_without_fabricating_netflow():
    rows = parse_exchange_flow_series(
        [{"date": "2026-11-05", "inflow_total": "3"}],
        [],
        [{"date": "2026-11-05", "close": "99000"}],
    )

    assert rows[0]["inflow_btc"] == pytest.approx(3)
    assert rows[0]["outflow_btc"] is None
    assert rows[0]["netflow_btc"] is None
    assert rows[0]["inflow_usd_est"] == pytest.approx(297_000)
    assert rows[0]["netflow_usd_est"] is None


def test_taker_buy_sell_values_are_normalized_as_usd_and_ratio():
    rows = parse_taker_buy_sell_series([
        {
            "date": "2026-11-05",
            "taker_buy_volume": "125000000",
            "taker_sell_volume": "100000000",
        }
    ])

    assert rows[0]["taker_buy_usd"] == pytest.approx(125_000_000)
    assert rows[0]["taker_sell_usd"] == pytest.approx(100_000_000)
    assert rows[0]["net_taker_usd"] == pytest.approx(25_000_000)
    assert rows[0]["buy_ratio_pct"] == pytest.approx(55.5555555556)
    assert rows[0]["sell_ratio_pct"] == pytest.approx(44.4444444444)
    assert rows[0]["buy_sell_ratio"] == pytest.approx(1.25)


def test_taker_buy_sell_zero_total_does_not_divide_by_zero():
    rows = parse_taker_buy_sell_series([
        {
            "datetime": "2026-11-05 10:00:00",
            "taker_buy_volume": 0,
            "taker_sell_volume": 0,
        }
    ])

    assert rows[0]["net_taker_usd"] == 0
    assert rows[0]["buy_ratio_pct"] is None
    assert rows[0]["sell_ratio_pct"] is None
    assert rows[0]["buy_sell_ratio"] is None


def test_malformed_and_negative_provider_rows_are_ignored():
    flow = parse_exchange_flow_series(
        [
            {"date": "2026-11-05", "inflow_total": -1},
            {"date": "2026-11-05", "inflow_total": "not-a-number"},
            {"inflow_total": "3"},
        ],
        [{"date": "2026-11-05", "outflow_total": 1}],
        [{"date": "2026-11-05", "close": 100_000}],
    )
    taker = parse_taker_buy_sell_series([
        {"date": "2026-11-05", "taker_buy_volume": -4, "taker_sell_volume": 1},
        {"date": "2026-11-05", "taker_buy_volume": "bad", "taker_sell_volume": 1},
        {"taker_buy_volume": 2, "taker_sell_volume": 1},
    ])

    assert flow[0]["inflow_btc"] is None
    assert flow[0]["netflow_btc"] is None
    assert len(taker) == 0


@pytest.mark.parametrize(
    ("flow_exchange", "taker_exchange", "flow_window", "taker_window", "limit"),
    [
        ("unknown", "all_exchange", "day", "day", 30),
        ("all_exchange", "unknown", "day", "day", 30),
        ("all_exchange", "all_exchange", "min", "day", 30),
        ("all_exchange", "all_exchange", "day", "block", 30),
        ("all_exchange", "all_exchange", "day", "day", 0),
    ],
)
def test_unsupported_fetch_options_are_rejected(
    flow_exchange, taker_exchange, flow_window, taker_window, limit
):
    from exchange_flows import fetch_exchange_flow_data

    with pytest.raises(ValueError):
        fetch_exchange_flow_data(
            "synthetic-key",
            flow_exchange=flow_exchange,
            taker_exchange=taker_exchange,
            flow_window=flow_window,
            taker_window=taker_window,
            limit=limit,
        )
