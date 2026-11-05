import pytest

from zone_strength import measure_zone_strength


def test_demand_zone_uses_origin_candle_tape_and_bid_depth():
    zone = {
        "direction": "bullish",
        "bottom": 100.0,
        "top": 110.0,
        "_mi": 1,
    }
    candles = [
        {"volume": 12},
        {"volume": 345},
    ]
    trades = [
        (1, 100.0, 10, "buy"),
        (2, 105.0, 5, "sell"),
        (3, 110.0, 2, "BUY"),
        (4, 111.0, 99, "buy"),
        (5, 105.0, 50, "unknown"),
    ]
    bids = {"100": "10", "105": "20", "110": "0", "99": "100"}
    asks = {"102": "4", "109": "6", "111": "100"}

    result = measure_zone_strength(
        zone, candles, trades, bids, asks, book_valid=True, contract_value_btc=0.001
    )

    assert result["formation_volume"] == 345
    assert result["buy"]["prints"] == 2
    assert result["buy"]["contracts"] == pytest.approx(12)
    assert result["buy"]["usd"] == pytest.approx((10 * 100 + 2 * 110) * 0.001)
    assert result["sell"]["prints"] == 1
    assert result["sell"]["contracts"] == pytest.approx(5)
    assert result["bid"]["levels"] == 2
    assert result["bid"]["contracts"] == pytest.approx(30)
    assert result["ask"]["levels"] == 2
    assert result["ask"]["contracts"] == pytest.approx(10)
    assert result["aligned_side"] == "BID"
    assert result["aligned_levels"] == 2
    assert result["aligned_opposing_usd_ratio"] == pytest.approx(3.0)


def test_supply_zone_uses_ask_depth_as_aligned_liquidity():
    zone = {"direction": "bearish", "bottom": 200.0, "top": 210.0, "_mi": 0}
    result = measure_zone_strength(
        zone,
        [{"volume": 7}],
        [],
        {"205": "10"},
        {"205": "20"},
        book_valid=True,
    )

    assert result["aligned_side"] == "ASK"
    assert result["aligned"]["contracts"] == pytest.approx(20)
    assert result["opposing"]["contracts"] == pytest.approx(10)
    assert result["aligned_opposing_usd_ratio"] == pytest.approx(2.0)


def test_resting_depth_is_unavailable_when_book_is_not_verified():
    zone = {"direction": "bullish", "bottom": 100.0, "top": 110.0}
    result = measure_zone_strength(
        zone, [], [], {"105": "10"}, {"105": "12"}, book_valid=False
    )

    assert result["book_available"] is False
    assert result["bid"] is None
    assert result["ask"] is None
    assert result["aligned"] is None
    assert result["aligned_opposing_usd_ratio"] is None


def test_empty_valid_book_reports_zero_levels_not_unavailable():
    zone = {"direction": "bullish", "bottom": 100.0, "top": 110.0}
    result = measure_zone_strength(zone, [], [], {}, {}, book_valid=True)

    assert result["book_available"] is True
    assert result["bid"]["levels"] == 0
    assert result["ask"]["levels"] == 0
    assert result["aligned_levels"] == 0
    assert result["aligned_opposing_usd_ratio"] is None


def test_invalid_or_outside_trades_and_levels_are_ignored():
    zone = {"direction": "bullish", "bottom": 100.0, "top": 110.0}
    result = measure_zone_strength(
        zone,
        [],
        [
            (1, 99.99, 5, "buy"),
            (2, 110.01, 5, "sell"),
            (3, float("nan"), 5, "buy"),
            (4, 105, -1, "sell"),
        ],
        {"99.9": "10", "105": "0", "108": "3"},
        {"111": "20"},
        book_valid=True,
    )

    assert result["buy"]["prints"] == 0
    assert result["sell"]["prints"] == 0
    assert result["bid"]["levels"] == 1
    assert result["bid"]["contracts"] == pytest.approx(3)
    assert result["ask"]["levels"] == 0


@pytest.mark.parametrize(
    "zone",
    [
        {"direction": "bullish", "bottom": 110.0, "top": 100.0},
        {"direction": "bullish", "bottom": float("nan"), "top": 100.0},
    ],
)
def test_invalid_zone_boundaries_are_rejected(zone):
    with pytest.raises(ValueError):
        measure_zone_strength(zone, [], [], {}, {}, book_valid=True)
