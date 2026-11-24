import pytest

from premium_discount import (
    build_premium_discount_map,
    classify_price_in_dealing_range,
)


def _swings():
    return [
        {"index": 20, "kind": "low", "price": 90.0},
        {"index": 35, "kind": "high", "price": 110.0},
    ]


def test_map_uses_confirmed_swing_pair_and_classifies_premium():
    result = build_premium_discount_map(_swings(), 105.0)
    assert result["status"] == "AVAILABLE"
    assert result["range_low"] == pytest.approx(90.0)
    assert result["range_high"] == pytest.approx(110.0)
    assert result["equilibrium"] == pytest.approx(100.0)
    assert result["position_pct"] == pytest.approx(75.0)
    assert result["location"] == "PREMIUM"


def test_map_classifies_discount_and_equilibrium():
    discount = build_premium_discount_map(_swings(), 95.0)
    equilibrium = build_premium_discount_map(_swings(), 100.0)
    assert discount["location"] == "DISCOUNT"
    assert discount["position_pct"] == pytest.approx(25.0)
    assert equilibrium["location"] == "EQUILIBRIUM"
    assert equilibrium["position_pct"] == pytest.approx(50.0)


def test_prices_outside_range_are_not_mislabeled_premium_or_discount():
    above = build_premium_discount_map(_swings(), 115.0)
    below = build_premium_discount_map(_swings(), 85.0)
    assert above["location"] == "ABOVE RANGE"
    assert above["position_pct"] == pytest.approx(125.0)
    assert below["location"] == "BELOW RANGE"
    assert below["position_pct"] == pytest.approx(-25.0)


def test_pair_selection_prefers_most_recent_valid_pivot_pair():
    swings = _swings() + [
        {"index": 80, "kind": "high", "price": 120.0},
        {"index": 92, "kind": "low", "price": 95.0},
    ]
    result = build_premium_discount_map(swings, 100.0)
    assert result["range_high"] == pytest.approx(120.0)
    assert result["range_low"] == pytest.approx(95.0)


def test_map_is_unavailable_when_no_valid_confirmed_range_exists():
    result = build_premium_discount_map(
        [{"index": 10, "kind": "high", "price": 100.0}], 95.0
    )
    assert result["status"].startswith("UNAVAILABLE")
    assert result["equilibrium"] is None
    assert classify_price_in_dealing_range(95.0, result) == "UNAVAILABLE"


def test_pair_too_far_apart_is_not_used():
    result = build_premium_discount_map(_swings(), 100.0, max_pair_bars=10)
    assert result["status"].startswith("UNAVAILABLE")


def test_classify_price_in_dealing_range_uses_zone_half():
    dealing_range = build_premium_discount_map(_swings(), 100.0)
    assert classify_price_in_dealing_range(108.0, dealing_range) == "PREMIUM"
    assert classify_price_in_dealing_range(92.0, dealing_range) == "DISCOUNT"
    assert classify_price_in_dealing_range(100.0, dealing_range) == "EQUILIBRIUM"


@pytest.mark.parametrize(
    "price",
    [None, 0, -1, float("nan"), float("inf")],
)
def test_invalid_current_price_is_rejected(price):
    with pytest.raises(ValueError):
        build_premium_discount_map(_swings(), price)


@pytest.mark.parametrize("lookback", [0, -1, True, 1.5])
def test_invalid_pair_lookback_is_rejected(lookback):
    with pytest.raises(ValueError):
        build_premium_discount_map(_swings(), 100.0, max_pair_bars=lookback)
