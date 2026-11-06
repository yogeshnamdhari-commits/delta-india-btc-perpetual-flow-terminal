import pytest

from liquidity_effectiveness import build_liquidity_effectiveness, evaluate_level_retests


def baseline_candles(count=40):
    return [
        {
            "time": index * 60,
            "open": 99.0,
            "high": 99.2,
            "low": 98.0,
            "close": 99.0,
            "volume": 10,
        }
        for index in range(count)
    ]


def test_sweep_reclaim_followed_by_atr_rejection_counts_as_effective():
    candles = baseline_candles()
    candles[17].update(high=100.6, low=99.0, close=99.7)
    candles[18].update(high=99.4, low=99.3, close=99.35)

    result = evaluate_level_retests(
        candles,
        level=100.0,
        kind="high",
        confirm_index=15,
        tolerance_points=0.5,
        horizon_bars=5,
    )

    assert result["attempts"] == 1
    assert result["sweeps"] == 1
    assert result["successes"] == 1
    assert result["failures"] == 0
    assert result["effectiveness_pct"] == pytest.approx(100.0)
    assert result["last_outcome"] == "REJECTION"


def test_two_closes_beyond_level_count_as_breakout_failure():
    candles = baseline_candles()
    candles[17].update(high=100.6, low=99.8, close=100.2)
    candles[18].update(high=100.5, low=100.0, close=100.4)
    candles[19].update(high=100.6, low=100.0, close=100.4)

    result = evaluate_level_retests(
        candles,
        level=100.0,
        kind="high",
        confirm_index=15,
        tolerance_points=0.5,
        horizon_bars=5,
    )

    assert result["attempts"] == 1
    assert result["successes"] == 0
    assert result["failures"] == 1
    assert result["effectiveness_pct"] == pytest.approx(0.0)
    assert result["last_outcome"] == "BREAK_ACCEPTED"


def test_incomplete_forward_window_is_pending_not_counted_as_failure():
    candles = baseline_candles(19)
    candles[17].update(high=100.6, low=99.8, close=100.2)

    result = evaluate_level_retests(
        candles,
        level=100.0,
        kind="high",
        confirm_index=15,
        tolerance_points=0.5,
        horizon_bars=5,
    )

    assert result["attempts"] == 1
    assert result["pending"] == 1
    assert result["resolved_samples"] == 0
    assert result["effectiveness_pct"] is None


def test_preconfirmation_price_action_is_not_counted_as_a_retest():
    candles = baseline_candles()
    candles[12].update(high=101.0, low=98.0, close=99.0)

    result = evaluate_level_retests(
        candles,
        level=100.0,
        kind="high",
        confirm_index=15,
        tolerance_points=0.5,
        horizon_bars=5,
    )

    assert result["attempts"] == 0
    assert result["last_outcome"] == "UNTESTED"


def test_low_level_rejection_measures_upward_reaction():
    candles = baseline_candles()
    # Mirror the fixture around 100: low-side liquidity is tested from above.
    mirrored = []
    for candle in candles:
        mirrored.append({
            **candle,
            "open": 101.0,
            "high": 102.0,
            "low": 100.8,
            "close": 101.0,
        })
    mirrored[17].update(high=101.0, low=99.4, close=100.3)
    mirrored[18].update(high=100.7, low=100.6, close=100.65)

    result = evaluate_level_retests(
        mirrored,
        level=100.0,
        kind="low",
        confirm_index=15,
        tolerance_points=0.5,
        horizon_bars=5,
    )

    assert result["attempts"] == 1
    assert result["sweeps"] == 1
    assert result["successes"] == 1
    assert result["last_outcome"] == "REJECTION"


def test_builder_returns_buy_and_sell_side_levels_and_discloses_sample_basis():
    candles = []
    for index in range(160):
        center = 100.0 + (index % 20 in (8, 9, 10)) * 4.0
        candles.append({
            "time": index * 60,
            "open": center,
            "high": center + (2.0 if index % 20 in (9, 10) else 0.6),
            "low": center - (2.0 if index % 20 in (0, 1) else 0.6),
            "close": center + (0.2 if index % 2 else -0.2),
            "volume": 10.0,
        })

    result = build_liquidity_effectiveness(
        candles, current_price=100.0, tolerance_points=2.0, limit_per_side=4
    )

    assert result["history_bars"] == len(candles)
    assert len(result["levels"]) <= 8
    assert all("Side effectiveness (%)" in row for row in result["levels"])
    assert all("Side resolved N" in row for row in result["levels"])
    assert all(row["Side"].startswith(("BUY-SIDE", "SELL-SIDE")) for row in result["levels"])


@pytest.mark.parametrize(
    "kwargs",
    [
        {"kind": "middle"},
        {"level": float("nan")},
        {"tolerance_points": -1},
        {"confirm_index": 1000},
    ],
)
def test_invalid_retest_inputs_are_rejected(kwargs):
    params = {
        "level": 100.0,
        "kind": "high",
        "confirm_index": 15,
        "tolerance_points": 0.5,
    }
    params.update(kwargs)
    with pytest.raises(ValueError):
        evaluate_level_retests(baseline_candles(), **params)
