import pytest

from trade_activity import summarize_trade_prints


def test_summarize_trade_prints_counts_and_values_by_aggressor_side():
    records = [
        (1000, 100_000.0, 10.0, "buy"),
        (1001, 120_000.0, 5.0, "BUY"),
        (1002, 200_000.0, 2.0, "sell"),
    ]

    result = summarize_trade_prints(records, contract_value_btc=0.001)

    assert result == [
        {
            "Side": "BUY",
            "Trade prints": 2,
            "Contracts": 15.0,
            "BTC equivalent": pytest.approx(0.015),
            "Approx USD value": pytest.approx(1600.0),
        },
        {
            "Side": "SELL",
            "Trade prints": 1,
            "Contracts": 2.0,
            "BTC equivalent": pytest.approx(0.002),
            "Approx USD value": pytest.approx(400.0),
        },
    ]


def test_summarize_trade_prints_ignores_unknown_and_invalid_records():
    records = [
        (1000, 100_000.0, 10.0, "unknown"),
        (1001, 100_000.0, 0.0, "buy"),
        (1002, -1.0, 2.0, "sell"),
        (1003, float("nan"), 2.0, "buy"),
        (1004, "not-a-price", 1.0, "sell"),
        ("short",),
    ]

    result = summarize_trade_prints(records)

    assert result[0]["Trade prints"] == 0
    assert result[0]["Contracts"] == 0.0
    assert result[0]["Approx USD value"] == 0.0
    assert result[1]["Trade prints"] == 0
    assert result[1]["Contracts"] == 0.0
    assert result[1]["Approx USD value"] == 0.0


def test_summarize_trade_prints_returns_both_sides_for_empty_tape():
    assert summarize_trade_prints([]) == [
        {
            "Side": "BUY",
            "Trade prints": 0,
            "Contracts": 0.0,
            "BTC equivalent": 0.0,
            "Approx USD value": 0.0,
        },
        {
            "Side": "SELL",
            "Trade prints": 0,
            "Contracts": 0.0,
            "BTC equivalent": 0.0,
            "Approx USD value": 0.0,
        },
    ]


@pytest.mark.parametrize("contract_value", [0, -0.001, float("inf"), float("nan"), "bad"])
def test_summarize_trade_prints_rejects_invalid_contract_size(contract_value):
    with pytest.raises(ValueError):
        summarize_trade_prints([], contract_value_btc=contract_value)
