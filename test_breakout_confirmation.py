import pytest

from breakout_confirmation import (
    DEFAULT_TIMEFRAMES,
    detect_multi_timeframe_breakouts,
)


def _candles(duration, count, *, base=100.0, volume=100.0):
    rows = []
    for i in range(count):
        timestamp = 1_800_000_000 + i * duration
        rows.append({
            "time": timestamp,
            "open": base,
            "high": base + 1,
            "low": base - 1,
            "close": base,
            "volume": volume,
        })
    return rows


def _args(now, start, last_seen=None):
    return {
        "now_ts": now,
        "feed_started_at": start,
        "connection_status": "connected",
        "trade_channel_last_seen": now - 1 if last_seen is None else last_seen,
    }


def _flow(start_minute, count, *, buy=20, sell=0, buy_contracts=200.0, sell_contracts=0.0):
    result = {}
    for i in range(count):
        minute = start_minute + i * 60
        result[minute] = {
            "buy_count": buy,
            "sell_count": sell,
            "buy_contracts": buy_contracts,
            "sell_contracts": sell_contracts,
            "buy_usd": buy_contracts * 0.001 * 100,
            "sell_usd": sell_contracts * 0.001 * 100,
        }
    return result


def _valid_bullish_history(duration=60, count=30):
    rows = _candles(duration, count, base=100, volume=100)
    # Bar 27 breaks the 20-bar resistance (101) by >0.10 ATR.
    rows[27].update({"open": 101, "high": 105, "low": 100.5, "close": 104, "volume": 200})
    # Bar 28 accepts above the broken range, then price continues.
    rows[28].update({"open": 104, "high": 106, "low": 103, "close": 105, "volume": 120})
    return rows


def test_default_timeframes_cover_six_intervals():
    assert DEFAULT_TIMEFRAMES == (
        ("1m", 60), ("5m", 300), ("15m", 900),
        ("1h", 3600), ("4h", 14400), ("1d", 86400),
    )


def test_genuine_bullish_breakout_requires_price_volume_and_buy_flow():
    duration = 60
    rows = _valid_bullish_history(duration)
    breakout_start = rows[27]["time"]
    flow = _flow(breakout_start, 1, buy=15, sell=5, buy_contracts=180, sell_contracts=20)
    now = rows[-1]["time"] + duration + 5

    result = detect_multi_timeframe_breakouts(
        {"1m": rows}, flow,
        **_args(now, rows[0]["time"] - 1),
        timeframes=(("1m", duration),),
    )
    item = result["windows"][0]

    assert item["classification"].startswith("GENUINE BUY BREAKOUT")
    assert item["price_accepted"] is True
    assert item["volume_confirmed"] is True
    assert item["flow_confirmed"] is True
    assert item["relative_volume"] == pytest.approx(2.0)
    assert item["flow"]["buy_share_pct"] == pytest.approx(90.0)


def test_genuine_bearish_breakout_requires_price_volume_and_sell_flow():
    duration = 60
    rows = _candles(duration, 30, base=100, volume=100)
    rows[27].update({"open": 99, "high": 99.5, "low": 95, "close": 96, "volume": 200})
    rows[28].update({"open": 96, "high": 97, "low": 94, "close": 95, "volume": 120})
    breakout_start = rows[27]["time"]
    flow = _flow(
        breakout_start, 1, buy=2, sell=18,
        buy_contracts=20, sell_contracts=180,
    )
    now = rows[-1]["time"] + duration + 5

    result = detect_multi_timeframe_breakouts(
        {"1m": rows}, flow,
        **_args(now, rows[0]["time"] - 1),
        timeframes=(("1m", duration),),
    )
    item = result["windows"][0]

    assert item["classification"].startswith("GENUINE SELL BREAKOUT")
    assert item["flow"]["sell_share_pct"] == pytest.approx(90.0)
    assert item["net_delta_contracts"] if "net_delta_contracts" in item else item["flow"]["net_delta_contracts"] < 0


def test_price_and_volume_breakout_is_not_genuine_when_taker_flow_is_unverified():
    duration = 60
    rows = _valid_bullish_history(duration)
    breakout_start = rows[27]["time"]
    now = rows[-1]["time"] + duration + 5

    result = detect_multi_timeframe_breakouts(
        {"1m": rows}, {},
        **_args(now, rows[0]["time"] - 1),
        timeframes=(("1m", duration),),
    )
    item = result["windows"][0]

    assert item["classification"] == "PRICE + VOLUME — ORDER FLOW UNVERIFIED"
    assert item["flow_confirmed"] is False


def test_opposing_taker_flow_is_reported_as_divergence():
    duration = 60
    rows = _valid_bullish_history(duration)
    breakout_start = rows[27]["time"]
    flow = _flow(
        breakout_start, 1, buy=2, sell=18,
        buy_contracts=20, sell_contracts=180,
    )
    now = rows[-1]["time"] + duration + 5

    item = detect_multi_timeframe_breakouts(
        {"1m": rows}, flow,
        **_args(now, rows[0]["time"] - 1),
        timeframes=(("1m", duration),),
    )["windows"][0]

    assert item["classification"] == "PRICE + VOLUME — ORDER-FLOW DIVERGENCE"


def test_failed_second_close_never_becomes_genuine():
    duration = 60
    rows = _valid_bullish_history(duration)
    rows[28].update({"open": 104, "high": 104.5, "low": 99, "close": 100})
    breakout_start = rows[27]["time"]
    flow = _flow(breakout_start, 1)
    now = rows[-1]["time"] + duration + 5

    item = detect_multi_timeframe_breakouts(
        {"1m": rows}, flow,
        **_args(now, rows[0]["time"] - 1),
        timeframes=(("1m", duration),),
    )["windows"][0]

    assert item["classification"] == "FAILED BREAKOUT"
    assert item["failed"] is True


def test_wick_through_level_and_close_back_inside_is_liquidity_sweep():
    duration = 60
    rows = _candles(duration, 30, base=100, volume=100)
    rows[-1].update({"open": 100, "high": 105, "low": 99, "close": 100, "volume": 180})
    now = rows[-1]["time"] + duration + 5

    item = detect_multi_timeframe_breakouts(
        {"1m": rows}, {},
        **_args(now, rows[0]["time"] - 1),
        timeframes=(("1m", duration),),
    )["windows"][0]

    assert item["classification"].startswith("BUY-SIDE LIQUIDITY SWEEP")


def test_flow_capture_requires_start_before_bar_and_minute_coverage():
    duration = 300
    rows = _candles(duration, 30, base=100, volume=100)
    rows[27].update({"open": 101, "high": 105, "low": 100.5, "close": 104, "volume": 200})
    rows[28].update({"open": 104, "high": 106, "low": 103, "close": 105, "volume": 120})
    start = rows[27]["time"]
    complete_flow = _flow(start, 5)

    partial_item = detect_multi_timeframe_breakouts(
        {"5m": rows}, complete_flow,
        **_args(rows[-1]["time"] + duration + 5, start + 1),
        timeframes=(("5m", duration),),
    )["windows"][0]
    assert partial_item["flow"]["capture_eligible"] is False
    assert "APP STARTED AFTER" in partial_item["flow"]["capture_status"]


def test_low_active_minute_coverage_blocks_genuine_label():
    duration = 300
    rows = _candles(duration, 30, base=100, volume=100)
    rows[27].update({"open": 101, "high": 105, "low": 100.5, "close": 104, "volume": 200})
    rows[28].update({"open": 104, "high": 106, "low": 103, "close": 105, "volume": 120})
    start = rows[27]["time"]
    flow = _flow(start, 4)  # 4 of 5 minutes = 80%

    item = detect_multi_timeframe_breakouts(
        {"5m": rows}, flow,
        **_args(rows[-1]["time"] + duration + 5, start - 1),
        timeframes=(("5m", duration),),
    )["windows"][0]

    assert item["classification"] == "PRICE + VOLUME — ORDER FLOW UNVERIFIED"
    assert item["flow"]["active_minute_coverage_pct"] == pytest.approx(80)


def test_in_progress_candles_are_excluded():
    duration = 60
    rows = _candles(duration, 30, base=100, volume=100)
    now = rows[-1]["time"] + 10
    item = detect_multi_timeframe_breakouts(
        {"1m": rows}, {},
        **_args(now, rows[0]["time"] - 1),
        timeframes=(("1m", duration),),
    )["windows"][0]

    assert item["completed_candles"] == 29
    assert item["classification"] == "NO RECENT BREAKOUT"


def test_no_breakout_without_completed_second_close_is_developing():
    duration = 60
    rows = _candles(duration, 28, base=100, volume=100)
    rows[27].update({"open": 101, "high": 105, "low": 100.5, "close": 104, "volume": 200})
    now = rows[-1]["time"] + duration + 5

    item = detect_multi_timeframe_breakouts(
        {"1m": rows}, {},
        **_args(now, rows[0]["time"] - 1),
        timeframes=(("1m", duration),),
    )["windows"][0]

    assert item["classification"] == "BREAKOUT DEVELOPING"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"now_ts": float("nan")},
        {"range_lookback": 1},
        {"atr_period": 1},
        {"min_taker_prints": 0},
        {"min_taker_share_pct": 50},
        {"min_active_minute_coverage_pct": 0},
        {"min_relative_volume": 0},
        {"breakout_buffer_atr": 0},
        {"acceptance_buffer_atr": -1},
        {"event_lookback_bars": 0},
        {"retest_lookahead_bars": 0},
    ],
)
def test_invalid_settings_are_rejected(kwargs):
    args = {
        "now_ts": 1_800_000_000,
        "feed_started_at": 1_799_000_000,
        "connection_status": "connected",
        "trade_channel_last_seen": 1_799_999_999,
    }
    args.update(kwargs)
    with pytest.raises(ValueError):
        detect_multi_timeframe_breakouts({}, {}, **args)


def test_candle_gap_in_required_range_window_blocks_breakout_confirmation():
    duration = 60
    rows = _valid_bullish_history(duration)
    # Remove one candle from the range/ATR segment. No gap is silently filled.
    del rows[12]
    breakout_time = 1_800_000_000 + 27 * duration
    flow = _flow(breakout_time, 1)
    now = rows[-1]["time"] + duration + 5

    item = detect_multi_timeframe_breakouts(
        {"1m": rows}, flow,
        **_args(now, rows[0]["time"] - 1),
        timeframes=(("1m", duration),),
    )["windows"][0]

    assert item["classification"] in (
        "INSUFFICIENT — CANDLE HISTORY GAP",
        "PRICE + VOLUME — ORDER FLOW UNVERIFIED",
        "NO RECENT BREAKOUT",
    )
    assert not str(item["classification"]).startswith("GENUINE ")
