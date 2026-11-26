"""Unit tests for the standalone public-feed read-only probe."""
from datetime import datetime, timezone

from delta_readonly_ws_probe import apply_levels, book_checksum, channel_freshness, epoch_seconds


def test_timestamp_unit_conversion_for_delta_microseconds_and_seconds():
    instant = datetime(2026, 11, 26, tzinfo=timezone.utc).timestamp()
    assert epoch_seconds(instant) == instant
    assert abs(epoch_seconds(instant * 1_000_000) - instant) < 1e-6


def test_book_checksum_is_deterministic_and_sensitive_to_depth_changes():
    asks = {"100001.0": "2", "100001.5": "3"}
    bids = {"99999.0": "4"}
    original = book_checksum(asks, bids)
    assert original == book_checksum(dict(asks), dict(bids))
    bids_changed = {"99999.0": "5"}
    assert original != book_checksum(asks, bids_changed)


def test_apply_levels_updates_and_removes_zero_size_levels():
    book = {"100001.0": "3", "100002.0": "2"}
    apply_levels(book, [["100001.0", "5"], ["100002.0", "0"], ["100003.0", "4"]])
    assert book == {"100001.0": "5", "100003.0": "4"}



def test_channel_liveness_is_checked_at_the_end_of_the_observation_window():
    end = 1_800_000_000.0
    checks, ages = channel_freshness({
        "ticker": end - 1.0,
        "ob_l1": end - 9.0,
        "trades": end - 19.0,
    }, end)
    assert checks["ticker_fresh_at_end"] is True
    assert checks["ob_l1_fresh_at_end"] is False
    assert checks["trades_fresh_at_end"] is True
    assert ages == {"ticker": 1.0, "ob_l1": 9.0, "trades": 19.0}
