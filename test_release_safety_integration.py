"""Release safety tests for signal_engine.py.

These are synthetic, deterministic tests. They do not connect to Delta Exchange,
place orders, or prove live-feed correctness or profitability.
Run beside signal_engine.py:
    python3 -m pytest -q test_release_safety_integration.py
"""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import unittest

from signal_engine import BookLevel, Candle, MarketSnapshot, SignalEngine, Trade

# Self-contained synthetic fixture: deliberately does not import test_signal_engine.py.
# This prevents fixture naming/exports in an existing local test file from breaking
# the release safety suite. These prices/candles are synthetic and not market data.
UTC = timezone.utc
NOW = datetime(2026, 10, 21, 10, 0, 0, tzinfo=UTC)


def candle(ts, o, h, l, c, volume=10.0, complete=True):
    return Candle(NOW + timedelta(seconds=ts), o, h, l, c, volume, complete)


def raw_candle_specs():
    # Completed 5m history: high pivot at 113, low pivot/sweep level at 108,
    # sweep/reclaim on index 9, structure break on index 10, later opposing
    # swing high at 134. All data is known by the snapshot timestamp.
    return [
        (109.0, 110.0, 108.0, 109.5),
        (109.5, 111.0, 109.0, 110.5),
        (110.5, 112.0, 110.0, 111.5),
        (111.5, 113.0, 111.0, 112.5),
        (112.5, 112.8, 110.0, 111.0),
        (111.0, 111.5, 109.0, 110.0),
        (110.0, 110.5, 108.0, 109.0),
        (109.0, 110.0, 108.5, 109.5),
        (109.5, 111.0, 109.0, 110.5),
        (113.0, 114.0, 104.0, 109.0),  # sweep/reclaim, bearish OB candle
        (109.0, 116.0, 108.5, 115.0), # close above pre-sweep swing high
        (120.0, 134.0, 118.0, 122.0), # target pivot high
        (122.0, 125.0, 119.0, 123.0),
        (123.0, 124.0, 112.0, 113.5),
    ]


def mirror(specs, axis=230.0):
    # Reflect price vertically: bullish structure becomes bearish structure.
    return [(axis-o, axis-l, axis-h, axis-c) for o, h, l, c in specs]


def build_snapshot(direction="BUY", now=NOW, overrides=None):
    specs5 = raw_candle_specs()
    if direction == "SELL":
        specs5 = mirror(specs5)
    # Align the last 5m candle to close exactly at `now`.
    base5 = now - timedelta(minutes=5 * len(specs5))
    c5 = tuple(Candle(base5 + timedelta(minutes=5*i), *spec, 20.0, True) for i, spec in enumerate(specs5))

    # 55 completed 15m candles: a simple monotonic trend, bullish for BUY and
    # bearish for SELL. Trend figures are fixtures, not recommended parameters.
    n15 = 60
    c15_rows = []
    for i in range(n15):
        close = 72.0 + 0.68*i
        o, h, lo = close - 0.15, close + 1.0, close - 1.0
        if direction == "SELL":
            o, h, lo, close = 230.0-o, 230.0-lo, 230.0-h, 230.0-close
        c15_rows.append((o, h, lo, close))
    base15 = now - timedelta(minutes=15*n15)
    c15 = tuple(Candle(base15 + timedelta(minutes=15*i), *spec, 200.0, True) for i, spec in enumerate(c15_rows))

    # 1m completed trigger candle comes after the 5m structure break.
    rows1 = [
        (112.55, 112.8, 112.4, 112.7),
        (112.7, 112.9, 112.6, 112.8),
        (112.8, 113.0, 112.7, 112.9),
        (112.9, 113.2, 112.8, 113.0),
        (113.2, 113.6, 113.1, 113.5),
    ]
    if direction == "SELL":
        rows1 = mirror(rows1)
    base1 = now - timedelta(minutes=len(rows1))
    c1 = tuple(Candle(base1 + timedelta(minutes=i), *spec, 10.0, True) for i, spec in enumerate(rows1))

    price = c1[-1].close
    bid, ask = (price-0.1, price) if direction == "BUY" else (price, price+0.1)
    start_price = price - 0.6 if direction == "BUY" else price + 0.6
    trades = []
    for i in range(12):
        ts = now - timedelta(seconds=205 - 18*i)
        p = start_price + (price-start_price) * i/11
        side = "BUY_TAKER" if direction == "BUY" else "SELL_TAKER"
        trades.append(Trade(ts, p, 2.0, side))

    channel_last_seen = {k: now-timedelta(seconds=1) for k in ("ticker", "ob_updates", "trades", "candlestick_1m")}
    bid_levels = (
        BookLevel(bid, 15.0, now-timedelta(seconds=1), "bid"),
        BookLevel(bid-0.1, 25.0, now-timedelta(seconds=1), "bid"),
    )
    ask_levels = (
        BookLevel(ask, 14.0, now-timedelta(seconds=1), "ask"),
        BookLevel(ask+0.1, 27.0, now-timedelta(seconds=1), "ask"),
    )
    data = dict(
        timestamp=now, connection="connected", last_trade_price=price,
        last_trade_at=trades[-1].timestamp, best_bid=bid, best_ask=ask,
        quote_timestamp=now-timedelta(seconds=1),
        candles={"1m": c1, "5m": c5, "15m": c15},
        channel_last_seen=channel_last_seen, book_synced=True, sequence_valid=True,
        sequence_gap_unresolved=False, checksum_status="verified", trades=tuple(trades),
        bids=bid_levels, asks=ask_levels, liquidity_targets=(), source="synthetic_test_fixture",
    )
    if overrides:
        data.update(overrides)
    return MarketSnapshot(**data)



def gate_map(result):
    """Return data-quality gates from the typed result or its serialized form."""
    gates = getattr(result, "data_quality_gates", None)
    if gates is None:
        payload = result.to_dict() if callable(getattr(result, "to_dict", None)) else {}
        gates = payload.get("data_quality_gates") or payload.get("data_quality_gate_results") or {}
    return dict(gates or {})


def reasons(result):
    value = getattr(result, "reason_codes", ())
    if value:
        return tuple(value)
    payload = result.to_dict() if callable(getattr(result, "to_dict", None)) else {}
    return tuple(payload.get("reason_codes") or ())


def assert_no_confirmed_entry(testcase, result):
    testcase.assertEqual(result.signal, "WAIT", result.to_dict())
    testcase.assertNotIn(result.state, {"BUY_CONFIRMED", "SELL_CONFIRMED"}, result.to_dict())
    testcase.assertFalse(result.is_new_signal, result.to_dict())


class TestNegativePathEntryBlocking(unittest.TestCase):
    """Every injected integrity failure must block confirmed entry."""

    def assert_blocked(self, snapshot, reason_code, failed_gate):
        result = SignalEngine().evaluate(snapshot)
        assert_no_confirmed_entry(self, result)
        self.assertIn(reason_code, reasons(result), result.to_dict())
        gates = gate_map(result)
        self.assertIs(gates.get(failed_gate), False, result.to_dict())
        return result

    def test_disconnected_feed_blocks_entry(self):
        self.assert_blocked(
            replace(build_snapshot("BUY"), connection="disconnected"),
            "WAIT_CONNECTION_UNHEALTHY", "connection_healthy",
        )

    def test_book_desynchronization_blocks_entry(self):
        self.assert_blocked(
            replace(build_snapshot("BUY"), book_synced=False),
            "WAIT_BOOK_UNSYNCED", "book_synced",
        )

    def test_invalid_sequence_blocks_entry(self):
        self.assert_blocked(
            replace(build_snapshot("BUY"), sequence_valid=False),
            "WAIT_SEQUENCE_INVALID", "sequence_valid",
        )

    def test_unresolved_sequence_gap_blocks_entry(self):
        self.assert_blocked(
            replace(build_snapshot("BUY"), sequence_gap_unresolved=True),
            "WAIT_SEQUENCE_GAP", "sequence_valid",
        )

    def test_checksum_failure_blocks_entry(self):
        self.assert_blocked(
            replace(build_snapshot("BUY"), checksum_status="failed"),
            "WAIT_CHECKSUM_FAILED", "checksum_verified",
        )

    def test_missing_checksum_verification_blocks_by_default(self):
        self.assert_blocked(
            replace(build_snapshot("BUY"), checksum_status="unavailable"),
            "WAIT_CHECKSUM_UNVERIFIED", "checksum_verified",
        )

    def test_stale_required_channel_blocks_entry(self):
        snap = build_snapshot("BUY")
        channel_times = dict(snap.channel_last_seen)
        channel_times["trades"] = NOW - timedelta(minutes=2)
        self.assert_blocked(
            replace(snap, channel_last_seen=channel_times),
            "WAIT_CHANNEL_STALE:trades", "channel_fresh:trades",
        )

    def test_missing_required_channel_blocks_entry(self):
        snap = build_snapshot("BUY")
        channel_times = dict(snap.channel_last_seen)
        channel_times.pop("ob_updates")
        result = SignalEngine().evaluate(replace(snap, channel_last_seen=channel_times))
        assert_no_confirmed_entry(self, result)
        self.assertIn("WAIT_CHANNEL_STALE:ob_updates", reasons(result), result.to_dict())
        self.assertIs(gate_map(result).get("channel_fresh:ob_updates"), False)

    def test_stale_trade_data_blocks_entry(self):
        self.assert_blocked(
            replace(build_snapshot("BUY"), last_trade_at=NOW - timedelta(minutes=1)),
            "WAIT_TRADES_STALE", "last_trade_fresh",
        )

    def test_missing_order_book_levels_blocks_entry(self):
        self.assert_blocked(
            replace(build_snapshot("BUY"), bids=(), asks=()),
            "WAIT_BOOK_LEVELS_STALE_OR_MISSING", "book_levels_fresh",
        )


class TestRecoveryAfterIntegrityFailures(unittest.TestCase):
    """A single engine instance must become eligible after a genuinely valid snapshot."""

    def assert_recovered_after(self, bad_snapshot, expected_reason):
        engine = SignalEngine()

        blocked = engine.evaluate(bad_snapshot)
        assert_no_confirmed_entry(self, blocked)
        self.assertIn(expected_reason, reasons(blocked), blocked.to_dict())

        # New, valid synthetic snapshot represents a resynchronized, fresh feed.
        recovered_snapshot = build_snapshot("BUY", now=NOW + timedelta(minutes=1))
        recovered = engine.evaluate(recovered_snapshot)

        self.assertEqual(recovered.signal, "BUY", recovered.to_dict())
        self.assertEqual(recovered.state, "BUY_CONFIRMED", recovered.to_dict())
        self.assertTrue(recovered.is_new_signal, recovered.to_dict())
        gates = gate_map(recovered)
        self.assertTrue(gates, "Expected gate evidence in engine result")
        self.assertTrue(all(value is True for value in gates.values()), gates)

    def test_recovers_after_invalid_sequence(self):
        self.assert_recovered_after(
            replace(build_snapshot("BUY"), sequence_valid=False),
            "WAIT_SEQUENCE_INVALID",
        )

    def test_recovers_after_unresolved_gap(self):
        self.assert_recovered_after(
            replace(build_snapshot("BUY"), sequence_gap_unresolved=True),
            "WAIT_SEQUENCE_GAP",
        )

    def test_recovers_after_book_desync(self):
        self.assert_recovered_after(
            replace(build_snapshot("BUY"), book_synced=False),
            "WAIT_BOOK_UNSYNCED",
        )

    def test_recovers_after_checksum_failure(self):
        self.assert_recovered_after(
            replace(build_snapshot("BUY"), checksum_status="failed"),
            "WAIT_CHECKSUM_FAILED",
        )

    def test_recovers_after_stale_trade_channel(self):
        snap = build_snapshot("BUY")
        channel_times = dict(snap.channel_last_seen)
        channel_times["trades"] = NOW - timedelta(minutes=2)
        self.assert_recovered_after(
            replace(snap, channel_last_seen=channel_times),
            "WAIT_CHANNEL_STALE:trades",
        )


class TestValidSyntheticControls(unittest.TestCase):
    """Prove the fixture can become eligible; this is not a live trade signal."""

    def test_valid_bullish_fixture_confirms_buy(self):
        result = SignalEngine().evaluate(build_snapshot("BUY"))
        self.assertEqual(result.signal, "BUY", result.to_dict())
        self.assertEqual(result.state, "BUY_CONFIRMED")
        self.assertTrue(all(value is True for value in gate_map(result).values()))

    def test_valid_bearish_fixture_confirms_sell(self):
        result = SignalEngine().evaluate(build_snapshot("SELL"))
        self.assertEqual(result.signal, "SELL", result.to_dict())
        self.assertEqual(result.state, "SELL_CONFIRMED")
        self.assertTrue(all(value is True for value in gate_map(result).values()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
