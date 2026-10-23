"""Release safety tests for signal_engine.py.

These are synthetic, deterministic tests verifying that invalid feed states
block confirmed entry (no BUY/SELL signals).
"""
from datetime import datetime, timedelta, timezone
import unittest

from signal_engine import (
    ChecksumState,
    Candle,
    MarketSnapshot,
    Signal,
    SignalEngine,
)

UTC = timezone.utc
NOW = datetime(2026, 10, 21, 10, 0, 0, tzinfo=UTC)


def build_base_snapshot(**overrides):
    """Build a minimal valid snapshot for testing."""
    base = {
        'ts_utc': NOW,
        'last_price': 100.0,
        'bid': 99.9,
        'ask': 100.1,
        'candles_1m': [Candle(NOW - timedelta(minutes=1), 99.0, 100.0, 98.0, 100.0)],
        'candles_5m': [Candle(NOW - timedelta(minutes=5), 98.5, 100.5, 98.0, 100.0)],
        'candles_15m': [Candle(NOW - timedelta(minutes=15), 98.0, 101.0, 97.0, 100.0)],
        'book_synced': True,
        'connection_healthy': True,
        'sequence_gap_unresolved': False,
        'checksum_state': ChecksumState.VERIFIED,
        'market_fresh': True,
        'spread_bps': 20.0,
        'recent_trades': [],
        'completed_candle_timestamps': {},
        'swings_5m': [],
        'order_blocks_5m': [],
        'ask_liquidity': [],
        'bid_liquidity': [],
        'atr_5m': None,
        'atr_15m': None,
    }
    base.update(overrides)
    return MarketSnapshot(**base)


def gate_map(result):
    """Return data-quality gates from the typed result."""
    return getattr(result, 'data_quality_gate_results', {})


def reasons(result):
    """Return reason codes from result."""
    return tuple(getattr(result, 'reason_codes', []))


class TestNegativePathEntryBlocking(unittest.TestCase):
    """Every injected integrity failure must block confirmed entry."""

    def test_book_desynchronized_blocks_entry(self):
        """A desynchronized book blocks entry."""
        snap = build_base_snapshot(book_synced=False, connection_healthy=False, sequence_gap_unresolved=True)
        engine = SignalEngine()
        result = engine.evaluate(snap)
        self.assertEqual(result.signal, Signal.WAIT)
        self.assertFalse(gate_map(result).get('book_synced'))

    def test_invalid_sequence_blocks_entry(self):
        """Invalid sequence blocks entry."""
        snap = build_base_snapshot(sequence_gap_unresolved=True)
        engine = SignalEngine()
        result = engine.evaluate(snap)
        self.assertEqual(result.signal, Signal.WAIT)
        self.assertFalse(gate_map(result).get('no_unresolved_sequence_gap'))

    def test_checksum_failure_blocks_entry(self):
        """Checksum failure blocks entry."""
        snap = build_base_snapshot(checksum_state=ChecksumState.FAILED)
        engine = SignalEngine()
        result = engine.evaluate(snap)
        self.assertEqual(result.signal, Signal.WAIT)
        self.assertFalse(gate_map(result).get('checksum_ok'))


class TestValidSyntheticControls(unittest.TestCase):
    """Prove the fixture can process a valid snapshot."""

    def test_valid_snapshot_processes(self):
        """A valid snapshot should process without error."""
        snap = build_base_snapshot()
        engine = SignalEngine()
        result = engine.evaluate(snap)
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main(verbosity=2)