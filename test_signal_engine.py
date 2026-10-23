"""test_signal_engine.py - deterministic tests for signal_engine.

All fixtures are synthetic, fixed and closed-candle based. No live market data
is fetched, and no result is a statement about profitability.
"""
import unittest
from datetime import datetime, timedelta, timezone

import signal_engine as se

T0 = datetime(2026, 10, 15, 0, 0, tzinfo=timezone.utc)


def c(ts, o, h, l, cl, v=10.0):
    return se.Candle(ts_utc=ts, open=o, high=h, low=l, close=cl, volume=v)


def build_15m(n=70, start=900.0, end=1095.0, end_ts=None):
    """Linear uptrend: EMA20 > EMA50 and price > EMA50.

    The newest completed bar closes at `end_ts` (default T0+60m) so no 15m
    candle lies in the future relative to the snapshot timestamp.
    """
    end_ts = end_ts or (T0 + timedelta(minutes=60))
    step = (end - start) / (n - 1)
    out = []
    for i in range(n):
        price = start + i * step
        ts = end_ts - timedelta(minutes=15 * (n - 1 - i))
        out.append(c(ts, round(price, 4), round(price + 1, 4),
                     round(price - 1, 4), round(price, 4)))
    return out


def build_15m_downtrend(n=70, start=1095.0, end=900.0, end_ts=None):
    return build_15m(n=n, start=start, end=end, end_ts=end_ts)


# --- 5m bullish setup -----------------------------------------------------
# bar 3  pivot low  1050 (confirmed by bar 6)
# bar 5  pivot high 1082 (confirmed by bar 8)
# bar 8  down candle -> the intact bullish order block 1064-1075
# bar 9  sweep: low 1044 pierces 1050, close 1068 reclaims it
# bar 12 structure break: close 1092 above 1082
BULL_5M = [
    (1060, 1065, 1055, 1062),
    (1062, 1068, 1058, 1066),
    (1066, 1070, 1062, 1064),
    (1064, 1066, 1050, 1058),
    (1058, 1075, 1056, 1072),
    (1072, 1082, 1068, 1078),
    (1078, 1079, 1070, 1072),
    (1072, 1078, 1066, 1076),
    (1076, 1077, 1064, 1066),
    (1066, 1070, 1044, 1068),
    (1068, 1074, 1062, 1072),
    (1072, 1080, 1068, 1078),
    (1078, 1095, 1076, 1092),
]


def build_5m_bullish():
    out = [c(T0 + timedelta(minutes=5 * i), *b) for i, b in enumerate(BULL_5M)]
    # one narrow drift bar after the structure bar (still completed before the
    # snapshot timestamp) so the trailing range stays tight
    price = 1094.0
    out.append(c(T0 + timedelta(minutes=5 * 13), price, price + 1.5, price - 1.5, price + 0.2))
    return out


MIRROR = 2200.0


def _reflect(bar):
    """Mirror an OHLC tuple around MIRROR (keeps high/low consistency)."""
    o, h, l, cl = bar
    return (MIRROR - o, MIRROR - l, MIRROR - h, MIRROR - cl)


BULL_1M = [
    (1096.0, 1098.5, 1094.0, 1098.0),
    (1098.0, 1100.0, 1096.5, 1099.5),
    (1099.5, 1101.0, 1097.0, 1100.0),
]


def build_1m_bullish(struct_ts):
    first = struct_ts + timedelta(minutes=5)
    return [c(first + timedelta(minutes=i), *p) for i, p in enumerate(BULL_1M)]


def build_1m_bearish(struct_ts):
    first = struct_ts + timedelta(minutes=5)
    return [c(first + timedelta(minutes=i), *_reflect(p)) for i, p in enumerate(BULL_1M)]


def build_trades(end_ts, direction):
    n = 15
    if direction == "bullish":
        out = [se.Trade(ts_utc=end_ts - timedelta(minutes=5) + timedelta(seconds=18 * i),
                        price=1080.0 + 0.4 * i, size=20.0 if i % 4 == 0 else 60.0,
                        side_aggressor=se.AGGRESSOR_BUY)
               for i in range(n)]
        out.append(se.Trade(ts_utc=end_ts, price=1094.0, size=30.0,
                            side_aggressor=se.AGGRESSOR_SELL))
    else:
        out = [se.Trade(ts_utc=end_ts - timedelta(minutes=5) + timedelta(seconds=18 * i),
                        price=1120.0 - 0.4 * i, size=20.0 if i % 4 == 0 else 60.0,
                        side_aggressor=se.AGGRESSOR_SELL)
               for i in range(n)]
        out.append(se.Trade(ts_utc=end_ts, price=1106.0, size=30.0,
                            side_aggressor=se.AGGRESSOR_BUY))
    return out


STRUCT_TS = T0 + timedelta(minutes=5 * 12)


def base_snapshot(direction="bullish", ask_liq=None, bid_liq=None, trades_end=None, **over):
    if direction == "bullish":
        c5 = build_5m_bullish()
        c1 = build_1m_bullish(STRUCT_TS)
        c15 = build_15m(end=1095.0)
        trades = build_trades(trades_end or c1[-1].ts_utc, "bullish")
        ask_liquidity = ask_liq if ask_liq is not None else [
            se.LiquidityLevel(price=1200.0, contracts=250.0, side="ask",
                              observed_ts_utc=c1[-1].ts_utc)]
        bid_liquidity = bid_liq if bid_liq is not None else []
    else:
        c5 = [c(cd.ts_utc, *_reflect((cd.open, cd.high, cd.low, cd.close)))
              for cd in build_5m_bullish()]
        c1 = build_1m_bearish(STRUCT_TS)
        c15 = build_15m_downtrend(end=1075.0)
        trades = build_trades(trades_end or c1[-1].ts_utc, "bearish")
        bid_liquidity = bid_liq if bid_liq is not None else [
            se.LiquidityLevel(price=1000.0, contracts=250.0, side="bid",
                              observed_ts_utc=c1[-1].ts_utc)]
        ask_liquidity = ask_liq if ask_liq is not None else []
    price = c1[-1].close
    snap = se.MarketSnapshot(
        ts_utc=c1[-1].ts_utc,
        last_price=price,
        bid=price - 0.5,
        ask=price + 0.5,
        candles_1m=c1,
        candles_5m=c5,
        candles_15m=c15,
        book_synced=True,
        connection_healthy=True,
        sequence_gap_unresolved=False,
        checksum_state=se.ChecksumState.VERIFIED,
        market_fresh=True,
        spread_bps=(1.0 / price) * 10000.0,
        recent_trades=trades,
        ask_liquidity=ask_liquidity,
        bid_liquidity=bid_liquidity,
        atr_5m=4.0,
        atr_15m=30.0,
    )
    for k, v in over.items():
        object.__setattr__(snap, k, v)
    return snap


def replace_1m(snap, bars, trades_end=None):
    object.__setattr__(snap, "candles_1m", bars)
    object.__setattr__(snap, "ts_utc", bars[-1].ts_utc)
    object.__setattr__(snap, "last_price", bars[-1].close)
    object.__setattr__(snap, "bid", bars[-1].close - 0.5)
    object.__setattr__(snap, "ask", bars[-1].close + 0.5)
    if trades_end:
        object.__setattr__(snap, "recent_trades",
                           build_trades(trades_end, "bullish"))
    return snap


def long_1m_bullish(n_minutes=30):
    """Trigger bar followed by a long flat sequence (used for expiry tests)."""
    out = list(BULL_1M_TS[:1])
    for i in range(1, n_minutes + 1):
        ts = STRUCT_TS + timedelta(minutes=5) + timedelta(minutes=i)
        out.append(c(ts, 1098.0, 1099.5, 1096.5, 1098.0))
    return out


BULL_1M_TS = build_1m_bullish(STRUCT_TS)


class TestDataQualityGates(unittest.TestCase):
    def _ok(self):
        snap = base_snapshot()
        return snap

    def test_all_gates_pass_on_valid_snapshot(self):
        res = se.evaluate(self._ok())
        for k, v in res.data_quality_gate_results.items():
            self.assertTrue(v, f"gate {k} unexpectedly failed")

    def test_disconnected_feed_blocks(self):
        snap = self._ok()
        object.__setattr__(snap, "connection_healthy", False)
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_DATA_STALE, res.reason_codes)

    def test_book_unsynced_blocks(self):
        snap = self._ok()
        object.__setattr__(snap, "book_synced", False)
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_BOOK_UNSYNCED, res.reason_codes)

    def test_unresolved_sequence_gap_blocks(self):
        snap = self._ok()
        object.__setattr__(snap, "sequence_gap_unresolved", True)
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_SEQUENCE_GAP, res.reason_codes)

    def test_checksum_failed_blocks(self):
        snap = self._ok()
        object.__setattr__(snap, "checksum_state", se.ChecksumState.FAILED)
        res = se.evaluate(snap)
        self.assertIn(se.WAIT_CHECKSUM_FAILED, res.reason_codes)

    def test_checksum_unavailable_blocks_by_default(self):
        snap = self._ok()
        object.__setattr__(snap, "checksum_state", se.ChecksumState.UNAVAILABLE)
        res = se.evaluate(snap)
        self.assertIn(se.WAIT_CHECKSUM_UNVERIFIED, res.reason_codes)

    def test_checksum_unavailable_allowed_only_when_opted_in(self):
        snap = self._ok()
        object.__setattr__(snap, "checksum_state", se.ChecksumState.UNAVAILABLE)
        res = se.evaluate(snap, config={"require_checksum_verified": False})
        self.assertNotIn(se.WAIT_CHECKSUM_UNVERIFIED, res.reason_codes)
        self.assertIs(res.signal, se.Signal.BUY)

    def test_crossed_quotes_block(self):
        snap = self._ok()
        object.__setattr__(snap, "bid", snap.ask + 10.0)
        res = se.evaluate(snap)
        self.assertIn(se.WAIT_INVALID_BID_ASK, res.reason_codes)

    def test_wide_spread_blocks(self):
        snap = self._ok()
        object.__setattr__(snap, "spread_bps", 500.0)
        res = se.evaluate(snap)
        self.assertIn(se.WAIT_SPREAD_TOO_WIDE, res.reason_codes)

    def test_stale_1m_candles_block(self):
        snap = self._ok()
        old = [c(cd.ts_utc - timedelta(minutes=90), cd.open, cd.high, cd.low, cd.close)
               for cd in snap.candles_1m]
        object.__setattr__(snap, "candles_1m", old)
        res = se.evaluate(snap)
        self.assertIn(se.WAIT_DATA_STALE, res.reason_codes)

    def test_stale_trades_block(self):
        snap = self._ok()
        old = [se.Trade(ts_utc=T0, price=1000.0, size=10.0, side_aggressor=se.AGGRESSOR_BUY)
               for _ in range(20)]
        object.__setattr__(snap, "recent_trades", old)
        res = se.evaluate(snap)
        self.assertIn(se.WAIT_FLOW_STALE, res.reason_codes)

    def test_time_inconsistent_blocks(self):
        snap = self._ok()
        object.__setattr__(snap, "ts_utc", T0)
        res = se.evaluate(snap)
        self.assertIn(se.WAIT_TIME_INCONSISTENT, res.reason_codes)

    def test_insufficient_history_blocks(self):
        snap = self._ok()
        object.__setattr__(snap, "candles_15m", snap.candles_15m[:10])
        res = se.evaluate(snap)
        self.assertIn(se.WAIT_NO_15M_TREND, res.reason_codes)

    def test_missing_candles_blocks(self):
        snap = self._ok()
        object.__setattr__(snap, "candles_1m", [])
        res = se.evaluate(snap)
        self.assertIn(se.WAIT_NO_CANDLES, res.reason_codes)

    def test_invalid_numerics_rejected(self):
        with self.assertRaises(ValueError):
            se.MarketSnapshot(ts_utc=T0, last_price=float("nan"), bid=1.0, ask=2.0)
        with self.assertRaises(ValueError):
            se.Candle(ts_utc=T0, open=float("inf"), high=2.0, low=1.0, close=1.5)
        with self.assertRaises(ValueError):
            se.Candle(ts_utc=T0, open=1.0, high=1.2, low=2.0, close=1.1)
        with self.assertRaises(ValueError):
            se.Trade(ts_utc=T0, price=10.0, size=-1.0, side_aggressor=se.AGGRESSOR_BUY)
        with self.assertRaises(ValueError):
            se.Trade(ts_utc=T0, price=10.0, size=1.0, side_aggressor="BUYER")

    def test_duplicate_candle_timestamps_rejected(self):
        bars = [c(T0 + timedelta(minutes=i), 1, 2, 0.5, 1.5) for i in range(3)] + \
               [c(T0 + timedelta(minutes=2), 1, 2, 0.5, 1.5)]
        snap = self._ok()
        object.__setattr__(snap, "candles_1m", bars)
        with self.assertRaises(ValueError):
            se.evaluate(snap)


class TestConfirmationPaths(unittest.TestCase):
    def test_bullish_setup_confirms(self):
        res = se.evaluate(base_snapshot("bullish"))
        self.assertIs(res.signal, se.Signal.BUY)
        self.assertIs(res.state, se.SignalState.BUY_CONFIRMED)
        self.assertTrue(res.signal_id)
        self.assertEqual(res.setup_timeframe, "5m")
        self.assertIsNotNone(res.proposed_entry)
        self.assertLess(res.stop_loss, res.invalidation_price)
        self.assertLess(res.invalidation_price, res.proposed_entry)
        self.assertLess(res.proposed_entry, res.take_profit)
        self.assertGreaterEqual(res.risk_reward_ratio, 1.5)

    def test_bearish_setup_confirms(self):
        res = se.evaluate(base_snapshot("bearish"))
        self.assertIs(res.signal, se.Signal.SELL)
        self.assertIs(res.state, se.SignalState.SELL_CONFIRMED)
        self.assertGreater(res.stop_loss, res.invalidation_price)
        self.assertGreater(res.invalidation_price, res.proposed_entry)
        self.assertGreater(res.proposed_entry, res.take_profit)
        self.assertGreaterEqual(res.risk_reward_ratio, 1.5)

    def test_no_setup_returns_wait(self):
        snap = base_snapshot("bullish")
        # flat 5m series: no sweep, no structure break
        flat = [c(T0 + timedelta(minutes=5 * i), 1092.0, 1093.0, 1091.0, 1092.0)
                for i in range(14)]
        object.__setattr__(snap, "candles_5m", flat)
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertTrue(set(res.reason_codes) & {se.WAIT_NO_SWEEP, se.WAIT_NO_15M_TREND})

    def test_sweep_without_structure_break_waits(self):
        snap = base_snapshot("bullish")
        bars = [
            (1060, 1065, 1055, 1062),
            (1062, 1068, 1058, 1066),
            (1066, 1070, 1062, 1064),
            (1064, 1066, 1050, 1058),
            (1058, 1075, 1056, 1072),
            (1072, 1082, 1068, 1078),
            (1078, 1079, 1070, 1072),
            (1072, 1078, 1066, 1076),
            (1076, 1077, 1064, 1066),
            (1066, 1070, 1044, 1068),
            (1068, 1074, 1062, 1072),
            (1072, 1078, 1068, 1076),
            (1076, 1078, 1060, 1064),
        ]
        out = [c(T0 + timedelta(minutes=5 * i), *b) for i, b in enumerate(bars)]
        out.append(c(T0 + timedelta(minutes=5 * 13), 1065.0, 1066.0, 1064.0, 1065.0))
        object.__setattr__(snap, "candles_5m", out)
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_NO_STRUCTURE_BREAK, res.reason_codes)

    def test_order_block_touch_without_trigger_waits(self):
        snap = base_snapshot("bullish")
        # 1m bars only touch the zone and never close above the reference level
        first = STRUCT_TS + timedelta(minutes=5)
        bars = [c(first + timedelta(minutes=i), 1076, 1080, 1074, 1078) for i in range(3)]
        object.__setattr__(snap, "candles_1m", bars)
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_NO_1M_TRIGGER, res.reason_codes)

    def test_conflicting_aggressor_flow_waits(self):
        snap = base_snapshot("bullish")
        trades = [se.Trade(ts_utc=snap.ts_utc - timedelta(minutes=4) + timedelta(seconds=15 * i),
                           price=1100.0 - 0.5 * i, size=80.0,
                           side_aggressor=se.AGGRESSOR_SELL) for i in range(14)]
        object.__setattr__(snap, "recent_trades", trades)
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_FLOW_CONFLICT, res.reason_codes)

    def test_insufficient_trades_waits(self):
        snap = base_snapshot("bullish")
        trades = [se.Trade(ts_utc=snap.ts_utc - timedelta(minutes=4) + timedelta(seconds=60 * i),
                           price=1090.0, size=50.0, side_aggressor=se.AGGRESSOR_BUY)
                  for i in range(3)]
        object.__setattr__(snap, "recent_trades", trades)
        res = se.evaluate(snap)
        self.assertIn(se.WAIT_FLOW_INSUFFICIENT_TRADES, res.reason_codes)

    def test_no_valid_target_waits(self):
        snap = base_snapshot("bullish", ask_liq=[], bid_liq=[])
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_NO_VALID_TARGET, res.reason_codes)

    def test_insufficient_risk_reward_waits(self):
        snap = base_snapshot("bullish", ask_liq=[
            se.LiquidityLevel(price=1150.0, contracts=250.0, side="ask",
                              observed_ts_utc=STRUCT_TS + timedelta(minutes=10))])
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_RR_TOO_LOW, res.reason_codes)

    def test_expired_setup_waits(self):
        snap = base_snapshot("bullish")
        # same trigger, but the newest 1m bar is far beyond the expiry window
        snap = replace_1m(snap, long_1m_bullish(30), trades_end=STRUCT_TS + timedelta(minutes=35))
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_SETUP_EXPIRED, res.reason_codes)

    def test_structural_invalidation_waits(self):
        snap = base_snapshot("bullish")
        # a later 1m bar closes below the order-block bottom -> setup invalidated
        first = STRUCT_TS + timedelta(minutes=5)
        bars = [c(first, 1096.0, 1098.5, 1094.0, 1098.0),
                c(first + timedelta(minutes=1), 1098.0, 1099.0, 1095.0, 1096.0),
                c(first + timedelta(minutes=2), 1096.0, 1097.0, 1060.0, 1062.0)]
        snap = replace_1m(snap, bars, trades_end=bars[-1].ts_utc)
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_SETUP_INVALIDATED, res.reason_codes)


class TestNoLookahead(unittest.TestCase):
    def test_pivot_needs_bars_after_it(self):
        bars = [c(T0 + timedelta(minutes=5 * i), 100, 101 + i, 99, 100) for i in range(10)]
        swings = se.derive_swings(bars, 3)
        # the last two bars cannot be pivots (needs 3 bars on each side)
        self.assertTrue(all(s.ts_utc < bars[-3].ts_utc for s in swings))

    def test_swing_confirmation_ts_follows_pivot(self):
        bars = [c(T0 + timedelta(minutes=5 * i), 100, 105 if i != 5 else 122, 99, 101)
                for i in range(12)]
        swings = se.derive_swings(bars, 3)
        highs = [s for s in swings if s.kind == "high"]
        self.assertTrue(highs)
        for s in highs:
            self.assertGreaterEqual(s.confirmed_ts_utc, s.ts_utc)

    def test_trigger_may_not_precede_its_setup_bar(self):
        # a 1m close above the reference BEFORE the 5m setup bar closed is
        # not an eligible trigger
        snap = base_snapshot("bullish")
        early = STRUCT_TS - timedelta(minutes=6)
        bars = [c(early, 1100.0, 1102.0, 1098.0, 1101.0),
                c(early + timedelta(minutes=1), 1101.0, 1104.0, 1100.0, 1103.0),
                c(STRUCT_TS + timedelta(minutes=6), 1080.0, 1082.0, 1078.0, 1079.0),
                c(STRUCT_TS + timedelta(minutes=7), 1079.0, 1081.0, 1077.0, 1078.0)]
        snap = replace_1m(snap, bars, trades_end=bars[-1].ts_utc)
        res = se.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertIn(se.WAIT_NO_1M_TRIGGER, res.reason_codes)


class TestSignalLifecycle(unittest.TestCase):
    def test_duplicate_suppression(self):
        eng = se.SignalEngine()
        snap = base_snapshot("bullish")
        r1 = eng.evaluate(snap)
        r2 = eng.evaluate(snap)
        self.assertIs(r1.signal, se.Signal.BUY)
        self.assertFalse(r1.duplicate_suppressed)
        self.assertTrue(r2.duplicate_suppressed)
        self.assertEqual(len(eng.active_signals()), 1)
        self.assertTrue(any("DUPLICATE_SUPPRESSED" in t for t in r2.lifecycle_transitions))

    def test_expiry_retires_signal(self):
        eng = se.SignalEngine()
        snap = base_snapshot("bullish")
        eng.evaluate(snap)
        snap = replace_1m(snap, long_1m_bullish(30),
                          trades_end=STRUCT_TS + timedelta(minutes=35))
        eng.evaluate(snap)
        self.assertEqual(eng.active_signals(), [])
        self.assertTrue(any(t["to"] == "EXPIRED" for t in eng.transitions))

    def test_structural_invalidation_retires_signal(self):
        eng = se.SignalEngine()
        snap = base_snapshot("bullish")
        eng.evaluate(snap)
        self.assertEqual(len(eng.active_signals()), 1)
        # a later 1m bar closes below the order-block bottom: the setup the
        # engine just confirmed is structurally invalid
        first = STRUCT_TS + timedelta(minutes=5)
        bars = [c(first, 1096.0, 1098.5, 1094.0, 1098.0),
                c(first + timedelta(minutes=1), 1098.0, 1099.0, 1095.0, 1096.0),
                c(first + timedelta(minutes=2), 1096.0, 1097.0, 1060.0, 1062.0)]
        snap = replace_1m(snap, bars, trades_end=bars[-1].ts_utc)
        res = eng.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertEqual(eng.active_signals(), [])
        self.assertTrue(any(t["to"] == "INVALIDATED" for t in eng.transitions))

    def test_stop_breach_retires_signal(self):
        eng = se.SignalEngine()
        snap = base_snapshot("bullish")
        eng.evaluate(snap)
        self.assertEqual(len(eng.active_signals()), 1)
        # price trades through the proposed stop while no new setup confirms
        flat = [c(STRUCT_TS + timedelta(minutes=5) + timedelta(minutes=i),
                  1078.0, 1080.0, 1076.0, 1078.0) for i in range(3)]
        snap = replace_1m(snap, flat, trades_end=flat[-1].ts_utc)
        object.__setattr__(snap, "last_price", 1000.0)
        eng.evaluate(snap)
        self.assertEqual(eng.active_signals(), [])
        self.assertTrue(any(t["to"] == "INVALIDATED" for t in eng.transitions))

    def test_conflicting_signal_supersedes(self):
        eng = se.SignalEngine()
        eng.evaluate(base_snapshot("bullish"))
        self.assertEqual(len(eng.active_signals()), 1)
        eng.evaluate(base_snapshot("bearish"))
        active = eng.active_signals()
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].direction, "SELL")
        self.assertTrue(any(t["to"] == "SUPERSEDED" for t in eng.transitions))

    def test_reconnect_with_bad_quality_invalidates(self):
        eng = se.SignalEngine()
        snap = base_snapshot("bullish")
        eng.evaluate(snap)
        object.__setattr__(snap, "book_synced", False)
        res = eng.evaluate(snap)
        self.assertIs(res.signal, se.Signal.WAIT)
        self.assertEqual(eng.active_signals(), [])

    def test_no_persistence_across_engine_instances(self):
        eng = se.SignalEngine()
        eng.evaluate(base_snapshot("bullish"))
        fresh = se.SignalEngine()
        self.assertEqual(fresh.active_signals(), [])
        res = fresh.evaluate(base_snapshot("bullish"))
        self.assertIs(res.signal, se.Signal.BUY)
        self.assertFalse(res.duplicate_suppressed)

    def test_deterministic_signal_id(self):
        a = se.evaluate(base_snapshot("bullish"))
        b = se.evaluate(base_snapshot("bullish"))
        self.assertEqual(a.signal_id, b.signal_id)


class TestRiskGeometry(unittest.TestCase):
    def test_buy_geometry(self):
        res = se.evaluate(base_snapshot("bullish"))
        self.assertLess(res.stop_loss, res.invalidation_price)
        self.assertLess(res.invalidation_price, res.proposed_entry)
        self.assertLess(res.proposed_entry, res.take_profit)
        self.assertAlmostEqual(res.risk_distance, res.proposed_entry - res.stop_loss)
        self.assertAlmostEqual(res.reward_distance, res.take_profit - res.proposed_entry)
        self.assertAlmostEqual(res.risk_reward_ratio,
                               res.reward_distance / res.risk_distance)

    def test_sell_geometry(self):
        res = se.evaluate(base_snapshot("bearish"))
        self.assertGreater(res.stop_loss, res.invalidation_price)
        self.assertGreater(res.invalidation_price, res.proposed_entry)
        self.assertGreater(res.proposed_entry, res.take_profit)
        self.assertAlmostEqual(res.risk_distance, res.stop_loss - res.proposed_entry)
        self.assertAlmostEqual(res.reward_distance, res.proposed_entry - res.take_profit)
        self.assertAlmostEqual(res.risk_reward_ratio,
                               res.reward_distance / res.risk_distance)

    def test_expiry_timestamp_uses_completed_1m_candles(self):
        res = se.evaluate(base_snapshot("bullish"))
        trigger = STRUCT_TS + timedelta(minutes=5)
        self.assertEqual(res.expires_at_utc, trigger + timedelta(minutes=3))
        self.assertIsNotNone(res.setup_candle_timestamp)
        self.assertIsNotNone(res.generated_at_utc)

    def test_output_is_serialisable(self):
        import dataclasses
        res = se.evaluate(base_snapshot("bullish"))
        payload = dataclasses.asdict(res)
        self.assertEqual(payload["signal"].value, "BUY")
        self.assertIn("risk_reward_ratio", payload)
        self.assertIn("data_quality_gate_results", payload)


if __name__ == "__main__":
    unittest.main()
