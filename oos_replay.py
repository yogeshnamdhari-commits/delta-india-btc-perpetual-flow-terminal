"""oos_replay.py - deterministic historical out-of-sample replay harness.

Read-only analysis tooling for `signal_engine`. It reconstructs 5m/15m candles from
1-minute history, replays the frozen signal engine chronologically, simulates the
proposed levels against the recorded tape and reports metrics.

Hard guarantees enforced by this module:

* No look-ahead. A snapshot at time T never contains a bar, bucket or print after T,
  and an aggregated 5m/15m candle is only emitted once its bucket has fully closed.
* No live connections. Data comes only from local CSV/JSON files. No WebSocket client
  and no REST call is made from here.
* No execution. Nothing places, amends or cancels an order.
* No fabricated results. Unless authentic historical files are loaded, this module
  only runs its own synthetic self-tests (see test_oos_replay.py).

This module does NOT perform a real out-of-sample evaluation by itself. It provides
the harness, the cost model, the metric definitions and the look-ahead guards; the
authentic historical data must be supplied separately (see OOS_REPLAY_README.md).
"""
from __future__ import annotations

import csv
import json
import math
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import signal_engine as se

# ---------------------------------------------------------------------------
# Frozen evaluation parameters. Freeze BEFORE any out-of-sample run; changing any
# value requires a new PARAMS_VERSION and a fresh, separately reported run.
# ---------------------------------------------------------------------------
PARAMS_VERSION = "oos-1.0"

#: Explicit, documented relaxation of the default data-quality policy: historical
#: files carry no exchange CRC32, so `ChecksumState.UNAVAILABLE` is fed truthfully and
#: the engine's `require_checksum_verified` default is opted out of. Every report from
#: this harness prints this relaxation next to its results.
FROZEN_ENGINE_OVERRIDES: Dict[str, Any] = {
    "require_checksum_verified": False,
}

#: Replay/cost assumptions. All bps values are of notional.
FROZEN_OOS_CONFIG: Dict[str, Any] = {
    "params_version": PARAMS_VERSION,
    "spread_bps": 2.0,                # assumed quote spread used to derive bid/ask
    "taker_fee_bps": 5.0,             # assumed taker fee per side
    "slippage_bps": 2.0,              # assumed slippage per fill
    "min_history_1m": 900,            # 900 x 1m == 60 completed 15m candles
    "flow_window_minutes": 5.0,
    "max_1m_candle_age_minutes": 5.0,
    "max_trade_age_minutes": 10.0,
    "intrabar_convention": "stop_first",  # conservative when stop+target share a bar
    "entry_reference": "next_bar_open",   # simulated fill reference, not a guarantee
    "assume_funding": "ignored",          # funding is not modelled by default
}

#: Config keys the harness refuses to accept, to catch typos silently changing a run.
_KNOWN_KEYS = set(FROZEN_OOS_CONFIG) | {"engine"}


class ReplayDataError(ValueError):
    """Raised for malformed, unordered, duplicated or non-UTC input data."""


# ---------------------------------------------------------------------------
# Data contract
# ---------------------------------------------------------------------------

def _parse_ts(value: Any) -> datetime:
    """Parse a UTC timestamp. Naive timestamps are rejected, not assumed."""
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            raise ReplayDataError("empty timestamp")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ReplayDataError(f"unparseable timestamp {value!r}") from exc
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ReplayDataError(f"timestamp {value!r} is not timezone-aware (UTC required)")
    return dt.astimezone(timezone.utc)


def signal_audit_key(signal_id: str, decision_ts_utc: datetime) -> str:
    """Stable identity for one emission of a structural signal setup."""
    return f"{signal_id}|{decision_ts_utc.isoformat()}"


def _num(value: Any, name: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ReplayDataError(f"{name} is not numeric: {value!r}") from exc
    if not math.isfinite(out):
        raise ReplayDataError(f"{name} is not finite: {value!r}")
    return out


@dataclass(frozen=True)
class Bar1m:
    ts_utc: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __post_init__(self):
        if self.low <= 0 or self.high < max(self.open, self.close) or \
                self.low > min(self.open, self.close):
            raise ReplayDataError(f"invalid OHLC bar at {self.ts_utc.isoformat()}")
        if self.volume < 0:
            raise ReplayDataError(f"negative volume at {self.ts_utc.isoformat()}")


@dataclass(frozen=True)
class Print1m:
    ts_utc: datetime
    price: float
    size: float
    side_aggressor: str


def load_bars_csv(path: str) -> List[Bar1m]:
    """Load 1m bars from CSV: `ts_utc,open,high,low,close,volume` (UTC required)."""
    out: List[Bar1m] = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        required = {"ts_utc", "open", "high", "low", "close", "volume"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ReplayDataError(f"{path}: missing columns {sorted(missing)}")
        for row in reader:
            out.append(Bar1m(
                ts_utc=_parse_ts(row["ts_utc"]),
                open=_num(row["open"], "open"), high=_num(row["high"], "high"),
                low=_num(row["low"], "low"), close=_num(row["close"], "close"),
                volume=_num(row["volume"], "volume")))
    return out


def load_trades_csv(path: str) -> List[Print1m]:
    """Load prints from CSV: `ts_utc,price,size,side_aggressor` (UTC required)."""
    out: List[Print1m] = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        required = {"ts_utc", "price", "size", "side_aggressor"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ReplayDataError(f"{path}: missing columns {sorted(missing)}")
        for row in reader:
            side = str(row["side_aggressor"]).strip()
            if side not in (se.AGGRESSOR_BUY, se.AGGRESSOR_SELL):
                raise ReplayDataError(f"{path}: unknown aggressor side {side!r}")
            out.append(Print1m(ts_utc=_parse_ts(row["ts_utc"]),
                               price=_num(row["price"], "price"),
                               size=_num(row["size"], "size"),
                               side_aggressor=side))
    return out


def load_bars_json(path: str) -> List[Bar1m]:
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    if isinstance(raw, dict):
        raw = raw.get("candles") or raw.get("bars") or []
    return [Bar1m(ts_utc=_parse_ts(r["ts_utc"]), open=_num(r["open"], "open"),
                  high=_num(r["high"], "high"), low=_num(r["low"], "low"),
                  close=_num(r["close"], "close"), volume=_num(r.get("volume", 0.0), "volume"))
            for r in raw]


def load_trades_json(path: str) -> List[Print1m]:
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    if isinstance(raw, dict):
        raw = raw.get("trades") or raw.get("prints") or []
    return [Print1m(ts_utc=_parse_ts(r["ts_utc"]), price=_num(r["price"], "price"),
                    size=_num(r["size"], "size"),
                    side_aggressor=str(r.get("side_aggressor") or r.get("side")))
            for r in raw]


def validate_chronology(bars: List[Bar1m], *, interval_seconds: int = 60) -> List[str]:
    """Return a list of gap descriptions; raise for disorder or duplicates."""
    notes: List[str] = []
    seen: set = set()
    for i, bar in enumerate(bars):
        if bar.ts_utc in seen:
            raise ReplayDataError(f"duplicate bar timestamp {bar.ts_utc.isoformat()}")
        seen.add(bar.ts_utc)
        if i and bar.ts_utc <= bars[i - 1].ts_utc:
            raise ReplayDataError(
                f"bars are not strictly increasing at {bar.ts_utc.isoformat()}")
        if i:
            delta = (bar.ts_utc - bars[i - 1].ts_utc).total_seconds()
            if delta != interval_seconds:
                notes.append(f"gap {bars[i - 1].ts_utc.isoformat()} -> "
                             f"{bar.ts_utc.isoformat()} ({delta:.0f}s)")
    return notes


# ---------------------------------------------------------------------------
# Candle aggregation (chronological, closed buckets only)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AggregatedBar:
    bucket_ts: datetime
    close_ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    complete: bool


def aggregate_candles(bars: List[Bar1m], period_minutes: int,
                      as_of: Optional[datetime] = None) -> List[AggregatedBar]:
    """Aggregate 1m bars into `period_minutes` buckets.

    A bucket is only returned when `as_of` is None (full history) or when the bucket
    has fully closed by `as_of` (`close_ts <= as_of`). Buckets with missing 1m bars
    are marked `complete=False`; the replay policy is to DROP incomplete buckets
    rather than fabricate them.
    """
    if period_minutes < 1:
        raise ReplayDataError("period_minutes must be >= 1")
    step = int(period_minutes) * 60
    buckets: Dict[int, List[Bar1m]] = {}
    for bar in bars:
        if as_of is not None and bar.ts_utc > as_of:
            continue
        epoch = int(bar.ts_utc.timestamp())
        buckets.setdefault(epoch - (epoch % step), []).append(bar)
    out: List[AggregatedBar] = []
    for bucket_epoch in sorted(buckets):
        members = buckets[bucket_epoch]
        bucket_ts = datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)
        close_ts = datetime.fromtimestamp(bucket_epoch + step, tz=timezone.utc)
        if as_of is not None and close_ts > as_of:
            continue
        expected = period_minutes  # 1m source bars per bucket
        out.append(AggregatedBar(
            bucket_ts=bucket_ts, close_ts=close_ts,
            open=members[0].open, high=max(b.high for b in members),
            low=min(b.low for b in members), close=members[-1].close,
            volume=sum(b.volume for b in members),
            complete=len(members) == expected))
    return out


def to_candles(bars: Iterable[AggregatedBar], drop_incomplete: bool = True) -> List[se.Candle]:
    out: List[se.Candle] = []
    for b in bars:
        if drop_incomplete and not b.complete:
            continue
        out.append(se.Candle(ts_utc=b.bucket_ts, open=b.open, high=b.high,
                             low=b.low, close=b.close, volume=b.volume))
    return out


# ---------------------------------------------------------------------------
# Replay runner
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CostModel:
    taker_fee_bps: float
    slippage_bps: float
    spread_bps: float = 0.0

    def __post_init__(self) -> None:
        for name, value in (("taker_fee_bps", self.taker_fee_bps),
                            ("slippage_bps", self.slippage_bps),
                            ("spread_bps", self.spread_bps)):
            if not math.isfinite(value) or value < 0:
                raise ReplayDataError(f"{name} must be finite and >= 0")

    def _adverse_fraction(self) -> float:
        # OHLC bars are trade-price references: cross half-spread plus slippage
        # on entry and again on exit.
        return (self.spread_bps / 2.0 + self.slippage_bps) / 10000.0

    def entry_price(self, reference: float, direction: str) -> float:
        adj = reference * self._adverse_fraction()
        return reference + adj if direction == "BUY" else reference - adj

    def exit_price(self, level: float, direction: str) -> float:
        adj = level * self._adverse_fraction()
        return level - adj if direction == "BUY" else level + adj

    def fees(self, notional: float) -> float:
        return notional * self.taker_fee_bps / 10000.0



@dataclass(frozen=True)
class SimulationDisposition:
    """Immutable, auditable outcome for one confirmed signal emission."""
    status: str
    reason_code: Optional[str] = None
    rejection_ts_utc: Optional[str] = None
    trade: Optional[Dict[str, Any]] = None


@dataclass
class ReplayStats:
    bars_total: int = 0
    decision_points: int = 0
    skipped_gap_windows: int = 0
    incomplete_buckets_dropped: int = 0
    signals_confirmed: int = 0
    signals_buy: int = 0
    signals_sell: int = 0
    signals_simulated: int = 0
    signals_rejected: int = 0
    simulation_rejection_counts: Dict[str, int] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


class ReplayRunner:
    """Chronologically replays the frozen engine over recorded 1m data."""

    def __init__(self, bars: List[Bar1m], trades: List[Print1m],
                 config: Optional[Dict[str, Any]] = None,
                 engine_config: Optional[Dict[str, Any]] = None,
                 ask_liquidity: Optional[List[se.LiquidityLevel]] = None,
                 bid_liquidity: Optional[List[se.LiquidityLevel]] = None) -> None:
        cfg = dict(FROZEN_OOS_CONFIG)
        if config:
            unknown = set(config) - _KNOWN_KEYS
            if unknown:
                raise ReplayDataError(f"unknown config keys: {sorted(unknown)}")
            cfg.update(config)
        for key in ("spread_bps", "taker_fee_bps", "slippage_bps"):
            if _num(cfg[key], key) < 0:
                raise ReplayDataError(f"{key} must be >= 0")
        if cfg["intrabar_convention"] not in ("stop_first", "target_first"):
            raise ReplayDataError("intrabar_convention must be 'stop_first' or 'target_first'")
        if "min_history_1m" in cfg and int(cfg["min_history_1m"]) < 1:
            raise ReplayDataError("min_history_1m must be >= 1")
        for key in ("flow_window_minutes", "max_1m_candle_age_minutes",
                    "max_trade_age_minutes"):
            if _num(cfg[key], key) <= 0:
                raise ReplayDataError(f"{key} must be > 0")
        self.cfg = cfg
        self.engine_cfg = dict(se.DEFAULT_CONFIG)
        self.engine_cfg.update(FROZEN_ENGINE_OVERRIDES)
        if engine_config:
            self.engine_cfg.update(engine_config)
        self.bars = sorted(bars, key=lambda b: b.ts_utc)
        self.trades = sorted(trades, key=lambda t: t.ts_utc)
        self.gap_notes = validate_chronology(self.bars)
        self._ts = [b.ts_utc for b in self.bars]
        self._trade_ts = [t.ts_utc for t in self.trades]
        self.ask_liquidity = sorted(ask_liquidity or [], key=lambda l: l.observed_ts_utc)
        self.bid_liquidity = sorted(bid_liquidity or [], key=lambda l: l.observed_ts_utc)
        self.stats = ReplayStats(bars_total=len(self.bars), notes=list(self.gap_notes))
        self.incomplete_buckets = sum(
            not b.complete for b in aggregate_candles(self.bars, 5)) + sum(
            not b.complete for b in aggregate_candles(self.bars, 15))
        self.stats.incomplete_buckets_dropped = self.incomplete_buckets
        self.first_decision_ts = self.bars[0].ts_utc + timedelta(
            minutes=cfg["min_history_1m"]) if self.bars else None

    # -- snapshot construction ------------------------------------------------
    def snapshot_at(self, now: datetime) -> Optional[se.MarketSnapshot]:
        """Build a snapshot that only uses data available at `now`."""
        if not self.bars or now < self.first_decision_ts:
            return None
        # Each bar timestamp is its open. Exclude the bar beginning at now; its
        # eventual high/low/close are not knowable at the decision timestamp.
        last_closed_open = now - timedelta(minutes=1)
        hi = bisect_right(self._ts, last_closed_open)
        # Bound the rolling calculation window to avoid O(N^2) multi-month replays.
        lookback = max(int(self.cfg["min_history_1m"]), 960)
        visible = self.bars[max(0, hi - lookback):hi]
        if not visible:
            return None
        c1 = [se.Candle(ts_utc=b.ts_utc, open=b.open, high=b.high, low=b.low,
                        close=b.close, volume=b.volume) for b in visible]
        c5 = to_candles(aggregate_candles(visible, 5, as_of=now))
        c15 = to_candles(aggregate_candles(visible, 15, as_of=now))
        if not (c1 and c5 and c15):
            return None
        last = visible[-1]
        price = last.close
        half = self.cfg["spread_bps"] / 2.0 / 10000.0
        # Prints stamped exactly at decision time are not visible until after it.
        trade_hi = bisect_left(self._trade_ts, now)
        window_start = now - timedelta(minutes=self.cfg["flow_window_minutes"])
        trade_lo = bisect_left(self._trade_ts, window_start)
        trades = [se.Trade(ts_utc=t.ts_utc, price=t.price, size=t.size,
                           side_aggressor=t.side_aggressor)
                  for t in self.trades[trade_lo:trade_hi]]
        return se.MarketSnapshot(
            ts_utc=now,
            last_price=price,
            bid=price * (1 - half),
            ask=price * (1 + half),
            candles_1m=c1, candles_5m=c5, candles_15m=c15,
            book_synced=True,
            connection_healthy=True,
            sequence_gap_unresolved=False,
            # Historical files carry no exchange CRC32: reported truthfully as
            # UNAVAILABLE and paired with the explicit opt-out override.
            checksum_state=se.ChecksumState.UNAVAILABLE,
            market_fresh=True,
            spread_bps=self.cfg["spread_bps"],
            recent_trades=trades,
            atr_5m=se.compute_atr(c5, 14),
            atr_15m=se.compute_atr(c15, 14),
            # swings derived only from 5m candles already closed at `now`
            swings_5m=se.derive_swings(c5, self.engine_cfg["swing_lookback_bars"]),
            ask_liquidity=[l for l in self.ask_liquidity if l.observed_ts_utc <= now],
            bid_liquidity=[l for l in self.bid_liquidity if l.observed_ts_utc <= now],
        )

    def iter_snapshots(self) -> Iterable[Tuple[datetime, se.MarketSnapshot]]:
        min_history = int(self.cfg["min_history_1m"])
        for i, bar in enumerate(self.bars):
            # Decision time is this bar's open, after the previous bar has closed.
            now = bar.ts_utc
            if now < self.first_decision_ts:
                continue
            window = self.bars[max(0, i - min_history):i]
            if len(window) != min_history or any(
                (window[j].ts_utc - window[j - 1].ts_utc).total_seconds() != 60
                for j in range(1, len(window))
            ):
                self.stats.skipped_gap_windows += 1
                continue
            snap = self.snapshot_at(now)
            if snap is None or len(snap.candles_1m) < min_history:
                continue
            self.stats.decision_points += 1
            yield now, snap


    # -- signal simulation ----------------------------------------------------
    def simulate_with_disposition(self, result: se.SignalResult,
                                  entry_ts_utc: datetime,
                                  entry_bar: Optional[Bar1m]) -> SimulationDisposition:
        """Resolve one signal and return an explicit simulated/rejected outcome.

        The expiry timestamp is an exclusive boundary for entry eligibility and
        for every one-minute bar inspected on the path.
        """
        entry_ts = _parse_ts(entry_ts_utc)
        expiry_raw = result.expires_at_utc
        if expiry_raw is None:
            return SimulationDisposition(
                "REJECTED", "REJECTED_INVALID_LEVELS", entry_ts.isoformat())
        expiry_ts = _parse_ts(expiry_raw)

        # Rejection precedence is intentional: an expired entry stays classified
        # as expired even when the entry bar is absent from the historical file.
        if entry_ts >= expiry_ts:
            return SimulationDisposition(
                "REJECTED", "REJECTED_EXPIRED_AT_ENTRY", entry_ts.isoformat())

        if expiry_ts.second != 0 or expiry_ts.microsecond != 0:
            return SimulationDisposition(
                "REJECTED", "REJECTED_EXPIRY_NOT_BAR_ALIGNED", expiry_ts.isoformat())

        if entry_bar is None or _parse_ts(entry_bar.ts_utc) != entry_ts:
            return SimulationDisposition(
                "REJECTED", "REJECTED_MISSING_ENTRY_BAR", entry_ts.isoformat())

        start = bisect_left(self._ts, entry_ts)
        if start >= len(self.bars) or self.bars[start].ts_utc != entry_ts:
            return SimulationDisposition(
                "REJECTED", "REJECTED_MISSING_ENTRY_BAR", entry_ts.isoformat())

        direction = "BUY" if result.signal is se.Signal.BUY else (
            "SELL" if result.signal is se.Signal.SELL else None)
        planned = (result.proposed_entry, result.stop_loss, result.take_profit)
        if direction is None or any(
            value is None or not math.isfinite(value) or value <= 0
            for value in planned
        ):
            return SimulationDisposition(
                "REJECTED", "REJECTED_INVALID_LEVELS", entry_ts.isoformat())
        if (result.invalidation_price is not None and
                (not math.isfinite(result.invalidation_price) or result.invalidation_price <= 0)):
            return SimulationDisposition(
                "REJECTED", "REJECTED_INVALID_LEVELS", entry_ts.isoformat())

        planned_entry = float(result.proposed_entry)
        stop = float(result.stop_loss)
        target = float(result.take_profit)
        planned_risk = abs(planned_entry - stop)
        valid_geometry = (stop < planned_entry < target) if direction == "BUY" else (
            target < planned_entry < stop)
        if not valid_geometry or not math.isfinite(planned_risk) or planned_risk <= 0:
            return SimulationDisposition(
                "REJECTED", "REJECTED_INVALID_LEVELS", entry_ts.isoformat())
        if result.invalidation_price is not None:
            invalidation = float(result.invalidation_price)
            valid_invalidation = (stop < invalidation < planned_entry) if direction == "BUY" else (
                planned_entry < invalidation < stop)
            if not valid_invalidation:
                return SimulationDisposition(
                    "REJECTED", "REJECTED_INVALID_LEVELS", entry_ts.isoformat())

        cost = CostModel(self.cfg["taker_fee_bps"], self.cfg["slippage_bps"],
                         self.cfg["spread_bps"])
        entry_fill = cost.entry_price(entry_bar.open, direction)
        if not math.isfinite(entry_fill) or entry_fill <= 0:
            return SimulationDisposition(
                "REJECTED", "REJECTED_INVALID_LEVELS", entry_ts.isoformat())
        entry_fee = cost.fees(entry_fill)
        if direction == "BUY" and not (stop < entry_fill < target):
            return SimulationDisposition(
                "REJECTED", "REJECTED_ENTRY_GAP_THROUGH_LEVEL", entry_ts.isoformat())
        if direction == "SELL" and not (target < entry_fill < stop):
            return SimulationDisposition(
                "REJECTED", "REJECTED_ENTRY_GAP_THROUGH_LEVEL", entry_ts.isoformat())

        risk = abs(entry_fill - stop)
        if not math.isfinite(risk) or risk <= 0:
            return SimulationDisposition(
                "REJECTED", "REJECTED_INVALID_LEVELS", entry_ts.isoformat())

        outcome, exit_fill, exit_bar_open_ts = None, None, None
        last_observed_bar = None
        previous_bar = None
        for bar in self.bars[start:]:
            if previous_bar is not None:
                expected_next = previous_bar.ts_utc + timedelta(minutes=1)
                # A later observed bar (including one at expiry) proves a gap
                # when an expected minute strictly before expiry is absent.
                if expected_next < expiry_ts and bar.ts_utc != expected_next:
                    return SimulationDisposition(
                        "REJECTED", "REJECTED_PATH_GAP", expected_next.isoformat())
            # Bar timestamps represent opens; a bar starting exactly at expiry
            # is excluded, even if its OHLC touches the stop or target.
            if bar.ts_utc >= expiry_ts:
                break
            previous_bar = bar
            last_observed_bar = bar
            hit_stop = bar.low <= stop if direction == "BUY" else bar.high >= stop
            hit_target = bar.high >= target if direction == "BUY" else bar.low <= target
            if self.cfg["intrabar_convention"] == "stop_first":
                if hit_stop:
                    outcome, exit_fill, exit_bar_open_ts = "stop", stop, bar.ts_utc
                    break
                if hit_target:
                    outcome, exit_fill, exit_bar_open_ts = "target", target, bar.ts_utc
                    break
            else:
                if hit_target:
                    outcome, exit_fill, exit_bar_open_ts = "target", target, bar.ts_utc
                    break
                if hit_stop:
                    outcome, exit_fill, exit_bar_open_ts = "stop", stop, bar.ts_utc
                    break

        if exit_fill is None:
            if last_observed_bar is None:
                return SimulationDisposition(
                    "REJECTED", "REJECTED_INCOMPLETE_HISTORY_BEFORE_EXPIRY",
                    entry_ts.isoformat())
            first_unavailable_ts = last_observed_bar.ts_utc + timedelta(minutes=1)
            if first_unavailable_ts < expiry_ts:
                return SimulationDisposition(
                    "REJECTED", "REJECTED_INCOMPLETE_HISTORY_BEFORE_EXPIRY",
                    first_unavailable_ts.isoformat())
            if first_unavailable_ts != expiry_ts:
                return SimulationDisposition(
                    "REJECTED", "REJECTED_INCOMPLETE_HISTORY_BEFORE_EXPIRY",
                    first_unavailable_ts.isoformat())
            outcome, exit_fill, exit_bar_open_ts = (
                "timeout", last_observed_bar.close, last_observed_bar.ts_utc)
            exit_ts = expiry_ts
        else:
            exit_ts = exit_bar_open_ts

        exit_fill = cost.exit_price(exit_fill, direction)
        exit_fee = cost.fees(exit_fill)
        pnl_per_unit = (exit_fill - entry_fill) if direction == "BUY" else (entry_fill - exit_fill)
        gross_r = pnl_per_unit / risk
        costs_r = (entry_fee + exit_fee) / risk
        trade = {
            "signal_id": result.signal_id,
            "decision_ts_utc": result.generated_at_utc.isoformat() if result.generated_at_utc else None,
            "direction": direction,
            "trigger_ts_utc": (result.trigger_ts_utc.isoformat() if result.trigger_ts_utc
                               else (expiry_ts - timedelta(minutes=self.engine_cfg["expiry_1m_candles"])).isoformat()),
            "planned_entry": result.proposed_entry,
            "simulated_entry": entry_fill,
            "stop_loss": result.stop_loss,
            "take_profit": result.take_profit,
            "planned_rr": result.risk_reward_ratio,
            "exit_fill": exit_fill,
            "exit_ts_utc": exit_ts.isoformat(),
            "exit_bar_open_ts_utc": exit_bar_open_ts.isoformat(),
            "outcome": outcome,
            "gross_r": gross_r,
            "costs_r": costs_r,
            "net_r": gross_r - costs_r,
            "fees_currency_equiv": entry_fee + exit_fee,
            "spread_bps_assumption": self.cfg["spread_bps"],
            "slippage_bps_per_fill": self.cfg["slippage_bps"],
            "taker_fee_bps_per_side": self.cfg["taker_fee_bps"],
        }
        return SimulationDisposition("SIMULATED", trade=trade)

    def simulate(self, result: se.SignalResult,
                 entry_bar: Optional[Bar1m]) -> Optional[Dict[str, Any]]:
        """Compatibility wrapper returning trade-or-None as before."""
        if entry_bar is None:
            return None
        return self.simulate_with_disposition(result, entry_bar.ts_utc, entry_bar).trade



def compute_metrics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Metrics in R multiples (currency conversion needs the contract multiplier)."""
    if not trades:
        return {"trade_count": 0}
    net = [t["net_r"] for t in trades]
    wins = [r for r in net if r > 0]
    losses = [r for r in net if r <= 0]
    curve, peak, max_dd = 0.0, 0.0, 0.0
    for r in net:
        curve += r
        peak = max(peak, curve)
        max_dd = min(max_dd, curve - peak)
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "trade_count": len(net),
        "wins": len(wins),
        "losses": len(losses),
        "timeouts": sum(1 for t in trades if t["outcome"] == "timeout"),
        "win_rate": len(wins) / len(net),
        "avg_win_r": (gross_win / len(wins)) if wins else 0.0,
        "avg_loss_r": (gross_loss / len(losses)) if losses else 0.0,
        "expectancy_r": sum(net) / len(net),
        "profit_factor": (gross_win / gross_loss) if gross_loss else float("inf"),
        "max_drawdown_r": max_dd,
        "net_r_total": sum(net),
        "by_outcome": {
            "target": sum(1 for t in trades if t["outcome"] == "target"),
            "stop": sum(1 for t in trades if t["outcome"] == "stop"),
            "timeout": sum(1 for t in trades if t["outcome"] == "timeout"),
        },
        "by_direction": {
            "BUY": sum(1 for t in trades if t["direction"] == "BUY"),
            "SELL": sum(1 for t in trades if t["direction"] == "SELL"),
        },
    }


def run_replay(bars: List[Bar1m], trades: List[Print1m],
               config: Optional[Dict[str, Any]] = None,
               ask_liquidity: Optional[List[se.LiquidityLevel]] = None,
               bid_liquidity: Optional[List[se.LiquidityLevel]] = None) -> Dict[str, Any]:
    """Replay the frozen engine and return signals, audit dispositions and metrics.

    Synthetic data is fine for self-tests. With authentic historical data this
    returns the out-of-sample measurements; it never returns a profitability claim
    on its own.
    """
    runner = ReplayRunner(bars, trades, config,
                          ask_liquidity=ask_liquidity, bid_liquidity=bid_liquidity)
    engine = se.SignalEngine(runner.engine_cfg)
    signals: List[Dict[str, Any]] = []
    expired_candidates: List[Dict[str, Any]] = []
    sims: List[Dict[str, Any]] = []
    confirmed_signal_ids: set[str] = set()
    for now, snap in runner.iter_snapshots():
        result = engine.evaluate(snap, now_utc=now)
        if result.signal is se.Signal.WAIT:
            if (se.WAIT_SETUP_EXPIRED in result.reason_codes
                    and result.signal_id not in confirmed_signal_ids):
                required_audit_fields = {
                    "signal_id": result.signal_id,
                    "decision_ts_utc": result.generated_at_utc,
                    "trigger_ts_utc": result.trigger_ts_utc,
                    "last_completed_candle_ts_utc": result.last_completed_candle_ts_utc,
                    "setup_candle_timestamp": result.setup_candle_timestamp,
                    "expires_at_utc": result.expires_at_utc,
                }
                missing_fields = [name for name, value in required_audit_fields.items()
                                  if value is None or (name == "signal_id" and not value)]
                if missing_fields:
                    raise ReplayDataError(
                        "expired candidate missing audit metadata: " + ", ".join(missing_fields))
                candidate = {
                    "signal_id": result.signal_id,
                    "audit_event_key": signal_audit_key(result.signal_id, result.generated_at_utc),
                    "decision_ts_utc": result.generated_at_utc.isoformat(),
                    "trigger_ts_utc": result.trigger_ts_utc.isoformat()
                    if result.trigger_ts_utc else None,
                    "last_completed_candle_ts_utc": result.last_completed_candle_ts_utc.isoformat()
                    if result.last_completed_candle_ts_utc else None,
                    "setup_candle_timestamp": result.setup_candle_timestamp.isoformat()
                    if result.setup_candle_timestamp else None,
                    "expires_at_utc": result.expires_at_utc.isoformat()
                    if result.expires_at_utc else None,
                    "reason_code": se.WAIT_SETUP_EXPIRED,
                }
                expired_candidates.append(candidate)
            continue
        if result.duplicate_suppressed:
            continue

        if result.signal_id:
            confirmed_signal_ids.add(result.signal_id)
        runner.stats.signals_confirmed += 1
        if result.signal is se.Signal.BUY:
            runner.stats.signals_buy += 1
        else:
            runner.stats.signals_sell += 1
        entry_bar = None
        # The current bar opens at `now`; its open is the next-bar fill reference.
        start = bisect_left(runner._ts, now)
        if start < len(runner.bars) and runner.bars[start].ts_utc == now:
            entry_bar = runner.bars[start]

        disposition = runner.simulate_with_disposition(result, now, entry_bar)
        audit_key = signal_audit_key(result.signal_id, now)
        rec = {
            "signal_id": result.signal_id,
            "signal": result.signal.value,
            "decision_ts_utc": now.isoformat(),
            "setup_candle_ts_utc": result.setup_candle_timestamp.isoformat()
            if result.setup_candle_timestamp else None,
            "entry": result.proposed_entry, "stop": result.stop_loss,
            "target": result.take_profit, "invalidation": result.invalidation_price,
            "rr": result.risk_reward_ratio, "expires_ts_utc": result.expires_at_utc.isoformat(),
            "reason_codes": list(result.reason_codes),
            "audit_event_key": audit_key,
            "simulation_status": disposition.status,
            "simulation_rejection_reason": disposition.reason_code,
            "rejection_ts_utc": disposition.rejection_ts_utc,
        }
        signals.append(rec)
        if disposition.status == "SIMULATED" and disposition.trade is not None:
            runner.stats.signals_simulated += 1
            sims.append(disposition.trade)
        elif disposition.status == "REJECTED":
            runner.stats.signals_rejected += 1
            reason = disposition.reason_code or "REJECTED_UNKNOWN"
            runner.stats.simulation_rejection_counts[reason] = (
                runner.stats.simulation_rejection_counts.get(reason, 0) + 1)
        else:
            # The disposition contract is closed: a malformed outcome must not
            # silently escape the reconciliation counters.
            raise ReplayDataError(
                f"invalid simulation disposition for {audit_key}: {disposition!r}")

    return {
        "params_version": PARAMS_VERSION,
        "config": {k: v for k, v in runner.cfg.items()},
        "engine_overrides": dict(FROZEN_ENGINE_OVERRIDES),
        "policy_relaxation": (
            "require_checksum_verified disabled: historical files carry no exchange "
            "CRC32, so checksum_state is UNAVAILABLE. This is an explicit opt-in and "
            "is reported with every result."),
        "stats": {
            "bars_total": runner.stats.bars_total,
            "decision_points": runner.stats.decision_points,
            "skipped_gap_windows": runner.stats.skipped_gap_windows,
            "incomplete_buckets_dropped": runner.stats.incomplete_buckets_dropped,
            "signals_confirmed": runner.stats.signals_confirmed,
            "signals_simulated": runner.stats.signals_simulated,
            "signals_rejected": runner.stats.signals_rejected,
            "simulation_rejection_counts": dict(sorted(
                runner.stats.simulation_rejection_counts.items())),
            "gaps": runner.stats.notes,
        },
        "signals": signals,
        "expired_candidates": expired_candidates,
        "simulated_trades": sims,
        "metrics": compute_metrics(sims),
    }


def main(argv: Optional[List[str]] = None) -> int:
    print("oos_replay: historical out-of-sample replay harness (read-only)")
    print(f"params_version={PARAMS_VERSION}")
    print("usage: python3 -c \"import oos_replay as o; "
          "print(o.run_replay(o.load_bars_csv('bars.csv'), o.load_trades_csv('trades.csv'))['metrics'])\"")
    print("No data is fetched. See OOS_REPLAY_README.md for the required schema and "
          "for how authentic Delta history is obtained from public REST endpoints.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
