"""signal_engine.py - read-only, deterministic BTCUSD confirmed-entry engine.

Analysis component only. Never executes trades.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

class Signal(Enum):
    BUY = "BUY"
    SELL = "SELL"
    WAIT = "WAIT"


class SignalState(Enum):
    WAIT = auto()
    BUY_CANDIDATE = auto()
    SELL_CANDIDATE = auto()
    BUY_CONFIRMED = auto()
    SELL_CONFIRMED = auto()
    EXPIRED = auto()
    INVALIDATED = auto()
    SUPERSEDED = auto()


# Data quality reason codes
WAIT_DATA_STALE = "WAIT_DATA_STALE"
WAIT_BOOK_UNSYNCED = "WAIT_BOOK_UNSYNCED"
WAIT_CHECKSUM_UNVERIFIED = "WAIT_CHECKSUM_UNVERIFIED"
WAIT_CHECKSUM_FAILED = "WAIT_CHECKSUM_FAILED"
WAIT_SEQUENCE_GAP = "WAIT_SEQUENCE_GAP"
WAIT_NO_CANDLES = "WAIT_NO_CANDLES"
WAIT_INVALID_PRICES = "WAIT_INVALID_PRICES"
WAIT_INVALID_BID_ASK = "WAIT_INVALID_BID_ASK"
WAIT_SPREAD_TOO_WIDE = "WAIT_SPREAD_TOO_WIDE"
WAIT_TIME_INCONSISTENT = "WAIT_TIME_INCONSISTENT"

# Setup reason codes
WAIT_NO_15M_TREND = "WAIT_NO_15M_TREND"
WAIT_NO_SWEEP = "WAIT_NO_SWEEP"
WAIT_NO_STRUCTURE_BREAK = "WAIT_NO_STRUCTURE_BREAK"
WAIT_NO_RELEVANT_OB = "WAIT_NO_RELEVANT_OB"
WAIT_NO_1M_TRIGGER = "WAIT_NO_1M_TRIGGER"
WAIT_FLOW_CONFLICT = "WAIT_FLOW_CONFLICT"
WAIT_FLOW_NO_TRADES = "WAIT_FLOW_NO_TRADES"
WAIT_FLOW_STALE = "WAIT_FLOW_STALE"
WAIT_FLOW_INSUFFICIENT_TRADES = "WAIT_FLOW_INSUFFICIENT_TRADES"
WAIT_NO_PRICE_RESPONSE = "WAIT_NO_PRICE_RESPONSE"
WAIT_NO_VALID_TARGET = "WAIT_NO_VALID_TARGET"
WAIT_RR_TOO_LOW = "WAIT_RR_TOO_LOW"
WAIT_STOP_INVALID = "WAIT_STOP_INVALID"
WAIT_ENTRY_INVALID = "WAIT_ENTRY_INVALID"
WAIT_SETUP_EXPIRED = "WAIT_SETUP_EXPIRED"
WAIT_SETUP_INVALIDATED = "WAIT_SETUP_INVALIDATED"

# Aggressor-side classifier values accepted by the data contract
AGGRESSOR_BUY = "BUY_TAKER"
AGGRESSOR_SELL = "SELL_TAKER"

class ChecksumState(Enum):
    VERIFIED = auto()
    FAILED = auto()
    UNAVAILABLE = auto()
    INDETERMINATE = auto()

@dataclass(frozen=True)
class Swing:
    """A price swing point together with the timestamp at which it became
    knowable. `confirmed_ts_utc` is the timestamp of the `swing_confirm_bars`-th
    candle *after* the pivot, so a pivot is never treated as confirmed before
    that candle closed (no look-ahead / no repainting)."""
    ts_utc: datetime
    price: float
    kind: str  # 'high' | 'low'
    confirmed_ts_utc: datetime

    def __post_init__(self):
        if self.kind not in ("high", "low"):
            raise ValueError("Swing kind must be 'high' or 'low'")
        if not math.isfinite(self.price) or self.price <= 0:
            raise ValueError("Invalid swing price")
        if self.confirmed_ts_utc < self.ts_utc:
            raise ValueError("Swing confirmation precedes the pivot")


@dataclass(frozen=True)
class OrderBlock:
    """A price-action zone derived from completed candles only."""
    ts_utc: datetime
    direction: str  # 'bullish' | 'bearish'
    top: float
    bottom: float
    confirmed_ts_utc: datetime
    active: bool = True

    def __post_init__(self):
        if self.direction not in ("bullish", "bearish"):
            raise ValueError("Order-block direction must be 'bullish' or 'bearish'")
        for v in (self.top, self.bottom):
            if not math.isfinite(v) or v <= 0:
                raise ValueError("Invalid order-block level")
        if self.top < self.bottom:
            raise ValueError("Order-block top must be >= bottom")


@dataclass(frozen=True)
class LiquidityLevel:
    """Observed resting liquidity: an *observed market fact*, not a signal."""
    price: float
    contracts: float
    side: str  # 'ask' | 'bid'
    observed_ts_utc: datetime

    def __post_init__(self):
        if self.side not in ("ask", "bid"):
            raise ValueError("Liquidity side must be 'ask' or 'bid'")
        if not math.isfinite(self.price) or self.price <= 0:
            raise ValueError("Invalid liquidity price")
        if not math.isfinite(self.contracts) or self.contracts < 0:
            raise ValueError("Invalid liquidity quantity")


@dataclass(frozen=True)
class Candle:
    ts_utc: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    def __post_init__(self):
        if not all(math.isfinite(v) for v in (self.open, self.high, self.low, self.close, self.volume)):
            raise ValueError("Invalid candle values")
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("Candle prices must be positive")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("Candle high/low inconsistent with open/close")
        if self.volume < 0:
            raise ValueError("Candle volume must be non-negative")

@dataclass(frozen=True)
class Trade:
    ts_utc: datetime
    price: float
    size: float
    side_aggressor: str  # 'BUY_TAKER' or 'SELL_TAKER' as documented

    def __post_init__(self):
        if not math.isfinite(self.price) or self.price <= 0:
            raise ValueError("Invalid trade price")
        if not math.isfinite(self.size) or self.size <= 0:
            raise ValueError("Invalid trade size")
        if self.side_aggressor not in (AGGRESSOR_BUY, AGGRESSOR_SELL):
            raise ValueError(f"Unknown aggressor classification: {self.side_aggressor}")

@dataclass(frozen=True)
class MarketSnapshot:
    ts_utc: datetime
    last_price: float
    bid: float
    ask: float
    candles_1m: List[Candle] = field(default_factory=list)
    candles_5m: List[Candle] = field(default_factory=list)
    candles_15m: List[Candle] = field(default_factory=list)
    book_synced: bool = False
    connection_healthy: bool = False
    sequence_gap_unresolved: bool = False
    checksum_state: ChecksumState = ChecksumState.UNAVAILABLE
    market_fresh: bool = False
    spread_bps: float = float('inf')
    recent_trades: List[Trade] = field(default_factory=list)
    completed_candle_timestamps: Dict[str, List[datetime]] = field(default_factory=dict)
    # Optional caller-supplied structure metadata. When omitted the engine
    # derives swings / order blocks from the completed 5m candles itself.
    swings_5m: List[Swing] = field(default_factory=list)
    order_blocks_5m: List[OrderBlock] = field(default_factory=list)
    ask_liquidity: List[LiquidityLevel] = field(default_factory=list)
    bid_liquidity: List[LiquidityLevel] = field(default_factory=list)
    atr_5m: Optional[float] = None
    atr_15m: Optional[float] = None

    def __post_init__(self):
        if not math.isfinite(self.last_price) or not math.isfinite(self.bid) or not math.isfinite(self.ask):
            raise ValueError("Invalid prices")
        if self.spread_bps < 0:
            raise ValueError("Invalid spread")
        # Normalise candle ordering so callers cannot pass newest-first data by
        # accident, and reject duplicated bar timestamps outright.
        for name in ("candles_1m", "candles_5m", "candles_15m"):
            object.__setattr__(self, name, _validate_series(name, getattr(self, name)))
        trades = sorted(self.recent_trades, key=lambda t: t.ts_utc)
        object.__setattr__(self, "recent_trades", trades)
        for tr in trades:
            if tr.side_aggressor not in (AGGRESSOR_BUY, AGGRESSOR_SELL):
                raise ValueError(f"Unknown aggressor classification: {tr.side_aggressor}")
        for atr_name in ("atr_5m", "atr_15m"):
            atr = getattr(self, atr_name)
            if atr is not None and (not math.isfinite(atr) or atr < 0):
                raise ValueError(f"Invalid {atr_name}")

@dataclass
class SignalResult:
    signal: Signal = Signal.WAIT
    state: SignalState = SignalState.WAIT
    signal_id: str = ""
    generated_at_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at_utc: Optional[datetime] = None
    proposed_entry: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    invalidation_price: Optional[float] = None
    risk_distance: Optional[float] = None
    reward_distance: Optional[float] = None
    risk_reward_ratio: Optional[float] = None
    setup_timeframe: Optional[str] = None
    setup_candle_timestamp: Optional[datetime] = None
    confirmation_conditions_passed: List[str] = field(default_factory=list)
    confirmation_conditions_failed: List[str] = field(default_factory=list)
    reason_codes: List[str] = field(default_factory=list)
    human_readable_explanation: str = ""
    input_candle_timestamps: Dict[str, List[datetime]] = field(default_factory=dict)
    data_quality_gate_results: Dict[str, bool] = field(default_factory=dict)
    # lifecycle bookkeeping (populated by SignalEngine.evaluate)
    duplicate_suppressed: bool = False
    superseded_signal_id: Optional[str] = None
    lifecycle_transitions: List[str] = field(default_factory=list)

DEFAULT_CONFIG: Dict[str, Any] = {
    # ---- data-quality gates ----
    "spread_bps_max": 20.0,
    "require_checksum_verified": True,
    # ---- structure ----
    "swing_lookback_bars": 3,        # bars required on EACH side of a pivot
    "swing_confirm_bars": 3,         # bars after the pivot before it is confirmed
    "structure_bars_15m": 20,        # minimum 15m bars required for the trend rule
    "ema_fast": 20,
    "ema_slow": 50,
    "sweep_tolerance": 0.0,          # extra penetration beyond the swing (price units)
    "ob_lookback_bars": 12,          # how far back to search for a setup zone
    "ob_tolerance_frac": 0.25,       # frac of ATR allowed to pierce the zone
    "trigger_max_bars": 6,           # max completed 1m bars accepted as a trigger
    # ---- order flow ----
    "flow_window_minutes": 5.0,
    "flow_min_trades": 10,
    "min_price_response_bps": 0.0,   # >0 means strictly-positive response only
    "max_1m_candle_age_minutes": 5.0,
    "max_trade_age_minutes": 10.0,
    # ---- risk ----
    "rr_min": 1.5,
    "atr_buffer_frac": 0.5,          # stop buffer beyond invalidation (fraction of ATR)
    # ---- lifecycle ----
    "expiry_1m_candles": 3,
}


# ===========================================================================
# Derived levels (an observed market fact) versus a derived condition.
# ===========================================================================

@dataclass(frozen=True)
class SwingPoint:
    ts_utc: datetime
    price: float
    kind: str
    confirmed_ts_utc: datetime


def derive_swings(candles: List[Candle], lookback: int) -> List[SwingPoint]:
    """Confirmed pivots using a strict symmetric window.

    A pivot at index i requires `lookback` bars on both sides, and is only
    *confirmed* at the timestamp of the `lookback`-th bar after it, so no
    future candle is used to declare a level.
    """
    out: List[SwingPoint] = []
    if lookback < 1:
        raise ValueError("lookback must be >= 1")
    for i in range(lookback, len(candles) - lookback):
        window = candles[i - lookback:i + lookback + 1]
        if candles[i].high == max(x.high for x in window):
            out.append(SwingPoint(candles[i].ts_utc, candles[i].high, "high",
                                  candles[i + lookback].ts_utc))
        if candles[i].low == min(x.low for x in window):
            out.append(SwingPoint(candles[i].ts_utc, candles[i].low, "low",
                                  candles[i + lookback].ts_utc))
    out.sort(key=lambda s: (s.ts_utc, s.kind))
    return out


def derive_order_block(candles: List[Candle], start_ts: datetime, direction: str,
                       lookback: int, atr: Optional[float], tolerance_frac: float) -> Optional[OrderBlock]:
    """Most recent still-intact zone before `start_ts`.

    For a bullish setup the zone is the last *down* candle before the sweep; it
    is only accepted while no later candle closed below its bottom by more than
    `tolerance_frac * atr`. Bearish is symmetric.
    """
    tolerance = (tolerance_frac * atr) if (atr and atr > 0) else 0.0
    idxs = [i for i, cd in enumerate(candles) if cd.ts_utc < start_ts]
    if not idxs:
        return None
    scan = idxs[-lookback:] if len(idxs) > lookback else idxs
    for i in reversed(scan):
        cd = candles[i]
        if direction == "bullish" and not cd.close < cd.open:
            continue
        if direction == "bearish" and not cd.close > cd.open:
            continue
        bottom, top = cd.low, cd.high
        intact = True
        for later in candles[i + 1:]:
            if later.ts_utc <= start_ts and (
                (direction == "bullish" and later.close < bottom - tolerance) or
                (direction == "bearish" and later.close > top + tolerance)):
                intact = False
                break
        if intact:
            return OrderBlock(ts_utc=cd.ts_utc, direction=direction, top=top, bottom=bottom,
                              confirmed_ts_utc=cd.ts_utc, active=True)
    return None


# ===========================================================================
# Order-flow confirmation
# ===========================================================================

@dataclass(frozen=True)
class FlowAssessment:
    direction: str  # 'bullish' | 'bearish' | 'neutral'
    signed_volume: float
    buy_volume: float
    sell_volume: float
    trade_count: int
    response_bps: float
    reasons: List[str] = field(default_factory=list)


def assess_order_flow(trades: List[Trade], direction: str, cfg: Dict[str, Any],
                      window_minutes: float = 5.0) -> FlowAssessment:
    """Rolling-window aggressor-flow assessment over completed trade prints.

    A BUY_TAKER / SELL_TAKER print is an observed fact only; it is evidence for
    a direction, never an entry trigger by itself.
    """
    if direction not in ("bullish", "bearish"):
        raise ValueError("direction must be 'bullish' or 'bearish'")
    if not trades:
        return FlowAssessment("neutral", 0.0, 0.0, 0.0, 0, 0.0, [WAIT_FLOW_NO_TRADES])
    window_start = trades[-1].ts_utc - timedelta(minutes=window_minutes)
    window = [t for t in trades if window_start <= t.ts_utc <= trades[-1].ts_utc]
    buy = sum(t.size for t in window if t.side_aggressor == AGGRESSOR_BUY)
    sell = sum(t.size for t in window if t.side_aggressor == AGGRESSOR_SELL)
    signed = buy - sell
    n = len(window)
    reasons: List[str] = []
    third = max(1, n // 3)
    early = window[:third]
    late = window[-third:]
    early_px = sum(t.price for t in early) / len(early) if early else 0.0
    late_px = sum(t.price for t in late) / len(late) if late else 0.0
    ref = window[0].price
    response_bps = ((late_px - early_px) / ref * 10000.0) if ref else 0.0

    if n < cfg.get("flow_min_trades", 10):
        reasons.append(WAIT_FLOW_INSUFFICIENT_TRADES)
    if direction == "bullish":
        if signed <= 0:
            reasons.append(WAIT_FLOW_CONFLICT)
        if response_bps <= cfg.get("min_price_response_bps", 0.0):
            reasons.append(WAIT_NO_PRICE_RESPONSE)
        new_direction = "bullish" if signed > 0 else "bearish"
    else:
        if signed >= 0:
            reasons.append(WAIT_FLOW_CONFLICT)
        if response_bps >= -cfg.get("min_price_response_bps", 0.0):
            reasons.append(WAIT_NO_PRICE_RESPONSE)
        new_direction = "bearish" if signed < 0 else "bullish"
    return FlowAssessment(new_direction, signed, buy, sell, n, response_bps, reasons)


# ===========================================================================
# Trade bookkeeping
# ===========================================================================

@dataclass(frozen=True)
class BookTrade:
    direction: str  # 'bullish' | 'bearish'
    setup_tf: str
    setup_ts: datetime
    entry: float
    stop: float
    target: float
    invalidation: float
    trigger_ts: datetime
    swept_level: float
    ob_top: float
    ob_bottom: float
    structure_ref: float
    signal_id: str
    created_ts: datetime
    expires_ts: datetime

def compute_atr(candles: List[Candle], period: int = 14) -> Optional[float]:
    """Simple ATR over completed candles (no look-ahead)."""
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        hi, lo, pc = candles[i].high, candles[i].low, candles[i - 1].close
        trs.append(max(hi - lo, abs(hi - pc), abs(lo - pc)))
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def _trend_15m(candles_15m: List[Candle], cfg: Dict[str, Any]) -> Tuple[str, List[str]]:
    """Documented trend rule: EMA(fast) vs EMA(slow) and price vs EMA(slow)."""
    need = max(cfg["ema_slow"], cfg["structure_bars_15m"])
    if len(candles_15m) < need:
        return "none", [f"WAIT_INSUFFICIENT_15M_HISTORY(need {need}, got {len(candles_15m)})"]
    closes = [c.close for c in candles_15m]
    fast = _ema(closes, cfg["ema_fast"])
    slow = _ema(closes, cfg["ema_slow"])
    price = closes[-1]
    if fast is None or slow is None:
        return "none", ["WAIT_NO_15M_TREND"]
    if fast > slow and price > slow:
        return "bullish", ["15m trend bullish (EMA20>EMA50, price>EMA50)"]
    if fast < slow and price < slow:
        return "bearish", ["15m trend bearish (EMA20<EMA50, price<EMA50)"]
    return "none", ["WAIT_NO_15M_TREND"]


@dataclass(frozen=True)
class Setup:
    """A detected 5m structure setup plus its 1m trigger."""
    direction: str
    swept_level: float
    sweep_ts: datetime
    structure_ref: float
    structure_ts: datetime
    order_block: OrderBlock
    trigger_ts: datetime
    setup_ts: datetime
    expired: bool
    invalidated: bool


def detect_setup(snap: MarketSnapshot, cfg: Dict[str, Any],
                 direction: str) -> Tuple[Optional[Setup], List[str], List[str]]:
    """Detect a completed-candle setup for `direction`.

    Steps (all on CLOSED candles, newest-to-oldest scan, no look-ahead):
      1. sweep of a confirmed swing level with a close back through it
      2. a subsequent close through the confirmed opposing structure level
      3. an intact relevant order block
      4. a completed 1m trigger bar after the structure bar closed
    """
    passed: List[str] = []
    failed: List[str] = []
    c5 = snap.candles_5m
    c1 = snap.candles_1m
    if not c5 or len(c5) < 3 or not c1:
        failed.append(WAIT_NO_CANDLES)
        return None, passed, failed

    swings = snap.swings_5m or derive_swings(c5, cfg["swing_lookback_bars"])
    if not swings:
        failed.append(WAIT_NO_SWEEP)
        return None, passed, failed
    lows = [s for s in swings if s.kind == "low"]
    highs = [s for s in swings if s.kind == "high"]
    if direction == "bullish" and not lows:
        failed.append(WAIT_NO_SWEEP)
        return None, passed, failed
    if direction == "bearish" and not highs:
        failed.append(WAIT_NO_SWEEP)
        return None, passed, failed

    index5 = {cd.ts_utc: i for i, cd in enumerate(c5)}
    sweep_cd = sweep_level = None
    for s in reversed(lows if direction == "bullish" else highs):
        i0 = index5.get(s.ts_utc)
        if i0 is None:
            continue
        for j in range(i0 + 1, len(c5)):
            cd = c5[j]
            pierced = cd.low < s.price - cfg["sweep_tolerance"] if direction == "bullish" \
                else cd.high > s.price + cfg["sweep_tolerance"]
            reclaimed = cd.close > s.price if direction == "bullish" else cd.close < s.price
            if pierced and reclaimed:
                sweep_cd, sweep_level = cd, s.price
                break
        if sweep_cd is not None:
            break
    if sweep_cd is None or sweep_level is None:
        failed.append(WAIT_NO_SWEEP)
        return None, passed, failed
    passed.append("5m sweep of confirmed swing level with close back through it")

    opp = highs if direction == "bullish" else lows
    ref = None
    for s in reversed(opp):
        if s.confirmed_ts_utc <= sweep_cd.ts_utc and index5.get(s.ts_utc, -1) < index5[sweep_cd.ts_utc]:
            ref = s
            break
    if ref is None:
        failed.append(WAIT_NO_STRUCTURE_BREAK)
        return None, passed, failed
    struct_cd = None
    for j in range(index5[sweep_cd.ts_utc] + 1, len(c5)):
        cd = c5[j]
        if (cd.close > ref.price) if direction == "bullish" else (cd.close < ref.price):
            struct_cd = cd
            break
    if struct_cd is None:
        failed.append(WAIT_NO_STRUCTURE_BREAK)
        return None, passed, failed
    passed.append(f"5m structure break past confirmed swing {'high' if direction == 'bullish' else 'low'}")

    atr5 = snap.atr_5m if snap.atr_5m is not None else compute_atr(c5, 14)
    ob = None
    for cand in reversed(snap.order_blocks_5m):
        if cand.direction == direction and cand.ts_utc <= sweep_cd.ts_utc:
            ob = cand
            break
    if ob is None:
        ob = derive_order_block(c5, struct_cd.ts_utc, direction,
                                cfg["ob_lookback_bars"], atr5, cfg["ob_tolerance_frac"])
    if ob is None:
        failed.append(WAIT_NO_RELEVANT_OB)
        return None, passed, failed
    passed.append("relevant intact order block identified")

    ob_edge = ob.top if direction == "bullish" else ob.bottom
    struct_edge = struct_cd.high if direction == "bullish" else struct_cd.low
    ref_px = max(ob_edge, struct_edge) if direction == "bullish" else min(ob_edge, struct_edge)
    trigger = None
    earliest = struct_cd.ts_utc + timedelta(minutes=5)
    eligible = [c for c in c1 if c.ts_utc >= earliest]
    for cd in eligible[-cfg["trigger_max_bars"]:]:
        if (cd.close > ref_px) if direction == "bullish" else (cd.close < ref_px):
            trigger = cd
            break
    if trigger is None:
        failed.append(WAIT_NO_1M_TRIGGER)
        return None, passed, failed
    passed.append("1m trigger bar completed after the setup bar closed")

    last_ts = c1[-1].ts_utc
    span_min = (last_ts - trigger.ts_utc).total_seconds() / 60.0
    expired = span_min > cfg["expiry_1m_candles"]
    invalidated = False
    if not expired:
        for cd in eligible:
            if cd.ts_utc <= trigger.ts_utc:
                continue
            if direction == "bullish" and cd.close < ob.bottom:
                invalidated = True
            elif direction == "bearish" and cd.close > ob.top:
                invalidated = True
    if expired:
        failed.append(WAIT_SETUP_EXPIRED)
    if invalidated:
        failed.append(WAIT_SETUP_INVALIDATED)
    return (Setup(direction, sweep_level, sweep_cd.ts_utc, ref.price, struct_cd.ts_utc,
                  ob, trigger.ts_utc, struct_cd.ts_utc, expired, invalidated),
            passed, failed)


def _select_target(snap: MarketSnapshot, setup: Setup, entry: float,
                   direction: str) -> Tuple[Optional[float], str]:
    """Nearest opposing structure/liquidity target above (BUY) or below (SELL)."""
    levels: List[Tuple[float, str]] = []
    for s in (snap.swings_5m or []):
        if s.kind == "high" and direction == "bullish" and s.price > entry:
            levels.append((s.price, "confirmed 5m swing high"))
        if s.kind == "low" and direction == "bearish" and s.price < entry:
            levels.append((s.price, "confirmed 5m swing low"))
    pool = snap.ask_liquidity if direction == "bullish" else snap.bid_liquidity
    for lv in pool:
        if (lv.side == "ask" and direction == "bullish" and lv.price > entry) or \
           (lv.side == "bid" and direction == "bearish" and lv.price < entry):
            levels.append((lv.price, f"observed {lv.side} liquidity"))
    # nearest opposing level: above the entry for BUY, below it for SELL
    levels.sort(key=lambda t: t[0] if direction == "bullish" else -t[0])
    return (levels[0][0], levels[0][1]) if levels else (None, "no opposing structure/liquidity level")


def _signal_id(direction: str, setup: Setup) -> str:
    """Deterministic identity of a SETUP (not of a floating quote).

    The id is derived from the structural facts only - direction, the 5m setup
    bar, the swept swing level, the structure reference and the zone - so that
    repeatedly re-evaluating the same setup within its expiry window produces the
    same id and is suppressed as a duplicate rather than re-emitted.
    """
    import hashlib
    raw = "|".join([
        direction,
        setup.setup_ts.isoformat(),
        setup.sweep_ts.isoformat(),
        f"{setup.swept_level:.6f}",
        f"{setup.structure_ref:.6f}",
        f"{setup.order_block.top:.6f}",
        f"{setup.order_block.bottom:.6f}",
    ])
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def _evaluate(snap: MarketSnapshot, cfg: Dict[str, Any]) -> Tuple[SignalResult, Optional[Setup]]:
    """Stateless, deterministic evaluation of one validated snapshot."""
    _validate_series("candles_1m", snap.candles_1m)
    _validate_series("candles_5m", snap.candles_5m)
    _validate_series("candles_15m", snap.candles_15m)
    now = snap.ts_utc
    res = SignalResult(generated_at_utc=now)
    res.input_candle_timestamps = {
        '1m': [c.ts_utc for c in snap.candles_1m],
        '5m': [c.ts_utc for c in snap.candles_5m],
        '15m': [c.ts_utc for c in snap.candles_15m],
    }
    res.expires_at_utc = (snap.candles_1m[-1].ts_utc + timedelta(minutes=cfg["expiry_1m_candles"])
                          if snap.candles_1m else now + timedelta(minutes=cfg["expiry_1m_candles"]))

    # ---------------- Phase 2: fail-closed data-quality gates ----------------
    gates: Dict[str, bool] = {
        "connection_healthy": snap.connection_healthy,
        "market_fresh": snap.market_fresh,
        "book_synced": snap.book_synced,
        "no_unresolved_sequence_gap": not snap.sequence_gap_unresolved,
        "candles_present": bool(snap.candles_1m and snap.candles_5m and snap.candles_15m),
        "prices_valid": all(math.isfinite(v) and v > 0 for v in (snap.last_price, snap.bid, snap.ask)),
        "bid_lt_ask": snap.bid < snap.ask,
        "spread_within_limit": snap.spread_bps <= cfg["spread_bps_max"],
        "checksum_ok": (
            snap.checksum_state == ChecksumState.VERIFIED if cfg["require_checksum_verified"]
            else snap.checksum_state in (ChecksumState.VERIFIED, ChecksumState.UNAVAILABLE)),
        "time_consistent": _time_consistent(snap),
        "candles_fresh": _candles_fresh(snap, cfg),
        "trades_fresh": _trades_fresh(snap, cfg),
    }
    res.data_quality_gate_results = gates
    failures: List[str] = []
    if not gates["connection_healthy"] or not gates["market_fresh"]:
        failures.append(WAIT_DATA_STALE)
    if not gates["book_synced"]:
        failures.append(WAIT_BOOK_UNSYNCED)
    if not gates["no_unresolved_sequence_gap"]:
        failures.append(WAIT_SEQUENCE_GAP)
    if not gates["checksum_ok"]:
        failures.append(WAIT_CHECKSUM_FAILED if snap.checksum_state == ChecksumState.FAILED
                        else WAIT_CHECKSUM_UNVERIFIED)
    if not gates["candles_present"]:
        failures.append(WAIT_NO_CANDLES)
    if not gates["prices_valid"]:
        failures.append(WAIT_INVALID_PRICES)
    if not gates["bid_lt_ask"]:
        failures.append(WAIT_INVALID_BID_ASK)
    if not gates["spread_within_limit"]:
        failures.append(WAIT_SPREAD_TOO_WIDE)
    if not gates["time_consistent"]:
        failures.append(WAIT_TIME_INCONSISTENT)
    if not gates["candles_fresh"]:
        failures.append(WAIT_DATA_STALE)
    if not gates["trades_fresh"]:
        failures.append(WAIT_FLOW_STALE)
    if failures:
        res.signal, res.state = Signal.WAIT, SignalState.WAIT
        res.reason_codes = sorted(set(failures))
        res.human_readable_explanation = "Data-quality gates failed: " + ", ".join(res.reason_codes)
        return res, None

    # ---------------- Phase 3: confirmation rules ----------------
    trend, trend_notes = _trend_15m(snap.candles_15m, cfg)
    if trend == "none":
        res.signal, res.state = Signal.WAIT, SignalState.WAIT
        res.reason_codes = [WAIT_NO_15M_TREND]
        res.confirmation_conditions_failed = ["15m directional trend"]
        res.human_readable_explanation = "; ".join(trend_notes)
        return res, None

    direction = trend
    setup, passed, failed = detect_setup(snap, cfg, direction)
    res.confirmation_conditions_passed = passed
    if setup is None:
        res.signal, res.state = Signal.WAIT, SignalState.WAIT
        res.confirmation_conditions_failed = ["5m sweep", "5m structure break",
                                              "order block", "1m trigger"]
        res.reason_codes = failed
        res.human_readable_explanation = "Setup not confirmed: " + ", ".join(failed)
        return res, None
    if setup.expired or setup.invalidated:
        res.signal, res.state = Signal.WAIT, SignalState.WAIT
        res.confirmation_conditions_failed = ["setup still live"]
        res.reason_codes = failed
        res.human_readable_explanation = "Setup no longer valid: " + ", ".join(failed)
        return res, None

    flow = assess_order_flow(snap.recent_trades, direction, cfg,
                             cfg["flow_window_minutes"])
    if flow.reasons:
        res.signal, res.state = Signal.WAIT, SignalState.WAIT
        res.confirmation_conditions_failed = ["order-flow agreement"]
        res.reason_codes = flow.reasons
        res.human_readable_explanation = ("Order-flow confirmation failed: " + ", ".join(flow.reasons))
        return res, setup
    res.confirmation_conditions_passed.append(
        f"order flow agrees (signed volume {flow.signed_volume:+,.0f} contracts, "
        f"{flow.trade_count} prints, price response {flow.response_bps:+.2f} bps)")

    # ---------------- Phase 4: entry and risk validation ----------------
    entry = snap.ask if direction == "bullish" else snap.bid
    if not math.isfinite(entry) or entry <= 0:
        res.signal, res.state = Signal.WAIT, SignalState.WAIT
        res.reason_codes = [WAIT_ENTRY_INVALID]
        res.human_readable_explanation = "No valid executable-side quote for the proposed entry"
        return res, setup
    atr5 = snap.atr_5m if snap.atr_5m is not None else compute_atr(snap.candles_5m, 14) or 0.0
    buffer = cfg["atr_buffer_frac"] * atr5
    invalidation = setup.swept_level
    stop = (invalidation - buffer) if direction == "bullish" else (invalidation + buffer)
    target, target_src = _select_target(snap, setup, entry, direction)
    res.proposed_entry, res.invalidation_price, res.stop_loss = entry, invalidation, stop
    res.setup_timeframe = "5m"
    res.setup_candle_timestamp = setup.setup_ts

    if (direction == "bullish" and stop >= entry) or (direction == "bearish" and stop <= entry):
        res.signal, res.state = Signal.WAIT, SignalState.WAIT
        res.reason_codes = [WAIT_STOP_INVALID]
        res.human_readable_explanation = "Proposed stop is on the wrong side of the entry"
        return res, setup
    if target is None:
        res.signal, res.state = Signal.WAIT, SignalState.WAIT
        res.reason_codes = [WAIT_NO_VALID_TARGET]
        res.human_readable_explanation = "No opposing structure/liquidity target above the entry"
        return res, setup
    if (direction == "bullish" and target <= entry) or (direction == "bearish" and target >= entry):
        res.signal, res.state = Signal.WAIT, SignalState.WAIT
        res.reason_codes = [WAIT_NO_VALID_TARGET]
        res.human_readable_explanation = "Target does not oppose the entry"
        return res, setup

    risk = abs(entry - stop)
    reward = abs(target - entry)
    rr = reward / risk if risk > 0 else 0.0
    res.risk_distance, res.reward_distance, res.risk_reward_ratio = risk, reward, rr
    res.take_profit = target
    if rr < cfg["rr_min"]:
        res.signal, res.state = Signal.WAIT, SignalState.WAIT
        res.reason_codes = [WAIT_RR_TOO_LOW]
        res.human_readable_explanation = (f"Risk/reward {rr:.2f} below the minimum "
                                          f"{cfg['rr_min']:.2f} (target: {target_src})")
        return res, setup

    confirmed = direction == "bullish"
    res.signal = Signal.BUY if confirmed else Signal.SELL
    res.state = SignalState.BUY_CONFIRMED if confirmed else SignalState.SELL_CONFIRMED
    res.signal_id = _signal_id(direction, setup)
    res.expires_at_utc = setup.trigger_ts + timedelta(minutes=cfg["expiry_1m_candles"])
    res.confirmation_conditions_passed.append(
        f"risk validated (RR {rr:.2f} vs minimum {cfg['rr_min']:.2f}; target: {target_src})")
    res.human_readable_explanation = (
        f"{res.signal.value} candidate on completed candles only. Swept swing "
        f"{invalidation:,.2f} at {setup.sweep_ts.isoformat()}; structure break at "
        f"{setup.structure_ts.isoformat()} through {setup.structure_ref:,.2f}; order block "
        f"{setup.order_block.bottom:,.2f}-{setup.order_block.top:,.2f}; 1m trigger "
        f"{setup.trigger_ts.isoformat()}. Proposed (not placed) entry {entry:,.2f}, "
        f"invalidation {invalidation:,.2f}, stop {stop:,.2f}, target {target:,.2f}, "
        f"RR {rr:.2f}. Proposed levels only - no order is placed or guaranteed to fill.")
    return res, setup


def evaluate(snap: MarketSnapshot, config: Optional[Dict[str, Any]] = None) -> SignalResult:
    """Evaluate one validated snapshot and return the signal decision."""
    cfg = dict(DEFAULT_CONFIG)
    if config:
        cfg.update(config)
    res, _setup = _evaluate(snap, cfg)
    return res


def _validate_series(name: str, series: List[Candle]) -> List[Candle]:
    """Re-validate a candle series (used on construction and on evaluation)."""
    ordered = sorted(series, key=lambda cd: cd.ts_utc)
    stamps = [cd.ts_utc for cd in ordered]
    if len(set(stamps)) != len(stamps):
        raise ValueError(f"Duplicate candle timestamps in {name}")
    for i, cd in enumerate(ordered):
        if i and cd.ts_utc <= ordered[i - 1].ts_utc:
            raise ValueError(f"Candles must be strictly increasing in {name}")
        if cd.high < max(cd.open, cd.close) or cd.low > min(cd.open, cd.close):
            raise ValueError(f"Candle {name}[{i}] high/low inconsistent with open/close")
        if cd.low <= 0 or cd.high <= 0:
            raise ValueError(f"Candle {name}[{i}] has non-positive price")
        if cd.volume < 0:
            raise ValueError(f"Candle {name}[{i}] has negative volume")
    return ordered


def _candles_fresh(snap: MarketSnapshot, cfg: Dict[str, Any]) -> bool:
    """The newest completed 1m candle must be recent relative to the snapshot."""
    if not snap.candles_1m:
        return False
    age_min = (snap.ts_utc - snap.candles_1m[-1].ts_utc).total_seconds() / 60.0
    return 0 <= age_min <= cfg["max_1m_candle_age_minutes"]


def _trades_fresh(snap: MarketSnapshot, cfg: Dict[str, Any]) -> bool:
    if not snap.recent_trades:
        return False
    age_min = (snap.ts_utc - snap.recent_trades[-1].ts_utc).total_seconds() / 60.0
    return 0 <= age_min <= cfg["max_trade_age_minutes"]


def _time_consistent(snap: MarketSnapshot) -> bool:
    """Required price/flow inputs must be mutually time-consistent."""
    if not snap.candles_1m:
        return True
    last_candle = snap.candles_1m[-1].ts_utc
    if snap.recent_trades and snap.recent_trades[-1].ts_utc > snap.ts_utc:
        return False
    if snap.candles_15m and snap.candles_15m[-1].ts_utc > snap.ts_utc:
        return False
    if snap.candles_5m and snap.candles_5m[-1].ts_utc > snap.ts_utc:
        return False
    if last_candle > snap.ts_utc:
        return False
    return True


# ===========================================================================
# Phase 5: signal lifecycle
# ===========================================================================

class SignalEngine:
    """Stateful wrapper that owns the active-signal lifecycle.

    Deliberately non-persistent: no file or database state is kept, so a fresh
    engine (e.g. after a restart or reconnect) starts with nothing active and
    every signal must be re-confirmed against a fresh validated snapshot.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.cfg = dict(DEFAULT_CONFIG)
        if config:
            self.cfg.update(config)
        self._active: Dict[str, BookTrade] = {}
        self.transitions: List[Dict[str, str]] = []

    # -- helpers -----------------------------------------------------------
    def _record(self, signal_id: str, to_state: str, reason: str, ts: datetime) -> None:
        self.transitions.append({
            "signal_id": signal_id,
            "from": "ACTIVE" if signal_id in self._active else "-",
            "to": to_state,
            "reason": reason,
            "ts_utc": ts.isoformat(),
        })

    def _retire(self, signal_id: str, reason: str, ts: datetime, state: str) -> None:
        trade = self._active.pop(signal_id, None)
        if trade is None:
            return
        self._record(signal_id, state, reason, ts)

    # -- lifecycle ---------------------------------------------------------
    def _sweep_active(self, snap: MarketSnapshot, now: datetime,
                      setup_reasons: Optional[List[str]] = None) -> List[str]:
        """Retire any active signal that is no longer valid.

        Order of precedence: data-quality failure, structural/stop breach,
        lifecycle expiry. `setup_reasons` carries the reason codes from the
        current evaluation so a setup the engine just judged expired or
        structurally invalid is retired for that exact reason.
        """
        notes: List[str] = []
        bad_quality = _data_quality_failures(snap, self.cfg)
        px = snap.last_price
        for sid in list(self._active):
            trade = self._active[sid]
            if bad_quality:
                self._retire(sid, "data-quality gate failed: " + ", ".join(bad_quality),
                             now, "INVALIDATED")
                notes.append(f"{sid}:INVALIDATED")
                continue
            structural = (px <= trade.invalidation) if trade.direction == "BUY" \
                else (px >= trade.invalidation)
            breached = (px <= trade.stop) if trade.direction == "BUY" else (px >= trade.stop)
            if structural or breached:
                self._retire(sid, f"price {px:,.2f} breached "
                                  f"{'structural invalidation' if structural else 'stop'} "
                                  f"{trade.invalidation if structural else trade.stop:,.2f}",
                             now, "INVALIDATED")
                notes.append(f"{sid}:INVALIDATED")
                continue
            if now > trade.expires_ts:
                self._retire(sid, f"expired after {self.cfg['expiry_1m_candles']} completed 1m candles",
                             now, "EXPIRED")
                notes.append(f"{sid}:EXPIRED")
                continue
            if setup_reasons:
                for code, state in ((WAIT_SETUP_INVALIDATED, "INVALIDATED"),
                                    (WAIT_SETUP_EXPIRED, "EXPIRED")):
                    if code in setup_reasons:
                        self._retire(sid, f"engine re-evaluation reported {code}", now, state)
                        notes.append(f"{sid}:{state}")
                        break
        return notes

    def evaluate(self, snap: MarketSnapshot,
                 now_utc: Optional[datetime] = None) -> SignalResult:
        """Evaluate a snapshot and maintain the active-signal lifecycle."""
        now = now_utc or snap.ts_utc
        lifecycle: List[str] = []
        # 1) retire anything that is no longer valid BEFORE judging the snapshot
        lifecycle += self._sweep_active(snap, now)
        # 2) evaluate
        result, setup = _evaluate(snap, self.cfg)
        if result.signal is Signal.WAIT and result.reason_codes:
            lifecycle += self._sweep_active(snap, now, result.reason_codes)
            result.duplicate_suppressed = result.signal_id in self._active
            result.lifecycle_transitions = lifecycle
            return result

        # a confirmed candidate: resolve against anything already active
        direction = result.signal.value
        for sid in list(self._active):
            trade = self._active[sid]
            if trade.direction != direction:
                self._retire(sid, f"conflicting {direction} setup supersedes", now, "SUPERSEDED")
                lifecycle.append(f"{sid}:SUPERSEDED")
            elif trade.setup_ts < result.setup_candle_timestamp:
                self._retire(sid, "newer setup for the same direction", now, "SUPERSEDED")
                lifecycle.append(f"{sid}:SUPERSEDED")
            elif trade.signal_id == result.signal_id:
                # duplicate: keep the original emission, do not re-emit
                result.duplicate_suppressed = True
                self._record(sid, "ACTIVE", "duplicate confirmed setup suppressed", now)
                lifecycle.append(f"{sid}:DUPLICATE_SUPPRESSED")
                result.lifecycle_transitions = lifecycle
                return result

        trade = BookTrade(direction=direction, setup_tf=result.setup_timeframe,
                          setup_ts=result.setup_candle_timestamp, entry=result.proposed_entry,
                          stop=result.stop_loss, target=result.take_profit,
                          invalidation=result.invalidation_price,
                          trigger_ts=setup.trigger_ts, swept_level=setup.swept_level,
                          ob_top=setup.order_block.top, ob_bottom=setup.order_block.bottom,
                          structure_ref=setup.structure_ref,
                          signal_id=result.signal_id,
                          created_ts=result.generated_at_utc,
                          expires_ts=result.expires_at_utc)
        self._active[result.signal_id] = trade
        self._record(result.signal_id, "CONFIRMED", "all gates passed", now)
        lifecycle.append(f"{result.signal_id}:CONFIRMED")
        result.lifecycle_transitions = lifecycle
        return result

    def active_signals(self) -> List[BookTrade]:
        return list(self._active.values())


def _data_quality_failures(snap: MarketSnapshot, cfg: Dict[str, Any]) -> List[str]:
    bad: List[str] = []
    if not snap.connection_healthy or not snap.market_fresh:
        bad.append(WAIT_DATA_STALE)
    if not snap.book_synced:
        bad.append(WAIT_BOOK_UNSYNCED)
    if snap.sequence_gap_unresolved:
        bad.append(WAIT_SEQUENCE_GAP)
    if snap.checksum_state == ChecksumState.FAILED:
        bad.append(WAIT_CHECKSUM_FAILED)
    if (snap.checksum_state == ChecksumState.UNAVAILABLE) and cfg["require_checksum_verified"]:
        bad.append(WAIT_CHECKSUM_UNVERIFIED)
    if not _candles_fresh(snap, cfg) or not _trades_fresh(snap, cfg):
        bad.append(WAIT_DATA_STALE)
    return bad


def evaluate_snapshot(snap: MarketSnapshot,
                      config: Optional[Dict[str, Any]] = None) -> SignalResult:
    """Backwards-compatible alias for :func:`evaluate`."""
    return evaluate(snap, config)


def _ema(series: List[float], period: int) -> Optional[float]:
    """EMA seeded with the SMA of the first `period` values (no look-ahead)."""
    if period < 1 or len(series) < period:
        return None
    ema_val = sum(series[:period]) / period
    alpha = 2.0 / (period + 1.0)
    for price in series[period:]:
        ema_val = alpha * price + (1 - alpha) * ema_val
    return ema_val
