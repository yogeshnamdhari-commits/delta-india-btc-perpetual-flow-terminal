#!/usr/bin/env python3
"""shadow_test.py — Prospective shadow-testing engine for the frozen BTCUSD strategy.

Runs alongside the live Delta feed on read-only market data. Every confirmed
signal from the frozen SignalEngine is recorded as a hypothetical shadow trade
with entry, stop-loss, take-profit and invalidation levels derived from the
same frozen execution assumptions used by the signal engine.

Shadow trades are tracked against live price updates until they reach a
terminal state (target hit, stop hit, invalidated, or expired). Completed
shadow trades are counted toward the 100-trade holdout threshold required
before any live-trading consideration.

No orders are ever placed. This module is strictly for prospective validation.
"""
from __future__ import annotations

import json
import os
import threading
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from signal_engine import (
    Signal,
    SignalResult,
    SignalEngine,
    BookTrade,
    AGGRESSOR_BUY,
    AGGRESSOR_SELL,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SHADOW_LOG_FILE = os.environ.get(
    "DELTA_SHADOW_LOG_FILE",
    "/Users/targetmobile/Downloads/delta-live-recovery/shadow_trades.jsonl",
)

SHADOW_DECISION_LOG = os.environ.get(
    "DELTA_SHADOW_DECISION_LOG",
    "/Users/targetmobile/Downloads/delta-live-recovery/shadow_decisions.jsonl",
)


# ---------------------------------------------------------------------------
# Shadow trade record
# ---------------------------------------------------------------------------

@dataclass
class ShadowTrade:
    """A hypothetical trade derived from a confirmed signal result.

    Fields mirror the frozen execution assumptions in SignalResult so that
    shadow P&L can be computed against live market data without any live
    order execution.
    """
    signal_id: str
    direction: str  # 'BUY' | 'SELL'
    setup_tf: str
    setup_ts_utc: str
    entry: float
    stop_loss: float
    take_profit: float
    invalidation_price: float
    risk_distance: Optional[float]
    reward_distance: Optional[float]
    risk_reward_ratio: Optional[float]
    trigger_ts_utc: Optional[str]
    created_ts_utc: str
    expires_ts_utc: Optional[str]
    swept_level: float
    ob_top: float
    ob_bottom: float
    structure_ref: float
    reason_codes: List[str] = field(default_factory=list)
    human_readable: str = ""

    # Lifecycle state (populated as the shadow trade is tracked)
    state: str = "OPEN"  # OPEN | TARGET_HIT | STOP_HIT | INVALIDATED | EXPIRED
    exit_price: Optional[float] = None
    exit_ts_utc: Optional[str] = None
    exit_reason: Optional[str] = None
    pnl_contracts: Optional[float] = None  # +1 or -1 per contract (direction-adjusted)
    pnl_r: Optional[float] = None          # P&L in R units


# ---------------------------------------------------------------------------
# Shadow test engine
# ---------------------------------------------------------------------------

class ShadowTestEngine:
    """Tracks hypothetical shadow trades derived from confirmed signal results.

    For every SignalResult with signal in (BUY, SELL) and state CONFIRMED,
    a ShadowTrade is created and tracked against live price updates.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.cfg = dict(config or {})
        self._lock = threading.Lock()
        self._open_trades: Dict[str, ShadowTrade] = {}
        self._completed_trades: List[ShadowTrade] = []
        self._all_signals: List[Dict[str, Any]] = []
        self._shadow_engine = SignalEngine(config)
        self.total_signals_evaluated = 0
        self.total_confirmed = 0
        self.total_wait = 0
        self.total_blocked = 0
        # Read log paths at init time so environment overrides take effect
        self._shadow_log_path = os.environ.get(
            "DELTA_SHADOW_LOG_FILE", SHADOW_LOG_FILE
        )
        self._shadow_decision_path = os.environ.get(
            "DELTA_SHADOW_DECISION_LOG", SHADOW_DECISION_LOG
        )
        # Crash-safe restoration: reload open and completed trades from
        # existing JSONL records so state survives application restart.
        self._restore_from_jsonl()

    # -- crash-safe restoration ---------------------------------------------

    def _restore_from_jsonl(self) -> None:
        """Rebuild open and completed shadow trades from persisted JSONL.

        Reads DELTA_SHADOW_LOG_FILE (or default) if it exists, restoring
        terminal-state trades into _completed_trades and still-open trades
        into _open_trades. Counters are recomputed from restored state.
        """
        if not self._shadow_log_path or not os.path.exists(self._shadow_log_path):
            return
        try:
            latest_by_signal: Dict[str, Dict[str, Any]] = {}
            with open(self._shadow_log_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    sid = record.get("signal_id", "")
                    if not sid:
                        continue
                    created = record.get("created_ts_utc", "")
                    existing = latest_by_signal.get(sid)
                    if existing is None or created >= existing.get("created_ts_utc", ""):
                        latest_by_signal[sid] = record
            for record in latest_by_signal.values():
                state = record.get("state", "")
                trade = self._record_to_shadow_trade(record)
                if trade is None:
                    continue
                if state in ("TARGET_HIT", "STOP_HIT", "INVALIDATED", "EXPIRED"):
                    self._completed_trades.append(trade)
                elif state == "OPEN":
                    self._open_trades[trade.signal_id] = trade
        except OSError:
            return
        # Recompute counters from restored state
        self.total_confirmed = len(self._completed_trades) + len(self._open_trades)
        self.total_signals_evaluated = self.total_confirmed
        # total_wait and total_blocked cannot be recovered from trade records
        # alone (they require decision-log replay); they remain zero.

    @staticmethod
    def _record_to_shadow_trade(record: Dict[str, Any]) -> Optional[ShadowTrade]:
        """Convert a persisted JSONL record back into a ShadowTrade."""
        try:
            return ShadowTrade(
                signal_id=str(record.get("signal_id", "")),
                direction=str(record.get("direction", "BUY")),
                setup_tf=str(record.get("setup_tf", "")),
                setup_ts_utc=str(record.get("setup_ts_utc", "")),
                entry=float(record.get("entry") or 0.0),
                stop_loss=float(record.get("stop_loss") or 0.0),
                take_profit=float(record.get("take_profit") or 0.0),
                invalidation_price=float(record.get("invalidation_price") or 0.0),
                risk_distance=record.get("risk_distance"),
                reward_distance=record.get("reward_distance"),
                risk_reward_ratio=record.get("risk_reward_ratio"),
                trigger_ts_utc=record.get("trigger_ts_utc"),
                created_ts_utc=str(record.get("created_ts_utc", "")),
                expires_ts_utc=record.get("expires_ts_utc"),
                swept_level=float(record.get("swept_level") or 0.0),
                ob_top=float(record.get("ob_top") or 0.0),
                ob_bottom=float(record.get("ob_bottom") or 0.0),
                structure_ref=float(record.get("structure_ref") or 0.0),
                reason_codes=list(record.get("reason_codes") or []),
                human_readable=str(record.get("human_readable", "")),
                state=str(record.get("state", "OPEN")),
                exit_price=record.get("exit_price"),
                exit_ts_utc=record.get("exit_ts_utc"),
                exit_reason=record.get("exit_reason"),
                pnl_contracts=record.get("pnl_contracts"),
                pnl_r=record.get("pnl_r"),
            )
        except (TypeError, ValueError, OverflowError):
            return None

    # -- signal ingestion ----------------------------------------------------

    def ingest_result(self, result: SignalResult, market_snapshot: Dict[str, Any]) -> None:
        """Ingest a SignalResult plus the matching market snapshot.

        Creates a shadow trade for confirmed results; persists the decision
        record regardless of outcome.
        """
        with self._lock:
            self.total_signals_evaluated += 1
            sig = result.signal
            sig_value = sig.value if hasattr(sig, "value") else str(sig)
            # Preserve BLOCKED as a separate decision category; do not
            # collapse it into WAIT. app.py sets decision_raw when the
            # original decision was BLOCKED.
            sig_value = getattr(result, "decision_raw", sig_value) or sig_value
            if sig_value in ("BUY", "SELL"):
                self.total_confirmed += 1
                self._create_shadow_trade(result, market_snapshot, sig_value)
            elif sig_value == "WAIT":
                self.total_wait += 1
            else:
                self.total_blocked += 1

            self._persist_decision(result, market_snapshot, sig_value)

    def _create_shadow_trade(self, result: SignalResult,
                             market_snapshot: Dict[str, Any],
                             sig_value: str) -> None:
        """Create a ShadowTrade from a confirmed SignalResult."""
        trade = ShadowTrade(
            signal_id=result.signal_id or "",
            direction=sig_value,
            setup_tf=result.setup_timeframe or "",
            setup_ts_utc=result.setup_candle_timestamp.isoformat() if result.setup_candle_timestamp else "",
            entry=float(result.proposed_entry) if result.proposed_entry is not None else 0.0,
            stop_loss=float(result.stop_loss) if result.stop_loss is not None else 0.0,
            take_profit=float(result.take_profit) if result.take_profit is not None else 0.0,
            invalidation_price=float(result.invalidation_price) if result.invalidation_price is not None else 0.0,
            risk_distance=result.risk_distance,
            reward_distance=result.reward_distance,
            risk_reward_ratio=result.risk_reward_ratio,
            trigger_ts_utc=None,
            created_ts_utc=result.generated_at_utc.isoformat(),
            expires_ts_utc=result.expires_at_utc.isoformat() if result.expires_at_utc else None,
            swept_level=0.0,
            ob_top=0.0,
            ob_bottom=0.0,
            structure_ref=0.0,
            reason_codes=list(result.reason_codes or []),
            human_readable=result.human_readable_explanation or "",
            state="OPEN",
        )
        self._open_trades[trade.signal_id] = trade
        # Persist OPEN trade immediately so it survives application restart.
        self._persist_shadow_trade(trade)

    # -- price tracking ------------------------------------------------------

    def track_price(self, last_price: float, ts_utc: datetime) -> List[ShadowTrade]:
        """Check all open shadow trades against a live price update.

        Expiry is evaluated before price-trigger logic: an already-expired
        trade is valued at the current fresh price and marked EXPIRED rather
        than being classified as TARGET_HIT or STOP_HIT.

        Returns the list of trades that just reached a terminal state.
        """
        just_completed: List[ShadowTrade] = []
        with self._lock:
            # First, process expirations - an expired trade must not be
            # re-classified as TARGET_HIT or STOP_HIT by a later price.
            for sid in list(self._open_trades.keys()):
                trade = self._open_trades[sid]
                if trade.state != "OPEN":
                    continue
                if trade.expires_ts_utc:
                    exp = datetime.fromisoformat(trade.expires_ts_utc)
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    if ts_utc >= exp:
                        trade.state = "EXPIRED"
                        trade.exit_price = last_price
                        trade.exit_ts_utc = ts_utc.isoformat()
                        trade.exit_reason = "expired"
                        self._compute_pnl(trade)
                        just_completed.append(trade)
                        self._completed_trades.append(trade)
                        del self._open_trades[sid]
            # Then check remaining open trades against target/stop/invalidation
            for sid in list(self._open_trades.keys()):
                trade = self._open_trades[sid]
                if trade.state != "OPEN":
                    continue
                terminal = self._check_terminal(trade, last_price, ts_utc)
                if terminal:
                    trade.state = terminal[0]
                    trade.exit_price = terminal[1]
                    trade.exit_ts_utc = ts_utc.isoformat()
                    trade.exit_reason = terminal[2]
                    self._compute_pnl(trade)
                    just_completed.append(trade)
                    self._completed_trades.append(trade)
                    del self._open_trades[sid]
        return just_completed

    def track_expiry(self, ts_utc: datetime,
                     last_price: Optional[float] = None) -> List[ShadowTrade]:
        """Expire any open shadow trades whose expires_ts has passed.

        If ``last_price`` is provided, P&L is computed against that price so
        expired trades receive a consistent, documented exit valuation rather
        than an automatic zero result. If no price is available, the trade is
        marked EXPIRED with no P&L (pnl_r remains None).
        """
        just_completed: List[ShadowTrade] = []
        with self._lock:
            for sid in list(self._open_trades.keys()):
                trade = self._open_trades[sid]
                if trade.state != "OPEN":
                    continue
                if trade.expires_ts_utc:
                    exp = datetime.fromisoformat(trade.expires_ts_utc)
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    if ts_utc >= exp:
                        trade.state = "EXPIRED"
                        trade.exit_ts_utc = ts_utc.isoformat()
                        trade.exit_reason = "expired"
                        if last_price is not None:
                            trade.exit_price = float(last_price)
                            self._compute_pnl(trade)
                        else:
                            trade.exit_price = None
                            trade.pnl_contracts = None
                            trade.pnl_r = None
                        just_completed.append(trade)
                        self._completed_trades.append(trade)
                        del self._open_trades[sid]
        return just_completed

    def _check_terminal(self, trade: ShadowTrade, price: float,
                        ts_utc: datetime) -> Optional[tuple]:
        """Check if a shadow trade has hit target, stop, or invalidation."""
        # For a BUY: target is above entry, stop/invalidation below
        # For a SELL: target is below entry, stop/invalidation above
        # Invalidation is checked before stop: a structural breach takes
        # precedence over a risk exit when both levels are touched at once.
        if trade.direction == "BUY":
            if price >= trade.take_profit:
                return ("TARGET_HIT", trade.take_profit, "target_hit")
            if price <= trade.invalidation_price:
                return ("INVALIDATED", trade.invalidation_price, "invalidated")
            if price <= trade.stop_loss:
                return ("STOP_HIT", trade.stop_loss, "stop_hit")
        elif trade.direction == "SELL":
            if price <= trade.take_profit:
                return ("TARGET_HIT", trade.take_profit, "target_hit")
            if price >= trade.invalidation_price:
                return ("INVALIDATED", trade.invalidation_price, "invalidated")
            if price >= trade.stop_loss:
                return ("STOP_HIT", trade.stop_loss, "stop_hit")
        return None

    def _compute_pnl(self, trade: ShadowTrade) -> None:
        """Compute P&L in contracts and R units for a completed trade."""
        if trade.exit_price is None:
            trade.pnl_contracts = 0.0
            trade.pnl_r = 0.0
            return
        if trade.direction == "BUY":
            raw_pnl = trade.exit_price - trade.entry
        else:
            raw_pnl = trade.entry - trade.exit_price
        # 1 contract = 0.001 BTC per contract; P&L in contracts = raw price diff
        # R-normalised P&L uses risk_distance (entry to stop distance)
        if trade.risk_distance and trade.risk_distance > 0:
            trade.pnl_r = raw_pnl / trade.risk_distance
        else:
            trade.pnl_r = None
        trade.pnl_contracts = raw_pnl  # price difference per contract

    # -- persistence ---------------------------------------------------------

    def _persist_decision(self, result: SignalResult,
                          market_snapshot: Dict[str, Any],
                          sig_value: str) -> None:
        """Persist the full decision record with market conditions."""
        record = {
            "ts_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "signal_id": result.signal_id or "",
            "signal": sig_value,
            "state": result.state.name,
            "decision": sig_value,
            "score": None,
            "confidence": 1.0,
            "blocking_reason": None,
            "feed_fresh": market_snapshot.get("feed_fresh", False),
            "book_synced": market_snapshot.get("book_synced", False),
            "no_unresolved_sequence_gap": market_snapshot.get("no_unresolved_sequence_gap", False),
            "crc_valid": market_snapshot.get("crc_valid", False),
            "orderbook_integrity_ok": market_snapshot.get("orderbook_integrity_ok", False),
            "feed_age_seconds": market_snapshot.get("feed_age_seconds"),
            "total_disconnects": market_snapshot.get("total_disconnects", 0),
            "total_reconnects": market_snapshot.get("total_reconnects", 0),
            "channel_last_seen": market_snapshot.get("channel_last_seen", {}),
            "engine_result": {
                "signal": sig_value,
                "state": result.state.name,
                "signal_id": result.signal_id or "",
                "generated_at_utc": result.generated_at_utc.isoformat(),
                "expires_at_utc": result.expires_at_utc.isoformat() if result.expires_at_utc else None,
                "proposed_entry": result.proposed_entry,
                "stop_loss": result.stop_loss,
                "take_profit": result.take_profit,
                "invalidation_price": result.invalidation_price,
                "risk_distance": result.risk_distance,
                "reward_distance": result.reward_distance,
                "risk_reward_ratio": result.risk_reward_ratio,
                "setup_timeframe": result.setup_timeframe,
                "setup_candle_timestamp": result.setup_candle_timestamp.isoformat() if result.setup_candle_timestamp else None,
                "confirmation_conditions_passed": list(result.confirmation_conditions_passed or []),
                "confirmation_conditions_failed": list(result.confirmation_conditions_failed or []),
                "reason_codes": list(result.reason_codes or []),
                "human_readable_explanation": result.human_readable_explanation,
                "input_candle_timestamps": {k: [t.isoformat() for t in v] for k, v in (result.input_candle_timestamps or {}).items()},
                "data_quality_gate_results": dict(result.data_quality_gate_results or {}),
                "duplicate_suppressed": result.duplicate_suppressed,
                "superseded_signal_id": result.superseded_signal_id,
                "lifecycle_transitions": list(result.lifecycle_transitions or []),
            },
            "market_snapshot": {
                "last_price": market_snapshot.get("last_price"),
                "bid": market_snapshot.get("bid"),
                "ask": market_snapshot.get("ask"),
                "spread_bps": market_snapshot.get("spread_bps"),
                "candle_count_1m": len(market_snapshot.get("candles_1m", [])),
                "candle_count_5m": len(market_snapshot.get("candles_5m", [])),
                "candle_count_15m": len(market_snapshot.get("candles_15m", [])),
                "recent_trade_count": len(market_snapshot.get("recent_trades", [])),
                "atr_5m": market_snapshot.get("atr_5m"),
                "atr_15m": market_snapshot.get("atr_15m"),
            },
            "shadow_trade_count": len(self._completed_trades) + len(self._open_trades),
            "completed_holdout_count": self.completed_holdout_count,
        }
        self._all_signals.append(record)
        self._append_jsonl(self._shadow_decision_path, record)

    def _persist_shadow_trade(self, trade: ShadowTrade) -> None:
        """Append a completed shadow trade to the shadow trade log."""
        record = asdict(trade)
        record["logged_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self._append_jsonl(self._shadow_log_path, record)

    @staticmethod
    def _append_jsonl(path: str, record: Dict[str, Any]) -> None:
        try:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            line = json.dumps(record, allow_nan=False, separators=(",", ":"), default=str)
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            print(f"shadow-test ERROR: failed to persist {path!r}: {type(exc).__name__}: {exc}")

    # -- queries -------------------------------------------------------------

    @property
    def completed_holdout_count(self) -> int:
        """Number of shadow trades that reached a terminal state AND have an
        economically measurable P&L (pnl_r is not None).

        Expired trades without a market price at expiry are excluded: they
        cannot contribute to profitability statistics or the 100-trade
        holdout threshold.
        """
        return sum(1 for t in self._completed_trades if t.pnl_r is not None)

    @property
    def open_trade_count(self) -> int:
        """Number of shadow trades currently open."""
        return len(self._open_trades)

    @property
    def total_signal_count(self) -> int:
        return self.total_signals_evaluated

    def get_completed_trades(self) -> List[ShadowTrade]:
        return list(self._completed_trades)

    def get_open_trades(self) -> List[ShadowTrade]:
        return list(self._open_trades.values())

    def get_summary(self) -> Dict[str, Any]:
        completed = [t for t in self._completed_trades if t.pnl_r is not None]
        wins = sum(1 for t in completed if t.pnl_r > 0)
        losses = sum(1 for t in completed if t.pnl_r < 0)
        breakeven = sum(1 for t in completed if t.pnl_r == 0)
        total_r = sum(t.pnl_r for t in completed)
        win_rate = (wins / len(completed) * 100.0) if completed else 0.0
        return {
            "total_signals_evaluated": self.total_signals_evaluated,
            "total_confirmed": self.total_confirmed,
            "total_wait": self.total_wait,
            "total_blocked": self.total_blocked,
            "open_shadow_trades": self.open_trade_count,
            "completed_holdout_trades": self.completed_holdout_count,
            "completed_toward_threshold": self.completed_holdout_count,
            "threshold_required": 100,
            "threshold_met": self.completed_holdout_count >= 100,
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "win_rate_percent": round(win_rate, 2),
            "total_r": round(total_r, 4),
        }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_shadow_engine: Optional[ShadowTestEngine] = None
_shadow_lock = threading.Lock()


def get_shadow_engine(config: Optional[Dict[str, Any]] = None) -> ShadowTestEngine:
    """Return the module-level ShadowTestEngine singleton."""
    global _shadow_engine
    with _shadow_lock:
        if _shadow_engine is None:
            _shadow_engine = ShadowTestEngine(config)
        return _shadow_engine


def reset_shadow_engine() -> None:
    """Reset the module-level shadow engine (mainly for testing)."""
    global _shadow_engine
    with _shadow_lock:
        _shadow_engine = None
