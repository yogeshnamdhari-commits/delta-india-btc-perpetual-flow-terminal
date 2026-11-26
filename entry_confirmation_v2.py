"""Independent V2 confirmation observer. Read-only and shadow-only."""
from __future__ import annotations
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any

UTC = timezone.utc


def _env_float(name: str, default: float | None = None) -> float | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    return default if raw is None else raw.strip().lower() in ("1", "true", "yes", "on")


def _ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return []
    alpha = 2.0 / (period + 1.0)
    out = [sum(values[:period]) / period]
    for value in values[period:]:
        out.append(alpha * value + (1.0 - alpha) * out[-1])
    return out


def _trend(candles: list[Any], direction: str, timeframe: str) -> dict[str, Any]:
    if len(candles) < 55:
        return {"timeframe": timeframe, "state": "UNAVAILABLE", "aligned": False, "bars": len(candles)}
    closes = [float(c.close) for c in candles]
    fast, slow = _ema(closes, 20), _ema(closes, 50)
    if not fast or not slow:
        return {"timeframe": timeframe, "state": "UNAVAILABLE", "aligned": False, "bars": len(candles)}
    slope_ref = fast[-6] if len(fast) >= 6 else fast[0]
    slope_bps = ((fast[-1] - slope_ref) / slope_ref * 10000.0) if slope_ref else 0.0
    state = ("BULLISH" if fast[-1] > slow[-1] and closes[-1] > slow[-1] and slope_bps > 0
             else "BEARISH" if fast[-1] < slow[-1] and closes[-1] < slow[-1] and slope_bps < 0
             else "NEUTRAL")
    aligned = state == ("BULLISH" if direction == "BUY" else "BEARISH")
    return {"timeframe": timeframe, "state": state, "aligned": aligned, "bars": len(candles),
            "ema20": fast[-1], "ema50": slow[-1], "ema20_slope_bps": slope_bps}


def _flow(trades: list[Any], direction: str) -> dict[str, Any]:
    if not trades:
        return {"status": "UNAVAILABLE", "aligned": False, "trade_count": 0}
    ordered = sorted(trades, key=lambda t: t.ts_utc)
    end = ordered[-1].ts_utc
    rows = [t for t in ordered if end - timedelta(minutes=5) <= t.ts_utc <= end]
    buys = [t for t in rows if t.side_aggressor == "BUY_TAKER"]
    sells = [t for t in rows if t.side_aggressor == "SELL_TAKER"]
    buy_qty, sell_qty = sum(float(t.size) for t in buys), sum(float(t.size) for t in sells)
    total, count = buy_qty + sell_qty, len(rows)
    span = (rows[-1].ts_utc - rows[0].ts_utc).total_seconds() if count >= 2 else 0.0

    def vwap(group):
        qty = sum(float(t.size) for t in group)
        return sum(float(t.price) * float(t.size) for t in group) / qty if qty else 0.0

    third = max(1, count // 3)
    early, late = vwap(rows[:third]), vwap(rows[-third:])
    response = ((late / early) - 1.0) * 10000.0 if early else 0.0
    buy_share, sell_share = (buy_qty / total, sell_qty / total) if total else (0.0, 0.0)
    share = buy_share if direction == "BUY" else sell_share
    response_ok = response > 0.5 if direction == "BUY" else response < -0.5
    enough = (count >= int(_env_float("ENTRY_CONFIRMATION_MIN_FLOW_TRADES", 20))
              and span >= float(_env_float("ENTRY_CONFIRMATION_MIN_FLOW_SPAN_SECONDS", 60)))
    dominance = share >= float(_env_float("ENTRY_CONFIRMATION_MIN_FLOW_DOMINANCE", 0.60))
    return {"status": "ALIGNED" if enough and dominance and response_ok else "INSUFFICIENT_OR_CONFLICTING",
            "aligned": bool(enough and dominance and response_ok), "trade_count": count,
            "span_seconds": span, "buy_contracts": buy_qty, "sell_contracts": sell_qty,
            "buy_share": buy_share, "sell_share": sell_share, "directional_share": share,
            "price_response_bps": response,
            "capture_limit": "Process-captured; Delta public trades have no per-print sequence or historical backfill."}


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, (int, float)):
            epoch = float(value)
            if not math.isfinite(epoch) or epoch <= 0:
                return None
            if epoch >= 1e14:
                epoch /= 1e6
            elif epoch >= 1e11:
                epoch /= 1e3
            parsed = datetime.fromtimestamp(epoch, UTC)
        else:
            raw = str(value).strip()
            parsed = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
        return parsed.astimezone(UTC) if parsed.tzinfo is not None else None
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _retest(snapshot: Any, result: Any, direction: str) -> dict[str, Any]:
    level, setup_ts = getattr(result, "structure_break_level", None), getattr(result, "setup_candle_timestamp", None)
    if level is None or setup_ts is None:
        return {"state": "UNAVAILABLE", "observed": False}
    atr = float(getattr(snapshot, "atr_5m", 0.0) or 0.0)
    for candle in getattr(snapshot, "candles_5m", [])[-6:]:
        if candle.ts_utc <= setup_ts:
            continue
        touched = (candle.low <= level + 0.10 * atr) if direction == "BUY" else (
            candle.high >= level - 0.10 * atr)
        held = (candle.close > level and candle.low >= level - 0.25 * atr) if direction == "BUY" else (
            candle.close < level and candle.high <= level + 0.25 * atr)
        if touched and held:
            return {"state": "RETEST_HOLD_OBSERVED", "observed": True,
                    "bar_timestamp_utc": candle.ts_utc.isoformat(), "level": level}
    return {"state": "NOT_OBSERVED_OPTIONAL", "observed": False, "level": level}



def _liquidity_behavior(events: list[Any], trades: list[Any], now: datetime,
                        tick_size: float = 0.5, window_seconds: float = 30.0) -> dict[str, Any]:
    """Flag same-price depth replenishment/withdrawal candidates, not hidden orders."""
    if not math.isfinite(tick_size) or tick_size <= 0:
        raise ValueError("tick_size must be positive and finite")
    if not math.isfinite(window_seconds) or window_seconds <= 0:
        raise ValueError("window_seconds must be positive and finite")
    cutoff = now - timedelta(seconds=window_seconds)
    grouped: dict[tuple[str, float], list[dict[str, Any]]] = {}
    for event in events or []:
        try:
            ts = _parse_ts(event.get("ts"))
            side = str(event.get("side", "")).lower()
            price = float(event.get("price"))
            delta = float(event.get("delta"))
            if ts is None or ts < cutoff or ts > now or side not in ("bid", "ask"):
                continue
            if not all(math.isfinite(x) for x in (price, delta)) or price <= 0:
                continue
            bucket_price = round(price / tick_size) * tick_size
            grouped.setdefault((side, bucket_price), []).append(event)
        except (AttributeError, TypeError, ValueError, OverflowError):
            continue
    replenishments, withdrawals = [], []
    for (side, price), rows in grouped.items():
        rows.sort(key=lambda row: _parse_ts(row.get("ts")) or cutoff)
        matched_side = "BUY_TAKER" if side == "ask" else "SELL_TAKER"
        matched = sum(float(t.size) for t in trades
                      if getattr(t, "side_aggressor", None) == matched_side
                      and abs(float(t.price) - price) <= tick_size / 2.0
                      and cutoff <= t.ts_utc <= now)
        additions = sum(max(0.0, float(row.get("delta", 0.0))) for row in rows)
        reductions = sum(max(0.0, -float(row.get("delta", 0.0))) for row in rows)
        final_size = float(rows[-1].get("new_size", 0.0) or 0.0)
        baseline_size = float(rows[0].get("previous_size", 0.0) or 0.0)
        if matched >= 5.0 and additions >= max(2.5, matched * 0.25):
            replenishments.append({
                "side": side.upper(), "price": price,
                "matched_taker_contracts": matched, "visible_additions_contracts": additions,
                "visible_reductions_contracts": reductions, "final_displayed_contracts": final_size,
                "classification": "SAME_PRICE_REPLENISHMENT_CANDIDATE",
            })
        elif reductions >= 10.0 and matched < reductions * 0.5:
            withdrawals.append({
                "side": side.upper(), "price": price,
                "visible_reductions_contracts": reductions, "matched_taker_contracts": matched,
                "baseline_displayed_contracts": baseline_size, "final_displayed_contracts": final_size,
                "classification": "UNEXPLAINED_VISIBLE_DEPTH_WITHDRAWAL_CANDIDATE",
            })
    return {
        "window_seconds": window_seconds,
        "replenishment_candidates": replenishments,
        "withdrawal_candidates": withdrawals,
        "limitations": "Aggregated L2 by price has no order IDs or participant identity; candidates are not proof of iceberg orders, spoofing, intent or absorption.",
    }

def evaluate_entry_confirmation(snapshot: Any, result: Any, *, feed_data: dict[str, Any] | None = None,
                                config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg, feed = dict(config or {}), feed_data or {}
    signal = getattr(getattr(result, "signal", None), "value", str(getattr(result, "signal", "WAIT")))
    if signal not in ("BUY", "SELL"):
        return {"version": "entry-confirmation-v2-shadow", "state": "WAIT_BASELINE_SIGNAL_NOT_CONFIRMED",
                "reason_codes": ["BASELINE_SIGNAL_NOT_CONFIRMED"], "shadow_only": True,
                "live_orders_allowed": False}

    reasons, passed = [], []
    gates = dict(getattr(result, "data_quality_gate_results", {}) or {})
    quality_ok = bool(gates) and all(value is True for value in gates.values())
    if quality_ok: passed.append("baseline feed integrity and checksum gates")
    else: reasons.append("WAIT_BASELINE_DATA_QUALITY")

    trend1h, trend15m = _trend(list(getattr(snapshot, "candles_1h", [])), signal, "1h"), _trend(
        list(getattr(snapshot, "candles_15m", [])), signal, "15m")
    if trend1h["aligned"]: passed.append("1h EMA20/EMA50 and slope aligned")
    else: reasons.append("WAIT_1H_CONTEXT_NOT_ALIGNED")
    if trend15m["aligned"]: passed.append("15m EMA20/EMA50 and slope aligned")
    else: reasons.append("WAIT_15M_CONTEXT_NOT_ALIGNED")

    structure = list(getattr(result, "confirmation_conditions_passed", []) or [])
    structural_ok = (any("sweep" in str(x).lower() for x in structure)
                     and any("structure break" in str(x).lower() for x in structure)
                     and any("order block" in str(x).lower() for x in structure)
                     and any("1m trigger" in str(x).lower() for x in structure))
    if structural_ok: passed.append("5m sweep + structure break + order block + 1m trigger")
    else: reasons.append("WAIT_STRUCTURE_CONFIRMATION_INCOMPLETE")

    flow = _flow(list(getattr(snapshot, "recent_trades", [])), signal)
    if flow["aligned"]: passed.append("5m taker-flow dominance and price response aligned")
    else: reasons.append("WAIT_TRADE_FLOW_NOT_CONFIRMED")

    queue, ofi = getattr(snapshot, "queue_imbalance", None), getattr(snapshot, "order_flow_imbalance_ratio", None)
    qmin, omin = float(cfg.get("min_queue_imbalance", 0.05)), float(cfg.get("min_ofi_ratio", 0.02))
    ofi_count = int(getattr(snapshot, "order_flow_imbalance_event_count", 0) or 0)
    ofi_span = float(getattr(snapshot, "order_flow_imbalance_span_seconds", 0.0) or 0.0)
    ofi_sample_ok = (ofi_count >= int(cfg.get("min_ofi_events", 20))
                     and ofi_span >= float(cfg.get("min_ofi_span_seconds", 60.0)))
    book_ok = (queue is not None and ofi is not None and ofi_sample_ok and
               ((queue >= qmin and ofi >= omin) if signal == "BUY" else (queue <= -qmin and ofi <= -omin)))
    if book_ok: passed.append("L1 queue imbalance and event-level OFI aligned")
    else: reasons.append("WAIT_ORDERBOOK_FLOW_NOT_CONFIRMED")

    retest = _retest(snapshot, result, signal)
    if retest["observed"]: passed.append("optional structure retest/hold observed")
    liquidity_behavior = _liquidity_behavior(
        list(feed.get("depth_level_events", [])),
        list(getattr(snapshot, "recent_trades", [])),
        snapshot.ts_utc,
        tick_size=float(cfg.get("tick_size", 0.5)),
        window_seconds=float(cfg.get("liquidity_event_window_seconds", 30.0)),
    )

    fee = cfg.get("fee_bps_per_side", _env_float("ENTRY_CONFIRMATION_FEE_BPS_PER_SIDE"))
    slip = cfg.get("slippage_bps_per_side", _env_float("ENTRY_CONFIRMATION_SLIPPAGE_BPS_PER_SIDE"))
    spread = float(getattr(snapshot, "spread_bps", 0.0))
    entry, risk, reward = (getattr(result, "proposed_entry", None),
                           getattr(result, "risk_distance", None), getattr(result, "reward_distance", None))
    net_rr, cost_bps = None, None
    if fee is None or slip is None or fee < 0 or slip < 0:
        reasons.append("WAIT_EXECUTION_COSTS_NOT_CONFIGURED")
    elif entry and risk and reward and entry > 0 and risk > 0:
        cost_bps = spread + 2.0 * fee + 2.0 * slip
        cost_price = entry * cost_bps / 10000.0
        net_reward, net_risk = reward - cost_price, risk + cost_price
        net_rr = net_reward / net_risk if net_risk > 0 else None
        if net_rr is not None and net_rr >= float(cfg.get("min_net_rr", 1.30)):
            passed.append("net RR after spread, fees and slippage meets minimum")
        else: reasons.append("WAIT_NET_RR_BELOW_MINIMUM")
    else:
        reasons.append("WAIT_RISK_GEOMETRY_UNAVAILABLE")

    zero_funding = bool(cfg.get("zero_funding_required",
                               _env_bool("ENTRY_CONFIRMATION_ZERO_FUNDING_REQUIRED", True)))
    funding_ok, funding_detail = True, "FUNDING_NOT_REQUIRED_BY_CONFIGURATION"
    if zero_funding:
        next_funding = _parse_ts(feed.get("next_funding_at"))
        max_hold = cfg.get("max_holding_minutes", _env_float("ENTRY_CONFIRMATION_MAX_HOLD_MINUTES"))
        buffer = float(cfg.get("funding_buffer_minutes", 2.0))
        if next_funding is None:
            funding_ok, funding_detail = False, "NEXT_FUNDING_TIME_UNAVAILABLE"
            reasons.append("WAIT_FUNDING_SCHEDULE_UNAVAILABLE")
        elif max_hold is None or float(max_hold) <= 0:
            funding_ok, funding_detail = False, "MAX_HOLDING_TIME_NOT_CONFIGURED"
            reasons.append("WAIT_MAX_HOLDING_TIME_NOT_CONFIGURED")
        else:
            latest_exit = snapshot.ts_utc + timedelta(minutes=float(max_hold) + buffer)
            funding_ok = latest_exit < next_funding
            funding_detail = "EXIT_WINDOW_BEFORE_FUNDING" if funding_ok else "HOLD_WINDOW_MAY_CROSS_FUNDING"
            if funding_ok: passed.append("maximum holding window exits before funding")
            else: reasons.append("WAIT_FUNDING_WINDOW_NOT_CLEAR")

    ready = (quality_ok and trend1h["aligned"] and trend15m["aligned"] and structural_ok
             and flow["aligned"] and book_ok and net_rr is not None
             and net_rr >= float(cfg.get("min_net_rr", 1.30)) and funding_ok)
    return {
        "version": "entry-confirmation-v2-shadow",
        "state": "PAPER_REVIEW_CANDIDATE" if ready else "WAIT",
        "reason_codes": reasons, "passed_conditions": passed,
        "timeframe_context": {"1h": trend1h, "15m": trend15m}, "trade_flow": flow,
        "orderbook": {"queue_imbalance": queue,
                      "microprice_edge_bps": getattr(snapshot, "microprice_edge_bps", None),
                      "event_ofi_ratio": ofi, "ofi_event_count": ofi_count,
                      "ofi_observed_span_seconds": ofi_span,
                      "ofi_sample_gate_passed": ofi_sample_ok, "aligned": bool(book_ok)},
        "liquidity_behavior": liquidity_behavior,
        "retest": retest,
        "execution_costs": {"spread_bps": spread, "fee_bps_per_side": fee,
                            "slippage_bps_per_side": slip,
                            "estimated_round_trip_cost_bps": cost_bps,
                            "net_risk_reward": net_rr,
                            "inputs_must_match_actual_account_and_contract": True},
        "funding_guard": {"required": zero_funding, "passed": funding_ok, "detail": funding_detail},
        "context_only": {"aggregate_open_interest_contracts": getattr(snapshot, "open_interest_contracts", None),
                         "funding_rate": getattr(snapshot, "funding_rate", None),
                         "mark_index_basis_bps": getattr(snapshot, "mark_index_basis_bps", None),
                         "note": "Aggregate OI, funding and basis are context, not standalone directional votes."},
        "shadow_only": True, "live_orders_allowed": False,
        "warning": "Paper-review candidate only; not profitability evidence or permission to trade.",
    }
