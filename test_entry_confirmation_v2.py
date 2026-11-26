"""Unit tests for independent V2 confirmation logic."""
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from entry_confirmation_v2 import evaluate_entry_confirmation, _liquidity_behavior

UTC = timezone.utc


def _rising(now, minutes, n=80):
    return [SimpleNamespace(ts_utc=now-timedelta(minutes=minutes*(n-i)),
                             open=100+i*.05, high=100.1+i*.05,
                             low=99.9+i*.05, close=100+i*.05, volume=10)
            for i in range(n)]


def _objects(now):
    snapshot=SimpleNamespace(ts_utc=now, candles_1h=_rising(now,60), candles_15m=_rising(now,15),
        recent_trades=[], queue_imbalance=None, order_flow_imbalance_ratio=None,
        microprice_edge_bps=None, spread_bps=2.0, open_interest_contracts=100.0,
        funding_rate=.0001, mark_index_basis_bps=1.0, candles_5m=[], atr_5m=None)
    result=SimpleNamespace(signal=SimpleNamespace(value="BUY"),
        data_quality_gate_results={"connection_healthy":True,"book_synced":True},
        confirmation_conditions_passed=["5m sweep","5m structure break","order block","1m trigger"],
        proposed_entry=100.0, stop_loss=99.0, take_profit=103.0,
        risk_distance=1.0, reward_distance=3.0, risk_reward_ratio=3.0,
        structure_break_level=101.0, setup_candle_timestamp=now-timedelta(minutes=5))
    return snapshot,result


def test_non_signal_does_not_receive_v2_candidate():
    now=datetime(2026,11,1,tzinfo=UTC)
    snapshot,_=_objects(now)
    result=SimpleNamespace(signal=SimpleNamespace(value="WAIT"))
    out=evaluate_entry_confirmation(snapshot,result)
    assert out["state"]=="WAIT_BASELINE_SIGNAL_NOT_CONFIRMED"
    assert out["live_orders_allowed"] is False


def test_missing_cost_and_funding_inputs_fail_closed(monkeypatch):
    for key in ("ENTRY_CONFIRMATION_FEE_BPS_PER_SIDE","ENTRY_CONFIRMATION_SLIPPAGE_BPS_PER_SIDE",
                "ENTRY_CONFIRMATION_MAX_HOLD_MINUTES"):
        monkeypatch.delenv(key,raising=False)
    now=datetime(2026,11,1,tzinfo=UTC)
    snapshot,result=_objects(now)
    out=evaluate_entry_confirmation(snapshot,result,feed_data={},config={"zero_funding_required":True})
    assert out["state"]=="WAIT"
    assert "WAIT_EXECUTION_COSTS_NOT_CONFIGURED" in out["reason_codes"]
    assert "WAIT_FUNDING_SCHEDULE_UNAVAILABLE" in out["reason_codes"]
    assert out["live_orders_allowed"] is False


def test_cost_and_funding_inputs_are_reported_without_enabling_orders():
    now=datetime(2026,11,1,tzinfo=UTC)
    snapshot,result=_objects(now)
    out=evaluate_entry_confirmation(snapshot,result,
        feed_data={"next_funding_at":(now+timedelta(hours=1)).timestamp()},
        config={"fee_bps_per_side":1.0,"slippage_bps_per_side":.5,
                "max_holding_minutes":20,"zero_funding_required":True})
    assert out["execution_costs"]["estimated_round_trip_cost_bps"]==5.0
    assert out["execution_costs"]["net_risk_reward"] is not None
    assert out["funding_guard"]["passed"] is True
    assert out["context_only"]["aggregate_open_interest_contracts"]==100.0
    assert out["live_orders_allowed"] is False



def test_price_level_replenishment_is_reported_only_as_a_candidate():
    now = datetime(2026, 11, 1, tzinfo=UTC)
    events = [
        {"ts": (now-timedelta(seconds=20)).timestamp(), "side": "ask", "price": 100.0,
         "previous_size": 50.0, "new_size": 20.0, "delta": -30.0},
        {"ts": (now-timedelta(seconds=10)).timestamp(), "side": "ask", "price": 100.0,
         "previous_size": 20.0, "new_size": 45.0, "delta": 25.0},
    ]
    trades = [SimpleNamespace(ts_utc=now-timedelta(seconds=15), price=100.0,
                              size=20.0, side_aggressor="BUY_TAKER")]
    result = _liquidity_behavior(events, trades, now)
    assert result["replenishment_candidates"]
    assert result["replenishment_candidates"][0]["side"] == "ASK"
    assert "not proof" in result["limitations"]



def test_ofi_ratio_without_minimum_event_count_and_span_does_not_pass_book_gate():
    now=datetime(2026,11,1,tzinfo=UTC)
    snapshot,result=_objects(now)
    snapshot.queue_imbalance=0.20
    snapshot.order_flow_imbalance_ratio=0.10
    snapshot.order_flow_imbalance_event_count=5
    snapshot.order_flow_imbalance_span_seconds=10.0
    out=evaluate_entry_confirmation(snapshot,result,feed_data={},
        config={"fee_bps_per_side":1.0,"slippage_bps_per_side":0.5,
                "max_holding_minutes":20,"zero_funding_required":False})
    assert out["orderbook"]["ofi_sample_gate_passed"] is False
    assert out["orderbook"]["aligned"] is False
    assert "WAIT_ORDERBOOK_FLOW_NOT_CONFIRMED" in out["reason_codes"]


def test_liquidity_behavior_rejects_invalid_tick_or_window():
    now=datetime(2026,11,1,tzinfo=UTC)
    with pytest.raises(ValueError):
        _liquidity_behavior([],[],now,tick_size=0)
    with pytest.raises(ValueError):
        _liquidity_behavior([],[],now,window_seconds=0)
