#!/usr/bin/env python3
"""Run the frozen chronological replay and write a provenance-bearing JSON report."""
from __future__ import annotations
import argparse, hashlib, json, math
from datetime import datetime, timezone
from pathlib import Path
import oos_replay as oos

def sha256(path: Path) -> str:
    h=hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024*1024),b''): h.update(block)
    return h.hexdigest()

def parse_ts(raw):
    if not raw: return None
    try:
        dt=datetime.fromisoformat(str(raw).replace('Z','+00:00'))
        return dt.astimezone(timezone.utc) if dt.tzinfo else None
    except ValueError: return None

def safe_json(value):
    if isinstance(value,float) and not math.isfinite(value):
        return 'Infinity (zero gross losses)' if value > 0 else 'Non-finite'
    if isinstance(value,dict): return {k:safe_json(v) for k,v in value.items()}
    if isinstance(value,list): return [safe_json(v) for v in value]
    return value

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bars',required=True); p.add_argument('--trades',required=True)
    p.add_argument('--output-dir',default='~/Downloads/delta_oos_data/oos_report')
    p.add_argument('--train-fraction',type=float,default=.60)
    p.add_argument('--validation-fraction',type=float,default=.20)
    a=p.parse_args()
    if not (.4<=a.train_fraction<=.8 and .1<=a.validation_fraction<=.3 and a.train_fraction+a.validation_fraction<1):
        p.error('train fraction must be 0.4-0.8; validation 0.1-0.3; sum below 1')
    bars_path=Path(a.bars).expanduser(); trades_path=Path(a.trades).expanduser()
    bars=oos.load_bars_csv(str(bars_path)); trades=oos.load_trades_csv(str(trades_path))
    if len(bars)<900: raise SystemExit(f'At least 900 one-minute bars required; found {len(bars)}')
    chronology_warnings=oos.validate_chronology(bars)
    n=len(bars)
    train_end=bars[max(0,int(n*a.train_fraction)-1)].ts_utc
    validation_end=bars[max(0,int(n*(a.train_fraction+a.validation_fraction))-1)].ts_utc

    # Check real flow-tape coverage before the computationally expensive replay.
    bar_minute_keys={int(b.ts_utc.timestamp()//60) for b in bars}
    trade_minute_keys={int(t.ts_utc.timestamp()//60) for t in trades}
    covered_minutes=len(bar_minute_keys & trade_minute_keys)
    tape_coverage_ratio=(covered_minutes/len(bar_minute_keys)) if bar_minute_keys else 0.0
    span_days=max(0.0,(bars[-1].ts_utc-bars[0].ts_utc).total_seconds()/86400.0)
    overlapping_trade_count=sum(1 for t in trades if bars[0].ts_utc<=t.ts_utc<=bars[-1].ts_utc)
    tape_first=min((t.ts_utc for t in trades),default=None)
    tape_last=max((t.ts_utc for t in trades),default=None)
    minimum_days=60.0
    minimum_prints=1000
    minimum_coverage=0.80
    data_checks={
        'bar_history_at_least_60_days':span_days>=minimum_days,
        'at_least_1000_real_trade_prints':overlapping_trade_count>=minimum_prints,
        'trade_tape_covers_at_least_80_percent_of_bar_minutes':tape_coverage_ratio>=minimum_coverage,
        'chronology_warnings_absent':len(chronology_warnings)==0,
    }
    basic_data_ok=all(data_checks.values())

    # Do not run a long replay when its required input evidence is already inadequate.
    if basic_data_ok:
        replay=oos.run_replay(bars,trades)
        replay_skipped=False
    else:
        replay={
            'params_version':getattr(oos,'PARAMS_VERSION','unknown'),
            'metrics':{}, 'stats':{'replay_skipped':'data coverage gates failed'},
            'simulated_trades':[],
        }
        replay_skipped=True
    segments={'train':[],'validation':[],'holdout_oos':[]}
    for trade in replay.get('simulated_trades',[]):
        ts=parse_ts(trade.get('decision_ts_utc'))
        if ts is None: continue
        if ts<=train_end: segments['train'].append(trade)
        elif ts<=validation_end: segments['validation'].append(trade)
        else: segments['holdout_oos'].append(trade)

    full_trades=replay.get('simulated_trades',[])
    holdout=segments['holdout_oos']
    if not basic_data_ok:
        status='BLOCKED_INSUFFICIENT_DATA'
    elif not full_trades:
        status='INCONCLUSIVE_NO_COMPLETED_TRADES'
    elif len(holdout)<100:
        status='RESEARCH_ONLY_INSUFFICIENT_HOLDOUT_SAMPLE'
    else:
        status='RESEARCH_ONLY_NOT_CERTIFIED'
    report={
        'report_generated_utc':datetime.now(timezone.utc).isoformat(),
        'status':status,
        'release_decision':'NOT_APPROVED',
        'params_version':replay.get('params_version'),
        'parameters_policy':'Frozen replay defaults only; this script performs no parameter tuning.',
        'replay_executed':not replay_skipped,
        'data':{
            'bars_file':str(bars_path),'bars_sha256':sha256(bars_path),'bars_count':len(bars),
            'trades_file':str(trades_path),'trades_sha256':sha256(trades_path),'trade_print_count':len(trades),
            'first_bar_utc':bars[0].ts_utc.isoformat(),'last_bar_utc':bars[-1].ts_utc.isoformat(),
            'bar_span_days':round(span_days,4),
            'trade_tape_first_utc':tape_first.isoformat() if tape_first else None,
            'trade_tape_last_utc':tape_last.isoformat() if tape_last else None,
            'trade_tape_prints_overlapping_bars':overlapping_trade_count,
            'bars_with_at_least_one_real_trade_print':covered_minutes,
            'trade_tape_coverage_ratio':round(tape_coverage_ratio,6),
            'train_end_utc':train_end.isoformat(),'validation_end_utc':validation_end.isoformat(),
            'chronology_warnings':chronology_warnings,
        },
        'evidence_gates':{
            'minimums':{'bar_history_days':minimum_days,'real_trade_print_count':minimum_prints,
                        'trade_tape_bar_minute_coverage_ratio':minimum_coverage,
                        'holdout_completed_trades_for_review':100},
            'checks':data_checks,'basic_data_checks_passed':basic_data_ok,
            'holdout_completed_trades':len(holdout),
            'status_note':'These are evidence-sufficiency screens, not proof of profitability or release approval.',
        },
        'cost_assumptions':dict(oos.FROZEN_OOS_CONFIG),
        'funding':'Not modelled by the frozen default harness; results exclude funding.',
        'full_period':replay.get('metrics',{}),
        'segments':{name:oos.compute_metrics(records) for name,records in segments.items()},
        'stats':replay.get('stats',{}), 'trades':full_trades,
        'limitations':[
            'A holdout generated after strategy development is retrospective, not prospective validation.',
            'A flow strategy needs real trade prints covering the bar window; candle-only coverage cannot pass this gate.',
            'Partial trade-tape coverage is blocked before replay, not treated as a flat market.',
            'A lack of completed trades is inconclusive, not a pass.',
            'Funding is excluded; validate funding, actual fee tier, spread and slippage for the intended exchange/account.',
            'Passing data gates or positive metrics is not release approval; independently review sample size, uncertainty, execution realism, drawdown and parameter sensitivity.',
        ]}
    out=Path(a.output_dir).expanduser(); out.mkdir(parents=True,exist_ok=True)
    path=out/'oos_report.json'; path.write_text(json.dumps(safe_json(report),indent=2,allow_nan=False)+'\n',encoding='utf-8')
    print(f'Report: {path}')
    print(f"Bars={len(bars):,}; real prints={len(trades):,}; trade-tape minute coverage={tape_coverage_ratio:.1%}; completed trades={len(full_trades):,}; holdout={len(holdout):,}")
    print('Replay executed:',not replay_skipped)
    print('Data gates:',json.dumps(data_checks,sort_keys=True))
    print('Holdout OOS:',json.dumps(safe_json(report['segments']['holdout_oos']),sort_keys=True))
    print('Status:',report['status'],'| Release decision:',report['release_decision'])
    return 0

if __name__=='__main__': raise SystemExit(main())
