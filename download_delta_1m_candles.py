#!/usr/bin/env python3
"""Download public Delta India BTCUSD 1m candles to oos_replay.py's CSV schema."""
from __future__ import annotations
import argparse, csv, hashlib, time
from datetime import datetime, timezone
from pathlib import Path
import requests
URL="https://api.india.delta.exchange/v2/history/candles"

def epoch_seconds(value):
    n=int(value)
    if n>10**17: return n//10**9
    if n>10**14: return n//10**6
    if n>10**11: return n//10**3
    return n

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--days",type=float,default=90)
    p.add_argument("--output",default="~/Downloads/delta_oos_data/bars_1m.csv")
    p.add_argument("--symbol",default="BTCUSD")
    p.add_argument("--sleep",type=float,default=.2)
    a=p.parse_args()
    if not (0<a.days<=3650) or a.sleep<0: p.error("--days must be in (0,3650] and --sleep >= 0")
    end=int(time.time())//60*60; start=end-int(a.days*86400)
    records={}; cursor=start; calls=0
    with requests.Session() as session:
        session.headers.update({"Accept":"application/json","User-Agent":"DeltaBTCFlowOOSResearch/1.0"})
        while cursor<end:
            chunk_end=min(end,cursor+2000*60)
            response=session.get(URL,params={"resolution":"1m","symbol":a.symbol,"start":cursor,"end":chunk_end},timeout=20)
            response.raise_for_status(); payload=response.json()
            if not isinstance(payload,dict) or not isinstance(payload.get("result"),list):
                raise RuntimeError(f"Unexpected candle API response for chunk {cursor}")
            for row in payload["result"]:
                try:
                    ts=(epoch_seconds(row["time"])//60)*60
                    o,h,l,c=(float(row[k]) for k in ("open","high","low","close")); v=float(row.get("volume") or 0)
                    if ts<start or ts>=end or min(o,h,l,c)<=0 or h<max(o,c) or l>min(o,c) or v<0: continue
                    records[ts]=(o,h,l,c,v)
                except (KeyError,TypeError,ValueError,OverflowError): continue
            calls+=1; cursor=chunk_end
            print(f"Chunk {calls}: through {datetime.fromtimestamp(cursor,timezone.utc).isoformat()}; bars={len(records):,}")
            if a.sleep: time.sleep(a.sleep)
    path=Path(a.output).expanduser(); path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["ts_utc","open","high","low","close","volume"])
        for ts,row in sorted(records.items()): w.writerow([datetime.fromtimestamp(ts,timezone.utc).isoformat(),*row])
    keys=sorted(records); gaps=sum(1 for x,y in zip(keys,keys[1:]) if y-x>60)
    print(f"Output: {path}; records={len(keys):,}; gap_intervals={gaps:,}; sha256={hashlib.sha256(path.read_bytes()).hexdigest()}")
    if not records: raise SystemExit("No candles returned; do not run replay on empty data")
    return 0
if __name__=="__main__": raise SystemExit(main())
