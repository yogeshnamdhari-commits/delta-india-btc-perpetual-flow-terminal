#!/usr/bin/env python3
"""Export logged, real trade events to the OOS replay CSV schema."""
from __future__ import annotations
import argparse,csv,hashlib,json
from datetime import datetime,timezone
from pathlib import Path

def epoch_seconds(value):
    n=float(value)
    if n>10**17: return n/10**9
    if n>10**14: return n/10**6
    if n>10**11: return n/10**3
    return n

def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--input",required=True); p.add_argument("--output",default="~/Downloads/delta_oos_data/trades.csv"); a=p.parse_args()
    source=Path(a.input).expanduser()
    if not source.is_file(): p.error(f"input file not found: {source}")
    rows=[]; unknown=bad=0
    with source.open(encoding="utf-8") as fh:
        for line in fh:
            try:
                e=json.loads(line)
                if e.get("event")!="trade": continue
                raw=e.get("ts",e.get("exchange_ts"))
                if raw is None: bad+=1; continue
                ts=datetime.fromtimestamp(epoch_seconds(raw),timezone.utc)
                price=float(e["price"]); size=float(e["size"]); role=str(e.get("buyer_role","")).lower()
                side="BUY_TAKER" if role=="t" else "SELL_TAKER" if role=="m" else None
                if side is None: unknown+=1; continue
                if price<=0 or size<=0: bad+=1; continue
                rows.append((ts,price,size,side))
            except (json.JSONDecodeError,KeyError,TypeError,ValueError,OverflowError): bad+=1
    rows.sort(key=lambda x:x[0]); target=Path(a.output).expanduser(); target.parent.mkdir(parents=True,exist_ok=True)
    with target.open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["ts_utc","price","size","side_aggressor"])
        for ts,price,size,side in rows: w.writerow([ts.isoformat(),price,size,side])
    print(f"trades={len(rows):,}; unknown_side={unknown:,}; malformed={bad:,}; output={target}")
    print(f"range={rows[0][0].isoformat() if rows else 'n/a'} .. {rows[-1][0].isoformat() if rows else 'n/a'}")
    print(f"sha256={hashlib.sha256(target.read_bytes()).hexdigest()}")
    if not rows:
        print("WARNING: No real trade events found. No synthetic trades were written; the OOS report should block on insufficient evidence.")
    return 0
if __name__=="__main__": raise SystemExit(main())
