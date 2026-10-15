"""Fetch negative-factor event tables from Tushare (read-only) into OUT dir as parquet.
Run on server: python fetch_events.py OUT START END"""
import sys, time, os, pandas as pd
from dotenv import dotenv_values
import tushare as ts
e = dotenv_values("/home/lighthouse/rquant/.env")
pro = ts.pro_api(e.get("TUSHARE_TOKEN") or [v for k, v in e.items() if "TUSHARE" in k][0])
OUT, S, E = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(OUT, exist_ok=True)
days = sorted(pro.trade_cal(exchange="SSE", start_date=S, end_date=E, is_open="1").cal_date)
def call(fn, **kw):
    for i in range(4):
        try:
            return getattr(pro, fn)(**kw)
        except Exception as x:
            if "权限" in str(x): raise
            time.sleep(2 + 3 * i)
    return None
JOBS = {  # name: (api, date-kw, extra)
    "stk_holdertrade": ("stk_holdertrade", "ann_date", {}),
    "forecast": ("forecast", "ann_date", {}),
    "share_float": ("share_float", "float_date", {}),
    "block_trade": ("block_trade", "trade_date", {}),
    "top_inst": ("top_inst", "trade_date", {}),
    "top_list": ("top_list", "trade_date", {}),
    "suspend_d": ("suspend_d", "trade_date", {}),
    "stock_st": ("stock_st", "trade_date", {}),
}
only = sys.argv[4].split(",") if len(sys.argv) > 4 else list(JOBS)
for name in only:
    p = f"{OUT}/{name}.parquet"
    if os.path.exists(p): continue
    api, kw, extra = JOBS[name]; fs = []
    for d in days:
        df = call(api, **{kw: d}, **extra)
        if df is not None and len(df): fs.append(df)
        time.sleep(0.13)
    pd.concat(fs).to_parquet(p); print(name, sum(map(len, fs)), flush=True)
# pledge_stat weekly snapshots
p = f"{OUT}/pledge_stat.parquet"
if "pledge_stat" in only or len(sys.argv) <= 4:
  if not os.path.exists(p):
    fs = []
    for d in pd.date_range(S, E, freq="W-FRI").strftime("%Y%m%d"):
        df = call("pledge_stat", end_date=d)
        if df is not None and len(df): fs.append(df)
        time.sleep(0.2)
    pd.concat(fs).to_parquet(p); print("pledge_stat", sum(map(len, fs)), flush=True)
