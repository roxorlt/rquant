"""fina_audit for every listed stock (one call per code). python fetch_audit.py OUT"""
import sys, time, os, pandas as pd
from dotenv import dotenv_values
import tushare as ts
e = dotenv_values("/home/lighthouse/rquant/.env")
pro = ts.pro_api(e.get("TUSHARE_TOKEN") or [v for k, v in e.items() if "TUSHARE" in k][0])
OUT = sys.argv[1]
codes = pd.concat([pro.stock_basic(list_status=s, fields="ts_code") for s in "LD"]).ts_code.tolist()
fs = []
for i, c in enumerate(codes):
    for k in range(3):
        try:
            df = pro.fina_audit(ts_code=c, start_date="20200101"); fs.append(df); break
        except Exception:
            time.sleep(3)
    time.sleep(0.12)
    if i % 500 == 0: print(i, flush=True)
pd.concat(fs).to_parquet(f"{OUT}/fina_audit.parquet"); print("fina_audit", sum(map(len, fs)))
