"""Predict next-day 异常波动/严重异常波动 triggers (read-only prototype, Tushare daily+index_daily).
Rule (SSE/SZSE 交易规则 2026): dev = stock cum return - index cum return over window (close/前收, 复权).
 异动: 3日 |dev| >= 20% (主板) / 30% (创业板/科创板).  严重: 10日 +100%/-50%, 30日 +200%/-70%,
 or 同向异动 4次(主板)/3次(创业/科创) in 10 days (not modelled).  BJ / no-limit stocks excluded.
Index: 沪主板 000001.SH, 深主板 399001.SZ, 创业板 399102.SZ(创业板综指), 科创板 000688.SH.
'Near' = next day a single limit move (主板10%, 创/科20%) would push the rolled window over the line.
Caveat: counter resets after the exchange announcement; not modelled (use 异动公告/top_list reason to reset)."""
import sys, time, pandas as pd, numpy as np
from dotenv import dotenv_values
import tushare as ts
e = dotenv_values("/home/lighthouse/rquant/.env")
pro = ts.pro_api(e.get("TUSHARE_TOKEN") or [v for k, v in e.items() if "TUSHARE" in k][0])
T = sys.argv[1] if len(sys.argv) > 1 else "20261014"
days = sorted(pro.trade_cal(exchange="SSE", end_date=T, start_date="20260701", is_open="1").cal_date)[-31:]
px = []
for d in days:
    df = pro.daily(trade_date=d, fields="ts_code,trade_date,close,pre_close"); px.append(df); time.sleep(0.15)
px = pd.concat(px)
adj = pro.adj_factor(trade_date=T)  # unused: daily pct = close/pre_close already ex-right adjusted per day
r = (px.close / px.pre_close).values; px["r"] = r
R = px.pivot(index="trade_date", columns="ts_code", values="r").sort_index()
IDX = {"000001.SH": None, "399001.SZ": None, "399102.SZ": None, "000688.SH": None}
for k in IDX:
    i = pro.index_daily(ts_code=k, start_date=days[0], end_date=T).set_index("trade_date").sort_index()
    IDX[k] = (i.close / i.pre_close).reindex(R.index)
c = R.columns.to_series()
grp = np.select([c.str.startswith(("30",)), c.str.startswith("68"), c.str.endswith(".SH"), c.str.endswith(".SZ")],
                ["399102.SZ", "000688.SH", "000001.SH", "399001.SZ"], "BJ")
def dev(n):  # cumulative deviation (%) over last n days
    s = R.tail(n).prod(min_count=n)
    ix = pd.Series({k: v.tail(n).prod() for k, v in IDX.items()})
    return (s - ix.reindex(grp).values) * 100
gem = pd.Series(np.isin(grp, ["399102.SZ", "000688.SH"]), index=c.index)
thr3 = np.where(gem, 30, 20); lim = np.where(gem, 20, 10)
d2, d3, d9, d10, d29, d30 = dev(2), dev(3), dev(9), dev(10), dev(29), dev(30)
out = pd.DataFrame({"grp": grp, "dev3": d3, "dev2": d2, "dev10": d10, "dev9": d9, "dev30": d30, "dev29": d29})
out = out[out.grp != "BJ"]
out["abn_today"] = out.dev3.abs() >= thr3[c.index.get_indexer(out.index)]
L = lim[c.index.get_indexer(out.index)]; th = thr3[c.index.get_indexer(out.index)]
out["near_abn_up"] = (out.dev2 + L >= th) & ~out.abn_today      # next-day limit-up would trigger 异动
out["near_abn_dn"] = (out.dev2 - L <= -th) & ~out.abn_today
out["serious_today"] = (out.dev10 >= 100) | (out.dev10 <= -50) | (out.dev30 >= 200) | (out.dev30 <= -70)
out["near_serious"] = ((out.dev9 + L >= 100) | (out.dev29 + L >= 200)) & ~out.serious_today
f = out[out[["abn_today", "near_abn_up", "near_abn_dn", "serious_today", "near_serious"]].any(axis=1)].round(1)
print("T", T, "universe", len(out), {k: int(out[k].sum()) for k in ["abn_today", "near_abn_up", "near_abn_dn", "serious_today", "near_serious"]})
print(f.sort_values("dev3", ascending=False).head(15).to_string())
