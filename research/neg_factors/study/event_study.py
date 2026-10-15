"""Negative-factor + calendar-node event study (read-only). Run on server:
  python event_study.py DATA_DIR OUT_DIR
Inputs: market.duckdb (daily_bar, daily_state, daily_basic, namechange), event parquets in DATA_DIR.
Signal day T = last trading day <= announcement date (info known before T+1 open).
Outcomes (close-to-close from T close, excess vs same-universe cross-sectional median):
  x1 (T+1), x5 (T+1..T+5), x20; gap1 = T+1 open/T close - 1 (raw, for short-term).
Universes: ALL (non-BJ, non-ST), POOL (circ_mv top1500 & listed>365d), LU (closed limit-up on T).
Split: train < 2025-07-01 <= test (events only exist from 2025-01, so train is short for them).
"""
import sys, json, time
import numpy as np, pandas as pd, duckdb

def t1(x):
    x = np.asarray(x, float); return x.mean() / (x.std(ddof=1) / np.sqrt(len(x)) + 1e-12)
def tw(a, b):
    a, b = np.asarray(a, float), np.asarray(b, float)
    return (a.mean() - b.mean()) / np.sqrt(a.var(ddof=1) / len(a) + b.var(ddof=1) / len(b) + 1e-12)

DATA, OUT = sys.argv[1], sys.argv[2]
SPLIT = pd.Timestamp("2025-07-01")
con = duckdb.connect("/home/lighthouse/rquant-research/market.duckdb", read_only=True)
bar = con.execute("""SELECT b.ts_code, b.trade_date, b.open, b.high, b.low, b.close, b.pre_close,
  s.limit_pct FROM daily_bar b LEFT JOIN daily_state s USING (ts_code, trade_date)
  WHERE b.trade_date >= DATE '2021-12-01'""").df()
bar["trade_date"] = pd.to_datetime(bar.trade_date)
basic = con.execute("SELECT ts_code, trade_date, circ_mv FROM daily_basic WHERE trade_date >= DATE '2021-12-01'").df()
basic["trade_date"] = pd.to_datetime(basic.trade_date)
sb = con.execute("SELECT ts_code, list_date FROM stock_basic").df()
nc = con.execute("SELECT ts_code, name, start_date, end_date FROM namechange").df()
days = pd.DatetimeIndex(sorted(bar.trade_date.unique()))
W = lambda col: bar.pivot(index="trade_date", columns="ts_code", values=col).reindex(days)
C, O, H, PC, LP = W("close"), W("open"), W("high"), W("pre_close"), W("limit_pct")
codes = C.columns
R = C / PC - 1                                    # ex-right adjusted daily return
LP = LP.fillna(pd.DataFrame(np.where(pd.Series(codes).str.startswith(("30", "68")), .2, .1)[None, :].repeat(len(days), 0), index=days, columns=codes))
UP = np.round(PC * (1 + LP) + 1e-9, 2)
LU = (C >= UP - 1e-6) & C.notna()
ZB = (H >= UP - 1e-6) & ~LU                       # 炸板
YZ = LU & (H == W("low"))                          # 一字
# consecutive limit-up count (tier)
tier = LU.astype(int)
for i in range(1, len(days)):
    tier.iloc[i] = (tier.iloc[i - 1] + 1) * LU.iloc[i]
# ST mask (PIT from namechange)
st = pd.DataFrame(False, index=days, columns=codes)
for r in nc[nc.name.str.contains("ST|退")].itertuples():
    if r.ts_code in st.columns:
        e = pd.Timestamp(r.end_date) if pd.notna(r.end_date) else days[-1]
        st.loc[pd.Timestamp(r.start_date):e, r.ts_code] = True
bj = pd.Series(codes).str.endswith(".BJ").values
mv = basic.pivot(index="trade_date", columns="ts_code", values="circ_mv").reindex(index=days, columns=codes)
ld = pd.to_datetime(sb.set_index("ts_code").list_date.reindex(codes), errors="coerce")
age_ok = pd.DataFrame((days.values[:, None] - ld.values[None, :]) > np.timedelta64(365, "D"), index=days, columns=codes)
U_ALL = (~st) & C.notna() & ~bj[None, :]
U_POOL = U_ALL & (mv.rank(axis=1, ascending=False) <= 1500) & age_ok
U_LU = U_ALL & LU

def fwd(h):
    lr = np.log1p(R)
    return np.expm1(lr[::-1].rolling(h, min_periods=h).sum()[::-1].shift(-1))
F = {"x1": fwd(1), "x5": fwd(5), "x20": fwd(20)}
GAP = O.shift(-1) / C - 1

def excess(f, U):
    med = f.where(U).median(axis=1)
    return f.sub(med, axis=0)

def tday(d):  # last trading day <= d
    i = days.searchsorted(pd.Timestamp(d), side="right") - 1
    return days[i] if i >= 0 else pd.NaT

def summarize(ev, name, include_st=False):
    """ev: DataFrame[ts_code, T]. Returns rows per universe/split."""
    ev = ev.dropna().drop_duplicates()
    ev = ev[ev.ts_code.isin(codes) & ev["sd"].isin(days)]
    rows = []
    for un, U in (("ALL", U_ALL | st if include_st else U_ALL), ("POOL", U_POOL), ("LU", U_LU)):
        ii = days.get_indexer(ev["sd"]); jj = codes.get_indexer(ev.ts_code)
        m = U.values[ii, jj]
        if m.sum() == 0:
            continue
        rec = {}
        for k, f in F.items():
            x = excess(f, U).values[ii, jj][m]
            rec[k] = x
        rec["gap"] = GAP.values[ii, jj][m]
        Ts = ev["sd"].values[m]
        for split, sm in (("train", Ts < SPLIT.to_datetime64()), ("test", Ts >= SPLIT.to_datetime64()), ("all", np.ones(len(Ts), bool))):
            row = {"factor": name, "universe": un, "split": split, "n": int(sm.sum()), "n_days": int(len(np.unique(Ts[sm])))}
            for k in ("x1", "x5", "x20", "gap"):
                x = rec[k][sm]; x = x[~np.isnan(x)]
                row[k] = round(float(np.mean(x)) * 100, 3) if len(x) else None
                row[k + "_t"] = round(float(t1(x)), 2) if len(x) > 2 else None
            rows.append(row)
    return rows

ROWS = []
rd = lambda f: pd.read_parquet(f"{DATA}/{f}.parquet")
# 1 减持
h = rd("stk_holdertrade"); h = h[h.in_de == "DE"]
ROWS += summarize(pd.DataFrame({"ts_code": h.ts_code, "sd": h.ann_date.map(tday)}), "减持(stk_holdertrade DE)")
big = h[pd.to_numeric(h.change_ratio, errors="coerce") >= 1]
ROWS += summarize(pd.DataFrame({"ts_code": big.ts_code, "sd": big.ann_date.map(tday)}), "减持≥1%股本")
# 2 业绩预告负面
fc = rd("forecast"); neg = fc[fc.type.isin(["预减", "首亏", "续亏", "略减"])]
ROWS += summarize(pd.DataFrame({"ts_code": neg.ts_code, "sd": neg.ann_date.map(tday)}), "业绩预告负面(预减/首亏/续亏/略减)")
ROWS += summarize(pd.DataFrame({"ts_code": fc[fc.type == "首亏"].ts_code, "sd": fc[fc.type == "首亏"].ann_date.map(tday)}), "业绩预告首亏")
# 3 解禁 (window: T = 5 trading days before float_date)
sf = rd("share_float"); sf["float_ratio"] = pd.to_numeric(sf.float_ratio, errors="coerce")
g = sf.groupby(["ts_code", "float_date"]).float_ratio.sum().reset_index()
def t_before(d, k=5):
    i = days.searchsorted(pd.Timestamp(d)) - k
    return days[i] if 0 <= i < len(days) else pd.NaT
for thr in (1, 5):
    gg = g[g.float_ratio >= thr]
    ROWS += summarize(pd.DataFrame({"ts_code": gg.ts_code, "sd": gg.float_date.map(t_before)}), f"解禁前5日(占比≥{thr}%)")
# 4 大宗折价
bt = rd("block_trade"); bt["sd"] = pd.to_datetime(bt.trade_date)
bt = bt[bt["sd"].isin(days) & bt.ts_code.isin(codes)]
bt["c"] = C.values[days.get_indexer(bt["sd"]), codes.get_indexer(bt.ts_code)]
bt["disc"] = bt.price / bt.c - 1
d5 = bt[bt.disc <= -0.05]
ROWS += summarize(d5[["ts_code", "sd"]], "大宗折价≥5%")
# 5 龙虎榜机构净卖
ti = rd("top_inst"); ti = ti[ti.exalter.astype(str).str.contains("机构专用")]
net = ti.groupby(["ts_code", "trade_date"]).net_buy.sum().reset_index()
ns = net[net.net_buy < 0]
ROWS += summarize(pd.DataFrame({"ts_code": ns.ts_code, "sd": pd.to_datetime(ns.trade_date)}), "龙虎榜机构净卖")
nb = net[net.net_buy > 0]
ROWS += summarize(pd.DataFrame({"ts_code": nb.ts_code, "sd": pd.to_datetime(nb.trade_date)}), "(对照)龙虎榜机构净买")
# 6 交易所异动公告 (top_list reason)
tl = rd("top_list"); tl["sd"] = pd.to_datetime(tl.trade_date)
ab = tl[tl.reason.astype(str).str.contains("偏离值累计")]
ROWS += summarize(ab[["ts_code", "sd"]], "已发布异常波动(top_list 3日偏离)")
sev = tl[tl.reason.astype(str).str.contains("严重异常")]
ROWS += summarize(sev[["ts_code", "sd"]], "已发布严重异常波动")
# 7 复牌
sp = rd("suspend_d"); rs = sp[sp.suspend_type == "R"]
ROWS += summarize(pd.DataFrame({"ts_code": rs.ts_code, "sd": pd.to_datetime(rs.trade_date)}), "复牌日")
# 8 新进 ST
s = rd("stock_st"); s["sd"] = pd.to_datetime(s.trade_date)
first = s.sort_values("sd").groupby("ts_code")["sd"].first().reset_index()
first = first[first["sd"] > pd.Timestamp("2025-01-10")]
ROWS += summarize(first, "新被ST(首次出现在stock_st)", include_st=True)
# 9 质押率
pl = rd("pledge_stat"); pl["sd"] = pd.to_datetime(pl.end_date).map(tday)
pl = pl[pd.to_numeric(pl.pledge_ratio, errors="coerce") >= 50].drop_duplicates(["ts_code", "sd"])
ROWS += summarize(pl[["ts_code", "sd"]], "质押率≥50%(周度状态)")
# 10 审计非标
try:
    au = rd("fina_audit")
    ns_ = au[~au.audit_result.astype(str).str.contains("标准无保留") & au.audit_result.notna()]
    ns_ = ns_[pd.to_datetime(ns_.ann_date) >= "2022-01-01"]
    ROWS += summarize(pd.DataFrame({"ts_code": ns_.ts_code, "sd": ns_.ann_date.map(tday)}), "审计非标意见")
except FileNotFoundError:
    pass
# 11 预测异动 (needs index): approximate with own index set
idx = con.execute("SELECT ts_code, trade_date, close FROM index_daily").df()
idx["trade_date"] = pd.to_datetime(idx.trade_date)
IX = idx.pivot(index="trade_date", columns="ts_code", values="close").reindex(days)
IR = IX.pct_change()
grp = np.select([pd.Series(codes).str.startswith("30"), pd.Series(codes).str.startswith("68"), pd.Series(codes).str.endswith(".SH")],
                ["399006.SZ", "000688.SH" if "000688.SH" in IX else "000001.SH", "000001.SH"], "000001.SH")
IRc = IR.reindex(columns=pd.unique(grp))
def dev(n):
    s = np.log1p(R).rolling(n).sum()
    ix = np.log1p(IRc).rolling(n).sum()
    ixm = ix.reindex(columns=grp).values
    return (np.expm1(s) - np.expm1(pd.DataFrame(ixm, index=days, columns=codes))) * 100
d2, d3 = dev(2), dev(3)
gem = pd.Series(codes).str.startswith(("30", "68")).values
thr = np.where(gem, 30, 20)[None, :]; lim = np.where(gem, 20, 10)[None, :]
near = (d2.values + lim >= thr) & ~(np.abs(d3.values) >= thr)
abn = d3.values >= thr
ii, jj = np.nonzero(near & U_ALL.values)
ROWS += summarize(pd.DataFrame({"ts_code": codes[jj], "sd": days[ii]}), "预测:再一个涨停即触发异动")
ii, jj = np.nonzero(abn & U_ALL.values)
ROWS += summarize(pd.DataFrame({"ts_code": codes[jj], "sd": days[ii]}), "自算:已达3日偏离阈值")
pd.DataFrame(ROWS).to_csv(f"{OUT}/factor_event_study.csv", index=False)
print("factors done", len(ROWS), flush=True)

# ── calendar nodes (market level) ─────────────────────────────────────────────
cal = pd.DataFrame(index=days)
nxt = pd.Series(days[1:].tolist() + [pd.NaT], index=days)
ym = days.to_period("M")
pos_from_end = pd.Series(range(len(days)), index=days).groupby(ym).transform(lambda s: s.max() - s)  # 0 = last day
cal["月末最后1日"] = pos_from_end.values == 0
cal["月末最后3日"] = pos_from_end.values <= 2
wk = days.to_period("W")
last_week = pd.Series(wk, index=days).groupby(ym).transform("max").values == wk
cal["月末周周四周五"] = last_week & np.isin(days.dayofweek, [3, 4])
cal["季末最后3日"] = cal["月末最后3日"] & np.isin(days.month, [3, 6, 9, 12])
cal["半年末/年末最后3日"] = cal["月末最后3日"] & np.isin(days.month, [6, 12])
ddl = []
for y in range(2021, 2027):
    for md in ("04-30", "08-31", "10-31"):
        dd = pd.Timestamp(f"{y}-{md}"); i = days.searchsorted(dd, side="right") - 1
        ddl += list(days[max(0, i - 4): i + 1])
cal["披露截止前一周"] = days.isin(ddl)
gapd = (nxt - pd.Series(days, index=days)).dt.days
hol = gapd >= 4  # next trading day ≥4 calendar days later -> holiday (not just weekend)
prev_hol = pd.Series(hol.values, index=days).shift(-1).fillna(False).values
cal["节前最后1日"] = hol.values
cal["节前最后2日"] = hol.values | prev_hol
# daily market-level outcomes for short-term trading, measured on T+1
lu_y = U_LU.shift(1).fillna(False)                    # yesterday's limit-ups
def mean_ret(mask, ret):
    return ret.where(mask).mean(axis=1) * 100
out = pd.DataFrame(index=days)
for name, m in (("首板", tier.eq(1)), ("2板", tier.eq(2)), ("3板+", tier.ge(3))):
    out[f"{name}次日收益"] = mean_ret(U_LU & m, R.shift(-1))
    out[f"{name}次日开盘"] = mean_ret(U_LU & m, GAP)
pool = U_LU & ~YZ & (GAP > 0.04)                       # 竞价池 proxy: 昨日涨停非一字 & 今日高开>4%
out["竞价池次日开→收"] = mean_ret(pool, C.shift(-1) / O.shift(-1) - 1)
zb1, lu1 = (ZB & U_ALL).shift(-1).sum(axis=1), (LU & U_ALL).shift(-1).sum(axis=1)
out["次日炸板率"] = zb1 / (zb1 + lu1).replace(0, np.nan) * 100
out["次日涨停家数"] = lu1
out["全市场次日中位收益"] = R.shift(-1).where(U_ALL).median(axis=1) * 100
out = out.iloc[:-1]
crow = []
for tag in cal.columns:
    t = cal[tag].reindex(out.index).fillna(False).astype(bool).values
    for split, sm in (("train", out.index < pd.Timestamp("2024-07-01")), ("test", out.index >= pd.Timestamp("2024-07-01"))):
        for k in out.columns:
            a, b = out[k][t & sm].dropna(), out[k][~t & sm].dropna()
            if len(a) < 2:
                continue
            crow.append({"tag": tag, "split": split, "metric": k, "n_tag": len(a), "tag_mean": round(a.mean(), 3),
                         "other_mean": round(b.mean(), 3), "diff": round(a.mean() - b.mean(), 3),
                         "t": round(float(tw(a, b)), 2)})
pd.DataFrame(crow).to_csv(f"{OUT}/calendar_study.csv", index=False)
print("calendar done", len(crow))
