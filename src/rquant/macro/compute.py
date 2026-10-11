"""Pure transforms raw_macro_* frames -> macro_* frames (no I/O)."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

ENTITY_PATTERNS: dict[str, str] = {
    "社保基金": "社保基金|全国社会保障基金",
    "中央汇金": "中央汇金|汇金资产管理",
}

NORTH_REGIME_CHANGE = "20240819"

OPTION_UNDERLYINGS: dict[str, str] = {
    "OP510050.SH": "50ETF",
    "OP510300.SH": "沪300ETF",
    "OP510500.SH": "沪500ETF",
    "OP588000.SH": "科创50ETF",
    "OP588080.SH": "科创板50ETF",
    "OP159919.SZ": "深300ETF",
    "OP159922.SZ": "深500ETF",
    "OP159915.SZ": "创业板ETF",
    "OP159901.SZ": "深100ETF",
}


def entity_of(name: str) -> str | None:
    for ent, pat in ENTITY_PATTERNS.items():
        if pd.Series([name]).str.contains(pat).iloc[0]:
            return ent
    return None


def quarter_label(d: pd.Series) -> pd.Series:
    dt = pd.to_datetime(d)
    return dt.dt.year.astype(str) + "Q" + dt.dt.quarter.astype(str)


def holder_quarterly(holders: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """holders: entity, ts_code, ann_date, end_date, hold_amount (shares, one row per holder
    account); prices: end_date, ts_code, close.  Returns one row per (entity, basis, quarter)
    with basis 'report' (by end_date) or 'announce' (by quarter of the disclosure date)."""
    h = holders.groupby(["entity", "ts_code", "end_date"], as_index=False).agg(
        hold_amount=("hold_amount", "sum"), ann_date=("ann_date", "min")
    )
    h = h.merge(prices, on=["end_date", "ts_code"], how="left")
    periods = sorted(h.end_date.unique())
    idx = {p: i for i, p in enumerate(periods)}
    rows = []
    for ent, g in h.groupby("entity"):
        piv = g.pivot_table(
            index="ts_code", columns="end_date", values="hold_amount", aggfunc="sum"
        ).reindex(columns=periods)
        cur = g.set_index(["ts_code", "end_date"])
        for i, p in enumerate(periods):
            now = piv[p].dropna()
            prev = piv[periods[i - 1]].dropna() if i else pd.Series(dtype=float)
            px = (
                cur.xs(p, level="end_date")["close"]
                if p in g.end_date.values
                else pd.Series(dtype=float)
            )
            codes = now.index.union(prev.index)
            delta = now.reindex(codes).fillna(0) - prev.reindex(codes).fillna(0)
            # exits have no current-period price row; their value delta is left out (unknown px)
            flow = (delta * px.reindex(codes)).sum(skipna=True)
            mv = (now * px.reindex(now.index)).sum(skipna=True)
            rows.append(
                dict(
                    entity=ent,
                    basis="report",
                    quarter=quarter_label(pd.Series([p])).iloc[0],
                    period=p,
                    n_hold=len(now),
                    n_new=int((~now.index.isin(prev.index)).sum()) if i else None,
                    n_exit=int((~prev.index.isin(now.index)).sum()) if i else None,
                    n_add=int((delta > 0).sum()) if i else None,
                    n_cut=int((delta < 0).sum()) if i else None,
                    share_chg=float(delta.sum()) if i else None,
                    mv=float(mv),
                    flow=float(flow) if i else None,
                )
            )
        # announce basis: each stock-period delta counted in the quarter it became public
        g = g.sort_values(["ts_code", "end_date"])
        g["prev_amt"] = g.groupby("ts_code").hold_amount.shift()
        g["prev_end"] = g.groupby("ts_code").end_date.shift()
        contiguous = g.prev_end.map(lambda x: idx.get(x, -9)) == g.end_date.map(idx) - 1
        g["delta"] = np.where(contiguous, g.hold_amount - g.prev_amt, g.hold_amount)
        g["is_new"] = ~contiguous
        g["flowv"] = g.delta * g.close
        g["aq"] = quarter_label(g.ann_date)
        g = g[g.end_date > periods[0]]
        for q, a in g.groupby("aq"):
            rows.append(
                dict(
                    entity=ent,
                    basis="announce",
                    quarter=q,
                    period=None,
                    n_hold=len(a),
                    n_new=int(a.is_new.sum()),
                    n_exit=None,
                    n_add=int((a.delta > 0).sum()),
                    n_cut=int((a.delta < 0).sum()),
                    share_chg=float(a.delta.sum()),
                    mv=float((a.hold_amount * a.close).sum()),
                    flow=float(a.flowv.sum()),
                )
            )
    return pd.DataFrame(rows)


def gdp_ttm(gdp: pd.DataFrame) -> pd.DataFrame:
    """cn_gdp is year-to-date cumulative (亿元). TTM = YTD + last year's annual - last year's YTD.
    available_from ≈ quarter end + 20 days (NBS preliminary release), to avoid look-ahead."""
    g = gdp[["quarter", "gdp"]].copy()
    g["year"] = g.quarter.str[:4].astype(int)
    g["q"] = g.quarter.str[-1].astype(int)
    ytd = g.set_index(["year", "q"]).gdp
    out = []
    for (y, q), v in ytd.items():
        if q == 4:
            ttm = v
        elif (y - 1, 4) in ytd and (y - 1, q) in ytd:
            ttm = v + ytd[(y - 1, 4)] - ytd[(y - 1, q)]
        else:
            continue
        qend = pd.Timestamp(year=y, month=3 * q, day=1) + pd.offsets.MonthEnd(0)
        out.append(
            dict(quarter=f"{y}Q{q}", gdp_ttm=ttm, available_from=qend + pd.Timedelta(days=20))
        )
    return pd.DataFrame(out).sort_values("available_from")


def buffett(mv: pd.DataFrame, gdp: pd.DataFrame) -> pd.DataFrame:
    """mv: trade_date, total_mv (万元). Ratio = total_mv / latest published TTM GDP."""
    t = gdp_ttm(gdp)
    m = mv.copy()
    m["trade_date"] = pd.to_datetime(m.trade_date)
    m = m.sort_values("trade_date")
    m = pd.merge_asof(m, t, left_on="trade_date", right_on="available_from")
    m["mv_yi"] = m.total_mv / 1e4
    m["ratio"] = m.mv_yi / m.gdp_ttm
    return m.dropna(subset=["ratio"])[["trade_date", "mv_yi", "gdp_ttm", "quarter", "ratio"]]


def _bs(s: float, k: float, t: float, r: float, v: float, call: bool) -> float:
    d1 = (math.log(s / k) + (r + v * v / 2) * t) / (v * math.sqrt(t))
    d2 = d1 - v * math.sqrt(t)
    n = lambda x: 0.5 * (1 + math.erf(x / math.sqrt(2)))  # noqa: E731
    return (
        s * n(d1) - k * math.exp(-r * t) * n(d2)
        if call
        else k * math.exp(-r * t) * n(-d2) - s * n(-d1)
    )


def implied_vol(
    price: float, s: float, k: float, t: float, call: bool, r: float = 0.015
) -> float | None:
    if not (price > 0 and s > 0 and t > 0):
        return None
    lo, hi = 1e-3, 3.0
    if _bs(s, k, t, r, hi, call) < price or _bs(s, k, t, r, lo, call) > price:
        return None
    for _ in range(60):
        mid = (lo + hi) / 2
        if _bs(s, k, t, r, mid, call) > price:
            hi = mid
        else:
            lo = mid
    return (lo + hi) / 2


def option_daily(basic: pd.DataFrame, daily: pd.DataFrame, und: pd.DataFrame) -> pd.DataFrame:
    """Per underlying per day: PCR (vol/OI), totals, 20d surge, ATM IV (near month ≥7d)."""
    b = basic[basic.opt_code.isin(OPTION_UNDERLYINGS)]
    d = daily.merge(b, on="ts_code")
    d["und"] = d.opt_code.map(OPTION_UNDERLYINGS)
    agg = d.pivot_table(
        index=["trade_date", "und"], columns="call_put", values=["vol", "oi"], aggfunc="sum"
    ).fillna(0)
    agg.columns = [f"{a}_{c}" for a, c in agg.columns]
    agg = agg.reset_index()
    agg["vol"] = agg.vol_C + agg.vol_P
    agg["oi"] = agg.oi_C + agg.oi_P
    agg["pcr_vol"] = agg.vol_P / agg.vol_C.replace(0, np.nan)
    agg["pcr_oi"] = agg.oi_P / agg.oi_C.replace(0, np.nan)
    agg = agg.sort_values(["und", "trade_date"])
    for c in ["vol", "oi"]:
        base = agg.groupby("und")[c].transform(
            lambda s: s.shift().rolling(20, min_periods=10).mean()
        )
        agg[f"{c}_surge"] = agg[c] / base
    code2und = {k.replace("OP", ""): v for k, v in OPTION_UNDERLYINGS.items()}
    u = und.assign(und=und.ts_code.map(code2und))[["trade_date", "und", "close"]].rename(
        columns={"close": "spot"}
    )
    d = d.merge(u, on=["trade_date", "und"])
    d = d[d.per_unit == 10000]
    d["days"] = (pd.to_datetime(d.maturity_date) - pd.to_datetime(d.trade_date)).dt.days
    d = d[d.days >= 7]
    ivs = []
    for (td, un), g in d.groupby(["trade_date", "und"]):
        g = g[g.days == g.days.min()]
        spot = g.spot.iloc[0]
        k = g.exercise_price.iloc[(g.exercise_price - spot).abs().argsort().iloc[0]]
        vals = []
        for _, r in g[g.exercise_price == k].iterrows():
            px = r.settle if r.settle and r.settle > 0 else r.close
            v = implied_vol(px, spot, k, r.days / 365, r.call_put == "C")
            if v:
                vals.append(v)
        ivs.append(
            dict(trade_date=td, und=un, atm_iv=float(np.mean(vals)) if vals else None, spot=spot)
        )
    out = agg.merge(pd.DataFrame(ivs), on=["trade_date", "und"], how="left")
    return out[
        [
            "trade_date",
            "und",
            "spot",
            "vol",
            "oi",
            "pcr_vol",
            "pcr_oi",
            "vol_surge",
            "oi_surge",
            "atm_iv",
        ]
    ]


def daily_panel(
    index: pd.DataFrame,
    margin: pd.DataFrame,
    hsgt: pd.DataFrame,
    idxbasic: pd.DataFrame,
    cn10y: pd.DataFrame,
) -> pd.DataFrame:
    """One row per trade date: indices, margin, northbound, equity-bond spread."""
    ix = index.pivot_table(index="trade_date", columns="ts_code", values="close")
    ix = ix.rename(columns={"000300.SH": "csi300", "000985.CSI": "all_a"})
    amt = index[index.ts_code == "000985.CSI"].set_index("trade_date").amount * 1e3  # 千元→元
    m = margin.groupby("trade_date")[["rzye", "rzmre", "rzrqye"]].sum()
    p = ix[[c for c in ["csi300", "all_a"] if c in ix]].join(m, how="left")
    p["fin_buy_share"] = p.rzmre / amt.reindex(p.index)
    h = hsgt.set_index("trade_date")[["north_money"]].astype(float)
    p = p.join(h, how="left")
    ib = idxbasic.set_index("trade_date")[["pe_ttm"]]
    p = p.join(ib, how="left")
    y = cn10y.copy()
    y["trade_date"] = pd.to_datetime(y.date).dt.strftime("%Y%m%d")
    p = p.join(y.set_index("trade_date")[["cn10y"]], how="left")
    p["cn10y"] = p.cn10y.ffill(limit=5)
    p["erp"] = 100 / p.pe_ttm - p.cn10y
    p = p.reset_index()
    # 2024-08-19 起交易所不再披露北向每日净买入；之后 Tushare north_money 为成交额口径
    post = p.trade_date >= NORTH_REGIME_CHANGE
    p["north_net"] = p.north_money.where(~post)
    p["north_net_cum"] = p.north_net.fillna(0).cumsum().where(~post)
    p["north_turnover"] = p.north_money.where(post)
    return p


# --------------------------------------------------------------------------- studies
HORIZONS = (5, 10, 20, 60)


def first_days(sig: pd.Series, cooldown: int = 10) -> pd.Series:
    """Keep only the first day of each signal cluster (no repeat within ``cooldown`` rows)."""
    out = pd.Series(False, index=sig.index)
    last = -(10**9)
    for i, v in enumerate(sig.fillna(False).to_numpy()):
        if v and i - last > cooldown:
            out.iloc[i] = True
            last = i
    return out


def event_study(name: str, sig: pd.Series, close: pd.Series, cooldown: int = 10) -> list[dict]:
    """Forward returns after signal days vs all days (same sample)."""
    close = close.dropna()
    sig = sig.reindex(close.index).fillna(False).astype(bool)
    ev = first_days(sig, cooldown)
    rows = []
    for h in HORIZONS:
        fwd = close.shift(-h) / close - 1
        e = fwd[ev].dropna()
        base = fwd.dropna()
        rows.append(
            dict(
                study=name,
                horizon=h,
                n=len(e),
                mean=float(e.mean()) if len(e) else None,
                median=float(e.median()) if len(e) else None,
                win=float((e > 0).mean()) if len(e) else None,
                base_mean=float(base.mean()),
                base_win=float((base > 0).mean()),
            )
        )
    return rows


def bollinger(s: pd.Series, n: int = 20, k: float = 2.0) -> pd.DataFrame:
    m = s.rolling(n, min_periods=n).mean()
    sd = s.rolling(n, min_periods=n).std()
    return pd.DataFrame({"mid": m, "up": m + k * sd, "lo": m - k * sd})


def pcr_bands(opt: pd.DataFrame) -> pd.DataFrame:
    """Add 20d/2σ Bollinger bands on PCR (volume and OI) per underlying."""
    parts = []
    for _, g in opt.sort_values("trade_date").groupby("und"):
        g = g.copy()
        for c in ["pcr_vol", "pcr_oi"]:
            b = bollinger(g[c])
            g[f"{c}_up"], g[f"{c}_lo"] = b.up.to_numpy(), b.lo.to_numpy()
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


def pcr_studies(opt: pd.DataFrame, csi300: pd.Series) -> list[dict]:
    o = pcr_bands(opt)
    rows: list[dict] = []
    piv = {
        c: o.pivot_table(index="trade_date", columns="und", values=c, dropna=False).reindex(
            sorted(o.trade_date.unique())
        )
        for c in ["pcr_vol", "pcr_vol_lo", "pcr_vol_up", "pcr_oi", "pcr_oi_lo", "pcr_oi_up"]
    }
    for kind in ["vol", "oi"]:
        v, lo, up = piv[f"pcr_{kind}"], piv[f"pcr_{kind}_lo"], piv[f"pcr_{kind}_up"]
        for und in ["50ETF", "沪300ETF"]:
            if und not in v:
                continue
            rows += event_study(f"{und} PCR({kind})跌破下轨", v[und] < lo[und], csi300)
            rows += event_study(f"{und} PCR({kind})突破上轨", v[und] > up[und], csi300)
        if {"50ETF", "沪300ETF"} <= set(v.columns):
            sig = (v["沪300ETF"] < lo["沪300ETF"]) & ~(v["50ETF"] < lo["50ETF"])
            rows += event_study(f"300破下轨而50未破 PCR({kind})", sig, csi300)
    return rows


def breadth(bars: pd.DataFrame, index_close: pd.Series) -> pd.DataFrame:
    """bars: ts_code, trade_date, close (hfq), pct_chg. Daily breadth panel."""
    b = bars.copy()
    b["trade_date"] = pd.to_datetime(b.trade_date).dt.strftime("%Y%m%d")
    px = b.pivot_table(index="trade_date", columns="ts_code", values="close").sort_index()
    ch = b.pivot_table(index="trade_date", columns="ts_code", values="pct_chg").sort_index()
    adv, dec = (ch > 0).sum(axis=1), (ch < 0).sum(axis=1)
    net = (adv - dec) / (adv + dec).replace(0, np.nan) * 1000
    out = pd.DataFrame(index=px.index)
    out["adv"], out["dec"] = adv, dec
    out["adl"] = (adv - dec).cumsum()
    out["adl_ema"] = out.adl.ewm(span=10, adjust=False).mean()
    out["mcclellan"] = net.ewm(span=19, adjust=False).mean() - net.ewm(span=39, adjust=False).mean()
    valid = px.notna()
    for n in (20, 60):
        ma = px.rolling(n, min_periods=n).mean()
        has = ma.notna() & valid
        out[f"pct_ma{n}"] = ((px > ma) & has).sum(axis=1) / has.sum(axis=1).replace(0, np.nan)
    hi = px.rolling(250, min_periods=250).max()
    lo = px.rolling(250, min_periods=250).min()
    full = hi.notna()
    out["nh_nl"] = ((px >= hi) & full).sum(axis=1) - ((px <= lo) & full).sum(axis=1)
    out.loc[full.sum(axis=1) < 100, "nh_nl"] = np.nan
    idx = index_close.reindex(out.index)
    out["index"] = idx
    at_hi = idx >= idx.rolling(60, min_periods=60).max()
    at_lo = idx <= idx.rolling(60, min_periods=60).min()
    out["bear_div"] = at_hi & (out.pct_ma20 < 0.5)
    out["bull_div"] = at_lo & (out.mcclellan > 0)
    return out.reset_index()


def breadth_studies(br: pd.DataFrame, close: pd.Series) -> list[dict]:
    b = br.set_index("trade_date")
    c = close.reindex(b.index)
    rows: list[dict] = []
    rows += event_study("麦克莱伦 上穿0", (b.mcclellan > 0) & (b.mcclellan.shift() <= 0), c)
    rows += event_study("麦克莱伦 下穿0", (b.mcclellan < 0) & (b.mcclellan.shift() >= 0), c)
    rows += event_study("MA20上方占比<15%（超卖）", b.pct_ma20 < 0.15, c)
    rows += event_study("MA20上方占比>85%（超买）", b.pct_ma20 > 0.85, c)
    rows += event_study("顶背离：指数60日新高但MA20上方<50%", b.bear_div, c)
    rows += event_study("底背离：指数60日新低但麦克莱伦>0", b.bull_div, c)
    return rows


def hk_system(hk: pd.DataFrame, hk_etf: pd.DataFrame, index: pd.DataFrame) -> pd.DataFrame:
    """'定价权系统C' approximation. HKTECH has no volume in Tushare, so volume comes from the
    A-share HSTECH ETFs (513180/513130). SMARTMONEY = cumulative EMA5 of (HKTECH daily return ×
    ETF volume / its 20d mean); signal = SMARTMONEY > MA20 and 50/300/创业板 all up on the day."""
    h = hk.set_index("trade_date").close.sort_index().rename("hk")
    v = hk_etf.groupby("trade_date").vol.sum().sort_index()
    vr = (v / v.rolling(20, min_periods=10).mean()).rename("vr")
    ix = index.pivot_table(index="trade_date", columns="ts_code", values="close").sort_index()
    d = ix.join(h, how="inner").join(vr, how="left")
    d["vr"] = d.vr.fillna(1.0)
    r = d.hk.pct_change().fillna(0)
    d["smart"] = (r * d.vr).ewm(span=5, adjust=False).mean().cumsum()
    d["smart_ma20"] = d.smart.rolling(20).mean()
    ups = [(d[c] > d[c].shift()) for c in ["000016.SH", "000300.SH", "399006.SZ"] if c in d]
    all_up = np.logical_and.reduce(ups) if ups else False
    d["signal"] = (d.smart > d.smart_ma20) & all_up
    d = d.rename(columns={"000300.SH": "csi300"})
    return d.reset_index()[["trade_date", "csi300", "hk", "smart", "smart_ma20", "signal"]]


def lead_lag(hk: pd.Series, a: pd.Series, max_lag: int = 60) -> pd.DataFrame:
    """corr(HK return on t, CSI300 return on t+k). HK closes an hour after A-shares, so k=0
    partly contains A→HK information; k≥1 is the meaningful 'HK leads' reading."""
    df = pd.concat([hk.pct_change(), a.pct_change()], axis=1, keys=["hk", "a"]).dropna()
    rows = []
    for k in range(max_lag + 1):
        rows.append(
            dict(
                lag=k,
                corr=float(df.hk.corr(df.a.shift(-k))),
                corr_rev=float(df.a.corr(df.hk.shift(-k))),
            )
        )
    return pd.DataFrame(rows)


def rolling_lag1(hk: pd.Series, a: pd.Series, window: int = 250) -> pd.DataFrame:
    df = pd.concat([hk.pct_change(), a.pct_change().shift(-1)], axis=1, keys=["hk", "a1"]).dropna()
    return df.hk.rolling(window).corr(df.a1).rename("corr_lag1").reset_index()


FUT_SPOT = {"IF": "000300.SH", "IH": "000016.SH", "IC": "000905.SH", "IM": "000852.SH"}


def futures(fut: pd.DataFrame, holding: pd.DataFrame, index: pd.DataFrame) -> pd.DataFrame:
    """Main-contract basis (close/spot−1) and CFFEX top-20 members' net long (long−short),
    summed over all listed months of IF/IH/IC/IM."""
    f = fut.assign(sym=fut.ts_code.str[:2])[["trade_date", "sym", "close", "oi"]]
    spot = index[index.ts_code.isin(FUT_SPOT.values())].rename(columns={"close": "spot"})
    spot = spot.assign(sym=spot.ts_code.map({v: k for k, v in FUT_SPOT.items()}))
    f = f.merge(spot[["trade_date", "sym", "spot"]], on=["trade_date", "sym"], how="left")
    f["basis_pct"] = f.close / f.spot - 1
    h = holding.assign(net=holding.long_hld - holding.short_hld)
    h["net_chg"] = h.long_chg.fillna(0) - h.short_chg.fillna(0)
    f = f.merge(
        h[["trade_date", "sym", "long_hld", "short_hld", "net", "net_chg"]],
        on=["trade_date", "sym"],
        how="left",
    )
    return f.sort_values(["sym", "trade_date"]).reset_index(drop=True)


def etf_flows(share: pd.DataFrame, und_close: pd.DataFrame | None = None) -> pd.DataFrame:
    """Daily share change per broad ETF (万份) — subscriptions(+)/redemptions(−)."""
    s = share.sort_values(["ts_code", "trade_date"]).copy()
    s["chg"] = s.groupby("ts_code").fd_share.diff()
    return s
