"""Breadth variants (raw + smoothed), grouped by principle, evaluated with a train/test split.

Groups: 累计涨跌 (ADL family, McClellan summation), 涨跌比率 (ADR, McClellan osc, Zweig),
均线参与度 (% above MA20/60), 新高新低 (NH-NL). Selection per group uses the TRAIN period
only (|rank IC| of level vs index forward 20d return); the TEST period is reported as is.
Forward windows overlap, so ICs are autocorrelated — treat significance loosely.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from rquant.macro.compute import event_study

SPLIT = "20240701"  # train < SPLIT <= test
HORIZONS = (5, 20, 60)


def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def variants(bars: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    """bars: ts_code, trade_date(YYYYMMDD), close (hfq), pct_chg. Returns (panel, group map)."""
    b = bars.copy()
    b["trade_date"] = pd.to_datetime(b.trade_date).dt.strftime("%Y%m%d")
    px = b.pivot_table(index="trade_date", columns="ts_code", values="close").sort_index()
    ch = b.pivot_table(index="trade_date", columns="ts_code", values="pct_chg").sort_index()
    adv, dec = (ch > 0).sum(axis=1).astype(float), (ch < 0).sum(axis=1).astype(float)
    tot = (adv + dec).replace(0, np.nan)
    net = adv - dec
    ratio_net = net / tot * 1000
    v: dict[str, pd.Series] = {}
    g: dict[str, str] = {}

    def add(name: str, group: str, s: pd.Series) -> None:
        v[name], g[name] = s, group

    adl = net.cumsum()
    add("ADL_20日变化", "累计涨跌", adl.diff(20))
    for n in (5, 10, 20):
        add(f"ADL_EMA{n}_20日变化", "累计涨跌", _ema(adl, n).diff(20))
    add("ADL偏离MA20", "累计涨跌", adl - adl.rolling(20).mean())
    osc = _ema(ratio_net, 19) - _ema(ratio_net, 39)
    summ = osc.cumsum()
    add("麦克莱伦累计指数_20日变化", "累计涨跌", summ.diff(20))
    add("麦克莱伦累计指数偏离MA60", "累计涨跌", summ - summ.rolling(60).mean())
    add("ADR当日", "涨跌比率", adv / dec.replace(0, np.nan))
    for n in (5, 10, 20):
        add(f"ADR{n}", "涨跌比率", adv.rolling(n).sum() / dec.rolling(n).sum().replace(0, np.nan))
    add("ADR10_EMA5", "涨跌比率", _ema(adv.rolling(10).sum() / dec.rolling(10).sum(), 5))
    add("麦克莱伦振荡", "涨跌比率", osc)
    add("麦克莱伦振荡_EMA5", "涨跌比率", _ema(osc, 5))
    zw = _ema(adv / tot, 10)
    add("Zweig_EMA10上涨占比", "涨跌比率", zw)
    valid = px.notna()
    for n in (20, 60):
        ma = px.rolling(n, min_periods=n).mean()
        has = ma.notna() & valid
        p = ((px > ma) & has).sum(axis=1) / has.sum(axis=1).replace(0, np.nan)
        add(f"MA{n}上方占比", "均线参与度", p)
        add(f"MA{n}上方占比_EMA5", "均线参与度", _ema(p, 5))
        add(f"MA{n}上方占比_EMA10", "均线参与度", _ema(p, 10))
    hi = px.rolling(250, min_periods=250).max()
    lo = px.rolling(250, min_periods=250).min()
    full = hi.notna()
    nh = ((px >= hi) & full).sum(axis=1).astype(float)
    nl = ((px <= lo) & full).sum(axis=1).astype(float)
    ok = full.sum(axis=1) >= 100
    nhnl = (nh - nl).where(ok)
    add("新高-新低", "新高新低", nhnl)
    for n in (5, 10, 20):
        add(f"新高-新低_MA{n}", "新高新低", nhnl.rolling(n).mean())
    add(
        "新高占比NH/(NH+NL)_MA10",
        "新高新低",
        (nh.rolling(10).sum() / (nh + nl).rolling(10).sum().replace(0, np.nan)).where(ok),
    )
    panel = pd.DataFrame(v)
    panel["adv"], panel["dec"] = adv, dec
    # Zweig breadth thrust: EMA10 of adv share from <0.40 to >0.615 within 10 days
    was_low = (zw < 0.40).astype(int).rolling(10, min_periods=1).max().astype(bool)
    panel["zweig_thrust"] = (zw > 0.615) & was_low.shift(1, fill_value=False)
    return panel, g


def _ic(x: pd.Series, fwd: pd.Series) -> float | None:
    d = pd.concat([x, fwd], axis=1).dropna()
    return float(d.iloc[:, 0].rank().corr(d.iloc[:, 1].rank())) if len(d) > 60 else None


def evaluate(
    panel: pd.DataFrame, groups: dict[str, str], close: pd.Series
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (scores per variant×split×horizon, event studies with split)."""
    close = close.reindex(panel.index)
    fwd = {h: close.shift(-h) / close - 1 for h in HORIZONS}
    tr = panel.index < SPLIT
    rows, events = [], []
    for name, grp in groups.items():
        x = panel[name]
        p10, p90 = x[tr].quantile(0.1), x[tr].quantile(0.9)
        for split, m in (("train", tr), ("test", ~tr)):
            for h in HORIZONS:
                f = fwd[h][m]
                xi = x[m]
                hi_r = f[xi > p90].dropna()
                lo_r = f[xi < p10].dropna()
                rows.append(
                    dict(
                        group=grp,
                        variant=name,
                        split=split,
                        horizon=h,
                        ic=_ic(xi, f),
                        top_mean=float(hi_r.mean()) if len(hi_r) else None,
                        top_win=float((hi_r > 0).mean()) if len(hi_r) else None,
                        top_n=len(hi_r),
                        bot_mean=float(lo_r.mean()) if len(lo_r) else None,
                        bot_win=float((lo_r > 0).mean()) if len(lo_r) else None,
                        bot_n=len(lo_r),
                        base_mean=float(f.dropna().mean()),
                    )
                )
        # divergences: index at 60d high while indicator below its train p50 (and vice versa)
        idx_hi = close >= close.rolling(60, min_periods=60).max()
        idx_lo = close <= close.rolling(60, min_periods=60).min()
        med = x[tr].median()
        for split, m in (("train", tr), ("test", ~tr)):
            for lab, sig in (
                (f"{name} 顶背离", idx_hi & (x < med)),
                (f"{name} 底背离", idx_lo & (x > med)),
            ):
                for r in event_study(lab, sig & m, close):
                    events.append({**r, "group": grp, "split": split})
    th = panel["zweig_thrust"]
    for split, m in (("train", tr), ("test", ~tr)):
        for r in event_study("Zweig 宽度推力", th & m, close):
            events.append({**r, "group": "涨跌比率", "split": split})
    return pd.DataFrame(rows), pd.DataFrame(events)


def select(scores: pd.DataFrame) -> pd.DataFrame:
    """Best variant per group: highest |train IC| averaged over 20d/60d, requiring the test IC
    to keep the same sign (otherwise the next best). Test numbers are reported, not tuned."""
    s = scores[scores.horizon.isin([20, 60])]
    piv = s.pivot_table(index=["group", "variant"], columns="split", values="ic").reset_index()
    piv["score"] = piv["train"].abs()
    piv["stable"] = np.sign(piv["train"]) == np.sign(piv["test"])
    out = []
    for grp, gdf in piv.groupby("group"):
        gdf = gdf.sort_values("score", ascending=False)
        pick = gdf[gdf.stable].head(1)
        if pick.empty:
            pick = gdf.head(1)
        r = pick.iloc[0]
        why = (
            f"训练期|IC|最高({r['train']:+.3f})且测试期同号({r['test']:+.3f})"
            if r.stable
            else f"全部变体测试期翻号，取训练期最高({r['train']:+.3f}/{r['test']:+.3f})，弱"
        )
        out.append(
            dict(
                group=grp,
                variant=r.variant,
                train_ic=float(r["train"]),
                test_ic=float(r["test"]),
                stable=bool(r.stable),
                reason=why,
            )
        )
    return pd.DataFrame(out)
