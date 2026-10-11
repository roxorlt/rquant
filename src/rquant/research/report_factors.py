"""研报因子: replicate sell-side (金工) factors on our daily + 5-min data.

    python -m rquant.research.report_factors run            # on the server, off-hours
    python -m rquant.research.report_factors run --min-parquet m.parquet --daily-parquet d.parquet

Catalog: ``research/factor_reports/catalog.json`` (in repo). Output under
``$RQUANT_RESEARCH_ROOT/report_factors/``: ``results.json`` (metrics + chart
series per factor) and ``latest.json`` (validated factors' cross-sectional
percentiles on the last day, merged into the screener's factor values).

Method: rebalance every 10 trading days (enter at close), forward 10d
hfq return, universe = non-ST, non-BJ, listed >=120d, not limit-up at entry.
Direction is fixed on the train window (first 60%), metrics reported on train
and test; cost 0.2% per unit of one-way turnover (commission+stamp+slippage).
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

KIND = "report_factors"
HOLD = 10
COST = 0.002
TRAIN_FRAC = 0.6
CATALOG = Path(__file__).resolve().parents[3] / "research" / "factor_reports" / "catalog.json"

MINUTE_SQL = """
with b as (
 select ts_code, trade_date, trade_time, close, vol, amount,
  close/lag(close) over (partition by ts_code, trade_date order by trade_time)-1 as r,
  hour(trade_time)*60+minute(trade_time) as hm
 from minute_bar where freq='5min' and vol>0 {where}),
s as (select *, abs(r)/sqrt(vol) as sm from b where r is not null),
k as (select *, sum(vol) over (partition by ts_code, trade_date order by sm desc
        rows unbounded preceding) as cv,
      sum(vol) over (partition by ts_code, trade_date) as tv from s)
select ts_code, trade_date, count(*) n,
 sum(r*r) rv, sum(case when r<0 then r*r else 0 end) dvar,
 skewness(r) skew, corr(close,vol) corr_pv, stddev(vol)/avg(vol) vol_cv,
 sum(case when hm>=870 then r else 0 end) r_tail30,
 sum(case when hm<=690 then r else 0 end) r_am,
 sum(case when hm>=780 then r else 0 end) r_pm,
 sum(case when cv-vol < 0.2*tv then amount else 0 end) sm_amt,
 sum(case when cv-vol < 0.2*tv then vol else 0 end) sm_vol,
 sum(amount) amt, sum(vol) v
from k group by 1,2
"""

DAILY_SQL = """
select h.ts_code, h.trade_date, h.high, h.low, h.close, h.pct_chg, h.amount,
 b.circ_mv, s.limit_pct,
 m.buy_lg_amount+m.buy_elg_amount-m.sell_lg_amount-m.sell_elg_amount as big_net
from daily_bar_hfq h left join daily_basic b using(ts_code,trade_date)
 left join moneyflow m using(ts_code,trade_date) left join daily_state s using(ts_code,trade_date)
{where}
"""


def research_root() -> Path:
    return Path(os.environ.get("RQUANT_RESEARCH_ROOT", Path.home() / "rquant-research"))


# ---------------------------------------------------------------- factors

def _p(df: pd.DataFrame, col: str) -> pd.DataFrame:
    return df.pivot(index="trade_date", columns="ts_code", values=col).sort_index()


def _ideal_amp(high: pd.DataFrame, low: pd.DataFrame, close: pd.DataFrame,
               n: int = 20, lam: float = 0.25) -> pd.DataFrame:
    amp = (high / low - 1).to_numpy()
    c = close.to_numpy()
    out = np.full(amp.shape, np.nan)
    k = max(1, int(round(n * lam)))
    for t in range(n - 1, len(c)):
        cw, aw = c[t - n + 1:t + 1], amp[t - n + 1:t + 1]
        ok = ~np.isnan(cw).any(0) & ~np.isnan(aw).any(0)
        order = np.argsort(cw[:, ok], axis=0)
        a = np.take_along_axis(aw[:, ok], order, axis=0)
        out[t, ok] = a[-k:].mean(0) - a[:k].mean(0)
    return pd.DataFrame(out, index=close.index, columns=close.columns)


def minute_factors(m: pd.DataFrame) -> dict[str, pd.DataFrame]:
    p = {c: _p(m, c) for c in ("rv", "dvar", "skew", "corr_pv", "vol_cv", "r_tail30",
                               "r_am", "r_pm", "sm_amt", "sm_vol", "amt", "v", "n")}
    r10 = lambda x: x.rolling(10, min_periods=8).sum()  # noqa: E731
    m20 = lambda x: x.rolling(20, min_periods=15).mean()  # noqa: E731
    smart = (r10(p["sm_amt"]) / r10(p["sm_vol"])) / (r10(p["amt"]) / r10(p["v"]))
    vstd = p["vol_cv"] * p["v"] / p["n"]
    am = p["r_am"].sub(p["r_am"].mean(axis=1), axis=0)
    pm = p["r_pm"].sub(p["r_pm"].mean(axis=1), axis=0)
    d = am - pm
    return {
        "ky_smart_money": smart,
        "dw_cpv": m20(p["corr_pv"]),
        "dw_utd": vstd.rolling(20, min_periods=15).std() / m20(vstd),
        "ht_skew": m20(p["skew"]),
        "ht_downvol": m20(p["dvar"] / p["rv"].where(p["rv"] > 0)),
        "hf_rvol": m20(np.sqrt(p["rv"])),
        "gj_apm": m20(d) / d.rolling(20, min_periods=15).std(),
        "tail_return": m20(p["r_tail30"]),
    }


def daily_factors(d: pd.DataFrame) -> dict[str, pd.DataFrame]:
    hi, lo, cl = _p(d, "high"), _p(d, "low"), _p(d, "close")
    net = _p(d, "big_net") * 10 / _p(d, "amount").where(lambda x: x > 0)  # 万元 vs 千元
    return {"ky_ideal_amp": _ideal_amp(hi, lo, cl),
            "ht_bigorder_net": net.rolling(20, min_periods=15).mean()}


META: dict[str, dict[str, str]] = {
    "ky_smart_money": {"name": "聪明钱Q(开源)", "data": "5min", "catalog": "ky_smart_money"},
    "dw_cpv": {"name": "价量相关CPV(东吴)", "data": "5min", "catalog": "dw_cpv"},
    "dw_utd": {"name": "换手分布均匀度UTD(东吴)", "data": "5min", "catalog": "dw_utd"},
    "ht_skew": {"name": "高频偏度(海通)", "data": "5min", "catalog": "ht_skew"},
    "ht_downvol": {"name": "下行波动占比(海通)", "data": "5min", "catalog": "ht_downvol"},
    "hf_rvol": {"name": "高频已实现波动", "data": "5min", "catalog": "ht_downvol"},
    "gj_apm": {"name": "APM上下午差(国君,近似)", "data": "5min", "catalog": "gj_apm"},
    "tail_return": {"name": "尾盘30分收益", "data": "5min", "catalog": "tail_return"},
    "ky_ideal_amp": {"name": "理想振幅(开源)", "data": "daily", "catalog": "ky_ideal_amp"},
    "ht_bigorder_net": {"name": "大单净买入占比(资金流)", "data": "daily",
                        "catalog": "ht_bigorder_net"},
}


# ---------------------------------------------------------------- evaluation

def _rank_corr(f: pd.DataFrame, r: pd.DataFrame) -> pd.Series:
    return f.rank(axis=1).corrwith(r.rank(axis=1), axis=1)


def _neutral(f: pd.DataFrame, size: pd.DataFrame) -> pd.DataFrame:
    """Residual of cross-sectional rank(f) on rank(log size), per date."""
    fr, sr = f.rank(axis=1, pct=True), size.reindex_like(f).rank(axis=1, pct=True)
    sr = sr.where(fr.notna())
    fc, sc = fr.sub(fr.mean(axis=1), axis=0), sr.sub(sr.mean(axis=1), axis=0)
    beta = (fc * sc).sum(axis=1) / (sc * sc).sum(axis=1)
    return fc - sc.mul(beta, axis=0)


def _stats(ic: pd.Series, per_year: float) -> dict[str, float | None]:
    ic = ic.dropna()
    if len(ic) < 3:
        return {"mean": None, "ir": None, "win": None, "n": len(ic)}
    sd = ic.std()
    return {"mean": round(float(ic.mean()), 4),
            "ir": round(float(ic.mean() / sd * np.sqrt(per_year)), 2) if sd > 0 else None,
            "win": round(float((ic > 0).mean()), 3), "n": int(len(ic))}


def evaluate(f: pd.DataFrame, close: pd.DataFrame, tradable: pd.DataFrame,
             size: pd.DataFrame, direction_hint: int | None = None) -> dict:
    f = f.reindex(index=close.index, columns=close.columns)
    fwd = {h: close.shift(-h) / close - 1 for h in sorted({1, 5, 10, 20, HOLD})}
    dates = close.index[::HOLD]
    dates = dates[dates.isin(f.dropna(how="all").index)]
    dates = dates[:-1]  # last one has no forward return
    fx = f.loc[dates].where(tradable.loc[dates])
    ret = fwd[HOLD].loc[dates]
    fx = fx.where(ret.notna())
    rank_ic = _rank_corr(fx, ret)
    ic = fx.corrwith(ret, axis=1)
    split = dates[int(len(dates) * TRAIN_FRAC)]
    train, test = rank_ic.index < split, rank_ic.index >= split
    tr_mean = rank_ic[train].mean()
    sign = int(np.sign(tr_mean)) if tr_mean == tr_mean and tr_mean != 0 else (direction_hint or 1)
    per_year = 252 / HOLD
    # quintiles (sign-adjusted so Q5 = expected best)
    q = (fx * sign).rank(axis=1, pct=True).mul(5).apply(np.ceil).clip(1, 5)
    grp = {k: ret.where(q == k).mean(axis=1) for k in range(1, 6)}
    univ = ret.where(fx.notna()).mean(axis=1)
    top = (q == 5).astype(float)
    top_w = top.div(top.sum(axis=1), axis=0)
    bot = (q == 1).astype(float)
    bot_w = bot.div(bot.sum(axis=1), axis=0)
    turn_top = (top_w - top_w.shift(1)).abs().sum(axis=1) / 2
    turn_bot = (bot_w - bot_w.shift(1)).abs().sum(axis=1) / 2
    turn_top.iloc[0] = turn_bot.iloc[0] = 1.0
    ls_net = grp[5] - grp[1] - (turn_top + turn_bot) * COST * 2
    long_ex_net = grp[5] - univ - turn_top * COST * 2
    ls_gross, long_ex_gross = grp[5] - grp[1], grp[5] - univ

    def ann(x: pd.Series) -> float | None:
        x = x.dropna()
        return round(float((1 + x).prod() ** (per_year / len(x)) - 1), 4) if len(x) else None

    def sharpe(x: pd.Series) -> float | None:
        x = x.dropna()
        if len(x) <= 2 or not x.std() > 0:
            return None
        return round(float(x.mean() / x.std() * np.sqrt(per_year)), 2)

    def block(mask: np.ndarray) -> dict:
        return {"rank_ic": _stats(rank_ic[mask] * sign, per_year),
                "ic": _stats(ic[mask] * sign, per_year),
                "ls_ann_gross": ann(ls_gross[mask]),
                "long_excess_ann_gross": ann(long_ex_gross[mask]),
                "ls_ann_net": ann(ls_net[mask]), "ls_sharpe_net": sharpe(ls_net[mask]),
                "long_excess_ann_net": ann(long_ex_net[mask]),
                "turnover_top": (round(float(turn_top[mask].iloc[1:].mean()), 3)
                                 if mask.sum() > 1 else None),
                "quintile_ann": [ann(grp[k][mask]) for k in range(1, 6)]}

    neu = _neutral(fx, size.loc[dates])
    decay = {str(h): round(float(_rank_corr(fx, fwd[h].loc[dates]).mean() * sign), 4) for h in fwd}
    res = {"sign": sign, "split": str(split.date()), "train": block(np.asarray(train)),
           "test": block(np.asarray(test)),
           "size_neutral_rank_ic_test": round(float(_rank_corr(neu, ret)[test].mean() * sign), 4),
           "horizon_rank_ic": decay}
    tr, te = res["train"]["rank_ic"]["mean"] or 0, res["test"]["rank_ic"]["mean"] or 0
    res["decay"] = bool(te < 0.5 * tr)
    res["validated"] = bool(te >= 0.02 and (res["test"]["rank_ic"]["ir"] or 0) >= 1.0
                            and (res["test"]["long_excess_ann_net"] or 0) > 0)
    nav = lambda x: (1 + x.fillna(0)).cumprod().round(4).tolist()  # noqa: E731
    res["series"] = {"dates": [str(d.date()) for d in dates],
                     "rank_ic": (rank_ic * sign).round(4).fillna(0).tolist(),
                     "q_nav": [nav(grp[k] - univ) for k in range(1, 6)],
                     "ls_nav_net": nav(ls_net), "long_excess_nav_net": nav(long_ex_net)}
    return res


def universe(d: pd.DataFrame, basic: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame,
                                                             pd.DataFrame]:
    if "pct_chg" not in d:
        d = d.assign(pct_chg=(d["close"] / d["pre_close"] - 1) * 100)
    close, pct, lim = _p(d, "close"), _p(d, "pct_chg"), _p(d, "limit_pct")
    size = np.log(_p(d, "circ_mv"))
    b = basic.set_index("ts_code")
    st = b["name"].fillna("").str.contains("ST")
    listed = pd.to_datetime(b["list_date"], errors="coerce")
    ok_code = ~st & ~b.index.str.endswith(".BJ")
    cols = close.columns
    ok_code = ok_code.reindex(cols).fillna(False).to_numpy()
    age = close.index.to_numpy()[:, None] - listed.reindex(cols).to_numpy()[None, :]
    seasoned = age >= np.timedelta64(180, "D")
    limit = lim.fillna(10).reindex_like(pct)
    not_up = ~(pct >= limit - 0.2)
    tradable = (close.notna() & not_up & seasoned) & ok_code[None, :]
    return close, tradable, size


def run(minute: pd.DataFrame, daily: pd.DataFrame, basic: pd.DataFrame) -> dict:
    daily = daily.assign(trade_date=pd.to_datetime(daily["trade_date"]))
    minute = minute.assign(trade_date=pd.to_datetime(minute["trade_date"]))
    close, tradable, size = universe(daily, basic)
    out: dict = {"generated_at": datetime.now(UTC).isoformat(), "hold_days": HOLD,
                 "cost_per_side": COST, "factors": {}}
    latest: dict[str, dict[str, float]] = {}
    jobs: list[tuple[Callable[[], dict[str, pd.DataFrame]], pd.DatetimeIndex]] = [
        (lambda: minute_factors(minute), pd.DatetimeIndex(sorted(minute["trade_date"].unique()))),
        (lambda: daily_factors(daily), close.index)]
    for build, idx in jobs:
        facs = build()
        c, t, s = close.loc[idx[0]:idx[-1]], tradable.loc[idx[0]:idx[-1]], size.loc[idx[0]:idx[-1]]
        for key, f in facs.items():
            r = evaluate(f, c, t, s)
            r.update(META[key])
            out["factors"][key] = r
            last = f.dropna(how="all").index[-1]
            row = (f.loc[last] * r["sign"]).where(tradable.loc[last]).dropna()
            r["latest_date"] = str(last.date())
            if r["validated"]:
                latest[f"研报·{META[key]['name']}"] = {
                    k: round(float(v), 2) for k, v in (row.rank(pct=True) * 100).items()}
            print(key, r["test"]["rank_ic"], r["validated"], flush=True)
    corr_keys = list(out["factors"])
    out["catalog"] = json.loads(CATALOG.read_text()) if CATALOG.exists() else []
    out["order"] = corr_keys
    out["latest"] = {"factors": list(latest)}
    return {"results": out, "latest": latest}


def write(doc: dict, root: Path | None = None) -> Path:
    folder = (root or research_root()) / KIND
    folder.mkdir(parents=True, exist_ok=True)
    for name, body in (("results.json", doc["results"]), ("latest.json", doc["latest"])):
        tmp = folder / f"{name}.tmp"
        tmp.write_text(json.dumps(body, ensure_ascii=False, allow_nan=False, default=str))
        tmp.replace(folder / name)
    return folder


def read_results(root: Path | None = None) -> dict | None:
    p = (root or research_root()) / KIND / "results.json"
    return json.loads(p.read_text()) if p.exists() else None


def read_latest(root: Path | None = None) -> dict[str, dict[str, float]]:
    p = (root or research_root()) / KIND / "latest.json"
    try:
        return json.loads(p.read_text()) if p.exists() else {}
    except ValueError:
        return {}


def _load_db(database: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    import duckdb
    con = duckdb.connect(str(database), read_only=True)
    con.execute("set threads=2; set memory_limit='3GB'; set preserve_insertion_order=false")
    try:
        minute = con.execute(MINUTE_SQL.format(where="")).df()
        daily = con.execute(DAILY_SQL.format(where="")).df()
        basic = con.execute("select ts_code, name, list_date from stock_basic").df()
    finally:
        con.close()
    return minute, daily, basic


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="rquant.research.report_factors")
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--database", type=Path, default=research_root() / "market.duckdb")
    ap.add_argument("--min-parquet")
    ap.add_argument("--daily-parquet")
    ap.add_argument("--basic-parquet")
    ap.add_argument("--out", type=Path)
    a = ap.parse_args(argv)
    if a.min_parquet:
        minute, daily, basic = (pd.read_parquet(x) for x in
                                (a.min_parquet, a.daily_parquet, a.basic_parquet))
    else:
        minute, daily, basic = _load_db(a.database)
    print("wrote", write(run(minute, daily, basic), a.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
