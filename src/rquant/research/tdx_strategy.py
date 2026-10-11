"""TDX formula strategies: evaluate a formula over the whole research history, store its
hits as a screen_result preset (so the normal backtest/strategy plumbing applies), and run
an event study (next-open entry, N-day forward returns vs benchmarks).

Caveats (also in the saved study): the ST filter uses today's name (survivorship/look-ahead
on ST status); volume is not split-adjusted (送转 days can distort volume rules); the index
leg uses signal-day close as the entry proxy since index_daily has no open.
"""

from __future__ import annotations

import json
import logging
import statistics
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd

log = logging.getLogger("rquant.research")

HJK = ("AA:=MA(C,5); BB:=MA(C,10); CC:=MA(C,30); DD:=REF(HHV(H,20),15); "
       "EE:=LLV(V,12)<MA(V,20)*0.6; FF:=V>MA(V,20)*1.2; GG:=CROSS(C,DD); HH:=CC>REF(CC,1); "
       "II:=CROSS(AA,BB); XG:EE AND FF AND GG AND HH AND II;")

LYT = ("AA:=MA(C,5); BB:=MA(C,10); CC:=MA(C,20); DD:=MA(C,60); "
       "EE:=EMA(CLOSE,12)-EMA(CLOSE,26); FF:=EMA(EE,9); GG:=CROSS(AA,BB); "
       "HH:=EE>0 AND FF>0 AND CROSS(EE,FF); II:=V>MA(V,20)*1.2; JJ:=REF(HHV(C,20),1); "
       "KK:=C>JJ; LL:=LLV(L,15)>DD*0.97; MM:=LLV(V,15)<MA(V,20)*0.7; "
       "NN:=AA>REF(AA,1) AND BB>REF(BB,1) AND CC>REF(CC,1) AND DD>REF(DD,1); "
       "XG:GG AND HH AND II AND KK AND LL AND MM AND NN;")

#: 量比>=3 + 内盘>外盘 + 大单净额>10% 成交额（大单吸收散户卖盘的背离）+ 站上 20 日线 + 5 日涨幅<=10%.
#: Best of a threshold scan (train 2024-05~2025-09); out-of-sample 2025-10~2026-10 is ~flat after costs.
VRDIV = ("VR:=V/REF(MA(V,5),1); XG:VR>=3 AND NWP>1 AND DDJE>0.1 AND C>=MA(C,20) "
         "AND C<=REF(C,5)*1.1;")

#: slug, title, condition name, formula, max positions, hold days
TDX_STRATEGIES = [("hjk-hold10", "黄金坑 持有 10 日", "黄金坑", HJK, 10, 10),
                  ("lyt-hold10", "老鸭头 持有 10 日", "老鸭头", LYT, 10, 10),
                  ("vrdiv-hold5", "量比+内外盘背离 持有 5 日", "量比+内外盘背离", VRDIV, 10, 5)]
HORIZONS = (5, 10, 20)
BENCH = {"沪深300": "000300.SH", "中证1000": "000852.SH"}


def preset_name(slug: str) -> str:
    return f"tdx_{slug}"


def hits(con: duckdb.DuckDBPyConnection, formula: str, start: date, end: date) -> pd.DataFrame:
    """All (trade_date, ts_code) where the formula fires, ST excluded (by current name)."""
    from rquant.factor.expr import evaluate
    from rquant.factor.store import load_panel
    from rquant.factor.tdx import translate

    panel = load_panel(con, start, end, table="daily_bar_hfq")
    out = evaluate(translate(formula), panel)
    out = out.loc[(out.index >= pd.Timestamp(start)) & (out.index <= pd.Timestamp(end))]
    st = {c for (c,) in con.execute("SELECT ts_code FROM stock_basic WHERE name LIKE '%ST%'")
          .fetchall()}
    s = out.stack()
    s = s[s > 0.5]
    df = s.reset_index()[["trade_date", "ts_code"]]
    df = df[~df["ts_code"].isin(st)]
    amount = panel["amount"].stack().rename("amount").reset_index()
    return df.merge(amount, on=["trade_date", "ts_code"], how="left")


def store_hits(con: duckdb.DuckDBPyConnection, preset: str, df: pd.DataFrame) -> int:
    con.execute("DELETE FROM screen_result WHERE preset_name = ?", [preset])
    if df.empty:
        return 0
    rows = df.assign(trade_date=df["trade_date"].dt.date, preset_name=preset,
                     extra=[json.dumps({"score": round(float(a or 0) / 1e4, 1)})
                            for a in df["amount"].fillna(0)])
    con.register("_hits", rows[["trade_date", "ts_code", "preset_name", "extra"]])
    con.execute("""INSERT INTO screen_result
      SELECT h.trade_date, h.ts_code, h.preset_name, s.name, b.close, b.pct_chg, h.extra
      FROM _hits h LEFT JOIN stock_basic s USING (ts_code)
      LEFT JOIN daily_bar b ON b.ts_code = h.ts_code AND b.trade_date = h.trade_date""")
    con.unregister("_hits")
    return len(rows)


def event_study(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> dict:
    if df.empty:
        return {"signals": 0}
    start = df["trade_date"].min().date()
    px = con.execute("SELECT trade_date, ts_code, open, close FROM daily_bar_qfq "
                     "WHERE trade_date >= ? AND ts_code IN (SELECT UNNEST(?))",
                     [start, sorted(df["ts_code"].unique())]).df()
    px["trade_date"] = pd.to_datetime(px["trade_date"])
    opens = px.pivot(index="trade_date", columns="ts_code", values="open").sort_index()
    closes = px.pivot(index="trade_date", columns="ts_code", values="close").reindex(opens.index)
    days = list(opens.index)
    pos = {d: i for i, d in enumerate(days)}
    idx = con.execute("SELECT trade_date, ts_code, close FROM index_daily WHERE trade_date >= ?",
                      [start]).df()
    idx["trade_date"] = pd.to_datetime(idx["trade_date"])
    bench = idx.pivot(index="trade_date", columns="ts_code", values="close").reindex(opens.index)
    out: dict = {"signals": len(df),
                 "per_year": {str(y): int(n) for y, n in
                              df.groupby(df["trade_date"].dt.year).size().items()},
                 "horizons": {}}
    for n in HORIZONS:
        r, ex = [], {k: [] for k in BENCH}
        for d, code in zip(df["trade_date"], df["ts_code"], strict=True):
            i = pos.get(d)
            if i is None or i + n >= len(days):
                continue
            o, c = opens[code].iat[i + 1], closes[code].iat[i + n]
            if pd.isna(o) or pd.isna(c) or o <= 0:
                continue  # suspended at entry/exit: skipped (a caveat, not a fill model)
            ret = c / o - 1
            r.append(ret)
            for name, b in BENCH.items():
                if b in bench:
                    b0, b1 = bench[b].iat[i], bench[b].iat[i + n]
                    if pd.notna(b0) and pd.notna(b1):
                        ex[name].append(ret - (b1 / b0 - 1))
        if not r:
            continue
        out["horizons"][str(n)] = {
            "n": len(r), "mean": statistics.fmean(r), "median": statistics.median(r),
            "win_rate": sum(x > 0 for x in r) / len(r),
            "excess_mean": {k: statistics.fmean(v) for k, v in ex.items() if v},
            "excess_win_rate": {k: sum(x > 0 for x in v) / len(v) for k, v in ex.items() if v}}
    return out


def study_path(root: Path, slug: str) -> Path:
    return root / "event_study" / f"{slug}.json"


def run(root: Path, db: Path, first: date, last: date) -> list[tuple]:
    """Rebuild every TDX preset over the full history; returns the strategies to backtest."""
    start = first + timedelta(days=60)
    con = duckdb.connect(str(db))
    try:
        for slug, title, _name, formula, _n, _hold in TDX_STRATEGIES:
            df = hits(con, formula, start, last)
            log.info("tdx %s: %d hits", slug, store_hits(con, preset_name(slug), df))
            study = event_study(con, df) | {
                "slug": slug, "title": title, "formula": formula,
                "window": [str(start), str(last)], "generated_at": datetime.now(UTC).isoformat(),
                "caveats": ["ST 用当前名称过滤（存在前视/幸存者偏差）",
                            "成交量未复权，送转日可能影响量能条件",
                            "指数超额以信号日收盘为基准（index_daily 无开盘价）",
                            "进出场日停牌的样本被跳过"]}
            p = study_path(root, slug)
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(".tmp")
            tmp.write_text(json.dumps(study, ensure_ascii=False, indent=1))
            tmp.replace(p)
    finally:
        con.close()
    return TDX_STRATEGIES
