# ruff: noqa: E501 - flag catalog / evidence text read better unwrapped
"""Shared default negative-factor risk filter for all rQuant strategies.

Two layers:
1. ``build_risk_flags(con, pro, day)`` — daily point-in-time job (after close of ``day``): every
   flag only uses information announced on or before ``day``. Stored in ``risk_flags_daily``
   (market.duckdb, research root) keyed by (trade_date, ts_code, flag); decisions for trading
   day D must read flags with ``trade_date < D`` (``load_risk_flags(con, before=D)``).
2. ``apply(df, flags, policy)`` — adds ``risk_flags`` / ``risk_hard`` / ``risk_score`` columns;
   ``filter_hard`` drops hard-excluded rows. ST is the one flag every strategy also needs on
   historical data: use ``st_pit`` (namechange-based, point-in-time), never today's name.

Severity is calibrated by the event study in research/neg_factors/REPORT.md (2025-01..2026-10,
excess returns vs universe median): only flags with robust negative forward returns are HARD;
flags without negative evidence in this sample stay INFO (shown, not scored).
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import duckdb
import pandas as pd

HARD, SOFT, INFO = "hard", "soft", "info"


@dataclass(frozen=True)
class FlagSpec:
    label: str
    severity: str
    source: str
    evidence: str
    valid_days: int = 1  # calendar days a flag stays active after its trade_date


FLAGS: dict[str, FlagSpec] = {
    "st": FlagSpec("ST/*ST/退市整理", HARD, "stock_st + namechange", "新被ST T+1 超额 −4.6%(test t=−9.3)；交易所风险警示", 1),
    "delisting": FlagSpec("退市整理/已终止上市", HARD, "stock_basic(list_status=D) / 名称含退", "不可交易/退市风险", 1),
    "suspended": FlagSpec("停牌", HARD, "suspend_d", "当日不可成交", 1),
    "severe_abnormal": FlagSpec("严重异常波动(已触发/已公告)", HARD, "top_list.reason + 自算", "T+1 −3.6%、5日 −9.4%(test，n=81)；常伴停牌核查", 10),
    "audit_nonstandard": FlagSpec("最近审计意见非标", SOFT, "fina_audit", "仅 T+1 开盘 −0.30%(t=−2.7)；5/20 日无负超额 → 软", 400),
    "forecast_negative": FlagSpec("业绩预告负面(预减/首亏/续亏/略减)", SOFT, "forecast", "T+1 开盘 −0.46%/−0.61%(train/test)，T+1 超额 −0.68%(train)", 5),
    "inst_net_sell": FlagSpec("龙虎榜机构净卖出", SOFT, "top_inst", "全市场 5日 −1.9%/−0.8%、20日 −4.6%/−2.0%(train/test)；涨停股中不成立", 5),
    "abn_triggered": FlagSpec("已触发异常波动(3日偏离)", SOFT, "自算 + top_list.reason", "非短线：20日超额 −3.7%/−1.3%；短线涨停股中为正", 3),
    "near_abn_up": FlagSpec("再一个涨停即触发异常波动", SOFT, "自算(daily+index_daily)", "非短线：20日 −0.7%(train)；监管压制风险", 1),
    "near_severe": FlagSpec("再一个涨停即触发严重异常波动", SOFT, "自算", "未单独回测；严重异动触发后显著负收益 + 停牌核查风险", 1),
    "holder_reduce": FlagSpec("近期股东减持", INFO, "stk_holdertrade(DE)", "本样本未见负超额（+0.3%/+1.0% 5日）", 30),
    "unlock_5d": FlagSpec("5日内限售解禁≥5%流通", INFO, "share_float", "本样本未见负超额", 5),
    "block_discount": FlagSpec("大宗折价≥5%", INFO, "block_trade", "本样本未见负超额", 5),
    "pledge_high": FlagSpec("质押率≥50%", INFO, "pledge_stat", "本样本未见负超额", 10),
    "near_abn_dn": FlagSpec("再一个跌停即触发下跌异动", INFO, "自算", "提示", 1),
}

#: per-strategy overrides of FLAGS severity. "short_term" = 打板/竞价/五虎 (limit-up universe),
#: where abnormal-move and institution-selling flags are NOT negative in the event study.
POLICIES: dict[str, dict[str, str]] = {
    "default": {},
    "short_term": {"abn_triggered": INFO, "near_abn_up": INFO, "inst_net_sell": INFO},
    "long_hold": {"abn_triggered": SOFT, "near_abn_up": SOFT},
}

SOFT_WEIGHT = 1.0

DDL = """
CREATE TABLE IF NOT EXISTS risk_flags_daily (
  trade_date DATE, ts_code VARCHAR, flag VARCHAR, severity VARCHAR, detail VARCHAR,
  source VARCHAR, valid_until DATE, built_at TIMESTAMP DEFAULT current_timestamp,
  PRIMARY KEY (trade_date, ts_code, flag))
"""

MARKET_CODE = "MARKET"  # market-level (calendar) flags use this pseudo code


# ── point-in-time ST (single source of truth for historical filters) ──────────


def st_pit_sql(code_col: str = "ts_code", date_col: str = "trade_date") -> str:
    """SQL predicate TRUE when the stock carried an ST/退 name on that day (namechange PIT)."""
    return (f"EXISTS (SELECT 1 FROM namechange _nc WHERE _nc.ts_code = {code_col} "
            f"AND (_nc.name LIKE '%ST%' OR _nc.name LIKE '%退%') "
            f"AND {date_col} >= _nc.start_date AND {date_col} <= coalesce(_nc.end_date, DATE '9999-12-31'))")


def st_pit(con: duckdb.DuckDBPyConnection, codes: pd.Series, dates: pd.Series) -> pd.Series:
    from rquant.research.pit import is_st

    return is_st(con, codes, dates)


def st_live(names: pd.Series) -> pd.Series:
    """Intraday: ST from the current security name (only valid for *today*)."""
    from rquant.state.derive import _detect_st

    return pd.Series(names).map(_detect_st).astype(bool)


# ── calendar (market-level) ───────────────────────────────────────────────────


def calendar_tags(day: date, trade_days: list[date]) -> list[str]:
    """Calendar-node tags for ``day`` (needs the trading calendar around it)."""
    td = sorted(d for d in trade_days)
    if day not in td:
        return []
    i = td.index(day)
    month_days = [d for d in td if (d.year, d.month) == (day.year, day.month)]
    from_end = len(month_days) - 1 - month_days.index(day)
    tags = []
    if from_end <= 2:
        tags.append("month_end_3d")
        if day.month in (3, 6, 9, 12):
            tags.append("quarter_end_3d")
    nxt = td[i + 1] if i + 1 < len(td) else None
    if nxt is not None and (nxt - day).days >= 4:
        tags.append("pre_holiday_1d")
    elif i + 2 < len(td) and (td[i + 2] - td[i + 1]).days >= 4:
        tags.append("pre_holiday_2d")
    for md in ((4, 30), (8, 31), (10, 31)):
        dl = date(day.year, *md)
        before = [d for d in td if d <= dl][-5:]
        if day in before:
            tags.append("pre_disclosure_deadline")
    return tags


#: market-level soft flags enabled by the calendar study (see REPORT.md §calendar). Empty:
#: no calendar node had a negative effect on short-term outcomes that held in BOTH the
#: 2022-01..2024-06 train and 2024-07..2026-10 test halves (|t|>2), so none is switched on.
CALENDAR_SOFT: dict[str, str] = {}


# ── daily build ───────────────────────────────────────────────────────────────

Pro = Callable[..., pd.DataFrame]


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _safe(pro: Pro, api: str, **kw: Any) -> pd.DataFrame:
    try:
        df = pro(api, **kw)
        return df if df is not None else pd.DataFrame()
    except Exception:  # noqa: BLE001 - one missing source must not kill the whole build
        return pd.DataFrame()


def build_risk_flags(con: duckdb.DuckDBPyConnection, pro: Pro, day: date,
                     trade_days: list[date] | None = None) -> pd.DataFrame:
    """All flags as of the close of ``day`` (PIT). ``con``: research market.duckdb."""
    rows: list[dict] = []

    def add(codes, flag: str, detail: str | pd.Series = "") -> None:
        codes = pd.Series(list(codes), dtype=object)
        det = detail if isinstance(detail, pd.Series) else pd.Series([detail] * len(codes))
        for c, d in zip(codes, det.reset_index(drop=True), strict=False):
            rows.append({"ts_code": str(c), "flag": flag, "detail": str(d)})

    ds = _ymd(day)
    st = _safe(pro, "stock_st", trade_date=ds)
    if not st.empty:
        add(st["ts_code"], "st", st.get("type_name", pd.Series([""] * len(st))))
    else:  # fallback: namechange PIT in market.duckdb
        try:
            cur = con.execute(f"SELECT DISTINCT ts_code FROM namechange WHERE {st_pit_sql('ts_code', '?')}", [day]).fetchall()
        except duckdb.Error:
            cur = []
        add([c for (c,) in cur], "st", "namechange")
    sp = _safe(pro, "suspend_d", trade_date=ds)
    if not sp.empty:
        add(sp.loc[sp["suspend_type"] == "S", "ts_code"], "suspended")
    h = _safe(pro, "stk_holdertrade", start_date=_ymd(day - timedelta(days=30)), end_date=ds)
    if not h.empty:
        h = h[h["in_de"] == "DE"]
        add(h["ts_code"].unique(), "holder_reduce", "近30日减持公告")
    fc = pd.concat([_safe(pro, "forecast", ann_date=_ymd(day - timedelta(days=k))) for k in range(5)])
    if not fc.empty:
        neg = fc[fc["type"].isin(["预减", "首亏", "续亏", "略减"])]
        add(neg["ts_code"], "forecast_negative", neg["type"])
    # per float_date (a date range is capped at 6000 rows and silently truncated)
    sf = pd.concat([_safe(pro, "share_float", float_date=_ymd(day + timedelta(days=k))) for k in range(1, 8)])
    if not sf.empty:
        sf["float_ratio"] = pd.to_numeric(sf["float_ratio"], errors="coerce")
        g = sf.groupby("ts_code")["float_ratio"].sum()
        g = g[g >= 5]
        add(g.index, "unlock_5d", g.map(lambda v: f"解禁 {v:.1f}%"))
    bt = _safe(pro, "block_trade", trade_date=ds)
    if not bt.empty:
        try:
            cl = dict(con.execute("SELECT ts_code, close FROM daily_bar WHERE trade_date = ?", [day]).fetchall())
        except duckdb.Error:
            cl = {}
        bt["c"] = bt["ts_code"].map(cl)
        bt["disc"] = bt["price"] / bt["c"] - 1
        b5 = bt[bt["disc"] <= -0.05].groupby("ts_code")["disc"].min()
        add(b5.index, "block_discount", b5.map(lambda v: f"折价 {v:.1%}"))
    ti = _safe(pro, "top_inst", trade_date=ds)
    if not ti.empty:
        ti = ti[ti["exalter"].astype(str).str.contains("机构专用")]
        net = ti.groupby("ts_code")["net_buy"].sum()
        ns = net[net < 0]
        add(ns.index, "inst_net_sell", ns.map(lambda v: f"机构净卖 {v / 1e4:.0f} 万"))
    tl = _safe(pro, "top_list", trade_date=ds)
    announced_today = set()
    if not tl.empty:
        rs = tl["reason"].astype(str)
        sev = tl[rs.str.contains("严重异常")]
        add(sev["ts_code"].unique(), "severe_abnormal", "交易所公告严重异常波动")
        announced_today = set(tl.loc[rs.str.contains("偏离值累计"), "ts_code"])
    fri = day - timedelta(days=(day.weekday() - 4) % 7)  # pledge_stat is weekly (Friday end_date)
    pl = _safe(pro, "pledge_stat", end_date=_ymd(fri))
    if pl.empty:
        pl = _safe(pro, "pledge_stat", end_date=_ymd(fri - timedelta(days=7)))
    if not pl.empty:
        pl = pl.sort_values("end_date").groupby("ts_code").tail(1)
        hi = pl[pd.to_numeric(pl["pledge_ratio"], errors="coerce") >= 50]
        add(hi["ts_code"], "pledge_high", hi["pledge_ratio"].map(lambda v: f"质押率 {float(v):.0f}%"))
    with contextlib.suppress(Exception):  # abnormal monitor is best effort
        rows += _abnormal_rows(con, pro, day, announced_today)
    for tag in calendar_tags(day, trade_days or []):
        if tag in CALENDAR_SOFT:
            rows.append({"ts_code": MARKET_CODE, "flag": f"cal_{tag}", "detail": CALENDAR_SOFT[tag]})
    df = pd.DataFrame(rows, columns=["ts_code", "flag", "detail"]).drop_duplicates(["ts_code", "flag"])
    df["trade_date"] = day
    df["severity"] = df["flag"].map(lambda f: FLAGS[f].severity if f in FLAGS else SOFT)
    df["source"] = df["flag"].map(lambda f: FLAGS[f].source if f in FLAGS else "calendar")
    df["valid_until"] = df["flag"].map(lambda f: day + timedelta(days=FLAGS[f].valid_days if f in FLAGS else 1))
    return df[["trade_date", "ts_code", "flag", "severity", "detail", "source", "valid_until"]]


def _abnormal_rows(con: duckdb.DuckDBPyConnection, pro: Pro, day: date, announced_today: set[str]) -> list[dict]:
    from rquant.risk import abnormal as ab

    start = day - timedelta(days=60)
    bar = con.execute("SELECT ts_code, trade_date, close, pre_close FROM daily_bar "
                      "WHERE trade_date BETWEEN ? AND ?", [start, day]).df()
    if bar.empty or pd.to_datetime(bar["trade_date"]).max().date() != day:
        return []
    bar["r"] = bar["close"] / bar["pre_close"] - 1
    sr = bar.pivot(index="trade_date", columns="ts_code", values="r").sort_index().tail(31)
    sr.index = pd.to_datetime(sr.index)
    ir = {}
    for ix in ab.BOARD_INDEX.values():
        d = _safe(pro, "index_daily", ts_code=ix, start_date=_ymd(start), end_date=_ymd(day))
        if not d.empty:
            d = d.set_index(pd.to_datetime(d["trade_date"])).sort_index()
            ir[ix] = d["close"] / d["pre_close"] - 1
    irf = pd.DataFrame(ir).reindex(sr.index)
    st = pd.DataFrame(False, index=sr.index, columns=sr.columns)
    try:
        iv = con.execute("SELECT ts_code, start_date, end_date FROM namechange WHERE name LIKE '%ST%' OR name LIKE '%退%'").df()
        for r in iv.itertuples():
            if r.ts_code in st.columns:
                e = pd.Timestamp(r.end_date) if pd.notna(r.end_date) else sr.index[-1]
                st.loc[pd.Timestamp(r.start_date):e, r.ts_code] = True
    except duckdb.Error:
        pass
    ann = pd.DataFrame(False, index=sr.index, columns=sr.columns)
    for c in announced_today & set(sr.columns):
        ann.loc[sr.index[-1], c] = True
    res = ab.compute(sr, irf, st=st, announced=ann)
    lf = ab.latest_flags(res)
    return lf.to_dict("records")


def store(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    con.execute(DDL)
    if df.empty:
        return 0
    days = sorted(set(df["trade_date"]))
    for d in days:
        con.execute("DELETE FROM risk_flags_daily WHERE trade_date = ?", [d])
    con.register("_rf", df)
    con.execute("INSERT INTO risk_flags_daily (trade_date, ts_code, flag, severity, detail, source, valid_until) "
                "SELECT trade_date, ts_code, flag, severity, detail, source, valid_until FROM _rf")
    con.unregister("_rf")
    return len(df)


def load_risk_flags(con: duckdb.DuckDBPyConnection, before: date) -> pd.DataFrame:
    """Flags usable for decisions on trading day ``before`` (built strictly earlier and still valid)."""
    try:
        return con.execute(
            "SELECT * FROM risk_flags_daily WHERE trade_date < ? AND valid_until >= ? "
            "QUALIFY row_number() OVER (PARTITION BY ts_code, flag ORDER BY trade_date DESC) = 1",
            [before, before]).df()
    except duckdb.Error:
        return pd.DataFrame(columns=["trade_date", "ts_code", "flag", "severity", "detail", "source", "valid_until"])


def apply(df: pd.DataFrame, flags: pd.DataFrame, policy: str = "default",
          code_col: str = "ts_code") -> pd.DataFrame:
    """Annotate candidates: risk_flags (list of labels), risk_hard (bool), risk_score (≥0)."""
    out = df.copy()
    if flags is None or flags.empty:
        out["risk_flags"] = [[] for _ in range(len(out))]
        out["risk_hard"] = False
        out["risk_score"] = 0.0
        return out
    over = POLICIES.get(policy, {})
    f = flags.copy()
    f["sev"] = [over.get(fl, s) for fl, s in zip(f["flag"], f["severity"], strict=True)]
    f["label"] = f["flag"].map(lambda x: FLAGS[x].label if x in FLAGS else x)
    g = f.groupby("ts_code")
    labels = g["label"].agg(list)
    hard = g["sev"].agg(lambda s: bool((s == HARD).any()))
    score = g["sev"].agg(lambda s: float((s == SOFT).sum() * SOFT_WEIGHT))
    codes = out[code_col].astype(str)
    out["risk_flags"] = [labels.get(c, []) for c in codes]
    out["risk_hard"] = codes.map(hard).fillna(False).astype(bool).to_numpy()
    out["risk_score"] = codes.map(score).fillna(0.0).astype(float).to_numpy()
    return out


def filter_hard(df: pd.DataFrame, flags: pd.DataFrame, policy: str = "default",
                code_col: str = "ts_code") -> pd.DataFrame:
    a = apply(df, flags, policy, code_col)
    return a[~a["risk_hard"]].reset_index(drop=True)


def market_flags(flags: pd.DataFrame) -> list[str]:
    if flags is None or flags.empty:
        return []
    return flags.loc[flags["ts_code"] == MARKET_CODE, "detail"].tolist()


def run_daily(con: duckdb.DuckDBPyConnection, pro: Pro, day: date) -> int:
    """Entry for the research daily job (``con`` read-write)."""
    try:
        tds = [d for (d,) in con.execute(
            "SELECT trade_date FROM (SELECT DISTINCT trade_date FROM daily_bar WHERE trade_date >= ?) "
            "ORDER BY 1", [day - timedelta(days=40)]).fetchall()]
    except duckdb.Error:
        tds = []
    cal = _safe(pro, "trade_cal", exchange="SSE", start_date=_ymd(day - timedelta(days=40)),
                end_date=_ymd(day + timedelta(days=20)), is_open="1")
    if not cal.empty:
        tds = sorted(pd.to_datetime(cal["cal_date"]).dt.date)
    return store(con, build_risk_flags(con, pro, day, tds))


__all__ = ["FLAGS", "HARD", "INFO", "POLICIES", "SOFT", "apply", "build_risk_flags", "calendar_tags",
           "filter_hard", "load_risk_flags", "market_flags", "run_daily", "st_live", "st_pit",
           "st_pit_sql", "store"]


def main(argv: list[str] | None = None) -> int:
    """python -m rquant.risk.neg_filter build|show [--day YYYY-MM-DD] (research market.duckdb)."""
    import argparse
    import os

    from rquant.research import ingest as ing

    p = argparse.ArgumentParser(prog="python -m rquant.risk.neg_filter")
    p.add_argument("cmd", choices=["build", "show"])
    p.add_argument("--day", type=date.fromisoformat, default=None)
    a = p.parse_args(argv)
    from rquant.backtest.store import research_root

    root = research_root()
    if a.cmd == "show":
        con = duckdb.connect(str(ing.db_path(root)), read_only=True)
        try:
            day = a.day or date.today() + timedelta(days=1)
            print(load_risk_flags(con, before=day).to_string())
        finally:
            con.close()
        return 0
    con = ing.connect(root)
    try:
        day = a.day or con.execute("SELECT MAX(trade_date) FROM daily_bar").fetchone()[0]
        pro = ing.Pro(os.environ.get("TUSHARE_TOKEN_MAIN") or os.environ["TUSHARE_TOKEN"])
        print(f"risk_flags_daily {day}: {run_daily(con, pro, day)} rows")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
