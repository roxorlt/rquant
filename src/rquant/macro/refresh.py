"""Incremental pull of raw_macro_* into the research market.duckdb, then recompute macro_*.

    python -m rquant.macro refresh [--db PATH] [--import-dir DIR]

Run after the close (rquant-macro.timer); the DuckDB write lock is held only for the short
final write, all network pulls happen before the connection opens.
"""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from rquant.macro import compute

INDEXES = ["000300.SH", "000985.CSI", "000016.SH", "399006.SZ", "000905.SH", "000852.SH"]
HK_ETFS = ["513180.SH", "513130.SH"]
ETFS = [
    "510300.SH",
    "159919.SZ",
    "510310.SH",
    "510330.SH",
    "510050.SH",
    "512100.SH",
    "510500.SH",
    "159915.SZ",
    "588000.SH",
    "588080.SH",
]
UNDERLYINGS = [k.replace("OP", "") for k in compute.OPTION_UNDERLYINGS]
HOLDER_PAT = "|".join(compute.ENTITY_PATTERNS.values())

RAW_TABLES = [
    "holders",
    "qprice",
    "mv",
    "gdp",
    "index",
    "etf_share",
    "opt_basic",
    "opt_daily",
    "und",
    "margin",
    "hsgt",
    "idxbasic",
    "cn10y",
    "hk",
    "hk_etf",
    "fut",
    "fut_holding",
]


def _retry(fn: Callable[..., pd.DataFrame], **kw: Any) -> pd.DataFrame:
    for i in range(5):
        try:
            return fn(**kw)
        except Exception:  # noqa: BLE001 - tushare raises bare Exception on rate limit
            if i == 4:
                raise
            time.sleep(20)
    raise AssertionError


def _paged(fn: Callable[..., pd.DataFrame], page: int, **kw: Any) -> pd.DataFrame:
    out, off = [], 0
    while True:
        d = _retry(fn, offset=off, limit=page, **kw)
        out.append(d)
        if d is None or len(d) < page:
            break
        off += page
    return pd.concat(out, ignore_index=True)


def _last(raw: dict[str, pd.DataFrame], name: str, col: str = "trade_date") -> str:
    d = raw.get(name)
    return "20050101" if d is None or d.empty else str(d[col].max())


def _plus1(d: str) -> str:
    return (pd.Timestamp(d) + timedelta(days=1)).strftime("%Y%m%d")


REPULL_DAYS = 10


def _minus(d: str, days: int) -> str:
    return (pd.Timestamp(d) - timedelta(days=days)).strftime("%Y%m%d")


def _upsert(old: pd.DataFrame | None, new: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if old is None or old.empty:
        return new.reset_index(drop=True)
    if new.empty:
        return old
    return (
        pd.concat([old, new], ignore_index=True)
        .drop_duplicates(keys, keep="last")
        .reset_index(drop=True)
    )


def quarter_ends(start: str, end: str) -> list[str]:
    return [d.strftime("%Y%m%d") for d in pd.date_range(start, end, freq="QE")]


def pull(pro: Any, raw: dict[str, pd.DataFrame], today: str) -> dict[str, pd.DataFrame]:
    """Fetch what is new since the stored max dates. Recent holder periods are always
    re-pulled (disclosures for a period trickle in over ~4 months)."""
    cal = (
        _retry(pro.trade_cal, exchange="SSE", start_date="20050101", end_date=today, is_open="1")
        .cal_date.sort_values()
        .tolist()
    )
    cal = [d for d in cal if d <= today]

    periods = quarter_ends(
        _plus1("20141231")
        if raw.get("holders") is None
        else (pd.Timestamp(today) - timedelta(days=400)).strftime("%Y%m%d"),
        today,
    )
    hs = []
    for p in periods:
        d = _paged(pro.top10_floatholders, 6000, period=p)
        hs.append(d[d.holder_name.str.contains(HOLDER_PAT, na=False)])
    h = pd.concat(hs, ignore_index=True)
    h["entity"] = h.holder_name.map(compute.entity_of)
    h = h[["entity", "ts_code", "ann_date", "end_date", "holder_name", "hold_amount"]]
    old = raw.get("holders")
    if old is not None:
        old = old[~old.end_date.isin(periods)]
    raw["holders"] = _upsert(old, h, ["ts_code", "end_date", "holder_name"])

    have = set() if raw.get("qprice") is None else set(raw["qprice"].end_date)
    qp = []
    for p in sorted(set(raw["holders"].end_date)):
        td = max(d for d in cal if d <= p)
        if p in have and p not in periods:
            continue
        px = _retry(pro.daily, trade_date=td, fields="ts_code,close")
        qp.append(px.assign(end_date=p))
    if qp:
        raw["qprice"] = _upsert(raw.get("qprice"), pd.concat(qp), ["end_date", "ts_code"])

    month_ends = pd.Series(cal).groupby(pd.Series(cal).str[:6]).max().tolist()
    last_mv = _last(raw, "mv")
    want = [d for d in month_ends if d > last_mv]
    if cal[-1] not in want:
        want.append(cal[-1])
    rows = []
    for d in want:
        x = _retry(pro.daily_basic, trade_date=d, fields="ts_code,total_mv,circ_mv")
        rows.append(
            dict(trade_date=d, total_mv=x.total_mv.sum(), circ_mv=x.circ_mv.sum(), n=len(x))
        )
    mv = _upsert(raw.get("mv"), pd.DataFrame(rows), ["trade_date"])
    # keep month-ends plus the single latest day
    raw["mv"] = mv[mv.trade_date.isin(month_ends) | (mv.trade_date == mv.trade_date.max())]

    raw["gdp"] = _retry(pro.cn_gdp)[["quarter", "gdp", "gdp_yoy"]]

    s = _plus1(_last(raw, "index"))
    raw["index"] = _upsert(
        raw.get("index"),
        pd.concat(
            [
                _retry(
                    pro.index_daily,
                    ts_code=c,
                    start_date=s,
                    fields="ts_code,trade_date,close,amount",
                )
                for c in INDEXES
            ]
        ),
        ["ts_code", "trade_date"],
    )
    s = _plus1(_last(raw, "etf_share")) if raw.get("etf_share") is not None else "20230101"
    raw["etf_share"] = _upsert(
        raw.get("etf_share"),
        pd.concat(
            [
                _retry(pro.fund_share, ts_code=c, start_date=s)[
                    ["ts_code", "trade_date", "fd_share"]
                ]
                for c in ETFS
            ]
        ),
        ["ts_code", "trade_date"],
    )

    raw["opt_basic"] = pd.concat(
        [
            _paged(
                pro.opt_basic,
                12000,
                exchange=ex,
                fields="ts_code,name,call_put,exercise_price,maturity_date,opt_code,per_unit",
            )
            for ex in ["SSE", "SZSE"]
        ]
    ).drop_duplicates("ts_code")
    s = _plus1(_last(raw, "opt_daily")) if raw.get("opt_daily") is not None else "20240101"
    od = [
        _retry(
            pro.opt_daily,
            trade_date=d,
            exchange=ex,
            fields="ts_code,trade_date,close,settle,vol,amount,oi",
        )
        for d in cal
        if d >= s
        for ex in ["SSE", "SZSE"]
    ]
    if od:
        raw["opt_daily"] = _upsert(raw.get("opt_daily"), pd.concat(od), ["ts_code", "trade_date"])
    s = _plus1(_last(raw, "und")) if raw.get("und") is not None else "20240101"
    raw["und"] = _upsert(
        raw.get("und"),
        pd.concat(
            [
                _retry(pro.fund_daily, ts_code=c, start_date=s)[["ts_code", "trade_date", "close"]]
                for c in UNDERLYINGS
            ]
        ),
        ["ts_code", "trade_date"],
    )

    s = _plus1(_last(raw, "margin")) if raw.get("margin") is not None else "20100101"
    for name, fn, keys in [
        ("margin", pro.margin, ["trade_date", "exchange_id"]),
        ("hsgt", pro.moneyflow_hsgt, ["trade_date"]),
    ]:
        # re-pull a trailing window: a date fetched before every exchange published is
        # otherwise never revisited (incremental start = max stored date + 1)
        start = _minus(_last(raw, name), REPULL_DAYS) if raw.get(name) is not None else s
        chunks = [
            _retry(fn, start_date=a.strftime("%Y%m%d"), end_date=b.strftime("%Y%m%d"))
            for a, b in _windows(start, today, 180)
        ]
        raw[name] = _upsert(raw.get(name), pd.concat(chunks), keys)
    start = _plus1(_last(raw, "idxbasic")) if raw.get("idxbasic") is not None else "20100101"
    raw["idxbasic"] = _upsert(
        raw.get("idxbasic"),
        pd.concat(
            [
                _retry(
                    pro.index_dailybasic,
                    ts_code="000300.SH",
                    start_date=a.strftime("%Y%m%d"),
                    end_date=b.strftime("%Y%m%d"),
                )
                for a, b in _windows(start, today, 365)
            ]
        ),
        ["trade_date"],
    )[["trade_date", "pe_ttm", "pb"]]
    s = _plus1(_last(raw, "hk")) if raw.get("hk") is not None else "20150101"
    raw["hk"] = _upsert(
        raw.get("hk"),
        pd.concat(
            [
                _retry(
                    pro.index_global,
                    ts_code="HKTECH",
                    start_date=a.strftime("%Y%m%d"),
                    end_date=b.strftime("%Y%m%d"),
                )
                for a, b in _windows(s, today, 1000)
            ]
        )[["ts_code", "trade_date", "close"]],
        ["trade_date"],
    )
    s = _minus(_last(raw, "hk_etf"), REPULL_DAYS) if raw.get("hk_etf") is not None else "20200101"
    raw["hk_etf"] = _upsert(
        raw.get("hk_etf"),
        pd.concat(
            [
                _retry(pro.fund_daily, ts_code=c, start_date=s)[
                    ["ts_code", "trade_date", "close", "vol", "amount"]
                ]
                for c in HK_ETFS
            ]
        ),
        ["ts_code", "trade_date"],
    )
    s = _plus1(_last(raw, "fut")) if raw.get("fut") is not None else "20150101"
    raw["fut"] = _upsert(
        raw.get("fut"),
        pd.concat(
            [
                _retry(
                    pro.fut_daily,
                    ts_code=f"{c}.CFX",
                    start_date=a.strftime("%Y%m%d"),
                    end_date=b.strftime("%Y%m%d"),
                    fields="ts_code,trade_date,close,settle,vol,oi",
                )
                for c in compute.FUT_SPOT
                for a, b in _windows(s, today, 1200)
            ]
        ),
        ["ts_code", "trade_date"],
    )
    s = _plus1(_last(raw, "fut_holding")) if raw.get("fut_holding") is not None else "20230101"
    fh = []
    for d in cal:
        if d < s:
            continue
        h = _retry(pro.fut_holding, trade_date=d, exchange="CFFEX")
        h = h[h.symbol.str.match(r"^(IF|IH|IC|IM)\d")].assign(sym=lambda x: x.symbol.str[:2])
        fh.append(
            h.groupby(["trade_date", "sym"])[["long_hld", "short_hld", "long_chg", "short_chg"]]
            .sum()
            .reset_index()
        )
    if fh:
        raw["fut_holding"] = _upsert(raw.get("fut_holding"), pd.concat(fh), ["trade_date", "sym"])
    import akshare as ak

    y = ak.bond_zh_us_rate(
        start_date=(pd.Timestamp(today) - timedelta(days=60)).strftime("%Y%m%d")
    )[["日期", "中国国债收益率10年"]]
    y.columns = ["date", "cn10y"]
    y["date"] = pd.to_datetime(y.date).dt.strftime("%Y%m%d")
    raw["cn10y"] = _upsert(raw.get("cn10y"), y.dropna(), ["date"])
    return raw


def _windows(start: str, end: str, days: int) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    a, e, out = pd.Timestamp(start), pd.Timestamp(end), []
    while a <= e:
        b = min(a + timedelta(days=days - 1), e)
        out.append((a, b))
        a = b + timedelta(days=1)
    return out


def derive(
    raw: dict[str, pd.DataFrame], bars: pd.DataFrame | None = None
) -> dict[str, pd.DataFrame]:
    prices = raw["qprice"][["end_date", "ts_code", "close"]]
    ix = raw["index"]
    csi = ix[ix.ts_code == "000300.SH"].set_index("trade_date").close.sort_index()
    alla = ix[ix.ts_code == "000985.CSI"].set_index("trade_date").close.sort_index()
    opt = compute.pcr_bands(compute.option_daily(raw["opt_basic"], raw["opt_daily"], raw["und"]))
    hk = compute.hk_system(raw["hk"], raw["hk_etf"], ix)
    hkc = raw["hk"].set_index("trade_date").close.sort_index()
    studies = compute.pcr_studies(opt, csi)
    hk_sig = hk.set_index("trade_date").signal
    studies += compute.event_study("定价权C近似 信号", hk_sig, csi)
    studies += compute.event_study(
        "SMARTMONEY上穿MA20",
        (
            hk.set_index("trade_date").pipe(
                lambda d: (d.smart > d.smart_ma20) & (d.smart.shift() <= d.smart_ma20.shift())
            )
        ),
        csi,
    )
    out = {
        "macro_holder_q": compute.holder_quarterly(raw["holders"], prices),
        "macro_buffett": compute.buffett(raw["mv"], raw["gdp"]),
        "macro_option_daily": opt,
        "macro_daily": compute.daily_panel(
            ix, raw["margin"], raw["hsgt"], raw["idxbasic"], raw["cn10y"]
        ),
        "macro_etf_share": compute.etf_flows(raw["etf_share"]),
        "macro_futures": compute.futures(raw["fut"], raw["fut_holding"], ix),
        "macro_hk": hk,
        "macro_leadlag": compute.lead_lag(hkc, csi),
        "macro_hk_rollcorr": compute.rolling_lag1(hkc, csi),
    }
    if bars is not None and not bars.empty:
        br = compute.breadth(bars, alla)
        out["macro_breadth"] = br
        studies += compute.breadth_studies(br, alla)
        from rquant.macro import breadth as bv

        panel, groups = bv.variants(bars)
        scores, events = bv.evaluate(panel, groups, alla)
        sel = bv.select(scores)
        keep = list(sel.variant)
        out["macro_breadth_select"] = sel
        out["macro_breadth_scores"] = scores
        out["macro_breadth_sel"] = (
            panel[keep]
            .assign(index=alla.reindex(panel.index))
            .rename_axis("trade_date")
            .reset_index()
        )
        studies += events.to_dict("records")
    out["macro_study"] = pd.DataFrame(studies)
    return out


def read_bars(db: Path) -> pd.DataFrame | None:
    import duckdb

    if not db.exists():
        return None
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(
            "SELECT ts_code, strftime(trade_date, '%Y%m%d') AS trade_date, close, pct_chg "
            "FROM daily_bar_hfq WHERE trade_date >= current_date - INTERVAL 6 YEAR"
        ).df()
    except duckdb.Error:
        return None
    finally:
        con.close()


def read_raw(db: Path) -> dict[str, pd.DataFrame]:
    import duckdb

    if not db.exists():
        return {}
    con = duckdb.connect(str(db), read_only=True)
    try:
        have = {
            r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()
        }
        return {
            t: con.execute(f"SELECT * FROM raw_macro_{t}").df()  # noqa: S608
            for t in RAW_TABLES
            if f"raw_macro_{t}" in have
        }
    finally:
        con.close()


def write(db: Path, raw: dict[str, pd.DataFrame], derived: dict[str, pd.DataFrame]) -> None:
    import duckdb

    con = duckdb.connect(str(db))
    try:
        con.execute("BEGIN")
        for name, frame in [*((f"raw_macro_{k}", v) for k, v in raw.items()), *derived.items()]:
            con.register("_f", frame)
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _f")
            con.unregister("_f")
        con.execute("COMMIT")
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    from rquant.research.snapshots import research_root

    ap = argparse.ArgumentParser(prog="python -m rquant.macro")
    ap.add_argument("cmd", choices=["refresh", "derive"])
    ap.add_argument("--db", type=Path, default=None)
    ap.add_argument(
        "--import-dir",
        type=Path,
        default=None,
        help="seed raw tables from <name>.parquet files (one-off backfill)",
    )
    ap.add_argument(
        "--summary-root",
        type=Path,
        default=None,
        help="where macro/daily/*.json goes (default: the db's directory)",
    )
    args = ap.parse_args(argv)
    db = args.db or research_root() / "market.duckdb"
    raw = read_raw(db)
    if args.import_dir:
        for t in RAW_TABLES:
            f = args.import_dir / f"{t}.parquet"
            if f.exists():
                raw[t] = pd.read_parquet(f)
    if args.cmd == "refresh":
        import tushare as ts

        token = os.environ.get("TUSHARE_TOKEN_MAIN") or os.environ.get("TUSHARE_TOKEN", "")
        raw = pull(ts.pro_api(token), raw, date.today().strftime("%Y%m%d"))
    bars = read_bars(db)
    if (
        (bars is None or bars.empty)
        and args.import_dir
        and (args.import_dir / "bars.parquet").exists()
    ):
        bars = pd.read_parquet(args.import_dir / "bars.parquet")
    derived = derive(raw, bars)
    write(db, raw, derived)
    from rquant.macro.summary import build, write_summary

    print(write_summary(args.summary_root or db.parent, build(derived)))
    print({k: len(v) for k, v in derived.items()})
    return 0
