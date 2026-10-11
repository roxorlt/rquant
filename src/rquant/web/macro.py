"""宏观页 API: reads the small macro_* tables from the research market.duckdb (read-only)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel


class HolderQuarter(BaseModel):
    entity: str
    basis: str
    quarter: str
    n_hold: int
    n_new: int | None
    n_exit: int | None
    n_add: int | None
    n_cut: int | None
    share_chg: float | None
    mv: float | None
    flow: float | None


class Bands(BaseModel):
    """Full-history percentiles / mean±σ (in-sample, descriptive only)."""

    p20: float
    p80: float
    mean: float
    std: float
    current: float
    current_pct: float
    current_date: str


class Columns(BaseModel):
    dates: list[str]
    cols: dict[str, list[float | None]]


class OptionLatest(BaseModel):
    und: str
    trade_date: str
    pcr_vol: float | None
    pcr_oi: float | None
    vol_surge: float | None
    oi_surge: float | None
    atm_iv: float | None
    atm_iv_pct: float | None


class StudyRow(BaseModel):
    study: str
    horizon: int
    n: int
    mean: float | None
    median: float | None
    win: float | None
    base_mean: float | None
    base_win: float | None


class MacroData(BaseModel):
    available: bool
    holders: list[HolderQuarter] = []
    index_m: Columns | None = None
    buffett: Columns | None = None
    buffett_bands: Bands | None = None
    daily: Columns | None = None
    erp_bands: Bands | None = None
    north_regime_change: str = "2024-08-19"
    options: dict[str, Columns] = {}
    options_latest: list[OptionLatest] = []
    etf_share: Columns | None = None
    breadth: Columns | None = None
    hk: Columns | None = None
    leadlag: Columns | None = None
    hk_rollcorr: Columns | None = None
    futures: dict[str, Columns] = {}
    studies: list[StudyRow] = []


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(f) else round(f, 6)


def _cols(df: pd.DataFrame, date_col: str, cols: list[str]) -> Columns:
    return Columns(
        dates=[str(d)[:10].replace("-", "") for d in df[date_col]],
        cols={c: [_num(v) for v in df[c]] for c in cols if c in df},
    )


def _bands(s: pd.Series, dates: pd.Series) -> Bands | None:
    s = s.dropna()
    if s.empty:
        return None
    cur = float(s.iloc[-1])
    return Bands(
        p20=float(s.quantile(0.2)),
        p80=float(s.quantile(0.8)),
        mean=float(s.mean()),
        std=float(s.std()),
        current=cur,
        current_pct=float((s <= cur).mean()),
        current_date=str(dates.loc[s.index[-1]])[:10].replace("-", ""),
    )


def load_macro(research: Path) -> MacroData:
    import duckdb

    db = research / "market.duckdb"
    if not db.exists():
        return MacroData(available=False)
    try:
        con = duckdb.connect(str(db), read_only=True)
    except duckdb.Error:
        return MacroData(available=False)
    try:
        q = lambda sql: con.execute(sql).df()  # noqa: E731
        tables = set(q("SELECT table_name FROM information_schema.tables").table_name)
        if "macro_daily" not in tables:
            return MacroData(available=False)
        h = q(
            "SELECT * FROM macro_holder_q WHERE quarter >= '2015Q2' ORDER BY basis, entity, quarter"
        )
        b = q("SELECT * FROM macro_buffett ORDER BY trade_date")
        d = q("SELECT * FROM macro_daily WHERE trade_date >= '20150101' ORDER BY trade_date")
        o = q("SELECT * FROM macro_option_daily ORDER BY und, trade_date")
        e = q("SELECT * FROM macro_etf_share ORDER BY trade_date")
        opt = lambda t, sql: q(sql) if t in tables else pd.DataFrame()  # noqa: E731
        br = opt("macro_breadth", "SELECT * FROM macro_breadth ORDER BY trade_date")
        hk = opt(
            "macro_hk", "SELECT * FROM macro_hk WHERE trade_date >= '20200101' ORDER BY trade_date"
        )
        ll = opt("macro_leadlag", "SELECT * FROM macro_leadlag ORDER BY lag")
        rc = opt("macro_hk_rollcorr", "SELECT * FROM macro_hk_rollcorr ORDER BY trade_date")
        fu = opt(
            "macro_futures",
            "SELECT * FROM macro_futures WHERE trade_date >= '20200101' ORDER BY sym, trade_date",
        )
        st = opt("macro_study", "SELECT * FROM macro_study")
    finally:
        con.close()
    h = h.astype(object).where(h.notna(), None)
    holders = [
        HolderQuarter(**{k: r[k] for k in HolderQuarter.model_fields}) for r in h.to_dict("records")
    ]
    dd = d.copy()
    dd["m"] = dd.trade_date.str[:6]
    im = dd.groupby("m").tail(1)
    options: dict[str, Columns] = {}
    latest: list[OptionLatest] = []
    for und, g in o.groupby("und"):
        g = g.reset_index(drop=True)
        options[str(und)] = _cols(
            g, "trade_date", ["pcr_vol", "pcr_oi", "atm_iv", "vol_surge", "oi_surge", "spot"]
        )
        r = g.iloc[-1]
        iv = g.atm_iv.dropna()
        latest.append(
            OptionLatest(
                und=str(und),
                trade_date=str(r.trade_date),
                pcr_vol=_num(r.pcr_vol),
                pcr_oi=_num(r.pcr_oi),
                vol_surge=_num(r.vol_surge),
                oi_surge=_num(r.oi_surge),
                atm_iv=_num(r.atm_iv),
                atm_iv_pct=float((iv <= iv.iloc[-1]).mean()) if len(iv) else None,
            )
        )
    ep = e.pivot_table(index="trade_date", columns="ts_code", values="fd_share").reset_index()
    return MacroData(
        available=True,
        holders=holders,
        index_m=_cols(im, "trade_date", ["csi300", "all_a"]),
        buffett=_cols(b, "trade_date", ["ratio", "mv_yi", "gdp_ttm"]),
        buffett_bands=_bands(b.ratio, b.trade_date),
        daily=_cols(
            d,
            "trade_date",
            [
                "csi300",
                "all_a",
                "rzye",
                "rzrqye",
                "fin_buy_share",
                "north_net",
                "north_net_cum",
                "north_turnover",
                "pe_ttm",
                "cn10y",
                "erp",
            ],
        ),
        erp_bands=_bands(d.erp, d.trade_date),
        options=options,
        options_latest=latest,
        breadth=_cols(
            br, "trade_date", ["index", "adl_ema", "mcclellan", "pct_ma20", "pct_ma60", "nh_nl"]
        )
        if len(br)
        else None,
        hk=_cols(
            hk.assign(signal=hk.signal.astype(float)),
            "trade_date",
            ["csi300", "hk", "smart", "smart_ma20", "signal"],
        )
        if len(hk)
        else None,
        leadlag=_cols(ll.assign(lag=ll.lag.astype(str)), "lag", ["corr", "corr_rev"])
        if len(ll)
        else None,
        hk_rollcorr=_cols(rc, "trade_date", ["corr_lag1"]) if len(rc) else None,
        futures={
            str(k): _cols(g, "trade_date", ["close", "spot", "basis_pct", "net", "net_chg"])
            for k, g in fu.groupby("sym")
        }
        if len(fu)
        else {},
        studies=[
            StudyRow(**{k: (None if pd.isna(r[k]) else r[k]) for k in StudyRow.model_fields})
            for r in st.to_dict("records")
        ]
        if len(st)
        else [],
        etf_share=_cols(ep, "trade_date", [c for c in ep.columns if c != "trade_date"]),
    )


def load_summary(research: Path) -> dict | None:
    import json

    p = research / "macro" / "daily" / "latest.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None
