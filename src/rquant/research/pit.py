"""Point-in-time helpers shared by the research studies.

* ST / 退市整理 status per (date, stock) from Tushare ``namechange`` history (falls back to
  the current name when the table is missing or empty). Delisted stocks stay in the
  universe: only the days on which they carried an ST/退 name are excluded.
* Split/bonus-adjusted volume: raw volume divided by the cumulative adjustment factor, so
  送转 days do not look like volume spikes (consistent with ``daily_bar_hfq`` prices).
"""

from __future__ import annotations

import duckdb
import pandas as pd

ST_PATTERN = "ST|退"


def st_intervals(con: duckdb.DuckDBPyConnection) -> pd.DataFrame | None:
    """(ts_code, start, end) intervals during which the name contained ST/退; None if no
    namechange history is available."""
    try:
        nc = con.execute("SELECT ts_code, name, start_date, end_date FROM namechange").df()
    except duckdb.Error:
        return None
    if nc.empty:
        return None
    nc = nc[nc["name"].str.contains(ST_PATTERN, na=False)]
    return pd.DataFrame({"ts_code": nc["ts_code"],
                         "start": pd.to_datetime(nc["start_date"]),
                         "end": pd.to_datetime(nc["end_date"]).fillna(pd.Timestamp.max)})


def current_st(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {c for (c,) in con.execute(
        "SELECT ts_code FROM stock_basic WHERE name LIKE '%ST%' OR name LIKE '%退%'").fetchall()}


def st_mask(con: duckdb.DuckDBPyConnection, index: pd.Index, columns: pd.Index) -> pd.DataFrame:
    """Boolean frame (trade_date x ts_code): True where the stock was ST/退 on that day."""
    out = pd.DataFrame(False, index=index, columns=columns)
    iv = st_intervals(con)
    if iv is None:
        cols = [c for c in current_st(con) if c in out.columns]
        out[cols] = True
        return out
    iv = iv[iv["ts_code"].isin(columns)]
    for code, s, e in zip(iv["ts_code"], iv["start"], iv["end"], strict=True):
        out.loc[(out.index >= s) & (out.index <= e), code] = True
    return out


def is_st(con: duckdb.DuckDBPyConnection, codes: pd.Series, dates: pd.Series) -> pd.Series:
    """Row-wise point-in-time ST flag for (code, date) pairs."""
    dates = pd.to_datetime(pd.Series(dates).reset_index(drop=True))
    codes = pd.Series(codes).reset_index(drop=True)
    iv = st_intervals(con)
    if iv is None:
        return codes.isin(current_st(con))
    df = pd.DataFrame({"ts_code": codes, "d": dates, "i": range(len(codes))})
    m = df.merge(iv, on="ts_code")
    hit = set(m.loc[(m["d"] >= m["start"]) & (m["d"] <= m["end"]), "i"])
    return pd.Series([i in hit for i in range(len(codes))])


def adj_panel(con: duckdb.DuckDBPyConnection, like: pd.DataFrame) -> pd.DataFrame:
    a = con.execute("SELECT trade_date, ts_code, adj_factor FROM adj_factor "
                    "WHERE trade_date BETWEEN ? AND ?",
                    [like.index.min(), like.index.max()]).df()
    a["trade_date"] = pd.to_datetime(a["trade_date"])
    return a.pivot(index="trade_date", columns="ts_code", values="adj_factor").reindex_like(like)


def adjust_volume(con: duckdb.DuckDBPyConnection, panel: dict[str, pd.DataFrame]) -> None:
    """In place: panel['vol'] /= adj_factor (送转-neutral volume)."""
    adj = adj_panel(con, panel["vol"])
    panel["vol"] = panel["vol"] / adj.where(adj > 0)
