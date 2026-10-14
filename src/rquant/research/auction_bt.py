"""One shared selection + backtest for the 竞价选股 strategy, used by the research study
(``studies.auction_study``), the live watchlist (``auction_pool.rank_picks``) and the paper
ledger (``auction_paper``), so research numbers and the live pool can never drift apart.

* same top-N (``TOP_N``, env ``AUCTION_POOL_TOP``; 0 = all)
* 竞价涨停 / 一字 open = unbuyable: kept in the list, flagged, excluded from returns
* costs: 0.11% fees + 0.2% slippage, round trip, subtracted from every trade
"""

from __future__ import annotations

import os

import pandas as pd

TOP_N = int(os.environ.get("AUCTION_POOL_TOP", "10"))
FEES = 0.0011
SLIPPAGE = 0.002
COST = FEES + SLIPPAGE
GAP_MIN = 0.04  # 同花顺: 竞价涨幅 > 4% (strict)


def select_top(df: pd.DataFrame, vol_col: str, top_n: int = TOP_N,
               day_col: str | None = "trade_date") -> pd.DataFrame:
    """Rank by auction volume desc (per day when day_col is set) and keep the top N."""
    if df.empty:
        return df.assign(rank=pd.Series(dtype=float))
    out = df.copy()
    if day_col:
        out["rank"] = out.groupby(day_col)[vol_col].rank(ascending=False, method="first")
    else:
        out["rank"] = out[vol_col].rank(ascending=False, method="first")
    out = out.sort_values([c for c in (day_col, "rank") if c])
    return out[out["rank"] <= top_n] if top_n > 0 else out


def net_trades(df: pd.DataFrame, unbuyable_col: str = "one_word",
               ret_cols: tuple[str, ...] = ("r_n_open", "r_n_close")) -> pd.DataFrame:
    """Buyable trades only, with ``<col>_net`` = gross - COST for each return column."""
    b = df[~df[unbuyable_col].astype(bool)].copy()
    for c in ret_cols:
        if c in b:
            b[f"{c}_net"] = b[c] - COST
    return b


def summary(r: pd.Series) -> dict:
    r = pd.Series(r).dropna()
    if r.empty:
        return {"n": 0, "mean": 0.0, "win_rate": 0.0, "ci_low": 0.0, "ci_high": 0.0}
    se = float(r.std(ddof=1) / len(r) ** 0.5) if len(r) > 1 else float("inf")
    m = float(r.mean())
    return {"n": int(len(r)), "mean": m, "win_rate": float((r > 0).mean()),
            "ci_low": m - 1.96 * se, "ci_high": m + 1.96 * se}
