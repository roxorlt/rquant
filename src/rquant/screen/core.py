"""screen() 主流程：加载宽表 → 应用规则 → 返回结果。"""

from __future__ import annotations

import pandas as pd

from rquant.screen.loader import load_universe
from rquant.screen.rules import Rule
from rquant.storage.duckdb import DuckDBStore

BASE_COLUMNS = ["ts_code", "name", "CLOSE[0]", "PCT_CHG[0]"]


def _infer_lookback(rules: list[Rule]) -> int:
    return max((getattr(r, "min_lookback", 0) for r in rules), default=0)


def screen(
    trade_date: str,
    rules: list[Rule],
    lookback: int | None = None,
    include_columns: list[str] | None = None,
    store: DuckDBStore | None = None,
) -> pd.DataFrame:
    """筛选：给定 trade_date 和 rules，返回命中股票。

    - rules 列表内部按 AND 合并
    - lookback 默认按 rules 的 min_lookback 推断，最小 0
    - include_columns 控制结果附加列（base 列 ts_code/name/CLOSE[0]/PCT_CHG[0] 必出）
    """
    if lookback is None:
        lookback = _infer_lookback(rules)

    df = load_universe(trade_date, lookback=lookback, store=store)

    if df.empty:
        cols = list(BASE_COLUMNS)
        if include_columns:
            cols += [c for c in include_columns if c not in cols]
        return pd.DataFrame(columns=cols)

    mask = pd.Series(True, index=df.index)
    for rule in rules:
        mask &= rule(df)

    result = df.loc[mask].copy()

    cols = list(BASE_COLUMNS)
    if include_columns:
        cols += [c for c in include_columns if c not in cols]
    cols = [c for c in cols if c in result.columns]

    return result[cols].sort_values("ts_code").reset_index(drop=True)
