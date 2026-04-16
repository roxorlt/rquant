"""宽表加载：把 daily_bar/daily_indicator/daily_state 合并成
每行 1 只股票、每字段带 [n] 后缀的宽表。"""

from __future__ import annotations

import pandas as pd

from rquant.storage.duckdb import DuckDBStore


def load_universe(
    trade_date: str,
    lookback: int = 5,
    store: DuckDBStore | None = None,
) -> pd.DataFrame:
    raise NotImplementedError
