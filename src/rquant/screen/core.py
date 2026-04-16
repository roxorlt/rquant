"""screen() 主流程：加载宽表 → 应用规则 → 返回结果。"""

from __future__ import annotations

from typing import Callable

import pandas as pd

from rquant.screen.loader import load_universe

Rule = Callable[[pd.DataFrame], pd.Series]


def screen(
    trade_date: str,
    rules: list[Rule],
    lookback: int | None = None,
    include_columns: list[str] | None = None,
    store=None,
) -> pd.DataFrame:
    raise NotImplementedError
