"""screen() 主流程：加载宽表 → 应用规则 → 返回结果。"""

from __future__ import annotations

import pandas as pd

from rquant.screen.rules import Rule


def screen(
    trade_date: str,
    rules: list[Rule],
    lookback: int | None = None,
    include_columns: list[str] | None = None,
    store=None,
) -> pd.DataFrame:
    raise NotImplementedError
