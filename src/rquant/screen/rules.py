"""筛选积木：每块是返回 (df) -> pd.Series[bool] 的工厂函数。"""

from __future__ import annotations

from typing import Callable

import pandas as pd

Rule = Callable[[pd.DataFrame], pd.Series]
