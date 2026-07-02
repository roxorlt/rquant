"""分钟 replay 交易样本缓存。"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd
from pydantic import BaseModel, ConfigDict


class ReplayCacheKey(BaseModel):
    """replay 样本缓存键。"""

    model_config = ConfigDict(frozen=True)

    preset_name: str
    start_date: str
    end_date: str
    entry_mode: str
    profile_variant: str
    max_hold_days: int
    paper_candidate_id: str = "baseline"
    freq: str = "1min"

    @property
    def cache_key(self) -> str:
        """稳定字符串键，方便 Streamlit / CLI 复用。"""
        return "|".join([
            self.preset_name,
            self.start_date,
            self.end_date,
            self.entry_mode,
            self.profile_variant,
            f"h{self.max_hold_days}",
            self.paper_candidate_id,
            self.freq,
        ])


class ReplayTradeCache:
    """进程内交易样本缓存。"""

    def __init__(self) -> None:
        self._frames: dict[str, pd.DataFrame] = {}

    def get_or_load(
        self,
        key: ReplayCacheKey,
        loader: Callable[[], pd.DataFrame],
    ) -> pd.DataFrame:
        """返回缓存副本；未命中时调用 loader。"""
        if key.cache_key not in self._frames:
            self._frames[key.cache_key] = loader().copy()
        return self._frames[key.cache_key].copy()

    def clear(self) -> None:
        """清空缓存。"""
        self._frames.clear()
