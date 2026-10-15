"""Exchange price-limit rules (single source of truth, date-aware).

- 主板 10%；主板风险警示（ST/*ST）5% → **10% from 2026-07-06**（沪深《交易规则（2026年修订）》，
  上交所 2026-04-24 公告，2026-07-06 起施行）。
- 创业板 20%（2020-08-24 起，含 ST；之前 10% / ST 5%）；科创板 20%；北交所 30%。
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

CHINEXT_20_PERCENT_START = date(2020, 8, 24)
MAIN_ST_10_PERCENT_START = date(2026, 7, 6)


def _today() -> date:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def board_of(ts_code: str) -> str:
    c = str(ts_code)
    if c.endswith(".BJ"):
        return "bj"
    if c.startswith(("688", "689")):
        return "star"
    if c.startswith(("300", "301", "302")):
        return "gem"
    return "main"


def limit_pct(is_st: bool, board_type: str, trade_date: date | None = None) -> float:
    d = trade_date or _today()
    if board_type == "bj":
        return 0.30
    if board_type == "star":
        return 0.20
    if board_type == "gem":
        if d >= CHINEXT_20_PERCENT_START:
            return 0.20
        return 0.05 if is_st else 0.10
    if is_st and d < MAIN_ST_10_PERCENT_START:
        return 0.05
    return 0.10


def is_st_name(names: pd.Series | np.ndarray) -> np.ndarray:
    s = pd.Series(names).fillna("").astype(str).str.upper()
    return s.str.contains("ST", regex=False).to_numpy()


def limit_pct_vec(codes, names_or_is_st, trade_date: date | None = None) -> np.ndarray:
    """Vectorized limit pct. ``names_or_is_st``: names (ST detected by substring) or bool array."""
    d = trade_date or _today()
    c = pd.Series(np.asarray(codes, dtype=object)).astype(str)
    v = np.asarray(names_or_is_st)
    st = v.astype(bool) if v.dtype == bool else is_st_name(v)
    gem = c.str.startswith(("300", "301", "302")).to_numpy()
    star = c.str.startswith(("688", "689")).to_numpy()
    bj = c.str.endswith(".BJ").to_numpy()
    main_st = 0.05 if d < MAIN_ST_10_PERCENT_START else 0.10
    gem_pct = np.where(st, 0.05, 0.10) if d < CHINEXT_20_PERCENT_START else 0.20
    out = np.where(st, main_st, 0.10)
    out = np.where(gem, gem_pct, out)
    out = np.where(star, 0.20, out)
    return np.where(bj, 0.30, out).astype(float)


def abnormal_threshold_3d(board_type: str, is_st: bool, trade_date: date | None = None) -> float:
    """异常波动 3 日偏离值阈值（%）：主板 20（主板 ST 在 2026-07-06 前为 12），创/科 30。"""
    d = trade_date or _today()
    if board_type in ("gem", "star"):
        return 30.0
    if is_st and d < MAIN_ST_10_PERCENT_START:
        return 12.0
    return 20.0
