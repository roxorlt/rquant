# ruff: noqa: E501 - rule texts and detail strings read better unwrapped
"""沪深交易所 异常波动 / 严重异常波动 monitor + next-day predictor (vectorized over stocks).

Rules (《上海/深圳证券交易所交易规则（2026年修订）》, effective 2026-07-06):
- 偏离值 = 个股涨跌幅 − 对应指数涨跌幅（除权复权）。对应指数：沪主板 000001.SH、深主板 399001.SZ、
  创业板 399102.SZ（创业板综指）、科创板 000688.SH（科创50）。北交所 / 无涨跌幅限制股票不计。
- 异常波动：连续 3 日偏离值累计 ±20%（主板；主板 ST 在 2026-07-06 前 ±12%）/ ±30%（创业板、科创板）。
- 严重异常波动：10 日内同向异常波动 4 次（主板）/ 3 次（创、科）；或 10 日 +100%/−50%；或 30 日 +200%/−70%。
- 异常波动指标自交易所公布的次一交易日起重新计算 → 计数器在触发（或交易所公告）后重置。

Inputs are wide frames (index = trading days, columns = ts_code). Loops run over DAYS only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from rquant.limit_rules import MAIN_ST_10_PERCENT_START, board_of, limit_pct

BOARD_INDEX = {"sh_main": "000001.SH", "sz_main": "399001.SZ", "gem": "399102.SZ",
               "star": "000688.SH"}


def index_key(ts_code: str) -> str | None:
    b = board_of(ts_code)
    if b == "bj":
        return None
    if b in ("gem", "star"):
        return b
    return "sh_main" if ts_code.endswith(".SH") else "sz_main"


@dataclass
class AbnormalResult:
    """All frames share (days x codes). Values for the LAST row are what a daily job stores."""

    dev3: pd.DataFrame          # cumulative deviation (%) over the live (reset-aware) ≤3-day window
    abnormal: pd.DataFrame      # +1 / -1 / 0 triggered on that day
    count10_up: pd.DataFrame    # same-direction triggers within the last 10 days (incl. today)
    count10_dn: pd.DataFrame
    dev10: pd.DataFrame
    dev30: pd.DataFrame
    severe: pd.DataFrame        # bool, severe abnormal on that day (any rule)
    near_abn_up: pd.DataFrame   # tomorrow one limit-up would trigger 异常波动
    near_abn_dn: pd.DataFrame
    near_severe: pd.DataFrame   # tomorrow one limit-up would trigger 严重异常波动


def compute(stock_ret: pd.DataFrame, index_ret: pd.DataFrame, st: pd.DataFrame | None = None,
            announced: pd.DataFrame | None = None) -> AbnormalResult:
    """``stock_ret``: daily return (close/pre_close-1, ex-right adjusted), NaN when not traded.
    ``index_ret``: daily index return, columns = BOARD_INDEX values. ``st``: bool PIT ST.
    ``announced``: bool, exchange published an 异常波动 for the stock that day (e.g. from
    Tushare top_list.reason); OR-ed with our own trigger for the counter reset."""
    days, codes = stock_ret.index, stock_ret.columns
    keys = np.array([index_key(c) for c in codes], dtype=object)
    valid_col = keys != None  # noqa: E711 - object array comparison
    ir = np.zeros((len(days), len(codes)))
    for k, ix in BOARD_INDEX.items():
        m = keys == k
        if m.any() and ix in index_ret.columns:
            ir[:, m] = index_ret[ix].reindex(days).fillna(0.0).to_numpy()[:, None]
    sr = stock_ret.to_numpy(float)
    traded = ~np.isnan(sr)
    # exchange formula: (期末收盘/期初前收 − 1) − (指数同期) → compounded period returns
    lr_s = np.log1p(np.where(traded, sr, 0.0))
    lr_i = np.log1p(np.where(traded, ir, 0.0))
    stv = st.reindex(index=days, columns=codes).fillna(False).to_numpy(bool) if st is not None \
        else np.zeros_like(traded)
    ann = announced.reindex(index=days, columns=codes).fillna(False).to_numpy(bool) \
        if announced is not None else np.zeros_like(traded)
    gemstar = np.array([board_of(c) in ("gem", "star") for c in codes])
    n_need = np.where(gemstar, 3, 4)
    nd, nc = sr.shape
    dev3 = np.full((nd, nc), np.nan)
    abn = np.zeros((nd, nc), dtype=np.int8)
    start = np.zeros(nc, dtype=np.int64)        # first day of the live window (after reset)
    cs_s = np.vstack([np.zeros(nc), np.cumsum(lr_s, axis=0)])
    cs_i = np.vstack([np.zeros(nc), np.cumsum(lr_i, axis=0)])
    tcount = np.vstack([np.zeros(nc), np.cumsum(traded, axis=0)])
    for t in range(nd):
        d = days[t].date() if hasattr(days[t], "date") else days[t]
        thr = np.where(gemstar, 30.0,
                       np.where(stv[t] & (d < MAIN_ST_10_PERCENT_START), 12.0, 20.0))
        # window = last 3 TRADED days of this stock, not before `start`
        lo = np.maximum(start, _nth_traded_back(tcount, t, 3))
        s = (np.expm1(cs_s[t + 1] - cs_s[lo]) - np.expm1(cs_i[t + 1] - cs_i[lo])) * 100
        s = np.where(traded[t] & valid_col, s, np.nan)
        dev3[t] = s
        up, dn = s >= thr, s <= -thr
        abn[t] = np.where(up, 1, np.where(dn, -1, 0))
        reset = up | dn | ann[t]
        start = np.where(reset, t + 1, start)
    up_m, dn_m = (abn == 1).astype(int), (abn == -1).astype(int)
    c10u = pd.DataFrame(up_m, index=days, columns=codes).rolling(10, min_periods=1).sum()
    c10d = pd.DataFrame(dn_m, index=days, columns=codes).rolling(10, min_periods=1).sum()

    def devn(n: int) -> pd.DataFrame:
        a = (np.expm1(pd.DataFrame(lr_s).rolling(n, min_periods=1).sum())
             - np.expm1(pd.DataFrame(lr_i).rolling(n, min_periods=1).sum())) * 100
        a = a.where(valid_col[None, :] & traded)
        return pd.DataFrame(a.to_numpy(), index=days, columns=codes)

    d10, d30 = devn(10), devn(30)
    d9, d29 = devn(9), devn(29)
    severe = ((d10 >= 100) | (d10 <= -50) | (d30 >= 200) | (d30 <= -70)
              | (c10u.to_numpy() >= n_need) | (c10d.to_numpy() >= n_need)) & traded
    # next-day predictor: tomorrow's 3-day window = last 2 traded days (not before reset) + T+1
    lim = np.array([limit_pct(bool(s_), board_of(c)) for c, s_ in zip(codes, stv[-1], strict=True)]) * 100
    last = nd - 1
    d_last = days[last].date() if hasattr(days[last], "date") else days[last]
    thr_n = np.where(gemstar, 30.0, np.where(stv[last] & (d_last < MAIN_ST_10_PERCENT_START), 12.0, 20.0))
    near_u = np.zeros((nd, nc), dtype=bool)
    near_d = np.zeros((nd, nc), dtype=bool)
    near_s = np.zeros((nd, nc), dtype=bool)
    for t in range(nd):  # vectorized per day (cheap); last row is what matters
        st_win = _window_start_after(abn, ann, t)
        lo = np.maximum(st_win, _nth_traded_back(tcount, t, 2))
        part = (np.expm1(cs_s[t + 1] - cs_s[lo]) - np.expm1(cs_i[t + 1] - cs_i[lo])) * 100
        part = np.where(valid_col, part, np.nan)
        near_u[t] = (part + lim >= thr_n) & ~(abn[t] != 0)
        near_d[t] = (part - lim <= -thr_n) & ~(abn[t] != 0)
        cu9 = up_m[max(0, t - 8): t + 1].sum(0)
        near_s[t] = (((d9.iloc[t].to_numpy() + lim) >= 100) | ((d29.iloc[t].to_numpy() + lim) >= 200)
                     | ((cu9 >= n_need - 1) & near_u[t])) & ~severe.iloc[t].to_numpy()
    mk = lambda a: pd.DataFrame(a, index=days, columns=codes)  # noqa: E731
    return AbnormalResult(dev3=mk(dev3), abnormal=mk(abn), count10_up=c10u, count10_dn=c10d,
                          dev10=d10, dev30=d30, severe=severe, near_abn_up=mk(near_u),
                          near_abn_dn=mk(near_d), near_severe=mk(near_s))


def _nth_traded_back(tcount: np.ndarray, t: int, n: int) -> np.ndarray:
    """Per stock: index of the earliest day such that days [idx..t] contain the last n traded
    days (for stocks with fewer, 0)."""
    target = tcount[t + 1] - n  # cumulative traded count just before the window
    # earliest idx with tcount[idx] >= target → searchsorted per column
    out = np.empty(tcount.shape[1], dtype=np.int64)
    for j in range(tcount.shape[1]):
        out[j] = np.searchsorted(tcount[: t + 2, j], max(target[j], 0), side="right") - 1
    return np.clip(out, 0, t)


def _window_start_after(abn: np.ndarray, ann: np.ndarray, t: int) -> np.ndarray:
    """Per stock: day after the most recent reset (trigger or announcement) at or before t."""
    ev = (abn[: t + 1] != 0) | ann[: t + 1]
    any_ = ev.any(0)
    lastidx = t - np.argmax(ev[::-1], axis=0)
    return np.where(any_, lastidx + 1, 0)


def latest_flags(res: AbnormalResult, day: date | None = None) -> pd.DataFrame:
    """Long table of the last row: ts_code, flag, detail (for risk_flags_daily)."""
    t = -1
    rows = []
    for flag, frame, fmt in (
        ("abn_triggered", res.abnormal.iloc[t] != 0, lambda c: f"3日偏离 {res.dev3.iloc[t][c]:.1f}%"),
        ("severe_abnormal", res.severe.iloc[t], lambda c: (
            f"10日 {res.dev10.iloc[t][c]:.0f}% / 30日 {res.dev30.iloc[t][c]:.0f}% / "
            f"10日同向异动 {int(max(res.count10_up.iloc[t][c], res.count10_dn.iloc[t][c]))} 次")),
        ("near_abn_up", res.near_abn_up.iloc[t], lambda c: f"再一个涨停即触发异动（当前窗口偏离 {res.dev3.iloc[t][c]:.1f}%）"),
        ("near_abn_dn", res.near_abn_dn.iloc[t], lambda c: "再一个跌停即触发下跌异动"),
        ("near_severe", res.near_severe.iloc[t], lambda c: (
            f"临近严重异动：10日 {res.dev10.iloc[t][c]:.0f}% / 30日 {res.dev30.iloc[t][c]:.0f}% / "
            f"10日同向异动 {int(res.count10_up.iloc[t][c])} 次")),
    ):
        hit = frame[frame.fillna(False).astype(bool)].index
        rows += [{"ts_code": c, "flag": flag, "detail": fmt(c)} for c in hit]
    return pd.DataFrame(rows, columns=["ts_code", "flag", "detail"])
