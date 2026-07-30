"""W6 · L1 候选池收紧的阈值扫描研究（只读，日线代理口径同 k1_signal_frequency.py）。

背景：P0 结果（docs/vp-strategy/p0/RESULTS.md）显示日线代理的 L1 候选池日均 465 只，
远超 VP-STRATEGY-SPEC.md §3.1 目标 50–300（K1 闸门信号量绰绰有余，问题反而是筛选太松）。
本脚本沿用 k1_signal_frequency.py 的口径与数据空洞处理，做单变量 + 组合阈值扫描，
只输出统计量（L1 日均池规模 / 年化信号数等），**不做任何写库操作**（`open_readonly_connection`）。

沿用 k1_signal_frequency.py 的定义（未在此重复推导）：
  vwap_d      = amount(千元)*1000 / (vol(手)*100)
  C_VAH*      = 60 日 vwap_d 滚动 N 分位（本脚本扫描 N）
  锚点        = 近 15 日内 pct_chg>7 且 amount ≥ R×avg20(amount)（本脚本扫描 R）
  AVP_POC*    = 锚点日..T-1 的 Σamount/Σvol
  POC_yest*   = vwap_d(T-1)
  L1(T)       = close(T-1) ≥ C_VAH*(T-1)
              ∧ close(T-1) ≤ D×AVP_POC*(T-1)（本脚本扫描 D，基线 D=1.15）
              ∧ 15 日内有锚点 ∧ 非 ST ∧ 上市≥90天
              ∧ [新增] 20 日均成交额(T-1) ≥ 流动性下限 L（本脚本扫描 L，基线 L=0 即不过滤）
  信号 E(T)   = L1(T) ∧ 当日价格触及 POC_yest*±0.8% 带 ∧ vol(T)<vol(T-1) ∧ close(T) 未涨停

扫描维度（单变量，其余固定在基线值）：
  1. c_vah_q       ：0.85(基线) / 0.90 / 0.95
  2. anchor_ratio  ：2.0(基线) / 2.5 / 3.0
  3. avp_distance  ：1.15(基线) / 1.10 / 1.08
  4. min_amt_wan   ：0(基线,不过滤) / 5000(万元) / 10000(万元=1亿)
     daily_bar.amount 单位千元，换算 min_amt_thousand = min_amt_wan × 10
  5. 组合预设 宽/中/严（研究者按单变量结果预先选定的参数组合，与单变量扫描同一次跑出）

统计窗与数据空洞处理同基线：云端 daily_bar 在 2024-09~2025-04-20 存在稀疏空洞
（历史回补止于 2024-08-30，新数据自 2025-04-21 起），故统计窗取 2025-05-01+
（START=2024-06-01 只是为了留出 60 日 C_VAH 滚动 + 15 日锚点回看的 warmup）。

运行（云服务器 82.156.0.68，lighthouse 用户，只读连接，零写库）：
  cd /home/lighthouse/rquant && .venv/bin/python ~/vp_p0/l1_tightening_sweep.py
"""
from __future__ import annotations

import time

import numpy as np
import pandas as pd

from rquant.storage.duckdb import open_readonly_connection

BAND = 0.008
START = "2024-06-01"          # 含 60 日 C_VAH warmup + 15 日锚点回看，同 k1_signal_frequency.py
WINDOW_START = "2025-05-01"   # 数据空洞后的完整段（见模块 docstring）
YEAR_DAYS = 244

C_VAH_QS = [0.85, 0.90, 0.95]
ANCHOR_RATIOS = [2.0, 2.5, 3.0]
AVP_DISTANCES = [1.15, 1.10, 1.08]
MIN_AMT_WAN = [0, 5000, 10000]  # 万元；0 / 5000万 / 1亿

BASE = {"c_vah_q": 0.85, "anchor_ratio": 2.0, "avp_distance": 1.15, "min_amt_wan": 0}


def _combo(name: str, **overrides) -> dict:
    d = dict(BASE)
    d.update(overrides)
    d["name"] = name
    return d


COMBOS: list[dict] = (
    [_combo("baseline")]
    + [_combo(f"c_vah_q={q}", c_vah_q=q) for q in C_VAH_QS if q != BASE["c_vah_q"]]
    + [_combo(f"anchor_ratio={r}", anchor_ratio=r) for r in ANCHOR_RATIOS if r != BASE["anchor_ratio"]]
    + [_combo(f"avp_distance={d}", avp_distance=d) for d in AVP_DISTANCES if d != BASE["avp_distance"]]
    + [_combo(f"min_amt_wan={m}", min_amt_wan=m) for m in MIN_AMT_WAN if m != BASE["min_amt_wan"]]
    + [
        # 三档预设：从单变量扫描的方向性判断出发（尚未看到本次数值前的合理猜测），
        # 由每维度取值集合中递进组合而成，覆盖「轻度收紧」到「多维叠加收紧」的区间；
        # 最终推荐档位由 L1-TIGHTENING.md 依据本次实测结果给出。
        _combo("preset_宽", c_vah_q=0.90, anchor_ratio=2.0, avp_distance=1.15, min_amt_wan=0),
        _combo("preset_中", c_vah_q=0.90, anchor_ratio=2.5, avp_distance=1.10, min_amt_wan=5000),
        _combo("preset_严", c_vah_q=0.95, anchor_ratio=3.0, avp_distance=1.08, min_amt_wan=10000),
    ]
)


def _c_vah_and_prev(s_vwap: pd.Series, q: float) -> tuple[np.ndarray, np.ndarray]:
    c_vah = s_vwap.rolling(60, min_periods=60).quantile(q).to_numpy()
    prev_vah = np.roll(c_vah, 1)
    prev_vah[0] = np.nan
    return c_vah, prev_vah


def _anchor_and_avp(
    pct_chg: np.ndarray,
    amount_raw: np.ndarray,
    ratio: float,
    avg20: np.ndarray,
    pos: np.ndarray,
    cum_a: np.ndarray,
    cum_v: np.ndarray,
    n: int,
) -> tuple[np.ndarray, np.ndarray]:
    is_anchor = (pct_chg > 7.0) & (amount_raw >= ratio * avg20)
    anchor_pos = pd.Series(np.where(is_anchor, pos, np.nan)).ffill().to_numpy()
    has_anchor = (~np.isnan(anchor_pos)) & (pos - anchor_pos <= 15) & (pos - anchor_pos >= 1)

    ai = np.nan_to_num(anchor_pos, nan=0).astype(int)
    valid = has_anchor & (pos >= 1)
    t_idx = pos.astype(int)
    num = cum_a[t_idx] - cum_a[ai]
    den = cum_v[t_idx] - cum_v[ai]
    avp = np.full(n, np.nan)
    avp[valid & (den > 0)] = (num / np.where(den > 0, den, np.nan))[valid & (den > 0)]
    return has_anchor, avp


def main() -> None:
    t0 = time.time()
    conn = open_readonly_connection()
    df = conn.execute(
        """
        SELECT b.ts_code, b.trade_date, b.open, b.high, b.low, b.close,
               b.vol, b.amount, b.pct_chg,
               s.is_st, s.limit_up_price,
               sb.list_date
        FROM daily_bar b
        LEFT JOIN daily_state s USING (ts_code, trade_date)
        LEFT JOIN stock_basic sb ON b.ts_code = sb.ts_code
        WHERE b.trade_date >= ? AND b.vol > 0 AND b.amount > 0
        ORDER BY b.ts_code, b.trade_date
        """,
        [START],
    ).fetchdf()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce")
    print(f"daily_bar 载入 {len(df):,} 行，{df['ts_code'].nunique()} 只，"
          f"{df['trade_date'].min().date()}~{df['trade_date'].max().date()}  "
          f"({time.time() - t0:.0f}s)")

    all_dates = np.sort(df["trade_date"].unique())
    n_dates = len(all_dates)
    window_mask = all_dates >= np.datetime64(WINDOW_START)
    days = int(window_mask.sum())
    print(f"统计窗 {WINDOW_START}+，交易日 {days} 天（约 {days / YEAR_DAYS:.1f} 年）")
    print(f"扫描组合数：{len(COMBOS)}\n")

    acc = {c["name"]: {"l1": np.zeros(n_dates, dtype=np.int64), "sig": np.zeros(n_dates, dtype=np.int64)}
           for c in COMBOS}

    n_stocks = 0
    for code, g in df.groupby("ts_code", sort=False):
        g = g.reset_index(drop=True)
        n = len(g)
        if n < 80:
            continue
        n_stocks += 1

        amt_el = g["amount"].to_numpy() * 1000.0  # 元
        vol_sh = g["vol"].to_numpy() * 100.0      # 股
        vwap = amt_el / vol_sh
        s_vwap = pd.Series(vwap)

        amount_raw = g["amount"].to_numpy()  # 千元，也是 avg20 / 流动性下限的单位
        avg20 = pd.Series(amount_raw).shift(1).rolling(20).mean().to_numpy()

        pos = np.arange(n, dtype=float)
        cum_a = np.concatenate([[0.0], np.cumsum(amt_el)])
        cum_v = np.concatenate([[0.0], np.cumsum(vol_sh)])
        pct_chg = g["pct_chg"].to_numpy()

        close_p = g["close"].to_numpy()
        prev_close = np.roll(close_p, 1); prev_close[0] = np.nan
        prev_vwap = np.roll(vwap, 1); prev_vwap[0] = np.nan
        prev_vol = np.roll(g["vol"].to_numpy(), 1); prev_vol[0] = np.nan

        is_st = g["is_st"].fillna(False).to_numpy(dtype=bool)
        age_ok = ((g["trade_date"] - g["list_date"]).dt.days >= 90).fillna(False).to_numpy()
        lup = g["limit_up_price"].to_numpy()
        not_limit = np.where(np.isnan(lup), True, close_p < lup - 1e-9)

        touch = ((g["low"].to_numpy() <= prev_vwap * (1 + BAND))
                 & (g["high"].to_numpy() >= prev_vwap * (1 - BAND)))
        shrink = g["vol"].to_numpy() < prev_vol

        date_codes = np.searchsorted(all_dates, g["trade_date"].to_numpy())

        c_vah_cache = {q: _c_vah_and_prev(s_vwap, q) for q in C_VAH_QS}
        anchor_cache = {
            r: _anchor_and_avp(pct_chg, amount_raw, r, avg20, pos, cum_a, cum_v, n)
            for r in ANCHOR_RATIOS
        }

        for c in COMBOS:
            _, prev_vah = c_vah_cache[c["c_vah_q"]]
            has_anchor, avp = anchor_cache[c["anchor_ratio"]]
            min_amt_thousand = c["min_amt_wan"] * 10  # 万元 -> 千元
            liq_ok = True if min_amt_thousand == 0 else (avg20 >= min_amt_thousand)

            l1 = ((prev_close >= prev_vah)
                  & (prev_close <= c["avp_distance"] * avp)
                  & has_anchor & ~is_st & age_ok & ~np.isnan(prev_vah) & liq_ok)
            sig = l1 & touch & shrink & not_limit

            a = acc[c["name"]]
            if l1.any():
                np.add.at(a["l1"], date_codes[l1], 1)
            if sig.any():
                np.add.at(a["sig"], date_codes[sig], 1)

        if n_stocks % 1000 == 0:
            print(f"  ... {n_stocks} 只已处理（{time.time() - t0:.0f}s）")

    print(f"\n共处理 {n_stocks} 只标的（{time.time() - t0:.0f}s），汇总结果：\n")

    rows = []
    for c in COMBOS:
        a = acc[c["name"]]
        l1_win = a["l1"][window_mask]
        sig_win = a["sig"][window_mask]
        total_sig = int(sig_win.sum())
        annual_sig = total_sig / (days / YEAR_DAYS)
        rows.append({
            "combo": c["name"],
            "c_vah_q": c["c_vah_q"],
            "anchor_ratio": c["anchor_ratio"],
            "avp_distance": c["avp_distance"],
            "min_amt_wan": c["min_amt_wan"],
            "l1_mean": round(float(l1_win.mean()), 1),
            "l1_median": round(float(np.median(l1_win)), 1),
            "l1_p95": round(float(np.quantile(l1_win, 0.95)), 1),
            "sig_total": total_sig,
            "sig_annual": round(annual_sig, 0),
        })

    result_df = pd.DataFrame(rows)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)
    print(result_df.to_string(index=False))
    print(f"\n总耗时 {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
