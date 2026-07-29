"""W3 · P0 K1 闸门:用日线代理估算三重锁相 VP 策略的信号频次。

对应 VP-STRATEGY-SPEC.md §9.2 P0 与 §6.3 K1:
  日线代理信号 < 100 次/年 → 样本不足,统计上无法证明任何事 → 终止项目。

日线代理口径(近似,只用于频次量级判断,不用于收益结论):
  vwap_d      = amount(千元)*1000 / (vol(手)*100)          # 当日成交均价,元
  C_POC*      = 60 日 vwap_d 滚动中位数
  C_VAH*      = 60 日 vwap_d 滚动 85 分位(70% 价值区上界的日线近似)
  锚点        = 近 15 日内 pct_chg>7 且 amount ≥ 2×avg20(amount)
  AVP_POC*    = 锚点日..T-1 的 Σamount/Σvol
  POC_yest*   = vwap_d(T-1)                                # 单日 Session POC 近似
  L1(T)       = close(T-1) ≥ C_VAH*(T-1)
              ∧ close(T-1) ≤ 1.15×AVP_POC*(T-1)
              ∧ 15 日内有锚点 ∧ 非 ST ∧ 上市≥90天
  信号 E(T)   = L1(T) ∧ 当日价格触及 POC_yest*±0.8% 带
              ∧ vol(T) < vol(T-1)(缩量粗代理) ∧ close(T) 未涨停

运行(云服务器 82.156.0.68,lighthouse 用户):
  cd /home/lighthouse/rquant && .venv/bin/python ~/vp_p0/k1_signal_frequency.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from rquant.storage.duckdb import open_readonly_connection

BAND = 0.008
START = "2024-06-01"  # 含 60 日 warmup;daily_state(ST/涨停价)从 2024-09-02 起


def main() -> None:
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
    print(f"daily_bar 载入 {len(df):,} 行,{df['ts_code'].nunique()} 只,"
          f"{df['trade_date'].min().date()}~{df['trade_date'].max().date()}")

    out = []
    for code, g in df.groupby("ts_code", sort=False):
        g = g.reset_index(drop=True)
        n = len(g)
        if n < 80:
            continue
        amt_el = g["amount"].to_numpy() * 1000.0          # 元
        vol_sh = g["vol"].to_numpy() * 100.0              # 股
        vwap = amt_el / vol_sh
        s_vwap = pd.Series(vwap)
        c_vah = s_vwap.rolling(60, min_periods=60).quantile(0.85).to_numpy()

        avg20 = pd.Series(g["amount"]).shift(1).rolling(20).mean().to_numpy()
        is_anchor = (g["pct_chg"].to_numpy() > 7.0) & (g["amount"].to_numpy() >= 2.0 * avg20)
        pos = np.arange(n, dtype=float)
        anchor_pos = pd.Series(np.where(is_anchor, pos, np.nan)).ffill().to_numpy()
        has_anchor = (~np.isnan(anchor_pos)) & (pos - anchor_pos <= 15) & (pos - anchor_pos >= 1)

        cum_a = np.concatenate([[0.0], np.cumsum(amt_el)])
        cum_v = np.concatenate([[0.0], np.cumsum(vol_sh)])
        ai = np.nan_to_num(anchor_pos, nan=0).astype(int)
        # AVP over [anchor .. T-1]: sums 用前缀和,索引 T-1+1 与 anchor
        avp = np.full(n, np.nan)
        valid = has_anchor & (pos >= 1)
        t_idx = pos.astype(int)
        num = cum_a[t_idx] - cum_a[ai]      # 到 T-1(含)的和:cum[t] = sum(0..t-1)
        den = cum_v[t_idx] - cum_v[ai]
        avp[valid & (den > 0)] = (num / np.where(den > 0, den, np.nan))[valid & (den > 0)]

        close_p = g["close"].to_numpy()
        prev_close = np.roll(close_p, 1); prev_close[0] = np.nan
        prev_vah = np.roll(c_vah, 1); prev_vah[0] = np.nan
        prev_vwap = np.roll(vwap, 1); prev_vwap[0] = np.nan
        prev_vol = np.roll(g["vol"].to_numpy(), 1); prev_vol[0] = np.nan

        is_st = g["is_st"].fillna(False).to_numpy(dtype=bool)
        age_ok = ((g["trade_date"] - g["list_date"]).dt.days >= 90).fillna(False).to_numpy()
        lup = g["limit_up_price"].to_numpy()
        not_limit = np.where(np.isnan(lup), True, close_p < lup - 1e-9)

        l1 = ((prev_close >= prev_vah) & (prev_close <= 1.15 * avp)
              & has_anchor & ~is_st & age_ok & ~np.isnan(prev_vah))
        touch = ((g["low"].to_numpy() <= prev_vwap * (1 + BAND))
                 & (g["high"].to_numpy() >= prev_vwap * (1 - BAND)))
        shrink = g["vol"].to_numpy() < prev_vol
        sig = l1 & touch & shrink & not_limit

        if l1.any() or sig.any():
            sub = g.loc[l1 | sig, ["trade_date"]].copy()
            sub["ts_code"] = code
            sub["l1"] = l1[l1 | sig]
            sub["sig"] = sig[l1 | sig]
            sub["board"] = ("gem_star" if code[:3] in ("300", "301", "688", "689")
                            else "bj" if code[:2] in ("82", "83", "87", "88", "92") else "main")
            out.append(sub)

    r = pd.concat(out, ignore_index=True)
    r["year"] = r["trade_date"].dt.year
    # 云端 daily_bar 存在 2024-09~2025-04-20 稀疏空洞(每日仅零星标的,
    # 见 DEPLOY.md 迁移记录:历史回补止于 2024-08-30,新数据自 2025-04-21 起),
    # 统计窗取数据完整段 2025-05-01+(留 60d 滚动 warmup)
    r = r[r["trade_date"] >= "2025-05-01"]
    days = df.loc[df["trade_date"] >= "2025-05-01", "trade_date"].nunique()

    print(f"\n统计窗 2024-12-01~,交易日 {days} 天(约 {days/244:.1f} 年)\n")
    print("== L1 候选池(日均规模)==")
    l1_daily = r[r["l1"]].groupby("trade_date").size()
    print(f"日均 {l1_daily.mean():.0f} 只 | 中位 {l1_daily.median():.0f} | P95 {l1_daily.quantile(.95):.0f}")

    print("\n== 买入信号 E(K1 闸门)==")
    sig_df = r[r["sig"]]
    total = len(sig_df)
    per_year = total / (days / 244)
    print(f"总信号 {total} 条 → 年化 {per_year:.0f} 条/年")
    print(sig_df.groupby("board").size().rename("信号数").to_string())
    print(sig_df.groupby(sig_df["trade_date"].dt.to_period("Q")).size().rename("按季度").to_string())
    verdict = "✅ K1 通过(≥100/年)" if per_year >= 100 else "❌ K1 不通过 → 按协议终止项目"
    print(f"\n**K1 判定:{verdict}**")


if __name__ == "__main__":
    main()
