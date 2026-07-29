"""W5 · 封单因子日频验证:封板强度是否预测次日收益。

对应 VP-STRATEGY-SPEC.md §9.2 P2 提前项。数据独立落盘(不写主库):
  fetch    从 tushare limit_list_d 逐日拉涨停/炸板榜 → ~/vp_p0/limit_list.parquet
  analyze  U 板样本,因子 = 封单额/流通市值(fd_ratio)、开板次数(open_times),
           结果 = T+1 开盘溢价 / T+1 收盘收益 / T+1 日内,训练(≤2025-12-31)/验证(2026+)
           按日横截面五分位看单调性。

运行(云服务器 82.156.0.68,lighthouse 用户):
  cd /home/lighthouse/rquant && .venv/bin/python ~/vp_p0/seal_factor_daily.py fetch
  cd /home/lighthouse/rquant && .venv/bin/python ~/vp_p0/seal_factor_daily.py analyze
"""
from __future__ import annotations

import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

DATA = Path.home() / "vp_p0"
PARQUET = DATA / "limit_list.parquet"
START, END = date(2023, 1, 1), date(2026, 7, 28)


def fetch() -> None:
    from rquant.adapter.tushare import TushareAdapter
    from rquant.storage.duckdb import open_readonly_connection

    DATA.mkdir(exist_ok=True)
    conn = open_readonly_connection()
    dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT trade_date FROM daily_bar WHERE trade_date BETWEEN ? AND ? ORDER BY 1",
        [START.isoformat(), END.isoformat()],
    ).fetchall()]
    print(f"目标 {len(dates)} 个交易日")

    done: set[str] = set()
    if PARQUET.exists():
        done = set(pd.read_parquet(PARQUET, columns=["trade_date"])["trade_date"].astype(str))
        print(f"已有 {len(done)} 日,增量续拉")

    adapter = TushareAdapter()
    frames = [pd.read_parquet(PARQUET)] if PARQUET.exists() else []
    new = 0
    for i, d in enumerate(dates):
        ds = str(d)
        if ds in done:
            continue
        try:
            df = adapter.limit_list_by_date(pd.Timestamp(ds).date())
        except Exception as exc:  # noqa: BLE001
            print(f"  {ds} 失败:{exc}", flush=True)
            continue
        if df is not None and not df.empty:
            frames.append(df)
            new += 1
        if new and new % 50 == 0:
            pd.concat(frames, ignore_index=True).to_parquet(PARQUET)
            print(f"  进度 {i+1}/{len(dates)},已存 {new} 新日", flush=True)
        time.sleep(0.35)  # limit_list_d 限频 200 次/分钟,0.35s ≈ 170/min 留余量
    if frames:
        allf = pd.concat(frames, ignore_index=True)
        allf.to_parquet(PARQUET)
        print(f"完成:{len(allf):,} 行,{allf['trade_date'].nunique()} 日 → {PARQUET}")


def _stat(s: pd.Series) -> str:
    s = s.dropna() * 100
    if s.empty:
        return "n=0"
    return f"n={len(s):5d} | 均值 {s.mean():+6.2f}% | 中位 {s.median():+6.2f}% | 胜率 {(s > 0).mean()*100:3.0f}%"


def analyze() -> None:
    from rquant.storage.duckdb import open_readonly_connection

    ll = pd.read_parquet(PARQUET)
    u = ll[ll["limit"] == "U"].copy()
    print(f"涨停样本 {len(u):,} 条,{u['trade_date'].nunique()} 日,"
          f"{u['trade_date'].min()}~{u['trade_date'].max()}")

    conn = open_readonly_connection()
    nxt = conn.execute(
        """
        WITH d AS (
          SELECT ts_code, trade_date, close, open,
                 lead(open)  OVER (PARTITION BY ts_code ORDER BY trade_date) AS open_n,
                 lead(close) OVER (PARTITION BY ts_code ORDER BY trade_date) AS close_n
          FROM daily_bar WHERE trade_date >= '2023-01-01'
        )
        SELECT ts_code, trade_date, close, open_n, close_n FROM d
        """
    ).fetchdf()
    nxt["trade_date"] = nxt["trade_date"].astype(str)
    u["trade_date"] = u["trade_date"].astype(str)
    m = u.merge(nxt, on=["ts_code", "trade_date"], how="inner", suffixes=("", "_bar"))
    m = m.dropna(subset=["open_n", "close_n", "fd_amount"])
    m["ret_gap"] = m["open_n"] / m["close_bar"] - 1          # T+1 开盘溢价
    m["ret_hold"] = m["close_n"] / m["close_bar"] - 1        # T+1 收盘收益
    m["ret_intra"] = m["close_n"] / m["open_n"] - 1          # T+1 开→收
    m["fd_ratio"] = m["fd_amount"] / (m["float_mv"].replace(0, pd.NA))
    m["is_train"] = m["trade_date"] <= "2025-12-31"
    print(f"可对齐样本 {len(m):,}(训练 {int(m['is_train'].sum()):,} / 验证 {int((~m['is_train']).sum()):,})")

    for split, name in [(True, "训练集 ≤2025-12-31"), (False, "验证集 2026+")]:
        s = m[m["is_train"] == split].copy()
        print(f"\n===== {name} (n={len(s):,}) =====")
        s["q"] = s.groupby("trade_date")["fd_ratio"].transform(
            lambda x: pd.qcut(x.rank(method="first"), 5, labels=False, duplicates="drop"))
        for col, cn in [("ret_gap", "T+1 开盘溢价"), ("ret_hold", "T+1 收盘收益"), ("ret_intra", "T+1 开→收")]:
            print(f"  -- {cn} 按封成比(fd_amount/float_mv)五分位 --")
            for q in range(5):
                tag = "(最弱封单)" if q == 0 else "(最强封单)" if q == 4 else ""
                print(f"    Q{q+1}{tag:<8s} {_stat(s.loc[s['q'] == q, col])}")
            q5 = s.loc[s["q"] == 4, col].mean(); q1 = s.loc[s["q"] == 0, col].mean()
            print(f"    → Q5−Q1 = {(q5 - q1)*100:+.2f}pp")
        print("  -- 按开板次数 --")
        for lab, mask in [("一字未开板(open_times=0)", s["open_times"] == 0),
                          ("开板 1 次", s["open_times"] == 1),
                          ("开板 ≥2 次", s["open_times"] >= 2)]:
            print(f"    {lab:22s} 开盘溢价 {_stat(s.loc[mask, 'ret_gap'])}")


if __name__ == "__main__":
    (fetch if (len(sys.argv) > 1 and sys.argv[1] == "fetch") else analyze)()
