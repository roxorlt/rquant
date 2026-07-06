#!/usr/bin/env python
"""盘中累计成交额进度曲线标定（一次性交付物）。

从**本地只读副本** minute_bar（1min）算市场级累计成交额进度曲线：每股每日
归一化（t 时刻累计额 / 全日额），先股内取中位、再跨股取中位，输出 241 点单调
序列 → ``src/rquant/data/intraday_progress_curve.json``（进 git，随代码到云端）。

surge-watch 启动时 ``load_progress_curve()`` 加载；文件缺失 → 线性兜底 + warning。

口径：只取当日恰好 241 根 1min bar 的干净 (股, 日)（滤掉重复源/异常根），按落库
顺序即 09:30..11:30 + 13:01..15:00 规范网格；用 DuckDB 侧聚合（median 两级）避免
把千万级分钟行拉进内存。

用法：
    PYTHONPATH=$PWD/src .venv/bin/python scripts/calibrate-intraday-curve.py
    # 可选 --freq 1min --source tushare --out <path>
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from rquant.storage.duckdb import open_readonly_store
from rquant.surge_watch import CURVE_POINTS, DEFAULT_CURVE_FILENAME


def _default_out() -> Path:
    root = Path(__file__).resolve().parent.parent
    return root / "src" / "rquant" / "data" / DEFAULT_CURVE_FILENAME


def calibrate(freq: str = "1min", source: str = "tushare") -> tuple[np.ndarray, int]:
    """返回 (241 点曲线, 参与样本股数)。两级中位在 DuckDB 侧算。"""
    store = open_readonly_store(required_tables=("minute_bar",))
    try:
        # 只取当日恰好 241 根的干净 (股, 日)（滤重复源/异常根），落库顺序即规范网格
        clean_days = store._conn.execute(
            """
            SELECT ts_code, CAST(trade_time AS DATE) AS d
            FROM minute_bar
            WHERE freq = ? AND source = ?
            GROUP BY ts_code, d
            HAVING COUNT(*) = ?
            """,
            [freq, source, CURVE_POINTS],
        ).fetchdf()
        if clean_days.empty:
            raise RuntimeError("minute_bar 无干净 241 根 (股,日) 样本，无法标定")
        sample_codes = int(clean_days["ts_code"].nunique())

        # 先股内跨日取中位（per_stock），再跨股取中位（final）——两级 median
        rows = store._conn.execute(
            """
            WITH clean AS (
              SELECT ts_code, CAST(trade_time AS DATE) AS d
              FROM minute_bar
              WHERE freq = ? AND source = ?
              GROUP BY ts_code, d
              HAVING COUNT(*) = ?
            ),
            bars AS (
              SELECT m.ts_code, CAST(m.trade_time AS DATE) AS d, m.trade_time, m.amount
              FROM minute_bar m
              JOIN clean c ON m.ts_code = c.ts_code AND CAST(m.trade_time AS DATE) = c.d
              WHERE m.freq = ? AND m.source = ?
            ),
            seq AS (
              SELECT ts_code, d,
                     ROW_NUMBER() OVER (PARTITION BY ts_code, d ORDER BY trade_time) - 1 AS midx,
                     SUM(amount) OVER (PARTITION BY ts_code, d ORDER BY trade_time) AS cum,
                     SUM(amount) OVER (PARTITION BY ts_code, d) AS total
              FROM bars
            ),
            norm AS (
              SELECT ts_code, midx, cum / total AS frac FROM seq WHERE total > 0
            ),
            per_stock AS (
              SELECT ts_code, midx, median(frac) AS m FROM norm GROUP BY ts_code, midx
            )
            SELECT midx, median(m) AS curve FROM per_stock GROUP BY midx ORDER BY midx
            """,
            [freq, source, CURVE_POINTS, freq, source],
        ).fetchall()
    finally:
        store.close()

    if len(rows) != CURVE_POINTS:
        raise RuntimeError(f"曲线点数 {len(rows)} ≠ {CURVE_POINTS}，样本网格异常")
    curve = np.array([float(r[1]) for r in rows], dtype="float64")
    curve = np.maximum.accumulate(curve)  # fp 噪声防御，强制单调不减
    if curve[-1] > 0:
        curve = curve / curve[-1]  # 末点归一到 1
    return curve, sample_codes


def _sanity(curve: np.ndarray) -> None:
    assert len(curve) == CURVE_POINTS, f"点数 {len(curve)} ≠ {CURVE_POINTS}"
    diffs = np.diff(curve)
    assert (diffs >= -1e-9).all(), "曲线非单调不减"
    assert curve[0] >= 0, "首值应 ≥ 0"
    assert curve[0] < 0.2, f"首值 {curve[0]:.4f} 偏大（应 ≈0）"
    assert abs(curve[-1] - 1.0) < 1e-9, f"尾值 {curve[-1]:.6f} ≠ 1"


def main() -> int:
    ap = argparse.ArgumentParser(description="标定盘中累计成交额进度曲线")
    ap.add_argument("--freq", default="1min")
    ap.add_argument("--source", default="tushare")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    curve, sample_codes = calibrate(freq=args.freq, source=args.source)
    _sanity(curve)

    out = args.out or _default_out()
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "v1",
        "points": [round(float(x), 8) for x in curve],
        "grid_points": CURVE_POINTS,
        "sample_codes": sample_codes,
        "freq": args.freq,
        "source": args.source,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "note": "市场级累计成交额进度曲线（股内中位→跨股中位）；surge-watch 粗筛用",
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    mid = CURVE_POINTS // 2
    print(f"标定完成 → {out}")
    print(f"样本股数: {sample_codes}")
    print(f"首/中/尾: {curve[0]:.6f} / {curve[mid]:.6f} / {curve[-1]:.6f}")
    print("sanity: 241 点单调不减 ✓ 首≈0 ✓ 尾=1 ✓")
    return 0


if __name__ == "__main__":
    sys.exit(main())
