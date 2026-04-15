"""一次性拉取历史日线入库。

示例：
    uv run python scripts/ingest_daily.py \\
        --codes 000001.SZ,600519.SH --start 2024-01-01 --end 2024-12-31
"""

from __future__ import annotations

import argparse
from datetime import date, datetime

from loguru import logger

from rquant.adapter import TushareAdapter
from rquant.indicator import compute_indicators
from rquant.logging import setup_logging
from rquant.storage import DuckDBStore


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest daily OHLCV from Tushare")
    parser.add_argument(
        "--codes",
        required=True,
        help="Tushare ts_code 列表，逗号分隔，如 000001.SZ,600519.SH",
    )
    parser.add_argument("--start", type=parse_date, required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", type=parse_date, required=True, help="结束日期 YYYY-MM-DD")
    args = parser.parse_args()

    setup_logging()

    ts_codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    logger.info(f"开始拉取 {len(ts_codes)} 只股票的日线 [{args.start} → {args.end}]")

    adapter = TushareAdapter()
    df_daily = adapter.daily(ts_codes=ts_codes, start=args.start, end=args.end)

    if df_daily.empty:
        logger.warning("daily 无数据返回，退出")
        return 1

    df_factor = adapter.adj_factor(ts_codes=ts_codes, start=args.start, end=args.end)

    with DuckDBStore() as store:
        n_daily = store.upsert_daily(df_daily)
        n_factor = store.upsert_adj_factor(df_factor)
        logger.info(f"入库完成：daily {n_daily} 行 + adj_factor {n_factor} 行")

        # 基于前复权价全量重算指标（小数据量下简单可靠）
        total_ind = 0
        for code in ts_codes:
            df_qfq = store.get_daily_qfq(code)
            if df_qfq.empty:
                continue
            df_ind = compute_indicators(df_qfq)
            total_ind += store.upsert_indicators(df_ind)
        logger.info(f"指标计算完成：{total_ind} 行")

        for code in ts_codes:
            d = store.count_daily(code)
            f = store.count_adj_factor(code)
            i = store.count_indicators(code)
            logger.info(f"  {code}: daily {d} / adj_factor {f} / indicator {i}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
