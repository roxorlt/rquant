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
from rquant.state import derive_state
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

    # stock_basic 全量拉一次（~5000 行），用于 ST 判断和名字缓存
    df_basic = adapter.stock_basic()
    basic_map = {row["ts_code"]: row["name"] for _, row in df_basic.iterrows()}

    with DuckDBStore() as store:
        n_daily = store.upsert_daily(df_daily)
        n_factor = store.upsert_adj_factor(df_factor)
        n_basic = store.upsert_stock_basic(df_basic)
        logger.info(
            f"入库完成：daily {n_daily} / adj_factor {n_factor} / stock_basic {n_basic}"
        )

        # 基于前复权价全量重算指标（小数据量下简单可靠）
        total_ind = 0
        for code in ts_codes:
            df_qfq = store.get_daily_qfq(code)
            if df_qfq.empty:
                continue
            df_ind = compute_indicators(df_qfq)
            total_ind += store.upsert_indicators(df_ind)
        logger.info(f"指标计算完成：{total_ind} 行")

        # 派生状态字段（用原始价，涨停判断依赖真实 pre_close）
        total_state = 0
        for code in ts_codes:
            df_raw = store.query(
                f"SELECT trade_date, open, high, low, close, pre_close "
                f"FROM daily_bar WHERE ts_code = '{code}' ORDER BY trade_date"
            )
            if df_raw.empty:
                continue
            name = basic_map.get(code)
            df_state = derive_state(df_raw, ts_code=code, name=name)
            total_state += store.upsert_state(df_state)
        logger.info(f"派生状态计算完成：{total_state} 行")

        for code in ts_codes:
            d = store.count_daily(code)
            f = store.count_adj_factor(code)
            i = store.count_indicators(code)
            s = store.count_state(code)
            logger.info(
                f"  {code}: daily {d} / adj_factor {f} / indicator {i} / state {s}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
