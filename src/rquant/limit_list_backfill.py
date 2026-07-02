"""Tushare 涨跌停/炸板榜（limit_list_d）历史回补 + 当日增量。

历史从 2020 年起（不含 ST），limit_type 空串一次拿齐 U/D/Z，每交易日 1 次
请求。与 limit_up_pool_daily（东财，只有当天数据）互为交叉验证：本表可
事后回补，缺一天不致命。云端 daily pipeline 不管这个表（同东财采集），
本地回补 + capture_today 日终增量落本地主库，经 research_sync 的 MERGE
语义在日终合并时保留。
"""

from __future__ import annotations

import time
from datetime import date, datetime
from typing import Protocol

import pandas as pd
from loguru import logger

from rquant.storage.duckdb import DuckDBStore

# Tushare 接口限流：~120ms 间隔（对齐 market_backfill._API_SLEEP）
_API_SLEEP = 0.15


class LimitListAdapter(Protocol):
    def trade_cal(self, start: date, end: date) -> list[date]:
        ...

    def limit_list_by_date(
        self, trade_date: date, limit_type: str = ""
    ) -> pd.DataFrame:
        ...


def _parse_date(value: str | date) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def backfill_limit_list(
    start_date: str | date,
    end_date: str | date,
    store: DuckDBStore,
    adapter: LimitListAdapter | None = None,
    *,
    dry_run: bool = False,
    api_sleep: float = _API_SLEEP,
) -> dict:
    """按交易日循环回补 limit_list_daily（U/D/Z 一次拿齐）。

    单日失败：warning + 记入 failed_dates 后继续下一日（故障隔离，upsert
    幂等，事后可对 failed_dates 重跑）。dry_run 只报告交易日数和预计请求数，
    不调数据接口、不写库。
    """
    start_d = _parse_date(start_date)
    end_d = _parse_date(end_date)
    if start_d > end_d:
        raise ValueError("start_date must be <= end_date")

    if adapter is None:
        from rquant.adapter.tushare import TushareAdapter

        adapter = TushareAdapter()

    dates = adapter.trade_cal(start_d, end_d)
    summary: dict = {
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "trading_dates_count": len(dates),
        "planned_requests": len(dates),
        "executed_dates": 0,
        "rows": 0,
        "failed_dates": [],
        "dry_run": dry_run,
    }
    logger.info(
        f"涨跌停榜回补计划: {start_d} ~ {end_d}, "
        f"交易日 {len(dates)} 个, 预计请求 {len(dates)} 次, dry_run={dry_run}"
    )
    if dry_run:
        return summary

    for i, trading_date in enumerate(dates):
        summary["executed_dates"] += 1
        try:
            df = adapter.limit_list_by_date(trading_date)
            time.sleep(api_sleep)
        except Exception as e:
            summary["failed_dates"].append(trading_date.isoformat())
            logger.warning(f"涨跌停榜回补单日失败，跳过: date={trading_date} err={e}")
            continue

        rows = store.upsert_limit_list(df)
        summary["rows"] += rows
        logger.info(
            f"{trading_date} 涨跌停榜回补完成 ({i + 1}/{len(dates)}): rows={rows}"
        )

    logger.info(
        f"涨跌停榜回补结束: rows={summary['rows']:,} "
        f"failed={len(summary['failed_dates'])}"
    )
    return summary


def capture_today(
    store: DuckDBStore, adapter: LimitListAdapter | None = None
) -> int:
    """拉当天涨跌停榜落库，返回写入行数（日终增量，脚本接线由 coordinator 做）。

    失败 / 空返回不炸（warning + return 0）：日终链路容忍缺采，且本表可
    事后用 backfill_limit_list 补，不值得触发运维告警。
    """
    if adapter is None:
        from rquant.adapter.tushare import TushareAdapter

        adapter = TushareAdapter()

    today = date.today()
    try:
        df = adapter.limit_list_by_date(today)
    except Exception as e:
        logger.warning(f"当日涨跌停榜拉取失败: date={today} err={e}")
        return 0
    if df.empty:
        logger.warning(f"当日涨跌停榜返回空: date={today}（非交易日 / 数据未出）")
        return 0

    rows = store.upsert_limit_list(df)
    logger.info(f"limit_list_daily 当日增量写入 {rows} 行: date={today}")
    return rows
