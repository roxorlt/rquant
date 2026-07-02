"""全市场日线历史回补：trade_cal → daily / daily_basic / adj_factor → daily_state 重算。

云端 daily pipeline 只覆盖 2024-09 起；策略研究需要 2020 年起的全市场日线。
回补落库后，daily_bar / daily_basic / adj_factor / daily_state / daily_indicator
在 research_sync 中必须是 MERGE 语义（主键冲突云端赢、本地独有历史行保留），
否则下一次日终合并会把回补的历史整表抹掉。
"""

from __future__ import annotations

import time
from datetime import date, datetime
from typing import Protocol

import pandas as pd
from loguru import logger

from rquant.storage.duckdb import DuckDBStore

# Tushare 接口限流：~120ms 间隔（对齐 ingest._API_SLEEP）
_API_SLEEP = 0.15
_REQUESTS_PER_DAY = 3  # daily + daily_basic + adj_factor
_STATE_LOG_EVERY = 500


class MarketDailyAdapter(Protocol):
    def trade_cal(self, start: date, end: date) -> list[date]:
        ...

    def daily_by_date(self, trade_date: date) -> pd.DataFrame:
        ...

    def daily_basic_by_date(self, trade_date: date) -> pd.DataFrame:
        ...

    def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
        ...


def _parse_date(value: str | date) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def backfill_market_daily(
    start_date: str,
    end_date: str,
    store: DuckDBStore,
    adapter: MarketDailyAdapter | None = None,
    *,
    dry_run: bool = False,
    api_sleep: float = _API_SLEEP,
) -> dict:
    """按交易日循环回补全市场 daily_bar / daily_basic / adj_factor。

    单日任一接口失败：warning + 记入 failed_dates 后继续下一日（故障隔离，
    upsert 幂等，事后可对 failed_dates 重跑）。dry_run 只报告交易日数和
    预计请求数，不调数据接口、不写库。
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
        "planned_requests": len(dates) * _REQUESTS_PER_DAY,
        "executed_dates": 0,
        "daily_rows": 0,
        "daily_basic_rows": 0,
        "adj_factor_rows": 0,
        "failed_dates": [],
        "dry_run": dry_run,
    }
    logger.info(
        f"全市场日线回补计划: {start_d} ~ {end_d}, "
        f"交易日 {len(dates)} 个, 预计请求 {summary['planned_requests']} 次, "
        f"dry_run={dry_run}"
    )
    if dry_run:
        return summary

    for i, trading_date in enumerate(dates):
        summary["executed_dates"] += 1
        try:
            df_daily = adapter.daily_by_date(trading_date)
            time.sleep(api_sleep)
            df_basic = adapter.daily_basic_by_date(trading_date)
            time.sleep(api_sleep)
            df_factor = adapter.adj_factor_by_date(trading_date)
            time.sleep(api_sleep)
        except Exception as e:
            summary["failed_dates"].append(trading_date.isoformat())
            logger.warning(f"全市场日线回补单日失败，跳过: date={trading_date} err={e}")
            continue

        daily_rows = store.upsert_daily(df_daily)
        basic_rows = store.upsert_daily_basic(df_basic)
        factor_rows = store.upsert_adj_factor(df_factor)
        summary["daily_rows"] += daily_rows
        summary["daily_basic_rows"] += basic_rows
        summary["adj_factor_rows"] += factor_rows
        logger.info(
            f"{trading_date} 回补完成 ({i + 1}/{len(dates)}): "
            f"daily={daily_rows} basic={basic_rows} factor={factor_rows}"
        )

    logger.info(
        f"全市场日线回补结束: daily={summary['daily_rows']:,} "
        f"basic={summary['daily_basic_rows']:,} factor={summary['adj_factor_rows']:,} "
        f"failed={len(summary['failed_dates'])}"
    )
    return summary


def recompute_daily_state(
    store: DuckDBStore, codes: list[str] | None = None
) -> int:
    """全市场逐只从 daily_bar 全历史重算 daily_state。

    consecutive_limit_ups / is_first_limit_up 是跨日字段，历史回补后必须
    全历史重算——按回补区间增量派生会在区间边界断链。
    """
    from rquant.state import derive_state

    if codes is None:
        codes = [
            r[0]
            for r in store._conn.execute(
                "SELECT DISTINCT ts_code FROM daily_bar ORDER BY ts_code"
            ).fetchall()
        ]
    name_map: dict[str, str] = dict(
        store._conn.execute("SELECT ts_code, name FROM stock_basic").fetchall()
    )

    logger.info(f"daily_state 全量重算: {len(codes)} 只...")
    total = 0
    for i, code in enumerate(codes):
        raw = store._conn.execute(
            "SELECT trade_date, open, high, low, close, pre_close "
            "FROM daily_bar WHERE ts_code = ? ORDER BY trade_date",
            [code],
        ).fetchdf()
        if raw.empty:
            continue
        st = derive_state(raw, ts_code=code, name=name_map.get(code))
        total += store.upsert_state(st)
        if (i + 1) % _STATE_LOG_EVERY == 0:
            logger.info(f"  daily_state 重算进度: {i + 1}/{len(codes)}")

    logger.info(f"daily_state 重算完成: {len(codes)} 只, {total:,} 行")
    return total
