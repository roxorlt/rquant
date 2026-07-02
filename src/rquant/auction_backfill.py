"""集合竞价历史回补。"""

from __future__ import annotations

from datetime import date, datetime, time
from typing import Protocol

import pandas as pd
from loguru import logger
from pydantic import BaseModel

from rquant.storage.duckdb import DuckDBStore


class AuctionAdapter(Protocol):
    def stk_auction(self, trade_date: date) -> pd.DataFrame:
        ...


class AuctionBackfillSummary(BaseModel):
    """集合竞价回补摘要。"""

    start_date: str
    end_date: str
    trading_dates_count: int
    planned_requests: int
    executed_requests: int
    failed_requests: int
    rows_written: int
    failed_dates: list[str]
    dry_run: bool = False


class AuctionMinuteFallbackSummary(BaseModel):
    """用 09:30 分钟线合成集合竞价缺行的摘要。"""

    trade_date: str
    minute_rows_count: int
    existing_primary_count: int
    planned_rows: int
    rows_written: int
    dry_run: bool = False


def _parse_date(value: str | date) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _trading_dates(
    store: DuckDBStore,
    start_date: date,
    end_date: date,
) -> list[date]:
    rows = store._conn.execute(
        """
        SELECT DISTINCT trade_date
        FROM daily_bar
        WHERE trade_date >= ?
          AND trade_date <= ?
        ORDER BY trade_date
        """,
        [start_date, end_date],
    ).fetchall()
    return [_parse_date(row[0]) for row in rows]


def synthesize_open_auction_from_minute(
    store: DuckDBStore,
    trade_date: str | date,
    *,
    dry_run: bool = False,
) -> AuctionMinuteFallbackSummary:
    """用 09:30 的 1 分钟 K 线补齐 Tushare 集合竞价缺行。

    该 fallback 只补 `source='tushare'` 主源不存在的股票，避免覆盖真实竞价数据。
    """
    trading_date = _parse_date(trade_date)
    start = datetime.combine(trading_date, time(9, 30))
    end = datetime.combine(trading_date, time(9, 31))
    minute = store._conn.execute(
        """
        SELECT ts_code, trade_time, open, close, vol, amount
        FROM minute_bar
        WHERE trade_time >= ?
          AND trade_time < ?
          AND freq = '1min'
        ORDER BY ts_code
        """,
        [start, end],
    ).fetchdf()
    existing = store._conn.execute(
        """
        SELECT ts_code
        FROM auction_bar
        WHERE trade_date = ?
          AND auction_type = 'open_realtime'
          AND source = 'tushare'
        """,
        [trading_date],
    ).fetchdf()
    existing_codes = set(existing["ts_code"].tolist()) if not existing.empty else set()
    if minute.empty:
        return AuctionMinuteFallbackSummary(
            trade_date=trading_date.isoformat(),
            minute_rows_count=0,
            existing_primary_count=len(existing_codes),
            planned_rows=0,
            rows_written=0,
            dry_run=dry_run,
        )

    fallback = minute[~minute["ts_code"].isin(existing_codes)].copy()
    fallback["price"] = fallback["open"].where(
        fallback["open"].notna(),
        fallback["close"],
    )
    fallback = fallback[fallback["price"].notna()].copy()
    payload = pd.DataFrame({
        "ts_code": fallback["ts_code"],
        "trade_date": trading_date,
        "auction_type": "open_realtime",
        "price": fallback["price"],
        "vol": fallback["vol"],
        "amount": fallback["amount"],
        "turnover_rate": None,
        "volume_ratio": None,
        "source": "minute_0930_fallback",
    })
    rows_written = 0 if dry_run else store.upsert_auction_bars(payload)
    return AuctionMinuteFallbackSummary(
        trade_date=trading_date.isoformat(),
        minute_rows_count=len(minute),
        existing_primary_count=len(existing_codes),
        planned_rows=len(payload),
        rows_written=rows_written,
        dry_run=dry_run,
    )


def backfill_stk_auction(
    store: DuckDBStore,
    adapter: AuctionAdapter,
    *,
    start_date: str | date,
    end_date: str | date,
    dry_run: bool = False,
) -> AuctionBackfillSummary:
    """按本地交易日历回补 Tushare 集合竞价数据。"""
    start_d = _parse_date(start_date)
    end_d = _parse_date(end_date)
    if start_d > end_d:
        raise ValueError("start_date must be <= end_date")

    dates = _trading_dates(store, start_d, end_d)
    logger.info(
        f"集合竞价回补计划: start={start_d} end={end_d} "
        f"dates={len(dates)} dry_run={dry_run}"
    )

    if dry_run:
        return AuctionBackfillSummary(
            start_date=start_d.isoformat(),
            end_date=end_d.isoformat(),
            trading_dates_count=len(dates),
            planned_requests=len(dates),
            executed_requests=0,
            failed_requests=0,
            rows_written=0,
            failed_dates=[],
            dry_run=True,
        )

    executed = 0
    failed = 0
    rows_written = 0
    failed_dates: list[str] = []
    for trading_date in dates:
        executed += 1
        try:
            df = adapter.stk_auction(trading_date)
        except Exception as e:
            failed += 1
            failed_dates.append(trading_date.isoformat())
            logger.error(f"集合竞价回补失败，跳过: date={trading_date} err={e}")
            continue
        if df.empty:
            continue
        rows_written += store.upsert_auction_bars(df)

    return AuctionBackfillSummary(
        start_date=start_d.isoformat(),
        end_date=end_d.isoformat(),
        trading_dates_count=len(dates),
        planned_requests=len(dates),
        executed_requests=executed,
        failed_requests=failed,
        rows_written=rows_written,
        failed_dates=failed_dates,
        dry_run=False,
    )
