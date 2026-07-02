"""历史分钟回补：Pool 命中后预拉 N 天分钟线作为盘中研究地基。"""

from __future__ import annotations

from datetime import date, datetime, time
from typing import Protocol

import pandas as pd
from loguru import logger
from pydantic import BaseModel

from rquant.storage.duckdb import DuckDBStore


class IntradayAdapter(Protocol):
    def stk_mins(
        self,
        ts_code: str,
        freq: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        ...


class MinuteBackfillSummary(BaseModel):
    """历史分钟回补摘要。"""

    screen_date: str
    preset_name: str
    lookback_days: int
    freq: str
    codes_count: int
    planned_requests: int
    executed_requests: int
    failed_requests: int
    rows_written: int
    dry_run: bool = False


class MinuteReplayBackfillSummary(BaseModel):
    """分钟 replay 窗口回补摘要。"""

    start_date: str
    end_date: str
    preset_name: str
    max_hold_days: int
    freq: str
    candidates_count: int
    planned_requests: int
    executed_requests: int
    failed_requests: int
    rows_written: int
    dry_run: bool = False


def _parse_date(value: str | date) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _dt(day: date, hhmm: time) -> datetime:
    return datetime.combine(day, hhmm)


def _trading_dates_for_lookback(
    store: DuckDBStore,
    screen_date: date,
    lookback_days: int,
) -> list[date]:
    rows = store._conn.execute(
        """
        SELECT DISTINCT trade_date
        FROM daily_bar
        WHERE trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        [screen_date, lookback_days],
    ).fetchall()
    dates = [row[0] for row in rows]
    return sorted(dates)


def _chunk_dates(dates: list[date], *, chunk_size: int = 30) -> list[list[date]]:
    return [dates[i:i + chunk_size] for i in range(0, len(dates), chunk_size)]


def _trading_calendar(store: DuckDBStore) -> list[date]:
    rows = store._conn.execute(
        """
        SELECT DISTINCT trade_date
        FROM daily_bar
        ORDER BY trade_date
        """
    ).fetchall()
    return [_parse_date(row[0]) for row in rows]


def _next_trading_date(calendar: list[date], current: date) -> date | None:
    for trading_date in calendar:
        if trading_date > current:
            return trading_date
    return None


def _window_trading_dates(
    calendar: list[date],
    start: date,
    max_hold_days: int,
) -> list[date]:
    return [
        trading_date for trading_date in calendar if trading_date >= start
    ][: max_hold_days + 1]


def _pool_codes(
    store: DuckDBStore,
    screen_date: date,
    preset_name: str,
    ts_code: str | None,
) -> list[str]:
    if ts_code:
        return [ts_code]

    df = store._conn.execute(
        """
        SELECT ts_code
        FROM screen_result
        WHERE trade_date = ?
          AND preset_name = ?
        ORDER BY ts_code
        """,
        [screen_date, preset_name],
    ).fetchdf()
    return df["ts_code"].tolist()


def backfill_pool1_minute_context(
    store: DuckDBStore,
    adapter: IntradayAdapter,
    *,
    screen_date: str | date,
    lookback_days: int = 30,
    freq: str = "1min",
    preset_name: str = "n-shape-pool1",
    ts_code: str | None = None,
    dry_run: bool = False,
) -> MinuteBackfillSummary:
    """回补 Pool 命中标的的历史分钟线。

    默认在 17:00 Pool1 出结果后使用：对每个 Pool1 标的拉取 `screen_date`
    向前 `lookback_days` 个交易日的分钟线，写入 `minute_bar`。
    """
    screen_d = _parse_date(screen_date)
    dates = _trading_dates_for_lookback(store, screen_d, lookback_days)
    if not dates:
        logger.warning(f"{screen_d} 无交易日历，跳过分钟回补")
        return MinuteBackfillSummary(
            screen_date=screen_d.isoformat(),
            preset_name=preset_name,
            lookback_days=lookback_days,
            freq=freq,
            codes_count=0,
            planned_requests=0,
            executed_requests=0,
            failed_requests=0,
            rows_written=0,
            dry_run=dry_run,
        )

    codes = _pool_codes(store, screen_d, preset_name, ts_code)
    chunks = _chunk_dates(dates, chunk_size=30)
    planned = len(codes) * len(chunks)
    executed = 0
    failed = 0
    rows_written = 0

    logger.info(
        f"分钟回补计划: date={screen_d} preset={preset_name} "
        f"codes={len(codes)} lookback={lookback_days} chunks={len(chunks)} "
        f"freq={freq} dry_run={dry_run}"
    )

    if dry_run:
        return MinuteBackfillSummary(
            screen_date=screen_d.isoformat(),
            preset_name=preset_name,
            lookback_days=lookback_days,
            freq=freq,
            codes_count=len(codes),
            planned_requests=planned,
            executed_requests=0,
            failed_requests=0,
            rows_written=0,
            dry_run=True,
        )

    for code in codes:
        for chunk in chunks:
            start = _dt(chunk[0], time(9, 30))
            end = _dt(chunk[-1], time(15, 0))
            executed += 1
            try:
                df = adapter.stk_mins(code, freq, start, end)
            except Exception as e:
                failed += 1
                logger.error(
                    f"分钟上下文回补失败，跳过: code={code} "
                    f"start={start} end={end} err={e}"
                )
                continue
            if df.empty:
                continue
            rows_written += store.upsert_minute_bars(df)

    return MinuteBackfillSummary(
        screen_date=screen_d.isoformat(),
        preset_name=preset_name,
        lookback_days=lookback_days,
        freq=freq,
        codes_count=len(codes),
        planned_requests=planned,
        executed_requests=executed,
        failed_requests=failed,
        rows_written=rows_written,
        dry_run=False,
    )


def backfill_minute_replay_window(
    store: DuckDBStore,
    adapter: IntradayAdapter,
    *,
    start_date: str | date,
    end_date: str | date,
    max_hold_days: int = 5,
    freq: str = "1min",
    preset_name: str = "n-shape-pool1",
    ts_code: str | None = None,
    dry_run: bool = False,
) -> MinuteReplayBackfillSummary:
    """回补分钟 replay 所需的 B 日到退出窗口分钟线。"""
    start_d = _parse_date(start_date)
    end_d = _parse_date(end_date)
    calendar = _trading_calendar(store)
    where_extra = "AND ts_code = ?" if ts_code else ""
    params: list[object] = [start_d, end_d, preset_name]
    if ts_code:
        params.append(ts_code)

    candidates = store._conn.execute(
        f"""
        SELECT trade_date, ts_code
        FROM screen_result
        WHERE trade_date >= ?
          AND trade_date <= ?
          AND preset_name = ?
          {where_extra}
        ORDER BY trade_date, ts_code
        """,
        params,
    ).fetchdf()

    requests: list[tuple[str, datetime, datetime]] = []
    for _, row in candidates.iterrows():
        t_date = _parse_date(row["trade_date"])
        buy_date = _next_trading_date(calendar, t_date)
        if buy_date is None:
            continue
        window_dates = _window_trading_dates(calendar, buy_date, max_hold_days)
        if len(window_dates) <= 1:
            continue
        requests.append((
            str(row["ts_code"]),
            _dt(window_dates[0], time(9, 30)),
            _dt(window_dates[-1], time(15, 0)),
        ))

    logger.info(
        f"分钟 replay 回补计划: start={start_d} end={end_d} "
        f"preset={preset_name} candidates={len(candidates)} "
        f"requests={len(requests)} hold={max_hold_days} "
        f"freq={freq} dry_run={dry_run}"
    )

    if dry_run:
        return MinuteReplayBackfillSummary(
            start_date=start_d.isoformat(),
            end_date=end_d.isoformat(),
            preset_name=preset_name,
            max_hold_days=max_hold_days,
            freq=freq,
            candidates_count=len(candidates),
            planned_requests=len(requests),
            executed_requests=0,
            failed_requests=0,
            rows_written=0,
            dry_run=True,
        )

    executed = 0
    failed = 0
    rows_written = 0
    for code, start_dt, end_dt in requests:
        executed += 1
        try:
            df = adapter.stk_mins(code, freq, start_dt, end_dt)
        except Exception as e:
            failed += 1
            logger.error(
                f"分钟 replay 回补失败，跳过: code={code} "
                f"start={start_dt} end={end_dt} err={e}"
            )
            continue
        if df.empty:
            continue
        rows_written += store.upsert_minute_bars(df)

    return MinuteReplayBackfillSummary(
        start_date=start_d.isoformat(),
        end_date=end_d.isoformat(),
        preset_name=preset_name,
        max_hold_days=max_hold_days,
        freq=freq,
        candidates_count=len(candidates),
        planned_requests=len(requests),
        executed_requests=executed,
        failed_requests=failed,
        rows_written=rows_written,
        dry_run=False,
    )


def backfill_auction_gap_minute_replay_window(
    store: DuckDBStore,
    adapter: IntradayAdapter,
    *,
    start_date: str | date,
    end_date: str | date,
    max_hold_days: int = 1,
    freq: str = "1min",
    gap_mode: str = "close",
    st_filter: str = "case_insensitive",
    min_ratio: float = 0.15,
    max_ratio: float = 5.0,
    ts_code: str | None = None,
    dry_run: bool = False,
) -> MinuteReplayBackfillSummary:
    """回补集合竞价跳空候选从信号日到退出窗口的分钟线。"""
    from rquant.auction_gap_strategy import AuctionGapConfig, run_auction_gap_replay

    start_d = _parse_date(start_date)
    end_d = _parse_date(end_date)
    candidates = run_auction_gap_replay(
        store,
        AuctionGapConfig(
            start_date=start_d.isoformat(),
            end_date=end_d.isoformat(),
            gap_mode=gap_mode,  # type: ignore[arg-type]
            min_auction_vol_ratio_5d=min_ratio,
            max_auction_vol_ratio_5d=max_ratio,
            st_filter=st_filter,  # type: ignore[arg-type]
        ),
    )
    if ts_code:
        candidates = candidates[candidates["ts_code"] == ts_code].copy()

    calendar = _trading_calendar(store)
    requests: list[tuple[str, datetime, datetime]] = []
    for _, row in candidates.iterrows():
        signal_date = _parse_date(row["signal_date"])
        window_dates = _window_trading_dates(calendar, signal_date, max_hold_days)
        if len(window_dates) <= 1:
            continue
        requests.append((
            str(row["ts_code"]),
            _dt(window_dates[0], time(9, 30)),
            _dt(window_dates[-1], time(15, 0)),
        ))

    logger.info(
        f"集合竞价分钟 replay 回补计划: start={start_d} end={end_d} "
        f"candidates={len(candidates)} requests={len(requests)} hold={max_hold_days} "
        f"freq={freq} dry_run={dry_run}"
    )

    if dry_run:
        return MinuteReplayBackfillSummary(
            start_date=start_d.isoformat(),
            end_date=end_d.isoformat(),
            preset_name="auction-gap",
            max_hold_days=max_hold_days,
            freq=freq,
            candidates_count=len(candidates),
            planned_requests=len(requests),
            executed_requests=0,
            failed_requests=0,
            rows_written=0,
            dry_run=True,
        )

    executed = 0
    failed = 0
    rows_written = 0
    for code, start_dt, end_dt in requests:
        executed += 1
        try:
            df = adapter.stk_mins(code, freq, start_dt, end_dt)
        except Exception as e:
            failed += 1
            logger.error(
                f"集合竞价分钟 replay 回补失败，跳过: code={code} "
                f"start={start_dt} end={end_dt} err={e}"
            )
            continue
        if df.empty:
            continue
        rows_written += store.upsert_minute_bars(df)

    return MinuteReplayBackfillSummary(
        start_date=start_d.isoformat(),
        end_date=end_d.isoformat(),
        preset_name="auction-gap",
        max_hold_days=max_hold_days,
        freq=freq,
        candidates_count=len(candidates),
        planned_requests=len(requests),
        executed_requests=executed,
        failed_requests=failed,
        rows_written=rows_written,
        dry_run=False,
    )
