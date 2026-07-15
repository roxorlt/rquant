"""历史分钟回补：Pool 命中后预拉 N 天分钟线作为盘中研究地基。"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from time import perf_counter
from typing import Protocol

import pandas as pd
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rquant.backfill_manifest import (
    MinuteBackfillTask,
    complete_minute_task_sessions,
)
from rquant.backfill_state import (
    BackfillFailure,
    BackfillStateStore,
    BackfillTaskMetrics,
    StaleTaskClaimError,
)
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


class BackfillRunSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest_id: str
    claimed_tasks: int = Field(ge=0)
    succeeded_tasks: int = Field(ge=0)
    failed_tasks: int = Field(ge=0)
    lost_claim_tasks: int = Field(default=0, ge=0)
    skipped_complete_tasks: int = Field(ge=0)
    request_count: int = Field(ge=0)
    returned_rows: int = Field(ge=0)
    written_rows: int = Field(ge=0)


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
    lookback_days: int = 0,
    dry_run: bool = False,
) -> MinuteReplayBackfillSummary:
    """回补集合竞价跳空候选从信号日到退出窗口的分钟线。

    lookback_days > 0 时窗口起点向前扩 N 个交易日（信号日前的历史分钟，
    供「同分钟历史放量/累计成交额分位」类特征计算；20 天 lookback + 持有窗
    约 22 天 × 241 根 ≈ 5300 行，仍在 stk_mins 单请求 8000 行上限内）。
    """
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
        fetch_start = window_dates[0]
        if lookback_days > 0:
            earlier = [d for d in calendar if d < signal_date]
            fetch_start = earlier[-lookback_days] if len(earlier) >= lookback_days else (
                earlier[0] if earlier else fetch_start
            )
        requests.append((
            str(row["ts_code"]),
            _dt(fetch_start, time(9, 30)),
            _dt(window_dates[-1], time(15, 0)),
        ))

    logger.info(
        f"集合竞价分钟 replay 回补计划: start={start_d} end={end_d} "
        f"candidates={len(candidates)} requests={len(requests)} hold={max_hold_days} "
        f"lookback={lookback_days} freq={freq} dry_run={dry_run}"
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


@dataclass(frozen=True)
class _FetchOutcome:
    frames: tuple[pd.DataFrame, ...]
    request_count: int
    returned_rows: int


class _BackfillTaskError(RuntimeError):
    def __init__(
        self,
        failure: BackfillFailure,
        *,
        request_count: int = 0,
        returned_rows: int = 0,
    ) -> None:
        self.failure = failure
        self.request_count = request_count
        self.returned_rows = returned_rows
        super().__init__(failure.message)


def _fetch_minute_dates(
    adapter: IntradayAdapter,
    task: MinuteBackfillTask,
    open_dates: tuple[date, ...],
) -> _FetchOutcome:
    start = _dt(open_dates[0], time(9, 30))
    end = _dt(open_dates[-1], time(15, 0))
    try:
        frame = adapter.stk_mins(task.ts_code, task.freq, start, end)
    except Exception as exc:
        raise _BackfillTaskError(
            BackfillFailure(
                code="source_error",
                message=f"Tushare stk_mins request failed: {exc}",
                retryable=True,
                details={
                    "ts_code": task.ts_code,
                    "start_date": open_dates[0].isoformat(),
                    "end_date": open_dates[-1].isoformat(),
                },
            ),
            request_count=1,
        ) from exc
    if frame is None:
        frame = pd.DataFrame()
    returned_rows = len(frame)
    if returned_rows >= task.response_row_limit:
        if len(open_dates) == 1:
            raise _BackfillTaskError(
                BackfillFailure(
                    code="source_truncated",
                    message="single-session response reached the provider row limit",
                    retryable=True,
                    details={
                        "ts_code": task.ts_code,
                        "trade_date": open_dates[0].isoformat(),
                        "returned_rows": returned_rows,
                        "row_limit": task.response_row_limit,
                    },
                ),
                request_count=1,
                returned_rows=returned_rows,
            )
        midpoint = len(open_dates) // 2
        try:
            left = _fetch_minute_dates(adapter, task, open_dates[:midpoint])
        except _BackfillTaskError as exc:
            raise _BackfillTaskError(
                exc.failure,
                request_count=1 + exc.request_count,
                returned_rows=returned_rows + exc.returned_rows,
            ) from exc
        try:
            right = _fetch_minute_dates(adapter, task, open_dates[midpoint:])
        except _BackfillTaskError as exc:
            raise _BackfillTaskError(
                exc.failure,
                request_count=1 + left.request_count + exc.request_count,
                returned_rows=(
                    returned_rows + left.returned_rows + exc.returned_rows
                ),
            ) from exc
        return _FetchOutcome(
            frames=(*left.frames, *right.frames),
            request_count=1 + left.request_count + right.request_count,
            returned_rows=returned_rows + left.returned_rows + right.returned_rows,
        )
    if frame.empty:
        return _FetchOutcome(frames=(), request_count=1, returned_rows=0)

    required = {
        "ts_code",
        "trade_time",
        "freq",
        "open",
        "high",
        "low",
        "close",
        "vol",
        "amount",
        "source",
    }
    missing_columns = required - set(frame.columns)
    if missing_columns:
        raise _BackfillTaskError(
            BackfillFailure(
                code="invalid_source_payload",
                message="minute response is missing required columns",
                retryable=False,
                details={"missing_columns": sorted(missing_columns)},
            ),
            request_count=1,
            returned_rows=returned_rows,
        )
    normalized = frame.copy()
    normalized["trade_time"] = pd.to_datetime(normalized["trade_time"])
    returned_dates = set(normalized["trade_time"].dt.date.tolist())
    invalid_identity = (
        set(normalized["ts_code"].astype(str)) != {task.ts_code}
        or set(normalized["freq"].astype(str)) != {task.freq}
        or set(normalized["source"].astype(str)) != {task.source}
        or not returned_dates.issubset(set(open_dates))
    )
    if invalid_identity:
        raise _BackfillTaskError(
            BackfillFailure(
                code="invalid_source_payload",
                message="minute response identity is outside the claimed task scope",
                retryable=False,
                details={
                    "ts_code": task.ts_code,
                    "start_date": open_dates[0].isoformat(),
                    "end_date": open_dates[-1].isoformat(),
                },
            ),
            request_count=1,
            returned_rows=returned_rows,
        )
    return _FetchOutcome(
        frames=(normalized,),
        request_count=1,
        returned_rows=returned_rows,
    )


def _missing_date_groups(
    task: MinuteBackfillTask,
    complete_dates: set[date],
) -> tuple[tuple[date, ...], ...]:
    groups: list[list[date]] = []
    for trading_date in task.open_dates:
        if trading_date in complete_dates:
            continue
        if not groups or task.open_dates.index(trading_date) != (
            task.open_dates.index(groups[-1][-1]) + 1
        ):
            groups.append([trading_date])
        else:
            groups[-1].append(trading_date)
    return tuple(tuple(group) for group in groups)


def _allowed_missing_dates(
    store: DuckDBStore,
    task: MinuteBackfillTask,
    missing_dates: set[date],
) -> dict[date, str]:
    if not missing_dates:
        return {}
    row = store._conn.execute(
        "SELECT list_date FROM stock_basic WHERE ts_code = ?",
        [task.ts_code],
    ).fetchone()
    if row is None or row[0] is None:
        return {}
    list_date = _parse_date(row[0])
    return {
        trading_date: "not_listed"
        for trading_date in missing_dates
        if trading_date < list_date
    }


def run_backfill_manifest(
    store: DuckDBStore | None,
    state: BackfillStateStore,
    adapter: IntradayAdapter,
    *,
    manifest_id: str,
    worker_id: str,
    retry_failed: bool = False,
    lease_seconds: int = 1_800,
    stop_before: datetime | None = None,
    store_factory: Callable[[], AbstractContextManager[DuckDBStore]] | None = None,
) -> BackfillRunSummary:
    """Run every currently claimable task once, continuing after task failures."""
    if (store is None) == (store_factory is None):
        raise ValueError("provide exactly one of store or store_factory")

    def open_task_store() -> AbstractContextManager[DuckDBStore]:
        if store_factory is not None:
            return store_factory()
        assert store is not None
        return nullcontext(store)

    if stop_before is not None:
        if stop_before.tzinfo is None or stop_before.utcoffset() is None:
            raise ValueError("stop_before must be timezone-aware")
        stop_before = stop_before.astimezone(UTC)
    last_ordinal = -1
    claimed_tasks = 0
    succeeded_tasks = 0
    failed_tasks = 0
    lost_claim_tasks = 0
    skipped_complete_tasks = 0
    request_count = 0
    returned_rows = 0
    written_rows = 0

    while True:
        if stop_before is not None and datetime.now(UTC) >= stop_before:
            logger.info(
                f"backfill manifest {manifest_id} paused before runtime deadline"
            )
            break
        claim = state.claim_task(
            manifest_id,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            retry_failed=retry_failed,
            after_ordinal=last_ordinal,
        )
        if claim is None:
            break
        last_ordinal = claim.ordinal
        claimed_tasks += 1
        started = perf_counter()
        task_requests = 0
        task_returned = 0
        task_written = 0
        try:
            task = MinuteBackfillTask.model_validate(claim.payload)
            with open_task_store() as task_store:
                complete_before = complete_minute_task_sessions(task_store, task)
            if complete_before == set(task.open_dates):
                metrics = BackfillTaskMetrics(
                    covered_sessions=len(complete_before),
                )
                state.mark_task_succeeded(
                    claim,
                    duration_seconds=perf_counter() - started,
                    metrics=metrics,
                )
                skipped_complete_tasks += 1
                succeeded_tasks += 1
                continue
            if claim.recovery_only:
                state.mark_task_failed(
                    claim,
                    failure=BackfillFailure(
                        code="lease_expired",
                        message=(
                            "final attempt expired and persisted minute rows remain "
                            "incomplete after recovery verification"
                        ),
                        retryable=False,
                        details={
                            "ts_code": task.ts_code,
                            "missing_dates": sorted(
                                value.isoformat()
                                for value in set(task.open_dates) - complete_before
                            ),
                        },
                    ),
                    metrics=BackfillTaskMetrics(
                        covered_sessions=len(complete_before),
                    ),
                )
                failed_tasks += 1
                continue

            outcomes: list[_FetchOutcome] = []
            for group in _missing_date_groups(task, complete_before):
                outcome = _fetch_minute_dates(adapter, task, group)
                outcomes.append(outcome)
                task_requests += outcome.request_count
                task_returned += outcome.returned_rows
            frames = [frame for outcome in outcomes for frame in outcome.frames]
            try:
                claim = state.renew_task_claim(
                    claim,
                    lease_seconds=lease_seconds,
                )
            except StaleTaskClaimError:
                lost_claim_tasks += 1
                logger.warning(
                    f"discarding late source rows after claim loss: {claim.task_id}"
                )
                request_count += task_requests
                returned_rows += task_returned
                continue
            with open_task_store() as task_store:
                complete_before_write = complete_minute_task_sessions(
                    task_store,
                    task,
                )
                if frames and complete_before_write != set(task.open_dates):
                    task_written = task_store.upsert_minute_bars(
                        pd.concat(frames, ignore_index=True)
                    )

                complete_after = complete_minute_task_sessions(task_store, task)
                missing_after = set(task.open_dates) - complete_after
                allowed = _allowed_missing_dates(
                    task_store,
                    task,
                    missing_after,
                )
            unresolved = missing_after - set(allowed)
            metrics = BackfillTaskMetrics(
                request_count=task_requests,
                returned_rows=task_returned,
                written_rows=task_written,
                covered_sessions=len(complete_after),
                allowed_missing_sessions=len(allowed),
            )
            if unresolved:
                failure_code = "source_empty" if task_returned == 0 else "incomplete_session"
                failure_message = (
                    "minute source returned no rows for required sessions"
                    if task_returned == 0
                    else "written minute rows do not form complete required sessions"
                )
                state.mark_task_failed(
                    claim,
                    failure=BackfillFailure(
                        code=failure_code,
                        message=failure_message,
                        retryable=True,
                        details={
                            "ts_code": task.ts_code,
                            "missing_dates": sorted(
                                value.isoformat() for value in unresolved
                            ),
                        },
                    ),
                    metrics=metrics,
                )
                failed_tasks += 1
            else:
                state.mark_task_succeeded(
                    claim,
                    duration_seconds=perf_counter() - started,
                    metrics=metrics,
                )
                succeeded_tasks += 1
        except ValidationError as exc:
            state.mark_task_failed(
                claim,
                failure=BackfillFailure(
                    code="invalid_task_payload",
                    message="persisted task payload does not match the runner contract",
                    retryable=False,
                    details={"validation_error": str(exc)},
                ),
            )
            failed_tasks += 1
        except _BackfillTaskError as exc:
            task_requests += exc.request_count
            task_returned += exc.returned_rows
            state.mark_task_failed(
                claim,
                failure=exc.failure,
                metrics=BackfillTaskMetrics(
                    request_count=task_requests,
                    returned_rows=task_returned,
                    written_rows=task_written,
                ),
            )
            failed_tasks += 1
        except StaleTaskClaimError:
            lost_claim_tasks += 1
            logger.warning(
                f"task claim was lost before state completion: {claim.task_id}"
            )
        except Exception as exc:
            state.mark_task_failed(
                claim,
                failure=BackfillFailure(
                    code="task_execution_error",
                    message="unexpected task execution failure",
                    retryable=True,
                    details={
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                ),
                metrics=BackfillTaskMetrics(
                    request_count=task_requests,
                    returned_rows=task_returned,
                    written_rows=task_written,
                ),
            )
            failed_tasks += 1
        request_count += task_requests
        returned_rows += task_returned
        written_rows += task_written

    return BackfillRunSummary(
        manifest_id=manifest_id,
        claimed_tasks=claimed_tasks,
        succeeded_tasks=succeeded_tasks,
        failed_tasks=failed_tasks,
        lost_claim_tasks=lost_claim_tasks,
        skipped_complete_tasks=skipped_complete_tasks,
        request_count=request_count,
        returned_rows=returned_rows,
        written_rows=written_rows,
    )
