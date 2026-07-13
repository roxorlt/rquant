"""Memory-bounded market history backfill and verified PIT state recomputation."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from typing import Literal, Protocol

import pandas as pd
from loguru import logger

from rquant.ingest import _load_daily_state_inputs
from rquant.security_status import (
    DEFAULT_REQUEST_INTERVAL_SECONDS,
    NAMECHANGE_EARLIEST_DATE,
    SHANGHAI,
    DailySecurityKey,
    SecurityStatusAdapter,
    SecurityStatusDaily,
    count_namechange_windows,
    prefetch_namechange_context,
    prefetch_security_status_for_date,
)
from rquant.storage.duckdb import DuckDBStore

_API_SLEEP = 0.35
_REQUESTS_PER_DAY = 3
_STATE_LOG_EVERY = 500
_REQUEST_COUNT_SEMANTICS = (
    "logical_adapter_operations; internal retries are not observable or countable"
)


class MarketDailyAdapter(SecurityStatusAdapter, Protocol):
    def trade_cal(self, start: date, end: date) -> list[date]: ...

    def daily_by_date(self, trade_date: date) -> pd.DataFrame: ...

    def daily_basic_by_date(self, trade_date: date) -> pd.DataFrame: ...

    def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame: ...


class _CountingSecurityStatusAdapter:
    def __init__(
        self,
        source: SecurityStatusAdapter,
        counters: dict[str, int],
    ) -> None:
        self._source = source
        self._counters = counters

    def namechange_raw(
        self,
        start_date: date,
        end_date: date,
        ts_code: str | None = None,
    ) -> pd.DataFrame:
        self._counters["attempted"] += 1
        result = self._source.namechange_raw(start_date, end_date, ts_code)
        self._counters["completed"] += 1
        return result

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        self._counters["attempted"] += 1
        result = self._source.stock_st_raw(trade_date)
        self._counters["completed"] += 1
        return result


def _sync_request_counts(
    summary: dict[str, object],
    counters: dict[str, int],
) -> None:
    summary["attempted_logical_api_operations"] = counters["attempted"]
    summary["completed_logical_api_operations"] = counters["completed"]
    summary["executed_requests"] = counters["attempted"]


def _parse_date(value: str | date) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _filter_prepared_date(frame: pd.DataFrame, trade_date: date) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    if "trade_date" not in frame.columns:
        raise ValueError("prepared market frame is missing trade_date")
    prepared = frame.copy()
    prepared["trade_date"] = pd.to_datetime(prepared["trade_date"]).dt.date
    return prepared.loc[prepared["trade_date"] == trade_date].copy()


def _frame_codes(*frames: pd.DataFrame) -> set[str]:
    return {
        str(code)
        for frame in frames
        if not frame.empty and "ts_code" in frame.columns
        for code in frame["ts_code"].dropna().tolist()
    }


def _existing_status_scope(
    store_factory: Callable[[], DuckDBStore],
    trade_date: date,
) -> tuple[set[DailySecurityKey], set[DailySecurityKey]]:
    with store_factory() as store:
        existing = set(
            store.list_daily_security_keys(trade_date, trade_date)
        )
        incomplete = set(
            store.list_incomplete_stock_status_keys(tuple(existing))
        )
    return existing, incomplete


def _apply_prepared_date(
    store_factory: Callable[[], DuckDBStore],
    *,
    trade_date: date,
    daily: pd.DataFrame,
    daily_basic: pd.DataFrame,
    adj_factor: pd.DataFrame,
    status_rows: Sequence[SecurityStatusDaily],
) -> tuple[int, int, int, int, set[str]]:
    """Atomically replace market facts and invalidate the derived state tail."""
    with store_factory() as store:
        affected_codes = _frame_codes(daily)
        transaction_open = False
        try:
            store._conn.execute("BEGIN")
            transaction_open = True
            daily_count = store.upsert_daily(daily)
            basic_count = store.upsert_daily_basic(daily_basic)
            factor_count = store.upsert_adj_factor(adj_factor)
            status_count = store.upsert_stock_status(
                status_rows,
                transaction_mode="existing",
                require_daily_keys=True,
            )
            if affected_codes:
                ordered_codes = sorted(affected_codes)
                store._conn.execute(
                    """
                    DELETE FROM daily_state
                    WHERE ts_code = ANY(?) AND trade_date >= ?
                    """,
                    [ordered_codes, trade_date],
                )
            store._conn.execute("COMMIT")
            transaction_open = False
        except BaseException as primary:
            if transaction_open:
                try:
                    store._conn.execute("ROLLBACK")
                except BaseException as rollback_error:
                    raise BaseExceptionGroup(
                        "prepared market write and rollback both failed",
                        [primary, rollback_error],
                    ) from None
            raise
    return (
        daily_count,
        basic_count,
        factor_count,
        status_count,
        affected_codes,
    )


def backfill_market_daily(
    start_date: str,
    end_date: str,
    adapter: MarketDailyAdapter | None = None,
    *,
    store_factory: Callable[[], DuckDBStore] = DuckDBStore,
    dry_run: bool = False,
    api_sleep: float = _API_SLEEP,
    status_adapter: SecurityStatusAdapter | None = None,
    status_ingested_at: datetime | None = None,
    status_source_as_of: date | None = None,
    status_namechange_start: date = NAMECHANGE_EARLIEST_DATE,
    status_window_years: int = 3,
    status_request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    """Prepare each date remotely, then apply it in one short writer session."""
    start_d = _parse_date(start_date)
    end_d = _parse_date(end_date)
    if start_d > end_d:
        raise ValueError("start_date must be <= end_date")
    if adapter is None:
        from rquant.adapter.tushare import TushareAdapter

        adapter = TushareAdapter()

    with store_factory() as planning_store:
        incomplete_dates = planning_store.list_incomplete_stock_status_dates(
            start_d,
            end_d,
        )
    counters = {"attempted": 0, "completed": 0}
    counters["attempted"] += 1
    returned_calendar_dates = [
        _parse_date(value) for value in adapter.trade_cal(start_d, end_d)
    ]
    counters["completed"] += 1
    calendar_dates = sorted(
        {day for day in returned_calendar_dates if start_d <= day <= end_d}
    )
    out_of_range_dates = sorted(
        {day for day in returned_calendar_dates if not start_d <= day <= end_d}
    )
    dates = sorted(set(calendar_dates).union(incomplete_dates))
    resolved_ingested_at = status_ingested_at or datetime.now(UTC)
    resolved_source_as_of = (
        status_source_as_of
        or resolved_ingested_at.astimezone(SHANGHAI).date()
    )
    namechange_start = min(status_namechange_start, start_d)
    namechange_operations = (
        count_namechange_windows(
            namechange_start,
            resolved_source_as_of,
            window_years=status_window_years,
        )
        if dates
        else 0
    )
    planned_breakdown = {
        "trade_cal": 1,
        "daily_daily_basic_adj_factor": len(dates) * _REQUESTS_PER_DAY,
        "namechange_windows_upper_bound": namechange_operations,
        "stock_st_dates_upper_bound": len(dates),
    }
    planned_operations = sum(planned_breakdown.values())
    summary: dict[str, object] = {
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "calendar_returned_dates_count": len(returned_calendar_dates),
        "calendar_trading_dates_count": len(calendar_dates),
        "calendar_out_of_range_dates": [
            day.isoformat() for day in out_of_range_dates
        ],
        "existing_incomplete_status_dates_count": len(incomplete_dates),
        "trading_dates_count": len(dates),
        "planned_logical_api_operations": planned_operations,
        "attempted_logical_api_operations": counters["attempted"],
        "completed_logical_api_operations": counters["completed"],
        "request_count_semantics": _REQUEST_COUNT_SEMANTICS,
        "internal_adapter_retries_observable": False,
        "planned_requests": planned_operations,
        "executed_requests": counters["attempted"],
        "executed_requests_semantics": (
            "compatibility alias of attempted_logical_api_operations"
        ),
        "planned_requests_is_upper_bound": True,
        "planned_request_breakdown": planned_breakdown,
        "planned_request_assumptions": (
            "one logical trade_cal operation",
            "date scope is returned calendar dates union existing incomplete status dates",
            "three logical market operations per planned date",
            "namechange windows are fetched once and reused across dates",
            "one logical stock_st operation per date is an upper bound",
            "adapter-internal retries are not observable or countable",
        ),
        "executed_dates": 0,
        "daily_rows": 0,
        "daily_basic_rows": 0,
        "adj_factor_rows": 0,
        "security_status_rows": 0,
        "security_status_unknown_rows": 0,
        "security_status_conflicts": 0,
        "failed_dates": [],
        "affected_codes": [],
        "dry_run": dry_run,
    }
    if dry_run or not dates:
        _sync_request_counts(summary, counters)
        return summary

    resolved_status_adapter = status_adapter or adapter
    counting_status_adapter = _CountingSecurityStatusAdapter(
        resolved_status_adapter,
        counters,
    )
    namechange_context = prefetch_namechange_context(
        counting_status_adapter,
        start=namechange_start,
        source_as_of=resolved_source_as_of,
        window_years=status_window_years,
        request_interval_seconds=status_request_interval_seconds,
        sleep=sleep,
    )
    affected_codes: set[str] = set()

    for index, trading_date in enumerate(dates):
        summary["executed_dates"] += 1
        try:
            existing_keys, incomplete_keys = _existing_status_scope(
                store_factory,
                trading_date,
            )
            counters["attempted"] += 1
            daily_response = adapter.daily_by_date(trading_date)
            counters["completed"] += 1
            df_daily = _filter_prepared_date(daily_response, trading_date)
            sleep(api_sleep)
            counters["attempted"] += 1
            basic_response = adapter.daily_basic_by_date(trading_date)
            counters["completed"] += 1
            df_basic = _filter_prepared_date(basic_response, trading_date)
            sleep(api_sleep)
            counters["attempted"] += 1
            factor_response = adapter.adj_factor_by_date(trading_date)
            counters["completed"] += 1
            df_factor = _filter_prepared_date(factor_response, trading_date)
            sleep(api_sleep)

            complete_keys = existing_keys - incomplete_keys
            fetched_keys = {
                DailySecurityKey(ts_code=code, trade_date=trading_date)
                for code in _frame_codes(df_daily)
            }
            status_keys = sorted(
                incomplete_keys.union(fetched_keys - complete_keys),
                key=lambda key: key.ts_code,
            )
            if status_keys:
                status_batch = prefetch_security_status_for_date(
                    counting_status_adapter,
                    status_keys,
                    namechange_context=namechange_context,
                    ingested_at=resolved_ingested_at,
                    request_interval_seconds=status_request_interval_seconds,
                    sleep=sleep,
                )
                status_rows = status_batch.rows
            else:
                status_rows = ()

            (
                daily_rows,
                basic_rows,
                factor_rows,
                status_rows_count,
                date_affected_codes,
            ) = _apply_prepared_date(
                store_factory,
                trade_date=trading_date,
                daily=df_daily,
                daily_basic=df_basic,
                adj_factor=df_factor,
                status_rows=status_rows,
            )
        except Exception as error:
            _sync_request_counts(summary, counters)
            summary["failed_dates"].append(trading_date.isoformat())
            logger.warning(
                f"全市场日线回补单日失败，跳过: date={trading_date} err={error}"
            )
            continue

        summary["daily_rows"] += daily_rows
        summary["daily_basic_rows"] += basic_rows
        summary["adj_factor_rows"] += factor_rows
        summary["security_status_rows"] += status_rows_count
        summary["security_status_unknown_rows"] += sum(
            row.is_st is None for row in status_rows
        )
        summary["security_status_conflicts"] += sum(
            row.conflict_reason is not None for row in status_rows
        )
        affected_codes.update(date_affected_codes)
        logger.info(
            f"{trading_date} 回补完成 ({index + 1}/{len(dates)}): "
            f"daily={daily_rows} basic={basic_rows} factor={factor_rows} "
            f"status={status_rows_count}"
        )

    summary["affected_codes"] = sorted(affected_codes)
    _sync_request_counts(summary, counters)
    return summary


def recompute_daily_state(
    store: DuckDBStore,
    codes: list[str] | None = None,
    *,
    status_mode: Literal["verified_no_fetch"],
) -> int:
    """Recompute only from already persisted, verified per-date status rows."""
    from rquant.state import derive_state

    if status_mode != "verified_no_fetch":
        raise ValueError("status_mode must be 'verified_no_fetch'")
    if codes is None:
        codes = [
            str(row[0])
            for row in store._conn.execute(
                "SELECT DISTINCT ts_code FROM daily_bar ORDER BY ts_code"
            ).fetchall()
        ]
    else:
        codes = sorted(set(codes))
    if not codes:
        return 0

    missing_status = store._conn.execute(
        """
        SELECT daily.ts_code, daily.trade_date
        FROM daily_bar AS daily
        LEFT JOIN stock_status_daily AS status USING (ts_code, trade_date)
        WHERE daily.ts_code = ANY(?) AND status.ts_code IS NULL
        ORDER BY daily.trade_date DESC, daily.ts_code
        LIMIT 1
        """,
        [codes],
    ).fetchone()
    if missing_status is not None:
        raise RuntimeError(
            "verified status coverage is missing for "
            f"{missing_status[0]} {missing_status[1]}"
        )

    logger.info(f"daily_state 全量重算: {len(codes)} 只...")
    total = 0
    for index, code in enumerate(codes):
        raw, status = _load_daily_state_inputs(store, code)
        if raw.empty:
            continue
        total += store.upsert_state(
            derive_state(raw, ts_code=code, status_daily=status)
        )
        if (index + 1) % _STATE_LOG_EVERY == 0:
            logger.info(f"  daily_state 重算进度: {index + 1}/{len(codes)}")
    logger.info(f"daily_state 重算完成: {len(codes)} 只, {total:,} 行")
    return total
