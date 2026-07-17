"""Derive daily technical indicators from local point-in-time price facts."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import BaseModel, Field

from rquant.indicator import compute_indicators
from rquant.storage.duckdb import DuckDBStore

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_PROTECTED_START = dtime(9, 15)
_PROTECTED_END = dtime(15, 10)
_WRITE_MARGIN = timedelta(seconds=60)
_INDICATOR_COLUMNS = [
    "ts_code",
    "trade_date",
    "ma5",
    "ma10",
    "ma20",
    "ma60",
    "rsi6",
    "rsi14",
    "macd",
    "macd_signal",
    "macd_hist",
    "kdj_k",
    "kdj_d",
    "kdj_j",
]
_PRICE_INDICATOR_COLUMNS = [
    "ma5",
    "ma10",
    "ma20",
    "ma60",
    "macd",
    "macd_signal",
    "macd_hist",
]


class DailyIndicatorBackfillResult(BaseModel):
    code_count: int = Field(ge=0)
    estimated_rows: int = Field(ge=0)
    actual_rows: int = Field(ge=0)
    start_date: date
    end_date: date
    dry_run: bool


class DailyIndicatorBackfillProtectedWindowError(RuntimeError):
    pass


def _now() -> datetime:
    return datetime.now(UTC)


def _write_window_blocked(now: datetime) -> bool:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("protected-window time must be timezone-aware")
    local = now.astimezone(_SHANGHAI)
    if local.weekday() >= 5:
        return False
    protected_start = datetime.combine(
        local.date(),
        _PROTECTED_START,
        tzinfo=_SHANGHAI,
    )
    protected_end = datetime.combine(
        local.date(),
        _PROTECTED_END,
        tzinfo=_SHANGHAI,
    ) + timedelta(minutes=1)
    return protected_start - _WRITE_MARGIN <= local < protected_end


def _require_write_window(now: datetime) -> None:
    if _write_window_blocked(now):
        raise DailyIndicatorBackfillProtectedWindowError(
            "daily_indicator apply is blocked during weekdays 09:15-15:10 "
            "Asia/Shanghai (including a 60-second write margin)"
        )


def derive_daily_indicators(
    store: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
    ts_codes: list[str] | None = None,
) -> pd.DataFrame:
    """Compute causal qfq indicators and return only the requested output range."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    if ts_codes is not None and not ts_codes:
        return pd.DataFrame(columns=_INDICATOR_COLUMNS)

    where = "daily.trade_date <= ?"
    params: list[object] = [end_date]
    if ts_codes is not None:
        where += " AND daily.ts_code = ANY(?)"
        params.append(ts_codes)
    history = store._conn.execute(
        f"""
        SELECT daily.ts_code, daily.trade_date,
               daily.open, daily.high, daily.low, daily.close,
               factor.adj_factor
        FROM daily_bar AS daily
        LEFT JOIN adj_factor AS factor
          ON factor.ts_code = daily.ts_code
         AND factor.trade_date = daily.trade_date
        WHERE {where}
        ORDER BY daily.ts_code, daily.trade_date
        """,
        params,
    ).fetchdf()
    if history.empty:
        return pd.DataFrame(columns=_INDICATOR_COLUMNS)

    frames: list[pd.DataFrame] = []
    for _, raw_code_rows in history.groupby("ts_code", sort=False):
        code_rows = raw_code_rows.reset_index(drop=True)
        factors = pd.to_numeric(
            code_rows["adj_factor"],
            errors="coerce",
        )
        adjusted = pd.DataFrame(
            {
                "ts_code": code_rows["ts_code"],
                "trade_date": code_rows["trade_date"],
                "qfq_open": pd.to_numeric(
                    code_rows["open"], errors="coerce"
                )
                * factors,
                "qfq_high": pd.to_numeric(
                    code_rows["high"], errors="coerce"
                )
                * factors,
                "qfq_low": pd.to_numeric(
                    code_rows["low"], errors="coerce"
                )
                * factors,
                "qfq_close": pd.to_numeric(
                    code_rows["close"], errors="coerce"
                )
                * factors,
            }
        )
        indicators = compute_indicators(adjusted)
        positive_factors = factors.where(factors > 0)
        indicators[_PRICE_INDICATOR_COLUMNS] = indicators[
            _PRICE_INDICATOR_COLUMNS
        ].div(positive_factors, axis=0)
        indicator_dates = pd.to_datetime(indicators["trade_date"]).dt.date
        target = indicators.loc[
            (indicator_dates >= start_date)
            & (indicator_dates <= end_date)
            & positive_factors.notna()
        ].copy()
        if not target.empty:
            frames.append(target[_INDICATOR_COLUMNS])

    if not frames:
        return pd.DataFrame(columns=_INDICATOR_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def _scope_counts(
    store: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
) -> tuple[int, int]:
    row = store._conn.execute(
        """
        SELECT count(DISTINCT ts_code), count(*)
        FROM daily_bar
        WHERE trade_date BETWEEN ? AND ?
        """,
        [start_date, end_date],
    ).fetchone()
    if row is None:
        return 0, 0
    return int(row[0]), int(row[1])


def backfill_daily_indicators(
    store: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
    apply: bool = False,
    now: datetime | None = None,
) -> DailyIndicatorBackfillResult:
    """Preview or atomically rebuild one local daily_indicator date range."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    resolved_now = now or _now()
    if apply:
        _require_write_window(resolved_now)

    code_count, estimated_rows = _scope_counts(
        store,
        start_date=start_date,
        end_date=end_date,
    )
    if not apply:
        return DailyIndicatorBackfillResult(
            code_count=code_count,
            estimated_rows=estimated_rows,
            actual_rows=0,
            start_date=start_date,
            end_date=end_date,
            dry_run=True,
        )

    indicators = derive_daily_indicators(
        store,
        start_date=start_date,
        end_date=end_date,
    )
    _require_write_window(now or _now())
    transaction_open = False
    try:
        store._conn.execute("BEGIN")
        transaction_open = True
        store._conn.execute(
            "DELETE FROM daily_indicator WHERE trade_date BETWEEN ? AND ?",
            [start_date, end_date],
        )
        actual_rows = store.upsert_indicators(indicators)
        store._conn.execute("COMMIT")
        transaction_open = False
    except BaseException as error:
        if transaction_open:
            try:
                store._conn.execute("ROLLBACK")
            except Exception as rollback_error:
                error.add_note(
                    f"daily indicator backfill rollback failed: {rollback_error}"
                )
        raise

    return DailyIndicatorBackfillResult(
        code_count=code_count,
        estimated_rows=estimated_rows,
        actual_rows=actual_rows,
        start_date=start_date,
        end_date=end_date,
        dry_run=False,
    )
