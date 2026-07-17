"""Derive daily technical indicators from local point-in-time price facts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from math import isfinite
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from rquant.config import settings
from rquant.indicator import compute_indicators
from rquant.storage.duckdb import DuckDBStore

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_PROTECTED_START = dtime(9, 15)
_PROTECTED_END = dtime(15, 10)
_WRITE_MARGIN = timedelta(seconds=60)
_DEFAULT_CODE_BATCH_SIZE = 300
_MIN_OUTPUT_COVERAGE = 0.99
_A_SHARE_TS_CODE_PATTERNS = (
    "000%.SZ",
    "001%.SZ",
    "002%.SZ",
    "003%.SZ",
    "300%.SZ",
    "301%.SZ",
    "600%.SH",
    "601%.SH",
    "603%.SH",
    "605%.SH",
    "688%.SH",
    "689%.SH",
)
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
    consistency_mode: Literal[
        "detached_snapshot_plus_source_fingerprint"
    ] = "detached_snapshot_plus_source_fingerprint"
    toctou_status: Literal[
        "not_applicable",
        "source_fingerprint_verified",
    ]


class DailyIndicatorBackfillProtectedWindowError(RuntimeError):
    pass


class DailyIndicatorBackfillSourceChangedError(RuntimeError):
    pass


class DailyIndicatorBackfillCoverageError(RuntimeError):
    pass


class PreparedDailyIndicatorBackfill(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    start_date: date
    end_date: date
    code_count: int = Field(ge=0)
    estimated_rows: int = Field(ge=0)
    source_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    indicators: pd.DataFrame


IndicatorStoreFactory = Callable[
    [],
    AbstractContextManager[DuckDBStore],
]


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


def require_daily_indicator_write_window(
    now: datetime | None = None,
) -> None:
    """Fail before opening a writer when the production write window is closed."""
    if _write_window_blocked(now or _now()):
        raise DailyIndicatorBackfillProtectedWindowError(
            "daily_indicator apply is blocked during weekdays 09:15-15:10 "
            "Asia/Shanghai (including a 60-second write margin)"
        )


def open_detached_daily_indicator_store() -> DuckDBStore:
    """Open the detached replica and fail closed instead of falling back to main."""
    primary_path = Path(settings.duckdb_path).resolve()
    replica_path = settings.duckdb_readonly_path_resolved
    if (
        not replica_path.is_file()
        or replica_path.is_symlink()
        or replica_path.resolve() == primary_path
    ):
        raise RuntimeError(
            "daily_indicator apply requires a detached read-only replica"
        )
    return DuckDBStore(replica_path, read_only=True)


def _empty_indicators() -> pd.DataFrame:
    return pd.DataFrame(columns=_INDICATOR_COLUMNS)


def _normalize_codes(ts_codes: Sequence[str]) -> list[str]:
    return sorted({str(ts_code) for ts_code in ts_codes})


def _a_share_code_predicate(column: str) -> str:
    return (
        "("
        + " OR ".join(
            f"{column} LIKE '{pattern}'"
            for pattern in _A_SHARE_TS_CODE_PATTERNS
        )
        + ")"
    )


def _list_indicator_codes(
    store: DuckDBStore,
    *,
    end_date: date,
    ts_codes: Sequence[str] | None,
) -> list[str]:
    if ts_codes is not None:
        return _normalize_codes(ts_codes)
    a_share = _a_share_code_predicate("ts_code")
    rows = store._conn.execute(
        f"""
        SELECT DISTINCT ts_code
        FROM daily_bar
        WHERE trade_date <= ?
          AND {a_share}
        ORDER BY ts_code
        """,
        [end_date],
    ).fetchall()
    return [str(row[0]) for row in rows]


def _code_batches(
    ts_codes: Sequence[str],
    *,
    batch_size: int,
) -> Iterator[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    for start in range(0, len(ts_codes), batch_size):
        yield list(ts_codes[start : start + batch_size])


def _load_indicator_history_batch(
    store: DuckDBStore,
    *,
    end_date: date,
    ts_codes: list[str],
) -> pd.DataFrame:
    return store._conn.execute(
        """
        SELECT daily.ts_code, daily.trade_date,
               daily.open, daily.high, daily.low, daily.close,
               factor.adj_factor
        FROM daily_bar AS daily
        LEFT JOIN adj_factor AS factor
          ON factor.ts_code = daily.ts_code
         AND factor.trade_date = daily.trade_date
        WHERE daily.trade_date <= ?
          AND daily.ts_code = ANY(?)
        ORDER BY daily.ts_code, daily.trade_date
        """,
        [end_date, ts_codes],
    ).fetchdf()


def _derive_history_frame(
    history: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    if history.empty:
        return _empty_indicators()

    frames: list[pd.DataFrame] = []
    for _, raw_code_rows in history.groupby("ts_code", sort=False):
        code_rows = raw_code_rows.reset_index(drop=True)
        factors = pd.to_numeric(
            code_rows["adj_factor"],
            errors="coerce",
        )
        factor_is_valid = factors.map(
            lambda factor: pd.notna(factor)
            and isfinite(float(factor))
            and float(factor) > 0
        )
        first_invalid = next(
            (
                position
                for position, valid in enumerate(factor_is_valid)
                if not valid
            ),
            len(code_rows),
        )
        # Match the price-basis contract: an invalid required factor poisons
        # that date and every later output until the source history is repaired.
        code_rows = code_rows.iloc[:first_invalid].reset_index(drop=True)
        factors = factors.iloc[:first_invalid].reset_index(drop=True)
        if code_rows.empty:
            continue
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
        indicators[_PRICE_INDICATOR_COLUMNS] = indicators[
            _PRICE_INDICATOR_COLUMNS
        ].div(factors, axis=0)
        indicator_dates = pd.to_datetime(indicators["trade_date"]).dt.date
        target = indicators.loc[
            (indicator_dates >= start_date)
            & (indicator_dates <= end_date)
        ].copy()
        if not target.empty:
            frames.append(target[_INDICATOR_COLUMNS])

    if not frames:
        return _empty_indicators()
    return pd.concat(frames, ignore_index=True)


def _derive_code_batches(
    store: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
    ts_codes: list[str],
    batch_size: int,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for code_batch in _code_batches(ts_codes, batch_size=batch_size):
        history = _load_indicator_history_batch(
            store,
            end_date=end_date,
            ts_codes=code_batch,
        )
        target = _derive_history_frame(
            history,
            start_date=start_date,
            end_date=end_date,
        )
        if not target.empty:
            frames.append(target)
    if not frames:
        return _empty_indicators()
    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["ts_code", "trade_date"], kind="stable")
        .reset_index(drop=True)
    )


def derive_daily_indicators(
    store: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
    ts_codes: Sequence[str] | None = None,
    batch_size: int = _DEFAULT_CODE_BATCH_SIZE,
) -> pd.DataFrame:
    """Compute causal qfq indicators and return only the requested output range."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    if ts_codes is not None and not ts_codes:
        return _empty_indicators()
    codes = _list_indicator_codes(
        store,
        end_date=end_date,
        ts_codes=ts_codes,
    )
    return _derive_code_batches(
        store,
        start_date=start_date,
        end_date=end_date,
        ts_codes=codes,
        batch_size=batch_size,
    )


def derive_target_daily_indicators(
    store: DuckDBStore,
    *,
    target_date: date,
    daily_rows: pd.DataFrame,
    factor_rows: pd.DataFrame | None,
    batch_size: int = _DEFAULT_CODE_BATCH_SIZE,
) -> pd.DataFrame:
    """Derive one ingest date from primary history plus uncommitted target facts."""
    codes = _normalize_codes(daily_rows["ts_code"].astype(str).tolist())
    if not codes:
        return _empty_indicators()
    target_prices = daily_rows[
        ["ts_code", "trade_date", "open", "high", "low", "close"]
    ].copy()
    target_prices["ts_code"] = target_prices["ts_code"].astype(str)
    target_prices["trade_date"] = pd.to_datetime(target_prices["trade_date"])
    target_factor_rows = (
        pd.DataFrame(columns=["ts_code", "trade_date", "adj_factor"])
        if factor_rows is None or factor_rows.empty
        else factor_rows[["ts_code", "trade_date", "adj_factor"]].copy()
    )
    target_factor_rows["ts_code"] = target_factor_rows["ts_code"].astype(str)
    target_factor_rows["trade_date"] = pd.to_datetime(
        target_factor_rows["trade_date"]
    )
    target = target_prices.merge(
        target_factor_rows,
        on=["ts_code", "trade_date"],
        how="left",
        validate="one_to_one",
    )

    frames: list[pd.DataFrame] = []
    history_end = target_date - timedelta(days=1)
    store._conn.execute("BEGIN TRANSACTION")
    transaction_open = True
    try:
        for code_batch in _code_batches(codes, batch_size=batch_size):
            history = _load_indicator_history_batch(
                store,
                end_date=history_end,
                ts_codes=code_batch,
            )
            combined = pd.concat(
                [
                    history,
                    target.loc[target["ts_code"].isin(code_batch)],
                ],
                ignore_index=True,
            ).sort_values(["ts_code", "trade_date"], kind="stable")
            indicators = _derive_history_frame(
                combined,
                start_date=target_date,
                end_date=target_date,
            )
            if not indicators.empty:
                frames.append(indicators)
        store._conn.execute("COMMIT")
        transaction_open = False
    except BaseException:
        if transaction_open:
            store._conn.execute("ROLLBACK")
        raise
    if not frames:
        return _empty_indicators()
    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["ts_code", "trade_date"], kind="stable")
        .reset_index(drop=True)
    )


def _scope_counts(
    store: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
) -> tuple[int, int]:
    a_share = _a_share_code_predicate("ts_code")
    row = store._conn.execute(
        f"""
        SELECT count(DISTINCT ts_code), count(*)
        FROM daily_bar
        WHERE trade_date BETWEEN ? AND ?
          AND {a_share}
        """,
        [start_date, end_date],
    ).fetchone()
    if row is None:
        return 0, 0
    return int(row[0]), int(row[1])


def _require_prepared_coverage(
    *,
    estimated_rows: int,
    actual_rows: int,
) -> None:
    if estimated_rows == 0:
        return
    coverage = actual_rows / estimated_rows
    if coverage < _MIN_OUTPUT_COVERAGE:
        raise DailyIndicatorBackfillCoverageError(
            f"daily_indicator coverage {coverage:.2%} is below "
            f"{_MIN_OUTPUT_COVERAGE:.2%} "
            f"({actual_rows}/{estimated_rows} rows)"
        )


def _indicator_source_fingerprint(
    store: DuckDBStore,
    *,
    end_date: date,
) -> str:
    table_queries = {
        "daily_bar": """
            WITH source AS (
                SELECT trade_date,
                       hash(
                           ts_code, trade_date, open, high, low, close
                       ) AS row_hash
                FROM daily_bar
                WHERE trade_date <= ?
            )
            SELECT count(*),
                   coalesce(bit_xor(row_hash), 0::UBIGINT),
                   coalesce(sum(CAST(row_hash AS HUGEINT)), 0::HUGEINT),
                   min(trade_date),
                   max(trade_date)
            FROM source
        """,
        "adj_factor": """
            WITH source AS (
                SELECT trade_date,
                       hash(ts_code, trade_date, adj_factor) AS row_hash
                FROM adj_factor
                WHERE trade_date <= ?
            )
            SELECT count(*),
                   coalesce(bit_xor(row_hash), 0::UBIGINT),
                   coalesce(sum(CAST(row_hash AS HUGEINT)), 0::HUGEINT),
                   min(trade_date),
                   max(trade_date)
            FROM source
        """,
    }
    evidence: dict[str, list[object]] = {}
    for table_name, query in table_queries.items():
        row = store._conn.execute(query, [end_date]).fetchone()
        if row is None:  # pragma: no cover - aggregate always returns one row
            raise RuntimeError(f"cannot fingerprint {table_name}")
        evidence[table_name] = [
            int(row[0]),
            int(row[1]),
            int(row[2]),
            None if row[3] is None else row[3].isoformat(),
            None if row[4] is None else row[4].isoformat(),
        ]
    payload = json.dumps(
        {
            "end_date": end_date.isoformat(),
            "tables": evidence,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def preview_daily_indicator_backfill(
    reader: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
) -> DailyIndicatorBackfillResult:
    """Report the backfill scope without deriving or writing indicators."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    code_count, estimated_rows = _scope_counts(
        reader,
        start_date=start_date,
        end_date=end_date,
    )
    return DailyIndicatorBackfillResult(
        code_count=code_count,
        estimated_rows=estimated_rows,
        actual_rows=0,
        start_date=start_date,
        end_date=end_date,
        dry_run=True,
        toctou_status="not_applicable",
    )


def prepare_daily_indicator_backfill(
    reader: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
    batch_size: int = _DEFAULT_CODE_BATCH_SIZE,
) -> PreparedDailyIndicatorBackfill:
    """Derive a stable in-memory plan inside one read-only snapshot."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    reader._conn.execute("BEGIN TRANSACTION")
    transaction_open = True
    try:
        code_count, estimated_rows = _scope_counts(
            reader,
            start_date=start_date,
            end_date=end_date,
        )
        codes = _list_indicator_codes(
            reader,
            end_date=end_date,
            ts_codes=None,
        )
        indicators = _derive_code_batches(
            reader,
            start_date=start_date,
            end_date=end_date,
            ts_codes=codes,
            batch_size=batch_size,
        )
        source_fingerprint = _indicator_source_fingerprint(
            reader,
            end_date=end_date,
        )
        reader._conn.execute("COMMIT")
        transaction_open = False
    except BaseException:
        if transaction_open:
            reader._conn.execute("ROLLBACK")
        raise
    plan = PreparedDailyIndicatorBackfill(
        start_date=start_date,
        end_date=end_date,
        code_count=code_count,
        estimated_rows=estimated_rows,
        source_fingerprint=source_fingerprint,
        indicators=indicators,
    )
    _require_prepared_coverage(
        estimated_rows=plan.estimated_rows,
        actual_rows=len(plan.indicators),
    )
    return plan


def apply_prepared_daily_indicator_backfill(
    writer: DuckDBStore,
    plan: PreparedDailyIndicatorBackfill,
) -> DailyIndicatorBackfillResult:
    """Apply a prepared plan under the production serial-writer contract."""
    _require_prepared_coverage(
        estimated_rows=plan.estimated_rows,
        actual_rows=len(plan.indicators),
    )
    # The aggregate source scan is deliberately much shorter than indicator
    # derivation and prevents a stale detached replica from overwriting main.
    transaction_open = False
    try:
        writer._conn.execute("BEGIN")
        transaction_open = True
        current_fingerprint = _indicator_source_fingerprint(
            writer,
            end_date=plan.end_date,
        )
        if current_fingerprint != plan.source_fingerprint:
            raise DailyIndicatorBackfillSourceChangedError(
                "daily_indicator source facts changed after detached snapshot"
            )
        a_share = _a_share_code_predicate("ts_code")
        writer._conn.execute(
            f"""
            DELETE FROM daily_indicator
            WHERE trade_date BETWEEN ? AND ?
              AND {a_share}
            """,
            [plan.start_date, plan.end_date],
        )
        actual_rows = writer.upsert_indicators(plan.indicators)
        writer._conn.execute("COMMIT")
        transaction_open = False
    except BaseException as error:
        if transaction_open:
            try:
                writer._conn.execute("ROLLBACK")
            except Exception as rollback_error:
                error.add_note(
                    f"daily indicator backfill rollback failed: {rollback_error}"
                )
        raise

    return DailyIndicatorBackfillResult(
        code_count=plan.code_count,
        estimated_rows=plan.estimated_rows,
        actual_rows=actual_rows,
        start_date=plan.start_date,
        end_date=plan.end_date,
        dry_run=False,
        toctou_status="source_fingerprint_verified",
    )


def run_daily_indicator_backfill(
    *,
    reader_factory: IndicatorStoreFactory,
    writer_factory: IndicatorStoreFactory,
    start_date: date,
    end_date: date,
    apply: bool = False,
    now: datetime | None = None,
    batch_size: int = _DEFAULT_CODE_BATCH_SIZE,
) -> DailyIndicatorBackfillResult:
    """Derive under a read snapshot, then open a writer only for short apply."""
    if apply:
        require_daily_indicator_write_window(now)
    with reader_factory() as reader:
        if not apply:
            return preview_daily_indicator_backfill(
                reader,
                start_date=start_date,
                end_date=end_date,
            )
        plan = prepare_daily_indicator_backfill(
            reader,
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
        )
    require_daily_indicator_write_window(now)
    with writer_factory() as writer:
        return apply_prepared_daily_indicator_backfill(writer, plan)
