"""DuckDB 存储层：建表、upsert、查询。"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Sequence
from copy import deepcopy
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal, TypeVar, cast

import duckdb
import pandas as pd
from loguru import logger
from pydantic import BaseModel, TypeAdapter

from rquant.config import settings
from rquant.data_metadata import (
    DataQualityIssue,
    DatasetCoverage,
    DatasetSnapshot,
    DatasetSnapshotFinalization,
    DatasetSnapshotWriteConflictError,
    normalize_utc_datetime,
    utc_now,
)
from rquant.security_status import (
    DailySecurityKey,
    SecurityStatusConcurrentWriteError,
    SecurityStatusCoverage,
    SecurityStatusDaily,
    SecurityStatusEligibilityChangedError,
    SecurityStatusWriteConflictError,
    deduplicate_security_status_rows,
)
from rquant.storage.migrations import initialize_schema
from rquant.trade_calendar import (
    TradeCalendarConflictError,
    TradeCalendarDay,
    TradeCalendarGapError,
    deduplicate_trade_calendar_rows,
    trade_calendar_business_facts,
)

_INVALID_STOCK_STATUS_PREDICATE = """
(
    status.name_source IS NULL
    OR length(trim(status.name_source)) = 0
    OR (status.name IS NOT NULL AND length(trim(status.name)) = 0)
    OR (status.st_source IS NOT NULL AND length(trim(status.st_source)) = 0)
    OR (
        status.conflict_reason IS NOT NULL
        AND (status.name IS NOT NULL OR status.is_st IS NOT NULL)
    )
    OR (
        status.is_st IS NOT NULL
        AND (
            status.name IS NULL
            OR status.available_at IS NULL
            OR lower(trim(status.name_source)) IN ('unknown', 'conflict')
            OR status.st_source IS NULL
            OR lower(trim(status.st_source)) IN ('unknown', 'conflict')
        )
    )
)
"""

ModelT = TypeVar("ModelT", bound=BaseModel)
_SECURITY_STATUS_ROWS_ADAPTER = TypeAdapter(list[SecurityStatusDaily])


def _is_retryable_stock_status_upsert_error(error: BaseException) -> bool:
    if not isinstance(error, duckdb.Error):
        return False
    message = str(error).lower()
    duplicate_or_unique = any(
        marker in message
        for marker in (
            "duplicate key",
            "primary key constraint",
            "unique constraint",
        )
    )
    transaction_conflict = (
        isinstance(error, duckdb.TransactionException)
        and "conflict" in message
    )
    return duplicate_or_unique or transaction_conflict


def _revalidate_for_write(model: ModelT) -> ModelT:
    """Deep-copy and revalidate a frozen model before crossing into SQL."""
    try:
        payload = deepcopy(
            model.model_dump(
                mode="python",
                exclude_computed_fields=True,
                warnings="error",
            )
        )
        return cast(
            ModelT,
            type(model).model_validate(payload),
        )
    except ValueError as exc:
        raise ValueError(
            f"{type(model).__name__} failed write-boundary validation: {exc}"
        ) from exc


def _validate_security_status_rows(
    rows: Sequence[SecurityStatusDaily],
) -> list[SecurityStatusDaily]:
    try:
        return _SECURITY_STATUS_ROWS_ADAPTER.validate_python(list(rows))
    except ValueError as exc:
        raise ValueError(
            f"SecurityStatusDaily failed write-boundary validation: {exc}"
        ) from exc


def _duckdb_transaction_is_active(
    conn: duckdb.DuckDBPyConnection,
) -> bool:
    # Autocommit assigns a fresh id per statement; an explicit transaction reuses it.
    first = conn.execute("SELECT current_transaction_id()").fetchone()
    second = conn.execute("SELECT current_transaction_id()").fetchone()
    if first is None or second is None:
        raise RuntimeError("cannot inspect DuckDB transaction state")
    return first[0] == second[0]


def _security_status_frame(rows: Sequence[SecurityStatusDaily]) -> pd.DataFrame:
    return pd.DataFrame.from_records(
        (
            {
                "ts_code": row.ts_code,
                "trade_date": row.trade_date,
                "name": row.name,
                "is_st": row.is_st,
                "name_source": row.name_source,
                "st_source": row.st_source,
                "available_at": row.available_at,
                "ingested_at": row.ingested_at,
                "conflict_reason": row.conflict_reason,
            }
            for row in rows
        ),
        columns=(
            "ts_code",
            "trade_date",
            "name",
            "is_st",
            "name_source",
            "st_source",
            "available_at",
            "ingested_at",
            "conflict_reason",
        ),
    )


def _utc_datetime_from_db(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"expected UTC datetime text from DuckDB, got {value!r}")
    return normalize_utc_datetime(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _snapshot_from_row(row: tuple[object, ...]) -> DatasetSnapshot:
    snapshot = DatasetSnapshot(
        strategy_name=str(row[1]),
        manifest_id=None if row[2] is None else str(row[2]),
        as_of_time=_utc_datetime_from_db(row[3]),
        code_commit=str(row[4]),
        origin=str(row[5]),
        status=cast(str, row[6]),
        table_watermarks=json.loads(str(row[7])),
        quality_issue_ids=tuple(json.loads(str(row[8]))),
        created_at=_utc_datetime_from_db(row[9]),
        completed_at=(
            None if row[10] is None else _utc_datetime_from_db(row[10])
        ),
    )
    stored_id = str(row[0])
    if snapshot.snapshot_id != stored_id:
        raise ValueError(
            "dataset_snapshot stable id mismatch: "
            f"stored={stored_id}, derived={snapshot.snapshot_id}"
        )
    return snapshot


def _coverage_from_row(row: tuple[object, ...]) -> DatasetCoverage:
    coverage = DatasetCoverage(
        snapshot_id=str(row[0]),
        dataset_id=str(row[1]),
        coverage_scope=str(row[2]),
        table_name=str(row[3]),
        expected_count=int(cast(int, row[4])),
        available_count=int(cast(int, row[5])),
        missing_reasons=tuple(json.loads(str(row[8]))),
        created_at=_utc_datetime_from_db(row[9]),
    )
    stored_missing = int(cast(int, row[6]))
    stored_ratio = cast(float | None, row[7])
    ratio_matches = (
        stored_ratio is None
        if coverage.coverage_ratio is None
        else stored_ratio is not None
        and math.isclose(stored_ratio, coverage.coverage_ratio, abs_tol=1e-9)
    )
    if stored_missing != coverage.missing_count or not ratio_matches:
        raise ValueError(
            "dataset_coverage derived values disagree with persisted counts: "
            f"snapshot_id={coverage.snapshot_id}, dataset_id={coverage.dataset_id}, "
            f"coverage_scope={coverage.coverage_scope}"
        )
    return coverage


def _quality_issue_from_row(row: tuple[object, ...]) -> DataQualityIssue:
    issue = DataQualityIssue(
        rule_id=str(row[1]),
        dataset_id=str(row[2]),
        severity=cast(str, row[3]),
        status=cast(str, row[4]),
        scope_key=str(row[5]),
        message=str(row[6]),
        evidence=json.loads(str(row[7])),
        first_seen_at=_utc_datetime_from_db(row[8]),
        last_seen_at=_utc_datetime_from_db(row[9]),
        resolved_at=(
            None if row[10] is None else _utc_datetime_from_db(row[10])
        ),
    )
    stored_id = str(row[0])
    if issue.issue_id != stored_id:
        raise ValueError(
            "data_quality_issue stable id mismatch: "
            f"stored={stored_id}, derived={issue.issue_id}"
        )
    return issue


def _snapshot_finalization_matches(
    snapshot: DatasetSnapshot,
    finalization: DatasetSnapshotFinalization,
) -> bool:
    return (
        snapshot.status == "ready"
        and snapshot.table_watermarks == finalization.table_watermarks
        and snapshot.quality_issue_ids == finalization.quality_issue_ids
        and snapshot.completed_at == finalization.completed_at
    )


def _coverage_payload_matches(
    stored: DatasetCoverage,
    requested: DatasetCoverage,
) -> bool:
    return (
        stored.snapshot_id,
        stored.dataset_id,
        stored.coverage_scope,
        stored.table_name,
        stored.expected_count,
        stored.available_count,
        stored.missing_reasons,
    ) == (
        requested.snapshot_id,
        requested.dataset_id,
        requested.coverage_scope,
        requested.table_name,
        requested.expected_count,
        requested.available_count,
        requested.missing_reasons,
    )


def _issue_effective_time(issue: DataQualityIssue) -> datetime:
    if issue.resolved_at is None:
        return issue.last_seen_at
    return max(issue.last_seen_at, issue.resolved_at)


def _trade_calendar_from_row(row: tuple[object, ...]) -> TradeCalendarDay:
    return TradeCalendarDay(
        exchange=str(row[0]),
        cal_date=cast(date, row[1]),
        is_open=cast(bool, row[2]),
        pretrade_date=cast(date | None, row[3]),
        source=str(row[4]),
        updated_at=_utc_datetime_from_db(row[5]),
    )


def _security_status_from_row(row: tuple[object, ...]) -> SecurityStatusDaily:
    return SecurityStatusDaily(
        ts_code=str(row[0]),
        trade_date=cast(date, row[1]),
        name=None if row[2] is None else str(row[2]),
        is_st=cast(bool | None, row[3]),
        name_source=str(row[4]),
        st_source=None if row[5] is None else str(row[5]),
        available_at=(
            None if row[6] is None else _utc_datetime_from_db(row[6])
        ),
        ingested_at=_utc_datetime_from_db(row[7]),
        conflict_reason=None if row[8] is None else str(row[8]),
    )


class DuckDBStore:
    def __init__(self, path: Path | None = None, *, read_only: bool = False) -> None:
        self.path = path or settings.duckdb_path
        self._conn = duckdb.connect(str(self.path), read_only=read_only)
        if not read_only:
            self._init_schema()

    def _init_schema(self) -> None:
        initialize_schema(self._conn)

    def list_daily_security_keys(
        self,
        start: date,
        end: date,
        *,
        ts_codes: Sequence[str] | None = None,
    ) -> list[DailySecurityKey]:
        if start > end:
            raise ValueError("daily security key start must not be after end")
        code_scope = tuple(sorted(set(ts_codes or ())))
        if ts_codes is not None and not code_scope:
            return []
        code_predicate = "AND ts_code = ANY(?)" if code_scope else ""
        parameters: list[object] = [start, end]
        if code_scope:
            parameters.append(list(code_scope))
        rows = self._conn.execute(
            f"""
            SELECT ts_code, trade_date
            FROM daily_bar
            WHERE trade_date BETWEEN ? AND ?
              {code_predicate}
            ORDER BY ts_code, trade_date
            """,
            parameters,
        ).fetchall()
        return [
            DailySecurityKey(ts_code=str(ts_code), trade_date=cast(date, trade_date))
            for ts_code, trade_date in rows
        ]

    def list_daily_security_dates(
        self,
        start: date,
        end: date,
        *,
        ts_codes: Sequence[str] | None = None,
    ) -> list[date]:
        if start > end:
            raise ValueError("daily security date start must not be after end")
        code_scope = tuple(sorted(set(ts_codes or ())))
        if ts_codes is not None and not code_scope:
            return []
        code_predicate = "AND ts_code = ANY(?)" if code_scope else ""
        parameters: list[object] = [start, end]
        if code_scope:
            parameters.append(list(code_scope))
        rows = self._conn.execute(
            f"""
            SELECT DISTINCT trade_date
            FROM daily_bar
            WHERE trade_date BETWEEN ? AND ?
              {code_predicate}
            ORDER BY trade_date
            """,
            parameters,
        ).fetchall()
        return [cast(date, row[0]) for row in rows]

    def list_incomplete_stock_status_dates(
        self,
        start: date,
        end: date,
        *,
        ts_codes: Sequence[str] | None = None,
    ) -> list[date]:
        if start > end:
            raise ValueError("stock status date start must not be after end")
        code_scope = tuple(sorted(set(ts_codes or ())))
        if ts_codes is not None and not code_scope:
            return []
        code_predicate = "AND daily.ts_code = ANY(?)" if code_scope else ""
        parameters: list[object] = [start, end]
        if code_scope:
            parameters.append(list(code_scope))
        rows = self._conn.execute(
            f"""
            SELECT DISTINCT daily.trade_date
            FROM daily_bar AS daily
            LEFT JOIN stock_status_daily AS status USING (ts_code, trade_date)
            WHERE daily.trade_date BETWEEN ? AND ?
              {code_predicate}
              AND (
                  status.ts_code IS NULL
                  OR status.is_st IS NULL
                  OR {_INVALID_STOCK_STATUS_PREDICATE}
              )
            ORDER BY daily.trade_date
            """,
            parameters,
        ).fetchall()
        return [cast(date, row[0]) for row in rows]

    def list_incomplete_stock_status_keys(
        self, keys: Sequence[DailySecurityKey]
    ) -> list[DailySecurityKey]:
        ordered = sorted(set(keys), key=lambda key: (key.ts_code, key.trade_date))
        if not ordered:
            return []
        stage_name = "_rquant_status_key_scope"
        frame = pd.DataFrame(
            [(key.ts_code, key.trade_date) for key in ordered],
            columns=["ts_code", "trade_date"],
        )
        try:
            self._conn.register(stage_name, frame)
            rows = self._conn.execute(
                f"""
                SELECT daily.ts_code, daily.trade_date
                FROM {stage_name} AS scope
                INNER JOIN daily_bar AS daily USING (ts_code, trade_date)
                LEFT JOIN stock_status_daily AS status USING (ts_code, trade_date)
                WHERE status.ts_code IS NULL
                   OR status.is_st IS NULL
                   OR {_INVALID_STOCK_STATUS_PREDICATE}
                ORDER BY daily.ts_code, daily.trade_date
                """
            ).fetchall()
        finally:
            self._conn.unregister(stage_name)
        return [
            DailySecurityKey(ts_code=str(ts_code), trade_date=cast(date, trade_date))
            for ts_code, trade_date in rows
        ]

    def upsert_stock_status(
        self,
        rows: Sequence[SecurityStatusDaily],
        *,
        transaction_mode: Literal["standalone", "existing"] = "standalone",
        require_daily_keys: bool = False,
    ) -> int:
        if transaction_mode not in {"standalone", "existing"}:
            raise ValueError(
                "stock status transaction_mode must be 'standalone' or 'existing'"
            )
        observations = _validate_security_status_rows(rows)
        if not observations:
            return 0
        ordered = deduplicate_security_status_rows(observations)
        stage_name = "_rquant_stock_status_stage"
        stage = _security_status_frame(observations)
        stage_registered = False
        transaction_open = False
        try:
            self._conn.register(stage_name, stage)
            stage_registered = True
            if transaction_mode == "standalone":
                self._conn.execute("BEGIN")
                transaction_open = True
            if require_daily_keys:
                # Writers are serialized by contract; revalidate exact keys in
                # this writer transaction instead of adding FK or broad locks.
                missing_rows = self._conn.execute(
                    f"""
                    SELECT DISTINCT stage.ts_code, stage.trade_date
                    FROM {stage_name} AS stage
                    LEFT JOIN daily_bar AS daily USING (ts_code, trade_date)
                    WHERE daily.ts_code IS NULL
                    ORDER BY stage.trade_date, stage.ts_code
                    """
                ).fetchall()
                if missing_rows:
                    raise SecurityStatusEligibilityChangedError(
                        [
                            DailySecurityKey(
                                ts_code=str(ts_code),
                                trade_date=cast(date, trade_date),
                            )
                            for ts_code, trade_date in missing_rows
                        ]
                    )
            conflict = self._conn.execute(
                f"""
                SELECT stage.ts_code, stage.trade_date,
                       strftime(stage.ingested_at AT TIME ZONE 'UTC',
                                '%Y-%m-%dT%H:%M:%S.%fZ')
                FROM {stage_name} AS stage
                JOIN stock_status_daily AS target USING (ts_code, trade_date)
                WHERE stage.ingested_at = target.ingested_at
                  AND (
                      stage.name IS DISTINCT FROM target.name
                      OR stage.is_st IS DISTINCT FROM target.is_st
                      OR stage.name_source IS DISTINCT FROM target.name_source
                      OR stage.st_source IS DISTINCT FROM target.st_source
                      OR stage.available_at IS DISTINCT FROM target.available_at
                      OR stage.conflict_reason
                         IS DISTINCT FROM target.conflict_reason
                  )
                ORDER BY stage.trade_date DESC, stage.ts_code
                LIMIT 1
                """
            ).fetchone()
            if conflict is not None:
                raise SecurityStatusWriteConflictError(
                    str(conflict[0]),
                    cast(date, conflict[1]),
                    _utc_datetime_from_db(conflict[2]),
                )
            self._conn.execute(
                f"""
                INSERT INTO stock_status_daily
                (ts_code, trade_date, name, is_st, name_source, st_source,
                 available_at, ingested_at, conflict_reason)
                SELECT ts_code, trade_date, name, is_st, name_source, st_source,
                       available_at, ingested_at, conflict_reason
                FROM (
                    SELECT *, row_number() OVER (
                        PARTITION BY ts_code, trade_date
                        ORDER BY ingested_at DESC
                    ) AS observation_rank
                    FROM {stage_name}
                ) AS selected
                WHERE observation_rank = 1
                ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                    name = excluded.name,
                    is_st = excluded.is_st,
                    name_source = excluded.name_source,
                    st_source = excluded.st_source,
                    available_at = excluded.available_at,
                    ingested_at = excluded.ingested_at,
                    conflict_reason = excluded.conflict_reason
                WHERE excluded.ingested_at > stock_status_daily.ingested_at
                """
            )
            if transaction_mode == "standalone":
                self._conn.execute("COMMIT")
                transaction_open = False
        except BaseException as primary:
            retryable_conflict = _is_retryable_stock_status_upsert_error(primary)
            rollback_error: BaseException | None = None
            if transaction_open:
                try:
                    self._conn.execute("ROLLBACK")
                    transaction_open = False
                except BaseException as error:
                    rollback_auto_ended = (
                        retryable_conflict
                        and isinstance(error, duckdb.TransactionException)
                        and "no transaction is active" in str(error).lower()
                    )
                    if rollback_auto_ended:
                        transaction_open = False
                    else:
                        rollback_error = error
            if rollback_error is not None:
                raise BaseExceptionGroup(
                    "stock status upsert and rollback both failed",
                    [primary, rollback_error],
                ) from None
            if retryable_conflict:
                raise SecurityStatusConcurrentWriteError(
                    "retry stock status upsert after concurrent transaction conflict"
                ) from primary
            raise
        finally:
            if stage_registered:
                try:
                    self._conn.unregister(stage_name)
                except duckdb.Error:
                    logger.exception("stock status staging view cleanup failed")
        return len(ordered)

    def list_stock_status(
        self,
        start: date,
        end: date,
        *,
        ts_code: str | None = None,
    ) -> list[SecurityStatusDaily]:
        if start > end:
            raise ValueError("stock status start must not be after end")
        filters = "trade_date BETWEEN ? AND ?"
        params: list[object] = [start, end]
        if ts_code is not None:
            filters += " AND ts_code = ?"
            params.append(ts_code)
        rows = self._conn.execute(
            f"""
            SELECT ts_code, trade_date, name, is_st, name_source, st_source,
                   CASE WHEN available_at IS NULL THEN NULL ELSE
                       strftime(available_at AT TIME ZONE 'UTC',
                                '%Y-%m-%dT%H:%M:%S.%fZ')
                   END,
                   strftime(ingested_at AT TIME ZONE 'UTC',
                            '%Y-%m-%dT%H:%M:%S.%fZ'),
                   conflict_reason
            FROM stock_status_daily
            WHERE {filters}
            ORDER BY ts_code, trade_date
            """,
            params,
        ).fetchall()
        return [_security_status_from_row(row) for row in rows]

    def missing_stock_status_keys(
        self, start: date, end: date
    ) -> list[DailySecurityKey]:
        if start > end:
            raise ValueError("stock status gap start must not be after end")
        rows = self._conn.execute(
            """
            SELECT daily.ts_code, daily.trade_date
            FROM daily_bar AS daily
            LEFT JOIN stock_status_daily AS status
              USING (ts_code, trade_date)
            WHERE daily.trade_date BETWEEN ? AND ?
              AND status.ts_code IS NULL
            ORDER BY daily.ts_code, daily.trade_date
            """,
            [start, end],
        ).fetchall()
        return [
            DailySecurityKey(ts_code=str(ts_code), trade_date=cast(date, trade_date))
            for ts_code, trade_date in rows
        ]

    def stock_status_coverage(
        self,
        start: date,
        end: date,
        *,
        sample_limit: int = 20,
    ) -> SecurityStatusCoverage:
        if start > end:
            raise ValueError("stock status coverage start must not be after end")
        if sample_limit < 1:
            raise ValueError("sample_limit must be positive")
        missing_rows = self._conn.execute(
            """
            SELECT counts.expected_count, counts.persisted_count,
                   counts.category_count, samples.ts_code, samples.trade_date
            FROM (
                SELECT COUNT(*) AS expected_count,
                       COUNT(status.ts_code) AS persisted_count,
                       COUNT(*) FILTER (WHERE status.ts_code IS NULL)
                           AS category_count
                FROM daily_bar AS daily
                LEFT JOIN stock_status_daily AS status
                  USING (ts_code, trade_date)
                WHERE daily.trade_date BETWEEN ? AND ?
            ) AS counts
            LEFT JOIN (
                SELECT daily.ts_code, daily.trade_date
                FROM daily_bar AS daily
                LEFT JOIN stock_status_daily AS status
                  USING (ts_code, trade_date)
                WHERE daily.trade_date BETWEEN ? AND ?
                  AND status.ts_code IS NULL
                ORDER BY daily.trade_date DESC, daily.ts_code
                LIMIT ?
            ) AS samples ON TRUE
            ORDER BY samples.trade_date DESC, samples.ts_code
            """,
            [start, end, start, end, sample_limit],
        ).fetchall()
        assert missing_rows
        expected_count = int(missing_rows[0][0])
        persisted_count = int(missing_rows[0][1])
        missing_count = int(missing_rows[0][2])
        missing_samples = tuple(
            DailySecurityKey(ts_code=str(row[3]), trade_date=cast(date, row[4]))
            for row in missing_rows
            if row[3] is not None
        )
        unknown_count, unknown_samples = self._stock_status_category_summary(
            start,
            end,
            predicate="status.is_st IS NULL",
            sample_limit=sample_limit,
        )
        conflict_count, conflict_samples = self._stock_status_category_summary(
            start,
            end,
            predicate="status.conflict_reason IS NOT NULL",
            sample_limit=sample_limit,
        )
        invalid_count, invalid_samples = self._stock_status_category_summary(
            start,
            end,
            predicate=_INVALID_STOCK_STATUS_PREDICATE,
            sample_limit=sample_limit,
        )
        return SecurityStatusCoverage(
            start=start,
            end=end,
            expected_count=expected_count,
            persisted_count=persisted_count,
            missing_count=missing_count,
            unknown_count=unknown_count,
            conflict_count=conflict_count,
            invalid_count=invalid_count,
            missing_samples=missing_samples,
            unknown_samples=unknown_samples,
            conflict_samples=conflict_samples,
            invalid_samples=invalid_samples,
        )

    def _stock_status_category_summary(
        self,
        start: date,
        end: date,
        *,
        predicate: str,
        sample_limit: int,
    ) -> tuple[int, tuple[DailySecurityKey, ...]]:
        allowed_predicates = {
            "status.is_st IS NULL",
            "status.conflict_reason IS NOT NULL",
            _INVALID_STOCK_STATUS_PREDICATE,
        }
        if predicate not in allowed_predicates:
            raise ValueError(f"unsupported stock status predicate: {predicate}")
        rows = self._conn.execute(
            f"""
            SELECT counts.category_count, samples.ts_code, samples.trade_date
            FROM (
                SELECT COUNT(*) AS category_count
                FROM stock_status_daily AS status
                INNER JOIN daily_bar AS daily USING (ts_code, trade_date)
                WHERE daily.trade_date BETWEEN ? AND ?
                  AND {predicate}
            ) AS counts
            LEFT JOIN (
                SELECT status.ts_code, status.trade_date
                FROM stock_status_daily AS status
                INNER JOIN daily_bar AS daily USING (ts_code, trade_date)
                WHERE daily.trade_date BETWEEN ? AND ?
                  AND {predicate}
                ORDER BY status.trade_date DESC, status.ts_code
                LIMIT ?
            ) AS samples ON TRUE
            ORDER BY samples.trade_date DESC, samples.ts_code
            """,
            [start, end, start, end, sample_limit],
        ).fetchall()
        assert rows
        samples = tuple(
            DailySecurityKey(ts_code=str(row[1]), trade_date=cast(date, row[2]))
            for row in rows
            if row[1] is not None
        )
        return int(rows[0][0]), samples

    def upsert_trade_calendar(self, rows: Sequence[TradeCalendarDay]) -> int:
        observations = [_revalidate_for_write(row) for row in rows]
        if not observations:
            return 0
        self._conn.execute("BEGIN")
        try:
            for incoming in observations:
                existing = self.get_trade_calendar_day(
                    incoming.exchange,
                    incoming.cal_date,
                )
                if (
                    existing is not None
                    and existing.updated_at == incoming.updated_at
                    and trade_calendar_business_facts(existing)
                    != trade_calendar_business_facts(incoming)
                ):
                    raise TradeCalendarConflictError(
                        incoming.exchange,
                        incoming.cal_date,
                        incoming.updated_at,
                    )
            selected = deduplicate_trade_calendar_rows(observations)
            self._conn.executemany(
                """
                INSERT INTO trade_calendar
                (exchange, cal_date, is_open, pretrade_date, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (exchange, cal_date) DO UPDATE SET
                    is_open = excluded.is_open,
                    pretrade_date = excluded.pretrade_date,
                    source = excluded.source,
                    updated_at = excluded.updated_at
                WHERE excluded.updated_at > trade_calendar.updated_at
                """,
                [
                    [
                        row.exchange,
                        row.cal_date,
                        row.is_open,
                        row.pretrade_date,
                        row.source,
                        row.updated_at,
                    ]
                    for row in selected
                ],
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return len(selected)

    def get_trade_calendar_day(
        self, exchange: str, cal_date: date
    ) -> TradeCalendarDay | None:
        row = self._conn.execute(
            """
            SELECT exchange, cal_date, is_open, pretrade_date, source,
                   strftime(updated_at AT TIME ZONE 'UTC',
                            '%Y-%m-%dT%H:%M:%S.%fZ') AS updated_at
            FROM trade_calendar
            WHERE exchange = ? AND cal_date = ?
            """,
            [exchange, cal_date],
        ).fetchone()
        return None if row is None else _trade_calendar_from_row(row)

    def list_trade_calendar(
        self, exchange: str, start: date, end: date
    ) -> list[TradeCalendarDay]:
        if start > end:
            return []
        rows = self._conn.execute(
            """
            SELECT exchange, cal_date, is_open, pretrade_date, source,
                   strftime(updated_at AT TIME ZONE 'UTC',
                            '%Y-%m-%dT%H:%M:%S.%fZ') AS updated_at
            FROM trade_calendar
            WHERE exchange = ? AND cal_date BETWEEN ? AND ?
            ORDER BY cal_date
            """,
            [exchange, start, end],
        ).fetchall()
        return [_trade_calendar_from_row(row) for row in rows]

    def missing_trade_calendar_dates(
        self, exchange: str, start: date, end: date
    ) -> list[date]:
        if start > end:
            raise ValueError("trade calendar range start must not be after end")
        present = {
            row.cal_date for row in self.list_trade_calendar(exchange, start, end)
        }
        return [
            start + timedelta(days=offset)
            for offset in range((end - start).days + 1)
            if start + timedelta(days=offset) not in present
        ]

    def is_trading_day(self, exchange: str, cal_date: date) -> bool:
        row = self.get_trade_calendar_day(exchange, cal_date)
        if row is None:
            raise TradeCalendarGapError(exchange, [cal_date])
        return row.is_open

    def _require_calendar_range(
        self, exchange: str, start: date, end: date
    ) -> None:
        missing = self.missing_trade_calendar_dates(exchange, start, end)
        if missing:
            raise TradeCalendarGapError(exchange, missing)

    def _require_calendar_anchor(self, exchange: str, anchor: date) -> None:
        if self.get_trade_calendar_day(exchange, anchor) is None:
            raise TradeCalendarGapError(exchange, [anchor])

    def previous_trading_day(
        self, anchor: date, *, exchange: str = "SSE"
    ) -> date:
        self._require_calendar_anchor(exchange, anchor)
        row = self._conn.execute(
            "SELECT MAX(cal_date) FROM trade_calendar "
            "WHERE exchange = ? AND cal_date < ? AND is_open",
            [exchange, anchor],
        ).fetchone()
        candidate = None if row is None else cast(date | None, row[0])
        if candidate is None:
            raise TradeCalendarGapError(
                exchange,
                detail=f"no previous trading day in stored coverage for {exchange}",
            )
        self._require_calendar_range(exchange, candidate, anchor)
        return candidate

    def next_trading_day(
        self, anchor: date, *, exchange: str = "SSE"
    ) -> date:
        self._require_calendar_anchor(exchange, anchor)
        row = self._conn.execute(
            "SELECT MIN(cal_date) FROM trade_calendar "
            "WHERE exchange = ? AND cal_date > ? AND is_open",
            [exchange, anchor],
        ).fetchone()
        candidate = None if row is None else cast(date | None, row[0])
        if candidate is None:
            raise TradeCalendarGapError(
                exchange,
                detail=f"no next trading day in stored coverage for {exchange}",
            )
        self._require_calendar_range(exchange, anchor, candidate)
        return candidate

    def latest_trading_day(
        self, anchor: date, *, exchange: str = "SSE"
    ) -> date:
        self._require_calendar_anchor(exchange, anchor)
        row = self._conn.execute(
            "SELECT MAX(cal_date) FROM trade_calendar "
            "WHERE exchange = ? AND cal_date <= ? AND is_open",
            [exchange, anchor],
        ).fetchone()
        candidate = None if row is None else cast(date | None, row[0])
        if candidate is None:
            raise TradeCalendarGapError(
                exchange,
                detail=f"no latest trading day in stored coverage for {exchange}",
            )
        self._require_calendar_range(exchange, candidate, anchor)
        return candidate

    def begin_dataset_snapshot(
        self, snapshot: DatasetSnapshot
    ) -> DatasetSnapshot:
        snapshot = _revalidate_for_write(snapshot)
        if snapshot.status != "building":
            raise ValueError("begin_dataset_snapshot requires a building snapshot")
        self._conn.execute("BEGIN")
        try:
            existing = self.get_dataset_snapshot(snapshot.snapshot_id)
            if existing is not None:
                existing_identity = (
                    existing.strategy_name,
                    existing.manifest_id,
                    existing.as_of_time,
                    existing.code_commit,
                    existing.origin,
                )
                requested_identity = (
                    snapshot.strategy_name,
                    snapshot.manifest_id,
                    snapshot.as_of_time,
                    snapshot.code_commit,
                    snapshot.origin,
                )
                if existing_identity != requested_identity:
                    raise ValueError(
                        f"dataset snapshot id conflict: {snapshot.snapshot_id}"
                    )
                self._conn.execute("COMMIT")
                return existing
            self._conn.execute(
                """
                INSERT INTO dataset_snapshot
                (snapshot_id, strategy_name, manifest_id, as_of_time, code_commit,
                 origin, status, table_watermarks, quality_issue_ids, created_at,
                 completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ?, ?)
                """,
                [
                    snapshot.snapshot_id,
                    snapshot.strategy_name,
                    snapshot.manifest_id,
                    snapshot.as_of_time,
                    snapshot.code_commit,
                    snapshot.origin,
                    snapshot.status,
                    json.dumps(snapshot.table_watermarks, sort_keys=True),
                    json.dumps(snapshot.quality_issue_ids),
                    snapshot.created_at,
                    snapshot.completed_at,
                ],
            )
            stored = self.get_dataset_snapshot(snapshot.snapshot_id)
            if stored is None:
                raise RuntimeError(
                    "dataset snapshot insert was not persisted: "
                    f"{snapshot.snapshot_id}"
                )
            self._conn.execute("COMMIT")
            return stored
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def finalize_dataset_snapshot(
        self,
        snapshot_id: str,
        finalization: DatasetSnapshotFinalization,
    ) -> DatasetSnapshot:
        finalization = _revalidate_for_write(finalization)
        transaction_open = True
        self._conn.execute("BEGIN")
        try:
            existing = self.get_dataset_snapshot(snapshot_id)
            if existing is None:
                raise KeyError(f"dataset snapshot not found: {snapshot_id}")
            missing_issue_ids = self._missing_quality_issue_ids(
                finalization.quality_issue_ids
            )
            if missing_issue_ids:
                missing = ", ".join(missing_issue_ids)
                raise KeyError(
                    f"dataset snapshot quality issue references missing: {missing}"
                )
            if finalization.completed_at < existing.created_at:
                raise ValueError(
                    "snapshot completed_at cannot be earlier than created_at: "
                    f"{snapshot_id}"
                )
            if existing.status == "ready":
                if not _snapshot_finalization_matches(existing, finalization):
                    raise ValueError(
                        "dataset snapshot already finalized with different data: "
                        f"{snapshot_id}"
                    )
                self._conn.execute("COMMIT")
                transaction_open = False
                return existing
            try:
                updated = self._conn.execute(
                    """
                    UPDATE dataset_snapshot
                    SET status = 'ready',
                        table_watermarks = CAST(? AS JSON),
                        quality_issue_ids = CAST(? AS JSON),
                        completed_at = ?
                    WHERE snapshot_id = ? AND status = 'building'
                    RETURNING snapshot_id
                    """,
                    [
                        json.dumps(finalization.table_watermarks, sort_keys=True),
                        json.dumps(finalization.quality_issue_ids),
                        finalization.completed_at,
                        snapshot_id,
                    ],
                ).fetchall()
            except duckdb.TransactionException as exc:
                self._conn.execute("ROLLBACK")
                transaction_open = False
                return self._snapshot_after_cas_loss(
                    snapshot_id,
                    finalization,
                    conflict_cause=exc,
                )
            if not updated:
                self._conn.execute("ROLLBACK")
                transaction_open = False
                return self._snapshot_after_cas_loss(snapshot_id, finalization)
            finalized = self.get_dataset_snapshot(snapshot_id)
            if finalized is None:
                raise RuntimeError(
                    f"dataset snapshot finalize was not persisted: {snapshot_id}"
                )
            self._conn.execute("COMMIT")
            transaction_open = False
            return finalized
        except Exception:
            if transaction_open:
                self._conn.execute("ROLLBACK")
            raise

    def _snapshot_after_cas_loss(
        self,
        snapshot_id: str,
        finalization: DatasetSnapshotFinalization,
        *,
        conflict_cause: Exception | None = None,
    ) -> DatasetSnapshot:
        current = self.get_dataset_snapshot(snapshot_id)
        if current is not None and _snapshot_finalization_matches(
            current, finalization
        ):
            return current
        if current is None or current.status == "building":
            conflict = DatasetSnapshotWriteConflictError(
                "dataset snapshot write conflict; retry finalization: "
                f"{snapshot_id}"
            )
            if conflict_cause is not None:
                raise conflict from conflict_cause
            raise conflict
        raise ValueError(
            "concurrent dataset snapshot finalization committed different data: "
            f"{snapshot_id}"
        )

    def _missing_quality_issue_ids(
        self, issue_ids: tuple[str, ...]
    ) -> list[str]:
        if not issue_ids:
            return []
        placeholders = ",".join("?" for _ in issue_ids)
        rows = self._conn.execute(
            f"SELECT issue_id FROM data_quality_issue "
            f"WHERE issue_id IN ({placeholders})",
            list(issue_ids),
        ).fetchall()
        existing = {str(row[0]) for row in rows}
        return [issue_id for issue_id in issue_ids if issue_id not in existing]

    def get_dataset_snapshot(self, snapshot_id: str) -> DatasetSnapshot | None:
        row = self._conn.execute(
            """
            SELECT snapshot_id, strategy_name, manifest_id,
                   strftime(as_of_time AT TIME ZONE 'UTC',
                            '%Y-%m-%dT%H:%M:%S.%fZ') AS as_of_time,
                   code_commit, origin, status, table_watermarks,
                   quality_issue_ids,
                   strftime(created_at AT TIME ZONE 'UTC',
                            '%Y-%m-%dT%H:%M:%S.%fZ') AS created_at,
                   CASE WHEN completed_at IS NULL THEN NULL ELSE
                       strftime(completed_at AT TIME ZONE 'UTC',
                                '%Y-%m-%dT%H:%M:%S.%fZ')
                   END AS completed_at
            FROM dataset_snapshot
            WHERE snapshot_id = ?
            """,
            [snapshot_id],
        ).fetchone()
        return None if row is None else _snapshot_from_row(row)

    def upsert_dataset_coverage(
        self, coverage: DatasetCoverage
    ) -> DatasetCoverage:
        coverage = _revalidate_for_write(coverage)
        transaction_open = True
        self._conn.execute("BEGIN")
        try:
            # Touching the parent row serializes coverage writes with finalization.
            touched = self._conn.execute(
                "UPDATE dataset_snapshot SET status = status "
                "WHERE snapshot_id = ? RETURNING snapshot_id",
                [coverage.snapshot_id],
            ).fetchone()
            if touched is None:
                raise KeyError(
                    f"dataset snapshot not found: {coverage.snapshot_id}"
                )
            snapshot = self.get_dataset_snapshot(coverage.snapshot_id)
            if snapshot is None:
                raise KeyError(
                    f"dataset snapshot not found: {coverage.snapshot_id}"
                )
            if snapshot.status == "ready":
                stored = self._conn.execute(
                    """
                    SELECT snapshot_id, dataset_id, coverage_scope, table_name,
                           expected_count, available_count, missing_count,
                           coverage_ratio, missing_reasons,
                           strftime(created_at AT TIME ZONE 'UTC',
                                    '%Y-%m-%dT%H:%M:%S.%fZ') AS created_at
                    FROM dataset_coverage
                    WHERE snapshot_id = ? AND dataset_id = ? AND coverage_scope = ?
                    """,
                    [
                        coverage.snapshot_id,
                        coverage.dataset_id,
                        coverage.coverage_scope,
                    ],
                ).fetchone()
                existing = None if stored is None else _coverage_from_row(stored)
                if existing is None or not _coverage_payload_matches(
                    existing, coverage
                ):
                    raise ValueError(
                        "finalized dataset snapshot coverage is immutable: "
                        f"{coverage.snapshot_id}/{coverage.dataset_id}/"
                        f"{coverage.coverage_scope}"
                    )
                self._conn.execute("COMMIT")
                transaction_open = False
                return existing
            self._conn.execute(
                """
                INSERT INTO dataset_coverage
                (snapshot_id, dataset_id, coverage_scope, table_name,
                 expected_count, available_count, missing_count, coverage_ratio,
                 missing_reasons, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?)
                ON CONFLICT (snapshot_id, dataset_id, coverage_scope) DO UPDATE SET
                    table_name = excluded.table_name,
                    expected_count = excluded.expected_count,
                    available_count = excluded.available_count,
                    missing_count = excluded.missing_count,
                    coverage_ratio = excluded.coverage_ratio,
                    missing_reasons = excluded.missing_reasons
                """,
                [
                    coverage.snapshot_id,
                    coverage.dataset_id,
                    coverage.coverage_scope,
                    coverage.table_name,
                    coverage.expected_count,
                    coverage.available_count,
                    coverage.missing_count,
                    coverage.coverage_ratio,
                    json.dumps(coverage.missing_reasons),
                    coverage.created_at,
                ],
            )
            stored = self._conn.execute(
                """
                SELECT snapshot_id, dataset_id, coverage_scope, table_name,
                       expected_count, available_count, missing_count,
                       coverage_ratio, missing_reasons,
                       strftime(created_at AT TIME ZONE 'UTC',
                                '%Y-%m-%dT%H:%M:%S.%fZ') AS created_at
                FROM dataset_coverage
                WHERE snapshot_id = ? AND dataset_id = ? AND coverage_scope = ?
                """,
                [
                    coverage.snapshot_id,
                    coverage.dataset_id,
                    coverage.coverage_scope,
                ],
            ).fetchone()
            if stored is None:
                raise RuntimeError(
                    "dataset coverage upsert was not persisted: "
                    f"{coverage.snapshot_id}/{coverage.dataset_id}/"
                    f"{coverage.coverage_scope}"
                )
            result = _coverage_from_row(stored)
            self._conn.execute("COMMIT")
            transaction_open = False
            return result
        except duckdb.TransactionException as exc:
            if transaction_open:
                self._conn.execute("ROLLBACK")
                transaction_open = False
            raise DatasetSnapshotWriteConflictError(
                "dataset snapshot write conflict; retry coverage upsert: "
                f"{coverage.snapshot_id}"
            ) from exc
        except Exception:
            if transaction_open:
                self._conn.execute("ROLLBACK")
            raise

    def list_dataset_coverages(self, snapshot_id: str) -> list[DatasetCoverage]:
        rows = self._conn.execute(
            """
            SELECT snapshot_id, dataset_id, coverage_scope, table_name,
                   expected_count, available_count, missing_count,
                   coverage_ratio, missing_reasons,
                   strftime(created_at AT TIME ZONE 'UTC',
                            '%Y-%m-%dT%H:%M:%S.%fZ') AS created_at
            FROM dataset_coverage
            WHERE snapshot_id = ?
            ORDER BY dataset_id, coverage_scope
            """,
            [snapshot_id],
        ).fetchall()
        return [_coverage_from_row(row) for row in rows]

    def record_data_quality_issue(
        self, issue: DataQualityIssue
    ) -> DataQualityIssue:
        issue = _revalidate_for_write(issue)
        if issue.status != "open":
            raise ValueError("record_data_quality_issue requires an open issue")
        self._conn.execute("BEGIN")
        try:
            existing = self.get_data_quality_issue(issue.issue_id)
            if existing is not None and (
                existing.rule_id,
                existing.dataset_id,
                existing.scope_key,
            ) != (issue.rule_id, issue.dataset_id, issue.scope_key):
                raise ValueError(
                    f"data quality issue id conflict: {issue.issue_id}"
                )
            if existing is not None and issue.last_seen_at <= _issue_effective_time(
                existing
            ):
                self._conn.execute("COMMIT")
                return existing
            if existing is None:
                self._conn.execute(
                    """
                    INSERT INTO data_quality_issue
                    (issue_id, rule_id, dataset_id, severity, status, scope_key,
                     message, evidence, first_seen_at, last_seen_at, resolved_at)
                    VALUES (?, ?, ?, ?, 'open', ?, ?, CAST(? AS JSON), ?, ?, NULL)
                    """,
                    [
                        issue.issue_id,
                        issue.rule_id,
                        issue.dataset_id,
                        issue.severity,
                        issue.scope_key,
                        issue.message,
                        json.dumps(issue.evidence, sort_keys=True),
                        issue.first_seen_at,
                        issue.last_seen_at,
                    ],
                )
            else:
                self._conn.execute(
                    """
                    UPDATE data_quality_issue
                    SET severity = ?, status = 'open', message = ?,
                        evidence = CAST(? AS JSON), last_seen_at = ?,
                        resolved_at = NULL
                    WHERE issue_id = ?
                      AND ? > greatest(
                          last_seen_at,
                          coalesce(resolved_at, last_seen_at)
                      )
                    """,
                    [
                        issue.severity,
                        issue.message,
                        json.dumps(issue.evidence, sort_keys=True),
                        issue.last_seen_at,
                        issue.issue_id,
                        issue.last_seen_at,
                    ],
                )
            stored = self.get_data_quality_issue(issue.issue_id)
            if stored is None:
                raise RuntimeError(
                    "data quality issue upsert was not persisted: "
                    f"{issue.issue_id}"
                )
            self._conn.execute("COMMIT")
            return stored
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def get_data_quality_issue(self, issue_id: str) -> DataQualityIssue | None:
        row = self._conn.execute(
            """
            SELECT issue_id, rule_id, dataset_id, severity, status, scope_key,
                   message, evidence,
                   strftime(first_seen_at AT TIME ZONE 'UTC',
                            '%Y-%m-%dT%H:%M:%S.%fZ') AS first_seen_at,
                   strftime(last_seen_at AT TIME ZONE 'UTC',
                            '%Y-%m-%dT%H:%M:%S.%fZ') AS last_seen_at,
                   CASE WHEN resolved_at IS NULL THEN NULL ELSE
                       strftime(resolved_at AT TIME ZONE 'UTC',
                                '%Y-%m-%dT%H:%M:%S.%fZ')
                   END AS resolved_at
            FROM data_quality_issue
            WHERE issue_id = ?
            """,
            [issue_id],
        ).fetchone()
        return None if row is None else _quality_issue_from_row(row)

    def resolve_data_quality_issue(
        self,
        issue_id: str,
        *,
        resolved_at: datetime | None = None,
    ) -> DataQualityIssue:
        timestamp_omitted = resolved_at is None
        resolution_time = normalize_utc_datetime(
            utc_now() if timestamp_omitted else resolved_at
        )
        existing = self.get_data_quality_issue(issue_id)
        if existing is None:
            raise KeyError(f"data quality issue not found: {issue_id}")
        if existing.status == "resolved":
            if timestamp_omitted or existing.resolved_at == resolution_time:
                return existing
            raise ValueError(
                "data quality issue already resolved with different timestamp: "
                f"{issue_id}"
            )
        if resolution_time < existing.last_seen_at:
            raise ValueError(
                "data quality issue resolved_at cannot be earlier than "
                f"last_seen_at: {issue_id}"
            )
        for attempt in range(2):
            self._conn.execute("BEGIN")
            transaction_open = True
            try:
                updated = self._conn.execute(
                    """
                    UPDATE data_quality_issue
                    SET status = 'resolved', resolved_at = ?
                    WHERE issue_id = ?
                      AND status = 'open'
                      AND last_seen_at <= ?
                    RETURNING issue_id
                    """,
                    [resolution_time, issue_id, resolution_time],
                ).fetchall()
                if not updated:
                    self._conn.execute("ROLLBACK")
                    transaction_open = False
                    winner = self._resolution_after_cas_loss(
                        issue_id,
                        resolution_time,
                        timestamp_omitted=timestamp_omitted,
                    )
                    if winner is not None:
                        return winner
                    if attempt == 0:
                        continue
                    raise RuntimeError(
                        "data quality issue resolution lost repeated concurrent "
                        f"updates: {issue_id}"
                    )
                resolved = self.get_data_quality_issue(issue_id)
                if resolved is None:
                    raise RuntimeError(
                        "data quality issue resolution was not persisted: "
                        f"{issue_id}"
                    )
                self._conn.execute("COMMIT")
                transaction_open = False
                return resolved
            except duckdb.TransactionException as exc:
                if transaction_open:
                    self._conn.execute("ROLLBACK")
                    transaction_open = False
                winner = self._resolution_after_cas_loss(
                    issue_id,
                    resolution_time,
                    timestamp_omitted=timestamp_omitted,
                )
                if winner is not None:
                    return winner
                if attempt == 0:
                    continue
                raise RuntimeError(
                    "data quality issue resolution lost repeated concurrent "
                    f"updates: {issue_id}"
                ) from exc
            except Exception:
                if transaction_open:
                    self._conn.execute("ROLLBACK")
                raise
        raise RuntimeError(f"data quality issue resolution failed: {issue_id}")

    def _resolution_after_cas_loss(
        self,
        issue_id: str,
        resolution_time: datetime,
        *,
        timestamp_omitted: bool,
    ) -> DataQualityIssue | None:
        current = self.get_data_quality_issue(issue_id)
        if current is None:
            raise KeyError(f"data quality issue not found after CAS loss: {issue_id}")
        if current.status == "resolved":
            if timestamp_omitted or current.resolved_at == resolution_time:
                return current
            raise ValueError(
                "data quality issue already resolved with different timestamp: "
                f"{issue_id}"
            )
        if resolution_time < current.last_seen_at:
            raise ValueError(
                "newer detection won resolution CAS; proposed resolved_at is "
                f"earlier than last_seen_at: {issue_id}"
            )
        return None

    def list_snapshot_quality_issues(
        self, snapshot_id: str
    ) -> list[DataQualityIssue]:
        snapshot = self.get_dataset_snapshot(snapshot_id)
        if snapshot is None or not snapshot.quality_issue_ids:
            return []
        placeholders = ",".join("?" for _ in snapshot.quality_issue_ids)
        rows = self._conn.execute(
            "SELECT issue_id, rule_id, dataset_id, severity, status, scope_key, "
            "message, evidence, "
            "strftime(first_seen_at AT TIME ZONE 'UTC', "
            "'%Y-%m-%dT%H:%M:%S.%fZ'), "
            "strftime(last_seen_at AT TIME ZONE 'UTC', "
            "'%Y-%m-%dT%H:%M:%S.%fZ'), "
            "CASE WHEN resolved_at IS NULL THEN NULL ELSE "
            "strftime(resolved_at AT TIME ZONE 'UTC', "
            "'%Y-%m-%dT%H:%M:%S.%fZ') END "
            f"FROM data_quality_issue WHERE issue_id IN ({placeholders})",
            list(snapshot.quality_issue_ids),
        ).fetchall()
        issues_by_id = {
            issue.issue_id: issue for issue in map(_quality_issue_from_row, rows)
        }
        missing_issue_ids = [
            issue_id
            for issue_id in snapshot.quality_issue_ids
            if issue_id not in issues_by_id
        ]
        if missing_issue_ids:
            missing = ", ".join(missing_issue_ids)
            raise RuntimeError(
                f"dataset snapshot references missing quality issue ids: {missing}"
            )
        return [
            issues_by_id[issue_id]
            for issue_id in snapshot.quality_issue_ids
        ]

    def upsert_daily(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("daily_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO daily_bar
            SELECT
                ts_code, trade_date,
                open, high, low, close,
                pre_close, change, pct_chg,
                vol, amount
            FROM daily_tmp
            """
        )
        self._conn.unregister("daily_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert daily_bar: {count} 行")
        return count

    def upsert_adj_factor(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("factor_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO adj_factor
            SELECT ts_code, trade_date, adj_factor
            FROM factor_tmp
            """
        )
        self._conn.unregister("factor_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert adj_factor: {count} 行")
        return count

    def upsert_index_daily(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("index_daily_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO index_daily_bar
            SELECT
                ts_code, trade_date,
                open, high, low, close,
                pre_close, change, pct_chg,
                vol, amount
            FROM index_daily_tmp
            """
        )
        self._conn.unregister("index_daily_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert index_daily_bar: {count} 行")
        return count

    def query_index_daily(self, trade_date: date | str) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, trade_date, open, high, low, close,
                   pre_close, change, pct_chg, vol, amount
            FROM index_daily_bar
            WHERE trade_date = ?
            ORDER BY ts_code
            """,
            [trade_date],
        ).fetchdf()

    def get_daily_qfq(
        self,
        ts_code: str,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """返回某只股票的前复权日线。

        前复权公式：qfq[t] = raw[t] * adj_factor[t] / adj_factor[latest]
        参考因子 = 该股票 adj_factor 表中最大 trade_date 对应的因子。

        同时返回原始价和 qfq 价，方便对比核验。
        """
        params: list[str] = [ts_code]
        where = "db.ts_code = ?"
        if start:
            where += " AND db.trade_date >= ?"
            params.append(start)
        if end:
            where += " AND db.trade_date <= ?"
            params.append(end)

        sql = f"""
        WITH ref AS (
            SELECT ts_code, adj_factor AS ref_factor
            FROM adj_factor
            WHERE ts_code = ?
              AND trade_date = (
                  SELECT MAX(trade_date) FROM adj_factor WHERE ts_code = ?
              )
        )
        SELECT
            db.ts_code,
            strftime(db.trade_date, '%Y-%m-%d') AS trade_date,
            db.open  AS raw_open,
            db.close AS raw_close,
            db.open  * af.adj_factor / r.ref_factor AS qfq_open,
            db.high  * af.adj_factor / r.ref_factor AS qfq_high,
            db.low   * af.adj_factor / r.ref_factor AS qfq_low,
            db.close * af.adj_factor / r.ref_factor AS qfq_close,
            db.vol,
            af.adj_factor,
            r.ref_factor
        FROM daily_bar db
        INNER JOIN adj_factor af
            ON db.ts_code = af.ts_code AND db.trade_date = af.trade_date
        INNER JOIN ref r
            ON db.ts_code = r.ts_code
        WHERE {where}
        ORDER BY db.trade_date
        """
        ref_params = [ts_code, ts_code]
        return self._conn.execute(sql, ref_params + params).fetchdf()

    def count_adj_factor(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM adj_factor WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM adj_factor").fetchone()
        return result[0] if result else 0

    def upsert_indicators(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("ind_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO daily_indicator
            SELECT ts_code, trade_date,
                   ma5, ma10, ma20, ma60,
                   rsi6, rsi14,
                   macd, macd_signal, macd_hist,
                   kdj_k, kdj_d, kdj_j
            FROM ind_tmp
            """
        )
        self._conn.unregister("ind_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert daily_indicator: {count} 行")
        return count

    def count_indicators(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM daily_indicator WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM daily_indicator").fetchone()
        return result[0] if result else 0

    def upsert_state(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("state_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO daily_state
            SELECT ts_code, trade_date,
                   is_st, is_bj, board_type, limit_pct,
                   limit_up_price, limit_down_price,
                   is_limit_up, is_limit_down,
                   is_first_limit_up, is_yiziban,
                   consecutive_limit_ups,
                   body_upper, body_lower
            FROM state_tmp
            """
        )
        self._conn.unregister("state_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert daily_state: {count} 行")
        return count

    def count_state(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM daily_state WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM daily_state").fetchone()
        return result[0] if result else 0

    def upsert_daily_basic(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("basic_mkt_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO daily_basic
            SELECT ts_code, trade_date,
                   turnover_rate, volume_ratio,
                   total_mv, circ_mv
            FROM basic_mkt_tmp
            """
        )
        self._conn.unregister("basic_mkt_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert daily_basic: {count} 行")
        return count

    def upsert_moneyflow_daily(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        payload = df.copy()
        if "source" not in payload.columns:
            payload["source"] = "tushare"
        self._conn.register("moneyflow_tmp", payload)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO moneyflow_daily
            (ts_code, trade_date, buy_lg_vol, sell_lg_vol,
             buy_elg_vol, sell_elg_vol, large_net_vol,
             large_net_amount, source)
            SELECT ts_code, trade_date, buy_lg_vol, sell_lg_vol,
                   buy_elg_vol, sell_elg_vol, large_net_vol,
                   large_net_amount, source
            FROM moneyflow_tmp
            """
        )
        self._conn.unregister("moneyflow_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert moneyflow_daily: {count} 行")
        return count

    def query_moneyflow_daily(
        self,
        trade_date: str | date | pd.Timestamp,
    ) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, trade_date, buy_lg_vol, sell_lg_vol,
                   buy_elg_vol, sell_elg_vol, large_net_vol,
                   large_net_amount, source
            FROM moneyflow_daily
            WHERE trade_date = ?
            ORDER BY ts_code
            """,
            [trade_date],
        ).fetchdf()

    def upsert_market_sentiment(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("market_sentiment_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO market_sentiment_daily
            (trade_date, stock_count, up_count, down_count, flat_count,
             limit_up_count, first_limit_up_count, limit_down_count, yiziban_count,
             max_consecutive_limit_ups, high_board_count,
             up_ratio_pct, limit_up_ratio_pct,
             avg_pct_chg, median_pct_chg, total_amount)
            SELECT trade_date, stock_count, up_count, down_count, flat_count,
                   limit_up_count, first_limit_up_count, limit_down_count, yiziban_count,
                   max_consecutive_limit_ups, high_board_count,
                   up_ratio_pct, limit_up_ratio_pct,
                   avg_pct_chg, median_pct_chg, total_amount
            FROM market_sentiment_tmp
            """
        )
        self._conn.unregister("market_sentiment_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert market_sentiment_daily: {count} 行")
        return count

    def query_market_sentiment(self, trade_date: date | str) -> pd.DataFrame | None:
        df = self._conn.execute(
            """
            SELECT trade_date, stock_count, up_count, down_count, flat_count,
                   limit_up_count, first_limit_up_count, limit_down_count,
                   yiziban_count, max_consecutive_limit_ups, high_board_count,
                   up_ratio_pct, limit_up_ratio_pct,
                   avg_pct_chg, median_pct_chg, total_amount
            FROM market_sentiment_daily
            WHERE trade_date = ?
            """,
            [trade_date],
        ).fetchdf()
        return None if df.empty else df

    def upsert_screen_result(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("screen_result_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO screen_result
            (trade_date, preset_name, ts_code, name, close, pct_chg, extra)
            SELECT trade_date, preset_name, ts_code, name, close, pct_chg, extra
            FROM screen_result_tmp
            """
        )
        self._conn.unregister("screen_result_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert screen_result: {count} 行")
        return count

    def query_screen_result(
        self, trade_date: str, preset_name: str
    ) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, name, close, pct_chg, extra
            FROM screen_result
            WHERE strftime(trade_date, '%Y-%m-%d') = ?
              AND preset_name = ?
            ORDER BY ts_code
            """,
            [trade_date, preset_name],
        ).fetchdf()

    # ── pool2_watch ──

    def upsert_pool2_watch(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        self._conn.register("p2w_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO pool2_watch
            (ts_code, entry_date, limit_up_date,
             body_upper, body_lower,
             level_40, level_30, level_20,
             stop_strong, stop_weak, status)
            SELECT ts_code, entry_date, limit_up_date,
                   body_upper, body_lower,
                   level_40, level_30, level_20,
                   stop_strong, stop_weak, status
            FROM p2w_tmp
            """
        )
        self._conn.unregister("p2w_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert pool2_watch: {count} 行")
        return count

    def query_pool2_active(self) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, entry_date, limit_up_date,
                   body_upper, body_lower,
                   level_40, level_30, level_20,
                   stop_strong, stop_weak, status
            FROM pool2_watch
            WHERE status = 'active'
            ORDER BY entry_date DESC
            """
        ).fetchdf()

    def update_pool2_exit(
        self, ts_code: str, exit_date: date, exit_reason: str
    ) -> None:
        self._conn.execute(
            """
            UPDATE pool2_watch
            SET status = 'exited', exit_date = ?, exit_reason = ?
            WHERE ts_code = ?
            """,
            [exit_date, exit_reason, ts_code],
        )

    def remove_pool2(self, ts_code: str) -> None:
        self._conn.execute(
            "DELETE FROM pool2_watch WHERE ts_code = ?", [ts_code]
        )

    def query_pool2_all(self) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, entry_date, limit_up_date,
                   body_upper, body_lower,
                   level_40, level_30, level_20,
                   stop_strong, stop_weak,
                   status, exit_date, exit_reason
            FROM pool2_watch
            ORDER BY status, entry_date DESC
            """
        ).fetchdf()

    # ── monitor_event ──

    def upsert_monitor_event(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        self._conn.register("mev_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO monitor_event
            (trade_date, ts_code, level, trigger_price, level_price,
             trigger_time, trigger_type, pool, body_upper, body_lower)
            SELECT trade_date, ts_code, level, trigger_price, level_price,
                   trigger_time, trigger_type, pool, body_upper, body_lower
            FROM mev_tmp
            """
        )
        self._conn.unregister("mev_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert monitor_event: {count} 行")
        return count

    def query_monitor_events(
        self, trade_date: str, ts_code: str | None = None
    ) -> pd.DataFrame:
        if ts_code:
            return self._conn.execute(
                """
                SELECT * FROM monitor_event
                WHERE strftime(trade_date, '%Y-%m-%d') = ?
                  AND ts_code = ?
                ORDER BY trigger_time
                """,
                [trade_date, ts_code],
            ).fetchdf()
        return self._conn.execute(
            """
            SELECT * FROM monitor_event
            WHERE strftime(trade_date, '%Y-%m-%d') = ?
            ORDER BY trigger_time
            """,
            [trade_date],
        ).fetchdf()

    # ── auction_bar / minute_bar / intraday research ──

    def upsert_auction_bars(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        payload = df.copy()
        if "source" not in payload.columns:
            payload["source"] = "tushare"
        if "auction_type" not in payload.columns:
            payload["auction_type"] = "open_realtime"
        self._conn.register("auction_tmp", payload)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO auction_bar
            (ts_code, trade_date, auction_type, price, vol, amount,
             turnover_rate, volume_ratio, source)
            SELECT ts_code, trade_date, auction_type, price, vol, amount,
                   turnover_rate, volume_ratio, source
            FROM auction_tmp
            """
        )
        self._conn.unregister("auction_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert auction_bar: {count} 行")
        return count

    def query_auction_bars(
        self,
        trade_date: str | date | pd.Timestamp,
        *,
        auction_type: str | None = None,
    ) -> pd.DataFrame:
        if auction_type:
            return self._conn.execute(
                """
                SELECT ts_code, trade_date, auction_type, price, vol, amount,
                       turnover_rate, volume_ratio, source
                FROM auction_bar
                WHERE trade_date = ?
                  AND auction_type = ?
                ORDER BY ts_code
                """,
                [trade_date, auction_type],
            ).fetchdf()
        return self._conn.execute(
            """
            SELECT ts_code, trade_date, auction_type, price, vol, amount,
                   turnover_rate, volume_ratio, source
            FROM auction_bar
            WHERE trade_date = ?
            ORDER BY ts_code, auction_type
            """,
            [trade_date],
        ).fetchdf()

    def upsert_minute_bars(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        payload = df.copy()
        if "source" not in payload.columns:
            payload["source"] = "tushare"
        self._conn.register("minute_tmp", payload)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO minute_bar
            (ts_code, trade_time, freq, open, high, low, close, vol, amount, source)
            SELECT ts_code, trade_time, freq, open, high, low, close, vol, amount, source
            FROM minute_tmp
            """
        )
        self._conn.unregister("minute_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert minute_bar: {count} 行")
        return count

    def query_minute_bars(
        self,
        ts_code: str,
        start: str | date | pd.Timestamp,
        end: str | date | pd.Timestamp,
        *,
        freq: str = "1min",
    ) -> pd.DataFrame:
        # minute_bar 主键含 source：盘中 rt_min 写 tushare_rt / 日终 stk_mins 写
        # tushare，同一分钟可能 2-3 行。研究/回测出数必须去重（不去重成交量翻倍），
        # 历史 stk_mins 是完整权威 bar，优先于盘中实时快照。
        return self._conn.execute(
            """
            SELECT ts_code, trade_time, freq, open, high, low, close, vol, amount, source
            FROM minute_bar
            WHERE ts_code = ?
              AND freq = ?
              AND trade_time >= ?
              AND trade_time <= ?
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY ts_code, trade_time, freq
                ORDER BY CASE source
                    WHEN 'tushare' THEN 0
                    WHEN 'tushare_rt' THEN 1
                    ELSE 2
                END
            ) = 1
            ORDER BY trade_time
            """,
            [ts_code, freq, start, end],
        ).fetchdf()

    def upsert_intraday_feature_snapshot(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        self._conn.register("ifs_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO intraday_feature_snapshot
            (snapshot_id, ts_code, trade_date, as_of_time,
             feature_set, lookback_days, payload, source)
            SELECT snapshot_id, ts_code, trade_date, as_of_time,
                   feature_set, lookback_days, CAST(payload AS JSON), source
            FROM ifs_tmp
            """
        )
        self._conn.unregister("ifs_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert intraday_feature_snapshot: {count} 行")
        return count

    # ── paper_position ──

    def upsert_paper_position(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        payload = df.copy()
        optional_cols = [
            "exit_time", "exit_price", "exit_reason", "holding_trading_days",
            "pnl_pct", "trailing_stop_price", "max_drawdown_pct",
            "take_profit_basis", "feature_snapshot_id", "param_payload",
            "strategy_name", "signal_factors", "run_id",
        ]
        for col in optional_cols:
            if col not in payload.columns:
                payload[col] = None
        if "entry_price_raw" not in payload.columns:
            payload["entry_price_raw"] = payload["entry_price"]
        if "run_mode" not in payload.columns:
            payload["run_mode"] = "live"
        else:
            payload["run_mode"] = payload["run_mode"].fillna("live")
        self._conn.register("pp_tmp", payload)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO paper_position
            (position_id, trade_date, ts_code, name, pool,
             entry_time, entry_price, entry_price_raw, entry_signal, candidate_id,
             entry_level_price, entry_t_date, earliest_exit_date,
             t_close, t_high, limit_up_price_next,
             stop_loss_price, stop_loss_basis, stop_loss_pct,
             take_profit_price, take_profit_pct, take_profit_basis, trailing_stop_pct,
             trailing_stop_price, status, exit_time, exit_price, exit_reason,
             holding_trading_days, pnl_pct, max_price_seen, max_drawdown_pct,
             feature_snapshot_id, param_payload,
             strategy_name, signal_factors, run_mode, run_id, updated_at)
            SELECT position_id, trade_date, ts_code, name, pool,
                   entry_time, entry_price, entry_price_raw, entry_signal, candidate_id,
                   entry_level_price, entry_t_date, earliest_exit_date,
                   t_close, t_high, limit_up_price_next,
                   stop_loss_price, stop_loss_basis, stop_loss_pct,
                   take_profit_price, take_profit_pct, take_profit_basis, trailing_stop_pct,
                   trailing_stop_price, status, exit_time, exit_price, exit_reason,
                   holding_trading_days, pnl_pct, max_price_seen, max_drawdown_pct,
                   feature_snapshot_id, CAST(param_payload AS JSON),
                   strategy_name, CAST(signal_factors AS JSON), run_mode, run_id,
                   CURRENT_TIMESTAMP
            FROM pp_tmp
            """
        )
        self._conn.unregister("pp_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert paper_position: {count} 行")
        return count

    def query_active_paper_positions(self, trade_date: str | None = None) -> pd.DataFrame:
        if trade_date:
            return self._conn.execute(
                """
                SELECT * FROM paper_position
                WHERE status = 'open'
                  AND strftime(trade_date, '%Y-%m-%d') = ?
                ORDER BY entry_time
                """,
                [trade_date],
            ).fetchdf()
        return self._conn.execute(
            """
            SELECT * FROM paper_position
            WHERE status = 'open'
            ORDER BY entry_time
            """
        ).fetchdf()

    def query_paper_positions(
        self,
        start: date | str | None = None,
        end: date | str | None = None,
        *,
        run_mode: str | None = "live",
        strategy_name: str | None = None,
        status: str | None = None,
    ) -> pd.DataFrame:
        """复盘查询：默认只看 live 仓（run_mode=None 时不过滤，含 replay）。"""
        clauses: list[str] = []
        params: list[object] = []
        if start is not None:
            clauses.append("trade_date >= ?")
            params.append(start)
        if end is not None:
            clauses.append("trade_date <= ?")
            params.append(end)
        if run_mode is not None:
            clauses.append("run_mode = ?")
            params.append(run_mode)
        if strategy_name is not None:
            clauses.append("strategy_name = ?")
            params.append(strategy_name)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._conn.execute(
            f"""
            SELECT * FROM paper_position
            {where}
            ORDER BY entry_time, position_id
            """,
            params,
        ).fetchdf()

    def aggregate_paper_positions_by_factor_hits(
        self,
        factors: Sequence[str],
        *,
        start: date | str | None = None,
        end: date | str | None = None,
        run_mode: str | None = "live",
        strategy_name: str | None = None,
    ) -> pd.DataFrame:
        """按 signal_factors 因子命中组合切片（closed 仓），列 = 因子 hit 值 + 面板指标。

        factors 键名必须来自 FactorSpec（如 limit_progress / rel_amount_same_minute_20d），
        JSON path 无法参数绑定，因此对键名做白名单校验后拼接。
        """
        if not factors:
            msg = "factors 不能为空"
            raise ValueError(msg)
        for factor in factors:
            if not re.fullmatch(r"[A-Za-z0-9_]+", factor):
                msg = f"非法因子键名: {factor!r}"
                raise ValueError(msg)

        dims = ",\n                   ".join(
            f"json_extract_string(signal_factors, '$.factors.{factor}.hit')"
            f' AS "{factor}"'
            for factor in factors
        )
        group_by = ", ".join(str(i + 1) for i in range(len(factors)))
        clauses = ["status = 'closed'", "signal_factors IS NOT NULL"]
        params: list[object] = []
        if run_mode is not None:
            clauses.append("run_mode = ?")
            params.append(run_mode)
        if strategy_name is not None:
            clauses.append("strategy_name = ?")
            params.append(strategy_name)
        if start is not None:
            clauses.append("trade_date >= ?")
            params.append(start)
        if end is not None:
            clauses.append("trade_date <= ?")
            params.append(end)
        return self._conn.execute(
            f"""
            SELECT {dims},
                   COUNT(*) AS trades,
                   ROUND(AVG(pnl_pct), 2) AS mean_ret,
                   ROUND(MEDIAN(pnl_pct), 2) AS median_ret,
                   ROUND(100.0 * SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END)
                         / COUNT(*), 1) AS win_rate_pct,
                   ROUND(AVG(max_drawdown_pct), 2) AS avg_mdd_pct
            FROM paper_position
            WHERE {" AND ".join(clauses)}
            GROUP BY {group_by}
            ORDER BY {group_by}
            """,
            params,
        ).fetchdf()

    def delete_paper_positions_by_run_id(self, run_id: str) -> int:
        """整批删除某个 replay run 的模拟仓及其入场快照（重跑清理用）。"""
        count = self._conn.execute(
            "SELECT COUNT(*) FROM paper_position WHERE run_id = ?",
            [run_id],
        ).fetchone()[0]
        self._conn.execute(
            """
            DELETE FROM intraday_feature_snapshot
            WHERE snapshot_id IN (
                SELECT feature_snapshot_id FROM paper_position
                WHERE run_id = ? AND feature_snapshot_id IS NOT NULL
            )
            """,
            [run_id],
        )
        self._conn.execute(
            "DELETE FROM paper_position WHERE run_id = ?",
            [run_id],
        )
        logger.info(f"DuckDB 清理 paper_position run_id={run_id}: {count} 行")
        return int(count)

    def upsert_paper_position_event(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        self._conn.register("ppe_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO paper_position_event
            (event_id, position_id, event_time, event_type, price, size_pct, payload)
            SELECT event_id, position_id, event_time, event_type,
                   price, size_pct, CAST(payload AS JSON)
            FROM ppe_tmp
            """
        )
        self._conn.unregister("ppe_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert paper_position_event: {count} 行")
        return count

    def query_paper_position_events(self, position_id: str) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT * FROM paper_position_event
            WHERE position_id = ?
            ORDER BY event_time
            """,
            [position_id],
        ).fetchdf()

    # ── limit_up_pool_daily ──

    def upsert_limit_up_pool(
        self,
        df: pd.DataFrame,
        *,
        transaction_mode: Literal["auto", "standalone", "existing"] = "auto",
    ) -> int:
        if df.empty:
            return 0
        if transaction_mode not in {"auto", "standalone", "existing"}:
            raise ValueError(
                "limit-up-pool transaction_mode must be auto, standalone, or existing"
            )
        active_transaction = _duckdb_transaction_is_active(self._conn)
        if transaction_mode == "standalone" and active_transaction:
            raise ValueError(
                "standalone limit-up-pool upsert cannot join an existing transaction"
            )
        if transaction_mode == "existing" and not active_transaction:
            raise ValueError(
                "existing limit-up-pool upsert requires an active transaction"
            )
        owns_transaction = transaction_mode == "standalone" or (
            transaction_mode == "auto" and not active_transaction
        )
        payload = df.copy()
        if "source" not in payload.columns:
            payload["source"] = "eastmoney"
        stage_name = "zt_pool_tmp"
        stage_registered = False
        transaction_open = False
        try:
            if owns_transaction:
                self._conn.execute("BEGIN")
                transaction_open = True
            self._conn.register(stage_name, payload)
            stage_registered = True
            guard = self._conn.execute(
                """
                UPDATE limit_up_pool_write_guard
                SET generation = generation + 1
                WHERE guard_id = 'limit_up_pool_daily'
                RETURNING generation
                """
            ).fetchone()
            if guard is None:
                raise RuntimeError("limit-up-pool write guard row is missing")
            self._conn.execute(
                f"""
                INSERT OR REPLACE INTO limit_up_pool_daily
                (ts_code, trade_date, name, pct_chg, close, amount,
                 circ_mv, total_mv, turnover_rate, seal_amount,
                 first_seal_time, last_seal_time, break_count,
                 limit_up_stat, consecutive_boards, industry, source)
                SELECT ts_code, trade_date, name, pct_chg, close, amount,
                       circ_mv, total_mv, turnover_rate, seal_amount,
                       first_seal_time, last_seal_time, break_count,
                       limit_up_stat, consecutive_boards, industry, source
                FROM {stage_name}
                """
            )
            if owns_transaction:
                self._conn.execute("COMMIT")
                transaction_open = False
        except BaseException as primary:
            if transaction_open:
                try:
                    self._conn.execute("ROLLBACK")
                except BaseException as rollback_error:
                    raise BaseExceptionGroup(
                        "limit-up-pool upsert failed and rollback failed",
                        [primary, rollback_error],
                    ) from None
            raise
        finally:
            if stage_registered:
                try:
                    self._conn.unregister(stage_name)
                except duckdb.Error:
                    logger.exception("limit-up-pool staging view cleanup failed")
        count = len(df)
        logger.info(f"DuckDB upsert limit_up_pool_daily: {count} 行")
        return count

    def query_limit_up_pool(
        self, trade_date: str | date | pd.Timestamp
    ) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, trade_date, name, pct_chg, close, amount,
                   circ_mv, total_mv, turnover_rate, seal_amount,
                   first_seal_time, last_seal_time, break_count,
                   limit_up_stat, consecutive_boards, industry, source
            FROM limit_up_pool_daily
            WHERE trade_date = ?
            ORDER BY consecutive_boards DESC, ts_code
            """,
            [trade_date],
        ).fetchdf()

    # ── limit_list_daily ──

    def upsert_limit_list(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        payload = df.copy()
        # Tushare 源字段名 limit 是 SQL 关键字，落库统一改名 limit_status
        if "limit" in payload.columns and "limit_status" not in payload.columns:
            payload = payload.rename(columns={"limit": "limit_status"})
        self._conn.register("limit_list_tmp", payload)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO limit_list_daily
            (ts_code, trade_date, name, industry, close, pct_chg, amount,
             limit_amount, float_mv, total_mv, turnover_ratio, fd_amount,
             first_time, last_time, open_times, up_stat, limit_times, limit_status)
            SELECT ts_code, trade_date, name, industry, close, pct_chg, amount,
                   limit_amount, float_mv, total_mv, turnover_ratio, fd_amount,
                   first_time, last_time, open_times, up_stat, limit_times, limit_status
            FROM limit_list_tmp
            """
        )
        self._conn.unregister("limit_list_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert limit_list_daily: {count} 行")
        return count

    def query_limit_list(
        self,
        trade_date: str | date | pd.Timestamp | None = None,
        *,
        start: str | date | None = None,
        end: str | date | None = None,
        limit_status: str | None = None,
    ) -> pd.DataFrame:
        """按单日（trade_date）或区间（start/end）查涨跌停榜。

        limit_status 可选过滤 U/D/Z；trade_date 与 start/end 互斥，
        都不传视为调用方约定错误直接抛。
        """
        where: list[str] = []
        params: list[object] = []
        if trade_date is not None:
            where.append("trade_date = ?")
            params.append(trade_date)
        else:
            if start is not None:
                where.append("trade_date >= ?")
                params.append(start)
            if end is not None:
                where.append("trade_date <= ?")
                params.append(end)
        if not where:
            raise ValueError("query_limit_list 需要 trade_date 或 start/end 区间")
        if limit_status:
            where.append("limit_status = ?")
            params.append(limit_status)
        sql = f"""
            SELECT ts_code, trade_date, name, industry, close, pct_chg, amount,
                   limit_amount, float_mv, total_mv, turnover_ratio, fd_amount,
                   first_time, last_time, open_times, up_stat, limit_times,
                   limit_status
            FROM limit_list_daily
            WHERE {" AND ".join(where)}
            ORDER BY trade_date, limit_status, limit_times DESC, ts_code
        """
        return self._conn.execute(sql, params).fetchdf()

    def count_daily_basic(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM daily_basic WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM daily_basic").fetchone()
        return result[0] if result else 0

    def get_state(
        self,
        ts_code: str,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        params: list[str] = [ts_code]
        where = "ts_code = ?"
        if start:
            where += " AND trade_date >= ?"
            params.append(start)
        if end:
            where += " AND trade_date <= ?"
            params.append(end)
        sql = f"""
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date,
               is_st, is_bj, board_type, limit_pct,
               limit_up_price, limit_down_price,
               is_limit_up, is_limit_down,
               is_first_limit_up, is_yiziban,
               consecutive_limit_ups,
               body_upper, body_lower
        FROM daily_state
        WHERE {where}
        ORDER BY trade_date
        """
        return self._conn.execute(sql, params).fetchdf()

    def upsert_stock_basic(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        df = df.copy()
        if "list_date" in df.columns:
            df["list_date"] = pd.to_datetime(df["list_date"], format="%Y%m%d").dt.date

        self._conn.register("basic_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO stock_basic
            (ts_code, symbol, name, area, industry, list_date, market)
            SELECT ts_code, symbol, name, area, industry, list_date, market
            FROM basic_tmp
            """
        )
        self._conn.unregister("basic_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert stock_basic: {count} 行")
        return count

    # ── dataset_backfill 通用落库 ──
    # 表名/列名拼进 SQL 前先过 information_schema 白名单（未知表直接抛），
    # 列取 df 与目标表交集并全部加引号，防关键字撞车（limit/rank 之类已在
    # normalize 层改名，这里是兜底）。

    def _dataset_table_columns(self, table: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = ?",
            [table],
        ).fetchall()
        return [r[0] for r in rows]

    def _dataset_pk_columns(self, table: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT unnest(constraint_column_names) FROM duckdb_constraints() "
            "WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'",
            [table],
        ).fetchall()
        return [r[0] for r in rows]

    def _dataset_insert_cols(self, table: str, df: pd.DataFrame) -> list[str]:
        cols = self._dataset_table_columns(table)
        if not cols:
            raise ValueError(f"未知数据表：{table}（schema.ALL_DDL 里没建过）")
        use = [c for c in df.columns if c in cols]
        if not use:
            raise ValueError(f"{table} 与 DataFrame 无共同列：{list(df.columns)}")
        return use

    def upsert_dataset(self, table: str, df: pd.DataFrame) -> int:
        """通用幂等 upsert：按 df 与目标表列交集 INSERT OR REPLACE。

        同批次内主键重复先去重保留末行——INSERT OR REPLACE 对批内自冲突
        直接报错，而榜单类源数据偶发重复行不值得整日失败。
        """
        if df.empty:
            return 0
        use = self._dataset_insert_cols(table, df)
        pk = [c for c in self._dataset_pk_columns(table) if c in use]
        payload = df.drop_duplicates(subset=pk, keep="last") if pk else df
        quoted = ", ".join(f'"{c}"' for c in use)
        self._conn.register("dataset_tmp", payload)
        self._conn.execute(
            f'INSERT OR REPLACE INTO "{table}" ({quoted}) '
            f"SELECT {quoted} FROM dataset_tmp"
        )
        self._conn.unregister("dataset_tmp")
        count = len(payload)
        logger.info(f"DuckDB upsert {table}: {count} 行")
        return count

    def replace_dataset(self, table: str, df: pd.DataFrame) -> int:
        """快照整表替换（事务内 DELETE + INSERT，成分调出即删）。

        空 df 拒绝替换（源抽风返回空不该清掉现有快照），调用方约定错误抛。
        """
        if df.empty:
            raise ValueError(f"replace_dataset 拒绝空快照：{table}")
        use = self._dataset_insert_cols(table, df)
        pk = [c for c in self._dataset_pk_columns(table) if c in use]
        payload = df.drop_duplicates(subset=pk, keep="last") if pk else df
        quoted = ", ".join(f'"{c}"' for c in use)
        self._conn.register("dataset_tmp", payload)
        try:
            self._conn.execute("BEGIN")
            self._conn.execute(f'DELETE FROM "{table}"')
            self._conn.execute(
                f'INSERT INTO "{table}" ({quoted}) '
                f"SELECT {quoted} FROM dataset_tmp"
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        finally:
            self._conn.unregister("dataset_tmp")
        count = len(payload)
        logger.info(f"DuckDB replace {table}: {count} 行（整表替换）")
        return count

    def query(self, sql: str) -> pd.DataFrame:
        return self._conn.execute(sql).fetchdf()

    def count_daily(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM daily_bar WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM daily_bar").fetchone()
        return result[0] if result else 0

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> DuckDBStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


# ── Read-only helpers (避开 monitor 写锁) ──────────────────────────────────────
#
# DuckDB 单文件锁：monitor 盘中 9:25-15:00 持写锁期间，任何新连接（含 read_only）
# 都开不了。dashboard / canvas / nl-screen 这类只读消费者应优先连副本
# (rquant_ro.duckdb)，由 rquant-replica-sync.timer 每 5min 同步。
#
# 副本不存在（首次部署 / sync 还没跑过）→ 降级主库 read_only（可能撞锁）
# 副本损坏（cp 时撞 monitor fsync）→ 降级主库 read_only

def _readonly_candidate_paths() -> list[Path]:
    """返回 read_only 打开的候选路径列表，按优先级排序（副本在前）。"""
    ro = settings.duckdb_readonly_path_resolved
    if ro.exists():
        return [ro, settings.duckdb_path]
    return [settings.duckdb_path]


def _store_has_required_tables(
    store: DuckDBStore,
    required_tables: list[str] | tuple[str, ...] | None,
) -> bool:
    if not required_tables:
        return True
    rows = store._conn.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name IN (
        """
        + ",".join("?" for _ in required_tables)
        + ")",
        list(required_tables),
    ).fetchall()
    existing = {row[0] for row in rows}
    missing = set(required_tables) - existing
    if missing:
        logger.warning(f"只读库缺少表 {sorted(missing)}，尝试下一个候选库")
        return False
    return True


def open_readonly_store(
    *,
    required_tables: list[str] | tuple[str, ...] | None = None,
) -> DuckDBStore:
    """打开 DuckDBStore（read_only），优先副本，副本不可用降级主库。

    主库也撞锁时 raise duckdb.IOException，由 caller 渲染友好提示。
    """
    paths = _readonly_candidate_paths()
    for p in paths[:-1]:
        try:
            store = DuckDBStore(p, read_only=True)
            if _store_has_required_tables(store, required_tables):
                return store
            store.close()
        except duckdb.IOException as e:
            logger.warning(f"副本打开失败 {p}: {e}，降级到主库 read_only")
    store = DuckDBStore(paths[-1], read_only=True)
    if not _store_has_required_tables(store, required_tables):
        store.close()
        raise duckdb.CatalogException(
            f"required tables missing: {list(required_tables or [])}"
        )
    return store


def open_readonly_connection() -> duckdb.DuckDBPyConnection:
    """裸 duckdb.connect 版本，给 dashboard/app.py 用（它不走 DuckDBStore）。"""
    paths = _readonly_candidate_paths()
    for p in paths[:-1]:
        try:
            return duckdb.connect(str(p), read_only=True)
        except duckdb.IOException as e:
            logger.warning(f"副本打开失败 {p}: {e}，降级到主库 read_only")
    return duckdb.connect(str(paths[-1]), read_only=True)
