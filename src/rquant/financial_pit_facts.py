"""Import sealed financial observations and select one field at a historical instant."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import duckdb
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from rquant.financial_pit import (
    FinancialFact,
    FinancialPITSelection,
    SSECalendar,
    SSECalendarDay,
    select_financial_fact,
)
from rquant.financial_pit_acquisition import (
    FinancialAPI,
    FinancialArchive,
    FinancialCommittedEntry,
    FinancialCommittedPage,
    FinancialScalar,
)

_TYPED_APIS = frozenset({"income", "balancesheet", "cashflow"})
_DEFAULT_REPORT_TYPE = "default"
_MAX_CANDIDATE_ROWS = 10_000
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class FinancialPITQuery(_Model):
    source_api: FinancialAPI
    field: str = Field(min_length=1)
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(SH|SZ|BJ)$")
    report_period: date
    report_type: str = Field(min_length=1)
    as_of: AwareDatetime


class FinancialObservation(_Model):
    source_api: FinancialAPI
    ts_code: str
    observed_at: AwareDatetime
    report_period: date | None
    report_type: str | None
    ann_date: date | None
    f_ann_date: date | None
    values: dict[str, FinancialScalar]
    pit_usable: bool
    conflicted: bool


def _date(value: FinancialScalar) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) != 8 or not value.isascii() or not value.isdigit():
        raise ValueError("sealed supplier date is invalid")
    return datetime.strptime(value, "%Y%m%d").date()


def _report_type(api: FinancialAPI, values: dict[str, FinancialScalar]) -> str | None:
    if api in _TYPED_APIS:
        raw = values.get("report_type")
    elif api == "forecast":
        raw = values.get("type")
    else:
        return _DEFAULT_REPORT_TYPE
    return raw if isinstance(raw, str) and raw.strip() else None


def _json(value: object) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _batch_record(archive_id: str, entry: FinancialCommittedEntry) -> tuple[object, ...]:
    receipt = entry.receipt
    return (
        archive_id,
        str(receipt.query.request_id),
        _json(receipt.query.model_dump(mode="json")),
        receipt.observed_at,
        receipt.status,
        receipt.row_count,
        receipt.relative_path,
        receipt.file_sha256,
        receipt.byte_count,
    )


def _observation_records(
    archive_id: str, entry: FinancialCommittedEntry
) -> tuple[tuple[object, ...], ...]:
    receipt = entry.receipt
    return tuple(
        (
            archive_id,
            str(receipt.query.request_id),
            index,
            receipt.query.api,
            receipt.query.ts_code,
            receipt.observed_at,
            _date(row.values.get("end_date")),
            _report_type(receipt.query.api, row.values),
            _date(row.values.get("ann_date")),
            _date(row.values.get("f_ann_date")),
            _json(row.values),
            row.content_sha256,
            row.pit_usable,
            row.conflicted,
        )
        for index, row in enumerate(entry.batch.rows)
    )


def _cursor(conn: duckdb.DuckDBPyConnection) -> tuple[str, datetime | None, int, str] | None:
    row = conn.execute(
        "SELECT archive_id, last_observed_at, anchor_generation, anchor_record_sha256 "
        "FROM financial_import_cursor WHERE singleton = 1"
    ).fetchone()
    latest = conn.execute("SELECT MAX(observed_at) FROM financial_import_batch").fetchone()[0]
    if row is None:
        if latest is not None:
            raise ValueError("financial import cursor is missing behind committed batches")
        return None
    if row[1] != latest:
        raise ValueError("financial import cursor has rolled back or advanced past batches")
    archives = conn.execute(
        "SELECT DISTINCT archive_id FROM financial_import_batch LIMIT 2"
    ).fetchall()
    if any(archive_id != row[0] for (archive_id,) in archives):
        raise ValueError("financial import ledger contains another archive")
    return row


def _same_record(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    key: tuple[object, ...],
    expected: tuple[object, ...],
) -> bool:
    predicates = " AND ".join(
        f"{name} = ?" for name in ("archive_id", "request_id", "row_index")[: len(key)]
    )
    actual = conn.execute(f"SELECT * FROM {table} WHERE {predicates}", key).fetchone()
    if actual is None:
        return False
    if actual != expected:
        raise ValueError(f"existing {table} record differs from sealed archive")
    return True


def _import_page(
    conn: duckdb.DuckDBPyConnection,
    page: FinancialCommittedPage,
    expected_cursor: tuple[str, datetime | None, int, str] | None,
) -> None:
    conn.execute("BEGIN TRANSACTION")
    try:
        current = _cursor(conn)
        if current != expected_cursor:
            raise ValueError("financial import cursor changed during page read")
        if current is not None:
            if current[0] != page.archive_id:
                raise ValueError("financial archive identity changed")
            if page.anchor_generation < current[2] or (
                page.anchor_generation == current[2] and page.anchor_record_sha256 != current[3]
            ):
                raise ValueError("financial archive anchor is not a successor")
        last_at = current[1] if current is not None else None
        previous_page_at: datetime | None = None
        for entry in page.entries:
            receipt = entry.receipt
            batch = entry.batch
            if (
                receipt.query != batch.query
                or receipt.observed_at != batch.observed_at
                or receipt.status != batch.status
                or receipt.row_count != len(batch.rows)
                or (previous_page_at is not None and receipt.observed_at <= previous_page_at)
            ):
                raise ValueError("financial page has an invalid or regressing batch")
            previous_page_at = receipt.observed_at
            batch_record = _batch_record(page.archive_id, entry)
            key = batch_record[:2]
            existing = _same_record(conn, "financial_import_batch", key, batch_record)
            if existing:
                count = conn.execute(
                    "SELECT COUNT(*) FROM financial_observation "
                    "WHERE archive_id = ? AND request_id = ?",
                    key,
                ).fetchone()[0]
                if count != receipt.row_count:
                    raise ValueError("existing financial batch has a different row count")
            else:
                if last_at is not None and receipt.observed_at <= last_at:
                    raise ValueError("financial page omits an earlier committed batch")
                conn.execute(
                    "INSERT INTO financial_import_batch VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    batch_record,
                )
            for record in _observation_records(page.archive_id, entry):
                row_exists = _same_record(conn, "financial_observation", record[:3], record)
                if existing and not row_exists:
                    raise ValueError("existing financial batch is missing an observation")
                if not existing and row_exists:
                    raise ValueError("new financial batch overlaps an observation")
                if not row_exists:
                    conn.execute(
                        "INSERT INTO financial_observation VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        record,
                    )
            last_at = (
                max(last_at, receipt.observed_at) if last_at is not None else receipt.observed_at
            )
        if last_at is not None and (page.high_water is None or last_at > page.high_water):
            raise ValueError("financial page exceeds archive high-water")
        conn.execute(
            "INSERT INTO financial_import_cursor VALUES (1, ?, ?, ?, ?) "
            "ON CONFLICT (singleton) DO UPDATE SET archive_id = excluded.archive_id, "
            "last_observed_at = excluded.last_observed_at, "
            "anchor_generation = excluded.anchor_generation, "
            "anchor_record_sha256 = excluded.anchor_record_sha256",
            (page.archive_id, last_at, page.anchor_generation, page.anchor_record_sha256),
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def import_financial_archive(
    conn: duckdb.DuckDBPyConnection,
    archive: FinancialArchive,
    *,
    page_size: int = 32,
) -> int:
    """Import validated pages into a temporary migrated DuckDB connection."""

    imported = 0
    while True:
        cursor = _cursor(conn)
        page = archive.committed_page(
            after=cursor[1] if cursor is not None else None,
            limit=page_size,
            accepted_anchor_generation=cursor[2] if cursor is not None else None,
            accepted_anchor_sha256=cursor[3] if cursor is not None else None,
        )
        _import_page(conn, page, cursor)
        imported += len(page.entries)
        if not page.has_more:
            return imported
        if not page.entries:
            raise ValueError("archive page claims more batches without advancing")


def _value(value: FinancialScalar) -> Decimal | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def field_versions(
    observations: Sequence[FinancialObservation], query: FinancialPITQuery
) -> tuple[FinancialFact, ...]:
    """Collapse only adjacent versions of the requested field across all query pages."""

    facts: list[FinancialFact] = []
    previous_signature: tuple[Decimal, date, date | None] | None = None
    previous_at: datetime | None = None
    for row in observations:
        if row.source_api != query.source_api or row.ts_code != query.ts_code:
            raise ValueError("financial observations do not match the requested security and API")
        observed_at = row.observed_at.astimezone(UTC)
        if previous_at is not None and observed_at < previous_at:
            raise ValueError("financial observations are not ordered")
        previous_at = observed_at
        unkeyed = row.report_period is None or row.report_type is None
        if not unkeyed and (row.report_period, row.report_type) != (
            query.report_period,
            query.report_type,
        ):
            raise ValueError("financial observations have mixed logical keys")
        raw_value = row.values.get(query.field)
        value = _value(raw_value)
        reason = (
            "unkeyed_observation"
            if unkeyed
            else "conflicted_observation"
            if row.conflicted
            else "missing_announcement_date"
            if row.ann_date is None
            else "missing_field"
            if query.field not in row.values
            else "missing_value"
            if value is None
            else "unusable_observation"
            if not row.pit_usable
            else None
        )
        if reason is not None:
            previous_signature = None
            facts.append(
                FinancialFact(
                    source_api=query.source_api,
                    field=query.field,
                    ts_code=query.ts_code,
                    report_period=query.report_period,
                    report_type=query.report_type,
                    ann_date=row.ann_date,
                    f_ann_date=row.f_ann_date,
                    first_observed_at=observed_at,
                    value=None,
                    block_reason=reason,
                )
            )
            continue
        assert value is not None and row.ann_date is not None
        signature = (value, row.ann_date, row.f_ann_date)
        if signature == previous_signature:
            continue
        previous_signature = signature
        facts.append(
            FinancialFact(
                source_api=query.source_api,
                field=query.field,
                ts_code=query.ts_code,
                report_period=query.report_period,
                report_type=query.report_type,
                ann_date=row.ann_date,
                f_ann_date=row.f_ann_date,
                first_observed_at=observed_at,
                value=value,
                update_flag=(
                    str(row.values["update_flag"])
                    if row.values.get("update_flag") is not None
                    else None
                ),
            )
        )
    return tuple(facts)


def _calendar(conn: duckdb.DuckDBPyConnection, start: date, end: date) -> SSECalendar:
    rows = conn.execute(
        "SELECT cal_date, is_open FROM trade_calendar "
        "WHERE exchange = 'SSE' AND cal_date BETWEEN ? AND ? ORDER BY cal_date",
        (start, end),
    ).fetchall()
    return SSECalendar(
        coverage_start=start,
        coverage_end=end,
        days=tuple(SSECalendarDay(day=day, is_open=is_open) for day, is_open in rows),
    )


def query_financial_pit(
    conn: duckdb.DuckDBPyConnection,
    query: FinancialPITQuery,
    *,
    max_candidate_rows: int = _MAX_CANDIDATE_ROWS,
) -> FinancialPITSelection:
    """Read every relevant raw row and let the sole financial selector apply both gates."""

    if type(max_candidate_rows) is not int or not 0 <= max_candidate_rows <= _MAX_CANDIDATE_ROWS:
        raise ValueError("financial candidate limit is outside its fixed bound")
    try:
        as_of = query.as_of.astimezone(UTC)
        local_day = query.as_of.astimezone(_SHANGHAI).date()
    except (OverflowError, ValueError):
        return FinancialPITSelection(status="unknown", reason="invalid_as_of")
    try:
        cursor = _cursor(conn)
    except ValueError:
        return FinancialPITSelection(status="unknown", reason="invalid_import_cursor")
    if cursor is None:
        return FinancialPITSelection(status="unknown", reason="no_facts")
    rows = conn.execute(
        "SELECT o.source_api, o.ts_code, o.observed_at, o.report_period, o.report_type, "
        "o.ann_date, o.f_ann_date, o.raw_json, o.pit_usable, o.conflicted "
        "FROM financial_observation AS o JOIN financial_import_batch AS b "
        "ON b.archive_id = o.archive_id AND b.request_id = o.request_id "
        "AND b.observed_at = o.observed_at "
        "WHERE o.archive_id = ? AND o.source_api = ? AND o.ts_code = ? "
        "AND o.observed_at < ? AND ((o.report_period = ? AND o.report_type = ?) "
        "OR o.report_period IS NULL OR o.report_type IS NULL) "
        "ORDER BY o.observed_at, o.request_id, o.row_index LIMIT ?",
        (
            cursor[0],
            query.source_api,
            query.ts_code,
            as_of,
            query.report_period,
            query.report_type,
            max_candidate_rows + 1,
        ),
    ).fetchall()
    if len(rows) > max_candidate_rows:
        return FinancialPITSelection(status="unknown", reason="candidate_limit")
    observations = tuple(
        FinancialObservation(
            source_api=row[0],
            ts_code=row[1],
            observed_at=row[2],
            report_period=row[3],
            report_type=row[4],
            ann_date=row[5],
            f_ann_date=row[6],
            values=json.loads(row[7]),
            pit_usable=row[8],
            conflicted=row[9],
        )
        for row in rows
    )
    facts = field_versions(observations, query)
    publication_dates = (
        max(fact.ann_date, fact.f_ann_date or fact.ann_date)
        for fact in facts
        if fact.block_reason is None and fact.ann_date is not None
    )
    start = min((day for day in publication_dates if day <= local_day), default=local_day)
    return select_financial_fact(
        facts,
        as_of=query.as_of,
        calendar=_calendar(conn, start, local_day),
    )
