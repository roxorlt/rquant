"""A frozen retrospective calendar needs complete civil days and open-day anchors."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.backtest.calendar_source import (
    CalendarSourceError,
    load_retrospective_sse_calendar,
    verify_retrospective_sse_calendar,
)
from rquant.storage.schema import TRADE_CALENDAR_DDL

START = date(2026, 1, 2)
END = date(2026, 1, 6)
UPDATED_AT = datetime(2026, 1, 8, 9, 0, tzinfo=UTC)


def _rows() -> list[tuple[str, date, bool, date, str, datetime]]:
    return [
        ("SSE", date(2025, 12, 31), True, date(2025, 12, 30), "tushare", UPDATED_AT),
        ("SSE", date(2026, 1, 1), False, date(2025, 12, 31), "tushare", UPDATED_AT),
        ("SSE", date(2026, 1, 2), True, date(2025, 12, 31), "tushare", UPDATED_AT),
        ("SSE", date(2026, 1, 3), False, date(2026, 1, 2), "tushare", UPDATED_AT),
        ("SSE", date(2026, 1, 4), False, date(2026, 1, 2), "tushare", UPDATED_AT),
        ("SSE", date(2026, 1, 5), True, date(2026, 1, 2), "tushare", UPDATED_AT),
        ("SSE", date(2026, 1, 6), True, date(2026, 1, 5), "tushare", UPDATED_AT),
        ("SSE", date(2026, 1, 7), True, date(2026, 1, 6), "tushare", UPDATED_AT),
    ]


def _frozen_db(
    path: Path,
    rows: list[tuple[str, date, bool, date, str, datetime]] | None = None,
    *,
    ddl: str = TRADE_CALENDAR_DDL,
) -> Path:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(ddl)
        connection.executemany(
            "INSERT INTO trade_calendar "
            "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            _rows() if rows is None else rows,
        )
    finally:
        connection.close()
    return path


class _AuditedConnection:
    def __init__(self, connection: duckdb.DuckDBPyConnection, statements: list[str]) -> None:
        self.connection = connection
        self.statements = statements

    def execute(self, query: str, parameters: list[object] | None = None) -> _AuditedConnection:
        self.statements.append(query)
        self.connection.execute(query, parameters)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self.connection.fetchone()

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.connection.fetchall()

    def close(self) -> None:
        self.connection.close()


def test_holiday_calendar_has_hand_checkable_previous_and_next_open_days(
    tmp_path: Path,
) -> None:
    snapshot = load_retrospective_sse_calendar(
        _frozen_db(tmp_path / "calendar.duckdb"), start=START, end=END
    )

    assert snapshot.source_mode == "retrospective_trade_calendar"
    assert snapshot.source_table == "trade_calendar"
    assert snapshot.requested_start == START
    assert snapshot.requested_end == END
    assert snapshot.calendar.exchange == "SSE"
    assert snapshot.calendar.dates == (
        date(2025, 12, 31),
        date(2026, 1, 2),
        date(2026, 1, 5),
        date(2026, 1, 6),
        date(2026, 1, 7),
    )
    assert snapshot.calendar.dates[1] == snapshot.rows[2].cal_date
    jan_2_index = snapshot.calendar.dates.index(date(2026, 1, 2))
    jan_5_index = snapshot.calendar.dates.index(date(2026, 1, 5))
    assert snapshot.calendar.dates[jan_2_index - 1] == date(2025, 12, 31)
    assert snapshot.calendar.dates[jan_2_index + 1] == date(2026, 1, 5)
    assert snapshot.calendar.dates[jan_5_index - 1] == date(2026, 1, 2)
    assert snapshot.calendar.dates[jan_5_index + 1] == date(2026, 1, 6)
    assert len(snapshot.rows) == 8
    assert len(snapshot.calendar.source_identity) == 64
    assert snapshot.calendar.source_identity == snapshot.source_identity


def test_read_is_read_only_single_transaction_and_digest_ignores_row_order_and_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _frozen_db(tmp_path / "first.duckdb")
    second = _frozen_db(tmp_path / "second.duckdb", list(reversed(_rows())))
    original_connect = duckdb.connect
    calls: list[bool | None] = []
    statements: list[str] = []

    def audited_connect(*args: object, **kwargs: object) -> _AuditedConnection:
        calls.append(kwargs.get("read_only"))
        return _AuditedConnection(original_connect(*args, **kwargs), statements)

    monkeypatch.setattr(duckdb, "connect", audited_connect)
    first_snapshot = load_retrospective_sse_calendar(first, start=START, end=END)
    second_snapshot = load_retrospective_sse_calendar(second, start=START, end=END)

    assert calls == [True, True]
    assert [sql for sql in statements if sql in {"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"}] == [
        "BEGIN TRANSACTION",
        "COMMIT",
        "BEGIN TRANSACTION",
        "COMMIT",
    ]
    assert first_snapshot.calendar.source_identity == second_snapshot.calendar.source_identity
    assert first_snapshot.rows == second_snapshot.rows


def test_core_reuses_callers_existing_read_only_transaction(tmp_path: Path) -> None:
    path = _frozen_db(tmp_path / "shared-transaction.duckdb")
    statements: list[str] = []
    connection = _AuditedConnection(duckdb.connect(str(path), read_only=True), statements)
    try:
        connection.execute("BEGIN TRANSACTION")
        statements.clear()
        snapshot = verify_retrospective_sse_calendar(connection, start=START, end=END)
        assert all(sql not in {"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"} for sql in statements)
        assert connection.execute("SELECT count(*) FROM trade_calendar").fetchone() == (8,)
        connection.execute("COMMIT")
    finally:
        connection.close()
    assert snapshot.calendar.dates[1] == START


@pytest.mark.parametrize("changed_field", ["is_open", "source", "updated_at"])
def test_business_facts_and_provenance_change_digest(tmp_path: Path, changed_field: str) -> None:
    original = _frozen_db(tmp_path / "original.duckdb")
    baseline = load_retrospective_sse_calendar(original, start=START, end=END)
    rows = _rows()
    if changed_field == "is_open":
        row = list(rows[4])
        row[2] = True
        rows[4] = tuple(row)  # type: ignore[assignment]
        next_row = list(rows[5])
        next_row[3] = date(2026, 1, 4)
        rows[5] = tuple(next_row)  # type: ignore[assignment]
    elif changed_field == "source":
        row = list(rows[4])
        row[4] = "corrected"
        rows[4] = tuple(row)  # type: ignore[assignment]
    else:
        row = list(rows[4])
        row[5] = UPDATED_AT + timedelta(minutes=1)
        rows[4] = tuple(row)  # type: ignore[assignment]
    changed = _frozen_db(tmp_path / "changed.duckdb", rows)

    revised = load_retrospective_sse_calendar(changed, start=START, end=END)
    assert revised.calendar.source_identity != baseline.calendar.source_identity


@pytest.mark.parametrize("missing", [date(2026, 1, 1), date(2026, 1, 3)])
def test_missing_civil_day_is_rejected(tmp_path: Path, missing: date) -> None:
    path = _frozen_db(tmp_path / "gap.duckdb", [row for row in _rows() if row[1] != missing])

    with pytest.raises(CalendarSourceError, match="missing.*trade calendar|missing.*calendar"):
        load_retrospective_sse_calendar(path, start=START, end=END)


def test_broken_pretrade_chain_is_rejected(tmp_path: Path) -> None:
    rows = _rows()
    bad = list(rows[5])
    bad[3] = date(2025, 12, 31)
    rows[5] = tuple(bad)  # type: ignore[assignment]
    path = _frozen_db(tmp_path / "chain.duckdb", rows)

    with pytest.raises(CalendarSourceError, match="pretrade_date chain mismatch"):
        load_retrospective_sse_calendar(path, start=START, end=END)


@pytest.mark.parametrize("conflicting", [False, True])
def test_duplicate_calendar_date_is_rejected_even_without_primary_key(
    tmp_path: Path, conflicting: bool
) -> None:
    ddl = """
        CREATE TABLE trade_calendar (
            exchange VARCHAR NOT NULL, cal_date DATE NOT NULL, is_open BOOLEAN NOT NULL,
            pretrade_date DATE, source VARCHAR NOT NULL, updated_at TIMESTAMPTZ NOT NULL
        )
    """
    duplicate = list(_rows()[4])
    if conflicting:
        duplicate[2] = True
    rows = [*_rows(), tuple(duplicate)]  # type: ignore[list-item]
    path = _frozen_db(tmp_path / "duplicate.duckdb", rows, ddl=ddl)

    with pytest.raises(CalendarSourceError, match="duplicate"):
        load_retrospective_sse_calendar(path, start=START, end=END)


@pytest.mark.parametrize(
    ("changed_date", "field_index", "new_value", "message"),
    [
        (date(2026, 1, 5), 4, "", "source"),
        (date(2026, 1, 5), 3, None, "pretrade_date"),
        (date(2026, 1, 5), 0, "SZSE", "missing"),
    ],
)
def test_invalid_or_wrong_exchange_fields_are_rejected(
    tmp_path: Path, changed_date: date, field_index: int, new_value: object, message: str
) -> None:
    rows = _rows()
    index = next(i for i, row in enumerate(rows) if row[1] == changed_date)
    bad = list(rows[index])
    bad[field_index] = new_value
    rows[index] = tuple(bad)  # type: ignore[assignment]
    path = _frozen_db(tmp_path / "invalid.duckdb", rows)

    with pytest.raises(CalendarSourceError, match=message):
        load_retrospective_sse_calendar(path, start=START, end=END)


@pytest.mark.parametrize("anchor", [date(2025, 12, 31), date(2026, 1, 7)])
def test_preceding_and_following_open_anchors_are_required(tmp_path: Path, anchor: date) -> None:
    path = _frozen_db(tmp_path / "anchor.duckdb", [row for row in _rows() if row[1] != anchor])

    with pytest.raises(CalendarSourceError, match="preceding|following"):
        load_retrospective_sse_calendar(path, start=START, end=END)


def test_requested_closed_only_range_is_rejected(tmp_path: Path) -> None:
    path = _frozen_db(tmp_path / "closed.duckdb")

    with pytest.raises(CalendarSourceError, match="no SSE open day"):
        load_retrospective_sse_calendar(path, start=date(2026, 1, 1), end=date(2026, 1, 1))


def test_missing_or_legacy_schema_and_naive_update_column_are_rejected(tmp_path: Path) -> None:
    empty_path = tmp_path / "missing.duckdb"
    with pytest.raises(CalendarSourceError, match="existing frozen"):
        load_retrospective_sse_calendar(empty_path, start=START, end=END)

    connection = duckdb.connect(str(empty_path))
    connection.close()
    with pytest.raises(CalendarSourceError, match="schema"):
        load_retrospective_sse_calendar(empty_path, start=START, end=END)

    legacy_path = tmp_path / "legacy.duckdb"
    connection = duckdb.connect(str(legacy_path))
    connection.execute(
        "CREATE TABLE trade_calendar (exchange VARCHAR, cal_date DATE, is_open BOOLEAN)"
    )
    connection.close()
    with pytest.raises(CalendarSourceError, match="schema"):
        load_retrospective_sse_calendar(legacy_path, start=START, end=END)

    naive_path = tmp_path / "naive.duckdb"
    naive_ddl = TRADE_CALENDAR_DDL.replace("TIMESTAMPTZ", "TIMESTAMP")
    _frozen_db(naive_path, _rows(), ddl=naive_ddl)
    with pytest.raises(CalendarSourceError, match="schema|timezone"):
        load_retrospective_sse_calendar(naive_path, start=START, end=END)


def test_range_limit_and_invalid_dates_fail_before_database_open(tmp_path: Path) -> None:
    absent = tmp_path / "absent.duckdb"
    with pytest.raises(CalendarSourceError, match="3660"):
        load_retrospective_sse_calendar(absent, start=date(2019, 1, 1), end=date(2030, 1, 1))
    with pytest.raises(CalendarSourceError, match="date"):
        load_retrospective_sse_calendar(absent, start="2026-01-02", end=END)  # type: ignore[arg-type]
    with pytest.raises(CalendarSourceError, match="start"):
        load_retrospective_sse_calendar(absent, start=END, end=START)
