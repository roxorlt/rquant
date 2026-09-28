"""Retrospective stock prices are exact frozen facts, not execution evidence."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from rquant.backtest.daily_price_source import (
    MAX_REQUEST_CODES,
    MAX_SQL_BATCH,
    DailyPriceSourceError,
    load_retrospective_daily_prices,
    verify_retrospective_daily_prices,
)
from rquant.storage.schema import DAILY_BAR_DDL

DAY_1 = date(2026, 8, 10)
DAY_2 = date(2026, 8, 11)
CODE_1 = "600000.SH"
CODE_2 = "000001.SZ"
REQUEST = ((DAY_1, CODE_1), (DAY_1, CODE_2), (DAY_2, CODE_1))
ROWS = (
    (CODE_1, DAY_1, 10.0, 10.5, 9.8),
    (CODE_2, DAY_1, 8.0, 8.2, 7.9),
    (CODE_1, DAY_2, 10.6, 10.4, 10.5),
)


def _frozen_db(
    path: Path,
    rows: tuple[tuple[str, date, float | None, float | None, float | None], ...] = ROWS,
    *,
    ddl: str = DAILY_BAR_DDL,
) -> Path:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(ddl)
        for row in rows:
            connection.execute(
                "INSERT INTO daily_bar (ts_code, trade_date, open, close, pre_close) "
                "VALUES (?, ?, ?, ?, ?)",
                list(row),
            )
    finally:
        connection.close()
    return path


def test_exact_prices_from_readonly_frozen_file_are_not_timed_execution_evidence(
    tmp_path: Path,
) -> None:
    snapshot = load_retrospective_daily_prices(_frozen_db(tmp_path / "frozen.duckdb"), REQUEST)

    assert snapshot.source_mode == "retrospective_daily_bar"
    assert snapshot.source_table == "daily_bar"
    assert [
        (row.trade_date, row.ts_code, row.open, row.close, row.pre_close) for row in snapshot.rows
    ] == [
        (DAY_1, CODE_2, 8.0, 8.2, 7.9),
        (DAY_1, CODE_1, 10.0, 10.5, 9.8),
        (DAY_2, CODE_1, 10.6, 10.4, 10.5),
    ]
    assert len(snapshot.source_identity) == 64
    assert not hasattr(snapshot, "observed_at")
    assert not hasattr(snapshot, "trade_conditions")
    assert not hasattr(snapshot.rows[0], "observed_at")
    assert not hasattr(snapshot.rows[0], "suspended")
    assert not hasattr(snapshot.rows[0], "buy_limit_locked")
    with pytest.raises(ValidationError, match="frozen"):
        snapshot.rows[0].open = 9.0


def test_digest_ignores_request_order_db_row_order_and_path(tmp_path: Path) -> None:
    first = load_retrospective_daily_prices(_frozen_db(tmp_path / "first.duckdb"), REQUEST)
    second = load_retrospective_daily_prices(
        _frozen_db(tmp_path / "second.duckdb", tuple(reversed(ROWS))), tuple(reversed(REQUEST))
    )
    assert first == second
    altered = list(ROWS)
    altered[0] = (CODE_1, DAY_1, 10.1, 10.5, 9.8)
    changed = load_retrospective_daily_prices(
        _frozen_db(tmp_path / "changed.duckdb", tuple(altered)), REQUEST
    )
    assert changed.source_identity != first.source_identity
    tampered = first.model_dump(mode="python")
    tampered["rows"][0]["close"] = 8.3
    with pytest.raises(ValidationError, match="source identity"):
        type(first).model_validate(tampered)


def test_file_wrapper_opens_one_readonly_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _frozen_db(tmp_path / "read-only.duckdb")
    original_connect = duckdb.connect
    modes: list[bool | None] = []

    def audited_connect(*args: object, **kwargs: object) -> duckdb.DuckDBPyConnection:
        modes.append(kwargs.get("read_only"))
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", audited_connect)
    snapshot = load_retrospective_daily_prices(path, REQUEST)
    assert len(snapshot.rows) == 3
    assert modes == [True]


def test_core_uses_callers_readonly_transaction_without_managing_it(tmp_path: Path) -> None:
    path = _frozen_db(tmp_path / "transaction.duckdb")
    connection = duckdb.connect(str(path), read_only=True)

    class AuditedConnection:
        def __init__(self, wrapped: duckdb.DuckDBPyConnection) -> None:
            self.wrapped = wrapped
            self.statements: list[str] = []

        def execute(self, query: str, parameters: list[object] | None = None) -> AuditedConnection:
            self.statements.append(query)
            self.wrapped.execute(query, parameters)
            return self

        def fetchall(self) -> list[tuple[object, ...]]:
            return self.wrapped.fetchall()

    try:
        connection.execute("BEGIN TRANSACTION")
        audited = AuditedConnection(connection)
        snapshot = verify_retrospective_daily_prices(audited, REQUEST)  # type: ignore[arg-type]
        assert len(snapshot.rows) == 3
        assert all(sql.startswith(("PRAGMA", "SELECT")) for sql in audited.statements)
        assert connection.execute("SELECT count(*) FROM daily_bar").fetchone() == (3,)
        connection.execute("COMMIT")
    finally:
        connection.close()


def test_queries_are_exact_pair_batches_with_a_returned_row_cap(tmp_path: Path) -> None:
    pairs = tuple((DAY_1, f"{i:06d}.SH") for i in range(MAX_SQL_BATCH + 1))
    rows = tuple((code, day, 10.0, 10.1, 9.9) for day, code in pairs)
    path = _frozen_db(tmp_path / "batched.duckdb", rows)
    connection = duckdb.connect(str(path), read_only=True)
    statements: list[tuple[str, list[object] | None]] = []

    class AuditedConnection:
        def execute(self, query: str, parameters: list[object] | None = None) -> AuditedConnection:
            statements.append((query, parameters))
            connection.execute(query, parameters)
            return self

        def fetchall(self) -> list[tuple[object, ...]]:
            return connection.fetchall()

    try:
        connection.execute("BEGIN TRANSACTION")
        snapshot = verify_retrospective_daily_prices(AuditedConnection(), pairs)  # type: ignore[arg-type]
        connection.execute("COMMIT")
    finally:
        connection.close()

    queries = [(sql, params) for sql, params in statements if sql.startswith("SELECT")]
    assert len(snapshot.rows) == len(pairs)
    assert len(queries) == 2
    assert all(
        "WHERE trade_date BETWEEN ? AND ? AND (trade_date, ts_code) IN" in sql and "LIMIT ?" in sql
        for sql, _ in queries
    )
    assert all(params is not None and len(params) <= MAX_SQL_BATCH * 2 + 3 for _, params in queries)


@pytest.mark.parametrize("missing", [REQUEST[0], REQUEST[1], REQUEST[2]])
def test_missing_requested_pair_fails_without_filling_previous_prices(
    tmp_path: Path, missing: tuple[date, str]
) -> None:
    rows = tuple(row for row in ROWS if (row[1], row[0]) != missing)
    with pytest.raises(DailyPriceSourceError, match="missing"):
        load_retrospective_daily_prices(_frozen_db(tmp_path / "missing.duckdb", rows), REQUEST)


def test_duplicate_pair_fails_even_without_a_primary_key(tmp_path: Path) -> None:
    path = _frozen_db(
        tmp_path / "duplicate.duckdb",
        (*ROWS, ROWS[0]),
        ddl="CREATE TABLE daily_bar (ts_code VARCHAR NOT NULL, trade_date DATE NOT NULL, "
        "open DOUBLE, close DOUBLE, pre_close DOUBLE)",
    )
    with pytest.raises(DailyPriceSourceError, match="duplicate"):
        load_retrospective_daily_prices(path, REQUEST)


@pytest.mark.parametrize("column", ["open", "close", "pre_close"])
@pytest.mark.parametrize("bad", [None, 0.0, -1.0, float("nan"), float("inf")])
def test_missing_or_invalid_price_is_rejected(
    tmp_path: Path, column: str, bad: float | None
) -> None:
    position = {"open": 2, "close": 3, "pre_close": 4}[column]
    rows = [list(row) for row in ROWS]
    rows[0][position] = bad
    path = _frozen_db(tmp_path / "bad.duckdb", tuple(tuple(row) for row in rows))
    with pytest.raises(DailyPriceSourceError, match=column):
        load_retrospective_daily_prices(path, REQUEST)


def test_old_or_missing_schema_fails_closed(tmp_path: Path) -> None:
    path = _frozen_db(
        tmp_path / "old.duckdb",
        (),
        ddl="CREATE TABLE daily_bar (ts_code VARCHAR, trade_date DATE, "
        "open DOUBLE, close DOUBLE, pre_close VARCHAR)",
    )
    with pytest.raises(DailyPriceSourceError, match="schema"):
        load_retrospective_daily_prices(path, REQUEST)
    blank = tmp_path / "blank.duckdb"
    duckdb.connect(str(blank)).close()
    with pytest.raises(DailyPriceSourceError, match="schema"):
        load_retrospective_daily_prices(blank, REQUEST)


def test_invalid_or_unbounded_requests_are_rejected_before_reading(tmp_path: Path) -> None:
    path = _frozen_db(tmp_path / "limits.duckdb")
    with pytest.raises(DailyPriceSourceError, match="duplicate request"):
        load_retrospective_daily_prices(path, (REQUEST[0], REQUEST[0]))
    with pytest.raises(DailyPriceSourceError, match="code"):
        load_retrospective_daily_prices(path, ((DAY_1, "bad-code"),))
    with pytest.raises(DailyPriceSourceError, match="civil dates"):
        load_retrospective_daily_prices(
            path, ((DAY_1, CODE_1), (DAY_1 + timedelta(days=3660), CODE_1))
        )
    too_many_codes = tuple((DAY_1, f"{i:06d}.SH") for i in range(MAX_REQUEST_CODES + 1))
    with pytest.raises(DailyPriceSourceError, match="codes"):
        load_retrospective_daily_prices(path, too_many_codes)
