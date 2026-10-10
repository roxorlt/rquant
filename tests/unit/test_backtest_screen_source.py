"""A frozen screen receipt is the only accepted source of pre-open candidates."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from rquant.backtest.screen_source import (
    MAX_SCREEN_CANDIDATES,
    ScreenCandidateSourceError,
    VerifiedScreenCandidateSnapshot,
    load_verified_screen_candidates,
)
from rquant.pool_result_receipt import ScreenRunReceipt, member_price_digest, member_set_digest
from rquant.storage.schema import (
    SCREEN_RESULT_DDL,
    SCREEN_RUN_PRICE_RECEIPT_MIGRATION_DDLS,
    SCREEN_RUN_RECEIPT_DDL,
)

RESULT_DAY = date(2026, 8, 7)
DECISION_DAY = date(2026, 8, 10)
COMPLETED = datetime(2026, 8, 7, 8, 12, tzinfo=UTC)
PRICES = [("600000.SH", 10.5), ("000001.SZ", 8.25)]


def _receipt(
    prices: list[tuple[str, float | None]],
    **changes: object,
) -> ScreenRunReceipt:
    fields: dict[str, object] = {
        "contract": "screen-run-receipt/v2",
        "trade_date": RESULT_DAY,
        "preset_name": "pool",
        "definition_version": "a" * 64,
        "hit_count": len(prices),
        "member_digest": member_set_digest([code for code, _ in prices]),
        "price_digest": member_price_digest(prices),
        "lineage_complete": True,
        "completed_at": COMPLETED,
    }
    fields.update(changes)
    return ScreenRunReceipt.model_validate(fields)


def _frozen_file(
    path: Path,
    prices: list[tuple[str, float | None]] | None = None,
    receipt: ScreenRunReceipt | None = None,
) -> Path:
    prices = PRICES if prices is None else prices
    receipt = _receipt(prices) if receipt is None else receipt
    connection = duckdb.connect(str(path))
    try:
        connection.execute(SCREEN_RESULT_DDL)
        connection.execute(SCREEN_RUN_RECEIPT_DDL)
        for ddl in SCREEN_RUN_PRICE_RECEIPT_MIGRATION_DDLS:
            connection.execute(ddl)
        for code, close in prices:
            connection.execute(
                "INSERT INTO screen_result (trade_date, preset_name, ts_code, close) "
                "VALUES (?, ?, ?, ?)",
                [RESULT_DAY, "pool", code, close],
            )
        _insert_receipt(connection, receipt)
    finally:
        connection.close()
    return path


def _insert_receipt(connection: duckdb.DuckDBPyConnection, receipt: ScreenRunReceipt) -> None:
    connection.execute(
        """
        INSERT INTO screen_run_receipt (
            trade_date, preset_name, definition_version, parent_trade_date,
            parent_result_version, hit_count, member_digest, lineage_complete,
            completed_at, result_version, contract, price_digest
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            receipt.trade_date,
            receipt.preset_name,
            receipt.definition_version,
            receipt.parent_trade_date,
            receipt.parent_result_version,
            receipt.hit_count,
            receipt.member_digest,
            receipt.lineage_complete,
            receipt.completed_at,
            receipt.result_version,
            receipt.contract,
            receipt.price_digest,
        ],
    )


def _load(path: Path) -> VerifiedScreenCandidateSnapshot:
    return load_verified_screen_candidates(path, RESULT_DAY, DECISION_DAY, "pool")


def test_v2_receipt_returns_exact_prices_and_actual_completion_without_ranking(
    tmp_path: Path,
) -> None:
    path = _frozen_file(tmp_path / "frozen.duckdb")

    snapshot = _load(path)

    assert snapshot.source_mode == "captured_receipt"
    assert snapshot.source_trade_date == RESULT_DAY
    assert snapshot.decision_trade_date == DECISION_DAY
    assert snapshot.preset_name == "pool"
    assert snapshot.completed_at == COMPLETED
    assert snapshot.result_version == snapshot.receipt.result_version
    assert [(item.ts_code, item.previous_close) for item in snapshot.candidates] == [
        ("000001.SZ", 8.25),
        ("600000.SH", 10.5),
    ]
    assert not hasattr(snapshot, "observed_at")
    assert not hasattr(snapshot.candidates[0], "rank_score")
    assert not hasattr(snapshot.candidates[0], "open_price")
    with pytest.raises(ValidationError, match="frozen"):
        snapshot.candidates[0].previous_close = 20.0


def test_zero_hit_v2_receipt_is_a_verified_empty_candidate_set(tmp_path: Path) -> None:
    snapshot = _load(_frozen_file(tmp_path / "empty.duckdb", []))
    assert snapshot.candidates == ()
    assert snapshot.receipt.hit_count == 0


@pytest.mark.parametrize(
    "completed_at",
    [
        datetime(2026, 8, 7, 7, 0, tzinfo=UTC),
        datetime(2026, 8, 10, 1, 24, 59, 999999, tzinfo=UTC),
    ],
)
def test_exact_close_and_last_pre_cutoff_instant_are_accepted(
    tmp_path: Path, completed_at: datetime
) -> None:
    receipt = _receipt(PRICES, completed_at=completed_at)
    snapshot = _load(_frozen_file(tmp_path / "valid-boundary.duckdb", receipt=receipt))
    assert snapshot.completed_at == completed_at


@pytest.mark.parametrize(
    ("sql", "parameters", "reason"),
    [
        ("UPDATE screen_result SET close = 10.6 WHERE ts_code = '600000.SH'", [], "price digest"),
        (
            "UPDATE screen_result SET ts_code = '600002.SH' WHERE ts_code = '600000.SH'",
            [],
            "member digest",
        ),
        ("DELETE FROM screen_result WHERE ts_code = '600000.SH'", [], "hit count"),
        (
            "INSERT INTO screen_result (trade_date, preset_name, ts_code, close) "
            "VALUES (?, ?, '600002.SH', 11.0)",
            [RESULT_DAY, "pool"],
            "hit count",
        ),
        ("UPDATE screen_result SET close = NULL WHERE ts_code = '600000.SH'", [], "close"),
        ("UPDATE screen_result SET close = 'inf' WHERE ts_code = '600000.SH'", [], "close"),
    ],
)
def test_changed_or_invalid_result_rows_fail_closed(
    tmp_path: Path, sql: str, parameters: list[object], reason: str
) -> None:
    path = _frozen_file(tmp_path / "changed.duckdb")
    connection = duckdb.connect(str(path))
    try:
        connection.execute(sql, parameters)
    finally:
        connection.close()

    with pytest.raises(ScreenCandidateSourceError, match=reason):
        _load(path)


@pytest.mark.parametrize(
    ("receipt", "reason"),
    [
        (
            _receipt(PRICES, contract="screen-run-receipt/v1", price_digest=None),
            "v2",
        ),
        (_receipt(PRICES, lineage_complete=False), "lineage"),
        (
            _receipt(PRICES, completed_at=datetime(2026, 8, 7, 6, 59, tzinfo=UTC)),
            "close",
        ),
        (
            _receipt(PRICES, completed_at=datetime(2026, 8, 10, 1, 25, tzinfo=UTC)),
            "09:25",
        ),
    ],
)
def test_receipt_contract_and_shanghai_time_window_are_enforced(
    tmp_path: Path, receipt: ScreenRunReceipt, reason: str
) -> None:
    path = _frozen_file(tmp_path / "boundary.duckdb", receipt=receipt)
    with pytest.raises(ScreenCandidateSourceError, match=reason):
        _load(path)


def test_receipt_version_tampering_is_rejected(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "tampered-receipt.duckdb")
    connection = duckdb.connect(str(path))
    try:
        connection.execute("UPDATE screen_run_receipt SET result_version = ?", ["f" * 64])
    finally:
        connection.close()
    with pytest.raises(ScreenCandidateSourceError, match="receipt"):
        _load(path)


def test_missing_receipt_is_rejected(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "missing-proof.duckdb")
    connection = duckdb.connect(str(path))
    try:
        connection.execute("DELETE FROM screen_run_receipt")
    finally:
        connection.close()
    with pytest.raises(ScreenCandidateSourceError, match="receipt"):
        _load(path)


def test_missing_v2_price_column_is_rejected(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "missing-column.duckdb")
    connection = duckdb.connect(str(path))
    try:
        connection.execute("ALTER TABLE screen_run_receipt DROP COLUMN price_digest")
    finally:
        connection.close()
    with pytest.raises(ScreenCandidateSourceError, match="price proof"):
        _load(path)


def test_v2_receipt_without_price_digest_is_rejected(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "missing-digest.duckdb")
    connection = duckdb.connect(str(path))
    try:
        connection.execute("UPDATE screen_run_receipt SET price_digest = NULL")
    finally:
        connection.close()
    with pytest.raises(ScreenCandidateSourceError, match="receipt"):
        _load(path)


def test_multiple_receipt_rows_are_rejected(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "multiple-receipts.duckdb")
    connection = duckdb.connect(str(path))
    try:
        connection.execute("CREATE TABLE unkeyed_receipt AS SELECT * FROM screen_run_receipt")
        connection.execute("DROP TABLE screen_run_receipt")
        connection.execute("ALTER TABLE unkeyed_receipt RENAME TO screen_run_receipt")
        _insert_receipt(connection, _receipt(PRICES))
    finally:
        connection.close()
    with pytest.raises(ScreenCandidateSourceError, match="exactly one"):
        _load(path)


def test_duplicate_code_is_rejected_even_in_a_malformed_snapshot(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "duplicate.duckdb")
    connection = duckdb.connect(str(path))
    try:
        connection.execute("CREATE TABLE unkeyed_result AS SELECT * FROM screen_result")
        connection.execute("DROP TABLE screen_result")
        connection.execute("ALTER TABLE unkeyed_result RENAME TO screen_result")
        connection.execute(
            "UPDATE screen_result SET ts_code = '600000.SH' WHERE ts_code = '000001.SZ'"
        )
    finally:
        connection.close()
    with pytest.raises(ScreenCandidateSourceError, match="duplicate"):
        _load(path)


def test_large_claim_and_absent_file_fail_before_creating_or_scanning_data(tmp_path: Path) -> None:
    absent = tmp_path / "missing.duckdb"
    with pytest.raises(ScreenCandidateSourceError, match="existing"):
        _load(absent)
    assert not absent.exists()

    path = _frozen_file(
        tmp_path / "too-large.duckdb",
        [],
        _receipt([], hit_count=MAX_SCREEN_CANDIDATES + 1),
    )
    with pytest.raises(ScreenCandidateSourceError, match="limit"):
        _load(path)


def test_receipt_and_result_are_read_from_one_read_only_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _frozen_file(tmp_path / "transaction.duckdb")
    real_connect = duckdb.connect
    calls: list[tuple[str, object]] = []

    class TracedConnection:
        def __init__(self, inner: duckdb.DuckDBPyConnection) -> None:
            self.inner = inner

        def execute(self, sql: str, parameters: list[object] | None = None) -> TracedConnection:
            calls.append(("sql", sql.strip().split()[0].upper()))
            if parameters is None:
                self.inner.execute(sql)
            else:
                self.inner.execute(sql, parameters)
            return self

        def fetchall(self) -> list[tuple[object, ...]]:
            return self.inner.fetchall()

        def close(self) -> None:
            calls.append(("close", ""))
            self.inner.close()

    def traced_connect(database: str, *, read_only: bool = False) -> TracedConnection:
        calls.append(("connect", (database, read_only)))
        return TracedConnection(real_connect(database, read_only=read_only))

    monkeypatch.setattr("rquant.backtest.screen_source.duckdb.connect", traced_connect)

    snapshot = _load(path)

    assert snapshot.candidates
    assert calls[0] == ("connect", (str(path), True))
    assert calls[1] == ("sql", "BEGIN")
    assert calls[-2:] == [("sql", "COMMIT"), ("close", "")]
    assert len([call for call in calls if call[0] == "connect"]) == 1
    assert sum(call == ("sql", "SELECT") for call in calls) >= 2
