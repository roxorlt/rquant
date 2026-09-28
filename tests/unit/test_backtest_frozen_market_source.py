"""A screen receipt, SSE days, and retrospective prices share one frozen read."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from rquant.backtest.daily_price_source import MAX_REQUEST_CODES, DailyPriceSourceError
from rquant.backtest.frozen_market_source import (
    FrozenMarketSourceError,
    VerifiedFrozenMarketSnapshot,
    load_frozen_market_source,
    verify_frozen_market_source,
)
from rquant.pool_result_receipt import ScreenRunReceipt, member_price_digest, member_set_digest
from rquant.storage.schema import (
    DAILY_BAR_DDL,
    SCREEN_RESULT_DDL,
    SCREEN_RUN_PRICE_RECEIPT_MIGRATION_DDLS,
    SCREEN_RUN_RECEIPT_DDL,
    TRADE_CALENDAR_DDL,
)

SOURCE = date(2026, 8, 7)
DECISION = date(2026, 8, 10)
PRICES = (("000001.SZ", 8.25), ("600000.SH", 10.5))
COMPLETED = datetime(2026, 8, 7, 8, 12, tzinfo=UTC)


def _frozen_file(
    path: Path,
    *,
    screen_prices: tuple[tuple[str, float], ...] = PRICES,
    daily_prices: tuple[tuple[str, float], ...] = PRICES,
) -> Path:
    receipt = ScreenRunReceipt(
        contract="screen-run-receipt/v2",
        trade_date=SOURCE,
        preset_name="pool",
        definition_version="a" * 64,
        hit_count=len(screen_prices),
        member_digest=member_set_digest([code for code, _ in screen_prices]),
        price_digest=member_price_digest(screen_prices),
        lineage_complete=True,
        completed_at=COMPLETED,
    )
    connection = duckdb.connect(str(path))
    try:
        for ddl in (DAILY_BAR_DDL, SCREEN_RESULT_DDL, SCREEN_RUN_RECEIPT_DDL, TRADE_CALENDAR_DDL):
            connection.execute(ddl)
        for ddl in SCREEN_RUN_PRICE_RECEIPT_MIGRATION_DDLS:
            connection.execute(ddl)
        if screen_prices:
            connection.executemany(
                "INSERT INTO screen_result (trade_date, preset_name, ts_code, close) "
                "VALUES (?, 'pool', ?, ?)",
                [(SOURCE, code, close) for code, close in screen_prices],
            )
        connection.execute(
            "INSERT INTO screen_run_receipt ("
            "trade_date, preset_name, definition_version, parent_trade_date, "
            "parent_result_version, hit_count, member_digest, lineage_complete, "
            "completed_at, result_version, contract, price_digest) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        connection.executemany(
            "INSERT INTO trade_calendar "
            "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
            "VALUES ('SSE', ?, ?, ?, 'tushare', ?)",
            [
                (SOURCE, True, date(2026, 8, 6), COMPLETED),
                (date(2026, 8, 8), False, SOURCE, COMPLETED),
                (date(2026, 8, 9), False, SOURCE, COMPLETED),
                (DECISION, True, SOURCE, COMPLETED),
                (date(2026, 8, 11), True, DECISION, COMPLETED),
            ],
        )
        if daily_prices:
            connection.executemany(
                "INSERT INTO daily_bar (ts_code, trade_date, open, close, pre_close) "
                "VALUES (?, ?, 10, ?, 9)",
                [(code, SOURCE, close) for code, close in daily_prices],
            )
    finally:
        connection.close()
    return path


def _load(path: Path) -> VerifiedFrozenMarketSnapshot:
    return load_frozen_market_source(
        path,
        source_trade_date=SOURCE,
        decision_trade_date=DECISION,
        preset_name="pool",
    )


def test_market_facts_bind_receipt_prices_to_same_day_daily_close(tmp_path: Path) -> None:
    first = _load(_frozen_file(tmp_path / "first.duckdb"))
    second = _load(_frozen_file(tmp_path / "second.duckdb", daily_prices=tuple(reversed(PRICES))))

    assert first.source_mode == "screen_receipt_with_retrospective_market"
    assert first.candidates.screen.receipt.completed_at == COMPLETED
    assert first.candidates.screen.source_mode == "captured_receipt"
    assert first.candidates.calendar.source_mode == "retrospective_trade_calendar"
    assert first.daily_prices is not None
    assert first.daily_prices.source_mode == "retrospective_daily_bar"
    assert [(pair.trade_date, pair.ts_code) for pair in first.daily_prices.requested_pairs] == [
        (SOURCE, "000001.SZ"),
        (SOURCE, "600000.SH"),
    ]
    assert [(row.trade_date, row.ts_code, row.close) for row in first.daily_prices.rows] == [
        (SOURCE, "000001.SZ", 8.25),
        (SOURCE, "600000.SH", 10.5),
    ]
    assert first.source_identity == second.source_identity
    assert len(first.source_identity) == 64
    assert not hasattr(first, "ranking")
    assert not hasattr(first, "open_fill")
    assert not hasattr(first.daily_prices.rows[0], "observed_at")


def test_daily_close_disagreement_fails_without_rounding(tmp_path: Path) -> None:
    path = _frozen_file(
        tmp_path / "different.duckdb",
        daily_prices=(("000001.SZ", 8.25), ("600000.SH", 10.5000001)),
    )
    with pytest.raises(FrozenMarketSourceError, match="daily close differs"):
        _load(path)


def test_missing_daily_pair_fails_closed(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "missing.duckdb", daily_prices=(PRICES[0],))
    with pytest.raises(DailyPriceSourceError, match="missing daily_bar pair"):
        _load(path)


def test_empty_verified_screen_has_no_price_claim(tmp_path: Path) -> None:
    snapshot = _load(_frozen_file(tmp_path / "empty.duckdb", screen_prices=(), daily_prices=()))
    assert snapshot.candidates.screen.candidates == ()
    assert snapshot.daily_prices is None
    assert len(snapshot.source_identity) == 64


def test_candidate_price_pair_limit_is_explicit(tmp_path: Path) -> None:
    candidates = tuple((f"{code:06d}.SH", 10.0) for code in range(MAX_REQUEST_CODES + 1))
    path = _frozen_file(tmp_path / "too-many.duckdb", screen_prices=candidates, daily_prices=())
    with pytest.raises(FrozenMarketSourceError, match=f"exceeds {MAX_REQUEST_CODES}"):
        _load(path)


class _AuditedConnection:
    def __init__(self, inner: duckdb.DuckDBPyConnection, statements: list[str]) -> None:
        self.inner = inner
        self.statements = statements
        self.closed = False

    def execute(self, sql: str, parameters: list[object] | None = None) -> _AuditedConnection:
        self.statements.append(sql)
        self.inner.execute(sql, parameters)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self.inner.fetchone()

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.inner.fetchall()

    def close(self) -> None:
        self.closed = True
        self.inner.close()


def test_caller_owned_core_uses_one_existing_transaction(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "caller.duckdb")
    statements: list[str] = []
    connection = _AuditedConnection(duckdb.connect(str(path), read_only=True), statements)
    try:
        connection.execute("BEGIN TRANSACTION")
        statements.clear()
        snapshot = verify_frozen_market_source(
            connection,  # type: ignore[arg-type]
            source_trade_date=SOURCE,
            decision_trade_date=DECISION,
            preset_name="pool",
        )
        assert snapshot.daily_prices is not None
        assert not any(sql in {"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"} for sql in statements)
        assert any("FROM trade_calendar" in sql for sql in statements)
        assert any("FROM screen_run_receipt" in sql for sql in statements)
        assert any("FROM daily_bar" in sql for sql in statements)
        assert connection.execute("SELECT COUNT(*) FROM daily_bar").fetchone() == (2,)
        connection.execute("COMMIT")
    finally:
        connection.close()


def test_path_wrapper_opens_one_readonly_connection_and_closes_after_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _frozen_file(tmp_path / "wrapper.duckdb")
    real_connect = duckdb.connect
    opens: list[tuple[str, bool]] = []
    statements: list[str] = []
    connections: list[_AuditedConnection] = []

    def audited_connect(database: str, *, read_only: bool = False) -> _AuditedConnection:
        opens.append((database, read_only))
        wrapped = _AuditedConnection(real_connect(database, read_only=read_only), statements)
        connections.append(wrapped)
        return wrapped

    monkeypatch.setattr(duckdb, "connect", audited_connect)
    assert _load(path).daily_prices is not None
    assert opens == [(str(path), True)]
    assert [sql for sql in statements if sql in {"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"}] == [
        "BEGIN TRANSACTION",
        "COMMIT",
    ]
    assert connections[0].closed


def test_path_wrapper_rolls_back_and_closes_on_binding_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _frozen_file(
        tmp_path / "rollback.duckdb",
        daily_prices=(("000001.SZ", 8.25), ("600000.SH", 10.5000001)),
    )
    real_connect = duckdb.connect
    statements: list[str] = []
    connections: list[_AuditedConnection] = []

    def audited_connect(database: str, *, read_only: bool = False) -> _AuditedConnection:
        assert database == str(path) and read_only
        wrapped = _AuditedConnection(real_connect(database, read_only=read_only), statements)
        connections.append(wrapped)
        return wrapped

    monkeypatch.setattr(duckdb, "connect", audited_connect)
    with pytest.raises(FrozenMarketSourceError, match="daily close differs"):
        _load(path)
    assert [sql for sql in statements if sql in {"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"}] == [
        "BEGIN TRANSACTION",
        "ROLLBACK",
    ]
    assert connections[0].closed
