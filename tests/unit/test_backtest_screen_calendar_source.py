"""Screen candidates and the SSE calendar must share one frozen evidence snapshot."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from rquant.backtest.calendar_source import CalendarSourceError
from rquant.backtest.equal_candidates import (
    EqualCandidateRankingError,
    build_equal_weight_ranking,
)
from rquant.backtest.screen_calendar_source import (
    ScreenCalendarSourceError,
    VerifiedScreenCalendarCandidates,
    load_screen_calendar_candidates,
    verify_screen_calendar_candidates,
)
from rquant.backtest.screen_source import (
    ScreenCandidateSourceError,
    VerifiedScreenCandidate,
    verify_screen_candidates,
)
from rquant.pool_result_receipt import ScreenRunReceipt, member_price_digest, member_set_digest
from rquant.portfolio.weights import PortfolioWeightRule
from rquant.storage.schema import (
    SCREEN_RESULT_DDL,
    SCREEN_RUN_PRICE_RECEIPT_MIGRATION_DDLS,
    SCREEN_RUN_RECEIPT_DDL,
    TRADE_CALENDAR_DDL,
)

SOURCE = date(2026, 8, 7)
DECISION = date(2026, 8, 10)
UPDATED = datetime(2026, 8, 12, 9, tzinfo=UTC)
PRICES = [("600000.SH", 10.5), ("000001.SZ", 8.25)]


def _calendar_rows() -> list[tuple[str, date, bool, date, str, datetime]]:
    return [
        ("SSE", date(2026, 8, 7), True, date(2026, 8, 6), "tushare", UPDATED),
        ("SSE", date(2026, 8, 8), False, SOURCE, "tushare", UPDATED),
        ("SSE", date(2026, 8, 9), False, SOURCE, "tushare", UPDATED),
        ("SSE", DECISION, True, SOURCE, "tushare", UPDATED),
        ("SSE", date(2026, 8, 11), True, DECISION, "tushare", UPDATED),
        ("SSE", date(2026, 8, 12), True, date(2026, 8, 11), "tushare", UPDATED),
    ]


def _frozen_file(path: Path, *, completed_at: datetime | None = None) -> Path:
    receipt = ScreenRunReceipt(
        contract="screen-run-receipt/v2",
        trade_date=SOURCE,
        preset_name="pool",
        definition_version="a" * 64,
        hit_count=len(PRICES),
        member_digest=member_set_digest([code for code, _ in PRICES]),
        price_digest=member_price_digest(PRICES),
        lineage_complete=True,
        completed_at=completed_at or datetime(2026, 8, 7, 8, 12, tzinfo=UTC),
    )
    connection = duckdb.connect(str(path))
    try:
        connection.execute(SCREEN_RESULT_DDL)
        connection.execute(SCREEN_RUN_RECEIPT_DDL)
        for ddl in SCREEN_RUN_PRICE_RECEIPT_MIGRATION_DDLS:
            connection.execute(ddl)
        connection.execute(TRADE_CALENDAR_DDL)
        connection.executemany(
            "INSERT INTO screen_result (trade_date, preset_name, ts_code, close) "
            "VALUES (?, 'pool', ?, ?)",
            [(SOURCE, code, close) for code, close in PRICES],
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
            "VALUES (?, ?, ?, ?, ?, ?)",
            _calendar_rows(),
        )
    finally:
        connection.close()
    return path


def _load(path: Path, *, source: date = SOURCE, decision: date = DECISION):
    return load_screen_calendar_candidates(
        path,
        source_trade_date=source,
        decision_trade_date=decision,
        preset_name="pool",
    )


def test_frozen_file_binds_exact_receipt_prices_to_previous_sse_open_day(tmp_path: Path) -> None:
    first = _load(_frozen_file(tmp_path / "first.duckdb"))
    second = _load(_frozen_file(tmp_path / "second.duckdb"))

    assert first.screen.source_trade_date == SOURCE
    assert first.screen.decision_trade_date == DECISION
    assert first.calendar.calendar.dates == (SOURCE, DECISION, date(2026, 8, 11))
    assert [(item.ts_code, item.previous_close) for item in first.screen.candidates] == [
        ("000001.SZ", 8.25),
        ("600000.SH", 10.5),
    ]
    assert first.screen.result_version == first.screen.receipt.result_version
    assert first.source_identity == second.source_identity
    assert len(first.source_identity) == 64
    assert not hasattr(first, "ranking")
    assert not hasattr(first.screen.candidates[0], "open_price")


def test_nonadjacent_source_day_is_rejected_before_screen_lookup(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "nonadjacent.duckdb")
    with pytest.raises(ScreenCalendarSourceError, match="previous SSE open day"):
        _load(path, source=date(2026, 8, 6))
    with pytest.raises(ScreenCalendarSourceError, match="previous SSE open day"):
        _load(path, decision=date(2026, 8, 11))


def test_missing_or_duplicate_calendar_day_is_rejected(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "broken-calendar.duckdb")
    connection = duckdb.connect(str(path))
    try:
        connection.execute("DELETE FROM trade_calendar WHERE cal_date = '2026-08-08'")
    finally:
        connection.close()
    with pytest.raises(CalendarSourceError, match="missing"):
        _load(path)

    path = _frozen_file(tmp_path / "duplicate-calendar.duckdb")
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            "CREATE TABLE unkeyed_calendar ("
            "exchange VARCHAR NOT NULL, cal_date DATE NOT NULL, is_open BOOLEAN NOT NULL, "
            "pretrade_date DATE, source VARCHAR NOT NULL, updated_at TIMESTAMPTZ NOT NULL)"
        )
        connection.execute("INSERT INTO unkeyed_calendar SELECT * FROM trade_calendar")
        connection.execute("DROP TABLE trade_calendar")
        connection.execute("ALTER TABLE unkeyed_calendar RENAME TO trade_calendar")
        connection.execute(
            "INSERT INTO trade_calendar SELECT * FROM trade_calendar WHERE cal_date = '2026-08-08'"
        )
    finally:
        connection.close()
    with pytest.raises(CalendarSourceError, match="duplicate"):
        _load(path)


def test_late_receipt_and_price_tamper_are_rejected(tmp_path: Path) -> None:
    late = _frozen_file(
        tmp_path / "late.duckdb", completed_at=datetime(2026, 8, 10, 1, 25, tzinfo=UTC)
    )
    with pytest.raises(ScreenCandidateSourceError, match="09:25"):
        _load(late)

    tampered = _frozen_file(tmp_path / "tampered.duckdb")
    connection = duckdb.connect(str(tampered))
    try:
        connection.execute("UPDATE screen_result SET close = 10.6 WHERE ts_code = '600000.SH'")
    finally:
        connection.close()
    with pytest.raises(ScreenCandidateSourceError, match="price digest"):
        _load(tampered)


class _AuditedConnection:
    def __init__(self, inner: duckdb.DuckDBPyConnection, statements: list[str]) -> None:
        self.inner = inner
        self.statements = statements

    def execute(self, sql: str, parameters: list[object] | None = None) -> _AuditedConnection:
        self.statements.append(sql)
        self.inner.execute(sql, parameters)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self.inner.fetchone()

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.inner.fetchall()

    def close(self) -> None:
        self.inner.close()


def test_caller_owned_core_uses_existing_transaction_without_finishing_it(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "caller-owned.duckdb")
    statements: list[str] = []
    connection = _AuditedConnection(duckdb.connect(str(path), read_only=True), statements)
    try:
        connection.execute("BEGIN TRANSACTION")
        statements.clear()
        evidence = verify_screen_calendar_candidates(
            connection,
            source_trade_date=SOURCE,
            decision_trade_date=DECISION,
            preset_name="pool",
        )
        assert not any(sql in {"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"} for sql in statements)
        assert any("FROM trade_calendar" in sql for sql in statements)
        assert any("FROM screen_run_receipt" in sql for sql in statements)
        assert any("FROM screen_result" in sql for sql in statements)
        assert connection.execute("SELECT COUNT(*) FROM screen_result").fetchone() == (2,)
        connection.execute("COMMIT")
    finally:
        connection.close()
    assert evidence.calendar.source_identity


def test_path_wrapper_opens_one_read_only_connection_and_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _frozen_file(tmp_path / "one-transaction.duckdb")
    real_connect = duckdb.connect
    opens: list[tuple[str, bool]] = []
    statements: list[str] = []

    def traced_connect(database: str, *, read_only: bool = False) -> _AuditedConnection:
        opens.append((database, read_only))
        return _AuditedConnection(real_connect(database, read_only=read_only), statements)

    monkeypatch.setattr(duckdb, "connect", traced_connect)
    evidence = _load(path)

    assert evidence.screen.candidates
    assert opens == [(str(path), True)]
    assert [sql for sql in statements if sql in {"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"}] == [
        "BEGIN TRANSACTION",
        "COMMIT",
    ]


def test_screen_core_can_be_reused_without_transaction_control(tmp_path: Path) -> None:
    path = _frozen_file(tmp_path / "screen-core.duckdb")
    statements: list[str] = []
    connection = _AuditedConnection(duckdb.connect(str(path), read_only=True), statements)
    try:
        connection.execute("BEGIN TRANSACTION")
        statements.clear()
        screen = verify_screen_candidates(connection, SOURCE, DECISION, "pool")
        assert screen.candidates
        assert not any(sql in {"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"} for sql in statements)
        connection.execute("ROLLBACK")
    finally:
        connection.close()


def test_invalid_input_fails_before_opening_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.duckdb"
    with pytest.raises(ScreenCalendarSourceError, match="preset"):
        load_screen_calendar_candidates(
            missing,
            source_trade_date=SOURCE,
            decision_trade_date=DECISION,
            preset_name="p" * 257,
        )
    with pytest.raises(ScreenCalendarSourceError, match="date"):
        load_screen_calendar_candidates(
            missing,
            source_trade_date=SOURCE,
            decision_trade_date="2026-08-10",  # type: ignore[arg-type]
            preset_name="pool",
        )
    assert not missing.exists()


def test_verified_receipt_becomes_equal_all_ranking_without_invented_scores(
    tmp_path: Path,
) -> None:
    evidence = _load(_frozen_file(tmp_path / "equal-candidates.duckdb"))

    ranking = build_equal_weight_ranking(
        evidence, PortfolioWeightRule(method="equal", max_positions=2)
    )

    assert ranking.source_trade_date == SOURCE
    assert ranking.observed_at == evidence.screen.receipt.completed_at
    assert [candidate.ts_code for candidate in ranking.candidates] == [
        "000001.SZ",
        "600000.SH",
    ]
    assert all(candidate.rank_score == 0 for candidate in ranking.candidates)
    assert all(candidate.industry_l1 is None for candidate in ranking.candidates)
    assert len(ranking.source_identity) == 64
    assert (
        ranking.source_identity
        == build_equal_weight_ranking(
            evidence, PortfolioWeightRule(method="equal", max_positions=2)
        ).source_identity
    )
    assert evidence.screen.candidates[0].previous_close == 8.25


@pytest.mark.parametrize(
    ("rule", "message"),
    [
        (PortfolioWeightRule(method="rank_score", max_positions=2), "equal"),
        (PortfolioWeightRule(method="equal", max_positions=1), "all candidates"),
    ],
)
def test_equal_all_adapter_rejects_unproved_ranking_or_top_n(
    tmp_path: Path, rule: PortfolioWeightRule, message: str
) -> None:
    evidence = _load(_frozen_file(tmp_path / "rank-rule.duckdb"))

    with pytest.raises(EqualCandidateRankingError, match=message):
        build_equal_weight_ranking(evidence, rule)


def test_equal_all_adapter_requires_receipt_v2_and_its_exact_candidate_prices(
    tmp_path: Path,
) -> None:
    evidence = _load(_frozen_file(tmp_path / "verified-v2.duckdb"))
    rule = PortfolioWeightRule(method="equal", max_positions=2)
    legacy_receipt = ScreenRunReceipt.model_validate(
        {
            **evidence.screen.receipt.model_dump(mode="python", exclude={"result_version"}),
            "contract": "screen-run-receipt/v1",
            "price_digest": None,
        }
    )
    legacy = VerifiedScreenCalendarCandidates(
        screen=evidence.screen.model_copy(update={"receipt": legacy_receipt}),
        calendar=evidence.calendar,
    )
    with pytest.raises(EqualCandidateRankingError, match="v2"):
        build_equal_weight_ranking(legacy, rule)

    changed = VerifiedScreenCalendarCandidates(
        screen=evidence.screen.model_copy(
            update={
                "candidates": (
                    VerifiedScreenCandidate(ts_code="000001.SZ", previous_close=8.5),
                    evidence.screen.candidates[1],
                )
            }
        ),
        calendar=evidence.calendar,
    )
    with pytest.raises(EqualCandidateRankingError, match="price digest"):
        build_equal_weight_ranking(changed, rule)


def test_equal_all_adapter_does_not_rewrite_receipted_stock_codes(tmp_path: Path) -> None:
    evidence = _load(_frozen_file(tmp_path / "raw-code.duckdb"))
    candidates = (
        VerifiedScreenCandidate(ts_code="000001.sz", previous_close=8.25),
        evidence.screen.candidates[1],
    )
    receipt = ScreenRunReceipt.model_validate(
        {
            **evidence.screen.receipt.model_dump(mode="python", exclude={"result_version"}),
            "member_digest": member_set_digest([item.ts_code for item in candidates]),
            "price_digest": member_price_digest(
                [(item.ts_code, item.previous_close) for item in candidates]
            ),
        }
    )
    changed = VerifiedScreenCalendarCandidates(
        screen=evidence.screen.model_copy(update={"receipt": receipt, "candidates": candidates}),
        calendar=evidence.calendar,
    )

    with pytest.raises(EqualCandidateRankingError, match="canonical code"):
        build_equal_weight_ranking(changed, PortfolioWeightRule(method="equal", max_positions=2))
