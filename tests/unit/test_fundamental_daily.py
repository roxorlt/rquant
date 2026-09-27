"""Six PIT fundamental fields share one decision instant and immutable version."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pytest

import rquant.fundamental_daily as fundamental_daily
from rquant.daily_valuation_pit import (
    DailyValuationBatch,
    DailyValuationRow,
    _record_daily_valuation_batch_in_transaction,
)
from rquant.financial_pit_acquisition import (
    FinancialArchive,
    FinancialQuery,
    acquire_financial_batches,
)
from rquant.financial_pit_facts import import_financial_archive
from rquant.fundamental_daily import (
    FundamentalDailyQuery,
    derive_fundamental_daily,
    read_fundamental_daily,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.storage.migrations import MIGRATIONS, initialize_schema

SHANGHAI = ZoneInfo("Asia/Shanghai")
SYMBOL = "600000.SH"
THURSDAY = date(2026, 9, 24)
FRIDAY = date(2026, 9, 25)
MONDAY = date(2026, 9, 28)
TUESDAY = date(2026, 9, 29)
OLD_PERIOD = date(2025, 12, 31)
NEW_PERIOD = date(2026, 6, 30)


class _Client:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows

    def fina_indicator(self, **kwargs: str) -> pd.DataFrame:
        return pd.DataFrame(self.rows)


def _conn(path: Path | None = None) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(path) if path is not None else ":memory:")
    initialize_schema(conn)
    if conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0]:
        return conn
    prior = date(2026, 9, 23)
    for offset in range(8):
        day = prior + timedelta(days=offset)
        conn.execute(
            "INSERT INTO trade_calendar "
            "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
            "VALUES ('SSE', ?, ?, ?, 'tushare', ?)",
            [
                day,
                day in {prior, THURSDAY, FRIDAY, MONDAY, TUESDAY, date(2026, 9, 30)},
                (prior if day == THURSDAY else THURSDAY if day == FRIDAY else FRIDAY),
                datetime(2026, 9, 23, tzinfo=UTC),
            ],
        )
    return conn


def _finance(
    conn: duckdb.DuckDBPyConnection,
    archive: FinancialArchive,
    *,
    period: date = OLD_PERIOD,
    observed_at: datetime = datetime(2026, 9, 24, 8, tzinfo=UTC),
    ann_date: str | None = "20260924",
    roe: float | None = 12.0,
    or_yoy: float | None = 18.0,
    netprofit_yoy: float | None = 20.0,
    omit: frozenset[str] = frozenset(),
) -> None:
    row: dict[str, object] = {
        "ts_code": SYMBOL,
        "end_date": period.strftime("%Y%m%d"),
        "ann_date": ann_date,
        "roe": roe,
        "or_yoy": or_yoy,
        "netprofit_yoy": netprofit_yoy,
    }
    for field in omit:
        row.pop(field)
    acquire_financial_batches(
        _Client([row]),
        archive,
        (FinancialQuery(request_id=uuid4(), api="fina_indicator", ts_code=SYMBOL, period=period),),
        run_day=date(2026, 10, 9),
        clock=lambda: observed_at,
    )
    import_financial_archive(conn, archive)


def _valuation(
    conn: duckdb.DuckDBPyConnection,
    *,
    revision: int = 1,
    trade_date: date = FRIDAY,
    observed_at: datetime = datetime(2026, 9, 25, 8, tzinfo=UTC),
    pe_ttm: float | None = 10.0,
    pb: float | None = 2.0,
    dv_ttm: float | None = 1.5,
    include_symbol: bool = True,
) -> None:
    _record_daily_valuation_batch_in_transaction(
        conn,
        DailyValuationBatch(
            candidate_generation_id=f"{revision:064x}",
            source_generation_id="a" * 64,
            source_sequence=revision,
            source_batch_id=f"{revision + 100:064x}",
            revision=revision,
            trade_date=trade_date,
            observed_at=observed_at,
            valuation_observed=True,
            rows=(
                (
                    DailyValuationRow(
                        ts_code=SYMBOL,
                        trade_date=trade_date,
                        pe_ttm=pe_ttm,
                        pb=pb,
                        dv_ttm=dv_ttm,
                    ),
                )
                if include_symbol
                else ()
            ),
        ),
    )


def _derive(conn: duckdb.DuckDBPyConnection, day: date = MONDAY):
    return derive_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=day))


def test_holiday_publication_uses_one_period_and_one_valuation_row(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(
            conn,
            archive,
            observed_at=datetime(2026, 9, 25, 8, tzinfo=UTC),
            ann_date="20260925",
        )
        _valuation(conn, pe_ttm=None)

        version = _derive(conn)

        assert version.decision_at == datetime(2026, 9, 28, 17, tzinfo=SHANGHAI)
        assert version.target_report_period == OLD_PERIOD
        assert {
            version.fields[name].source_date for name in ("roe", "or_yoy", "netprofit_yoy")
        } == {OLD_PERIOD}
        assert [version.fields[name].value for name in ("roe", "or_yoy", "netprofit_yoy")] == [
            12,
            18,
            20,
        ]
        assert {version.fields[name].source_date for name in ("pe_ttm", "pb", "dv_ttm")} == {FRIDAY}
        assert version.fields["pe_ttm"].status == "unknown"
        assert version.fields["pe_ttm"].value is None
        assert version.fields["pb"].value == 2
        assert (
            version.fields["pb"].source_version_sha256
            == version.fields["dv_ttm"].source_version_sha256
        )
        assert (
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))
            == version
        )


def test_announcement_day_and_late_observation_do_not_enter_history(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(
            conn,
            archive,
            observed_at=datetime(2026, 9, 25, 8, tzinfo=UTC),
            ann_date="20260925",
        )
        friday = _derive(conn, FRIDAY)
        assert friday.fields["roe"].status == "unknown"
        assert friday.fields["roe"].value is None

        _finance(
            conn,
            archive,
            period=NEW_PERIOD,
            observed_at=datetime(2026, 9, 28, 10, tzinfo=UTC),
            ann_date="20260928",
            roe=99,
        )
        monday = _derive(conn)
        assert monday.target_report_period == OLD_PERIOD
        assert monday.fields["roe"].value == 12


def test_long_sse_holiday_waits_for_first_next_open_day(tmp_path: Path) -> None:
    with _conn() as conn:
        for offset in range(9):
            day = date(2026, 10, 1) + timedelta(days=offset)
            conn.execute(
                "INSERT INTO trade_calendar "
                "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
                "VALUES ('SSE', ?, ?, ?, 'tushare', ?)",
                [
                    day,
                    day in {date(2026, 10, 8), date(2026, 10, 9)},
                    date(2026, 10, 8) if day == date(2026, 10, 9) else date(2026, 9, 30),
                    datetime(2026, 9, 30, tzinfo=UTC),
                ],
            )
        archive = FinancialArchive(tmp_path / "archive")
        _finance(
            conn,
            archive,
            observed_at=datetime(2026, 9, 30, 8, tzinfo=UTC),
            ann_date="20260930",
        )
        _valuation(
            conn,
            trade_date=date(2026, 9, 30),
            observed_at=datetime(2026, 9, 30, 8, tzinfo=UTC),
        )
        before = _derive(conn, date(2026, 9, 30))
        after = _derive(conn, date(2026, 10, 8))
        assert before.fields["roe"].status == "unknown"
        assert after.fields["roe"].value == 12
        assert after.fields["pb"].source_date == date(2026, 9, 30)


@pytest.mark.parametrize("ann_date", ["20260928", None])
def test_newer_period_blocks_fallback_even_when_fields_empty_or_announcement_missing(
    tmp_path: Path, ann_date: str | None
) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _finance(
            conn,
            archive,
            period=NEW_PERIOD,
            observed_at=datetime(2026, 9, 28, 2, tzinfo=UTC),
            ann_date=ann_date,
            omit=frozenset({"roe", "or_yoy", "netprofit_yoy"}),
        )
        version = _derive(conn)
        assert version.target_report_period == NEW_PERIOD
        assert all(
            version.fields[field].status == "unknown"
            for field in ("roe", "or_yoy", "netprofit_yoy")
        )
        assert all(
            version.fields[field].value is None for field in ("roe", "or_yoy", "netprofit_yoy")
        )


def test_missing_one_financial_field_keeps_other_fields_and_no_old_value(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive, omit=frozenset({"or_yoy"}))
        version = _derive(conn)
        assert version.fields["roe"].value == 12
        assert version.fields["or_yoy"].status == "unknown"
        assert version.fields["or_yoy"].reason == "missing_field"
        assert version.fields["netprofit_yoy"].value == 20


def test_unrepresentable_financial_numeric_stays_unknown(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        row = {
            "ts_code": SYMBOL,
            "end_date": "20251231",
            "ann_date": "20260924",
            "roe": "1e999",
            "or_yoy": 18.0,
            "netprofit_yoy": 20.0,
        }
        acquire_financial_batches(
            _Client([row]),
            archive,
            (
                FinancialQuery(
                    request_id=uuid4(),
                    api="fina_indicator",
                    ts_code=SYMBOL,
                    period=OLD_PERIOD,
                ),
            ),
            run_day=TUESDAY,
            clock=lambda: datetime(2026, 9, 24, 8, tzinfo=UTC),
        )
        import_financial_archive(conn, archive)
        version = _derive(conn)
        assert version.fields["roe"].status == "unknown"
        assert version.fields["roe"].reason == "unrepresentable_numeric"
        assert version.fields["roe"].value is None
        assert version.fields["or_yoy"].value == 18


def test_conflicted_new_period_blocks_prior_period(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        rows = [
            {
                "ts_code": SYMBOL,
                "end_date": "20260630",
                "ann_date": "20260924",
                "roe": value,
                "or_yoy": 18.0,
                "netprofit_yoy": 20.0,
            }
            for value in (13.0, 14.0)
        ]
        acquire_financial_batches(
            _Client(rows),
            archive,
            (
                FinancialQuery(
                    request_id=uuid4(),
                    api="fina_indicator",
                    ts_code=SYMBOL,
                    period=NEW_PERIOD,
                ),
            ),
            run_day=TUESDAY,
            clock=lambda: datetime(2026, 9, 28, 2, tzinfo=UTC),
        )
        import_financial_archive(conn, archive)
        version = _derive(conn)
        assert version.target_report_period == NEW_PERIOD
        assert version.fields["roe"].status == "unknown"
        assert version.fields["roe"].reason == "conflicted_observation"


def test_expected_valuation_day_missing_row_never_uses_older_day(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn, trade_date=THURSDAY, observed_at=datetime(2026, 9, 24, 8, tzinfo=UTC))
        version = _derive(conn)
        assert all(
            version.fields[field].status == "unknown" for field in ("pe_ttm", "pb", "dv_ttm")
        )
        _valuation(conn, revision=2, include_symbol=False)
        again = _derive(conn)
        assert all(again.fields[field].status == "unknown" for field in ("pe_ttm", "pb", "dv_ttm"))


def test_revision_is_atomic_idempotent_and_old_version_is_still_readable(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn)
        a = _derive(conn)
        assert _derive(conn) == a
        assert conn.execute("SELECT COUNT(*) FROM fundamental_daily_version").fetchone() == (1,)

        _finance(
            conn,
            archive,
            observed_at=datetime(2026, 9, 28, 3, tzinfo=UTC),
            roe=13,
        )
        _valuation(conn, revision=2, observed_at=datetime(2026, 9, 28, 7, tzinfo=UTC), pb=3)
        b = _derive(conn)
        assert b.revision == a.revision + 1
        assert b.version_id != a.version_id
        assert b.fields["roe"].value == 13
        assert b.fields["pb"].value == 3
        assert (
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))
            == b
        )
        assert (
            read_fundamental_daily(
                conn,
                FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY),
                version_id=a.version_id,
            )
            == a
        )
        assert conn.execute("SELECT COUNT(*) FROM fundamental_daily_head").fetchone() == (1,)
        assert conn.execute("SELECT COUNT(*) FROM fundamental_daily_version").fetchone() == (2,)
        assert conn.execute(
            "SELECT v.roe, v.pb FROM fundamental_daily_head AS h "
            "JOIN fundamental_daily_version AS v ON v.version_id = h.version_id "
            "AND v.ts_code = h.ts_code AND v.trade_date = h.trade_date "
            "WHERE h.ts_code = ? AND h.trade_date = ?",
            [SYMBOL, MONDAY],
        ).fetchall() == [(13, 3)]
        assert conn.execute(
            "SELECT roe, pb FROM fundamental_daily_version WHERE version_id = ?", [b.version_id]
        ).fetchone() == (13, 3)

        conn.execute(
            "DELETE FROM daily_basic_valuation_observation WHERE candidate_generation_id = ?",
            [f"{2:064x}"],
        )
        conn.execute(
            "DELETE FROM daily_basic_valuation_batch WHERE candidate_generation_id = ?",
            [f"{2:064x}"],
        )
        with pytest.raises(ValueError, match="rollback|regress|source"):
            _derive(conn)
        assert (
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))
            == b
        )


def test_financial_archive_rollback_cannot_move_head(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn)
        a = _derive(conn)
        _finance(
            conn,
            archive,
            observed_at=datetime(2026, 9, 28, 3, tzinfo=UTC),
            roe=13,
        )
        b = _derive(conn)
        assert b.fields["roe"].value == 13
        conn.execute(
            "DELETE FROM financial_observation WHERE observed_at > ?",
            [a.financial_source.last_observed_at],
        )
        conn.execute(
            "DELETE FROM financial_import_batch WHERE observed_at > ?",
            [a.financial_source.last_observed_at],
        )
        conn.execute(
            "UPDATE financial_import_cursor SET last_observed_at = ?, "
            "anchor_generation = ?, anchor_record_sha256 = ? WHERE singleton = 1",
            [
                a.financial_source.last_observed_at,
                a.financial_source.anchor_generation,
                a.financial_source.anchor_record_sha256,
            ],
        )
        with pytest.raises(ValueError, match="rollback|source"):
            _derive(conn)
        assert (
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))
            == b
        )


def test_changed_prior_financial_observation_cannot_move_head(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        version = _derive(conn)
        values = json.loads(
            conn.execute("SELECT raw_json FROM financial_observation").fetchone()[0]
        )
        values["roe"] = 99
        conn.execute(
            "UPDATE financial_observation SET raw_json = ?",
            [json.dumps(values, sort_keys=True, separators=(",", ":"))],
        )
        with pytest.raises(ValueError, match="financial source|observation"):
            _derive(conn)
        assert (
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))
            == version
        )


def test_valuation_observed_at_exact_decision_time_is_unknown(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn, observed_at=datetime(2026, 9, 28, 9, tzinfo=UTC))
        version = _derive(conn)
        assert all(
            version.fields[field].status == "unknown" for field in ("pe_ttm", "pb", "dv_ttm")
        )
        assert version.fields["pb"].reason == "observed_at_decision_boundary"


def test_changed_prior_valuation_row_cannot_replace_head(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn)
        version = _derive(conn)
        conn.execute(
            "UPDATE daily_basic_valuation_observation SET pb = 999 "
            "WHERE candidate_generation_id = ?",
            [version.valuation_source.candidate_generation_id],
        )
        with pytest.raises(ValueError, match="source|changed|rollback"):
            _derive(conn)
        assert (
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))
            == version
        )


def test_prior_valuation_observed_flag_change_cannot_replace_head(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn, pb=2)
        version = _derive(conn)
        assert version.fields["pb"].value == 2
        conn.execute(
            "UPDATE daily_basic_valuation_batch SET valuation_observed = FALSE "
            "WHERE candidate_generation_id = ?",
            [version.valuation_source.candidate_generation_id],
        )
        with pytest.raises(ValueError, match="valuation source"):
            _derive(conn)
        assert (
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))
            == version
        )


def test_prior_valuation_first_observed_change_cannot_replace_head(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn, pb=2)
        version = _derive(conn)
        assert version.fields["pb"].value == 2
        conn.execute(
            "UPDATE daily_basic_valuation_observation SET first_observed_at = ? "
            "WHERE candidate_generation_id = ? AND ts_code = ?",
            [
                datetime(2026, 9, 25, 7, tzinfo=UTC),
                version.valuation_source.candidate_generation_id,
                SYMBOL,
            ],
        )
        with pytest.raises(ValueError, match="valuation source"):
            _derive(conn)
        assert (
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))
            == version
        )


def test_prior_valuation_batch_date_change_cannot_switch_to_successor(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn, pb=2)
        version = _derive(conn)
        _valuation(conn, revision=2, observed_at=datetime(2026, 9, 28, 7, tzinfo=UTC), pb=3)
        conn.execute(
            "UPDATE daily_basic_valuation_batch SET trade_date = ? "
            "WHERE candidate_generation_id = ?",
            [THURSDAY, version.valuation_source.candidate_generation_id],
        )
        with pytest.raises(ValueError, match="valuation source"):
            _derive(conn)
        assert (
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))
            == version
        )


def test_version_read_rejects_scalar_or_receipt_drift(tmp_path: Path) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn)
        version = _derive(conn)
        conn.execute(
            "UPDATE fundamental_daily_version SET roe = 999 WHERE version_id = ?",
            [version.version_id],
        )
        with pytest.raises(ValueError, match="mismatch"):
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))


def test_mid_write_failure_rolls_back_version_and_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _conn() as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn)
        a = _derive(conn)
        _finance(
            conn,
            archive,
            observed_at=datetime(2026, 9, 28, 3, tzinfo=UTC),
            roe=14,
        )

        def crash(*args: object, **kwargs: object) -> None:
            raise RuntimeError("injected head failure")

        monkeypatch.setattr(fundamental_daily, "_write_head", crash)
        with pytest.raises(RuntimeError, match="injected head failure"):
            _derive(conn)
        assert conn.execute("SELECT COUNT(*) FROM fundamental_daily_version").fetchone() == (1,)
        assert (
            read_fundamental_daily(conn, FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY))
            == a
        )


def test_legacy_state_upsert_and_tail_delete_leave_fundamental_version_intact(
    tmp_path: Path,
) -> None:
    database = tmp_path / "facts.duckdb"
    with _conn(database) as conn:
        archive = FinancialArchive(tmp_path / "archive")
        _finance(conn, archive)
        _valuation(conn)
        version = _derive(conn)
    with DuckDBStore(database) as store:
        state = pd.DataFrame(
            [
                {
                    "ts_code": SYMBOL,
                    "trade_date": MONDAY,
                    "is_st": False,
                    "is_bj": False,
                    "board_type": "main",
                    "limit_pct": 0.1,
                    "limit_up_price": None,
                    "limit_down_price": None,
                    "is_limit_up": False,
                    "is_limit_down": False,
                    "is_first_limit_up": False,
                    "is_yiziban": False,
                    "consecutive_limit_ups": 0,
                    "body_upper": None,
                    "body_lower": None,
                }
            ]
        )
        store.upsert_state(state)
        store.upsert_state(state)
        store._conn.execute("DELETE FROM daily_state WHERE trade_date >= ?", [MONDAY])
    with _conn(database) as conn:
        assert (
            read_fundamental_daily(
                conn,
                FundamentalDailyQuery(ts_code=SYMBOL, trade_date=MONDAY),
                version_id=version.version_id,
            )
            == version
        )
        assert _derive(conn) == version


def test_v15_migration_adds_version_and_head_to_existing_database() -> None:
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn, migrations=MIGRATIONS[:14])
        assert conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name LIKE 'fundamental_daily_%'"
        ).fetchone() == (0,)
        initialize_schema(conn)
        assert conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name LIKE 'fundamental_daily_%' ORDER BY table_name"
        ).fetchall() == [("fundamental_daily_head",), ("fundamental_daily_version",)]
