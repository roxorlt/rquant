"""Offline archive to DuckDB financial observation and PIT query tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pytest

from rquant.data_catalog.build import build_catalog, schema_from_connection
from rquant.data_catalog.descriptions import DATASETS, FIELDS
from rquant.data_contracts import DATASET_CONTRACTS
from rquant.financial_pit_acquisition import (
    FinancialArchive,
    FinancialQuery,
    acquire_financial_batches,
)
from rquant.financial_pit_facts import (
    FinancialPITQuery,
    _import_page,
    _observation_records,
    import_financial_archive,
    query_financial_pit,
)
from rquant.pit_visibility import VisibilityInput, evaluate_visibility, query_visible_rows
from rquant.storage.migrations import MIGRATIONS, initialize_schema

SHANGHAI = ZoneInfo("Asia/Shanghai")
SYMBOL = "600000.SH"
PERIOD = date(2025, 12, 31)


class _Client:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows

    def __getattr__(self, name: str) -> object:
        def call(**kwargs: str) -> pd.DataFrame:
            return pd.DataFrame(self.rows)

        return call


def _query(api: str) -> FinancialQuery:
    values: dict[str, object] = {"request_id": uuid4(), "api": api, "ts_code": SYMBOL}
    if api == "fina_indicator":
        values["period"] = PERIOD
    elif api == "dividend":
        values["ann_date"] = date(2026, 9, 24)
    else:
        values.update(start_date=date(2026, 9, 1), end_date=date(2026, 9, 24))
    return FinancialQuery(**values)


def _row(api: str = "income", **changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ts_code": SYMBOL,
        "end_date": "20251231",
        "ann_date": "20260924",
        "revenue": 100,
    }
    if api in {"income", "balancesheet", "cashflow"}:
        row.update(f_ann_date="20260924", report_type="1")
    elif api == "forecast":
        row["type"] = "预增"
    elif api == "dividend":
        row["div_proc"] = "预案"
    row.update(changes)
    return row


def _observe(
    archive: FinancialArchive,
    at: datetime,
    rows: list[dict[str, object]],
    api: str = "income",
) -> None:
    acquire_financial_batches(
        _Client(rows), archive, (_query(api),), run_day=date(2026, 9, 28), clock=lambda: at
    )


def _calendar(conn: duckdb.DuckDBPyConnection, *, omit: date | None = None) -> None:
    for offset in range(8):
        day = date(2026, 9, 24) + timedelta(days=offset)
        if day == omit:
            continue
        conn.execute(
            "INSERT INTO trade_calendar "
            "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
            "VALUES ('SSE', ?, ?, NULL, 'test', ?)",
            [
                day,
                day in {date(2026, 9, 24), date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30)},
                datetime(2026, 9, 24, tzinfo=UTC),
            ],
        )


def _pit_query(api: str = "income", **changes: object) -> FinancialPITQuery:
    values: dict[str, object] = {
        "source_api": api,
        "field": "revenue",
        "ts_code": SYMBOL,
        "report_period": PERIOD,
        "report_type": "1" if api in {"income", "balancesheet", "cashflow"} else "default",
        "as_of": datetime(2026, 9, 28, 10, tzinfo=SHANGHAI),
    }
    values.update(changes)
    return FinancialPITQuery(**values)


def test_committed_pages_are_bounded_and_prove_anchor_prefix(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    first_at = datetime(2026, 9, 28, 1, tzinfo=UTC)
    _observe(archive, first_at, [_row()])
    _observe(archive, first_at + timedelta(minutes=1), [_row(revenue=110)])
    (archive.root / "batches" / f"{uuid4()}.json").write_text("orphan", encoding="utf-8")

    first = archive.committed_page(limit=1)
    assert len(first.entries) == 1
    assert first.has_more
    assert first.entries[0].batch.observed_at == first_at
    second = archive.committed_page(
        after=first_at,
        limit=1,
        accepted_anchor_generation=first.anchor_generation,
        accepted_anchor_sha256=first.anchor_record_sha256,
    )
    assert [entry.batch.observed_at for entry in second.entries] == [
        first_at + timedelta(minutes=1)
    ]
    assert not second.has_more
    with pytest.raises(ValueError, match="limit"):
        archive.committed_page(limit=33)
    with pytest.raises(ValueError, match="high-water"):
        archive.committed_page(after=first_at + timedelta(days=1))
    with pytest.raises(ValueError, match="anchor"):
        archive.committed_page(
            limit=1,
            accepted_anchor_generation=first.anchor_generation,
            accepted_anchor_sha256="0" * 64,
        )


def test_same_id_archive_fork_with_higher_water_fails_prefix_proof(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    archive = FinancialArchive(root)
    start = datetime(2026, 9, 28, 1, tzinfo=UTC)
    _observe(archive, start, [_row(revenue=100)])
    old_snapshot = (root / "manifest.sqlite3").read_bytes()
    old_anchor = (root / "manifest.anchor.jsonl").read_bytes()
    _observe(archive, start + timedelta(minutes=1), [_row(revenue=110)])
    accepted = archive.committed_page(limit=1)
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        assert import_financial_archive(conn, archive, page_size=1) == 2
        cursor_before = conn.execute("SELECT * FROM financial_import_cursor").fetchone()

        (root / "manifest.sqlite3").write_bytes(old_snapshot)
        (root / "manifest.anchor.jsonl").write_bytes(old_anchor)
        _observe(archive, start + timedelta(minutes=2), [_row(revenue=120)])

        with pytest.raises(ValueError, match="anchor.*prefix"):
            archive.committed_page(
                after=start,
                accepted_anchor_generation=accepted.anchor_generation,
                accepted_anchor_sha256=accepted.anchor_record_sha256,
            )
        with pytest.raises(ValueError, match="anchor.*prefix"):
            import_financial_archive(conn, archive)
        assert conn.execute("SELECT * FROM financial_import_cursor").fetchone() == cursor_before
        assert conn.execute("SELECT COUNT(*) FROM financial_import_batch").fetchone() == (2,)


@pytest.mark.parametrize(
    "api",
    ["fina_indicator", "income", "balancesheet", "cashflow", "forecast", "express", "dividend"],
)
def test_seven_apis_import_raw_rows_and_empty_batches(tmp_path: Path, api: str) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    _observe(archive, datetime(2026, 9, 28, 1, tzinfo=UTC), [_row(api)], api)
    _observe(archive, datetime(2026, 9, 28, 2, tzinfo=UTC), [], api)
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        assert import_financial_archive(conn, archive, page_size=1) == 2
        assert import_financial_archive(conn, archive, page_size=1) == 0
        assert conn.execute(
            "SELECT status, row_count FROM financial_import_batch ORDER BY observed_at"
        ).fetchall() == [("observed", 1), ("empty", 0)]
        observed = conn.execute(
            "SELECT source_api, report_period, report_type, raw_json, pit_usable "
            "FROM financial_observation"
        ).fetchone()
        assert observed[0] == api
        assert observed[1] == PERIOD
        assert observed[2] == (
            "1"
            if api in {"income", "balancesheet", "cashflow"}
            else "预增"
            if api == "forecast"
            else "default"
        )
        assert '"revenue":100' in observed[3]
        assert observed[4] is True


def test_field_versions_keep_first_seen_across_pages_and_other_field_changes(
    tmp_path: Path,
) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    first = datetime(2026, 9, 24, 8, tzinfo=UTC)
    _observe(archive, first, [_row(revenue=100, cost=30)])
    _observe(archive, first + timedelta(minutes=1), [_row(revenue=100, cost=31)])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        _calendar(conn)
        assert import_financial_archive(conn, archive, page_size=1) == 2
        selected = query_financial_pit(conn, _pit_query())
        assert selected.status == "selected"
        assert selected.fact.first_observed_at == first


def test_missing_field_blocks_old_value_and_later_complete_row_recovers(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    _observe(archive, datetime(2026, 9, 24, 8, tzinfo=UTC), [_row(revenue=100)])
    missing = _row(cost=30)
    del missing["revenue"]
    _observe(archive, datetime(2026, 9, 29, 1, tzinfo=UTC), [missing])
    _observe(archive, datetime(2026, 9, 29, 3, tzinfo=UTC), [_row(revenue=120)])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        _calendar(conn)
        import_financial_archive(conn, archive, page_size=1)
        before = query_financial_pit(
            conn, _pit_query(as_of=datetime(2026, 9, 29, 8, tzinfo=SHANGHAI))
        )
        blocked = query_financial_pit(
            conn, _pit_query(as_of=datetime(2026, 9, 29, 10, tzinfo=SHANGHAI))
        )
        recovered = query_financial_pit(
            conn, _pit_query(as_of=datetime(2026, 9, 29, 13, tzinfo=SHANGHAI))
        )
        assert before.fact.value == 100
        assert blocked.status == "unknown"
        assert recovered.fact.value == 120


def test_missing_calendar_day_and_candidate_cap_fail_closed(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    _observe(archive, datetime(2026, 9, 24, 8, tzinfo=UTC), [_row()])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        _calendar(conn, omit=date(2026, 9, 27))
        import_financial_archive(conn, archive)
        assert query_financial_pit(conn, _pit_query()).reason == "invalid_calendar"
        assert (
            query_financial_pit(conn, _pit_query(), max_candidate_rows=0).reason
            == "candidate_limit"
        )


def test_financial_contract_is_hidden_and_all_generic_query_paths_reject() -> None:
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        assert conn.execute(
            "SELECT COUNT(*) FROM schema_migration WHERE version = 13"
        ).fetchone() == (1,)
        contract = next(
            item for item in DATASET_CONTRACTS if item.dataset_id == "financial_observation"
        )
        catalog = build_catalog(DATASET_CONTRACTS, schema_from_connection(conn), DATASETS, FIELDS)
        assert contract.visibility.value == "financial_pit"
        assert "financial_observation" not in {item.dataset_id for item in catalog.datasets}
        event = VisibilityInput(dataset_id="financial_observation")
        with pytest.raises(ValueError, match="financial PIT"):
            evaluate_visibility(event, as_of_time=datetime(2026, 9, 28, 10, tzinfo=SHANGHAI))
        with pytest.raises(ValueError, match="financial PIT"):
            query_visible_rows(
                conn, "financial_observation", datetime(2026, 9, 28, 10, tzinfo=SHANGHAI)
            )


def test_v13_migration_adds_tables_to_an_existing_migrated_database() -> None:
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn, migrations=MIGRATIONS[:12])
        assert conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'financial_%'"
        ).fetchone() == (0,)
        initialize_schema(conn)
        assert conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name LIKE 'financial_%' ORDER BY table_name"
        ).fetchall() == [
            ("financial_import_batch",),
            ("financial_import_cursor",),
            ("financial_observation",),
        ]
        initialize_schema(conn)
        assert conn.execute(
            "SELECT COUNT(*) FROM schema_migration WHERE version = 13"
        ).fetchone() == (1,)


def test_replay_is_idempotent_but_different_existing_row_rolls_back(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    _observe(archive, datetime(2026, 9, 28, 1, tzinfo=UTC), [_row()])
    page = archive.committed_page()
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        assert import_financial_archive(conn, archive) == 1
        _import_page(
            conn,
            page,
            conn.execute(
                "SELECT archive_id, last_observed_at, anchor_generation, anchor_record_sha256 "
                "FROM financial_import_cursor"
            ).fetchone(),
        )
        assert conn.execute("SELECT COUNT(*) FROM financial_observation").fetchone() == (1,)
        conn.execute("UPDATE financial_observation SET row_sha256 = '0' WHERE row_index = 0")
        with pytest.raises(ValueError, match="differs"):
            _import_page(
                conn,
                page,
                conn.execute(
                    "SELECT archive_id, last_observed_at, anchor_generation, anchor_record_sha256 "
                    "FROM financial_import_cursor"
                ).fetchone(),
            )
        assert conn.execute("SELECT COUNT(*) FROM financial_import_batch").fetchone() == (1,)


def test_second_batch_conflict_rolls_back_first_batch_in_same_page(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    at = datetime(2026, 9, 28, 1, tzinfo=UTC)
    _observe(archive, at, [_row(revenue=100)])
    _observe(archive, at + timedelta(minutes=1), [_row(revenue=120)])
    page = archive.committed_page(limit=2)
    corrupt = list(_observation_records(page.archive_id, page.entries[1])[0])
    corrupt[11] = "0" * 64
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        conn.execute(
            "INSERT INTO financial_observation VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            corrupt,
        )
        with pytest.raises(ValueError, match="differs"):
            import_financial_archive(conn, archive, page_size=2)
        assert conn.execute("SELECT COUNT(*) FROM financial_import_batch").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM financial_import_cursor").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM financial_observation").fetchone() == (1,)


def test_tampered_batch_and_archive_swap_do_not_advance_import_cursor(tmp_path: Path) -> None:
    first = FinancialArchive(tmp_path / "first")
    other = FinancialArchive(tmp_path / "other")
    at = datetime(2026, 9, 28, 1, tzinfo=UTC)
    _observe(first, at, [_row()])
    _observe(other, at, [_row(revenue=200)])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        assert import_financial_archive(conn, first) == 1
        original = conn.execute("SELECT * FROM financial_import_cursor").fetchone()
        with pytest.raises(ValueError, match="anchor|identity"):
            import_financial_archive(conn, other)
        assert conn.execute("SELECT * FROM financial_import_cursor").fetchone() == original
        _observe(first, at + timedelta(minutes=1), [_row(revenue=120)])
        newest = first.list_committed()[-1]
        target = tmp_path / "first" / "batches" / f"{newest.query.request_id}.json"
        target.write_bytes(target.read_bytes().replace(b"120", b"121", 1))
        with pytest.raises(ValueError, match="digest"):
            import_financial_archive(conn, first)
        assert conn.execute("SELECT * FROM financial_import_cursor").fetchone() == original
        assert conn.execute("SELECT COUNT(*) FROM financial_import_batch").fetchone() == (1,)


def test_cursor_rollback_is_rejected_before_replaying_rows(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    at = datetime(2026, 9, 28, 1, tzinfo=UTC)
    _observe(archive, at, [_row()])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        import_financial_archive(conn, archive)
        conn.execute("UPDATE financial_import_cursor SET last_observed_at = NULL")
        with pytest.raises(ValueError, match="cursor.*rolled back"):
            import_financial_archive(conn, archive)
        assert conn.execute("SELECT COUNT(*) FROM financial_import_batch").fetchone() == (1,)


def test_field_a_b_a_is_a_new_version_and_late_first_seen_is_not_backfilled(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    start = datetime(2026, 9, 24, 8, tzinfo=UTC)
    _observe(archive, start, [_row(revenue=100)])
    _observe(archive, datetime(2026, 9, 28, 2, tzinfo=UTC), [_row(revenue=120)])
    last = datetime(2026, 9, 29, 1, tzinfo=UTC)
    _observe(archive, last, [_row(revenue=100)])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        _calendar(conn)
        import_financial_archive(conn, archive, page_size=1)
        selected = query_financial_pit(
            conn, _pit_query(as_of=datetime(2026, 9, 29, 12, tzinfo=SHANGHAI))
        )
        assert selected.fact.value == 100
        assert selected.fact.first_observed_at == last
        late = FinancialArchive(tmp_path / "late")
        _observe(late, datetime(2026, 10, 1, 1, tzinfo=UTC), [_row(revenue=130)])
        with duckdb.connect(":memory:") as other:
            initialize_schema(other)
            _calendar(other)
            import_financial_archive(other, late)
            assert query_financial_pit(other, _pit_query()).status == "unknown"


@pytest.mark.parametrize("missing_key_column", ["report_type", "end_date"])
def test_unkeyed_observation_persists_after_keyed_recovery(
    tmp_path: Path, missing_key_column: str,
) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    _observe(archive, datetime(2026, 9, 24, 8, tzinfo=UTC), [_row(revenue=100)])
    unkeyed = _row(revenue=110)
    unkeyed[missing_key_column] = None
    _observe(
        archive,
        datetime(2026, 9, 29, 1, tzinfo=UTC),
        [unkeyed],
    )
    _observe(
        archive,
        datetime(2026, 9, 29, 2, tzinfo=UTC),
        [_row(ann_date=None, revenue=115)],
    )
    _observe(archive, datetime(2026, 9, 29, 3, tzinfo=UTC), [_row(revenue=120)])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        _calendar(conn)
        import_financial_archive(conn, archive, page_size=1)
        historical = query_financial_pit(
            conn, _pit_query(as_of=datetime(2026, 9, 29, 8, 59, tzinfo=SHANGHAI))
        )
        before = query_financial_pit(
            conn, _pit_query(as_of=datetime(2026, 9, 29, 10, tzinfo=SHANGHAI))
        )
        masked = query_financial_pit(
            conn, _pit_query(as_of=datetime(2026, 9, 29, 11, tzinfo=SHANGHAI))
        )
        recovered = query_financial_pit(
            conn, _pit_query(as_of=datetime(2026, 9, 29, 12, tzinfo=SHANGHAI))
        )
        assert historical.fact.value == 100
        assert before.reason == "unkeyed_observation"
        assert masked.reason == "unkeyed_observation"
        assert recovered.status == "unknown"
        assert recovered.reason == "unkeyed_observation"


def test_keyed_missing_announcement_is_cleared_by_later_complete_version(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    _observe(archive, datetime(2026, 9, 24, 8, tzinfo=UTC), [_row(revenue=100)])
    _observe(archive, datetime(2026, 9, 29, 2, tzinfo=UTC), [_row(ann_date=None)])
    _observe(archive, datetime(2026, 9, 29, 3, tzinfo=UTC), [_row(revenue=120)])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        _calendar(conn)
        import_financial_archive(conn, archive, page_size=1)
        masked = query_financial_pit(
            conn, _pit_query(as_of=datetime(2026, 9, 29, 11, tzinfo=SHANGHAI))
        )
        recovered = query_financial_pit(
            conn, _pit_query(as_of=datetime(2026, 9, 29, 12, tzinfo=SHANGHAI))
        )
        assert masked.reason == "missing_announcement_date"
        assert recovered.fact.value == 120


def test_same_batch_conflict_and_empty_observation_do_not_imply_zero(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    _observe(
        archive,
        datetime(2026, 9, 24, 8, tzinfo=UTC),
        [_row(revenue=100), _row(revenue=110)],
    )
    _observe(archive, datetime(2026, 9, 24, 9, tzinfo=UTC), [])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        _calendar(conn)
        import_financial_archive(conn, archive)
        assert conn.execute(
            "SELECT COUNT(*) FROM financial_observation WHERE conflicted"
        ).fetchone() == (2,)
        assert query_financial_pit(conn, _pit_query()).reason == "conflicted_observation"
        assert (
            query_financial_pit(conn, _pit_query(field="missing_financial_field")).status
            == "unknown"
        )


def test_possible_truncation_is_logged_without_claiming_field_coverage(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    _observe(
        archive,
        datetime(2026, 9, 24, 8, tzinfo=UTC),
        [_row("fina_indicator") for _ in range(100)],
        "fina_indicator",
    )
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        _calendar(conn)
        import_financial_archive(conn, archive)
        assert conn.execute(
            "SELECT status, row_count FROM financial_import_batch"
        ).fetchone() == ("possibly_truncated", 100)
        assert query_financial_pit(
            conn, _pit_query("fina_indicator", field="not_returned")
        ).status == "unknown"


def test_pit_query_ignores_unledgered_and_other_archive_rows(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    observed_at = datetime(2026, 9, 24, 8, tzinfo=UTC)
    _observe(archive, observed_at, [_row(revenue=100)])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        _calendar(conn)
        import_financial_archive(conn, archive)
        saved = conn.execute("SELECT * FROM financial_observation").fetchone()
        unledgered = list(saved)
        unledgered[1] = str(uuid4())
        unledgered[5] = datetime(2026, 9, 28, 1, tzinfo=UTC)
        unledgered[10] = unledgered[10].replace('"revenue":100', '"revenue":200')
        conn.execute(
            "INSERT INTO financial_observation VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            unledgered,
        )
        other_archive = list(saved)
        other_archive[0] = "f" * 32
        other_archive[5] = datetime(2026, 9, 28, 2, tzinfo=UTC)
        other_archive[10] = other_archive[10].replace('"revenue":100', '"revenue":300')
        conn.execute(
            "INSERT INTO financial_observation VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            other_archive,
        )
        assert query_financial_pit(conn, _pit_query()).fact.value == 100


def test_out_of_range_decision_time_is_explicit_unknown(tmp_path: Path) -> None:
    archive = FinancialArchive(tmp_path / "archive")
    _observe(archive, datetime(2026, 9, 24, 8, tzinfo=UTC), [_row()])
    with duckdb.connect(":memory:") as conn:
        initialize_schema(conn)
        import_financial_archive(conn, archive)
        assert query_financial_pit(
            conn, _pit_query(as_of=datetime.max.replace(tzinfo=UTC))
        ).reason == "invalid_as_of"
