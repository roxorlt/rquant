"""Membership publication joins one replica's receipts, calendar and raw closes."""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.serving_page_projection_source import DuckDBSignalPageProjectionSource
from rquant.serving_read_models import ServingProjectionPayload
from tests.unit.test_pool_result_publication import (
    NOW,
    OLD_DAY,
    TODAY,
    TWO_DAYS_BEFORE,
    _database,
    _insert_receipt,
    _seal_database_before_cutoff,
    _trading_evidence,
)

_POOL = "n-shape-pool1"


def _projection(path: Path) -> dict[str, ServingProjectionPayload]:
    snapshot = DuckDBSignalPageProjectionSource(path)(NOW)
    return {item.table_name: item for item in snapshot.projections}


def _member_result(path: Path, *, close: float = 12.0) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            INSERT INTO screen_result (
                trade_date, preset_name, ts_code, name, close, pct_chg, extra, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (TODAY, _POOL, "600000.SH", "PF", close, 0.0, "{}", "2026-08-03 15:05:00"),
        )


def _three_day_history(path: Path, *, calendar_gap: date | None = None) -> str:
    _database(path)
    _trading_evidence(path, calendar_gap=calendar_gap)
    _member_result(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute("UPDATE daily_bar SET close = 10.6 WHERE trade_date = ?", (OLD_DAY,))
        connection.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES (?, ?, ?)",
            ("600000.SH", TODAY, 12.0),
        )
    _insert_receipt(path, day=TWO_DAYS_BEFORE)
    entered = _insert_receipt(path, day=OLD_DAY, members=("600000.SH",))
    _insert_receipt(path, day=TODAY, members=("600000.SH",))
    _seal_database_before_cutoff(path)
    assert entered.result_version is not None
    return entered.result_version


def test_same_replica_history_publishes_only_proven_entry_close(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    entered_version = _three_day_history(path)

    projections = _projection(path)
    rows = [row for row in projections["pool_membership"].rows if row["pool_name"] == _POOL]

    assert rows[0]["row_kind"] == "status"
    assert rows[0]["status"] == "verified"
    member = next(row for row in rows if row["row_kind"] == "member")
    assert member["trade_date"] == TODAY.isoformat()
    assert member["ts_code"] == "600000.SH"
    assert member["entry_trade_date"] == OLD_DAY.isoformat()
    assert member["entry_close"] == 10.6
    assert member["entry_result_version"] == entered_version
    assert member["unknown_reason"] is None
    assert "gain_pct" not in member


def test_zero_hit_receipt_is_verified_empty_not_unrun(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    _trading_evidence(path)
    _insert_receipt(path, day=TODAY)
    _seal_database_before_cutoff(path)

    published = _projection(path)["pool_membership"].rows
    rows = [row for row in published if row["pool_name"] == _POOL]

    assert len(rows) == 1
    assert rows[0]["row_kind"] == "status"
    assert rows[0]["status"] == "verified"
    assert rows[0]["result_version"] is not None
    assert rows[0]["trade_date"] == TODAY.isoformat()
    other = next(row for row in published if row["pool_name"] == "n-shape-pool2")
    assert other["status"] == "not_run"


def test_legacy_replica_keeps_existing_hits_and_marks_entry_unverified(tmp_path: Path) -> None:
    from tests.unit.test_serving_page_projection_source import _signal_projection_database

    path = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(path)

    projections = _projection(path)

    assert len(projections["canvas_hit"].rows) == 1
    rows = [row for row in projections["pool_membership"].rows if row["pool_name"] == _POOL]
    assert len(rows) == 1
    assert rows[0]["status"] == "legacy_unproven"
    assert rows[0]["trade_date"] == OLD_DAY.isoformat()


def test_complete_calendar_gap_revokes_entry_but_not_base_members(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path, calendar_gap=date(2026, 8, 1))

    projections = _projection(path)
    rows = [row for row in projections["pool_membership"].rows if row["pool_name"] == _POOL]

    assert len(projections["canvas_hit"].rows) == 1
    assert len(rows) == 1
    assert rows[0]["status"] == "calendar_incomplete"


@pytest.mark.parametrize("corruption", ["missing", "changed"])
def test_missing_or_changed_authoritative_daily_close_suppresses_entry_price(
    tmp_path: Path, corruption: str
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path)
    with duckdb.connect(str(path)) as connection:
        if corruption == "missing":
            connection.execute("UPDATE daily_bar SET close = NULL WHERE trade_date = ?", (OLD_DAY,))
        else:
            connection.execute("UPDATE daily_bar SET close = 11.6 WHERE trade_date = ?", (OLD_DAY,))
    _seal_database_before_cutoff(path)

    rows = [row for row in _projection(path)["pool_membership"].rows if row["pool_name"] == _POOL]
    member = next(row for row in rows if row["row_kind"] == "member")

    assert member["entry_trade_date"] is None
    assert member["entry_close"] is None
    assert member["unknown_reason"] == "entry_price_missing"


def test_tampered_current_member_set_never_publishes_entry(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_result SET ts_code = '600001.SH' "
            "WHERE trade_date = ? AND preset_name = ?",
            (TODAY, _POOL),
        )
    _seal_database_before_cutoff(path)

    rows = [row for row in _projection(path)["pool_membership"].rows if row["pool_name"] == _POOL]

    assert len(rows) == 1
    assert rows[0]["status"] == "unverified"


def test_unverified_historical_run_breaks_current_member_entry(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_run_receipt SET member_digest = ? WHERE trade_date = ?",
            ("f" * 64, OLD_DAY),
        )
    _seal_database_before_cutoff(path)

    rows = [row for row in _projection(path)["pool_membership"].rows if row["pool_name"] == _POOL]
    member = next(row for row in rows if row["row_kind"] == "member")

    assert rows[0]["status"] == "verified"
    assert member["entry_trade_date"] is None
    assert member["entry_close"] is None
    assert member["unknown_reason"] == "legacy_unproven"


def test_invalid_future_dated_candidate_does_not_break_optional_projection(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    _insert_receipt(path, day=TODAY)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_run_receipt SET completed_at = TIMESTAMPTZ '2026-07-31 07:10:00+00' "
            "WHERE trade_date = ?",
            (TODAY,),
        )

    rows = [row for row in _projection(path)["pool_membership"].rows if row["pool_name"] == _POOL]

    assert len(rows) == 1
    assert rows[0]["status"] == "unverified"
    assert rows[0]["trade_date"] is None


def test_membership_source_bound_degrades_only_optional_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.serving_page_projection_source as source_module

    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path)
    monkeypatch.setattr(source_module, "_MAX_POOL_MEMBERSHIP_SOURCE_RECEIPTS", 1)

    projections = _projection(path)
    rows = [row for row in projections["pool_membership"].rows if row["pool_name"] == _POOL]

    assert len(projections["canvas_hit"].rows) == 1
    assert len(rows) == 1
    assert rows[0]["status"] == "source_limited"


def test_atomic_replica_replacement_does_not_mix_old_entry_with_new_zero(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path)
    source = DuckDBSignalPageProjectionSource(path, atomically_published=True)
    first = {item.table_name: item for item in source(NOW).projections}
    assert any(
        row["row_kind"] == "member"
        for row in first["pool_membership"].rows
        if row["pool_name"] == _POOL
    )

    replacement = tmp_path / "next.duckdb"
    _database(replacement)
    _trading_evidence(replacement)
    _insert_receipt(replacement, day=TODAY)
    _seal_database_before_cutoff(replacement)
    os.replace(replacement, path)

    second = {item.table_name: item for item in source(NOW + timedelta(seconds=1)).projections}
    rows = [row for row in second["pool_membership"].rows if row["pool_name"] == _POOL]
    assert len(rows) == 1
    assert rows[0]["status"] == "verified"
    assert rows[0]["result_version"] != first["pool_membership"].rows[0]["result_version"]
