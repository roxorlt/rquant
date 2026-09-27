"""Serving trusts only bounded run receipts checked against the same replica."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.builtin_presets import BUILTIN_PRESET_SCREENS, builtin_definition_version
from rquant.notification_state import NotificationStateStore
from rquant.page_control import PageControlStatus
from rquant.pool_result_receipt import ScreenRunReceipt, member_set_digest
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    PageProjectionSourceIntegrityError,
    SignalPageProjectionProducer,
)
from rquant.serving_read_models import ServingProjectionPayload
from rquant.storage.schema import DAILY_BAR_DDL, SCREEN_RUN_RECEIPT_DDL, TRADE_CALENDAR_DDL
from tests.unit.test_pool_definition_publication import _service, _v2
from tests.unit.test_serving_page_projection_source import _signal_projection_database

NOW = datetime(2026, 8, 3, 8, 0, tzinfo=UTC)
OLD_DAY = date(2026, 7, 31)
TODAY = date(2026, 8, 3)
TWO_DAYS_BEFORE = date(2026, 7, 30)


def _database(path: Path) -> None:
    _signal_projection_database(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute(SCREEN_RUN_RECEIPT_DDL)


def _seal_database_before_cutoff(path: Path) -> None:
    stamp = (NOW - timedelta(minutes=1)).timestamp()
    os.utime(path, (stamp, stamp))


def _trading_evidence(
    path: Path,
    *,
    calendar_gap: date | None = None,
    include_target_bar: bool = True,
    calendar_updated_at: datetime = datetime(2026, 7, 29, tzinfo=UTC),
) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute(TRADE_CALENDAR_DDL)
        connection.execute(DAILY_BAR_DDL)
        connection.executemany(
            "INSERT INTO trade_calendar VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("SSE", day, is_open, pretrade, "fixture", calendar_updated_at)
                for day, is_open, pretrade in (
                    (TWO_DAYS_BEFORE, True, date(2026, 7, 29)),
                    (OLD_DAY, True, TWO_DAYS_BEFORE),
                    (date(2026, 8, 1), False, OLD_DAY),
                    (date(2026, 8, 2), False, OLD_DAY),
                    (TODAY, True, OLD_DAY),
                )
                if day != calendar_gap
            ],
        )
        if include_target_bar:
            connection.executemany(
                "INSERT INTO daily_bar (ts_code, trade_date) VALUES ('600000.SH', ?)",
                [(TWO_DAYS_BEFORE,), (OLD_DAY,)],
            )


def _insert_receipt(
    path: Path,
    *,
    day: date,
    pool: str = "n-shape-pool1",
    members: tuple[str, ...] = (),
    version: str | None = None,
    parent_day: date | None = None,
    parent_version: str | None = None,
    completed_at: datetime | None = None,
    lineage_complete: bool = True,
) -> ScreenRunReceipt:
    receipt = ScreenRunReceipt(
        trade_date=day,
        preset_name=pool,
        definition_version=version or builtin_definition_version(BUILTIN_PRESET_SCREENS[pool]),
        parent_trade_date=parent_day,
        parent_result_version=parent_version,
        hit_count=len(members),
        member_digest=member_set_digest(list(members)),
        lineage_complete=lineage_complete,
        completed_at=completed_at or datetime(2026, 8, 3, 7, 10, tzinfo=UTC),
    )
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            INSERT INTO screen_run_receipt VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ],
        )
    return receipt


def _projections(path: Path, observed: datetime = NOW) -> dict[str, ServingProjectionPayload]:
    snapshot = DuckDBSignalPageProjectionSource(path)(observed)
    return {item.table_name: item for item in snapshot.projections}


def test_zero_hit_receipt_advances_canvas_day_without_old_hits_or_fake_step(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    receipt = _insert_receipt(path, day=TODAY)

    projections = _projections(path)

    assert projections["canvas_latest_trade_date"].rows == (
        {"snapshot_key": "current", "trade_date": "2026-08-03"},
    )
    assert projections["canvas_hit"].rows == ()
    assert projections["canvas_diagnostic"].rows == ()
    row = projections["screen_run_receipt"].rows[0]
    assert row["preset_name"] == "n-shape-pool1"
    assert row["hit_count"] == 0
    assert row["result_version"] == receipt.result_version
    builtin = next(
        item for item in projections["pool_definition"].rows if item["pool_name"] == "n-shape-pool1"
    )
    assert row["definition_version"] == builtin["version"]


def test_member_tamper_and_forged_receipt_version_cannot_be_trusted(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    _insert_receipt(path, day=OLD_DAY, members=("600000.SH",))
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_result SET ts_code = '600001.SH' WHERE trade_date = '2026-07-31'"
        )
    assert _projections(path)["screen_run_receipt"].rows == ()

    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_result SET ts_code = '600000.SH' WHERE trade_date = '2026-07-31'"
        )
        connection.execute(
            "UPDATE screen_run_receipt SET result_version = ? WHERE trade_date = '2026-07-31'",
            ["f" * 64],
        )
    assert _projections(path)["screen_run_receipt"].rows == ()


def test_child_with_missing_parent_version_does_not_publish_false_zero(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    _insert_receipt(path, day=OLD_DAY, members=("600000.SH",))
    _insert_receipt(
        path,
        day=TODAY,
        pool="user/child",
        version="a" * 64,
        parent_day=OLD_DAY,
        parent_version="b" * 64,
    )

    projections = _projections(path)

    assert all(row["preset_name"] != "user/child" for row in projections["screen_run_receipt"].rows)
    assert projections["canvas_hit"].rows == ()


@pytest.mark.parametrize(
    ("parent_day", "calendar_state", "expected_current"),
    [
        (OLD_DAY, "complete", False),
        (TWO_DAYS_BEFORE, "complete", True),
        (TWO_DAYS_BEFORE, "missing_calendar", False),
        (TWO_DAYS_BEFORE, "calendar_gap", False),
        (TWO_DAYS_BEFORE, "missing_target_calendar", False),
        (TWO_DAYS_BEFORE, "missing_target_bar", False),
        (TWO_DAYS_BEFORE, "future_calendar", False),
        (TWO_DAYS_BEFORE, "unsealed_generation", False),
    ],
    ids=(
        "wrong-t1",
        "exact-t2",
        "missing-calendar",
        "missing-calendar-day",
        "missing-target-calendar",
        "missing-bar",
        "future-calendar",
        "unsealed-generation",
    ),
)
def test_exact_t2_current_definition_requires_complete_calendar_and_target_data(
    tmp_path: Path,
    parent_day: date,
    calendar_state: str,
    expected_current: bool,
) -> None:
    service = _service(tmp_path)
    saved = service.submit(_v2("save-v2"))
    assert saved.status is PageControlStatus.SUCCEEDED
    assert isinstance(saved.result, dict)
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    if calendar_state != "missing_calendar":
        _trading_evidence(
            path,
            calendar_gap=(
                date(2026, 8, 1)
                if calendar_state == "calendar_gap"
                else TWO_DAYS_BEFORE
                if calendar_state == "missing_target_calendar"
                else None
            ),
            include_target_bar=calendar_state != "missing_target_bar",
            calendar_updated_at=(
                NOW + timedelta(minutes=1)
                if calendar_state == "future_calendar"
                else datetime(2026, 7, 29, tzinfo=UTC)
            ),
        )
    parent = _insert_receipt(
        path,
        day=parent_day,
        members=("600000.SH",) if parent_day == OLD_DAY else (),
    )
    _insert_receipt(
        path,
        day=TODAY,
        pool="user/breakout",
        version=str(saved.result["version"]),
        parent_day=parent_day,
        parent_version=parent.result_version,
    )
    if calendar_state != "unsealed_generation":
        _seal_database_before_cutoff(path)
    source = DuckDBSignalPageProjectionSource(
        path,
        user_presets_root=tmp_path / "data" / "user_presets",
        page_control_outbox=tmp_path / "control.sqlite3",
    )

    projections = {item.table_name: item for item in source(NOW).projections}
    child = next(
        row
        for row in projections["screen_run_receipt"].rows
        if row["preset_name"] == "user/breakout"
    )
    assert child["current_definition"] is expected_current


def test_legacy_window_receipt_remains_readable_without_exact_calendar(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    _insert_receipt(
        path,
        day=TODAY,
        pool="n-shape-pool2",
        lineage_complete=False,
    )

    projections = _projections(path)

    row = projections["screen_run_receipt"].rows[0]
    assert row["preset_name"] == "n-shape-pool2"
    assert row["lineage_complete"] is False
    assert row["current_definition"] is False


def test_exact_child_requires_current_parent_members_and_rule_versions(tmp_path: Path) -> None:
    service = _service(tmp_path)
    saved = service.submit(_v2("save-v2", requested_at=datetime(2026, 8, 3, 6, 0, tzinfo=UTC)))
    assert saved.status is PageControlStatus.SUCCEEDED
    assert isinstance(saved.result, dict)
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    _trading_evidence(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_result SET trade_date = '2026-07-30' WHERE trade_date = '2026-07-31'"
        )
    parent = _insert_receipt(path, day=TWO_DAYS_BEFORE, members=("600000.SH",))
    _insert_receipt(
        path,
        day=TODAY,
        pool="user/breakout",
        version=str(saved.result["version"]),
        parent_day=TWO_DAYS_BEFORE,
        parent_version=parent.result_version,
    )
    _seal_database_before_cutoff(path)
    source = DuckDBSignalPageProjectionSource(
        path,
        user_presets_root=tmp_path / "data" / "user_presets",
        page_control_outbox=tmp_path / "control.sqlite3",
    )
    first = {item.table_name: item for item in source(NOW).projections}
    child = next(
        row for row in first["screen_run_receipt"].rows if row["preset_name"] == "user/breakout"
    )
    assert child["current_definition"] is True
    assert child["parent_result_version"] == parent.result_version

    changed = service.submit(
        _v2(
            "save-new",
            expected_version=str(saved.result["version"]),
            display_name="新版突破观察",
        )
    )
    assert changed.status is PageControlStatus.SUCCEEDED
    second = {item.table_name: item for item in source(NOW).projections}
    child = next(
        row for row in second["screen_run_receipt"].rows if row["preset_name"] == "user/breakout"
    )
    definition = next(
        row for row in second["pool_definition"].rows if row["pool_name"] == "user/breakout"
    )
    assert child["current_definition"] is False
    assert child["definition_version"] != definition["version"]

    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_result SET ts_code = '600001.SH' WHERE trade_date = '2026-07-30'"
        )
    third = _projections(path)
    assert all(row["preset_name"] != "user/breakout" for row in third["screen_run_receipt"].rows)


def test_zero_receipt_is_valid_pit_evidence_without_any_screen_rows(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute("DELETE FROM screen_result")
        connection.execute("DELETE FROM minute_bar")
    _insert_receipt(path, day=TODAY)

    projections = _projections(path)

    assert projections["canvas_latest_trade_date"].rows[0]["trade_date"] == "2026-08-03"
    assert projections["canvas_hit"].rows == ()
    assert projections["screen_run_receipt"].rows[0]["hit_count"] == 0


def test_legacy_replica_without_receipt_table_keeps_old_result_unverified(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(path)

    projections = _projections(path)

    assert "screen_run_receipt" not in projections
    assert projections["canvas_latest_trade_date"].rows == (
        {"snapshot_key": "current", "trade_date": "2026-07-31"},
    )
    assert len(projections["canvas_hit"].rows) == 1


def test_receipt_after_cutoff_is_not_published_until_observed(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    _insert_receipt(
        path,
        day=TODAY,
        completed_at=datetime(2026, 8, 3, 8, 5, tzinfo=UTC),
    )

    before = _projections(path, NOW)
    after = _projections(path, datetime(2026, 8, 3, 8, 10, tzinfo=UTC))

    assert before["screen_run_receipt"].rows == ()
    assert before["canvas_latest_trade_date"].rows[0]["trade_date"] == "2026-07-31"
    assert after["screen_run_receipt"].rows[0]["trade_date"] == "2026-08-03"
    assert after["canvas_latest_trade_date"].rows[0]["trade_date"] == "2026-08-03"


def test_atomic_replica_rotation_replaces_old_day_with_zero_receipt(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(path)
    source = DuckDBSignalPageProjectionSource(path, atomically_published=True)
    first = {item.table_name: item for item in source(NOW).projections}
    assert first["canvas_latest_trade_date"].rows[0]["trade_date"] == "2026-07-31"

    next_path = tmp_path / "next.duckdb"
    _database(next_path)
    _insert_receipt(next_path, day=TODAY)
    os.replace(next_path, path)

    second = {item.table_name: item for item in source(NOW + timedelta(seconds=1)).projections}
    assert second["canvas_latest_trade_date"].rows[0]["trade_date"] == "2026-08-03"
    assert second["canvas_hit"].rows == ()
    assert second["screen_run_receipt"].rows[0]["hit_count"] == 0


def test_receipt_source_and_contract_reject_row_overflow(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    seed = _insert_receipt(path, day=TODAY)
    with duckdb.connect(str(path)) as connection:
        connection.executemany(
            "INSERT INTO screen_run_receipt VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    TODAY,
                    f"user/pool-{index}",
                    seed.definition_version,
                    None,
                    None,
                    0,
                    seed.member_digest,
                    True,
                    seed.completed_at,
                    seed.result_version,
                )
                for index in range(512)
            ],
        )
    with pytest.raises(PageProjectionSourceIntegrityError, match="receipt.*bound"):
        _projections(path)

    row = {
        "trade_date": "2026-08-03",
        "preset_name": "n-shape-pool1",
        "definition_version": seed.definition_version,
        "result_version": seed.result_version,
        "parent_trade_date": None,
        "parent_result_version": None,
        "hit_count": 0,
        "member_digest": seed.member_digest,
        "lineage_complete": True,
        "current_definition": True,
        "completed_at": seed.completed_at.isoformat(),
    }
    with pytest.raises(ValueError, match="row budget"):
        ServingProjectionPayload(
            table_name="screen_run_receipt",
            available_at=NOW,
            rows=tuple({**row, "preset_name": f"pool-{index}"} for index in range(513)),
        )


def test_missing_replica_revokes_previous_receipt_authority(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    _insert_receipt(path, day=TODAY)
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(path),
        store=store,
    )
    producer.publish(NOW)
    path.unlink()

    later = NOW + timedelta(seconds=1)
    producer.publish(later)

    latest = store.serving_snapshot(observed_at=later, history_limit=1)
    assert "screen_run_receipt" not in {item.table_name for item in latest.payload.projections}
    assert "pool_membership" not in {item.table_name for item in latest.payload.projections}
