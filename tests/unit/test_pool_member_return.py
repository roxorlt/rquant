"""A member's adjusted return needs both sealed prices and same-replica factors."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.notification_state import NotificationStateStore
from rquant.pool_member_return import calculate_adjusted_pool_return
from rquant.pool_result_receipt import ScreenRunReceipt, member_set_digest
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    SignalPageProjectionProducer,
    _return_projection,
)
from rquant.storage.schema import ADJ_FACTOR_DDL
from tests.unit.test_pool_membership_publication import _POOL, _projection, _three_day_history
from tests.unit.test_pool_price_serving import _seal_v2, _upgrade
from tests.unit.test_pool_result_publication import (
    NOW,
    OLD_DAY,
    TODAY,
    _seal_database_before_cutoff,
)


def _sealed_history(path: Path) -> None:
    _three_day_history(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_result SET close = 10 WHERE trade_date = ? AND preset_name = ?",
            (OLD_DAY, _POOL),
        )
        connection.execute("UPDATE daily_bar SET close = 10 WHERE trade_date = ?", (OLD_DAY,))
        connection.execute(
            "UPDATE screen_result SET close = 8 WHERE trade_date = ? AND preset_name = ?",
            (TODAY, _POOL),
        )
        connection.execute(
            "UPDATE daily_bar SET close = 8, vol = 100 WHERE trade_date = ?", (TODAY,)
        )
        connection.execute(ADJ_FACTOR_DDL)
        connection.executemany(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) VALUES (?, ?, ?)",
            [("600000.SH", OLD_DAY, 1.0), ("600000.SH", TODAY, 2.0)],
        )
    _upgrade(path)
    _seal_v2(path, OLD_DAY)
    _seal_v2(path, TODAY)
    _seal_database_before_cutoff(path)


def _return_row(path: Path) -> dict[str, object]:
    rows = _projection(path)["pool_member_return"].rows
    return next(row for row in rows if row["pool_name"] == _POOL)


def _rewrite_as_v1(path: Path, day: date, members: tuple[str, ...]) -> None:
    fields = (
        "trade_date",
        "preset_name",
        "definition_version",
        "parent_trade_date",
        "parent_result_version",
        "hit_count",
        "member_digest",
        "lineage_complete",
        "completed_at",
        "result_version",
        "contract",
        "price_digest",
    )
    with duckdb.connect(str(path)) as connection:
        raw = connection.execute(
            f"SELECT {', '.join(fields)} FROM screen_run_receipt "
            "WHERE trade_date = ? AND preset_name = ?",
            (day, _POOL),
        ).fetchone()
        assert raw is not None
        old = ScreenRunReceipt.model_validate(dict(zip(fields, raw, strict=True)))
        rewritten = ScreenRunReceipt.model_validate(
            {
                **old.model_dump(
                    mode="python", exclude={"result_version", "contract", "price_digest"}
                ),
                "hit_count": len(members),
                "member_digest": member_set_digest(members),
            }
        )
        connection.execute(
            "UPDATE screen_run_receipt SET hit_count = ?, member_digest = ?, "
            "result_version = ?, contract = ?, price_digest = NULL "
            "WHERE trade_date = ? AND preset_name = ?",
            (
                rewritten.hit_count,
                rewritten.member_digest,
                rewritten.result_version,
                rewritten.contract,
                day,
                _POOL,
            ),
        )


def test_pure_adjusted_return_changes_sign_and_line_rule() -> None:
    changed = calculate_adjusted_pool_return(
        entry_close=10.0,
        current_close=8.0,
        entry_factor=1.0,
        current_factor=2.0,
        current_volume=100.0,
    )
    assert changed is not None
    assert changed.gain_pct == pytest.approx(60.0)
    assert changed.entry_line_price is None

    unchanged = calculate_adjusted_pool_return(
        entry_close=10.0,
        current_close=12.0,
        entry_factor=2.0,
        current_factor=2.0,
        current_volume=100.0,
    )
    assert unchanged is not None
    assert unchanged.gain_pct == pytest.approx(20.0)
    assert unchanged.entry_line_price == 10.0

    same_day = calculate_adjusted_pool_return(
        entry_close=8.0,
        current_close=8.0,
        entry_factor=1.7,
        current_factor=1.7,
        current_volume=100.0,
    )
    assert same_day is not None
    assert same_day.gain_pct == 0.0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("entry_factor", None),
        ("current_factor", 0.0),
        ("current_factor", float("inf")),
        ("current_close", None),
        ("current_volume", 0.0),
    ],
)
def test_pure_adjusted_return_fail_closed(field: str, value: float | None) -> None:
    inputs: dict[str, float | None] = {
        "entry_close": 10.0,
        "current_close": 8.0,
        "entry_factor": 1.0,
        "current_factor": 2.0,
        "current_volume": 100.0,
    }
    inputs[field] = value
    assert calculate_adjusted_pool_return(**inputs) is None


def test_current_v2_receipt_and_factors_publish_adjusted_return(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _sealed_history(path)

    row = _return_row(path)
    assert row["trade_date"] == TODAY.isoformat()
    assert row["entry_trade_date"] == OLD_DAY.isoformat()
    assert row["ts_code"] == "600000.SH"
    assert row["gain_pct"] == pytest.approx(60.0)
    assert row["entry_line_price"] is None


def test_newer_daily_bar_does_not_extend_right_endpoint_without_a_run(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _sealed_history(path)
    later = date(2026, 8, 4)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close, vol) VALUES (?, ?, ?, ?)",
            ("600000.SH", later, 20.0, 100.0),
        )
        connection.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) VALUES (?, ?, ?)",
            ("600000.SH", later, 4.0),
        )
    _seal_database_before_cutoff(path)

    source = DuckDBSignalPageProjectionSource(path)
    projections = {item.table_name: item for item in source(NOW + timedelta(days=1)).projections}
    row = projections["pool_member_return"].rows[0]
    assert row["trade_date"] == TODAY.isoformat()
    assert row["gain_pct"] == pytest.approx(60.0)


def test_same_day_entry_publishes_zero_and_raw_price_line(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _sealed_history(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute("DELETE FROM screen_result WHERE trade_date = ?", (OLD_DAY,))
        connection.execute("UPDATE adj_factor SET adj_factor = 1 WHERE trade_date = ?", (TODAY,))
    _rewrite_as_v1(path, OLD_DAY, ())
    _seal_v2(path, OLD_DAY)
    _seal_database_before_cutoff(path)

    row = _return_row(path)
    assert row["entry_trade_date"] == TODAY.isoformat()
    assert row["gain_pct"] == 0.0
    assert row["entry_line_price"] == 8.0


@pytest.mark.parametrize(
    "change",
    (
        "missing-entry-factor",
        "missing-current-factor",
        "zero-volume",
        "current-double-rewrite",
        "current-v1",
        "current-missing-close",
    ),
)
def test_missing_or_rewritten_right_evidence_only_hides_return(tmp_path: Path, change: str) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _sealed_history(path)
    with duckdb.connect(str(path)) as connection:
        if change == "missing-entry-factor":
            connection.execute("DELETE FROM adj_factor WHERE trade_date = ?", (OLD_DAY,))
        elif change == "missing-current-factor":
            connection.execute("DELETE FROM adj_factor WHERE trade_date = ?", (TODAY,))
        elif change == "zero-volume":
            connection.execute("UPDATE daily_bar SET vol = 0 WHERE trade_date = ?", (TODAY,))
        elif change == "current-double-rewrite":
            connection.execute("UPDATE screen_result SET close = 9 WHERE trade_date = ?", (TODAY,))
            connection.execute("UPDATE daily_bar SET close = 9 WHERE trade_date = ?", (TODAY,))
        elif change == "current-missing-close":
            connection.execute("UPDATE daily_bar SET close = NULL WHERE trade_date = ?", (TODAY,))
        else:
            assert change == "current-v1"
    if change == "current-v1":
        _rewrite_as_v1(path, TODAY, ("600000.SH",))
    _seal_database_before_cutoff(path)

    projections = _projection(path)
    membership = next(
        row
        for row in projections["pool_membership"].rows
        if row["pool_name"] == _POOL and row["row_kind"] == "member"
    )
    assert membership["entry_trade_date"] == OLD_DAY.isoformat()
    assert membership["entry_close"] == 10.0
    assert projections["pool_member_return"].rows == ()


def test_duplicate_factor_rows_hide_return_without_revoking_entry(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _sealed_history(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute("DROP TABLE adj_factor")
        connection.execute(
            "CREATE TABLE adj_factor (ts_code VARCHAR, trade_date DATE, adj_factor DOUBLE)"
        )
        connection.executemany(
            "INSERT INTO adj_factor VALUES ('600000.SH', ?, ?)",
            [(OLD_DAY, 1.0), (TODAY, 2.0), (TODAY, 3.0)],
        )
    _seal_database_before_cutoff(path)

    projections = _projection(path)
    member = next(
        row
        for row in projections["pool_membership"].rows
        if row["pool_name"] == _POOL and row["row_kind"] == "member"
    )
    assert member["entry_trade_date"] == OLD_DAY.isoformat()
    assert member["entry_close"] == 10.0
    assert projections["pool_member_return"].rows == ()


def test_preclose_v2_run_is_transient_even_when_replica_is_read_after_close(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _sealed_history(path)
    with duckdb.connect(str(path)) as connection:
        fields = (
            "trade_date",
            "preset_name",
            "definition_version",
            "parent_trade_date",
            "parent_result_version",
            "hit_count",
            "member_digest",
            "lineage_complete",
            "completed_at",
            "result_version",
            "contract",
            "price_digest",
        )
        raw = connection.execute(
            f"SELECT {', '.join(fields)} FROM screen_run_receipt "
            "WHERE trade_date = ? AND preset_name = ?",
            (TODAY, _POOL),
        ).fetchone()
        assert raw is not None
        old = ScreenRunReceipt.model_validate(dict(zip(fields, raw, strict=True)))
        early = datetime(2026, 8, 3, 6, 59, tzinfo=UTC)
        revised = ScreenRunReceipt.model_validate(
            {**old.model_dump(mode="python", exclude={"result_version"}), "completed_at": early}
        )
        connection.execute(
            "UPDATE screen_run_receipt SET completed_at = ?, result_version = ? "
            "WHERE trade_date = ? AND preset_name = ?",
            (early, revised.result_version, TODAY, _POOL),
        )
    _seal_database_before_cutoff(path)

    projections = _projection(path)
    member = next(
        row
        for row in projections["pool_membership"].rows
        if row["pool_name"] == _POOL and row["row_kind"] == "member"
    )
    assert member["entry_close"] == 10.0
    assert projections["pool_member_return"].rows == ()


def test_return_projection_rejects_membership_from_previous_result_version(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _sealed_history(path)
    old_membership = _projection(path)["pool_membership"]
    old_version = next(
        row["result_version"]
        for row in old_membership.rows
        if row["pool_name"] == _POOL and row["row_kind"] == "status"
    )
    with duckdb.connect(str(path)) as connection:
        connection.execute("UPDATE screen_result SET close = 9 WHERE trade_date = ?", (TODAY,))
        connection.execute("UPDATE daily_bar SET close = 9 WHERE trade_date = ?", (TODAY,))
    _seal_v2(path, TODAY)
    _seal_database_before_cutoff(path)

    source = DuckDBSignalPageProjectionSource(path)
    database = source._read_database_projection(datetime(2026, 8, 3, 16), observed=NOW)
    assert database.run_receipts is not None
    assert (
        next(
            receipt.result_version
            for receipt in database.run_receipts.latest
            if receipt.preset_name == _POOL
        )
        != old_version
    )
    mismatched = _return_projection(
        database.membership,
        membership=old_membership,
        receipts=database.run_receipts,
        observed=NOW,
    )
    assert mismatched is not None
    assert mismatched.rows == ()


def test_old_nonempty_authority_without_return_projection_upgrades(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _sealed_history(path)
    store = NotificationStateStore(tmp_path / "state.sqlite3")
    source = DuckDBSignalPageProjectionSource(path)
    first = SignalPageProjectionProducer(source=source, store=store).publish(NOW)
    assert first.written
    before = store.serving_snapshot(observed_at=NOW, history_limit=1)
    assert any(
        item.table_name == "screen_run_receipt" and item.rows for item in before.payload.projections
    )

    # Simulate the previous authority's optional table set without changing its receipts.
    from rquant.notification_state import (
        NotificationProjectionAuthoritySnapshot,
        NotificationProjectionSourceReceipt,
    )

    old_projections = tuple(
        item for item in before.payload.projections if item.table_name != "pool_member_return"
    )
    old_source = NotificationProjectionSourceReceipt.create(
        dataset_id="old-page",
        generation_id="1" * 64,
        sequence=1,
        event_time=NOW,
        published_at=NOW,
        projections=old_projections,
    )
    old_authority = NotificationProjectionAuthoritySnapshot.create_from_sources(
        observed_at=NOW, sources=(old_source,)
    )
    old_store = NotificationStateStore(tmp_path / "old.sqlite3")
    assert old_store.publish_projection_authority(old_authority).written

    published = SignalPageProjectionProducer(source=source, store=old_store).publish(
        NOW + timedelta(seconds=1)
    )
    assert published.written
    after = old_store.serving_snapshot(observed_at=NOW + timedelta(seconds=1), history_limit=1)
    assert any(item.table_name == "pool_member_return" for item in after.payload.projections)


def test_source_failure_does_not_republish_stale_return(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _sealed_history(path)
    store = NotificationStateStore(tmp_path / "state.sqlite3")
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(path), store=store
    )
    assert producer.publish(NOW).written
    initial = store.serving_snapshot(observed_at=NOW, history_limit=1)
    assert any(
        item.table_name == "pool_member_return" and item.rows
        for item in initial.payload.projections
    )

    path.unlink()
    assert producer.publish(NOW + timedelta(seconds=1)).written
    after = store.serving_snapshot(observed_at=NOW + timedelta(seconds=1), history_limit=1)
    assert all(item.table_name != "pool_member_return" for item in after.payload.projections)
