"""A versioned run receipt proves entry prices from one sealed read replica."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from types import MappingProxyType

import duckdb
import pytest

import rquant.serving_read_models as read_models
from rquant.notification_state import (
    NotificationProjectionAuthoritySnapshot,
    NotificationProjectionSourceReceipt,
    NotificationStateStore,
)
from rquant.pool_result_receipt import ScreenRunReceipt, member_price_digest
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    SignalPageProjectionProducer,
)
from rquant.serving_read_models import ServingProjectionPayload
from rquant.storage.schema import SCREEN_RUN_PRICE_RECEIPT_MIGRATION_DDLS
from tests.unit.test_pool_membership_publication import _POOL, _projection, _three_day_history
from tests.unit.test_pool_result_publication import (
    NOW,
    OLD_DAY,
    TODAY,
    _database,
    _insert_receipt,
    _seal_database_before_cutoff,
    _trading_evidence,
)

_LEGACY_RECEIPT_PROJECTION_COLUMNS = (
    "trade_date",
    "preset_name",
    "definition_version",
    "result_version",
    "parent_trade_date",
    "parent_result_version",
    "hit_count",
    "member_digest",
    "lineage_complete",
    "current_definition",
    "completed_at",
)


def _upgrade(path: Path) -> None:
    with duckdb.connect(str(path)) as connection:
        for migration in SCREEN_RUN_PRICE_RECEIPT_MIGRATION_DDLS:
            connection.execute(migration)


def _seal_v2(path: Path, day: date) -> ScreenRunReceipt:
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
            (day, _POOL),
        ).fetchone()
        assert raw is not None
        legacy = ScreenRunReceipt.model_validate(dict(zip(fields, raw, strict=True)))
        prices = connection.execute(
            "SELECT ts_code, close FROM screen_result "
            "WHERE trade_date = ? AND preset_name = ? ORDER BY ts_code",
            (day, _POOL),
        ).fetchall()
        sealed = ScreenRunReceipt.model_validate(
            {
                **legacy.model_dump(mode="python", exclude={"result_version"}),
                "contract": "screen-run-receipt/v2",
                "price_digest": member_price_digest(prices),
            }
        )
        connection.execute(
            "UPDATE screen_run_receipt "
            "SET result_version = ?, contract = ?, price_digest = ? "
            "WHERE trade_date = ? AND preset_name = ?",
            (sealed.result_version, sealed.contract, sealed.price_digest, day, _POOL),
        )
    return sealed


def _entry(path: Path) -> dict[str, object]:
    rows = [row for row in _projection(path)["pool_membership"].rows if row["pool_name"] == _POOL]
    assert rows[0]["status"] == "verified"
    return next(row for row in rows if row["row_kind"] == "member")


def test_new_producer_reads_nonempty_legacy_receipt_authority_before_publishing_v2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    _trading_evidence(path)
    _insert_receipt(path, day=OLD_DAY, members=("600000.SH",))
    source = DuckDBSignalPageProjectionSource(path)
    seed_store = NotificationStateStore(tmp_path / "seed.sqlite3")
    SignalPageProjectionProducer(source=source, store=seed_store).publish(NOW)
    seeded = seed_store.serving_snapshot(observed_at=NOW, history_limit=1)
    receipt_projection = next(
        item for item in seeded.payload.projections if item.table_name == "screen_run_receipt"
    )
    assert len(receipt_projection.rows) == 1
    assert receipt_projection.rows[0]["hit_count"] == 1

    contracts = dict(read_models.PAGE_PROJECTION_CONTRACTS)
    contracts["screen_run_receipt"] = replace(
        contracts["screen_run_receipt"],
        columns=tuple(
            column
            for column in contracts["screen_run_receipt"].columns
            if column[0] in _LEGACY_RECEIPT_PROJECTION_COLUMNS
        ),
    )
    assert tuple(name for name, _kind in contracts["screen_run_receipt"].columns) == (
        _LEGACY_RECEIPT_PROJECTION_COLUMNS
    )
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    with monkeypatch.context() as legacy:
        legacy.setattr(read_models, "PAGE_PROJECTION_CONTRACTS", MappingProxyType(contracts))
        old_receipt = ServingProjectionPayload(
            table_name="screen_run_receipt",
            available_at=receipt_projection.available_at,
            rows=tuple(
                {
                    key: value
                    for key, value in row.items()
                    if key in _LEGACY_RECEIPT_PROJECTION_COLUMNS
                }
                for row in receipt_projection.rows
            ),
        )
        old_projections = tuple(
            old_receipt if item.table_name == "screen_run_receipt" else item
            for item in seeded.payload.projections
        )
        old_source = NotificationProjectionSourceReceipt.create(
            dataset_id="legacy-signal",
            generation_id="1" * 64,
            sequence=1,
            event_time=NOW,
            published_at=NOW,
            projections=old_projections,
        )
        old_authority = NotificationProjectionAuthoritySnapshot.create_from_sources(
            observed_at=NOW,
            sources=(old_source,),
        )
        assert store.publish_projection_authority(old_authority).written
        persisted = store.serving_snapshot(observed_at=NOW, history_limit=1)
        old_row = next(
            item.rows[0]
            for item in persisted.payload.projections
            if item.table_name == "screen_run_receipt"
        )
        assert old_row["hit_count"] == 1
        assert set(old_row) == set(_LEGACY_RECEIPT_PROJECTION_COLUMNS)

    _insert_receipt(path, day=TODAY)
    _upgrade(path)
    sealed = _seal_v2(path, TODAY)
    _seal_database_before_cutoff(path)
    publication = SignalPageProjectionProducer(source=source, store=store).publish(
        NOW + timedelta(seconds=1)
    )

    assert publication.written
    current = store.serving_snapshot(observed_at=NOW + timedelta(seconds=1), history_limit=1)
    row = next(
        item.rows[0]
        for item in current.payload.projections
        if item.table_name == "screen_run_receipt"
    )
    assert row["result_version"] == sealed.result_version
    assert row["hit_count"] == 0


def test_v2_zero_hit_is_a_verified_success(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _database(path)
    _trading_evidence(path)
    _insert_receipt(path, day=TODAY)
    _upgrade(path)
    sealed = _seal_v2(path, TODAY)
    _seal_database_before_cutoff(path)

    projections = _projection(path)

    receipt = projections["screen_run_receipt"].rows[0]
    assert receipt["result_version"] == sealed.result_version
    assert receipt["hit_count"] == 0
    status = next(row for row in projections["pool_membership"].rows if row["pool_name"] == _POOL)
    assert status["status"] == "verified"
    assert status["result_version"] == sealed.result_version


def test_v2_sealed_entry_price_requires_same_day_daily_close(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path)
    _upgrade(path)
    sealed = _seal_v2(path, OLD_DAY)
    _seal_v2(path, TODAY)
    _seal_database_before_cutoff(path)

    entry = _entry(path)

    assert entry["entry_trade_date"] == OLD_DAY.isoformat()
    assert entry["entry_result_version"] == sealed.result_version
    assert entry["entry_close"] == 10.6
    assert entry["unknown_reason"] is None


def test_migrated_v1_receipts_do_not_inherit_a_price_proof(tmp_path: Path) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    entered_version = _three_day_history(path)
    _upgrade(path)
    _seal_database_before_cutoff(path)

    entry = _entry(path)

    assert entry["entry_trade_date"] == OLD_DAY.isoformat()
    assert entry["entry_result_version"] == entered_version
    assert entry["entry_close"] is None
    assert entry["unknown_reason"] == "entry_price_missing"


def test_latest_v2_price_rewrite_keeps_member_receipt_and_historical_entry(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path)
    _upgrade(path)
    _seal_v2(path, OLD_DAY)
    latest = _seal_v2(path, TODAY)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_result SET close = 19.9 WHERE trade_date = ? AND preset_name = ?",
            (TODAY, _POOL),
        )
        connection.execute(
            "UPDATE daily_bar SET close = 19.9 WHERE trade_date = ? AND ts_code = '600000.SH'",
            (TODAY,),
        )
    _seal_database_before_cutoff(path)

    projections = _projection(path)
    receipt = next(
        row for row in projections["screen_run_receipt"].rows if row["preset_name"] == _POOL
    )
    member = next(
        row
        for row in projections["pool_membership"].rows
        if row["pool_name"] == _POOL and row["row_kind"] == "member"
    )

    assert receipt["result_version"] == latest.result_version
    assert member["entry_trade_date"] == OLD_DAY.isoformat()
    assert member["entry_close"] == 10.6


@pytest.mark.parametrize("change", ("member", "result-version"))
def test_v2_continuity_tamper_revokes_entry_day_and_price(tmp_path: Path, change: str) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path)
    _upgrade(path)
    _seal_v2(path, OLD_DAY)
    _seal_v2(path, TODAY)
    with duckdb.connect(str(path)) as connection:
        if change == "member":
            connection.execute(
                "UPDATE screen_result SET ts_code = '600001.SH' "
                "WHERE trade_date = ? AND preset_name = ?",
                (OLD_DAY, _POOL),
            )
        else:
            connection.execute(
                "UPDATE screen_run_receipt SET result_version = ? "
                "WHERE trade_date = ? AND preset_name = ?",
                ("f" * 64, OLD_DAY, _POOL),
            )
    _seal_database_before_cutoff(path)

    entry = _entry(path)

    assert entry["entry_trade_date"] is None
    assert entry["entry_result_version"] is None
    assert entry["entry_close"] is None
    assert entry["unknown_reason"] == "legacy_unproven"


@pytest.mark.parametrize(
    "change",
    ("screen", "daily", "both", "one-bit", "missing-daily"),
)
def test_post_receipt_price_changes_preserve_day_but_revoke_price(
    tmp_path: Path, change: str
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path)
    _upgrade(path)
    sealed = _seal_v2(path, OLD_DAY)
    _seal_v2(path, TODAY)
    with duckdb.connect(str(path)) as connection:
        if change in {"screen", "both", "one-bit"}:
            value = 10.600000000000001 if change == "one-bit" else 19.9
            connection.execute(
                "UPDATE screen_result SET close = ? "
                "WHERE trade_date = ? AND preset_name = ? AND ts_code = '600000.SH'",
                (value, OLD_DAY, _POOL),
            )
        if change in {"daily", "both"}:
            connection.execute(
                "UPDATE daily_bar SET close = 19.9 WHERE trade_date = ? AND ts_code = '600000.SH'",
                (OLD_DAY,),
            )
        if change == "missing-daily":
            connection.execute(
                "DELETE FROM daily_bar WHERE trade_date = ? AND ts_code = '600000.SH'",
                (OLD_DAY,),
            )
    _seal_database_before_cutoff(path)

    entry = _entry(path)

    assert entry["entry_trade_date"] == OLD_DAY.isoformat()
    assert entry["entry_result_version"] == sealed.result_version
    assert entry["entry_close"] is None
    assert entry["unknown_reason"] == "entry_price_missing"


@pytest.mark.parametrize("price", (None, float("inf"), float("nan"), 0.0, -1.0))
def test_sealed_missing_or_invalid_price_keeps_only_entry_day(
    tmp_path: Path, price: float | None
) -> None:
    path = tmp_path / "rquant_ro.duckdb"
    _three_day_history(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_result SET close = ? "
            "WHERE trade_date = ? AND preset_name = ? AND ts_code = '600000.SH'",
            (price, OLD_DAY, _POOL),
        )
        connection.execute(
            "UPDATE daily_bar SET close = ? WHERE trade_date = ? AND ts_code = '600000.SH'",
            (price, OLD_DAY),
        )
    _upgrade(path)
    sealed = _seal_v2(path, OLD_DAY)
    _seal_v2(path, TODAY)
    _seal_database_before_cutoff(path)

    entry = _entry(path)

    assert entry["entry_trade_date"] == OLD_DAY.isoformat()
    assert entry["entry_result_version"] == sealed.result_version
    assert entry["entry_close"] is None
    assert entry["unknown_reason"] == "entry_price_missing"
