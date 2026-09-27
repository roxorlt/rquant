"""A versioned run receipt proves entry prices from one sealed read replica."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from rquant.pool_result_receipt import ScreenRunReceipt, member_price_digest
from rquant.storage.schema import SCREEN_RUN_PRICE_RECEIPT_MIGRATION_DDLS
from tests.unit.test_pool_membership_publication import _POOL, _projection, _three_day_history
from tests.unit.test_pool_result_publication import (
    OLD_DAY,
    TODAY,
    _database,
    _insert_receipt,
    _seal_database_before_cutoff,
    _trading_evidence,
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
    assert receipt["contract"] == "screen-run-receipt/v2"
    assert receipt["price_digest_verified"] is True
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


def test_latest_v2_price_rewrite_keeps_member_receipt_without_price_proof(tmp_path: Path) -> None:
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
    assert receipt["price_digest_verified"] is False
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
