"""Immutable price proof for one materialized pool run."""

import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.pool_result_receipt import (
    ScreenRunReceipt,
    ScreenRunReceiptDraft,
    member_set_digest,
)
from rquant.research_sync import sync_from_backup
from rquant.runtime_contracts import canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.storage.migrations import MIGRATIONS, initialize_schema
from rquant.trade_calendar import TradeCalendarDay

DAY = date(2026, 8, 4)
STAMP = datetime(2026, 8, 4, 9, 0, tzinfo=UTC)
DEFINITION = "a" * 64
V2 = "screen-run-receipt/v2"


def _frame(prices: list[tuple[str, float | None]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": DAY.isoformat(),
                "preset_name": "pool",
                "ts_code": code,
                "name": code,
                "close": close,
                "pct_chg": None,
                "extra": None,
            }
            for code, close in prices
        ],
        columns=("trade_date", "preset_name", "ts_code", "name", "close", "pct_chg", "extra"),
    )


def _v2_receipt(prices: list[tuple[str, float | None]]) -> ScreenRunReceipt:
    from rquant.pool_result_receipt import member_price_digest

    return ScreenRunReceipt(
        contract=V2,
        trade_date=DAY,
        preset_name="pool",
        definition_version=DEFINITION,
        hit_count=len(prices),
        member_digest=member_set_digest([code for code, _ in prices]),
        price_digest=member_price_digest(prices),
        lineage_complete=True,
        completed_at=STAMP,
    )


def _v2_draft(prices: list[tuple[str, float | None]]) -> ScreenRunReceiptDraft:
    return ScreenRunReceiptDraft(
        trade_date=DAY,
        preset_name="pool",
        definition_version=DEFINITION,
        hit_count=len(prices),
        member_digest=member_set_digest([code for code, _ in prices]),
        lineage_complete=True,
        completed_at=STAMP,
    )


def _v1_receipt() -> ScreenRunReceipt:
    return ScreenRunReceipt(
        trade_date=DAY,
        preset_name="pool",
        definition_version=DEFINITION,
        hit_count=1,
        member_digest=member_set_digest(["A"]),
        lineage_complete=True,
        completed_at=STAMP,
    )


def _insert_v1(connection: duckdb.DuckDBPyConnection) -> ScreenRunReceipt:
    receipt = _v1_receipt()
    connection.execute(
        """
        INSERT INTO screen_run_receipt (
            trade_date, preset_name, definition_version, parent_trade_date,
            parent_result_version, hit_count, member_digest, lineage_complete,
            completed_at, result_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def test_price_digest_sorts_codes_and_normalizes_finite_and_null() -> None:
    from rquant.pool_result_receipt import member_price_digest

    prices = [("B", 10.5), ("A", None)]
    digest = member_price_digest(prices)

    assert digest == "81e2e6f1c0daf10f9ccf8975cea9ee40aa1662a76cbf55be8619d4beb8d0929f"
    assert member_price_digest([]) == (
        "46e45f1e5983fb9f1a91d6f45ba7a73f3459f663b283d2c7b410f34e2ba8d387"
    )
    assert digest == member_price_digest(list(reversed(prices)))
    assert digest != member_price_digest([("A", float("nan")), ("B", 10.5)])
    assert digest != member_price_digest([("A", float("inf")), ("B", 10.5)])
    assert digest != member_price_digest([("A", None), ("B", 10.6)])
    assert member_price_digest([("A", -0.0)]) != member_price_digest([("A", 0.0)])
    assert member_price_digest([]) != member_price_digest([("A", None)])
    assert member_price_digest([("A", 10.5)]) != member_price_digest(
        [("A", math.nextafter(10.5, math.inf))]
    )
    with pytest.raises(ValueError, match="unique"):
        member_price_digest([("A", 1.0), ("A", 2.0)])


def test_v2_result_version_binds_price_and_v1_hash_stays_unchanged() -> None:
    old = _v1_receipt()
    first = _v2_receipt([("A", 10.5)])
    changed = _v2_receipt([("A", 10.6)])

    assert old.contract == "screen-run-receipt/v1"
    assert old.price_digest is None
    assert old.result_version == canonical_sha256(
        old.model_dump(mode="python", exclude={"result_version", "price_digest"})
    )
    assert first.member_digest == changed.member_digest
    assert first.price_digest != changed.price_digest
    assert first.result_version != changed.result_version
    assert first.result_version != old.result_version
    with pytest.raises(ValidationError, match="result version"):
        ScreenRunReceipt.model_validate(
            {**first.model_dump(mode="python"), "price_digest": "f" * 64}
        )
    with pytest.raises(ValidationError, match="price_digest"):
        ScreenRunReceipt(
            contract=V2,
            trade_date=DAY,
            preset_name="pool",
            definition_version=DEFINITION,
            hit_count=0,
            member_digest=member_set_digest([]),
            lineage_complete=True,
            completed_at=STAMP,
        )


def test_atomic_same_day_price_change_zero_hit_and_rollback(tmp_path: Path) -> None:
    with DuckDBStore(tmp_path / "price.duckdb") as store:
        first_frame = _frame([("A", 10.5)])
        store.replace_screen_result_with_receipt(
            DAY.isoformat(), "pool", first_frame, _v2_draft([("A", 10.5)])
        )
        first = store.query_screen_run_receipt(DAY.isoformat(), "pool")
        assert first is not None and first.contract == V2

        changed_frame = _frame([("A", 10.6)])
        store.replace_screen_result_with_receipt(
            DAY.isoformat(), "pool", changed_frame, _v2_draft([("A", 10.6)])
        )
        changed = store.query_screen_run_receipt(DAY.isoformat(), "pool")
        assert changed is not None and changed.result_version != first.result_version
        assert changed.price_digest != first.price_digest
        assert store.query_screen_result(DAY.isoformat(), "pool")["close"].tolist() == [10.6]
        assert store.query_screen_run_receipt(DAY.isoformat(), "pool") == changed

        with (
            patch.object(store, "_upsert_screen_run_receipt", side_effect=RuntimeError("failed")),
            pytest.raises(RuntimeError, match="failed"),
        ):
            store.replace_screen_result_with_receipt(
                DAY.isoformat(),
                "pool",
                _frame([("A", 20.0)]),
                _v2_draft([("A", 20.0)]),
            )
        assert store.query_screen_result(DAY.isoformat(), "pool")["close"].tolist() == [10.6]
        assert store.query_screen_run_receipt(DAY.isoformat(), "pool") == changed

        store.replace_screen_result_with_receipt(DAY.isoformat(), "pool", _frame([]), _v2_draft([]))
        zero = store.query_screen_run_receipt(DAY.isoformat(), "pool")
        assert store.query_screen_result(DAY.isoformat(), "pool").empty
        assert zero is not None and zero.hit_count == 0 and zero.price_digest is not None


def test_nonfinite_close_participates_in_digest_and_is_not_a_null(tmp_path: Path) -> None:
    prices = [("A", float("inf")), ("B", None), ("C", float("nan"))]
    with DuckDBStore(tmp_path / "nonfinite.duckdb") as store:
        store.replace_screen_result_with_receipt(
            DAY.isoformat(), "pool", _frame(prices), _v2_draft(prices)
        )
        persisted = store._conn.execute(
            "SELECT ts_code, close FROM screen_result ORDER BY ts_code"
        ).fetchall()
        receipt = store.query_screen_run_receipt(DAY.isoformat(), "pool")
        assert persisted == [("A", float("inf")), ("B", None), ("C", None)]
        assert receipt is not None
        from rquant.pool_result_receipt import member_price_digest

        assert receipt.price_digest == member_price_digest(persisted)
        assert receipt.price_digest != member_price_digest([("A", None), ("B", None), ("C", None)])


def test_v2_seals_readback_even_if_input_price_changes_during_persistence(
    tmp_path: Path,
) -> None:
    from rquant.pool_result_receipt import member_price_digest

    with DuckDBStore(tmp_path / "readback.duckdb") as store:
        real_replace = store.replace_screen_result

        def persist_changed(trade_date: str, preset_name: str, frame: pd.DataFrame) -> int:
            actual = frame.copy()
            actual.loc[actual["ts_code"] == "A", "close"] = 10.6
            return real_replace(trade_date, preset_name, actual)

        with patch.object(store, "replace_screen_result", side_effect=persist_changed):
            store.replace_screen_result_with_receipt(
                DAY.isoformat(),
                "pool",
                _frame([("A", 10.5)]),
                _v2_draft([("A", 10.5)]),
            )
        sealed = store.query_screen_run_receipt(DAY.isoformat(), "pool")

    assert sealed is not None
    assert sealed.price_digest == member_price_digest([("A", 10.6)])
    assert sealed.price_digest != member_price_digest([("A", 10.5)])


def test_store_refuses_presealed_v2_receipt(tmp_path: Path) -> None:
    with (
        DuckDBStore(tmp_path / "presealed.duckdb") as store,
        pytest.raises(ValueError, match="draft|persisted"),
    ):
        store.replace_screen_result_with_receipt(
            DAY.isoformat(),
            "pool",
            _frame([("A", 10.5)]),
            _v2_receipt([("A", 10.5)]),
        )


def test_v11_receipt_upgrades_without_acquiring_price_proof(tmp_path: Path) -> None:
    path = tmp_path / "legacy.duckdb"
    with duckdb.connect(str(path)) as connection:
        initialize_schema(connection, migrations=MIGRATIONS[:11])
        old = _insert_v1(connection)
    with DuckDBStore(path, read_only=True) as store:
        assert store.query_screen_run_receipt(DAY.isoformat(), "pool") == old
    with duckdb.connect(str(path)) as connection:
        initialize_schema(connection)
        assert connection.execute(
            "SELECT contract, price_digest FROM screen_run_receipt"
        ).fetchone() == ("screen-run-receipt/v1", None)
        assert connection.execute(
            "SELECT version FROM schema_migration ORDER BY version"
        ).fetchall() == [(migration.version,) for migration in MIGRATIONS]
    with DuckDBStore(path, read_only=True) as store:
        assert store.query_screen_run_receipt(DAY.isoformat(), "pool") == old


def test_research_sync_from_v11_downgrades_price_proof(tmp_path: Path) -> None:
    source = tmp_path / "v11.duckdb"
    local = tmp_path / "v12.duckdb"
    with duckdb.connect(str(source)) as connection:
        initialize_schema(connection, migrations=MIGRATIONS[:11])
        connection.execute(
            "INSERT INTO screen_result (trade_date, preset_name, ts_code, close) "
            "VALUES (?, 'pool', 'A', 11.0)",
            [DAY],
        )
        old = _insert_v1(connection)
    with DuckDBStore(local) as store:
        store.replace_screen_result_with_receipt(
            DAY.isoformat(), "pool", _frame([("A", 20.0)]), _v2_draft([("A", 20.0)])
        )

    report = sync_from_backup(source, local, refresh_replica=False)

    assert not report.has_errors
    with DuckDBStore(local, read_only=True) as store:
        assert store.query_screen_result(DAY.isoformat(), "pool")["close"].tolist() == [11.0]
        assert store.query_screen_run_receipt(DAY.isoformat(), "pool") == old


def test_research_sync_refuses_partial_v2_schema_without_losing_local_proof(
    tmp_path: Path,
) -> None:
    source = tmp_path / "partial.duckdb"
    local = tmp_path / "local.duckdb"
    for path, close in ((source, 11.0), (local, 20.0)):
        with DuckDBStore(path) as store:
            store.replace_screen_result_with_receipt(
                DAY.isoformat(), "pool", _frame([("A", close)]), _v2_draft([("A", close)])
            )
    with duckdb.connect(str(source)) as connection:
        connection.execute("ALTER TABLE screen_run_receipt DROP COLUMN price_digest")

    report = sync_from_backup(source, local, refresh_replica=False)

    assert report.has_errors
    with DuckDBStore(local, read_only=True) as store:
        assert store.query_screen_result(DAY.isoformat(), "pool")["close"].tolist() == [20.0]
        receipt = store.query_screen_run_receipt(DAY.isoformat(), "pool")
        assert receipt is not None and receipt.contract == V2
        assert receipt.price_digest == _v2_receipt([("A", 20.0)]).price_digest


def test_pipeline_writes_v2_price_receipt_for_same_members_with_new_close(
    tmp_path: Path,
) -> None:
    from rquant.pipeline import run_daily_screen_stage

    path = tmp_path / "pipeline.duckdb"
    with DuckDBStore(path) as store:
        previous_day = DAY - timedelta(days=1)
        store.upsert_trade_calendar(
            [
                TradeCalendarDay(exchange="SSE", cal_date=previous_day, is_open=True),
                TradeCalendarDay(exchange="SSE", cal_date=DAY, is_open=True),
            ]
        )
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES ('A', ?, 10.5)",
            [previous_day],
        )
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES ('A', ?, 10.5)",
            [DAY],
        )

        def screen_result(close: float) -> pd.DataFrame:
            return pd.DataFrame(
                {"ts_code": ["A"], "name": ["A"], "CLOSE[0]": [close], "PCT_CHG[0]": [0.0]}
            )

        with patch("rquant.pipeline.screen", return_value=screen_result(10.5)):
            run_daily_screen_stage(
                DAY.isoformat(),
                preset_names=["n-shape-pool1"],
                store=store,
                preset_directory=tmp_path / "empty-defs",
            )
        first = store.query_screen_run_receipt(DAY.isoformat(), "n-shape-pool1")
        with patch("rquant.pipeline.screen", return_value=screen_result(10.6)):
            run_daily_screen_stage(
                DAY.isoformat(),
                preset_names=["n-shape-pool1"],
                store=store,
                preset_directory=tmp_path / "empty-defs",
            )
        second = store.query_screen_run_receipt(DAY.isoformat(), "n-shape-pool1")

    assert first is not None and second is not None
    assert first.contract == second.contract == V2
    assert first.member_digest == second.member_digest
    assert first.price_digest != second.price_digest
    assert first.result_version != second.result_version
