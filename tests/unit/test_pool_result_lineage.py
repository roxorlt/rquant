"""Pool run evidence must identify the compiled definition and exact parent result."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

import rquant.research_sync as research_sync
from rquant.pipeline import run_daily_screen_stage
from rquant.pool_definition_projection import build_pool_definition_rows
from rquant.research_sync import sync_from_backup
from rquant.runtime_contracts import canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.trade_calendar import TradeCalendarDay


def _definition(directory: Path, name: str, *, parent: str | None = None) -> str:
    directory.mkdir(parents=True, exist_ok=True)
    raw = {
        "schema_version": 2,
        "name": name,
        "display_name": name,
        "description": "test",
        "rules": [{"name": "not_st", "args": {}}],
        "include_columns": ["CLOSE[0]"],
        "depends_on": parent,
        "delay_days": 1 if parent else 0,
    }
    (directory / f"{name}.json").write_text(json.dumps(raw), encoding="utf-8")
    return canonical_sha256(raw)


def _market(store: DuckDBStore, days: tuple[date, ...]) -> None:
    start, end = min(days), max(days)
    store.upsert_trade_calendar(
        [
            TradeCalendarDay(
                exchange="SSE",
                cal_date=start + timedelta(days=n),
                is_open=(start + timedelta(days=n)).weekday() < 5,
            )
            for n in range((end - start).days + 1)
        ]
    )
    for day in days:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES ('X', ?, 1)",
            [day],
        )


def _frame(*codes: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": list(codes),
            "name": list(codes),
            "CLOSE[0]": [1.0] * len(codes),
            "PCT_CHG[0]": [0.0] * len(codes),
        }
    )


def _receipt(store: DuckDBStore, day: str, pool: str) -> tuple[object, ...] | None:
    return store._conn.execute(
        "SELECT definition_version, parent_trade_date, parent_result_version, "
        "hit_count, member_digest, result_version "
        "FROM screen_run_receipt WHERE trade_date = ? AND preset_name = ?",
        [day, pool],
    ).fetchone()


def test_same_day_rerun_replaces_members_and_receipt_even_when_zero(tmp_path: Path) -> None:
    directory = tmp_path / "defs"
    version = _definition(directory, "pool")
    with DuckDBStore(tmp_path / "db.duckdb") as store:
        _market(store, (date(2026, 8, 4),))
        with patch("rquant.pipeline.screen", side_effect=(_frame("B", "A"), _frame())):
            first = run_daily_screen_stage(
                "2026-08-04", preset_names=["user/pool"], store=store,
                preset_directory=directory,
            )
            first_receipt = _receipt(store, "2026-08-04", "user/pool")
            second = run_daily_screen_stage(
                "2026-08-04", preset_names=["user/pool"], store=store,
                preset_directory=directory,
            )
        second_receipt = _receipt(store, "2026-08-04", "user/pool")
        assert first.preset_hits == {"user/pool": 2}
        assert second.preset_hits == {"user/pool": 0}
        assert store.query_screen_result("2026-08-04", "user/pool").empty
        assert first_receipt is not None and second_receipt is not None
        assert first_receipt[0] == second_receipt[0] == version
        assert first_receipt[3] == 2 and second_receipt[3] == 0
        assert first_receipt[4] == canonical_sha256(["A", "B"])
        assert second_receipt[4] == canonical_sha256([])
        assert first_receipt[5] != second_receipt[5]


def test_builtin_run_version_matches_publication_definition(tmp_path: Path) -> None:
    with DuckDBStore(tmp_path / "db.duckdb") as store:
        _market(store, (date(2026, 8, 4),))
        with patch("rquant.pipeline.screen", return_value=_frame("X")):
            result = run_daily_screen_stage(
                "2026-08-04", preset_names=["n-shape-pool1"], store=store,
                preset_directory=tmp_path / "empty-defs",
            )
        receipt = _receipt(store, "2026-08-04", "n-shape-pool1")
    published = {
        row["pool_name"]: row for row in build_pool_definition_rows({}, {}, root_path="/unused")
    }
    assert result.preset_hits == {"n-shape-pool1": 1}
    assert receipt is not None
    assert receipt[0] == published["n-shape-pool1"]["version"]


def test_exact_child_requires_parent_receipt_and_accepts_verified_zero(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "defs"
    _definition(directory, "parent")
    _definition(directory, "child", parent="user/parent")
    with DuckDBStore(tmp_path / "db.duckdb") as store:
        _market(store, (date(2026, 8, 3), date(2026, 8, 4)))
        with patch("rquant.pipeline.screen") as screened:
            missing = run_daily_screen_stage(
                "2026-08-04", preset_names=["user/child"], store=store,
                preset_directory=directory,
            )
        screened.assert_not_called()
        assert missing.preset_hits == {"user/child": -1}
        assert _receipt(store, "2026-08-04", "user/child") is None

        with patch("rquant.pipeline.screen", return_value=_frame()):
            parent = run_daily_screen_stage(
                "2026-08-03", preset_names=["user/parent"], store=store,
                preset_directory=directory,
            )
        assert parent.preset_hits == {"user/parent": 0}
        parent_receipt = _receipt(store, "2026-08-03", "user/parent")
        assert parent_receipt is not None and parent_receipt[3] == 0
        with patch("rquant.pipeline.screen") as screened:
            child = run_daily_screen_stage(
                "2026-08-04", preset_names=["user/child"], store=store,
                preset_directory=directory,
            )
        screened.assert_not_called()
        child_receipt = _receipt(store, "2026-08-04", "user/child")
        assert child.preset_hits == {"user/child": 0}
        assert child_receipt is not None
        assert child_receipt[1] == date(2026, 8, 3)
        assert child_receipt[2] == parent_receipt[5]


def test_exact_child_rejects_changed_parent_definition_and_tampered_members(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "defs"
    _definition(directory, "parent")
    _definition(directory, "child", parent="user/parent")
    with DuckDBStore(tmp_path / "db.duckdb") as store:
        _market(store, (date(2026, 8, 3), date(2026, 8, 4)))
        with patch("rquant.pipeline.screen", return_value=_frame("X")):
            run_daily_screen_stage(
                "2026-08-03", preset_names=["user/parent"], store=store,
                preset_directory=directory,
            )
        store._conn.execute(
            "UPDATE screen_result SET ts_code = 'Y' "
            "WHERE trade_date = '2026-08-03' AND preset_name = 'user/parent'"
        )
        with patch("rquant.pipeline.screen") as screened:
            tampered = run_daily_screen_stage(
                "2026-08-04", preset_names=["user/child"], store=store,
                preset_directory=directory,
            )
        screened.assert_not_called()
        assert tampered.preset_hits == {"user/child": -1}

        store._conn.execute(
            "UPDATE screen_result SET ts_code = 'X' "
            "WHERE trade_date = '2026-08-03' AND preset_name = 'user/parent'"
        )
        raw = json.loads((directory / "parent.json").read_text(encoding="utf-8"))
        raw["description"] = "saved again"
        (directory / "parent.json").write_text(json.dumps(raw), encoding="utf-8")
        with patch("rquant.pipeline.screen") as screened:
            stale = run_daily_screen_stage(
                "2026-08-04", preset_names=["user/child"], store=store,
                preset_directory=directory,
            )
        screened.assert_not_called()
        assert stale.preset_hits == {"user/child": -1}
        assert _receipt(store, "2026-08-04", "user/child") is None


def test_legacy_window_zero_still_has_receipt_without_full_lineage(tmp_path: Path) -> None:
    directory = tmp_path / "defs"
    _definition(directory, "legacy", parent="n-shape-pool1")
    path = directory / "legacy.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw.pop("delay_days")
    raw["offset_days"] = 1
    path.write_text(json.dumps(raw), encoding="utf-8")
    with DuckDBStore(tmp_path / "db.duckdb") as store:
        _market(store, (date(2026, 8, 3), date(2026, 8, 4)))
        with patch("rquant.pipeline.screen") as screened:
            result = run_daily_screen_stage(
                "2026-08-04", preset_names=["user/legacy"], store=store,
                preset_directory=directory,
            )
        screened.assert_not_called()
        receipt = _receipt(store, "2026-08-04", "user/legacy")
        assert result.preset_hits == {"user/legacy": 0}
        assert receipt is not None and receipt[3] == 0
        assert store._conn.execute(
            "SELECT lineage_complete FROM screen_run_receipt "
            "WHERE trade_date = '2026-08-04' AND preset_name = 'user/legacy'"
        ).fetchone() == (False,)


def test_parent_rerun_changes_child_bound_result_version(tmp_path: Path) -> None:
    directory = tmp_path / "defs"
    _definition(directory, "parent")
    _definition(directory, "child", parent="user/parent")
    with DuckDBStore(tmp_path / "db.duckdb") as store:
        _market(store, (date(2026, 8, 3), date(2026, 8, 4)))
        with patch("rquant.pipeline.screen", return_value=_frame("A")):
            run_daily_screen_stage(
                "2026-08-03", preset_names=["user/parent"], store=store,
                preset_directory=directory,
            )
        with patch("rquant.pipeline.screen", return_value=_frame("A")):
            run_daily_screen_stage(
                "2026-08-04", preset_names=["user/child"], store=store,
                preset_directory=directory,
            )
        first = _receipt(store, "2026-08-04", "user/child")
        with patch("rquant.pipeline.screen", return_value=_frame("B")):
            run_daily_screen_stage(
                "2026-08-03", preset_names=["user/parent"], store=store,
                preset_directory=directory,
            )
        parent = _receipt(store, "2026-08-03", "user/parent")
        assert first is not None and parent is not None
        assert first[2] != parent[5]
        with patch("rquant.pipeline.screen", return_value=_frame("B")):
            result = run_daily_screen_stage(
                "2026-08-04", preset_names=["user/child"], store=store,
                preset_directory=directory,
            )
        second = _receipt(store, "2026-08-04", "user/child")
        assert result.preset_hits == {"user/child": 1}
        assert second is not None and second[2] == parent[5]
        assert second[5] != first[5]


def test_run_binds_definition_compiled_before_concurrent_save(tmp_path: Path) -> None:
    directory = tmp_path / "defs"
    compiled_version = _definition(directory, "pool")
    with DuckDBStore(tmp_path / "db.duckdb") as store:
        _market(store, (date(2026, 8, 4),))

        def save_while_running(**_: object) -> pd.DataFrame:
            raw = json.loads((directory / "pool.json").read_text(encoding="utf-8"))
            raw["description"] = "new saved version"
            (directory / "pool.json").write_text(json.dumps(raw), encoding="utf-8")
            return _frame("X")

        with patch("rquant.pipeline.screen", side_effect=save_while_running):
            result = run_daily_screen_stage(
                "2026-08-04", preset_names=["user/pool"], store=store,
                preset_directory=directory,
            )
        receipt = _receipt(store, "2026-08-04", "user/pool")
        assert result.preset_hits == {"user/pool": 1}
        assert receipt is not None and receipt[0] == compiled_version
        assert canonical_sha256(json.loads((directory / "pool.json").read_text())) != receipt[0]


def test_snapshot_and_receipt_roll_back_if_receipt_insert_fails(tmp_path: Path) -> None:
    directory = tmp_path / "defs"
    _definition(directory, "pool")
    with DuckDBStore(tmp_path / "db.duckdb") as store:
        _market(store, (date(2026, 8, 4),))
        with patch("rquant.pipeline.screen", return_value=_frame("OLD")):
            run_daily_screen_stage(
                "2026-08-04", preset_names=["user/pool"], store=store,
                preset_directory=directory,
            )
        old_receipt = _receipt(store, "2026-08-04", "user/pool")
        with (
            patch("rquant.pipeline.screen", return_value=_frame("NEW")),
            patch.object(
                store, "_upsert_screen_run_receipt", side_effect=RuntimeError("insert failed")
            ),
        ):
            failed = run_daily_screen_stage(
                "2026-08-04", preset_names=["user/pool"], store=store,
                preset_directory=directory,
            )
        assert failed.preset_hits == {"user/pool": -1}
        assert store.query_screen_result("2026-08-04", "user/pool")["ts_code"].tolist() == [
            "OLD"
        ]
        assert _receipt(store, "2026-08-04", "user/pool") == old_receipt


def test_enclosing_daily_transaction_rolls_back_failed_receipt_write(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "defs"
    _definition(directory, "pool")
    with DuckDBStore(tmp_path / "db.duckdb") as store:
        _market(store, (date(2026, 8, 4),))
        with patch("rquant.pipeline.screen", return_value=_frame("OLD")):
            run_daily_screen_stage(
                "2026-08-04", preset_names=["user/pool"], store=store,
                preset_directory=directory,
            )
        old_receipt = _receipt(store, "2026-08-04", "user/pool")
        store._conn.execute("BEGIN")
        try:
            with (
                patch("rquant.pipeline.screen", return_value=_frame("NEW")),
                patch.object(
                    store, "_upsert_screen_run_receipt",
                    side_effect=RuntimeError("insert failed"),
                ),
                pytest.raises(RuntimeError, match="insert failed"),
            ):
                run_daily_screen_stage(
                    "2026-08-04", preset_names=["user/pool"], store=store,
                    preset_directory=directory, transaction_open=True,
                )
        finally:
            store._conn.execute("ROLLBACK")
        assert store.query_screen_result("2026-08-04", "user/pool")["ts_code"].tolist() == [
            "OLD"
        ]
        assert _receipt(store, "2026-08-04", "user/pool") == old_receipt


def test_research_sync_replaces_members_and_receipts_in_one_generation(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.duckdb"
    local_path = tmp_path / "local.duckdb"
    directory = tmp_path / "defs"
    _definition(directory, "pool")
    for path, member in ((source_path, "CLOUD"), (local_path, "LOCAL")):
        with DuckDBStore(path) as store:
            _market(store, (date(2026, 8, 4),))
            with patch("rquant.pipeline.screen", return_value=_frame(member)):
                run_daily_screen_stage(
                    "2026-08-04", preset_names=["user/pool"], store=store,
                    preset_directory=directory,
                )
    report = sync_from_backup(source_path, local_path, refresh_replica=False)
    assert not report.has_errors
    assert {row.table for row in report.tables if row.mode == "replace"} >= {
        "screen_result", "screen_run_receipt",
    }
    with DuckDBStore(local_path, read_only=True) as store:
        assert store.query_screen_result("2026-08-04", "user/pool")["ts_code"].tolist() == [
            "CLOUD"
        ]
        local_receipt = _receipt(store, "2026-08-04", "user/pool")
    with DuckDBStore(source_path, read_only=True) as store:
        assert local_receipt == _receipt(store, "2026-08-04", "user/pool")


def test_old_source_without_receipts_replaces_members_and_clears_unverifiable_proof(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.duckdb"
    local_path = tmp_path / "local.duckdb"
    directory = tmp_path / "defs"
    _definition(directory, "pool")
    for path, member in ((source_path, "CLOUD"), (local_path, "LOCAL")):
        with DuckDBStore(path) as store:
            _market(store, (date(2026, 8, 4),))
            with patch("rquant.pipeline.screen", return_value=_frame(member)):
                run_daily_screen_stage(
                    "2026-08-04", preset_names=["user/pool"], store=store,
                    preset_directory=directory,
                )
    with DuckDBStore(source_path) as store:
        store._conn.execute("DROP TABLE screen_run_receipt")
    with DuckDBStore(local_path, read_only=True) as store:
        prior = _receipt(store, "2026-08-04", "user/pool")
    assert prior is not None
    report = sync_from_backup(source_path, local_path, refresh_replica=False)
    assert not report.has_errors
    receipt_sync = next(row for row in report.tables if row.table == "screen_run_receipt")
    assert receipt_sync.mode == "skipped"
    assert "legacy" in receipt_sync.detail
    with DuckDBStore(local_path, read_only=True) as store:
        assert store.query_screen_result("2026-08-04", "user/pool")["ts_code"].tolist() == [
            "CLOUD"
        ]
        assert _receipt(store, "2026-08-04", "user/pool") is None


def test_later_sync_failure_rolls_back_both_result_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_path = tmp_path / "source.duckdb"
    local_path = tmp_path / "local.duckdb"
    directory = tmp_path / "defs"
    _definition(directory, "pool")
    for path, member in ((source_path, "CLOUD"), (local_path, "LOCAL")):
        with DuckDBStore(path) as store:
            _market(store, (date(2026, 8, 4),))
            with patch("rquant.pipeline.screen", return_value=_frame(member)):
                run_daily_screen_stage(
                    "2026-08-04", preset_names=["user/pool"], store=store,
                    preset_directory=directory,
                )
    with DuckDBStore(local_path, read_only=True) as store:
        prior = _receipt(store, "2026-08-04", "user/pool")
    real_sync = research_sync._sync_table

    def fail_after_results(*args: object, **kwargs: object) -> object:
        if args[1] == "trade_calendar":
            raise RuntimeError("later table failed")
        return real_sync(*args, **kwargs)

    monkeypatch.setattr(research_sync, "_sync_table", fail_after_results)
    report = sync_from_backup(source_path, local_path, refresh_replica=False)
    assert report.has_errors
    with DuckDBStore(local_path, read_only=True) as store:
        assert store.query_screen_result("2026-08-04", "user/pool")["ts_code"].tolist() == [
            "LOCAL"
        ]
        assert _receipt(store, "2026-08-04", "user/pool") == prior
