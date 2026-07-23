"""research_sync：云端备份合并 / 研究表恢复 / 副本刷新 / WAL 抢救。"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

import rquant.data_quality as data_quality
import rquant.research_sync as research_sync
from rquant.data_metadata import (
    DataQualityIssue,
    DatasetCoverage,
    DatasetSnapshot,
    DatasetSnapshotArtifact,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingFinalization,
    DatasetSnapshotBindingManifest,
    DatasetSnapshotFinalization,
)
from rquant.research_sync import (
    LOCAL_ONLY_TABLES,
    MERGE_TABLES,
    REPLACE_TABLES,
    TableSyncResult,
    _rescue_stale_wal,
    refresh_readonly_replica,
    restore_research_tables,
    sync_from_backup,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.suspension import (
    normalize_suspend_d_snapshot,
    persist_suspension_snapshot,
)


def _make_local_db(path: Path) -> None:
    """本地研究库：旧 daily_bar 1 行 + 研究表 minute_bar 2 行 + monitor_event 1 行。"""
    store = DuckDBStore(path)
    store._conn.execute(
        "INSERT INTO daily_bar (ts_code, trade_date, close) "
        "VALUES ('600000.SH', DATE '2026-06-01', 10.0)"
    )
    store._conn.execute(
        "INSERT INTO minute_bar (ts_code, trade_time, freq, open, high, low, close, source) "
        "VALUES ('600000.SH', TIMESTAMP '2026-06-01 09:31:00', '1min', 1, 2, 1, 2, 'tushare'), "
        "('600000.SH', TIMESTAMP '2026-06-01 09:32:00', '1min', 2, 3, 2, 3, 'tushare')"
    )
    store._conn.execute(
        "INSERT INTO monitor_event "
        "(trade_date, ts_code, level, trigger_price, level_price, trigger_time) "
        "VALUES (DATE '2026-06-01', '600000.SH', 'local_only', 1.0, 1.0, "
        "TIMESTAMP '2026-06-01 10:00:00')"
    )
    store.close()


def _make_backup_db(path: Path) -> None:
    """云端备份：新 daily_bar 2 行 + monitor_event 1 行（云端流），无 minute_bar 数据。"""
    store = DuckDBStore(path)
    store._conn.execute(
        "INSERT INTO daily_bar (ts_code, trade_date, close) "
        "VALUES ('600000.SH', DATE '2026-07-01', 11.0), "
        "('000001.SZ', DATE '2026-07-01', 12.0)"
    )
    store._conn.execute(
        "INSERT INTO monitor_event "
        "(trade_date, ts_code, level, trigger_price, level_price, trigger_time) "
        "VALUES (DATE '2026-07-01', '000001.SZ', 'cloud_only', 2.0, 2.0, "
        "TIMESTAMP '2026-07-01 10:00:00')"
    )
    store.close()


@pytest.fixture()
def local_db(tmp_path: Path) -> Path:
    p = tmp_path / "rquant.duckdb"
    _make_local_db(p)
    return p


@pytest.fixture()
def backup_db(tmp_path: Path) -> Path:
    p = tmp_path / "cloud_backup.duckdb"
    _make_backup_db(p)
    return p


def _insert_paper_position(db_path: Path, position_id: str, status: str) -> None:
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "INSERT INTO paper_position (position_id, trade_date, ts_code, entry_time, "
        "entry_price, entry_signal, candidate_id, earliest_exit_date, stop_loss_price, "
        "stop_loss_basis, stop_loss_pct, status) "
        "VALUES (?, DATE '2026-07-01', '600000.SH', TIMESTAMP '2026-07-01 09:31:00', "
        "10.0, 'auction_gap', 'cand-1', DATE '2026-07-02', 9.5, 'entry', 5.0, ?)",
        [position_id, status],
    )
    conn.close()


def _insert_trade_calendar_row(
    db_path: Path,
    *,
    cal_date: date,
    is_open: bool,
    pretrade_date: date | None,
    source: str,
    updated_at: datetime,
) -> None:
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "INSERT INTO trade_calendar "
        "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
        "VALUES ('SSE', ?, ?, ?, ?, ?)",
        [cal_date, is_open, pretrade_date, source, updated_at],
    )
    conn.close()


def _insert_stock_status_row(
    db_path: Path,
    *,
    name: str,
    is_st: bool,
    ingested_at: datetime,
    trade_date: date = date(2026, 7, 1),
) -> None:
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "INSERT INTO stock_status_daily "
        "(ts_code, trade_date, name, is_st, name_source, st_source, "
        "available_at, ingested_at, conflict_reason) "
        "VALUES ('600000.SH', ?, ?, ?, 'tushare.namechange', "
        "'tushare.namechange', ?, ?, NULL)",
        [
            trade_date,
            name,
            is_st,
            datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC),
            ingested_at,
        ],
    )
    conn.close()


class _ConnectionProxy:
    def __init__(self, inner: duckdb.DuckDBPyConnection) -> None:
        self._inner = inner

    def execute(
        self, query: str, *args: Any, **kwargs: Any
    ) -> duckdb.DuckDBPyConnection:
        return self._inner.execute(query, *args, **kwargs)

    def close(self) -> None:
        self._inner.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _RollbackFailingConnection(_ConnectionProxy):
    def execute(
        self, query: str, *args: Any, **kwargs: Any
    ) -> duckdb.DuckDBPyConnection:
        if query.strip().upper() == "ROLLBACK":
            raise duckdb.TransactionException("forced rollback failure")
        return super().execute(query, *args, **kwargs)


class _CommitThenRaiseConnection(_ConnectionProxy):
    def execute(
        self, query: str, *args: Any, **kwargs: Any
    ) -> duckdb.DuckDBPyConnection:
        result = super().execute(query, *args, **kwargs)
        if query.strip().upper() == "COMMIT":
            raise duckdb.TransactionException("commit acknowledgement lost")
        return result


class _CloseFailingConnection(_ConnectionProxy):
    def __init__(
        self, inner: duckdb.DuckDBPyConnection, failure_message: str
    ) -> None:
        super().__init__(inner)
        self._failure_message = failure_message

    def close(self) -> None:
        self._inner.close()
        raise duckdb.IOException(self._failure_message)


class TestSyncFromBackup:
    def test_missing_authoritative_replace_table_errors_without_replica_publish(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conn = duckdb.connect(str(local_db))
        conn.execute(
            "INSERT INTO stock_basic (ts_code, name) VALUES ('600000.SH', 'local')"
        )
        conn.close()
        conn = duckdb.connect(str(backup_db))
        conn.execute("DROP TABLE stock_basic")
        conn.close()
        refresh_calls = 0

        def spy_refresh(
            db_path: Path | None = None,
            replica_path: Path | None = None,
        ) -> tuple[bool, str]:
            nonlocal refresh_calls
            refresh_calls += 1
            return True, "must not refresh"

        monkeypatch.setattr(research_sync, "refresh_readonly_replica", spy_refresh)

        report = sync_from_backup(backup_db, local_db, refresh_replica=True)

        conn = duckdb.connect(str(local_db), read_only=True)
        names = conn.execute(
            "SELECT name FROM stock_basic WHERE ts_code = '600000.SH'"
        ).fetchall()
        conn.close()
        result = next(item for item in report.tables if item.table == "stock_basic")
        assert report.has_errors
        assert result.mode == "error"
        assert "authoritative" in result.detail
        assert names == [("local",)]
        assert refresh_calls == 0
        assert not report.replica_refreshed

    def test_missing_merge_table_remains_skipped(
        self, local_db: Path, backup_db: Path
    ) -> None:
        conn = duckdb.connect(str(backup_db))
        conn.execute("DROP TABLE minute_bar")
        conn.close()

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        result = next(item for item in report.tables if item.table == "minute_bar")
        assert not report.has_errors
        assert result.mode == "skipped"
        assert "备份中无此表" in result.detail

    def test_attempted_replica_refresh_failure_is_report_error(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def fail_refresh(
            db_path: Path | None = None,
            replica_path: Path | None = None,
        ) -> tuple[bool, str]:
            del db_path, replica_path
            return False, "replica lock failure"

        monkeypatch.setattr(
            research_sync,
            "refresh_readonly_replica",
            fail_refresh,
        )

        report = sync_from_backup(backup_db, local_db, refresh_replica=True)

        replica_result = next(
            result for result in report.tables if result.table == "<replica>"
        )
        assert report.has_errors
        assert not report.replica_refreshed
        assert report.replica_detail == "replica lock failure"
        assert replica_result.mode == "error"
        assert replica_result.rows == 0
        assert replica_result.detail == "replica lock failure"

    def test_table_failure_rolls_back_all_primary_changes(
        self, local_db: Path, backup_db: Path
    ) -> None:
        local = duckdb.connect(str(local_db))
        local.execute(
            "INSERT INTO stock_basic (ts_code, name) VALUES ('600000.SH', 'local')"
        )
        local.close()
        backup = duckdb.connect(str(backup_db))
        backup.execute(
            "INSERT INTO stock_basic (ts_code, name) VALUES ('000001.SZ', 'cloud')"
        )
        backup.execute("ALTER TABLE trade_calendar DROP COLUMN updated_at")
        backup.close()

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        conn = duckdb.connect(str(local_db), read_only=True)
        stocks = conn.execute(
            "SELECT ts_code, name FROM stock_basic ORDER BY ts_code"
        ).fetchall()
        daily_dates = conn.execute(
            "SELECT DISTINCT trade_date FROM daily_bar ORDER BY trade_date"
        ).fetchall()
        conn.close()
        assert report.has_errors
        assert stocks == [("600000.SH", "local")]
        assert daily_dates == [(date(2026, 6, 1),)]
        assert any("rolled back" in item.detail for item in report.tables)

    def test_direct_table_exception_zeroes_prior_results_and_names_table(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        local = duckdb.connect(str(local_db))
        local.execute(
            "INSERT INTO stock_basic (ts_code, name) VALUES ('600000.SH', 'local')"
        )
        local.close()
        backup = duckdb.connect(str(backup_db))
        backup.execute(
            "INSERT INTO stock_basic (ts_code, name) VALUES ('000001.SZ', 'cloud')"
        )
        backup.close()
        real_sync_table = research_sync._sync_table
        refresh_calls = 0

        def raising_sync_table(
            conn: Any,
            table: str,
            alias: str,
            mode: str,
            *,
            manage_transaction: bool = True,
        ) -> TableSyncResult:
            if table == "daily_bar":
                raise RuntimeError("direct table failure")
            return real_sync_table(
                conn,
                table,
                alias,
                mode,
                manage_transaction=manage_transaction,
            )

        def spy_refresh(
            db_path: Path | None = None,
            replica_path: Path | None = None,
        ) -> tuple[bool, str]:
            nonlocal refresh_calls
            refresh_calls += 1
            return True, "must not refresh"

        monkeypatch.setattr(research_sync, "_sync_table", raising_sync_table)
        monkeypatch.setattr(research_sync, "refresh_readonly_replica", spy_refresh)

        report = sync_from_backup(backup_db, local_db, refresh_replica=True)

        by_table = {result.table: result for result in report.tables}
        assert report.has_errors
        assert by_table["stock_basic"].mode == "skipped"
        assert by_table["stock_basic"].rows == 0
        assert "rolled back" in by_table["stock_basic"].detail
        assert by_table["daily_bar"].mode == "error"
        assert by_table["daily_bar"].rows == 0
        assert "direct table failure" in by_table["daily_bar"].detail
        assert all(result.rows == 0 for result in report.tables)
        assert refresh_calls == 0

        conn = duckdb.connect(str(local_db), read_only=True)
        assert conn.execute(
            "SELECT ts_code, name FROM stock_basic"
        ).fetchall() == [("600000.SH", "local")]
        conn.close()

    def test_direct_metadata_exception_zeroes_results_and_names_bundle(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def raising_metadata_sync(
            conn: Any,
            alias: str,
            *,
            manage_transaction: bool = True,
        ) -> list[TableSyncResult]:
            del conn, alias, manage_transaction
            raise RuntimeError("direct metadata failure")

        monkeypatch.setattr(
            research_sync,
            "_sync_data_metadata_bundle",
            raising_metadata_sync,
        )

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        by_table = {result.table: result for result in report.tables}
        assert report.has_errors
        assert by_table["daily_bar"].mode == "skipped"
        assert by_table["daily_bar"].rows == 0
        assert "rolled back" in by_table["daily_bar"].detail
        assert by_table["<metadata_bundle>"].mode == "error"
        assert by_table["<metadata_bundle>"].rows == 0
        assert "direct metadata failure" in by_table["<metadata_bundle>"].detail
        assert all(result.rows == 0 for result in report.tables)

    def test_rollback_failure_is_reported_as_unconfirmed(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        real_sync_table = research_sync._sync_table
        real_connect = duckdb.connect
        refresh_calls = 0

        def open_with_failed_rollback(path: Path) -> _RollbackFailingConnection:
            return _RollbackFailingConnection(real_connect(str(path)))

        def table_error(
            conn: Any,
            table: str,
            alias: str,
            mode: str,
            *,
            manage_transaction: bool = True,
        ) -> TableSyncResult:
            if table == "daily_bar":
                return TableSyncResult(
                    table=table,
                    mode="error",
                    detail="forced table failure",
                )
            return real_sync_table(
                conn,
                table,
                alias,
                mode,
                manage_transaction=manage_transaction,
            )

        def spy_refresh(
            db_path: Path | None = None,
            replica_path: Path | None = None,
        ) -> tuple[bool, str]:
            nonlocal refresh_calls
            refresh_calls += 1
            return True, "must not refresh"

        monkeypatch.setattr(
            research_sync,
            "_rescue_stale_wal",
            open_with_failed_rollback,
        )
        monkeypatch.setattr(research_sync, "_sync_table", table_error)
        monkeypatch.setattr(research_sync, "refresh_readonly_replica", spy_refresh)

        report = sync_from_backup(backup_db, local_db, refresh_replica=True)

        by_table = {result.table: result for result in report.tables}
        assert report.has_errors
        assert by_table["daily_bar"].mode == "error"
        assert "outcome unconfirmed" in by_table["daily_bar"].detail
        assert by_table["stock_basic"].mode == "error"
        assert by_table["stock_basic"].rows == 0
        assert "changes may be durable" in by_table["stock_basic"].detail
        assert all(
            "all primary changes rolled back" not in result.detail
            for result in report.tables
        )
        assert refresh_calls == 0
        assert not report.replica_refreshed

    def test_commit_acknowledgement_failure_marks_all_outcomes_unknown(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        backup = duckdb.connect(str(backup_db))
        backup.execute(
            "INSERT INTO stock_basic (ts_code, name) VALUES ('000001.SZ', 'durable')"
        )
        backup.close()
        real_connect = duckdb.connect

        def open_with_ambiguous_commit(path: Path) -> _CommitThenRaiseConnection:
            return _CommitThenRaiseConnection(real_connect(str(path)))

        monkeypatch.setattr(
            research_sync,
            "_rescue_stale_wal",
            open_with_ambiguous_commit,
        )

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        assert report.has_errors
        assert all(result.rows == 0 for result in report.tables)
        assert all(result.mode == "error" for result in report.tables)
        assert all(
            "outcome unconfirmed; changes may be durable" in result.detail
            for result in report.tables
        )
        commit_result = next(
            result for result in report.tables if result.table == "<commit>"
        )
        assert "commit acknowledgement lost" in commit_result.detail

        conn = real_connect(str(local_db), read_only=True)
        assert conn.execute(
            "SELECT name FROM stock_basic WHERE ts_code = '000001.SZ'"
        ).fetchall() == [("durable",)]
        conn.close()

    def test_metadata_error_with_failed_outer_rollback_never_claims_rollback(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        real_connect = duckdb.connect
        refresh_calls = 0

        def open_with_failed_rollback(path: Path) -> _RollbackFailingConnection:
            return _RollbackFailingConnection(real_connect(str(path)))

        def fail_metadata_load(
            conn: duckdb.DuckDBPyConnection,
            alias: str,
        ) -> dict[str, DatasetSnapshot]:
            del conn, alias
            raise ValueError("forced metadata validation failure")

        def spy_refresh(
            db_path: Path | None = None,
            replica_path: Path | None = None,
        ) -> tuple[bool, str]:
            nonlocal refresh_calls
            refresh_calls += 1
            return True, "must not refresh"

        monkeypatch.setattr(
            research_sync,
            "_rescue_stale_wal",
            open_with_failed_rollback,
        )
        monkeypatch.setattr(
            research_sync,
            "_load_source_snapshots",
            fail_metadata_load,
        )
        monkeypatch.setattr(research_sync, "refresh_readonly_replica", spy_refresh)

        report = sync_from_backup(backup_db, local_db, refresh_replica=True)

        assert report.has_errors
        assert all(result.rows == 0 for result in report.tables)
        assert all(result.mode == "error" for result in report.tables)
        assert any(
            "outcome unconfirmed; changes may be durable" in result.detail
            for result in report.tables
            if result.mode == "error"
        )
        assert all("rolled back" not in result.detail for result in report.tables)
        assert refresh_calls == 0
        assert not report.replica_refreshed

    def test_standalone_metadata_failure_confirms_internal_rollback(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def fail_metadata_load(
            conn: duckdb.DuckDBPyConnection,
            alias: str,
        ) -> dict[str, DatasetSnapshot]:
            del conn, alias
            raise ValueError("forced standalone metadata failure")

        monkeypatch.setattr(
            research_sync,
            "_load_source_snapshots",
            fail_metadata_load,
        )
        conn = duckdb.connect(str(local_db))
        research_sync._attach_readonly(conn, backup_db, "cloud_backup")

        results = research_sync._sync_data_metadata_bundle(
            conn,
            "cloud_backup",
            manage_transaction=True,
        )

        error = next(result for result in results if result.mode == "error")
        assert "linked metadata bundle rolled back" in error.detail
        conn.execute("DETACH cloud_backup")
        conn.close()

    def test_uses_shared_schema_initializer(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[duckdb.DuckDBPyConnection] = []
        real_initialize_schema = research_sync.initialize_schema

        def spy_initialize_schema(conn: duckdb.DuckDBPyConnection) -> None:
            calls.append(conn)
            real_initialize_schema(conn)

        monkeypatch.setattr(
            research_sync, "initialize_schema", spy_initialize_schema
        )

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        assert not report.has_errors
        assert len(calls) == 1

    def test_schema_migration_is_not_imported_from_backup(
        self, local_db: Path, backup_db: Path
    ) -> None:
        local_conn = duckdb.connect(str(local_db))
        local_rows = local_conn.execute(
            "SELECT version, name, checksum FROM schema_migration ORDER BY version"
        ).fetchall()
        local_conn.close()

        backup_conn = duckdb.connect(str(backup_db))
        backup_conn.execute(
            "UPDATE schema_migration SET name = 'cloud-only', "
            "checksum = 'cloud-only'"
        )
        backup_conn.close()

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        conn = duckdb.connect(str(local_db), read_only=True)
        after_rows = conn.execute(
            "SELECT version, name, checksum FROM schema_migration ORDER BY version"
        ).fetchall()
        conn.close()
        assert not report.has_errors
        assert "schema_migration" not in {result.table for result in report.tables}
        assert after_rows == local_rows

    def test_merge_keeps_local_history_and_replace_tables_replaced(
        self, local_db: Path, backup_db: Path
    ) -> None:
        """日线族改 merge 语义（2020-2024 历史回补后）：本地旧 6-01 行保留，
        云端 7-01 行进来；stock_basic 等云端权威表仍整表替换。"""
        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        conn = duckdb.connect(str(local_db), read_only=True)
        # merge 表：本地独有 6-01 历史行保留 + 云端 7-01 两行进来
        daily = conn.execute(
            "SELECT trade_date, COUNT(*) FROM daily_bar GROUP BY 1 ORDER BY 1"
        ).fetchall()
        assert daily == [(date(2026, 6, 1), 1), (date(2026, 7, 1), 2)]
        # merge 表：本地 local_only + 云端 cloud_only 共存
        levels = {
            r[0] for r in conn.execute("SELECT level FROM monitor_event").fetchall()
        }
        assert levels == {"local_only", "cloud_only"}
        # 备份中不存在数据的研究表：本地行原样保留
        assert conn.execute("SELECT COUNT(*) FROM minute_bar").fetchone()[0] == 2
        conn.close()

        assert not report.has_errors
        by_table = {t.table: t for t in report.tables}
        assert by_table["daily_bar"].mode == "merge"
        assert by_table["stock_basic"].mode == "replace"
        assert by_table["monitor_event"].mode == "merge"

    def test_daily_bar_pk_conflict_cloud_wins(
        self, local_db: Path, backup_db: Path
    ) -> None:
        """merge 语义下主键冲突云端赢：云端最新数据仍是权威。"""
        conn = duckdb.connect(str(local_db))
        conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) "
            "VALUES ('600000.SH', DATE '2026-07-01', 999.0)"
        )
        conn.close()

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)
        assert not report.has_errors

        conn = duckdb.connect(str(local_db), read_only=True)
        close = conn.execute(
            "SELECT close FROM daily_bar "
            "WHERE ts_code = '600000.SH' AND trade_date = DATE '2026-07-01'"
        ).fetchone()[0]
        local_only = conn.execute(
            "SELECT COUNT(*) FROM daily_bar WHERE trade_date = DATE '2026-06-01'"
        ).fetchone()[0]
        conn.close()
        assert close == 11.0
        assert local_only == 1

    def test_backup_missing_reports_error_not_raise(
        self, local_db: Path, tmp_path: Path
    ) -> None:
        """A7：顶层 FileNotFoundError 转报告，告警只由脚本推，不经 main() notify。"""
        report = sync_from_backup(
            tmp_path / "nope.duckdb", local_db, refresh_replica=False
        )
        assert report.has_errors
        assert report.tables[0].mode == "error"
        assert "不存在" in report.tables[0].detail

    def test_merge_cloud_row_wins_on_pk_conflict(
        self, local_db: Path, backup_db: Path
    ) -> None:
        """A2 对照：云端持续同步（MERGE_TABLES）保持 OR REPLACE，云端覆盖本地。"""
        conn = duckdb.connect(str(local_db))
        conn.execute(
            "INSERT INTO monitor_event "
            "(trade_date, ts_code, level, trigger_price, level_price, trigger_time) "
            "VALUES (DATE '2026-07-01', '000001.SZ', 'cloud_only', 999.0, 999.0, "
            "TIMESTAMP '2026-07-01 09:00:00')"
        )
        conn.close()

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)
        assert not report.has_errors

        conn = duckdb.connect(str(local_db), read_only=True)
        price = conn.execute(
            "SELECT trigger_price FROM monitor_event "
            "WHERE ts_code = '000001.SZ' AND level = 'cloud_only'"
        ).fetchone()[0]
        conn.close()
        assert price == 2.0

    def test_backup_path_with_single_quote(
        self, local_db: Path, tmp_path: Path
    ) -> None:
        """A1：ATTACH 路径含单引号不炸 SQL。"""
        qdir = tmp_path / "roxor's backup"
        qdir.mkdir()
        backup = qdir / "cloud_backup.duckdb"
        _make_backup_db(backup)

        report = sync_from_backup(backup, local_db, refresh_replica=False)
        assert not report.has_errors

        conn = duckdb.connect(str(local_db), read_only=True)
        # merge 语义：本地 1 行 + 云端 2 行
        assert conn.execute("SELECT COUNT(*) FROM daily_bar").fetchone()[0] == 3
        conn.close()

    def test_top_level_failure_reported_not_raised(
        self, local_db: Path, backup_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A7：ATTACH 撞锁等顶层异常转报告返回，不向上抛。"""

        def boom(conn, path, alias):  # noqa: ANN001
            raise duckdb.IOException("Could not set lock on file")

        monkeypatch.setattr(research_sync, "_attach_readonly", boom)
        report = sync_from_backup(backup_db, local_db, refresh_replica=False)
        assert report.has_errors
        assert report.tables[-1].table == "<sync>"
        assert "lock" in report.tables[-1].detail

    def test_replica_skipped_when_table_error(
        self, local_db: Path, backup_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A3：部分表失败时不刷新只读副本，避免发布跨表不一致快照。"""
        real_sync_table = research_sync._sync_table

        def flaky(  # noqa: ANN001
            conn, table, alias, mode, *, manage_transaction=True
        ):
            if table == "daily_bar":
                return TableSyncResult(table=table, mode="error", detail="boom")
            return real_sync_table(
                conn,
                table,
                alias,
                mode,
                manage_transaction=manage_transaction,
            )

        calls = {"n": 0}

        def spy_refresh(db_path=None, replica_path=None):  # noqa: ANN001
            calls["n"] += 1
            return True, "不应被调用"

        monkeypatch.setattr(research_sync, "_sync_table", flaky)
        monkeypatch.setattr(research_sync, "refresh_readonly_replica", spy_refresh)

        report = sync_from_backup(backup_db, local_db, refresh_replica=True)
        assert report.has_errors
        assert calls["n"] == 0
        assert not report.replica_refreshed
        assert "跳过副本刷新" in report.replica_detail

    def test_merge_idempotent(self, local_db: Path, backup_db: Path) -> None:
        sync_from_backup(backup_db, local_db, refresh_replica=False)
        report2 = sync_from_backup(backup_db, local_db, refresh_replica=False)
        assert not report2.has_errors

        conn = duckdb.connect(str(local_db), read_only=True)
        assert conn.execute("SELECT COUNT(*) FROM monitor_event").fetchone()[0] == 2
        # merge 语义：本地 1 行 + 云端 2 行，重跑不重复
        assert conn.execute("SELECT COUNT(*) FROM daily_bar").fetchone()[0] == 3
        conn.close()

    def test_sync_cannot_regress_ready_snapshot_to_building(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        finalization = DatasetSnapshotFinalization(
            table_watermarks={"daily_bar": "ready-target"},
            completed_at=t0 + timedelta(minutes=1),
        )
        with DuckDBStore(local_db) as local:
            local.begin_dataset_snapshot(snapshot)
            local.finalize_dataset_snapshot(snapshot.snapshot_id, finalization)
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(snapshot)

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored = local.get_dataset_snapshot(snapshot.snapshot_id)
        assert not report.has_errors
        assert stored is not None
        assert stored.status == "ready"
        assert stored.table_watermarks == {"daily_bar": "ready-target"}

    def test_sync_older_open_cannot_replace_newer_resolution(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        initial = DataQualityIssue.detected(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P1",
            scope_key="all",
            message="initial",
            observed_at=t0,
        )
        older_open = DataQualityIssue.detected(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P0",
            scope_key="all",
            message="older open",
            evidence={"stale": True},
            observed_at=t0 + timedelta(minutes=1),
        )
        with DuckDBStore(local_db) as local:
            local.record_data_quality_issue(initial)
            resolved = local.resolve_data_quality_issue(
                initial.issue_id,
                resolved_at=t0 + timedelta(minutes=2),
            )
        with DuckDBStore(backup_db) as backup:
            backup.record_data_quality_issue(older_open)

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored = local.get_data_quality_issue(initial.issue_id)
        assert not report.has_errors
        assert stored is not None
        assert stored.status == "resolved"
        assert stored.first_seen_at == t0
        assert stored.last_seen_at == t0 + timedelta(minutes=1)
        assert stored.resolved_at == resolved.resolved_at
        assert stored.severity == "P0"
        assert stored.message == "older open"
        assert stored.evidence == {"stale": True}

    def test_sync_newer_detection_reopens_and_preserves_first_seen(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        initial = DataQualityIssue.detected(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P2",
            scope_key="all",
            message="initial",
            observed_at=t0,
        )
        newer_open = DataQualityIssue.detected(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P0",
            scope_key="all",
            message="new detection",
            evidence={"new": True},
            observed_at=t0 + timedelta(minutes=3),
        )
        with DuckDBStore(local_db) as local:
            local.record_data_quality_issue(initial)
            local.resolve_data_quality_issue(
                initial.issue_id,
                resolved_at=t0 + timedelta(minutes=2),
            )
        with DuckDBStore(backup_db) as backup:
            backup.record_data_quality_issue(newer_open)

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored = local.get_data_quality_issue(initial.issue_id)
        assert not report.has_errors
        assert stored is not None
        assert stored.status == "open"
        assert stored.first_seen_at == t0
        assert stored.last_seen_at == t0 + timedelta(minutes=3)
        assert stored.message == "new detection"
        assert stored.resolved_at is None

    def test_sync_reports_conflicting_ready_snapshot_finalization(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        target_finalization = DatasetSnapshotFinalization(
            table_watermarks={"daily_bar": "target"},
            completed_at=t0 + timedelta(minutes=1),
        )
        source_finalization = DatasetSnapshotFinalization(
            table_watermarks={"daily_bar": "source"},
            completed_at=t0 + timedelta(minutes=1),
        )
        with DuckDBStore(local_db) as local:
            local.begin_dataset_snapshot(snapshot)
            local.finalize_dataset_snapshot(
                snapshot.snapshot_id,
                target_finalization,
            )
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(snapshot)
            backup.finalize_dataset_snapshot(
                snapshot.snapshot_id,
                source_finalization,
            )

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored = local.get_dataset_snapshot(snapshot.snapshot_id)
        assert report.has_errors
        assert "immutable" in next(
            result.detail
            for result in report.tables
            if result.table == "dataset_snapshot"
        )
        assert stored is not None
        assert stored.table_watermarks == {"daily_bar": "target"}

    def test_sync_promotes_snapshot_with_earliest_created_at(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        source_snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        target_snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0 + timedelta(minutes=2),
        )
        finalization = DatasetSnapshotFinalization(
            table_watermarks={"daily_bar": "ready-source"},
            completed_at=t0 + timedelta(minutes=1),
        )
        with DuckDBStore(local_db) as local:
            local.begin_dataset_snapshot(target_snapshot)
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(source_snapshot)
            backup.finalize_dataset_snapshot(
                source_snapshot.snapshot_id,
                finalization,
            )

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored = local.get_dataset_snapshot(source_snapshot.snapshot_id)
        assert not report.has_errors
        assert stored is not None
        assert stored.status == "ready"
        assert stored.created_at == t0
        assert stored.completed_at == t0 + timedelta(minutes=1)

    def test_sync_source_only_building_snapshot_and_coverage_together(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        snapshot = DatasetSnapshot.create(
            strategy_name="building-source",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        coverage = DatasetCoverage(
            snapshot_id=snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=10,
            available_count=7,
            missing_reasons=("backfill still running",),
            created_at=t0,
        )
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(snapshot)
            backup.upsert_dataset_coverage(coverage)

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored_snapshot = local.get_dataset_snapshot(snapshot.snapshot_id)
            stored_coverages = local.list_dataset_coverages(snapshot.snapshot_id)
        by_table = {result.table: result for result in report.tables}
        assert not report.has_errors
        assert stored_snapshot == snapshot
        assert stored_coverages == [coverage]
        assert by_table["dataset_snapshot"].rows == 1
        assert by_table["dataset_coverage"].rows == 1

    def test_sync_building_source_conflicting_with_ready_coverage_fails(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        target_coverage = DatasetCoverage(
            snapshot_id=snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=10,
            available_count=9,
            missing_reasons=("target missing one",),
            created_at=t0,
        )
        source_coverage = DatasetCoverage(
            snapshot_id=snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=10,
            available_count=8,
            missing_reasons=("source missing two",),
            created_at=t0,
        )
        with DuckDBStore(local_db) as local:
            local.begin_dataset_snapshot(snapshot)
            local.upsert_dataset_coverage(target_coverage)
            local.finalize_dataset_snapshot(
                snapshot.snapshot_id,
                DatasetSnapshotFinalization(
                    completed_at=t0 + timedelta(minutes=1)
                ),
            )
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(snapshot)
            backup.upsert_dataset_coverage(source_coverage)

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored = local.list_dataset_coverages(snapshot.snapshot_id)
        coverage_result = next(
            result
            for result in report.tables
            if result.table == "dataset_coverage"
        )
        assert report.has_errors
        assert coverage_result.mode == "error"
        assert coverage_result.rows == 0
        assert "ready snapshot" in coverage_result.detail
        assert stored == [target_coverage]

    def test_sync_building_source_identical_to_ready_is_noop(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        target_coverage = DatasetCoverage(
            snapshot_id=snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=10,
            available_count=9,
            missing_reasons=("one missing",),
            created_at=t0 + timedelta(minutes=1),
        )
        source_coverage = target_coverage.model_copy(update={"created_at": t0})
        with DuckDBStore(local_db) as local:
            local.begin_dataset_snapshot(snapshot)
            local.upsert_dataset_coverage(target_coverage)
            local.finalize_dataset_snapshot(
                snapshot.snapshot_id,
                DatasetSnapshotFinalization(
                    completed_at=t0 + timedelta(minutes=2)
                ),
            )
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(snapshot)
            backup.upsert_dataset_coverage(source_coverage)

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored_snapshot = local.get_dataset_snapshot(snapshot.snapshot_id)
            stored_coverages = local.list_dataset_coverages(snapshot.snapshot_id)
        by_table = {result.table: result for result in report.tables}
        assert not report.has_errors
        assert stored_snapshot is not None
        assert stored_snapshot.status == "ready"
        assert stored_coverages == [target_coverage]
        assert by_table["dataset_snapshot"].rows == 0
        assert by_table["dataset_coverage"].rows == 0

    def test_sync_orphan_coverage_rolls_back_entire_metadata_bundle(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        valid_snapshot = DatasetSnapshot.create(
            strategy_name="valid-building",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        valid_coverage = DatasetCoverage(
            snapshot_id=valid_snapshot.snapshot_id,
            dataset_id="valid-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=1,
            available_count=1,
            created_at=t0,
        )
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(valid_snapshot)
            backup.upsert_dataset_coverage(valid_coverage)
            backup._conn.execute(
                """
                INSERT INTO dataset_coverage
                (snapshot_id, dataset_id, coverage_scope, table_name,
                 expected_count, available_count, missing_count, coverage_ratio,
                 missing_reasons, created_at)
                VALUES ('orphan-snapshot', 'orphan-bars', 'all', 'minute_bar',
                        1, 1, 0, 1.0, CAST('[]' AS JSON), ?)
                """,
                [t0],
            )

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored_snapshot = local.get_dataset_snapshot(valid_snapshot.snapshot_id)
            coverage_count = local._conn.execute(
                "SELECT COUNT(*) FROM dataset_coverage"
            ).fetchone()[0]
        by_table = {result.table: result for result in report.tables}
        assert report.has_errors
        assert by_table["dataset_coverage"].mode == "error"
        assert "missing dataset_snapshot" in by_table["dataset_coverage"].detail
        assert stored_snapshot is None
        assert coverage_count == 0

    def test_sync_conflicting_ready_coverage_fails_and_preserves_target(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        finalization = DatasetSnapshotFinalization(
            completed_at=t0 + timedelta(minutes=1)
        )
        target_coverage = DatasetCoverage(
            snapshot_id=snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=10,
            available_count=9,
            created_at=t0,
        )
        source_coverage = DatasetCoverage(
            snapshot_id=snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=10,
            available_count=8,
            created_at=t0,
        )
        with DuckDBStore(local_db) as local:
            local.begin_dataset_snapshot(snapshot)
            local.upsert_dataset_coverage(target_coverage)
            local.finalize_dataset_snapshot(snapshot.snapshot_id, finalization)
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(snapshot)
            backup.upsert_dataset_coverage(source_coverage)
            backup.finalize_dataset_snapshot(snapshot.snapshot_id, finalization)

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored = local.list_dataset_coverages(snapshot.snapshot_id)
        coverage_result = next(
            result
            for result in report.tables
            if result.table == "dataset_coverage"
        )
        assert report.has_errors
        assert coverage_result.mode == "error"
        assert "conflict" in coverage_result.detail
        assert stored == [target_coverage]

    def test_sync_ready_coverage_new_and_identical_rows_are_idempotent(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        coverage = DatasetCoverage(
            snapshot_id=snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=10,
            available_count=9,
            missing_reasons=("one missing",),
            created_at=t0,
        )
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(snapshot)
            backup.upsert_dataset_coverage(coverage)
            backup.finalize_dataset_snapshot(
                snapshot.snapshot_id,
                DatasetSnapshotFinalization(
                    completed_at=t0 + timedelta(minutes=1)
                ),
            )

        first = sync_from_backup(backup_db, local_db, refresh_replica=False)
        second = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored = local.list_dataset_coverages(snapshot.snapshot_id)
        assert not first.has_errors
        assert not second.has_errors
        assert stored == [coverage]

    def test_sync_reconciles_issue_observation_and_resolution_timelines(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        target_observation = DataQualityIssue.detected(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P0",
            scope_key="all",
            message="newest observation",
            evidence={"observation": "t10"},
            observed_at=t0 + timedelta(minutes=10),
        )
        source_observation = DataQualityIssue.detected(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P2",
            scope_key="all",
            message="old observation",
            evidence={"observation": "t0"},
            observed_at=t0,
        )
        with DuckDBStore(local_db) as local:
            local.record_data_quality_issue(target_observation)
        with DuckDBStore(backup_db) as backup:
            backup.record_data_quality_issue(source_observation)
            backup.resolve_data_quality_issue(
                source_observation.issue_id,
                resolved_at=t0 + timedelta(minutes=12),
            )

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored = local.get_data_quality_issue(target_observation.issue_id)
        assert not report.has_errors
        assert stored is not None
        assert stored.status == "resolved"
        assert stored.first_seen_at == t0
        assert stored.last_seen_at == t0 + timedelta(minutes=10)
        assert stored.resolved_at == t0 + timedelta(minutes=12)
        assert stored.severity == "P0"
        assert stored.message == "newest observation"
        assert stored.evidence == {"observation": "t10"}

    def test_sync_quality_issue_counts_only_actual_reconciliation(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        initial = DataQualityIssue.detected(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P2",
            scope_key="all",
            message="initial",
            evidence={"observation": "t0"},
            observed_at=t0,
        )
        target_newer = DataQualityIssue.detected(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P1",
            scope_key="all",
            message="target newer",
            evidence={"observation": "t10"},
            observed_at=t0 + timedelta(minutes=10),
        )
        source_newest = DataQualityIssue.detected(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P0",
            scope_key="all",
            message="source newest",
            evidence={"observation": "t20"},
            observed_at=t0 + timedelta(minutes=20),
        )
        with DuckDBStore(local_db) as local:
            local.record_data_quality_issue(initial)
            local.record_data_quality_issue(target_newer)
        with DuckDBStore(backup_db) as backup:
            backup.record_data_quality_issue(initial)

        stale = sync_from_backup(backup_db, local_db, refresh_replica=False)
        with DuckDBStore(backup_db) as backup:
            backup.record_data_quality_issue(source_newest)
        newer = sync_from_backup(backup_db, local_db, refresh_replica=False)
        idempotent = sync_from_backup(
            backup_db, local_db, refresh_replica=False
        )

        with DuckDBStore(local_db) as local:
            stored = local.get_data_quality_issue(initial.issue_id)
        stale_rows = {
            result.table: result.rows for result in stale.tables
        }
        newer_rows = {
            result.table: result.rows for result in newer.tables
        }
        idempotent_rows = {
            result.table: result.rows for result in idempotent.tables
        }
        assert not stale.has_errors
        assert not newer.has_errors
        assert not idempotent.has_errors
        assert stale_rows["data_quality_issue"] == 0
        assert newer_rows["data_quality_issue"] == 1
        assert idempotent_rows["data_quality_issue"] == 0
        assert stored is not None
        assert stored.severity == "P0"
        assert stored.last_seen_at == t0 + timedelta(minutes=20)
        assert stored.message == "source newest"
        assert stored.evidence == {"observation": "t20"}

    def test_metadata_bundle_promotes_snapshot_and_source_coverage_together(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        source_snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        target_snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0 + timedelta(minutes=2),
        )
        target_coverage = DatasetCoverage(
            snapshot_id=target_snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=10,
            available_count=8,
            missing_reasons=("local building",),
            created_at=t0 + timedelta(minutes=2),
        )
        source_coverage = DatasetCoverage(
            snapshot_id=source_snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=10,
            available_count=9,
            missing_reasons=("source ready",),
            created_at=t0,
        )
        with DuckDBStore(local_db) as local:
            local.begin_dataset_snapshot(target_snapshot)
            local.upsert_dataset_coverage(target_coverage)
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(source_snapshot)
            backup.upsert_dataset_coverage(source_coverage)
            backup.finalize_dataset_snapshot(
                source_snapshot.snapshot_id,
                DatasetSnapshotFinalization(completed_at=t0 + timedelta(minutes=1)),
            )

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored_snapshot = local.get_dataset_snapshot(source_snapshot.snapshot_id)
            stored_coverage = local.list_dataset_coverages(source_snapshot.snapshot_id)
        assert not report.has_errors
        assert stored_snapshot is not None
        assert stored_snapshot.status == "ready"
        assert stored_snapshot.created_at == t0
        assert stored_snapshot.completed_at == t0 + timedelta(minutes=1)
        assert stored_coverage == [
            source_coverage.model_copy(
                update={"created_at": target_coverage.created_at}
            )
        ]
        by_table = {result.table: result for result in report.tables}
        assert by_table["dataset_snapshot"].rows == 1
        assert by_table["dataset_coverage"].rows == 1

    def test_metadata_bundle_conflict_rolls_back_linked_rows_and_retry(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        source_issue = DataQualityIssue.detected(
            rule_id="source-only",
            dataset_id="minute-bars",
            severity="P1",
            scope_key="all",
            message="must not leak",
            observed_at=t0,
        )
        source_coverage = DatasetCoverage(
            snapshot_id=snapshot.snapshot_id,
            dataset_id="source-only",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=1,
            available_count=1,
            created_at=t0,
        )
        target_finalization = DatasetSnapshotFinalization(
            table_watermarks={"daily_bar": "target"},
            completed_at=t0 + timedelta(minutes=1),
        )
        source_finalization = DatasetSnapshotFinalization(
            table_watermarks={"daily_bar": "source"},
            quality_issue_ids=(source_issue.issue_id,),
            completed_at=t0 + timedelta(minutes=1),
        )
        with DuckDBStore(local_db) as local:
            local.begin_dataset_snapshot(snapshot)
            local.finalize_dataset_snapshot(
                snapshot.snapshot_id,
                target_finalization,
            )
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(snapshot)
            backup.record_data_quality_issue(source_issue)
            backup.upsert_dataset_coverage(source_coverage)
            backup.finalize_dataset_snapshot(
                snapshot.snapshot_id,
                source_finalization,
            )
        refresh_calls = 0

        def spy_refresh(db_path=None, replica_path=None):  # noqa: ANN001
            nonlocal refresh_calls
            refresh_calls += 1
            return True, "must not refresh"

        monkeypatch.setattr(research_sync, "refresh_readonly_replica", spy_refresh)

        first = sync_from_backup(backup_db, local_db, refresh_replica=True)
        second = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored_snapshot = local.get_dataset_snapshot(snapshot.snapshot_id)
            stored_coverage = local.list_dataset_coverages(snapshot.snapshot_id)
            stored_issue = local.get_data_quality_issue(source_issue.issue_id)
        first_results = {result.table: result for result in first.tables}
        assert first.has_errors
        assert second.has_errors
        assert refresh_calls == 0
        assert not first.replica_refreshed
        assert "跳过副本刷新" in first.replica_detail
        assert first_results["dataset_snapshot"].mode == "error"
        assert first_results["dataset_coverage"].mode == "skipped"
        assert first_results["data_quality_issue"].mode == "skipped"
        assert stored_snapshot is not None
        assert stored_snapshot.table_watermarks == {"daily_bar": "target"}
        assert stored_snapshot.quality_issue_ids == ()
        assert stored_coverage == []
        assert stored_issue is None

    def test_invalid_source_only_issue_rolls_back_bundle_and_replica(
        self,
        local_db: Path,
        backup_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        snapshot = DatasetSnapshot.create(
            strategy_name="source-only",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        coverage = DatasetCoverage(
            snapshot_id=snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=1,
            available_count=1,
            created_at=t0,
        )
        invalid_issue = DataQualityIssue.detected(
            rule_id="invalid-timeline",
            dataset_id="minute-bars",
            severity="P1",
            scope_key="all",
            message="invalid source row",
            observed_at=t0,
        )
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(snapshot)
            backup.upsert_dataset_coverage(coverage)
            backup.finalize_dataset_snapshot(
                snapshot.snapshot_id,
                DatasetSnapshotFinalization(completed_at=t0 + timedelta(minutes=1)),
            )
            backup._conn.execute(
                """
                INSERT INTO data_quality_issue
                (issue_id, rule_id, dataset_id, severity, status, scope_key,
                 message, evidence, first_seen_at, last_seen_at, resolved_at)
                VALUES (?, ?, ?, ?, 'resolved', ?, ?, CAST('{}' AS JSON), ?, ?, ?)
                """,
                [
                    invalid_issue.issue_id,
                    invalid_issue.rule_id,
                    invalid_issue.dataset_id,
                    invalid_issue.severity,
                    invalid_issue.scope_key,
                    invalid_issue.message,
                    t0,
                    t0 + timedelta(minutes=2),
                    t0 + timedelta(minutes=1),
                ],
            )
        refresh_calls = 0

        def spy_refresh(db_path=None, replica_path=None):  # noqa: ANN001
            nonlocal refresh_calls
            refresh_calls += 1
            return True, "must not refresh"

        monkeypatch.setattr(research_sync, "refresh_readonly_replica", spy_refresh)

        report = sync_from_backup(backup_db, local_db, refresh_replica=True)

        with DuckDBStore(local_db) as local:
            snapshot_after = local.get_dataset_snapshot(snapshot.snapshot_id)
            coverage_after = local.list_dataset_coverages(snapshot.snapshot_id)
            issue_count = local._conn.execute(
                "SELECT COUNT(*) FROM data_quality_issue WHERE issue_id = ?",
                [invalid_issue.issue_id],
            ).fetchone()[0]
        results = {result.table: result for result in report.tables}
        assert report.has_errors
        assert refresh_calls == 0
        assert not report.replica_refreshed
        assert results["data_quality_issue"].mode == "error"
        assert results["dataset_snapshot"].mode == "skipped"
        assert results["dataset_coverage"].mode == "skipped"
        assert snapshot_after is None
        assert coverage_after == []
        assert issue_count == 0

    def test_ready_coverage_created_at_difference_preserves_target_write_once(
        self, local_db: Path, backup_db: Path
    ) -> None:
        t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
        snapshot = DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=t0,
            code_commit="abc123",
            origin="shared",
            created_at=t0,
        )
        finalization = DatasetSnapshotFinalization(completed_at=t0 + timedelta(minutes=2))
        target_coverage = DatasetCoverage(
            snapshot_id=snapshot.snapshot_id,
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=10,
            available_count=9,
            missing_reasons=("one missing",),
            created_at=t0 + timedelta(minutes=1),
        )
        source_coverage = target_coverage.model_copy(update={"created_at": t0})
        with DuckDBStore(local_db) as local:
            local.begin_dataset_snapshot(snapshot)
            local.upsert_dataset_coverage(target_coverage)
            local.finalize_dataset_snapshot(snapshot.snapshot_id, finalization)
        with DuckDBStore(backup_db) as backup:
            backup.begin_dataset_snapshot(snapshot)
            backup.upsert_dataset_coverage(source_coverage)
            backup.finalize_dataset_snapshot(snapshot.snapshot_id, finalization)

        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        with DuckDBStore(local_db) as local:
            stored = local.list_dataset_coverages(snapshot.snapshot_id)
        by_table = {result.table: result for result in report.tables}
        assert not report.has_errors
        assert stored == [target_coverage]
        assert by_table["dataset_snapshot"].rows == 0
        assert by_table["dataset_coverage"].rows == 0


class TestRestoreResearchTables:
    def test_uses_shared_schema_initializer(
        self,
        tmp_path: Path,
        local_db: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        source = tmp_path / "source.duckdb"
        DuckDBStore(source).close()
        calls: list[duckdb.DuckDBPyConnection] = []
        real_initialize_schema = research_sync.initialize_schema

        def spy_initialize_schema(conn: duckdb.DuckDBPyConnection) -> None:
            calls.append(conn)
            real_initialize_schema(conn)

        monkeypatch.setattr(
            research_sync, "initialize_schema", spy_initialize_schema
        )

        report = restore_research_tables(
            source,
            local_db,
            tables=["minute_bar"],
            refresh_replica=False,
        )

        assert not report.has_errors
        assert len(calls) == 1

    def test_restore_merges_research_only(
        self, tmp_path: Path, local_db: Path
    ) -> None:
        old_replica = tmp_path / "rquant_ro.duckdb"
        store = DuckDBStore(old_replica)
        store._conn.execute(
            "INSERT INTO minute_bar (ts_code, trade_time, freq, open, high, low, close, source) "
            "VALUES ('300750.SZ', TIMESTAMP '2026-05-20 09:31:00', '1min', 5, 6, 5, 6, 'tushare')"
        )
        store._conn.execute(
            "INSERT INTO auction_bar (ts_code, trade_date, auction_type, price, source) "
            "VALUES ('300750.SZ', DATE '2026-05-20', 'open_realtime', 5.5, 'tushare')"
        )
        store.close()

        report = restore_research_tables(
            old_replica, local_db, refresh_replica=False
        )
        assert not report.has_errors

        conn = duckdb.connect(str(local_db), read_only=True)
        assert conn.execute("SELECT COUNT(*) FROM minute_bar").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM auction_bar").fetchone()[0] == 1
        conn.close()

    def test_restore_rejects_production_table(
        self, tmp_path: Path, local_db: Path
    ) -> None:
        src = tmp_path / "src.duckdb"
        _make_backup_db(src)
        with pytest.raises(ValueError, match="screen_result"):
            restore_research_tables(src, local_db, tables=["screen_result"])

    def test_restore_explicit_metadata_table_is_disallowed(
        self, tmp_path: Path, local_db: Path
    ) -> None:
        source = tmp_path / "metadata-source.duckdb"
        DuckDBStore(source).close()

        with pytest.raises(ValueError, match="metadata|bundle"):
            restore_research_tables(
                source,
                local_db,
                tables=["dataset_snapshot"],
                refresh_replica=False,
            )

    def test_restore_default_skips_linked_metadata_bundle(
        self, tmp_path: Path, local_db: Path
    ) -> None:
        source = tmp_path / "default-source.duckdb"
        DuckDBStore(source).close()

        report = restore_research_tables(
            source,
            local_db,
            refresh_replica=False,
        )

        metadata_tables = {
            "dataset_snapshot",
            "dataset_coverage",
            "data_quality_issue",
        }
        assert not report.has_errors
        assert metadata_tables.isdisjoint(
            result.table for result in report.tables
        )

    def test_restore_keeps_local_row_on_pk_conflict(
        self, tmp_path: Path, local_db: Path
    ) -> None:
        """A2：restore 用 INSERT OR IGNORE——旧副本的过期 open 状态
        不能复活覆盖本地今天已平仓的 paper_position；缺失行照常补入。"""
        _insert_paper_position(local_db, "pos-1", "closed")

        old_replica = tmp_path / "old_replica.duckdb"
        DuckDBStore(old_replica).close()
        _insert_paper_position(old_replica, "pos-1", "open")
        _insert_paper_position(old_replica, "pos-2", "open")

        report = restore_research_tables(
            old_replica, local_db, tables=["paper_position"], refresh_replica=False
        )
        assert not report.has_errors
        by_table = {t.table: t for t in report.tables}
        assert "restore" in by_table["paper_position"].detail

        conn = duckdb.connect(str(local_db), read_only=True)
        rows = dict(
            conn.execute("SELECT position_id, status FROM paper_position").fetchall()
        )
        conn.close()
        # 冲突行保留本地新值（closed），本地缺失的 pos-2 被补入
        assert rows == {"pos-1": "closed", "pos-2": "open"}

    def test_restore_suspension_snapshot_is_an_atomic_newer_wins_bundle(
        self,
        tmp_path: Path,
        local_db: Path,
    ) -> None:
        source = tmp_path / "suspension-source.duckdb"
        _write_suspension_snapshot(
            local_db,
            queried_at=datetime(2026, 7, 14, 8, tzinfo=UTC),
            suspended=True,
        )
        _write_suspension_snapshot(
            source,
            queried_at=datetime(2026, 7, 14, 9, tzinfo=UTC),
            suspended=False,
        )

        report = restore_research_tables(
            source,
            local_db,
            tables=["stock_suspend_event", "stock_suspend_coverage"],
            refresh_replica=False,
        )

        assert not report.has_errors
        with DuckDBStore(local_db, read_only=True) as store:
            coverage = store._conn.execute(  # noqa: SLF001
                "SELECT row_count, snapshot_hash FROM stock_suspend_coverage"
            ).fetchone()
            event_count = store._conn.execute(  # noqa: SLF001
                "SELECT COUNT(*) FROM stock_suspend_event"
            ).fetchone()[0]
        assert coverage is not None and coverage[0] == 0
        assert event_count == 0

    def test_restore_rejects_partial_suspension_snapshot_bundle(
        self,
        tmp_path: Path,
        local_db: Path,
    ) -> None:
        source = tmp_path / "partial-suspension-source.duckdb"
        DuckDBStore(source).close()

        with pytest.raises(ValueError, match="suspension.*bundle"):
            restore_research_tables(
                source,
                local_db,
                tables=["stock_suspend_event"],
                refresh_replica=False,
            )

    def test_restore_source_path_with_single_quote(
        self, tmp_path: Path, local_db: Path
    ) -> None:
        """A1：restore 源路径含单引号（如 roxor's backup）正常工作。"""
        qdir = tmp_path / "roxor's backup"
        qdir.mkdir()
        src = qdir / "old.duckdb"
        store = DuckDBStore(src)
        store._conn.execute(
            "INSERT INTO minute_bar (ts_code, trade_time, freq, open, high, low, close, source) "
            "VALUES ('300750.SZ', TIMESTAMP '2026-05-20 09:31:00', '1min', 5, 6, 5, 6, 'tushare')"
        )
        store.close()

        report = restore_research_tables(src, local_db, refresh_replica=False)
        assert not report.has_errors

        conn = duckdb.connect(str(local_db), read_only=True)
        assert conn.execute("SELECT COUNT(*) FROM minute_bar").fetchone()[0] == 3
        conn.close()

    def test_restore_source_missing_reports_error_not_raise(
        self, tmp_path: Path, local_db: Path
    ) -> None:
        """A7：恢复源缺失转报告返回，不抛 FileNotFoundError。"""
        report = restore_research_tables(
            tmp_path / "nope.duckdb", local_db, refresh_replica=False
        )
        assert report.has_errors
        assert "不存在" in report.tables[0].detail

    def test_restore_replica_skipped_when_table_error(
        self, tmp_path: Path, local_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A3：restore 部分表失败时同样跳过副本刷新。"""
        src = tmp_path / "src.duckdb"
        DuckDBStore(src).close()

        def always_error(conn, table, alias, mode):  # noqa: ANN001
            return TableSyncResult(table=table, mode="error", detail="boom")

        calls = {"n": 0}

        def spy_refresh(db_path=None, replica_path=None):  # noqa: ANN001
            calls["n"] += 1
            return True, "不应被调用"

        monkeypatch.setattr(research_sync, "_sync_table", always_error)
        monkeypatch.setattr(research_sync, "refresh_readonly_replica", spy_refresh)

        report = restore_research_tables(src, local_db, refresh_replica=True)
        assert report.has_errors
        assert calls["n"] == 0
        assert not report.replica_refreshed
        assert "跳过副本刷新" in report.replica_detail


class TestReplicaRefresh:
    def test_refresh_creates_openable_replica(
        self, local_db: Path, tmp_path: Path
    ) -> None:
        replica = tmp_path / "rquant_ro.duckdb"
        ok, detail = refresh_readonly_replica(local_db, replica)
        assert ok, detail

        generation = json.loads(
            Path(f"{replica}.generation.json").read_text(encoding="utf-8")
        )
        assert generation["schema_version"] == 1
        assert generation["source_database"] == str(local_db.resolve())
        assert generation["source_before"] == generation["source_after"]

        conn = duckdb.connect(str(replica), read_only=True)
        assert conn.execute("SELECT COUNT(*) FROM minute_bar").fetchone()[0] == 2
        conn.close()

    def test_generation_write_failure_keeps_previous_replica_and_sidecar(
        self,
        local_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        replica = tmp_path / "rquant_ro.duckdb"
        ok, detail = refresh_readonly_replica(local_db, replica)
        assert ok, detail
        previous_replica = replica.read_bytes()
        generation_path = Path(f"{replica}.generation.json")
        previous_generation = generation_path.read_bytes()

        local = duckdb.connect(str(local_db))
        local.execute(
            "INSERT INTO minute_bar "
            "(ts_code, trade_time, freq, open, high, low, close, source) "
            "VALUES ('000001.SZ', TIMESTAMP '2026-07-13 09:33:00', "
            "'1min', 1, 1, 1, 1, 'test')"
        )
        local.close()

        def fail_generation_write(*args: Any, **kwargs: Any) -> None:
            raise OSError("generation write failed")

        monkeypatch.setattr(
            research_sync,
            "write_replica_generation_metadata",
            fail_generation_write,
        )

        ok, detail = refresh_readonly_replica(local_db, replica)

        assert not ok
        assert "generation write failed" in detail
        assert replica.read_bytes() == previous_replica
        assert generation_path.read_bytes() == previous_generation

    def test_refresh_refuses_when_wal_present(
        self, local_db: Path, tmp_path: Path
    ) -> None:
        wal = local_db.with_name(local_db.name + ".wal")
        wal.write_bytes(b"pretend active wal")
        try:
            ok, detail = refresh_readonly_replica(
                local_db, tmp_path / "rquant_ro.duckdb"
            )
            assert not ok
            assert "WAL" in detail
        finally:
            wal.unlink()

    def test_refresh_blocks_writer_during_copy_and_keeps_previous_replica_on_failure(
        self,
        local_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        replica = tmp_path / "rquant_ro.duckdb"
        ok, detail = refresh_readonly_replica(local_db, replica)
        assert ok, detail
        writer_blocked = False

        def interrupted_copy(src: Path, dst: Path) -> None:
            nonlocal writer_blocked
            try:
                writer = duckdb.connect(str(src))
            except duckdb.Error:
                writer_blocked = True
            else:
                try:
                    writer.execute(
                        "INSERT INTO daily_bar (ts_code, trade_date, close) "
                        "VALUES ('000001.SZ', DATE '2026-07-13', 12.0)"
                    )
                finally:
                    writer.close()
            Path(dst).write_bytes(b"partial replica")
            raise OSError("copy interrupted")

        monkeypatch.setattr(research_sync.shutil, "copy2", interrupted_copy)

        ok, detail = refresh_readonly_replica(local_db, replica)

        assert not ok
        assert "copy interrupted" in detail
        assert writer_blocked
        assert not replica.with_name(replica.name + ".sync-tmp").exists()
        conn = duckdb.connect(str(replica), read_only=True)
        assert conn.execute("SELECT COUNT(*) FROM minute_bar").fetchone()[0] == 2
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM daily_bar "
                "WHERE ts_code = '000001.SZ' AND trade_date = DATE '2026-07-13'"
            ).fetchone()[0]
            == 0
        )
        conn.close()

    def test_verify_close_failure_keeps_previous_replica(
        self,
        local_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        replica = tmp_path / "rquant_ro.duckdb"
        ok, detail = refresh_readonly_replica(local_db, replica)
        assert ok, detail
        local = duckdb.connect(str(local_db))
        local.execute(
            "INSERT INTO minute_bar "
            "(ts_code, trade_time, freq, open, high, low, close, source) "
            "VALUES ('000001.SZ', TIMESTAMP '2026-07-13 09:33:00', "
            "'1min', 1, 1, 1, 1, 'test')"
        )
        local.close()
        real_connect = duckdb.connect
        tmp_replica = replica.with_name(replica.name + ".sync-tmp")

        def connect_with_verify_close_failure(
            database: str | Path,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            connection = real_connect(str(database), *args, **kwargs)
            if Path(database) == tmp_replica:
                return _CloseFailingConnection(
                    connection,
                    "verify close failed",
                )
            return connection

        monkeypatch.setattr(
            research_sync.duckdb,
            "connect",
            connect_with_verify_close_failure,
        )

        ok, detail = refresh_readonly_replica(local_db, replica)

        assert not ok
        assert "verify close failed" in detail
        assert not tmp_replica.exists()
        check = real_connect(str(replica), read_only=True)
        assert check.execute("SELECT COUNT(*) FROM minute_bar").fetchone()[0] == 2
        check.close()

    def test_guard_close_failure_keeps_previous_replica(
        self,
        local_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        replica = tmp_path / "rquant_ro.duckdb"
        ok, detail = refresh_readonly_replica(local_db, replica)
        assert ok, detail
        local = duckdb.connect(str(local_db))
        local.execute(
            "INSERT INTO minute_bar "
            "(ts_code, trade_time, freq, open, high, low, close, source) "
            "VALUES ('000001.SZ', TIMESTAMP '2026-07-13 09:33:00', "
            "'1min', 1, 1, 1, 1, 'test')"
        )
        local.close()
        real_connect = duckdb.connect
        tmp_replica = replica.with_name(replica.name + ".sync-tmp")

        def connect_with_guard_close_failure(
            database: str | Path,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            connection = real_connect(str(database), *args, **kwargs)
            if Path(database) == local_db and kwargs.get("read_only") is True:
                return _CloseFailingConnection(
                    connection,
                    "guard close failed",
                )
            return connection

        monkeypatch.setattr(
            research_sync.duckdb,
            "connect",
            connect_with_guard_close_failure,
        )

        ok, detail = refresh_readonly_replica(local_db, replica)

        assert not ok
        assert "guard close failed" in detail
        assert not tmp_replica.exists()
        check = real_connect(str(replica), read_only=True)
        assert check.execute("SELECT COUNT(*) FROM minute_bar").fetchone()[0] == 2
        check.close()


class TestStaleWalRescue:
    def test_rescue_renames_wal_and_retries(
        self, local_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        wal = local_db.with_name(local_db.name + ".wal")
        wal.write_bytes(b"stale generation wal")

        real_connect = duckdb.connect
        calls = {"n": 0}

        def fake_connect(path: str, *args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise duckdb.InternalException(
                    'INTERNAL Error: Failure while replaying WAL file "x.wal"'
                )
            return real_connect(path, *args, **kwargs)

        monkeypatch.setattr("rquant.research_sync.duckdb.connect", fake_connect)
        conn = _rescue_stale_wal(local_db)
        conn.close()

        assert not wal.exists()
        baks = list(local_db.parent.glob("*.wal.corrupt-*.bak"))
        assert len(baks) == 1

    def test_non_wal_error_propagates(
        self, local_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_connect(path: str, *args, **kwargs):
            raise duckdb.IOException("Could not set lock on file")

        monkeypatch.setattr("rquant.research_sync.duckdb.connect", fake_connect)
        with pytest.raises(duckdb.IOException):
            _rescue_stale_wal(local_db)


def test_table_classification_complete(tmp_path: Path) -> None:
    """新表进 schema 时强制在 replace、merge、local-only 中表态。"""
    store = DuckDBStore(tmp_path / "classification.duckdb")
    schema_tables = {
        row[0]
        for row in store._conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    store.close()
    replace = set(REPLACE_TABLES)
    merge = set(MERGE_TABLES)
    local_only = set(LOCAL_ONLY_TABLES)

    assert not replace & merge
    assert not replace & local_only
    assert not merge & local_only
    assert replace | merge | local_only == schema_tables


def test_schema_migration_is_local_only() -> None:
    assert "schema_migration" in LOCAL_ONLY_TABLES
    assert "schema_migration" not in REPLACE_TABLES
    assert "schema_migration" not in MERGE_TABLES


def test_data_metadata_tables_use_merge_semantics() -> None:
    metadata_tables = {
        "dataset_snapshot",
        "dataset_snapshot_binding",
        "dataset_coverage",
        "data_quality_issue",
    }

    assert metadata_tables <= set(MERGE_TABLES)
    assert not metadata_tables & set(REPLACE_TABLES)
    assert not metadata_tables & set(LOCAL_ONLY_TABLES)


def test_ready_snapshot_binding_syncs_atomically_and_idempotently(
    tmp_path: Path,
) -> None:
    local_path = tmp_path / "local-binding.duckdb"
    backup_path = tmp_path / "backup-binding.duckdb"
    t0 = datetime(2026, 7, 15, 8, tzinfo=UTC)
    snapshot = DatasetSnapshot.create(
        strategy_name="growth_board_surge",
        manifest_id="m" * 64,
        as_of_time=t0,
        code_commit="1" * 40,
        origin="test",
        created_at=t0,
    )
    manifest = DatasetSnapshotBindingManifest(
        snapshot_id=snapshot.snapshot_id,
        strategy_name=snapshot.strategy_name,
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        as_of_time=snapshot.as_of_time,
        code_commit=snapshot.code_commit,
        dependency_contract_version="stage1-v1",
        builder_version="snapshot-builder-v1",
        artifacts=(
            DatasetSnapshotArtifact(
                artifact_type="lake_partition",
                dataset_id="minute_bar",
                table_name="minute_bar",
                artifact_key="minute_bar:2026-07-14:1min",
                relative_path="minute/versions/" + "a" * 64 + ".parquet",
                row_count=241,
                schema_hash="b" * 64,
                content_hash="c" * 64,
                file_hash="a" * 64,
            ),
        ),
    )
    binding = DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root="research_lake",
        manifest_relative_path=(
            f"snapshots/{snapshot.snapshot_id}/{manifest.manifest_hash}/manifest.json"
        ),
        created_at=t0,
    )

    with DuckDBStore(local_path):
        pass
    with DuckDBStore(backup_path) as backup:
        backup.begin_dataset_snapshot(snapshot)
        backup.finalize_dataset_snapshot(
            snapshot.snapshot_id,
            DatasetSnapshotFinalization(completed_at=t0 + timedelta(minutes=1)),
        )
        backup.begin_dataset_snapshot_binding(binding)
        ready = backup.finalize_dataset_snapshot_binding(
            snapshot.snapshot_id,
            DatasetSnapshotBindingFinalization(
                completed_at=t0 + timedelta(minutes=2)
            ),
        )

    first = sync_from_backup(backup_path, local_path, refresh_replica=False)
    second = sync_from_backup(backup_path, local_path, refresh_replica=False)

    with DuckDBStore(local_path) as local:
        stored = local.get_dataset_snapshot_binding(snapshot.snapshot_id)
    assert not first.has_errors
    assert not second.has_errors
    assert stored == ready
    assert next(
        item for item in first.tables if item.table == "dataset_snapshot_binding"
    ).rows == 1
    assert next(
        item for item in second.tables if item.table == "dataset_snapshot_binding"
    ).rows == 0


def test_legacy_metadata_bundle_without_binding_table_still_syncs(
    tmp_path: Path,
) -> None:
    local_path = tmp_path / "local-legacy-metadata.duckdb"
    backup_path = tmp_path / "backup-legacy-metadata.duckdb"
    t0 = datetime(2026, 7, 15, 8, tzinfo=UTC)
    snapshot = DatasetSnapshot.create(
        strategy_name="growth_board_surge",
        manifest_id="m" * 64,
        as_of_time=t0,
        code_commit="1" * 40,
        origin="legacy-test",
        created_at=t0,
    )

    with DuckDBStore(local_path):
        pass
    with DuckDBStore(backup_path) as backup:
        backup.begin_dataset_snapshot(snapshot)
        backup.finalize_dataset_snapshot(
            snapshot.snapshot_id,
            DatasetSnapshotFinalization(completed_at=t0 + timedelta(minutes=1)),
        )
        backup._conn.execute("DROP TABLE dataset_snapshot_binding")

    report = sync_from_backup(
        backup_path,
        local_path,
        refresh_replica=False,
    )

    with DuckDBStore(local_path) as local:
        stored = local.get_dataset_snapshot(snapshot.snapshot_id)
    binding_result = next(
        item for item in report.tables
        if item.table == "dataset_snapshot_binding"
    )
    assert not report.has_errors
    assert stored is not None
    assert stored.status == "ready"
    assert binding_result.mode == "skipped"
    assert binding_result.rows == 0
    assert "legacy" in binding_result.detail


def test_suspension_facts_merge_but_audit_runs_stay_local() -> None:
    suspension_tables = {
        "stock_suspend_event",
        "stock_suspend_coverage",
    }

    assert suspension_tables <= set(MERGE_TABLES)
    assert not suspension_tables & set(REPLACE_TABLES)
    assert "data_audit_run" in LOCAL_ONLY_TABLES
    assert "data_audit_run" not in MERGE_TABLES
    assert "data_audit_run" not in REPLACE_TABLES


def _write_suspension_snapshot(
    db_path: Path,
    *,
    queried_at: datetime,
    suspended: bool,
) -> None:
    trade_date = date(2026, 7, 14)
    frame = (
        pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260714",
                    "suspend_timing": "全天",
                    "suspend_type": "S",
                }
            ]
        )
        if suspended
        else pd.DataFrame()
    )
    with DuckDBStore(db_path) as store:
        persist_suspension_snapshot(
            store,
            normalize_suspend_d_snapshot(
                frame,
                trade_date=trade_date,
                queried_at=queried_at,
            ),
        )


def test_newer_cloud_suspension_snapshot_replaces_events_atomically(
    local_db: Path,
    backup_db: Path,
) -> None:
    _write_suspension_snapshot(
        local_db,
        queried_at=datetime(2026, 7, 14, 8, tzinfo=UTC),
        suspended=True,
    )
    _write_suspension_snapshot(
        backup_db,
        queried_at=datetime(2026, 7, 14, 9, tzinfo=UTC),
        suspended=False,
    )

    report = sync_from_backup(backup_db, local_db, refresh_replica=False)

    assert not report.has_errors
    with DuckDBStore(local_db, read_only=True) as store:
        known = store.known_full_day_suspensions(
            ("600000.SH",),
            date(2026, 7, 14),
            date(2026, 7, 14),
        )
        coverage = store._conn.execute(  # noqa: SLF001
            """
            SELECT row_count,
                   strftime(queried_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ')
            FROM stock_suspend_coverage
            """
        ).fetchone()
    assert known == set()
    assert coverage == (0, "2026-07-14T09:00:00Z")


def test_older_cloud_suspension_snapshot_cannot_overwrite_local(
    local_db: Path,
    backup_db: Path,
) -> None:
    _write_suspension_snapshot(
        local_db,
        queried_at=datetime(2026, 7, 14, 10, tzinfo=UTC),
        suspended=True,
    )
    _write_suspension_snapshot(
        backup_db,
        queried_at=datetime(2026, 7, 14, 9, tzinfo=UTC),
        suspended=False,
    )

    report = sync_from_backup(backup_db, local_db, refresh_replica=False)

    assert not report.has_errors
    with DuckDBStore(local_db, read_only=True) as store:
        known = store.known_full_day_suspensions(
            ("600000.SH",),
            date(2026, 7, 14),
            date(2026, 7, 14),
        )
        queried_at = store._conn.execute(  # noqa: SLF001
            """
            SELECT strftime(
                queried_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ'
            )
            FROM stock_suspend_coverage
            """
        ).fetchone()[0]
    assert known == {("600000.SH", date(2026, 7, 14))}
    assert queried_at == "2026-07-14T10:00:00Z"


def test_trade_calendar_uses_merge_semantics() -> None:
    assert "trade_calendar" in MERGE_TABLES
    assert "trade_calendar" not in REPLACE_TABLES
    assert "trade_calendar" not in LOCAL_ONLY_TABLES


def test_stock_status_daily_uses_merge_semantics() -> None:
    assert "stock_status_daily" in MERGE_TABLES
    assert "stock_status_daily" not in REPLACE_TABLES
    assert "stock_status_daily" not in LOCAL_ONLY_TABLES


def test_stock_status_merge_keeps_local_history_and_newer_cloud_wins(
    local_db: Path, backup_db: Path
) -> None:
    local_conn = duckdb.connect(str(local_db))
    local_conn.execute(
        "INSERT INTO stock_status_daily VALUES "
        "('600000.SH', DATE '2020-01-02', 'local-history', FALSE, "
        "'tushare.namechange', 'tushare.namechange', "
        "TIMESTAMPTZ '2020-01-02 01:25:00+00', "
        "TIMESTAMPTZ '2026-07-14 00:00:00+00', NULL), "
        "('600000.SH', DATE '2026-07-01', 'stale-local', FALSE, "
        "'tushare.namechange', 'tushare.namechange', "
        "TIMESTAMPTZ '2026-07-01 01:25:00+00', "
        "TIMESTAMPTZ '2026-07-14 00:00:00+00', NULL)"
    )
    local_conn.close()
    backup_conn = duckdb.connect(str(backup_db))
    backup_conn.execute(
        "INSERT INTO stock_status_daily VALUES "
        "('600000.SH', DATE '2026-07-01', '*STcloud', TRUE, "
        "'tushare.namechange', 'tushare.namechange+tushare.stock_st', "
        "TIMESTAMPTZ '2026-07-01 01:25:00+00', "
        "TIMESTAMPTZ '2026-07-14 01:00:00+00', NULL)"
    )
    backup_conn.close()

    report = sync_from_backup(backup_db, local_db, refresh_replica=False)

    assert not report.has_errors
    conn = duckdb.connect(str(local_db), read_only=True)
    rows = conn.execute(
        "SELECT trade_date, name, is_st FROM stock_status_daily ORDER BY trade_date"
    ).fetchall()
    conn.close()
    assert rows == [
        (date(2020, 1, 2), "local-history", False),
        (date(2026, 7, 1), "*STcloud", True),
    ]


def test_stock_status_merge_preserves_newer_local_fact(
    local_db: Path, backup_db: Path
) -> None:
    newer = datetime(2026, 7, 14, 2, tzinfo=UTC)
    _insert_stock_status_row(
        local_db,
        name="local-newer",
        is_st=False,
        ingested_at=newer,
    )
    _insert_stock_status_row(
        backup_db,
        name="*STcloud-older",
        is_st=True,
        ingested_at=newer - timedelta(hours=1),
    )

    report = sync_from_backup(backup_db, local_db, refresh_replica=False)

    conn = duckdb.connect(str(local_db), read_only=True)
    row = conn.execute(
        "SELECT name, is_st FROM stock_status_daily "
        "WHERE ts_code = '600000.SH' AND trade_date = DATE '2026-07-01'"
    ).fetchone()
    conn.close()
    assert not report.has_errors
    assert row == ("local-newer", False)


def test_stock_status_merge_equal_time_identical_fact_is_idempotent(
    local_db: Path, backup_db: Path
) -> None:
    observed_at = datetime(2026, 7, 14, 2, tzinfo=UTC)
    for db_path in (local_db, backup_db):
        _insert_stock_status_row(
            db_path,
            name="same-fact",
            is_st=False,
            ingested_at=observed_at,
        )

    first = sync_from_backup(backup_db, local_db, refresh_replica=False)
    second = sync_from_backup(backup_db, local_db, refresh_replica=False)

    conn = duckdb.connect(str(local_db), read_only=True)
    rows = conn.execute(
        "SELECT name, is_st FROM stock_status_daily "
        "WHERE ts_code = '600000.SH' AND trade_date = DATE '2026-07-01'"
    ).fetchall()
    conn.close()
    assert not first.has_errors
    assert not second.has_errors
    assert rows == [("same-fact", False)]


def test_stock_status_equal_time_conflict_rolls_back_entire_sync(
    local_db: Path, backup_db: Path
) -> None:
    observed_at = datetime(2026, 7, 14, 2, tzinfo=UTC)
    _insert_stock_status_row(
        local_db,
        name="local-fact",
        is_st=False,
        ingested_at=observed_at,
    )
    _insert_stock_status_row(
        backup_db,
        name="*STcloud-conflict",
        is_st=True,
        ingested_at=observed_at,
    )

    report = sync_from_backup(backup_db, local_db, refresh_replica=False)

    conn = duckdb.connect(str(local_db), read_only=True)
    status = conn.execute(
        "SELECT name, is_st FROM stock_status_daily "
        "WHERE ts_code = '600000.SH' AND trade_date = DATE '2026-07-01'"
    ).fetchone()
    daily_dates = conn.execute(
        "SELECT DISTINCT trade_date FROM daily_bar ORDER BY trade_date"
    ).fetchall()
    conn.close()
    assert report.has_errors
    assert status == ("local-fact", False)
    assert daily_dates == [(date(2026, 6, 1),)]
    stock_result = next(
        result for result in report.tables if result.table == "stock_status_daily"
    )
    assert stock_result.mode == "error"
    assert "equal ingested_at" in stock_result.detail


def test_trade_calendar_merge_keeps_local_history_and_cloud_wins(
    local_db: Path, backup_db: Path
) -> None:
    local_conn = duckdb.connect(str(local_db))
    local_conn.execute(
        "INSERT INTO trade_calendar VALUES "
        "('SSE', DATE '2026-06-01', TRUE, DATE '2026-05-29', 'local', "
        "TIMESTAMPTZ '2026-06-01 00:00:00+00'), "
        "('SSE', DATE '2026-07-01', FALSE, DATE '2026-06-30', 'stale', "
        "TIMESTAMPTZ '2026-07-01 00:00:00+00')"
    )
    local_conn.close()
    backup_conn = duckdb.connect(str(backup_db))
    backup_conn.execute(
        "INSERT INTO trade_calendar VALUES "
        "('SSE', DATE '2026-07-01', TRUE, DATE '2026-06-30', 'tushare', "
        "TIMESTAMPTZ '2026-07-01 01:00:00+00')"
    )
    backup_conn.close()

    report = sync_from_backup(backup_db, local_db, refresh_replica=False)

    conn = duckdb.connect(str(local_db), read_only=True)
    rows = conn.execute(
        "SELECT cal_date, is_open, source FROM trade_calendar ORDER BY cal_date"
    ).fetchall()
    conn.close()
    assert not report.has_errors
    assert rows == [
        (date(2026, 6, 1), True, "local"),
        (date(2026, 7, 1), True, "tushare"),
    ]


def test_trade_calendar_merge_preserves_newer_target_row(
    local_db: Path, backup_db: Path
) -> None:
    newer_time = datetime(2026, 7, 1, 2, 0, tzinfo=UTC)
    older_time = newer_time - timedelta(hours=1)
    _insert_trade_calendar_row(
        local_db,
        cal_date=date(2026, 7, 1),
        is_open=True,
        pretrade_date=date(2026, 6, 30),
        source="local-newer",
        updated_at=newer_time,
    )
    _insert_trade_calendar_row(
        backup_db,
        cal_date=date(2026, 7, 1),
        is_open=False,
        pretrade_date=date(2026, 6, 27),
        source="cloud-older",
        updated_at=older_time,
    )

    report = sync_from_backup(backup_db, local_db, refresh_replica=False)

    with DuckDBStore(local_db) as store:
        row = store.get_trade_calendar_day("SSE", date(2026, 7, 1))
    assert not report.has_errors
    assert row is not None
    assert row.is_open is True
    assert row.pretrade_date == date(2026, 6, 30)
    assert row.source == "local-newer"
    assert row.updated_at == newer_time


def test_trade_calendar_merge_equal_time_identical_facts_is_idempotent(
    local_db: Path, backup_db: Path
) -> None:
    observed_at = datetime(2026, 7, 1, 2, 0, tzinfo=UTC)
    for db_path, source in (
        (local_db, "local-provenance"),
        (backup_db, "cloud-provenance"),
    ):
        _insert_trade_calendar_row(
            db_path,
            cal_date=date(2026, 7, 1),
            is_open=True,
            pretrade_date=date(2026, 6, 30),
            source=source,
            updated_at=observed_at,
        )

    report = sync_from_backup(backup_db, local_db, refresh_replica=False)

    with DuckDBStore(local_db) as store:
        row = store.get_trade_calendar_day("SSE", date(2026, 7, 1))
    assert not report.has_errors
    assert row is not None
    assert row.source == "local-provenance"
    assert row.updated_at == observed_at


def test_trade_calendar_merge_inserts_source_only_row(
    local_db: Path, backup_db: Path
) -> None:
    observed_at = datetime(2026, 7, 2, 2, 0, tzinfo=UTC)
    _insert_trade_calendar_row(
        backup_db,
        cal_date=date(2026, 7, 2),
        is_open=True,
        pretrade_date=date(2026, 7, 1),
        source="cloud-only",
        updated_at=observed_at,
    )

    report = sync_from_backup(backup_db, local_db, refresh_replica=False)

    with DuckDBStore(local_db) as store:
        row = store.get_trade_calendar_day("SSE", date(2026, 7, 2))
    assert not report.has_errors
    assert row is not None
    assert row.source == "cloud-only"
    assert row.updated_at == observed_at


def test_trade_calendar_equal_time_conflict_rolls_back_and_skips_replica(
    local_db: Path,
    backup_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 7, 1, 2, 0, tzinfo=UTC)
    _insert_trade_calendar_row(
        local_db,
        cal_date=date(2026, 7, 1),
        is_open=True,
        pretrade_date=date(2026, 6, 30),
        source="local",
        updated_at=observed_at,
    )
    _insert_trade_calendar_row(
        backup_db,
        cal_date=date(2026, 7, 1),
        is_open=False,
        pretrade_date=date(2026, 6, 30),
        source="conflict",
        updated_at=observed_at,
    )
    _insert_trade_calendar_row(
        backup_db,
        cal_date=date(2026, 7, 2),
        is_open=True,
        pretrade_date=date(2026, 7, 1),
        source="source-only",
        updated_at=observed_at + timedelta(days=1),
    )
    replica_calls = 0

    def spy_refresh(db_path: Path) -> tuple[bool, str]:
        nonlocal replica_calls
        replica_calls += 1
        return True, "unexpected"

    monkeypatch.setattr(research_sync, "refresh_readonly_replica", spy_refresh)

    report = sync_from_backup(backup_db, local_db, refresh_replica=True)

    with DuckDBStore(local_db) as store:
        rows = store.list_trade_calendar(
            "SSE", date(2026, 7, 1), date(2026, 7, 2)
        )
    result = next(item for item in report.tables if item.table == "trade_calendar")
    assert report.has_errors
    assert result.mode == "error"
    assert "equal updated_at" in result.detail
    assert len(rows) == 1
    assert rows[0].is_open is True
    assert rows[0].source == "local"
    assert rows[0].cal_date == date(2026, 7, 1)
    assert replica_calls == 0
    assert not report.replica_refreshed
    assert "跳过副本刷新" in report.replica_detail


def test_backfilled_history_tables_are_merge() -> None:
    """防回归：这些表若回到 REPLACE，下一次 research-sync 会把本地回补的
    2020-2024 历史（及本地独有的涨停池采集、Tushare 涨跌停榜、统一数据集
    回补层的所有表）整表抹掉。"""
    must_merge = (
        "daily_bar",
        "daily_basic",
        "adj_factor",
        "daily_state",
        "daily_indicator",
        "stock_status_daily",
        "limit_up_pool_daily",
        "limit_list_daily",
        # dataset_backfill 数据集表（本地回补权威，云端没有）
        "ths_index_daily",
        "dc_index_daily",
        "ths_board",
        "dc_board",
        "ths_board_member",
        "dc_board_member",
        "moneyflow_daily",
        "moneyflow_dc_daily",
        "moneyflow_ths_daily",
        "moneyflow_ind_ths_daily",
        "moneyflow_ind_dc_daily",
        "moneyflow_cnt_ths_daily",
        "moneyflow_mkt_daily",
        "top_list_daily",
        "top_inst_daily",
        "kpl_list_daily",
        "kpl_concept_member",
        "kpl_concept_member_daily",
        "market_daily_info",
        "hm_list",
        "index_daily_bar",
    )
    for table in must_merge:
        assert table in MERGE_TABLES, f"{table} 必须是 merge 语义"
        assert table not in REPLACE_TABLES, f"{table} 不允许整表替换"


@pytest.mark.parametrize("mode", ["merge", "restore"])
def test_limit_up_pool_research_sync_writer_blocks_concurrent_repair(
    tmp_path: Path,
    mode: str,
) -> None:
    target_path = tmp_path / f"target-{mode}.duckdb"
    source_path = tmp_path / f"source-{mode}.duckdb"
    closed = date(2026, 7, 12)

    with DuckDBStore(target_path) as target:
        target._conn.execute(
            "INSERT INTO trade_calendar "
            "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
            "VALUES ('SSE', ?, FALSE, NULL, 'test', ?)",
            [closed, datetime(2026, 7, 14, tzinfo=UTC)],
        )
        target._conn.execute(
            "INSERT INTO limit_up_pool_daily (ts_code, trade_date, source) "
            "VALUES ('600001.SH', ?, 'eastmoney')",
            [closed],
        )
    with DuckDBStore(source_path) as source:
        source._conn.execute(
            "INSERT INTO limit_up_pool_daily (ts_code, trade_date, source) "
            "VALUES ('000001.SZ', ?, 'eastmoney')",
            [closed],
        )

    with DuckDBStore(target_path) as repair_store:
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(
            repair_store
        )
        assert plan.plan_id is not None
        sync_conn = duckdb.connect(str(target_path))
        research_sync._attach_readonly(sync_conn, source_path, "sync_source")
        transaction_open = False
        try:
            sync_conn.execute("BEGIN")
            transaction_open = True
            result = research_sync._sync_table(
                sync_conn,
                "limit_up_pool_daily",
                "sync_source",
                mode,
                manage_transaction=False,
            )
            assert result.mode == "merge"

            with pytest.raises(duckdb.TransactionException, match="Conflict on"):
                data_quality.apply_limit_up_pool_closed_day_repair(
                    repair_store,
                    plan.plan_id,
                )

            sync_conn.execute("COMMIT")
            transaction_open = False
        finally:
            if transaction_open:
                sync_conn.execute("ROLLBACK")
            sync_conn.close()

    with DuckDBStore(target_path, read_only=True) as check:
        pool_rows = check._conn.execute(
            "SELECT ts_code FROM limit_up_pool_daily ORDER BY ts_code"
        ).fetchall()
        audit_count = check._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert pool_rows == [("000001.SZ",), ("600001.SH",)]
    assert audit_count == (0,)
