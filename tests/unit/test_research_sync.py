"""research_sync：云端备份合并 / 研究表恢复 / 副本刷新 / WAL 抢救。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

import rquant.research_sync as research_sync
from rquant.data_metadata import (
    DataQualityIssue,
    DatasetCoverage,
    DatasetSnapshot,
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

        def spy_refresh(db_path=None, replica_path=None):  # noqa: ANN001
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

    def test_sync_building_source_cannot_regress_ready_coverage(
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
        assert not report.has_errors
        assert stored == [target_coverage]

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
        assert stored_coverage == [source_coverage]

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

    def test_ready_coverage_created_at_difference_keeps_earliest(
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
        assert not report.has_errors
        assert stored == [source_coverage]


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

        conn = duckdb.connect(str(replica), read_only=True)
        assert conn.execute("SELECT COUNT(*) FROM minute_bar").fetchone()[0] == 2
        conn.close()

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
        "dataset_coverage",
        "data_quality_issue",
    }

    assert metadata_tables <= set(MERGE_TABLES)
    assert not metadata_tables & set(REPLACE_TABLES)
    assert not metadata_tables & set(LOCAL_ONLY_TABLES)


def test_trade_calendar_uses_merge_semantics() -> None:
    assert "trade_calendar" in MERGE_TABLES
    assert "trade_calendar" not in REPLACE_TABLES
    assert "trade_calendar" not in LOCAL_ONLY_TABLES


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
