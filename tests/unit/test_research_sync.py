"""research_sync：云端备份合并 / 研究表恢复 / 副本刷新 / WAL 抢救。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

import rquant.research_sync as research_sync
from rquant.research_sync import (
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


class TestSyncFromBackup:
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

        def flaky(conn, table, alias, mode):  # noqa: ANN001
            if table == "daily_bar":
                return TableSyncResult(table=table, mode="error", detail="boom")
            return real_sync_table(conn, table, alias, mode)

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


class TestRestoreResearchTables:
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


def test_table_classification_complete() -> None:
    """新表进 schema 时强制在这里表态：replace 还是 merge。"""
    overlap = set(REPLACE_TABLES) & set(MERGE_TABLES)
    assert not overlap, f"表不能同时出现在两类：{overlap}"


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
        "market_daily_info",
        "hm_list",
        "index_daily_bar",
    )
    for table in must_merge:
        assert table in MERGE_TABLES, f"{table} 必须是 merge 语义"
        assert table not in REPLACE_TABLES, f"{table} 不允许整表替换"
