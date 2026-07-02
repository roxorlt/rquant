"""research_sync：云端备份合并 / 研究表恢复 / 副本刷新 / WAL 抢救。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from rquant.research_sync import (
    MERGE_TABLES,
    REPLACE_TABLES,
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


class TestSyncFromBackup:
    def test_replace_and_merge(self, local_db: Path, backup_db: Path) -> None:
        report = sync_from_backup(backup_db, local_db, refresh_replica=False)

        conn = duckdb.connect(str(local_db), read_only=True)
        # replace 表：整表换成云端 2 行，旧 6-01 行消失
        daily = conn.execute(
            "SELECT trade_date, COUNT(*) FROM daily_bar GROUP BY 1 ORDER BY 1"
        ).fetchall()
        assert daily == [(date(2026, 7, 1), 2)]
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
        assert by_table["daily_bar"].mode == "replace"
        assert by_table["monitor_event"].mode == "merge"

    def test_backup_missing_raises(self, local_db: Path, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            sync_from_backup(tmp_path / "nope.duckdb", local_db)

    def test_merge_idempotent(self, local_db: Path, backup_db: Path) -> None:
        sync_from_backup(backup_db, local_db, refresh_replica=False)
        report2 = sync_from_backup(backup_db, local_db, refresh_replica=False)
        assert not report2.has_errors

        conn = duckdb.connect(str(local_db), read_only=True)
        assert conn.execute("SELECT COUNT(*) FROM monitor_event").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM daily_bar").fetchone()[0] == 2
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
        with pytest.raises(ValueError, match="daily_bar"):
            restore_research_tables(src, local_db, tables=["daily_bar"])


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
