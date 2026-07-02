"""云端热备与本地研究库的分家同步。

背景（2026-07-02 事故）：sync-from-cloud.sh 原来直接原子替换本地主库
data/rquant.duckdb。盘中本地 monitor 持旧 inode 写分钟线，文件被换后
所有写入进了被 unlink 的幽灵文件；同时 monitor 按路径写的 WAL 与新文件
代际错配，DuckDB 回放 WAL 直接 InternalException，主库打不开。

分家后的拓扑：
- 云端快照只落 data/cloud_backup.duckdb（纯备份工件，本地无进程写它）
- 本地主库 rquant.duckdb 是唯一常驻库：生产表从备份合并进来，
  研究表（分钟线/竞价/模拟盘）由本地回补和 monitor 直接写
- 合并后原子刷新本地只读副本 rquant_ro.duckdb 供 Strategy Lab 读

表分两类：
- REPLACE_TABLES：云端权威，整表替换（DELETE + INSERT，保留本地 DDL/主键）
- MERGE_TABLES：双端追加流，按主键 INSERT OR REPLACE，绝不整表替换
"""

from __future__ import annotations

import os
import shutil
import time
from pathlib import Path

import duckdb
from loguru import logger
from pydantic import BaseModel

from rquant.config import settings
from rquant.storage.schema import ALL_DDL

# 云端 daily/monitor 流水线权威产出，本地无独立增量 → 整表替换
REPLACE_TABLES: tuple[str, ...] = (
    "daily_bar",
    "stock_basic",
    "adj_factor",
    "daily_indicator",
    "daily_state",
    "daily_basic",
    "screen_result",
    "pool2_watch",
    "risk_blacklist",
)

# 双端都可能追加（本地研究回补 / 本地盘中 monitor / 未来云端 rt_min），
# 按主键合并，本地独有行永不丢失
MERGE_TABLES: tuple[str, ...] = (
    "monitor_event",
    "minute_bar",
    "auction_bar",
    "index_daily_bar",
    "moneyflow_daily",
    "market_sentiment_daily",
    "intraday_feature_snapshot",
    "paper_position",
    "paper_position_event",
)


class TableSyncResult(BaseModel):
    table: str
    mode: str  # replace | merge | skipped | error
    rows: int = 0
    detail: str = ""


class ResearchSyncReport(BaseModel):
    backup_path: str
    db_path: str
    tables: list[TableSyncResult]
    replica_refreshed: bool = False
    replica_detail: str = ""

    @property
    def has_errors(self) -> bool:
        return any(t.mode == "error" for t in self.tables)


def _rescue_stale_wal(db_path: Path) -> duckdb.DuckDBPyConnection:
    """打开主库；WAL 回放失败时把陈旧 WAL 挪成 .bak 后重试一次。

    只在库根本打不开时才动 WAL（此时 WAL 已不可用），文件保留为
    .corrupt-<ts>.bak 供人工排查，不静默删除。
    """
    try:
        return duckdb.connect(str(db_path))
    except duckdb.Error as e:
        wal_path = db_path.with_name(db_path.name + ".wal")
        if "WAL" not in str(e) or not wal_path.exists():
            raise
        backup = wal_path.with_name(
            f"{wal_path.name}.corrupt-{time.strftime('%Y%m%d%H%M%S')}.bak"
        )
        wal_path.rename(backup)
        logger.warning(
            f"主库 WAL 回放失败（{e}），已挪至 {backup} 后重试打开。"
            f"若近期本地有未落盘写入需人工确认。"
        )
        return duckdb.connect(str(db_path))


def _common_columns(
    conn: duckdb.DuckDBPyConnection, table: str, alias: str
) -> tuple[list[str], list[str]]:
    """返回 (本地列 ∩ 备份列, 本地主键列)。"""
    local_cols = [
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = current_catalog() AND table_name = ?",
            [table],
        ).fetchall()
    ]
    src_cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_catalog = '{alias}' AND table_name = ?",
            [table],
        ).fetchall()
    }
    pk_cols = [
        r[0]
        for r in conn.execute(
            "SELECT unnest(constraint_column_names) FROM duckdb_constraints() "
            "WHERE table_name = ? AND constraint_type = 'PRIMARY KEY' "
            "AND database_name = current_catalog()",
            [table],
        ).fetchall()
    ]
    return [c for c in local_cols if c in src_cols], pk_cols


def _sync_table(
    conn: duckdb.DuckDBPyConnection, table: str, alias: str, mode: str
) -> TableSyncResult:
    src_exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog = '{alias}' AND table_name = ?",
        [table],
    ).fetchone()[0]
    if not src_exists:
        return TableSyncResult(table=table, mode="skipped", detail="备份中无此表")

    cols, pk_cols = _common_columns(conn, table, alias)
    if not cols:
        return TableSyncResult(table=table, mode="skipped", detail="无共同列")
    if mode == "merge" and any(pk not in cols for pk in pk_cols):
        return TableSyncResult(
            table=table, mode="skipped", detail=f"备份缺主键列 {pk_cols}"
        )

    col_list = ", ".join(cols)
    src_rows = conn.execute(f'SELECT COUNT(*) FROM {alias}."{table}"').fetchone()[0]

    try:
        conn.execute("BEGIN")
        if mode == "replace":
            conn.execute(f'DELETE FROM "{table}"')
            conn.execute(
                f'INSERT INTO "{table}" ({col_list}) '
                f'SELECT {col_list} FROM {alias}."{table}"'
            )
        else:
            conn.execute(
                f'INSERT OR REPLACE INTO "{table}" ({col_list}) '
                f'SELECT {col_list} FROM {alias}."{table}"'
            )
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.exception(f"research-sync 表 {table} 同步失败")
        return TableSyncResult(table=table, mode="error", detail=str(e)[:200])

    return TableSyncResult(table=table, mode=mode, rows=src_rows)


def refresh_readonly_replica(
    db_path: Path | None = None, replica_path: Path | None = None
) -> tuple[bool, str]:
    """把主库原子复制成只读副本（cp → 只读验证 → os.replace）。

    主库若还有 WAL（存在其他活跃写者，如盘中 monitor），拒绝刷新——
    只拷 .duckdb 不拷 WAL 会产生撕裂副本。
    """
    db_path = db_path or settings.duckdb_path
    replica_path = replica_path or settings.duckdb_readonly_path_resolved
    wal_path = db_path.with_name(db_path.name + ".wal")
    if wal_path.exists():
        return False, f"主库存在活跃 WAL（{wal_path.name}），跳过副本刷新"

    tmp = replica_path.with_name(replica_path.name + ".sync-tmp")
    try:
        shutil.copy2(db_path, tmp)
        verify = duckdb.connect(str(tmp), read_only=True)
        verify.execute("SELECT COUNT(*) FROM daily_bar").fetchone()
        verify.close()
        os.replace(tmp, replica_path)
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return False, f"副本刷新失败：{e}"
    return True, "副本已刷新"


def sync_from_backup(
    backup_path: Path | None = None,
    db_path: Path | None = None,
    *,
    refresh_replica: bool = True,
) -> ResearchSyncReport:
    """把云端备份里的生产表合并进本地研究库。"""
    backup_path = backup_path or settings.data_dir / "cloud_backup.duckdb"
    db_path = db_path or settings.duckdb_path

    if not backup_path.exists():
        raise FileNotFoundError(f"云端备份不存在：{backup_path}")

    conn = _rescue_stale_wal(db_path)
    results: list[TableSyncResult] = []
    try:
        for ddl in ALL_DDL:
            conn.execute(ddl)
        conn.execute(
            f"ATTACH '{backup_path}' AS cloud_backup (READ_ONLY)"
        )
        for table in REPLACE_TABLES:
            results.append(_sync_table(conn, table, "cloud_backup", "replace"))
        for table in MERGE_TABLES:
            results.append(_sync_table(conn, table, "cloud_backup", "merge"))
        conn.execute("DETACH cloud_backup")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    report = ResearchSyncReport(
        backup_path=str(backup_path),
        db_path=str(db_path),
        tables=results,
    )
    if refresh_replica:
        ok, detail = refresh_readonly_replica(db_path)
        report.replica_refreshed = ok
        report.replica_detail = detail

    synced = sum(t.rows for t in results if t.mode in ("replace", "merge"))
    logger.info(
        f"research-sync 完成：{synced:,} 行来自备份，"
        f"replica={'OK' if report.replica_refreshed else report.replica_detail}"
    )
    return report


def restore_research_tables(
    source_path: Path,
    db_path: Path | None = None,
    tables: list[str] | None = None,
    *,
    refresh_replica: bool = True,
) -> ResearchSyncReport:
    """从旧库/旧副本按主键恢复研究表（灾后恢复用，只合并不替换）。"""
    db_path = db_path or settings.duckdb_path
    tables = tables or list(MERGE_TABLES)

    unknown = [t for t in tables if t not in MERGE_TABLES]
    if unknown:
        raise ValueError(
            f"只允许恢复 MERGE_TABLES 中的研究表，非法表：{unknown}"
        )
    if not source_path.exists():
        raise FileNotFoundError(f"恢复源不存在：{source_path}")

    conn = _rescue_stale_wal(db_path)
    results: list[TableSyncResult] = []
    try:
        for ddl in ALL_DDL:
            conn.execute(ddl)
        conn.execute(f"ATTACH '{source_path}' AS restore_src (READ_ONLY)")
        for table in tables:
            results.append(_sync_table(conn, table, "restore_src", "merge"))
        conn.execute("DETACH restore_src")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    report = ResearchSyncReport(
        backup_path=str(source_path),
        db_path=str(db_path),
        tables=results,
    )
    if refresh_replica:
        ok, detail = refresh_readonly_replica(db_path)
        report.replica_refreshed = ok
        report.replica_detail = detail
    return report
