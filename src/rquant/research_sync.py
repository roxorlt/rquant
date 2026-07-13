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

表分三类：
- REPLACE_TABLES：云端权威且本地无独有行，整表替换（DELETE + INSERT，
  保留本地 DDL/主键）
- MERGE_TABLES：本地存在独有行的表；普通表按主键合并，生命周期元数据表
  按状态与事件时间协调，绝不整表替换。三种来源：
  a) 双端追加流（monitor 事件 / 分钟线 / 竞价 / 模拟盘）
  b) 日线族 daily_bar/daily_basic/adj_factor/daily_state/daily_indicator——
     2026-07 本地回补了 2020 起全市场历史，云端只有 2024-09 起；若保持
     整表替换，一次日终合并就会把回补历史全部抹掉
  c) 本地独有采集 limit_up_pool_daily（云端东财源被屏蔽，永远没有）与
     limit_list_daily（Tushare 涨跌停榜本地回补 + 日终增量，云端 daily 不拉）
  灾后恢复（restore_research_tables）改用 INSERT OR IGNORE：只补本地
  缺失的行，主键冲突时保留本地现值，绝不用旧副本覆盖本地已更新的行
- LOCAL_ONLY_TABLES：只描述本机状态，不从云端备份导入

错误语义：顶层失败（备份缺失 / 主库打不开 / ATTACH 失败）不抛异常，
转成 has_errors 的报告返回——告警由 sync-from-cloud.sh 统一推，避免
cli main() 的 notify 与脚本 PushDeer 双重告警。
"""

from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

import duckdb
from loguru import logger
from pydantic import BaseModel

from rquant.config import settings
from rquant.storage.migrations import initialize_schema

# 云端 daily/monitor 流水线权威产出，本地无独立增量 → 整表替换
REPLACE_TABLES: tuple[str, ...] = (
    "stock_basic",
    "screen_result",
    "pool2_watch",
    "risk_blacklist",
)

# 本地存在独有行（历史回补 / 盘中 monitor / 本地采集），按主键合并：
# 主键冲突云端赢，本地独有行永不丢失（分类依据见模块 docstring）
MERGE_TABLES: tuple[str, ...] = (
    "daily_bar",
    "daily_basic",
    "adj_factor",
    "daily_state",
    "daily_indicator",
    "monitor_event",
    "minute_bar",
    "auction_bar",
    "index_daily_bar",
    "moneyflow_daily",
    "market_sentiment_daily",
    "intraday_feature_snapshot",
    "paper_position",
    "paper_position_event",
    "limit_up_pool_daily",
    "limit_list_daily",
    # 统一数据集回补层（dataset_backfill）：本地回补权威，云端没有。
    # 快照表（ths_board*/dc_board*/hm_list）云端永远无同名表 → merge 是 no-op，
    # 归入 MERGE 只为语义一致（本地独有行绝不被整表替换抹掉）
    "ths_index_daily",
    "dc_index_daily",
    "ths_board",
    "dc_board",
    "ths_board_member",
    "dc_board_member",
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
    "dataset_snapshot",
    "dataset_coverage",
    "data_quality_issue",
)

LOCAL_ONLY_TABLES: tuple[str, ...] = ("schema_migration",)


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


def _attach_readonly(
    conn: duckdb.DuckDBPyConnection, path: Path, alias: str
) -> None:
    """ATTACH 只读库。duckdb 1.5.2 实测 ATTACH 不支持 ? 参数绑定，
    路径只能拼字符串——单引号双写转义，防含 ' 的路径（如 roxor's backup）炸 SQL。
    """
    escaped = str(path).replace("'", "''")
    conn.execute(f"ATTACH '{escaped}' AS {alias} (READ_ONLY)")


def _failure_report(
    source_path: Path, db_path: Path, detail: str
) -> ResearchSyncReport:
    logger.error(detail)
    return ResearchSyncReport(
        backup_path=str(source_path),
        db_path=str(db_path),
        tables=[TableSyncResult(table="<sync>", mode="error", detail=detail[:200])],
        replica_refreshed=False,
        replica_detail="同步失败，跳过副本刷新",
    )


def _refresh_replica_if_clean(
    report: ResearchSyncReport, db_path: Path, refresh_replica: bool
) -> None:
    """有任何表同步失败时跳过副本刷新，避免把跨表不一致快照发布给 Strategy Lab。"""
    if not refresh_replica:
        return
    if report.has_errors:
        report.replica_refreshed = False
        report.replica_detail = "存在同步失败的表，跳过副本刷新（避免发布跨表不一致快照）"
        return
    ok, detail = refresh_readonly_replica(db_path)
    report.replica_refreshed = ok
    report.replica_detail = detail


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


def _merge_dataset_snapshots(
    conn: duckdb.DuckDBPyConnection, alias: str
) -> None:
    overlapping = conn.execute(
        f"""
        SELECT target.snapshot_id,
               target.strategy_name, source.strategy_name,
               target.manifest_id, source.manifest_id,
               epoch_us(target.as_of_time), epoch_us(source.as_of_time),
               target.code_commit, source.code_commit,
               target.origin, source.origin,
               target.status, source.status,
               target.table_watermarks, source.table_watermarks,
               target.quality_issue_ids, source.quality_issue_ids,
               epoch_us(target.completed_at), epoch_us(source.completed_at)
        FROM dataset_snapshot AS target
        JOIN {alias}.dataset_snapshot AS source USING (snapshot_id)
        """
    ).fetchall()
    for row in overlapping:
        snapshot_id = str(row[0])
        target_identity = (row[1], row[3], row[5], row[7], row[9])
        source_identity = (row[2], row[4], row[6], row[8], row[10])
        if target_identity != source_identity:
            raise ValueError(
                f"dataset_snapshot stable identity conflict: {snapshot_id}"
            )
        target_status, source_status = str(row[11]), str(row[12])
        if target_status == source_status == "ready":
            target_finalization = (
                json.loads(str(row[13])),
                json.loads(str(row[15])),
                row[17],
            )
            source_finalization = (
                json.loads(str(row[14])),
                json.loads(str(row[16])),
                row[18],
            )
            if target_finalization != source_finalization:
                raise ValueError(
                    "immutable dataset_snapshot finalization conflict: "
                    f"{snapshot_id}"
                )

    conn.execute(
        f"""
        INSERT INTO dataset_snapshot
        (snapshot_id, strategy_name, manifest_id, as_of_time, code_commit,
         origin, status, table_watermarks, quality_issue_ids, created_at,
         completed_at)
        SELECT source.snapshot_id, source.strategy_name, source.manifest_id,
               source.as_of_time, source.code_commit, source.origin,
               source.status, source.table_watermarks,
               source.quality_issue_ids, source.created_at, source.completed_at
        FROM {alias}.dataset_snapshot AS source
        WHERE NOT EXISTS (
            SELECT 1 FROM dataset_snapshot AS target
            WHERE target.snapshot_id = source.snapshot_id
        )
        """
    )
    conn.execute(
        f"""
        UPDATE dataset_snapshot AS target
        SET status = 'ready',
            table_watermarks = source.table_watermarks,
            quality_issue_ids = source.quality_issue_ids,
            completed_at = source.completed_at
        FROM {alias}.dataset_snapshot AS source
        WHERE target.snapshot_id = source.snapshot_id
          AND target.status = 'building'
          AND source.status = 'ready'
        """
    )


def _merge_data_quality_issues(
    conn: duckdb.DuckDBPyConnection, alias: str
) -> None:
    conflict = conn.execute(
        f"""
        SELECT target.issue_id
        FROM data_quality_issue AS target
        JOIN {alias}.data_quality_issue AS source USING (issue_id)
        WHERE target.rule_id IS DISTINCT FROM source.rule_id
           OR target.dataset_id IS DISTINCT FROM source.dataset_id
           OR target.scope_key IS DISTINCT FROM source.scope_key
        LIMIT 1
        """
    ).fetchone()
    if conflict is not None:
        raise ValueError(
            f"data_quality_issue stable identity conflict: {conflict[0]}"
        )

    conn.execute(
        f"""
        INSERT INTO data_quality_issue
        (issue_id, rule_id, dataset_id, severity, status, scope_key, message,
         evidence, first_seen_at, last_seen_at, resolved_at)
        SELECT source.issue_id, source.rule_id, source.dataset_id,
               source.severity, source.status, source.scope_key, source.message,
               source.evidence, source.first_seen_at, source.last_seen_at,
               source.resolved_at
        FROM {alias}.data_quality_issue AS source
        WHERE NOT EXISTS (
            SELECT 1 FROM data_quality_issue AS target
            WHERE target.issue_id = source.issue_id
        )
        """
    )
    conn.execute(
        f"""
        UPDATE data_quality_issue AS target
        SET severity = source.severity,
            status = source.status,
            message = source.message,
            evidence = source.evidence,
            first_seen_at = least(target.first_seen_at, source.first_seen_at),
            last_seen_at = source.last_seen_at,
            resolved_at = source.resolved_at
        FROM {alias}.data_quality_issue AS source
        WHERE target.issue_id = source.issue_id
          AND greatest(
              source.last_seen_at,
              coalesce(source.resolved_at, source.last_seen_at)
          ) > greatest(
              target.last_seen_at,
              coalesce(target.resolved_at, target.last_seen_at)
          )
        """
    )


def _sync_table(
    conn: duckdb.DuckDBPyConnection, table: str, alias: str, mode: str
) -> TableSyncResult:
    """mode 三种执行策略：
    - replace：整表 DELETE + INSERT（云端权威表）
    - merge：通常源覆盖本地；生命周期元数据按状态和事件时间协调
    - restore：INSERT OR IGNORE，主键冲突时保留本地（灾后恢复，只补缺失行）

    注意返回的 TableSyncResult.mode 词汇固定为 replace|merge|skipped|error
    （cli 渲染按此映射），restore 对外归类为 merge，detail 标注恢复语义。
    """
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
    if mode in ("merge", "restore") and any(pk not in cols for pk in pk_cols):
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
        elif mode == "restore":
            conn.execute(
                f'INSERT OR IGNORE INTO "{table}" ({col_list}) '
                f'SELECT {col_list} FROM {alias}."{table}"'
            )
        elif table == "dataset_snapshot":
            _merge_dataset_snapshots(conn, alias)
        elif table == "data_quality_issue":
            _merge_data_quality_issues(conn, alias)
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

    if mode == "restore":
        return TableSyncResult(
            table=table,
            mode="merge",
            rows=src_rows,
            detail="restore：冲突保留本地行，只补缺失行",
        )
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
    """把云端备份里的生产表合并进本地研究库。

    顶层失败不抛异常，返回 has_errors 的报告（见模块 docstring 错误语义）。
    """
    backup_path = backup_path or settings.data_dir / "cloud_backup.duckdb"
    db_path = db_path or settings.duckdb_path

    if not backup_path.exists():
        return _failure_report(
            backup_path, db_path, f"云端备份不存在：{backup_path}"
        )

    try:
        conn = _rescue_stale_wal(db_path)
    except Exception as e:
        logger.exception("research-sync 打开主库失败")
        return _failure_report(backup_path, db_path, f"打开主库失败：{e}")

    results: list[TableSyncResult] = []
    try:
        initialize_schema(conn)
        _attach_readonly(conn, backup_path, "cloud_backup")
        for table in REPLACE_TABLES:
            results.append(_sync_table(conn, table, "cloud_backup", "replace"))
        for table in MERGE_TABLES:
            results.append(_sync_table(conn, table, "cloud_backup", "merge"))
        conn.execute("DETACH cloud_backup")
        conn.execute("CHECKPOINT")
    except Exception as e:
        logger.exception("research-sync 顶层失败")
        results.append(
            TableSyncResult(table="<sync>", mode="error", detail=str(e)[:200])
        )
    finally:
        conn.close()

    report = ResearchSyncReport(
        backup_path=str(backup_path),
        db_path=str(db_path),
        tables=results,
    )
    _refresh_replica_if_clean(report, db_path, refresh_replica)

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
    """从旧库/旧副本恢复研究表（灾后恢复用）。

    INSERT OR IGNORE 语义：只补本地缺失的行，主键冲突时保留本地现值——
    旧副本里的过期行（如今天已平仓的 paper_position 的旧 open 状态）
    绝不覆盖本地。非法表名仍抛 ValueError（调用方约定错误应当炸），
    其余顶层失败转成 has_errors 的报告返回。
    """
    db_path = db_path or settings.duckdb_path
    tables = tables or list(MERGE_TABLES)

    unknown = [t for t in tables if t not in MERGE_TABLES]
    if unknown:
        raise ValueError(
            f"只允许恢复 MERGE_TABLES 中的研究表，非法表：{unknown}"
        )
    if not source_path.exists():
        return _failure_report(
            source_path, db_path, f"恢复源不存在：{source_path}"
        )

    try:
        conn = _rescue_stale_wal(db_path)
    except Exception as e:
        logger.exception("research-restore 打开主库失败")
        return _failure_report(source_path, db_path, f"打开主库失败：{e}")

    results: list[TableSyncResult] = []
    try:
        initialize_schema(conn)
        _attach_readonly(conn, source_path, "restore_src")
        for table in tables:
            results.append(_sync_table(conn, table, "restore_src", "restore"))
        conn.execute("DETACH restore_src")
        conn.execute("CHECKPOINT")
    except Exception as e:
        logger.exception("research-restore 顶层失败")
        results.append(
            TableSyncResult(table="<sync>", mode="error", detail=str(e)[:200])
        )
    finally:
        conn.close()

    report = ResearchSyncReport(
        backup_path=str(source_path),
        db_path=str(db_path),
        tables=results,
    )
    _refresh_replica_if_clean(report, db_path, refresh_replica)
    return report
