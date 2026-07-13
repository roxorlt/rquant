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
  d) trade_calendar 按 updated_at 单调合并，等时事实冲突整表回滚
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
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
from loguru import logger
from pydantic import BaseModel

from rquant.config import settings
from rquant.data_metadata import DataQualityIssue, DatasetCoverage, DatasetSnapshot
from rquant.storage.duckdb import (
    _coverage_from_row,
    _quality_issue_from_row,
    _snapshot_from_row,
)
from rquant.storage.migrations import initialize_schema
from rquant.trade_calendar import TradeCalendarConflictError

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
    "trade_calendar",
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

DATA_METADATA_TABLES: tuple[str, ...] = (
    "dataset_snapshot",
    "dataset_coverage",
    "data_quality_issue",
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


def _load_source_snapshots(
    conn: duckdb.DuckDBPyConnection, alias: str
) -> dict[str, DatasetSnapshot]:
    rows = conn.execute(
        f"""
        SELECT snapshot_id, strategy_name, manifest_id,
               strftime(as_of_time AT TIME ZONE 'UTC',
                        '%Y-%m-%dT%H:%M:%S.%fZ'),
               code_commit, origin, status, table_watermarks,
               quality_issue_ids,
               strftime(created_at AT TIME ZONE 'UTC',
                        '%Y-%m-%dT%H:%M:%S.%fZ'),
               CASE WHEN completed_at IS NULL THEN NULL ELSE
                   strftime(completed_at AT TIME ZONE 'UTC',
                            '%Y-%m-%dT%H:%M:%S.%fZ')
               END
        FROM {alias}.dataset_snapshot
        """
    ).fetchall()
    snapshots: dict[str, DatasetSnapshot] = {}
    for row in rows:
        try:
            snapshot = _snapshot_from_row(row)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"invalid source dataset_snapshot {row[0]}: {exc}"
            ) from exc
        snapshots[snapshot.snapshot_id] = snapshot
    return snapshots


def _load_source_coverages(
    conn: duckdb.DuckDBPyConnection, alias: str
) -> list[DatasetCoverage]:
    rows = conn.execute(
        f"""
        SELECT snapshot_id, dataset_id, coverage_scope, table_name,
               expected_count, available_count, missing_count,
               coverage_ratio, missing_reasons,
               strftime(created_at AT TIME ZONE 'UTC',
                        '%Y-%m-%dT%H:%M:%S.%fZ')
        FROM {alias}.dataset_coverage
        """
    ).fetchall()
    coverages: list[DatasetCoverage] = []
    for row in rows:
        try:
            coverages.append(_coverage_from_row(row))
        except (TypeError, ValueError) as exc:
            key = f"{row[0]}/{row[1]}/{row[2]}"
            raise ValueError(
                f"invalid source dataset_coverage {key}: {exc}"
            ) from exc
    return coverages


def _load_source_quality_issues(
    conn: duckdb.DuckDBPyConnection, alias: str
) -> dict[str, DataQualityIssue]:
    rows = conn.execute(
        f"""
        SELECT issue_id, rule_id, dataset_id, severity, status, scope_key,
               message, evidence,
               strftime(first_seen_at AT TIME ZONE 'UTC',
                        '%Y-%m-%dT%H:%M:%S.%fZ'),
               strftime(last_seen_at AT TIME ZONE 'UTC',
                        '%Y-%m-%dT%H:%M:%S.%fZ'),
               CASE WHEN resolved_at IS NULL THEN NULL ELSE
                   strftime(resolved_at AT TIME ZONE 'UTC',
                            '%Y-%m-%dT%H:%M:%S.%fZ')
               END
        FROM {alias}.data_quality_issue
        """
    ).fetchall()
    issues: dict[str, DataQualityIssue] = {}
    for row in rows:
        try:
            issue = _quality_issue_from_row(row)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"invalid source data_quality_issue {row[0]}: {exc}"
            ) from exc
        issues[issue.issue_id] = issue
    return issues


def _validate_source_coverage_references(
    snapshots: dict[str, DatasetSnapshot],
    coverages: list[DatasetCoverage],
) -> None:
    for coverage in coverages:
        if coverage.snapshot_id not in snapshots:
            raise ValueError(
                "source dataset_coverage references missing dataset_snapshot: "
                f"{coverage.snapshot_id}"
            )


def _validate_source_snapshot_issue_references(
    conn: duckdb.DuckDBPyConnection,
    snapshots: dict[str, DatasetSnapshot],
    source_issues: dict[str, DataQualityIssue],
) -> None:
    target_issue_ids = {
        str(row[0])
        for row in conn.execute("SELECT issue_id FROM data_quality_issue").fetchall()
    }
    available_issue_ids = target_issue_ids | set(source_issues)
    for snapshot in snapshots.values():
        missing = [
            issue_id
            for issue_id in snapshot.quality_issue_ids
            if issue_id not in available_issue_ids
        ]
        if missing:
            raise ValueError(
                "source dataset_snapshot references missing quality issues: "
                f"{snapshot.snapshot_id}: {', '.join(missing)}"
            )


def _validate_dataset_snapshot_conflicts(
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


def _merge_dataset_snapshots(
    conn: duckdb.DuckDBPyConnection, alias: str
) -> None:
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
            created_at = least(target.created_at, source.created_at),
            completed_at = source.completed_at
        FROM {alias}.dataset_snapshot AS source
        WHERE target.snapshot_id = source.snapshot_id
          AND target.status = 'building'
          AND source.status = 'ready'
        """
    )


def _merge_dataset_coverages(
    conn: duckdb.DuckDBPyConnection, alias: str
) -> None:
    overlapping = conn.execute(
        f"""
        SELECT target.snapshot_id, target.dataset_id, target.coverage_scope,
               target.table_name, source.table_name,
               target.expected_count, source.expected_count,
               target.available_count, source.available_count,
               target.missing_count, source.missing_count,
               target.coverage_ratio, source.coverage_ratio,
               target.missing_reasons, source.missing_reasons
        FROM dataset_coverage AS target
        JOIN {alias}.dataset_coverage AS source
          USING (snapshot_id, dataset_id, coverage_scope)
        JOIN dataset_snapshot AS target_snapshot
          ON target_snapshot.snapshot_id = target.snapshot_id
         AND target_snapshot.status = 'ready'
        JOIN {alias}.dataset_snapshot AS source_snapshot
          ON source_snapshot.snapshot_id = source.snapshot_id
         AND source_snapshot.status = 'ready'
        """
    ).fetchall()
    for row in overlapping:
        target_payload = (
            row[3],
            row[5],
            row[7],
            row[9],
            row[11],
            json.loads(str(row[13])),
        )
        source_payload = (
            row[4],
            row[6],
            row[8],
            row[10],
            row[12],
            json.loads(str(row[14])),
        )
        if target_payload != source_payload:
            key = f"{row[0]}/{row[1]}/{row[2]}"
            raise ValueError(
                f"conflicting dataset_coverage for ready snapshot: {key}"
            )

    conn.execute(
        f"""
        UPDATE dataset_coverage AS target
        SET table_name = source.table_name,
            expected_count = source.expected_count,
            available_count = source.available_count,
            missing_count = source.missing_count,
            coverage_ratio = source.coverage_ratio,
            missing_reasons = source.missing_reasons,
            created_at = least(target.created_at, source.created_at)
        FROM {alias}.dataset_coverage AS source,
             {alias}.dataset_snapshot AS source_snapshot,
             dataset_snapshot AS target_snapshot
        WHERE target.snapshot_id = source.snapshot_id
          AND target.dataset_id = source.dataset_id
          AND target.coverage_scope = source.coverage_scope
          AND source_snapshot.snapshot_id = source.snapshot_id
          AND source_snapshot.status = 'ready'
          AND target_snapshot.snapshot_id = target.snapshot_id
          AND target_snapshot.status = 'building'
        """
    )
    conn.execute(
        f"""
        UPDATE dataset_coverage AS target
        SET created_at = least(target.created_at, source.created_at)
        FROM {alias}.dataset_coverage AS source,
             {alias}.dataset_snapshot AS source_snapshot,
             dataset_snapshot AS target_snapshot
        WHERE target.snapshot_id = source.snapshot_id
          AND target.dataset_id = source.dataset_id
          AND target.coverage_scope = source.coverage_scope
          AND source_snapshot.snapshot_id = source.snapshot_id
          AND source_snapshot.status = 'ready'
          AND target_snapshot.snapshot_id = target.snapshot_id
          AND target_snapshot.status = 'ready'
        """
    )
    conn.execute(
        f"""
        INSERT INTO dataset_coverage
        (snapshot_id, dataset_id, coverage_scope, table_name, expected_count,
         available_count, missing_count, coverage_ratio, missing_reasons,
         created_at)
        SELECT source.snapshot_id, source.dataset_id, source.coverage_scope,
               source.table_name, source.expected_count, source.available_count,
               source.missing_count, source.coverage_ratio,
               source.missing_reasons, source.created_at
        FROM {alias}.dataset_coverage AS source
        JOIN {alias}.dataset_snapshot AS source_snapshot
          ON source_snapshot.snapshot_id = source.snapshot_id
         AND source_snapshot.status = 'ready'
        WHERE NOT EXISTS (
            SELECT 1
            FROM dataset_coverage AS target
            WHERE target.snapshot_id = source.snapshot_id
              AND target.dataset_id = source.dataset_id
              AND target.coverage_scope = source.coverage_scope
        )
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

    overlapping = conn.execute(
        f"""
        SELECT target.issue_id,
               target.severity, source.severity,
               target.message, source.message,
               target.evidence, source.evidence,
               epoch_us(target.first_seen_at), epoch_us(source.first_seen_at),
               epoch_us(target.last_seen_at), epoch_us(source.last_seen_at),
               epoch_us(target.resolved_at), epoch_us(source.resolved_at)
        FROM data_quality_issue AS target
        JOIN {alias}.data_quality_issue AS source USING (issue_id)
        """
    ).fetchall()

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
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    for row in overlapping:
        target_last_seen = int(row[9])
        source_last_seen = int(row[10])
        source_observation_is_newer = source_last_seen > target_last_seen
        severity = row[2] if source_observation_is_newer else row[1]
        message = row[4] if source_observation_is_newer else row[3]
        evidence = row[6] if source_observation_is_newer else row[5]
        first_seen = min(int(row[7]), int(row[8]))
        last_seen = max(target_last_seen, source_last_seen)
        resolution_events = [
            int(value) for value in (row[11], row[12]) if value is not None
        ]
        latest_resolution = (
            max(resolution_events) if resolution_events else None
        )
        status = (
            "resolved"
            if latest_resolution is not None and latest_resolution >= last_seen
            else "open"
        )
        resolved_at = (
            epoch + timedelta(microseconds=latest_resolution)
            if status == "resolved" and latest_resolution is not None
            else None
        )
        conn.execute(
            """
            UPDATE data_quality_issue
            SET severity = ?, status = ?, message = ?, evidence = CAST(? AS JSON),
                first_seen_at = ?, last_seen_at = ?, resolved_at = ?
            WHERE issue_id = ?
            """,
            [
                severity,
                status,
                message,
                json.dumps(json.loads(str(evidence)), sort_keys=True),
                epoch + timedelta(microseconds=first_seen),
                epoch + timedelta(microseconds=last_seen),
                resolved_at,
                row[0],
            ],
        )


def _metadata_bundle_failure_results(
    failed_table: str, detail: str
) -> list[TableSyncResult]:
    rollback_detail = f"linked metadata bundle rolled back: {detail}"
    return [
        TableSyncResult(
            table=table,
            mode="error" if table == failed_table else "skipped",
            detail=(
                rollback_detail
                if table == failed_table
                else f"linked metadata bundle skipped after {failed_table} failed"
            )[:200],
        )
        for table in DATA_METADATA_TABLES
    ]


def _sync_data_metadata_bundle(
    conn: duckdb.DuckDBPyConnection,
    alias: str,
    *,
    manage_transaction: bool = True,
) -> list[TableSyncResult]:
    """Validate and merge linked dataset metadata in one transaction."""
    current_table = DATA_METADATA_TABLES[0]
    transaction_started = False
    try:
        if manage_transaction:
            conn.execute("BEGIN")
            transaction_started = True
        present_tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_catalog = '{alias}' "
                "AND table_name IN (?, ?, ?)",
                list(DATA_METADATA_TABLES),
            ).fetchall()
        }
        if not present_tables:
            if manage_transaction:
                conn.execute("COMMIT")
                transaction_started = False
            return [
                TableSyncResult(
                    table=table,
                    mode="skipped",
                    detail="备份中无 linked metadata bundle",
                )
                for table in DATA_METADATA_TABLES
            ]
        missing_tables = set(DATA_METADATA_TABLES) - present_tables
        if missing_tables:
            current_table = next(
                table for table in DATA_METADATA_TABLES if table in missing_tables
            )
            raise ValueError(
                "incomplete linked metadata bundle; missing source tables: "
                f"{', '.join(sorted(missing_tables))}"
            )

        row_counts = {
            table: int(
                conn.execute(
                    f'SELECT COUNT(*) FROM {alias}."{table}"'
                ).fetchone()[0]
            )
            for table in DATA_METADATA_TABLES
        }

        current_table = "dataset_snapshot"
        source_snapshots = _load_source_snapshots(conn, alias)
        current_table = "dataset_coverage"
        source_coverages = _load_source_coverages(conn, alias)
        _validate_source_coverage_references(
            source_snapshots, source_coverages
        )
        current_table = "data_quality_issue"
        source_issues = _load_source_quality_issues(conn, alias)
        current_table = "dataset_snapshot"
        _validate_source_snapshot_issue_references(
            conn, source_snapshots, source_issues
        )
        _validate_dataset_snapshot_conflicts(conn, alias)

        # Coverage reconciliation must see the target before snapshot promotion.
        current_table = "dataset_coverage"
        _merge_dataset_coverages(conn, alias)
        current_table = "data_quality_issue"
        _merge_data_quality_issues(conn, alias)
        current_table = "dataset_snapshot"
        _merge_dataset_snapshots(conn, alias)
        if manage_transaction:
            conn.execute("COMMIT")
            transaction_started = False
    except Exception as exc:
        if transaction_started:
            try:
                conn.execute("ROLLBACK")
            except duckdb.Error:
                logger.exception("research-sync linked metadata bundle 回滚失败")
        logger.exception("research-sync linked metadata bundle 同步失败")
        return _metadata_bundle_failure_results(current_table, str(exc))

    return [
        TableSyncResult(table=table, mode="merge", rows=row_counts[table])
        for table in DATA_METADATA_TABLES
    ]


def _sync_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    alias: str,
    mode: str,
    *,
    manage_transaction: bool = True,
) -> TableSyncResult:
    """mode 三种执行策略：
    - replace：整表 DELETE + INSERT（云端权威表）
    - merge：通常源覆盖本地；日历和生命周期元数据按事件时间协调
    - restore：INSERT OR IGNORE，主键冲突时保留本地（灾后恢复，只补缺失行）

    注意返回的 TableSyncResult.mode 词汇固定为 replace|merge|skipped|error
    （cli 渲染按此映射），restore 对外归类为 merge，detail 标注恢复语义。
    """
    if table in DATA_METADATA_TABLES:
        return TableSyncResult(
            table=table,
            mode="error",
            detail="linked dataset metadata must sync as one atomic bundle",
        )

    src_exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog = '{alias}' AND table_name = ?",
        [table],
    ).fetchone()[0]
    if not src_exists:
        if mode == "replace":
            return TableSyncResult(
                table=table,
                mode="error",
                detail="authoritative replace source table is missing",
            )
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

    trade_calendar_columns = {
        "exchange",
        "cal_date",
        "is_open",
        "pretrade_date",
        "source",
        "updated_at",
    }
    if (
        table == "trade_calendar"
        and mode == "merge"
        and not trade_calendar_columns <= set(cols)
    ):
        missing = sorted(trade_calendar_columns - set(cols))
        return TableSyncResult(
            table=table,
            mode="error",
            detail=f"trade_calendar merge missing columns: {missing}",
        )

    transaction_started = False
    try:
        if manage_transaction:
            conn.execute("BEGIN")
            transaction_started = True
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
        elif table == "trade_calendar":
            conflict = conn.execute(
                f"""
                SELECT source.exchange, source.cal_date,
                       strftime(source.updated_at AT TIME ZONE 'UTC',
                                '%Y-%m-%dT%H:%M:%S.%fZ')
                FROM {alias}.trade_calendar AS source
                JOIN trade_calendar AS target
                  ON target.exchange = source.exchange
                 AND target.cal_date = source.cal_date
                WHERE source.updated_at = target.updated_at
                  AND (
                      source.is_open IS DISTINCT FROM target.is_open
                      OR source.pretrade_date IS DISTINCT FROM target.pretrade_date
                  )
                ORDER BY source.exchange, source.cal_date
                LIMIT 1
                """
            ).fetchone()
            if conflict is not None:
                raise TradeCalendarConflictError(
                    str(conflict[0]),
                    conflict[1],
                    datetime.fromisoformat(
                        str(conflict[2]).replace("Z", "+00:00")
                    ),
                )
            conn.execute(
                f"""
                INSERT INTO trade_calendar
                (exchange, cal_date, is_open, pretrade_date, source, updated_at)
                SELECT exchange, cal_date, is_open, pretrade_date, source, updated_at
                FROM {alias}.trade_calendar
                ON CONFLICT (exchange, cal_date) DO UPDATE SET
                    is_open = excluded.is_open,
                    pretrade_date = excluded.pretrade_date,
                    source = excluded.source,
                    updated_at = excluded.updated_at
                WHERE excluded.updated_at > trade_calendar.updated_at
                """
            )
        else:
            conn.execute(
                f'INSERT OR REPLACE INTO "{table}" ({col_list}) '
                f'SELECT {col_list} FROM {alias}."{table}"'
            )
        if manage_transaction:
            conn.execute("COMMIT")
            transaction_started = False
    except Exception as e:
        if transaction_started:
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
    tmp = replica_path.with_name(replica_path.name + ".sync-tmp")
    wal_path = db_path.with_name(db_path.name + ".wal")
    guard: duckdb.DuckDBPyConnection | None = None
    verify: duckdb.DuckDBPyConnection | None = None
    try:
        guard = duckdb.connect(str(db_path), read_only=True)
        if wal_path.exists():
            return False, f"主库存在活跃 WAL（{wal_path.name}），跳过副本刷新"
        shutil.copy2(db_path, tmp)
        verify = duckdb.connect(str(tmp), read_only=True)
        verify.execute("SELECT COUNT(*) FROM daily_bar").fetchone()
        verify.close()
        verify = None
        os.replace(tmp, replica_path)
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return False, f"副本刷新失败：{e}"
    finally:
        if verify is not None:
            verify.close()
        if guard is not None:
            guard.close()
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
    transaction_started = False
    failed_table: str | None = None
    try:
        initialize_schema(conn)
        _attach_readonly(conn, backup_path, "cloud_backup")
        conn.execute("BEGIN")
        transaction_started = True
        for table in REPLACE_TABLES:
            result = _sync_table(
                conn,
                table,
                "cloud_backup",
                "replace",
                manage_transaction=False,
            )
            results.append(result)
            if result.mode == "error":
                failed_table = table
                raise RuntimeError(result.detail)
        for table in MERGE_TABLES:
            if table in DATA_METADATA_TABLES:
                continue
            result = _sync_table(
                conn,
                table,
                "cloud_backup",
                "merge",
                manage_transaction=False,
            )
            results.append(result)
            if result.mode == "error":
                failed_table = table
                raise RuntimeError(result.detail)
        metadata_results = _sync_data_metadata_bundle(
            conn,
            "cloud_backup",
            manage_transaction=False,
        )
        results.extend(metadata_results)
        metadata_error = next(
            (result for result in metadata_results if result.mode == "error"),
            None,
        )
        if metadata_error is not None:
            failed_table = metadata_error.table
            raise RuntimeError(metadata_error.detail)
        conn.execute("COMMIT")
        transaction_started = False
        conn.execute("DETACH cloud_backup")
        conn.execute("CHECKPOINT")
    except Exception as e:
        logger.exception("research-sync 顶层失败")
        if transaction_started:
            try:
                conn.execute("ROLLBACK")
                transaction_started = False
            except duckdb.Error:
                logger.exception("research-sync 跨表事务回滚失败")
        if failed_table is None:
            results.append(
                TableSyncResult(
                    table="<sync>", mode="error", detail=str(e)[:200]
                )
            )
        else:
            rollback_reason = f"rolled back after {failed_table} failed"
            results = [
                result.model_copy(
                    update={
                        "mode": (
                            "error" if result.table == failed_table else "skipped"
                        ),
                        "rows": 0,
                        "detail": (
                            f"{result.detail}; all primary changes rolled back"
                            if result.table == failed_table
                            else rollback_reason
                        )[:200],
                    }
                )
                for result in results
            ]
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
    绝不覆盖本地。linked dataset metadata 默认跳过，也不允许单表恢复，
    避免破坏引用与生命周期。非法表名仍抛 ValueError（调用方约定错误
    应当炸），其余顶层失败转成 has_errors 的报告返回。
    """
    db_path = db_path or settings.duckdb_path
    if tables is None:
        tables = [
            table for table in MERGE_TABLES if table not in DATA_METADATA_TABLES
        ]
    else:
        metadata_tables = [
            table for table in tables if table in DATA_METADATA_TABLES
        ]
        if metadata_tables:
            raise ValueError(
                "linked dataset metadata bundle cannot be restored partially: "
                f"{metadata_tables}"
            )

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
