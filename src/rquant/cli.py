"""CLI 入口：rquant serve / rquant run-daily / rquant ingest。"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Sequence
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from types import FrameType, TracebackType
from zoneinfo import ZoneInfo

from loguru import logger

from rquant.backfill_state import BackfillStateStore, UnknownManifestError
from rquant.logging import setup_logging
from rquant.storage.duckdb import DuckDBStore, open_readonly_store

# 重试配置
_RETRY_COUNT = 3
_RETRY_INTERVAL = 900  # 数据未就绪：15 分钟（等 tushare 数据出来）
_NETWORK_RETRY_INTERVAL = 60  # 网络异常：1 分钟（tushare 抖动通常很快恢复）


def _ingest_with_retry(trade_date: str) -> int:
    """拉取数据，最多重试 _RETRY_COUNT 次。

    两类可重试情况：
    - ingest 抛异常（短间隔重试）：覆盖两种 tushare 故障——
      a) 网络层异常（ReadTimeout / ConnectionError，6/4 事故）；
      b) 服务端业务错误（限频 / 接口临时故障，tushare 客户端抛**裸 Exception**
         而非 RequestException）。两者都该短重试。故用 `except Exception`——
         真正的代码 bug 也会被重试，但重试耗尽后 `raise` 抛出不吞（daily 非实时，
         延迟暴露可接受），换取对 tushare 抖动的鲁棒性。
    - 数据未就绪（bar_count == 0）：非交易日或 tushare 数据当天还没出，长间隔重试。
    """
    from rquant.ingest import ingest_daily

    for attempt in range(1, _RETRY_COUNT + 1):
        try:
            bar_count = ingest_daily(trade_date)
        except Exception as e:
            if attempt < _RETRY_COUNT:
                logger.warning(
                    f"ingest 异常 {type(e).__name__}，"
                    f"{_NETWORK_RETRY_INTERVAL}s 后重试 "
                    f"({attempt}/{_RETRY_COUNT}): {e}"
                )
                time.sleep(_NETWORK_RETRY_INTERVAL)
                continue
            logger.error(
                f"{trade_date} ingest 重试 {_RETRY_COUNT} 次仍失败: {e}"
            )
            raise

        if bar_count > 0:
            return bar_count
        if attempt < _RETRY_COUNT:
            logger.warning(
                f"数据未就绪，{_RETRY_INTERVAL // 60} 分钟后重试 "
                f"({attempt}/{_RETRY_COUNT})"
            )
            time.sleep(_RETRY_INTERVAL)

    logger.error(f"{trade_date} 数据拉取失败（重试 {_RETRY_COUNT} 次后仍无数据）")
    return 0


def _bridge_apscheduler_logging() -> None:
    """将 APScheduler 的标准 logging 桥接到 loguru。"""
    import logging

    class LoguruHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            level = record.levelname
            logger.opt(depth=6, exception=record.exc_info).log(level, record.getMessage())

    logging.getLogger("apscheduler").handlers = [LoguruHandler()]
    logging.getLogger("apscheduler").setLevel(logging.INFO)


def _parse_hhmm(value: str) -> dtime:
    """解析 CLI 的 HH:MM 时间。"""
    try:
        hour, minute = value.split(":", 1)
        return dtime(int(hour), int(minute))
    except ValueError as e:
        msg = f"时间格式应为 HH:MM: {value}"
        raise argparse.ArgumentTypeError(msg) from e


def _parse_iso_date(value: str) -> date:
    """Parse the exact YYYY-MM-DD CLI form."""
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"日期格式应为 YYYY-MM-DD: {value}") from e
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError(f"日期格式应为 YYYY-MM-DD: {value}")
    return parsed


def _parse_iso_datetime(value: str) -> datetime:
    """Parse an ISO-8601 instant and normalize it to UTC."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"时间格式应为带时区 ISO-8601: {value}"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError(f"时间必须显式包含时区: {value}")
    return parsed.astimezone(UTC)


def _parse_sha256(value: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64:
        raise argparse.ArgumentTypeError("plan id 必须是 64 位 SHA256")
    try:
        int(normalized, 16)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("plan id 必须是 64 位 SHA256") from exc
    return normalized


_SHANGHAI = ZoneInfo("Asia/Shanghai")
_BACKFILL_PROTECTED_START = dtime(9, 15)
_BACKFILL_PROTECTED_END = dtime(15, 10)
_SNAPSHOT_BINDING_ESTIMATED_SECONDS = 1_800.0
_SNAPSHOT_DEADLINE_MARGIN_SECONDS = 60


class _SnapshotWriteDeadlineError(RuntimeError):
    pass


def _snapshot_now() -> datetime:
    return datetime.now(UTC)


def _in_backfill_protected_window(now: datetime) -> bool:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("protected-window time must be timezone-aware")
    local = now.astimezone(_SHANGHAI)
    return (
        local.weekday() < 5
        and _BACKFILL_PROTECTED_START
        <= local.time()
        <= _BACKFILL_PROTECTED_END
    )


def _next_backfill_protected_start(now: datetime) -> datetime:
    local = now.astimezone(_SHANGHAI)
    candidate_date = local.date()
    if local.weekday() >= 5 or local.time() >= _BACKFILL_PROTECTED_START:
        candidate_date += timedelta(days=1)
    while candidate_date.weekday() >= 5:
        candidate_date += timedelta(days=1)
    return datetime.combine(
        candidate_date,
        _BACKFILL_PROTECTED_START,
        tzinfo=_SHANGHAI,
    )


def _backfill_write_window_safe(now: datetime, estimated_seconds: float) -> bool:
    """Keep a conservative long DuckDB writer away from monitor hours."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("backfill write-window time must be timezone-aware")
    local = now.astimezone(_SHANGHAI)
    if _in_backfill_protected_window(local):
        return False
    conservative_seconds = max(0.0, estimated_seconds) * 2 + 1_800
    expected_finish = local + timedelta(seconds=conservative_seconds)
    return expected_finish < _next_backfill_protected_start(local)


def _dataset_snapshot_write_window_safe(
    now: datetime,
    estimated_seconds: float,
) -> bool:
    """Keep snapshot publication out of monitor hours on every environment."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("snapshot write-window time must be timezone-aware")
    local = now.astimezone(_SHANGHAI)
    if local.weekday() >= 5:
        return True
    return _backfill_write_window_safe(
        local,
        max(_SNAPSHOT_BINDING_ESTIMATED_SECONDS, estimated_seconds),
    )


def _dataset_snapshot_apply_deadline(now: datetime) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("snapshot deadline time must be timezone-aware")
    local = now.astimezone(_SHANGHAI)
    return _next_backfill_protected_start(local) - timedelta(
        seconds=_SNAPSHOT_DEADLINE_MARGIN_SECONDS
    )


def _require_snapshot_before_deadline(deadline: datetime | None) -> None:
    if deadline is not None and _snapshot_now().astimezone(_SHANGHAI) >= deadline:
        raise _SnapshotWriteDeadlineError(
            "dataset snapshot execution reached the protected-window deadline"
        )


class _SnapshotDeadlineStoreContext:
    def __init__(
        self,
        context: AbstractContextManager[DuckDBStore],
        *,
        deadline: datetime | None,
    ) -> None:
        self._context = context
        self.deadline = deadline
        self.expired = False
        self._previous_handler: object | None = None
        self._previous_timer: tuple[float, float] | None = None

    def _handle_deadline(
        self,
        _signum: int,
        _frame: FrameType | None,
    ) -> None:
        raise _SnapshotWriteDeadlineError(
            "dataset snapshot execution reached the protected-window deadline"
        )

    def _arm(self) -> None:
        if self.deadline is None:
            return
        remaining = (
            self.deadline
            - _snapshot_now().astimezone(_SHANGHAI)
        ).total_seconds()
        if remaining <= 0:
            raise _SnapshotWriteDeadlineError(
                "dataset snapshot execution reached the protected-window deadline"
            )
        self._previous_handler = signal.getsignal(signal.SIGALRM)
        signal.signal(signal.SIGALRM, self._handle_deadline)
        self._previous_timer = signal.setitimer(signal.ITIMER_REAL, remaining)

    def _disarm(self) -> None:
        if self.deadline is None or self._previous_handler is None:
            return
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, self._previous_handler)
        if self._previous_timer is not None and self._previous_timer[0] > 0:
            signal.setitimer(
                signal.ITIMER_REAL,
                self._previous_timer[0],
                self._previous_timer[1],
            )
        self._previous_handler = None
        self._previous_timer = None

    def __enter__(self) -> DuckDBStore:
        self._arm()
        try:
            return self._context.__enter__()
        except Exception:
            self._disarm()
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        try:
            suppressed = self._context.__exit__(exc_type, exc, traceback)
        finally:
            self._disarm()
        if exc_type is not None and issubclass(
            exc_type,
            _SnapshotWriteDeadlineError,
        ):
            self.expired = True
            return True
        return bool(suppressed)


def _run_deadline_supervised_process(
    command: Sequence[str],
    *,
    deadline: datetime,
) -> int:
    remaining = (
        deadline
        - _snapshot_now().astimezone(_SHANGHAI)
    ).total_seconds()
    if remaining <= 0:
        logger.error("dataset snapshot worker deadline already elapsed")
        return 2
    try:
        result = subprocess.run(
            list(command),
            check=False,
            timeout=remaining,
        )
    except subprocess.TimeoutExpired:
        logger.error(
            "dataset snapshot worker was killed at the protected-window deadline"
        )
        return 2
    return int(result.returncode)


def _run_dataset_snapshot_supervised_worker(
    args: argparse.Namespace,
    *,
    deadline: datetime,
) -> int:
    command = (
        sys.executable,
        "-m",
        "rquant.cli",
        "dataset-snapshot",
        "--strategy",
        str(args.strategy),
        "--as-of",
        args.as_of.isoformat(),
        "--manifest-id",
        str(args.manifest_id),
        "--apply",
        "--deadline-worker",
    )
    return _run_deadline_supervised_process(
        command,
        deadline=deadline,
    )


class _RQuantArgumentParser(argparse.ArgumentParser):
    def parse_args(
        self,
        args: list[str] | None = None,
        namespace: argparse.Namespace | None = None,
    ) -> argparse.Namespace:
        parsed = super().parse_args(args, namespace)
        if getattr(parsed, "command", None) == "zt-pool-repair":
            apply_requested = bool(getattr(parsed, "apply", False))
            plan_supplied = getattr(parsed, "plan_id", None) is not None
            if apply_requested != plan_supplied:
                self.error("真正执行修复必须同时传 --apply 和 --plan-id")
        return parsed


def cmd_serve(args: argparse.Namespace) -> int:
    """启动 APScheduler 常驻进程。"""
    from apscheduler.schedulers.blocking import BlockingScheduler

    from rquant.pipeline import run_daily_pipeline

    setup_logging()
    _bridge_apscheduler_logging()

    scheduler = BlockingScheduler()

    @scheduler.scheduled_job(
        "cron", hour=args.hour, minute=0, day_of_week="mon-fri",
        misfire_grace_time=7200,  # 允许延迟 2 小时仍执行
        coalesce=True,            # 多次 misfire 合并为一次执行
    )
    def daily_job() -> None:
        trade_date = date.today().isoformat()
        logger.info(f"=== 每日任务开始 {trade_date} ===")

        try:
            bar_count = _ingest_with_retry(trade_date)
            if bar_count == 0:
                logger.warning(f"{trade_date} 非交易日或数据未就绪，跳过筛选")
                return

            logger.info(f"数据就绪（{bar_count} 行），开始筛选...")
            summary = run_daily_pipeline(trade_date)
            logger.info(f"=== 每日任务完成: {summary} ===")
        except Exception as e:
            logger.exception(f"=== 每日任务异常 {trade_date} ===")
            from rquant.notify import notify
            notify("error", component="daily_job", exc=e)

    def handle_signal(signum: int, frame: object) -> None:
        logger.info("收到退出信号，正在关闭调度器...")
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info(f"调度器启动，每日 {args.hour}:00 (Mon-Fri) 执行")
    scheduler.start()
    return 0


def cmd_run_daily(args: argparse.Namespace) -> int:
    """一次性执行 ingest + 全流水线。"""
    from rquant.pipeline import run_daily_pipeline

    setup_logging()
    trade_date = args.date or date.today().isoformat()
    preset_names = [args.preset] if args.preset else None

    if not args.no_ingest:
        logger.info(f"拉取数据: {trade_date}")
        bar_count = _ingest_with_retry(trade_date)
        if bar_count == 0:
            logger.warning("无数据（非交易日或数据未就绪）")
            return 1

    logger.info(f"执行流水线: {trade_date}")
    summary = run_daily_pipeline(
        trade_date,
        preset_names=preset_names,
        minute_backfill=not args.skip_minute_backfill,
        minute_backfill_lookback_days=args.minute_lookback_days,
    )

    if not summary:
        logger.warning("无结果")
        return 1

    for name, count in summary.items():
        logger.info(f"  {name}: {count} 命中")
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    """仅拉取数据，不跑筛选。"""
    setup_logging()
    trade_date = args.date or date.today().isoformat()

    logger.info(f"拉取数据: {trade_date}")
    bar_count = _ingest_with_retry(trade_date)

    if bar_count == 0:
        logger.warning("无数据（非交易日或数据未就绪）")
        return 1

    logger.info(f"完成: {bar_count} 行 daily_bar")
    return 0


def cmd_daily_indicator_backfill(args: argparse.Namespace) -> int:
    """Preview or rebuild daily indicators from local daily facts."""
    from rquant.indicator_backfill import (
        DailyIndicatorBackfillProtectedWindowError,
        backfill_daily_indicators,
        require_daily_indicator_write_window,
    )

    setup_logging()
    if args.apply:
        try:
            require_daily_indicator_write_window()
        except DailyIndicatorBackfillProtectedWindowError as error:
            logger.error(str(error))
            return 2
    context = DuckDBStore() if args.apply else open_readonly_store()
    try:
        with context as store:
            result = backfill_daily_indicators(
                store,
                start_date=args.start_date,
                end_date=args.end_date,
                apply=args.apply,
            )
    except DailyIndicatorBackfillProtectedWindowError as error:
        logger.error(str(error))
        return 2
    print(result.model_dump_json(indent=2))
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    """启动盘中实时监控。"""
    from rquant.monitor import run_monitor

    setup_logging()
    return run_monitor(interval=args.interval)


def _split_ts_codes(values: list[str]) -> list[str]:
    codes: list[str] = []
    for value in values:
        codes.extend(code.strip() for code in value.split(",") if code.strip())
    return codes


def cmd_rt_minute_fetch(args: argparse.Namespace) -> int:
    """拉取 Tushare 实时分钟最新 K 线并写入 minute_bar。"""
    from rquant.adapter.tushare import TushareAdapter

    setup_logging()
    ts_codes = _split_ts_codes(args.ts_code)
    if not ts_codes:
        logger.warning("未提供 ts_code，跳过")
        return 0

    df = TushareAdapter().rt_min(ts_codes, freq=args.freq)
    if df.empty:
        logger.warning("rt_min 返回空，未写入 minute_bar")
        return 0

    with DuckDBStore() as store:
        rows = store.upsert_minute_bars(df)
    latest_time = df["trade_time"].max()
    logger.info(
        f"rt_min 写入 minute_bar: rows={rows}, codes={len(ts_codes)}, latest={latest_time}"
    )
    return 0


def cmd_rt_minute_daily_fetch(args: argparse.Namespace) -> int:
    """拉取 Tushare 当日累计实时分钟 K 线并写入 minute_bar。"""
    from rquant.adapter.tushare import TushareAdapter

    setup_logging()
    ts_codes = _split_ts_codes(args.ts_code)
    if not ts_codes:
        logger.warning("未提供 ts_code，跳过")
        return 0

    df = TushareAdapter().rt_min_daily(ts_codes, freq=args.freq)
    if df.empty:
        logger.warning("rt_min_daily 返回空，未写入 minute_bar")
        return 0

    with DuckDBStore() as store:
        rows = store.upsert_minute_bars(df)
    latest_time = df["trade_time"].max()
    logger.info(
        "rt_min_daily 写入 minute_bar: "
        f"rows={rows}, codes={len(ts_codes)}, latest={latest_time}"
    )
    return 0


def cmd_research_sync(args: argparse.Namespace) -> int:
    """云端备份合并进本地研究库 / 从旧库恢复研究表。"""
    from rquant.research_sync import restore_research_tables, sync_from_backup

    setup_logging()
    refresh = not args.no_refresh_replica

    if args.restore_from:
        tables = args.tables.split(",") if args.tables else None
        report = restore_research_tables(
            Path(args.restore_from), tables=tables, refresh_replica=refresh
        )
    else:
        backup = Path(args.backup) if args.backup else None
        report = sync_from_backup(backup, refresh_replica=refresh)

    for t in report.tables:
        mark = {"replace": "替换", "merge": "合并", "skipped": "跳过", "error": "失败"}[t.mode]
        logger.info(f"  {t.table}: {mark} {t.rows:,} 行 {t.detail}")
    logger.info(
        f"replica: {'已刷新' if report.replica_refreshed else report.replica_detail}"
    )
    return 1 if report.has_errors else 0


def cmd_research_export(args: argparse.Namespace) -> int:
    """Export validated research partitions from the local read-only replica."""
    from rquant.config import settings
    from rquant.research_catalog import ResearchCatalog, exclusive_file_lock
    from rquant.research_lake import export_research_dataset
    from rquant.research_manifest import detect_code_commit
    from rquant.storage.duckdb import open_readonly_connection

    catalog_path = settings.research_db_path_resolved
    lake_root = settings.research_lake_dir_resolved
    connection = open_readonly_connection(require_replica=True)
    try:
        publish_guard = (
            nullcontext()
            if args.dry_run
            else exclusive_file_lock(settings.data_dir / "research-publish.lock")
        )
        with publish_guard:
            authority_markers = (
                settings.data_dir / "research-authority-candidate.json",
                settings.data_dir / "research-authority-current.json",
            )
            if not args.dry_run and any(
                marker.exists() or marker.is_symlink() for marker in authority_markers
            ):
                logger.error(
                    "研究 authority 已建立；正式目录禁止直接 research-export，"
                    "请使用 research-ingest 维护每日证据链"
                )
                return 3
            summary = export_research_dataset(
                connection,
                catalog=ResearchCatalog(catalog_path),
                lake_root=lake_root,
                dataset=args.dataset,
                start_date=args.start_date,
                end_date=args.end_date,
                code_commit=detect_code_commit() or "unknown",
                dry_run=args.dry_run,
            )
    finally:
        connection.close()
    print(summary.model_dump_json(indent=2))
    return 0


def cmd_research_ingest(args: argparse.Namespace) -> int:
    """Seal one cloud research day and publish a fail-closed observation."""
    from rquant.config import settings
    from rquant.research_ingest import (
        ResearchIngestPaths,
        ResearchIngestSkipResult,
        research_trade_date_is_open,
        run_daily_research_ingest,
    )
    from rquant.research_manifest import detect_code_commit

    if args.recover and args.date is None:
        raise ValueError("research-ingest --recover requires --date")
    if args.scheduled and args.date is None:
        raise ValueError("research-ingest --scheduled requires --date")
    if args.recover and args.scheduled:
        raise ValueError("research-ingest --recover and --scheduled are mutually exclusive")
    if not args.dry_run and not settings.research_cloud_ingest_enabled:
        logger.error(
            "研究云增量开关未开启；设置 RESEARCH_CLOUD_INGEST_ENABLED=true 后再执行"
        )
        return 3
    trade_date = args.date or datetime.now(_SHANGHAI).date()
    source_database = settings.duckdb_readonly_path_resolved
    if (args.date is None or args.scheduled) and not research_trade_date_is_open(
        source_database,
        trade_date,
    ):
        print(ResearchIngestSkipResult(trade_date=trade_date).model_dump_json())
        return 0
    adapter = None
    if not args.dry_run:
        from rquant.adapter.tushare import TushareAdapter

        adapter = TushareAdapter()
    result = run_daily_research_ingest(
        source_database=source_database,
        paths=ResearchIngestPaths(
            state_dir=settings.data_dir,
            catalog_path=settings.research_db_path_resolved,
            readonly_catalog_path=settings.research_readonly_db_path_resolved,
            lake_root=settings.research_lake_dir_resolved,
            staging_root=settings.research_staging_dir_resolved,
        ),
        trade_date=trade_date,
        adapter=adapter,
        code_commit=detect_code_commit() or "unknown",
        dry_run=args.dry_run,
        recovery=args.recover,
    )
    print(result.model_dump_json(indent=2))
    return 2 if result.status == "degraded" else 0


def cmd_research_ingest_readiness(args: argparse.Namespace) -> int:
    """Check that the refreshed operational replica is ready for research ingest."""
    from rquant.config import settings
    from rquant.research_ingest import assess_research_ingest_readiness

    trade_date = args.date or datetime.now(_SHANGHAI).date()
    result = assess_research_ingest_readiness(
        settings.duckdb_readonly_path_resolved,
        trade_date,
    )
    print(result.model_dump_json(indent=2))
    return 1 if result.status == "not_ready" else 0


def cmd_research_authority_status(args: argparse.Namespace) -> int:
    """Verify the rolling research candidate without opening production DuckDB."""
    from rquant.config import settings
    from rquant.research_ingest import ResearchIngestPaths, inspect_research_authority

    paths = (
        ResearchIngestPaths.from_data_dir(args.data_dir)
        if args.data_dir is not None
        else ResearchIngestPaths(
            state_dir=settings.data_dir,
            catalog_path=settings.research_db_path_resolved,
            readonly_catalog_path=settings.research_readonly_db_path_resolved,
            lake_root=settings.research_lake_dir_resolved,
            staging_root=settings.research_staging_dir_resolved,
        )
    )
    status = inspect_research_authority(paths)
    print(status.model_dump_json(indent=2))
    return 1 if status.status in {"missing", "invalid"} else 0


def cmd_research_migration(args: argparse.Namespace) -> int:
    """Run one explicit, resumable phase of the research cloud bootstrap."""
    from rquant.research_migration import (
        create_recovery_snapshot,
        prepare_research_migration_bundle,
        publish_research_migration_bundle,
        verify_research_migration_bundle,
    )

    if args.migration_command == "snapshot":
        result = create_recovery_snapshot(
            args.source_database,
            recovery_dir=args.recovery_dir,
            artifact_dir=args.artifact_dir,
            snapshot_id=args.snapshot_id,
            code_commit=args.code_commit,
        )
    elif args.migration_command == "prepare":
        result = prepare_research_migration_bundle(
            args.source_snapshot,
            bundle_dir=args.bundle_dir,
            artifact_dir=args.artifact_dir,
            snapshot_id=args.snapshot_id,
            code_commit=args.code_commit,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    elif args.migration_command == "verify":
        result = verify_research_migration_bundle(args.bundle_path)
    elif args.migration_command == "publish":
        result = publish_research_migration_bundle(
            args.bundle_path,
            target_data_dir=args.target_data_dir,
        )
    else:  # pragma: no cover - argparse requires one supported subcommand
        raise ValueError(f"unsupported research migration phase: {args.migration_command}")
    print(result.model_dump_json(indent=2))
    return 0


def cmd_trade_calendar_bootstrap(args: argparse.Namespace) -> int:
    """Bootstrap a complete authoritative SSE civil-date calendar."""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.trade_calendar import (
        fetch_trade_calendar_rows,
        persist_verified_trade_calendar,
    )

    setup_logging()
    if args.start_date > args.end_date:
        logger.error("trade calendar start date must not be after end date")
        return 2

    rows = fetch_trade_calendar_rows(
        TushareAdapter(),
        exchange="SSE",
        start=args.start_date,
        end=args.end_date,
    )
    with DuckDBStore() as store:
        result = persist_verified_trade_calendar(
            store,
            rows,
            exchange="SSE",
            start=args.start_date,
            end=args.end_date,
        )
    logger.info(
        "trade calendar bootstrap complete: "
        f"exchange=SSE, range={args.start_date}..{args.end_date}, "
        f"processed={result.upserted_days}, verified={result.requested_days}"
    )
    return 0


def cmd_sentiment_recompute(args: argparse.Namespace) -> int:
    """重算市场情绪/温度区间。"""
    from rquant.market_context import recompute_market_sentiment_range

    setup_logging()
    rows = recompute_market_sentiment_range(args.start_date, args.end_date)
    logger.info(f"market_sentiment_daily 重算完成: {rows} 行 ({args.start_date} ~ {args.end_date})")
    return 0


def cmd_moneyflow_backfill(args: argparse.Namespace) -> int:
    """拉取 Tushare 日级资金流并写入 moneyflow_daily。"""
    from rquant.adapter.tushare import TushareAdapter

    setup_logging()
    trade_date = date.fromisoformat(args.date)
    df = TushareAdapter().moneyflow(trade_date)
    if df.empty:
        logger.warning("moneyflow 返回空，未写入 moneyflow_daily")
        return 0

    with DuckDBStore() as store:
        rows = store.upsert_moneyflow_daily(df)
    logger.info(f"moneyflow 写入 moneyflow_daily: rows={rows}, date={args.date}")
    return 0


def cmd_market_daily_backfill(args: argparse.Namespace) -> int:
    """全市场日线历史回补，再对失效的 daily_state 做一次重算。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.market_backfill import backfill_market_daily, recompute_daily_state

    setup_logging()
    summary = backfill_market_daily(
        args.start_date,
        args.end_date,
        TushareAdapter(),
        store_factory=DuckDBStore,
        dry_run=args.dry_run,
    )
    logger.info(summary)
    affected_codes = summary.get("affected_codes", [])
    if (
        not args.dry_run
        and not args.skip_state_recompute
        and affected_codes
    ):
        with DuckDBStore() as store:
            recompute_daily_state(
                store,
                codes=affected_codes,
                status_mode="verified_no_fetch",
            )
    return 1 if summary["failed_dates"] else 0


def cmd_zt_pool_capture(args: argparse.Namespace) -> int:
    """采集当日东财涨停池到 limit_up_pool_daily。"""
    from rquant.limit_up_pool import (
        LimitUpPoolCaptureError,
        capture_zt_pool,
    )

    setup_logging()
    trade_date = date.fromisoformat(args.date) if args.date else None
    try:
        rows = capture_zt_pool(trade_date)
    except LimitUpPoolCaptureError as exc:
        logger.error(f"zt-pool-capture 被阻断: {exc}")
        return 1
    logger.info(f"zt-pool-capture 完成: rows={rows}")
    return 0


def cmd_zt_pool_repair(args: argparse.Namespace) -> int:
    """Dry-run or CAS-apply the closed-day limit-up-pool repair."""
    from rquant.data_quality import (
        LimitUpPoolRepairBlockedError,
        LimitUpPoolRepairNoOpError,
        LimitUpPoolRepairPlanMismatchError,
        apply_limit_up_pool_closed_day_repair,
        build_limit_up_pool_closed_day_repair_plan,
    )

    setup_logging()
    with DuckDBStore() as store:
        if not args.apply:
            plan = build_limit_up_pool_closed_day_repair_plan(store)
            logger.info(f"zt-pool-repair dry-run:\n{plan.model_dump_json(indent=2)}")
            return 1 if plan.status == "blocked" else 0
        try:
            result = apply_limit_up_pool_closed_day_repair(
                store,
                args.plan_id,
            )
        except (
            LimitUpPoolRepairBlockedError,
            LimitUpPoolRepairNoOpError,
            LimitUpPoolRepairPlanMismatchError,
        ) as exc:
            logger.error(f"zt-pool-repair 拒绝执行: {exc}")
            return 1
    logger.info(f"zt-pool-repair applied:\n{result.model_dump_json(indent=2)}")
    return 0


def cmd_limit_list_backfill(args: argparse.Namespace) -> int:
    """Tushare 涨跌停/炸板榜历史回补，或 --today 当日增量。"""
    from rquant.limit_list_backfill import backfill_limit_list, capture_today

    setup_logging()
    if args.today:
        with DuckDBStore() as store:
            rows = capture_today(store)
        logger.info(f"limit-list-backfill --today 完成: rows={rows}")
        return 0

    if not args.start_date or not args.end_date:
        logger.error("需要 --start-date 和 --end-date（或改用 --today 拉当天）")
        return 1
    with DuckDBStore() as store:
        summary = backfill_limit_list(
            args.start_date, args.end_date, store, dry_run=args.dry_run
        )
    logger.info(summary)
    return 1 if summary["failed_dates"] else 0


def cmd_data_backfill(args: argparse.Namespace) -> int:
    """统一数据集回补（dataset_backfill 注册表），--dataset all 跑全部。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.dataset_backfill import DATASETS, backfill_dataset

    setup_logging()
    if args.dataset != "all" and args.dataset not in DATASETS:
        logger.error(
            f"未知数据集：{args.dataset}"
            f"（可用：all, {', '.join(sorted(DATASETS))}）"
        )
        return 1
    if args.today:
        start = end = date.today().isoformat()
    elif args.start_date and args.end_date:
        start, end = args.start_date, args.end_date
    else:
        logger.error("需要 --start-date 和 --end-date（或改用 --today 日终增量）")
        return 1

    names = list(DATASETS) if args.dataset == "all" else [args.dataset]
    adapter = TushareAdapter()
    has_failure = False
    with DuckDBStore() as store:
        for name in names:
            summary = backfill_dataset(
                name, start, end, store, adapter, dry_run=args.dry_run
            )
            logger.info(summary)
            has_failure = has_failure or bool(summary["failed_dates"])
    return 1 if has_failure else 0


def _print_json(payload: object) -> None:
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _valid_clean_commit(value: str | None) -> bool:
    if value is None or len(value) != 40:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def cmd_backfill_plan(args: argparse.Namespace) -> int:
    """Resolve PIT eligibility, plan exact minute coverage, and persist it."""
    from rquant.backfill_manifest import (
        STRATEGY_BACKFILL_SPECS,
        BackfillManifest,
        backfill_state_input,
        plan_minute_backfill,
        resolve_strategy_eligibility,
    )
    from rquant.config import settings
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_manifest import detect_code_commit
    from rquant.research_snapshot import (
        SnapshotArtifactResolver,
        resolve_strategy_eligibility_from_artifacts,
    )

    setup_logging()
    code_commit = detect_code_commit()
    if not _valid_clean_commit(code_commit):
        logger.error("backfill plan requires a clean 40-character git commit")
        return 2
    spec = STRATEGY_BACKFILL_SPECS[args.strategy]
    as_of_time = datetime.now(UTC)
    catalog = ResearchCatalog(settings.research_db_path_resolved)
    with open_readonly_store() as store:
        if args.strategy == "auction_gap":
            eligibility_artifacts = SnapshotArtifactResolver(
                catalog=catalog,
                lake_root=settings.research_lake_dir_resolved,
            ).resolve_lake_partitions(
                dataset="auction_bar",
                start_date=args.start_date,
                end_date=args.end_date,
                as_of_time=as_of_time,
            )
            if not eligibility_artifacts:
                logger.error(
                    "auction eligibility requires immutable auction_bar "
                    "research-lake partitions"
                )
                return 2
            eligibility_resolution = (
                resolve_strategy_eligibility_from_artifacts(
                    store,
                    strategy_id=args.strategy,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    input_artifacts=eligibility_artifacts,
                    lake_root=settings.research_lake_dir_resolved,
                    as_of_time=as_of_time,
                )
            )
        else:
            eligibility_resolution = resolve_strategy_eligibility(
                store,
                strategy_id=args.strategy,
                start_date=args.start_date,
                end_date=args.end_date,
            )
        manifest = BackfillManifest.build(
            spec=spec,
            start_date=args.start_date,
            end_date=args.end_date,
            as_of_time=as_of_time,
            code_commit=code_commit,
            eligibilities=eligibility_resolution.records,
            eligibility_resolution=eligibility_resolution,
        )
        plan = plan_minute_backfill(
            store,
            manifest,
            coverage_authority="combined",
            research_catalog=catalog,
            research_lake_root=settings.research_lake_dir_resolved,
        )
    BackfillStateStore().persist_manifest(backfill_state_input(plan))
    _print_json(
        {
            "manifest_id": plan.manifest.manifest_id,
            "strategy": plan.manifest.spec.strategy_id,
            "eligibility_count": len(plan.manifest.eligibilities),
            "eligibility_resolution_hash": (
                eligibility_resolution.resolution_hash
            ),
            "eligibility_expected_dates": eligibility_resolution.expected_count,
            "eligibility_complete_dates": eligibility_resolution.available_count,
            "task_count": len(plan.tasks),
            "requested_session_count": plan.requested_session_count,
            "coverage": plan.coverage.model_dump(mode="json"),
            "estimate": plan.estimate.model_dump(mode="json"),
        }
    )
    return 0


def cmd_backfill_run(args: argparse.Namespace) -> int:
    """Execute one persisted manifest without crossing monitor hours."""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.backfill_manifest import MinuteBackfillPlan
    from rquant.intraday_backfill import run_backfill_manifest

    setup_logging()
    state = BackfillStateStore()
    persisted = state.load_manifest(args.manifest_id)
    if persisted is None:
        logger.error(f"unknown backfill manifest: {args.manifest_id}")
        return 2
    status_before = state.get_manifest_status(args.manifest_id)
    if status_before.status == "completed":
        _print_json(status_before.model_dump(mode="json"))
        return 0
    if status_before.status == "failed" and not args.retry_failed:
        logger.error("manifest has failed tasks; pass --retry-failed after inspection")
        return 2

    plan = MinuteBackfillPlan.model_validate(persisted.payload)
    estimated_seconds = (
        status_before.eta_seconds
        if status_before.eta_seconds is not None
        else plan.estimate.total_seconds
    )
    now = datetime.now(UTC)
    if not _backfill_write_window_safe(now, estimated_seconds):
        logger.error(
            "backfill run would overlap the protected 09:15-15:10 monitor window"
        )
        return 2

    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    summary = run_backfill_manifest(
        None,
        state,
        TushareAdapter(),
        manifest_id=args.manifest_id,
        worker_id=worker_id,
        retry_failed=args.retry_failed,
        stop_before=_next_backfill_protected_start(now) - timedelta(minutes=5),
        store_factory=DuckDBStore,
    )
    status_after = state.get_manifest_status(args.manifest_id)
    _print_json(
        {
            "run": summary.model_dump(mode="json"),
            "status": status_after.model_dump(mode="json"),
        }
    )
    return 0 if status_after.status == "completed" else 1


def cmd_backfill_status(args: argparse.Namespace) -> int:
    """Read manifest progress from SQLite without opening DuckDB."""
    setup_logging()
    try:
        status = BackfillStateStore().get_manifest_status(args.manifest_id)
    except UnknownManifestError:
        logger.error(f"unknown backfill manifest: {args.manifest_id}")
        return 2
    payload = status.model_dump(mode="json")
    if args.json:
        _print_json(payload)
    else:
        logger.info(
            f"manifest={status.manifest_id} status={status.status} "
            f"tasks={status.succeeded}/{status.task_count} "
            f"failed={status.failed} eta={status.eta_seconds}"
        )
    if status.status != "failed":
        return 0
    return 2 if status.terminal else 1


def cmd_suspension_backfill(args: argparse.Namespace) -> int:
    """Backfill exact Tushare suspend_d snapshots for open sessions."""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.suspension import backfill_suspension_facts

    setup_logging()
    result = backfill_suspension_facts(
        TushareAdapter(),
        store_factory=DuckDBStore,
        start=args.start_date,
        end=args.end_date,
        queried_at=datetime.now(UTC),
        missing_only=not args.full_refresh,
    )
    _print_json(result.model_dump(mode="json"))
    return 0


def cmd_security_status_backfill(args: argparse.Namespace) -> int:
    """Plan or backfill historical name/ST facts for daily eligibility keys."""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.security_status import (
        backfill_historical_security_status,
        plan_historical_security_status_backfill,
    )

    setup_logging()
    source_as_of = args.source_as_of or datetime.now(_SHANGHAI).date()
    if args.dry_run:
        plan = plan_historical_security_status_backfill(
            store_factory=open_readonly_store,
            start=args.start_date,
            end=args.end_date,
            source_as_of=source_as_of,
            missing_only=not args.full_refresh,
        )
        payload = plan.model_dump(mode="json")
        payload["total_logical_api_operations"] = (
            plan.total_logical_api_operations
        )
        _print_json(payload)
        return 0

    result = backfill_historical_security_status(
        TushareAdapter(),
        store_factory=DuckDBStore,
        start=args.start_date,
        end=args.end_date,
        source_as_of=source_as_of,
        ingested_at=datetime.now(UTC),
        missing_only=not args.full_refresh,
    )
    _print_json(result.model_dump(mode="json"))
    return 0


def _watermark_text(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def cmd_dataset_snapshot(args: argparse.Namespace) -> int:
    """Finalize research metadata after recomputing exact minute coverage."""
    from rquant.backfill_manifest import (
        MinuteBackfillPlan,
        plan_minute_backfill,
        resolve_strategy_eligibility,
    )
    from rquant.config import settings
    from rquant.data_metadata import (
        DatasetCoverage,
        DatasetSnapshot,
        DatasetSnapshotFinalization,
    )
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_snapshot import (
        SnapshotArtifactResolver,
        build_dataset_snapshot_binding,
        resolve_strategy_eligibility_from_artifacts,
    )
    from rquant.strategy_dependencies import strategy_execution_dependencies

    setup_logging()
    dry_run = not bool(getattr(args, "apply", False))
    state = BackfillStateStore()
    persisted = state.load_manifest(args.manifest_id)
    if persisted is None:
        logger.error(f"unknown backfill manifest: {args.manifest_id}")
        return 2
    status = state.get_manifest_status(args.manifest_id)
    if status.status != "completed":
        logger.error(
            f"manifest must be completed before snapshot finalization: {status.status}"
        )
        return 2
    original = MinuteBackfillPlan.model_validate(persisted.payload)
    if original.manifest.spec.strategy_id != args.strategy:
        logger.error("snapshot strategy does not match the persisted manifest")
        return 2
    started_at = _snapshot_now()
    if not dry_run and not _dataset_snapshot_write_window_safe(
        started_at,
        original.estimate.total_seconds,
    ):
        logger.error(
            "dataset snapshot apply would overlap the protected "
            "09:15-15:10 market window"
        )
        return 2
    deadline = (
        None
        if dry_run
        else _dataset_snapshot_apply_deadline(started_at)
    )
    deadline_worker = bool(getattr(args, "deadline_worker", True))
    if not dry_run and not deadline_worker:
        if deadline is None:
            logger.error("weekday snapshot apply requires a worker deadline")
            return 2
        return _run_dataset_snapshot_supervised_worker(
            args,
            deadline=deadline,
        )

    store_context = open_readonly_store() if dry_run else DuckDBStore()
    guarded_store = _SnapshotDeadlineStoreContext(
        store_context,
        deadline=deadline,
    )
    catalog = ResearchCatalog(settings.research_db_path_resolved)
    with guarded_store as store:
        _require_snapshot_before_deadline(deadline)
        planned_resolution = original.manifest.eligibility_resolution
        if planned_resolution is None:
            logger.error(
                "dataset snapshot requires an independently verified "
                "eligibility resolution"
            )
            return 2
        if (
            args.strategy == "auction_gap"
            and not planned_resolution.input_artifacts
        ):
            logger.error(
                "auction snapshot requires immutable eligibility input artifacts; "
                "generate a new backfill manifest"
            )
            return 2
        if args.strategy != "auction_gap":
            live_resolution = resolve_strategy_eligibility(
                store,
                strategy_id=args.strategy,
                start_date=original.manifest.start_date,
                end_date=original.manifest.end_date,
            )
            if live_resolution.resolution_hash != planned_resolution.resolution_hash:
                logger.error(
                    "strategy eligibility changed after manifest planning; "
                    "generate a new backfill manifest before snapshot publication"
                )
                return 2
        current = plan_minute_backfill(
            store,
            original.manifest,
            coverage_authority="research_lake",
            research_catalog=catalog,
            research_lake_root=settings.research_lake_dir_resolved,
            coverage_as_of_time=args.as_of,
        )
        _require_snapshot_before_deadline(deadline)
        coverage = current.coverage
        resolution = planned_resolution
        if resolution.expected_count == 0 or resolution.coverage_ratio < 0.99:
            logger.error(
                "eligibility coverage gate failed: requested trading dates "
                "must be non-empty and >=99% complete"
            )
            return 2
        if not coverage.baseline_gate_passed or not coverage.entry_exit_gate_passed:
            logger.error(
                "dataset coverage gate failed: baseline must be >=95% and B/S >=99%"
            )
            return 2

        as_of_shanghai = args.as_of.astimezone(ZoneInfo("Asia/Shanghai"))
        if current.windows:
            required_through = datetime.combine(
                max(window.end_date for window in current.windows),
                dtime(15, 0),
                tzinfo=ZoneInfo("Asia/Shanghai"),
            )
            if as_of_shanghai < required_through:
                logger.error(
                    "snapshot as-of precedes the final required minute window: "
                    f"as_of={as_of_shanghai.isoformat()} "
                    f"required_through={required_through.isoformat()}"
                )
                return 2

        binding_start = min(
            (window.start_date for window in current.windows),
            default=current.manifest.start_date,
        )
        binding_end = max(
            (window.end_date for window in current.windows),
            default=current.manifest.end_date,
        )
        ts_codes = tuple(
            sorted(
                {
                    row.ts_code
                    for row in current.manifest.eligibilities
                }
            )
        )
        dependencies = strategy_execution_dependencies(args.strategy)
        pinned_lake_artifacts = list(current.minute_coverage_artifacts)
        resolver = SnapshotArtifactResolver(
            catalog=catalog,
            lake_root=settings.research_lake_dir_resolved,
        )
        for dataset in dependencies.lake_datasets:
            if dataset == "minute_bar":
                continue
            resolved = resolver.resolve_lake_partitions(
                dataset=dataset,
                start_date=binding_start,
                end_date=binding_end,
                as_of_time=args.as_of,
            )
            if not resolved:
                logger.error(
                    f"research lake has no {dataset} partitions in binding range"
                )
                return 2
            if dataset == "auction_bar" and resolution.input_artifacts:
                by_key = {
                    artifact.artifact_key: artifact
                    for artifact in resolved
                }
                by_key.update(
                    {
                        artifact.artifact_key: artifact
                        for artifact in resolution.input_artifacts
                    }
                )
                resolved = tuple(
                    by_key[key] for key in sorted(by_key)
                )
            pinned_lake_artifacts.extend(resolved)
        if args.strategy == "auction_gap":
            live_resolution = resolve_strategy_eligibility_from_artifacts(
                store,
                strategy_id=args.strategy,
                start_date=original.manifest.start_date,
                end_date=original.manifest.end_date,
                input_artifacts=resolution.input_artifacts,
                lake_root=settings.research_lake_dir_resolved,
                as_of_time=args.as_of,
            )
            _require_snapshot_before_deadline(deadline)
            if live_resolution.resolution_hash != resolution.resolution_hash:
                logger.error(
                    "strategy eligibility changed after manifest planning; "
                    "generate a new backfill manifest before snapshot publication"
                )
                return 2
        if dry_run:
            _print_json(
                {
                    "status": "dry_run",
                    "strategy": args.strategy,
                    "manifest_id": args.manifest_id,
                    "as_of": args.as_of.isoformat(),
                    "binding_start": binding_start.isoformat(),
                    "binding_end": binding_end.isoformat(),
                    "candidate_count": len(ts_codes),
                    "coverage": coverage.model_dump(mode="json"),
                    "lake_datasets": dependencies.lake_datasets,
                    "lake_artifact_count": len(pinned_lake_artifacts),
                    "materialized_tables": tuple(
                        dependency.table_name
                        for dependency in dependencies.materialized_tables
                    ),
                    "apply_required": True,
                }
            )
            return 0

        _require_snapshot_before_deadline(deadline)
        snapshot = DatasetSnapshot.create(
            strategy_name=args.strategy,
            manifest_id=args.manifest_id,
            as_of_time=args.as_of,
            code_commit=original.manifest.code_commit,
            origin="rquant.backfill_manifest.metadata_only",
        )
        begun = store.begin_dataset_snapshot(snapshot)
        if begun.status == "ready":
            finalized = begun
        else:
            store.upsert_dataset_coverage(
                DatasetCoverage(
                    snapshot_id=begun.snapshot_id,
                    dataset_id="strategy_eligibility",
                    coverage_scope="eligibility",
                    table_name="backfill_manifest",
                    expected_count=resolution.expected_count,
                    available_count=resolution.available_count,
                    missing_reasons=tuple(
                        sorted({row.reason for row in resolution.incomplete})
                    ),
                )
            )
            phases = {
                "baseline": coverage.baseline,
                "entry": coverage.entry,
                "exit": coverage.exit,
            }
            accepted_missing_reasons = tuple(
                sorted(
                    {
                        row.reason
                        for row in current.unavailable_sessions
                    }
                )
            )
            for scope, phase in phases.items():
                missing_reasons = tuple(
                    reason
                    for reason, applies in (
                        *(
                            (reason, phase.accepted_missing_sessions > 0)
                            for reason in accepted_missing_reasons
                        ),
                        (
                            "incomplete_full_session",
                            phase.satisfied_sessions < phase.expected_sessions,
                        ),
                    )
                    if applies
                )
                store.upsert_dataset_coverage(
                    DatasetCoverage(
                        snapshot_id=begun.snapshot_id,
                        dataset_id="minute_bar:tushare:1min",
                        coverage_scope=scope,
                        table_name="minute_bar",
                        expected_count=phase.expected_sessions,
                        available_count=phase.satisfied_sessions,
                        missing_reasons=missing_reasons,
                    )
                )
            minute_row = store._conn.execute(
                "SELECT MAX(trade_time) FROM minute_bar "
                "WHERE source = 'tushare' AND freq = '1min' "
                "AND trade_time <= ?",
                [as_of_shanghai.replace(tzinfo=None)],
            ).fetchone()
            calendar_row = store._conn.execute(
                "SELECT MAX(cal_date) FROM trade_calendar "
                "WHERE exchange = 'SSE' AND cal_date <= ?",
                [as_of_shanghai.date()],
            ).fetchone()
            watermarks: dict[str, str] = {}
            watermarks["manifest_start_date"] = (
                current.manifest.start_date.isoformat()
            )
            watermarks["manifest_end_date"] = current.manifest.end_date.isoformat()
            watermarks["eligibility_resolution_hash"] = (
                resolution.resolution_hash
            )
            if minute_row and minute_row[0] is not None:
                watermarks["minute_bar"] = _watermark_text(minute_row[0])
            if calendar_row and calendar_row[0] is not None:
                watermarks["trade_calendar"] = _watermark_text(calendar_row[0])
            finalized = store.finalize_dataset_snapshot(
                begun.snapshot_id,
                DatasetSnapshotFinalization(
                    table_watermarks=watermarks,
                    completed_at=datetime.now(UTC),
                ),
            )
        _require_snapshot_before_deadline(deadline)
        binding = build_dataset_snapshot_binding(
            metadata_store=store,
            source_connection=store._conn,
            catalog=ResearchCatalog(settings.research_db_path_resolved),
            lake_root=settings.research_lake_dir_resolved,
            snapshot_id=finalized.snapshot_id,
            start_date=binding_start,
            end_date=binding_end,
            ts_codes=ts_codes,
            dependencies=dependencies,
            lake_artifacts=tuple(pinned_lake_artifacts),
            eligibility_resolution=resolution,
        )
        _require_snapshot_before_deadline(deadline)
    if guarded_store.expired:
        logger.error(
            "dataset snapshot apply stopped before the protected "
            "09:15-15:10 market window"
        )
        return 2
    _print_json(
        {
            "snapshot_id": finalized.snapshot_id,
            "status": finalized.status,
            "binding_hash": binding.binding_hash,
            "scope": "immutable execution binding",
        }
    )
    return 0


def cmd_data_audit(args: argparse.Namespace) -> int:
    """Run and persist the Stage-1 research data audit as positive evidence."""
    from rquant.data_metadata import DataAuditRun, DataAuditRunFinalization
    from rquant.data_quality import (
        STAGE1_AUDIT_RULE_SET_VERSION,
        build_stage1_audit_rules,
        record_audit_report,
        resolve_audit_issues,
        run_audit,
    )

    setup_logging()
    range_start = args.start_date or (args.as_of - timedelta(days=120))
    if range_start > args.as_of:
        _print_json(
            {
                "status": "failed",
                "error": "start_date cannot be after as_of",
            }
        )
        return 2
    observed_at = datetime.now(UTC)
    audit_run = DataAuditRun.create(
        as_of_date=args.as_of,
        range_start=range_start,
        range_end=args.as_of,
        rule_set_version=STAGE1_AUDIT_RULE_SET_VERSION,
        observed_at=observed_at,
    )
    try:
        with DuckDBStore() as writable:
            writable.begin_data_audit_run(audit_run)
        rules = build_stage1_audit_rules(range_start, args.as_of)
        # Positive research evidence must observe the primary generation, not a
        # replica that may legitimately trail it by several minutes.
        with DuckDBStore(read_only=True) as readonly:
            report = run_audit(readonly, rules, observed_at=observed_at)
        current_issue_ids = set(report.issue_ids)
        with DuckDBStore() as writable:
            previously_open = writable.list_open_data_quality_issues(
                rule_ids=report.rule_ids
            )
            record_audit_report(writable, report)
            resolve_audit_issues(
                writable,
                [
                    issue.issue_id
                    for issue in previously_open
                    if issue.issue_id not in current_issue_ids
                    and _quality_issue_matches_audit_range(
                        issue.scope_key,
                        range_start,
                        args.as_of,
                    )
                ],
                resolved_at=observed_at,
            )
            finalized = writable.finalize_data_audit_run(
                audit_run.audit_run_id,
                DataAuditRunFinalization(
                    finding_issue_ids=report.issue_ids,
                    p0_count=sum(
                        1 for finding in report.findings if finding.severity == "P0"
                    ),
                    completed_at=datetime.now(UTC),
                ),
            )
    except Exception as exc:
        try:
            with DuckDBStore() as writable:
                writable.fail_data_audit_run(
                    audit_run.audit_run_id,
                    error_message=f"{type(exc).__name__}: {exc}",
                )
        except Exception:
            logger.exception("failed to persist failed data audit evidence")
        _print_json(
            {
                "audit_run_id": audit_run.audit_run_id,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        return 2

    _print_json(
        {
            "audit_run_id": finalized.audit_run_id,
            "status": finalized.status,
            "as_of_date": finalized.as_of_date.isoformat(),
            "range_start": finalized.range_start.isoformat(),
            "range_end": finalized.range_end.isoformat(),
            "rule_set_version": finalized.rule_set_version,
            "finding_count": len(finalized.finding_issue_ids),
            "p0_count": finalized.p0_count,
        }
    )
    return 1 if finalized.p0_count else 0


def _quality_issue_matches_audit_range(
    scope_key: str,
    range_start: date,
    range_end: date,
) -> bool:
    """Only let a clean rerun resolve findings owned by the exact same range."""
    parts = scope_key.split("/")
    expected = (range_start.isoformat(), range_end.isoformat())
    return any(tuple(parts[index : index + 2]) == expected for index in range(len(parts) - 1))


def cmd_minute_backfill(args: argparse.Namespace) -> int:
    """回补 Pool 命中标的历史分钟线。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.intraday_backfill import backfill_pool1_minute_context
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()
    with DuckDBStore() as store:
        summary = backfill_pool1_minute_context(
            store,
            TushareAdapter(),
            screen_date=args.date,
            lookback_days=args.lookback_days,
            freq=args.freq,
            preset_name=args.preset,
            ts_code=args.ts_code,
            dry_run=args.dry_run,
        )
    logger.info(summary.model_dump())
    return 0


def cmd_minute_replay_backfill(args: argparse.Namespace) -> int:
    """回补分钟 replay 所需的 B 日到退出窗口分钟线。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.intraday_backfill import backfill_minute_replay_window
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()
    with DuckDBStore() as store:
        summary = backfill_minute_replay_window(
            store,
            TushareAdapter(),
            start_date=args.start_date,
            end_date=args.end_date,
            max_hold_days=args.max_hold_days,
            freq=args.freq,
            preset_name=args.preset,
            ts_code=args.ts_code,
            dry_run=args.dry_run,
        )
    logger.info(summary.model_dump())
    return 0


def cmd_auction_backfill(args: argparse.Namespace) -> int:
    """回补 Tushare 集合竞价数据。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.auction_backfill import backfill_stk_auction
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()
    with DuckDBStore() as store:
        summary = backfill_stk_auction(
            store,
            TushareAdapter(),
            start_date=args.start_date,
            end_date=args.end_date,
            dry_run=args.dry_run,
        )
    logger.info(summary.model_dump())
    return 1 if summary.failed_requests else 0


def cmd_auction_minute_fallback(args: argparse.Namespace) -> int:
    """用 09:30 分钟线补齐集合竞价缺行。"""
    from rquant.auction_backfill import synthesize_open_auction_from_minute

    setup_logging()
    with DuckDBStore() as store:
        summary = synthesize_open_auction_from_minute(
            store,
            args.date,
            dry_run=args.dry_run,
        )
    logger.info(summary.model_dump())
    return 0


def cmd_auction_gap_replay(args: argparse.Namespace) -> int:
    """回测集合竞价跳空策略。"""
    from rquant.auction_gap_strategy import (
        AuctionGapConfig,
        run_auction_gap_replay,
        summarize_auction_gap_replay,
    )

    setup_logging()
    config = AuctionGapConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        gap_mode=args.gap_mode,
        min_auction_vol_ratio_5d=args.min_ratio,
        max_auction_vol_ratio_5d=args.max_ratio,
        st_filter=args.st_filter,
    )
    with open_readonly_store(
        required_tables=[
            "auction_bar",
            "daily_bar",
            "daily_state",
            "stock_status_daily",
        ]
    ) as store:
        trades = run_auction_gap_replay(store, config)

    summary = summarize_auction_gap_replay(trades)
    logger.info(summary.model_dump())

    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(output, index=False)
        logger.info(f"auction gap replay 结果已写出: {output}")

    if trades.empty:
        logger.warning("auction gap replay 无候选")
        return 0

    preview_cols = [
        "signal_date", "ts_code", "name", "entry_price",
        "auction_vol_ratio_5d", "gap_pct_close", "gap_pct_high",
        "hit_limit_up_today", "intraday_high_ret_pct",
        "next_trade_date", "next_open_ret_pct", "next_close_ret_pct",
    ]
    available_cols = [col for col in preview_cols if col in trades.columns]
    logger.info("\n" + trades[available_cols].tail(20).to_string(index=False))
    return 0


def cmd_auction_gap_minute_replay(args: argparse.Namespace) -> int:
    """回测集合竞价候选 + 分钟 B/S 策略。"""
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
        run_auction_gap_replay,
        summarize_auction_gap_minute_replay,
    )

    setup_logging()
    seal_hold_enabled = args.seal_hold_days is not None
    config = AuctionGapMinuteReplayConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        gap_mode=args.gap_mode,
        min_auction_vol_ratio_5d=args.min_ratio,
        max_auction_vol_ratio_5d=args.max_ratio,
        st_filter=args.st_filter,
        max_hold_days=args.max_hold_days,
        seal_hold_enabled=seal_hold_enabled,
        seal_hold_max_days=args.seal_hold_days if seal_hold_enabled else 3,
        seal_hold_max_open_times=args.seal_hold_max_open_times,
        factor_score_threshold=args.factor_score_threshold,
    )
    required_tables = [
        "auction_bar",
        "daily_bar",
        "daily_state",
        "minute_bar",
        "stock_status_daily",
    ]
    if seal_hold_enabled:
        required_tables.append("limit_list_daily")
    if args.persist_positions:
        # persist 要写 paper_position/快照表 → 必须写模式直连主库。
        # 盘中 monitor 持写锁会直接撞锁，明确警告后仍执行（撞锁自然报错）
        from datetime import datetime as _dt
        now = _dt.now().time()
        if dtime(9, 25) <= now <= dtime(15, 5):
            logger.warning(
                "盘中时段 persist 落库会与本地 monitor 抢写锁，建议收盘后执行"
            )
        with DuckDBStore() as store:
            candidates = run_auction_gap_replay(store, config.auction_config())
            trades = run_auction_gap_minute_replay(
                store, config,
                persist_positions=True,
                run_id=args.run_id,
            )
    else:
        with open_readonly_store(required_tables=required_tables) as store:
            candidates = run_auction_gap_replay(store, config.auction_config())
            trades = run_auction_gap_minute_replay(store, config)

    summary = summarize_auction_gap_minute_replay(
        trades,
        candidates_count=len(candidates),
    )
    logger.info(summary.model_dump())

    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(output, index=False)
        logger.info(f"auction gap minute replay 结果已写出: {output}")

    if trades.empty:
        logger.warning("auction gap minute replay 无成交")
        return 0

    preview_cols = [
        "signal_date", "ts_code", "name", "auction_price",
        "entry_time", "entry_price", "b_first_limit_up_time",
        "b_close_at_limit_up", "hold_policy", "exit_time", "exit_price",
        "exit_reason", "ret_pct",
    ]
    available_cols = [col for col in preview_cols if col in trades.columns]
    logger.info("\n" + trades[available_cols].tail(20).to_string(index=False))
    return 0


def cmd_auction_gap_minute_backfill(args: argparse.Namespace) -> int:
    """按集合竞价跳空候选回补分钟 replay 窗口。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.intraday_backfill import backfill_auction_gap_minute_replay_window

    setup_logging()
    with DuckDBStore() as store:
        summary = backfill_auction_gap_minute_replay_window(
            store,
            TushareAdapter(),
            start_date=args.start_date,
            end_date=args.end_date,
            max_hold_days=args.max_hold_days,
            freq=args.freq,
            gap_mode=args.gap_mode,
            st_filter=args.st_filter,
            min_ratio=args.min_ratio,
            max_ratio=args.max_ratio,
            ts_code=args.ts_code,
            lookback_days=args.lookback_days,
            dry_run=args.dry_run,
        )
    logger.info(summary.model_dump())
    return 1 if summary.failed_requests else 0


def cmd_minute_replay(args: argparse.Namespace) -> int:
    """基于已入库历史分钟线跑强承接/突破模拟回放。"""
    from rquant.minute_replay import run_minute_strong_carry_replay
    from rquant.storage.duckdb import DuckDBStore
    from rquant.volume_profile import VolumeProfileRuleConfig

    setup_logging()
    volume_profile_config = VolumeProfileRuleConfig(
        enabled=args.volume_profile,
        lookback_days=tuple(args.volume_profile_lookbacks),
    )
    with DuckDBStore() as store:
        trades = run_minute_strong_carry_replay(
            store,
            start_date=args.start_date,
            end_date=args.end_date,
            preset_name=args.preset,
            freq=args.freq,
            entry_mode=args.entry_mode,
            max_hold_days=args.max_hold_days,
            volume_profile_config=volume_profile_config,
            factor_score_threshold=args.factor_score_threshold,
        )

    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(output, index=False)
        logger.info(f"minute replay 结果已写出: {output}")

    if trades.empty:
        logger.warning("minute replay 无成交记录")
        return 0

    win_rate = (trades["ret_pct"] > 0).mean() * 100
    logger.info(
        "minute replay: "
        f"n={len(trades)}, "
        f"mean={trades['ret_pct'].mean():.2f}%, "
        f"median={trades['ret_pct'].median():.2f}%, "
        f"win={win_rate:.1f}%"
    )
    preview_cols = [
        "signal_date", "ts_code", "name", "entry_time", "entry_price_raw",
        "entry_price", "exit_time", "exit_price", "exit_reason",
        "holding_trading_days", "ret_pct",
    ]
    available_cols = [col for col in preview_cols if col in trades.columns]
    logger.info("\n" + trades[available_cols].tail(20).to_string(index=False))
    return 0


def cmd_growth_board_surge_replay(args: argparse.Namespace) -> int:
    """回测科创/创业板盘中放量追击策略。"""
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    setup_logging()
    config = GrowthBoardSurgeConfig(
        freq=args.freq,
        min_signal_time=_parse_hhmm(args.min_signal_time),
        lookback_days=args.lookback_days,
        min_hist_days=args.min_hist_days,
        min_cum_amount_ratio=args.min_cum_amount_ratio,
        min_same_minute_amount_ratio=args.min_same_minute_amount_ratio,
        require_inner_outer=args.require_inner_outer,
        min_inner_outer_ratio=args.min_inner_outer_ratio,
        require_large_net_vol=args.require_large_net_vol,
        min_large_net_vol=args.min_large_net_vol,
        require_fresh_surge=args.require_fresh_surge,
        fresh_lookback_days=args.fresh_lookback_days,
        min_listing_trading_days=args.min_listing_trading_days,
        require_board_favor=args.require_board_favor,
        min_board_gap_up_ratio=args.min_board_gap_up_ratio,
        min_board_auction_amount_ratio=args.min_board_auction_amount_ratio,
        board_hist_days=args.board_hist_days,
        enable_factor_confirm=args.factor_confirm,
        factor_score_threshold=args.factor_score_threshold,
        max_hold_days=args.max_hold_days,
    )
    required_tables = [
        "daily_bar",
        "daily_indicator",
        "daily_state",
        "minute_bar",
        "stock_status_daily",
    ]
    if config.factor_layer_enabled:
        required_tables += ["moneyflow_daily", "market_sentiment_daily"]
    if config.require_board_favor:
        required_tables += ["auction_bar", "kpl_concept_member_daily"]
    with open_readonly_store(required_tables=required_tables) as store:
        trades = run_growth_board_surge_replay(
            store,
            start_date=args.start_date,
            end_date=args.end_date,
            config=config,
        )

    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(output, index=False)
        logger.info(f"growth board surge replay 结果已写出: {output}")

    if trades.empty:
        logger.warning("growth board surge replay 无成交记录")
        return 0

    win_rate = (trades["ret_pct"] > 0).mean() * 100
    logger.info(
        "growth board surge replay: "
        f"n={len(trades)}, "
        f"mean={trades['ret_pct'].mean():.2f}%, "
        f"median={trades['ret_pct'].median():.2f}%, "
        f"win={win_rate:.1f}%"
    )
    preview_cols = [
        "signal_date", "ts_code", "name", "board_type",
        "entry_time", "entry_price", "limit_up_price",
        "hit_limit_up_today", "exit_time", "exit_price",
        "exit_reason", "ret_pct",
    ]
    available_cols = [col for col in preview_cols if col in trades.columns]
    logger.info("\n" + trades[available_cols].tail(20).to_string(index=False))
    return 0


def cmd_notify_test(args: argparse.Namespace) -> int:
    """推测试消息验证所有配置的通道（PushDeer + PushPlus）。"""
    from datetime import datetime

    from rquant.config import settings
    from rquant.notify.client import PushDeerClient, PushPlusClient

    setup_logging()
    keys = settings.pushdeer_key_list
    tokens = settings.pushplus_token_list
    if not keys and not tokens:
        logger.error("PUSHDEER_KEYS 和 PUSHPLUS_TOKENS 都未配置，请检查 .env")
        return 1

    title = "✅ rQuant 通道测试"
    body = (
        f"时间：{datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"PushDeer keys: {len(keys)} 个 / PushPlus tokens: {len(tokens)} 个\n"
        f"这是测试消息，忽略即可"
    )

    total_success = 0
    total_count = 0

    if keys:
        pd_results = PushDeerClient(keys, settings.pushdeer_endpoint).push(title, body)
        for i, (s, err) in enumerate(pd_results):
            label = "PushDeer " + keys[i][:8] + "…"
            (logger.info if s else logger.error)(
                f"  {'✅' if s else '❌'} {label}{'' if s else ': ' + str(err)}"
            )
        total_success += sum(1 for s, _ in pd_results if s)
        total_count += len(pd_results)

    if tokens:
        pp_results = PushPlusClient(tokens, settings.pushplus_endpoint).push(title, body)
        for i, (s, err) in enumerate(pp_results):
            label = "PushPlus " + tokens[i][:8] + "…"
            (logger.info if s else logger.error)(
                f"  {'✅' if s else '❌'} {label}{'' if s else ': ' + str(err)}"
            )
        total_success += sum(1 for s, _ in pp_results if s)
        total_count += len(pp_results)

    logger.info(f"测试发送完成: {total_success}/{total_count} 成功")
    return 0 if total_success > 0 else 1


def cmd_daily_report(args: argparse.Namespace) -> int:
    """生成 + 推送当日健康摘要（systemd timer 15:30 自动跑）。"""
    from rquant.health import generate_and_send_daily_report

    setup_logging()
    n = generate_and_send_daily_report(dry_run=args.dry_run)
    return 0 if (args.dry_run or n > 0) else 1


def cmd_morning_pulse(args: argparse.Namespace) -> int:
    """盘中 30 分钟脉搏（launchd 10:00/10:30/11:00/11:30 自动跑）。"""
    from rquant.midday_briefing import run_morning_pulse

    setup_logging()
    return run_morning_pulse(slot=args.slot, force=args.force, dry_run=args.dry_run)


def cmd_midday_report(args: argparse.Namespace) -> int:
    """午间战报（launchd 12:00 自动跑）。"""
    from rquant.midday_briefing import run_midday_report

    setup_logging()
    return run_midday_report(report_date=args.date, force=args.force, dry_run=args.dry_run)


def cmd_surge_watch(args: argparse.Namespace) -> int:
    """每分钟爆量推送（云端 systemd timer 09:25 拉起，15:02 自然退出）。

    --simulate DIR：离线回放目录内快照 parquet 序列（可测性设施）；
    --dry-run：全流程跑但不推送（打印报文）；--force-session：忽略时段守卫（盘后验收）。
    """
    from pathlib import Path as _Path

    from rquant.surge_watch import SurgeConfig, run_simulate, run_surge_watch

    setup_logging()
    config = SurgeConfig(
        k_cum=args.k_cum,
        ratio_cap=args.ratio_cap,
        skip_first_minutes=args.skip_first_minutes,
        k_delta_confirm=args.k_delta,
        require_vwap=args.require_vwap,
        max_room_to_limit_pct=args.max_room,
    )
    if args.simulate:
        return run_simulate(_Path(args.simulate), dry_run=args.dry_run, config=config)
    return run_surge_watch(
        dry_run=args.dry_run,
        force_session=args.force_session,
        config=config,
        max_ticks=args.max_ticks,
    )


def cmd_alert(args: argparse.Namespace) -> int:
    """发一条运维告警（用于 systemd OnFailure / watchdog 等场景）。

    刻意做成最小接口：subject 必填，body 可选。直接走 PushDeer + PushPlus，
    不依赖 notify scene 体系，避免被新增字段牵连。
    """
    from rquant.config import settings
    from rquant.notify.client import PushDeerClient, PushPlusClient
    from rquant.notify.gate import NotificationGate

    setup_logging()
    keys = settings.pushdeer_key_list
    tokens = settings.pushplus_token_list
    if not keys and not tokens:
        logger.error("PUSHDEER_KEYS 和 PUSHPLUS_TOKENS 都未配置，alert 无处可发")
        return 1

    gate = None
    lease = None
    cooldown_seconds = (
        settings.notify_ops_cooldown_seconds
        if args.cooldown_seconds is None
        else args.cooldown_seconds
    )
    if not args.force and cooldown_seconds > 0:
        try:
            gate = NotificationGate(
                settings.notification_state_path_resolved,
                busy_timeout_ms=settings.notification_state_busy_timeout_ms,
            )
            event_key = args.dedup_key or f"ops:{args.subject.strip().lower()}"
            lease = gate.claim(event_key, cooldown_seconds)
        except Exception as e:
            logger.error(f"alert 去重状态完全不可用，已抑制 Push: {e}")
            return 1
        else:
            if lease is None:
                logger.warning(f"alert 同类事件仍在冷却期，已抑制: {args.subject!r}")
                return 0

    body = args.body or ""
    success = 0
    total = 0
    if keys:
        for s, err in PushDeerClient(keys, settings.pushdeer_endpoint).push(args.subject, body):
            total += 1
            success += int(s)
            if not s:
                logger.error(f"PushDeer 失败: {err}")
    if tokens:
        for s, err in PushPlusClient(tokens, settings.pushplus_endpoint).push(args.subject, body):
            total += 1
            success += int(s)
            if not s:
                logger.error(f"PushPlus 失败: {err}")

    logger.info(f"alert 发送: {success}/{total} 成功 (subject={args.subject!r})")
    if gate is not None and lease is not None:
        try:
            if success > 0:
                gate.complete(lease, cooldown_seconds)
            else:
                gate.release(lease)
        except Exception as e:
            logger.error(f"alert 更新投递去重状态失败: {e}")
    return 0 if success > 0 else 1


def cmd_alert_resolve(args: argparse.Namespace) -> int:
    """关闭已恢复的运维事故，让之后的新故障可以再次告警。"""
    from rquant.config import settings
    from rquant.notify.gate import NotificationGate

    setup_logging()
    try:
        gate = NotificationGate(
            settings.notification_state_path_resolved,
            busy_timeout_ms=settings.notification_state_busy_timeout_ms,
        )
        gate.clear(args.dedup_key)
    except Exception as e:
        logger.error(f"alert 事故关闭失败 ({args.dedup_key!r}): {e}")
        return 1
    logger.info(f"alert 事故已关闭: {args.dedup_key!r}")
    return 0


def cmd_preflight(args: argparse.Namespace) -> int:
    """全家服务深度体检（手动触发，dry-run，不重启服务）。

    跑 5 项深度检查（unit verify / 服务详情 / DuckDB 锁布局 / 数据新鲜度 /
    screen smoke），打印 markdown 报告到 stdout。--notify 推 PushDeer 摘要。

    任何 fail → 退出码 1。
    """
    from rquant.config import settings
    from rquant.notify.client import PushDeerClient, PushPlusClient
    from rquant.preflight import format_pushdeer_summary, format_report, run_all_checks

    setup_logging()
    results = run_all_checks(freshness_profile=args.profile)
    print(format_report(results))

    if args.notify:
        subject, body = format_pushdeer_summary(results)
        keys = settings.pushdeer_key_list
        tokens = settings.pushplus_token_list
        if not keys and not tokens:
            logger.warning("PUSHDEER_KEYS / PUSHPLUS_TOKENS 都未配置，跳过推送")
        else:
            if keys:
                for s, err in PushDeerClient(keys, settings.pushdeer_endpoint).push(subject, body):
                    if not s:
                        logger.error(f"PushDeer 失败: {err}")
            if tokens:
                client = PushPlusClient(tokens, settings.pushplus_endpoint)
                for s, err in client.push(subject, body):
                    if not s:
                        logger.error(f"PushPlus 失败: {err}")

    fails = [r for r in results if r.status == "fail"]
    return 1 if fails else 0


def cmd_pre_market_check(args: argparse.Namespace) -> int:
    """开盘前主动体检（systemd timer Mon..Fri 09:00 触发）。

    跑 pre_market_check.run_all_checks() → format_summary → PushDeer 推一条摘要。
    任何 fail 项视为命令失败，触发 systemd OnFailure 兜底告警；warn 不触发 OnFailure
    （已经在 PushDeer 里说明）。
    """
    from rquant.config import settings
    from rquant.notify.client import PushDeerClient, PushPlusClient
    from rquant.pre_market_check import format_summary, run_all_checks

    setup_logging()
    results = run_all_checks()
    subject, body = format_summary(results)

    print(subject)
    print(body)

    if not args.dry_run:
        keys = settings.pushdeer_key_list
        tokens = settings.pushplus_token_list
        if not keys and not tokens:
            logger.warning("PUSHDEER_KEYS / PUSHPLUS_TOKENS 都未配置，跳过推送")
        else:
            if keys:
                for s, err in PushDeerClient(keys, settings.pushdeer_endpoint).push(subject, body):
                    if not s:
                        logger.error(f"PushDeer 失败: {err}")
            if tokens:
                client = PushPlusClient(tokens, settings.pushplus_endpoint)
                for s, err in client.push(subject, body):
                    if not s:
                        logger.error(f"PushPlus 失败: {err}")

    fails = [r for r in results if r.status == "fail"]
    return 1 if fails else 0


def cmd_pool2(args: argparse.Namespace) -> int:
    """管理 Pool 2 持久池。"""
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()
    with DuckDBStore() as store:
        if args.pool2_action == "list":
            df = store.query_pool2_all()
            if df.empty:
                logger.info("Pool 2 持久池为空")
                return 0
            for _, row in df.iterrows():
                status_mark = "🟢" if row["status"] == "active" else "⬜"
                logger.info(
                    f"  {status_mark} {row['ts_code']} "
                    f"入池 {row['entry_date']} "
                    f"涨停 {row['limit_up_date']} "
                    f"body ¥{row['body_lower']:.2f}-¥{row['body_upper']:.2f} "
                    f"[{row['status']}]"
                )
            return 0

        elif args.pool2_action == "remove":
            store.remove_pool2(args.ts_code)
            logger.info(f"已从 Pool 2 移除: {args.ts_code}")
            return 0

    return 0


def cmd_blacklist(args: argparse.Namespace) -> int:
    """管理风险黑名单。"""
    from datetime import date
    from pathlib import Path

    from rquant.risk.blacklist import (
        export_blacklist_parquet,
        import_blacklist,
        load_active_blacklist,
        load_blacklist_parquet,
        parse_blacklist_pdf,
    )
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()

    if args.blacklist_action == "import":
        pdf = Path(args.pdf).expanduser()
        if not pdf.exists():
            logger.error(f"PDF 不存在: {pdf}")
            return 1
        entries = parse_blacklist_pdf(pdf)
        with DuckDBStore() as store:
            n = import_blacklist(
                entries,
                list_label=args.label,
                source_file=pdf.name,
                store=store,
                validity_days=args.validity,
            )
        logger.info(f"导入完成：{n} 只 → '{args.label}'")
        return 0

    elif args.blacklist_action == "load-parquet":
        parquet = Path(args.parquet).expanduser().resolve()
        if not parquet.exists():
            logger.error(f"parquet 不存在: {parquet}")
            return 1
        with DuckDBStore() as store:
            n = load_blacklist_parquet(
                parquet, store, list_label=args.label,
            )
        logger.info(
            f"parquet 落库完成：{n} 行 → "
            f"{'list_label=' + args.label if args.label else '全表覆盖'}"
        )
        return 0

    elif args.blacklist_action == "export-parquet":
        out = Path(args.output).expanduser().resolve()
        with DuckDBStore() as store:
            n = export_blacklist_parquet(
                store, out, list_label=args.label,
            )
        logger.info(f"parquet 导出完成：{n} 行 → {out}")
        return 0

    elif args.blacklist_action == "list":
        with DuckDBStore() as store:
            today = date.today()
            blacklist = load_active_blacklist(
                store,
                list_label=args.label,
                include_expired=args.include_expired,
            )
            if not blacklist:
                logger.info("无活动黑名单")
                return 0
            for code, hit in sorted(blacklist.items()):
                expired_mark = " [已过期]" if hit.is_expired else ""
                days_left = (hit.expires_at - today).days
                logger.info(
                    f"  {code} {hit.name:8s} {hit.list_label} "
                    f"(剩 {days_left}d, {len(hit.sub_categories)} 类){expired_mark}"
                )
            logger.info(f"共 {len(blacklist)} 只")
        return 0

    elif args.blacklist_action == "check":
        with DuckDBStore() as store:
            blacklist = load_active_blacklist(store, include_expired=True)
        hit = blacklist.get(args.ts_code)
        if hit is None:
            logger.info(f"{args.ts_code} 不在任何黑名单中")
            return 0
        expired_mark = " [已过期]" if hit.is_expired else ""
        logger.info(
            f"{args.ts_code} {hit.name} 在 '{hit.list_label}'{expired_mark}\n"
            f"  类别: {hit.sub_categories}\n"
            f"  导入: {hit.imported_at} → 失效: {hit.expires_at}"
        )
        return 0

    elif args.blacklist_action == "remove":
        with DuckDBStore() as store:
            n = store._conn.execute(
                "DELETE FROM risk_blacklist WHERE list_label = ?", [args.label]
            ).fetchone()
        logger.info(f"已删除 list_label='{args.label}' 的所有条目")
        return 0

    return 0


def cmd_lab_run(args: argparse.Namespace) -> int:
    """执行 Strategy Lab 后台任务 spec（由 launch_background_run 派生的子进程调用）。

    execute_spec 内部已把 error 写进 status 文件，这里只负责 exit code；
    spec 本身缺失/损坏时 execute_spec 没机会写状态，按文件名约定
    <run_id>.spec.json 提取 run_id 补写 error status，UI 不至于永远显示"运行中"。
    """
    import json

    from rquant.dashboard.strategy_lab_worker import execute_spec

    setup_logging()
    spec_path = Path(args.spec).expanduser().resolve()
    try:
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.exception(f"lab-run spec 读取/解析失败: {spec_path}")
        try:
            from datetime import datetime

            from rquant.dashboard.strategy_lab_worker import (
                CST,
                LabRunStatus,
                write_run_status,
            )

            run_id = spec_path.name.removesuffix(".spec.json")
            if run_id == spec_path.name:
                run_id = spec_path.stem
            # spec 与 status 同目录（strategy_lab_runs/），从 spec 路径反推 base_dir
            base_dir = (
                spec_path.parent.parent
                if spec_path.parent.name == "strategy_lab_runs"
                else None
            )
            write_run_status(
                LabRunStatus(
                    run_id=run_id,
                    run_type="unknown",
                    state="error",
                    finished_at=datetime.now(CST),
                    error=f"spec 读取/解析失败: {type(e).__name__}: {e}",
                ),
                base_dir=base_dir,
            )
        except Exception:
            logger.exception("lab-run 补写 error status 失败")
        return 1
    try:
        run_id = execute_spec(spec)
    except Exception:
        logger.exception("lab-run 执行失败（error 已写入 status 文件）")
        return 1
    logger.info(f"lab-run 完成: {run_id}")
    return 0


def cmd_panorama_auth_serve(args: argparse.Namespace) -> int:
    """启动全景页登录网关服务（标准库 http.server，systemd 拉起）。"""
    from rquant.config import settings
    from rquant.panorama_auth import serve_auth

    setup_logging()
    # map 方案：登录成功下发固定网关令牌（显式配置优先，否则由 cookie_secret 派生）。
    return serve_auth(
        settings.panorama_gate_token_resolved,
        settings.panorama_users_path_resolved,
        host=args.host,
        port=args.port,
    )


def cmd_panorama_gate_token(args: argparse.Namespace) -> int:
    """打印当前生效的 map 网关令牌（部署时写进 nginx map 文件，只读打印无副作用）。

    仅把令牌打到 stdout，供 `echo "\"$(rquant panorama-gate-token)\" 1;" | tee map` 取用；
    未配置（GATE_TOKEN 与 COOKIE_SECRET 均空）时向 stderr 报错并返回 1，避免写入空令牌。
    """
    from rquant.config import settings

    token = settings.panorama_gate_token_resolved
    if not token:
        print(
            "RQUANT_PANORAMA_GATE_TOKEN / RQUANT_PANORAMA_COOKIE_SECRET 均未配置，"
            "无法生成网关令牌",
            file=sys.stderr,
        )
        return 1
    print(token)
    return 0


def cmd_panorama_user_add(args: argparse.Namespace) -> int:
    """交互式添加/更新全景页登录用户（getpass 输密码两次确认，不回显）。"""
    import getpass

    from rquant.config import settings
    from rquant.panorama_auth import UserStore

    setup_logging()
    pw1 = getpass.getpass(f"为 {args.name} 设置密码: ")
    pw2 = getpass.getpass("再次输入确认: ")
    if pw1 != pw2:
        logger.error("两次输入不一致，未修改用户库")
        return 1
    if not pw1:
        logger.error("密码不能为空，未修改用户库")
        return 1

    store = UserStore(settings.panorama_users_path_resolved)
    existed = args.name in store.list_users()
    try:
        store.add(args.name, pw1)
    except ValueError as e:
        logger.error(f"用户名不合法: {e}")
        return 1
    logger.info(f"✅ 已{'更新' if existed else '添加'} {args.name}（用户库 {store.path}）")
    return 0


def cmd_panorama_user_remove(args: argparse.Namespace) -> int:
    """移除全景页登录用户（不存在为 no-op）。"""
    from rquant.config import settings
    from rquant.panorama_auth import UserStore

    setup_logging()
    store = UserStore(settings.panorama_users_path_resolved)
    if store.remove(args.name):
        logger.info(f"已移除 {args.name}（用户库 {store.path}）")
    else:
        logger.warning(f"{args.name} 不在用户库，无需移除")
    return 0


def cmd_panorama_user_list(args: argparse.Namespace) -> int:
    """列出全景页登录用户名（不含哈希）。"""
    from rquant.config import settings
    from rquant.panorama_auth import UserStore

    setup_logging()
    store = UserStore(settings.panorama_users_path_resolved)
    users = store.list_users()
    if not users:
        logger.info(f"用户库为空（{store.path}）")
        return 0
    for name in users:
        logger.info(f"  {name}")
    logger.info(f"共 {len(users)} 个用户")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """构建 CLI 参数解析器。"""
    parser = _RQuantArgumentParser(
        prog="rquant", description="rQuant 量化选股平台"
    )
    sub = parser.add_subparsers(dest="command")

    serve_p = sub.add_parser("serve", help="启动 APScheduler 常驻进程")
    serve_p.add_argument(
        "--hour", type=int, default=17, help="每日触发小时 (默认 17)"
    )

    run_p = sub.add_parser("run-daily", help="拉取数据 + 执行全流水线")
    run_p.add_argument(
        "--date", type=str, default=None,
        help="交易日期 YYYY-MM-DD (默认今天)",
    )
    run_p.add_argument(
        "--preset", type=str, default=None,
        help="只跑指定预设 (默认全部)",
    )
    run_p.add_argument(
        "--no-ingest", action="store_true",
        help="跳过数据拉取，只跑筛选",
    )
    run_p.add_argument(
        "--skip-minute-backfill", action="store_true",
        help="跳过日终 Pool1 90 日分钟上下文回补",
    )
    run_p.add_argument(
        "--minute-lookback-days", type=int, default=90,
        help="日终分钟上下文回补交易日数量 (默认 90)",
    )

    ingest_p = sub.add_parser("ingest", help="仅拉取数据（不跑筛选）")
    ingest_p.add_argument(
        "--date", type=str, default=None,
        help="交易日期 YYYY-MM-DD (默认今天)",
    )

    indicator_backfill_p = sub.add_parser(
        "daily-indicator-backfill",
        help="从本地日线与复权因子预演或重建 daily_indicator",
    )
    indicator_backfill_p.add_argument(
        "--start-date",
        type=_parse_iso_date,
        required=True,
        help="开始日期 YYYY-MM-DD（含）",
    )
    indicator_backfill_p.add_argument(
        "--end-date",
        type=_parse_iso_date,
        required=True,
        help="结束日期 YYYY-MM-DD（含）",
    )
    indicator_backfill_p.add_argument(
        "--apply",
        action="store_true",
        help="显式执行写入；默认只预演",
    )

    monitor_p = sub.add_parser("monitor", help="启动盘中实时监控")
    monitor_p.add_argument(
        "--interval", type=int, default=5,
        help="轮询间隔秒数 (默认 5)",
    )

    rt_min_p = sub.add_parser(
        "rt-minute-fetch",
        help="拉取 Tushare 实时分钟最新 K 线并写入 minute_bar",
    )
    rt_min_p.add_argument(
        "--ts-code",
        action="append",
        required=True,
        help="股票代码，支持逗号分隔或重复传参",
    )
    rt_min_p.add_argument(
        "--freq",
        type=str,
        default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )

    rt_min_daily_p = sub.add_parser(
        "rt-minute-daily-fetch",
        help="拉取 Tushare 当日累计实时分钟 K 线并写入 minute_bar",
    )
    rt_min_daily_p.add_argument(
        "--ts-code",
        action="append",
        required=True,
        help="股票代码，支持逗号分隔或重复传参",
    )
    rt_min_daily_p.add_argument(
        "--freq",
        type=str,
        default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )

    rs_p = sub.add_parser(
        "research-sync",
        help="云端备份(cloud_backup.duckdb)合并进本地研究库，或从旧库恢复研究表",
    )
    rs_p.add_argument(
        "--backup", type=str, default=None,
        help="云端备份文件路径（默认 data/cloud_backup.duckdb）",
    )
    rs_p.add_argument(
        "--restore-from", type=str, default=None,
        help="恢复模式：从指定旧库/旧副本按主键合并研究表",
    )
    rs_p.add_argument(
        "--tables", type=str, default=None,
        help="恢复模式下只处理这些表（逗号分隔，默认全部研究表）",
    )
    rs_p.add_argument(
        "--no-refresh-replica", action="store_true",
        help="跳过只读副本刷新",
    )

    research_export_p = sub.add_parser(
        "research-export",
        help="从只读副本导出校验过的分钟/竞价研究湖分区",
    )
    research_export_p.add_argument(
        "--dataset",
        required=True,
        choices=["minute_bar", "auction_bar"],
        help="研究数据集",
    )
    research_export_p.add_argument(
        "--start-date",
        type=_parse_iso_date,
        required=True,
        help="开始日期 YYYY-MM-DD（含）",
    )
    research_export_p.add_argument(
        "--end-date",
        type=_parse_iso_date,
        required=True,
        help="结束日期 YYYY-MM-DD（含）",
    )
    research_export_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只报告分区和行数，不创建目录、Parquet 或 catalog",
    )

    research_ingest_p = sub.add_parser(
        "research-ingest",
        help="日终补齐并封存云端分钟/竞价研究分区",
    )
    research_ingest_p.add_argument(
        "--date",
        type=_parse_iso_date,
        default=None,
        help="目标交易日 YYYY-MM-DD（默认今天）",
    )
    research_ingest_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只检查本地副本已有数据，不请求接口或发布研究目录",
    )
    research_ingest_p.add_argument(
        "--recover",
        action="store_true",
        help="按交易日顺序使用 stk_mins 恢复遗漏的历史分区（必须同时传 --date）",
    )
    research_ingest_p.add_argument(
        "--scheduled",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    research_readiness_p = sub.add_parser(
        "research-ingest-readiness",
        help="检查日线副本是否已具备研究日增量所需的当日完整数据",
    )
    research_readiness_p.add_argument(
        "--date",
        type=_parse_iso_date,
        default=None,
        help="目标交易日 YYYY-MM-DD（默认上海时区今天）",
    )

    research_status_p = sub.add_parser(
        "research-authority-status",
        help="只读核验研究候选、主副目录哈希和连续观察天数",
    )
    research_status_p.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="研究数据根目录（默认 DATA_DIR）",
    )

    migration_p = sub.add_parser(
        "research-migration",
        help="创建恢复快照、迁移包并在云端校验发布研究数据",
    )
    migration_sub = migration_p.add_subparsers(
        dest="migration_command",
        required=True,
    )
    migration_snapshot_p = migration_sub.add_parser(
        "snapshot",
        help="checkpoint 后创建只读、不可变恢复快照",
    )
    migration_snapshot_p.add_argument(
        "--source-database",
        type=Path,
        required=True,
        help="本地研究主库绝对路径",
    )
    migration_snapshot_p.add_argument(
        "--recovery-dir",
        type=Path,
        required=True,
        help="恢复快照父目录",
    )
    migration_snapshot_p.add_argument(
        "--artifact-dir",
        type=Path,
        required=True,
        help="需要与 DuckDB 快照绑定的 Strategy Lab artifact 目录",
    )
    migration_snapshot_p.add_argument("--snapshot-id", required=True)
    migration_snapshot_p.add_argument("--code-commit", required=True)

    migration_prepare_p = migration_sub.add_parser(
        "prepare",
        help="从不可变恢复快照生成自校验迁移包",
    )
    migration_prepare_p.add_argument("--source-snapshot", type=Path, required=True)
    migration_prepare_p.add_argument("--bundle-dir", type=Path, required=True)
    migration_prepare_p.add_argument("--artifact-dir", type=Path, required=True)
    migration_prepare_p.add_argument("--snapshot-id", required=True)
    migration_prepare_p.add_argument("--code-commit", required=True)
    migration_prepare_p.add_argument(
        "--start-date",
        type=_parse_iso_date,
        required=True,
    )
    migration_prepare_p.add_argument(
        "--end-date",
        type=_parse_iso_date,
        required=True,
    )

    migration_verify_p = migration_sub.add_parser(
        "verify",
        help="重新计算迁移包文件与数据语义证据",
    )
    migration_verify_p.add_argument("--bundle-path", type=Path, required=True)

    migration_publish_p = migration_sub.add_parser(
        "publish",
        help="逐分区发布到研究目录并最后写候选权威标记",
    )
    migration_publish_p.add_argument("--bundle-path", type=Path, required=True)
    migration_publish_p.add_argument("--target-data-dir", type=Path, required=True)
    migration_publish_p.add_argument(
        "--apply",
        action="store_true",
        required=True,
        help="确认执行云端研究目录写入",
    )

    calendar_p = sub.add_parser(
        "trade-calendar-bootstrap",
        help="从 Tushare 初始化权威 SSE 交易日历",
    )
    calendar_p.add_argument(
        "--start-date",
        type=_parse_iso_date,
        default=date(2020, 1, 1),
        help="开始日期 YYYY-MM-DD (默认 2020-01-01)",
    )
    calendar_p.add_argument(
        "--end-date",
        type=_parse_iso_date,
        default=date(date.today().year, 12, 31),
        help="结束日期 YYYY-MM-DD (默认运行当年 12-31)",
    )

    moneyflow_p = sub.add_parser(
        "moneyflow-backfill",
        help="拉取 Tushare 日级个股资金流并写入 moneyflow_daily",
    )
    moneyflow_p.add_argument(
        "--date",
        type=str,
        required=True,
        help="交易日期 YYYY-MM-DD",
    )

    market_backfill_p = sub.add_parser(
        "market-daily-backfill",
        help="全市场日线历史回补，并在最后统一重算 daily_state",
    )
    market_backfill_p.add_argument(
        "--start-date", type=str, required=True,
        help="开始日期 YYYY-MM-DD",
    )
    market_backfill_p.add_argument(
        "--end-date", type=str, required=True,
        help="结束日期 YYYY-MM-DD",
    )
    market_backfill_p.add_argument(
        "--dry-run", action="store_true",
        help="只报告交易日数与预计请求数，不调 Tushare、不写库",
    )
    market_backfill_p.add_argument(
        "--skip-state-recompute",
        dest="skip_state_recompute",
        action="store_true",
        help="跳过最终 daily_state 重算；逐日写入仍会删除受影响日期起的陈旧状态尾部",
    )
    market_backfill_p.add_argument(
        "--skip-state",
        dest="skip_state_recompute",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    zt_pool_p = sub.add_parser(
        "zt-pool-capture",
        help="采集当日东财涨停池到 limit_up_pool_daily（源只有当天数据，需每日采集）",
    )
    zt_pool_p.add_argument(
        "--date", type=str, default=None,
        help="交易日期 YYYY-MM-DD (默认今天)",
    )

    zt_repair_p = sub.add_parser(
        "zt-pool-repair",
        help="生成休市日污染修复计划；显式确认 plan id 后原子执行",
    )
    zt_repair_p.add_argument(
        "--apply",
        action="store_true",
        help="执行已确认的修复计划（必须同时传 --plan-id）",
    )
    zt_repair_p.add_argument(
        "--plan-id",
        type=_parse_sha256,
        default=None,
        help="dry-run 输出的 64 位 plan id（必须同时传 --apply）",
    )

    limit_list_p = sub.add_parser(
        "limit-list-backfill",
        help="Tushare 涨跌停/炸板榜（limit_list_d）回补到 limit_list_daily"
             "（2020 起，U/D/Z 一次拿齐，不含 ST）",
    )
    limit_list_p.add_argument(
        "--start-date", type=str, default=None,
        help="开始日期 YYYY-MM-DD",
    )
    limit_list_p.add_argument(
        "--end-date", type=str, default=None,
        help="结束日期 YYYY-MM-DD",
    )
    limit_list_p.add_argument(
        "--dry-run", action="store_true",
        help="只报告交易日数与预计请求数，不调 Tushare、不写库",
    )
    limit_list_p.add_argument(
        "--today", action="store_true",
        help="只拉当天（日终增量），失败不炸、幂等可重跑",
    )

    data_backfill_p = sub.add_parser(
        "data-backfill",
        help="统一数据集回补（板块行情/成分/资金流/龙虎榜/开盘啦等，"
             "注册表见 rquant.dataset_backfill.DATASETS）",
    )
    data_backfill_p.add_argument(
        "--dataset", type=str, required=True,
        help="数据集名（Tushare 接口名，如 ths_daily / moneyflow_dc / "
             "top_list），all 跑全部",
    )
    data_backfill_p.add_argument(
        "--start-date", type=str, default=None,
        help="开始日期 YYYY-MM-DD（snapshot 数据集忽略）",
    )
    data_backfill_p.add_argument(
        "--end-date", type=str, default=None,
        help="结束日期 YYYY-MM-DD（snapshot 数据集取该日往前最近交易日）",
    )
    data_backfill_p.add_argument(
        "--dry-run", action="store_true",
        help="只报告交易日数与预计请求数，不调 Tushare、不写库",
    )
    data_backfill_p.add_argument(
        "--today", action="store_true",
        help="日终增量：start=end=今天（snapshot 数据集即刷新快照）",
    )

    strategy_choices = ["auction_gap", "growth_board_surge", "n_shape"]
    backfill_plan_p = sub.add_parser(
        "backfill-plan",
        help="按策略和 PIT 候选生成可恢复的历史分钟回补计划",
    )
    backfill_plan_p.add_argument(
        "--strategy",
        required=True,
        choices=strategy_choices,
        help="策略标识",
    )
    backfill_plan_p.add_argument(
        "--start-date",
        required=True,
        type=_parse_iso_date,
        help="候选开始日期 YYYY-MM-DD",
    )
    backfill_plan_p.add_argument(
        "--end-date",
        required=True,
        type=_parse_iso_date,
        help="候选结束日期 YYYY-MM-DD",
    )

    backfill_run_p = sub.add_parser(
        "backfill-run",
        help="在安全写入窗口执行已持久化的分钟回补计划",
    )
    backfill_run_p.add_argument(
        "--manifest-id",
        required=True,
        type=_parse_sha256,
        help="backfill-plan 输出的 64 位 manifest id",
    )
    backfill_run_p.add_argument(
        "--retry-failed",
        action="store_true",
        help="重试尚未耗尽次数的可重试失败任务",
    )

    backfill_status_p = sub.add_parser(
        "backfill-status",
        help="仅从独立 SQLite 查询回补进度与 ETA",
    )
    backfill_status_p.add_argument(
        "--manifest-id",
        required=True,
        type=_parse_sha256,
        help="64 位 manifest id",
    )
    backfill_status_p.add_argument(
        "--json",
        action="store_true",
        help="输出稳定的单个 JSON 对象",
    )

    suspension_backfill_p = sub.add_parser(
        "suspension-backfill",
        help="按权威交易日历回补 Tushare 停复牌事实与查询覆盖",
    )
    suspension_backfill_p.add_argument(
        "--start-date",
        required=True,
        type=_parse_iso_date,
        help="开始日期 YYYY-MM-DD",
    )
    suspension_backfill_p.add_argument(
        "--end-date",
        required=True,
        type=_parse_iso_date,
        help="结束日期 YYYY-MM-DD",
    )
    suspension_backfill_p.add_argument(
        "--full-refresh",
        action="store_true",
        help="重拉已有 complete 覆盖；默认只补缺失日期",
    )

    security_status_backfill_p = sub.add_parser(
        "security-status-backfill",
        help="预估或回补历史股票简称与 ST 状态事实",
    )
    security_status_backfill_p.add_argument(
        "--start-date",
        required=True,
        type=_parse_iso_date,
        help="开始日期 YYYY-MM-DD",
    )
    security_status_backfill_p.add_argument(
        "--end-date",
        required=True,
        type=_parse_iso_date,
        help="结束日期 YYYY-MM-DD",
    )
    security_status_backfill_p.add_argument(
        "--source-as-of",
        type=_parse_iso_date,
        default=None,
        help="数据源认知截止日（默认今天）",
    )
    security_status_backfill_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只计算标的数、交易日数和逻辑 API 调用数，不联网、不写库",
    )
    security_status_backfill_p.add_argument(
        "--full-refresh",
        action="store_true",
        help="重算已有状态；默认只补缺失或未知状态",
    )

    dataset_snapshot_p = sub.add_parser(
        "dataset-snapshot",
        help="覆盖率验收通过后固化研究数据元信息快照",
    )
    dataset_snapshot_p.add_argument(
        "--strategy",
        required=True,
        choices=strategy_choices,
        help="策略标识，必须与 manifest 一致",
    )

    data_audit_p = sub.add_parser(
        "data-audit",
        help="运行 Stage-1 研究数据审计并保存正式研究门凭证",
    )
    data_audit_p.add_argument(
        "--as-of",
        type=_parse_iso_date,
        required=True,
        help="审计截止交易日 YYYY-MM-DD",
    )
    data_audit_p.add_argument(
        "--start-date",
        type=_parse_iso_date,
        default=None,
        help="审计开始日期（默认向前 120 个自然日）",
    )
    dataset_snapshot_p.add_argument(
        "--as-of",
        required=True,
        type=_parse_iso_datetime,
        help="带时区 ISO-8601 数据截止时刻",
    )
    dataset_snapshot_p.add_argument(
        "--manifest-id",
        required=True,
        type=_parse_sha256,
        help="已完成的 64 位 manifest id",
    )
    snapshot_mode = dataset_snapshot_p.add_mutually_exclusive_group()
    snapshot_mode.add_argument(
        "--dry-run",
        action="store_true",
        help="只验证覆盖并展示执行绑定计划（默认行为）",
    )
    snapshot_mode.add_argument(
        "--apply",
        action="store_true",
        help="在非交易保护窗口写入快照元数据和不可变执行绑定",
    )
    dataset_snapshot_p.add_argument(
        "--deadline-worker",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    minute_p = sub.add_parser(
        "minute-backfill", help="回补 Pool 命中标的历史分钟线"
    )
    minute_p.add_argument(
        "--date", type=str, required=True,
        help="Pool 筛选日期 YYYY-MM-DD",
    )
    minute_p.add_argument(
        "--lookback-days", type=int, default=90,
        help="向前回补交易日数量 (默认 90)",
    )
    minute_p.add_argument(
        "--freq", type=str, default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    minute_p.add_argument(
        "--preset", type=str, default="n-shape-pool1",
        help="筛选 preset (默认 n-shape-pool1)",
    )
    minute_p.add_argument(
        "--ts-code", type=str, default=None,
        help="只回补单只股票，调试用",
    )
    minute_p.add_argument(
        "--dry-run", action="store_true",
        help="只估算请求数，不调用 Tushare、不写库",
    )

    replay_p = sub.add_parser(
        "minute-replay", help="基于历史分钟线跑强承接/突破模拟回放"
    )
    replay_p.add_argument(
        "--start-date", type=str, required=True,
        help="Pool 筛选开始日期 YYYY-MM-DD",
    )
    replay_p.add_argument(
        "--end-date", type=str, required=True,
        help="Pool 筛选结束日期 YYYY-MM-DD",
    )
    replay_p.add_argument(
        "--preset", type=str, default="n-shape-pool1",
        help="筛选 preset (默认 n-shape-pool1)",
    )
    replay_p.add_argument(
        "--freq", type=str, default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    replay_p.add_argument(
        "--entry-mode", type=str, default="first_break",
        choices=[
            "first_break",
            "break_retest",
            "late_confirm",
            "vwap_confirm",
            "amount_surge",
            "factor_confirm",
        ],
        help="入场模式 (默认 first_break)",
    )
    replay_p.add_argument(
        "--factor-score-threshold", type=float, default=35.0,
        help="factor_confirm 的 n_shape_b_v1 评分入场阈值，仅该模式生效 (默认 35)",
    )
    replay_p.add_argument(
        "--max-hold-days", type=int, default=5,
        help="最多持有交易日数量 (默认 5)",
    )
    replay_p.add_argument(
        "--volume-profile", action="store_true",
        help="启用 90 日价量分布入场过滤与动态风控",
    )
    replay_p.add_argument(
        "--volume-profile-lookbacks", type=int, nargs="+", default=[90],
        help="价量分布 lookback 交易日列表 (默认 90)",
    )
    replay_p.add_argument(
        "--output", type=str, default=None,
        help="CSV 输出路径（可选）",
    )

    growth_replay_p = sub.add_parser(
        "growth-board-surge-replay",
        help="回测科创/创业板盘中放量追击策略",
    )
    growth_replay_p.add_argument(
        "--start-date", type=str, required=True,
        help="回测开始日期 YYYY-MM-DD",
    )
    growth_replay_p.add_argument(
        "--end-date", type=str, required=True,
        help="回测结束日期 YYYY-MM-DD",
    )
    growth_replay_p.add_argument(
        "--freq", type=str, default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    growth_replay_p.add_argument(
        "--min-signal-time", type=str, default="09:30",
        help="最早 B 信号时间 HH:MM (默认 09:33)",
    )
    growth_replay_p.add_argument(
        "--lookback-days", type=int, default=20,
        help="分钟历史基准 lookback 交易日数量 (默认 20)",
    )
    growth_replay_p.add_argument(
        "--min-hist-days", type=int, default=10,
        help="至少需要的历史分钟样本交易日数量 (默认 10)",
    )
    growth_replay_p.add_argument(
        "--min-cum-amount-ratio", type=float, default=1.4,
        help="截至当前累计成交额相对历史同时间中位数倍数 (默认 1.4)",
    )
    growth_replay_p.add_argument(
        "--min-same-minute-amount-ratio", type=float, default=2.0,
        help="当前分钟成交额相对历史同分钟中位数倍数 (默认 2.0)",
    )
    growth_replay_p.add_argument(
        "--max-hold-days", type=int, default=3,
        help="最多持有交易日数量 (默认 3；接住 2-3 日延续涨幅，见退出结构报告)",
    )
    growth_replay_p.add_argument(
        "--require-inner-outer", action="store_true",
        help="要求信号分钟内盘>外盘（分钟 tick-rule 近似，用户条件 2）",
    )
    growth_replay_p.add_argument(
        "--min-inner-outer-ratio", type=float, default=1.0,
        help="内盘/外盘比下限，须严格大于 (默认 1.0 即内盘>外盘)",
    )
    growth_replay_p.add_argument(
        "--require-large-net-vol", action="store_true",
        help="要求 T-1 moneyflow 大单净量>阈值（用户条件 3，T 日盘中不可知）",
    )
    growth_replay_p.add_argument(
        "--min-large-net-vol", type=float, default=0.0,
        help="T-1 大单净量下限，须严格大于 (默认 0)",
    )
    growth_replay_p.add_argument(
        "--require-fresh-surge", action="store_true",
        help="首爆过滤：放量当天之前 N 日没放量过（经典量比口径，用户条件）",
    )
    growth_replay_p.add_argument(
        "--fresh-lookback-days", type=int, default=5,
        help="首爆回看交易日数 (默认 5)",
    )
    growth_replay_p.add_argument(
        "--min-listing-trading-days", type=int, default=0,
        help="不做新股：上市不满 N 个交易日过滤 (默认 0=关闭；推荐 180)",
    )
    growth_replay_p.add_argument(
        "--require-board-favor", action="store_true",
        help="板块集合竞价强度闸门：候选票所在题材当日竞价整体达标才入场",
    )
    growth_replay_p.add_argument(
        "--min-board-gap-up-ratio", type=float, default=0.5,
        help="板块竞价高开占比下限 (默认 0.5)",
    )
    growth_replay_p.add_argument(
        "--min-board-auction-amount-ratio", type=float, default=1.0,
        help="板块竞价总额相对历史中位下限 (默认 1.0)",
    )
    growth_replay_p.add_argument(
        "--board-hist-days", type=int, default=3,
        help="板块竞价额历史比较窗口天数 (默认 3；短窗口抓当下资金青睐)",
    )
    growth_replay_p.add_argument(
        "--factor-confirm", action="store_true",
        help="启用 growth_surge_b_v1 多因子评分确认层（宽门不动，评分过阈值才入场）",
    )
    growth_replay_p.add_argument(
        "--factor-score-threshold", type=float, default=45.0,
        help="factor_confirm 评分入场阈值，仅 --factor-confirm 时生效 (默认 45)",
    )
    growth_replay_p.add_argument(
        "--output", type=str, default=None,
        help="CSV 输出路径（可选）",
    )

    replay_backfill_p = sub.add_parser(
        "minute-replay-backfill",
        help="回补分钟 replay 所需的 B 日到退出窗口分钟线",
    )
    replay_backfill_p.add_argument(
        "--start-date", type=str, required=True,
        help="Pool 筛选开始日期 YYYY-MM-DD",
    )
    replay_backfill_p.add_argument(
        "--end-date", type=str, required=True,
        help="Pool 筛选结束日期 YYYY-MM-DD",
    )
    replay_backfill_p.add_argument(
        "--preset", type=str, default="n-shape-pool1",
        help="筛选 preset (默认 n-shape-pool1)",
    )
    replay_backfill_p.add_argument(
        "--freq", type=str, default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    replay_backfill_p.add_argument(
        "--max-hold-days", type=int, default=5,
        help="最多持有交易日数量 (默认 5)",
    )
    replay_backfill_p.add_argument(
        "--ts-code", type=str, default=None,
        help="只回补单只股票，调试用",
    )
    replay_backfill_p.add_argument(
        "--dry-run", action="store_true",
        help="只估算请求数，不调用 Tushare、不写库",
    )

    auction_p = sub.add_parser(
        "auction-backfill",
        help="回补 Tushare 集合竞价数据",
    )
    auction_p.add_argument(
        "--start-date", type=str, required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_p.add_argument(
        "--end-date", type=str, required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_p.add_argument(
        "--dry-run", action="store_true",
        help="只估算交易日请求数，不调用 Tushare、不写库",
    )

    auction_fallback_p = sub.add_parser(
        "auction-minute-fallback",
        help="用 09:30 分钟线补齐 Tushare 集合竞价缺行",
    )
    auction_fallback_p.add_argument(
        "--date",
        type=str,
        required=True,
        help="交易日期 YYYY-MM-DD",
    )
    auction_fallback_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只估算补齐行数，不写库",
    )

    auction_gap_p = sub.add_parser(
        "auction-gap-replay",
        help="回测集合竞价跳空高开策略",
    )
    auction_gap_p.add_argument(
        "--start-date", type=str, required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_gap_p.add_argument(
        "--end-date", type=str, required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_gap_p.add_argument(
        "--gap-mode", type=str, default="close",
        choices=["close", "strict_high"],
        help="跳空定义：close=竞价价高于昨收；strict_high=竞价价高于昨高",
    )
    auction_gap_p.add_argument(
        "--st-filter", type=str, default="case_insensitive",
        choices=["case_insensitive", "literal_lower", "none"],
        help="ST 过滤：默认大小写不敏感过滤 ST/*ST",
    )
    auction_gap_p.add_argument(
        "--min-ratio", type=float, default=0.15,
        help="竞价量/近5日均量下限 (默认 0.15)",
    )
    auction_gap_p.add_argument(
        "--max-ratio", type=float, default=5.0,
        help="竞价量/近5日均量上限 (默认 5)",
    )
    auction_gap_p.add_argument(
        "--output", type=str, default=None,
        help="CSV 输出路径（可选）",
    )

    auction_gap_minute_p = sub.add_parser(
        "auction-gap-minute-replay",
        help="回测集合竞价候选 + 分钟 B/S 策略",
    )
    auction_gap_minute_p.add_argument(
        "--start-date", type=str, required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_gap_minute_p.add_argument(
        "--end-date", type=str, required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_gap_minute_p.add_argument(
        "--gap-mode", type=str, default="close",
        choices=["close", "strict_high"],
        help="跳空定义：close=竞价价高于昨收；strict_high=竞价价高于昨高",
    )
    auction_gap_minute_p.add_argument(
        "--st-filter", type=str, default="case_insensitive",
        choices=["case_insensitive", "literal_lower", "none"],
        help="ST 过滤：默认大小写不敏感过滤 ST/*ST",
    )
    auction_gap_minute_p.add_argument(
        "--min-ratio", type=float, default=0.15,
        help="竞价量/近5日均量下限 (默认 0.15)",
    )
    auction_gap_minute_p.add_argument(
        "--max-ratio", type=float, default=5.0,
        help="竞价量/近5日均量上限 (默认 5)",
    )
    auction_gap_minute_p.add_argument(
        "--max-hold-days", type=int, default=1,
        help="最多持有交易日数量 (默认 1)",
    )
    auction_gap_minute_p.add_argument(
        "--seal-hold-days", type=int, default=None,
        help="封板质量达标仓位的持有上限（交易日）；不传保持关闭（全部 T+1）",
    )
    auction_gap_minute_p.add_argument(
        "--seal-hold-max-open-times", type=int, default=0,
        help="seal_hold 允许的最大开板次数（官方 limit_list_daily.open_times，默认 0）",
    )
    auction_gap_minute_p.add_argument(
        "--factor-score-threshold", type=float, default=None,
        help="分钟 B 确认的 auction_gap_b_v1 评分阈值（不传=现状不评分；判死复核用）",
    )
    auction_gap_minute_p.add_argument(
        "--output", type=str, default=None,
        help="CSV 输出路径（可选）",
    )
    auction_gap_minute_p.add_argument(
        "--persist-positions", action="store_true",
        help="模拟仓落库（run_mode=replay，带信号溯源；写主库，盘中会撞 monitor 写锁）",
    )
    auction_gap_minute_p.add_argument(
        "--run-id", type=str, default=None,
        help="落库批次标识（不传自动生成；可按 run_id 整批清理）",
    )

    auction_gap_minute_backfill_p = sub.add_parser(
        "auction-gap-minute-backfill",
        help="回补集合竞价跳空候选的分钟 replay 窗口",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--start-date", type=str, required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--end-date", type=str, required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--gap-mode", type=str, default="close",
        choices=["close", "strict_high"],
        help="跳空定义：close=竞价价高于昨收；strict_high=竞价价高于昨高",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--st-filter", type=str, default="case_insensitive",
        choices=["case_insensitive", "literal_lower", "none"],
        help="ST 过滤：默认大小写不敏感过滤 ST/*ST",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--min-ratio", type=float, default=0.15,
        help="竞价量/近5日均量下限 (默认 0.15)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--max-ratio", type=float, default=5.0,
        help="竞价量/近5日均量上限 (默认 5)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--max-hold-days", type=int, default=1,
        help="最多持有交易日数量 (默认 1)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--freq", type=str, default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--ts-code", type=str, default=None,
        help="只回补单只股票，调试用",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--lookback-days", type=int, default=0,
        help="窗口起点向前扩 N 个交易日（相对放量特征需要信号日前的历史分钟，默认 0）",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--dry-run", action="store_true",
        help="只估算请求数，不调用 Tushare、不写库",
    )

    sentiment_p = sub.add_parser(
        "sentiment-recompute",
        help="重算市场情绪/温度指标（market_sentiment_daily，含 60 日新高占比等）",
    )
    sentiment_p.add_argument(
        "--start-date", type=str, required=True,
        help="开始日期 YYYY-MM-DD",
    )
    sentiment_p.add_argument(
        "--end-date", type=str, required=True,
        help="结束日期 YYYY-MM-DD",
    )

    pool2_p = sub.add_parser("pool2", help="管理 Pool 2 持久池")
    pool2_sub = pool2_p.add_subparsers(dest="pool2_action")
    pool2_sub.add_parser("list", help="列出 Pool 2 标的")
    pool2_rm = pool2_sub.add_parser("remove", help="移除标的")
    pool2_rm.add_argument("ts_code", type=str, help="股票代码 (如 002415.SZ)")

    bl_p = sub.add_parser("blacklist", help="管理风险黑名单")
    bl_sub = bl_p.add_subparsers(dest="blacklist_action")

    bl_imp = bl_sub.add_parser("import", help="从 PDF 导入黑名单（mac 端用）")
    bl_imp.add_argument("pdf", type=str, help="PDF 路径")
    bl_imp.add_argument(
        "--label", type=str, default="430黑名单",
        help="名单标签 (默认 430黑名单)",
    )
    bl_imp.add_argument(
        "--validity", type=int, default=365,
        help="有效期天数 (默认 365)",
    )

    bl_load = bl_sub.add_parser(
        "load-parquet", help="从 parquet 加载到 DuckDB（云端推送后用）"
    )
    bl_load.add_argument("parquet", type=str, help="parquet 文件路径")
    bl_load.add_argument(
        "--label", type=str, default=None,
        help="只替换该 label 的行（默认全表覆盖）",
    )

    bl_exp = bl_sub.add_parser(
        "export-parquet", help="导出黑名单到 parquet（mac 端推云前用）"
    )
    bl_exp.add_argument(
        "--output", type=str, default="data/risk_blacklist.parquet",
        help="输出路径 (默认 data/risk_blacklist.parquet)",
    )
    bl_exp.add_argument(
        "--label", type=str, default=None,
        help="只导出该 label（默认全表导出）",
    )

    bl_ls = bl_sub.add_parser("list", help="列出黑名单")
    bl_ls.add_argument("--label", type=str, default=None, help="过滤 label")
    bl_ls.add_argument(
        "--include-expired", action="store_true", help="包含已过期条目"
    )

    bl_chk = bl_sub.add_parser("check", help="查询某只股票是否在黑名单")
    bl_chk.add_argument("ts_code", type=str, help="股票代码 (如 600340.SH)")

    bl_rm = bl_sub.add_parser("remove", help="删除整个 list_label")
    bl_rm.add_argument("--label", type=str, required=True, help="要删除的 label")

    sub.add_parser("notify-test", help="推一条 PushDeer 测试消息")
    dr_p = sub.add_parser("daily-report", help="生成并推送当日健康摘要（systemd timer 自动跑）")
    dr_p.add_argument(
        "--dry-run", action="store_true",
        help="只打印不推送（mac 本地 smoke 测试用）",
    )

    mp_p = sub.add_parser(
        "morning-pulse", help="盘中 30 分钟脉搏（launchd 10:00/10:30/11:00/11:30 自动跑）",
    )
    mp_p.add_argument(
        "--slot", type=str, default=None,
        help="手动补跑指定槽位 HH:MM（10:00/10:30/11:00/11:30）；不传按当前时间归槽",
    )
    mp_p.add_argument("--force", action="store_true", help="绕过当日去重，覆盖重跑")
    mp_p.add_argument(
        "--dry-run", action="store_true", help="全流程跑但不推送（打印报文，parquet 照落）",
    )

    mdr_p = sub.add_parser("midday-report", help="午间战报（launchd 12:00 自动跑）")
    mdr_p.add_argument("--date", type=str, default=None, help="指定日期 YYYY-MM-DD（默认今天）")
    mdr_p.add_argument("--force", action="store_true", help="绕过当日去重，覆盖重跑")
    mdr_p.add_argument(
        "--dry-run", action="store_true", help="全流程跑但不推送（打印报文，parquet 照落）",
    )

    pmc_p = sub.add_parser(
        "pre-market-check", help="开盘前主动健康体检（systemd timer Mon..Fri 09:00 自动跑）",
    )
    pmc_p.add_argument(
        "--dry-run", action="store_true",
        help="只打印不推送（mac 本地 smoke 测试用）",
    )

    pf_p = sub.add_parser(
        "preflight", help="全家服务深度体检（手动触发，dry-run，不重启服务）",
    )
    pf_p.add_argument(
        "--notify", action="store_true",
        help="跑完推一条摘要到 PushDeer（默认只 stdout）",
    )
    pf_p.add_argument(
        "--profile",
        choices=("production", "research"),
        default="production",
        help="数据新鲜度契约范围（默认 production）",
    )

    from rquant.surge_watch import SurgeConfig

    sw_p = sub.add_parser(
        "surge-watch", help="每分钟爆量推送（云端 systemd timer 09:25 拉起，15:02 自退）",
    )
    sw_p.add_argument(
        "--dry-run", action="store_true", help="全流程跑但不推送（打印报文，parquet 照落）",
    )
    sw_p.add_argument(
        "--simulate", type=str, default=None,
        help="离线回放目录内快照 parquet 序列（逐分钟，可测性设施）",
    )
    sw_p.add_argument(
        "--force-session", action="store_true", help="忽略时段守卫（盘后验收用）",
    )
    sw_p.add_argument(
        "--max-ticks", type=int, default=None,
        help="限定循环次数（dry-run / 盘后 smoke，默认跑到 15:02）",
    )
    sw_p.add_argument(
        "--k-cum", type=float, default=SurgeConfig.model_fields["k_cum"].default,
        help="确认层纯累计比值下门（默认 2.5，2026-07-06 全天分钟回测标定）",
    )
    sw_p.add_argument(
        "--ratio-cap", type=float, default=SurgeConfig.model_fields["ratio_cap"].default,
        help="累计比值上门/毒尾封顶（默认 8.0，超过视为极端出货不推）",
    )
    sw_p.add_argument(
        "--skip-first-minutes", type=int,
        default=SurgeConfig.model_fields["skip_first_minutes"].default,
        help="跳过开盘前 N 分钟确认（默认 1，9:32 起才确认，base 分母噪声大）",
    )
    sw_p.add_argument(
        "--k-delta", type=float, default=SurgeConfig.model_fields["k_delta_confirm"].default,
        help="单分钟增量门倍数（v2 遗留，默认 0=关闭）",
    )
    sw_p.add_argument(
        "--require-vwap", action="store_true",
        help="启用 VWAP 门（v2 遗留，默认关；现价 ≥ 当日均价才确认）",
    )
    sw_p.add_argument(
        "--max-room", type=float,
        default=SurgeConfig.model_fields["max_room_to_limit_pct"].default,
        help="可买性守卫：现价距涨停 ≤ 该%%（或已封板）不推送（默认 1.0）",
    )

    alert_p = sub.add_parser("alert", help="发运维告警（systemd OnFailure / watchdog 用）")
    alert_p.add_argument("--subject", required=True, help="告警主题")
    alert_p.add_argument("--body", default="", help="告警正文（可选）")
    alert_p.add_argument("--dedup-key", default=None, help="跨进程去重事件键（默认按主题）")
    alert_p.add_argument(
        "--cooldown-seconds",
        type=int,
        default=None,
        help="同类事件冷却秒数（默认读取 NOTIFY_OPS_COOLDOWN_SECONDS；0 表示关闭）",
    )
    alert_p.add_argument("--force", action="store_true", help="忽略冷却期强制发送")

    alert_resolve_p = sub.add_parser("alert-resolve", help="服务恢复后关闭运维告警事故")
    alert_resolve_p.add_argument("--dedup-key", required=True, help="要关闭的事故事件键")

    lab_run_p = sub.add_parser(
        "lab-run", help="执行 Strategy Lab 后台任务 spec（UI「后台运行」派生，内部命令）",
    )
    lab_run_p.add_argument(
        "--spec", type=str, required=True,
        help="任务 spec JSON 路径（launch_background_run 生成）",
    )

    pa_serve_p = sub.add_parser(
        "panorama-auth-serve",
        help="启动全景页登录网关服务（微信友好 cookie 登录，标准库 http.server）",
    )
    pa_serve_p.add_argument(
        "--host", type=str, default="127.0.0.1",
        help="监听地址 (默认 127.0.0.1，只给 nginx 反代)",
    )
    pa_serve_p.add_argument(
        "--port", type=int, default=8507,
        help="监听端口 (默认 8507)",
    )

    pa_add_p = sub.add_parser(
        "panorama-user-add", help="添加/更新全景页登录用户（交互式输密码，覆盖同名）",
    )
    pa_add_p.add_argument("name", type=str, help="用户名（字母/数字/点/下划线/短横）")

    pa_rm_p = sub.add_parser("panorama-user-remove", help="移除全景页登录用户")
    pa_rm_p.add_argument("name", type=str, help="用户名")

    sub.add_parser("panorama-user-list", help="列出全景页登录用户名（不含哈希）")

    sub.add_parser(
        "panorama-gate-token",
        help="打印当前生效的 map 网关令牌（写进 nginx map 文件，只读）",
    )

    return parser


def main() -> int:
    """CLI 入口函数。一次性命令的异常顶层捕获后推 PushDeer。

    serve 内的 daily_job 自有 try/except + notify，main 不重复抓。
    """
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {
        "serve": cmd_serve,
        "run-daily": cmd_run_daily,
        "ingest": cmd_ingest,
        "daily-indicator-backfill": cmd_daily_indicator_backfill,
        "monitor": cmd_monitor,
        "rt-minute-fetch": cmd_rt_minute_fetch,
        "rt-minute-daily-fetch": cmd_rt_minute_daily_fetch,
        "research-sync": cmd_research_sync,
        "research-export": cmd_research_export,
        "research-ingest": cmd_research_ingest,
        "research-ingest-readiness": cmd_research_ingest_readiness,
        "research-authority-status": cmd_research_authority_status,
        "research-migration": cmd_research_migration,
        "trade-calendar-bootstrap": cmd_trade_calendar_bootstrap,
        "sentiment-recompute": cmd_sentiment_recompute,
        "moneyflow-backfill": cmd_moneyflow_backfill,
        "market-daily-backfill": cmd_market_daily_backfill,
        "zt-pool-capture": cmd_zt_pool_capture,
        "zt-pool-repair": cmd_zt_pool_repair,
        "limit-list-backfill": cmd_limit_list_backfill,
        "data-backfill": cmd_data_backfill,
        "backfill-plan": cmd_backfill_plan,
        "backfill-run": cmd_backfill_run,
        "backfill-status": cmd_backfill_status,
        "suspension-backfill": cmd_suspension_backfill,
        "security-status-backfill": cmd_security_status_backfill,
        "dataset-snapshot": cmd_dataset_snapshot,
        "data-audit": cmd_data_audit,
        "minute-backfill": cmd_minute_backfill,
        "minute-replay-backfill": cmd_minute_replay_backfill,
        "auction-backfill": cmd_auction_backfill,
        "auction-minute-fallback": cmd_auction_minute_fallback,
        "auction-gap-replay": cmd_auction_gap_replay,
        "auction-gap-minute-replay": cmd_auction_gap_minute_replay,
        "auction-gap-minute-backfill": cmd_auction_gap_minute_backfill,
        "minute-replay": cmd_minute_replay,
        "growth-board-surge-replay": cmd_growth_board_surge_replay,
        "pool2": cmd_pool2,
        "blacklist": cmd_blacklist,
        "notify-test": cmd_notify_test,
        "alert": cmd_alert,
        "alert-resolve": cmd_alert_resolve,
        "daily-report": cmd_daily_report,
        "morning-pulse": cmd_morning_pulse,
        "midday-report": cmd_midday_report,
        "pre-market-check": cmd_pre_market_check,
        "preflight": cmd_preflight,
        "surge-watch": cmd_surge_watch,
        "lab-run": cmd_lab_run,
        "panorama-auth-serve": cmd_panorama_auth_serve,
        "panorama-user-add": cmd_panorama_user_add,
        "panorama-user-remove": cmd_panorama_user_remove,
        "panorama-user-list": cmd_panorama_user_list,
        "panorama-gate-token": cmd_panorama_gate_token,
    }
    handler = dispatch.get(args.command)
    if handler is None:
        parser.print_help()
        return 0

    # alert / daily-report 自身就是日常运维路径，main 不该再吞它的异常包一层；
    # lab-run 的失败已有 status 文件 + UI 展示，不该再推运维告警
    # panorama-auth-* 是独立登录网关，不依赖 notify 体系，SECRET 缺失走 SystemExit
    # （非 Exception，本就不被下方 except 捕获），不该再包一层运维告警。
    if args.command in (
        "serve", "notify-test", "alert", "alert-resolve",
        "daily-report", "pre-market-check", "preflight", "data-audit", "lab-run",
        "panorama-auth-serve", "panorama-user-add",
        "panorama-user-remove", "panorama-user-list", "panorama-gate-token",
    ):
        return handler(args)

    try:
        return handler(args)
    except Exception as e:
        logger.exception(f"=== {args.command} 异常 ===")
        from rquant.notify import notify
        if args.command not in {"monitor", "surge-watch"}:
            notify("error", component=f"cli:{args.command}", exc=e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
