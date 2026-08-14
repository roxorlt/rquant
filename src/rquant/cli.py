"""CLI 入口：rquant serve / rquant run-daily / rquant ingest。"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from types import FrameType, SimpleNamespace, TracebackType
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from loguru import logger

from rquant.backfill_state import (
    BackfillStateStore,
    BackfillWorkloadTelemetry,
    ManifestAbandonmentConflictError,
    ManifestContentConflictError,
    StaleManifestAbandonmentError,
    UnknownManifestError,
    open_backfill_state_snapshot,
)
from rquant.logging import setup_logging
from rquant.storage.duckdb import DuckDBStore, open_readonly_store

if TYPE_CHECKING:
    from rquant.config import Settings
    from rquant.daily_pipeline_report_authority import (
        DailyPipelineDevelopmentTestReportAuthority,
        DailyPipelineReportAuthorityCapability,
    )
    from rquant.lab_daemon import AttestedLabRuntimeGuard
    from rquant.lab_worker import LabResourceAuthorityManifest
    from rquant.runtime_code_attestation import CodeTrustEvidence
    from rquant.runtime_deployment_bundle import RuntimeDeploymentReceipt
    from rquant.runtime_deployment_profile import LabHighWaterRuntimeProfile
    from rquant.runtime_resource_admission import (
        ResourceProbe,
        RuntimeResourceAdmissionBindings,
    )

# 重试配置
_RETRY_COUNT = 3
_RETRY_INTERVAL = 900  # 数据未就绪：15 分钟（等 tushare 数据出来）
_NETWORK_RETRY_INTERVAL = 60  # 网络异常：1 分钟（tushare 抖动通常很快恢复）


def _daily_notification_producer_commit() -> str:
    """Use the deployment-bound commit when available; mark legacy CLI events as unverified."""
    candidate = os.getenv("RQUANT_CODE_COMMIT", "").strip().lower()
    if len(candidate) == 40 and all(character in "0123456789abcdef" for character in candidate):
        return candidate
    return "0" * 40


def _record_daily_error_outbox(
    *,
    component: str,
    exc: BaseException,
    trade_date: date,
) -> None:
    """Persist a typed daily-close error signal; notification failures stop at health logging."""
    try:
        from rquant.config import settings
        from rquant.daily_notification_producer import (
            DailyNotificationProducer,
            build_daily_error_signal,
        )
        from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
        from rquant.signal_bus import SignalBusStore

        targets = tuple(
            [
                *(
                    DeliveryTarget(
                        recipient_id=recipient_id,
                        channel=DeliveryChannel.PUSHDEER,
                    )
                    for recipient_id in settings.pushdeer_recipient_id_list
                ),
                *(
                    DeliveryTarget(
                        recipient_id=recipient_id,
                        channel=DeliveryChannel.PUSHPLUS,
                    )
                    for recipient_id in settings.pushplus_recipient_id_list
                ),
            ]
            if settings.notify_enabled and settings.notify_error
            else []
        )
        observed_at = datetime.now(UTC)
        signal = build_daily_error_signal(
            component=component,
            error=exc,
            trade_date=trade_date,
            observed_at=observed_at,
            producer_commit=_daily_notification_producer_commit(),
        )
        receipt = DailyNotificationProducer(
            signal_bus=SignalBusStore(settings.data_dir / "daily-close-signal-bus.sqlite3"),
            targets=targets,
        ).emit(signal, received_at=observed_at)
        logger.error(
            "daily error persisted to notification outbox: component={} trade_date={} "
            "signal_id={} targets={}",
            component,
            trade_date.isoformat(),
            receipt.signal_id,
            len(receipt.outbox_ids),
        )
    except Exception as notification_error:
        logger.error(
            "daily_notification_health=degraded component={} trade_date={} error_type={}",
            component,
            trade_date.isoformat(),
            type(notification_error).__name__,
        )


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
            logger.error(f"{trade_date} ingest 重试 {_RETRY_COUNT} 次仍失败: {e}")
            raise

        if bar_count > 0:
            return bar_count
        if attempt < _RETRY_COUNT:
            logger.warning(
                f"数据未就绪，{_RETRY_INTERVAL // 60} 分钟后重试 ({attempt}/{_RETRY_COUNT})"
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
        raise argparse.ArgumentTypeError(f"时间格式应为带时区 ISO-8601: {value}") from exc
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


def _parse_commit_sha(value: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 40:
        raise argparse.ArgumentTypeError("code SHA 必须是 40 位十六进制")
    try:
        int(normalized, 16)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("code SHA 必须是 40 位十六进制") from exc
    return normalized


def _parse_formal_smoke_timeout_seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("formal smoke timeout must be a number") from exc
    if not math.isfinite(parsed) or not 0.1 <= parsed <= 86_400:
        raise argparse.ArgumentTypeError(
            "formal smoke timeout must be between 0.1 and 86400 seconds"
        )
    return parsed


def _parse_bounded_int(
    value: str,
    *,
    minimum: int,
    maximum: int,
    label: str,
) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{label} must be an integer") from exc
    if not minimum <= parsed <= maximum:
        raise argparse.ArgumentTypeError(f"{label} must be between {minimum} and {maximum}")
    return parsed


def _parse_recovery_deadline_seconds(value: str) -> int:
    return _parse_bounded_int(
        value,
        minimum=1,
        maximum=86_400,
        label="recovery deadline seconds",
    )


def _parse_rehearsal_interval_seconds(value: str) -> int:
    return _parse_bounded_int(
        value,
        minimum=60,
        maximum=31_536_000,
        label="rehearsal interval seconds",
    )


def _parse_recovery_max_attempts(value: str) -> int:
    return _parse_bounded_int(
        value,
        minimum=1,
        maximum=10,
        label="recovery max attempts",
    )


def _parse_recovery_retry_delay_seconds(value: str) -> int:
    return _parse_bounded_int(
        value,
        minimum=1,
        maximum=3_600,
        label="recovery retry delay seconds",
    )


_SHANGHAI = ZoneInfo("Asia/Shanghai")
_BACKFILL_PROTECTED_START = dtime(9, 15)
_BACKFILL_PROTECTED_END = dtime(15, 10)
_BACKFILL_DEADLINE_MARGIN_MINUTES = 10
_BACKFILL_HARD_DEADLINE_GRACE_MINUTES = 5
_BACKFILL_PARALLEL_OVERHEAD = 1.25
_BACKFILL_MIN_TELEMETRY_SAMPLES = 32
_BACKFILL_COLD_TASK_SECONDS = 10.416
_BACKFILL_COLD_ROWS_PER_SECOND = 651.0
_SNAPSHOT_BINDING_ESTIMATED_SECONDS = 1_800.0
_SNAPSHOT_DEADLINE_MARGIN_SECONDS = 60


class _SnapshotWriteDeadlineError(RuntimeError):
    pass


@dataclass(frozen=True)
class _BackfillExecutionEstimate:
    serial_seconds: float
    point_seconds: float
    guard_seconds: float
    source: str


def _estimate_parallel_backfill(
    *,
    static_total_seconds: float,
    total_tasks: int,
    worker_count: int,
    telemetry: BackfillWorkloadTelemetry,
    static_rate_limit_seconds: float = 0.0,
) -> _BackfillExecutionEstimate:
    if total_tasks < 0:
        raise ValueError("total_tasks must not be negative")
    if not 1 <= worker_count <= 16:
        raise ValueError("worker_count must be between 1 and 16")
    remaining_ratio = telemetry.remaining_tasks / total_tasks if total_tasks else 0.0
    remaining_rate_limit_seconds = max(0.0, static_rate_limit_seconds) * remaining_ratio
    task_floor_seconds = 0.0
    candidates = [
        (
            max(0.0, static_total_seconds) * remaining_ratio,
            "static",
        )
    ]
    if telemetry.sample_task_count < _BACKFILL_MIN_TELEMETRY_SAMPLES:
        task_floor_seconds = _BACKFILL_COLD_TASK_SECONDS
        candidates.extend(
            (
                (
                    telemetry.remaining_tasks * _BACKFILL_COLD_TASK_SECONDS,
                    "production_cold_start",
                ),
                (
                    telemetry.remaining_expected_rows / _BACKFILL_COLD_ROWS_PER_SECOND,
                    "production_cold_start",
                ),
            )
        )
    if (
        telemetry.sample_task_count
        and telemetry.p75_task_seconds is not None
        and telemetry.p75_seconds_per_row is not None
    ):
        task_floor_seconds = max(
            task_floor_seconds,
            telemetry.p75_task_seconds,
        )
        candidates.extend(
            (
                (
                    telemetry.remaining_tasks * telemetry.p75_task_seconds,
                    "historical_p75",
                ),
                (
                    (telemetry.remaining_expected_rows * telemetry.p75_seconds_per_row),
                    "historical_p75",
                ),
            )
        )
    serial_seconds, source = max(candidates, key=lambda candidate: candidate[0])
    effective_workers = max(
        1,
        min(worker_count, telemetry.remaining_tasks or 1),
    )
    point_seconds = max(
        serial_seconds / effective_workers * _BACKFILL_PARALLEL_OVERHEAD,
        task_floor_seconds,
        remaining_rate_limit_seconds,
    )
    return _BackfillExecutionEstimate(
        serial_seconds=serial_seconds,
        point_seconds=point_seconds,
        guard_seconds=point_seconds * 2 + 1_800,
        source=source,
    )


def _resolve_backfill_stop_before(
    now: datetime,
    *,
    max_runtime_minutes: int | None,
) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("backfill deadline time must be timezone-aware")
    local = now.astimezone(_SHANGHAI)
    if _in_backfill_protected_window(local):
        raise ValueError("backfill cannot start inside the protected window")
    protected_deadline = _next_backfill_protected_start(local) - timedelta(
        minutes=_BACKFILL_DEADLINE_MARGIN_MINUTES
    )
    if max_runtime_minutes is None:
        return protected_deadline
    if max_runtime_minutes < 1:
        raise ValueError("max_runtime_minutes must be positive")
    requested_deadline = local + timedelta(minutes=max_runtime_minutes)
    return min(protected_deadline, requested_deadline)


def _resolve_backfill_hard_deadline(stop_before: datetime) -> datetime:
    if stop_before.tzinfo is None or stop_before.utcoffset() is None:
        raise ValueError("backfill stop deadline must be timezone-aware")
    return stop_before.astimezone(_SHANGHAI) + timedelta(
        minutes=_BACKFILL_HARD_DEADLINE_GRACE_MINUTES
    )


def _snapshot_now() -> datetime:
    return datetime.now(UTC)


def _backfill_now() -> datetime:
    return datetime.now(UTC)


def _in_backfill_protected_window(now: datetime) -> bool:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("protected-window time must be timezone-aware")
    local = now.astimezone(_SHANGHAI)
    return (
        local.weekday() < 5 and _BACKFILL_PROTECTED_START <= local.time() <= _BACKFILL_PROTECTED_END
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
        remaining = (self.deadline - _snapshot_now().astimezone(_SHANGHAI)).total_seconds()
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
    remaining = (deadline - _snapshot_now().astimezone(_SHANGHAI)).total_seconds()
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
        logger.error("dataset snapshot worker was killed at the protected-window deadline")
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


def _run_backfill_supervised_worker(
    args: argparse.Namespace,
    *,
    stop_before: datetime,
    hard_deadline: datetime,
) -> int:
    command = [
        sys.executable,
        "-m",
        "rquant.cli",
        "backfill-run",
        "--manifest-id",
        str(args.manifest_id),
        "--workers",
        str(args.workers),
    ]
    if bool(args.retry_failed):
        command.append("--retry-failed")
    command.extend(
        (
            "--stop-before",
            stop_before.isoformat(),
            "--deadline-worker",
        )
    )
    return _run_deadline_supervised_process(
        command,
        deadline=hard_deadline,
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
        if getattr(parsed, "command", None) == "research-repair-auction":
            apply_requested = bool(getattr(parsed, "apply", False))
            plan_supplied = getattr(parsed, "plan_id", None) is not None
            if apply_requested != plan_supplied:
                self.error("竞价历史修复必须同时传 --apply 和 --plan-id")
        if getattr(parsed, "command", None) == "research-repair-minute":
            apply_requested = bool(getattr(parsed, "apply", False))
            plan_supplied = getattr(parsed, "plan_id", None) is not None
            if apply_requested != plan_supplied:
                self.error("分钟历史修复必须同时传 --apply 和 --plan-id")
        if getattr(parsed, "command", None) == "runtime-deployment-profile":
            apply_requested = bool(getattr(parsed, "apply", False))
            profile_id_supplied = getattr(parsed, "profile_id", None) is not None
            if apply_requested != profile_id_supplied:
                self.error("运行时画像正式安装必须同时传 --apply 和 --profile-id")
        if getattr(parsed, "command", None) == "runtime-production-prerequisites":
            apply_requested = bool(getattr(parsed, "apply", False))
            profile_id_supplied = getattr(parsed, "profile_id", None) is not None
            if apply_requested != profile_id_supplied:
                self.error("生产前置 authority 安装必须同时传 --apply 和 --profile-id")
        if getattr(parsed, "command", None) == "runtime-production-profile":
            apply_requested = bool(getattr(parsed, "apply", False))
            profile_id_supplied = getattr(parsed, "profile_id", None) is not None
            if apply_requested != profile_id_supplied:
                self.error("生产运行时画像发布必须同时传 --apply 和 --profile-id")
        return parsed


def cmd_serve(args: argparse.Namespace) -> int:
    """启动 APScheduler 常驻进程。"""
    from apscheduler.schedulers.blocking import BlockingScheduler

    from rquant.pipeline import run_daily_pipeline

    setup_logging()
    _bridge_apscheduler_logging()

    scheduler = BlockingScheduler()

    @scheduler.scheduled_job(
        "cron",
        hour=args.hour,
        minute=0,
        day_of_week="mon-fri",
        misfire_grace_time=7200,  # 允许延迟 2 小时仍执行
        coalesce=True,  # 多次 misfire 合并为一次执行
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
            _record_daily_error_outbox(
                component="daily_job",
                exc=e,
                trade_date=date.fromisoformat(trade_date),
            )

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
    from rquant import indicator_backfill as indicator_backfill_module
    from rquant.indicator_backfill import (
        DailyIndicatorBackfillCoverageError,
        DailyIndicatorBackfillProtectedWindowError,
    )

    setup_logging()
    try:
        result = indicator_backfill_module.run_daily_indicator_backfill(
            reader_factory=(
                indicator_backfill_module.open_detached_daily_indicator_store
                if args.apply
                else open_readonly_store
            ),
            writer_factory=DuckDBStore,
            start_date=args.start_date,
            end_date=args.end_date,
            apply=args.apply,
        )
    except (
        DailyIndicatorBackfillCoverageError,
        DailyIndicatorBackfillProtectedWindowError,
    ) as error:
        logger.error(str(error))
        return 2
    print(result.model_dump_json(indent=2))
    return 0


def _daily_dag_control_plan(args: argparse.Namespace):
    """Build one immutable plan without opening the writable daily ledger."""
    from rquant.daily_pipeline_control import (
        DailyPipelineControlPlan,
        resolve_production_daily_storage_profile,
    )
    from rquant.daily_pipeline_ledger import DailyPipelineMode, DailyPipelineStorageProfile
    from rquant.daily_pipeline_orchestrator import DEFAULT_DAILY_CLOSE_PIPELINE

    if args.command == "daily-dag":
        mode = DailyPipelineMode.PRODUCTION
        storage_profile = resolve_production_daily_storage_profile(
            expected_code_commit=args.code_commit,
            expected_profile_hash=args.profile_hash,
        )
    elif args.command == "daily-dag-dev":
        mode = DailyPipelineMode.SHADOW
        storage_profile = DailyPipelineStorageProfile.create(
            root=args.profile_root,
            mode=mode,
            profile_hash=args.profile_hash,
        )
    else:  # pragma: no cover - parser and dispatch constrain this boundary
        raise ValueError("daily DAG command mode is unsupported")
    spec = DEFAULT_DAILY_CLOSE_PIPELINE.to_run_spec(
        mode=mode,
        trade_date=args.trade_date,
        source_generation_id=args.source_generation_id,
        source_content_hash=args.source_content_hash,
        command_manifest_hash=args.command_manifest_hash,
        code_commit=args.code_commit,
        profile_hash=args.profile_hash,
        deadline_at=args.deadline_at,
    )
    return DailyPipelineControlPlan.create(
        mode=mode,
        run_spec=spec,
        command_manifest_hash=args.command_manifest_hash,
        storage_profile=storage_profile,
    )


def cmd_daily_dag(
    args: argparse.Namespace,
    *,
    development_test_report_authority: DailyPipelineDevelopmentTestReportAuthority | None = None,
) -> int:
    """Preview or advance one exact, externally receipted daily DAG stage."""
    from rquant.daily_pipeline_report_authority import (
        DailyPipelineDevelopmentTestReportAuthority,
    )

    production_command = args.command == "daily-dag"
    development_command = args.command == "daily-dag-dev"
    if not production_command and not development_command:
        logger.error("daily-dag command mode is unsupported")
        return 2
    development_absence_guard = None
    if development_command:
        from rquant.daily_pipeline_control import (
            DailyPipelineProductionProfileError,
            assert_daily_dag_dev_allowed,
        )

        try:
            development_absence_guard = assert_daily_dag_dev_allowed()
        except DailyPipelineProductionProfileError as exc:
            logger.error("daily-dag-dev refused fixed production profile state: {}", exc)
            return 2

    def reconfirm_development_absence() -> bool:
        if development_absence_guard is None:
            return True
        try:
            development_absence_guard.assert_still_absent()
        except DailyPipelineProductionProfileError as exc:
            logger.error("daily-dag-dev production root absence changed: {}", exc)
            return False
        return True

    from rquant.config import settings as runtime_settings

    if development_test_report_authority is not None:
        if not isinstance(
            development_test_report_authority,
            DailyPipelineDevelopmentTestReportAuthority,
        ):
            logger.error("daily-dag rejects an untyped report authority override")
            return 2
        if production_command:
            logger.error("daily-dag production rejects development-test report authority")
            return 2
    if development_command and runtime_settings.app_env == "prod":
        logger.error("daily-dag-dev is disabled in the production application profile")
        return 2
    if production_command and runtime_settings.app_env != "prod":
        logger.error("daily-dag production requires the production application profile")
        return 2
    try:
        plan = _daily_dag_control_plan(args)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.error("daily-dag refused an invalid immutable storage profile: {}", exc)
        return 2
    if not reconfirm_development_absence():
        return 2
    if args.action == "preview":
        _print_json(
            {
                "command": args.command,
                "action": "preview",
                "mode": plan.mode,
                "plan_hash": plan.plan_hash,
                "run_id": plan.run_spec.run_id,
                "state_path": str(plan.storage_profile.state_path),
                "command_manifest_path": str(plan.storage_profile.command_manifest_path),
                "receipt_root": str(plan.storage_profile.receipt_root),
                "report_root": str(plan.storage_profile.report_root),
                "stage_ids": [stage.stage_id for stage in plan.run_spec.stages],
                "write_performed": False,
            }
        )
        return 0
    if args.run_id != plan.run_spec.run_id or args.plan_hash != plan.plan_hash:
        logger.error("daily-dag action binding does not match the previewed plan/run")
        return 2
    if args.action == "status":
        if not reconfirm_development_absence():
            return 2
        if not plan.storage_profile.state_path.exists():
            logger.error(
                "daily-dag status state does not exist: {}",
                plan.storage_profile.state_path,
            )
            return 2
        from rquant.daily_pipeline_ledger import DailyPipelineLedger
        from rquant.daily_pipeline_orchestrator import DailyPipelineStatus

        ledger = DailyPipelineLedger(
            storage_profile=plan.storage_profile,
            service_owner=args.service_owner,
        )
        run = ledger.run(plan.run_spec.run_id)
        if run.spec != plan.run_spec:
            logger.error("daily-dag status state run does not match the bound plan")
            return 2
        stage_states = {
            stage.stage_id: ledger.stage(run.run_id, stage.stage_id).state
            for stage in plan.run_spec.stages
        }
        status = DailyPipelineStatus(
            run_id=run.run_id,
            state=run.state,
            stage_states=stage_states,
        )
        _print_json(
            {
                "command": args.command,
                "action": "status",
                "mode": plan.mode,
                "plan_hash": plan.plan_hash,
                "run_id": plan.run_spec.run_id,
                "status": status.model_dump(mode="json"),
                "write_performed": False,
            }
        )
        return 0
    if not args.apply:
        logger.error("daily-dag {} requires explicit --apply", args.action)
        return 2
    if args.source_spool_root is None:
        logger.error("daily-dag execution requires --source-spool-root")
        return 2
    from rquant.daily_pipeline_command_manifest import load_daily_pipeline_command_manifest
    from rquant.daily_pipeline_ledger import DailyPipelineLedger
    from rquant.daily_pipeline_orchestrator import (
        DailyCloseSpoolSourceResolver,
        DailyPipelineOrchestrator,
    )
    from rquant.live_spool import LiveBatchSpool

    if not reconfirm_development_absence():
        return 2
    manifest = load_daily_pipeline_command_manifest(
        plan.storage_profile.command_manifest_path,
        expected_storage_profile=plan.storage_profile,
    )
    if args.command_manifest_hash != manifest.manifest_hash:
        logger.error(
            "daily-dag command manifest hash is missing or does not match the reviewed file"
        )
        return 2
    expected_stage_ids = tuple(stage.stage_id for stage in plan.run_spec.stages)
    manifest_stage_ids = tuple(sorted(stage.stage_id for stage in manifest.stages))
    if manifest_stage_ids != tuple(sorted(expected_stage_ids)):
        logger.error("daily-dag command manifest does not exactly cover the immutable DAG")
        return 2
    if plan.mode == "production" and not getattr(args, "confirm_production", False):
        logger.error("daily-dag production execution requires --confirm-production")
        return 2
    if not reconfirm_development_absence():
        return 2
    ledger = DailyPipelineLedger(
        storage_profile=plan.storage_profile,
        service_owner=args.service_owner,
    )
    orchestrator = DailyPipelineOrchestrator(
        ledger=ledger,
        service_owner=args.service_owner,
        adapters=tuple(manifest.adapter_for(stage_id) for stage_id in expected_stage_ids),
        source_resolver=DailyCloseSpoolSourceResolver(
            LiveBatchSpool(args.source_spool_root, read_only=True, source_read_only=True)
        ),
        clock=lambda: datetime.now(UTC),
    )
    if args.action == "recover":
        if not reconfirm_development_absence():
            return 2
        recovery = orchestrator.recover(run_id=plan.run_spec.run_id)
        report = _publish_daily_dag_report_if_complete(
            orchestrator=orchestrator,
            run_id=plan.run_spec.run_id,
            plan_hash=plan.plan_hash,
            storage_profile=plan.storage_profile,
            expected_mode=plan.mode,
            development_test_authority=(
                None
                if development_test_report_authority is None
                else development_test_report_authority.capability
            ),
        )
        _print_json(
            {
                "command": args.command,
                "action": "recover",
                "mode": plan.mode,
                "plan_hash": plan.plan_hash,
                "run_id": plan.run_spec.run_id,
                "recovery": recovery.model_dump(mode="json"),
                "report": report,
            }
        )
        return 0
    if not reconfirm_development_absence():
        return 2
    try:
        run = orchestrator.create_run(
            mode=plan.run_spec.mode,
            trade_date=plan.run_spec.trade_date,
            source_generation_id=plan.run_spec.source_generation_id,
            source_content_hash=plan.run_spec.source_content_hash,
            command_manifest_hash=plan.run_spec.command_manifest_hash,
            code_commit=plan.run_spec.code_commit,
            profile_hash=plan.run_spec.profile_hash,
            deadline_at=plan.run_spec.deadline_at,
        )
    except (RuntimeError, ValueError) as exc:
        logger.error("daily-dag refused stale or invalid source identity: {}", exc)
        return 2
    if args.action == "retry":
        orchestrator.recover(run_id=run.run_id)
    outcome = orchestrator.advance(run.run_id)
    status = orchestrator.status(run.run_id)
    report = _publish_daily_dag_report_if_complete(
        orchestrator=orchestrator,
        run_id=run.run_id,
        plan_hash=plan.plan_hash,
        storage_profile=plan.storage_profile,
        expected_mode=plan.mode,
        development_test_authority=(
            None
            if development_test_report_authority is None
            else development_test_report_authority.capability
        ),
    )
    _print_json(
        {
            "command": args.command,
            "action": args.action,
            "mode": plan.mode,
            "plan_hash": plan.plan_hash,
            "run_id": run.run_id,
            "outcome": None if outcome is None else outcome.model_dump(mode="json"),
            "status": status.model_dump(mode="json"),
            "report": report,
        }
    )
    return 0


def _publish_daily_dag_report_if_complete(
    *,
    orchestrator: object,
    run_id: str,
    plan_hash: str,
    storage_profile: object,
    expected_mode: object,
    development_test_authority: DailyPipelineReportAuthorityCapability | None,
) -> dict[str, str] | None:
    """Publish terminal evidence through the separate monotonic CAS authority."""
    from rquant.daily_pipeline_ledger import (
        DailyPipelineMode,
        DailyPipelineStorageProfile,
        DailyRunState,
        DailyStageState,
    )
    from rquant.daily_pipeline_orchestrator import DailyPipelineOrchestrator
    from rquant.daily_pipeline_report_authority import (
        DailyPipelineReportAuthorityClient,
        DailyPipelineReportStore,
        DailyPipelineRunReport,
    )

    if not isinstance(orchestrator, DailyPipelineOrchestrator):
        raise TypeError("daily DAG report publisher requires DailyPipelineOrchestrator")
    run = orchestrator.ledger.run(run_id)
    if run.state is not DailyRunState.SUCCEEDED:
        return None
    profile = DailyPipelineStorageProfile.model_validate(storage_profile)
    required_mode = DailyPipelineMode(expected_mode)
    if run.spec.mode is not required_mode or profile.mode is not required_mode:
        raise RuntimeError("daily DAG report mode does not match the native run mode")
    if required_mode is DailyPipelineMode.PRODUCTION and (
        run.spec.mode is not DailyPipelineMode.PRODUCTION
    ):
        raise RuntimeError("production daily report requires a native production run")
    receipts = []
    for stage_id in orchestrator.definition.stage_ids:
        stage = orchestrator.ledger.stage(run_id, stage_id)
        if stage.state is not DailyStageState.SUCCEEDED or stage.terminal_receipt_id is None:
            raise RuntimeError("completed daily DAG is missing a terminal stage receipt")
        receipt = orchestrator.ledger.receipt(stage.terminal_receipt_id)
        if receipt is None:
            raise RuntimeError("completed daily DAG terminal receipt is unavailable")
        receipts.append(receipt)
    report = DailyPipelineRunReport.create(
        mode=run.spec.mode,
        profile_hash=run.spec.profile_hash,
        namespace_id=str(profile.namespace_id),
        run_id=run_id,
        plan_hash=plan_hash,
        trade_date=run.spec.trade_date,
        receipt_ids=tuple(receipt.receipt_id for receipt in receipts),
        # The last terminal receipt is immutable; using its prepare time makes
        # report publication idempotent across repeated apply/recover calls.
        generated_at=receipts[-1].prepared_at,
    )
    if development_test_authority is None:
        authority = DailyPipelineReportAuthorityClient.from_production_profile(
            code_identity=run.spec.code_commit,
            profile_identity=run.spec.profile_hash,
            mode=run.spec.mode,
            namespace_id=str(profile.namespace_id),
        )
    else:
        authority = development_test_authority
    path = DailyPipelineReportStore(
        storage_profile=profile,
        authority=authority,
    ).publish(report)
    return {"report_id": str(report.report_id), "path": str(path)}


def cmd_daily_dag_shadow(args: argparse.Namespace) -> int:
    """Read signed daily-DAG shadow evidence; this command never changes authority."""
    from rquant.daily_shadow_validation import (
        DailyRetirementGate,
        DailyRetirementGateConfig,
        DailyShadowHmacSigner,
        DailyShadowReportStore,
    )

    secret = os.environ.get(args.signing_key_env, "").encode("utf-8")
    if len(secret) < 32:
        logger.error("daily shadow signing key is missing or too short: {}", args.signing_key_env)
        return 2
    store = DailyShadowReportStore(
        Path(args.report_root),
        signer=DailyShadowHmacSigner(key_id=args.key_id, secret=secret),
        create=False,
    )
    expected = tuple(args.expected_trade_date)
    if args.calendar_path is None or args.calendar_commit is None:
        if args.action == "retirement-gate":
            logger.error(
                "daily-dag-shadow retirement-gate requires --calendar-path and --calendar-commit"
            )
            return 2
        decision = {
            "eligible": False,
            "counted_trade_dates": [],
            "reasons": ["calendar_authority_required_for_retirement"],
            "freeze_identity": None,
        }
    else:
        from rquant.runtime_market_session import load_market_calendar_authority

        decision = DailyRetirementGate(
            DailyRetirementGateConfig(minimum_real_trading_days=args.minimum_real_trading_days)
        ).evaluate(
            store,
            expected_trade_dates=expected,
            calendar=load_market_calendar_authority(
                args.calendar_path,
                expected_commit=args.calendar_commit,
            ),
        )
    reports = []
    for trade_date in expected:
        report = store.load_optional(trade_date)
        if report is None:
            reports.append({"trade_date": trade_date.isoformat(), "status": "missing"})
            continue
        reports.append(
            {
                "trade_date": trade_date.isoformat(),
                "status": "passed" if report.passed else "failed",
                "evidence_origin": report.session.evidence_origin,
                "report_id": report.report_id,
                "freeze_identity": report.session.freeze_identity,
                "discrepancy_counts": report.discrepancy_counts,
            }
        )
    _print_json(
        {
            "command": "daily-dag-shadow",
            "mode": "shadow_readonly",
            "legacy_daily_authority": "unchanged",
            "action": args.action,
            "retirement_gate": (
                decision.model_dump(mode="json") if hasattr(decision, "model_dump") else decision
            ),
            "reports": reports,
        }
    )
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    """启动盘中实时监控。"""
    from rquant.monitor import run_monitor

    setup_logging()
    return run_monitor(interval=args.interval)


def cmd_legacy_shadow_recover(args: argparse.Namespace) -> int:
    """Promote only an existing signed legacy-shadow staging batch."""
    from rquant.config import settings
    from rquant.legacy_shadow_export import recover_production_legacy_shadow_exports

    recovered = recover_production_legacy_shadow_exports(
        data_dir=settings.data_dir,
        trade_date=date.fromisoformat(args.date),
        source=args.source,
    )
    _print_json(
        {
            "command": "legacy-shadow-recover",
            "mode": "recovery_only",
            "source": args.source,
            "trade_date": args.date,
            "recovered": {key: str(value) for key, value in recovered.items()},
        }
    )
    return 0


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
    logger.info(f"rt_min 写入 minute_bar: rows={rows}, codes={len(ts_codes)}, latest={latest_time}")
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
        f"rt_min_daily 写入 minute_bar: rows={rows}, codes={len(ts_codes)}, latest={latest_time}"
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
    logger.info(f"replica: {'已刷新' if report.replica_refreshed else report.replica_detail}")
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
        logger.error("研究云增量开关未开启；设置 RESEARCH_CLOUD_INGEST_ENABLED=true 后再执行")
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


def cmd_research_repair_auction(args: argparse.Namespace) -> int:
    """Plan or atomically publish selected historical auction partitions."""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.config import settings
    from rquant.research_ingest import ResearchIngestPaths
    from rquant.research_manifest import detect_code_commit
    from rquant.research_repair import run_research_auction_repair

    if args.apply and not settings.research_cloud_ingest_enabled:
        logger.error("研究云增量开关未开启；设置 RESEARCH_CLOUD_INGEST_ENABLED=true 后再执行")
        return 3
    result = run_research_auction_repair(
        source_database=settings.duckdb_readonly_path_resolved,
        paths=ResearchIngestPaths(
            state_dir=settings.data_dir,
            catalog_path=settings.research_db_path_resolved,
            readonly_catalog_path=settings.research_readonly_db_path_resolved,
            lake_root=settings.research_lake_dir_resolved,
            staging_root=settings.research_staging_dir_resolved,
        ),
        trade_dates=tuple(args.date),
        adapter=TushareAdapter(),
        code_commit=detect_code_commit() or "unknown",
        apply=args.apply,
        plan_id=args.plan_id,
    )
    payload = result.model_dump(mode="json")
    payload["plan_id"] = result.plan_id
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_research_repair_minute(args: argparse.Namespace) -> int:
    """Plan or atomically publish a manifest-bound minute-lake repair."""
    from rquant.config import settings
    from rquant.research_manifest import detect_code_commit
    from rquant.research_minute_repair import (
        ResearchIngestPaths,
        run_research_minute_repair,
    )

    if args.apply and not settings.research_cloud_ingest_enabled:
        logger.error("研究云增量开关未开启；设置 RESEARCH_CLOUD_INGEST_ENABLED=true 后再执行")
        return 3
    with open_backfill_state_snapshot(
        settings.backfill_state_path_resolved,
        busy_timeout_ms=settings.backfill_state_busy_timeout_ms,
    ) as state:
        result = run_research_minute_repair(
            source_database=settings.duckdb_readonly_path_resolved,
            primary_database=settings.duckdb_path,
            paths=ResearchIngestPaths(
                state_dir=settings.data_dir,
                catalog_path=settings.research_db_path_resolved,
                readonly_catalog_path=settings.research_readonly_db_path_resolved,
                lake_root=settings.research_lake_dir_resolved,
                staging_root=settings.research_staging_dir_resolved,
            ),
            state=state,
            manifest_id=args.manifest_id,
            code_commit=detect_code_commit() or "unknown",
            apply=args.apply,
            plan_id=args.plan_id,
        )
    payload = result.model_dump(mode="json")
    payload["plan_id"] = result.plan_id
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_formal_smoke_replay(args: argparse.Namespace) -> int:
    """Run one fixed strategy spec through the exact formal research binding."""
    setup_logging(enqueue=False)
    from rquant.formal_runtime_composition import (
        FormalRuntimeCompositionError,
        open_formal_runtime_capability,
    )
    from rquant.formal_smoke_execution import (
        FormalSmokeExecutionError,
        run_attested_formal_smoke,
    )
    from rquant.formal_smoke_protocol import FormalSmokeBootstrapReference

    try:
        capability = open_formal_runtime_capability(
            configuration_path=args.runtime_code_config,
            trusted_base=args.runtime_code_trusted_base,
            expected_authority_uid=args.runtime_code_authority_uid,
            expected_authority_gid=args.runtime_code_authority_gid,
            startup_deadline_monotonic=time.monotonic() + 30,
        )
        capability.require_live()
    except (FormalRuntimeCompositionError, RuntimeError) as exc:
        logger.error(f"formal smoke replay runtime capability is unavailable: {exc}")
        return 2
    try:
        if args.output_dir is None:
            from rquant.config import settings

            output_dir = settings.data_dir
        else:
            output_dir = args.output_dir
        result = run_attested_formal_smoke(
            capability,
            strategy=args.strategy,
            start_date=args.start_date,
            end_date=args.end_date,
            audit_run_id=args.audit_run_id,
            dataset_snapshot_id=args.snapshot_id,
            dataset_binding_hash=args.binding_hash,
            output_dir=output_dir,
            bootstrap_reference=FormalSmokeBootstrapReference(
                configuration_path=args.runtime_code_config,
                trusted_base=args.runtime_code_trusted_base,
                expected_authority_uid=args.runtime_code_authority_uid,
                expected_authority_gid=args.runtime_code_authority_gid,
            ),
            environment_source=os.environ,
            execution_deadline_monotonic=(time.monotonic() + args.execution_timeout_seconds),
        )
        _print_json(result.model_dump(mode="json"))
        return 0
    except FormalSmokeExecutionError as exc:
        logger.error(f"formal smoke replay attested execution failed: {exc}")
        return 2
    finally:
        capability.close()


def cmd_formal_smoke_runtime_execute(args: argparse.Namespace) -> int:
    """Run the generation-only side of the private FD protocol."""
    from rquant.formal_smoke_runtime_entry import (
        FormalSmokeGenerationEntryError,
        run_formal_smoke_generation_entry,
    )

    try:
        return run_formal_smoke_generation_entry(
            request_fd=args.request_fd,
            receipt_fd=args.receipt_fd,
        )
    except (FormalSmokeGenerationEntryError, OSError, RuntimeError, ValueError) as exc:
        logger.error(f"formal smoke generation entry failed: {exc}")
        return 70


def cmd_stage1_acceptance(args: argparse.Namespace) -> int:
    """Build one no-write, manifest-bound Stage 1 acceptance plan."""
    from pydantic import ValidationError

    from rquant.research_manifest import detect_verified_code_commit
    from rquant.stage1_acceptance import (
        Stage1AcceptanceIdentityError,
        Stage1AcceptanceSpec,
        build_stage1_acceptance_plan,
    )

    setup_logging()
    code_commit = detect_verified_code_commit()
    if not _valid_clean_commit(code_commit):
        logger.error("Stage 1 acceptance requires a clean 40-character git commit")
        return 2
    try:
        spec = Stage1AcceptanceSpec(
            strategy=args.strategy,
            manifest_id=args.manifest_id,
            start_date=args.start_date,
            end_date=args.end_date,
            expected_code_commit=args.expected_code_commit,
        )
        with open_backfill_state_snapshot() as state:
            plan = build_stage1_acceptance_plan(
                state,
                spec,
                observed_code_commit=code_commit,
                now=datetime.now(_SHANGHAI),
            )
    except (
        ManifestContentConflictError,
        Stage1AcceptanceIdentityError,
        UnknownManifestError,
        ValidationError,
        ValueError,
    ) as exc:
        logger.error(f"cannot build Stage 1 acceptance plan: {exc}")
        return 2
    _print_json(plan.model_dump(mode="json"))
    return 1 if plan.disposition == "blocked" else 0


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
    if not args.dry_run and not args.skip_state_recompute and affected_codes:
        tail_start_value = summary.get("state_tail_start_date")
        if not isinstance(tail_start_value, str):
            raise RuntimeError("market backfill result is missing state_tail_start_date")
        with DuckDBStore() as store:
            recompute_daily_state(
                store,
                codes=affected_codes,
                start_date=date.fromisoformat(tail_start_value),
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
        summary = backfill_limit_list(args.start_date, args.end_date, store, dry_run=args.dry_run)
    logger.info(summary)
    return 1 if summary["failed_dates"] else 0


def cmd_data_backfill(args: argparse.Namespace) -> int:
    """统一数据集回补（dataset_backfill 注册表），--dataset all 跑全部。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.dataset_backfill import DATASETS, backfill_dataset

    setup_logging()
    if args.dataset != "all" and args.dataset not in DATASETS:
        logger.error(f"未知数据集：{args.dataset}（可用：all, {', '.join(sorted(DATASETS))}）")
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
            summary = backfill_dataset(name, start, end, store, adapter, dry_run=args.dry_run)
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


def _configure_backfill_planner_resources(
    store: DuckDBStore,
    *,
    memory_limit_mb: int,
    threads: int,
    spill_directory: Path,
) -> None:
    """Bound one planner connection without changing global DuckDB defaults."""
    spill_directory.mkdir(parents=True, exist_ok=True)
    store._conn.execute("SET memory_limit = ?", [f"{memory_limit_mb}MB"])
    store._conn.execute("SET threads = ?", [threads])
    store._conn.execute("SET temp_directory = ?", [str(spill_directory)])


def cmd_backfill_plan(args: argparse.Namespace) -> int:
    """Resolve PIT eligibility, plan exact minute coverage, and persist it."""
    from rquant.backfill_manifest import (
        STRATEGY_BACKFILL_SPECS,
        BackfillCalendarError,
        BackfillManifest,
        backfill_state_input,
        latest_observable_eligibility_date,
        plan_minute_backfill,
        resolve_requested_eligibility_end,
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
    with (
        TemporaryDirectory(prefix="rquant-backfill-plan-") as spill_directory,
        open_readonly_store() as store,
    ):
        _configure_backfill_planner_resources(
            store,
            memory_limit_mb=settings.backfill_planner_memory_limit_mb,
            threads=settings.backfill_planner_threads,
            spill_directory=Path(spill_directory),
        )
        try:
            observable_end = latest_observable_eligibility_date(
                store,
                spec=spec,
                as_of_time=as_of_time,
            )
            end_date = resolve_requested_eligibility_end(
                requested_end=args.end_date,
                observable_end=observable_end,
                start_date=args.start_date,
            )
        except BackfillCalendarError as exc:
            logger.error(str(exc))
            return 2
        if args.strategy == "auction_gap":
            eligibility_artifacts = SnapshotArtifactResolver(
                catalog=catalog,
                lake_root=settings.research_lake_dir_resolved,
            ).resolve_lake_partitions(
                dataset="auction_bar",
                start_date=args.start_date,
                end_date=end_date,
                as_of_time=as_of_time,
            )
            if not eligibility_artifacts:
                logger.error(
                    "auction eligibility requires immutable auction_bar research-lake partitions"
                )
                return 2
            eligibility_resolution = resolve_strategy_eligibility_from_artifacts(
                store,
                strategy_id=args.strategy,
                start_date=args.start_date,
                end_date=end_date,
                input_artifacts=eligibility_artifacts,
                lake_root=settings.research_lake_dir_resolved,
                as_of_time=as_of_time,
            )
        else:
            eligibility_resolution = resolve_strategy_eligibility(
                store,
                strategy_id=args.strategy,
                start_date=args.start_date,
                end_date=end_date,
            )
        manifest = BackfillManifest.build(
            spec=spec,
            start_date=args.start_date,
            end_date=end_date,
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
            "observable_end_date": observable_end.isoformat(),
            "effective_end_date": end_date.isoformat(),
            "eligibility_count": len(plan.manifest.eligibilities),
            "eligibility_resolution_hash": (eligibility_resolution.resolution_hash),
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
    from rquant.backfill_manifest import (
        BackfillCalendarError,
        BackfillPlanIntegrityError,
        MinuteBackfillPlan,
        validate_executable_backfill_plan,
        validate_persisted_backfill_tasks,
    )
    from rquant.intraday_backfill import run_backfill_manifest_workers

    setup_logging()
    state = BackfillStateStore()
    try:
        persisted = state.load_manifest(args.manifest_id)
    except ManifestContentConflictError as exc:
        logger.error(f"persisted backfill state failed integrity check: {exc}")
        return 2
    if persisted is None:
        logger.error(f"unknown backfill manifest: {args.manifest_id}")
        return 2
    status_before = state.get_manifest_status(args.manifest_id)
    if status_before.status == "completed":
        _print_json(status_before.model_dump(mode="json"))
        return 0
    if status_before.status == "abandoned":
        _print_json(status_before.model_dump(mode="json"))
        return 2
    if status_before.status == "failed" and not args.retry_failed:
        logger.error("manifest has failed tasks; pass --retry-failed after inspection")
        return 2

    plan = MinuteBackfillPlan.model_validate(persisted.payload)
    try:
        validate_persisted_backfill_tasks(persisted, plan)
        with open_readonly_store() as store:
            validate_executable_backfill_plan(store, plan)
    except (BackfillCalendarError, BackfillPlanIntegrityError) as exc:
        logger.error(f"persisted backfill plan is not executable: {exc}")
        return 2
    worker_count = int(getattr(args, "workers", 8))
    first_task = plan.tasks[0]
    telemetry = state.get_workload_telemetry(
        args.manifest_id,
        source=first_task.source,
        freq=first_task.freq,
        response_row_limit=first_task.response_row_limit,
    )
    execution_estimate = _estimate_parallel_backfill(
        static_total_seconds=plan.estimate.total_seconds,
        static_rate_limit_seconds=plan.estimate.rate_limit_seconds,
        total_tasks=len(plan.tasks),
        worker_count=worker_count,
        telemetry=telemetry,
    )
    now = _backfill_now()
    max_runtime_minutes = getattr(args, "max_runtime_minutes", None)
    explicit_stop_before = getattr(args, "stop_before", None)
    if explicit_stop_before is None:
        try:
            stop_before = _resolve_backfill_stop_before(
                now,
                max_runtime_minutes=max_runtime_minutes,
            )
        except ValueError as exc:
            logger.error(str(exc))
            return 2
    else:
        stop_before = explicit_stop_before.astimezone(_SHANGHAI)
        if _in_backfill_protected_window(now) or now >= stop_before:
            logger.error("backfill worker deadline already elapsed")
            return 2
    if (
        max_runtime_minutes is None
        and explicit_stop_before is None
        and not _backfill_write_window_safe(
            now,
            execution_estimate.point_seconds,
        )
    ):
        logger.error("backfill run would overlap the protected 09:15-15:10 monitor window")
        return 2

    logger.info(
        "backfill execution estimate: "
        f"workers={worker_count}, source={execution_estimate.source}, "
        f"point={execution_estimate.point_seconds:.1f}s, "
        f"guard={execution_estimate.guard_seconds:.1f}s, "
        f"stop_before={stop_before.isoformat()}"
    )
    deadline_worker = bool(getattr(args, "deadline_worker", True))
    if not deadline_worker:
        return _run_backfill_supervised_worker(
            args,
            stop_before=stop_before,
            hard_deadline=_resolve_backfill_hard_deadline(stop_before),
        )
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    summary = run_backfill_manifest_workers(
        state,
        TushareAdapter,
        manifest_id=args.manifest_id,
        worker_id=worker_id,
        worker_count=worker_count,
        retry_failed=args.retry_failed,
        stop_before=stop_before,
        store_factory=DuckDBStore,
    )
    status_after = state.get_manifest_status(args.manifest_id)
    _print_json(
        {
            "run": summary.model_dump(mode="json"),
            "status": status_after.model_dump(mode="json"),
            "estimate": {
                "serial_seconds": execution_estimate.serial_seconds,
                "point_seconds": execution_estimate.point_seconds,
                "guard_seconds": execution_estimate.guard_seconds,
                "source": execution_estimate.source,
                "stop_before": stop_before.isoformat(),
            },
        }
    )
    return 0 if status_after.status == "completed" else 1


def cmd_backfill_abandon(args: argparse.Namespace) -> int:
    """Plan or apply an auditable terminal state for a retired manifest."""
    from rquant.research_manifest import detect_code_commit

    setup_logging()
    code_commit = detect_code_commit()
    if not _valid_clean_commit(code_commit):
        logger.error("manifest abandonment requires a clean 40-character git commit")
        return 2
    try:
        if args.apply:
            state = BackfillStateStore()
            plan = state.plan_manifest_abandonment(
                args.manifest_id,
                reason=args.reason,
                code_commit=code_commit,
            )
        else:
            with open_backfill_state_snapshot() as state:
                plan = state.plan_manifest_abandonment(
                    args.manifest_id,
                    reason=args.reason,
                    code_commit=code_commit,
                )
    except (
        ManifestAbandonmentConflictError,
        UnknownManifestError,
        ValueError,
    ) as exc:
        logger.error(f"cannot plan manifest abandonment: {exc}")
        return 2

    if not args.apply:
        _print_json(
            {
                "status": "dry_run",
                "apply_required": True,
                "plan": plan.model_dump(mode="json"),
            }
        )
        return 0
    if args.plan_id is None:
        logger.error("--apply requires the exact --plan-id from dry-run")
        return 2
    if args.plan_id != plan.plan_id:
        logger.error("manifest abandonment plan-id mismatch; rerun dry-run and inspect changes")
        return 2
    try:
        status = state.apply_manifest_abandonment(plan)
    except (
        ManifestAbandonmentConflictError,
        StaleManifestAbandonmentError,
        UnknownManifestError,
    ) as exc:
        logger.error(f"cannot apply manifest abandonment: {exc}")
        return 2
    _print_json(
        {
            "status": "abandoned",
            "plan": plan.model_dump(mode="json"),
            "manifest": status.model_dump(mode="json"),
        }
    )
    return 0


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
    from rquant.suspension import (
        backfill_suspension_facts,
        plan_suspension_backfill,
    )

    setup_logging()
    if args.dry_run:
        plan = plan_suspension_backfill(
            store_factory=open_readonly_store,
            start=args.start_date,
            end=args.end_date,
            missing_only=not args.full_refresh,
        )
        _print_json(plan.model_dump(mode="json"))
        return 0
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
        payload["total_logical_api_operations"] = plan.total_logical_api_operations
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
        logger.error(f"manifest must be completed before snapshot finalization: {status.status}")
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
        logger.error("dataset snapshot apply would overlap the protected 09:15-15:10 market window")
        return 2
    deadline = None if dry_run else _dataset_snapshot_apply_deadline(started_at)
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
                "dataset snapshot requires an independently verified eligibility resolution"
            )
            return 2
        if args.strategy == "auction_gap" and not planned_resolution.input_artifacts:
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
            logger.error("dataset coverage gate failed: baseline must be >=95% and B/S >=99%")
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
        ts_codes = tuple(sorted({row.ts_code for row in current.manifest.eligibilities}))
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
                logger.error(f"research lake has no {dataset} partitions in binding range")
                return 2
            if dataset == "auction_bar" and resolution.input_artifacts:
                by_key = {artifact.artifact_key: artifact for artifact in resolved}
                by_key.update(
                    {artifact.artifact_key: artifact for artifact in resolution.input_artifacts}
                )
                resolved = tuple(by_key[key] for key in sorted(by_key))
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
                        dependency.table_name for dependency in dependencies.materialized_tables
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
                    missing_reasons=tuple(sorted({row.reason for row in resolution.incomplete})),
                )
            )
            phases = {
                "baseline": coverage.baseline,
                "entry": coverage.entry,
                "exit": coverage.exit,
            }
            accepted_missing_reasons = tuple(
                sorted({row.reason for row in current.unavailable_sessions})
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
                "SELECT MAX(cal_date) FROM trade_calendar WHERE exchange = 'SSE' AND cal_date <= ?",
                [as_of_shanghai.date()],
            ).fetchone()
            watermarks: dict[str, str] = {}
            watermarks["manifest_start_date"] = current.manifest.start_date.isoformat()
            watermarks["manifest_end_date"] = current.manifest.end_date.isoformat()
            watermarks["eligibility_resolution_hash"] = resolution.resolution_hash
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
            "dataset snapshot apply stopped before the protected 09:15-15:10 market window"
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
            previously_open = writable.list_open_data_quality_issues(rule_ids=report.rule_ids)
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
                    p0_count=sum(1 for finding in report.findings if finding.severity == "P0"),
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
        "signal_date",
        "ts_code",
        "name",
        "entry_price",
        "auction_vol_ratio_5d",
        "gap_pct_close",
        "gap_pct_high",
        "hit_limit_up_today",
        "intraday_high_ret_pct",
        "next_trade_date",
        "next_open_ret_pct",
        "next_close_ret_pct",
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
            logger.warning("盘中时段 persist 落库会与本地 monitor 抢写锁，建议收盘后执行")
        with DuckDBStore() as store:
            candidates = run_auction_gap_replay(store, config.auction_config())
            trades = run_auction_gap_minute_replay(
                store,
                config,
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
        "signal_date",
        "ts_code",
        "name",
        "auction_price",
        "entry_time",
        "entry_price",
        "b_first_limit_up_time",
        "b_close_at_limit_up",
        "hold_policy",
        "exit_time",
        "exit_price",
        "exit_reason",
        "ret_pct",
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
        "signal_date",
        "ts_code",
        "name",
        "entry_time",
        "entry_price_raw",
        "entry_price",
        "exit_time",
        "exit_price",
        "exit_reason",
        "holding_trading_days",
        "ret_pct",
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
        max_inner_outer_ratio=args.max_inner_outer_ratio,
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
        "signal_date",
        "ts_code",
        "name",
        "board_type",
        "entry_time",
        "entry_price",
        "limit_up_price",
        "hit_limit_up_today",
        "exit_time",
        "exit_price",
        "exit_reason",
        "ret_pct",
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
    from rquant.preflight import CheckResult, format_pushdeer_summary, format_report, run_all_checks

    setup_logging()
    recovery_config = None
    recovery_failure: CheckResult | None = None
    runtime_root = getattr(args, "runtime_root", None)
    if runtime_root is not None:
        try:
            from rquant.runtime_deployment_profile import (
                build_runtime_recovery_preflight_config,
                load_current_runtime_deployment_profile,
            )

            runtime_profile = load_current_runtime_deployment_profile(Path(runtime_root))
            recovery_config = build_runtime_recovery_preflight_config(runtime_profile)
        except Exception as exc:
            recovery_failure = CheckResult(
                "runtime_recovery",
                "fail",
                f"recovery production profile 验证失败: {type(exc).__name__}",
            )
    results = run_all_checks(
        freshness_profile=args.profile,
        recovery_config=recovery_config,
        runtime_root=None if runtime_root is None else Path(runtime_root),
    )
    if recovery_failure is not None:
        results.append(recovery_failure)
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


def _serve_closed_unix_authority(service: object, *, label: str) -> int:
    stop = threading.Event()

    def handle_signal(signum: int, frame: object) -> None:
        del frame
        logger.info(f"{label} 收到信号 {signum}，请求停止")
        stop.set()
        service.wake()

    previous = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    for signum in previous:
        signal.signal(signum, handle_signal)
    try:
        service.serve_forever(stop=stop)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    return 0


def cmd_external_monotonic_root_serve(args: argparse.Namespace) -> int:
    """Run the closed persistent external monotonic-root authority."""
    from rquant.resource_authority_service import (
        EXTERNAL_ROOT_ENVIRONMENT_KEYS,
        EXTERNAL_ROOT_ENVIRONMENT_PATH,
        ResourceAuthorityServiceError,
        compose_external_monotonic_root_daemon,
        load_closed_authority_environment,
        load_external_monotonic_root_daemon_configuration,
    )

    environment_path = EXTERNAL_ROOT_ENVIRONMENT_PATH
    environment = load_closed_authority_environment(
        environment_path,
        allowed_keys=EXTERNAL_ROOT_ENVIRONMENT_KEYS,
        required_keys=EXTERNAL_ROOT_ENVIRONMENT_KEYS,
        expected_uid=0,
        expected_gid=0,
    )
    path = Path(args.config)
    if environment["APP_ENV"] != "prod" or path != Path(
        environment["RQUANT_EXTERNAL_MONOTONIC_ROOT_SERVICE_CONFIG_PATH"]
    ):
        raise ResourceAuthorityServiceError(
            "external monotonic root CLI requires the configured production manifest"
        )
    service = compose_external_monotonic_root_daemon(
        load_external_monotonic_root_daemon_configuration(
            path,
            expected_uid=0,
            expected_gid=0,
        )
    )
    return _serve_closed_unix_authority(service, label="external-monotonic-root")


def cmd_resource_authority_serve(args: argparse.Namespace) -> int:
    """Run the closed resource journal authority backed by the external root."""
    from rquant.lab_resource_authority_adapter import parse_resource_authority_adapter_config
    from rquant.resource_authority_service import (
        RESOURCE_AUTHORITY_ENVIRONMENT_KEYS,
        RESOURCE_AUTHORITY_ENVIRONMENT_PATH,
        ResourceAuthorityServiceError,
        compose_resource_authority_daemon,
        load_closed_authority_environment,
        load_resource_authority_daemon_configuration,
    )
    from rquant.runtime_resource_admission import admission_policy_for_version

    environment = load_closed_authority_environment(
        RESOURCE_AUTHORITY_ENVIRONMENT_PATH,
        allowed_keys=RESOURCE_AUTHORITY_ENVIRONMENT_KEYS,
        required_keys=RESOURCE_AUTHORITY_ENVIRONMENT_KEYS,
        expected_uid=0,
        expected_gid=0,
    )
    path = Path(args.config)
    code_sha = environment["RQUANT_CODE_COMMIT"].strip().lower()
    if (
        environment["APP_ENV"] != "prod"
        or path != Path(environment["RQUANT_RESOURCE_AUTHORITY_SERVICE_CONFIG_PATH"])
        or (args.code_sha is not None and args.code_sha.strip().lower() != code_sha)
        or len(code_sha) != 40
        or any(character not in "0123456789abcdef" for character in code_sha)
    ):
        raise ResourceAuthorityServiceError(
            "resource authority CLI requires the configured production identity"
        )
    configuration = load_resource_authority_daemon_configuration(
        path,
        expected_uid=0,
        expected_gid=0,
    )
    worker_adapter = parse_resource_authority_adapter_config(
        environment["RQUANT_LAB_RESOURCE_AUTHORITY_CONFIG_JSON"]
    )
    if worker_adapter != configuration.service_configuration.adapter_configuration:
        raise ResourceAuthorityServiceError(
            "resource authority service and worker manifests conflict"
        )
    authority_settings = SimpleNamespace(
        app_env="prod",
        lab_worker_artifact_dir_resolved=Path(environment["RQUANT_RESOURCE_AUTHORITY_STATE_DIR"]),
        rquant_lab_live_slo_authority_root=Path(environment["RQUANT_LAB_LIVE_SLO_AUTHORITY_ROOT"]),
        rquant_lab_trade_calendar_path=Path(environment["RQUANT_LAB_TRADE_CALENDAR_PATH"]),
        rquant_lab_resource_policy_version=environment["RQUANT_LAB_RESOURCE_POLICY_VERSION"],
    )
    admission = _build_lab_worker_resource_admission(
        settings=authority_settings,
        code_sha=code_sha,
        legacy_opt_out=False,
    )
    snapshot_provider = admission.resource_snapshot_provider
    if not admission.require_resource_admission or snapshot_provider is None:
        raise ResourceAuthorityServiceError(
            "resource authority runtime snapshot provider is unavailable"
        )
    policy = admission_policy_for_version(authority_settings.rquant_lab_resource_policy_version)
    service = compose_resource_authority_daemon(
        configuration=configuration,
        policy_provider=lambda: policy,
        snapshot_provider=snapshot_provider,
    )
    return _serve_closed_unix_authority(service, label="resource-authority")


def cmd_runtime_deployment_profile(args: argparse.Namespace) -> int:
    """Preview or atomically install one immutable isolated-runtime profile."""
    from rquant.runtime_deployment_profile import (
        install_runtime_deployment_profile,
        load_runtime_deployment_profile,
        load_runtime_schema_v1_migration_authorization,
        preview_runtime_deployment_profile,
    )

    profile = load_runtime_deployment_profile(
        Path(args.profile),
        expected_commit=str(args.expected_commit),
    )
    if not args.apply:
        preview = preview_runtime_deployment_profile(
            profile,
            runtime_root=Path(args.runtime_root),
            environ=os.environ,
            schema_bootstrap_reason=args.schema_bootstrap_reason,
        )
        print(preview.model_dump_json(indent=2))
        return 0
    if args.profile_id != profile.profile_id:
        logger.error("运行时画像已变化；请重新 dry-run 并核对新的 profile id")
        return 2
    migration_path = getattr(args, "schema_v1_migration_authority", None)
    if profile.schema_v1_migration_authority is not None:
        if migration_path is None:
            logger.error("首次 schema v1 migration 必须显式传入审核授权文件")
            return 2
        explicit_authority = load_runtime_schema_v1_migration_authorization(Path(migration_path))
        if explicit_authority != profile.schema_v1_migration_authority:
            logger.error("显式 schema v1 migration 授权与 hash-bound profile 不一致")
            return 2
    elif migration_path is not None:
        logger.error("当前 profile 不包含 schema v1 migration，拒绝无关授权文件")
        return 2
    receipt = install_runtime_deployment_profile(
        profile,
        runtime_root=Path(args.runtime_root),
        environ=os.environ,
        schema_bootstrap_reason=args.schema_bootstrap_reason,
    )
    print(receipt.model_dump_json(indent=2))
    return 0


def cmd_runtime_production_profile(args: argparse.Namespace) -> int:
    """Preview or publish one immutable production profile from canonical inputs."""
    from rquant.runtime_production_profile import (
        build_production_runtime_profile,
        load_production_runtime_profile_inputs,
        publish_production_runtime_profile,
    )

    inputs = load_production_runtime_profile_inputs(
        Path(args.inputs),
        expected_commit=str(args.expected_commit),
        expected_runtime_mode=str(getattr(args, "runtime_mode", "local-test")),
    )
    profile = build_production_runtime_profile(inputs)
    if profile.profile_id is None:  # pragma: no cover - profile model invariant
        raise ValueError("production runtime profile id is missing")
    output = Path(args.output_dir) / f"{profile.profile_id}.json"
    apply_requested = bool(getattr(args, "apply", False))
    expected_profile_id = getattr(args, "profile_id", None)
    if apply_requested:
        if expected_profile_id != profile.profile_id:
            raise ValueError("production runtime profile changed after preview")
        published = publish_production_runtime_profile(
            profile,
            output,
            production_runtime_root=inputs.runtime_root,
        )
    else:
        if expected_profile_id is not None:
            raise ValueError("production runtime profile id requires apply")
        published = output
    print(
        json.dumps(
            {
                "producer_commit": profile.producer_commit,
                "profile_id": profile.profile_id,
                "profile_path": str(published),
                "runtime_root": str(inputs.runtime_root),
                "service_count": len(profile.manifests),
                "status": "published" if apply_requested else "dry_run",
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def cmd_runtime_production_prerequisites(args: argparse.Namespace) -> int:
    """Preview or install immutable authorities required by a production profile."""
    from rquant.runtime_market_calendar_generation import market_calendar_generation_path
    from rquant.runtime_production_profile import (
        build_production_runtime_profile,
        install_production_runtime_prerequisites,
        load_production_runtime_profile_inputs,
    )

    inputs = load_production_runtime_profile_inputs(
        Path(args.inputs),
        expected_commit=str(args.expected_commit),
        expected_runtime_mode=str(getattr(args, "runtime_mode", "local-test")),
    )
    profile = build_production_runtime_profile(inputs)
    if profile.profile_id is None:  # pragma: no cover - profile model invariant
        raise ValueError("production runtime profile id is missing")
    target = market_calendar_generation_path(
        inputs.runtime_root,
        inputs.market_calendar_content_sha256,
    )
    retention_manifests = tuple(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind.value == "artifact_retention"
    )
    if len(retention_manifests) != 1:
        raise ValueError("production profile must contain exactly one retention owner")
    retention_catalog_receipt = (
        Path(str(retention_manifests[0].settings["catalog_authority_root"])) / "current.json"
    )
    targets = (target, inputs.definition_registry_root, retention_catalog_receipt)
    if not args.apply:
        print(
            json.dumps(
                {
                    "profile_id": profile.profile_id,
                    "status": "dry_run",
                    "targets": [str(path) for path in targets],
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.profile_id != profile.profile_id:
        logger.error("生产画像已变化；请重新 dry-run 并核对新的 profile id")
        return 2
    installed = install_production_runtime_prerequisites(inputs)
    print(
        json.dumps(
            {
                "profile_id": profile.profile_id,
                "status": "applied",
                "targets": [str(path) for path in installed],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def cmd_runtime_deployment_rollout(args: argparse.Namespace) -> int:
    """Roll out one installed runtime generation through audited systemd health gates."""
    from rquant.runtime_deployment_bundle import (
        activate_runtime_deployment_generation,
        load_current_runtime_deployment_receipt,
        load_runtime_deployment_generation_receipt,
    )
    from rquant.runtime_deployment_rollout import (
        SystemdRuntimeRolloutController,
        build_runtime_generation_health_probe,
        rollout_runtime_deployment,
    )

    runtime_root = Path(args.runtime_root)
    current = load_current_runtime_deployment_receipt(
        runtime_root,
        expected_commit=str(args.expected_commit),
        expected_profile_id=str(args.profile_id),
    )
    if current.generation_hash != args.generation_hash:
        logger.error("运行时 current generation 与请求不一致")
        return 2
    receipt = current.model_copy(update={"previous_generation_hash": args.previous_generation_hash})

    def load_previous(generation_hash: str) -> RuntimeDeploymentReceipt | None:
        previous = load_runtime_deployment_generation_receipt(
            runtime_root,
            generation_hash=generation_hash,
        )
        return previous if previous.producer_commit == current.producer_commit else None

    def activate_previous(previous: RuntimeDeploymentReceipt) -> object:
        if previous.deployment_profile_id is None:
            raise ValueError("previous runtime generation lacks a profile identity")
        return activate_runtime_deployment_generation(
            runtime_root,
            generation_hash=previous.generation_hash,
            expected_commit=previous.producer_commit,
            expected_profile_id=previous.deployment_profile_id,
        )

    audit = rollout_runtime_deployment(
        receipt,
        controller=SystemdRuntimeRolloutController(
            health_probe=build_runtime_generation_health_probe()
        ),
        current_receipt_loader=lambda: load_current_runtime_deployment_receipt(
            runtime_root,
            expected_commit=current.producer_commit,
            expected_profile_id=str(current.deployment_profile_id),
        ),
        previous_receipt_loader=load_previous,
        previous_generation_activator=activate_previous,
        audit_root=(
            Path(args.audit_root)
            if args.audit_root is not None
            else runtime_root / "control" / "deployment-rollouts"
        ),
        health_timeout_seconds=float(args.health_timeout_seconds),
    )
    print(audit.model_dump_json(indent=2))
    return 0 if audit.status == "succeeded" else 2


def cmd_runtime_deployment_rollback(args: argparse.Namespace) -> int:
    """Restore the exact previous runtime generation before code rollback."""

    from rquant.runtime_deployment_bundle import (
        activate_runtime_deployment_generation,
        load_current_runtime_deployment_receipt_unbound,
        load_runtime_deployment_generation_receipt,
    )
    from rquant.runtime_deployment_rollout import (
        SystemdRuntimeRolloutController,
        build_runtime_generation_health_probe,
        rollback_runtime_deployment,
    )

    runtime_root = Path(args.runtime_root)
    current = load_current_runtime_deployment_receipt_unbound(runtime_root)
    if current.producer_commit == args.expected_previous_commit:
        print(
            json.dumps(
                {
                    "status": "already_rolled_back",
                    "generation_hash": current.generation_hash,
                    "producer_commit": current.producer_commit,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    if current.producer_commit != args.failed_commit:
        logger.error("runtime current commit 既不是失败版本也不是预期回退版本")
        return 2
    if current.previous_generation_hash is None:
        logger.error("runtime current generation 没有 previous generation")
        return 2

    def load_previous(generation_hash: str) -> RuntimeDeploymentReceipt | None:
        previous = load_runtime_deployment_generation_receipt(
            runtime_root,
            generation_hash=generation_hash,
        )
        return previous if previous.producer_commit == args.expected_previous_commit else None

    def activate_previous(previous: RuntimeDeploymentReceipt) -> object:
        if previous.deployment_profile_id is None:
            raise ValueError("previous runtime generation lacks a profile identity")
        return activate_runtime_deployment_generation(
            runtime_root,
            generation_hash=previous.generation_hash,
            expected_commit=previous.producer_commit,
            expected_profile_id=previous.deployment_profile_id,
        )

    audit = rollback_runtime_deployment(
        current,
        operation_id=str(args.operation_id),
        controller=SystemdRuntimeRolloutController(
            health_probe=build_runtime_generation_health_probe()
        ),
        current_receipt_loader=lambda: load_current_runtime_deployment_receipt_unbound(
            runtime_root
        ),
        previous_receipt_loader=load_previous,
        previous_generation_activator=activate_previous,
        audit_root=(
            Path(args.audit_root)
            if args.audit_root is not None
            else runtime_root / "control" / "deployment-rollbacks"
        ),
        health_timeout_seconds=float(args.health_timeout_seconds),
    )
    print(audit.model_dump_json(indent=2))
    return 0


def cmd_runtime_schema_retirement(args: argparse.Namespace) -> int:
    """Inspect or explicitly retire one post-cutover schema plan."""

    from rquant.runtime_deployment_bundle import load_current_runtime_deployment_receipt
    from rquant.runtime_deployment_rollout import (
        load_runtime_deployment_rollout_audit,
        preview_runtime_schema_retirement,
        retire_runtime_schema_plan,
    )

    runtime_root = Path(args.runtime_root)
    receipt = load_current_runtime_deployment_receipt(
        runtime_root,
        expected_commit=str(args.expected_commit),
        expected_profile_id=str(args.profile_id),
    )
    if receipt.generation_hash != args.generation_hash:
        logger.error("schema retirement generation 与 current runtime 不一致")
        return 2
    audit_root = (
        Path(args.audit_root)
        if args.audit_root is not None
        else runtime_root / "control" / "deployment-rollouts"
    )
    audit = load_runtime_deployment_rollout_audit(
        audit_root,
        operation_id=str(args.rollout_operation_id),
    )
    now = datetime.now(UTC)
    statuses = preview_runtime_schema_retirement(
        receipt,
        rollout_audit=audit,
        now=now,
    )
    if args.retirement_action == "status":
        print(
            json.dumps(
                {
                    "status": "ready",
                    "observed_at": now.isoformat(),
                    "plans": [item.model_dump(mode="json") for item in statuses],
                },
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    selected = next((item for item in statuses if item.plan_id == args.plan_id), None)
    if selected is None:
        logger.error("schema retirement plan 不属于当前 rollout")
        return 2
    if args.retirement_action == "dry-run":
        print(
            json.dumps(
                {
                    "status": "eligible" if selected.eligible else "waiting",
                    "observed_at": now.isoformat(),
                    "plan": selected.model_dump(mode="json"),
                },
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    retired = retire_runtime_schema_plan(
        receipt,
        rollout_audit=audit,
        plan_id=str(args.plan_id),
        now=now,
        operation_id=str(args.operation_id),
    )
    print(retired.model_dump_json(indent=2))
    return 0


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


def cmd_runtime_recovery_backup(args: argparse.Namespace) -> int:
    """Produce or inspect one signed, consistent recovery backup generation."""

    from rquant.runtime_recovery_backup import (
        RecoveryBackupAuthenticator,
        RecoveryBackupProducer,
        load_recovery_backup_config,
        load_recovery_backup_generation,
        recovery_backup_trusted_verifiers_for_active,
    )

    config = load_recovery_backup_config(args.config)
    authenticator = RecoveryBackupAuthenticator.from_file(args.credential_file)
    trusted_verifiers = recovery_backup_trusted_verifiers_for_active(authenticator)
    if args.recovery_action == "status":
        pointer, receipt, _target, tool, _expectations = load_recovery_backup_generation(
            Path(config.publication_root),
            trusted_verifiers=trusted_verifiers,
        )
        tool_verifier = trusted_verifiers.get(tool.key_id)
        if tool_verifier is None or not tool_verifier.verify(
            tool.signing_payload(), tool.signature
        ):
            raise RuntimeError("recovery backup verifier signature is invalid")
        print(
            json.dumps(
                {
                    "status": "ready",
                    "manifest_id": pointer.manifest_id,
                    "profile_generation": pointer.profile_generation,
                    "receipt_id": receipt.receipt_id,
                    "completed_at": receipt.completed_at.isoformat(),
                    "paper_ledger_head": receipt.paper_ledger_head.head_id,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    producer = RecoveryBackupProducer(
        config=config,
        signer=authenticator,
        trusted_verifiers=trusted_verifiers,
    )
    preview = producer.preview()
    if args.recovery_action == "dry-run":
        print(preview.model_dump_json())
        return 0
    receipt = producer.execute(expected_plan_id=args.plan_id)
    print(receipt.model_dump_json())
    return 0


def cmd_runtime_recovery_production_config(args: argparse.Namespace) -> int:
    """Resolve backup settings only from the current hash-bound production profile."""

    from rquant.runtime_deployment_profile import (
        load_current_runtime_deployment_profile,
        validate_runtime_recovery_backup_config,
    )
    from rquant.runtime_recovery_backup import load_recovery_backup_config

    runtime_root = Path(args.runtime_root)
    profile = load_current_runtime_deployment_profile(runtime_root)
    recovery = profile.recovery
    if recovery is None or recovery.profile_generation is None:
        raise ValueError("current runtime profile has no recovery production configuration")
    backup_config = load_recovery_backup_config(recovery.backup_config_path)
    validate_runtime_recovery_backup_config(profile, backup_config)
    print(
        json.dumps(
            {
                "status": "ready",
                "runtime_root": str(runtime_root),
                "producer_commit": profile.producer_commit,
                "profile_id": profile.profile_id,
                "profile_generation": recovery.profile_generation,
                "backup_environment": dict(recovery.backup_environment()),
                "recovery_service_arguments": dict(recovery.recovery_service_arguments()),
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _runtime_recovery_rehearsal_due(
    *,
    state_path: Path,
    receipt_root: Path,
    interval_seconds: int,
    now: datetime,
) -> tuple[bool, datetime | None, datetime | None]:
    from rquant.runtime_recovery_service import load_verified_recovery_service_receipts

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("recovery rehearsal clock must be timezone-aware")
    if type(interval_seconds) is not int or interval_seconds < 60:
        raise ValueError("recovery rehearsal interval must be at least 60 seconds")
    state_exists = state_path.exists()
    receipts_exist = receipt_root.exists()
    if not state_exists and not receipts_exist:
        return True, None, None
    if state_exists != receipts_exist:
        raise ValueError("recovery rehearsal state and receipt root are inconsistent")
    receipts = load_verified_recovery_service_receipts(
        state_path=state_path,
        receipt_root=receipt_root,
    )
    successful = tuple(
        receipt
        for receipt in receipts
        if receipt.status == "succeeded" and receipt.verification_level == "full"
    )
    if not successful:
        return True, None, None
    last = max(receipt.completed_at for receipt in successful).astimezone(UTC)
    observed_now = now.astimezone(UTC)
    if last > observed_now:
        raise ValueError("recovery rehearsal receipt is dated in the future")
    next_due = last + timedelta(seconds=interval_seconds)
    return observed_now >= next_due, last, next_due


def cmd_runtime_recovery_production(args: argparse.Namespace) -> int:
    """Run recovery using only the current trusted production profile."""

    from rquant.runtime_deployment_profile import (
        load_current_runtime_deployment_profile,
        validate_runtime_recovery_backup_config,
    )
    from rquant.runtime_recovery_backup import load_recovery_backup_config

    runtime_root = Path(args.runtime_root)
    profile = load_current_runtime_deployment_profile(runtime_root)
    recovery = profile.recovery
    if recovery is None or recovery.profile_generation is None:
        raise ValueError("current runtime profile has no recovery production configuration")
    if recovery.profile_generation != str(args.expected_profile_generation):
        raise ValueError("recovery unit profile generation is stale")
    backup_config = load_recovery_backup_config(recovery.backup_config_path)
    validate_runtime_recovery_backup_config(profile, backup_config)
    arguments = dict(recovery.recovery_service_arguments())
    required = {
        "publication_root",
        "state_path",
        "receipt_root",
        "restore_root",
        "credential_file",
        "lease_seconds",
        "max_attempts",
        "retry_delay_seconds",
        "deadline_seconds",
        "rehearsal_interval_seconds",
    }
    if set(arguments) != required:
        raise ValueError("current recovery profile service arguments are incomplete")
    action = str(args.production_recovery_action)
    if action not in {"execute", "rehearse"}:  # pragma: no cover - argparse guards this
        raise ValueError("unknown production recovery action")
    rehearsal_interval = int(arguments["rehearsal_interval_seconds"])
    if action == "rehearse":
        due, last_successful, next_due = _runtime_recovery_rehearsal_due(
            state_path=Path(arguments["state_path"]),
            receipt_root=Path(arguments["receipt_root"]),
            interval_seconds=rehearsal_interval,
            now=_utc_now(),
        )
        if not due:
            print(
                json.dumps(
                    {
                        "last_successful_at": (
                            None if last_successful is None else last_successful.isoformat()
                        ),
                        "next_due_at": None if next_due is None else next_due.isoformat(),
                        "profile_generation": recovery.profile_generation,
                        "reason": "rehearsal_not_due",
                        "status": "skipped",
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
            return 0
    return cmd_runtime_recovery(
        argparse.Namespace(
            recovery_action="execute",
            publication_root=Path(arguments["publication_root"]),
            state_path=Path(arguments["state_path"]),
            receipt_root=Path(arguments["receipt_root"]),
            restore_root=Path(arguments["restore_root"]),
            credential_file=Path(arguments["credential_file"]),
            lease_seconds=int(arguments["lease_seconds"]),
            max_attempts=int(arguments["max_attempts"]),
            retry_delay_seconds=int(arguments["retry_delay_seconds"]),
            deadline_seconds=int(arguments["deadline_seconds"]),
            schedule_cycle_seconds=(None if action == "execute" else rehearsal_interval),
            worker_id=(f"runtime-recovery-{action}-{recovery.profile_generation[:12]}"),
            accept_current_plan=True,
            plan_id=None,
        )
    )


def _runtime_recovery_preview(args: argparse.Namespace) -> tuple[dict[str, object], object]:
    from rquant.runtime_contracts import canonical_sha256
    from rquant.runtime_recovery_backup import (
        RecoveryBackupAuthenticator,
        load_recovery_backup_generation,
        recovery_backup_trusted_verifiers_for_active,
    )
    from rquant.runtime_recovery_coordinator import RuntimeRecoveryFixedReplayVerifier

    authenticator = RecoveryBackupAuthenticator.from_file(args.credential_file)
    trusted_verifiers = recovery_backup_trusted_verifiers_for_active(authenticator)
    pointer, receipt, target, tool, expectations = load_recovery_backup_generation(
        args.publication_root,
        trusted_verifiers=trusted_verifiers,
    )
    verifier = RuntimeRecoveryFixedReplayVerifier(expectations=expectations.expectations)
    tool_verifier = trusted_verifiers.get(tool.key_id)
    if (
        tool_verifier is None
        or not tool_verifier.verify(tool.signing_payload(), tool.signature)
        or verifier.fingerprint != tool.executable_fingerprint
    ):
        raise RuntimeError("recovery verifier bundle is not trusted")
    plan = {
        "contract": "runtime-recovery-execution-plan/v2",
        "manifest_id": str(target.manifest_id),
        "tool_bundle_id": str(tool.bundle_id),
        "profile_generation": target.target_profile_generation,
        "backup_receipt_id": str(receipt.receipt_id),
        "publication_root": str(args.publication_root),
        "state_path": str(args.state_path),
        "receipt_root": str(args.receipt_root),
        "restore_root": str(args.restore_root),
        "credential_file": str(args.credential_file),
        "worker_id": str(args.worker_id),
        "lease_seconds": _runtime_recovery_lease_seconds(args),
        "max_attempts": args.max_attempts,
        "retry_delay_seconds": args.retry_delay_seconds,
        "deadline_seconds": args.deadline_seconds,
        "schedule_cycle_seconds": getattr(args, "schedule_cycle_seconds", None),
    }
    output = {
        "status": "ready",
        "plan_id": canonical_sha256(plan),
        "manifest_id": pointer.manifest_id,
        "profile_generation": pointer.profile_generation,
        "deadline_seconds": args.deadline_seconds,
    }
    return output, (pointer, target, tool, verifier, tool_verifier)


def _runtime_recovery_lease_seconds(args: argparse.Namespace) -> int:
    configured = getattr(args, "lease_seconds", None)
    if configured is not None:
        return int(configured)
    return min(300, max(10, int(args.deadline_seconds) // 3))


def _runtime_recovery_request_id(
    *,
    manifest_id: str,
    now: datetime,
    schedule_cycle_seconds: int | None,
) -> str:
    """Return one stable request identity per external scheduler cycle."""

    if schedule_cycle_seconds is None:
        return f"rehearsal-{manifest_id}"
    if type(schedule_cycle_seconds) is not int or schedule_cycle_seconds < 60:
        raise ValueError("schedule cycle must be at least 60 seconds")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("schedule cycle timestamp must be timezone-aware")
    cycle = math.floor(now.astimezone(UTC).timestamp() / schedule_cycle_seconds)
    return f"rehearsal-{manifest_id}-{schedule_cycle_seconds}-{cycle}"


def cmd_runtime_recovery(args: argparse.Namespace) -> int:
    """Dry-run, execute, or inspect one isolated runtime recovery rehearsal."""

    from rquant.runtime_recovery_service import (
        RuntimeRecoveryService,
        load_verified_recovery_service_receipts,
    )

    preview, bindings = _runtime_recovery_preview(args)
    if args.recovery_action == "dry-run":
        print(json.dumps(preview, separators=(",", ":"), sort_keys=True))
        return 0
    if args.recovery_action == "status":
        receipts = load_verified_recovery_service_receipts(
            state_path=args.state_path,
            receipt_root=args.receipt_root,
        )
        latest = (
            None
            if not receipts
            else max(receipts, key=lambda item: (item.completed_at, str(item.receipt_id)))
        )
        print(
            json.dumps(
                {
                    **preview,
                    "status": "missing" if latest is None else latest.status,
                    "service_receipt_id": None if latest is None else latest.receipt_id,
                    "recovery_receipt_id": (None if latest is None else latest.recovery_receipt_id),
                    "verification_level": (None if latest is None else latest.verification_level),
                    "completed_at": (None if latest is None else latest.completed_at.isoformat()),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    if not getattr(args, "accept_current_plan", False) and args.plan_id != preview["plan_id"]:
        raise RuntimeError("recovery execution plan changed after dry-run")
    pointer, target, _tool, verifier, signature_verifier = bindings
    generation = args.publication_root.joinpath(*Path(pointer.generation_path).parts)
    args.restore_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    service = RuntimeRecoveryService(
        state_path=args.state_path,
        receipt_root=args.receipt_root,
        worker_id=args.worker_id,
        lease_seconds=_runtime_recovery_lease_seconds(args),
        max_attempts=args.max_attempts,
        retry_delay_seconds=args.retry_delay_seconds,
    )
    now = datetime.now(UTC)
    service.submit(
        request_id=_runtime_recovery_request_id(
            manifest_id=str(target.manifest_id),
            now=now,
            schedule_cycle_seconds=getattr(args, "schedule_cycle_seconds", None),
        ),
        backup_root=generation,
        manifest_path=generation / "recovery-target.json",
        tool_bundle_path=generation / "recovery-tool.json",
        restore_root=args.restore_root,
        deadline_at=now + timedelta(seconds=args.deadline_seconds),
    )
    result = service.run_real_once(
        signature_verifier=signature_verifier,
        fixed_replay_verifier=verifier,
    )
    print(
        json.dumps(
            {
                **preview,
                "status": "idle" if result is None else result.status,
                "service_result": None if result is None else result.model_dump(mode="json"),
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
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
                parquet,
                store,
                list_label=args.label,
            )
        logger.info(
            f"parquet 落库完成：{n} 行 → {'list_label=' + args.label if args.label else '全表覆盖'}"
        )
        return 0

    elif args.blacklist_action == "export-parquet":
        out = Path(args.output).expanduser().resolve()
        with DuckDBStore() as store:
            n = export_blacklist_parquet(
                store,
                out,
                list_label=args.label,
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
                spec_path.parent.parent if spec_path.parent.name == "strategy_lab_runs" else None
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
    research_snapshot = getattr(args, "research_snapshot", None)
    if research_snapshot is not None:
        resolved_snapshot = str(Path(research_snapshot).expanduser().resolve())
        bound_snapshot = spec.get("research_snapshot")
        if bound_snapshot is None:
            spec["research_snapshot"] = resolved_snapshot
        elif not isinstance(bound_snapshot, str) or (
            str(Path(bound_snapshot).expanduser().resolve()) != resolved_snapshot
        ):
            spec["research_snapshot_mismatch"] = True
    try:
        run_id = execute_spec(spec)
    except Exception:
        logger.exception("lab-run 执行失败（error 已写入 status 文件）")
        return 1
    logger.info(f"lab-run 完成: {run_id}")
    return 0


def cmd_lab_integrity_audit(args: argparse.Namespace) -> int:
    """Run the explicit full-ledger audit for a scheduleable Lab health check."""

    from rquant.lab_highwater_authority import (
        PRODUCTION_LAB_HIGHWATER_COMMAND,
        LabHighWaterAuthorityClient,
        LabHighWaterAuthorityConfig,
        LabHighWaterAuthorityError,
        load_highwater_trusted_keys,
    )
    from rquant.lab_jobs import (
        InvalidStoredJobError,
        LabDatabaseIdentityError,
        LabJobReader,
    )

    path = Path(args.jobs_path).expanduser().resolve()
    machine_receipt = bool(getattr(args, "machine_receipt", False))
    require_highwater = bool(getattr(args, "require_external_highwater", False))
    highwater_production_mode = bool(getattr(args, "highwater_production_mode", False))
    highwater_observer = None
    if require_highwater:
        values = (
            getattr(args, "highwater_command_json", None),
            getattr(args, "highwater_stable_identity", None),
            getattr(args, "highwater_code_identity", None),
            getattr(args, "highwater_profile_identity", None),
            getattr(args, "highwater_trusted_keyring", None),
        )
        if any(value is None or not str(value).strip() for value in values):
            if not machine_receipt:
                logger.error("lab integrity audit high-water options must be supplied together")
            return 2
        try:
            command = json.loads(str(args.highwater_command_json))
            if not isinstance(command, list) or not command:
                raise ValueError("high-water command must be a nonempty JSON array")
            command_parts = tuple(command)
            if any(not isinstance(part, str) or not part for part in command_parts):
                raise ValueError("high-water command contains an invalid argument")
            if highwater_production_mode and command_parts != PRODUCTION_LAB_HIGHWATER_COMMAND:
                raise ValueError("production high-water command must be the fixed sudo helper")
            trusted_keys = load_highwater_trusted_keys(
                Path(args.highwater_trusted_keyring).expanduser().resolve()
            )
            highwater_observer = LabHighWaterAuthorityClient(
                LabHighWaterAuthorityConfig(
                    command=command_parts,
                    stable_identity=str(args.highwater_stable_identity),
                    code_identity=str(args.highwater_code_identity),
                    profile_identity=str(args.highwater_profile_identity),
                    trusted_key_provider=trusted_keys.get,
                    timeout_seconds=float(getattr(args, "highwater_timeout_seconds", 10.0)),
                    allow_identity_rotation=bool(
                        getattr(args, "highwater_allow_identity_rotation", False)
                    ),
                    production_mode=highwater_production_mode,
                )
            )
        except (OSError, TypeError, ValueError) as exc:
            if not machine_receipt:
                logger.error("lab integrity audit high-water credential is invalid: {}", exc)
            return 2
    try:
        receipt = LabJobReader(
            path,
            highwater_observer=highwater_observer,
            production_mode=require_highwater,
        ).audit_integrity()
    except (
        InvalidStoredJobError,
        LabDatabaseIdentityError,
        LabHighWaterAuthorityError,
        sqlite3.Error,
        OSError,
        ValueError,
    ) as exc:
        if not machine_receipt:
            logger.error(
                "lab integrity audit degraded: jobs_path={} error_type={} message={}",
                path,
                type(exc).__name__,
                " ".join(str(exc).split())[:400],
            )
        return 2
    if machine_receipt:
        print(
            json.dumps(
                {"receipt_hash": receipt.receipt_hash},
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    else:
        logger.info("lab integrity audit healthy: {}", receipt.model_dump_json())
    return 0


def _lab_deployment_generation_binding(args: argparse.Namespace) -> dict[str, object]:
    generation = getattr(args, "deployment_generation", None)
    lock_path = getattr(args, "deployment_lock_path", None)
    descriptor = getattr(args, "deployment_generation_fd", None)
    values = (generation, lock_path, descriptor)
    if all(value is None for value in values):
        return {}
    if any(value is None for value in values):
        raise RuntimeError("incomplete Lab deployment generation binding")
    return {
        "deployment_generation": generation,
        "deployment_lock_path": Path(lock_path),
        "deployment_generation_fd": descriptor,
    }


def _lab_startup_deadline_binding(args: argparse.Namespace) -> dict[str, float]:
    deadline = getattr(args, "startup_deadline_monotonic", None)
    if deadline is None:
        raise RuntimeError("Lab startup deadline binding is missing")
    absolute_deadline = float(deadline)
    if not math.isfinite(absolute_deadline) or time.monotonic() >= absolute_deadline:
        raise RuntimeError("Lab startup deadline binding is invalid or expired")
    return {"startup_deadline_monotonic": absolute_deadline}


def _lab_daemon_readiness_context(
    args: argparse.Namespace,
    *,
    label: str,
    code_sha: str,
    runtime_guard: object,
    runtime_identity: object,
    daemon_lock: object,
) -> AbstractContextManager[object]:
    generation_binding = _lab_deployment_generation_binding(args)
    if not generation_binding:
        return nullcontext()
    operation_id = getattr(args, "deployment_operation_id", None)
    environment_generation = getattr(args, "deployment_environment_generation", None)
    if not isinstance(operation_id, str) or not isinstance(environment_generation, str):
        raise RuntimeError("incomplete Lab deployment readiness binding")
    from rquant.config import settings
    from rquant.lab_daemon import LabDaemonReadinessPublisher

    verify_identity = getattr(runtime_guard, "verify_identity", None)
    if not callable(verify_identity):
        raise RuntimeError("Lab runtime guard cannot publish readiness")
    mutation_guard = partial(verify_identity, runtime_identity)
    duplicate_lease = getattr(daemon_lock, "duplicate_authority_lease", None)
    if not callable(duplicate_lease):
        raise RuntimeError("Lab daemon lock cannot provide a readiness authority lease")
    lease_fd = int(duplicate_lease())
    try:
        return LabDaemonReadinessPublisher(
            deployment_lock_path=Path(str(generation_binding["deployment_lock_path"])),
            deployment_lock_fd=int(generation_binding["deployment_generation_fd"]),
            daemon_authority_lease_fd=lease_fd,
            label=label,
            operation_id=operation_id,
            environment_generation_id=environment_generation,
            code_sha=code_sha,
            heartbeat_interval_seconds=2,
            readiness_root=settings.lab_readiness_dir_resolved,
            mutation_guard=mutation_guard,
        )
    except BaseException:
        os.close(lease_fd)
        raise


@dataclass(frozen=True)
class _LabHighWaterRuntimeBinding:
    observer: object | None
    audit_command: tuple[str, ...] | None
    state_path: Path | None
    remediation_authorizer: Callable[[], None] | None
    degradation_reporter: Callable[[str], None] | None
    production_mode: bool


def _resolve_lab_highwater_runtime_binding(
    *,
    settings: Settings,
    code_sha: str,
    profile_identity: str,
    highwater_profile: LabHighWaterRuntimeProfile | None = None,
    require_profile: bool = False,
) -> _LabHighWaterRuntimeBinding:
    """Bind Lab integrity checks to the profile-owned external authority."""

    from rquant.lab_daemon import LabDaemonConfigurationError
    from rquant.lab_highwater_authority import (
        PRODUCTION_LAB_HIGHWATER_COMMAND,
        LabHighWaterAuthorityClient,
        LabHighWaterAuthorityConfig,
        load_highwater_trusted_keys,
    )

    environment_overrides = (
        settings.lab_highwater_authority_command_json.strip(),
        settings.lab_highwater_stable_identity.strip(),
        settings.lab_highwater_trusted_keyring_path,
    )
    if highwater_profile is None:
        if require_profile:
            raise LabDaemonConfigurationError(
                "production Lab high-water authority must come from the immutable profile"
            )
        if any(environment_overrides):
            raise LabDaemonConfigurationError(
                "Lab high-water runtime environment overrides are not accepted"
            )
        return _LabHighWaterRuntimeBinding(None, None, None, None, None, False)
    try:
        command = tuple(highwater_profile.authority_command)
        stable_identity = str(highwater_profile.stable_identity)
        credential_path = Path(highwater_profile.trusted_keyring_path)
        timeout_seconds = float(highwater_profile.timeout_seconds)
        allow_identity_rotation = bool(highwater_profile.allow_identity_rotation)
        production = bool(highwater_profile.production_mode)
    except (TypeError, ValueError, AttributeError) as exc:
        raise LabDaemonConfigurationError("Lab high-water immutable profile is invalid") from exc
    if not command or any(not part for part in command) or not stable_identity:
        raise LabDaemonConfigurationError("Lab high-water immutable profile is incomplete")
    runtime_root = settings.lab_runtime_dir_resolved
    if credential_path.is_relative_to(runtime_root):
        raise LabDaemonConfigurationError(
            "Lab high-water verification credential must be outside the Lab runtime root"
        )
    if production and command != PRODUCTION_LAB_HIGHWATER_COMMAND:
        raise LabDaemonConfigurationError(
            "production Lab high-water authority must use the fixed sudo helper"
        )
    if production and any(option in {"--state-root", "--keys-file"} for option in command):
        raise LabDaemonConfigurationError(
            "production Lab cannot name high-water authority storage or signing keys"
        )
    try:
        trusted_keys = load_highwater_trusted_keys(credential_path)
        observer = LabHighWaterAuthorityClient(
            LabHighWaterAuthorityConfig(
                command=command,
                stable_identity=stable_identity,
                code_identity=code_sha,
                profile_identity=profile_identity,
                trusted_key_provider=trusted_keys.get,
                timeout_seconds=timeout_seconds,
                allow_identity_rotation=allow_identity_rotation,
                production_mode=production,
            )
        )
    except (OSError, ValueError) as exc:
        raise LabDaemonConfigurationError(
            "Lab high-water verification credential is unavailable or invalid"
        ) from exc
    audit_command = (
        (
            sys.executable,
            "-m",
            "rquant.cli",
            "lab-integrity-audit",
            "--jobs-path",
            str(settings.lab_jobs_path_resolved),
            "--require-external-highwater",
            "--highwater-command-json",
            json.dumps(list(command), separators=(",", ":")),
            "--highwater-stable-identity",
            stable_identity,
            "--highwater-code-identity",
            code_sha,
            "--highwater-profile-identity",
            profile_identity,
            "--highwater-trusted-keyring",
            str(credential_path),
            "--highwater-timeout-seconds",
            str(timeout_seconds),
            "--machine-receipt",
        )
        + (("--highwater-production-mode",) if production else ())
        + (("--highwater-allow-identity-rotation",) if allow_identity_rotation else ())
    )
    return _LabHighWaterRuntimeBinding(
        observer,
        audit_command,
        settings.lab_finalizer_state_dir_resolved / "full-integrity-audit.json",
        observer.authorize_remediation,
        observer.mark_degraded,
        production,
    )


def _lab_runtime_layout() -> tuple[dict[str, Path], dict[str, Path], dict[Path, Path]]:
    from rquant.config import settings

    directories = {
        "lab command spool": settings.lab_job_command_dir_resolved,
        "lab claim spool": settings.lab_job_claim_dir_resolved,
        "lab report spool": settings.lab_job_report_dir_resolved,
        "lab worker artifact root": settings.lab_worker_artifact_dir_resolved,
        "lab final artifact root": settings.lab_final_artifact_dir_resolved,
        "lab artifact commit spool": settings.lab_artifact_commit_dir_resolved,
        "lab daemon lock root": settings.lab_daemon_lock_dir_resolved,
        "lab finalizer state root": settings.lab_finalizer_state_dir_resolved,
        "lab readiness root": settings.lab_readiness_dir_resolved,
    }
    files = {"lab jobs SQLite": settings.lab_jobs_path_resolved}
    legacy = {
        settings.lab_jobs_path_resolved: settings.data_dir / "lab_jobs.sqlite3",
        settings.lab_job_command_dir_resolved: settings.data_dir / "lab_job_commands",
        settings.lab_job_claim_dir_resolved: settings.data_dir / "lab_shard_claims",
        settings.lab_job_report_dir_resolved: settings.data_dir / "lab_worker_reports",
        settings.lab_worker_artifact_dir_resolved: settings.data_dir / "lab_worker_artifacts",
        settings.lab_final_artifact_dir_resolved: settings.data_dir / "lab_final_artifacts",
        settings.lab_artifact_commit_dir_resolved: settings.data_dir / "lab_artifact_commits",
        settings.lab_daemon_lock_dir_resolved: settings.data_dir / "lab_daemon_locks",
        settings.lab_finalizer_state_dir_resolved: settings.data_dir / "lab_finalizer_state",
    }
    return directories, files, legacy


def _establish_lab_runtime_identity(
    args: argparse.Namespace,
) -> tuple[
    str,
    AttestedLabRuntimeGuard,
    CodeTrustEvidence,
    Callable[[], str],
]:
    from rquant.formal_runtime_composition import (
        FormalRuntimeCompositionError,
        open_formal_runtime_capability,
    )
    from rquant.lab_daemon import (
        AttestedLabRuntimeGuard,
        LabDaemonConfigurationError,
        require_lab_runtime_binding,
    )

    required_bootstrap_options = (
        ("runtime_code_config", "--runtime-code-config"),
        ("runtime_code_trusted_base", "--runtime-code-trusted-base"),
        ("runtime_code_authority_uid", "--runtime-code-authority-uid"),
        ("runtime_code_authority_gid", "--runtime-code-authority-gid"),
    )
    missing = tuple(
        option
        for attribute, option in required_bootstrap_options
        if getattr(args, attribute, None) is None
    )
    if missing:
        raise LabDaemonConfigurationError(
            "formal Lab runtime bootstrap requires " + ", ".join(missing)
        )
    deadline_binding = _lab_startup_deadline_binding(args)
    try:
        capability = open_formal_runtime_capability(
            configuration_path=Path(str(args.runtime_code_config)),
            trusted_base=Path(str(args.runtime_code_trusted_base)),
            expected_authority_uid=int(args.runtime_code_authority_uid),
            expected_authority_gid=int(args.runtime_code_authority_gid),
            startup_deadline_monotonic=deadline_binding["startup_deadline_monotonic"],
        )
    except FormalRuntimeCompositionError as exc:
        raise LabDaemonConfigurationError("formal Lab runtime bootstrap failed") from exc
    runtime_identity = require_lab_runtime_binding(capability)
    runtime_guard = AttestedLabRuntimeGuard(
        capability=capability,
        startup_evidence=runtime_identity,
    )
    code_sha = runtime_guard.verify(**deadline_binding)
    deployment_generation = getattr(args, "deployment_generation", None)
    if deployment_generation is not None and deployment_generation != code_sha:
        capability.close()
        raise LabDaemonConfigurationError(
            "formal Lab deployment generation does not match signed provenance"
        )
    identity_guard = runtime_guard
    return code_sha, runtime_guard, runtime_identity, identity_guard


def _verify_prepared_lab_runtime(
    checkout_root: Path,
    code_sha: str,
    *,
    allow_uninitialized_database: bool = False,
) -> None:
    from rquant.config import settings
    from rquant.lab_daemon import verify_lab_runtime_prepared

    directories, files, legacy = _lab_runtime_layout()
    verify_lab_runtime_prepared(
        settings.lab_runtime_dir_resolved,
        checkout_root=checkout_root,
        expected_commit=code_sha,
        managed_directories=directories,
        managed_files=files,
        legacy_paths=legacy,
        allow_missing_files=(
            frozenset({"lab jobs SQLite"}) if allow_uninitialized_database else frozenset()
        ),
    )


def cmd_lab_runtime_prepare(args: argparse.Namespace) -> int:
    """Create/migrate the dedicated private Lab runtime namespace once."""
    from rquant.artifact_retention_catalog_authority import (
        initialize_retention_catalog_authority,
    )
    from rquant.config import settings
    from rquant.job_center_authority import (
        publish_install_current_job_center_authority,
        resolve_current_job_center_authority_binding,
    )
    from rquant.lab_daemon import (
        LabDaemonConfigurationError,
        prepare_lab_runtime_layout,
        prepare_lab_runtime_sqlite_authority,
    )
    from rquant.lab_jobs import LabJobStore

    code_sha, runtime_guard, _runtime_identity, runtime_identity_guard = (
        _establish_lab_runtime_identity(args)
    )
    expected_code_sha = getattr(args, "expected_code_sha", None)
    if expected_code_sha is not None and expected_code_sha != code_sha:
        raise RuntimeError("Lab runtime prepare code SHA does not match deployment target")
    from rquant.runtime_deployment_profile import load_current_runtime_deployment_profile
    from rquant.runtime_service_entrypoint import RuntimeServiceKind

    deployment_root = Path(args.runtime_deployment_root)
    profile = load_current_runtime_deployment_profile(deployment_root)
    retention_manifests = tuple(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION
    )
    if len(retention_manifests) != 1:
        raise RuntimeError("Lab runtime prepare requires exactly one retention owner")
    retention_manifest = retention_manifests[0]
    if profile.producer_commit != code_sha or retention_manifest.producer_commit != code_sha:
        raise RuntimeError("Lab runtime prepare retention owner is stale")
    retention_settings = retention_manifest.settings
    retention_state_root = Path(str(retention_settings["state_root"]))
    retention_reference_store = Path(str(retention_settings["reference_store_path"]))
    initialize_retention_catalog_authority(
        state_root=retention_state_root,
        reference_store_path=retention_reference_store,
        producer_commit=code_sha,
    )
    binding = resolve_current_job_center_authority_binding(
        deployment_root,
        expected_code_sha=code_sha,
        runtime_root=settings.lab_runtime_dir_resolved,
        lab_jobs_path=settings.lab_jobs_path_resolved,
        command_spool_path=settings.lab_job_command_dir_resolved,
        final_artifact_root=settings.lab_final_artifact_dir_resolved,
    )
    directories, files, legacy = _lab_runtime_layout()
    release_root = getattr(runtime_guard.capability, "release_root", None)
    if not isinstance(release_root, Path):
        raise LabDaemonConfigurationError(
            "formal Lab runtime capability has no immutable release root"
        )
    prepare_lab_runtime_layout(
        settings.lab_runtime_dir_resolved,
        checkout_root=release_root,
        managed_directories=directories,
        managed_files=files,
        legacy_paths=legacy,
        mutation_guard=runtime_identity_guard,
    )
    sqlite_authority = prepare_lab_runtime_sqlite_authority(
        settings.lab_runtime_dir_resolved,
        label="lab jobs SQLite",
        path=settings.lab_jobs_path_resolved,
        mutation_guard=runtime_identity_guard,
    )
    try:
        LabJobStore(
            settings.lab_jobs_path_resolved,
            busy_timeout_ms=settings.lab_jobs_busy_timeout_ms,
            identity_authority=sqlite_authority,
            mutation_guard=runtime_identity_guard,
        ).initialize()
    finally:
        sqlite_authority.close()
    publish_install_current_job_center_authority(
        code_sha=code_sha,
        deployment_profile_id=binding.deployment_profile_id,
        deployment_generation_hash=binding.deployment_generation_hash,
        runtime_deployment_root=binding.runtime_deployment_root,
        current_code_sha=runtime_identity_guard,
        runtime_root=binding.runtime_root,
        lab_jobs_path=binding.lab_jobs_path,
        command_spool_path=binding.command_spool_path,
        final_artifact_root=binding.final_artifact_root,
        definition_registry_root=binding.definition_registry_root,
        experiment_registry_path=binding.experiment_registry_path,
        dataset_authority_path=binding.dataset_authority_path,
        catalog_authority_root=binding.catalog_authority_root,
        catalog_authority_receipt_path=binding.catalog_authority_receipt_path,
    )
    logger.info(f"Lab runtime 已就绪: {settings.lab_runtime_dir_resolved}")
    return 0


def cmd_lab_launchd_install(args: argparse.Namespace) -> int:
    """Materialize and optionally load generation-bound Lab LaunchAgents."""
    from rquant.lab_launchd_install import LabLaunchdInstaller

    result = LabLaunchdInstaller(
        checkout_root=Path(args.expected_checkout_root),
        deployment_lock_path=Path(args.deployment_lock_path),
        launch_agents_dir=Path(args.launch_agents_dir),
        trusted_git_path=Path(args.trusted_git_path),
        worker_id=args.worker_id,
    ).install(activate=not args.no_activate)
    logger.info(
        "Lab launchd installer prepared generation "
        f"{result.environment_generation_id[:12]} ({result.code_sha[:12]})"
    )
    return 0


def cmd_lab_launchd_uninstall(args: argparse.Namespace) -> int:
    """Unload and remove only the exact recorded Lab LaunchAgents."""
    from rquant.lab_launchd_install import LabLaunchdInstaller

    LabLaunchdInstaller(
        checkout_root=Path(args.expected_checkout_root),
        deployment_lock_path=Path(args.deployment_lock_path),
        launch_agents_dir=Path(args.launch_agents_dir),
        trusted_git_path=Path(args.trusted_git_path),
    ).uninstall(deactivate=not args.no_deactivate)
    logger.info("Lab launchd installation removed")
    return 0


def cmd_lab_scheduler(args: argparse.Namespace) -> int:
    """Run the durable Strategy Lab control-plane scheduler."""
    code_sha, runtime_guard, runtime_identity, runtime_identity_guard = (
        _establish_lab_runtime_identity(args)
    )
    from rquant.config import settings
    from rquant.job_center_authority import resolve_current_job_center_authority_binding
    from rquant.lab_artifact_protocol import LabArtifactCommitSpool
    from rquant.lab_artifacts import LabJobArtifactStore
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutPhase, FinalizerRolloutStore
    from rquant.lab_daemon import (
        LabAuthorityKeyring,
        LabDaemonConfigurationError,
        LabDaemonLock,
        ensure_private_directory,
        load_lab_job_center_authority_manifest,
        prepare_lab_runtime_sqlite_authority,
        require_unique_runtime_paths,
    )
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabFullIntegrityAuditStateStore, LabScheduler
    from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
    from rquant.lab_worker import LabArtifactReclaimer
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    def load_current_authority() -> object:
        binding = resolve_current_job_center_authority_binding(
            Path(args.runtime_deployment_root),
            expected_code_sha=runtime_identity_guard(),
            runtime_root=settings.lab_runtime_dir_resolved,
            lab_jobs_path=settings.lab_jobs_path_resolved,
            command_spool_path=settings.lab_job_command_dir_resolved,
            final_artifact_root=settings.lab_final_artifact_dir_resolved,
        )
        return load_lab_job_center_authority_manifest(
            binding.runtime_root / "job-center-authority.json",
            expected_code_sha=code_sha,
            expected_research_root=binding.runtime_root,
            expected_lab_jobs_path=binding.lab_jobs_path,
            expected_command_spool_path=binding.command_spool_path,
            expected_final_artifact_root=binding.final_artifact_root,
            expected_runtime_deployment_root=binding.runtime_deployment_root,
            expected_deployment_profile_id=binding.deployment_profile_id,
            expected_deployment_generation_hash=binding.deployment_generation_hash,
        )

    load_current_authority()
    setup_logging()
    rollout = None
    if settings.lab_v2_claim_publication_enabled:
        rollout = FinalizerRolloutStore(
            settings.lab_finalizer_state_dir_resolved / "claim-finalizer-rollout.sqlite3",
            create=False,
        )
        rollout.require_scheduler_v2_emit()

    def v2_emit_permit(holder: str) -> AbstractContextManager[object]:
        if not settings.lab_v2_claim_publication_enabled:
            return nullcontext()
        return FinalizerRolloutStore(
            settings.lab_finalizer_state_dir_resolved / "claim-finalizer-rollout.sqlite3",
            create=False,
        ).emit_permit(holder=holder)

    if (
        not settings.lab_finalizer_authority_key_id
        or settings.lab_finalizer_authority_key_path is None
        or settings.lab_finalizer_authority_keyring_path is None
    ):
        raise LabDaemonConfigurationError("lab authority key configuration is incomplete")
    keyring = LabAuthorityKeyring.load(
        active_key_id=settings.lab_finalizer_authority_key_id,
        active_key_path=settings.lab_finalizer_authority_key_path,
        verification_keyring_path=settings.lab_finalizer_authority_keyring_path,
    )
    binding = resolve_current_job_center_authority_binding(
        Path(args.runtime_deployment_root),
        expected_code_sha=runtime_identity_guard(),
        runtime_root=settings.lab_runtime_dir_resolved,
        lab_jobs_path=settings.lab_jobs_path_resolved,
        command_spool_path=settings.lab_job_command_dir_resolved,
        final_artifact_root=settings.lab_final_artifact_dir_resolved,
    )
    highwater = _resolve_lab_highwater_runtime_binding(
        settings=settings,
        code_sha=code_sha,
        profile_identity=binding.deployment_profile_id,
        highwater_profile=getattr(binding, "lab_highwater", None),
        require_profile=getattr(binding, "runtime_mode", "local-test") == "linux-production",
    )
    for label, path in (
        ("lab command spool", settings.lab_job_command_dir_resolved),
        ("lab claim spool", settings.lab_job_claim_dir_resolved),
        ("lab report spool", settings.lab_job_report_dir_resolved),
        ("lab worker artifact root", settings.lab_worker_artifact_dir_resolved),
        ("lab final artifact root", settings.lab_final_artifact_dir_resolved),
        ("lab artifact commit spool", settings.lab_artifact_commit_dir_resolved),
        ("lab daemon lock root", settings.lab_daemon_lock_dir_resolved),
        ("lab finalizer state root", settings.lab_finalizer_state_dir_resolved),
    ):
        ensure_private_directory(path, label=label, mutation_guard=runtime_identity_guard)
    runtime_paths = {
        "lab command spool": settings.lab_job_command_dir_resolved,
        "lab claim spool": settings.lab_job_claim_dir_resolved,
        "lab report spool": settings.lab_job_report_dir_resolved,
        "lab worker artifact root": settings.lab_worker_artifact_dir_resolved,
        "lab final artifact root": settings.lab_final_artifact_dir_resolved,
        "lab artifact commit spool": settings.lab_artifact_commit_dir_resolved,
        "lab daemon lock root": settings.lab_daemon_lock_dir_resolved,
        "lab finalizer state root": settings.lab_finalizer_state_dir_resolved,
        "lab authority signing key": settings.lab_finalizer_authority_key_path,
        "lab authority keyring": settings.lab_finalizer_authority_keyring_path,
    }
    if os.path.lexists(settings.lab_jobs_path_resolved):
        runtime_paths["lab jobs SQLite"] = settings.lab_jobs_path_resolved
    require_unique_runtime_paths(runtime_paths)
    with LabDaemonLock(
        settings.lab_daemon_lock_dir_resolved,
        "scheduler",
        mutation_guard=runtime_identity_guard,
    ) as daemon_lock:
        sqlite_authority = prepare_lab_runtime_sqlite_authority(
            settings.lab_runtime_dir_resolved,
            label="lab jobs SQLite",
            path=settings.lab_jobs_path_resolved,
            mutation_guard=runtime_identity_guard,
        )
        artifact_store = None
        try:
            artifact_store = LabJobArtifactStore(
                settings.lab_final_artifact_dir_resolved,
                mutation_guard=runtime_identity_guard,
            )
            store = LabJobStore(
                settings.lab_jobs_path_resolved,
                busy_timeout_ms=settings.lab_jobs_busy_timeout_ms,
                identity_authority=sqlite_authority,
                mutation_guard=runtime_identity_guard,
            )
            store.initialize()
            integrity_reader = LabJobReader(
                settings.lab_jobs_path_resolved,
                busy_timeout_ms=settings.lab_jobs_busy_timeout_ms,
                identity_authority=sqlite_authority,
                highwater_observer=highwater.observer,
                production_mode=highwater.production_mode,
            )
            report_spool = LabReportSpool(
                settings.lab_job_report_dir_resolved,
                mutation_guard=runtime_identity_guard,
            )
            artifact_reclaimer = LabArtifactReclaimer(
                artifact_root=settings.lab_worker_artifact_dir_resolved,
                report_spool=report_spool,
                mutation_guard=runtime_identity_guard,
            )
            claim_spool = LabClaimSpool(
                settings.lab_job_claim_dir_resolved,
                claim_advance_hook=artifact_reclaimer.reclaim,
                mutation_guard=runtime_identity_guard,
            )
            scheduler = LabScheduler(
                store=store,
                spool=LabCommandSpool(
                    settings.lab_job_command_dir_resolved,
                    mutation_guard=runtime_identity_guard,
                ),
                owner_id=f"{socket.gethostname()}:{os.getpid()}:{code_sha[:12]}",
                lease_seconds=settings.lab_scheduler_lease_seconds,
                heartbeat_seconds=settings.lab_scheduler_heartbeat_seconds,
                poll_interval_ms=settings.lab_scheduler_poll_interval_ms,
                max_commands_per_tick=settings.lab_scheduler_max_commands_per_tick,
                report_spool=report_spool,
                claim_spool=claim_spool,
                claim_worker_ids=settings.lab_scheduler_worker_id_list,
                shard_lease_seconds=settings.lab_scheduler_shard_lease_seconds,
                max_reports_per_tick=settings.lab_scheduler_max_reports_per_tick,
                adapter_registry=default_strategy_job_adapter_registry(),
                max_plans_per_tick=settings.lab_scheduler_max_plans_per_tick,
                max_claims_per_tick=settings.lab_scheduler_max_claims_per_tick,
                max_claim_authority_per_tick=(settings.lab_scheduler_max_claim_authority_per_tick),
                artifact_commit_spool=LabArtifactCommitSpool(
                    settings.lab_artifact_commit_dir_resolved,
                    mutation_guard=runtime_identity_guard,
                ),
                artifact_store=artifact_store,
                finalizer_authority_key_provider=keyring.verification_key,
                max_artifact_commits_per_tick=(
                    settings.lab_scheduler_max_artifact_commits_per_tick
                ),
                runtime_guard=runtime_identity_guard,
                require_authority_manifest=True,
                authority_manifest_loader=load_current_authority,
                integrity_auditor=integrity_reader,
                full_integrity_command=highwater.audit_command,
                full_integrity_state_store=(
                    None
                    if highwater.state_path is None
                    else LabFullIntegrityAuditStateStore(highwater.state_path)
                ),
                full_integrity_interval_seconds=(
                    settings.lab_scheduler_full_integrity_interval_seconds
                ),
                full_integrity_budget_seconds=(
                    settings.lab_scheduler_full_integrity_budget_seconds
                ),
                full_integrity_remediation_authorizer=highwater.remediation_authorizer,
                full_integrity_degradation_reporter=highwater.degradation_reporter,
                v2_emit_permit=v2_emit_permit,
            )
            readiness = _lab_daemon_readiness_context(
                args,
                label="com.roxor.rquant-lab-scheduler",
                code_sha=code_sha,
                runtime_guard=runtime_guard,
                runtime_identity=runtime_identity,
                daemon_lock=daemon_lock,
            )
            with readiness:
                if (
                    rollout is not None
                    and rollout.snapshot().phase is FinalizerRolloutPhase.V2_WORKERS_READY
                ):
                    rollout.transition(
                        FinalizerRolloutPhase.SCHEDULER_EMITS_V2,
                        evidence=f"scheduler-ready:{code_sha}",
                    )
                if bool(getattr(args, "remediate_full_integrity", False)):
                    try:
                        scheduler.remediate_full_integrity()
                        logger.info("lab-scheduler full integrity remediation completed")
                        return 0
                    finally:
                        scheduler.release()
                if args.once:
                    try:
                        result = scheduler.run_once()
                        logger.info(f"lab-scheduler tick: {result.model_dump_json()}")
                        return 0
                    finally:
                        scheduler.release()

                def handle_signal(signum: int, frame: object) -> None:
                    del frame
                    logger.info(f"lab-scheduler 收到信号 {signum}，请求停止")
                    scheduler.request_stop()

                previous = {
                    signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
                }
                for signum in previous:
                    signal.signal(signum, handle_signal)
                try:
                    scheduler.run_forever()
                finally:
                    for signum, handler in previous.items():
                        signal.signal(signum, handler)
                return 0
        finally:
            if artifact_store is not None:
                artifact_store.close()
            sqlite_authority.close()
            sqlite_authority.close()


def _build_lab_worker_resource_admission(
    *,
    settings: Settings,
    code_sha: str,
    legacy_opt_out: bool,
    clock: Callable[[], datetime] | None = None,
    probe: ResourceProbe | None = None,
) -> RuntimeResourceAdmissionBindings:
    from rquant.runtime_market_session import load_market_calendar_authority
    from rquant.runtime_resource_admission import (
        RuntimeHealthAuthorityLiveSloProbeConfig,
        RuntimeTradeCalendarSessionResolver,
        build_runtime_resource_admission,
    )

    live_slo_root = settings.rquant_lab_live_slo_authority_root
    live_slo_config = (
        RuntimeHealthAuthorityLiveSloProbeConfig(
            authority_root=live_slo_root,
            expected_producer_commit=code_sha,
        )
        if live_slo_root is not None
        else None
    )
    calendar_path = settings.rquant_lab_trade_calendar_path
    session_resolver = (
        RuntimeTradeCalendarSessionResolver(
            load_market_calendar_authority(
                calendar_path,
                expected_commit=code_sha,
            )
        )
        if calendar_path is not None
        else None
    )
    return build_runtime_resource_admission(
        app_env=settings.app_env,
        disk_path=settings.lab_worker_artifact_dir_resolved,
        configured_policy_version=settings.rquant_lab_resource_policy_version,
        legacy_opt_out=legacy_opt_out,
        clock=clock,
        probe=probe,
        live_slo_probe_config=live_slo_config,
        session_resolver=session_resolver,
    )


def _build_lab_worker_resource_authority_manifest(
    *,
    settings: Settings,
    resource_admission: RuntimeResourceAdmissionBindings,
) -> LabResourceAuthorityManifest | None:
    from rquant.lab_daemon import LabDaemonConfigurationError
    from rquant.lab_resource_authority_adapter import (
        ResourceAuthorityAdapterConfigurationError,
        parse_resource_authority_adapter_config,
    )
    from rquant.lab_worker import (
        build_builtin_resource_authority_manifest,
        build_resource_journal_authority_manifest,
    )

    raw_config = settings.rquant_lab_resource_authority_config_json.strip()
    if raw_config:
        try:
            configuration = parse_resource_authority_adapter_config(raw_config)
        except ResourceAuthorityAdapterConfigurationError as exc:
            raise LabDaemonConfigurationError(
                "resource authority explicit V2 configuration is invalid"
            ) from exc
        if settings.app_env == "prod" and configuration.mode != "production":
            raise LabDaemonConfigurationError(
                "production worker requires an explicit V2 production resource authority"
            )
        return build_resource_journal_authority_manifest(configuration)
    if settings.app_env == "prod":
        raise LabDaemonConfigurationError(
            "production worker requires an explicit V2 resource authority configuration"
        )
    if not resource_admission.require_resource_admission:
        return None
    if (
        resource_admission.resource_snapshot_provider is None
        or resource_admission.admission_policy_provider is None
    ):
        raise LabDaemonConfigurationError(
            "required resource admission has no closed authority providers"
        )
    return build_builtin_resource_authority_manifest(
        resource_admission.resource_snapshot_provider,
        resource_admission.admission_policy_provider,
    )


def _build_lab_claim_publication_worker_verifier(
    *,
    settings: Settings,
    ledger: object,
    claim_spool: object,
) -> object | None:
    """Compose the V2 worker D gate from public-only, canonical material."""

    from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
    from rquant.lab_claim_finalizer import LabClaimPublicationWorkerVerifier
    from rquant.lab_claim_finalizer_trust import (
        LabClaimFinalizerTrustError,
        LabClaimFinalizerTrustVerifier,
        LabClaimPublicationWorkerVerificationConfig,
    )
    from rquant.lab_claim_publication import (
        LabClaimSpoolReceiptAuthorityV2,
        LabClaimSpoolReceiptVerifier,
    )
    from rquant.lab_daemon import LabDaemonConfigurationError
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_shard_protocol import LabClaimSpool
    from rquant.source_broker_protocol import ServerCredentialsPolicy, SocketEndpointPolicy
    from rquant.source_broker_v2_authority_service import SourceBrokerV2CurrentClaimUnixClient
    from rquant.strict_json import strict_model_validate_canonical_json

    if not settings.lab_v2_claim_publication_enabled:
        return None
    if settings.lab_claim_finalizer_runtime_material_root is None:
        raise LabDaemonConfigurationError(
            "V2 worker requires a controlled finalizer runtime material root"
        )
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRuntimeError,
        load_current_lab_claim_finalizer_generation,
    )

    try:
        path = load_current_lab_claim_finalizer_generation(
            settings.lab_claim_finalizer_runtime_material_root,
            trusted_base=settings.lab_claim_finalizer_runtime_trusted_base,
        ).worker_verifier_path
    except FinalizerRuntimeError as exc:
        raise LabDaemonConfigurationError(
            "V2 worker cannot select a valid finalizer runtime generation"
        ) from exc
    try:
        from rquant.authority_path_security import read_secure_regular_file

        raw = read_secure_regular_file(
            path,
            trusted_root=settings.lab_claim_finalizer_runtime_material_root,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            allowed_final_uids=frozenset({os.getuid()}),
            allowed_final_gids=frozenset({os.getgid()}),
            allowed_modes=frozenset({0o640}),
            max_bytes=1_048_576,
        )
        configuration = strict_model_validate_canonical_json(
            LabClaimPublicationWorkerVerificationConfig,
            raw,
        )
        configuration.require_verify_only_roles()

        def keyring(records: tuple[object, ...], purpose: str) -> VerifyOnlyEd25519Keyring:
            typed = tuple(records)
            return VerifyOnlyEd25519Keyring(
                records=typed,  # type: ignore[arg-type]
                issuer_allowlist={purpose: frozenset(record.issuer for record in typed)},  # type: ignore[attr-defined]
                rotation_allowlist={
                    (record.issuer, purpose): frozenset(
                        item.key_id for item in typed if item.issuer == record.issuer
                    )
                    for record in typed
                },
            )

        trust_verifier = LabClaimFinalizerTrustVerifier(
            root_keyring=keyring(
                configuration.root_public_keys,
                "lab_claim_finalizer_root",
            ),
            finalizer_keyring=keyring(
                configuration.finalizer_public_keys,
                "lab_claim_finalizer",
            ),
        )
        if type(ledger) is not LabJobStore:
            raise TypeError("V2 worker requires an exact LabJobStore ledger")
        with ledger._connect() as connection:  # noqa: SLF001 - fixed ledger binding
            binding = ledger._finalizer_authority_binding(  # noqa: SLF001
                connection,
                path=ledger.path,
            )
        trust_verifier.require_certificate(
            configuration.trust_certificate,
            store_id=str(binding["store_id"]),
            database_generation=binding["database_generation"],
            schema_version=int(binding["schema_version"]),
            now=datetime.now(UTC),
        )
        source_plan_keyring = keyring(
            configuration.source_plan_public_keys,
            "source_use_plan_v2",
        )
        client = SourceBrokerV2CurrentClaimUnixClient(
            endpoint=SocketEndpointPolicy(
                path=Path(configuration.current_claim_socket_path),
                owner_uid=configuration.current_claim_socket_owner_uid,
                group_gid=configuration.current_claim_socket_group_gid,
                mode=configuration.current_claim_socket_mode,
            ),
            server_policy=ServerCredentialsPolicy(
                expected_uid=configuration.current_claim_server_uid,
                expected_gid=configuration.current_claim_server_gid,
                expected_pid=configuration.current_claim_server_pid,
            ),
            timeout_ms=configuration.current_claim_timeout_ms,
        )

        class _VerifyOnlyCurrentClaimAuthority:
            __slots__ = ("_client",)

            def __init__(self, current_client: SourceBrokerV2CurrentClaimUnixClient) -> None:
                self._client = current_client

            def verify_current(self, *, binding: object, now: datetime) -> object:
                return self._client.verify_current(binding=binding, now=now)  # type: ignore[arg-type]

        if type(claim_spool) is not LabClaimSpool:
            raise TypeError("V2 worker requires an exact LabClaimSpool")
        return LabClaimPublicationWorkerVerifier(
            ledger=ledger,
            current_claim_authority=_VerifyOnlyCurrentClaimAuthority(client),
            keyring=source_plan_keyring,
            audience=configuration.audience,
            spool_receipt_verifier=LabClaimSpoolReceiptVerifier(
                spool=claim_spool,
                authority=LabClaimSpoolReceiptAuthorityV2.model_validate(
                    configuration.spool_receipt_authority,
                    strict=True,
                ),
            ),
            trust_verifier=trust_verifier,
        )
    except (LabClaimFinalizerTrustError, OSError, TypeError, ValueError) as exc:
        raise LabDaemonConfigurationError(
            "V2 claim publication public verifier material is invalid"
        ) from exc


def cmd_lab_worker(args: argparse.Namespace) -> int:
    """Run a fenced Strategy Lab shard worker."""
    code_sha, runtime_guard, runtime_identity, runtime_identity_guard = (
        _establish_lab_runtime_identity(args)
    )
    from rquant.config import settings
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutPhase, FinalizerRolloutStore
    from rquant.lab_daemon import (
        LabDaemonConfigurationError,
        LabDaemonLock,
        ensure_private_directory,
        require_unique_runtime_paths,
    )
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
    from rquant.lab_worker import (
        LAB_WORKER_MAX_SHARDS_PER_TICK,
        LabWorker,
        build_builtin_shard_runtime_manifest,
    )

    setup_logging()
    rollout = None
    if settings.lab_v2_claim_publication_enabled:
        rollout = FinalizerRolloutStore(
            settings.lab_finalizer_state_dir_resolved / "claim-finalizer-rollout.sqlite3",
            create=False,
        )
        rollout.require_v2_worker_enable()
    worker_id = (args.worker_id or settings.lab_worker_id).strip()
    if worker_id != settings.lab_worker_id:
        raise LabDaemonConfigurationError("worker CLI id does not match configured stable id")
    if worker_id not in settings.lab_scheduler_worker_id_list:
        raise LabDaemonConfigurationError("worker id is not present in scheduler allowlist")
    if settings.lab_worker_max_shards_per_tick != LAB_WORKER_MAX_SHARDS_PER_TICK:
        raise LabDaemonConfigurationError("worker batch must remain exactly one shard per tick")
    resource_admission = _build_lab_worker_resource_admission(
        settings=settings,
        code_sha=code_sha,
        legacy_opt_out=bool(getattr(args, "legacy_no_resource_admission", False)),
    )
    resource_authority_manifest = _build_lab_worker_resource_authority_manifest(
        settings=settings,
        resource_admission=resource_admission,
    )
    shard_runtime_manifest = build_builtin_shard_runtime_manifest(
        catalog_path=settings.research_readonly_db_path_resolved,
        forbidden_paths=(
            settings.duckdb_path,
            settings.duckdb_readonly_path_resolved,
            settings.research_db_path_resolved,
        ),
        snapshot_root=settings.lab_worker_artifact_dir_resolved,
        research_lake_root=settings.research_lake_dir_resolved,
    )

    for label, path in (
        ("lab claim spool", settings.lab_job_claim_dir_resolved),
        ("lab report spool", settings.lab_job_report_dir_resolved),
        ("lab worker artifact root", settings.lab_worker_artifact_dir_resolved),
        ("lab daemon lock root", settings.lab_daemon_lock_dir_resolved),
    ):
        ensure_private_directory(path, label=label, mutation_guard=runtime_identity_guard)
    require_unique_runtime_paths(
        {
            "lab claim spool": settings.lab_job_claim_dir_resolved,
            "lab report spool": settings.lab_job_report_dir_resolved,
            "lab worker artifact root": settings.lab_worker_artifact_dir_resolved,
            "lab daemon lock root": settings.lab_daemon_lock_dir_resolved,
        }
    )
    with LabDaemonLock(
        settings.lab_daemon_lock_dir_resolved,
        "worker",
        mutation_guard=runtime_identity_guard,
    ) as daemon_lock:
        claim_spool = LabClaimSpool(
            settings.lab_job_claim_dir_resolved,
            mutation_guard=runtime_identity_guard,
        )
        publication_verifier = _build_lab_claim_publication_worker_verifier(
            settings=settings,
            ledger=LabJobStore(
                settings.lab_jobs_path_resolved,
                busy_timeout_ms=settings.lab_jobs_busy_timeout_ms,
            ),
            claim_spool=claim_spool,
        )
        if (
            rollout is not None
            and rollout.snapshot().phase is FinalizerRolloutPhase.FINALIZER_READY
        ):
            rollout.transition(
                FinalizerRolloutPhase.V2_WORKERS_READY,
                evidence=f"worker-verified:{worker_id}:{code_sha}",
            )
        worker = LabWorker(
            worker_id=worker_id,
            claim_spool=claim_spool,
            claim_publication_verifier=publication_verifier,  # type: ignore[arg-type]
            v2_claim_publication_enabled=settings.lab_v2_claim_publication_enabled,
            report_spool=LabReportSpool(
                settings.lab_job_report_dir_resolved,
                mutation_guard=runtime_identity_guard,
            ),
            artifact_root=settings.lab_worker_artifact_dir_resolved,
            shard_runtime_manifest=shard_runtime_manifest,
            heartbeat_interval_seconds=settings.lab_worker_heartbeat_seconds,
            lease_extension_seconds=settings.lab_worker_lease_extension_seconds,
            poll_interval_ms=settings.lab_worker_poll_interval_ms,
            receipt_timeout_seconds=settings.lab_worker_receipt_timeout_seconds,
            verified_code_sha_provider=runtime_identity_guard,
            resource_authority_manifest=resource_authority_manifest,
            require_resource_admission=resource_admission.require_resource_admission,
            production_mode=settings.app_env == "prod",
        )
        readiness = _lab_daemon_readiness_context(
            args,
            label="com.roxor.rquant-lab-worker",
            code_sha=code_sha,
            runtime_guard=runtime_guard,
            runtime_identity=runtime_identity,
            daemon_lock=daemon_lock,
        )
        with readiness:
            if args.once:
                result = worker.run_once()
                logger.info(f"lab-worker tick: {result.model_dump_json()}")
                if result.status in {"idle", "succeeded"}:
                    return 0
                if result.status in {"failed", "stopped"}:
                    return 1
                return 2

            def handle_signal(signum: int, frame: object) -> None:
                del frame
                logger.info(f"lab-worker 收到信号 {signum}，请求停止")
                worker.request_stop()

            previous = {
                signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
            }
            for signum in previous:
                signal.signal(signum, handle_signal)
            try:
                worker.run_forever(install_signal_handlers=False)
            finally:
                for signum, handler in previous.items():
                    signal.signal(signum, handler)
            return 0


def cmd_lab_finalizer(args: argparse.Namespace) -> int:
    """Finalize ready Strategy Lab jobs without writable SQLite access."""
    code_sha, runtime_guard, runtime_identity, runtime_identity_guard = (
        _establish_lab_runtime_identity(args)
    )
    from rquant.config import settings
    from rquant.lab_artifact_protocol import LabArtifactCommitSpool
    from rquant.lab_artifacts import LabJobArtifactStore
    from rquant.lab_daemon import (
        LabAuthorityKeyring,
        LabDaemonConfigurationError,
        LabDaemonLock,
        LabFinalizerDaemon,
        LabFinalizerStateStore,
        ensure_private_directory,
        prepare_private_sqlite_path,
        require_unique_runtime_paths,
    )
    from rquant.lab_finalizer import LabFinalizer
    from rquant.lab_jobs import LabJobReader
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    setup_logging()
    if (
        not settings.lab_finalizer_authority_key_id
        or settings.lab_finalizer_authority_key_path is None
        or settings.lab_finalizer_authority_keyring_path is None
    ):
        raise LabDaemonConfigurationError("lab authority key configuration is incomplete")
    keyring = LabAuthorityKeyring.load(
        active_key_id=settings.lab_finalizer_authority_key_id,
        active_key_path=settings.lab_finalizer_authority_key_path,
        verification_keyring_path=settings.lab_finalizer_authority_keyring_path,
    )
    for label, path in (
        ("lab worker artifact root", settings.lab_worker_artifact_dir_resolved),
        ("lab final artifact root", settings.lab_final_artifact_dir_resolved),
        ("lab artifact commit spool", settings.lab_artifact_commit_dir_resolved),
        ("lab daemon lock root", settings.lab_daemon_lock_dir_resolved),
        ("lab finalizer state root", settings.lab_finalizer_state_dir_resolved),
    ):
        ensure_private_directory(path, label=label, mutation_guard=runtime_identity_guard)
    runtime_paths = {
        "lab worker artifact root": settings.lab_worker_artifact_dir_resolved,
        "lab final artifact root": settings.lab_final_artifact_dir_resolved,
        "lab artifact commit spool": settings.lab_artifact_commit_dir_resolved,
        "lab daemon lock root": settings.lab_daemon_lock_dir_resolved,
        "lab finalizer state root": settings.lab_finalizer_state_dir_resolved,
        "lab authority signing key": settings.lab_finalizer_authority_key_path,
        "lab authority keyring": settings.lab_finalizer_authority_keyring_path,
    }
    if os.path.lexists(settings.lab_jobs_path_resolved):
        runtime_paths["lab jobs SQLite"] = settings.lab_jobs_path_resolved
    require_unique_runtime_paths(runtime_paths)
    with LabDaemonLock(
        settings.lab_daemon_lock_dir_resolved,
        "finalizer",
        mutation_guard=runtime_identity_guard,
    ) as daemon_lock:
        sqlite_authority = prepare_private_sqlite_path(
            settings.lab_jobs_path_resolved,
            label="lab jobs SQLite",
            create=False,
            mutation_guard=runtime_identity_guard,
        )
        artifact_store = None
        try:
            artifact_store = LabJobArtifactStore(
                settings.lab_final_artifact_dir_resolved,
                mutation_guard=runtime_identity_guard,
            )
            reader = LabJobReader(
                settings.lab_jobs_path_resolved,
                busy_timeout_ms=settings.lab_jobs_busy_timeout_ms,
                identity_authority=sqlite_authority,
            )
            finalizer = LabFinalizer(
                reader=reader,
                shard_artifact_root=settings.lab_worker_artifact_dir_resolved,
                artifact_store=artifact_store,
                commit_spool=LabArtifactCommitSpool(
                    settings.lab_artifact_commit_dir_resolved,
                    mutation_guard=runtime_identity_guard,
                ),
                verified_code_sha_provider=runtime_identity_guard,
                finalizer_authority_key_provider=keyring.signing_key,
                finalizer_authority_verification_key_provider=keyring.verification_key,
                adapter_registry=default_strategy_job_adapter_registry(),
            )
            daemon = LabFinalizerDaemon(
                reader=reader,
                finalizer=finalizer,
                state_store=LabFinalizerStateStore(settings.lab_finalizer_state_dir_resolved),
                max_jobs_per_tick=settings.lab_finalizer_max_jobs_per_tick,
                poll_interval_ms=settings.lab_finalizer_poll_interval_ms,
                failure_cooldown_seconds=(settings.lab_finalizer_failure_cooldown_seconds),
                failure_cooldown_max_seconds=(settings.lab_finalizer_failure_cooldown_max_seconds),
                runtime_guard=runtime_identity_guard,
            )
            readiness = _lab_daemon_readiness_context(
                args,
                label="com.roxor.rquant-lab-finalizer",
                code_sha=code_sha,
                runtime_guard=runtime_guard,
                runtime_identity=runtime_identity,
                daemon_lock=daemon_lock,
            )
            with readiness:
                if args.once:
                    result = daemon.run_once()
                    logger.info(f"lab-finalizer tick: {result.model_dump_json()}")
                    return 1 if result.failed else 0

                def handle_signal(signum: int, frame: object) -> None:
                    del frame
                    logger.info(f"lab-finalizer 收到信号 {signum}，请求停止")
                    daemon.request_stop()

                previous = {
                    signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
                }
                for signum in previous:
                    signal.signal(signum, handle_signal)
                try:
                    daemon.run_forever()
                finally:
                    for signum, handler in previous.items():
                        signal.signal(signum, handler)
                return 0
        finally:
            try:
                if artifact_store is not None:
                    artifact_store.close()
            finally:
                sqlite_authority.close()


def cmd_lab_claim_finalizer(args: argparse.Namespace) -> int:
    """Run the dedicated authority-owned V2 claim publication finalizer."""

    from rquant.config import settings
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutPhase, FinalizerRolloutStore
    from rquant.lab_daemon import (
        LabDaemonConfigurationError,
        LabDaemonLock,
        ensure_private_directory,
    )

    if not settings.lab_claim_finalizer_enabled:
        raise LabDaemonConfigurationError("claim finalizer is not enabled")
    if settings.lab_claim_finalizer_runtime_material_root is None:
        raise LabDaemonConfigurationError("claim finalizer runtime generation is missing")

    code_sha, runtime_guard, runtime_identity, runtime_identity_guard = (
        _establish_lab_runtime_identity(args)
    )
    setup_logging()
    ensure_private_directory(
        settings.lab_job_claim_dir_resolved,
        label="lab claim spool",
        mutation_guard=runtime_identity_guard,
    )
    ensure_private_directory(
        settings.lab_daemon_lock_dir_resolved,
        label="lab daemon lock root",
        mutation_guard=runtime_identity_guard,
    )
    from rquant.lab_claim_finalizer_composition import (
        compose_production_lab_claim_finalizer_daemon,
    )

    with LabDaemonLock(
        settings.lab_daemon_lock_dir_resolved,
        "claim-finalizer",
        mutation_guard=runtime_identity_guard,
    ) as daemon_lock:
        daemon = compose_production_lab_claim_finalizer_daemon(
            settings=settings,
            mutation_guard=runtime_identity_guard,
        )
        try:
            readiness = _lab_daemon_readiness_context(
                args,
                label="com.roxor.rquant-lab-claim-finalizer",
                code_sha=code_sha,
                runtime_guard=runtime_guard,
                runtime_identity=runtime_identity,
                daemon_lock=daemon_lock,
            )
            with readiness:
                if settings.lab_v2_claim_publication_enabled:
                    rollout = FinalizerRolloutStore(
                        settings.lab_finalizer_state_dir_resolved
                        / "claim-finalizer-rollout.sqlite3",
                        create=False,
                    )
                    if rollout.snapshot().phase is FinalizerRolloutPhase.PREFLIGHT_OK:
                        rollout.transition(
                            FinalizerRolloutPhase.FINALIZER_READY,
                            evidence=f"finalizer-ready:{code_sha}",
                        )
                if args.once:
                    result = daemon.run_once()
                    logger.info(f"lab-claim-finalizer tick: {result.model_dump_json()}")
                    return 1 if result.blocked else 0

                def handle_signal(signum: int, frame: object) -> None:
                    del frame
                    logger.info(f"lab-claim-finalizer received signal {signum}; stopping")
                    daemon.request_stop()

                previous = {
                    signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
                }
                for signum in previous:
                    signal.signal(signum, handle_signal)
                try:
                    daemon.run_forever()
                finally:
                    for signum, handler in previous.items():
                        signal.signal(signum, handler)
                return 0
        finally:
            daemon.close()


def cmd_lab_claim_finalizer_trust(args: argparse.Namespace) -> int:
    """Run the offline-only claim-finalizer certificate ceremony CLI."""

    from rquant.adapter_manifest import Ed25519ContractSigner, Ed25519PublicKeyRecord
    from rquant.lab_claim_finalizer_composition import _OpenSslFileSigningClient
    from rquant.lab_claim_finalizer_runtime import (
        inspect_offline_finalizer_certificate,
        issue_offline_finalizer_certificate,
        load_offline_finalizer_certificate,
        read_offline_finalizer_material,
        write_offline_finalizer_certificate,
    )

    if args.action == "inspect":
        _print_json(
            inspect_offline_finalizer_certificate(
                load_offline_finalizer_certificate(args.certificate)
            )
        )
        return 0
    try:
        not_before = datetime.fromisoformat(args.not_before)
        expires_at = datetime.fromisoformat(args.expires_at)
    except ValueError as exc:
        raise ValueError("certificate timestamps must be ISO-8601") from exc
    if not_before.tzinfo is None or expires_at.tzinfo is None:
        raise ValueError("certificate timestamps must include a UTC offset")
    root_record = Ed25519PublicKeyRecord(
        key_id=args.root_key_id,
        issuer=args.root_issuer,
        key_purpose="lab_claim_finalizer_root",
        rotation="active",
        public_key_pem=read_offline_finalizer_material(args.root_public_key, private=False),
    )
    finalizer_record = Ed25519PublicKeyRecord(
        key_id=args.finalizer_key_id,
        issuer=args.finalizer_issuer,
        key_purpose="lab_claim_finalizer",
        rotation="active",
        public_key_pem=read_offline_finalizer_material(args.finalizer_public_key, private=False),
    )
    root_signer = Ed25519ContractSigner(
        key_id=root_record.key_id,
        issuer=root_record.issuer,
        key_purpose=root_record.key_purpose,
        client=_OpenSslFileSigningClient(
            private_key_path=args.root_private_key,
            public_record=root_record,
            allowed_namespaces=frozenset({"rquant-lab-claim-finalizer-root/v1"}),
        ),
    )
    finalizer_identity = SimpleNamespace(
        key_id=finalizer_record.key_id,
        issuer=finalizer_record.issuer,
        key_purpose=finalizer_record.key_purpose,
        public_key_fingerprint=finalizer_record.public_key_fingerprint,
    )
    certificate = issue_offline_finalizer_certificate(
        root_signer=root_signer,
        finalizer_signer=finalizer_identity,
        store_id=args.store_id,
        database_generation=(args.database_device, args.database_inode),
        schema_version=16,
        not_before=not_before,
        expires_at=expires_at,
    )
    if args.output is not None:
        write_offline_finalizer_certificate(args.output, certificate)
    _print_json(inspect_offline_finalizer_certificate(certificate))
    return 0


def cmd_lab_claim_finalizer_preflight(args: argparse.Namespace) -> int:
    """Collect finalizer readiness from Settings and the current generation only."""

    from rquant.config import settings
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerPreflightCollector,
        FinalizerRolloutPhase,
        FinalizerRolloutStore,
    )

    report = FinalizerPreflightCollector(settings).collect()
    if args.format == "markdown":
        print(report.render_markdown(), end="")
    else:
        _print_json(
            {"status": report.status, "checks": [item.model_dump() for item in report.checks]}
        )
    if args.apply and report.status == "ok":
        state = FinalizerRolloutStore(
            settings.lab_finalizer_state_dir_resolved / "claim-finalizer-rollout.sqlite3",
            create=False,
        )
        if state.snapshot().phase is FinalizerRolloutPhase.MATERIAL_INSTALLED:
            state.transition(
                FinalizerRolloutPhase.PREFLIGHT_OK,
                evidence=hashlib.sha256(report.model_dump_json().encode("utf-8")).hexdigest(),
            )
    return {"ok": 0, "skip": 0, "warn": 1, "fail": 2}[report.status]


def cmd_lab_claim_finalizer_runtime(args: argparse.Namespace) -> int:
    """Install or inspect already-signed finalizer material; this command cannot sign."""

    from rquant.adapter_manifest import Ed25519PublicKeyRecord, VerifyOnlyEd25519Keyring
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRuntimeInstallRequest,
        LabClaimFinalizerGenerationInstaller,
        load_current_lab_claim_finalizer_generation,
        read_offline_finalizer_material,
    )

    def service_identity() -> tuple[int, int]:
        import grp
        import pwd

        try:
            return (pwd.getpwnam(args.service_user).pw_uid, grp.getgrnam(args.service_group).gr_gid)
        except KeyError as exc:
            raise ValueError("service user/group cannot be resolved locally") from exc

    if args.action == "inspect":
        uid, gid = service_identity()
        selected = load_current_lab_claim_finalizer_generation(
            args.runtime_root,
            expected_uid=uid,
            expected_gid=gid,
            trusted_base=args.trusted_base,
        )
        _print_json(
            {
                "generation_id": selected.generation_id,
                "runtime_material": str(selected.runtime_material_path),
                "worker_verifier": str(selected.worker_verifier_path),
                "store_id": selected.manifest.store_id,
            }
        )
        return 0
    request = FinalizerRuntimeInstallRequest.model_validate_json(
        read_offline_finalizer_material(args.request, private=False)
    )
    root_record = Ed25519PublicKeyRecord(
        key_id=args.root_key_id,
        issuer=args.root_issuer,
        key_purpose="lab_claim_finalizer_root",
        rotation="active",
        public_key_pem=read_offline_finalizer_material(args.root_public_key, private=False),
    )
    root_keyring = VerifyOnlyEd25519Keyring(
        records=(root_record,),
        issuer_allowlist={"lab_claim_finalizer_root": frozenset({root_record.issuer})},
        rotation_allowlist={
            (root_record.issuer, "lab_claim_finalizer_root"): frozenset({root_record.key_id})
        },
    )
    finalizer = request.finalizer_public_key
    finalizer_keyring = VerifyOnlyEd25519Keyring(
        records=(finalizer,),
        issuer_allowlist={"lab_claim_finalizer": frozenset({finalizer.issuer})},
        rotation_allowlist={
            (finalizer.issuer, "lab_claim_finalizer"): frozenset({finalizer.key_id})
        },
    )
    receipt = LabClaimFinalizerGenerationInstaller(
        runtime_root=args.runtime_root,
        root_keyring=root_keyring,
        finalizer_keyring=finalizer_keyring,
        expected_uid=service_identity()[0],
        expected_gid=service_identity()[1],
        trusted_base=args.trusted_base,
    ).install(request, dry_run=args.dry_run)
    _print_json(receipt.model_dump(mode="json"))
    return 0


def cmd_lab_claim_finalizer_rollout(args: argparse.Namespace) -> int:
    """Operate the audited V2 drain gate without mutating publications."""

    from rquant.config import settings
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutStore
    from rquant.lab_jobs import LabJobStore

    state = FinalizerRolloutStore(
        settings.lab_finalizer_state_dir_resolved / "claim-finalizer-rollout.sqlite3",
        create=False,
    )
    if args.action == "status":
        _print_json(state.snapshot().model_dump(mode="json"))
        return 0
    if args.action == "begin-drain":
        _print_json(state.begin_rollback(evidence=args.evidence).model_dump(mode="json"))
        return 0
    job_store = LabJobStore(settings.lab_jobs_path_resolved)
    _print_json(
        state.complete_drain(evidence=args.evidence, job_store=job_store).model_dump(mode="json")
    )
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
            "RQUANT_PANORAMA_GATE_TOKEN / RQUANT_PANORAMA_COOKIE_SECRET 均未配置，无法生成网关令牌",
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


def _add_formal_runtime_bootstrap_arguments(parser: argparse.ArgumentParser) -> None:
    from rquant.formal_runtime_command import add_formal_runtime_bootstrap_arguments

    add_formal_runtime_bootstrap_arguments(parser)


def _add_formal_runtime_deployment_arguments(parser: argparse.ArgumentParser) -> None:
    from rquant.formal_runtime_command import add_formal_runtime_deployment_arguments

    add_formal_runtime_deployment_arguments(parser)


def cmd_runtime_code(args: argparse.Namespace) -> int:
    """Run explicit offline packaging and privileged immutable-generation operations."""

    from rquant.runtime_code_operations import (
        RUNTIME_CODE_EXIT_INVALID,
        RUNTIME_CODE_EXIT_OK,
        RuntimeCodeMigrationRequest,
        RuntimeCodeOperationError,
        RuntimeCodePackageCeremonyRequest,
        RuntimeCodeRotateCeremonyRequest,
        compose_runtime_code_generation_operator,
        load_runtime_code_bootstrap_configuration,
        load_runtime_code_operation_request,
        offline_contract_signer,
        stable_runtime_code_error_payload,
    )

    def emit(payload: object) -> None:
        if args.format == "json":
            _print_json(payload)
            return
        if isinstance(payload, dict):
            status = payload.get("status", "unknown")
            action = payload.get("action", args.action)
            print(f"runtime-code {action}: {status}")
            result = payload.get("result")
            if isinstance(result, dict):
                for key in (
                    "generation_id",
                    "promotion_sequence",
                    "previous_generation_id",
                    "write_performed",
                    "external_promotion_required",
                ):
                    if key in result:
                        print(f"{key}: {result[key]}")
            message = payload.get("message")
            if isinstance(message, str):
                print(message, file=sys.stderr)

    try:
        trusted_base = Path(args.runtime_code_trusted_base)
        configuration = load_runtime_code_bootstrap_configuration(
            Path(args.runtime_code_config),
            trusted_base=trusted_base,
            expected_uid=int(args.runtime_code_authority_uid),
            expected_gid=int(args.runtime_code_authority_gid),
        )
        operator = compose_runtime_code_generation_operator(
            configuration,
            offline=args.action in {"package", "rotate"},
        )
        if args.action == "inspect":
            result = operator.inspect()
        else:
            request_path = Path(args.request)
            request_model = {
                "package": RuntimeCodePackageCeremonyRequest,
                "install": RuntimeCodeMigrationRequest,
                "rotate": RuntimeCodeRotateCeremonyRequest,
                "dry-run": RuntimeCodeMigrationRequest,
            }[args.action]
            request = load_runtime_code_operation_request(
                request_path,
                request_model,
                trusted_base=trusted_base,
                expected_uid=int(args.runtime_code_authority_uid),
                expected_gid=int(args.runtime_code_authority_gid),
            )
            if isinstance(request, RuntimeCodePackageCeremonyRequest):
                runtime_records = tuple(
                    record
                    for record in configuration.runtime_keys
                    if record.key_id == request.runtime_key_id
                )
                if len(runtime_records) != 1:
                    raise RuntimeCodeOperationError("runtime code package signer is not pinned")
                runtime_signer = offline_contract_signer(
                    private_key_path=request.runtime_private_key_path,
                    public_record=runtime_records[0],
                    expected_uid=int(args.runtime_code_authority_uid),
                    expected_gid=int(args.runtime_code_authority_gid),
                )
                promotion_signer = offline_contract_signer(
                    private_key_path=request.promotion_private_key_path,
                    public_record=configuration.promotion_key,
                    expected_uid=int(args.runtime_code_authority_uid),
                    expected_gid=int(args.runtime_code_authority_gid),
                )
                result = operator.package(
                    request.package,
                    runtime_signer=runtime_signer,
                    promotion_signer=promotion_signer,
                )
            elif isinstance(request, RuntimeCodeRotateCeremonyRequest):
                promotion_signer = offline_contract_signer(
                    private_key_path=request.promotion_private_key_path,
                    public_record=configuration.promotion_key,
                    expected_uid=int(args.runtime_code_authority_uid),
                    expected_gid=int(args.runtime_code_authority_gid),
                )
                result = operator.rotate(request.rotation, promotion_signer=promotion_signer)
            elif isinstance(request, RuntimeCodeMigrationRequest):
                if request.expected_configuration_path != Path(args.runtime_code_config):
                    raise RuntimeCodeOperationError(
                        "runtime code migration configuration path does not match CLI"
                    )
                if request.expected_trusted_base != trusted_base:
                    raise RuntimeCodeOperationError(
                        "runtime code migration trusted base does not match CLI"
                    )
                if request.expected_authority_uid != int(
                    args.runtime_code_authority_uid
                ) or request.expected_authority_gid != int(args.runtime_code_authority_gid):
                    raise RuntimeCodeOperationError(
                        "runtime code migration authority identity does not match CLI"
                    )
                result = (
                    operator.dry_run(request)
                    if args.action == "dry-run"
                    else operator.install(request)
                )
            else:
                raise RuntimeCodeOperationError("runtime code operation request type is invalid")
        emit(
            {
                "action": args.action,
                "exit_code": RUNTIME_CODE_EXIT_OK,
                "result": result.model_dump(mode="json"),
                "status": "ok",
            }
        )
        return RUNTIME_CODE_EXIT_OK
    except RuntimeCodeOperationError as exc:
        emit(stable_runtime_code_error_payload(args.action, exc))
        return exc.exit_code
    except (OSError, TypeError, ValueError) as exc:
        error = RuntimeCodeOperationError(
            "runtime code operation is unavailable",
            exit_code=RUNTIME_CODE_EXIT_INVALID,
        )
        error.__cause__ = exc
        emit(stable_runtime_code_error_payload(args.action, error))
        return error.exit_code


def build_parser() -> argparse.ArgumentParser:
    """构建 CLI 参数解析器。"""
    parser = _RQuantArgumentParser(prog="rquant", description="rQuant 量化选股平台")
    sub = parser.add_subparsers(dest="command")

    runtime_code_p = sub.add_parser(
        "runtime-code",
        help="package, verify, and atomically install formal runtime generations",
    )
    runtime_code_sub = runtime_code_p.add_subparsers(dest="action", required=True)
    for action in ("package", "install", "rotate", "inspect", "dry-run"):
        runtime_code_action = runtime_code_sub.add_parser(action)
        _add_formal_runtime_bootstrap_arguments(runtime_code_action)
        runtime_code_action.add_argument("--format", choices=("json", "text"), default="text")
        if action != "inspect":
            runtime_code_action.add_argument("--request", type=Path, required=True)

    serve_p = sub.add_parser("serve", help="启动 APScheduler 常驻进程")
    serve_p.add_argument("--hour", type=int, default=17, help="每日触发小时 (默认 17)")

    run_p = sub.add_parser("run-daily", help="拉取数据 + 执行全流水线")
    run_p.add_argument(
        "--date",
        type=str,
        default=None,
        help="交易日期 YYYY-MM-DD (默认今天)",
    )
    run_p.add_argument(
        "--preset",
        type=str,
        default=None,
        help="只跑指定预设 (默认全部)",
    )
    run_p.add_argument(
        "--no-ingest",
        action="store_true",
        help="跳过数据拉取，只跑筛选",
    )
    run_p.add_argument(
        "--skip-minute-backfill",
        action="store_true",
        help="跳过日终 Pool1 90 日分钟上下文回补",
    )
    run_p.add_argument(
        "--minute-lookback-days",
        type=int,
        default=90,
        help="日终分钟上下文回补交易日数量 (默认 90)",
    )

    ingest_p = sub.add_parser("ingest", help="仅拉取数据（不跑筛选）")
    ingest_p.add_argument(
        "--date",
        type=str,
        default=None,
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

    daily_shadow_p = sub.add_parser(
        "daily-dag-shadow",
        help="只读检查新 daily DAG 的签名影子对账与退休门，不改变旧 daily 权威",
    )
    daily_shadow_p.add_argument(
        "--report-root",
        type=Path,
        required=True,
        help="daily DAG 影子报告根目录",
    )
    daily_shadow_p.add_argument(
        "--expected-trade-date",
        type=_parse_iso_date,
        action="append",
        required=True,
        help="权威交易日（可重复），用于检查连续真实影子证据",
    )
    daily_shadow_p.add_argument(
        "--minimum-real-trading-days",
        type=int,
        default=10,
        help="退休门最少真实交易日（生产不得低于 10）",
    )
    daily_shadow_p.add_argument("--key-id", default="daily-shadow-v1", help="报告签名 key id")
    daily_shadow_p.add_argument(
        "--signing-key-env",
        default="RQUANT_DAILY_SHADOW_SIGNING_KEY",
        help="签名密钥环境变量名",
    )
    daily_shadow_p.add_argument(
        "--calendar-path",
        type=Path,
        default=None,
        help="SSE 交易日历 authority（retirement-gate 必填）",
    )
    daily_shadow_p.add_argument(
        "--calendar-commit",
        default=None,
        help="calendar authority 绑定的 40 位 code commit（retirement-gate 必填）",
    )
    daily_shadow_p.add_argument(
        "--action",
        choices=("status", "retirement-gate"),
        default="status",
        help="只读操作（默认 status）",
    )

    daily_dag_p = sub.add_parser(
        "daily-dag",
        help="从已安装不可变 production profile 预演或受控推进 daily-close DAG",
    )
    daily_dag_dev_p = sub.add_parser(
        "daily-dag-dev",
        help="仅供非生产测试使用的 shadow daily-close DAG",
    )
    daily_dag_dev_p.add_argument(
        "--profile-root",
        type=Path,
        required=True,
        help="开发测试根目录；生产入口不接受自由根",
    )
    for daily_parser in (daily_dag_p, daily_dag_dev_p):
        daily_parser.add_argument("--trade-date", type=_parse_iso_date, required=True)
        daily_parser.add_argument(
            "--source-generation-id",
            type=_parse_sha256,
            required=True,
        )
        daily_parser.add_argument(
            "--source-content-hash",
            type=_parse_sha256,
            required=True,
        )
        daily_parser.add_argument("--code-commit", type=_parse_commit_sha, required=True)
        daily_parser.add_argument("--profile-hash", type=_parse_sha256, required=True)
        daily_parser.add_argument(
            "--command-manifest-hash",
            type=_parse_sha256,
            required=True,
            help="已审核 stage command manifest 的内容哈希；绑定进 run/effect/receipt",
        )
        daily_parser.add_argument(
            "--source-spool-root",
            type=Path,
            default=None,
            help="daily_close immutable spool root（执行动作必填）",
        )
        daily_parser.add_argument(
            "--service-owner",
            default="daily-close",
            help="daily ledger stable service owner",
        )
        daily_parser.add_argument("--deadline-at", type=_parse_iso_datetime, default=None)
        daily_parser.add_argument(
            "--action",
            choices=("preview", "status", "apply", "retry", "recover"),
            default="preview",
        )
        daily_parser.add_argument(
            "--run-id",
            default=None,
            help="apply/retry/recover 的预览 run id",
        )
        daily_parser.add_argument("--plan-hash", type=_parse_sha256, default=None)
        daily_parser.add_argument(
            "--apply",
            action="store_true",
            help="确认执行非 preview 动作；没有已安装 adapter manifest 时 fail closed",
        )
    daily_dag_p.add_argument(
        "--confirm-production",
        action="store_true",
        help="执行 production DAG 的第二确认",
    )

    monitor_p = sub.add_parser("monitor", help="启动盘中实时监控")
    monitor_p.add_argument(
        "--interval",
        type=int,
        default=5,
        help="轮询间隔秒数 (默认 5)",
    )
    legacy_shadow_recover_p = sub.add_parser(
        "legacy-shadow-recover",
        help="仅恢复窗口内已签名的完整 legacy-shadow staging",
    )
    legacy_shadow_recover_p.add_argument(
        "--source",
        required=True,
        choices=("monitor", "surge", "isolated-runners"),
    )
    legacy_shadow_recover_p.add_argument("--date", required=True)

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
        "--backup",
        type=str,
        default=None,
        help="云端备份文件路径（默认 data/cloud_backup.duckdb）",
    )
    rs_p.add_argument(
        "--restore-from",
        type=str,
        default=None,
        help="恢复模式：从指定旧库/旧副本按主键合并研究表",
    )
    rs_p.add_argument(
        "--tables",
        type=str,
        default=None,
        help="恢复模式下只处理这些表（逗号分隔，默认全部研究表）",
    )
    rs_p.add_argument(
        "--no-refresh-replica",
        action="store_true",
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

    research_repair_auction_p = sub.add_parser(
        "research-repair-auction",
        help="真实取数生成计划，或按 plan id 原子修复历史集合竞价研究分区",
    )
    research_repair_auction_p.add_argument(
        "--date",
        type=_parse_iso_date,
        action="append",
        required=True,
        help="目标历史交易日 YYYY-MM-DD；可重复传入多个日期",
    )
    research_repair_auction_p.add_argument(
        "--apply",
        action="store_true",
        help="执行已确认的批次修复（必须同时传 --plan-id）",
    )
    research_repair_auction_p.add_argument(
        "--plan-id",
        type=_parse_sha256,
        default=None,
        help="预演输出的 64 位 plan id（必须同时传 --apply）",
    )

    research_repair_minute_p = sub.add_parser(
        "research-repair-minute",
        help="按已完成回补 manifest 预演或原子修复历史分钟研究分区",
    )
    research_repair_minute_p.add_argument(
        "--manifest-id",
        type=_parse_sha256,
        required=True,
        help="已完成的分钟回补 manifest id",
    )
    research_repair_minute_p.add_argument(
        "--apply",
        action="store_true",
        help="执行已确认的批次修复（必须同时传 --plan-id）",
    )
    research_repair_minute_p.add_argument(
        "--plan-id",
        type=_parse_sha256,
        default=None,
        help="预演输出的 64 位 plan id（必须同时传 --apply）",
    )

    formal_smoke_p = sub.add_parser(
        "formal-smoke-replay",
        help="用精确审计、快照和绑定运行三策略固定正式冒烟回放",
    )
    formal_smoke_p.add_argument(
        "--strategy",
        required=True,
        choices=("n_shape", "growth_board_surge", "auction_gap"),
        help="固定策略标识",
    )
    formal_smoke_p.add_argument(
        "--start-date",
        required=True,
        type=_parse_iso_date,
        help="正式回放开始日期 YYYY-MM-DD",
    )
    formal_smoke_p.add_argument(
        "--end-date",
        required=True,
        type=_parse_iso_date,
        help="正式回放结束日期 YYYY-MM-DD",
    )
    formal_smoke_p.add_argument(
        "--audit-run-id",
        required=True,
        type=_parse_sha256,
        help="覆盖完整区间的数据审计 run id",
    )
    formal_smoke_p.add_argument(
        "--snapshot-id",
        required=True,
        type=_parse_sha256,
        help="ready dataset snapshot id",
    )
    formal_smoke_p.add_argument(
        "--binding-hash",
        required=True,
        type=_parse_sha256,
        help="ready immutable execution binding hash",
    )
    formal_smoke_p.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Strategy Lab 记录根目录（默认使用配置 data_dir）",
    )
    formal_smoke_p.add_argument(
        "--execution-timeout-seconds",
        type=_parse_formal_smoke_timeout_seconds,
        default=3600.0,
        help=argparse.SUPPRESS,
    )
    _add_formal_runtime_bootstrap_arguments(formal_smoke_p)

    formal_smoke_runtime_p = sub.add_parser(
        "formal-smoke-runtime-execute",
        help=argparse.SUPPRESS,
    )
    formal_smoke_runtime_p.add_argument("--request-fd", type=int, required=True)
    formal_smoke_runtime_p.add_argument("--receipt-fd", type=int, required=True)

    stage1_acceptance_p = sub.add_parser(
        "stage1-acceptance",
        help="只读预演一个策略的 Stage 1 验收身份、状态与资源预算",
    )
    stage1_acceptance_p.add_argument(
        "--strategy",
        required=True,
        choices=("n_shape", "growth_board_surge", "auction_gap"),
        help="本次唯一验收的策略",
    )
    stage1_acceptance_p.add_argument(
        "--manifest-id",
        required=True,
        type=_parse_sha256,
        help="该策略精确的 completed 或 abandoned manifest id",
    )
    stage1_acceptance_p.add_argument(
        "--start-date",
        required=True,
        type=_parse_iso_date,
        help="必须与 manifest 一致的资格开始日期",
    )
    stage1_acceptance_p.add_argument(
        "--end-date",
        required=True,
        type=_parse_iso_date,
        help="必须与 manifest 一致的资格结束日期",
    )
    stage1_acceptance_p.add_argument(
        "--expected-code-commit",
        required=True,
        help="当前 checkout 的 40 位 commit；保留策略还必须与 manifest 一致",
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
        "--start-date",
        type=str,
        required=True,
        help="开始日期 YYYY-MM-DD",
    )
    market_backfill_p.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="结束日期 YYYY-MM-DD",
    )
    market_backfill_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只报告交易日数与预计请求数，不调 Tushare、不写库",
    )
    market_backfill_p.add_argument(
        "--skip-state-recompute",
        dest="skip_state_recompute",
        action="store_true",
        help=("跳过最终 daily_state 原子尾段重算；受影响的状态和日指标尾段仍会保持失效"),
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
        "--date",
        type=str,
        default=None,
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
        "--start-date",
        type=str,
        default=None,
        help="开始日期 YYYY-MM-DD",
    )
    limit_list_p.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="结束日期 YYYY-MM-DD",
    )
    limit_list_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只报告交易日数与预计请求数，不调 Tushare、不写库",
    )
    limit_list_p.add_argument(
        "--today",
        action="store_true",
        help="只拉当天（日终增量），失败不炸、幂等可重跑",
    )

    data_backfill_p = sub.add_parser(
        "data-backfill",
        help="统一数据集回补（板块行情/成分/资金流/龙虎榜/开盘啦等，"
        "注册表见 rquant.dataset_backfill.DATASETS）",
    )
    data_backfill_p.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="数据集名（Tushare 接口名，如 ths_daily / moneyflow_dc / top_list），all 跑全部",
    )
    data_backfill_p.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="开始日期 YYYY-MM-DD（snapshot 数据集忽略）",
    )
    data_backfill_p.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="结束日期 YYYY-MM-DD（snapshot 数据集取该日往前最近交易日）",
    )
    data_backfill_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只报告交易日数与预计请求数，不调 Tushare、不写库",
    )
    data_backfill_p.add_argument(
        "--today",
        action="store_true",
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
        type=_parse_iso_date,
        default=None,
        help="候选结束日期 YYYY-MM-DD；省略时自动取完整 B/S 窗口可观测上限",
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
    backfill_run_p.add_argument(
        "--workers",
        type=int,
        choices=range(1, 17),
        default=8,
        metavar="1..16",
        help="并发 Tushare 拉取 worker 数；DuckDB 仍保持单写（默认 8）",
    )
    backfill_run_p.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=None,
        help="本次最多运行分钟数；实际仍会在下一交易保护窗口前 10 分钟停止",
    )
    backfill_run_p.add_argument(
        "--stop-before",
        type=_parse_iso_datetime,
        default=None,
        help=argparse.SUPPRESS,
    )
    backfill_run_p.add_argument(
        "--deadline-worker",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    backfill_abandon_p = sub.add_parser(
        "backfill-abandon",
        help="dry-run 后将已退役的回补 manifest 标记为可审计终态",
    )
    backfill_abandon_p.add_argument(
        "--manifest-id",
        required=True,
        type=_parse_sha256,
        help="64 位 manifest id",
    )
    backfill_abandon_p.add_argument(
        "--reason",
        required=True,
        help="停止该回补任务的非空业务原因",
    )
    backfill_abandon_p.add_argument(
        "--plan-id",
        type=_parse_sha256,
        default=None,
        help="apply 时必须传入 dry-run 返回的精确 plan id",
    )
    backfill_abandon_p.add_argument(
        "--apply",
        action="store_true",
        help="按精确 plan id 写入 abandoned 终态；默认仅 dry-run",
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
    suspension_backfill_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只输出权威开市日和精确刷新日期，不请求 Tushare 或写库",
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

    minute_p = sub.add_parser("minute-backfill", help="回补 Pool 命中标的历史分钟线")
    minute_p.add_argument(
        "--date",
        type=str,
        required=True,
        help="Pool 筛选日期 YYYY-MM-DD",
    )
    minute_p.add_argument(
        "--lookback-days",
        type=int,
        default=90,
        help="向前回补交易日数量 (默认 90)",
    )
    minute_p.add_argument(
        "--freq",
        type=str,
        default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    minute_p.add_argument(
        "--preset",
        type=str,
        default="n-shape-pool1",
        help="筛选 preset (默认 n-shape-pool1)",
    )
    minute_p.add_argument(
        "--ts-code",
        type=str,
        default=None,
        help="只回补单只股票，调试用",
    )
    minute_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只估算请求数，不调用 Tushare、不写库",
    )

    replay_p = sub.add_parser("minute-replay", help="基于历史分钟线跑强承接/突破模拟回放")
    replay_p.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Pool 筛选开始日期 YYYY-MM-DD",
    )
    replay_p.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="Pool 筛选结束日期 YYYY-MM-DD",
    )
    replay_p.add_argument(
        "--preset",
        type=str,
        default="n-shape-pool1",
        help="筛选 preset (默认 n-shape-pool1)",
    )
    replay_p.add_argument(
        "--freq",
        type=str,
        default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    replay_p.add_argument(
        "--entry-mode",
        type=str,
        default="first_break",
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
        "--factor-score-threshold",
        type=float,
        default=35.0,
        help="factor_confirm 的 n_shape_b_v1 评分入场阈值，仅该模式生效 (默认 35)",
    )
    replay_p.add_argument(
        "--max-hold-days",
        type=int,
        default=5,
        help="最多持有交易日数量 (默认 5)",
    )
    replay_p.add_argument(
        "--volume-profile",
        action="store_true",
        help="启用 90 日价量分布入场过滤与动态风控",
    )
    replay_p.add_argument(
        "--volume-profile-lookbacks",
        type=int,
        nargs="+",
        default=[90],
        help="价量分布 lookback 交易日列表 (默认 90)",
    )
    replay_p.add_argument(
        "--output",
        type=str,
        default=None,
        help="CSV 输出路径（可选）",
    )

    growth_replay_p = sub.add_parser(
        "growth-board-surge-replay",
        help="回测科创/创业板盘中放量追击策略",
    )
    growth_replay_p.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="回测开始日期 YYYY-MM-DD",
    )
    growth_replay_p.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="回测结束日期 YYYY-MM-DD",
    )
    growth_replay_p.add_argument(
        "--freq",
        type=str,
        default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    growth_replay_p.add_argument(
        "--min-signal-time",
        type=str,
        default="09:30",
        help="最早 B 信号时间 HH:MM (默认 09:33)",
    )
    growth_replay_p.add_argument(
        "--lookback-days",
        type=int,
        default=20,
        help="分钟历史基准 lookback 交易日数量 (默认 20)",
    )
    growth_replay_p.add_argument(
        "--min-hist-days",
        type=int,
        default=10,
        help="至少需要的历史分钟样本交易日数量 (默认 10)",
    )
    growth_replay_p.add_argument(
        "--min-cum-amount-ratio",
        type=float,
        default=1.4,
        help="截至当前累计成交额相对历史同时间中位数倍数 (默认 1.4)",
    )
    growth_replay_p.add_argument(
        "--min-same-minute-amount-ratio",
        type=float,
        default=2.0,
        help="当前分钟成交额相对历史同分钟中位数倍数 (默认 2.0)",
    )
    growth_replay_p.add_argument(
        "--max-hold-days",
        type=int,
        default=3,
        help="最多持有交易日数量 (默认 3；接住 2-3 日延续涨幅，见退出结构报告)",
    )
    growth_replay_p.add_argument(
        "--require-inner-outer",
        action="store_true",
        help="要求信号分钟外盘>内盘（分钟 tick-rule 近似）",
    )
    growth_replay_p.add_argument(
        "--max-inner-outer-ratio",
        "--min-inner-outer-ratio",
        dest="max_inner_outer_ratio",
        type=float,
        default=1.0,
        help="内盘/外盘比上限，须严格小于 (默认 1.0 即外盘>内盘；旧参数名仍兼容)",
    )
    growth_replay_p.add_argument(
        "--require-large-net-vol",
        action="store_true",
        help="要求 T-1 moneyflow 大单净量>阈值（用户条件 3，T 日盘中不可知）",
    )
    growth_replay_p.add_argument(
        "--min-large-net-vol",
        type=float,
        default=0.0,
        help="T-1 大单净量下限，须严格大于 (默认 0)",
    )
    growth_replay_p.add_argument(
        "--require-fresh-surge",
        action="store_true",
        help="首爆过滤：放量当天之前 N 日没放量过（经典量比口径，用户条件）",
    )
    growth_replay_p.add_argument(
        "--fresh-lookback-days",
        type=int,
        default=5,
        help="首爆回看交易日数 (默认 5)",
    )
    growth_replay_p.add_argument(
        "--min-listing-trading-days",
        type=int,
        default=0,
        help="不做新股：上市不满 N 个交易日过滤 (默认 0=关闭；推荐 180)",
    )
    growth_replay_p.add_argument(
        "--require-board-favor",
        action="store_true",
        help="板块集合竞价强度闸门：候选票所在题材当日竞价整体达标才入场",
    )
    growth_replay_p.add_argument(
        "--min-board-gap-up-ratio",
        type=float,
        default=0.5,
        help="板块竞价高开占比下限 (默认 0.5)",
    )
    growth_replay_p.add_argument(
        "--min-board-auction-amount-ratio",
        type=float,
        default=1.0,
        help="板块竞价总额相对历史中位下限 (默认 1.0)",
    )
    growth_replay_p.add_argument(
        "--board-hist-days",
        type=int,
        default=3,
        help="板块竞价额历史比较窗口天数 (默认 3；短窗口抓当下资金青睐)",
    )
    growth_replay_p.add_argument(
        "--factor-confirm",
        action="store_true",
        help="启用 growth_surge_b_v1 多因子评分确认层（宽门不动，评分过阈值才入场）",
    )
    growth_replay_p.add_argument(
        "--factor-score-threshold",
        type=float,
        default=45.0,
        help="factor_confirm 评分入场阈值，仅 --factor-confirm 时生效 (默认 45)",
    )
    growth_replay_p.add_argument(
        "--output",
        type=str,
        default=None,
        help="CSV 输出路径（可选）",
    )

    replay_backfill_p = sub.add_parser(
        "minute-replay-backfill",
        help="回补分钟 replay 所需的 B 日到退出窗口分钟线",
    )
    replay_backfill_p.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Pool 筛选开始日期 YYYY-MM-DD",
    )
    replay_backfill_p.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="Pool 筛选结束日期 YYYY-MM-DD",
    )
    replay_backfill_p.add_argument(
        "--preset",
        type=str,
        default="n-shape-pool1",
        help="筛选 preset (默认 n-shape-pool1)",
    )
    replay_backfill_p.add_argument(
        "--freq",
        type=str,
        default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    replay_backfill_p.add_argument(
        "--max-hold-days",
        type=int,
        default=5,
        help="最多持有交易日数量 (默认 5)",
    )
    replay_backfill_p.add_argument(
        "--ts-code",
        type=str,
        default=None,
        help="只回补单只股票，调试用",
    )
    replay_backfill_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只估算请求数，不调用 Tushare、不写库",
    )

    auction_p = sub.add_parser(
        "auction-backfill",
        help="回补 Tushare 集合竞价数据",
    )
    auction_p.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_p.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_p.add_argument(
        "--dry-run",
        action="store_true",
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
        "--start-date",
        type=str,
        required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_gap_p.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_gap_p.add_argument(
        "--gap-mode",
        type=str,
        default="close",
        choices=["close", "strict_high"],
        help="跳空定义：close=竞价价高于昨收；strict_high=竞价价高于昨高",
    )
    auction_gap_p.add_argument(
        "--st-filter",
        type=str,
        default="case_insensitive",
        choices=["case_insensitive", "literal_lower", "none"],
        help="ST 过滤：默认大小写不敏感过滤 ST/*ST",
    )
    auction_gap_p.add_argument(
        "--min-ratio",
        type=float,
        default=0.15,
        help="竞价量/近5日均量下限 (默认 0.15)",
    )
    auction_gap_p.add_argument(
        "--max-ratio",
        type=float,
        default=5.0,
        help="竞价量/近5日均量上限 (默认 5)",
    )
    auction_gap_p.add_argument(
        "--output",
        type=str,
        default=None,
        help="CSV 输出路径（可选）",
    )

    auction_gap_minute_p = sub.add_parser(
        "auction-gap-minute-replay",
        help="回测集合竞价候选 + 分钟 B/S 策略",
    )
    auction_gap_minute_p.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_gap_minute_p.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_gap_minute_p.add_argument(
        "--gap-mode",
        type=str,
        default="close",
        choices=["close", "strict_high"],
        help="跳空定义：close=竞价价高于昨收；strict_high=竞价价高于昨高",
    )
    auction_gap_minute_p.add_argument(
        "--st-filter",
        type=str,
        default="case_insensitive",
        choices=["case_insensitive", "literal_lower", "none"],
        help="ST 过滤：默认大小写不敏感过滤 ST/*ST",
    )
    auction_gap_minute_p.add_argument(
        "--min-ratio",
        type=float,
        default=0.15,
        help="竞价量/近5日均量下限 (默认 0.15)",
    )
    auction_gap_minute_p.add_argument(
        "--max-ratio",
        type=float,
        default=5.0,
        help="竞价量/近5日均量上限 (默认 5)",
    )
    auction_gap_minute_p.add_argument(
        "--max-hold-days",
        type=int,
        default=1,
        help="最多持有交易日数量 (默认 1)",
    )
    auction_gap_minute_p.add_argument(
        "--seal-hold-days",
        type=int,
        default=None,
        help="封板质量达标仓位的持有上限（交易日）；不传保持关闭（全部 T+1）",
    )
    auction_gap_minute_p.add_argument(
        "--seal-hold-max-open-times",
        type=int,
        default=0,
        help="seal_hold 允许的最大开板次数（官方 limit_list_daily.open_times，默认 0）",
    )
    auction_gap_minute_p.add_argument(
        "--factor-score-threshold",
        type=float,
        default=None,
        help="分钟 B 确认的 auction_gap_b_v1 评分阈值（不传=现状不评分；判死复核用）",
    )
    auction_gap_minute_p.add_argument(
        "--output",
        type=str,
        default=None,
        help="CSV 输出路径（可选）",
    )
    auction_gap_minute_p.add_argument(
        "--persist-positions",
        action="store_true",
        help="模拟仓落库（run_mode=replay，带信号溯源；写主库，盘中会撞 monitor 写锁）",
    )
    auction_gap_minute_p.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="落库批次标识（不传自动生成；可按 run_id 整批清理）",
    )

    auction_gap_minute_backfill_p = sub.add_parser(
        "auction-gap-minute-backfill",
        help="回补集合竞价跳空候选的分钟 replay 窗口",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--gap-mode",
        type=str,
        default="close",
        choices=["close", "strict_high"],
        help="跳空定义：close=竞价价高于昨收；strict_high=竞价价高于昨高",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--st-filter",
        type=str,
        default="case_insensitive",
        choices=["case_insensitive", "literal_lower", "none"],
        help="ST 过滤：默认大小写不敏感过滤 ST/*ST",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--min-ratio",
        type=float,
        default=0.15,
        help="竞价量/近5日均量下限 (默认 0.15)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--max-ratio",
        type=float,
        default=5.0,
        help="竞价量/近5日均量上限 (默认 5)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--max-hold-days",
        type=int,
        default=1,
        help="最多持有交易日数量 (默认 1)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--freq",
        type=str,
        default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--ts-code",
        type=str,
        default=None,
        help="只回补单只股票，调试用",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="窗口起点向前扩 N 个交易日（相对放量特征需要信号日前的历史分钟，默认 0）",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只估算请求数，不调用 Tushare、不写库",
    )

    sentiment_p = sub.add_parser(
        "sentiment-recompute",
        help="重算市场情绪/温度指标（market_sentiment_daily，含 60 日新高占比等）",
    )
    sentiment_p.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="开始日期 YYYY-MM-DD",
    )
    sentiment_p.add_argument(
        "--end-date",
        type=str,
        required=True,
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
        "--label",
        type=str,
        default="430黑名单",
        help="名单标签 (默认 430黑名单)",
    )
    bl_imp.add_argument(
        "--validity",
        type=int,
        default=365,
        help="有效期天数 (默认 365)",
    )

    bl_load = bl_sub.add_parser("load-parquet", help="从 parquet 加载到 DuckDB（云端推送后用）")
    bl_load.add_argument("parquet", type=str, help="parquet 文件路径")
    bl_load.add_argument(
        "--label",
        type=str,
        default=None,
        help="只替换该 label 的行（默认全表覆盖）",
    )

    bl_exp = bl_sub.add_parser("export-parquet", help="导出黑名单到 parquet（mac 端推云前用）")
    bl_exp.add_argument(
        "--output",
        type=str,
        default="data/risk_blacklist.parquet",
        help="输出路径 (默认 data/risk_blacklist.parquet)",
    )
    bl_exp.add_argument(
        "--label",
        type=str,
        default=None,
        help="只导出该 label（默认全表导出）",
    )

    bl_ls = bl_sub.add_parser("list", help="列出黑名单")
    bl_ls.add_argument("--label", type=str, default=None, help="过滤 label")
    bl_ls.add_argument("--include-expired", action="store_true", help="包含已过期条目")

    bl_chk = bl_sub.add_parser("check", help="查询某只股票是否在黑名单")
    bl_chk.add_argument("ts_code", type=str, help="股票代码 (如 600340.SH)")

    bl_rm = bl_sub.add_parser("remove", help="删除整个 list_label")
    bl_rm.add_argument("--label", type=str, required=True, help="要删除的 label")

    sub.add_parser("notify-test", help="推一条 PushDeer 测试消息")
    dr_p = sub.add_parser("daily-report", help="生成并推送当日健康摘要（systemd timer 自动跑）")
    dr_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印不推送（mac 本地 smoke 测试用）",
    )

    mp_p = sub.add_parser(
        "morning-pulse",
        help="盘中 30 分钟脉搏（launchd 10:00/10:30/11:00/11:30 自动跑）",
    )
    mp_p.add_argument(
        "--slot",
        type=str,
        default=None,
        help="手动补跑指定槽位 HH:MM（10:00/10:30/11:00/11:30）；不传按当前时间归槽",
    )
    mp_p.add_argument("--force", action="store_true", help="绕过当日去重，覆盖重跑")
    mp_p.add_argument(
        "--dry-run",
        action="store_true",
        help="全流程跑但不推送（打印报文，parquet 照落）",
    )

    mdr_p = sub.add_parser("midday-report", help="午间战报（launchd 12:00 自动跑）")
    mdr_p.add_argument("--date", type=str, default=None, help="指定日期 YYYY-MM-DD（默认今天）")
    mdr_p.add_argument("--force", action="store_true", help="绕过当日去重，覆盖重跑")
    mdr_p.add_argument(
        "--dry-run",
        action="store_true",
        help="全流程跑但不推送（打印报文，parquet 照落）",
    )

    pmc_p = sub.add_parser(
        "pre-market-check",
        help="开盘前主动健康体检（systemd timer Mon..Fri 09:00 自动跑）",
    )
    pmc_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印不推送（mac 本地 smoke 测试用）",
    )

    pf_p = sub.add_parser(
        "preflight",
        help="全家服务深度体检（手动触发，dry-run，不重启服务）",
    )
    pf_p.add_argument(
        "--notify",
        action="store_true",
        help="跑完推一条摘要到 PushDeer（默认只 stdout）",
    )
    pf_p.add_argument(
        "--profile",
        choices=("production", "research"),
        default="production",
        help="数据新鲜度契约范围（默认 production）",
    )
    pf_p.add_argument(
        "--runtime-root",
        type=Path,
        help="验证当前 hash-bound runtime profile 的 recovery RPO/RTO 与演练凭据",
    )

    external_root_p = sub.add_parser(
        "external-monotonic-root-serve",
        help="运行持久 external monotonic root Unix authority",
    )
    external_root_p.add_argument("--config", type=Path, required=True)

    resource_authority_p = sub.add_parser(
        "resource-authority-serve",
        help="运行 external-root-backed resource journal Unix authority",
    )
    resource_authority_p.add_argument("--config", type=Path, required=True)
    resource_authority_p.add_argument(
        "--code-sha",
        help="完整生产 commit SHA；缺省读取 RQUANT_CODE_COMMIT",
    )

    from rquant.surge_watch import SurgeConfig

    sw_p = sub.add_parser(
        "surge-watch",
        help="每分钟爆量推送（云端 systemd timer 09:25 拉起，15:02 自退）",
    )
    sw_p.add_argument(
        "--dry-run",
        action="store_true",
        help="全流程跑但不推送（打印报文，parquet 照落）",
    )
    sw_p.add_argument(
        "--simulate",
        type=str,
        default=None,
        help="离线回放目录内快照 parquet 序列（逐分钟，可测性设施）",
    )
    sw_p.add_argument(
        "--force-session",
        action="store_true",
        help="忽略时段守卫（盘后验收用）",
    )
    sw_p.add_argument(
        "--max-ticks",
        type=int,
        default=None,
        help="限定循环次数（dry-run / 盘后 smoke，默认跑到 15:02）",
    )
    sw_p.add_argument(
        "--k-cum",
        type=float,
        default=SurgeConfig.model_fields["k_cum"].default,
        help="确认层纯累计比值下门（默认 2.5，2026-07-06 全天分钟回测标定）",
    )
    sw_p.add_argument(
        "--ratio-cap",
        type=float,
        default=SurgeConfig.model_fields["ratio_cap"].default,
        help="累计比值上门/毒尾封顶（默认 8.0，超过视为极端出货不推）",
    )
    sw_p.add_argument(
        "--skip-first-minutes",
        type=int,
        default=SurgeConfig.model_fields["skip_first_minutes"].default,
        help="跳过开盘前 N 分钟确认（默认 1，9:32 起才确认，base 分母噪声大）",
    )
    sw_p.add_argument(
        "--k-delta",
        type=float,
        default=SurgeConfig.model_fields["k_delta_confirm"].default,
        help="单分钟增量门倍数（v2 遗留，默认 0=关闭）",
    )
    sw_p.add_argument(
        "--require-vwap",
        action="store_true",
        help="启用 VWAP 门（v2 遗留，默认关；现价 ≥ 当日均价才确认）",
    )
    sw_p.add_argument(
        "--max-room",
        type=float,
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

    runtime_profile_p = sub.add_parser(
        "runtime-deployment-profile",
        help="预演或安装精确提交绑定的隔离运行时画像",
    )
    runtime_profile_p.add_argument("--profile", type=Path, required=True)
    runtime_profile_p.add_argument("--runtime-root", type=Path, required=True)
    runtime_profile_p.add_argument("--expected-commit", required=True)
    runtime_profile_p.add_argument("--apply", action="store_true")
    runtime_profile_p.add_argument("--profile-id", type=_parse_sha256)
    runtime_profile_p.add_argument("--schema-bootstrap-reason")
    runtime_profile_p.add_argument("--schema-v1-migration-authority", type=Path)

    runtime_production_profile_p = sub.add_parser(
        "runtime-production-profile",
        help="从 canonical 输入生成内容寻址的生产运行时画像",
    )
    runtime_production_profile_p.add_argument("--inputs", type=Path, required=True)
    runtime_production_profile_p.add_argument("--output-dir", type=Path, required=True)
    runtime_production_profile_p.add_argument("--expected-commit", required=True)
    runtime_production_profile_p.add_argument(
        "--runtime-mode",
        choices=("local-test", "linux-production"),
        default="local-test",
    )
    runtime_production_profile_p.add_argument("--apply", action="store_true")
    runtime_production_profile_p.add_argument("--profile-id", type=_parse_sha256)

    runtime_production_prerequisites_p = sub.add_parser(
        "runtime-production-prerequisites",
        help="预演或安装生产画像所需的不可变 authority generation",
    )
    runtime_production_prerequisites_p.add_argument("--inputs", type=Path, required=True)
    runtime_production_prerequisites_p.add_argument("--expected-commit", required=True)
    runtime_production_prerequisites_p.add_argument(
        "--runtime-mode",
        choices=("local-test", "linux-production"),
        default="local-test",
    )
    runtime_production_prerequisites_p.add_argument("--apply", action="store_true")
    runtime_production_prerequisites_p.add_argument("--profile-id", type=_parse_sha256)

    runtime_rollout_p = sub.add_parser(
        "runtime-deployment-rollout",
        help="按 generation 健康门滚动启动已安装的隔离运行时",
    )
    runtime_rollout_p.add_argument("--runtime-root", type=Path, required=True)
    runtime_rollout_p.add_argument("--expected-commit", required=True)
    runtime_rollout_p.add_argument("--profile-id", type=_parse_sha256, required=True)
    runtime_rollout_p.add_argument("--generation-hash", type=_parse_sha256, required=True)
    runtime_rollout_p.add_argument("--previous-generation-hash", type=_parse_sha256)
    runtime_rollout_p.add_argument("--audit-root", type=Path)
    runtime_rollout_p.add_argument("--health-timeout-seconds", type=float, default=120.0)

    runtime_rollback_p = sub.add_parser(
        "runtime-deployment-rollback",
        help="在代码回滚前显式恢复 previous runtime generation",
    )
    runtime_rollback_p.add_argument("--runtime-root", type=Path, required=True)
    runtime_rollback_p.add_argument("--failed-commit", required=True)
    runtime_rollback_p.add_argument("--expected-previous-commit", required=True)
    runtime_rollback_p.add_argument("--operation-id", type=_parse_sha256, required=True)
    runtime_rollback_p.add_argument("--audit-root", type=Path)
    runtime_rollback_p.add_argument("--health-timeout-seconds", type=float, default=120.0)

    runtime_retirement_p = sub.add_parser(
        "runtime-schema-retirement",
        help="只读检查或显式执行 CUTOVER 后的 schema RETIRE",
    )
    runtime_retirement_sub = runtime_retirement_p.add_subparsers(
        dest="retirement_action",
        required=True,
    )
    for action in ("status", "dry-run", "apply"):
        action_parser = runtime_retirement_sub.add_parser(action)
        action_parser.add_argument("--runtime-root", type=Path, required=True)
        action_parser.add_argument("--expected-commit", required=True)
        action_parser.add_argument("--profile-id", type=_parse_sha256, required=True)
        action_parser.add_argument("--generation-hash", type=_parse_sha256, required=True)
        action_parser.add_argument(
            "--rollout-operation-id",
            type=_parse_sha256,
            required=True,
        )
        action_parser.add_argument("--audit-root", type=Path)
        if action != "status":
            action_parser.add_argument("--plan-id", type=_parse_sha256, required=True)
        if action == "apply":
            action_parser.add_argument("--operation-id", type=_parse_sha256, required=True)

    recovery_backup_p = sub.add_parser(
        "runtime-recovery-backup",
        help="生成或检查签名 recovery backup generation",
    )
    recovery_backup_sub = recovery_backup_p.add_subparsers(
        dest="recovery_action",
        required=True,
    )
    for action in ("dry-run", "execute", "status"):
        action_parser = recovery_backup_sub.add_parser(action)
        action_parser.add_argument("--config", type=Path, required=True)
        action_parser.add_argument("--credential-file", type=Path, required=True)
        if action == "execute":
            action_parser.add_argument("--plan-id", type=_parse_sha256, required=True)

    recovery_production_config_p = sub.add_parser(
        "runtime-recovery-production-config",
        help="从当前 hash-bound production profile 解析 recovery backup 环境",
    )
    recovery_production_config_p.add_argument("--runtime-root", type=Path, required=True)

    recovery_production_p = sub.add_parser(
        "runtime-recovery-production",
        help="只从当前可信 production profile 执行 recovery",
    )
    recovery_production_sub = recovery_production_p.add_subparsers(
        dest="production_recovery_action",
        required=True,
    )
    for action in ("execute", "rehearse"):
        action_parser = recovery_production_sub.add_parser(action)
        action_parser.add_argument("--runtime-root", type=Path, required=True)
        action_parser.add_argument(
            "--expected-profile-generation",
            type=_parse_sha256,
            required=True,
        )

    recovery_p = sub.add_parser(
        "runtime-recovery",
        help="执行或检查隔离 runtime recovery rehearsal",
    )
    recovery_sub = recovery_p.add_subparsers(dest="recovery_action", required=True)
    for action in ("dry-run", "execute", "status"):
        action_parser = recovery_sub.add_parser(action)
        action_parser.add_argument("--publication-root", type=Path, required=True)
        action_parser.add_argument("--state-path", type=Path, required=True)
        action_parser.add_argument("--receipt-root", type=Path, required=True)
        action_parser.add_argument("--restore-root", type=Path, required=True)
        action_parser.add_argument("--credential-file", type=Path, required=True)
        action_parser.add_argument(
            "--deadline-seconds",
            type=_parse_recovery_deadline_seconds,
            default=3600,
        )
        action_parser.add_argument(
            "--schedule-cycle-seconds",
            type=_parse_rehearsal_interval_seconds,
            default=None,
            help="外部 timer 周期，用于生成同周期幂等 request_id",
        )
        action_parser.add_argument("--worker-id", default="runtime-recovery")
        action_parser.add_argument(
            "--lease-seconds",
            type=_parse_recovery_deadline_seconds,
            default=None,
        )
        action_parser.add_argument(
            "--max-attempts",
            type=_parse_recovery_max_attempts,
            default=3,
        )
        action_parser.add_argument(
            "--retry-delay-seconds",
            type=_parse_recovery_retry_delay_seconds,
            default=60,
        )
        if action == "execute":
            plan_group = action_parser.add_mutually_exclusive_group(required=True)
            plan_group.add_argument("--plan-id", type=_parse_sha256)
            plan_group.add_argument(
                "--accept-current-plan",
                action="store_true",
                help="仅供受控 systemd oneshot 在同一进程内预演并执行当前不可变 generation",
            )

    lab_run_p = sub.add_parser(
        "lab-run",
        help="执行 Strategy Lab 后台任务 spec（UI「后台运行」派生，内部命令）",
    )
    lab_run_p.add_argument(
        "--spec",
        type=str,
        required=True,
        help="任务 spec JSON 路径（launch_background_run 生成）",
    )
    lab_run_p.add_argument(
        "--research-snapshot",
        type=Path,
        help="仅本地兼容任务：显式不可变 research DuckDB 快照；正式研究请使用 lab-worker",
    )

    lab_integrity_audit_p = sub.add_parser(
        "lab-integrity-audit",
        help="运行 Strategy Lab 全账本完整性审计并输出健康状态",
    )
    lab_integrity_audit_p.add_argument(
        "--jobs-path",
        type=Path,
        required=True,
        help="待审计的 Lab SQLite 账本绝对路径",
    )
    lab_integrity_audit_p.add_argument("--require-external-highwater", action="store_true")
    lab_integrity_audit_p.add_argument("--highwater-production-mode", action="store_true")
    lab_integrity_audit_p.add_argument("--highwater-command-json")
    lab_integrity_audit_p.add_argument("--highwater-stable-identity")
    lab_integrity_audit_p.add_argument("--highwater-code-identity")
    lab_integrity_audit_p.add_argument("--highwater-profile-identity")
    lab_integrity_audit_p.add_argument("--highwater-trusted-keyring", type=Path)
    lab_integrity_audit_p.add_argument("--highwater-timeout-seconds", type=float, default=10.0)
    lab_integrity_audit_p.add_argument("--highwater-allow-identity-rotation", action="store_true")
    lab_integrity_audit_p.add_argument("--machine-receipt", action="store_true")

    lab_runtime_prepare_p = sub.add_parser(
        "lab-runtime-prepare",
        help="首次安装时创建并迁移私有 Lab runtime 根目录",
    )
    _add_formal_runtime_bootstrap_arguments(lab_runtime_prepare_p)
    lab_runtime_prepare_p.add_argument("--runtime-deployment-root", type=Path, required=True)
    lab_runtime_prepare_p.add_argument("--expected-code-sha", type=_parse_commit_sha)
    lab_runtime_prepare_p.add_argument("--deployment-generation")
    lab_runtime_prepare_p.add_argument("--deployment-lock-path")
    lab_runtime_prepare_p.add_argument("--deployment-generation-fd", type=int)
    lab_runtime_prepare_p.add_argument("--startup-deadline-monotonic", required=True, type=float)
    lab_runtime_prepare_p.add_argument("--deployment-operation-id")
    lab_runtime_prepare_p.add_argument("--deployment-environment-generation")

    for command_name, help_text in (
        ("lab-launchd-install", "安装并加载 generation-bound Strategy Lab LaunchAgents"),
        ("lab-launchd-uninstall", "卸载精确登记的 Strategy Lab LaunchAgents"),
    ):
        launchd_p = sub.add_parser(command_name, help=help_text)
        launchd_p.add_argument("--expected-checkout-root", required=True)
        launchd_p.add_argument("--trusted-git-path", default="/usr/bin/git")
        launchd_p.add_argument("--deployment-lock-path", required=True)
        launchd_p.add_argument(
            "--launch-agents-dir",
            default=str(Path.home() / "Library" / "LaunchAgents"),
        )
        if command_name == "lab-launchd-install":
            launchd_p.add_argument("--worker-id", default="rquant-mac-primary")
            launchd_p.add_argument("--no-activate", action="store_true")
        else:
            launchd_p.add_argument("--no-deactivate", action="store_true")

    lab_scheduler_p = sub.add_parser(
        "lab-scheduler",
        help="运行 Strategy Lab 持久任务控制面",
    )
    _add_formal_runtime_bootstrap_arguments(lab_scheduler_p)
    lab_scheduler_p.add_argument("--runtime-deployment-root", type=Path, required=True)
    _add_formal_runtime_deployment_arguments(lab_scheduler_p)
    lab_scheduler_p.add_argument(
        "--once",
        action="store_true",
        help="只消费一批命令并退出",
    )
    lab_scheduler_p.add_argument(
        "--remediate-full-integrity",
        action="store_true",
        help="仅在 authority 已消费管理员一次性 remediation 授权后清除持久 degraded",
    )

    lab_worker_p = sub.add_parser(
        "lab-worker",
        help="运行 Strategy Lab 后台分片 worker",
    )
    _add_formal_runtime_bootstrap_arguments(lab_worker_p)
    _add_formal_runtime_deployment_arguments(lab_worker_p)
    lab_worker_p.add_argument(
        "--worker-id",
        default=None,
        help="稳定 worker id；默认读取 LAB_WORKER_ID，显式值必须与配置一致",
    )
    lab_worker_p.add_argument(
        "--once",
        action="store_true",
        help="只消费一个分片并退出",
    )
    lab_worker_p.add_argument(
        "--legacy-no-resource-admission",
        action="store_true",
        help="仅开发/测试允许：显式停用资源准入；生产环境拒绝启动",
    )

    lab_claim_finalizer_p = sub.add_parser(
        "lab-claim-finalizer",
        help="运行 authority-owned V2 claim publication finalizer",
    )
    _add_formal_runtime_bootstrap_arguments(lab_claim_finalizer_p)
    _add_formal_runtime_deployment_arguments(lab_claim_finalizer_p)
    lab_claim_finalizer_p.add_argument(
        "--once",
        action="store_true",
        help="只处理一批 claim publication 并退出",
    )

    trust_p = sub.add_parser(
        "lab-claim-finalizer-trust",
        help="离线签发、检查或轮换 claim finalizer 信任证书",
    )
    trust_sub = trust_p.add_subparsers(dest="action", required=True)
    trust_inspect_p = trust_sub.add_parser("inspect", help="只读检查 canonical 证书")
    trust_inspect_p.add_argument("--certificate", type=Path, required=True)
    for action in ("issue", "rotate"):
        trust_action_p = trust_sub.add_parser(action, help="离线 root 签发新的 runtime 证书")
        trust_action_p.add_argument("--root-private-key", type=Path, required=True)
        trust_action_p.add_argument("--root-public-key", type=Path, required=True)
        trust_action_p.add_argument("--root-issuer", default="lab-offline-root")
        trust_action_p.add_argument("--root-key-id", default="lab-finalizer-root")
        trust_action_p.add_argument("--finalizer-public-key", type=Path, required=True)
        trust_action_p.add_argument("--finalizer-issuer", default="lab-finalizer")
        trust_action_p.add_argument("--finalizer-key-id", default="lab-finalizer-runtime")
        trust_action_p.add_argument("--store-id", required=True)
        trust_action_p.add_argument("--database-device", type=int, required=True)
        trust_action_p.add_argument("--database-inode", type=int, required=True)
        trust_action_p.add_argument("--not-before", required=True)
        trust_action_p.add_argument("--expires-at", required=True)
        trust_action_p.add_argument("--output", type=Path)

    claim_preflight_p = sub.add_parser(
        "lab-claim-finalizer-preflight",
        help="从 current generation 收集 claim finalizer 依赖和 SLO",
    )
    claim_preflight_p.add_argument("--format", choices=("json", "markdown"), default="json")
    claim_preflight_p.add_argument(
        "--apply",
        action="store_true",
        help="仅在真实 preflight OK 时 CAS 推进 MATERIAL_INSTALLED 到 PREFLIGHT_OK",
    )

    runtime_p = sub.add_parser(
        "lab-claim-finalizer-runtime",
        help="安装或检查已签 finalizer generation；没有签发权限",
    )
    runtime_sub = runtime_p.add_subparsers(dest="action", required=True)
    runtime_inspect_p = runtime_sub.add_parser("inspect", help="只读检查 current generation")
    runtime_inspect_p.add_argument("--runtime-root", type=Path, required=True)
    runtime_inspect_p.add_argument("--service-user", required=True)
    runtime_inspect_p.add_argument("--service-group", required=True)
    runtime_inspect_p.add_argument("--trusted-base", type=Path, default=Path("/etc/rquant"))
    runtime_install_p = runtime_sub.add_parser("install", help="验证并原子安装已签 generation")
    runtime_install_p.add_argument("--runtime-root", type=Path, required=True)
    runtime_install_p.add_argument("--request", type=Path, required=True)
    runtime_install_p.add_argument("--root-public-key", type=Path, required=True)
    runtime_install_p.add_argument("--root-issuer", required=True)
    runtime_install_p.add_argument("--root-key-id", required=True)
    runtime_install_p.add_argument("--service-user", required=True)
    runtime_install_p.add_argument("--service-group", required=True)
    runtime_install_p.add_argument("--trusted-base", type=Path, default=Path("/etc/rquant"))
    runtime_install_p.add_argument("--dry-run", action="store_true")

    rollout_p = sub.add_parser(
        "lab-claim-finalizer-rollout",
        help="查询或受控 drain V2 claim publication rollout",
    )
    rollout_sub = rollout_p.add_subparsers(dest="action", required=True)
    rollout_sub.add_parser("status", help="读取 rollout CAS state")
    begin_drain_p = rollout_sub.add_parser("begin-drain", help="先停止 scheduler V2 emit")
    begin_drain_p.add_argument("--evidence", required=True)
    complete_drain_p = rollout_sub.add_parser("complete-drain", help="仅清空非终态和 outbox 后 OFF")
    complete_drain_p.add_argument("--evidence", required=True)

    lab_finalizer_p = sub.add_parser(
        "lab-finalizer",
        help="只读聚合已完成分片并发布完整结果 commit",
    )
    _add_formal_runtime_bootstrap_arguments(lab_finalizer_p)
    _add_formal_runtime_deployment_arguments(lab_finalizer_p)
    lab_finalizer_p.add_argument(
        "--once",
        action="store_true",
        help="只处理一批待 finalization 任务并退出",
    )

    pa_serve_p = sub.add_parser(
        "panorama-auth-serve",
        help="启动全景页登录网关服务（微信友好 cookie 登录，标准库 http.server）",
    )
    pa_serve_p.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="监听地址 (默认 127.0.0.1，只给 nginx 反代)",
    )
    pa_serve_p.add_argument(
        "--port",
        type=int,
        default=8507,
        help="监听端口 (默认 8507)",
    )

    pa_add_p = sub.add_parser(
        "panorama-user-add",
        help="添加/更新全景页登录用户（交互式输密码，覆盖同名）",
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

    serve 内的 daily_job 自有 try/except + typed outbox，main 不重复抓。
    """
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {
        "serve": cmd_serve,
        "run-daily": cmd_run_daily,
        "ingest": cmd_ingest,
        "daily-indicator-backfill": cmd_daily_indicator_backfill,
        "daily-dag": cmd_daily_dag,
        "daily-dag-dev": cmd_daily_dag,
        "daily-dag-shadow": cmd_daily_dag_shadow,
        "monitor": cmd_monitor,
        "rt-minute-fetch": cmd_rt_minute_fetch,
        "rt-minute-daily-fetch": cmd_rt_minute_daily_fetch,
        "research-sync": cmd_research_sync,
        "research-export": cmd_research_export,
        "research-ingest": cmd_research_ingest,
        "research-repair-auction": cmd_research_repair_auction,
        "research-repair-minute": cmd_research_repair_minute,
        "formal-smoke-replay": cmd_formal_smoke_replay,
        "formal-smoke-runtime-execute": cmd_formal_smoke_runtime_execute,
        "stage1-acceptance": cmd_stage1_acceptance,
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
        "backfill-abandon": cmd_backfill_abandon,
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
        "external-monotonic-root-serve": cmd_external_monotonic_root_serve,
        "resource-authority-serve": cmd_resource_authority_serve,
        "runtime-production-prerequisites": cmd_runtime_production_prerequisites,
        "runtime-production-profile": cmd_runtime_production_profile,
        "runtime-deployment-profile": cmd_runtime_deployment_profile,
        "runtime-deployment-rollout": cmd_runtime_deployment_rollout,
        "runtime-deployment-rollback": cmd_runtime_deployment_rollback,
        "runtime-schema-retirement": cmd_runtime_schema_retirement,
        "runtime-recovery-backup": cmd_runtime_recovery_backup,
        "runtime-recovery-production-config": cmd_runtime_recovery_production_config,
        "runtime-recovery-production": cmd_runtime_recovery_production,
        "runtime-recovery": cmd_runtime_recovery,
        "legacy-shadow-recover": cmd_legacy_shadow_recover,
        "surge-watch": cmd_surge_watch,
        "lab-run": cmd_lab_run,
        "lab-integrity-audit": cmd_lab_integrity_audit,
        "lab-runtime-prepare": cmd_lab_runtime_prepare,
        "lab-launchd-install": cmd_lab_launchd_install,
        "lab-launchd-uninstall": cmd_lab_launchd_uninstall,
        "lab-scheduler": cmd_lab_scheduler,
        "lab-worker": cmd_lab_worker,
        "lab-claim-finalizer": cmd_lab_claim_finalizer,
        "lab-claim-finalizer-trust": cmd_lab_claim_finalizer_trust,
        "lab-claim-finalizer-preflight": cmd_lab_claim_finalizer_preflight,
        "lab-claim-finalizer-runtime": cmd_lab_claim_finalizer_runtime,
        "lab-claim-finalizer-rollout": cmd_lab_claim_finalizer_rollout,
        "lab-finalizer": cmd_lab_finalizer,
        "runtime-code": cmd_runtime_code,
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
        "serve",
        "notify-test",
        "alert",
        "alert-resolve",
        "daily-report",
        "pre-market-check",
        "preflight",
        "external-monotonic-root-serve",
        "resource-authority-serve",
        "daily-dag",
        "daily-dag-dev",
        "daily-dag-shadow",
        "runtime-production-prerequisites",
        "runtime-production-profile",
        "runtime-deployment-profile",
        "runtime-deployment-rollout",
        "runtime-recovery-backup",
        "runtime-recovery-production-config",
        "runtime-recovery-production",
        "runtime-recovery",
        "data-audit",
        "lab-run",
        "lab-integrity-audit",
        "lab-runtime-prepare",
        "lab-launchd-install",
        "lab-launchd-uninstall",
        "lab-scheduler",
        "lab-worker",
        "lab-finalizer",
        "runtime-code",
        "formal-smoke-runtime-execute",
        "panorama-auth-serve",
        "panorama-user-add",
        "panorama-user-remove",
        "panorama-user-list",
        "panorama-gate-token",
    ):
        return handler(args)

    try:
        return handler(args)
    except Exception as e:
        logger.exception(f"=== {args.command} 异常 ===")
        if args.command == "run-daily":
            _record_daily_error_outbox(
                component="cli:run-daily",
                exc=e,
                trade_date=date.fromisoformat(args.date) if args.date else date.today(),
            )
        elif args.command not in {"monitor", "surge-watch"}:
            from rquant.notify import notify

            notify("error", component=f"cli:{args.command}", exc=e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
