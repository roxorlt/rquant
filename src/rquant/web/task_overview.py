"""Bounded task and resource views from one borrowed Serving generation."""

from __future__ import annotations

from datetime import datetime, timedelta

from pydantic import ValidationError

from rquant.dashboard.runtime_console_data import RuntimeServiceRow
from rquant.ops_status import OpsResourceEvidence, OpsSnapshot, OpsUnitEvidence
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.web.calendar import CalendarDay
from rquant.web.labels import PLANE_LABELS, service_label
from rquant.web.market import MarketPhase, market_phase
from rquant.web.models.common import StatusInfo
from rquant.web.models.tasks import (
    ResourceGroupItem,
    ResourcesData,
    RuntimeServiceItem,
    RuntimeServicesData,
    ScheduledTaskItem,
    ScheduledTasksData,
)
from rquant.web.serving import BorrowedGeneration
from rquant.web.status import Status, UserState, service_status

_OPS_TABLES = ("ops_host_status", "ops_unit_status", "ops_resource_status")
_SLICES = (
    "rquant.slice",
    "rquant-live.slice",
    "rquant-serving.slice",
    "rquant-research.slice",
    "rquant-maintenance.slice",
)
_GROUP_NAMES = ("盘中", "页面数据", "研究", "维护")
_HOST_COLUMNS = (
    "host_name",
    "boot_id",
    "manifest_digest",
    "sampled_at",
    "memory_total_bytes",
    "memory_available_bytes",
)
_UNIT_COLUMNS = (
    "timer",
    "service",
    "label",
    "expected_enabled",
    "session",
    "resource_group",
    "timer_load_state",
    "timer_unit_file_state",
    "timer_active_state",
    "timer_sub_state",
    "last_trigger_at",
    "next_at",
    "service_load_state",
    "service_active_state",
    "service_sub_state",
    "service_result",
    "service_invocation_id",
    "service_exec_status",
    "service_start_at",
    "service_exit_at",
    "last_result",
)
_RESOURCE_COLUMNS = (
    "slice_name",
    "load_state",
    "active_state",
    "memory_current_bytes",
    "memory_peak_bytes",
)
_SERVICE_COLUMNS = (
    "service_id",
    "plane",
    "status",
    "stale",
    "observed_at",
    "heartbeat_at",
    "input_sequence",
    "output_sequence",
    "backlog_count",
    "consecutive_failures",
    "last_error",
)
_OPS_TTL = timedelta(seconds=120)


def _mark(borrowed: BorrowedGeneration, dataset_id: str) -> ServingDatasetWatermark | None:
    return next(
        (item for item in borrowed.manifest.watermarks if item.dataset_id == dataset_id),
        None,
    )


def _unavailable_scheduled(note: str) -> ScheduledTasksData:
    return ScheduledTasksData(
        source_state="unavailable",
        source_label="定时任务暂不可用",
        source_note=note,
        source_updated_at=None,
        expires_at=None,
        items=[],
    )


def _unavailable_resources(note: str) -> ResourcesData:
    return ResourcesData(
        source_state="unavailable",
        source_label="资源状态暂不可用",
        source_note=note,
        source_updated_at=None,
        expires_at=None,
        host_memory_total_bytes=None,
        host_memory_available_bytes=None,
        rquant_memory_current_bytes=None,
        rquant_memory_peak_bytes=None,
        groups=[],
        cpu_usage_percent=None,
        cpu_note="暂无可信 CPU 数据",
    )


def unavailable_ops(note: str = "还没有可信的任务状态，等待状态采集更新。") -> tuple[
    ScheduledTasksData, ResourcesData
]:
    return _unavailable_scheduled(note), _unavailable_resources(note)


def unavailable_services(
    note: str = "还没有可信的服务清单，等待健康状态更新。",
) -> RuntimeServicesData:
    return RuntimeServicesData(
        source_state="unavailable",
        source_label="运行服务暂不可用",
        source_note=note,
        source_updated_at=None,
        items=[],
    )


def _projection_counts(borrowed: BorrowedGeneration) -> dict[str, int] | None:
    rows = borrowed.cursor.execute(
        "SELECT table_name, available, row_count FROM projection_status "
        "WHERE table_name IN ('ops_host_status', 'ops_unit_status', 'ops_resource_status') "
        "ORDER BY table_name LIMIT 4"
    ).fetchall()
    if len(rows) != len(_OPS_TABLES) or any(available is not True for _, available, _ in rows):
        return None
    counts = {str(name): count for name, _available, count in rows}
    if set(counts) != set(_OPS_TABLES):
        return None
    for name, count in counts.items():
        if type(count) is not int or count != borrowed.manifest.row_counts.get(name):
            return None
    if counts["ops_host_status"] != 1 or not 14 <= counts["ops_unit_status"] <= 32:
        return None
    if counts["ops_resource_status"] != 5:
        return None
    return counts


def _rows(borrowed: BorrowedGeneration, table: str, columns: tuple[str, ...], limit: int):
    # Every identifier comes from this module's fixed tuples, never an HTTP parameter.
    return borrowed.cursor.execute(
        f"SELECT {', '.join(columns)} FROM {table} ORDER BY {columns[0]} LIMIT ?", (limit,)
    ).fetchall()


def _ops_sample(borrowed: BorrowedGeneration, mark: ServingDatasetWatermark) -> OpsSnapshot | None:
    counts = _projection_counts(borrowed)
    if counts is None:
        return None
    hosts = _rows(borrowed, "ops_host_status", _HOST_COLUMNS, 2)
    units = _rows(borrowed, "ops_unit_status", _UNIT_COLUMNS, 33)
    resources = _rows(borrowed, "ops_resource_status", _RESOURCE_COLUMNS, 6)
    if (len(hosts), len(units), len(resources)) != (
        counts["ops_host_status"],
        counts["ops_unit_status"],
        counts["ops_resource_status"],
    ):
        return None
    try:
        host = dict(zip(_HOST_COLUMNS, hosts[0], strict=True))
        by_slice = {
            row[0]: OpsResourceEvidence.model_validate(
                dict(zip(_RESOURCE_COLUMNS, row, strict=True))
            )
            for row in resources
        }
        if set(by_slice) != set(_SLICES):
            return None
        sample = OpsSnapshot(
            sampled_at=host["sampled_at"],
            host_name=host["host_name"],
            boot_id=host["boot_id"],
            manifest_digest=host["manifest_digest"],
            host_memory_total_bytes=host["memory_total_bytes"],
            host_memory_available_bytes=host["memory_available_bytes"],
            units=tuple(
                OpsUnitEvidence.model_validate(dict(zip(_UNIT_COLUMNS, row, strict=True)))
                for row in units
            ),
            resources=tuple(by_slice[name] for name in _SLICES),
        )
    except (ValidationError, ValueError, TypeError):
        return None
    return sample if sample.sampled_at == mark.event_time else None


def _timer_status(item: OpsUnitEvidence, phase: MarketPhase, now: datetime) -> Status:
    if not item.expected_enabled:
        return Status(UserState.IDLE, "未运行", "这项任务已按计划停用")
    if item.timer_load_state != "loaded":
        return Status(UserState.CRIT, "异常", "定时任务没有正确安装")
    if item.timer_unit_file_state not in {"enabled", "enabled-runtime", "static"}:
        return Status(UserState.WARN, "注意", "定时任务未启用")
    if item.service_active_state == "failed" or item.service_result not in {None, "success"}:
        return Status(UserState.WARN, "注意", "最近一次服务运行异常，触发归属待确认")
    if item.session == "trading_day" and phase is MarketPhase.NON_TRADING_DAY:
        return Status(UserState.WAITING, "等待开盘", "休市日不执行")
    if item.session == "market_hours":
        if phase in {MarketPhase.NON_TRADING_DAY, MarketPhase.PRE_OPEN}:
            return Status(UserState.WAITING, "等待开盘", "盘中任务将在开盘后运行")
        if phase is MarketPhase.AFTER_CLOSE:
            return Status(UserState.WAITING, "已收盘", "盘中任务今天已结束")
        if phase is MarketPhase.NOON_BREAK:
            return Status(UserState.WAITING, "等待开盘", "午间休市，下午继续")
    if item.timer_active_state != "active":
        return Status(UserState.WARN, "注意", "定时任务当前没有运行")
    if item.next_at is None:
        return Status(UserState.WARN, "注意", "下次触发时间待确认")
    if item.next_at < now:
        return Status(UserState.WARN, "注意", "下次触发时间已过，等待状态更新")
    return Status(UserState.OK, "正常", "定时任务正在等待下次触发")


def ops_sections(
    borrowed: BorrowedGeneration | None, *, now: datetime, day: CalendarDay | None
) -> tuple[ScheduledTasksData, ResourcesData]:
    if borrowed is None:
        return unavailable_ops()
    mark = _mark(borrowed, "ops_status")
    if mark is None or mark.status is not FreshnessStatus.FRESH:
        return unavailable_ops()
    if mark.event_time > now:
        return unavailable_ops("任务状态时间暂时无法核实，等待下一次更新。")
    if now - mark.event_time >= _OPS_TTL:
        return unavailable_ops("任务状态已过期，等待下一次更新。")
    sample = _ops_sample(borrowed, mark)
    if sample is None:
        return unavailable_ops("任务状态暂时无法核实，等待下一次更新。")
    phase = market_phase(now, None if day is None else day.is_trading_day)
    scheduled = ScheduledTasksData(
        source_state="ready",
        source_label="定时任务",
        source_note=None,
        source_updated_at=sample.sampled_at,
        expires_at=sample.sampled_at + _OPS_TTL,
        items=[
            ScheduledTaskItem(
                name=item.label,
                status=StatusInfo.of(_timer_status(item, phase, now)),
                last_trigger_at=item.last_trigger_at,
                next_at=item.next_at,
                duration_seconds=(
                    (item.service_exit_at - item.service_start_at).total_seconds()
                    if item.last_result is not None
                    and item.service_start_at is not None
                    and item.service_exit_at is not None
                    else None
                ),
                result_label=(
                    "未知"
                    if item.last_result is None
                    else "成功"
                    if item.last_result == "success"
                    else "失败"
                ),
                timer_unit=item.timer,
                service_unit=item.service,
            )
            for item in sample.units
        ],
    )
    parent = sample.resources[0]
    resources = ResourcesData(
        source_state="ready",
        source_label="资源使用",
        source_note=None,
        source_updated_at=sample.sampled_at,
        expires_at=sample.sampled_at + _OPS_TTL,
        host_memory_total_bytes=sample.host_memory_total_bytes,
        host_memory_available_bytes=sample.host_memory_available_bytes,
        rquant_memory_current_bytes=parent.memory_current_bytes,
        rquant_memory_peak_bytes=parent.memory_peak_bytes,
        groups=[
            ResourceGroupItem(
                name=name,
                slice_unit=item.slice_name,
                memory_current_bytes=item.memory_current_bytes,
                memory_peak_bytes=item.memory_peak_bytes,
            )
            for name, item in zip(_GROUP_NAMES, sample.resources[1:], strict=True)
        ],
        cpu_usage_percent=None,
        cpu_note="暂无可信 CPU 数据",
    )
    return scheduled, resources


def service_section(
    borrowed: BorrowedGeneration | None, *, now: datetime, day: CalendarDay | None
) -> RuntimeServicesData:
    if borrowed is None:
        return unavailable_services()
    # runtime_services is emitted by RuntimeHealthSourceReader from its installed
    # settings.sources roster, including missing roles. Heartbeat files cannot add rows.
    mark = _mark(borrowed, "runtime_health")
    if mark is None or mark.status not in {FreshnessStatus.FRESH, FreshnessStatus.DEGRADED}:
        return unavailable_services()
    count = borrowed.manifest.row_counts.get("runtime_services")
    if type(count) is not int or not 1 <= count <= 32:
        return unavailable_services()
    rows = _rows(borrowed, "runtime_services", _SERVICE_COLUMNS, 33)
    if len(rows) != count or len({row[0] for row in rows}) != count:
        return unavailable_services("服务清单暂时无法核实，等待健康状态更新。")
    try:
        services = tuple(
            RuntimeServiceRow.model_validate(dict(zip(_SERVICE_COLUMNS, row, strict=True)))
            for row in rows
        )
    except (ValidationError, ValueError, TypeError):
        return unavailable_services("服务清单暂时无法核实，等待健康状态更新。")
    if any(len(item.service_id) > 128 for item in services):
        return unavailable_services("服务清单暂时无法核实，等待健康状态更新。")
    phase = market_phase(now, None if day is None else day.is_trading_day)
    return RuntimeServicesData(
        source_state="ready",
        source_label="运行服务",
        source_note=None,
        source_updated_at=mark.published_at,
        items=[
            RuntimeServiceItem(
                name=service_label(item.service_id),
                plane_label=PLANE_LABELS.get(item.plane, "其他"),
                status=StatusInfo.of(
                    service_status(
                        service_id=item.service_id,
                        plane=item.plane,
                        status=item.status,
                        stale=item.stale,
                        heartbeat_at=item.heartbeat_at,
                        consecutive_failures=item.consecutive_failures,
                        backlog_count=item.backlog_count,
                        phase=phase,
                        now=now,
                    )
                ),
                heartbeat_at=item.heartbeat_at,
                service_id=item.service_id,
            )
            for item in services
        ],
    )


__all__ = ["ops_sections", "service_section", "unavailable_ops", "unavailable_services"]
