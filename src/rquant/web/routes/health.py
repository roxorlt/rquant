"""``GET /api/v1/health``: 系统健康 — services by plane, data freshness, page data."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response

from rquant.dashboard.runtime_console_data import RuntimeConsoleSections, RuntimeServiceRow
from rquant.serving_contracts import ServingGenerationManifest
from rquant.web import readers
from rquant.web.calendar import CalendarDay, calendar_day
from rquant.web.envelope import Envelope
from rquant.web.labels import PLANE_LABELS, dataset_label, service_label, table_label
from rquant.web.market import MarketPhase, market_phase, shanghai_trade_date
from rquant.web.models.common import StateCounts, StatusInfo
from rquant.web.models.health import (
    ErrorItem,
    FreshnessItem,
    HealthData,
    PageDataStatus,
    ServiceItem,
    TableItem,
)
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.status import (
    STATE_ORDER,
    Status,
    UserState,
    daily_status,
    expected_daily_date,
    generation_status,
    service_status,
    watermark_status,
)

router = APIRouter()

_PLANE_ORDER = {"live": 0, "serving": 1, "research": 2}
#: Once-a-day data is due after the evening jobs (rquant-daily 17:00, research-ingest 18:10).
_DAILY_READY = time(18, 30)
_MINUTE_READY = time(19, 30)
#: The calendar should reach at least this far ahead of today.
_CALENDAR_HORIZON = timedelta(days=30)
_MAX_ERRORS = 20


@dataclass(frozen=True)
class GenerationContext:
    """Everything one request derives from its borrowed generation."""

    borrowed: BorrowedGeneration
    now: datetime
    day: CalendarDay
    phase: MarketPhase
    tables: Mapping[str, readers.TableState]
    sections: RuntimeConsoleSections

    @property
    def manifest(self) -> ServingGenerationManifest:
        return self.borrowed.manifest


def generation_context(borrowed: BorrowedGeneration, now: datetime) -> GenerationContext:
    day = calendar_day(borrowed.cursor, shanghai_trade_date(now))
    return GenerationContext(
        borrowed=borrowed,
        now=now,
        day=day,
        phase=market_phase(now, day.is_trading_day),
        tables=readers.table_states(borrowed.cursor),
        sections=readers.sections(borrowed.cursor),
    )


# ------------------------------------------------------------------ services


def service_item(row: RuntimeServiceRow, *, phase: MarketPhase, now: datetime) -> ServiceItem:
    status = service_status(
        service_id=row.service_id,
        plane=row.plane,
        status=row.status,
        stale=row.stale,
        heartbeat_at=row.heartbeat_at,
        consecutive_failures=row.consecutive_failures,
        backlog_count=row.backlog_count,
        phase=phase,
        now=now,
    )
    return ServiceItem(
        service_id=row.service_id,
        name=service_label(row.service_id),
        plane=row.plane,
        plane_label=PLANE_LABELS.get(row.plane, "其他"),
        status=StatusInfo.of(status),
        heartbeat_at=row.heartbeat_at,
        observed_at=row.observed_at,
        raw_status=row.status,
        stale=row.stale,
        input_sequence=row.input_sequence,
        output_sequence=row.output_sequence,
        backlog_count=row.backlog_count,
        consecutive_failures=row.consecutive_failures,
        last_error=row.last_error,
    )


def service_items(context: GenerationContext) -> list[ServiceItem]:
    items = [
        service_item(row, phase=context.phase, now=context.now) for row in context.sections.services
    ]
    return sorted(
        items,
        key=lambda item: (
            STATE_ORDER[item.status.state],
            _PLANE_ORDER.get(item.plane, 9),
            item.name,
            item.service_id,
        ),
    )


def state_counts(states: Sequence[UserState]) -> StateCounts:
    counts = Counter(states)
    return StateCounts(
        total=len(states),
        ok=counts[UserState.OK],
        warn=counts[UserState.WARN],
        crit=counts[UserState.CRIT],
        idle=counts[UserState.IDLE],
        waiting=counts[UserState.WAITING],
    )


def _error_summary(item: ServiceItem) -> str:
    if item.consecutive_failures > 0:
        return f"连续失败 {item.consecutive_failures} 次"
    return "报告了一个错误"


def error_items(services: Sequence[ServiceItem]) -> list[ErrorItem]:
    with_errors = [item for item in services if item.last_error]
    with_errors.sort(
        key=lambda item: item.heartbeat_at or item.observed_at,
        reverse=True,
    )
    return [
        ErrorItem(
            service_id=item.service_id,
            name=item.name,
            at=item.heartbeat_at or item.observed_at,
            summary=_error_summary(item),
            message=str(item.last_error),
        )
        for item in with_errors[:_MAX_ERRORS]
    ]


# ------------------------------------------------------------------ freshness


def _daily_item(
    context: GenerationContext,
    *,
    key: str,
    name: str,
    latest: date | None,
    latest_at: datetime | None,
    ready_at: time,
    ready_note: str,
) -> FreshnessItem:
    expected = expected_daily_date(
        today_is_trading_day=context.day.is_trading_day,
        today=context.day.trade_date,
        previous_trading_day=context.day.previous_trading_day,
        now=context.now,
        ready_at=ready_at,
    )
    behind = (
        readers.trading_days_after(context.borrowed.cursor, context.tables, latest, expected)
        if latest is not None and expected is not None and latest < expected
        else None
    )
    status = daily_status(latest, expected, behind_days=behind, ready_note=ready_note)
    return FreshnessItem(
        key=key,
        name=name,
        kind="market",
        latest_at=latest_at,
        latest_date=latest,
        status=StatusInfo.of(status),
    )


def _calendar_item(context: GenerationContext) -> FreshnessItem:
    last = readers.calendar_last_date(context.borrowed.cursor, context.tables)
    today = context.day.trade_date
    if last is None:
        status = Status(UserState.IDLE, "未发布", "交易日历暂时没有数据")
    elif last < today + _CALENDAR_HORIZON:
        status = Status(
            UserState.WARN,
            "快到期",
            f"只覆盖到 {last.isoformat()}，需要补充下一段交易日历",
        )
    else:
        status = Status(UserState.OK, "按时", f"覆盖到 {last.isoformat()}")
    return FreshnessItem(
        key="trade_calendar",
        name="交易日历",
        kind="market",
        latest_at=None,
        latest_date=last,
        status=StatusInfo.of(status),
    )


def freshness_items(context: GenerationContext) -> list[FreshnessItem]:
    cursor = context.borrowed.cursor
    minute_at = readers.minute_latest(cursor, context.tables)
    minute_date = None if minute_at is None else shanghai_trade_date(minute_at)
    items = [
        _daily_item(
            context,
            key="daily_bar",
            name="日线",
            latest=readers.daily_latest(cursor, context.tables),
            latest_at=None,
            ready_at=_DAILY_READY,
            ready_note="每个交易日 18:30 后应更新到当天",
        ),
        _daily_item(
            context,
            key="minute_coverage",
            name="分钟线",
            latest=minute_date,
            latest_at=minute_at,
            ready_at=_MINUTE_READY,
            ready_note="每个交易日 19:30 后应补齐当天",
        ),
        _daily_item(
            context,
            key="canvas_latest_trade_date",
            name="选股结果",
            latest=readers.screen_latest(cursor, context.tables),
            latest_at=None,
            ready_at=_DAILY_READY,
            ready_note="每个交易日收盘后选出下一交易日的候选",
        ),
        _calendar_item(context),
    ]
    for watermark in context.manifest.watermarks:
        status = watermark_status(watermark)
        unavailable = status.state is UserState.IDLE
        items.append(
            FreshnessItem(
                key=watermark.dataset_id,
                name=dataset_label(watermark.dataset_id),
                kind="dataset",
                latest_at=None if unavailable else watermark.event_time,
                latest_date=None,
                status=StatusInfo.of(status),
            )
        )
    return items


# ------------------------------------------------------------------ page data


def page_data_status(
    context: GenerationContext,
    *,
    stale_after: timedelta,
    fallback_detail: str | None,
) -> PageDataStatus:
    manifest = context.manifest
    age = max((context.now - manifest.built_at).total_seconds(), 0.0)
    status = generation_status(age, stale_after)
    if status.state is UserState.OK and fallback_detail:
        status = Status(UserState.WARN, "注意", "最新一批数据没有通过校验，暂时显示上一批")
    unpublished = sorted(
        (state for state in context.tables.values() if not state.available),
        key=lambda state: table_label(state.table_name),
    )
    pointer = context.borrowed.pointer
    return PageDataStatus(
        status=StatusInfo.of(status),
        built_at=manifest.built_at,
        published_at=None if pointer is None else pointer.published_at,
        age_seconds=age,
        generation_id=manifest.generation_id,
        tables_total=len(context.tables),
        unpublished=[
            TableItem(key=state.table_name, name=table_label(state.table_name))
            for state in unpublished
        ],
    )


def build_health(
    context: GenerationContext,
    *,
    stale_after: timedelta,
    fallback_detail: str | None,
) -> HealthData:
    services = service_items(context)
    return HealthData(
        counts=state_counts([item.status.state for item in services]),
        services=services,
        freshness=freshness_items(context),
        page_data=page_data_status(
            context, stale_after=stale_after, fallback_detail=fallback_detail
        ),
        errors=error_items(services),
    )


def empty_health(stale_after: timedelta) -> HealthData:
    return HealthData(
        counts=state_counts([]),
        services=[],
        freshness=[],
        page_data=PageDataStatus(
            status=StatusInfo.of(generation_status(None, stale_after)),
            built_at=None,
            published_at=None,
            age_seconds=None,
            generation_id=None,
            tables_total=0,
            unpublished=[],
        ),
        errors=[],
    )


@router.get("/health", response_model=Envelope[HealthData], summary="系统健康")
def get_health(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[HealthData]:
    web = request.app.state.web
    now = web.clock()
    stale_after = web.settings.stale_after
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(borrowed, now=now, stale_after=stale_after, failure=web.tracker.failure)
        if borrowed is None:
            data = empty_health(stale_after)
        else:
            data = build_health(
                generation_context(borrowed, now),
                stale_after=stale_after,
                fallback_detail=borrowed.fallback_detail,
            )
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[HealthData](data=data, serving=meta)


__all__ = [
    "GenerationContext",
    "build_health",
    "freshness_items",
    "generation_context",
    "router",
    "service_items",
    "state_counts",
]
