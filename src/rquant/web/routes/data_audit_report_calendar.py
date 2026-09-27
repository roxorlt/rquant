"""Closed SSE dates for the daily-bar audit form, from one Serving generation."""

from __future__ import annotations

from bisect import bisect_left
from datetime import date, datetime, timedelta
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from rquant.data_audit_contracts import MAX_AUDIT_DAYS
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS
from rquant.web.calendar import CalendarDay, last_closed_trading_day
from rquant.web.envelope import Envelope
from rquant.web.market import market_phase, shanghai_trade_date
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/data/audit-report")
_CONTRACT = PAGE_PROJECTION_CONTRACTS["trade_calendar"]
_OWNER = _CONTRACT.owner_dataset_id
_CHANGED = "交易日历已更新，请重新选择日期。"
_UNREADABLE = "交易日历暂时无法读取，请稍后重试。"


class AuditReportCalendarData(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    availability: Literal["ready", "unavailable"]
    latest_closed_date: date | None
    earliest_selectable_date: date | None
    open_dates: list[date] = Field(max_length=MAX_AUDIT_DAYS)


def _unavailable() -> AuditReportCalendarData:
    return AuditReportCalendarData(
        availability="unavailable",
        latest_closed_date=None,
        earliest_selectable_date=None,
        open_dates=[],
    )


def _published_count(borrowed: BorrowedGeneration) -> int | None:
    row = borrowed.cursor.execute(
        "SELECT available, row_count, owner_dataset_id, owner_generation_id, available_at "
        "FROM projection_status WHERE table_name = 'trade_calendar'"
    ).fetchone()
    manifest_count = borrowed.manifest.row_counts.get("trade_calendar")
    if row is None:
        if manifest_count not in (None, 0):
            raise ValueError("calendar rows lack projection status")
        return None
    available, count, owner, generation, available_at = row
    if type(available) is not bool or type(count) is not int or owner != _OWNER:
        raise ValueError("calendar projection status is invalid")
    if not available:
        if (
            count
            or manifest_count not in (None, 0)
            or generation is not None
            or available_at is not None
        ):
            raise ValueError("unpublished calendar contains rows or provenance")
        return None
    watermark = next(
        (item for item in borrowed.manifest.watermarks if item.dataset_id == _OWNER), None
    )
    if (
        watermark is None
        or generation != watermark.generation_id
        or type(available_at) is not datetime
        or available_at.tzinfo is None
        or available_at > borrowed.manifest.built_at
        or not 1 <= count <= _CONTRACT.max_rows
        or count != manifest_count
    ):
        raise ValueError("calendar projection disagrees with generation")
    return count


def _snapshot(borrowed: BorrowedGeneration | None, *, now: datetime) -> AuditReportCalendarData:
    if borrowed is None:
        return _unavailable()
    expected = _published_count(borrowed)
    if expected is None:
        return _unavailable()
    rows = borrowed.cursor.execute(
        "SELECT trade_date, is_open FROM trade_calendar WHERE exchange = 'SSE' "
        "ORDER BY trade_date LIMIT ?",
        (_CONTRACT.max_rows + 1,),
    ).fetchall()
    dates = [row[0] for row in rows]
    if (
        len(rows) != expected
        or any(type(day) is not date or flag is not True for day, flag in rows)
        or any(previous >= following for previous, following in zip(dates, dates[1:], strict=False))
    ):
        raise ValueError("calendar rows are invalid")
    today = shanghai_trade_date(now)
    if not dates[0] <= today <= dates[-1]:
        return _unavailable()
    index = bisect_left(dates, today)
    is_open = index < len(dates) and dates[index] == today
    day = CalendarDay(
        trade_date=today,
        is_trading_day=is_open,
        previous_trading_day=dates[index - 1] if index else None,
        next_trading_day=dates[index + int(is_open)] if index + int(is_open) < len(dates) else None,
    )
    latest = last_closed_trading_day(day, market_phase(now, is_open))
    if latest is None:
        return _unavailable()
    earliest_bound = today - timedelta(days=MAX_AUDIT_DAYS - 1)
    selectable = [value for value in dates if earliest_bound <= value <= latest]
    if not selectable:
        return _unavailable()
    return AuditReportCalendarData(
        availability="ready",
        latest_closed_date=latest,
        earliest_selectable_date=selectable[0],
        open_dates=selectable,
    )


@router.get(
    "/calendar", response_model=Envelope[AuditReportCalendarData], summary="审计日期可选交易日"
)
def get_data_audit_report_calendar(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    generation: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
) -> Envelope[AuditReportCalendarData]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=now,
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if generation is not None and generation != meta.generation_id:
            raise HTTPException(status_code=409, detail=_CHANGED)
        try:
            data = _snapshot(None if meta.state == "unavailable" else borrowed, now=now)
        except Exception as error:
            raise HTTPException(status_code=503, detail=_UNREADABLE) from error
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    response.headers["Cache-Control"] = "no-store"
    return Envelope[AuditReportCalendarData](data=data, serving=meta)
