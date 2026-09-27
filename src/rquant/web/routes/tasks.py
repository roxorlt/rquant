"""Read-only research jobs from one verified Serving generation."""

from __future__ import annotations

import hashlib
import hmac
from base64 import b64decode, urlsafe_b64encode
from binascii import Error as Base64Error
from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rquant.runtime_contracts import AwareUtcDatetime
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingQueryError
from rquant.serving_read_models import LAB_EVENT_ALLOWED_LABELS
from rquant.web.calendar import calendar_day
from rquant.web.envelope import Envelope
from rquant.web.market import shanghai_trade_date
from rquant.web.models.common import StatusInfo
from rquant.web.models.tasks import (
    JobCounts,
    ResearchJobItem,
    ResearchJobsData,
    ResearchTaskEvent,
    ResearchTaskEventsData,
    ResearchTaskEventsState,
    TaskOverviewData,
)
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.status import UserState
from rquant.web.task_overview import (
    ops_sections,
    service_section,
    unavailable_ops,
    unavailable_services,
)

router = APIRouter(prefix="/tasks")

_STATUS_KEYS = ("queued", "running", "checkpointed", "succeeded", "failed", "cancelled")
_COUNTS_SQL = (
    "SELECT count(*), "
    + ", ".join(f"count(*) FILTER (WHERE status = '{key}')" for key in _STATUS_KEYS)
    + ", count(*) FILTER (WHERE status NOT IN ("
    + ", ".join(f"'{key}'" for key in _STATUS_KEYS)
    + ")) FROM lab_jobs"
)
_PAGE_COLUMNS = (
    "job_id, strategy_name, job_type, resource_class, status, control_intent, "
    "progress_fraction, terminal_shards, total_shards, eta_status, "
    "eta_finish_low, eta_finish_center, eta_finish_high, updated_at"
)
_PAGE_FIRST = f"SELECT {_PAGE_COLUMNS} FROM lab_jobs ORDER BY updated_at DESC, job_id ASC LIMIT ?"
_PAGE_AFTER = (
    f"SELECT {_PAGE_COLUMNS} FROM lab_jobs "
    "WHERE updated_at < ? OR (updated_at = ? AND job_id > ?) "
    "ORDER BY updated_at DESC, job_id ASC LIMIT ?"
)
_CHANGED = "任务数据已更新，请从第一页重新查看。"
_UNREADABLE = "研究任务数据暂时无法读取，请稍后重试。"
_EVENT_CHANGED = "任务进展已更新，请重新打开查看。"
_EVENT_NOTES: dict[ResearchTaskEventsState, str] = {
    "ready": "任务进展",
    "empty": "还没有进展记录。",
    "truncated": "仅显示最近记录。",
    "not_published": "任务进展尚未发布。",
    "not_included": "当前数据未包含该任务。",
    "unavailable": "任务进展暂时无法读取，请稍后重试。",
}
_EVENT_TABLES = ("lab_job_event", "lab_job_event_window")

_TYPE_LABELS = {
    "strategy_replay": "策略回放",
    "parameter_search": "参数搜索",
    "ablation": "消融实验",
}
_RESOURCE_LABELS = {"interactive": "快速", "standard": "标准", "heavy": "大型"}
_STATUS_INFO = {
    "queued": (UserState.WAITING, "排队中", "正在等待研究资源"),
    "running": (UserState.OK, "运行中", "任务正在运行"),
    "checkpointed": (UserState.WAITING, "已暂停", "可从保存的进度继续"),
    "succeeded": (UserState.OK, "已完成", "任务已完成"),
    "failed": (UserState.CRIT, "失败", "任务未完成"),
    "cancelled": (UserState.IDLE, "已取消", "任务已取消"),
}


class _Cursor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["research_jobs_v1", "task_overview_v1"]
    generation_id: str = Field(min_length=1, max_length=128)
    last_at: AwareUtcDatetime
    last_id: UUID
    page_size: int = Field(ge=1, le=50)


def _segment(raw: bytes) -> str:
    return urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _encode_cursor(cursor: _Cursor, key: bytes) -> str:
    payload = cursor.model_dump_json().encode("utf-8")
    return f"{_segment(payload)}.{_segment(hmac.new(key, payload, hashlib.sha256).digest())}"


def _decode_cursor(token: str, key: bytes) -> _Cursor:
    try:
        if len(token) > 512:
            raise ValueError("cursor too long")
        payload_text, signature_text = token.split(".")
        payload = b64decode(
            payload_text + "=" * (-len(payload_text) % 4), altchars=b"-_", validate=True
        )
        signature = b64decode(
            signature_text + "=" * (-len(signature_text) % 4), altchars=b"-_", validate=True
        )
        if _segment(payload) != payload_text or _segment(signature) != signature_text:
            raise ValueError("non-canonical cursor")
        expected = hmac.new(key, payload, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, signature):
            raise ValueError("cursor signature differs")
        return _Cursor.model_validate_json(payload)
    except (Base64Error, UnicodeError, ValueError, TypeError, ValidationError) as error:
        raise HTTPException(status_code=409, detail=_CHANGED) from error


def _status(raw: str, intent: str) -> StatusInfo:
    if raw == "running" and intent == "pause_requested":
        return StatusInfo(
            state=UserState.WAITING, label="暂停中", reason="已请求暂停，等待当前步骤结束"
        )
    if raw == "running" and intent == "cancel_requested":
        return StatusInfo(
            state=UserState.WAITING, label="取消中", reason="已请求取消，等待当前步骤结束"
        )
    state, label, reason = _STATUS_INFO.get(
        raw, (UserState.WARN, "状态待确认", "任务状态暂时无法确认")
    )
    return StatusInfo(state=state, label=label, reason=reason)


def _eta_label(status: str, intent: str) -> str:
    if status == "queued":
        return "排队中"
    if status == "checkpointed":
        return "已暂停"
    if status in {"succeeded", "failed", "cancelled"}:
        return "已结束"
    if intent == "pause_requested":
        return "暂停中"
    if intent == "cancel_requested":
        return "取消中"
    return "暂无法预计"


def _item(row: tuple[Any, ...], *, name_limit: int | None = None) -> ResearchJobItem:
    (
        job_id,
        name,
        job_type,
        resource,
        status,
        intent,
        fraction,
        terminal,
        total,
        eta_status,
        eta_low,
        eta_center,
        eta_high,
        updated_at,
    ) = row
    raw_status = str(status)
    raw_intent = str(intent)
    show_eta = (
        eta_center is not None
        and raw_intent == "none"
        and (raw_status, eta_status) in {("queued", "queued"), ("running", "running")}
    )
    display_name = str(name).strip() or "未命名研究任务"
    return ResearchJobItem(
        job_id=str(UUID(str(job_id))),
        strategy_name=display_name if name_limit is None else display_name[:name_limit],
        job_type_label=_TYPE_LABELS.get(str(job_type), "研究任务"),
        resource_label=_RESOURCE_LABELS.get(str(resource), "未分类"),
        status=_status(raw_status, raw_intent),
        progress_fraction=fraction,
        terminal_shards=terminal,
        total_shards=total,
        eta_at=eta_center if show_eta else None,
        eta_low=eta_low if show_eta else None,
        eta_high=eta_high if show_eta else None,
        eta_label="预计结束" if show_eta else _eta_label(raw_status, raw_intent),
        updated_at=updated_at,
    )


def _watermark(borrowed: BorrowedGeneration) -> ServingDatasetWatermark | None:
    return next(
        (mark for mark in borrowed.manifest.watermarks if mark.dataset_id == "lab_jobs"), None
    )


def _can_view_research_logs(viewer: str | None, request: Request) -> bool:
    settings = request.app.state.web.settings
    return (
        settings.ingress_socket_path is not None
        and viewer is not None
        and viewer in settings.log_admin_users
    )


def _events_data(
    state: ResearchTaskEventsState,
    *,
    generation_id: str | None,
    updated_at: datetime | None = None,
    events: list[ResearchTaskEvent] | None = None,
) -> ResearchTaskEventsData:
    selected = events if events is not None else []
    note = (
        "当前没有可展示的最近记录。"
        if state == "truncated" and not selected
        else _EVENT_NOTES[state]
    )
    return ResearchTaskEventsData(
        state=state,
        note=note,
        generation_id=generation_id,
        updated_at=updated_at,
        events=selected,
        truncated=state == "truncated",
    )


def _published_event_time(borrowed: BorrowedGeneration) -> datetime | None:
    rows = borrowed.cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, "
        "owner_generation_id, available_at FROM projection_status "
        "WHERE table_name IN (?, ?) ORDER BY table_name LIMIT 3",
        _EVENT_TABLES,
    ).fetchall()
    if not rows and all(borrowed.manifest.row_counts.get(name, 0) == 0 for name in _EVENT_TABLES):
        return None
    if len(rows) != 2 or tuple(row[0] for row in rows) != _EVENT_TABLES:
        raise ValueError("research event projection status is incomplete")
    mark = _watermark(borrowed)
    if all(not row[1] for row in rows):
        if any(
            row[2] != 0
            or row[3] != "lab_jobs"
            or row[4] is not None
            or row[5] is not None
            or borrowed.manifest.row_counts.get(str(row[0]), 0) != 0
            for row in rows
        ):
            raise ValueError("unpublished research event projection is inconsistent")
        return None
    if mark is None or mark.status is FreshnessStatus.UNAVAILABLE:
        raise ValueError("research event owner is unavailable")
    at = rows[0][5]
    if (
        any(
            type(available) is not bool
            or not available
            or type(count) is not int
            or count != borrowed.manifest.row_counts.get(str(name))
            or owner != "lab_jobs"
            or owner_generation != mark.generation_id
            or available_at != at
            for name, available, count, owner, owner_generation, available_at in rows
        )
        or not isinstance(at, datetime)
        or at.tzinfo is None
        or at > borrowed.manifest.built_at
    ):
        raise ValueError("research event projection is not from this source generation")
    return at


def _read_events(borrowed: BorrowedGeneration, job_id: str) -> ResearchTaskEventsData:
    generation = borrowed.manifest.generation_id
    at = _published_event_time(borrowed)
    if at is None:
        return _events_data("not_published", generation_id=generation)
    jobs = borrowed.cursor.execute(
        "SELECT status FROM lab_jobs WHERE job_id = ? LIMIT 2", (job_id,)
    ).fetchall()
    if not jobs:
        return _events_data("not_included", generation_id=generation, updated_at=at)
    if len(jobs) != 1 or jobs[0][0] not in _STATUS_INFO:
        raise ValueError("published research job is invalid")
    windows = borrowed.cursor.execute(
        "SELECT job_version, state, retained_count, truncated "
        "FROM lab_job_event_window WHERE job_id = ? LIMIT 2",
        (job_id,),
    ).fetchall()
    if len(windows) != 1:
        raise ValueError("published research event window is missing")
    version, state, retained, truncated = windows[0]
    if (
        type(version) is not int
        or version < 0
        or type(retained) is not int
        or not 0 <= retained <= 500
        or type(truncated) is not bool
        or (state, truncated)
        not in {
            ("empty", False),
            ("available", False),
            ("truncated", True),
        }
        or (state == "empty" and retained != 0)
        or (state == "available" and retained == 0)
    ):
        raise ValueError("published research event window is invalid")
    rows = borrowed.cursor.execute(
        "SELECT event_id, job_version, occurred_at, new_status, label "
        "FROM lab_job_event WHERE job_id = ? ORDER BY event_id DESC LIMIT 501",
        (job_id,),
    ).fetchall()
    if len(rows) != retained:
        raise ValueError("published research event count differs from its window")
    events: list[ResearchTaskEvent] = []
    previous_id: int | None = None
    previous_version: int | None = None
    previous_time: datetime | None = None
    for event_id, event_version, occurred_at, new_status, label in rows:
        if (
            type(event_id) is not int
            or event_id < 1
            or (previous_id is not None and event_id >= previous_id)
            or type(event_version) is not int
            or not 0 <= event_version <= version
            or (previous_version is not None and event_version > previous_version)
            or not isinstance(occurred_at, datetime)
            or occurred_at.tzinfo is None
            or occurred_at > at
            or (previous_time is not None and occurred_at > previous_time)
            or new_status not in _STATUS_INFO
            or label not in LAB_EVENT_ALLOWED_LABELS
        ):
            raise ValueError("published research event is invalid")
        events.append(
            ResearchTaskEvent(
                event_id=event_id,
                occurred_at=occurred_at,
                label=label,
                status_label=_STATUS_INFO[new_status][1],
            )
        )
        previous_id, previous_version, previous_time = event_id, event_version, occurred_at
    if rows and (rows[0][1] != version or rows[0][3] != jobs[0][0]):
        raise ValueError("latest research event differs from the published job")
    result_state: ResearchTaskEventsState = (
        "truncated" if truncated else "empty" if not events else "ready"
    )
    return _events_data(result_state, generation_id=generation, updated_at=at, events=events)


def _empty(
    source_state: Literal["unavailable", "not_published"], page_size: int
) -> ResearchJobsData:
    return ResearchJobsData(
        source_state=source_state,
        source_label=(
            "暂时读不到页面数据" if source_state == "unavailable" else "研究任务尚未发布"
        ),
        source_note=None,
        source_updated_at=None,
        total=None,
        counts=None,
        page_size=page_size,
        items=[],
        next_cursor=None,
    )


def _page(
    borrowed: BorrowedGeneration,
    *,
    mark: ServingDatasetWatermark,
    page_size: int,
    after: _Cursor | None,
    key: bytes,
    cursor_kind: Literal["research_jobs_v1", "task_overview_v1"] = "research_jobs_v1",
) -> ResearchJobsData:
    try:
        count_values = borrowed.cursor.execute(_COUNTS_SQL).fetchone()
        if count_values is None:
            raise ValueError("job counts are missing")
        total = int(count_values[0])
        if total != borrowed.manifest.row_counts.get("lab_jobs"):
            raise ValueError("job count differs from manifest")
        counts = JobCounts(**dict(zip((*_STATUS_KEYS, "other"), count_values[1:], strict=True)))
        if sum(count_values[1:]) != total:
            raise ValueError("job status counts differ from total")
        if after is None:
            rows = borrowed.cursor.execute(_PAGE_FIRST, (page_size + 1,)).fetchall()
        else:
            rows = borrowed.cursor.execute(
                _PAGE_AFTER,
                (after.last_at, after.last_at, str(after.last_id), page_size + 1),
            ).fetchall()
        selected = rows[:page_size]
        items = [
            _item(row, name_limit=80 if cursor_kind == "task_overview_v1" else None)
            for row in selected
        ]
    except (ServingQueryError, ValueError, TypeError, ValidationError) as error:
        raise HTTPException(status_code=503, detail=_UNREADABLE) from error
    next_cursor = (
        _encode_cursor(
            _Cursor(
                kind=cursor_kind,
                generation_id=borrowed.manifest.generation_id,
                last_at=items[-1].updated_at,
                last_id=UUID(items[-1].job_id),
                page_size=page_size,
            ),
            key,
        )
        if len(rows) > page_size and items
        else None
    )
    source_note = (
        "研究任务数据更新延迟，以下记录可能不是最新的。"
        if mark.status is FreshnessStatus.STALE
        else "研究任务数据暂不完整，以下记录仅供参考。"
        if mark.status is FreshnessStatus.DEGRADED
        else None
    )
    return ResearchJobsData(
        source_state="ready" if total else "empty",
        source_label="研究任务" if total else "还没有研究任务",
        source_note=source_note,
        source_updated_at=mark.event_time,
        total=total,
        counts=counts,
        page_size=page_size,
        items=items,
        next_cursor=next_cursor,
    )


@router.get("/jobs", response_model=Envelope[ResearchJobsData], summary="研究任务队列")
def get_jobs(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    page_size: Annotated[int, Query(ge=1, le=50)] = 20,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> Envelope[ResearchJobsData]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure
        )
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        decoded = _decode_cursor(cursor, web.cursor_key) if cursor is not None else None
        if decoded is not None and (
            decoded.kind != "research_jobs_v1"
            or decoded.generation_id != meta.generation_id
            or decoded.page_size != page_size
        ):
            raise HTTPException(status_code=409, detail=_CHANGED)
        if borrowed is None or meta.state == "unavailable":
            if decoded is not None:
                raise HTTPException(status_code=409, detail=_CHANGED)
            data = _empty("unavailable", page_size)
        else:
            mark = _watermark(borrowed)
            if mark is None or mark.status is FreshnessStatus.UNAVAILABLE:
                if decoded is not None:
                    raise HTTPException(status_code=409, detail=_CHANGED)
                data = _empty("not_published", page_size)
            else:
                data = _page(
                    borrowed, mark=mark, page_size=page_size, after=decoded, key=web.cursor_key
                )
    return Envelope[ResearchJobsData](data=data, serving=meta)


@router.get(
    "/jobs/{job_id}/events",
    response_model=ResearchTaskEventsData,
    summary="研究任务进展",
)
def get_job_events(
    job_id: str,
    request: Request,
    response: Response,
    viewer: Annotated[str | None, Depends(current_user)],
    generation_id: Annotated[str | None, Query(max_length=128)] = None,
) -> ResearchTaskEventsData:
    web = request.app.state.web
    if web.settings.ingress_socket_path is None or not web.settings.log_admin_users:
        raise HTTPException(status_code=503, detail="任务进展尚未开放。")
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if viewer not in web.settings.log_admin_users:
        raise HTTPException(status_code=403, detail="当前账号不能查看任务进展。")
    try:
        canonical_job_id = str(UUID(job_id))
    except ValueError:
        raise HTTPException(status_code=404, detail="任务标识无效。") from None
    if canonical_job_id != job_id:
        raise HTTPException(status_code=404, detail="任务标识无效。")
    with web.tracker.borrow() as borrowed:
        if borrowed is None:
            if generation_id is not None:
                raise HTTPException(status_code=409, detail=_EVENT_CHANGED)
            return _events_data("unavailable", generation_id=None)
        current = borrowed.manifest.generation_id
        if generation_id is not None and generation_id != current:
            raise HTTPException(status_code=409, detail=_EVENT_CHANGED)
        response.headers["X-Rquant-Generation"] = current
        if borrowed.manifest.built_at > web.clock():
            return _events_data("unavailable", generation_id=current)
        try:
            return _read_events(borrowed, canonical_job_id)
        except (ServingQueryError, ValueError, TypeError, ValidationError):
            return _events_data("unavailable", generation_id=current)


@router.get("/overview", response_model=Envelope[TaskOverviewData], summary="任务与运行状态")
def get_overview(
    request: Request,
    response: Response,
    viewer: Annotated[str | None, Depends(current_user)],
    page_size: Annotated[int, Query(ge=1, le=50)] = 20,
    cursor: Annotated[str | None, Query(max_length=512)] = None,
) -> Envelope[TaskOverviewData]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure
        )
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        decoded = _decode_cursor(cursor, web.cursor_key) if cursor is not None else None
        if decoded is not None and (
            decoded.kind != "task_overview_v1"
            or decoded.generation_id != meta.generation_id
            or decoded.page_size != page_size
        ):
            raise HTTPException(status_code=409, detail=_CHANGED)
        if borrowed is None or meta.state == "unavailable":
            if decoded is not None:
                raise HTTPException(status_code=409, detail=_CHANGED)
            scheduled, resources = unavailable_ops()
            services = unavailable_services()
            research = _empty("unavailable", page_size)
        else:
            day = calendar_day(borrowed.cursor, shanghai_trade_date(now))
            scheduled, resources = ops_sections(borrowed, now=now, day=day)
            services = service_section(borrowed, now=now, day=day)
            mark = _watermark(borrowed)
            if mark is None or mark.status is FreshnessStatus.UNAVAILABLE:
                if decoded is not None:
                    raise HTTPException(status_code=409, detail=_CHANGED)
                research = _empty("not_published", page_size)
            else:
                research = _page(
                    borrowed,
                    mark=mark,
                    page_size=page_size,
                    after=decoded,
                    key=web.cursor_key,
                    cursor_kind="task_overview_v1",
                )
    return Envelope[TaskOverviewData](
        data=TaskOverviewData(
            scheduled=scheduled,
            services=services,
            resources=resources,
            research=research,
            can_view_research_logs=_can_view_research_logs(viewer, request),
        ),
        serving=meta,
    )
