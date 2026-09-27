"""Read-only research jobs from one verified Serving generation."""

from __future__ import annotations

import hashlib
import hmac
from base64 import b64decode, urlsafe_b64encode
from binascii import Error as Base64Error
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rquant.runtime_contracts import AwareUtcDatetime
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingQueryError
from rquant.web.calendar import calendar_day
from rquant.web.envelope import Envelope
from rquant.web.market import shanghai_trade_date
from rquant.web.models.common import StatusInfo
from rquant.web.models.tasks import JobCounts, ResearchJobItem, ResearchJobsData, TaskOverviewData
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


@router.get("/overview", response_model=Envelope[TaskOverviewData], summary="任务与运行状态")
def get_overview(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
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
        ),
        serving=meta,
    )
