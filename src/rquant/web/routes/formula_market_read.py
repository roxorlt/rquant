"""Read formula market tasks and sealed matches from one Serving generation."""

from __future__ import annotations

import hashlib
import hmac
from base64 import b64decode, urlsafe_b64encode
from binascii import Error as Base64Error
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rquant.formula_market_job_projection import (
    FormulaMarketArtifactIndexRow,
    FormulaMarketJobRow,
    FormulaMarketJobSnapshot,
    read_formula_market_result,
)
from rquant.screen.formula_market_jobs import FormulaMarketJobResult
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.formula_market_read import read_formula_market_snapshot
from rquant.web.models.formula_market_read import (
    FormulaMarketJobDetailData,
    FormulaMarketJobItem,
    FormulaMarketJobListData,
    FormulaMarketMatchesData,
    FormulaMarketResultSummary,
    FormulaMarketUnknownReason,
)
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/screen/tdx/market")
_UNREADABLE = "选股任务暂时无法读取，请稍后重试。"
_RESULT_UNREADABLE = "选股结果暂时无法读取，请稍后重试。"
_CHANGED = "选股结果已更新，请重新打开查看。"
_STATUS = {
    "queued": ("排队中", "正在等待选股。"),
    "running": ("选股中", "正在计算，完成后可查看结果。"),
    "succeeded": ("已完成", "可以查看命中股票。"),
    "failed": ("未完成", "请检查公式和数据后重试。"),
}
_ERROR_HINTS = {
    "source_changed": "数据已更新，请重新发起选股。",
    "formula_rejected": "公式无法计算，请修改后重试。",
    "timeout": "本次计算超时，请缩小公式范围后重试。",
    "capacity": "当前任务较多，请稍后重试。",
    "artifact_invalid": "结果保存未完成，请稍后重试。",
    "lease_exhausted": "计算中断，请重新发起选股。",
    "internal_error": "计算未完成，请稍后重试。",
}
_UNKNOWN_LABELS = {
    "missing_projection_code": "缺少行情资料",
    "listing_conflict": "上市资料不一致",
    "missing_listing": "缺少上市资料",
    "history_budget": "历史数据过多",
    "evaluation_budget": "计算量超限",
    "missing_date": "缺少当日行情",
    "insufficient_history": "历史行情不足",
    "incomplete_history": "历史行情不完整",
    "missing_value": "行情字段缺失",
    "division_by_zero": "公式出现除零",
    "non_finite": "计算结果无效",
    "numeric_underflow": "计算数值过小",
    "never_true": "历史条件尚未成立",
}


class _MatchCursor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["formula_market_matches_v1"] = "formula_market_matches_v1"
    generation_id: str = Field(min_length=1, max_length=128)
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    result_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    offset: int = Field(ge=1, le=10_000)
    page_size: int = Field(ge=1, le=100)


def _segment(raw: bytes) -> str:
    return urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _encode_cursor(cursor: _MatchCursor, key: bytes) -> str:
    payload = cursor.model_dump_json().encode("utf-8")
    return f"{_segment(payload)}.{_segment(hmac.new(key, payload, hashlib.sha256).digest())}"


def _decode_cursor(token: str, key: bytes) -> _MatchCursor:
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
        if not hmac.compare_digest(hmac.new(key, payload, hashlib.sha256).digest(), signature):
            raise ValueError("cursor signature differs")
        return _MatchCursor.model_validate_json(payload)
    except (Base64Error, UnicodeError, ValueError, TypeError, ValidationError) as error:
        raise HTTPException(status_code=409, detail=_CHANGED) from error


def _only_queries(request: Request, allowed: frozenset[str]) -> None:
    if set(request.query_params) - allowed:
        raise HTTPException(status_code=422, detail="查询条件有误，请刷新后重试。")


def _snapshot(borrowed: BorrowedGeneration | None) -> tuple[str, FormulaMarketJobSnapshot | None]:
    try:
        return read_formula_market_snapshot(borrowed)
    except Exception as error:
        raise HTTPException(status_code=503, detail=_UNREADABLE) from error


def _job(snapshot: FormulaMarketJobSnapshot, task_id: str) -> FormulaMarketJobRow:
    found = next((item for item in snapshot.jobs if item.task_id == task_id), None)
    if found is None:
        raise HTTPException(status_code=404, detail="没有找到这项选股任务。")
    return found


def _artifact(
    snapshot: FormulaMarketJobSnapshot, job: FormulaMarketJobRow
) -> FormulaMarketArtifactIndexRow:
    found = next((item for item in snapshot.artifacts if item.task_id == job.task_id), None)
    if found is None:
        raise HTTPException(status_code=503, detail=_RESULT_UNREADABLE)
    return found


def _result(request: Request, artifact: FormulaMarketArtifactIndexRow) -> FormulaMarketJobResult:
    root = request.app.state.web.settings.formula_market_result_root
    if root is None:
        raise HTTPException(status_code=503, detail=_RESULT_UNREADABLE)
    try:
        return read_formula_market_result(root, artifact)
    except Exception as error:
        raise HTTPException(status_code=503, detail=_RESULT_UNREADABLE) from error


def _item(job: FormulaMarketJobRow, *, result_available: bool) -> FormulaMarketJobItem:
    label, hint = _STATUS[job.status]
    if job.status == "failed":
        hint = _ERROR_HINTS.get(job.error_code or "", hint)
    if job.status == "succeeded" and not result_available:
        label, hint = "结果暂不可用", "请稍后重试。"
    return FormulaMarketJobItem(
        task_id=job.task_id,
        status=job.status,
        status_label=label,
        hint=hint,
        formula=job.formula,
        trade_date=job.trade_date,
        created_at=job.created_at,
        updated_at=job.updated_at,
        result_available=result_available,
    )


def _summary(result: FormulaMarketJobResult) -> FormulaMarketResultSummary:
    summary = result.summary
    return FormulaMarketResultSummary(
        market_total=summary.market_total,
        listed_count=summary.listed_count,
        paused_count=summary.paused_count,
        match_count=summary.match_count,
        no_match_count=summary.no_match_count,
        unknown_count=summary.unknown_count,
        unknown_reasons=[
            FormulaMarketUnknownReason(
                reason=reason,
                label=_UNKNOWN_LABELS.get(reason, "其他资料不足"),
                count=count,
            )
            for reason, count in sorted(summary.unknown_reasons.items())
        ],
    )


def _meta(request: Request, borrowed: BorrowedGeneration | None, response: Response) -> ServingMeta:
    web = request.app.state.web
    meta = serving_meta(
        borrowed,
        now=web.clock(),
        stale_after=web.settings.stale_after,
        failure=web.tracker.failure,
    )
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return meta


@router.get("/jobs", response_model=Envelope[FormulaMarketJobListData], summary="公式选股任务")
def list_formula_market_jobs(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FormulaMarketJobListData]:
    _only_queries(request, frozenset())
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = _meta(request, borrowed, response)
        availability, snapshot = _snapshot(
            None if meta.state == ServingState.UNAVAILABLE else borrowed
        )
        if snapshot is None:
            message = (
                "选股任务数据暂不可用，请稍后重试。"
                if availability == "unavailable"
                else "选股任务尚未发布。"
            )
            data = FormulaMarketJobListData(
                availability=availability,
                message=message,
                available_at=None,
                total_task_count=0,
                has_older_tasks=False,
                jobs=[],
            )
        else:
            indices = {item.task_id: item for item in snapshot.artifacts}
            jobs: list[FormulaMarketJobItem] = []
            for job in snapshot.jobs:
                available = False
                if (
                    job.status == "succeeded"
                    and web.settings.formula_market_result_root is not None
                ):
                    try:
                        read_formula_market_result(
                            web.settings.formula_market_result_root, indices[job.task_id]
                        )
                        available = True
                    except Exception:
                        pass
                jobs.append(_item(job, result_available=available))
            data = FormulaMarketJobListData(
                availability=availability,
                message={
                    "unavailable": "选股任务来源暂不可用，请稍后重试。",
                    "empty": "还没有选股任务。",
                    "ready": "",
                }[availability],
                available_at=None if availability == "unavailable" else snapshot.available_at,
                total_task_count=snapshot.state.total_task_count,
                has_older_tasks=snapshot.state.has_older_tasks,
                jobs=jobs,
            )
    return Envelope[FormulaMarketJobListData](data=data, serving=meta)


@router.get(
    "/jobs/{task_id}",
    response_model=Envelope[FormulaMarketJobDetailData],
    summary="公式选股任务详情",
)
def get_formula_market_job(
    task_id: Annotated[str, Path(pattern=r"^[0-9a-f]{32}$")],
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FormulaMarketJobDetailData]:
    _only_queries(request, frozenset())
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = _meta(request, borrowed, response)
        availability, snapshot = _snapshot(
            None if meta.state == ServingState.UNAVAILABLE else borrowed
        )
        if availability in {"unavailable", "not_published"} or snapshot is None:
            raise HTTPException(status_code=503, detail=_UNREADABLE)
        job = _job(snapshot, task_id)
        result = _result(request, _artifact(snapshot, job)) if job.status == "succeeded" else None
        data = FormulaMarketJobDetailData(
            job=_item(job, result_available=result is not None),
            summary=None if result is None else _summary(result),
        )
    return Envelope[FormulaMarketJobDetailData](data=data, serving=meta)


@router.get(
    "/jobs/{task_id}/matches",
    response_model=Envelope[FormulaMarketMatchesData],
    summary="公式选股命中代码",
)
def get_formula_market_matches(
    task_id: Annotated[str, Path(pattern=r"^[0-9a-f]{32}$")],
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    page_size: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
) -> Envelope[FormulaMarketMatchesData]:
    _only_queries(request, frozenset({"page_size", "cursor"}))
    web = request.app.state.web
    decoded = _decode_cursor(cursor, web.cursor_key) if cursor is not None else None
    with web.tracker.borrow() as borrowed:
        meta = _meta(request, borrowed, response)
        if decoded is not None and (
            decoded.generation_id != meta.generation_id
            or decoded.task_id != task_id
            or decoded.page_size != page_size
        ):
            raise HTTPException(status_code=409, detail=_CHANGED)
        availability, snapshot = _snapshot(
            None if meta.state == ServingState.UNAVAILABLE else borrowed
        )
        if availability in {"unavailable", "not_published"} or snapshot is None:
            raise HTTPException(status_code=503, detail=_UNREADABLE)
        job = _job(snapshot, task_id)
        if job.status != "succeeded":
            raise HTTPException(status_code=409, detail="选股尚未完成，请稍后查看。")
        artifact = _artifact(snapshot, job)
        if decoded is not None and decoded.result_sha256 != artifact.content_sha256:
            raise HTTPException(status_code=409, detail=_CHANGED)
        result = _result(request, artifact)
        offset = 0 if decoded is None else decoded.offset
        codes = result.summary.match_codes
        if offset > len(codes):
            raise HTTPException(status_code=409, detail=_CHANGED)
        next_offset = offset + page_size
        next_cursor = None
        if next_offset < len(codes):
            next_cursor = _encode_cursor(
                _MatchCursor(
                    generation_id=meta.generation_id,
                    task_id=task_id,
                    result_sha256=artifact.content_sha256,
                    offset=next_offset,
                    page_size=page_size,
                ),
                web.cursor_key,
            )
        data = FormulaMarketMatchesData(
            task_id=task_id,
            total=len(codes),
            offset=offset,
            match_codes=list(codes[offset:next_offset]),
            next_cursor=next_cursor,
        )
    return Envelope[FormulaMarketMatchesData](data=data, serving=meta)
