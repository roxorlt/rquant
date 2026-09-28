"""Read published formula pools and bounded daily members."""

from __future__ import annotations

import hashlib
import hmac
from base64 import b64decode, urlsafe_b64encode
from binascii import Error as Base64Error
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rquant.formula_pool_definition import _NAME
from rquant.formula_pool_serving_projection import (
    FormulaPoolDefinitionRow,
    FormulaPoolLatestResultRow,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.formula_pool_read import (
    Availability,
    read_formula_pool_snapshot,
    read_indexed_daily_result,
)
from rquant.web.models.formula_pool_read import (
    FormulaPoolItem,
    FormulaPoolLatestResult,
    FormulaPoolListData,
    FormulaPoolMembersData,
    FormulaPoolUnknownReason,
)
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/pools/formula")
_UNREADABLE = "公式池暂时无法读取，请稍后重试。"
_MEMBERS_UNREADABLE = "池子结果暂时无法读取，请稍后重试。"
_CHANGED = "池子结果已更新，请重新打开查看。"
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


class _MembersCursor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["formula_pool_members_v1"] = "formula_pool_members_v1"
    generation_id: str = Field(min_length=1, max_length=128)
    pool_name: str = Field(min_length=6, max_length=85)
    definition_version: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    offset: int = Field(ge=1, le=10_000)
    page_size: int = Field(ge=1, le=100)


def _segment(raw: bytes) -> str:
    return urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _encode_cursor(cursor: _MembersCursor, key: bytes) -> str:
    payload = canonical_json_bytes(cursor.model_dump(mode="json"))
    return f"{_segment(payload)}.{_segment(hmac.new(key, payload, hashlib.sha256).digest())}"


def _decode_cursor(token: str, key: bytes) -> _MembersCursor:
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
            raise ValueError("cursor encoding is not canonical")
        if not hmac.compare_digest(hmac.new(key, payload, hashlib.sha256).digest(), signature):
            raise ValueError("cursor signature differs")
        parsed = _MembersCursor.model_validate(strict_canonical_json_loads(payload))
        if canonical_json_bytes(parsed.model_dump(mode="json")) != payload:
            raise ValueError("cursor payload is not canonical")
        return parsed
    except (Base64Error, UnicodeError, ValueError, TypeError, ValidationError) as error:
        raise HTTPException(status_code=409, detail=_CHANGED) from error


def _only_queries(request: Request, allowed: frozenset[str]) -> None:
    if set(request.query_params) - allowed:
        raise HTTPException(status_code=422, detail="查询条件有误，请刷新后重试。")


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


def _snapshot(
    borrowed: BorrowedGeneration | None,
) -> tuple[
    Availability,
    datetime | None,
    tuple[FormulaPoolDefinitionRow, ...],
    tuple[FormulaPoolLatestResultRow, ...],
]:
    try:
        return read_formula_pool_snapshot(borrowed)
    except Exception as error:
        raise HTTPException(status_code=503, detail=_UNREADABLE) from error


def _latest(index: FormulaPoolLatestResultRow) -> FormulaPoolLatestResult:
    reasons = strict_canonical_json_loads(index.unknown_reasons_json.encode())
    return FormulaPoolLatestResult(
        trade_date=index.trade_date,
        market_total=index.market_total,
        match_count=index.match_count,
        no_match_count=index.no_match_count,
        unknown_count=index.unknown_count,
        unknown_reasons=[
            FormulaPoolUnknownReason(
                reason=reason,
                label=_UNKNOWN_LABELS.get(reason, "其他资料不足"),
                count=count,
            )
            for reason, count in sorted(reasons.items())
            if count > 0
        ],
    )


@router.get("", response_model=Envelope[FormulaPoolListData], summary="公式池列表")
def list_formula_pools(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FormulaPoolListData]:
    _only_queries(request, frozenset())
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = _meta(request, borrowed, response)
        availability, available_at, definitions, latest = _snapshot(
            None if meta.state == ServingState.UNAVAILABLE else borrowed
        )
        by_name = {item.pool_name: item for item in latest}
        pools = [
            FormulaPoolItem(
                pool_name=item.pool_name,
                display_name=item.display_name,
                formula=item.formula,
                syntax_version=item.syntax_version,
                created_at=item.created_at,
                status_label="尚未运行" if item.pool_name not in by_name else "已有结果",
                latest_result=(
                    None if item.pool_name not in by_name else _latest(by_name[item.pool_name])
                ),
            )
            for item in definitions
        ]
        message = {
            "unavailable": "公式池数据暂不可用，请稍后重试。",
            "not_published": "公式池数据尚未发布。",
            "empty": "还没有公式池。",
            "ready": "",
        }[availability]
        data = FormulaPoolListData(
            availability=availability,
            message=message,
            available_at=available_at,
            pools=pools,
        )
    return Envelope[FormulaPoolListData](data=data, serving=meta)


@router.get(
    "/{base_name}/members",
    response_model=Envelope[FormulaPoolMembersData],
    summary="公式池命中代码",
)
def get_formula_pool_members(
    base_name: Annotated[str, Path(min_length=1, max_length=80, pattern=r"^[\w\u4e00-\u9fff-]+$")],
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    page_size: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
) -> Envelope[FormulaPoolMembersData]:
    _only_queries(request, frozenset({"page_size", "cursor"}))
    if _NAME.fullmatch(base_name) is None:
        raise HTTPException(status_code=422, detail="池子名称有误。")
    web = request.app.state.web
    decoded = _decode_cursor(cursor, web.cursor_key) if cursor is not None else None
    pool_name = f"user/{base_name}"
    with web.tracker.borrow() as borrowed:
        meta = _meta(request, borrowed, response)
        if decoded is not None and (
            decoded.generation_id != meta.generation_id
            or decoded.pool_name != pool_name
            or decoded.page_size != page_size
        ):
            raise HTTPException(status_code=409, detail=_CHANGED)
        availability, _, definitions, latest = _snapshot(
            None if meta.state == ServingState.UNAVAILABLE else borrowed
        )
        if availability in {"unavailable", "not_published"}:
            raise HTTPException(status_code=503, detail=_UNREADABLE)
        definition = next((item for item in definitions if item.pool_name == pool_name), None)
        if definition is None:
            raise HTTPException(status_code=404, detail="没有找到这个公式池。")
        index = next((item for item in latest if item.pool_name == pool_name), None)
        if index is None:
            raise HTTPException(status_code=409, detail="这个池子尚未运行。")
        if decoded is not None and (
            decoded.definition_version != definition.version
            or decoded.result_sha256 != index.content_sha256
        ):
            raise HTTPException(status_code=409, detail=_CHANGED)
        root = web.settings.formula_pool_daily_result_root
        if root is None:
            raise HTTPException(status_code=503, detail=_MEMBERS_UNREADABLE)
        try:
            daily = read_indexed_daily_result(root, index)
        except Exception as error:
            raise HTTPException(status_code=503, detail=_MEMBERS_UNREADABLE) from error
        offset = 0 if decoded is None else decoded.offset
        if offset > len(daily.match_codes):
            raise HTTPException(status_code=409, detail=_CHANGED)
        next_offset = offset + page_size
        next_cursor = None
        if next_offset < len(daily.match_codes):
            next_cursor = _encode_cursor(
                _MembersCursor(
                    generation_id=meta.generation_id,
                    pool_name=pool_name,
                    definition_version=definition.version,
                    result_sha256=index.content_sha256,
                    offset=next_offset,
                    page_size=page_size,
                ),
                web.cursor_key,
            )
        data = FormulaPoolMembersData(
            pool_name=pool_name,
            trade_date=index.trade_date,
            total=len(daily.match_codes),
            offset=offset,
            match_codes=list(daily.match_codes[offset:next_offset]),
            next_cursor=next_cursor,
        )
    return Envelope[FormulaPoolMembersData](data=data, serving=meta)
