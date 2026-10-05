"""Private price-rule reads and original-command admission."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from loguru import logger

from rquant.runtime_contracts import normalize_aware_utc
from rquant.strict_json import strict_json_loads
from rquant.web.envelope import Envelope, ServingState
from rquant.web.models.price_alert_rules import (
    PriceAlertRuleCommandReceipt,
    PriceAlertRuleCommandRequest,
    PriceAlertRuleHeadData,
    PriceAlertRuleListData,
    PriceAlertRuleMember,
)
from rquant.web.price_alert_commands import execute_price_rule, private_meta
from rquant.web.price_alert_read import (
    PRIORITY_LABELS,
    PriceAlertRuleView,
    read_price_alert_rules,
    rule_item,
)
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/monitor/price-rules")
MAX_COMMAND_REQUEST_BYTES = 8 * 1024


def _viewer(request: Request, viewer: Annotated[str | None, Depends(current_user)]) -> str:
    if request.app.state.web.settings.ingress_socket_path is None:
        raise HTTPException(503, "规则暂未开放。")
    if viewer is None:
        raise HTTPException(401, "请先登录。")
    allowed = {"rule_id"} if request.url.path.endswith("/head") else set()
    keys = list(request.query_params.keys())
    if set(keys) - allowed or any(len(request.query_params.getlist(key)) != 1 for key in keys):
        raise HTTPException(422, "查询条件有误，请刷新后重试。")
    return viewer


def _message(view: PriceAlertRuleView) -> str:
    return {
        "ready": "",
        "not_activated": "规则尚未开放。",
        "unavailable": "规则暂不可用，请稍后重试。",
    }[view.availability]


@router.get("", response_model=Envelope[PriceAlertRuleListData], summary="我的到价规则")
def list_price_rules(
    request: Request, owner: Annotated[str, Depends(_viewer)]
) -> Envelope[PriceAlertRuleListData]:
    web = request.app.state.web
    now = normalize_aware_utc(web.clock())
    with web.tracker.borrow() as borrowed:
        meta = private_meta(request, borrowed, now)
        view = PriceAlertRuleView(availability="unavailable")
        if borrowed is not None and meta.state is ServingState.READY:
            try:
                view = read_price_alert_rules(borrowed, owner_id=owner, now=now)
            except Exception:
                logger.exception("Private price rule list unavailable")
        writable = (
            view.availability == "ready"
            and view.members_ready
            and getattr(web, "price_alert_admission", None) is not None
        )
        return Envelope(
            serving=meta,
            data=PriceAlertRuleListData(
                availability=view.availability,
                message=_message(view),
                available_at=view.available_at,
                items=[rule_item(row, view, now=now) for row in view.rules if not row.deleted],
                members=[
                    PriceAlertRuleMember(
                        ts_code=row.ts_code, version=row.version, expires_at=row.expires_at
                    )
                    for row in view.members
                    if not row.deleted and (row.expires_at is None or row.expires_at > now)
                ],
                can_write=writable,
                write_message=""
                if writable
                else "规则操作暂未开放。"
                if getattr(web, "price_alert_admission", None) is None
                else "规则或盯盘名单暂不可用，请稍后重试。",
                priority_options=[
                    {"value": value, "label": label} for value, label in PRIORITY_LABELS.items()
                ],
            ),
        )


@router.get("/head", response_model=Envelope[PriceAlertRuleHeadData], summary="到价规则当前设置")
def price_rule_head(
    request: Request,
    owner: Annotated[str, Depends(_viewer)],
    rule_id: Annotated[str, Query(min_length=1, max_length=128)],
) -> Envelope[PriceAlertRuleHeadData]:
    web = request.app.state.web
    now = normalize_aware_utc(web.clock())
    with web.tracker.borrow() as borrowed:
        meta = private_meta(request, borrowed, now)
        view = PriceAlertRuleView(availability="unavailable")
        if borrowed is not None and meta.state is ServingState.READY:
            try:
                view = read_price_alert_rules(borrowed, owner_id=owner, now=now)
            except Exception:
                logger.exception("Private price rule exact head unavailable")
        row = next((row for row in view.rules if row.rule_id == rule_id), None)
        return Envelope(
            serving=meta,
            data=PriceAlertRuleHeadData(
                availability=view.availability,
                message=_message(view),
                rule_id=rule_id,
                status=None
                if view.availability != "ready"
                else "absent"
                if row is None
                else "deleted"
                if row.deleted
                else "live",
                version=None if row is None else row.version,
                item=None if row is None or row.deleted else rule_item(row, view, now=now),
            ),
        )


async def _strict_body(request: Request) -> None:
    raw = await request.body()
    if len(raw) > MAX_COMMAND_REQUEST_BYTES:
        raise HTTPException(413, "请求过长，请减少内容后重试。")
    try:
        strict_json_loads(
            raw, parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite JSON"))
        )
    except (ValueError, UnicodeError) as error:
        raise HTTPException(422, "请求内容有误，请检查后重试。") from error


def _send(
    request: Request, body: PriceAlertRuleCommandRequest, owner: str, *, resume: bool
) -> PriceAlertRuleCommandReceipt | JSONResponse:
    response, status = execute_price_rule(request, body, owner, resume_only=resume)
    return (
        response
        if status == 200
        else JSONResponse(status_code=status, content=response.model_dump(mode="json"))
    )


@router.post(
    "/commands", response_model=PriceAlertRuleCommandReceipt, summary="保存、启停或删除到价规则"
)
def submit_price_rule(
    request: Request,
    body: PriceAlertRuleCommandRequest,
    owner: Annotated[str, Depends(_viewer)],
    _csrf: Annotated[None, Depends(require_csrf)],
    _strict: Annotated[None, Depends(_strict_body)],
) -> PriceAlertRuleCommandReceipt | JSONResponse:
    return _send(request, body, owner, resume=False)


@router.post(
    "/commands/resume",
    response_model=PriceAlertRuleCommandReceipt,
    summary="继续核对原到价规则操作",
)
def resume_price_rule(
    request: Request,
    body: PriceAlertRuleCommandRequest,
    owner: Annotated[str, Depends(_viewer)],
    _csrf: Annotated[None, Depends(require_csrf)],
    _strict: Annotated[None, Depends(_strict_body)],
) -> PriceAlertRuleCommandReceipt | JSONResponse:
    return _send(request, body, owner, resume=True)
