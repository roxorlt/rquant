"""Owner-only full-condition monitoring using the existing admission peer."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from loguru import logger

from rquant.condition_alert_runtime_projection import (
    condition_consumer_ready,
    read_condition_rule_authority,
)
from rquant.runtime_contracts import normalize_aware_utc
from rquant.screen.intraday_contracts import INTRADAY_FIELD_LABELS
from rquant.strict_json import strict_json_loads
from rquant.web.condition_alert_commands import execute_condition_rule
from rquant.web.condition_alert_read import (
    condition_rule_item,
    condition_scope_options,
    owned_condition_heads,
)
from rquant.web.envelope import Envelope, ServingState
from rquant.web.models.condition_alert_rules import (
    ConditionAlertRuleCommandReceipt,
    ConditionAlertRuleCommandRequest,
    ConditionAlertRuleHeadData,
    ConditionAlertRuleListData,
)
from rquant.web.price_alert_commands import private_meta
from rquant.web.screen_catalog import (
    _FUNDAMENTAL_FIELDS,
    RANKING_METRIC_LABELS,
    available_ranking_metrics,
    screen_blocks,
)
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/monitor/condition-rules")
MAX_COMMAND_REQUEST_BYTES = 64 * 1024


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


@router.get("", response_model=Envelope[ConditionAlertRuleListData], summary="我的条件规则")
def list_condition_rules(
    request: Request, owner: Annotated[str, Depends(_viewer)]
) -> Envelope[ConditionAlertRuleListData]:
    web = request.app.state.web
    now = normalize_aware_utc(web.clock())
    with web.tracker.borrow() as borrowed:
        meta = private_meta(request, borrowed, now)
        availability, at, items, scopes, enabled, triggers = "unavailable", None, [], [], False, []
        if borrowed is not None and meta.state is ServingState.READY:
            try:
                authority = read_condition_rule_authority(borrowed, now=now)
                availability = "not_activated" if authority is None else "ready"
                at = None if authority is None else authority.activated_at
                items = [
                    condition_rule_item(row, borrowed, now=now)
                    for row in (owned_condition_heads(borrowed, owner_id=owner, now=now) or ())
                    if not row.deleted
                ]
                scopes = condition_scope_options(borrowed, owner_id=owner, now=now)
                enabled = condition_consumer_ready(borrowed, now=now)
                from rquant.condition_alert_runtime_projection import read_condition_triggers

                triggers = read_condition_triggers(borrowed, owner_id=owner, now=now)
            except Exception:
                logger.exception("Private condition rule list unavailable")
        writable = availability == "ready" and web.price_alert_admission is not None
        return Envelope(
            serving=meta,
            data=ConditionAlertRuleListData(
                availability=availability,
                message=""
                if availability == "ready"
                else "规则尚未开放。"
                if availability == "not_activated"
                else "规则暂不可用，请稍后重试。",
                available_at=at,
                items=items,
                scopes=scopes,
                blocks=screen_blocks(
                    dynamic_ma=True,
                    dynamic_rsi=True,
                    fundamental_fields=tuple(_FUNDAMENTAL_FIELDS),
                    extra_fields=tuple(INTRADAY_FIELD_LABELS.items()),
                    daily_anchor=True,
                ),
                ranking_metrics=available_ranking_metrics(tuple(RANKING_METRIC_LABELS)),
                can_write=writable,
                write_message="" if writable else "规则操作暂未开放。",
                can_enable=enabled,
                enable_message="" if enabled else "提醒暂不可运行，可先保存为停用。",
                triggers=triggers,
            ),
        )


@router.get(
    "/head", response_model=Envelope[ConditionAlertRuleHeadData], summary="条件规则当前设置"
)
def condition_head(
    request: Request,
    owner: Annotated[str, Depends(_viewer)],
    rule_id: Annotated[str, Query(min_length=1, max_length=128)],
) -> Envelope[ConditionAlertRuleHeadData]:
    web = request.app.state.web
    now = normalize_aware_utc(web.clock())
    with web.tracker.borrow() as borrowed:
        meta = private_meta(request, borrowed, now)
        availability, head = "unavailable", None
        if borrowed is not None and meta.state is ServingState.READY:
            try:
                authority = read_condition_rule_authority(borrowed, now=now)
                availability = "not_activated" if authority is None else "ready"
                head = next(
                    (
                        row
                        for row in (owned_condition_heads(borrowed, owner_id=owner, now=now) or ())
                        if row.rule_id == rule_id
                    ),
                    None,
                )
            except Exception:
                logger.exception("Condition exact head unavailable")
        return Envelope(
            serving=meta,
            data=ConditionAlertRuleHeadData(
                availability=availability,
                rule_id=rule_id,
                status=None
                if availability != "ready"
                else "absent"
                if head is None
                else "deleted"
                if head.deleted
                else "live",
                version=None if head is None else head.version,
                item=None
                if head is None or head.deleted
                else condition_rule_item(head, borrowed, now=now),
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
    except (ValueError, UnicodeError) as exc:
        raise HTTPException(422, "请求内容有误，请检查后重试。") from exc


def _send(
    request: Request, body: ConditionAlertRuleCommandRequest, owner: str, *, resume: bool
) -> ConditionAlertRuleCommandReceipt | JSONResponse:
    response, status = execute_condition_rule(request, body, owner, resume_only=resume)
    return (
        response
        if status == 200
        else JSONResponse(status_code=status, content=response.model_dump(mode="json"))
    )


@router.post(
    "/commands", response_model=ConditionAlertRuleCommandReceipt, summary="保存、启停或删除条件规则"
)
def submit_condition_rule(
    request: Request,
    body: ConditionAlertRuleCommandRequest,
    owner: Annotated[str, Depends(_viewer)],
    _csrf: Annotated[None, Depends(require_csrf)],
    _strict: Annotated[None, Depends(_strict_body)],
) -> ConditionAlertRuleCommandReceipt | JSONResponse:
    return _send(request, body, owner, resume=False)


@router.post(
    "/commands/resume",
    response_model=ConditionAlertRuleCommandReceipt,
    summary="继续核对原条件规则操作",
)
def resume_condition_rule(
    request: Request,
    body: ConditionAlertRuleCommandRequest,
    owner: Annotated[str, Depends(_viewer)],
    _csrf: Annotated[None, Depends(require_csrf)],
    _strict: Annotated[None, Depends(_strict_body)],
) -> ConditionAlertRuleCommandReceipt | JSONResponse:
    return _send(request, body, owner, resume=True)
