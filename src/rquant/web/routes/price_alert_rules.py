"""Owner-bound price-rule reads and private command admission."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from loguru import logger

from rquant.page_control import (
    DeletePriceAlertRule,
    PageControlReceipt,
    PageControlStatus,
    SavePriceAlertRule,
    SetPriceAlertRuleEnabled,
)
from rquant.price_alert_admission import (
    PriceAlertAdmissionClient,
    PriceAlertAdmissionRejectedError,
    PriceAlertAdmissionUnavailableError,
)
from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_manual_watchlist_projection import ManualWatchlistProjectionRow
from rquant.serving_price_alert_rule_projection import PriceAlertRuleProjectionRow
from rquant.serving_publisher import ServingReader
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.models.price_alert_rule import (
    DeletePriceAlertRuleRequest,
    PriceAlertRuleCommandReceipt,
    PriceAlertRuleCommandRequest,
    PriceAlertRuleConflictReason,
    PriceAlertRuleItemData,
    PriceAlertRuleListData,
    SavePriceAlertRuleRequest,
    SetPriceAlertRuleEnabledRequest,
)
from rquant.web.price_alert_rule_read import read_price_alert_rules
from rquant.web.security import current_user, require_csrf, require_current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/monitor/rules")
MAX_COMMAND_REQUEST_BYTES = 8192
_UNAVAILABLE = "价格规则暂不可用，请稍后重试。"
_CHANGED = "规则已变化，请刷新后重试。"
_COMMAND = SavePriceAlertRule | SetPriceAlertRuleEnabled | DeletePriceAlertRule


class _PreflightConflict(HTTPException):
    def __init__(self, detail: str, reason: PriceAlertRuleConflictReason) -> None:
        super().__init__(status_code=409, detail=detail)
        self.reason = reason


def _viewer(request: Request, viewer: Annotated[str | None, Depends(current_user)]) -> str:
    if request.app.state.web.settings.ingress_socket_path is None:
        raise HTTPException(status_code=503, detail="价格规则暂未开放。")
    owner = require_current_user(viewer)
    if request.query_params:
        raise HTTPException(status_code=422, detail="查询条件有误，请刷新后重试。")
    return owner


def _meta(request: Request, borrowed: BorrowedGeneration | None, now: datetime) -> ServingMeta:
    web = request.app.state.web
    meta = serving_meta(
        borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure
    )
    return meta.model_copy(update={"detail": ""})


def _current_pointer_matches(request: Request, borrowed: BorrowedGeneration) -> bool:
    current = ServingReader(request.app.state.web.settings.serving_root).current_pointer()
    return (
        borrowed.pointer is not None
        and current.generation_id == borrowed.pointer.generation_id
        and current.manifest_sha256 == borrowed.pointer.manifest_sha256
    )


def _scope_status(
    row: PriceAlertRuleProjectionRow,
    members: dict[tuple[str, str], ManualWatchlistProjectionRow],
    now: datetime,
) -> str:
    if row.deleted:
        return "deleted"
    assert row.ts_code is not None
    member = members.get((row.owner_id, row.ts_code))
    if member is None or member.deleted:
        return "removed"
    if member.expires_at is not None and member.expires_at <= now:
        return "expired"
    if member.version != row.membership_version:
        return "changed"
    return "valid"


@router.get("", response_model=Envelope[PriceAlertRuleListData], summary="我的价格规则")
def list_price_alert_rules(
    request: Request, owner_id: Annotated[str, Depends(_viewer)]
) -> Envelope[PriceAlertRuleListData]:
    web = request.app.state.web
    web.tracker.refresh()
    now = normalize_aware_utc(web.clock())
    with web.tracker.borrow() as borrowed:
        meta = _meta(request, borrowed, now)
        if borrowed is not None and meta.state is ServingState.READY:
            try:
                read = read_price_alert_rules(borrowed, now=now)
                if not _current_pointer_matches(request, borrowed):
                    raise ValueError("Serving pointer changed during price rule read")
                if read.availability == "not_ready":
                    return Envelope(
                        data=PriceAlertRuleListData(
                            availability="not_ready",
                            message="价格规则尚未开放。",
                            available_at=None,
                            items=[],
                        ),
                        serving=meta,
                    )
                if read.availability == "unavailable":
                    return Envelope(
                        data=PriceAlertRuleListData(
                            availability="unavailable",
                            message=_UNAVAILABLE,
                            available_at=None,
                            items=[],
                        ),
                        serving=meta,
                    )
                members = {(row.owner_id, row.ts_code): row for row in read.members}
                items = [
                    PriceAlertRuleItemData(
                        **row.model_dump(exclude={"owner_id"}),
                        scope_status=_scope_status(row, members, now),
                    )
                    for row in read.rules
                    if row.owner_id == owner_id
                ]
                return Envelope(
                    data=PriceAlertRuleListData(
                        availability="ready",
                        message="暂无价格规则。" if not items else "",
                        available_at=read.available_at,
                        items=items,
                    ),
                    serving=meta,
                )
            except Exception:
                logger.exception("Private price rule list read failed")
    return Envelope(
        data=PriceAlertRuleListData(
            availability="unavailable", message=_UNAVAILABLE, available_at=None, items=[]
        ),
        serving=meta,
    )


def _command(body: PriceAlertRuleCommandRequest) -> _COMMAND:
    common = {"command_id": body.command_id, "requested_at": body.requested_at}
    if isinstance(body, SavePriceAlertRuleRequest):
        return SavePriceAlertRule(
            **common,
            ts_code=body.ts_code,
            membership_version=body.membership_version,
            expected_version=body.expected_version,
            rule=body.rule,
        )
    if isinstance(body, SetPriceAlertRuleEnabledRequest):
        return SetPriceAlertRuleEnabled(
            **common,
            rule_id=body.rule_id,
            expected_version=body.expected_version,
            enabled=body.enabled,
        )
    return DeletePriceAlertRule(
        **common, rule_id=body.rule_id, expected_version=body.expected_version
    )


def _rule_id(body: PriceAlertRuleCommandRequest) -> str:
    return body.rule.rule_id if isinstance(body, SavePriceAlertRuleRequest) else body.rule_id


def _new_command_allowed(request: Request, body: PriceAlertRuleCommandRequest, owner: str) -> None:
    web = request.app.state.web
    web.tracker.refresh()
    now = normalize_aware_utc(web.clock())
    try:
        with web.tracker.borrow() as borrowed:
            meta = _meta(request, borrowed, now)
            if borrowed is None or meta.state is not ServingState.READY:
                raise HTTPException(status_code=503, detail=_UNAVAILABLE)
            if meta.generation_id != body.generation_id:
                raise _PreflightConflict(_CHANGED, "generation_changed")
            read = read_price_alert_rules(borrowed, now=now)
            if read.availability != "ready":
                raise HTTPException(status_code=503, detail=_UNAVAILABLE)
            current = next(
                (
                    row
                    for row in read.rules
                    if row.owner_id == owner and row.rule_id == _rule_id(body)
                ),
                None,
            )
            expected = body.expected_version
            if (None if current is None else current.version) != expected:
                raise _PreflightConflict(_CHANGED, "version_conflict")
            if isinstance(body, SavePriceAlertRuleRequest):
                member = next(
                    (
                        row
                        for row in read.members
                        if row.owner_id == owner and row.ts_code == body.ts_code
                    ),
                    None,
                )
                if (
                    member is None
                    or member.deleted
                    or member.expires_at is not None
                    and member.expires_at <= now
                    or member.version != body.membership_version
                ):
                    raise _PreflightConflict("盯盘状态已变化，请刷新后重试。", "membership_changed")
            elif current is None or current.deleted:
                raise _PreflightConflict(_CHANGED, "version_conflict")
            if not _current_pointer_matches(request, borrowed):
                raise _PreflightConflict(_CHANGED, "generation_changed")
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Private price rule command preflight failed")
        raise HTTPException(status_code=503, detail=_UNAVAILABLE) from exc


def _matches_published(
    body: PriceAlertRuleCommandRequest, row: PriceAlertRuleProjectionRow
) -> bool:
    if row.deleted:
        return isinstance(body, DeletePriceAlertRuleRequest)
    if isinstance(body, DeletePriceAlertRuleRequest):
        return False
    if isinstance(body, SetPriceAlertRuleEnabledRequest):
        return row.enabled == body.enabled
    rule = body.rule
    return (
        row.ts_code == body.ts_code
        and row.membership_version == body.membership_version
        and row.name == rule.name
        and row.priority == rule.priority
        and row.enabled == rule.enabled
        and row.comparison == rule.comparison
        and row.threshold == format(rule.threshold, "f")
        and row.valid_from == rule.valid_from.isoformat()
        and row.valid_until == rule.valid_until.isoformat()
    )


def _published(
    request: Request,
    body: PriceAlertRuleCommandRequest,
    owner: str,
    receipt: PageControlReceipt,
) -> bool:
    if receipt.completed_at is None:
        raise ValueError("successful price rule receipt lacks completion time")
    web = request.app.state.web
    web.tracker.refresh()
    now = normalize_aware_utc(web.clock())
    try:
        with web.tracker.borrow() as borrowed:
            meta = _meta(request, borrowed, now)
            if (
                borrowed is None
                or meta.state is not ServingState.READY
                or meta.generation_id == body.generation_id
                or borrowed.manifest.built_at <= receipt.completed_at
            ):
                return False
            read = read_price_alert_rules(borrowed, now=now)
            if read.availability != "ready" or not _current_pointer_matches(request, borrowed):
                return False
            row = next(
                (
                    row
                    for row in read.rules
                    if row.owner_id == owner and row.rule_id == _rule_id(body)
                ),
                None,
            )
            return (
                row is not None
                and row.version == receipt.result["version"]
                and row.updated_at >= receipt.completed_at
                and _matches_published(body, row)
            )
    except Exception:
        logger.exception("Private price rule publication check failed")
        return False


def _receipt(
    request: Request,
    body: PriceAlertRuleCommandRequest,
    owner: str,
    receipt: PageControlReceipt,
) -> PriceAlertRuleCommandReceipt:
    if receipt.command_id != body.command_id or receipt.enqueued_at != body.requested_at:
        raise ValueError("price rule receipt does not match command")
    common = {"command_id": body.command_id, "kind": body.kind, "rule_id": _rule_id(body)}
    if receipt.status is PageControlStatus.SUCCEEDED:
        result = receipt.result
        action = (
            "save"
            if isinstance(body, SavePriceAlertRuleRequest)
            else "set_enabled"
            if isinstance(body, SetPriceAlertRuleEnabledRequest)
            else "delete"
        )
        expected_enabled = (
            body.rule.enabled
            if isinstance(body, SavePriceAlertRuleRequest)
            else body.enabled
            if isinstance(body, SetPriceAlertRuleEnabledRequest)
            else None
        )
        if (
            not isinstance(result, dict)
            or result.get("rule_id") != _rule_id(body)
            or result.get("action") != action
            or type(result.get("version")) is not int
            or result["version"] != (body.expected_version or 0) + 1
            or type(result.get("deleted")) is not bool
            or result["deleted"] is not isinstance(body, DeletePriceAlertRuleRequest)
            or (
                result.get("enabled") is not None
                if expected_enabled is None
                else type(result.get("enabled")) is not bool
                or result["enabled"] is not expected_enabled
            )
        ):
            raise ValueError("price rule receipt result is invalid")
        published = _published(request, body, owner, receipt)
        return PriceAlertRuleCommandReceipt(
            **common,
            status="published" if published else "saved_syncing",
            version=result["version"],
            message="规则已删除。"
            if published and action == "delete"
            else "规则已保存。"
            if published
            else "已保存，正在同步；请稍后核对规则。",
        )
    if receipt.status is PageControlStatus.FAILED:
        result = receipt.result
        if (
            not isinstance(result, dict)
            or result.get("rule_id") != _rule_id(body)
            or result.get("action")
            != (
                "save"
                if isinstance(body, SavePriceAlertRuleRequest)
                else "set_enabled"
                if isinstance(body, SetPriceAlertRuleEnabledRequest)
                else "delete"
            )
        ):
            raise ValueError("failed price rule receipt result is invalid")
        status, reason = {
            "version_conflict": ("conflict", "version_conflict"),
            "scope_invalid": ("conflict", "membership_changed"),
            "capacity_exceeded": ("capacity", "capacity_exceeded"),
        }.get(result.get("code"), ("failed", None))
        return PriceAlertRuleCommandReceipt(
            **common,
            status=status,
            reason=reason,
            message={
                "conflict": "规则或盯盘状态已变化，请刷新后重试。",
                "capacity": "价格规则已满，请删除其他规则后重试。",
                "failed": "请求失败，请检查后重新发起。",
            }[status],
        )
    if receipt.status is PageControlStatus.AMBIGUOUS:
        status = "uncertain"
        message = "状态待核对，请使用原请求重试。"
    else:
        status = receipt.status.value
        message = "已受理，等待处理。" if status == "pending" else "正在处理，请稍后核对。"
    return PriceAlertRuleCommandReceipt(**common, status=status, message=message)


def _reply(
    body: PriceAlertRuleCommandRequest,
    *,
    status: str,
    message: str,
    http_status: int,
    reason: PriceAlertRuleConflictReason | None = None,
) -> JSONResponse:
    response = PriceAlertRuleCommandReceipt.model_validate(
        {
            "command_id": body.command_id,
            "kind": body.kind,
            "rule_id": _rule_id(body),
            "status": status,
            "reason": reason,
            "message": message,
        }
    )
    return JSONResponse(status_code=http_status, content=response.model_dump(mode="json"))


def _uncertain(body: PriceAlertRuleCommandRequest, *, http_status: int = 503) -> JSONResponse:
    return _reply(
        body,
        status="uncertain",
        message="状态待核对，请保留原请求并重试。",
        http_status=http_status,
    )


def _rejected(
    body: PriceAlertRuleCommandRequest, error: PriceAlertAdmissionRejectedError
) -> JSONResponse:
    if str(error) != "command_conflict":
        return _uncertain(body)
    return _reply(
        body,
        status="conflict",
        message="命令内容已变化，请保留原请求。",
        http_status=409,
        reason="command_conflict",
    )


def _public_receipt(
    request: Request,
    body: PriceAlertRuleCommandRequest,
    owner: str,
    receipt: PageControlReceipt,
) -> PriceAlertRuleCommandReceipt | JSONResponse:
    try:
        rendered = _receipt(request, body, owner, receipt)
    except ValueError:
        logger.exception("Private price rule receipt cannot be verified")
        return _uncertain(body, http_status=502)
    if rendered.status in {"conflict", "capacity"}:
        return JSONResponse(status_code=409, content=rendered.model_dump(mode="json"))
    return rendered


def _existing(
    request: Request,
    body: PriceAlertRuleCommandRequest,
    owner: str,
    command: _COMMAND,
    admission: PriceAlertAdmissionClient,
    original: PageControlReceipt,
) -> PriceAlertRuleCommandReceipt | JSONResponse:
    if original.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}:
        try:
            original = admission.resume(command, authenticated_owner_id=owner)
        except PriceAlertAdmissionRejectedError as error:
            return _rejected(body, error) if str(error) == "command_conflict" else _uncertain(body)
        except PriceAlertAdmissionUnavailableError:
            try:
                recovered = admission.lookup(command, authenticated_owner_id=owner)
            except (PriceAlertAdmissionRejectedError, PriceAlertAdmissionUnavailableError):
                return _uncertain(body)
            if recovered is None:
                return _uncertain(body)
            original = recovered
    return _public_receipt(request, body, owner, original)


@router.post("/commands", response_model=PriceAlertRuleCommandReceipt, summary="保存或删除价格规则")
def submit_price_alert_rule_command(
    request: Request,
    body: PriceAlertRuleCommandRequest,
    owner_id: Annotated[str, Depends(_viewer)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> PriceAlertRuleCommandReceipt | JSONResponse:
    admission = request.app.state.web.price_rule_admission
    if admission is None:
        raise HTTPException(status_code=503, detail="价格规则操作暂未开放。")
    command = _command(body)
    try:
        original = admission.lookup(command, authenticated_owner_id=owner_id)
    except PriceAlertAdmissionRejectedError as error:
        return _rejected(body, error)
    except PriceAlertAdmissionUnavailableError:
        return _uncertain(body)
    if original is not None:
        return _existing(request, body, owner_id, command, admission, original)
    try:
        _new_command_allowed(request, body, owner_id)
    except HTTPException as rejection:
        try:
            raced = admission.lookup(command, authenticated_owner_id=owner_id)
        except PriceAlertAdmissionRejectedError as error:
            return _rejected(body, error)
        except PriceAlertAdmissionUnavailableError:
            return _uncertain(body)
        if raced is not None:
            return _existing(request, body, owner_id, command, admission, raced)
        return _reply(
            body,
            status="conflict" if rejection.status_code == 409 else "uncertain",
            message=str(rejection.detail),
            http_status=rejection.status_code,
            reason=rejection.reason if isinstance(rejection, _PreflightConflict) else None,
        )
    try:
        receipt = admission.submit(command, authenticated_owner_id=owner_id)
    except PriceAlertAdmissionRejectedError as error:
        return _rejected(body, error)
    except PriceAlertAdmissionUnavailableError:
        try:
            raced = admission.lookup(command, authenticated_owner_id=owner_id)
        except PriceAlertAdmissionRejectedError as error:
            return _rejected(body, error)
        except PriceAlertAdmissionUnavailableError:
            return _uncertain(body)
        if raced is None:
            return _uncertain(body)
        return _existing(request, body, owner_id, command, admission, raced)
    return _public_receipt(request, body, owner_id, receipt)
