"""Private manual watchlist reads and owner-bound command admission."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Request
from fastapi.responses import JSONResponse
from loguru import logger

from rquant.manual_watchlist import ManualWatchlistDelete, ManualWatchlistUpsert, WatchlistSource
from rquant.page_control import (
    AddWatchlistItem,
    PageControlReceipt,
    PageControlStatus,
    RemoveWatchlistItem,
)
from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_manual_watchlist_projection import ManualWatchlistProjectionRow
from rquant.serving_publisher import ServingReader
from rquant.watchlist_admission import (
    WatchlistAdmissionClient,
    WatchlistAdmissionRejectedError,
    WatchlistAdmissionUnavailableError,
)
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.manual_watchlist_read import read_manual_watchlist, read_manual_watchlist_item
from rquant.web.models.manual_watchlist import (
    ManualWatchlistCommandReceipt,
    ManualWatchlistCommandRequest,
    ManualWatchlistExactData,
    ManualWatchlistItemData,
    ManualWatchlistListData,
)
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/watchlist")
_UNAVAILABLE = "名单暂不可用，请稍后重试。"
MAX_COMMAND_REQUEST_BYTES = 4096


def _viewer(request: Request, viewer: Annotated[str | None, Depends(current_user)]) -> str:
    if request.app.state.web.settings.ingress_socket_path is None:
        raise HTTPException(status_code=503, detail="名单暂未开放。")
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if request.query_params:
        raise HTTPException(status_code=422, detail="查询条件有误，请刷新后重试。")
    return viewer


def _meta(request: Request, borrowed: BorrowedGeneration | None, now: datetime) -> ServingMeta:
    web = request.app.state.web
    meta = serving_meta(
        borrowed,
        now=now,
        stale_after=web.settings.stale_after,
        failure=web.tracker.failure,
    )
    # Technical diagnostics belong in server logs, never in private page responses.
    return meta.model_copy(update={"detail": ""})


def _item(row: ManualWatchlistProjectionRow) -> ManualWatchlistItemData:
    assert row.source is not None and row.updated_at is not None
    return ManualWatchlistItemData(
        ts_code=row.ts_code,
        version=row.version,
        source=row.source,
        price_levels=json.loads(row.price_levels_json),
        expires_at=row.expires_at,
        updated_at=row.updated_at,
    )


@router.get("", response_model=Envelope[ManualWatchlistListData], summary="我的盯盘名单")
def list_manual_watchlist(
    request: Request,
    owner_id: Annotated[str, Depends(_viewer)],
) -> Envelope[ManualWatchlistListData]:
    web = request.app.state.web
    now = normalize_aware_utc(web.clock())
    with web.tracker.borrow() as borrowed:
        meta = _meta(request, borrowed, now)
        if borrowed is not None and meta.state is ServingState.READY:
            try:
                available_at, rows = read_manual_watchlist(borrowed, owner_id=owner_id, now=now)
            except Exception:
                logger.exception("Private manual watchlist list read failed")
                available_at, rows = None, ()
            if available_at is not None:
                items = [_item(row) for row in rows]
                return Envelope(
                    data=ManualWatchlistListData(
                        availability="ready",
                        message="名单为空。" if not items else "",
                        available_at=available_at,
                        items=items,
                    ),
                    serving=meta,
                )
    return Envelope(
        data=ManualWatchlistListData(
            availability="unavailable", message=_UNAVAILABLE, available_at=None, items=[]
        ),
        serving=meta,
    )


@router.get("/{ts_code}", response_model=Envelope[ManualWatchlistExactData], summary="盯盘状态")
def get_manual_watchlist_item(
    request: Request,
    owner_id: Annotated[str, Depends(_viewer)],
    ts_code: Annotated[str, Path(min_length=9, max_length=9, pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")],
) -> Envelope[ManualWatchlistExactData]:
    web = request.app.state.web
    now = normalize_aware_utc(web.clock())
    with web.tracker.borrow() as borrowed:
        meta = _meta(request, borrowed, now)
        if borrowed is not None and meta.state is ServingState.READY:
            try:
                available_at, row = read_manual_watchlist_item(
                    borrowed, owner_id=owner_id, ts_code=ts_code
                )
            except Exception:
                logger.exception("Private manual watchlist exact read failed")
                available_at, row = None, None
            if available_at is not None:
                status = "absent"
                if row is not None:
                    status = (
                        "deleted"
                        if row.deleted
                        else "expired"
                        if row.expires_at is not None and row.expires_at <= now
                        else "active"
                    )
                return Envelope(
                    data=ManualWatchlistExactData(
                        availability="ready",
                        message="",
                        available_at=available_at,
                        ts_code=ts_code,
                        status=status,
                        version=None if row is None else row.version,
                        source=None if row is None else row.source,
                        price_levels=[] if row is None else json.loads(row.price_levels_json),
                        expires_at=None if row is None else row.expires_at,
                        updated_at=None if row is None else row.updated_at,
                    ),
                    serving=meta,
                )
    return Envelope(
        data=ManualWatchlistExactData(
            availability="unavailable",
            message=_UNAVAILABLE,
            available_at=None,
            ts_code=ts_code,
            status=None,
            version=None,
            source=None,
            price_levels=[],
            expires_at=None,
            updated_at=None,
        ),
        serving=meta,
    )


def _command(
    body: ManualWatchlistCommandRequest, owner_id: str
) -> AddWatchlistItem | RemoveWatchlistItem:
    common = {"command_id": body.command_id, "requested_at": body.requested_at}
    if body.action == "add":
        return AddWatchlistItem(
            **common,
            item=ManualWatchlistUpsert(
                owner_id=owner_id,
                ts_code=body.ts_code,
                expected_version=body.expected_version,
                source=body.source or WatchlistSource.DETAIL,
                price_levels=body.price_levels or (),
                expires_at=body.expires_at,
            ),
        )
    assert body.expected_version is not None
    return RemoveWatchlistItem(
        **common,
        item=ManualWatchlistDelete(
            owner_id=owner_id,
            ts_code=body.ts_code,
            expected_version=body.expected_version,
        ),
    )


def _new_command_allowed(request: Request, body: ManualWatchlistCommandRequest, owner: str) -> None:
    web = request.app.state.web
    web.tracker.refresh()
    now = normalize_aware_utc(web.clock())
    try:
        with web.tracker.borrow() as borrowed:
            meta = _meta(request, borrowed, now)
            if borrowed is None or meta.state is not ServingState.READY:
                raise HTTPException(status_code=503, detail=_UNAVAILABLE)
            if meta.generation_id != body.generation_id:
                raise HTTPException(status_code=409, detail="名单已更新，请刷新后重试。")
            available_at, row = read_manual_watchlist_item(
                borrowed, owner_id=owner, ts_code=body.ts_code
            )
            if available_at is None:
                raise HTTPException(status_code=503, detail=_UNAVAILABLE)
            version = None if row is None else row.version
            if version != body.expected_version:
                raise HTTPException(status_code=409, detail="名单版本已变化，请刷新后重试。")
            if body.action == "remove" and (
                row is None or row.deleted or (row.expires_at is not None and row.expires_at <= now)
            ):
                raise HTTPException(status_code=409, detail="名单状态已变化，请刷新后重试。")
            pointer = ServingReader(web.settings.serving_root).current_pointer()
            if (
                borrowed.pointer is None
                or pointer.generation_id != body.generation_id
                or pointer.manifest_sha256 != borrowed.pointer.manifest_sha256
            ):
                raise HTTPException(status_code=409, detail="名单已更新，请刷新后重试。")
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Private manual watchlist command preflight failed")
        raise HTTPException(status_code=503, detail=_UNAVAILABLE) from exc


def _published(
    request: Request, body: ManualWatchlistCommandRequest, owner: str, receipt: PageControlReceipt
) -> bool:
    if receipt.completed_at is None:
        raise ValueError("successful watchlist receipt lacks completion time")
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
            ):
                return False
            available_at, row = read_manual_watchlist_item(
                borrowed, owner_id=owner, ts_code=body.ts_code
            )
            result = receipt.result
            pointer = ServingReader(web.settings.serving_root).current_pointer()
            return (
                available_at is not None
                and borrowed.manifest.built_at > receipt.completed_at
                and row is not None
                and row.version == result["version"]
                and borrowed.pointer is not None
                and pointer.generation_id == borrowed.pointer.generation_id
                and pointer.manifest_sha256 == borrowed.pointer.manifest_sha256
                and (
                    not row.deleted and (row.expires_at is None or row.expires_at > now)
                    if body.action == "add"
                    else row.deleted
                )
            )
    except Exception:
        logger.exception("Private manual watchlist publication check failed")
        return False


def _receipt(
    request: Request,
    body: ManualWatchlistCommandRequest,
    owner: str,
    receipt: PageControlReceipt,
) -> ManualWatchlistCommandReceipt:
    if receipt.command_id != body.command_id or receipt.enqueued_at != body.requested_at:
        raise ValueError("watchlist receipt does not match command")
    if receipt.status is PageControlStatus.SUCCEEDED:
        result = receipt.result
        if (
            not isinstance(result, dict)
            or result.get("ts_code") != body.ts_code
            or result.get("action") != body.action
            or result.get("state") != ("active" if body.action == "add" else "deleted")
            or type(result.get("version")) is not int
            or result["version"] < 1
        ):
            raise ValueError("watchlist receipt result is invalid")
        published = _published(request, body, owner, receipt)
        return ManualWatchlistCommandReceipt(
            command_id=body.command_id,
            ts_code=body.ts_code,
            action=body.action,
            status="published" if published else "saved_syncing",
            version=result["version"],
            message=("已加入盯盘。" if body.action == "add" else "已移出盯盘。")
            if published
            else "已保存，正在同步；请稍后核对名单。",
        )
    if receipt.status is PageControlStatus.FAILED:
        result = receipt.result
        if (
            not isinstance(result, dict)
            or result.get("ts_code") != body.ts_code
            or result.get("action") != body.action
        ):
            raise ValueError("failed watchlist receipt result is invalid")
        code = result.get("code")
        status = {
            "version_conflict": "conflict",
            "capacity_exceeded": "capacity",
        }.get(code, "failed")
        message = {
            "conflict": "名单版本已变化，请刷新后重试。",
            "capacity": "盯盘名单已满，请移出其他股票后重试。",
            "failed": "请求失败，请检查后重新发起。",
        }[status]
        return ManualWatchlistCommandReceipt(
            command_id=body.command_id,
            ts_code=body.ts_code,
            action=body.action,
            status=status,
            message=message,
        )
    status = "uncertain" if receipt.status is PageControlStatus.AMBIGUOUS else receipt.status.value
    return ManualWatchlistCommandReceipt(
        command_id=body.command_id,
        ts_code=body.ts_code,
        action=body.action,
        status=status,
        message={
            "pending": "已受理，等待处理。",
            "processing": "正在处理，请稍后核对。",
            "ambiguous": "状态待核对，请使用原请求重试。",
        }[receipt.status.value],
    )


def _reply(
    body: ManualWatchlistCommandRequest,
    *,
    status: str,
    message: str,
    http_status: int,
) -> JSONResponse:
    response = ManualWatchlistCommandReceipt.model_validate(
        {
            "command_id": body.command_id,
            "ts_code": body.ts_code,
            "action": body.action,
            "status": status,
            "message": message,
        }
    )
    return JSONResponse(status_code=http_status, content=response.model_dump(mode="json"))


def _uncertain(body: ManualWatchlistCommandRequest, *, http_status: int = 503) -> JSONResponse:
    return _reply(
        body,
        status="uncertain",
        message="状态待核对，请保留原请求并重试。",
        http_status=http_status,
    )


def _rejected(
    body: ManualWatchlistCommandRequest, error: WatchlistAdmissionRejectedError
) -> JSONResponse:
    if str(error) == "owner_mismatch":
        return _reply(body, status="failed", message="身份无法核对，请重新登录。", http_status=403)
    if str(error) in {"not_found", "rejected"}:
        return _uncertain(body)
    return _reply(
        body,
        status="conflict",
        message="命令内容或名单状态已变化，请刷新后保留原请求。",
        http_status=409,
    )


def _public_receipt(
    request: Request,
    body: ManualWatchlistCommandRequest,
    owner: str,
    receipt: PageControlReceipt,
) -> ManualWatchlistCommandReceipt | JSONResponse:
    try:
        rendered = _receipt(request, body, owner, receipt)
    except ValueError:
        logger.exception("Private manual watchlist receipt cannot be verified")
        return _uncertain(body, http_status=502)
    if rendered.status in {"conflict", "capacity"}:
        return JSONResponse(status_code=409, content=rendered.model_dump(mode="json"))
    return rendered


def _existing(
    request: Request,
    body: ManualWatchlistCommandRequest,
    owner: str,
    command: AddWatchlistItem | RemoveWatchlistItem,
    admission: WatchlistAdmissionClient,
    original: PageControlReceipt,
) -> ManualWatchlistCommandReceipt | JSONResponse:
    if original.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}:
        try:
            original = admission.resume(command, authenticated_owner_id=owner)
        except WatchlistAdmissionRejectedError as error:
            if str(error) == "command_conflict":
                return _rejected(body, error)
            return _uncertain(body)
        except WatchlistAdmissionUnavailableError:
            try:
                recovered = admission.lookup(command, authenticated_owner_id=owner)
            except (WatchlistAdmissionRejectedError, WatchlistAdmissionUnavailableError):
                return _uncertain(body)
            if recovered is None:
                return _uncertain(body)
            original = recovered
    return _public_receipt(request, body, owner, original)


@router.post(
    "/commands",
    response_model=ManualWatchlistCommandReceipt,
    summary="加入或移出我的盯盘名单",
)
def submit_manual_watchlist_command(
    request: Request,
    body: ManualWatchlistCommandRequest,
    owner_id: Annotated[str, Depends(_viewer)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> ManualWatchlistCommandReceipt | JSONResponse:
    web = request.app.state.web
    admission = web.watchlist_admission
    if admission is None:
        raise HTTPException(status_code=503, detail="名单操作暂未开放，请稍后重试。")
    command = _command(body, owner_id)
    try:
        original = admission.lookup(command, authenticated_owner_id=owner_id)
    except WatchlistAdmissionRejectedError as error:
        return _rejected(body, error)
    except WatchlistAdmissionUnavailableError:
        return _uncertain(body)
    if original is not None:
        return _existing(request, body, owner_id, command, admission, original)
    try:
        _new_command_allowed(request, body, owner_id)
    except HTTPException as rejection:
        try:
            raced = admission.lookup(command, authenticated_owner_id=owner_id)
        except WatchlistAdmissionRejectedError as error:
            return _rejected(body, error)
        except WatchlistAdmissionUnavailableError:
            return _uncertain(body)
        if raced is not None:
            return _existing(request, body, owner_id, command, admission, raced)
        return _reply(
            body,
            status="conflict" if rejection.status_code == 409 else "uncertain",
            message=str(rejection.detail),
            http_status=rejection.status_code,
        )
    try:
        receipt = admission.submit(command, authenticated_owner_id=owner_id)
    except WatchlistAdmissionRejectedError as error:
        return _rejected(body, error)
    except WatchlistAdmissionUnavailableError:
        try:
            raced = admission.lookup(command, authenticated_owner_id=owner_id)
        except WatchlistAdmissionRejectedError as error:
            return _rejected(body, error)
        except WatchlistAdmissionUnavailableError:
            return _uncertain(body)
        if raced is None:
            return _uncertain(body)
        return _existing(request, body, owner_id, command, admission, raced)
    return _public_receipt(request, body, owner_id, receipt)
