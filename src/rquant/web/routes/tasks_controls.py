"""Admit four fixed Lab actions through the existing PageControl authority."""

from __future__ import annotations

import json
from typing import Annotated
from uuid import NAMESPACE_URL, uuid5

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import TypeAdapter, ValidationError

from rquant.lab_job_center import CommandSubmissionResult
from rquant.lab_job_protocol import (
    CancelJobCommand,
    PauseJobCommand,
    ResumeJobCommand,
    RetryJobCommand,
)
from rquant.page_control import SubmitLabCommand
from rquant.web.lab_control_gateway import (
    LabControlConflictError,
    LabControlInvalidReceiptError,
    LabControlUnavailableError,
    LabControlWireReceipt,
)
from rquant.web.models.lab_controls import (
    LabControlCapabilities,
    LabControlReceipt,
    LabControlRequest,
)
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/tasks/jobs")
MAX_REQUEST_BYTES = 1024
_RESULT = TypeAdapter(CommandSubmissionResult)
_REASONS = {
    "pause": "网页暂停研究任务",
    "resume": "网页恢复研究任务",
    "cancel": "网页取消研究任务",
    "retry": "网页重试研究任务",
}
_COMMANDS = {
    "pause": PauseJobCommand,
    "resume": ResumeJobCommand,
    "cancel": CancelJobCommand,
    "retry": RetryJobCommand,
}


@router.get(
    "/control-capabilities",
    response_model=LabControlCapabilities,
    summary="研究任务操作权限",
)
def control_capabilities(
    request: Request,
    viewer: Annotated[str | None, Depends(current_user)],
) -> LabControlCapabilities:
    web = request.app.state.web
    return LabControlCapabilities(
        can_control=(
            viewer is not None
            and web.settings.ingress_socket_path is not None
            and web.proxy_identity is not None
            and viewer in web.settings.lab_control_users
        )
    )


def _interaction_key(viewer: str, body: LabControlRequest) -> str:
    return f"web.lab-control:{viewer}:{body.job_id}:{body.action}:{body.expected_version}"


def _public_receipt(
    body: LabControlRequest, wire: LabControlWireReceipt, *, interaction_key: str
) -> LabControlReceipt:
    if wire.status != "succeeded":
        if wire.result is not None:
            raise LabControlInvalidReceiptError("unfinished PageControl receipt has a result")
        return LabControlReceipt(
            command_id=body.command_id,
            status="unknown" if wire.status in {"failed", "ambiguous"} else wire.status,
            message="提交状态待确认，请查询或重试原请求。",
        )
    if wire.completed_at is None or wire.error is not None or not isinstance(wire.result, dict):
        raise LabControlInvalidReceiptError("completed PageControl receipt is invalid")
    try:
        result = _RESULT.validate_json(json.dumps(wire.result))
    except (TypeError, ValueError, ValidationError) as error:
        raise LabControlInvalidReceiptError("Lab command result is invalid") from error
    request_id = uuid5(NAMESPACE_URL, f"rquant.lab-job-center.interaction:{interaction_key}")
    if result.request_id != request_id or result.job_id != body.job_id:
        raise LabControlInvalidReceiptError("Lab command identity differs")
    if result.result == "submitted":
        if result.command_type != body.action or result.expected_version != body.expected_version:
            raise LabControlInvalidReceiptError("Lab submitted action differs")
        return LabControlReceipt(
            command_id=body.command_id,
            status="submitted",
            message="已提交，等待状态更新。",
        )
    if result.result == "stale" and result.expected_version != body.expected_version:
        raise LabControlInvalidReceiptError("Lab stale version differs")
    if result.result == "unavailable" and result.command_type != body.action:
        raise LabControlInvalidReceiptError("Lab unavailable action differs")
    return LabControlReceipt(
        command_id=body.command_id,
        status="conflict",
        message=(
            "任务已更新，请刷新后重试。"
            if result.result == "stale"
            else "当前状态不能执行此操作，请刷新后查看。"
            if result.result == "unavailable"
            else "请求未被接受，请刷新后查看。"
        ),
    )


@router.post(
    "/commands",
    response_model=LabControlReceipt,
    responses={409: {"model": LabControlReceipt}},
    summary="控制研究任务",
)
async def submit_lab_control(
    request: Request,
    body: LabControlRequest,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> LabControlReceipt | JSONResponse:
    web = request.app.state.web
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if (
        web.settings.ingress_socket_path is None
        or web.proxy_identity is None
        or viewer not in web.settings.lab_control_users
    ):
        raise HTTPException(status_code=403, detail="当前账号不能操作研究任务。")
    if len(await request.body()) > MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="请求内容过长，请重试。")
    interaction_key = _interaction_key(viewer, body)
    command = _COMMANDS[body.action](
        job_id=body.job_id,
        expected_version=body.expected_version,
        reason=_REASONS[body.action],
    )
    payload = SubmitLabCommand(
        command_id=str(body.command_id),
        requested_at=body.requested_at,
        command=command,
        interaction_key=interaction_key,
    ).model_dump(mode="json")
    try:
        wire = await anyio.to_thread.run_sync(web.lab_controls.submit, payload)
        public = _public_receipt(body, wire, interaction_key=interaction_key)
        if public.status == "conflict":
            return JSONResponse(status_code=409, content=public.model_dump(mode="json"))
        return public
    except LabControlConflictError:
        conflict = LabControlReceipt(
            command_id=body.command_id,
            status="conflict",
            message="请求编号已用于其他内容，请核对原请求。",
        )
        return JSONResponse(status_code=409, content=conflict.model_dump(mode="json"))
    except LabControlUnavailableError as error:
        raise HTTPException(status_code=503, detail="提交状态待确认，请重试原请求。") from error
    except LabControlInvalidReceiptError as error:
        raise HTTPException(status_code=502, detail="回执待核对，请重试原请求。") from error
