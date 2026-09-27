"""Admit a read-only audit task; its generated report is a separate result."""

from __future__ import annotations

import re
from typing import Annotated

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Request

from rquant.web.data_audit_report_command_gateway import (
    AuditReportCommandConflictError,
    AuditReportCommandInvalidReceiptError,
    AuditReportCommandUnavailableError,
    AuditReportWireReceipt,
)
from rquant.web.models.data_audit_report_commands import (
    AuditReportCommandReceipt,
    AuditReportCommandRequest,
)
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/data/audit-report")
MAX_REQUEST_BYTES = 4096
_TASK_ID = re.compile(r"[0-9a-f]{32}\Z")


def _public_receipt(
    body: AuditReportCommandRequest, wire: AuditReportWireReceipt
) -> AuditReportCommandReceipt:
    if wire.status == "succeeded":
        result = wire.result
        if (
            wire.completed_at is None
            or wire.error is not None
            or not isinstance(result, dict)
            or set(result) != {"outcome", "task_id"}
            or result["outcome"] != "task_queued"
            or not isinstance(result["task_id"], str)
            or _TASK_ID.fullmatch(result["task_id"]) is None
        ):
            raise AuditReportCommandInvalidReceiptError("queued task result is invalid")
        return AuditReportCommandReceipt(
            command_id=body.command_id,
            status="queued",
            task_id=result["task_id"],
            message="已排队，等待生成",
        )
    return AuditReportCommandReceipt(
        command_id=body.command_id,
        status=wire.status,
        message={
            "pending": "已受理，等待处理",
            "processing": "正在处理",
            "failed": "请求失败，请检查后重新发起。",
            "ambiguous": "状态待确认，请使用原请求重试。",
        }[wire.status],
    )


@router.post(
    "/commands",
    response_model=AuditReportCommandReceipt,
    summary="生成日线数据审计报告",
)
async def submit_data_audit_report_command(
    request: Request,
    body: AuditReportCommandRequest,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> AuditReportCommandReceipt:
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if len(await request.body()) > MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="请求内容过长，请重试。")
    web = request.app.state.web
    payload = {
        "kind": "submit_data_audit_report",
        **body.model_dump(mode="json"),
        "actor_id": viewer,
    }
    try:
        wire = await anyio.to_thread.run_sync(web.audit_report_commands.submit, payload)
        return _public_receipt(body, wire)
    except AuditReportCommandConflictError as error:
        raise HTTPException(
            status_code=409, detail="命令内容与已有记录不一致，请保留原请求。"
        ) from error
    except AuditReportCommandUnavailableError as error:
        raise HTTPException(status_code=503, detail="提交状态待确认，请使用原请求重试。") from error
    except AuditReportCommandInvalidReceiptError as error:
        raise HTTPException(
            status_code=502, detail="回执无法核对，状态待确认，请使用原请求重试。"
        ) from error
