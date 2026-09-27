"""Admit a read-only plan task; its generated artifact is a separate result."""

from __future__ import annotations

import re
from typing import Annotated

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Request

from rquant.web.backfill_plan_command_gateway import (
    BackfillPlanCommandConflictError,
    BackfillPlanCommandInvalidReceiptError,
    BackfillPlanCommandUnavailableError,
    BackfillPlanWireReceipt,
)
from rquant.web.models.backfill_plan_commands import (
    BackfillPlanCommandReceipt,
    BackfillPlanCommandRequest,
)
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/data/backfill-plans")
MAX_REQUEST_BYTES = 4096
_TASK_ID = re.compile(r"[0-9a-f]{32}\Z")


def _public_receipt(
    body: BackfillPlanCommandRequest, wire: BackfillPlanWireReceipt
) -> BackfillPlanCommandReceipt:
    if wire.status == "succeeded":
        result = wire.result
        if (
            not isinstance(result, dict)
            or set(result) != {"outcome", "task_id"}
            or result["outcome"] != "task_queued"
            or not isinstance(result["task_id"], str)
            or _TASK_ID.fullmatch(result["task_id"]) is None
        ):
            raise BackfillPlanCommandInvalidReceiptError("queued task result is invalid")
        return BackfillPlanCommandReceipt(
            command_id=body.command_id,
            status="queued",
            task_id=result["task_id"],
            message="已排队，等待生成",
        )
    return BackfillPlanCommandReceipt(
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
    response_model=BackfillPlanCommandReceipt,
    summary="生成历史日线回补计划",
)
async def submit_backfill_plan_command(
    request: Request,
    body: BackfillPlanCommandRequest,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> BackfillPlanCommandReceipt:
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if len(await request.body()) > MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="请求内容过长，请重试。")
    web = request.app.state.web
    payload = {
        "kind": "submit_backfill_plan",
        **body.model_dump(mode="json"),
        "actor_id": viewer,
    }
    try:
        wire = await anyio.to_thread.run_sync(web.backfill_plan_commands.submit, payload)
        return _public_receipt(body, wire)
    except BackfillPlanCommandConflictError as error:
        raise HTTPException(
            status_code=409, detail="命令内容与已有记录不一致，请保留原请求。"
        ) from error
    except BackfillPlanCommandUnavailableError as error:
        raise HTTPException(status_code=503, detail="提交状态待确认，请使用原请求重试。") from error
    except BackfillPlanCommandInvalidReceiptError as error:
        raise HTTPException(
            status_code=502, detail="回执无法核对，状态待确认，请使用原请求重试。"
        ) from error
