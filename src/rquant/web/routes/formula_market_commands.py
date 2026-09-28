"""Admit an offline formula market run through the control authority."""

from __future__ import annotations

import re
from typing import Annotated

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from rquant.screen.tdx.tokens import MAX_SOURCE_BYTES
from rquant.web.formula_market_command_gateway import (
    FormulaMarketCommandConflictError,
    FormulaMarketCommandInvalidReceiptError,
    FormulaMarketCommandUnavailableError,
    FormulaMarketWireReceipt,
)
from rquant.web.models.formula_market_commands import (
    FormulaMarketCommandReceipt,
    FormulaMarketCommandRequest,
)
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/screen/tdx/market")
MAX_REQUEST_BYTES = 8192
_TASK_ID = re.compile(r"[0-9a-f]{32}\Z")


def _public_receipt(
    body: FormulaMarketCommandRequest, wire: FormulaMarketWireReceipt,
) -> FormulaMarketCommandReceipt:
    if wire.status == "succeeded":
        result = wire.result
        if (
            wire.completed_at is not None
            and wire.error is None
            and result == {"outcome": "task_conflict", "reason": "task_active"}
        ):
            return FormulaMarketCommandReceipt(
                command_id=body.command_id,
                status="conflict",
                message="已有选股任务，完成后再试。",
            )
        if (
            wire.completed_at is None
            or wire.error is not None
            or not isinstance(result, dict)
            or set(result) != {"outcome", "task_id"}
            or result["outcome"] != "task_queued"
            or not isinstance(result["task_id"], str)
            or _TASK_ID.fullmatch(result["task_id"]) is None
        ):
            raise FormulaMarketCommandInvalidReceiptError("queued task result is invalid")
        return FormulaMarketCommandReceipt(
            command_id=body.command_id,
            status="queued",
            task_id=result["task_id"],
            message="已提交，等待选股结果。",
        )
    return FormulaMarketCommandReceipt(
        command_id=body.command_id,
        status=wire.status,
        message={
            "pending": "状态待确认，请使用原请求重试。",
            "processing": "正在提交，请稍后使用原请求查看。",
            "failed": "提交失败，请检查公式和数据后重试。",
            "ambiguous": "状态待确认，请使用原请求重试。",
        }[wire.status],
    )


@router.post(
    "/commands",
    response_model=FormulaMarketCommandReceipt,
    responses={409: {"model": FormulaMarketCommandReceipt}},
    summary="提交全市场公式选股",
)
async def submit_formula_market_command(
    request: Request,
    body: FormulaMarketCommandRequest,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> FormulaMarketCommandReceipt | JSONResponse:
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if len(await request.body()) > MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="请求内容过长，请删减后重试。")
    try:
        formula_bytes = body.formula.encode("utf-8")
    except UnicodeEncodeError as error:
        raise HTTPException(status_code=422, detail="公式输入有误，请修改后重试。") from error
    if len(formula_bytes) > MAX_SOURCE_BYTES:
        raise HTTPException(status_code=413, detail="公式太长，请删减后重试。")
    web = request.app.state.web
    payload = {
        "kind": "submit_formula_market_run",
        **body.model_dump(mode="json"),
        "actor_id": viewer,
    }
    try:
        wire = await anyio.to_thread.run_sync(web.formula_market_commands.submit, payload)
        public = _public_receipt(body, wire)
        if public.status == "conflict":
            return JSONResponse(status_code=409, content=public.model_dump(mode="json"))
        return public
    except FormulaMarketCommandConflictError:
        conflict = FormulaMarketCommandReceipt(
            command_id=body.command_id,
            status="conflict",
            message="请求编号已用于其他内容，请重新发起。",
        )
        return JSONResponse(status_code=409, content=conflict.model_dump(mode="json"))
    except FormulaMarketCommandUnavailableError as error:
        raise HTTPException(
            status_code=503, detail="提交状态待确认，请使用原请求重试。"
        ) from error
    except FormulaMarketCommandInvalidReceiptError as error:
        raise HTTPException(
            status_code=502, detail="回执无法核对，状态待确认，请使用原请求重试。"
        ) from error
