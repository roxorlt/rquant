"""Submit a formula pool save through the authenticated PageControl boundary."""

from __future__ import annotations

import re
from typing import Annotated

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from rquant.web.formula_market_command_gateway import (
    FormulaMarketCommandConflictError,
    FormulaMarketCommandInvalidReceiptError,
    FormulaMarketCommandUnavailableError,
    FormulaMarketWireReceipt,
)
from rquant.web.models.formula_pool_save_commands import (
    FormulaPoolSaveCommandReceipt,
    FormulaPoolSaveCommandRequest,
)
from rquant.web.security import current_user, require_csrf

router = APIRouter(prefix="/pools/formula")
MAX_REQUEST_BYTES = 4096
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _public_receipt(
    body: FormulaPoolSaveCommandRequest,
    wire: FormulaMarketWireReceipt,
) -> FormulaPoolSaveCommandReceipt:
    if wire.status == "succeeded":
        result = wire.result
        expected_pool_name = f"user/{body.base_name}"
        if (
            wire.completed_at is None
            or wire.error is not None
            or not isinstance(result, dict)
            or set(result) != {"pool_name", "version"}
            or result["pool_name"] != expected_pool_name
            or not isinstance(result["version"], str)
            or _SHA256.fullmatch(result["version"]) is None
        ):
            raise FormulaMarketCommandInvalidReceiptError("saved pool result is invalid")
        return FormulaPoolSaveCommandReceipt(
            command_id=body.command_id,
            status="succeeded",
            pool_name=expected_pool_name,
            version=result["version"],
            message="公式池已保存。",
        )
    return FormulaPoolSaveCommandReceipt(
        command_id=body.command_id,
        status=wire.status,
        message={
            "pending": "状态待确认，请使用原请求重试。",
            "processing": "正在处理，请稍后使用原请求重试。",
            "failed": "保存失败，请检查任务和名称后重试。",
            "ambiguous": "状态待确认，请使用原请求重试。",
        }[wire.status],
    )


@router.post(
    "/commands",
    response_model=FormulaPoolSaveCommandReceipt,
    responses={409: {"model": FormulaPoolSaveCommandReceipt}},
    summary="保存公式池",
)
async def submit_formula_pool_save_command(
    request: Request,
    body: FormulaPoolSaveCommandRequest,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> FormulaPoolSaveCommandReceipt | JSONResponse:
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if len(await request.body()) > MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="请求内容过长，请删减后重试。")
    payload = {
        "kind": "save_formula_pool_v1",
        **body.model_dump(mode="json"),
        "actor_id": viewer,
    }
    try:
        wire = await anyio.to_thread.run_sync(
            request.app.state.web.formula_market_commands.submit, payload
        )
        return _public_receipt(body, wire)
    except FormulaMarketCommandConflictError:
        conflict = FormulaPoolSaveCommandReceipt(
            command_id=body.command_id,
            status="conflict",
            message="请求编号已用于其他内容，请重新发起。",
        )
        return JSONResponse(status_code=409, content=conflict.model_dump(mode="json"))
    except FormulaMarketCommandUnavailableError as error:
        raise HTTPException(status_code=503, detail="保存状态待确认，请使用原请求重试。") from error
    except FormulaMarketCommandInvalidReceiptError as error:
        raise HTTPException(
            status_code=502, detail="回执无法核对，状态待确认，请使用原请求重试。"
        ) from error
