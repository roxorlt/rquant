"""Read published editor facts and submit bounded PageControl commands."""

from __future__ import annotations

import json
import re
from typing import Annotated

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import ValidationError

from rquant.llm.registry import REGISTRY_BY_NAME
from rquant.screen.loader import FUNDAMENTAL_COLS_MAP
from rquant.web.envelope import Envelope
from rquant.web.models.pool_editor import (
    AttachPoolCommand,
    CreateCanvasCommand,
    PoolEditorCommand,
    PoolEditorData,
    PoolEditorReceipt,
    PoolNlPreview,
    PoolNlPreviewRequest,
    SavePoolCommand,
)
from rquant.web.nl_parser import NlClarificationNeededError, NlParserUnavailableError
from rquant.web.pool_editor_gateway import (
    PoolCommandConflictError,
    PoolCommandInvalidReceiptError,
    PoolCommandUnavailableError,
    PoolCommandWireReceipt,
)
from rquant.web.pool_editor_read import PoolEditorSnapshot, read_pool_editor
from rquant.web.pool_nl_preview import (
    InvalidPoolDraftError,
    NoPoolRuleChangeError,
    validate_pool_draft,
)
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import serving_meta

router = APIRouter(prefix="/pools/editor")
MAX_REQUEST_BYTES = 32_768
MAX_NL_REQUEST_BYTES = 4_096
MAX_NL_BASE_BYTES = 32_768
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_UNPUBLISHED_POOL_COLUMNS = frozenset(f"{name}[0]" for name in FUNDAMENTAL_COLS_MAP.values())


@router.get("", response_model=Envelope[PoolEditorData], summary="可编辑池子与画布")
def get_pool_editor(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[PoolEditorData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        snapshot = read_pool_editor(borrowed)
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    data = snapshot.data.model_copy(
        update={
            "nl_preview_available": (
                web.nl_parser is not None
                and snapshot.data.state == "ready"
                and meta.state == "ready"
            )
        }
    )
    return Envelope[PoolEditorData](data=data, serving=meta)


@router.post("/nl-preview", response_model=PoolNlPreview, summary="预览一句话修改池子")
async def preview_pool_natural_language(
    request: Request,
    body: PoolNlPreviewRequest,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> PoolNlPreview:
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if len(await request.body()) > MAX_NL_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="描述过长，请删减后重试。")
    web = request.app.state.web
    parser = web.nl_parser
    if parser is None:
        raise HTTPException(status_code=503, detail="暂不能生成，仍可手动编辑。")
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if meta.state != "ready" or meta.generation_id != body.generation_id:
            raise HTTPException(status_code=409, detail="池子数据已变化，请刷新后重试。")
        snapshot = read_pool_editor(borrowed)
        if snapshot.data.state != "ready":
            raise HTTPException(status_code=409, detail="池子规则暂不可编辑，请刷新后重试。")
        base = next((pool for pool in snapshot.data.pools if pool.key == body.pool_key), None)
        if base is None:
            detail = (
                "内置池需先复制为自建池。"
                if body.pool_key in snapshot.builtin_names
                else "这份池子规则尚不可编辑，请刷新后重试。"
            )
            raise HTTPException(status_code=409, detail=detail)
        if base.version != body.expected_version:
            raise HTTPException(status_code=409, detail="规则已变化，请刷新后重试。")
        if any(column in _UNPUBLISHED_POOL_COLUMNS for column in base.include_columns):
            raise HTTPException(status_code=409, detail="这份池子含暂不可用的数据项，请手动编辑。")
        base_bytes = json.dumps(
            [rule.model_dump(mode="json") for rule in base.rule_calls], ensure_ascii=False
        ).encode("utf-8")
        if len(base_bytes) > MAX_NL_BASE_BYTES:
            raise HTTPException(status_code=409, detail="这份池子条件较多，请手动编辑。")
    if not web.nl_rate_limiter.admit(viewer):
        raise HTTPException(
            status_code=429, detail="操作太频繁，请一分钟后再试。", headers={"Retry-After": "60"}
        )
    if not web.nl_gate.acquire(blocking=False):
        raise HTTPException(
            status_code=429, detail="正在生成，请稍后再试。", headers={"Retry-After": "1"}
        )
    try:
        try:
            raw = await anyio.to_thread.run_sync(
                parser.parse_edit, body.instruction.strip(), base.rule_calls
            )
        except NlClarificationNeededError as error:
            raise HTTPException(
                status_code=422, detail="请具体说明要增加、删除或修改哪个条件。"
            ) from error
        except NlParserUnavailableError as error:
            raise HTTPException(status_code=503, detail="暂不能生成，请稍后重试。") from error
        with web.tracker.borrow() as borrowed:
            meta = serving_meta(
                borrowed,
                now=web.clock(),
                stale_after=web.settings.stale_after,
                failure=web.tracker.failure,
            )
            snapshot = read_pool_editor(borrowed)
            current = next(
                (pool for pool in snapshot.data.pools if pool.key == body.pool_key), None
            )
            if (
                meta.state != "ready"
                or meta.generation_id != body.generation_id
                or snapshot.data.state != "ready"
                or current is None
                or current.version != body.expected_version
            ):
                raise HTTPException(status_code=409, detail="池子数据已变化，请刷新后重试。")
        try:
            calls, changes = validate_pool_draft(raw, base)
        except NoPoolRuleChangeError as error:
            raise HTTPException(
                status_code=422, detail="没有识别出规则变化，请说得更具体。"
            ) from error
        except InvalidPoolDraftError as error:
            raise HTTPException(
                status_code=422, detail="没能生成可用的修改，请换一种说法。"
            ) from error
        return PoolNlPreview(
            pool_key=base.key,
            base_generation_id=body.generation_id,
            base_version=base.version,
            rule_calls=calls,
            changes=changes,
        )
    finally:
        web.nl_gate.release()


def _authorize(snapshot: PoolEditorSnapshot, body: PoolEditorCommand) -> None:
    if isinstance(body, CreateCanvasCommand):
        if snapshot.data.state != "ready" or not snapshot.data.canvas_create_available:
            raise HTTPException(status_code=409, detail="画布尚不可创建，请刷新后重试。")
        return
    if snapshot.data.state != "ready":
        raise HTTPException(status_code=409, detail="池子规则尚不可编辑，请刷新后重试。")
    if isinstance(body, AttachPoolCommand):
        if body.canvas_name not in {canvas.name for canvas in snapshot.data.canvases}:
            raise HTTPException(status_code=409, detail="当前画布尚不可编辑，请刷新后重试。")
        return
    key = f"user/{body.base_name}"
    if key in snapshot.present_user_names and key not in {pool.key for pool in snapshot.data.pools}:
        raise HTTPException(status_code=409, detail="这份池子规则尚不可编辑，请刷新后重试。")
    if body.base_name in snapshot.builtin_names and key not in snapshot.present_user_names:
        raise HTTPException(status_code=409, detail="内置池需先复制为自建池。")
    for rule in body.rule_calls:
        if any(
            type(value) is str and value in _UNPUBLISHED_POOL_COLUMNS
            for value in rule.args.values()
        ):
            raise HTTPException(status_code=422, detail="这个数据项在池子中暂不可用。")
        spec = REGISTRY_BY_NAME.get(rule.name)
        if spec is None or not set(rule.args) <= set(spec.args_model.model_fields):
            raise HTTPException(status_code=422, detail="选股条件有误，请检查后重试。")
        try:
            spec.args_model.model_validate(rule.args)
        except (TypeError, ValueError, ValidationError) as error:
            raise HTTPException(status_code=422, detail="选股条件有误，请检查后重试。") from error
    if any(column in _UNPUBLISHED_POOL_COLUMNS for column in body.include_columns):
        raise HTTPException(status_code=422, detail="这个数据项在池子中暂不可用。")
    if any(not 1 <= len(column) <= 64 for column in body.include_columns):
        raise HTTPException(status_code=422, detail="选股字段有误，请检查后重试。")


def _failed_message(error: str | None, body: PoolEditorCommand) -> str:
    text = (error or "").lower()
    if "clock skew" in text:
        return "设备时间可能不准确，请校准后重试。"
    if isinstance(body, CreateCanvasCommand):
        if "canvas name is already occupied" in text:
            return "画布名称已被使用，请换一个名称。"
        if "canvas current head" in text or "watermark" in text:
            return "画布状态已变化，请刷新后重试。"
        return "画布创建失败，请检查后重试。"
    if "version conflict" in text or "definition changed" in text:
        return "规则已变化，请刷新后重试。"
    if "parent pool" in text or "dependency" in text:
        return "父池已变化，请检查后重试。"
    return "保存失败，请检查条件后重试。"


def _receipt(body: PoolEditorCommand, wire: PoolCommandWireReceipt) -> PoolEditorReceipt:
    status = wire.status
    if status == "succeeded":
        result = wire.result
        if not isinstance(result, dict):
            raise PoolCommandInvalidReceiptError("succeeded command has no result")
        if isinstance(body, SavePoolCommand):
            version = result.get("version")
            if not isinstance(version, str) or _SHA256.fullmatch(version) is None:
                raise PoolCommandInvalidReceiptError("saved pool version is invalid")
            return PoolEditorReceipt(
                command_id=body.command_id,
                status="succeeded",
                message="池子已保存",
                pool_version=version,
            )
        if isinstance(body, CreateCanvasCommand):
            record_hash = result.get("record_hash")
            receipt_id = result.get("publication_receipt_id")
            if (
                result.get("canvas_name") != body.name
                or not isinstance(record_hash, str)
                or _SHA256.fullmatch(record_hash) is None
                or not isinstance(receipt_id, str)
                or _SHA256.fullmatch(receipt_id) is None
            ):
                raise PoolCommandInvalidReceiptError(
                    "created canvas publication identity is invalid"
                )
            return PoolEditorReceipt(
                command_id=body.command_id,
                status="succeeded",
                message="画布已保存，等待发布",
                canvas_name=body.name,
                canvas_record_hash=record_hash,
            )
        if (
            result.get("canvas_name") != body.canvas_name
            or result.get("pool_name") != body.pool_name
            or result.get("pool_version") != body.expected_pool_version
            or not isinstance(result.get("publication_receipt_id"), str)
            or _SHA256.fullmatch(result["publication_receipt_id"]) is None
        ):
            raise PoolCommandInvalidReceiptError("canvas attachment identity is invalid")
        return PoolEditorReceipt(
            command_id=body.command_id,
            status="succeeded",
            message="池子已加入当前画布",
            pool_version=body.expected_pool_version,
            canvas_name=body.canvas_name,
        )
    return PoolEditorReceipt(
        command_id=body.command_id,
        status=status,
        message={
            "pending": "已受理，等待处理",
            "processing": "正在处理",
            "failed": _failed_message(wire.error, body),
            "ambiguous": "状态待确认，请保留原请求。",
        }[status],
    )


@router.post("/commands", response_model=PoolEditorReceipt, summary="保存池子或管理画布")
async def submit_pool_editor_command(
    request: Request,
    body: PoolEditorCommand,
    viewer: Annotated[str | None, Depends(current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> PoolEditorReceipt:
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if len(await request.body()) > MAX_REQUEST_BYTES:
        raise HTTPException(status_code=413, detail="条件内容过长，请删减后重试。")
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        _authorize(read_pool_editor(borrowed), body)
    payload = body.model_dump(mode="json")
    try:
        wire = await anyio.to_thread.run_sync(web.pool_commands.submit, payload)
        return _receipt(body, wire)
    except PoolCommandConflictError as error:
        raise HTTPException(
            status_code=409, detail="命令内容与已有记录不一致，请刷新后重试。"
        ) from error
    except PoolCommandUnavailableError as error:
        raise HTTPException(
            status_code=503, detail="连接暂不可用，状态待确认，请使用原请求重试。"
        ) from error
    except PoolCommandInvalidReceiptError as error:
        raise HTTPException(
            status_code=502, detail="回执无法核对，状态待确认，请使用原请求重试。"
        ) from error
