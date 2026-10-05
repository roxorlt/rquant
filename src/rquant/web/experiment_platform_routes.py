"""Owner-bound reads and original PageControl writes for formal experiments."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Annotated, TypeVar

import anyio.to_thread
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel

from rquant.experiment_platform_commands import ExperimentCommand, ExperimentCommandResult
from rquant.web.envelope import Envelope
from rquant.web.experiment_platform_models import (
    ExperimentCapabilities,
    ExperimentComparisonData,
    ExperimentFamilyData,
    ExperimentHeatmapData,
    ExperimentMineData,
    ExperimentResultData,
    ExperimentStatisticsData,
    ExperimentWrite,
    ExperimentWriteReceipt,
)
from rquant.web.experiment_platform_service import ExperimentWebService
from rquant.web.lab_control_gateway import (
    LabControlConflictError,
    LabControlInvalidReceiptError,
    LabControlUnavailableError,
)
from rquant.web.security import require_csrf, require_current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/experiments")
MAX_REQUEST_BYTES = 40 * 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
T = TypeVar("T", bound=BaseModel)


def _service(request: Request) -> ExperimentWebService:
    service = request.app.state.web.experiment_platform
    if service is None:
        raise HTTPException(503, "正式实验暂不可用，请稍后重试。")
    return service


def _response(
    request: Request,
    owner: str,
    operation: Callable[[ExperimentWebService, BorrowedGeneration, str], BaseModel],
    generation_id: str | None = None,
) -> Response:
    web = request.app.state.web
    try:
        with web.tracker.borrow() as borrowed:
            meta = serving_meta(
                borrowed,
                now=web.clock(),
                stale_after=web.settings.stale_after,
                failure=web.tracker.failure,
            )
            if generation_id is not None and generation_id != meta.generation_id:
                raise HTTPException(409, "数据已更新，请重新查看实验。")
            data = operation(_service(request), borrowed, owner)
            value = Envelope(data=data, serving=meta)
            raw = json.dumps(
                value.model_dump(mode="json"),
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
            if len(raw) > MAX_RESPONSE_BYTES:
                raise ValueError("experiment response exceeds 8 MiB")
            headers = {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}
            if meta.generation_id is not None:
                headers["X-Rquant-Generation"] = meta.generation_id
            return Response(raw, media_type="application/json", headers=headers)
    except HTTPException:
        raise
    except PermissionError as error:
        raise HTTPException(404, "找不到这份实验。") from error
    except LookupError as error:
        raise HTTPException(404, "找不到这份实验。") from error
    except ValueError as error:
        raise HTTPException(409, "资料待核对，请重新查看实验。") from error
    except Exception as error:
        raise HTTPException(503, "实验暂无法读取，请稍后重试。") from error


@router.get(
    "/capabilities", response_model=Envelope[ExperimentCapabilities], summary="正式实验能力"
)
def capabilities(
    request: Request, owner: Annotated[str, Depends(require_current_user)]
) -> Response:
    return _response(request, owner, lambda s, b, o: s.capabilities(b, o))


@router.get("/mine", response_model=Envelope[ExperimentMineData], summary="我的正式实验")
def mine(
    request: Request,
    owner: Annotated[str, Depends(require_current_user)],
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    cursor: Annotated[str | None, Query(max_length=1024)] = None,
    generation_id: Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")] = None,
) -> Response:
    if cursor is not None and generation_id is None:
        raise HTTPException(422, "请从首批重新查看实验。")
    return _response(
        request,
        owner,
        lambda s, b, o: s.mine(
            b, o, limit=limit, cursor=cursor, cursor_key=request.app.state.web.cursor_key
        ),
        generation_id,
    )


@router.get(
    "/families/{family_id}", response_model=Envelope[ExperimentFamilyData], summary="完整搜索族"
)
def family(
    request: Request,
    family_id: str,
    owner: Annotated[str, Depends(require_current_user)],
    generation_id: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
) -> Response:
    return _response(request, owner, lambda s, b, o: s.family(b, o, family_id), generation_id)


@router.get(
    "/results/{experiment_id}",
    response_model=Envelope[ExperimentResultData],
    summary="封存实验结果",
)
def result(
    request: Request,
    experiment_id: str,
    owner: Annotated[str, Depends(require_current_user)],
    generation_id: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
    result_hash: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
) -> Response:
    return _response(
        request,
        owner,
        lambda s, b, o: s.result(b, o, experiment_id, result_hash=result_hash),
        generation_id,
    )


@router.get(
    "/compare", response_model=Envelope[ExperimentComparisonData], summary="比较两份封存实验"
)
def compare(
    request: Request,
    owner: Annotated[str, Depends(require_current_user)],
    a: str,
    b: str,
    generation_id: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
) -> Response:
    return _response(request, owner, lambda s, gen, o: s.compare(gen, o, a, b), generation_id)


@router.get(
    "/families/{family_id}/heatmap",
    response_model=Envelope[ExperimentHeatmapData],
    summary="参数热力图与邻域",
)
def heatmap(
    request: Request,
    family_id: str,
    owner: Annotated[str, Depends(require_current_user)],
    selected: str,
    x: str,
    y: str,
    metric: str,
    phase: Annotated[str, Query(pattern=r"^(training|validation|outer)$")],
    generation_id: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
) -> Response:
    return _response(
        request,
        owner,
        lambda s, b, o: s.heatmap(
            b, o, family_id, selected=selected, x=x, y=y, metric=metric, phase=phase
        ),
        generation_id,
    )


@router.get(
    "/results/{experiment_id}/statistics",
    response_model=Envelope[ExperimentStatisticsData],
    summary="实验过拟合证据",
)
def statistics(
    request: Request,
    experiment_id: str,
    owner: Annotated[str, Depends(require_current_user)],
    generation_id: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
) -> Response:
    return _response(
        request, owner, lambda s, b, o: s.statistics(b, o, experiment_id), generation_id
    )


@router.post(
    "/commands", response_model=ExperimentWriteReceipt, summary="登记、取消、备注与样本外解封"
)
async def command(
    request: Request,
    body: ExperimentWrite,
    owner: Annotated[str, Depends(require_current_user)],
    _same_site: Annotated[None, Depends(require_csrf)],
) -> ExperimentWriteReceipt:
    from pydantic import TypeAdapter

    service = _service(request)
    if not service.can_submit(owner, policy=body.kind == "set_experiment_holdout_policy"):
        raise HTTPException(403, "当前账号不能修改这份实验。")
    payload = body.model_dump(mode="json")
    payload["actor_id"] = owner
    command = TypeAdapter(ExperimentCommand).validate_python(payload)
    if len(json.dumps(payload, ensure_ascii=True).encode()) > MAX_REQUEST_BYTES:
        raise HTTPException(413, "搜索参数过多，请减少取值。")
    try:
        wire = await anyio.to_thread.run_sync(service.gateway.submit, payload)
    except LabControlConflictError as error:
        raise HTTPException(409, "请求编号已用于其他内容，请核对原请求。") from error
    except LabControlUnavailableError:
        return ExperimentWriteReceipt(
            command_id=body.command_id, status="unknown", message="提交状态待确认，请重试原请求。"
        )
    except LabControlInvalidReceiptError as error:
        raise HTTPException(502, "回执待核对，请重试原请求。") from error
    if wire.status in ("pending", "processing", "ambiguous"):
        if wire.result is not None or wire.completed_at is not None:
            raise HTTPException(502, "回执待核对，请重试原请求。")
        return ExperimentWriteReceipt(
            command_id=body.command_id,
            status="unknown" if wire.status == "ambiguous" else wire.status,
            message="正在核对提交结果，请重试原请求。",
        )
    if wire.status == "failed":
        if wire.completed_at is None or wire.result is not None or not wire.error:
            raise HTTPException(502, "回执待核对，请重试原请求。")
        return ExperimentWriteReceipt(
            command_id=body.command_id, status="failed", message="请求未完成，请核对来源与参数。"
        )
    try:
        result = ExperimentCommandResult.model_validate(wire.result)
        if (
            (result.command_id, result.owner, result.action)
            != (body.command_id, owner, command.kind)
            or wire.completed_at is None
            or wire.error is not None
        ):
            raise ValueError("experiment receipt identity differs")
        service.verify_receipt(command, result)
    except (ValueError, TypeError) as error:
        raise HTTPException(502, "回执待核对，请重试原请求。") from error
    messages = {
        "registered": "已登记搜索，完成后显示结果。",
        "cancellation_pending": "正在核对取消结果。",
        "cancelled": "未完成项已取消，已有结果仍保留。",
        "already_completed": "实验已完成，结果仍保留。",
        "already_finished": "实验已结束，历史记录仍保留。",
        "note_saved": "备注已保存。",
        "policy_saved": "封存设置已保存。",
        "outer_admitted": "已解封，结果未完成。",
    }
    return ExperimentWriteReceipt(
        command_id=body.command_id,
        status=result.status,
        message=messages[result.status],
        family_id=result.family_id,
        job_ids=result.job_ids,
        planned_count=result.planned_count,
        version=result.version,
    )
