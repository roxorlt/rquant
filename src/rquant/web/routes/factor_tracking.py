"""Same-generation tracking reads and original trusted toggle recovery."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response

from rquant.factor.tracking import (
    FactorTrackingOperationResult,
    FactorTrackingPanel,
    FactorTrackingRequest,
)
from rquant.factor.tracking_serving import (
    FactorTrackingServingSnapshot,
    validate_factor_tracking_projections,
)
from rquant.factor_tracking_admission import (
    FactorTrackingAdmissionRejectedError,
    FactorTrackingAdmissionUnavailableError,
)
from rquant.serving_publisher import ServingReader
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.web.envelope import Envelope, ServingState
from rquant.web.routes.factor_runs import _envelope
from rquant.web.routes.factors import _read_catalog, _verified_registry_instance_id
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/factors")
MAX_TRACKING_REQUEST_BYTES = 8192
_TABLES = ("factor_tracking", "factor_tracking_state")


def _read_tracking(borrowed: BorrowedGeneration | None) -> FactorTrackingServingSnapshot | None:
    if borrowed is None:
        return None
    try:
        marks = borrowed.cursor.execute(
            "SELECT table_name, available, row_count, owner_dataset_id, owner_generation_id, "
            "available_at FROM projection_status WHERE table_name IN (?, ?) "
            "ORDER BY table_name LIMIT 3",
            _TABLES,
        ).fetchall()
        if not marks and all(name not in borrowed.manifest.row_counts for name in _TABLES):
            return None
        if (
            len(marks) != 2
            or tuple(row[0] for row in marks) != _TABLES
            or any(type(row[1]) is not bool or type(row[2]) is not int for row in marks)
        ):
            raise ValueError("tracking projection status is incomplete")
        if all(not row[1] for row in marks):
            if any(
                row[2] != 0
                or row[3] != "lab_jobs"
                or row[4] is not None
                or row[5] is not None
                or borrowed.manifest.row_counts.get(row[0], 0) != 0
                for row in marks
            ):
                raise ValueError("unpublished tracking projection has rows")
            return None
        watermark = next(
            (w for w in borrowed.manifest.watermarks if w.dataset_id == "lab_jobs"), None
        )
        at = marks[0][5]
        if watermark is None or any(
            not row[1]
            or row[3] != "lab_jobs"
            or row[4] != watermark.generation_id
            or row[5] != at
            or at is None
            or at > borrowed.manifest.built_at
            or row[2] != borrowed.manifest.row_counts.get(row[0])
            for row in marks
        ):
            raise ValueError("tracking projection differs from Serving generation")
        projections = {}
        for name, _, count, *_ in marks:
            contract = PAGE_PROJECTION_CONTRACTS[name]
            rows = borrowed.cursor.execute(
                f"SELECT {', '.join(contract.column_names)} FROM {name} "
                f"ORDER BY {', '.join(contract.sort_keys)} LIMIT ?",
                (contract.max_rows + 1,),
            ).fetchall()
            if len(rows) != count or len(rows) > contract.max_rows:
                raise ValueError("tracking physical rows differ")
            projections[name] = ServingProjectionPayload(
                table_name=name,
                available_at=at,
                rows=tuple(
                    dict(zip(contract.column_names, values, strict=True)) for values in rows
                ),
            )
        snapshot = validate_factor_tracking_projections(projections)
        if snapshot is None or snapshot.registry_instance_id != _verified_registry_instance_id(
            borrowed
        ):
            raise ValueError("tracking registry differs from same-generation definitions")
        catalog = _read_catalog(borrowed)
        if not set(p.factor_id for p in snapshot.panels) <= set(
            d.factor_id for d in catalog.definitions
        ):
            raise ValueError("tracking definition is absent")
        return snapshot
    except Exception as error:
        raise HTTPException(status_code=503, detail="因子跟踪数据暂时无法核验。") from error


@router.get(
    "/{factor_id}/tracking", response_model=Envelope[FactorTrackingPanel], summary="因子持续跟踪"
)
def factor_tracking_panel(
    request: Request,
    response: Response,
    factor_id: Annotated[str, Path(pattern=r"^[a-z][a-z0-9_]{0,63}$")],
    viewer: Annotated[str | None, Depends(current_user)],
    generation_id: Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")] = None,
) -> Envelope[FactorTrackingPanel]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if generation_id is not None and generation_id != meta.generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请刷新跟踪面板。")
        snapshot = _read_tracking(borrowed)
        if snapshot is None:
            panel = FactorTrackingPanel(
                factor_id=factor_id,
                availability="unavailable",
                status="unavailable",
                reason="跟踪数据尚未发布。",
            )
        else:
            catalog = _read_catalog(borrowed)
            definition = next((d for d in catalog.definitions if d.factor_id == factor_id), None)
            if definition is None:
                raise HTTPException(status_code=404, detail="因子尚未发布。")
            panel = next((p for p in snapshot.panels if p.factor_id == factor_id), None)
            if panel is None:
                from rquant.factor.registry import FactorHeadRef

                panel = FactorTrackingPanel(
                    factor_id=factor_id,
                    availability="not_tracked",
                    status="not_tracked",
                    definition_head=FactorHeadRef(
                        version=definition.version, content_sha256=definition.content_sha256
                    ),
                )
            allowed = (
                web.settings.factor_tracking_enabled
                and web.factor_tracking_admission is not None
                and viewer in web.settings.factor_tracking_users
                and (not definition.archived or panel.tracked)
            )
            panel = panel.model_copy(update={"can_set_tracked": allowed})
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        return Envelope(data=panel, serving=meta)


def _preflight(request: Request, body: FactorTrackingRequest) -> str:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if borrowed is None or meta.state is not ServingState.READY:
            raise HTTPException(status_code=503, detail="跟踪来源暂不可用。")
        if meta.generation_id != body.serving_generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请刷新后重试。")
        snapshot = _read_tracking(borrowed)
        if snapshot is None:
            raise HTTPException(status_code=503, detail="跟踪数据尚未发布。")
        catalog = _read_catalog(borrowed)
        definition = next((d for d in catalog.definitions if d.factor_id == body.factor_id), None)
        if (
            definition is None
            or (body.tracked and definition.archived)
            or definition.version != body.expected_head.version
            or definition.content_sha256 != body.expected_head.content_sha256
        ):
            raise HTTPException(status_code=409, detail="定义已变化，请刷新后重试。")
        current = next((p for p in snapshot.panels if p.factor_id == body.factor_id), None)
        if body.expected_tracking_generation != (
            None if current is None else current.tracking_generation
        ):
            raise HTTPException(status_code=409, detail="跟踪状态已变化，请刷新后重试。")
        pointer = ServingReader(web.settings.serving_root).current_pointer()
        if (
            borrowed.pointer is None
            or pointer.generation_id != body.serving_generation_id
            or pointer.manifest_sha256 != borrowed.pointer.manifest_sha256
        ):
            raise HTTPException(status_code=409, detail="数据已更新，请刷新后重试。")
        return snapshot.registry_instance_id


def _toggle(
    request: Request,
    response: Response,
    body: FactorTrackingRequest,
    viewer: str | None,
    *,
    resume_only: bool,
) -> Envelope[FactorTrackingOperationResult]:
    web = request.app.state.web
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if viewer not in web.settings.factor_tracking_users:
        raise HTTPException(status_code=403, detail="当前账号不能管理因子跟踪。")
    if not web.settings.factor_tracking_enabled or web.factor_tracking_admission is None:
        raise HTTPException(status_code=503, detail="跟踪入口尚未开启。")
    if request.query_params:
        raise HTTPException(status_code=422, detail="请使用完整原请求重试。")
    client = web.factor_tracking_admission
    try:
        original = client.lookup(body, authenticated_actor_id=viewer)
        if original is not None:
            result = client.resume(body, authenticated_actor_id=viewer)
        elif resume_only:
            raise HTTPException(status_code=404, detail="原跟踪请求尚未确认。")
        else:
            result = client.submit(
                body,
                authenticated_actor_id=viewer,
                verified_registry_instance_id=_preflight(request, body),
            )
    except FactorTrackingAdmissionRejectedError as error:
        raise HTTPException(
            status_code=409, detail="原跟踪请求暂不可推进，请核对原操作。"
        ) from error
    except FactorTrackingAdmissionUnavailableError:
        result = FactorTrackingOperationResult(
            original_request=body, status="uncertain", reason="跟踪操作暂未确认，请核对原请求。"
        )
    checked = FactorTrackingOperationResult.model_validate(result)
    if checked.original_request != body:
        raise HTTPException(status_code=503, detail="跟踪回执与原请求不符。")
    return _envelope(request, response, checked)


@router.post(
    "/tracking/commands",
    response_model=Envelope[FactorTrackingOperationResult],
    dependencies=[Depends(require_csrf)],
    summary="加入或取消因子跟踪",
)
def set_factor_tracked(
    request: Request,
    response: Response,
    body: FactorTrackingRequest,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FactorTrackingOperationResult]:
    return _toggle(request, response, body, viewer, resume_only=False)


@router.post(
    "/tracking/commands/resume",
    response_model=Envelope[FactorTrackingOperationResult],
    dependencies=[Depends(require_csrf)],
    summary="核对原因子跟踪操作",
)
def resume_factor_tracking(
    request: Request,
    response: Response,
    body: FactorTrackingRequest,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FactorTrackingOperationResult]:
    return _toggle(request, response, body, viewer, resume_only=True)


@router.post(
    "/tracking/commands/retry",
    response_model=Envelope[FactorTrackingOperationResult],
    dependencies=[Depends(require_csrf)],
    summary="重试原因子跟踪操作",
)
def retry_factor_tracking(
    request: Request,
    response: Response,
    body: FactorTrackingRequest,
    viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[FactorTrackingOperationResult]:
    return _toggle(request, response, body, viewer, resume_only=False)
