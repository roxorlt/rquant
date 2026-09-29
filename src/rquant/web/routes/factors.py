"""One borrowed, verified Serving generation for factor definitions."""

from __future__ import annotations

from datetime import date
from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response

from rquant.factor.registry import FactorDefinitionReceipt
from rquant.factor.serving_projection import validate_factor_definition_projections
from rquant.factor_definition_admission import (
    FactorArchiveAdmissionResult,
    FactorDefinitionAdmissionRejectedError,
    FactorDefinitionAdmissionUnavailableError,
)
from rquant.page_control import ArchiveFactor, PageControlStatus
from rquant.serving_publisher import ServingReader
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.web.envelope import Envelope, ServingState
from rquant.web.models.factors import (
    FactorArchiveCommandData,
    FactorArchiveCommandRequest,
    FactorCatalogData,
    FactorDefinitionItem,
)
from rquant.web.security import current_user, require_csrf
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/factors")
_TABLES = ("factor_definition", "factor_definition_state")
_UNREADABLE = "因子库暂时无法核验，请稍后重试。"
MAX_ARCHIVE_REQUEST_BYTES = 4096
_CATEGORY_LABELS = {
    "technical": "技术",
    "fundamental": "基本面",
    "price_volume": "价量",
    "quality": "质量",
    "momentum": "动量",
    "value": "估值",
}
_DIRECTION_LABELS = {"higher_is_better": "偏好高值", "lower_is_better": "偏好低值"}


def _read_catalog(borrowed: BorrowedGeneration | None) -> FactorCatalogData:
    if borrowed is None:
        return FactorCatalogData(availability="unavailable", available_at=None, definitions=[])
    try:
        marks = borrowed.cursor.execute(
            "SELECT table_name, available, row_count, owner_dataset_id, "
            "owner_generation_id, available_at FROM projection_status "
            "WHERE table_name IN (?, ?) ORDER BY table_name LIMIT 3",
            _TABLES,
        ).fetchall()
        if len(marks) != 2 or tuple(row[0] for row in marks) != _TABLES:
            raise ValueError("factor catalog status is incomplete")
        if any(type(row[1]) is not bool or type(row[2]) is not int for row in marks):
            raise ValueError("factor catalog status types are invalid")
        if all(not row[1] for row in marks):
            if any(
                row[2] != 0
                or row[3] != "lab_jobs"
                or row[4] is not None
                or row[5] is not None
                or borrowed.manifest.row_counts.get(str(row[0]), 0) != 0
                for row in marks
            ):
                raise ValueError("unpublished factor catalog has rows")
            return FactorCatalogData(availability="unavailable", available_at=None, definitions=[])
        watermark = next(
            (item for item in borrowed.manifest.watermarks if item.dataset_id == "lab_jobs"),
            None,
        )
        at = marks[0][5]
        if watermark is None or any(
            not row[1]
            or row[3] != "lab_jobs"
            or row[4] != watermark.generation_id
            or row[5] != at
            or at is None
            or at > borrowed.manifest.built_at
            or row[2] != borrowed.manifest.row_counts.get(str(row[0]))
            for row in marks
        ):
            raise ValueError("factor catalog status disagrees with generation")
        if marks[0][2] > 512 or marks[1][2] != 1:
            raise ValueError("factor catalog exceeds row budget")

        projections: dict[str, ServingProjectionPayload] = {}
        for name, _, count, *_ in marks:
            contract = PAGE_PROJECTION_CONTRACTS[name]
            columns = contract.column_names
            selected = borrowed.cursor.execute(
                f"SELECT {', '.join(columns)} FROM {name} "
                f"ORDER BY {', '.join(contract.sort_keys)} LIMIT ?",
                (contract.max_rows + 1,),
            ).fetchall()
            if len(selected) != count:
                raise ValueError("factor catalog physical rows disagree with status")
            rows = []
            for values in selected:
                row = dict(zip(columns, values, strict=True))
                for key, value in row.items():
                    if isinstance(value, date) and not hasattr(value, "tzinfo"):
                        row[key] = value.isoformat()
                rows.append(row)
            projections[name] = ServingProjectionPayload(
                table_name=name, available_at=at, rows=tuple(rows)
            )
        verified = validate_factor_definition_projections(projections)
        if verified is None:
            raise ValueError("factor catalog pair is absent")
        return FactorCatalogData(
            availability=verified.state.status,
            available_at=verified.available_at,
            definitions=[
                FactorDefinitionItem(
                    factor_id=row.factor_id,
                    content_sha256=row.content_sha256,
                    name_zh=row.name_zh,
                    category_label=_CATEGORY_LABELS.get(
                        row.category,
                        row.category
                        if any("\u4e00" <= char <= "\u9fff" for char in row.category)
                        else "其他",
                    ),
                    direction=row.direction,
                    direction_label=_DIRECTION_LABELS[row.direction],
                    version=row.version,
                    earliest_available_date=row.earliest_available_date,
                    archived=row.archived,
                    expression=row.expression,
                    dependency_columns=list(row.dependency_columns),
                    max_history_window=row.max_history_window,
                )
                for row in verified.definitions
            ],
        )
    except (ValueError, TypeError, KeyError, duckdb.Error) as exc:
        raise HTTPException(status_code=503, detail=_UNREADABLE) from exc


@router.get("/definitions", response_model=Envelope[FactorCatalogData], summary="因子定义目录")
def list_factor_definitions(
    request: Request,
    response: Response,
    viewer: Annotated[str | None, Depends(current_user)],
    generation_id: Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")] = None,
) -> Envelope[FactorCatalogData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if generation_id is not None and meta.generation_id != generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请重新查看因子。")
        data = _read_catalog(borrowed)
        data = data.model_copy(
            update={
                "can_archive": (
                    viewer in web.settings.factor_editor_users
                    and web.factor_admission is not None
                    and meta.state is ServingState.READY
                    and data.availability == "populated"
                )
            }
        )
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[FactorCatalogData](data=data, serving=meta)


def _editor(request: Request, viewer: Annotated[str | None, Depends(current_user)]) -> str:
    web = request.app.state.web
    if viewer is None:
        raise HTTPException(status_code=401, detail="请先登录。")
    if not web.settings.factor_editor_users or web.factor_admission is None:
        raise HTTPException(status_code=503, detail="归档暂未开放。")
    if viewer not in web.settings.factor_editor_users:
        raise HTTPException(status_code=403, detail="当前账号不能归档因子。")
    if request.query_params:
        raise HTTPException(status_code=422, detail="请刷新后重试。")
    return viewer


def _verified_registry_instance_id(borrowed: BorrowedGeneration) -> str:
    rows = borrowed.cursor.execute(
        "SELECT registry_instance_id FROM factor_definition_state "
        "WHERE status_key = 'current' LIMIT 2"
    ).fetchall()
    if len(rows) != 1:
        raise ValueError("verified factor registry identity is missing")
    value = rows[0][0]
    if not isinstance(value, str) or len(value) != 32:
        raise ValueError("verified factor registry identity is invalid")
    return value


def _archive_preflight(request: Request, factor_id: str, body: FactorArchiveCommandRequest) -> str:
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
            raise HTTPException(status_code=503, detail=_UNREADABLE)
        if meta.generation_id != body.generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请刷新因子后重试。")
        catalog = _read_catalog(borrowed)
        if catalog.availability != "populated":
            raise HTTPException(status_code=409, detail="当前因子尚未发布。")
        row = next((item for item in catalog.definitions if item.factor_id == factor_id), None)
        if row is None or row.archived:
            raise HTTPException(status_code=409, detail="当前因子已变化，请刷新后重试。")
        if (
            row.version != body.expected_head.version
            or row.content_sha256 != body.expected_head.content_sha256
        ):
            raise HTTPException(status_code=409, detail="当前因子已变化，请刷新后重试。")
        try:
            instance_id = _verified_registry_instance_id(borrowed)
            pointer = ServingReader(web.settings.serving_root).current_pointer()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=_UNREADABLE) from exc
        if (
            borrowed.pointer is None
            or pointer.generation_id != body.generation_id
            or pointer.manifest_sha256 != borrowed.pointer.manifest_sha256
        ):
            raise HTTPException(status_code=409, detail="数据已更新，请刷新因子后重试。")
        return instance_id


def _archive_response(
    request: Request,
    factor_id: str,
    body: FactorArchiveCommandRequest,
    result: FactorArchiveAdmissionResult | None,
    *,
    fallback_status: str = "unavailable",
) -> Envelope[FactorArchiveCommandData]:
    web = request.app.state.web
    web.tracker.refresh()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        ).model_copy(update={"detail": ""})
        state = fallback_status
        message = (
            "归档未受理，请刷新后重试。"
            if fallback_status == "rejected"
            else "归档暂不可用，请用原命令重试。"
        )
        current_head_updated = False
        if result is not None:
            receipt = result.receipt
            if receipt.command_id != body.command_id or receipt.enqueued_at != body.requested_at:
                raise HTTPException(status_code=503, detail="归档回执无法核对。")
            if receipt.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}:
                state, message = "pending", "归档正在处理，请稍后查看。"
            elif receipt.status is PageControlStatus.FAILED:
                state, message = "rejected", "当前因子已变化，请刷新后重试。"
            elif receipt.status is PageControlStatus.SUCCEEDED:
                try:
                    effect = FactorDefinitionReceipt.model_validate(receipt.result)
                except ValueError as exc:
                    raise HTTPException(status_code=503, detail="归档回执无法核对。") from exc
                if (
                    effect.command_id != body.command_id
                    or effect.action != "archive"
                    or effect.factor_id != factor_id
                    or effect.version != body.expected_head.version
                    or effect.content_sha256 != body.expected_head.content_sha256
                    or effect.archived is not True
                    or receipt.completed_at is None
                ):
                    raise HTTPException(status_code=503, detail="归档回执无法核对。")
                state, message = "succeeded_waiting_publication", "已提交，等待更新。"
                if (
                    borrowed is not None
                    and meta.state is ServingState.READY
                    and meta.generation_id != body.generation_id
                    and borrowed.manifest.built_at > receipt.completed_at
                ):
                    try:
                        catalog = _read_catalog(borrowed)
                        instance_id = _verified_registry_instance_id(borrowed)
                        pointer = ServingReader(web.settings.serving_root).current_pointer()
                        pointer_matches = (
                            borrowed.pointer is not None
                            and pointer.generation_id == borrowed.pointer.generation_id
                            and pointer.manifest_sha256 == borrowed.pointer.manifest_sha256
                        )
                    except (HTTPException, OSError, ValueError, duckdb.Error):
                        catalog = None
                        instance_id = None
                        pointer_matches = False
                    if (
                        catalog is not None
                        and pointer_matches
                        and instance_id == result.registry_instance_id
                    ):
                        row = next(
                            (item for item in catalog.definitions if item.factor_id == factor_id),
                            None,
                        )
                        if (
                            row is not None
                            and row.version == body.expected_head.version
                            and row.content_sha256 == body.expected_head.content_sha256
                            and row.archived
                        ):
                            state, message = "published", "已归档，历史记录仍会保留。"
                        elif row is not None and row.version > body.expected_head.version:
                            state = "published"
                            current_head_updated = True
                            message = "原版本已归档，当前已有新版本。"
        data = FactorArchiveCommandData(
            status=state,
            command_id=body.command_id,
            factor_id=factor_id,
            version=body.expected_head.version,
            content_sha256=body.expected_head.content_sha256,
            current_head_updated=current_head_updated,
            message=message,
        )
        return Envelope[FactorArchiveCommandData](data=data, serving=meta)


@router.post(
    "/definitions/{factor_id}/archive",
    response_model=Envelope[FactorArchiveCommandData],
    summary="归档当前因子定义",
)
def archive_factor_definition(
    request: Request,
    body: FactorArchiveCommandRequest,
    factor_id: Annotated[str, Path(pattern=r"^[a-z][a-z0-9_]{0,63}$")],
    actor: Annotated[str, Depends(_editor)],
    _csrf: Annotated[None, Depends(require_csrf)],
) -> Envelope[FactorArchiveCommandData]:
    instance_id = _archive_preflight(request, factor_id, body)
    command = ArchiveFactor(
        command_id=body.command_id,
        requested_at=body.requested_at,
        factor_id=factor_id,
        expected_head=body.expected_head,
    )
    try:
        result = request.app.state.web.factor_admission.submit(
            command,
            authenticated_actor_id=actor,
            verified_registry_instance_id=instance_id,
        )
    except FactorDefinitionAdmissionRejectedError:
        return _archive_response(request, factor_id, body, None, fallback_status="rejected")
    except (FactorDefinitionAdmissionUnavailableError, OSError, TimeoutError):
        return _archive_response(request, factor_id, body, None)
    return _archive_response(request, factor_id, body, result)


@router.post(
    "/definitions/{factor_id}/archive/resume",
    response_model=Envelope[FactorArchiveCommandData],
    summary="续查原归档命令",
)
def resume_factor_archive(
    request: Request,
    body: FactorArchiveCommandRequest,
    factor_id: Annotated[str, Path(pattern=r"^[a-z][a-z0-9_]{0,63}$")],
    actor: Annotated[str, Depends(_editor)],
    _csrf: Annotated[None, Depends(require_csrf)],
) -> Envelope[FactorArchiveCommandData]:
    command = ArchiveFactor(
        command_id=body.command_id,
        requested_at=body.requested_at,
        factor_id=factor_id,
        expected_head=body.expected_head,
    )
    try:
        client = request.app.state.web.factor_admission
        result = client.lookup(command, authenticated_actor_id=actor)
        if result is None:
            return _archive_response(request, factor_id, body, None, fallback_status="rejected")
        if result.receipt.status in {PageControlStatus.PENDING, PageControlStatus.PROCESSING}:
            result = client.resume(command, authenticated_actor_id=actor)
    except FactorDefinitionAdmissionRejectedError:
        return _archive_response(request, factor_id, body, None, fallback_status="rejected")
    except (FactorDefinitionAdmissionUnavailableError, OSError, TimeoutError):
        return _archive_response(request, factor_id, body, None)
    return _archive_response(request, factor_id, body, result)
