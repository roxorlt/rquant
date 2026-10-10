"""Fixed-generation read-only views of published factor research."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response

from rquant.factor.result_serving import (
    FactorResultIndexRow,
    FactorResultServingSnapshot,
    validate_factor_result_projections,
)
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.web.envelope import Envelope
from rquant.web.collaboration_gateway import CollaborationGateway, CollaborationUnavailableError
from rquant.web.models.factor_results import (
    FactorResearchDisplay,
    FactorResultDetailData,
    FactorResultItem,
    FactorResultListData,
)
from rquant.web.models.factors import FactorDefinitionItem
from rquant.web.routes.factors import _read_catalog
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/factors/results")
_TABLES = ("factor_result_display", "factor_result_index", "factor_result_state")
_UNREADABLE = "因子结果暂时无法核验，请稍后重试。"
_STATE_LABELS = {
    "queued": "等待检验",
    "running": "检验中",
    "succeeded": "已完成",
    "failed": "检验失败",
}
_FAILURE_MESSAGES = {
    "deadline_expired": "检验超时，请重试。",
    "source_unavailable": "研究数据暂不可用，请稍后重试。",
    "artifact_invalid": "结果无法核验，请重新检验。",
    "evaluation_failed": "检验未完成，请检查输入。",
    "internal_error": "检验暂不可用，请稍后重试。",
}
_DISPLAY_MESSAGES = {
    "not_ready": "检验完成后可查看结果。",
    "display_unavailable": "历史结果暂无图表。",
    "not_published": "图表暂未发布。",
    "available": "可查看研究图表。",
}
_BASIS_LABEL = "历史回溯研究；分组曲线不含撮合与交易费用。"


def _owned(request: Request, actor: str | None, row: FactorResultIndexRow) -> bool:
    web = request.app.state.web
    if web.settings.collaboration_mode == "legacy":
        return True
    gateway = web.collaboration
    if actor is None or type(gateway) is not CollaborationGateway:
        raise HTTPException(503, "当前权限暂无法核验。")
    try:
        gateway.result_owner(actor, domain="factor", job_id=row.job_id, spec_hash=row.spec_sha256)
        return True
    except (PermissionError, LookupError):
        return False
    except CollaborationUnavailableError as exc:
        raise HTTPException(503, "当前权限暂无法核验。") from exc


def _neutralization_fields(display: object) -> dict[str, object]:
    from rquant.factor.run_request import neutralization_label

    mode = getattr(display, "neutralization", "none")
    context = getattr(display, "context", None)
    missing = context is not None and any(
        count.reason == "missing_context" and count.count > 0
        for day in display.coverage_days
        for count in day.coverage.factor_missing_by_reason
    )
    return {
        "neutralization": mode,
        "neutralization_label": neutralization_label(mode),
        "context_basis_label": "行业归属来自独立 API 的历史回顾，不代表当时已采集。"
        if context is not None and context.industry is not None
        else None,
        "context_note": "缺少行业或市值数据的股票未参与检验。" if missing else None,
    }


def _read_results(borrowed: BorrowedGeneration | None) -> FactorResultServingSnapshot | None:
    if borrowed is None:
        return None
    try:
        marks = borrowed.cursor.execute(
            "SELECT table_name, available, row_count, owner_dataset_id, "
            "owner_generation_id, available_at FROM projection_status "
            "WHERE table_name IN (?, ?, ?) ORDER BY table_name LIMIT 4",
            _TABLES,
        ).fetchall()
        if len(marks) != 3 or tuple(row[0] for row in marks) != _TABLES:
            raise ValueError("factor result status is incomplete")
        if any(type(row[1]) is not bool or type(row[2]) is not int for row in marks):
            raise ValueError("factor result status types are invalid")
        if all(not row[1] for row in marks):
            if any(
                row[2] != 0
                or row[3] != "lab_jobs"
                or row[4] is not None
                or row[5] is not None
                or borrowed.manifest.row_counts.get(str(row[0]), 0) != 0
                for row in marks
            ):
                raise ValueError("unpublished factor results have rows")
            return None
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
            raise ValueError("factor result status disagrees with generation")

        projections: dict[str, ServingProjectionPayload] = {}
        for name, _, count, *_ in marks:
            contract = PAGE_PROJECTION_CONTRACTS[name]
            columns = contract.column_names
            order = (
                "updated_at DESC, job_id DESC"
                if name == "factor_result_index"
                else ", ".join(contract.sort_keys)
            )
            selected = borrowed.cursor.execute(
                f"SELECT {', '.join(columns)} FROM {name} ORDER BY {order} LIMIT ?",
                (contract.max_rows + 1,),
            ).fetchall()
            if len(selected) != count:
                raise ValueError("factor result physical rows disagree with status")
            rows = []
            for values in selected:
                row = dict(zip(columns, values, strict=True))
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.astimezone(UTC).isoformat().replace("+00:00", "Z")
                rows.append(row)
            projections[name] = ServingProjectionPayload(
                table_name=name,
                available_at=at.astimezone(UTC),
                rows=tuple(rows),
            )
        verified = validate_factor_result_projections(projections)
        if verified is None:
            raise ValueError("factor result group is absent")
        return verified
    except Exception as exc:
        raise HTTPException(status_code=503, detail=_UNREADABLE) from exc


def _item(
    row: FactorResultIndexRow, definitions: dict[str, FactorDefinitionItem]
) -> FactorResultItem:
    definition = definitions.get(row.factor_id)
    current = (
        definition is not None
        and definition.version == row.factor_version
        and definition.content_sha256 == row.definition_content_sha256
    )
    return FactorResultItem(
        job_id=row.job_id,
        spec_sha256=row.spec_sha256,
        definition_content_sha256=row.definition_content_sha256,
        factor_id=row.factor_id,
        factor_version=row.factor_version,
        factor_name_zh=definition.name_zh if current else None,
        definition_status="current" if current else "historical_unavailable",
        status=row.status,
        status_label=_STATE_LABELS[row.status],
        failure_message=_FAILURE_MESSAGES.get(row.failure_code),
        updated_at=row.updated_at,
        as_of_time=row.as_of_time,
        display_status=row.display_status,
        display_message=_DISPLAY_MESSAGES[row.display_status],
    )


def _data(
    borrowed: BorrowedGeneration | None,
) -> tuple[FactorResultServingSnapshot | None, list[FactorResultItem]]:
    snapshot = _read_results(borrowed)
    if snapshot is None:
        return None, []
    catalog = _read_catalog(borrowed)
    definitions = {definition.factor_id: definition for definition in catalog.definitions}
    items = [_item(row, definitions) for row in snapshot.index]
    displays = iter(snapshot.displays)
    for i, row in enumerate(snapshot.index):
        if row.display_status == "available":
            artifact = next(displays)
            if (
                artifact.definition.factor_id != row.factor_id
                or artifact.definition.version != row.factor_version
            ):
                raise HTTPException(status_code=503, detail="原版本结果暂不可核验。")
            items[i] = items[i].model_copy(update={"factor_name_zh": artifact.definition.name_zh})
    return snapshot, items


@router.get("", response_model=Envelope[FactorResultListData], summary="因子检验结果")
def list_factor_results(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    generation_id: Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")] = None,
) -> Envelope[FactorResultListData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if generation_id is not None and meta.generation_id != generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请重新查看结果。")
        snapshot, items = _data(borrowed)
        if snapshot is not None:
            items = [item for item, row in zip(items, snapshot.index, strict=True) if _owned(request, _viewer, row)]
        if web.settings.collaboration_mode == "enforced":
            web.collaboration.me(_viewer)
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[FactorResultListData](
        data=FactorResultListData(
            availability="unavailable" if snapshot is None else snapshot.state.status,
            available_at=None if snapshot is None else snapshot.available_at,
            results=items,
        ),
        serving=meta,
    )


@router.get("/{job_id}", response_model=Envelope[FactorResultDetailData], summary="因子检验详情")
def get_factor_result(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    job_id: Annotated[str, Path(pattern=r"^[0-9a-f]{32}$")],
    generation_id: Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")] = None,
) -> Envelope[FactorResultDetailData]:
    web = request.app.state.web
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed,
            now=web.clock(),
            stale_after=web.settings.stale_after,
            failure=web.tracker.failure,
        )
        if generation_id is not None and meta.generation_id != generation_id:
            raise HTTPException(status_code=409, detail="数据已更新，请重新查看结果。")
        snapshot, items = _data(borrowed)
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    item = next((item for item in items if item.job_id == job_id), None)
    row = None if snapshot is None else next((row for row in snapshot.index if row.job_id == job_id), None)
    if row is not None and not _owned(request, _viewer, row):
        raise HTTPException(404, "找不到这份结果。")
    display = (
        None
        if snapshot is None
        else next(
            (
                artifact
                for row, artifact in zip(
                    (row for row in snapshot.index if row.display_status == "available"),
                    snapshot.displays,
                    strict=True,
                )
                if row.job_id == job_id
            ),
            None,
        )
    )
    from rquant.web.models.factor_results import FactorStreamResearchDisplay

    research = (
        None
        if display is None
        else (
            FactorStreamResearchDisplay if display.schema_version == 2 else FactorResearchDisplay
        )(
            **({"schema_version": 2} if display.schema_version == 2 else {}),
            basis_label=_BASIS_LABEL,
            pool_label=display.pool_label if display.schema_version == 2 else "固定样本",
            return_price_basis=display.return_price_basis,
            holding_sessions=display.holding_sessions,
            summary_status=display.summary_status,
            ic_summary=display.ic_summary,
            ic_points=list(display.ic_points),
            decay_periods=list(display.decay_periods),
            portfolio_status=display.portfolio_status,
            portfolio_days=list(display.portfolio_days),
            coverage_days=list(display.coverage_days),
            **_neutralization_fields(display),
            **(
                {
                    "mad_multiple": display.mad_multiple,
                    "extended_statistics": display.extended_statistics,
                    "daily_features": display.daily_features,
                    "daily_feature_coverage_days": display.daily_feature_coverage_days,
                }
                if display.schema_version == 2
                else {}
            ),
        )
    )
    availability = (
        "unavailable"
        if snapshot is None
        else "empty"
        if not snapshot.index
        else "not_found"
        if item is None
        else "ready"
    )
    if web.settings.collaboration_mode == "enforced":
        web.collaboration.me(_viewer)
    return Envelope[FactorResultDetailData](
        data=FactorResultDetailData(
            availability=availability,
            available_at=None if snapshot is None else snapshot.available_at,
            result=item,
            research=research,
            can_report=web.settings.collaboration_mode == "enforced" and item is not None
            and item.status == "succeeded" and display is not None,
        ),
        serving=meta,
    )


@router.get("/{job_id}/report", summary="下载因子封存报告")
def factor_report(
    request: Request,
    _viewer: Annotated[str | None, Depends(current_user)],
    job_id: Annotated[str, Path(pattern=r"^[0-9a-f]{32}$")],
    generation_id: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
) -> Response:
    from rquant.sealed_result_html import render_factor_html
    from rquant.sealed_result_ownership import SealedArtifactFact

    web = request.app.state.web
    gateway = web.collaboration
    if web.settings.collaboration_mode != "enforced" or type(gateway) is not CollaborationGateway or _viewer is None:
        raise HTTPException(503, "封存报告暂不可用。")
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(borrowed, now=web.clock(), stale_after=web.settings.stale_after, failure=web.tracker.failure)
        if generation_id != meta.generation_id:
            raise HTTPException(409, "数据已更新，请重新查看结果。")
        snapshot, _items = _data(borrowed)
        row = None if snapshot is None else next((row for row in snapshot.index if row.job_id == job_id), None)
        if row is None or not _owned(request, _viewer, row):
            raise HTTPException(404, "找不到这份结果。")
        display = next((display for indexed, display in zip(
            (indexed for indexed in snapshot.index if indexed.display_status == "available"),
            snapshot.displays, strict=True) if indexed.job_id == job_id), None)
        if row.status != "succeeded" or display is None:
            raise HTTPException(409, "完整结果尚未封存。")
        # Factor's original completion authority is its terminal completion digest.
        artifact = SealedArtifactFact(domain="factor", job_id=str(UUID(hex=job_id)),
            spec_hash=row.spec_sha256, manifest_hash=row.completion_sha256,
            complete_result_hash=row.full_artifact_sha256, full_artifact_hash=row.full_artifact_sha256,
            result_payload_hash=row.result_sha256, input_hash=display.input_sha256,
            display_hash=display.content_sha256, complete=True)
        try:
            binding = gateway.bind_sealed_artifact(_viewer, artifact)
            raw = render_factor_html(display, binding=binding, current_artifact=artifact, requester=_viewer)
            gateway.me(_viewer)
        except (PermissionError, LookupError) as exc:
            raise HTTPException(404, "找不到这份结果。") from exc
        except ValueError as exc:
            raise HTTPException(409, "报告资料待核对，请重新查看结果。") from exc
    return Response(raw, media_type="text/html; charset=utf-8", headers={
        "Cache-Control": "no-store", "Content-Disposition": 'attachment; filename="factor-report.html"',
        "X-Rquant-Generation": generation_id, "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'",
    })
