"""Fixed-generation read-only views of published factor research."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response

from rquant.factor.result_serving import (
    FactorResultIndexRow,
    FactorResultServingSnapshot,
    validate_factor_result_projections,
)
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.web.envelope import Envelope
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
    except (ValueError, TypeError, KeyError, duckdb.Error) as exc:
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
    return snapshot, [_item(row, definitions) for row in snapshot.index]


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
    research = (
        None
        if display is None
        else FactorResearchDisplay(
            basis_label=_BASIS_LABEL,
            pool_label="固定样本",
            return_price_basis=display.return_price_basis,
            holding_sessions=display.holding_sessions,
            summary_status=display.summary_status,
            ic_summary=display.ic_summary,
            ic_points=list(display.ic_points),
            decay_periods=list(display.decay_periods),
            portfolio_status=display.portfolio_status,
            portfolio_days=list(display.portfolio_days),
            coverage_days=list(display.coverage_days),
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
    return Envelope[FactorResultDetailData](
        data=FactorResultDetailData(
            availability=availability,
            available_at=None if snapshot is None else snapshot.available_at,
            result=item,
            research=research,
        ),
        serving=meta,
    )
