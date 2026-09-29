"""One borrowed, verified Serving generation for factor definitions."""

from __future__ import annotations

from datetime import date
from typing import Annotated

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from rquant.factor.serving_projection import validate_factor_definition_projections
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.web.envelope import Envelope
from rquant.web.models.factors import FactorCatalogData, FactorDefinitionItem
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta

router = APIRouter(prefix="/factors")
_TABLES = ("factor_definition", "factor_definition_state")
_UNREADABLE = "因子库暂时无法核验，请稍后重试。"
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
    _viewer: Annotated[str | None, Depends(current_user)],
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
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[FactorCatalogData](data=data, serving=meta)
