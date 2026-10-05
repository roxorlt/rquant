"""Restore the exact three-table template graph from one borrowed generation."""

from __future__ import annotations

from datetime import datetime

from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.strategy_authoring_projection import (
    StrategyAuthoringSnapshot,
    validate_strategy_authoring_projections,
)
from rquant.strategy_authoring_projection_contract import STRATEGY_TEMPLATE_PROJECTION_TABLES
from rquant.web.serving import BorrowedGeneration


def read_strategy_authoring(
    borrowed: BorrowedGeneration | None,
) -> StrategyAuthoringSnapshot | None:
    if borrowed is None:
        return None
    names = tuple(sorted(STRATEGY_TEMPLATE_PROJECTION_TABLES))
    marks = borrowed.cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, owner_generation_id, available_at FROM projection_status WHERE table_name IN (?, ?, ?) ORDER BY table_name LIMIT 4",
        names,
    ).fetchall()
    if (
        len(marks) != 3
        or tuple(row[0] for row in marks) != names
        or any(type(row[1]) is not bool or type(row[2]) is not int for row in marks)
    ):
        raise ValueError("template projection status is incomplete")
    if all(not row[1] for row in marks):
        if any(
            row[2] != 0
            or row[3] != "lab_jobs"
            or row[4] is not None
            or row[5] is not None
            or borrowed.manifest.row_counts.get(row[0], 0) != 0
            for row in marks
        ):
            raise ValueError("unpublished template projections contain facts")
        return None
    watermark = next(
        (item for item in borrowed.manifest.watermarks if item.dataset_id == "lab_jobs"), None
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
        raise ValueError("template projection generation differs")
    projections: dict[str, ServingProjectionPayload] = {}
    for name, _, count, *_ in marks:
        contract = PAGE_PROJECTION_CONTRACTS[name]
        if count > contract.max_rows:
            raise ValueError("template projection exceeds row budget")
        values = borrowed.cursor.execute(
            f"SELECT {', '.join(contract.column_names)} FROM {name} ORDER BY {', '.join(contract.sort_keys)} LIMIT ?",
            (contract.max_rows + 1,),
        ).fetchall()
        if len(values) != count:
            raise ValueError("template projection physical rows differ")
        projections[name] = ServingProjectionPayload(
            table_name=name,
            available_at=at,
            rows=tuple(
                {
                    key: value.isoformat() if isinstance(value, datetime) else value
                    for key, value in zip(contract.column_names, row, strict=True)
                }
                for row in values
            ),
        )
    restored = validate_strategy_authoring_projections(projections)
    if restored is None:
        raise ValueError("template committed graph is absent")
    return restored
