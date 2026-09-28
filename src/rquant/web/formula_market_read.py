"""Bounded formula task snapshot from one already verified Serving lease."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from rquant.formula_market_job_projection import (
    FORMULA_MARKET_PROJECTION_TABLES,
    FormulaMarketArtifactIndexRow,
    FormulaMarketJobRow,
    FormulaMarketJobSnapshot,
    FormulaMarketJobStateRow,
)
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS
from rquant.web.serving import BorrowedGeneration

Availability = Literal["unavailable", "not_published", "empty", "ready"]
_TABLES = tuple(sorted(FORMULA_MARKET_PROJECTION_TABLES))
_MAX_RESULT_INDEX_BYTES = 32 * 1024 * 1024


def read_formula_market_snapshot(
    borrowed: BorrowedGeneration | None,
) -> tuple[Availability, FormulaMarketJobSnapshot | None]:
    """Reject mixed, incomplete or oversized tables before any public task is assembled."""
    if borrowed is None:
        return "unavailable", None
    marks = borrowed.cursor.execute(
        "SELECT table_name, available, row_count, owner_dataset_id, "
        "owner_generation_id, available_at FROM projection_status "
        f"WHERE table_name IN ({', '.join('?' for _ in _TABLES)}) ORDER BY table_name LIMIT ?",
        (*_TABLES, len(_TABLES) + 1),
    ).fetchall()
    if not marks:
        if any(borrowed.manifest.row_counts.get(name, 0) for name in _TABLES):
            raise ValueError("formula task status missing from nonempty generation")
        return "not_published", None
    if len(marks) != len(_TABLES) or tuple(row[0] for row in marks) != _TABLES:
        raise ValueError("formula task projection status is incomplete")
    if any(
        type(available) is not bool or type(count) is not int for _, available, count, *_ in marks
    ):
        raise ValueError("formula task projection status types are invalid")
    if not any(row[1] for row in marks):
        if any(
            count
            or borrowed.manifest.row_counts.get(str(name), 0)
            or owner != "lab_jobs"
            or generation is not None
            or at is not None
            for name, _, count, owner, generation, at in marks
        ):
            raise ValueError("unpublished formula task status is inconsistent")
        return "not_published", None
    watermark = next(
        (item for item in borrowed.manifest.watermarks if item.dataset_id == "lab_jobs"), None
    )
    if watermark is None:
        raise ValueError("formula task owner watermark is absent")
    available_at = marks[0][5]
    if not isinstance(available_at, datetime) or any(
        not available
        or owner != "lab_jobs"
        or generation != watermark.generation_id
        or at != available_at
        or at > borrowed.manifest.built_at
        or count != borrowed.manifest.row_counts.get(str(name))
        for name, available, count, owner, generation, at in marks
    ):
        raise ValueError("formula task projections disagree with Serving generation")
    expected = {str(name): count for name, _, count, *_ in marks}
    rows: dict[str, list[object]] = {}
    for table in _TABLES:
        contract = PAGE_PROJECTION_CONTRACTS[table]
        count = expected[table]
        if not 0 <= count <= contract.max_rows:
            raise ValueError("formula task projection exceeds row budget")
        columns = contract.column_names
        selected = borrowed.cursor.execute(
            f"SELECT {', '.join(columns)} FROM {table} "
            f"ORDER BY {', '.join(contract.sort_keys)} LIMIT ?",
            (contract.max_rows + 1,),
        ).fetchall()
        if len(selected) != count:
            raise ValueError("formula task projection count differs from manifest")
        model = {
            "formula_market_job_state": FormulaMarketJobStateRow,
            "formula_market_job": FormulaMarketJobRow,
            "research_artifact_index": FormulaMarketArtifactIndexRow,
        }[table]
        rows[table] = [
            model.model_validate(dict(zip(columns, item, strict=True))) for item in selected
        ]
    states = rows["formula_market_job_state"]
    if len(states) != 1:
        raise ValueError("formula task state row is missing")
    snapshot = FormulaMarketJobSnapshot(
        state=states[0],
        jobs=tuple(rows["formula_market_job"]),
        artifacts=tuple(rows["research_artifact_index"]),
        available_at=available_at,
    )
    if sum(item.byte_count for item in snapshot.artifacts) > _MAX_RESULT_INDEX_BYTES:
        raise ValueError("formula result index exceeds total byte budget")
    return snapshot.state.availability, snapshot
