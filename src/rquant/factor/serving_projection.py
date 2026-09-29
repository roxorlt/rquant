"""Lossless, bounded Serving pair for verified factor definition heads."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date

from rquant.factor.definition_serving import (
    FactorDefinitionServingRow,
    FactorDefinitionServingSnapshot,
    FactorDefinitionServingState,
)
from rquant.serving_read_models import ServingProjectionInput, ServingProjectionPayload
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

FACTOR_DEFINITION_PROJECTION_TABLES = frozenset({"factor_definition_state", "factor_definition"})


def project_factor_definition_projections(
    snapshot: FactorDefinitionServingSnapshot,
) -> tuple[ServingProjectionPayload, ServingProjectionPayload]:
    """Carry one verified registry snapshot into both Lab-owned physical tables."""
    verified = FactorDefinitionServingSnapshot.model_validate(snapshot.model_dump(mode="python"))
    if verified.available_at.utcoffset() != UTC.utcoffset(None):
        raise ValueError("factor definition Serving availability must be UTC")
    state = ServingProjectionPayload(
        table_name="factor_definition_state",
        available_at=verified.available_at,
        rows=(
            {
                "status_key": "current",
                "status": verified.state.status,
                "definition_count": verified.state.definition_count,
                "registry_instance_id": verified.registry_instance_id,
                "snapshot_sha256": verified.sha256,
            },
        ),
    )
    definitions = ServingProjectionPayload(
        table_name="factor_definition",
        available_at=verified.available_at,
        rows=tuple(
            {
                "factor_id": row.factor_id,
                "version": row.version,
                "content_sha256": row.content_sha256,
                "name_zh": row.name_zh,
                "category": row.category,
                "direction": row.direction,
                "expression": row.expression,
                "earliest_available_date": row.earliest_available_date.isoformat(),
                "dependency_columns_json": canonical_json_bytes(row.dependency_columns).decode(
                    "utf-8"
                ),
                "max_history_window": row.max_history_window,
                "archived": row.archived,
            }
            for row in verified.definitions
        ),
    )
    return state, definitions


def validate_factor_definition_projections(
    projections: Mapping[str, ServingProjectionPayload | ServingProjectionInput],
) -> FactorDefinitionServingSnapshot | None:
    """Rebuild the domain snapshot and refuse any incomplete or altered pair."""
    present = FACTOR_DEFINITION_PROJECTION_TABLES & projections.keys()
    if not present:
        return None
    if present != FACTOR_DEFINITION_PROJECTION_TABLES:
        raise ValueError("factor definition Serving projections are incomplete")
    state_projection = projections["factor_definition_state"]
    definitions_projection = projections["factor_definition"]
    if state_projection.available_at != definitions_projection.available_at:
        raise ValueError("factor definition projections have different availability")
    bound = tuple(
        projection
        for projection in (state_projection, definitions_projection)
        if isinstance(projection, ServingProjectionInput)
    )
    if bound and (
        len(bound) != 2
        or {projection.owner_dataset_id for projection in bound} != {"lab_jobs"}
        or len({projection.owner_generation_id for projection in bound}) != 1
    ):
        raise ValueError("factor definition projections mix Serving generations")
    if len(state_projection.rows) != 1:
        raise ValueError("factor definition state must have one row")
    state_row = state_projection.rows[0]
    if state_row["status_key"] != "current":
        raise ValueError("factor definition state key is invalid")
    rows: list[FactorDefinitionServingRow] = []
    for row in definitions_projection.rows:
        dependencies_json = row["dependency_columns_json"]
        if not isinstance(dependencies_json, str):
            raise ValueError("factor definition dependencies must be canonical JSON")
        dependencies = strict_canonical_json_loads(dependencies_json)
        if not isinstance(dependencies, list) or any(
            type(column) is not str for column in dependencies
        ):
            raise ValueError("factor definition dependencies must be a string list")
        earliest = row["earliest_available_date"]
        if not isinstance(earliest, str) or date.fromisoformat(earliest).isoformat() != earliest:
            raise ValueError("factor definition earliest date must be canonical")
        row_data = dict(row)
        row_data.pop("dependency_columns_json")
        row_data["earliest_available_date"] = date.fromisoformat(earliest)
        row_data["dependency_columns"] = tuple(dependencies)
        rows.append(FactorDefinitionServingRow.model_validate(row_data))
    return FactorDefinitionServingSnapshot(
        registry_instance_id=state_row["registry_instance_id"],
        available_at=state_projection.available_at,
        state=FactorDefinitionServingState(
            status=state_row["status"], definition_count=state_row["definition_count"]
        ),
        definitions=tuple(rows),
        sha256=state_row["snapshot_sha256"],
    )
