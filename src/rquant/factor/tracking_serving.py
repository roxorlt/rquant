"""Bounded, lossless tracking projection; Web never reads the research authority."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from rquant.factor.registry import FactorDefinitionRegistry, FactorHeadRef, FactorRegistryIdentity
from rquant.factor.run_request import RUN_IMMUTABLE
from rquant.factor.tracking import (
    MAX_TRACKED_FACTORS,
    MAX_TRACKING_DAYS,
    FactorTrackingDay,
    FactorTrackingIdentity,
    FactorTrackingIntegrityError,
    FactorTrackingPanel,
    FactorTrackingState,
    FactorTrackingStore,
    summarize_factor_tracking,
)
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256
from rquant.serving_read_models import ServingProjectionInput, ServingProjectionPayload
from rquant.strict_json import canonical_json_bytes, strict_model_validate_canonical_json

FACTOR_TRACKING_PROJECTION_TABLES = frozenset({"factor_tracking_state", "factor_tracking"})


class FactorTrackingServingSnapshot(BaseModel):
    model_config = RUN_IMMUTABLE
    tracking_instance_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    registry_instance_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    available_at: AwareUtcDatetime
    panels: tuple[FactorTrackingPanel, ...] = Field(max_length=MAX_TRACKED_FACTORS)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _verified(self) -> FactorTrackingServingSnapshot:
        ids = tuple(p.factor_id for p in self.panels)
        if ids != tuple(sorted(set(ids))) or any(
            p.availability == "unavailable" or p.can_set_tracked for p in self.panels
        ):
            raise ValueError("tracking projection rows are invalid")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("tracking snapshot digest differs")
        return self


def _days(
    store: FactorTrackingStore, connection: sqlite3.Connection, state: FactorTrackingState
) -> tuple[FactorTrackingDay, ...]:
    if state.segment_id is None:
        return ()
    from rquant.factor.tracking_runner import _run_row

    expected = []
    pending = False
    for index, row in enumerate(
        connection.execute(
            "SELECT * FROM tracking_runs WHERE segment_id=? ORDER BY rowid LIMIT ?",
            (state.segment_id, MAX_TRACKING_DAYS + 1),
        )
    ):
        if index >= MAX_TRACKING_DAYS:
            raise FactorTrackingIntegrityError("tracking run history exceeds its capacity")
        run = _run_row(store, row)
        if (
            run.factor_id != state.factor_id
            or run.plan.registry_identity != state.registry_identity
            or run.plan.request.parameters.expected_head != state.head
            or pending
            or run.original_cursor != (expected[-1] if expected else None)
        ):
            raise FactorTrackingIntegrityError("tracking committed run chain differs")
        if run.status == "planned":
            pending = True
            continue
        dates = run.plan.spec.adapter_request.evaluation_days
        if (
            not dates
            or (expected and dates[0] <= expected[-1])
            or len(expected) + len(dates) > MAX_TRACKING_DAYS
        ):
            raise FactorTrackingIntegrityError("tracking committed date grid is invalid")
        expected.extend(dates)
    rows = connection.execute(
        "SELECT * FROM tracking_days WHERE segment_id=? ORDER BY trade_date LIMIT ?",
        (state.segment_id, MAX_TRACKING_DAYS + 1),
    ).fetchall()
    if len(rows) > MAX_TRACKING_DAYS:
        raise FactorTrackingIntegrityError("tracking history exceeds its capacity")
    result = []
    for row in rows:
        day = store._decode(FactorTrackingDay, row["payload"], row["sha256"])
        if day.trade_date.isoformat() != row["trade_date"]:
            raise FactorTrackingIntegrityError("tracking contribution date differs")
        result.append(day)
    if tuple(day.trade_date for day in result) != tuple(expected) or (
        state.start_date,
        state.cursor,
    ) != (
        expected[0] if expected else None,
        expected[-1] if expected else None,
    ):
        raise FactorTrackingIntegrityError("tracking contributions differ from committed date grid")
    return tuple(result)


def project_factor_tracking_snapshot(
    identity: FactorTrackingIdentity,
    *,
    registry_identity: FactorRegistryIdentity,
    available_at: datetime,
) -> FactorTrackingServingSnapshot:
    store = FactorTrackingStore(Path(identity.path))
    registry = FactorDefinitionRegistry(Path(registry_identity.path))
    panels = []
    with (
        registry._reader(registry_identity) as definitions,
        store._connection(identity) as connection,
    ):
        ids = connection.execute(
            "SELECT factor_id FROM tracking_states ORDER BY factor_id LIMIT ?",
            (MAX_TRACKED_FACTORS + 1,),
        ).fetchall()
        if len(ids) > MAX_TRACKED_FACTORS:
            raise ValueError("tracking collection exceeds its capacity")
        for row in ids:
            state = store._state(connection, row[0])
            if state.registry_identity != registry_identity:
                raise ValueError("tracking registry identity differs")
            current, _ = registry._load_factor(definitions, state.factor_id)
            if current is None:
                raise ValueError("tracked definition is absent")
            changed = (
                current.archived
                or FactorHeadRef(version=current.version, content_sha256=current.content_sha256)
                != state.head
            )
            days = _days(store, connection, state)
            panels.append(
                FactorTrackingPanel(
                    factor_id=state.factor_id,
                    availability="tracked" if state.tracked else "not_tracked",
                    status="paused" if state.tracked and changed else state.status,
                    tracked=state.tracked,
                    tracking_generation=state.generation,
                    definition_head=state.head,
                    actual_start_date=state.start_date,
                    updated_at=state.updated_at,
                    summary=summarize_factor_tracking(days) if days else None,
                    reason="定义已更新或归档，请重新加入跟踪。"
                    if state.tracked and changed
                    else state.reason,
                )
            )
    fields = dict(
        tracking_instance_id=identity.instance_id,
        registry_instance_id=registry_identity.instance_id,
        available_at=available_at,
        panels=tuple(panels),
    )
    return FactorTrackingServingSnapshot(**fields, sha256=canonical_sha256(fields))


def project_factor_tracking_projections(
    snapshot: FactorTrackingServingSnapshot,
) -> tuple[ServingProjectionPayload, ServingProjectionPayload]:
    snapshot = FactorTrackingServingSnapshot.model_validate(snapshot)
    state = ServingProjectionPayload(
        table_name="factor_tracking_state",
        available_at=snapshot.available_at,
        rows=(
            {
                "status_key": "current",
                "status": "populated" if snapshot.panels else "empty",
                "row_count": len(snapshot.panels),
                "tracking_instance_id": snapshot.tracking_instance_id,
                "registry_instance_id": snapshot.registry_instance_id,
                "snapshot_sha256": snapshot.sha256,
            },
        ),
    )
    rows = ServingProjectionPayload(
        table_name="factor_tracking",
        available_at=snapshot.available_at,
        rows=tuple(
            {
                "factor_id": panel.factor_id,
                "panel_json": canonical_json_bytes(panel.model_dump(mode="json")).decode(),
            }
            for panel in snapshot.panels
        ),
    )
    return state, rows


def validate_factor_tracking_projections(
    projections: Mapping[str, ServingProjectionInput | ServingProjectionPayload],
) -> FactorTrackingServingSnapshot | None:
    present = projections.keys() & FACTOR_TRACKING_PROJECTION_TABLES
    if not present:
        return None
    if present != FACTOR_TRACKING_PROJECTION_TABLES:
        raise ValueError("tracking projection pair is incomplete")
    state, rows = projections["factor_tracking_state"], projections["factor_tracking"]
    if (
        state.available_at != rows.available_at
        or state.available_at.utcoffset() != UTC.utcoffset(None)
        or len(state.rows) != 1
    ):
        raise ValueError("tracking projection availability differs")
    bound = tuple(p for p in (state, rows) if isinstance(p, ServingProjectionInput))
    if bound and (
        len(bound) != 2
        or {p.owner_dataset_id for p in bound} != {"lab_jobs"}
        or len({p.owner_generation_id for p in bound}) != 1
    ):
        raise ValueError("tracking projections mix generations")
    mark = state.rows[0]
    if (
        mark["status_key"] != "current"
        or mark["row_count"] != len(rows.rows)
        or mark["status"] != ("populated" if rows.rows else "empty")
    ):
        raise ValueError("tracking projection count differs")
    panels = tuple(
        strict_model_validate_canonical_json(FactorTrackingPanel, row["panel_json"])
        for row in rows.rows
    )
    if tuple(row["factor_id"] for row in rows.rows) != tuple(panel.factor_id for panel in panels):
        raise ValueError("tracking projection factor differs")
    snapshot = FactorTrackingServingSnapshot(
        tracking_instance_id=mark["tracking_instance_id"],
        registry_instance_id=mark["registry_instance_id"],
        available_at=state.available_at,
        panels=panels,
        sha256=mark["snapshot_sha256"],
    )
    if "factor_definition_state" in projections or "factor_definition" in projections:
        from rquant.factor.serving_projection import validate_factor_definition_projections

        catalog = validate_factor_definition_projections(projections)
        if (
            catalog is None
            or catalog.registry_instance_id != snapshot.registry_instance_id
            or not set(p.factor_id for p in panels) <= set(d.factor_id for d in catalog.definitions)
        ):
            raise ValueError("tracking and definition projections disagree")
    return snapshot
