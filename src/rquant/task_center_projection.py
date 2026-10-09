"""Typed task evidence bound to the original ops snapshot and exact projections."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Literal

from pydantic import Field, model_validator

from rquant.ops_status import OpsSnapshot
from rquant.lab_scheduling_control import LabSchedulingControlState
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.task_cpu import SLICES, TaskCpuEvidence
from rquant.task_unit_control import TaskUnitRunEvidence

if TYPE_CHECKING:
    from rquant.serving_read_models import ServingProjectionPayload

CPU_COLUMNS = (
    ("slice_name", "string"), ("host_name", "string"), ("boot_id", "string"),
    ("manifest_digest", "string"), ("percent", "string"), ("reason", "string"),
    ("capacity", "string"), ("effective_cpus", "string"),
    ("observed_at", "timestamp"), ("material_hash", "string"),
)
RUN_COLUMNS = (
    ("service", "string"), ("host_name", "string"), ("boot_id", "string"),
    ("invocation_id", "string"), ("origin", "string"), ("request_id", "string"),
    ("started_at", "timestamp"), ("ended_at", "timestamp"), ("duration_ns", "int"),
    ("result", "string"), ("exec_status", "int"), ("observed_at", "timestamp"),
    ("material_hash", "string"),
    ("evidence_json", "string"),
)
SCHEDULING_COLUMNS = (
    ("control_key", "string"), ("state_json", "string"),
    ("observed_at", "timestamp"), ("material_hash", "string"),
)


def scheduling_projection(state: LabSchedulingControlState, *, cutoff: datetime) -> ServingProjectionPayload:
    from rquant.serving_read_models import ServingProjectionPayload

    state = LabSchedulingControlState.model_validate(state)
    if cutoff.tzinfo is None or state.observed_at > cutoff:
        raise ValueError("scheduler state has facts after the publication cutoff")
    return ServingProjectionPayload(table_name="lab_scheduler_control", available_at=cutoff, rows=({
        "control_key": "scheduler", "state_json": state.model_dump_json(),
        "observed_at": state.observed_at.isoformat(), "material_hash": state.state_hash,
    },))


def validate_scheduling_projection(state: LabSchedulingControlState | None, projections: tuple[ServingProjectionPayload, ...]) -> None:
    selected = tuple(p for p in projections if p.table_name == "lab_scheduler_control")
    if state is None:
        if selected:
            raise ValueError("scheduler projection has no original typed state")
    elif len(selected) != 1 or selected[0] != scheduling_projection(state, cutoff=selected[0].available_at):
        raise ValueError("scheduler projection does not match complete original state")


def read_scheduling_projection(
    projections: tuple[ServingProjectionPayload, ...],
) -> LabSchedulingControlState | None:
    selected = tuple(p for p in projections if p.table_name == "lab_scheduler_control")
    if not selected:
        return None
    if len(selected) != 1 or len(selected[0].rows) != 1:
        raise ValueError("scheduler projection requires one complete original control row")
    payload = selected[0].rows[0]["state_json"]
    if not isinstance(payload, str):
        raise ValueError("scheduler projection state JSON must be a string")
    state = LabSchedulingControlState.model_validate_json(payload)
    validate_scheduling_projection(state, projections)
    return state


class TaskOpsEvidence(RuntimeContractModel):
    cpu: TaskCpuEvidence | None = None
    cpu_unavailable_reason: Literal["capture_unavailable", "budget_exceeded"] | None = None
    runs: tuple[TaskUnitRunEvidence, ...] = Field(default=(), max_length=32)

    @model_validator(mode="after")
    def validate_cpu_availability(self) -> TaskOpsEvidence:
        if (self.cpu is None) != (self.cpu_unavailable_reason is not None):
            raise ValueError("task CPU requires complete raw evidence or explicit unavailable reason")
        if len({run.unit for run in self.runs}) != len(self.runs):
            raise ValueError("ops run evidence contains duplicate exact units")
        return self


class TaskOpsSample(RuntimeContractModel):
    snapshot: OpsSnapshot
    evidence: TaskOpsEvidence

    @model_validator(mode="after")
    def bind_snapshot(self) -> TaskOpsSample:
        if self.evidence.cpu is not None:
            cpu = self.evidence.cpu
            raw = cpu.pair.current
            if cpu.cutoff != self.snapshot.sampled_at or raw.observed_at > self.snapshot.sampled_at:
                raise ValueError("CPU evidence differs from ops cutoff")
            if (raw.host_name, raw.boot_id, raw.manifest_digest) != (self.snapshot.host_name, self.snapshot.boot_id, self.snapshot.manifest_digest):
                raise ValueError("CPU evidence differs from ops source identity")
        exact = {unit.service for unit in self.snapshot.units}
        for run in self.evidence.runs:
            if (run.host_name, run.boot_id, run.manifest_digest) != (self.snapshot.host_name, self.snapshot.boot_id, self.snapshot.manifest_digest) or run.unit not in exact:
                raise ValueError("unit run differs from exact ops source identity")
            if run.observed_at > self.snapshot.sampled_at:
                raise ValueError("unit run has facts after ops cutoff")
        return self


def task_ops_projections(sample: TaskOpsSample) -> tuple[ServingProjectionPayload, ...]:
    from rquant.serving_read_models import ServingProjectionPayload

    sample = TaskOpsSample.model_validate(sample)
    snapshot, evidence = sample.snapshot, sample.evidence
    cpu = evidence.cpu
    rows = tuple({
        "slice_name": name, "host_name": snapshot.host_name, "boot_id": snapshot.boot_id,
        "manifest_digest": snapshot.manifest_digest,
        "percent": None if cpu is None else cpu.groups[index].percent,
        "reason": evidence.cpu_unavailable_reason if cpu is None else cpu.groups[index].reason,
        "capacity": None if cpu is None else cpu.groups[index].capacity,
        "effective_cpus": None if cpu is None else ",".join(str(c) for c in cpu.groups[index].effective_cpus),
        "observed_at": snapshot.sampled_at.isoformat(),
        "material_hash": canonical_sha256(evidence) if cpu is None else cpu.material_hash,
    } for index, name in enumerate(SLICES))
    return (
        ServingProjectionPayload(table_name="ops_task_cpu", available_at=snapshot.sampled_at, rows=rows),
        ServingProjectionPayload(table_name="ops_task_runs", available_at=snapshot.sampled_at, rows=tuple({
            "service": run.unit, "host_name": run.host_name, "boot_id": run.boot_id,
            "invocation_id": run.invocation_id, "origin": run.origin, "request_id": run.request_id,
            "started_at": run.started_at.isoformat(), "ended_at": None if run.ended_at is None else run.ended_at.isoformat(),
            "duration_ns": run.duration_ns, "result": run.result, "exec_status": run.exec_status,
            "observed_at": run.observed_at.isoformat(), "material_hash": run.material_hash, "evidence_json": run.model_dump_json(),
        } for run in evidence.runs)),
    )
