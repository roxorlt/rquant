"""Independent exploratory admission for verified factor history snapshots."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Literal, Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import DatasetSnapshot, DatasetSnapshotBinding
from rquant.research_snapshot import (
    FACTOR_SNAPSHOT_BUILDER_VERSION,
    FactorReadLease,
    ResearchExecutionSession,
)
from rquant.strategy_dependencies import factor_execution_dependencies

FactorAdmissionFailureCode = Literal[
    "snapshot_missing",
    "snapshot_invalid",
    "snapshot_identity",
    "snapshot_not_ready",
    "snapshot_range",
    "binding_missing",
    "binding_invalid",
    "binding_identity",
    "binding_not_ready",
    "binding_range",
    "binding_contract",
    "binding_builder",
    "binding_artifacts",
    "session_verification_failed",
    "generation_changed",
]
_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")


class FactorSnapshotAdmissionRequest(BaseModel):
    model_config = _IMMUTABLE

    snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    start_date: date
    end_date: date
    source_mode: Literal["historical_retrospective"]

    @model_validator(mode="after")
    def _valid_range(self) -> FactorSnapshotAdmissionRequest:
        if self.start_date > self.end_date:
            raise ValueError("factor admission start_date follows end_date")
        return self


class FactorSnapshotAdmissionFailure(BaseModel):
    model_config = _IMMUTABLE

    code: FactorAdmissionFailureCode
    message: str


class FactorSnapshotAdmissionDecision(BaseModel):
    model_config = _IMMUTABLE

    allowed: bool
    research_status: Literal["exploratory"]
    snapshot_id: str
    binding_hash: str
    as_of_time: AwareDatetime | None
    source_mode: Literal["historical_retrospective"]
    source_read_boundary: Literal["single_snapshot_transaction"] | None
    failures: tuple[FactorSnapshotAdmissionFailure, ...]


class FactorSnapshotAdmissionError(PermissionError):
    def __init__(self, decision: FactorSnapshotAdmissionDecision) -> None:
        self.decision = decision
        super().__init__(
            "factor snapshot admission denied: "
            + ", ".join(failure.code for failure in decision.failures)
        )


class FactorSnapshotMetadataStore(Protocol):
    def get_dataset_snapshot(self, snapshot_id: str) -> DatasetSnapshot | None: ...

    def get_dataset_snapshot_binding(self, snapshot_id: str) -> DatasetSnapshotBinding | None: ...


def _failure(code: FactorAdmissionFailureCode, message: str) -> FactorSnapshotAdmissionFailure:
    return FactorSnapshotAdmissionFailure(code=code, message=message)


def _decision(
    request: FactorSnapshotAdmissionRequest,
    *,
    as_of_time: AwareDatetime | None,
    failures: list[FactorSnapshotAdmissionFailure],
) -> FactorSnapshotAdmissionDecision:
    return FactorSnapshotAdmissionDecision(
        allowed=not failures,
        research_status="exploratory",
        snapshot_id=request.snapshot_id,
        binding_hash=request.binding_hash,
        as_of_time=as_of_time,
        source_mode="historical_retrospective",
        source_read_boundary="single_snapshot_transaction" if not failures else None,
        failures=tuple(failures),
    )


def evaluate_factor_snapshot_admission(
    request: FactorSnapshotAdmissionRequest,
    *,
    snapshot: DatasetSnapshot | None,
    binding: DatasetSnapshotBinding | None,
) -> FactorSnapshotAdmissionDecision:
    """Validate factor-only identity and exact materialized dependency closure."""
    checked = FactorSnapshotAdmissionRequest.model_validate(request)
    failures: list[FactorSnapshotAdmissionFailure] = []
    if snapshot is None:
        failures.append(_failure("snapshot_missing", "factor snapshot is missing"))
    else:
        try:
            snapshot = DatasetSnapshot.model_validate(
                snapshot.model_dump(exclude_computed_fields=True)
            )
        except Exception:
            failures.append(_failure("snapshot_invalid", "factor snapshot is invalid"))
            snapshot = None
    if snapshot is not None:
        if snapshot.snapshot_id != checked.snapshot_id or snapshot.strategy_name != "factor_eval":
            failures.append(_failure("snapshot_identity", "snapshot is not requested factor_eval"))
        if snapshot.status != "ready":
            failures.append(_failure("snapshot_not_ready", "factor snapshot is not ready"))
        first = snapshot.table_watermarks.get("manifest_start_date")
        last = snapshot.table_watermarks.get("manifest_end_date")
        if (
            first is None
            or last is None
            or first > checked.start_date.isoformat()
            or last < checked.end_date.isoformat()
            or snapshot.as_of_time.date() < checked.end_date
        ):
            failures.append(_failure("snapshot_range", "factor snapshot range is insufficient"))

    if binding is None:
        failures.append(_failure("binding_missing", "factor binding is missing"))
    else:
        try:
            binding = DatasetSnapshotBinding.model_validate(
                binding.model_dump(exclude_computed_fields=True)
            )
        except Exception:
            failures.append(_failure("binding_invalid", "factor binding is invalid"))
            binding = None
    if binding is not None:
        manifest = binding.manifest
        if (
            binding.snapshot_id != checked.snapshot_id
            or binding.binding_hash != checked.binding_hash
            or manifest.strategy_name != "factor_eval"
            or binding.artifact_root != "research_lake"
            or snapshot is None
            or manifest.code_commit != snapshot.code_commit
            or manifest.as_of_time != snapshot.as_of_time
        ):
            failures.append(_failure("binding_identity", "factor binding identity differs"))
        if binding.status != "ready":
            failures.append(_failure("binding_not_ready", "factor binding is not ready"))
        if manifest.start_date > checked.start_date or manifest.end_date < checked.end_date:
            failures.append(_failure("binding_range", "factor binding range is insufficient"))
        dependencies = factor_execution_dependencies()
        if manifest.dependency_contract_version != dependencies.contract_version:
            failures.append(_failure("binding_contract", "factor dependency version differs"))
        if manifest.builder_version != FACTOR_SNAPSHOT_BUILDER_VERSION:
            failures.append(
                _failure("binding_builder", "factor source transaction proof is absent")
            )
        if any(
            value is not None
            for value in (
                manifest.eligibility_resolution_hash,
                manifest.eligibility_expected_dates,
                manifest.eligibility_complete_dates,
            )
        ):
            failures.append(
                _failure("binding_artifacts", "factor binding has strategy eligibility")
            )
        expected = {item.table_name: item for item in dependencies.materialized_tables}
        artifacts = manifest.artifacts
        observed_names = [item.table_name for item in artifacts]
        if len(artifacts) != 3 or set(observed_names) != set(expected):
            failures.append(
                _failure("binding_artifacts", "factor binding needs exactly three tables")
            )
        else:
            for artifact in artifacts:
                dependency = expected[artifact.table_name]
                required_key = (
                    ("exchange", "cal_date")
                    if artifact.table_name == "trade_calendar"
                    else ("ts_code", "trade_date")
                )
                if (
                    artifact.artifact_type != "materialized_table"
                    or artifact.dataset_id != dependency.dataset_id
                    or artifact.table_name != dependency.table_name
                    or artifact.artifact_key
                    != (
                        f"{dependency.dataset_id}:{manifest.start_date.isoformat()}:"
                        f"{manifest.end_date.isoformat()}"
                    )
                    or artifact.primary_key != required_key
                    or artifact.event_column != dependency.date_column
                ):
                    failures.append(
                        _failure("binding_artifacts", "factor artifact contract differs")
                    )
                    break
    return _decision(
        checked,
        as_of_time=None if snapshot is None else snapshot.as_of_time,
        failures=failures,
    )


@contextmanager
def open_factor_snapshot_admission(
    request: FactorSnapshotAdmissionRequest,
    *,
    metadata_store: FactorSnapshotMetadataStore,
    lake_root: Path,
) -> Iterator[tuple[FactorReadLease, FactorSnapshotAdmissionDecision]]:
    """Open one verified session before exposing its factor-only read lease."""
    checked = FactorSnapshotAdmissionRequest.model_validate(request)
    snapshot = metadata_store.get_dataset_snapshot(checked.snapshot_id)
    binding = metadata_store.get_dataset_snapshot_binding(checked.snapshot_id)
    decision = evaluate_factor_snapshot_admission(checked, snapshot=snapshot, binding=binding)
    if not decision.allowed or binding is None:
        raise FactorSnapshotAdmissionError(decision)
    try:
        session = ResearchExecutionSession(binding=binding, lake_root=lake_root)
    except Exception as exc:
        raise FactorSnapshotAdmissionError(
            _decision(
                checked,
                as_of_time=decision.as_of_time,
                failures=[
                    _failure("session_verification_failed", "bound artifacts failed verification")
                ],
            )
        ) from exc
    with session:
        current_snapshot = metadata_store.get_dataset_snapshot(checked.snapshot_id)
        current_binding = metadata_store.get_dataset_snapshot_binding(checked.snapshot_id)
        verified = evaluate_factor_snapshot_admission(
            checked, snapshot=current_snapshot, binding=current_binding
        )
        if (
            not verified.allowed
            or current_binding != binding
            or session.snapshot_id != checked.snapshot_id
            or session.binding_hash != checked.binding_hash
        ):
            raise FactorSnapshotAdmissionError(
                _decision(
                    checked,
                    as_of_time=verified.as_of_time,
                    failures=[
                        _failure("generation_changed", "factor binding changed during verification")
                    ],
                )
            )
        yield (
            FactorReadLease(session, start_date=checked.start_date, end_date=checked.end_date),
            verified,
        )
