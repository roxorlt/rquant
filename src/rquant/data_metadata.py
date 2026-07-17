"""研究数据快照、覆盖率与质量问题的跨层数据契约。"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import PurePosixPath
from typing import Literal, Self, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    computed_field,
    field_validator,
    model_validator,
)

SnapshotStatus: TypeAlias = Literal["building", "ready"]
SnapshotArtifactType: TypeAlias = Literal["lake_partition", "materialized_table"]
QualitySeverity: TypeAlias = Literal["P0", "P1", "P2", "P3"]
QualityIssueStatus: TypeAlias = Literal["open", "resolved"]
DataAuditRunStatus: TypeAlias = Literal["running", "completed", "failed"]
StableIdValue: TypeAlias = str | datetime | None
QualityEvidence: TypeAlias = dict[str, JsonValue]
TableWatermarks: TypeAlias = dict[str, str]


class DatasetSnapshotWriteConflictError(RuntimeError):
    """A retryable conflict between concurrent snapshot metadata writers."""


def utc_now() -> datetime:
    return datetime.now(UTC)


def normalize_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("business datetime must be timezone-aware UTC")
    return value.astimezone(UTC)


def _validate_finite_json(value: JsonValue) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON numeric values must be finite")
    if isinstance(value, list):
        for item in value:
            _validate_finite_json(item)
    elif isinstance(value, dict):
        for item in value.values():
            _validate_finite_json(item)


def stable_sha256(
    namespace: str,
    fields: Mapping[str, StableIdValue],
) -> str:
    """Return a canonical SHA-256 hex digest for a typed identity payload."""
    normalized_fields: dict[str, str | None] = {}
    for key, value in fields.items():
        normalized_key = key.strip()
        if isinstance(value, datetime):
            utc_value = normalize_utc_datetime(value)
            normalized_fields[normalized_key] = utc_value.isoformat(
                timespec="microseconds"
            ).replace("+00:00", "Z")
        elif isinstance(value, str):
            normalized_fields[normalized_key] = value.strip()
        else:
            normalized_fields[normalized_key] = value
    payload = json.dumps(
        {"namespace": namespace.strip(), "fields": normalized_fields},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def canonical_json_sha256(namespace: str, payload: JsonValue) -> str:
    """Hash a JSON payload after canonical key ordering and compact encoding."""
    _validate_finite_json(payload)
    encoded = json.dumps(
        {"namespace": namespace.strip(), "payload": payload},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class MetadataModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class DatasetSnapshot(MetadataModel):
    strategy_name: str = Field(min_length=1)
    manifest_id: str | None = None
    as_of_time: datetime
    code_commit: str = Field(min_length=1)
    origin: str = Field(min_length=1)
    status: SnapshotStatus = "building"
    table_watermarks: TableWatermarks = Field(default_factory=dict)
    quality_issue_ids: tuple[str, ...] = ()
    created_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None

    @field_validator("as_of_time", "created_at", "completed_at")
    @classmethod
    def validate_business_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_lifecycle(self) -> DatasetSnapshot:
        if self.status == "building" and (
            self.completed_at is not None
            or self.table_watermarks
            or self.quality_issue_ids
        ):
            raise ValueError("building snapshot cannot contain finalized metadata")
        if self.status == "ready" and self.completed_at is None:
            raise ValueError("ready snapshot requires completed_at")
        if self.completed_at is not None and self.completed_at < self.created_at:
            raise ValueError("completed_at cannot be earlier than created_at")
        return self

    @computed_field
    @property
    def snapshot_id(self) -> str:
        return stable_sha256(
            "dataset_snapshot",
            {
                "strategy_name": self.strategy_name,
                "manifest_id": self.manifest_id,
                "as_of_time": self.as_of_time,
                "code_commit": self.code_commit,
                "origin": self.origin,
            },
        )

    @classmethod
    def create(
        cls,
        *,
        strategy_name: str,
        as_of_time: datetime,
        code_commit: str,
        origin: str,
        manifest_id: str | None = None,
        created_at: datetime | None = None,
    ) -> Self:
        values: dict[str, object] = {
            "strategy_name": strategy_name,
            "manifest_id": manifest_id,
            "as_of_time": as_of_time,
            "code_commit": code_commit,
            "origin": origin,
        }
        if created_at is not None:
            values["created_at"] = created_at
        return cls.model_validate(values)

    def finalize(self, finalization: DatasetSnapshotFinalization) -> Self:
        if self.status != "building":
            raise ValueError("only a building snapshot can be finalized")
        return self.model_copy(
            update={
                "status": "ready",
                "table_watermarks": finalization.table_watermarks,
                "quality_issue_ids": finalization.quality_issue_ids,
                "completed_at": finalization.completed_at,
            }
        )


class DatasetSnapshotFinalization(MetadataModel):
    table_watermarks: TableWatermarks = Field(default_factory=dict)
    quality_issue_ids: tuple[str, ...] = ()
    completed_at: datetime = Field(default_factory=utc_now)

    @field_validator("completed_at")
    @classmethod
    def validate_completed_at(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)


class DatasetSnapshotArtifact(MetadataModel):
    artifact_type: SnapshotArtifactType
    dataset_id: str = Field(min_length=1)
    table_name: str = Field(min_length=1)
    artifact_key: str = Field(min_length=1)
    partition_id: str | None = None
    relative_path: str = Field(min_length=1)
    row_count: int = Field(ge=0)
    schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_size: int | None = Field(default=None, gt=0)
    earliest_time: str | None = None
    latest_time: str | None = None
    event_column: str | None = None
    source: str | None = None
    primary_key: tuple[str, ...] = ()
    revision_created_at: datetime | None = None
    catalog_updated_at: datetime | None = None

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("artifact relative_path must stay within its root")
        return path.as_posix()

    @field_validator("revision_created_at", "catalog_updated_at")
    @classmethod
    def validate_revision_time(
        cls,
        value: datetime | None,
    ) -> datetime | None:
        return None if value is None else normalize_utc_datetime(value)


class DatasetSnapshotBindingManifest(MetadataModel):
    manifest_version: Literal[1] = 1
    snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    strategy_name: str = Field(min_length=1)
    start_date: date
    end_date: date
    as_of_time: datetime
    code_commit: str = Field(min_length=1)
    dependency_contract_version: str = Field(min_length=1)
    builder_version: str = Field(min_length=1)
    eligibility_resolution_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    eligibility_expected_dates: int | None = Field(default=None, ge=0)
    eligibility_complete_dates: int | None = Field(default=None, ge=0)
    artifacts: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=1)

    @field_validator("as_of_time")
    @classmethod
    def validate_as_of_time(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_manifest(self) -> DatasetSnapshotBindingManifest:
        if self.start_date > self.end_date:
            raise ValueError("binding manifest start_date cannot follow end_date")
        keys = [artifact.artifact_key for artifact in self.artifacts]
        if len(keys) != len(set(keys)):
            raise ValueError("binding manifest artifact_key values must be unique")
        counts = (
            self.eligibility_expected_dates,
            self.eligibility_complete_dates,
        )
        if (self.eligibility_resolution_hash is None) != all(
            value is None for value in counts
        ):
            raise ValueError(
                "eligibility resolution hash and counts must be provided together"
            )
        if (
            self.eligibility_expected_dates is not None
            and self.eligibility_complete_dates is not None
            and self.eligibility_complete_dates > self.eligibility_expected_dates
        ):
            raise ValueError(
                "eligibility complete dates cannot exceed expected dates"
            )
        return self

    @computed_field
    @property
    def manifest_hash(self) -> str:
        payload = self.model_dump(
            mode="json",
            exclude_computed_fields=True,
        )
        payload["artifacts"] = sorted(
            payload["artifacts"],
            key=lambda artifact: str(artifact["artifact_key"]),
        )
        return canonical_json_sha256("dataset_snapshot_binding_manifest", payload)


class DatasetSnapshotBinding(MetadataModel):
    binding_version: Literal[1] = 1
    snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest: DatasetSnapshotBindingManifest
    artifact_root: str = Field(min_length=1)
    manifest_relative_path: str = Field(min_length=1)
    status: SnapshotStatus = "building"
    created_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None

    @field_validator("manifest_relative_path")
    @classmethod
    def validate_manifest_relative_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("manifest_relative_path must stay within artifact_root")
        return path.as_posix()

    @field_validator("created_at", "completed_at")
    @classmethod
    def validate_business_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_binding(self) -> DatasetSnapshotBinding:
        if self.snapshot_id != self.manifest.snapshot_id:
            raise ValueError("binding snapshot_id must match manifest snapshot_id")
        if self.manifest_hash != self.manifest.manifest_hash:
            raise ValueError("binding manifest_hash must match canonical manifest")
        if self.status == "building" and self.completed_at is not None:
            raise ValueError("building binding cannot contain completed_at")
        if self.status == "ready" and self.completed_at is None:
            raise ValueError("ready binding requires completed_at")
        if self.completed_at is not None and self.completed_at < self.created_at:
            raise ValueError("binding completed_at cannot precede created_at")
        return self

    @computed_field
    @property
    def binding_hash(self) -> str:
        return stable_sha256(
            "dataset_snapshot_binding",
            {
                "binding_version": str(self.binding_version),
                "snapshot_id": self.snapshot_id,
                "manifest_hash": self.manifest_hash,
            },
        )

    @classmethod
    def create(
        cls,
        *,
        manifest: DatasetSnapshotBindingManifest,
        artifact_root: str,
        manifest_relative_path: str,
        created_at: datetime | None = None,
    ) -> Self:
        values: dict[str, object] = {
            "snapshot_id": manifest.snapshot_id,
            "manifest_hash": manifest.manifest_hash,
            "manifest": manifest,
            "artifact_root": artifact_root,
            "manifest_relative_path": manifest_relative_path,
        }
        if created_at is not None:
            values["created_at"] = created_at
        return cls.model_validate(values)

    def finalize(
        self,
        finalization: DatasetSnapshotBindingFinalization,
    ) -> Self:
        if self.status != "building":
            raise ValueError("only a building binding can be finalized")
        return self.model_copy(
            update={
                "status": "ready",
                "completed_at": finalization.completed_at,
            }
        )


class DatasetSnapshotBindingFinalization(MetadataModel):
    completed_at: datetime = Field(default_factory=utc_now)

    @field_validator("completed_at")
    @classmethod
    def validate_completed_at(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)


class DataAuditRunFinalization(MetadataModel):
    finding_issue_ids: tuple[str, ...] = ()
    p0_count: int = Field(ge=0)
    completed_at: datetime = Field(default_factory=utc_now)

    @field_validator("completed_at")
    @classmethod
    def validate_completed_at(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_counts(self) -> DataAuditRunFinalization:
        if len(self.finding_issue_ids) != len(set(self.finding_issue_ids)):
            raise ValueError("finding_issue_ids cannot contain duplicates")
        if self.p0_count > len(self.finding_issue_ids):
            raise ValueError("p0_count cannot exceed finding count")
        return self


class DataAuditRun(MetadataModel):
    as_of_date: date
    range_start: date
    range_end: date
    rule_set_version: str = Field(min_length=1)
    status: DataAuditRunStatus = "running"
    finding_issue_ids: tuple[str, ...] = ()
    p0_count: int = Field(default=0, ge=0)
    observed_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None
    error_message: str | None = None

    @field_validator("observed_at", "completed_at")
    @classmethod
    def validate_business_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_lifecycle(self) -> DataAuditRun:
        if not (self.range_start <= self.range_end <= self.as_of_date):
            raise ValueError("audit range must end on or before as_of_date")
        if len(self.finding_issue_ids) != len(set(self.finding_issue_ids)):
            raise ValueError("finding_issue_ids cannot contain duplicates")
        if self.p0_count > len(self.finding_issue_ids):
            raise ValueError("p0_count cannot exceed finding count")
        if self.status == "running":
            if self.completed_at is not None or self.error_message is not None:
                raise ValueError("running audit cannot contain terminal fields")
            if self.finding_issue_ids or self.p0_count:
                raise ValueError("running audit cannot contain findings")
        elif self.status == "completed":
            if self.completed_at is None or self.error_message is not None:
                raise ValueError("completed audit requires completed_at and no error")
        elif self.completed_at is None or not self.error_message:
            raise ValueError("failed audit requires completed_at and error_message")
        if self.completed_at is not None and self.completed_at < self.observed_at:
            raise ValueError("completed_at cannot be earlier than observed_at")
        return self

    @computed_field
    @property
    def audit_run_id(self) -> str:
        return stable_sha256(
            "data_audit_run",
            {
                "as_of_date": self.as_of_date.isoformat(),
                "range_start": self.range_start.isoformat(),
                "range_end": self.range_end.isoformat(),
                "rule_set_version": self.rule_set_version,
                "observed_at": self.observed_at,
            },
        )

    @classmethod
    def create(
        cls,
        *,
        as_of_date: date,
        range_start: date,
        range_end: date,
        rule_set_version: str,
        observed_at: datetime | None = None,
    ) -> Self:
        values: dict[str, object] = {
            "as_of_date": as_of_date,
            "range_start": range_start,
            "range_end": range_end,
            "rule_set_version": rule_set_version,
        }
        if observed_at is not None:
            values["observed_at"] = observed_at
        return cls.model_validate(values)

    def finalize(self, finalization: DataAuditRunFinalization) -> Self:
        if self.status != "running":
            raise ValueError("only a running audit can be finalized")
        return self.model_copy(
            update={
                "status": "completed",
                "finding_issue_ids": finalization.finding_issue_ids,
                "p0_count": finalization.p0_count,
                "completed_at": finalization.completed_at,
            }
        )


class DatasetCoverage(MetadataModel):
    snapshot_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    coverage_scope: str = Field(min_length=1)
    table_name: str = Field(min_length=1)
    expected_count: int = Field(ge=0)
    available_count: int = Field(ge=0)
    missing_reasons: tuple[str, ...] = ()
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_counts(self) -> DatasetCoverage:
        if self.available_count > self.expected_count:
            raise ValueError("available_count cannot exceed expected_count")
        return self

    @computed_field
    @property
    def missing_count(self) -> int:
        return self.expected_count - self.available_count

    @computed_field
    @property
    def coverage_ratio(self) -> float | None:
        if self.expected_count == 0:
            return None
        return self.available_count / self.expected_count


class DataQualityIssue(MetadataModel):
    rule_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    severity: QualitySeverity
    status: QualityIssueStatus = "open"
    scope_key: str = Field(min_length=1)
    message: str = Field(min_length=1)
    evidence: QualityEvidence = Field(default_factory=dict)
    first_seen_at: datetime = Field(default_factory=utc_now)
    last_seen_at: datetime = Field(default_factory=utc_now)
    resolved_at: datetime | None = None

    @field_validator("first_seen_at", "last_seen_at", "resolved_at")
    @classmethod
    def validate_business_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else normalize_utc_datetime(value)

    @field_validator("evidence")
    @classmethod
    def validate_evidence(cls, value: QualityEvidence) -> QualityEvidence:
        _validate_finite_json(value)
        return value

    @model_validator(mode="after")
    def validate_lifecycle(self) -> DataQualityIssue:
        if self.last_seen_at < self.first_seen_at:
            raise ValueError("last_seen_at cannot be earlier than first_seen_at")
        if self.status == "open" and self.resolved_at is not None:
            raise ValueError("open data quality issue cannot have resolved_at")
        if self.status == "resolved" and self.resolved_at is None:
            raise ValueError("resolved data quality issue requires resolved_at")
        if self.resolved_at is not None and self.resolved_at < self.last_seen_at:
            raise ValueError("resolved_at cannot be earlier than last_seen_at")
        return self

    @computed_field
    @property
    def issue_id(self) -> str:
        return stable_sha256(
            "data_quality_issue",
            {
                "rule_id": self.rule_id,
                "dataset_id": self.dataset_id,
                "scope_key": self.scope_key,
            },
        )

    @classmethod
    def detected(
        cls,
        *,
        rule_id: str,
        dataset_id: str,
        severity: QualitySeverity,
        scope_key: str,
        message: str,
        evidence: QualityEvidence | None = None,
        observed_at: datetime | None = None,
    ) -> Self:
        detected_at = utc_now() if observed_at is None else observed_at
        return cls(
            rule_id=rule_id,
            dataset_id=dataset_id,
            severity=severity,
            status="open",
            scope_key=scope_key,
            message=message,
            evidence={} if evidence is None else evidence,
            first_seen_at=detected_at,
            last_seen_at=detected_at,
        )
