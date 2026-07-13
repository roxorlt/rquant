"""研究数据快照、覆盖率与质量问题的跨层数据契约。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
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
QualitySeverity: TypeAlias = Literal["P0", "P1", "P2", "P3"]
QualityIssueStatus: TypeAlias = Literal["open", "resolved"]
StableIdValue: TypeAlias = str | datetime | None
QualityEvidence: TypeAlias = dict[str, JsonValue]
TableWatermarks: TypeAlias = dict[str, str]


def utc_now() -> datetime:
    return datetime.now(UTC)


def normalize_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("business datetime must be timezone-aware UTC")
    return value.astimezone(UTC)


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


class DatasetSnapshotFinalization(MetadataModel):
    table_watermarks: TableWatermarks = Field(default_factory=dict)
    quality_issue_ids: tuple[str, ...] = ()
    completed_at: datetime = Field(default_factory=utc_now)

    @field_validator("completed_at")
    @classmethod
    def validate_completed_at(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)


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

    @model_validator(mode="after")
    def validate_lifecycle(self) -> DataQualityIssue:
        if self.last_seen_at < self.first_seen_at:
            raise ValueError("last_seen_at cannot be earlier than first_seen_at")
        if self.status == "open" and self.resolved_at is not None:
            raise ValueError("open data quality issue cannot have resolved_at")
        if self.status == "resolved" and self.resolved_at is None:
            raise ValueError("resolved data quality issue requires resolved_at")
        if self.resolved_at is not None and self.resolved_at < self.first_seen_at:
            raise ValueError("resolved_at cannot be earlier than first_seen_at")
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
