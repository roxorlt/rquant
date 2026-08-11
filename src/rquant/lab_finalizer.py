"""Read-only Strategy Lab result finalization and durable commit publication."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Literal, TypeVar
from uuid import NAMESPACE_URL, UUID, uuid5

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.canonical_json_stream import CANONICAL_JSON_STREAM_SCRATCH_BYTES
from rquant.lab_artifact_protocol import (
    LabAcknowledgedArtifactCommit,
    LabArtifactCommit,
    LabArtifactCommitEnvelope,
    LabArtifactCommitReceipt,
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
    LabAuthenticatedArtifactCommitIdentity,
    LabFinalizerAuthorityAuthenticationError,
    LabFinalizerAuthorityClaims,
    LabFinalizerAuthorityKey,
    LabFinalizerAuthorityShardEvidence,
    LabFinalizerAuthoritySigningKeyProvider,
    LabFinalizerAuthorityVerificationKeyProvider,
    authenticate_artifact_commit_identity,
    sign_finalizer_authority,
)
from rquant.lab_artifacts import (
    LabArtifactError,
    LabArtifactFinalizationLockError,
    LabArtifactFinalizationLockTimeoutError,
    LabArtifactPayloadBudget,
    LabArtifactPayloadLimitError,
    LabArtifactRecoveryAuthority,
    LabArtifactRecoveryRecord,
    LabJobArtifactCandidate,
    LabJobArtifactPlan,
    LabJobArtifactStore,
    LabSealedJobArtifact,
)
from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.lab_jobs import (
    COMPLETE_RESULT_CONTRACT_VERSION,
    MAX_JOB_SHARDS,
    LabFinalizationShardEvidence,
    LabFinalizationSnapshot,
    LabJobReader,
)
from rquant.lab_result_digest import (
    LabResultDigestPolicy,
    LabResultDigestProvenanceError,
    require_matching_manifest_digest_provenance,
    resolve_success_digest_provenance,
)
from rquant.lab_shard_protocol import LabShardSucceeded
from rquant.lab_worker import LabShardResultManifest, canonical_shard_frame_digest
from rquant.strategy_job_adapters import (
    LabJobExecutionResult,
    LabShardExecutionResult,
    LabShardMetric,
    LabShardTable,
    StrategyJobAdapterRegistry,
    default_strategy_job_adapter_registry,
)
from rquant.strict_json import canonical_json_bytes, strict_model_validate_canonical_json


class LabFinalizationError(RuntimeError):
    """Base error for independent complete-result finalization."""


class LabFinalizationIntegrityError(LabFinalizationError):
    """Accepted ledger evidence or immutable artifact bytes failed validation."""


class LabFinalizationResourceLimitError(LabFinalizationIntegrityError):
    """A valid-looking finalization input exceeded configured service resources."""


class LabFinalizationCodeMismatchError(LabFinalizationError):
    """The ready job was produced by code other than this finalizer runtime."""

    def __init__(self, *, expected: str, actual: str) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"finalizer runtime code SHA {actual} does not match ready job code SHA {expected}"
        )


class LabFinalizationCodeProviderError(LabFinalizationError):
    """The trusted runtime code provider failed or returned an invalid SHA."""


class LabFinalizationCoordinationError(LabFinalizationError):
    """The per-result finalization decision could not be safely coordinated."""


class LabFinalizationCoordinationTimeoutError(LabFinalizationCoordinationError):
    """The per-result finalization decision lock exceeded its deadline."""


class LabFinalizerModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=False,
    )


class LabFinalizerShardSummary(LabFinalizerModel):
    shard_index: int = Field(ge=0)
    shard_id: UUID
    result_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    metrics: tuple[LabShardMetric, ...]


class LabFinalizerTableSummary(LabFinalizerModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    row_count: int = Field(ge=0)
    columns: tuple[str, ...]


class LabFinalizerMetrics(LabFinalizerModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    result_contract_version: str = Field(min_length=1)
    finalizer_code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    shard_count: int = Field(ge=1)
    shards: tuple[LabFinalizerShardSummary, ...]
    tables: tuple[LabFinalizerTableSummary, ...]

    @model_validator(mode="after")
    def validate_summary(self) -> LabFinalizerMetrics:
        if self.shard_count != len(self.shards):
            raise ValueError("shard_count does not match shard summaries")
        if tuple(item.shard_index for item in self.shards) != tuple(range(self.shard_count)):
            raise ValueError("shard summaries must be complete and ordered")
        if not self.tables:
            raise ValueError("finalizer metrics require complete result tables")
        return self


class LabFinalizerResult(LabFinalizerModel):
    status: Literal["not_ready", "published", "acknowledged", "rejected"]
    job_id: UUID
    request_id: UUID | None = None
    manifest_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    complete_result_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    rejection_reason: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_result_identity(self) -> LabFinalizerResult:
        identities = (self.request_id, self.manifest_hash, self.complete_result_hash)
        if self.status == "not_ready" and any(value is not None for value in identities):
            raise ValueError("not_ready result cannot claim an artifact identity")
        if self.status != "not_ready" and any(value is None for value in identities):
            raise ValueError("published result requires a complete artifact identity")
        if (self.status == "rejected") != (self.rejection_reason is not None):
            raise ValueError("only rejected results may contain a rejection reason")
        return self


class LabShardBundleLimits(LabFinalizerModel):
    max_manifest_bytes: int = Field(default=4 * 1024 * 1024, ge=1)
    max_manifest_peak_bytes: int = Field(default=64 * 1024 * 1024, ge=1)
    manifest_model_expansion_factor: int = Field(default=8, ge=1, le=64)
    max_artifact_count: int = Field(default=256, ge=1)
    max_metric_count: int = Field(default=256, ge=0)
    max_metric_name_bytes: int = Field(default=128, ge=1)
    max_metric_value_bytes: int = Field(default=64 * 1024, ge=1)
    max_metrics_encoded_bytes: int = Field(default=1024 * 1024, ge=2)
    max_single_file_bytes: int = Field(default=128 * 1024 * 1024, ge=1)
    max_bundle_total_bytes: int = Field(default=256 * 1024 * 1024, ge=1)
    max_row_count: int = Field(default=5_000_000, ge=1)
    max_column_count: int = Field(default=512, ge=1)
    max_parquet_uncompressed_bytes: int = Field(default=128 * 1024 * 1024, ge=1)
    max_arrow_table_bytes: int = Field(default=128 * 1024 * 1024, ge=1)
    max_materialized_dataframe_bytes: int = Field(default=256 * 1024 * 1024, ge=1)
    max_consecutive_interrupted_reads: int = Field(default=8, ge=0, le=1024)

    @model_validator(mode="after")
    def validate_bundle_limits(self) -> LabShardBundleLimits:
        if self.max_single_file_bytes > self.max_bundle_total_bytes:
            raise ValueError("single file limit cannot exceed bundle total limit")
        return self


class LabFinalizerJobLimits(LabFinalizerModel):
    max_shards: int = Field(default=MAX_JOB_SHARDS, ge=1, le=MAX_JOB_SHARDS)
    max_total_shard_rows: int = Field(default=5_000_000, ge=1)
    max_total_compressed_bytes: int = Field(default=512 * 1024 * 1024, ge=1)
    max_total_declared_uncompressed_bytes: int = Field(default=512 * 1024 * 1024, ge=1)
    max_total_arrow_bytes: int = Field(default=256 * 1024 * 1024, ge=1)
    max_total_shard_dataframe_bytes: int = Field(default=256 * 1024 * 1024, ge=1)
    max_total_shard_metric_bytes: int = Field(default=8 * 1024 * 1024, ge=1)
    max_snapshot_control_bytes: int = Field(default=32 * 1024 * 1024, ge=1)
    max_spec_bytes: int = Field(default=2 * 1024 * 1024, ge=1)
    max_final_metrics_bytes: int = Field(default=16 * 1024 * 1024, ge=1)
    max_report_markdown_bytes: int = Field(default=32 * 1024 * 1024, ge=1)
    max_aggregate_rows: int = Field(default=5_000_000, ge=1)
    max_aggregate_dataframe_bytes: int = Field(default=256 * 1024 * 1024, ge=1)
    max_final_artifact_payload_bytes: int = Field(default=128 * 1024 * 1024, ge=1)
    max_final_artifact_single_payload_bytes: int = Field(
        default=64 * 1024 * 1024,
        ge=1,
    )
    max_final_artifact_table_count: int = Field(default=128, ge=1)
    max_peak_resident_bytes: int = Field(default=640 * 1024 * 1024, ge=1)
    finalization_lock_timeout_seconds: float = Field(default=30.0, gt=0, le=3600)
    finalization_lock_poll_interval_seconds: float = Field(default=0.01, gt=0, le=1)

    @model_validator(mode="after")
    def validate_job_limits(self) -> LabFinalizerJobLimits:
        if self.max_final_artifact_single_payload_bytes > self.max_final_artifact_payload_bytes:
            raise ValueError("single final payload limit cannot exceed total payload limit")
        if self.finalization_lock_poll_interval_seconds > self.finalization_lock_timeout_seconds:
            raise ValueError("finalization lock poll interval cannot exceed its timeout")
        return self


class LabArtifactRoundtripPeakUsage(LabFinalizerModel):
    source_dataframe_bytes: int = Field(ge=0)
    roundtrip_dataframe_bytes: int = Field(ge=0)
    arrow_working_bytes: int = Field(ge=0)
    payload_bytes: int = Field(ge=0)
    payload_copy_bytes: int = Field(ge=0)
    hash_scratch_bytes: int = Field(ge=0)
    control_bytes: int = Field(default=0, ge=0)

    @property
    def peak_resident_bytes(self) -> int:
        return (
            self.source_dataframe_bytes
            + self.roundtrip_dataframe_bytes
            + self.arrow_working_bytes
            + self.payload_bytes
            + self.payload_copy_bytes
            + self.hash_scratch_bytes
            + self.control_bytes
        )

    @classmethod
    def conservative(
        cls,
        *,
        aggregate_dataframe_bytes: int,
        payload_bytes: int,
        control_bytes: int = 0,
    ) -> LabArtifactRoundtripPeakUsage:
        return cls(
            source_dataframe_bytes=aggregate_dataframe_bytes,
            roundtrip_dataframe_bytes=aggregate_dataframe_bytes,
            arrow_working_bytes=aggregate_dataframe_bytes,
            payload_bytes=payload_bytes,
            payload_copy_bytes=payload_bytes,
            hash_scratch_bytes=CANONICAL_JSON_STREAM_SCRATCH_BYTES,
            control_bytes=control_bytes,
        )


class LabManifestResourceUsage(LabFinalizerModel):
    raw_bytes: int = Field(ge=0)
    estimated_model_bytes: int = Field(ge=0)
    validation_scratch_bytes: int = Field(ge=0)
    metric_count: int = Field(ge=0)
    max_metric_value_bytes: int = Field(ge=0)
    metrics_encoded_bytes: int = Field(ge=0)
    retained_metric_bytes: int = Field(ge=0)

    @property
    def peak_resident_bytes(self) -> int:
        return self.raw_bytes + self.estimated_model_bytes + self.validation_scratch_bytes


class LabFinalizerControlUsage(LabFinalizerModel):
    snapshot_control_bytes: int = Field(ge=0)
    spec_bytes: int = Field(ge=0)
    retained_shard_metric_bytes: int = Field(default=0, ge=0)
    final_metrics_bytes: int = Field(default=0, ge=0)
    report_bytes: int = Field(default=0, ge=0)

    @property
    def resident_bytes(self) -> int:
        return (
            self.snapshot_control_bytes
            + self.spec_bytes
            + self.retained_shard_metric_bytes
            + self.final_metrics_bytes
            + self.report_bytes
        )


class LabShardBundleUsage(LabFinalizerModel):
    row_count: int = Field(ge=0)
    compressed_bytes: int = Field(ge=0)
    declared_uncompressed_bytes: int = Field(ge=0)
    arrow_bytes: int = Field(ge=0)
    materialized_dataframe_bytes: int = Field(ge=0)
    manifest_peak_bytes: int = Field(ge=0)
    retained_metric_bytes: int = Field(ge=0)


class LabShardBundleInspection(LabFinalizerModel):
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    row_count: int = Field(ge=0)
    compressed_bytes: int = Field(ge=0)
    declared_uncompressed_bytes: int = Field(ge=0)
    estimated_arrow_bytes: int = Field(ge=0)
    estimated_pandas_bytes: int = Field(ge=0)
    manifest_usage: LabManifestResourceUsage


class LabParquetResourceSummary(LabFinalizerModel):
    row_count: int = Field(ge=0)
    column_count: int = Field(ge=0)
    columns: tuple[str, ...]
    declared_uncompressed_bytes: int = Field(ge=0)
    estimated_arrow_bytes: int = Field(ge=0)
    estimated_pandas_bytes: int = Field(ge=0)


@dataclass(frozen=True)
class _ParquetMaterialization:
    frame: pd.DataFrame
    arrow_bytes: int
    materialized_dataframe_bytes: int


@dataclass(frozen=True)
class _PathBinding:
    parent_descriptor: int
    name: str
    descriptor: int
    observation: tuple[int, int, int, int, int, int, int]


def _observation(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _resident_object_bytes(value: object, *, _seen: set[int] | None = None) -> int:
    seen = _seen if _seen is not None else set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, BaseModel):
        return size + sum(
            _resident_object_bytes(getattr(value, name), _seen=seen)
            for name in type(value).model_fields
        )
    if isinstance(value, Mapping):
        return size + sum(
            _resident_object_bytes(key, _seen=seen) + _resident_object_bytes(item, _seen=seen)
            for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return size + sum(_resident_object_bytes(item, _seen=seen) for item in value)
    return size


def _scan_json_string_end(payload: bytes, start: int, *, label: str) -> int:
    if start >= len(payload) or payload[start] != ord('"'):
        raise LabFinalizationIntegrityError(f"accepted shard {label} is not a JSON string")
    index = start + 1
    while index < len(payload):
        value = payload[index]
        if value == ord('"'):
            return index + 1
        if value == ord("\\"):
            index += 1
            if index >= len(payload):
                break
            escape = payload[index]
            if escape == ord("u"):
                end = index + 5
                if end > len(payload) or any(
                    character not in b"0123456789abcdefABCDEF"
                    for character in payload[index + 1 : end]
                ):
                    raise LabFinalizationIntegrityError(
                        f"accepted shard {label} has an invalid Unicode escape"
                    )
                index = end
                continue
            if escape not in b'"\\/bfnrt':
                raise LabFinalizationIntegrityError(
                    f"accepted shard {label} has an invalid JSON escape"
                )
        elif value < 0x20 or value > 0x7F:
            raise LabFinalizationIntegrityError(
                f"accepted shard {label} is not canonical ASCII JSON"
            )
        index += 1
    raise LabFinalizationIntegrityError(f"accepted shard {label} is unterminated")


def _preflight_manifest_metrics(
    payload: bytes,
    *,
    limits: LabShardBundleLimits,
) -> LabManifestResourceUsage:
    marker = b'"metrics":['
    if payload.count(marker) != 1:
        raise LabFinalizationIntegrityError("accepted shard manifest metrics are ambiguous")
    array_start = payload.index(marker) + len(marker) - 1
    index = array_start + 1
    count = 0
    maximum_value_bytes = 0
    if index >= len(payload):
        raise LabFinalizationIntegrityError("accepted shard manifest metrics are truncated")
    while payload[index] != ord("]"):
        if not payload.startswith(b'{"name":', index):
            raise LabFinalizationIntegrityError("accepted shard manifest metric is not canonical")
        name_start = index + len(b'{"name":')
        name_end = _scan_json_string_end(payload, name_start, label="metric name")
        name_bytes = name_end - name_start - 2
        if name_bytes > limits.max_metric_name_bytes:
            raise LabFinalizationResourceLimitError(
                "accepted shard metric name exceeds configured byte limit"
            )
        if not payload.startswith(b',"value":', name_end):
            raise LabFinalizationIntegrityError("accepted shard manifest metric is not canonical")
        value_start = name_end + len(b',"value":')
        if value_start >= len(payload):
            raise LabFinalizationIntegrityError("accepted shard metric value is truncated")
        if payload[value_start] == ord('"'):
            value_end = _scan_json_string_end(payload, value_start, label="metric value")
            value_bytes = value_end - value_start - 2
        else:
            value_end = payload.find(b"}", value_start)
            if value_end < 0:
                raise LabFinalizationIntegrityError("accepted shard metric value is truncated")
            value_bytes = value_end - value_start
        if value_bytes > limits.max_metric_value_bytes:
            raise LabFinalizationResourceLimitError(
                "accepted shard metric value exceeds configured byte limit"
            )
        if value_end >= len(payload) or payload[value_end] != ord("}"):
            raise LabFinalizationIntegrityError("accepted shard manifest metric is not canonical")
        count += 1
        if count > limits.max_metric_count:
            raise LabFinalizationResourceLimitError(
                "accepted shard metric count exceeds configured limit"
            )
        maximum_value_bytes = max(maximum_value_bytes, value_bytes)
        index = value_end + 1
        if index >= len(payload):
            raise LabFinalizationIntegrityError("accepted shard manifest metrics are truncated")
        if payload[index] == ord(","):
            index += 1
        elif payload[index] != ord("]"):
            raise LabFinalizationIntegrityError("accepted shard manifest metrics are not canonical")
    metrics_encoded_bytes = index - array_start + 1
    if metrics_encoded_bytes > limits.max_metrics_encoded_bytes:
        raise LabFinalizationResourceLimitError(
            "accepted shard metrics exceed configured encoded byte limit"
        )
    raw_bytes = len(payload)
    return LabManifestResourceUsage(
        raw_bytes=raw_bytes,
        estimated_model_bytes=raw_bytes * limits.manifest_model_expansion_factor,
        validation_scratch_bytes=CANONICAL_JSON_STREAM_SCRATCH_BYTES,
        metric_count=count,
        max_metric_value_bytes=maximum_value_bytes,
        metrics_encoded_bytes=metrics_encoded_bytes,
        retained_metric_bytes=(metrics_encoded_bytes * 4) + (count * 256),
    )


def _read_descriptor_bounded(
    descriptor: int,
    *,
    expected_size: int,
    max_bytes: int,
    max_consecutive_interrupted_reads: int = 8,
) -> bytes:
    if expected_size > max_bytes:
        raise LabFinalizationIntegrityError("accepted shard file exceeds configured byte limit")
    os.lseek(descriptor, 0, os.SEEK_SET)
    payload = bytearray()
    interrupted_reads = 0
    while len(payload) < expected_size:
        try:
            chunk = os.read(descriptor, min(1024 * 1024, expected_size - len(payload)))
        except InterruptedError:
            interrupted_reads += 1
            if interrupted_reads > max_consecutive_interrupted_reads:
                raise LabFinalizationIntegrityError(
                    "accepted shard exceeded interrupted read limit"
                ) from None
            continue
        interrupted_reads = 0
        if not chunk:
            raise LabFinalizationIntegrityError("accepted shard file ended before declared size")
        payload.extend(chunk)
    while True:
        try:
            extra = os.read(descriptor, 1)
            break
        except InterruptedError:
            interrupted_reads += 1
            if interrupted_reads > max_consecutive_interrupted_reads:
                raise LabFinalizationIntegrityError(
                    "accepted shard exceeded interrupted read limit"
                ) from None
            continue
    if extra:
        raise LabFinalizationIntegrityError("accepted shard file exceeds declared size")
    return bytes(payload)


def _sha256_descriptor_bounded(
    descriptor: int,
    *,
    expected_size: int,
    max_consecutive_interrupted_reads: int = 8,
) -> str:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    consumed = 0
    interrupted_reads = 0
    while consumed < expected_size:
        try:
            chunk = os.read(descriptor, min(1024 * 1024, expected_size - consumed))
        except InterruptedError:
            interrupted_reads += 1
            if interrupted_reads > max_consecutive_interrupted_reads:
                raise LabFinalizationIntegrityError(
                    "accepted shard exceeded interrupted read limit"
                ) from None
            continue
        interrupted_reads = 0
        if not chunk:
            raise LabFinalizationIntegrityError("accepted shard file ended before declared size")
        digest.update(chunk)
        consumed += len(chunk)
    while True:
        try:
            extra = os.read(descriptor, 1)
            break
        except InterruptedError:
            interrupted_reads += 1
            if interrupted_reads > max_consecutive_interrupted_reads:
                raise LabFinalizationIntegrityError(
                    "accepted shard exceeded interrupted read limit"
                ) from None
            continue
    if extra:
        raise LabFinalizationIntegrityError("accepted shard file exceeds declared size")
    return digest.hexdigest()


_StreamResult = TypeVar("_StreamResult")


def _run_with_descriptor_stream(
    descriptor: int,
    operation: Callable[[BinaryIO], _StreamResult],
    *,
    label: str,
) -> _StreamResult:
    duplicate = -1
    stream: BinaryIO | None = None
    result: _StreamResult | None = None
    errors: list[BaseException] = []
    try:
        duplicate = os.dup(descriptor)
        os.lseek(duplicate, 0, os.SEEK_SET)
        stream = os.fdopen(duplicate, "rb")
        duplicate = -1
        result = operation(stream)
    except BaseException as exc:
        errors.append(exc)
    if stream is not None:
        try:
            stream.close()
        except BaseException as exc:
            errors.append(exc)
    elif duplicate >= 0:
        try:
            os.close(duplicate)
        except BaseException as exc:
            errors.append(exc)
    if errors:
        if len(errors) == 1:
            raise errors[0]
        raise BaseExceptionGroup(f"{label} and stream cleanup failed", errors)
    return result  # type: ignore[return-value]


class LabSealedShardBundleReader:
    """Load one exact accepted worker attempt without trusting mutable paths."""

    def __init__(
        self,
        artifact_root: Path,
        *,
        limits: LabShardBundleLimits | None = None,
        max_peak_resident_bytes: int | None = None,
        result_digest_policy: LabResultDigestPolicy | None = None,
    ) -> None:
        self.artifact_root = Path(artifact_root).resolve()
        self.limits = limits or LabShardBundleLimits()
        if max_peak_resident_bytes is not None and max_peak_resident_bytes < 1:
            raise ValueError("bundle reader peak resident byte limit must be positive")
        self.max_peak_resident_bytes = min(
            self.limits.max_manifest_peak_bytes,
            max_peak_resident_bytes or self.limits.max_manifest_peak_bytes,
        )
        self.result_digest_policy = LabResultDigestPolicy.model_validate(
            result_digest_policy or LabResultDigestPolicy()
        )

    def _preflight_manifest_size(self, size: int, *, resident_bytes: int) -> None:
        estimated_peak = (
            resident_bytes
            + size
            + (size * self.limits.manifest_model_expansion_factor)
            + CANONICAL_JSON_STREAM_SCRATCH_BYTES
        )
        if estimated_peak > self.max_peak_resident_bytes:
            raise LabFinalizationResourceLimitError(
                "accepted shard manifest parsing peak exceeds configured memory limit"
            )

    @staticmethod
    def _after_file_read(_name: str) -> None:
        """Fault hook; every pathname is rebound to its read inode before return."""

    @staticmethod
    def _attempt_name(evidence: LabFinalizationShardEvidence) -> str:
        report = evidence.accepted_success.report
        return (
            f"{report.scheduler_fencing_token:020d}-"
            f"{report.claim_generation:020d}-{report.claim_token}"
        )

    @staticmethod
    def _open_directory(parent_descriptor: int, name: str) -> tuple[int, tuple[int, ...]]:
        if not name or name in {".", ".."} or "/" in name or "\x00" in name:
            raise LabFinalizationIntegrityError("unsafe shard artifact path segment")
        descriptor = -1
        errors: list[BaseException] = []
        try:
            before = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
            if not stat.S_ISDIR(before.st_mode):
                raise LabFinalizationIntegrityError("shard artifact ancestor is not a directory")
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_descriptor,
            )
            opened = os.fstat(descriptor)
            if _observation(before) != _observation(opened):
                raise LabFinalizationIntegrityError("shard artifact ancestor changed while opening")
            return descriptor, _observation(opened)
        except LabFinalizationIntegrityError as exc:
            errors.append(exc)
        except OSError as exc:
            error = LabFinalizationIntegrityError("accepted shard artifact path is unavailable")
            error.__cause__ = exc
            errors.append(error)
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except BaseException as exc:
                errors.append(exc)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup(
                "shard artifact path validation and cleanup failed",
                errors,
            )
        raise AssertionError("directory validation failed without an error")

    @staticmethod
    def _open_regular_file(
        bundle_descriptor: int,
        name: str,
        *,
        max_bytes: int,
        expected_size: int | None = None,
    ) -> tuple[int, tuple[int, int, int, int, int, int, int]]:
        if not name or name in {".", ".."} or "/" in name or "\x00" in name:
            raise LabFinalizationIntegrityError("unsafe shard artifact file name")
        descriptor = -1
        errors: list[BaseException] = []
        try:
            before = os.stat(name, dir_fd=bundle_descriptor, follow_symlinks=False)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise LabFinalizationIntegrityError("shard artifact is not a private regular file")
            if before.st_size > max_bytes:
                raise LabFinalizationIntegrityError(
                    "accepted shard file exceeds configured single file byte limit"
                )
            if expected_size is not None and before.st_size != expected_size:
                raise LabFinalizationIntegrityError(
                    "accepted shard artifact size conflicts with manifest"
                )
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=bundle_descriptor,
            )
            opened = os.fstat(descriptor)
            if _observation(before) != _observation(opened):
                raise LabFinalizationIntegrityError("shard artifact changed while opening")
            return descriptor, _observation(opened)
        except LabFinalizationIntegrityError as exc:
            errors.append(exc)
        except OSError as exc:
            error = LabFinalizationIntegrityError("accepted shard artifact file is unavailable")
            error.__cause__ = exc
            errors.append(error)
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except BaseException as exc:
                errors.append(exc)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup(
                "shard artifact file validation and cleanup failed",
                errors,
            )
        raise AssertionError("file validation failed without an error")

    def _validate_manifest_resources(
        self,
        manifest: LabShardResultManifest,
        *,
        manifest_size: int,
    ) -> None:
        if len(manifest.artifacts) > self.limits.max_artifact_count:
            raise LabFinalizationIntegrityError(
                "accepted shard artifact count exceeds configured limit"
            )
        total_size = manifest_size
        total_rows = 0
        for artifact in manifest.artifacts:
            if artifact.file_size > self.limits.max_single_file_bytes:
                raise LabFinalizationIntegrityError(
                    "accepted shard single file exceeds configured byte limit"
                )
            total_size += artifact.file_size
            if total_size > self.limits.max_bundle_total_bytes:
                raise LabFinalizationIntegrityError(
                    "accepted shard bundle exceeds configured total byte limit"
                )
            total_rows += artifact.row_count
            if total_rows > self.limits.max_row_count:
                raise LabFinalizationIntegrityError(
                    "accepted shard row count exceeds configured limit"
                )
            if len(artifact.columns) > self.limits.max_column_count:
                raise LabFinalizationIntegrityError(
                    "accepted shard column count exceeds configured limit"
                )

    def _parquet_resource_summary(self, descriptor: int) -> LabParquetResourceSummary:
        def inspect(stream: BinaryIO) -> LabParquetResourceSummary:
            parquet = pq.ParquetFile(stream)
            metadata = parquet.metadata
            declared_uncompressed_bytes = 0
            for row_group_index in range(metadata.num_row_groups):
                row_group = metadata.row_group(row_group_index)
                for column_index in range(row_group.num_columns):
                    declared = row_group.column(column_index).total_uncompressed_size
                    if declared is None or declared < 0:
                        raise LabFinalizationIntegrityError(
                            "accepted shard Parquet has invalid uncompressed size metadata"
                        )
                    declared_uncompressed_bytes += declared
                    if declared_uncompressed_bytes > self.limits.max_parquet_uncompressed_bytes:
                        raise LabFinalizationIntegrityError(
                            "accepted shard Parquet uncompressed size exceeds configured limit"
                        )
            schema = parquet.schema_arrow
            estimated_arrow_bytes = declared_uncompressed_bytes + (
                metadata.num_rows * max(metadata.num_columns, 1) * 8
            )
            estimated_pandas_bytes = estimated_arrow_bytes + self._pandas_schema_overhead(
                schema,
                metadata.num_rows,
            )
            if estimated_arrow_bytes > self.limits.max_arrow_table_bytes:
                raise LabFinalizationResourceLimitError(
                    "accepted shard estimated Arrow table exceeds configured memory limit"
                )
            if estimated_pandas_bytes > self.limits.max_materialized_dataframe_bytes:
                raise LabFinalizationResourceLimitError(
                    "accepted shard estimated pandas DataFrame exceeds configured memory limit"
                )
            return LabParquetResourceSummary(
                row_count=metadata.num_rows,
                column_count=metadata.num_columns,
                columns=tuple(schema.names),
                declared_uncompressed_bytes=declared_uncompressed_bytes,
                estimated_arrow_bytes=estimated_arrow_bytes,
                estimated_pandas_bytes=estimated_pandas_bytes,
            )

        try:
            return _run_with_descriptor_stream(
                descriptor,
                inspect,
                label="Parquet metadata inspection",
            )
        except (LabFinalizationIntegrityError, BaseExceptionGroup):
            raise
        except Exception as exc:
            raise LabFinalizationIntegrityError(
                "accepted shard Parquet metadata is invalid"
            ) from exc

    @staticmethod
    def _before_arrow_to_pandas(_table: pa.Table) -> None:
        """Fault hook after bounded Arrow loading and before pandas allocation."""

    @staticmethod
    def _pandas_schema_overhead(schema: pa.Schema, row_count: int) -> int:
        estimate = max(row_count, 1) * 8
        for field in schema:
            data_type = field.type
            if isinstance(data_type, pa.ExtensionType):
                raise LabFinalizationIntegrityError(
                    "accepted shard Parquet uses an unsupported extension type"
                )
            supported = (
                pa.types.is_null(data_type)
                or pa.types.is_boolean(data_type)
                or pa.types.is_integer(data_type)
                or pa.types.is_floating(data_type)
                or pa.types.is_decimal(data_type)
                or pa.types.is_date(data_type)
                or pa.types.is_time(data_type)
                or pa.types.is_timestamp(data_type)
                or pa.types.is_duration(data_type)
                or pa.types.is_string(data_type)
                or pa.types.is_large_string(data_type)
                or pa.types.is_binary(data_type)
                or pa.types.is_large_binary(data_type)
                or pa.types.is_fixed_size_binary(data_type)
                or pa.types.is_dictionary(data_type)
            )
            if not supported:
                raise LabFinalizationIntegrityError(
                    "accepted shard Parquet uses an unsupported nested or logical type"
                )
            object_backed = (
                pa.types.is_null(data_type)
                or pa.types.is_boolean(data_type)
                or pa.types.is_decimal(data_type)
                or pa.types.is_date(data_type)
                or pa.types.is_time(data_type)
                or pa.types.is_string(data_type)
                or pa.types.is_large_string(data_type)
                or pa.types.is_binary(data_type)
                or pa.types.is_large_binary(data_type)
                or pa.types.is_fixed_size_binary(data_type)
                or pa.types.is_dictionary(data_type)
            )
            if object_backed:
                estimate += row_count * 80
        return estimate

    @classmethod
    def _estimated_pandas_bytes(cls, table: pa.Table) -> int:
        return int(table.nbytes) + cls._pandas_schema_overhead(
            table.schema,
            table.num_rows,
        )

    def _read_parquet(self, descriptor: int) -> _ParquetMaterialization:
        def materialize(stream: BinaryIO) -> _ParquetMaterialization:
            table = pq.ParquetFile(stream).read()
            arrow_bytes = int(table.nbytes)
            if arrow_bytes > self.limits.max_arrow_table_bytes:
                raise LabFinalizationResourceLimitError(
                    "accepted shard Arrow table exceeds configured memory limit"
                )
            estimated_pandas_bytes = self._estimated_pandas_bytes(table)
            if estimated_pandas_bytes > self.limits.max_materialized_dataframe_bytes:
                raise LabFinalizationResourceLimitError(
                    "accepted shard estimated pandas DataFrame exceeds configured memory limit"
                )
            self._before_arrow_to_pandas(table)
            frame = table.to_pandas()
            if not isinstance(frame, pd.DataFrame):
                raise LabFinalizationIntegrityError(
                    "accepted shard Parquet did not materialize as a DataFrame"
                )
            materialized_bytes = int(frame.memory_usage(index=True, deep=True).sum())
            if materialized_bytes > self.limits.max_materialized_dataframe_bytes:
                raise LabFinalizationResourceLimitError(
                    "accepted shard materialized DataFrame exceeds configured memory limit"
                )
            return _ParquetMaterialization(
                frame=frame,
                arrow_bytes=arrow_bytes,
                materialized_dataframe_bytes=materialized_bytes,
            )

        try:
            return _run_with_descriptor_stream(
                descriptor,
                materialize,
                label="Parquet materialization",
            )
        except (LabFinalizationIntegrityError, BaseExceptionGroup):
            raise
        except Exception as exc:
            raise LabFinalizationIntegrityError("accepted shard Parquet is invalid") from exc

    @staticmethod
    def _assert_file_binding(
        bundle_descriptor: int,
        name: str,
        descriptor: int,
        observed: tuple[int, int, int, int, int, int, int],
    ) -> None:
        try:
            linked = os.stat(name, dir_fd=bundle_descriptor, follow_symlinks=False)
            opened = os.fstat(descriptor)
        except OSError as exc:
            raise LabFinalizationIntegrityError(
                "accepted shard file identity changed while reading"
            ) from exc
        if _observation(linked) != observed or _observation(opened) != observed:
            raise LabFinalizationIntegrityError(
                "accepted shard file identity changed while reading"
            )

    def _read_bound_bundle(
        self,
        evidence: LabFinalizationShardEvidence,
        *,
        descriptors: list[int],
        observe_usage: Callable[[LabShardBundleUsage], None] | None,
        materialize: bool,
        expected_inspection: LabShardBundleInspection | None,
        resident_bytes: int,
        expected_job_code_sha: str | None,
    ) -> LabShardExecutionResult | LabShardBundleInspection:
        report = evidence.accepted_success.report
        body = report.body
        if not isinstance(body, LabShardSucceeded):
            raise LabFinalizationIntegrityError("accepted attempt is not shard_succeeded")
        job_code_sha = expected_job_code_sha or body.worker_code_sha
        if job_code_sha is None:
            raise LabFinalizationIntegrityError(
                "legacy accepted attempt requires explicit job code provenance"
            )
        try:
            digest_provenance = resolve_success_digest_provenance(
                expected_job_code_sha=job_code_sha,
                result_manifest_schema_version=body.result_manifest_schema_version,
                content_digest_algorithm=body.content_digest_algorithm,
                worker_code_sha=body.worker_code_sha,
                policy=self.result_digest_policy,
            )
        except LabResultDigestProvenanceError as exc:
            raise LabFinalizationIntegrityError(
                "accepted shard digest provenance is not authorized"
            ) from exc
        segments = (
            "jobs",
            str(report.job_id),
            "shards",
            str(report.shard_id),
            "attempts",
            self._attempt_name(evidence),
        )
        bindings: list[_PathBinding] = []
        file_bindings: dict[
            str,
            tuple[int, tuple[int, int, int, int, int, int, int]],
        ] = {}
        try:
            try:
                root_descriptor = os.open(
                    self.artifact_root,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                )
            except OSError as exc:
                raise LabFinalizationIntegrityError("shard artifact root is unavailable") from exc
            descriptors.append(root_descriptor)
            root_observation = _observation(os.fstat(root_descriptor))
            parent = root_descriptor
            for segment in segments:
                child, observed = self._open_directory(parent, segment)
                descriptors.append(child)
                bindings.append(
                    _PathBinding(
                        parent_descriptor=parent,
                        name=segment,
                        descriptor=child,
                        observation=observed,
                    )
                )
                parent = child
            bundle_descriptor = descriptors[-1]
            manifest_descriptor, manifest_observed = self._open_regular_file(
                bundle_descriptor,
                "manifest.json",
                max_bytes=self.limits.max_manifest_bytes,
            )
            descriptors.append(manifest_descriptor)
            file_bindings["manifest.json"] = (
                manifest_descriptor,
                manifest_observed,
            )
            self._preflight_manifest_size(
                manifest_observed[4],
                resident_bytes=resident_bytes,
            )
            manifest_bytes = _read_descriptor_bounded(
                manifest_descriptor,
                expected_size=manifest_observed[4],
                max_bytes=self.limits.max_manifest_bytes,
                max_consecutive_interrupted_reads=(self.limits.max_consecutive_interrupted_reads),
            )
            self._after_file_read("manifest.json")
            try:
                manifest_usage = _preflight_manifest_metrics(
                    manifest_bytes,
                    limits=self.limits,
                )
            except LabFinalizationResourceLimitError:
                raise
            except LabFinalizationIntegrityError as exc:
                raise LabFinalizationIntegrityError("accepted shard manifest is invalid") from exc
            if resident_bytes + manifest_usage.peak_resident_bytes > self.max_peak_resident_bytes:
                raise LabFinalizationResourceLimitError(
                    "accepted shard manifest parsing peak exceeds configured memory limit"
                )
            try:
                manifest = strict_model_validate_canonical_json(
                    LabShardResultManifest, manifest_bytes
                )
            except Exception as exc:
                raise LabFinalizationIntegrityError("accepted shard manifest is invalid") from exc
            if manifest_bytes != manifest.canonical_json().encode("utf-8"):
                raise LabFinalizationIntegrityError("accepted shard manifest is not canonical JSON")
            try:
                require_matching_manifest_digest_provenance(
                    digest_provenance,
                    manifest_schema_version=manifest.schema_version,
                    content_digest_algorithm=manifest.content_digest_algorithm,
                    worker_code_sha=manifest.worker_code_sha,
                )
            except LabResultDigestProvenanceError as exc:
                raise LabFinalizationIntegrityError(
                    "accepted shard manifest digest provenance conflicts"
                ) from exc
            self._validate_manifest_resources(
                manifest,
                manifest_size=len(manifest_bytes),
            )
            if len(manifest.metrics) != manifest_usage.metric_count:
                raise LabFinalizationIntegrityError(
                    "accepted shard manifest metric preflight changed during parsing"
                )
            expected_manifest_identity = (
                report.job_id,
                report.shard_id,
                report.claim_token,
                report.claim_generation,
                report.scheduler_fencing_token,
                report.spec_hash,
                report.payload_hash,
                evidence.shard.plan_hash,
                evidence.shard.adapter_id,
                evidence.shard.adapter_version,
            )
            actual_manifest_identity = (
                manifest.job_id,
                manifest.shard_id,
                manifest.claim_token,
                manifest.claim_generation,
                manifest.scheduler_fencing_token,
                manifest.spec_hash,
                manifest.payload_hash,
                manifest.plan_hash,
                manifest.adapter_id,
                manifest.adapter_version,
            )
            if actual_manifest_identity != expected_manifest_identity:
                raise LabFinalizationIntegrityError(
                    "accepted shard manifest conflicts with ledger attempt identity"
                )
            if manifest.manifest_hash != body.result_manifest_hash:
                raise LabFinalizationIntegrityError(
                    "accepted shard manifest hash conflicts with success evidence"
                )
            expected_names = {"manifest.json"} | {
                artifact.file_name for artifact in manifest.artifacts
            }
            try:
                actual_names = set(os.listdir(bundle_descriptor))
            except OSError as exc:
                raise LabFinalizationIntegrityError(
                    "accepted shard inventory is unavailable"
                ) from exc
            if actual_names != expected_names:
                raise LabFinalizationIntegrityError("accepted shard bundle inventory conflicts")

            tables: list[LabShardTable] = []
            total_uncompressed_bytes = 0
            total_estimated_arrow_bytes = 0
            total_estimated_pandas_bytes = 0
            total_arrow_bytes = 0
            total_materialized_bytes = 0
            for index, artifact in enumerate(manifest.artifacts):
                if artifact.file_name != f"{index:03d}-{artifact.name}.parquet":
                    raise LabFinalizationIntegrityError("accepted shard artifact order is invalid")
                descriptor, observed = self._open_regular_file(
                    bundle_descriptor,
                    artifact.file_name,
                    max_bytes=self.limits.max_single_file_bytes,
                    expected_size=artifact.file_size,
                )
                descriptors.append(descriptor)
                file_bindings[artifact.file_name] = (descriptor, observed)
                if (
                    _sha256_descriptor_bounded(
                        descriptor,
                        expected_size=artifact.file_size,
                        max_consecutive_interrupted_reads=(
                            self.limits.max_consecutive_interrupted_reads
                        ),
                    )
                    != artifact.file_sha256
                ):
                    raise LabFinalizationIntegrityError("accepted shard artifact bytes conflict")
                self._after_file_read(artifact.file_name)
                parquet_summary = self._parquet_resource_summary(descriptor)
                total_uncompressed_bytes += parquet_summary.declared_uncompressed_bytes
                total_estimated_arrow_bytes += parquet_summary.estimated_arrow_bytes
                total_estimated_pandas_bytes += parquet_summary.estimated_pandas_bytes
                if total_uncompressed_bytes > self.limits.max_parquet_uncompressed_bytes:
                    raise LabFinalizationIntegrityError(
                        "accepted shard Parquet uncompressed size exceeds configured limit"
                    )
                if (
                    parquet_summary.row_count != artifact.row_count
                    or parquet_summary.column_count != len(artifact.columns)
                    or parquet_summary.columns != artifact.columns
                ):
                    raise LabFinalizationIntegrityError("accepted shard Parquet shape conflicts")
                if materialize:
                    materialization = self._read_parquet(descriptor)
                    frame = materialization.frame
                    total_arrow_bytes += materialization.arrow_bytes
                    if total_arrow_bytes > self.limits.max_arrow_table_bytes:
                        raise LabFinalizationResourceLimitError(
                            "accepted shard Arrow tables exceed configured memory limit"
                        )
                    total_materialized_bytes += materialization.materialized_dataframe_bytes
                    if total_materialized_bytes > self.limits.max_materialized_dataframe_bytes:
                        raise LabFinalizationResourceLimitError(
                            "accepted shard materialized DataFrame exceeds configured memory limit"
                        )
                    if len(frame) != artifact.row_count or tuple(frame.columns) != artifact.columns:
                        raise LabFinalizationIntegrityError(
                            "accepted shard Parquet shape conflicts"
                        )
                    content_hash = canonical_shard_frame_digest(frame)
                    if content_hash != artifact.content_sha256 and not digest_provenance.legacy:
                        raise LabFinalizationIntegrityError(
                            "accepted shard Parquet content conflicts"
                        )
                    tables.append(LabShardTable(name=artifact.name, frame=frame))

            inspection = LabShardBundleInspection(
                manifest_hash=manifest.manifest_hash,
                row_count=sum(artifact.row_count for artifact in manifest.artifacts),
                compressed_bytes=len(manifest_bytes)
                + sum(artifact.file_size for artifact in manifest.artifacts),
                declared_uncompressed_bytes=total_uncompressed_bytes,
                estimated_arrow_bytes=total_estimated_arrow_bytes,
                estimated_pandas_bytes=total_estimated_pandas_bytes,
                manifest_usage=manifest_usage,
            )
            if expected_inspection is not None and inspection != expected_inspection:
                raise LabFinalizationIntegrityError(
                    "accepted shard changed after resource preflight"
                )

            if materialize and observe_usage is not None:
                observe_usage(
                    LabShardBundleUsage(
                        row_count=sum(artifact.row_count for artifact in manifest.artifacts),
                        compressed_bytes=len(manifest_bytes)
                        + sum(artifact.file_size for artifact in manifest.artifacts),
                        declared_uncompressed_bytes=total_uncompressed_bytes,
                        arrow_bytes=total_arrow_bytes,
                        materialized_dataframe_bytes=total_materialized_bytes,
                        manifest_peak_bytes=manifest_usage.peak_resident_bytes,
                        retained_metric_bytes=manifest_usage.retained_metric_bytes,
                    )
                )

            if set(os.listdir(bundle_descriptor)) != expected_names:
                raise LabFinalizationIntegrityError(
                    "accepted shard inventory changed while reading"
                )
            for name, (descriptor, observed) in sorted(file_bindings.items()):
                self._assert_file_binding(
                    bundle_descriptor,
                    name,
                    descriptor,
                    observed,
                )
            root_path = os.lstat(self.artifact_root)
            if _observation(root_path) != root_observation:
                raise LabFinalizationIntegrityError("shard artifact root changed while reading")
            for binding in bindings:
                linked = os.stat(
                    binding.name,
                    dir_fd=binding.parent_descriptor,
                    follow_symlinks=False,
                )
                if (
                    _observation(linked) != binding.observation
                    or _observation(os.fstat(binding.descriptor)) != binding.observation
                ):
                    raise LabFinalizationIntegrityError("shard artifact path changed while reading")
            if not materialize:
                return inspection
            return LabShardExecutionResult(
                shard_id=manifest.shard_id,
                spec_hash=manifest.spec_hash,
                payload_hash=manifest.payload_hash,
                plan_hash=manifest.plan_hash,
                adapter_id=manifest.adapter_id,
                adapter_version=manifest.adapter_version,
                tables=tuple(tables),
                metrics=manifest.metrics,
            )
        except (LabFinalizationIntegrityError, BaseExceptionGroup):
            raise
        except Exception as exc:
            raise LabFinalizationIntegrityError(
                "accepted shard bundle could not be safely reconstructed"
            ) from exc

    def read(
        self,
        evidence: LabFinalizationShardEvidence,
        *,
        observe_usage: Callable[[LabShardBundleUsage], None] | None = None,
        expected_inspection: LabShardBundleInspection | None = None,
        resident_bytes: int = 0,
        expected_job_code_sha: str | None = None,
    ) -> LabShardExecutionResult:
        descriptors: list[int] = []
        result: LabShardExecutionResult | None = None
        errors: list[BaseException] = []
        try:
            result = self._read_bound_bundle(
                evidence,
                descriptors=descriptors,
                observe_usage=observe_usage,
                materialize=True,
                expected_inspection=expected_inspection,
                resident_bytes=resident_bytes,
                expected_job_code_sha=expected_job_code_sha,
            )
        except BaseException as exc:
            errors.append(exc)
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except BaseException as exc:
                errors.append(exc)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup(
                "accepted shard bundle validation and cleanup failed",
                errors,
            )
        if result is None:
            raise AssertionError("bundle validation completed without a result")
        if not isinstance(result, LabShardExecutionResult):
            raise AssertionError("bundle materialization returned an inspection")
        return result

    def inspect(
        self,
        evidence: LabFinalizationShardEvidence,
        *,
        resident_bytes: int = 0,
        expected_job_code_sha: str | None = None,
    ) -> LabShardBundleInspection:
        descriptors: list[int] = []
        result: LabShardExecutionResult | LabShardBundleInspection | None = None
        errors: list[BaseException] = []
        try:
            result = self._read_bound_bundle(
                evidence,
                descriptors=descriptors,
                observe_usage=None,
                materialize=False,
                expected_inspection=None,
                resident_bytes=resident_bytes,
                expected_job_code_sha=expected_job_code_sha,
            )
        except BaseException as exc:
            errors.append(exc)
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except BaseException as exc:
                errors.append(exc)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup(
                "accepted shard inspection and cleanup failed",
                errors,
            )
        if not isinstance(result, LabShardBundleInspection):
            raise AssertionError("bundle inspection did not return resource evidence")
        return result


class LabFinalizer:
    """Finalize one ready job without ever opening a writable SQLite connection."""

    def __init__(
        self,
        *,
        reader: LabJobReader,
        shard_artifact_root: Path,
        artifact_store: LabJobArtifactStore,
        commit_spool: LabArtifactCommitSpool,
        verified_code_sha_provider: Callable[[], str | None],
        finalizer_authority_key_provider: LabFinalizerAuthoritySigningKeyProvider,
        finalizer_authority_verification_key_provider: (
            LabFinalizerAuthorityVerificationKeyProvider | None
        ) = None,
        adapter_registry: StrategyJobAdapterRegistry | None = None,
        bundle_limits: LabShardBundleLimits | None = None,
        job_limits: LabFinalizerJobLimits | None = None,
        result_digest_policy: LabResultDigestPolicy | None = None,
    ) -> None:
        self.reader = reader
        self.job_limits = job_limits or LabFinalizerJobLimits()
        self.result_digest_policy = LabResultDigestPolicy.model_validate(
            result_digest_policy or LabResultDigestPolicy()
        )
        self.bundle_reader = LabSealedShardBundleReader(
            shard_artifact_root,
            limits=bundle_limits,
            max_peak_resident_bytes=self.job_limits.max_peak_resident_bytes,
            result_digest_policy=self.result_digest_policy,
        )
        self.artifact_store = artifact_store
        self.commit_spool = commit_spool
        self.verified_code_sha_provider = verified_code_sha_provider
        self.finalizer_authority_key_provider = finalizer_authority_key_provider
        if finalizer_authority_verification_key_provider is None:

            def verify_active_key(key_id: str) -> LabFinalizerAuthorityKey | None:
                key = self.finalizer_authority_key_provider()
                return (
                    key
                    if isinstance(key, LabFinalizerAuthorityKey) and key.key_id == key_id
                    else None
                )

            self.finalizer_authority_verification_key_provider = verify_active_key
        else:
            self.finalizer_authority_verification_key_provider = (
                finalizer_authority_verification_key_provider
            )
        self.adapter_registry = adapter_registry or default_strategy_job_adapter_registry()

    def _verified_runtime_code_sha(self, *, expected_sha: str | None = None) -> str:
        try:
            runtime_code_sha = self.verified_code_sha_provider()
        except LabDaemonConfigurationError:
            raise
        except Exception as exc:
            raise LabFinalizationCodeProviderError("verified code SHA provider failed") from exc
        if (
            not isinstance(runtime_code_sha, str)
            or re.fullmatch(r"[0-9a-f]{40}", runtime_code_sha) is None
        ):
            raise LabFinalizationCodeProviderError(
                "verified code SHA provider returned an invalid commit"
            )
        if expected_sha is not None and runtime_code_sha != expected_sha:
            raise LabFinalizationCodeMismatchError(
                expected=expected_sha,
                actual=runtime_code_sha,
            )
        return runtime_code_sha

    @staticmethod
    def _after_candidate_prepared(_candidate: LabJobArtifactCandidate) -> None:
        """Fault-injection boundary after durable candidate publication."""

    @staticmethod
    def _after_artifact_sealed(_sealed: LabSealedJobArtifact) -> None:
        """Fault-injection boundary after immutable job artifact sealing."""

    @staticmethod
    def _after_commit_published(
        _published: LabArtifactCommitSpoolEntry | LabAcknowledgedArtifactCommit,
    ) -> None:
        """Fault-injection boundary after durable commit-spool publication."""

    def _metrics(
        self,
        snapshot: LabFinalizationSnapshot,
        result: LabJobExecutionResult,
        shard_results: tuple[LabShardExecutionResult, ...],
        *,
        finalizer_code_sha: str,
    ) -> LabFinalizerMetrics:
        first = snapshot.shards[0].shard
        return LabFinalizerMetrics(
            job_id=snapshot.job.job_id,
            spec_hash=snapshot.job.spec_hash,
            plan_hash=first.plan_hash,
            adapter_id=first.adapter_id,
            adapter_version=first.adapter_version,
            result_contract_version=COMPLETE_RESULT_CONTRACT_VERSION,
            finalizer_code_sha=finalizer_code_sha,
            result_hash=result.result_hash,
            shard_count=len(snapshot.shards),
            shards=tuple(
                LabFinalizerShardSummary(
                    shard_index=evidence.shard.shard_index,
                    shard_id=evidence.shard.shard_id,
                    result_manifest_hash=evidence.shard.result_manifest_hash or "",
                    metrics=shard_result.metrics,
                )
                for evidence, shard_result in zip(
                    snapshot.shards,
                    shard_results,
                    strict=True,
                )
            ),
            tables=tuple(
                LabFinalizerTableSummary(
                    name=table.name,
                    row_count=len(table.frame),
                    columns=tuple(table.frame.columns),
                )
                for table in result.tables
            ),
        )

    @staticmethod
    def _report(metrics: LabFinalizerMetrics) -> str:
        def canonical_json(value: object) -> str:
            return canonical_json_bytes(value).decode("utf-8")

        summary = canonical_json(
            {
                "adapter_id": metrics.adapter_id,
                "adapter_version": metrics.adapter_version,
                "finalizer_code_sha": metrics.finalizer_code_sha,
                "job_id": str(metrics.job_id),
                "plan_hash": metrics.plan_hash,
                "result_contract_version": metrics.result_contract_version,
                "result_hash": metrics.result_hash,
                "shard_count": metrics.shard_count,
                "spec_hash": metrics.spec_hash,
            }
        )
        tables = canonical_json([table.model_dump(mode="json") for table in metrics.tables])
        shards = canonical_json([shard.model_dump(mode="json") for shard in metrics.shards])
        lines = [
            "# Strategy Lab Complete Result",
            "",
            "## Summary",
            "",
            f"    {summary}",
            "",
            "## Tables",
            "",
            f"    {tables}",
            "",
            "## Shards",
            "",
            f"    {shards}",
        ]
        return "\n".join(lines) + "\n"

    @staticmethod
    def _authority(plan: LabJobArtifactPlan) -> LabArtifactRecoveryAuthority:
        manifest = plan.manifest
        return LabArtifactRecoveryAuthority(
            job_id=manifest.job_id,
            spec_hash=manifest.spec_hash,
            plan_hash=manifest.plan_hash,
            adapter_id=manifest.adapter_id,
            adapter_version=manifest.adapter_version,
            result_contract_version=manifest.result_contract_version,
            code_sha=manifest.code_sha,
            dataset_snapshot=manifest.dataset_snapshot,
            expected_manifest_hash=plan.manifest_hash,
        )

    @staticmethod
    def _related_recovery_records(
        plan: LabJobArtifactPlan,
        records: tuple[LabArtifactRecoveryRecord, ...],
    ) -> tuple[LabArtifactRecoveryRecord, ...]:
        prefix = f"{plan.job_id.hex}-"
        related = tuple(
            record
            for record in records
            if record.status != "quarantined"
            and (record.job_id == plan.job_id or record.path.name.startswith(prefix))
        )
        for record in related:
            if record.status in {"recoverable", "needs_authority", "recoverable_torn"} and (
                record.job_id != plan.job_id or record.manifest_hash != plan.manifest_hash
            ):
                raise LabFinalizationIntegrityError(
                    "job candidate recovery conflicts with current aggregate result"
                )
        priority = {"recoverable_torn": 0, "recoverable": 1, "needs_authority": 2}
        return tuple(
            sorted(
                related,
                key=lambda record: (
                    priority.get(record.status, 3),
                    record.path.name,
                ),
            )
        )

    def _verify_or_recover_sealed(self, plan: LabJobArtifactPlan) -> LabSealedJobArtifact | None:
        target = self.artifact_store.sealed_root / plan.job_id.hex
        if not os.path.lexists(target):
            return None
        try:
            self._verified_runtime_code_sha(expected_sha=plan.manifest.code_sha)
            sealed = self.artifact_store.recover_interrupted_seal(target)
        except LabArtifactError as interrupted_error:
            try:
                sealed = self.artifact_store.verify_sealed(target)
            except LabArtifactError as verify_error:
                raise LabFinalizationIntegrityError(
                    "existing sealed job artifact is neither complete nor recoverable"
                ) from ExceptionGroup(
                    "sealed artifact recovery and verification failed",
                    [interrupted_error, verify_error],
                )
        if sealed.manifest != plan.manifest or sealed.manifest_hash != plan.manifest_hash:
            raise LabFinalizationIntegrityError(
                "existing sealed artifact conflicts with deterministic finalization output"
            )
        self._cleanup_redundant_candidates(sealed)
        return sealed

    def _cleanup_redundant_candidates(self, sealed: LabSealedJobArtifact) -> None:
        try:
            records = self.artifact_store.list_candidate_recovery()
        except LabArtifactError as exc:
            raise LabFinalizationIntegrityError(
                "redundant matching candidates could not be inspected"
            ) from exc
        matching = tuple(
            record
            for record in records
            if record.status in {"recoverable", "needs_authority", "recoverable_torn"}
            and record.job_id == sealed.manifest.job_id
            and record.manifest_hash == sealed.manifest_hash
        )
        for record in matching:
            try:
                self._verified_runtime_code_sha(expected_sha=sealed.manifest.code_sha)
                self.artifact_store.quarantine_recovery_record(
                    record,
                    reason="redundant deterministic candidate after sealed publication",
                )
            except LabArtifactError as exc:
                raise LabFinalizationIntegrityError(
                    "redundant matching candidate could not be safely isolated"
                ) from exc

    def _recover_candidate_from_plan(
        self,
        plan: LabJobArtifactPlan,
    ) -> LabSealedJobArtifact | None:
        authority = self._authority(plan)
        records = self._related_recovery_records(
            plan,
            self.artifact_store.list_candidate_recovery(),
        )

        if any(record.status == "invalid" for record in records):
            raise LabFinalizationIntegrityError(
                "job candidate recovery contains invalid filesystem evidence"
            )
        recoverable = tuple(
            record
            for record in records
            if record.status in {"recoverable", "needs_authority", "recoverable_torn"}
        )
        if recoverable:
            primary, *redundant = recoverable
            try:
                self._verified_runtime_code_sha(expected_sha=plan.manifest.code_sha)
                sealed = self.artifact_store.recover_candidate(
                    primary,
                    authority=authority,
                )
            except LabArtifactError as exc:
                raise LabFinalizationIntegrityError(
                    "job artifact could not be sealed from retained candidate"
                ) from exc
            for record in redundant:
                try:
                    self._verified_runtime_code_sha(expected_sha=plan.manifest.code_sha)
                    self.artifact_store.quarantine_recovery_record(
                        record,
                        reason="redundant deterministic candidate after recovery",
                    )
                except LabArtifactError as exc:
                    raise LabFinalizationIntegrityError(
                        "redundant matching candidate could not be safely isolated"
                    ) from exc
            if sealed.manifest != plan.manifest or sealed.manifest_hash != plan.manifest_hash:
                raise LabFinalizationIntegrityError(
                    "recovered job artifact conflicts with deterministic finalization output"
                )
            return sealed
        return None

    def _prepare_and_seal(self, plan: LabJobArtifactPlan) -> LabSealedJobArtifact:
        try:
            self._verified_runtime_code_sha(expected_sha=plan.manifest.code_sha)
            candidate = self.artifact_store.prepare_candidate_from_plan(plan)
        except LabArtifactError as exc:
            raise LabFinalizationIntegrityError(
                "complete result candidate could not be prepared"
            ) from exc
        self._after_candidate_prepared(candidate)
        try:
            self._verified_runtime_code_sha(expected_sha=plan.manifest.code_sha)
            sealed = self.artifact_store.seal_candidate(candidate)
        except LabArtifactError as exc:
            # The verified candidate is durable retry state; moving it would create
            # one full quarantine copy per persistent seal failure.
            raise LabFinalizationIntegrityError("job artifact could not be sealed") from exc
        if sealed.manifest != plan.manifest or sealed.manifest_hash != plan.manifest_hash:
            raise LabFinalizationIntegrityError(
                "sealed job artifact conflicts with deterministic finalization output"
            )
        return sealed

    def _recover_or_prepare(self, plan: LabJobArtifactPlan) -> LabSealedJobArtifact:
        try:
            self._verified_runtime_code_sha(expected_sha=plan.manifest.code_sha)
            with self.artifact_store.finalization_identity_lock(
                job_id=plan.job_id,
                manifest_hash=plan.manifest_hash,
                timeout_seconds=self.job_limits.finalization_lock_timeout_seconds,
                poll_interval_seconds=(self.job_limits.finalization_lock_poll_interval_seconds),
            ):
                sealed = self._verify_or_recover_sealed(plan)
                if sealed is not None:
                    return sealed
                sealed = self._recover_candidate_from_plan(plan)
                if sealed is not None:
                    return sealed
                return self._prepare_and_seal(plan)
        except LabArtifactFinalizationLockTimeoutError as exc:
            raise LabFinalizationCoordinationTimeoutError(
                "finalization decision lock timed out"
            ) from exc
        except LabArtifactFinalizationLockError as exc:
            raise LabFinalizationCoordinationError(
                "finalization decision lock failed integrity validation"
            ) from exc

    def _envelope(
        self,
        sealed: LabSealedJobArtifact,
        snapshot: LabFinalizationSnapshot,
        *,
        finalizer_code_sha: str,
    ) -> LabArtifactCommitEnvelope:
        manifest = sealed.manifest
        commit = LabArtifactCommit(
            job_id=manifest.job_id,
            spec_hash=manifest.spec_hash,
            plan_hash=manifest.plan_hash,
            adapter_id=manifest.adapter_id,
            adapter_version=manifest.adapter_version,
            result_contract_version=manifest.result_contract_version,
            code_sha=manifest.code_sha,
            dataset_snapshot=manifest.dataset_snapshot,
            manifest_hash=sealed.manifest_hash,
            complete_result_hash=manifest.complete_result_hash,
            sealed_path=sealed.path,
        )
        commit_identity = hashlib.sha256(commit.canonical_json_bytes()).hexdigest()
        request_id = uuid5(
            NAMESPACE_URL,
            "rquant:lab-artifact-commit:v2:"
            f"{manifest.job_id}:{snapshot.ready_epoch.job_version}:"
            f"{snapshot.ready_epoch.event.event_id}:{commit_identity}",
        )
        ready_fence = snapshot.ready_epoch.event.scheduler_fencing_token
        if ready_fence is None:
            raise LabFinalizationIntegrityError("ready epoch is missing its scheduler fence")
        claims = LabFinalizerAuthorityClaims(
            request_id=request_id,
            commit_content_hash=commit_identity,
            job_id=manifest.job_id,
            ready_event_id=snapshot.ready_epoch.event.event_id,
            ready_job_version=snapshot.ready_epoch.job_version,
            scheduler_fencing_token=ready_fence,
            spec_hash=snapshot.job.spec_hash,
            finalizer_code_sha=finalizer_code_sha,
            shards=tuple(
                LabFinalizerAuthorityShardEvidence(
                    shard_index=evidence.shard.shard_index,
                    shard_id=evidence.shard.shard_id,
                    payload_hash=evidence.shard.payload_hash,
                    plan_hash=evidence.shard.plan_hash,
                    result_manifest_hash=evidence.shard.result_manifest_hash or "",
                    accepted_report_content_hash=(evidence.accepted_success.report.content_hash),
                    claim_token=evidence.accepted_success.report.claim_token,
                    claim_generation=(evidence.accepted_success.report.claim_generation),
                    scheduler_fencing_token=(
                        evidence.accepted_success.report.scheduler_fencing_token
                    ),
                )
                for evidence in snapshot.shards
            ),
            artifact_manifest_hash=sealed.manifest_hash,
            complete_result_hash=manifest.complete_result_hash,
        )
        proof = sign_finalizer_authority(
            claims,
            key_provider=self.finalizer_authority_key_provider,
        )
        return LabArtifactCommitEnvelope(
            schema_version=2,
            request_id=request_id,
            commit=commit,
            authority_proof=proof,
        )

    def _verified_pending_for_envelope(
        self,
        envelope: LabArtifactCommitEnvelope,
    ) -> LabArtifactCommitSpoolEntry | None:
        durable = self.commit_spool.inspect(envelope.request_id)
        if not isinstance(durable, LabArtifactCommitSpoolEntry):
            return None
        try:
            durable_identity = authenticate_artifact_commit_identity(
                durable.envelope,
                key_provider=self.finalizer_authority_verification_key_provider,
            )
            expected_identity = authenticate_artifact_commit_identity(
                envelope,
                key_provider=self.finalizer_authority_verification_key_provider,
            )
        except LabFinalizerAuthorityAuthenticationError as exc:
            raise LabFinalizationIntegrityError(
                "existing deterministic pending commit is not authenticated"
            ) from exc
        if durable_identity != expected_identity:
            raise LabFinalizationIntegrityError(
                "existing deterministic pending commit conflicts with finalization identity"
            )
        return durable

    def _authenticated_commit_identity(
        self,
        envelope: LabArtifactCommitEnvelope,
        *,
        label: str,
    ) -> LabAuthenticatedArtifactCommitIdentity:
        try:
            return authenticate_artifact_commit_identity(
                envelope,
                key_provider=self.finalizer_authority_verification_key_provider,
            )
        except LabFinalizerAuthorityAuthenticationError as exc:
            raise LabFinalizationIntegrityError(f"{label} is not authenticated") from exc

    @staticmethod
    def _sealed_matches_snapshot(
        sealed: LabSealedJobArtifact,
        snapshot: LabFinalizationSnapshot,
    ) -> bool:
        manifest = sealed.manifest
        first = snapshot.shards[0].shard
        return (
            manifest.job_id,
            manifest.spec_hash,
            manifest.plan_hash,
            manifest.adapter_id,
            manifest.adapter_version,
            manifest.result_contract_version,
            manifest.code_sha,
            manifest.dataset_snapshot,
        ) == (
            snapshot.job.job_id,
            snapshot.job.spec_hash,
            first.plan_hash,
            first.adapter_id,
            first.adapter_version,
            COMPLETE_RESULT_CONTRACT_VERSION,
            snapshot.job.spec.code_sha,
            snapshot.job.spec.dataset_snapshot,
        )

    @staticmethod
    def _result_from_receipt(
        sealed: LabSealedJobArtifact,
        envelope: LabArtifactCommitEnvelope,
        receipt: LabArtifactCommitReceipt,
    ) -> LabFinalizerResult:
        rejected = receipt.status == "rejected"
        return LabFinalizerResult(
            status="rejected" if rejected else "acknowledged",
            job_id=sealed.manifest.job_id,
            request_id=envelope.request_id,
            manifest_hash=sealed.manifest_hash,
            complete_result_hash=sealed.manifest.complete_result_hash,
            rejection_reason=receipt.reason if rejected else None,
        )

    def _validate_acknowledgement(
        self,
        sealed: LabSealedJobArtifact,
        envelope: LabArtifactCommitEnvelope,
        acknowledged: LabAcknowledgedArtifactCommit,
    ) -> LabFinalizerResult:
        ledger = self.reader.get_artifact_commit(envelope.request_id)
        self._verified_runtime_code_sha(expected_sha=sealed.manifest.code_sha)
        if ledger is None:
            raise LabFinalizationIntegrityError(
                "artifact acknowledgement has no authoritative SQLite ledger commit"
            )
        if (
            self._authenticated_commit_identity(
                ledger.envelope,
                label="authoritative SQLite artifact commit",
            )
            != self._authenticated_commit_identity(
                envelope,
                label="replayed artifact commit",
            )
            or ledger.receipt != acknowledged.receipt
        ):
            raise LabFinalizationIntegrityError(
                "artifact acknowledgement conflicts with authoritative SQLite ledger"
            )
        return self._result_from_receipt(sealed, envelope, ledger.receipt)

    def _fast_replay(
        self,
        snapshot: LabFinalizationSnapshot,
        *,
        finalizer_code_sha: str,
    ) -> LabFinalizerResult | None:
        target = self.artifact_store.sealed_root / snapshot.job.job_id.hex
        if not os.path.lexists(target):
            return None
        try:
            sealed = self.artifact_store.verify_sealed(target)
        except LabArtifactError:
            return None
        if not self._sealed_matches_snapshot(sealed, snapshot):
            return None
        envelope = self._envelope(
            sealed,
            snapshot,
            finalizer_code_sha=finalizer_code_sha,
        )
        ledger = self.reader.get_artifact_commit(envelope.request_id)
        self._verified_runtime_code_sha(expected_sha=finalizer_code_sha)
        durable = self.commit_spool.inspect(envelope.request_id)
        self._verified_runtime_code_sha(expected_sha=finalizer_code_sha)
        if ledger is None:
            if isinstance(durable, LabAcknowledgedArtifactCommit):
                raise LabFinalizationIntegrityError(
                    "artifact acknowledgement has no authoritative SQLite ledger commit"
                )
            return None
        if self._authenticated_commit_identity(
            ledger.envelope,
            label="authoritative SQLite artifact commit",
        ) != self._authenticated_commit_identity(
            envelope,
            label="replayed artifact commit",
        ):
            raise LabFinalizationIntegrityError(
                "sealed replay conflicts with authoritative SQLite ledger"
            )
        if isinstance(durable, LabAcknowledgedArtifactCommit):
            result = self._validate_acknowledgement(sealed, envelope, durable)
            self._verified_runtime_code_sha(expected_sha=finalizer_code_sha)
            self._cleanup_redundant_candidates(sealed)
            return result
        if isinstance(
            durable,
            LabArtifactCommitSpoolEntry,
        ) and self._authenticated_commit_identity(
            durable.envelope,
            label="pending artifact commit",
        ) != self._authenticated_commit_identity(
            ledger.envelope,
            label="authoritative SQLite artifact commit",
        ):
            raise LabFinalizationIntegrityError(
                "pending artifact commit conflicts with authoritative SQLite ledger"
            )
        result = self._result_from_receipt(sealed, envelope, ledger.receipt)
        self._verified_runtime_code_sha(expected_sha=finalizer_code_sha)
        self._cleanup_redundant_candidates(sealed)
        return result

    def _isolate_uncommitted_conflicting_replay(
        self,
        snapshot: LabFinalizationSnapshot,
        *,
        finalizer_code_sha: str,
    ) -> None:
        target = self.artifact_store.sealed_root / snapshot.job.job_id.hex
        if not os.path.lexists(target):
            return
        try:
            sealed = self.artifact_store.verify_sealed(target)
        except LabArtifactError:
            return
        if not self._sealed_matches_snapshot(sealed, snapshot):
            return
        envelope = self._envelope(
            sealed,
            snapshot,
            finalizer_code_sha=finalizer_code_sha,
        )
        ledger = self.reader.get_artifact_commit(envelope.request_id)
        self._verified_runtime_code_sha(expected_sha=finalizer_code_sha)
        if ledger is not None:
            return
        durable = self.commit_spool.inspect(envelope.request_id)
        self._verified_runtime_code_sha(expected_sha=finalizer_code_sha)
        if not isinstance(durable, LabArtifactCommitSpoolEntry):
            return
        if self._authenticated_commit_identity(
            durable.envelope,
            label="uncommitted pending artifact commit",
        ) != self._authenticated_commit_identity(
            envelope,
            label="replayed artifact commit",
        ):
            raise LabFinalizationIntegrityError(
                "uncommitted artifact commit conflicts with sealed replay identity"
            )
        try:
            self._verified_runtime_code_sha(expected_sha=finalizer_code_sha)
            self.commit_spool.quarantine(
                durable,
                reason="uncommitted artifact conflicts with deterministic aggregate",
            )
        except Exception as exc:
            raise LabFinalizationIntegrityError(
                "uncommitted artifact commit could not be safely isolated"
            ) from exc

    @staticmethod
    def _dataframe_bytes(frames: tuple[pd.DataFrame, ...]) -> int:
        return sum(int(frame.memory_usage(index=True, deep=True).sum()) for frame in frames)

    @staticmethod
    def _require_within_limit(*, actual: int, maximum: int, label: str) -> None:
        if actual > maximum:
            raise LabFinalizationResourceLimitError(
                f"finalization {label} exceeds configured job limit"
            )

    def finalize(self, job_id: UUID) -> LabFinalizerResult:
        snapshot = self.reader.get_finalization_snapshot(job_id)
        if snapshot is None:
            return LabFinalizerResult(status="not_ready", job_id=job_id)
        runtime_code_sha = self._verified_runtime_code_sha(expected_sha=snapshot.job.spec.code_sha)
        snapshot_control_bytes = _resident_object_bytes(snapshot)
        self._require_within_limit(
            actual=snapshot_control_bytes,
            maximum=self.job_limits.max_snapshot_control_bytes,
            label="snapshot control bytes",
        )
        self._require_within_limit(
            actual=snapshot_control_bytes + CANONICAL_JSON_STREAM_SCRATCH_BYTES,
            maximum=self.job_limits.max_peak_resident_bytes,
            label="snapshot control peak resident bytes",
        )
        resident_spec_bytes = _resident_object_bytes(snapshot.job.spec)
        self._require_within_limit(
            actual=resident_spec_bytes,
            maximum=self.job_limits.max_spec_bytes,
            label="spec resident bytes",
        )
        spec_bytes = len(snapshot.job.spec.canonical_json().encode("utf-8"))
        self._require_within_limit(
            actual=spec_bytes,
            maximum=self.job_limits.max_spec_bytes,
            label="spec canonical bytes",
        )
        control_usage = LabFinalizerControlUsage(
            snapshot_control_bytes=snapshot_control_bytes,
            spec_bytes=spec_bytes,
        )
        self._require_within_limit(
            actual=len(snapshot.shards),
            maximum=self.job_limits.max_shards,
            label="shard count",
        )
        replay = self._fast_replay(snapshot, finalizer_code_sha=runtime_code_sha)
        if replay is not None:
            return replay
        inspections = tuple(
            self.bundle_reader.inspect(
                evidence,
                resident_bytes=control_usage.resident_bytes,
                expected_job_code_sha=snapshot.job.spec.code_sha,
            )
            for evidence in snapshot.shards
        )
        estimated_rows = sum(item.row_count for item in inspections)
        estimated_compressed_bytes = sum(item.compressed_bytes for item in inspections)
        estimated_uncompressed_bytes = sum(item.declared_uncompressed_bytes for item in inspections)
        estimated_arrow_bytes = sum(item.estimated_arrow_bytes for item in inspections)
        estimated_dataframe_bytes = sum(item.estimated_pandas_bytes for item in inspections)
        retained_metric_bytes = sum(
            item.manifest_usage.retained_metric_bytes for item in inspections
        )
        self._require_within_limit(
            actual=retained_metric_bytes,
            maximum=self.job_limits.max_total_shard_metric_bytes,
            label="total shard metric bytes",
        )
        control_usage = control_usage.model_copy(
            update={"retained_shard_metric_bytes": retained_metric_bytes}
        )
        for actual, maximum, label in (
            (
                estimated_rows,
                self.job_limits.max_total_shard_rows,
                "total shard row count",
            ),
            (
                estimated_compressed_bytes,
                self.job_limits.max_total_compressed_bytes,
                "total compressed shard bytes",
            ),
            (
                estimated_uncompressed_bytes,
                self.job_limits.max_total_declared_uncompressed_bytes,
                "total declared uncompressed shard bytes",
            ),
            (
                estimated_arrow_bytes,
                self.job_limits.max_total_arrow_bytes,
                "total estimated Arrow shard bytes",
            ),
            (
                estimated_dataframe_bytes,
                self.job_limits.max_total_shard_dataframe_bytes,
                "total estimated shard DataFrame bytes",
            ),
        ):
            self._require_within_limit(actual=actual, maximum=maximum, label=label)
        hash_scratch_bytes = CANONICAL_JSON_STREAM_SCRATCH_BYTES
        estimated_aggregate_bytes = min(
            self.job_limits.max_aggregate_dataframe_bytes,
            max(estimated_dataframe_bytes * 2, 1),
        )
        preflight_peak = max(
            control_usage.resident_bytes
            + estimated_dataframe_bytes
            + max((item.estimated_arrow_bytes for item in inspections), default=0)
            + max(
                (item.manifest_usage.peak_resident_bytes for item in inspections),
                default=0,
            )
            + hash_scratch_bytes,
            control_usage.resident_bytes
            + estimated_dataframe_bytes
            + estimated_aggregate_bytes
            + hash_scratch_bytes,
        )
        self._require_within_limit(
            actual=preflight_peak,
            maximum=self.job_limits.max_peak_resident_bytes,
            label="preflight peak resident bytes",
        )
        try:
            shard_result_items: list[LabShardExecutionResult] = []
            total_rows = 0
            total_compressed_bytes = 0
            total_declared_uncompressed_bytes = 0
            total_arrow_bytes = 0
            total_dataframe_bytes = 0
            total_retained_metric_bytes = 0
            for evidence, inspection in zip(snapshot.shards, inspections, strict=True):
                usages: list[LabShardBundleUsage] = []
                shard_result_items.append(
                    self.bundle_reader.read(
                        evidence,
                        observe_usage=usages.append,
                        expected_inspection=inspection,
                        resident_bytes=(
                            control_usage.resident_bytes
                            + total_dataframe_bytes
                            + total_retained_metric_bytes
                        ),
                        expected_job_code_sha=snapshot.job.spec.code_sha,
                    )
                )
                if len(usages) != 1:
                    raise LabFinalizationIntegrityError(
                        "accepted shard read did not produce exact resource evidence"
                    )
                usage = usages[0]
                total_rows += usage.row_count
                total_compressed_bytes += usage.compressed_bytes
                total_declared_uncompressed_bytes += usage.declared_uncompressed_bytes
                total_arrow_bytes += usage.arrow_bytes
                total_dataframe_bytes += usage.materialized_dataframe_bytes
                total_retained_metric_bytes += usage.retained_metric_bytes
                self._require_within_limit(
                    actual=total_rows,
                    maximum=self.job_limits.max_total_shard_rows,
                    label="total shard row count",
                )
                self._require_within_limit(
                    actual=total_compressed_bytes,
                    maximum=self.job_limits.max_total_compressed_bytes,
                    label="total compressed shard bytes",
                )
                self._require_within_limit(
                    actual=total_declared_uncompressed_bytes,
                    maximum=self.job_limits.max_total_declared_uncompressed_bytes,
                    label="total declared uncompressed shard bytes",
                )
                self._require_within_limit(
                    actual=total_arrow_bytes,
                    maximum=self.job_limits.max_total_arrow_bytes,
                    label="total Arrow shard bytes",
                )
                self._require_within_limit(
                    actual=total_dataframe_bytes,
                    maximum=self.job_limits.max_total_shard_dataframe_bytes,
                    label="total shard DataFrame bytes",
                )
                self._require_within_limit(
                    actual=total_retained_metric_bytes,
                    maximum=self.job_limits.max_total_shard_metric_bytes,
                    label="total shard metric bytes",
                )
            shard_results = tuple(shard_result_items)
            self._require_within_limit(
                actual=(
                    control_usage.resident_bytes
                    + total_dataframe_bytes
                    + min(
                        self.job_limits.max_aggregate_dataframe_bytes,
                        max(total_dataframe_bytes * 2, 1),
                    )
                    + hash_scratch_bytes
                ),
                maximum=self.job_limits.max_peak_resident_bytes,
                label="aggregate preflight peak resident bytes",
            )
            result = self.adapter_registry.aggregate_results(snapshot.job.spec, shard_results)
        except (LabFinalizationIntegrityError, BaseExceptionGroup):
            raise
        except Exception as exc:
            raise LabFinalizationIntegrityError(
                "accepted shard results could not be aggregated"
            ) from exc
        aggregate_frames = tuple(table.frame for table in result.tables)
        self._require_within_limit(
            actual=sum(len(frame) for frame in aggregate_frames),
            maximum=self.job_limits.max_aggregate_rows,
            label="aggregate row count",
        )
        aggregate_dataframe_bytes = self._dataframe_bytes(aggregate_frames)
        self._require_within_limit(
            actual=aggregate_dataframe_bytes,
            maximum=self.job_limits.max_aggregate_dataframe_bytes,
            label="aggregate DataFrame bytes",
        )
        self._require_within_limit(
            actual=(
                control_usage.resident_bytes
                + total_dataframe_bytes
                + aggregate_dataframe_bytes
                + hash_scratch_bytes
            ),
            maximum=self.job_limits.max_peak_resident_bytes,
            label="aggregate peak resident bytes",
        )
        self._require_within_limit(
            actual=len(result.tables),
            maximum=self.job_limits.max_final_artifact_table_count,
            label="final artifact table count",
        )
        metadata_characters = sum(
            len(str(column)) for table in result.tables for column in table.frame.columns
        ) + sum(len(table.name) for table in result.tables)
        estimated_final_metrics_bytes = (
            4096
            + (retained_metric_bytes * 2)
            + (metadata_characters * 12)
            + (len(snapshot.shards) * 512)
        )
        self._require_within_limit(
            actual=estimated_final_metrics_bytes,
            maximum=self.job_limits.max_final_metrics_bytes,
            label="final metrics estimated bytes",
        )
        estimated_report_bytes = (estimated_final_metrics_bytes * 2) + 4096
        self._require_within_limit(
            actual=estimated_report_bytes,
            maximum=self.job_limits.max_report_markdown_bytes,
            label="report estimated bytes",
        )
        estimated_control_usage = control_usage.model_copy(
            update={
                "final_metrics_bytes": estimated_final_metrics_bytes,
                "report_bytes": estimated_report_bytes,
            }
        )
        estimated_roundtrip_usage = LabArtifactRoundtripPeakUsage.conservative(
            aggregate_dataframe_bytes=aggregate_dataframe_bytes,
            payload_bytes=self.job_limits.max_final_artifact_payload_bytes,
            control_bytes=estimated_control_usage.resident_bytes,
        )
        self._require_within_limit(
            actual=estimated_roundtrip_usage.peak_resident_bytes,
            maximum=self.job_limits.max_peak_resident_bytes,
            label="artifact roundtrip peak resident bytes",
        )
        metrics = self._metrics(
            snapshot,
            result,
            shard_results,
            finalizer_code_sha=runtime_code_sha,
        )
        metrics_payload = metrics.model_dump(mode="json")
        metrics_bytes = len(canonical_json_bytes(metrics_payload))
        self._require_within_limit(
            actual=metrics_bytes,
            maximum=self.job_limits.max_final_metrics_bytes,
            label="final metrics canonical bytes",
        )
        report_markdown = self._report(metrics)
        report_bytes = len(report_markdown.encode("utf-8"))
        self._require_within_limit(
            actual=report_bytes,
            maximum=self.job_limits.max_report_markdown_bytes,
            label="report Markdown bytes",
        )
        control_usage = control_usage.model_copy(
            update={
                "final_metrics_bytes": max(_resident_object_bytes(metrics), metrics_bytes * 2),
                "report_bytes": max(sys.getsizeof(report_markdown), report_bytes),
            }
        )
        self._require_within_limit(
            actual=control_usage.resident_bytes + hash_scratch_bytes,
            maximum=self.job_limits.max_peak_resident_bytes,
            label="final control peak resident bytes",
        )
        del shard_result_items, shard_results
        first = snapshot.shards[0].shard
        try:
            plan = self.artifact_store.preview_candidate(
                job_id=snapshot.job.job_id,
                spec=snapshot.job.spec,
                plan_hash=first.plan_hash,
                adapter_id=first.adapter_id,
                adapter_version=first.adapter_version,
                result_contract_version=COMPLETE_RESULT_CONTRACT_VERSION,
                metrics=metrics_payload,
                report_markdown=report_markdown,
                tables={table.name: table.frame for table in result.tables},
                payload_budget=LabArtifactPayloadBudget(
                    max_single_payload_bytes=(
                        self.job_limits.max_final_artifact_single_payload_bytes
                    ),
                    max_total_payload_bytes=(self.job_limits.max_final_artifact_payload_bytes),
                    max_table_count=self.job_limits.max_final_artifact_table_count,
                ),
            )
        except LabArtifactPayloadLimitError as exc:
            raise LabFinalizationResourceLimitError(
                f"final artifact payload exceeds configured job limit: {exc}"
            ) from exc
        except LabArtifactError as exc:
            raise LabFinalizationIntegrityError(
                "complete result candidate could not be previewed"
            ) from exc
        planned_payload_bytes = sum(len(item.payload) for item in plan.payloads)
        actual_roundtrip_usage = LabArtifactRoundtripPeakUsage.conservative(
            aggregate_dataframe_bytes=aggregate_dataframe_bytes,
            payload_bytes=planned_payload_bytes,
            control_bytes=control_usage.resident_bytes,
        )
        self._require_within_limit(
            actual=actual_roundtrip_usage.peak_resident_bytes,
            maximum=self.job_limits.max_peak_resident_bytes,
            label="materialized artifact roundtrip peak resident bytes",
        )
        try:
            self._verified_runtime_code_sha(expected_sha=runtime_code_sha)
            sealed = self._recover_or_prepare(plan)
        except LabFinalizationIntegrityError as primary_error:
            try:
                self._isolate_uncommitted_conflicting_replay(
                    snapshot,
                    finalizer_code_sha=runtime_code_sha,
                )
            except BaseException as cleanup_error:
                raise BaseExceptionGroup(
                    "finalization conflict and uncommitted replay isolation failed",
                    [primary_error, cleanup_error],
                ) from None
            raise
        self._after_artifact_sealed(sealed)
        envelope = self._envelope(
            sealed,
            snapshot,
            finalizer_code_sha=runtime_code_sha,
        )
        published = self._verified_pending_for_envelope(envelope)
        if published is None:
            self._verified_runtime_code_sha(expected_sha=runtime_code_sha)
            published = self.commit_spool.publish(envelope)
        self._verified_runtime_code_sha(expected_sha=runtime_code_sha)
        self._after_commit_published(published)
        if isinstance(published, LabAcknowledgedArtifactCommit):
            return self._validate_acknowledgement(sealed, envelope, published)
        return LabFinalizerResult(
            status="published",
            job_id=job_id,
            request_id=envelope.request_id,
            manifest_hash=sealed.manifest_hash,
            complete_result_hash=sealed.manifest.complete_result_hash,
        )
