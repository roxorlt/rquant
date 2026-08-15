"""Filesystem-fenced background worker for Strategy Lab shard claims."""

from __future__ import annotations

import base64
import errno
import hashlib
import hmac
import multiprocessing
import os
import re
import selectors
import signal
import socket
import stat
import struct
import tempfile
import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from multiprocessing.connection import Client, Connection
from multiprocessing.context import AuthenticationError
from multiprocessing.process import BaseProcess
from pathlib import Path
from types import FrameType
from typing import Literal, TypeVar
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.canonical_json_stream import (
    CanonicalJsonStreamWriter,
    write_legacy_pandas_table_json,
)
from rquant.data_metadata import DatasetSnapshotBinding
from rquant.lab_claim_finalizer import LabClaimFinalizerError, LabClaimPublicationWorkerVerifier
from rquant.lab_claim_publication import V2_UNASSIGNED_WORKER_ID
from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.lab_job_protocol import InvalidCommandEnvelopeError
from rquant.lab_logging import _safe_structured_log
from rquant.lab_resource_authority_adapter import (
    LAB_RESOURCE_AUTHORITY_REGISTRY_HASH,
    LAB_RESOURCE_AUTHORITY_REGISTRY_ID,
    LAB_RESOURCE_AUTHORITY_REGISTRY_VERSION,
    LabResourceAuthorityReservationAdapter,
    ResourceAuthorityAdapterConfig,
    ResourceAuthorityJournalClient,
    parse_resource_authority_adapter_config,
)
from rquant.lab_result_digest import (
    CURRENT_CONTENT_DIGEST_ALGORITHM,
    CURRENT_RESULT_MANIFEST_SCHEMA_VERSION,
)
from rquant.lab_shard_protocol import (
    LabClaimAlreadyConsumedError,
    LabClaimNotConsumedError,
    LabClaimRevokedError,
    LabClaimSpool,
    LabClaimSpoolEntry,
    LabClaimSupersededError,
    LabReportReceipt,
    LabReportSpool,
    LabReportSpoolEntry,
    LabShardClaim,
    LabShardClaimV2,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabShardTelemetry,
    LabWorkerReport,
    LabWorkerStopped,
)
from rquant.research_gate import open_gated_research_store
from rquant.research_run_spec import ResearchRunSpec
from rquant.research_snapshot import ResearchExecutionSession
from rquant.resource_admission import (
    MICROSECONDS_PER_SECOND,
    AdmissionDecision,
    AdmissionOutcome,
    AdmissionPolicy,
    AdmissionRequest,
    ResourceReservationIdentity,
    ResourceReservationLease,
    ResourceSnapshot,
    SourceQuotaLease,
    TradingSession,
    derive_lab_admission_request,
    evaluate_admission,
    seconds_to_microseconds,
    timedelta_microseconds,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_resource_admission import (
    LocalResourceSnapshotProvider,
    PersistentResourceReservationStore,
    RuntimeHealthAuthorityLiveSloProbeConfig,
    RuntimeHealthAuthorityWatermark,
    RuntimeTradeCalendarSessionResolver,
    SystemResourceProbe,
)
from rquant.runtime_resource_admission import (
    StaticAdmissionPolicyProvider as RuntimeStaticAdmissionPolicyProvider,
)
from rquant.runtime_resource_admission import (
    _system_clock as _runtime_resource_system_clock,
)
from rquant.source_operation_contracts import SourceOperationContractError
from rquant.strategy_job_adapters import (
    MAX_RESULT_WIRE_BYTES,
    LabShardExecutionResult,
    LabShardExecutionWireResult,
    LabShardMetric,
    StrategyJobAdapterRegistry,
    StrategyShardPayload,
    ValidatedStrategyShard,
    default_strategy_job_adapter_registry,
)
from rquant.strict_json import (
    canonical_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

LAB_WORKER_MAX_SHARDS_PER_TICK = 1
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_GARBAGE_LEDGER_NAME = re.compile(
    r"(?P<garbage_id>[0-9a-f]{32})-(?P<sequence>[0-2])-"
    r"(?P<state>prepared|quarantined|deferred_gc)\.json"
)
_GARBAGE_STATE_SEQUENCE = {
    "prepared": 0,
    "quarantined": 1,
    "deferred_gc": 2,
}
_GARBAGE_INTENT_NAME = re.compile(r"(?P<garbage_id>[0-9a-f]{32})-prepared-intent-v1\.json")
_GARBAGE_INTENT_TEMP_NAME = re.compile(
    r"\.prepared-intent-tmp-v1-(?P<garbage_id>[0-9a-f]{32})-[0-9a-f]{32}\.tmp"
)
_GARBAGE_DERIVED_TEMP_NAME = re.compile(r"\.derived-json-tmp-v1-[0-9a-f]{32}\.tmp")
_GARBAGE_ORPHAN_METADATA_TEMP_NAME = re.compile(
    r"\.orphan-metadata-tmp-v1-(?P<metadata_hash>[0-9a-f]{64})-[0-9a-f]{32}\.tmp"
)
_LEGACY_EMPTY_STAGING_ORPHAN_NAME = re.compile(
    r"legacy-empty-staging-(?P<staging_id>[0-9a-f]{32})"
    r"(?:-(?P<orphan_token>[0-9a-f]{32}))?"
)
_GARBAGE_RECOVERY_QUEUE_NAME = re.compile(r"(?P<sequence>[0-9]{20})\.json")
_QUEUE_MIGRATION_CHAIN_GENESIS = hashlib.sha256(
    b"rquant:lab-quarantine-recovery-migration-chain:v3"
).hexdigest()
_LIVE_TRADING_SESSIONS = frozenset(
    {
        TradingSession.PRE_MARKET,
        TradingSession.MORNING,
        TradingSession.LUNCH,
        TradingSession.AFTERNOON,
    }
)
_CHILD_TERMINATE_GRACE_MICROSECONDS = 50_000
_CHILD_OUTCOME_EXIT_GRACE_MICROSECONDS = 250_000
_ISOLATION_READY_TIMEOUT_MICROSECONDS = 2_000_000
_PROCESS_CLEANUP_RETRIES = 3
_RESOURCE_RESERVATION_LOCK_WAIT_MAX_MICROSECONDS = 50_000
_RESOURCE_AUTHORITY_POLL_MICROSECONDS = 10_000
_MAX_CONTROL_WIRE_BYTES = 1024 * 1024
_MAX_SHARD_RESULT_WIRE_BYTES = MAX_RESULT_WIRE_BYTES
_AUTHORITY_SPAWN_ALLOWANCE_MICROSECONDS = 750_000
_PRESTART_AUTHORITY_CLEANUP_RESERVE_MICROSECONDS = 250_000
_AUTHORITY_CHILD_CLEANUP_BUDGET_MICROSECONDS = 250_000
_WIRE_CHALLENGE = b"#CHALLENGE#"
_WIRE_WELCOME = b"#WELCOME#"
_WIRE_FAILURE = b"#FAILURE#"
_WIRE_DIGEST_PREFIX = b"{sha256}"
_WIRE_CHALLENGE_BYTES = 40
_BUILTIN_SHARD_REGISTRY_ID = "rquant.lab-shard.builtin"
_BUILTIN_SHARD_REGISTRY_VERSION = 1
_BUILTIN_SHARD_REGISTRY_HASH = hashlib.sha256(b"rquant:lab-shard:builtin:v1").hexdigest()
_BUILTIN_AUTHORITY_REGISTRY_ID = "rquant.lab-authority.builtin"
_BUILTIN_AUTHORITY_REGISTRY_VERSION = 1
_BUILTIN_AUTHORITY_REGISTRY_HASH = hashlib.sha256(b"rquant:lab-authority:builtin:v1").hexdigest()
_TEST_AUTHORITY_REGISTRY_ID = "rquant.lab-authority.test-fixture"
_TEST_AUTHORITY_REGISTRY_VERSION = 1
_TEST_AUTHORITY_REGISTRY_HASH = hashlib.sha256(b"rquant:lab-authority:test-fixture:v1").hexdigest()
_TEST_SHARD_REGISTRY_ID = "rquant.lab-shard.test-fixture"
_TEST_SHARD_REGISTRY_VERSION = 1
_TEST_SHARD_REGISTRY_HASH = hashlib.sha256(b"rquant:lab-shard:test-fixture:v1").hexdigest()


def _microseconds_to_seconds(value: int) -> float:
    return max(0, value) / MICROSECONDS_PER_SECOND


def _positive_duration_microseconds(value: object, *, label: str) -> int:
    microseconds = seconds_to_microseconds(value, label=label)
    if microseconds <= 0:
        raise ValueError(f"{label} must be positive")
    return microseconds


def _monotonic_microseconds() -> int:
    return time.monotonic_ns() // 1_000


def _canonical_monotonic_clock(
    clock: Callable[[], float],
    *,
    label: str,
) -> Callable[[], int]:
    if clock is time.monotonic:
        return _monotonic_microseconds

    def read_microseconds() -> int:
        return seconds_to_microseconds(clock(), label=label)

    return read_microseconds


def _system_clock() -> datetime:
    return datetime.now(UTC)


def _start_isolated_session() -> None:
    os.setsid()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("worker clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_json(value: object) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _bounded_exception_message(error: BaseException) -> str:
    if isinstance(error, BaseExceptionGroup):
        details = "; ".join(
            f"{type(nested).__name__}: {_bounded_exception_message(nested)}"
            for nested in error.exceptions
        )
        message = f"{error.message}: {details}"
    else:
        message = " ".join(str(error).split()) or type(error).__name__
    return message[:400]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def canonical_shard_frame_digest(
    frame: pd.DataFrame,
) -> str:
    if any(not isinstance(column, str) for column in frame.columns):
        raise ValueError("artifact DataFrame columns must be strings")
    digest = hashlib.sha256()
    writer = CanonicalJsonStreamWriter(digest.update)
    write_legacy_pandas_table_json(writer, frame)
    return digest.hexdigest()


class LabWorkerModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )


class LabWireModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
        strict=True,
    )


class LabClosedRegistryBinding(LabWireModel):
    registry_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{2,127}$")
    registry_version: int = Field(ge=1, le=1_000_000)
    registry_hash: str = Field(pattern=_HASH_PATTERN)
    configuration_json: str

    @model_validator(mode="after")
    def validate_configuration(self) -> LabClosedRegistryBinding:
        strict_canonical_json_loads(self.configuration_json)
        return self


class LabShardRuntimeManifest(LabWireModel):
    schema_version: Literal[1] = 1
    registry: LabClosedRegistryBinding


class LabResourceAuthorityManifest(LabWireModel):
    schema_version: Literal[1] = 1
    registry: LabClosedRegistryBinding


class LabSnapshotAuthorityState(LabWireModel):
    schema_version: Literal[1] = 1
    state_kind: Literal["runtime-health-watermark", "test-fixture"]
    state_json: str

    @model_validator(mode="after")
    def validate_state_json(self) -> LabSnapshotAuthorityState:
        strict_canonical_json_loads(self.state_json)
        return self


class _BuiltinSnapshotAuthorityConfig(LabWireModel):
    disk_path: Path
    live_slo_config: RuntimeHealthAuthorityLiveSloProbeConfig
    market_calendar: MarketCalendarAuthority


class _BuiltinResourceAuthorityConfig(LabWireModel):
    snapshot: _BuiltinSnapshotAuthorityConfig
    policy: AdmissionPolicy


class _AuthorityWireRequest(LabWireModel):
    message_type: Literal["authority-request"] = "authority-request"
    operation: Literal["admission", "policy", "snapshot", "quota"]
    manifest: LabResourceAuthorityManifest
    spec: ResearchRunSpec | None = None
    admission_request: AdmissionRequest | None = None
    snapshot: ResourceSnapshot | None = None
    authority_state: LabSnapshotAuthorityState | None = None


class _AuthorityWireResult(LabWireModel):
    message_type: Literal["authority-result"] = "authority-result"
    operation: Literal["admission", "policy", "snapshot", "quota"]
    policy: AdmissionPolicy | None = None
    snapshot: ResourceSnapshot | None = None
    quota_lease: SourceQuotaLease | None = None
    authority_state: LabSnapshotAuthorityState | None = None
    error_type: str | None = None
    message: str | None = None


class _ShardWireRequest(LabWireModel):
    message_type: Literal["shard-request"] = "shard-request"
    manifest: LabShardRuntimeManifest
    validated: ValidatedStrategyShard
    runtime_code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")


class _IsolatedExecutionWireOutcome(LabWireModel):
    message_type: Literal["shard-outcome"] = "shard-outcome"
    result: LabShardExecutionWireResult | None = None
    phase: Literal["session", "execute"] | None = None
    error_type: str | None = None
    message: str | None = None
    configuration_error: bool = False


def _closed_configuration_json(value: LabWireModel) -> str:
    return canonical_json_bytes(value.model_dump(mode="json", round_trip=True)).decode("utf-8")


def build_builtin_resource_authority_manifest(
    snapshot_provider: object,
    policy_provider: object,
    quota_provider: object | None = None,
) -> LabResourceAuthorityManifest:
    if quota_provider is not None:
        raise LabDaemonConfigurationError(
            "legacy source quota provider is not registered in the closed authority registry"
        )
    if type(snapshot_provider) is not LocalResourceSnapshotProvider:
        raise LabDaemonConfigurationError(
            "legacy resource snapshot provider is not registered in the closed authority registry"
        )
    if type(policy_provider) is not RuntimeStaticAdmissionPolicyProvider:
        raise LabDaemonConfigurationError(
            "legacy admission policy provider is not registered in the closed authority registry"
        )
    if (
        type(snapshot_provider.probe) is not SystemResourceProbe
        or type(snapshot_provider.session_resolver) is not RuntimeTradeCalendarSessionResolver
        or snapshot_provider.clock is not _runtime_resource_system_clock
        or snapshot_provider.live_slo_config is None
    ):
        raise LabDaemonConfigurationError(
            "legacy resource snapshot provider has no closed built-in descriptor"
        )
    configuration = _BuiltinResourceAuthorityConfig(
        snapshot=_BuiltinSnapshotAuthorityConfig(
            disk_path=snapshot_provider.disk_path,
            live_slo_config=snapshot_provider.live_slo_config,
            market_calendar=snapshot_provider.session_resolver.authority,
        ),
        policy=policy_provider.policy,
    )
    return LabResourceAuthorityManifest(
        registry=LabClosedRegistryBinding(
            registry_id=_BUILTIN_AUTHORITY_REGISTRY_ID,
            registry_version=_BUILTIN_AUTHORITY_REGISTRY_VERSION,
            registry_hash=_BUILTIN_AUTHORITY_REGISTRY_HASH,
            configuration_json=_closed_configuration_json(configuration),
        )
    )


def build_resource_journal_authority_manifest(
    configuration: ResourceAuthorityAdapterConfig,
) -> LabResourceAuthorityManifest:
    """Bind the worker explicitly to the V2 external resource-journal registry."""

    validated = ResourceAuthorityAdapterConfig.model_validate(configuration, strict=True)
    return LabResourceAuthorityManifest(
        registry=LabClosedRegistryBinding(
            registry_id=LAB_RESOURCE_AUTHORITY_REGISTRY_ID,
            registry_version=LAB_RESOURCE_AUTHORITY_REGISTRY_VERSION,
            registry_hash=LAB_RESOURCE_AUTHORITY_REGISTRY_HASH,
            configuration_json=canonical_json_bytes(
                validated.model_dump(mode="json", round_trip=True)
            ).decode("utf-8"),
        )
    )


def build_builtin_shard_runtime_manifest(
    *,
    catalog_path: Path,
    forbidden_paths: tuple[Path, ...],
    snapshot_root: Path,
    research_lake_root: Path,
) -> LabShardRuntimeManifest:
    from rquant.lab_worker_registry import builtin_lab_shard_configuration

    configuration = builtin_lab_shard_configuration(
        catalog_path=catalog_path,
        forbidden_paths=forbidden_paths,
        snapshot_root=snapshot_root,
        research_lake_root=research_lake_root,
    )
    return LabShardRuntimeManifest(
        registry=LabClosedRegistryBinding(
            registry_id=_BUILTIN_SHARD_REGISTRY_ID,
            registry_version=_BUILTIN_SHARD_REGISTRY_VERSION,
            registry_hash=_BUILTIN_SHARD_REGISTRY_HASH,
            configuration_json=canonical_json_bytes(
                configuration.model_dump(mode="json", round_trip=True)
            ).decode("utf-8"),
        )
    )


WireModelT = TypeVar("WireModelT", bound=LabWireModel)


def _encode_wire_message(value: LabWireModel) -> bytes:
    return canonical_json_bytes(value.model_dump(mode="json", round_trip=True))


def _decode_wire_message(
    payload: bytes,
    *,
    model: type[WireModelT],
    max_bytes: int,
    label: str,
) -> WireModelT:
    if type(payload) is not bytes:
        raise LabDaemonConfigurationError(f"{label} must be bytes")
    if len(payload) > max_bytes:
        raise LabDaemonConfigurationError(f"{label} exceeds the wire size limit")
    try:
        strict_canonical_json_loads(payload)
        validated = model.model_validate_json(payload)
        if _encode_wire_message(validated) != payload:
            raise ValueError("wire schema validation changed the canonical payload")
        return validated
    except Exception as exc:
        if isinstance(exc, LabDaemonConfigurationError):
            raise
        raise LabDaemonConfigurationError(
            f"{label} is malformed: {_bounded_exception_message(exc)}"
        ) from exc


def _assert_primitive_process_start(
    target: Callable[..., object],
    arguments: tuple[object, ...],
) -> None:
    allowed_targets = {
        globals().get("_authority_wire_child"),
        globals().get("_shard_wire_child"),
    }
    if target not in allowed_targets:
        raise LabDaemonConfigurationError("child process target is not registered")
    if any(type(argument) not in {bytes, str, int} for argument in arguments):
        raise LabDaemonConfigurationError("child process arguments must be primitive wire values")


class LabShardArtifactManifest(LabWorkerModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    file_name: str = Field(pattern=r"^[0-9]{3}-[a-z][a-z0-9_]*\.parquet$")
    format: Literal["parquet"] = "parquet"
    row_count: int = Field(ge=0)
    columns: tuple[str, ...]
    file_size: int = Field(gt=0)
    file_sha256: str = Field(pattern=_HASH_PATTERN)
    content_sha256: str = Field(pattern=_HASH_PATTERN)


class LabShardResultManifest(LabWorkerModel):
    schema_version: Literal[1, 2] = CURRENT_RESULT_MANIFEST_SCHEMA_VERSION
    worker_code_sha: str | None = Field(default=None, pattern=r"^[0-9a-f]{40}$")
    content_digest_algorithm: Literal["rquant-pandas-table-json-sha256-v2"] | None = None
    job_id: UUID
    shard_id: UUID
    claim_token: UUID
    claim_generation: int = Field(ge=1)
    scheduler_fencing_token: int = Field(ge=1)
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    payload_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    experiment_id: str | None = Field(default=None, pattern=_HASH_PATTERN)
    experiment_attempt_identity: str | None = Field(default=None, pattern=_HASH_PATTERN)
    strategy_execution_identity_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    strategy_spec_fingerprint: str | None = Field(default=None, pattern=_HASH_PATTERN)
    strategy_executable_fingerprint: str | None = Field(default=None, pattern=_HASH_PATTERN)
    candidate_schema_fingerprint: str | None = Field(default=None, pattern=_HASH_PATTERN)
    artifacts: tuple[LabShardArtifactManifest, ...]
    metrics: tuple[LabShardMetric, ...] = ()

    @model_validator(mode="after")
    def validate_artifacts(self) -> LabShardResultManifest:
        provenance = (self.worker_code_sha, self.content_digest_algorithm)
        if self.schema_version == CURRENT_RESULT_MANIFEST_SCHEMA_VERSION:
            if (
                self.worker_code_sha is None
                or self.content_digest_algorithm != CURRENT_CONTENT_DIGEST_ALGORITHM
            ):
                raise ValueError("current result manifest requires complete digest provenance")
        elif provenance != (None, None):
            raise ValueError("legacy result manifest cannot carry current digest provenance")
        ownership = (
            self.experiment_id,
            self.experiment_attempt_identity,
            self.strategy_execution_identity_hash,
            self.strategy_spec_fingerprint,
            self.strategy_executable_fingerprint,
            self.candidate_schema_fingerprint,
        )
        if any(value is not None for value in ownership) and any(
            value is None for value in ownership
        ):
            raise ValueError("result manifest ownership identity must be complete")
        names = tuple(artifact.name for artifact in self.artifacts)
        files = tuple(artifact.file_name for artifact in self.artifacts)
        if not names:
            raise ValueError("result manifest requires at least one artifact")
        if len(names) != len(set(names)) or len(files) != len(set(files)):
            raise ValueError("result manifest artifact identities must be unique")
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json", exclude_none=True),
        )

    @property
    def manifest_hash(self) -> str:
        return _sha256_bytes(self.canonical_json().encode("utf-8"))


_LabWorkerFailureKind = Literal[
    "claim_validation",
    "session_startup",
    "session",
    "execution",
    "deadline",
    "fence",
    "seal",
]
_WireFailureKind = Literal["session_startup", "session"]


class LabWorkerFailure(LabWorkerModel):
    phase: Literal["claim", "session", "execute", "deadline", "fence", "seal"]
    failure_kind: _LabWorkerFailureKind
    error_type: str = Field(min_length=1)
    message: str = Field(min_length=1)

    @model_validator(mode="before")
    @classmethod
    def add_legacy_failure_kind(cls, value: object) -> object:
        if not isinstance(value, dict) or "failure_kind" in value:
            return value
        phase = value.get("phase")
        legacy_kinds = {
            "claim": "claim_validation",
            "session": "session",
            "execute": "execution",
            "deadline": "deadline",
            "fence": "fence",
            "seal": "seal",
        }
        kind = legacy_kinds.get(phase)
        if kind is None:
            return value
        return {**value, "failure_kind": kind}

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabWorkerHealthWarning(LabWorkerModel):
    category: Literal["quarantine_reconcile_failed"]
    error_type: str = Field(min_length=1)
    message: str = Field(min_length=1)


class LabWorkerTickResult(LabWorkerModel):
    status: Literal[
        "idle",
        "deferred",
        "succeeded",
        "failed",
        "stopped",
        "reported",
        "awaiting_receipt",
        "unknown",
    ]
    claim_token: UUID | None = None
    manifest_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    report_id: UUID | None = None
    admission_decision: AdmissionDecision | None = None
    health_warnings: tuple[LabWorkerHealthWarning, ...] = ()

    @model_validator(mode="after")
    def validate_admission_decision(self) -> LabWorkerTickResult:
        if (self.status == "deferred") != (self.admission_decision is not None):
            raise ValueError("only a deferred worker tick may carry an admission decision")
        return self


@dataclass(frozen=True)
class _ResourceAdmissionEvaluation:
    decision: AdmissionDecision
    request: AdmissionRequest
    snapshot: ResourceSnapshot
    policy: AdmissionPolicy
    quota_lease: SourceQuotaLease | None = None


@dataclass(frozen=True)
class _IsolatedExecutionOutcome:
    result: LabShardExecutionResult | None = None
    phase: Literal["session", "execute"] | None = None
    error_type: str | None = None
    message: str | None = None
    configuration_error: bool = False


class _IsolationReadiness(LabWireModel):
    message_type: Literal["readiness"] = "readiness"
    ready: bool
    child_pid: int
    group_id: int | None = None
    error_type: str | None = None
    message: str | None = None


class _IsolationStartAck(LabWireModel):
    message_type: Literal["start-ack"] = "start-ack"
    accepted: bool
    not_after_monotonic_microseconds: int | None
    execution_limit_microseconds: int | None = Field(default=None, ge=1)


@dataclass(frozen=True)
class _ClassifiedWireFailure:
    failure_kind: _WireFailureKind
    error: Exception


@dataclass(frozen=True)
class _IsolatedExecutionControl:
    outcome: _IsolatedExecutionOutcome | None = None
    stop_reason: str | None = None
    preemption: AdmissionDecision | None = None
    session_failure: _ClassifiedWireFailure | None = None
    resource_error: Exception | None = None
    heartbeat_error: Exception | None = None


@dataclass
class _WireChild:
    process: BaseProcess
    connection: Connection | _DeadlineWireEndpoint
    group_id: int
    address: str


@dataclass
class _ManagedAuthorityChild:
    """One registered authority process with one mutable cleanup owner."""

    child: _WireChild
    process_id: int
    cached_pid: int
    operation: Literal["admission", "policy", "snapshot", "quota"]
    owner: Literal[
        "direct",
        "startup",
        "ready",
        "startup_cleanup",
        "consumer",
        "canceller",
        "tick_cleanup",
        "reap_pending",
        "closed",
    ]
    lock: threading.Lock
    cancelled: threading.Event
    cleanup_complete: threading.Event
    os_process_exited_verified: bool = False
    ipc_closed: bool = False
    process_handle_closed: bool = False
    last_errors: tuple[BaseException, ...] = ()
    cleanup_error: BaseException | None = None
    cleanup_retry_count: int = 0
    cleanup_in_progress: bool = False


@dataclass
class _PrestartedAuthorityStage:
    handoff: threading.Event
    startup_complete: threading.Event
    cleanup_complete: threading.Event
    cancelled: threading.Event
    lock: threading.Lock
    deadline_microseconds: int
    cleanup_deadline_microseconds: int
    startup_thread: threading.Thread | None = None
    managed_child: _ManagedAuthorityChild | None = None
    error: BaseException | None = None
    owner: Literal["startup", "ready", "startup_cleanup", "consumer", "canceller", "closed"] = (
        "startup"
    )

    @property
    def child(self) -> _WireChild | None:
        """Compatibility view used only by stage lifecycle assertions."""

        return None if self.managed_child is None else self.managed_child.child


@dataclass
class _PreAckAdmissionStage:
    cancelled: threading.Event
    completion: threading.Event
    deadline_microseconds: int
    thread: threading.Thread | None = None
    evaluation: _ResourceAdmissionEvaluation | None = None
    stop_reason: str | None = None
    session_failure: _ClassifiedWireFailure | None = None
    resource_error: Exception | None = None


class LabIsolatedExecutionError(RuntimeError):
    def __init__(self, *, remote_error_type: str, message: str) -> None:
        super().__init__(message)
        self.remote_error_type = remote_error_type


class LabPreparedFileIdentity(LabWorkerModel):
    file_name: str = Field(pattern=r"^(?:manifest\.json|[0-9]{3}-[a-z][a-z0-9_]*\.parquet)$")
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)


class LabPreparedShardBundle(LabWorkerModel):
    temporary: Path | None
    manifest: LabShardResultManifest
    file_identities: tuple[LabPreparedFileIdentity, ...]
    reuses_existing: bool = False
    existing_device: int | None = Field(default=None, ge=0)
    existing_inode: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_existing_identity(self) -> LabPreparedShardBundle:
        has_identity = self.existing_device is not None and self.existing_inode is not None
        if self.reuses_existing != has_identity or self.reuses_existing != (self.temporary is None):
            raise ValueError("prepared bundle reuse identity is inconsistent")
        expected_files = {"manifest.json"} | {
            artifact.file_name for artifact in self.manifest.artifacts
        }
        observed_files = tuple(item.file_name for item in self.file_identities)
        if observed_files != tuple(sorted(observed_files)) or set(observed_files) != expected_files:
            raise ValueError("prepared bundle file identities are incomplete")
        return self


class LabReclaimInventoryEntry(LabWorkerModel):
    relative_path: str = Field(pattern=r"^(?:manifest\.json|[0-9]{3}-[a-z][a-z0-9_]*\.parquet)$")
    file_type: Literal["regular"] = "regular"
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)
    sha256: str = Field(pattern=_HASH_PATTERN)


class LabRegularFileIdentity(LabWorkerModel):
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int = Field(ge=0)
    sha256: str = Field(pattern=_HASH_PATTERN)


class LabGarbageInventoryEntry(LabWorkerModel):
    relative_path: str = Field(min_length=1)
    file_type: Literal["directory", "regular"]
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    size: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_file_identity(self) -> LabGarbageInventoryEntry:
        parts = self.relative_path.split("/")
        if self.relative_path != "." and (
            self.relative_path.startswith("/") or any(part in {"", ".", ".."} for part in parts)
        ):
            raise ValueError("garbage inventory path is unsafe")
        has_content = self.size is not None and self.sha256 is not None
        if has_content != (self.file_type == "regular"):
            raise ValueError("garbage regular inventory requires size and hash")
        return self


class LabGarbageOwner(LabWorkerModel):
    schema_version: Literal[1] = 1
    garbage_id: UUID = UUID(int=0)
    purpose: str = Field(min_length=1)
    original_relative_path: str = Field(min_length=1)
    protocol_phase: Literal["source_identified"] = "source_identified"
    source_device: int = Field(default=0, ge=0)
    source_inode: int = Field(default=0, ge=0)
    payload_type: Literal["directory", "regular"]
    inventory: tuple[LabGarbageInventoryEntry, ...]
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabGarbageOwner:
        path_parts = self.original_relative_path.split("/")
        if self.original_relative_path.startswith("/") or any(
            part in {"", ".", ".."} for part in path_parts
        ):
            raise ValueError("garbage original path is unsafe")
        paths = tuple(entry.relative_path for entry in self.inventory)
        if not paths or paths[0] != "." or paths != tuple(sorted(paths)):
            raise ValueError("garbage inventory must be non-empty and sorted")
        if len(paths) != len(set(paths)):
            raise ValueError("garbage inventory paths must be unique")
        if self.inventory[0].file_type != self.payload_type:
            raise ValueError("garbage root inventory type conflicts with payload")
        source = self.inventory[0]
        if self.source_device and self.source_device != source.device:
            raise ValueError("garbage owner source device conflicts with inventory")
        if self.source_inode and self.source_inode != source.inode:
            raise ValueError("garbage owner source inode conflicts with inventory")
        object.__setattr__(self, "source_device", source.device)
        object.__setattr__(self, "source_inode", source.inode)
        canonical = _canonical_json(
            {
                "inventory": [entry.model_dump(mode="json") for entry in self.inventory],
                "original_relative_path": self.original_relative_path,
                "payload_type": self.payload_type,
                "protocol_phase": self.protocol_phase,
                "purpose": self.purpose,
                "schema_version": self.schema_version,
                "source_device": source.device,
                "source_inode": source.inode,
            },
        )
        content_hash = _sha256_bytes(canonical.encode("utf-8"))
        garbage_id = uuid5(NAMESPACE_URL, f"rquant:lab-garbage:{content_hash}")
        if self.content_hash and self.content_hash != content_hash:
            raise ValueError("garbage owner content_hash conflicts with inventory")
        if self.garbage_id.int and self.garbage_id != garbage_id:
            raise ValueError("garbage_id conflicts with deterministic inventory")
        object.__setattr__(self, "content_hash", content_hash)
        object.__setattr__(self, "garbage_id", garbage_id)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabGarbagePreparedIntent(LabWorkerModel):
    schema_version: Literal[1, 2] = 2
    state: Literal["prepared"] = "prepared"
    source_relative_path: str = Field(min_length=1)
    staging_relative_path: str = Field(min_length=1)
    owner: LabGarbageOwner
    created_at: datetime | None = None
    intent_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabGarbagePreparedIntent:
        if self.schema_version == 1:
            if self.created_at is not None:
                raise ValueError("legacy prepared intent cannot contain created_at")
        elif self.created_at is None:
            raise ValueError("prepared intent requires created_at")
        else:
            object.__setattr__(self, "created_at", _utc(self.created_at))
        expected_staging = f".garbage-v1/staging/{self.owner.garbage_id.hex}"
        if self.source_relative_path != self.owner.original_relative_path:
            raise ValueError("prepared intent source conflicts with owner")
        if self.staging_relative_path != expected_staging:
            raise ValueError("prepared intent staging conflicts with owner")
        identity: dict[str, object] = {
            "owner": self.owner.model_dump(mode="json"),
            "schema_version": self.schema_version,
            "source_relative_path": self.source_relative_path,
            "staging_relative_path": self.staging_relative_path,
            "state": self.state,
        }
        if self.created_at is not None:
            identity["created_at"] = self.model_dump(mode="json")["created_at"]
        canonical = _canonical_json(
            identity,
        )
        intent_hash = _sha256_bytes(canonical.encode("utf-8"))
        if self.intent_hash and self.intent_hash != intent_hash:
            raise ValueError("prepared intent hash conflicts with canonical content")
        object.__setattr__(self, "intent_hash", intent_hash)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json", exclude_none=True),
        )


class LabGarbageOrphanMetadata(LabWorkerModel):
    schema_version: Literal[1] = 1
    reason: Literal["no_proven_source"] = "no_proven_source"
    staging_id: UUID
    orphan_token: UUID | None = None
    original_staging_relative_path: str
    orphan_relative_path: str
    expected_device: int | None = Field(default=None, ge=0)
    expected_inode: int | None = Field(default=None, ge=0)
    expected_file_type: Literal["directory"] | None = None
    expected_nlink: int | None = Field(default=None, ge=1)
    expected_empty: Literal[True] | None = None
    metadata_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabGarbageOrphanMetadata:
        expected_source = f".garbage-v1/staging/{self.staging_id.hex}"
        token_suffix = f"-{self.orphan_token.hex}" if self.orphan_token is not None else ""
        expected_orphan = (
            f".garbage-v1/intent_orphans/legacy-empty-staging-{self.staging_id.hex}{token_suffix}"
        )
        if self.original_staging_relative_path != expected_source:
            raise ValueError("orphan metadata source conflicts with staging identity")
        if self.orphan_relative_path != expected_orphan:
            raise ValueError("orphan metadata target conflicts with staging identity")
        identity_fields = (
            self.expected_device,
            self.expected_inode,
            self.expected_file_type,
            self.expected_nlink,
            self.expected_empty,
        )
        if any(value is not None for value in identity_fields) and any(
            value is None for value in identity_fields
        ):
            raise ValueError("orphan metadata expected identity is incomplete")
        canonical_payload: dict[str, object] = {
            "orphan_relative_path": self.orphan_relative_path,
            "original_staging_relative_path": self.original_staging_relative_path,
            "reason": self.reason,
            "schema_version": self.schema_version,
            "staging_id": str(self.staging_id),
        }
        if self.orphan_token is not None:
            canonical_payload["orphan_token"] = str(self.orphan_token)
        if self.expected_device is not None:
            canonical_payload.update(
                {
                    "expected_device": self.expected_device,
                    "expected_empty": self.expected_empty,
                    "expected_file_type": self.expected_file_type,
                    "expected_inode": self.expected_inode,
                    "expected_nlink": self.expected_nlink,
                }
            )
        canonical = _canonical_json(
            canonical_payload,
        )
        metadata_hash = _sha256_bytes(canonical.encode("utf-8"))
        if self.metadata_hash and self.metadata_hash != metadata_hash:
            raise ValueError("orphan metadata hash conflicts with canonical content")
        object.__setattr__(self, "metadata_hash", metadata_hash)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json", exclude_none=True),
        )


class LabGarbageLedger(LabWorkerModel):
    schema_version: Literal[1] = 1
    state: Literal["prepared", "quarantined", "deferred_gc"]
    owner: LabGarbageOwner

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineEntry(LabWorkerModel):
    state: Literal["prepared", "quarantined", "deferred_gc"]
    owner: LabGarbageOwner
    ledger_paths: tuple[Path, ...]
    bundle_path: Path
    retained_bytes: int = Field(ge=0)


class LabQuarantineSummary(LabWorkerModel):
    bundle_count: int = Field(ge=0)
    retained_bytes: int = Field(ge=0)


class LabQuarantineMigrationComplete(LabWorkerModel):
    schema_version: Literal[1] = 1
    state: Literal["complete"] = "complete"
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineMigrationComplete:
        canonical = _canonical_json(
            self.model_dump(mode="json", exclude={"content_hash"}),
        )
        expected = _sha256_bytes(canonical.encode("utf-8"))
        if self.content_hash and self.content_hash != expected:
            raise ValueError("quarantine migration marker hash conflicts")
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineQueueEntry(LabWorkerModel):
    schema_version: Literal[1] = 1
    sequence: int = Field(strict=True, ge=1)
    phase: Literal["active", "cold_health"]
    intent: LabGarbagePreparedIntent
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineQueueEntry:
        expected = _sha256_bytes(
            canonical_json_bytes(
                self.model_dump(mode="json", exclude={"content_hash"}),
            )
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("quarantine queue entry hash conflicts")
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineQueueSequence(LabWorkerModel):
    schema_version: Literal[1] = 1
    last_sequence: int = Field(default=0, strict=True, ge=0)
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineQueueSequence:
        expected = _sha256_bytes(
            canonical_json_bytes(
                self.model_dump(mode="json", exclude={"content_hash"}),
            )
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("quarantine queue sequence hash conflicts")
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineQueueCursor(LabWorkerModel):
    schema_version: Literal[1] = 1
    last_sequence: int = Field(default=0, strict=True, ge=0)
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineQueueCursor:
        expected = _sha256_bytes(
            canonical_json_bytes(
                self.model_dump(mode="json", exclude={"content_hash"}),
            )
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("quarantine queue cursor hash conflicts")
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineQueueConflictObservation(LabWorkerModel):
    location: Literal["pending", "archive"]
    status: Literal["missing", "regular", "symlink", "directory", "other"]
    device: int | None = Field(default=None, ge=0)
    inode: int | None = Field(default=None, ge=1)
    mode: int | None = Field(default=None, ge=0)
    nlink: int | None = Field(default=None, ge=1)
    size: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, pattern=_HASH_PATTERN)
    raw_base64: str | None = None

    @model_validator(mode="after")
    def validate_evidence(self) -> LabQuarantineQueueConflictObservation:
        identity = (self.device, self.inode, self.mode, self.nlink, self.size)
        if self.status == "missing":
            if any(value is not None for value in identity) or any(
                value is not None for value in (self.sha256, self.raw_base64)
            ):
                raise ValueError("missing queue observation cannot contain identity")
            return self
        if any(value is None for value in identity):
            raise ValueError("queue conflict observation requires complete identity")
        if self.raw_base64 is None:
            if self.sha256 is not None:
                raise ValueError("queue conflict hash requires preserved bytes")
            return self
        try:
            payload = base64.b64decode(self.raw_base64, validate=True)
        except Exception as exc:
            raise ValueError("queue conflict bytes are not canonical base64") from exc
        if self.status != "regular" or self.sha256 != _sha256_bytes(payload):
            raise ValueError("queue conflict preserved bytes conflict with identity")
        return self

    @property
    def raw_bytes(self) -> bytes | None:
        if self.raw_base64 is None:
            return None
        return base64.b64decode(self.raw_base64, validate=True)


class LabQuarantineQueueConflict(LabWorkerModel):
    schema_version: Literal[1] = 1
    sequence: int = Field(ge=1)
    reason: Literal[
        "missing_pending",
        "corrupt_pending",
        "corrupt_archived",
        "ambiguous_delivery",
    ]
    pending: LabQuarantineQueueConflictObservation
    archived: LabQuarantineQueueConflictObservation
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineQueueConflict:
        if self.pending.location != "pending" or self.archived.location != "archive":
            raise ValueError("queue conflict observations are mislabelled")
        expected = _sha256_bytes(
            canonical_json_bytes(
                self.model_dump(mode="json", exclude={"content_hash"}),
            )
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("queue conflict hash conflicts")
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineQueueRepairIntent(LabWorkerModel):
    schema_version: Literal[1] = 1
    sequence: int = Field(ge=1)
    phase: Literal["active", "cold_health"]
    intent: LabGarbagePreparedIntent
    conflict_hash: str = Field(pattern=_HASH_PATTERN)
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineQueueRepairIntent:
        expected = _sha256_bytes(
            canonical_json_bytes(
                self.model_dump(mode="json", exclude={"content_hash"}),
            )
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("queue repair intent hash conflicts")
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineQueueRepairResult(LabWorkerModel):
    schema_version: Literal[1] = 1
    sequence: int = Field(ge=1)
    new_sequence: int = Field(ge=1)
    phase: Literal["active", "cold_health"]
    intent_hash: str = Field(pattern=_HASH_PATTERN)
    conflict_hash: str = Field(pattern=_HASH_PATTERN)
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineQueueRepairResult:
        if self.new_sequence <= self.sequence:
            raise ValueError("queue repair must publish a later sequence")
        expected = _sha256_bytes(
            canonical_json_bytes(
                self.model_dump(mode="json", exclude={"content_hash"}),
            )
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("queue repair result hash conflicts")
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineMigrationResult(LabWorkerModel):
    scanned: int = Field(ge=0)
    enqueued: int = Field(ge=0)
    complete: bool


class LabQuarantineMigrationInitializationResult(LabWorkerModel):
    indexed: int = Field(ge=0)
    complete: bool


class LabQuarantineQueueMigrationIndexEntry(LabWorkerModel):
    schema_version: Literal[3] = 3
    index: int = Field(ge=1)
    namespace: Literal["active", "cold_health", "authority"]
    file_name: str = Field(pattern=r"^[0-9a-f]{32}-prepared-intent-v1\.json$")
    previous_chain_hash: str = Field(pattern=_HASH_PATTERN)
    chain_hash: str = ""
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineQueueMigrationIndexEntry:
        chain_hash = _sha256_bytes(
            canonical_json_bytes(
                self.model_dump(mode="json", exclude={"chain_hash", "content_hash"}),
            )
        )
        if self.chain_hash and self.chain_hash != chain_hash:
            raise ValueError("quarantine migration index chain hash conflicts")
        object.__setattr__(self, "chain_hash", chain_hash)
        expected = _sha256_bytes(
            canonical_json_bytes(
                self.model_dump(mode="json", exclude={"content_hash"}),
            )
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("quarantine migration index hash conflicts")
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineQueueMigrationDirectory(LabWorkerModel):
    namespace: Literal["active", "cold_health", "authority"]
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    mode: int = Field(ge=0)
    nlink: int = Field(ge=1)
    mtime_ns: int = Field(ge=0)
    ctime_ns: int = Field(ge=0)


class LabQuarantineQueueMigrationCycle(LabWorkerModel):
    schema_version: Literal[3] = 3
    cycle_id: UUID = Field(default_factory=lambda: UUID(int=0))
    total_entries: int = Field(ge=0)
    index_hash: str = Field(pattern=_HASH_PATTERN)
    directories: tuple[LabQuarantineQueueMigrationDirectory, ...]
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineQueueMigrationCycle:
        namespaces = tuple(item.namespace for item in self.directories)
        if namespaces != ("active", "cold_health", "authority"):
            raise ValueError("quarantine migration directories are incomplete or unordered")
        if self.total_entries == 0 and self.index_hash != _QUEUE_MIGRATION_CHAIN_GENESIS:
            raise ValueError("empty quarantine migration cycle has a non-genesis index hash")
        identity = self.model_dump(mode="json", exclude={"cycle_id", "content_hash"})
        canonical = _canonical_json(
            identity,
        )
        content_hash = _sha256_bytes(canonical.encode("utf-8"))
        cycle_id = uuid5(NAMESPACE_URL, f"rquant:lab-quarantine-migration:{content_hash}")
        if self.content_hash and self.content_hash != content_hash:
            raise ValueError("quarantine migration cycle hash conflicts")
        if self.cycle_id.int and self.cycle_id != cycle_id:
            raise ValueError("quarantine migration cycle identity conflicts")
        object.__setattr__(self, "content_hash", content_hash)
        object.__setattr__(self, "cycle_id", cycle_id)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineQueueMigrationCursor(LabWorkerModel):
    schema_version: Literal[3] = 3
    cycle_id: UUID
    last_index: int = Field(default=0, ge=0)
    last_chain_hash: str = Field(
        default=_QUEUE_MIGRATION_CHAIN_GENESIS,
        pattern=_HASH_PATTERN,
    )
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineQueueMigrationCursor:
        expected = _sha256_bytes(
            canonical_json_bytes(
                self.model_dump(mode="json", exclude={"content_hash"}),
            )
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("quarantine migration cursor hash conflicts")
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineQueueMigrationComplete(LabWorkerModel):
    schema_version: Literal[3] = 3
    state: Literal["complete"] = "complete"
    cycle_id: UUID
    index_hash: str = Field(pattern=_HASH_PATTERN)
    final_index: int = Field(ge=0)
    final_chain_hash: str = Field(pattern=_HASH_PATTERN)
    directories: tuple[LabQuarantineQueueMigrationDirectory, ...]
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_identity(self) -> LabQuarantineQueueMigrationComplete:
        namespaces = tuple(item.namespace for item in self.directories)
        if namespaces != ("active", "cold_health", "authority"):
            raise ValueError("quarantine migration completion directories are incomplete")
        expected = _sha256_bytes(
            canonical_json_bytes(
                self.model_dump(mode="json", exclude={"content_hash"}),
            )
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("quarantine queue migration marker hash conflicts")
        object.__setattr__(self, "content_hash", expected)
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabQuarantineRecoveryResult(LabWorkerModel):
    inspected: int = Field(ge=0)
    reconciled: int = Field(ge=0)
    cold_metadata_checked: int = Field(ge=0)
    queue_conflicts: int = Field(default=0, ge=0)


class LabReclaimLedger(LabWorkerModel):
    schema_version: Literal[2] = 2
    state: Literal["prepared", "isolated", "deferred_gc"]
    current_claim: LabShardClaim
    obsolete_claim: LabShardClaim
    manifest: LabShardResultManifest
    inventory: tuple[LabReclaimInventoryEntry, ...]
    source_name: str = Field(min_length=1)
    tombstone_name: str = Field(min_length=1)
    source_device: int = Field(ge=0)
    source_inode: int = Field(ge=1)
    quarantine_id: UUID | None = None

    @model_validator(mode="after")
    def validate_inventory(self) -> LabReclaimLedger:
        paths = tuple(entry.relative_path for entry in self.inventory)
        expected = ("manifest.json",) + tuple(
            artifact.file_name for artifact in self.manifest.artifacts
        )
        if paths != tuple(sorted(paths)) or set(paths) != set(expected):
            raise ValueError("reclaim inventory must exactly cover manifest files")
        if len(paths) != len(set(paths)):
            raise ValueError("reclaim inventory paths must be unique")
        if self.state == "deferred_gc" and self.quarantine_id is None:
            raise ValueError("deferred reclaim ledger requires quarantine identity")
        return self

    def canonical_json(self) -> str:
        return _canonical_json(
            self.model_dump(mode="json"),
        )


class LabSealedShardBundle(LabWorkerModel):
    path: Path
    manifest: LabShardResultManifest
    created: bool
    device: int = Field(ge=0)
    inode: int = Field(ge=1)


class LabPendingSuccess(LabWorkerModel):
    claim: LabShardClaim | LabShardClaimV2
    report: LabWorkerReport
    bundle: LabSealedShardBundle
    receipt_state: Literal["reported", "awaiting_receipt", "unknown"]

    @model_validator(mode="after")
    def validate_success_identity(self) -> LabPendingSuccess:
        if not isinstance(self.report.body, LabShardSucceeded):
            raise ValueError("pending success must contain shard_succeeded report")
        if self.report.body.result_manifest_hash != self.bundle.manifest.manifest_hash:
            raise ValueError("pending success manifest hash does not match sealed bundle")
        return self


class LabArtifactConflictError(RuntimeError):
    """A sealed shard bundle exists but is not the expected immutable result."""


class LabStopSignal:
    """Cooperative stop flag whose waits are immediately interruptible."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def request(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout_seconds: float) -> bool:
        return self._event.wait(max(0.0, timeout_seconds))


StoreFactory = Callable[[], AbstractContextManager[object]]
ReceiptWaiter = Callable[[LabWorkerReport, float, LabStopSignal], LabReportReceipt]
CodeShaProvider = Callable[[], str | None]
ResourceSnapshotProvider = Callable[[], ResourceSnapshot]
AdmissionPolicyProvider = Callable[[ResearchRunSpec], AdmissionPolicy]
SourceQuotaLeaseProvider = Callable[
    [AdmissionRequest, ResourceSnapshot],
    SourceQuotaLease | None,
]


def _runtime_watermark_state(
    watermark: RuntimeHealthAuthorityWatermark | None,
) -> LabSnapshotAuthorityState | None:
    if watermark is None:
        return None
    return LabSnapshotAuthorityState(
        state_kind="runtime-health-watermark",
        state_json=canonical_json_bytes(watermark.model_dump(mode="json", round_trip=True)).decode(
            "utf-8"
        ),
    )


def _runtime_watermark_from_state(
    state: LabSnapshotAuthorityState | None,
) -> RuntimeHealthAuthorityWatermark | None:
    if state is None:
        return None
    if state.state_kind != "runtime-health-watermark":
        raise LabDaemonConfigurationError("snapshot authority state kind mismatch")
    return RuntimeHealthAuthorityWatermark.model_validate_json(
        state.state_json,
        strict=True,
    )


def _verify_registry_binding(
    binding: LabClosedRegistryBinding,
    *,
    registry_id: str,
    registry_version: int,
    registry_hash: str,
    label: str,
) -> None:
    if binding.registry_id != registry_id or binding.registry_version != registry_version:
        raise LabDaemonConfigurationError(f"{label} registry identity mismatch")
    if binding.registry_hash != registry_hash:
        raise LabDaemonConfigurationError(f"{label} registry hash mismatch")


def _resolve_builtin_authority(
    request: _AuthorityWireRequest,
) -> _AuthorityWireResult:
    binding = request.manifest.registry
    _verify_registry_binding(
        binding,
        registry_id=_BUILTIN_AUTHORITY_REGISTRY_ID,
        registry_version=_BUILTIN_AUTHORITY_REGISTRY_VERSION,
        registry_hash=_BUILTIN_AUTHORITY_REGISTRY_HASH,
        label="resource authority",
    )
    configuration = _BuiltinResourceAuthorityConfig.model_validate_json(
        binding.configuration_json,
        strict=True,
    )
    if request.operation == "policy":
        return _AuthorityWireResult(operation="policy", policy=configuration.policy)
    if request.operation == "quota":
        if request.admission_request is None or request.snapshot is None:
            raise LabDaemonConfigurationError("quota authority request is incomplete")
        return _AuthorityWireResult(operation="quota")

    watermark = _runtime_watermark_from_state(request.authority_state)
    spawned = LocalResourceSnapshotProvider(
        disk_path=configuration.snapshot.disk_path,
        clock=_runtime_resource_system_clock,
        probe=SystemResourceProbe(),
        live_slo_config=configuration.snapshot.live_slo_config,
        session_resolver=RuntimeTradeCalendarSessionResolver(
            configuration.snapshot.market_calendar
        ),
    ).spawn_probe_provider()
    spawned.authority_watermark = watermark
    snapshot = spawned()
    return _AuthorityWireResult(
        operation=request.operation,
        policy=(configuration.policy if request.operation == "admission" else None),
        snapshot=snapshot,
        quota_lease=None,
        authority_state=_runtime_watermark_state(spawned.export_probe_state()),
    )


def _resolve_test_authority(request: _AuthorityWireRequest) -> _AuthorityWireResult:
    binding = request.manifest.registry
    _verify_registry_binding(
        binding,
        registry_id=_TEST_AUTHORITY_REGISTRY_ID,
        registry_version=_TEST_AUTHORITY_REGISTRY_VERSION,
        registry_hash=_TEST_AUTHORITY_REGISTRY_HASH,
        label="test resource authority",
    )
    from tests.lab_worker_authority_fixture import evaluate_lab_authority_fixture

    raw = evaluate_lab_authority_fixture(
        strict_canonical_json_loads(binding.configuration_json),
        operation=request.operation,
        spec=request.spec,
        admission_request=request.admission_request,
        snapshot=request.snapshot,
        authority_state=request.authority_state,
    )
    return _AuthorityWireResult.model_validate(raw, strict=True)


def _resolve_resource_journal_authority(
    request: _AuthorityWireRequest,
) -> _AuthorityWireResult:
    binding = request.manifest.registry
    _verify_registry_binding(
        binding,
        registry_id=LAB_RESOURCE_AUTHORITY_REGISTRY_ID,
        registry_version=LAB_RESOURCE_AUTHORITY_REGISTRY_VERSION,
        registry_hash=LAB_RESOURCE_AUTHORITY_REGISTRY_HASH,
        label="resource journal authority",
    )
    configuration = ResourceAuthorityAdapterConfig.model_validate_json(
        binding.configuration_json,
        strict=True,
    )
    client = ResourceAuthorityJournalClient(configuration)
    operation_id = hashlib.sha256(
        canonical_json_bytes(
            {
                "admission_request": (
                    None
                    if request.admission_request is None
                    else request.admission_request.model_dump(mode="json")
                ),
                "contract": "rquant-lab-resource-authority-stage/v2",
                "operation": request.operation,
                "spec": None if request.spec is None else request.spec.model_dump(mode="json"),
            }
        )
    ).hexdigest()
    if request.operation == "policy":
        return _AuthorityWireResult(
            operation="policy", policy=client.policy(operation_id=operation_id)
        )
    if request.operation == "snapshot":
        return _AuthorityWireResult(
            operation="snapshot", snapshot=client.snapshot(operation_id=operation_id)
        )
    if request.operation == "admission":
        policy, snapshot = client.admission(operation_id=operation_id)
        return _AuthorityWireResult(
            operation="admission",
            policy=policy,
            snapshot=snapshot,
        )
    raise LabDaemonConfigurationError(
        "resource journal authority does not provide source quota leases"
    )


def _resolve_authority(request: _AuthorityWireRequest) -> _AuthorityWireResult:
    registry_id = request.manifest.registry.registry_id
    if registry_id == _BUILTIN_AUTHORITY_REGISTRY_ID:
        return _resolve_builtin_authority(request)
    if registry_id == _TEST_AUTHORITY_REGISTRY_ID:
        return _resolve_test_authority(request)
    if registry_id == LAB_RESOURCE_AUTHORITY_REGISTRY_ID:
        return _resolve_resource_journal_authority(request)
    raise LabDaemonConfigurationError("resource authority registry is not registered")


def _research_execution_session_factory(
    binding: DatasetSnapshotBinding,
    lake_root: Path,
) -> AbstractContextManager[object]:
    return ResearchExecutionSession(binding=binding, lake_root=lake_root)


def _resolve_shard_result(request: _ShardWireRequest) -> LabShardExecutionResult:
    binding = request.manifest.registry
    if binding.registry_id == _BUILTIN_SHARD_REGISTRY_ID:
        _verify_registry_binding(
            binding,
            registry_id=_BUILTIN_SHARD_REGISTRY_ID,
            registry_version=_BUILTIN_SHARD_REGISTRY_VERSION,
            registry_hash=_BUILTIN_SHARD_REGISTRY_HASH,
            label="shard runtime",
        )
        from rquant.lab_worker_registry import execute_builtin_lab_shard

        return execute_builtin_lab_shard(
            strict_canonical_json_loads(binding.configuration_json),
            request.validated,
            runtime_code_sha=request.runtime_code_sha,
        )
    if binding.registry_id == _TEST_SHARD_REGISTRY_ID:
        _verify_registry_binding(
            binding,
            registry_id=_TEST_SHARD_REGISTRY_ID,
            registry_version=_TEST_SHARD_REGISTRY_VERSION,
            registry_hash=_TEST_SHARD_REGISTRY_HASH,
            label="test shard runtime",
        )
        from tests.lab_worker_shard_fixture import execute_lab_shard_fixture

        return execute_lab_shard_fixture(
            strict_canonical_json_loads(binding.configuration_json),
            request.validated,
            runtime_code_sha=request.runtime_code_sha,
        )
    raise LabDaemonConfigurationError("shard runtime registry is not registered")


def _connect_wire_child(address: str, authkey: bytes) -> Connection:
    return Client(address, family="AF_UNIX", authkey=authkey)


def _validate_outbound_wire_size(
    payload_size: int,
    *,
    max_bytes: int,
    label: str,
) -> None:
    if type(payload_size) is not int or payload_size < 0 or payload_size > max_bytes:
        raise LabDaemonConfigurationError(f"{label} exceeds the outbound wire size limit")


class _DeadlineWireEndpoint:
    """One accepted AF_UNIX stream with deadline-aware framed I/O."""

    def __init__(self, accepted: socket.socket) -> None:
        accepted.setblocking(False)
        self._socket = accepted
        self._selector = selectors.DefaultSelector()
        self._selector.register(accepted, selectors.EVENT_READ)
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def fileno(self) -> int:
        return self._socket.fileno()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        selector_error: BaseException | None = None
        try:
            self._selector.close()
        except BaseException as exc:
            selector_error = exc
        try:
            self._socket.close()
        except BaseException:
            if selector_error is None:
                raise
        if selector_error is not None:
            raise selector_error

    def _wait(
        self,
        events: int,
        *,
        deadline_microseconds: int | None,
        cancel_requested: Callable[[], bool] | None,
        label: str,
    ) -> None:
        while True:
            if cancel_requested is not None and cancel_requested():
                raise InterruptedError(f"{label} cancelled")
            timeout = _microseconds_to_seconds(_RESOURCE_AUTHORITY_POLL_MICROSECONDS)
            if deadline_microseconds is not None:
                remaining = deadline_microseconds - _monotonic_microseconds()
                if remaining <= 0:
                    raise TimeoutError(f"{label} timed out")
                timeout = _microseconds_to_seconds(
                    min(_RESOURCE_AUTHORITY_POLL_MICROSECONDS, remaining)
                )
            self._selector.modify(self._socket, events)
            if self._selector.select(timeout):
                return

    def _send_exact(
        self,
        payload: bytes | bytearray | memoryview,
        *,
        deadline_microseconds: int | None,
        cancel_requested: Callable[[], bool] | None,
        label: str,
    ) -> None:
        view = memoryview(payload)
        sent = 0
        try:
            while sent < len(view):
                self._wait(
                    selectors.EVENT_WRITE,
                    deadline_microseconds=deadline_microseconds,
                    cancel_requested=cancel_requested,
                    label=label,
                )
                try:
                    count = self._socket.send(view[sent:])
                except BlockingIOError:
                    continue
                if count == 0:
                    raise EOFError(f"{label} peer closed during send")
                sent += count
        except BaseException:
            with suppress(BaseException):
                self.close()
            raise

    def _recv_exact(
        self,
        size: int,
        *,
        deadline_microseconds: int | None,
        cancel_requested: Callable[[], bool] | None,
        label: str,
    ) -> bytes:
        payload = bytearray()
        try:
            while len(payload) < size:
                self._wait(
                    selectors.EVENT_READ,
                    deadline_microseconds=deadline_microseconds,
                    cancel_requested=cancel_requested,
                    label=label,
                )
                try:
                    chunk = self._socket.recv(size - len(payload))
                except BlockingIOError:
                    continue
                if not chunk:
                    raise EOFError(f"{label} peer closed during receive")
                payload.extend(chunk)
        except BaseException:
            with suppress(BaseException):
                self.close()
            raise
        return bytes(payload)

    def send_bytes(
        self,
        payload: bytes,
        *,
        deadline_microseconds: int | None = None,
        cancel_requested: Callable[[], bool] | None = None,
        label: str = "wire send",
    ) -> None:
        size = len(payload)
        header = struct.pack("!iQ", -1, size) if size > 0x7FFFFFFF else struct.pack("!i", size)
        self._send_exact(
            header,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label=f"{label} header",
        )
        self._send_exact(
            payload,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label=f"{label} payload",
        )

    def recv_bytes(
        self,
        maxlength: int | None = None,
        *,
        deadline_microseconds: int | None = None,
        cancel_requested: Callable[[], bool] | None = None,
        label: str = "wire receive",
    ) -> bytes:
        header = self._recv_exact(
            4,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label=f"{label} header",
        )
        size = struct.unpack("!i", header)[0]
        if size == -1:
            extended = self._recv_exact(
                8,
                deadline_microseconds=deadline_microseconds,
                cancel_requested=cancel_requested,
                label=f"{label} extended header",
            )
            size = struct.unpack("!Q", extended)[0]
        if size < 0:
            with suppress(BaseException):
                self.close()
            raise OSError(f"{label} has an invalid frame length")
        if maxlength is not None and size > maxlength:
            with suppress(BaseException):
                self.close()
            raise OSError(f"{label} exceeds the inbound wire size limit")
        return self._recv_exact(
            size,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label=f"{label} payload",
        )

    def poll(self, timeout: float = 0.0) -> bool:
        if self._closed:
            raise OSError("wire endpoint is closed")
        self._selector.modify(self._socket, selectors.EVENT_READ)
        return bool(self._selector.select(max(0.0, timeout)))

    def authenticate_server(
        self,
        authkey: bytes,
        *,
        deadline_microseconds: int,
        cancel_requested: Callable[[], bool] | None,
    ) -> None:
        challenge = _WIRE_DIGEST_PREFIX + os.urandom(_WIRE_CHALLENGE_BYTES)
        self.send_bytes(
            _WIRE_CHALLENGE + challenge,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label="wire authentication challenge",
        )
        response = self.recv_bytes(
            256,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label="wire authentication response",
        )
        expected = _WIRE_DIGEST_PREFIX + hmac.new(authkey, challenge, "sha256").digest()
        if not hmac.compare_digest(expected, response):
            with suppress(BaseException):
                self.send_bytes(
                    _WIRE_FAILURE,
                    deadline_microseconds=deadline_microseconds,
                    cancel_requested=cancel_requested,
                    label="wire authentication rejection",
                )
            raise AuthenticationError("wire authentication digest was wrong")
        self.send_bytes(
            _WIRE_WELCOME,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label="wire authentication welcome",
        )

        peer_challenge_frame = self.recv_bytes(
            256,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label="wire mutual authentication challenge",
        )
        if not peer_challenge_frame.startswith(_WIRE_CHALLENGE):
            raise AuthenticationError("wire mutual authentication challenge was malformed")
        peer_challenge = peer_challenge_frame[len(_WIRE_CHALLENGE) :]
        if not peer_challenge.startswith(_WIRE_DIGEST_PREFIX):
            raise AuthenticationError("wire mutual authentication digest is not SHA-256")
        peer_response = (
            _WIRE_DIGEST_PREFIX
            + hmac.new(
                authkey,
                peer_challenge,
                "sha256",
            ).digest()
        )
        self.send_bytes(
            peer_response,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label="wire mutual authentication response",
        )
        welcome = self.recv_bytes(
            256,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label="wire mutual authentication welcome",
        )
        if welcome != _WIRE_WELCOME:
            raise AuthenticationError("wire mutual authentication response was rejected")


def _send_wire(
    connection: Connection | _DeadlineWireEndpoint,
    value: LabWireModel,
    *,
    max_bytes: int = _MAX_CONTROL_WIRE_BYTES,
    label: str = "wire message",
    deadline_microseconds: int | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> None:
    payload = _encode_wire_message(value)
    _validate_outbound_wire_size(len(payload), max_bytes=max_bytes, label=label)
    if isinstance(connection, _DeadlineWireEndpoint):
        connection.send_bytes(
            payload,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancel_requested,
            label=label,
        )
    else:
        connection.send_bytes(payload)


def _recv_wire(
    connection: Connection | _DeadlineWireEndpoint,
    *,
    model: type[WireModelT],
    max_bytes: int,
    label: str,
    deadline_microseconds: int | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> WireModelT:
    try:
        if isinstance(connection, _DeadlineWireEndpoint):
            payload = connection.recv_bytes(
                maxlength=max_bytes,
                deadline_microseconds=deadline_microseconds,
                cancel_requested=cancel_requested,
                label=label,
            )
        else:
            payload = connection.recv_bytes(maxlength=max_bytes)
    except (InterruptedError, TimeoutError):
        raise
    except (EOFError, OSError) as exc:
        raise LabDaemonConfigurationError(f"{label} transport failed") from exc
    return _decode_wire_message(
        payload,
        model=model,
        max_bytes=max_bytes,
        label=label,
    )


class LabWireSessionStartupError(LabDaemonConfigurationError):
    """A private AF_UNIX session could not be created safely."""


class LabWireSessionError(RuntimeError):
    """An authenticated wire session failed after its start ACK."""


def _extract_wire_session_error(
    error: BaseException,
) -> LabWireSessionStartupError | LabWireSessionError | None:
    if isinstance(error, (LabWireSessionStartupError, LabWireSessionError)):
        return error
    if not isinstance(error, BaseExceptionGroup):
        return None
    for nested in error.exceptions:
        extracted = _extract_wire_session_error(nested)
        if extracted is not None:
            return extracted
    return None


def _classify_wire_failure(error: Exception) -> _ClassifiedWireFailure:
    wire_error = _extract_wire_session_error(error)
    if wire_error is None:
        raise ValueError("wire failure classification requires a wire session error")
    return _ClassifiedWireFailure(
        failure_kind=(
            "session_startup" if isinstance(wire_error, LabWireSessionStartupError) else "session"
        ),
        error=error,
    )


@dataclass(frozen=True)
class _WireFilesystemIdentity:
    device: int
    inode: int

    @classmethod
    def from_stat(cls, observed: os.stat_result) -> _WireFilesystemIdentity:
        return cls(device=observed.st_dev, inode=observed.st_ino)

    def matches(self, observed: os.stat_result) -> bool:
        return (observed.st_dev, observed.st_ino) == (self.device, self.inode)


class _RawWireListener:
    """A minimal authenticated listener without Listener's path finalizer."""

    def __init__(self, socket_listener: socket.socket, authkey: bytes) -> None:
        socket_listener.setblocking(False)
        self._socket = socket_listener
        self._authkey = authkey
        self._selector = selectors.DefaultSelector()
        self._selector.register(socket_listener, selectors.EVENT_READ)
        self._closed = False

    def fileno(self) -> int:
        return self._socket.fileno()

    def accept(
        self,
        *,
        deadline_microseconds: int | None = None,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> _DeadlineWireEndpoint:
        accepted: socket.socket | None = None
        try:
            while accepted is None:
                if cancel_requested is not None and cancel_requested():
                    raise InterruptedError("wire listener accept cancelled")
                timeout = _microseconds_to_seconds(_RESOURCE_AUTHORITY_POLL_MICROSECONDS)
                if deadline_microseconds is not None:
                    remaining = deadline_microseconds - _monotonic_microseconds()
                    if remaining <= 0:
                        raise TimeoutError("wire listener accept timed out")
                    timeout = _microseconds_to_seconds(
                        min(_RESOURCE_AUTHORITY_POLL_MICROSECONDS, remaining)
                    )
                if not self._selector.select(timeout):
                    continue
                try:
                    accepted, _address = self._socket.accept()
                except BlockingIOError:
                    continue
            endpoint = _DeadlineWireEndpoint(accepted)
            accepted = None
            endpoint.authenticate_server(
                self._authkey,
                deadline_microseconds=(
                    deadline_microseconds if deadline_microseconds is not None else 2**63 - 1
                ),
                cancel_requested=cancel_requested,
            )
        except BaseException:
            if accepted is not None:
                accepted.close()
            if "endpoint" in locals():
                with suppress(BaseException):
                    endpoint.close()
            raise
        return endpoint

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._selector.close()
        self._socket.close()


@dataclass
class _WireSession:
    root_path: Path
    path: Path
    endpoint: Path
    root_fd: int
    session_fd: int
    root_identity: _WireFilesystemIdentity
    session_identity: _WireFilesystemIdentity
    endpoint_identity: _WireFilesystemIdentity
    listener: _RawWireListener
    authkey: bytes
    _cleaned: bool = False

    @property
    def address(self) -> str:
        return str(self.endpoint)

    def cleanup(self) -> None:
        if self._cleaned:
            return
        self._cleaned = True
        _cleanup_wire_session_identity(
            close_listener=self.listener.close,
            root_path=self.root_path,
            session_name=self.path.name,
            endpoint_name=self.endpoint.name,
            root_fd=self.root_fd,
            session_fd=self.session_fd,
            root_identity=self.root_identity,
            session_identity=self.session_identity,
            endpoint_identity=self.endpoint_identity,
        )


@dataclass
class _ProvisionalWireSessionOwner:
    root_path: Path
    session_name: str
    endpoint_name: str
    root_fd: int
    session_fd: int
    root_identity: _WireFilesystemIdentity
    session_identity: _WireFilesystemIdentity
    endpoint_identity: _WireFilesystemIdentity
    listener_socket: socket.socket
    _cleaned: bool = False

    def cleanup(self) -> None:
        if self._cleaned:
            return
        self._cleaned = True
        _cleanup_wire_session_identity(
            close_listener=self.listener_socket.close,
            root_path=self.root_path,
            session_name=self.session_name,
            endpoint_name=self.endpoint_name,
            root_fd=self.root_fd,
            session_fd=self.session_fd,
            root_identity=self.root_identity,
            session_identity=self.session_identity,
            endpoint_identity=self.endpoint_identity,
        )


def _cleanup_wire_session_identity(
    *,
    close_listener: Callable[[], None],
    root_path: Path,
    session_name: str,
    endpoint_name: str,
    root_fd: int,
    session_fd: int,
    root_identity: _WireFilesystemIdentity,
    session_identity: _WireFilesystemIdentity,
    endpoint_identity: _WireFilesystemIdentity,
) -> None:
    errors: list[BaseException] = []
    try:
        close_listener()
    except BaseException as exc:
        errors.append(exc)

    def session_path_is_original() -> bool:
        try:
            root_observed = os.stat(root_path, follow_symlinks=False)
            session_observed = os.stat(
                session_name,
                dir_fd=root_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return False
        return (
            stat.S_ISDIR(root_observed.st_mode)
            and root_identity.matches(root_observed)
            and stat.S_ISDIR(session_observed.st_mode)
            and session_identity.matches(session_observed)
        )

    try:
        if session_path_is_original():
            try:
                endpoint = os.stat(
                    endpoint_name,
                    dir_fd=session_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                endpoint = None
            endpoint_removed_or_absent = endpoint is None
            if (
                endpoint is not None
                and stat.S_ISSOCK(endpoint.st_mode)
                and endpoint_identity.matches(endpoint)
            ):
                os.unlink(endpoint_name, dir_fd=session_fd)
                endpoint_removed_or_absent = True
            if (
                endpoint_removed_or_absent
                and session_path_is_original()
                and not os.listdir(session_fd)
            ):
                os.rmdir(session_name, dir_fd=root_fd)
    except BaseException as exc:
        errors.append(exc)
    finally:
        for descriptor in (session_fd, root_fd):
            try:
                os.close(descriptor)
            except BaseException as exc:
                errors.append(exc)
    if errors:
        raise BaseExceptionGroup("wire session cleanup failed", errors)


def _wire_root_candidates(roots: tuple[Path, ...] | None) -> tuple[Path, ...]:
    candidates = roots or (
        Path(tempfile.gettempdir()),
        Path("/tmp"),
        Path("/private/tmp"),
    )
    unique: list[Path] = []
    for candidate in candidates:
        normalized = Path(candidate)
        if normalized not in unique:
            unique.append(normalized)
    return tuple(unique)


def _open_wire_root(root: Path) -> tuple[int, _WireFilesystemIdentity]:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(root, flags)
    try:
        observed = os.fstat(descriptor)
        path_observed = os.stat(root, follow_symlinks=False)
        mode = stat.S_IMODE(observed.st_mode)
        current_uid = os.geteuid()
        shared_root = bool(observed.st_mode & stat.S_ISVTX)
        root_is_safe = (
            stat.S_ISDIR(observed.st_mode)
            and _WireFilesystemIdentity.from_stat(observed).matches(path_observed)
            and observed.st_uid in {current_uid, 0}
            and (
                (shared_root and observed.st_uid in {current_uid, 0})
                or (not shared_root and observed.st_uid == current_uid and mode == 0o700)
            )
        )
        if not root_is_safe:
            raise LabWireSessionStartupError("wire session root is not a safe directory")
        return descriptor, _WireFilesystemIdentity.from_stat(observed)
    except BaseException:
        os.close(descriptor)
        raise


def _open_wire_session_directory(
    root_fd: int,
    session_name: str,
) -> tuple[int, _WireFilesystemIdentity]:
    os.mkdir(session_name, mode=0o700, dir_fd=root_fd)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(session_name, flags, dir_fd=root_fd)
    try:
        os.fchown(descriptor, os.geteuid(), os.getegid())
        os.fchmod(descriptor, 0o700)
        observed = os.fstat(descriptor)
        path_observed = os.stat(session_name, dir_fd=root_fd, follow_symlinks=False)
        if (
            not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_gid != os.getegid()
            or stat.S_IMODE(observed.st_mode) != 0o700
            or not _WireFilesystemIdentity.from_stat(observed).matches(path_observed)
        ):
            raise LabWireSessionStartupError("wire session directory identity validation failed")
        return descriptor, _WireFilesystemIdentity.from_stat(observed)
    except BaseException:
        os.close(descriptor)
        raise


def _new_wire_session(*, roots: tuple[Path, ...] | None = None) -> _WireSession:
    session_name = f"rqlw-{uuid4().hex}"
    errors: list[BaseException] = []
    for root in _wire_root_candidates(roots):
        root_fd: int | None = None
        session_fd: int | None = None
        listener_socket: socket.socket | None = None
        provisional: _ProvisionalWireSessionOwner | None = None
        session: _WireSession | None = None
        try:
            root_fd, root_identity = _open_wire_root(root)
            session_fd, session_identity = _open_wire_session_directory(root_fd, session_name)
            path = root / session_name
            endpoint = path / "wire.sock"
            listener_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener_socket.bind(str(endpoint))
            provisional_endpoint_stat = os.stat(
                endpoint.name,
                dir_fd=session_fd,
                follow_symlinks=False,
            )
            provisional = _ProvisionalWireSessionOwner(
                root_path=root,
                session_name=session_name,
                endpoint_name=endpoint.name,
                root_fd=root_fd,
                session_fd=session_fd,
                root_identity=root_identity,
                session_identity=session_identity,
                endpoint_identity=_WireFilesystemIdentity.from_stat(provisional_endpoint_stat),
                listener_socket=listener_socket,
            )
            os.chown(endpoint, os.geteuid(), os.getegid(), follow_symlinks=False)
            os.chmod(endpoint, 0o600, follow_symlinks=False)
            endpoint_stat = os.stat(
                endpoint.name,
                dir_fd=session_fd,
                follow_symlinks=False,
            )
            authkey = os.urandom(32)
            # macOS socket fstat() identifies the socket object, not its filesystem vnode.
            if (
                not stat.S_ISSOCK(endpoint_stat.st_mode)
                or endpoint_stat.st_uid != os.geteuid()
                or endpoint_stat.st_gid != os.getegid()
                or stat.S_IMODE(endpoint_stat.st_mode) != 0o600
                or not provisional.endpoint_identity.matches(endpoint_stat)
            ):
                raise LabWireSessionStartupError("wire endpoint identity validation failed")
            listener_socket.listen(1)
            listener = _RawWireListener(listener_socket, authkey)
            session = _WireSession(
                root_path=root,
                path=path,
                endpoint=endpoint,
                root_fd=root_fd,
                session_fd=session_fd,
                root_identity=root_identity,
                session_identity=session_identity,
                endpoint_identity=provisional.endpoint_identity,
                listener=listener,
                authkey=authkey,
            )
            provisional._cleaned = True
            return session
        except BaseException as exc:
            if session is not None:
                try:
                    session.cleanup()
                except BaseException as cleanup_error:
                    exc = BaseExceptionGroup(
                        "wire session startup and cleanup failed",
                        [exc, cleanup_error],
                    )
            elif provisional is not None:
                try:
                    provisional.cleanup()
                except BaseException as cleanup_error:
                    exc = BaseExceptionGroup(
                        "wire session startup and cleanup failed",
                        [exc, cleanup_error],
                    )
            else:
                if listener_socket is not None:
                    with suppress(BaseException):
                        listener_socket.close()
                if session_fd is not None:
                    with suppress(BaseException):
                        os.close(session_fd)
                if root_fd is not None:
                    with suppress(BaseException):
                        with suppress(FileNotFoundError):
                            os.rmdir(session_name, dir_fd=root_fd)
                        os.close(root_fd)
            errors.append(exc)
    raise LabWireSessionStartupError(
        "could not create a private wire session"
    ) from BaseExceptionGroup("wire session startup failures", errors)


def _authority_wire_child(
    request_bytes: bytes,
    address: str,
    authkey: bytes,
    max_wire_bytes: int,
) -> None:
    connection: Connection | None = None
    try:
        child_pid = os.getpid()
        os.setsid()
        connection = _connect_wire_child(address, authkey)
        request = _decode_wire_message(
            request_bytes,
            model=_AuthorityWireRequest,
            max_bytes=max_wire_bytes,
            label="authority request",
        )
        _send_wire(
            connection,
            _IsolationReadiness(
                ready=True,
                child_pid=child_pid,
                group_id=os.getpgrp(),
            ),
        )
        acknowledgement = _recv_wire(
            connection,
            model=_IsolationStartAck,
            max_bytes=max_wire_bytes,
            label="authority start acknowledgement",
        )
        if not acknowledgement.accepted:
            return
        deadline = acknowledgement.not_after_monotonic_microseconds
        if acknowledgement.execution_limit_microseconds is not None:
            execution_deadline = (
                _monotonic_microseconds() + acknowledgement.execution_limit_microseconds
            )
            deadline = execution_deadline if deadline is None else min(deadline, execution_deadline)
        if deadline is not None and _monotonic_microseconds() >= deadline:
            return
        try:
            result = _resolve_authority(request)
        except BaseException as exc:
            result = _AuthorityWireResult(
                operation=request.operation,
                error_type=type(exc).__name__,
                message=_bounded_exception_message(exc),
            )
        _send_wire(connection, result)
    except BaseException as exc:
        if connection is not None:
            with suppress(BrokenPipeError, EOFError, OSError):
                _send_wire(
                    connection,
                    _IsolationReadiness(
                        ready=False,
                        child_pid=os.getpid(),
                        error_type=type(exc).__name__,
                        message=_bounded_exception_message(exc),
                    ),
                )
    finally:
        if connection is not None:
            connection.close()


def _prepare_test_shard_fixture(request: _ShardWireRequest) -> None:
    if request.manifest.registry.registry_id != _TEST_SHARD_REGISTRY_ID:
        return
    from tests.lab_worker_shard_fixture import prepare_lab_shard_fixture

    prepare_lab_shard_fixture(
        strict_canonical_json_loads(request.manifest.registry.configuration_json)
    )


def _shard_wire_child(
    request_bytes: bytes,
    address: str,
    authkey: bytes,
    max_wire_bytes: int,
) -> None:
    connection: Connection | None = None
    request: _ShardWireRequest | None = None
    try:
        child_pid = os.getpid()
        os.setsid()
        connection = _connect_wire_child(address, authkey)
        request = _decode_wire_message(
            request_bytes,
            model=_ShardWireRequest,
            max_bytes=max_wire_bytes,
            label="shard request",
        )
        _prepare_test_shard_fixture(request)
        _send_wire(
            connection,
            _IsolationReadiness(
                ready=True,
                child_pid=child_pid,
                group_id=os.getpgrp(),
            ),
        )
        acknowledgement = _recv_wire(
            connection,
            model=_IsolationStartAck,
            max_bytes=max_wire_bytes,
            label="shard start acknowledgement",
        )
        if not acknowledgement.accepted:
            return
        deadline = acknowledgement.not_after_monotonic_microseconds
        if deadline is not None and _monotonic_microseconds() >= deadline:
            return
        try:
            result = _resolve_shard_result(request)
            if deadline is not None and _monotonic_microseconds() >= deadline:
                raise TimeoutError("isolated shard deadline reached before result publication")
            outcome = _IsolatedExecutionWireOutcome(
                result=LabShardExecutionWireResult.from_result(result)
            )
        except PermissionError as exc:
            outcome = _IsolatedExecutionWireOutcome(
                phase="session",
                error_type=type(exc).__name__,
                message=_bounded_exception_message(exc),
            )
        except LabDaemonConfigurationError as exc:
            outcome = _IsolatedExecutionWireOutcome(
                phase="session",
                error_type=type(exc).__name__,
                message=_bounded_exception_message(exc),
                configuration_error=True,
            )
        except BaseException as exc:
            outcome = _IsolatedExecutionWireOutcome(
                phase="execute",
                error_type=type(exc).__name__,
                message=_bounded_exception_message(exc),
            )
        _send_wire(
            connection,
            outcome,
            max_bytes=MAX_RESULT_WIRE_BYTES,
            label="isolated shard outcome",
        )
    except BaseException as exc:
        if connection is not None:
            with suppress(BrokenPipeError, EOFError, OSError):
                _send_wire(
                    connection,
                    _IsolationReadiness(
                        ready=False,
                        child_pid=os.getpid(),
                        error_type=type(exc).__name__,
                        message=_bounded_exception_message(exc),
                    ),
                )
    finally:
        if connection is not None:
            connection.close()


class LabWorker:
    def __init__(
        self,
        *,
        worker_id: str,
        claim_spool: LabClaimSpool,
        claim_publication_verifier: LabClaimPublicationWorkerVerifier | None = None,
        v2_claim_publication_enabled: bool = False,
        report_spool: LabReportSpool,
        artifact_root: Path,
        adapter_registry: StrategyJobAdapterRegistry | None = None,
        exploratory_store_factory: StoreFactory | None = None,
        metadata_store_factory: StoreFactory | None = None,
        research_lake_root: Path | None = None,
        heartbeat_interval_seconds: float = 30.0,
        resource_recheck_interval_seconds: float = 1.0,
        resource_probe_timeout_seconds: float = 1.0,
        lease_extension_seconds: int = 120,
        poll_interval_ms: int = 250,
        receipt_timeout_seconds: float = 30.0,
        quarantine_reconcile_interval_seconds: float = 300.0,
        receipt_waiter: ReceiptWaiter | None = None,
        verified_code_sha_provider: CodeShaProvider | None = None,
        resource_snapshot_provider: ResourceSnapshotProvider | None = None,
        admission_policy_provider: AdmissionPolicyProvider | None = None,
        source_quota_lease_provider: SourceQuotaLeaseProvider | None = None,
        resource_reservation_store: PersistentResourceReservationStore | None = None,
        require_resource_admission: bool = False,
        clock: Callable[[], datetime] = _system_clock,
        monotonic_clock: Callable[[], float] = time.monotonic,
        isolation_monotonic_clock: Callable[[], float] = time.monotonic,
        isolation_session_initializer: Callable[[], None] | None = None,
        execution_session_factory: Callable[
            [DatasetSnapshotBinding, Path], AbstractContextManager[object]
        ] = _research_execution_session_factory,
        research_store_opener: Callable[
            ..., AbstractContextManager[tuple[object, object]]
        ] = open_gated_research_store,
        shard_runtime_manifest: LabShardRuntimeManifest | None = None,
        resource_authority_manifest: LabResourceAuthorityManifest | None = None,
        production_mode: bool = False,
    ) -> None:
        normalized_worker_id = worker_id.strip()
        if not normalized_worker_id:
            raise ValueError("worker_id must not be empty")
        heartbeat_interval_microseconds = _positive_duration_microseconds(
            heartbeat_interval_seconds,
            label="heartbeat_interval_seconds",
        )
        resource_recheck_interval_microseconds = _positive_duration_microseconds(
            resource_recheck_interval_seconds,
            label="resource_recheck_interval_seconds",
        )
        resource_probe_timeout_microseconds = _positive_duration_microseconds(
            resource_probe_timeout_seconds,
            label="resource_probe_timeout_seconds",
        )
        receipt_timeout_microseconds = _positive_duration_microseconds(
            receipt_timeout_seconds,
            label="receipt_timeout_seconds",
        )
        quarantine_reconcile_interval_microseconds = _positive_duration_microseconds(
            quarantine_reconcile_interval_seconds,
            label="quarantine_reconcile_interval_seconds",
        )
        if (
            isinstance(lease_extension_seconds, bool)
            or not isinstance(lease_extension_seconds, int)
            or lease_extension_seconds < 1
            or lease_extension_seconds > 3_600
        ):
            raise ValueError("lease_extension_seconds must be from 1 through 3600")
        if isinstance(poll_interval_ms, bool) or not isinstance(poll_interval_ms, int):
            raise ValueError("poll_interval_ms must be a positive integer")
        if poll_interval_ms < 1:
            raise ValueError("poll_interval_ms must be positive")
        if not isinstance(require_resource_admission, bool):
            raise ValueError("require_resource_admission must be a boolean")
        if not isinstance(v2_claim_publication_enabled, bool):
            raise ValueError("v2_claim_publication_enabled must be a boolean")
        if v2_claim_publication_enabled and claim_publication_verifier is None:
            raise LabDaemonConfigurationError(
                "V2 claim publication requires a published-claim verifier"
            )
        closed_adapter_registry = default_strategy_job_adapter_registry()
        if adapter_registry is not None and adapter_registry is not closed_adapter_registry:
            raise LabDaemonConfigurationError(
                "legacy or third-party adapter registry is not registered"
            )
        custom_shard_runtime = (
            any(
                value is not None
                for value in (
                    exploratory_store_factory,
                    metadata_store_factory,
                    research_lake_root,
                    isolation_session_initializer,
                )
            )
            or execution_session_factory is not _research_execution_session_factory
            or (research_store_opener is not open_gated_research_store)
        )
        if custom_shard_runtime:
            raise LabDaemonConfigurationError(
                "legacy shard callbacks require a closed shard runtime manifest"
            )
        legacy_admission_providers = (
            resource_snapshot_provider,
            admission_policy_provider,
            source_quota_lease_provider,
        )
        if type(production_mode) is not bool:
            raise TypeError("worker production_mode must be bool")
        if production_mode and any(provider is not None for provider in legacy_admission_providers):
            raise LabDaemonConfigurationError(
                "production worker requires an explicit V2 resource authority manifest"
            )
        if resource_authority_manifest is not None and any(
            provider is not None for provider in legacy_admission_providers
        ):
            raise LabDaemonConfigurationError(
                "closed resource authority manifest conflicts with legacy providers"
            )
        if (
            not production_mode
            and resource_authority_manifest is None
            and any(provider is not None for provider in legacy_admission_providers)
        ):
            if resource_snapshot_provider is None or admission_policy_provider is None:
                raise LabDaemonConfigurationError(
                    "resource admission providers must be configured together"
                )
            resource_authority_manifest = build_builtin_resource_authority_manifest(
                resource_snapshot_provider,
                admission_policy_provider,
                source_quota_lease_provider,
            )
        if production_mode and (
            resource_authority_manifest is None
            or (
                resource_authority_manifest.registry.registry_id,
                resource_authority_manifest.registry.registry_version,
                resource_authority_manifest.registry.registry_hash,
            )
            != (
                LAB_RESOURCE_AUTHORITY_REGISTRY_ID,
                LAB_RESOURCE_AUTHORITY_REGISTRY_VERSION,
                LAB_RESOURCE_AUTHORITY_REGISTRY_HASH,
            )
        ):
            raise LabDaemonConfigurationError(
                "production worker requires an explicit V2 resource authority manifest"
            )
        if require_resource_admission and resource_authority_manifest is None:
            raise LabDaemonConfigurationError(
                "isolated worker resource admission providers require a closed authority manifest"
            )
        if resource_reservation_store is not None and resource_authority_manifest is None:
            raise LabDaemonConfigurationError(
                "resource reservation store requires a closed resource authority manifest"
            )
        resource_journal_configuration: ResourceAuthorityAdapterConfig | None = None
        if (
            resource_authority_manifest is not None
            and resource_authority_manifest.registry.registry_id
            == LAB_RESOURCE_AUTHORITY_REGISTRY_ID
        ):
            if resource_reservation_store is not None:
                raise LabDaemonConfigurationError(
                    "resource journal registry owns the reservation adapter"
                )
            resource_journal_configuration = parse_resource_authority_adapter_config(
                resource_authority_manifest.registry.configuration_json
            )
            if production_mode and resource_journal_configuration.mode != "production":
                raise LabDaemonConfigurationError(
                    "production worker requires a production V2 resource authority"
                )
        self.worker_id = normalized_worker_id
        self.production_mode = production_mode
        self.claim_spool = claim_spool
        self.claim_publication_verifier = claim_publication_verifier
        self.v2_claim_publication_enabled = v2_claim_publication_enabled
        self.report_spool = report_spool
        self.artifact_root = Path(artifact_root).resolve()
        self.adapter_registry = closed_adapter_registry
        self.heartbeat_interval_microseconds = heartbeat_interval_microseconds
        self.resource_recheck_interval_microseconds = resource_recheck_interval_microseconds
        self.resource_probe_timeout_microseconds = resource_probe_timeout_microseconds
        self.lease_extension_seconds = lease_extension_seconds
        self.poll_interval_microseconds = poll_interval_ms * 1_000
        self.receipt_timeout_microseconds = receipt_timeout_microseconds
        # This is deliberately only the pre-publication admission budget.  Receipt
        # convergence and background cleanup have their own bounded phases.
        self.prepublication_admission_budget_microseconds = max(
            receipt_timeout_microseconds * 2,
            resource_probe_timeout_microseconds * 2 + _AUTHORITY_SPAWN_ALLOWANCE_MICROSECONDS,
        )
        # The post-seal check has its own rollback-safety phase.  It must be
        # long enough to acquire fresh evidence, but cannot reopen a full
        # pre-publication-sized wait after the bundle is already sealed.
        self.post_publish_rollback_safety_budget_microseconds = min(
            resource_probe_timeout_microseconds,
            _AUTHORITY_SPAWN_ALLOWANCE_MICROSECONDS,
        )
        self.quarantine_reconcile_interval_microseconds = quarantine_reconcile_interval_microseconds
        self._uses_local_receipt_waiter = receipt_waiter is None
        self.receipt_waiter = receipt_waiter or self._wait_for_receipt
        self.verified_code_sha_provider = verified_code_sha_provider
        self.resource_authority_manifest = resource_authority_manifest
        self.resource_reservation_store = (
            None
            if resource_authority_manifest is None
            else (
                LabResourceAuthorityReservationAdapter(resource_journal_configuration)
                if resource_journal_configuration is not None
                else resource_reservation_store
                or PersistentResourceReservationStore(
                    self.artifact_root.parent / "resource-reservations.db",
                    clock=clock,
                )
            )
        )
        self.require_resource_admission = require_resource_admission
        self.clock = clock
        self.monotonic_microseconds_clock = _canonical_monotonic_clock(
            monotonic_clock,
            label="monotonic_clock",
        )
        self.isolation_monotonic_microseconds_clock = _canonical_monotonic_clock(
            isolation_monotonic_clock,
            label="isolation_monotonic_clock",
        )
        if shard_runtime_manifest is None:
            from rquant.lab_worker_registry import (
                unconfigured_builtin_lab_shard_configuration,
            )

            shard_configuration = unconfigured_builtin_lab_shard_configuration()
            shard_runtime_manifest = LabShardRuntimeManifest(
                registry=LabClosedRegistryBinding(
                    registry_id=_BUILTIN_SHARD_REGISTRY_ID,
                    registry_version=_BUILTIN_SHARD_REGISTRY_VERSION,
                    registry_hash=_BUILTIN_SHARD_REGISTRY_HASH,
                    configuration_json=canonical_json_bytes(
                        shard_configuration.model_dump(mode="json", round_trip=True)
                    ).decode("utf-8"),
                )
            )
        self.shard_runtime_manifest = shard_runtime_manifest
        self.artifact_reclaimer = LabArtifactReclaimer(
            artifact_root=self.artifact_root,
            report_spool=self.report_spool,
            mutation_guard=self._verify_runtime_guard,
        )
        self.claim_spool.set_claim_advance_hook(self.artifact_reclaimer.reclaim)
        self._stop = LabStopSignal()
        self._isolation_start_gate = threading.RLock()
        self._isolation_start_condition = threading.Condition(self._isolation_start_gate)
        self._isolation_stop_generation = 0
        self._terminal_lock = threading.Lock()
        self._pending_success: LabPendingSuccess | None = None
        self._next_quarantine_reconcile_at_microseconds = 0
        self._resource_retry_at: dict[UUID, datetime] = {}
        self._resource_reservation_lock = threading.Lock()
        self._active_resource_reservation: ResourceReservationLease | None = None
        self._resource_snapshot_authority_state_lock = threading.Lock()
        self._resource_snapshot_authority_state: LabSnapshotAuthorityState | None = None
        self._managed_authority_children_lock = threading.Lock()
        self._managed_authority_children: dict[int, _ManagedAuthorityChild] = {}
        self._pre_ack_admission_diagnostics: tuple[str, ...] = ()

    def request_stop(self) -> None:
        with self._isolation_start_condition:
            self._isolation_stop_generation += 1
            self._stop.request()
            self._isolation_start_condition.notify_all()

    @staticmethod
    def _before_isolation_start_commit_for_test() -> None:
        """Deterministic test barrier immediately before the atomic start gate."""

    @staticmethod
    def _during_isolation_start_commit_for_test() -> None:
        """Deterministic test barrier while stop/check/ACK commit is serialized."""

    @staticmethod
    def _before_prestarted_authority_start_for_test(
        _stage: _PrestartedAuthorityStage,
    ) -> None:
        """Deterministic test barrier before the prestarted authority child exists."""

    @staticmethod
    def _before_prestarted_authority_handoff_for_test(
        _stage: _PrestartedAuthorityStage,
        _child: _WireChild,
    ) -> None:
        """Deterministic test barrier before startup transfers child ownership."""

    @staticmethod
    def _after_managed_authority_process_close_for_test(
        _managed: _ManagedAuthorityChild,
    ) -> None:
        """Deterministic test hook after a managed process handle has closed."""

    def _verify_closed_registries(self) -> None:
        binding = self.shard_runtime_manifest.registry
        allowed_shard_registries = {
            (
                _BUILTIN_SHARD_REGISTRY_ID,
                _BUILTIN_SHARD_REGISTRY_VERSION,
                _BUILTIN_SHARD_REGISTRY_HASH,
            ),
            (
                _TEST_SHARD_REGISTRY_ID,
                _TEST_SHARD_REGISTRY_VERSION,
                _TEST_SHARD_REGISTRY_HASH,
            ),
        }
        if (
            binding.registry_id,
            binding.registry_version,
            binding.registry_hash,
        ) not in allowed_shard_registries:
            raise LabDaemonConfigurationError("shard registry hash or identity mismatch")
        if self.resource_authority_manifest is None:
            return
        authority = self.resource_authority_manifest.registry
        allowed_authority_registries = {
            (
                _BUILTIN_AUTHORITY_REGISTRY_ID,
                _BUILTIN_AUTHORITY_REGISTRY_VERSION,
                _BUILTIN_AUTHORITY_REGISTRY_HASH,
            ),
            (
                _TEST_AUTHORITY_REGISTRY_ID,
                _TEST_AUTHORITY_REGISTRY_VERSION,
                _TEST_AUTHORITY_REGISTRY_HASH,
            ),
            (
                LAB_RESOURCE_AUTHORITY_REGISTRY_ID,
                LAB_RESOURCE_AUTHORITY_REGISTRY_VERSION,
                LAB_RESOURCE_AUTHORITY_REGISTRY_HASH,
            ),
        }
        if (
            authority.registry_id,
            authority.registry_version,
            authority.registry_hash,
        ) not in allowed_authority_registries:
            raise LabDaemonConfigurationError("authority registry hash or identity mismatch")
        if self.production_mode and (
            authority.registry_id,
            authority.registry_version,
            authority.registry_hash,
        ) != (
            LAB_RESOURCE_AUTHORITY_REGISTRY_ID,
            LAB_RESOURCE_AUTHORITY_REGISTRY_VERSION,
            LAB_RESOURCE_AUTHORITY_REGISTRY_HASH,
        ):
            raise LabDaemonConfigurationError(
                "production worker requires an explicit V2 resource authority manifest"
            )

    def _verify_runtime_guard(self, *, expected_sha: str | None = None) -> str:
        if self.verified_code_sha_provider is None:
            raise PermissionError("worker execution requires verified runtime code SHA")
        try:
            runtime_code_sha = self.verified_code_sha_provider()
        except LabDaemonConfigurationError:
            raise
        except Exception as exc:
            raise PermissionError("verified runtime code SHA provider failed") from exc
        if (
            not isinstance(runtime_code_sha, str)
            or re.fullmatch(r"[0-9a-f]{40}", runtime_code_sha) is None
        ):
            raise PermissionError("worker runtime code SHA is invalid")
        if expected_sha is not None and runtime_code_sha != expected_sha:
            raise PermissionError("runtime clean code SHA does not match ResearchRunSpec")
        return runtime_code_sha

    def sealed_bundle_path(self, claim: LabShardClaim) -> Path:
        shard_root = (
            self.artifact_root / "jobs" / str(claim.job_id) / "shards" / str(claim.shard_id)
        )
        if not all(
            hasattr(claim, field)
            for field in (
                "scheduler_fencing_token",
                "claim_generation",
                "claim_token",
            )
        ):
            return shard_root / "accepted"
        return shard_root / "attempts" / self._attempt_name(claim)

    @staticmethod
    def _attempt_name(claim: LabShardClaim) -> str:
        return (
            f"{claim.scheduler_fencing_token:020d}-"
            f"{claim.claim_generation:020d}-{claim.claim_token}"
        )

    def _temporary_bundle_path(self, claim: LabShardClaim) -> Path:
        return (
            self.artifact_root
            / ".tmp"
            / str(claim.job_id)
            / str(claim.shard_id)
            / self._attempt_name(claim)
        )

    @staticmethod
    def _parse_attempt_name(name: str) -> tuple[int, int, UUID]:
        parts = name.split("-", 2)
        if (
            len(parts) != 3
            or len(parts[0]) != 20
            or len(parts[1]) != 20
            or not parts[0].isdigit()
            or not parts[1].isdigit()
        ):
            raise LabArtifactConflictError(f"invalid temporary attempt identity: {name}")
        try:
            token = UUID(parts[2])
        except ValueError as exc:
            raise LabArtifactConflictError(f"invalid temporary attempt token: {name}") from exc
        return int(parts[0]), int(parts[1]), token

    @staticmethod
    def _assert_safe_temporary_tree(path: Path) -> None:
        root = path.lstat()
        if not stat.S_ISDIR(root.st_mode) or path.is_symlink():
            raise LabArtifactConflictError(
                f"obsolete temporary attempt is a symlink or not a directory: {path.name}"
            )
        for root, directories, files in os.walk(path, followlinks=False):
            for name in directories:
                child = Path(root) / name
                observed = child.lstat()
                if child.is_symlink() or not stat.S_ISDIR(observed.st_mode):
                    raise LabArtifactConflictError(
                        f"obsolete temporary attempt contains an unsafe directory: {name}"
                    )
            for name in files:
                child = Path(root) / name
                observed = child.lstat()
                if child.is_symlink() or not stat.S_ISREG(observed.st_mode):
                    raise LabArtifactConflictError(
                        f"obsolete temporary attempt contains an unsafe file: {name}"
                    )
                if observed.st_nlink != 1:
                    raise LabArtifactConflictError(
                        f"obsolete temporary attempt contains a hard link: {name}"
                    )

    def _assert_safe_artifact_ancestors(self, path: Path) -> None:
        try:
            relative = path.relative_to(self.artifact_root)
        except ValueError as exc:
            raise LabArtifactConflictError("artifact path escapes configured root") from exc
        current = self.artifact_root
        for part in relative.parts:
            if part in {"", ".", ".."}:
                raise LabArtifactConflictError("artifact path contains traversal components")
            current /= part
            if current.is_symlink():
                raise LabArtifactConflictError(f"artifact path ancestor is a symlink: {part}")
            if os.path.lexists(current) and not current.is_dir():
                raise LabArtifactConflictError(f"artifact path ancestor is not a directory: {part}")

    def _reclaim_current_candidate_directories(
        self,
        attempt_root: Path,
        _shard_root: Path,
        claim: LabShardClaim,
    ) -> None:
        self._assert_safe_temporary_tree(attempt_root)
        for child in tuple(attempt_root.iterdir()):
            try:
                candidate_id = UUID(child.name)
            except ValueError:
                continue
            if candidate_id.hex != child.name:
                continue
            self.artifact_reclaimer.logical_delete_temporary_tree(
                child,
                current_claim=claim,
            )

    def _reclaim_obsolete_temporaries(self, claim: LabShardClaim) -> None:
        current_root = self._temporary_bundle_path(claim)
        shard_root = current_root.parent
        self._assert_safe_artifact_ancestors(shard_root)
        if not shard_root.exists():
            return
        if shard_root.is_symlink() or not shard_root.is_dir():
            raise LabArtifactConflictError("temporary shard root is unsafe")
        for candidate in tuple(shard_root.iterdir()):
            fence, generation, token = self._parse_attempt_name(candidate.name)
            if generation > claim.claim_generation:
                continue
            if generation == claim.claim_generation:
                if (
                    fence,
                    token,
                ) != (
                    claim.scheduler_fencing_token,
                    claim.claim_token,
                ):
                    raise LabArtifactConflictError(
                        "same-generation temporary attempt has conflicting identity"
                    )
                if not self.claim_spool.is_current(claim):
                    raise LabArtifactConflictError(
                        "current temporary attempt is no longer the claim high-water"
                    )
                self._reclaim_current_candidate_directories(candidate, shard_root, claim)
                continue
            self.artifact_reclaimer.logical_delete_temporary_tree(
                candidate,
                current_claim=claim,
            )

    @staticmethod
    def _validate_receipt_identity(
        report: LabWorkerReport,
        receipt: LabReportReceipt,
    ) -> None:
        if (
            receipt.report_id != report.report_id
            or receipt.content_hash != report.content_hash
            or receipt.job_id != report.job_id
            or receipt.shard_id != report.shard_id
        ):
            raise ValueError("report receipt identity does not match published report")
        if (
            receipt.worker_id,
            receipt.claim_token,
            receipt.claim_generation,
            receipt.scheduler_fencing_token,
            receipt.report_type,
            receipt.result_manifest_hash,
        ) != (
            report.worker_id,
            report.claim_token,
            report.claim_generation,
            report.scheduler_fencing_token,
            report.body.report_type,
            (
                report.body.result_manifest_hash
                if isinstance(report.body, LabShardSucceeded)
                else None
            ),
        ):
            raise ValueError("report receipt attempt identity does not match published report")

    def _make_report(
        self,
        claim: LabShardClaim,
        body: LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
    ) -> LabWorkerReport:
        return LabWorkerReport.from_claim(
            claim,
            report_id=uuid4(),
            reported_at=_utc(self.clock()),
            body=body,
        )

    def _publish_report(
        self,
        claim: LabShardClaim,
        body: LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
    ) -> LabWorkerReport:
        self._verify_runtime_guard()
        report = self._make_report(claim, body)
        try:
            self._verify_runtime_guard()
            self.report_spool.publish(report)
        except Exception as exc:
            _safe_structured_log(
                "error",
                "report_publish_failed",
                message=str(exc) or type(exc).__name__,
                component="lab_worker",
                worker_id=self.worker_id,
                job_id=str(report.job_id),
                shard_id=str(report.shard_id),
                claim_token=str(report.claim_token),
                report_id=str(report.report_id),
                report_type=report.body.report_type,
                error_type=type(exc).__name__,
            )
            raise
        return report

    def _wait_for_receipt(
        self,
        report: LabWorkerReport,
        timeout_seconds: float,
        stop: LabStopSignal,
    ) -> LabReportReceipt:
        timeout_microseconds = _positive_duration_microseconds(
            timeout_seconds,
            label="receipt timeout_seconds",
        )
        timeout_at_microseconds = _monotonic_microseconds() + timeout_microseconds
        receipt_path = self.report_spool.ack_dir / f"{report.report_id}.json"
        while True:
            if os.path.lexists(receipt_path):
                receipt = self.report_spool.load_receipt(receipt_path)
                if (
                    receipt.report_id != report.report_id
                    or receipt.content_hash != report.content_hash
                    or receipt.job_id != report.job_id
                    or receipt.shard_id != report.shard_id
                ):
                    raise ValueError("report receipt identity does not match published report")
                return receipt
            if stop.is_set():
                raise InterruptedError("worker stop requested while waiting for report receipt")
            remaining_microseconds = timeout_at_microseconds - _monotonic_microseconds()
            if remaining_microseconds <= 0:
                raise TimeoutError(f"report receipt timed out: {report.report_id}")
            stop.wait(_microseconds_to_seconds(min(50_000, remaining_microseconds)))

    def _receipt_wait_timeout_seconds(self) -> float:
        return _microseconds_to_seconds(self.receipt_timeout_microseconds)

    def _retry_local_receipt_wait(
        self,
        report: LabWorkerReport,
    ) -> LabReportReceipt:
        timeout_seconds = self._receipt_wait_timeout_seconds()
        self.report_spool.publish(report)
        return self._wait_for_receipt(
            report,
            timeout_seconds,
            self._stop,
        )

    def _publish_and_wait(
        self,
        claim: LabShardClaim,
        body: LabShardHeartbeat | LabShardSucceeded,
        *,
        stop: LabStopSignal,
    ) -> LabReportReceipt:
        timeout_seconds = self._receipt_wait_timeout_seconds()
        report = self._publish_report(claim, body)
        try:
            receipt = self.receipt_waiter(
                report,
                timeout_seconds,
                stop,
            )
        except TimeoutError as exc:
            if self._uses_local_receipt_waiter:
                try:
                    receipt = self._retry_local_receipt_wait(report)
                except TimeoutError:
                    pass
                else:
                    self._validate_receipt_identity(report, receipt)
                    if receipt.status != "accepted":
                        raise PermissionError(f"worker report rejected: {receipt.reason}")
                    return receipt
            _safe_structured_log(
                "warning",
                "report_receipt_timeout",
                message=str(exc) or type(exc).__name__,
                component="lab_worker",
                worker_id=self.worker_id,
                job_id=str(report.job_id),
                shard_id=str(report.shard_id),
                claim_token=str(report.claim_token),
                report_id=str(report.report_id),
                report_type=report.body.report_type,
            )
            raise
        except Exception as exc:
            _safe_structured_log(
                "error",
                "report_receipt_transport_failed",
                message=str(exc) or type(exc).__name__,
                component="lab_worker",
                worker_id=self.worker_id,
                job_id=str(report.job_id),
                shard_id=str(report.shard_id),
                claim_token=str(report.claim_token),
                report_id=str(report.report_id),
                report_type=report.body.report_type,
                error_type=type(exc).__name__,
            )
            raise
        self._validate_receipt_identity(report, receipt)
        if receipt.status != "accepted":
            raise PermissionError(f"worker report rejected: {receipt.reason}")
        return receipt

    def _best_effort_report(
        self,
        claim: LabShardClaim,
        body: LabShardFailed | LabWorkerStopped,
    ) -> bool:
        try:
            self._publish_report(claim, body)
        except LabDaemonConfigurationError:
            raise
        except Exception as exc:
            normalized_message = " ".join(str(exc).split()) or type(exc).__name__
            _safe_structured_log(
                "warning",
                "terminal_report_publish_failed",
                message=normalized_message,
                component="lab_worker",
                worker_id=self.worker_id,
                job_id=str(claim.job_id),
                shard_id=str(claim.shard_id),
                claim_token=str(claim.claim_token),
                claim_generation=claim.claim_generation,
                report_type=body.report_type,
                error_type=type(exc).__name__,
            )
            return False
        return True

    def _next_owned_claim_entry(self) -> LabClaimSpoolEntry | None:
        now = _utc(self.clock())
        actionable_tokens: set[UUID] = set()
        selected: LabClaimSpoolEntry | None = None
        for path in self.claim_spool.pending_paths():
            try:
                entry = self.claim_spool.load(path)
            except InvalidCommandEnvelopeError:
                continue
            claim = entry.claim
            try:
                marker = self.claim_spool.current(claim.job_id, claim.shard_id)
            except InvalidCommandEnvelopeError:
                continue
            if marker.claim != claim:
                if (
                    claim.worker_id == self.worker_id
                    and claim.claim_generation <= marker.claim.claim_generation
                ):
                    self._verify_runtime_guard()
                    with suppress(InvalidCommandEnvelopeError):
                        self.claim_spool.quarantine(
                            entry,
                            reason="superseded_by_current_claim_marker",
                        )
                continue
            if claim.lease_expires_at <= now:
                continue
            owned = claim.worker_id == self.worker_id or (
                isinstance(claim, LabShardClaimV2) and claim.worker_id == V2_UNASSIGNED_WORKER_ID
            )
            if not owned:
                continue
            actionable_tokens.add(claim.claim_token)
            if selected is not None:
                continue
            retry_at = self._resource_retry_at.get(claim.claim_token)
            if retry_at is not None and retry_at > now:
                continue
            selected = entry
        self._resource_retry_at = {
            token: retry_at
            for token, retry_at in self._resource_retry_at.items()
            if token in actionable_tokens and retry_at > now
        }
        if selected is None:
            return None
        self._resource_retry_at.pop(selected.claim.claim_token, None)
        self._verify_runtime_guard()
        return selected

    def _consume_selected_claim(
        self, entry: LabClaimSpoolEntry
    ) -> LabShardClaim | LabShardClaimV2 | None:
        try:
            self._verify_runtime_guard()
            if isinstance(entry.claim, LabShardClaimV2):
                if self.claim_publication_verifier is None:
                    raise LabDaemonConfigurationError(
                        "V2 claim publication verifier is not configured"
                    )
                self.claim_publication_verifier.require_published_claim(
                    entry.claim,
                    now=_utc(self.clock()),
                )
            return self.claim_spool.consume(entry)
        except (
            InvalidCommandEnvelopeError,
            LabClaimAlreadyConsumedError,
            LabClaimRevokedError,
            LabClaimSupersededError,
            LabClaimFinalizerError,
            SourceOperationContractError,
            OSError,
        ):
            return None
        finally:
            self._resource_retry_at.pop(entry.claim.claim_token, None)

    def _require_v2_publication_before_admission(self, entry: LabClaimSpoolEntry) -> None:
        """Gate V2 entries before any resource or authority-child side effect."""

        if not isinstance(entry.claim, LabShardClaimV2):
            return
        if self.claim_publication_verifier is None:
            raise LabDaemonConfigurationError("V2 claim publication verifier is not configured")
        self.claim_publication_verifier.require_published_claim(
            entry.claim,
            now=_utc(self.clock()),
        )

    def _resource_admission_decision(
        self,
        claim: LabShardClaim,
        spec: ResearchRunSpec,
        *,
        tick_deadline_microseconds: int | None = None,
    ) -> AdmissionDecision | None:
        evaluation = self._resource_admission_evaluation(
            claim,
            spec,
            tick_deadline_microseconds=tick_deadline_microseconds,
        )
        return None if evaluation is None else evaluation.decision

    def _receive_wire_before_deadline(
        self,
        child: _WireChild,
        *,
        model: type[WireModelT],
        max_bytes: int,
        deadline_microseconds: int,
        label: str,
        honor_worker_stop: bool = True,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> WireModelT:
        def cancelled() -> bool:
            return (honor_worker_stop and self._stop.is_set()) or (
                cancel_requested is not None and cancel_requested()
            )

        return _recv_wire(
            child.connection,
            model=model,
            max_bytes=max_bytes,
            label=label,
            deadline_microseconds=deadline_microseconds,
            cancel_requested=cancelled,
        )

    def _start_wire_child(
        self,
        *,
        target: Callable[..., object],
        request_bytes: bytes,
        process_name: str,
        deadline_microseconds: int,
        label: str,
        max_wire_bytes: int,
        honor_worker_stop_during_readiness: bool = True,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> _WireChild:
        def externally_cancelled() -> bool:
            return (honor_worker_stop_during_readiness and self._stop.is_set()) or (
                cancel_requested is not None and cancel_requested()
            )

        if externally_cancelled():
            raise InterruptedError(f"{label} startup cancelled")
        session = _new_wire_session()
        listener = session.listener
        address = session.address
        authkey = session.authkey
        context = multiprocessing.get_context("spawn")
        arguments: tuple[object, ...] = (
            request_bytes,
            address,
            authkey,
            max_wire_bytes,
        )
        _assert_primitive_process_start(target, arguments)
        process = context.Process(
            target=target,
            args=arguments,
            name=process_name,
            daemon=False,
        )
        connection: _DeadlineWireEndpoint | None = None
        started = False
        try:
            if externally_cancelled():
                raise InterruptedError(f"{label} startup cancelled")
            process.start()
            started = True

            def startup_cancelled() -> bool:
                if externally_cancelled():
                    return True
                if not process.is_alive():
                    raise LabDaemonConfigurationError(f"{label} process exited before connecting")
                return False

            connection = listener.accept(
                deadline_microseconds=deadline_microseconds,
                cancel_requested=startup_cancelled,
            )
            provisional = _WireChild(
                process=process,
                connection=connection,
                group_id=-1,
                address=address,
            )
            readiness = self._receive_wire_before_deadline(
                provisional,
                model=_IsolationReadiness,
                max_bytes=_MAX_CONTROL_WIRE_BYTES,
                deadline_microseconds=deadline_microseconds,
                label=f"{label} readiness",
                honor_worker_stop=honor_worker_stop_during_readiness,
                cancel_requested=cancel_requested,
            )
            if not readiness.ready:
                raise LabDaemonConfigurationError(
                    readiness.message or f"{label} process readiness failed"
                )
            if (
                process.pid is None
                or readiness.child_pid != process.pid
                or readiness.group_id != process.pid
            ):
                raise LabWireSessionStartupError(f"{label} process identity verification failed")
            try:
                observed_group_id = os.getpgid(process.pid)
            except ProcessLookupError as exc:
                raise LabDaemonConfigurationError(
                    f"{label} process exited before identity verification"
                ) from exc
            if observed_group_id != readiness.group_id:
                raise LabDaemonConfigurationError(f"{label} process group verification failed")
            return _WireChild(
                process=process,
                connection=connection,
                group_id=observed_group_id,
                address=address,
            )
        except BaseException as exc:
            if connection is not None:
                with suppress(BaseException):
                    connection.close()
            if started:
                with suppress(BaseException):
                    self._terminate_isolated_process(
                        process,
                        isolated_group_id=process.pid,
                    )
            with suppress(BaseException):
                process.close()
            if isinstance(
                exc,
                (
                    InterruptedError,
                    TimeoutError,
                    LabWireSessionStartupError,
                ),
            ):
                raise
            if isinstance(exc, LabDaemonConfigurationError):
                raise LabWireSessionStartupError(str(exc) or f"{label} startup failed") from exc
            raise LabWireSessionStartupError(
                f"{label} startup failed: {_bounded_exception_message(exc)}"
            ) from exc
        finally:
            session.cleanup()

    def _close_wire_child(
        self,
        child: _WireChild,
        *,
        label: str,
        allow_graceful_termination: bool = True,
    ) -> None:
        errors: list[BaseException] = []
        try:
            child_pid = child.process.pid
        except BaseException as exc:
            errors.append(exc)
            child_pid = None
        try:
            self._terminate_isolated_process(
                child.process,
                isolated_group_id=child.group_id,
                allow_graceful_termination=allow_graceful_termination,
            )
        except BaseException as exc:
            errors.append(exc)
        try:
            child.connection.close()
        except BaseException as exc:
            errors.append(exc)
        if child_pid is not None:
            try:
                child.process.join(timeout=0)
                if any(
                    active_child.pid == child_pid
                    for active_child in multiprocessing.active_children()
                ):
                    raise RuntimeError(
                        f"{label} child remained in multiprocessing registry after reap"
                    )
            except BaseException as exc:
                errors.append(exc)
        try:
            child.process.close()
        except BaseException as exc:
            errors.append(exc)
        if errors:
            raise BaseExceptionGroup(f"{label} cleanup failed", errors)

    def _register_authority_child(
        self,
        child: _WireChild,
        *,
        operation: Literal["admission", "policy", "snapshot", "quota"],
        owner: Literal["direct", "startup"],
        cancelled: threading.Event | None = None,
    ) -> _ManagedAuthorityChild:
        process_id = child.process.pid
        if process_id is None:  # pragma: no cover - _start_wire_child verifies this
            raise LabDaemonConfigurationError("authority child has no process PID")
        managed = _ManagedAuthorityChild(
            child=child,
            process_id=process_id,
            cached_pid=process_id,
            operation=operation,
            owner=owner,
            lock=threading.Lock(),
            cancelled=cancelled or threading.Event(),
            cleanup_complete=threading.Event(),
        )
        with self._managed_authority_children_lock:
            if process_id in self._managed_authority_children:
                raise LabDaemonConfigurationError("authority child PID is already registered")
            self._managed_authority_children[process_id] = managed
        return managed

    @staticmethod
    def _transfer_managed_authority_owner(
        managed: _ManagedAuthorityChild,
        *,
        expected: Literal[
            "direct",
            "startup",
            "ready",
            "startup_cleanup",
            "consumer",
            "canceller",
            "tick_cleanup",
            "reap_pending",
        ],
        target: Literal[
            "ready",
            "startup_cleanup",
            "consumer",
            "canceller",
            "tick_cleanup",
            "reap_pending",
        ],
    ) -> None:
        with managed.lock:
            if managed.owner != expected:
                raise LabDaemonConfigurationError(
                    "managed authority child ownership changed unexpectedly"
                )
            managed.owner = target

    def _close_managed_authority_child(
        self,
        managed: _ManagedAuthorityChild,
        *,
        owner: Literal[
            "direct",
            "startup_cleanup",
            "consumer",
            "canceller",
            "tick_cleanup",
            "reap_pending",
        ],
        label: str,
    ) -> BaseException | None:
        errors: list[BaseException] = []
        with managed.lock:
            if managed.owner != owner:
                return LabDaemonConfigurationError(
                    "managed authority child cleanup owner changed unexpectedly"
                )
            if managed.cleanup_in_progress:
                return LabDaemonConfigurationError(
                    "managed authority child cleanup is already in progress"
                )
            managed.cleanup_in_progress = True

        if not managed.os_process_exited_verified:
            process = managed.child.process
            try:
                self._terminate_isolated_process(
                    process,
                    isolated_group_id=managed.child.group_id,
                )
            except BaseException as exc:
                errors.append(exc)
            try:
                process.join(timeout=0)
            except BaseException as exc:
                errors.append(exc)
            else:
                try:
                    os.kill(managed.cached_pid, 0)
                except ProcessLookupError:
                    active_child_exists = any(
                        active_child.pid == managed.cached_pid
                        for active_child in multiprocessing.active_children()
                    )
                    if not active_child_exists:
                        with managed.lock:
                            managed.os_process_exited_verified = True
                    else:
                        errors.append(
                            RuntimeError(
                                f"{label} child remained in multiprocessing registry after reap"
                            )
                        )
                except BaseException as exc:
                    errors.append(exc)
                else:
                    errors.append(RuntimeError(f"{label} child remained alive after reap"))

        if not managed.ipc_closed:
            connection = managed.child.connection
            try:
                connection.close()
            except BaseException as exc:
                errors.append(exc)
            finally:
                try:
                    ipc_closed = bool(connection.closed)
                except BaseException as exc:
                    errors.append(exc)
                    ipc_closed = False
                if ipc_closed:
                    with managed.lock:
                        managed.ipc_closed = True

        if managed.os_process_exited_verified and not managed.process_handle_closed:
            process = managed.child.process
            try:
                process.close()
            except BaseException as exc:
                errors.append(exc)
            else:
                with managed.lock:
                    managed.process_handle_closed = True
                try:
                    self._after_managed_authority_process_close_for_test(managed)
                except BaseException as exc:
                    errors.append(exc)

        cleanup_error: BaseException | None = None
        if errors:
            cleanup_error = (
                errors[0]
                if len(errors) == 1
                else BaseExceptionGroup(f"{label} cleanup diagnostics", errors)
            )
        with managed.lock:
            complete = (
                managed.os_process_exited_verified
                and managed.ipc_closed
                and managed.process_handle_closed
            )
            if not complete and cleanup_error is None:
                cleanup_error = RuntimeError(f"{label} cleanup did not converge")
            if cleanup_error is not None:
                managed.last_errors = tuple(errors) or (cleanup_error,)
                managed.cleanup_error = cleanup_error
            managed.cleanup_in_progress = False
            if complete:
                managed.owner = "closed"
            else:
                managed.owner = "reap_pending"
                managed.cleanup_retry_count += 1

        if complete:
            with self._managed_authority_children_lock:
                if self._managed_authority_children.get(managed.process_id) is managed:
                    del self._managed_authority_children[managed.process_id]
            managed.cleanup_complete.set()
        return cleanup_error

    def _reap_managed_authority_children(self) -> None:
        deadline_microseconds = (
            _monotonic_microseconds() + _AUTHORITY_CHILD_CLEANUP_BUDGET_MICROSECONDS
        )
        errors: list[BaseException] = []
        while True:
            with self._managed_authority_children_lock:
                managed_children = tuple(self._managed_authority_children.values())
            if not managed_children:
                break
            progressed = False
            for managed in managed_children:
                with managed.lock:
                    owner = managed.owner
                    if managed.cleanup_in_progress:
                        continue
                    if owner == "startup":
                        managed.cancelled.set()
                    elif owner in {"direct", "ready", "reap_pending"}:
                        managed.owner = "tick_cleanup"
                        owner = "tick_cleanup"
                        progressed = True
                if owner == "tick_cleanup":
                    error = self._close_managed_authority_child(
                        managed,
                        owner="tick_cleanup",
                        label="resource authority tick cleanup",
                    )
                    if error is not None:
                        errors.append(error)
            with self._managed_authority_children_lock:
                if not self._managed_authority_children:
                    break
            if _monotonic_microseconds() >= deadline_microseconds:
                errors.append(RuntimeError("authority child cleanup budget exhausted"))
                break
            if not progressed:
                managed_children[0].cleanup_complete.wait(
                    _microseconds_to_seconds(_RESOURCE_AUTHORITY_POLL_MICROSECONDS)
                )
        if errors:
            raise BaseExceptionGroup("authority child cleanup failed", errors)

    def _prestart_authority_stage(
        self,
        *,
        operation: Literal["admission", "policy", "snapshot", "quota"],
        spec: ResearchRunSpec | None,
        admission_request: AdmissionRequest | None,
        deadline_microseconds: int,
    ) -> _PrestartedAuthorityStage:
        manifest = self.resource_authority_manifest
        if manifest is None:
            raise LabDaemonConfigurationError("resource authority manifest is unavailable")
        request = _AuthorityWireRequest(
            operation=operation,
            manifest=manifest,
            spec=spec,
            admission_request=admission_request,
            authority_state=(
                self._accepted_snapshot_authority_state()
                if operation in {"admission", "snapshot"}
                else None
            ),
        )
        stage = _PrestartedAuthorityStage(
            handoff=threading.Event(),
            startup_complete=threading.Event(),
            cleanup_complete=threading.Event(),
            cancelled=threading.Event(),
            lock=threading.Lock(),
            deadline_microseconds=deadline_microseconds,
            cleanup_deadline_microseconds=(
                deadline_microseconds + _PRESTART_AUTHORITY_CLEANUP_RESERVE_MICROSECONDS
            ),
        )
        label = {
            "admission": "resource admission authority",
            "policy": "admission policy provider",
            "snapshot": "resource snapshot provider",
            "quota": "source quota lease provider",
        }[operation]

        def prestart() -> None:
            child: _WireChild | None = None
            managed_child: _ManagedAuthorityChild | None = None
            cleanup_as_startup = False
            startup_error: BaseException | None = None
            try:
                self._before_prestarted_authority_start_for_test(stage)
                with stage.lock:
                    if stage.cancelled.is_set():
                        stage.owner = "closed"
                        stage.handoff.set()
                        stage.cleanup_complete.set()
                        return
                child = self._start_wire_child(
                    target=_authority_wire_child,
                    request_bytes=_encode_wire_message(request),
                    process_name=(
                        "lab-resource-probe"
                        if operation == "snapshot"
                        else "lab-resource-authority"
                    ),
                    deadline_microseconds=deadline_microseconds,
                    label=label,
                    max_wire_bytes=_MAX_CONTROL_WIRE_BYTES,
                    cancel_requested=stage.cancelled.is_set,
                )
                managed_child = self._register_authority_child(
                    child,
                    operation=operation,
                    owner="startup",
                    cancelled=stage.cancelled,
                )
                # Startup owns the child until this lock commits a handoff.  A
                # concurrent cancellation can observe that owner, but never
                # closes the child while startup may still be using it.
                with stage.lock:
                    if stage.owner != "startup":  # pragma: no cover - invariant
                        raise LabDaemonConfigurationError(
                            "prestarted authority startup ownership changed unexpectedly"
                        )
                    stage.managed_child = managed_child
                self._before_prestarted_authority_handoff_for_test(stage, child)
                with stage.lock:
                    if stage.cancelled.is_set():
                        self._transfer_managed_authority_owner(
                            managed_child,
                            expected="startup",
                            target="startup_cleanup",
                        )
                        stage.owner = "startup_cleanup"
                        cleanup_as_startup = True
                    else:
                        self._transfer_managed_authority_owner(
                            managed_child,
                            expected="startup",
                            target="ready",
                        )
                        stage.owner = "ready"
                    stage.handoff.set()
            except BaseException as exc:
                startup_error = exc
                with stage.lock:
                    if child is None:
                        stage.error = exc
                        stage.owner = "closed"
                        stage.handoff.set()
                        stage.cleanup_complete.set()
                    else:
                        if managed_child is None:
                            try:
                                self._close_wire_child(child, label=label)
                            except BaseException as cleanup_error:
                                stage.error = BaseExceptionGroup(
                                    "prestarted authority registration cleanup failed",
                                    [exc, cleanup_error],
                                )
                            else:
                                stage.error = exc
                            stage.owner = "closed"
                            stage.handoff.set()
                            stage.cleanup_complete.set()
                        else:
                            stage.managed_child = managed_child
                            self._transfer_managed_authority_owner(
                                managed_child,
                                expected="startup",
                                target="startup_cleanup",
                            )
                            stage.owner = "startup_cleanup"
                            stage.handoff.set()
                            cleanup_as_startup = True
            finally:
                if cleanup_as_startup and managed_child is not None:
                    self._finish_prestarted_authority_cleanup(
                        stage,
                        managed_child,
                        owner="startup_cleanup",
                        label=label,
                        startup_error=startup_error,
                    )
                with stage.lock:
                    if stage.owner == "startup":
                        stage.owner = "closed"
                        stage.handoff.set()
                        stage.cleanup_complete.set()
                stage.startup_complete.set()

        startup_thread = threading.Thread(
            target=prestart,
            name=f"lab-prestart-authority-{operation}",
            daemon=False,
        )
        stage.startup_thread = startup_thread
        startup_thread.start()
        return stage

    @staticmethod
    def _await_prestarted_authority_event(
        event: threading.Event,
        *,
        deadline_microseconds: int,
        label: str,
    ) -> None:
        while not event.is_set():
            remaining = deadline_microseconds - _monotonic_microseconds()
            if remaining <= 0:
                raise TimeoutError(f"{label} timed out")
            event.wait(
                _microseconds_to_seconds(min(_RESOURCE_AUTHORITY_POLL_MICROSECONDS, remaining))
            )

    def _await_prestarted_authority_shutdown(
        self,
        stage: _PrestartedAuthorityStage,
        *,
        label: str,
        deadline_microseconds: int,
    ) -> None:
        self._await_prestarted_authority_event(
            stage.handoff,
            deadline_microseconds=deadline_microseconds,
            label=f"{label} handoff",
        )
        self._await_prestarted_authority_event(
            stage.cleanup_complete,
            deadline_microseconds=deadline_microseconds,
            label=f"{label} cleanup",
        )
        startup_thread = stage.startup_thread
        if startup_thread is not None:
            remaining = deadline_microseconds - _monotonic_microseconds()
            if remaining <= 0 and startup_thread.is_alive():
                raise TimeoutError(f"{label} startup thread cleanup timed out")
            startup_thread.join(_microseconds_to_seconds(max(0, remaining)))
            if startup_thread.is_alive():
                raise TimeoutError(f"{label} startup thread cleanup timed out")

    def _cancel_prestarted_authority_stage(
        self,
        stage: _PrestartedAuthorityStage | None,
        *,
        operation: Literal["admission", "policy", "snapshot", "quota"],
        deadline_microseconds: int | None = None,
    ) -> None:
        if stage is None:
            return
        label = {
            "admission": "resource admission authority",
            "policy": "admission policy provider",
            "snapshot": "resource snapshot provider",
            "quota": "source quota lease provider",
        }[operation]
        phase_deadline_microseconds = min(
            stage.cleanup_deadline_microseconds,
            deadline_microseconds
            if deadline_microseconds is not None
            else stage.cleanup_deadline_microseconds,
        )
        stage.cancelled.set()
        managed_child: _ManagedAuthorityChild | None = None
        with stage.lock:
            if stage.owner == "ready":
                managed_child = stage.managed_child
                if managed_child is None:  # pragma: no cover - ownership invariant
                    raise LabDaemonConfigurationError(
                        "prestarted authority ready stage has no child"
                    )
                self._transfer_managed_authority_owner(
                    managed_child,
                    expected="ready",
                    target="canceller",
                )
                stage.owner = "canceller"
        if managed_child is not None:
            cleanup_error = self._finish_prestarted_authority_cleanup(
                stage,
                managed_child,
                owner="canceller",
                label=label,
            )
            if cleanup_error is not None:
                raise cleanup_error
        self._await_prestarted_authority_shutdown(
            stage,
            label=label,
            deadline_microseconds=phase_deadline_microseconds,
        )

    def _finish_prestarted_authority_cleanup(
        self,
        stage: _PrestartedAuthorityStage,
        managed_child: _ManagedAuthorityChild,
        *,
        owner: Literal["startup_cleanup", "canceller", "consumer"],
        label: str,
        startup_error: BaseException | None = None,
    ) -> BaseException | None:
        cleanup_error = self._close_managed_authority_child(
            managed_child,
            owner=owner,
            label=label,
        )
        with stage.lock:
            if stage.owner != owner or stage.managed_child is not managed_child:
                cleanup_error = cleanup_error or LabDaemonConfigurationError(
                    "prestarted authority child ownership changed during cleanup"
                )
            if stage.error is None:
                stage.error = startup_error or cleanup_error
            if cleanup_error is None:
                stage.managed_child = None
                stage.owner = "closed"
                stage.handoff.set()
                stage.cleanup_complete.set()
        return cleanup_error

    def _complete_prestarted_authority_stage(
        self,
        stage: _PrestartedAuthorityStage,
        *,
        operation: Literal["admission", "policy", "snapshot", "quota"],
        deadline_microseconds: int,
    ) -> _AuthorityWireResult:
        label = {
            "admission": "resource admission authority",
            "policy": "admission policy provider",
            "snapshot": "resource snapshot provider",
            "quota": "source quota lease provider",
        }[operation]
        while not stage.handoff.is_set():
            if self._stop.is_set():
                raise InterruptedError(f"worker stop requested during {label} prestart")
            remaining = deadline_microseconds - _monotonic_microseconds()
            if remaining <= 0:
                raise TimeoutError(f"{label} prestart timed out")
            stage.handoff.wait(
                _microseconds_to_seconds(min(_RESOURCE_AUTHORITY_POLL_MICROSECONDS, remaining))
            )
        interrupted = False
        with stage.lock:
            error = stage.error
            if stage.cancelled.is_set():
                interrupted = True
            elif stage.owner != "ready":
                managed_child = None
            else:
                managed_child = stage.managed_child
                if managed_child is not None:
                    self._transfer_managed_authority_owner(
                        managed_child,
                        expected="ready",
                        target="consumer",
                    )
                stage.owner = "consumer"
        if interrupted:
            self._await_prestarted_authority_shutdown(
                stage,
                label=label,
                deadline_microseconds=deadline_microseconds,
            )
            raise InterruptedError(f"worker stop requested during {label} prestart")
        if error is not None:
            self._await_prestarted_authority_shutdown(
                stage,
                label=label,
                deadline_microseconds=deadline_microseconds,
            )
            raise error
        if managed_child is None:
            self._await_prestarted_authority_shutdown(
                stage,
                label=label,
                deadline_microseconds=deadline_microseconds,
            )
            raise LabDaemonConfigurationError(f"{label} prestart returned no child")
        primary_error: BaseException | None = None
        result: _AuthorityWireResult | None = None
        try:
            try:
                _send_wire(
                    managed_child.child.connection,
                    _IsolationStartAck(
                        accepted=True,
                        not_after_monotonic_microseconds=deadline_microseconds,
                    ),
                    deadline_microseconds=deadline_microseconds,
                    cancel_requested=lambda: stage.cancelled.is_set() or self._stop.is_set(),
                )
            except (InterruptedError, TimeoutError):
                raise
            except Exception as exc:
                raise LabWireSessionStartupError(
                    f"{label} start acknowledgement transport failed: "
                    f"{_bounded_exception_message(exc)}"
                ) from exc
            try:
                result = self._receive_wire_before_deadline(
                    managed_child.child,
                    model=_AuthorityWireResult,
                    max_bytes=_MAX_CONTROL_WIRE_BYTES,
                    deadline_microseconds=deadline_microseconds,
                    label=label,
                    cancel_requested=stage.cancelled.is_set,
                )
            except (InterruptedError, TimeoutError):
                raise
            except Exception as exc:
                raise LabWireSessionError(
                    f"{label} session transport failed: {_bounded_exception_message(exc)}"
                ) from exc
            if result.operation != operation:
                raise LabDaemonConfigurationError(f"{label} operation mismatch")
            if result.error_type is not None:
                raise LabDaemonConfigurationError(
                    f"{label} failed: {result.error_type}: {result.message or 'unknown failure'}"
                )
        except BaseException as exc:
            primary_error = exc
        cleanup_error = self._finish_prestarted_authority_cleanup(
            stage,
            managed_child,
            owner="consumer",
            label=label,
        )
        self._await_prestarted_authority_shutdown(
            stage,
            label=label,
            deadline_microseconds=deadline_microseconds,
        )
        if primary_error is not None and cleanup_error is not None:
            raise BaseExceptionGroup(f"{label} and cleanup failed", [primary_error, cleanup_error])
        if primary_error is not None:
            raise primary_error
        if cleanup_error is not None:
            raise cleanup_error
        if result is None:  # pragma: no cover - guarded above
            raise LabDaemonConfigurationError(f"{label} returned no evidence")
        return result

    def _run_authority_stage(
        self,
        *,
        operation: Literal["admission", "policy", "snapshot", "quota"],
        spec: ResearchRunSpec | None,
        admission_request: AdmissionRequest | None = None,
        snapshot: ResourceSnapshot | None = None,
        timeout_microseconds: int,
        not_after_monotonic_microseconds: int | None = None,
        include_spawn_allowance: bool = True,
        cancellation_requested: Callable[[], bool] | None = None,
    ) -> _AuthorityWireResult:
        manifest = self.resource_authority_manifest
        if manifest is None:
            raise LabDaemonConfigurationError("resource authority manifest is unavailable")
        if timeout_microseconds <= 0:
            raise TimeoutError(f"{operation} authority timed out")
        total_budget = timeout_microseconds + (
            _AUTHORITY_SPAWN_ALLOWANCE_MICROSECONDS if include_spawn_allowance else 0
        )
        deadline_microseconds = _monotonic_microseconds() + total_budget
        if not_after_monotonic_microseconds is not None:
            deadline_microseconds = min(
                deadline_microseconds,
                not_after_monotonic_microseconds,
            )
        request = _AuthorityWireRequest(
            operation=operation,
            manifest=manifest,
            spec=spec,
            admission_request=admission_request,
            snapshot=snapshot,
            authority_state=(
                self._accepted_snapshot_authority_state()
                if operation in {"admission", "snapshot"}
                else None
            ),
        )
        label = {
            "admission": "resource admission authority",
            "policy": "admission policy provider",
            "snapshot": "resource snapshot provider",
            "quota": "source quota lease provider",
        }[operation]
        child: _WireChild | None = None
        managed_child: _ManagedAuthorityChild | None = None
        primary_error: BaseException | None = None
        result: _AuthorityWireResult | None = None
        try:
            child = self._start_wire_child(
                target=_authority_wire_child,
                request_bytes=_encode_wire_message(request),
                process_name=(
                    "lab-resource-probe" if operation == "snapshot" else "lab-resource-authority"
                ),
                deadline_microseconds=deadline_microseconds,
                label=label,
                max_wire_bytes=_MAX_CONTROL_WIRE_BYTES,
                cancel_requested=cancellation_requested,
            )
            managed_child = self._register_authority_child(
                child,
                operation=operation,
                owner="direct",
            )
            try:
                _send_wire(
                    managed_child.child.connection,
                    _IsolationStartAck(
                        accepted=True,
                        not_after_monotonic_microseconds=deadline_microseconds,
                    ),
                    deadline_microseconds=deadline_microseconds,
                    cancel_requested=cancellation_requested,
                )
            except (InterruptedError, TimeoutError):
                raise
            except Exception as exc:
                raise LabWireSessionStartupError(
                    f"{label} start acknowledgement transport failed: "
                    f"{_bounded_exception_message(exc)}"
                ) from exc
            try:
                result = self._receive_wire_before_deadline(
                    managed_child.child,
                    model=_AuthorityWireResult,
                    max_bytes=_MAX_CONTROL_WIRE_BYTES,
                    deadline_microseconds=deadline_microseconds,
                    label=label,
                    cancel_requested=cancellation_requested,
                )
            except (InterruptedError, TimeoutError):
                raise
            except Exception as exc:
                raise LabWireSessionError(
                    f"{label} session transport failed: {_bounded_exception_message(exc)}"
                ) from exc
            if result.operation != operation:
                raise LabDaemonConfigurationError(f"{label} operation mismatch")
            if result.error_type is not None:
                raise LabDaemonConfigurationError(
                    f"{label} failed: {result.error_type}: {result.message or 'unknown failure'}"
                )
        except BaseException as exc:
            primary_error = exc
        cleanup_error: BaseException | None = None
        if managed_child is not None:
            cleanup_error = self._close_managed_authority_child(
                managed_child,
                owner="direct",
                label=label,
            )
        elif child is not None:
            try:
                self._close_wire_child(child, label=label)
            except BaseException as exc:
                cleanup_error = exc
        if primary_error is not None and cleanup_error is not None:
            raise BaseExceptionGroup(
                f"{label} and cleanup failed",
                [primary_error, cleanup_error],
            )
        if primary_error is not None:
            raise primary_error
        if cleanup_error is not None:
            raise cleanup_error
        if result is None:  # pragma: no cover - guarded above
            raise LabDaemonConfigurationError(f"{label} returned no evidence")
        return result

    def _run_admission_authority(
        self,
        *,
        spec: ResearchRunSpec,
        request: AdmissionRequest,
        timeout_microseconds: int,
        not_after_monotonic_microseconds: int,
        cancellation_requested: Callable[[], bool] | None = None,
    ) -> _ResourceAdmissionEvaluation:
        if cancellation_requested is not None and cancellation_requested():
            raise InterruptedError("resource admission authority was cancelled")
        stage_count = 2 + int(request.expected_quota_units > 0)
        result = self._run_authority_stage(
            operation="admission",
            spec=spec,
            admission_request=request,
            timeout_microseconds=timeout_microseconds * stage_count,
            not_after_monotonic_microseconds=not_after_monotonic_microseconds,
            cancellation_requested=cancellation_requested,
        )
        return self._admission_evaluation_from_authority_result(
            result,
            request=request,
            cancellation_requested=cancellation_requested,
        )

    def _admission_evaluation_from_authority_result(
        self,
        result: _AuthorityWireResult,
        *,
        request: AdmissionRequest,
        cancellation_requested: Callable[[], bool] | None = None,
    ) -> _ResourceAdmissionEvaluation:
        if cancellation_requested is not None and cancellation_requested():
            raise InterruptedError("resource admission authority was cancelled")
        if result.policy is None:
            raise LabDaemonConfigurationError(
                "admission policy provider returned an invalid contract"
            )
        if result.snapshot is None:
            raise LabDaemonConfigurationError(
                "resource snapshot provider returned an invalid contract"
            )
        if cancellation_requested is not None and cancellation_requested():
            raise InterruptedError("resource admission authority was cancelled")
        self._record_snapshot_authority_state(result.authority_state)
        now = _utc(self.clock())
        if result.snapshot.observed_at > now:
            raise LabDaemonConfigurationError("resource snapshot is from the future")
        if (
            timedelta_microseconds(now - result.snapshot.observed_at)
            > result.policy.max_snapshot_age_microseconds
        ):
            raise LabDaemonConfigurationError("resource snapshot is stale")
        return _ResourceAdmissionEvaluation(
            decision=evaluate_admission(
                request,
                result.snapshot,
                result.policy,
                quota_lease=result.quota_lease,
            ),
            request=request,
            snapshot=result.snapshot,
            policy=result.policy,
            quota_lease=result.quota_lease,
        )

    def _remaining_prepublication_budget_microseconds(
        self,
        prepublication_deadline_microseconds: int | None,
        *,
        operation: str,
    ) -> int | None:
        if prepublication_deadline_microseconds is None:
            return None
        remaining_microseconds = (
            prepublication_deadline_microseconds - self.monotonic_microseconds_clock()
        )
        if remaining_microseconds <= 0:
            raise TimeoutError(f"pre-publication admission deadline reached before {operation}")
        return remaining_microseconds

    def _early_prepublication_probe_timeout_microseconds(
        self,
        prepublication_deadline_microseconds: int | None,
        *,
        operation: str,
    ) -> int:
        if prepublication_deadline_microseconds is None:
            return self.resource_probe_timeout_microseconds
        remaining_microseconds = self._remaining_prepublication_budget_microseconds(
            prepublication_deadline_microseconds,
            operation=operation,
        )
        if remaining_microseconds is None:  # pragma: no cover - guarded above
            return self.resource_probe_timeout_microseconds
        final_probe_reserve_microseconds = min(
            self.resource_probe_timeout_microseconds,
            _AUTHORITY_SPAWN_ALLOWANCE_MICROSECONDS,
        )
        available_microseconds = remaining_microseconds - final_probe_reserve_microseconds
        if available_microseconds <= 0:
            raise TimeoutError("pre-publication admission deadline has no final authority reserve")
        return min(self.resource_probe_timeout_microseconds, available_microseconds)

    def _resource_admission_evaluation(
        self,
        claim: LabShardClaim,
        spec: ResearchRunSpec,
        *,
        probe_timeout_microseconds: int | None = None,
        authority_not_after_monotonic_microseconds: int | None = None,
        reservation_recheck_gate: Callable[[], None] | None = None,
        cancellation_requested: Callable[[], bool] | None = None,
        tick_deadline_microseconds: int | None = None,
        prestarted_admission_stage: _PrestartedAuthorityStage | None = None,
    ) -> _ResourceAdmissionEvaluation | None:
        def cancelled() -> bool:
            return self._stop.is_set() or (
                cancellation_requested is not None and cancellation_requested()
            )

        if cancelled():
            raise InterruptedError("resource admission evaluation was cancelled")
        if self.resource_authority_manifest is None:
            if self.require_resource_admission:
                raise LabDaemonConfigurationError(
                    "isolated worker resource authority is unavailable"
                )
            return None
        timeout_microseconds = (
            self.resource_probe_timeout_microseconds
            if probe_timeout_microseconds is None
            else min(
                self.resource_probe_timeout_microseconds,
                probe_timeout_microseconds,
            )
        )
        remaining_tick_budget = self._remaining_prepublication_budget_microseconds(
            tick_deadline_microseconds,
            operation="resource admission authority",
        )
        if remaining_tick_budget is not None:
            timeout_microseconds = min(timeout_microseconds, remaining_tick_budget)
        if authority_not_after_monotonic_microseconds is None:
            remaining_spec = timedelta_microseconds(spec.deadline - _utc(self.clock()))
            authority_not_after_monotonic_microseconds = _monotonic_microseconds() + max(
                0, remaining_spec
            )
        if remaining_tick_budget is not None:
            authority_not_after_monotonic_microseconds = min(
                authority_not_after_monotonic_microseconds,
                _monotonic_microseconds() + remaining_tick_budget,
            )
        request = self._resource_admission_request(claim, spec)
        if prestarted_admission_stage is None:
            evidence = self._run_admission_authority(
                spec=spec,
                request=request,
                timeout_microseconds=timeout_microseconds,
                not_after_monotonic_microseconds=authority_not_after_monotonic_microseconds,
                cancellation_requested=cancelled,
            )
        else:
            evidence = self._admission_evaluation_from_authority_result(
                self._complete_prestarted_authority_stage(
                    prestarted_admission_stage,
                    operation="admission",
                    deadline_microseconds=authority_not_after_monotonic_microseconds,
                ),
                request=request,
                cancellation_requested=cancelled,
            )
        if cancelled():
            raise InterruptedError("resource admission evaluation was cancelled")
        self._remaining_prepublication_budget_microseconds(
            tick_deadline_microseconds,
            operation="resource admission recheck",
        )
        policy = evidence.policy
        snapshot = evidence.snapshot
        quota_lease = evidence.quota_lease

        def immutable_snapshot_provider() -> ResourceSnapshot:
            return snapshot

        def immutable_quota_lease_provider(
            _request: AdmissionRequest,
            _snapshot: ResourceSnapshot,
        ) -> SourceQuotaLease | None:
            return quota_lease

        if reservation_recheck_gate is not None:
            reservation_recheck_gate()
        remaining_tick_budget = self._remaining_prepublication_budget_microseconds(
            tick_deadline_microseconds,
            operation="resource reservation recheck",
        )
        with self._resource_reservation_lock:
            if cancelled():
                raise InterruptedError("resource admission evaluation was cancelled")
            active_lease = self._active_resource_reservation
        if active_lease is not None:
            store = self.resource_reservation_store
            if store is None:  # pragma: no cover - constructor invariant
                raise LabDaemonConfigurationError(
                    "active resource reservation has no persistent store"
                )
            try:
                admitted = store.recheck(
                    lease=active_lease,
                    identity=self._resource_reservation_identity(claim),
                    request=request,
                    policy=policy,
                    snapshot_provider=immutable_snapshot_provider,
                    lease_seconds=self.lease_extension_seconds,
                    quota_lease_provider=immutable_quota_lease_provider,
                    lock_wait_timeout_seconds=_microseconds_to_seconds(
                        min(
                            _RESOURCE_RESERVATION_LOCK_WAIT_MAX_MICROSECONDS,
                            remaining_tick_budget
                            if remaining_tick_budget is not None
                            else _RESOURCE_RESERVATION_LOCK_WAIT_MAX_MICROSECONDS,
                            max(
                                0,
                                authority_not_after_monotonic_microseconds
                                - _monotonic_microseconds(),
                            ),
                        )
                    ),
                    stop_requested=cancelled,
                )
            except Exception as exc:
                if cancelled():
                    raise InterruptedError(
                        "worker stop requested during resource reservation recheck"
                    ) from exc
                if _monotonic_microseconds() >= authority_not_after_monotonic_microseconds:
                    raise TimeoutError("resource admission recheck timed out") from exc
                if isinstance(exc, LabDaemonConfigurationError):
                    raise
                raise LabDaemonConfigurationError(
                    str(exc) or "resource reservation recheck failed"
                ) from exc
            if cancelled():
                raise InterruptedError("resource admission evaluation was cancelled")
            self._remaining_prepublication_budget_microseconds(
                tick_deadline_microseconds,
                operation="resource reservation recheck result",
            )
            with self._resource_reservation_lock:
                if cancelled():
                    raise InterruptedError("resource admission evaluation was cancelled")
                if self._active_resource_reservation != active_lease:
                    raise LabDaemonConfigurationError("resource reservation changed during recheck")
                if admitted.lease is not None:
                    self._active_resource_reservation = admitted.lease
            return _ResourceAdmissionEvaluation(
                decision=admitted.decision,
                request=admitted.request,
                snapshot=admitted.snapshot,
                policy=admitted.policy,
                quota_lease=quota_lease,
            )
        if cancelled():
            raise InterruptedError("resource admission evaluation was cancelled")
        self._remaining_prepublication_budget_microseconds(
            tick_deadline_microseconds,
            operation="resource admission result",
        )
        return _ResourceAdmissionEvaluation(
            decision=evaluate_admission(request, snapshot, policy, quota_lease=quota_lease),
            request=request,
            snapshot=snapshot,
            policy=policy,
            quota_lease=quota_lease,
        )

    def _resource_admission_inputs(
        self,
        claim: LabShardClaim,
        spec: ResearchRunSpec,
        *,
        policy_override: AdmissionPolicy | None = None,
        authority_callback_timeout_microseconds: int | None = None,
    ) -> tuple[AdmissionPolicy, AdmissionRequest]:
        policy = policy_override
        if policy is None:
            result = self._run_authority_stage(
                operation="policy",
                spec=spec,
                timeout_microseconds=(
                    self.resource_probe_timeout_microseconds
                    if authority_callback_timeout_microseconds is None
                    else authority_callback_timeout_microseconds
                ),
            )
            policy = result.policy
        if policy is None:
            raise LabDaemonConfigurationError(
                "admission policy provider returned an invalid contract"
            )
        return policy, self._resource_admission_request(claim, spec)

    @staticmethod
    def _resource_admission_request(
        claim: LabShardClaim,
        spec: ResearchRunSpec,
    ) -> AdmissionRequest:
        try:
            return derive_lab_admission_request(
                job_id=claim.job_id,
                spec=spec,
                work_plan=claim.definition.work_plan,
            )
        except Exception as exc:
            raise LabDaemonConfigurationError(
                "resource admission request derivation failed"
            ) from exc

    def _bounded_initial_resource_admission(
        self,
        claim: LabShardClaim,
        spec: ResearchRunSpec,
        *,
        timeout_microseconds: int,
        not_after_monotonic_microseconds: int,
    ) -> _ResourceAdmissionEvaluation | None:
        if self.resource_authority_manifest is None:
            if self.require_resource_admission:
                raise LabDaemonConfigurationError(
                    "isolated worker resource authority is unavailable"
                )
            return None
        request = self._resource_admission_request(claim, spec)
        return self._run_admission_authority(
            spec=spec,
            request=request,
            timeout_microseconds=timeout_microseconds,
            not_after_monotonic_microseconds=not_after_monotonic_microseconds,
        )

    def _source_quota_lease(
        self,
        spec: ResearchRunSpec,
        request: AdmissionRequest,
        snapshot: ResourceSnapshot,
        *,
        authority_callback_timeout_microseconds: int | None = None,
        authority_not_after_monotonic_microseconds: int | None = None,
    ) -> SourceQuotaLease | None:
        if request.expected_quota_units <= 0:
            return None
        result = self._run_authority_stage(
            operation="quota",
            spec=spec,
            admission_request=request,
            snapshot=snapshot,
            timeout_microseconds=(
                self.resource_probe_timeout_microseconds
                if authority_callback_timeout_microseconds is None
                else authority_callback_timeout_microseconds
            ),
            not_after_monotonic_microseconds=(authority_not_after_monotonic_microseconds),
        )
        return result.quota_lease

    def _resource_reservation_identity(
        self,
        claim: LabShardClaim,
    ) -> ResourceReservationIdentity:
        return ResourceReservationIdentity(
            job_id=claim.job_id,
            run_id=claim.spec_hash,
            shard_id=claim.shard_id,
            attempt_id=claim.claim_token,
            claim_generation=claim.claim_generation,
            scheduler_fencing_token=claim.scheduler_fencing_token,
            worker_id=claim.worker_id,
        )

    def _reserve_resource_admission(
        self,
        claim: LabShardClaim,
        spec: ResearchRunSpec,
        *,
        tick_deadline_microseconds: int | None = None,
    ) -> _ResourceAdmissionEvaluation | None:
        if self.resource_authority_manifest is None:
            if self.require_resource_admission:
                raise LabDaemonConfigurationError(
                    "isolated worker resource authority is unavailable"
                )
            return None
        remaining_microseconds = timedelta_microseconds(spec.deadline - _utc(self.clock()))
        remaining_tick_budget = self._remaining_prepublication_budget_microseconds(
            tick_deadline_microseconds,
            operation="initial resource admission",
        )
        if remaining_tick_budget is not None:
            remaining_microseconds = min(remaining_microseconds, remaining_tick_budget)
        if remaining_microseconds <= 0:
            raise TimeoutError("ResearchRunSpec deadline reached before resource reservation")
        admission_deadline_microseconds = (
            self.monotonic_microseconds_clock() + remaining_microseconds
        )
        authority_not_after_monotonic_microseconds = (
            _monotonic_microseconds() + remaining_microseconds
        )
        operation_timeout_microseconds = min(
            remaining_microseconds,
            self._early_prepublication_probe_timeout_microseconds(
                tick_deadline_microseconds,
                operation="initial resource admission authority",
            ),
        )
        evaluation = self._bounded_initial_resource_admission(
            claim,
            spec,
            timeout_microseconds=operation_timeout_microseconds,
            not_after_monotonic_microseconds=authority_not_after_monotonic_microseconds,
        )
        if evaluation is None:
            return None
        if self._stop.is_set():
            raise InterruptedError("worker stop requested during resource reservation admission")
        remaining_tick_budget = self._remaining_prepublication_budget_microseconds(
            tick_deadline_microseconds,
            operation="resource reservation transaction",
        )
        if remaining_tick_budget is not None:
            remaining_tick_budget -= min(
                self.resource_probe_timeout_microseconds,
                _AUTHORITY_SPAWN_ALLOWANCE_MICROSECONDS,
            )
        transaction_timeout_microseconds = min(
            operation_timeout_microseconds,
            remaining_tick_budget
            if remaining_tick_budget is not None
            else operation_timeout_microseconds,
            max(
                0,
                admission_deadline_microseconds - self.monotonic_microseconds_clock(),
            ),
        )
        if transaction_timeout_microseconds <= 0:
            raise TimeoutError(
                "ResearchRunSpec deadline reached before resource reservation transaction"
            )
        if self.resource_reservation_store is None:
            return evaluation

        def immutable_snapshot_provider() -> ResourceSnapshot:
            return evaluation.snapshot

        def immutable_quota_lease_provider(
            _request: AdmissionRequest,
            _snapshot: ResourceSnapshot,
        ) -> SourceQuotaLease | None:
            return evaluation.quota_lease

        with self._resource_reservation_lock:
            if self._active_resource_reservation is not None:
                raise LabDaemonConfigurationError(
                    "worker already owns an active resource reservation"
                )
            try:
                admitted = self.resource_reservation_store.reserve(
                    identity=self._resource_reservation_identity(claim),
                    request=evaluation.request,
                    policy=evaluation.policy,
                    snapshot_provider=immutable_snapshot_provider,
                    lease_seconds=self.lease_extension_seconds,
                    quota_lease_provider=immutable_quota_lease_provider,
                    lock_wait_timeout_seconds=_microseconds_to_seconds(
                        min(
                            _RESOURCE_RESERVATION_LOCK_WAIT_MAX_MICROSECONDS,
                            transaction_timeout_microseconds,
                        )
                    ),
                    stop_requested=self._stop.is_set,
                )
            except Exception as exc:
                if self._stop.is_set():
                    raise InterruptedError(
                        "worker stop requested during resource reservation admission"
                    ) from exc
                if self.monotonic_microseconds_clock() >= admission_deadline_microseconds:
                    raise TimeoutError(
                        "ResearchRunSpec deadline reached during resource reservation admission"
                    ) from exc
                if isinstance(exc, LabDaemonConfigurationError):
                    raise
                raise LabDaemonConfigurationError(
                    str(exc) or "resource reservation admission failed"
                ) from exc
            if admitted.lease is not None:
                self._remaining_prepublication_budget_microseconds(
                    tick_deadline_microseconds,
                    operation="resource reservation admission result",
                )
                self._active_resource_reservation = admitted.lease
            if self._stop.is_set():
                raise InterruptedError(
                    "worker stop requested during resource reservation admission"
                )
            if (
                _utc(self.clock()) >= spec.deadline
                or self.monotonic_microseconds_clock() >= admission_deadline_microseconds
            ):
                raise TimeoutError(
                    "ResearchRunSpec deadline reached during resource reservation admission"
                )
            self._remaining_prepublication_budget_microseconds(
                tick_deadline_microseconds,
                operation="resource reservation admission result",
            )
            return _ResourceAdmissionEvaluation(
                decision=admitted.decision,
                request=admitted.request,
                snapshot=admitted.snapshot,
                policy=admitted.policy,
                quota_lease=evaluation.quota_lease,
            )

    def _release_resource_reservation(self) -> None:
        with self._resource_reservation_lock:
            lease = self._active_resource_reservation
            if lease is None:
                return
            store = self.resource_reservation_store
            if store is None:  # pragma: no cover - constructor invariant
                raise LabDaemonConfigurationError(
                    "active resource reservation has no persistent store"
                )
            self._active_resource_reservation = None
            store.release(
                lease,
                identity=lease.identity,
                lock_wait_timeout_seconds=_microseconds_to_seconds(
                    _RESOURCE_RESERVATION_LOCK_WAIT_MAX_MICROSECONDS
                ),
            )

    def _accepted_snapshot_authority_state(self) -> LabSnapshotAuthorityState | None:
        with self._resource_snapshot_authority_state_lock:
            return self._resource_snapshot_authority_state

    def _record_snapshot_authority_state(
        self,
        state: LabSnapshotAuthorityState | None,
    ) -> None:
        if state is not None and type(state) is not LabSnapshotAuthorityState:
            raise LabDaemonConfigurationError(
                "resource snapshot authority returned an invalid closed state"
            )
        with self._resource_snapshot_authority_state_lock:
            self._resource_snapshot_authority_state = state

    @property
    def snapshot_authority_watermark(
        self,
    ) -> RuntimeHealthAuthorityWatermark | None:
        state = self._accepted_snapshot_authority_state()
        if state is None or state.state_kind != "runtime-health-watermark":
            return None
        return _runtime_watermark_from_state(state)

    def _bounded_resource_snapshot(self, *, timeout_seconds: float) -> ResourceSnapshot:
        timeout_microseconds = _positive_duration_microseconds(
            timeout_seconds,
            label="resource snapshot timeout_seconds",
        )
        try:
            return self._bounded_resource_snapshot_microseconds(
                timeout_microseconds=timeout_microseconds
            )
        except TimeoutError as exc:
            raise LabDaemonConfigurationError("resource snapshot provider timed out") from exc
        except BaseExceptionGroup as exc:
            if exc.subgroup(TimeoutError) is None:
                raise

            def replace_timeout(error: BaseException) -> BaseException:
                if isinstance(error, TimeoutError):
                    replacement = LabDaemonConfigurationError(
                        "resource snapshot provider timed out"
                    )
                    replacement.__cause__ = error
                    return replacement
                if isinstance(error, BaseExceptionGroup):
                    return error.derive(
                        tuple(replace_timeout(nested) for nested in error.exceptions)
                    )
                return error

            raise replace_timeout(exc) from exc

    def _bounded_resource_snapshot_microseconds(
        self,
        *,
        timeout_microseconds: int,
        not_after_monotonic_microseconds: int | None = None,
        include_spawn_allowance: bool = False,
    ) -> ResourceSnapshot:
        result = self._run_authority_stage(
            operation="snapshot",
            spec=None,
            timeout_microseconds=timeout_microseconds,
            not_after_monotonic_microseconds=not_after_monotonic_microseconds,
            include_spawn_allowance=include_spawn_allowance,
        )
        if result.snapshot is None:
            raise LabDaemonConfigurationError(
                "resource snapshot provider returned an invalid contract"
            )
        self._record_snapshot_authority_state(result.authority_state)
        return result.snapshot

    def _verified_runtime_code_sha(self, spec: ResearchRunSpec) -> str:
        return self._verify_runtime_guard(expected_sha=spec.code_sha)

    def _validate_closed_claim(
        self, claim: LabShardClaim | LabShardClaimV2
    ) -> ValidatedStrategyShard:
        binding = self.shard_runtime_manifest.registry
        if binding.registry_id == _TEST_SHARD_REGISTRY_ID:
            configuration = strict_canonical_json_loads(binding.configuration_json)
            if (
                isinstance(configuration, dict)
                and configuration.get("bypass_parent_validation") is True
            ):
                payload = StrategyShardPayload.model_validate_json(claim.definition.payload_json)
                return ValidatedStrategyShard(
                    claim=claim,
                    spec=payload.spec,
                    shard=payload.shard,
                )
        return self.adapter_registry.validate_claim(claim)

    def _heartbeat_loop(
        self,
        claim: LabShardClaim,
        finished: threading.Event,
        errors: list[Exception],
    ) -> None:
        heartbeat_interval_seconds = _microseconds_to_seconds(self.heartbeat_interval_microseconds)
        while not finished.wait(heartbeat_interval_seconds):
            try:
                self._publish_report(
                    claim,
                    LabShardHeartbeat(
                        lease_extension_seconds=self.lease_extension_seconds,
                    ),
                )
            except Exception as exc:
                errors.append(exc)
                finished.set()

    def _resource_monitor_loop(
        self,
        claim: LabShardClaim,
        spec: ResearchRunSpec,
        finished: threading.Event,
        preemptions: list[AdmissionDecision],
        session_failures: list[_ClassifiedWireFailure],
        errors: list[Exception],
    ) -> None:
        resource_recheck_interval_seconds = _microseconds_to_seconds(
            self.resource_recheck_interval_microseconds
        )
        while True:
            if finished.wait(resource_recheck_interval_seconds):
                return
            try:
                decision = self._resource_admission_decision(
                    claim,
                    spec,
                )
            except (LabWireSessionStartupError, LabWireSessionError) as exc:
                session_failures.append(_classify_wire_failure(exc))
                return
            except Exception as exc:
                if _extract_wire_session_error(exc) is not None:
                    session_failures.append(_classify_wire_failure(exc))
                else:
                    errors.append(exc)
                return
            if decision is not None and decision.outcome is not AdmissionOutcome.ADMITTED:
                preemptions.append(decision)
                return

    @staticmethod
    def _terminate_isolated_process(
        process: BaseProcess,
        *,
        isolated_group_id: int | None,
        allow_graceful_termination: bool = True,
    ) -> None:
        errors: list[BaseException] = []

        def attempt(
            label: str,
            action: Callable[[], object],
            *,
            process_lookup_is_success: bool = False,
        ) -> None:
            try:
                action()
            except ProcessLookupError as exc:
                if not process_lookup_is_success:
                    errors.append(BaseExceptionGroup(f"{label} failed", [exc]))
            except PermissionError as exc:
                if not process_lookup_is_success:
                    errors.append(BaseExceptionGroup(f"{label} failed", [exc]))
            except BaseException as exc:
                errors.append(BaseExceptionGroup(f"{label} failed", [exc]))

        try:
            pid = process.pid
        except BaseException as exc:
            errors.append(BaseExceptionGroup("process PID probe failed", [exc]))
            pid = None
        try:
            parent_group_id = os.getpgrp()
        except BaseException as exc:
            errors.append(BaseExceptionGroup("parent process-group probe failed", [exc]))
            parent_group_id = None
        group_owned = (
            pid is not None
            and isolated_group_id == pid
            and parent_group_id is not None
            and isolated_group_id != parent_group_id
        )

        def pid_exists() -> bool:
            if pid is None:
                return False
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return False
            except BaseException as exc:
                errors.append(BaseExceptionGroup("PID existence probe failed", [exc]))
                return True
            return True

        def direct_child_active() -> bool:
            try:
                if process.exitcode is not None:
                    return False
            except BaseException as exc:
                errors.append(BaseExceptionGroup("process exitcode probe failed", [exc]))
            return pid_exists()

        def group_exists() -> bool:
            if not group_owned or isolated_group_id is None:
                return False
            try:
                os.killpg(isolated_group_id, 0)
            except ProcessLookupError:
                return False
            except PermissionError:
                return True
            except BaseException as exc:
                errors.append(BaseExceptionGroup("process-group existence probe failed", [exc]))
                return True
            return True

        attempt("initial process reap", lambda: process.join(timeout=0))
        if allow_graceful_termination:
            if group_owned and isolated_group_id is not None:
                attempt(
                    "process-group SIGTERM",
                    lambda: os.killpg(isolated_group_id, signal.SIGTERM),
                    process_lookup_is_success=True,
                )
            if direct_child_active():
                attempt(
                    "PID SIGTERM",
                    process.terminate,
                    process_lookup_is_success=True,
                )
            attempt(
                "post-SIGTERM process reap",
                lambda: process.join(_microseconds_to_seconds(_CHILD_TERMINATE_GRACE_MICROSECONDS)),
            )

        retry_join_microseconds = max(
            1,
            _CHILD_TERMINATE_GRACE_MICROSECONDS // _PROCESS_CLEANUP_RETRIES,
        )
        for _attempt_number in range(_PROCESS_CLEANUP_RETRIES):
            if direct_child_active():
                attempt(
                    "PID SIGKILL",
                    process.kill,
                    process_lookup_is_success=True,
                )
            if group_owned and isolated_group_id is not None:
                attempt(
                    "process-group SIGKILL",
                    lambda: os.killpg(isolated_group_id, signal.SIGKILL),
                    process_lookup_is_success=True,
                )
            attempt(
                "post-SIGKILL process reap",
                lambda: process.join(_microseconds_to_seconds(retry_join_microseconds)),
            )

        process_reported_alive = False
        try:
            process_reported_alive = process.is_alive()
        except BaseException as exc:
            errors.append(BaseExceptionGroup("process liveness probe failed", [exc]))

        process_pid_exists = direct_child_active()
        process_group_exists = group_exists()
        verification_deadline_microseconds = (
            _monotonic_microseconds() + _CHILD_TERMINATE_GRACE_MICROSECONDS
        )
        while (
            process_pid_exists or process_group_exists
        ) and _monotonic_microseconds() < verification_deadline_microseconds:
            remaining_microseconds = max(
                1,
                verification_deadline_microseconds - _monotonic_microseconds(),
            )
            attempt(
                "verification process reap",
                lambda remaining_microseconds=remaining_microseconds: process.join(
                    _microseconds_to_seconds(min(10_000, remaining_microseconds))
                ),
            )
            process_pid_exists = direct_child_active()
            process_group_exists = group_exists()

        try:
            process_reported_alive = process.is_alive()
        except BaseException as exc:
            errors.append(BaseExceptionGroup("final process liveness probe failed", [exc]))

        if process_reported_alive or process_pid_exists:
            errors.append(RuntimeError("isolated shard child could not be terminated and reaped"))
        if process_group_exists:
            errors.append(RuntimeError("isolated shard process group still exists after cleanup"))
        if errors:
            raise BaseExceptionGroup("isolated process cleanup failed", errors)

    def _finish_pre_ack_admission_stage(
        self,
        stage: _PreAckAdmissionStage,
    ) -> BaseException | None:
        stage.cancelled.set()
        thread = stage.thread
        if thread is None:
            return LabDaemonConfigurationError("pre-ACK admission stage has no thread")
        cleanup_deadline = _monotonic_microseconds() + _AUTHORITY_CHILD_CLEANUP_BUDGET_MICROSECONDS
        try:
            remaining = cleanup_deadline - _monotonic_microseconds()
            thread.join(_microseconds_to_seconds(remaining))
        except BaseException as exc:
            cleanup_error: BaseException = exc
        else:
            cleanup_error = TimeoutError("pre-ACK admission thread cleanup timed out")
            if not thread.is_alive() and stage.completion.is_set():
                self._pre_ack_admission_diagnostics = ()
                return None
            if not thread.is_alive():
                cleanup_error = RuntimeError(
                    "pre-ACK admission thread exited without completion evidence"
                )

        with self._managed_authority_children_lock:
            managed_children = tuple(self._managed_authority_children.values())
        diagnostics = [
            f"thread={thread.name}",
            f"thread_alive={thread.is_alive()}",
            f"completion={stage.completion.is_set()}",
            f"authority_deadline={stage.deadline_microseconds}",
        ]
        for managed in managed_children:
            with managed.lock:
                diagnostics.append(
                    "authority_child="
                    f"pid:{managed.cached_pid},owner:{managed.owner},"
                    f"cleanup_in_progress:{managed.cleanup_in_progress},"
                    f"cleanup_retries:{managed.cleanup_retry_count}"
                )
        self._pre_ack_admission_diagnostics = tuple(diagnostics)
        return cleanup_error

    def _execute_shard_isolated(
        self,
        claim: LabShardClaim,
        validated: ValidatedStrategyShard,
        *,
        runtime_code_sha: str,
        hard_limit_seconds: float,
        initial_session: TradingSession,
        tick_deadline_microseconds: int | None = None,
    ) -> _IsolatedExecutionControl:
        hard_limit_microseconds = _positive_duration_microseconds(
            hard_limit_seconds,
            label="isolated live shard hard_limit_seconds",
        )
        hard_limit_milliseconds = hard_limit_microseconds // 1_000
        parent_started = self.isolation_monotonic_microseconds_clock()
        deadline_remaining_microseconds = max(
            0,
            timedelta_microseconds(validated.spec.deadline - _utc(self.clock())),
        )
        spec_deadline = parent_started + deadline_remaining_microseconds
        hard_deadline = spec_deadline
        child_budget = deadline_remaining_microseconds
        spec_child_deadline = _monotonic_microseconds() + child_budget
        result_child_deadline = spec_child_deadline
        ack_live_limit_microseconds = (
            hard_limit_microseconds if initial_session in _LIVE_TRADING_SESSIONS else None
        )
        request = _ShardWireRequest(
            manifest=self.shard_runtime_manifest,
            validated=validated,
            runtime_code_sha=runtime_code_sha,
        )
        finished = threading.Event()
        heartbeat_errors: list[Exception] = []
        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(claim, finished, heartbeat_errors),
            name=f"lab-heartbeat-{claim.claim_token}",
            daemon=True,
        )
        child: _WireChild | None = None
        outcome: _IsolatedExecutionOutcome | None = None
        stop_reason: str | None = None
        preemption: AdmissionDecision | None = None
        session_failure: _ClassifiedWireFailure | None = None
        resource_error: Exception | None = None
        lifecycle_error: BaseException | None = None
        cleanup_errors: list[BaseException] = []
        pre_ack_stage: _PreAckAdmissionStage | None = None
        child_ready = False
        isolation_start_aborted = False

        def deadline_stop_reason(now_microseconds: int) -> str:
            if now_microseconds >= spec_deadline:
                return "ResearchRunSpec deadline reached during isolated shard execution"
            return f"hard live execution limit reached: {hard_limit_milliseconds}ms"

        def apply_resource_evaluation(
            evaluation: _ResourceAdmissionEvaluation | None,
            now_microseconds: int,
            *,
            execution_active: bool,
        ) -> None:
            nonlocal ack_live_limit_microseconds, hard_deadline, preemption
            nonlocal result_child_deadline, stop_reason
            if (
                evaluation is not None
                and evaluation.decision.outcome is not AdmissionOutcome.ADMITTED
            ):
                preemption = evaluation.decision
                return
            if evaluation is not None and evaluation.snapshot.session in _LIVE_TRADING_SESSIONS:
                live_limit = evaluation.policy.max_live_shard_duration_ms * 1_000
                if execution_active:
                    hard_deadline = min(hard_deadline, now_microseconds + live_limit)
                    result_child_deadline = min(
                        result_child_deadline,
                        _monotonic_microseconds() + live_limit,
                    )
                else:
                    ack_live_limit_microseconds = (
                        live_limit
                        if ack_live_limit_microseconds is None
                        else min(ack_live_limit_microseconds, live_limit)
                    )
            active_deadline = hard_deadline if execution_active else spec_deadline
            if self.isolation_monotonic_microseconds_clock() >= active_deadline:
                stop_reason = deadline_stop_reason(self.isolation_monotonic_microseconds_clock())

        def await_child_readiness_before_reservation_recheck() -> None:
            with self._isolation_start_condition:
                self._isolation_start_condition.wait_for(
                    lambda: child_ready or isolation_start_aborted or self._stop.is_set()
                )
                if self._stop.is_set():
                    raise InterruptedError(
                        "worker stop requested before resource reservation recheck"
                    )
                if isolation_start_aborted:
                    raise InterruptedError(
                        "isolated shard startup aborted before resource reservation recheck"
                    )

        def refresh_admission(
            *,
            execution_active: bool,
            wait_for_child_readiness: bool = False,
        ) -> None:
            nonlocal resource_error, session_failure, stop_reason
            authority_deadline = hard_deadline if execution_active else spec_deadline
            authority_child_deadline = (
                result_child_deadline if execution_active else spec_child_deadline
            )
            remaining = authority_deadline - self.isolation_monotonic_microseconds_clock()
            if remaining <= 0:
                stop_reason = deadline_stop_reason(self.isolation_monotonic_microseconds_clock())
                return
            try:
                evaluation = self._resource_admission_evaluation(
                    claim,
                    validated.spec,
                    probe_timeout_microseconds=min(
                        self.resource_probe_timeout_microseconds,
                        remaining,
                    ),
                    authority_not_after_monotonic_microseconds=min(
                        authority_child_deadline,
                        _monotonic_microseconds() + remaining,
                    ),
                    reservation_recheck_gate=(
                        await_child_readiness_before_reservation_recheck
                        if wait_for_child_readiness
                        else None
                    ),
                    tick_deadline_microseconds=(
                        None if execution_active else tick_deadline_microseconds
                    ),
                )
            except (InterruptedError, TimeoutError):
                if self._stop.is_set():
                    stop_reason = "worker stop requested during resource reservation recheck"
                else:
                    stop_reason = deadline_stop_reason(
                        self.isolation_monotonic_microseconds_clock()
                    )
                return
            except (LabWireSessionStartupError, LabWireSessionError) as exc:
                session_failure = _classify_wire_failure(exc)
                return
            except Exception as exc:
                if _extract_wire_session_error(exc) is not None:
                    session_failure = _classify_wire_failure(exc)
                else:
                    resource_error = exc
                return
            apply_resource_evaluation(
                evaluation,
                self.isolation_monotonic_microseconds_clock(),
                execution_active=execution_active,
            )

        def refresh_pre_ack_admission() -> None:
            if pre_ack_stage is None:  # pragma: no cover - startup invariant
                raise RuntimeError("pre-ACK admission stage is unavailable")
            stage = pre_ack_stage

            def cancelled() -> bool:
                return stage.cancelled.is_set() or self._stop.is_set()

            try:
                remaining = spec_deadline - self.isolation_monotonic_microseconds_clock()
                if remaining <= 0:
                    if not cancelled():
                        stage.stop_reason = deadline_stop_reason(
                            self.isolation_monotonic_microseconds_clock()
                        )
                    return
                pre_ack_timeout_microseconds = (
                    self._early_prepublication_probe_timeout_microseconds(
                        tick_deadline_microseconds,
                        operation="pre-ACK resource admission authority",
                    )
                )
                evaluation = self._resource_admission_evaluation(
                    claim,
                    validated.spec,
                    probe_timeout_microseconds=min(
                        self.resource_probe_timeout_microseconds,
                        remaining,
                        pre_ack_timeout_microseconds,
                    ),
                    authority_not_after_monotonic_microseconds=min(
                        spec_child_deadline,
                        _monotonic_microseconds() + remaining,
                    ),
                    reservation_recheck_gate=await_child_readiness_before_reservation_recheck,
                    cancellation_requested=cancelled,
                    tick_deadline_microseconds=tick_deadline_microseconds,
                )
            except (InterruptedError, TimeoutError):
                if not cancelled():
                    stage.stop_reason = deadline_stop_reason(
                        self.isolation_monotonic_microseconds_clock()
                    )
            except (LabWireSessionStartupError, LabWireSessionError) as exc:
                if not cancelled():
                    stage.session_failure = _classify_wire_failure(exc)
            except Exception as exc:
                if not cancelled():
                    if _extract_wire_session_error(exc) is not None:
                        stage.session_failure = _classify_wire_failure(exc)
                    else:
                        stage.resource_error = exc
            else:
                if not cancelled():
                    stage.evaluation = evaluation
            finally:
                stage.completion.set()

        try:
            if self.resource_authority_manifest is not None:
                pre_ack_stage = _PreAckAdmissionStage(
                    cancelled=threading.Event(),
                    completion=threading.Event(),
                    deadline_microseconds=spec_child_deadline,
                )
                pre_ack_resource_refresh = threading.Thread(
                    target=refresh_pre_ack_admission,
                    name=f"lab-pre-ack-resource-{claim.claim_token}",
                    daemon=False,
                )
                pre_ack_stage.thread = pre_ack_resource_refresh
                pre_ack_resource_refresh.start()
            readiness_deadline = min(
                spec_child_deadline,
                _monotonic_microseconds() + _ISOLATION_READY_TIMEOUT_MICROSECONDS,
            )
            child = self._start_wire_child(
                target=_shard_wire_child,
                request_bytes=_encode_wire_message(request),
                process_name=f"lab-shard-{claim.claim_token}",
                deadline_microseconds=readiness_deadline,
                label="isolated shard",
                max_wire_bytes=_MAX_CONTROL_WIRE_BYTES,
                honor_worker_stop_during_readiness=False,
                cancel_requested=self._stop.is_set,
            )
            with self._isolation_start_condition:
                child_ready = True
                self._isolation_start_condition.notify_all()
            if pre_ack_stage is not None:
                while not pre_ack_stage.completion.is_set():
                    now = self.isolation_monotonic_microseconds_clock()
                    if self._stop.is_set():
                        pre_ack_stage.cancelled.set()
                        stop_reason = "worker stop requested before isolated shard start"
                        break
                    if now >= spec_deadline:
                        pre_ack_stage.cancelled.set()
                        stop_reason = deadline_stop_reason(now)
                        break
                    pre_ack_stage.completion.wait(
                        _microseconds_to_seconds(min(10_000, spec_deadline - now))
                    )
                if pre_ack_stage.completion.is_set() and not pre_ack_stage.cancelled.is_set():
                    if pre_ack_stage.stop_reason is not None:
                        stop_reason = pre_ack_stage.stop_reason
                    elif pre_ack_stage.session_failure is not None:
                        session_failure = pre_ack_stage.session_failure
                    elif pre_ack_stage.resource_error is not None:
                        resource_error = pre_ack_stage.resource_error
                    else:
                        apply_resource_evaluation(
                            pre_ack_stage.evaluation,
                            self.isolation_monotonic_microseconds_clock(),
                            execution_active=False,
                        )
            self._before_isolation_start_commit_for_test()
            with self._isolation_start_gate:
                start_generation = self._isolation_stop_generation
                if self._stop.is_set():
                    stop_reason = "worker stop requested before isolated shard start"
                elif self.isolation_monotonic_microseconds_clock() >= hard_deadline:
                    stop_reason = deadline_stop_reason(
                        self.isolation_monotonic_microseconds_clock()
                    )
                if not any(
                    value is not None
                    for value in (stop_reason, preemption, session_failure, resource_error)
                ):
                    self._during_isolation_start_commit_for_test()
                    if self._stop.is_set() or self._isolation_stop_generation != start_generation:
                        stop_reason = "worker stop requested before isolated shard start"
                    else:
                        # The accepted ACK is the child's adapter-start permit.  Stop
                        # requests serialize on this same gate, so they observe either
                        # a pre-ACK stop or an already committed post-ACK execution.
                        try:
                            _send_wire(
                                child.connection,
                                _IsolationStartAck(
                                    accepted=True,
                                    not_after_monotonic_microseconds=spec_child_deadline,
                                    execution_limit_microseconds=ack_live_limit_microseconds,
                                ),
                                deadline_microseconds=spec_child_deadline,
                                cancel_requested=self._stop.is_set,
                            )
                        except (InterruptedError, TimeoutError):
                            raise
                        except Exception as exc:
                            raise LabWireSessionStartupError(
                                "isolated shard start acknowledgement transport failed: "
                                f"{_bounded_exception_message(exc)}"
                            ) from exc
                        ack_success_microseconds = self.isolation_monotonic_microseconds_clock()
                        if ack_success_microseconds >= spec_deadline:
                            stop_reason = deadline_stop_reason(ack_success_microseconds)
                        elif ack_live_limit_microseconds is not None:
                            hard_deadline = min(
                                spec_deadline,
                                ack_success_microseconds + ack_live_limit_microseconds,
                            )
                            result_child_deadline = min(
                                spec_child_deadline,
                                _monotonic_microseconds() + ack_live_limit_microseconds,
                            )
            if not any(
                value is not None
                for value in (stop_reason, preemption, session_failure, resource_error)
            ):
                heartbeat.start()
                next_resource_check = (
                    self.isolation_monotonic_microseconds_clock()
                    + self.resource_recheck_interval_microseconds
                )
                while True:
                    now = self.isolation_monotonic_microseconds_clock()
                    if self._stop.is_set():
                        stop_reason = "worker stop requested during isolated shard execution"
                        break
                    if heartbeat_errors:
                        break
                    if now >= hard_deadline:
                        stop_reason = deadline_stop_reason(now)
                        break
                    try:
                        outcome_available = child.connection.poll(0)
                    except Exception as exc:
                        session_failure = _classify_wire_failure(
                            LabWireSessionError(
                                "isolated shard session poll failed: "
                                f"{_bounded_exception_message(exc)}"
                            )
                        )
                        break
                    if outcome_available:
                        try:
                            candidate = self._receive_wire_before_deadline(
                                child,
                                model=_IsolatedExecutionWireOutcome,
                                max_bytes=_MAX_SHARD_RESULT_WIRE_BYTES,
                                deadline_microseconds=result_child_deadline,
                                label="isolated shard outcome",
                                cancel_requested=self._stop.is_set,
                            )
                        except (InterruptedError, TimeoutError):
                            raise
                        except Exception as exc:
                            raise LabWireSessionError(
                                "isolated shard outcome transport failed: "
                                f"{_bounded_exception_message(exc)}"
                            ) from exc
                        now = self.isolation_monotonic_microseconds_clock()
                        if now >= hard_deadline:
                            stop_reason = deadline_stop_reason(now)
                            break
                        if candidate.result is not None:
                            outcome = _IsolatedExecutionOutcome(result=candidate.result.to_result())
                        else:
                            outcome = _IsolatedExecutionOutcome(
                                phase=candidate.phase,
                                error_type=candidate.error_type,
                                message=candidate.message,
                                configuration_error=candidate.configuration_error,
                            )
                        break
                    if not child.process.is_alive():
                        session_failure = _classify_wire_failure(
                            LabWireSessionError("isolated shard child exited without an outcome")
                        )
                        break
                    if self.resource_authority_manifest is not None and now >= next_resource_check:
                        refresh_admission(execution_active=True)
                        if any(
                            value is not None
                            for value in (
                                stop_reason,
                                preemption,
                                session_failure,
                                resource_error,
                            )
                        ):
                            break
                        next_resource_check = (
                            self.isolation_monotonic_microseconds_clock()
                            + self.resource_recheck_interval_microseconds
                        )
                    wait_microseconds = min(
                        50_000,
                        max(1_000, hard_deadline - now),
                        max(1_000, next_resource_check - now),
                    )
                    self._stop.wait(_microseconds_to_seconds(wait_microseconds))
        except (InterruptedError, TimeoutError) as exc:
            if self._stop.is_set():
                stop_reason = str(exc) or "worker stop requested during isolated shard execution"
            else:
                stop_reason = deadline_stop_reason(self.isolation_monotonic_microseconds_clock())
        except (LabWireSessionStartupError, LabWireSessionError) as exc:
            session_failure = _classify_wire_failure(exc)
        except Exception as exc:
            if _extract_wire_session_error(exc) is not None:
                session_failure = _classify_wire_failure(exc)
            else:
                resource_error = exc
        except BaseException as exc:
            lifecycle_error = exc
        finally:
            if pre_ack_stage is not None:
                pre_ack_stage.cancelled.set()
            with self._isolation_start_condition:
                isolation_start_aborted = True
                self._isolation_start_condition.notify_all()
            if pre_ack_stage is not None:
                stage_cleanup_error = self._finish_pre_ack_admission_stage(pre_ack_stage)
                if stage_cleanup_error is not None:
                    primary_error: BaseException
                    if lifecycle_error is not None:
                        primary_error = lifecycle_error
                    elif session_failure is not None:
                        primary_error = session_failure.error
                    elif resource_error is not None:
                        primary_error = resource_error
                    elif stop_reason is not None:
                        primary_error = InterruptedError(stop_reason)
                    elif preemption is not None:
                        primary_error = RuntimeError(
                            "resource admission revoked before isolated shard start"
                        )
                    else:
                        primary_error = RuntimeError("pre-ACK admission stage did not settle")
                    lifecycle_error = BaseExceptionGroup(
                        "pre-ACK admission and cleanup failed",
                        [primary_error, stage_cleanup_error],
                    )
            try:
                finished.set()
            except BaseException as exc:
                cleanup_errors.append(exc)
            if heartbeat.ident is not None:
                try:
                    heartbeat.join()
                except BaseException as exc:
                    cleanup_errors.append(exc)
            if child is not None:
                try:
                    self._close_wire_child(
                        child,
                        label="isolated shard",
                        allow_graceful_termination=(stop_reason is None and preemption is None),
                    )
                except BaseException as exc:
                    cleanup_errors.append(exc)

        if lifecycle_error is not None and cleanup_errors:
            raise BaseExceptionGroup(
                "isolated shard lifecycle and cleanup failed",
                [lifecycle_error, *cleanup_errors],
            )
        if lifecycle_error is not None:
            raise lifecycle_error
        fatal_cleanup_errors = [
            error for error in cleanup_errors if not isinstance(error, Exception)
        ]
        if fatal_cleanup_errors:
            raise BaseExceptionGroup("isolated shard cleanup failed", cleanup_errors)
        if cleanup_errors:
            primary_error: Exception | None = (
                None if session_failure is None else session_failure.error
            )
            if primary_error is None:
                primary_error = resource_error
            if primary_error is None and stop_reason is not None:
                primary_error = RuntimeError(stop_reason)
            if primary_error is None and preemption is not None:
                primary_error = RuntimeError(
                    "resource admission revoked during isolated shard execution: "
                    + ",".join(preemption.reason_codes)
                )
            if primary_error is None and heartbeat_errors:
                primary_error = heartbeat_errors[0]
            if (
                primary_error is None
                and outcome is not None
                and outcome.result is None
                and outcome.error_type is not None
            ):
                primary_error = LabIsolatedExecutionError(
                    remote_error_type=outcome.error_type,
                    message=outcome.message or "isolated shard execution failed",
                )
            combined = ([primary_error] if primary_error is not None else []) + cleanup_errors
            combined_error = (
                combined[0]
                if len(combined) == 1
                else ExceptionGroup(
                    "isolated shard execution and cleanup failed",
                    combined,
                )
            )
            if session_failure is not None:
                session_failure = _ClassifiedWireFailure(
                    failure_kind=session_failure.failure_kind,
                    error=combined_error,
                )
            else:
                resource_error = combined_error
            outcome = None
            stop_reason = None
            preemption = None
        return _IsolatedExecutionControl(
            outcome=outcome,
            stop_reason=stop_reason,
            preemption=preemption,
            session_failure=session_failure,
            resource_error=resource_error,
            heartbeat_error=heartbeat_errors[0] if heartbeat_errors else None,
        )

    def _check_deadline(self, spec: ResearchRunSpec) -> None:
        if _utc(self.clock()) >= spec.deadline:
            raise TimeoutError("ResearchRunSpec deadline reached")

    @staticmethod
    def _validate_result_identity(
        claim: LabShardClaim,
        result: LabShardExecutionResult,
    ) -> None:
        expected = (
            claim.shard_id,
            claim.spec_hash,
            claim.payload_hash,
            claim.plan_hash,
            claim.definition.adapter_id,
            claim.definition.adapter_version,
        )
        actual = (
            result.shard_id,
            result.spec_hash,
            result.payload_hash,
            result.plan_hash,
            result.adapter_id,
            result.adapter_version,
        )
        if actual != expected:
            raise ValueError("adapter result identity does not match claim")

    @staticmethod
    def _after_result_staging_created(_temporary: Path) -> None:
        """Fault-injection boundary after an incomplete staging directory exists."""

    @staticmethod
    def _before_result_staging_creation(_temporary: Path) -> None:
        """Fault-injection boundary after private parents exist but before staging creation."""

    @staticmethod
    def _after_result_parquet_temp_fsync(_temporary: Path, _parquet_temp: Path) -> None:
        """Fault-injection boundary before a parquet payload receives its final name."""

    @staticmethod
    def _after_result_manifest_temp_fsync(_temporary: Path, _manifest_temp: Path) -> None:
        """Fault-injection boundary before manifest.json marks a complete prepared bundle."""

    def _ensure_result_directory(self, path: Path, *, worker_code_sha: str) -> None:
        try:
            relative = path.relative_to(self.artifact_root)
        except ValueError as exc:
            raise LabArtifactConflictError("result directory escapes artifact root") from exc
        current = self.artifact_root
        for part in relative.parts:
            current = current / part
            if os.path.lexists(current):
                observed = current.lstat()
                if (
                    current.is_symlink()
                    or not stat.S_ISDIR(observed.st_mode)
                    or observed.st_uid != os.getuid()
                    or stat.S_IMODE(observed.st_mode) != 0o700
                ):
                    raise LabArtifactConflictError(
                        "result directory ancestor is not an owned physical 0700 directory"
                    )
                continue
            self._verify_runtime_guard(expected_sha=worker_code_sha)
            try:
                current.mkdir(mode=0o700)
            except FileExistsError:
                observed = current.lstat()
                if (
                    current.is_symlink()
                    or not stat.S_ISDIR(observed.st_mode)
                    or observed.st_uid != os.getuid()
                    or stat.S_IMODE(observed.st_mode) != 0o700
                ):
                    raise LabArtifactConflictError(
                        "result directory ancestor raced with an unsafe entry"
                    ) from None
            else:
                _fsync_directory(current.parent)

    def _write_bundle(
        self,
        temporary: Path,
        claim: LabShardClaim,
        result: LabShardExecutionResult,
        *,
        worker_code_sha: str,
    ) -> LabShardResultManifest:
        self._ensure_result_directory(temporary.parent, worker_code_sha=worker_code_sha)
        self._before_result_staging_creation(temporary)
        self._verify_runtime_guard(expected_sha=worker_code_sha)
        temporary.mkdir(mode=0o700, exist_ok=False)
        _fsync_directory(temporary.parent)
        self._after_result_staging_created(temporary)
        artifacts: list[LabShardArtifactManifest] = []
        for index, table in enumerate(result.tables):
            file_name = f"{index:03d}-{table.name}.parquet"
            path = temporary / file_name
            parquet_temp = temporary / f".{file_name}.{uuid4().hex}.tmp"
            self._verify_runtime_guard(expected_sha=worker_code_sha)
            table.frame.to_parquet(parquet_temp, index=False)
            _fsync_file(parquet_temp)
            self._after_result_parquet_temp_fsync(temporary, parquet_temp)
            self._verify_runtime_guard(expected_sha=worker_code_sha)
            os.rename(parquet_temp, path)
            _fsync_directory(temporary)
            persisted = pd.read_parquet(path)
            if len(persisted) != len(table.frame) or tuple(persisted.columns) != tuple(
                table.frame.columns
            ):
                raise ValueError(f"artifact round-trip shape changed: {table.name}")
            artifacts.append(
                LabShardArtifactManifest(
                    name=table.name,
                    file_name=file_name,
                    row_count=len(persisted),
                    columns=tuple(persisted.columns),
                    file_size=path.stat().st_size,
                    file_sha256=_file_sha256(path),
                    content_sha256=canonical_shard_frame_digest(persisted),
                )
            )
        validated_spec = self._validate_closed_claim(claim).spec
        execution = validated_spec.strategy_execution
        experiment = validated_spec.experiment
        manifest = LabShardResultManifest(
            worker_code_sha=worker_code_sha,
            content_digest_algorithm=CURRENT_CONTENT_DIGEST_ALGORITHM,
            job_id=claim.job_id,
            shard_id=claim.shard_id,
            claim_token=claim.claim_token,
            claim_generation=claim.claim_generation,
            scheduler_fencing_token=claim.scheduler_fencing_token,
            spec_hash=claim.spec_hash,
            payload_hash=claim.payload_hash,
            plan_hash=claim.plan_hash,
            adapter_id=claim.definition.adapter_id,
            adapter_version=claim.definition.adapter_version,
            experiment_id=None if experiment is None else experiment.experiment_id,
            experiment_attempt_identity=(
                None if experiment is None else experiment.attempt_identity
            ),
            strategy_execution_identity_hash=(
                None if execution is None else execution.identity_hash
            ),
            strategy_spec_fingerprint=(
                None if execution is None else execution.strategy_spec_fingerprint
            ),
            strategy_executable_fingerprint=(
                None if execution is None else execution.strategy_executable_fingerprint
            ),
            candidate_schema_fingerprint=(
                None if execution is None else execution.candidate_schema_fingerprint
            ),
            artifacts=tuple(artifacts),
            metrics=result.metrics,
        )
        manifest_path = temporary / "manifest.json"
        manifest_temp = temporary / f".manifest.{uuid4().hex}.tmp"
        self._verify_runtime_guard(expected_sha=worker_code_sha)
        with manifest_temp.open("x", encoding="utf-8", newline="") as stream:
            stream.write(manifest.canonical_json())
            stream.flush()
            os.fsync(stream.fileno())
        self._after_result_manifest_temp_fsync(temporary, manifest_temp)
        self._verify_runtime_guard(expected_sha=worker_code_sha)
        os.rename(manifest_temp, manifest_path)
        _fsync_directory(temporary)
        return manifest

    @staticmethod
    def _expected_manifest_identity(
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
    ) -> bool:
        return (
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
        ) == (
            claim.job_id,
            claim.shard_id,
            claim.claim_token,
            claim.claim_generation,
            claim.scheduler_fencing_token,
            claim.spec_hash,
            claim.payload_hash,
            claim.plan_hash,
            claim.definition.adapter_id,
            claim.definition.adapter_version,
        )

    def _validate_bundle(
        self,
        bundle: Path,
        claim: LabShardClaim,
    ) -> LabShardResultManifest:
        try:
            bundle_before = bundle.lstat()
        except OSError as exc:
            raise LabArtifactConflictError("sealed shard bundle is missing") from exc
        if not stat.S_ISDIR(bundle_before.st_mode) or bundle.is_symlink():
            raise LabArtifactConflictError("sealed shard bundle is not a regular directory")
        manifest_path = bundle / "manifest.json"
        try:
            manifest_before = manifest_path.lstat()
            if not stat.S_ISREG(manifest_before.st_mode):
                raise LabArtifactConflictError("sealed result manifest is not regular")
            if manifest_before.st_nlink != 1:
                raise LabArtifactConflictError("sealed result manifest has an external hard link")
            raw = manifest_path.read_text(encoding="utf-8")
            manifest_after = manifest_path.lstat()
            if (
                manifest_after.st_dev,
                manifest_after.st_ino,
                manifest_after.st_size,
                manifest_after.st_nlink,
            ) != (
                manifest_before.st_dev,
                manifest_before.st_ino,
                manifest_before.st_size,
                1,
            ):
                raise LabArtifactConflictError("sealed result manifest changed while validating")
            manifest = strict_model_validate_canonical_json(LabShardResultManifest, raw)
        except LabArtifactConflictError:
            raise
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid sealed result manifest: {exc}") from exc
        if raw != manifest.canonical_json():
            raise LabArtifactConflictError("sealed result manifest is not canonical JSON")
        if not self._expected_manifest_identity(claim, manifest):
            raise LabArtifactConflictError("sealed result manifest identity conflicts with claim")
        expected_files = {"manifest.json"} | {artifact.file_name for artifact in manifest.artifacts}
        actual_files = {child.name for child in bundle.iterdir()}
        if actual_files != expected_files:
            unexpected = sorted(actual_files - expected_files)
            missing = sorted(expected_files - actual_files)
            raise LabArtifactConflictError(
                f"sealed bundle has unexpected={unexpected} missing={missing} files"
            )
        for artifact in manifest.artifacts:
            path = bundle / artifact.file_name
            if path.parent != bundle or path.is_symlink() or not path.is_file():
                raise LabArtifactConflictError(
                    f"sealed artifact path is missing or unsafe: {artifact.file_name}"
                )
            before = path.lstat()
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise LabArtifactConflictError(
                    f"sealed artifact has an external hard link: {artifact.file_name}"
                )
            if before.st_size != artifact.file_size or _file_sha256(path) != artifact.file_sha256:
                raise LabArtifactConflictError(
                    f"sealed artifact bytes conflict: {artifact.file_name}"
                )
            frame = pd.read_parquet(path)
            if len(frame) != artifact.row_count or tuple(frame.columns) != artifact.columns:
                raise LabArtifactConflictError(
                    f"sealed artifact shape conflicts: {artifact.file_name}"
                )
            content_hash = canonical_shard_frame_digest(frame)
            if content_hash != artifact.content_sha256:
                raise LabArtifactConflictError(
                    f"sealed artifact content conflicts: {artifact.file_name}"
                )
            after = path.lstat()
            if (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_nlink,
            ) != (before.st_dev, before.st_ino, before.st_size, 1):
                raise LabArtifactConflictError(
                    f"sealed artifact changed while validating: {artifact.file_name}"
                )
        bundle_after = bundle.lstat()
        if (bundle_after.st_dev, bundle_after.st_ino) != (
            bundle_before.st_dev,
            bundle_before.st_ino,
        ):
            raise LabArtifactConflictError("sealed shard bundle changed while validating")
        return manifest

    def _cleanup_temporary(self, temporary: Path) -> None:
        if not os.path.lexists(temporary):
            return
        self.artifact_reclaimer.logical_quarantine_tree(
            temporary,
            purpose="worker candidate temporary cleanup",
        )

    @staticmethod
    def _prepared_file_identities(
        bundle: Path,
        manifest: LabShardResultManifest,
    ) -> tuple[LabPreparedFileIdentity, ...]:
        names = sorted(
            ("manifest.json",) + tuple(artifact.file_name for artifact in manifest.artifacts)
        )
        identities: list[LabPreparedFileIdentity] = []
        for name in names:
            path = bundle / name
            observed = path.lstat()
            if not stat.S_ISREG(observed.st_mode) or path.is_symlink():
                raise LabArtifactConflictError(f"prepared bundle file is unsafe: {name}")
            if observed.st_nlink != 1:
                raise LabArtifactConflictError(
                    f"prepared bundle file has an external hard link: {name}"
                )
            identities.append(
                LabPreparedFileIdentity(
                    file_name=name,
                    device=observed.st_dev,
                    inode=observed.st_ino,
                    size=observed.st_size,
                )
            )
        actual = {child.name for child in bundle.iterdir()}
        if actual != set(names):
            raise LabArtifactConflictError("prepared bundle file inventory changed")
        return tuple(identities)

    @staticmethod
    def _bundle_file_identity(path: Path) -> tuple[int, int]:
        file_stat = os.lstat(path)
        if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISDIR(file_stat.st_mode):
            raise LabArtifactConflictError("sealed shard bundle is not a regular directory")
        return file_stat.st_dev, file_stat.st_ino

    def _prepare_result(
        self,
        claim: LabShardClaim,
        result: LabShardExecutionResult,
        *,
        worker_code_sha: str | None = None,
    ) -> LabPreparedShardBundle:
        self._verify_runtime_guard(expected_sha=worker_code_sha)
        self._validate_result_identity(claim, result)
        resolved_code_sha = worker_code_sha or self._verified_runtime_code_sha(
            self._validate_closed_claim(claim).spec
        )
        sealed = self.sealed_bundle_path(claim)
        temporary_root = self._temporary_bundle_path(claim)
        self._assert_safe_artifact_ancestors(temporary_root)
        self._assert_safe_artifact_ancestors(sealed.parent)
        self._ensure_result_directory(sealed.parent, worker_code_sha=resolved_code_sha)
        temporary = temporary_root / uuid4().hex
        try:
            self._write_bundle(
                temporary,
                claim,
                result,
                worker_code_sha=resolved_code_sha,
            )
            candidate = self._validate_bundle(temporary, claim)
            candidate_files = self._prepared_file_identities(temporary, candidate)
            if sealed.exists() or sealed.is_symlink():
                existing = self._validate_bundle(sealed, claim)
                if existing.manifest_hash != candidate.manifest_hash:
                    raise LabArtifactConflictError(
                        "same attempt produced a conflicting result manifest"
                    )
                device, inode = self._bundle_file_identity(sealed)
                existing_files = self._prepared_file_identities(sealed, existing)
                self._cleanup_temporary(temporary)
                return LabPreparedShardBundle(
                    temporary=None,
                    manifest=existing,
                    file_identities=existing_files,
                    reuses_existing=True,
                    existing_device=device,
                    existing_inode=inode,
                )
            return LabPreparedShardBundle(
                temporary=temporary,
                manifest=candidate,
                file_identities=candidate_files,
            )
        except BaseException:
            self._cleanup_temporary(temporary)
            raise

    def _discard_prepared(self, prepared: LabPreparedShardBundle | None) -> None:
        if prepared is not None and prepared.temporary is not None:
            self._cleanup_temporary(prepared.temporary)

    def _assert_publish_boundary(
        self,
        claim: LabShardClaim,
        *,
        deadline: datetime | None,
        effective_expiry: datetime | None,
        require_current_claim: bool,
    ) -> None:
        self._verify_runtime_guard()
        if self._stop.is_set():
            raise InterruptedError("worker stop requested before success point-of-no-return")
        now = _utc(self.clock())
        if deadline is not None and now >= deadline:
            raise TimeoutError("ResearchRunSpec deadline reached before success publish")
        if effective_expiry is not None and now >= effective_expiry:
            raise PermissionError("accepted heartbeat lease expired before success publish")
        if require_current_claim and not self.claim_spool.is_current(claim):
            raise PermissionError("claim is no longer the durable shard high-water")

    def _rollback_sealed(
        self,
        claim: LabShardClaim,
        bundle: LabSealedShardBundle,
    ) -> None:
        if not bundle.created or not os.path.lexists(bundle.path):
            return
        self._validate_bundle(bundle.path, claim)
        device, inode = self._bundle_file_identity(bundle.path)
        if (device, inode) != (bundle.device, bundle.inode):
            raise LabArtifactConflictError(
                "sealed bundle changed identity before compensating rollback"
            )
        self._verify_runtime_guard(expected_sha=bundle.manifest.worker_code_sha)
        self.artifact_reclaimer.logical_quarantine_tree(
            bundle.path,
            purpose=(
                "sealed rollback "
                f"job={claim.job_id} shard={claim.shard_id} "
                f"generation={claim.claim_generation} token={claim.claim_token}"
            ),
        )

    def _publish_candidate(
        self,
        claim: LabShardClaim,
        prepared: LabPreparedShardBundle,
        *,
        deadline: datetime | None,
        effective_expiry: datetime | None,
        validate_concurrent_race: bool,
    ) -> LabSealedShardBundle:
        sealed = self.sealed_bundle_path(claim)
        self._assert_publish_boundary(
            claim,
            deadline=deadline,
            effective_expiry=effective_expiry,
            require_current_claim=effective_expiry is not None,
        )
        if prepared.reuses_existing:
            if (
                self._prepared_file_identities(sealed, prepared.manifest)
                != prepared.file_identities
            ):
                raise LabArtifactConflictError(
                    "sealed bundle files changed after candidate validation"
                )
            device, inode = self._bundle_file_identity(sealed)
            if (device, inode) != (prepared.existing_device, prepared.existing_inode):
                raise LabArtifactConflictError("sealed bundle changed after candidate validation")
            self._assert_publish_boundary(
                claim,
                deadline=deadline,
                effective_expiry=effective_expiry,
                require_current_claim=effective_expiry is not None,
            )
            return LabSealedShardBundle(
                path=sealed,
                manifest=prepared.manifest,
                created=False,
                device=device,
                inode=inode,
            )

        temporary = prepared.temporary
        if temporary is None:  # pragma: no cover - enforced by prepared model
            raise RuntimeError("new prepared bundle has no temporary path")
        created_bundle: LabSealedShardBundle | None = None
        preserve_temporary = False
        try:
            if (
                self._prepared_file_identities(temporary, prepared.manifest)
                != prepared.file_identities
            ):
                raise LabArtifactConflictError(
                    "prepared bundle files changed before atomic publish"
                )
            try:
                self._verify_runtime_guard(expected_sha=prepared.manifest.worker_code_sha)
                os.rename(temporary, sealed)
            except OSError as exc:
                if exc.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                    raise
                if not validate_concurrent_race:
                    raise LabArtifactConflictError(
                        "sealed bundle appeared after final fence confirmation"
                    ) from exc
                existing = self._validate_bundle(sealed, claim)
                if existing.manifest_hash != prepared.manifest.manifest_hash:
                    raise LabArtifactConflictError(
                        "concurrent attempt produced a conflicting result manifest"
                    ) from exc
                device, inode = self._bundle_file_identity(sealed)
                return LabSealedShardBundle(
                    path=sealed,
                    manifest=existing,
                    created=False,
                    device=device,
                    inode=inode,
                )
            if (
                self._prepared_file_identities(sealed, prepared.manifest)
                != prepared.file_identities
            ):
                raise LabArtifactConflictError(
                    "prepared bundle files changed during atomic publish"
                )
            device, inode = self._bundle_file_identity(sealed)
            created_bundle = LabSealedShardBundle(
                path=sealed,
                manifest=prepared.manifest,
                created=True,
                device=device,
                inode=inode,
            )
            _fsync_directory(sealed.parent)
            self._assert_publish_boundary(
                claim,
                deadline=deadline,
                effective_expiry=effective_expiry,
                require_current_claim=effective_expiry is not None,
            )
            return created_bundle
        except LabDaemonConfigurationError:
            preserve_temporary = created_bundle is None
            raise
        except BaseException:
            if created_bundle is not None:
                self._rollback_sealed(claim, created_bundle)
            raise
        finally:
            if not preserve_temporary:
                self._cleanup_temporary(temporary)

    def _seal_result(
        self,
        claim: LabShardClaim,
        result: LabShardExecutionResult,
        *,
        deadline: datetime | None = None,
    ) -> LabShardResultManifest:
        prepared = self._prepare_result(claim, result)
        bundle = self._publish_candidate(
            claim,
            prepared,
            deadline=deadline,
            effective_expiry=None,
            validate_concurrent_race=True,
        )
        return bundle.manifest

    def _reuse_sealed(self, claim: LabShardClaim) -> LabShardResultManifest | None:
        sealed = self.sealed_bundle_path(claim)
        if not sealed.exists() and not sealed.is_symlink():
            return None
        return self._validate_bundle(sealed, claim)

    @staticmethod
    def _pending_tick_result(pending: LabPendingSuccess) -> LabWorkerTickResult:
        return LabWorkerTickResult(
            status=pending.receipt_state,
            claim_token=pending.claim.claim_token,
            manifest_hash=pending.bundle.manifest.manifest_hash,
            report_id=pending.report.report_id,
        )

    def _set_pending_receipt_state(
        self,
        state: Literal["reported", "awaiting_receipt", "unknown"],
    ) -> LabWorkerTickResult:
        pending = self._pending_success
        if pending is None:  # pragma: no cover - internal state invariant
            raise RuntimeError("worker has no pending success report")
        pending = pending.model_copy(update={"receipt_state": state})
        self._pending_success = pending
        return self._pending_tick_result(pending)

    def _await_pending_success(self) -> LabWorkerTickResult:
        pending = self._pending_success
        if pending is None:  # pragma: no cover - guarded by caller
            raise RuntimeError("worker has no pending success report")
        self._verify_runtime_guard(expected_sha=pending.bundle.manifest.worker_code_sha)
        try:
            self.report_spool.publish(pending.report)
        except Exception as exc:
            _safe_structured_log(
                "error",
                "success_report_publish_failed",
                message=str(exc) or type(exc).__name__,
                component="lab_worker",
                worker_id=self.worker_id,
                job_id=str(pending.claim.job_id),
                shard_id=str(pending.claim.shard_id),
                claim_token=str(pending.claim.claim_token),
                report_id=str(pending.report.report_id),
                error_type=type(exc).__name__,
            )
            return self._set_pending_receipt_state("unknown")
        try:
            receipt = self.receipt_waiter(
                pending.report,
                self._receipt_wait_timeout_seconds(),
                self._stop,
            )
            self._validate_receipt_identity(pending.report, receipt)
        except TimeoutError as exc:
            if self._uses_local_receipt_waiter:
                try:
                    receipt = self._retry_local_receipt_wait(pending.report)
                    self._validate_receipt_identity(pending.report, receipt)
                except TimeoutError:
                    pass
                except InterruptedError:
                    return self._set_pending_receipt_state("reported")
                except Exception as retry_error:
                    _safe_structured_log(
                        "error",
                        "success_receipt_transport_failed",
                        message=str(retry_error) or type(retry_error).__name__,
                        component="lab_worker",
                        worker_id=self.worker_id,
                        job_id=str(pending.claim.job_id),
                        shard_id=str(pending.claim.shard_id),
                        claim_token=str(pending.claim.claim_token),
                        report_id=str(pending.report.report_id),
                        error_type=type(retry_error).__name__,
                    )
                    return self._set_pending_receipt_state("unknown")
                else:
                    if receipt.status == "rejected":
                        self._rollback_sealed(pending.claim, pending.bundle)
                        self._pending_success = None
                        self._resource_retry_at.pop(pending.claim.claim_token, None)
                        return LabWorkerTickResult(
                            status="failed",
                            claim_token=pending.claim.claim_token,
                            manifest_hash=pending.bundle.manifest.manifest_hash,
                            report_id=pending.report.report_id,
                        )
                    self._pending_success = None
                    self._resource_retry_at.pop(pending.claim.claim_token, None)
                    return LabWorkerTickResult(
                        status="succeeded",
                        claim_token=pending.claim.claim_token,
                        manifest_hash=pending.bundle.manifest.manifest_hash,
                        report_id=pending.report.report_id,
                    )
            _safe_structured_log(
                "warning",
                "success_receipt_timeout",
                message=str(exc) or type(exc).__name__,
                component="lab_worker",
                worker_id=self.worker_id,
                job_id=str(pending.claim.job_id),
                shard_id=str(pending.claim.shard_id),
                claim_token=str(pending.claim.claim_token),
                report_id=str(pending.report.report_id),
            )
            return self._set_pending_receipt_state("awaiting_receipt")
        except InterruptedError:
            return self._set_pending_receipt_state("reported")
        except Exception as exc:
            _safe_structured_log(
                "error",
                "success_receipt_transport_failed",
                message=str(exc) or type(exc).__name__,
                component="lab_worker",
                worker_id=self.worker_id,
                job_id=str(pending.claim.job_id),
                shard_id=str(pending.claim.shard_id),
                claim_token=str(pending.claim.claim_token),
                report_id=str(pending.report.report_id),
                error_type=type(exc).__name__,
            )
            return self._set_pending_receipt_state("unknown")
        if receipt.status == "rejected":
            self._rollback_sealed(pending.claim, pending.bundle)
            self._pending_success = None
            self._resource_retry_at.pop(pending.claim.claim_token, None)
            return LabWorkerTickResult(
                status="failed",
                claim_token=pending.claim.claim_token,
                manifest_hash=pending.bundle.manifest.manifest_hash,
                report_id=pending.report.report_id,
            )
        self._pending_success = None
        self._resource_retry_at.pop(pending.claim.claim_token, None)
        return LabWorkerTickResult(
            status="succeeded",
            claim_token=pending.claim.claim_token,
            manifest_hash=pending.bundle.manifest.manifest_hash,
            report_id=pending.report.report_id,
        )

    def _failure_result(
        self,
        claim: LabShardClaim,
        *,
        phase: Literal["claim", "session", "execute", "deadline", "fence", "seal"],
        error: Exception,
        failure_kind: _LabWorkerFailureKind | None = None,
    ) -> LabWorkerTickResult:
        message = _bounded_exception_message(error)
        error_type = getattr(error, "remote_error_type", type(error).__name__)
        self._resource_retry_at.pop(claim.claim_token, None)
        _safe_structured_log(
            "warning" if phase in {"deadline", "fence"} else "error",
            "shard_execution_failed",
            message=message,
            component="lab_worker",
            worker_id=self.worker_id,
            phase=phase,
            job_id=str(claim.job_id),
            shard_id=str(claim.shard_id),
            claim_token=str(claim.claim_token),
            claim_generation=claim.claim_generation,
            scheduler_fencing_token=claim.scheduler_fencing_token,
            error_type=error_type,
        )
        failure = LabWorkerFailure(
            phase=phase,
            failure_kind=(
                failure_kind
                if failure_kind is not None
                else (
                    "session_startup"
                    if phase == "session" and isinstance(error, LabWireSessionStartupError)
                    else {
                        "claim": "claim_validation",
                        "session": "session",
                        "execute": "execution",
                        "deadline": "deadline",
                        "fence": "fence",
                        "seal": "seal",
                    }[phase]
                )
            ),
            error_type=error_type,
            message=message,
        )
        self._best_effort_report(
            claim,
            LabShardFailed(failure_json=failure.canonical_json()),
        )
        return LabWorkerTickResult(status="failed", claim_token=claim.claim_token)

    def _stopped_result(self, claim: LabShardClaim, *, reason: str) -> LabWorkerTickResult:
        self._resource_retry_at.pop(claim.claim_token, None)
        self._best_effort_report(claim, LabWorkerStopped(reason=reason))
        return LabWorkerTickResult(status="stopped", claim_token=claim.claim_token)

    def _maybe_reconcile_quarantine(self) -> tuple[LabWorkerHealthWarning, ...]:
        now_microseconds = _monotonic_microseconds()
        if now_microseconds < self._next_quarantine_reconcile_at_microseconds:
            return ()
        self._next_quarantine_reconcile_at_microseconds = (
            now_microseconds + self.quarantine_reconcile_interval_microseconds
        )
        self._verify_runtime_guard()
        try:
            self.artifact_reclaimer.recover_active(max_entries=16)
        except Exception as exc:
            message = " ".join((str(exc) or type(exc).__name__).split())[:400]
            _safe_structured_log(
                "warning",
                "quarantine_reconcile_failed",
                message=message,
                component="lab_worker",
                worker_id=self.worker_id,
                error_type=type(exc).__name__,
            )
            return (
                LabWorkerHealthWarning(
                    category="quarantine_reconcile_failed",
                    error_type=type(exc).__name__,
                    message=message,
                ),
            )
        return ()

    def close(self) -> None:
        """Boundedly retry any retained authority-child cleanup handles."""

        self._reap_managed_authority_children()

    def run_once(self) -> LabWorkerTickResult:
        primary_error: BaseException | None = None
        result: LabWorkerTickResult | None = None
        try:
            self._reap_managed_authority_children()
            self._verify_closed_registries()
            self._verify_runtime_guard()
            prepublication_deadline_microseconds = (
                None
                if self.resource_authority_manifest is None
                else (
                    self.monotonic_microseconds_clock()
                    + self.prepublication_admission_budget_microseconds
                )
            )
            warnings = (
                ()
                if self._pending_success is not None or self._stop.is_set()
                else self._maybe_reconcile_quarantine()
            )
            result = self._run_claim_once(
                tick_deadline_microseconds=prepublication_deadline_microseconds,
            )
            if warnings:
                result = result.model_copy(update={"health_warnings": warnings})
        except BaseException as exc:
            primary_error = exc

        cleanup_errors: list[BaseException] = []
        try:
            if self._active_resource_reservation is not None:
                self._release_resource_reservation()
        except BaseException as exc:
            cleanup_errors.append(exc)
        try:
            self._reap_managed_authority_children()
        except BaseException as exc:
            cleanup_errors.append(exc)

        if primary_error is not None:
            if cleanup_errors:
                raise BaseExceptionGroup(
                    "worker tick and cleanup failed",
                    [primary_error, *cleanup_errors],
                ) from primary_error
            raise primary_error
        if cleanup_errors:
            if len(cleanup_errors) == 1:
                raise cleanup_errors[0]
            raise BaseExceptionGroup("worker tick cleanup failed", cleanup_errors)
        if result is None:  # pragma: no cover - control-flow invariant
            raise LabDaemonConfigurationError("worker tick returned no result")
        return result

    def _run_claim_once(
        self,
        *,
        tick_deadline_microseconds: int | None,
    ) -> LabWorkerTickResult:
        if self._pending_success is not None:
            return self._await_pending_success()
        if self._stop.is_set():
            return LabWorkerTickResult(status="stopped")
        entry = self._next_owned_claim_entry()
        if entry is None:
            return LabWorkerTickResult(status="idle")
        try:
            self._require_v2_publication_before_admission(entry)
        except (LabClaimFinalizerError, SourceOperationContractError):
            return LabWorkerTickResult(status="idle")
        claim = entry.claim
        if self._stop.is_set():
            return LabWorkerTickResult(status="stopped", claim_token=claim.claim_token)

        try:
            validated = self._validate_closed_claim(claim)
        except Exception as exc:
            if self._consume_selected_claim(entry) is None:
                return LabWorkerTickResult(status="idle")
            return self._failure_result(claim, phase="claim", error=exc)

        try:
            runtime_code_sha = self._verified_runtime_code_sha(validated.spec)
        except LabDaemonConfigurationError:
            raise
        except Exception as exc:
            if self._consume_selected_claim(entry) is None:
                return LabWorkerTickResult(status="idle")
            return self._failure_result(claim, phase="session", error=exc)

        try:
            self._check_deadline(validated.spec)
        except Exception as exc:
            if self._consume_selected_claim(entry) is None:
                return LabWorkerTickResult(status="idle")
            return self._failure_result(claim, phase="deadline", error=exc)

        try:
            admission_evaluation = self._reserve_resource_admission(
                claim,
                validated.spec,
                tick_deadline_microseconds=tick_deadline_microseconds,
            )
        except (InterruptedError, TimeoutError) as exc:
            if isinstance(exc, TimeoutError):
                if self._consume_selected_claim(entry) is None:
                    return LabWorkerTickResult(status="idle")
                return self._stopped_result(
                    claim,
                    reason=str(exc) or "worker tick deadline reached",
                )
            if self._consume_selected_claim(entry) is None:
                return LabWorkerTickResult(status="idle")
            return self._stopped_result(
                claim,
                reason=str(exc) or type(exc).__name__,
            )
        decision = None if admission_evaluation is None else admission_evaluation.decision
        if decision is not None and decision.outcome is not AdmissionOutcome.ADMITTED:
            retry_at = decision.retry_at or (
                _utc(self.clock())
                + timedelta(
                    seconds=max(
                        1,
                        _microseconds_to_seconds(self.poll_interval_microseconds),
                    )
                )
            )
            self._resource_retry_at[claim.claim_token] = retry_at
            _safe_structured_log(
                "info",
                "shard_resource_admission_deferred",
                message="research shard remains pending until resource admission recovers",
                component="lab_worker",
                worker_id=self.worker_id,
                job_id=str(claim.job_id),
                shard_id=str(claim.shard_id),
                claim_token=str(claim.claim_token),
                admission_outcome=decision.outcome.value,
                reason_codes=decision.reason_codes,
                retry_at=retry_at.isoformat(),
            )
            return LabWorkerTickResult(
                status="deferred",
                claim_token=claim.claim_token,
                admission_decision=decision,
            )

        consumed = self._consume_selected_claim(entry)
        if consumed is None:
            return LabWorkerTickResult(status="idle")
        claim = consumed

        if self._stop.is_set():
            return self._stopped_result(
                claim,
                reason="worker stop requested before shard execution",
            )

        try:
            self._verify_runtime_guard()
            self._reclaim_obsolete_temporaries(claim)
            self.artifact_reclaimer.reclaim(claim)
        except LabDaemonConfigurationError:
            raise
        except Exception as exc:
            return self._failure_result(claim, phase="claim", error=exc)

        try:
            self._verify_runtime_guard(expected_sha=runtime_code_sha)
            self.claim_spool.admit_execution(claim)
        except (
            LabClaimNotConsumedError,
            LabClaimRevokedError,
            LabClaimSupersededError,
        ):
            return self._stopped_result(
                claim,
                reason="claim revoked or superseded before shard execution",
            )
        except Exception as exc:
            return self._failure_result(claim, phase="claim", error=exc)

        monotonic_started_microseconds = self.monotonic_microseconds_clock()
        result: LabShardExecutionResult | None = None
        heartbeat_errors: list[Exception] = []
        resource_preemptions: list[AdmissionDecision] = []
        session_failures: list[_ClassifiedWireFailure] = []
        resource_errors: list[Exception] = []
        prepared: LabPreparedShardBundle | None = None
        operation_error: Exception | None = None
        operation_phase: Literal["session", "execute", "deadline", "seal"] = "execute"
        stop_reason: str | None = None
        background_finished: threading.Event | None = None
        background_heartbeat: threading.Thread | None = None
        background_resource_monitor: threading.Thread | None = None
        work_plan = claim.definition.work_plan
        default_hard_limit_seconds = (
            5.0 if work_plan is None else max(0.001, work_plan.static_duration_ms / 1_000)
        )
        control = self._execute_shard_isolated(
            claim,
            validated,
            runtime_code_sha=runtime_code_sha,
            hard_limit_seconds=(
                default_hard_limit_seconds
                if admission_evaluation is None
                else admission_evaluation.policy.max_live_shard_duration_ms / 1_000
            ),
            initial_session=(
                TradingSession.CLOSED
                if admission_evaluation is None
                else admission_evaluation.snapshot.session
            ),
            tick_deadline_microseconds=tick_deadline_microseconds,
        )
        stop_reason = control.stop_reason
        if control.preemption is not None:
            resource_preemptions.append(control.preemption)
        if control.session_failure is not None:
            operation_phase = "session"
            operation_error = control.session_failure.error
            session_failures.append(control.session_failure)
        if control.resource_error is not None:
            resource_errors.append(control.resource_error)
        if control.heartbeat_error is not None:
            heartbeat_errors.append(control.heartbeat_error)
        if control.outcome is not None:
            if control.outcome.configuration_error:
                raise LabDaemonConfigurationError(
                    control.outcome.message or "isolated shard configuration failed"
                )
            if control.outcome.result is not None:
                try:
                    result = LabShardExecutionResult.model_validate(control.outcome.result)
                except Exception as exc:
                    operation_phase = "execute"
                    operation_error = exc
            elif control.outcome.phase is not None:
                operation_phase = control.outcome.phase
                operation_error = LabIsolatedExecutionError(
                    remote_error_type=(control.outcome.error_type or "RuntimeError"),
                    message=(control.outcome.message or "isolated shard execution failed"),
                )
        if (
            result is not None
            and stop_reason is None
            and not resource_preemptions
            and not session_failures
            and not resource_errors
            and not heartbeat_errors
            and operation_error is None
        ):
            background_finished = threading.Event()
            background_heartbeat = threading.Thread(
                target=self._heartbeat_loop,
                args=(claim, background_finished, heartbeat_errors),
                name=f"lab-heartbeat-{claim.claim_token}",
                daemon=True,
            )
            background_heartbeat.start()
            background_resource_monitor = threading.Thread(
                target=self._resource_monitor_loop,
                args=(
                    claim,
                    validated.spec,
                    background_finished,
                    resource_preemptions,
                    session_failures,
                    resource_errors,
                ),
                name=f"lab-resource-monitor-{claim.claim_token}",
                daemon=True,
            )
            background_resource_monitor.start()

        if operation_error is None and stop_reason is None:
            if self._stop.is_set():
                stop_reason = "worker stop requested after shard execution"
            else:
                try:
                    self._check_deadline(validated.spec)
                except Exception as exc:
                    operation_phase = "deadline"
                    operation_error = exc

        if background_finished is not None:
            background_finished.set()
        if background_heartbeat is not None:
            background_heartbeat.join()
        if background_resource_monitor is not None:
            background_resource_monitor.join()

        effective_expiry: datetime | None = None
        publish_decision: AdmissionDecision | None = None
        post_publish_admission_stage: _PrestartedAuthorityStage | None = None

        if (
            operation_error is None
            and stop_reason is None
            and not resource_preemptions
            and not session_failures
            and not resource_errors
            and not heartbeat_errors
            and result is not None
        ):
            try:
                self._verify_runtime_guard(expected_sha=runtime_code_sha)
                self._assert_publish_boundary(
                    claim,
                    deadline=validated.spec.deadline,
                    effective_expiry=None,
                    require_current_claim=True,
                )
                try:
                    receipt = self._publish_and_wait(
                        claim,
                        LabShardHeartbeat(
                            lease_extension_seconds=self.lease_extension_seconds,
                        ),
                        stop=self._stop,
                    )
                except TimeoutError:
                    effective_expiry = claim.lease_expires_at
                else:
                    effective_expiry = receipt.accepted_at + timedelta(
                        seconds=self.lease_extension_seconds
                    )
                self._assert_publish_boundary(
                    claim,
                    deadline=validated.spec.deadline,
                    effective_expiry=effective_expiry,
                    require_current_claim=True,
                )
                # Starting the isolated child is not an authority call: the child
                # cannot resolve the request until the post-seal start ACK below.
                # It overlaps only process startup with the required final fresh
                # pre-publication admission, keeping the additional safety phase
                # bounded without making the post-seal evidence stale.
                if self.resource_authority_manifest is not None:
                    post_publish_admission_stage = self._prestart_authority_stage(
                        operation="admission",
                        spec=validated.spec,
                        admission_request=self._resource_admission_request(claim, validated.spec),
                        deadline_microseconds=(
                            self.monotonic_microseconds_clock()
                            + self.post_publish_rollback_safety_budget_microseconds
                        ),
                    )
                publish_evaluation = self._resource_admission_evaluation(
                    claim,
                    validated.spec,
                    tick_deadline_microseconds=tick_deadline_microseconds,
                )
                publish_decision = (
                    None if publish_evaluation is None else publish_evaluation.decision
                )
                if (
                    publish_decision is not None
                    and publish_decision.outcome is not AdmissionOutcome.ADMITTED
                ):
                    self._cancel_prestarted_authority_stage(
                        post_publish_admission_stage,
                        operation="admission",
                    )
                    resource_preemptions.append(publish_decision)
                else:
                    self._assert_publish_boundary(
                        claim,
                        deadline=validated.spec.deadline,
                        effective_expiry=effective_expiry,
                        require_current_claim=True,
                    )
                    prepared = self._prepare_result(
                        claim,
                        result,
                        worker_code_sha=runtime_code_sha,
                    )
                    try:
                        self._check_deadline(validated.spec)
                    except TimeoutError as exc:
                        operation_phase = "seal"
                        operation_error = exc
            except InterruptedError:
                self._cancel_prestarted_authority_stage(
                    post_publish_admission_stage,
                    operation="admission",
                )
                stop_reason = "worker stop requested while confirming final shard fence"
            except TimeoutError as exc:
                self._cancel_prestarted_authority_stage(
                    post_publish_admission_stage,
                    operation="admission",
                )
                resource_errors.append(exc)
            except (LabWireSessionStartupError, LabWireSessionError) as exc:
                self._cancel_prestarted_authority_stage(
                    post_publish_admission_stage,
                    operation="admission",
                )
                session_failures.append(_classify_wire_failure(exc))
            except LabDaemonConfigurationError:
                self._cancel_prestarted_authority_stage(
                    post_publish_admission_stage,
                    operation="admission",
                )
                raise
            except Exception as exc:
                self._cancel_prestarted_authority_stage(
                    post_publish_admission_stage,
                    operation="admission",
                )
                if _extract_wire_session_error(exc) is not None:
                    session_failures.append(_classify_wire_failure(exc))
                else:
                    resource_errors.append(exc)
            if self._stop.is_set():
                stop_reason = "worker stop requested after candidate serialization"

        for index, resource_error in enumerate(resource_errors):
            if _extract_wire_session_error(resource_error) is not None:
                session_failures.append(_classify_wire_failure(resource_errors.pop(index)))
                break
        if session_failures or (operation_error is not None and operation_phase == "session"):
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._discard_prepared(prepared)
            if session_failures:
                session_failure = session_failures[0]
            else:
                session_failure = _ClassifiedWireFailure(
                    failure_kind="session",
                    error=operation_error,
                )
            return self._failure_result(
                claim,
                phase="session",
                error=session_failure.error,
                failure_kind=session_failure.failure_kind,
            )
        if resource_preemptions:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._discard_prepared(prepared)
            reasons = ",".join(resource_preemptions[0].reason_codes)
            return self._stopped_result(
                claim,
                reason=f"resource admission revoked during shard execution: {reasons}",
            )
        if resource_errors:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._discard_prepared(prepared)
            if isinstance(resource_errors[0], LabDaemonConfigurationError):
                raise resource_errors[0]
            if isinstance(resource_errors[0], TimeoutError):
                return self._stopped_result(
                    claim,
                    reason=str(resource_errors[0]) or "worker tick deadline reached",
                )
            return self._failure_result(
                claim,
                phase="fence",
                error=resource_errors[0],
            )

        if stop_reason is not None:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._discard_prepared(prepared)
            return self._stopped_result(claim, reason=stop_reason)
        if operation_error is not None:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._discard_prepared(prepared)
            return self._failure_result(
                claim,
                phase=operation_phase,
                error=operation_error,
            )
        if heartbeat_errors:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._discard_prepared(prepared)
            if isinstance(heartbeat_errors[0], LabDaemonConfigurationError):
                raise heartbeat_errors[0]
            return self._failure_result(
                claim,
                phase="fence",
                error=heartbeat_errors[0],
            )
        if result is None:  # pragma: no cover - execution state invariant
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            return self._failure_result(
                claim,
                phase="execute",
                error=RuntimeError("worker did not receive a shard execution result"),
            )
        if self._stop.is_set():
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._discard_prepared(prepared)
            return self._stopped_result(
                claim,
                reason="worker stop requested after candidate serialization",
            )
        if prepared is None:  # pragma: no cover - operation state invariant
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            return self._failure_result(
                claim,
                phase="seal",
                error=RuntimeError("worker did not prepare a shard result"),
            )

        try:
            self._assert_publish_boundary(
                claim,
                deadline=validated.spec.deadline,
                effective_expiry=effective_expiry,
                require_current_claim=True,
            )
        except InterruptedError:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._discard_prepared(prepared)
            return self._stopped_result(
                claim,
                reason="worker stop requested while confirming final shard fence",
            )
        except LabDaemonConfigurationError:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._discard_prepared(prepared)
            raise
        except Exception as exc:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._discard_prepared(prepared)
            return self._failure_result(claim, phase="fence", error=exc)

        try:
            self._remaining_prepublication_budget_microseconds(
                tick_deadline_microseconds,
                operation="atomic shard publish",
            )
            bundle = self._publish_candidate(
                claim,
                prepared,
                deadline=validated.spec.deadline,
                effective_expiry=effective_expiry,
                validate_concurrent_race=False,
            )
        except InterruptedError:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            return self._stopped_result(
                claim,
                reason="worker stop requested at atomic shard publish boundary",
            )
        except TimeoutError as exc:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            return self._failure_result(claim, phase="deadline", error=exc)
        except LabDaemonConfigurationError:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            raise
        except Exception as exc:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            return self._failure_result(claim, phase="seal", error=exc)

        post_publish_rollback_deadline_microseconds = (
            None
            if self.resource_authority_manifest is None
            else (
                self.monotonic_microseconds_clock()
                + self.post_publish_rollback_safety_budget_microseconds
            )
        )

        try:
            with self._terminal_lock:
                post_publish_evaluation = self._resource_admission_evaluation(
                    claim,
                    validated.spec,
                    tick_deadline_microseconds=post_publish_rollback_deadline_microseconds,
                    prestarted_admission_stage=post_publish_admission_stage,
                )
                post_publish_decision = (
                    None if post_publish_evaluation is None else post_publish_evaluation.decision
                )
                if (
                    post_publish_decision is not None
                    and post_publish_decision.outcome is not AdmissionOutcome.ADMITTED
                ):
                    reasons = ",".join(post_publish_decision.reason_codes)
                    try:
                        self._rollback_sealed(claim, bundle)
                    except Exception as rollback_error:
                        return self._failure_result(
                            claim,
                            phase="fence",
                            error=rollback_error,
                        )
                    return self._stopped_result(
                        claim,
                        reason=(f"resource admission revoked before success report: {reasons}"),
                    )
                self._assert_publish_boundary(
                    claim,
                    deadline=validated.spec.deadline,
                    effective_expiry=effective_expiry,
                    require_current_claim=True,
                )
                work_plan = claim.definition.work_plan
                telemetry = None
                if work_plan is not None:
                    monotonic_finished_microseconds = self.monotonic_microseconds_clock()
                    elapsed_microseconds = (
                        monotonic_finished_microseconds - monotonic_started_microseconds
                    )
                    telemetry = LabShardTelemetry.from_work_plan(
                        work_plan,
                        monotonic_started=0.0,
                        monotonic_finished=_microseconds_to_seconds(elapsed_microseconds),
                    )
                report = self._make_report(
                    claim,
                    LabShardSucceeded.current(
                        result_manifest_hash=bundle.manifest.manifest_hash,
                        worker_code_sha=runtime_code_sha,
                        telemetry=telemetry,
                    ),
                )
                self._pending_success = LabPendingSuccess(
                    claim=claim,
                    report=report,
                    bundle=bundle,
                    receipt_state="reported",
                )
                self._verify_runtime_guard(expected_sha=runtime_code_sha)
                self.report_spool.publish(report)
        except InterruptedError:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            try:
                self._rollback_sealed(claim, bundle)
            except Exception as rollback_error:
                return self._failure_result(claim, phase="fence", error=rollback_error)
            return self._stopped_result(
                claim,
                reason="worker stop requested before success point-of-no-return",
            )
        except TimeoutError as exc:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            try:
                self._rollback_sealed(claim, bundle)
            except Exception as rollback_error:
                return self._failure_result(claim, phase="fence", error=rollback_error)
            return self._failure_result(claim, phase="fence", error=exc)
        except (LabWireSessionStartupError, LabWireSessionError) as exc:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            try:
                self._rollback_sealed(claim, bundle)
            except Exception as rollback_error:
                return self._failure_result(claim, phase="fence", error=rollback_error)
            wire_failure = _classify_wire_failure(exc)
            return self._failure_result(
                claim,
                phase="session",
                error=wire_failure.error,
                failure_kind=wire_failure.failure_kind,
            )
        except LabDaemonConfigurationError:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            self._rollback_sealed(claim, bundle)
            raise
        except Exception as exc:
            self._cancel_prestarted_authority_stage(
                post_publish_admission_stage,
                operation="admission",
            )
            if _extract_wire_session_error(exc) is not None:
                try:
                    self._rollback_sealed(claim, bundle)
                except Exception as rollback_error:
                    return self._failure_result(
                        claim,
                        phase="fence",
                        error=rollback_error,
                    )
                wire_failure = _classify_wire_failure(exc)
                return self._failure_result(
                    claim,
                    phase="session",
                    error=wire_failure.error,
                    failure_kind=wire_failure.failure_kind,
                )
            if self._pending_success is None:
                try:
                    self._rollback_sealed(claim, bundle)
                except Exception as rollback_error:
                    return self._failure_result(
                        claim,
                        phase="fence",
                        error=rollback_error,
                    )
                return self._failure_result(claim, phase="fence", error=exc)
            _safe_structured_log(
                "error",
                "success_report_publish_failed",
                message=str(exc) or type(exc).__name__,
                component="lab_worker",
                worker_id=self.worker_id,
                job_id=str(claim.job_id),
                shard_id=str(claim.shard_id),
                claim_token=str(claim.claim_token),
                report_id=str(self._pending_success.report.report_id),
                error_type=type(exc).__name__,
            )
            return self._set_pending_receipt_state("unknown")
        return self._await_pending_success()

    def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        previous_handler: object | None = None

        def handle_stop(_signum: int, _frame: FrameType | None) -> None:
            self.request_stop()

        if install_signal_handlers and threading.current_thread() is threading.main_thread():
            previous_handler = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, handle_stop)
        try:
            while True:
                result = self.run_once()
                if result.status == "stopped" or (
                    self._stop.is_set()
                    and result.status in {"idle", "reported", "awaiting_receipt", "unknown"}
                ):
                    return
                self._stop.wait(_microseconds_to_seconds(self.poll_interval_microseconds))
        finally:
            try:
                self.close()
            finally:
                if previous_handler is not None:
                    signal.signal(signal.SIGTERM, previous_handler)


class LabArtifactReclaimer:
    """Quarantine superseded bundles; physical deletion belongs to the later lifecycle GC."""

    _TOMBSTONE_NAME = re.compile(
        r"\.reclaim-v1-"
        r"(?P<fence>[0-9]{20})-"
        r"(?P<generation>[0-9]{20})-"
        r"(?P<token>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12})-"
        r"(?P<manifest_hash>[0-9a-f]{64})"
    )
    _LEDGER_TEMP_NAME = re.compile(r"\.reclaim-ledger-tmp-v1-[0-9a-f]{32}\.tmp")

    def __init__(
        self,
        *,
        artifact_root: Path,
        report_spool: LabReportSpool,
        mutation_guard: Callable[[], object] | None = None,
    ) -> None:
        self.artifact_root = Path(artifact_root).resolve()
        self.report_spool = report_spool
        self.mutation_guard = mutation_guard
        self.garbage_root = self.artifact_root / ".garbage-v1"
        garbage_namespace_was_missing = not os.path.lexists(self.garbage_root)
        self.garbage_intent_dir = self.garbage_root / "prepared_intents"
        self.garbage_active_intent_dir = self.garbage_root / "active_intents"
        self.garbage_cold_health_dir = self.garbage_root / "cold_health_pending"
        self.garbage_cold_conflict_dir = self.garbage_root / "cold_health_conflicts"
        self.garbage_cold_intent_dir = self.garbage_root / "archive" / "deferred_intents"
        self.garbage_recovery_queue_root = self.garbage_root / "recovery_queue"
        self.garbage_recovery_queue_pending_dir = self.garbage_recovery_queue_root / "pending"
        self.garbage_recovery_queue_archive_dir = self.garbage_recovery_queue_root / "archive"
        self.garbage_recovery_queue_enqueued_dir = self.garbage_recovery_queue_root / "enqueued"
        self.garbage_recovery_queue_conflict_dir = self.garbage_recovery_queue_root / "conflicts"
        self.garbage_recovery_queue_conflict_markers_dir = (
            self.garbage_recovery_queue_root / "conflict_markers"
        )
        self.garbage_recovery_queue_repair_intents_dir = (
            self.garbage_recovery_queue_root / "repair_intents"
        )
        self.garbage_recovery_queue_repair_results_dir = (
            self.garbage_recovery_queue_root / "repair_results"
        )
        self.garbage_recovery_queue_sequence_path = (
            self.garbage_recovery_queue_root / "sequence-v1.json"
        )
        self.garbage_recovery_queue_cursor_path = (
            self.garbage_recovery_queue_root / "cursor-v1.json"
        )
        self.garbage_queue_migration_root = self.garbage_recovery_queue_root / "migration-v2"
        self.garbage_queue_migration_cycles_dir = self.garbage_queue_migration_root / "cycles-v3"
        self.garbage_queue_migration_active_path = (
            self.garbage_queue_migration_root / "active-cycle-v3.json"
        )
        self.garbage_queue_migration_legacy_complete_path = (
            self.garbage_queue_migration_root / "complete-v2.json"
        )
        self.garbage_queue_migration_complete_path = (
            self.garbage_queue_migration_root / "complete-v3.json"
        )
        self.garbage_queue_migration_complete_archive_dir = (
            self.garbage_queue_migration_root / "complete_archive"
        )
        self.garbage_intent_temp_dir = self.garbage_root / "intent_temporary"
        self.garbage_intent_orphan_dir = self.garbage_root / "intent_orphans"
        self.garbage_orphan_metadata_dir = self.garbage_root / "intent_orphans_metadata"
        self.garbage_owner_dir = self.garbage_root / "owners"
        self.garbage_ledger_dir = self.garbage_root / "ledger"
        self.garbage_staging_dir = self.garbage_root / "staging"
        self.garbage_deferred_dir = self.garbage_root / "deferred_gc"
        self.garbage_legacy_complete_path = self.garbage_root / "legacy-complete-v1.json"
        self.garbage_pending_dir = self.garbage_deferred_dir
        self._guard_mutation()
        for directory in (
            self.garbage_intent_dir,
            self.garbage_active_intent_dir,
            self.garbage_cold_health_dir,
            self.garbage_cold_conflict_dir,
            self.garbage_cold_intent_dir,
            self.garbage_recovery_queue_root,
            self.garbage_recovery_queue_pending_dir,
            self.garbage_recovery_queue_archive_dir,
            self.garbage_recovery_queue_enqueued_dir,
            self.garbage_recovery_queue_conflict_dir,
            self.garbage_recovery_queue_conflict_markers_dir,
            self.garbage_recovery_queue_repair_intents_dir,
            self.garbage_recovery_queue_repair_results_dir,
            self.garbage_queue_migration_root,
            self.garbage_queue_migration_cycles_dir,
            self.garbage_queue_migration_complete_archive_dir,
            self.garbage_intent_temp_dir,
            self.garbage_intent_orphan_dir,
            self.garbage_orphan_metadata_dir,
            self.garbage_owner_dir,
            self.garbage_ledger_dir,
            self.garbage_staging_dir,
            self.garbage_deferred_dir,
        ):
            self._guard_mutation()
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            if directory.is_symlink() or not directory.is_dir():
                raise LabArtifactConflictError("garbage quarantine directory is unsafe")
            if stat.S_IMODE(directory.lstat().st_mode) != 0o700:
                self._guard_mutation()
                directory.chmod(0o700)
        if garbage_namespace_was_missing:
            self._guard_mutation()
            self._write_migration_complete_locked()
            directories = self._migration_directory_identities()
            cycle = self._ensure_queue_migration_cycle_locked((), directories)
            cursor = self._load_queue_migration_cursor(cycle)
            if not self._write_queue_migration_complete_locked(cycle, cursor, directories):
                raise LabArtifactConflictError(
                    "fresh quarantine migration namespace changed during initialization"
                )

    def _guard_mutation(self) -> None:
        if self.mutation_guard is not None:
            self.mutation_guard()

    @staticmethod
    def _attempt_name(claim: LabShardClaim) -> str:
        return LabWorker._attempt_name(claim)

    @staticmethod
    def _parse_attempt_name(name: str) -> tuple[int, int, UUID]:
        return LabWorker._parse_attempt_name(name)

    @staticmethod
    def _expected_manifest_identity(
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
    ) -> bool:
        return LabWorker._expected_manifest_identity(claim, manifest)

    @staticmethod
    def _assert_safe_temporary_tree(path: Path) -> None:
        LabWorker._assert_safe_temporary_tree(path)

    def _assert_safe_artifact_ancestors(self, path: Path) -> None:
        LabWorker._assert_safe_artifact_ancestors(self, path)

    def _validate_bundle(
        self,
        bundle: Path,
        claim: LabShardClaim,
    ) -> LabShardResultManifest:
        return LabWorker._validate_bundle(self, bundle, claim)

    def sealed_bundle_path(self, claim: LabShardClaim) -> Path:
        return (
            self.artifact_root
            / "jobs"
            / str(claim.job_id)
            / "shards"
            / str(claim.shard_id)
            / "attempts"
            / self._attempt_name(claim)
        )

    @staticmethod
    def _report_matches_attempt(
        report: LabWorkerReport,
        claim: LabShardClaim,
    ) -> bool:
        return (
            report.job_id,
            report.shard_id,
            report.claim_token,
            report.claim_generation,
            report.scheduler_fencing_token,
        ) == (
            claim.job_id,
            claim.shard_id,
            claim.claim_token,
            claim.claim_generation,
            claim.scheduler_fencing_token,
        )

    @staticmethod
    def _receipt_matches_attempt(
        receipt: LabReportReceipt,
        claim: LabShardClaim,
    ) -> bool:
        return (
            receipt.job_id,
            receipt.shard_id,
            receipt.claim_token,
            receipt.claim_generation,
            receipt.scheduler_fencing_token,
        ) == (
            claim.job_id,
            claim.shard_id,
            claim.claim_token,
            claim.claim_generation,
            claim.scheduler_fencing_token,
        )

    @classmethod
    def _tombstone_name(
        cls,
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
    ) -> str:
        return (
            f".reclaim-v1-{claim.scheduler_fencing_token:020d}-"
            f"{claim.claim_generation:020d}-{claim.claim_token}-"
            f"{manifest.manifest_hash}"
        )

    @classmethod
    def _parse_tombstone_name(cls, name: str) -> tuple[int, int, UUID, str]:
        match = cls._TOMBSTONE_NAME.fullmatch(name)
        if match is None:
            raise LabArtifactConflictError(f"invalid reclaim tombstone identity: {name}")
        return (
            int(match.group("fence")),
            int(match.group("generation")),
            UUID(match.group("token")),
            match.group("manifest_hash"),
        )

    def _ledger_dir(self, current_claim: LabShardClaim) -> Path:
        return (
            self.artifact_root
            / ".reclaim-ledger"
            / str(current_claim.job_id)
            / str(current_claim.shard_id)
        )

    def _ledger_path(self, current_claim: LabShardClaim, tombstone_name: str) -> Path:
        return self._ledger_dir(current_claim) / f"{tombstone_name}.json"

    def _write_ledger(self, ledger: LabReclaimLedger) -> Path:
        directory = self._ledger_dir(ledger.current_claim)
        self._assert_safe_artifact_ancestors(directory)
        self._guard_mutation()
        directory.mkdir(parents=True, exist_ok=True)
        target = self._ledger_path(ledger.current_claim, ledger.tombstone_name)
        if os.path.lexists(target):
            self._load_ledger(target)
        temporary = directory / f".reclaim-ledger-tmp-v1-{uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as stream:
                stream.write(ledger.canonical_json().encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
            self._guard_mutation()
            os.replace(temporary, target)
            _fsync_directory(directory)
        finally:
            if os.path.lexists(temporary):
                identity = self._regular_file_identity(
                    temporary,
                    label="reclaim ledger temporary file",
                )
                self._safe_remove_regular_child(
                    temporary,
                    expected=identity,
                    label="reclaim ledger temporary file",
                )
        return target

    def _load_ledger(self, path: Path) -> LabReclaimLedger:
        try:
            before = path.lstat()
        except OSError as exc:
            raise LabArtifactConflictError("reclaim ledger is missing or unsafe") from exc
        if not stat.S_ISREG(before.st_mode) or path.is_symlink():
            raise LabArtifactConflictError("reclaim ledger is missing or unsafe")
        if before.st_nlink != 1:
            raise LabArtifactConflictError("reclaim ledger has an external hard link")
        try:
            raw = path.read_text(encoding="utf-8")
            after = path.lstat()
            if (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_nlink,
            ) != (before.st_dev, before.st_ino, before.st_size, 1):
                raise LabArtifactConflictError("reclaim ledger changed while validating")
            ledger = strict_model_validate_canonical_json(LabReclaimLedger, raw)
        except LabArtifactConflictError:
            raise
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid reclaim ledger: {exc}") from exc
        if raw != ledger.canonical_json():
            raise LabArtifactConflictError("reclaim ledger is not canonical JSON")
        if path != self._ledger_path(ledger.current_claim, ledger.tombstone_name):
            raise LabArtifactConflictError("reclaim ledger path conflicts with its identity")
        return ledger

    def _validate_ledger(
        self,
        ledger: LabReclaimLedger,
        *,
        current_claim: LabShardClaim,
        obsolete_claim: LabShardClaim,
        manifest: LabShardResultManifest,
    ) -> None:
        isolation_claim = ledger.current_claim
        same_shard_plan = (
            isolation_claim.job_id,
            isolation_claim.shard_id,
            isolation_claim.spec_hash,
            isolation_claim.definition,
        ) == (
            current_claim.job_id,
            current_claim.shard_id,
            current_claim.spec_hash,
            current_claim.definition,
        )
        monotonic_high_water = (
            obsolete_claim.claim_generation
            < isolation_claim.claim_generation
            <= current_claim.claim_generation
            and obsolete_claim.scheduler_fencing_token
            <= isolation_claim.scheduler_fencing_token
            <= current_claim.scheduler_fencing_token
        )
        exact_if_same_generation = (
            isolation_claim.claim_generation != current_claim.claim_generation
            or isolation_claim == current_claim
        )
        same_obsolete_attempt = (
            ledger.obsolete_claim.job_id,
            ledger.obsolete_claim.shard_id,
            ledger.obsolete_claim.spec_hash,
            ledger.obsolete_claim.definition,
            self._attempt_identity(ledger.obsolete_claim),
        ) == (
            obsolete_claim.job_id,
            obsolete_claim.shard_id,
            obsolete_claim.spec_hash,
            obsolete_claim.definition,
            self._attempt_identity(obsolete_claim),
        )
        if (
            not same_shard_plan
            or not monotonic_high_water
            or not exact_if_same_generation
            or not same_obsolete_attempt
            or ledger.manifest != manifest
            or ledger.source_name != self._attempt_name(obsolete_claim)
            or ledger.tombstone_name != self._tombstone_name(obsolete_claim, manifest)
        ):
            raise LabArtifactConflictError("reclaim ledger identity conflicts with artifact")

    @staticmethod
    def _regular_file_identity(path: Path, *, label: str) -> LabRegularFileIdentity:
        try:
            before = path.lstat()
        except OSError as exc:
            raise LabArtifactConflictError(f"{label} is missing or unsafe") from exc
        if not stat.S_ISREG(before.st_mode) or path.is_symlink():
            raise LabArtifactConflictError(f"{label} is not a regular file")
        if before.st_nlink != 1:
            raise LabArtifactConflictError(f"{label} has an external hard link")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise LabArtifactConflictError(f"{label} changed while opening") from exc
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or (opened.st_dev, opened.st_ino, opened.st_size)
                != (before.st_dev, before.st_ino, before.st_size)
            ):
                raise LabArtifactConflictError(f"{label} changed while opening")
            digest = hashlib.sha256()
            while chunk := os.read(descriptor, 1024 * 1024):
                digest.update(chunk)
            after_open = os.fstat(descriptor)
            try:
                after_path = path.lstat()
            except OSError as exc:
                raise LabArtifactConflictError(f"{label} changed while validating") from exc
            if (
                after_open.st_dev,
                after_open.st_ino,
                after_open.st_size,
                after_open.st_nlink,
                after_path.st_dev,
                after_path.st_ino,
                after_path.st_size,
                after_path.st_nlink,
            ) != (
                before.st_dev,
                before.st_ino,
                before.st_size,
                1,
                before.st_dev,
                before.st_ino,
                before.st_size,
                1,
            ):
                raise LabArtifactConflictError(f"{label} changed while validating")
        finally:
            os.close(descriptor)
        return LabRegularFileIdentity(
            device=before.st_dev,
            inode=before.st_ino,
            size=before.st_size,
            sha256=digest.hexdigest(),
        )

    def _garbage_relative_path(self, path: Path) -> str:
        try:
            relative = path.relative_to(self.artifact_root)
        except ValueError as exc:
            raise LabArtifactConflictError("garbage source escapes artifact root") from exc
        if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
            raise LabArtifactConflictError("garbage source path is unsafe")
        return relative.as_posix()

    @staticmethod
    def _inventory_regular(
        path: Path,
        *,
        relative_path: str,
        label: str,
    ) -> LabGarbageInventoryEntry:
        identity = LabArtifactReclaimer._regular_file_identity(path, label=label)
        return LabGarbageInventoryEntry(
            relative_path=relative_path,
            file_type="regular",
            device=identity.device,
            inode=identity.inode,
            size=identity.size,
            sha256=identity.sha256,
        )

    def _garbage_inventory(self, path: Path) -> tuple[LabGarbageInventoryEntry, ...]:
        root = path.lstat()
        if stat.S_ISREG(root.st_mode):
            if root.st_nlink != 1 or path.is_symlink():
                raise LabArtifactConflictError("garbage regular payload is unsafe")
            return (
                self._inventory_regular(
                    path,
                    relative_path=".",
                    label="garbage regular payload",
                ),
            )
        if not stat.S_ISDIR(root.st_mode) or path.is_symlink():
            raise LabArtifactConflictError("garbage directory payload is unsafe")
        entries: list[LabGarbageInventoryEntry] = [
            LabGarbageInventoryEntry(
                relative_path=".",
                file_type="directory",
                device=root.st_dev,
                inode=root.st_ino,
            )
        ]
        for current, directories, files in os.walk(path, followlinks=False):
            directories.sort()
            files.sort()
            current_path = Path(current)
            for name in directories:
                child = current_path / name
                observed = child.lstat()
                if child.is_symlink() or not stat.S_ISDIR(observed.st_mode):
                    raise LabArtifactConflictError(
                        f"garbage tree contains unsafe directory: {name}"
                    )
                entries.append(
                    LabGarbageInventoryEntry(
                        relative_path=child.relative_to(path).as_posix(),
                        file_type="directory",
                        device=observed.st_dev,
                        inode=observed.st_ino,
                    )
                )
            for name in files:
                child = current_path / name
                entries.append(
                    self._inventory_regular(
                        child,
                        relative_path=child.relative_to(path).as_posix(),
                        label=f"garbage tree file {name}",
                    )
                )
        return tuple(sorted(entries, key=lambda entry: entry.relative_path))

    def _garbage_owner(
        self,
        path: Path,
        *,
        purpose: str,
        inventory: tuple[LabGarbageInventoryEntry, ...] | None = None,
    ) -> LabGarbageOwner:
        observed = inventory or self._garbage_inventory(path)
        return LabGarbageOwner(
            purpose=" ".join(purpose.split()),
            original_relative_path=self._garbage_relative_path(path),
            payload_type=observed[0].file_type,
            inventory=observed,
        )

    @staticmethod
    def _garbage_bundle_name(owner: LabGarbageOwner) -> str:
        return owner.garbage_id.hex

    def _recovery_queue_path(self, sequence: int, *, archived: bool = False) -> Path:
        directory = (
            self.garbage_recovery_queue_archive_dir
            if archived
            else self.garbage_recovery_queue_pending_dir
        )
        return directory / f"{sequence:020d}.json"

    def _recovery_queue_enqueued_path(
        self,
        intent: LabGarbagePreparedIntent,
        phase: Literal["active", "cold_health"],
    ) -> Path:
        return self.garbage_recovery_queue_enqueued_dir / (
            f"{phase}-{intent.owner.garbage_id.hex}.json"
        )

    @staticmethod
    def _read_recovery_metadata_bytes(path: Path, *, label: str) -> bytes:
        parent_descriptor = -1
        descriptor = -1
        try:
            parent_flags = (
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            )
            parent_descriptor = os.open(path.parent, parent_flags)
            parent_before = os.fstat(parent_descriptor)
            before = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
            if (
                not stat.S_ISDIR(parent_before.st_mode)
                or not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
            ):
                raise LabArtifactConflictError(f"{label} is not a private regular file")
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(path.name, flags, dir_fd=parent_descriptor)
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or (opened.st_dev, opened.st_ino, opened.st_size)
                != (before.st_dev, before.st_ino, before.st_size)
            ):
                raise LabArtifactConflictError(f"{label} changed while opening")
            chunks: list[bytes] = []
            while chunk := os.read(descriptor, 1024 * 1024):
                chunks.append(chunk)
            after_open = os.fstat(descriptor)
            after_path = path.lstat()
            parent_after = os.fstat(parent_descriptor)
            parent_after_path = path.parent.lstat()
        except OSError as exc:
            raise LabArtifactConflictError(f"{label} cannot be read") from exc
        finally:
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
            if parent_descriptor >= 0:
                with suppress(OSError):
                    os.close(parent_descriptor)

        def parent_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
            return (
                value.st_dev,
                value.st_ino,
                stat.S_IFMT(value.st_mode),
                value.st_nlink,
                value.st_mtime_ns,
                value.st_ctime_ns,
            )

        def file_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
            return (
                value.st_dev,
                value.st_ino,
                stat.S_IFMT(value.st_mode),
                value.st_size,
                value.st_nlink,
            )

        if (
            parent_identity(parent_after) != parent_identity(parent_before)
            or parent_identity(parent_after_path) != parent_identity(parent_before)
            or file_identity(after_open) != file_identity(opened)
            or file_identity(after_path) != file_identity(opened)
        ):
            raise LabArtifactConflictError(f"{label} changed while reading")
        return b"".join(chunks)

    def _read_recovery_metadata(self, path: Path, *, label: str) -> str:
        payload = self._read_recovery_metadata_bytes(path, label=label)
        try:
            return payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise LabArtifactConflictError(f"{label} is not valid UTF-8") from exc

    def _load_recovery_queue_entry(self, path: Path) -> LabQuarantineQueueEntry:
        match = _GARBAGE_RECOVERY_QUEUE_NAME.fullmatch(path.name)
        if match is None:
            raise LabArtifactConflictError("quarantine queue entry name is invalid")
        raw = self._read_recovery_metadata(path, label="quarantine queue entry")
        try:
            entry = strict_model_validate_canonical_json(LabQuarantineQueueEntry, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid quarantine queue entry: {exc}") from exc
        if entry.sequence != int(match.group("sequence")) or raw != entry.canonical_json():
            raise LabArtifactConflictError("quarantine queue entry identity conflicts")
        return entry

    def _load_recovery_queue_marker(self, path: Path) -> LabQuarantineQueueEntry:
        raw = self._read_recovery_metadata(path, label="quarantine queue marker")
        try:
            entry = strict_model_validate_canonical_json(LabQuarantineQueueEntry, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid quarantine queue marker: {exc}") from exc
        expected_name = f"{entry.phase}-{entry.intent.owner.garbage_id.hex}.json"
        if path.name != expected_name or raw != entry.canonical_json():
            raise LabArtifactConflictError("quarantine queue marker identity conflicts")
        return entry

    def _recovery_queue_conflict_path(self, sequence: int) -> Path:
        return self.garbage_recovery_queue_conflict_dir / f"{sequence:020d}.json"

    def _recovery_queue_repair_intent_path(self, sequence: int) -> Path:
        return self.garbage_recovery_queue_repair_intents_dir / f"{sequence:020d}.json"

    def _recovery_queue_repair_result_path(self, sequence: int) -> Path:
        return self.garbage_recovery_queue_repair_results_dir / f"{sequence:020d}.json"

    def _observe_recovery_queue_delivery(
        self,
        path: Path,
        *,
        location: Literal["pending", "archive"],
    ) -> LabQuarantineQueueConflictObservation:
        if not os.path.lexists(path):
            return LabQuarantineQueueConflictObservation(
                location=location,
                status="missing",
            )
        try:
            observed = path.lstat()
        except OSError as exc:
            raise LabArtifactConflictError(f"quarantine queue {location} evidence changed") from exc
        if stat.S_ISLNK(observed.st_mode):
            status: Literal["regular", "symlink", "directory", "other"] = "symlink"
        elif stat.S_ISREG(observed.st_mode):
            status = "regular"
        elif stat.S_ISDIR(observed.st_mode):
            status = "directory"
        else:
            status = "other"
        payload: bytes | None = None
        if status == "regular" and observed.st_nlink == 1:
            payload = self._read_recovery_metadata_bytes(
                path,
                label=f"quarantine queue {location} evidence",
            )
            after = path.lstat()
            if (
                after.st_dev,
                after.st_ino,
                stat.S_IFMT(after.st_mode),
                after.st_nlink,
                after.st_size,
            ) != (
                observed.st_dev,
                observed.st_ino,
                stat.S_IFMT(observed.st_mode),
                observed.st_nlink,
                observed.st_size,
            ):
                raise LabArtifactConflictError(f"quarantine queue {location} evidence changed")
        return LabQuarantineQueueConflictObservation(
            location=location,
            status=status,
            device=observed.st_dev,
            inode=observed.st_ino,
            mode=stat.S_IFMT(observed.st_mode),
            nlink=observed.st_nlink,
            size=observed.st_size,
            sha256=_sha256_bytes(payload) if payload is not None else None,
            raw_base64=(base64.b64encode(payload).decode("ascii") if payload is not None else None),
        )

    def _load_recovery_queue_conflict(self, path: Path) -> LabQuarantineQueueConflict:
        raw = self._read_recovery_metadata(path, label="quarantine queue conflict")
        try:
            conflict = strict_model_validate_canonical_json(LabQuarantineQueueConflict, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid quarantine queue conflict: {exc}") from exc
        if path != self._recovery_queue_conflict_path(conflict.sequence):
            raise LabArtifactConflictError("quarantine queue conflict path is invalid")
        if raw != conflict.canonical_json():
            raise LabArtifactConflictError("quarantine queue conflict is not canonical")
        return conflict

    def _ensure_recovery_queue_conflict_locked(
        self,
        sequence: int,
        *,
        reason: Literal[
            "missing_pending",
            "corrupt_pending",
            "corrupt_archived",
            "ambiguous_delivery",
        ],
    ) -> LabQuarantineQueueConflict:
        conflict = LabQuarantineQueueConflict(
            sequence=sequence,
            reason=reason,
            pending=self._observe_recovery_queue_delivery(
                self._recovery_queue_path(sequence),
                location="pending",
            ),
            archived=self._observe_recovery_queue_delivery(
                self._recovery_queue_path(sequence, archived=True),
                location="archive",
            ),
        )
        path = self._recovery_queue_conflict_path(sequence)
        if os.path.lexists(path):
            if self._load_recovery_queue_conflict(path) != conflict:
                raise LabArtifactConflictError("quarantine queue conflict evidence changed")
            return conflict
        self._write_derived_canonical_file(path, conflict.canonical_json())
        if self._load_recovery_queue_conflict(path) != conflict:
            raise LabArtifactConflictError("quarantine queue conflict publication changed")
        return conflict

    def _retire_recovery_queue_conflict_locked(
        self,
        sequence: int,
        *,
        reason: Literal[
            "missing_pending",
            "corrupt_pending",
            "corrupt_archived",
            "ambiguous_delivery",
        ],
    ) -> LabQuarantineQueueCursor:
        conflict = self._ensure_recovery_queue_conflict_locked(sequence, reason=reason)
        _safe_structured_log(
            "warning",
            "lab_quarantine_queue_conflict",
            message="durable quarantine queue conflict retired from hot recovery",
            component="lab_worker",
            sequence=sequence,
            reason=conflict.reason,
            conflict_hash=conflict.content_hash,
        )
        cursor = LabQuarantineQueueCursor(last_sequence=sequence)
        self._write_recovery_queue_cursor_locked(cursor)
        return cursor

    def _load_recovery_queue_repair_intent(
        self,
        path: Path,
    ) -> LabQuarantineQueueRepairIntent:
        raw = self._read_recovery_metadata(path, label="quarantine queue repair intent")
        try:
            intent = strict_model_validate_canonical_json(LabQuarantineQueueRepairIntent, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid queue repair intent: {exc}") from exc
        if path != self._recovery_queue_repair_intent_path(intent.sequence):
            raise LabArtifactConflictError("queue repair intent path conflicts")
        if raw != intent.canonical_json():
            raise LabArtifactConflictError("queue repair intent is not canonical")
        return intent

    def _load_recovery_queue_repair_result(
        self,
        path: Path,
    ) -> LabQuarantineQueueRepairResult:
        raw = self._read_recovery_metadata(path, label="quarantine queue repair result")
        try:
            result = strict_model_validate_canonical_json(LabQuarantineQueueRepairResult, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid queue repair result: {exc}") from exc
        if path != self._recovery_queue_repair_result_path(result.sequence):
            raise LabArtifactConflictError("queue repair result path conflicts")
        if raw != result.canonical_json():
            raise LabArtifactConflictError("queue repair result is not canonical")
        return result

    def _load_recovery_queue_sequence_locked(self) -> LabQuarantineQueueSequence:
        if not os.path.lexists(self.garbage_recovery_queue_sequence_path):
            return LabQuarantineQueueSequence()
        raw = self._read_recovery_metadata(
            self.garbage_recovery_queue_sequence_path,
            label="quarantine queue sequence",
        )
        try:
            state = strict_model_validate_canonical_json(LabQuarantineQueueSequence, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid quarantine queue sequence: {exc}") from exc
        if raw != state.canonical_json():
            raise LabArtifactConflictError("quarantine queue sequence is not canonical")
        return state

    def _load_recovery_queue_cursor_locked(self) -> LabQuarantineQueueCursor:
        if not os.path.lexists(self.garbage_recovery_queue_cursor_path):
            return LabQuarantineQueueCursor()
        raw = self._read_recovery_metadata(
            self.garbage_recovery_queue_cursor_path,
            label="quarantine queue cursor",
        )
        try:
            cursor = strict_model_validate_canonical_json(LabQuarantineQueueCursor, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid quarantine queue cursor: {exc}") from exc
        if raw != cursor.canonical_json():
            raise LabArtifactConflictError("quarantine queue cursor is not canonical")
        return cursor

    def _replace_recovery_queue_state(self, target: Path, payload: str) -> None:
        temporary = self.garbage_recovery_queue_root / (f".queue-state-tmp-v1-{uuid4().hex}.tmp")
        try:
            with temporary.open("xb") as stream:
                stream.write(payload.encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
            self._guard_mutation()
            os.replace(temporary, target)
            _fsync_directory(target.parent)
            if target.parent != self.garbage_recovery_queue_root:
                _fsync_directory(self.garbage_recovery_queue_root)
        finally:
            self._guard_mutation()
            temporary.unlink(missing_ok=True)

    def _write_recovery_queue_sequence_locked(
        self,
        state: LabQuarantineQueueSequence,
    ) -> None:
        validated = LabQuarantineQueueSequence.model_validate(state)
        current = self._load_recovery_queue_sequence_locked()
        if validated.last_sequence < current.last_sequence:
            raise LabArtifactConflictError("quarantine queue sequence cannot move backward")
        if validated == current:
            return
        self._replace_recovery_queue_state(
            self.garbage_recovery_queue_sequence_path,
            validated.canonical_json(),
        )
        if self._load_recovery_queue_sequence_locked() != validated:
            raise LabArtifactConflictError("quarantine queue sequence readback mismatch")

    def _write_recovery_queue_cursor_locked(
        self,
        cursor: LabQuarantineQueueCursor,
    ) -> None:
        validated = LabQuarantineQueueCursor.model_validate(cursor)
        current = self._load_recovery_queue_cursor_locked()
        if validated.last_sequence < current.last_sequence:
            raise LabArtifactConflictError("quarantine queue cursor cannot move backward")
        if validated == current:
            return
        self._replace_recovery_queue_state(
            self.garbage_recovery_queue_cursor_path,
            validated.canonical_json(),
        )
        if self._load_recovery_queue_cursor_locked() != validated:
            raise LabArtifactConflictError("quarantine queue cursor readback mismatch")

    def _ensure_recovery_queue_marker(
        self,
        entry: LabQuarantineQueueEntry,
    ) -> None:
        marker = self._recovery_queue_enqueued_path(entry.intent, entry.phase)
        if os.path.lexists(marker):
            if self._load_recovery_queue_marker(marker) != entry:
                raise LabArtifactConflictError("quarantine queue marker conflicts")
            return
        self._write_derived_canonical_file(marker, entry.canonical_json())
        if self._load_recovery_queue_marker(marker) != entry:
            raise LabArtifactConflictError("quarantine queue marker changed")

    def _commit_unsequenced_recovery_entry_locked(
        self,
        state: LabQuarantineQueueSequence,
    ) -> tuple[LabQuarantineQueueSequence, LabQuarantineQueueEntry | None]:
        sequence = state.last_sequence + 1
        pending = self._recovery_queue_path(sequence)
        archived = self._recovery_queue_path(sequence, archived=True)
        pending_exists = os.path.lexists(pending)
        archived_exists = os.path.lexists(archived)
        if pending_exists and archived_exists:
            raise LabArtifactConflictError("unsequenced recovery entry has two deliveries")
        if archived_exists:
            raise LabArtifactConflictError("unsequenced recovery entry is already archived")
        if not pending_exists:
            return state, None
        entry = self._load_recovery_queue_entry(pending)
        committed = LabQuarantineQueueSequence(last_sequence=sequence)
        self._write_recovery_queue_sequence_locked(committed)
        self._ensure_recovery_queue_marker(entry)
        return committed, entry

    def _enqueue_recovery_intent(
        self,
        intent: LabGarbagePreparedIntent,
        *,
        phase: Literal["active", "cold_health"],
    ) -> LabQuarantineQueueEntry:
        marker = self._recovery_queue_enqueued_path(intent, phase)
        if os.path.lexists(marker):
            entry = self._load_recovery_queue_marker(marker)
            if entry.intent != intent or entry.phase != phase:
                raise LabArtifactConflictError("quarantine queue identity conflicts")
            state = self._load_recovery_queue_sequence_locked()
            if state.last_sequence < entry.sequence:
                if state.last_sequence + 1 != entry.sequence:
                    raise LabArtifactConflictError("quarantine queue sequence has a gap")
                self._write_recovery_queue_sequence_locked(
                    LabQuarantineQueueSequence(last_sequence=entry.sequence)
                )
            pending = self._recovery_queue_path(entry.sequence)
            archived = self._recovery_queue_path(entry.sequence, archived=True)
            if os.path.lexists(pending) == os.path.lexists(archived):
                raise LabArtifactConflictError("quarantine queue delivery state conflicts")
            return entry
        state = self._load_recovery_queue_sequence_locked()
        state, unsequenced = self._commit_unsequenced_recovery_entry_locked(state)
        if unsequenced is not None and unsequenced.intent == intent and unsequenced.phase == phase:
            return unsequenced
        sequence = state.last_sequence + 1
        entry = LabQuarantineQueueEntry(
            sequence=sequence,
            phase=phase,
            intent=intent,
        )
        target = self._recovery_queue_path(sequence)
        self._write_derived_canonical_file(target, entry.canonical_json())
        if self._load_recovery_queue_entry(target) != entry:
            raise LabArtifactConflictError("quarantine queue publication changed")
        self._write_recovery_queue_sequence_locked(
            LabQuarantineQueueSequence(last_sequence=sequence)
        )
        self._ensure_recovery_queue_marker(entry)
        return entry

    def repair_recovery_queue_conflict(
        self,
        *,
        sequence: int,
        intent: LabGarbagePreparedIntent,
        phase: Literal["active", "cold_health"],
    ) -> LabQuarantineQueueRepairResult:
        """Requeue one dead-letter only from canonical marker and authority evidence."""
        with self.report_spool.evidence_lock():
            conflict = self._load_recovery_queue_conflict(
                self._recovery_queue_conflict_path(sequence)
            )
            if conflict.reason == "ambiguous_delivery":
                raise LabArtifactConflictError(
                    "ambiguous queue delivery cannot reassign its marker"
                )
            cursor = self._load_recovery_queue_cursor_locked()
            if cursor.last_sequence < sequence:
                raise LabArtifactConflictError("queue conflict has not retired from hot recovery")
            expected_pending = self._observe_recovery_queue_delivery(
                self._recovery_queue_path(sequence),
                location="pending",
            )
            expected_archived = self._observe_recovery_queue_delivery(
                self._recovery_queue_path(sequence, archived=True),
                location="archive",
            )
            if expected_pending != conflict.pending or expected_archived != conflict.archived:
                raise LabArtifactConflictError("queue conflict delivery evidence changed")
            authoritative = self._load_prepared_intent(
                self._prepared_intent_path(intent.owner.garbage_id)
            )
            if authoritative != intent:
                raise LabArtifactConflictError("queue repair authority intent conflicts")
            repair_intent = LabQuarantineQueueRepairIntent(
                sequence=sequence,
                phase=phase,
                intent=intent,
                conflict_hash=conflict.content_hash,
            )
            repair_intent_path = self._recovery_queue_repair_intent_path(sequence)
            if os.path.lexists(repair_intent_path):
                if self._load_recovery_queue_repair_intent(repair_intent_path) != repair_intent:
                    raise LabArtifactConflictError("queue repair intent conflicts")
            else:
                self._write_derived_canonical_file(
                    repair_intent_path,
                    repair_intent.canonical_json(),
                )
            result_path = self._recovery_queue_repair_result_path(sequence)
            if os.path.lexists(result_path):
                result = self._load_recovery_queue_repair_result(result_path)
                if (
                    result.phase != phase
                    or result.intent_hash != intent.intent_hash
                    or result.conflict_hash != conflict.content_hash
                ):
                    raise LabArtifactConflictError("queue repair result conflicts")
                return result

            marker = self._recovery_queue_enqueued_path(intent, phase)
            marker_archive_dir = (
                self.garbage_recovery_queue_conflict_markers_dir / f"{sequence:020d}"
            )
            self._guard_mutation()
            marker_archive_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            if marker_archive_dir.is_symlink() or not marker_archive_dir.is_dir():
                raise LabArtifactConflictError("queue conflict marker archive is unsafe")
            self._guard_mutation()
            marker_archive_dir.chmod(0o700)
            archived_marker = marker_archive_dir / marker.name
            new_entry: LabQuarantineQueueEntry | None = None
            marker_exists = os.path.lexists(marker)
            archived_marker_exists = os.path.lexists(archived_marker)
            if marker_exists and archived_marker_exists:
                current_marker = self._load_recovery_queue_marker(marker)
                retired_marker = self._load_recovery_queue_marker(archived_marker)
                if (
                    current_marker.intent != intent
                    or current_marker.phase != phase
                    or current_marker.sequence <= sequence
                    or retired_marker.intent != intent
                    or retired_marker.phase != phase
                    or retired_marker.sequence != sequence
                ):
                    raise LabArtifactConflictError("queue repair marker states conflict")
                new_entry = current_marker
            elif marker_exists:
                marker_entry = self._load_recovery_queue_marker(marker)
                if marker_entry.intent != intent or marker_entry.phase != phase:
                    raise LabArtifactConflictError("queue repair marker identity conflicts")
                if marker_entry.sequence == sequence:
                    self._guard_mutation()
                    os.rename(marker, archived_marker)
                    _fsync_directory(self.garbage_recovery_queue_enqueued_dir)
                    _fsync_directory(marker_archive_dir)
                    if self._load_recovery_queue_marker(archived_marker) != marker_entry:
                        raise LabArtifactConflictError("queue repair marker archive changed")
                elif marker_entry.sequence > sequence:
                    new_entry = marker_entry
                else:
                    raise LabArtifactConflictError("queue repair marker sequence regressed")
            elif archived_marker_exists:
                marker_entry = self._load_recovery_queue_marker(archived_marker)
                if (
                    marker_entry.sequence != sequence
                    or marker_entry.intent != intent
                    or marker_entry.phase != phase
                ):
                    raise LabArtifactConflictError("archived queue repair marker conflicts")
            else:
                raise LabArtifactConflictError("queue repair has no canonical enqueued marker")
            if new_entry is None:
                new_entry = self._enqueue_recovery_intent(intent, phase=phase)
            result = LabQuarantineQueueRepairResult(
                sequence=sequence,
                new_sequence=new_entry.sequence,
                phase=phase,
                intent_hash=intent.intent_hash,
                conflict_hash=conflict.content_hash,
            )
            self._write_derived_canonical_file(result_path, result.canonical_json())
            if self._load_recovery_queue_repair_result(result_path) != result:
                raise LabArtifactConflictError("queue repair completion changed")
            return result

    def _prepared_intent(
        self,
        owner: LabGarbageOwner,
        *,
        created_at: datetime | None = None,
    ) -> LabGarbagePreparedIntent:
        target = self._prepared_intent_path(owner.garbage_id)
        if os.path.lexists(target):
            existing = self._load_prepared_intent(target)
            if existing.owner != owner:
                raise LabArtifactConflictError("prepared intent conflicts with owner")
            if created_at is not None and existing.created_at != _utc(created_at):
                raise LabArtifactConflictError("prepared intent created_at conflicts")
            return existing
        return LabGarbagePreparedIntent(
            source_relative_path=owner.original_relative_path,
            staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
            owner=owner,
            created_at=_utc(created_at) if created_at is not None else _system_clock(),
        )

    def _prepared_intent_path(self, garbage_id: UUID) -> Path:
        return self.garbage_intent_dir / f"{garbage_id.hex}-prepared-intent-v1.json"

    @staticmethod
    def _read_prepared_intent_file(
        path: Path,
        *,
        allowed_links: frozenset[int] = frozenset({1}),
    ) -> LabGarbagePreparedIntent:
        try:
            before = path.lstat()
        except OSError as exc:
            raise LabArtifactConflictError("prepared intent is missing or unsafe") from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink not in allowed_links
        ):
            raise LabArtifactConflictError("prepared intent is not an owned regular file")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise LabArtifactConflictError("prepared intent changed while opening") from exc
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink not in allowed_links
                or (opened.st_dev, opened.st_ino, opened.st_size)
                != (before.st_dev, before.st_ino, before.st_size)
            ):
                raise LabArtifactConflictError("prepared intent changed while opening")
            chunks: list[bytes] = []
            while chunk := os.read(descriptor, 1024 * 1024):
                chunks.append(chunk)
            after_open = os.fstat(descriptor)
            after_path = path.lstat()
            if (
                after_open.st_dev,
                after_open.st_ino,
                after_open.st_size,
                after_open.st_nlink,
                after_path.st_dev,
                after_path.st_ino,
                after_path.st_size,
                after_path.st_nlink,
            ) != (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_nlink,
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_nlink,
            ):
                raise LabArtifactConflictError("prepared intent changed while validating")
        except OSError as exc:
            raise LabArtifactConflictError("prepared intent changed while validating") from exc
        finally:
            os.close(descriptor)
        try:
            raw = b"".join(chunks).decode("utf-8")
            intent = strict_model_validate_canonical_json(LabGarbagePreparedIntent, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid prepared intent: {exc}") from exc
        if raw != intent.canonical_json():
            raise LabArtifactConflictError("prepared intent is not canonical JSON")
        return intent

    def _load_prepared_intent(self, path: Path) -> LabGarbagePreparedIntent:
        match = _GARBAGE_INTENT_NAME.fullmatch(path.name)
        if match is None:
            raise LabArtifactConflictError("prepared intent name is invalid")
        intent = self._read_prepared_intent_file(path)
        if intent.owner.garbage_id.hex != match.group("garbage_id"):
            raise LabArtifactConflictError("prepared intent path conflicts with owner")
        return intent

    def _isolate_intent_temporary(self, temporary: Path) -> None:
        target = self.garbage_intent_orphan_dir / temporary.name
        if os.path.lexists(target):
            raise LabArtifactConflictError("prepared intent temporary orphan conflicts")
        self._guard_mutation()
        os.rename(temporary, target)
        _fsync_directory(self.garbage_intent_temp_dir)
        _fsync_directory(self.garbage_intent_orphan_dir)

    def _drop_published_intent_temporary(self, temporary: Path, target: Path) -> None:
        temporary_stat = temporary.lstat()
        target_stat = target.lstat()
        if (
            temporary.is_symlink()
            or target.is_symlink()
            or not stat.S_ISREG(temporary_stat.st_mode)
            or not stat.S_ISREG(target_stat.st_mode)
            or (temporary_stat.st_dev, temporary_stat.st_ino)
            != (target_stat.st_dev, target_stat.st_ino)
            or temporary_stat.st_nlink != 2
            or target_stat.st_nlink != 2
        ):
            raise LabArtifactConflictError("published intent temporary identity conflicts")
        self._guard_mutation()
        os.unlink(temporary)
        _fsync_directory(self.garbage_intent_temp_dir)
        if target.lstat().st_nlink != 1:
            raise LabArtifactConflictError("prepared intent retained an unexpected hard link")

    def _write_prepared_intent(self, intent: LabGarbagePreparedIntent) -> Path:
        self._enqueue_recovery_intent(intent, phase="active")
        target = self._prepared_intent_path(intent.owner.garbage_id)
        if os.path.lexists(target):
            existing = self._load_prepared_intent(target)
            if existing != intent:
                raise LabArtifactConflictError("prepared intent conflicts with durable intent")
            self._ensure_intent_recovery_marker(intent)
            return target
        temporary = self.garbage_intent_temp_dir / (
            f".prepared-intent-tmp-v1-{intent.owner.garbage_id.hex}-{uuid4().hex}.tmp"
        )
        linked = False
        try:
            with temporary.open("xb") as stream:
                stream.write(intent.canonical_json().encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
            _fsync_directory(self.garbage_intent_temp_dir)
            try:
                self._guard_mutation()
                os.link(temporary, target, follow_symlinks=False)
                linked = True
                _fsync_directory(self.garbage_intent_dir)
            except FileExistsError as exc:
                existing = self._load_prepared_intent(target)
                if existing != intent:
                    raise LabArtifactConflictError(
                        "prepared intent no-clobber publication found conflicting content"
                    ) from exc
            if linked:
                self._drop_published_intent_temporary(temporary, target)
            else:
                self._isolate_intent_temporary(temporary)
            if self._load_prepared_intent(target) != intent:
                raise LabArtifactConflictError("prepared intent publication changed content")
        except BaseException:
            if os.path.lexists(temporary):
                self._isolate_intent_temporary(temporary)
            raise
        self._ensure_intent_recovery_marker(intent)
        return target

    @staticmethod
    def _intent_marker_path(directory: Path, garbage_id: UUID) -> Path:
        return directory / f"{garbage_id.hex}-prepared-intent-v1.json"

    def _ensure_intent_recovery_marker(
        self,
        intent: LabGarbagePreparedIntent,
    ) -> Literal["active", "cold_health", "cold", "cold_conflict"]:
        marker_directories = {
            "active": self.garbage_active_intent_dir,
            "cold_health": self.garbage_cold_health_dir,
            "cold": self.garbage_cold_intent_dir,
            "cold_conflict": self.garbage_cold_conflict_dir,
        }
        existing = {
            state: self._intent_marker_path(directory, intent.owner.garbage_id)
            for state, directory in marker_directories.items()
            if os.path.lexists(self._intent_marker_path(directory, intent.owner.garbage_id))
        }
        if len(existing) > 1:
            raise LabArtifactConflictError("prepared intent has duplicate recovery markers")
        if existing:
            state, marker = next(iter(existing.items()))
            if self._load_prepared_intent(marker) != intent:
                raise LabArtifactConflictError("prepared intent recovery marker conflicts")
            if state == "active":
                self._enqueue_recovery_intent(intent, phase="active")
                return "active"
            if state == "cold_health":
                self._enqueue_recovery_intent(intent, phase="cold_health")
                return "cold_health"
            if state == "cold":
                return "cold"
            return "cold_conflict"
        deferred = self.garbage_deferred_dir / intent.owner.garbage_id.hex
        state = "cold_health" if os.path.lexists(deferred) else "active"
        self._enqueue_recovery_intent(intent, phase=state)
        target = self._intent_marker_path(
            marker_directories[state],
            intent.owner.garbage_id,
        )
        self._write_derived_canonical_file(target, intent.canonical_json())
        if self._load_prepared_intent(target) != intent:
            raise LabArtifactConflictError("prepared intent recovery marker changed")
        return state

    def _retire_active_intent_marker(self, intent: LabGarbagePreparedIntent) -> None:
        self._enqueue_recovery_intent(intent, phase="cold_health")
        active = self._intent_marker_path(
            self.garbage_active_intent_dir,
            intent.owner.garbage_id,
        )
        health = self._intent_marker_path(
            self.garbage_cold_health_dir,
            intent.owner.garbage_id,
        )
        completed = tuple(
            marker
            for marker in (
                health,
                self._intent_marker_path(
                    self.garbage_cold_intent_dir,
                    intent.owner.garbage_id,
                ),
                self._intent_marker_path(
                    self.garbage_cold_conflict_dir,
                    intent.owner.garbage_id,
                ),
            )
            if os.path.lexists(marker)
        )
        if completed:
            if len(completed) != 1:
                raise LabArtifactConflictError("prepared intent retirement has duplicate markers")
            if os.path.lexists(active):
                raise LabArtifactConflictError("prepared intent retirement has duplicate markers")
            if self._load_prepared_intent(completed[0]) != intent:
                raise LabArtifactConflictError("retired prepared intent marker conflicts")
            return
        if not os.path.lexists(active) or self._load_prepared_intent(active) != intent:
            raise LabArtifactConflictError("active prepared intent marker is missing or conflicts")
        self._guard_mutation()
        os.rename(active, health)
        _fsync_directory(self.garbage_active_intent_dir)
        _fsync_directory(self.garbage_cold_health_dir)
        if os.path.lexists(active) or self._load_prepared_intent(health) != intent:
            raise LabArtifactConflictError("prepared intent marker retirement changed identity")

    def _legacy_empty_staging_orphan_metadata(
        self,
        staging_id: UUID,
        orphan_token: UUID | None,
        expected_identity: tuple[int, int, int, int] | None = None,
    ) -> LabGarbageOrphanMetadata:
        token_suffix = f"-{orphan_token.hex}" if orphan_token is not None else ""
        if expected_identity is not None and (
            expected_identity[2] != stat.S_IFDIR or expected_identity[3] < 1
        ):
            raise LabArtifactConflictError("legacy empty staging expected identity is unsafe")
        return LabGarbageOrphanMetadata(
            staging_id=staging_id,
            orphan_token=orphan_token,
            original_staging_relative_path=f".garbage-v1/staging/{staging_id.hex}",
            orphan_relative_path=(
                f".garbage-v1/intent_orphans/legacy-empty-staging-{staging_id.hex}{token_suffix}"
            ),
            expected_device=(expected_identity[0] if expected_identity is not None else None),
            expected_inode=(expected_identity[1] if expected_identity is not None else None),
            expected_file_type=("directory" if expected_identity is not None else None),
            expected_nlink=(expected_identity[3] if expected_identity is not None else None),
            expected_empty=(True if expected_identity is not None else None),
        )

    @staticmethod
    def _read_garbage_orphan_metadata_file(
        marker: Path,
        *,
        allowed_links: frozenset[int] = frozenset({1}),
    ) -> LabGarbageOrphanMetadata:
        try:
            before = marker.lstat()
        except OSError as exc:
            raise LabArtifactConflictError("garbage orphan metadata is missing") from exc
        if (
            marker.is_symlink()
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink not in allowed_links
        ):
            raise LabArtifactConflictError("garbage orphan metadata is unsafe")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(marker, flags)
        except OSError as exc:
            raise LabArtifactConflictError("garbage orphan metadata changed while opening") from exc
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink not in allowed_links
                or (opened.st_dev, opened.st_ino, opened.st_size)
                != (before.st_dev, before.st_ino, before.st_size)
            ):
                raise LabArtifactConflictError("garbage orphan metadata changed while opening")
            chunks: list[bytes] = []
            while chunk := os.read(descriptor, 1024 * 1024):
                chunks.append(chunk)
            after_open = os.fstat(descriptor)
            after_path = marker.lstat()
            if (
                after_open.st_dev,
                after_open.st_ino,
                after_open.st_size,
                after_open.st_nlink,
                after_path.st_dev,
                after_path.st_ino,
                after_path.st_size,
                after_path.st_nlink,
            ) != (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_nlink,
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_nlink,
            ):
                raise LabArtifactConflictError("garbage orphan metadata changed while validating")
        except OSError as exc:
            raise LabArtifactConflictError(
                "garbage orphan metadata changed while validating"
            ) from exc
        finally:
            os.close(descriptor)
        try:
            raw = b"".join(chunks).decode("utf-8")
            metadata = strict_model_validate_canonical_json(LabGarbageOrphanMetadata, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid garbage orphan metadata: {exc}") from exc
        if raw != metadata.canonical_json():
            raise LabArtifactConflictError("garbage orphan metadata is not canonical")
        return metadata

    def _load_garbage_orphan_metadata(self, marker: Path) -> LabGarbageOrphanMetadata:
        return self._read_garbage_orphan_metadata_file(marker)

    def _external_orphan_metadata_path(self, metadata: LabGarbageOrphanMetadata) -> Path:
        return self.garbage_orphan_metadata_dir / (
            f"{Path(metadata.orphan_relative_path).name}.json"
        )

    @staticmethod
    def _has_external_orphan_identity(metadata: LabGarbageOrphanMetadata) -> bool:
        return all(
            value is not None
            for value in (
                metadata.expected_device,
                metadata.expected_inode,
                metadata.expected_file_type,
                metadata.expected_nlink,
                metadata.expected_empty,
            )
        )

    def _load_external_orphan_metadata(self, marker: Path) -> LabGarbageOrphanMetadata:
        metadata = self._load_garbage_orphan_metadata(marker)
        if not self._has_external_orphan_identity(
            metadata
        ) or marker != self._external_orphan_metadata_path(metadata):
            raise LabArtifactConflictError("external orphan metadata identity conflicts")
        return metadata

    def _assert_external_orphan_identity(
        self,
        orphan: Path,
        metadata: LabGarbageOrphanMetadata,
    ) -> None:
        if not self._has_external_orphan_identity(metadata):
            raise LabArtifactConflictError("external orphan metadata has no expected identity")
        expected = (
            metadata.expected_device,
            metadata.expected_inode,
            stat.S_IFDIR,
            metadata.expected_nlink,
        )
        try:
            entry = orphan.lstat()
        except OSError as exc:
            raise LabArtifactConflictError(
                "legacy empty staging orphan identity conflicts"
            ) from exc
        if (
            orphan.parent != self.garbage_intent_orphan_dir
            or stat.S_ISLNK(entry.st_mode)
            or not stat.S_ISDIR(entry.st_mode)
            or self._directory_identity(entry) != expected
        ):
            raise LabArtifactConflictError("legacy empty staging orphan identity conflicts")
        descriptor: int | None = None
        try:
            descriptor = os.open(
                orphan,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
            )
            opened = os.fstat(descriptor)
            opened_identity = self._directory_identity(opened)
            if (
                stat.S_ISLNK(opened.st_mode)
                or not stat.S_ISDIR(opened.st_mode)
                or opened_identity != expected
            ):
                raise LabArtifactConflictError("legacy empty staging orphan identity conflicts")
            children = os.listdir(descriptor)
            exit_opened = os.fstat(descriptor)
            exit_path = orphan.lstat()
        except OSError as exc:
            raise LabArtifactConflictError(
                "legacy empty staging orphan identity conflicts"
            ) from exc
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError as exc:
                    raise LabArtifactConflictError(
                        "legacy empty staging orphan identity conflicts"
                    ) from exc
        exit_opened_identity = self._directory_identity(exit_opened)
        if (
            children
            or stat.S_ISLNK(exit_opened.st_mode)
            or not stat.S_ISDIR(exit_opened.st_mode)
            or exit_opened_identity != expected
            or exit_opened_identity != opened_identity
            or stat.S_ISLNK(exit_path.st_mode)
            or not stat.S_ISDIR(exit_path.st_mode)
            or self._directory_identity(exit_path) != expected
        ):
            raise LabArtifactConflictError("legacy empty staging orphan identity conflicts")

    def _drop_published_orphan_metadata_temporary(
        self,
        temporary: Path,
        target: Path,
    ) -> None:
        temporary_stat = temporary.lstat()
        target_stat = target.lstat()
        if (
            temporary.is_symlink()
            or target.is_symlink()
            or not stat.S_ISREG(temporary_stat.st_mode)
            or not stat.S_ISREG(target_stat.st_mode)
            or (temporary_stat.st_dev, temporary_stat.st_ino)
            != (target_stat.st_dev, target_stat.st_ino)
            or temporary_stat.st_nlink != 2
            or target_stat.st_nlink != 2
        ):
            raise LabArtifactConflictError("published orphan metadata temporary conflicts")
        self._guard_mutation()
        os.unlink(temporary)
        _fsync_directory(self.garbage_orphan_metadata_dir)
        if target.lstat().st_nlink != 1:
            raise LabArtifactConflictError("orphan metadata retained an unexpected hard link")

    def _isolate_orphan_metadata_temporary(self, temporary: Path) -> None:
        target = self.garbage_intent_orphan_dir / f".derived-json-tmp-v1-{uuid4().hex}.tmp"
        if os.path.lexists(target):
            raise LabArtifactConflictError("orphan metadata temporary isolation conflicts")
        self._guard_mutation()
        os.rename(temporary, target)
        _fsync_directory(self.garbage_orphan_metadata_dir)
        _fsync_directory(self.garbage_intent_orphan_dir)

    def _write_external_orphan_metadata(self, metadata: LabGarbageOrphanMetadata) -> Path:
        if not self._has_external_orphan_identity(metadata):
            raise LabArtifactConflictError("external orphan metadata has no expected identity")
        target = self._external_orphan_metadata_path(metadata)
        if os.path.lexists(target):
            if self._load_external_orphan_metadata(target) != metadata:
                raise LabArtifactConflictError("external orphan metadata conflicts")
            return target
        temporary = self.garbage_orphan_metadata_dir / (
            f".orphan-metadata-tmp-v1-{metadata.metadata_hash}-{uuid4().hex}.tmp"
        )
        with temporary.open("xb") as stream:
            stream.write(metadata.canonical_json().encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        _fsync_directory(self.garbage_orphan_metadata_dir)
        try:
            self._guard_mutation()
            os.link(temporary, target, follow_symlinks=False)
            _fsync_directory(self.garbage_orphan_metadata_dir)
        except FileExistsError as exc:
            if self._load_external_orphan_metadata(target) != metadata:
                raise LabArtifactConflictError(
                    "external orphan metadata no-clobber publication conflicts"
                ) from exc
            self._isolate_orphan_metadata_temporary(temporary)
        else:
            self._drop_published_orphan_metadata_temporary(temporary, target)
        if self._load_external_orphan_metadata(target) != metadata:
            raise LabArtifactConflictError("external orphan metadata changed after publication")
        return target

    def _ensure_legacy_empty_staging_orphan_metadata(
        self,
        orphan: Path,
        staging_id: UUID,
        orphan_token: UUID | None,
        expected_identity: tuple[int, int, int, int],
    ) -> None:
        expected = self._legacy_empty_staging_orphan_metadata(
            staging_id,
            orphan_token,
            expected_identity,
        )
        self._assert_external_orphan_identity(orphan, expected)
        self._write_external_orphan_metadata(expected)
        self._assert_external_orphan_identity(orphan, expected)

    def _reconcile_orphan_metadata_temporaries_locked(self) -> None:
        for temporary in tuple(sorted(self.garbage_orphan_metadata_dir.iterdir())):
            match = _GARBAGE_ORPHAN_METADATA_TEMP_NAME.fullmatch(temporary.name)
            if match is None:
                continue
            observed = temporary.lstat()
            if (
                temporary.is_symlink()
                or not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink not in {1, 2}
            ):
                raise LabArtifactConflictError("orphan metadata temporary is unsafe")
            metadata = self._read_garbage_orphan_metadata_file(
                temporary,
                allowed_links=frozenset({observed.st_nlink}),
            )
            if metadata.metadata_hash != match.group(
                "metadata_hash"
            ) or not self._has_external_orphan_identity(metadata):
                raise LabArtifactConflictError("orphan metadata temporary identity conflicts")
            target = self._external_orphan_metadata_path(metadata)
            if observed.st_nlink == 2:
                if not os.path.lexists(target):
                    raise LabArtifactConflictError("linked orphan metadata temporary has no target")
                target_stat = target.lstat()
                if (target_stat.st_dev, target_stat.st_ino) != (
                    observed.st_dev,
                    observed.st_ino,
                ):
                    raise LabArtifactConflictError(
                        "linked orphan metadata temporary conflicts with target"
                    )
                self._drop_published_orphan_metadata_temporary(temporary, target)
                continue
            if os.path.lexists(target):
                if self._load_external_orphan_metadata(target) != metadata:
                    raise LabArtifactConflictError(
                        "orphan metadata temporary conflicts with durable target"
                    )
                self._isolate_orphan_metadata_temporary(temporary)
                continue
            self._guard_mutation()
            os.link(temporary, target, follow_symlinks=False)
            _fsync_directory(self.garbage_orphan_metadata_dir)
            self._drop_published_orphan_metadata_temporary(temporary, target)

    def _reconcile_intent_temporaries_locked(self) -> None:
        self._reconcile_orphan_metadata_temporaries_locked()
        external_metadata: dict[str, LabGarbageOrphanMetadata] = {}
        for marker in tuple(sorted(self.garbage_orphan_metadata_dir.iterdir())):
            if _GARBAGE_ORPHAN_METADATA_TEMP_NAME.fullmatch(marker.name) is not None:
                raise LabArtifactConflictError("orphan metadata temporary remained unreconciled")
            metadata = self._load_external_orphan_metadata(marker)
            orphan_name = Path(metadata.orphan_relative_path).name
            if orphan_name in external_metadata:
                raise LabArtifactConflictError("duplicate external orphan metadata")
            external_metadata[orphan_name] = metadata
        for orphan in tuple(sorted(self.garbage_intent_orphan_dir.iterdir())):
            observed = orphan.lstat()
            if orphan.is_symlink():
                raise LabArtifactConflictError("prepared intent orphan is unsafe")
            if stat.S_ISDIR(observed.st_mode):
                match = _LEGACY_EMPTY_STAGING_ORPHAN_NAME.fullmatch(orphan.name)
                if match is None:
                    raise LabArtifactConflictError("prepared intent orphan directory is unsafe")
                names = {child.name for child in orphan.iterdir()}
                if names == {"orphan.json"}:
                    legacy = self._load_garbage_orphan_metadata(orphan / "orphan.json")
                    expected_legacy = self._legacy_empty_staging_orphan_metadata(
                        UUID(hex=match.group("staging_id")),
                        (
                            UUID(hex=match.group("orphan_token"))
                            if match.group("orphan_token") is not None
                            else None
                        ),
                    )
                    if legacy != expected_legacy:
                        raise LabArtifactConflictError(
                            "legacy empty staging orphan metadata conflicts"
                        )
                    continue
                metadata = external_metadata.pop(orphan.name, None)
                if metadata is not None:
                    self._assert_external_orphan_identity(orphan, metadata)
                    continue
                if names:
                    raise LabArtifactConflictError(
                        "prepared intent orphan directory has unexpected metadata"
                    )
                raise LabArtifactConflictError(
                    "legacy empty staging orphan has no external metadata"
                )
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink != 1
                or (
                    _GARBAGE_INTENT_TEMP_NAME.fullmatch(orphan.name) is None
                    and _GARBAGE_DERIVED_TEMP_NAME.fullmatch(orphan.name) is None
                )
            ):
                raise LabArtifactConflictError("prepared intent orphan file is unsafe")
        if external_metadata:
            raise LabArtifactConflictError("external orphan metadata has no matching orphan")
        for temporary in tuple(sorted(self.garbage_intent_temp_dir.iterdir())):
            match = _GARBAGE_INTENT_TEMP_NAME.fullmatch(temporary.name)
            if match is None or temporary.is_symlink():
                raise LabArtifactConflictError("unknown prepared intent temporary")
            observed = temporary.lstat()
            if not stat.S_ISREG(observed.st_mode) or observed.st_nlink not in {1, 2}:
                raise LabArtifactConflictError("prepared intent temporary is unsafe")
            target = self.garbage_intent_dir / (
                f"{match.group('garbage_id')}-prepared-intent-v1.json"
            )
            if observed.st_nlink == 2:
                if not os.path.lexists(target):
                    raise LabArtifactConflictError("linked intent temporary has no target")
                intent = self._read_prepared_intent_file(
                    temporary,
                    allowed_links=frozenset({2}),
                )
                target_intent = self._read_prepared_intent_file(
                    target,
                    allowed_links=frozenset({2}),
                )
                if intent != target_intent or intent.owner.garbage_id.hex != match.group(
                    "garbage_id"
                ):
                    raise LabArtifactConflictError("linked intent temporary conflicts with target")
                self._drop_published_intent_temporary(temporary, target)
                continue
            try:
                intent = self._read_prepared_intent_file(temporary)
            except LabArtifactConflictError:
                self._isolate_intent_temporary(temporary)
                continue
            if intent.owner.garbage_id.hex != match.group("garbage_id"):
                raise LabArtifactConflictError("intent temporary name conflicts with content")
            if os.path.lexists(target):
                if self._load_prepared_intent(target) != intent:
                    raise LabArtifactConflictError("intent temporary conflicts with durable target")
                self._isolate_intent_temporary(temporary)
                continue
            self._guard_mutation()
            os.link(temporary, target, follow_symlinks=False)
            _fsync_directory(self.garbage_intent_dir)
            self._drop_published_intent_temporary(temporary, target)

    def _write_derived_canonical_file(self, target: Path, payload: str) -> None:
        if os.path.lexists(target):
            raise LabArtifactConflictError("derived garbage metadata target already exists")
        temporary = self.garbage_intent_orphan_dir / (f".derived-json-tmp-v1-{uuid4().hex}.tmp")
        with temporary.open("xb") as stream:
            stream.write(payload.encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        _fsync_directory(self.garbage_intent_orphan_dir)
        if os.path.lexists(target):
            return
        self._guard_mutation()
        os.rename(temporary, target)
        _fsync_directory(target.parent)
        _fsync_directory(self.garbage_intent_orphan_dir)

    def _garbage_ledger_path(
        self,
        owner: LabGarbageOwner,
        state: Literal["prepared", "quarantined", "deferred_gc"],
    ) -> Path:
        sequence = _GARBAGE_STATE_SEQUENCE[state]
        return self.garbage_ledger_dir / (f"{owner.garbage_id.hex}-{sequence}-{state}.json")

    def _write_garbage_ledger(
        self,
        owner: LabGarbageOwner,
        state: Literal["prepared", "quarantined", "deferred_gc"],
    ) -> Path:
        ledger = LabGarbageLedger(state=state, owner=owner)
        target = self._garbage_ledger_path(owner, state)
        if os.path.lexists(target):
            existing = self._load_garbage_ledger(target)
            if existing != ledger:
                raise LabArtifactConflictError("garbage ledger state conflicts")
            return target
        self._write_derived_canonical_file(target, ledger.canonical_json())
        if self._load_garbage_ledger(target) != ledger:
            raise LabArtifactConflictError("derived garbage ledger changed after publication")
        return target

    def _load_garbage_ledger(self, path: Path) -> LabGarbageLedger:
        identity = self._regular_file_identity(path, label="garbage ledger")
        match = _GARBAGE_LEDGER_NAME.fullmatch(path.name)
        if match is None:
            raise LabArtifactConflictError("garbage ledger name is invalid")
        try:
            raw = path.read_text(encoding="utf-8")
            ledger = strict_model_validate_canonical_json(LabGarbageLedger, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid garbage ledger: {exc}") from exc
        after = self._regular_file_identity(path, label="garbage ledger")
        expected_state = match.group("state")
        if (
            after != identity
            or raw != ledger.canonical_json()
            or ledger.owner.garbage_id.hex != match.group("garbage_id")
            or ledger.state != expected_state
            or _GARBAGE_STATE_SEQUENCE[ledger.state] != int(match.group("sequence"))
        ):
            raise LabArtifactConflictError("garbage ledger identity is not canonical")
        return ledger

    def _garbage_ledgers(self, owner: LabGarbageOwner) -> tuple[Path, ...]:
        paths = tuple(sorted(self.garbage_ledger_dir.glob(f"{owner.garbage_id.hex}-*.json")))
        if not paths:
            raise LabArtifactConflictError("garbage owner has no durable ledger")
        ledgers = tuple(self._load_garbage_ledger(path) for path in paths)
        for ledger in ledgers:
            if ledger.owner != owner:
                raise LabArtifactConflictError("garbage ledger owner conflicts")
        sequences = tuple(_GARBAGE_STATE_SEQUENCE[ledger.state] for ledger in ledgers)
        if sequences != tuple(range(max(sequences) + 1)):
            raise LabArtifactConflictError("garbage ledger state history is incomplete")
        return paths

    def _latest_garbage_ledger(self, owner: LabGarbageOwner) -> LabGarbageLedger:
        paths = self._garbage_ledgers(owner)
        return self._load_garbage_ledger(paths[-1])

    def _write_garbage_owner(self, bundle: Path, owner: LabGarbageOwner) -> None:
        self._ensure_global_garbage_owner(owner)
        self._ensure_bundle_garbage_owner(bundle, owner)
        self._write_garbage_ledger(owner, "prepared")

    def _ensure_global_garbage_owner(self, owner: LabGarbageOwner) -> Path:
        marker = self.garbage_owner_dir / f"{owner.garbage_id.hex}.json"
        if os.path.lexists(marker):
            if self._load_garbage_owner(marker) != owner:
                raise LabArtifactConflictError("global garbage owner marker conflicts")
            return marker
        self._write_derived_canonical_file(marker, owner.canonical_json())
        if self._load_garbage_owner(marker) != owner:
            raise LabArtifactConflictError("global garbage owner changed after publication")
        return marker

    def _ensure_garbage_staging(self, intent: LabGarbagePreparedIntent) -> Path:
        staging = self.artifact_root / intent.staging_relative_path
        expected = self.garbage_staging_dir / intent.owner.garbage_id.hex
        if staging != expected:
            raise LabArtifactConflictError("prepared intent staging path is unsafe")
        if not os.path.lexists(staging):
            self._guard_mutation()
            staging.mkdir(mode=0o700)
            _fsync_directory(self.garbage_staging_dir)
        observed = staging.lstat()
        if staging.is_symlink() or not stat.S_ISDIR(observed.st_mode):
            raise LabArtifactConflictError("garbage staging bundle is unsafe")
        names = {child.name for child in staging.iterdir()}
        if not names.issubset({"owner.json", "payload"}):
            raise LabArtifactConflictError(
                f"garbage staging bundle has unexpected entries: {sorted(names)}"
            )
        return staging

    def _ensure_bundle_garbage_owner(
        self,
        staging: Path,
        owner: LabGarbageOwner,
    ) -> Path:
        marker = staging / "owner.json"
        if os.path.lexists(marker):
            if self._load_garbage_owner(marker) != owner:
                raise LabArtifactConflictError("bundle garbage owner marker conflicts")
            return marker
        payload = staging / "payload"
        if os.path.lexists(payload) and self._garbage_inventory(payload) != owner.inventory:
            raise LabArtifactConflictError("staged payload conflicts with prepared intent")
        self._write_derived_canonical_file(marker, owner.canonical_json())
        if self._load_garbage_owner(marker) != owner:
            raise LabArtifactConflictError("bundle garbage owner changed after publication")
        return marker

    def _load_garbage_owner(self, marker: Path) -> LabGarbageOwner:
        identity = self._regular_file_identity(marker, label="garbage owner marker")
        try:
            raw = marker.read_text(encoding="utf-8")
            owner = strict_model_validate_canonical_json(LabGarbageOwner, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid garbage owner marker: {exc}") from exc
        after = self._regular_file_identity(marker, label="garbage owner marker")
        if after != identity or raw != owner.canonical_json():
            raise LabArtifactConflictError("garbage owner marker changed or is not canonical")
        return owner

    def _load_garbage_owner_ledger(self, garbage_id: UUID) -> LabGarbageOwner:
        marker = self.garbage_owner_dir / f"{garbage_id.hex}.json"
        owner = self._load_garbage_owner(marker)
        if owner.garbage_id != garbage_id:
            raise LabArtifactConflictError("garbage owner ledger identity conflicts")
        return owner

    def _validate_deferred_bundle_metadata(
        self,
        bundle: Path,
        *,
        expected_owner: LabGarbageOwner,
    ) -> None:
        try:
            root = bundle.lstat()
            bundle_id = UUID(hex=bundle.name)
        except (OSError, ValueError) as exc:
            raise LabArtifactConflictError(
                "deferred quarantine bundle metadata is invalid"
            ) from exc
        if bundle.is_symlink() or not stat.S_ISDIR(root.st_mode):
            raise LabArtifactConflictError("deferred quarantine bundle is unsafe")
        try:
            names = {child.name for child in bundle.iterdir()}
        except OSError as exc:
            raise LabArtifactConflictError(
                "deferred quarantine bundle metadata cannot be enumerated"
            ) from exc
        if names != {"owner.json", "payload"}:
            raise LabArtifactConflictError(
                f"deferred quarantine has unexpected top-level entries: {sorted(names)}"
            )
        owner = self._load_garbage_owner(bundle / "owner.json")
        if bundle_id != expected_owner.garbage_id or owner != expected_owner:
            raise LabArtifactConflictError("deferred quarantine owner identity conflicts")
        if self._load_garbage_owner_ledger(bundle_id) != expected_owner:
            raise LabArtifactConflictError("deferred quarantine owner ledger conflicts")
        if self._latest_garbage_ledger(expected_owner).state != "deferred_gc":
            raise LabArtifactConflictError("deferred quarantine has no deferred_gc ledger")
        try:
            after = bundle.lstat()
        except OSError as exc:
            raise LabArtifactConflictError(
                "deferred quarantine bundle changed during metadata validation"
            ) from exc
        if (after.st_dev, after.st_ino, stat.S_IFMT(after.st_mode)) != (
            root.st_dev,
            root.st_ino,
            stat.S_IFMT(root.st_mode),
        ):
            raise LabArtifactConflictError(
                "deferred quarantine bundle changed during metadata validation"
            )

    def _validate_garbage_bundle(self, bundle: Path) -> LabGarbageOwner:
        root = bundle.lstat()
        if bundle.is_symlink() or not stat.S_ISDIR(root.st_mode):
            raise LabArtifactConflictError("garbage bundle is unsafe")
        try:
            bundle_id = UUID(hex=bundle.name)
        except ValueError as exc:
            raise LabArtifactConflictError("garbage bundle name is invalid") from exc
        names = {child.name for child in bundle.iterdir()}
        if names != {"owner.json", "payload"}:
            raise LabArtifactConflictError(
                f"garbage bundle has unexpected entries: {sorted(names)}"
            )
        owner = self._load_garbage_owner(bundle / "owner.json")
        if owner.garbage_id != bundle_id:
            raise LabArtifactConflictError("garbage bundle name conflicts with owner")
        intent = self._load_prepared_intent(self._prepared_intent_path(bundle_id))
        if intent.owner != owner:
            raise LabArtifactConflictError("garbage bundle conflicts with prepared intent")
        if self._load_garbage_owner_ledger(bundle_id) != owner:
            raise LabArtifactConflictError("garbage bundle owner conflicts with ledger")
        observed = self._garbage_inventory(bundle / "payload")
        if observed != owner.inventory:
            raise LabArtifactConflictError("garbage payload conflicts with owner inventory")
        after = bundle.lstat()
        if (after.st_dev, after.st_ino) != (root.st_dev, root.st_ino):
            raise LabArtifactConflictError("garbage bundle changed while validating")
        return owner

    def _promote_garbage_bundle(self, source: Path, target: Path) -> None:
        if os.path.lexists(target):
            raise LabArtifactConflictError("garbage state has duplicate bundle ownership")
        self._guard_mutation()
        os.rename(source, target)
        _fsync_directory(source.parent)
        _fsync_directory(target.parent)
        if os.path.lexists(source):
            raise LabArtifactConflictError("garbage source path was replaced during promotion")

    def _staging_owner(self, staging: Path) -> LabGarbageOwner:
        root = staging.lstat()
        if staging.is_symlink() or not stat.S_ISDIR(root.st_mode):
            raise LabArtifactConflictError("garbage staging bundle is unsafe")
        names = {child.name for child in staging.iterdir()}
        if names not in ({"owner.json"}, {"owner.json", "payload"}):
            raise LabArtifactConflictError(
                f"garbage staging bundle has unexpected entries: {sorted(names)}"
            )
        owner = self._load_garbage_owner(staging / "owner.json")
        if staging.name != owner.garbage_id.hex:
            raise LabArtifactConflictError("garbage staging name conflicts with owner")
        if self._load_garbage_owner_ledger(owner.garbage_id) != owner:
            raise LabArtifactConflictError("garbage staging owner conflicts with ledger")
        self._garbage_ledgers(owner)
        return owner

    def _source_path_for_owner(self, owner: LabGarbageOwner) -> Path:
        source = self.artifact_root / owner.original_relative_path
        if source.parent == source or not source.is_relative_to(self.artifact_root):
            raise LabArtifactConflictError("garbage owner source path escapes artifact root")
        self._assert_safe_artifact_ancestors(source.parent)
        return source

    def _reconcile_staging_bundle(self, staging: Path) -> LabGarbageOwner:
        owner = self._staging_owner(staging)
        intent_path = self._prepared_intent_path(owner.garbage_id)
        if not os.path.lexists(intent_path):
            self._write_prepared_intent(self._prepared_intent(owner))
        intent = self._load_prepared_intent(intent_path)
        if intent.owner != owner:
            raise LabArtifactConflictError("staging owner conflicts with prepared intent")
        self._reconcile_prepared_intent(intent)
        return owner

    @staticmethod
    def _register_legacy_owner(
        owners: dict[UUID, LabGarbageOwner],
        owner: LabGarbageOwner,
    ) -> None:
        existing = owners.get(owner.garbage_id)
        if existing is not None and existing != owner:
            raise LabArtifactConflictError("legacy garbage owner identities conflict")
        owners[owner.garbage_id] = owner

    def _prepared_intents_locked(self) -> dict[UUID, LabGarbagePreparedIntent]:
        intents: dict[UUID, LabGarbagePreparedIntent] = {}
        for path in sorted(self.garbage_intent_dir.iterdir()):
            if path.is_symlink() or not path.is_file():
                raise LabArtifactConflictError("prepared intent namespace is unsafe")
            intent = self._load_prepared_intent(path)
            if intent.owner.garbage_id in intents:
                raise LabArtifactConflictError("duplicate prepared intent identity")
            intents[intent.owner.garbage_id] = intent
        return intents

    @staticmethod
    def _directory_identity(observed: os.stat_result) -> tuple[int, int, int, int]:
        return (
            observed.st_dev,
            observed.st_ino,
            stat.S_IFMT(observed.st_mode),
            observed.st_nlink,
        )

    def _restore_changed_staging_from_orphan(
        self,
        *,
        staging: Path,
        orphan: Path,
        moved: os.stat_result,
    ) -> None:
        if os.path.lexists(staging):
            raise LabArtifactConflictError(
                "legacy empty staging changed during orphan isolation; "
                "original path is occupied and both paths were preserved"
            )
        if orphan.is_symlink() or not stat.S_ISDIR(moved.st_mode):
            raise LabArtifactConflictError(
                "legacy empty staging changed during orphan isolation; "
                "isolated replacement is unsafe and was preserved"
            )
        try:
            self._guard_mutation()
            os.rename(orphan, staging)
        except OSError as exc:
            raise LabArtifactConflictError(
                "legacy empty staging changed during orphan isolation; "
                "replacement could not be restored and both paths were preserved"
            ) from exc
        _fsync_directory(self.garbage_intent_orphan_dir)
        _fsync_directory(self.garbage_staging_dir)
        restored = staging.lstat()
        if (
            staging.is_symlink()
            or not stat.S_ISDIR(restored.st_mode)
            or self._directory_identity(restored) != self._directory_identity(moved)
            or os.path.lexists(orphan)
        ):
            raise LabArtifactConflictError(
                "legacy empty staging changed during orphan isolation; "
                "replacement restore identity conflicts"
            )

    def _orphan_legacy_empty_staging_locked(self, staging: Path) -> None:
        try:
            legacy_id = UUID(hex=staging.name)
        except ValueError as exc:
            raise LabArtifactConflictError("legacy empty staging name is invalid") from exc
        expected = staging.lstat()
        if (
            staging.is_symlink()
            or not stat.S_ISDIR(expected.st_mode)
            or expected.st_nlink < 1
            or any(staging.iterdir())
        ):
            raise LabArtifactConflictError("legacy empty staging is unsafe")
        orphan_token = uuid4()
        orphan = self.garbage_intent_orphan_dir / (
            f"legacy-empty-staging-{legacy_id.hex}-{orphan_token.hex}"
        )
        if os.path.lexists(orphan):
            raise LabArtifactConflictError("legacy empty staging orphan already exists")
        self._guard_mutation()
        os.rename(staging, orphan)
        _fsync_directory(self.garbage_staging_dir)
        _fsync_directory(self.garbage_intent_orphan_dir)
        moved = orphan.lstat()
        moved_is_empty = not any(orphan.iterdir())
        source_is_absent = not os.path.lexists(staging)
        if (
            orphan.is_symlink()
            or not stat.S_ISDIR(moved.st_mode)
            or self._directory_identity(moved) != self._directory_identity(expected)
            or not moved_is_empty
            or not source_is_absent
        ):
            if source_is_absent:
                self._restore_changed_staging_from_orphan(
                    staging=staging,
                    orphan=orphan,
                    moved=moved,
                )
            raise LabArtifactConflictError(
                "legacy empty staging changed during orphan isolation; "
                "successful orphan metadata was not written"
            )
        self._ensure_legacy_empty_staging_orphan_metadata(
            orphan,
            legacy_id,
            orphan_token,
            self._directory_identity(expected),
        )

    def _migrate_legacy_prepared_state_locked(self) -> None:
        intents = self._prepared_intents_locked()
        owners: dict[UUID, LabGarbageOwner] = {}
        empty_staging: list[Path] = []
        for marker in sorted(self.garbage_owner_dir.iterdir()):
            if marker.is_symlink() or not marker.is_file() or marker.suffix != ".json":
                raise LabArtifactConflictError("garbage owner namespace is unsafe")
            try:
                garbage_id = UUID(hex=marker.stem)
            except ValueError as exc:
                raise LabArtifactConflictError("garbage owner ledger name is invalid") from exc
            owner = self._load_garbage_owner_ledger(garbage_id)
            self._register_legacy_owner(owners, owner)
        for ledger_path in sorted(self.garbage_ledger_dir.iterdir()):
            if ledger_path.is_symlink() or not ledger_path.is_file():
                raise LabArtifactConflictError("garbage ledger namespace is unsafe")
            ledger = self._load_garbage_ledger(ledger_path)
            self._register_legacy_owner(owners, ledger.owner)
        for staging in sorted(self.garbage_staging_dir.iterdir()):
            observed = staging.lstat()
            if staging.is_symlink() or not stat.S_ISDIR(observed.st_mode):
                raise LabArtifactConflictError("garbage staging namespace is unsafe")
            try:
                staging_id = UUID(hex=staging.name)
            except ValueError as exc:
                raise LabArtifactConflictError("garbage staging name is invalid") from exc
            names = {child.name for child in staging.iterdir()}
            if not names:
                if staging_id not in intents and staging_id not in owners:
                    empty_staging.append(staging)
                continue
            if not names.issubset({"owner.json", "payload"}):
                raise LabArtifactConflictError("legacy staging contains unknown derived state")
            if "owner.json" in names:
                owner = self._load_garbage_owner(staging / "owner.json")
                if owner.garbage_id != staging_id:
                    raise LabArtifactConflictError("legacy staging owner conflicts with directory")
                self._register_legacy_owner(owners, owner)
            elif staging_id not in owners and staging_id not in intents:
                raise LabArtifactConflictError("legacy staged payload has no provable owner")
        for deferred in sorted(self.garbage_deferred_dir.iterdir()):
            observed = deferred.lstat()
            if deferred.is_symlink() or not stat.S_ISDIR(observed.st_mode):
                raise LabArtifactConflictError("deferred garbage namespace is unsafe")
            names = {child.name for child in deferred.iterdir()}
            if names != {"owner.json", "payload"}:
                raise LabArtifactConflictError("legacy deferred bundle inventory is unsafe")
            owner = self._load_garbage_owner(deferred / "owner.json")
            if deferred.name != owner.garbage_id.hex:
                raise LabArtifactConflictError("legacy deferred owner conflicts with directory")
            if self._garbage_inventory(deferred / "payload") != owner.inventory:
                raise LabArtifactConflictError("legacy deferred payload conflicts with owner")
            self._register_legacy_owner(owners, owner)
        for garbage_id, owner in sorted(owners.items(), key=lambda item: item[0].hex):
            existing = intents.get(garbage_id)
            expected = self._prepared_intent(owner)
            if existing is not None:
                if existing != expected:
                    raise LabArtifactConflictError("legacy owner conflicts with prepared intent")
                continue
            self._write_prepared_intent(expected)
            intents[garbage_id] = expected
        for staging in empty_staging:
            self._orphan_legacy_empty_staging_locked(staging)

    def _reconcile_prepared_intent(self, intent: LabGarbagePreparedIntent) -> None:
        owner = intent.owner
        if self._load_prepared_intent(self._prepared_intent_path(owner.garbage_id)) != intent:
            raise LabArtifactConflictError("prepared intent changed before reconciliation")
        recovery_state = self._ensure_intent_recovery_marker(intent)
        if recovery_state == "cold_conflict":
            raise LabArtifactConflictError("quarantine health check previously failed")
        source = self._source_path_for_owner(owner)
        staging = self.artifact_root / intent.staging_relative_path
        deferred = self.garbage_deferred_dir / owner.garbage_id.hex
        source_exists = os.path.lexists(source)
        staging_exists = os.path.lexists(staging)
        deferred_exists = os.path.lexists(deferred)
        if deferred_exists:
            if source_exists or staging_exists:
                raise LabArtifactConflictError("deferred quarantine conflicts with active source")
            self._ensure_global_garbage_owner(owner)
            self._validate_deferred_bundle_metadata(
                deferred,
                expected_owner=owner,
            )
            for state in ("prepared", "quarantined", "deferred_gc"):
                self._write_garbage_ledger(owner, state)
            if recovery_state == "active":
                self._retire_active_intent_marker(intent)
            return
        staging = self._ensure_garbage_staging(intent)
        self._ensure_global_garbage_owner(owner)
        self._ensure_bundle_garbage_owner(staging, owner)
        self._write_garbage_ledger(owner, "prepared")
        payload = staging / "payload"
        source_exists = os.path.lexists(source)
        payload_exists = os.path.lexists(payload)
        if source_exists and payload_exists:
            raise LabArtifactConflictError("garbage source and staged payload both exist")
        if not source_exists and not payload_exists:
            raise LabArtifactConflictError("prepared intent has neither source nor staged payload")
        if source_exists:
            if self._garbage_inventory(source) != owner.inventory:
                raise LabArtifactConflictError(
                    "garbage source conflicts with owner inventory and prepared intent"
                )
            self._guard_mutation()
            os.rename(source, payload)
            _fsync_directory(source.parent)
            _fsync_directory(staging)
            if os.path.lexists(source):
                raise LabArtifactConflictError("garbage source was replaced during isolation")
        if self._validate_garbage_bundle(staging) != owner:
            raise LabArtifactConflictError("staged garbage conflicts with prepared intent")
        self._write_garbage_ledger(owner, "quarantined")
        self._promote_garbage_bundle(staging, deferred)
        if self._validate_garbage_bundle(deferred) != owner:
            raise LabArtifactConflictError("promoted garbage conflicts with prepared intent")
        self._write_garbage_ledger(owner, "deferred_gc")
        self._retire_active_intent_marker(intent)

    def _collect_garbage_locked(self) -> None:
        self._reconcile_intent_temporaries_locked()
        self._migrate_legacy_prepared_state_locked()
        intents = self._prepared_intents_locked()
        for intent in sorted(intents.values(), key=lambda item: item.owner.garbage_id.hex):
            self._reconcile_prepared_intent(intent)
            deferred = self.garbage_deferred_dir / intent.owner.garbage_id.hex
            if (
                os.path.lexists(deferred)
                and self._validate_garbage_bundle(deferred) != intent.owner
            ):
                raise LabArtifactConflictError(
                    "explicit quarantine inspection found conflicting deferred payload"
                )
        if any(self.garbage_staging_dir.iterdir()):
            raise LabArtifactConflictError("garbage staging remained after intent reconciliation")

    def collect_garbage(self) -> None:
        """Reconcile durable quarantine state without physically deleting retained bytes."""
        with self.report_spool.evidence_lock():
            self._collect_garbage_locked()

    def _load_migration_complete_locked(self) -> None:
        path = self.garbage_legacy_complete_path
        payload: object = None
        try:
            raw = self._read_recovery_metadata(
                path,
                label="legacy quarantine migration marker",
            )
            payload = strict_canonical_json_loads(raw)
            marker = LabQuarantineMigrationComplete.model_validate(payload)
            canonical = marker.canonical_json()
        except Exception:
            try:
                if not isinstance(payload, dict):
                    raise ValueError("legacy marker is not an object")
                expected_keys = {
                    "after_name",
                    "content_hash",
                    "cycle_ceiling",
                    "schema_version",
                }
                if set(payload) != expected_keys or payload["schema_version"] != 1:
                    raise ValueError("legacy marker shape conflicts")
                after_name = payload["after_name"]
                cycle_ceiling = payload["cycle_ceiling"]
                if (after_name is None) != (cycle_ceiling is None):
                    raise ValueError("legacy marker bounds conflict")
                without_hash = {
                    key: value for key, value in payload.items() if key != "content_hash"
                }
                expected_hash = _sha256_bytes(
                    canonical_json_bytes(
                        without_hash,
                    )
                )
                if payload["content_hash"] != expected_hash:
                    raise ValueError("legacy marker hash conflicts")
                canonical = _canonical_json(
                    payload,
                )
            except Exception as exc:
                raise LabArtifactConflictError(
                    f"invalid quarantine migration marker: {exc}"
                ) from exc
        if raw != canonical:
            raise LabArtifactConflictError("legacy quarantine migration marker is not canonical")

    def _write_migration_complete_locked(self) -> None:
        if os.path.lexists(self.garbage_legacy_complete_path):
            self._load_migration_complete_locked()
            return
        marker = LabQuarantineMigrationComplete()
        self._write_derived_canonical_file(
            self.garbage_legacy_complete_path,
            marker.canonical_json(),
        )
        self._load_migration_complete_locked()

    def _load_queue_migration_complete_locked(self) -> LabQuarantineQueueMigrationComplete:
        raw = self._read_recovery_metadata(
            self.garbage_queue_migration_complete_path,
            label="quarantine queue migration marker",
        )
        try:
            marker = strict_model_validate_canonical_json(LabQuarantineQueueMigrationComplete, raw)
        except Exception as exc:
            raise LabArtifactConflictError(
                f"invalid quarantine queue migration marker: {exc}"
            ) from exc
        if raw != marker.canonical_json():
            raise LabArtifactConflictError("quarantine queue migration marker is not canonical")
        cycle = self._load_active_queue_migration_cycle_locked()
        cursor = self._load_queue_migration_cursor(cycle)
        if (
            marker.cycle_id != cycle.cycle_id
            or marker.index_hash != cycle.index_hash
            or marker.final_index != cycle.total_entries
            or marker.final_chain_hash != cycle.index_hash
            or marker.directories != cycle.directories
            or cursor.last_index != cycle.total_entries
            or cursor.last_chain_hash != cycle.index_hash
        ):
            raise LabArtifactConflictError(
                "quarantine queue migration marker conflicts with active cycle"
            )
        return marker

    def _archive_queue_migration_complete_locked(
        self,
        marker: LabQuarantineQueueMigrationComplete,
    ) -> None:
        target = self.garbage_queue_migration_complete_archive_dir / (
            f"{marker.cycle_id.hex}-{marker.content_hash}-{uuid4().hex}.json"
        )
        if os.path.lexists(target):  # pragma: no cover - UUID collision
            raise LabArtifactConflictError("quarantine migration completion archive conflicts")
        try:
            self._guard_mutation()
            os.rename(self.garbage_queue_migration_complete_path, target)
            _fsync_directory(self.garbage_queue_migration_root)
            _fsync_directory(self.garbage_queue_migration_complete_archive_dir)
        except OSError as exc:
            raise LabArtifactConflictError(
                "quarantine migration completion archive failed"
            ) from exc
        raw = self._read_recovery_metadata(
            target,
            label="archived quarantine queue migration marker",
        )
        if raw != marker.canonical_json():
            raise LabArtifactConflictError(
                "archived quarantine queue migration marker changed identity"
            )

    def _write_queue_migration_complete_locked(
        self,
        cycle: LabQuarantineQueueMigrationCycle,
        cursor: LabQuarantineQueueMigrationCursor,
        directories: tuple[LabQuarantineQueueMigrationDirectory, ...],
    ) -> bool:
        if (
            cursor.cycle_id != cycle.cycle_id
            or cursor.last_index != cycle.total_entries
            or cursor.last_chain_hash != cycle.index_hash
            or directories != cycle.directories
        ):
            raise LabArtifactConflictError("quarantine migration cannot complete an unbound cycle")
        marker = LabQuarantineQueueMigrationComplete(
            cycle_id=cycle.cycle_id,
            index_hash=cycle.index_hash,
            final_index=cursor.last_index,
            final_chain_hash=cursor.last_chain_hash,
            directories=directories,
        )
        if self._migration_directory_identities() != directories:
            return False
        if os.path.lexists(self.garbage_queue_migration_complete_path):
            existing = self._load_queue_migration_complete_locked()
            if existing != marker:
                raise LabArtifactConflictError(
                    "quarantine queue migration completion identity conflicts"
                )
            if self._migration_directory_identities() != directories:
                self._archive_queue_migration_complete_locked(existing)
                return False
            return True
        self._write_derived_canonical_file(
            self.garbage_queue_migration_complete_path,
            marker.canonical_json(),
        )
        persisted = self._load_queue_migration_complete_locked()
        if persisted != marker:
            raise LabArtifactConflictError("quarantine queue migration marker changed identity")
        if self._migration_directory_identities() != directories:
            self._archive_queue_migration_complete_locked(persisted)
            return False
        return True

    def _recovery_intent_paths_locked(self, directory: Path) -> tuple[Path, ...]:
        paths: list[Path] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        observed = entry.stat(follow_symlinks=False)
                    except OSError as exc:
                        raise LabArtifactConflictError(
                            f"prepared intent scan failed for {entry.name}"
                        ) from exc
                    if (
                        _GARBAGE_INTENT_NAME.fullmatch(entry.name) is None
                        or not stat.S_ISREG(observed.st_mode)
                        or observed.st_nlink != 1
                    ):
                        raise LabArtifactConflictError(
                            f"prepared intent recovery entry is unsafe: {entry.name}"
                        )
                    paths.append(Path(entry.path))
        except OSError as exc:
            raise LabArtifactConflictError("prepared intent recovery scan failed") from exc
        return tuple(paths)

    def _migration_namespace_directories(
        self,
    ) -> tuple[tuple[Literal["active", "cold_health", "authority"], Path], ...]:
        return (
            ("active", self.garbage_active_intent_dir),
            ("cold_health", self.garbage_cold_health_dir),
            ("authority", self.garbage_intent_dir),
        )

    @staticmethod
    def _migration_directory_identity(
        namespace: Literal["active", "cold_health", "authority"],
        directory: Path,
    ) -> LabQuarantineQueueMigrationDirectory:
        try:
            observed = directory.lstat()
        except OSError as exc:
            raise LabArtifactConflictError(
                f"quarantine migration {namespace} directory is unavailable"
            ) from exc
        if directory.is_symlink() or not stat.S_ISDIR(observed.st_mode):
            raise LabArtifactConflictError(f"quarantine migration {namespace} directory is unsafe")
        return LabQuarantineQueueMigrationDirectory(
            namespace=namespace,
            device=observed.st_dev,
            inode=observed.st_ino,
            mode=stat.S_IFMT(observed.st_mode),
            nlink=observed.st_nlink,
            mtime_ns=observed.st_mtime_ns,
            ctime_ns=observed.st_ctime_ns,
        )

    def _migration_directory_identities(
        self,
    ) -> tuple[LabQuarantineQueueMigrationDirectory, ...]:
        return tuple(
            self._migration_directory_identity(namespace, directory)
            for namespace, directory in self._migration_namespace_directories()
        )

    def _migration_index_path(
        self,
        cycle: LabQuarantineQueueMigrationCycle,
        index: int,
    ) -> Path:
        return (
            self.garbage_queue_migration_cycles_dir
            / cycle.cycle_id.hex
            / "index"
            / f"{index:020d}.json"
        )

    def _migration_cycle_path(self, cycle_id: UUID) -> Path:
        return self.garbage_queue_migration_cycles_dir / cycle_id.hex / "cycle-v3.json"

    def _migration_cursor_path(self, cycle_id: UUID) -> Path:
        return self.garbage_queue_migration_cycles_dir / cycle_id.hex / "cursor-v3.json"

    def _load_queue_migration_cycle(
        self,
        path: Path,
    ) -> LabQuarantineQueueMigrationCycle:
        raw = self._read_recovery_metadata(path, label="quarantine migration cycle")
        try:
            cycle = strict_model_validate_canonical_json(LabQuarantineQueueMigrationCycle, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid quarantine migration cycle: {exc}") from exc
        if raw != cycle.canonical_json():
            raise LabArtifactConflictError("quarantine migration cycle is not canonical")
        return cycle

    def _load_active_queue_migration_cycle_locked(
        self,
    ) -> LabQuarantineQueueMigrationCycle:
        active = self._load_queue_migration_cycle(self.garbage_queue_migration_active_path)
        authoritative = self._load_queue_migration_cycle(
            self._migration_cycle_path(active.cycle_id)
        )
        if active != authoritative:
            raise LabArtifactConflictError("active quarantine migration cycle conflicts")
        return active

    def _load_queue_migration_cursor(
        self,
        cycle: LabQuarantineQueueMigrationCycle,
    ) -> LabQuarantineQueueMigrationCursor:
        raw = self._read_recovery_metadata(
            self._migration_cursor_path(cycle.cycle_id),
            label="quarantine migration cursor",
        )
        try:
            cursor = strict_model_validate_canonical_json(LabQuarantineQueueMigrationCursor, raw)
        except Exception as exc:
            raise LabArtifactConflictError(f"invalid quarantine migration cursor: {exc}") from exc
        if (
            raw != cursor.canonical_json()
            or cursor.cycle_id != cycle.cycle_id
            or cursor.last_index > cycle.total_entries
            or (cursor.last_index == 0 and cursor.last_chain_hash != _QUEUE_MIGRATION_CHAIN_GENESIS)
            or (
                cursor.last_index == cycle.total_entries
                and cursor.last_chain_hash != cycle.index_hash
            )
        ):
            raise LabArtifactConflictError("quarantine migration cursor conflicts with cycle")
        if cursor.last_index:
            entry = self._load_queue_migration_index_entry(cycle, cursor.last_index)
            if entry.chain_hash != cursor.last_chain_hash:
                raise LabArtifactConflictError(
                    "quarantine migration cursor chain conflicts with index"
                )
        return cursor

    def _write_queue_migration_cursor(
        self,
        cycle: LabQuarantineQueueMigrationCycle,
        cursor: LabQuarantineQueueMigrationCursor,
        entry: LabQuarantineQueueMigrationIndexEntry,
    ) -> None:
        validated = LabQuarantineQueueMigrationCursor.model_validate(cursor)
        current = self._load_queue_migration_cursor(cycle)
        if (
            validated.cycle_id != cycle.cycle_id
            or validated.last_index > cycle.total_entries
            or validated.last_index != current.last_index + 1
            or entry.index != validated.last_index
            or entry.previous_chain_hash != current.last_chain_hash
            or entry.chain_hash != validated.last_chain_hash
            or (entry.index == cycle.total_entries and entry.chain_hash != cycle.index_hash)
        ):
            raise LabArtifactConflictError("quarantine migration cursor chain cannot advance")
        path = self._migration_cursor_path(cycle.cycle_id)
        self._replace_recovery_queue_state(path, validated.canonical_json())
        if self._load_queue_migration_cursor(cycle) != validated:
            raise LabArtifactConflictError("quarantine migration cursor readback mismatch")

    def _load_queue_migration_index_entry(
        self,
        cycle: LabQuarantineQueueMigrationCycle,
        index: int,
    ) -> LabQuarantineQueueMigrationIndexEntry:
        path = self._migration_index_path(cycle, index)
        raw = self._read_recovery_metadata(path, label="quarantine migration index entry")
        try:
            entry = strict_model_validate_canonical_json(LabQuarantineQueueMigrationIndexEntry, raw)
        except Exception as exc:
            raise LabArtifactConflictError(
                f"invalid quarantine migration index entry: {exc}"
            ) from exc
        if entry.index != index or raw != entry.canonical_json():
            raise LabArtifactConflictError("quarantine migration index identity conflicts")
        if index == cycle.total_entries and entry.chain_hash != cycle.index_hash:
            raise LabArtifactConflictError("quarantine migration index final chain conflicts")
        return entry

    def _migration_index_entries_locked(
        self,
    ) -> tuple[
        tuple[LabQuarantineQueueMigrationIndexEntry, ...],
        tuple[LabQuarantineQueueMigrationDirectory, ...],
    ]:
        before = self._migration_directory_identities()
        candidates: list[tuple[Literal["active", "cold_health", "authority"], str]] = []
        for namespace, directory in self._migration_namespace_directories():
            for path in self._recovery_intent_paths_locked(directory):
                match = _GARBAGE_INTENT_NAME.fullmatch(path.name)
                if match is None:  # pragma: no cover - scanner validates names
                    raise LabArtifactConflictError("legacy recovery marker name is invalid")
                garbage_id = UUID(hex=match.group("garbage_id"))
                if namespace == "authority":
                    if self._has_recovery_marker_locked(garbage_id):
                        continue
                elif os.path.lexists(
                    self.garbage_recovery_queue_enqueued_dir / f"{namespace}-{garbage_id.hex}.json"
                ):
                    continue
                candidates.append((namespace, path.name))
        after = self._migration_directory_identities()
        if after != before:
            raise LabArtifactConflictError("legacy recovery namespaces changed while indexing")
        ordered = sorted(candidates, key=lambda item: (item[0], item[1]))
        entries: list[LabQuarantineQueueMigrationIndexEntry] = []
        previous_chain_hash = _QUEUE_MIGRATION_CHAIN_GENESIS
        for index, (namespace, file_name) in enumerate(ordered, start=1):
            entry = LabQuarantineQueueMigrationIndexEntry(
                index=index,
                namespace=namespace,
                file_name=file_name,
                previous_chain_hash=previous_chain_hash,
            )
            entries.append(entry)
            previous_chain_hash = entry.chain_hash
        return tuple(entries), after

    def _ensure_queue_migration_cycle_locked(
        self,
        entries: tuple[LabQuarantineQueueMigrationIndexEntry, ...],
        directories: tuple[LabQuarantineQueueMigrationDirectory, ...],
    ) -> LabQuarantineQueueMigrationCycle:
        previous_chain_hash = _QUEUE_MIGRATION_CHAIN_GENESIS
        for expected_index, entry in enumerate(entries, start=1):
            if entry.index != expected_index or entry.previous_chain_hash != previous_chain_hash:
                raise LabArtifactConflictError("quarantine migration index chain is discontinuous")
            previous_chain_hash = entry.chain_hash
        index_hash = entries[-1].chain_hash if entries else _QUEUE_MIGRATION_CHAIN_GENESIS
        cycle = LabQuarantineQueueMigrationCycle(
            total_entries=len(entries),
            index_hash=index_hash,
            directories=directories,
        )
        cycle_root = self.garbage_queue_migration_cycles_dir / cycle.cycle_id.hex
        index_root = cycle_root / "index"
        for directory in (cycle_root, index_root):
            self._guard_mutation()
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            if directory.is_symlink() or not directory.is_dir():
                raise LabArtifactConflictError("quarantine migration cycle directory is unsafe")
            self._guard_mutation()
            directory.chmod(0o700)
        for entry in entries:
            path = self._migration_index_path(cycle, entry.index)
            if os.path.lexists(path):
                if self._load_queue_migration_index_entry(cycle, entry.index) != entry:
                    raise LabArtifactConflictError("quarantine migration index conflicts")
            else:
                self._write_derived_canonical_file(path, entry.canonical_json())
                if self._load_queue_migration_index_entry(cycle, entry.index) != entry:
                    raise LabArtifactConflictError("quarantine migration index changed")
        cycle_path = self._migration_cycle_path(cycle.cycle_id)
        if os.path.lexists(cycle_path):
            if self._load_queue_migration_cycle(cycle_path) != cycle:
                raise LabArtifactConflictError("quarantine migration cycle conflicts")
        else:
            self._write_derived_canonical_file(cycle_path, cycle.canonical_json())
        cursor_path = self._migration_cursor_path(cycle.cycle_id)
        if not os.path.lexists(cursor_path):
            cursor = LabQuarantineQueueMigrationCursor(
                cycle_id=cycle.cycle_id,
                last_chain_hash=_QUEUE_MIGRATION_CHAIN_GENESIS,
            )
            self._write_derived_canonical_file(cursor_path, cursor.canonical_json())
        self._load_queue_migration_cursor(cycle)
        if os.path.lexists(self.garbage_queue_migration_active_path):
            current = self._load_queue_migration_cycle(self.garbage_queue_migration_active_path)
            if current != cycle:
                self._replace_recovery_queue_state(
                    self.garbage_queue_migration_active_path,
                    cycle.canonical_json(),
                )
        else:
            self._write_derived_canonical_file(
                self.garbage_queue_migration_active_path,
                cycle.canonical_json(),
            )
        if self._load_active_queue_migration_cycle_locked() != cycle:
            raise LabArtifactConflictError("quarantine migration activation changed")
        return cycle

    def initialize_legacy_recovery_migration(
        self,
    ) -> LabQuarantineMigrationInitializationResult:
        """Build one explicit immutable legacy snapshot outside ordinary worker recovery."""
        with self.report_spool.evidence_lock():
            if os.path.lexists(self.garbage_queue_migration_complete_path):
                marker = self._load_queue_migration_complete_locked()
                if self._migration_directory_identities() == marker.directories:
                    return LabQuarantineMigrationInitializationResult(indexed=0, complete=True)
                self._archive_queue_migration_complete_locked(marker)
            if os.path.lexists(self.garbage_queue_migration_active_path):
                active = self._load_active_queue_migration_cycle_locked()
                cursor = self._load_queue_migration_cursor(active)
                if cursor.last_index < active.total_entries:
                    return LabQuarantineMigrationInitializationResult(
                        indexed=active.total_entries,
                        complete=False,
                    )
                directories = self._migration_directory_identities()
                if (
                    directories == active.directories
                    and self._write_queue_migration_complete_locked(
                        active,
                        cursor,
                        directories,
                    )
                ):
                    return LabQuarantineMigrationInitializationResult(indexed=0, complete=True)
            entries, directories = self._migration_index_entries_locked()
            cycle = self._ensure_queue_migration_cycle_locked(entries, directories)
            complete = not entries
            if complete:
                cursor = self._load_queue_migration_cursor(cycle)
                complete = self._write_queue_migration_complete_locked(
                    cycle,
                    cursor,
                    directories,
                )
            return LabQuarantineMigrationInitializationResult(
                indexed=len(entries),
                complete=complete,
            )

    def _has_recovery_marker_locked(self, garbage_id: UUID) -> bool:
        return any(
            os.path.lexists(self._intent_marker_path(directory, garbage_id))
            for directory in (
                self.garbage_active_intent_dir,
                self.garbage_cold_health_dir,
                self.garbage_cold_conflict_dir,
                self.garbage_cold_intent_dir,
            )
        )

    def _is_recovery_phase_enqueued(
        self,
        intent: LabGarbagePreparedIntent,
        phase: Literal["active", "cold_health"],
    ) -> bool:
        return os.path.lexists(self._recovery_queue_enqueued_path(intent, phase))

    def migrate_legacy_recovery_queue(
        self,
        *,
        max_entries: int,
    ) -> LabQuarantineMigrationResult:
        """Consume a bounded explicit migration snapshot without namespace scans."""
        if max_entries < 1:
            raise ValueError("legacy quarantine migration max_entries must be positive")
        with self.report_spool.evidence_lock():
            if os.path.lexists(self.garbage_queue_migration_complete_path):
                marker = self._load_queue_migration_complete_locked()
                if self._migration_directory_identities() == marker.directories:
                    return LabQuarantineMigrationResult(scanned=0, enqueued=0, complete=True)
                self._archive_queue_migration_complete_locked(marker)
                return LabQuarantineMigrationResult(scanned=0, enqueued=0, complete=False)
            if not os.path.lexists(self.garbage_queue_migration_active_path):
                raise LabArtifactConflictError(
                    "legacy recovery migration requires explicit initialization"
                )
            cycle = self._load_active_queue_migration_cycle_locked()
            cursor = self._load_queue_migration_cursor(cycle)
            scanned = 0
            enqueued = 0
            directories = dict(self._migration_namespace_directories())
            while scanned < max_entries and cursor.last_index < cycle.total_entries:
                index = cursor.last_index + 1
                entry = self._load_queue_migration_index_entry(cycle, index)
                if entry.previous_chain_hash != cursor.last_chain_hash:
                    raise LabArtifactConflictError(
                        "quarantine migration index previous chain conflicts"
                    )
                path = directories[entry.namespace] / entry.file_name
                intent = self._load_prepared_intent(path)
                match = _GARBAGE_INTENT_NAME.fullmatch(entry.file_name)
                if match is None:  # pragma: no cover - model validates the name
                    raise LabArtifactConflictError("legacy migration index name is invalid")
                if intent.owner.garbage_id.hex != match.group("garbage_id"):
                    raise LabArtifactConflictError("legacy migration source identity conflicts")
                if entry.namespace == "authority":
                    self._ensure_intent_recovery_marker(intent)
                else:
                    self._enqueue_recovery_intent(intent, phase=entry.namespace)
                scanned += 1
                enqueued += 1
                cursor = LabQuarantineQueueMigrationCursor(
                    cycle_id=cycle.cycle_id,
                    last_index=index,
                    last_chain_hash=entry.chain_hash,
                )
                self._write_queue_migration_cursor(cycle, cursor, entry)
            complete = False
            if cursor.last_index == cycle.total_entries:
                if cursor.last_chain_hash != cycle.index_hash:
                    raise LabArtifactConflictError(
                        "quarantine migration final cursor chain conflicts"
                    )
                observed_directories = self._migration_directory_identities()
                if observed_directories == cycle.directories:
                    complete = self._write_queue_migration_complete_locked(
                        cycle,
                        cursor,
                        observed_directories,
                    )
            return LabQuarantineMigrationResult(
                scanned=scanned,
                enqueued=enqueued,
                complete=complete,
            )

    def _retire_cold_health_marker_locked(
        self,
        path: Path,
        intent: LabGarbagePreparedIntent,
        *,
        conflict: bool,
    ) -> None:
        target_directory = (
            self.garbage_cold_conflict_dir if conflict else self.garbage_cold_intent_dir
        )
        target = self._intent_marker_path(target_directory, intent.owner.garbage_id)
        if os.path.lexists(target):
            raise LabArtifactConflictError("cold health marker retirement conflicts")
        self._guard_mutation()
        os.rename(path, target)
        _fsync_directory(self.garbage_cold_health_dir)
        _fsync_directory(target_directory)
        if os.path.lexists(path) or self._load_prepared_intent(target) != intent:
            raise LabArtifactConflictError("cold health marker retirement changed identity")

    def _ensure_authoritative_queue_intent(
        self,
        intent: LabGarbagePreparedIntent,
    ) -> None:
        target = self._prepared_intent_path(intent.owner.garbage_id)
        if not os.path.lexists(target):
            self._write_derived_canonical_file(target, intent.canonical_json())
        if self._load_prepared_intent(target) != intent:
            raise LabArtifactConflictError("queue intent conflicts with authoritative intent")

    def _process_active_queue_entry(self, entry: LabQuarantineQueueEntry) -> None:
        self._ensure_authoritative_queue_intent(entry.intent)
        self._ensure_intent_recovery_marker(entry.intent)
        self._reconcile_prepared_intent(entry.intent)

    def _recovery_marker_paths(
        self,
        intent: LabGarbagePreparedIntent,
    ) -> dict[str, Path]:
        return {
            state: self._intent_marker_path(directory, intent.owner.garbage_id)
            for state, directory in {
                "active": self.garbage_active_intent_dir,
                "cold_health": self.garbage_cold_health_dir,
                "cold": self.garbage_cold_intent_dir,
                "cold_conflict": self.garbage_cold_conflict_dir,
            }.items()
        }

    def _process_cold_health_queue_entry(
        self,
        entry: LabQuarantineQueueEntry,
    ) -> Exception | None:
        intent = entry.intent
        self._ensure_authoritative_queue_intent(intent)
        paths = self._recovery_marker_paths(intent)
        existing = {state: path for state, path in paths.items() if os.path.lexists(path)}
        if len(existing) > 1:
            raise LabArtifactConflictError("cold health queue has duplicate intent markers")
        if "cold" in existing:
            if self._load_prepared_intent(existing["cold"]) != intent:
                raise LabArtifactConflictError("cold intent marker conflicts")
            return None
        if "cold_conflict" in existing:
            if self._load_prepared_intent(existing["cold_conflict"]) != intent:
                raise LabArtifactConflictError("cold conflict marker conflicts")
            return None
        if "active" in existing:
            self._retire_active_intent_marker(intent)
            existing = {"cold_health": paths["cold_health"]}
        if not existing:
            self._write_derived_canonical_file(
                paths["cold_health"],
                intent.canonical_json(),
            )
            existing = {"cold_health": paths["cold_health"]}
        health = existing.get("cold_health")
        if health is None or self._load_prepared_intent(health) != intent:
            raise LabArtifactConflictError("cold health intent marker conflicts")
        try:
            self._validate_deferred_bundle_metadata(
                self.garbage_deferred_dir / intent.owner.garbage_id.hex,
                expected_owner=intent.owner,
            )
        except Exception as exc:
            self._retire_cold_health_marker_locked(health, intent, conflict=True)
            return exc
        self._retire_cold_health_marker_locked(health, intent, conflict=False)
        return None

    def _retire_recovery_queue_entry(self, entry: LabQuarantineQueueEntry) -> None:
        pending = self._recovery_queue_path(entry.sequence)
        archived = self._recovery_queue_path(entry.sequence, archived=True)
        pending_exists = os.path.lexists(pending)
        archived_exists = os.path.lexists(archived)
        if pending_exists and archived_exists:
            raise LabArtifactConflictError("quarantine queue entry exists in two states")
        if archived_exists:
            if self._load_recovery_queue_entry(archived) != entry:
                raise LabArtifactConflictError("archived quarantine queue entry conflicts")
            return
        if not pending_exists or self._load_recovery_queue_entry(pending) != entry:
            raise LabArtifactConflictError("pending quarantine queue entry is missing")
        self._guard_mutation()
        os.rename(pending, archived)
        _fsync_directory(self.garbage_recovery_queue_pending_dir)
        _fsync_directory(self.garbage_recovery_queue_archive_dir)
        if os.path.lexists(pending) or self._load_recovery_queue_entry(archived) != entry:
            raise LabArtifactConflictError("quarantine queue retirement changed identity")

    def recover_active(self, *, max_entries: int = 16) -> LabQuarantineRecoveryResult:
        """Consume a bounded durable recovery queue without enumerating intent history."""
        if max_entries < 1:
            raise ValueError("quarantine recovery max_entries must be positive")
        first_error: Exception | None = None
        reconciled = 0
        cold_metadata_checked = 0
        queue_conflicts = 0
        with self.report_spool.evidence_lock():
            sequence_state = self._load_recovery_queue_sequence_locked()
            sequence_state, _unsequenced = self._commit_unsequenced_recovery_entry_locked(
                sequence_state
            )
            cursor = self._load_recovery_queue_cursor_locked()
            processed = 0
            probes = 0
            probe_limit = max(16, max_entries * 4)
            while (
                processed < max_entries
                and probes < probe_limit
                and cursor.last_sequence < sequence_state.last_sequence
            ):
                sequence = cursor.last_sequence + 1
                probes += 1
                pending = self._recovery_queue_path(sequence)
                archived = self._recovery_queue_path(sequence, archived=True)
                if os.path.lexists(pending) and os.path.lexists(archived):
                    cursor = self._retire_recovery_queue_conflict_locked(
                        sequence,
                        reason="ambiguous_delivery",
                    )
                    queue_conflicts += 1
                    processed += 1
                    continue
                if os.path.lexists(archived):
                    try:
                        self._load_recovery_queue_entry(archived)
                    except LabArtifactConflictError:
                        cursor = self._retire_recovery_queue_conflict_locked(
                            sequence,
                            reason="corrupt_archived",
                        )
                        queue_conflicts += 1
                        processed += 1
                        continue
                    cursor = LabQuarantineQueueCursor(last_sequence=sequence)
                    self._write_recovery_queue_cursor_locked(cursor)
                    continue
                if not os.path.lexists(pending):
                    cursor = self._retire_recovery_queue_conflict_locked(
                        sequence,
                        reason="missing_pending",
                    )
                    queue_conflicts += 1
                    processed += 1
                    continue
                try:
                    entry = self._load_recovery_queue_entry(pending)
                except LabArtifactConflictError:
                    cursor = self._retire_recovery_queue_conflict_locked(
                        sequence,
                        reason="corrupt_pending",
                    )
                    queue_conflicts += 1
                    processed += 1
                    continue
                self._ensure_recovery_queue_marker(entry)
                if entry.phase == "active":
                    self._process_active_queue_entry(entry)
                    reconciled += 1
                else:
                    cold_metadata_checked += 1
                    failure = self._process_cold_health_queue_entry(entry)
                    if failure is not None and first_error is None:
                        first_error = failure
                self._retire_recovery_queue_entry(entry)
                cursor = LabQuarantineQueueCursor(last_sequence=sequence)
                self._write_recovery_queue_cursor_locked(cursor)
                processed += 1
        if first_error is not None:
            raise first_error
        return LabQuarantineRecoveryResult(
            inspected=reconciled,
            reconciled=reconciled,
            cold_metadata_checked=cold_metadata_checked,
            queue_conflicts=queue_conflicts,
        )

    def quarantine_entries(self) -> tuple[LabQuarantineEntry, ...]:
        with self.report_spool.evidence_lock():
            self._collect_garbage_locked()
            entries: list[LabQuarantineEntry] = []
            for bundle in sorted(self.garbage_deferred_dir.iterdir()):
                owner = self._validate_garbage_bundle(bundle)
                latest = self._latest_garbage_ledger(owner)
                if latest.state != "deferred_gc":
                    raise LabArtifactConflictError("deferred quarantine has no deferred_gc ledger")
                retained_bytes = sum(
                    entry.size or 0 for entry in owner.inventory if entry.file_type == "regular"
                )
                entries.append(
                    LabQuarantineEntry(
                        state=latest.state,
                        owner=owner,
                        ledger_paths=self._garbage_ledgers(owner),
                        bundle_path=bundle,
                        retained_bytes=retained_bytes,
                    )
                )
            return tuple(entries)

    def quarantine_summary(self) -> LabQuarantineSummary:
        """Expose retained P1.3 bytes for the later exclusive-window lifecycle GC."""
        with self.report_spool.evidence_lock():
            bundle_count = 0
            retained_bytes = 0
            for bundle in sorted(self.garbage_deferred_dir.iterdir()):
                try:
                    bundle_id = UUID(hex=bundle.name)
                    root = bundle.lstat()
                except (OSError, ValueError) as exc:
                    raise LabArtifactConflictError(
                        "deferred quarantine bundle identity is invalid"
                    ) from exc
                if bundle.is_symlink() or not stat.S_ISDIR(root.st_mode):
                    raise LabArtifactConflictError("deferred quarantine bundle is unsafe")
                names = {child.name for child in bundle.iterdir()}
                if names != {"owner.json", "payload"}:
                    raise LabArtifactConflictError(
                        f"deferred quarantine has unexpected entries: {sorted(names)}"
                    )
                owner = self._load_garbage_owner_ledger(bundle_id)
                if self._load_garbage_owner(bundle / "owner.json") != owner:
                    raise LabArtifactConflictError(
                        "deferred quarantine bundle owner conflicts with ledger"
                    )
                intent = self._load_prepared_intent(self._prepared_intent_path(bundle_id))
                if intent.owner != owner:
                    raise LabArtifactConflictError(
                        "deferred quarantine prepared intent conflicts with ledger"
                    )
                latest = self._latest_garbage_ledger(owner)
                if latest.state != "deferred_gc":
                    raise LabArtifactConflictError("deferred quarantine has no deferred_gc ledger")
                payload = bundle / "payload"
                try:
                    payload_stat = payload.lstat()
                except OSError as exc:
                    raise LabArtifactConflictError(
                        "deferred quarantine payload root is missing"
                    ) from exc
                root_inventory = owner.inventory[0]
                expected_mode = (
                    stat.S_ISDIR(payload_stat.st_mode)
                    if root_inventory.file_type == "directory"
                    else stat.S_ISREG(payload_stat.st_mode)
                )
                if (
                    payload.is_symlink()
                    or not expected_mode
                    or (payload_stat.st_dev, payload_stat.st_ino)
                    != (root_inventory.device, root_inventory.inode)
                    or (root_inventory.file_type == "regular" and payload_stat.st_nlink != 1)
                ):
                    raise LabArtifactConflictError(
                        "deferred quarantine payload root conflicts with ledger"
                    )
                bundle_count += 1
                retained_bytes += sum(
                    entry.size or 0 for entry in owner.inventory if entry.file_type == "regular"
                )
            return LabQuarantineSummary(
                bundle_count=bundle_count,
                retained_bytes=retained_bytes,
            )

    def _logical_delete(
        self,
        path: Path,
        *,
        owner: LabGarbageOwner,
    ) -> bool:
        intent = self._prepared_intent(owner)
        self._write_prepared_intent(intent)
        self._reconcile_prepared_intent(intent)
        return True

    def logical_quarantine_tree(
        self,
        path: Path,
        *,
        purpose: str,
    ) -> bool:
        if not os.path.lexists(path):
            return False
        self._assert_safe_artifact_ancestors(path.parent)
        inventory = self._garbage_inventory(path)
        owner = self._garbage_owner(
            path,
            purpose=purpose,
            inventory=inventory,
        )
        with self.report_spool.evidence_lock():
            self._guard_mutation()
            return self._logical_delete(path, owner=owner)

    def logical_delete_temporary_tree(
        self,
        path: Path,
        *,
        current_claim: LabShardClaim,
    ) -> bool:
        self._assert_safe_temporary_tree(path)
        return self.logical_quarantine_tree(
            path,
            purpose=(
                "crash temporary cleanup "
                f"job={current_claim.job_id} shard={current_claim.shard_id} "
                f"generation={current_claim.claim_generation} "
                f"token={current_claim.claim_token}"
            ),
        )

    def _safe_remove_regular_child(
        self,
        path: Path,
        *,
        expected: LabRegularFileIdentity,
        label: str,
    ) -> bool:
        parent = path.parent
        self._assert_safe_artifact_ancestors(parent)
        if path != parent / path.name or path.name in {"", ".", ".."}:
            raise LabArtifactConflictError(f"{label} path is unsafe")
        if parent.is_symlink() or not parent.is_dir():
            raise LabArtifactConflictError(f"{label} parent is unsafe")
        try:
            observed = self._regular_file_identity(path, label=label)
        except LabArtifactConflictError:
            if not os.path.lexists(path):
                return False
            raise
        if observed != expected:
            raise LabArtifactConflictError(f"{label} changed before deletion")
        inventory = (
            LabGarbageInventoryEntry(
                relative_path=".",
                file_type="regular",
                device=expected.device,
                inode=expected.inode,
                size=expected.size,
                sha256=expected.sha256,
            ),
        )
        owner = self._garbage_owner(path, purpose=label, inventory=inventory)
        return self._logical_delete(path, owner=owner)

    def _remove_ledger(self, path: Path) -> None:
        if not os.path.lexists(path):
            return
        expected = self._regular_file_identity(path, label="reclaim ledger")
        self._load_ledger(path)
        self._safe_remove_regular_child(
            path,
            expected=expected,
            label="reclaim ledger",
        )

    @staticmethod
    def _inventory_entry(path: Path, *, relative_path: str) -> LabReclaimInventoryEntry:
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode) or path.is_symlink():
            raise LabArtifactConflictError(f"reclaim inventory path is unsafe: {relative_path}")
        if before.st_nlink != 1:
            raise LabArtifactConflictError(
                f"reclaim inventory file has an external hard link: {relative_path}"
            )
        digest = _file_sha256(path)
        after = path.lstat()
        if (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_nlink,
        ) != (before.st_dev, before.st_ino, before.st_size, 1):
            raise LabArtifactConflictError(
                f"reclaim inventory file changed while hashing: {relative_path}"
            )
        return LabReclaimInventoryEntry(
            relative_path=relative_path,
            device=before.st_dev,
            inode=before.st_ino,
            size=before.st_size,
            sha256=digest,
        )

    def _build_inventory(
        self,
        bundle: Path,
        manifest: LabShardResultManifest,
    ) -> tuple[LabReclaimInventoryEntry, ...]:
        names = sorted(
            ("manifest.json",) + tuple(artifact.file_name for artifact in manifest.artifacts)
        )
        return tuple(self._inventory_entry(bundle / name, relative_path=name) for name in names)

    def _sealed_quarantine_owner(
        self,
        tombstone: Path,
        *,
        obsolete_claim: LabShardClaim,
        source_device: int,
        source_inode: int,
        inventory: tuple[LabReclaimInventoryEntry, ...],
    ) -> LabGarbageOwner:
        garbage_inventory = (
            LabGarbageInventoryEntry(
                relative_path=".",
                file_type="directory",
                device=source_device,
                inode=source_inode,
            ),
            *(
                LabGarbageInventoryEntry(
                    relative_path=entry.relative_path,
                    file_type="regular",
                    device=entry.device,
                    inode=entry.inode,
                    size=entry.size,
                    sha256=entry.sha256,
                )
                for entry in inventory
            ),
        )
        return LabGarbageOwner(
            purpose=(
                "obsolete sealed attempt "
                f"job={obsolete_claim.job_id} shard={obsolete_claim.shard_id} "
                f"generation={obsolete_claim.claim_generation} "
                f"token={obsolete_claim.claim_token}"
            ),
            original_relative_path=self._garbage_relative_path(tombstone),
            payload_type="directory",
            inventory=tuple(sorted(garbage_inventory, key=lambda entry: entry.relative_path)),
        )

    def _reclaim_quarantine_owner(
        self,
        tombstone: Path,
        ledger: LabReclaimLedger,
    ) -> LabGarbageOwner:
        owner = self._sealed_quarantine_owner(
            tombstone,
            obsolete_claim=ledger.obsolete_claim,
            source_device=ledger.source_device,
            source_inode=ledger.source_inode,
            inventory=ledger.inventory,
        )
        if ledger.quarantine_id is not None and ledger.quarantine_id != owner.garbage_id:
            raise LabArtifactConflictError("reclaim quarantine identity conflicts with ledger")
        return owner

    def _validate_isolated_tree(
        self,
        path: Path,
        ledger: LabReclaimLedger,
    ) -> None:
        root_before = path.lstat()
        if path.is_symlink() or not stat.S_ISDIR(root_before.st_mode):
            raise LabArtifactConflictError("reclaim tombstone is unsafe")
        if (root_before.st_dev, root_before.st_ino) != (
            ledger.source_device,
            ledger.source_inode,
        ):
            raise LabArtifactConflictError("reclaim tombstone inode conflicts with durable ledger")
        expected = {entry.relative_path: entry for entry in ledger.inventory}
        actual = {candidate.name: candidate for candidate in path.iterdir()}
        unknown = set(actual) - set(expected)
        if unknown:
            raise LabArtifactConflictError(
                f"reclaim tombstone contains unknown paths: {sorted(unknown)}"
            )
        if ledger.state == "prepared" and set(actual) != set(expected):
            raise LabArtifactConflictError("prepared reclaim tombstone is incomplete")
        for name, candidate in actual.items():
            observed = self._inventory_entry(candidate, relative_path=name)
            if observed != expected[name]:
                raise LabArtifactConflictError(
                    f"reclaim tombstone inventory identity conflicts: {name}"
                )
        root_after = path.lstat()
        if (root_after.st_dev, root_after.st_ino) != (
            root_before.st_dev,
            root_before.st_ino,
        ):
            raise LabArtifactConflictError("reclaim tombstone changed while validating")

    def _delete_inventory_entry(
        self,
        directory: Path,
        entry: LabReclaimInventoryEntry,
    ) -> None:
        target = directory / entry.relative_path
        if not os.path.lexists(target):
            return
        if self._inventory_entry(target, relative_path=entry.relative_path) != entry:
            raise LabArtifactConflictError(
                f"reclaim inventory changed before deletion: {entry.relative_path}"
            )
        self._safe_remove_regular_child(
            target,
            expected=LabRegularFileIdentity(
                device=entry.device,
                inode=entry.inode,
                size=entry.size,
                sha256=entry.sha256,
            ),
            label=f"reclaim inventory {entry.relative_path}",
        )

    def _delete_isolated_tombstone(
        self,
        tombstone: Path,
        ledger: LabReclaimLedger,
    ) -> LabGarbageOwner:
        self._validate_isolated_tree(tombstone, ledger)
        owner = self._reclaim_quarantine_owner(tombstone, ledger)
        self._logical_delete(tombstone, owner=owner)
        return owner

    def _cleanup_ledger_temporaries(self, directory: Path) -> None:
        if not directory.exists():
            return
        if directory.is_symlink() or not directory.is_dir():
            raise LabArtifactConflictError("reclaim ledger directory is unsafe")
        for candidate in sorted(directory.iterdir(), key=lambda path: path.name):
            if not candidate.name.endswith(".tmp"):
                continue
            if self._LEDGER_TEMP_NAME.fullmatch(candidate.name) is None:
                raise LabArtifactConflictError("unknown reclaim ledger temporary file")
            expected = self._regular_file_identity(
                candidate,
                label="reclaim ledger temporary file",
            )
            self._safe_remove_regular_child(
                candidate,
                expected=expected,
                label="reclaim ledger temporary file",
            )

    def _assert_no_terminal_success_evidence_from(
        self,
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
        current_claim: LabShardClaim,
        *,
        pending: tuple[LabReportSpoolEntry, ...],
        receipt_paths: tuple[Path, ...],
    ) -> None:
        for entry in pending:
            report = entry.report
            if not self._report_matches_attempt(report, claim):
                continue
            if not isinstance(report.body, LabShardSucceeded):
                continue
            if claim.claim_generation < current_claim.claim_generation:
                continue
            if report.body.result_manifest_hash != manifest.manifest_hash:
                raise LabArtifactConflictError(
                    "pending success manifest conflicts with sealed attempt"
                )
            raise LabArtifactConflictError(
                "pending success may already be committed before receipt ack"
            )

        for path in receipt_paths:
            receipt = self.report_spool.load_receipt(path)
            if (receipt.job_id, receipt.shard_id) != (claim.job_id, claim.shard_id):
                continue
            if receipt.claim_token is None:
                if receipt.status == "accepted":
                    raise LabArtifactConflictError(
                        "legacy accepted receipt cannot prove a safe attempt deletion"
                    )
                continue
            if not self._receipt_matches_attempt(receipt, claim):
                continue
            if receipt.report_type != "shard_succeeded":
                continue
            if receipt.result_manifest_hash != manifest.manifest_hash:
                raise LabArtifactConflictError(
                    "success receipt manifest conflicts with sealed attempt"
                )
            if receipt.status == "accepted":
                raise LabArtifactConflictError(
                    "accepted success receipt protects terminal artifact"
                )

    def _assert_no_terminal_success_evidence(
        self,
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
        current_claim: LabShardClaim,
    ) -> None:
        self._assert_no_terminal_success_evidence_from(
            claim,
            manifest,
            current_claim,
            pending=self.report_spool.pending(),
            receipt_paths=tuple(sorted(self.report_spool.ack_dir.glob("*.json"))),
        )

    def _assert_no_terminal_success_evidence_locked(
        self,
        claim: LabShardClaim,
        manifest: LabShardResultManifest,
        current_claim: LabShardClaim,
    ) -> None:
        self._assert_no_terminal_success_evidence_from(
            claim,
            manifest,
            current_claim,
            pending=self.report_spool.pending_locked(),
            receipt_paths=self.report_spool.receipt_paths_locked(),
        )

    @staticmethod
    def _obsolete_claim(
        current_claim: LabShardClaim,
        *,
        fence: int,
        generation: int,
        token: UUID,
    ) -> LabShardClaim:
        if generation >= current_claim.claim_generation:
            raise LabArtifactConflictError(
                "reclaim identity is not older than durable claim high-water"
            )
        if fence > current_claim.scheduler_fencing_token:
            raise LabArtifactConflictError("obsolete attempt has a future scheduler fencing token")
        return current_claim.model_copy(
            update={
                "claim_token": token,
                "claim_generation": generation,
                "scheduler_fencing_token": fence,
            }
        )

    def _validate_tombstone(
        self,
        path: Path,
        current_claim: LabShardClaim,
    ) -> tuple[LabShardClaim, LabShardResultManifest]:
        fence, generation, token, expected_hash = self._parse_tombstone_name(path.name)
        obsolete_claim = self._obsolete_claim(
            current_claim,
            fence=fence,
            generation=generation,
            token=token,
        )
        ledger = self._load_ledger(self._ledger_path(current_claim, path.name))
        manifest = ledger.manifest
        self._validate_ledger(
            ledger,
            current_claim=current_claim,
            obsolete_claim=obsolete_claim,
            manifest=manifest,
        )
        if manifest.manifest_hash != expected_hash:
            raise LabArtifactConflictError(
                "reclaim tombstone manifest does not match its durable identity"
            )
        self._validate_isolated_tree(path, ledger)
        return obsolete_claim, manifest

    def _classify_attempt(
        self,
        candidate: Path,
        current_claim: LabShardClaim,
    ) -> tuple[LabShardClaim, LabShardResultManifest] | None:
        fence, generation, token = self._parse_attempt_name(candidate.name)
        candidate_identity = (fence, generation, token)
        current_identity = (
            current_claim.scheduler_fencing_token,
            current_claim.claim_generation,
            current_claim.claim_token,
        )
        if generation > current_claim.claim_generation:
            raise LabArtifactConflictError(
                "future sealed attempt conflicts with durable claim high-water"
            )
        if generation == current_claim.claim_generation:
            if candidate_identity != current_identity:
                raise LabArtifactConflictError(
                    "current-generation sealed attempt has conflicting identity"
                )
            return None
        obsolete_claim = self._obsolete_claim(
            current_claim,
            fence=fence,
            generation=generation,
            token=token,
        )
        if self.sealed_bundle_path(obsolete_claim) != candidate:
            raise LabArtifactConflictError(
                "sealed attempt directory does not match parsed identity"
            )
        return obsolete_claim, self._validate_bundle(candidate, obsolete_claim)

    @staticmethod
    def _attempt_identity(claim: LabShardClaim) -> tuple[int, int, UUID]:
        return (
            claim.scheduler_fencing_token,
            claim.claim_generation,
            claim.claim_token,
        )

    def _inventory(
        self,
        current_claim: LabShardClaim,
        attempts_root: Path,
    ) -> tuple[
        tuple[tuple[Path, LabShardClaim, LabShardResultManifest], ...],
        tuple[tuple[Path, LabShardClaim, LabShardResultManifest], ...],
    ]:
        sources: dict[
            tuple[int, int, UUID],
            tuple[Path, LabShardClaim, LabShardResultManifest],
        ] = {}
        tombstones: dict[
            tuple[int, int, UUID],
            tuple[Path, LabShardClaim, LabShardResultManifest],
        ] = {}
        candidates = tuple(sorted(attempts_root.iterdir(), key=lambda path: path.name))
        source_names: set[tuple[int, int, UUID]] = set()
        tombstone_names: set[tuple[int, int, UUID]] = set()
        for candidate in candidates:
            if candidate.name.startswith(".reclaim-"):
                fence, generation, token, _manifest_hash = self._parse_tombstone_name(
                    candidate.name
                )
                identity = (fence, generation, token)
                if identity in tombstone_names:
                    raise LabArtifactConflictError(
                        "multiple tombstones claim the same attempt identity"
                    )
                tombstone_names.add(identity)
            else:
                identity = self._parse_attempt_name(candidate.name)
                if identity in source_names:
                    raise LabArtifactConflictError(
                        "multiple sources claim the same attempt identity"
                    )
                source_names.add(identity)
        if source_names & tombstone_names:
            raise LabArtifactConflictError(
                "source and tombstone coexist for the same attempt identity"
            )

        for candidate in candidates:
            if candidate.is_symlink() or not candidate.is_dir():
                raise LabArtifactConflictError(
                    f"sealed attempt is a symlink or not a directory: {candidate.name}"
                )
            if candidate.name.startswith(".reclaim-"):
                obsolete_claim, manifest = self._validate_tombstone(
                    candidate,
                    current_claim,
                )
                identity = self._attempt_identity(obsolete_claim)
                if identity in tombstones:
                    raise LabArtifactConflictError(
                        "multiple tombstones claim the same attempt identity"
                    )
                tombstones[identity] = (candidate, obsolete_claim, manifest)
                continue
            classified = self._classify_attempt(candidate, current_claim)
            if classified is None:
                continue
            obsolete_claim, manifest = classified
            identity = self._attempt_identity(obsolete_claim)
            if identity in sources:
                raise LabArtifactConflictError("multiple sources claim the same attempt identity")
            sources[identity] = (candidate, obsolete_claim, manifest)
        return tuple(sources.values()), tuple(tombstones.values())

    def _preflight(self, current_claim: LabShardClaim, attempts_root: Path) -> None:
        sources, tombstones = self._inventory(current_claim, attempts_root)
        for _candidate, obsolete_claim, manifest in sources:
            self._assert_no_terminal_success_evidence(
                obsolete_claim,
                manifest,
                current_claim,
            )
        for tombstone, obsolete_claim, manifest in tombstones:
            ledger_path = self._ledger_path(current_claim, tombstone.name)
            ledger = self._load_ledger(ledger_path)
            self._validate_ledger(
                ledger,
                current_claim=current_claim,
                obsolete_claim=obsolete_claim,
                manifest=manifest,
            )
            self._validate_isolated_tree(tombstone, ledger)
            self._assert_no_terminal_success_evidence(
                obsolete_claim,
                manifest,
                current_claim,
            )

    def _reclaim_locked(self, current_claim: LabShardClaim, attempts_root: Path) -> None:
        sources, tombstones = self._inventory(current_claim, attempts_root)
        for tombstone, obsolete_claim, manifest in tombstones:
            ledger_path = self._ledger_path(current_claim, tombstone.name)
            ledger = self._load_ledger(ledger_path)
            self._validate_ledger(
                ledger,
                current_claim=current_claim,
                obsolete_claim=obsolete_claim,
                manifest=manifest,
            )
            self._validate_isolated_tree(tombstone, ledger)
            self._assert_no_terminal_success_evidence_locked(
                obsolete_claim,
                manifest,
                current_claim,
            )
            if ledger.state == "prepared":
                ledger = ledger.model_copy(update={"state": "isolated"})
                self._write_ledger(ledger)
            owner = self._delete_isolated_tombstone(tombstone, ledger)
            ledger = ledger.model_copy(
                update={
                    "state": "deferred_gc",
                    "quarantine_id": owner.garbage_id,
                }
            )
            self._write_ledger(ledger)

        for candidate, obsolete_claim, manifest in sources:
            self._assert_no_terminal_success_evidence_locked(
                obsolete_claim,
                manifest,
                current_claim,
            )
            tombstone = attempts_root / self._tombstone_name(obsolete_claim, manifest)
            source_identity = LabWorker._bundle_file_identity(candidate)
            inventory = self._build_inventory(candidate, manifest)
            owner = self._sealed_quarantine_owner(
                tombstone,
                obsolete_claim=obsolete_claim,
                source_device=source_identity[0],
                source_inode=source_identity[1],
                inventory=inventory,
            )
            ledger_path = self._ledger_path(current_claim, tombstone.name)
            if os.path.lexists(ledger_path):
                stale = self._load_ledger(ledger_path)
                self._validate_ledger(
                    stale,
                    current_claim=current_claim,
                    obsolete_claim=obsolete_claim,
                    manifest=manifest,
                )
                if (
                    stale.state != "prepared"
                    or source_identity
                    != (
                        stale.source_device,
                        stale.source_inode,
                    )
                    or stale.inventory != inventory
                ):
                    raise LabArtifactConflictError(
                        "stale reclaim ledger conflicts with live source"
                    )
                if stale.quarantine_id not in {None, owner.garbage_id}:
                    raise LabArtifactConflictError(
                        "stale reclaim ledger quarantine identity conflicts"
                    )
                ledger = stale
            else:
                ledger = LabReclaimLedger(
                    state="prepared",
                    current_claim=current_claim,
                    obsolete_claim=obsolete_claim,
                    manifest=manifest,
                    inventory=inventory,
                    source_name=candidate.name,
                    tombstone_name=tombstone.name,
                    source_device=source_identity[0],
                    source_inode=source_identity[1],
                    quarantine_id=owner.garbage_id,
                )
                self._write_ledger(ledger)
            try:
                self._guard_mutation()
                os.rename(candidate, tombstone)
            except OSError as exc:
                if exc.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                    raise
                if not tombstone.is_dir() or candidate.exists():
                    raise LabArtifactConflictError(
                        "reclaim tombstone conflicts with sealed attempt"
                    ) from exc
                self._validate_isolated_tree(tombstone, ledger)
            _fsync_directory(attempts_root)
            if LabWorker._bundle_file_identity(tombstone) != source_identity:
                if os.path.lexists(candidate):
                    raise LabArtifactConflictError(
                        "sealed attempt was replaced during isolation and cannot be restored"
                    )
                self._guard_mutation()
                os.rename(tombstone, candidate)
                _fsync_directory(attempts_root)
                raise LabArtifactConflictError("sealed attempt was replaced during isolation")
            self._validate_isolated_tree(tombstone, ledger)
            ledger = ledger.model_copy(update={"state": "isolated"})
            self._write_ledger(ledger)
            self._assert_no_terminal_success_evidence_locked(
                obsolete_claim,
                manifest,
                current_claim,
            )
            quarantined_owner = self._delete_isolated_tombstone(tombstone, ledger)
            ledger = ledger.model_copy(
                update={
                    "state": "deferred_gc",
                    "quarantine_id": quarantined_owner.garbage_id,
                }
            )
            self._write_ledger(ledger)

    def _reconcile_orphan_ledgers(self, current_claim: LabShardClaim) -> None:
        directory = self._ledger_dir(current_claim)
        if not directory.exists():
            return
        attempts_root = self.sealed_bundle_path(current_claim).parent
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            if path.name.endswith(".tmp"):
                raise LabArtifactConflictError("reclaim ledger temporary remained after cleanup")
            if path.suffix != ".json":
                raise LabArtifactConflictError("unknown reclaim ledger file")
            ledger = self._load_ledger(path)
            source = attempts_root / ledger.source_name
            tombstone = attempts_root / ledger.tombstone_name
            if os.path.lexists(source) or os.path.lexists(tombstone):
                continue
            self._validate_ledger(
                ledger,
                current_claim=current_claim,
                obsolete_claim=ledger.obsolete_claim,
                manifest=ledger.manifest,
            )
            self._assert_no_terminal_success_evidence_locked(
                ledger.obsolete_claim,
                ledger.manifest,
                current_claim,
            )
            owner = self._reclaim_quarantine_owner(tombstone, ledger)
            if ledger.quarantine_id not in {None, owner.garbage_id}:
                raise LabArtifactConflictError(
                    "orphan reclaim ledger quarantine identity conflicts"
                )
            self._collect_garbage_locked()
            deferred = self.garbage_deferred_dir / owner.garbage_id.hex
            if not os.path.lexists(deferred):
                raise LabArtifactConflictError(
                    "reclaim ledger has no source, tombstone, or deferred quarantine"
                )
            if self._validate_garbage_bundle(deferred) != owner:
                raise LabArtifactConflictError("deferred reclaim quarantine conflicts")
            if ledger.state != "deferred_gc":
                ledger = ledger.model_copy(
                    update={
                        "state": "deferred_gc",
                        "quarantine_id": owner.garbage_id,
                    }
                )
                self._write_ledger(ledger)

    def reclaim(self, current_claim: LabShardClaim | LabShardClaimV2) -> None:
        validated = (
            LabShardClaimV2.model_validate(current_claim, strict=True)
            if isinstance(current_claim, LabShardClaimV2)
            else LabShardClaim.model_validate(current_claim)
        )
        attempts_root = self.sealed_bundle_path(validated).parent
        ledger_dir = self._ledger_dir(validated)
        self._assert_safe_artifact_ancestors(attempts_root)
        self._assert_safe_artifact_ancestors(ledger_dir)
        if not attempts_root.exists() and not ledger_dir.exists():
            return
        if attempts_root.exists() and (attempts_root.is_symlink() or not attempts_root.is_dir()):
            raise LabArtifactConflictError("sealed attempts root is unsafe")
        if attempts_root.exists():
            self._preflight(validated, attempts_root)
        with self.report_spool.evidence_lock():
            self._guard_mutation()
            self._cleanup_ledger_temporaries(ledger_dir)
            if attempts_root.exists():
                self._guard_mutation()
                self._reclaim_locked(validated, attempts_root)
            self._guard_mutation()
            self._reconcile_orphan_ledgers(validated)
