"""Durable, externally fenced runner for closed SourceBroker v2 providers."""

from __future__ import annotations

import fcntl
import os
import re
import secrets
import sqlite3
import stat
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from threading import Event
from types import MappingProxyType
from typing import Any, Literal, Protocol, TypeVar

from pydantic import ConfigDict, Field, model_validator

from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.source_broker_protocol import (
    ServerCredentialsPolicy,
    SocketEndpointPolicy,
    validate_socket_endpoint,
)
from rquant.source_broker_v2 import (
    SourceAuthorityKeyring,
    SourceBrokerV2ClaimOnceRequest,
    SourceBrokerV2ClaimOnceResponse,
    SourceBrokerV2ClaimStatus,
    SourceBrokerV2DispatchEnvelope,
    SourceBrokerV2DispatchRequest,
    SourceBrokerV2DispatchResponse,
    SourceBrokerV2FinalizeEnvelope,
    SourceBrokerV2FinalizeRequest,
    SourceBrokerV2FinalizeResponse,
    SourceBrokerV2OutboxPhase,
    SourceBrokerV2ReplayRequest,
    SourceBrokerV2ReplayResponse,
    SourceBrokerV2ReplayStatus,
    SourceBrokerV2Transport,
    SourceBrokerV2UnixClient,
)
from rquant.source_broker_v2_authority import SourceBrokerV2SchedulerClients
from rquant.source_broker_v2_authority_service import (
    SourceBrokerV2CurrentClaimUnixClient,
    SourceBrokerV2ReplayLineageUnixClient,
    SourceBrokerV2SourceQuotaUnixClient,
)
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2AuthorityRef,
    SourceBrokerV2JobIntentEnvelope,
    SourceBrokerV2JobOutcomeEnvelope,
    SourceBrokerV2JobOutcomeStatus,
    SourceBrokerV2NativeEvidence,
    build_verified_job_outcome,
    canonical_job_model_bytes,
    canonical_job_sha256,
    parse_job_intent,
    parse_job_outcome,
)
from rquant.source_broker_v2_runtime import (
    SourceBrokerV2AuthorityRuntime,
    SourceBrokerV2RootRole,
)
from rquant.source_operation_contracts import require_authorized_source_broker_v2_job_intent
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

_OWNER_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,198}[A-Za-z0-9])?$"
_ENV_PATTERN = r"^[A-Z][A-Z0-9_]{0,127}$"
_T = TypeVar("_T")
_MAPPING_PROXY_TYPE = type(MappingProxyType({}))


class SourceBrokerV2RunnerError(RuntimeError):
    """Base error for the durable SourceBroker v2 job runner."""


class SourceBrokerV2RunnerConflictError(SourceBrokerV2RunnerError):
    """An idempotency key is already bound to different durable intent bytes."""


class SourceBrokerV2RunnerBackpressureError(SourceBrokerV2RunnerError):
    """The runner inbox is at its configured durable capacity."""


class SourceBrokerV2RunnerFencedError(SourceBrokerV2RunnerError):
    """A lease owner attempted to mutate a job after its generation changed."""


class SourceBrokerV2StoreConfigError(SourceBrokerV2RunnerError):
    """The durable runner-owned store configuration is missing or conflicting."""


def _require_stage_store(stage_store: object) -> object:
    from rquant.lab_source_stage import LabSourceStageStore

    if type(stage_store) is not LabSourceStageStore:
        raise TypeError("runner requires an exact LabSourceStageStore authority")
    return stage_store


class _RunnerDeadlineError(SourceBrokerV2RunnerError):
    pass


class SourceBrokerV2JobRunnerState(StrEnum):
    NEW = "NEW"
    CLAIMED = "CLAIMED"
    DISPATCHING = "DISPATCHING"
    TERMINAL = "TERMINAL"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"
    PUBLISHED = "PUBLISHED"


SOURCE_BROKER_V2_JOB_STORE_SCHEMA_VERSION = 2
_PUBLISHED_COMMIT_COLUMNS = (
    "operation_id",
    "intent",
    "intent_hash",
    "source_id",
    "operation_hash",
    "request_hash",
    "deadline_at",
    "stage_authority_hash",
    "stage_record_commitment",
    "state",
    "owner_id",
    "lease_generation",
    "lease_expires_at",
    "heartbeat_at",
    "claim_receipt",
    "dispatch_receipt",
    "source_evidence",
    "claim_evidence",
    "quota_evidence",
    "lineage_evidence",
    "finalize_receipt",
    "outcome",
    "terminal_reason",
    "created_at",
    "updated_at",
)
_PUBLISHED_COMMIT_BLOB_COLUMNS = frozenset(
    {
        "intent",
        "claim_receipt",
        "dispatch_receipt",
        "source_evidence",
        "claim_evidence",
        "quota_evidence",
        "lineage_evidence",
        "finalize_receipt",
        "outcome",
    }
)
_PUBLISHED_REQUIRED_BLOB_COLUMNS = _PUBLISHED_COMMIT_BLOB_COLUMNS - {"claim_receipt"}


def _source_broker_v2_store_config_hash(*, store_id: str, max_inbox: int) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-source-broker-v2-job-store-config/v2",
            "max_inbox": max_inbox,
            "schema_version": SOURCE_BROKER_V2_JOB_STORE_SCHEMA_VERSION,
            "store_id": store_id,
        }
    )


class SourceBrokerV2JobStoreConfig(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[2] = SOURCE_BROKER_V2_JOB_STORE_SCHEMA_VERSION
    store_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    max_inbox: int = Field(strict=True, ge=1, le=100_000)
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @classmethod
    def create(cls, *, store_id: str, max_inbox: int) -> SourceBrokerV2JobStoreConfig:
        return cls(
            store_id=store_id,
            max_inbox=max_inbox,
            config_hash=_source_broker_v2_store_config_hash(
                store_id=store_id,
                max_inbox=max_inbox,
            ),
        )

    @model_validator(mode="after")
    def validate_config_hash(self) -> SourceBrokerV2JobStoreConfig:
        expected = _source_broker_v2_store_config_hash(
            store_id=self.store_id,
            max_inbox=self.max_inbox,
        )
        if self.config_hash != expected:
            raise ValueError("store config hash conflicts with immutable capacity")
        return self


def source_broker_v2_published_commit_hash(values: Mapping[str, object]) -> str:
    """Hash every durable publication field without claiming external authenticity."""

    if not set(_PUBLISHED_COMMIT_COLUMNS).issubset(values):
        raise SourceBrokerV2StoreConfigError("published row commitment fields are incomplete")
    if values["state"] != SourceBrokerV2JobRunnerState.PUBLISHED.value:
        raise SourceBrokerV2StoreConfigError("published row commitment requires PUBLISHED state")
    normalized: dict[str, object] = {}
    for name in _PUBLISHED_COMMIT_COLUMNS:
        value = values[name]
        if name in _PUBLISHED_COMMIT_BLOB_COLUMNS:
            if value is None:
                if name in _PUBLISHED_REQUIRED_BLOB_COLUMNS:
                    raise SourceBrokerV2StoreConfigError(
                        f"published row is missing required {name} bytes"
                    )
                normalized[name] = None
            elif isinstance(value, bytes | bytearray | memoryview):
                normalized[name] = canonical_job_sha256(bytes(value))
            else:
                raise SourceBrokerV2StoreConfigError(f"published row {name} must contain bytes")
        elif value is None or type(value) in {str, int}:
            normalized[name] = value
        else:
            raise SourceBrokerV2StoreConfigError(
                f"published row {name} has an invalid durable type"
            )
    return canonical_job_sha256(
        {
            "contract": "rquant-source-broker-v2-published-row/v2",
            "row": normalized,
        }
    )


@contextmanager
def open_source_broker_v2_job_storage_connection(
    db_path: Path,
    *,
    busy_timeout_ms: int,
    configure_journal: bool = False,
) -> Iterator[sqlite3.Connection]:
    """Open the shared job store without changing journal mode on ordinary calls."""

    connection = sqlite3.connect(
        db_path,
        timeout=busy_timeout_ms / 1_000,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    try:
        connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
        if configure_journal:
            connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        yield connection
    finally:
        connection.close()


@contextmanager
def _source_broker_v2_job_schema_lock(
    db_path: Path,
    *,
    timeout_ms: int,
) -> Iterator[None]:
    """Serialize first-open schema/WAL setup across independent scheduler processes."""

    lock_path = db_path.with_name(f".{db_path.name}.source-broker-v2-schema.lock")
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    deadline = time.monotonic() + timeout_ms / 1_000
    acquired = False
    try:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except BlockingIOError as exc:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "source broker v2 schema initialization lock timed out"
                    ) from exc
                time.sleep(min(0.01, remaining))
        yield
    finally:
        if acquired:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def initialize_source_broker_v2_job_storage(
    db_path: Path,
    *,
    busy_timeout_ms: int,
    max_inbox: int = 1_000,
) -> None:
    """Initialize the sole durable schema shared by executor and scheduler queue."""

    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    SourceBrokerV2JobStoreConfig.create(
        store_id="0" * 64,
        max_inbox=max_inbox,
    )
    with (
        _source_broker_v2_job_schema_lock(path, timeout_ms=busy_timeout_ms),
        open_source_broker_v2_job_storage_connection(
            path,
            busy_timeout_ms=busy_timeout_ms,
            configure_journal=True,
        ) as connection,
    ):
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                    CREATE TABLE IF NOT EXISTS source_broker_v2_jobs (
                        operation_id TEXT PRIMARY KEY NOT NULL,
                        intent BLOB NOT NULL,
                        intent_hash TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        operation_hash TEXT NOT NULL,
                        request_hash TEXT NOT NULL,
                        deadline_at TEXT NOT NULL,
                        stage_authority_hash TEXT,
                        stage_record_commitment TEXT,
                        state TEXT NOT NULL CHECK (state IN (
                            'NEW', 'CLAIMED', 'DISPATCHING', 'TERMINAL',
                            'RECONCILE_REQUIRED', 'PUBLISHED'
                        )),
                        owner_id TEXT,
                        lease_generation INTEGER NOT NULL,
                        lease_expires_at TEXT,
                        heartbeat_at TEXT,
                        claim_receipt BLOB,
                        dispatch_receipt BLOB,
                        source_evidence BLOB,
                        claim_evidence BLOB,
                        quota_evidence BLOB,
                        lineage_evidence BLOB,
                        finalize_receipt BLOB,
                        outcome BLOB,
                        published_commit_hash TEXT,
                        terminal_reason TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(source_broker_v2_jobs)")
            }
            for name in (
                "source_evidence",
                "claim_evidence",
                "quota_evidence",
                "lineage_evidence",
                "finalize_receipt",
                "published_commit_hash",
                "stage_authority_hash",
                "stage_record_commitment",
            ):
                if name not in columns:
                    declared_type = (
                        "TEXT"
                        if name
                        in {
                            "published_commit_hash",
                            "stage_authority_hash",
                            "stage_record_commitment",
                        }
                        else "BLOB"
                    )
                    connection.execute(
                        f"ALTER TABLE source_broker_v2_jobs ADD COLUMN {name} {declared_type}"
                    )
            connection.execute(
                """
                    CREATE TABLE IF NOT EXISTS source_broker_v2_store_config (
                        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                        schema_version INTEGER NOT NULL,
                        store_id TEXT NOT NULL,
                        max_inbox INTEGER NOT NULL,
                        config_hash TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                    """
            )
            config_rows = connection.execute(
                "SELECT * FROM source_broker_v2_store_config ORDER BY singleton"
            ).fetchall()
            if not config_rows:
                config = SourceBrokerV2JobStoreConfig.create(
                    store_id=secrets.token_hex(32),
                    max_inbox=max_inbox,
                )
                connection.execute(
                    """
                        INSERT INTO source_broker_v2_store_config (
                            singleton, schema_version, store_id, max_inbox,
                            config_hash, created_at
                        ) VALUES (1, ?, ?, ?, ?, ?)
                        """,
                    (
                        config.schema_version,
                        config.store_id,
                        config.max_inbox,
                        config.config_hash,
                        _encode_time(datetime.now(UTC)),
                    ),
                )
            elif len(config_rows) == 1:
                config = source_broker_v2_job_store_config_from_row(config_rows[0])
                if config.max_inbox != max_inbox:
                    raise SourceBrokerV2StoreConfigError(
                        "runner max_inbox conflicts with immutable store config"
                    )
            else:
                raise SourceBrokerV2StoreConfigError(
                    "runner store has multiple configuration authorities"
                )
            connection.execute(
                """
                    CREATE INDEX IF NOT EXISTS source_broker_v2_jobs_runnable
                    ON source_broker_v2_jobs (state, created_at, operation_id)
                    """
            )
            connection.execute(
                """
                    CREATE TRIGGER IF NOT EXISTS source_broker_v2_store_config_no_update
                    BEFORE UPDATE ON source_broker_v2_store_config
                    BEGIN
                        SELECT RAISE(ABORT, 'source broker v2 store config is immutable');
                    END
                    """
            )
            connection.execute(
                """
                    CREATE TRIGGER IF NOT EXISTS source_broker_v2_store_config_no_delete
                    BEFORE DELETE ON source_broker_v2_store_config
                    BEGIN
                        SELECT RAISE(ABORT, 'source broker v2 store config is immutable');
                    END
                    """
            )
            connection.execute(
                """
                    CREATE TRIGGER IF NOT EXISTS source_broker_v2_published_immutable
                    BEFORE UPDATE ON source_broker_v2_jobs
                    WHEN OLD.state = 'PUBLISHED'
                    BEGIN
                        SELECT RAISE(ABORT, 'source broker v2 published row is immutable');
                    END
                    """
            )
            connection.execute(
                """
                    CREATE TRIGGER IF NOT EXISTS source_broker_v2_published_no_delete
                    BEFORE DELETE ON source_broker_v2_jobs
                    WHEN OLD.state = 'PUBLISHED'
                    BEGIN
                        SELECT RAISE(ABORT, 'source broker v2 published row is immutable');
                    END
                    """
            )
            connection.execute(
                """
                    CREATE INDEX IF NOT EXISTS source_broker_v2_jobs_expiry_ordered
                    ON source_broker_v2_jobs (lease_expires_at, state, operation_id)
                    WHERE state IN ('CLAIMED', 'DISPATCHING')
                    """
            )
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        else:
            connection.execute("COMMIT")


def source_broker_v2_job_store_config_from_row(
    row: Mapping[str, object] | sqlite3.Row,
) -> SourceBrokerV2JobStoreConfig:
    try:
        if int(row["singleton"]) != 1:
            raise ValueError("store config singleton is invalid")
        return SourceBrokerV2JobStoreConfig(
            schema_version=int(row["schema_version"]),
            store_id=str(row["store_id"]),
            max_inbox=int(row["max_inbox"]),
            config_hash=str(row["config_hash"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceBrokerV2StoreConfigError(
            "runner store configuration is missing or malformed"
        ) from exc


def load_source_broker_v2_job_store_config(
    connection: sqlite3.Connection,
) -> SourceBrokerV2JobStoreConfig:
    try:
        rows = connection.execute(
            "SELECT * FROM source_broker_v2_store_config ORDER BY singleton"
        ).fetchall()
    except sqlite3.Error as exc:
        raise SourceBrokerV2StoreConfigError(
            "runner store has not been initialized by an executor"
        ) from exc
    if len(rows) != 1:
        raise SourceBrokerV2StoreConfigError(
            "runner store must contain exactly one configuration authority"
        )
    return source_broker_v2_job_store_config_from_row(rows[0])


class SourceBrokerV2RegistryProfile(StrEnum):
    PRODUCTION = "production"
    NONPRODUCTION_TEST = "nonproduction-test"


class SourceBrokerV2ReconcileCode(StrEnum):
    DEADLINE_EXCEEDED = "deadline_exceeded"
    EXTERNAL_RECONCILE_REQUIRED = "external_reconcile_required"
    INTEGRITY_REJECTED = "integrity_rejected"
    INTERNAL_ERROR = "internal_error"
    LEASE_EXPIRED = "lease_expired"


class _RunnerModel(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceBrokerV2JobRunnerConfig(_RunnerModel):
    owner_id: str = Field(pattern=_OWNER_PATTERN, min_length=1, max_length=200)
    lease_seconds: float = Field(default=30.0, gt=0, le=3600)
    total_deadline_seconds: float = Field(default=20.0, gt=0, le=3600)
    takeover_grace_seconds: float = Field(default=5.0, ge=0, le=300)
    busy_timeout_ms: int = Field(default=5_000, ge=1, le=120_000)
    max_batch: int = Field(default=32, ge=1, le=1_000)
    max_inbox: int = Field(default=1_000, ge=1, le=100_000)

    @model_validator(mode="after")
    def validate_lease(self) -> SourceBrokerV2JobRunnerConfig:
        required = self.total_deadline_seconds + self.takeover_grace_seconds
        if self.lease_seconds < required:
            raise ValueError("lease_seconds must cover the total deadline and takeover grace")
        return self


class SourceBrokerV2CredentialRoot(_RunnerModel):
    path: Path
    owner_uid: int = Field(strict=True, ge=0)
    owner_gid: int = Field(strict=True, ge=0)
    directory_modes: tuple[int, ...] = (0o700,)
    file_mode: int = Field(default=0o600, strict=True)

    @model_validator(mode="after")
    def validate_root(self) -> SourceBrokerV2CredentialRoot:
        _require_canonical_absolute_path(self.path, label="credential root")
        if (
            not self.directory_modes
            or len(set(self.directory_modes)) != len(self.directory_modes)
            or any(mode not in {0o700, 0o750} for mode in self.directory_modes)
        ):
            raise ValueError("credential root directory modes are invalid")
        if self.file_mode != 0o600:
            raise ValueError("credential file mode must be 0600")
        return self


class SourceBrokerV2CredentialPolicy(_RunnerModel):
    """Per-source allowlist; credential values never enter durable job metadata."""

    allowed_env: tuple[str, ...] = ()
    allowed_files: tuple[Path, ...] = ()
    trusted_file_roots: tuple[SourceBrokerV2CredentialRoot, ...] = ()

    @model_validator(mode="after")
    def validate_allowlists(self) -> SourceBrokerV2CredentialPolicy:
        if len(set(self.allowed_env)) != len(self.allowed_env):
            raise ValueError("credential environment allowlist contains duplicates")
        for name in self.allowed_env:
            if not re.fullmatch(_ENV_PATTERN, name):
                raise ValueError("credential environment allowlist entry is invalid")
        normalized_files = tuple(Path(path) for path in self.allowed_files)
        for path in normalized_files:
            _require_canonical_absolute_path(path, label="credential file allowlist entry")
        if len(set(normalized_files)) != len(normalized_files):
            raise ValueError("credential file allowlist contains duplicates")
        root_paths = tuple(root.path for root in self.trusted_file_roots)
        if len(set(root_paths)) != len(root_paths):
            raise ValueError("credential trusted root allowlist contains duplicates")
        for path in normalized_files:
            matching_roots = [
                root for root in self.trusted_file_roots if _is_strict_descendant(path, root.path)
            ]
            if len(matching_roots) != 1:
                raise ValueError("credential file must belong to exactly one trusted root")
        object.__setattr__(self, "allowed_files", normalized_files)
        return self


class SourceBrokerV2CredentialReader:
    """Narrow credential accessor constructed from one source's explicit allowlist."""

    def __init__(self, policy: SourceBrokerV2CredentialPolicy) -> None:
        if type(policy) is not SourceBrokerV2CredentialPolicy:
            raise TypeError("credential reader requires an exact credential policy")
        self._allowed_env = frozenset(policy.allowed_env)
        self._allowed_files = frozenset(policy.allowed_files)
        self._root_by_file = {
            path: next(
                root for root in policy.trusted_file_roots if _is_strict_descendant(path, root.path)
            )
            for path in policy.allowed_files
        }

    def env(self, name: str) -> str:
        if name not in self._allowed_env:
            raise PermissionError(f"credential environment variable {name!r} is not allowlisted")
        value = os.getenv(name)
        if not value:
            raise SourceBrokerV2RunnerError(
                f"credential environment variable {name} is unavailable"
            )
        return value

    def file(self, path: Path) -> str:
        requested = Path(path)
        if requested not in self._allowed_files:
            raise PermissionError(f"credential file {requested!s} is not in the allowlist")
        return _read_secure_credential_file(
            root=self._root_by_file[requested],
            requested=requested,
        )


class _NativeAuthorityClient(Protocol):
    def observe(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        authority: SourceBrokerV2AuthorityRef,
        subject_hash: str,
        deadline: float,
    ) -> SourceBrokerV2NativeEvidence: ...

    def verify(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        authority: SourceBrokerV2AuthorityRef,
        subject_hash: str,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class _ProductionAuthorityEvidence:
    scheduler_binding_hash: str
    source_authority: SourceBrokerV2AuthorityRef
    claim_authority: SourceBrokerV2AuthorityRef
    quota_authority: SourceBrokerV2AuthorityRef
    lineage_authority: SourceBrokerV2AuthorityRef
    external_root_hash: str

    def require_intent(self, intent: SourceBrokerV2JobIntentEnvelope) -> None:
        if (
            intent.source_authority != self.source_authority
            or intent.claim.authority != self.claim_authority
            or intent.quota.authority != self.quota_authority
            or intent.lineage.authority != self.lineage_authority
            or intent.fence.external_root_hash != self.external_root_hash
        ):
            raise ValueError(
                "job authority, key, purpose, schema, generation, fence, or root "
                "conflicts with the production authority graph"
            )


@dataclass(frozen=True, slots=True)
class _ProductionRequestLiveProof:
    evidence: _ProductionAuthorityEvidence
    runtime_identity: int
    clients_identity: int
    component_identity: tuple[int, int, int, int, int, int]
    deadline: float

    def require_graph(self, graph: _ProductionRunnerGraph) -> None:
        graph.require_in_memory()
        if (
            id(graph.runtime) != self.runtime_identity
            or id(graph.scheduler_clients) != self.clients_identity
            or graph.component_identity != self.component_identity
            or graph.evidence != self.evidence
        ):
            raise SourceBrokerV2RunnerError(
                "request live proof conflicts with the production authority graph"
            )


@dataclass(frozen=True, slots=True)
class _ProductionRunnerGraph:
    runtime: SourceBrokerV2AuthorityRuntime
    scheduler_clients: SourceBrokerV2SchedulerClients
    evidence: _ProductionAuthorityEvidence
    component_identity: tuple[int, int, int, int, int, int]
    runtime_identity: int
    clients_identity: int

    @classmethod
    def compose(
        cls,
        *,
        runtime: SourceBrokerV2AuthorityRuntime,
        scheduler_clients: SourceBrokerV2SchedulerClients,
    ) -> _ProductionRunnerGraph:
        evidence = _validated_production_authority_evidence(
            runtime,
            scheduler_clients,
            deadline=None,
            validate_filesystem=True,
        )
        return cls(
            runtime=runtime,
            scheduler_clients=scheduler_clients,
            evidence=evidence,
            component_identity=_scheduler_client_graph_identity(scheduler_clients),
            runtime_identity=id(runtime),
            clients_identity=id(scheduler_clients),
        )

    def require_in_memory(self) -> _ProductionAuthorityEvidence:
        if (
            type(self.runtime) is not SourceBrokerV2AuthorityRuntime
            or type(self.scheduler_clients) is not SourceBrokerV2SchedulerClients
            or id(self.runtime) != self.runtime_identity
            or id(self.scheduler_clients) != self.clients_identity
            or _scheduler_client_graph_identity(self.scheduler_clients) != self.component_identity
        ):
            raise TypeError("production runtime or scheduler client graph was replaced")
        _require_production_scheduler_clients_in_memory(self.runtime, self.scheduler_clients)
        current = _derive_production_authority_evidence(self.runtime, self.scheduler_clients)
        if current != self.evidence:
            raise SourceBrokerV2RunnerError(
                "production authority or trusted root evidence changed after composition"
            )
        return current

    def require_live(self) -> _ProductionAuthorityEvidence:
        self.require_in_memory()
        current = _validated_production_authority_evidence(
            self.runtime,
            self.scheduler_clients,
            deadline=None,
            validate_filesystem=True,
        )
        self.require_in_memory()
        if current != self.evidence:
            raise SourceBrokerV2RunnerError(
                "production authority or trusted root evidence changed after composition"
            )
        return current

    def request_live_proof(self, *, deadline: float) -> _ProductionRequestLiveProof:
        _require_deadline(deadline, "production authority preflight")
        self.require_in_memory()
        current = _validated_production_authority_evidence(
            self.runtime,
            self.scheduler_clients,
            deadline=deadline,
            validate_filesystem=False,
        )
        _require_deadline(deadline, "production authority preflight")
        self.require_in_memory()
        if current != self.evidence:
            raise SourceBrokerV2RunnerError(
                "production authority or trusted root evidence changed during request"
            )
        return _ProductionRequestLiveProof(
            evidence=current,
            runtime_identity=self.runtime_identity,
            clients_identity=self.clients_identity,
            component_identity=self.component_identity,
            deadline=deadline,
        )

    def require_intent(self, intent: SourceBrokerV2JobIntentEnvelope) -> None:
        self.require_live().require_intent(intent)


class SourceBrokerV2StrictNativeEvidenceVerifier:
    """Strict facade over the source keyring and three native authority clients."""

    def __init__(
        self,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        raise TypeError("native verifier requires an explicit profile factory")

    def _configure(
        self,
        *,
        profile: SourceBrokerV2RegistryProfile,
        source_keyring: SourceAuthorityKeyring,
        claim_client: _NativeAuthorityClient | SourceBrokerV2CurrentClaimUnixClient,
        quota_client: _NativeAuthorityClient | SourceBrokerV2SourceQuotaUnixClient,
        lineage_client: _NativeAuthorityClient | SourceBrokerV2ReplayLineageUnixClient,
        production_graph: _ProductionRunnerGraph | None,
    ) -> None:
        if type(source_keyring) is not SourceAuthorityKeyring:
            raise TypeError("source verifier requires the exact native source keyring")
        if profile is SourceBrokerV2RegistryProfile.PRODUCTION:
            expected_types = (
                SourceBrokerV2CurrentClaimUnixClient,
                SourceBrokerV2SourceQuotaUnixClient,
                SourceBrokerV2ReplayLineageUnixClient,
            )
            if (
                tuple(type(client) for client in (claim_client, quota_client, lineage_client))
                != expected_types
                or type(production_graph) is not _ProductionRunnerGraph
            ):
                raise TypeError(
                    "production verifier requires concrete authenticated Unix clients "
                    "and an internally composed authority graph"
                )
        else:
            for label, client in (
                ("claim", claim_client),
                ("quota", quota_client),
                ("lineage", lineage_client),
            ):
                if not callable(getattr(client, "observe", None)) or not callable(
                    getattr(client, "verify", None)
                ):
                    raise TypeError(f"nonproduction {label} authority client is invalid")
            if production_graph is not None:
                raise TypeError("nonproduction verifier cannot carry a production graph")
        self._profile = profile
        self._source_keyring = source_keyring
        self._claim_client = claim_client
        self._quota_client = quota_client
        self._lineage_client = lineage_client
        self._production_graph = production_graph
        self._verified_source_receipts: set[tuple[str, str, str]] = set()

    @classmethod
    def for_production(
        cls,
        *,
        runtime: SourceBrokerV2AuthorityRuntime,
        scheduler_clients: SourceBrokerV2SchedulerClients,
    ) -> SourceBrokerV2StrictNativeEvidenceVerifier:
        graph = _ProductionRunnerGraph.compose(
            runtime=runtime,
            scheduler_clients=scheduler_clients,
        )
        return cls._for_production_graph(graph)

    @classmethod
    def _for_production_graph(
        cls,
        graph: _ProductionRunnerGraph,
    ) -> SourceBrokerV2StrictNativeEvidenceVerifier:
        graph.require_live()
        scheduler_clients = graph.scheduler_clients
        instance = object.__new__(cls)
        instance._configure(
            profile=SourceBrokerV2RegistryProfile.PRODUCTION,
            source_keyring=scheduler_clients.source_authority_keyring,
            claim_client=scheduler_clients.current_claim,
            quota_client=scheduler_clients.source_quota,
            lineage_client=scheduler_clients.replay_lineage,
            production_graph=graph,
        )
        return instance

    @classmethod
    def for_nonproduction_test(
        cls,
        *,
        source_keyring: SourceAuthorityKeyring,
        claim_client: _NativeAuthorityClient,
        quota_client: _NativeAuthorityClient,
        lineage_client: _NativeAuthorityClient,
    ) -> SourceBrokerV2StrictNativeEvidenceVerifier:
        instance = object.__new__(cls)
        instance._configure(
            profile=SourceBrokerV2RegistryProfile.NONPRODUCTION_TEST,
            source_keyring=source_keyring,
            claim_client=claim_client,
            quota_client=quota_client,
            lineage_client=lineage_client,
            production_graph=None,
        )
        return instance

    def require_live(self) -> _ProductionRunnerGraph | None:
        profile = getattr(self, "_profile", None)
        if profile is SourceBrokerV2RegistryProfile.PRODUCTION:
            graph = getattr(self, "_production_graph", None)
            if type(graph) is not _ProductionRunnerGraph:
                raise SourceBrokerV2RunnerError(
                    "production verifier lacks its original authority graph"
                )
            graph.require_live()
            return self._require_production_in_memory()
        if profile is not SourceBrokerV2RegistryProfile.NONPRODUCTION_TEST:
            raise SourceBrokerV2RunnerError("native verifier profile is invalid")
        if getattr(self, "_production_graph", None) is not None:
            raise SourceBrokerV2RunnerError(
                "nonproduction verifier cannot carry a production graph"
            )
        return None

    def _require_production_in_memory(
        self,
        *,
        proof: _ProductionRequestLiveProof | None = None,
    ) -> _ProductionRunnerGraph:
        if getattr(self, "_profile", None) is not SourceBrokerV2RegistryProfile.PRODUCTION:
            raise SourceBrokerV2RunnerError("native verifier is not production-bound")
        graph = getattr(self, "_production_graph", None)
        if type(graph) is not _ProductionRunnerGraph:
            raise SourceBrokerV2RunnerError(
                "production verifier lacks its original authority graph"
            )
        graph.require_in_memory()
        clients = graph.scheduler_clients
        if (
            self._source_keyring is not clients.source_authority_keyring
            or self._claim_client is not clients.current_claim
            or self._quota_client is not clients.source_quota
            or self._lineage_client is not clients.replay_lineage
        ):
            raise SourceBrokerV2RunnerError("production verifier clients changed after composition")
        if proof is not None:
            proof.require_graph(graph)
        return graph

    def observe_claim(
        self, *, intent: SourceBrokerV2JobIntentEnvelope, deadline: float
    ) -> SourceBrokerV2NativeEvidence:
        return self._observe(
            client=self._claim_client,
            kind="claim",
            intent=intent,
            authority=intent.claim.authority,
            subject_hash=_claim_subject_hash(intent),
            deadline=deadline,
        )

    def observe_quota(
        self, *, intent: SourceBrokerV2JobIntentEnvelope, deadline: float
    ) -> SourceBrokerV2NativeEvidence:
        return self._observe(
            client=self._quota_client,
            kind="quota",
            intent=intent,
            authority=intent.quota.authority,
            subject_hash=_quota_subject_hash(intent),
            deadline=deadline,
        )

    def observe_lineage(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        source_receipt_hash: str,
        claim_receipt_hash: str,
        quota_receipt_hash: str,
        deadline: float,
    ) -> SourceBrokerV2NativeEvidence:
        return self._observe(
            client=self._lineage_client,
            kind="lineage",
            intent=intent,
            authority=intent.lineage.authority,
            subject_hash=_lineage_subject_hash(
                intent,
                source_receipt_hash=source_receipt_hash,
                claim_receipt_hash=claim_receipt_hash,
                quota_receipt_hash=quota_receipt_hash,
            ),
            deadline=deadline,
        )

    def verify_source(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        response: bytes,
        status: SourceBrokerV2JobOutcomeStatus,
        deadline: float,
    ) -> None:
        _require_deadline(deadline, "source evidence verification")
        if evidence.kind != "source":
            raise ValueError("source native evidence kind is invalid")
        request_json = evidence.request_json
        contract = request_json.get("contract")
        if contract == "rquant-source-broker-replay/v2":
            request = _parse_native(SourceBrokerV2ReplayRequest, evidence.request)
            receipt = _parse_native(SourceBrokerV2ReplayResponse, evidence.receipt)
            self.verify_replay(
                intent=intent,
                request=request,
                receipt=receipt,
                deadline=deadline,
            )
            if receipt.status is not SourceBrokerV2ReplayStatus.FOUND or receipt.result is None:
                raise ValueError("source replay evidence is not terminal")
            result = receipt.result
        elif contract == "rquant-source-broker-claim-once/v2":
            request = _parse_native(SourceBrokerV2ClaimOnceRequest, evidence.request)
            receipt = _parse_native(SourceBrokerV2ClaimOnceResponse, evidence.receipt)
            self.verify_claim_once(
                intent=intent,
                request=request,
                receipt=receipt,
                deadline=deadline,
            )
            if (
                receipt.status
                not in {
                    SourceBrokerV2ClaimStatus.SUCCESS,
                    SourceBrokerV2ClaimStatus.FAILURE,
                }
                or receipt.result is None
            ):
                raise ValueError("source claim evidence is not terminal")
            result = receipt.result
        else:
            raise ValueError("source native evidence request contract is invalid")
        dispatch = _parse_native(SourceBrokerV2DispatchResponse, result)
        expected_request = _dispatch_request(intent)
        expected_status = SourceBrokerV2JobOutcomeStatus(dispatch.outcome.value)
        if (
            request.saga_id != intent.claim.saga_id
            or request.operation_id != intent.operation_id
            or request.phase is not SourceBrokerV2OutboxPhase.DISPATCH
            or request.operation_request_hash != expected_request.request_hash
            or dispatch.saga_id != intent.claim.saga_id
            or dispatch.operation_id != intent.operation_id
            or dispatch.call_id != intent.quota.parent_id
            or dispatch.request_hash != expected_request.request_hash
            or dispatch.response != response
            or expected_status is not status
        ):
            raise ValueError("source native evidence is not bound to the exact job operation")
        _require_deadline(deadline, "source evidence verification")

    def verify_claim(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None:
        self._verify_external(
            client=self._claim_client,
            kind="claim",
            intent=intent,
            authority=intent.claim.authority,
            subject_hash=_claim_subject_hash(intent),
            evidence=evidence,
            deadline=deadline,
        )

    def verify_quota(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None:
        self._verify_external(
            client=self._quota_client,
            kind="quota",
            intent=intent,
            authority=intent.quota.authority,
            subject_hash=_quota_subject_hash(intent),
            evidence=evidence,
            deadline=deadline,
        )

    def verify_lineage(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        source_receipt_hash: str,
        claim_receipt_hash: str,
        quota_receipt_hash: str,
        deadline: float,
    ) -> None:
        self._verify_external(
            client=self._lineage_client,
            kind="lineage",
            intent=intent,
            authority=intent.lineage.authority,
            subject_hash=_lineage_subject_hash(
                intent,
                source_receipt_hash=source_receipt_hash,
                claim_receipt_hash=claim_receipt_hash,
                quota_receipt_hash=quota_receipt_hash,
            ),
            evidence=evidence,
            deadline=deadline,
        )

    def verify_replay(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        request: SourceBrokerV2ReplayRequest,
        receipt: SourceBrokerV2ReplayResponse,
        deadline: float,
    ) -> None:
        _require_deadline(deadline, "source replay verification")
        self._require_source_authority(intent, receipt)
        cache_key = (
            "replay",
            canonical_job_sha256(canonical_model_json_bytes(request)),
            canonical_job_sha256(canonical_model_json_bytes(receipt)),
        )
        if cache_key not in self._verified_source_receipts:
            self._source_keyring.require_verified_replay(request=request, receipt=receipt)
            self._verified_source_receipts.add(cache_key)
        _require_deadline(deadline, "source replay verification")

    def verify_claim_once(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        request: SourceBrokerV2ClaimOnceRequest,
        receipt: SourceBrokerV2ClaimOnceResponse,
        deadline: float,
    ) -> None:
        _require_deadline(deadline, "source claim verification")
        self._require_source_authority(intent, receipt)
        cache_key = (
            "claim",
            canonical_job_sha256(canonical_model_json_bytes(request)),
            canonical_job_sha256(canonical_model_json_bytes(receipt)),
        )
        if cache_key not in self._verified_source_receipts:
            self._source_keyring.require_verified_claim(request=request, receipt=receipt)
            self._verified_source_receipts.add(cache_key)
        _require_deadline(deadline, "source claim verification")

    def _observe(
        self,
        *,
        client: _NativeAuthorityClient,
        kind: str,
        intent: SourceBrokerV2JobIntentEnvelope,
        authority: SourceBrokerV2AuthorityRef,
        subject_hash: str,
        deadline: float,
    ) -> SourceBrokerV2NativeEvidence:
        self._require_nonproduction_native_contract(intent, kind=kind)
        _require_deadline(deadline, f"{kind} authority observation")
        assert hasattr(client, "observe")
        evidence = client.observe(
            intent=intent,
            authority=authority,
            subject_hash=subject_hash,
            deadline=deadline,
        )
        self._verify_external(
            client=client,
            kind=kind,
            intent=intent,
            authority=authority,
            subject_hash=subject_hash,
            evidence=evidence,
            deadline=deadline,
        )
        return evidence

    def _verify_external(
        self,
        *,
        client: _NativeAuthorityClient,
        kind: str,
        intent: SourceBrokerV2JobIntentEnvelope,
        authority: SourceBrokerV2AuthorityRef,
        subject_hash: str,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None:
        self._require_nonproduction_native_contract(intent, kind=kind)
        _require_deadline(deadline, f"{kind} authority verification")
        if evidence.kind != kind:
            raise ValueError(f"{kind} native evidence kind is invalid")
        assert hasattr(client, "verify")
        client.verify(
            intent=intent,
            authority=authority,
            subject_hash=subject_hash,
            evidence=evidence,
            deadline=deadline,
        )
        _require_deadline(deadline, f"{kind} authority verification")

    def _require_source_authority(
        self,
        intent: SourceBrokerV2JobIntentEnvelope,
        receipt: SourceBrokerV2ReplayResponse | SourceBrokerV2ClaimOnceResponse,
    ) -> None:
        if self._profile is SourceBrokerV2RegistryProfile.PRODUCTION:
            if self._production_graph is None:
                raise SourceBrokerV2RunnerError("production authority graph is unavailable")
            self._production_graph.require_intent(intent)
        authority = intent.source_authority
        if (
            authority.authority_id != self._source_keyring.expected_authority_id
            or authority.purpose != self._source_keyring.expected_purpose
            or authority.schema_version != self._source_keyring.expected_schema_version
            or authority.key_id != receipt.key_id
            or authority.authority_id != receipt.authority_id
            or (
                self._profile is SourceBrokerV2RegistryProfile.NONPRODUCTION_TEST
                and authority.fence_hash != intent.fence.external_root_hash
            )
        ):
            raise ValueError("source authority identity, key, purpose, schema, or fence is invalid")

    def _require_nonproduction_native_contract(
        self,
        intent: SourceBrokerV2JobIntentEnvelope,
        *,
        kind: str,
    ) -> None:
        if self._profile is SourceBrokerV2RegistryProfile.PRODUCTION:
            if self._production_graph is None:
                raise SourceBrokerV2RunnerError("production authority graph is unavailable")
            self._production_graph.require_intent(intent)
            raise SourceBrokerV2RunnerError(
                f"production {kind} authority requires the concrete Saga native "
                "binding; generic observe/verify evidence is forbidden"
            )


@dataclass(frozen=True, slots=True)
class SourceBrokerV2ProviderBinding:
    transport: SourceBrokerV2Transport
    verifier: SourceBrokerV2StrictNativeEvidenceVerifier
    _production_graph: _ProductionRunnerGraph | None = None
    _original_transport: object | None = None
    _original_verifier: SourceBrokerV2StrictNativeEvidenceVerifier | None = None

    @classmethod
    def _for_production(
        cls,
        *,
        graph: _ProductionRunnerGraph,
        verifier: SourceBrokerV2StrictNativeEvidenceVerifier,
    ) -> SourceBrokerV2ProviderBinding:
        graph.require_live()
        if verifier.require_live() is not graph:
            raise SourceBrokerV2RunnerError(
                "production binding verifier uses a different authority graph"
            )
        transport = graph.scheduler_clients.source_client
        return cls(
            transport=transport,
            verifier=verifier,
            _production_graph=graph,
            _original_transport=transport,
            _original_verifier=verifier,
        )

    def require_live(self) -> None:
        if self._production_graph is None:
            if self._original_transport is not None or self._original_verifier is not None:
                raise SourceBrokerV2RunnerError(
                    "nonproduction binding carries incomplete production state"
                )
            if self.verifier.require_live() is not None:
                raise SourceBrokerV2RunnerError(
                    "nonproduction binding cannot use a production verifier"
                )
            return
        graph = self._production_graph
        graph.require_live()
        self._require_production_in_memory()

    def _require_production_in_memory(
        self,
        *,
        proof: _ProductionRequestLiveProof | None = None,
    ) -> None:
        graph = self._production_graph
        if type(graph) is not _ProductionRunnerGraph:
            raise SourceBrokerV2RunnerError("production binding lacks its original authority graph")
        graph.require_in_memory()
        clients = graph.scheduler_clients
        if (
            type(self.transport) is not SourceBrokerV2UnixClient
            or self.transport is not self._original_transport
            or self.transport is not clients.source_client
            or type(self.verifier) is not SourceBrokerV2StrictNativeEvidenceVerifier
            or self.verifier is not self._original_verifier
            or self.verifier._require_production_in_memory(proof=proof) is not graph
        ):
            raise SourceBrokerV2RunnerError(
                "production binding transport or verifier changed after composition"
            )
        if proof is not None:
            proof.require_graph(graph)


ProviderFactory = Callable[[SourceBrokerV2CredentialReader], SourceBrokerV2ProviderBinding]


class SourceBrokerV2ProviderRegistration:
    __slots__ = (
        "_binding",
        "_clients_identity",
        "_factory",
        "_original_binding",
        "_original_transport",
        "_original_verifier",
        "_production_graph",
        "_runtime_identity",
        "_scheduler_clients",
        "_runtime",
        "credential_policy",
        "profile",
    )

    def __init__(
        self,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        raise TypeError("provider registration requires an explicit profile factory")

    def _configure(
        self,
        *,
        profile: SourceBrokerV2RegistryProfile,
        credential_policy: SourceBrokerV2CredentialPolicy,
        factory: ProviderFactory | None,
        binding: SourceBrokerV2ProviderBinding | None,
    ) -> None:
        if type(credential_policy) is not SourceBrokerV2CredentialPolicy:
            raise TypeError("provider registration requires an exact credential policy")
        if profile is SourceBrokerV2RegistryProfile.PRODUCTION:
            if factory is not None or type(binding) is not SourceBrokerV2ProviderBinding:
                raise TypeError("production registration cannot use a provider factory")
            if (
                type(binding.transport) is not SourceBrokerV2UnixClient
                or type(binding.verifier) is not SourceBrokerV2StrictNativeEvidenceVerifier
                or binding.verifier._profile is not SourceBrokerV2RegistryProfile.PRODUCTION
                or credential_policy.allowed_env
                or credential_policy.allowed_files
            ):
                raise TypeError(
                    "production registration requires exact composed Unix clients "
                    "without local credentials"
                )
        elif not callable(factory) or binding is not None:
            raise TypeError("nonproduction registration requires a test factory")
        self.profile = profile
        self.credential_policy = credential_policy
        self._factory = factory
        self._binding = binding

    @classmethod
    def for_production(
        cls,
        *,
        runtime: SourceBrokerV2AuthorityRuntime,
        scheduler_clients: SourceBrokerV2SchedulerClients,
    ) -> SourceBrokerV2ProviderRegistration:
        graph = _ProductionRunnerGraph.compose(
            runtime=runtime,
            scheduler_clients=scheduler_clients,
        )
        verifier = SourceBrokerV2StrictNativeEvidenceVerifier._for_production_graph(graph)
        binding = SourceBrokerV2ProviderBinding._for_production(
            graph=graph,
            verifier=verifier,
        )
        instance = object.__new__(cls)
        instance._configure(
            profile=SourceBrokerV2RegistryProfile.PRODUCTION,
            credential_policy=SourceBrokerV2CredentialPolicy(),
            factory=None,
            binding=binding,
        )
        instance._runtime = runtime
        instance._scheduler_clients = scheduler_clients
        instance._production_graph = graph
        instance._original_binding = binding
        instance._original_transport = binding.transport
        instance._original_verifier = verifier
        instance._runtime_identity = id(runtime)
        instance._clients_identity = id(scheduler_clients)
        return instance

    @classmethod
    def for_nonproduction_test(
        cls,
        *,
        factory: ProviderFactory,
        credential_policy: SourceBrokerV2CredentialPolicy,
    ) -> SourceBrokerV2ProviderRegistration:
        instance = object.__new__(cls)
        instance._configure(
            profile=SourceBrokerV2RegistryProfile.NONPRODUCTION_TEST,
            credential_policy=credential_policy,
            factory=factory,
            binding=None,
        )
        instance._runtime = None
        instance._scheduler_clients = None
        instance._production_graph = None
        instance._original_binding = None
        instance._original_transport = None
        instance._original_verifier = None
        instance._runtime_identity = 0
        instance._clients_identity = 0
        return instance

    def require_live(
        self,
        *,
        runtime: SourceBrokerV2AuthorityRuntime | None = None,
        scheduler_clients: SourceBrokerV2SchedulerClients | None = None,
    ) -> SourceBrokerV2ProviderBinding | None:
        if type(self) is not SourceBrokerV2ProviderRegistration:
            raise SourceBrokerV2RunnerError("provider registration type is invalid")
        profile = getattr(self, "profile", None)
        if profile is SourceBrokerV2RegistryProfile.PRODUCTION:
            binding = self._require_production_in_memory(
                runtime=runtime,
                scheduler_clients=scheduler_clients,
            )
            graph = self._production_graph
            assert type(graph) is _ProductionRunnerGraph
            graph.require_live()
            self._require_production_in_memory(
                runtime=runtime,
                scheduler_clients=scheduler_clients,
            )
            return binding
        if profile is not SourceBrokerV2RegistryProfile.NONPRODUCTION_TEST:
            raise SourceBrokerV2RunnerError("provider registration profile is invalid")
        if (
            runtime is not None
            or scheduler_clients is not None
            or not callable(getattr(self, "_factory", None))
            or getattr(self, "_binding", None) is not None
        ):
            raise SourceBrokerV2RunnerError(
                "nonproduction registration cannot use production scheduler state"
            )
        return None

    def _require_production_in_memory(
        self,
        *,
        runtime: SourceBrokerV2AuthorityRuntime | None = None,
        scheduler_clients: SourceBrokerV2SchedulerClients | None = None,
        proof: _ProductionRequestLiveProof | None = None,
    ) -> SourceBrokerV2ProviderBinding:
        if (
            type(self) is not SourceBrokerV2ProviderRegistration
            or getattr(self, "profile", None) is not SourceBrokerV2RegistryProfile.PRODUCTION
        ):
            raise SourceBrokerV2RunnerError("provider registration is not production-bound")
        original_runtime = getattr(self, "_runtime", None)
        original_clients = getattr(self, "_scheduler_clients", None)
        graph = getattr(self, "_production_graph", None)
        binding = getattr(self, "_binding", None)
        original_binding = getattr(self, "_original_binding", None)
        original_transport = getattr(self, "_original_transport", None)
        original_verifier = getattr(self, "_original_verifier", None)
        if (
            type(original_runtime) is not SourceBrokerV2AuthorityRuntime
            or type(original_clients) is not SourceBrokerV2SchedulerClients
            or type(graph) is not _ProductionRunnerGraph
            or type(binding) is not SourceBrokerV2ProviderBinding
            or binding is not original_binding
            or id(original_runtime) != getattr(self, "_runtime_identity", None)
            or id(original_clients) != getattr(self, "_clients_identity", None)
            or graph.runtime is not original_runtime
            or graph.scheduler_clients is not original_clients
            or binding.transport is not original_transport
            or binding.verifier is not original_verifier
            or (runtime is not None and runtime is not original_runtime)
            or (scheduler_clients is not None and scheduler_clients is not original_clients)
        ):
            raise SourceBrokerV2RunnerError(
                "production registration graph or binding changed after composition"
            )
        graph.require_in_memory()
        binding._require_production_in_memory(proof=proof)
        if proof is not None:
            proof.require_graph(graph)
        return binding

    def open(self) -> SourceBrokerV2ProviderBinding:
        if self.profile is SourceBrokerV2RegistryProfile.PRODUCTION:
            binding = self.require_live()
            if binding is None:
                raise SourceBrokerV2RunnerError("production provider binding is unavailable")
            return binding
        self.require_live()
        if self._factory is None:
            raise SourceBrokerV2RunnerError("nonproduction provider factory is unavailable")
        binding = self._factory(SourceBrokerV2CredentialReader(self.credential_policy))
        if type(binding) is not SourceBrokerV2ProviderBinding:
            raise SourceBrokerV2RunnerError("provider factory returned an invalid binding")
        if (
            type(binding.verifier) is not SourceBrokerV2StrictNativeEvidenceVerifier
            or binding.verifier._profile is not SourceBrokerV2RegistryProfile.NONPRODUCTION_TEST
        ):
            raise SourceBrokerV2RunnerError(
                "nonproduction provider lacks an explicit test verifier"
            )
        return binding


class SourceBrokerV2StaticProviderRegistry:
    """Closed broker-process registry; factories and credentials are never persisted."""

    __slots__ = (
        "_clients_identity",
        "_original_entries",
        "_original_entry_by_source",
        "_production_graph",
        "_profile",
        "_registrations",
        "_runtime",
        "_runtime_identity",
        "_scheduler_clients",
    )

    def __init__(
        self,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        raise TypeError("provider registry requires an explicit profile factory")

    def _configure(
        self,
        *,
        profile: SourceBrokerV2RegistryProfile,
        registrations: Mapping[str, SourceBrokerV2ProviderRegistration],
    ) -> None:
        if not registrations:
            raise ValueError("provider registry must not be empty")
        normalized: dict[str, SourceBrokerV2ProviderRegistration] = {}
        for source_id, registration in registrations.items():
            if not source_id or type(registration) is not SourceBrokerV2ProviderRegistration:
                raise ValueError("provider registry entry is invalid")
            if registration.profile is not profile:
                raise TypeError(
                    f"{profile.value} registry rejects {registration.profile.value} registration"
                )
            normalized[source_id] = registration
        self._profile = profile
        self._registrations = normalized

    @classmethod
    def for_production(
        cls,
        *,
        runtime: SourceBrokerV2AuthorityRuntime,
        scheduler_clients: SourceBrokerV2SchedulerClients,
        registrations: Mapping[str, SourceBrokerV2ProviderRegistration],
    ) -> SourceBrokerV2StaticProviderRegistry:
        graph = _ProductionRunnerGraph.compose(
            runtime=runtime,
            scheduler_clients=scheduler_clients,
        )
        entries: list[
            tuple[
                str,
                SourceBrokerV2ProviderRegistration,
                SourceBrokerV2ProviderBinding,
                SourceBrokerV2UnixClient,
                SourceBrokerV2StrictNativeEvidenceVerifier,
            ]
        ] = []
        for source_id, registration in registrations.items():
            if type(registration) is not SourceBrokerV2ProviderRegistration:
                raise TypeError("production registry requires exact registrations")
            binding = registration._require_production_in_memory(
                runtime=runtime,
                scheduler_clients=scheduler_clients,
            )
            if (
                binding is None
                or type(binding.transport) is not SourceBrokerV2UnixClient
                or type(binding.verifier) is not SourceBrokerV2StrictNativeEvidenceVerifier
            ):
                raise SourceBrokerV2RunnerError(
                    "production registry registration binding is invalid"
                )
            entries.append(
                (
                    source_id,
                    registration,
                    binding,
                    binding.transport,
                    binding.verifier,
                )
            )
        instance = object.__new__(cls)
        instance._configure(
            profile=SourceBrokerV2RegistryProfile.PRODUCTION,
            registrations=registrations,
        )
        instance._runtime = runtime
        instance._scheduler_clients = scheduler_clients
        instance._production_graph = graph
        instance._runtime_identity = id(runtime)
        instance._clients_identity = id(scheduler_clients)
        instance._original_entries = tuple(entries)
        instance._original_entry_by_source = MappingProxyType(
            {entry[0]: entry for entry in entries}
        )
        instance.require_live()
        return instance

    @classmethod
    def for_nonproduction_test(
        cls,
        registrations: Mapping[str, SourceBrokerV2ProviderRegistration],
    ) -> SourceBrokerV2StaticProviderRegistry:
        instance = object.__new__(cls)
        instance._configure(
            profile=SourceBrokerV2RegistryProfile.NONPRODUCTION_TEST,
            registrations=registrations,
        )
        instance._runtime = None
        instance._scheduler_clients = None
        instance._production_graph = None
        instance._runtime_identity = 0
        instance._clients_identity = 0
        instance._original_entries = ()
        instance._original_entry_by_source = MappingProxyType({})
        return instance

    def require_live(
        self,
        *,
        source_id: str | None = None,
        binding: SourceBrokerV2ProviderBinding | None = None,
    ) -> None:
        if type(self) is not SourceBrokerV2StaticProviderRegistry:
            raise SourceBrokerV2RunnerError("provider registry type is invalid")
        profile = getattr(self, "_profile", None)
        if profile is SourceBrokerV2RegistryProfile.PRODUCTION:
            selected = self._require_production_in_memory(
                source_id=source_id,
                binding=binding,
            )
            graph = self._production_graph
            assert type(graph) is _ProductionRunnerGraph
            graph.require_live()
            if (
                self._require_production_in_memory(
                    source_id=source_id,
                    binding=binding,
                )
                is not selected
            ):
                raise SourceBrokerV2RunnerError(
                    "production registry selection changed during live validation"
                )
            return
        if profile is not SourceBrokerV2RegistryProfile.NONPRODUCTION_TEST:
            raise SourceBrokerV2RunnerError("provider registry profile is invalid")
        if (
            getattr(self, "_runtime", None) is not None
            or getattr(self, "_scheduler_clients", None) is not None
            or getattr(self, "_production_graph", None) is not None
        ):
            raise SourceBrokerV2RunnerError(
                "nonproduction registry cannot use production scheduler state"
            )

    def _require_production_in_memory(
        self,
        *,
        source_id: str | None,
        binding: SourceBrokerV2ProviderBinding | None,
        proof: _ProductionRequestLiveProof | None = None,
    ) -> SourceBrokerV2ProviderBinding | None:
        if (
            type(self) is not SourceBrokerV2StaticProviderRegistry
            or getattr(self, "_profile", None) is not SourceBrokerV2RegistryProfile.PRODUCTION
        ):
            raise SourceBrokerV2RunnerError("provider registry is not production-bound")
        runtime = getattr(self, "_runtime", None)
        clients = getattr(self, "_scheduler_clients", None)
        graph = getattr(self, "_production_graph", None)
        entries = getattr(self, "_original_entries", None)
        entry_by_source = getattr(self, "_original_entry_by_source", None)
        registrations = getattr(self, "_registrations", None)
        if (
            type(runtime) is not SourceBrokerV2AuthorityRuntime
            or type(clients) is not SourceBrokerV2SchedulerClients
            or type(graph) is not _ProductionRunnerGraph
            or type(entries) is not tuple
            or type(entry_by_source) is not _MAPPING_PROXY_TYPE
            or type(registrations) is not dict
            or id(runtime) != getattr(self, "_runtime_identity", None)
            or id(clients) != getattr(self, "_clients_identity", None)
            or graph.runtime is not runtime
            or graph.scheduler_clients is not clients
            or len(entries) != len(registrations)
            or len(entry_by_source) != len(entries)
        ):
            raise SourceBrokerV2RunnerError(
                "production registry lacks its original runtime or client graph"
            )
        graph.require_in_memory()
        if proof is not None:
            proof.require_graph(graph)
        selected_entries = entries if source_id is None else (entry_by_source.get(source_id),)
        if selected_entries == (None,):
            raise SourceBrokerV2RunnerError(f"source {source_id!r} is not in the closed registry")
        selected_binding: SourceBrokerV2ProviderBinding | None = None
        for entry in selected_entries:
            if type(entry) is not tuple or len(entry) != 5:
                raise SourceBrokerV2RunnerError("production registry entry is invalid")
            (
                original_source_id,
                original_registration,
                original_binding,
                original_transport,
                original_verifier,
            ) = entry
            if registrations.get(original_source_id) is not original_registration:
                raise SourceBrokerV2RunnerError(
                    "production registry entry changed after composition"
                )
            current_binding = original_registration._require_production_in_memory(
                runtime=runtime,
                scheduler_clients=clients,
                proof=proof,
            )
            if (
                current_binding is not original_binding
                or original_binding.transport is not original_transport
                or original_binding.verifier is not original_verifier
            ):
                raise SourceBrokerV2RunnerError(
                    "production registry binding changed after composition"
                )
            if original_source_id == source_id:
                selected_binding = original_binding
        if source_id is not None:
            if selected_binding is None:
                raise SourceBrokerV2RunnerError(
                    f"source {source_id!r} is not in the closed registry"
                )
            if binding is not None and binding is not selected_binding:
                raise SourceBrokerV2RunnerError(
                    "runner binding differs from the production registry"
                )
        elif binding is not None:
            raise SourceBrokerV2RunnerError(
                "production registry binding requires an exact source selection"
            )
        return selected_binding

    def _transport_for_request(
        self,
        *,
        source_id: str,
        binding: SourceBrokerV2ProviderBinding,
        deadline: float,
    ) -> SourceBrokerV2UnixClient:
        _require_deadline(deadline, "production request live proof")
        selected = self._require_production_in_memory(
            source_id=source_id,
            binding=binding,
        )
        graph = self._production_graph
        assert type(graph) is _ProductionRunnerGraph
        proof = graph.request_live_proof(deadline=deadline)
        if (
            self._require_production_in_memory(
                source_id=source_id,
                binding=binding,
                proof=proof,
            )
            is not selected
        ):
            raise SourceBrokerV2RunnerError(
                "production registry selection changed during request preflight"
            )
        _require_deadline(deadline, "production source transport")
        if selected is None or type(selected.transport) is not SourceBrokerV2UnixClient:
            raise SourceBrokerV2RunnerError("production source transport is invalid")
        return selected.transport

    @property
    def source_ids(self) -> frozenset[str]:
        return frozenset(self._registrations)

    def open(self, source_id: str) -> SourceBrokerV2ProviderBinding:
        if self._profile is SourceBrokerV2RegistryProfile.PRODUCTION:
            binding = self._require_production_in_memory(
                source_id=source_id,
                binding=None,
            )
            if binding is None:
                raise SourceBrokerV2RunnerError("production provider binding is unavailable")
            return binding
        self.require_live(source_id=source_id)
        try:
            registration = self._registrations[source_id]
        except KeyError as exc:
            raise SourceBrokerV2RunnerError(
                f"source {source_id!r} is not in the closed registry"
            ) from exc
        binding = registration.open()
        self.require_live(source_id=source_id, binding=binding)
        for method in ("claim_once", "dispatch", "replay", "finalize"):
            if not callable(getattr(binding.transport, method, None)):
                raise SourceBrokerV2RunnerError(
                    "registered provider does not implement SourceBrokerV2Transport"
                )
        if type(binding.verifier) is not SourceBrokerV2StrictNativeEvidenceVerifier:
            raise SourceBrokerV2RunnerError("provider lacks the strict native evidence verifier")
        if binding.verifier._profile is not self._profile:
            raise SourceBrokerV2RunnerError("provider verifier profile conflicts with registry")
        return binding


@dataclass(frozen=True, slots=True)
class _Lease:
    operation_id: str
    generation: int


@dataclass(frozen=True, slots=True)
class _ClaimedJob:
    lease: _Lease
    state: SourceBrokerV2JobRunnerState
    intent: SourceBrokerV2JobIntentEnvelope
    stage_authority_hash: str
    stage_record_commitment: str
    claim_receipt: bytes | None
    dispatch_receipt: bytes | None
    source_evidence: bytes | None
    claim_evidence: bytes | None
    quota_evidence: bytes | None
    finalize_receipt: bytes | None


@dataclass(frozen=True, slots=True)
class _MonotonicBudget:
    deadline: float
    wall_deadline: datetime
    clock: Callable[[], datetime]

    def ensure(self, stage: str) -> None:
        if time.monotonic() >= self.deadline:
            raise _RunnerDeadlineError(f"total monotonic deadline exceeded during {stage}")
        if self.clock() >= self.wall_deadline:
            raise _RunnerDeadlineError(f"intent deadline exceeded during {stage}")

    def call(self, stage: str, function: Callable[[], _T]) -> _T:
        self.ensure(stage)
        result = function()
        self.ensure(stage)
        return result


@dataclass(frozen=True, slots=True)
class _ProductionRunnerRegistrySnapshot:
    registry: SourceBrokerV2StaticProviderRegistry
    graph: _ProductionRunnerGraph
    entry_by_source: Mapping[
        str,
        tuple[
            str,
            SourceBrokerV2ProviderRegistration,
            SourceBrokerV2ProviderBinding,
            SourceBrokerV2UnixClient,
            SourceBrokerV2StrictNativeEvidenceVerifier,
        ],
    ]

    @classmethod
    def compose(
        cls,
        registry: SourceBrokerV2StaticProviderRegistry,
    ) -> _ProductionRunnerRegistrySnapshot:
        registry._require_production_in_memory(source_id=None, binding=None)
        graph = registry._production_graph
        entries = registry._original_entry_by_source
        if type(graph) is not _ProductionRunnerGraph or type(entries) is not _MAPPING_PROXY_TYPE:
            raise SourceBrokerV2RunnerError(
                "production runner cannot snapshot the registry authority graph"
            )
        return cls(
            registry=registry,
            graph=graph,
            entry_by_source=MappingProxyType(dict(entries)),
        )

    def require_selected(
        self,
        *,
        registry: SourceBrokerV2StaticProviderRegistry,
        source_id: str,
        binding: SourceBrokerV2ProviderBinding,
    ) -> None:
        expected = self.entry_by_source.get(source_id)
        current_entries = getattr(registry, "_original_entry_by_source", None)
        registrations = getattr(registry, "_registrations", None)
        if (
            registry is not self.registry
            or registry._production_graph is not self.graph
            or type(current_entries) is not _MAPPING_PROXY_TYPE
            or type(registrations) is not dict
            or expected is None
            or current_entries.get(source_id) is not expected
            or registrations.get(source_id) is not expected[1]
            or binding is not expected[2]
            or binding.transport is not expected[3]
            or binding.verifier is not expected[4]
        ):
            raise SourceBrokerV2RunnerError(
                "production runner registry graph, entry, or binding was replaced"
            )
        self.graph.require_in_memory()
        expected[1]._require_production_in_memory(
            runtime=self.graph.runtime,
            scheduler_clients=self.graph.scheduler_clients,
        )


class SourceBrokerV2JobRunner:
    """SQLite state machine whose effects are authorized by external native receipts."""

    def __init__(
        self,
        *,
        db_path: Path,
        registry: SourceBrokerV2StaticProviderRegistry,
        config: SourceBrokerV2JobRunnerConfig,
        manifest_keyring: VerifyOnlyEd25519Keyring,
        authorization_keyring: VerifyOnlyEd25519Keyring,
        stage_store: object,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if type(registry) is not SourceBrokerV2StaticProviderRegistry:
            raise TypeError("runner requires the exact closed provider registry")
        if type(config) is not SourceBrokerV2JobRunnerConfig:
            raise TypeError("runner requires the exact runner configuration")
        if not isinstance(manifest_keyring, VerifyOnlyEd25519Keyring):
            raise TypeError("runner requires a verify-only manifest keyring")
        if not isinstance(authorization_keyring, VerifyOnlyEd25519Keyring):
            raise TypeError("runner requires a verify-only authorization keyring")
        registry.require_live()
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._registry = registry
        self._production_registry_snapshot = (
            _ProductionRunnerRegistrySnapshot.compose(registry)
            if registry._profile is SourceBrokerV2RegistryProfile.PRODUCTION
            else None
        )
        self._config = config
        self._manifest_keyring = manifest_keyring
        self._authorization_keyring = authorization_keyring
        self._stage_store = _require_stage_store(stage_store)
        self._clock = clock or (lambda: datetime.now(UTC))
        process_nonce = secrets.token_hex(32)
        self._executor_owner_token_hash = canonical_job_sha256(
            {
                "nonce": process_nonce,
                "owner_id": config.owner_id,
                "purpose": "rquant-source-broker-v2-runner-executor/v2",
            }
        )
        self._lease_owner_id = f"{config.owner_id[:120]}:{self._executor_owner_token_hash}"
        self._leases: dict[str, int] = {}
        self._wake_event = Event()
        self._stop_event = Event()
        self._closed = False
        self._initialize()

    @property
    def closed(self) -> bool:
        return self._closed

    def enqueue_intent(self, intent: SourceBrokerV2JobIntentEnvelope) -> str:
        return self.enqueue_intent_bytes(canonical_job_model_bytes(intent))

    def enqueue_intent_bytes(self, payload: bytes) -> str:
        self._ensure_open()
        try:
            intent = parse_job_intent(payload)
        except Exception as exc:
            raise SourceBrokerV2RunnerConflictError(
                "job intent bytes are malformed or conflicting"
            ) from exc
        now = self._now()
        try:
            self._require_authorized_intent(intent, now=now)
            stage_authority_hash, stage_record_commitment = self._stage_proof(intent, now=now)
        except Exception as exc:
            raise SourceBrokerV2RunnerConflictError(
                "job intent authorization is invalid or expired"
            ) from exc
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT intent FROM source_broker_v2_jobs WHERE operation_id = ?",
                (intent.operation_id,),
            ).fetchone()
            if existing is not None:
                if bytes(existing["intent"]) == payload:
                    return intent.operation_id
                raise SourceBrokerV2RunnerConflictError(
                    "operation_id is already bound to another intent"
                )
            active = connection.execute(
                """
                SELECT COUNT(*) AS count FROM source_broker_v2_jobs
                WHERE state IN ('NEW', 'CLAIMED', 'DISPATCHING', 'RECONCILE_REQUIRED')
                """
            ).fetchone()
            if active is None or int(active["count"]) >= self._config.max_inbox:
                raise SourceBrokerV2RunnerBackpressureError("runner inbox is full")
            connection.execute(
                """
                INSERT INTO source_broker_v2_jobs (
                    operation_id, intent, intent_hash, source_id, operation_hash, request_hash,
                    deadline_at, stage_authority_hash, stage_record_commitment,
                    state, lease_generation, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'NEW', 0, ?, ?)
                """,
                (
                    intent.operation_id,
                    payload,
                    intent.intent_hash,
                    intent.source_id,
                    intent.operation_hash,
                    intent.request_hash,
                    _encode_time(intent.deadline),
                    stage_authority_hash,
                    stage_record_commitment,
                    _encode_time(now),
                    _encode_time(now),
                ),
            )
        self.wake()
        return intent.operation_id

    def claim_pending(self, *, limit: int | None = None) -> tuple[str, ...]:
        self._ensure_open()
        return tuple(lease.operation_id for lease in self._claim_new(limit=limit))

    def run_once(self) -> int:
        self._ensure_open()
        processed = 0
        for lease in self._claim_new(limit=self._config.max_batch):
            processed += 1
            self._process_new_claim(lease)
        remaining = max(self._config.max_batch - processed, 0)
        if remaining:
            for lease in self._claim_terminal(limit=remaining):
                processed += 1
                self._finish_terminal_claim(lease)
        return processed

    def reconcile_once(self, *, limit: int | None = None) -> int:
        self._ensure_open()
        reconciled = 0
        for lease in self._claim_reconcile(limit=limit or self._config.max_batch):
            reconciled += 1
            self._reconcile_claim(lease)
        return reconciled

    def get_state(self, operation_id: str) -> SourceBrokerV2JobRunnerState:
        self._ensure_open()
        with self._connection() as connection:
            row = connection.execute(
                "SELECT state FROM source_broker_v2_jobs WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
        if row is None:
            raise KeyError(operation_id)
        return SourceBrokerV2JobRunnerState(str(row["state"]))

    def get_outcome(self, operation_id: str) -> SourceBrokerV2JobOutcomeEnvelope:
        self._ensure_open()
        with self._connection() as connection:
            row = connection.execute(
                "SELECT outcome FROM source_broker_v2_jobs WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
        if row is None or row["outcome"] is None:
            raise KeyError(operation_id)
        return parse_job_outcome(bytes(row["outcome"]))

    def mark_dispatching_for_recovery_test(self, operation_id: str) -> None:
        """Create a local ambiguous state while preserving the current lease fence."""

        self._ensure_open()
        generation = self._leases.get(operation_id)
        if generation is None:
            raise SourceBrokerV2RunnerFencedError("job was not claimed by this runner")
        lease = _Lease(operation_id, generation)
        self._owned_job(lease, SourceBrokerV2JobRunnerState.CLAIMED)
        self._start_dispatch(lease, b"{}")

    def sqlite_pragmas(self) -> dict[str, int | str]:
        self._ensure_open()
        with self._connection() as connection:
            busy_timeout = int(connection.execute("PRAGMA busy_timeout").fetchone()[0])
            journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            synchronous = int(connection.execute("PRAGMA synchronous").fetchone()[0])
        return {
            "busy_timeout": busy_timeout,
            "journal_mode": journal_mode,
            "synchronous": synchronous,
        }

    def checkpoint(self) -> None:
        """Run an explicit FULL WAL checkpoint outside business transactions."""

        self._ensure_open()
        self._checkpoint()

    def recover_expired_once(self, *, limit: int | None = None) -> int:
        """Recover at most one configured batch of expired leases."""

        self._ensure_open()
        count = self._normalize_limit(limit)
        with self._transaction() as connection:
            return self._recover_expired(connection, self._now(), limit=count)

    def wake(self) -> None:
        self._wake_event.set()

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        self._leases.clear()

    def close(self) -> None:
        self.stop()
        self._closed = True

    def serve_forever(self, *, poll_interval_seconds: float = 1.0) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        self._ensure_open()
        try:
            while not self._stop_event.is_set():
                processed = self.run_once()
                self.reconcile_once()
                if processed >= self._config.max_batch:
                    continue
                self._wake_event.wait(poll_interval_seconds)
                self._wake_event.clear()
        finally:
            self._leases.clear()
            self._closed = True

    def _process_new_claim(self, lease: _Lease) -> None:
        try:
            job = self._owned_job(lease, SourceBrokerV2JobRunnerState.CLAIMED)
            self._require_authorized_intent(job.intent, now=self._now())
            self._require_stored_stage_proof(
                job.intent,
                authority_hash=job.stage_authority_hash,
                record_commitment=job.stage_record_commitment,
                now=self._now(),
            )
            budget = self._budget(job.intent)
            binding = self._registry.open(job.intent.source_id)
            dispatch_request = _dispatch_request(job.intent)

            replay_request, replay, source_evidence = self._replay_source_operation(
                job=job,
                binding=binding,
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                operation_id=dispatch_request.operation_id,
                operation_request_hash=dispatch_request.request_hash,
                budget=budget,
            )
            del replay_request
            if replay.status is SourceBrokerV2ReplayStatus.UNKNOWN:
                raise SourceBrokerV2RunnerError("external source replay returned UNKNOWN")

            claim_evidence = budget.call(
                "claim authority observation",
                lambda: binding.verifier.observe_claim(
                    intent=job.intent,
                    deadline=budget.deadline,
                ),
            )
            quota_evidence = budget.call(
                "quota authority observation",
                lambda: binding.verifier.observe_quota(
                    intent=job.intent,
                    deadline=budget.deadline,
                ),
            )

            if replay.status is SourceBrokerV2ReplayStatus.FOUND:
                dispatch = self._dispatch_from_replay(
                    job.intent,
                    dispatch_request,
                    replay,
                )
                self._store_terminal(
                    job.lease,
                    dispatch=dispatch,
                    source_evidence=source_evidence,
                    claim_evidence=claim_evidence,
                    quota_evidence=quota_evidence,
                    allowed_state=SourceBrokerV2JobRunnerState.CLAIMED,
                    budget=budget,
                )
                self._finish_terminal_claim(job.lease, binding=binding, budget=budget)
                return

            claim_request, claim = self._claim_source_operation(
                job=job,
                binding=binding,
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                operation_id=dispatch_request.operation_id,
                operation_request_hash=dispatch_request.request_hash,
                budget=budget,
            )
            if claim.status in {
                SourceBrokerV2ClaimStatus.SUCCESS,
                SourceBrokerV2ClaimStatus.FAILURE,
            }:
                if claim.result is None:
                    raise ValueError("terminal source claim omitted its result")
                dispatch = _parse_native(SourceBrokerV2DispatchResponse, claim.result)
                _require_dispatch_binding(job.intent, dispatch_request, dispatch)
                terminal_evidence = SourceBrokerV2NativeEvidence.create(
                    kind="source",
                    request=canonical_model_json_bytes(claim_request),
                    receipt=canonical_model_json_bytes(claim),
                )
                self._store_terminal(
                    job.lease,
                    dispatch=dispatch,
                    source_evidence=terminal_evidence,
                    claim_evidence=claim_evidence,
                    quota_evidence=quota_evidence,
                    allowed_state=SourceBrokerV2JobRunnerState.CLAIMED,
                    budget=budget,
                )
                self._finish_terminal_claim(job.lease, binding=binding, budget=budget)
                return
            if claim.status is not SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT:
                raise SourceBrokerV2RunnerError(f"external source claim is {claim.status.value}")

            claim_bytes = canonical_model_json_bytes(claim)
            self._start_dispatch(job.lease, claim_bytes, budget=budget)
            raw_dispatch: bytes | None = None
            try:
                raw_dispatch = self._external_call(
                    job=job,
                    state=SourceBrokerV2JobRunnerState.DISPATCHING,
                    budget=budget,
                    stage="source dispatch",
                    function=lambda: self._transport_call(
                        source_id=job.intent.source_id,
                        binding=binding,
                        operation="dispatch",
                        payload=canonical_model_json_bytes(
                            SourceBrokerV2DispatchEnvelope(
                                request=dispatch_request,
                                claim_receipt=claim,
                            )
                        ),
                        deadline=budget.deadline,
                    ),
                )
                _parse_native(SourceBrokerV2DispatchResponse, raw_dispatch)
            except _RunnerDeadlineError:
                raise
            except Exception:
                raw_dispatch = None

            dispatching_job = self._owned_job(
                job.lease,
                SourceBrokerV2JobRunnerState.DISPATCHING,
            )
            _, replay, source_evidence = self._replay_source_operation(
                job=dispatching_job,
                binding=binding,
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                operation_id=dispatch_request.operation_id,
                operation_request_hash=dispatch_request.request_hash,
                budget=budget,
            )
            if replay.status is not SourceBrokerV2ReplayStatus.FOUND:
                raise SourceBrokerV2RunnerError(
                    f"dispatch terminal observation is {replay.status.value}"
                )
            dispatch = self._dispatch_from_replay(
                job.intent,
                dispatch_request,
                replay,
            )
            if raw_dispatch is not None and raw_dispatch != canonical_model_json_bytes(dispatch):
                raise ValueError("raw dispatch response conflicts with signed terminal observation")
            self._store_terminal(
                job.lease,
                dispatch=dispatch,
                source_evidence=source_evidence,
                claim_evidence=claim_evidence,
                quota_evidence=quota_evidence,
                allowed_state=SourceBrokerV2JobRunnerState.DISPATCHING,
                budget=budget,
            )
            self._finish_terminal_claim(job.lease, binding=binding, budget=budget)
        except SourceBrokerV2RunnerFencedError:
            return
        except Exception as exc:
            self._mark_reconcile_any(lease, exc, phase="process_new")
        finally:
            self._release_lease(lease)

    def _finish_terminal_claim(
        self,
        lease: _Lease,
        *,
        binding: SourceBrokerV2ProviderBinding | None = None,
        budget: _MonotonicBudget | None = None,
    ) -> None:
        try:
            job = self._owned_job(lease, SourceBrokerV2JobRunnerState.TERMINAL)
            self._require_authorized_intent(job.intent, now=self._now())
            self._require_stored_stage_proof(
                job.intent,
                authority_hash=job.stage_authority_hash,
                record_commitment=job.stage_record_commitment,
                now=self._now(),
            )
            active_budget = budget or self._budget(job.intent)
            active_binding = binding or self._registry.open(job.intent.source_id)
            if (
                job.dispatch_receipt is None
                or job.source_evidence is None
                or job.claim_evidence is None
                or job.quota_evidence is None
            ):
                raise ValueError("terminal job lacks its prerequisite native evidence")
            dispatch = _parse_native(SourceBrokerV2DispatchResponse, job.dispatch_receipt)
            source_evidence = _parse_native(SourceBrokerV2NativeEvidence, job.source_evidence)
            claim_evidence = _parse_native(SourceBrokerV2NativeEvidence, job.claim_evidence)
            quota_evidence = _parse_native(SourceBrokerV2NativeEvidence, job.quota_evidence)
            status = SourceBrokerV2JobOutcomeStatus(dispatch.outcome.value)
            active_budget.call(
                "terminal source evidence verification",
                lambda: active_binding.verifier.verify_source(
                    intent=job.intent,
                    evidence=source_evidence,
                    response=dispatch.response,
                    status=status,
                    deadline=active_budget.deadline,
                ),
            )

            finalize_request = _finalize_request(job.intent, dispatch)
            _, replay, _ = self._replay_source_operation(
                job=job,
                binding=active_binding,
                phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                operation_id=finalize_request.operation_id,
                operation_request_hash=finalize_request.request_hash,
                budget=active_budget,
            )
            if replay.status is SourceBrokerV2ReplayStatus.UNKNOWN:
                raise SourceBrokerV2RunnerError("external finalize replay returned UNKNOWN")
            if replay.status is SourceBrokerV2ReplayStatus.FOUND:
                if replay.result is None:
                    raise ValueError("found finalize replay omitted its result")
                finalize = _parse_native(SourceBrokerV2FinalizeResponse, replay.result)
            else:
                claim_request, claim = self._claim_source_operation(
                    job=job,
                    binding=active_binding,
                    phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                    operation_id=finalize_request.operation_id,
                    operation_request_hash=finalize_request.request_hash,
                    budget=active_budget,
                )
                if claim.status in {
                    SourceBrokerV2ClaimStatus.SUCCESS,
                    SourceBrokerV2ClaimStatus.FAILURE,
                }:
                    if claim.result is None:
                        raise ValueError("terminal finalize claim omitted its result")
                    finalize = _parse_native(SourceBrokerV2FinalizeResponse, claim.result)
                elif claim.status is SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT:
                    raw_finalize: bytes | None = None
                    try:
                        raw_finalize = self._external_call(
                            job=job,
                            state=SourceBrokerV2JobRunnerState.TERMINAL,
                            budget=active_budget,
                            stage="source finalize",
                            function=lambda: self._transport_call(
                                source_id=job.intent.source_id,
                                binding=active_binding,
                                operation="finalize",
                                payload=canonical_model_json_bytes(
                                    SourceBrokerV2FinalizeEnvelope(
                                        request=finalize_request,
                                        claim_receipt=claim,
                                    )
                                ),
                                deadline=active_budget.deadline,
                            ),
                        )
                        _parse_native(SourceBrokerV2FinalizeResponse, raw_finalize)
                    except _RunnerDeadlineError:
                        raise
                    except Exception:
                        raw_finalize = None
                    terminal_job = self._owned_job(
                        job.lease,
                        SourceBrokerV2JobRunnerState.TERMINAL,
                    )
                    _, final_replay, _ = self._replay_source_operation(
                        job=terminal_job,
                        binding=active_binding,
                        phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                        operation_id=finalize_request.operation_id,
                        operation_request_hash=finalize_request.request_hash,
                        budget=active_budget,
                    )
                    if (
                        final_replay.status is not SourceBrokerV2ReplayStatus.FOUND
                        or final_replay.result is None
                    ):
                        raise SourceBrokerV2RunnerError(
                            f"finalize terminal observation is {final_replay.status.value}"
                        )
                    finalize = _parse_native(
                        SourceBrokerV2FinalizeResponse,
                        final_replay.result,
                    )
                    if raw_finalize is not None and raw_finalize != canonical_model_json_bytes(
                        finalize
                    ):
                        raise ValueError(
                            "raw finalize response conflicts with signed terminal observation"
                        )
                else:
                    raise SourceBrokerV2RunnerError(
                        f"external finalize claim is {claim.status.value}"
                    )
                del claim_request
            _require_finalize_binding(job.intent, finalize_request, finalize)
            self._store_finalize(job.lease, finalize, budget=active_budget)

            lineage_evidence = active_budget.call(
                "lineage authority observation",
                lambda: active_binding.verifier.observe_lineage(
                    intent=job.intent,
                    source_receipt_hash=source_evidence.receipt_hash,
                    claim_receipt_hash=claim_evidence.receipt_hash,
                    quota_receipt_hash=quota_evidence.receipt_hash,
                    deadline=active_budget.deadline,
                ),
            )
            outcome = active_budget.call(
                "four-authority outcome verification",
                lambda: build_verified_job_outcome(
                    intent=job.intent,
                    status=status,
                    response=dispatch.response,
                    source_evidence=source_evidence,
                    claim_evidence=claim_evidence,
                    quota_evidence=quota_evidence,
                    lineage_evidence=lineage_evidence,
                    verifier=active_binding.verifier,
                    deadline=active_budget.deadline,
                ),
            )
            self._publish(
                job.lease,
                outcome,
                lineage_evidence=lineage_evidence,
                budget=active_budget,
            )
        except SourceBrokerV2RunnerFencedError:
            return
        except Exception as exc:
            self._mark_reconcile_any(lease, exc, phase="finish_terminal")
        finally:
            self._release_lease(lease)

    def _reconcile_claim(self, lease: _Lease) -> None:
        try:
            job = self._owned_job(lease, SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED)
            self._require_authorized_intent(job.intent, now=self._now())
            self._require_stored_stage_proof(
                job.intent,
                authority_hash=job.stage_authority_hash,
                record_commitment=job.stage_record_commitment,
                now=self._now(),
            )
            budget = self._budget(job.intent)
            binding = self._registry.open(job.intent.source_id)
            dispatch_request = _dispatch_request(job.intent)
            _, replay, source_evidence = self._replay_source_operation(
                job=job,
                binding=binding,
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                operation_id=dispatch_request.operation_id,
                operation_request_hash=dispatch_request.request_hash,
                budget=budget,
            )
            if replay.status is not SourceBrokerV2ReplayStatus.FOUND:
                return
            dispatch = self._dispatch_from_replay(job.intent, dispatch_request, replay)
            claim_evidence = budget.call(
                "claim authority recovery observation",
                lambda: binding.verifier.observe_claim(
                    intent=job.intent,
                    deadline=budget.deadline,
                ),
            )
            quota_evidence = budget.call(
                "quota authority recovery observation",
                lambda: binding.verifier.observe_quota(
                    intent=job.intent,
                    deadline=budget.deadline,
                ),
            )
            self._store_terminal(
                job.lease,
                dispatch=dispatch,
                source_evidence=source_evidence,
                claim_evidence=claim_evidence,
                quota_evidence=quota_evidence,
                allowed_state=SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED,
                budget=budget,
            )
            self._finish_terminal_claim(job.lease, binding=binding, budget=budget)
        except SourceBrokerV2RunnerFencedError:
            return
        except Exception as exc:
            self._mark_reconcile_any(lease, exc, phase="recover_reconcile")
        finally:
            self._release_lease(lease)

    def _replay_source_operation(
        self,
        *,
        job: _ClaimedJob,
        binding: SourceBrokerV2ProviderBinding,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        operation_request_hash: str,
        budget: _MonotonicBudget,
    ) -> tuple[
        SourceBrokerV2ReplayRequest,
        SourceBrokerV2ReplayResponse,
        SourceBrokerV2NativeEvidence,
    ]:
        request = SourceBrokerV2ReplayRequest(
            saga_id=job.intent.claim.saga_id,
            operation_id=operation_id,
            phase=phase,
            operation_request_hash=operation_request_hash,
            challenge=secrets.token_hex(32),
        )
        request_bytes = canonical_model_json_bytes(request)
        receipt_bytes = self._external_call(
            job=job,
            state=job.state,
            budget=budget,
            stage=f"source {phase.value} replay",
            function=lambda: self._transport_call(
                source_id=job.intent.source_id,
                binding=binding,
                operation="replay",
                payload=request_bytes,
                deadline=budget.deadline,
            ),
        )
        receipt = _parse_native(SourceBrokerV2ReplayResponse, receipt_bytes)
        budget.call(
            f"source {phase.value} replay verification",
            lambda: binding.verifier.verify_replay(
                intent=job.intent,
                request=request,
                receipt=receipt,
                deadline=budget.deadline,
            ),
        )
        if (
            receipt.saga_id != job.intent.claim.saga_id
            or receipt.operation_id != operation_id
            or receipt.phase is not phase
        ):
            raise ValueError("source replay is bound to a different operation")
        return (
            request,
            receipt,
            SourceBrokerV2NativeEvidence.create(
                kind="source",
                request=request_bytes,
                receipt=receipt_bytes,
            ),
        )

    def _claim_source_operation(
        self,
        *,
        job: _ClaimedJob,
        binding: SourceBrokerV2ProviderBinding,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        operation_request_hash: str,
        budget: _MonotonicBudget,
    ) -> tuple[SourceBrokerV2ClaimOnceRequest, SourceBrokerV2ClaimOnceResponse]:
        budget.ensure(f"source {phase.value} claim preparation")
        remaining = budget.deadline - time.monotonic()
        max_external_deadline = min(
            job.intent.deadline,
            self._now() + timedelta(seconds=remaining),
        )
        request = SourceBrokerV2ClaimOnceRequest(
            saga_id=job.intent.claim.saga_id,
            operation_id=operation_id,
            phase=phase,
            operation_request_hash=operation_request_hash,
            challenge=secrets.token_hex(32),
            claim_binding_hash=job.intent.claim.claim_binding_hash,
            claim_generation=job.intent.claim.claim_generation,
            scheduler_fencing_token=job.intent.claim.scheduler_fencing_token,
            executor_owner_token_hash=self._executor_owner_token_hash,
            executor_generation=job.lease.generation,
            max_external_deadline=max_external_deadline,
            not_before_takeover_at=max_external_deadline
            + timedelta(seconds=self._config.takeover_grace_seconds),
        )
        request_bytes = canonical_model_json_bytes(request)
        receipt_bytes = self._external_call(
            job=job,
            state=job.state,
            budget=budget,
            stage=f"source {phase.value} claim",
            function=lambda: self._transport_call(
                source_id=job.intent.source_id,
                binding=binding,
                operation="claim_once",
                payload=request_bytes,
                deadline=budget.deadline,
            ),
        )
        receipt = _parse_native(SourceBrokerV2ClaimOnceResponse, receipt_bytes)
        budget.call(
            f"source {phase.value} claim verification",
            lambda: binding.verifier.verify_claim_once(
                intent=job.intent,
                request=request,
                receipt=receipt,
                deadline=budget.deadline,
            ),
        )
        if (
            receipt.saga_id != job.intent.claim.saga_id
            or receipt.operation_id != operation_id
            or receipt.phase is not phase
        ):
            raise ValueError("source claim is bound to a different operation")
        return request, receipt

    def _transport_call(
        self,
        *,
        source_id: str,
        binding: SourceBrokerV2ProviderBinding,
        operation: Literal["claim_once", "dispatch", "finalize", "replay"],
        payload: bytes,
        deadline: float,
    ) -> bytes:
        if self._registry._profile is SourceBrokerV2RegistryProfile.PRODUCTION:
            snapshot = self._production_registry_snapshot
            if type(snapshot) is not _ProductionRunnerRegistrySnapshot:
                raise SourceBrokerV2RunnerError(
                    "production runner lacks its original registry snapshot"
                )
            snapshot.require_selected(
                registry=self._registry,
                source_id=source_id,
                binding=binding,
            )
            transport: SourceBrokerV2Transport = self._registry._transport_for_request(
                source_id=source_id,
                binding=binding,
                deadline=deadline,
            )
            snapshot.require_selected(
                registry=self._registry,
                source_id=source_id,
                binding=binding,
            )
        else:
            self._registry.require_live(source_id=source_id, binding=binding)
            binding.require_live()
            transport = binding.transport
        _require_deadline(deadline, f"source {operation} transport")
        function = getattr(transport, operation, None)
        if not callable(function):
            raise SourceBrokerV2RunnerError(
                f"registered provider lacks transport operation {operation}"
            )
        return function(payload, deadline=deadline)

    def _external_call(
        self,
        *,
        job: _ClaimedJob,
        state: SourceBrokerV2JobRunnerState,
        budget: _MonotonicBudget,
        stage: str,
        function: Callable[[], _T],
    ) -> _T:
        # Every durable state transition refreshes the heartbeat. The lease is
        # required to cover this whole budget, so another FULL fsync per wire
        # call adds latency without extending the valid ownership interval.
        del job, state
        return budget.call(stage, function)

    def _dispatch_from_replay(
        self,
        intent: SourceBrokerV2JobIntentEnvelope,
        request: SourceBrokerV2DispatchRequest,
        replay: SourceBrokerV2ReplayResponse,
    ) -> SourceBrokerV2DispatchResponse:
        if replay.status is not SourceBrokerV2ReplayStatus.FOUND or replay.result is None:
            raise ValueError("source replay does not contain a dispatch result")
        dispatch = _parse_native(SourceBrokerV2DispatchResponse, replay.result)
        _require_dispatch_binding(intent, request, dispatch)
        return dispatch

    def _budget(self, intent: SourceBrokerV2JobIntentEnvelope) -> _MonotonicBudget:
        return _MonotonicBudget(
            deadline=time.monotonic() + self._config.total_deadline_seconds,
            wall_deadline=intent.deadline,
            clock=self._now,
        )

    def _require_authorized_intent(
        self,
        intent: SourceBrokerV2JobIntentEnvelope,
        *,
        now: datetime,
    ) -> SourceBrokerV2JobIntentEnvelope:
        validated = require_authorized_source_broker_v2_job_intent(
            intent,
            manifest_keyring=self._manifest_keyring,
            authorization_keyring=self._authorization_keyring,
            now=now,
        )
        return validated

    def _stage_proof(
        self,
        intent: SourceBrokerV2JobIntentEnvelope,
        *,
        now: datetime,
    ) -> tuple[str, str]:
        return self._stage_store.require_execution_intent(intent, now=now)

    def _require_stored_stage_proof(
        self,
        intent: SourceBrokerV2JobIntentEnvelope,
        *,
        authority_hash: object,
        record_commitment: object,
        now: datetime,
    ) -> None:
        observed = self._stage_proof(intent, now=now)
        if observed != (authority_hash, record_commitment):
            raise SourceBrokerV2RunnerError("source-stage execution proof changed or conflicts")

    def _claim_new(self, *, limit: int | None) -> tuple[_Lease, ...]:
        count = self._normalize_limit(limit)
        now = self._now()
        expires = now + timedelta(seconds=self._config.lease_seconds)
        leases: list[_Lease] = []
        with self._transaction() as connection:
            self._recover_expired(connection, now, limit=count)
            rows = connection.execute(
                """
                SELECT operation_id, intent, stage_authority_hash, stage_record_commitment
                FROM source_broker_v2_jobs
                WHERE state = 'NEW'
                ORDER BY created_at, operation_id
                LIMIT ?
                """,
                (count,),
            ).fetchall()
            for row in rows:
                operation_id = str(row["operation_id"])
                try:
                    self._require_authorized_intent(
                        parse_job_intent(bytes(row["intent"])),
                        now=now,
                    )
                    self._require_stored_stage_proof(
                        parse_job_intent(bytes(row["intent"])),
                        authority_hash=row["stage_authority_hash"],
                        record_commitment=row["stage_record_commitment"],
                        now=now,
                    )
                except Exception as exc:
                    connection.execute(
                        """
                        UPDATE source_broker_v2_jobs
                        SET state = 'RECONCILE_REQUIRED', terminal_reason = ?, updated_at = ?
                        WHERE operation_id = ? AND state = 'NEW'
                        """,
                        (
                            f"authorization rejected before claim: {exc}"[:500],
                            _encode_time(now),
                            operation_id,
                        ),
                    )
                    continue
                updated = connection.execute(
                    """
                    UPDATE source_broker_v2_jobs
                    SET state = 'CLAIMED', owner_id = ?, lease_generation = lease_generation + 1,
                        lease_expires_at = ?, heartbeat_at = ?, updated_at = ?
                    WHERE operation_id = ? AND state = 'NEW'
                    """,
                    (
                        self._lease_owner_id,
                        _encode_time(expires),
                        _encode_time(now),
                        _encode_time(now),
                        operation_id,
                    ),
                )
                if updated.rowcount == 1:
                    row_generation = connection.execute(
                        "SELECT lease_generation FROM source_broker_v2_jobs WHERE operation_id = ?",
                        (operation_id,),
                    ).fetchone()
                    generation = int(row_generation["lease_generation"])
                    lease = _Lease(operation_id, generation)
                    self._leases[operation_id] = generation
                    leases.append(lease)
        return tuple(leases)

    def _claim_terminal(self, *, limit: int) -> tuple[_Lease, ...]:
        return self._claim_existing(SourceBrokerV2JobRunnerState.TERMINAL, limit)

    def _claim_reconcile(self, *, limit: int) -> tuple[_Lease, ...]:
        return self._claim_existing(SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED, limit)

    def _claim_existing(
        self, state: SourceBrokerV2JobRunnerState, limit: int
    ) -> tuple[_Lease, ...]:
        now = self._now()
        expires = now + timedelta(seconds=self._config.lease_seconds)
        leases: list[_Lease] = []
        with self._transaction() as connection:
            if state is SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED:
                self._recover_expired(connection, now, limit=limit)
            rows = connection.execute(
                """
                SELECT operation_id, owner_id, lease_generation, lease_expires_at
                FROM source_broker_v2_jobs
                WHERE state = ?
                  AND (owner_id IS NULL OR owner_id = ? OR lease_expires_at <= ?)
                ORDER BY updated_at, operation_id
                LIMIT ?
                """,
                (state.value, self._lease_owner_id, _encode_time(now), limit),
            ).fetchall()
            for row in rows:
                operation_id = str(row["operation_id"])
                same_live_owner = (
                    row["owner_id"] == self._lease_owner_id
                    and row["lease_expires_at"] is not None
                    and str(row["lease_expires_at"]) > _encode_time(now)
                )
                generation = int(row["lease_generation"]) + (0 if same_live_owner else 1)
                updated = connection.execute(
                    """
                    UPDATE source_broker_v2_jobs
                    SET owner_id = ?, lease_generation = ?, lease_expires_at = ?,
                        heartbeat_at = ?, updated_at = ?
                    WHERE operation_id = ? AND state = ?
                      AND (owner_id IS NULL OR owner_id = ? OR lease_expires_at <= ?)
                    """,
                    (
                        self._lease_owner_id,
                        generation,
                        _encode_time(expires),
                        _encode_time(now),
                        _encode_time(now),
                        operation_id,
                        state.value,
                        self._lease_owner_id,
                        _encode_time(now),
                    ),
                )
                if updated.rowcount == 1:
                    lease = _Lease(operation_id, generation)
                    self._leases[operation_id] = generation
                    leases.append(lease)
        return tuple(leases)

    def _owned_job(self, lease: _Lease, state: SourceBrokerV2JobRunnerState) -> _ClaimedJob:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT state, intent, stage_authority_hash, stage_record_commitment,
                       claim_receipt, dispatch_receipt, source_evidence, claim_evidence,
                       quota_evidence, finalize_receipt, lease_generation
                FROM source_broker_v2_jobs
                WHERE operation_id = ? AND state = ? AND owner_id = ?
                  AND lease_generation = ? AND lease_expires_at > ?
                """,
                (
                    lease.operation_id,
                    state.value,
                    self._lease_owner_id,
                    lease.generation,
                    _encode_time(self._now()),
                ),
            ).fetchone()
        if row is None:
            self._release_lease(lease)
            raise SourceBrokerV2RunnerFencedError("job lease is no longer owned by this runner")
        return _ClaimedJob(
            lease=lease,
            state=state,
            intent=parse_job_intent(bytes(row["intent"])),
            stage_authority_hash=str(row["stage_authority_hash"]),
            stage_record_commitment=str(row["stage_record_commitment"]),
            claim_receipt=_optional_bytes(row["claim_receipt"]),
            dispatch_receipt=_optional_bytes(row["dispatch_receipt"]),
            source_evidence=_optional_bytes(row["source_evidence"]),
            claim_evidence=_optional_bytes(row["claim_evidence"]),
            quota_evidence=_optional_bytes(row["quota_evidence"]),
            finalize_receipt=_optional_bytes(row["finalize_receipt"]),
        )

    def _start_dispatch(
        self,
        lease: _Lease,
        claim_receipt: bytes,
        *,
        budget: _MonotonicBudget | None = None,
    ) -> None:
        self._mutate_owned(
            lease,
            from_state=SourceBrokerV2JobRunnerState.CLAIMED,
            to_state=SourceBrokerV2JobRunnerState.DISPATCHING,
            assignments="claim_receipt = ?",
            values=(claim_receipt,),
            budget=budget,
        )

    def _store_terminal(
        self,
        lease: _Lease,
        *,
        dispatch: SourceBrokerV2DispatchResponse,
        source_evidence: SourceBrokerV2NativeEvidence,
        claim_evidence: SourceBrokerV2NativeEvidence,
        quota_evidence: SourceBrokerV2NativeEvidence,
        allowed_state: SourceBrokerV2JobRunnerState,
        budget: _MonotonicBudget,
    ) -> None:
        self._mutate_owned(
            lease,
            from_state=allowed_state,
            to_state=SourceBrokerV2JobRunnerState.TERMINAL,
            assignments=(
                "dispatch_receipt = ?, source_evidence = ?, claim_evidence = ?, "
                "quota_evidence = ?, terminal_reason = NULL"
            ),
            values=(
                canonical_model_json_bytes(dispatch),
                canonical_job_model_bytes(source_evidence),
                canonical_job_model_bytes(claim_evidence),
                canonical_job_model_bytes(quota_evidence),
            ),
            budget=budget,
        )

    def _store_finalize(
        self,
        lease: _Lease,
        receipt: SourceBrokerV2FinalizeResponse,
        *,
        budget: _MonotonicBudget,
    ) -> None:
        self._mutate_owned(
            lease,
            from_state=SourceBrokerV2JobRunnerState.TERMINAL,
            to_state=SourceBrokerV2JobRunnerState.TERMINAL,
            assignments="finalize_receipt = ?",
            values=(canonical_model_json_bytes(receipt),),
            budget=budget,
        )

    def _publish(
        self,
        lease: _Lease,
        outcome: SourceBrokerV2JobOutcomeEnvelope,
        *,
        lineage_evidence: SourceBrokerV2NativeEvidence,
        budget: _MonotonicBudget,
    ) -> None:
        budget.ensure("durable published transition")
        now = self._now()
        expires = now + timedelta(seconds=self._config.lease_seconds)
        outcome_bytes = canonical_job_model_bytes(outcome)
        lineage_bytes = canonical_job_model_bytes(lineage_evidence)
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT * FROM source_broker_v2_jobs
                WHERE operation_id = ? AND state = 'TERMINAL' AND owner_id = ?
                  AND lease_generation = ? AND lease_expires_at > ?
                """,
                (
                    lease.operation_id,
                    self._lease_owner_id,
                    lease.generation,
                    _encode_time(now),
                ),
            ).fetchone()
            if row is None:
                self._release_lease(lease)
                raise SourceBrokerV2RunnerFencedError(
                    "job lease was fenced before durable publication"
                )
            committed = dict(row)
            committed.update(
                {
                    "state": SourceBrokerV2JobRunnerState.PUBLISHED.value,
                    "lineage_evidence": lineage_bytes,
                    "outcome": outcome_bytes,
                    "terminal_reason": None,
                    "lease_expires_at": _encode_time(expires),
                    "heartbeat_at": _encode_time(now),
                    "updated_at": _encode_time(now),
                }
            )
            published_commit_hash = source_broker_v2_published_commit_hash(
                {name: committed[name] for name in _PUBLISHED_COMMIT_COLUMNS}
            )
            updated = connection.execute(
                """
                UPDATE source_broker_v2_jobs
                SET state = 'PUBLISHED', lineage_evidence = ?, outcome = ?,
                    published_commit_hash = ?, terminal_reason = NULL,
                    lease_expires_at = ?, heartbeat_at = ?, updated_at = ?
                WHERE operation_id = ? AND state = 'TERMINAL' AND owner_id = ?
                  AND lease_generation = ? AND lease_expires_at > ?
                """,
                (
                    lineage_bytes,
                    outcome_bytes,
                    published_commit_hash,
                    _encode_time(expires),
                    _encode_time(now),
                    _encode_time(now),
                    lease.operation_id,
                    self._lease_owner_id,
                    lease.generation,
                    _encode_time(now),
                ),
            )
            if updated.rowcount != 1:
                self._release_lease(lease)
                raise SourceBrokerV2RunnerFencedError(
                    "job lease was fenced during durable publication"
                )
            budget.ensure("durable published acceptance")
        self._release_lease(lease)

    def _mark_reconcile_any(
        self,
        lease: _Lease,
        exc: Exception,
        *,
        phase: str,
    ) -> None:
        try:
            with self._connection() as connection:
                row = connection.execute(
                    """
                    SELECT state, operation_hash FROM source_broker_v2_jobs
                    WHERE operation_id = ? AND owner_id = ? AND lease_generation = ?
                    """,
                    (lease.operation_id, self._lease_owner_id, lease.generation),
                ).fetchone()
            if row is None:
                return
            state = SourceBrokerV2JobRunnerState(str(row["state"]))
            if state not in {
                SourceBrokerV2JobRunnerState.CLAIMED,
                SourceBrokerV2JobRunnerState.DISPATCHING,
                SourceBrokerV2JobRunnerState.TERMINAL,
            }:
                return
            self._mutate_owned(
                lease,
                from_state=state,
                to_state=SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED,
                assignments="terminal_reason = ?",
                values=(
                    _reconcile_diagnostic(
                        code=_reconcile_code(exc),
                        exception_class=type(exc).__name__,
                        phase=phase,
                        operation_hash=str(row["operation_hash"]),
                    ),
                ),
            )
        except SourceBrokerV2RunnerFencedError:
            return
        finally:
            self._release_lease(lease)

    def _mutate_owned(
        self,
        lease: _Lease,
        *,
        from_state: SourceBrokerV2JobRunnerState,
        to_state: SourceBrokerV2JobRunnerState,
        assignments: str,
        values: tuple[object, ...],
        budget: _MonotonicBudget | None = None,
    ) -> None:
        if budget is not None:
            budget.ensure(f"durable {to_state.value.lower()} transition")
        now = self._now()
        expires = now + timedelta(seconds=self._config.lease_seconds)
        with self._transaction() as connection:
            updated = connection.execute(
                f"""
                UPDATE source_broker_v2_jobs
                SET state = ?, {assignments}, lease_expires_at = ?, heartbeat_at = ?, updated_at = ?
                WHERE operation_id = ? AND state = ? AND owner_id = ? AND lease_generation = ?
                  AND lease_expires_at > ?
                """,
                (
                    to_state.value,
                    *values,
                    _encode_time(expires),
                    _encode_time(now),
                    _encode_time(now),
                    lease.operation_id,
                    from_state.value,
                    self._lease_owner_id,
                    lease.generation,
                    _encode_time(now),
                ),
            )
            if updated.rowcount != 1:
                self._release_lease(lease)
                raise SourceBrokerV2RunnerFencedError(
                    "job lease was fenced before durable mutation"
                )
            if budget is not None:
                budget.ensure(f"durable {to_state.value.lower()} acceptance")
        if to_state in {
            SourceBrokerV2JobRunnerState.PUBLISHED,
            SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED,
        }:
            self._release_lease(lease)

    def _recover_expired(
        self,
        connection: sqlite3.Connection,
        now: datetime,
        *,
        limit: int,
    ) -> int:
        encoded_now = _encode_time(now)
        rows = connection.execute(
            """
            SELECT operation_id, operation_hash, state, lease_generation
            FROM source_broker_v2_jobs
                INDEXED BY source_broker_v2_jobs_expiry_ordered
            WHERE state IN ('CLAIMED', 'DISPATCHING') AND lease_expires_at <= ?
            ORDER BY lease_expires_at, state, operation_id
            LIMIT ?
            """,
            (encoded_now, limit),
        ).fetchall()
        recovered = 0
        for row in rows:
            state = SourceBrokerV2JobRunnerState(str(row["state"]))
            if state is SourceBrokerV2JobRunnerState.CLAIMED:
                to_state = SourceBrokerV2JobRunnerState.NEW
                reason: str | None = None
            else:
                to_state = SourceBrokerV2JobRunnerState.RECONCILE_REQUIRED
                reason = _reconcile_diagnostic(
                    code=SourceBrokerV2ReconcileCode.LEASE_EXPIRED,
                    exception_class="LeaseExpired",
                    phase="expired_dispatch",
                    operation_hash=str(row["operation_hash"]),
                )
            updated = connection.execute(
                """
                UPDATE source_broker_v2_jobs
                SET state = ?, owner_id = NULL, lease_expires_at = NULL,
                    heartbeat_at = NULL, terminal_reason = ?, updated_at = ?
                WHERE operation_id = ? AND state = ? AND lease_expires_at <= ?
                    AND lease_generation = ?
                """,
                (
                    to_state.value,
                    reason,
                    encoded_now,
                    str(row["operation_id"]),
                    state.value,
                    encoded_now,
                    int(row["lease_generation"]),
                ),
            )
            if updated.rowcount == 1:
                recovered += 1
                self._release_lease(_Lease(str(row["operation_id"]), int(row["lease_generation"])))
        return recovered

    def _initialize(self) -> None:
        initialize_source_broker_v2_job_storage(
            self._db_path,
            busy_timeout_ms=self._config.busy_timeout_ms,
            max_inbox=self._config.max_inbox,
        )

    @contextmanager
    def _connection(self, *, configure_journal: bool = False) -> Iterator[sqlite3.Connection]:
        with open_source_broker_v2_job_storage_connection(
            self._db_path,
            busy_timeout_ms=self._config.busy_timeout_ms,
            configure_journal=configure_journal,
        ) as connection:
            yield connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            else:
                connection.execute("COMMIT")

    def _checkpoint(self) -> None:
        with self._connection() as connection:
            connection.execute("PRAGMA wal_checkpoint(FULL)")

    def _release_lease(self, lease: _Lease) -> None:
        if self._leases.get(lease.operation_id) == lease.generation:
            self._leases.pop(lease.operation_id, None)

    def _normalize_limit(self, limit: int | None) -> int:
        if limit is None:
            return self._config.max_batch
        if limit < 1:
            raise ValueError("limit must be positive")
        return min(limit, self._config.max_batch)

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("runner clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    def _ensure_open(self) -> None:
        if self._closed:
            raise SourceBrokerV2RunnerError("runner is closed")


def _dispatch_request(intent: SourceBrokerV2JobIntentEnvelope) -> SourceBrokerV2DispatchRequest:
    return SourceBrokerV2DispatchRequest(
        saga_id=intent.claim.saga_id,
        operation_id=intent.operation_id,
        call_id=intent.quota.parent_id,
        attempt_identity_hash=intent.claim.attempt_identity_hash,
        claim_plan_hash=intent.claim.claim_plan_hash,
        claim_binding_hash=intent.claim.claim_binding_hash,
        manifest_hash=intent.claim.manifest_hash,
        payload=intent.request,
        claim_payload_hash=intent.claim.claim_payload_hash,
        dispatch_payload_hash=canonical_sha256(strict_canonical_json_loads(intent.request)),
    )


def _scheduler_client_graph_identity(
    clients: SourceBrokerV2SchedulerClients,
) -> tuple[int, int, int, int, int, int]:
    return tuple(
        id(value)
        for value in (
            clients.current_claim,
            clients.source_quota,
            clients.replay_lineage,
            clients.authority_keyring,
            clients.source_authority_keyring,
            clients.source_client,
        )
    )


def _derive_production_authority_evidence(
    runtime: SourceBrokerV2AuthorityRuntime,
    clients: SourceBrokerV2SchedulerClients,
) -> _ProductionAuthorityEvidence:
    source_root_hash = runtime.source_daemon.binding.binding_hash
    claim_root_hash = runtime.root(SourceBrokerV2RootRole.CURRENT_CLAIM).binding.binding_hash
    quota_root_hash = runtime.root(SourceBrokerV2RootRole.SOURCE_QUOTA).binding.binding_hash
    lineage_root_hash = runtime.root(SourceBrokerV2RootRole.REPLAY_LINEAGE).binding.binding_hash
    return _ProductionAuthorityEvidence(
        scheduler_binding_hash=clients.binding_hash,
        source_authority=_authority_ref(
            runtime.source_daemon.binding,
            fence_hash=source_root_hash,
        ),
        claim_authority=_authority_ref(
            runtime.current_claim.binding,
            fence_hash=claim_root_hash,
        ),
        quota_authority=_authority_ref(
            runtime.source_quota.binding,
            fence_hash=quota_root_hash,
        ),
        lineage_authority=_authority_ref(
            runtime.replay_lineage.binding,
            fence_hash=lineage_root_hash,
        ),
        external_root_hash=canonical_job_sha256(
            {
                "claim_root_hash": claim_root_hash,
                "contract": "rquant-source-broker-v2-runner-trusted-roots/v1",
                "lineage_root_hash": lineage_root_hash,
                "quota_root_hash": quota_root_hash,
                "source_root_hash": source_root_hash,
            }
        ),
    )


def _validated_production_authority_evidence(
    runtime: SourceBrokerV2AuthorityRuntime,
    clients: SourceBrokerV2SchedulerClients,
    *,
    deadline: float | None = None,
    validate_filesystem: bool = True,
) -> _ProductionAuthorityEvidence:
    if validate_filesystem:
        _require_production_scheduler_clients(runtime, clients)
    else:
        _require_production_scheduler_clients_in_memory(runtime, clients)
    if deadline is not None:
        _require_deadline(deadline, "current-claim authority preflight")
    current_preflight = clients.current_claim.preflight(deadline=deadline)
    if deadline is not None:
        _require_deadline(deadline, "source-quota authority preflight")
    quota_preflight = clients.source_quota.preflight(deadline=deadline)
    if deadline is not None:
        _require_deadline(deadline, "replay-lineage authority preflight")
    replay_preflight = clients.replay_lineage.preflight(deadline=deadline)
    if deadline is not None:
        _require_deadline(deadline, "source endpoint validation")
    validate_socket_endpoint(clients.source_client._endpoint)
    if deadline is not None:
        _require_deadline(deadline, "source endpoint validation")
    if (
        current_preflight.mode != "production"
        or current_preflight.non_production
        or not current_preflight.root_required
        or not current_preflight.root_configured
        or current_preflight.root_authority_id
        != runtime.root(SourceBrokerV2RootRole.CURRENT_CLAIM).authority_id
        or quota_preflight.accepted is not True
        or replay_preflight.mode != "production"
        or replay_preflight.non_production
        or not replay_preflight.root_required
        or not replay_preflight.root_configured
        or replay_preflight.root_authority_id
        != runtime.root(SourceBrokerV2RootRole.REPLAY_LINEAGE).authority_id
    ):
        raise SourceBrokerV2RunnerError(
            "production authority preflight or trusted root binding is invalid"
        )
    return _derive_production_authority_evidence(runtime, clients)


def _require_production_scheduler_clients(
    runtime: SourceBrokerV2AuthorityRuntime,
    clients: SourceBrokerV2SchedulerClients,
) -> None:
    _require_production_scheduler_clients_in_memory(runtime, clients)
    runtime.validate_layout()
    runtime.validate_scheduler_filesystem()


def _require_production_scheduler_clients_in_memory(
    runtime: SourceBrokerV2AuthorityRuntime,
    clients: SourceBrokerV2SchedulerClients,
) -> None:
    if type(runtime) is not SourceBrokerV2AuthorityRuntime:
        raise TypeError("production graph requires the exact authority runtime")
    if type(clients) is not SourceBrokerV2SchedulerClients:
        raise TypeError("production graph requires exact scheduler clients")
    if clients.binding_hash != _scheduler_clients_binding_hash(runtime):
        raise SourceBrokerV2RunnerError(
            "scheduler client binding hash conflicts with production runtime"
        )
    expected_types = (
        SourceBrokerV2CurrentClaimUnixClient,
        SourceBrokerV2SourceQuotaUnixClient,
        SourceBrokerV2ReplayLineageUnixClient,
        VerifyOnlyEd25519Keyring,
        SourceAuthorityKeyring,
        SourceBrokerV2UnixClient,
    )
    observed_types = tuple(
        type(value)
        for value in (
            clients.current_claim,
            clients.source_quota,
            clients.replay_lineage,
            clients.authority_keyring,
            clients.source_authority_keyring,
            clients.source_client,
        )
    )
    if observed_types != expected_types:
        raise TypeError("production scheduler composition contains wrapped clients")
    scheduler = runtime.scheduler_client
    for client, layout in (
        (clients.current_claim, runtime.current_claim),
        (clients.source_quota, runtime.source_quota),
        (clients.replay_lineage, runtime.replay_lineage),
    ):
        endpoint, server_policy = _expected_unix_client_policy(
            layout,
            scheduler_identity=scheduler.identity,
        )
        if client.endpoint != endpoint or client.server_policy != server_policy:
            raise SourceBrokerV2RunnerError(
                "authority endpoint or authenticated server peer conflicts with runtime"
            )
        if layout is not runtime.source_quota:
            _require_authority_key_binding(
                clients.authority_keyring,
                authority_id=layout.binding.authority_id,
                key_id=layout.binding.key_id,
                purpose=layout.binding.key_purpose,
            )
    source_endpoint, source_server_policy = _expected_unix_client_policy(
        runtime.source_daemon,
        scheduler_identity=scheduler.identity,
    )
    if (
        clients.source_client._endpoint != source_endpoint
        or clients.source_client._server_policy != source_server_policy
        or clients.source_client.source_authority_keyring is not clients.source_authority_keyring
    ):
        raise SourceBrokerV2RunnerError(
            "source endpoint, authenticated server peer, or keyring conflicts with runtime"
        )
    source_keyring = clients.source_authority_keyring
    if (
        source_keyring.expected_authority_id != runtime.source_daemon.binding.authority_id
        or source_keyring.expected_purpose != runtime.source_daemon.binding.key_purpose
        or source_keyring.expected_schema_version != runtime.source_daemon.binding.schema_version
        or set(source_keyring.allowed_key_ids)
        != {
            runtime.source_authority_current_key_id,
            runtime.source_authority_next_key_id,
        }
    ):
        raise SourceBrokerV2RunnerError(
            "source authority, key, purpose, or schema conflicts with runtime"
        )


def _scheduler_clients_binding_hash(runtime: SourceBrokerV2AuthorityRuntime) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-source-broker-v2-scheduler-clients/v1",
            "identities": runtime.identities.model_dump(mode="json"),
            "authority_bindings": [
                layout.binding.model_dump(mode="json")
                for layout in (
                    runtime.current_claim,
                    runtime.source_quota,
                    runtime.replay_lineage,
                    runtime.source_daemon,
                )
            ],
            "authority_sockets": [
                os.fspath(layout.socket_path)
                for layout in (
                    runtime.current_claim,
                    runtime.source_quota,
                    runtime.replay_lineage,
                    runtime.source_daemon,
                )
            ],
            "manifest_verification_key_id": runtime.manifest_verification_key_id,
            "source_authority_key_ids": [
                runtime.source_authority_current_key_id,
                runtime.source_authority_next_key_id,
            ],
        }
    )


def _expected_unix_client_policy(
    layout: Any,
    *,
    scheduler_identity: Any,
) -> tuple[SocketEndpointPolicy, ServerCredentialsPolicy]:
    policy = layout.unix_policy(scheduler_identity)
    return (
        SocketEndpointPolicy(
            path=policy.socket_path,
            owner_uid=policy.service_identity.uid,
            group_gid=policy.access_gid,
            mode=layout.socket_mode,
        ),
        ServerCredentialsPolicy(
            expected_uid=policy.service_identity.uid,
            expected_gid=policy.service_identity.gid,
        ),
    )


def _require_authority_key_binding(
    keyring: VerifyOnlyEd25519Keyring,
    *,
    authority_id: str,
    key_id: str,
    purpose: str,
) -> None:
    record = keyring._records.get(key_id)
    if (
        record is None
        or record.issuer != authority_id
        or record.key_purpose != purpose
        or record.public_key_fingerprint not in keyring.fingerprints_for_purpose(record.key_purpose)
    ):
        raise SourceBrokerV2RunnerError(
            "authority id, key, purpose, schema, or trusted keyring binding is invalid"
        )


def _authority_ref(binding: Any, *, fence_hash: str) -> SourceBrokerV2AuthorityRef:
    return SourceBrokerV2AuthorityRef(
        authority_id=binding.authority_id,
        key_id=binding.key_id,
        purpose=binding.key_purpose,
        schema_version=binding.schema_version,
        generation=binding.generation,
        fence_hash=fence_hash,
    )


def _require_canonical_absolute_path(path: Path, *, label: str) -> None:
    if (
        not isinstance(path, Path)
        or not path.is_absolute()
        or Path(os.path.abspath(path)) != path
        or any(part in {"", ".", ".."} for part in path.parts[1:])
    ):
        raise ValueError(f"{label} must be canonical and absolute")


def _is_strict_descendant(path: Path, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return bool(relative.parts)


def _read_secure_credential_file(
    *,
    root: SourceBrokerV2CredentialRoot,
    requested: Path,
) -> str:
    relative = requested.relative_to(root.path)
    directory_descriptor = _open_absolute_directory(root.path)
    try:
        _require_secure_credential_directory(directory_descriptor, root=root)
        for component in relative.parts[:-1]:
            next_descriptor = _openat_directory(directory_descriptor, component)
            os.close(directory_descriptor)
            directory_descriptor = next_descriptor
            _require_secure_credential_directory(directory_descriptor, root=root)
        leaf_descriptor = _openat_credential_leaf(
            directory_descriptor,
            relative.parts[-1],
        )
        try:
            observed = os.fstat(leaf_descriptor)
            if not stat.S_ISREG(observed.st_mode):
                raise SourceBrokerV2RunnerError("credential file must be a regular file")
            if observed.st_nlink != 1:
                raise SourceBrokerV2RunnerError(
                    "credential file hardlink count must be exactly one"
                )
            if observed.st_uid != root.owner_uid or observed.st_gid != root.owner_gid:
                raise SourceBrokerV2RunnerError("credential file owner is unsafe")
            if stat.S_IMODE(observed.st_mode) != root.file_mode:
                raise SourceBrokerV2RunnerError("credential file mode is unsafe")
            payload = _bounded_descriptor_read(leaf_descriptor, limit=64 * 1024)
        finally:
            os.close(leaf_descriptor)
    except OSError as exc:
        raise SourceBrokerV2RunnerError(
            "credential path contains a symlink, unsafe directory, or unavailable leaf"
        ) from exc
    finally:
        os.close(directory_descriptor)
    try:
        value = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SourceBrokerV2RunnerError("credential file is not valid UTF-8") from exc
    if not value:
        raise SourceBrokerV2RunnerError("credential file is empty")
    return value


def _open_absolute_directory(path: Path) -> int:
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptor = os.open("/", flags)
    try:
        for component in path.parts[1:]:
            next_descriptor = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _openat_directory(parent_descriptor: int, component: str) -> int:
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    return os.open(component, flags, dir_fd=parent_descriptor)


def _openat_credential_leaf(parent_descriptor: int, leaf: str) -> int:
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
    return os.open(leaf, flags, dir_fd=parent_descriptor)


def _require_secure_credential_directory(
    descriptor: int,
    *,
    root: SourceBrokerV2CredentialRoot,
) -> None:
    observed = os.fstat(descriptor)
    if not stat.S_ISDIR(observed.st_mode):
        raise SourceBrokerV2RunnerError("credential ancestor is not a directory")
    if observed.st_uid != root.owner_uid or observed.st_gid != root.owner_gid:
        raise SourceBrokerV2RunnerError("credential directory owner is unsafe")
    if stat.S_IMODE(observed.st_mode) not in root.directory_modes:
        raise SourceBrokerV2RunnerError("credential directory mode is unsafe")


def _bounded_descriptor_read(descriptor: int, *, limit: int) -> bytes:
    chunks: list[bytes] = []
    observed = 0
    while True:
        chunk = os.read(descriptor, min(64 * 1024, limit + 1 - observed))
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        observed += len(chunk)
        if observed > limit:
            raise SourceBrokerV2RunnerError("credential file exceeds the bounded reader limit")


def _finalize_request(
    intent: SourceBrokerV2JobIntentEnvelope,
    dispatch: SourceBrokerV2DispatchResponse,
) -> SourceBrokerV2FinalizeRequest:
    operation_id = canonical_job_sha256(
        {
            "contract": "rquant-source-broker-v2-job-finalize-operation/v2",
            "job_operation_id": intent.operation_id,
            "operation_hash": intent.operation_hash,
        }
    )
    return SourceBrokerV2FinalizeRequest(
        saga_id=intent.claim.saga_id,
        operation_id=operation_id,
        dispatch_evidence_hash=dispatch.evidence_hash,
        claim_binding_hash=intent.claim.claim_binding_hash,
    )


def _require_dispatch_binding(
    intent: SourceBrokerV2JobIntentEnvelope,
    request: SourceBrokerV2DispatchRequest,
    response: SourceBrokerV2DispatchResponse,
) -> None:
    if (
        response.saga_id != intent.claim.saga_id
        or response.operation_id != intent.operation_id
        or response.call_id != intent.quota.parent_id
        or response.request_hash != request.request_hash
    ):
        raise ValueError("dispatch response conflicts with the exact job operation")


def _require_finalize_binding(
    intent: SourceBrokerV2JobIntentEnvelope,
    request: SourceBrokerV2FinalizeRequest,
    response: SourceBrokerV2FinalizeResponse,
) -> None:
    if (
        response.saga_id != intent.claim.saga_id
        or response.operation_id != request.operation_id
        or response.request_hash != request.request_hash
    ):
        raise ValueError("finalize response conflicts with the exact job operation")


def _claim_subject_hash(intent: SourceBrokerV2JobIntentEnvelope) -> str:
    return canonical_job_sha256(
        {
            "authority": intent.claim.authority.model_dump(mode="python"),
            "claim": intent.claim.model_dump(mode="python"),
            "contract": "rquant-source-broker-v2-job-claim-subject/v2",
            "external_root_hash": intent.fence.external_root_hash,
            "external_claim_token_hash": intent.fence.claim_token_hash,
            "operation_hash": intent.operation_hash,
            "operation_id": intent.operation_id,
        }
    )


def _quota_subject_hash(intent: SourceBrokerV2JobIntentEnvelope) -> str:
    return canonical_job_sha256(
        {
            "authority": intent.quota.authority.model_dump(mode="python"),
            "claim_binding_hash": intent.claim.claim_binding_hash,
            "contract": "rquant-source-broker-v2-job-quota-subject/v2",
            "external_root_hash": intent.fence.external_root_hash,
            "external_claim_token_hash": intent.fence.claim_token_hash,
            "operation_hash": intent.operation_hash,
            "operation_id": intent.operation_id,
            "quota": intent.quota.model_dump(mode="python"),
        }
    )


def _lineage_subject_hash(
    intent: SourceBrokerV2JobIntentEnvelope,
    *,
    source_receipt_hash: str,
    claim_receipt_hash: str,
    quota_receipt_hash: str,
) -> str:
    return canonical_job_sha256(
        {
            "authority": intent.lineage.authority.model_dump(mode="python"),
            "claim_receipt_hash": claim_receipt_hash,
            "contract": "rquant-source-broker-v2-job-lineage-subject/v2",
            "external_root_hash": intent.fence.external_root_hash,
            "external_claim_token_hash": intent.fence.claim_token_hash,
            "lineage_id": intent.lineage.lineage_id,
            "operation_hash": intent.operation_hash,
            "operation_id": intent.operation_id,
            "quota_receipt_hash": quota_receipt_hash,
            "source_receipt_hash": source_receipt_hash,
        }
    )


def _require_deadline(deadline: float, stage: str) -> None:
    if time.monotonic() >= deadline:
        raise _RunnerDeadlineError(f"total monotonic deadline exceeded during {stage}")


def _parse_native(model: type[_T], payload: bytes) -> _T:
    return strict_model_validate_canonical_json(model, payload)


def _optional_bytes(value: Any) -> bytes | None:
    return None if value is None else bytes(value)


def _reconcile_code(exc: Exception) -> SourceBrokerV2ReconcileCode:
    if isinstance(exc, _RunnerDeadlineError):
        return SourceBrokerV2ReconcileCode.DEADLINE_EXCEEDED
    if isinstance(exc, SourceBrokerV2RunnerError):
        return SourceBrokerV2ReconcileCode.EXTERNAL_RECONCILE_REQUIRED
    if isinstance(exc, (PermissionError, TypeError, ValueError)):
        return SourceBrokerV2ReconcileCode.INTEGRITY_REJECTED
    return SourceBrokerV2ReconcileCode.INTERNAL_ERROR


def _reconcile_diagnostic(
    *,
    code: SourceBrokerV2ReconcileCode,
    exception_class: str,
    phase: str,
    operation_hash: str,
) -> str:
    safe_exception_class = (
        exception_class
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", exception_class)
        else "Exception"
    )
    if phase not in {
        "expired_dispatch",
        "finish_terminal",
        "process_new",
        "recover_reconcile",
    }:
        raise ValueError("reconcile diagnostic phase is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", operation_hash):
        raise ValueError("reconcile diagnostic operation hash is invalid")
    return canonical_json_bytes(
        {
            "code": code.value,
            "exception_class": safe_exception_class,
            "operation_hash": operation_hash,
            "phase": phase,
        }
    ).decode("utf-8")


def _encode_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")
