from __future__ import annotations

import fcntl
import hashlib
import inspect
import os
import re
import secrets
import sqlite3
import stat
import textwrap
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
from rquant.lab_shard_protocol import (
    StrategyShardPayload,
    StrategyShardPayloadV1,
    StrategyShardPayloadV2,
)
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2JobIntentEnvelope,
    SourceBrokerV2JobOutcomeEnvelope,
    SourceBrokerV2JobOutcomeStatus,
    canonical_job_model_bytes,
    parse_job_intent,
    parse_job_outcome,
)
from rquant.source_broker_v2_queue import SourceBrokerV2SchedulerQueue
from rquant.source_broker_v2_runner import (
    SourceBrokerV2JobRunnerState,
    SourceBrokerV2JobStoreConfig,
    load_source_broker_v2_job_store_config,
)
from rquant.source_operation_contracts import SourceAttemptBindingV2
from rquant.strict_json import canonical_model_json_bytes

if TYPE_CHECKING:
    from rquant.lab_jobs import CurrentSchedulerFenceReceipt, JobStoreSchedulerFenceVerifier

_SCHEMA_VERSION = 6
_ZERO_HASH = "0" * 64
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_SAFE_ID_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,198}[A-Za-z0-9])?$"
_REASON_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_STATE_VALUES = "'QUEUED', 'PENDING', 'READY', 'FAILED', 'RECONCILE_REQUIRED'"
_RECORD_COLUMNS = (
    "job_id",
    "shard_id",
    "claim_token",
    "attempt_id",
    "claim_generation",
    "scheduler_fencing_token",
    "scheduler_fence_receipt_commitment",
    "scheduler_fence_authority_commitment",
    "worker_id",
    "spec_hash",
    "plan_hash",
    "attempt_identity_hash",
    "binding_hash",
    "state",
    "intent",
    "intent_hash",
    "operation_id",
    "operation_hash",
    "outcome",
    "outcome_hash",
    "evidence_chain_hash",
    "writer_owner_id",
    "writer_lease_id",
    "writer_token",
    "writer_fencing_token",
    "writer_lease_expires_at",
    "terminal_reason",
    "created_at",
    "updated_at",
    "ready_at",
)
_BLOB_COLUMNS = frozenset({"intent", "outcome"})

_QUEUE_CLASS = SourceBrokerV2SchedulerQueue
_QUEUE_METHOD_NAMES = (
    "__init__",
    "get_state",
    "get_verified_published_outcome",
    "_connection",
    "_require_stored_intent_binding",
    "_require_outcome_binding",
)
_QUEUE_METHOD_DESCRIPTORS = {
    name: inspect.getattr_static(_QUEUE_CLASS, name) for name in _QUEUE_METHOD_NAMES
}
_QUEUE_INIT = _QUEUE_CLASS.__init__
_QUEUE_GET_STATE = _QUEUE_CLASS.get_state
_QUEUE_GET_VERIFIED_OUTCOME = _QUEUE_CLASS.get_verified_published_outcome


def _normalized_source_digest(source: str) -> str:
    normalized = textwrap.dedent(source.replace("\r\n", "\n").replace("\r", "\n"))
    lines = [line.rstrip(" \t") for line in normalized.split("\n")]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    payload = ("\n".join(lines) + "\n").encode()
    return hashlib.sha256(payload).hexdigest()


def _queue_source_implementation_digest(
    queue_class: type[SourceBrokerV2SchedulerQueue],
) -> str:
    try:
        source = inspect.getsource(queue_class)
    except (OSError, TypeError) as exc:
        raise RuntimeError("scheduler queue source is unavailable") from exc
    return canonical_sha256(
        {
            "class_source_sha256": _normalized_source_digest(source),
            "contract": "rquant-lab-source-stage-queue-implementation/v2",
            "module": queue_class.__module__,
            "qualname": queue_class.__qualname__,
            "trusted_methods": _QUEUE_METHOD_NAMES,
        }
    )


_QUEUE_IMPLEMENTATION_DIGEST = _queue_source_implementation_digest(_QUEUE_CLASS)
_TRUSTED_SCHEDULER_FENCE_DISPATCH_DIGEST = (
    "f48beea933f3c697c9313aa4b36581243493a8ada5ab44705e4b1cf9271987c9"
)


class LabSourceStageError(RuntimeError):
    """Base failure for durable source-stage state."""


class LabSourceStageConflictError(LabSourceStageError):
    """A logical attempt is already bound to different canonical evidence."""


class LabSourceStageLeaseFencedError(LabSourceStageError):
    """A mutation does not hold the current independent writer lease."""


class LabSourceStageTransitionError(LabSourceStageError):
    """The requested state transition is closed or invalid."""


class LabSourceStageOutcomeError(LabSourceStageError):
    """The narrow queue reader did not provide an exact PUBLISHED outcome."""


class LabSourceStageIntegrityError(LabSourceStageError):
    """Durable source-stage evidence is malformed or no longer self-consistent."""


class LabSourceStageAuthorityError(LabSourceStageIntegrityError):
    """The configured runner-store authority is absent, changed, or untrusted."""


class LabSourceStageState(StrEnum):
    NONE = "NONE"
    QUEUED = "QUEUED"
    PENDING = "PENDING"
    READY = "READY"
    FAILED = "FAILED"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
    )


class LabSourceStageQueueAuthority(_FrozenModel):
    canonical_db_path: str = Field(min_length=1)
    runner_schema_version: int = Field(strict=True, ge=1)
    runner_store_id: str = Field(pattern=_HASH_PATTERN)
    runner_max_inbox: int = Field(strict=True, ge=1, le=100_000)
    runner_config_hash: str = Field(pattern=_HASH_PATTERN)
    queue_implementation_digest: str = Field(pattern=_HASH_PATTERN)
    authority_digest: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def create(
        cls,
        *,
        canonical_db_path: Path,
        config: SourceBrokerV2JobStoreConfig,
        queue_implementation_digest: str,
    ) -> LabSourceStageQueueAuthority:
        values = {
            "canonical_db_path": str(canonical_db_path),
            "runner_schema_version": config.schema_version,
            "runner_store_id": config.store_id,
            "runner_max_inbox": config.max_inbox,
            "runner_config_hash": config.config_hash,
            "queue_implementation_digest": queue_implementation_digest,
        }
        return cls(
            **values,
            authority_digest=_queue_authority_digest(values),
        )

    @model_validator(mode="after")
    def validate_authority_digest(self) -> LabSourceStageQueueAuthority:
        if not Path(self.canonical_db_path).is_absolute():
            raise ValueError("queue authority path must be absolute")
        values = self.model_dump(exclude={"authority_digest"}, mode="python")
        if self.authority_digest != _queue_authority_digest(values):
            raise ValueError("queue authority digest is inconsistent")
        return self


class LabSourceStageStoreAuthority(_FrozenModel):
    canonical_stage_db_path: str = Field(min_length=1)
    stage_store_id: UUID
    canonical_queue_db_path: str = Field(min_length=1)
    queue_runner_schema_version: int = Field(strict=True, ge=1)
    queue_runner_store_id: str = Field(pattern=_HASH_PATTERN)
    queue_runner_max_inbox: int = Field(strict=True, ge=1, le=100_000)
    queue_runner_config_hash: str = Field(pattern=_HASH_PATTERN)
    queue_implementation_digest: str = Field(pattern=_HASH_PATTERN)
    queue_authority_digest: str = Field(pattern=_HASH_PATTERN)
    authority_hash: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def create(
        cls,
        *,
        canonical_stage_db_path: Path,
        stage_store_id: UUID,
        queue_authority: LabSourceStageQueueAuthority,
    ) -> LabSourceStageStoreAuthority:
        values = {
            "canonical_stage_db_path": str(canonical_stage_db_path),
            "stage_store_id": stage_store_id,
            "canonical_queue_db_path": queue_authority.canonical_db_path,
            "queue_runner_schema_version": queue_authority.runner_schema_version,
            "queue_runner_store_id": queue_authority.runner_store_id,
            "queue_runner_max_inbox": queue_authority.runner_max_inbox,
            "queue_runner_config_hash": queue_authority.runner_config_hash,
            "queue_implementation_digest": queue_authority.queue_implementation_digest,
            "queue_authority_digest": queue_authority.authority_digest,
        }
        return cls(**values, authority_hash=_store_authority_hash(values))

    @model_validator(mode="after")
    def validate_authority_hash(self) -> LabSourceStageStoreAuthority:
        if (
            not Path(self.canonical_stage_db_path).is_absolute()
            or not Path(self.canonical_queue_db_path).is_absolute()
        ):
            raise ValueError("store authority paths must be absolute")
        values = self.model_dump(exclude={"authority_hash"}, mode="python")
        if self.authority_hash != _store_authority_hash(values):
            raise ValueError("store authority hash is inconsistent")
        return self

    @property
    def queue_authority(self) -> LabSourceStageQueueAuthority:
        return LabSourceStageQueueAuthority(
            canonical_db_path=self.canonical_queue_db_path,
            runner_schema_version=self.queue_runner_schema_version,
            runner_store_id=self.queue_runner_store_id,
            runner_max_inbox=self.queue_runner_max_inbox,
            runner_config_hash=self.queue_runner_config_hash,
            queue_implementation_digest=self.queue_implementation_digest,
            authority_digest=self.queue_authority_digest,
        )


class LabSourceStageBinding(_FrozenModel):
    job_id: UUID
    shard_id: UUID
    claim_token: UUID
    attempt_id: UUID
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    worker_id: str = Field(pattern=_SAFE_ID_PATTERN, min_length=1, max_length=200)
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_attempt_alias(self) -> LabSourceStageBinding:
        if self.claim_token != self.attempt_id:
            raise ValueError("claim_token and attempt_id must identify the same attempt")
        return self

    @property
    def attempt_binding(self) -> SourceAttemptBindingV2:
        return SourceAttemptBindingV2(
            job_id=self.job_id,
            spec_hash=self.spec_hash,
            shard_id=self.shard_id,
            attempt_id=self.attempt_id,
            claim_generation=self.claim_generation,
            scheduler_fencing_token=self.scheduler_fencing_token,
            worker_id=self.worker_id,
        )

    @property
    def attempt_identity_hash(self) -> str:
        return self.attempt_binding.attempt_identity_hash

    @property
    def claim_token_hash(self) -> str:
        return canonical_sha256({"claim_token": self.claim_token})

    @property
    def binding_hash(self) -> str:
        return canonical_sha256(
            {
                "attempt_binding": self.attempt_binding,
                "claim_token": self.claim_token,
                "contract": "rquant-lab-source-stage-binding/v1",
                "plan_hash": self.plan_hash,
            }
        )


class LabSourceStageWriterLease(_FrozenModel):
    owner_id: str = Field(pattern=_SAFE_ID_PATTERN, min_length=1, max_length=200)
    lease_id: UUID
    token: UUID
    fencing_token: int = Field(strict=True, ge=1)
    adoption_request_id: UUID | None = None
    lease_commitment: str = Field(pattern=_HASH_PATTERN)
    acquired_at: datetime
    heartbeat_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def validate_times(self) -> LabSourceStageWriterLease:
        acquired = normalize_aware_utc(self.acquired_at)
        heartbeat = normalize_aware_utc(self.heartbeat_at)
        expires = normalize_aware_utc(self.expires_at)
        if heartbeat < acquired or expires <= heartbeat:
            raise ValueError("writer lease timestamps are invalid")
        object.__setattr__(self, "acquired_at", acquired)
        object.__setattr__(self, "heartbeat_at", heartbeat)
        object.__setattr__(self, "expires_at", expires)
        expected = _writer_lease_commitment(
            owner_id=self.owner_id,
            lease_id=self.lease_id,
            token=self.token,
            fencing_token=self.fencing_token,
            acquired_at=acquired,
            expires_at=expires,
            adoption_request_id=self.adoption_request_id,
        )
        if self.lease_commitment != expected:
            raise ValueError("writer lease commitment is invalid")
        return self

    @property
    def writer_fence(self) -> int:
        return self.fencing_token


class LabSourceStageRecord(_FrozenModel):
    binding: LabSourceStageBinding
    state: LabSourceStageState
    intent: SourceBrokerV2JobIntentEnvelope
    intent_bytes: bytes
    intent_hash: str = Field(pattern=_HASH_PATTERN)
    operation_id: str = Field(pattern=_HASH_PATTERN)
    operation_hash: str = Field(pattern=_HASH_PATTERN)
    outcome: SourceBrokerV2JobOutcomeEnvelope | None = None
    outcome_bytes: bytes | None = None
    outcome_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    evidence_chain_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    attempt_identity_hash: str = Field(pattern=_HASH_PATTERN)
    scheduler_fence_receipt_commitment: str | None = Field(default=None, pattern=_HASH_PATTERN)
    scheduler_fence_authority_commitment: str | None = Field(default=None, pattern=_HASH_PATTERN)
    writer_owner_id: str | None = None
    writer_lease_id: UUID | None = None
    writer_token: UUID | None = None
    writer_fencing_token: int | None = Field(default=None, strict=True, ge=1)
    writer_lease_expires_at: datetime | None = None
    terminal_reason: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]{0,63}$")
    created_at: datetime
    updated_at: datetime
    ready_at: datetime | None = None
    record_hash: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_evidence(self) -> LabSourceStageRecord:
        if (self.scheduler_fence_receipt_commitment is None) != (
            self.scheduler_fence_authority_commitment is None
        ):
            raise ValueError("scheduler fence receipt proof is incomplete")
        if canonical_job_model_bytes(self.intent) != self.intent_bytes:
            raise ValueError("intent bytes are not canonical")
        if (
            self.intent.intent_hash != self.intent_hash
            or self.intent.operation_id != self.operation_id
            or self.intent.operation_hash != self.operation_hash
        ):
            raise ValueError("intent identity conflicts with durable columns")
        if self.attempt_identity_hash != self.binding.attempt_identity_hash:
            raise ValueError("attempt identity conflicts with binding")
        _validate_intent_binding(self.binding, self.intent)
        outcome_values = (
            self.outcome,
            self.outcome_bytes,
            self.outcome_hash,
            self.evidence_chain_hash,
        )
        if self.state is LabSourceStageState.READY:
            if any(value is None for value in outcome_values) or self.ready_at is None:
                raise ValueError("READY requires complete published outcome evidence")
            assert self.outcome is not None
            assert self.outcome_bytes is not None
            if canonical_job_model_bytes(self.outcome) != self.outcome_bytes:
                raise ValueError("outcome bytes are not canonical")
            if (
                self.outcome.outcome_hash != self.outcome_hash
                or self.outcome.evidence_chain_hash != self.evidence_chain_hash
            ):
                raise ValueError("outcome identity conflicts with durable columns")
            _validate_outcome_binding(self.binding, self.intent, self.outcome)
        elif any(value is not None for value in outcome_values) or self.ready_at is not None:
            raise ValueError("non-READY state cannot expose outcome evidence")
        if self.state in {
            LabSourceStageState.FAILED,
            LabSourceStageState.RECONCILE_REQUIRED,
        }:
            if self.terminal_reason is None:
                raise ValueError("terminal source-stage state requires a reason code")
        elif self.terminal_reason is not None:
            raise ValueError("non-terminal source-stage state cannot expose a reason")
        if self.state is LabSourceStageState.PENDING and any(
            value is None
            for value in (
                self.writer_owner_id,
                self.writer_lease_id,
                self.writer_token,
                self.writer_fencing_token,
                self.writer_lease_expires_at,
            )
        ):
            raise ValueError("PENDING requires writer lease evidence")
        created = normalize_aware_utc(self.created_at)
        updated = normalize_aware_utc(self.updated_at)
        if updated < created:
            raise ValueError("source-stage timestamps are not monotonic")
        object.__setattr__(self, "created_at", created)
        object.__setattr__(self, "updated_at", updated)
        return self


class LabSourceStageEvent(_FrozenModel):
    event_id: int = Field(strict=True, ge=1)
    job_id: UUID
    shard_id: UUID
    attempt_id: UUID
    binding_hash: str = Field(pattern=_HASH_PATTERN)
    event_type: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    prior_state: LabSourceStageState
    new_state: LabSourceStageState
    writer_fencing_token: int = Field(strict=True, ge=1)
    record_hash: str = Field(pattern=_HASH_PATTERN)
    previous_event_hash: str = Field(pattern=_HASH_PATTERN)
    event_hash: str = Field(pattern=_HASH_PATTERN)
    created_at: datetime


class LabSourceStageWriterLeaseAdoption(_FrozenModel):
    audit_id: int = Field(strict=True, ge=1)
    request_id: UUID
    binding: LabSourceStageBinding
    owner_id: str = Field(pattern=_SAFE_ID_PATTERN, min_length=1, max_length=200)
    reason: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    old_lease_commitment: str = Field(pattern=_HASH_PATTERN)
    new_lease_commitment: str = Field(pattern=_HASH_PATTERN)
    old_writer_fence: int = Field(strict=True, ge=1)
    new_writer_fence: int = Field(strict=True, ge=1)
    scheduler_fence_receipt_commitment: str | None = Field(default=None, pattern=_HASH_PATTERN)
    previous_audit_hash: str = Field(pattern=_HASH_PATTERN)
    audit_hash: str = Field(pattern=_HASH_PATTERN)
    created_at: datetime


class LabSourceStageStore:
    """Independent single-writer evidence store for one Lab external-source stage."""

    __slots__ = (
        "_authorization_keyring",
        "_busy_timeout_ms",
        "_initial_authority_hash",
        "_manifest_keyring",
        "_path",
        "_queue_path_input",
    )

    def __init__(
        self,
        path: Path,
        *,
        queue_store_path: Path,
        busy_timeout_ms: int = 5_000,
        manifest_keyring: VerifyOnlyEd25519Keyring | None = None,
        authorization_keyring: VerifyOnlyEd25519Keyring | None = None,
    ) -> None:
        if not 1 <= busy_timeout_ms <= 120_000:
            raise ValueError("busy_timeout_ms must be between 1 and 120000")
        self._path = Path(os.path.abspath(path))
        self._busy_timeout_ms = busy_timeout_ms
        self._queue_path_input = Path(os.path.abspath(queue_store_path))
        if (manifest_keyring is None) != (authorization_keyring is None):
            raise TypeError("source-stage queue verification requires both public keyrings")
        if manifest_keyring is not None and type(manifest_keyring) is not VerifyOnlyEd25519Keyring:
            raise TypeError("source-stage manifest keyring must be verify-only")
        if (
            authorization_keyring is not None
            and type(authorization_keyring) is not VerifyOnlyEd25519Keyring
        ):
            raise TypeError("source-stage authorization keyring must be verify-only")
        self._manifest_keyring = manifest_keyring
        self._authorization_keyring = authorization_keyring
        authority = _read_queue_authority(
            self._queue_path_input,
            busy_timeout_ms=busy_timeout_ms,
        )
        canonical_stage_path = self._path.resolve(strict=False)
        self._initialize(authority, canonical_stage_path=canonical_stage_path)
        self._initial_authority_hash = self._verified_store_authority(
            enforce_initial_fence=False
        ).authority_hash

    @property
    def path(self) -> Path:
        return self._path

    @property
    def busy_timeout_ms(self) -> int:
        return self._busy_timeout_ms

    @property
    def queue_store_path(self) -> Path:
        return Path(self.authority.canonical_queue_db_path)

    @property
    def authority(self) -> LabSourceStageStoreAuthority:
        return self._verified_store_authority(enforce_initial_fence=True)

    def require_execution_intent(
        self,
        intent: SourceBrokerV2JobIntentEnvelope,
        *,
        now: datetime,
        allow_ready: bool = False,
    ) -> tuple[str, str]:
        """Return immutable proof for one exact executable or completed stage intent."""

        current = normalize_aware_utc(now)
        authority = self.authority
        try:
            canonical_intent = parse_job_intent(canonical_job_model_bytes(intent))
            with self._connection() as connection:
                rows = connection.execute(
                    "SELECT * FROM lab_source_stage WHERE operation_id = ?",
                    (canonical_intent.operation_id,),
                ).fetchall()
            if len(rows) != 1:
                raise ValueError("source-stage operation record is missing or ambiguous")
            record = self._record_from_row(rows[0])
            allowed_states = {LabSourceStageState.QUEUED, LabSourceStageState.PENDING}
            if allow_ready:
                allowed_states.add(LabSourceStageState.READY)
            if record.state not in allowed_states:
                raise ValueError("source-stage operation is not executable")
            if record.intent_bytes != canonical_job_model_bytes(canonical_intent):
                raise ValueError("source-stage operation intent differs from queue intent")
            if record.state is LabSourceStageState.PENDING and (
                record.writer_lease_expires_at is None or record.writer_lease_expires_at <= current
            ):
                raise ValueError("source-stage pending writer lease is stale")
            return authority.authority_hash, _execution_record_commitment(record)
        except LabSourceStageError:
            raise
        except Exception as exc:
            raise LabSourceStageAuthorityError(
                "source-stage execution record is unavailable or conflicts"
            ) from exc

    def acquire_writer_lease(
        self,
        *,
        owner_id: str,
        lease_seconds: float,
        now: datetime,
        scheduler_fence_receipt: CurrentSchedulerFenceReceipt | None = None,
        scheduler_fence_verifier: JobStoreSchedulerFenceVerifier | None = None,
        binding: LabSourceStageBinding | None = None,
    ) -> LabSourceStageWriterLease:
        if not re.fullmatch(_SAFE_ID_PATTERN, owner_id):
            raise ValueError("writer owner_id is invalid")
        if not 0 < lease_seconds <= 3_600:
            raise ValueError("lease_seconds must be between 0 and 3600")
        current_time = normalize_aware_utc(now)
        expires = current_time + timedelta(seconds=lease_seconds)
        with (
            self._scheduler_fence_guard(
                receipt=scheduler_fence_receipt,
                verifier=scheduler_fence_verifier,
                binding=binding,
                now=current_time,
            ),
            self._transaction() as connection,
        ):
            row = connection.execute(
                "SELECT * FROM lab_source_stage_writer_lease WHERE singleton = 1"
            ).fetchone()
            if row is not None and _decode_time(row["expires_at"]) > current_time:
                raise LabSourceStageLeaseFencedError("source-stage writer lease is already held")
            fencing_token = (
                int(scheduler_fence_receipt.scheduler_fencing_token)
                if scheduler_fence_receipt is not None
                else (1 if row is None else int(row["fencing_token"]) + 1)
            )
            if row is not None and fencing_token <= int(row["fencing_token"]):
                raise LabSourceStageLeaseFencedError("writer fence is not newer than stage lease")
            lease = _new_writer_lease(
                owner_id=owner_id,
                fencing_token=fencing_token,
                acquired_at=current_time,
                expires_at=expires,
            )
            connection.execute(
                """
                    INSERT INTO lab_source_stage_writer_lease (
                        singleton, owner_id, lease_id, token, fencing_token, adoption_request_id,
                        lease_commitment, acquired_at, heartbeat_at, expires_at
                    ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(singleton) DO UPDATE SET
                        owner_id = excluded.owner_id,
                        lease_id = excluded.lease_id,
                        token = excluded.token,
                        fencing_token = excluded.fencing_token,
                        adoption_request_id = excluded.adoption_request_id,
                        lease_commitment = excluded.lease_commitment,
                        acquired_at = excluded.acquired_at,
                        heartbeat_at = excluded.heartbeat_at,
                        expires_at = excluded.expires_at
                    """,
                (
                    lease.owner_id,
                    str(lease.lease_id),
                    str(lease.token),
                    lease.fencing_token,
                    str(lease.adoption_request_id)
                    if lease.adoption_request_id is not None
                    else None,
                    lease.lease_commitment,
                    _encode_time(lease.acquired_at),
                    _encode_time(lease.heartbeat_at),
                    _encode_time(lease.expires_at),
                ),
            )
        return lease

    def adopt_writer_lease(
        self,
        *,
        owner_id: str,
        scheduler_fence_receipt: CurrentSchedulerFenceReceipt,
        scheduler_fence_verifier: JobStoreSchedulerFenceVerifier,
        request_id: UUID,
        binding: LabSourceStageBinding,
        reason: str,
        lease_seconds: float,
        now: datetime,
    ) -> LabSourceStageWriterLease:
        """Atomically replace one live same-owner writer lease with a higher fence."""

        writer_fence = getattr(scheduler_fence_receipt, "scheduler_fencing_token", 0)
        if not re.fullmatch(_SAFE_ID_PATTERN, owner_id):
            raise LabSourceStageLeaseFencedError("scheduler fence receipt owner is invalid")
        if (
            not isinstance(writer_fence, int)
            or writer_fence < 1
            or binding.scheduler_fencing_token > writer_fence
        ):
            raise LabSourceStageLeaseFencedError("writer fence conflicts with current attempt")
        if _REASON_PATTERN.fullmatch(reason) is None:
            raise ValueError("adoption reason must be a redacted reason code")
        if not 0 < lease_seconds <= 3_600:
            raise ValueError("lease_seconds must be between 0 and 3600")
        current_time = normalize_aware_utc(now)
        _ = self.authority
        with (
            self._scheduler_fence_guard(
                receipt=scheduler_fence_receipt,
                verifier=scheduler_fence_verifier,
                binding=binding,
                now=current_time,
            ),
            self._transaction() as connection,
        ):
            row = connection.execute(
                "SELECT * FROM lab_source_stage_writer_lease WHERE singleton = 1"
            ).fetchone()
            if row is None:
                raise LabSourceStageLeaseFencedError("source-stage writer lease is unavailable")
            existing = _writer_lease_from_row(row)
            if existing.expires_at <= current_time:
                raise LabSourceStageLeaseFencedError("source-stage writer lease has expired")
            if existing.owner_id != owner_id:
                raise LabSourceStageLeaseFencedError(
                    "source-stage writer lease belongs to another owner"
                )
            if writer_fence < existing.fencing_token:
                raise LabSourceStageLeaseFencedError("writer fence is lower than the active lease")
            if writer_fence == existing.fencing_token:
                if existing.adoption_request_id == request_id:
                    return existing
                raise LabSourceStageLeaseFencedError(
                    "writer fence is not greater than the active lease"
                )
            record_row = self._row_for_binding(connection, binding)
            if record_row is None:
                raise LabSourceStageLeaseFencedError("writer adoption requires the current attempt")
            record = self._record_from_row(record_row)
            if record.state not in {LabSourceStageState.QUEUED, LabSourceStageState.PENDING}:
                raise LabSourceStageLeaseFencedError(
                    "writer adoption requires an active source stage"
                )
            replacement = _new_writer_lease(
                owner_id=owner_id,
                fencing_token=writer_fence,
                acquired_at=current_time,
                expires_at=current_time + timedelta(seconds=lease_seconds),
                adoption_request_id=request_id,
            )
            updated = connection.execute(
                """
                    UPDATE lab_source_stage_writer_lease SET
                        lease_id = ?, token = ?, fencing_token = ?, adoption_request_id = ?,
                        lease_commitment = ?, acquired_at = ?, heartbeat_at = ?, expires_at = ?
                    WHERE singleton = 1 AND owner_id = ? AND lease_id = ? AND token = ?
                      AND fencing_token = ? AND lease_commitment = ? AND expires_at > ?
                    """,
                (
                    str(replacement.lease_id),
                    str(replacement.token),
                    replacement.fencing_token,
                    str(request_id),
                    replacement.lease_commitment,
                    _encode_time(replacement.acquired_at),
                    _encode_time(replacement.heartbeat_at),
                    _encode_time(replacement.expires_at),
                    existing.owner_id,
                    str(existing.lease_id),
                    str(existing.token),
                    existing.fencing_token,
                    existing.lease_commitment,
                    _encode_time(current_time),
                ),
            )
            if updated.rowcount != 1:
                raise LabSourceStageLeaseFencedError("source-stage writer adoption CAS lost")
            if record.state is LabSourceStageState.PENDING:
                values = self._values_from_row(record_row)
                values.update(_lease_record_values(replacement))
                values.update(_scheduler_fence_record_values(scheduler_fence_receipt))
                values["updated_at"] = _encode_time(current_time)
                values["record_hash"] = _record_hash(values)
                self._update_record(connection, values)
                self._append_event(
                    connection,
                    binding=binding,
                    event_type="writer_adopted",
                    prior_state=LabSourceStageState.PENDING,
                    new_state=LabSourceStageState.PENDING,
                    writer_fencing_token=replacement.fencing_token,
                    record_hash=str(values["record_hash"]),
                    now=current_time,
                )
            self._append_writer_lease_adoption(
                connection,
                request_id=request_id,
                binding=binding,
                owner_id=owner_id,
                reason=reason,
                old_lease=existing,
                new_lease=replacement,
                now=current_time,
                scheduler_fence_receipt_commitment=str(
                    getattr(scheduler_fence_receipt, "receipt_commitment", "")
                ),
            )
        _ = self.authority
        return replacement

    def enqueue_external(
        self,
        binding: LabSourceStageBinding,
        intent: SourceBrokerV2JobIntentEnvelope,
        *,
        lease: LabSourceStageWriterLease,
        now: datetime,
        scheduler_fence_receipt: CurrentSchedulerFenceReceipt | None = None,
        scheduler_fence_verifier: JobStoreSchedulerFenceVerifier | None = None,
    ) -> LabSourceStageRecord:
        current_time = normalize_aware_utc(now)
        canonical_intent = _canonical_intent(binding, intent)
        with (
            self._scheduler_fence_guard(
                receipt=scheduler_fence_receipt,
                verifier=scheduler_fence_verifier,
                binding=binding,
                now=current_time,
            ),
            self._transaction() as connection,
        ):
            self._require_writer_lease(connection, lease, now=current_time, binding=binding)
            existing = self._row_for_binding(connection, binding)
            if existing is not None:
                record = self._record_from_row(existing)
                self._require_same_attempt(record, binding, canonical_intent)
                return record
            values = self._new_values(
                binding,
                canonical_intent,
                state=LabSourceStageState.QUEUED,
                now=current_time,
                scheduler_fence_receipt=scheduler_fence_receipt,
            )
            self._insert_record(connection, values)
            self._append_event(
                connection,
                binding=binding,
                event_type="queued",
                prior_state=LabSourceStageState.NONE,
                new_state=LabSourceStageState.QUEUED,
                writer_fencing_token=lease.fencing_token,
                record_hash=str(values["record_hash"]),
                now=current_time,
            )
            return self._record_from_values(values)

    def begin_external(
        self,
        binding: LabSourceStageBinding,
        intent: SourceBrokerV2JobIntentEnvelope,
        *,
        lease: LabSourceStageWriterLease,
        now: datetime,
        scheduler_fence_receipt: CurrentSchedulerFenceReceipt | None = None,
        scheduler_fence_verifier: JobStoreSchedulerFenceVerifier | None = None,
    ) -> LabSourceStageRecord:
        current_time = normalize_aware_utc(now)
        canonical_intent = _canonical_intent(binding, intent)
        with (
            self._scheduler_fence_guard(
                receipt=scheduler_fence_receipt,
                verifier=scheduler_fence_verifier,
                binding=binding,
                now=current_time,
            ),
            self._transaction() as connection,
        ):
            self._require_writer_lease(connection, lease, now=current_time, binding=binding)
            row = self._row_for_binding(connection, binding)
            if row is None:
                values = self._new_values(
                    binding,
                    canonical_intent,
                    state=LabSourceStageState.PENDING,
                    now=current_time,
                    lease=lease,
                    scheduler_fence_receipt=scheduler_fence_receipt,
                )
                self._insert_record(connection, values)
                prior_state = LabSourceStageState.NONE
            else:
                record = self._record_from_row(row)
                self._require_same_attempt(record, binding, canonical_intent)
                if record.state is LabSourceStageState.PENDING:
                    if (
                        record.writer_token == lease.token
                        and record.writer_fencing_token == lease.fencing_token
                    ):
                        return record
                    raise LabSourceStageLeaseFencedError(
                        "PENDING source stage belongs to another writer lease"
                    )
                if record.state is not LabSourceStageState.QUEUED:
                    raise LabSourceStageTransitionError(
                        f"cannot begin source stage from {record.state.value}"
                    )
                prior_state = record.state
                values = self._values_from_row(row)
                if record.state is not LabSourceStageState.QUEUED:
                    self._require_scheduler_fence_record(
                        record,
                        receipt=scheduler_fence_receipt,
                        verifier=scheduler_fence_verifier,
                    )
                values.update(
                    {
                        "state": LabSourceStageState.PENDING.value,
                        **_lease_record_values(lease),
                        "updated_at": _encode_time(current_time),
                    }
                )
                if scheduler_fence_receipt is not None:
                    values.update(_scheduler_fence_record_values(scheduler_fence_receipt))
                values["record_hash"] = _record_hash(values)
                self._update_record(connection, values)
            self._append_event(
                connection,
                binding=binding,
                event_type="pending",
                prior_state=prior_state,
                new_state=LabSourceStageState.PENDING,
                writer_fencing_token=lease.fencing_token,
                record_hash=str(values["record_hash"]),
                now=current_time,
            )
            return self._record_from_values(values)

    def bind_published_outcome(
        self,
        binding: LabSourceStageBinding,
        *,
        lease: LabSourceStageWriterLease,
        now: datetime,
        scheduler_fence_receipt: CurrentSchedulerFenceReceipt | None = None,
        scheduler_fence_verifier: JobStoreSchedulerFenceVerifier | None = None,
    ) -> LabSourceStageRecord:
        current_time = normalize_aware_utc(now)
        with self._connection() as connection:
            row = self._row_for_binding(connection, binding)
        if row is None:
            raise KeyError(binding.attempt_id)
        record = self._record_from_row(row)
        self._require_scheduler_fence_record(
            record,
            receipt=scheduler_fence_receipt,
            verifier=scheduler_fence_verifier,
        )
        queue = self._authorized_queue()
        try:
            state = _QUEUE_GET_STATE(queue, record.operation_id)
            if state is not SourceBrokerV2JobRunnerState.PUBLISHED:
                raise ValueError("source broker outcome is not PUBLISHED")
            outcome = SourceBrokerV2JobOutcomeEnvelope.model_validate(
                _QUEUE_GET_VERIFIED_OUTCOME(queue, record.operation_id),
                strict=True,
            )
            outcome_bytes = canonical_job_model_bytes(outcome)
            _validate_outcome_binding(binding, record.intent, outcome)
            if outcome.status is not SourceBrokerV2JobOutcomeStatus.SUCCESS:
                raise ValueError("published source outcome is not successful")
        except Exception as exc:
            raise LabSourceStageOutcomeError(
                "queue reader did not return the exact successful PUBLISHED outcome"
            ) from exc
        with (
            self._scheduler_fence_guard(
                receipt=scheduler_fence_receipt,
                verifier=scheduler_fence_verifier,
                binding=binding,
                now=current_time,
            ),
            self._transaction() as connection,
        ):
            row = self._row_for_binding(connection, binding)
            if row is None:
                raise KeyError(binding.attempt_id)
            current = self._record_from_row(row)
            self._require_scheduler_fence_record(
                current,
                receipt=scheduler_fence_receipt,
                verifier=scheduler_fence_verifier,
            )
            if current.state is LabSourceStageState.READY:
                if current.outcome_bytes == outcome_bytes:
                    return current
                raise LabSourceStageConflictError("READY is bound to another outcome")
            self._require_writer_lease(connection, lease, now=current_time, binding=binding)
            self._require_pending_owner(current, lease)
            values = self._values_from_row(row)
            values.update(
                {
                    "state": LabSourceStageState.READY.value,
                    "outcome": outcome_bytes,
                    "outcome_hash": outcome.outcome_hash,
                    "evidence_chain_hash": outcome.evidence_chain_hash,
                    "terminal_reason": None,
                    "updated_at": _encode_time(current_time),
                    "ready_at": _encode_time(current_time),
                }
            )
            values["record_hash"] = _record_hash(values)
            self._update_record(connection, values)
            self._append_event(
                connection,
                binding=binding,
                event_type="ready",
                prior_state=LabSourceStageState.PENDING,
                new_state=LabSourceStageState.READY,
                writer_fencing_token=lease.fencing_token,
                record_hash=str(values["record_hash"]),
                now=current_time,
            )
            return self._record_from_values(values)

    def _persisted_queue_authority(self) -> LabSourceStageQueueAuthority:
        return self._verified_store_authority(enforce_initial_fence=True).queue_authority

    def _verified_store_authority(
        self,
        *,
        enforce_initial_fence: bool,
    ) -> LabSourceStageStoreAuthority:
        try:
            with self._connection() as connection:
                rows = connection.execute(
                    "SELECT * FROM lab_source_stage_meta ORDER BY singleton"
                ).fetchall()
            if len(rows) != 1:
                raise ValueError("metadata authority cardinality is invalid")
            authority = _store_authority_from_meta_row(rows[0])
            canonical_stage_path = self.path.resolve(strict=True)
            if authority.canonical_stage_db_path != str(canonical_stage_path):
                raise ValueError("stage path conflicts with persisted authority")
            observed_queue = _read_queue_authority(
                self._queue_path_input,
                busy_timeout_ms=self.busy_timeout_ms,
            )
            if observed_queue != authority.queue_authority:
                raise ValueError("queue store conflicts with persisted authority")
            if enforce_initial_fence and authority.authority_hash != self._initial_authority_hash:
                raise ValueError("stage store identity changed after initialization")
            return authority
        except LabSourceStageAuthorityError:
            raise
        except Exception as exc:
            raise LabSourceStageAuthorityError(
                "source-stage store authority is unavailable or changed"
            ) from exc

    def _authorized_queue(self) -> SourceBrokerV2SchedulerQueue:
        _require_frozen_queue_implementation()
        persisted = self._persisted_queue_authority()
        observed = _read_queue_authority(
            self._queue_path_input,
            busy_timeout_ms=self.busy_timeout_ms,
        )
        if observed != persisted:
            raise LabSourceStageAuthorityError(
                "configured runner store conflicts with persisted queue authority"
            )
        if self._manifest_keyring is None or self._authorization_keyring is None:
            raise LabSourceStageAuthorityError(
                "authorized scheduler queue public keyrings are unavailable"
            )
        queue = object.__new__(_QUEUE_CLASS)
        try:
            _QUEUE_INIT(
                queue,
                Path(persisted.canonical_db_path),
                busy_timeout_ms=self.busy_timeout_ms,
                manifest_keyring=self._manifest_keyring,
                authorization_keyring=self._authorization_keyring,
                stage_store=self,
            )
        except Exception as exc:
            raise LabSourceStageAuthorityError(
                "authorized scheduler queue could not be constructed"
            ) from exc
        if type(queue) is not _QUEUE_CLASS:
            raise LabSourceStageAuthorityError("authorized queue implementation is not exact")
        return queue

    def mark_failed(
        self,
        binding: LabSourceStageBinding,
        *,
        code: str,
        lease: LabSourceStageWriterLease,
        now: datetime,
        scheduler_fence_receipt: CurrentSchedulerFenceReceipt | None = None,
        scheduler_fence_verifier: JobStoreSchedulerFenceVerifier | None = None,
    ) -> LabSourceStageRecord:
        return self._mark_terminal(
            binding,
            state=LabSourceStageState.FAILED,
            code=code,
            lease=lease,
            now=now,
            scheduler_fence_receipt=scheduler_fence_receipt,
            scheduler_fence_verifier=scheduler_fence_verifier,
        )

    def mark_reconcile_required(
        self,
        binding: LabSourceStageBinding,
        *,
        code: str,
        lease: LabSourceStageWriterLease,
        now: datetime,
        scheduler_fence_receipt: CurrentSchedulerFenceReceipt | None = None,
        scheduler_fence_verifier: JobStoreSchedulerFenceVerifier | None = None,
    ) -> LabSourceStageRecord:
        return self._mark_terminal(
            binding,
            state=LabSourceStageState.RECONCILE_REQUIRED,
            code=code,
            lease=lease,
            now=now,
            scheduler_fence_receipt=scheduler_fence_receipt,
            scheduler_fence_verifier=scheduler_fence_verifier,
        )

    def recover_expired_pending(
        self,
        *,
        lease: LabSourceStageWriterLease,
        now: datetime,
        scheduler_fence_proof_provider: (
            Callable[
                [LabSourceStageBinding],
                tuple[CurrentSchedulerFenceReceipt, JobStoreSchedulerFenceVerifier],
            ]
            | None
        ) = None,
    ) -> int:
        current_time = normalize_aware_utc(now)
        with self._transaction() as connection:
            self._require_writer_lease(connection, lease, now=current_time)
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM lab_source_stage WHERE state = 'PENDING' "
                "AND writer_lease_expires_at <= ? ORDER BY job_id, shard_id, attempt_id",
                (_encode_time(current_time),),
            ).fetchall()
        candidates: list[tuple[LabSourceStageBinding, object | None, object | None]] = []
        for row in rows:
            record = self._record_from_row(row)
            receipt: CurrentSchedulerFenceReceipt | None = None
            verifier: JobStoreSchedulerFenceVerifier | None = None
            if record.scheduler_fence_receipt_commitment is not None:
                if scheduler_fence_proof_provider is None:
                    self._require_scheduler_fence_record(record, receipt=None, verifier=None)
                try:
                    receipt, verifier = scheduler_fence_proof_provider(record.binding)
                except Exception as exc:
                    raise LabSourceStageLeaseFencedError(
                        "scheduler recovery fence proof is unavailable"
                    ) from exc
                self._require_scheduler_fence_record(
                    record,
                    receipt=receipt,
                    verifier=verifier,
                )
            candidates.append((record.binding, receipt, verifier))
        recovered = 0
        for binding, receipt, verifier in candidates:
            with (
                self._scheduler_fence_guard(
                    receipt=receipt,
                    verifier=verifier,
                    binding=binding,
                    now=current_time,
                ),
                self._transaction() as connection,
            ):
                self._require_writer_lease(connection, lease, now=current_time, binding=binding)
                row = self._row_for_binding(connection, binding)
                if row is None:
                    continue
                record = self._record_from_row(row)
                self._require_scheduler_fence_record(
                    record,
                    receipt=receipt,
                    verifier=verifier,
                )
                if (
                    record.state is not LabSourceStageState.PENDING
                    or record.writer_lease_expires_at is None
                    or record.writer_lease_expires_at > current_time
                ):
                    continue
                values = self._values_from_row(row)
                values.update(
                    {
                        "state": LabSourceStageState.RECONCILE_REQUIRED.value,
                        "terminal_reason": "writer_lease_expired",
                        "updated_at": _encode_time(current_time),
                    }
                )
                values["record_hash"] = _record_hash(values)
                self._update_record(connection, values)
                self._append_event(
                    connection,
                    binding=record.binding,
                    event_type="reconcile_required",
                    prior_state=LabSourceStageState.PENDING,
                    new_state=LabSourceStageState.RECONCILE_REQUIRED,
                    writer_fencing_token=lease.fencing_token,
                    record_hash=str(values["record_hash"]),
                    now=current_time,
                )
                recovered += 1
        return recovered

    def get(self, binding: LabSourceStageBinding) -> LabSourceStageRecord | None:
        with self._connection() as connection:
            row = self._row_for_binding(connection, binding)
        if row is None:
            return None
        record = self._record_from_row(row)
        if record.binding != binding:
            raise LabSourceStageConflictError("attempt key is bound to different identity")
        return record

    def is_claim_publishable(
        self,
        binding: LabSourceStageBinding | None,
        *,
        payload: StrategyShardPayload,
    ) -> bool:
        if isinstance(payload, StrategyShardPayloadV1):
            return True
        if not isinstance(payload, StrategyShardPayloadV2) or binding is None:
            return False
        try:
            record = self.get(binding)
        except LabSourceStageError:
            return False
        return record is not None and record.state is LabSourceStageState.READY

    def list_events(self, binding: LabSourceStageBinding) -> tuple[LabSourceStageEvent, ...]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM lab_source_stage_event WHERE job_id = ? AND shard_id = ? "
                "AND attempt_id = ? ORDER BY event_id",
                (str(binding.job_id), str(binding.shard_id), str(binding.attempt_id)),
            ).fetchall()
        events = tuple(_event_from_row(row) for row in rows)
        if any(event.binding_hash != binding.binding_hash for event in events):
            raise LabSourceStageIntegrityError("source-stage audit binding is inconsistent")
        return events

    def list_writer_lease_adoptions(
        self, binding: LabSourceStageBinding
    ) -> tuple[LabSourceStageWriterLeaseAdoption, ...]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM lab_source_stage_writer_lease_audit "
                "WHERE job_id = ? AND shard_id = ? AND attempt_id = ? ORDER BY audit_id",
                (str(binding.job_id), str(binding.shard_id), str(binding.attempt_id)),
            ).fetchall()
        adoptions = tuple(_writer_lease_adoption_from_row(row) for row in rows)
        if any(adoption.binding != binding for adoption in adoptions):
            raise LabSourceStageIntegrityError("writer lease adoption binding is inconsistent")
        return adoptions

    def verify_audit_chain(self) -> None:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM lab_source_stage_event ORDER BY event_id"
            ).fetchall()
        previous = _ZERO_HASH
        for row in rows:
            event = _event_from_row(row)
            expected = _event_hash(
                job_id=event.job_id,
                shard_id=event.shard_id,
                attempt_id=event.attempt_id,
                binding_hash=event.binding_hash,
                event_type=event.event_type,
                prior_state=event.prior_state,
                new_state=event.new_state,
                writer_fencing_token=event.writer_fencing_token,
                record_hash=event.record_hash,
                previous_event_hash=previous,
                created_at=event.created_at,
            )
            if event.previous_event_hash != previous or event.event_hash != expected:
                raise LabSourceStageIntegrityError("source-stage audit hash chain is invalid")
            previous = event.event_hash
        with self._connection() as connection:
            adoption_rows = connection.execute(
                "SELECT * FROM lab_source_stage_writer_lease_audit ORDER BY audit_id"
            ).fetchall()
        previous = _ZERO_HASH
        for row in adoption_rows:
            adoption = _writer_lease_adoption_from_row(row)
            expected = _writer_lease_adoption_hash(
                request_id=adoption.request_id,
                binding=adoption.binding,
                owner_id=adoption.owner_id,
                reason=adoption.reason,
                old_lease_commitment=adoption.old_lease_commitment,
                new_lease_commitment=adoption.new_lease_commitment,
                old_writer_fence=adoption.old_writer_fence,
                new_writer_fence=adoption.new_writer_fence,
                scheduler_fence_receipt_commitment=adoption.scheduler_fence_receipt_commitment,
                previous_audit_hash=previous,
                created_at=adoption.created_at,
            )
            if adoption.previous_audit_hash != previous or adoption.audit_hash != expected:
                raise LabSourceStageIntegrityError("writer lease adoption audit chain is invalid")
            previous = adoption.audit_hash

    @staticmethod
    def _append_writer_lease_adoption(
        connection: sqlite3.Connection,
        *,
        request_id: UUID,
        binding: LabSourceStageBinding,
        owner_id: str,
        reason: str,
        old_lease: LabSourceStageWriterLease,
        new_lease: LabSourceStageWriterLease,
        now: datetime,
        scheduler_fence_receipt_commitment: str,
    ) -> None:
        previous_row = connection.execute(
            "SELECT audit_hash FROM lab_source_stage_writer_lease_audit "
            "ORDER BY audit_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = _ZERO_HASH if previous_row is None else str(previous_row["audit_hash"])
        audit_hash = _writer_lease_adoption_hash(
            request_id=request_id,
            binding=binding,
            owner_id=owner_id,
            reason=reason,
            old_lease_commitment=old_lease.lease_commitment,
            new_lease_commitment=new_lease.lease_commitment,
            old_writer_fence=old_lease.fencing_token,
            new_writer_fence=new_lease.fencing_token,
            scheduler_fence_receipt_commitment=scheduler_fence_receipt_commitment,
            previous_audit_hash=previous_hash,
            created_at=now,
        )
        connection.execute(
            """
            INSERT INTO lab_source_stage_writer_lease_audit (
                request_id, job_id, shard_id, attempt_id, binding_hash, binding_json,
                owner_id, reason,
                old_lease_commitment, new_lease_commitment, old_writer_fence,
                new_writer_fence, scheduler_fence_receipt_commitment, previous_audit_hash,
                audit_hash, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(request_id),
                str(binding.job_id),
                str(binding.shard_id),
                str(binding.attempt_id),
                binding.binding_hash,
                canonical_model_json_bytes(binding).decode("utf-8"),
                owner_id,
                reason,
                old_lease.lease_commitment,
                new_lease.lease_commitment,
                old_lease.fencing_token,
                new_lease.fencing_token,
                scheduler_fence_receipt_commitment,
                previous_hash,
                audit_hash,
                _encode_time(now),
            ),
        )

    def _mark_terminal(
        self,
        binding: LabSourceStageBinding,
        *,
        state: LabSourceStageState,
        code: str,
        lease: LabSourceStageWriterLease,
        now: datetime,
        scheduler_fence_receipt: CurrentSchedulerFenceReceipt | None = None,
        scheduler_fence_verifier: JobStoreSchedulerFenceVerifier | None = None,
    ) -> LabSourceStageRecord:
        if _REASON_PATTERN.fullmatch(code) is None:
            raise ValueError("terminal reason must be a redacted reason code")
        current_time = normalize_aware_utc(now)
        with self._connection() as connection:
            row = self._row_for_binding(connection, binding)
        if row is None:
            raise KeyError(binding.attempt_id)
        self._require_scheduler_fence_record(
            self._record_from_row(row),
            receipt=scheduler_fence_receipt,
            verifier=scheduler_fence_verifier,
        )
        with (
            self._scheduler_fence_guard(
                receipt=scheduler_fence_receipt,
                verifier=scheduler_fence_verifier,
                binding=binding,
                now=current_time,
            ),
            self._transaction() as connection,
        ):
            self._require_writer_lease(connection, lease, now=current_time, binding=binding)
            row = self._row_for_binding(connection, binding)
            if row is None:
                raise KeyError(binding.attempt_id)
            record = self._record_from_row(row)
            self._require_scheduler_fence_record(
                record,
                receipt=scheduler_fence_receipt,
                verifier=scheduler_fence_verifier,
            )
            if record.state is state and record.terminal_reason == code:
                return record
            self._require_pending_owner(record, lease)
            values = self._values_from_row(row)
            values.update(
                {
                    "state": state.value,
                    "terminal_reason": code,
                    "updated_at": _encode_time(current_time),
                }
            )
            values["record_hash"] = _record_hash(values)
            self._update_record(connection, values)
            self._append_event(
                connection,
                binding=binding,
                event_type=state.value.lower(),
                prior_state=LabSourceStageState.PENDING,
                new_state=state,
                writer_fencing_token=lease.fencing_token,
                record_hash=str(values["record_hash"]),
                now=current_time,
            )
            return self._record_from_values(values)

    @staticmethod
    def _require_pending_owner(
        record: LabSourceStageRecord,
        lease: LabSourceStageWriterLease,
    ) -> None:
        if record.state is not LabSourceStageState.PENDING:
            raise LabSourceStageTransitionError(
                f"source-stage mutation requires PENDING, got {record.state.value}"
            )
        if (
            record.writer_owner_id != lease.owner_id
            or record.writer_lease_id != lease.lease_id
            or record.writer_token != lease.token
            or record.writer_fencing_token != lease.fencing_token
        ):
            raise LabSourceStageLeaseFencedError("source-stage PENDING owner is fenced")

    @staticmethod
    def _require_same_attempt(
        record: LabSourceStageRecord,
        binding: LabSourceStageBinding,
        canonical_intent: SourceBrokerV2JobIntentEnvelope,
    ) -> None:
        if record.binding != binding:
            raise LabSourceStageConflictError("attempt key is already bound differently")
        if record.intent_bytes != canonical_job_model_bytes(canonical_intent):
            raise LabSourceStageConflictError("attempt is already bound to another intent")

    @staticmethod
    @contextmanager
    def _scheduler_fence_guard(
        *,
        receipt: CurrentSchedulerFenceReceipt | None,
        verifier: JobStoreSchedulerFenceVerifier | None,
        binding: LabSourceStageBinding | None,
        now: datetime,
    ) -> Iterator[None]:
        if receipt is None and verifier is None:
            yield
            return
        if receipt is None or verifier is None or binding is None:
            raise LabSourceStageLeaseFencedError(
                "scheduler stage mutation requires an exact fence receipt"
            )
        from rquant.lab_jobs import (
            CurrentSchedulerFenceReceipt,
            JobStoreSchedulerFenceVerifier,
            hold_current_trusted_scheduler_fence,
        )

        if (
            type(receipt) is not CurrentSchedulerFenceReceipt
            or type(verifier) is not JobStoreSchedulerFenceVerifier
        ):
            raise LabSourceStageLeaseFencedError(
                "scheduler stage mutation requires a trusted JobStore verifier"
            )
        if receipt.binding != binding:
            raise LabSourceStageLeaseFencedError("scheduler fence receipt binding is not exact")
        try:
            if (
                hold_current_trusted_scheduler_fence.__module__ != "rquant.lab_jobs"
                or hold_current_trusted_scheduler_fence.__qualname__
                != "hold_current_trusted_scheduler_fence"
                or _normalized_source_digest(
                    inspect.getsource(hold_current_trusted_scheduler_fence)
                )
                != _TRUSTED_SCHEDULER_FENCE_DISPATCH_DIGEST
            ):
                raise LabSourceStageLeaseFencedError("scheduler fence dispatch is not trusted")
            with hold_current_trusted_scheduler_fence(
                verifier,
                receipt,
                binding=binding,
                now=now,
            ):
                yield
        except LabSourceStageError:
            raise
        except Exception as exc:
            raise LabSourceStageLeaseFencedError("scheduler fence receipt is not current") from exc

    @staticmethod
    def _require_scheduler_fence_record(
        record: LabSourceStageRecord,
        *,
        receipt: CurrentSchedulerFenceReceipt | None,
        verifier: JobStoreSchedulerFenceVerifier | None,
    ) -> None:
        persisted_receipt = record.scheduler_fence_receipt_commitment
        persisted_authority = record.scheduler_fence_authority_commitment
        if persisted_receipt is None and persisted_authority is None:
            return
        if receipt is None or verifier is None:
            raise LabSourceStageLeaseFencedError(
                "scheduler-origin source stage requires an exact fence receipt"
            )
        try:
            observed = _scheduler_fence_record_values(receipt)
        except (TypeError, ValueError, AttributeError) as exc:
            raise LabSourceStageLeaseFencedError(
                "scheduler fence receipt proof is invalid"
            ) from exc
        if (
            observed["scheduler_fence_receipt_commitment"] != persisted_receipt
            or observed["scheduler_fence_authority_commitment"] != persisted_authority
        ):
            raise LabSourceStageLeaseFencedError(
                "scheduler fence receipt does not match the persisted stage proof"
            )

    def _require_writer_lease(
        self,
        connection: sqlite3.Connection,
        lease: LabSourceStageWriterLease,
        *,
        now: datetime,
        binding: LabSourceStageBinding | None = None,
    ) -> None:
        row = connection.execute(
            "SELECT * FROM lab_source_stage_writer_lease WHERE singleton = 1"
        ).fetchone()
        if (
            row is None
            or str(row["owner_id"]) != lease.owner_id
            or str(row["lease_id"]) != str(lease.lease_id)
            or str(row["token"]) != str(lease.token)
            or int(row["fencing_token"]) != lease.fencing_token
            or str(row["lease_commitment"]) != lease.lease_commitment
            or _decode_time(row["expires_at"]) <= now
            or lease.expires_at <= now
        ):
            raise LabSourceStageLeaseFencedError("source-stage writer lease is stale or expired")
        if binding is not None and binding.scheduler_fencing_token > lease.fencing_token:
            raise LabSourceStageLeaseFencedError(
                "attempt scheduler fencing token conflicts with writer authority"
            )

    def _new_values(
        self,
        binding: LabSourceStageBinding,
        intent: SourceBrokerV2JobIntentEnvelope,
        *,
        state: LabSourceStageState,
        now: datetime,
        lease: LabSourceStageWriterLease | None = None,
        scheduler_fence_receipt: object | None = None,
    ) -> dict[str, object]:
        values: dict[str, object] = {
            "job_id": str(binding.job_id),
            "shard_id": str(binding.shard_id),
            "claim_token": str(binding.claim_token),
            "attempt_id": str(binding.attempt_id),
            "claim_generation": binding.claim_generation,
            "scheduler_fencing_token": binding.scheduler_fencing_token,
            **(
                _scheduler_fence_record_values(scheduler_fence_receipt)
                if scheduler_fence_receipt is not None
                else {
                    "scheduler_fence_receipt_commitment": None,
                    "scheduler_fence_authority_commitment": None,
                }
            ),
            "worker_id": binding.worker_id,
            "spec_hash": binding.spec_hash,
            "plan_hash": binding.plan_hash,
            "attempt_identity_hash": binding.attempt_identity_hash,
            "binding_hash": binding.binding_hash,
            "state": state.value,
            "intent": canonical_job_model_bytes(intent),
            "intent_hash": intent.intent_hash,
            "operation_id": intent.operation_id,
            "operation_hash": intent.operation_hash,
            "outcome": None,
            "outcome_hash": None,
            "evidence_chain_hash": None,
            "writer_owner_id": lease.owner_id if lease is not None else None,
            "writer_lease_id": str(lease.lease_id) if lease is not None else None,
            "writer_token": str(lease.token) if lease is not None else None,
            "writer_fencing_token": lease.fencing_token if lease is not None else None,
            "writer_lease_expires_at": (
                _encode_time(lease.expires_at) if lease is not None else None
            ),
            "terminal_reason": None,
            "created_at": _encode_time(now),
            "updated_at": _encode_time(now),
            "ready_at": None,
        }
        values["record_hash"] = _record_hash(values)
        return values

    @staticmethod
    def _row_for_binding(
        connection: sqlite3.Connection,
        binding: LabSourceStageBinding,
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM lab_source_stage WHERE job_id = ? AND shard_id = ? AND attempt_id = ?",
            (str(binding.job_id), str(binding.shard_id), str(binding.attempt_id)),
        ).fetchone()

    @staticmethod
    def _insert_record(connection: sqlite3.Connection, values: Mapping[str, object]) -> None:
        columns = (*_RECORD_COLUMNS, "record_hash")
        connection.execute(
            f"INSERT INTO lab_source_stage ({', '.join(columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})",
            tuple(values[column] for column in columns),
        )

    @staticmethod
    def _update_record(connection: sqlite3.Connection, values: Mapping[str, object]) -> None:
        identity = {"job_id", "shard_id", "attempt_id"}
        mutable = tuple(column for column in _RECORD_COLUMNS if column not in identity)
        updated = connection.execute(
            f"UPDATE lab_source_stage SET "
            f"{', '.join(f'{column} = ?' for column in mutable)}, record_hash = ? "
            "WHERE job_id = ? AND shard_id = ? AND attempt_id = ?",
            (
                *(values[column] for column in mutable),
                values["record_hash"],
                values["job_id"],
                values["shard_id"],
                values["attempt_id"],
            ),
        )
        if updated.rowcount != 1:
            raise LabSourceStageConflictError("source-stage CAS update lost its target")

    def _record_from_row(self, row: sqlite3.Row) -> LabSourceStageRecord:
        values = self._values_from_row(row)
        expected = _record_hash(values)
        if str(row["record_hash"]) != expected:
            raise LabSourceStageIntegrityError("source-stage record commitment is invalid")
        return self._record_from_values({**values, "record_hash": str(row["record_hash"])})

    @staticmethod
    def _values_from_row(row: sqlite3.Row) -> dict[str, object]:
        return {column: row[column] for column in _RECORD_COLUMNS}

    @staticmethod
    def _record_from_values(values: Mapping[str, object]) -> LabSourceStageRecord:
        try:
            binding = LabSourceStageBinding(
                job_id=UUID(str(values["job_id"])),
                shard_id=UUID(str(values["shard_id"])),
                claim_token=UUID(str(values["claim_token"])),
                attempt_id=UUID(str(values["attempt_id"])),
                claim_generation=int(values["claim_generation"]),
                scheduler_fencing_token=int(values["scheduler_fencing_token"]),
                worker_id=str(values["worker_id"]),
                spec_hash=str(values["spec_hash"]),
                plan_hash=str(values["plan_hash"]),
            )
            intent_bytes = bytes(values["intent"])
            intent = parse_job_intent(intent_bytes)
            outcome_bytes = bytes(values["outcome"]) if values.get("outcome") is not None else None
            outcome = parse_job_outcome(outcome_bytes) if outcome_bytes is not None else None
            return LabSourceStageRecord(
                binding=binding,
                state=LabSourceStageState(str(values["state"])),
                intent=intent,
                intent_bytes=intent_bytes,
                intent_hash=str(values["intent_hash"]),
                operation_id=str(values["operation_id"]),
                operation_hash=str(values["operation_hash"]),
                outcome=outcome,
                outcome_bytes=outcome_bytes,
                outcome_hash=(
                    str(values["outcome_hash"]) if values.get("outcome_hash") is not None else None
                ),
                evidence_chain_hash=(
                    str(values["evidence_chain_hash"])
                    if values.get("evidence_chain_hash") is not None
                    else None
                ),
                attempt_identity_hash=str(values["attempt_identity_hash"]),
                scheduler_fence_receipt_commitment=(
                    str(values["scheduler_fence_receipt_commitment"])
                    if values.get("scheduler_fence_receipt_commitment") is not None
                    else None
                ),
                scheduler_fence_authority_commitment=(
                    str(values["scheduler_fence_authority_commitment"])
                    if values.get("scheduler_fence_authority_commitment") is not None
                    else None
                ),
                writer_owner_id=(
                    str(values["writer_owner_id"])
                    if values.get("writer_owner_id") is not None
                    else None
                ),
                writer_lease_id=(
                    UUID(str(values["writer_lease_id"]))
                    if values.get("writer_lease_id") is not None
                    else None
                ),
                writer_token=(
                    UUID(str(values["writer_token"]))
                    if values.get("writer_token") is not None
                    else None
                ),
                writer_fencing_token=(
                    int(values["writer_fencing_token"])
                    if values.get("writer_fencing_token") is not None
                    else None
                ),
                writer_lease_expires_at=(
                    _decode_time(values["writer_lease_expires_at"])
                    if values.get("writer_lease_expires_at") is not None
                    else None
                ),
                terminal_reason=(
                    str(values["terminal_reason"])
                    if values.get("terminal_reason") is not None
                    else None
                ),
                created_at=_decode_time(values["created_at"]),
                updated_at=_decode_time(values["updated_at"]),
                ready_at=(
                    _decode_time(values["ready_at"]) if values.get("ready_at") is not None else None
                ),
                record_hash=str(values["record_hash"]),
            )
        except Exception as exc:
            raise LabSourceStageIntegrityError("source-stage row is malformed") from exc

    @staticmethod
    def _append_event(
        connection: sqlite3.Connection,
        *,
        binding: LabSourceStageBinding,
        event_type: str,
        prior_state: LabSourceStageState,
        new_state: LabSourceStageState,
        writer_fencing_token: int,
        record_hash: str,
        now: datetime,
    ) -> None:
        previous_row = connection.execute(
            "SELECT event_hash FROM lab_source_stage_event ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = _ZERO_HASH if previous_row is None else str(previous_row["event_hash"])
        event_hash = _event_hash(
            job_id=binding.job_id,
            shard_id=binding.shard_id,
            attempt_id=binding.attempt_id,
            binding_hash=binding.binding_hash,
            event_type=event_type,
            prior_state=prior_state,
            new_state=new_state,
            writer_fencing_token=writer_fencing_token,
            record_hash=record_hash,
            previous_event_hash=previous_hash,
            created_at=now,
        )
        connection.execute(
            """
            INSERT INTO lab_source_stage_event (
                job_id, shard_id, attempt_id, binding_hash, event_type,
                prior_state, new_state, writer_fencing_token, record_hash,
                previous_event_hash, event_hash, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(binding.job_id),
                str(binding.shard_id),
                str(binding.attempt_id),
                binding.binding_hash,
                event_type,
                prior_state.value,
                new_state.value,
                writer_fencing_token,
                record_hash,
                previous_hash,
                event_hash,
                _encode_time(now),
            ),
        )

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA synchronous = FULL")
            yield connection
        finally:
            connection.close()

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

    def _initialize(
        self,
        authority: LabSourceStageQueueAuthority,
        *,
        canonical_stage_path: Path,
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.path.with_name(f".{self.path.name}.schema.lock")
        with _bounded_file_lock(lock_path, timeout_ms=self.busy_timeout_ms):
            connection = sqlite3.connect(
                self.path,
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            try:
                connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
                journal = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
                if str(journal).lower() != "wal":
                    raise LabSourceStageIntegrityError("source-stage store requires WAL")
                connection.execute("PRAGMA synchronous = FULL")
                connection.execute("BEGIN IMMEDIATE")
                _initialize_schema(
                    connection,
                    authority,
                    canonical_stage_path=canonical_stage_path,
                )
                connection.execute("COMMIT")
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()


def _queue_authority_digest(values: Mapping[str, object]) -> str:
    return canonical_sha256(
        {
            "authority": dict(values),
            "contract": "rquant-lab-source-stage-queue-authority/v1",
        }
    )


def _store_authority_hash(values: Mapping[str, object]) -> str:
    return canonical_sha256(
        {
            "authority": dict(values),
            "contract": "rquant-lab-source-stage-store-authority/v1",
        }
    )


def _require_frozen_queue_implementation() -> None:
    for name, expected in _QUEUE_METHOD_DESCRIPTORS.items():
        if inspect.getattr_static(_QUEUE_CLASS, name) is not expected:
            raise LabSourceStageAuthorityError(f"scheduler queue implementation changed at {name}")
    observed_digest = _queue_source_implementation_digest(_QUEUE_CLASS)
    if observed_digest != _QUEUE_IMPLEMENTATION_DIGEST:
        raise LabSourceStageAuthorityError("scheduler queue implementation digest changed")


def _read_queue_authority(
    path: Path,
    *,
    busy_timeout_ms: int,
) -> LabSourceStageQueueAuthority:
    _require_frozen_queue_implementation()
    try:
        canonical_path = path.resolve(strict=True)
        if not stat.S_ISREG(canonical_path.stat().st_mode):
            raise ValueError("runner store authority is not a regular file")
        connection = sqlite3.connect(
            f"{canonical_path.as_uri()}?mode=ro",
            uri=True,
            timeout=busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            connection.execute("PRAGMA query_only = ON")
            config = load_source_broker_v2_job_store_config(connection)
        finally:
            connection.close()
        return LabSourceStageQueueAuthority.create(
            canonical_db_path=canonical_path,
            config=config,
            queue_implementation_digest=_QUEUE_IMPLEMENTATION_DIGEST,
        )
    except LabSourceStageAuthorityError:
        raise
    except Exception as exc:
        raise LabSourceStageAuthorityError(
            "runner store authority is unavailable or malformed"
        ) from exc


def _queue_authority_from_meta_row(row: Mapping[str, object]) -> LabSourceStageQueueAuthority:
    try:
        return LabSourceStageQueueAuthority(
            canonical_db_path=str(row["queue_db_path"]),
            runner_schema_version=int(row["queue_store_schema_version"]),
            runner_store_id=str(row["queue_store_id"]),
            runner_max_inbox=int(row["queue_max_inbox"]),
            runner_config_hash=str(row["queue_config_hash"]),
            queue_implementation_digest=str(row["queue_implementation_digest"]),
            authority_digest=str(row["queue_authority_digest"]),
        )
    except Exception as exc:
        raise LabSourceStageAuthorityError(
            "source-stage queue authority metadata is malformed"
        ) from exc


def _store_authority_from_meta_row(row: Mapping[str, object]) -> LabSourceStageStoreAuthority:
    try:
        return LabSourceStageStoreAuthority(
            canonical_stage_db_path=str(row["stage_db_path"]),
            stage_store_id=UUID(str(row["stage_store_id"])),
            canonical_queue_db_path=str(row["queue_db_path"]),
            queue_runner_schema_version=int(row["queue_store_schema_version"]),
            queue_runner_store_id=str(row["queue_store_id"]),
            queue_runner_max_inbox=int(row["queue_max_inbox"]),
            queue_runner_config_hash=str(row["queue_config_hash"]),
            queue_implementation_digest=str(row["queue_implementation_digest"]),
            queue_authority_digest=str(row["queue_authority_digest"]),
            authority_hash=str(row["store_authority_hash"]),
        )
    except Exception as exc:
        raise LabSourceStageAuthorityError(
            "source-stage store authority metadata is malformed"
        ) from exc


def _canonical_intent(
    binding: LabSourceStageBinding,
    intent: SourceBrokerV2JobIntentEnvelope,
) -> SourceBrokerV2JobIntentEnvelope:
    try:
        canonical = parse_job_intent(canonical_job_model_bytes(intent))
        _validate_intent_binding(binding, canonical)
        return canonical
    except Exception as exc:
        raise LabSourceStageConflictError("intent does not match source-stage attempt") from exc


def _validate_intent_binding(
    binding: LabSourceStageBinding,
    intent: SourceBrokerV2JobIntentEnvelope,
) -> None:
    if (
        intent.claim.claim_binding_hash != binding.binding_hash
        or intent.claim.claim_generation != binding.claim_generation
        or intent.claim.scheduler_fencing_token != binding.scheduler_fencing_token
        or intent.claim.attempt_identity_hash != binding.attempt_identity_hash
        or intent.claim.claim_plan_hash != binding.plan_hash
        or intent.fence.claim_token_hash != binding.claim_token_hash
    ):
        raise ValueError("intent claim/fence identity conflicts with source-stage binding")


def _validate_outcome_binding(
    binding: LabSourceStageBinding,
    intent: SourceBrokerV2JobIntentEnvelope,
    outcome: SourceBrokerV2JobOutcomeEnvelope,
) -> None:
    if (
        outcome.source_id != intent.source_id
        or outcome.operation_id != intent.operation_id
        or outcome.operation_hash != intent.operation_hash
        or outcome.source_authority != intent.source_authority
        or outcome.claim != intent.claim
        or outcome.quota != intent.quota
        or outcome.fence != intent.fence
        or outcome.lineage != intent.lineage
    ):
        raise ValueError("published outcome conflicts with canonical intent")
    _validate_intent_binding(binding, intent)


def _record_hash(values: Mapping[str, object]) -> str:
    missing = set(_RECORD_COLUMNS) - set(values)
    if missing:
        raise LabSourceStageIntegrityError(
            f"source-stage record commitment is missing columns: {sorted(missing)}"
        )
    normalized: dict[str, object] = {}
    for column in _RECORD_COLUMNS:
        value = values[column]
        if column in _BLOB_COLUMNS and value is not None:
            normalized[column] = hashlib.sha256(bytes(value)).hexdigest()
        else:
            normalized[column] = value
    return canonical_sha256(
        {
            "contract": "rquant-lab-source-stage-record/v1",
            "record": normalized,
        }
    )


def _scheduler_fence_record_values(receipt: object) -> dict[str, str]:
    try:
        receipt_commitment = receipt.receipt_commitment  # type: ignore[attr-defined]
        authority = {
            "application_id": receipt.application_id,  # type: ignore[attr-defined]
            "canonical_job_store_path": receipt.canonical_job_store_path,  # type: ignore[attr-defined]
            "database_generation": receipt.database_generation,  # type: ignore[attr-defined]
            "implementation_digest": receipt.implementation_digest,  # type: ignore[attr-defined]
            "schema_version": receipt.schema_version,  # type: ignore[attr-defined]
            "store_id": receipt.store_id,  # type: ignore[attr-defined]
        }
    except AttributeError as exc:
        raise ValueError("scheduler fence receipt authority is unavailable") from exc
    if (
        not isinstance(receipt_commitment, str)
        or re.fullmatch(_HASH_PATTERN, receipt_commitment) is None
    ):
        raise ValueError("scheduler fence receipt commitment is invalid")
    if (
        not isinstance(authority["canonical_job_store_path"], str)
        or not Path(authority["canonical_job_store_path"]).is_absolute()
        or not isinstance(authority["database_generation"], tuple)
        or len(authority["database_generation"]) != 2
        or any(type(value) is not int for value in authority["database_generation"])
        or type(authority["application_id"]) is not int
        or type(authority["schema_version"]) is not int
        or not isinstance(authority["implementation_digest"], str)
        or re.fullmatch(_HASH_PATTERN, authority["implementation_digest"]) is None
        or not isinstance(authority["store_id"], str)
        or re.fullmatch(_HASH_PATTERN, authority["store_id"]) is None
    ):
        raise ValueError("scheduler fence receipt authority is invalid")
    return {
        "scheduler_fence_receipt_commitment": receipt_commitment,
        "scheduler_fence_authority_commitment": canonical_sha256(
            {
                "authority": authority,
                "contract": "rquant-lab-source-stage-scheduler-fence-authority/v1",
            }
        ),
    }


def _writer_lease_commitment(
    *,
    owner_id: str,
    lease_id: UUID,
    token: UUID,
    fencing_token: int,
    acquired_at: datetime,
    expires_at: datetime,
    adoption_request_id: UUID | None,
) -> str:
    return canonical_sha256(
        {
            "acquired_at": normalize_aware_utc(acquired_at),
            "adoption_request_id": adoption_request_id,
            "contract": "rquant-lab-source-stage-writer-lease/v1",
            "expires_at": normalize_aware_utc(expires_at),
            "fencing_token": fencing_token,
            "lease_id": lease_id,
            "owner_id": owner_id,
            "token": token,
        }
    )


def _new_writer_lease(
    *,
    owner_id: str,
    fencing_token: int,
    acquired_at: datetime,
    expires_at: datetime,
    adoption_request_id: UUID | None = None,
) -> LabSourceStageWriterLease:
    lease_id = uuid4()
    token = uuid4()
    return LabSourceStageWriterLease(
        owner_id=owner_id,
        lease_id=lease_id,
        token=token,
        fencing_token=fencing_token,
        adoption_request_id=adoption_request_id,
        lease_commitment=_writer_lease_commitment(
            owner_id=owner_id,
            lease_id=lease_id,
            token=token,
            fencing_token=fencing_token,
            acquired_at=acquired_at,
            expires_at=expires_at,
            adoption_request_id=adoption_request_id,
        ),
        acquired_at=acquired_at,
        heartbeat_at=acquired_at,
        expires_at=expires_at,
    )


def _writer_lease_from_row(row: sqlite3.Row) -> LabSourceStageWriterLease:
    try:
        return LabSourceStageWriterLease(
            owner_id=str(row["owner_id"]),
            lease_id=UUID(str(row["lease_id"])),
            token=UUID(str(row["token"])),
            fencing_token=int(row["fencing_token"]),
            adoption_request_id=(
                UUID(str(row["adoption_request_id"]))
                if row["adoption_request_id"] is not None
                else None
            ),
            lease_commitment=str(row["lease_commitment"]),
            acquired_at=_decode_time(row["acquired_at"]),
            heartbeat_at=_decode_time(row["heartbeat_at"]),
            expires_at=_decode_time(row["expires_at"]),
        )
    except Exception as exc:
        raise LabSourceStageIntegrityError("source-stage writer lease is malformed") from exc


def _lease_record_values(lease: LabSourceStageWriterLease) -> dict[str, object]:
    return {
        "writer_owner_id": lease.owner_id,
        "writer_lease_id": str(lease.lease_id),
        "writer_token": str(lease.token),
        "writer_fencing_token": lease.fencing_token,
        "writer_lease_expires_at": _encode_time(lease.expires_at),
    }


def _execution_record_commitment(record: LabSourceStageRecord) -> str:
    """Commit only the immutable queued-attempt identity, not its QUEUED/PENDING state."""

    return canonical_sha256(
        {
            "attempt_identity_hash": record.attempt_identity_hash,
            "binding": record.binding.model_dump(mode="python"),
            "claim": record.intent.claim.model_dump(mode="python"),
            "contract": "rquant-lab-source-stage-execution-record/v1",
            "intent_hash": record.intent_hash,
            "operation_hash": record.operation_hash,
            "operation_id": record.operation_id,
            "scheduler_authorization": (
                record.intent.authorization.model_dump(mode="python")
                if record.intent.authorization is not None
                else None
            ),
            "template_commitment": record.intent.authorization_template_commitment,
        }
    )


def _event_hash(
    *,
    job_id: UUID,
    shard_id: UUID,
    attempt_id: UUID,
    binding_hash: str,
    event_type: str,
    prior_state: LabSourceStageState,
    new_state: LabSourceStageState,
    writer_fencing_token: int,
    record_hash: str,
    previous_event_hash: str,
    created_at: datetime,
) -> str:
    return canonical_sha256(
        {
            "attempt_id": attempt_id,
            "binding_hash": binding_hash,
            "contract": "rquant-lab-source-stage-event/v1",
            "created_at": created_at,
            "event_type": event_type,
            "job_id": job_id,
            "new_state": new_state,
            "previous_event_hash": previous_event_hash,
            "prior_state": prior_state,
            "record_hash": record_hash,
            "shard_id": shard_id,
            "writer_fencing_token": writer_fencing_token,
        }
    )


def _writer_lease_adoption_hash(
    *,
    request_id: UUID,
    binding: LabSourceStageBinding,
    owner_id: str,
    reason: str,
    old_lease_commitment: str,
    new_lease_commitment: str,
    old_writer_fence: int,
    new_writer_fence: int,
    scheduler_fence_receipt_commitment: str | None = None,
    previous_audit_hash: str,
    created_at: datetime,
) -> str:
    values: dict[str, object] = {
        "binding": binding.model_dump(mode="python"),
        "contract": "rquant-lab-source-stage-writer-adoption/v2"
        if scheduler_fence_receipt_commitment is not None
        else "rquant-lab-source-stage-writer-adoption/v1",
        "created_at": normalize_aware_utc(created_at),
        "new_lease_commitment": new_lease_commitment,
        "new_writer_fence": new_writer_fence,
        "old_lease_commitment": old_lease_commitment,
        "old_writer_fence": old_writer_fence,
        "owner_id": owner_id,
        "previous_audit_hash": previous_audit_hash,
        "reason": reason,
        "request_id": request_id,
    }
    if scheduler_fence_receipt_commitment is not None:
        values["scheduler_fence_receipt_commitment"] = scheduler_fence_receipt_commitment
    return canonical_sha256(values)


def _event_from_row(row: sqlite3.Row) -> LabSourceStageEvent:
    try:
        return LabSourceStageEvent(
            event_id=int(row["event_id"]),
            job_id=UUID(str(row["job_id"])),
            shard_id=UUID(str(row["shard_id"])),
            attempt_id=UUID(str(row["attempt_id"])),
            binding_hash=str(row["binding_hash"]),
            event_type=str(row["event_type"]),
            prior_state=LabSourceStageState(str(row["prior_state"])),
            new_state=LabSourceStageState(str(row["new_state"])),
            writer_fencing_token=int(row["writer_fencing_token"]),
            record_hash=str(row["record_hash"]),
            previous_event_hash=str(row["previous_event_hash"]),
            event_hash=str(row["event_hash"]),
            created_at=_decode_time(row["created_at"]),
        )
    except Exception as exc:
        raise LabSourceStageIntegrityError("source-stage audit event is malformed") from exc


def _writer_lease_adoption_from_row(row: sqlite3.Row) -> LabSourceStageWriterLeaseAdoption:
    try:
        return LabSourceStageWriterLeaseAdoption(
            audit_id=int(row["audit_id"]),
            request_id=UUID(str(row["request_id"])),
            binding=_binding_for_adoption_row(row),
            owner_id=str(row["owner_id"]),
            reason=str(row["reason"]),
            old_lease_commitment=str(row["old_lease_commitment"]),
            new_lease_commitment=str(row["new_lease_commitment"]),
            old_writer_fence=int(row["old_writer_fence"]),
            new_writer_fence=int(row["new_writer_fence"]),
            scheduler_fence_receipt_commitment=(
                str(row["scheduler_fence_receipt_commitment"])
                if "scheduler_fence_receipt_commitment" in set(row.keys())
                and row["scheduler_fence_receipt_commitment"] is not None
                else None
            ),
            previous_audit_hash=str(row["previous_audit_hash"]),
            audit_hash=str(row["audit_hash"]),
            created_at=_decode_time(row["created_at"]),
        )
    except Exception as exc:
        raise LabSourceStageIntegrityError(
            "writer lease adoption audit event is malformed"
        ) from exc


def _binding_for_adoption_row(row: sqlite3.Row) -> LabSourceStageBinding:
    binding = LabSourceStageBinding.model_validate_json(str(row["binding_json"]), strict=True)
    if (
        str(row["binding_hash"]) != binding.binding_hash
        or str(row["job_id"]) != str(binding.job_id)
        or str(row["shard_id"]) != str(binding.shard_id)
        or str(row["attempt_id"]) != str(binding.attempt_id)
    ):
        raise ValueError("writer adoption binding conflicts with durable columns")
    return binding


def _create_source_stage_meta_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE lab_source_stage_meta (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            schema_version INTEGER NOT NULL,
            store_id TEXT NOT NULL,
            stage_store_id TEXT NOT NULL,
            stage_db_path TEXT NOT NULL,
            queue_db_path TEXT NOT NULL,
            queue_store_schema_version INTEGER NOT NULL,
            queue_store_id TEXT NOT NULL,
            queue_max_inbox INTEGER NOT NULL,
            queue_config_hash TEXT NOT NULL,
            queue_implementation_digest TEXT NOT NULL,
            queue_authority_digest TEXT NOT NULL,
            store_authority_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )


def _insert_source_stage_meta(
    connection: sqlite3.Connection,
    *,
    authority: LabSourceStageQueueAuthority,
    canonical_stage_path: Path,
    stage_store_id: UUID,
    store_id: str,
    created_at: str,
) -> None:
    store_authority = LabSourceStageStoreAuthority.create(
        canonical_stage_db_path=canonical_stage_path,
        stage_store_id=stage_store_id,
        queue_authority=authority,
    )
    connection.execute(
        """
        INSERT INTO lab_source_stage_meta (
            singleton, schema_version, store_id, stage_store_id, stage_db_path, queue_db_path,
            queue_store_schema_version, queue_store_id, queue_max_inbox,
            queue_config_hash, queue_implementation_digest,
            queue_authority_digest, store_authority_hash, created_at
        ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _SCHEMA_VERSION,
            store_id,
            str(stage_store_id),
            str(canonical_stage_path),
            authority.canonical_db_path,
            authority.runner_schema_version,
            authority.runner_store_id,
            authority.runner_max_inbox,
            authority.runner_config_hash,
            authority.queue_implementation_digest,
            authority.authority_digest,
            store_authority.authority_hash,
            created_at,
        ),
    )


def _initialize_schema(
    connection: sqlite3.Connection,
    authority: LabSourceStageQueueAuthority,
    *,
    canonical_stage_path: Path,
) -> None:
    expected_meta_columns = {
        "singleton",
        "schema_version",
        "store_id",
        "stage_store_id",
        "stage_db_path",
        "queue_db_path",
        "queue_store_schema_version",
        "queue_store_id",
        "queue_max_inbox",
        "queue_config_hash",
        "queue_implementation_digest",
        "queue_authority_digest",
        "store_authority_hash",
        "created_at",
    }
    existing = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'lab_source_stage_meta'"
    ).fetchone()
    if existing is None:
        _create_source_stage_meta_table(connection)
    else:
        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(lab_source_stage_meta)")
        }
        if columns != expected_meta_columns:
            legacy_rows = connection.execute(
                "SELECT * FROM lab_source_stage_meta ORDER BY singleton"
            ).fetchall()
            if (
                not {"singleton", "schema_version", "store_id", "created_at"}.issubset(columns)
                or len(legacy_rows) != 1
                or int(legacy_rows[0]["singleton"]) != 1
                or int(legacy_rows[0]["schema_version"]) not in {0, 1, 2}
            ):
                raise LabSourceStageIntegrityError("source-stage metadata authority is malformed")
            legacy = legacy_rows[0]
            if int(legacy["schema_version"]) == 2 and (
                _queue_authority_from_meta_row(legacy) != authority
            ):
                raise LabSourceStageAuthorityError(
                    "v2 queue authority conflicts with configured runner store"
                )
            connection.execute(
                "ALTER TABLE lab_source_stage_meta RENAME TO lab_source_stage_meta_legacy"
            )
            _create_source_stage_meta_table(connection)
            _insert_source_stage_meta(
                connection,
                authority=authority,
                canonical_stage_path=canonical_stage_path,
                stage_store_id=uuid4(),
                store_id=str(legacy["store_id"]),
                created_at=str(legacy["created_at"]),
            )
            connection.execute("DROP TABLE lab_source_stage_meta_legacy")
        elif int(
            connection.execute("SELECT schema_version FROM lab_source_stage_meta").fetchone()[0]
        ) in {3, 4, 5}:
            connection.execute("DROP TRIGGER IF EXISTS lab_source_stage_meta_no_update")
            connection.execute(
                "UPDATE lab_source_stage_meta SET schema_version = ?", (_SCHEMA_VERSION,)
            )
    rows = connection.execute("SELECT * FROM lab_source_stage_meta ORDER BY singleton").fetchall()
    if not rows:
        _insert_source_stage_meta(
            connection,
            authority=authority,
            canonical_stage_path=canonical_stage_path,
            stage_store_id=uuid4(),
            store_id=secrets.token_hex(32),
            created_at=_encode_time(datetime.now(UTC)),
        )
        rows = connection.execute(
            "SELECT * FROM lab_source_stage_meta ORDER BY singleton"
        ).fetchall()
    if (
        len(rows) != 1
        or int(rows[0]["singleton"]) != 1
        or int(rows[0]["schema_version"]) != _SCHEMA_VERSION
    ):
        raise LabSourceStageIntegrityError("source-stage metadata authority is malformed")
    persisted_store_authority = _store_authority_from_meta_row(rows[0])
    if (
        persisted_store_authority.canonical_stage_db_path != str(canonical_stage_path)
        or persisted_store_authority.queue_authority != authority
    ):
        raise LabSourceStageAuthorityError(
            "configured paths conflict with persisted store authority"
        )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS lab_source_stage_writer_lease (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            owner_id TEXT NOT NULL,
            lease_id TEXT NOT NULL,
            token TEXT NOT NULL,
            fencing_token INTEGER NOT NULL CHECK (fencing_token >= 1),
            adoption_request_id TEXT,
            lease_commitment TEXT NOT NULL,
            acquired_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
        """
    )
    writer_lease_columns = {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(lab_source_stage_writer_lease)")
    }
    if "lease_id" not in writer_lease_columns:
        connection.execute("ALTER TABLE lab_source_stage_writer_lease ADD COLUMN lease_id TEXT")
        connection.execute(
            "ALTER TABLE lab_source_stage_writer_lease ADD COLUMN adoption_request_id TEXT"
        )
        connection.execute(
            "ALTER TABLE lab_source_stage_writer_lease ADD COLUMN lease_commitment TEXT"
        )
        rows = connection.execute(
            "SELECT * FROM lab_source_stage_writer_lease WHERE singleton = 1"
        ).fetchall()
        for row in rows:
            lease_id = uuid4()
            commitment = _writer_lease_commitment(
                owner_id=str(row["owner_id"]),
                lease_id=lease_id,
                token=UUID(str(row["token"])),
                fencing_token=int(row["fencing_token"]),
                acquired_at=_decode_time(row["acquired_at"]),
                expires_at=_decode_time(row["expires_at"]),
                adoption_request_id=None,
            )
            connection.execute(
                "UPDATE lab_source_stage_writer_lease SET lease_id = ?, lease_commitment = ? "
                "WHERE singleton = 1",
                (str(lease_id), commitment),
            )
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS lab_source_stage (
            job_id TEXT NOT NULL,
            shard_id TEXT NOT NULL,
            claim_token TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            claim_generation INTEGER NOT NULL CHECK (claim_generation >= 1),
            scheduler_fencing_token INTEGER NOT NULL CHECK (scheduler_fencing_token >= 1),
            scheduler_fence_receipt_commitment TEXT,
            scheduler_fence_authority_commitment TEXT,
            worker_id TEXT NOT NULL,
            spec_hash TEXT NOT NULL,
            plan_hash TEXT NOT NULL,
            attempt_identity_hash TEXT NOT NULL,
            binding_hash TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ({_STATE_VALUES})),
            intent BLOB NOT NULL,
            intent_hash TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            operation_hash TEXT NOT NULL,
            outcome BLOB,
            outcome_hash TEXT,
            evidence_chain_hash TEXT,
            writer_owner_id TEXT,
            writer_lease_id TEXT,
            writer_token TEXT,
            writer_fencing_token INTEGER,
            writer_lease_expires_at TEXT,
            terminal_reason TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            ready_at TEXT,
            record_hash TEXT NOT NULL,
            PRIMARY KEY (job_id, shard_id, attempt_id),
            UNIQUE (job_id, shard_id, claim_token),
            UNIQUE (operation_id),
            CHECK (claim_token = attempt_id)
        )
        """
    )
    stage_columns = {
        str(row["name"]) for row in connection.execute("PRAGMA table_info(lab_source_stage)")
    }
    if "scheduler_fence_receipt_commitment" not in stage_columns:
        connection.execute(
            "ALTER TABLE lab_source_stage ADD COLUMN scheduler_fence_receipt_commitment TEXT"
        )
        connection.execute(
            "ALTER TABLE lab_source_stage ADD COLUMN scheduler_fence_authority_commitment TEXT"
        )
        stage_columns.update(
            {"scheduler_fence_receipt_commitment", "scheduler_fence_authority_commitment"}
        )
        for row in connection.execute("SELECT * FROM lab_source_stage").fetchall():
            values = {column: row[column] for column in _RECORD_COLUMNS}
            connection.execute(
                "UPDATE lab_source_stage SET record_hash = ? "
                "WHERE job_id = ? AND shard_id = ? AND attempt_id = ?",
                (
                    _record_hash(values),
                    values["job_id"],
                    values["shard_id"],
                    values["attempt_id"],
                ),
            )
    if "writer_lease_id" not in stage_columns:
        connection.execute("ALTER TABLE lab_source_stage ADD COLUMN writer_lease_id TEXT")
        lease_row = connection.execute(
            "SELECT * FROM lab_source_stage_writer_lease WHERE singleton = 1"
        ).fetchone()
        rows = connection.execute("SELECT * FROM lab_source_stage").fetchall()
        for row in rows:
            values = {column: row[column] for column in _RECORD_COLUMNS}
            if (
                lease_row is not None
                and values["writer_owner_id"] == lease_row["owner_id"]
                and values["writer_token"] == lease_row["token"]
            ):
                values["writer_lease_id"] = lease_row["lease_id"]
            elif values["state"] == LabSourceStageState.PENDING.value:
                values["state"] = LabSourceStageState.RECONCILE_REQUIRED.value
                values["terminal_reason"] = "writer_lease_migration_unbound"
            record_hash = _record_hash(values)
            connection.execute(
                "UPDATE lab_source_stage SET writer_lease_id = ?, state = ?, terminal_reason = ?, "
                "record_hash = ? WHERE job_id = ? AND shard_id = ? AND attempt_id = ?",
                (
                    values["writer_lease_id"],
                    values["state"],
                    values["terminal_reason"],
                    record_hash,
                    values["job_id"],
                    values["shard_id"],
                    values["attempt_id"],
                ),
            )
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS lab_source_stage_event (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            shard_id TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            binding_hash TEXT NOT NULL,
            event_type TEXT NOT NULL,
            prior_state TEXT NOT NULL CHECK (prior_state IN ('NONE', {_STATE_VALUES})),
            new_state TEXT NOT NULL CHECK (new_state IN ({_STATE_VALUES})),
            writer_fencing_token INTEGER NOT NULL CHECK (writer_fencing_token >= 1),
            record_hash TEXT NOT NULL,
            previous_event_hash TEXT NOT NULL,
            event_hash TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS lab_source_stage_writer_lease_audit (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL UNIQUE,
            job_id TEXT NOT NULL,
            shard_id TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            binding_hash TEXT NOT NULL,
            binding_json TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            old_lease_commitment TEXT NOT NULL,
            new_lease_commitment TEXT NOT NULL,
            old_writer_fence INTEGER NOT NULL CHECK (old_writer_fence >= 1),
            new_writer_fence INTEGER NOT NULL CHECK (new_writer_fence >= 1),
            scheduler_fence_receipt_commitment TEXT,
            previous_audit_hash TEXT NOT NULL,
            audit_hash TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        )
        """
    )
    adoption_columns = {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(lab_source_stage_writer_lease_audit)")
    }
    if "scheduler_fence_receipt_commitment" not in adoption_columns:
        connection.execute(
            "ALTER TABLE lab_source_stage_writer_lease_audit "
            "ADD COLUMN scheduler_fence_receipt_commitment TEXT"
        )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS lab_source_stage_writer_lease_audit_attempt "
        "ON lab_source_stage_writer_lease_audit (job_id, shard_id, attempt_id, audit_id)"
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS lab_source_stage_ready_immutable
        BEFORE UPDATE ON lab_source_stage
        WHEN OLD.state = 'READY'
        BEGIN
            SELECT RAISE(ABORT, 'READY source-stage evidence is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS lab_source_stage_ready_no_delete
        BEFORE DELETE ON lab_source_stage
        WHEN OLD.state = 'READY'
        BEGIN
            SELECT RAISE(ABORT, 'READY source-stage evidence is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS lab_source_stage_meta_no_update
        BEFORE UPDATE ON lab_source_stage_meta
        BEGIN
            SELECT RAISE(ABORT, 'source-stage metadata authority is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER IF NOT EXISTS lab_source_stage_meta_no_delete
        BEFORE DELETE ON lab_source_stage_meta
        BEGIN
            SELECT RAISE(ABORT, 'source-stage metadata authority is immutable');
        END
        """
    )
    connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")


@contextmanager
def _bounded_file_lock(path: Path, *, timeout_ms: int) -> Iterator[None]:
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    deadline = time.monotonic() + timeout_ms / 1_000
    acquired = False
    try:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("source-stage schema lock timed out") from None
                time.sleep(min(0.01, remaining))
        yield
    finally:
        if acquired:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _encode_time(value: datetime) -> str:
    return normalize_aware_utc(value).isoformat(timespec="microseconds")


def _decode_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("durable timestamp must be text")
    return normalize_aware_utc(datetime.fromisoformat(value))
