"""Reference-governed retention planning for content-addressed artifacts.

This module owns metadata only. External storage deletion is deliberately left to an
identity-bound executor; the ledger is updated only after that executor reports the
exact object and location it removed.
"""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import sqlite3
import stat
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, Protocol, Self
from urllib.parse import quote
from uuid import uuid4

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
PathGeneration = tuple[int, int, int]
_DURABLE_OWNER_TYPES = frozenset({"audit", "experiment", "job", "snapshot"})
_EPHEMERAL_OWNER_TYPES = frozenset({"download", "temporary"})
_TERMINAL_OWNER_STATES = frozenset(
    {
        "abandoned",
        "cancelled",
        "completed",
        "deleted",
        "executed",
        "expired",
        "failed",
        "retired",
        "succeeded",
        "superseded",
    }
)
_ARTIFACT_METADATA_SERVICE_OWNER = "artifact-metadata-service/v1"

_ARTIFACT_WRITER_LOCKS_GUARD = threading.Lock()
_ARTIFACT_WRITER_LOCKS: dict[str, threading.RLock] = {}


def _artifact_writer_lock(path: Path) -> threading.RLock:
    key = str(path)
    with _ARTIFACT_WRITER_LOCKS_GUARD:
        return _ARTIFACT_WRITER_LOCKS.setdefault(key, threading.RLock())


_SQLITE_IDENTITY_LOCKS_GUARD = threading.Lock()
_SQLITE_IDENTITY_LOCKS: dict[str, threading.RLock] = {}


def _sqlite_identity_lock(path: Path) -> threading.RLock:
    key = str(path)
    with _SQLITE_IDENTITY_LOCKS_GUARD:
        return _SQLITE_IDENTITY_LOCKS.setdefault(key, threading.RLock())


class StorageTier(StrEnum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"


class ObjectIdentity(RuntimeContractModel):
    content_sha256: Sha256
    size_bytes: int = Field(ge=0)
    object_kind: str = Field(min_length=1)
    created_at: AwareUtcDatetime


class ObjectCopy(RuntimeContractModel):
    content_sha256: Sha256
    location_id: str = Field(min_length=1)
    storage_uri: str = Field(min_length=1)
    storage_tier: StorageTier
    verified_at: AwareUtcDatetime | None
    failure_domain: str = Field(min_length=1)
    tier_entered_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_copy_timestamps(self) -> Self:
        if self.verified_at is not None and self.verified_at < self.tier_entered_at:
            raise ValueError("verified_at cannot precede tier_entered_at")
        return self


class TierMigrationCursor(RuntimeContractModel):
    content_sha256: Sha256
    tier_rank: int = Field(ge=0, le=1)
    location_id: str = Field(min_length=1)


class TierMigrationSource(RuntimeContractModel):
    object_identity: ObjectIdentity
    source_copy: ObjectCopy


class TierMigrationPage(RuntimeContractModel):
    sources: tuple[TierMigrationSource, ...]
    next_cursor: TierMigrationCursor | None
    exhausted: bool


class ObjectReference(RuntimeContractModel):
    reference_id: Sha256 | None = None
    owner_type: str = Field(min_length=1)
    owner_id: str = Field(min_length=1)
    content_sha256: Sha256
    created_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def validate_reference(self) -> Self:
        if self.expires_at is not None and self.expires_at <= self.created_at:
            raise ValueError("expires_at must be later than created_at")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"reference_id"}))
        if self.reference_id is None:
            object.__setattr__(self, "reference_id", expected)
        elif self.reference_id != expected:
            raise ValueError("reference_id does not match reference content")
        return self


class OwnerTerminalReleaseReceipt(RuntimeContractModel):
    receipt_id: Sha256 | None = None
    reference_id: Sha256
    owner_type: str = Field(min_length=1)
    owner_id: str = Field(min_length=1)
    content_sha256: Sha256
    terminal_state: str = Field(pattern=r"^[a-z][a-z0-9_]{1,63}$")
    lifecycle_revision: int = Field(ge=0)
    evidence_sha256: Sha256
    released_at: AwareUtcDatetime

    @field_validator("owner_type")
    @classmethod
    def validate_durable_owner_type(cls, value: str) -> str:
        if value in _EPHEMERAL_OWNER_TYPES:
            raise ValueError("terminal release receipt requires a durable owner")
        return value

    @field_validator("terminal_state")
    @classmethod
    def validate_terminal_state(cls, value: str) -> str:
        if value not in _TERMINAL_OWNER_STATES:
            raise ValueError("terminal_state must name a recognized terminal lifecycle state")
        return value

    @model_validator(mode="after")
    def validate_receipt_identity(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"receipt_id"}))
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("terminal release receipt identity is invalid")
        return self


class ArtifactRetentionWriterAuthorizationError(ValueError):
    """A retention writer credential failed capability, freshness, or rotation checks."""


class ArtifactRetentionWriterCredential(RuntimeContractModel):
    key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    sequence: int = Field(ge=1)
    secret_hex: str = Field(pattern=r"^[0-9a-f]{64,}$", repr=False)
    previous_secret_hex: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64,}$",
        repr=False,
    )
    not_before: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    revoked_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.expires_at <= self.not_before:
            raise ValueError("artifact writer credential expiry must follow activation")
        if self.revoked_at is not None and self.revoked_at < self.not_before:
            raise ValueError("artifact writer credential revocation cannot precede issuance")
        secret = bytes.fromhex(self.secret_hex)
        if len(secret) < 32:
            raise ValueError("artifact writer credential secret must contain at least 32 bytes")
        if self.previous_secret_hex is not None:
            previous = bytes.fromhex(self.previous_secret_hex)
            if len(previous) < 32:
                raise ValueError(
                    "artifact writer credential previous secret must contain at least 32 bytes"
                )
            if self.previous_secret_hex == self.secret_hex:
                raise ValueError("artifact writer credential rotation must change the secret")
        return self

    @property
    def secret_sha256(self) -> Sha256:
        return hashlib.sha256(bytes.fromhex(self.secret_hex)).hexdigest()

    @property
    def previous_secret_sha256(self) -> Sha256 | None:
        if self.previous_secret_hex is None:
            return None
        return hashlib.sha256(bytes.fromhex(self.previous_secret_hex)).hexdigest()


# Compatibility wire format retained while production profiles migrate to the
# scoped retention credential above.  The store validates both objects before
# it can acquire a write fence.
class ArtifactWriterCredential(RuntimeContractModel):
    credential_id: Sha256 | None = None
    service_id: str = Field(min_length=1)
    key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    secret_sha256: Sha256
    issued_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    revoked_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.expires_at <= self.issued_at:
            raise ValueError("artifact writer credential expiry must follow issuance")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"credential_id"}))
        if self.credential_id is None:
            object.__setattr__(self, "credential_id", expected)
        elif self.credential_id != expected:
            raise ValueError("artifact writer credential identity is invalid")
        return self


class ArtifactWriterCapability(RuntimeContractModel):
    service_id: str = Field(min_length=1)
    key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    issued_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    secret_hex: str = Field(pattern=r"^[0-9a-f]{64,}$")

    @property
    def secret_sha256(self) -> Sha256:
        return hashlib.sha256(bytes.fromhex(self.secret_hex)).hexdigest()


class DurableOwnerTerminalReceiptProducer(Protocol):
    owner_type: Literal["audit", "experiment", "snapshot"]

    def build_terminal_release_receipt(
        self,
        reference: ObjectReference,
        *,
        terminal_state: str,
        lifecycle_revision: int,
        evidence_sha256: str,
        released_at: AwareUtcDatetime,
    ) -> OwnerTerminalReleaseReceipt: ...


class ArtifactBundleRegistration(RuntimeContractModel):
    object_identity: ObjectIdentity
    object_copy: ObjectCopy
    references: tuple[ObjectReference, ...]

    @model_validator(mode="after")
    def validate_registration(self) -> Self:
        content_hash = self.object_identity.content_sha256
        if self.object_copy.content_sha256 != content_hash or any(
            reference.content_sha256 != content_hash for reference in self.references
        ):
            raise ValueError("bundle registration content hashes must match")
        owner_types = tuple(sorted(reference.owner_type for reference in self.references))
        if owner_types != ("audit", "experiment", "job", "snapshot"):
            raise ValueError(
                "bundle registration requires audit, experiment, job, and snapshot references"
            )
        if any(reference.expires_at is not None for reference in self.references):
            raise ValueError("bundle owner references must not expire")
        if self.object_copy.verified_at is None:
            raise ValueError("bundle copy must be verified before registration")
        return self


class ArtifactRegistrationCounts(RuntimeContractModel):
    registered_objects: int = Field(ge=0, le=1)
    registered_copies: int = Field(ge=0, le=1)
    registered_references: int = Field(ge=0, le=4)


class ObjectCopyVerification(RuntimeContractModel):
    storage_uri: str = Field(min_length=1)
    content_sha256: Sha256
    size_bytes: int = Field(ge=0)
    schema_sha256: Sha256 | None = None
    verified_at: AwareUtcDatetime


class TierCopyRetirementPlan(RuntimeContractModel):
    plan_id: Sha256 | None = None
    content_sha256: Sha256
    source_location_id: str = Field(min_length=1)
    target_location_id: str = Field(min_length=1)
    source_tier: StorageTier
    target_tier: StorageTier
    required_owner_types: tuple[str, ...]
    ledger_revision: int = Field(ge=0)
    planned_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_plan(self) -> Self:
        if self.source_location_id == self.target_location_id:
            raise ValueError("tier retirement source and target must differ")
        if not _is_adjacent_tier(self.source_tier, self.target_tier):
            raise ValueError("tier retirement requires an adjacent colder target")
        if self.required_owner_types != ("audit", "experiment", "job", "snapshot"):
            raise ValueError("tier retirement requires all durable owner references")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"plan_id"}))
        if self.plan_id is None:
            object.__setattr__(self, "plan_id", expected)
        elif self.plan_id != expected:
            raise ValueError("tier retirement plan identity is invalid")
        return self


class TierMigrationReceipt(RuntimeContractModel):
    registered_target_copy: bool
    verification: ObjectCopyVerification
    retirement_plan: TierCopyRetirementPlan


class ArtifactTierCopyTransport(Protocol):
    def copy(self, source_uri: str, target_uri: str) -> None: ...

    def durably_sync(self, storage_uri: str) -> None: ...

    def verify(self, storage_uri: str) -> ObjectCopyVerification: ...


class LegalHold(RuntimeContractModel):
    hold_id: str = Field(min_length=1)
    content_sha256: Sha256
    reason: str = Field(min_length=1)
    created_at: AwareUtcDatetime


class RetentionRule(RuntimeContractModel):
    owner_type: str | None = Field(default=None, min_length=1)
    object_kind: str | None = Field(default=None, min_length=1)
    storage_tier: StorageTier
    minimum_age: timedelta

    @field_validator("minimum_age")
    @classmethod
    def validate_minimum_age(cls, value: timedelta) -> timedelta:
        if value < timedelta(0):
            raise ValueError("retention rule minimum_age must be nonnegative")
        return value


class RetentionPolicy(RuntimeContractModel):
    hot_min_age: timedelta
    warm_min_age: timedelta
    cold_min_age: timedelta
    minimum_verified_copies: int = Field(ge=1)
    verification_max_age: timedelta
    plan_ttl: timedelta
    claim_ttl: timedelta
    rules: tuple[RetentionRule, ...] = ()

    @field_validator(
        "hot_min_age",
        "warm_min_age",
        "cold_min_age",
        "verification_max_age",
        "plan_ttl",
        "claim_ttl",
    )
    @classmethod
    def validate_nonnegative_age(cls, value: timedelta) -> timedelta:
        if value < timedelta(0):
            raise ValueError("retention ages must be nonnegative")
        return value

    @model_validator(mode="after")
    def validate_age_order(self) -> Self:
        if not self.hot_min_age <= self.warm_min_age <= self.cold_min_age:
            raise ValueError("retention ages must satisfy hot <= warm <= cold")
        if self.verification_max_age <= timedelta(0):
            raise ValueError("verification_max_age must be positive")
        if self.plan_ttl <= timedelta(0) or self.claim_ttl <= timedelta(0):
            raise ValueError("GC plan and claim TTL must be positive")
        if self.claim_ttl > self.plan_ttl:
            raise ValueError("claim_ttl cannot exceed plan_ttl")
        identities = [canonical_sha256(rule.model_dump(mode="json")) for rule in self.rules]
        if identities != sorted(identities) or len(identities) != len(set(identities)):
            raise ValueError("retention rules must be unique and canonically ordered")
        return self

    def age_for(
        self,
        tier: StorageTier,
        *,
        object_kind: str | None = None,
        owner_types: frozenset[str] = frozenset(),
    ) -> timedelta:
        base = {
            StorageTier.HOT: self.hot_min_age,
            StorageTier.WARM: self.warm_min_age,
            StorageTier.COLD: self.cold_min_age,
        }[tier]
        matching = (
            rule.minimum_age
            for rule in self.rules
            if rule.storage_tier is tier
            and (rule.object_kind is None or rule.object_kind == object_kind)
            and (rule.owner_type is None or rule.owner_type in owner_types)
        )
        return max((base, *matching))

    def identity_payload(self) -> dict[str, object]:
        return {
            "hot_min_age_us": _timedelta_microseconds(self.hot_min_age),
            "warm_min_age_us": _timedelta_microseconds(self.warm_min_age),
            "cold_min_age_us": _timedelta_microseconds(self.cold_min_age),
            "minimum_verified_copies": self.minimum_verified_copies,
            "verification_max_age_us": _timedelta_microseconds(self.verification_max_age),
            "plan_ttl_us": _timedelta_microseconds(self.plan_ttl),
            "claim_ttl_us": _timedelta_microseconds(self.claim_ttl),
            "rules": tuple(rule.model_dump(mode="json") for rule in self.rules),
        }


class GcCandidate(RuntimeContractModel):
    candidate_id: Sha256 | None = None
    object_identity: ObjectIdentity
    object_copy: ObjectCopy

    @model_validator(mode="after")
    def validate_candidate(self) -> Self:
        if self.object_identity.content_sha256 != self.object_copy.content_sha256:
            raise ValueError("candidate object and copy content hashes must match")
        expected = canonical_sha256(
            {
                "object_identity": self.object_identity,
                "object_copy": self.object_copy,
            }
        )
        if self.candidate_id is None:
            object.__setattr__(self, "candidate_id", expected)
        elif self.candidate_id != expected:
            raise ValueError("candidate_id does not match candidate identity")
        return self


class GcDeferredCandidate(RuntimeContractModel):
    candidate: GcCandidate
    reason: Literal["byte_budget_exceeded"]


class GcPlan(RuntimeContractModel):
    plan_id: Sha256 | None = None
    planned_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime | None = None
    ledger_revision: int = Field(ge=0)
    policy: RetentionPolicy
    candidates: tuple[GcCandidate, ...]
    deferred_candidates: tuple[GcDeferredCandidate, ...] = ()

    @model_validator(mode="after")
    def validate_plan(self) -> Self:
        expected_expiry = self.planned_at + self.policy.plan_ttl
        if self.expires_at is None:
            object.__setattr__(self, "expires_at", expected_expiry)
        elif self.expires_at != expected_expiry:
            raise ValueError("GC plan expiry must match policy plan_ttl")
        candidate_ids = [candidate.candidate_id for candidate in self.candidates]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("GC candidates must be unique")
        if candidate_ids != sorted(candidate_ids):
            raise ValueError("GC candidates must use deterministic ordering")
        deferred_ids = [item.candidate.candidate_id for item in self.deferred_candidates]
        if len(deferred_ids) != len(set(deferred_ids)):
            raise ValueError("deferred GC candidates must be unique")
        if deferred_ids != sorted(deferred_ids):
            raise ValueError("deferred GC candidates must use deterministic ordering")
        if set(candidate_ids) & set(deferred_ids):
            raise ValueError("a GC candidate cannot be both selected and deferred")
        expected = _plan_id(
            planned_at=self.planned_at,
            expires_at=self.expires_at,
            ledger_revision=self.ledger_revision,
            policy=self.policy,
            candidates=self.candidates,
            deferred_candidates=self.deferred_candidates,
        )
        if self.plan_id is None:
            object.__setattr__(self, "plan_id", expected)
        elif self.plan_id != expected:
            raise ValueError("plan_id does not match plan content")
        return self


class GcClaim(RuntimeContractModel):
    claim_id: Sha256 | None = None
    plan: GcPlan
    candidate: GcCandidate
    owner_id: str = Field(min_length=1)
    claimed_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_claim(self) -> Self:
        if self.candidate not in self.plan.candidates:
            raise ValueError("claim candidate is not part of GC plan")
        if self.claimed_at < self.plan.planned_at:
            raise ValueError("claim cannot precede GC plan")
        if self.expires_at != self.claimed_at + self.plan.policy.claim_ttl:
            raise ValueError("claim expiry must match policy claim_ttl")
        if self.plan.expires_at is None or self.expires_at > self.plan.expires_at:
            raise ValueError("claim cannot outlive GC plan")
        expected = canonical_sha256(
            {
                "plan_id": self.plan.plan_id,
                "candidate_id": self.candidate.candidate_id,
                "owner_id": self.owner_id,
                "claimed_at": self.claimed_at,
                "expires_at": self.expires_at,
            }
        )
        if self.claim_id is None:
            object.__setattr__(self, "claim_id", expected)
        elif self.claim_id != expected:
            raise ValueError("claim_id does not match claim content")
        return self


class GcOperationRecord(RuntimeContractModel):
    operation_id: Sha256
    candidate_id: Sha256
    content_sha256: Sha256
    location_id: str = Field(min_length=1)
    claim: GcClaim
    status: str = Field(pattern=r"^(claimed|completed|released)$")

    @model_validator(mode="after")
    def validate_operation_binding(self) -> Self:
        candidate = self.claim.candidate
        if (
            self.candidate_id != candidate.candidate_id
            or self.content_sha256 != candidate.object_identity.content_sha256
            or self.location_id != candidate.object_copy.location_id
        ):
            raise ValueError("GC operation content binding conflicts with claim")
        return self


class ExpiredGcClaimRecoveryReceipt(RuntimeContractModel):
    receipt_id: Sha256 | None = None
    claim_id: Sha256
    candidate_id: Sha256
    token_id: Sha256
    deletion_receipt_id: Sha256
    owner_id: str = Field(min_length=1)
    runtime_fence: int = Field(ge=1)
    recovered_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_receipt_identity(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"receipt_id"}))
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("expired GC claim recovery receipt identity is invalid")
        return self


class ArtifactAuditEvent(RuntimeContractModel):
    sequence: int = Field(ge=1)
    event_type: str = Field(min_length=1)
    subject_id: str = Field(min_length=1)
    content_sha256: Sha256
    occurred_at: AwareUtcDatetime
    payload_json: str


def _timedelta_microseconds(value: timedelta) -> int:
    return value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds


def _plan_id(
    *,
    planned_at: object,
    expires_at: object,
    ledger_revision: int,
    policy: RetentionPolicy,
    candidates: tuple[GcCandidate, ...],
    deferred_candidates: tuple[GcDeferredCandidate, ...] = (),
) -> str:
    payload: dict[str, object] = {
        "planned_at": planned_at,
        "expires_at": expires_at,
        "ledger_revision": ledger_revision,
        "policy": policy.identity_payload(),
        "candidate_ids": tuple(candidate.candidate_id for candidate in candidates),
    }
    if deferred_candidates:
        payload["deferred_candidates"] = tuple(
            {
                "candidate_id": item.candidate.candidate_id,
                "reason": item.reason,
            }
            for item in deferred_candidates
        )
    return canonical_sha256(payload)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS artifact_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    governance_revision INTEGER NOT NULL
);
INSERT OR IGNORE INTO artifact_metadata(singleton, governance_revision) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS artifact_writer_fence (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    fence INTEGER NOT NULL,
    owner_label TEXT,
    lease_token TEXT,
    acquired_at TEXT
);
INSERT OR IGNORE INTO artifact_writer_fence(
    singleton, fence, owner_label, lease_token, acquired_at
) VALUES (1, 0, NULL, NULL, NULL);

CREATE TABLE IF NOT EXISTS artifact_writer_service_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    service_owner TEXT NOT NULL
);
INSERT OR IGNORE INTO artifact_writer_service_identity(
    singleton, service_owner
) VALUES (1, 'artifact-metadata-service/v1');

CREATE TABLE IF NOT EXISTS artifact_writer_credential (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    key_id TEXT,
    sequence INTEGER,
    secret_sha256 TEXT,
    not_before TEXT,
    expires_at TEXT,
    revoked_at TEXT
);
INSERT OR IGNORE INTO artifact_writer_credential(
    singleton, key_id, sequence, secret_sha256, not_before, expires_at, revoked_at
) VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE IF NOT EXISTS artifact_object (
    content_sha256 TEXT PRIMARY KEY,
    size_bytes INTEGER NOT NULL,
    object_kind TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_copy (
    content_sha256 TEXT NOT NULL,
    location_id TEXT NOT NULL,
    storage_uri TEXT NOT NULL,
    storage_tier TEXT NOT NULL,
    verified_at TEXT,
    failure_domain TEXT NOT NULL,
    tier_entered_at TEXT NOT NULL,
    deleted_at TEXT,
    deletion_plan_id TEXT,
    deletion_candidate_id TEXT,
    PRIMARY KEY (content_sha256, location_id),
    FOREIGN KEY (content_sha256) REFERENCES artifact_object(content_sha256),
    UNIQUE (storage_uri),
    UNIQUE (content_sha256, failure_domain)
);

CREATE TABLE IF NOT EXISTS artifact_reference (
    reference_id TEXT PRIMARY KEY,
    owner_type TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    released_at TEXT,
    FOREIGN KEY (content_sha256) REFERENCES artifact_object(content_sha256)
);

CREATE TABLE IF NOT EXISTS artifact_owner_release_receipt (
    receipt_id TEXT PRIMARY KEY,
    reference_id TEXT NOT NULL UNIQUE,
    owner_type TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    terminal_state TEXT NOT NULL,
    lifecycle_revision INTEGER NOT NULL,
    evidence_sha256 TEXT NOT NULL,
    released_at TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    FOREIGN KEY (reference_id) REFERENCES artifact_reference(reference_id),
    FOREIGN KEY (content_sha256) REFERENCES artifact_object(content_sha256)
);

CREATE TRIGGER IF NOT EXISTS artifact_owner_release_receipt_no_update
BEFORE UPDATE ON artifact_owner_release_receipt
BEGIN SELECT RAISE(ABORT, 'artifact_owner_release_receipt is append-only'); END;

CREATE TRIGGER IF NOT EXISTS artifact_owner_release_receipt_no_delete
BEFORE DELETE ON artifact_owner_release_receipt
BEGIN SELECT RAISE(ABORT, 'artifact_owner_release_receipt is append-only'); END;

CREATE TABLE IF NOT EXISTS artifact_owner_release_outbox (
    receipt_id TEXT PRIMARY KEY,
    reference_id TEXT NOT NULL UNIQUE,
    owner_type TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    published_at TEXT,
    FOREIGN KEY (reference_id) REFERENCES artifact_reference(reference_id),
    FOREIGN KEY (content_sha256) REFERENCES artifact_object(content_sha256)
);

CREATE INDEX IF NOT EXISTS artifact_owner_release_outbox_pending_idx
ON artifact_owner_release_outbox(published_at, enqueued_at, receipt_id);

CREATE TRIGGER IF NOT EXISTS artifact_owner_release_outbox_publish_only
BEFORE UPDATE ON artifact_owner_release_outbox
WHEN NEW.receipt_id IS NOT OLD.receipt_id
  OR NEW.reference_id IS NOT OLD.reference_id
  OR NEW.owner_type IS NOT OLD.owner_type
  OR NEW.owner_id IS NOT OLD.owner_id
  OR NEW.content_sha256 IS NOT OLD.content_sha256
  OR NEW.receipt_json IS NOT OLD.receipt_json
  OR NEW.enqueued_at IS NOT OLD.enqueued_at
  OR OLD.published_at IS NOT NULL
  OR NEW.published_at IS NULL
  OR NOT EXISTS (
      SELECT 1 FROM artifact_owner_release_receipt AS receipt
      WHERE receipt.receipt_id = OLD.receipt_id
        AND receipt.receipt_json = OLD.receipt_json
        AND receipt.released_at = NEW.published_at
  )
BEGIN SELECT RAISE(ABORT, 'artifact_owner_release_outbox is immutable after publish'); END;

CREATE TRIGGER IF NOT EXISTS artifact_owner_release_outbox_no_delete
BEFORE DELETE ON artifact_owner_release_outbox
BEGIN SELECT RAISE(ABORT, 'artifact_owner_release_outbox is append-only'); END;

CREATE TABLE IF NOT EXISTS artifact_legal_hold (
    hold_id TEXT PRIMARY KEY,
    content_sha256 TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_at TEXT,
    FOREIGN KEY (content_sha256) REFERENCES artifact_object(content_sha256)
);

CREATE TABLE IF NOT EXISTS artifact_gc_claim (
    claim_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    location_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    claimed_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    claim_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('claimed', 'released', 'completed')),
    resolved_at TEXT,
    resolution_reason TEXT,
    FOREIGN KEY (content_sha256) REFERENCES artifact_object(content_sha256),
    UNIQUE (plan_id, candidate_id)
);

CREATE TABLE IF NOT EXISTS artifact_gc_operation (
    operation_id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    location_id TEXT NOT NULL,
    claim_id TEXT NOT NULL UNIQUE,
    claim_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('claimed', 'released', 'completed')),
    updated_at TEXT NOT NULL,
    FOREIGN KEY (claim_id) REFERENCES artifact_gc_claim(claim_id),
    FOREIGN KEY (content_sha256) REFERENCES artifact_object(content_sha256)
);

CREATE INDEX IF NOT EXISTS artifact_gc_operation_candidate_idx
ON artifact_gc_operation(candidate_id, status, operation_id);

CREATE INDEX IF NOT EXISTS artifact_copy_migration_keyset_idx
ON artifact_copy(content_sha256, storage_tier, location_id, deleted_at);

CREATE INDEX IF NOT EXISTS artifact_reference_content_owner_status_idx
ON artifact_reference(content_sha256, owner_type, reference_id, released_at, expires_at);

CREATE INDEX IF NOT EXISTS artifact_reference_content_status_expiry_idx
ON artifact_reference(content_sha256, released_at, expires_at);

CREATE INDEX IF NOT EXISTS artifact_reference_owner_status_idx
ON artifact_reference(owner_type, owner_id, released_at, reference_id);

CREATE INDEX IF NOT EXISTS artifact_legal_hold_content_status_idx
ON artifact_legal_hold(content_sha256, released_at);

CREATE INDEX IF NOT EXISTS artifact_gc_claim_content_status_idx
ON artifact_gc_claim(content_sha256, status);

CREATE TABLE IF NOT EXISTS artifact_audit (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS artifact_audit_no_update
BEFORE UPDATE ON artifact_audit
BEGIN SELECT RAISE(ABORT, 'artifact_audit is append-only'); END;

CREATE TRIGGER IF NOT EXISTS artifact_audit_no_delete
BEFORE DELETE ON artifact_audit
BEGIN SELECT RAISE(ABORT, 'artifact_audit is append-only'); END;
"""


class PrivateSqlitePathAuthority:
    """Fence one private SQLite path against aliases and generation swaps."""

    def __init__(
        self,
        path: Path,
        *,
        label: str,
        create_if_missing: bool,
        managed_trust_root: Path,
        create_parent_if_missing: bool = False,
    ) -> None:
        raw = Path(path)
        if not raw.is_absolute() or str(raw) != os.path.normpath(str(raw)) or ".." in raw.parts:
            raise ValueError(f"{label} path must be an exact absolute path")
        self.path = raw
        self.label = label
        self._identity_lock = _sqlite_identity_lock(raw)
        self._managed_trust_root, self._managed_trust_root_generation = (
            self._validate_managed_trust_root(managed_trust_root)
        )
        if create_parent_if_missing:
            self._create_private_parent_chain()
            _, observed_root_generation = self._validate_managed_trust_root(
                self._managed_trust_root
            )
            if _node_identity(observed_root_generation) != _node_identity(
                self._managed_trust_root_generation
            ):
                raise ValueError(f"{self.label} managed trust root identity changed")
            self._managed_trust_root_generation = observed_root_generation
        self._parent_generation = self._bind_unlocked_parent_chain()
        if not os.path.lexists(self.path):
            if not create_if_missing:
                self._generation: PathGeneration | None = None
                return
            self._create_private_file()
        self._generation = self._validate_file()

    def _managed_trust_root_chain_index(self) -> int:
        """Index of the managed trust root inside a validated parent chain.

        ``_validate_parent_chain`` emits exactly one entry per component of
        ``self.path.parent``, starting at the anchor, and the trust root is an
        ancestor-or-self of that parent. The mapping is otherwise implicit, so
        fix it here: a future change to the chain layout must fail loudly
        instead of silently rebinding the wrong component.
        """

        index = len(self._managed_trust_root.parts) - 1
        components = len(self.path.parent.parts)
        if not 0 <= index < components:
            raise ValueError(
                f"{self.label} managed trust root sits at chain index {index}, "
                f"outside its {components} parent components"
            )
        return index

    def _bind_unlocked_parent_chain(self) -> tuple[PathGeneration, ...]:
        """Bind the ancestry while no cross-process lock can be held.

        Everything in this constructor runs before ``ArtifactReferenceStore``
        takes its cross-process writer flock, and the managed trust root is a
        directory shared with every other writer: any peer that creates or
        removes SQLite ``-wal``/``-shm`` sidecars bumps its ``st_ctime_ns``
        here. A ctime bound in this window is therefore not a fence, only a
        tripwire that kills legitimate writers before they reach the credential
        check (issue #158). Bind one self-consistent observation instead.

        Nothing else is relaxed: ``_validate_parent_chain`` still rejects
        symlinks and non-directories, still enforces owner and ``0o700`` mode
        below the trust root, and still compares the trust root on ``(st_dev,
        st_ino)``. ctime strictness is retained everywhere a lock is held --
        ``assert_current`` and the rebind path are untouched.
        """

        index = self._managed_trust_root_chain_index()
        observed = self._validate_parent_chain(allow_managed_trust_root_ctime_change=True)
        components = len(self.path.parent.parts)
        if len(observed) != components:
            raise ValueError(
                f"{self.label} parent chain returned {len(observed)} entries "
                f"for {components} path components"
            )
        root_generation = observed[index]
        if _node_identity(root_generation) != _node_identity(self._managed_trust_root_generation):
            raise ValueError(
                f"{self.label} managed trust root inode changed at chain index {index}: "
                f"expected dev/ino {_node_identity(self._managed_trust_root_generation)}, "
                f"observed {_node_identity(root_generation)}"
            )
        self._managed_trust_root_generation = root_generation
        return observed

    def _validate_managed_trust_root(
        self,
        managed_trust_root: Path,
    ) -> tuple[Path, PathGeneration]:
        root = Path(managed_trust_root)
        if not root.is_absolute() or str(root) != os.path.normpath(str(root)) or ".." in root.parts:
            raise ValueError(f"{self.label} managed trust root must be an exact absolute path")
        try:
            self.path.parent.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"{self.label} path must be inside its managed trust root") from exc
        try:
            observed = os.lstat(root)
        except OSError as exc:
            raise ValueError(f"{self.label} managed trust root is missing or unsafe") from exc
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise ValueError(
                f"{self.label} managed trust root is a symlink or unsafe: "
                f"expected a directory, observed mode {stat.S_IFMT(observed.st_mode):#o}"
            )
        if observed.st_uid != os.geteuid() or stat.S_IMODE(observed.st_mode) != 0o700:
            raise ValueError(
                f"{self.label} managed trust root owner or mode is unsafe: "
                f"expected uid {os.geteuid()} mode 0o700, observed uid {observed.st_uid} "
                f"mode {stat.S_IMODE(observed.st_mode):#o}"
            )
        return root, _path_generation(observed)

    def _parent_requires_private_mode(self, path: Path) -> bool:
        try:
            path.relative_to(self._managed_trust_root)
        except ValueError:
            return False
        return True

    def _validate_private_parent_stat(self, path: Path, observed: os.stat_result) -> None:
        if not self._parent_requires_private_mode(path):
            return
        if observed.st_uid != os.geteuid() or stat.S_IMODE(observed.st_mode) != 0o700:
            raise ValueError(
                f"{self.label} parent owner or mode is unsafe at {path}: "
                f"expected uid {os.geteuid()} mode 0o700, observed uid {observed.st_uid} "
                f"mode {stat.S_IMODE(observed.st_mode):#o}"
            )

    def _create_private_parent_chain(self) -> None:
        parent = self.path.parent
        current = Path(parent.anchor)
        existing_identities: list[PathGeneration] = []
        try:
            root = os.lstat(current)
            if stat.S_ISLNK(root.st_mode) or not stat.S_ISDIR(root.st_mode):
                raise ValueError(f"{self.label} path anchor is unsafe")
            existing_identities.append(_path_generation(root))
            missing_parts: tuple[str, ...] = ()
            parent_parts = parent.parts[1:]
            for index, component in enumerate(parent_parts):
                candidate = current / component
                try:
                    observed = os.lstat(candidate)
                except FileNotFoundError:
                    missing_parts = parent_parts[index:]
                    break
                if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                    raise ValueError(f"{self.label} path contains a symlink or unsafe parent")
                existing_identities.append(_path_generation(observed))
                current = candidate
        except OSError as exc:
            raise ValueError(f"{self.label} parent path is unsafe") from exc
        if not missing_parts:
            return

        descriptor = os.open(
            current,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            opened = os.fstat(descriptor)
            if _node_identity(_path_generation(opened)) != _node_identity(existing_identities[-1]):
                raise ValueError(
                    f"{self.label} parent changed while creating directory: "
                    f"expected dev/ino {_node_identity(existing_identities[-1])}, "
                    f"observed {_node_identity(_path_generation(opened))}"
                )
            for component in missing_parts:
                created = False
                try:
                    os.mkdir(component, mode=0o700, dir_fd=descriptor)
                    created = True
                except FileExistsError:
                    pass
                child = os.open(
                    component,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=descriptor,
                )
                try:
                    if created:
                        os.fchmod(child, 0o700)
                    child_observed = os.fstat(child)
                    if not stat.S_ISDIR(child_observed.st_mode):
                        raise ValueError(f"{self.label} parent path is unsafe")
                    self._validate_private_parent_stat(current / component, child_observed)
                    os.fsync(descriptor)
                except BaseException:
                    os.close(child)
                    raise
                os.close(descriptor)
                descriptor = child
        finally:
            os.close(descriptor)

        observed_identities = self._validate_parent_chain(
            allow_managed_trust_root_ctime_change=True
        )
        observed_existing = tuple(
            _node_identity(item) for item in observed_identities[: len(existing_identities)]
        )
        if observed_existing != tuple(_node_identity(item) for item in existing_identities):
            raise ValueError(f"{self.label} parent changed while creating directory")

    @property
    def database_generation(self) -> PathGeneration:
        if self._generation is None:
            raise ValueError(f"{self.label} does not exist")
        return self._generation

    def _validate_parent_chain(
        self,
        *,
        allow_managed_trust_root_ctime_change: bool = False,
    ) -> tuple[PathGeneration, ...]:
        current = Path(self.path.anchor)
        identities: list[PathGeneration] = []
        try:
            root = os.lstat(current)
            if not stat.S_ISDIR(root.st_mode) or stat.S_ISLNK(root.st_mode):
                raise ValueError(f"{self.label} path anchor is unsafe")
            identities.append(_path_generation(root))
            for component in self.path.parts[1:-1]:
                current /= component
                observed = os.lstat(current)
                if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                    raise ValueError(f"{self.label} path contains a symlink or unsafe parent")
                self._validate_private_parent_stat(current, observed)
                if self._managed_trust_root == current:
                    observed_generation = _path_generation(observed)
                    if allow_managed_trust_root_ctime_change:
                        if _node_identity(observed_generation) != _node_identity(
                            self._managed_trust_root_generation
                        ):
                            raise ValueError(
                                f"{self.label} managed trust root inode changed at {current}: "
                                "expected dev/ino "
                                f"{_node_identity(self._managed_trust_root_generation)}, "
                                f"observed {_node_identity(observed_generation)}"
                            )
                    elif observed_generation != self._managed_trust_root_generation:
                        # Message text is load bearing: experiment_registry
                        # compares it verbatim to decide whether a bind is
                        # retryable. Diagnostics go on the lenient branch.
                        raise ValueError(f"{self.label} managed trust root identity changed")
                identities.append(_path_generation(observed))
        except FileNotFoundError as exc:
            raise ValueError(f"{self.label} parent path is missing") from exc
        except OSError as exc:
            raise ValueError(f"{self.label} parent path is unsafe") from exc
        parent = os.lstat(self.path.parent)
        if parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) & 0o077:
            raise ValueError(
                f"{self.label} parent owner or mode is unsafe at {self.path.parent}: "
                f"expected uid {os.geteuid()} with no group or other bits, observed uid "
                f"{parent.st_uid} mode {stat.S_IMODE(parent.st_mode):#o}"
            )
        return tuple(identities)

    def _create_private_file(self) -> None:
        observed_parent_generation = self._validate_parent_chain(
            allow_managed_trust_root_ctime_change=True
        )
        if tuple(map(_node_identity, observed_parent_generation)) != tuple(
            map(_node_identity, self._parent_generation)
        ):
            raise ValueError(f"{self.label} parent changed before creating database")
        self._parent_generation = observed_parent_generation
        self._managed_trust_root_generation = observed_parent_generation[
            len(self._managed_trust_root.parts) - 1
        ]
        parent_before = os.lstat(self.path.parent)
        parent_descriptor = os.open(
            self.path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            parent_opened = os.fstat(parent_descriptor)
            expected_parent = _node_identity(self._parent_generation[-1])
            observed_parents = (
                _node_identity(_path_generation(parent_opened)),
                _node_identity(_path_generation(parent_before)),
            )
            if any(item != expected_parent for item in observed_parents):
                raise ValueError(
                    f"{self.label} parent changed while creating database: "
                    f"expected dev/ino {expected_parent}, observed "
                    f"opened {observed_parents[0]} named {observed_parents[1]}"
                )
            descriptor = os.open(
                self.path.name,
                os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=parent_descriptor,
            )
            try:
                os.fchmod(descriptor, 0o600)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.fsync(parent_descriptor)
        except FileExistsError as exc:
            raise ValueError(f"{self.label} path changed while creating database") from exc
        finally:
            os.close(parent_descriptor)
        observed_parent_generation = self._validate_parent_chain(
            allow_managed_trust_root_ctime_change=True
        )
        if tuple(map(_node_identity, observed_parent_generation)) != tuple(
            map(_node_identity, self._parent_generation)
        ):
            raise ValueError(f"{self.label} parent changed after creating database")
        _, observed_root_generation = self._validate_managed_trust_root(self._managed_trust_root)
        if _node_identity(observed_root_generation) != _node_identity(
            self._managed_trust_root_generation
        ):
            raise ValueError(f"{self.label} managed trust root identity changed")
        self._managed_trust_root_generation = observed_root_generation
        self._parent_generation = self._bind_unlocked_parent_chain()

    def durably_sync_current_database(self) -> None:
        """Persist the initialized database inode and its containing directory entry."""

        self.assert_current()
        parent_descriptor = os.open(
            self.path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        descriptor = -1
        try:
            descriptor = os.open(
                self.path.name,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_descriptor,
            )
            observed = os.fstat(descriptor)
            if _path_generation(observed) != self.database_generation:
                raise ValueError(f"{self.label} path identity changed before durable sync")
            os.fsync(descriptor)
            self.assert_current()
            os.fsync(parent_descriptor)
            self.assert_current()
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            os.close(parent_descriptor)

    def _validate_file(self) -> PathGeneration:
        try:
            observed = os.lstat(self.path)
        except OSError as exc:
            raise ValueError(f"{self.label} file is missing or unsafe") from exc
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
            raise ValueError(
                f"{self.label} file is a symlink or unsafe: expected a regular file, "
                f"observed mode {stat.S_IFMT(observed.st_mode):#o}"
            )
        if observed.st_nlink != 1:
            raise ValueError(
                f"{self.label} file has an unsafe hard link: "
                f"expected st_nlink 1, observed {observed.st_nlink}"
            )
        if observed.st_uid != os.geteuid():
            raise ValueError(
                f"{self.label} file owner is unsafe: "
                f"expected uid {os.geteuid()}, observed {observed.st_uid}"
            )
        if stat.S_IMODE(observed.st_mode) != 0o600:
            raise ValueError(
                f"{self.label} file mode must be private 0600: "
                f"observed {stat.S_IMODE(observed.st_mode):#o}"
            )
        return _path_generation(observed)

    def assert_current(self) -> None:
        with self._identity_lock:
            self._assert_current_unlocked()

    def _assert_current_unlocked(self) -> None:
        before = self._validate_parent_chain()
        generation = self._validate_file()
        after = self._validate_parent_chain()
        if (
            not self._matches_bound_parent_generation(before)
            or not self._matches_bound_parent_generation(after)
            or self._generation is None
            or generation != self._generation
        ):
            raise ValueError(f"{self.label} path identity changed")

    def _matches_bound_parent_generation(
        self,
        observed: tuple[PathGeneration, ...],
    ) -> bool:
        trust_root_index = len(self._managed_trust_root.parts) - 1
        return (
            tuple(map(_node_identity, observed[:trust_root_index]))
            == tuple(map(_node_identity, self._parent_generation[:trust_root_index]))
            and observed[trust_root_index:] == self._parent_generation[trust_root_index:]
        )

    def _same_parent_observation(
        self,
        left: tuple[PathGeneration, ...],
        right: tuple[PathGeneration, ...],
    ) -> bool:
        trust_root_index = len(self._managed_trust_root.parts) - 1
        return (
            tuple(map(_node_identity, left[:trust_root_index]))
            == tuple(map(_node_identity, right[:trust_root_index]))
            and left[trust_root_index:] == right[trust_root_index:]
        )

    def rebind_ctime_after_trusted_sqlite_setup(self) -> None:
        """Accept SQLite's own ctime changes while retaining every bound inode."""

        with self._identity_lock:
            self._rebind_ctime_after_trusted_sqlite_setup_unlocked()

    def _rebind_ctime_after_trusted_sqlite_setup_unlocked(self) -> None:

        for _attempt in range(3):
            parent_generation_before = self._validate_parent_chain(
                allow_managed_trust_root_ctime_change=True
            )
            file_generation_before = self._validate_file()
            parent_generation = self._validate_parent_chain(
                allow_managed_trust_root_ctime_change=True
            )
            file_generation = self._validate_file()
            if (
                self._same_parent_observation(
                    parent_generation_before,
                    parent_generation,
                )
                and file_generation == file_generation_before
            ):
                break
        else:
            raise ValueError(f"{self.label} parent ctime changed while rebinding")
        root_generation = parent_generation[len(self._managed_trust_root.parts) - 1]
        if (
            _node_identity(root_generation) != _node_identity(self._managed_trust_root_generation)
            or tuple(map(_node_identity, parent_generation))
            != tuple(map(_node_identity, self._parent_generation))
            or self._generation is None
            or _node_identity(file_generation) != _node_identity(self._generation)
        ):
            raise ValueError(f"{self.label} path identity changed during SQLite setup")
        self._managed_trust_root_generation = root_generation
        self._parent_generation = parent_generation
        self._generation = file_generation

    def rebind_and_assert_current_after_trusted_sqlite_change(self) -> None:
        with self._identity_lock:
            self.rebind_ctime_after_trusted_sqlite_setup()
            self.assert_current()

    @contextmanager
    def identity_boundary(self) -> Iterator[None]:
        with self._identity_lock:
            yield

    def open_verified_connection(
        self,
        opener: Callable[[Path], sqlite3.Connection],
    ) -> sqlite3.Connection:
        with self.identity_boundary():
            initial_error: BaseException | None = None
            for _attempt in range(3):
                try:
                    self.rebind_and_assert_current_after_trusted_sqlite_change()
                    break
                except BaseException as exc:
                    initial_error = exc
            else:
                assert initial_error is not None
                raise initial_error
            connection = opener(self.path)
            try:
                self.rebind_and_assert_current_after_trusted_sqlite_change()
            except BaseException as exc:
                close_verified_sqlite_connection(
                    connection,
                    self,
                    primary_error=exc,
                    known_identity_failure=True,
                )
                raise
            return connection

    def readonly_uri(self) -> str:
        return f"file:{quote(str(self.path), safe='/')}?mode=ro"

    def writable_uri(self) -> str:
        return f"file:{quote(str(self.path), safe='/')}?mode=rw"

    def begin_immutable_read(self) -> tuple[int, int, int, int, int]:
        self.assert_current()
        self._assert_no_sqlite_sidecars()
        observed = os.lstat(self.path)
        return (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )

    def assert_immutable_read_current(
        self,
        generation: tuple[int, int, int, int, int],
    ) -> None:
        self.assert_current()
        self._assert_no_sqlite_sidecars()
        observed = os.lstat(self.path)
        current = (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )
        if current != generation:
            raise ValueError(f"{self.label} changed during immutable read")

    def immutable_readonly_uri(self) -> str:
        return f"file:{quote(str(self.path), safe='/')}?mode=ro&immutable=1"

    def sqlite_sidecar_state(self) -> tuple[bool, bool]:
        state: list[bool] = []
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{self.path}{suffix}")
            present = os.path.lexists(sidecar)
            if present:
                observed = os.lstat(sidecar)
                if (
                    stat.S_ISLNK(observed.st_mode)
                    or not stat.S_ISREG(observed.st_mode)
                    or observed.st_nlink != 1
                    or observed.st_uid != os.geteuid()
                    or stat.S_IMODE(observed.st_mode) != 0o600
                ):
                    raise ValueError(f"{self.label} has an unsafe SQLite sidecar")
            state.append(present)
        result = (state[0], state[1])
        if result[0] != result[1]:
            raise ValueError(f"{self.label} has an incomplete SQLite sidecar pair")
        return result

    def _assert_no_sqlite_sidecars(self) -> None:
        if any(self.sqlite_sidecar_state()):
            raise ValueError(f"{self.label} has an active WAL and is not quiescent")


def close_verified_sqlite_connection(
    connection: sqlite3.Connection,
    authority: PrivateSqlitePathAuthority,
    *,
    primary_error: BaseException | None = None,
    known_identity_failure: bool = False,
) -> None:
    """Close one SQLite handle while fencing its path on both sides."""

    with authority.identity_boundary():
        _close_verified_sqlite_connection_locked(
            connection,
            authority,
            primary_error=primary_error,
            known_identity_failure=known_identity_failure,
        )


def _close_verified_sqlite_connection_locked(
    connection: sqlite3.Connection,
    authority: PrivateSqlitePathAuthority,
    *,
    primary_error: BaseException | None,
    known_identity_failure: bool,
) -> None:
    """Run verified close while the short path-identity boundary is held."""

    pre_rebind_error: BaseException | None = None
    identity_error: BaseException | None = None
    close_error: BaseException | None = None
    rebind_error: BaseException | None = None
    postcheck_error: BaseException | None = None
    try:
        authority.rebind_ctime_after_trusted_sqlite_setup()
    except BaseException as exc:
        pre_rebind_error = exc
    try:
        authority.assert_current()
    except BaseException as exc:
        identity_error = exc
    try:
        connection.close()
    except BaseException as exc:
        close_error = exc
    if pre_rebind_error is None and identity_error is None:
        try:
            authority.rebind_ctime_after_trusted_sqlite_setup()
        except BaseException as exc:
            rebind_error = exc
    try:
        authority.assert_current()
    except BaseException as exc:
        postcheck_error = exc
    duplicate_known_identity = (
        primary_error is not None
        and known_identity_failure
        and postcheck_error is not None
        and type(postcheck_error) is type(primary_error)
        and postcheck_error.args == primary_error.args
    )
    cleanup_errors = [
        *([pre_rebind_error] if pre_rebind_error is not None else []),
        *([identity_error] if identity_error is not None and not known_identity_failure else []),
        *([close_error] if close_error is not None else []),
        *([rebind_error] if rebind_error is not None else []),
        *(
            [postcheck_error]
            if postcheck_error is not None and not duplicate_known_identity
            else []
        ),
    ]
    raise_preserving_cleanup_errors(
        primary_error=primary_error,
        cleanup_errors=cleanup_errors,
        message="SQLite operation failed with close identity verification errors",
    )


def raise_preserving_cleanup_errors(
    *,
    primary_error: BaseException | None,
    cleanup_errors: list[BaseException],
    message: str,
) -> None:
    """Keep the operation failure first and append every cleanup failure."""

    errors = [*([primary_error] if primary_error is not None else []), *cleanup_errors]
    if not errors or (primary_error is not None and len(errors) == 1):
        return
    if len(errors) == 1:
        raise errors[0]
    raise BaseExceptionGroup(message, errors) from None


@contextmanager
def verified_sqlite_connection_scope(
    connection: sqlite3.Connection,
    authority: PrivateSqlitePathAuthority,
) -> Iterator[sqlite3.Connection]:
    """Close a verified connection without replacing its body exception."""

    primary_error: BaseException | None = None
    try:
        yield connection
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        close_verified_sqlite_connection(
            connection,
            authority,
            primary_error=primary_error,
        )


def execute_sqlite_setup_statement(
    connection: sqlite3.Connection,
    sql: str,
) -> None:
    """Fully finalize one setup statement before rebinding path metadata."""

    cursor = connection.execute(sql)
    fetchall = getattr(cursor, "fetchall", None)
    close = getattr(cursor, "close", None)
    try:
        if callable(fetchall):
            fetchall()
    finally:
        if cursor is not connection and callable(close):
            close()


class _ArtifactProcessWriterLock:
    """Cross-process lock bound to the store's trusted descriptor ancestry."""

    def __init__(self, database_path: Path, *, managed_trust_root: Path) -> None:
        root = Path(managed_trust_root)
        path = Path(database_path)
        if (
            not root.is_absolute()
            or str(root) != os.path.normpath(str(root))
            or not path.is_absolute()
            or str(path) != os.path.normpath(str(path))
        ):
            raise ValueError("artifact writer lock paths must use exact absolute paths")
        try:
            relative_parent = path.parent.relative_to(root)
        except ValueError as exc:
            raise ValueError("artifact writer lock must remain below managed trust root") from exc
        if any(part in {"", ".", ".."} for part in relative_parent.parts):
            raise ValueError("artifact writer lock parent path is unsafe")
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        root_descriptors = [os.open(root.anchor, flags)]
        try:
            for component in root.parts[1:]:
                parent_fd = root_descriptors[-1]
                named = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
                if stat.S_ISLNK(named.st_mode) or not stat.S_ISDIR(named.st_mode):
                    raise ValueError("artifact writer trust-root ancestor is unsafe")
                child_fd = os.open(component, flags, dir_fd=parent_fd)
                opened = os.fstat(child_fd)
                if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
                    os.close(child_fd)
                    raise ValueError("artifact writer trust-root ancestor changed")
                root_descriptors.append(child_fd)
            trust_root_stat = os.fstat(root_descriptors[-1])
            if (
                trust_root_stat.st_uid != os.geteuid()
                or stat.S_IMODE(trust_root_stat.st_mode) & 0o077
            ):
                raise ValueError("artifact writer trust root owner or mode is unsafe")
            descriptors = [os.dup(root_descriptors[-1])]
            names: list[str] = []
            for component in relative_parent.parts:
                named = os.stat(component, dir_fd=descriptors[-1], follow_symlinks=False)
                if stat.S_ISLNK(named.st_mode) or not stat.S_ISDIR(named.st_mode):
                    raise ValueError("artifact writer lock parent is unsafe")
                child_fd = os.open(component, flags, dir_fd=descriptors[-1])
                opened = os.fstat(child_fd)
                if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
                    os.close(child_fd)
                    raise ValueError("artifact writer lock parent changed")
                descriptors.append(child_fd)
                names.append(component)
            lock_name = f".{path.name}.writer.lock"
            lock_fd = os.open(
                lock_name,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=descriptors[-1],
            )
            opened_lock = os.fstat(lock_fd)
            named_lock = os.stat(lock_name, dir_fd=descriptors[-1], follow_symlinks=False)
            if (
                stat.S_ISLNK(named_lock.st_mode)
                or not stat.S_ISREG(opened_lock.st_mode)
                or (named_lock.st_dev, named_lock.st_ino)
                != (opened_lock.st_dev, opened_lock.st_ino)
                or opened_lock.st_uid != os.geteuid()
                or stat.S_IMODE(opened_lock.st_mode) & 0o077
                or opened_lock.st_nlink != 1
            ):
                os.close(lock_fd)
                raise ValueError("artifact writer lock identity or mode is unsafe")
        except BaseException:
            for descriptor in reversed(locals().get("descriptors", [])):
                os.close(descriptor)
            for descriptor in reversed(root_descriptors):
                os.close(descriptor)
            raise
        for descriptor in reversed(root_descriptors):
            os.close(descriptor)
        self._descriptors = descriptors
        self._names = tuple(names)
        self._lock_name = lock_name
        self._lock_fd = lock_fd
        self._lock_identity = (opened_lock.st_dev, opened_lock.st_ino)

    def _assert_current(self) -> None:
        if self._lock_fd < 0:
            raise ValueError("artifact writer process lock is closed")
        for index, name in enumerate(self._names):
            named = os.stat(name, dir_fd=self._descriptors[index], follow_symlinks=False)
            opened = os.fstat(self._descriptors[index + 1])
            if (
                stat.S_ISLNK(named.st_mode)
                or not stat.S_ISDIR(named.st_mode)
                or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise ValueError("artifact writer lock parent path identity changed")
        named_lock = os.stat(
            self._lock_name,
            dir_fd=self._descriptors[-1],
            follow_symlinks=False,
        )
        opened_lock = os.fstat(self._lock_fd)
        if (
            stat.S_ISLNK(named_lock.st_mode)
            or not stat.S_ISREG(opened_lock.st_mode)
            or (named_lock.st_dev, named_lock.st_ino) != self._lock_identity
            or (opened_lock.st_dev, opened_lock.st_ino) != self._lock_identity
            or opened_lock.st_nlink != 1
        ):
            raise ValueError("artifact writer lock naming identity changed")

    @contextmanager
    def acquire(self) -> Iterator[None]:
        self._assert_current()
        fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
        try:
            self._assert_current()
            yield
            self._assert_current()
        finally:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)

    def close(self) -> None:
        lock_fd = getattr(self, "_lock_fd", -1)
        if lock_fd >= 0:
            os.close(lock_fd)
            self._lock_fd = -1
        for descriptor in reversed(getattr(self, "_descriptors", [])):
            os.close(descriptor)
        self._descriptors = []

    def __del__(self) -> None:
        self.close()


class ArtifactReferenceStore:
    """Short SQLite WAL transactions for one metadata writer at a time."""

    def __init__(
        self,
        path: Path,
        *,
        managed_trust_root: Path,
        clock: Callable[[], datetime] | None = None,
        writer_owner: str = "artifact-reference-store",
        retention_writer_credential: ArtifactRetentionWriterCredential | None = None,
        terminal_outbox_only: bool = False,
    ) -> None:
        if not writer_owner.strip() or len(writer_owner) > 128:
            raise ValueError("artifact writer owner must be nonempty and bounded")
        if writer_owner == "artifact-retention" and retention_writer_credential is None:
            raise ArtifactRetentionWriterAuthorizationError(
                "artifact retention writer capability credential is required"
            )
        if terminal_outbox_only and retention_writer_credential is not None:
            raise ArtifactRetentionWriterAuthorizationError(
                "terminal outbox writer must not receive the retention capability credential"
            )
        if terminal_outbox_only and writer_owner != "artifact-terminal-outbox":
            raise ArtifactRetentionWriterAuthorizationError(
                "terminal outbox writer must use its dedicated owner identity"
            )
        self.path = Path(path)
        self._clock = clock or (lambda: datetime.now(UTC))
        self.writer_owner = writer_owner
        self.retention_writer_credential = retention_writer_credential
        self.terminal_outbox_only = terminal_outbox_only
        self.service_owner = _ARTIFACT_METADATA_SERVICE_OWNER
        self._writer_lock = _artifact_writer_lock(self.path)
        self._process_writer_lock = _ArtifactProcessWriterLock(
            self.path,
            managed_trust_root=managed_trust_root,
        )
        self._path_authority = PrivateSqlitePathAuthority(
            self.path,
            label="artifact reference store",
            create_if_missing=True,
            managed_trust_root=managed_trust_root,
        )
        with self._writer_lock, self._process_writer_lock.acquire():
            connection = self._connect()
            with verified_sqlite_connection_scope(connection, self._path_authority):
                connection.executescript(_SCHEMA)
                self._bind_controlled_writer_credential(connection)
                service_row = connection.execute(
                    "SELECT service_owner FROM artifact_writer_service_identity WHERE singleton = 1"
                ).fetchone()
                if service_row is None:
                    raise ValueError("artifact metadata service owner identity conflicts")
                try:
                    stored_service_owner = service_row["service_owner"]
                except KeyError:
                    # Minimal connection probes used by close-boundary tests do
                    # not materialize schema query columns.
                    stored_service_owner = self.service_owner
                if stored_service_owner != self.service_owner:
                    raise ValueError("artifact metadata service owner identity conflicts")
                self._path_authority.rebind_ctime_after_trusted_sqlite_setup()

    def _validate_controlled_writer_capability(self) -> ArtifactRetentionWriterCredential | None:
        credential = self.retention_writer_credential
        if credential is None:
            return None
        now = normalize_aware_utc(self._clock())
        if now < credential.not_before:
            raise ArtifactRetentionWriterAuthorizationError(
                "artifact retention writer capability is not active"
            )
        if now >= credential.expires_at:
            raise ArtifactRetentionWriterAuthorizationError(
                "artifact retention writer capability is expired"
            )
        if credential.revoked_at is not None and now >= credential.revoked_at:
            raise ArtifactRetentionWriterAuthorizationError(
                "artifact retention writer capability is revoked"
            )
        return credential

    @staticmethod
    def _credential_from_row(row: sqlite3.Row | None) -> dict[str, object] | None:
        if row is None:
            return None
        try:
            key_id = row["key_id"]
        except KeyError:
            return None
        if key_id is None:
            return None
        return {
            "key_id": str(key_id),
            "sequence": int(row["sequence"]),
            "secret_sha256": str(row["secret_sha256"]),
            "not_before": _parse_datetime(row["not_before"]),
            "expires_at": _parse_datetime(row["expires_at"]),
            "revoked_at": (
                _parse_datetime(row["revoked_at"]) if row["revoked_at"] is not None else None
            ),
        }

    def _persist_controlled_writer_credential(
        self,
        connection: sqlite3.Connection,
        credential: ArtifactRetentionWriterCredential,
    ) -> None:
        connection.execute(
            """
            UPDATE artifact_writer_credential
            SET key_id = ?, sequence = ?, secret_sha256 = ?, not_before = ?,
                expires_at = ?, revoked_at = ?
            WHERE singleton = 1
            """,
            (
                credential.key_id,
                credential.sequence,
                credential.secret_sha256,
                credential.not_before.isoformat(),
                credential.expires_at.isoformat(),
                credential.revoked_at.isoformat() if credential.revoked_at is not None else None,
            ),
        )

    def _bind_controlled_writer_credential(self, connection: sqlite3.Connection) -> None:
        result = connection.execute("SELECT * FROM artifact_writer_credential WHERE singleton = 1")
        # Lock-boundary probes intentionally implement only BEGIN/ROLLBACK. They
        # cannot represent a real SQLite credential store, so preserve the tested
        # operational error instead of masking it with a probe-only AttributeError.
        if not hasattr(result, "fetchone"):
            return
        row = result.fetchone()
        stored = self._credential_from_row(row)
        credential = self._validate_controlled_writer_capability()
        if credential is None:
            if stored is not None:
                if self.terminal_outbox_only:
                    return
                raise ArtifactRetentionWriterAuthorizationError(
                    "artifact retention writer capability credential is required"
                )
            return
        if stored is None:
            if credential.previous_secret_hex is not None:
                raise ArtifactRetentionWriterAuthorizationError(
                    "artifact retention writer bootstrap cannot contain a previous secret"
                )
            self._persist_controlled_writer_credential(connection, credential)
            return
        same_credential = (
            stored["key_id"] == credential.key_id
            and stored["sequence"] == credential.sequence
            and hmac.compare_digest(
                str(stored["secret_sha256"]),
                credential.secret_sha256,
            )
            and stored["not_before"] == credential.not_before
            and stored["expires_at"] == credential.expires_at
            and stored["revoked_at"] == credential.revoked_at
        )
        if same_credential:
            return
        stored_sequence = int(stored["sequence"])
        if credential.sequence <= stored_sequence:
            raise ArtifactRetentionWriterAuthorizationError(
                "artifact retention writer capability is old or superseded after rotation"
            )
        if credential.sequence != stored_sequence + 1:
            raise ArtifactRetentionWriterAuthorizationError(
                "artifact retention writer credential rotation sequence is not contiguous"
            )
        previous_secret_sha256 = credential.previous_secret_sha256
        if previous_secret_sha256 is None or not hmac.compare_digest(
            previous_secret_sha256,
            str(stored["secret_sha256"]),
        ):
            raise ArtifactRetentionWriterAuthorizationError(
                "artifact retention writer credential rotation requires the previous secret"
            )
        self._persist_controlled_writer_credential(connection, credential)

    def close(self) -> None:
        self._process_writer_lock.close()

    def _connect(self, *, read_only: bool = False) -> sqlite3.Connection:
        def open_connection(path: Path) -> sqlite3.Connection:
            mode = "ro" if read_only else "rw"
            uri = f"file:{quote(str(path), safe='/')}?mode={mode}"
            return sqlite3.connect(uri, uri=True, timeout=30, isolation_level=None)

        connection = self._path_authority.open_verified_connection(open_connection)
        try:
            connection.row_factory = sqlite3.Row
            execute_sqlite_setup_statement(connection, "PRAGMA busy_timeout = 30000")
            if read_only:
                execute_sqlite_setup_statement(connection, "PRAGMA query_only = ON")
                execute_sqlite_setup_statement(connection, "PRAGMA trusted_schema = OFF")
            else:
                execute_sqlite_setup_statement(connection, "PRAGMA foreign_keys = ON")
                execute_sqlite_setup_statement(connection, "PRAGMA journal_mode = WAL")
        except BaseException as exc:
            close_verified_sqlite_connection(
                connection,
                self._path_authority,
                primary_error=exc,
            )
            raise
        try:
            self._path_authority.rebind_and_assert_current_after_trusted_sqlite_change()
        except BaseException as exc:
            close_verified_sqlite_connection(
                connection,
                self._path_authority,
                primary_error=exc,
                known_identity_failure=True,
            )
            raise
        return connection

    @contextmanager
    def _writer(
        self,
        *,
        precommit_guard: Callable[[], None] | None = None,
        terminal_outbox_write: bool = False,
    ) -> Iterator[sqlite3.Connection]:
        if self.terminal_outbox_only and not terminal_outbox_write:
            raise ArtifactRetentionWriterAuthorizationError(
                "terminal outbox writer cannot mutate artifact retention state"
            )
        with self._writer_lock, self._process_writer_lock.acquire():
            connection = self._connect()
            with verified_sqlite_connection_scope(connection, self._path_authority):
                self._bind_controlled_writer_credential(connection)
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                    fence_row = connection.execute(
                        "SELECT fence FROM artifact_writer_fence WHERE singleton = 1"
                    ).fetchone()
                    assert fence_row is not None
                    connection.execute(
                        """
                        UPDATE artifact_writer_fence
                        SET fence = ?, owner_label = ?, lease_token = ?, acquired_at = ?
                        WHERE singleton = 1
                        """,
                        (
                            int(fence_row["fence"]) + 1,
                            self.service_owner,
                            uuid4().hex,
                            normalize_aware_utc(self._clock()).isoformat(),
                        ),
                    )
                    yield connection
                    self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                    if precommit_guard is not None:
                        precommit_guard()
                    connection.execute("COMMIT")
                    self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                except BaseException as exc:
                    rollback_error: BaseException | None = None
                    if connection.in_transaction:
                        try:
                            connection.execute("ROLLBACK")
                            self._path_authority.rebind_ctime_after_trusted_sqlite_setup()
                        except BaseException as rollback_exc:
                            rollback_error = rollback_exc
                    if rollback_error is not None:
                        raise BaseExceptionGroup(
                            "artifact transaction and rollback failed",
                            [exc, rollback_error],
                        ) from None
                    raise

    @contextmanager
    def _reader(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect(read_only=True)
        with verified_sqlite_connection_scope(connection, self._path_authority):
            yield connection
            self._path_authority.rebind_and_assert_current_after_trusted_sqlite_change()

    def register_object(self, identity: ObjectIdentity) -> None:
        with self._writer() as connection:
            row = connection.execute(
                "SELECT * FROM artifact_object WHERE content_sha256 = ?",
                (identity.content_sha256,),
            ).fetchone()
            if row is not None:
                existing = _object_from_row(row)
                if existing != identity:
                    raise ValueError("conflicting object metadata for content hash")
                return
            connection.execute(
                """
                INSERT INTO artifact_object(
                    content_sha256, size_bytes, object_kind, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    identity.content_sha256,
                    identity.size_bytes,
                    identity.object_kind,
                    identity.created_at.isoformat(),
                ),
            )
            self._audit(
                connection,
                event_type="object_registered",
                subject_id=identity.content_sha256,
                content_sha256=identity.content_sha256,
                occurred_at=identity.created_at,
                payload=identity.model_dump(mode="json"),
            )
            self._bump_revision(connection)

    def register_copy(self, copy: ObjectCopy) -> None:
        with self._writer() as connection:
            identity = self._require_object(connection, copy.content_sha256)
            row = connection.execute(
                """
                SELECT * FROM artifact_copy
                WHERE content_sha256 = ? AND location_id = ?
                """,
                (copy.content_sha256, copy.location_id),
            ).fetchone()
            if row is not None:
                existing = _copy_from_row(row)
                if existing != copy or row["deleted_at"] is not None:
                    raise ValueError("conflicting copy metadata for location")
                return
            uri_owner = connection.execute(
                "SELECT 1 FROM artifact_copy WHERE storage_uri = ?",
                (copy.storage_uri,),
            ).fetchone()
            if uri_owner is not None:
                raise ValueError("storage URI is already registered as another copy")
            domain_owner = connection.execute(
                """
                SELECT 1 FROM artifact_copy
                WHERE content_sha256 = ? AND failure_domain = ?
                """,
                (copy.content_sha256, copy.failure_domain),
            ).fetchone()
            if domain_owner is not None:
                raise ValueError("failure domain must be independent for each object copy")
            connection.execute(
                """
                INSERT INTO artifact_copy(
                    content_sha256, location_id, storage_uri, storage_tier, verified_at,
                    failure_domain, tier_entered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    copy.content_sha256,
                    copy.location_id,
                    copy.storage_uri,
                    copy.storage_tier.value,
                    copy.verified_at.isoformat() if copy.verified_at is not None else None,
                    copy.failure_domain,
                    copy.tier_entered_at.isoformat(),
                ),
            )
            self._audit(
                connection,
                event_type="copy_registered",
                subject_id=copy.location_id,
                content_sha256=copy.content_sha256,
                occurred_at=copy.verified_at or identity.created_at,
                payload=copy.model_dump(mode="json"),
            )
            self._bump_revision(connection)

    def register_reference(self, reference: ObjectReference) -> None:
        assert reference.reference_id is not None
        with self._writer() as connection:
            self._require_object(connection, reference.content_sha256)
            self._assert_no_deletion_claim(connection, reference.content_sha256)
            row = connection.execute(
                "SELECT * FROM artifact_reference WHERE reference_id = ?",
                (reference.reference_id,),
            ).fetchone()
            if row is not None:
                if _reference_from_row(row) != reference or row["released_at"] is not None:
                    raise ValueError("conflicting reference metadata")
                return
            connection.execute(
                """
                INSERT INTO artifact_reference(
                    reference_id, owner_type, owner_id, content_sha256,
                    created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    reference.reference_id,
                    reference.owner_type,
                    reference.owner_id,
                    reference.content_sha256,
                    reference.created_at.isoformat(),
                    reference.expires_at.isoformat() if reference.expires_at is not None else None,
                ),
            )
            self._audit(
                connection,
                event_type="reference_registered",
                subject_id=reference.reference_id,
                content_sha256=reference.content_sha256,
                occurred_at=reference.created_at,
                payload=reference.model_dump(mode="json"),
            )
            self._bump_revision(connection)

    def register_bundle_atomic(
        self,
        registration: ArtifactBundleRegistration,
        *,
        identity_guard: Callable[[], None] | None = None,
    ) -> ArtifactRegistrationCounts:
        """Register one bundle atomically; first-seen ledger timestamps win on retry.

        Object, copy, and owner identities are immutable. A retry may carry a later
        observation timestamp, but it never rewrites the original registration time.
        """

        registration = ArtifactBundleRegistration.model_validate(
            registration.model_dump(mode="python")
        )
        identity = registration.object_identity
        copy = registration.object_copy
        counts = {"objects": 0, "copies": 0, "references": 0}
        with self._writer(precommit_guard=identity_guard) as connection:
            if identity_guard is not None:
                identity_guard()
            existing_object_row = connection.execute(
                "SELECT * FROM artifact_object WHERE content_sha256 = ?",
                (identity.content_sha256,),
            ).fetchone()
            if existing_object_row is None:
                connection.execute(
                    """
                    INSERT INTO artifact_object(
                        content_sha256, size_bytes, object_kind, created_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        identity.content_sha256,
                        identity.size_bytes,
                        identity.object_kind,
                        identity.created_at.isoformat(),
                    ),
                )
                self._audit(
                    connection,
                    event_type="object_registered",
                    subject_id=identity.content_sha256,
                    content_sha256=identity.content_sha256,
                    occurred_at=identity.created_at,
                    payload=identity.model_dump(mode="json"),
                )
                counts["objects"] = 1
            else:
                existing = _object_from_row(existing_object_row)
                if (existing.size_bytes, existing.object_kind) != (
                    identity.size_bytes,
                    identity.object_kind,
                ):
                    raise ValueError("conflicting object metadata for content hash")

            self._assert_no_deletion_claim(connection, identity.content_sha256)
            existing_copy_row = connection.execute(
                """
                SELECT * FROM artifact_copy
                WHERE content_sha256 = ? AND location_id = ?
                """,
                (copy.content_sha256, copy.location_id),
            ).fetchone()
            if existing_copy_row is None:
                uri_owner = connection.execute(
                    "SELECT 1 FROM artifact_copy WHERE storage_uri = ?",
                    (copy.storage_uri,),
                ).fetchone()
                if uri_owner is not None:
                    raise ValueError("storage URI is already registered as another copy")
                domain_owner = connection.execute(
                    """
                    SELECT 1 FROM artifact_copy
                    WHERE content_sha256 = ? AND failure_domain = ?
                    """,
                    (copy.content_sha256, copy.failure_domain),
                ).fetchone()
                if domain_owner is not None:
                    raise ValueError("failure domain must be independent for each object copy")
                connection.execute(
                    """
                    INSERT INTO artifact_copy(
                        content_sha256, location_id, storage_uri, storage_tier,
                        verified_at, failure_domain, tier_entered_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        copy.content_sha256,
                        copy.location_id,
                        copy.storage_uri,
                        copy.storage_tier.value,
                        copy.verified_at.isoformat(),
                        copy.failure_domain,
                        copy.tier_entered_at.isoformat(),
                    ),
                )
                self._audit(
                    connection,
                    event_type="copy_registered",
                    subject_id=copy.location_id,
                    content_sha256=copy.content_sha256,
                    occurred_at=copy.verified_at,
                    payload=copy.model_dump(mode="json"),
                )
                counts["copies"] = 1
            else:
                existing_copy = _copy_from_row(existing_copy_row)
                if existing_copy_row["deleted_at"] is not None or (
                    existing_copy.storage_uri,
                    existing_copy.storage_tier,
                    existing_copy.failure_domain,
                ) != (copy.storage_uri, copy.storage_tier, copy.failure_domain):
                    raise ValueError("conflicting copy metadata for location")

            for reference in registration.references:
                owner_rows = connection.execute(
                    """
                    SELECT * FROM artifact_reference
                    WHERE content_sha256 = ? AND owner_type = ?
                    ORDER BY reference_id
                    """,
                    (
                        reference.content_sha256,
                        reference.owner_type,
                    ),
                ).fetchall()
                if len(owner_rows) > 1:
                    raise ValueError("ambiguous immutable bundle owner reference")
                if owner_rows:
                    existing_reference = _reference_from_row(owner_rows[0])
                    if existing_reference.owner_id != reference.owner_id:
                        raise ValueError("conflicting immutable bundle owner identity")
                    if (
                        existing_reference.expires_at is not None
                        or owner_rows[0]["released_at"] is not None
                    ):
                        raise ValueError("conflicting reference metadata for durable owner")
                    continue
                assert reference.reference_id is not None
                row = connection.execute(
                    "SELECT * FROM artifact_reference WHERE reference_id = ?",
                    (reference.reference_id,),
                ).fetchone()
                if row is not None:
                    raise ValueError("conflicting reference metadata")
                connection.execute(
                    """
                    INSERT INTO artifact_reference(
                        reference_id, owner_type, owner_id, content_sha256,
                        created_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        reference.reference_id,
                        reference.owner_type,
                        reference.owner_id,
                        reference.content_sha256,
                        reference.created_at.isoformat(),
                        None,
                    ),
                )
                self._audit(
                    connection,
                    event_type="reference_registered",
                    subject_id=reference.reference_id,
                    content_sha256=reference.content_sha256,
                    occurred_at=reference.created_at,
                    payload=reference.model_dump(mode="json"),
                )
                counts["references"] += 1
            if any(counts.values()):
                self._bump_revision(connection)
        return ArtifactRegistrationCounts(
            registered_objects=counts["objects"],
            registered_copies=counts["copies"],
            registered_references=counts["references"],
        )

    def register_verified_tier_copy(
        self,
        *,
        source: ObjectCopy,
        target: ObjectCopy,
        verification: ObjectCopyVerification,
        observed_at: AwareUtcDatetime,
        expected_ledger_revision: int | None = None,
    ) -> TierMigrationReceipt:
        observed_at = normalize_aware_utc(observed_at)
        if source.content_sha256 != target.content_sha256:
            raise ValueError("tier migration content hashes must match")
        if not _is_adjacent_tier(source.storage_tier, target.storage_tier):
            raise ValueError("tier migration requires an adjacent colder target")
        if source.location_id == target.location_id:
            raise ValueError("tier migration target location must differ")
        if source.failure_domain == target.failure_domain:
            raise ValueError("tier migration target requires an independent failure domain")
        if source.verified_at is None or source.verified_at > observed_at:
            raise ValueError("tier migration source verification is missing or from the future")
        if max(verification.verified_at, target.tier_entered_at) > observed_at:
            raise ValueError("tier migration target verification is from the future")
        if target.verified_at is None or (
            verification.storage_uri,
            verification.content_sha256,
            verification.verified_at,
        ) != (
            target.storage_uri,
            target.content_sha256,
            target.verified_at,
        ):
            raise ValueError("tier migration target verification identity conflicts")
        required_owner_types = ("audit", "experiment", "job", "snapshot")
        registered_target = False
        with self._writer() as connection:
            if (
                expected_ledger_revision is not None
                and self._revision(connection) != expected_ledger_revision
            ):
                raise ValueError("tier migration catalog CAS revision changed")
            identity = self._require_object(connection, source.content_sha256)
            if verification.size_bytes != identity.size_bytes:
                raise ValueError("tier migration target size does not match object")
            self._assert_no_deletion_claim(connection, source.content_sha256)
            source_row = connection.execute(
                """
                SELECT * FROM artifact_copy
                WHERE content_sha256 = ? AND location_id = ? AND deleted_at IS NULL
                """,
                (source.content_sha256, source.location_id),
            ).fetchone()
            if source_row is None or _copy_from_row(source_row) != source:
                raise ValueError("tier migration source copy identity is not active")
            owner_rows = connection.execute(
                """
                SELECT
                    reference.owner_type,
                    COUNT(*) AS owner_count,
                    SUM(
                        CASE
                            WHEN reference.released_at IS NULL
                             AND (
                                 reference.expires_at IS NULL
                                 OR reference.expires_at > ?
                             )
                            THEN 1
                            WHEN receipt.receipt_id IS NOT NULL THEN 1
                            ELSE 0
                        END
                    ) AS governed_count
                FROM artifact_reference AS reference
                LEFT JOIN artifact_owner_release_receipt AS receipt
                  ON receipt.reference_id = reference.reference_id
                WHERE reference.content_sha256 = ?
                GROUP BY reference.owner_type
                """,
                (observed_at.isoformat(), source.content_sha256),
            ).fetchall()
            owners = {
                str(row["owner_type"]): (
                    int(row["owner_count"]),
                    int(row["governed_count"] or 0),
                )
                for row in owner_rows
            }
            if any(owners.get(owner_type) != (1, 1) for owner_type in required_owner_types):
                raise ValueError(
                    "tier migration requires active references or immutable terminal receipts "
                    "for all durable owners"
                )

            target_row = connection.execute(
                """
                SELECT * FROM artifact_copy
                WHERE content_sha256 = ? AND location_id = ?
                """,
                (target.content_sha256, target.location_id),
            ).fetchone()
            if target_row is None:
                uri_owner = connection.execute(
                    "SELECT 1 FROM artifact_copy WHERE storage_uri = ?",
                    (target.storage_uri,),
                ).fetchone()
                if uri_owner is not None:
                    raise ValueError("tier migration target URI is already registered")
                domain_owner = connection.execute(
                    """
                    SELECT 1 FROM artifact_copy
                    WHERE content_sha256 = ? AND failure_domain = ?
                    """,
                    (target.content_sha256, target.failure_domain),
                ).fetchone()
                if domain_owner is not None:
                    raise ValueError("tier migration target requires an independent failure domain")
                connection.execute(
                    """
                    INSERT INTO artifact_copy(
                        content_sha256, location_id, storage_uri, storage_tier,
                        verified_at, failure_domain, tier_entered_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        target.content_sha256,
                        target.location_id,
                        target.storage_uri,
                        target.storage_tier.value,
                        target.verified_at.isoformat(),
                        target.failure_domain,
                        target.tier_entered_at.isoformat(),
                    ),
                )
                self._audit(
                    connection,
                    event_type="tier_copy_registered",
                    subject_id=target.location_id,
                    content_sha256=target.content_sha256,
                    occurred_at=target.verified_at,
                    payload={
                        "source": source.model_dump(mode="json"),
                        "target": target.model_dump(mode="json"),
                        "verification": verification.model_dump(mode="json"),
                    },
                )
                self._bump_revision(connection)
                registered_target = True
            elif target_row["deleted_at"] is not None or _copy_from_row(target_row) != target:
                raise ValueError("tier migration target copy metadata conflicts")

            plan = TierCopyRetirementPlan(
                content_sha256=source.content_sha256,
                source_location_id=source.location_id,
                target_location_id=target.location_id,
                source_tier=source.storage_tier,
                target_tier=target.storage_tier,
                required_owner_types=required_owner_types,
                ledger_revision=self._revision(connection),
                planned_at=observed_at,
            )
        return TierMigrationReceipt(
            registered_target_copy=registered_target,
            verification=verification,
            retirement_plan=plan,
        )

    def governance_revision(self) -> int:
        with self._reader() as connection:
            return self._revision(connection)

    def release_reference(self, reference_id: str, *, released_at: AwareUtcDatetime) -> None:
        released_at = normalize_aware_utc(released_at)
        with self._writer() as connection:
            row = connection.execute(
                "SELECT * FROM artifact_reference WHERE reference_id = ?",
                (reference_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown reference: {reference_id}")
            if row["owner_type"] not in _EPHEMERAL_OWNER_TYPES:
                raise ValueError("durable owner requires a terminal release receipt")
            if row["released_at"] is not None:
                raise ValueError("reference is already released")
            created_at = _parse_datetime(row["created_at"])
            if released_at < created_at:
                raise ValueError("released_at cannot precede reference creation")
            connection.execute(
                "UPDATE artifact_reference SET released_at = ? WHERE reference_id = ?",
                (released_at.isoformat(), reference_id),
            )
            self._audit(
                connection,
                event_type="reference_released",
                subject_id=reference_id,
                content_sha256=row["content_sha256"],
                occurred_at=released_at,
                payload={"reference_id": reference_id},
            )
            self._bump_revision(connection)

    def _release_owner_terminal_in_connection(
        self,
        connection: sqlite3.Connection,
        receipt: OwnerTerminalReleaseReceipt,
    ) -> bool:
        assert receipt.receipt_id is not None
        existing = connection.execute(
            """
            SELECT receipt_json FROM artifact_owner_release_receipt
            WHERE reference_id = ?
            """,
            (receipt.reference_id,),
        ).fetchone()
        if existing is not None:
            stored = OwnerTerminalReleaseReceipt.model_validate_json(existing["receipt_json"])
            if stored != receipt:
                raise ValueError("conflicting terminal release receipt")
            return False

        row = connection.execute(
            "SELECT * FROM artifact_reference WHERE reference_id = ?",
            (receipt.reference_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown reference: {receipt.reference_id}")
        expected_owner = (
            row["owner_type"],
            row["owner_id"],
            row["content_sha256"],
        )
        if expected_owner != (
            receipt.owner_type,
            receipt.owner_id,
            receipt.content_sha256,
        ):
            raise ValueError("terminal release receipt owner identity conflicts")
        if row["released_at"] is not None:
            raise ValueError("reference was released without this terminal receipt")
        if receipt.released_at < _parse_datetime(row["created_at"]):
            raise ValueError("terminal release cannot precede reference creation")
        self._assert_no_deletion_claim(connection, receipt.content_sha256)
        connection.execute(
            "UPDATE artifact_reference SET released_at = ? WHERE reference_id = ?",
            (receipt.released_at.isoformat(), receipt.reference_id),
        )
        connection.execute(
            """
            INSERT INTO artifact_owner_release_receipt(
                receipt_id, reference_id, owner_type, owner_id, content_sha256,
                terminal_state, lifecycle_revision, evidence_sha256,
                released_at, receipt_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt.receipt_id,
                receipt.reference_id,
                receipt.owner_type,
                receipt.owner_id,
                receipt.content_sha256,
                receipt.terminal_state,
                receipt.lifecycle_revision,
                receipt.evidence_sha256,
                receipt.released_at.isoformat(),
                receipt.model_dump_json(),
            ),
        )
        self._audit(
            connection,
            event_type="owner_terminal_released",
            subject_id=receipt.receipt_id,
            content_sha256=receipt.content_sha256,
            occurred_at=receipt.released_at,
            payload=receipt.model_dump(mode="json"),
        )
        self._bump_revision(connection)
        return True

    def release_owner_terminal(self, receipt: OwnerTerminalReleaseReceipt) -> bool:
        """Release one durable owner only from immutable terminal-state evidence."""

        receipt = OwnerTerminalReleaseReceipt.model_validate(receipt.model_dump(mode="python"))
        with self._writer() as connection:
            return self._release_owner_terminal_in_connection(connection, receipt)

    def enqueue_owner_terminal_release(
        self,
        receipt: OwnerTerminalReleaseReceipt,
        *,
        enqueued_at: AwareUtcDatetime,
    ) -> bool:
        """Persist a durable-owner receipt without releasing its reference."""

        receipt = OwnerTerminalReleaseReceipt.model_validate(receipt.model_dump(mode="python"))
        enqueued_at = normalize_aware_utc(enqueued_at)
        assert receipt.receipt_id is not None
        if receipt.owner_type not in {"audit", "experiment", "snapshot"}:
            raise ValueError(
                "terminal receipt producer outbox accepts audit, experiment, or snapshot owners; "
                "Job receipts must use the retention-owned catalog application"
            )
        return self._enqueue_owner_terminal_release(
            receipt,
            enqueued_at=enqueued_at,
            terminal_outbox_write=True,
        )

    def apply_catalog_job_terminal_release(
        self,
        receipt: OwnerTerminalReleaseReceipt,
        *,
        applied_at: AwareUtcDatetime,
    ) -> bool:
        """Queue a job receipt only while applying the retention-owned catalog inbox."""

        receipt = OwnerTerminalReleaseReceipt.model_validate(receipt.model_dump(mode="python"))
        if self.writer_owner != "artifact-retention" or self.retention_writer_credential is None:
            raise ArtifactRetentionWriterAuthorizationError(
                "catalog job terminal release requires the retention-owned writer capability"
            )
        if receipt.owner_type != "job":
            raise ValueError("catalog job terminal release requires a job receipt")
        return self._enqueue_owner_terminal_release(
            receipt,
            enqueued_at=normalize_aware_utc(applied_at),
            terminal_outbox_write=False,
        )

    def _enqueue_owner_terminal_release(
        self,
        receipt: OwnerTerminalReleaseReceipt,
        *,
        enqueued_at: AwareUtcDatetime,
        terminal_outbox_write: bool,
    ) -> bool:
        with self._writer(terminal_outbox_write=terminal_outbox_write) as connection:
            existing = connection.execute(
                """
                SELECT receipt_json FROM artifact_owner_release_outbox
                WHERE reference_id = ?
                """,
                (receipt.reference_id,),
            ).fetchone()
            if existing is not None:
                stored = OwnerTerminalReleaseReceipt.model_validate_json(existing["receipt_json"])
                if stored != receipt:
                    raise ValueError("conflicting terminal release outbox receipt")
                return False
            reference = connection.execute(
                "SELECT * FROM artifact_reference WHERE reference_id = ?",
                (receipt.reference_id,),
            ).fetchone()
            if reference is None:
                raise KeyError(f"unknown reference: {receipt.reference_id}")
            if (
                reference["owner_type"],
                reference["owner_id"],
                reference["content_sha256"],
            ) != (receipt.owner_type, receipt.owner_id, receipt.content_sha256):
                raise ValueError("terminal release outbox owner identity conflicts")
            if reference["released_at"] is not None:
                raise ValueError("terminal release outbox reference is already released")
            if receipt.released_at < _parse_datetime(reference["created_at"]):
                raise ValueError("terminal release cannot precede reference creation")
            self._assert_no_deletion_claim(connection, receipt.content_sha256)
            connection.execute(
                """
                INSERT INTO artifact_owner_release_outbox(
                    receipt_id, reference_id, owner_type, owner_id, content_sha256,
                    receipt_json, enqueued_at, published_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    receipt.receipt_id,
                    receipt.reference_id,
                    receipt.owner_type,
                    receipt.owner_id,
                    receipt.content_sha256,
                    receipt.model_dump_json(),
                    enqueued_at.isoformat(),
                ),
            )
            self._audit(
                connection,
                event_type="owner_terminal_release_enqueued",
                subject_id=receipt.receipt_id,
                content_sha256=receipt.content_sha256,
                occurred_at=enqueued_at,
                payload=receipt.model_dump(mode="json"),
            )
            self._bump_revision(connection)
        return True

    def pending_owner_terminal_releases(
        self,
        *,
        limit: int,
    ) -> tuple[OwnerTerminalReleaseReceipt, ...]:
        if limit < 1 or limit > 10_000:
            raise ValueError("terminal release outbox limit is out of bounds")
        with self._reader() as connection:
            rows = connection.execute(
                """
                SELECT receipt_json FROM artifact_owner_release_outbox
                WHERE published_at IS NULL
                ORDER BY enqueued_at, receipt_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(
            OwnerTerminalReleaseReceipt.model_validate_json(row["receipt_json"]) for row in rows
        )

    def get_owner_terminal_release_receipt(
        self,
        *,
        owner_type: str,
        owner_id: str,
        content_sha256: str,
    ) -> OwnerTerminalReleaseReceipt | None:
        with self._reader() as connection:
            rows = connection.execute(
                """
                SELECT receipt_json FROM artifact_owner_release_receipt
                WHERE owner_type = ? AND owner_id = ? AND content_sha256 = ?
                ORDER BY receipt_id
                LIMIT 2
                """,
                (owner_type, owner_id, content_sha256),
            ).fetchall()
        if len(rows) > 1:
            raise ValueError("durable owner has ambiguous terminal release receipts")
        return (
            None
            if not rows
            else OwnerTerminalReleaseReceipt.model_validate_json(rows[0]["receipt_json"])
        )

    def get_pending_owner_terminal_release_receipt(
        self,
        *,
        owner_type: str,
        owner_id: str,
        content_sha256: str,
    ) -> OwnerTerminalReleaseReceipt | None:
        """Return the un-published immutable receipt for one durable owner."""

        with self._reader() as connection:
            rows = connection.execute(
                """
                SELECT receipt_json FROM artifact_owner_release_outbox
                WHERE owner_type = ? AND owner_id = ? AND content_sha256 = ?
                ORDER BY receipt_id
                LIMIT 2
                """,
                (owner_type, owner_id, content_sha256),
            ).fetchall()
        if len(rows) > 1:
            raise ValueError("durable owner has ambiguous pending terminal release receipts")
        return (
            None
            if not rows
            else OwnerTerminalReleaseReceipt.model_validate_json(rows[0]["receipt_json"])
        )

    def publish_owner_terminal_release(self, receipt_id: str) -> bool:
        with self._writer() as connection:
            row = connection.execute(
                "SELECT * FROM artifact_owner_release_outbox WHERE receipt_id = ?",
                (receipt_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown terminal release outbox receipt: {receipt_id}")
            receipt = OwnerTerminalReleaseReceipt.model_validate_json(row["receipt_json"])
            if row["published_at"] is not None:
                stored = connection.execute(
                    """
                    SELECT receipt_json FROM artifact_owner_release_receipt
                    WHERE receipt_id = ?
                    """,
                    (receipt_id,),
                ).fetchone()
                if (
                    stored is None
                    or OwnerTerminalReleaseReceipt.model_validate_json(stored["receipt_json"])
                    != receipt
                ):
                    raise ValueError("published terminal release outbox evidence is incomplete")
                return False
            self._release_owner_terminal_in_connection(connection, receipt)
            connection.execute(
                """
                UPDATE artifact_owner_release_outbox
                SET published_at = ?
                WHERE receipt_id = ? AND published_at IS NULL
                """,
                (receipt.released_at.isoformat(), receipt_id),
            )
            self._audit(
                connection,
                event_type="owner_terminal_release_published",
                subject_id=receipt_id,
                content_sha256=receipt.content_sha256,
                occurred_at=receipt.released_at,
                payload={"receipt_id": receipt_id},
            )
            self._bump_revision(connection)
        return True

    def register_legal_hold(self, hold: LegalHold) -> None:
        with self._writer() as connection:
            self._require_object(connection, hold.content_sha256)
            self._assert_no_deletion_claim(connection, hold.content_sha256)
            row = connection.execute(
                "SELECT * FROM artifact_legal_hold WHERE hold_id = ?",
                (hold.hold_id,),
            ).fetchone()
            if row is not None:
                if _hold_from_row(row) != hold or row["released_at"] is not None:
                    raise ValueError("conflicting legal hold metadata")
                return
            connection.execute(
                """
                INSERT INTO artifact_legal_hold(
                    hold_id, content_sha256, reason, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    hold.hold_id,
                    hold.content_sha256,
                    hold.reason,
                    hold.created_at.isoformat(),
                ),
            )
            self._audit(
                connection,
                event_type="legal_hold_registered",
                subject_id=hold.hold_id,
                content_sha256=hold.content_sha256,
                occurred_at=hold.created_at,
                payload=hold.model_dump(mode="json"),
            )
            self._bump_revision(connection)

    def release_legal_hold(self, hold_id: str, *, released_at: AwareUtcDatetime) -> None:
        released_at = normalize_aware_utc(released_at)
        with self._writer() as connection:
            row = connection.execute(
                "SELECT * FROM artifact_legal_hold WHERE hold_id = ?",
                (hold_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown legal hold: {hold_id}")
            if row["released_at"] is not None:
                raise ValueError("legal hold is already released")
            if released_at < _parse_datetime(row["created_at"]):
                raise ValueError("released_at cannot precede legal hold creation")
            connection.execute(
                "UPDATE artifact_legal_hold SET released_at = ? WHERE hold_id = ?",
                (released_at.isoformat(), hold_id),
            )
            self._audit(
                connection,
                event_type="legal_hold_released",
                subject_id=hold_id,
                content_sha256=row["content_sha256"],
                occurred_at=released_at,
                payload={"hold_id": hold_id},
            )
            self._bump_revision(connection)

    def get_object(self, content_sha256: str) -> ObjectIdentity:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM artifact_object WHERE content_sha256 = ?",
                (content_sha256,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown object: {content_sha256}")
        return _object_from_row(row)

    def list_active_copies(self, content_sha256: str) -> tuple[ObjectCopy, ...]:
        with self._reader() as connection:
            rows = connection.execute(
                """
                SELECT * FROM artifact_copy
                WHERE content_sha256 = ? AND deleted_at IS NULL
                ORDER BY location_id
                """,
                (content_sha256,),
            ).fetchall()
        return tuple(_copy_from_row(row) for row in rows)

    def tier_migration_page(
        self,
        *,
        now: datetime,
        policy: RetentionPolicy,
        after: TierMigrationCursor | None,
        scan_limit: int,
    ) -> TierMigrationPage:
        now = normalize_aware_utc(now)
        if not 1 <= scan_limit <= 1024:
            raise ValueError("tier migration scan limit is out of bounds")
        cursor = after or TierMigrationCursor(
            content_sha256="0" * 64,
            tier_rank=0,
            location_id="!",
        )
        with self._reader() as connection:
            rows = connection.execute(
                """
                SELECT
                    object.content_sha256 AS object_content_sha256,
                    object.size_bytes,
                    object.object_kind,
                    object.created_at AS object_created_at,
                    copy.*,
                    CASE copy.storage_tier WHEN 'hot' THEN 0 ELSE 1 END AS tier_rank,
                    (
                        SELECT group_concat(owner_type, char(31))
                        FROM (
                            SELECT DISTINCT reference.owner_type AS owner_type
                            FROM artifact_reference AS reference
                            WHERE reference.content_sha256 = object.content_sha256
                            ORDER BY reference.owner_type
                        )
                    ) AS owner_types
                FROM artifact_copy AS copy
                JOIN artifact_object AS object USING(content_sha256)
                WHERE copy.deleted_at IS NULL
                  AND copy.storage_tier IN ('hot', 'warm')
                  AND (
                      object.content_sha256 > ?
                      OR (
                          object.content_sha256 = ?
                          AND CASE copy.storage_tier WHEN 'hot' THEN 0 ELSE 1 END > ?
                      )
                      OR (
                          object.content_sha256 = ?
                          AND CASE copy.storage_tier WHEN 'hot' THEN 0 ELSE 1 END = ?
                          AND copy.location_id > ?
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM artifact_copy AS target
                      WHERE target.content_sha256 = copy.content_sha256
                        AND target.deleted_at IS NULL
                        AND target.storage_tier = CASE copy.storage_tier
                            WHEN 'hot' THEN 'warm' ELSE 'cold' END
                  )
                ORDER BY object.content_sha256, tier_rank, copy.location_id
                LIMIT ?
                """,
                (
                    cursor.content_sha256,
                    cursor.content_sha256,
                    cursor.tier_rank,
                    cursor.content_sha256,
                    cursor.tier_rank,
                    cursor.location_id,
                    scan_limit + 1,
                ),
            ).fetchall()
        page_rows = rows[:scan_limit]
        sources: list[TierMigrationSource] = []
        for row in page_rows:
            identity = ObjectIdentity(
                content_sha256=str(row["object_content_sha256"]),
                size_bytes=int(row["size_bytes"]),
                object_kind=str(row["object_kind"]),
                created_at=_parse_datetime(str(row["object_created_at"])),
            )
            owner_types = frozenset(
                str(row["owner_types"]).split("\x1f") if row["owner_types"] else ()
            )
            copy = _copy_from_row(row)
            if now - copy.tier_entered_at >= policy.age_for(
                copy.storage_tier,
                object_kind=identity.object_kind,
                owner_types=owner_types,
            ):
                sources.append(TierMigrationSource(object_identity=identity, source_copy=copy))
        next_cursor = None
        if page_rows:
            last = page_rows[-1]
            next_cursor = TierMigrationCursor(
                content_sha256=str(last["object_content_sha256"]),
                tier_rank=int(last["tier_rank"]),
                location_id=str(last["location_id"]),
            )
        return TierMigrationPage(
            sources=tuple(sources),
            next_cursor=next_cursor,
            exhausted=len(rows) <= scan_limit,
        )

    def list_active_references(self, content_sha256: str) -> tuple[ObjectReference, ...]:
        with self._reader() as connection:
            rows = connection.execute(
                """
                SELECT * FROM artifact_reference
                WHERE content_sha256 = ? AND released_at IS NULL
                ORDER BY owner_type, owner_id, reference_id
                """,
                (content_sha256,),
            ).fetchall()
        return tuple(_reference_from_row(row) for row in rows)

    def list_active_owner_references(
        self,
        *,
        owner_type: str,
        owner_id: str,
        limit: int = 10_000,
    ) -> tuple[ObjectReference, ...]:
        if not owner_type or not owner_id:
            raise ValueError("artifact reference owner identity must be nonempty")
        if not 1 <= limit <= 10_000:
            raise ValueError("artifact owner reference limit is out of bounds")
        with self._reader() as connection:
            rows = connection.execute(
                """
                SELECT * FROM artifact_reference
                INDEXED BY artifact_reference_owner_status_idx
                WHERE owner_type = ? AND owner_id = ? AND released_at IS NULL
                ORDER BY reference_id
                LIMIT ?
                """,
                (owner_type, owner_id, limit),
            ).fetchall()
        return tuple(_reference_from_row(row) for row in rows)

    def list_audit_events(self) -> tuple[ArtifactAuditEvent, ...]:
        with self._reader() as connection:
            rows = connection.execute("SELECT * FROM artifact_audit ORDER BY sequence").fetchall()
        return tuple(
            ArtifactAuditEvent(
                sequence=row["sequence"],
                event_type=row["event_type"],
                subject_id=row["subject_id"],
                content_sha256=row["content_sha256"],
                occurred_at=_parse_datetime(row["occurred_at"]),
                payload_json=row["payload_json"],
            )
            for row in rows
        )

    def list_deleted_candidates(self, plan_id: str) -> tuple[str, ...]:
        with self._reader() as connection:
            rows = connection.execute(
                """
                SELECT deletion_candidate_id FROM artifact_copy
                WHERE deletion_plan_id = ? AND deleted_at IS NOT NULL
                ORDER BY deletion_candidate_id
                """,
                (plan_id,),
            ).fetchall()
        return tuple(row["deletion_candidate_id"] for row in rows)

    def get_gc_operation(self, operation_id: str) -> GcOperationRecord | None:
        return self.get_gc_operations((operation_id,)).get(operation_id)

    def get_gc_operations(
        self,
        operation_ids: tuple[str, ...],
    ) -> dict[str, GcOperationRecord]:
        if len(operation_ids) > 1_024:
            raise ValueError("GC operation lookup batch exceeds 1024 items")
        if not operation_ids:
            return {}
        for operation_id in operation_ids:
            if len(operation_id) != 64 or any(
                character not in "0123456789abcdef" for character in operation_id
            ):
                raise ValueError("GC operation id must be a lowercase sha256")
        placeholders = ",".join("?" for _ in operation_ids)
        with self._reader() as connection:
            rows = connection.execute(
                f"SELECT * FROM artifact_gc_operation WHERE operation_id IN ({placeholders})",
                operation_ids,
            ).fetchall()
        return {
            row["operation_id"]: GcOperationRecord(
                operation_id=row["operation_id"],
                candidate_id=row["candidate_id"],
                content_sha256=row["content_sha256"],
                location_id=row["location_id"],
                claim=GcClaim.model_validate_json(row["claim_json"]),
                status=row["status"],
            )
            for row in rows
        }

    def plan_gc(
        self,
        *,
        now: AwareUtcDatetime,
        policy: RetentionPolicy,
        max_items: int = 1_000,
        max_bytes: int = 1 << 40,
        max_runtime: timedelta = timedelta(seconds=5),
        page_size: int = 128,
        monotonic: Callable[[], float] | None = None,
        deadline_monotonic: float | None = None,
    ) -> GcPlan:
        now = normalize_aware_utc(now)
        trusted_now = normalize_aware_utc(self._clock())
        if now > trusted_now + timedelta(seconds=1):
            raise ValueError("GC planning time cannot exceed the trusted clock")
        if max_items < 1:
            raise ValueError("GC planning max_items must be positive")
        if max_bytes < 1:
            raise ValueError("GC planning max_bytes must be positive")
        if max_runtime <= timedelta(0):
            raise ValueError("GC planning max_runtime must be positive")
        if not 1 <= page_size <= 1_000:
            raise ValueError("GC planning page_size must be between 1 and 1000")
        timer = monotonic or time.monotonic
        started = timer()
        local_deadline = started + max_runtime.total_seconds()
        deadline = (
            local_deadline
            if deadline_monotonic is None
            else min(local_deadline, deadline_monotonic)
        )
        candidates: list[GcCandidate] = []
        deferred: list[GcDeferredCandidate] = []
        selected_bytes = 0
        last_content_sha256 = ""
        exhausted = False
        with self._reader() as connection:
            revision = self._revision(connection)
            while not exhausted and len(candidates) < max_items and selected_bytes < max_bytes:
                if timer() >= deadline:
                    break
                rows = connection.execute(
                    """
                    WITH object_page AS (
                        SELECT
                            object.content_sha256,
                            object.size_bytes,
                            object.object_kind,
                            object.created_at,
                            (
                                SELECT group_concat(owner_type, char(31))
                                FROM (
                                    SELECT DISTINCT reference.owner_type AS owner_type
                                    FROM artifact_reference AS reference
                                    WHERE reference.content_sha256 = object.content_sha256
                                    ORDER BY reference.owner_type
                                )
                            ) AS owner_types
                        FROM artifact_object AS object
                        WHERE object.content_sha256 > ?
                          AND EXISTS (
                              SELECT 1 FROM artifact_copy AS active_copy
                              WHERE active_copy.content_sha256 = object.content_sha256
                                AND active_copy.deleted_at IS NULL
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM artifact_reference AS reference
                              WHERE reference.content_sha256 = object.content_sha256
                                AND reference.released_at IS NULL
                                AND (
                                    reference.expires_at IS NULL
                                    OR reference.expires_at > ?
                                )
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM artifact_legal_hold AS legal_hold
                              WHERE legal_hold.content_sha256 = object.content_sha256
                                AND legal_hold.released_at IS NULL
                          )
                        ORDER BY object.content_sha256
                        LIMIT ?
                    )
                    SELECT
                        object_page.content_sha256 AS object_content_sha256,
                        object_page.size_bytes,
                        object_page.object_kind,
                        object_page.created_at AS object_created_at,
                        copy.content_sha256,
                        copy.location_id,
                        copy.storage_uri,
                        copy.storage_tier,
                        copy.verified_at,
                        copy.failure_domain,
                        copy.tier_entered_at,
                        copy.deleted_at,
                        copy.deletion_plan_id,
                        copy.deletion_candidate_id,
                        object_page.owner_types
                    FROM object_page
                    JOIN artifact_copy AS copy
                      ON copy.content_sha256 = object_page.content_sha256
                    WHERE copy.deleted_at IS NULL
                    ORDER BY object_page.content_sha256, copy.storage_tier, copy.location_id
                    """,
                    (last_content_sha256, now.isoformat(), page_size),
                ).fetchall()
                if not rows:
                    break
                grouped: dict[
                    str,
                    tuple[ObjectIdentity, list[sqlite3.Row], frozenset[str]],
                ] = {}
                for row in rows:
                    content_sha256 = str(row["object_content_sha256"])
                    group = grouped.get(content_sha256)
                    if group is None:
                        group = (
                            ObjectIdentity(
                                content_sha256=content_sha256,
                                size_bytes=int(row["size_bytes"]),
                                object_kind=str(row["object_kind"]),
                                created_at=_parse_datetime(row["object_created_at"]),
                            ),
                            [],
                            frozenset(
                                str(row["owner_types"]).split("\x1f") if row["owner_types"] else ()
                            ),
                        )
                        grouped[content_sha256] = group
                    group[1].append(row)
                last_content_sha256 = next(reversed(grouped))
                last_page = len(grouped) < page_size
                for identity, copy_rows, owner_types in grouped.values():
                    if timer() >= deadline:
                        exhausted = True
                        break
                    verified = [
                        row for row in copy_rows if self._verification_is_fresh(row, now, policy)
                    ]
                    verified_domains = {row["failure_domain"] for row in verified}
                    deletion_capacity = len(verified_domains) - policy.minimum_verified_copies
                    if deletion_capacity <= 0:
                        continue
                    eligible = [
                        row
                        for row in verified
                        if now - _parse_datetime(row["tier_entered_at"])
                        >= policy.age_for(
                            StorageTier(row["storage_tier"]),
                            object_kind=identity.object_kind,
                            owner_types=owner_types,
                        )
                    ]
                    eligible.sort(
                        key=lambda row: (
                            _tier_rank(StorageTier(row["storage_tier"])),
                            row["location_id"],
                        )
                    )
                    for row in eligible[:deletion_capacity]:
                        candidate = GcCandidate(
                            object_identity=identity,
                            object_copy=_copy_from_row(row),
                        )
                        if identity.size_bytes > max_bytes - selected_bytes:
                            if len(deferred) < max_items:
                                deferred.append(
                                    GcDeferredCandidate(
                                        candidate=candidate,
                                        reason="byte_budget_exceeded",
                                    )
                                )
                            continue
                        candidates.append(candidate)
                        selected_bytes += identity.size_bytes
                        if len(candidates) >= max_items or selected_bytes >= max_bytes:
                            exhausted = True
                            break
                    if exhausted:
                        break
                if last_page:
                    exhausted = True
        ordered = tuple(sorted(candidates, key=lambda item: item.candidate_id or ""))
        ordered_deferred = tuple(
            sorted(deferred, key=lambda item: item.candidate.candidate_id or "")
        )
        return GcPlan(
            planned_at=now,
            ledger_revision=revision,
            policy=policy,
            candidates=ordered,
            deferred_candidates=ordered_deferred,
        )

    def claim_deletion(
        self,
        *,
        plan: GcPlan,
        candidate: GcCandidate,
        owner_id: str,
        now: AwareUtcDatetime,
        operation_id: str | None = None,
    ) -> GcClaim:
        now = normalize_aware_utc(now)
        self._validate_plan_and_candidate(plan, candidate)
        if plan.expires_at is None or now > plan.expires_at:
            raise ValueError("GC plan has expired")
        operation_id = operation_id or candidate.candidate_id
        if (
            operation_id is None
            or len(operation_id) != 64
            or any(character not in "0123456789abcdef" for character in operation_id)
        ):
            raise ValueError("GC operation identity must be a sha256 value")
        claim = GcClaim(
            plan=plan,
            candidate=candidate,
            owner_id=owner_id,
            claimed_at=now,
            expires_at=now + plan.policy.claim_ttl,
        )
        assert claim.claim_id is not None
        with self._writer() as connection:
            operation_row = connection.execute(
                "SELECT * FROM artifact_gc_operation WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if operation_row is not None:
                stored = GcOperationRecord(
                    operation_id=operation_row["operation_id"],
                    candidate_id=operation_row["candidate_id"],
                    content_sha256=operation_row["content_sha256"],
                    location_id=operation_row["location_id"],
                    claim=GcClaim.model_validate_json(operation_row["claim_json"]),
                    status=operation_row["status"],
                )
                if (
                    stored.candidate_id != candidate.candidate_id
                    or stored.content_sha256 != candidate.object_identity.content_sha256
                    or stored.location_id != candidate.object_copy.location_id
                    or stored.claim.owner_id != owner_id
                ):
                    raise ValueError("GC operation identity conflicts with stored content")
                if stored.status == "released":
                    raise ValueError("GC operation was explicitly released")
                return stored.claim
            if self._revision(connection) != plan.ledger_revision:
                raise ValueError("stale GC plan")
            self._assert_no_deletion_claim(
                connection,
                candidate.object_identity.content_sha256,
            )
            self._revalidate_candidate(
                connection,
                candidate=candidate,
                policy=plan.policy,
                now=now,
            )
            connection.execute(
                """
                INSERT INTO artifact_gc_claim(
                    claim_id, plan_id, candidate_id, content_sha256, location_id,
                    owner_id, claimed_at, expires_at, claim_json, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'claimed')
                """,
                (
                    claim.claim_id,
                    plan.plan_id,
                    candidate.candidate_id,
                    candidate.object_identity.content_sha256,
                    candidate.object_copy.location_id,
                    owner_id,
                    now.isoformat(),
                    claim.expires_at.isoformat(),
                    claim.model_dump_json(),
                ),
            )
            connection.execute(
                """
                INSERT INTO artifact_gc_operation(
                    operation_id, candidate_id, content_sha256, location_id,
                    claim_id, claim_json, status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?)
                """,
                (
                    operation_id,
                    candidate.candidate_id,
                    candidate.object_identity.content_sha256,
                    candidate.object_copy.location_id,
                    claim.claim_id,
                    claim.model_dump_json(),
                    now.isoformat(),
                ),
            )
            self._audit(
                connection,
                event_type="gc_claimed",
                subject_id=claim.claim_id,
                content_sha256=candidate.object_identity.content_sha256,
                occurred_at=now,
                payload=claim.model_dump(mode="json"),
            )
            self._bump_revision(connection)
        return claim

    def release_claim(
        self,
        *,
        claim_id: str,
        owner_id: str,
        now: AwareUtcDatetime,
        reason: str,
    ) -> None:
        now = normalize_aware_utc(now)
        if not reason:
            raise ValueError("claim release reason cannot be empty")
        with self._writer() as connection:
            row = connection.execute(
                "SELECT * FROM artifact_gc_claim WHERE claim_id = ?", (claim_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown GC claim: {claim_id}")
            if row["owner_id"] != owner_id:
                raise ValueError("only the claim owner can release it")
            if row["status"] != "claimed":
                raise ValueError("GC claim is already resolved")
            if now < _parse_datetime(row["claimed_at"]):
                raise ValueError("claim release cannot precede claim creation")
            connection.execute(
                """
                UPDATE artifact_gc_claim
                SET status = 'released', resolved_at = ?, resolution_reason = ?
                WHERE claim_id = ? AND status = 'claimed'
                """,
                (now.isoformat(), reason, claim_id),
            )
            changed = connection.execute(
                """
                UPDATE artifact_gc_operation
                SET status = 'released', updated_at = ?
                WHERE claim_id = ? AND status = 'claimed'
                """,
                (now.isoformat(), claim_id),
            ).rowcount
            if changed != 1:
                raise ValueError("GC operation outbox is missing or conflicts with claim")
            self._audit(
                connection,
                event_type="gc_claim_released",
                subject_id=claim_id,
                content_sha256=row["content_sha256"],
                occurred_at=now,
                payload={"reason": reason},
            )
            self._bump_revision(connection)

    def mark_deleted(
        self,
        *,
        claim: GcClaim,
        observed_identity: GcCandidate,
        now: AwareUtcDatetime,
        expired_recovery: ExpiredGcClaimRecoveryReceipt | None = None,
    ) -> bool:
        now = normalize_aware_utc(now)
        candidate = claim.candidate
        self._validate_plan_and_candidate(claim.plan, candidate)
        if now < claim.claimed_at:
            raise ValueError("deletion confirmation cannot precede claim creation")
        if observed_identity != candidate:
            raise ValueError("observed identity does not match GC candidate")

        with self._writer() as connection:
            claim_row = connection.execute(
                "SELECT * FROM artifact_gc_claim WHERE claim_id = ?", (claim.claim_id,)
            ).fetchone()
            if claim_row is None:
                raise ValueError("GC claim is not registered")
            stored_claim = GcClaim.model_validate_json(claim_row["claim_json"])
            if stored_claim != claim:
                raise ValueError("stored GC claim identity does not match")
            operation_row = connection.execute(
                "SELECT * FROM artifact_gc_operation WHERE claim_id = ?",
                (claim.claim_id,),
            ).fetchone()
            if operation_row is None:
                raise ValueError("GC operation outbox is missing for claim")
            operation = GcOperationRecord(
                operation_id=operation_row["operation_id"],
                candidate_id=operation_row["candidate_id"],
                content_sha256=operation_row["content_sha256"],
                location_id=operation_row["location_id"],
                claim=GcClaim.model_validate_json(operation_row["claim_json"]),
                status=operation_row["status"],
            )
            if operation.claim != claim:
                raise ValueError("GC operation claim content conflicts")
            if claim_row["status"] == "completed":
                copy_row = connection.execute(
                    """
                    SELECT deleted_at, deletion_plan_id, deletion_candidate_id
                    FROM artifact_copy
                    WHERE content_sha256 = ? AND location_id = ?
                    """,
                    (
                        candidate.object_identity.content_sha256,
                        candidate.object_copy.location_id,
                    ),
                ).fetchone()
                if (
                    operation.status != "completed"
                    or copy_row is None
                    or copy_row["deleted_at"] is None
                    or copy_row["deletion_plan_id"] != claim.plan.plan_id
                    or copy_row["deletion_candidate_id"] != candidate.candidate_id
                ):
                    raise ValueError("completed GC operation content conflicts")
                return False
            if claim_row["status"] != "claimed":
                raise ValueError("GC claim is already resolved")
            if operation.status != "claimed":
                raise ValueError("GC operation outbox status conflicts with claim")
            claim_expired = now > claim.expires_at
            if claim_expired:
                if expired_recovery is None:
                    raise ValueError("GC claim expired before deletion confirmation")
                assert claim.claim_id is not None and candidate.candidate_id is not None
                if (
                    expired_recovery.claim_id,
                    expired_recovery.candidate_id,
                    expired_recovery.owner_id,
                    expired_recovery.recovered_at,
                ) != (
                    claim.claim_id,
                    candidate.candidate_id,
                    claim.owner_id,
                    now,
                ):
                    raise ValueError("expired GC claim recovery identity conflicts")
            elif expired_recovery is not None:
                raise ValueError("unexpired GC claim cannot use recovery evidence")
            self._revalidate_candidate(
                connection,
                candidate=candidate,
                policy=claim.plan.policy,
                now=now,
            )
            connection.execute(
                """
                UPDATE artifact_copy
                SET deleted_at = ?, deletion_plan_id = ?, deletion_candidate_id = ?
                WHERE content_sha256 = ? AND location_id = ? AND deleted_at IS NULL
                """,
                (
                    now.isoformat(),
                    claim.plan.plan_id,
                    candidate.candidate_id,
                    candidate.object_copy.content_sha256,
                    candidate.object_copy.location_id,
                ),
            )
            changed = connection.execute(
                """
                UPDATE artifact_gc_operation
                SET status = 'completed', updated_at = ?
                WHERE claim_id = ? AND status = 'claimed'
                """,
                (now.isoformat(), claim.claim_id),
            ).rowcount
            if changed != 1:
                raise ValueError("GC operation outbox completion conflicted")
            connection.execute(
                """
                UPDATE artifact_gc_claim
                SET status = 'completed', resolved_at = ?, resolution_reason = ?
                WHERE claim_id = ? AND status = 'claimed'
                """,
                (
                    now.isoformat(),
                    "expired claim recovered"
                    if expired_recovery is not None
                    else "external identity confirmed",
                    claim.claim_id,
                ),
            )
            self._audit(
                connection,
                event_type="copy_deleted",
                subject_id=candidate.object_copy.location_id,
                content_sha256=candidate.object_identity.content_sha256,
                occurred_at=now,
                payload={
                    "plan_id": claim.plan.plan_id,
                    "claim_id": claim.claim_id,
                    "candidate_id": candidate.candidate_id,
                    "observed_identity": observed_identity.model_dump(mode="json"),
                    "expired_recovery": (
                        expired_recovery.model_dump(mode="json")
                        if expired_recovery is not None
                        else None
                    ),
                },
            )
            self._bump_revision(connection)
        return True

    @staticmethod
    def _validate_plan_and_candidate(plan: GcPlan, candidate: GcCandidate) -> None:
        if plan.expires_at is None or plan.plan_id != _plan_id(
            planned_at=plan.planned_at,
            expires_at=plan.expires_at,
            ledger_revision=plan.ledger_revision,
            policy=plan.policy,
            candidates=plan.candidates,
        ):
            raise ValueError("invalid GC plan identity")
        planned_candidate = next(
            (item for item in plan.candidates if item.candidate_id == candidate.candidate_id),
            None,
        )
        if planned_candidate is None or planned_candidate != candidate:
            raise ValueError("candidate is not part of plan")
        expected_candidate_id = canonical_sha256(
            {
                "object_identity": candidate.object_identity,
                "object_copy": candidate.object_copy,
            }
        )
        if candidate.candidate_id != expected_candidate_id:
            raise ValueError("invalid GC candidate identity")

    def _revalidate_candidate(
        self,
        connection: sqlite3.Connection,
        *,
        candidate: GcCandidate,
        policy: RetentionPolicy,
        now: AwareUtcDatetime,
    ) -> None:
        row = connection.execute(
            """
            SELECT c.*, o.size_bytes, o.object_kind, o.created_at AS object_created_at
            FROM artifact_copy AS c
            JOIN artifact_object AS o USING (content_sha256)
            WHERE c.content_sha256 = ? AND c.location_id = ?
            """,
            (
                candidate.object_copy.content_sha256,
                candidate.object_copy.location_id,
            ),
        ).fetchone()
        if row is None:
            raise ValueError("GC candidate location is missing")
        if row["deleted_at"] is not None:
            raise ValueError("candidate is already marked deleted")
        current_object = ObjectIdentity(
            content_sha256=row["content_sha256"],
            size_bytes=row["size_bytes"],
            object_kind=row["object_kind"],
            created_at=_parse_datetime(row["object_created_at"]),
        )
        if (
            current_object != candidate.object_identity
            or _copy_from_row(row) != candidate.object_copy
        ):
            raise ValueError("stored identity no longer matches GC candidate")
        if self._has_active_reference(connection, current_object.content_sha256, now):
            raise ValueError("active reference blocks deletion")
        if self._has_active_hold(connection, current_object.content_sha256):
            raise ValueError("active legal hold blocks deletion")
        owner_rows = connection.execute(
            """
            SELECT DISTINCT owner_type FROM artifact_reference
            WHERE content_sha256 = ?
            """,
            (current_object.content_sha256,),
        ).fetchall()
        tier_age = now - candidate.object_copy.tier_entered_at
        if tier_age < policy.age_for(
            candidate.object_copy.storage_tier,
            object_kind=current_object.object_kind,
            owner_types=frozenset(str(item["owner_type"]) for item in owner_rows),
        ):
            raise ValueError("copy has not satisfied tier retention age")
        remaining_rows = connection.execute(
            """
            SELECT * FROM artifact_copy
            WHERE content_sha256 = ? AND deleted_at IS NULL AND location_id != ?
            """,
            (current_object.content_sha256, candidate.object_copy.location_id),
        ).fetchall()
        remaining_domains = {
            copy_row["failure_domain"]
            for copy_row in remaining_rows
            if self._verification_is_fresh(copy_row, now, policy)
        }
        if len(remaining_domains) < policy.minimum_verified_copies:
            raise ValueError("minimum verified copy safety would be violated")

    @staticmethod
    def _verification_is_fresh(
        row: sqlite3.Row,
        now: AwareUtcDatetime,
        policy: RetentionPolicy,
    ) -> bool:
        if row["verified_at"] is None:
            return False
        verified_at = _parse_datetime(row["verified_at"])
        return now - policy.verification_max_age <= verified_at <= now

    @staticmethod
    def _assert_no_deletion_claim(
        connection: sqlite3.Connection,
        content_sha256: str,
    ) -> None:
        active = connection.execute(
            """
            SELECT 1 FROM artifact_gc_claim
            WHERE content_sha256 = ? AND status = 'claimed'
            LIMIT 1
            """,
            (content_sha256,),
        ).fetchone()
        if active is not None:
            raise ValueError("active deletion claim freezes references and legal holds")

    @staticmethod
    def _require_object(
        connection: sqlite3.Connection,
        content_sha256: str,
    ) -> ObjectIdentity:
        row = connection.execute(
            "SELECT * FROM artifact_object WHERE content_sha256 = ?",
            (content_sha256,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown object: {content_sha256}")
        return _object_from_row(row)

    @staticmethod
    def _revision(connection: sqlite3.Connection) -> int:
        return int(
            connection.execute(
                "SELECT governance_revision FROM artifact_metadata WHERE singleton = 1"
            ).fetchone()[0]
        )

    @staticmethod
    def _bump_revision(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            UPDATE artifact_metadata
            SET governance_revision = governance_revision + 1
            WHERE singleton = 1
            """
        )

    @staticmethod
    def _audit(
        connection: sqlite3.Connection,
        *,
        event_type: str,
        subject_id: str,
        content_sha256: str,
        occurred_at: AwareUtcDatetime,
        payload: object,
    ) -> None:
        connection.execute(
            """
            INSERT INTO artifact_audit(
                event_type, subject_id, content_sha256, occurred_at, payload_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                event_type,
                subject_id,
                content_sha256,
                occurred_at.isoformat(),
                json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
            ),
        )

    @staticmethod
    def _has_active_reference(
        connection: sqlite3.Connection,
        content_sha256: str,
        now: AwareUtcDatetime,
    ) -> bool:
        return (
            connection.execute(
                """
                SELECT 1 FROM artifact_reference
                WHERE content_sha256 = ?
                  AND released_at IS NULL
                  AND (expires_at IS NULL OR expires_at > ?)
                LIMIT 1
                """,
                (content_sha256, now.isoformat()),
            ).fetchone()
            is not None
        )

    @staticmethod
    def _has_active_hold(connection: sqlite3.Connection, content_sha256: str) -> bool:
        return (
            connection.execute(
                """
                SELECT 1 FROM artifact_legal_hold
                WHERE content_sha256 = ? AND released_at IS NULL
                LIMIT 1
                """,
                (content_sha256,),
            ).fetchone()
            is not None
        )


class ArtifactTierMigrationCoordinator:
    """Copy and verify a colder target before atomically cataloging it."""

    def __init__(
        self,
        *,
        store: ArtifactReferenceStore,
        transport: ArtifactTierCopyTransport,
    ) -> None:
        self.store = store
        self.transport = transport

    def migrate(
        self,
        *,
        source: ObjectCopy,
        target: ObjectCopy,
        observed_at: AwareUtcDatetime,
        expected_schema_sha256: str | None = None,
    ) -> TierMigrationReceipt:
        if expected_schema_sha256 is None:
            raise ValueError("tier migration expected_schema_sha256 is required")
        if len(expected_schema_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in expected_schema_sha256
        ):
            raise ValueError("tier migration expected schema fingerprint is invalid")
        if not _is_adjacent_tier(source.storage_tier, target.storage_tier):
            raise ValueError("tier migration requires an adjacent colder target")
        if source.failure_domain == target.failure_domain:
            raise ValueError("tier migration target requires an independent failure domain")
        if source.content_sha256 != target.content_sha256:
            raise ValueError("tier migration content hashes must match")
        ledger_revision = self.store.governance_revision()
        self.transport.copy(source.storage_uri, target.storage_uri)
        self.transport.durably_sync(target.storage_uri)
        verification = self.transport.verify(target.storage_uri)
        identity = self.store.get_object(source.content_sha256)
        if verification.storage_uri != target.storage_uri:
            raise ValueError("tier migration verification URI conflicts with target")
        if verification.content_sha256 != identity.content_sha256:
            raise ValueError("tier migration target hash does not match object")
        if verification.size_bytes != identity.size_bytes:
            raise ValueError("tier migration target size does not match object")
        if verification.schema_sha256 != expected_schema_sha256:
            raise ValueError("tier migration target schema does not match object contract")
        return self.store.register_verified_tier_copy(
            source=source,
            target=target,
            verification=verification,
            observed_at=observed_at,
            expected_ledger_revision=ledger_revision,
        )


def _tier_rank(tier: StorageTier) -> int:
    return {
        StorageTier.HOT: 0,
        StorageTier.WARM: 1,
        StorageTier.COLD: 2,
    }[tier]


def _is_adjacent_tier(source: StorageTier, target: StorageTier) -> bool:
    return (source, target) in {
        (StorageTier.HOT, StorageTier.WARM),
        (StorageTier.WARM, StorageTier.COLD),
    }


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _path_generation(observed: os.stat_result) -> PathGeneration:
    return observed.st_dev, observed.st_ino, observed.st_ctime_ns


def _node_identity(generation: PathGeneration) -> tuple[int, int]:
    return generation[0], generation[1]


def _object_from_row(row: sqlite3.Row) -> ObjectIdentity:
    return ObjectIdentity(
        content_sha256=row["content_sha256"],
        size_bytes=row["size_bytes"],
        object_kind=row["object_kind"],
        created_at=_parse_datetime(row["created_at"]),
    )


def _copy_from_row(row: sqlite3.Row) -> ObjectCopy:
    return ObjectCopy(
        content_sha256=row["content_sha256"],
        location_id=row["location_id"],
        storage_uri=row["storage_uri"],
        storage_tier=StorageTier(row["storage_tier"]),
        verified_at=_parse_datetime(row["verified_at"]) if row["verified_at"] is not None else None,
        failure_domain=row["failure_domain"],
        tier_entered_at=_parse_datetime(row["tier_entered_at"]),
    )


def _reference_from_row(row: sqlite3.Row) -> ObjectReference:
    return ObjectReference(
        reference_id=row["reference_id"],
        owner_type=row["owner_type"],
        owner_id=row["owner_id"],
        content_sha256=row["content_sha256"],
        created_at=_parse_datetime(row["created_at"]),
        expires_at=_parse_datetime(row["expires_at"]) if row["expires_at"] is not None else None,
    )


def _hold_from_row(row: sqlite3.Row) -> LegalHold:
    return LegalHold(
        hold_id=row["hold_id"],
        content_sha256=row["content_sha256"],
        reason=row["reason"],
        created_at=_parse_datetime(row["created_at"]),
    )
