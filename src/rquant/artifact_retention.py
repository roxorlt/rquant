"""Reference-governed retention planning for content-addressed artifacts.

This module owns metadata only. External storage deletion is deliberately left to an
identity-bound executor; the ledger is updated only after that executor reports the
exact object and location it removed.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Self

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


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


class LegalHold(RuntimeContractModel):
    hold_id: str = Field(min_length=1)
    content_sha256: Sha256
    reason: str = Field(min_length=1)
    created_at: AwareUtcDatetime


class RetentionPolicy(RuntimeContractModel):
    hot_min_age: timedelta
    warm_min_age: timedelta
    cold_min_age: timedelta
    minimum_verified_copies: int = Field(ge=1)
    verification_max_age: timedelta
    plan_ttl: timedelta
    claim_ttl: timedelta

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
        return self

    def age_for(self, tier: StorageTier) -> timedelta:
        return {
            StorageTier.HOT: self.hot_min_age,
            StorageTier.WARM: self.warm_min_age,
            StorageTier.COLD: self.cold_min_age,
        }[tier]

    def identity_payload(self) -> dict[str, int]:
        return {
            "hot_min_age_us": _timedelta_microseconds(self.hot_min_age),
            "warm_min_age_us": _timedelta_microseconds(self.warm_min_age),
            "cold_min_age_us": _timedelta_microseconds(self.cold_min_age),
            "minimum_verified_copies": self.minimum_verified_copies,
            "verification_max_age_us": _timedelta_microseconds(self.verification_max_age),
            "plan_ttl_us": _timedelta_microseconds(self.plan_ttl),
            "claim_ttl_us": _timedelta_microseconds(self.claim_ttl),
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


class GcPlan(RuntimeContractModel):
    plan_id: Sha256 | None = None
    planned_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime | None = None
    ledger_revision: int = Field(ge=0)
    policy: RetentionPolicy
    candidates: tuple[GcCandidate, ...]

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
        expected = _plan_id(
            planned_at=self.planned_at,
            expires_at=self.expires_at,
            ledger_revision=self.ledger_revision,
            policy=self.policy,
            candidates=self.candidates,
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
) -> str:
    return canonical_sha256(
        {
            "planned_at": planned_at,
            "expires_at": expires_at,
            "ledger_revision": ledger_revision,
            "policy": policy.identity_payload(),
            "candidate_ids": tuple(candidate.candidate_id for candidate in candidates),
        }
    )


_SCHEMA = """
CREATE TABLE IF NOT EXISTS artifact_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    governance_revision INTEGER NOT NULL
);
INSERT OR IGNORE INTO artifact_metadata(singleton, governance_revision) VALUES (1, 0);

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


class ArtifactReferenceStore:
    """Short SQLite WAL transactions for one metadata writer at a time."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self._clock = clock or (lambda: datetime.now(UTC))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(_SCHEMA)

    def close(self) -> None:
        """Compatibility hook; transactions use short-lived connections."""

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    @contextmanager
    def _writer(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @contextmanager
    def _reader(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

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

    def release_reference(self, reference_id: str, *, released_at: AwareUtcDatetime) -> None:
        released_at = normalize_aware_utc(released_at)
        with self._writer() as connection:
            row = connection.execute(
                "SELECT * FROM artifact_reference WHERE reference_id = ?",
                (reference_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown reference: {reference_id}")
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

    def plan_gc(self, *, now: AwareUtcDatetime, policy: RetentionPolicy) -> GcPlan:
        now = normalize_aware_utc(now)
        trusted_now = normalize_aware_utc(self._clock())
        if now > trusted_now + timedelta(seconds=1):
            raise ValueError("GC planning time cannot exceed the trusted clock")
        with self._reader() as connection:
            revision = self._revision(connection)
            object_rows = connection.execute(
                "SELECT * FROM artifact_object ORDER BY content_sha256"
            ).fetchall()
            candidates: list[GcCandidate] = []
            for object_row in object_rows:
                identity = _object_from_row(object_row)
                if self._has_active_reference(connection, identity.content_sha256, now):
                    continue
                if self._has_active_hold(connection, identity.content_sha256):
                    continue
                copy_rows = connection.execute(
                    """
                    SELECT * FROM artifact_copy
                    WHERE content_sha256 = ? AND deleted_at IS NULL
                    ORDER BY storage_tier, location_id
                    """,
                    (identity.content_sha256,),
                ).fetchall()
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
                    >= policy.age_for(StorageTier(row["storage_tier"]))
                ]
                eligible.sort(
                    key=lambda row: (
                        _tier_rank(StorageTier(row["storage_tier"])),
                        row["location_id"],
                    )
                )
                for row in eligible[:deletion_capacity]:
                    candidates.append(
                        GcCandidate(
                            object_identity=identity,
                            object_copy=_copy_from_row(row),
                        )
                    )
        ordered = tuple(sorted(candidates, key=lambda item: item.candidate_id or ""))
        return GcPlan(
            planned_at=now,
            ledger_revision=revision,
            policy=policy,
            candidates=ordered,
        )

    def claim_deletion(
        self,
        *,
        plan: GcPlan,
        candidate: GcCandidate,
        owner_id: str,
        now: AwareUtcDatetime,
    ) -> GcClaim:
        now = normalize_aware_utc(now)
        self._validate_plan_and_candidate(plan, candidate)
        if plan.expires_at is None or now > plan.expires_at:
            raise ValueError("GC plan has expired")
        claim = GcClaim(
            plan=plan,
            candidate=candidate,
            owner_id=owner_id,
            claimed_at=now,
            expires_at=now + plan.policy.claim_ttl,
        )
        assert claim.claim_id is not None
        with self._writer() as connection:
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
    ) -> None:
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
            if claim_row["status"] == "completed":
                raise ValueError("candidate is already marked deleted")
            if claim_row["status"] != "claimed":
                raise ValueError("GC claim is already resolved")
            if now > claim.expires_at:
                raise ValueError("GC claim expired before deletion confirmation")
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
            connection.execute(
                """
                UPDATE artifact_gc_claim
                SET status = 'completed', resolved_at = ?, resolution_reason = ?
                WHERE claim_id = ? AND status = 'claimed'
                """,
                (now.isoformat(), "external identity confirmed", claim.claim_id),
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
                },
            )
            self._bump_revision(connection)

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
        tier_age = now - candidate.object_copy.tier_entered_at
        if tier_age < policy.age_for(candidate.object_copy.storage_tier):
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


def _tier_rank(tier: StorageTier) -> int:
    return {
        StorageTier.HOT: 0,
        StorageTier.WARM: 1,
        StorageTier.COLD: 2,
    }[tier]


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


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
