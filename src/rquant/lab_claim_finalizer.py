"""Authority-owned V2 source-claim publication finalization.

This module deliberately owns only the source-stage reader, C/D ledger CAS,
current-claim plan issuer, claim spool publisher/verifier, clock, and metrics.
It has no scheduler, provider, queue, runtime, adapter, or worker authority.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from rquant.adapter_manifest import Ed25519ContractSigner, VerifyOnlyEd25519Keyring
from rquant.lab_claim_finalizer_trust import (
    LabClaimFinalizerTrustCertificate,
    LabClaimFinalizerTrustVerifier,
)
from rquant.lab_claim_publication import (
    ClaimPublicationStatus,
    LabClaimPublicationFinalizerAuthority,
    LabClaimPublicationFinalizerRootKey,
    LabClaimPublicationIdentity,
    LabClaimPublicationMutation,
    LabClaimPublicationRecord,
    LabClaimSpoolPublishReceiptV2,
    LabClaimSpoolReceiptVerifier,
    PublishReceipt,
    require_v2_spool_receipt_provenance,
)
from rquant.lab_jobs import ClaimPublicationConflictError, InvalidClaimPublicationTransitionError
from rquant.lab_shard_protocol import LabClaimSpool, LabShardClaimV2
from rquant.lab_source_stage import LabSourceStageBinding, LabSourceStageRecord, LabSourceStageState
from rquant.source_broker_v2_job_protocol import SourceBrokerV2JobOutcomeStatus
from rquant.source_operation_contracts import (
    CurrentClaimAuthorityProtocol,
    SourceOperationContractError,
    sign_source_use_plan_v2,
)
from rquant.strict_json import canonical_model_json_bytes, strict_model_validate_canonical_json


class LabClaimFinalizerError(RuntimeError):
    """A redacted, fail-closed finalizer error."""


class LabClaimFinalizerObservationDegradedError(LabClaimFinalizerError):
    """Both primary and durable fallback observation persistence failed."""


class LabClaimPublicationFinalizerLedger(Protocol):
    def get_claim_publication(self, attempt_id: UUID) -> LabClaimPublicationRecord | None: ...

    def finalizer_mark_claim_publication_ready(
        self,
        identity: LabClaimPublicationIdentity,
        signed_plan: object,
        final_bound_claim: LabShardClaimV2,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> LabClaimPublicationMutation: ...

    def finalizer_publish_claim_publication(
        self,
        identity: LabClaimPublicationIdentity,
        spool_receipt: PublishReceipt,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        spool_receipt_verifier: LabClaimSpoolReceiptVerifier,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> LabClaimPublicationMutation: ...

    def validate_ready_claim_for_publication(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        now: datetime,
    ) -> LabShardClaimV2: ...

    def validate_finalizer_ready_attestation(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        trust_verifier: LabClaimFinalizerTrustVerifier,
        now: datetime,
    ) -> None: ...

    def finalizer_record_claim_publication_observation(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        event_type: Literal["ready", "published", "replayed", "blocked"],
        reason_code: str,
        now: datetime,
    ) -> None: ...

    def finalizer_record_claim_publication_observation_degradation(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        event_type: Literal["ready", "published", "replayed", "blocked"],
        reason_code: str,
        error_class: str,
        next_retry_at: datetime,
        now: datetime,
    ) -> None: ...


class LabClaimPublicationFinalizerAuthorityLeaseStore(Protocol):
    def _acquire_claim_publication_finalizer_authority(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        root_key: LabClaimPublicationFinalizerRootKey,
        trust_certificate: LabClaimFinalizerTrustCertificate,
        trust_verifier: LabClaimFinalizerTrustVerifier,
        runtime_signer: Ed25519ContractSigner,
        now: datetime,
    ) -> LabClaimPublicationFinalizerAuthority: ...

    def _renew_claim_publication_finalizer_authority(
        self,
        authority: LabClaimPublicationFinalizerAuthority,
        *,
        lease_seconds: int,
        now: datetime,
    ) -> LabClaimPublicationFinalizerAuthority: ...

    def _release_claim_publication_finalizer_authority(
        self, authority: LabClaimPublicationFinalizerAuthority, *, now: datetime
    ) -> None: ...


class LabClaimPublicationFinalizerAuthorityIssuer:
    """The only finalizer-facing lifecycle for the durable C/D capability."""

    def __init__(
        self,
        *,
        store: LabClaimPublicationFinalizerAuthorityLeaseStore,
        root_key: LabClaimPublicationFinalizerRootKey,
        trust_certificate: LabClaimFinalizerTrustCertificate,
        trust_verifier: LabClaimFinalizerTrustVerifier,
        runtime_signer: Ed25519ContractSigner,
    ) -> None:
        self._store = store
        if type(root_key) is not LabClaimPublicationFinalizerRootKey:
            raise TypeError("finalizer issuer requires an exact root key")
        if type(trust_verifier) is not LabClaimFinalizerTrustVerifier:
            raise TypeError("finalizer issuer requires an exact trust verifier")
        if type(runtime_signer) is not Ed25519ContractSigner:
            raise TypeError("finalizer issuer requires an exact runtime signer")
        self._root_key = root_key
        self._trust_certificate = trust_certificate
        self._trust_verifier = trust_verifier
        self._runtime_signer = runtime_signer

    def acquire(
        self, *, owner_id: str, lease_seconds: int, now: datetime
    ) -> LabClaimPublicationFinalizerAuthority:
        return self._store._acquire_claim_publication_finalizer_authority(
            owner_id=owner_id,
            lease_seconds=lease_seconds,
            root_key=self._root_key,
            trust_certificate=self._trust_certificate,
            trust_verifier=self._trust_verifier,
            runtime_signer=self._runtime_signer,
            now=now,
        )

    def renew(
        self,
        authority: LabClaimPublicationFinalizerAuthority,
        *,
        lease_seconds: int,
        now: datetime,
    ) -> LabClaimPublicationFinalizerAuthority:
        return self._store._renew_claim_publication_finalizer_authority(
            authority, lease_seconds=lease_seconds, now=now
        )

    def release(self, authority: LabClaimPublicationFinalizerAuthority, *, now: datetime) -> None:
        self._store._release_claim_publication_finalizer_authority(authority, now=now)


class LabClaimFinalizerStageReader(Protocol):
    def get(self, binding: LabSourceStageBinding) -> LabSourceStageRecord | None: ...


class LabClaimPublicationWorkerLedger(Protocol):
    def get_claim_publication(self, attempt_id: UUID) -> LabClaimPublicationRecord | None: ...

    def validate_published_claim_for_worker(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        now: datetime,
    ) -> LabShardClaimV2: ...

    def validate_finalizer_published_attestation(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        trust_verifier: LabClaimFinalizerTrustVerifier,
        now: datetime,
    ) -> None: ...


class LabClaimFinalizerMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ready_transitions: int = Field(default=0, ge=0)
    publish_transitions: int = Field(default=0, ge=0)
    replays: int = Field(default=0, ge=0)
    blocked: int = Field(default=0, ge=0)


class LabClaimFinalizerResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    attempt_id: UUID
    status: Literal["published", "replayed", "blocked", "not_ready"]
    reason: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    record: LabClaimPublicationRecord | None = None


def _system_clock() -> datetime:
    return datetime.now(UTC)


# Closed stage vocabulary for blocked results. Every value is a compile-time
# literal: no path, identity, byte, key, or exception message ever reaches it.
_FinalizerStage = Literal[
    "record",
    "issue_ready",
    "ready_attestation",
    "ready_claim",
    "spool_publish",
    "publish_cas",
    "recovery",
    "observe",
]


class _FinalizerStageCursor:
    """Per-call stage marker. One instance per finalize() frame, never shared."""

    __slots__ = ("stage",)

    def __init__(self, stage: _FinalizerStage = "record") -> None:
        self.stage: _FinalizerStage = stage


# Closed (exception class, error code) -> category allowlist. Without it the
# class-name rule below redacts every ClaimPublicationConflictError message into
# "authority_conflict", which made a ledger signature failure indistinguishable
# from a fencing/authority failure.
_REDACTED_CATEGORIES: dict[tuple[type[BaseException], str], str] = {
    (ClaimPublicationConflictError, "publication_cas_conflict"): "cas_conflict",
    (
        ClaimPublicationConflictError,
        "finalizer_publication_signature_invalid",
    ): "signature_invalid",
    (
        ClaimPublicationConflictError,
        "finalizer_publication_signature_missing",
    ): "signature_missing",
    (ClaimPublicationConflictError, "finalizer_external_trust_invalid"): "trust_invalid",
    (ClaimPublicationConflictError, "finalizer_authority_conflict"): "authority_conflict",
    (ClaimPublicationConflictError, "ready_content_conflict"): "ready_content_conflict",
    (ClaimPublicationConflictError, "published_receipt_conflict"): "receipt_conflict",
    (ClaimPublicationConflictError, "ready_binding_conflict"): "ready_binding_conflict",
    (ClaimPublicationConflictError, "attempt_identity_conflict"): "identity_conflict",
    (InvalidClaimPublicationTransitionError, "transition_not_allowed"): "finalization_blocked",
    (InvalidClaimPublicationTransitionError, "terminal_status_immutable"): "finalization_blocked",
}


def _redacted_category(exc: Exception) -> str:
    mapped = _REDACTED_CATEGORIES.get((type(exc), str(exc)))
    if mapped is not None:
        return mapped
    if isinstance(exc, SourceOperationContractError):
        return "current_claim_evidence_invalid"
    if isinstance(exc, (ValueError, TypeError, UnicodeDecodeError)):
        return "canonical_evidence_invalid"
    name = type(exc).__name__.lower()
    if "conflict" in name or "fenced" in name or "stale" in name:
        return "authority_conflict"
    return "finalization_blocked"


def _redacted_reason(exc: Exception, *, stage: _FinalizerStage) -> str:
    """Stage-tagged redacted reason: closed stage enum x closed category set."""

    return f"{stage}_{_redacted_category(exc)}"


class LabClaimPublicationWorkerVerifier:
    """Read-only V2 D gate used immediately before a worker consumes a claim."""

    def __init__(
        self,
        *,
        ledger: LabClaimPublicationWorkerLedger,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        spool_receipt_verifier: LabClaimSpoolReceiptVerifier,
        trust_verifier: LabClaimFinalizerTrustVerifier | None = None,
    ) -> None:
        self._ledger = ledger
        self._current_claim_authority = current_claim_authority
        self._keyring = keyring
        self._audience = audience
        self._spool_receipt_verifier = spool_receipt_verifier
        self._trust_verifier = trust_verifier

    def require_published_claim(self, claim: LabShardClaimV2, *, now: datetime) -> None:
        identity = LabClaimPublicationIdentity.from_claim(claim)
        record = self._ledger.get_claim_publication(identity.attempt_id)
        if record is None or record.identity != identity:
            raise LabClaimFinalizerError("publication_missing")
        if record.status is not ClaimPublicationStatus.PUBLISHED:
            raise LabClaimFinalizerError("publication_not_published")
        if self._trust_verifier is None:
            raise LabClaimFinalizerError("publication_trust_verifier_missing")
        try:
            self._ledger.validate_finalizer_published_attestation(
                identity,
                trust_verifier=self._trust_verifier,
                now=now,
            )
        except Exception as exc:
            raise LabClaimFinalizerError("publication_signature_invalid") from exc
        final_claim = self._ledger.validate_published_claim_for_worker(
            identity,
            current_claim_authority=self._current_claim_authority,
            keyring=self._keyring,
            audience=self._audience,
            now=now,
        )
        if canonical_model_json_bytes(final_claim) != canonical_model_json_bytes(claim):
            raise LabClaimFinalizerError("published_claim_conflict")
        try:
            receipt = PublishReceipt(
                spool_receipt_bytes=record.spool_receipt_bytes or b"",
                spool_receipt_hash=record.spool_receipt_hash or "",
            )
            require_v2_spool_receipt_provenance(
                receipt,
                final_claim=claim,
                verifier=self._spool_receipt_verifier,
            )
        except Exception as exc:
            raise LabClaimFinalizerError("published_receipt_invalid") from exc


class LabClaimFinalizer:
    """Finalize one V2 source claim with deterministic C/D recovery semantics."""

    def __init__(
        self,
        *,
        ledger: LabClaimPublicationFinalizerLedger,
        stage_reader: LabClaimFinalizerStageReader,
        authority: LabClaimPublicationFinalizerAuthority,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        spool: LabClaimSpool,
        spool_receipt_verifier: LabClaimSpoolReceiptVerifier,
        clock: Callable[[], datetime] = _system_clock,
    ) -> None:
        if not audience:
            raise ValueError("finalizer audience must not be empty")
        self._ledger = ledger
        self._stage_reader = stage_reader
        self._authority = authority
        self._current_claim_authority = current_claim_authority
        self._keyring = keyring
        self._audience = audience
        self._spool = spool
        self._spool_receipt_verifier = spool_receipt_verifier
        self._clock = clock
        self._metrics = LabClaimFinalizerMetrics()

    @property
    def metrics(self) -> LabClaimFinalizerMetrics:
        return self._metrics

    @staticmethod
    def _after_ready_before_issue(_record: LabClaimPublicationRecord) -> None:
        """Test-only crash boundary before deterministic plan issue."""

    @staticmethod
    def _after_issue_before_ready(_record: LabClaimPublicationRecord) -> None:
        """Test-only crash boundary after issue_plan_once and before C."""

    @staticmethod
    def _before_ready_attestation(_record: LabClaimPublicationRecord) -> None:
        """Test-only crash boundary after C and before the D pre-checks."""

    @staticmethod
    def _after_ready_before_spool(_record: LabClaimPublicationRecord) -> None:
        """Test-only crash boundary after C and before spool publication."""

    @staticmethod
    def _after_spool_before_sidecar(_record: LabClaimPublicationRecord) -> None:
        """Test-only crash boundary after immutable claim bytes and before sidecar."""

    @staticmethod
    def _after_sidecar_before_published(_record: LabClaimPublicationRecord) -> None:
        """Test-only crash boundary after typed sidecar and before D."""

    @staticmethod
    def _after_published(_record: LabClaimPublicationRecord) -> None:
        """Test-only crash boundary after D."""

    def _observe(
        self,
        identity: LabClaimPublicationIdentity,
        *,
        event_type: Literal["ready", "published", "replayed", "blocked"],
        reason_code: str,
        now: datetime,
    ) -> None:
        try:
            self._ledger.finalizer_record_claim_publication_observation(
                identity,
                authority=self._authority,
                event_type=event_type,
                reason_code=reason_code,
                now=now,
            )
        except Exception as primary_exc:
            primary_class = type(primary_exc).__name__
            if not primary_class.isascii() or not primary_class.isidentifier():
                primary_class = "Exception"
            try:
                self._ledger.finalizer_record_claim_publication_observation_degradation(
                    identity,
                    authority=self._authority,
                    event_type=event_type,
                    reason_code=reason_code,
                    error_class=primary_class,
                    next_retry_at=now + timedelta(seconds=5),
                    now=now,
                )
            except Exception as fallback_exc:
                fallback_class = type(fallback_exc).__name__
                if not fallback_class.isascii() or not fallback_class.isidentifier():
                    fallback_class = "Exception"
                logger.error(
                    "lab claim finalizer observation persistence degraded: "
                    "event_type={} primary_error_class={} fallback_error_class={}",
                    event_type,
                    primary_class,
                    fallback_class,
                )
                raise LabClaimFinalizerObservationDegradedError(
                    "observation_persistence_degraded"
                ) from fallback_exc

    def _record(self, identity: LabClaimPublicationIdentity) -> LabClaimPublicationRecord:
        record = self._ledger.get_claim_publication(identity.attempt_id)
        if record is None or record.identity != identity:
            raise LabClaimFinalizerError("publication_missing")
        return record

    @staticmethod
    def _preimage(record: LabClaimPublicationRecord) -> LabShardClaimV2:
        try:
            claim = strict_model_validate_canonical_json(
                LabShardClaimV2, record.claim_preimage_bytes
            )
        except LabClaimFinalizerObservationDegradedError:
            raise
        except Exception as exc:
            raise LabClaimFinalizerError("claim_preimage_invalid") from exc
        if LabClaimPublicationIdentity.from_claim(claim) != record.identity:
            raise LabClaimFinalizerError("claim_preimage_conflict")
        return claim

    def _require_ready_stage(self, record: LabClaimPublicationRecord) -> None:
        try:
            binding = strict_model_validate_canonical_json(
                LabSourceStageBinding, record.source_stage_binding_bytes or b""
            )
        except Exception as exc:
            raise LabClaimFinalizerError("source_stage_binding_invalid") from exc
        stage = self._stage_reader.get(binding)
        if (
            stage is None
            or stage.state is not LabSourceStageState.READY
            or stage.binding != binding
            or stage.intent_bytes != record.source_intent_bytes
            or stage.intent_hash != record.source_intent_hash
            or stage.operation_id != record.source_operation_id
            or stage.operation_hash != record.source_operation_hash
            or stage.outcome is None
            or stage.outcome.status is not SourceBrokerV2JobOutcomeStatus.SUCCESS
            or stage.outcome.outcome_hash != stage.outcome_hash
            or stage.outcome.evidence_chain_hash != stage.evidence_chain_hash
            or stage.record_hash == "0" * 64
        ):
            raise LabClaimFinalizerError("source_stage_evidence_invalid")

    def _issue_ready(
        self, record: LabClaimPublicationRecord, *, now: datetime
    ) -> LabClaimPublicationMutation:
        self._require_ready_stage(record)
        self._after_ready_before_issue(record)
        preimage = self._preimage(record)
        signed_plan = sign_source_use_plan_v2(
            claim=preimage,
            current_claim_authority=self._current_claim_authority,
            keyring=self._keyring,
            operation_id=record.source_operation_id or "",
            audience=self._audience,
            now=now,
            expires_at=preimage.lease_expires_at - timedelta(seconds=1),
            nonce="claim-publication-plan",
        )
        final_claim = preimage.bind_source_use_plan(signed_plan)
        self._after_issue_before_ready(record)
        return self._ledger.finalizer_mark_claim_publication_ready(
            record.identity,
            signed_plan,
            final_claim,
            current_claim_authority=self._current_claim_authority,
            keyring=self._keyring,
            audience=self._audience,
            authority=self._authority,
            now=now,
        )

    def _publish_ready(
        self,
        record: LabClaimPublicationRecord,
        *,
        now: datetime,
        stage: _FinalizerStageCursor,
    ) -> LabClaimPublicationMutation:
        stage.stage = "ready_attestation"
        self._before_ready_attestation(record)
        if self._authority._trust_verifier is None:  # noqa: SLF001
            raise LabClaimFinalizerError("finalizer_trust_verifier_missing")
        self._ledger.validate_finalizer_ready_attestation(
            record.identity,
            trust_verifier=self._authority._trust_verifier,  # noqa: SLF001
            now=now,
        )
        stage.stage = "ready_claim"
        final_claim = self._ledger.validate_ready_claim_for_publication(
            record.identity,
            current_claim_authority=self._current_claim_authority,
            keyring=self._keyring,
            audience=self._audience,
            now=now,
        )
        stage.stage = "spool_publish"
        self._after_ready_before_spool(record)
        entry = self._spool.publish(final_claim)
        self._after_spool_before_sidecar(record)
        typed_receipt = LabClaimSpoolPublishReceiptV2.from_published_entry(
            spool=self._spool,
            entry=entry,
            final_claim=final_claim,
            committed_at=now,
        )
        receipt = typed_receipt.to_publish_receipt()
        stage.stage = "publish_cas"
        self._after_sidecar_before_published(record)
        return self._ledger.finalizer_publish_claim_publication(
            record.identity,
            receipt,
            current_claim_authority=self._current_claim_authority,
            keyring=self._keyring,
            audience=self._audience,
            spool_receipt_verifier=self._spool_receipt_verifier,
            authority=self._authority,
            now=now,
        )

    @staticmethod
    def _is_recoverable_concurrency_error(exc: Exception) -> bool:
        if isinstance(exc, ClaimPublicationConflictError):
            return str(exc) == "publication_cas_conflict"
        if isinstance(exc, InvalidClaimPublicationTransitionError):
            return str(exc) in {"transition_not_allowed", "terminal_status_immutable"}
        if isinstance(exc, sqlite3.OperationalError):
            message = str(exc).lower()
            return "busy" in message or "locked" in message
        return False

    def _published_replay(
        self,
        identity: LabClaimPublicationIdentity,
        record: LabClaimPublicationRecord,
        *,
        now: datetime,
        observation_reason: str,
    ) -> LabClaimFinalizerResult:
        if record.status is not ClaimPublicationStatus.PUBLISHED:
            raise LabClaimFinalizerError("concurrent_terminal_invalid")
        LabClaimPublicationWorkerVerifier(
            ledger=self._ledger,
            current_claim_authority=self._current_claim_authority,
            keyring=self._keyring,
            audience=self._audience,
            spool_receipt_verifier=self._spool_receipt_verifier,
            trust_verifier=self._authority._trust_verifier,  # noqa: SLF001
        ).require_published_claim(
            strict_model_validate_canonical_json(LabShardClaimV2, record.final_claim_bytes or b""),
            now=now,
        )
        self._metrics = self._metrics.model_copy(update={"replays": self._metrics.replays + 1})
        self._observe(identity, event_type="replayed", reason_code=observation_reason, now=now)
        return LabClaimFinalizerResult(
            attempt_id=identity.attempt_id,
            status="replayed",
            reason="published_replay",
            record=record,
        )

    def _concurrent_terminal_recovery(
        self,
        identity: LabClaimPublicationIdentity,
    ) -> LabClaimFinalizerResult | None:
        """Bounded recovery after a C/D CAS or SQLite writer race.

        A PUBLISHED reread is accepted only through the complete worker D gate.
        A READY reread may run the existing D saga once; a second race gets one
        final reread. No other state is a recoverable concurrency outcome.

        Failures raised from here are reported under the "recovery" stage, so the
        local cursor below is deliberately not the caller's cursor.
        """

        stage = _FinalizerStageCursor("recovery")
        for attempt in range(2):
            now = self._clock()
            record = self._record(identity)
            if record.status is ClaimPublicationStatus.PUBLISHED:
                return self._published_replay(
                    identity,
                    record,
                    now=now,
                    observation_reason="concurrent_published_replay",
                )
            if record.status is not ClaimPublicationStatus.READY_TO_PUBLISH:
                return None
            try:
                mutation = self._publish_ready(record, now=now, stage=stage)
            except (
                ClaimPublicationConflictError,
                InvalidClaimPublicationTransitionError,
                sqlite3.OperationalError,
            ) as retry_exc:
                if not self._is_recoverable_concurrency_error(retry_exc) or attempt == 1:
                    raise
                continue
            self._metrics = self._metrics.model_copy(
                update={
                    "publish_transitions": self._metrics.publish_transitions
                    + int(not mutation.replayed),
                    "replays": self._metrics.replays + int(mutation.replayed),
                }
            )
            self._observe(
                identity,
                event_type="replayed" if mutation.replayed else "published",
                reason_code=(
                    "concurrent_published_replay" if mutation.replayed else "concurrent_published"
                ),
                now=now,
            )
            return LabClaimFinalizerResult(
                attempt_id=identity.attempt_id,
                status="replayed" if mutation.replayed else "published",
                reason="published_replay" if mutation.replayed else "published",
                record=mutation.record,
            )
        return None

    def finalize(self, identity: LabClaimPublicationIdentity) -> LabClaimFinalizerResult:
        """Drive one attempt to D. A failed check leaves the ledger and spool unchanged."""

        cursor = _FinalizerStageCursor()
        try:
            record = self._record(identity)
            now = self._clock()
            if record.status is ClaimPublicationStatus.SOURCE_QUEUED:
                cursor.stage = "issue_ready"
                mutation = self._issue_ready(record, now=now)
                self._metrics = self._metrics.model_copy(
                    update={
                        "ready_transitions": self._metrics.ready_transitions
                        + int(not mutation.replayed),
                        "replays": self._metrics.replays + int(mutation.replayed),
                    }
                )
                record = mutation.record
                cursor.stage = "observe"
                self._observe(
                    identity,
                    event_type="replayed" if mutation.replayed else "ready",
                    reason_code="ready_replay" if mutation.replayed else "ready",
                    now=now,
                )
            if record.status is ClaimPublicationStatus.READY_TO_PUBLISH:
                mutation = self._publish_ready(record, now=now, stage=cursor)
                self._metrics = self._metrics.model_copy(
                    update={
                        "publish_transitions": self._metrics.publish_transitions
                        + int(not mutation.replayed),
                        "replays": self._metrics.replays + int(mutation.replayed),
                    }
                )
                cursor.stage = "observe"
                self._observe(
                    identity,
                    event_type="replayed" if mutation.replayed else "published",
                    reason_code="published_replay" if mutation.replayed else "published",
                    now=now,
                )
                self._after_published(mutation.record)
                return LabClaimFinalizerResult(
                    attempt_id=identity.attempt_id,
                    status="replayed" if mutation.replayed else "published",
                    reason="published_replay" if mutation.replayed else "published",
                    record=mutation.record,
                )
            if record.status is ClaimPublicationStatus.PUBLISHED:
                cursor.stage = "publish_cas"
                return self._published_replay(
                    identity,
                    record,
                    now=now,
                    observation_reason="published_replay",
                )
            return LabClaimFinalizerResult(
                attempt_id=identity.attempt_id,
                status="not_ready",
                reason="publication_not_ready",
                record=record,
            )
        except Exception as exc:
            if self._is_recoverable_concurrency_error(exc):
                primary_stage = cursor.stage
                cursor.stage = "recovery"
                try:
                    recovered = self._concurrent_terminal_recovery(identity)
                except Exception as recovery_exc:
                    exc = recovery_exc
                else:
                    if recovered is not None:
                        return recovered
                    cursor.stage = primary_stage
            self._metrics = self._metrics.model_copy(update={"blocked": self._metrics.blocked + 1})
            now = self._clock()
            reason = _redacted_reason(exc, stage=cursor.stage)
            self._observe(
                identity,
                event_type="blocked",
                reason_code=reason,
                now=now,
            )
            return LabClaimFinalizerResult(
                attempt_id=identity.attempt_id,
                status="blocked",
                reason=reason,
            )
