"""Narrow production loop for authority-owned V2 claim publication finalization."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from threading import Event
from typing import Protocol

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
from rquant.lab_claim_finalizer import (
    LabClaimFinalizer,
    LabClaimFinalizerStageReader,
    LabClaimPublicationFinalizerAuthorityIssuer,
    LabClaimPublicationWorkerVerifier,
)
from rquant.lab_claim_publication import (
    ClaimPublicationStatus,
    LabClaimPublicationFinalizerAuthority,
    LabClaimPublicationRecord,
    LabClaimPublicationRolloutEvidenceOutboxItem,
    LabClaimSpoolReceiptVerifier,
)
from rquant.lab_shard_protocol import LabClaimSpool, LabShardClaimV2
from rquant.lab_source_stage import LabSourceStageBinding, LabSourceStageState
from rquant.source_operation_contracts import CurrentClaimAuthorityProtocol
from rquant.strict_json import strict_model_validate_canonical_json


class LabClaimFinalizerDaemonLedger(Protocol):
    def list_claim_publication_finalizer_candidates(
        self,
        *,
        limit: int,
    ) -> tuple[LabClaimPublicationRecord, ...]: ...

    def finalizer_drain_claim_publication_observation_degradations(
        self,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
        limit: int,
    ) -> int: ...

    def list_due_claim_publication_rollout_evidence(
        self,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
        limit: int,
    ) -> tuple[LabClaimPublicationRolloutEvidenceOutboxItem, ...]: ...

    def finalizer_ack_claim_publication_rollout_evidence(
        self,
        item: LabClaimPublicationRolloutEvidenceOutboxItem,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> None: ...

    def finalizer_defer_claim_publication_rollout_evidence(
        self,
        item: LabClaimPublicationRolloutEvidenceOutboxItem,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        error_class: str,
        next_retry_at: datetime,
        now: datetime,
    ) -> None: ...


class LabClaimFinalizerPublishedEvidenceRecorder(Protocol):
    def __call__(
        self,
        *,
        attempt_id: str,
        evidence_hash: str,
        publication_identity: str,
    ) -> None: ...


class LabClaimFinalizerDaemonResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidates: int = Field(ge=0)
    published: int = Field(ge=0)
    replayed: int = Field(ge=0)
    blocked: int = Field(ge=0)
    not_ready: int = Field(ge=0)
    observations_recovered: int = Field(ge=0)
    rollout_evidence_recovered: int = Field(ge=0)


def _system_clock() -> datetime:
    return datetime.now(UTC)


class LabClaimFinalizerDaemon:
    """Own one fenced finalizer lease and process one bounded candidate batch."""

    __slots__ = (
        "_audience",
        "_authority",
        "_authority_issuer",
        "_clock",
        "_closed",
        "_current_claim_authority",
        "_failure_backoff_max_seconds",
        "_failure_backoff_seconds",
        "_keyring",
        "_lease_seconds",
        "_ledger",
        "_max_publications_per_tick",
        "_owner_id",
        "_poll_interval_seconds",
        "_published_evidence_recorder",
        "_spool",
        "_spool_receipt_verifier",
        "_stage_reader",
        "_stop",
    )

    def __init__(
        self,
        *,
        ledger: LabClaimFinalizerDaemonLedger,
        stage_reader: LabClaimFinalizerStageReader,
        authority_issuer: LabClaimPublicationFinalizerAuthorityIssuer,
        current_claim_authority: CurrentClaimAuthorityProtocol,
        keyring: VerifyOnlyEd25519Keyring,
        audience: str,
        spool: LabClaimSpool,
        spool_receipt_verifier: LabClaimSpoolReceiptVerifier,
        owner_id: str,
        lease_seconds: int,
        max_publications_per_tick: int,
        poll_interval_ms: int,
        failure_backoff_seconds: int,
        failure_backoff_max_seconds: int,
        published_evidence_recorder: LabClaimFinalizerPublishedEvidenceRecorder | None = None,
        clock: Callable[[], datetime] = _system_clock,
    ) -> None:
        if not owner_id.strip() or len(owner_id) > 200:
            raise ValueError("claim finalizer owner id is invalid")
        if not 1 <= lease_seconds <= 3_600:
            raise ValueError("claim finalizer lease seconds is invalid")
        if not 1 <= max_publications_per_tick <= 100:
            raise ValueError("claim finalizer batch is invalid")
        if not 1 <= poll_interval_ms <= 60_000:
            raise ValueError("claim finalizer poll interval is invalid")
        if not 1 <= failure_backoff_seconds <= failure_backoff_max_seconds <= 3_600:
            raise ValueError("claim finalizer failure backoff is invalid")
        self._ledger = ledger
        self._stage_reader = stage_reader
        self._authority_issuer = authority_issuer
        self._current_claim_authority = current_claim_authority
        self._keyring = keyring
        self._audience = audience
        self._spool = spool
        self._spool_receipt_verifier = spool_receipt_verifier
        self._owner_id = owner_id.strip()
        self._lease_seconds = lease_seconds
        self._max_publications_per_tick = max_publications_per_tick
        self._poll_interval_seconds = poll_interval_ms / 1_000
        self._failure_backoff_seconds = failure_backoff_seconds
        self._failure_backoff_max_seconds = failure_backoff_max_seconds
        self._published_evidence_recorder = published_evidence_recorder
        self._clock = clock
        self._authority: LabClaimPublicationFinalizerAuthority | None = None
        self._stop = Event()
        self._closed = False

    def request_stop(self) -> None:
        self._stop.set()

    def _current_authority(self, *, now: datetime) -> LabClaimPublicationFinalizerAuthority:
        if self._authority is None:
            self._authority = self._authority_issuer.acquire(
                owner_id=self._owner_id,
                lease_seconds=self._lease_seconds,
                now=now,
            )
        else:
            self._authority = self._authority_issuer.renew(
                self._authority,
                lease_seconds=self._lease_seconds,
                now=now,
            )
        return self._authority

    def _source_stage_is_ready(self, record: LabClaimPublicationRecord) -> bool:
        if record.status is ClaimPublicationStatus.READY_TO_PUBLISH:
            return True
        if record.status is not ClaimPublicationStatus.SOURCE_QUEUED:
            return False
        try:
            binding = strict_model_validate_canonical_json(
                LabSourceStageBinding,
                record.source_stage_binding_bytes or b"",
            )
            stage = self._stage_reader.get(binding)
        except Exception:
            return False
        return (
            stage is not None
            and stage.binding == binding
            and stage.state is LabSourceStageState.READY
        )

    @staticmethod
    def _after_rollout_evidence_recorded() -> None:
        """Test-only crash boundary after external idempotency and before local ack."""

    def _drain_rollout_evidence(
        self,
        *,
        authority: LabClaimPublicationFinalizerAuthority,
        now: datetime,
    ) -> int:
        if self._published_evidence_recorder is None:
            return 0
        pending = self._ledger.list_due_claim_publication_rollout_evidence(
            authority=authority,
            now=now,
            limit=self._max_publications_per_tick,
        )
        worker_verifier = LabClaimPublicationWorkerVerifier(
            ledger=self._ledger,  # type: ignore[arg-type]
            current_claim_authority=self._current_claim_authority,
            keyring=self._keyring,
            audience=self._audience,
            spool_receipt_verifier=self._spool_receipt_verifier,
            trust_verifier=authority._trust_verifier,  # noqa: SLF001
        )
        recovered = 0
        for item in pending:
            final_claim = strict_model_validate_canonical_json(
                LabShardClaimV2,
                item.record.final_claim_bytes or b"",
            )
            worker_verifier.require_published_claim(final_claim, now=now)
            try:
                self._published_evidence_recorder(
                    attempt_id=str(item.evidence.attempt_id),
                    evidence_hash=item.evidence.evidence_hash,
                    publication_identity=item.evidence.publication_identity,
                )
            except (OSError, sqlite3.Error) as exc:
                error_class = type(exc).__name__
                if not error_class.isascii() or not error_class.isidentifier():
                    error_class = "Exception"
                self._ledger.finalizer_defer_claim_publication_rollout_evidence(
                    item,
                    authority=authority,
                    error_class=error_class,
                    next_retry_at=now + timedelta(seconds=self._failure_backoff_seconds),
                    now=now,
                )
                continue
            self._after_rollout_evidence_recorded()
            self._ledger.finalizer_ack_claim_publication_rollout_evidence(
                item,
                authority=authority,
                now=now,
            )
            recovered += 1
        return recovered

    def run_once(self) -> LabClaimFinalizerDaemonResult:
        if self._closed:
            raise RuntimeError("claim finalizer daemon is closed")
        now = self._clock()
        authority = self._current_authority(now=now)
        rollout_recovered = self._drain_rollout_evidence(
            authority=authority,
            now=now,
        )
        recovered = self._ledger.finalizer_drain_claim_publication_observation_degradations(
            authority=authority,
            now=now,
            limit=self._max_publications_per_tick,
        )
        candidates = self._ledger.list_claim_publication_finalizer_candidates(
            limit=self._max_publications_per_tick,
        )
        published = replayed = blocked = not_ready = 0
        finalizer = LabClaimFinalizer(
            ledger=self._ledger,  # type: ignore[arg-type]
            stage_reader=self._stage_reader,
            authority=authority,
            current_claim_authority=self._current_claim_authority,
            keyring=self._keyring,
            audience=self._audience,
            spool=self._spool,
            spool_receipt_verifier=self._spool_receipt_verifier,
            clock=self._clock,
        )
        for record in candidates:
            if not self._source_stage_is_ready(record):
                not_ready += 1
                continue
            result = finalizer.finalize(record.identity)
            published += int(result.status == "published")
            replayed += int(result.status == "replayed")
            blocked += int(result.status == "blocked")
            not_ready += int(result.status == "not_ready")
        rollout_recovered += self._drain_rollout_evidence(
            authority=authority,
            now=now,
        )
        return LabClaimFinalizerDaemonResult(
            candidates=len(candidates),
            published=published,
            replayed=replayed,
            blocked=blocked,
            not_ready=not_ready,
            observations_recovered=recovered,
            rollout_evidence_recovered=rollout_recovered,
        )

    def run_forever(self) -> None:
        backoff = self._failure_backoff_seconds
        try:
            while not self._stop.is_set():
                try:
                    self.run_once()
                except Exception as exc:
                    logger.error(
                        "lab claim finalizer tick failed: error_class={}",
                        type(exc).__name__,
                    )
                    self._stop.wait(backoff)
                    backoff = min(backoff * 2, self._failure_backoff_max_seconds)
                else:
                    backoff = self._failure_backoff_seconds
                    self._stop.wait(self._poll_interval_seconds)
        finally:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        if self._authority is not None:
            authority = self._authority
            self._authority = None
            self._authority_issuer.release(authority, now=self._clock())


__all__ = [
    "LabClaimFinalizerDaemon",
    "LabClaimFinalizerDaemonResult",
]
