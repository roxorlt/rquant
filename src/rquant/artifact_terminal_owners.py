"""Authority-bound terminal receipt producers for durable artifact owners."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Protocol

from rquant.artifact_retention import (
    ArtifactReferenceStore,
    ObjectReference,
    OwnerTerminalReleaseReceipt,
)
from rquant.data_metadata import DataAuditRun, DatasetSnapshot
from rquant.experiment_registry import ExperimentAttempt, ExperimentStatus
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc


class DataAuditRunAuthority(Protocol):
    def get_data_audit_run(self, audit_run_id: str) -> DataAuditRun | None: ...


class DatasetSnapshotAuthority(Protocol):
    def get_dataset_snapshot(self, snapshot_id: str) -> DatasetSnapshot | None: ...


class ExperimentAttemptAuthority(Protocol):
    def get_attempt(self, experiment_id: str) -> ExperimentAttempt: ...


class _TerminalReceiptProducer:
    owner_type: str

    def __init__(self, reference_store: ArtifactReferenceStore) -> None:
        self.reference_store = reference_store

    def _reference(self, *, owner_id: str, content_sha256: str) -> ObjectReference:
        matches = tuple(
            reference
            for reference in self.reference_store.list_active_references(content_sha256)
            if (reference.owner_type, reference.owner_id) == (self.owner_type, owner_id)
        )
        if len(matches) != 1:
            raise ValueError(
                f"{self.owner_type} terminal producer requires exactly one active reference"
            )
        return matches[0]

    def _enqueue(
        self,
        *,
        owner_id: str,
        content_sha256: str,
        terminal_state: str,
        authority_record: object,
        terminal_at: datetime,
        observed_at: datetime,
    ) -> OwnerTerminalReleaseReceipt:
        observed = normalize_aware_utc(observed_at)
        terminal = normalize_aware_utc(terminal_at)
        if terminal > observed:
            raise ValueError(f"{self.owner_type} terminal authority is from the future")
        evidence_sha256 = canonical_sha256(
            {
                "contract": f"artifact-{self.owner_type}-terminal-authority/v1",
                "owner_id": owner_id,
                "terminal_at": terminal,
                "authority_record": authority_record,
            }
        )
        existing = self.reference_store.get_pending_owner_terminal_release_receipt(
            owner_type=self.owner_type,
            owner_id=owner_id,
            content_sha256=content_sha256,
        ) or self.reference_store.get_owner_terminal_release_receipt(
            owner_type=self.owner_type,
            owner_id=owner_id,
            content_sha256=content_sha256,
        )
        if existing is not None:
            if (
                existing.terminal_state,
                existing.lifecycle_revision,
                existing.evidence_sha256,
            ) != (terminal_state, 1, evidence_sha256):
                raise ValueError(
                    f"{self.owner_type} terminal authority conflicts with published receipt"
                )
            return existing
        reference = self._reference(owner_id=owner_id, content_sha256=content_sha256)
        receipt = OwnerTerminalReleaseReceipt(
            reference_id=reference.reference_id,
            owner_type=self.owner_type,
            owner_id=owner_id,
            content_sha256=content_sha256,
            terminal_state=terminal_state,
            lifecycle_revision=1,
            evidence_sha256=evidence_sha256,
            released_at=observed,
        )
        self.reference_store.enqueue_owner_terminal_release(receipt, enqueued_at=observed)
        self._after_enqueued(receipt)
        return receipt

    @staticmethod
    def _after_enqueued(_receipt: OwnerTerminalReleaseReceipt) -> None:
        """Fault-injection boundary after the immutable outbox write."""


class AuditTerminalReceiptProducer(_TerminalReceiptProducer):
    owner_type = "audit"

    def __init__(
        self,
        reference_store: ArtifactReferenceStore,
        authority: DataAuditRunAuthority,
    ) -> None:
        super().__init__(reference_store)
        self.authority = authority

    def on_terminal(
        self,
        *,
        owner_id: str,
        content_sha256: str,
        observed_at: datetime,
    ) -> OwnerTerminalReleaseReceipt:
        audit = self.authority.get_data_audit_run(owner_id)
        if audit is None:
            raise ValueError("audit terminal authority is missing")
        if audit.audit_run_id != owner_id:
            raise ValueError("audit terminal authority identity conflicts")
        if audit.status not in {"completed", "failed"} or audit.completed_at is None:
            raise ValueError("audit authority is not terminal")
        return self._enqueue(
            owner_id=owner_id,
            content_sha256=content_sha256,
            terminal_state=audit.status,
            authority_record=audit,
            terminal_at=audit.completed_at,
            observed_at=observed_at,
        )


class SnapshotTerminalReceiptProducer(_TerminalReceiptProducer):
    owner_type = "snapshot"

    def __init__(
        self,
        reference_store: ArtifactReferenceStore,
        authority: DatasetSnapshotAuthority,
    ) -> None:
        super().__init__(reference_store)
        self.authority = authority

    def on_terminal(
        self,
        *,
        owner_id: str,
        content_sha256: str,
        observed_at: datetime,
    ) -> OwnerTerminalReleaseReceipt:
        snapshot = self.authority.get_dataset_snapshot(owner_id)
        if snapshot is None:
            raise ValueError("snapshot terminal authority is missing")
        if snapshot.snapshot_id != owner_id:
            raise ValueError("snapshot terminal authority identity conflicts")
        if snapshot.status != "ready" or snapshot.completed_at is None:
            raise ValueError("snapshot authority is not terminal")
        return self._enqueue(
            owner_id=owner_id,
            content_sha256=content_sha256,
            terminal_state="completed",
            authority_record=snapshot,
            terminal_at=snapshot.completed_at,
            observed_at=observed_at,
        )


class ExperimentTerminalReceiptProducer(_TerminalReceiptProducer):
    owner_type = "experiment"

    def __init__(
        self,
        reference_store: ArtifactReferenceStore,
        authority: ExperimentAttemptAuthority,
    ) -> None:
        super().__init__(reference_store)
        self.authority = authority

    def on_terminal(
        self,
        *,
        owner_id: str,
        content_sha256: str,
        observed_at: datetime,
    ) -> OwnerTerminalReleaseReceipt:
        try:
            attempt = self.authority.get_attempt(owner_id)
        except KeyError as exc:
            raise ValueError("experiment terminal authority is missing") from exc
        if attempt.spec.experiment_id != owner_id:
            raise ValueError("experiment terminal authority identity conflicts")
        terminal_statuses = {
            ExperimentStatus.EXECUTED,
            ExperimentStatus.SUCCEEDED,
            ExperimentStatus.FAILED,
            ExperimentStatus.CANCELLED,
        }
        if attempt.status not in terminal_statuses or attempt.completed_at is None:
            raise ValueError("experiment authority is not terminal")
        return self._enqueue(
            owner_id=owner_id,
            content_sha256=content_sha256,
            terminal_state=attempt.status.value,
            authority_record=attempt,
            terminal_at=attempt.completed_at,
            observed_at=observed_at,
        )


class TerminalReleaseOutboxPublisher:
    """Idempotently publish authority-produced receipts from the durable outbox."""

    def __init__(self, reference_store: ArtifactReferenceStore) -> None:
        self.reference_store = reference_store

    def run_batch(self, *, limit: int) -> int:
        published = 0
        for receipt in self.reference_store.pending_owner_terminal_releases(limit=limit):
            published += int(
                self.reference_store.publish_owner_terminal_release(receipt.receipt_id)
            )
        return published


class ArtifactTerminalLifecycleRouter:
    """Route real owner terminal transitions into the immutable release outbox."""

    def __init__(
        self,
        *,
        reference_store: ArtifactReferenceStore,
        producers: Mapping[str, _TerminalReceiptProducer],
        max_references_per_owner: int = 10_000,
    ) -> None:
        if not producers:
            raise ValueError("artifact terminal lifecycle router requires producers")
        if not 1 <= max_references_per_owner <= 10_000:
            raise ValueError("artifact terminal lifecycle reference budget is out of bounds")
        for owner_type, producer in producers.items():
            if owner_type != producer.owner_type:
                raise ValueError("artifact terminal producer route identity conflicts")
            if producer.reference_store is not reference_store:
                raise ValueError("artifact terminal producers must share one reference store")
        self.reference_store = reference_store
        self.producers = dict(producers)
        self.max_references_per_owner = max_references_per_owner

    def __call__(self, owner_type: str, owner_id: str, observed_at: datetime) -> None:
        try:
            producer = self.producers[owner_type]
        except KeyError as exc:
            raise ValueError(f"artifact terminal producer is missing for {owner_type}") from exc
        references = self.reference_store.list_active_owner_references(
            owner_type=owner_type,
            owner_id=owner_id,
            limit=self.max_references_per_owner,
        )
        for reference in references:
            producer.on_terminal(
                owner_id=owner_id,
                content_sha256=reference.content_sha256,
                observed_at=observed_at,
            )


class ArtifactTerminalLifecycleHooks:
    """One injectable terminal-event endpoint for all durable artifact owners.

    Source authorities call ``on_terminal`` only after their own terminal state is
    durable. The endpoint writes immutable release evidence to the shared outbox;
    the retention runtime publishes that outbox in its normal ``run_step``.
    """

    def __init__(
        self,
        *,
        reference_store: ArtifactReferenceStore,
        producers: Mapping[str, _TerminalReceiptProducer],
        max_references_per_owner: int = 10_000,
    ) -> None:
        self.router = ArtifactTerminalLifecycleRouter(
            reference_store=reference_store,
            producers=producers,
            max_references_per_owner=max_references_per_owner,
        )

    def on_terminal(
        self,
        owner_type: str,
        owner_id: str,
        observed_at: datetime,
    ) -> None:
        self.router(owner_type, owner_id, observed_at)

    def __call__(self, owner_type: str, owner_id: str, observed_at: datetime) -> None:
        self.on_terminal(owner_type, owner_id, observed_at)
