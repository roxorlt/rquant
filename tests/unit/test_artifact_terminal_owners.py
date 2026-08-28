from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.artifact_retention import (
    ArtifactReferenceStore,
    ObjectIdentity,
    ObjectReference,
)
from rquant.artifact_terminal_owners import (
    AuditTerminalReceiptProducer,
    ExperimentTerminalReceiptProducer,
    SnapshotTerminalReceiptProducer,
    TerminalReleaseOutboxPublisher,
)
from rquant.data_metadata import DataAuditRun, DatasetSnapshot
from rquant.experiment_registry import (
    DateRange,
    ExperimentAttempt,
    ExperimentSpec,
    ExperimentStatus,
)

NOW = datetime(2026, 7, 31, 8, 0, tzinfo=UTC)
CONTENT_HASH = "a" * 64


class AuditAuthority:
    def __init__(self, value: DataAuditRun | None) -> None:
        self.value = value

    def get_data_audit_run(self, audit_run_id: str) -> DataAuditRun | None:
        assert self.value is None or self.value.audit_run_id == audit_run_id
        return self.value


class SnapshotAuthority:
    def __init__(self, value: DatasetSnapshot | None) -> None:
        self.value = value

    def get_dataset_snapshot(self, snapshot_id: str) -> DatasetSnapshot | None:
        assert self.value is None or self.value.snapshot_id == snapshot_id
        return self.value


class ExperimentAuthority:
    def __init__(self, value: ExperimentAttempt | None) -> None:
        self.value = value

    def get_attempt(self, experiment_id: str) -> ExperimentAttempt:
        if self.value is None or self.value.spec.experiment_id != experiment_id:
            raise KeyError(experiment_id)
        return self.value


def _store(tmp_path: Path, *, owner_type: str, owner_id: str) -> ArtifactReferenceStore:
    store = ArtifactReferenceStore(
        tmp_path / "artifact-references.sqlite3",
        managed_trust_root=tmp_path,
        clock=lambda: NOW,
    )
    store.register_object(
        ObjectIdentity(
            content_sha256=CONTENT_HASH,
            size_bytes=1,
            object_kind="test-artifact",
            created_at=NOW - timedelta(minutes=2),
        )
    )
    store.register_reference(
        ObjectReference(
            owner_type=owner_type,
            owner_id=owner_id,
            content_sha256=CONTENT_HASH,
            created_at=NOW - timedelta(minutes=1),
        )
    )
    return store


def _audit(*, status: str) -> DataAuditRun:
    return DataAuditRun(
        as_of_date=date(2026, 7, 30),
        range_start=date(2026, 7, 1),
        range_end=date(2026, 7, 30),
        rule_set_version="stage1-v2",
        status=status,
        observed_at=NOW - timedelta(hours=2),
        completed_at=(NOW - timedelta(hours=1) if status != "running" else None),
        error_message=("failed" if status == "failed" else None),
    )


def _snapshot(*, status: str) -> DatasetSnapshot:
    return DatasetSnapshot(
        strategy_name="n-shape",
        as_of_time=NOW - timedelta(days=1),
        code_commit="1" * 40,
        origin="production",
        status=status,
        created_at=NOW - timedelta(hours=2),
        completed_at=(NOW - timedelta(hours=1) if status == "ready" else None),
    )


def _experiment(*, status: ExperimentStatus) -> ExperimentAttempt:
    spec = ExperimentSpec(
        strategy_spec_fingerprint="1" * 64,
        strategy_executable_fingerprint="2" * 64,
        candidate_schema_fingerprint="3" * 64,
        dataset_snapshot_id="4" * 64,
        code_commit="5" * 40,
        parameter_fingerprint="6" * 64,
        hypothesis_family="family-a",
        metric_definition_fingerprint="7" * 64,
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 3, 31)),
        validation_range=DateRange(start_date=date(2025, 4, 1), end_date=date(2025, 6, 30)),
        frozen_outer_test_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 9, 30)),
        cost_model_fingerprint="8" * 64,
        execution_model_fingerprint="9" * 64,
        seed=7,
    )
    registered = NOW - timedelta(hours=3)
    if status is ExperimentStatus.REGISTERED:
        return ExperimentAttempt(spec=spec, status=status, registered_at=registered)
    if status is ExperimentStatus.RUNNING:
        return ExperimentAttempt(
            spec=spec,
            status=status,
            registered_at=registered,
            started_at=NOW - timedelta(hours=2),
        )
    return ExperimentAttempt(
        spec=spec,
        status=status,
        registered_at=registered,
        started_at=NOW - timedelta(hours=2),
        completed_at=NOW - timedelta(hours=1),
        first_error=(None if status is ExperimentStatus.EXECUTED else "terminal failure"),
    )


@pytest.mark.parametrize(
    ("owner_type", "record_factory", "producer_factory", "terminal_state"),
    [
        (
            "audit",
            lambda: _audit(status="completed"),
            lambda store, value: AuditTerminalReceiptProducer(store, AuditAuthority(value)),
            "completed",
        ),
        (
            "snapshot",
            lambda: _snapshot(status="ready"),
            lambda store, value: SnapshotTerminalReceiptProducer(store, SnapshotAuthority(value)),
            "completed",
        ),
        (
            "experiment",
            lambda: _experiment(status=ExperimentStatus.FAILED),
            lambda store, value: ExperimentTerminalReceiptProducer(
                store, ExperimentAuthority(value)
            ),
            "failed",
        ),
        (
            "experiment",
            lambda: _experiment(status=ExperimentStatus.EXECUTED),
            lambda store, value: ExperimentTerminalReceiptProducer(
                store, ExperimentAuthority(value)
            ),
            "executed",
        ),
    ],
)
def test_authority_terminal_callpoints_enqueue_content_bound_receipts_and_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owner_type: str,
    record_factory: object,
    producer_factory: object,
    terminal_state: str,
) -> None:
    record = record_factory()  # type: ignore[operator]
    owner_id = (
        record.audit_run_id
        if owner_type == "audit"
        else record.snapshot_id
        if owner_type == "snapshot"
        else record.spec.experiment_id
    )
    store = _store(tmp_path, owner_type=owner_type, owner_id=owner_id)
    producer = producer_factory(store, record)  # type: ignore[operator]

    def crash(_receipt: object) -> None:
        raise RuntimeError("crash after durable enqueue")

    monkeypatch.setattr(producer, "_after_enqueued", crash)
    with pytest.raises(RuntimeError, match="crash after durable enqueue"):
        producer.on_terminal(owner_id=owner_id, content_sha256=CONTENT_HASH, observed_at=NOW)

    restarted = producer_factory(store, record)  # type: ignore[operator]
    receipt = restarted.on_terminal(
        owner_id=owner_id,
        content_sha256=CONTENT_HASH,
        observed_at=NOW + timedelta(seconds=1),
    )
    assert receipt.owner_type == owner_type
    assert receipt.terminal_state == terminal_state
    assert receipt.evidence_sha256 != CONTENT_HASH
    assert store.pending_owner_terminal_releases(limit=10) == (receipt,)

    publisher = TerminalReleaseOutboxPublisher(store)
    assert publisher.run_batch(limit=10) == 1
    assert publisher.run_batch(limit=10) == 0
    assert store.pending_owner_terminal_releases(limit=10) == ()
    assert (
        restarted.on_terminal(
            owner_id=owner_id,
            content_sha256=CONTENT_HASH,
            observed_at=NOW,
        )
        == receipt
    )


@pytest.mark.parametrize(
    ("owner_type", "record", "producer_factory"),
    [
        (
            "audit",
            _audit(status="running"),
            lambda store, value: AuditTerminalReceiptProducer(store, AuditAuthority(value)),
        ),
        (
            "snapshot",
            _snapshot(status="building"),
            lambda store, value: SnapshotTerminalReceiptProducer(store, SnapshotAuthority(value)),
        ),
        (
            "experiment",
            _experiment(status=ExperimentStatus.RUNNING),
            lambda store, value: ExperimentTerminalReceiptProducer(
                store, ExperimentAuthority(value)
            ),
        ),
    ],
)
def test_nonterminal_authority_never_enqueues_release(
    tmp_path: Path,
    owner_type: str,
    record: object,
    producer_factory: object,
) -> None:
    owner_id = (
        record.audit_run_id
        if owner_type == "audit"
        else record.snapshot_id
        if owner_type == "snapshot"
        else record.spec.experiment_id
    )
    store = _store(tmp_path, owner_type=owner_type, owner_id=owner_id)
    producer = producer_factory(store, record)  # type: ignore[operator]

    with pytest.raises(ValueError, match="not terminal"):
        producer.on_terminal(owner_id=owner_id, content_sha256=CONTENT_HASH, observed_at=NOW)

    assert store.pending_owner_terminal_releases(limit=10) == ()
