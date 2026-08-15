from __future__ import annotations

import fcntl
import inspect
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.lab_source_stage import LabSourceStageStore
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2AuthorityRef,
    SourceBrokerV2JobIntentEnvelope,
    SourceBrokerV2JobOutcomeStatus,
    SourceBrokerV2JobOutcomeVerifier,
    SourceBrokerV2NativeEvidence,
    build_verified_job_outcome,
    canonical_job_model_bytes,
    canonical_request_bytes,
)
from rquant.source_broker_v2_queue import (
    SourceBrokerV2SchedulerQueue,
    SourceBrokerV2SchedulerQueueBackpressureError,
    SourceBrokerV2SchedulerQueueConflictError,
    SourceBrokerV2SchedulerQueueIntegrityError,
)
from rquant.source_broker_v2_runner import (
    SourceBrokerV2JobRunnerState,
    SourceBrokerV2StoreConfigError,
    initialize_source_broker_v2_job_storage,
    load_source_broker_v2_job_store_config,
)

from .source_broker_v2_authorized_intent_fixture import (
    authorities,
    authorized_intent,
    authorized_intent_from_payload_and_claim,
    authorized_payload_and_claim,
    stage_authorized_intent,
)

HASH_1 = "1" * 64
HASH_2 = "2" * 64
HASH_3 = "3" * 64
HASH_4 = "4" * 64
HASH_5 = "5" * 64
HASH_6 = "6" * 64
HASH_7 = "7" * 64


class _AcceptAllVerifier(SourceBrokerV2JobOutcomeVerifier):
    def verify_source(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        response: bytes,
        status: SourceBrokerV2JobOutcomeStatus,
        deadline: float,
    ) -> None:
        del intent, evidence, response, status, deadline

    def verify_claim(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None:
        del intent, evidence, deadline

    def verify_quota(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None:
        del intent, evidence, deadline

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
        del (
            intent,
            evidence,
            source_receipt_hash,
            claim_receipt_hash,
            quota_receipt_hash,
            deadline,
        )


def _authority(kind: str) -> SourceBrokerV2AuthorityRef:
    return SourceBrokerV2AuthorityRef(
        authority_id=f"{kind}-authority",
        key_id=f"{kind}-key-v2",
        purpose=f"rquant-{kind}-receipt",
        schema_version=2,
        generation=7,
        fence_hash=HASH_7,
    )


def _intent(
    stage_store: LabSourceStageStore,
    symbol: str = "000001.SZ",
) -> SourceBrokerV2JobIntentEnvelope:
    intent = authorized_intent(symbol=symbol)
    stage_authorized_intent(stage_store, intent)
    return intent


def _outcome(intent: SourceBrokerV2JobIntentEnvelope, *, response_rows: int = 1):
    evidence = tuple(
        SourceBrokerV2NativeEvidence.create(
            kind=kind,
            request=canonical_request_bytes({"kind": kind, "operation": intent.operation_id}),
            receipt=canonical_request_bytes({"kind": kind, "receipt": "verified"}),
        )
        for kind in ("source", "claim", "quota", "lineage")
    )
    return build_verified_job_outcome(
        intent=intent,
        status=SourceBrokerV2JobOutcomeStatus.SUCCESS,
        response=canonical_request_bytes({"rows": response_rows}),
        source_evidence=evidence[0],
        claim_evidence=evidence[1],
        quota_evidence=evidence[2],
        lineage_evidence=evidence[3],
        verifier=_AcceptAllVerifier(),
        deadline=1.0,
    )


def _stage_store(
    db_path: Path,
    *,
    max_inbox: int = 2,
) -> LabSourceStageStore:
    initialize_source_broker_v2_job_storage(
        db_path,
        busy_timeout_ms=2_000,
        max_inbox=max_inbox,
    )
    return LabSourceStageStore(
        db_path.with_name(f"{db_path.stem}.source-stage.sqlite3"),
        queue_store_path=db_path,
        manifest_keyring=authorities().authorization_keyring,
        authorization_keyring=authorities().authorization_keyring,
    )


def _open_queue(
    db_path: Path,
    stage_store: LabSourceStageStore,
    *,
    max_inbox: int = 2,
) -> SourceBrokerV2SchedulerQueue:
    initialize_source_broker_v2_job_storage(
        db_path,
        busy_timeout_ms=2_000,
        max_inbox=max_inbox,
    )
    return SourceBrokerV2SchedulerQueue(
        db_path,
        manifest_keyring=authorities().authorization_keyring,
        authorization_keyring=authorities().authorization_keyring,
        stage_store=stage_store,
        busy_timeout_ms=2_000,
    )


def _queue(
    db_path: Path,
    *,
    max_inbox: int = 2,
) -> tuple[SourceBrokerV2SchedulerQueue, LabSourceStageStore]:
    stage_store = _stage_store(db_path, max_inbox=max_inbox)
    return _open_queue(db_path, stage_store, max_inbox=max_inbox), stage_store


def _create_legacy_job_store(db_path: Path) -> None:
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE source_broker_v2_jobs (
                operation_id TEXT PRIMARY KEY NOT NULL,
                intent BLOB NOT NULL,
                intent_hash TEXT NOT NULL,
                source_id TEXT NOT NULL,
                operation_hash TEXT NOT NULL,
                request_hash TEXT NOT NULL,
                deadline_at TEXT NOT NULL,
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
                outcome BLOB,
                terminal_reason TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def test_scheduler_queue_has_only_canonical_non_executor_surface(tmp_path: Path) -> None:
    queue, _stage_store = _queue(tmp_path / "runner.sqlite3")

    assert {
        name for name in dir(queue) if not name.startswith("_") and callable(getattr(queue, name))
    } == {
        "enqueue_intent",
        "enqueue_intent_bytes",
        "get_state",
        "get_verified_published_outcome",
    }
    for forbidden in (
        "claim_pending",
        "close",
        "reconcile_once",
        "registry",
        "run_once",
        "serve_forever",
        "wake",
    ):
        assert not hasattr(queue, forbidden)


def test_scheduler_queue_is_idempotent_bounded_and_only_returns_verified_published_outcome(
    tmp_path: Path,
) -> None:
    queue, stage_store = _queue(tmp_path / "runner.sqlite3", max_inbox=1)
    intent = _intent(stage_store)

    assert queue.enqueue_intent(intent) == intent.operation_id
    assert queue.enqueue_intent(intent) == intent.operation_id
    assert queue.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.NEW
    with pytest.raises(KeyError):
        queue.get_verified_published_outcome(intent.operation_id)
    with pytest.raises(SourceBrokerV2SchedulerQueueBackpressureError):
        queue.enqueue_intent(_intent(stage_store, "000002.SZ"))


def test_scheduler_queue_rejects_direct_legacy_envelope_without_writing(tmp_path: Path) -> None:
    queue, stage_store = _queue(tmp_path / "runner.sqlite3")
    authorized = _intent(stage_store)
    legacy = SourceBrokerV2JobIntentEnvelope.model_validate(
        {
            **authorized.model_dump(mode="python"),
            "authorization": None,
            "authorization_payload": None,
            "authorization_payload_commitment": None,
            "authorization_template_commitment": None,
        },
        strict=True,
    )

    with pytest.raises(SourceBrokerV2SchedulerQueueConflictError, match="authorization"):
        queue.enqueue_intent(legacy)
    with sqlite3.connect(queue.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone() == (0,)


def test_scheduler_queue_requires_exact_current_stage_attempt_before_writing(
    tmp_path: Path,
) -> None:
    queue, stage_store = _queue(tmp_path / "runner.sqlite3")
    now = datetime.now(UTC)
    payload, claim = authorized_payload_and_claim(now=now, job_id=uuid4(), attempt_id=uuid4())
    intent = authorized_intent_from_payload_and_claim(payload, claim)
    stage_authorized_intent(stage_store, intent, now=now)
    assert queue.enqueue_intent(intent) == intent.operation_id

    altered_claims = (
        authorized_payload_and_claim(
            now=now,
            job_id=claim.job_id,
            attempt_id=uuid4(),
        )[1],
        authorized_payload_and_claim(
            now=now,
            job_id=claim.job_id,
            attempt_id=claim.claim_token,
            claim_generation=claim.claim_generation + 1,
        )[1],
        authorized_payload_and_claim(
            now=now,
            job_id=claim.job_id,
            attempt_id=claim.claim_token,
            scheduler_fencing_token=claim.scheduler_fencing_token + 1,
        )[1],
    )
    for altered_claim in altered_claims:
        altered = authorized_intent_from_payload_and_claim(payload, altered_claim)
        with pytest.raises(SourceBrokerV2SchedulerQueueConflictError, match="authorization"):
            queue.enqueue_intent(altered)
    with sqlite3.connect(queue.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone() == (1,)

    for candidate in (
        stage_store.path,
        Path(f"{stage_store.path}-wal"),
        Path(f"{stage_store.path}-shm"),
    ):
        candidate.unlink(missing_ok=True)
    LabSourceStageStore(
        stage_store.path,
        queue_store_path=queue.db_path,
        manifest_keyring=authorities().authorization_keyring,
        authorization_keyring=authorities().authorization_keyring,
    )
    with pytest.raises(SourceBrokerV2SchedulerQueueIntegrityError, match="source-stage"):
        queue.get_state(intent.operation_id)


def test_scheduler_queue_rejects_conflicting_or_tampered_shared_storage(tmp_path: Path) -> None:
    queue, stage_store = _queue(tmp_path / "runner.sqlite3")
    intent = _intent(stage_store)
    queue.enqueue_intent(intent)
    other = _intent(stage_store, "000002.SZ")
    with sqlite3.connect(queue.db_path) as connection:
        connection.execute(
            "UPDATE source_broker_v2_jobs SET intent = ? WHERE operation_id = ?",
            (canonical_job_model_bytes(other), intent.operation_id),
        )
    with pytest.raises(SourceBrokerV2SchedulerQueueConflictError):
        queue.enqueue_intent(intent)

    queue, stage_store = _queue(tmp_path / "tampered.sqlite3")
    stage_authorized_intent(stage_store, intent)
    queue.enqueue_intent(intent)
    with sqlite3.connect(queue.db_path) as connection:
        connection.execute(
            "UPDATE source_broker_v2_jobs SET state = 'PUBLISHED', outcome = ? "
            "WHERE operation_id = ?",
            (b"{}", intent.operation_id),
        )
    with pytest.raises(SourceBrokerV2SchedulerQueueIntegrityError):
        queue.get_verified_published_outcome(intent.operation_id)


def test_scheduler_queue_initialization_is_concurrent_and_leaves_wal_for_ordinary_connections(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "runner.sqlite3"
    initialize_source_broker_v2_job_storage(
        db_path,
        max_inbox=8,
        busy_timeout_ms=2_000,
    )
    stage_store = LabSourceStageStore(
        tmp_path / "source-stage.sqlite3",
        queue_store_path=db_path,
        manifest_keyring=authorities().authorization_keyring,
        authorization_keyring=authorities().authorization_keyring,
    )

    def open_queue(_: int) -> SourceBrokerV2SchedulerQueue:
        initialize_source_broker_v2_job_storage(
            db_path,
            max_inbox=8,
            busy_timeout_ms=2_000,
        )
        return SourceBrokerV2SchedulerQueue(
            db_path,
            manifest_keyring=authorities().authorization_keyring,
            authorization_keyring=authorities().authorization_keyring,
            stage_store=stage_store,
            busy_timeout_ms=2_000,
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        queues = tuple(executor.map(open_queue, range(8)))

    assert len(queues) == 8
    with sqlite3.connect(db_path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name = 'source_broker_v2_jobs'"
            ).fetchone()
            is not None
        )


def test_queue_rejects_self_consistent_outcome_without_runner_published_commit(
    tmp_path: Path,
) -> None:
    queue, stage_store = _queue(tmp_path / "runner.sqlite3", max_inbox=1)
    intent = _intent(stage_store)
    queue.enqueue_intent(intent)
    replacement = _outcome(intent, response_rows=999)
    with sqlite3.connect(queue.db_path) as connection:
        connection.execute(
            """
            UPDATE source_broker_v2_jobs
            SET state = 'PUBLISHED', outcome = ?
            WHERE operation_id = ?
            """,
            (canonical_job_model_bytes(replacement), intent.operation_id),
        )

    with pytest.raises(SourceBrokerV2SchedulerQueueIntegrityError):
        queue.get_verified_published_outcome(intent.operation_id)


def test_scheduler_queue_does_not_offer_a_free_max_inbox_override() -> None:
    parameters = inspect.signature(SourceBrokerV2SchedulerQueue).parameters

    assert "max_inbox" not in parameters


def test_schema_initialization_lock_has_a_bounded_monotonic_timeout(tmp_path: Path) -> None:
    db_path = tmp_path / "runner.sqlite3"
    lock_path = db_path.with_name(f".{db_path.name}.source-broker-v2-schema.lock")
    descriptor = open(lock_path, "a+b")  # noqa: SIM115
    errors: list[BaseException] = []
    worker = threading.Thread(
        target=lambda: _capture_error(
            errors,
            lambda: initialize_source_broker_v2_job_storage(
                db_path,
                busy_timeout_ms=40,
            ),
        )
    )
    fcntl.flock(descriptor.fileno(), fcntl.LOCK_EX)
    started = time.monotonic()
    try:
        worker.start()
        worker.join(timeout=0.25)
        elapsed = time.monotonic() - started
        assert not worker.is_alive()
        assert elapsed < 0.25
        assert len(errors) == 1
        assert isinstance(errors[0], TimeoutError)
    finally:
        fcntl.flock(descriptor.fileno(), fcntl.LOCK_UN)
        descriptor.close()
        worker.join(timeout=1)


def test_legacy_db_without_config_migrates_to_immutable_runner_capacity(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite3"
    _create_legacy_job_store(db_path)

    initialize_source_broker_v2_job_storage(
        db_path,
        busy_timeout_ms=2_000,
        max_inbox=3,
    )
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        first_config = load_source_broker_v2_job_store_config(connection)

    initialize_source_broker_v2_job_storage(
        db_path,
        busy_timeout_ms=2_000,
        max_inbox=3,
    )
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        assert load_source_broker_v2_job_store_config(connection) == first_config
    assert first_config.max_inbox == 3
    assert len(first_config.store_id) == 64

    with pytest.raises(SourceBrokerV2StoreConfigError, match="max_inbox"):
        initialize_source_broker_v2_job_storage(
            db_path,
            busy_timeout_ms=2_000,
            max_inbox=4,
        )


def test_legacy_published_row_without_commit_stays_fail_closed_after_migration(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy.sqlite3"
    stage_store = _stage_store(db_path, max_inbox=3)
    intent = _intent(stage_store)
    with sqlite3.connect(db_path) as connection:
        connection.execute("DROP TABLE source_broker_v2_jobs")
    _create_legacy_job_store(db_path)
    now = datetime.now(UTC).isoformat(timespec="microseconds")
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO source_broker_v2_jobs (
                operation_id, intent, intent_hash, source_id, operation_hash,
                request_hash, deadline_at, state, lease_generation, outcome,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'PUBLISHED', 0, ?, ?, ?)
            """,
            (
                intent.operation_id,
                canonical_job_model_bytes(intent),
                intent.intent_hash,
                intent.source_id,
                intent.operation_hash,
                intent.request_hash,
                intent.deadline.isoformat(timespec="microseconds"),
                canonical_job_model_bytes(_outcome(intent)),
                now,
                now,
            ),
        )

    queue = _open_queue(db_path, stage_store, max_inbox=3)
    with sqlite3.connect(db_path) as connection:
        commitment = connection.execute(
            "SELECT published_commit_hash FROM source_broker_v2_jobs WHERE operation_id = ?",
            (intent.operation_id,),
        ).fetchone()
    assert commitment == (None,)
    with pytest.raises(SourceBrokerV2SchedulerQueueIntegrityError):
        queue.get_verified_published_outcome(intent.operation_id)


def _capture_error(errors: list[BaseException], function: object) -> None:
    try:
        function()  # type: ignore[operator]
    except BaseException as exc:
        errors.append(exc)
