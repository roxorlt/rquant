from __future__ import annotations

import inspect
import shutil
import sqlite3
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from uuid import UUID, uuid4

import pytest

import rquant.lab_source_stage as lab_source_stage
from rquant.lab_jobs import (
    CurrentSchedulerFenceReceipt,
    JobStoreSchedulerFenceVerifier,
    LabJobStore,
    LabLeaseRecord,
    _test_only_scheduler_fence_hold_hook,
)
from rquant.lab_shard_protocol import (
    LabShardClaimV2,
    StrategyShardPayloadV1,
    StrategyShardPayloadV2,
)
from rquant.lab_source_stage import (
    LabSourceStageAuthorityError,
    LabSourceStageBinding,
    LabSourceStageConflictError,
    LabSourceStageIntegrityError,
    LabSourceStageLeaseFencedError,
    LabSourceStageState,
    LabSourceStageStore,
    LabSourceStageTransitionError,
)
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2AuthorityRef,
    SourceBrokerV2JobIntentEnvelope,
)
from rquant.source_broker_v2_queue import SourceBrokerV2SchedulerQueue
from rquant.source_broker_v2_runner import (
    SourceBrokerV2JobRunnerState,
    initialize_source_broker_v2_job_storage,
)
from rquant.source_operation_contracts import (
    SourceIntentV2,
    SourceResourceRequestV2,
    build_source_broker_v2_scheduler_intent,
)

from . import test_source_broker_v2_runner as runner_test
from .source_broker_v2_authorized_intent_fixture import (
    authorities as authorization_authorities,
)
from .source_broker_v2_authorized_intent_fixture import (
    authorized_payload_and_claim,
)
from .test_adapter_manifest import create_test_authorities, signed_manifest

HASH_1 = "1" * 64
HASH_2 = "2" * 64
HASH_3 = "3" * 64
HASH_4 = "4" * 64
HASH_5 = "5" * 64
HASH_6 = "6" * 64
HASH_7 = "7" * 64
NOW = datetime(2026, 8, 10, 1, 0, tzinfo=UTC)
JOB_ID = UUID("11111111-2222-3333-4444-555555555555")
SHARD_ID = UUID("22222222-3333-4444-5555-666666666666")
ATTEMPT_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_INTENT_CLAIMS: dict[str, tuple[StrategyShardPayloadV2, LabShardClaimV2]] = {}


class _FakePublishedOutcomeReader:
    def __init__(
        self,
        outcome: object,
        *,
        state: SourceBrokerV2JobRunnerState = SourceBrokerV2JobRunnerState.PUBLISHED,
    ) -> None:
        self.outcome = outcome
        self.state = state

    def get_state(self, operation_id: str) -> SourceBrokerV2JobRunnerState:
        assert operation_id == self.outcome.operation_id  # type: ignore[attr-defined]
        return self.state

    def get_verified_published_outcome(self, operation_id: str):
        assert operation_id == self.outcome.operation_id  # type: ignore[attr-defined]
        return self.outcome


def _authority(kind: str) -> SourceBrokerV2AuthorityRef:
    return SourceBrokerV2AuthorityRef(
        authority_id=f"{kind}-authority",
        key_id=f"{kind}-key-v2",
        purpose=f"rquant-{kind}-receipt",
        schema_version=2,
        generation=7,
        fence_hash=HASH_7,
    )


def _binding(
    *,
    scheduler_fencing_token: int,
    attempt_id: UUID = ATTEMPT_ID,
    spec_hash: str = HASH_1,
    plan_hash: str = HASH_3,
) -> LabSourceStageBinding:
    return _authorized_binding(
        scheduler_fencing_token=scheduler_fencing_token,
        attempt_id=attempt_id,
        spec_hash=spec_hash,
        plan_hash=plan_hash,
    )


def _authorized_binding(
    *,
    scheduler_fencing_token: int,
    attempt_id: UUID = ATTEMPT_ID,
    spec_hash: str = HASH_1,
    plan_hash: str = HASH_3,
    source_authority: SourceBrokerV2AuthorityRef | None = None,
    now: datetime = NOW,
) -> LabSourceStageBinding:
    payload, claim = authorized_payload_and_claim(
        source_authority=source_authority,
        now=now,
        job_id=JOB_ID,
        spec_hash=spec_hash,
        attempt_id=attempt_id,
        claim_generation=3,
        scheduler_fencing_token=scheduler_fencing_token,
        worker_id="lab-worker-a",
        plan_hash=plan_hash,
        shard_index=0,
        payload_json='{"partition":"2026-08-10"}',
    )
    binding = LabSourceStageBinding(
        job_id=claim.job_id,
        shard_id=claim.shard_id,
        claim_token=claim.claim_token,
        attempt_id=claim.claim_token,
        claim_generation=claim.claim_generation,
        scheduler_fencing_token=claim.scheduler_fencing_token,
        worker_id=claim.worker_id,
        spec_hash=claim.spec_hash,
        plan_hash=claim.definition.plan_hash,
    )
    _INTENT_CLAIMS[binding.binding_hash] = (payload, claim)
    return binding


def _intent(
    binding: LabSourceStageBinding,
) -> SourceBrokerV2JobIntentEnvelope:
    payload, claim = _INTENT_CLAIMS[binding.binding_hash]
    return build_source_broker_v2_scheduler_intent(
        payload,
        claim=claim,
        manifest_keyring=authorization_authorities().authorization_keyring,
        authorization_keyring=authorization_authorities().authorization_keyring,
        deadline=claim.claimed_at + timedelta(seconds=60),
        now=claim.claimed_at,
    )


def _payloads(tmp_path: Path) -> tuple[StrategyShardPayloadV1, StrategyShardPayloadV2]:
    offline = StrategyShardPayloadV1(
        adapter_id="research.local",
        adapter_version="1.0.0",
        payload_json='{"partition":"2026-08-10"}',
    )
    authorities = create_test_authorities(tmp_path / "authorities")
    manifest = signed_manifest(authorities)
    request = SourceResourceRequestV2.from_manifest(manifest, requested_calls=1)
    source_intent = SourceIntentV2.from_manifest(manifest, resource_request=request)
    external = StrategyShardPayloadV2.from_source_intent(
        adapter_id=manifest.adapter_id,
        adapter_version=manifest.adapter_version,
        payload_json='{"partition":"2026-08-10"}',
        source_intent=source_intent,
    )
    return offline, external


def _queue_store_path(tmp_path: Path, name: str = "runner-store") -> Path:
    path = tmp_path / name / "runner.sqlite3"
    initialize_source_broker_v2_job_storage(
        path,
        busy_timeout_ms=2_000,
        max_inbox=100,
    )
    return path


def _store(
    tmp_path: Path,
    *,
    queue_store_path: Path | None = None,
) -> LabSourceStageStore:
    return LabSourceStageStore(
        tmp_path / "source-stage.sqlite3",
        queue_store_path=queue_store_path or _queue_store_path(tmp_path),
        manifest_keyring=authorization_authorities().authorization_keyring,
        authorization_keyring=authorization_authorities().authorization_keyring,
    )


def _unlink_sqlite_family(path: Path) -> None:
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        candidate.unlink(missing_ok=True)


def _start_pending(store: LabSourceStageStore, *, owner_id: str = "scheduler-a"):
    writer = store.acquire_writer_lease(owner_id=owner_id, lease_seconds=30, now=NOW)
    binding = _binding(scheduler_fencing_token=writer.fencing_token)
    intent = _intent(binding)
    queued = store.enqueue_external(binding, intent, lease=writer, now=NOW)
    assert queued.state is LabSourceStageState.QUEUED
    pending = store.begin_external(binding, intent, lease=writer, now=NOW)
    assert pending.state is LabSourceStageState.PENDING
    return writer, binding, intent, pending


@contextmanager
def _real_published_queue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    stage_store: LabSourceStageStore | None = None,
    stage_writer: lab_source_stage.LabSourceStageWriterLease | None = None,
) -> Iterator[
    tuple[
        SourceBrokerV2SchedulerQueue,
        LabSourceStageBinding,
        SourceBrokerV2JobIntentEnvelope,
        runner_test.SourceBrokerV2JobRunner,
    ]
]:
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    security = runner_test._RunnerSourceAuthoritySecurity()
    runner = None
    try:
        transport = runner_test._RunnerTestTransport(security)
        runner = runner_test._runner(
            tmp_path / "runner-store",
            transport,
            stage_store=stage_store,
        )
        runner_stage_store = runner_test._STAGE_STORES_BY_TRANSPORT[id(transport)]
        if stage_store is not None and stage_writer is None:
            raise ValueError("exact source-stage store requires its writer lease")
        writer = stage_writer or runner_stage_store.acquire_writer_lease(
            owner_id="stage-authority-test",
            lease_seconds=120,
            now=datetime.now(UTC),
        )
        binding = _authorized_binding(
            scheduler_fencing_token=writer.fencing_token,
            source_authority=runner_test._authority("source", source_transport=transport),
            now=datetime.now(UTC),
        )
        intent = _intent(binding)
        runner_stage_store.enqueue_external(
            binding,
            intent,
            lease=writer,
            now=writer.acquired_at,
        )
        runner.enqueue_intent(intent)
        assert runner.run_once() == 1
        assert runner.get_state(intent.operation_id) is SourceBrokerV2JobRunnerState.PUBLISHED
        keyring = authorization_authorities().authorization_keyring
        queue = SourceBrokerV2SchedulerQueue(
            tmp_path / "runner-store" / "runner.sqlite3",
            manifest_keyring=keyring,
            authorization_keyring=keyring,
            stage_store=runner_stage_store,
        )
        yield queue, binding, intent, runner
    finally:
        if runner is not None:
            runner.close()
        security.close()


def test_source_stage_attempt_binding_begin_restart_ready_and_payload_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    offline, external = _payloads(tmp_path)
    current = datetime.now(UTC)
    writer = store.acquire_writer_lease(owner_id="scheduler-a", lease_seconds=30, now=current)
    with _real_published_queue(
        tmp_path,
        monkeypatch,
        stage_store=store,
        stage_writer=writer,
    ) as (queue, binding, intent, _runner):
        store.enqueue_external(binding, intent, lease=writer, now=current)
        pending = store.begin_external(binding, intent, lease=writer, now=current)

        assert store.is_claim_publishable(None, payload=offline)
        assert not store.is_claim_publishable(binding, payload=external)
        assert store.begin_external(binding, intent, lease=writer, now=current) == pending

        restarted = LabSourceStageStore(
            store.path,
            queue_store_path=store.queue_store_path,
            manifest_keyring=authorization_authorities().authorization_keyring,
            authorization_keyring=authorization_authorities().authorization_keyring,
        )
        assert restarted.get(binding) == pending
        ready = restarted.bind_published_outcome(
            binding,
            lease=writer,
            now=current + timedelta(seconds=1),
        )
        assert ready.state is LabSourceStageState.READY
        assert ready.outcome == queue.get_verified_published_outcome(intent.operation_id)
        assert ready.outcome_hash == ready.outcome.outcome_hash
        assert ready.evidence_chain_hash == ready.outcome.evidence_chain_hash
        assert ready.attempt_identity_hash == binding.attempt_identity_hash
        assert restarted.is_claim_publishable(binding, payload=external)
        assert (
            restarted.bind_published_outcome(
                binding,
                lease=writer,
                now=current + timedelta(seconds=1),
            )
            == ready
        )


def test_scheduler_origin_ready_requires_the_persisted_current_fence_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    current = datetime.now(UTC)
    scheduler_lease = jobs.acquire_scheduler_lease(
        owner_id="scheduler-a",
        lease_seconds=30,
        now=current,
    )
    bootstrap_binding = _binding(scheduler_fencing_token=scheduler_lease.fencing_token)
    receipt = jobs.issue_current_scheduler_fence_receipt(
        lease=scheduler_lease,
        binding=bootstrap_binding,
        now=current,
    )
    verifier = JobStoreSchedulerFenceVerifier(jobs)
    writer = store.acquire_writer_lease(
        owner_id="scheduler-a",
        lease_seconds=30,
        now=current,
        scheduler_fence_receipt=receipt,
        scheduler_fence_verifier=verifier,
        binding=bootstrap_binding,
    )
    with _real_published_queue(
        tmp_path,
        monkeypatch,
        stage_store=store,
        stage_writer=writer,
    ) as (_queue, binding, intent, _runner):
        receipt = jobs.issue_current_scheduler_fence_receipt(
            lease=scheduler_lease,
            binding=binding,
            now=current,
        )
        pending = store.begin_external(
            binding,
            intent,
            lease=writer,
            now=current,
            scheduler_fence_receipt=receipt,
            scheduler_fence_verifier=verifier,
        )
        assert pending.scheduler_fence_receipt_commitment == receipt.receipt_commitment
        assert pending.scheduler_fence_authority_commitment is not None
        with pytest.raises(LabSourceStageLeaseFencedError, match="exact fence receipt"):
            store.bind_published_outcome(binding, lease=writer, now=current)
        ready = store.bind_published_outcome(
            binding,
            lease=writer,
            now=current,
            scheduler_fence_receipt=receipt,
            scheduler_fence_verifier=verifier,
        )
    assert ready.state is LabSourceStageState.READY


def test_source_stage_begin_is_idempotent_but_rejects_cross_binding_and_intent(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    writer, binding, intent, pending = _start_pending(store)

    assert store.begin_external(binding, intent, lease=writer, now=NOW) == pending
    conflicting_binding = binding.model_copy(update={"spec_hash": HASH_2})
    with pytest.raises(LabSourceStageConflictError):
        store.begin_external(conflicting_binding, intent, lease=writer, now=NOW)
    conflicting_intent = _intent(binding).model_copy(update={"operation_id": "f" * 64})
    with pytest.raises(LabSourceStageConflictError):
        store.begin_external(binding, conflicting_intent, lease=writer, now=NOW)


def test_source_stage_requires_live_writer_fence_and_recovers_expired_pending(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    old_writer = store.acquire_writer_lease(owner_id="scheduler-a", lease_seconds=5, now=NOW)
    binding = _binding(scheduler_fencing_token=old_writer.fencing_token)
    intent = _intent(binding)
    store.begin_external(binding, intent, lease=old_writer, now=NOW)

    restarted = LabSourceStageStore(
        store.path,
        queue_store_path=store.queue_store_path,
    )
    replacement = restarted.acquire_writer_lease(
        owner_id="scheduler-b",
        lease_seconds=10,
        now=NOW + timedelta(seconds=6),
    )
    assert replacement.fencing_token == old_writer.fencing_token + 1
    with pytest.raises(LabSourceStageLeaseFencedError):
        restarted.mark_failed(
            binding,
            code="source_unavailable",
            lease=old_writer,
            now=NOW + timedelta(seconds=6),
        )
    assert (
        restarted.recover_expired_pending(
            lease=replacement,
            now=NOW + timedelta(seconds=6),
        )
        == 1
    )
    assert restarted.get(binding).state is LabSourceStageState.RECONCILE_REQUIRED  # type: ignore[union-attr]


def _expire_pending_record(
    store: LabSourceStageStore,
    binding: LabSourceStageBinding,
    *,
    now: datetime,
) -> None:
    with store._transaction() as connection:
        row = store._row_for_binding(connection, binding)
        assert row is not None
        values = store._values_from_row(row)
        values["writer_lease_expires_at"] = lab_source_stage._encode_time(now)
        values["record_hash"] = lab_source_stage._record_hash(values)
        store._update_record(connection, values)


def _scheduler_pending(
    tmp_path: Path,
) -> tuple[
    LabSourceStageStore,
    LabJobStore,
    LabLeaseRecord,
    LabSourceStageBinding,
    CurrentSchedulerFenceReceipt,
    JobStoreSchedulerFenceVerifier,
    lab_source_stage.LabSourceStageWriterLease,
]:
    store = _store(tmp_path)
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    scheduler_lease = jobs.acquire_scheduler_lease(
        owner_id="scheduler-a", lease_seconds=30, now=NOW
    )
    binding = _binding(scheduler_fencing_token=scheduler_lease.fencing_token)
    receipt = jobs.issue_current_scheduler_fence_receipt(
        lease=scheduler_lease,
        binding=binding,
        now=NOW,
    )
    verifier = JobStoreSchedulerFenceVerifier(jobs)
    writer = store.acquire_writer_lease(
        owner_id="scheduler-a",
        lease_seconds=30,
        now=NOW,
        scheduler_fence_receipt=receipt,
        scheduler_fence_verifier=verifier,
        binding=binding,
    )
    intent = _intent(binding)
    store.enqueue_external(
        binding,
        intent,
        lease=writer,
        now=NOW,
        scheduler_fence_receipt=receipt,
        scheduler_fence_verifier=verifier,
    )
    store.begin_external(
        binding,
        intent,
        lease=writer,
        now=NOW,
        scheduler_fence_receipt=receipt,
        scheduler_fence_verifier=verifier,
    )
    _expire_pending_record(store, binding, now=NOW + timedelta(seconds=1))
    return store, jobs, scheduler_lease, binding, receipt, verifier, writer


def test_recover_expired_pending_requires_and_holds_exact_scheduler_fence(
    tmp_path: Path,
) -> None:
    store, _jobs, _scheduler_lease, binding, receipt, verifier, writer = _scheduler_pending(
        tmp_path
    )

    with pytest.raises(LabSourceStageLeaseFencedError, match="exact fence receipt"):
        store.recover_expired_pending(lease=writer, now=NOW + timedelta(seconds=2))
    assert store.get(binding).state is LabSourceStageState.PENDING  # type: ignore[union-attr]

    assert (
        store.recover_expired_pending(
            lease=writer,
            now=NOW + timedelta(seconds=2),
            scheduler_fence_proof_provider=lambda observed: (
                (
                    receipt,
                    verifier,
                )
                if observed == binding
                else (_ for _ in ()).throw(AssertionError("unexpected binding"))
            ),
        )
        == 1
    )
    assert store.get(binding).state is LabSourceStageState.RECONCILE_REQUIRED  # type: ignore[union-attr]


@pytest.mark.parametrize("variant", ("forged", "stale", "replaced"))
def test_recover_expired_pending_fails_closed_for_invalid_scheduler_fence(
    tmp_path: Path,
    variant: str,
) -> None:
    store, jobs, scheduler_lease, binding, receipt, verifier, writer = _scheduler_pending(tmp_path)
    if variant == "forged":
        proof = receipt.model_copy(update={"owner_id": "scheduler-forged"})
        proof_verifier = verifier
    elif variant == "stale":
        _replace_scheduler_lease(jobs, scheduler_lease, NOW + timedelta(seconds=2))
        proof = receipt
        proof_verifier = verifier
    else:
        _unlink_sqlite_family(jobs.path)
        replacement = LabJobStore(jobs.path)
        replacement.initialize()
        proof = receipt
        proof_verifier = JobStoreSchedulerFenceVerifier(replacement)

    with pytest.raises(LabSourceStageLeaseFencedError, match="scheduler fence receipt"):
        store.recover_expired_pending(
            lease=writer,
            now=NOW + timedelta(seconds=2),
            scheduler_fence_proof_provider=lambda observed: (
                (proof, proof_verifier)
                if observed == binding
                else (_ for _ in ()).throw(AssertionError("unexpected binding"))
            ),
        )
    assert store.get(binding).state is LabSourceStageState.PENDING  # type: ignore[union-attr]


def test_recover_expired_pending_holds_job_fence_before_stage_commit(
    tmp_path: Path,
) -> None:
    store, jobs, scheduler_lease, binding, receipt, _verifier, writer = _scheduler_pending(tmp_path)
    entered = Event()
    release = Event()

    @contextmanager
    def paused_hold_current(
        _receipt: CurrentSchedulerFenceReceipt,
        _binding: LabSourceStageBinding,
        _now: datetime,
    ) -> Iterator[None]:
        entered.set()
        assert release.wait(timeout=2)
        yield

    verifier = JobStoreSchedulerFenceVerifier(jobs)
    with (
        _test_only_scheduler_fence_hold_hook(paused_hold_current),
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        recovery = executor.submit(
            store.recover_expired_pending,
            lease=writer,
            now=NOW + timedelta(seconds=2),
            scheduler_fence_proof_provider=lambda observed: (
                (receipt, verifier)
                if observed == binding
                else (_ for _ in ()).throw(AssertionError("unexpected binding"))
            ),
        )
        assert entered.wait(timeout=2)
        replacement = executor.submit(
            _replace_scheduler_lease,
            jobs,
            scheduler_lease,
            NOW + timedelta(seconds=2),
        )
        assert not replacement.done()
        release.set()
        assert recovery.result(timeout=2) == 1
        assert replacement.result(timeout=2).fencing_token > scheduler_lease.fencing_token

    assert store.get(binding).state is LabSourceStageState.RECONCILE_REQUIRED  # type: ignore[union-attr]


def test_recover_expired_pending_rejects_a_duck_typed_fence_verifier(
    tmp_path: Path,
) -> None:
    store, jobs, scheduler_lease, binding, receipt, _verifier, writer = _scheduler_pending(tmp_path)
    _replace_scheduler_lease(jobs, scheduler_lease, NOW + timedelta(seconds=2))

    class _ForgedVerifier:
        @contextmanager
        def hold_current(
            self,
            _receipt: CurrentSchedulerFenceReceipt,
            *,
            binding: LabSourceStageBinding,
            now: datetime,
        ) -> Iterator[None]:
            assert binding == receipt.binding
            assert now == NOW + timedelta(seconds=2)
            yield

    with pytest.raises(LabSourceStageLeaseFencedError, match="trusted JobStore verifier"):
        store.recover_expired_pending(
            lease=writer,
            now=NOW + timedelta(seconds=2),
            scheduler_fence_proof_provider=lambda observed: (
                (receipt, _ForgedVerifier())
                if observed == binding
                else (_ for _ in ()).throw(AssertionError("unexpected binding"))
            ),
        )
    assert store.get(binding).state is LabSourceStageState.PENDING  # type: ignore[union-attr]


def test_recover_expired_pending_rejects_class_method_replay_after_lease_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, jobs, scheduler_lease, binding, receipt, verifier, writer = _scheduler_pending(tmp_path)
    _replace_scheduler_lease(jobs, scheduler_lease, NOW + timedelta(seconds=2))

    @contextmanager
    def forged_hold_current(
        _verifier: JobStoreSchedulerFenceVerifier,
        _receipt: CurrentSchedulerFenceReceipt,
        *,
        binding: LabSourceStageBinding,
        now: datetime,
    ) -> Iterator[None]:
        assert binding == receipt.binding
        assert now == NOW + timedelta(seconds=2)
        yield

    monkeypatch.setattr(
        JobStoreSchedulerFenceVerifier,
        "hold_current",
        forged_hold_current,
    )

    with pytest.raises(LabSourceStageLeaseFencedError, match="scheduler fence receipt"):
        store.recover_expired_pending(
            lease=writer,
            now=NOW + timedelta(seconds=2),
            scheduler_fence_proof_provider=lambda observed: (
                (receipt, verifier)
                if observed == binding
                else (_ for _ in ()).throw(AssertionError("unexpected binding"))
            ),
        )
    assert store.get(binding).state is LabSourceStageState.PENDING  # type: ignore[union-attr]


def test_same_owner_adopts_live_writer_lease_with_a_higher_fence_and_audit(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    old_writer = store.acquire_writer_lease(owner_id="scheduler-family", lease_seconds=30, now=NOW)
    binding = _binding(scheduler_fencing_token=old_writer.fencing_token)
    intent = _intent(binding)
    store.enqueue_external(binding, intent, lease=old_writer, now=NOW)
    reopened = LabSourceStageStore(
        store.path,
        queue_store_path=store.queue_store_path,
        manifest_keyring=authorization_authorities().authorization_keyring,
        authorization_keyring=authorization_authorities().authorization_keyring,
    )

    request_id = UUID("dddddddd-eeee-ffff-aaaa-bbbbbbbbbbbb")
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    first_scheduler_lease = jobs.acquire_scheduler_lease(
        owner_id="scheduler-family", lease_seconds=30, now=NOW
    )
    jobs.release_scheduler_lease(first_scheduler_lease, now=NOW + timedelta(milliseconds=1))
    current_scheduler_lease = jobs.acquire_scheduler_lease(
        owner_id="scheduler-family", lease_seconds=30, now=NOW + timedelta(seconds=1)
    )
    receipt = jobs.issue_current_scheduler_fence_receipt(
        lease=current_scheduler_lease,
        binding=binding,
        now=NOW + timedelta(seconds=1),
    )
    verifier = JobStoreSchedulerFenceVerifier(jobs)
    adopted = reopened.adopt_writer_lease(
        owner_id="scheduler-family",
        scheduler_fence_receipt=receipt,
        scheduler_fence_verifier=verifier,
        request_id=request_id,
        binding=binding,
        reason="scheduler_restart_recovery",
        lease_seconds=30,
        now=NOW + timedelta(seconds=1),
    )
    assert adopted.fencing_token == old_writer.fencing_token + 1
    assert adopted.lease_id != old_writer.lease_id
    assert (
        reopened.adopt_writer_lease(
            owner_id="scheduler-family",
            scheduler_fence_receipt=receipt,
            scheduler_fence_verifier=verifier,
            request_id=request_id,
            binding=binding,
            reason="scheduler_restart_recovery",
            lease_seconds=30,
            now=NOW + timedelta(seconds=1),
        )
        == adopted
    )
    with pytest.raises(LabSourceStageLeaseFencedError):
        reopened.begin_external(binding, intent, lease=old_writer, now=NOW + timedelta(seconds=1))
    pending = reopened.begin_external(
        binding,
        intent,
        lease=adopted,
        now=NOW + timedelta(seconds=1),
        scheduler_fence_receipt=receipt,
        scheduler_fence_verifier=verifier,
    )
    assert pending.state is LabSourceStageState.PENDING
    assert pending.writer_lease_id == adopted.lease_id
    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(
            executor.map(
                lambda lease: _begin_outcome(
                    reopened,
                    binding=binding,
                    intent=intent,
                    lease=lease,
                    now=NOW + timedelta(seconds=1),
                ),
                (old_writer, adopted),
            )
        )
    assert outcomes.count("fenced") == 1
    assert outcomes.count("pending") == 1
    audit = reopened.list_writer_lease_adoptions(binding)
    assert len(audit) == 1
    assert audit[0].old_writer_fence == old_writer.fencing_token
    assert audit[0].new_writer_fence == adopted.fencing_token
    assert str(old_writer.token) not in audit[0].model_dump_json()
    reopened.verify_audit_chain()

    with pytest.raises(LabSourceStageLeaseFencedError):
        reopened.adopt_writer_lease(
            owner_id="other-family",
            scheduler_fence_receipt=receipt,
            scheduler_fence_verifier=verifier,
            request_id=uuid4(),
            binding=binding,
            reason="scheduler_restart_recovery",
            lease_seconds=30,
            now=NOW + timedelta(seconds=1),
        )
    with pytest.raises(LabSourceStageLeaseFencedError):
        reopened.adopt_writer_lease(
            owner_id="other-family",
            scheduler_fence_receipt=receipt.model_copy(update={"owner_id": "other-family"}),
            scheduler_fence_verifier=verifier,
            request_id=uuid4(),
            binding=binding,
            reason="scheduler_restart_recovery",
            lease_seconds=30,
            now=NOW + timedelta(seconds=1),
        )


def test_source_stage_holds_job_store_fence_through_its_commit_before_takeover(
    tmp_path: Path,
) -> None:
    stage_store = _store(tmp_path)
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    old_scheduler_lease = jobs.acquire_scheduler_lease(
        owner_id="scheduler-a", lease_seconds=30, now=NOW
    )
    binding = _binding(scheduler_fencing_token=old_scheduler_lease.fencing_token)
    intent = _intent(binding)
    old_receipt = jobs.issue_current_scheduler_fence_receipt(
        lease=old_scheduler_lease,
        binding=binding,
        now=NOW,
    )
    writer = stage_store.acquire_writer_lease(
        owner_id="scheduler-family",
        lease_seconds=30,
        now=NOW,
        scheduler_fence_receipt=old_receipt,
        scheduler_fence_verifier=JobStoreSchedulerFenceVerifier(jobs),
        binding=binding,
    )
    stage_store.enqueue_external(
        binding,
        intent,
        lease=writer,
        now=NOW,
        scheduler_fence_receipt=old_receipt,
        scheduler_fence_verifier=JobStoreSchedulerFenceVerifier(jobs),
    )
    verified = Event()
    continue_commit = Event()

    @contextmanager
    def paused_hold_current(
        _receipt: CurrentSchedulerFenceReceipt,
        _binding: LabSourceStageBinding,
        _now: datetime,
    ) -> Iterator[None]:
        verified.set()
        assert continue_commit.wait(timeout=2)
        yield

    verifier = JobStoreSchedulerFenceVerifier(jobs)
    with (
        _test_only_scheduler_fence_hold_hook(paused_hold_current),
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        old_write = executor.submit(
            stage_store.begin_external,
            binding,
            intent,
            lease=writer,
            now=NOW + timedelta(seconds=1),
            scheduler_fence_receipt=old_receipt,
            scheduler_fence_verifier=verifier,
        )
        assert verified.wait(timeout=2)
        replacement = executor.submit(
            _replace_scheduler_lease,
            jobs,
            old_scheduler_lease,
            NOW + timedelta(seconds=1),
        )
        assert not replacement.done()
        continue_commit.set()
        old_record = old_write.result(timeout=2)
        replacement_lease = replacement.result(timeout=2)
        assert replacement_lease.fencing_token > old_scheduler_lease.fencing_token
        assert old_record.state is LabSourceStageState.PENDING

    stage = stage_store.get(binding)
    assert stage is not None and stage.state is LabSourceStageState.PENDING


def test_job_store_fence_verifier_rejects_forged_stale_and_replaced_authorities(
    tmp_path: Path,
) -> None:
    stage_store = _store(tmp_path)
    jobs_path = tmp_path / "jobs.sqlite3"
    jobs = LabJobStore(jobs_path)
    jobs.initialize()
    old_scheduler_lease = jobs.acquire_scheduler_lease(
        owner_id="scheduler-a", lease_seconds=30, now=NOW
    )
    binding = _binding(scheduler_fencing_token=old_scheduler_lease.fencing_token)
    receipt = jobs.issue_current_scheduler_fence_receipt(
        lease=old_scheduler_lease,
        binding=binding,
        now=NOW,
    )
    verifier = JobStoreSchedulerFenceVerifier(jobs)
    verifier.verify_current(receipt, binding=binding, now=NOW)

    for forged in (
        receipt.model_copy(
            update={"scheduler_fencing_token": receipt.scheduler_fencing_token + 100}
        ),
        receipt.model_copy(update={"scheduler_fencing_token": receipt.scheduler_fencing_token - 1}),
        receipt.model_copy(update={"owner_id": "scheduler-b"}),
        receipt.model_copy(update={"expires_at": NOW}),
    ):
        with pytest.raises(Exception, match="scheduler fence receipt"):
            verifier.verify_current(forged, binding=binding, now=NOW)

    writer = stage_store.acquire_writer_lease(
        owner_id="scheduler-family",
        lease_seconds=30,
        now=NOW,
        scheduler_fence_receipt=receipt,
        scheduler_fence_verifier=verifier,
        binding=binding,
    )
    stage_store.enqueue_external(
        binding,
        _intent(binding),
        lease=writer,
        now=NOW,
        scheduler_fence_receipt=receipt,
        scheduler_fence_verifier=verifier,
    )
    with pytest.raises(LabSourceStageLeaseFencedError, match="not greater"):
        stage_store.adopt_writer_lease(
            owner_id="scheduler-family",
            scheduler_fence_receipt=receipt,
            scheduler_fence_verifier=verifier,
            request_id=uuid4(),
            binding=binding,
            reason="scheduler_restart_recovery",
            lease_seconds=30,
            now=NOW,
        )
    jobs.release_scheduler_lease(old_scheduler_lease, now=NOW + timedelta(seconds=1))
    replacement_lease = jobs.acquire_scheduler_lease(
        owner_id="scheduler-b", lease_seconds=30, now=NOW + timedelta(seconds=1)
    )
    assert replacement_lease.fencing_token > old_scheduler_lease.fencing_token
    with pytest.raises(LabSourceStageLeaseFencedError, match="scheduler fence receipt"):
        stage_store.begin_external(
            binding,
            _intent(binding),
            lease=writer,
            now=NOW + timedelta(seconds=1),
            scheduler_fence_receipt=receipt,
            scheduler_fence_verifier=verifier,
        )

    _unlink_sqlite_family(jobs_path)
    replacement_store = LabJobStore(jobs_path)
    replacement_store.initialize()
    # ext4 hands the just-freed inode back, so a store recreated at the same
    # path can present the same (device, inode) generation the receipt recorded
    # and the verifier reaches its lease check first. Either way the fence must
    # refuse the receipt.
    with pytest.raises(
        Exception,
        match="scheduler fence receipt (authority changed|lease is missing)",
    ):
        JobStoreSchedulerFenceVerifier(replacement_store).verify_current(
            receipt,
            binding=binding,
            now=NOW,
        )


def _replace_scheduler_lease(
    store: LabJobStore,
    lease: LabLeaseRecord,
    now: datetime,
) -> LabLeaseRecord:
    store.release_scheduler_lease(lease, now=now)
    return store.acquire_scheduler_lease(owner_id="scheduler-b", lease_seconds=30, now=now)


def _begin_outcome(
    store: LabSourceStageStore,
    *,
    binding: LabSourceStageBinding,
    intent: SourceBrokerV2JobIntentEnvelope,
    lease: lab_source_stage.LabSourceStageWriterLease,
    now: datetime,
) -> str:
    try:
        return store.begin_external(binding, intent, lease=lease, now=now).state.value.lower()
    except LabSourceStageLeaseFencedError:
        return "fenced"


def test_source_stage_bind_has_no_caller_supplied_authority_and_rejects_exact_attempt_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    writer = store.acquire_writer_lease(owner_id="scheduler-a", lease_seconds=30, now=NOW)
    with _real_published_queue(tmp_path, monkeypatch) as (queue, binding, intent, runner):
        store.begin_external(binding, intent, lease=writer, now=NOW)
        outcome = queue.get_verified_published_outcome(intent.operation_id)
        fake = _FakePublishedOutcomeReader(outcome)

        class QueueSubclass(SourceBrokerV2SchedulerQueue):
            pass

        parameters = inspect.signature(store.bind_published_outcome).parameters
        assert {"queue", "reader", "queue_store_path", "outcome"}.isdisjoint(parameters)
        for rejected in (fake, runner, outcome, queue, QueueSubclass):
            with pytest.raises(TypeError, match="unexpected keyword argument 'queue'"):
                store.bind_published_outcome(
                    binding,
                    queue=rejected,  # type: ignore[call-arg]
                    lease=writer,
                    now=NOW,
                )

        other_binding = binding.model_copy(
            update={
                "claim_token": UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
                "attempt_id": UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
            }
        )
        with pytest.raises(LabSourceStageConflictError):
            store.begin_external(other_binding, intent, lease=writer, now=NOW)


@pytest.mark.parametrize(
    "method_name",
    (
        "__init__",
        "get_state",
        "get_verified_published_outcome",
        "_connection",
        "_require_stored_intent_binding",
        "_require_outcome_binding",
    ),
)
def test_source_stage_bind_rejects_queue_class_method_monkeypatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    store = _store(tmp_path)
    writer = store.acquire_writer_lease(owner_id="scheduler-a", lease_seconds=30, now=NOW)
    with _real_published_queue(tmp_path, monkeypatch) as (_queue, binding, intent, _runner):
        store.begin_external(binding, intent, lease=writer, now=NOW)

        monkeypatch.setattr(SourceBrokerV2SchedulerQueue, method_name, lambda *_args: object())
        with pytest.raises(LabSourceStageAuthorityError, match="implementation"):
            store.bind_published_outcome(binding, lease=writer, now=NOW)


def test_source_stage_restart_rejects_wrong_store_and_queue_config_tamper(
    tmp_path: Path,
) -> None:
    authorized = _queue_store_path(tmp_path, "authorized-runner")
    wrong = _queue_store_path(tmp_path, "wrong-runner")
    store = _store(tmp_path, queue_store_path=authorized)

    restarted = LabSourceStageStore(store.path, queue_store_path=authorized)
    assert restarted.queue_store_path == authorized.resolve(strict=True)
    with pytest.raises(LabSourceStageAuthorityError, match="authority"):
        LabSourceStageStore(store.path, queue_store_path=wrong)

    with sqlite3.connect(wrong) as connection:
        replacement = connection.execute(
            "SELECT schema_version, store_id, max_inbox, config_hash "
            "FROM source_broker_v2_store_config WHERE singleton = 1"
        ).fetchone()
    assert replacement is not None
    with sqlite3.connect(authorized) as connection:
        connection.execute("DROP TRIGGER source_broker_v2_store_config_no_update")
        connection.execute(
            "UPDATE source_broker_v2_store_config SET "
            "schema_version = ?, store_id = ?, max_inbox = ?, config_hash = ?",
            replacement,
        )
    with pytest.raises(LabSourceStageAuthorityError, match="authority"):
        LabSourceStageStore(store.path, queue_store_path=authorized)


def test_source_stage_rejects_symlink_retarget_and_internal_path_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorized = _queue_store_path(tmp_path, "authorized-runner")
    wrong = _queue_store_path(tmp_path, "wrong-runner")
    alias = tmp_path / "runner-authority.sqlite3"
    alias.symlink_to(authorized)
    store = _store(tmp_path, queue_store_path=alias)
    with pytest.raises(AttributeError):
        store.path = tmp_path / "replacement-stage.sqlite3"  # type: ignore[misc]
    writer = store.acquire_writer_lease(owner_id="scheduler-a", lease_seconds=30, now=NOW)
    with _real_published_queue(tmp_path, monkeypatch) as (_queue, binding, intent, _runner):
        store.begin_external(binding, intent, lease=writer, now=NOW)
        object.__setattr__(store, "_queue_path_input", wrong)
        with pytest.raises(LabSourceStageAuthorityError, match="authority"):
            store.bind_published_outcome(binding, lease=writer, now=NOW)

    alias.unlink()
    alias.symlink_to(wrong)
    with pytest.raises(LabSourceStageAuthorityError, match="authority"):
        LabSourceStageStore(store.path, queue_store_path=alias)


def test_source_stage_authority_restart_alias_copy_hash_and_serialization(
    tmp_path: Path,
) -> None:
    queue_store_path = _queue_store_path(tmp_path)
    store = _store(tmp_path, queue_store_path=queue_store_path)
    authority = store.authority

    restarted = LabSourceStageStore(store.path, queue_store_path=queue_store_path)
    assert restarted.authority == authority
    assert restarted.authority.authority_hash == authority.authority_hash

    alias = tmp_path / "source-stage-alias.sqlite3"
    alias.symlink_to(store.path)
    assert LabSourceStageStore(alias, queue_store_path=queue_store_path).authority == authority

    copied = tmp_path / "source-stage-copy.sqlite3"
    shutil.copy2(store.path, copied)
    with pytest.raises(LabSourceStageAuthorityError, match="authority"):
        LabSourceStageStore(copied, queue_store_path=queue_store_path)

    serialized = authority.model_dump_json()
    assert authority.canonical_stage_db_path == str(store.path.resolve(strict=True))
    assert authority.canonical_queue_db_path == str(queue_store_path.resolve(strict=True))
    for forbidden in ("credential", "private_key", "provider", "runtime", "client", "secret"):
        assert forbidden not in serialized.lower()


def test_source_stage_same_path_replacement_changes_identity_and_fences_old_store(
    tmp_path: Path,
) -> None:
    queue_store_path = _queue_store_path(tmp_path)
    store = _store(tmp_path, queue_store_path=queue_store_path)
    old_authority = store.authority
    stage_path = store.path

    _unlink_sqlite_family(stage_path)
    replacement = LabSourceStageStore(stage_path, queue_store_path=queue_store_path)
    assert replacement.authority.stage_store_id != old_authority.stage_store_id
    assert replacement.authority.authority_hash != old_authority.authority_hash
    with pytest.raises(LabSourceStageAuthorityError, match="authority"):
        _ = store.authority


def test_source_stage_queue_same_path_replacement_and_stage_id_tamper_fail_closed(
    tmp_path: Path,
) -> None:
    queue_store_path = _queue_store_path(tmp_path)
    store = _store(tmp_path, queue_store_path=queue_store_path)

    _unlink_sqlite_family(queue_store_path)
    initialize_source_broker_v2_job_storage(
        queue_store_path,
        busy_timeout_ms=2_000,
        max_inbox=100,
    )
    with pytest.raises(LabSourceStageAuthorityError, match="authority"):
        _ = store.authority
    with pytest.raises(LabSourceStageAuthorityError, match="authority"):
        LabSourceStageStore(store.path, queue_store_path=queue_store_path)

    clean = _store(tmp_path / "tamper", queue_store_path=queue_store_path)
    with sqlite3.connect(clean.path) as connection:
        connection.execute("DROP TRIGGER lab_source_stage_meta_no_update")
        connection.execute(
            "UPDATE lab_source_stage_meta SET stage_store_id = ? WHERE singleton = 1",
            (str(uuid4()),),
        )
    with pytest.raises(LabSourceStageAuthorityError, match="authority"):
        _ = clean.authority


def test_source_stage_ready_is_immutable_and_tamper_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    current = datetime.now(UTC)
    writer = store.acquire_writer_lease(owner_id="scheduler-a", lease_seconds=30, now=current)
    with _real_published_queue(
        tmp_path,
        monkeypatch,
        stage_store=store,
        stage_writer=writer,
    ) as (queue, binding, intent, _runner):
        store.begin_external(binding, intent, lease=writer, now=current)
        store.bind_published_outcome(
            binding,
            lease=writer,
            now=current,
        )

    with sqlite3.connect(store.path) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE lab_source_stage SET outcome_hash = ? WHERE attempt_id = ?",
            (HASH_7, str(binding.attempt_id)),
        )
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TRIGGER lab_source_stage_ready_immutable")
        connection.execute(
            "UPDATE lab_source_stage SET record_hash = ? WHERE attempt_id = ?",
            (HASH_7, str(binding.attempt_id)),
        )
    with pytest.raises(LabSourceStageIntegrityError):
        store.get(binding)


def test_source_stage_terminal_reason_is_code_only_and_no_secret_is_persisted(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    writer, binding, intent, _pending = _start_pending(store)

    with pytest.raises(ValueError):
        store.mark_failed(
            binding,
            code="source unavailable secret=do-not-store",
            lease=writer,
            now=NOW,
        )
    failed = store.mark_failed(
        binding,
        code="source_unavailable",
        lease=writer,
        now=NOW,
    )
    assert failed.state is LabSourceStageState.FAILED
    assert failed.terminal_reason == "source_unavailable"
    assert b"do-not-store" not in store.path.read_bytes()
    with pytest.raises(LabSourceStageTransitionError):
        store.begin_external(binding, intent, lease=writer, now=NOW)


def test_source_stage_concurrent_initialization_writer_cas_audit_and_v2_migration(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source-stage.sqlite3"
    queue_store_path = _queue_store_path(tmp_path)
    queue_authority = lab_source_stage._read_queue_authority(
        queue_store_path,
        busy_timeout_ms=2_000,
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE lab_source_stage_meta ("
            "singleton INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL, "
            "store_id TEXT NOT NULL, queue_db_path TEXT NOT NULL, "
            "queue_store_schema_version INTEGER NOT NULL, queue_store_id TEXT NOT NULL, "
            "queue_max_inbox INTEGER NOT NULL, queue_config_hash TEXT NOT NULL, "
            "queue_implementation_digest TEXT NOT NULL, queue_authority_digest TEXT NOT NULL, "
            "created_at TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO lab_source_stage_meta VALUES (1, 2, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                HASH_7,
                queue_authority.canonical_db_path,
                queue_authority.runner_schema_version,
                queue_authority.runner_store_id,
                queue_authority.runner_max_inbox,
                queue_authority.runner_config_hash,
                queue_authority.queue_implementation_digest,
                queue_authority.authority_digest,
                NOW.isoformat(),
            ),
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        stores = tuple(
            executor.map(
                lambda _: LabSourceStageStore(
                    path,
                    queue_store_path=queue_store_path,
                ),
                range(8),
            )
        )
    assert len(stores) == 8
    assert len({store.authority.authority_hash for store in stores}) == 1
    assert stores[0].authority.stage_store_id == stores[-1].authority.stage_store_id
    with sqlite3.connect(path) as connection:
        assert (
            connection.execute("PRAGMA user_version").fetchone()[0]
            == lab_source_stage._SCHEMA_VERSION
        )
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"

    def acquire(owner_id: str):
        try:
            return stores[0].acquire_writer_lease(
                owner_id=owner_id,
                lease_seconds=10,
                now=NOW,
            )
        except LabSourceStageLeaseFencedError:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        writers = tuple(executor.map(acquire, ("scheduler-a", "scheduler-b")))
    assert sum(writer is not None for writer in writers) == 1
    writer = next(writer for writer in writers if writer is not None)
    binding = _binding(scheduler_fencing_token=writer.fencing_token)
    intent = _intent(binding)
    stores[0].enqueue_external(binding, intent, lease=writer, now=NOW)
    stores[0].begin_external(binding, intent, lease=writer, now=NOW)
    events = stores[0].list_events(binding)
    assert [event.event_type for event in events] == ["queued", "pending"]
    stores[0].verify_audit_chain()


def test_source_stage_public_surface_has_no_executor_or_spool_capability(tmp_path: Path) -> None:
    store = _store(tmp_path)
    public_methods = {
        name for name in dir(store) if not name.startswith("_") and callable(getattr(store, name))
    }
    assert "claim_next" not in public_methods
    assert "publish_spool" not in public_methods
    schema = (
        sqlite3.connect(store.path)
        .execute("SELECT group_concat(sql, ' ') FROM sqlite_master WHERE sql IS NOT NULL")
        .fetchone()[0]
    )
    assert "credential" not in schema.lower()
    assert "private_key" not in schema.lower()
    assert "provider_client" not in schema.lower()

    queue_source = inspect.getsource(SourceBrokerV2SchedulerQueue)
    source_digest = lab_source_stage._normalized_source_digest(queue_source)
    assert source_digest == lab_source_stage._normalized_source_digest(
        queue_source.replace("\n", "\r\n")
    )
    assert source_digest != lab_source_stage._normalized_source_digest(
        queue_source.replace("if not self.db_path.is_file():", "if self.db_path.is_file():", 1)
    )
    assert (
        lab_source_stage._queue_source_implementation_digest(SourceBrokerV2SchedulerQueue)
        == lab_source_stage._QUEUE_IMPLEMENTATION_DIGEST
    )
