from __future__ import annotations

import inspect
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from loguru import logger

from rquant.current_claim_authority import PersistentCurrentClaimAuthority
from rquant.lab_claim_finalizer import (
    LabClaimFinalizer,
    LabClaimPublicationFinalizerAuthorityIssuer,
    LabClaimPublicationWorkerVerifier,
)
from rquant.lab_claim_finalizer_trust import (
    LabClaimFinalizerTrustCertificate,
    LabClaimFinalizerTrustVerifier,
    sign_lab_claim_finalizer_trust_certificate,
)
from rquant.lab_claim_publication import (
    LabClaimPublicationFinalizerRootKey,
    LabClaimSpoolReceiptVerifier,
)
from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabReportSpool,
    LabShardClaimV2,
    LabShardDefinition,
)
from rquant.lab_source_stage import LabSourceStageBinding, LabSourceStageState, LabSourceStageStore
from rquant.source_broker_v2_queue import SourceBrokerV2SchedulerQueue
from rquant.source_broker_v2_runner import initialize_source_broker_v2_job_storage

from . import test_source_broker_v2_runner as runner_test
from .source_broker_v2_authorized_intent_fixture import authorities, authorized_payload_and_claim
from .test_adapter_manifest import create_test_authorities
from .test_lab_scheduler import _spec
from .test_lab_worker import _worker

_FINALIZER_TRUST_BY_STORE: dict[
    str, tuple[LabClaimFinalizerTrustCertificate, LabClaimFinalizerTrustVerifier, object]
] = {}


def test_scheduler_module_has_no_finalizer_root_or_runtime_signer_surface() -> None:
    import rquant.lab_scheduler as lab_scheduler

    source = inspect.getsource(lab_scheduler)
    assert "LabClaimFinalizerTrustVerifier" not in source
    assert "Ed25519ContractSigner" not in source
    assert "LabClaimPublicationFinalizerAuthorityIssuer" not in source


def _finalizer_issuer(store: LabJobStore) -> LabClaimPublicationFinalizerAuthorityIssuer:
    root_key = LabClaimPublicationFinalizerRootKey(
        secret=b"scheduler-e2e-finalizer-root-key-0001",
    )
    key = str(store.path.resolve())
    material = _FINALIZER_TRUST_BY_STORE.get(key)
    if material is None:
        authorities = create_test_authorities(store.path.parent / "finalizer-trust")
        with store._connect() as connection:  # noqa: SLF001
            binding = store._finalizer_authority_binding(connection, path=store.path)  # noqa: SLF001
        unsigned = LabClaimFinalizerTrustCertificate(
            root_issuer=authorities.finalizer_trust_root.issuer,
            root_key_id=authorities.finalizer_trust_root.key_id,
            finalizer_issuer=authorities.finalizer_runtime.issuer,
            finalizer_key_id=authorities.finalizer_runtime.key_id,
            finalizer_public_key_fingerprint=authorities.finalizer_runtime.public_key_fingerprint,
            store_id=str(binding["store_id"]),
            database_device=binding["database_generation"][0],
            database_inode=binding["database_generation"][1],
            schema_version_bound=int(binding["schema_version"]),
            not_before=datetime(2020, 1, 1, tzinfo=UTC),
            expires_at=datetime(2030, 1, 1, tzinfo=UTC),
            signature="unsigned",
        )
        material = (
            sign_lab_claim_finalizer_trust_certificate(
                root_signer=authorities.finalizer_trust_root,
                certificate=unsigned,
            ),
            LabClaimFinalizerTrustVerifier(
                root_keyring=authorities.finalizer_trust_root_keyring,
                finalizer_keyring=authorities.finalizer_runtime_keyring,
            ),
            authorities.finalizer_runtime,
        )
        _FINALIZER_TRUST_BY_STORE[key] = material
    certificate, verifier, signer = material
    return LabClaimPublicationFinalizerAuthorityIssuer(
        store=store,
        root_key=root_key,
        trust_certificate=certificate,
        trust_verifier=verifier,
        runtime_signer=signer,  # type: ignore[arg-type]
    )


@dataclass
class _PendingSourceStage:
    current: datetime
    claim_spool: LabClaimSpool
    queue_path: Path
    scheduler: LabScheduler
    stage_store: LabSourceStageStore
    store: LabJobStore
    binding: LabSourceStageBinding
    claim_token: object


def _pending_source_stage(
    tmp_path: Path,
    *,
    scheduler_type: type[LabScheduler] = LabScheduler,
    v2_emit_permit: Callable[[str], object] | None = None,
    advance_source_stage: bool = True,
) -> _PendingSourceStage:
    current = datetime.now(UTC).replace(microsecond=0)
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    command_spool = LabCommandSpool(tmp_path / "commands")
    claim_spool = LabClaimSpool(tmp_path / "claims")
    queue_path = tmp_path / "source-broker-v2.sqlite3"
    initialize_source_broker_v2_job_storage(queue_path, busy_timeout_ms=2_000, max_inbox=8)
    public_keys = authorities().authorization_keyring
    stage_store = LabSourceStageStore(
        tmp_path / "source-stage.sqlite3",
        queue_store_path=queue_path,
        manifest_keyring=public_keys,
        authorization_keyring=public_keys,
    )
    queue = SourceBrokerV2SchedulerQueue(
        queue_path,
        manifest_keyring=public_keys,
        authorization_keyring=public_keys,
        stage_store=stage_store,
    )
    scheduler = scheduler_type(
        store=store,
        spool=command_spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        shard_lease_seconds=120,
        source_stage_store=stage_store,
        source_scheduler_queue=queue,
        source_manifest_keyring=public_keys,
        source_authorization_keyring=public_keys,
        source_wait_timeout_seconds=60,
        publication_timeout_seconds=90,
        source_stage_owner_id="scheduler-family-a",
        v2_emit_permit=v2_emit_permit,
        clock=lambda: current,
    )
    scheduler.run_once()
    assert scheduler.lease is not None
    envelope = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(
            job_id=uuid4(),
            spec=_spec(deadline=current + timedelta(minutes=5)),
            max_attempts=1,
        ),
    )
    assert store.apply_command(envelope, lease=scheduler.lease, now=current).status == "applied"
    _payload, source_claim = authorized_payload_and_claim(now=current, shard_index=0)
    store.plan_job(
        envelope.command.job_id,
        (source_claim.definition,),
        lease=scheduler.lease,
        now=current,
    )
    scheduler.run_once()
    shard = LabJobReader(store.path).list_shards(envelope.command.job_id)[0]
    assert shard.claim_token is not None
    publication = store.get_claim_publication(shard.claim_token)
    assert publication is not None
    if advance_source_stage:
        assert publication.status.value == "SOURCE_QUEUED"
    else:
        assert publication.status.value == "HELD_SOURCE"
    binding = LabSourceStageBinding(
        job_id=publication.identity.job_id,
        shard_id=publication.identity.shard_id,
        claim_token=publication.identity.claim_token,
        attempt_id=publication.identity.attempt_id,
        claim_generation=publication.identity.claim_generation,
        scheduler_fencing_token=publication.identity.scheduler_fencing_token,
        worker_id=publication.identity.worker_id,
        spec_hash=publication.identity.spec_hash,
        plan_hash=publication.identity.plan_hash,
    )
    if advance_source_stage:
        stage = stage_store.get(binding)
        assert stage is not None and stage.state is LabSourceStageState.PENDING
    return _PendingSourceStage(
        current=current,
        claim_spool=claim_spool,
        queue_path=queue_path,
        scheduler=scheduler,
        stage_store=stage_store,
        store=store,
        binding=binding,
        claim_token=shard.claim_token,
    )


def _advance_rollout_to_scheduler_emit(path: Path) -> object:
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutPhase, FinalizerRolloutStore

    rollout = FinalizerRolloutStore(path)
    for phase, evidence in (
        (FinalizerRolloutPhase.MATERIAL_INSTALLED, "install:a"),
        (FinalizerRolloutPhase.PREFLIGHT_OK, "preflight:a"),
        (FinalizerRolloutPhase.FINALIZER_READY, "ready:a"),
        (FinalizerRolloutPhase.V2_WORKERS_READY, "workers:a"),
        (FinalizerRolloutPhase.SCHEDULER_EMITS_V2, "scheduler:a"),
    ):
        rollout.transition(phase, evidence=evidence)
    return rollout


def test_v2_emit_permit_blocks_drain_until_source_queue_and_pending_are_durable(
    tmp_path: Path,
) -> None:
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutPhase

    staged = threading.Event()
    release = threading.Event()
    rollout = _advance_rollout_to_scheduler_emit(tmp_path / "rollout.sqlite3")

    class _BarrierScheduler(LabScheduler):
        def _source_stage_writer(self, **_kwargs: object) -> None:
            return None

        @staticmethod
        def _after_source_stage_queued(_record: object) -> None:
            staged.set()
            assert release.wait(timeout=5)

    pending = _pending_source_stage(
        tmp_path,
        scheduler_type=_BarrierScheduler,
        v2_emit_permit=lambda holder: rollout.emit_permit(holder=holder, timeout_seconds=2),  # type: ignore[union-attr]
        advance_source_stage=False,
    )
    pending.scheduler._source_stage_writer = LabScheduler._source_stage_writer.__get__(  # type: ignore[method-assign]  # noqa: SLF001
        pending.scheduler, LabScheduler
    )
    emitted = threading.Event()
    emit_errors: list[BaseException] = []

    def emit() -> None:
        try:
            pending.scheduler.run_once()
        except BaseException as exc:  # pragma: no cover - assertion reports below
            emit_errors.append(exc)
        finally:
            emitted.set()

    emitter = threading.Thread(target=emit)
    emitter.start()
    assert staged.wait(timeout=5)
    drained = threading.Event()

    def drain() -> None:
        rollout.begin_rollback(evidence="drain:barrier")  # type: ignore[union-attr]
        drained.set()

    drainer = threading.Thread(target=drain)
    drainer.start()
    time.sleep(0.15)
    assert not emitted.is_set()
    assert not drained.is_set()
    release.set()
    emitter.join(timeout=5)
    drainer.join(timeout=5)
    assert not emitter.is_alive() and not drainer.is_alive()
    assert not emit_errors
    assert drained.is_set()
    assert rollout.snapshot().phase is FinalizerRolloutPhase.DRAINING  # type: ignore[union-attr]
    stage = pending.stage_store.get(pending.binding)
    assert stage is not None and stage.state is LabSourceStageState.PENDING
    with sqlite3.connect(pending.queue_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone() == (1,)

    next_job = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(
            job_id=uuid4(),
            spec=_spec(deadline=pending.current + timedelta(minutes=5)),
            max_attempts=1,
        ),
    )
    assert (
        pending.store.apply_command(
            next_job, lease=pending.scheduler.lease, now=pending.current
        ).status
        == "applied"
    )
    _payload, source_claim = authorized_payload_and_claim(now=pending.current, shard_index=0)
    pending.store.plan_job(
        next_job.command.job_id,
        (source_claim.definition,),
        lease=pending.scheduler.lease,
        now=pending.current,
    )
    pending.scheduler.run_once()
    next_shard = LabJobReader(pending.store.path).list_shards(next_job.command.job_id)[0]
    assert next_shard.claim_token is None
    with sqlite3.connect(pending.queue_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone() == (1,)


def test_v2_scheduler_has_no_worker_or_spool_authority(tmp_path: Path) -> None:
    pending = _pending_source_stage(tmp_path)

    assert pending.scheduler.claim_spool is None
    assert pending.scheduler.claim_worker_ids == ()
    source = inspect.getsource(LabScheduler.run_once)
    v2_branch = source.split("if self._source_stage_enabled:", 1)[1].split(
        "elif self.claim_spool", 1
    )[0]
    assert "claim_next_source_stage" in v2_branch
    assert "claim_next_shard" not in v2_branch
    assert ".publish(" not in v2_branch


def test_every_v2_queue_write_is_fenced_by_the_emit_permit_saga() -> None:
    scheduler_source = inspect.getsource(LabScheduler)
    candidate_source = inspect.getsource(LabScheduler._emit_v2_source_candidate)
    queued_recovery_source = inspect.getsource(LabScheduler._advance_source_queued)

    assert scheduler_source.count("enqueue_external(") == 1
    assert scheduler_source.count("enqueue_intent_bytes(") == 1
    assert "with self._v2_emit_permit(record)" in candidate_source
    assert candidate_source.index("with self._v2_emit_permit(record)") < candidate_source.index(
        "enqueue_external("
    )
    assert "if not permit_held:" in queued_recovery_source
    assert "with self._v2_emit_permit(record)" in queued_recovery_source
    assert queued_recovery_source.index("with self._v2_emit_permit(record)") < (
        queued_recovery_source.index("enqueue_intent_bytes(")
    )


def test_scheduler_observes_failed_source_stage_without_enqueuing_or_publishing_worker_claim(
    tmp_path: Path,
) -> None:
    pending = _pending_source_stage(tmp_path)
    writer = pending.scheduler._source_stage_lease  # noqa: SLF001
    assert writer is not None
    failed = pending.stage_store.mark_failed(
        pending.binding,
        code="source_unavailable",
        lease=writer,
        now=pending.current,
        scheduler_fence_receipt=pending.scheduler._source_stage_receipt(  # noqa: SLF001
            lease=pending.scheduler.lease,
            binding=pending.binding,
            now=pending.current,
        ),
        scheduler_fence_verifier=pending.scheduler._source_stage_fence_verifier,  # noqa: SLF001
    )
    assert failed.state is LabSourceStageState.FAILED

    first = pending.scheduler.run_once()
    second = pending.scheduler.run_once()

    publication = pending.store.get_claim_publication(pending.claim_token)  # type: ignore[arg-type]
    assert publication is not None and publication.status.value == "SOURCE_QUEUED"
    assert pending.stage_store.get(pending.binding) == failed
    assert first.source_stage_failed == second.source_stage_failed == 1
    assert failed.terminal_reason == "source_unavailable"
    assert pending.claim_spool.pending() == ()
    with sqlite3.connect(pending.queue_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone() == (1,)


def test_scheduler_observes_reconcile_required_source_stage_without_new_side_effects(
    tmp_path: Path,
) -> None:
    pending = _pending_source_stage(tmp_path)
    writer = pending.scheduler._source_stage_lease  # noqa: SLF001
    assert writer is not None
    reconciled = pending.stage_store.mark_reconcile_required(
        pending.binding,
        code="source_queue_uncertain",
        lease=writer,
        now=pending.current,
        scheduler_fence_receipt=pending.scheduler._source_stage_receipt(  # noqa: SLF001
            lease=pending.scheduler.lease,
            binding=pending.binding,
            now=pending.current,
        ),
        scheduler_fence_verifier=pending.scheduler._source_stage_fence_verifier,  # noqa: SLF001
    )
    assert reconciled.state is LabSourceStageState.RECONCILE_REQUIRED
    with sqlite3.connect(pending.queue_path) as connection:
        before = connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone()

    first = pending.scheduler.run_once()
    second = pending.scheduler.run_once()

    with sqlite3.connect(pending.queue_path) as connection:
        after = connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone()
    assert first.source_stage_reconcile_required == second.source_stage_reconcile_required == 1
    assert pending.stage_store.get(pending.binding) == reconciled
    assert pending.store.get_claim_publication(pending.claim_token) is not None  # type: ignore[arg-type]
    assert before == after == (1,)
    assert pending.claim_spool.pending() == ()


def test_scheduler_reports_redacted_stable_reason_after_source_stage_root_replacement(
    tmp_path: Path,
) -> None:
    pending = _pending_source_stage(tmp_path)
    stage_path = pending.stage_store.path
    for candidate in (stage_path, Path(f"{stage_path}-wal"), Path(f"{stage_path}-shm")):
        candidate.unlink(missing_ok=True)
    LabSourceStageStore(
        stage_path,
        queue_store_path=pending.queue_path,
        manifest_keyring=authorities().authorization_keyring,
        authorization_keyring=authorities().authorization_keyring,
    )
    records: list[dict[str, object]] = []
    sink_id = logger.add(lambda message: records.append(message.record), level="WARNING")
    try:
        tick = pending.scheduler.run_once()
    finally:
        logger.remove(sink_id)

    assert tick.source_stage_reconcile_required == 1
    matched = [
        record
        for record in records
        if record["extra"].get("failure") == "source_stage_reconcile_required"
    ]
    assert len(matched) == 1
    context = matched[0]["extra"]
    assert context["reason"] == "source_stage_missing"
    assert str(stage_path) not in str(matched[0]["message"])
    writer = pending.scheduler._source_stage_lease  # noqa: SLF001
    assert writer is not None
    assert str(writer.token) not in str(matched[0])


def test_scheduler_observes_runner_ready_stage_without_publishing_worker_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = datetime.now(UTC).replace(microsecond=0)
    monkeypatch.setenv("RQUANT_SOURCE_TOKEN", "source-secret")
    security = runner_test._RunnerSourceAuthoritySecurity()
    runner = None
    worker = None
    try:
        transport = runner_test._RunnerTestTransport(security)
        queue_root = tmp_path / "runner"
        queue_path = queue_root / "runner.sqlite3"
        initialize_source_broker_v2_job_storage(queue_path, busy_timeout_ms=2_000, max_inbox=100)
        public_keys = authorities().authorization_keyring
        stage_store = LabSourceStageStore(
            tmp_path / "source-stage.sqlite3",
            queue_store_path=queue_path,
            manifest_keyring=public_keys,
            authorization_keyring=public_keys,
        )
        runner = runner_test._runner(queue_root, transport, stage_store=stage_store)
        queue = SourceBrokerV2SchedulerQueue(
            queue_path,
            manifest_keyring=public_keys,
            authorization_keyring=public_keys,
            stage_store=stage_store,
        )
        store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
        store.initialize()
        claim_spool = LabClaimSpool(
            tmp_path / "claims",
            publish_receipt_publisher=runner_test._authority(
                "finalizer", source_transport=transport
            ),
        )
        scheduler = LabScheduler(
            store=store,
            spool=LabCommandSpool(tmp_path / "commands"),
            owner_id="scheduler-a",
            lease_seconds=60,
            heartbeat_seconds=10,
            poll_interval_ms=10,
            shard_lease_seconds=120,
            source_stage_store=stage_store,
            source_scheduler_queue=queue,
            source_manifest_keyring=public_keys,
            source_authorization_keyring=public_keys,
            source_wait_timeout_seconds=60,
            publication_timeout_seconds=90,
            source_stage_owner_id="scheduler-family-a",
            clock=lambda: current,
        )
        scheduler.run_once()
        assert scheduler.lease is not None
        envelope = LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=uuid4(),
                spec=_spec(deadline=current + timedelta(minutes=5)),
                max_attempts=1,
            ),
        )
        assert store.apply_command(envelope, lease=scheduler.lease, now=current).status == "applied"
        _payload, source_claim = authorized_payload_and_claim(
            now=current,
            shard_index=0,
            source_authority=runner_test._authority("source", source_transport=transport),
        )
        store.plan_job(
            envelope.command.job_id,
            (source_claim.definition,),
            lease=scheduler.lease,
            now=current,
        )
        scheduler.run_once()
        shard = LabJobReader(store.path).list_shards(envelope.command.job_id)[0]
        assert shard.claim_token is not None
        publication = store.get_claim_publication(shard.claim_token)
        assert publication is not None and publication.status.value == "SOURCE_QUEUED"
        binding = LabSourceStageBinding(
            job_id=publication.identity.job_id,
            shard_id=publication.identity.shard_id,
            claim_token=publication.identity.claim_token,
            attempt_id=publication.identity.attempt_id,
            claim_generation=publication.identity.claim_generation,
            scheduler_fencing_token=publication.identity.scheduler_fencing_token,
            worker_id=publication.identity.worker_id,
            spec_hash=publication.identity.spec_hash,
            plan_hash=publication.identity.plan_hash,
        )
        assert runner.run_once() == 1
        writer = scheduler._source_stage_lease  # noqa: SLF001
        assert writer is not None
        ready = stage_store.bind_published_outcome(
            binding,
            lease=writer,
            now=current,
            scheduler_fence_receipt=scheduler._source_stage_receipt(  # noqa: SLF001
                lease=scheduler.lease,
                binding=binding,
                now=current,
            ),
            scheduler_fence_verifier=scheduler._source_stage_fence_verifier,  # noqa: SLF001
        )
        assert ready.state is LabSourceStageState.READY

        preimage = LabShardClaimV2.model_validate_json(publication.claim_preimage_bytes)
        current_authority = PersistentCurrentClaimAuthority(
            tmp_path / "current-claim-authority.sqlite3",
            authority_id="scheduler-e2e-current-claim-authority",
            signer=authorities().plan_v2,
            keyring=public_keys,
            mode="test-standalone",
        )
        current_authority.replace_current(preimage)
        finalizer = LabClaimFinalizer(
            ledger=store,
            stage_reader=stage_store,
            authority=_finalizer_issuer(store).acquire(
                owner_id="scheduler-e2e-finalizer", lease_seconds=60, now=current
            ),
            current_claim_authority=current_authority,
            keyring=public_keys,
            audience="lab-claim-publication",
            spool=claim_spool,
            spool_receipt_verifier=LabClaimSpoolReceiptVerifier.from_spool(claim_spool),
            clock=lambda: current,
        )
        assert finalizer.finalize(publication.identity).status == "published"
        published = store.get_claim_publication(shard.claim_token)
        assert published is not None and published.status.value == "PUBLISHED"
        worker = _worker(
            tmp_path,
            worker_id="worker-a",
            claims=claim_spool,
            reports=LabReportSpool(tmp_path / "e2e-reports"),
            claim_publication_verifier=LabClaimPublicationWorkerVerifier(
                ledger=store,
                current_claim_authority=current_authority,
                keyring=public_keys,
                audience="lab-claim-publication",
                spool_receipt_verifier=LabClaimSpoolReceiptVerifier.from_spool(claim_spool),
                trust_verifier=_finalizer_issuer(store)._trust_verifier,  # noqa: SLF001
            ),
            v2_claim_publication_enabled=True,
            clock=lambda: current,
        )
        entry = claim_spool.pending()[0]
        assert worker._consume_selected_claim(entry) is not None  # noqa: SLF001
        assert claim_spool.pending() == ()

        first = scheduler.run_once()
        second = scheduler.run_once()

        assert store.get_claim_publication(shard.claim_token) == published
        assert first.source_stage_ready == second.source_stage_ready == 0
        with sqlite3.connect(queue_path) as connection:
            count = connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone()
        assert count == (1,)
    finally:
        if worker is not None:
            worker.close()
        if runner is not None:
            runner.close()
        security.close()


def test_scheduler_reopens_held_source_after_crash_without_changing_attempt_identity(
    tmp_path: Path,
) -> None:
    current = datetime.now(UTC).replace(microsecond=0)
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    command_spool = LabCommandSpool(tmp_path / "commands")
    claim_spool = LabClaimSpool(tmp_path / "claims")
    queue_path = tmp_path / "source-broker-v2.sqlite3"
    initialize_source_broker_v2_job_storage(queue_path, busy_timeout_ms=2_000, max_inbox=8)
    public_keys = authorities().authorization_keyring
    stage_store = LabSourceStageStore(
        tmp_path / "source-stage.sqlite3",
        queue_store_path=queue_path,
        manifest_keyring=public_keys,
        authorization_keyring=public_keys,
    )
    queue = SourceBrokerV2SchedulerQueue(
        queue_path,
        manifest_keyring=public_keys,
        authorization_keyring=public_keys,
        stage_store=stage_store,
    )
    scheduler_args = dict(
        store=store,
        spool=command_spool,
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        shard_lease_seconds=120,
        source_stage_store=stage_store,
        source_scheduler_queue=queue,
        source_manifest_keyring=public_keys,
        source_authorization_keyring=public_keys,
        source_wait_timeout_seconds=60,
        publication_timeout_seconds=90,
        source_stage_owner_id="scheduler-family-a",
        clock=lambda: current,
    )

    class _CrashAfterHeldScheduler(LabScheduler):
        @staticmethod
        def _after_source_claim_held(_record: object) -> None:
            raise RuntimeError("crash after HELD_SOURCE")

    scheduler = _CrashAfterHeldScheduler(owner_id="scheduler-a", **scheduler_args)
    scheduler.run_once()
    assert scheduler.lease is not None
    envelope = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(
            job_id=uuid4(),
            spec=_spec(deadline=current + timedelta(minutes=5)),
            max_attempts=1,
        ),
    )
    assert store.apply_command(envelope, lease=scheduler.lease, now=current).status == "applied"
    _payload, source_claim = authorized_payload_and_claim(now=current, shard_index=0)
    store.plan_job(
        envelope.command.job_id,
        (source_claim.definition,),
        lease=scheduler.lease,
        now=current,
    )

    with pytest.raises(RuntimeError, match="crash after HELD_SOURCE"):
        scheduler.run_once()

    shard = LabJobReader(store.path).list_shards(envelope.command.job_id)[0]
    assert shard.claim_token is not None
    held = store.get_claim_publication(shard.claim_token)
    assert held is not None and held.status.value == "HELD_SOURCE"
    identity = held.identity
    scheduler.release()

    replacement = LabScheduler(owner_id="scheduler-b", **scheduler_args)
    tick = replacement.run_once()
    reopened = store.get_claim_publication(identity.claim_token)
    assert reopened is not None and reopened.status.value == "SOURCE_QUEUED"
    assert reopened.identity == identity
    binding = LabSourceStageBinding(
        job_id=identity.job_id,
        shard_id=identity.shard_id,
        claim_token=identity.claim_token,
        attempt_id=identity.attempt_id,
        claim_generation=identity.claim_generation,
        scheduler_fencing_token=identity.scheduler_fencing_token,
        worker_id=identity.worker_id,
        spec_hash=identity.spec_hash,
        plan_hash=identity.plan_hash,
    )
    stage = stage_store.get(binding)
    assert stage is not None and stage.state is LabSourceStageState.PENDING
    assert tick.source_stage_pending == 1
    with sqlite3.connect(queue_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone() == (1,)
    assert claim_spool.pending() == ()


def test_scheduler_advances_a_v2_claim_to_pending_without_publishing_worker_claim(
    tmp_path: Path,
) -> None:
    current = datetime.now(UTC).replace(microsecond=0)
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    command_spool = LabCommandSpool(tmp_path / "commands")
    claim_spool = LabClaimSpool(tmp_path / "claims")
    queue_path = tmp_path / "source-broker-v2.sqlite3"
    initialize_source_broker_v2_job_storage(queue_path, busy_timeout_ms=2_000, max_inbox=8)
    public_keys = authorities().authorization_keyring
    stage_store = LabSourceStageStore(
        tmp_path / "source-stage.sqlite3",
        queue_store_path=queue_path,
        manifest_keyring=public_keys,
        authorization_keyring=public_keys,
    )
    queue = SourceBrokerV2SchedulerQueue(
        queue_path,
        manifest_keyring=public_keys,
        authorization_keyring=public_keys,
        stage_store=stage_store,
    )
    envelope = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(
            job_id=uuid4(),
            spec=_spec(deadline=current + timedelta(minutes=5)),
            max_attempts=1,
        ),
    )
    scheduler = LabScheduler(
        store=store,
        spool=command_spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        shard_lease_seconds=120,
        source_stage_store=stage_store,
        source_scheduler_queue=queue,
        source_manifest_keyring=public_keys,
        source_authorization_keyring=public_keys,
        source_wait_timeout_seconds=60,
        publication_timeout_seconds=90,
        source_stage_owner_id="scheduler-family-a",
        clock=lambda: current,
    )

    scheduler.run_once()
    assert scheduler.lease is not None
    assert store.apply_command(envelope, lease=scheduler.lease, now=current).status == "applied"
    _payload, source_claim = authorized_payload_and_claim(now=current, shard_index=0)
    store.plan_job(
        envelope.command.job_id,
        (source_claim.definition,),
        lease=scheduler.lease,
        now=current,
    )

    tick = scheduler.run_once()

    shards = LabJobReader(store.path).list_shards(envelope.command.job_id)
    assert len(shards) == 1
    assert shards[0].claim_token is not None
    publication = store.get_claim_publication(shards[0].claim_token)
    assert publication is not None
    assert publication.status.value == "SOURCE_QUEUED"
    binding = LabSourceStageBinding(
        job_id=publication.identity.job_id,
        shard_id=publication.identity.shard_id,
        claim_token=publication.identity.claim_token,
        attempt_id=publication.identity.attempt_id,
        claim_generation=publication.identity.claim_generation,
        scheduler_fencing_token=publication.identity.scheduler_fencing_token,
        worker_id=publication.identity.worker_id,
        spec_hash=publication.identity.spec_hash,
        plan_hash=publication.identity.plan_hash,
    )
    stage = stage_store.get(binding)
    assert stage is not None and stage.state is LabSourceStageState.PENDING
    assert queue.get_state(publication.source_operation_id or "") == "NEW"
    assert tick.source_stage_pending == 1
    assert claim_spool.pending() == ()


@pytest.mark.parametrize("crash_boundary", ("queued", "queue_enqueued", "pending"))
def test_scheduler_replays_source_enqueue_once_after_crash_before_pending(
    tmp_path: Path,
    crash_boundary: str,
) -> None:
    current = datetime.now(UTC).replace(microsecond=0)
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    command_spool = LabCommandSpool(tmp_path / "commands")
    queue_path = tmp_path / "source-broker-v2.sqlite3"
    initialize_source_broker_v2_job_storage(queue_path, busy_timeout_ms=2_000, max_inbox=8)
    public_keys = authorities().authorization_keyring
    stage_store = LabSourceStageStore(
        tmp_path / "source-stage.sqlite3",
        queue_store_path=queue_path,
        manifest_keyring=public_keys,
        authorization_keyring=public_keys,
    )
    queue = SourceBrokerV2SchedulerQueue(
        queue_path,
        manifest_keyring=public_keys,
        authorization_keyring=public_keys,
        stage_store=stage_store,
    )
    scheduler_args = dict(
        store=store,
        spool=command_spool,
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        shard_lease_seconds=120,
        source_stage_store=stage_store,
        source_scheduler_queue=queue,
        source_manifest_keyring=public_keys,
        source_authorization_keyring=public_keys,
        source_wait_timeout_seconds=60,
        publication_timeout_seconds=90,
        source_stage_owner_id="scheduler-family-a",
        clock=lambda: current,
    )

    class _CrashAfterQueuedScheduler(LabScheduler):
        @staticmethod
        def _after_source_stage_queued(_record: object) -> None:
            if crash_boundary == "queued":
                raise RuntimeError("crash after durable queued evidence")

        @staticmethod
        def _after_source_queue_enqueued(_record: object) -> None:
            if crash_boundary == "queue_enqueued":
                raise RuntimeError("crash after queue enqueue")

        @staticmethod
        def _after_source_stage_pending(_record: object) -> None:
            if crash_boundary == "pending":
                raise RuntimeError("crash after durable pending evidence")

    scheduler = _CrashAfterQueuedScheduler(owner_id="scheduler-a", **scheduler_args)
    scheduler.run_once()
    assert scheduler.lease is not None
    envelope = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(
            job_id=uuid4(),
            spec=_spec(deadline=current + timedelta(minutes=5)),
            max_attempts=1,
        ),
    )
    assert store.apply_command(envelope, lease=scheduler.lease, now=current).status == "applied"
    _payload, source_claim = authorized_payload_and_claim(now=current, shard_index=0)
    store.plan_job(
        envelope.command.job_id,
        (source_claim.definition,),
        lease=scheduler.lease,
        now=current,
    )
    with pytest.raises(RuntimeError, match="crash after"):
        scheduler.run_once()
    shard = LabJobReader(store.path).list_shards(envelope.command.job_id)[0]
    assert shard.claim_token is not None
    publication = store.get_claim_publication(shard.claim_token)
    assert publication is not None and publication.status.value == "SOURCE_QUEUED"
    scheduler.release()

    replacement = LabScheduler(owner_id="scheduler-b", **scheduler_args)
    tick = replacement.run_once()
    assert replacement.lease is not None
    assert replacement.lease.fencing_token > publication.identity.scheduler_fencing_token
    binding = LabSourceStageBinding(
        job_id=publication.identity.job_id,
        shard_id=publication.identity.shard_id,
        claim_token=publication.identity.claim_token,
        attempt_id=publication.identity.attempt_id,
        claim_generation=publication.identity.claim_generation,
        scheduler_fencing_token=publication.identity.scheduler_fencing_token,
        worker_id=publication.identity.worker_id,
        spec_hash=publication.identity.spec_hash,
        plan_hash=publication.identity.plan_hash,
    )
    stage = stage_store.get(binding)
    assert stage is not None and stage.state is LabSourceStageState.PENDING
    assert stage.writer_fencing_token == replacement.lease.fencing_token
    assert len(stage_store.list_writer_lease_adoptions(binding)) == 1
    assert tick.source_stage_pending == 1
    assert queue.get_state(publication.source_operation_id or "") == "NEW"
    with sqlite3.connect(queue_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM source_broker_v2_jobs").fetchone() == (1,)


def test_missing_source_dependencies_skip_v2_and_continue_with_v1(tmp_path: Path) -> None:
    current = datetime.now(UTC).replace(microsecond=0)
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    command_spool = LabCommandSpool(tmp_path / "commands")
    claim_spool = LabClaimSpool(tmp_path / "claims")
    scheduler = LabScheduler(
        store=store,
        spool=command_spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        claim_spool=claim_spool,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=120,
        clock=lambda: current,
    )
    scheduler.run_once()
    assert scheduler.lease is not None
    v2_job = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(
            job_id=uuid4(),
            spec=_spec(deadline=current + timedelta(minutes=5)),
            max_attempts=1,
        ),
    )
    v1_job = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(
            job_id=uuid4(),
            spec=_spec(deadline=current + timedelta(minutes=5)),
            max_attempts=1,
        ),
    )
    assert store.apply_command(v2_job, lease=scheduler.lease, now=current).status == "applied"
    assert store.apply_command(v1_job, lease=scheduler.lease, now=current).status == "applied"
    _payload, v2_claim = authorized_payload_and_claim(now=current, shard_index=0)
    v1_definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id="research.local",
        adapter_version="1.0.0",
        plan_hash="f" * 64,
        payload_json='{"partition":"v1"}',
    )
    store.plan_job(
        v2_job.command.job_id,
        (v2_claim.definition,),
        lease=scheduler.lease,
        now=current,
    )
    store.plan_job(v1_job.command.job_id, (v1_definition,), lease=scheduler.lease, now=current)

    tick = scheduler.run_once()

    v2_shard = LabJobReader(store.path).list_shards(v2_job.command.job_id)[0]
    v1_shard = LabJobReader(store.path).list_shards(v1_job.command.job_id)[0]
    assert v2_shard.claim_token is None
    assert v1_shard.claim_token is not None
    assert tick.claims_blocked_by_source_stage == 0
    assert len(claim_spool.pending()) == 1
