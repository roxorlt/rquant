from __future__ import annotations

import hashlib
import inspect
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier, Event, Thread
from typing import get_args
from unittest.mock import patch
from uuid import uuid4

import pytest
from pydantic import ValidationError

import rquant.lab_claim_finalizer as lab_claim_finalizer
import rquant.lab_jobs as lab_jobs
import rquant.lab_source_stage as lab_source_stage
from rquant.current_claim_authority import PersistentCurrentClaimAuthority
from rquant.lab_claim_finalizer import (
    LabClaimFinalizer,
    LabClaimFinalizerError,
    LabClaimPublicationFinalizerAuthorityIssuer,
    LabClaimPublicationWorkerVerifier,
)
from rquant.lab_claim_finalizer_trust import (
    LabClaimFinalizerTrustCertificate,
    LabClaimFinalizerTrustVerifier,
    sign_lab_claim_finalizer_trust_certificate,
)
from rquant.lab_claim_publication import (
    ClaimPublicationAuditAction,
    ClaimPublicationStatus,
    HeldDraft,
    LabClaimPublicationFinalizerRootKey,
    LabClaimPublicationIdentity,
    LabClaimPublicationMutation,
    LabClaimPublicationRecord,
    LabClaimSpoolPublishReceiptV2,
    LabClaimSpoolReceiptVerifier,
    PublishReceipt,
    QueueBinding,
    ReadyBinding,
)
from rquant.lab_jobs import (
    ClaimPublicationConflictError,
    InvalidClaimPublicationTransitionError,
    LabJobReader,
    LabJobStore,
    LabLeaseRecord,
    SchedulerLeaseFencedError,
)
from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabReportReceipt,
    LabReportSpool,
    LabShardClaim,
    LabShardClaimV2,
    LabShardDefinition,
    LabShardWorkPlan,
)
from rquant.lab_source_stage import (
    LabSourceStageBinding,
    LabSourceStageState,
    LabSourceStageStore,
    LabSourceStageStoreAuthority,
    LabSourceStageWriterLease,
)
from rquant.research_run_spec import ResearchRunSpec
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2AuthorityRef,
    SourceBrokerV2JobIntentEnvelope,
    SourceBrokerV2JobOutcomeStatus,
    SourceBrokerV2NativeEvidence,
    build_verified_job_outcome,
    canonical_job_model_bytes,
    canonical_request_bytes,
)
from rquant.source_broker_v2_runner import SourceBrokerV2JobRunnerState
from rquant.source_operation_contracts import (
    CurrentClaimPlanIssueV2,
    SourceOperationContractError,
    SourceUsePlanV2,
    build_source_broker_v2_scheduler_intent,
    require_source_use_plan_v2,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)
from tests.unit.test_adapter_manifest import (
    ADAPTER_MANIFEST_NAMESPACE,
    Authorities,
    create_test_authorities,
    signed_manifest,
)
from tests.unit.test_lab_jobs import NOW as LAB_JOBS_NOW
from tests.unit.test_lab_jobs import _lease, _store, _submit, _submit_job
from tests.unit.test_lab_worker import RecordingRegistry, _worker
from tests.unit.test_strategy_job_adapters import _claim as _strategy_claim
from tests.unit.test_strategy_job_adapters import _nshape_compare_spec

from .source_broker_v2_authorized_intent_fixture import authorized_payload_and_claim

NOW = LAB_JOBS_NOW
_AUTHORITIES_BY_PAYLOAD_HASH: dict[str, Authorities] = {}
_FINALIZER_TRUST_BY_STORE: dict[
    str, tuple[LabClaimFinalizerTrustCertificate, LabClaimFinalizerTrustVerifier, object]
] = {}


# These joins are watchdogs, not subjects: the property under test is that a
# finalizer thread terminates, not that it terminates within five seconds. A
# tight cap turns a slow runner into a false "left a finalizer thread alive",
# which is exactly what happened on x64 CI; a generous one still fails a thread
# that never finishes.
_FINALIZER_JOIN_WATCHDOG_SECONDS = 60


def _finalizer_root_key() -> LabClaimPublicationFinalizerRootKey:
    return LabClaimPublicationFinalizerRootKey(
        secret=b"test-lab-claim-finalizer-root-key-0001",
    )


def _test_only_preseed_finalizer_root_anchor(store: LabJobStore) -> None:
    """Fixture-only offline bootstrap; runtime has no matching API."""

    root = _finalizer_root_key()
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "INSERT OR IGNORE INTO lab_claim_publication_finalizer_root_anchor "
            "(singleton, root_descriptor, root_key_digest) VALUES (1, ?, ?)",
            (root.descriptor, root.key_digest),
        )


def _finalizer_issuer(
    store: LabJobStore,
    *,
    authority_set: Authorities | None = None,
) -> LabClaimPublicationFinalizerAuthorityIssuer:
    key = str(store.path.resolve())
    material = _FINALIZER_TRUST_BY_STORE.get(key)
    if material is None:
        authorities = authority_set or create_test_authorities(
            store.path.parent / "finalizer-trust"
        )
        with store._connect() as connection:  # noqa: SLF001 - fixture builds offline cert binding
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
        root_key=_finalizer_root_key(),
        trust_certificate=certificate,
        trust_verifier=verifier,
        runtime_signer=signer,  # type: ignore[arg-type]
    )


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _initialize_source_stage_queue(path: Path, *, max_inbox: int) -> None:
    store_id = _sha256(canonical_json_bytes({"path": str(path.resolve())}))
    config_hash = _sha256(
        canonical_json_bytes(
            {
                "contract": "rquant-source-broker-v2-job-store-config/v2",
                "max_inbox": max_inbox,
                "schema_version": 2,
                "store_id": store_id,
            }
        )
    )
    with sqlite3.connect(path) as connection:
        existing = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'source_broker_v2_store_config'"
        ).fetchone()
        if existing is not None:
            return
        connection.execute(
            """
            CREATE TABLE source_broker_v2_store_config (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL,
                store_id TEXT NOT NULL,
                max_inbox INTEGER NOT NULL,
                config_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
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
                state TEXT NOT NULL,
                lease_generation INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO source_broker_v2_store_config (
                singleton, schema_version, store_id, max_inbox, config_hash, created_at
            ) VALUES (1, 2, ?, ?, ?, ?)
            """,
            (store_id, max_inbox, config_hash, NOW.isoformat()),
        )


def _source_stage_store(tmp_path: Path) -> LabSourceStageStore:
    queue_path = tmp_path / "source-runner.sqlite3"
    _initialize_source_stage_queue(queue_path, max_inbox=100)
    return LabSourceStageStore(
        tmp_path / "source-stage.sqlite3",
        queue_store_path=queue_path,
    )


def _authority(kind: str) -> SourceBrokerV2AuthorityRef:
    return SourceBrokerV2AuthorityRef(
        authority_id=f"{kind}-authority",
        key_id=f"{kind}-key-v2",
        purpose=f"rquant-{kind}-receipt",
        schema_version=2,
        generation=7,
        fence_hash="7" * 64,
    )


class _AcceptingOutcomeVerifier:
    def verify_source(self, **_: object) -> None:
        return None

    def verify_claim(self, **_: object) -> None:
        return None

    def verify_quota(self, **_: object) -> None:
        return None

    def verify_lineage(self, **_: object) -> None:
        return None


def _source_definition(
    authorities: Authorities,
    *,
    now: datetime = NOW,
) -> LabShardDefinition:
    payload, _ = authorized_payload_and_claim(
        now=now,
        plan_hash="2" * 64,
        shard_index=0,
        payload_json='{"partition":"2026-07-24"}',
        authority_set=authorities,
    )
    definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id=payload.adapter_id,
        adapter_version=payload.adapter_version,
        plan_hash="2" * 64,
        payload_json=payload.model_dump_json(round_trip=True),
        work_plan=LabShardWorkPlan(
            phase="strategy_replay",
            work_unit_name="symbol",
            work_units=1,
            static_duration_ms=1_000,
        ),
    )
    _AUTHORITIES_BY_PAYLOAD_HASH[definition.payload_hash] = authorities
    return definition


def _identity_from_claim(claim: LabShardClaimV2) -> LabClaimPublicationIdentity:
    return LabClaimPublicationIdentity.from_claim(claim)


def _held_draft(claim: LabShardClaimV2) -> HeldDraft:
    base_time = claim.claimed_at - timedelta(seconds=2)
    preimage_bytes = canonical_model_json_bytes(claim)
    return HeldDraft(
        identity=_identity_from_claim(claim),
        claim_preimage_bytes=preimage_bytes,
        claim_preimage_hash=_sha256(preimage_bytes),
        source_wait_deadline=base_time + timedelta(seconds=30),
        publication_deadline=base_time + timedelta(seconds=60),
    )


def _claimed_attempt(
    tmp_path: Path,
    *,
    lease_seconds: int = 600,
    definition: LabShardDefinition | None = None,
    execution_spec: ResearchRunSpec | None = None,
    authority_set: Authorities | None = None,
    now: datetime = NOW,
) -> tuple[LabJobStore, LabLeaseRecord, LabShardClaimV2, LabShardClaimV2, HeldDraft, Authorities]:
    authorities = authority_set or create_test_authorities(tmp_path / "authorities")
    store = _store(tmp_path, timeout=5_000)
    lease = _lease(store, seconds=lease_seconds, now=now)
    if execution_spec is None:
        envelope = _submit()
        if envelope.command.spec.deadline <= now:
            envelope = _submit(
                spec=envelope.command.spec.model_copy(update={"deadline": now + timedelta(days=1)})
            )
        assert store.apply_command(envelope, lease=lease, now=now).status == "applied"
        job = LabJobReader(store.path).get_job(envelope.command.job_id)
        assert job is not None
    else:
        envelope = _submit(spec=execution_spec)
        assert store.apply_command(envelope, lease=lease, now=now).status == "applied"
        job = LabJobReader(store.path).get_job(envelope.command.job_id)
        assert job is not None
    selected_definition = definition or _source_definition(authorities, now=now)
    store.plan_job(job.job_id, (selected_definition,), lease=lease, now=now + timedelta(seconds=1))
    source_store = _source_stage_store(tmp_path)
    claim = store.claim_next_shard(
        worker_id="held-worker",
        shard_lease_seconds=120,
        source_stage_store=source_store,
        source_wait_deadline=now + timedelta(seconds=30),
        publication_deadline=now + timedelta(seconds=60),
        lease=lease,
        now=now + timedelta(seconds=2),
    )
    assert isinstance(claim, LabShardClaimV2)
    held = _held_draft(claim)
    persisted = store.get_claim_publication(claim.claim_token)
    assert persisted is not None and persisted.status is ClaimPublicationStatus.HELD_SOURCE
    assert persisted.claim_preimage_bytes == canonical_model_json_bytes(claim)
    return store, lease, claim, claim, held, authorities


def _legacy_claimed_attempt(tmp_path: Path) -> tuple[LabJobStore, LabLeaseRecord, LabShardClaim]:
    store = _store(tmp_path, timeout=5_000)
    lease = _lease(store, seconds=600)
    job = _submit_job(store, lease)
    definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id="research.local",
        adapter_version="1.0.0",
        plan_hash="3" * 64,
        payload_json='{"partition":"2026-07-24"}',
        work_plan=LabShardWorkPlan(
            phase="strategy_replay",
            work_unit_name="symbol",
            work_units=1,
            static_duration_ms=1_000,
        ),
    )
    store.plan_job(job.job_id, (definition,), lease=lease, now=NOW + timedelta(seconds=1))
    claim = store.claim_next_shard(
        worker_id="legacy-worker",
        shard_lease_seconds=120,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    assert isinstance(claim, LabShardClaim)
    return store, lease, claim


def _unclaimed_held_draft(held: HeldDraft) -> HeldDraft:
    preimage = LabShardClaimV2.model_validate_json(held.claim_preimage_bytes, strict=True)
    unclaimed = LabShardClaimV2.model_validate(
        {
            **preimage.model_dump(mode="python"),
            "claim_token": uuid4(),
        },
        strict=True,
    )
    return _held_draft(unclaimed)


def _stage_binding(claim: LabShardClaimV2) -> LabSourceStageBinding:
    return LabSourceStageBinding(
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


def _intent(claim: LabShardClaimV2) -> SourceBrokerV2JobIntentEnvelope:
    authorities = _AUTHORITIES_BY_PAYLOAD_HASH[claim.definition.payload_hash]
    return build_source_broker_v2_scheduler_intent(
        claim.strategy_payload,
        claim=claim,
        manifest_keyring=authorities.authorization_keyring,
        authorization_keyring=authorities.authorization_keyring,
        deadline=claim.claimed_at + timedelta(seconds=60),
        now=claim.claimed_at,
    )


def _queue_binding(claim: LabShardClaimV2) -> QueueBinding:
    binding = _stage_binding(claim)
    intent = _intent(claim)
    binding_bytes = canonical_job_model_bytes(binding)
    intent_bytes = canonical_job_model_bytes(intent)
    return QueueBinding(
        source_stage_binding_bytes=binding_bytes,
        source_stage_binding_hash=_sha256(binding_bytes),
        source_intent_bytes=intent_bytes,
        source_intent_hash=_sha256(intent_bytes),
        source_operation_id=intent.operation_id,
        source_operation_hash=intent.operation_hash,
    )


def _outcome(intent: SourceBrokerV2JobIntentEnvelope) -> object:
    evidence = tuple(
        SourceBrokerV2NativeEvidence.create(
            kind=kind,
            request=canonical_request_bytes({"request": kind}),
            receipt=canonical_request_bytes({"receipt": kind}),
        )
        for kind in ("source", "claim", "quota", "lineage")
    )
    return build_verified_job_outcome(
        intent=intent,
        status=SourceBrokerV2JobOutcomeStatus.SUCCESS,
        response=canonical_request_bytes({"rows": 1}),
        source_evidence=evidence[0],
        claim_evidence=evidence[1],
        quota_evidence=evidence[2],
        lineage_evidence=evidence[3],
        verifier=_AcceptingOutcomeVerifier(),
        deadline=1.0,
    )


def _source_plan(
    claim: LabShardClaimV2,
    *,
    operation_id: str,
    authorities: Authorities,
    current_claim_authority: PersistentCurrentClaimAuthority,
) -> SourceUsePlanV2:
    payload = claim.strategy_payload
    issue_time = claim.claimed_at + timedelta(seconds=2)
    current_claim_authority.replace_current(claim)
    unsigned = SourceUsePlanV2.from_source_intent(
        payload.source_intent,
        issuer=current_claim_authority.plan_signer_identity.issuer,
        key_id=current_claim_authority.plan_signer_identity.key_id,
        attempt_binding=claim.attempt_binding,
        adapter_id=claim.definition.adapter_id,
        adapter_version=claim.definition.adapter_version,
        adapter_code_hash=claim.adapter_code_hash,
        payload_hash=claim.definition.payload_hash,
        payload_source_contract_hash=claim.payload_source_contract_hash,
        operation_id=operation_id,
        audience="lab-claim-publication",
        not_before=claim.claimed_at,
        expires_at=claim.lease_expires_at - timedelta(seconds=1),
        lease_expires_at=claim.lease_expires_at,
        nonce="claim-publication-plan",
        single_use_authority_id=current_claim_authority.authority_id,
    )
    receipt = current_claim_authority.issue_plan_once(
        issue=CurrentClaimPlanIssueV2.from_unsigned_plan(unsigned),
        now=issue_time,
    )
    return require_source_use_plan_v2(
        receipt.signed_plan,
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        now=issue_time,
    )


def _current_claim_authority(
    tmp_path: Path,
    authorities: Authorities,
) -> PersistentCurrentClaimAuthority:
    return PersistentCurrentClaimAuthority(
        tmp_path / "current-claim-authority.sqlite3",
        authority_id="test-current-claim-authority",
        signer=authorities.plan_v2,
        keyring=authorities.authorization_keyring,
        mode="test-standalone",
    )


def _ready_inputs(
    claim: LabShardClaimV2,
    queue: QueueBinding,
    authorities: Authorities,
    tmp_path: Path,
    source_store: LabSourceStageStore,
    writer: LabSourceStageWriterLease,
) -> tuple[SourceUsePlanV2, LabShardClaimV2]:
    base_time = claim.claimed_at - timedelta(seconds=2)
    binding = _stage_binding(claim)
    intent = _intent(claim)
    outcome = _outcome(intent)
    source_store.begin_external(
        binding,
        intent,
        lease=writer,
        now=base_time + timedelta(seconds=3),
    )

    with (
        patch.object(LabSourceStageStore, "_authorized_queue", return_value=object()),
        patch.object(
            lab_source_stage,
            "_QUEUE_GET_STATE",
            return_value=SourceBrokerV2JobRunnerState.PUBLISHED,
        ),
        patch.object(lab_source_stage, "_QUEUE_GET_VERIFIED_OUTCOME", return_value=outcome),
    ):
        source_store.bind_published_outcome(
            binding,
            lease=writer,
            now=base_time + timedelta(seconds=3),
        )
    current_claim_authority = _current_claim_authority(tmp_path, authorities)
    plan = _source_plan(
        claim,
        operation_id=intent.operation_id,
        authorities=authorities,
        current_claim_authority=current_claim_authority,
    )
    final_claim = claim.bind_source_use_plan(plan)
    return plan, final_claim


def _receipt(*, receipt_id: str = "receipt-0001") -> PublishReceipt:
    receipt_bytes = canonical_json_bytes({"receipt_id": receipt_id, "status": "published"})
    return PublishReceipt(
        spool_receipt_bytes=receipt_bytes,
        spool_receipt_hash=_sha256(receipt_bytes),
    )


def _typed_receipt(tmp_path: Path, final_claim: LabShardClaimV2) -> PublishReceipt:
    spool = LabClaimSpool(
        tmp_path / "published-claims",
        publish_receipt_publisher=_authority("finalizer"),
    )
    entry = spool.publish(final_claim)
    assert not isinstance(entry, (LabShardClaim,))
    receipt = LabClaimSpoolPublishReceiptV2.from_published_entry(
        spool=spool,
        entry=entry,
        final_claim=final_claim,
        committed_at=final_claim.claimed_at + timedelta(seconds=1),
    )
    return receipt.to_publish_receipt()


def _typed_receipt_verifier(tmp_path: Path) -> LabClaimSpoolReceiptVerifier:
    return LabClaimSpoolReceiptVerifier.from_spool(
        LabClaimSpool(
            tmp_path / "published-claims",
            publish_receipt_publisher=_authority("finalizer"),
        )
    )


def _conflicting_typed_receipt(receipt: PublishReceipt) -> PublishReceipt:
    typed = LabClaimSpoolPublishReceiptV2.model_validate_json(receipt.spool_receipt_bytes)
    return LabClaimSpoolPublishReceiptV2.model_validate(
        {
            **typed.model_dump(mode="python"),
            "committed_at": typed.committed_at + timedelta(seconds=1),
        }
    ).to_publish_receipt()


def _queue(
    store: LabJobStore,
    lease: LabLeaseRecord,
    held: HeldDraft,
    binding: QueueBinding,
    source_store: LabSourceStageStore,
) -> tuple[LabClaimPublicationRecord, LabSourceStageWriterLease]:
    claim = LabShardClaimV2.model_validate_json(held.claim_preimage_bytes, strict=True)
    base_time = claim.claimed_at - timedelta(seconds=2)
    stage_binding = _stage_binding(claim)
    writer = source_store.acquire_writer_lease(
        owner_id="source-stage-queue-writer",
        lease_seconds=120,
        now=base_time + timedelta(seconds=1),
    )
    source_store.enqueue_external(
        stage_binding,
        _intent(claim),
        lease=writer,
        now=base_time + timedelta(seconds=2),
    )
    return store.queue_claim_publication(
        held.identity,
        binding,
        lease=lease,
        now=base_time + timedelta(seconds=3),
    ).record, writer


def _ready(
    store: LabJobStore,
    lease: LabLeaseRecord,
    held: HeldDraft,
    signed_plan: SourceUsePlanV2,
    final_bound_claim: LabShardClaimV2,
    authorities: Authorities,
    tmp_path: Path,
) -> LabClaimPublicationRecord:
    return store.mark_claim_publication_ready(
        held.identity,
        signed_plan,
        final_bound_claim,
        current_claim_authority=_current_claim_authority(tmp_path, authorities),
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        lease=lease,
        now=NOW + timedelta(seconds=4),
    ).record


def test_held_persists_exact_source_stage_store_authority(tmp_path: Path) -> None:
    store, lease, _claim, _preimage, held, _authorities = _claimed_attempt(tmp_path)
    queue_path = tmp_path / "source-runner.sqlite3"
    _initialize_source_stage_queue(queue_path, max_inbox=100)
    source_store = LabSourceStageStore(
        tmp_path / "source-stage.sqlite3",
        queue_store_path=queue_path,
    )

    created = store.create_held_claim_publication(
        held,
        source_stage_store=source_store,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )

    expected = canonical_model_json_bytes(source_store.authority)
    assert created.record.source_stage_authority_bytes == expected
    assert created.record.source_stage_authority_hash == _sha256(expected)
    assert (
        LabSourceStageStoreAuthority.model_validate_json(
            created.record.source_stage_authority_bytes
        )
        == source_store.authority
    )


def test_same_source_paths_with_reinitialized_queue_authority_are_rejected(
    tmp_path: Path,
) -> None:
    store, lease, _claim, preimage, held, _authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    queue_path = source_store.queue_store_path
    queue_store_id = _sha256(canonical_json_bytes({"path": str(queue_path.resolve())}))
    replacement_max_inbox = 101
    replacement_config_hash = _sha256(
        canonical_json_bytes(
            {
                "contract": "rquant-source-broker-v2-job-store-config/v2",
                "max_inbox": replacement_max_inbox,
                "schema_version": 2,
                "store_id": queue_store_id,
            }
        )
    )
    with sqlite3.connect(queue_path) as connection:
        connection.execute(
            "UPDATE source_broker_v2_store_config "
            "SET max_inbox = ?, config_hash = ? WHERE singleton = 1",
            (replacement_max_inbox, replacement_config_hash),
        )

    with pytest.raises(ClaimPublicationConflictError, match="source_stage_authority_conflict"):
        store.queue_claim_publication(
            held.identity,
            _queue_binding(preimage),
            lease=lease,
            now=NOW + timedelta(seconds=3),
        )


def test_queue_reads_the_actual_queued_source_stage_record(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, _authorities = _claimed_attempt(tmp_path)
    queue_path = tmp_path / "source-runner.sqlite3"
    _initialize_source_stage_queue(queue_path, max_inbox=100)
    source_store = LabSourceStageStore(
        tmp_path / "source-stage.sqlite3",
        queue_store_path=queue_path,
    )
    stage_lease = source_store.acquire_writer_lease(
        owner_id="source-stage-writer",
        lease_seconds=120,
        now=NOW + timedelta(seconds=1),
    )
    assert stage_lease.fencing_token == lease.fencing_token
    queue = _queue_binding(preimage)
    stage_record = source_store.enqueue_external(
        _stage_binding(preimage),
        _intent(preimage),
        lease=stage_lease,
        now=NOW + timedelta(seconds=2),
    )
    created = store.create_held_claim_publication(
        held,
        source_stage_store=source_store,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )

    queued = store.queue_claim_publication(
        created.record.identity,
        queue,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    ).record

    assert queued.queued_source_stage_record_hash == stage_record.record_hash


def test_models_are_frozen_and_held_draft_knows_only_a_fields(tmp_path: Path) -> None:
    _store_value, _lease_value, _claim, preimage, held, _authorities = _claimed_attempt(tmp_path)

    assert tuple(ClaimPublicationStatus) == (
        ClaimPublicationStatus.HELD_SOURCE,
        ClaimPublicationStatus.SOURCE_QUEUED,
        ClaimPublicationStatus.READY_TO_PUBLISH,
        ClaimPublicationStatus.PUBLISHED,
        ClaimPublicationStatus.ABORTED,
    )
    assert set(HeldDraft.model_fields).isdisjoint(set(QueueBinding.model_fields))
    assert preimage.source_use_plan is None
    with pytest.raises(ValidationError, match="frozen"):
        held.claim_preimage_hash = "f" * 64  # type: ignore[misc]
    with pytest.raises(ValidationError, match="extra_forbidden"):
        HeldDraft.model_validate({**held.model_dump(), "source_operation_id": "forbidden"})


@pytest.mark.parametrize("model_name", ["held", "record"])
def test_deadlines_are_canonical_utc_and_source_wait_never_exceeds_publication(
    tmp_path: Path,
    model_name: str,
) -> None:
    _store_value, _lease_value, _claim, _preimage, held, _authorities = _claimed_attempt(tmp_path)
    values = held.model_dump(mode="python")
    values.update(
        source_wait_deadline=datetime(2026, 7, 24, 9, 0, tzinfo=timezone(timedelta(hours=8))),
        publication_deadline=datetime(2026, 7, 24, 1, 0, tzinfo=UTC),
    )
    canonical_held = HeldDraft.model_validate(values)
    assert canonical_held.source_wait_deadline.tzinfo is UTC
    assert canonical_held.source_wait_deadline == canonical_held.publication_deadline
    if model_name == "record":
        identity = LabClaimPublicationIdentity.model_validate(
            canonical_held.identity.model_dump(mode="python")
        )
        queue_path = tmp_path / "source-runner.sqlite3"
        _initialize_source_stage_queue(queue_path, max_inbox=100)
        authority = LabSourceStageStore(
            tmp_path / "source-stage.sqlite3", queue_store_path=queue_path
        ).authority
        authority_bytes = canonical_model_json_bytes(authority)
        provisional = LabClaimPublicationRecord.model_construct(
            **{
                **canonical_held.model_dump(mode="python"),
                "identity": identity,
                "source_stage_authority_bytes": authority_bytes,
                "source_stage_authority_hash": _sha256(authority_bytes),
                "status": ClaimPublicationStatus.HELD_SOURCE,
                "version": 0,
                "created_at": NOW,
                "updated_at": NOW,
                "queued_at": None,
                "ready_at": None,
                "published_at": None,
                "aborted_at": None,
                "terminal_reason": None,
                "record_commitment": "0" * 64,
            }
        )
        LabClaimPublicationRecord.model_validate(
            {
                **provisional.model_dump(mode="python"),
                "record_commitment": provisional.recomputed_commitment(),
            }
        )
    values["source_wait_deadline"] = values["publication_deadline"] + timedelta(microseconds=1)
    with pytest.raises(ValidationError, match="source_wait_deadline"):
        HeldDraft.model_validate(values)


def test_store_adds_fields_only_at_a_b_c_d_and_commits_each_version(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)

    created = store.create_held_claim_publication(
        held,
        source_stage_store=source_store,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    queued, stage_writer = _queue(store, lease, held, queue, source_store)
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    ready_record = _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)
    published = store.publish_claim_publication(
        held.identity,
        _typed_receipt(tmp_path, final_bound_claim),
        current_claim_authority=_current_claim_authority(tmp_path, authorities),
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    ).record

    assert created.record.status is ClaimPublicationStatus.HELD_SOURCE
    assert created.record.version == 0
    assert created.record.source_stage_binding_bytes is None
    assert queued.status is ClaimPublicationStatus.SOURCE_QUEUED
    assert queued.version == 1
    assert queued.source_stage_binding_bytes == queue.source_stage_binding_bytes
    assert queued.ready_source_stage_record_bytes is None
    assert ready_record.status is ClaimPublicationStatus.READY_TO_PUBLISH
    assert ready_record.version == 2
    assert ready_record.source_use_plan_bytes == canonical_model_json_bytes(signed_plan)
    assert ready_record.spool_receipt_bytes is None
    assert published.status is ClaimPublicationStatus.PUBLISHED
    assert published.version == 3
    assert (
        published.spool_receipt_bytes
        == _typed_receipt(tmp_path, final_bound_claim).spool_receipt_bytes
    )
    assert published.published_at == NOW + timedelta(seconds=5)
    assert all(
        record.record_commitment == record.recomputed_commitment()
        for record in (created.record, queued, ready_record, published)
    )


def test_publish_accepts_only_spool_receipt_and_revalidates_persisted_ready_claim(
    tmp_path: Path,
) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)
    authority = _current_claim_authority(tmp_path, authorities)

    assert set(inspect.signature(store.publish_claim_publication).parameters) == {
        "identity",
        "spool_receipt",
        "current_claim_authority",
        "keyring",
        "audience",
        "spool_receipt_verifier",
        "lease",
        "now",
    }
    empty_receipt_bytes = b"{}"
    with pytest.raises(ValueError, match="typed spool receipt"):
        store.publish_claim_publication(
            held.identity,
            PublishReceipt(
                spool_receipt_bytes=empty_receipt_bytes,
                spool_receipt_hash=_sha256(empty_receipt_bytes),
            ),
            current_claim_authority=authority,
            keyring=authorities.authorization_keyring,
            audience="lab-claim-publication",
            spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )
    with pytest.raises(ValidationError, match="spool_receipt_hash"):
        PublishReceipt(
            spool_receipt_bytes=empty_receipt_bytes,
            spool_receipt_hash="f" * 64,
        )

    with pytest.raises(ValueError, match="configured spool receipt verifier"):
        store.publish_claim_publication(
            held.identity,
            _typed_receipt(tmp_path, final_bound_claim),
            current_claim_authority=authority,
            keyring=authorities.authorization_keyring,
            audience="lab-claim-publication",
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )

    published = store.publish_claim_publication(
        held.identity,
        _typed_receipt(tmp_path, final_bound_claim),
        current_claim_authority=authority,
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    audit_count = len(store.list_claim_publication_audit(held.identity.attempt_id))
    replay = store.publish_claim_publication(
        held.identity,
        _typed_receipt(tmp_path, final_bound_claim),
        current_claim_authority=authority,
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )

    assert published.record.status is ClaimPublicationStatus.PUBLISHED
    assert not published.replayed
    assert replay.replayed
    assert replay.audit_ref is None
    assert len(store.list_claim_publication_audit(held.identity.attempt_id)) == audit_count


def test_published_replay_bypasses_expired_shard_validation_without_audit(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)
    authority = _current_claim_authority(tmp_path, authorities)
    receipt = _typed_receipt(tmp_path, final_bound_claim)
    store.publish_claim_publication(
        held.identity,
        receipt,
        current_claim_authority=authority,
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )
    audit_count = len(store.list_claim_publication_audit(held.identity.attempt_id))

    replay = store.publish_claim_publication(
        held.identity,
        receipt,
        current_claim_authority=authority,
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
        lease=lease,
        now=preimage.lease_expires_at + timedelta(seconds=1),
    )

    assert replay.replayed
    assert replay.audit_ref is None
    assert len(store.list_claim_publication_audit(held.identity.attempt_id)) == audit_count
    with pytest.raises(ValueError, match="sidecar conflicts"):
        store.publish_claim_publication(
            held.identity,
            _conflicting_typed_receipt(receipt),
            current_claim_authority=authority,
            keyring=authorities.authorization_keyring,
            audience="lab-claim-publication",
            spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
            lease=lease,
            now=preimage.lease_expires_at + timedelta(seconds=2),
        )


def test_authorized_sql_cannot_mutate_source_authority_or_ready_receipt(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    ready = _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)

    with sqlite3.connect(store.path) as connection:
        connection.create_function(lab_jobs._CLAIM_PUBLICATION_AUTH_FUNCTION, 4, lambda *_: 1)
        for column, value in (
            ("source_stage_authority_bytes", b'{"forged":true}'),
            ("source_stage_authority_hash", "f" * 64),
            ("current_claim_receipt_bytes", b'{"forged":true}'),
            ("current_claim_receipt_hash", "f" * 64),
        ):
            with pytest.raises(sqlite3.IntegrityError, match="terminal publication is immutable"):
                connection.execute(
                    f"UPDATE lab_claim_publication SET {column} = ? WHERE attempt_id = ?",
                    (value, str(ready.identity.attempt_id)),
                )


def test_store_replays_same_bytes_and_rejects_noncanonical_bindings(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    created = store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    assert store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=3)
    ).replayed
    _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    assert _queued.status is ClaimPublicationStatus.SOURCE_QUEUED
    assert store.queue_claim_publication(
        held.identity,
        queue,
        lease=lease,
        now=NOW + timedelta(seconds=4),
    ).replayed
    conflicting_intent = canonical_json_bytes({"not": "the queued intent"})
    conflict = queue.model_copy(
        update={
            "source_intent_bytes": conflicting_intent,
            "source_intent_hash": _sha256(conflicting_intent),
        }
    )
    with pytest.raises(ValidationError, match="canonical SourceBrokerV2JobIntentEnvelope"):
        store.queue_claim_publication(
            held.identity,
            conflict,
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    assert (
        _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path).status
        is ClaimPublicationStatus.READY_TO_PUBLISH
    )
    assert store.mark_claim_publication_ready(
        held.identity,
        signed_plan,
        final_bound_claim,
        current_claim_authority=_current_claim_authority(tmp_path, authorities),
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        lease=lease,
        now=NOW + timedelta(seconds=5),
    ).replayed
    receipt = _typed_receipt(tmp_path, final_bound_claim)
    assert (
        store.publish_claim_publication(
            held.identity,
            receipt,
            current_claim_authority=_current_claim_authority(tmp_path, authorities),
            keyring=authorities.authorization_keyring,
            audience="lab-claim-publication",
            spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
            lease=lease,
            now=NOW + timedelta(seconds=6),
        ).record.status
        is ClaimPublicationStatus.PUBLISHED
    )
    assert store.publish_claim_publication(
        held.identity,
        receipt,
        current_claim_authority=_current_claim_authority(tmp_path, authorities),
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
        lease=lease,
        now=NOW + timedelta(seconds=7),
    ).replayed
    with pytest.raises(ValueError, match="sidecar conflicts"):
        store.publish_claim_publication(
            held.identity,
            _conflicting_typed_receipt(receipt),
            current_claim_authority=_current_claim_authority(tmp_path, authorities),
            keyring=authorities.authorization_keyring,
            audience="lab-claim-publication",
            spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
            lease=lease,
            now=NOW + timedelta(seconds=8),
        )
    assert created.record != store.get_claim_publication(held.identity.attempt_id)


def test_expired_source_listing_includes_source_queued_records(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, _authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queue(store, lease, held, _queue_binding(preimage), source_store)

    expired = store.list_expired_source_claim_publications(
        now=NOW + timedelta(seconds=31),
    )

    assert tuple(record.status for record in expired) == (ClaimPublicationStatus.SOURCE_QUEUED,)
    assert store.list_expired_held_claim_publications(now=NOW + timedelta(seconds=31)) == expired


def test_store_rejects_skip_and_cross_attempt_binding(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    caller_attestation_fields = {
        "queued_source_stage_record_bytes",
        "queued_source_stage_record_hash",
        "ready_source_stage_record_bytes",
        "ready_source_stage_record_hash",
        "verified_source_outcome_hash",
        "verified_evidence_chain_hash",
        "current_claim_receipt_bytes",
        "current_claim_receipt_hash",
    }
    assert caller_attestation_fields.isdisjoint(
        inspect.signature(store.queue_claim_publication).parameters
    )
    assert caller_attestation_fields.isdisjoint(
        inspect.signature(store.mark_claim_publication_ready).parameters
    )
    with pytest.raises(ValidationError, match="extra_forbidden"):
        QueueBinding.model_validate(
            {
                **queue.model_dump(mode="python"),
                "queued_source_stage_record_hash": "0" * 64,
            }
        )
    current_claim_authority = _current_claim_authority(tmp_path, authorities)
    signed_plan = _source_plan(
        preimage,
        operation_id=queue.source_operation_id,
        authorities=authorities,
        current_claim_authority=current_claim_authority,
    )
    final_bound_claim = preimage.bind_source_use_plan(signed_plan)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    stage_binding = _stage_binding(preimage)
    stage_writer = source_store.acquire_writer_lease(
        owner_id="source-stage-queue-writer",
        lease_seconds=120,
        now=NOW + timedelta(seconds=1),
    )
    stage_intent = _intent(preimage)
    stage_record = source_store.enqueue_external(
        stage_binding,
        stage_intent,
        lease=stage_writer,
        now=NOW + timedelta(seconds=2),
    )
    assert stage_record.state is LabSourceStageState.QUEUED
    assert canonical_job_model_bytes(stage_record.binding) == queue.source_stage_binding_bytes
    assert stage_record.intent_hash == queue.source_intent_hash
    assert stage_record.operation_id == queue.source_operation_id
    assert stage_record.operation_hash == queue.source_operation_hash
    with pytest.raises(InvalidClaimPublicationTransitionError, match="transition_not_allowed"):
        _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)
    with pytest.raises(InvalidClaimPublicationTransitionError, match="transition_not_allowed"):
        store.publish_claim_publication(
            held.identity,
            _receipt(),
            current_claim_authority=_current_claim_authority(tmp_path, authorities),
            keyring=authorities.authorization_keyring,
            audience="lab-claim-publication",
            spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
            lease=lease,
            now=NOW + timedelta(seconds=3),
        )
    wrong_attempt_id = uuid4()
    wrong_identity = held.identity.model_copy(
        update={"claim_token": wrong_attempt_id, "attempt_id": wrong_attempt_id}
    )
    with pytest.raises(ClaimPublicationConflictError, match=r"^attempt_identity_conflict$"):
        store.queue_claim_publication(
            wrong_identity,
            queue,
            lease=lease,
            now=NOW + timedelta(seconds=3),
        )


@pytest.mark.parametrize(
    "through",
    [
        ClaimPublicationStatus.HELD_SOURCE,
        ClaimPublicationStatus.SOURCE_QUEUED,
        ClaimPublicationStatus.READY_TO_PUBLISH,
    ],
)
def test_abort_is_terminal_without_inventing_later_fields(
    tmp_path: Path,
    through: ClaimPublicationStatus,
) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held,
        source_stage_store=source_store,
        lease=lease,
        now=NOW + timedelta(seconds=2),
    )
    if through is not ClaimPublicationStatus.HELD_SOURCE:
        _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    if through is ClaimPublicationStatus.READY_TO_PUBLISH:
        signed_plan, final_bound_claim = _ready_inputs(
            preimage,
            queue,
            authorities,
            tmp_path,
            source_store,
            stage_writer,
        )
        _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)
    aborted = store.abort_claim_publication(
        held.identity,
        terminal_reason="source_wait_expired",
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )

    assert aborted.record.status is ClaimPublicationStatus.ABORTED
    assert (
        aborted.record.version
        == {
            ClaimPublicationStatus.HELD_SOURCE: 1,
            ClaimPublicationStatus.SOURCE_QUEUED: 2,
            ClaimPublicationStatus.READY_TO_PUBLISH: 3,
        }[through]
    )
    assert aborted.record.spool_receipt_bytes is None
    assert (aborted.record.ready_source_stage_record_bytes is None) is (
        through is not ClaimPublicationStatus.READY_TO_PUBLISH
    )
    assert store.abort_claim_publication(
        held.identity,
        terminal_reason="source_wait_expired",
        lease=lease,
        now=NOW + timedelta(seconds=6),
    ).replayed
    with pytest.raises(InvalidClaimPublicationTransitionError, match="terminal_status_immutable"):
        store.queue_claim_publication(
            held.identity,
            queue,
            lease=lease,
            now=NOW + timedelta(seconds=7),
        )


def test_ready_requires_exact_record_outcome_verified_plan_and_final_bound_claim(
    tmp_path: Path,
) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    parameter_names = set(inspect.signature(store.mark_claim_publication_ready).parameters)
    assert parameter_names == {
        "identity",
        "signed_plan",
        "final_bound_claim",
        "current_claim_authority",
        "keyring",
        "audience",
        "lease",
        "now",
    }
    with pytest.raises(TypeError, match="ready_source_stage_record_bytes"):
        store.mark_claim_publication_ready(
            held.identity,
            signed_plan,
            final_bound_claim,
            ready_source_stage_record_bytes=b"caller-controlled",
            keyring=authorities.authorization_keyring,
            current_claim_authority=_current_claim_authority(tmp_path, authorities),
            audience="lab-claim-publication",
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )
    early_signed_plan = signed_plan.model_copy(update={"not_before": NOW + timedelta(seconds=5)})
    with pytest.raises(SourceOperationContractError, match="signature is invalid"):
        _ready(
            store,
            lease,
            held,
            early_signed_plan,
            final_bound_claim,
            authorities,
            tmp_path,
        )
    with pytest.raises(ClaimPublicationConflictError, match="ready_binding_conflict"):
        _ready(store, lease, held, signed_plan, preimage, authorities, tmp_path)
    ready_record = _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)
    assert ready_record.status is ClaimPublicationStatus.READY_TO_PUBLISH
    ready_binding = ReadyBinding.model_validate(
        {
            "ready_source_stage_record_bytes": ready_record.ready_source_stage_record_bytes,
            "ready_source_stage_record_hash": ready_record.ready_source_stage_record_hash,
            "verified_source_outcome_hash": ready_record.verified_source_outcome_hash,
            "verified_evidence_chain_hash": ready_record.verified_evidence_chain_hash,
            "source_use_plan_bytes": ready_record.source_use_plan_bytes,
            "source_use_plan_hash": ready_record.source_use_plan_hash,
            "final_claim_bytes": ready_record.final_claim_bytes,
            "final_claim_hash": ready_record.final_claim_hash,
            "current_claim_receipt_bytes": ready_record.current_claim_receipt_bytes,
            "current_claim_receipt_hash": ready_record.current_claim_receipt_hash,
        }
    )
    with pytest.raises(ValidationError, match="extra_forbidden"):
        ReadyBinding.model_validate(
            {
                **ready_binding.model_dump(mode="python"),
                "caller_reported_current_claim_receipt_hash": "0" * 64,
            }
        )
    assert store.mark_claim_publication_ready(
        held.identity,
        signed_plan,
        final_bound_claim,
        current_claim_authority=_current_claim_authority(tmp_path, authorities),
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        lease=lease,
        now=NOW + timedelta(seconds=5),
    ).replayed
    wrong_attempt = uuid4()
    with pytest.raises(ClaimPublicationConflictError, match="attempt_identity_conflict"):
        _ready(
            store,
            lease,
            held.model_copy(
                update={
                    "identity": held.identity.model_copy(
                        update={"attempt_id": wrong_attempt, "claim_token": wrong_attempt}
                    )
                }
            ),
            signed_plan,
            final_bound_claim,
            authorities,
            tmp_path,
        )


def test_ready_requires_persisted_source_stage_to_be_ready(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queue(store, lease, held, queue, source_store)
    current_claim_authority = _current_claim_authority(tmp_path, authorities)
    signed_plan = _source_plan(
        preimage,
        operation_id=queue.source_operation_id,
        authorities=authorities,
        current_claim_authority=current_claim_authority,
    )
    with pytest.raises(ClaimPublicationConflictError, match="ready_source_stage_conflict"):
        _ready(
            store,
            lease,
            held,
            signed_plan,
            preimage.bind_source_use_plan(signed_plan),
            authorities,
            tmp_path,
        )


def test_internal_held_helper_does_not_probe_shard_and_rolls_back(tmp_path: Path) -> None:
    store, lease, _claim, _preimage, held, _authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    isolated_held = _unclaimed_held_draft(held)
    observed_sql: list[str] = []
    with (
        pytest.raises(RuntimeError, match="caller rollback"),
        store._transaction() as connection,
    ):
        connection.set_trace_callback(observed_sql.append)
        result = store._create_held_claim_publication_in_transaction(
            connection,
            isolated_held,
            source_stage_authority=source_store.authority,
            lease=lease,
            now=NOW + timedelta(seconds=2),
        )
        assert result.mutation is not None
        raise RuntimeError("caller rollback")
    assert store.get_claim_publication(isolated_held.identity.attempt_id) is None
    assert not any("FROM lab_shard" in statement for statement in observed_sql)


def test_stale_lease_fence_and_attempt_generation_are_rejected(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, _authorities = _claimed_attempt(
        tmp_path, lease_seconds=10
    )
    source_store = _source_stage_store(tmp_path)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    queue = _queue_binding(preimage)
    with pytest.raises(ClaimPublicationConflictError, match="attempt_identity_conflict"):
        store.queue_claim_publication(
            held.identity.model_copy(update={"claim_generation": 2}),
            queue,
            lease=lease,
            now=NOW + timedelta(seconds=3),
        )
    with pytest.raises(SchedulerLeaseFencedError):
        store.queue_claim_publication(
            held.identity,
            queue,
            lease=lease,
            now=NOW + timedelta(seconds=11),
        )
    replacement = _lease(
        store,
        owner="scheduler-replacement",
        now=NOW + timedelta(seconds=11),
        seconds=60,
    )
    with pytest.raises(SchedulerLeaseFencedError, match="publication_fence_conflict"):
        store.queue_claim_publication(
            held.identity,
            queue,
            lease=replacement,
            now=NOW + timedelta(seconds=12),
        )


def test_ready_external_authority_verification_does_not_hold_publication_write_lock(
    tmp_path: Path,
) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    authority = _current_claim_authority(tmp_path, authorities)
    verification_started = Event()
    allow_verification = Event()
    completed = Event()
    errors: list[BaseException] = []
    original_verify = lab_jobs.require_current_claim_consumption_v2

    def blocking_verify(**kwargs: object) -> object:
        verification_started.set()
        assert allow_verification.wait(timeout=2)
        return original_verify(**kwargs)  # type: ignore[arg-type]

    def mark_ready() -> None:
        try:
            store.mark_claim_publication_ready(
                held.identity,
                signed_plan,
                final_bound_claim,
                current_claim_authority=authority,
                keyring=authorities.authorization_keyring,
                audience="lab-claim-publication",
                lease=lease,
                now=NOW + timedelta(seconds=4),
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            completed.set()

    with patch.object(
        lab_jobs,
        "require_current_claim_consumption_v2",
        side_effect=blocking_verify,
    ):
        thread = Thread(target=mark_ready)
        thread.start()
        assert verification_started.wait(timeout=2)

        competing = LabJobStore(store.path, busy_timeout_ms=100)
        with competing._transaction():
            pass
        competing.abort_claim_publication(
            held.identity,
            terminal_reason="concurrent_abort",
            lease=lease,
            now=NOW + timedelta(seconds=5),
        )

        allow_verification.set()
        assert completed.wait(timeout=2)
        thread.join(timeout=2)

    assert len(errors) == 1
    assert isinstance(errors[0], ClaimPublicationConflictError)
    assert str(errors[0]) == "publication_cas_conflict"
    assert (
        store.get_claim_publication(held.identity.attempt_id).status
        is ClaimPublicationStatus.ABORTED
    )


def test_validate_ready_claim_for_publication_returns_stable_bound_claim(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)

    assert (
        store.validate_ready_claim_for_publication(
            held.identity,
            current_claim_authority=_current_claim_authority(tmp_path, authorities),
            keyring=authorities.authorization_keyring,
            audience="lab-claim-publication",
            now=NOW + timedelta(seconds=4),
        )
        == final_bound_claim
    )


def test_validate_ready_claim_for_publication_rejects_abort_after_snapshot_before_shard_check(
    tmp_path: Path,
) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)

    snapshot_read = Event()
    allow_shard_check = Event()
    validation_done = Event()
    writer_done = Event()
    validation_results: list[LabShardClaimV2] = []
    validation_errors: list[BaseException] = []
    writer_errors: list[BaseException] = []
    original_validate_shard_binding = LabJobStore._validate_claim_publication_shard_binding

    def pause_after_publication_snapshot(
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        *,
        now: datetime,
    ) -> None:
        snapshot_read.set()
        assert allow_shard_check.wait(timeout=2)
        original_validate_shard_binding(connection, identity, now=now)

    def validate_ready_claim() -> None:
        try:
            validation_results.append(
                store.validate_ready_claim_for_publication(
                    held.identity,
                    current_claim_authority=_current_claim_authority(tmp_path, authorities),
                    keyring=authorities.authorization_keyring,
                    audience="lab-claim-publication",
                    now=NOW + timedelta(seconds=4),
                )
            )
        except BaseException as exc:
            validation_errors.append(exc)
        finally:
            validation_done.set()

    def abort_from_other_store() -> None:
        try:
            LabJobStore(store.path, busy_timeout_ms=100).abort_claim_publication(
                held.identity,
                terminal_reason="concurrent_abort",
                lease=lease,
                now=NOW + timedelta(seconds=5),
            )
        except BaseException as exc:
            writer_errors.append(exc)
        finally:
            writer_done.set()

    with patch.object(
        LabJobStore,
        "_validate_claim_publication_shard_binding",
        new=staticmethod(pause_after_publication_snapshot),
    ):
        validation_thread = Thread(target=validate_ready_claim)
        validation_thread.start()
        assert snapshot_read.wait(timeout=2)

        writer_thread = Thread(target=abort_from_other_store)
        writer_thread.start()
        assert writer_done.wait(timeout=2)
        assert writer_errors == []

        allow_shard_check.set()
        assert validation_done.wait(timeout=2)
        validation_thread.join(timeout=2)
        writer_thread.join(timeout=2)

    assert validation_results == []
    assert len(validation_errors) == 1
    assert isinstance(validation_errors[0], ClaimPublicationConflictError)
    assert str(validation_errors[0]) == "publication_cas_conflict"


def test_validate_ready_claim_for_publication_rejects_external_commit_during_snapshot(
    tmp_path: Path,
) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    _ready(store, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)

    snapshot_read = Event()
    allow_shard_check = Event()
    validation_done = Event()
    writer_done = Event()
    validation_results: list[LabShardClaimV2] = []
    validation_errors: list[BaseException] = []
    writer_errors: list[BaseException] = []
    original_validate_shard_binding = LabJobStore._validate_claim_publication_shard_binding

    def pause_after_publication_snapshot(
        connection: sqlite3.Connection,
        identity: LabClaimPublicationIdentity,
        *,
        now: datetime,
    ) -> None:
        snapshot_read.set()
        assert allow_shard_check.wait(timeout=2)
        original_validate_shard_binding(connection, identity, now=now)

    def validate_ready_claim() -> None:
        try:
            validation_results.append(
                store.validate_ready_claim_for_publication(
                    held.identity,
                    current_claim_authority=_current_claim_authority(tmp_path, authorities),
                    keyring=authorities.authorization_keyring,
                    audience="lab-claim-publication",
                    now=NOW + timedelta(seconds=4),
                )
            )
        except BaseException as exc:
            validation_errors.append(exc)
        finally:
            validation_done.set()

    def renew_from_other_store() -> None:
        try:
            LabJobStore(store.path, busy_timeout_ms=100).renew_scheduler_lease(
                lease,
                lease_seconds=600,
                now=NOW + timedelta(seconds=5),
            )
        except BaseException as exc:
            writer_errors.append(exc)
        finally:
            writer_done.set()

    with patch.object(
        LabJobStore,
        "_validate_claim_publication_shard_binding",
        new=staticmethod(pause_after_publication_snapshot),
    ):
        validation_thread = Thread(target=validate_ready_claim)
        validation_thread.start()
        assert snapshot_read.wait(timeout=2)

        writer_thread = Thread(target=renew_from_other_store)
        writer_thread.start()
        assert writer_done.wait(timeout=2)
        assert writer_errors == []

        allow_shard_check.set()
        assert validation_done.wait(timeout=2)
        validation_thread.join(timeout=2)
        writer_thread.join(timeout=2)

    assert validation_results == []
    assert len(validation_errors) == 1
    assert isinstance(validation_errors[0], ClaimPublicationConflictError)
    assert str(validation_errors[0]) == "publication_cas_conflict"


def test_restart_concurrency_audit_and_terminal_sql_guard(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    reopened = LabJobStore(store.path, busy_timeout_ms=5_000)
    reopened.initialize()
    queue = _queue_binding(preimage)
    stage_writer = source_store.acquire_writer_lease(
        owner_id="source-stage-queue-writer",
        lease_seconds=120,
        now=NOW + timedelta(seconds=1),
    )
    source_store.enqueue_external(
        _stage_binding(preimage),
        _intent(preimage),
        lease=stage_writer,
        now=NOW + timedelta(seconds=2),
    )

    def queue_once(_: int) -> bool:
        return (
            LabJobStore(store.path, busy_timeout_ms=5_000)
            .queue_claim_publication(
                held.identity,
                queue,
                lease=lease,
                now=NOW + timedelta(seconds=3),
            )
            .replayed
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        replayed = tuple(executor.map(queue_once, range(2)))
    assert sorted(replayed) == [False, True]
    signed_plan, final_bound_claim = _ready_inputs(
        preimage, queue, authorities, tmp_path, source_store, stage_writer
    )
    _ready(reopened, lease, held, signed_plan, final_bound_claim, authorities, tmp_path)
    published = reopened.publish_claim_publication(
        held.identity,
        _typed_receipt(tmp_path, final_bound_claim),
        current_claim_authority=_current_claim_authority(tmp_path, authorities),
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        spool_receipt_verifier=_typed_receipt_verifier(tmp_path),
        lease=lease,
        now=NOW + timedelta(seconds=5),
    ).record
    audit = reopened.list_claim_publication_audit(held.identity.attempt_id)
    assert audit[0].action is ClaimPublicationAuditAction.CREATED
    assert tuple(item.action for item in audit).count(ClaimPublicationAuditAction.TRANSITIONED) == 3
    assert tuple(item.action for item in audit).count(ClaimPublicationAuditAction.REPLAYED) == 1
    assert all(item.audit_hash == item.recomputed_hash() for item in audit)
    assert audit[-1].record_commitment == published.record_commitment
    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(
            sqlite3.IntegrityError,
            match="terminal publication is immutable",
        ),
    ):
        connection.create_function(lab_jobs._CLAIM_PUBLICATION_AUTH_FUNCTION, 4, lambda *_: 1)
        connection.execute(
            "UPDATE lab_claim_publication SET status = 'ABORTED' WHERE attempt_id = ?",
            (str(held.identity.attempt_id),),
        )
    assert LabJobReader(store.path).audit_integrity().table_counts.lab_claim_publication == 1


def test_v7_migrates_directly_to_final_v12_and_provisional_v8_fails_closed(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy-v7.sqlite3"
    with sqlite3.connect(legacy) as connection:
        for statement in lab_jobs._V7_SCHEMA_STATEMENTS:
            connection.execute(statement)
        connection.execute(f"PRAGMA application_id = {LabJobStore.APPLICATION_ID}")
        connection.execute("PRAGMA user_version = 7")
    store = LabJobStore(legacy)
    store.initialize()
    store.initialize()
    with sqlite3.connect(legacy) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == LabJobStore.SCHEMA_VERSION
        sql = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'lab_claim_publication'"
        ).fetchone()[0]
    assert "SOURCE_QUEUED" in sql.upper()
    assert "CLAIM_PREIMAGE_BYTES" in sql.upper()

    provisional = tmp_path / "provisional-v8.sqlite3"
    with sqlite3.connect(provisional) as connection:
        for statement in lab_jobs._V7_SCHEMA_STATEMENTS:
            connection.execute(statement)
        connection.execute(
            "CREATE TABLE lab_claim_publication (attempt_id TEXT PRIMARY KEY, "
            "canonical_claim_bytes BLOB NOT NULL, source_operation_id TEXT NOT NULL)"
        )
        connection.execute(f"PRAGMA application_id = {LabJobStore.APPLICATION_ID}")
        connection.execute("PRAGMA user_version = 8")
    with pytest.raises(lab_jobs.LabDatabaseIdentityError, match="lab_claim_publication.*invalid"):
        LabJobStore(provisional).initialize()


def test_legacy_claim_and_active_claim_listing_remain_publication_free(tmp_path: Path) -> None:
    store, lease, claim = _legacy_claimed_attempt(tmp_path)

    active = store.list_active_claims(
        lease,
        now=NOW + timedelta(seconds=3),
        initial_lease_seconds=120,
    )

    assert active == (claim,)
    assert store.get_claim_publication(claim.claim_token) is None


def test_authority_owned_finalizer_drives_exact_c_d_and_worker_d_gate(tmp_path: Path) -> None:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(tmp_path)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queued, stage_writer = _queue(store, lease, held, queue, source_store)
    _ready_inputs(preimage, queue, authorities, tmp_path, source_store, stage_writer)
    current_authority = _current_claim_authority(tmp_path, authorities)
    spool = LabClaimSpool(
        tmp_path / "finalizer-claims",
        publish_receipt_publisher=_authority("finalizer"),
    )
    _test_only_preseed_finalizer_root_anchor(store)
    finalizer = LabClaimFinalizer(
        ledger=store,
        stage_reader=source_store,
        authority=_finalizer_issuer(store).acquire(
            owner_id="finalizer-a", lease_seconds=60, now=NOW + timedelta(seconds=5)
        ),
        current_claim_authority=current_authority,
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        spool=spool,
        spool_receipt_verifier=LabClaimSpoolReceiptVerifier.from_spool(spool),
        clock=lambda: NOW + timedelta(seconds=5),
    )

    published = finalizer.finalize(held.identity)
    replay = finalizer.finalize(held.identity)

    assert published.status == "published"
    assert replay.status == "replayed"
    assert published.record is not None
    assert published.record.status is ClaimPublicationStatus.PUBLISHED
    audit = store.list_claim_publication_audit(held.identity.attempt_id)
    transition_reasons = tuple(
        item.reason_code for item in audit if item.action.value == "transitioned"
    )
    assert transition_reasons.count("finalizer_source_queued_to_ready_to_publish") == 1
    assert transition_reasons.count("finalizer_ready_to_published") == 1
    final_claim = strict_model_validate_canonical_json(
        LabShardClaimV2, published.record.final_claim_bytes or b""
    )
    LabClaimPublicationWorkerVerifier(
        ledger=store,
        current_claim_authority=current_authority,
        keyring=authorities.authorization_keyring,
        audience="lab-claim-publication",
        spool_receipt_verifier=LabClaimSpoolReceiptVerifier.from_spool(spool),
        trust_verifier=_finalizer_issuer(store)._trust_verifier,  # noqa: SLF001
    ).require_published_claim(final_claim, now=NOW + timedelta(seconds=5))


def _prepared_authority_finalizer(
    tmp_path: Path,
    *,
    finalizer_type: type[LabClaimFinalizer] = LabClaimFinalizer,
    authority_set: Authorities | None = None,
    now: datetime = NOW,
) -> tuple[LabClaimFinalizer, LabJobStore, HeldDraft]:
    store, lease, _claim, preimage, held, authorities = _claimed_attempt(
        tmp_path,
        authority_set=authority_set,
        now=now,
    )
    base_time = preimage.claimed_at - timedelta(seconds=2)
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held,
        source_stage_store=source_store,
        lease=lease,
        now=base_time + timedelta(seconds=2),
    )
    _queued, writer = _queue(store, lease, held, queue, source_store)
    _ready_inputs(preimage, queue, authorities, tmp_path, source_store, writer)
    current_authority = _current_claim_authority(tmp_path, authorities)
    spool = LabClaimSpool(
        tmp_path / "finalizer-claims",
        publish_receipt_publisher=_authority("finalizer"),
    )
    _test_only_preseed_finalizer_root_anchor(store)
    authority = _finalizer_issuer(store, authority_set=authorities).acquire(
        owner_id="finalizer-replay",
        lease_seconds=60,
        now=base_time + timedelta(seconds=5),
    )
    return (
        finalizer_type(
            ledger=store,
            stage_reader=source_store,
            authority=authority,
            current_claim_authority=current_authority,
            keyring=authorities.authorization_keyring,
            audience="lab-claim-publication",
            spool=spool,
            spool_receipt_verifier=LabClaimSpoolReceiptVerifier.from_spool(spool),
            clock=lambda: base_time + timedelta(seconds=5),
        ),
        store,
        held,
    )


def _source_execution_definition(
    authorities: Authorities,
) -> tuple[LabShardDefinition, ResearchRunSpec]:
    """Build a V2 envelope whose signed inner payload is a real strategy shard."""

    legacy = _strategy_claim(_nshape_compare_spec(hold_days=(1,)), shard_index=0)
    base = signed_manifest(authorities)
    unsigned = base.model_copy(
        update={
            "adapter_id": legacy.definition.adapter_id,
            "adapter_version": legacy.definition.adapter_version,
            "adapter_code_hash": legacy.spec_hash,
            "signature": "",
        }
    )
    manifest = unsigned.model_copy(
        update={
            "signature": authorities.manifest_active.sign(
                namespace=ADAPTER_MANIFEST_NAMESPACE,
                payload=unsigned.signing_bytes(),
            )
        }
    )
    payload, _claim = authorized_payload_and_claim(
        now=NOW,
        plan_hash=legacy.definition.plan_hash,
        shard_index=legacy.shard_index,
        payload_json=legacy.definition.payload_json,
        authority_set=authorities,
        manifest=manifest,
    )
    definition = LabShardDefinition.from_payload(
        shard_index=legacy.shard_index,
        adapter_id=payload.adapter_id,
        adapter_version=payload.adapter_version,
        plan_hash=legacy.definition.plan_hash,
        payload_json=payload.model_dump_json(round_trip=True),
        work_plan=legacy.definition.work_plan,
    )
    _AUTHORITIES_BY_PAYLOAD_HASH[definition.payload_hash] = authorities
    return definition, _nshape_compare_spec(hold_days=(1,))


def _prepared_execution_finalizer(
    tmp_path: Path,
) -> tuple[LabClaimFinalizer, LabJobStore, HeldDraft]:
    authorities = create_test_authorities(tmp_path / "authorities")
    definition, spec = _source_execution_definition(authorities)
    store, lease, _claim, preimage, held, _authorities = _claimed_attempt(
        tmp_path,
        definition=definition,
        execution_spec=spec,
        authority_set=authorities,
    )
    source_store = _source_stage_store(tmp_path)
    queue = _queue_binding(preimage)
    store.create_held_claim_publication(
        held, source_stage_store=source_store, lease=lease, now=NOW + timedelta(seconds=2)
    )
    _queued, writer = _queue(store, lease, held, queue, source_store)
    _ready_inputs(preimage, queue, authorities, tmp_path, source_store, writer)
    current_authority = _current_claim_authority(tmp_path, authorities)
    spool = LabClaimSpool(
        tmp_path / "finalizer-claims",
        publish_receipt_publisher=_authority("finalizer"),
    )
    _test_only_preseed_finalizer_root_anchor(store)
    authority = _finalizer_issuer(store).acquire(
        owner_id="finalizer-execution", lease_seconds=60, now=NOW + timedelta(seconds=5)
    )
    return (
        LabClaimFinalizer(
            ledger=store,
            stage_reader=source_store,
            authority=authority,
            current_claim_authority=current_authority,
            keyring=authorities.authorization_keyring,
            audience="lab-claim-publication",
            spool=spool,
            spool_receipt_verifier=LabClaimSpoolReceiptVerifier.from_spool(spool),
            clock=lambda: NOW + timedelta(seconds=5),
        ),
        store,
        held,
    )


@pytest.mark.parametrize(
    "window",
    (
        "ready_before_issue",
        "issue_before_ready",
        "ready_before_spool",
        "spool_before_sidecar",
        "sidecar_before_published",
        "published_after_d",
    ),
)
def test_finalizer_six_crash_windows_replay_exactly_once(tmp_path: Path, window: str) -> None:
    class _CrashFinalizer(LabClaimFinalizer):
        @staticmethod
        def _crash(name: str) -> None:
            if window == name:
                raise RuntimeError("injected finalizer crash")

        @staticmethod
        def _after_ready_before_issue(record: LabClaimPublicationRecord) -> None:
            del record
            _CrashFinalizer._crash("ready_before_issue")

        @staticmethod
        def _after_issue_before_ready(record: LabClaimPublicationRecord) -> None:
            del record
            _CrashFinalizer._crash("issue_before_ready")

        @staticmethod
        def _after_ready_before_spool(record: LabClaimPublicationRecord) -> None:
            del record
            _CrashFinalizer._crash("ready_before_spool")

        @staticmethod
        def _after_spool_before_sidecar(record: LabClaimPublicationRecord) -> None:
            del record
            _CrashFinalizer._crash("spool_before_sidecar")

        @staticmethod
        def _after_sidecar_before_published(record: LabClaimPublicationRecord) -> None:
            del record
            _CrashFinalizer._crash("sidecar_before_published")

        @staticmethod
        def _after_published(record: LabClaimPublicationRecord) -> None:
            del record
            _CrashFinalizer._crash("published_after_d")

    crashed, store, held = _prepared_authority_finalizer(tmp_path, finalizer_type=_CrashFinalizer)
    assert crashed.finalize(held.identity).status == "blocked"

    # Reopen every durable authority boundary. Only the injected root key survives
    # process restart; the ledger, stage reader, current authority, spool, and
    # finalizer are fresh objects.
    reopened_store = LabJobStore(store.path)
    reopened_store.initialize()
    reopened_stage = LabSourceStageStore(
        crashed._stage_reader.path,  # noqa: SLF001
        queue_store_path=crashed._stage_reader.queue_store_path,  # noqa: SLF001
    )
    reopened_current = PersistentCurrentClaimAuthority(
        crashed._current_claim_authority.path,  # noqa: SLF001
        authority_id=crashed._current_claim_authority.authority_id,  # noqa: SLF001
        signer=crashed._current_claim_authority._signer,  # noqa: SLF001
        keyring=crashed._keyring,  # noqa: SLF001
        mode="test-standalone",
    )
    reopened_spool = LabClaimSpool(
        tmp_path / "finalizer-claims",
        publish_receipt_publisher=_authority("finalizer"),
    )
    replay = LabClaimFinalizer(
        ledger=reopened_store,
        stage_reader=reopened_stage,
        authority=_finalizer_issuer(reopened_store).acquire(
            owner_id="finalizer-replay", lease_seconds=60, now=NOW + timedelta(seconds=5)
        ),
        current_claim_authority=reopened_current,
        keyring=crashed._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool=reopened_spool,
        spool_receipt_verifier=LabClaimSpoolReceiptVerifier.from_spool(reopened_spool),
        clock=lambda: NOW + timedelta(seconds=5),
    )
    completed = replay.finalize(held.identity)
    record = reopened_store.get_claim_publication(held.identity.attempt_id)
    assert completed.status in {"published", "replayed"}
    assert record is not None and record.status is ClaimPublicationStatus.PUBLISHED
    assert record.final_claim_bytes is not None
    assert record.spool_receipt_bytes is not None
    audit = reopened_store.list_claim_publication_audit(held.identity.attempt_id)
    assert (
        sum(item.reason_code == "finalizer_source_queued_to_ready_to_publish" for item in audit)
        == 1
    )
    assert sum(item.reason_code == "finalizer_ready_to_published" for item in audit) == 1
    observations = reopened_store.list_claim_publication_finalizer_observations(
        held.identity.attempt_id
    )
    assert any(event == "blocked" for event, *_rest in observations)
    assert any(event in {"published", "replayed"} for event, *_rest in observations)


@pytest.mark.parametrize("phase", ("c", "d"))
def test_finalizer_current_guard_blocks_replacement_until_ledger_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    finalizer, store, held = _prepared_authority_finalizer(tmp_path)
    if phase == "d":
        ready = finalizer._issue_ready(
            finalizer._record(held.identity), now=NOW + timedelta(seconds=5)
        )  # noqa: SLF001
        assert ready.record.status is ClaimPublicationStatus.READY_TO_PUBLISH

    preimage = finalizer._preimage(finalizer._record(held.identity))  # noqa: SLF001
    replacement = LabShardClaimV2.model_validate(
        preimage.model_dump(mode="python")
        | {
            "claim_generation": preimage.claim_generation + 1,
            "claim_token": uuid4(),
            "scheduler_fencing_token": preimage.scheduler_fencing_token + 1,
        }
    )
    replacement_authority = PersistentCurrentClaimAuthority(
        finalizer._current_claim_authority.path,  # noqa: SLF001
        authority_id=finalizer._current_claim_authority.authority_id,  # noqa: SLF001
        signer=finalizer._current_claim_authority._signer,  # noqa: SLF001
        keyring=finalizer._keyring,  # noqa: SLF001
        mode="test-standalone",
    )
    started = Event()
    completed = Event()
    failures: list[BaseException] = []

    def replace() -> None:
        try:
            started.set()
            replacement_authority.replace_current(replacement)
        except BaseException as exc:
            failures.append(exc)
        finally:
            completed.set()

    target = (
        "_finalizer_mark_claim_publication_ready_in_transaction"
        if phase == "c"
        else "_finalizer_publish_claim_publication_in_transaction"
    )
    original = getattr(store, target)
    replacement_thread: Thread | None = None

    def barrier(*args: object, **kwargs: object) -> object:
        nonlocal replacement_thread
        replacement_thread = Thread(target=replace)
        replacement_thread.start()
        assert started.wait(timeout=1)
        assert not completed.wait(timeout=0.1)
        return original(*args, **kwargs)

    monkeypatch.setattr(store, target, barrier)
    result = finalizer.finalize(held.identity)
    assert replacement_thread is not None
    replacement_thread.join(timeout=_FINALIZER_JOIN_WATCHDOG_SECONDS)
    assert not replacement_thread.is_alive()
    assert failures == []
    record = store.get_claim_publication(held.identity.attempt_id)
    assert record is not None
    if phase == "c":
        assert result.status == "blocked"
        assert record.status is ClaimPublicationStatus.READY_TO_PUBLISH
    else:
        assert result.status == "published"
        assert record.status is ClaimPublicationStatus.PUBLISHED


@pytest.mark.parametrize("phase", ("c", "d"))
def test_finalizer_replacement_before_current_guard_leaves_old_c_or_d_unwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    finalizer, store, held = _prepared_authority_finalizer(tmp_path)
    if phase == "d":
        finalizer._issue_ready(finalizer._record(held.identity), now=NOW + timedelta(seconds=5))  # noqa: SLF001
    preimage = finalizer._preimage(finalizer._record(held.identity))  # noqa: SLF001
    replacement = LabShardClaimV2.model_validate(
        preimage.model_dump(mode="python")
        | {
            "claim_generation": preimage.claim_generation + 1,
            "claim_token": uuid4(),
            "scheduler_fencing_token": preimage.scheduler_fencing_token + 1,
        }
    )
    replacement_authority = PersistentCurrentClaimAuthority(
        finalizer._current_claim_authority.path,  # noqa: SLF001
        authority_id=finalizer._current_claim_authority.authority_id,  # noqa: SLF001
        signer=finalizer._current_claim_authority._signer,  # noqa: SLF001
        keyring=finalizer._keyring,  # noqa: SLF001
        mode="test-standalone",
    )
    target = "_ready_binding_for_record" if phase == "c" else "validate_ready_claim_for_publication"
    original = getattr(store, target)
    replaced = False

    def replace_before_guard(*args: object, **kwargs: object) -> object:
        nonlocal replaced
        if not replaced:
            replacement_authority.replace_current(replacement)
            replaced = True
        return original(*args, **kwargs)

    monkeypatch.setattr(store, target, replace_before_guard)
    assert finalizer.finalize(held.identity).status == "blocked"
    assert replaced
    record = store.get_claim_publication(held.identity.attempt_id)
    assert record is not None
    assert record.status is (
        ClaimPublicationStatus.SOURCE_QUEUED
        if phase == "c"
        else ClaimPublicationStatus.READY_TO_PUBLISH
    )


@pytest.mark.parametrize("phase", ("c", "d"))
@pytest.mark.parametrize("tamper", ("module", "class"))
def test_finalizer_current_guard_rejects_tampered_frozen_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    tamper: str,
) -> None:
    finalizer, store, held = _prepared_authority_finalizer(tmp_path)
    if phase == "d":
        ready = finalizer._issue_ready(  # noqa: SLF001
            finalizer._record(held.identity),  # noqa: SLF001
            now=NOW + timedelta(seconds=5),
        )
        assert ready.record.status is ClaimPublicationStatus.READY_TO_PUBLISH
    if tamper == "module":
        monkeypatch.setattr(
            lab_jobs,
            "_FROZEN_JOB_STORE_CURRENT_CLAIM_HOLD_CURRENT",
            lambda *args, **kwargs: nullcontext(),
        )
    else:
        monkeypatch.setattr(
            PersistentCurrentClaimAuthority,
            "hold_current",
            lambda *args, **kwargs: nullcontext(),
        )

    result = finalizer.finalize(held.identity)
    record = store.get_claim_publication(held.identity.attempt_id)
    assert result.status == "blocked"
    assert record is not None
    assert record.status is (
        ClaimPublicationStatus.SOURCE_QUEUED
        if phase == "c"
        else ClaimPublicationStatus.READY_TO_PUBLISH
    )

    class _DuckAuthority:
        pass

    with (
        pytest.raises(ClaimPublicationConflictError, match="untrusted"),
        lab_jobs.hold_trusted_current_claim(  # type: ignore[arg-type]
            _DuckAuthority(),
            binding=object(),  # type: ignore[arg-type]
            now=NOW,
        ),
    ):
        pass


def test_two_finalizers_race_for_one_durable_fence_and_stale_owner_cannot_write(
    tmp_path: Path,
) -> None:
    seed, store, held = _prepared_authority_finalizer(tmp_path)
    issuer = _finalizer_issuer(store)
    issuer.release(seed._authority, now=NOW + timedelta(seconds=5))  # noqa: SLF001
    barrier = Barrier(2)
    outcomes: list[tuple[str, str]] = []
    failures: list[BaseException] = []

    def contend(owner_id: str) -> None:
        try:
            local = LabJobStore(store.path, busy_timeout_ms=5_000)
            local.initialize()
            local_issuer = _finalizer_issuer(local)
            barrier.wait(timeout=2)
            try:
                authority = local_issuer.acquire(
                    owner_id=owner_id, lease_seconds=30, now=NOW + timedelta(seconds=6)
                )
            except ClaimPublicationConflictError:
                outcomes.append((owner_id, "fenced"))
                return
            finalizer = LabClaimFinalizer(
                ledger=local,
                stage_reader=seed._stage_reader,  # noqa: SLF001
                authority=authority,
                current_claim_authority=seed._current_claim_authority,  # noqa: SLF001
                keyring=seed._keyring,  # noqa: SLF001
                audience="lab-claim-publication",
                spool=seed._spool,  # noqa: SLF001
                spool_receipt_verifier=seed._spool_receipt_verifier,  # noqa: SLF001
                clock=lambda: NOW + timedelta(seconds=6),
            )
            outcomes.append((owner_id, finalizer.finalize(held.identity).status))
        except BaseException as exc:
            failures.append(exc)

    threads = tuple(
        Thread(target=contend, args=(owner,)) for owner in ("finalizer-a", "finalizer-b")
    )
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=_FINALIZER_JOIN_WATCHDOG_SECONDS)
        assert not thread.is_alive()

    assert failures == []
    assert sorted(status for _owner, status in outcomes) == ["fenced", "published"]
    record = store.get_claim_publication(held.identity.attempt_id)
    assert record is not None and record.status is ClaimPublicationStatus.PUBLISHED
    assert record.final_claim_bytes is not None and record.spool_receipt_bytes is not None
    audit = store.list_claim_publication_audit(held.identity.attempt_id)
    assert (
        sum(item.reason_code == "finalizer_source_queued_to_ready_to_publish" for item in audit)
        == 1
    )
    assert sum(item.reason_code == "finalizer_ready_to_published" for item in audit) == 1

    winner = next(owner for owner, status in outcomes if status == "published")
    winner_authority = _finalizer_issuer(store).renew(
        _finalizer_issuer(store).acquire(
            owner_id=winner, lease_seconds=30, now=NOW + timedelta(seconds=7)
        ),
        lease_seconds=1,
        now=NOW + timedelta(seconds=7),
    )
    restarted_store = LabJobStore(store.path)
    restarted_store.initialize()
    takeover = _finalizer_issuer(restarted_store).acquire(
        owner_id="finalizer-restarted", lease_seconds=30, now=NOW + timedelta(seconds=9)
    )
    assert takeover.fencing_token > winner_authority.fencing_token
    with pytest.raises(ClaimPublicationConflictError, match="conflict"):
        _finalizer_issuer(store).renew(
            winner_authority, lease_seconds=30, now=NOW + timedelta(seconds=9)
        )


def test_two_finalizers_sharing_one_valid_capability_transition_once_then_replay(
    tmp_path: Path,
) -> None:
    seed, store, held = _prepared_authority_finalizer(tmp_path)
    barrier = Barrier(2)
    outcomes: list[str] = []
    failures: list[BaseException] = []

    def finalize_from_connection() -> None:
        try:
            local = LabJobStore(store.path, busy_timeout_ms=5_000)
            local.initialize()
            finalizer = LabClaimFinalizer(
                ledger=local,
                stage_reader=seed._stage_reader,  # noqa: SLF001
                authority=seed._authority,  # noqa: SLF001 - intentionally shared valid capability
                current_claim_authority=seed._current_claim_authority,  # noqa: SLF001
                keyring=seed._keyring,  # noqa: SLF001
                audience="lab-claim-publication",
                spool=seed._spool,  # noqa: SLF001
                spool_receipt_verifier=seed._spool_receipt_verifier,  # noqa: SLF001
                clock=lambda: NOW + timedelta(seconds=5),
            )
            barrier.wait(timeout=2)
            outcomes.append(finalizer.finalize(held.identity).status)
        except BaseException as exc:
            failures.append(exc)

    threads = tuple(Thread(target=finalize_from_connection) for _ in range(2))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=_FINALIZER_JOIN_WATCHDOG_SECONDS)
        assert not thread.is_alive()

    assert failures == []
    assert sorted(outcomes) == ["published", "replayed"]
    record = store.get_claim_publication(held.identity.attempt_id)
    assert record is not None and record.status is ClaimPublicationStatus.PUBLISHED
    audit = store.list_claim_publication_audit(held.identity.attempt_id)
    assert (
        sum(item.reason_code == "finalizer_source_queued_to_ready_to_publish" for item in audit)
        == 1
    )
    assert sum(item.reason_code == "finalizer_ready_to_published" for item in audit) == 1
    assert record.spool_receipt_bytes is not None


def test_shared_capability_finalizer_concurrency_replays_after_cas_race_stress(
    tmp_path: Path,
) -> None:
    """Each bounded two-connection race must converge to D plus one replay."""

    repetitions = 20
    shared_authorities = create_test_authorities(tmp_path / "shared-authorities")
    for iteration in range(repetitions):
        race_root = tmp_path / f"race-{iteration:02d}"
        race_root.mkdir(mode=0o700)
        seed, store, held = _prepared_authority_finalizer(
            race_root,
            authority_set=shared_authorities,
        )
        barrier = Barrier(2)
        outcomes: list[tuple[str, str]] = []
        failures: list[BaseException] = []

        def finalize_from_connection(
            *,
            _store: LabJobStore = store,
            _seed: LabClaimFinalizer = seed,
            _held: HeldDraft = held,
            _barrier: Barrier = barrier,
            _outcomes: list[tuple[str, str]] = outcomes,
            _failures: list[BaseException] = failures,
        ) -> None:
            try:
                local = LabJobStore(_store.path, busy_timeout_ms=5_000)
                local.initialize()
                finalizer = LabClaimFinalizer(
                    ledger=local,
                    stage_reader=_seed._stage_reader,  # noqa: SLF001
                    authority=_seed._authority,  # noqa: SLF001 - intentional shared capability
                    current_claim_authority=_seed._current_claim_authority,  # noqa: SLF001
                    keyring=_seed._keyring,  # noqa: SLF001
                    audience="lab-claim-publication",
                    spool=_seed._spool,  # noqa: SLF001
                    spool_receipt_verifier=_seed._spool_receipt_verifier,  # noqa: SLF001
                    clock=lambda: NOW + timedelta(seconds=5),
                )
                _barrier.wait(timeout=2)
                result = finalizer.finalize(_held.identity)
                _outcomes.append((result.status, result.reason))
            except BaseException as exc:
                _failures.append(exc)

        threads = tuple(Thread(target=finalize_from_connection) for _ in range(2))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=_FINALIZER_JOIN_WATCHDOG_SECONDS)
            assert not thread.is_alive(), f"race {iteration} left a finalizer thread alive"

        assert failures == [], f"race {iteration} raised {failures!r}"
        assert sorted(status for status, _reason in outcomes) == ["published", "replayed"], (
            iteration,
            outcomes,
        )
        record = store.get_claim_publication(held.identity.attempt_id)
        assert record is not None and record.status is ClaimPublicationStatus.PUBLISHED
        assert record.final_claim_bytes is not None and record.spool_receipt_bytes is not None
        audit = store.list_claim_publication_audit(held.identity.attempt_id)
        assert (
            sum(
                item.action is ClaimPublicationAuditAction.TRANSITIONED
                and item.reason_code == "finalizer_source_queued_to_ready_to_publish"
                for item in audit
            )
            == 1
        ), (iteration, audit)
        assert (
            sum(
                item.action is ClaimPublicationAuditAction.TRANSITIONED
                and item.reason_code == "finalizer_ready_to_published"
                for item in audit
            )
            == 1
        ), (iteration, audit)
        assert sum(item.action is ClaimPublicationAuditAction.CONFLICT for item in audit) == 0, (
            iteration,
            audit,
        )


def _tamper_finalizer_attestation(
    store: LabJobStore,
    held: HeldDraft,
    publication_status: ClaimPublicationStatus,
) -> None:
    """Flip one byte of a durable finalizer attestation for a fail-closed probe."""

    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            """
            SELECT attestation_bytes
            FROM lab_claim_publication_finalizer_attestation
            WHERE attempt_id = ? AND publication_status = ?
            """,
            (str(held.identity.attempt_id), publication_status.value),
        ).fetchone()
        assert row is not None
        original = bytes(row[0])
        tampered = original[:-1] + bytes([original[-1] ^ 0x01])
        assert tampered != original
        connection.execute(
            """
            UPDATE lab_claim_publication_finalizer_attestation
            SET attestation_bytes = ?
            WHERE attempt_id = ? AND publication_status = ?
            """,
            (tampered, str(held.identity.attempt_id), publication_status.value),
        )


@pytest.mark.parametrize(
    "window",
    ("ready_attestation", "ready_before_spool", "sidecar_before_published"),
)
def test_shared_capability_finalizer_replays_when_peer_publishes_before_each_d_window(
    tmp_path: Path,
    window: str,
) -> None:
    """A peer that commits D first must leave the stalled finalizer a replay.

    The interleaving is deterministic instead of load dependent: the slow
    finalizer parks on exactly one D-saga boundary and is released only after the
    peer's D transition is durable. On the pre-fix tree the ``ready_attestation``
    window returns ``blocked``/``authority_conflict`` on every run.
    """

    race_root = tmp_path / "race"
    race_root.mkdir(mode=0o700)
    seed, store, held = _prepared_authority_finalizer(race_root)
    parked = Event()
    published = Event()
    entered: list[str] = []

    def stall(name: str) -> None:
        if name != window:
            return
        entered.append(name)
        parked.set()
        assert published.wait(timeout=_FINALIZER_JOIN_WATCHDOG_SECONDS)

    class _PeerFinalizer(LabClaimFinalizer):
        @staticmethod
        def _after_published(record: LabClaimPublicationRecord) -> None:
            del record
            published.set()

    class _StalledFinalizer(LabClaimFinalizer):
        @staticmethod
        def _before_ready_attestation(record: LabClaimPublicationRecord) -> None:
            del record
            stall("ready_attestation")

        @staticmethod
        def _after_ready_before_spool(record: LabClaimPublicationRecord) -> None:
            del record
            stall("ready_before_spool")

        @staticmethod
        def _after_sidecar_before_published(record: LabClaimPublicationRecord) -> None:
            del record
            stall("sidecar_before_published")

    outcomes: dict[str, tuple[str, str]] = {}
    results: dict[str, LabClaimPublicationRecord | None] = {}
    failures: list[BaseException] = []

    def finalize_from_connection(name: str, finalizer_type: type[LabClaimFinalizer]) -> None:
        try:
            local = LabJobStore(store.path, busy_timeout_ms=5_000)
            local.initialize()
            finalizer = finalizer_type(
                ledger=local,
                stage_reader=seed._stage_reader,  # noqa: SLF001
                authority=seed._authority,  # noqa: SLF001 - intentional shared capability
                current_claim_authority=seed._current_claim_authority,  # noqa: SLF001
                keyring=seed._keyring,  # noqa: SLF001
                audience="lab-claim-publication",
                spool=seed._spool,  # noqa: SLF001
                spool_receipt_verifier=seed._spool_receipt_verifier,  # noqa: SLF001
                clock=lambda: NOW + timedelta(seconds=5),
            )
            result = finalizer.finalize(held.identity)
            outcomes[name] = (result.status, result.reason)
            results[name] = result.record
        except BaseException as exc:  # noqa: BLE001 - surfaced by the assertions below
            failures.append(exc)

    stalled_thread = Thread(target=finalize_from_connection, args=("stalled", _StalledFinalizer))
    stalled_thread.start()
    assert parked.wait(timeout=_FINALIZER_JOIN_WATCHDOG_SECONDS), "stall window never reached"
    peer_thread = Thread(target=finalize_from_connection, args=("peer", _PeerFinalizer))
    peer_thread.start()
    for thread in (peer_thread, stalled_thread):
        thread.join(timeout=_FINALIZER_JOIN_WATCHDOG_SECONDS)
        assert not thread.is_alive(), f"{window} left a finalizer thread alive"

    assert failures == [], f"{window} raised {failures!r}"
    assert entered == [window]
    assert sorted(status for status, _reason in outcomes.values()) == ["published", "replayed"], (
        window,
        outcomes,
    )
    assert outcomes["peer"] == ("published", "published")
    assert outcomes["stalled"] == ("replayed", "published_replay")
    assert all("authority_conflict" not in reason for _status, reason in outcomes.values())

    record = store.get_claim_publication(held.identity.attempt_id)
    assert record is not None
    assert record.status is ClaimPublicationStatus.PUBLISHED
    assert record.version == 3
    assert record.published_at is not None
    assert record.final_claim_bytes is not None
    assert record.spool_receipt_bytes is not None

    peer_record = results["peer"]
    stalled_record = results["stalled"]
    assert peer_record is not None and stalled_record is not None
    assert peer_record.final_claim_bytes == record.final_claim_bytes
    assert stalled_record.final_claim_bytes == record.final_claim_bytes
    assert peer_record.spool_receipt_bytes == record.spool_receipt_bytes
    assert stalled_record.spool_receipt_bytes == record.spool_receipt_bytes

    final_claim = strict_model_validate_canonical_json(LabShardClaimV2, record.final_claim_bytes)
    LabClaimPublicationWorkerVerifier(
        ledger=store,
        current_claim_authority=seed._current_claim_authority,  # noqa: SLF001
        keyring=seed._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool_receipt_verifier=seed._spool_receipt_verifier,  # noqa: SLF001
        trust_verifier=seed._authority._trust_verifier,  # noqa: SLF001
    ).require_published_claim(final_claim, now=NOW + timedelta(seconds=5))

    spool = seed._spool  # noqa: SLF001
    assert len(tuple(spool.pending_dir.glob("*.json"))) == 1
    sidecars = tuple(
        path
        for path in spool.publish_receipt_dir.glob("*.json")
        if path != spool.publish_receipt_authority_path
    )
    assert spool.publish_receipt_authority_path.is_file()
    assert len(sidecars) == 1
    assert sidecars[0].read_bytes() == record.spool_receipt_bytes
    assert record.spool_receipt_hash == _sha256(record.spool_receipt_bytes)

    audit = store.list_claim_publication_audit(held.identity.attempt_id)
    assert (
        sum(
            item.action is ClaimPublicationAuditAction.TRANSITIONED
            and item.reason_code == "finalizer_source_queued_to_ready_to_publish"
            for item in audit
        )
        == 1
    ), audit
    assert (
        sum(
            item.action is ClaimPublicationAuditAction.TRANSITIONED
            and item.reason_code == "finalizer_ready_to_published"
            for item in audit
        )
        == 1
    ), audit
    assert sum(item.action is ClaimPublicationAuditAction.CONFLICT for item in audit) == 0, audit


def test_ready_attestation_status_advance_is_a_recoverable_concurrency_outcome(
    tmp_path: Path,
) -> None:
    """A durable record past READY is a concurrency outcome, not a trust failure."""

    seed, store, held = _prepared_authority_finalizer(tmp_path)
    assert seed.finalize(held.identity).status == "published"
    trust_verifier = seed._authority._trust_verifier  # noqa: SLF001
    assert trust_verifier is not None

    with pytest.raises(InvalidClaimPublicationTransitionError, match="transition_not_allowed"):
        store.validate_finalizer_ready_attestation(
            held.identity,
            trust_verifier=trust_verifier,
            now=NOW + timedelta(seconds=5),
        )
    assert LabClaimFinalizer._is_recoverable_concurrency_error(  # noqa: SLF001
        InvalidClaimPublicationTransitionError("transition_not_allowed")
    )

    # The READY attestation row still exists and the PUBLISHED one still verifies:
    # the pre-check failed on status, not on the trust chain.
    store.validate_finalizer_published_attestation(
        held.identity,
        trust_verifier=trust_verifier,
        now=NOW + timedelta(seconds=5),
    )


def test_tampered_ready_attestation_stays_fail_closed_under_concurrency(tmp_path: Path) -> None:
    """A forged READY attestation blocks at its own stage and never enters recovery."""

    class _StopAfterReady(LabClaimFinalizer):
        @staticmethod
        def _after_ready_before_spool(record: LabClaimPublicationRecord) -> None:
            del record
            raise RuntimeError("stop after C")

    stopped, store, held = _prepared_authority_finalizer(tmp_path, finalizer_type=_StopAfterReady)
    assert stopped.finalize(held.identity).status == "blocked"
    ready = store.get_claim_publication(held.identity.attempt_id)
    assert ready is not None and ready.status is ClaimPublicationStatus.READY_TO_PUBLISH

    _tamper_finalizer_attestation(store, held, ClaimPublicationStatus.READY_TO_PUBLISH)

    finalizer = LabClaimFinalizer(
        ledger=store,
        stage_reader=stopped._stage_reader,  # noqa: SLF001
        authority=stopped._authority,  # noqa: SLF001
        current_claim_authority=stopped._current_claim_authority,  # noqa: SLF001
        keyring=stopped._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool=stopped._spool,  # noqa: SLF001
        spool_receipt_verifier=stopped._spool_receipt_verifier,  # noqa: SLF001
        clock=lambda: NOW + timedelta(seconds=5),
    )
    result = finalizer.finalize(held.identity)

    assert result.status == "blocked"
    # The stage tag localizes the failure to the pre-check that raised it and
    # proves the forged signature was never routed into the recovery loop.
    assert result.reason == "ready_attestation_signature_invalid"
    assert finalizer.metrics.replays == 0
    unchanged = store.get_claim_publication(held.identity.attempt_id)
    assert unchanged is not None
    assert unchanged.status is ClaimPublicationStatus.READY_TO_PUBLISH


def test_tampered_published_attestation_cannot_replay_through_the_worker_gate(
    tmp_path: Path,
) -> None:
    """The worker D gate is the only door into "replayed"; forging it fails closed."""

    seed, store, held = _prepared_authority_finalizer(tmp_path)
    assert seed.finalize(held.identity).status == "published"

    _tamper_finalizer_attestation(store, held, ClaimPublicationStatus.PUBLISHED)

    record = store.get_claim_publication(held.identity.attempt_id)
    assert record is not None and record.status is ClaimPublicationStatus.PUBLISHED
    with pytest.raises(LabClaimFinalizerError, match="publication_signature_invalid"):
        seed._published_replay(  # noqa: SLF001
            held.identity,
            record,
            now=NOW + timedelta(seconds=5),
            observation_reason="published_replay",
        )

    result = seed.finalize(held.identity)

    assert result.status == "blocked"
    assert result.reason == "publish_cas_finalization_blocked"
    assert seed.metrics.replays == 0


def test_finalizer_recoverable_error_whitelist_is_exactly_three_forms() -> None:
    """Pin the recovery door: CAS conflict, allowed transitions, SQLite busy only."""

    recoverable = LabClaimFinalizer._is_recoverable_concurrency_error  # noqa: SLF001

    assert recoverable(ClaimPublicationConflictError("publication_cas_conflict"))
    assert recoverable(InvalidClaimPublicationTransitionError("transition_not_allowed"))
    assert recoverable(InvalidClaimPublicationTransitionError("terminal_status_immutable"))
    assert recoverable(sqlite3.OperationalError("database is locked"))
    assert recoverable(sqlite3.OperationalError("database table is busy"))

    for message in (
        "finalizer_publication_signature_invalid",
        "finalizer_publication_signature_missing",
        "finalizer_authority_conflict",
        "finalizer_external_trust_invalid",
        "ready_content_conflict",
        "published_receipt_conflict",
        "ready_binding_conflict",
        "attempt_identity_conflict",
    ):
        assert not recoverable(ClaimPublicationConflictError(message)), message
    assert not recoverable(InvalidClaimPublicationTransitionError("some_other_transition"))
    assert not recoverable(sqlite3.OperationalError("no such table"))
    assert not recoverable(SchedulerLeaseFencedError("scheduler_lease_fenced"))
    assert not recoverable(LabClaimFinalizerError("publication_signature_invalid"))
    assert not recoverable(RuntimeError("business failure"))


def test_stage_tagged_blocked_reasons_stay_within_the_redaction_grammar() -> None:
    """Stage x category is a closed product that always satisfies the frozen pattern."""

    stages = get_args(lab_claim_finalizer._FinalizerStage)  # noqa: SLF001
    assert stages == (
        "record",
        "issue_ready",
        "ready_attestation",
        "ready_claim",
        "spool_publish",
        "publish_cas",
        "recovery",
        "observe",
    )
    categories = set(lab_claim_finalizer._REDACTED_CATEGORIES.values()) | {  # noqa: SLF001
        "current_claim_evidence_invalid",
        "canonical_evidence_invalid",
        "authority_conflict",
        "finalization_blocked",
    }
    for stage in stages:
        for category in categories:
            assert re.fullmatch(r"[a-z][a-z0-9_]{0,63}", f"{stage}_{category}") is not None

    redacted = lab_claim_finalizer._redacted_reason  # noqa: SLF001
    assert (
        redacted(
            ClaimPublicationConflictError("finalizer_publication_signature_invalid"),
            stage="ready_attestation",
        )
        == "ready_attestation_signature_invalid"
    )
    assert (
        redacted(ClaimPublicationConflictError("finalizer_authority_conflict"), stage="record")
        == "record_authority_conflict"
    )
    assert (
        redacted(ClaimPublicationConflictError("publication_cas_conflict"), stage="publish_cas")
        == "publish_cas_cas_conflict"
    )
    assert (
        redacted(RuntimeError("business failure"), stage="issue_ready")
        == "issue_ready_finalization_blocked"
    )


def test_finalizer_concurrency_recovery_replays_only_exact_published_terminal(
    tmp_path: Path,
) -> None:
    seed, _store, held = _prepared_authority_finalizer(tmp_path)
    assert seed.finalize(held.identity).status == "published"

    class _ConcurrentTerminalMismatchFinalizer(LabClaimFinalizer):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)  # type: ignore[arg-type]
            self._first_read = True

        def _record(self, identity: LabClaimPublicationIdentity) -> LabClaimPublicationRecord:
            record = super()._record(identity)
            if self._first_read:
                self._first_read = False
                return record.model_copy(update={"status": ClaimPublicationStatus.SOURCE_QUEUED})
            return record.model_copy(update={"final_claim_bytes": b"{}"})

        def _issue_ready(
            self, _record: LabClaimPublicationRecord, *, now: datetime
        ) -> LabClaimPublicationMutation:
            del now
            raise ClaimPublicationConflictError("publication_cas_conflict")

    mismatch = _ConcurrentTerminalMismatchFinalizer(
        ledger=seed._ledger,  # noqa: SLF001 - exercise the finalizer recovery boundary
        stage_reader=seed._stage_reader,  # noqa: SLF001
        authority=seed._authority,  # noqa: SLF001
        current_claim_authority=seed._current_claim_authority,  # noqa: SLF001
        keyring=seed._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool=seed._spool,  # noqa: SLF001
        spool_receipt_verifier=seed._spool_receipt_verifier,  # noqa: SLF001
        clock=lambda: NOW + timedelta(seconds=5),
    )

    result = mismatch.finalize(held.identity)

    assert result.status == "blocked"
    assert mismatch.metrics.replays == 0


def test_finalizer_business_error_never_enters_concurrent_replay_recovery(tmp_path: Path) -> None:
    seed, _store, held = _prepared_authority_finalizer(tmp_path)

    class _BusinessFailureFinalizer(LabClaimFinalizer):
        def _issue_ready(
            self, _record: LabClaimPublicationRecord, *, now: datetime
        ) -> LabClaimPublicationMutation:
            del now
            raise RuntimeError("business failure")

    failing = _BusinessFailureFinalizer(
        ledger=seed._ledger,  # noqa: SLF001 - exercise top-level failure handling
        stage_reader=seed._stage_reader,  # noqa: SLF001
        authority=seed._authority,  # noqa: SLF001
        current_claim_authority=seed._current_claim_authority,  # noqa: SLF001
        keyring=seed._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool=seed._spool,  # noqa: SLF001
        spool_receipt_verifier=seed._spool_receipt_verifier,  # noqa: SLF001
        clock=lambda: NOW + timedelta(seconds=5),
    )

    result = failing.finalize(held.identity)

    assert result.status == "blocked"
    assert failing.metrics.replays == 0


def test_observation_primary_failure_persists_and_idempotently_drains_redacted_outbox(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    finalizer, store, held = _prepared_authority_finalizer(tmp_path)
    original = store.finalizer_record_claim_publication_observation

    def fail_primary(*_args: object, **_kwargs: object) -> None:
        raise sqlite3.OperationalError("injected primary observation failure")

    monkeypatch.setattr(store, "finalizer_record_claim_publication_observation", fail_primary)
    result = finalizer.finalize(held.identity)

    assert result.status == "published"
    pending = store.list_claim_publication_finalizer_observation_degradations(
        held.identity.attempt_id
    )
    assert len(pending) == 2
    assert {item.event_type for item in pending} == {"ready", "published"}
    assert all(
        item.reason_code_hash == _sha256(item.reason_code.encode("ascii")) for item in pending
    )
    serialized = repr(pending)
    assert str(store.path) not in serialized
    assert "claim_preimage" not in serialized

    monkeypatch.setattr(store, "finalizer_record_claim_publication_observation", original)
    drained = store.finalizer_drain_claim_publication_observation_degradations(
        authority=finalizer._authority,  # noqa: SLF001 - same current finalizer lease
        now=NOW + timedelta(seconds=10),
        limit=10,
    )
    assert drained == 2
    assert (
        store.finalizer_drain_claim_publication_observation_degradations(
            authority=finalizer._authority,  # noqa: SLF001
            now=NOW + timedelta(seconds=10),
            limit=10,
        )
        == 0
    )
    assert (
        store.list_claim_publication_finalizer_observation_degradations(held.identity.attempt_id)
        == ()
    )
    observations = store.list_claim_publication_finalizer_observations(held.identity.attempt_id)
    assert {item[0] for item in observations} == {"ready", "published"}


def test_observation_primary_and_fallback_failure_raises_and_logs_redacted_degradation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_claim_finalizer as finalizer_module

    finalizer, store, held = _prepared_authority_finalizer(tmp_path)
    logged: list[tuple[object, ...]] = []

    def fail_primary(*_args: object, **_kwargs: object) -> None:
        raise sqlite3.OperationalError("primary includes /private/secret and payload")

    def fail_fallback(*_args: object, **_kwargs: object) -> None:
        raise sqlite3.OperationalError("fallback includes /private/secret and payload")

    monkeypatch.setattr(store, "finalizer_record_claim_publication_observation", fail_primary)
    monkeypatch.setattr(
        store,
        "finalizer_record_claim_publication_observation_degradation",
        fail_fallback,
        raising=False,
    )
    monkeypatch.setattr(
        finalizer_module.logger,
        "error",
        lambda *args, **_kwargs: logged.append(args),
    )

    with pytest.raises(RuntimeError, match="observation_persistence_degraded"):
        finalizer.finalize(held.identity)

    rendered = repr(logged)
    assert "primary includes" not in rendered
    assert "fallback includes" not in rendered
    assert "/private/secret" not in rendered


def test_claim_finalizer_daemon_real_run_once_has_narrow_surface_and_publishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_claim_finalizer_daemon import LabClaimFinalizerDaemon
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutStore
    from rquant.strict_json import canonical_model_json_bytes

    seed, store, held = _prepared_authority_finalizer(tmp_path)
    candidate = store.list_claim_publication_finalizer_candidates(limit=1)[0]
    rollout = FinalizerRolloutStore(tmp_path / "claim-finalizer-rollout.sqlite3")
    daemon = LabClaimFinalizerDaemon(
        ledger=store,
        stage_reader=seed._stage_reader,  # noqa: SLF001
        authority_issuer=_finalizer_issuer(store),
        current_claim_authority=seed._current_claim_authority,  # noqa: SLF001
        keyring=seed._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool=seed._spool,  # noqa: SLF001
        spool_receipt_verifier=seed._spool_receipt_verifier,  # noqa: SLF001
        owner_id="finalizer-replay",
        lease_seconds=60,
        max_publications_per_tick=1,
        poll_interval_ms=10,
        failure_backoff_seconds=1,
        failure_backoff_max_seconds=2,
        published_evidence_recorder=rollout.record_published,
        clock=lambda: NOW + timedelta(seconds=5),
    )

    result = daemon.run_once()

    assert result.candidates == 1
    assert result.published == 1
    assert result.replayed == 0
    record = store.get_claim_publication(held.identity.attempt_id)
    assert record is not None and record.status is ClaimPublicationStatus.PUBLISHED
    expected = (
        str(record.identity.attempt_id),
        hashlib.sha256(canonical_model_json_bytes(record.identity)).hexdigest(),
        canonical_model_json_bytes(record.identity).decode("utf-8"),
    )
    with sqlite3.connect(tmp_path / "claim-finalizer-rollout.sqlite3") as connection:
        assert connection.execute(
            "SELECT attempt_id, evidence_hash, publication_identity "
            "FROM finalizer_rollout_published"
        ).fetchall() == [expected]

    monkeypatch.setattr(
        store,
        "list_claim_publication_finalizer_candidates",
        lambda *, limit: (candidate,),
    )
    replay = daemon.run_once()
    assert replay.published == 0 and replay.replayed == 1
    with sqlite3.connect(tmp_path / "claim-finalizer-rollout.sqlite3") as connection:
        assert connection.execute(
            "SELECT attempt_id, evidence_hash, publication_identity "
            "FROM finalizer_rollout_published"
        ).fetchall() == [expected]
    surface = set(dir(daemon))
    assert {
        "provider",
        "provider_registry",
        "adapter",
        "adapter_registry",
        "runtime_client",
        "worker",
        "worker_client",
        "claim_worker",
    }.isdisjoint(surface)
    daemon.close()
    daemon.close()


def test_published_d_atomically_enqueues_exact_rollout_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    finalizer, store, held = _prepared_authority_finalizer(tmp_path)

    result = finalizer.finalize(held.identity)

    assert result.status == "published"
    identity_bytes = canonical_model_json_bytes(held.identity)
    pending = store.list_due_claim_publication_rollout_evidence(
        authority=finalizer._authority,  # noqa: SLF001 - exact current finalizer lease
        now=NOW + timedelta(seconds=5),
        limit=10,
    )
    assert len(pending) == 1
    assert pending[0].evidence.attempt_id == held.identity.attempt_id
    assert pending[0].evidence.evidence_hash == hashlib.sha256(identity_bytes).hexdigest()
    assert pending[0].evidence.publication_identity == identity_bytes.decode("utf-8")
    assert store.count_pending_claim_publication_rollout_evidence() == 1
    assert (
        store.finalizer_drain_claim_publication_observation_degradations(
            authority=finalizer._authority,  # noqa: SLF001
            now=NOW + timedelta(seconds=5),
            limit=10,
        )
        == 0
    )

    failed_root = tmp_path / "atomic-failure"
    failed_root.mkdir(mode=0o700)
    failed, failed_store, failed_held = _prepared_authority_finalizer(failed_root)

    def fail_outbox(*_args: object, **_kwargs: object) -> None:
        raise sqlite3.OperationalError("injected rollout outbox failure")

    monkeypatch.setattr(
        LabJobStore,
        "_insert_claim_publication_rollout_evidence_in_transaction",
        fail_outbox,
        raising=False,
    )
    assert failed.finalize(failed_held.identity).status == "blocked"
    rolled_back = failed_store.get_claim_publication(failed_held.identity.attempt_id)
    assert rolled_back is not None
    assert rolled_back.status is ClaimPublicationStatus.READY_TO_PUBLISH
    assert failed_store.count_pending_claim_publication_rollout_evidence() == 0


def test_rollout_evidence_outage_and_ack_crash_replay_from_durable_outbox(
    tmp_path: Path,
) -> None:
    from rquant.lab_claim_finalizer_daemon import LabClaimFinalizerDaemon
    from rquant.lab_claim_finalizer_runtime import FinalizerRolloutStore

    seed, store, held = _prepared_authority_finalizer(tmp_path)
    rollout = FinalizerRolloutStore(tmp_path / "claim-finalizer-rollout.sqlite3")
    clock_now = [NOW + timedelta(seconds=5)]
    recorder_available = [False]

    def recorder(*, attempt_id: str, evidence_hash: str, publication_identity: str) -> None:
        if not recorder_available[0]:
            raise sqlite3.OperationalError("rollout recorder unavailable /private/secret")
        rollout.record_published(
            attempt_id=attempt_id,
            evidence_hash=evidence_hash,
            publication_identity=publication_identity,
        )

    daemon = LabClaimFinalizerDaemon(
        ledger=store,
        stage_reader=seed._stage_reader,  # noqa: SLF001
        authority_issuer=_finalizer_issuer(store),
        current_claim_authority=seed._current_claim_authority,  # noqa: SLF001
        keyring=seed._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool=seed._spool,  # noqa: SLF001
        spool_receipt_verifier=seed._spool_receipt_verifier,  # noqa: SLF001
        owner_id="finalizer-replay",
        lease_seconds=60,
        max_publications_per_tick=1,
        poll_interval_ms=10,
        failure_backoff_seconds=1,
        failure_backoff_max_seconds=2,
        published_evidence_recorder=recorder,
        clock=lambda: clock_now[0],
    )

    first = daemon.run_once()

    assert first.published == 1
    assert first.rollout_evidence_recovered == 0
    assert rollout.published_count() == 0
    assert store.count_pending_claim_publication_rollout_evidence() == 1
    with sqlite3.connect(store.path) as connection:
        error_class, next_retry_at = connection.execute(
            "SELECT error_class, next_retry_at "
            "FROM lab_claim_publication_finalizer_observation_degradation "
            "WHERE reason_code = 'rollout_evidence_pending' AND drained_at IS NULL"
        ).fetchone()
    assert error_class == "OperationalError"
    assert "/private/secret" not in error_class
    assert datetime.fromisoformat(next_retry_at) > clock_now[0]

    recorder_available[0] = True
    clock_now[0] += timedelta(seconds=10)
    recovered = daemon.run_once()
    assert recovered.candidates == 0
    assert recovered.rollout_evidence_recovered == 1
    assert rollout.published_count() == 1
    assert store.count_pending_claim_publication_rollout_evidence() == 0
    daemon.close()

    crash_root = tmp_path / "ack-crash"
    crash_root.mkdir(mode=0o700)
    crash_seed, crash_store, crash_held = _prepared_authority_finalizer(crash_root)
    crash_rollout = FinalizerRolloutStore(tmp_path / "ack-crash-rollout.sqlite3")

    class _AckCrashDaemon(LabClaimFinalizerDaemon):
        @staticmethod
        def _after_rollout_evidence_recorded() -> None:
            raise RuntimeError("injected ack crash")

    crashing = _AckCrashDaemon(
        ledger=crash_store,
        stage_reader=crash_seed._stage_reader,  # noqa: SLF001
        authority_issuer=_finalizer_issuer(crash_store),
        current_claim_authority=crash_seed._current_claim_authority,  # noqa: SLF001
        keyring=crash_seed._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool=crash_seed._spool,  # noqa: SLF001
        spool_receipt_verifier=crash_seed._spool_receipt_verifier,  # noqa: SLF001
        owner_id="finalizer-replay",
        lease_seconds=60,
        max_publications_per_tick=1,
        poll_interval_ms=10,
        failure_backoff_seconds=1,
        failure_backoff_max_seconds=2,
        published_evidence_recorder=crash_rollout.record_published,
        clock=lambda: NOW + timedelta(seconds=5),
    )
    with pytest.raises(RuntimeError, match="ack crash"):
        crashing.run_once()
    assert crash_store.get_claim_publication(crash_held.identity.attempt_id).status is (
        ClaimPublicationStatus.PUBLISHED
    )
    assert crash_rollout.published_count() == 1
    assert crash_store.count_pending_claim_publication_rollout_evidence() == 1
    crashing.close()

    reopened_store = LabJobStore(crash_store.path)
    reopened = LabClaimFinalizerDaemon(
        ledger=reopened_store,
        stage_reader=crash_seed._stage_reader,  # noqa: SLF001
        authority_issuer=_finalizer_issuer(reopened_store),
        current_claim_authority=crash_seed._current_claim_authority,  # noqa: SLF001
        keyring=crash_seed._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool=crash_seed._spool,  # noqa: SLF001
        spool_receipt_verifier=crash_seed._spool_receipt_verifier,  # noqa: SLF001
        owner_id="finalizer-ack-replay",
        lease_seconds=60,
        max_publications_per_tick=1,
        poll_interval_ms=10,
        failure_backoff_seconds=1,
        failure_backoff_max_seconds=2,
        published_evidence_recorder=crash_rollout.record_published,
        clock=lambda: NOW + timedelta(seconds=10),
    )
    replayed = reopened.run_once()
    assert replayed.rollout_evidence_recovered == 1
    assert crash_rollout.published_count() == 1
    assert crash_store.count_pending_claim_publication_rollout_evidence() == 0
    reopened.close()


def test_complete_drain_requires_zero_pending_and_exact_published_evidence(
    tmp_path: Path,
) -> None:
    from rquant.lab_claim_finalizer_runtime import (
        FinalizerRolloutError,
        FinalizerRolloutPhase,
        FinalizerRolloutStore,
        require_lab_claim_finalizer_rollout_drain_ready,
    )

    finalizer, store, held = _prepared_authority_finalizer(tmp_path)
    assert finalizer.finalize(held.identity).status == "published"
    rollout = FinalizerRolloutStore(tmp_path / "drain-rollout.sqlite3")
    for phase in (
        FinalizerRolloutPhase.MATERIAL_INSTALLED,
        FinalizerRolloutPhase.PREFLIGHT_OK,
        FinalizerRolloutPhase.FINALIZER_READY,
        FinalizerRolloutPhase.V2_WORKERS_READY,
        FinalizerRolloutPhase.SCHEDULER_EMITS_V2,
    ):
        rollout.transition(phase, evidence=f"advance:{phase.value}")
    rollout.begin_rollback(evidence="stop-new-emits")

    with pytest.raises(FinalizerRolloutError, match="rollout evidence outbox"):
        require_lab_claim_finalizer_rollout_drain_ready(store, rollout)

    item = store.list_due_claim_publication_rollout_evidence(
        authority=finalizer._authority,  # noqa: SLF001
        now=NOW + timedelta(seconds=5),
        limit=1,
    )[0]
    rollout.record_published(
        attempt_id=str(item.evidence.attempt_id),
        evidence_hash=item.evidence.evidence_hash,
        publication_identity=item.evidence.publication_identity,
    )
    with pytest.raises(FinalizerRolloutError, match="rollout evidence outbox"):
        require_lab_claim_finalizer_rollout_drain_ready(store, rollout)
    store.finalizer_ack_claim_publication_rollout_evidence(
        item,
        authority=finalizer._authority,  # noqa: SLF001
        now=NOW + timedelta(seconds=5),
    )
    completed = rollout.complete_drain(evidence="audit-exact", job_store=store)
    assert completed.phase is FinalizerRolloutPhase.OFF
    assert rollout.published_count() == 1

    unknown_identity = held.identity.model_copy(
        update={"attempt_id": uuid4(), "claim_token": uuid4()}
    )
    unknown_identity = unknown_identity.model_copy(
        update={"claim_token": unknown_identity.attempt_id}
    )
    unknown_bytes = canonical_model_json_bytes(unknown_identity)
    rollout.record_published(
        attempt_id=str(unknown_identity.attempt_id),
        evidence_hash=hashlib.sha256(unknown_bytes).hexdigest(),
        publication_identity=unknown_bytes.decode("utf-8"),
    )
    with pytest.raises(FinalizerRolloutError, match="published evidence set differs"):
        require_lab_claim_finalizer_rollout_drain_ready(store, rollout)


def test_claim_finalizer_daemon_failure_backoff_is_bounded_redacted_and_closes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_claim_finalizer_daemon as daemon_module

    seed, store, _held = _prepared_authority_finalizer(tmp_path)
    logged: list[tuple[object, ...]] = []

    class StopAfterWait:
        def __init__(self) -> None:
            self.stopped = False
            self.waits: list[float] = []

        def is_set(self) -> bool:
            return self.stopped

        def wait(self, timeout: float) -> bool:
            self.waits.append(timeout)
            self.stopped = True
            return True

        def set(self) -> None:
            self.stopped = True

    class FailingDaemon(daemon_module.LabClaimFinalizerDaemon):
        def run_once(self) -> object:
            raise RuntimeError("secret payload at /private/provider")

    daemon = FailingDaemon(
        ledger=store,
        stage_reader=seed._stage_reader,  # noqa: SLF001
        authority_issuer=_finalizer_issuer(store),
        current_claim_authority=seed._current_claim_authority,  # noqa: SLF001
        keyring=seed._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool=seed._spool,  # noqa: SLF001
        spool_receipt_verifier=seed._spool_receipt_verifier,  # noqa: SLF001
        owner_id="finalizer-replay",
        lease_seconds=60,
        max_publications_per_tick=1,
        poll_interval_ms=10,
        failure_backoff_seconds=1,
        failure_backoff_max_seconds=2,
        clock=lambda: NOW + timedelta(seconds=5),
    )
    stop = StopAfterWait()
    daemon._stop = stop  # type: ignore[assignment]  # noqa: SLF001
    monkeypatch.setattr(
        daemon_module.logger,
        "error",
        lambda *args, **_kwargs: logged.append(args),
    )

    daemon.run_forever()

    assert stop.waits == [1]
    assert daemon._closed is True  # noqa: SLF001
    assert "/private/provider" not in repr(logged)


def test_actual_worker_executes_published_v2_through_full_worker_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import _IsolatedExecutionControl, _IsolatedExecutionOutcome

    finalizer, store, held = _prepared_execution_finalizer(tmp_path)
    published = finalizer.finalize(held.identity)
    assert published.status == "published"
    verifier = LabClaimPublicationWorkerVerifier(
        ledger=store,
        current_claim_authority=finalizer._current_claim_authority,  # noqa: SLF001
        keyring=finalizer._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool_receipt_verifier=finalizer._spool_receipt_verifier,  # noqa: SLF001
        trust_verifier=finalizer._authority._trust_verifier,  # noqa: SLF001
    )
    registry = RecordingRegistry()
    worker = _worker(
        tmp_path,
        worker_id="held-worker",
        registry=registry,
        claims=finalizer._spool,  # noqa: SLF001
        reports=LabReportSpool(tmp_path / "reports"),
        claim_publication_verifier=verifier,
        v2_claim_publication_enabled=True,
        clock=lambda: NOW + timedelta(seconds=5),
        receipt_waiter=lambda report, _timeout, _stop: LabReportReceipt.from_report(
            report,
            status="accepted",
            reason="accepted",
            accepted_at=NOW + timedelta(seconds=5),
        ),
    )
    try:
        monkeypatch.setattr(
            worker,
            "_execute_shard_isolated",
            lambda _claim, validated, **_kwargs: _IsolatedExecutionControl(
                outcome=_IsolatedExecutionOutcome(
                    result=registry.execute_shard(validated, object())
                )
            ),
        )
        result = worker.run_once()

        reports = worker.report_spool.pending()
        assert result.status == "succeeded", reports[0].report.body
        assert worker.claim_spool.pending() == ()
        assert reports
        assert registry.executions == 1
    finally:
        worker.close()


def test_actual_worker_rejects_unpublished_or_foreign_v2_before_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    local.mkdir(mode=0o700)
    finalizer, store, held = _prepared_authority_finalizer(local)
    ready = finalizer._issue_ready(finalizer._record(held.identity), now=NOW + timedelta(seconds=5))  # noqa: SLF001
    final_claim = store.validate_ready_claim_for_publication(
        held.identity,
        current_claim_authority=finalizer._current_claim_authority,  # noqa: SLF001
        keyring=finalizer._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        now=NOW + timedelta(seconds=5),
    )
    assert ready.record.status is ClaimPublicationStatus.READY_TO_PUBLISH
    finalizer._spool.publish(final_claim)  # noqa: SLF001
    verifier = LabClaimPublicationWorkerVerifier(
        ledger=store,
        current_claim_authority=finalizer._current_claim_authority,  # noqa: SLF001
        keyring=finalizer._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool_receipt_verifier=finalizer._spool_receipt_verifier,  # noqa: SLF001
        trust_verifier=finalizer._authority._trust_verifier,  # noqa: SLF001
    )
    worker = _worker(
        tmp_path,
        worker_id="held-worker",
        claims=finalizer._spool,  # noqa: SLF001
        reports=LabReportSpool(tmp_path / "reports"),
        claim_publication_verifier=verifier,
        v2_claim_publication_enabled=True,
        clock=lambda: NOW + timedelta(seconds=5),
    )
    calls: list[str] = []
    reserve = worker._reserve_resource_admission  # noqa: SLF001
    consume = worker._consume_selected_claim  # noqa: SLF001
    execute = worker._execute_shard_isolated  # noqa: SLF001
    monkeypatch.setattr(
        worker,
        "_reserve_resource_admission",
        lambda *args, **kwargs: (calls.append("admission"), reserve(*args, **kwargs))[1],
    )
    monkeypatch.setattr(
        worker,
        "_consume_selected_claim",
        lambda *args, **kwargs: (calls.append("consume"), consume(*args, **kwargs))[1],
    )
    monkeypatch.setattr(
        worker,
        "_execute_shard_isolated",
        lambda *args, **kwargs: (calls.append("authority_child"), execute(*args, **kwargs))[1],
    )

    foreign_worker = None
    try:
        result = worker.run_once()

        assert result.status == "idle"
        assert calls == []
        assert len(worker.claim_spool.pending()) == 1

        foreign_root = tmp_path / "foreign"
        foreign_root.mkdir(mode=0o700)
        foreign_finalizer, _foreign_store, foreign_held = _prepared_authority_finalizer(
            foreign_root
        )
        assert foreign_finalizer.finalize(foreign_held.identity).status == "published"
        foreign_worker = _worker(
            tmp_path,
            worker_id="held-worker",
            claims=foreign_finalizer._spool,  # noqa: SLF001
            reports=LabReportSpool(tmp_path / "foreign-reports"),
            claim_publication_verifier=verifier,
            v2_claim_publication_enabled=True,
            clock=lambda: NOW + timedelta(seconds=5),
        )
        foreign_reserve = foreign_worker._reserve_resource_admission  # noqa: SLF001
        foreign_consume = foreign_worker._consume_selected_claim  # noqa: SLF001
        foreign_execute = foreign_worker._execute_shard_isolated  # noqa: SLF001
        monkeypatch.setattr(
            foreign_worker,
            "_reserve_resource_admission",
            lambda *args, **kwargs: (
                calls.append("foreign-admission"),
                foreign_reserve(*args, **kwargs),
            )[1],
        )
        monkeypatch.setattr(
            foreign_worker,
            "_consume_selected_claim",
            lambda *args, **kwargs: (
                calls.append("foreign-consume"),
                foreign_consume(*args, **kwargs),
            )[1],
        )
        monkeypatch.setattr(
            foreign_worker,
            "_execute_shard_isolated",
            lambda *args, **kwargs: (
                calls.append("foreign-authority-child"),
                foreign_execute(*args, **kwargs),
            )[1],
        )

        foreign_result = foreign_worker.run_once()

        assert foreign_result.status == "idle"
        assert calls == []
        assert len(foreign_worker.claim_spool.pending()) == 1
    finally:
        if foreign_worker is not None:
            foreign_worker.close()
        worker.close()


def test_actual_worker_rejects_raw_sql_forged_published_attestation_before_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    finalizer, store, held = _prepared_authority_finalizer(tmp_path)
    assert finalizer.finalize(held.identity).status == "published"
    with sqlite3.connect(store.path) as connection:
        forged = b'{"signature":"forged"}'
        connection.execute(
            "UPDATE lab_claim_publication_finalizer_attestation "
            "SET attestation_bytes = ?, attestation_hash = ? "
            "WHERE attempt_id = ? AND publication_status = 'PUBLISHED'",
            (forged, _sha256(forged), str(held.identity.attempt_id)),
        )
    verifier = LabClaimPublicationWorkerVerifier(
        ledger=store,
        current_claim_authority=finalizer._current_claim_authority,  # noqa: SLF001
        keyring=finalizer._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool_receipt_verifier=finalizer._spool_receipt_verifier,  # noqa: SLF001
        trust_verifier=finalizer._authority._trust_verifier,  # noqa: SLF001
    )
    worker = _worker(
        tmp_path,
        worker_id="held-worker",
        claims=finalizer._spool,  # noqa: SLF001
        reports=LabReportSpool(tmp_path / "reports"),
        claim_publication_verifier=verifier,
        v2_claim_publication_enabled=True,
        clock=lambda: NOW + timedelta(seconds=5),
    )
    calls: list[str] = []
    try:
        for method in (
            "_reserve_resource_admission",
            "_consume_selected_claim",
            "_execute_shard_isolated",
        ):
            original = getattr(worker, method)
            monkeypatch.setattr(
                worker,
                method,
                lambda *args, _method=method, _original=original, **kwargs: (
                    calls.append(_method),
                    _original(*args, **kwargs),
                )[1],
            )
        assert worker.run_once().status == "idle"
        assert calls == []
    finally:
        worker.close()


@pytest.mark.parametrize("replacement", ("stale", "forged"))
def test_actual_worker_rejects_replaced_current_v2_before_any_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    finalizer, store, held = _prepared_authority_finalizer(tmp_path)
    assert finalizer.finalize(held.identity).status == "published"
    record = store.get_claim_publication(held.identity.attempt_id)
    assert record is not None
    preimage = strict_model_validate_canonical_json(LabShardClaimV2, record.claim_preimage_bytes)
    replacement_claim = LabShardClaimV2.model_validate(
        preimage.model_dump(mode="python")
        | {
            "claim_generation": preimage.claim_generation + 1,
            "claim_token": uuid4(),
            "scheduler_fencing_token": preimage.scheduler_fencing_token
            + (1 if replacement == "stale" else 2),
        }
    )
    finalizer._current_claim_authority.replace_current(replacement_claim)  # noqa: SLF001
    verifier = LabClaimPublicationWorkerVerifier(
        ledger=store,
        current_claim_authority=finalizer._current_claim_authority,  # noqa: SLF001
        keyring=finalizer._keyring,  # noqa: SLF001
        audience="lab-claim-publication",
        spool_receipt_verifier=finalizer._spool_receipt_verifier,  # noqa: SLF001
        trust_verifier=finalizer._authority._trust_verifier,  # noqa: SLF001
    )
    worker = _worker(
        tmp_path,
        worker_id="held-worker",
        claims=finalizer._spool,  # noqa: SLF001
        reports=LabReportSpool(tmp_path / "reports"),
        claim_publication_verifier=verifier,
        v2_claim_publication_enabled=True,
        clock=lambda: NOW + timedelta(seconds=5),
    )
    calls: list[str] = []
    for method in (
        "_reserve_resource_admission",
        "_consume_selected_claim",
        "_execute_shard_isolated",
    ):
        original = getattr(worker, method)
        monkeypatch.setattr(
            worker,
            method,
            lambda *args, _method=method, _original=original, **kwargs: (
                calls.append(_method),
                _original(*args, **kwargs),
            )[1],
        )

    try:
        assert worker.run_once().status == "idle"
        assert calls == []
        assert len(worker.claim_spool.pending()) == 1
    finally:
        worker.close()
