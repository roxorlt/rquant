from __future__ import annotations

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from rquant.adapter_manifest import (
    ADAPTER_MANIFEST_NAMESPACE,
    AdapterManifest,
    AdapterManifestTemporalPolicyV2,
)
from rquant.lab_shard_protocol import LabShardClaimV2, LabShardDefinition, StrategyShardPayloadV2
from rquant.lab_source_stage import LabSourceStageBinding, LabSourceStageStore
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2AuthorityRef,
    SourceBrokerV2JobIntentEnvelope,
)
from rquant.source_operation_contracts import (
    SourceAttemptBindingV2,
    SourceBrokerV2PublicRequest,
    SourceBrokerV2SchedulerIntentTemplate,
    SourceIntentV2,
    SourceResourceRequestV2,
    build_source_broker_v2_scheduler_intent,
    issue_scheduler_intent_authorization_v1,
)

from .test_adapter_manifest import Authorities, create_test_authorities, signed_manifest

_AUTHORITIES: Authorities | None = None
_CLAIMS_BY_OPERATION_ID: dict[str, LabShardClaimV2] = {}
_WRITERS_BY_STAGE_PATH: dict[Path, object] = {}


def authorities() -> Authorities:
    global _AUTHORITIES
    if _AUTHORITIES is None:
        _AUTHORITIES = create_test_authorities(Path(tempfile.mkdtemp(prefix="rquant-intent-auth-")))
    return _AUTHORITIES


def authorized_intent(
    *,
    source_authority: SourceBrokerV2AuthorityRef | None = None,
    symbol: str = "000001.SZ",
    now: datetime | None = None,
) -> SourceBrokerV2JobIntentEnvelope:
    payload, claim = authorized_payload_and_claim(
        source_authority=source_authority,
        symbol=symbol,
        now=now,
    )
    current = claim.claimed_at
    authority_set = authorities()
    intent = build_source_broker_v2_scheduler_intent(
        payload,
        claim=claim,
        manifest_keyring=authority_set.authorization_keyring,
        authorization_keyring=authority_set.authorization_keyring,
        deadline=current + timedelta(seconds=60),
        now=current,
    )
    _CLAIMS_BY_OPERATION_ID[intent.operation_id] = claim
    return intent


def authorized_intent_from_payload_and_claim(
    payload: StrategyShardPayloadV2,
    claim: LabShardClaimV2,
) -> SourceBrokerV2JobIntentEnvelope:
    """Build a real signed scheduler intent for a supplied exact claim."""

    authority_set = authorities()
    intent = build_source_broker_v2_scheduler_intent(
        payload,
        claim=claim,
        manifest_keyring=authority_set.authorization_keyring,
        authorization_keyring=authority_set.authorization_keyring,
        deadline=claim.claimed_at + timedelta(seconds=60),
        now=claim.claimed_at,
    )
    _CLAIMS_BY_OPERATION_ID[intent.operation_id] = claim
    return intent


def stage_authorized_intent(
    stage_store: LabSourceStageStore,
    intent: SourceBrokerV2JobIntentEnvelope,
    *,
    now: datetime | None = None,
) -> None:
    claim = _CLAIMS_BY_OPERATION_ID[intent.operation_id]
    current = (now or datetime.now(UTC)).astimezone(UTC)
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
    writer = _WRITERS_BY_STAGE_PATH.get(stage_store.path)
    if writer is None:
        writer = stage_store.acquire_writer_lease(
            owner_id="stage-authority-test",
            lease_seconds=120,
            now=current,
        )
        _WRITERS_BY_STAGE_PATH[stage_store.path] = writer
    stage_store.enqueue_external(binding, intent, lease=writer, now=current)


def authorized_payload_and_claim(
    *,
    source_authority: SourceBrokerV2AuthorityRef | None = None,
    symbol: str = "000001.SZ",
    now: datetime | None = None,
    job_id: UUID | None = None,
    spec_hash: str = "a" * 64,
    attempt_id: UUID | None = None,
    claim_generation: int = 1,
    scheduler_fencing_token: int = 1,
    worker_id: str = "lab-worker-a",
    plan_hash: str = "b" * 64,
    shard_index: int = 1,
    payload_json: str = '{"partition":"test"}',
    authority_set: Authorities | None = None,
    manifest: AdapterManifest | None = None,
) -> tuple[StrategyShardPayloadV2, LabShardClaimV2]:
    """Build one exact signed payload and its unbound claim for execution tests."""

    current = (now or datetime.now(UTC)).astimezone(UTC)
    authority_set = authority_set or authorities()
    base_manifest = manifest or signed_manifest(authority_set)
    unsigned_manifest = base_manifest.model_copy(
        update={
            "source": "daily-bars",
            "temporal_policy": AdapterManifestTemporalPolicyV2(
                valid_from=current - timedelta(minutes=1),
                expires_at=current + timedelta(minutes=10),
                availability_lag_seconds=0,
            ),
            "signature": "",
        }
    )
    manifest = unsigned_manifest.model_copy(
        update={
            "signature": authority_set.manifest_active.sign(
                namespace=ADAPTER_MANIFEST_NAMESPACE,
                payload=unsigned_manifest.signing_bytes(),
            )
        }
    )
    source_intent = SourceIntentV2.from_manifest(
        manifest,
        resource_request=SourceResourceRequestV2.from_manifest(manifest, requested_calls=1),
    )
    source_ref = source_authority or _authority("source")
    template = SourceBrokerV2SchedulerIntentTemplate.from_source_intent(
        source_intent=source_intent,
        source_id="daily-bars",
        request=SourceBrokerV2PublicRequest(
            symbols=(symbol,),
            requested_start=current.date(),
            requested_end=current.date(),
            as_of=current.date(),
            fields=("close",),
        ),
        deadline_offset_seconds=60,
        saga_id="saga-daily-bars",
        source_authority=source_ref,
        claim_authority=_authority("claim"),
        quota_parent_id="quota-parent-daily-bars",
        quota_authority=_authority("quota"),
        lineage_id="lineage-daily-bars",
        lineage_authority=_authority("lineage"),
        fence_external_root_hash=source_ref.fence_hash,
    )
    unsigned_payload = StrategyShardPayloadV2.from_source_intent(
        adapter_id=manifest.adapter_id,
        adapter_version=manifest.adapter_version,
        payload_json=payload_json,
        source_intent=source_intent,
        scheduler_intent_template=template,
    )
    payload = unsigned_payload.with_scheduler_intent_authorization(
        issue_scheduler_intent_authorization_v1(
            unsigned_payload,
            signer=authority_set.scheduler_intent,
            valid_from=current - timedelta(seconds=1),
            expires_at=current + timedelta(minutes=5),
        )
    )
    definition = LabShardDefinition.from_payload(
        shard_index=shard_index,
        adapter_id=payload.adapter_id,
        adapter_version=payload.adapter_version,
        plan_hash=plan_hash,
        payload_json=payload.model_dump_json(round_trip=True),
    )
    binding = SourceAttemptBindingV2(
        job_id=job_id or uuid4(),
        spec_hash=spec_hash,
        shard_id=definition.shard_id,
        attempt_id=attempt_id or uuid4(),
        claim_generation=claim_generation,
        scheduler_fencing_token=scheduler_fencing_token,
        worker_id=worker_id,
    )
    claim = LabShardClaimV2.from_current_attempt(
        definition=definition,
        attempt_binding=binding,
        claimed_at=current,
        lease_expires_at=current + timedelta(minutes=2),
    )
    return payload, claim


def _authority(kind: str) -> SourceBrokerV2AuthorityRef:
    return SourceBrokerV2AuthorityRef(
        authority_id=f"{kind}-authority",
        key_id=f"{kind}-key-v2",
        purpose=f"rquant-{kind}-receipt",
        schema_version=2,
        generation=7,
        fence_hash="7" * 64,
    )
