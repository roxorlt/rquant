from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event, Lock
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant.adapter_manifest import SOURCE_USE_PLAN_NAMESPACE, SOURCE_USE_PLAN_V2_NAMESPACE
from rquant.lab_shard_protocol import (
    LabShardClaimV2,
    LabShardDefinition,
    StrategyShardPayloadV1,
    StrategyShardPayloadV2,
    parse_strategy_shard_payload,
    require_external_strategy_shard_payload,
)
from rquant.source_broker_v2_job_protocol import SourceBrokerV2AuthorityRef
from rquant.source_operation_contracts import (
    CurrentClaimAuthorityProtocol,
    CurrentClaimConsumptionBindingV2,
    CurrentClaimConsumptionV2,
    CurrentClaimPlanIssueV2,
    CurrentClaimPlanSignerIdentityV2,
    SchedulerIntentAuthorizationV1,
    SourceAttemptBindingV2,
    SourceBrokerV2PublicRequest,
    SourceBrokerV2SchedulerIntentTemplate,
    SourceIntentV2,
    SourceOperationContractError,
    SourceResourceRequestV2,
    SourceUsePlanV2,
    issue_scheduler_intent_authorization_v1,
    require_current_claim_plan_issue_v2,
    require_source_use_plan_v2,
    sign_source_use_plan_v2,
)
from rquant.strict_json import canonical_json_bytes, canonical_model_json_bytes

from .test_adapter_manifest import NOW, Authorities, create_test_authorities, signed_manifest

ATTEMPT_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
OPERATION_ID = "1" * 64
OTHER_OPERATION_ID = "2" * 64


def _intent(authorities: Authorities) -> SourceIntentV2:
    manifest = signed_manifest(authorities)
    request = SourceResourceRequestV2.from_manifest(manifest, requested_calls=1)
    return SourceIntentV2.from_manifest(manifest, resource_request=request)


def test_scheduler_intent_authorization_v1_has_a_canonical_temporal_preimage() -> None:
    authorization = SchedulerIntentAuthorizationV1(
        authority_id="lab-intent-authority",
        issuer="lab-intent-authority",
        key_id="intent-v1",
        payload_commitment="1" * 64,
        template_commitment="2" * 64,
        public_request_commitment="3" * 64,
        manifest_commitment="4" * 64,
        source_contract_commitment="5" * 64,
        resource_quota_commitment="6" * 64,
        lineage_authority_commitment="7" * 64,
        valid_from=NOW,
        expires_at=NOW + timedelta(minutes=1),
        signature="",
    )

    assert authorization.signing_preimage() == authorization.signing_bytes()
    assert b'"schema_version":1' in authorization.signing_preimage()
    with pytest.raises(ValidationError, match="expires_at"):
        SchedulerIntentAuthorizationV1.model_validate(
            authorization.model_dump() | {"expires_at": NOW}
        )


def test_scheduler_intent_authorization_is_issued_by_signer_and_verified_by_public_keyring(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    unsigned_payload = _scheduler_payload(authorities)

    authorization = issue_scheduler_intent_authorization_v1(
        unsigned_payload,
        signer=authorities.scheduler_intent,
        valid_from=NOW,
        expires_at=NOW + timedelta(minutes=1),
    )

    authorization.require_verified(authorities.authorization_keyring, now=NOW)
    with pytest.raises(ValueError, match="not currently valid"):
        authorization.require_verified(
            authorities.authorization_keyring,
            now=NOW + timedelta(minutes=1),
        )


def test_scheduler_intent_authorization_binds_the_payload_without_self_reference(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    unsigned_payload = _scheduler_payload(authorities)
    authorization = issue_scheduler_intent_authorization_v1(
        unsigned_payload,
        signer=authorities.scheduler_intent,
        valid_from=NOW,
        expires_at=NOW + timedelta(minutes=1),
    )

    authorized_payload = unsigned_payload.with_scheduler_intent_authorization(authorization)

    assert authorized_payload.scheduler_intent_authorization == authorization
    assert authorized_payload.authorization_payload_commitment == authorization.payload_commitment


def _payload(authorities: Authorities) -> StrategyShardPayloadV2:
    intent = _intent(authorities)
    return StrategyShardPayloadV2.from_source_intent(
        adapter_id=intent.manifest.adapter_id,
        adapter_version=intent.manifest.adapter_version,
        payload_json='{"partition":"2026-08-05"}',
        source_intent=intent,
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


def _scheduler_payload(authorities: Authorities) -> StrategyShardPayloadV2:
    bare = _payload(authorities)
    template = SourceBrokerV2SchedulerIntentTemplate.from_source_intent(
        source_intent=bare.source_intent,
        source_id=bare.source_intent.manifest.source or "",
        request=SourceBrokerV2PublicRequest(
            symbols=("000001.SZ",),
            requested_start=date(2026, 8, 5),
            requested_end=date(2026, 8, 5),
            as_of=date(2026, 8, 5),
            fields=("close",),
        ),
        deadline_offset_seconds=60,
        saga_id="saga-daily-bars",
        source_authority=_authority("source"),
        claim_authority=_authority("claim"),
        quota_parent_id="quota-parent-daily-bars",
        quota_authority=_authority("quota"),
        lineage_id="lineage-daily-bars",
        lineage_authority=_authority("lineage"),
        fence_external_root_hash="8" * 64,
    )
    return StrategyShardPayloadV2.model_validate(
        {
            **bare.model_dump(mode="json"),
            "scheduler_intent_template": template.model_dump(mode="json"),
        }
    )


def _definition(
    authorities: Authorities,
    *,
    adapter_id: str | None = None,
    adapter_version: str | None = None,
    payload: StrategyShardPayloadV2 | None = None,
) -> LabShardDefinition:
    source_payload = payload or _payload(authorities)
    return LabShardDefinition.from_payload(
        shard_index=2,
        adapter_id=adapter_id or source_payload.adapter_id,
        adapter_version=adapter_version or source_payload.adapter_version,
        plan_hash="b" * 64,
        payload_json=source_payload.model_dump_json(round_trip=True),
    )


def _binding(
    *,
    shard_id: UUID,
    attempt_id: UUID = ATTEMPT_ID,
    generation: int = 3,
    fence: int = 9,
    worker_id: str = "lab-worker-a",
) -> SourceAttemptBindingV2:
    return SourceAttemptBindingV2(
        job_id=UUID("11111111-2222-3333-4444-555555555555"),
        spec_hash="a" * 64,
        shard_id=shard_id,
        attempt_id=attempt_id,
        claim_generation=generation,
        scheduler_fencing_token=fence,
        worker_id=worker_id,
    )


def _claim(
    authorities: Authorities,
    *,
    definition: LabShardDefinition | None = None,
    attempt_id: UUID = ATTEMPT_ID,
    generation: int = 3,
    fence: int = 9,
    worker_id: str = "lab-worker-a",
) -> LabShardClaimV2:
    source_definition = definition or _definition(authorities)
    binding = _binding(
        shard_id=source_definition.shard_id,
        attempt_id=attempt_id,
        generation=generation,
        fence=fence,
        worker_id=worker_id,
    )
    return LabShardClaimV2.from_current_attempt(
        definition=source_definition,
        attempt_binding=binding,
        claimed_at=NOW,
        lease_expires_at=NOW + timedelta(minutes=5),
    )


class MemoryCurrentClaimAuthority(CurrentClaimAuthorityProtocol):
    def __init__(self, current_claim: LabShardClaimV2, authorities: Authorities) -> None:
        self._authority_id = "global-source-use"
        self.current_claim = current_claim
        self._plan_signer = authorities.plan_v2
        self._keyring = authorities.authorization_keyring
        self._lock = Lock()
        self.receipts: dict[str, CurrentClaimConsumptionV2] = {}
        self.issue_hashes: dict[str, str] = {}
        self.attempt_operations: dict[str, str] = {}
        self.signing_calls = 0
        self.fail_after_commit_once = False

    @property
    def authority_id(self) -> str:
        return self._authority_id

    @property
    def plan_signer_identity(self) -> CurrentClaimPlanSignerIdentityV2:
        return CurrentClaimPlanSignerIdentityV2(
            issuer=self._plan_signer.issuer,
            key_id=self._plan_signer.key_id,
        )

    def replace_current(self, claim: LabShardClaimV2) -> None:
        with self._lock:
            self.current_claim = claim

    def _require_current(
        self,
        *,
        binding: CurrentClaimConsumptionBindingV2,
        now: datetime,
    ) -> None:
        payload = self.current_claim.strategy_payload
        manifest = payload.source_intent.manifest
        expected_source_identity = (
            self.current_claim.definition.adapter_id,
            self.current_claim.definition.adapter_version,
            self.current_claim.adapter_code_hash,
            self.current_claim.definition.payload_hash,
            self.current_claim.payload_source_contract_hash,
            self.current_claim.manifest_hash,
            payload.source_intent.resource_request_hash,
        )
        observed_source_identity = (
            binding.adapter_id,
            binding.adapter_version,
            binding.adapter_code_hash,
            binding.payload_hash,
            binding.payload_source_contract_hash,
            binding.manifest_hash,
            binding.resource_request_hash,
        )
        if (
            binding.authority_id != self.authority_id
            or binding.attempt_binding != self.current_claim.attempt_binding
            or binding.lease_expires_at != self.current_claim.lease_expires_at
            or observed_source_identity != expected_source_identity
            or (binding.adapter_id, binding.adapter_version)
            != (manifest.adapter_id, manifest.adapter_version)
        ):
            raise SourceOperationContractError("claim is not the current authority high-water")
        if now < binding.not_before:
            raise SourceOperationContractError("current source plan is not active")
        if now >= min(binding.expires_at, binding.lease_expires_at):
            raise SourceOperationContractError("current source plan or claim lease is expired")

    def issue_plan_once(
        self,
        *,
        issue: CurrentClaimPlanIssueV2,
        now: datetime,
    ) -> CurrentClaimConsumptionV2:
        try:
            structurally_valid = CurrentClaimPlanIssueV2.model_validate(issue, strict=True)
        except ValidationError as exc:
            raise SourceOperationContractError(
                f"current claim issue contract is invalid: {exc}"
            ) from exc
        with self._lock:
            binding = structurally_valid.binding
            existing = self.receipts.get(binding.operation_id)
            if existing is not None:
                if self.issue_hashes[binding.operation_id] != structurally_valid.issue_hash:
                    raise SourceOperationContractError("operation_id consumption binding changed")
                return existing
            validated = require_current_claim_plan_issue_v2(
                structurally_valid,
                keyring=self._keyring,
                authority_id=self.authority_id,
                signer_identity=self.plan_signer_identity,
            )
            binding = validated.binding
            prior_operation = self.attempt_operations.get(binding.attempt_identity_hash)
            if prior_operation is not None:
                raise SourceOperationContractError(
                    "source attempt was already consumed by a different operation_id"
                )
            self._require_current(binding=binding, now=now)
            self.signing_calls += 1
            unsigned = validated.unsigned_plan
            signed_plan = unsigned.model_copy(
                update={
                    "signature": self._plan_signer.sign(
                        namespace=SOURCE_USE_PLAN_V2_NAMESPACE,
                        payload=unsigned.signing_bytes(),
                    )
                }
            )
            signed_plan = require_source_use_plan_v2(
                signed_plan,
                keyring=self._keyring,
                audience=unsigned.audience,
                now=now,
            )
            if signed_plan.signing_payload() != unsigned.signing_payload():
                raise SourceOperationContractError("authority signer rebound unsigned plan")
            self._require_current(binding=binding, now=now)
            receipt = CurrentClaimConsumptionV2.from_signed_plan(
                binding=binding,
                signed_plan=signed_plan,
                committed_at=now,
            )
            self.receipts[binding.operation_id] = receipt
            self.issue_hashes[binding.operation_id] = validated.issue_hash
            self.attempt_operations[binding.attempt_identity_hash] = binding.operation_id
            if self.fail_after_commit_once:
                self.fail_after_commit_once = False
                raise ConnectionError("current claim commit response was lost")
            return receipt

    def verify_current(
        self,
        *,
        binding: CurrentClaimConsumptionBindingV2,
        now: datetime,
    ) -> CurrentClaimConsumptionV2:
        self._require_current(binding=binding, now=now)
        receipt = self.receipts.get(binding.operation_id)
        if receipt is None or receipt.binding != binding:
            raise SourceOperationContractError("current claim has no exact committed operation")
        return receipt


def _plan(
    authorities: Authorities,
    *,
    claim: LabShardClaimV2,
    authority: MemoryCurrentClaimAuthority,
    operation_id: str = OPERATION_ID,
    nonce: str = "nonce-v2",
) -> SourceUsePlanV2:
    return sign_source_use_plan_v2(
        claim=claim,
        current_claim_authority=authority,
        keyring=authorities.authorization_keyring,
        operation_id=operation_id,
        audience="lab-broker-a",
        now=NOW,
        expires_at=NOW + timedelta(minutes=3),
        nonce=nonce,
    )


def _unsigned_issue(
    *,
    claim: LabShardClaimV2,
    authority: MemoryCurrentClaimAuthority,
    operation_id: str = OPERATION_ID,
    nonce: str = "nonce-v2",
) -> CurrentClaimPlanIssueV2:
    payload = claim.strategy_payload
    identity = authority.plan_signer_identity
    unsigned = SourceUsePlanV2.from_source_intent(
        payload.source_intent,
        issuer=identity.issuer,
        key_id=identity.key_id,
        attempt_binding=claim.attempt_binding,
        adapter_id=claim.definition.adapter_id,
        adapter_version=claim.definition.adapter_version,
        adapter_code_hash=claim.adapter_code_hash,
        payload_hash=claim.definition.payload_hash,
        payload_source_contract_hash=claim.payload_source_contract_hash,
        operation_id=operation_id,
        audience="lab-broker-a",
        not_before=claim.claimed_at,
        expires_at=NOW + timedelta(minutes=3),
        lease_expires_at=claim.lease_expires_at,
        nonce=nonce,
        single_use_authority_id=authority.authority_id,
    )
    return CurrentClaimPlanIssueV2.from_unsigned_plan(unsigned)


def _signed_raw_plan(
    plan: SourceUsePlanV2,
    *,
    authorities: Authorities,
    updates: dict[str, object],
    use_v1_signer: bool = False,
) -> bytes:
    payload = plan.model_dump(mode="json")
    payload.update(updates)
    signer = authorities.plan if use_v1_signer else authorities.plan_v2
    namespace = SOURCE_USE_PLAN_NAMESPACE if use_v1_signer else SOURCE_USE_PLAN_V2_NAMESPACE
    if use_v1_signer:
        payload.update(
            {
                "issuer": signer.issuer,
                "key_id": signer.key_id,
                "key_purpose": "source_use_plan",
            }
        )
    signing_payload = {key: value for key, value in payload.items() if key != "signature"}
    payload["signature"] = signer.sign(
        namespace=namespace,
        payload=canonical_json_bytes(signing_payload),
    )
    return canonical_json_bytes(payload)


def test_attempt_binding_v2_is_canonical_and_requires_exactly_one_run_identity() -> None:
    binding = _binding(shard_id=UUID("99999999-aaaa-bbbb-cccc-dddddddddddd"))

    assert len(binding.attempt_identity_hash) == 64
    with pytest.raises(ValidationError, match="exactly one"):
        SourceAttemptBindingV2.model_validate(
            {
                **binding.model_dump(),
                "run_id": UUID("99999999-2222-3333-4444-555555555555"),
            }
        )


def test_plan_issuance_requires_an_existing_current_unbound_claim(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    current = _claim(authorities)
    authority = MemoryCurrentClaimAuthority(current, authorities)

    plan = _plan(authorities, claim=current, authority=authority)

    assert plan.attempt_binding == current.attempt_binding
    assert plan.lease_expires_at == current.lease_expires_at
    assert plan.adapter_id == current.definition.adapter_id
    assert plan.payload_hash == current.definition.payload_hash
    assert plan.operation_id == OPERATION_ID
    assert authority.receipts[OPERATION_ID].signed_plan == plan
    stale = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=2,
        fence=8,
        worker_id="lab-worker-stale",
    )
    with pytest.raises(SourceOperationContractError, match="current|high-water"):
        _plan(
            authorities,
            claim=stale,
            authority=authority,
            operation_id=OTHER_OPERATION_ID,
            nonce="stale-plan",
        )


def test_current_claim_factory_rejects_shard_rebinding(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    definition = _definition(authorities)
    rebound = _binding(shard_id=UUID("99999999-aaaa-bbbb-cccc-dddddddddddd"))

    with pytest.raises(ValueError, match="shard_id"):
        LabShardClaimV2.from_current_attempt(
            definition=definition,
            attempt_binding=rebound,
            claimed_at=NOW,
            lease_expires_at=NOW + timedelta(minutes=5),
        )


def test_signing_commit_response_loss_recovers_same_signed_receipt(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    authority = MemoryCurrentClaimAuthority(claim, authorities)
    authority.fail_after_commit_once = True

    with pytest.raises(ConnectionError, match="response was lost"):
        _plan(authorities, claim=claim, authority=authority)
    committed = authority.receipts[OPERATION_ID]

    recovered = _plan(authorities, claim=claim, authority=authority)

    assert recovered == committed.signed_plan
    assert authority.receipts[OPERATION_ID] == committed
    assert authority.signing_calls == 1
    assert committed.binding.attempt_binding == claim.attempt_binding
    assert committed.binding.lease_expires_at == claim.lease_expires_at
    assert committed.binding.plan_hash == recovered.plan_hash
    assert committed.binding.manifest_hash == recovered.manifest_hash
    assert committed.binding.resource_request_hash == recovered.resource_request_hash
    assert committed.binding.attempt_identity_hash == recovered.attempt_identity_hash


def test_consumption_rejects_operation_or_binding_replay(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    authority = MemoryCurrentClaimAuthority(claim, authorities)
    _plan(authorities, claim=claim, authority=authority)
    rebound = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=claim.claim_generation + 1,
        fence=claim.scheduler_fencing_token + 1,
        worker_id="lab-worker-rebound",
    )

    with pytest.raises(SourceOperationContractError, match="binding changed"):
        _plan(authorities, claim=rebound, authority=authority)
    with pytest.raises(SourceOperationContractError, match="different operation_id"):
        _plan(
            authorities,
            claim=claim,
            authority=authority,
            operation_id=OTHER_OPERATION_ID,
        )


def test_concurrent_same_operation_calls_authority_signer_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    authority = MemoryCurrentClaimAuthority(claim, authorities)
    start = Barrier(3)
    signer_entered = Event()
    release_signer = Event()
    original_sign = authorities.plan_v2.sign

    def blocked_sign(*, namespace: str, payload: bytes) -> str:
        signer_entered.set()
        if not release_signer.wait(timeout=5):
            raise TimeoutError("test did not release authority signer")
        return original_sign(namespace=namespace, payload=payload)

    def invoke() -> SourceUsePlanV2:
        start.wait(timeout=5)
        return _plan(authorities, claim=claim, authority=authority)

    monkeypatch.setattr(authorities.plan_v2, "sign", blocked_sign)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(invoke)
        second = pool.submit(invoke)
        start.wait(timeout=5)
        assert signer_entered.wait(timeout=5)
        release_signer.set()
        first_plan = first.result(timeout=10)
        second_plan = second.result(timeout=10)

    assert first_plan == second_plan
    assert authority.signing_calls == 1
    assert len(authority.receipts) == 1


def test_invalid_manifest_and_unsigned_plan_leave_zero_authority_state(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    payload = _payload(authorities)
    manifest = payload.source_intent.manifest
    invalid_manifest = manifest.model_copy(update={"signature": f"AAAA{manifest.signature[4:]}"})
    invalid_intent = SourceIntentV2.from_manifest(
        invalid_manifest,
        resource_request=payload.source_intent.resource_request,
    )
    invalid_payload = StrategyShardPayloadV2.from_source_intent(
        adapter_id=invalid_manifest.adapter_id,
        adapter_version=invalid_manifest.adapter_version,
        payload_json=payload.payload_json,
        source_intent=invalid_intent,
    )
    invalid_claim = _claim(
        authorities,
        definition=_definition(authorities, payload=invalid_payload),
    )
    invalid_manifest_authority = MemoryCurrentClaimAuthority(
        invalid_claim,
        authorities,
    )

    with pytest.raises(SourceOperationContractError, match="manifest signature"):
        _plan(
            authorities,
            claim=invalid_claim,
            authority=invalid_manifest_authority,
        )
    assert invalid_manifest_authority.signing_calls == 0
    assert invalid_manifest_authority.receipts == {}
    assert invalid_manifest_authority.attempt_operations == {}

    valid_claim = _claim(authorities)
    invalid_plan_authority = MemoryCurrentClaimAuthority(valid_claim, authorities)
    issue = _unsigned_issue(claim=valid_claim, authority=invalid_plan_authority)
    invalid_issue = issue.model_copy(
        update={
            "unsigned_plan": issue.unsigned_plan.model_copy(
                update={"attempt_identity_hash": "f" * 64}
            )
        }
    )
    with pytest.raises(SourceOperationContractError, match="contract is invalid"):
        invalid_plan_authority.issue_plan_once(issue=invalid_issue, now=NOW)
    assert invalid_plan_authority.signing_calls == 0
    assert invalid_plan_authority.receipts == {}
    assert invalid_plan_authority.attempt_operations == {}


def test_invalid_authority_signer_result_rolls_back_issue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    authority = MemoryCurrentClaimAuthority(claim, authorities)

    def invalid_sign(*, namespace: str, payload: bytes) -> str:
        return f"{'A' * 86}=="

    monkeypatch.setattr(authorities.plan_v2, "sign", invalid_sign)
    with pytest.raises(SourceOperationContractError, match="signature is invalid"):
        _plan(authorities, claim=claim, authority=authority)
    assert authority.signing_calls == 1
    assert authority.receipts == {}
    assert authority.issue_hashes == {}
    assert authority.attempt_operations == {}


def test_signer_failure_rolls_back_current_claim_consumption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    authority = MemoryCurrentClaimAuthority(claim, authorities)
    original_sign = authorities.plan_v2.sign

    def fail_sign(*, namespace: str, payload: bytes) -> str:
        raise RuntimeError("signer failed")

    monkeypatch.setattr(authorities.plan_v2, "sign", fail_sign)
    with pytest.raises(RuntimeError, match="signer failed"):
        _plan(authorities, claim=claim, authority=authority)
    assert authority.receipts == {}
    assert authority.attempt_operations == {}

    monkeypatch.setattr(authorities.plan_v2, "sign", original_sign)
    assert _plan(authorities, claim=claim, authority=authority).operation_id == OPERATION_ID


@pytest.mark.parametrize(
    ("updates", "use_v1_signer"),
    (
        ({"schema_version": 1}, False),
        ({"schema_version": True}, False),
        ({"key_purpose": "source_use_plan"}, True),
        ({"attempt_binding": {"schema_version": 2}}, False),
    ),
)
def test_require_plan_rejects_signed_schema_purpose_and_shape_downgrade(
    tmp_path: Path,
    updates: dict[str, object],
    use_v1_signer: bool,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    plan = _plan(
        authorities,
        claim=claim,
        authority=MemoryCurrentClaimAuthority(claim, authorities),
    )
    raw = _signed_raw_plan(
        plan,
        authorities=authorities,
        updates=updates,
        use_v1_signer=use_v1_signer,
    )

    with pytest.raises(SourceOperationContractError, match="contract|canonical"):
        require_source_use_plan_v2(
            raw,
            keyring=authorities.authorization_keyring,
            audience="lab-broker-a",
            now=NOW,
        )


@pytest.mark.parametrize("bad_generation", (True, 3.0))
def test_require_plan_rejects_validly_signed_bool_and_float_attempt_fields(
    tmp_path: Path,
    bad_generation: object,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    plan = _plan(
        authorities,
        claim=claim,
        authority=MemoryCurrentClaimAuthority(claim, authorities),
    )
    attempt = plan.attempt_binding.model_dump(mode="json")
    attempt["claim_generation"] = bad_generation
    raw = _signed_raw_plan(plan, authorities=authorities, updates={"attempt_binding": attempt})

    with pytest.raises(SourceOperationContractError, match="contract"):
        require_source_use_plan_v2(
            raw,
            keyring=authorities.authorization_keyring,
            audience="lab-broker-a",
            now=NOW,
        )


def test_require_plan_rejects_alias_and_noncanonical_bytes_before_signature(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    plan = _plan(
        authorities,
        claim=claim,
        authority=MemoryCurrentClaimAuthority(claim, authorities),
    )
    attempt = plan.attempt_binding.model_dump(mode="json")
    raw_alias = _signed_raw_plan(
        plan,
        authorities=authorities,
        updates={"attemptBinding": attempt},
    )
    decoded = plan.model_dump(mode="json")
    noncanonical = b" " + canonical_json_bytes(decoded)

    for raw in (raw_alias, noncanonical):
        with pytest.raises(SourceOperationContractError, match="contract|canonical"):
            require_source_use_plan_v2(
                raw,
                keyring=authorities.authorization_keyring,
                audience="lab-broker-a",
                now=NOW,
            )


def test_require_plan_strictly_roundtrips_before_signature_and_time_checks(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    plan = _plan(
        authorities,
        claim=claim,
        authority=MemoryCurrentClaimAuthority(claim, authorities),
    )

    assert (
        require_source_use_plan_v2(
            canonical_model_json_bytes(plan),
            keyring=authorities.authorization_keyring,
            audience="lab-broker-a",
            now=NOW,
        )
        == plan
    )
    assert (
        require_source_use_plan_v2(
            plan.model_dump(mode="python"),
            keyring=authorities.authorization_keyring,
            audience="lab-broker-a",
            now=NOW,
        )
        == plan
    )
    with pytest.raises(SourceOperationContractError, match="audience"):
        require_source_use_plan_v2(
            plan,
            keyring=authorities.authorization_keyring,
            audience="another-broker",
            now=NOW,
        )
    with pytest.raises(SourceOperationContractError, match="expired"):
        require_source_use_plan_v2(
            plan,
            keyring=authorities.authorization_keyring,
            audience="lab-broker-a",
            now=NOW + timedelta(minutes=6),
        )


@pytest.mark.parametrize(
    "field,value",
    (
        ("job_id", UUID("99999999-2222-3333-4444-555555555555")),
        ("spec_hash", "c" * 64),
        ("shard_id", UUID("99999999-aaaa-bbbb-cccc-dddddddddddd")),
        ("attempt_id", UUID("99999999-aaaa-bbbb-cccc-dddddddddddd")),
        ("claim_generation", 4),
        ("scheduler_fencing_token", 10),
        ("worker_id", "lab-worker-b"),
    ),
)
def test_v2_plan_rejects_any_attempt_binding_tampering(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    plan = _plan(
        authorities,
        claim=claim,
        authority=MemoryCurrentClaimAuthority(claim, authorities),
    )
    tampered = plan.model_copy(
        update={"attempt_binding": plan.attempt_binding.model_copy(update={field: value})}
    )

    with pytest.raises(SourceOperationContractError, match="contract|signature"):
        require_source_use_plan_v2(
            tampered,
            keyring=authorities.authorization_keyring,
            audience="lab-broker-a",
            now=NOW,
        )


@pytest.mark.parametrize(
    "field",
    (
        "operation_id",
        "attempt_identity_hash",
        "adapter_code_hash",
        "payload_hash",
        "payload_source_contract_hash",
        "manifest_hash",
        "resource_request_hash",
    ),
)
def test_v2_plan_rejects_each_signed_operation_or_identity_tampering(
    tmp_path: Path,
    field: str,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    plan = _plan(
        authorities,
        claim=claim,
        authority=MemoryCurrentClaimAuthority(claim, authorities),
    )

    with pytest.raises(SourceOperationContractError, match="contract|signature"):
        require_source_use_plan_v2(
            plan.model_copy(update={field: "f" * 64}),
            keyring=authorities.authorization_keyring,
            audience="lab-broker-a",
            now=NOW,
        )


def test_v1_payload_stays_offline_and_v2_external_payload_requires_signed_intent(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    offline = StrategyShardPayloadV1(
        adapter_id="research.local-snapshot",
        adapter_version="1",
        payload_json='{"partition":"2026-08-05"}',
    )
    external = _payload(authorities)

    assert offline.network == "none"
    assert parse_strategy_shard_payload(offline.model_dump_json()) == offline
    assert (
        require_external_strategy_shard_payload(
            external,
            keyring=authorities.authorization_keyring,
        )
        == external
    )
    with pytest.raises(SourceOperationContractError, match="v1.*offline"):
        require_external_strategy_shard_payload(
            offline,
            keyring=authorities.authorization_keyring,
        )
    with pytest.raises(ValidationError):
        StrategyShardPayloadV1.model_validate(
            {**offline.model_dump(), "source_intent": external.source_intent.model_dump()}
        )
