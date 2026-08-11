from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant.source_broker_v2_job_protocol import (
    SOURCE_BROKER_V2_JOB_MAX_REQUEST_BYTES,
    SourceBrokerV2AuthorityRef,
    SourceBrokerV2ClaimRef,
    SourceBrokerV2FenceRef,
    SourceBrokerV2JobIntentEnvelope,
    SourceBrokerV2JobOutcomeEnvelope,
    SourceBrokerV2JobOutcomeStatus,
    SourceBrokerV2LineageRef,
    SourceBrokerV2NativeEvidence,
    SourceBrokerV2QuotaRef,
    build_verified_job_outcome,
    canonical_job_model_bytes,
    canonical_job_sha256,
    canonical_request_bytes,
    parse_job_intent,
    parse_job_outcome,
)
from rquant.source_operation_contracts import build_source_broker_v2_scheduler_intent

from .source_broker_v2_authorized_intent_fixture import (
    authorities as authorization_authorities,
)
from .source_broker_v2_authorized_intent_fixture import (
    authorized_payload_and_claim,
)
from .source_broker_v2_legacy_intent_fixture import legacy_intent_envelope

NOW = datetime(2026, 8, 9, 4, tzinfo=UTC)
HASH_1 = "1" * 64
HASH_2 = "2" * 64
HASH_3 = "3" * 64
HASH_4 = "4" * 64
HASH_5 = "5" * 64
HASH_6 = "6" * 64
HASH_7 = "7" * 64


def _authority(kind: str) -> SourceBrokerV2AuthorityRef:
    return SourceBrokerV2AuthorityRef(
        authority_id=f"{kind}-authority",
        key_id=f"{kind}-key-v2",
        purpose=f"rquant-{kind}-receipt",
        schema_version=2,
        generation=7,
        fence_hash=HASH_7,
    )


def _claim() -> SourceBrokerV2ClaimRef:
    return SourceBrokerV2ClaimRef(
        saga_id="saga-daily-bars",
        claim_binding_hash=HASH_1,
        claim_generation=3,
        scheduler_fencing_token=11,
        attempt_identity_hash=HASH_2,
        claim_plan_hash=HASH_3,
        manifest_hash=HASH_4,
        claim_payload_hash=HASH_5,
        authority=_authority("claim"),
    )


def _quota() -> SourceBrokerV2QuotaRef:
    return SourceBrokerV2QuotaRef(
        parent_id="quota-parent-daily-bars",
        quota_cost=1,
        authority=_authority("quota"),
    )


def _fence() -> SourceBrokerV2FenceRef:
    return SourceBrokerV2FenceRef(
        owner_id="lab-worker-a",
        owner_token_hash=HASH_6,
        generation=5,
        external_root_hash=HASH_7,
        claim_token_hash=HASH_1,
    )


def _lineage() -> SourceBrokerV2LineageRef:
    return SourceBrokerV2LineageRef(
        lineage_id="lineage-daily-bars",
        authority=_authority("lineage"),
    )


def _intent() -> SourceBrokerV2JobIntentEnvelope:
    payload, claim = authorized_payload_and_claim(
        now=NOW,
        job_id=UUID("11111111-2222-3333-4444-555555555555"),
        attempt_id=UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        claim_generation=3,
        scheduler_fencing_token=11,
        plan_hash=HASH_3,
        spec_hash=HASH_1,
        payload_json='{"partition":"2026-08-07"}',
    )
    keyring = authorization_authorities().authorization_keyring
    return build_source_broker_v2_scheduler_intent(
        payload,
        claim=claim,
        manifest_keyring=keyring,
        authorization_keyring=keyring,
        deadline=claim.claimed_at + timedelta(seconds=60),
        now=claim.claimed_at,
    )


def _malicious_legacy_intent(
    *,
    request: bytes,
) -> SourceBrokerV2JobIntentEnvelope:
    return legacy_intent_envelope(
        source_id="daily-bars",
        source_authority=_authority("source"),
        request=request,
        deadline=NOW + timedelta(seconds=30),
        claim=_claim(),
        quota=_quota(),
        fence=_fence(),
        lineage=_lineage(),
    )


def _evidence(kind: str, *, forged: bool = False) -> SourceBrokerV2NativeEvidence:
    request = canonical_request_bytes({"challenge": f"{kind}-challenge", "kind": kind})
    unsigned = {
        "authority_id": f"{kind}-authority",
        "challenge": f"{kind}-challenge",
        "fence_hash": HASH_7,
        "generation": 7,
        "key_id": f"{kind}-key-v2",
        "kind": kind,
        "operation_hash": HASH_2,
        "operation_id": HASH_1,
        "purpose": f"rquant-{kind}-receipt",
        "request_hash": canonical_job_sha256(request),
        "schema_version": 2,
    }
    signature = canonical_job_sha256({"receipt": unsigned, "secret": f"{kind}-secret"})
    receipt = canonical_request_bytes({**unsigned, "signature": HASH_3 if forged else signature})
    return SourceBrokerV2NativeEvidence.create(kind=kind, request=request, receipt=receipt)


class _StrictVerifier:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def _verify(self, kind: str, evidence: SourceBrokerV2NativeEvidence) -> None:
        payload = evidence.receipt_json
        unsigned = {key: value for key, value in payload.items() if key != "signature"}
        expected = canonical_job_sha256({"receipt": unsigned, "secret": f"{kind}-secret"})
        if payload.get("signature") != expected:
            raise ValueError(f"{kind} native signature is invalid")
        self.calls.append(kind)

    def verify_source(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        response: bytes,
        status: SourceBrokerV2JobOutcomeStatus,
        deadline: float,
    ) -> None:
        assert intent.operation_id
        assert response
        assert status
        assert deadline > 0
        self._verify("source", evidence)

    def verify_claim(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None:
        assert intent.claim.authority.authority_id == "claim-authority"
        assert deadline > 0
        self._verify("claim", evidence)

    def verify_quota(
        self,
        *,
        intent: SourceBrokerV2JobIntentEnvelope,
        evidence: SourceBrokerV2NativeEvidence,
        deadline: float,
    ) -> None:
        assert intent.quota.authority.authority_id == "quota-authority"
        assert deadline > 0
        self._verify("quota", evidence)

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
        assert intent.lineage.authority.authority_id == "lineage-authority"
        assert source_receipt_hash and claim_receipt_hash and quota_receipt_hash
        assert deadline > 0
        self._verify("lineage", evidence)


def _outcome(
    *, verifier: _StrictVerifier, forged_source: bool = False
) -> SourceBrokerV2JobOutcomeEnvelope:
    return build_verified_job_outcome(
        intent=_intent(),
        status=SourceBrokerV2JobOutcomeStatus.SUCCESS,
        response=canonical_request_bytes({"rows": [{"symbol": "000001.SZ"}]}),
        source_evidence=_evidence("source", forged=forged_source),
        claim_evidence=_evidence("claim"),
        quota_evidence=_evidence("quota"),
        lineage_evidence=_evidence("lineage"),
        verifier=verifier,
        deadline=100.0,
    )


def test_intent_is_strict_frozen_canonical_and_deterministic() -> None:
    intent = _intent()

    assert intent == _intent()
    assert parse_job_intent(canonical_job_model_bytes(intent)) == intent
    assert (
        SourceBrokerV2JobIntentEnvelope.model_validate_json(canonical_job_model_bytes(intent))
        == intent
    )
    with pytest.raises(ValidationError, match="extra_forbidden"):
        SourceBrokerV2JobIntentEnvelope.model_validate(
            {**intent.model_dump(mode="python"), "callback": "pkg.mod:fn"},
            strict=True,
        )
    with pytest.raises(ValidationError, match="frozen"):
        intent.source_id = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    "field",
    (
        "accessToken",
        "AccessToken",
        "apiKey",
        "source-token",
        "source.token",
        "clientSecret",
        "dbPassword",
        "runtimeCredential",
        "callbackImport",
        "picklePayload",
    ),
)
def test_forbidden_identifier_normalization_rejects_every_spelling(field: str) -> None:
    with pytest.raises(ValueError, match="forbidden"):
        _malicious_legacy_intent(request=canonical_request_bytes({"outer": {field: "not-allowed"}}))


def test_intent_requires_canonical_bounded_request_bytes() -> None:
    with pytest.raises(ValueError, match="canonical"):
        _malicious_legacy_intent(request=b'{"trade_date":"2026-08-07", "symbol":"000001.SZ"}')
    oversized = canonical_request_bytes({"payload": "x" * SOURCE_BROKER_V2_JOB_MAX_REQUEST_BYTES})
    with pytest.raises(ValidationError, match="at most"):
        _malicious_legacy_intent(request=oversized)


def test_outcome_builder_requires_four_verified_native_receipts() -> None:
    verifier = _StrictVerifier()
    outcome = _outcome(verifier=verifier)

    assert verifier.calls == ["source", "claim", "quota", "lineage"]
    assert parse_job_outcome(canonical_job_model_bytes(outcome)) == outcome
    assert outcome.evidence_chain_hash
    assert not hasattr(SourceBrokerV2JobOutcomeEnvelope, "from_protocol_receipts")

    missing_quota = outcome.model_dump(mode="python")
    del missing_quota["quota_evidence"]
    with pytest.raises(ValidationError, match="quota_evidence"):
        SourceBrokerV2JobOutcomeEnvelope.model_validate(missing_quota, strict=True)


def test_forged_native_signature_cannot_construct_outcome() -> None:
    with pytest.raises(ValueError, match="source native signature"):
        _outcome(verifier=_StrictVerifier(), forged_source=True)
