from __future__ import annotations

import importlib
from datetime import date, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
from rquant.lab_shard_protocol import LabShardDefinition, StrategyShardPayloadV2
from rquant.source_broker_v2_job_protocol import (
    SourceBrokerV2AuthorityRef,
    canonical_job_model_bytes,
    parse_job_intent,
)
from rquant.source_operation_contracts import (
    SourceBrokerV2PublicRequest,
    SourceBrokerV2SchedulerIntentTemplate,
    SourceIntentV2,
    SourceOperationContractError,
    build_source_broker_v2_scheduler_intent,
    issue_scheduler_intent_authorization_v1,
)
from rquant.strict_json import canonical_model_json_bytes

from .test_adapter_manifest import NOW, create_test_authorities
from .test_source_operation_contracts import _claim


def _authority(kind: str) -> SourceBrokerV2AuthorityRef:
    return SourceBrokerV2AuthorityRef(
        authority_id=f"{kind}-authority",
        key_id=f"{kind}-key-v2",
        purpose=f"rquant-{kind}-receipt",
        schema_version=2,
        generation=7,
        fence_hash="7" * 64,
    )


def _template(claim: object) -> SourceBrokerV2SchedulerIntentTemplate:
    payload = claim.strategy_payload  # type: ignore[attr-defined]
    intent = payload.source_intent
    return SourceBrokerV2SchedulerIntentTemplate.from_source_intent(
        source_intent=intent,
        source_id=intent.manifest.source or "",
        request=SourceBrokerV2PublicRequest(
            dataset="daily_bars",
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


def _payload_and_claim(
    authorities: object,
    *,
    template: SourceBrokerV2SchedulerIntentTemplate | None = None,
) -> tuple[StrategyShardPayloadV2, object]:
    bare_claim = _claim(authorities)  # type: ignore[arg-type]
    bound_template = template or _template(bare_claim)
    unsigned_payload = StrategyShardPayloadV2.model_validate(
        {
            **bare_claim.strategy_payload.model_dump(mode="json"),
            "source_intent": bound_template.source_intent.model_dump(mode="json"),
            "source_contract_hash": bound_template.source_contract_hash,
            "scheduler_intent_template": bound_template.model_dump(mode="json"),
        }
    )
    authorization = issue_scheduler_intent_authorization_v1(
        unsigned_payload,
        signer=authorities.scheduler_intent,  # type: ignore[attr-defined]
        valid_from=NOW,
        expires_at=NOW + timedelta(minutes=4),
    )
    payload = unsigned_payload.with_scheduler_intent_authorization(authorization)
    definition = LabShardDefinition.from_payload(
        shard_index=bare_claim.definition.shard_index,
        adapter_id=bare_claim.definition.adapter_id,
        adapter_version=bare_claim.definition.adapter_version,
        plan_hash=bare_claim.definition.plan_hash,
        payload_json=canonical_model_json_bytes(payload).decode("utf-8"),
        work_plan=bare_claim.definition.work_plan,
    )
    return payload, _claim(authorities, definition=definition)  # type: ignore[arg-type]


def test_scheduler_intent_template_is_manifest_attempt_and_deadline_bound(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    payload, claim = _payload_and_claim(authorities)
    deadline = claim.claimed_at + timedelta(seconds=60)

    first = build_source_broker_v2_scheduler_intent(
        payload,
        claim=claim,
        manifest_keyring=authorities.authorization_keyring,
        authorization_keyring=authorities.authorization_keyring,
        deadline=deadline,
        now=claim.claimed_at,
    )
    second = build_source_broker_v2_scheduler_intent(
        payload,
        claim=claim,
        manifest_keyring=authorities.authorization_keyring,
        authorization_keyring=authorities.authorization_keyring,
        deadline=deadline,
        now=claim.claimed_at,
    )

    assert canonical_job_model_bytes(first) == canonical_job_model_bytes(second)
    assert parse_job_intent(canonical_job_model_bytes(first)) == first
    assert first.claim.claim_generation == claim.claim_generation
    assert first.claim.scheduler_fencing_token == claim.scheduler_fencing_token
    assert first.claim.manifest_hash == claim.manifest_hash
    assert first.claim.claim_payload_hash == claim.definition.payload_hash
    assert first.quota.quota_cost == (
        claim.strategy_payload.source_intent.resource_request.cost_per_call
        * claim.strategy_payload.source_intent.resource_request.requested_calls
    )
    assert first.fence.owner_id == claim.worker_id
    assert first.fence.generation == claim.claim_generation


def test_scheduler_intent_template_fails_closed_for_cross_bound_inputs(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    payload, claim = _payload_and_claim(authorities)
    deadline = claim.claimed_at + timedelta(seconds=60)
    other_template = _template(_claim(authorities)).model_copy(
        update={"lineage_id": "lineage-daily-bars-other"}
    )
    _, other_claim = _payload_and_claim(authorities, template=other_template)

    with pytest.raises(SourceOperationContractError, match="payload conflicts"):
        build_source_broker_v2_scheduler_intent(
            payload,
            claim=other_claim,
            manifest_keyring=authorities.authorization_keyring,
            authorization_keyring=authorities.authorization_keyring,
            deadline=deadline,
            now=other_claim.claimed_at,
        )
    with pytest.raises(SourceOperationContractError, match="deadline"):
        build_source_broker_v2_scheduler_intent(
            payload,
            claim=claim,
            manifest_keyring=authorities.authorization_keyring,
            authorization_keyring=authorities.authorization_keyring,
            deadline=deadline + timedelta(seconds=1),
            now=claim.claimed_at,
        )


def test_payload_v2_keeps_legacy_template_absence_but_factory_rejects_it(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    payload = StrategyShardPayloadV2.model_validate_json(claim.definition.payload_json)

    assert payload.scheduler_intent_template is None
    with pytest.raises(SourceOperationContractError, match="template"):
        build_source_broker_v2_scheduler_intent(
            payload,
            claim=claim,
            manifest_keyring=authorities.authorization_keyring,
            authorization_keyring=authorities.authorization_keyring,
            deadline=claim.claimed_at + timedelta(seconds=60),
            now=claim.claimed_at,
        )


def test_scheduler_intent_template_is_strict_and_has_no_capability_fields(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    template = _template(_claim(authorities))

    with pytest.raises(ValidationError, match="extra_forbidden"):
        SourceBrokerV2SchedulerIntentTemplate.model_validate(
            {**template.model_dump(mode="python"), "credential": "forbidden"}
        )
    assert not {
        "credential",
        "token",
        "provider",
        "client",
        "runtime",
        "private_key",
    }.intersection(SourceBrokerV2SchedulerIntentTemplate.model_fields)
    module = importlib.import_module("rquant.source_operation_contracts")
    assert not hasattr(module, "_build_source_broker_v2_scheduler_intent")


def test_scheduler_template_request_is_typed_and_closed_to_free_form_fields(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    request = _template(_claim(authorities)).request

    with pytest.raises(ValidationError, match="extra_forbidden"):
        SourceBrokerV2PublicRequest.model_validate(
            {**request.model_dump(mode="python"), "headers": {"Authorization": "x"}}
        )
    with pytest.raises(ValidationError, match="extra_forbidden"):
        SourceBrokerV2PublicRequest.model_validate(
            {**request.model_dump(mode="python"), "options": {"apiKey": "x"}}
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("symbols", ("c2VjcmV0",)),
        ("symbols", ("000001.XX",)),
        ("frequency", "daily"),
        ("fields", ("authorization",)),
        ("fields", ("encoded_secret",)),
        ("dataset", "c2VjcmV0LXRva2Vu"),
    ),
)
def test_scheduler_template_request_rejects_values_outside_public_data_domain(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    request = _template(_claim(authorities)).request

    with pytest.raises(ValidationError):
        SourceBrokerV2PublicRequest.model_validate(
            {**request.model_dump(mode="python"), field: value}
        )


@pytest.mark.parametrize(
    "request_value",
    (
        {"nested": [{"ApiKey": "redacted"}]},
        {"nested": [{"SESSION_COOKIE": "redacted"}]},
        {"nested": [{"privateKey": "redacted"}]},
        {"nested": [{"authorization": "redacted"}]},
    ),
)
def test_scheduler_intent_template_rejects_nested_capability_fields(
    tmp_path: Path,
    request_value: object,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    template = _template(_claim(authorities))
    with pytest.raises(ValidationError, match="request"):
        SourceBrokerV2SchedulerIntentTemplate.model_validate(
            {
                **template.model_dump(mode="python"),
                "request": request_value,
            }
        )


def test_scheduler_intent_factory_rejects_invalid_or_untrusted_manifest_signature(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    bare_claim = _claim(authorities)
    template = _template(bare_claim)
    invalid_manifest = template.source_intent.manifest.model_copy(
        update={"signature": f"AAAA{template.source_intent.manifest.signature[4:]}"}
    )
    invalid_intent = SourceIntentV2.from_manifest(
        invalid_manifest,
        resource_request=template.source_intent.resource_request,
    )
    invalid_template = SourceBrokerV2SchedulerIntentTemplate.from_source_intent(
        source_intent=invalid_intent,
        source_id=template.source_id,
        request=template.request,
        deadline_offset_seconds=template.deadline_offset_seconds,
        saga_id=template.saga_id,
        source_authority=template.source_authority,
        claim_authority=template.claim_authority,
        quota_parent_id=template.quota_parent_id,
        quota_authority=template.quota_authority,
        lineage_id=template.lineage_id,
        lineage_authority=template.lineage_authority,
        fence_external_root_hash=template.fence_external_root_hash,
    )
    invalid_payload, invalid_claim = _payload_and_claim(authorities, template=invalid_template)

    with pytest.raises(SourceOperationContractError, match="manifest signature"):
        build_source_broker_v2_scheduler_intent(
            invalid_payload,
            claim=invalid_claim,
            manifest_keyring=authorities.authorization_keyring,
            authorization_keyring=authorities.authorization_keyring,
            deadline=invalid_claim.claimed_at + timedelta(seconds=60),
            now=invalid_claim.claimed_at,
        )

    valid_payload, valid_claim = _payload_and_claim(authorities)
    untrusted_keyring = VerifyOnlyEd25519Keyring(
        records=authorities.records,
        issuer_allowlist={"adapter_manifest": frozenset({"release-authority"})},
        rotation_allowlist={
            ("release-authority", "adapter_manifest"): frozenset({"manifest-v1"}),
        },
    )
    with pytest.raises(SourceOperationContractError, match="manifest signature"):
        build_source_broker_v2_scheduler_intent(
            valid_payload,
            claim=valid_claim,
            manifest_keyring=untrusted_keyring,
            authorization_keyring=authorities.authorization_keyring,
            deadline=valid_claim.claimed_at + timedelta(seconds=60),
            now=valid_claim.claimed_at,
        )
