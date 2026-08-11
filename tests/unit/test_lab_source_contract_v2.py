from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Event
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant.lab_shard_protocol import (
    LabShardClaimV2,
    require_source_bound_claim_v2,
)
from rquant.source_operation_contracts import SourceOperationContractError

from .test_adapter_manifest import NOW, create_test_authorities
from .test_source_operation_contracts import (
    OPERATION_ID,
    OTHER_OPERATION_ID,
    MemoryCurrentClaimAuthority,
    _claim,
    _definition,
    _payload,
    _plan,
)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("adapter_id", "research.unrelated-adapter"),
        ("adapter_version", "999"),
    ),
)
def test_claim_rejects_definition_adapter_and_signed_manifest_mismatch(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    unrelated = _definition(authorities, **{field: value})

    with pytest.raises(ValidationError, match="adapter identity"):
        _claim(authorities, definition=unrelated)


def test_claim_rejects_adapter_code_and_payload_source_contract_hash_tampering(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)

    with pytest.raises(ValidationError, match="adapter_code_hash"):
        LabShardClaimV2.model_validate(
            {**claim.model_dump(mode="python"), "adapter_code_hash": "f" * 64}
        )
    payload = _payload(authorities)
    tampered_payload = payload.model_dump(mode="json")
    tampered_payload["source_contract_hash"] = "f" * 64
    tampered_definition = _definition(
        authorities,
        payload=payload,
    ).model_copy(update={"payload_json": json.dumps(tampered_payload)})
    with pytest.raises(ValidationError, match="source_contract_hash|payload_hash"):
        _claim(authorities, definition=tampered_definition)


@pytest.mark.parametrize(
    ("field", "value", "match"),
    (
        ("source_plan_hash", "f" * 64, "source_plan_hash"),
        ("manifest_hash", "f" * 64, "manifest_hash"),
        ("adapter_code_hash", "f" * 64, "adapter_code_hash"),
        (
            "payload_source_contract_hash",
            "f" * 64,
            "source_contract_hash",
        ),
    ),
)
def test_bound_claim_rejects_each_rebound_identity_hash(
    tmp_path: Path,
    field: str,
    value: str,
    match: str,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    claim = _claim(authorities)
    plan = _plan(
        authorities,
        claim=claim,
        authority=MemoryCurrentClaimAuthority(claim, authorities),
    )
    bound = claim.bind_source_use_plan(plan)

    with pytest.raises(ValidationError, match=match):
        LabShardClaimV2.model_validate(
            {**bound.model_dump(mode="python"), field: value},
            strict=True,
        )


def test_current_claim_authority_rejects_lease_rebinding(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    current = _claim(authorities)
    authority = MemoryCurrentClaimAuthority(current, authorities)
    rebound = LabShardClaimV2.model_validate(
        {
            **current.model_dump(mode="python"),
            "lease_expires_at": current.lease_expires_at + timedelta(minutes=1),
        },
        strict=True,
    )

    with pytest.raises(SourceOperationContractError, match="current|high-water"):
        _plan(authorities, claim=rebound, authority=authority)


def test_reclaim_after_commit_only_recovers_exact_old_operation(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    old_current = _claim(authorities)
    authority = MemoryCurrentClaimAuthority(old_current, authorities)
    old_plan = _plan(authorities, claim=old_current, authority=authority)
    old_bound = old_current.bind_source_use_plan(old_plan)
    new_current = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=old_current.claim_generation + 1,
        fence=old_current.scheduler_fencing_token + 1,
        worker_id="lab-worker-reclaimed",
    )

    authority.replace_current(new_current)

    recovered = _plan(authorities, claim=old_current, authority=authority)
    assert recovered == old_plan
    assert authority.signing_calls == 1
    with pytest.raises(SourceOperationContractError, match="different operation_id"):
        _plan(
            authorities,
            claim=old_current,
            authority=authority,
            operation_id=OTHER_OPERATION_ID,
        )

    with pytest.raises(SourceOperationContractError, match="current|high-water"):
        require_source_bound_claim_v2(
            old_bound,
            keyring=authorities.authorization_keyring,
            current_claim_authority=authority,
            audience="lab-broker-a",
            now=NOW,
        )
    new_plan = _plan(
        authorities,
        claim=new_current,
        authority=authority,
        operation_id=OTHER_OPERATION_ID,
        nonce="reclaimed-plan",
    )
    new_bound = new_current.bind_source_use_plan(new_plan)
    assert (
        require_source_bound_claim_v2(
            new_bound,
            keyring=authorities.authorization_keyring,
            current_claim_authority=authority,
            audience="lab-broker-a",
            now=NOW,
        )
        == new_bound
    )

    assert authority.receipts[OPERATION_ID].signed_plan == old_plan
    assert authority.receipts[OTHER_OPERATION_ID].signed_plan == new_plan


def test_reclaim_during_signing_shares_authority_serialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    old_current = _claim(authorities)
    authority = MemoryCurrentClaimAuthority(old_current, authorities)
    new_current = _claim(
        authorities,
        attempt_id=UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
        generation=old_current.claim_generation + 1,
        fence=old_current.scheduler_fencing_token + 1,
        worker_id="lab-worker-reclaimed",
    )
    signer_entered = Event()
    release_signer = Event()
    reclaim_started = Event()
    reclaim_finished = Event()
    original_sign = authorities.plan_v2.sign

    def blocked_sign(*, namespace: str, payload: bytes) -> str:
        signer_entered.set()
        if not release_signer.wait(timeout=5):
            raise TimeoutError("test did not release authority signer")
        return original_sign(namespace=namespace, payload=payload)

    def reclaim() -> None:
        reclaim_started.set()
        authority.replace_current(new_current)
        reclaim_finished.set()

    monkeypatch.setattr(authorities.plan_v2, "sign", blocked_sign)
    with ThreadPoolExecutor(max_workers=2) as pool:
        sign_future = pool.submit(
            _plan,
            authorities,
            claim=old_current,
            authority=authority,
        )
        assert signer_entered.wait(timeout=5)
        reclaim_future = pool.submit(reclaim)
        assert reclaim_started.wait(timeout=5)
        assert not reclaim_finished.wait(timeout=0.05)
        release_signer.set()
        old_plan = sign_future.result(timeout=10)
        reclaim_future.result(timeout=10)

    assert authority.current_claim == new_current
    assert authority.signing_calls == 1
    assert authority.receipts[OPERATION_ID].signed_plan == old_plan
    assert _plan(authorities, claim=old_current, authority=authority) == old_plan
    assert authority.signing_calls == 1
