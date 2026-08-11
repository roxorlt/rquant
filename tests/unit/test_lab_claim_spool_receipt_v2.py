from __future__ import annotations

import hashlib
import os
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.lab_claim_publication import (
    LabClaimSpoolPublishReceiptV2,
    LabClaimSpoolReceiptVerifier,
    require_v2_publish_receipt_for_final_claim,
)
from rquant.lab_shard_protocol import LabClaimSpool
from rquant.source_broker_v2_job_protocol import SourceBrokerV2AuthorityRef
from rquant.strict_json import canonical_model_json_bytes, strict_model_validate_canonical_json

from .test_adapter_manifest import create_test_authorities
from .test_source_operation_contracts import MemoryCurrentClaimAuthority, _claim, _plan


def _publisher() -> SourceBrokerV2AuthorityRef:
    return SourceBrokerV2AuthorityRef(
        authority_id="lab-finalizer-authority",
        key_id="lab-finalizer-key-v2",
        purpose="rquant-lab-spool-publish",
        schema_version=2,
        generation=7,
        fence_hash="7" * 64,
    )


def _spool(root: Path) -> LabClaimSpool:
    return LabClaimSpool(root, publish_receipt_publisher=_publisher())


def test_v2_spool_receipt_binds_real_current_final_claim_entry(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    preimage = _claim(authorities)
    plan = _plan(
        authorities,
        claim=preimage,
        authority=MemoryCurrentClaimAuthority(preimage, authorities),
    )
    final_claim = preimage.bind_source_use_plan(plan)
    spool = _spool(tmp_path / "claims")
    entry = spool.publish(final_claim)

    receipt = LabClaimSpoolPublishReceiptV2.from_published_entry(
        spool=spool,
        entry=entry,
        final_claim=final_claim,
        committed_at=final_claim.claimed_at + timedelta(seconds=1),
    )

    assert receipt.final_claim_bytes == canonical_model_json_bytes(final_claim)
    assert receipt.spool_entry_relative_locator.startswith("pending/")
    assert (
        strict_model_validate_canonical_json(
            LabClaimSpoolPublishReceiptV2,
            canonical_model_json_bytes(receipt),
        )
        == receipt
    )
    ledger_receipt = receipt.to_publish_receipt()
    assert ledger_receipt.spool_receipt_bytes == canonical_model_json_bytes(receipt)
    assert (
        ledger_receipt.spool_receipt_hash
        == hashlib.sha256(ledger_receipt.spool_receipt_bytes).hexdigest()
    )
    assert receipt.require_current_published_entry(spool=spool, final_claim=final_claim) == receipt
    verifier = LabClaimSpoolReceiptVerifier.from_spool(spool)
    assert verifier.verify(receipt, final_claim=final_claim) == receipt

    forged = receipt.model_copy(
        update={"committed_at": receipt.committed_at + timedelta(seconds=1)}
    )
    with pytest.raises(ValueError, match="sidecar conflicts"):
        verifier.verify(forged, final_claim=final_claim)

    sidecar = spool.publish_receipt_dir / f"{receipt.spool_entry_id}.json"
    sidecar.unlink()
    with pytest.raises(ValueError, match="provenance is unavailable"):
        verifier.verify(receipt, final_claim=final_claim)


def test_v2_spool_receipt_rejects_tamper_and_noncurrent_entry(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    preimage = _claim(authorities)
    plan = _plan(
        authorities,
        claim=preimage,
        authority=MemoryCurrentClaimAuthority(preimage, authorities),
    )
    final_claim = preimage.bind_source_use_plan(plan)
    spool = _spool(tmp_path / "claims")
    entry = spool.publish(final_claim)
    receipt = LabClaimSpoolPublishReceiptV2.from_published_entry(
        spool=spool,
        entry=entry,
        final_claim=final_claim,
        committed_at=final_claim.claimed_at + timedelta(seconds=1),
    )

    with pytest.raises(ValidationError, match="final_claim_hash"):
        LabClaimSpoolPublishReceiptV2.model_validate(
            {**receipt.model_dump(mode="python"), "final_claim_hash": "f" * 64}
        )
    spool.consume(entry)
    with pytest.raises(ValueError, match="pending|current"):
        receipt.require_current_published_entry(spool=spool, final_claim=final_claim)


def test_v2_spool_receipt_persists_first_bytes_across_duplicate_and_reopen(
    tmp_path: Path,
) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    preimage = _claim(authorities)
    plan = _plan(
        authorities,
        claim=preimage,
        authority=MemoryCurrentClaimAuthority(preimage, authorities),
    )
    final_claim = preimage.bind_source_use_plan(plan)
    root = tmp_path / "claims"
    spool = _spool(root)
    entry = spool.publish(final_claim)
    first = LabClaimSpoolPublishReceiptV2.from_published_entry(
        spool=spool,
        entry=entry,
        final_claim=final_claim,
        committed_at=final_claim.claimed_at + timedelta(seconds=1),
    )
    duplicate = LabClaimSpoolPublishReceiptV2.from_published_entry(
        spool=spool,
        entry=entry,
        final_claim=final_claim,
        committed_at=final_claim.claimed_at + timedelta(seconds=2),
    )
    reopened = _spool(root)
    reopened_entry = reopened.load(entry.path)
    reopened_receipt = LabClaimSpoolPublishReceiptV2.from_published_entry(
        spool=reopened,
        entry=reopened_entry,
        final_claim=final_claim,
        committed_at=final_claim.claimed_at + timedelta(seconds=3),
    )

    assert duplicate == first
    assert reopened_receipt == first
    assert "device" not in first.model_dump(mode="json")
    assert "inode" not in first.model_dump(mode="json")
    assert str(root) not in canonical_model_json_bytes(first).decode("utf-8")

    replacement = tmp_path / "same-final-claim.json"
    replacement.write_bytes(entry.path.read_bytes())
    os.replace(replacement, entry.path)
    replaced_entry = reopened.load(entry.path)
    assert (
        LabClaimSpoolPublishReceiptV2.from_published_entry(
            spool=reopened,
            entry=replaced_entry,
            final_claim=final_claim,
            committed_at=final_claim.claimed_at + timedelta(seconds=4),
        )
        == first
    )


def test_v2_spool_receipt_rejects_cross_final_claim_bytes(tmp_path: Path) -> None:
    authorities = create_test_authorities(tmp_path / "keys")
    preimage = _claim(authorities)
    plan = _plan(
        authorities,
        claim=preimage,
        authority=MemoryCurrentClaimAuthority(preimage, authorities),
    )
    final_claim = preimage.bind_source_use_plan(plan)
    spool = _spool(tmp_path / "claims")
    entry = spool.publish(final_claim)
    receipt = LabClaimSpoolPublishReceiptV2.from_published_entry(
        spool=spool,
        entry=entry,
        final_claim=final_claim,
        committed_at=final_claim.claimed_at + timedelta(seconds=1),
    ).to_publish_receipt()

    other_preimage = _claim(
        authorities,
        attempt_id=preimage.claim_token.__class__("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
    )
    other_plan = _plan(
        authorities,
        claim=other_preimage,
        authority=MemoryCurrentClaimAuthority(other_preimage, authorities),
    )
    other_final_claim = other_preimage.bind_source_use_plan(other_plan)

    with pytest.raises(ValueError, match="exact final claim"):
        require_v2_publish_receipt_for_final_claim(
            receipt,
            final_claim=other_final_claim,
        )
    with pytest.raises(ValueError, match="entry conflicts"):
        LabClaimSpoolPublishReceiptV2.from_published_entry(
            spool=spool,
            entry=entry,
            final_claim=other_final_claim,
            committed_at=other_final_claim.claimed_at + timedelta(seconds=1),
        )
