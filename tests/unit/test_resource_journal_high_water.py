from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path

import pytest

from rquant.resource_journal_high_water import (
    RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
    RESOURCE_JOURNAL_HEAD_NAMESPACE,
    RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
    RESOURCE_JOURNAL_SIGNING_PURPOSE,
    TRUSTED_RESOURCE_ROLE_PURPOSES,
    ResourceJournalAntiRollbackReceipt,
    ResourceJournalHighWaterCheckpoint,
    ResourceJournalHighWaterError,
    SQLiteResourceJournalHighWaterAuthority,
    TrustedRoleInventory,
)
from rquant.runtime_contracts import canonical_sha256

ZERO_HASH = "0" * 64
JOURNAL_ISSUER = "resource-journal-test-issuer"
ROOT_ISSUER = "resource-root-test-issuer"


class _Signer:
    signature_algorithm = "ed25519"

    def __init__(
        self,
        *,
        issuer: str,
        key_id: str,
        key_purpose: str,
        secret: bytes,
    ) -> None:
        self.issuer = issuer
        self.key_id = key_id
        self.key_purpose = key_purpose
        self.public_key_fingerprint = hashlib.sha256(secret).hexdigest()
        self._secret = secret

    def sign(self, *, namespace: str, payload: bytes) -> str:
        return hmac.new(
            self._secret,
            namespace.encode("ascii") + b"\0" + payload,
            hashlib.sha256,
        ).hexdigest()

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool:
        return hmac.compare_digest(
            self.sign(namespace=namespace, payload=payload),
            signature,
        )


JOURNAL_SIGNER = _Signer(
    issuer=JOURNAL_ISSUER,
    key_id="resource-journal-key",
    key_purpose=RESOURCE_JOURNAL_SIGNING_PURPOSE,
    secret=b"trusted-resource-journal-key",
)
ROOT_SIGNER = _Signer(
    issuer=ROOT_ISSUER,
    key_id="resource-root-key",
    key_purpose=RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
    secret=b"independent-resource-root-key",
)


def _inventory() -> TrustedRoleInventory:
    fingerprints = {
        purpose: frozenset({canonical_sha256(f"role:{purpose}")})
        for purpose in TRUSTED_RESOURCE_ROLE_PURPOSES
    }
    fingerprints[RESOURCE_JOURNAL_SIGNING_PURPOSE] = frozenset(
        {JOURNAL_SIGNER.public_key_fingerprint}
    )
    fingerprints[RESOURCE_JOURNAL_HIGH_WATER_PURPOSE] = frozenset(
        {ROOT_SIGNER.public_key_fingerprint}
    )
    return TrustedRoleInventory(role_fingerprints=fingerprints)


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _checkpoint(
    *,
    sequence: int,
    previous_head_hash: str,
    label: str,
    lineage_id: str = "a" * 64,
    signer: _Signer = JOURNAL_SIGNER,
    purpose: str | None = None,
) -> ResourceJournalHighWaterCheckpoint:
    materialized_state_root = canonical_sha256([])
    head: dict[str, object] = {
        "authority_id": "resource-authority-test",
        "lineage_id": lineage_id,
        "genesis_hash": "b" * 64,
        "keyring_policy_hash": "c" * 64,
        "sequence": sequence,
        "entry_hash": canonical_sha256(label),
        "previous_head_hash": previous_head_hash,
        "materialized_state_root": materialized_state_root,
        "issuer": signer.issuer,
        "key_id": signer.key_id,
        "key_purpose": purpose or signer.key_purpose,
        "namespace": RESOURCE_JOURNAL_HEAD_NAMESPACE,
        "signature_algorithm": signer.signature_algorithm,
        "public_key_fingerprint": signer.public_key_fingerprint,
    }
    signing_value = dict(head)
    signing_value["contract"] = "rquant-resource-admission-journal-head/v1"
    head["signature"] = signer.sign(
        namespace=RESOURCE_JOURNAL_HEAD_NAMESPACE,
        payload=_canonical_json(signing_value).encode(),
    )
    signed_head_json = _canonical_json(head)
    return ResourceJournalHighWaterCheckpoint(
        schema_version=1,
        contract="rquant-resource-journal-high-water-checkpoint/v1",
        journal_authority_id="resource-authority-test",
        lineage_id=lineage_id,
        sequence=sequence,
        previous_head_hash=previous_head_hash,
        head_hash=canonical_sha256(head),
        materialized_state_root=materialized_state_root,
        signed_head_json=signed_head_json,
    )


class _MemoryAntiRollbackRoot:
    def __init__(
        self,
        *,
        lose_advance_response: bool = False,
        tamper_receipt: bool = False,
        tamper_current: bool = False,
    ) -> None:
        self.authority_id = "independent-resource-anti-rollback-root"
        self.verifier_fingerprints = frozenset({ROOT_SIGNER.public_key_fingerprint})
        self._current: dict[str, ResourceJournalAntiRollbackReceipt] = {}
        self._operations: dict[str, tuple[str, ResourceJournalAntiRollbackReceipt]] = {}
        self._lose_advance_response = lose_advance_response
        self._tamper_receipt = tamper_receipt
        self._tamper_current = tamper_current

    def current(
        self,
        *,
        journal_authority_id: str,
    ) -> ResourceJournalAntiRollbackReceipt | None:
        receipt = self._current.get(journal_authority_id)
        if receipt is not None and self._tamper_current:
            return receipt.model_copy(update={"signature": "tampered-current"})
        return receipt

    def pin(
        self,
        *,
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        if journal_authority_id in self._current:
            raise ResourceJournalHighWaterError("external root lineage is already pinned")
        return self._close(
            operation_id=operation_id,
            high_water_authority_id=high_water_authority_id,
            journal_authority_id=journal_authority_id,
            previous_checkpoint_hash=ZERO_HASH,
            checkpoint=checkpoint,
        )

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        current = self._current.get(journal_authority_id)
        if current is None:
            raise ResourceJournalHighWaterError("external root lineage is not pinned")
        if (
            previous_checkpoint_hash != current.checkpoint.checkpoint_hash
            or checkpoint.sequence != current.checkpoint.sequence + 1
            or checkpoint.previous_head_hash != current.checkpoint.head_hash
            or checkpoint.lineage_id != current.checkpoint.lineage_id
        ):
            raise ResourceJournalHighWaterError("external root compare-and-advance failed")
        receipt = self._close(
            operation_id=operation_id,
            high_water_authority_id=high_water_authority_id,
            journal_authority_id=journal_authority_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
        )
        if self._lose_advance_response:
            self._lose_advance_response = False
            raise ConnectionError("simulated external root response loss")
        return receipt

    def _close(
        self,
        *,
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        request_hash = canonical_sha256(
            {
                "checkpoint": checkpoint,
                "high_water_authority_id": high_water_authority_id,
                "journal_authority_id": journal_authority_id,
                "operation_id": operation_id,
                "previous_checkpoint_hash": previous_checkpoint_hash,
            }
        )
        existing = self._operations.get(operation_id)
        if existing is not None:
            if existing[0] != request_hash:
                raise ResourceJournalHighWaterError("external root operation payload conflicts")
            return existing[1]
        unsigned = ResourceJournalAntiRollbackReceipt(
            schema_version=1,
            contract="rquant-resource-journal-anti-rollback-receipt/v1",
            root_authority_id=self.authority_id,
            high_water_authority_id=high_water_authority_id,
            journal_authority_id=journal_authority_id,
            operation_id=operation_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
            issuer=ROOT_SIGNER.issuer,
            key_id=ROOT_SIGNER.key_id,
            key_purpose=ROOT_SIGNER.key_purpose,
            namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
            signature_algorithm=ROOT_SIGNER.signature_algorithm,
            public_key_fingerprint=ROOT_SIGNER.public_key_fingerprint,
            signature="pending",
        )
        receipt = unsigned.model_copy(
            update={
                "signature": ROOT_SIGNER.sign(
                    namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )
        self._current[journal_authority_id] = receipt
        self._operations[operation_id] = (request_hash, receipt)
        if self._tamper_receipt:
            return receipt.model_copy(update={"signature": "tampered"})
        return receipt


def _authority(
    tmp_path: Path,
    *,
    root: _MemoryAntiRollbackRoot | None = None,
    filename: str = "resource-journal-high-water.sqlite3",
) -> SQLiteResourceJournalHighWaterAuthority:
    return SQLiteResourceJournalHighWaterAuthority(
        tmp_path / filename,
        authority_id="resource-journal-cache",
        trusted_role_inventory=_inventory(),
        journal_verifiers=(JOURNAL_SIGNER,),
        trusted_journal_issuer=JOURNAL_ISSUER,
        anti_rollback_root=root or _MemoryAntiRollbackRoot(),
        root_verifiers=(ROOT_SIGNER,),
        trusted_root_issuer=ROOT_ISSUER,
        mode="production",
    )


def test_production_cache_requires_an_authenticated_external_root(tmp_path: Path) -> None:
    with pytest.raises(ResourceJournalHighWaterError, match="production.*external.*root"):
        SQLiteResourceJournalHighWaterAuthority(
            tmp_path / "missing-root.sqlite3",
            authority_id="resource-journal-cache",
            trusted_role_inventory=_inventory(),
            journal_verifiers=(JOURNAL_SIGNER,),
            trusted_journal_issuer=JOURNAL_ISSUER,
            anti_rollback_root=None,
            root_verifiers=(ROOT_SIGNER,),
            trusted_root_issuer=ROOT_ISSUER,
        )

    standalone = SQLiteResourceJournalHighWaterAuthority(
        tmp_path / "standalone.sqlite3",
        authority_id="resource-journal-test-cache",
        trusted_role_inventory=_inventory(),
        journal_verifiers=(JOURNAL_SIGNER,),
        trusted_journal_issuer=JOURNAL_ISSUER,
        anti_rollback_root=None,
        root_verifiers=(),
        trusted_root_issuer=None,
        mode="test-standalone",
    )
    assert standalone.mode == "test-standalone"


def test_authenticated_cache_pins_genesis_and_advances_exactly_one_head(
    tmp_path: Path,
) -> None:
    root = _MemoryAntiRollbackRoot()
    authority = _authority(tmp_path, root=root)
    genesis = _checkpoint(sequence=0, previous_head_hash=ZERO_HASH, label="genesis")
    pinned = authority.pin(
        operation_id=canonical_sha256("pin-genesis"),
        journal_authority_id="resource-authority-test",
        checkpoint=genesis,
    )
    next_checkpoint = _checkpoint(
        sequence=1,
        previous_head_hash=genesis.head_hash,
        label="operation-1",
    )
    advanced = authority.compare_and_advance(
        operation_id=canonical_sha256("operation-1"),
        journal_authority_id="resource-authority-test",
        previous_checkpoint_hash=genesis.checkpoint_hash,
        checkpoint=next_checkpoint,
    )

    assert pinned.checkpoint == genesis
    assert advanced.previous_checkpoint_hash == genesis.checkpoint_hash
    assert authority.current(journal_authority_id="resource-authority-test") == advanced
    assert (
        _authority(tmp_path, root=root).current(journal_authority_id="resource-authority-test")
        == advanced
    )


def test_cache_operation_replay_is_stable_and_payload_rebind_fails(
    tmp_path: Path,
) -> None:
    authority = _authority(tmp_path)
    operation_id = canonical_sha256("pin-genesis")
    genesis = _checkpoint(sequence=0, previous_head_hash=ZERO_HASH, label="genesis")
    first = authority.pin(
        operation_id=operation_id,
        journal_authority_id="resource-authority-test",
        checkpoint=genesis,
    )

    assert (
        authority.pin(
            operation_id=operation_id,
            journal_authority_id="resource-authority-test",
            checkpoint=genesis,
        )
        == first
    )
    with pytest.raises(ResourceJournalHighWaterError, match="operation_id|payload"):
        authority.pin(
            operation_id=operation_id,
            journal_authority_id="resource-authority-test",
            checkpoint=_checkpoint(
                sequence=0,
                previous_head_hash=ZERO_HASH,
                label="different-genesis",
            ),
        )


@pytest.mark.parametrize(
    ("sequence", "previous_head_hash"),
    (
        (2, canonical_sha256("genesis-head")),
        (1, canonical_sha256("forked-head")),
    ),
)
def test_cache_rejects_sequence_skip_and_previous_head_fork(
    tmp_path: Path,
    sequence: int,
    previous_head_hash: str,
) -> None:
    authority = _authority(tmp_path)
    genesis = _checkpoint(sequence=0, previous_head_hash=ZERO_HASH, label="genesis")
    authority.pin(
        operation_id=canonical_sha256("pin-genesis"),
        journal_authority_id="resource-authority-test",
        checkpoint=genesis,
    )

    with pytest.raises(ResourceJournalHighWaterError, match="sequence|previous"):
        authority.compare_and_advance(
            operation_id=canonical_sha256({"previous": previous_head_hash, "sequence": sequence}),
            journal_authority_id="resource-authority-test",
            previous_checkpoint_hash=genesis.checkpoint_hash,
            checkpoint=_checkpoint(
                sequence=sequence,
                previous_head_hash=previous_head_hash,
                label="invalid-advance",
            ),
        )


def test_candidate_rejects_forged_self_consistent_head_and_wrong_purpose(
    tmp_path: Path,
) -> None:
    authority = _authority(tmp_path)
    forged = _Signer(
        issuer=JOURNAL_ISSUER,
        key_id=JOURNAL_SIGNER.key_id,
        key_purpose=RESOURCE_JOURNAL_SIGNING_PURPOSE,
        secret=b"forged-self-consistent-resource-head",
    )

    with pytest.raises(ResourceJournalHighWaterError, match="signature|verifier|identity"):
        authority.pin(
            operation_id=canonical_sha256("forged-head"),
            journal_authority_id="resource-authority-test",
            checkpoint=_checkpoint(
                sequence=0,
                previous_head_hash=ZERO_HASH,
                label="forged-head",
                signer=forged,
            ),
        )
    with pytest.raises(ResourceJournalHighWaterError, match="purpose|identity"):
        authority.pin(
            operation_id=canonical_sha256("wrong-purpose"),
            journal_authority_id="resource-authority-test",
            checkpoint=_checkpoint(
                sequence=0,
                previous_head_hash=ZERO_HASH,
                label="wrong-purpose",
                purpose=RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
            ),
        )


def test_candidate_verifier_set_must_match_the_trusted_role_inventory(
    tmp_path: Path,
) -> None:
    wrong = _Signer(
        issuer=JOURNAL_ISSUER,
        key_id="wrong-journal-key",
        key_purpose=RESOURCE_JOURNAL_SIGNING_PURPOSE,
        secret=b"wrong-journal-verifier",
    )
    with pytest.raises(ResourceJournalHighWaterError, match="verifier.*inventory"):
        SQLiteResourceJournalHighWaterAuthority(
            tmp_path / "wrong-verifier.sqlite3",
            authority_id="resource-journal-cache",
            trusted_role_inventory=_inventory(),
            journal_verifiers=(wrong,),
            trusted_journal_issuer=JOURNAL_ISSUER,
            anti_rollback_root=_MemoryAntiRollbackRoot(),
            root_verifiers=(ROOT_SIGNER,),
            trusted_root_issuer=ROOT_ISSUER,
        )


def test_signed_external_root_receipt_tamper_fails_closed(tmp_path: Path) -> None:
    authority = _authority(tmp_path, root=_MemoryAntiRollbackRoot(tamper_receipt=True))
    with pytest.raises(ResourceJournalHighWaterError, match="root.*signature|receipt"):
        authority.pin(
            operation_id=canonical_sha256("pin-genesis"),
            journal_authority_id="resource-authority-test",
            checkpoint=_checkpoint(
                sequence=0,
                previous_head_hash=ZERO_HASH,
                label="genesis",
            ),
        )


def test_tampered_external_current_receipt_fails_every_cache_read(tmp_path: Path) -> None:
    root = _MemoryAntiRollbackRoot()
    authority = _authority(tmp_path, root=root)
    authority.pin(
        operation_id=canonical_sha256("pin-genesis"),
        journal_authority_id="resource-authority-test",
        checkpoint=_checkpoint(
            sequence=0,
            previous_head_hash=ZERO_HASH,
            label="genesis",
        ),
    )
    root._tamper_current = True

    with pytest.raises(ResourceJournalHighWaterError, match="root.*signature|receipt"):
        authority.current(journal_authority_id="resource-authority-test")


def test_external_response_loss_recovers_stable_pending_operation(tmp_path: Path) -> None:
    root = _MemoryAntiRollbackRoot(lose_advance_response=True)
    authority = _authority(tmp_path, root=root)
    genesis = _checkpoint(sequence=0, previous_head_hash=ZERO_HASH, label="genesis")
    authority.pin(
        operation_id=canonical_sha256("pin-genesis"),
        journal_authority_id="resource-authority-test",
        checkpoint=genesis,
    )
    next_checkpoint = _checkpoint(
        sequence=1,
        previous_head_hash=genesis.head_hash,
        label="operation-1",
    )
    operation_id = canonical_sha256("operation-1")

    with pytest.raises(ConnectionError, match="response loss"):
        authority.compare_and_advance(
            operation_id=operation_id,
            journal_authority_id="resource-authority-test",
            previous_checkpoint_hash=genesis.checkpoint_hash,
            checkpoint=next_checkpoint,
        )

    recovered = _authority(tmp_path, root=root)
    receipt = recovered.current(journal_authority_id="resource-authority-test")
    assert receipt is not None
    assert receipt.operation_id == operation_id
    assert receipt.checkpoint == next_checkpoint
