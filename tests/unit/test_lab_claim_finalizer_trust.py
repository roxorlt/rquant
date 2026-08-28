from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
from rquant.lab_claim_finalizer_trust import (
    LAB_CLAIM_FINALIZER_PUBLICATION_PURPOSE,
    LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_CONTRACT,
    LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_SCHEMA_VERSION,
    LabClaimFinalizerTrustCertificate,
    LabClaimFinalizerTrustError,
    LabClaimFinalizerTrustVerifier,
    sign_lab_claim_finalizer_trust_certificate,
)

from .test_adapter_manifest import _key_pair


def _keyring(record: object, *, purpose: str) -> VerifyOnlyEd25519Keyring:
    typed = record
    return VerifyOnlyEd25519Keyring(
        records=(typed,),  # type: ignore[arg-type]
        issuer_allowlist={purpose: frozenset({typed.issuer})},  # type: ignore[attr-defined]
        rotation_allowlist={(typed.issuer, purpose): frozenset({typed.key_id})},  # type: ignore[attr-defined]
    )


def test_external_root_rejects_self_signed_or_store_rewritten_certificate(tmp_path: Path) -> None:
    root, root_record = _key_pair(
        tmp_path / "root",
        key_id="offline-root",
        issuer="lab-offline-root",
        key_purpose="lab_claim_finalizer_root",
        rotation="active",
    )
    runtime, runtime_record = _key_pair(
        tmp_path / "runtime",
        key_id="finalizer-runtime",
        issuer="lab-finalizer",
        key_purpose="lab_claim_finalizer",
        rotation="active",
    )
    now = datetime(2026, 8, 11, tzinfo=UTC)
    unsigned = LabClaimFinalizerTrustCertificate(
        root_issuer=root.issuer,
        root_key_id=root.key_id,
        finalizer_issuer=runtime.issuer,
        finalizer_key_id=runtime.key_id,
        finalizer_public_key_fingerprint=runtime.public_key_fingerprint,
        store_id="a" * 64,
        database_device=1,
        database_inode=2,
        schema_version_bound=16,
        not_before=now - timedelta(seconds=1),
        expires_at=now + timedelta(minutes=5),
        signature="unsigned",
    )
    certificate = sign_lab_claim_finalizer_trust_certificate(
        root_signer=root,
        certificate=unsigned,
    )
    verifier = LabClaimFinalizerTrustVerifier(
        root_keyring=_keyring(root_record, purpose="lab_claim_finalizer_root"),
        finalizer_keyring=_keyring(runtime_record, purpose="lab_claim_finalizer"),
    )
    verifier.require_certificate(
        certificate,
        store_id="a" * 64,
        database_generation=(1, 2),
        schema_version=16,
        now=now,
    )
    verifier.require_runtime_signer(certificate, runtime)

    with pytest.raises(LabClaimFinalizerTrustError, match="certificate_invalid"):
        verifier.require_certificate(
            certificate.model_copy(update={"store_id": "b" * 64}),
            store_id="b" * 64,
            database_generation=(1, 2),
            schema_version=16,
            now=now,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("schema_version", 2),
        ("contract", "rquant-lab-claim-finalizer-trust-certificate/v0"),
        ("purpose", "rquant-lab-claim-finalizer-recovery/v1"),
    ),
)
def test_certificate_contract_constants_reject_bypassed_model_before_trust_use(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    root, root_record = _key_pair(
        tmp_path / "root",
        key_id="offline-root",
        issuer="lab-offline-root",
        key_purpose="lab_claim_finalizer_root",
        rotation="active",
    )
    runtime, runtime_record = _key_pair(
        tmp_path / "runtime",
        key_id="finalizer-runtime",
        issuer="lab-finalizer",
        key_purpose="lab_claim_finalizer",
        rotation="active",
    )
    now = datetime(2026, 8, 11, tzinfo=UTC)
    certificate = sign_lab_claim_finalizer_trust_certificate(
        root_signer=root,
        certificate=LabClaimFinalizerTrustCertificate(
            schema_version=LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_SCHEMA_VERSION,
            contract=LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_CONTRACT,
            root_issuer=root.issuer,
            root_key_id=root.key_id,
            finalizer_issuer=runtime.issuer,
            finalizer_key_id=runtime.key_id,
            finalizer_public_key_fingerprint=runtime.public_key_fingerprint,
            store_id="a" * 64,
            database_device=1,
            database_inode=2,
            schema_version_bound=16,
            purpose=LAB_CLAIM_FINALIZER_PUBLICATION_PURPOSE,
            not_before=now - timedelta(seconds=1),
            expires_at=now + timedelta(minutes=5),
            signature="unsigned",
        ),
    )
    bypassed = certificate.model_construct(**{**certificate.__dict__, field: value})
    verifier = LabClaimFinalizerTrustVerifier(
        root_keyring=_keyring(root_record, purpose="lab_claim_finalizer_root"),
        finalizer_keyring=_keyring(runtime_record, purpose="lab_claim_finalizer"),
    )
    with pytest.raises(LabClaimFinalizerTrustError, match="certificate_invalid"):
        verifier.require_certificate(
            bypassed,
            store_id="a" * 64,
            database_generation=(1, 2),
            schema_version=16,
            now=now,
        )
