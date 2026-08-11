"""External Ed25519 trust certificates for Lab claim publication finalizers.

The JobStore may cache a certificate for diagnostics, but no database row is a
trust anchor. Every consumer verifies the certificate against an independently
injected, public-only root keyring.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Final, Literal

from pydantic import Field, ValidationError

from rquant.adapter_manifest import (
    LAB_CLAIM_FINALIZER_NAMESPACE,
    LAB_CLAIM_FINALIZER_ROOT_NAMESPACE,
    Ed25519ContractSigner,
    Ed25519PublicKeyRecord,
    VerifyOnlyEd25519Keyring,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

_HASH_PATTERN = r"^[0-9a-f]{64}$"
LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_SCHEMA_VERSION: Final = 1
LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_CONTRACT: Final = (
    "rquant-lab-claim-finalizer-trust-certificate/v1"
)
LAB_CLAIM_FINALIZER_PUBLICATION_PURPOSE: Final = "rquant-lab-claim-finalizer-publication/v1"
LAB_CLAIM_FINALIZER_PUBLICATION_ATTESTATION_CONTRACT: Final = (
    "rquant-lab-claim-finalizer-publication-attestation/v1"
)
LAB_CLAIM_PUBLICATION_WORKER_VERIFIER_SCHEMA_VERSION: Final = 1
LAB_CLAIM_PUBLICATION_WORKER_VERIFIER_CONTRACT: Final = (
    "rquant-lab-claim-publication-worker-verifier/v1"
)


class LabClaimFinalizerTrustError(ValueError):
    """The external finalizer trust chain is missing, expired, or invalid."""


class LabClaimPublicationWorkerVerificationConfig(RuntimeContractModel):
    """Canonical public-only material used by the worker's V2 D gate.

    The current-claim endpoint exposes verification through a narrow wrapper in
    CLI composition.  This document deliberately has no private-key, root-MAC,
    or finalizer-issuer fields.
    """

    schema_version: Literal[1] = LAB_CLAIM_PUBLICATION_WORKER_VERIFIER_SCHEMA_VERSION
    contract: Literal["rquant-lab-claim-publication-worker-verifier/v1"] = (
        LAB_CLAIM_PUBLICATION_WORKER_VERIFIER_CONTRACT
    )
    audience: str = Field(min_length=1, max_length=200)
    trust_certificate: LabClaimFinalizerTrustCertificate
    root_public_keys: tuple[Ed25519PublicKeyRecord, ...] = Field(min_length=1)
    finalizer_public_keys: tuple[Ed25519PublicKeyRecord, ...] = Field(min_length=1)
    source_plan_public_keys: tuple[Ed25519PublicKeyRecord, ...] = Field(min_length=1)
    spool_receipt_authority: dict[str, object]
    current_claim_socket_path: str = Field(min_length=1, max_length=4_096)
    current_claim_socket_owner_uid: int = Field(strict=True, ge=0)
    current_claim_socket_group_gid: int = Field(strict=True, ge=0)
    current_claim_socket_mode: Literal[384, 432]
    current_claim_server_uid: int = Field(strict=True, ge=0)
    current_claim_server_gid: int = Field(strict=True, ge=0)
    current_claim_server_pid: int | None = Field(default=None, strict=True, ge=1)
    current_claim_timeout_ms: int = Field(strict=True, ge=1, le=30_000)

    def require_verify_only_roles(self) -> None:
        expected = (
            (self.root_public_keys, "lab_claim_finalizer_root"),
            (self.finalizer_public_keys, "lab_claim_finalizer"),
            (self.source_plan_public_keys, "source_use_plan_v2"),
        )
        for records, purpose in expected:
            if any(record.key_purpose != purpose for record in records):
                raise LabClaimFinalizerTrustError("worker_public_verification_material_invalid")


class LabClaimFinalizerTrustCertificate(RuntimeContractModel):
    """Offline-root signed permission for one finalizer runtime key and store."""

    schema_version: Literal[1] = LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_SCHEMA_VERSION
    contract: Literal["rquant-lab-claim-finalizer-trust-certificate/v1"] = (
        LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_CONTRACT
    )
    root_issuer: str = Field(min_length=1, max_length=200)
    root_key_id: str = Field(min_length=1, max_length=200)
    finalizer_issuer: str = Field(min_length=1, max_length=200)
    finalizer_key_id: str = Field(min_length=1, max_length=200)
    finalizer_public_key_fingerprint: str = Field(pattern=_HASH_PATTERN)
    store_id: str = Field(pattern=_HASH_PATTERN)
    database_device: int = Field(strict=True, ge=0)
    database_inode: int = Field(strict=True, ge=0)
    schema_version_bound: int = Field(strict=True, ge=1)
    purpose: Literal["rquant-lab-claim-finalizer-publication/v1"] = (
        LAB_CLAIM_FINALIZER_PUBLICATION_PURPOSE
    )
    not_before: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    def verify_root(self, keyring: VerifyOnlyEd25519Keyring) -> bool:
        return (
            self.schema_version == LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_SCHEMA_VERSION
            and self.contract == LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_CONTRACT
            and self.purpose == LAB_CLAIM_FINALIZER_PUBLICATION_PURPOSE
            and self.signature_algorithm == "ed25519"
            and keyring.verify(
                issuer=self.root_issuer,
                key_id=self.root_key_id,
                key_purpose="lab_claim_finalizer_root",
                namespace=LAB_CLAIM_FINALIZER_ROOT_NAMESPACE,
                payload=self.signing_bytes(),
                signature=self.signature,
            )
        )


class LabClaimFinalizerTrustVerifier:
    """Verify-only external root and finalizer-key boundary for C/D and workers."""

    def __init__(
        self,
        *,
        root_keyring: VerifyOnlyEd25519Keyring,
        finalizer_keyring: VerifyOnlyEd25519Keyring,
    ) -> None:
        self._root_keyring = root_keyring
        self._finalizer_keyring = finalizer_keyring

    def require_certificate(
        self,
        certificate: LabClaimFinalizerTrustCertificate,
        *,
        store_id: str,
        database_generation: tuple[int, int],
        schema_version: int,
        now: datetime,
    ) -> LabClaimFinalizerTrustCertificate:
        try:
            validated = LabClaimFinalizerTrustCertificate.model_validate(certificate, strict=True)
        except ValidationError as exc:
            raise LabClaimFinalizerTrustError("finalizer_trust_certificate_invalid") from exc
        current = now.astimezone(UTC)
        if (
            validated.schema_version != LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_SCHEMA_VERSION
            or validated.contract != LAB_CLAIM_FINALIZER_TRUST_CERTIFICATE_CONTRACT
            or validated.purpose != LAB_CLAIM_FINALIZER_PUBLICATION_PURPOSE
            or validated.signature_algorithm != "ed25519"
            or not validated.verify_root(self._root_keyring)
            or validated.store_id != store_id
            or (validated.database_device, validated.database_inode) != database_generation
            or validated.schema_version_bound != schema_version
            or not (validated.not_before <= current < validated.expires_at)
        ):
            raise LabClaimFinalizerTrustError("finalizer_trust_certificate_invalid")
        return validated

    def require_runtime_signer(
        self,
        certificate: LabClaimFinalizerTrustCertificate,
        signer: Ed25519ContractSigner,
    ) -> None:
        if (
            signer.key_purpose != "lab_claim_finalizer"
            or signer.issuer != certificate.finalizer_issuer
            or signer.key_id != certificate.finalizer_key_id
            or signer.public_key_fingerprint != certificate.finalizer_public_key_fingerprint
            or not self._finalizer_keyring.allows_signer(signer)
        ):
            raise LabClaimFinalizerTrustError("finalizer_runtime_signer_invalid")

    def verify_finalizer_signature(
        self,
        certificate: LabClaimFinalizerTrustCertificate,
        *,
        payload: bytes,
        signature: str,
    ) -> None:
        if not self._finalizer_keyring.verify(
            issuer=certificate.finalizer_issuer,
            key_id=certificate.finalizer_key_id,
            key_purpose="lab_claim_finalizer",
            namespace=LAB_CLAIM_FINALIZER_NAMESPACE,
            payload=payload,
            signature=signature,
        ):
            raise LabClaimFinalizerTrustError("finalizer_publication_signature_invalid")


class LabClaimFinalizerPublicationAttestation(RuntimeContractModel):
    """One runtime-signed C or D transition, stored beside the mutable ledger row."""

    contract: Literal["rquant-lab-claim-finalizer-publication-attestation/v1"] = (
        LAB_CLAIM_FINALIZER_PUBLICATION_ATTESTATION_CONTRACT
    )
    attempt_id: str = Field(min_length=36, max_length=36)
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    finalizer_fencing_token: int = Field(strict=True, ge=1)
    publication_status: str = Field(pattern=r"^(READY_TO_PUBLISH|PUBLISHED)$")
    source_use_plan_hash: str = Field(pattern=_HASH_PATTERN)
    final_claim_hash: str = Field(pattern=_HASH_PATTERN)
    spool_receipt_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    store_id: str = Field(pattern=_HASH_PATTERN)
    schema_version: int = Field(strict=True, ge=1)
    certificate_hash: str = Field(pattern=_HASH_PATTERN)
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


def build_lab_claim_finalizer_publication_attestation(
    *,
    certificate: LabClaimFinalizerTrustCertificate,
    signer: Ed25519ContractSigner,
    attempt_id: str,
    claim_generation: int,
    scheduler_fencing_token: int,
    finalizer_fencing_token: int,
    publication_status: str,
    source_use_plan_hash: str,
    final_claim_hash: str,
    spool_receipt_hash: str | None,
    store_id: str,
    schema_version: int,
) -> tuple[bytes, bytes]:
    certificate_bytes = canonical_model_json_bytes(certificate)
    unsigned = LabClaimFinalizerPublicationAttestation(
        attempt_id=attempt_id,
        claim_generation=claim_generation,
        scheduler_fencing_token=scheduler_fencing_token,
        finalizer_fencing_token=finalizer_fencing_token,
        publication_status=publication_status,
        source_use_plan_hash=source_use_plan_hash,
        final_claim_hash=final_claim_hash,
        spool_receipt_hash=spool_receipt_hash,
        store_id=store_id,
        schema_version=schema_version,
        certificate_hash=hashlib.sha256(certificate_bytes).hexdigest(),
        signature="unsigned",
    )
    signed = unsigned.model_copy(
        update={
            "signature": signer.sign(
                namespace=LAB_CLAIM_FINALIZER_NAMESPACE,
                payload=unsigned.signing_bytes(),
            )
        }
    )
    return certificate_bytes, canonical_model_json_bytes(signed)


def require_lab_claim_finalizer_publication_attestation(
    *,
    verifier: LabClaimFinalizerTrustVerifier,
    certificate_bytes: bytes,
    attestation_bytes: bytes,
    store_id: str,
    database_generation: tuple[int, int],
    schema_version: int,
    now: datetime,
    attempt_id: str,
    claim_generation: int,
    scheduler_fencing_token: int,
    finalizer_fencing_token: int,
    publication_status: str,
    source_use_plan_hash: str,
    final_claim_hash: str,
    spool_receipt_hash: str | None,
) -> None:
    certificate = strict_model_validate_canonical_json(
        LabClaimFinalizerTrustCertificate, certificate_bytes
    )
    attestation = strict_model_validate_canonical_json(
        LabClaimFinalizerPublicationAttestation, attestation_bytes
    )
    verifier.require_certificate(
        certificate,
        store_id=store_id,
        database_generation=database_generation,
        schema_version=schema_version,
        now=now,
    )
    if (
        attestation.contract != LAB_CLAIM_FINALIZER_PUBLICATION_ATTESTATION_CONTRACT
        or attestation.attempt_id != attempt_id
        or attestation.claim_generation != claim_generation
        or attestation.scheduler_fencing_token != scheduler_fencing_token
        or attestation.finalizer_fencing_token != finalizer_fencing_token
        or attestation.publication_status != publication_status
        or attestation.source_use_plan_hash != source_use_plan_hash
        or attestation.final_claim_hash != final_claim_hash
        or attestation.spool_receipt_hash != spool_receipt_hash
        or attestation.store_id != store_id
        or attestation.schema_version != schema_version
        or attestation.certificate_hash != hashlib.sha256(certificate_bytes).hexdigest()
    ):
        raise LabClaimFinalizerTrustError("finalizer_publication_attestation_binding_invalid")
    verifier.verify_finalizer_signature(
        certificate,
        payload=attestation.signing_bytes(),
        signature=attestation.signature,
    )


def sign_lab_claim_finalizer_trust_certificate(
    *,
    root_signer: Ed25519ContractSigner,
    certificate: LabClaimFinalizerTrustCertificate,
) -> LabClaimFinalizerTrustCertificate:
    """Offline-only composition helper; runtime callers receive a completed cert."""

    if root_signer.key_purpose != "lab_claim_finalizer_root":
        raise LabClaimFinalizerTrustError("finalizer_trust_root_signer_invalid")
    unsigned = certificate.model_copy(update={"signature": "unsigned"})
    return unsigned.model_copy(
        update={
            "signature": root_signer.sign(
                namespace=LAB_CLAIM_FINALIZER_ROOT_NAMESPACE,
                payload=unsigned.signing_bytes(),
            )
        }
    )


LabClaimPublicationWorkerVerificationConfig.model_rebuild()
