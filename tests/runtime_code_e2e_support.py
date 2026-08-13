from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from rquant.runtime_code_attestation import (
    RUNTIME_CODE_ATTESTATION_NAMESPACE,
    RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
    RUNTIME_CODE_ROOT_NAMESPACE,
    RuntimeCodeBundleEntry,
    RuntimeCodeExecutionSpec,
    RuntimeCodePromotionReceipt,
    RuntimeCodePromotionTrustBoundary,
    build_runtime_code_bundle,
    compute_runtime_code_generation_id,
    sign_runtime_code_attestation,
    sign_runtime_code_trust_certificate,
)
from rquant.runtime_code_generation import (
    RuntimeCodeGenerationCapability,
    RuntimeCodeGenerationInstaller,
    RuntimeCodeInstallRequest,
    open_attested_runtime_generation,
)
from rquant.strict_json import canonical_model_json_bytes
from tests.runtime_code_support import contract_key_pair

NOW = datetime(2026, 8, 12, 8, tzinfo=UTC)
INTERPRETER_BYTES = b"RQUANT-TEST-INTERPRETER\n"
LAUNCHER_BYTES = b"TARGET_STARTED = True\n"


@dataclass
class CurrentPromotion:
    current_bytes: bytes | None = None

    def read(self, installation_id: str, target_platform: str) -> bytes | None:
        del installation_id, target_platform
        return self.current_bytes


@dataclass(frozen=True)
class RuntimeCodeTestPackage:
    authorities: tuple[object, ...]
    root_keyring: object
    runtime_keyring: object
    promotion_state: CurrentPromotion
    promotion_trust: object
    package_root: Path
    paths: dict[str, Path]
    bundle_bytes: bytes
    attestation_bytes: bytes
    certificate_bytes: bytes
    receipt_bytes: bytes
    receipt: RuntimeCodePromotionReceipt
    now: datetime

    def request(self) -> RuntimeCodeInstallRequest:
        return RuntimeCodeInstallRequest(
            source_root=self.package_root,
            bundle_path=self.paths["runtime-code.bundle"],
            attestation_path=self.paths["runtime-code-attestation.json"],
            certificate_path=self.paths["runtime-code-certificate.json"],
            receipt_path=self.paths["runtime-code-promotion-receipt.json"],
            expected_audience="formal-lab",
            expected_installation_id="installation-a",
            expected_target_platform="test-platform",
            now=self.now,
        )


def build_test_package(
    root: Path,
    *,
    sequence: int = 1,
    previous_receipt_sha256: str = "0" * 64,
    source: bytes = b"VALUE = 1\n",
    source_path: str = "release/src/rquant/app.py",
    provenance_commit: str = "2" * 40,
    authorities: tuple[object, ...] | None = None,
    promotion_state: CurrentPromotion | None = None,
    extra_entries: tuple[RuntimeCodeBundleEntry, ...] = (),
    environment_allowlist: tuple[str, ...] = ("RQUANT_ALLOWED",),
    interpreter_bytes: bytes = INTERPRETER_BYTES,
    launcher_bytes: bytes = LAUNCHER_BYTES,
    import_roots: tuple[str, ...] = ("release/src",),
    python_abi: str = "test-abi",
    now: datetime = NOW,
) -> RuntimeCodeTestPackage:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if authorities is None:
        root_authority = contract_key_pair(
            root / "keys" / "root",
            key_id="root-v1",
            issuer="offline-root",
            key_purpose="rquant_runtime_code_root",
            namespace=RUNTIME_CODE_ROOT_NAMESPACE,
        )
        runtime_authority = contract_key_pair(
            root / "keys" / "runtime",
            key_id="runtime-v1",
            issuer="runtime-issuer",
            key_purpose="rquant_runtime_code_signer",
            namespace=RUNTIME_CODE_ATTESTATION_NAMESPACE,
        )
        promotion_authority = contract_key_pair(
            root / "keys" / "promotion",
            key_id="promotion-v1",
            issuer="promotion-issuer",
            key_purpose="rquant_runtime_code_promotion_root",
            namespace=RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
        )
        authorities = (*root_authority, *runtime_authority, *promotion_authority)
    (
        root_signer,
        _root_record,
        root_keyring,
        runtime_signer,
        _runtime_record,
        runtime_keyring,
        promotion_signer,
        _promotion_record,
        promotion_keyring,
    ) = authorities
    bundle = build_runtime_code_bundle(
        (
            RuntimeCodeBundleEntry(
                path="release/bin/python",
                mode=0o555,
                content=interpreter_bytes,
            ),
            RuntimeCodeBundleEntry(
                path="release/bin/rquant",
                mode=0o555,
                content=launcher_bytes,
            ),
            RuntimeCodeBundleEntry(
                path=source_path,
                mode=0o444,
                content=source,
            ),
            *extra_entries,
        )
    )
    certificate = sign_runtime_code_trust_certificate(
        root_signer=root_signer,
        runtime_signer=runtime_signer,
        audience="formal-lab",
        installation_id="installation-a",
        target_platform="test-platform",
        not_before=now - timedelta(minutes=1),
        expires_at=now + timedelta(days=1),
    )
    attestation = sign_runtime_code_attestation(
        signer=runtime_signer,
        bundle=bundle,
        execution_spec=RuntimeCodeExecutionSpec(
            launcher_path="release/bin/rquant",
            working_directory="release",
            import_roots=import_roots,
            interpreter_path="release/bin/python",
            interpreter_sha256=hashlib.sha256(interpreter_bytes).hexdigest(),
            python_abi=python_abi,
            environment_allowlist=environment_allowlist,
        ),
        audience="formal-lab",
        installation_id="installation-a",
        target_platform="test-platform",
        provenance_commit=provenance_commit,
        not_before=now - timedelta(minutes=1),
        expires_at=now + timedelta(hours=1),
    )
    attestation_bytes = canonical_model_json_bytes(attestation)
    generation_id = compute_runtime_code_generation_id(
        attestation_sha256=hashlib.sha256(attestation_bytes).hexdigest(),
        bundle_sha256=bundle.bundle_sha256,
        content_root_sha256=bundle.content_root_sha256,
        installation_id="installation-a",
        target_platform="test-platform",
        promotion_sequence=sequence,
    )
    unsigned_receipt = RuntimeCodePromotionReceipt(
        role="rquant_runtime_code_promotion_root",
        root_authority_id="external-root",
        root_store_id="external-store",
        issuer=promotion_signer.issuer,
        key_id=promotion_signer.key_id,
        key_purpose="rquant_runtime_code_promotion_root",
        namespace=RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
        public_key_fingerprint=promotion_signer.public_key_fingerprint,
        rollback_domain_id="external-runtime-code-domain",
        attestation_sha256=hashlib.sha256(attestation_bytes).hexdigest(),
        bundle_sha256=bundle.bundle_sha256,
        content_root_sha256=bundle.content_root_sha256,
        installation_id="installation-a",
        target_platform="test-platform",
        generation_id=generation_id,
        promotion_sequence=sequence,
        previous_receipt_sha256=previous_receipt_sha256,
        signature="unsigned",
    )
    receipt = unsigned_receipt.model_copy(
        update={
            "signature": promotion_signer.sign(
                namespace=RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
                payload=unsigned_receipt.signing_bytes(),
            )
        }
    )
    receipt_bytes = canonical_model_json_bytes(receipt)
    state = promotion_state or CurrentPromotion()
    state.current_bytes = receipt_bytes
    config = SimpleNamespace(
        role=receipt.role,
        root_authority_id=receipt.root_authority_id,
        root_store_id=receipt.root_store_id,
        root_issuer=receipt.issuer,
        root_key_id=receipt.key_id,
        root_key_purpose=receipt.key_purpose,
        root_receipt_namespace=receipt.namespace,
        root_signature_algorithm="ed25519",
        root_public_key_fingerprint=receipt.public_key_fingerprint,
        witness_rollback_domain_id=receipt.rollback_domain_id,
    )

    def verify_receipt(**values: object) -> None:
        if not promotion_keyring.verify(
            issuer=receipt.issuer,
            key_id=receipt.key_id,
            key_purpose="rquant_runtime_code_promotion_root",
            namespace=receipt.namespace,
            payload=values["signing_bytes"],
            signature=values["signature"],
        ):
            raise ValueError("promotion receipt signature is invalid")

    base_trust = SimpleNamespace(config=config, verify_receipt=verify_receipt)
    trust = RuntimeCodePromotionTrustBoundary(
        trust=base_trust,
        current_reader=state.read,
    )
    package_root = root / f"package-{sequence}"
    package_root.mkdir(mode=0o700)
    certificate_bytes = canonical_model_json_bytes(certificate)
    paths: dict[str, Path] = {}
    for name, payload in (
        ("runtime-code.bundle", bundle.bundle_bytes),
        ("runtime-code-attestation.json", attestation_bytes),
        ("runtime-code-certificate.json", certificate_bytes),
        ("runtime-code-promotion-receipt.json", receipt_bytes),
    ):
        path = package_root / name
        path.write_bytes(payload)
        path.chmod(0o444)
        paths[name] = path
    return RuntimeCodeTestPackage(
        authorities=authorities,
        root_keyring=root_keyring,
        runtime_keyring=runtime_keyring,
        promotion_state=state,
        promotion_trust=trust,
        package_root=package_root,
        paths=paths,
        bundle_bytes=bundle.bundle_bytes,
        attestation_bytes=attestation_bytes,
        certificate_bytes=certificate_bytes,
        receipt_bytes=receipt_bytes,
        receipt=receipt,
        now=now,
    )


def install_test_package(
    root: Path,
    package: RuntimeCodeTestPackage,
) -> tuple[Path, Path, RuntimeCodeGenerationInstaller]:
    trusted_base = root / "trusted"
    trusted_base.mkdir(mode=0o700, parents=True)
    runtime_root = trusted_base / "runtime-code"
    runtime_root.mkdir(mode=0o700)
    installer = RuntimeCodeGenerationInstaller(
        runtime_root=runtime_root,
        trusted_base=trusted_base,
        root_keyring=package.root_keyring,
        runtime_keyring=package.runtime_keyring,
        promotion_trust=package.promotion_trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    installer.install(package.request())
    return trusted_base, runtime_root, installer


def open_test_capability(
    *,
    trusted_base: Path,
    runtime_root: Path,
    package: RuntimeCodeTestPackage,
) -> RuntimeCodeGenerationCapability:
    return open_attested_runtime_generation(
        runtime_root=runtime_root,
        trusted_base=trusted_base,
        root_keyring=package.root_keyring,
        runtime_keyring=package.runtime_keyring,
        promotion_trust=package.promotion_trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
        now=package.now,
    )
