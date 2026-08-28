from __future__ import annotations

import hashlib
import io
import os
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from tests.runtime_code_support import contract_key_pair

NOW = datetime(2026, 8, 12, 4, tzinfo=UTC)
INTERPRETER_BYTES = b"TEST-INTERPRETER\n"


def _authorities(tmp_path: Path) -> tuple[object, ...]:
    from rquant.runtime_code_attestation import (
        RUNTIME_CODE_ATTESTATION_NAMESPACE,
        RUNTIME_CODE_ROOT_NAMESPACE,
    )

    root = contract_key_pair(
        tmp_path / "root",
        key_id="runtime-code-root-v1",
        issuer="offline-runtime-code-root",
        key_purpose="rquant_runtime_code_root",
        namespace=RUNTIME_CODE_ROOT_NAMESPACE,
    )
    runtime = contract_key_pair(
        tmp_path / "runtime",
        key_id="runtime-code-v1",
        issuer="runtime-code-issuer",
        key_purpose="rquant_runtime_code_signer",
        namespace=RUNTIME_CODE_ATTESTATION_NAMESPACE,
    )
    return (*root, *runtime)


def _bundle() -> object:
    from rquant.runtime_code_attestation import RuntimeCodeBundleEntry, build_runtime_code_bundle

    return build_runtime_code_bundle(
        (
            RuntimeCodeBundleEntry(
                path="release/bin/python",
                mode=0o555,
                content=INTERPRETER_BYTES,
            ),
            RuntimeCodeBundleEntry(
                path="release/bin/rquant",
                mode=0o555,
                content=b"#!/usr/bin/python3\n",
            ),
            RuntimeCodeBundleEntry(
                path="release/src/rquant/__init__.py",
                mode=0o444,
                content=b'VERSION = "test"\n',
            ),
        )
    )


def _signed_attestation(tmp_path: Path) -> tuple[object, ...]:
    from rquant.runtime_code_attestation import (
        RuntimeCodeExecutionSpec,
        sign_runtime_code_attestation,
        sign_runtime_code_trust_certificate,
    )

    root_signer, root_record, root_keyring, runtime_signer, runtime_record, runtime_keyring = (
        _authorities(tmp_path)
    )
    bundle = _bundle()
    certificate = sign_runtime_code_trust_certificate(
        root_signer=root_signer,
        runtime_signer=runtime_signer,
        audience="rquant-formal-runtime",
        installation_id="lab-installation-a",
        target_platform="macos-arm64-cpython-313",
        not_before=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(days=30),
    )
    execution = RuntimeCodeExecutionSpec(
        launcher_path="release/bin/rquant",
        working_directory="release",
        import_roots=("release/src",),
        interpreter_path="release/bin/python",
        interpreter_sha256=hashlib.sha256(INTERPRETER_BYTES).hexdigest(),
        python_abi="cpython-313-darwin-arm64",
    )
    attestation = sign_runtime_code_attestation(
        signer=runtime_signer,
        bundle=bundle,
        execution_spec=execution,
        audience="rquant-formal-runtime",
        installation_id="lab-installation-a",
        target_platform="macos-arm64-cpython-313",
        provenance_commit="b" * 40,
        not_before=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
    )
    return (
        bundle,
        certificate,
        attestation,
        root_record,
        root_keyring,
        runtime_record,
        runtime_keyring,
    )


def test_bundle_is_deterministic_and_rejects_noncanonical_file_tables() -> None:
    from rquant.runtime_code_attestation import (
        RuntimeCodeBundleEntry,
        RuntimeCodeFile,
        build_runtime_code_bundle,
        require_canonical_runtime_code_bundle,
    )

    first = _bundle()
    second = build_runtime_code_bundle(tuple(reversed(first.entries)))
    assert first.bundle_bytes == second.bundle_bytes
    assert first.files == second.files
    assert (
        require_canonical_runtime_code_bundle(
            first.bundle_bytes,
            expected_files=first.files,
            expected_content_root_sha256=first.content_root_sha256,
        ).entries
        == first.entries
    )

    for path in (
        "/absolute.py",
        "../escape.py",
        "release//bad.py",
        "release/./bad.py",
        "release\\bad.py",
        "release/caf\N{LATIN SMALL LETTER E WITH ACUTE}.py",
        "release/.git/index",
    ):
        with pytest.raises(ValidationError):
            RuntimeCodeFile(path=path, mode=0o444, size=1, sha256="a" * 64)
    with pytest.raises(ValidationError):
        RuntimeCodeFile(path="release/a.py", mode=0o644, size=1, sha256="a" * 64)
    with pytest.raises(ValueError, match="duplicate|collision"):
        build_runtime_code_bundle(
            (
                RuntimeCodeBundleEntry(path="release/A.py", mode=0o444, content=b"a"),
                RuntimeCodeBundleEntry(path="release/a.py", mode=0o444, content=b"b"),
            )
        )
    special = io.BytesIO()
    with tarfile.open(fileobj=special, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        link = tarfile.TarInfo(first.files[0].path)
        link.type = tarfile.SYMTYPE
        link.linkname = "elsewhere"
        archive.addfile(link)
    with pytest.raises(ValueError):
        require_canonical_runtime_code_bundle(
            special.getvalue(),
            expected_files=first.files,
            expected_content_root_sha256=first.content_root_sha256,
        )


def test_execution_spec_requires_attested_generation_local_interpreter(
    tmp_path: Path,
) -> None:
    from rquant.runtime_code_attestation import (
        RuntimeCodeExecutionSpec,
        sign_runtime_code_attestation,
    )

    _root, _record, _keys, runtime_signer, _runtime_record, _runtime_keys = _authorities(tmp_path)
    bundle = _bundle()
    with pytest.raises(ValidationError):
        RuntimeCodeExecutionSpec(
            launcher_path="release/bin/rquant",
            working_directory="release",
            import_roots=("release/src",),
            interpreter_path="/usr/bin/python3",
            interpreter_sha256="a" * 64,
            python_abi="cpython-313-darwin-arm64",
        )
    execution = RuntimeCodeExecutionSpec(
        launcher_path="release/bin/rquant",
        working_directory="release",
        import_roots=("release/src",),
        interpreter_path="release/bin/python",
        interpreter_sha256="a" * 64,
        python_abi="cpython-313-darwin-arm64",
    )
    with pytest.raises(ValueError, match="interpreter"):
        sign_runtime_code_attestation(
            signer=runtime_signer,
            bundle=bundle,
            execution_spec=execution,
            audience="rquant-formal-runtime",
            installation_id="lab-installation-a",
            target_platform="macos-arm64-cpython-313",
            provenance_commit="b" * 40,
            not_before=NOW - timedelta(minutes=1),
            expires_at=NOW + timedelta(hours=1),
        )


def test_attestation_binds_certificate_bundle_table_content_and_expiry(tmp_path: Path) -> None:
    from rquant.runtime_code_attestation import (
        RuntimeCodeTrustError,
        require_runtime_code_attestation,
    )
    from rquant.strict_json import canonical_model_json_bytes

    (
        bundle,
        certificate,
        attestation,
        _root_record,
        root_keyring,
        _runtime_record,
        runtime_keyring,
    ) = _signed_attestation(tmp_path)
    verified = require_runtime_code_attestation(
        attestation_bytes=canonical_model_json_bytes(attestation),
        certificate_bytes=canonical_model_json_bytes(certificate),
        bundle_bytes=bundle.bundle_bytes,
        root_keyring=root_keyring,
        runtime_keyring=runtime_keyring,
        expected_audience="rquant-formal-runtime",
        expected_installation_id="lab-installation-a",
        expected_target_platform="macos-arm64-cpython-313",
        now=NOW,
    )
    assert verified.attestation == attestation
    assert verified.bundle.content_root_sha256 == bundle.content_root_sha256

    substituted = bytearray(bundle.bundle_bytes)
    substituted[600] ^= 1
    for changed_bundle, changed_attestation, changed_now in (
        (bytes(substituted), attestation, NOW),
        (
            bundle.bundle_bytes,
            attestation.model_copy(update={"content_root_sha256": "c" * 64}),
            NOW,
        ),
        (bundle.bundle_bytes, attestation, NOW + timedelta(hours=2)),
    ):
        with pytest.raises(RuntimeCodeTrustError):
            require_runtime_code_attestation(
                attestation_bytes=canonical_model_json_bytes(changed_attestation),
                certificate_bytes=canonical_model_json_bytes(certificate),
                bundle_bytes=changed_bundle,
                root_keyring=root_keyring,
                runtime_keyring=runtime_keyring,
                expected_audience="rquant-formal-runtime",
                expected_installation_id="lab-installation-a",
                expected_target_platform="macos-arm64-cpython-313",
                now=changed_now,
            )


def test_promotion_receipt_uses_external_root_identity_and_rejects_rollback(
    tmp_path: Path,
) -> None:
    from rquant.runtime_code_attestation import (
        RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
        RuntimeCodePromotionReceipt,
        RuntimeCodeTrustError,
        compute_runtime_code_generation_id,
        require_runtime_code_promotion_receipt,
    )
    from rquant.strict_json import canonical_model_json_bytes

    bundle, _certificate, attestation, *_unused = _signed_attestation(tmp_path)
    attestation_sha256 = hashlib.sha256(canonical_model_json_bytes(attestation)).hexdigest()
    generation_id = compute_runtime_code_generation_id(
        attestation_sha256=attestation_sha256,
        bundle_sha256=bundle.bundle_sha256,
        content_root_sha256=bundle.content_root_sha256,
        installation_id="lab-installation-a",
        target_platform="macos-arm64-cpython-313",
        promotion_sequence=8,
    )
    receipt = RuntimeCodePromotionReceipt(
        role="rquant_runtime_code_promotion_root",
        root_authority_id="external-root-a",
        root_store_id="external-store-a",
        issuer="external-root-issuer",
        key_id="external-root-v1",
        key_purpose="rquant_runtime_code_promotion_root",
        namespace=RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
        public_key_fingerprint="d" * 64,
        rollback_domain_id="runtime-code-domain-a",
        attestation_sha256=attestation_sha256,
        bundle_sha256=bundle.bundle_sha256,
        content_root_sha256=bundle.content_root_sha256,
        installation_id="lab-installation-a",
        target_platform="macos-arm64-cpython-313",
        generation_id=generation_id,
        promotion_sequence=8,
        previous_receipt_sha256="e" * 64,
        signature="external-signature",
    )
    trust = SimpleNamespace(
        config=SimpleNamespace(
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
        ),
        verify_receipt=lambda **_kwargs: None,
    )
    assert (
        require_runtime_code_promotion_receipt(
            receipt_bytes=canonical_model_json_bytes(receipt),
            trust=trust,
            attestation_sha256=attestation_sha256,
            bundle_sha256=bundle.bundle_sha256,
            content_root_sha256=bundle.content_root_sha256,
            installation_id="lab-installation-a",
            target_platform="macos-arm64-cpython-313",
            minimum_promotion_sequence=8,
            expected_previous_receipt_sha256="e" * 64,
        )
        == receipt
    )
    with pytest.raises(RuntimeCodeTrustError, match="rollback"):
        require_runtime_code_promotion_receipt(
            receipt_bytes=canonical_model_json_bytes(receipt),
            trust=trust,
            attestation_sha256=attestation_sha256,
            bundle_sha256=bundle.bundle_sha256,
            content_root_sha256=bundle.content_root_sha256,
            installation_id="lab-installation-a",
            target_platform="macos-arm64-cpython-313",
            minimum_promotion_sequence=9,
            expected_previous_receipt_sha256="e" * 64,
        )


def test_generation_manifest_and_code_evidence_are_strict() -> None:
    from rquant.runtime_code_attestation import (
        CodeTrustEvidence,
        RuntimeCodeGenerationArtifact,
        RuntimeCodeGenerationManifest,
    )

    artifact = RuntimeCodeGenerationArtifact(
        path="runtime-code.bundle",
        mode=0o444,
        size=10,
        sha256="a" * 64,
    )
    manifest = RuntimeCodeGenerationManifest(
        generation_id="b" * 64,
        attestation_sha256="c" * 64,
        receipt_sha256="d" * 64,
        bundle_sha256="e" * 64,
        materialized_tree_root_sha256="f" * 64,
        artifacts=(artifact,),
    )
    evidence = CodeTrustEvidence(
        generation_id=manifest.generation_id,
        attestation_sha256=manifest.attestation_sha256,
        content_root_sha256="1" * 64,
        promotion_sequence=1,
        provenance_commit="2" * 40,
    )
    assert evidence.generation_id == manifest.generation_id
    with pytest.raises(ValidationError):
        RuntimeCodeGenerationManifest.model_validate(
            {
                **manifest.model_dump(),
                "artifacts": (artifact, artifact),
            }
        )


def test_p0_03_attestation_bundle_mismatch_fails_before_target(tmp_path: Path) -> None:
    from rquant.runtime_code_attestation import (
        RuntimeCodeTrustError,
        require_runtime_code_attestation,
    )
    from tests.runtime_code_e2e_support import build_test_package

    first = build_test_package(tmp_path / "first", source=b"VALUE = 'A'\n")
    second = build_test_package(
        tmp_path / "second",
        source=b"VALUE = 'B'\n",
        authorities=first.authorities,
    )
    assert len(first.bundle_bytes) == len(second.bundle_bytes)
    target_started = False
    with pytest.raises(RuntimeCodeTrustError):
        require_runtime_code_attestation(
            attestation_bytes=first.attestation_bytes,
            certificate_bytes=first.certificate_bytes,
            bundle_bytes=second.bundle_bytes,
            root_keyring=first.root_keyring,
            runtime_keyring=first.runtime_keyring,
            expected_audience="formal-lab",
            expected_installation_id="installation-a",
            expected_target_platform="test-platform",
            now=NOW,
        )
    assert not target_started


def test_p0_04_any_attestation_table_root_or_manifest_tamper_fails(
    tmp_path: Path,
) -> None:
    from rquant.runtime_code_attestation import (
        RuntimeCodeAttestation,
        RuntimeCodeTrustError,
        require_runtime_code_attestation,
    )
    from rquant.runtime_code_generation import (
        RuntimeCodeGenerationError,
        require_attested_runtime_generation,
    )
    from rquant.strict_json import (
        canonical_json_bytes,
        canonical_model_json_bytes,
        strict_model_validate_canonical_json,
    )
    from tests.runtime_code_e2e_support import build_test_package, install_test_package

    package = build_test_package(tmp_path / "package")
    attestation = strict_model_validate_canonical_json(
        RuntimeCodeAttestation,
        package.attestation_bytes,
    )
    changed_bundle = bytearray(package.bundle_bytes)
    changed_bundle[700] ^= 1
    changed_table = {
        **attestation.model_dump(mode="json"),
        "files": [file.model_dump(mode="json") for file in reversed(attestation.files)],
    }
    changed_root = attestation.model_copy(update={"content_root_sha256": "f" * 64})
    for bundle_bytes, attestation_bytes in (
        (bytes(changed_bundle), package.attestation_bytes),
        (package.bundle_bytes, canonical_json_bytes(changed_table)),
        (package.bundle_bytes, canonical_model_json_bytes(changed_root)),
    ):
        with pytest.raises(RuntimeCodeTrustError):
            require_runtime_code_attestation(
                attestation_bytes=attestation_bytes,
                certificate_bytes=package.certificate_bytes,
                bundle_bytes=bundle_bytes,
                root_keyring=package.root_keyring,
                runtime_keyring=package.runtime_keyring,
                expected_audience="formal-lab",
                expected_installation_id="installation-a",
                expected_target_platform="test-platform",
                now=NOW,
            )

    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    manifest_path = (
        runtime_root / "generations" / package.receipt.generation_id / "generation-manifest.json"
    )
    manifest_bytes = bytearray(manifest_path.read_bytes())
    manifest_bytes[-2] ^= 1
    manifest_path.chmod(0o644)
    manifest_path.write_bytes(manifest_bytes)
    manifest_path.chmod(0o444)
    with pytest.raises(RuntimeCodeGenerationError):
        require_attested_runtime_generation(
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
            now=NOW,
        )


def test_p0_09_stale_foreign_unpinned_and_lower_sequence_receipts_fail(
    tmp_path: Path,
) -> None:
    from rquant.adapter_manifest import VerifyOnlyEd25519Keyring
    from rquant.runtime_code_attestation import (
        RUNTIME_CODE_ATTESTATION_NAMESPACE,
        RuntimeCodeExecutionSpec,
        RuntimeCodeTrustError,
        require_runtime_code_attestation,
        sign_runtime_code_attestation,
        sign_runtime_code_trust_certificate,
    )
    from rquant.runtime_code_generation import (
        RuntimeCodeGenerationError,
        require_attested_runtime_generation,
    )
    from rquant.strict_json import canonical_model_json_bytes
    from tests.runtime_code_e2e_support import (
        NOW as E2E_NOW,
    )
    from tests.runtime_code_e2e_support import (
        build_test_package,
        install_test_package,
    )

    first = build_test_package(tmp_path / "first", source=b"VALUE = 1\n")
    trusted_base, runtime_root, installer = install_test_package(tmp_path, first)
    second = build_test_package(
        tmp_path / "second",
        sequence=2,
        previous_receipt_sha256=first.receipt.receipt_hash,
        source=b"VALUE = 2\n",
        authorities=first.authorities,
        promotion_state=first.promotion_state,
    )
    with pytest.raises(RuntimeCodeGenerationError, match="stale|invalid"):
        require_attested_runtime_generation(
            runtime_root=runtime_root,
            trusted_base=trusted_base,
            root_keyring=first.root_keyring,
            runtime_keyring=first.runtime_keyring,
            promotion_trust=first.promotion_trust,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            expected_audience="formal-lab",
            expected_installation_id="installation-a",
            expected_target_platform="test-platform",
            now=E2E_NOW,
        )
    installer.install(second.request())
    lower = build_test_package(
        tmp_path / "lower",
        sequence=1,
        previous_receipt_sha256=second.receipt.receipt_hash,
        source=b"VALUE = 1\n",
        authorities=first.authorities,
        promotion_state=first.promotion_state,
    )
    with pytest.raises(RuntimeCodeGenerationError, match="rollback"):
        installer.install(lower.request())
    rollback = build_test_package(
        tmp_path / "rollback",
        sequence=3,
        previous_receipt_sha256=second.receipt.receipt_hash,
        source=b"VALUE = 1\n",
        authorities=first.authorities,
        promotion_state=first.promotion_state,
    )
    assert installer.install(rollback.request()).generation_id == rollback.receipt.generation_id

    root_signer, _rr, root_keys, _old_signer, old_record, _old_keys, *_rest = first.authorities
    new_signer, new_record, _new_keys = contract_key_pair(
        tmp_path / "rotation",
        key_id="runtime-v2",
        issuer=old_record.issuer,
        key_purpose="rquant_runtime_code_signer",
        namespace=RUNTIME_CODE_ATTESTATION_NAMESPACE,
        rotation="active",
    )
    rotated_keys = VerifyOnlyEd25519Keyring(
        records=(old_record, new_record),
        issuer_allowlist={"rquant_runtime_code_signer": frozenset({old_record.issuer})},
        rotation_allowlist={
            (old_record.issuer, "rquant_runtime_code_signer"): frozenset(
                {old_record.key_id, new_record.key_id}
            )
        },
    )
    bundle = _bundle()
    certificate = sign_runtime_code_trust_certificate(
        root_signer=root_signer,
        runtime_signer=new_signer,
        audience="rquant-formal-runtime",
        installation_id="lab-installation-a",
        target_platform="macos-arm64-cpython-313",
        not_before=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(days=1),
    )
    rotated = sign_runtime_code_attestation(
        signer=new_signer,
        bundle=bundle,
        execution_spec=RuntimeCodeExecutionSpec(
            launcher_path="release/bin/rquant",
            working_directory="release",
            import_roots=("release/src",),
            interpreter_path="release/bin/python",
            interpreter_sha256=hashlib.sha256(INTERPRETER_BYTES).hexdigest(),
            python_abi="cpython-313-darwin-arm64",
        ),
        audience="rquant-formal-runtime",
        installation_id="lab-installation-a",
        target_platform="macos-arm64-cpython-313",
        provenance_commit="b" * 40,
        not_before=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
    )
    assert (
        require_runtime_code_attestation(
            attestation_bytes=canonical_model_json_bytes(rotated),
            certificate_bytes=canonical_model_json_bytes(certificate),
            bundle_bytes=bundle.bundle_bytes,
            root_keyring=root_keys,
            runtime_keyring=rotated_keys,
            expected_audience="rquant-formal-runtime",
            expected_installation_id="lab-installation-a",
            expected_target_platform="macos-arm64-cpython-313",
            now=NOW,
        ).attestation.key_id
        == "runtime-v2"
    )
    with pytest.raises(RuntimeCodeTrustError):
        require_runtime_code_attestation(
            attestation_bytes=canonical_model_json_bytes(rotated),
            certificate_bytes=canonical_model_json_bytes(certificate),
            bundle_bytes=bundle.bundle_bytes,
            root_keyring=root_keys,
            runtime_keyring=first.runtime_keyring,
            expected_audience="rquant-formal-runtime",
            expected_installation_id="lab-installation-a",
            expected_target_platform="macos-arm64-cpython-313",
            now=NOW,
        )
