from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.runtime_code_support import contract_key_pair

NOW = datetime(2026, 8, 12, 6, tzinfo=UTC)


def _package(
    root: Path,
    *,
    sequence: int,
    previous_receipt_sha256: str,
    source: bytes,
    authorities: tuple[object, ...] | None = None,
) -> tuple[object, ...]:
    from rquant.runtime_code_attestation import (
        RUNTIME_CODE_ATTESTATION_NAMESPACE,
        RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
        RUNTIME_CODE_ROOT_NAMESPACE,
        RuntimeCodeBundleEntry,
        RuntimeCodeExecutionSpec,
        RuntimeCodePromotionReceipt,
        build_runtime_code_bundle,
        compute_runtime_code_generation_id,
        sign_runtime_code_attestation,
        sign_runtime_code_trust_certificate,
    )
    from rquant.strict_json import canonical_model_json_bytes

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
        authorities = (*root_authority, *runtime_authority)
    root_signer, _root_record, root_keyring, runtime_signer, _runtime_record, runtime_keyring = (
        authorities
    )
    bundle = build_runtime_code_bundle(
        (
            RuntimeCodeBundleEntry(
                path="release/bin/rquant",
                mode=0o555,
                content=b"#!/usr/bin/python3\n",
            ),
            RuntimeCodeBundleEntry(
                path="release/src/rquant/app.py",
                mode=0o444,
                content=source,
            ),
        )
    )
    certificate = sign_runtime_code_trust_certificate(
        root_signer=root_signer,
        runtime_signer=runtime_signer,
        audience="formal-lab",
        installation_id="installation-a",
        target_platform="test-platform",
        not_before=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(days=1),
    )
    attestation = sign_runtime_code_attestation(
        signer=runtime_signer,
        bundle=bundle,
        execution_spec=RuntimeCodeExecutionSpec(
            launcher_path="release/bin/rquant",
            working_directory="release",
            import_roots=("release/src",),
            interpreter_path="/usr/bin/python3",
            interpreter_sha256="1" * 64,
            python_abi="test-abi",
        ),
        audience="formal-lab",
        installation_id="installation-a",
        target_platform="test-platform",
        provenance_commit="2" * 40,
        not_before=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
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
    receipt = RuntimeCodePromotionReceipt(
        role="rquant_runtime_code_promotion_root",
        root_authority_id="external-root",
        root_store_id="external-store",
        issuer="external-issuer",
        key_id="external-v1",
        key_purpose="rquant_runtime_code_promotion_root",
        namespace=RUNTIME_CODE_PROMOTION_RECEIPT_NAMESPACE,
        public_key_fingerprint="3" * 64,
        rollback_domain_id="external-runtime-code-domain",
        attestation_sha256=hashlib.sha256(attestation_bytes).hexdigest(),
        bundle_sha256=bundle.bundle_sha256,
        content_root_sha256=bundle.content_root_sha256,
        installation_id="installation-a",
        target_platform="test-platform",
        generation_id=generation_id,
        promotion_sequence=sequence,
        previous_receipt_sha256=previous_receipt_sha256,
        signature="test-external-signature",
    )
    package_root = root / f"package-{sequence}"
    package_root.mkdir(mode=0o700)
    paths = {}
    for name, payload in (
        ("runtime-code.bundle", bundle.bundle_bytes),
        ("runtime-code-attestation.json", attestation_bytes),
        ("runtime-code-certificate.json", canonical_model_json_bytes(certificate)),
        ("runtime-code-promotion-receipt.json", canonical_model_json_bytes(receipt)),
    ):
        path = package_root / name
        path.write_bytes(payload)
        path.chmod(0o444)
        paths[name] = path
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
    return authorities, root_keyring, runtime_keyring, trust, package_root, paths, receipt


def _request(package_root: Path, paths: dict[str, Path]) -> object:
    from rquant.runtime_code_generation import RuntimeCodeInstallRequest

    return RuntimeCodeInstallRequest(
        source_root=package_root,
        bundle_path=paths["runtime-code.bundle"],
        attestation_path=paths["runtime-code-attestation.json"],
        certificate_path=paths["runtime-code-certificate.json"],
        receipt_path=paths["runtime-code-promotion-receipt.json"],
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
        now=NOW,
    )


def test_retaining_lease_rejects_symlink_hardlink_and_fifo(tmp_path: Path) -> None:
    from rquant.authority_path_security import (
        AuthorityPathSecurityError,
        open_secure_regular_file_lease,
    )

    root = tmp_path / "inputs"
    root.mkdir(mode=0o700)
    regular = root / "regular"
    regular.write_bytes(b"payload")
    regular.chmod(0o444)
    symlink = root / "symlink"
    symlink.symlink_to(regular)
    hardlink = root / "hardlink"
    os.link(regular, hardlink)
    fifo = root / "fifo"
    os.mkfifo(fifo, 0o444)
    for path in (symlink, hardlink, fifo):
        with pytest.raises(AuthorityPathSecurityError):
            open_secure_regular_file_lease(
                path,
                trusted_root=root,
                expected_uid=os.getuid(),
                expected_gid=os.getgid(),
                allowed_modes=frozenset({0o444}),
                max_bytes=1024,
            )


def test_collector_is_fd_anchored_and_rejects_input_replacement(
    tmp_path: Path,
) -> None:
    from rquant.runtime_code_generation import (
        RuntimeCodeCollectFile,
        RuntimeCodeGenerationError,
        collect_runtime_code_bundle,
    )

    checkout = tmp_path / "checkout"
    source = checkout / "src" / "rquant" / "app.py"
    source.parent.mkdir(parents=True, mode=0o700)
    source.write_bytes(b"ORIGINAL = True\n")
    source.chmod(0o644)
    replacement = checkout / "replacement.py"
    replacement.write_bytes(b"REPLACED = True\n")
    replacement.chmod(0o644)

    def replace(stage: str) -> None:
        if stage == "collector:after-open:src/rquant/app.py":
            os.replace(replacement, source)

    with pytest.raises(RuntimeCodeGenerationError, match="changed"):
        collect_runtime_code_bundle(
            checkout,
            (
                RuntimeCodeCollectFile(
                    source_path="src/rquant/app.py",
                    bundle_path="release/src/rquant/app.py",
                    mode=0o444,
                ),
            ),
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            fault_hook=replace,
        )


def test_installer_publishes_atomically_retains_old_generation_and_loads(
    tmp_path: Path,
) -> None:
    from rquant.runtime_code_generation import (
        RuntimeCodeGenerationInstaller,
        require_attested_runtime_generation,
    )

    trusted = tmp_path / "trusted"
    trusted.mkdir(mode=0o700)
    runtime_root = trusted / "runtime-code"
    runtime_root.mkdir(mode=0o700)
    authorities, root_keys, runtime_keys, trust, package_root, paths, first_receipt = _package(
        tmp_path / "first",
        sequence=1,
        previous_receipt_sha256="0" * 64,
        source=b"VERSION = 1\n",
    )
    installer = RuntimeCodeGenerationInstaller(
        runtime_root=runtime_root,
        trusted_base=trusted,
        root_keyring=root_keys,
        runtime_keyring=runtime_keys,
        promotion_trust=trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    first = installer.install(_request(package_root, paths))
    assert first.write_performed
    assert (runtime_root / "current").read_text(encoding="ascii") == first.generation_id + "\n"
    loaded = require_attested_runtime_generation(
        runtime_root=runtime_root,
        trusted_base=trusted,
        root_keyring=root_keys,
        runtime_keyring=runtime_keys,
        promotion_trust=trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
        now=NOW,
    )
    assert loaded.evidence.generation_id == first.generation_id
    assert loaded.release_root.joinpath("src/rquant/app.py").read_bytes() == b"VERSION = 1\n"

    _authorities, _rk, _sk, _trust, second_root, second_paths, second_receipt = _package(
        tmp_path / "second",
        sequence=2,
        previous_receipt_sha256=first_receipt.receipt_hash,
        source=b"VERSION = 2\n",
        authorities=authorities,
    )
    second = installer.install(_request(second_root, second_paths))
    assert second.previous_generation_id == first.generation_id
    assert (runtime_root / "previous").read_text(encoding="ascii") == first.generation_id + "\n"
    assert (runtime_root / "generations" / first.generation_id).is_dir()
    release_file = (
        runtime_root / "generations" / second_receipt.generation_id / "release/src/rquant/app.py"
    )
    observed = release_file.lstat()
    assert observed.st_nlink == 1
    assert observed.st_mode & 0o777 == 0o444


def test_installer_crash_before_pointer_preserves_current_and_recovers(tmp_path: Path) -> None:
    from rquant.runtime_code_generation import (
        RuntimeCodeGenerationError,
        RuntimeCodeGenerationInstaller,
    )

    trusted = tmp_path / "trusted"
    trusted.mkdir(mode=0o700)
    runtime_root = trusted / "runtime-code"
    runtime_root.mkdir(mode=0o700)
    authorities, root_keys, runtime_keys, trust, first_root, first_paths, first_receipt = _package(
        tmp_path / "first",
        sequence=1,
        previous_receipt_sha256="0" * 64,
        source=b"VERSION = 1\n",
    )
    installer = RuntimeCodeGenerationInstaller(
        runtime_root=runtime_root,
        trusted_base=trusted,
        root_keyring=root_keys,
        runtime_keyring=runtime_keys,
        promotion_trust=trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    first = installer.install(_request(first_root, first_paths))
    _a, _r, _s, _t, second_root, second_paths, _second_receipt = _package(
        tmp_path / "second",
        sequence=2,
        previous_receipt_sha256=first_receipt.receipt_hash,
        source=b"VERSION = 2\n",
        authorities=authorities,
    )
    crashing = RuntimeCodeGenerationInstaller(
        runtime_root=runtime_root,
        trusted_base=trusted,
        root_keyring=root_keys,
        runtime_keyring=runtime_keys,
        promotion_trust=trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        fault_hook=lambda stage: (
            (_ for _ in ()).throw(RuntimeError("crash"))
            if stage == "installer:before-pointer"
            else None
        ),
    )
    with pytest.raises(RuntimeCodeGenerationError, match="crash"):
        crashing.install(_request(second_root, second_paths))
    assert (runtime_root / "current").read_text(encoding="ascii") == first.generation_id + "\n"
    recovered = installer.install(_request(second_root, second_paths))
    assert recovered.previous_generation_id == first.generation_id
    assert not any(
        path.name.endswith(".staging") for path in (runtime_root / "generations").iterdir()
    )
