from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pytest

from rquant.runtime_code_attestation import RuntimeCodeAttestation
from rquant.runtime_code_generation import RuntimeCodeCollectFile
from rquant.runtime_code_operations import (
    RuntimeCodeFormalService,
    RuntimeCodeGenerationOperator,
    RuntimeCodeMigrationRequest,
    RuntimeCodeOperationError,
    RuntimeCodePackageRequest,
    RuntimeCodeRotateRequest,
)
from rquant.strict_json import strict_model_validate_canonical_json
from tests.runtime_code_e2e_support import (
    NOW,
    RuntimeCodeTestPackage,
    build_test_package,
    install_test_package,
)

ROOT = Path(__file__).resolve().parents[2]


def _operator(
    root: Path,
    package: RuntimeCodeTestPackage,
    *,
    runtime_root: Path | None = None,
) -> RuntimeCodeGenerationOperator:
    trusted_base = root / "trusted"
    trusted_base.mkdir(mode=0o700, parents=True, exist_ok=True)
    selected_runtime_root = runtime_root or trusted_base / "runtime-code"
    selected_runtime_root.mkdir(mode=0o700, exist_ok=True)
    return RuntimeCodeGenerationOperator(
        runtime_root=selected_runtime_root,
        trusted_base=trusted_base,
        root_keyring=package.root_keyring,
        runtime_keyring=package.runtime_keyring,
        promotion_trust=package.promotion_trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
    )


def _migration(package: RuntimeCodeTestPackage) -> RuntimeCodeMigrationRequest:
    return RuntimeCodeMigrationRequest(
        install=package.request(),
        formal_services=(
            RuntimeCodeFormalService(
                command="lab-claim-finalizer",
                unit_path=ROOT / "deploy/systemd/rquant-lab-claim-finalizer.service",
            ),
        ),
        expected_configuration_path=Path("/etc/rquant/runtime-code-bootstrap.json"),
        expected_trusted_base=Path("/etc/rquant"),
        expected_authority_uid=0,
        expected_authority_gid=0,
    )


def test_package_uses_existing_certificate_and_emits_installable_canonical_artifacts(
    tmp_path: Path,
) -> None:
    existing = build_test_package(tmp_path / "existing")
    source = tmp_path / "checkout"
    for relative, payload, mode in (
        ("bin/python", b"RQUANT-TEST-INTERPRETER\n", 0o555),
        ("bin/rquant", b"TARGET_STARTED = True\n", 0o555),
        ("src/rquant/app.py", b"VALUE = 7\n", 0o444),
    ):
        path = source / relative
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.write_bytes(payload)
        path.chmod(mode)
    request = RuntimeCodePackageRequest(
        checkout_root=source,
        output_root=tmp_path / "candidate",
        certificate_path=existing.paths["runtime-code-certificate.json"],
        files=(
            RuntimeCodeCollectFile(
                source_path="bin/python",
                bundle_path="release/bin/python",
                mode=0o555,
            ),
            RuntimeCodeCollectFile(
                source_path="bin/rquant",
                bundle_path="release/bin/rquant",
                mode=0o555,
            ),
            RuntimeCodeCollectFile(
                source_path="src/rquant/app.py",
                bundle_path="release/src/rquant/app.py",
                mode=0o444,
            ),
        ),
        execution_spec=strict_model_validate_canonical_json(
            RuntimeCodeAttestation,
            existing.attestation_bytes,
        ).execution_spec,
        audience="formal-lab",
        installation_id="installation-a",
        target_platform="test-platform",
        provenance_commit="7" * 40,
        not_before=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
        promotion_sequence=1,
        previous_receipt_sha256="0" * 64,
        now=NOW,
        expected_source_uid=os.getuid(),
        expected_source_gid=os.getgid(),
    )
    operator = _operator(tmp_path / "operator", existing)

    result = operator.package(
        request,
        runtime_signer=existing.authorities[3],
        promotion_signer=existing.authorities[6],
    )

    assert result.status == "packaged"
    assert result.output_root == request.output_root
    assert tuple(sorted(path.name for path in result.output_root.iterdir())) == (
        "runtime-code-attestation.json",
        "runtime-code-certificate.json",
        "runtime-code-promotion-receipt.json",
        "runtime-code.bundle",
    )
    assert all(path.stat().st_mode & 0o777 == 0o444 for path in result.output_root.iterdir())


def test_dry_run_is_read_only_and_failed_install_keeps_selected_generation(
    tmp_path: Path,
) -> None:
    first = build_test_package(tmp_path / "packages", sequence=1)
    trusted_base, runtime_root, _installer = install_test_package(tmp_path / "installed", first)
    second = build_test_package(
        tmp_path / "packages-next",
        sequence=2,
        previous_receipt_sha256=first.receipt.receipt_hash,
        authorities=first.authorities,
        promotion_state=first.promotion_state,
        source=b"VALUE = 2\n",
    )
    operator = RuntimeCodeGenerationOperator(
        runtime_root=runtime_root,
        trusted_base=trusted_base,
        root_keyring=second.root_keyring,
        runtime_keyring=second.runtime_keyring,
        promotion_trust=second.promotion_trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
    )
    current_before = (runtime_root / "current").read_bytes()
    generations_before = tuple(
        sorted(path.name for path in (runtime_root / "generations").iterdir())
    )

    plan = operator.dry_run(_migration(second))

    assert plan.status == "ready"
    assert plan.generation_id == second.receipt.generation_id
    assert (runtime_root / "current").read_bytes() == current_before
    assert tuple(sorted(path.name for path in (runtime_root / "generations").iterdir())) == (
        generations_before
    )

    second.promotion_state.current_bytes = first.receipt_bytes
    with pytest.raises(RuntimeCodeOperationError, match="promotion"):
        operator.install(_migration(second))
    assert (runtime_root / "current").read_bytes() == current_before


def test_preflight_rejects_legacy_service_arguments_and_residue_without_writes(
    tmp_path: Path,
) -> None:
    package = build_test_package(tmp_path / "package")
    operator = _operator(tmp_path / "operator", package)
    legacy = tmp_path / "legacy-checkout-authority.json"
    legacy.write_text("{}", encoding="ascii")
    request = _migration(package).model_copy(
        update={
            "formal_services": (
                RuntimeCodeFormalService(
                    command="lab-claim-finalizer",
                    unit_path=ROOT / "deploy/systemd/rquant-lab-claim-finalizer.service",
                ),
            ),
            "legacy_paths": (legacy,),
        }
    )

    with pytest.raises(RuntimeCodeOperationError, match="legacy"):
        operator.dry_run(request)

    assert not (operator.runtime_root / "current").exists()
    assert not (operator.runtime_root / "generations").exists()


def test_preflight_reads_actual_unit_and_rejects_static_drift(
    tmp_path: Path,
) -> None:
    package = build_test_package(tmp_path / "package")
    operator = _operator(tmp_path / "operator", package)
    unit = tmp_path / "rquant-lab-claim-finalizer.service"
    raw = (ROOT / "deploy/systemd/rquant-lab-claim-finalizer.service").read_text(encoding="utf-8")
    assert "--role lab_claim_finalizer" in raw
    unit.write_text(raw.replace("--role lab_claim_finalizer", "--role lab_claim_finalizer --drift"))
    request = _migration(package).model_copy(
        update={
            "formal_services": (
                RuntimeCodeFormalService(
                    command="lab-claim-finalizer",
                    unit_path=unit,
                ),
            ),
        }
    )

    with pytest.raises(RuntimeCodeOperationError, match="service artifact"):
        operator.dry_run(request)


def test_install_atomically_selects_candidate_and_retains_previous_pointer(
    tmp_path: Path,
) -> None:
    first = build_test_package(tmp_path / "packages", sequence=1)
    trusted_base, runtime_root, _installer = install_test_package(tmp_path / "installed", first)
    second = build_test_package(
        tmp_path / "packages-next",
        sequence=2,
        previous_receipt_sha256=first.receipt.receipt_hash,
        authorities=first.authorities,
        promotion_state=first.promotion_state,
        source=b"VALUE = 2\n",
    )
    operator = RuntimeCodeGenerationOperator(
        runtime_root=runtime_root,
        trusted_base=trusted_base,
        root_keyring=second.root_keyring,
        runtime_keyring=second.runtime_keyring,
        promotion_trust=second.promotion_trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
    )

    result = operator.install(_migration(second))

    assert result.status == "installed"
    assert result.receipt.generation_id == second.receipt.generation_id
    assert (runtime_root / "current").read_text(encoding="ascii") == (
        f"{second.receipt.generation_id}\n"
    )
    assert (runtime_root / "previous").read_text(encoding="ascii") == (
        f"{first.receipt.generation_id}\n"
    )


def test_rotate_reissues_retained_bytes_only_at_a_higher_sequence(tmp_path: Path) -> None:
    current = build_test_package(tmp_path / "current-package", sequence=1)
    trusted_base, runtime_root, _installer = install_test_package(tmp_path / "installed", current)
    retained = build_test_package(
        tmp_path / "retained-package",
        sequence=1,
        authorities=current.authorities,
        source=b"OLD = True\n",
    )
    operator = RuntimeCodeGenerationOperator(
        runtime_root=runtime_root,
        trusted_base=trusted_base,
        root_keyring=current.root_keyring,
        runtime_keyring=current.runtime_keyring,
        promotion_trust=current.promotion_trust,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
    )
    request = RuntimeCodeRotateRequest(
        retained_package_root=retained.package_root,
        output_root=tmp_path / "rollback-package",
        promotion_sequence=2,
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
        now=NOW,
    )

    result = operator.rotate(request, promotion_signer=current.authorities[6])

    assert result.promotion_sequence == 2
    assert result.previous_receipt_sha256 == current.receipt.receipt_hash
    assert (result.output_root / "runtime-code.bundle").read_bytes() == retained.bundle_bytes
    with pytest.raises(RuntimeCodeOperationError, match="higher"):
        operator.rotate(
            request.model_copy(update={"promotion_sequence": 1, "output_root": tmp_path / "bad"}),
            promotion_signer=current.authorities[6],
        )
