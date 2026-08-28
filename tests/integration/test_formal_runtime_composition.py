from __future__ import annotations

import os
from pathlib import Path

import pytest

from rquant.external_monotonic_root import (
    ExternalMonotonicRootConfig,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.formal_runtime_composition import (
    FormalRuntimeBootstrapConfiguration,
    open_formal_runtime_capability,
)
from rquant.strict_json import canonical_model_json_bytes
from tests.runtime_code_e2e_support import (
    NOW,
    build_test_package,
    install_test_package,
)


def test_real_formal_composition_opens_generation_from_root_protected_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.formal_runtime_composition as composition

    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    receipt = package.receipt
    transport = UnixSocketExternalMonotonicRootManifest(
        role=receipt.role,
        authority_id=receipt.root_authority_id,
        store_id=receipt.root_store_id,
        rollback_domain_id=receipt.rollback_domain_id,
        socket_path=trusted_base / "promotion.sock",
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        peer_uid=os.getuid(),
        peer_gid=os.getgid(),
        connect_timeout_ms=100,
        max_response_bytes=1024 * 1024,
    )
    promotion_config = ExternalMonotonicRootConfig(
        transport="unix-socket-v1",
        transport_manifest_hash=transport.manifest_hash,
        role=receipt.role,
        root_authority_id=receipt.root_authority_id,
        root_store_id=receipt.root_store_id,
        root_issuer=receipt.issuer,
        root_key_id=receipt.key_id,
        root_key_purpose=receipt.key_purpose,
        root_receipt_namespace=receipt.namespace,
        root_public_key_fingerprint=receipt.public_key_fingerprint,
        witness_rollback_domain_id=receipt.rollback_domain_id,
        local_rollback_domain_id="local-runtime-code-domain",
    )
    configuration = FormalRuntimeBootstrapConfiguration(
        runtime_root=runtime_root,
        trusted_base=trusted_base,
        expected_material_uid=os.getuid(),
        expected_material_gid=os.getgid(),
        expected_audience="formal-lab",
        expected_installation_id="installation-a",
        expected_target_platform="test-platform",
        expected_python_abi="test-abi",
        root_keys=(package.authorities[1],),
        runtime_keys=(package.authorities[4],),
        promotion_key=package.authorities[7],
        promotion_config=promotion_config,
        promotion_transport=transport,
        promotion_subject_authority_id="installation-a-test-platform",
    )
    configuration_path = trusted_base / "runtime-code-bootstrap.json"
    configuration_path.write_bytes(canonical_model_json_bytes(configuration))
    configuration_path.chmod(0o444)

    class FakeClient:
        def __init__(self, manifest: UnixSocketExternalMonotonicRootManifest) -> None:
            self.role = manifest.role
            self.authority_id = manifest.authority_id
            self.store_id = manifest.store_id
            self.transport = manifest.transport
            self.manifest_hash = manifest.manifest_hash
            self.rollback_domain_id = manifest.rollback_domain_id

        def invoke(self, *, request_json: str) -> str:
            assert "installation-a-test-platform" in request_json
            return package.receipt_bytes.decode("utf-8")

    class FixedDatetime:
        @classmethod
        def now(cls, _timezone: object) -> object:
            return NOW

    monkeypatch.setattr(composition, "UnixSocketExternalMonotonicRootClient", FakeClient)
    monkeypatch.setattr(composition, "datetime", FixedDatetime)
    capability = open_formal_runtime_capability(
        configuration_path=configuration_path,
        trusted_base=trusted_base,
        expected_authority_uid=os.getuid(),
        expected_authority_gid=os.getgid(),
        startup_deadline_monotonic=10**12,
    )
    try:
        evidence = capability.evidence
        assert capability.loaded.promotion_receipt == package.receipt
        assert evidence.generation_id == package.receipt.generation_id
        assert evidence.attestation_sha256 == package.receipt.attestation_sha256
        assert evidence.content_root_sha256 == package.receipt.content_root_sha256
        assert evidence.promotion_sequence == package.receipt.promotion_sequence
        assert evidence.provenance_commit == capability.loaded.attestation.provenance_commit
        assert evidence.provenance_commit == "2" * 40
        capability.require_live()
    finally:
        capability.close()
